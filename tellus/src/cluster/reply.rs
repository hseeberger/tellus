use crate::{
    ActorId, ReplyTo,
    cluster::{
        codec::{Codec, CodecError},
        discovery::Nonce,
        endpoint::{self, EndpointInner},
        frame::Frame,
        node::NodeId,
        sink::{RemoteSendError, encode_and_send},
    },
    request_response::SendReply,
    sync::lock,
    watch::{ActorTerminated, TerminatedHandler, Watcher, WatcherRegistry},
};
use serde::{
    Deserialize, Deserializer, Serialize, Serializer,
    de::{self, DeserializeOwned},
    ser,
};
use std::{
    any::{Any, type_name},
    borrow::Cow,
    cell::RefCell,
    collections::HashMap,
    mem,
    net::SocketAddr,
    sync::{Arc, Mutex, atomic::AtomicU64},
};
use thiserror::Error;
use tracing::{debug, warn};

type DeliverReply = fn(Box<dyn Any + Send>, &[u8], &dyn Codec) -> Result<(), CodecError>;

thread_local! {
    static MINTED_TAGS: RefCell<Option<Vec<ReplyTag>>> = const { RefCell::new(None) };
}

#[cfg_attr(docsrs, doc(cfg(feature = "cluster")))]
impl<R> Serialize for ReplyTo<R>
where
    R: DeserializeOwned + Send + 'static,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let Some(endpoint) = endpoint::get() else {
            return Err(ser::Error::custom(ReplyToError::EndpointNotStarted));
        };

        let Some(send_reply) = self.take_send_reply() else {
            return Err(ser::Error::custom(ReplyToError::AlreadySerialized));
        };

        let nonce = endpoint.pending_replies().add(
            Box::new(send_reply),
            deliver_reply::<R>,
            self.recipient(),
            self.recipient_watchers(),
        );
        note_minted(ReplyTag {
            nonce,
            recipient: self.recipient(),
        });

        WireReplyTo {
            node: endpoint.node(),
            nonce,
            recipient: self.recipient(),
        }
        .serialize(serializer)
    }
}

#[cfg_attr(docsrs, doc(cfg(feature = "cluster")))]
impl<'de, R> Deserialize<'de> for ReplyTo<R>
where
    R: Serialize + Send + 'static,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let WireReplyTo {
            node,
            nonce,
            recipient,
        } = WireReplyTo::deserialize(deserializer)?;

        let Some(endpoint) = endpoint::get() else {
            return Err(de::Error::custom(ReplyToError::EndpointNotStarted));
        };

        if node == endpoint.node() {
            let Some(pending) = endpoint.pending_replies().take(nonce) else {
                return Err(de::Error::custom(ReplyToError::NoPendingReply));
            };
            let send_reply = pending
                .send_reply
                .downcast::<SendReply<R>>()
                .map_err(|_| de::Error::custom(ReplyToError::ReplyType))?;
            let recipient_watchers =
                recipient.and_then(|recipient| endpoint.registry().watcher_registry(recipient));
            Ok(ReplyTo::new(*send_reply).with_recipient(recipient, recipient_watchers))
        } else {
            Ok(remote_proxy(node, nonce, recipient))
        }
    }
}

/// A reply destination which cannot be serialized or resolved; rendered into the serde error.
#[derive(Debug, Error)]
pub(crate) enum ReplyToError {
    #[error("remoting endpoint not started")]
    EndpointNotStarted,

    #[error("reply destination already serialized")]
    AlreadySerialized,

    #[error("no pending reply destination under this nonce")]
    NoPendingReply,

    #[error("reply destination of another reply type")]
    ReplyType,
}

/// Dropping an entry resolves its ask as `NoReply`; otherwise only the ask timeout resolves it.
pub(crate) struct PendingReplies {
    next_nonce: AtomicU64,
    evictor_id: ActorId,
    pending: Mutex<HashMap<Nonce, PendingReply>>,
}

impl PendingReplies {
    pub(crate) fn new() -> Self {
        Self {
            next_nonce: AtomicU64::new(0),
            evictor_id: ActorId::new(),
            pending: Mutex::new(HashMap::new()),
        }
    }

    /// Stamping arms an entry's eviction on that node's death or its lane's give-up.
    pub(crate) fn stamp(&self, tags: &[ReplyTag], peer: NodeId) {
        if tags.is_empty() {
            return;
        }

        let mut pending = lock(&self.pending);
        for tag in tags {
            if let Some(entry) = pending.get_mut(&tag.nonce) {
                entry.peer = Some(peer);
            }
        }
    }

    pub(crate) fn discard(&self, tags: &[ReplyTag]) {
        if tags.is_empty() {
            return;
        }

        let mut pending = lock(&self.pending);
        for tag in tags {
            pending.remove(&tag.nonce);
        }
    }

    /// Releases only the destinations no actor awaits; those are their actor's to wait for.
    pub(crate) fn discard_ask(&self, tags: &[ReplyTag]) {
        if tags.is_empty() {
            return;
        }

        let mut pending = lock(&self.pending);
        for tag in tags {
            if tag.recipient.is_none() {
                pending.remove(&tag.nonce);
            }
        }
    }

    /// An actor-origin request has no timeout, so its entry must not outlive its recipient.
    pub(crate) fn fail_recipient(&self, recipient: ActorId) {
        lock(&self.pending).retain(|_, entry| entry.recipient != Some(recipient));
    }

    /// A given-up lane owes nothing: every incarnation at its address is evicted, as on node death.
    pub(crate) fn fail_addr(&self, addr: SocketAddr) {
        lock(&self.pending).retain(|_, entry| entry.peer.is_none_or(|peer| peer.addr() != addr));
    }

    pub(crate) fn fail_fenced(&self, fence: NodeId) {
        lock(&self.pending).retain(|_, entry| entry.peer.is_none_or(|peer| !fence.covers(peer)));
    }

    /// The eviction is registered after the insert and never under the pending lock the sink takes.
    fn add(
        &self,
        send_reply: Box<dyn Any + Send>,
        deliver: DeliverReply,
        recipient: Option<ActorId>,
        recipient_watchers: Option<&WatcherRegistry>,
    ) -> Nonce {
        let nonce = Nonce::mint(&self.next_nonce);

        lock(&self.pending).insert(
            nonce,
            PendingReply {
                peer: None,
                recipient,
                send_reply,
                deliver,
            },
        );

        if let Some(watchers) = recipient_watchers
            && watchers
                .add(Watcher::new(self.evictor_id, Arc::new(RecipientEvictor)))
                .is_err()
        {
            lock(&self.pending).remove(&nonce);
        }

        nonce
    }

    fn take(&self, nonce: Nonce) -> Option<PendingReply> {
        lock(&self.pending).remove(&nonce)
    }

    fn take_from(&self, nonce: Nonce, peer: NodeId) -> Option<PendingReply> {
        let mut pending = lock(&self.pending);

        pending
            .get(&nonce)
            .is_some_and(|entry| entry.peer == Some(peer))
            .then(|| pending.remove(&nonce))
            .flatten()
    }
}

/// Repeated in the frame, so a node dead-lettering the message undecoded can still answer each.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ReplyTag {
    pub(crate) nonce: Nonce,
    pub(crate) recipient: Option<ActorId>,
}

/// Releases the ask's own reply destinations on drop, however the ask ends.
pub(crate) struct AskReplyTags<'a> {
    pending: &'a PendingReplies,
    tags: Vec<ReplyTag>,
}

impl<'a> AskReplyTags<'a> {
    pub(crate) fn new(pending: &'a PendingReplies, tags: Vec<ReplyTag>) -> Self {
        Self { pending, tags }
    }

    /// Only once the ask resolved: its entry was taken or dropped with it, nothing is left.
    pub(crate) fn disarm(&mut self) {
        self.tags.clear();
    }
}

impl Drop for AskReplyTags<'_> {
    fn drop(&mut self) {
        self.pending.discard_ask(&self.tags);
    }
}

/// Runs `f` recording every reply destination minted on this thread, so a send site learns them.
pub(crate) fn record_minted<T, F>(f: F) -> (T, Vec<ReplyTag>)
where
    F: FnOnce() -> T,
{
    /// Must restore the outer recording even if `f` unwinds!
    struct Restore {
        outer: Option<Vec<ReplyTag>>,
    }

    impl Drop for Restore {
        fn drop(&mut self) {
            MINTED_TAGS.set(self.outer.take());
        }
    }

    let restore = Restore {
        outer: MINTED_TAGS.replace(Some(Vec::new())),
    };
    let value = f();
    let tags = MINTED_TAGS.replace(None).unwrap_or_default();
    drop(restore);

    (value, tags)
}

/// Called once a request's frame is known never to reach its peer; each entry holds a sink.
pub(crate) fn discard_replies(tags: &[ReplyTag]) {
    if let Some(endpoint) = endpoint::get() {
        endpoint.pending_replies().discard(tags);
    }
}

pub(crate) fn ask_reply_tags(tags: Vec<ReplyTag>) -> Option<AskReplyTags<'static>> {
    endpoint::get().map(|endpoint| AskReplyTags::new(endpoint.pending_replies(), tags))
}

/// Only the peer a request was sent to may answer it: a nonce is unique to this node only.
pub(crate) fn on_reply(endpoint: &EndpointInner, peer: NodeId, nonce: Nonce, payload: &[u8]) {
    match endpoint.pending_replies().take_from(nonce, peer) {
        Some(pending) => {
            if let Err(error) = (pending.deliver)(pending.send_reply, payload, endpoint.codec()) {
                warn!(%peer, %nonce, %error, "dead letter");
            }
        }

        None => warn!(
            %peer,
            %nonce,
            error = "no pending reply destination this peer was asked under this nonce",
            "dead letter"
        ),
    }
}

/// Dropping the entry resolves its ask as `NoReply`, hence only its peer may drop it.
pub(crate) fn on_reply_dropped(endpoint: &EndpointInner, peer: NodeId, nonce: Nonce) {
    endpoint.pending_replies().take_from(nonce, peer);
}

/// For a message dead-lettered undecoded only, and tolerant of a repeated notification.
pub(crate) fn on_undeliverable(endpoint: &EndpointInner, peer: NodeId, reply_tags: &[ReplyTag]) {
    for tag in reply_tags {
        let frame = Frame::ReplyDropped {
            nonce: tag.nonce,
            recipient: tag.recipient,
        };
        if let Err(error) = endpoint.send(peer, frame) {
            debug!(%peer, nonce = %tag.nonce, %error, "cannot send reply-dropped notification");
        }
    }
}

struct PendingReply {
    peer: Option<NodeId>,
    recipient: Option<ActorId>,
    send_reply: Box<dyn Any + Send>,
    deliver: DeliverReply,
}

/// Registered on the actor a reply is delivered to, hence the signaled ID is the one to evict.
struct RecipientEvictor;

impl TerminatedHandler for RecipientEvictor {
    fn handle_terminated(&self, actor_id: ActorId) -> Result<(), ActorTerminated> {
        if let Some(endpoint) = endpoint::get() {
            endpoint.pending_replies().fail_recipient(actor_id);
        }

        Ok(())
    }
}

/// Notifies from `drop`; a proxy which sends its reply forgets the guard instead.
struct ReplyGuard {
    origin: NodeId,
    nonce: Nonce,
    recipient: Option<ActorId>,
}

impl Drop for ReplyGuard {
    fn drop(&mut self) {
        let Some(endpoint) = endpoint::get() else {
            return;
        };

        let frame = Frame::ReplyDropped {
            nonce: self.nonce,
            recipient: self.recipient,
        };
        if let Err(error) = endpoint.send(self.origin, frame) {
            debug!(
                origin = %self.origin,
                nonce = %self.nonce,
                %error,
                "cannot send reply-dropped notification"
            );
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct WireReplyTo {
    node: NodeId,
    nonce: Nonce,
    recipient: Option<ActorId>,
}

fn deliver_reply<R>(
    send_reply: Box<dyn Any + Send>,
    payload: &[u8],
    codec: &dyn Codec,
) -> Result<(), CodecError>
where
    R: DeserializeOwned + Send + 'static,
{
    let send_reply = send_reply
        .downcast::<SendReply<R>>()
        .expect("the entry was added with this reply type's sink");
    let reply = codec.decode_to::<R>(payload)?;
    send_reply(reply);
    Ok(())
}

fn note_minted(tag: ReplyTag) {
    MINTED_TAGS.with_borrow_mut(|tags| {
        if let Some(tags) = tags {
            tags.push(tag);
        }
    });
}

fn remote_proxy<R>(origin: NodeId, nonce: Nonce, recipient: Option<ActorId>) -> ReplyTo<R>
where
    R: Serialize + Send + 'static,
{
    let guard = ReplyGuard {
        origin,
        nonce,
        recipient,
    };

    ReplyTo::new(
        move |reply| match send_reply_frame(&reply, origin, nonce, recipient) {
            Ok(()) => mem::forget(guard),

            Err(error) => warn!(
                origin = %origin,
                recipient = ?recipient,
                reply_type = type_name::<R>(),
                %error,
                "dead letter"
            ),
        },
    )
    .with_recipient(recipient, None)
}

fn send_reply_frame<R>(
    reply: &R,
    origin: NodeId,
    nonce: Nonce,
    recipient: Option<ActorId>,
) -> Result<(), RemoteSendError>
where
    R: Serialize,
{
    let endpoint = endpoint::get().ok_or(RemoteSendError::EndpointNotStarted)?;

    encode_and_send(
        endpoint,
        origin,
        || endpoint.codec().encode(reply),
        |payload, _| Frame::Reply {
            nonce,
            recipient,
            payload: Cow::Owned(payload),
        },
    )?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::{
        ActorId, MailboxCapacity,
        cluster::{
            codec::{Codec, CodecError},
            discovery::Nonce,
            node::NodeId,
            reply::{AskReplyTags, PendingReplies, ReplyTag, note_minted, record_minted},
        },
        mailbox::make_mailbox,
    };
    use std::any::Any;

    fn add_entry(pending: &PendingReplies, recipient: Option<ActorId>) -> Nonce {
        pending.add(Box::new(()), deliver_nothing, recipient, None)
    }

    fn tag(nonce: Nonce) -> ReplyTag {
        ReplyTag {
            nonce,
            recipient: Some(ActorId::new()),
        }
    }

    /// An entry is taken exactly once; a second take under the same nonce finds nothing, which is
    /// also what makes a repeated reply-dropped notification for one entry a no-op.
    #[test]
    fn entries_are_taken_at_most_once() {
        let pending = PendingReplies::new();

        let nonce = add_entry(&pending, None);

        assert!(pending.take(nonce).is_some());
        assert!(pending.take(nonce).is_none());
    }

    /// An unstamped entry and a newer incarnation at the address survive the fence.
    #[test]
    fn fail_fenced_evicts_only_covered_stamps() {
        let addr = "127.0.0.1:1234".parse().expect("valid address");
        let peer = NodeId::new(addr);
        let other = NodeId::new(addr);
        let pending = PendingReplies::new();

        assert!(peer.incarnation() < other.incarnation());
        let stamped = add_entry(&pending, None);
        let other_stamped = add_entry(&pending, None);
        let unstamped = add_entry(&pending, None);
        pending.stamp(&[tag(stamped)], peer);
        pending.stamp(&[tag(other_stamped)], other);

        pending.fail_fenced(peer);

        assert!(pending.take(stamped).is_none());
        assert!(pending.take(other_stamped).is_some());
        assert!(pending.take(unstamped).is_some());
    }

    /// Giving up a lane evicts the entries stamped with any incarnation at its address, so their
    /// asks resolve as `NoReply` instead of waiting out their timeout; an unstamped entry and one
    /// stamped with another address survive.
    #[test]
    fn fail_addr_evicts_every_incarnation_at_the_address() {
        let addr = "127.0.0.1:1234".parse().expect("valid address");
        let other_addr = "127.0.0.1:5678".parse().expect("valid address");
        let pending = PendingReplies::new();

        let stamped = add_entry(&pending, None);
        let successor_stamped = add_entry(&pending, None);
        let other_stamped = add_entry(&pending, None);
        let unstamped = add_entry(&pending, None);
        pending.stamp(&[tag(stamped)], NodeId::new(addr));
        pending.stamp(&[tag(successor_stamped)], NodeId::new(addr));
        pending.stamp(&[tag(other_stamped)], NodeId::new(other_addr));

        pending.fail_addr(addr);

        assert!(pending.take(stamped).is_none());
        assert!(pending.take(successor_stamped).is_none());
        assert!(pending.take(other_stamped).is_some());
        assert!(pending.take(unstamped).is_some());
    }

    /// Only the peer a request was sent to settles its entry: a nonce is unique to this node, not
    /// to the member naming it, so another member's reply or reply-dropped frame finds nothing.
    #[test]
    fn only_the_asked_peer_settles_an_entry() {
        let asked = NodeId::new("127.0.0.1:1234".parse().expect("valid address"));
        let other = NodeId::new("127.0.0.1:5678".parse().expect("valid address"));
        let pending = PendingReplies::new();

        let nonce = add_entry(&pending, None);
        pending.stamp(&[tag(nonce)], asked);

        assert!(pending.take_from(nonce, other).is_none());
        assert!(pending.take_from(nonce, asked).is_some());
    }

    /// An entry is stamped before its frame is sent, so an unstamped one was never asked of
    /// anybody and no peer can settle it.
    #[test]
    fn an_unstamped_entry_is_settled_by_nobody() {
        let peer = NodeId::new("127.0.0.1:1234".parse().expect("valid address"));
        let pending = PendingReplies::new();

        let nonce = add_entry(&pending, None);

        assert!(pending.take_from(nonce, peer).is_none());
        assert!(pending.take(nonce).is_some());
    }

    /// An actor-origin request has no timeout, so the entry is bounded by the life of the actor
    /// awaiting the reply; entries of another recipient and recipientless ones survive.
    #[test]
    fn fail_recipient_evicts_only_that_recipients_entries() {
        let (recipient, other) = (ActorId::new(), ActorId::new());
        let pending = PendingReplies::new();

        let of_recipient = add_entry(&pending, Some(recipient));
        let of_other = add_entry(&pending, Some(other));
        let of_nobody = add_entry(&pending, None);

        pending.fail_recipient(recipient);

        assert!(pending.take(of_recipient).is_none());
        assert!(pending.take(of_other).is_some());
        assert!(pending.take(of_nobody).is_some());
    }

    /// One eviction registration serves all of a recipient's entries: registering is idempotent
    /// per watcher ID, so a busy asker does not accumulate watchers on itself.
    #[test]
    fn entries_of_one_recipient_register_one_eviction() {
        let (mailbox_handle, mailbox) = make_mailbox::<()>(MailboxCapacity::Unbounded);
        let recipient = ActorId::new();
        let pending = PendingReplies::new();

        for _ in 0..3 {
            pending.add(
                Box::new(()),
                deliver_nothing,
                Some(recipient),
                Some(mailbox_handle.watcher_registry()),
            );
        }

        let (_incoming_rx, watchers) = mailbox.split();
        assert_eq!(watchers.close().len(), 1);
    }

    /// A recipient which terminated before the entry was added can never receive the reply, so the
    /// entry is reverted rather than left for the peer's death to collect.
    #[test]
    fn an_entry_for_a_terminated_recipient_is_reverted() {
        let (mailbox_handle, mailbox) = make_mailbox::<()>(MailboxCapacity::Unbounded);
        let (_incoming_rx, watchers) = mailbox.split();
        watchers.close();
        let pending = PendingReplies::new();

        let nonce = pending.add(
            Box::new(()),
            deliver_nothing,
            Some(ActorId::new()),
            Some(mailbox_handle.watcher_registry()),
        );

        assert!(pending.take(nonce).is_none());
    }

    /// Discarding removes exactly the given entries, e.g. after a failed send.
    #[test]
    fn discard_removes_the_given_entries() {
        let pending = PendingReplies::new();

        let discarded = add_entry(&pending, None);
        let kept = add_entry(&pending, None);

        pending.discard(&[tag(discarded)]);

        assert!(pending.take(discarded).is_none());
        assert!(pending.take(kept).is_some());
    }

    /// An ask releases only the destination it awaits itself; one the request carried for an
    /// actor is that actor's to wait for.
    #[test]
    fn discard_ask_releases_only_destinations_no_actor_awaits() {
        let pending = PendingReplies::new();

        let own = add_entry(&pending, None);
        let actor = ActorId::new();
        let actors = add_entry(&pending, Some(actor));

        pending.discard_ask(&[
            ReplyTag {
                nonce: own,
                recipient: None,
            },
            ReplyTag {
                nonce: actors,
                recipient: Some(actor),
            },
        ]);

        assert!(pending.take(own).is_none());
        assert!(pending.take(actors).is_some());
    }

    /// Dropping the guard is what releases an ask's own destination, so a cancelled ask costs
    /// its entry no longer than a timed out one.
    #[test]
    fn dropping_the_guard_releases_the_asks_own_destinations() {
        let pending = PendingReplies::new();

        let own = add_entry(&pending, None);
        let actor = ActorId::new();
        let actors = add_entry(&pending, Some(actor));

        drop(AskReplyTags::new(
            &pending,
            vec![
                ReplyTag {
                    nonce: own,
                    recipient: None,
                },
                ReplyTag {
                    nonce: actors,
                    recipient: Some(actor),
                },
            ],
        ));

        assert!(pending.take(own).is_none());
        assert!(pending.take(actors).is_some());
    }

    /// A resolved ask disarms its guard; the entry is gone already, so nothing is released.
    #[test]
    fn a_disarmed_guard_releases_nothing() {
        let pending = PendingReplies::new();

        let own = add_entry(&pending, None);
        let mut guard = AskReplyTags::new(
            &pending,
            vec![ReplyTag {
                nonce: own,
                recipient: None,
            }],
        );

        guard.disarm();
        drop(guard);

        assert!(pending.take(own).is_some());
    }

    /// A nested recording keeps its tags to itself and restores the outer one, so a send during
    /// an encode cannot corrupt the outer send's bookkeeping.
    #[test]
    fn record_minted_scopes_nest() {
        let ((), outer) = record_minted(|| {
            note_minted(tag(Nonce::first()));

            let ((), inner) = record_minted(|| note_minted(tag(Nonce::first())));
            assert_eq!(inner.len(), 1);

            note_minted(tag(Nonce::first()));
        });

        assert_eq!(outer.len(), 2);
    }

    fn deliver_nothing(
        _send_reply: Box<dyn Any + Send>,
        _payload: &[u8],
        _codec: &dyn Codec,
    ) -> Result<(), CodecError> {
        Ok(())
    }
}
