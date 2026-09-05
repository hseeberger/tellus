use crate::{
    ActorId, ActorRef,
    cluster::{
        endpoint::{self, EndpointInner},
        frame::Frame,
        node::NodeId,
        registry::{LocalRefError, Named},
        wire,
    },
    sync::lock,
};
use derive_more::Display;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::{
    any,
    collections::HashMap,
    fmt::{self, Debug, Formatter},
    marker::PhantomData,
    net::SocketAddr,
    sync::{
        Mutex,
        atomic::{AtomicU64, Ordering},
    },
};
use thiserror::Error;
use tokio::sync::oneshot;
use tracing::warn;

/// The name an actor is registered under for discovery, typed by the actor's message type: the
/// type travels with the name, so a [lookup] naming the wrong one is refused rather than resolved
/// into a reference which drops every message it is told.
///
/// The type is compared by its name as both nodes' compilers spell it, which assumes they are
/// built from the same source. That is the practical case for the nodes of one system and the
/// same assumption the wire format already makes.
pub struct Key<M> {
    name: String,
    message: PhantomData<fn() -> M>,
}

impl<M> Key<M> {
    /// A key under the given name.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            message: PhantomData,
        }
    }

    fn wire(&self) -> WireKey {
        WireKey::new::<M>(self.name.clone())
    }
}

impl<M> Debug for Key<M> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("Key")
            .field("name", &self.name)
            .field("message", &any::type_name::<M>())
            .finish()
    }
}

// A derived `Clone` would needlessly require `M: Clone`.
impl<M> Clone for Key<M> {
    fn clone(&self) -> Self {
        Self {
            name: self.name.clone(),
            message: PhantomData,
        }
    }
}

/// An actor which cannot be registered for discovery.
#[derive(Debug, Error)]
pub enum RegisterError {
    /// The remoting endpoint has not been started, see
    /// [start_endpoint](crate::cluster::start_endpoint).
    #[error("remoting endpoint not started")]
    EndpointNotStarted,

    /// The reference names an actor on another node; only actors of this node can be registered
    /// here.
    #[error("reference names an actor on another node")]
    NotLocal,
}

/// A key which cannot be resolved.
#[derive(Debug, Error)]
pub enum LookupError {
    /// The remoting endpoint has not been started, see
    /// [start_endpoint](crate::cluster::start_endpoint).
    #[error("remoting endpoint not started")]
    EndpointNotStarted,

    /// No actor could be resolved under this key: either the node answered that it has nothing
    /// registered, or the actor it named terminated before the reference was resolved. During
    /// bootstrap the former is the ordinary answer of a node whose actor is not registered *yet*,
    /// hence worth retrying.
    #[error("no actor registered under this key")]
    NotFound,

    /// The node has an actor registered under this name, but for another message type.
    #[error("actor registered under this key expects another message type")]
    TypeMismatch,

    /// No Up member of the cluster advertises this address. During bootstrap this is the
    /// ordinary answer while the node there has not joined yet, hence worth retrying.
    #[error("no member at {0}")]
    NotAMember(SocketAddr),

    /// The node could not be reached, or was given up on before it answered.
    #[error("node at {0} unreachable")]
    Unreachable(SocketAddr),
}

/// Register an actor of this node under a key, so other nodes can [lookup] it by name and address
/// instead of being handed a serialized reference out of band.
///
/// More than one actor may be registered under one key; a lookup then answers with one of them.
/// The registration is dropped once the actor terminates, along with the reference binding it
/// shares with reference serialization. Registering the same actor again under the same key
/// changes nothing.
pub fn register<M>(key: &Key<M>, actor_ref: &ActorRef<M>) -> Result<(), RegisterError>
where
    M: DeserializeOwned + Send + 'static,
{
    let endpoint = endpoint::get().ok_or(RegisterError::EndpointNotStarted)?;
    if actor_ref.watcher_registry().is_none() {
        return Err(RegisterError::NotLocal);
    }

    endpoint.registry().register(key.name.clone(), actor_ref);
    Ok(())
}

/// Resolve a key at the Up member advertising the given address, messaging it like any other
/// frame would. The resolved reference names the incarnation which answered, so it stops working
/// when that node is replaced, exactly like one which travelled inside a message.
///
/// There is no timeout: a lookup towards a node which is up but silent waits. Wrap it in
/// [timeout](tokio::time::timeout) and retry [LookupError::NotAMember], for a node which has not
/// joined yet, and [LookupError::NotFound], for one which has not registered its actor yet, to
/// bootstrap.
pub async fn lookup<M>(key: &Key<M>, addr: SocketAddr) -> Result<ActorRef<M>, LookupError>
where
    M: Serialize + Send + 'static,
{
    let endpoint = endpoint::get().ok_or(LookupError::EndpointNotStarted)?;
    let Some(node) = endpoint.membership().up_member_at(addr) else {
        return Err(LookupError::NotAMember(addr));
    };

    let key = key.wire();
    let mut pending = endpoint.pending_lookups().add(node, key.clone());

    if let Err(error) = endpoint.send(
        node,
        Frame::Lookup {
            nonce: pending.nonce(),
            key,
        },
    ) {
        warn!(peer_addr = %addr, %error, "cannot send lookup");
        return Err(LookupError::Unreachable(addr));
    }

    let (answerer, result) = pending
        .receive()
        .await
        .map_err(|_| LookupError::Unreachable(addr))?;

    match result {
        LookupResult::Found { id } => {
            wire::resolve(endpoint, answerer, id).map_err(|error| match error {
                LocalRefError::MessageType => LookupError::TypeMismatch,
                LocalRefError::Unbound => LookupError::NotFound,
            })
        }

        LookupResult::NotFound => Err(LookupError::NotFound),

        LookupResult::TypeMismatch => Err(LookupError::TypeMismatch),
    }
}

/// The type tag is always derived from the type, never passed in, so it cannot swap with the name.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct WireKey {
    name: String,
    type_tag: String,
}

impl WireKey {
    pub(crate) fn new<M>(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            type_tag: any::type_name::<M>().to_string(),
        }
    }

    pub(crate) fn name(&self) -> &str {
        &self.name
    }

    pub(crate) fn type_tag(&self) -> &str {
        &self.type_tag
    }
}

/// Answers match by nonce, not by key, so two lookups of one key cannot resolve each other.
#[derive(Debug, Display, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub(crate) struct Nonce(u64);

impl Nonce {
    pub(crate) fn mint(next: &AtomicU64) -> Self {
        Self(next.fetch_add(1, Ordering::Relaxed))
    }

    pub(crate) fn as_u64(self) -> u64 {
        self.0
    }

    #[cfg(test)]
    pub(crate) fn first() -> Self {
        Self(0)
    }
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum LookupResult {
    Found { id: ActorId },
    NotFound,
    TypeMismatch,
}

/// A reconnect re-sends these, a node given up on fails them; else they wait for nobody.
pub(crate) struct PendingLookups {
    next_nonce: AtomicU64,
    pending: Mutex<HashMap<Nonce, Pending>>,
}

impl PendingLookups {
    pub(crate) fn new() -> Self {
        Self {
            next_nonce: AtomicU64::new(0),
            pending: Mutex::new(HashMap::new()),
        }
    }

    pub(crate) fn frames(&self, peer: NodeId) -> Vec<Frame<'static>> {
        lock(&self.pending)
            .iter()
            .filter(|(_, pending)| pending.peer == peer)
            .map(|(nonce, pending)| Frame::Lookup {
                nonce: *nonce,
                key: pending.key.clone(),
            })
            .collect()
    }

    pub(crate) fn fail_addr(&self, addr: SocketAddr) {
        lock(&self.pending).retain(|_, pending| pending.peer.addr() != addr);
    }

    pub(crate) fn fail_fenced(&self, fence: NodeId) {
        lock(&self.pending).retain(|_, pending| !fence.covers(pending.peer));
    }

    fn add(&self, peer: NodeId, key: WireKey) -> PendingLookup<'_> {
        let nonce = Nonce::mint(&self.next_nonce);
        let (result_tx, result_rx) = oneshot::channel();

        lock(&self.pending).insert(
            nonce,
            Pending {
                peer,
                key,
                result_tx,
            },
        );
        PendingLookup {
            table: self,
            nonce,
            result_rx,
        }
    }

    fn take_from(&self, nonce: Nonce, peer: NodeId) -> Option<Pending> {
        let mut pending = lock(&self.pending);

        pending
            .get(&nonce)
            .is_some_and(|pending| pending.peer == peer)
            .then(|| pending.remove(&nonce))
            .flatten()
    }
}

pub(crate) fn on_lookup(endpoint: &EndpointInner, peer: NodeId, nonce: Nonce, key: WireKey) {
    let result = resolve_key(endpoint, &key);

    if let Err(error) = endpoint.send(peer, Frame::LookupReply { nonce, result }) {
        warn!(%peer, name = key.name(), %error, "cannot answer lookup");
    }
}

/// Only the member a lookup was sent to may answer it: a nonce is unique to this node only.
pub(crate) fn on_lookup_reply(
    endpoint: &EndpointInner,
    peer: NodeId,
    nonce: Nonce,
    result: LookupResult,
) {
    match endpoint.pending_lookups().take_from(nonce, peer) {
        Some(pending) => {
            let _ = pending.result_tx.send((peer, result));
        }

        None => warn!(
            %peer,
            %nonce,
            "dropping a lookup reply this peer was not asked for"
        ),
    }
}

struct Pending {
    peer: NodeId,
    key: WireKey,
    result_tx: oneshot::Sender<(NodeId, LookupResult)>,
}

struct PendingLookup<'a> {
    table: &'a PendingLookups,
    nonce: Nonce,
    result_rx: oneshot::Receiver<(NodeId, LookupResult)>,
}

impl PendingLookup<'_> {
    fn nonce(&self) -> Nonce {
        self.nonce
    }

    async fn receive(&mut self) -> Result<(NodeId, LookupResult), oneshot::error::RecvError> {
        (&mut self.result_rx).await
    }
}

impl Drop for PendingLookup<'_> {
    fn drop(&mut self) {
        lock(&self.table.pending).remove(&self.nonce);
    }
}

fn resolve_key(endpoint: &EndpointInner, key: &WireKey) -> LookupResult {
    match endpoint.registry().named(key) {
        Named::Found(id) => LookupResult::Found { id },

        Named::NotFound => LookupResult::NotFound,

        Named::TypeMismatch => LookupResult::TypeMismatch,
    }
}

#[cfg(test)]
mod tests {
    use crate::cluster::{
        discovery::{PendingLookups, WireKey},
        node::NodeId,
    };

    /// A cancelled lookup leaves nothing for a reconnect to replay.
    #[test]
    fn dropping_a_pending_lookup_removes_its_reconnect_frame() {
        let addr = "127.0.0.1:1234".parse().expect("valid address");
        let peer = NodeId::new(addr);
        let pending = PendingLookups::new();
        let lookup = pending.add(peer, WireKey::new::<u64>("answer"));

        assert_eq!(pending.frames(peer).len(), 1);

        drop(lookup);

        assert!(pending.frames(peer).is_empty());
    }

    /// A node fence fails only lookups sent to the covered incarnation; a lookup already sent to
    /// its successor at the same address remains pending.
    #[test]
    fn fenced_failure_preserves_a_successor_lookup() {
        let addr = "127.0.0.1:1234".parse().expect("valid address");
        let old = NodeId::new(addr);
        let successor = NodeId::new(addr);
        assert!(old.incarnation() < successor.incarnation());
        let pending = PendingLookups::new();
        let mut old_lookup = pending.add(old, WireKey::new::<u64>("old"));
        let successor_lookup = pending.add(successor, WireKey::new::<u64>("successor"));

        pending.fail_fenced(old);

        assert!(matches!(
            old_lookup.result_rx.try_recv(),
            Err(tokio::sync::oneshot::error::TryRecvError::Closed)
        ));
        assert!(pending.frames(old).is_empty());
        assert_eq!(pending.frames(successor).len(), 1);
        drop(successor_lookup);
    }
}
