//! Publish-subscribe over the [receptionist](crate::cluster::receptionist): a [Topic] is a [Key]
//! under another name, [subscribe] registers a local actor under it and [publish] tells every
//! subscriber the receptionist resolves, see docs/cluster.md.

use crate::{
    ActorRef,
    cluster::{
        discovery::{Key, RegisterError, register},
        endpoint::{self, EndpointInner},
        receptionist::{ReceptionistError, lookup_at},
    },
};
use serde::{Serialize, de::DeserializeOwned};
use std::fmt::{self, Debug, Formatter};

/// A topic, i.e. a [Key] under another name: [subscribe] registers under it, [publish] resolves
/// it. Topic names and the names of [register] share one namespace, so a topic and a discovery key
/// of the same name and message type name the same registrations.
pub struct Topic<M> {
    key: Key<M>,
}

impl<M> Topic<M> {
    /// A topic under the given name.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            key: Key::new(name),
        }
    }

    /// The key the topic's subscribers are registered under, for
    /// [receptionist::lookup](crate::cluster::receptionist::lookup) and
    /// [receptionist::subscribe](crate::cluster::receptionist::subscribe) on the subscriber set.
    pub fn key(&self) -> &Key<M> {
        &self.key
    }
}

impl<M> Debug for Topic<M> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("Topic").field("key", &self.key).finish()
    }
}

// A derived `Clone` would needlessly require `M: Clone`.
impl<M> Clone for Topic<M> {
    fn clone(&self) -> Self {
        Self {
            key: self.key.clone(),
        }
    }
}

/// Subscribe an actor of this node to a topic by registering it under the topic's key: a
/// member's [publish] tells it once the registration has replicated there, not before.
///
/// There is no unsubscribe: a subscription lives as long as its actor, exactly like a
/// [registration](crate::cluster::register). An actor which leaves a topic while running
/// subscribes a child it can stop instead.
///
/// # Errors
/// Fails if the endpoint is not started, if the reference names an actor on another node, or if
/// the registration does not fit one frame.
pub fn subscribe<M>(topic: &Topic<M>, actor_ref: &ActorRef<M>) -> Result<(), RegisterError>
where
    M: DeserializeOwned + Send + 'static,
{
    register(topic.key(), actor_ref)
}

/// Tell the message to every subscriber of the topic the receptionist currently resolves, this
/// node's own subscribers included, and answer how many were told.
///
/// Each subscriber is told separately, so [ActorRef::tell]'s contract holds per subscriber: the
/// answer counts the tells attempted, not the messages delivered. A subscriber whose registration
/// has not reached this node yet is not told. [receptionist::settled] says whether every Up
/// member has delivered a first snapshot at all; a later registration still in transit is
/// invisible to it and to this answer alike.
///
/// # Errors
/// Fails if the endpoint is not started, or if the name is registered on an Up member only for
/// other message types.
///
/// [ActorRef::tell]: crate::ActorRef::tell
/// [receptionist::settled]: crate::cluster::receptionist::settled
pub fn publish<M>(topic: &Topic<M>, message: M) -> Result<usize, ReceptionistError>
where
    M: Clone + Serialize + Send + 'static,
{
    let endpoint = endpoint::get().ok_or(ReceptionistError::EndpointNotStarted)?;
    publish_at(endpoint, topic, message)
}

fn publish_at<M>(
    endpoint: &EndpointInner,
    topic: &Topic<M>,
    message: M,
) -> Result<usize, ReceptionistError>
where
    M: Clone + Serialize + Send + 'static,
{
    let subscribers = lookup_at(endpoint, topic.key())?;
    let count = subscribers.len();
    let mut subscribers = subscribers.into_iter();
    let last = subscribers.next_back();
    for subscriber in subscribers {
        subscriber.tell(message.clone());
    }
    if let Some(last) = last {
        last.tell(message);
    }
    Ok(count)
}

#[cfg(test)]
mod tests {
    use crate::{
        ActorId, Incoming, MailboxCapacity,
        actor_ref::SelfRef,
        cluster::{
            endpoint::{EndpointConfig, EndpointInner},
            pubsub::{Topic, publish_at},
            receptionist::ReceptionistError,
        },
        mailbox::Mailbox,
    };
    use serde::de::DeserializeOwned;
    use std::net::SocketAddr;

    fn addr(port: u16) -> SocketAddr {
        format!("127.0.0.1:{port}").parse().expect("valid address")
    }

    fn endpoint() -> &'static EndpointInner {
        EndpointInner::for_tests(EndpointConfig::new(addr(1))).0
    }

    fn subscriber<M>(endpoint: &EndpointInner, name: &str) -> (ActorId, Mailbox<M>)
    where
        M: DeserializeOwned + Send + 'static,
    {
        let id = ActorId::new();
        let (self_ref, mailbox) = SelfRef::<M>::new(id, MailboxCapacity::Unbounded);
        endpoint
            .registry()
            .register(name.to_string(), self_ref.actor_ref());
        replicate(endpoint);
        (id, mailbox)
    }

    fn replicate(endpoint: &EndpointInner) {
        let registrations = endpoint.registry().registrations();
        endpoint.receptionist().apply(
            endpoint.node(),
            registrations.version,
            registrations.entries,
            true,
        );
    }

    fn received<M>(mailbox: Mailbox<M>) -> Vec<M> {
        mailbox
            .split()
            .0
            .drain()
            .filter_map(|incoming| match incoming {
                Incoming::Message(message) => Some(message),
                Incoming::Terminated(_) => None,
            })
            .collect()
    }

    /// Every subscriber is told once, and the count is what the publisher resolved.
    #[test]
    fn a_publish_tells_every_subscriber_once() {
        let endpoint = endpoint();
        let topic = Topic::<u64>::new("notes");
        let (_, first) = subscriber::<u64>(endpoint, "notes");
        let (_, second) = subscriber::<u64>(endpoint, "notes");

        assert_eq!(publish_at(endpoint, &topic, 42).expect("resolves"), 2);

        assert_eq!(received(first), vec![42]);
        assert_eq!(received(second), vec![42]);
    }

    /// A topic nobody subscribed to is not an error: it resolves to nobody.
    #[test]
    fn a_topic_without_subscribers_tells_nobody() {
        let endpoint = endpoint();
        let topic = Topic::<u64>::new("notes");

        assert_eq!(publish_at(endpoint, &topic, 42).expect("resolves"), 0);
    }

    /// A topic of another message type is refused rather than answered as an empty set, and the
    /// subscribers of the matching type are told whatever else the name holds.
    #[test]
    fn a_topic_of_another_message_type_is_refused() {
        let endpoint = endpoint();
        let (_, mailbox) = subscriber::<u64>(endpoint, "notes");

        assert!(matches!(
            publish_at(endpoint, &Topic::<String>::new("notes"), "note".to_string()),
            Err(ReceptionistError::TypeMismatch)
        ));
        assert_eq!(
            publish_at(endpoint, &Topic::<u64>::new("notes"), 42).expect("resolves"),
            1
        );
        assert_eq!(received(mailbox), vec![42]);
    }

    /// A subscription ends with its actor: the registry evicts the name on termination and the
    /// next replication takes it out of the set a publish resolves.
    #[test]
    fn a_terminated_subscriber_is_not_told() {
        let endpoint = endpoint();
        let topic = Topic::<u64>::new("notes");
        let (_, first) = subscriber::<u64>(endpoint, "notes");
        let (leaving, second) = subscriber::<u64>(endpoint, "notes");
        // Take the receiver before the eviction: one taken after it starts at the evicted
        // version and waits for a change which never comes.
        let mut changes = endpoint.registry().changes();
        let version = *changes.borrow_and_update();

        for watcher in second.split().1.close() {
            watcher.handle_terminated(leaving).expect("watcher alive");
        }
        assert!(*changes.borrow_and_update() > version);
        replicate(endpoint);

        assert_eq!(publish_at(endpoint, &topic, 42).expect("resolves"), 1);
        assert_eq!(received(first), vec![42]);
    }
}
