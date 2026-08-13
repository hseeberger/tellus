use crate::{
    ActorRef,
    cluster::{
        endpoint,
        receptionist::{self, WireRegistration},
    },
};
use derive_more::Display;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::{
    any,
    fmt::{self, Debug, Formatter},
    marker::PhantomData,
    sync::atomic::{AtomicU64, Ordering},
};
use thiserror::Error;

/// The name an actor is registered under for discovery, typed by the actor's message type: the
/// type travels with the name, so a [lookup](receptionist::lookup_at) naming the wrong one is
/// refused rather than resolved
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

    pub(crate) fn wire(&self) -> WireKey {
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

    /// The key does not fit one frame of `max_frame_size` together with the actor's ID, so it
    /// could never be replicated to the other members; a shorter name is the remedy.
    #[error("registration of {len} bytes exceeds the {max} bytes a frame carries")]
    Oversize {
        /// The bytes the registration takes inside a frame.
        len: usize,

        /// The configured `max_frame_size`.
        max: usize,
    },

    /// This node already holds the most registrations every other member accepts from one
    /// node; a registration ends with its actor.
    #[error("this node already holds {max} registrations")]
    TooMany {
        /// The most registrations one node may hold.
        max: usize,
    },
}

/// Register an actor of this node under a key, so other nodes can
/// [look it up](receptionist::lookup_at) by name and address
/// instead of being handed a serialized reference out of band.
///
/// More than one actor may be registered under one key; a lookup then answers with all of them.
/// The registration is dropped once the actor terminates, along with the reference binding it
/// shares with reference serialization. Registering the same actor again under the same key
/// changes nothing.
///
/// # Errors
/// Fails if the endpoint is not started, if the reference names an actor on another node, if the
/// registration does not fit one frame, or if this node already holds the most registrations one
/// node may.
pub fn register<M>(key: &Key<M>, actor_ref: &ActorRef<M>) -> Result<(), RegisterError>
where
    M: DeserializeOwned + Send + 'static,
{
    let endpoint = endpoint::get().ok_or(RegisterError::EndpointNotStarted)?;
    if actor_ref.watcher_registry().is_none() {
        return Err(RegisterError::NotLocal);
    }
    let len = receptionist::framed_len(&WireRegistration {
        key: key.wire(),
        id: actor_ref.actor_id(),
    });
    let max = endpoint.config().max_frame_size.get();
    if len > max {
        return Err(RegisterError::Oversize { len, max });
    }

    endpoint
        .register_own(key.name.clone(), actor_ref)
        .map_err(|_| RegisterError::TooMany {
            max: receptionist::MAX_REGISTRATIONS,
        })
}

/// The type tag is always derived from the type, never passed in, so it cannot swap with the name.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
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

    /// Only for a tag `new` produced and the registry kept, never for one made up.
    pub(crate) fn from_parts(name: String, type_tag: String) -> Self {
        Self { name, type_tag }
    }

    pub(crate) fn into_parts(self) -> (String, String) {
        (self.name, self.type_tag)
    }
}

/// Answers match by nonce, so two requests from one asker cannot resolve each other.
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
