use crate::{ActorId, sync::lock};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};
use thiserror::Error;

/// Shared by the sending mailbox half and the watching contexts.
#[derive(Clone)]
pub(crate) struct WatcherRegistry(Arc<Mutex<Option<HashMap<ActorId, Watcher>>>>);

impl WatcherRegistry {
    pub(crate) fn new() -> (Self, Watchers) {
        let registry = Self(Arc::new(Mutex::new(Some(HashMap::new()))));

        (registry.clone(), Watchers(registry))
    }

    pub(crate) fn add(&self, watcher: Watcher) -> Result<(), ActorTerminated> {
        let mut registry = lock(&self.0);
        let watchers = registry.as_mut().ok_or(ActorTerminated)?;
        watchers.entry(watcher.watcher_id()).or_insert(watcher);

        Ok(())
    }

    pub(crate) fn remove(&self, watcher_id: ActorId) {
        if let Some(watchers) = lock(&self.0).as_mut() {
            watchers.remove(&watcher_id);
        }
    }

    fn take(&self) -> Vec<Watcher> {
        lock(&self.0)
            .take()
            .map(|watchers| watchers.into_values().collect())
            .unwrap_or_default()
    }
}

pub(crate) struct Watchers(WatcherRegistry);

impl Watchers {
    pub(crate) fn close(self) -> Vec<Watcher> {
        self.0.take()
    }
}

pub(crate) struct Watcher {
    watcher_id: ActorId,
    terminated_handler: Arc<dyn TerminatedHandler>,
}

impl Watcher {
    pub(crate) fn new(watcher_id: ActorId, terminated_handler: Arc<dyn TerminatedHandler>) -> Self {
        Self {
            watcher_id,
            terminated_handler,
        }
    }

    pub(crate) fn watcher_id(&self) -> ActorId {
        self.watcher_id
    }

    pub(crate) fn handle_terminated(&self, actor_id: ActorId) -> Result<(), ActorTerminated> {
        self.terminated_handler.handle_terminated(actor_id)
    }
}

pub(crate) trait TerminatedHandler
where
    Self: Send + Sync,
{
    fn handle_terminated(&self, actor_id: ActorId) -> Result<(), ActorTerminated>;
}

#[derive(Debug, Error)]
#[error("actor terminated")]
pub(crate) struct ActorTerminated;

#[cfg(test)]
mod tests {
    use crate::{
        ActorId,
        watch::{ActorTerminated, TerminatedHandler, Watcher, WatcherRegistry},
    };
    use std::sync::Arc;

    /// Registering the same watcher twice signals once: a terminated signal only names the
    /// terminated actor, hence a second one would carry nothing.
    #[test]
    fn adding_a_watcher_twice_registers_once() {
        let (registry, watchers) = WatcherRegistry::new();

        let watcher_id = ActorId::new();
        for _ in 0..3 {
            assert!(registry.add(watcher(watcher_id)).is_ok());
        }

        assert_eq!(watchers.close().len(), 1);
    }

    /// Removing a watcher deregisters it, so no terminated signal is sent to it and no reference
    /// to it is held anymore.
    #[test]
    fn removing_a_watcher_deregisters_it() {
        let (registry, watchers) = WatcherRegistry::new();

        let watcher_id = ActorId::new();
        assert!(registry.add(watcher(watcher_id)).is_ok());
        registry.remove(watcher_id);

        assert!(watchers.close().is_empty());
    }

    /// Removing after registration has been closed has no effect, in particular it must not
    /// reopen registration.
    #[test]
    fn removing_after_take_is_a_noop() {
        let (registry, watchers) = WatcherRegistry::new();

        let watcher_id = ActorId::new();
        assert!(registry.add(watcher(watcher_id)).is_ok());
        assert_eq!(watchers.close().len(), 1);

        registry.remove(watcher_id);

        assert!(registry.add(watcher(watcher_id)).is_err());
    }

    /// Taking the watchers closes registration, hence a watcher racing with termination either is
    /// taken or learns that the actor has terminated, but is never lost.
    #[test]
    fn taking_watchers_closes_registration() {
        let (registry, watchers) = WatcherRegistry::new();

        assert!(registry.add(watcher(ActorId::new())).is_ok());
        assert_eq!(watchers.close().len(), 1);

        assert!(registry.add(watcher(ActorId::new())).is_err());
    }

    fn watcher(watcher_id: ActorId) -> Watcher {
        Watcher::new(watcher_id, Arc::new(Discard))
    }

    struct Discard;

    impl TerminatedHandler for Discard {
        fn handle_terminated(&self, _actor_id: ActorId) -> Result<(), ActorTerminated> {
            Ok(())
        }
    }
}
