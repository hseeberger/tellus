use crate::{
    ActorId, Incoming, MailboxCapacity,
    quota::{CountedSendError, CountedSender, Full, Quota},
    watch::{ActorTerminated, TerminatedHandler, WatcherRegistry, Watchers},
};
use flume::Receiver;
use std::sync::Arc;
use thiserror::Error;

pub(crate) struct MailboxHandle<M> {
    incoming_tx: CountedSender<Incoming<M>>,
    watcher_registry: WatcherRegistry,
}

impl<M> MailboxHandle<M> {
    pub(crate) fn try_send_message(&self, message: M) -> Result<(), SendError> {
        self.incoming_tx
            .try_send_counted(Incoming::Message(message))?;

        Ok(())
    }

    pub(crate) fn watcher_registry(&self) -> &WatcherRegistry {
        &self.watcher_registry
    }

    pub(crate) fn terminated_handler(&self) -> Arc<dyn TerminatedHandler>
    where
        M: Send + 'static,
    {
        Arc::new(self.incoming_tx.clone())
    }
}

// A derived `Clone` would needlessly require `M: Clone`.
impl<M> Clone for MailboxHandle<M> {
    fn clone(&self) -> Self {
        Self {
            incoming_tx: self.incoming_tx.clone(),
            watcher_registry: self.watcher_registry.clone(),
        }
    }
}

pub(crate) struct Mailbox<M> {
    incoming_rx: Receiver<Incoming<M>>,
    watchers: Watchers,
    quota: Quota,
}

impl<M> Mailbox<M> {
    /// One consumer per mailbox: `&mut self` enforces it.
    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    pub(crate) async fn recv(&mut self) -> Option<Incoming<M>> {
        let incoming = self.incoming_rx.recv_async().await.ok()?;
        if matches!(incoming, Incoming::Message(_)) {
            self.quota.unreserve();
        }
        Some(incoming)
    }

    pub(crate) fn split(self) -> (Receiver<Incoming<M>>, Watchers) {
        (self.incoming_rx, self.watchers)
    }
}

#[derive(Debug, Error)]
pub(crate) enum SendError {
    #[error("mailbox full")]
    MailboxFull(#[from] Full),

    #[error(transparent)]
    ActorTerminated(#[from] ActorTerminated),
}

impl From<CountedSendError> for SendError {
    fn from(error: CountedSendError) -> Self {
        match error {
            CountedSendError::Full(full) => Self::MailboxFull(full),
            CountedSendError::Disconnected(_) => Self::ActorTerminated(ActorTerminated),
        }
    }
}

impl<M> TerminatedHandler for CountedSender<Incoming<M>>
where
    M: Send + 'static,
{
    fn handle_terminated(&self, actor_id: ActorId) -> Result<(), ActorTerminated> {
        self.try_send_uncounted(Incoming::Terminated(actor_id))
            .map_err(|_| ActorTerminated)
    }
}

pub(crate) fn make_mailbox<M>(mailbox_capacity: MailboxCapacity) -> (MailboxHandle<M>, Mailbox<M>) {
    let (incoming_tx, incoming_rx) = flume::unbounded();

    let quota = match mailbox_capacity {
        MailboxCapacity::Unbounded => Quota::unbounded(),
        MailboxCapacity::Bounded(capacity) => Quota::bounded(capacity),
    };
    let (watcher_registry, watchers) = WatcherRegistry::new();

    let mailbox_handle = MailboxHandle {
        incoming_tx: CountedSender::new(incoming_tx, quota.clone()),
        watcher_registry,
    };
    let mailbox = Mailbox {
        incoming_rx,
        watchers,
        quota,
    };

    (mailbox_handle, mailbox)
}

#[cfg(test)]
mod tests {
    use crate::{
        ActorId, Incoming, MailboxCapacity,
        mailbox::{SendError, make_mailbox},
        watch::Watcher,
    };
    use std::{num::NonZeroUsize, time::Duration};
    use tokio::time::timeout;

    #[test]
    fn unbounded_never_fills() {
        let (mailbox_handle, _mailbox) = make_mailbox::<()>(MailboxCapacity::Unbounded);

        for _ in 0..1_000 {
            assert!(mailbox_handle.try_send_message(()).is_ok());
        }
    }

    #[test]
    fn bounded_rejects_beyond_capacity() {
        let (mailbox_handle, _mailbox) =
            make_mailbox::<()>(MailboxCapacity::Bounded(NonZeroUsize::MIN));

        assert!(mailbox_handle.try_send_message(()).is_ok());
        assert!(matches!(
            mailbox_handle.try_send_message(()),
            Err(SendError::MailboxFull(_))
        ));
    }

    /// A bounded mailbox which is full when the actor terminates reports the termination, not the
    /// full mailbox, as the reason for a rejected send.
    #[test]
    fn terminated_overrides_full() {
        let (mailbox_handle, mailbox) =
            make_mailbox::<()>(MailboxCapacity::Bounded(NonZeroUsize::MIN));

        assert!(mailbox_handle.try_send_message(()).is_ok());
        drop(mailbox);

        assert!(matches!(
            mailbox_handle.try_send_message(()),
            Err(SendError::ActorTerminated(_))
        ));
    }

    /// Splitting the mailbox already fails sends as terminated while registration stays open, so
    /// termination can reject senders early yet signal its watchers last.
    #[test]
    fn splitting_disconnects_senders_but_keeps_registration_open() {
        let (mailbox_handle, mailbox) =
            make_mailbox::<()>(MailboxCapacity::Bounded(NonZeroUsize::MIN));
        assert!(mailbox_handle.try_send_message(()).is_ok());

        let (incoming_rx, watchers) = mailbox.split();
        drop(incoming_rx);

        assert!(matches!(
            mailbox_handle.try_send_message(()),
            Err(SendError::ActorTerminated(_))
        ));

        let watcher = Watcher::new(ActorId::new(), mailbox_handle.terminated_handler());
        assert!(mailbox_handle.watcher_registry().add(watcher).is_ok());
        assert_eq!(watchers.close().len(), 1);
    }

    #[tokio::test]
    async fn receiving_a_message_frees_capacity() {
        let (mailbox_handle, mut mailbox) =
            make_mailbox::<()>(MailboxCapacity::Bounded(NonZeroUsize::MIN));

        assert!(mailbox_handle.try_send_message(()).is_ok());
        assert!(mailbox.recv().await.is_some());
        assert!(mailbox_handle.try_send_message(()).is_ok());
    }

    #[tokio::test]
    async fn recv_drains_queued_messages_before_ending() {
        let (mailbox_handle, mut mailbox) = make_mailbox::<u32>(MailboxCapacity::Unbounded);

        assert!(mailbox_handle.try_send_message(1).is_ok());
        assert!(mailbox_handle.try_send_message(2).is_ok());
        drop(mailbox_handle);

        assert!(matches!(mailbox.recv().await, Some(Incoming::Message(1))));
        assert!(matches!(mailbox.recv().await, Some(Incoming::Message(2))));
        assert!(mailbox.recv().await.is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn recv_ends_only_once_every_handle_is_dropped() {
        let (mailbox_handle, mut mailbox) = make_mailbox::<u32>(MailboxCapacity::Unbounded);
        let clone = mailbox_handle.clone();
        drop(mailbox_handle);

        assert!(
            timeout(Duration::from_secs(5), mailbox.recv())
                .await
                .is_err()
        );

        drop(clone);
        assert!(mailbox.recv().await.is_none());
    }

    #[test]
    fn clones_share_one_capacity() {
        let (mailbox_handle, _mailbox) =
            make_mailbox::<()>(MailboxCapacity::Bounded(NonZeroUsize::MIN));
        let clone = mailbox_handle.clone();

        assert!(mailbox_handle.try_send_message(()).is_ok());
        assert!(matches!(
            clone.try_send_message(()),
            Err(SendError::MailboxFull(_))
        ));
    }

    /// Cloning a handle shares the watcher registration as well as the capacity: a watcher
    /// registered through a clone is taken by the receiving half, hence signaled at termination.
    #[test]
    fn clones_share_one_watcher_registry() {
        let (mailbox_handle, mailbox) = make_mailbox::<()>(MailboxCapacity::Unbounded);
        let clone = mailbox_handle.clone();

        let watcher = Watcher::new(ActorId::new(), mailbox_handle.terminated_handler());
        assert!(clone.watcher_registry().add(watcher).is_ok());

        assert_eq!(mailbox.split().1.close().len(), 1);
    }

    /// A send to a terminated actor reports the termination rather than a full mailbox, also when
    /// capacity is still available: that is the reserve-then-send path, whereas
    /// `terminated_overrides_full` covers the one where the quota is already exhausted.
    #[test]
    fn terminated_with_spare_capacity() {
        let capacity = NonZeroUsize::new(2).expect("2 is not zero");
        let (mailbox_handle, mailbox) = make_mailbox::<()>(MailboxCapacity::Bounded(capacity));

        drop(mailbox);

        for _ in 0..2 * capacity.get() {
            assert!(matches!(
                mailbox_handle.try_send_message(()),
                Err(SendError::ActorTerminated(_))
            ));
        }
    }

    #[tokio::test]
    async fn terminated_signals_ignore_capacity() {
        let (mailbox_handle, mut mailbox) =
            make_mailbox::<()>(MailboxCapacity::Bounded(NonZeroUsize::MIN));
        let terminated_handler = mailbox_handle.terminated_handler();

        assert!(mailbox_handle.try_send_message(()).is_ok());
        assert!(terminated_handler.handle_terminated(ActorId::new()).is_ok());

        assert!(matches!(mailbox.recv().await, Some(Incoming::Message(_))));
        assert!(matches!(
            mailbox.recv().await,
            Some(Incoming::Terminated(_))
        ));

        assert!(mailbox_handle.try_send_message(()).is_ok());
        assert!(matches!(
            mailbox_handle.try_send_message(()),
            Err(SendError::MailboxFull(_))
        ));
    }

    #[test]
    fn watching_ignores_capacity() {
        let (mailbox_handle, _mailbox) =
            make_mailbox::<()>(MailboxCapacity::Bounded(NonZeroUsize::MIN));

        assert!(mailbox_handle.try_send_message(()).is_ok());

        let watcher = Watcher::new(ActorId::new(), mailbox_handle.terminated_handler());
        assert!(mailbox_handle.watcher_registry().add(watcher).is_ok());
    }

    /// A watcher delivers the terminated signal into the watching actor's mailbox and reports an
    /// error once that mailbox is gone, i.e. the watching actor itself has terminated.
    #[tokio::test]
    async fn watcher_sends_terminated_into_watching_mailbox() {
        let (mailbox_handle, mut mailbox) = make_mailbox::<()>(MailboxCapacity::Unbounded);
        let watcher = Watcher::new(ActorId::new(), mailbox_handle.terminated_handler());

        let actor_id = ActorId::new();
        assert!(watcher.handle_terminated(actor_id).is_ok());
        assert!(matches!(
            mailbox.recv().await,
            Some(Incoming::Terminated(other)) if other == actor_id
        ));

        drop(mailbox);
        assert!(watcher.handle_terminated(actor_id).is_err());
    }
}
