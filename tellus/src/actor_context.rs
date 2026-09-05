#[cfg(feature = "cluster")]
use crate::cluster;
use crate::{
    Actor, ActorConfig, ActorId, ActorRef, Control, Incoming, SupervisionStrategy,
    actor_ref::{SelfRef, WatchTarget},
    mailbox::Mailbox,
    watch::WatcherRegistry,
};
use derive_more::Debug;
use std::{
    any::Any,
    cell::RefCell,
    collections::HashMap,
    error::Error,
    fmt::{self, Display, Formatter},
    future::Future,
    mem,
    panic::{AssertUnwindSafe, catch_unwind},
    pin::{Pin, pin},
    time::Duration,
};
use tokio::{
    select,
    sync::watch,
    task,
    time::{Instant, sleep},
};
use tracing::{debug, error};

pub(crate) const STATE_FAILED_TO_DROP: &str = "actor state failed to drop";

/// Contextual methods for a given actor, provided to [Actor::init] and [Actor::receive].
#[derive(Debug)]
pub struct ActorContext<M> {
    self_ref: SelfRef<M>,

    #[debug(skip)]
    stopping_tx: watch::Sender<()>,

    #[debug(skip)]
    stopping_rx: watch::Receiver<()>,

    #[debug(skip)]
    watched: RefCell<HashMap<ActorId, Watched>>,
}

impl<M> ActorContext<M> {
    /// The reference for the actor itself.
    pub fn self_ref(&self) -> &ActorRef<M> {
        self.self_ref.actor_ref()
    }

    /// Spawn a child actor with the given [Actor], using the default [ActorConfig].
    ///
    /// # Panics
    /// Panics if called outside of a Tokio runtime.
    pub fn spawn<A>(&self, actor: A) -> ActorRef<A::Message>
    where
        A: Actor + Send + 'static,
        A::Message: Send + 'static,
        A::State: Send + 'static,
    {
        self.spawn_with_config(actor, ActorConfig::default())
    }

    /// Spawn a child actor with the given [Actor] and [ActorConfig].
    ///
    /// # Panics
    /// Panics if called outside of a Tokio runtime.
    pub fn spawn_with_config<A>(&self, actor: A, config: ActorConfig) -> ActorRef<A::Message>
    where
        A: Actor + Send + 'static,
        A::Message: Send + 'static,
        A::State: Send + 'static,
    {
        spawn(self.stopping_rx.clone(), actor, config)
    }

    /// Watch another actor, i.e. receive an [Incoming::Terminated] signal once that actor has
    /// terminated. If it has already terminated, the signal is received right away. Watching an
    /// already watched actor again has no effect: the signal is delivered once.
    ///
    /// The signal is ordered behind all messages the other actor has delivered to this actor.
    /// Receiving it hence proves that this actor has seen every message from the other one it will
    /// ever see: each arrived before the signal or was dropped as a dead letter.
    ///
    /// With the `cluster` feature the other actor may live on another node. The ordering guarantee
    /// is kept there, too. Yet only a signal coming from that node proves that the other actor has
    /// terminated. A signal synthesized here, after that node was declared dead, does not: the
    /// other actor may still be alive. See docs/cluster.md for the exact contract.
    pub fn watch<N>(&self, other: &ActorRef<N>) {
        match other.watch_target() {
            WatchTarget::Local(registry) => {
                let registry = registry.clone();
                let registration = registry.add(self.self_ref.make_watcher());
                self.watched
                    .borrow_mut()
                    .insert(other.actor_id(), Watched::Local(registry));

                if registration.is_err() {
                    self.self_ref.send_terminated(other.actor_id());
                }
            }

            #[cfg(feature = "cluster")]
            WatchTarget::Remote(node) => {
                self.watched.borrow_mut().insert(
                    other.actor_id(),
                    Watched::Remote {
                        node,
                        target: other.actor_id(),
                    },
                );
                cluster::watch_remote(node, other.actor_id(), self.self_ref.make_watcher());
            }
        }
    }

    /// Stop watching another actor: no terminated signal for it will be received anymore, even if
    /// it has already terminated and the signal is already enqueued. Unwatching an actor which is
    /// not watched, has no effect.
    pub fn unwatch<N>(&self, other: &ActorRef<N>) {
        if let Some(watched) = self.watched.borrow_mut().remove(&other.actor_id()) {
            unwatch(self.self_ref().actor_id(), watched);
        }
    }

    #[cfg(feature = "persistence")]
    pub(crate) fn stopping_rx(&self) -> watch::Receiver<()> {
        self.stopping_rx.clone()
    }

    pub(crate) fn new(self_ref: SelfRef<M>) -> Self {
        let (stopping_tx, stopping_rx) = watch::channel(());

        Self {
            self_ref,
            stopping_tx,
            stopping_rx,
            watched: RefCell::new(HashMap::new()),
        }
    }

    pub(crate) fn take_watched_for(&mut self, other_id: ActorId) -> bool {
        self.watched.get_mut().remove(&other_id).is_some()
    }

    async fn stop_children(&mut self) {
        let (next_stopping_tx, next_stopping_rx) = watch::channel(());

        let stopping_tx = mem::replace(&mut self.stopping_tx, next_stopping_tx);
        let _ = stopping_tx.send(());

        // The assignment also drops the old `stopping_rx` without which `closed` never resolves.
        self.stopping_rx = next_stopping_rx;
        stopping_tx.closed().await;
    }

    fn take_watched(&mut self) -> HashMap<ActorId, Watched> {
        mem::take(self.watched.get_mut())
    }
}

/// `dyn Any` formats as "Any { .. }", hence the payload has to be downcast.
pub(crate) struct PanicPayload<'a>(pub(crate) &'a (dyn Any + Send));

impl Display for PanicPayload<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let payload = self
            .0
            .downcast_ref::<&'static str>()
            .copied()
            .or_else(|| self.0.downcast_ref::<String>().map(String::as_str))
            .unwrap_or("<non-string panic payload>");

        f.write_str(payload)
    }
}

// A macro, not an async fn: the run loops' message hot path must not pay for a nested state
// machine. Expanded, the awaits below are states of the run loop itself, not of a future it has
// to poll into once per message.
macro_rules! next_incoming {
    ($actor_id:expr, $mailbox:expr, $context:expr, $stopped_by_parent:expr) => {
        'next_incoming: loop {
            // Flume's receive future does not participate in Tokio's cooperative budget. Charge
            // one unit explicitly, so an actor whose mailbox never runs empty still yields to the
            // other tasks on its worker.
            tokio::task::coop::consume_budget().await;

            let incoming = tokio::select! {
                biased;

                _ = &mut $stopped_by_parent => {
                    tracing::debug!(
                        actor_id = %$actor_id,
                        "stopping, because parent stopped this actor"
                    );
                    break 'next_incoming None;
                }

                incoming = $mailbox.recv() => {
                    incoming.expect("self_ref keeps a mailbox handle alive")
                }
            };

            if let crate::Incoming::Terminated(other) = &incoming
                && !$context.take_watched_for(*other)
            {
                tracing::debug!(
                    actor_id = %$actor_id,
                    other_id = %*other,
                    "dropping terminated signal for an unwatched actor"
                );
                continue 'next_incoming;
            }

            break 'next_incoming Some(incoming);
        }
    };
}

#[cfg(feature = "persistence")]
pub(crate) use next_incoming;

pub(crate) fn spawn<A>(
    parent_stopping_rx: watch::Receiver<()>,
    actor: A,
    config: ActorConfig,
) -> ActorRef<A::Message>
where
    A: Actor + Send + 'static,
    A::Message: Send + 'static,
    A::State: Send + 'static,
{
    let actor_id = ActorId::new();
    let (self_ref, mut mailbox) = SelfRef::new(actor_id, config.mailbox_capacity);
    let actor_ref = self_ref.actor_ref().clone();

    task::spawn(async move {
        let mut context = ActorContext::new(self_ref);

        let mut rx = parent_stopping_rx.clone();
        let mut stopped_by_parent = pin!(rx.changed());

        let mut restarts = 0;

        'run: loop {
            let state = catch_and_log(actor_id, "actor failed to initialize", || {
                actor.init(&context)
            });
            let mut up_since = None;

            if let Some(mut state) = state {
                up_since = Some(Instant::now());

                loop {
                    let incoming = next_incoming!(actor_id, mailbox, context, stopped_by_parent);
                    let Some(incoming) = incoming else {
                        drop_containing_panic(actor_id, STATE_FAILED_TO_DROP, state);
                        break 'run;
                    };

                    match receive_incoming(actor_id, &actor, &context, incoming, state) {
                        Some(Control::Continue(next_state)) => state = next_state,

                        Some(Control::Stop) => {
                            debug!(%actor_id, "stopping as decided by actor");
                            break 'run;
                        }

                        None => break,
                    }
                }
            }

            let restart = should_restart(
                actor_id,
                config.supervision_strategy,
                up_since,
                &mut restarts,
                &parent_stopping_rx,
                &mut stopped_by_parent,
                &mut context,
            )
            .await;
            if !restart {
                break;
            }
        }

        terminate(actor, context, mailbox).await;
    });

    actor_ref
}

pub(crate) fn catch_and_log<T, E, F>(actor_id: ActorId, failure: &str, f: F) -> Option<T>
where
    E: Error,
    F: FnOnce() -> Result<T, E>,
{
    log_failure(actor_id, failure, catch_panic_and_log(actor_id, failure, f))
}

pub(crate) fn log_failure<T, E>(
    actor_id: ActorId,
    failure: &str,
    result: Option<Result<T, E>>,
) -> Option<T>
where
    E: Error,
{
    match result? {
        Ok(value) => Some(value),

        Err(error) => {
            error!(%actor_id, %error, source = error.source(), "{failure}");
            None
        }
    }
}

pub(crate) fn catch_panic_and_log<T, F>(actor_id: ActorId, failure: &str, f: F) -> Option<T>
where
    F: FnOnce() -> T,
{
    match catch_unwind(AssertUnwindSafe(f)) {
        Ok(value) => Some(value),

        Err(panic) => {
            error!(%actor_id, panic = %PanicPayload(panic.as_ref()), "{failure}");
            None
        }
    }
}

pub(crate) fn drop_containing_panic<T>(actor_id: ActorId, failure: &str, value: T) {
    if let Err(panic) = catch_unwind(AssertUnwindSafe(|| drop(value))) {
        error!(%actor_id, panic = %PanicPayload(panic.as_ref()), "{failure}");
    }
}

pub(crate) async fn should_restart<F, M>(
    actor_id: ActorId,
    supervision_strategy: SupervisionStrategy,
    up_since: Option<Instant>,
    restarts: &mut u32,
    parent_stopping_rx: &watch::Receiver<()>,
    stopped_by_parent: &mut Pin<&mut F>,
    context: &mut ActorContext<M>,
) -> bool
where
    F: Future,
{
    let delay = match next_restart(supervision_strategy, up_since, restarts) {
        Restart::After(delay) => delay,

        Restart::LimitExceeded => {
            error!(%actor_id, "stopping, because the restart limit is exceeded");
            return false;
        }

        Restart::NotConfigured => return false,
    };

    if parent_stopping_rx.has_changed().unwrap_or(true) {
        debug!(%actor_id, "stopping, because parent stopped this actor");
        return false;
    }
    debug!(%actor_id, restarts = *restarts, ?delay, "restarting");

    context.stop_children().await;

    match await_backoff(delay, stopped_by_parent.as_mut()).await {
        Interrupted::No => true,

        Interrupted::StoppedByParent => {
            debug!(%actor_id, "stopping, because parent stopped this actor");
            false
        }
    }
}

#[cfg_attr(feature = "hotpath", hotpath::measure)]
pub(crate) async fn terminate<A, M>(actor: A, mut context: ActorContext<M>, mailbox: Mailbox<M>) {
    let actor_id = context.self_ref().actor_id();

    let (incoming_rx, watchers) = mailbox.split();
    let drained = incoming_rx.drain().collect::<Vec<_>>();
    drop_containing_panic(actor_id, "mailbox failed to drop", incoming_rx);
    for incoming in drained {
        drop_containing_panic(actor_id, "queued message failed to drop", incoming);
    }

    for watched in context.take_watched().into_values() {
        unwatch(actor_id, watched);
    }

    context.stop_children().await;
    debug!(%actor_id, "all child actors terminated");
    drop(context);

    drop_containing_panic(actor_id, "actor failed to drop", actor);

    for watcher in watchers.close() {
        if let Err(error) = watcher.handle_terminated(actor_id) {
            debug!(
                %actor_id,
                watcher_id = %watcher.watcher_id(),
                %error,
                source = error.source(),
                "cannot send terminated signal"
            );
        }
    }

    debug!(%actor_id, "terminated");
}

enum Watched {
    Local(WatcherRegistry),

    #[cfg(feature = "cluster")]
    Remote {
        node: cluster::NodeId,
        target: ActorId,
    },
}

#[derive(Debug, PartialEq, Eq)]
enum Restart {
    After(Duration),
    LimitExceeded,
    NotConfigured,
}

#[derive(Debug, PartialEq, Eq)]
enum Interrupted {
    No,
    StoppedByParent,
}

fn unwatch(watcher_id: ActorId, watched: Watched) {
    match watched {
        Watched::Local(registry) => registry.remove(watcher_id),

        #[cfg(feature = "cluster")]
        Watched::Remote { node, target } => cluster::unwatch_remote(node, target, watcher_id),
    }
}

#[cfg_attr(feature = "hotpath", hotpath::measure)]
fn receive_incoming<A>(
    actor_id: ActorId,
    actor: &A,
    context: &ActorContext<A::Message>,
    incoming: Incoming<A::Message>,
    state: A::State,
) -> Option<Control<A::State>>
where
    A: Actor,
{
    catch_and_log(actor_id, "actor failed", || {
        actor.receive(context, incoming, state)
    })
}

fn next_restart(
    supervision_strategy: SupervisionStrategy,
    up_since: Option<Instant>,
    restarts: &mut u32,
) -> Restart {
    let SupervisionStrategy::Restart(policy) = supervision_strategy else {
        return Restart::NotConfigured;
    };

    if up_since.is_some_and(|up_since| up_since.elapsed() >= policy.reset_after) {
        *restarts = 0;
    }
    if *restarts >= policy.max_restarts.get() {
        return Restart::LimitExceeded;
    }

    let delay = policy.backoff.duration(*restarts);
    *restarts += 1;

    Restart::After(delay)
}

async fn await_backoff<F>(delay: Duration, stopped_by_parent: Pin<&mut F>) -> Interrupted
where
    F: Future,
{
    select! {
        biased;
        _ = stopped_by_parent => Interrupted::StoppedByParent,
        _ = sleep(delay) => Interrupted::No,
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        Backoff, RestartPolicy, SupervisionStrategy,
        actor_context::{Interrupted, PanicPayload, Restart, await_backoff, next_restart},
    };
    use std::{future, num::NonZeroU32, pin::pin, time::Duration};
    use tokio::time::Instant;

    const MIN: Duration = Duration::from_millis(250);
    const MAX: Duration = Duration::from_secs(1);
    const NO_RESET: Duration = Duration::from_secs(3600);

    #[tokio::test(start_paused = true)]
    async fn a_backoff_elapses_uninterrupted() {
        let interrupted =
            await_backoff(Duration::from_secs(1), pin!(future::pending::<()>())).await;

        assert_eq!(interrupted, Interrupted::No);
    }

    #[tokio::test(start_paused = true)]
    async fn a_parent_stop_beats_the_backoff() {
        let interrupted = await_backoff(Duration::from_secs(1), pin!(future::ready(()))).await;

        assert_eq!(interrupted, Interrupted::StoppedByParent);
    }

    /// A panic payload is a `&'static str` for a literal panic and a `String` for a formatted one,
    /// so both must format as the message itself; anything else is named as such rather than
    /// silently swallowed.
    #[test]
    fn panic_payload_displays_both_string_shapes() {
        assert_eq!(PanicPayload(&"boom").to_string(), "boom");
        assert_eq!(PanicPayload(&"boom".to_string()).to_string(), "boom");
        assert_eq!(PanicPayload(&42).to_string(), "<non-string panic payload>");
    }

    /// Under `Stop` a failure is never retried, however many failures came before it.
    #[test]
    fn stop_never_restarts() {
        let mut restarts = 0;

        assert_eq!(
            next_restart(SupervisionStrategy::Stop, None, &mut restarts),
            Restart::NotConfigured
        );
        assert_eq!(restarts, 0);
    }

    /// The n-th consecutive restart is delayed by the backoff's `min * 2^(n-1)`, capped at its
    /// `max`, and each one advances the count by exactly one.
    #[test]
    fn the_delay_doubles_and_advances_the_count() {
        let strategy = restart(NonZeroU32::MAX, Duration::ZERO);
        let mut restarts = 0;

        for expected in [MIN, MIN * 2, MIN * 4, MAX, MAX] {
            assert_eq!(
                next_restart(strategy, None, &mut restarts),
                Restart::After(expected)
            );
        }
        assert_eq!(restarts, 5);
    }

    /// One failure more than `max_restarts` stops the actor rather than restarting
    /// it again.
    #[test]
    fn exceeding_the_limit_stops() {
        let strategy = restart(NonZeroU32::new(2).expect("2 is not zero"), Duration::ZERO);
        let mut restarts = 0;

        assert_eq!(
            next_restart(strategy, None, &mut restarts),
            Restart::After(MIN)
        );
        assert_eq!(
            next_restart(strategy, None, &mut restarts),
            Restart::After(MIN * 2)
        );
        assert_eq!(
            next_restart(strategy, None, &mut restarts),
            Restart::LimitExceeded
        );
    }

    /// Running for at least `reset_after` resets the count, so an actor which keeps recovering is
    /// restarted indefinitely instead of exhausting its limit.
    #[tokio::test]
    async fn running_long_enough_resets_the_count() {
        let strategy = restart(NonZeroU32::MIN, Duration::ZERO);
        let mut restarts = 7;

        assert_eq!(
            next_restart(strategy, Some(Instant::now()), &mut restarts),
            Restart::After(MIN)
        );
        assert_eq!(restarts, 1);
    }

    /// Running for less than `reset_after` keeps the count, so an actor which fails again right
    /// after coming up still escalates to a stop instead of restarting forever.
    #[tokio::test]
    async fn running_briefly_keeps_the_count() {
        let strategy = restart(NonZeroU32::MIN, NO_RESET);
        let mut restarts = 1;

        assert_eq!(
            next_restart(strategy, Some(Instant::now()), &mut restarts),
            Restart::LimitExceeded
        );
        assert_eq!(restarts, 1);
    }

    fn restart(max_restarts: NonZeroU32, reset_after: Duration) -> SupervisionStrategy {
        SupervisionStrategy::Restart(RestartPolicy {
            max_restarts,
            backoff: Backoff::new(MIN, MAX).expect("the bounds are valid"),
            reset_after,
        })
    }
}
