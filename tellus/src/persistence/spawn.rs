use crate::{
    ActorConfig, ActorContext, ActorId, ActorRef, ActorSystem,
    actor_context::{
        PanicPayload, STATE_FAILED_TO_DROP, await_restart, catch_and_log, catch_panic_and_log,
        drop_containing_panic, next_incoming, terminate,
    },
    actor_ref::SelfRef,
    actor_system::watch_root,
    persistence::{
        Persistence,
        codec::{Codec, EncodeError},
        effect::Effect,
        event_sourced::EventSourced,
        persistence_id::PersistenceId,
        seq_no::SeqNo,
        store::{
            AppendError, EncodedEvent, EncodedSnapshot, EventStore, SnapshotStore, StoredEvent,
            StoredSnapshot,
        },
        versioned::{DecodeError, Versioned, decode_versioned},
    },
};
use std::{
    any::Any,
    error::Error,
    future::{Future, poll_fn},
    num::NonZeroUsize,
    panic::{AssertUnwindSafe, catch_unwind},
    pin::pin,
    task::Poll,
};
use thiserror::Error;
use tokio::{select, sync::watch, task, time::Instant};
use tracing::{debug, error, warn};

const FAILED_TO_RECOVER: &str = "actor failed to recover";

const SNAPSHOT_NOT_SAVED: &str = "snapshot not saved";

const VALUES_FAILED_TO_DROP: &str = "actor values failed to drop";

const REPLAY_PAGE: NonZeroUsize = NonZeroUsize::new(512).unwrap();

impl<M> ActorSystem<M>
where
    M: Send + 'static,
{
    /// Create an actor system by giving the [EventSourced] actor and [Persistence] for the root
    /// actor, using the default [ActorConfig].
    ///
    /// # Panics
    /// Panics if called outside of a Tokio runtime.
    pub fn event_sourced<A, E, S, C>(actor: A, persistence: Persistence<E, S, C>) -> Self
    where
        A: EventSourced<Command = M> + Send + Sync + 'static,
        A::State: Send + 'static,
        A::Event: Send + 'static,
        A::Snapshot: Send + 'static,
        E: EventStore,
        S: SnapshotStore,
        C: Codec + Send + Sync + 'static,
    {
        Self::event_sourced_with_config(actor, persistence, ActorConfig::default())
    }

    /// Create an actor system by giving the [EventSourced] actor, [Persistence] and [ActorConfig]
    /// for the root actor.
    ///
    /// # Panics
    /// Panics if called outside of a Tokio runtime.
    pub fn event_sourced_with_config<A, E, S, C>(
        actor: A,
        persistence: Persistence<E, S, C>,
        config: ActorConfig,
    ) -> Self
    where
        A: EventSourced<Command = M> + Send + Sync + 'static,
        A::State: Send + 'static,
        A::Event: Send + 'static,
        A::Snapshot: Send + 'static,
        E: EventStore,
        S: SnapshotStore,
        C: Codec + Send + Sync + 'static,
    {
        let (stopping_tx, stopping_rx) = watch::channel(());

        let root = spawn_event_sourced(stopping_rx, actor, persistence, config);
        let terminated_rx = watch_root(&root, stopping_tx);

        Self::from_parts(root, terminated_rx)
    }
}

impl<M> ActorContext<M> {
    /// Spawn an event-sourced child actor with the given [EventSourced] actor and [Persistence],
    /// using the default [ActorConfig].
    ///
    /// # Panics
    /// Panics if called outside of a Tokio runtime.
    pub fn spawn_event_sourced<A, E, S, C>(
        &self,
        actor: A,
        persistence: Persistence<E, S, C>,
    ) -> ActorRef<A::Command>
    where
        A: EventSourced + Send + Sync + 'static,
        A::Command: Send + 'static,
        A::State: Send + 'static,
        A::Event: Send + 'static,
        A::Snapshot: Send + 'static,
        E: EventStore,
        S: SnapshotStore,
        C: Codec + Send + Sync + 'static,
    {
        self.spawn_event_sourced_with_config(actor, persistence, ActorConfig::default())
    }

    /// Spawn an event-sourced child actor with the given [EventSourced] actor, [Persistence] and
    /// [ActorConfig].
    ///
    /// # Panics
    /// Panics if called outside of a Tokio runtime.
    pub fn spawn_event_sourced_with_config<A, E, S, C>(
        &self,
        actor: A,
        persistence: Persistence<E, S, C>,
        config: ActorConfig,
    ) -> ActorRef<A::Command>
    where
        A: EventSourced + Send + Sync + 'static,
        A::Command: Send + 'static,
        A::State: Send + 'static,
        A::Event: Send + 'static,
        A::Snapshot: Send + 'static,
        E: EventStore,
        S: SnapshotStore,
        C: Codec + Send + Sync + 'static,
    {
        spawn_event_sourced(self.stopping_rx(), actor, persistence, config)
    }
}

struct Recovered<S> {
    id: PersistenceId,
    state: S,
    next_seq_no: SeqNo,
}

struct Settled<S> {
    state: S,
    next_seq_no: SeqNo,
    stop: bool,
}

#[derive(Debug, Error)]
enum ReplayError {
    #[error(transparent)]
    Decode(#[from] DecodeError),

    #[error("event stream gap: sequence number {seq_no}, expected {expected}")]
    Gap { seq_no: SeqNo, expected: SeqNo },
}

fn spawn_event_sourced<A, E, S, C>(
    parent_stopping_rx: watch::Receiver<()>,
    actor: A,
    persistence: Persistence<E, S, C>,
    config: ActorConfig,
) -> ActorRef<A::Command>
where
    A: EventSourced + Send + Sync + 'static,
    A::Command: Send + 'static,
    A::State: Send + 'static,
    A::Event: Send + 'static,
    A::Snapshot: Send + 'static,
    E: EventStore,
    S: SnapshotStore,
    C: Codec + Send + Sync + 'static,
{
    let actor_id = ActorId::new();
    let (self_ref, mailbox) = SelfRef::new(actor_id, config.mailbox_capacity);
    let actor_ref = self_ref.actor_ref().clone();

    task::spawn(async move {
        let mut context = ActorContext::new(self_ref);

        let mut rx = parent_stopping_rx.clone();
        let mut stopped_by_parent = pin!(rx.changed());

        let mut restarts = 0;

        'run: loop {
            let recovered = select! {
                biased;

                _ = &mut stopped_by_parent => {
                    debug!(%actor_id, "stopping, because parent stopped this actor");
                    break 'run;
                }

                recovered = recover(actor_id, &actor, &persistence) => recovered,
            };
            let mut up_since = None;

            if let Some(Recovered {
                id,
                state,
                mut next_seq_no,
            }) = recovered
            {
                let state = catch_and_log(actor_id, FAILED_TO_RECOVER, || {
                    actor.recovered(&context, state)
                });

                if let Some(mut state) = state {
                    up_since = Some(Instant::now());

                    loop {
                        let incoming =
                            next_incoming!(actor_id, mailbox, context, stopped_by_parent);
                        let Some(incoming) = incoming else {
                            drop_containing_panic(actor_id, STATE_FAILED_TO_DROP, state);
                            break 'run;
                        };

                        let effect = catch_and_log(actor_id, "actor failed", || {
                            actor.handle(&context, incoming, &state)
                        });
                        let Some(effect) = effect else {
                            drop_containing_panic(actor_id, STATE_FAILED_TO_DROP, state);
                            break;
                        };

                        match settle(
                            actor_id,
                            &actor,
                            &persistence,
                            &id,
                            state,
                            next_seq_no,
                            effect,
                        )
                        .await
                        {
                            Some(settled) => {
                                state = settled.state;
                                next_seq_no = settled.next_seq_no;

                                if settled.stop {
                                    debug!(%actor_id, "stopping as decided by actor");
                                    drop_containing_panic(actor_id, STATE_FAILED_TO_DROP, state);
                                    break 'run;
                                }
                            }

                            None => break,
                        }
                    }
                }
            }

            let restart = await_restart(
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

#[cfg_attr(feature = "hotpath", hotpath::measure)]
async fn recover<A, E, S, C>(
    actor_id: ActorId,
    actor: &A,
    persistence: &Persistence<E, S, C>,
) -> Option<Recovered<A::State>>
where
    A: EventSourced,
    E: EventStore,
    S: SnapshotStore,
    C: Codec,
{
    let id = catch_panic_and_log(actor_id, FAILED_TO_RECOVER, || actor.persistence_id())?;

    let loaded = catch_panic_and_log_async(actor_id, FAILED_TO_RECOVER, async {
        persistence.snapshot_store.load(&id).await
    })
    .await?;
    let snapshot = match loaded {
        Ok(snapshot) => snapshot,

        Err(error) => {
            error!(%actor_id, %error, source = error.source(), "{FAILED_TO_RECOVER}");
            return None;
        }
    };

    let decoded = snapshot.and_then(|stored| {
        let StoredSnapshot {
            next_seq_no,
            snapshot,
        } = stored;

        match catch_unwind(AssertUnwindSafe(|| {
            decode_versioned::<A::Snapshot, C>(
                &persistence.codec,
                &snapshot.manifest,
                snapshot.schema_version,
                &snapshot.payload,
            )
        })) {
            Ok(Ok(snapshot)) => Some((snapshot, next_seq_no)),

            Ok(Err(error)) => {
                warn!(
                    %actor_id,
                    %error,
                    source = error.source(),
                    "snapshot discarded, replaying in full"
                );
                None
            }

            Err(panic) => {
                warn!(
                    %actor_id,
                    panic = %PanicPayload(panic.as_ref()),
                    "snapshot discarded, replaying in full"
                );
                None
            }
        }
    });

    let (mut state, mut next_seq_no) = match decoded {
        Some((snapshot, next_seq_no)) => {
            let state = catch_and_log(actor_id, FAILED_TO_RECOVER, || {
                actor.init_from_snapshot(snapshot)
            })?;

            (state, next_seq_no)
        }

        None => {
            let state = catch_and_log(actor_id, FAILED_TO_RECOVER, || actor.init())?;

            (state, SeqNo::ZERO)
        }
    };

    loop {
        let page = catch_panic_and_log_async(
            actor_id,
            FAILED_TO_RECOVER,
            read_page(&persistence.event_store, &id, next_seq_no),
        )
        .await;
        let page = match page {
            Some(Ok(page)) => page,

            Some(Err(error)) => {
                error!(%actor_id, %error, source = error.source(), "{FAILED_TO_RECOVER}");
                drop_containing_panic(actor_id, STATE_FAILED_TO_DROP, state);
                return None;
            }

            None => {
                drop_containing_panic(actor_id, STATE_FAILED_TO_DROP, state);
                return None;
            }
        };
        let page_len = page.len();

        (state, next_seq_no) = catch_and_log(actor_id, FAILED_TO_RECOVER, || {
            replay_page(actor, &persistence.codec, state, next_seq_no, page)
        })?;

        if page_len < REPLAY_PAGE.get() {
            break;
        }
    }

    Some(Recovered {
        id,
        state,
        next_seq_no,
    })
}

#[cfg_attr(feature = "hotpath", hotpath::measure)]
async fn settle<A, E, S, C>(
    actor_id: ActorId,
    actor: &A,
    persistence: &Persistence<E, S, C>,
    id: &PersistenceId,
    mut state: A::State,
    mut next_seq_no: SeqNo,
    effect: Effect<A>,
) -> Option<Settled<A::State>>
where
    A: EventSourced,
    E: EventStore,
    S: SnapshotStore,
    C: Codec,
{
    let Effect {
        events,
        stop,
        thens,
    } = effect;
    let appended = !events.is_empty();

    if appended {
        let encoded = catch_and_log(actor_id, "actor failed to encode events", || {
            encode_events::<A, C>(&persistence.codec, &events)
        });
        let Some(encoded) = encoded else {
            drop_containing_panic(actor_id, VALUES_FAILED_TO_DROP, (state, events, thens));
            return None;
        };

        let appended_events = catch_panic_and_log_async(
            actor_id,
            "actor failed to append events",
            append_events(&persistence.event_store, id, next_seq_no, encoded),
        )
        .await;
        match appended_events {
            Some(Ok(())) => {}

            Some(Err(error)) => {
                error!(
                    %actor_id,
                    %error,
                    source = error.source(),
                    "actor failed to append events"
                );
                drop_containing_panic(actor_id, VALUES_FAILED_TO_DROP, (state, events, thens));
                return None;
            }

            None => {
                drop_containing_panic(actor_id, VALUES_FAILED_TO_DROP, (state, events, thens));
                return None;
            }
        }

        let appended_count = events.len();
        let applied = catch_panic_and_log(actor_id, "actor failed to apply events", || {
            apply_events(actor, state, events)
        });
        let Some(applied) = applied else {
            drop_containing_panic(actor_id, VALUES_FAILED_TO_DROP, thens);
            return None;
        };
        state = applied;
        next_seq_no = next_seq_no.advanced_by(appended_count);
    }

    let ran = catch_panic_and_log(actor_id, "actor continuation failed", || {
        for then in thens {
            then(&state);
        }
    });
    if ran.is_none() {
        drop_containing_panic(actor_id, STATE_FAILED_TO_DROP, state);
        return None;
    }

    if appended {
        let encoded = catch_unwind(AssertUnwindSafe(|| {
            encode_snapshot(actor_id, actor, &persistence.codec, &state)
        }));

        if let Some(Some(snapshot)) = warn_unless_snapshot_saved(actor_id, encoded) {
            let saved = catch_unwind_async(save_snapshot(
                &persistence.snapshot_store,
                id,
                next_seq_no,
                snapshot,
            ))
            .await;
            warn_unless_snapshot_saved(actor_id, saved);
        }
    }

    Some(Settled {
        state,
        next_seq_no,
        stop,
    })
}

fn warn_unless_snapshot_saved<T, E>(
    actor_id: ActorId,
    result: Result<Result<T, E>, Box<dyn Any + Send>>,
) -> Option<T>
where
    E: Error,
{
    match result {
        Ok(Ok(value)) => Some(value),

        Ok(Err(error)) => {
            warn!(%actor_id, %error, source = error.source(), "{SNAPSHOT_NOT_SAVED}");
            None
        }

        Err(panic) => {
            warn!(%actor_id, panic = %PanicPayload(panic.as_ref()), "{SNAPSHOT_NOT_SAVED}");
            None
        }
    }
}

async fn catch_panic_and_log_async<T, Fut>(actor_id: ActorId, failure: &str, fut: Fut) -> Option<T>
where
    Fut: Future<Output = T>,
{
    match catch_unwind_async(fut).await {
        Ok(value) => Some(value),

        Err(panic) => {
            error!(%actor_id, panic = %PanicPayload(panic.as_ref()), "{failure}");
            None
        }
    }
}

// A store panic must not unwind the task past `terminate`. Only polls are caught: `fut` must be
// a local async fn or async block, whose construction cannot run store code.
async fn catch_unwind_async<T, Fut>(fut: Fut) -> Result<T, Box<dyn Any + Send>>
where
    Fut: Future<Output = T>,
{
    let mut fut = pin!(fut);

    poll_fn(
        |cx| match catch_unwind(AssertUnwindSafe(|| fut.as_mut().poll(cx))) {
            Ok(poll) => poll.map(Ok),
            Err(panic) => Poll::Ready(Err(panic)),
        },
    )
    .await
}

#[cfg_attr(feature = "hotpath", hotpath::measure)]
async fn read_page<E>(
    event_store: &E,
    id: &PersistenceId,
    from_seq_no: SeqNo,
) -> Result<Vec<StoredEvent>, E::Error>
where
    E: EventStore,
{
    event_store.read(id, from_seq_no, REPLAY_PAGE).await
}

#[cfg_attr(feature = "hotpath", hotpath::measure)]
fn replay_page<A, C>(
    actor: &A,
    codec: &C,
    state: A::State,
    next_seq_no: SeqNo,
    page: Vec<StoredEvent>,
) -> Result<(A::State, SeqNo), ReplayError>
where
    A: EventSourced,
    C: Codec,
{
    page.into_iter()
        .try_fold((state, next_seq_no), |(state, next_seq_no), stored| {
            if stored.seq_no != next_seq_no {
                return Err(ReplayError::Gap {
                    seq_no: stored.seq_no,
                    expected: next_seq_no,
                });
            }

            let event = decode_versioned::<A::Event, C>(
                codec,
                &stored.event.manifest,
                stored.event.schema_version,
                &stored.event.payload,
            )?;

            Ok((actor.apply(state, event), next_seq_no.succ()))
        })
}

#[cfg_attr(feature = "hotpath", hotpath::measure)]
fn encode_events<A, C>(codec: &C, events: &[A::Event]) -> Result<Vec<EncodedEvent>, EncodeError>
where
    A: EventSourced,
    C: Codec,
{
    events
        .iter()
        .map(|event| {
            let payload = codec.encode(event)?;

            Ok(EncodedEvent {
                manifest: A::Event::MANIFEST.to_string(),
                schema_version: A::Event::VERSION,
                payload,
            })
        })
        .collect()
}

#[cfg_attr(feature = "hotpath", hotpath::measure)]
async fn append_events<E>(
    event_store: &E,
    id: &PersistenceId,
    next_seq_no: SeqNo,
    events: Vec<EncodedEvent>,
) -> Result<(), AppendError<E::Error>>
where
    E: EventStore,
{
    event_store.append(id, next_seq_no, events).await
}

#[cfg_attr(feature = "hotpath", hotpath::measure)]
fn apply_events<A>(actor: &A, state: A::State, events: Vec<A::Event>) -> A::State
where
    A: EventSourced,
{
    events
        .into_iter()
        .fold(state, |state, event| actor.apply(state, event))
}

#[cfg_attr(feature = "hotpath", hotpath::measure)]
fn encode_snapshot<A, C>(
    actor_id: ActorId,
    actor: &A,
    codec: &C,
    state: &A::State,
) -> Result<Option<EncodedSnapshot>, EncodeError>
where
    A: EventSourced,
    C: Codec,
{
    let snapshot = match actor.snapshot(state) {
        Ok(Some(snapshot)) => snapshot,

        Ok(None) => return Ok(None),

        Err(error) => {
            warn!(%actor_id, %error, source = error.source(), "{SNAPSHOT_NOT_SAVED}");
            return Ok(None);
        }
    };
    let payload = codec.encode(&snapshot)?;

    Ok(Some(EncodedSnapshot {
        manifest: A::Snapshot::MANIFEST.to_string(),
        schema_version: A::Snapshot::VERSION,
        payload,
    }))
}

#[cfg_attr(feature = "hotpath", hotpath::measure)]
async fn save_snapshot<S>(
    snapshot_store: &S,
    id: &PersistenceId,
    next_seq_no: SeqNo,
    snapshot: EncodedSnapshot,
) -> Result<(), S::Error>
where
    S: SnapshotStore,
{
    snapshot_store.save(id, next_seq_no, snapshot).await
}
