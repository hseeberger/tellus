//! Persistence benchmarks against an in-memory store, isolating framework overhead (codec,
//! settlement, replay) from backend latency:
//!
//! - `persist`: the bench thread floods a single event-sourced counter, one persisted event per
//!   command, without snapshots and with a snapshot every [SNAPSHOT_EACH] events.
//! - `recover`: an actor recovers from a stream of [EVENTS] events, by full replay and from a
//!   snapshot plus replay tail.

mod in_memory_store {
    include!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/support/in_memory_store.rs"
    ));
}

use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use in_memory_store::InMemoryStore;
use serde::{Deserialize, Serialize};
use std::{
    convert::Infallible,
    time::{Duration, Instant},
};
use tellus::{
    ActorContext, ActorSystem, Effect, EventSourced, Incoming, Persistence, PersistenceId,
    SchemaVersion, Versioned,
};
use tokio::runtime::Runtime;

const EVENTS: u64 = 10_000;
const SNAPSHOT_EACH: u64 = 3_000;

const _: () = assert!(
    !EVENTS.is_multiple_of(SNAPSHOT_EACH),
    "recovery from a snapshot needs a replay tail after the last snapshot"
);

fn persist(c: &mut Criterion) {
    let rt = Runtime::new().expect("tokio runtime can be created");

    let mut group = c.benchmark_group("persist");
    group.throughput(Throughput::Elements(EVENTS));

    for (label, snapshot_each) in [("no_snapshots", None), ("snapshots", Some(SNAPSHOT_EACH))] {
        group.bench_function(label, |b| {
            b.to_async(&rt).iter_custom(|iters| async move {
                let mut elapsed = Duration::ZERO;

                for _ in 0..iters {
                    let store = InMemoryStore::default();

                    let start = Instant::now();
                    let system = spawn(&store, snapshot_each);
                    for _ in 0..EVENTS {
                        system.root().tell(Increase);
                    }
                    system
                        .terminated()
                        .await
                        .expect("awaiting actor system termination");
                    elapsed += start.elapsed();
                }

                elapsed
            });
        });
    }

    group.finish();
}

fn recover(c: &mut Criterion) {
    let rt = Runtime::new().expect("tokio runtime can be created");

    let mut group = c.benchmark_group("recover");

    for (label, snapshot_each) in [("replay", None), ("snapshot", Some(SNAPSHOT_EACH))] {
        let store = InMemoryStore::default();
        rt.block_on(async {
            let system = spawn(&store, snapshot_each);
            for _ in 0..EVENTS {
                system.root().tell(Increase);
            }
            system
                .terminated()
                .await
                .expect("awaiting actor system termination");
        });

        group.bench_function(label, |b| {
            let store = store.clone();
            b.to_async(&rt).iter_custom(move |iters| {
                let store = store.clone();

                async move {
                    let mut elapsed = Duration::ZERO;

                    for _ in 0..iters {
                        let start = Instant::now();
                        let system = spawn(&store, snapshot_each);
                        system.root().tell(Increase);
                        system
                            .terminated()
                            .await
                            .expect("awaiting actor system termination");
                        elapsed += start.elapsed();
                    }

                    elapsed
                }
            });
        });
    }

    group.finish();
}

fn spawn(store: &InMemoryStore, snapshot_each: Option<u64>) -> ActorSystem<Increase> {
    let counter = Counter {
        events: EVENTS,
        snapshot_each,
    };
    let persistence = Persistence::new(store.clone()).with_snapshot_store(store.clone());

    ActorSystem::event_sourced(counter, persistence)
}

struct Increase;

#[derive(Serialize, Deserialize)]
struct Increased;

impl Versioned for Increased {
    const MANIFEST: &'static str = "increased";
    const VERSION: SchemaVersion = SchemaVersion::new(1);
}

#[derive(Serialize, Deserialize)]
struct Count(u64);

impl Versioned for Count {
    const MANIFEST: &'static str = "count";
    const VERSION: SchemaVersion = SchemaVersion::new(1);
}

struct Counter {
    events: u64,
    snapshot_each: Option<u64>,
}

impl EventSourced for Counter {
    type Command = Increase;
    type Event = Increased;
    type State = u64;
    type Snapshot = Count;
    type Error = Infallible;

    fn persistence_id(&self) -> PersistenceId {
        PersistenceId::new("counter", "bench").expect("the segments are valid")
    }

    fn init(&self) -> Result<Self::State, Self::Error> {
        Ok(0)
    }

    fn init_from_snapshot(&self, Count(count): Self::Snapshot) -> Result<Self::State, Self::Error> {
        Ok(count)
    }

    fn handle(
        &self,
        _: &ActorContext<Self::Command>,
        _: Incoming<Self::Command>,
        count: &Self::State,
    ) -> Result<Effect<Self>, Self::Error> {
        let effect = if *count >= self.events {
            Effect::stop()
        } else if count + 1 == self.events {
            Effect::persist(Increased).and_stop()
        } else {
            Effect::persist(Increased)
        };

        Ok(effect)
    }

    fn apply(&self, count: Self::State, Increased: Self::Event) -> Self::State {
        count + 1
    }

    fn snapshot(&self, count: &Self::State) -> Result<Option<Self::Snapshot>, Self::Error> {
        let due = self
            .snapshot_each
            .is_some_and(|snapshot_each| count.is_multiple_of(snapshot_each));

        Ok(due.then_some(Count(*count)))
    }
}

criterion_group!(
    name = benches;
    config = Criterion::default()
        .sample_size(10)
        .noise_threshold(0.05)
        .warm_up_time(Duration::from_secs(1))
        .measurement_time(Duration::from_secs(5));
    targets = persist, recover
);
criterion_main!(benches);
