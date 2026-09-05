# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this
repository.

## Commands

Tasks are defined in the [justfile](justfile):

- `just all`: check, fmt, lint, test and doc; the full local gate for every crate except
  `tellus-comparison`. The `tellus-persistence-postgres` tests use testcontainers, so the gate
  needs Docker.
- `just check` / `just lint` / `just test`: the individual steps, each a feature matrix, every run
  scoped to one crate. Check and lint drive the matrix with `cargo hack --feature-powerset`: the
  `powerset` variable in the justfile takes `hotpath` and `hotpath-alloc` off the axes and declares
  the two implication pairs (`persistence`/`persistence-tests`, `cluster`/`cluster-dev`) mutually
  exclusive, which leaves 18 runs of `-p tellus`, plus one `hotpath` run, one `--all-features` run
  (cargo-hack drops its own once either of those flags is given), the two-run powerset of each of
  `-p tellus-bootstrap-dns` and `-p tellus-bootstrap-k8s`, and one run of `-p
  tellus-persistence-postgres`. Adding a feature therefore extends check and lint by itself; only a
  new implication pair needs a hand edit. Test stays hand-picked, since its combinations cost test
  execution rather than a compile: seven runs of `-p tellus` (no features, `serde`, `persistence`,
  `persistence-tests`, `cluster-dev`, `cluster-dev,serde`, all features) plus two each of `-p
  tellus-bootstrap-dns` and `-p tellus-bootstrap-k8s` and one of `-p tellus-persistence-postgres`,
  with `cluster-dev` in place of `cluster` since the remoting test needs it. The combined cluster
  and serde run is what covers the serde-gated cluster code without the other optional features,
  which `--all-features` would hide. No run spans the workspace: a build containing multiple crates
  feature-unifies `tellus` with their features enabled, so only
  scoped runs exercise the reduced-feature configurations, and only a scoped run builds a backend
  crate against the feature set it actually declares (`cargo hack` keeps that property: it is
  invoked per crate, never with `--workspace`). `just doc` runs workspace-wide with all features
  and `--cfg docsrs`.
- `.cargo/config.toml` sets `K8S_OPENAPI_ENABLED_VERSION`, which `k8s-openapi` needs whenever
  dev-dependencies are not built: plain `cargo check`, `cargo build`, `just doc` and
  rust-analyzer. Only a binary may pick the Kubernetes version through a `v1_*` feature, so
  `tellus-bootstrap-k8s` enables `latest` in its dev-dependency alone, which is what covers every
  `--all-targets` run, while `tellus-cluster-demo`, being a binary, names `v1_36` itself. All
  three must name the same version; a mismatch fails the build loudly.
- `just fmt`: formats Rust (nightly rustfmt, the justfile derives the matching nightly from the
  installed stable) and TOML (taplo). Plain `cargo fmt` is not enough; the rustfmt config uses
  unstable options.
- Single test: `cargo test -p tellus --test watch <test_name>` (integration tests live in
  `tellus/tests/`: `ask.rs`, `persistence.rs`, `persistence_tests.rs`, `supervision.rs`,
  `termination.rs`, `watch.rs`, plus `cluster.rs`, which needs `--features cluster-dev`, has no
  test harness and runs its scenarios from `main`, spawning itself as further nodes).
- Examples: `just run-examples-hello`, `just run-examples-scatter-gather`, `just
  run-examples-remote-scatter-gather`, `just run-examples-cluster`, `just
  run-examples-event-sourced-counter`.
- Cluster demo: `just cluster-demo-up` starts the forever running five node chaos cluster
  (`tellus-cluster-demo`) in Docker Compose, `cluster-demo-logs`, `cluster-demo-status` and
  `cluster-demo-violations` inspect it, `cluster-demo-down` stops it and drops its volumes. It is
  not part of `just all`; its own gate is `just cluster-demo-check`, `just cluster-demo-lint` and
  `just cluster-demo-test`.

Benchmarks:

- `just profile` / `just profile-alloc`: run `tellus/examples/profile.rs` with hotpath profiling,
  reporting per-function timings or allocation bytes for the instrumented hot path (send path,
  mailbox, run loop, termination). Instrumentation is gated behind the off-by-default `hotpath`
  feature; read the report as relative attribution, criterion stays the source of truth for
  absolute regressions.
- `just profile-alloc-gate`: run the profiling workload with allocation tracking and fail unless
  `tell`, `reserve` and `receive_incoming` allocate exactly 0 bytes; `profile-alloc-check <file>`
  applies that check to an existing JSON report. CI (`profile.yml`) profiles every PR against its
  merge base, posts the per-function comparison as an informational PR comment, and fails the
  build only on the zero-alloc check.
- `just profile-persistence` / `just profile-persistence-alloc`: the same for the persistence code
  (`tellus/examples/profile_persistence.rs`, features `hotpath` plus `persistence`): command
  settlement (encode, append, apply, snapshot) and recovery (snapshot load, paged read, replay)
  against an in-memory store. Settlement allocates by design (payload buffers, manifests), so most
  of the alloc report is a budget, not a zero gate; the exceptions are `apply_events` and
  `replay_page`, which must stay allocation-free: `just profile-persistence-alloc-gate` runs the
  workload and fails on a violation, `profile-persistence-alloc-check <file>` applies that check
  to an existing JSON report, and CI enforces it alongside the messaging zero-alloc check and
  includes the comparison in the profile comment.
- `just bench`: tellus's own criterion regression benchmarks, messaging
  (`tellus/benches/messaging.rs`) and persistence against an in-memory store
  (`tellus/benches/persistence.rs`, feature `persistence`); `just bench-save <baseline>` / `just
  bench-compare <baseline>` and the `bench-persistence-*` pair for local before/after comparisons.
  CI benchmarks every PR against its merge base, both benches, and posts the comparison as an
  informational PR comment; it does not fail the build. The report flags a benchmark whose 95%
  confidence lower bound exceeds `bench_regression_threshold`, but that verdict is not gate-grade
  on a shared CI runner: `flood/bounded` measured +19.9%, +13.3%, -9.5%, -35.7% and +8.1% across
  five runs of identical code, each with a tight criterion confidence interval, because criterion
  sees within-run variance only. Pushes to `main` additionally publish both benches to the
  gh-pages dashboard (`dev/bench`) via `bench-bencher` and `bench-persistence-bencher`, one
  section per store-result step name: "Core" (messaging) and "Persistence". Adding a section means
  a bencher recipe plus a benchmark and a store-result step in `bench.yml`; `dev/bench/index.html`
  on `gh-pages` is a customized copy of the action's default page (headline plus one sub-heading
  per section) which the action leaves alone once it exists.
- `just comparison` plus `comparison-check` / `comparison-lint`: the `tellus-comparison` crate
  benchmarking tellus against kameo and ractor. It is deliberately excluded from `just all` and
  from per-PR CI so its dependencies stay out of tellus's build; touching it means running its own
  check and lint recipes.

CI enforces that a PR consists of exactly one commit; squash before pushing.

## Workspace layout

- `tellus/`: the actor framework, the only published-facing crate.
- `tellus-bootstrap-dns/`: DNS seed discovery (`DnsSeeds` implements `SeedDiscovery`, SRV or
  A/AAAA records) for cluster bootstrap; the trait and the run-once `cluster::bootstrap` loop live
  in `tellus/src/cluster/bootstrap.rs` (feature `cluster`). Its tests resolve against an in-process
  hickory-server, no network.
- `tellus-bootstrap-k8s/`: Kubernetes seed discovery (`K8sSeeds` implements `SeedDiscovery`,
  listing the pods matching a label selector) for cluster bootstrap; one address per pod, the
  primary one, since bootstrap counts addresses and admits a peer only at the address it
  advertises. Its tests answer the kube client from a `tower-test` mock, no cluster.
- `tellus-persistence-postgres/`: the PostgreSQL stores (`PostgresStore` implements `EventStore`
  and `SnapshotStore`). Tests need Docker (testcontainers) and run as part of `just all`; their
  image tag and credentials come from the root `docker-compose.yaml` via `composed`, so there is
  one place to change either. Examples via `just run-examples-event-sourced-counter` and `just
  run-examples-event-sourced-supervision`, which start the `postgres` service from the root
  `docker-compose.yaml`; `docker compose down` stops it, `down -v` resets the data.
- `tellus/src/persistence_tests.rs` (feature `persistence-tests`, implies `persistence`): the
  contract test suite for persistence stores; backend crates run it from their integration tests
  against real servers. `tellus/tests/persistence_tests.rs` validates the suite itself against a
  minimal in-memory store, `tellus/tests/support/in_memory_store.rs`, which the persistence bench
  and profiling example `include!` as well.
- `composed`: a crates.io dependency which reads image name, tag and environment variables out of
  a `docker-compose.yaml` at compile time, so testcontainers-based tests use the same image
  version and credentials as the local stack instead of hardcoding them.
  `tellus-persistence-postgres/tests/store.rs` holds it in a `static COMPOSE: LazyLock<Compose>`
  built by the `compose!` macro, whose path argument is relative to the *calling* crate's
  manifest. Lookups panic like assertions, naming the compose file. Deliberately exposes no
  `ports` or `command`: tests use the port testcontainers maps.
- `tellus-cluster-demo/`: a five node cluster running forever under continuous chaos, in two
  stacks running the same two binaries. Docker Compose (`cluster-demo-up`, `-down`, `-logs`,
  `-status`, `-violations`): DNS seed discovery through a Docker network alias shared by all five
  nodes, pumba crashing one node or partitioning two off the other three every cycle, Caddy load
  balancing the nodes' HTTP APIs, and a second network carrying nothing but HTTP so a partitioned
  node stays observable while it self-downs. Kubernetes in kind (`cluster-demo-k8s-up
  [dns|k8s]`, `-down`, `-logs`, `-status`, `-violations`, needing `kind` v0.33 or later and
  `kubectl`): a StatefulSet behind a headless service, a service instead of Caddy, faults from
  `k8s/chaos.sh` through kubectl with the partition a pair of label-driven NetworkPolicies, and
  either discovery backend. Which one a node uses is `CONFIG_OVERLAYS` naming `config/dns.yaml`
  or `config/k8s.yaml`; `seeds` is deliberately absent from `config/default.yaml`, since overlays
  merge and a default would leave both variants. The verifier is the same binary on both stacks
  and holds the cluster to convergence and cross-node messaging in every quiet window. Like
  `tellus-comparison` both stay out of `just all` and per-PR CI; read its README and
  `k8s/README.md` before changing either.
- `tellus-comparison/`: competitive benchmarks with strict fairness rules (unbounded mailboxes
  everywhere, fire-and-forget sends only, identical timing boundaries); read its README before
  changing any benchmark.
- `docs/actors.md`: the authoritative top-down explanation of the core, from the `Actor` trait to
  the run loop, with links into the implementation. Read it before changing `tellus/src`; keep it
  consistent with implementation changes.
- `docs/persistence.md`: the equally authoritative contract of event-sourced persistence (feature
  `persistence`, `tellus/src/persistence/`): replay equals live execution, effects gated on
  durability, strict per-command settlement, fencing via conditional append, schema evolution via
  manifest plus schema version. `tellus/tests/persistence.rs` encodes these guarantees.
- `docs/cluster.md`: the same for the feature-gated `cluster` module, in particular which core
  guarantees carry over the network and which weaken; keep it consistent with `tellus/src/cluster`.
- `mentor/`: generated code-review artifacts, not source code.

## Architecture

The core is small (~2300 lines in `tellus/src`, plus ~2500 for persistence) but dense with
cross-file invariants:

- An actor is a state machine: `Actor::receive` (`actor.rs`) is a synchronous function from owned
  state and an `Incoming` (message or terminated signal) to `Control::Continue(next_state)` or
  `Control::Stop`. No async, no `&mut self`; each actor runs as one Tokio task.
- `actor_context.rs` holds the run loop, spawning and the termination sequence. Actors form a
  tree; termination is bottom-up, with each actor's Tokio watch channel closing (all child
  receivers dropped) as the barrier proving every descendant has terminated. The state is dropped
  before the children are stopped; only the actor value waits for the barrier, and the watchers
  are signaled last: a terminated signal must prove the actor's destructors have run.
- `mailbox.rs` is where messaging and death watch meet: one FIFO flume channel per actor, always
  unbounded underneath, with a bounded capacity enforced by a reservation counter (`quota.rs`) in
  front. Terminated signals bypass the capacity check but ride the same FIFO channel as ordinary
  messages; that shared queue is the entire mechanism behind the ordering guarantee (a terminated
  signal arrives behind all messages the terminated actor delivered to the watcher). Death watch
  itself lives in `watch.rs` (`Watcher`, `WatcherRegistry` and the `TerminatedHandler` a watcher
  runs); the mailbox half holds the registry and implements that trait for its own sender, and
  registration closes atomically with termination, so watch is race-free.
- `unwatch` is enforced on the watcher's side: the run loop drops a terminated signal whose sender
  is no longer watched before `receive` ever sees it.
- Supervision (`actor_config.rs`): errors and panics (both `init` and `receive` run under
  `catch_unwind`) are handled identically; `Restart` rebuilds only the state via `init` on the
  same actor value, retains the mailbox, and backs off exponentially while failures keep coming.
- `actor_system.rs`: `spawn_root` spawns the root through the same `spawn` path as any child and
  registers a watcher directly in the root's watcher registry; the watcher's handler resolves
  `terminated()` and owns the sender keeping the root running.
- Clustered remoting (`cluster/`, `cluster` feature) hangs off one place: `ActorRef` holds a `Sink`
  enum whose second variant is a remote sink, so `tell`, `watch` and `unwatch` stay one API. The
  module's public shape is flat for what every node calls (`start_endpoint`, `form`, `join`,
  `leave`, `members`, `down`, `register`, `lookup`, `bootstrap`, `serialize_ref`) plus one submodule
  per pluggable family, each holding a trait, its factory and the provided implementations:
  `cluster::transport` (with QUIC in `transport/quic.rs`), `cluster::codec`, `cluster::downing`,
  `cluster::failure` and `cluster::formation`. `ActorRef` is
  serializable via a process-wide endpoint singleton (`endpoint.rs`) plus a lazy registry
  (`registry.rs`) evicting through the core's watcher mechanism. All frames towards a node ride
  one lane, split into a control queue and a bounded pool of data queues over the transport's
  streams (`max_streams_per_peer`), each frame picking its queue by hashing the actor it is
  delivered to, so FIFO holds per recipient; system frames (watch, terminated, gossip) bypass the
  outbound capacity but not the order, mirroring `mailbox.rs`. A terminated signal names its
  watcher, which is what puts it on the same queue as the messages it must stay behind.
  Request-response crosses nodes via a nonce-keyed pending-replies table (`reply.rs`) beside the
  lookup table: serializing a `ReplyTo` moves its sink into the table, the far side gets a proxy
  whose `Reply` frame names the actor it is delivered to and hence rides that actor's queue,
  keying on its nonce when no actor awaits it (a dropped proxy sends `ReplyDropped` instead);
  downing a member evicts the entries stamped with it, and a terminating recipient the entries
  awaiting it. Membership (`membership.rs`) is always on: a started endpoint is *not* a
  cluster and refuses joins (`RefusalReason::NoCluster`), `form` (`endpoint.rs`) makes it a
  cluster of one and `join` makes it a member through a seed's member snapshot, applied as one
  snapshot once its last chunk arrived; joins are exclusive per endpoint and pin the cluster
  which answered, and `bootstrap` asks a `FormationProvider` (`formation.rs`, default `Majority`:
  lowest address of a majority of the discovered universe) when no resolved address is a member
  of anything, which is what stops a downed minority from re-forming inside a partition, `leave`
  (`leave.rs`) announces a departure by moving this node's own entry to Down, pushing it to every Up
  member, draining the outbound queues within `leave_timeout` and then severing
  (`leave_on_terminated` folds that into the `ActorSystem::terminated` wait, so the departure is
  queued behind the tree's terminated signals), the member list (a per-incarnation `Up < Down`
  lattice, so gossip merge is a convergent CRDT union) rides `Gossip` frames which double as
  heartbeats, non-members are refused at the handshake (retryably while gossip converges), and a
  detailed Down entry is swept after `down_retention` while its address keeps a capped incarnation
  watermark, which rides connection setup (chunked into frames within `max_frame_size`) rather than
  the heartbeat, so gossip stays the size of the cluster. Failure detection (`failure.rs`, default
  phi accrual) marks members locally unreachable per tick of `membership_loop` in `endpoint.rs`; the
  pluggable `DowningProvider` (`downing.rs`, default `KeepMajority`, which self-downs the minority
  side of a partition; `DownAfterDeadline` is the unilateral one the tests and only they use)
  decides node death, whose sequence (mark Down, close lane, quiesce the delivery gate, fail asks,
  flush synthesized signals) makes the weaker watch contract true by construction. Remote watch is
  two tier: a real termination keeps the local guarantees, a synthesized signal only promises that
  nothing from that node is delivered again. Under mutual TLS the admission (`peer.rs`) binds a
  peer's advertised IP to its certificate's IP SANs, both directions. Discovery (`discovery.rs`) is
  a point query: `register` names a local actor in a second registry keyspace evicted with the
  route, `lookup` resolves that name at an Up member through a nonce-keyed pending table, answering
  `NotAMember` for any other address. Bootstrap (`bootstrap.rs`) turns seeds into discovery:
  `cluster::bootstrap` polls a `SeedDiscovery` until the resolved set has held `min_peers` addresses
  through the settle window, then runs `join` through every address but its own, lowest first (a
  connected join graph), once; it does not keep running, and `Downed` means the process must exit
  and restart.

Changes to the run loop, mailbox or termination sequence almost always affect the ordering and
watch guarantees spelled out in `docs/actors.md`; the integration tests in `tellus/tests/` encode
those guarantees.
