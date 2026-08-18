# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

Tasks are defined in the [justfile](justfile):

- `just all`: check, fmt, lint, test and doc; the full local gate for every crate except `tellus-comparison`. The `tellus-persistence-postgres` tests use testcontainers, so the gate needs Docker.
- `just check` / `just lint` / `just test`: the individual steps, each a feature matrix, every run scoped to one crate: five runs of `-p tellus` (no features, `serde`, `persistence`, `persistence-tests`, all features) plus one of `-p tellus-persistence-postgres`; check and lint additionally run `-p tellus` with the `hotpath` feature. No run spans the workspace: a build containing both crates feature-unifies `tellus` with `persistence` enabled, so only scoped runs exercise the reduced-feature configurations, and only a scoped run builds `tellus-persistence-postgres` against the feature set it actually declares. `just doc` runs workspace-wide with all features.
- `just fmt`: formats Rust (nightly rustfmt, the justfile derives the matching nightly from the installed stable) and TOML (taplo). Plain `cargo fmt` is not enough; the rustfmt config uses unstable options.
- Single test: `cargo test -p tellus --test watch <test_name>` (integration tests live in `tellus/tests/`: `supervision.rs`, `termination.rs`, `watch.rs`).
- Examples: `just run-examples-hello`, `just run-examples-scatter-gather`.

Benchmarks:

- `just profile` / `just profile-alloc`: run `tellus/examples/profile.rs` with hotpath profiling, reporting per-function timings or allocation bytes for the instrumented hot path (send path, mailbox, run loop, termination). Instrumentation is gated behind the off-by-default `hotpath` feature; read the report as relative attribution, criterion stays the source of truth for absolute regressions.
- `just profile-alloc-gate`: run the profiling workload with allocation tracking and fail unless `tell`, `reserve` and `receive_incoming` allocate exactly 0 bytes; `profile-alloc-check <file>` applies that check to an existing JSON report. CI (`profile.yml`) profiles every PR against its merge base, posts the per-function comparison as an informational PR comment, and fails the build only on the zero-alloc check.
- `just profile-persistence` / `just profile-persistence-alloc`: the same for the persistence code (`tellus/examples/profile_persistence.rs`, features `hotpath` plus `persistence`): command settlement (encode, append, apply, snapshot) and recovery (snapshot load, paged read, replay) against an in-memory store. Settlement allocates by design (payload buffers, manifests), so most of the alloc report is a budget, not a zero gate; the exceptions are `apply_events` and `replay_page`, which must stay allocation-free: `just profile-persistence-alloc-gate` runs the workload and fails on a violation, `profile-persistence-alloc-check <file>` applies that check to an existing JSON report, and CI enforces it alongside the messaging zero-alloc check and includes the comparison in the profile comment.
- `just bench`: tellus's own criterion regression benchmarks, messaging (`tellus/benches/messaging.rs`) and persistence against an in-memory store (`tellus/benches/persistence.rs`, feature `persistence`); `just bench-save <baseline>` / `just bench-compare <baseline>` and the `bench-persistence-*` pair for local before/after comparisons. CI benchmarks every PR against its merge base, both benches, and fails the build on a regression at 95% confidence of at least 15% slowdown (`just bench-gate`). Pushes to `main` additionally publish both benches to the gh-pages dashboard (`dev/bench`) via `bench-bencher` and `bench-persistence-bencher`, one section per store-result step name: "Core" (messaging) and "Persistence". Adding a section means a bencher recipe plus a benchmark and a store-result step in `bench.yml`; `dev/bench/index.html` on `gh-pages` is a customized copy of the action's default page (headline plus one sub-heading per section) which the action leaves alone once it exists.
- `just comparison` plus `comparison-check` / `comparison-lint`: the `tellus-comparison` crate benchmarking tellus against kameo and ractor. It is deliberately excluded from `just all` and from per-PR CI so its dependencies stay out of tellus's build; touching it means running its own check and lint recipes.

CI enforces that a PR consists of exactly one commit; squash before pushing.

## Workspace layout

- `tellus/`: the actor framework, the only published-facing crate.
- `tellus-persistence-postgres/`: the PostgreSQL stores (`PostgresStore` implements `EventStore` and `SnapshotStore`). Tests need Docker (testcontainers) and run as part of `just all`; their image tag and credentials come from the root `docker-compose.yaml` via `composed`, so there is one place to change either. Examples via `just run-examples-event-sourced-counter` and `just run-examples-event-sourced-supervision`, which start the `postgres` service from the root `docker-compose.yaml`; `docker compose down` stops it, `down -v` resets the data.
- `tellus/src/persistence_tests.rs` (feature `persistence-tests`, implies `persistence`): the contract test suite for persistence stores; backend crates run it from their integration tests against real servers. `tellus/tests/persistence_tests.rs` validates the suite itself against a minimal in-memory store, `tellus/tests/support/in_memory_store.rs`, which the persistence bench and profiling example `include!` as well.
- `composed`: a crates.io dependency which reads image name, tag and environment variables out of a `docker-compose.yaml` at compile time, so testcontainers-based tests use the same image version and credentials as the local stack instead of hardcoding them. `tellus-persistence-postgres/tests/store.rs` holds it in a `static COMPOSE: LazyLock<Compose>` built by the `compose!` macro, whose path argument is relative to the *calling* crate's manifest. Lookups panic like assertions, naming the compose file. Deliberately exposes no `ports` or `command`: tests use the port testcontainers maps.
- `tellus-comparison/`: competitive benchmarks with strict fairness rules (unbounded mailboxes everywhere, fire-and-forget sends only, identical timing boundaries); read its README before changing any benchmark.
- `docs/actors.md`: the authoritative top-down explanation of the core, from the `Actor` trait to the run loop, with links into the implementation. Read it before changing `tellus/src`; keep it consistent with implementation changes.
- `docs/persistence.md`: the equally authoritative contract of event-sourced persistence (feature `persistence`, `tellus/src/persistence/`): replay equals live execution, effects gated on durability, strict per-command settlement, fencing via conditional append, schema evolution via manifest plus schema version. `tellus/tests/persistence.rs` encodes these guarantees.
- `mentor/`: generated code-review artifacts, not source code.

## Architecture

The core is small (~1200 lines in `tellus/src`) but dense with cross-file invariants:

- An actor is a state machine: `Actor::receive` (`actor.rs`) is a synchronous function from owned state and an `Incoming` (message or terminated signal) to `Control::Continue(next_state)` or `Control::Stop`. No async, no `&mut self`; each actor runs as one Tokio task.
- `actor_context.rs` holds the run loop, spawning and the termination sequence. Actors form a tree; termination is bottom-up, with each actor's Tokio watch channel closing (all child receivers dropped) as the barrier proving every descendant has terminated. The state is dropped before the children are stopped; only the actor value waits for the barrier, and the watchers are signaled last: a terminated signal must prove the actor's destructors have run.
- `mailbox.rs` is where messaging and death watch meet: one FIFO flume channel per actor, always unbounded underneath, with a bounded capacity enforced by a reservation counter (`quota.rs`) in front. Terminated signals bypass the capacity check but ride the same FIFO channel as ordinary messages; that shared queue is the entire mechanism behind the ordering guarantee (a terminated signal arrives behind all messages the terminated actor delivered to the watcher). Watcher registration is also owned by the mailbox and closes atomically with termination, so watch is race-free.
- `unwatch` is enforced on the watcher's side: the run loop drops a terminated signal whose sender is no longer watched before `receive` ever sees it.
- Supervision (`actor_config.rs`): errors and panics (both `init` and `receive` run under `catch_unwind`) are handled identically; `Restart` rebuilds only the state via `init` on the same actor value, retains the mailbox, and backs off exponentially in failure streaks.
- `actor_system.rs`: `spawn_root` spawns the root through the same `spawn` path as any child and registers a watcher directly in the root's watcher registry; the watcher's sink resolves `terminated()` and owns the sender keeping the root running.

Changes to the run loop, mailbox or termination sequence almost always affect the ordering and watch guarantees spelled out in `docs/actors.md`; the integration tests in `tellus/tests/` encode those guarantees.
