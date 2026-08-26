# tellus-comparison

Messaging benchmarks comparing [tellus](../tellus) against three other Rust actor frameworks,
[kameo](https://crates.io/crates/kameo), [ractor](https://crates.io/crates/ractor) and
[weaver](https://github.com/jjimenezroda/weaver).

This package is never published and is not part of `just all`, so its dependencies stay out of
tellus's own build and out of the per-pull-request CI.

**weaver is local only.** It is unreleased and lives in a private repository, pinned in the
[workspace manifest](../Cargo.toml) by git revision over SSH, so the package only builds for
someone with access to that repository. That is why the weaver comparison lives on the local
`bench/weaver` branch: it is deliberately not wired into CI and its numbers are never published to
the comparison dashboard.

## Running

```shell
just comparison           # run the benchmarks
just comparison-report    # render the HTML report from the results
just comparison-check     # cargo check
just comparison-lint      # clippy
```

Results are written to `target/criterion-comparison`, deliberately separate from the
`target/criterion` tree used by tellus's own regression benchmarks, so the two never mix.

Runs published by CI are collected on the
[comparison dashboard](https://hseeberger.github.io/tellus/comparison/), one directory per version
tag and per manual run.

## Benchmarks

Three shapes, modeled on [`tellus/benches/messaging.rs`](../tellus/benches/messaging.rs):

- `flood`: the bench thread floods a single counting actor with 100,000 messages.
- `ping_pong`: pairs of actors play ping-pong for 1,000 rounds, with one pair and eight pairs.
- `fan_out`: the bench thread sends 100,000 messages round-robin to eight and to 32 workers.

Actor counts are fixed rather than derived from `available_parallelism`, so benchmark ids stay
stable across machines and published results remain comparable. Also unlike the tellus bench,
`fan_out` has the bench thread rather than a root actor distribute the messages.

## Why these competitors

kameo and ractor are actively released and, decisively, both run on a plain Tokio runtime like
tellus, which makes the comparison structurally meaningful. `actix` was considered and rejected despite being far
more popular: it uses its own `System`/`Arbiter` and places actors on a single-threaded arbiter by
default, so a fair comparison would require spreading actors across arbiters and would still be
architecturally apples-to-oranges. `xtra` and `coerce` are dormant.

weaver is the outlier of the four: unreleased, in a private repository, and architecturally the
odd one out, because every message is serialized into an envelope even for local delivery (see the
caveats). It runs on a plain Tokio runtime too, which is what makes it comparable at all.

## Fairness rules

The point of these benchmarks is that all four frameworks perform the *same work*:

1. **One run, one machine, back-to-back.** Numbers are only ever compared within a single run.
   Never compare figures across runs or machines.
2. **Unbounded mailboxes everywhere, or as close as the framework allows.** tellus defaults to
   unbounded, ractor is unbounded, and kameo is explicitly spawned via
   `spawn_with_mailbox(.., mailbox::unbounded())` because its default is a *bounded* mailbox of
   capacity 64, which would otherwise apply backpressure the others do not. weaver has no unbounded
   mailbox at all: capacity is a semaphore in front of two preallocated channels, so each weaver
   actor's mailbox is instead sized to exactly the number of messages that actor will receive and
   no send ever waits for a permit. tellus's own `flood/bounded` benchmark uses the same trick.
3. **Non-blocking fire-and-forget sends only.** tellus `ActorRef::tell`, kameo
   `tell(..).try_send()`, ractor `ActorRef::send_message`, weaver `ActorAddress::send_event`. No
   awaited sends (that is backpressure, a different guarantee) and no request-response calls.
   weaver's send is an `async fn`, but with the capacity of rule 2 it only ever awaits the enqueue,
   never a free slot; its command and request APIs, which do await an acknowledgement or a
   response, are exactly the request-response calls this rule excludes.
4. **Identical timing boundaries.** Every framework goes through the same `measure` helper: spawning
   happens outside the measured region, and the timer covers sending plus awaiting termination. In
   `ping_pong` that includes both actors of a pair: tellus tears the ponger down as a child of the
   pinger, kameo, ractor and weaver stop and await it in the pinger's stop hook (`on_stop`,
   `post_stop` and `on_stopped` respectively), each of which the runtime completes before the
   pinger's own termination resolves.
5. **Identical runtime**: one multi-threaded Tokio runtime, same configuration for all, and the
   default system allocator for all. weaver's documentation recommends running it on `mimalloc`,
   but the library installs no global allocator itself and a global allocator would apply to every
   framework in the process, so none is installed.
6. **Competitors get their fastest configuration** (see below).

Termination is also the correctness check: each actor only stops once it has processed exactly its
expected number of messages, so a dropped or lost message makes a benchmark hang rather than finish
early.

## Competitors are configured for speed, not defaults

kameo and ractor ship per-message instrumentation enabled by default, which tellus has no equivalent
of (tellus depends on `tracing` too, but emits nothing per message). Benchmarking them as-shipped
would charge them for an observability feature while measuring tellus without one, so both are built
with those features off:

- `kameo`: `default-features = false`, dropping `tracing` (and `macros`, which has no runtime cost).
  Measured **12.5% faster** than with defaults on `flood`.
- `ractor`: `default-features = false, features = ["tokio_runtime"]`, dropping
  `message_span_propogation`. Measured **21% faster** than with defaults on `flood`.
- `weaver`: no features enabled and no equivalent switch to turn off. Its per-message `tracing`
  callsites cost close to nothing with no subscriber installed, which is how the benchmark runs.
  It is also measured on its *direct* mailbox path (`ActorAddress::send_event`), which skips the
  supervisor hop that its own documentation calls the default choice for business traffic.

This deliberately biases the setup *in the competitors' favour*, which is the appropriate direction
for a comparison published by tellus's own maintainer. Anyone reproducing the out-of-the-box
experience should expect both to be correspondingly slower.

## Caveats

Read these before drawing conclusions from any number. The published report repeats this list,
folding the speed configuration above into it; keep both in sync with
[`src/bin/report.rs`](src/bin/report.rs).

1. **tellus's `receive` is synchronous; the other three take `async fn` handlers.** tellus
   therefore avoids allocating and polling a future per message, but in exchange it cannot await
   inside `receive`. This is a capability difference, not only a speed difference, and it favours
   tellus on exactly these microbenchmarks.
2. **tellus's mailbox is statically typed; the others erase message types.** kameo and ractor box
   messages to support their richer messaging APIs, which costs an allocation and a dynamic dispatch
   per message that tellus does not pay.
3. **weaver serializes every message, even for a local send.** Each `send_event` bincode-encodes the
   payload and wraps it in an `Envelope` carrying an owned type name, subject and `Notify`; the
   receiving actor deserializes it again before the handler runs. The other three move a Rust value
   into the mailbox. This is a deliberate design choice, the same envelope is what travels to a
   broker in distributed mode, not an implementation defect, but it dominates weaver's numbers on
   these benchmarks. weaver's own benchmarks build one `EventMessage` up front and resend that same
   `Arc`; sending one pre-encoded message 100,000 times is not the work the other three do, so this
   benchmark constructs and sends a message per iteration like everyone else.
4. **weaver is unreleased.** It is measured at a pinned git revision of a private repository, not at
   a published version, and it is the youngest of the four by a wide margin.
5. **These are messaging microbenchmarks only.** They say nothing about supervision, distribution,
   ergonomics, memory use or production readiness. kameo and ractor are mature,
   feature-rich frameworks; tellus and weaver are under active development and do far less.
6. **CI numbers come from a shared 2-core GitHub hosted runner**, so absolute figures there are not
   representative of real deployments; only the relative comparison within a run is meaningful.
7. **Written and run by tellus's maintainer.** The methodology and every line of the benchmark are in
   this package; corrections and pull requests are welcome.
