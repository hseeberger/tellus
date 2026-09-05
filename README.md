# tellus

**Typed actors for Rust, on Tokio.**

[![Crates.io][crates-badge]][crates-url]
[![license][license-badge]][license-url]
[![build][build-badge]][build-url]
[![docs][docs-badge]][docs-url]
[![benchmarks][benchmarks-badge]][benchmarks-url]
[![comparison][comparison-badge]][comparison-url]

[crates-badge]: https://img.shields.io/crates/v/tellus
[crates-url]: https://crates.io/crates/tellus
[license-badge]: https://img.shields.io/github/license/hseeberger/tellus
[license-url]: https://github.com/hseeberger/tellus/blob/main/LICENSE
[build-badge]: https://img.shields.io/github/actions/workflow/status/hseeberger/tellus/ci.yml
[build-url]: https://github.com/hseeberger/tellus/actions/workflows/ci.yml
[docs-badge]: https://img.shields.io/docsrs/tellus/latest
[docs-url]: https://docs.rs/tellus/latest/tellus/
[benchmarks-badge]: https://img.shields.io/badge/benchmarks-dashboard-informational
[benchmarks-url]: https://hseeberger.github.io/tellus/dev/bench/
[comparison-badge]: https://img.shields.io/badge/comparison-dashboard-informational
[comparison-url]: https://hseeberger.github.io/tellus/comparison/

An actor framework for Rust, built on [Tokio](https://tokio.rs): typed messages, supervision
trees, death watch with an ordering guarantee, optional event-sourced persistence and optional
clustered remoting over QUIC. Inspired by Carl Hewitt's
[Actor Model](https://en.wikipedia.org/wiki/Actor_model) and strongly influenced by
[Akka](https://akka.io).

tellus is available on [crates.io](https://crates.io/crates/tellus):

```sh
cargo add tellus
```

## Packages

- [`tellus`](tellus): the actor framework itself. See [its README](tellus/README.md) for
  highlights, getting started and the core concepts.
- [`tellus-bootstrap-dns`](tellus-bootstrap-dns): DNS seed discovery for cluster bootstrap, SRV or
  A/AAAA records; the discovery contract and the bootstrap logic ship with `tellus` itself, behind
  the `cluster` feature.
- [`tellus-bootstrap-k8s`](tellus-bootstrap-k8s): Kubernetes seed discovery for cluster bootstrap,
  the pods matching a label selector listed through the API, which sees a pod before it is ready.
- [`tellus-persistence-postgres`](tellus-persistence-postgres): PostgreSQL event and snapshot
  stores for tellus persistence. The contract test suite any store implementation must pass ships
  with `tellus` itself, behind the `persistence-tests` feature.
- [`tellus-cluster-demo`](tellus-cluster-demo): a five node cluster running forever under
  continuous chaos, managed by Docker Compose: DNS seed discovery, crashes, departures and
  network partitions once a cycle, and a verifier checking convergence and cross-node messaging
  in every quiet window. See [its README](tellus-cluster-demo/README.md).
- [`tellus-comparison`](tellus-comparison): messaging benchmarks comparing tellus against
  [kameo](https://crates.io/crates/kameo) and [ractor](https://crates.io/crates/ractor). See
  [its README](tellus-comparison/README.md) for the methodology, fairness rules and caveats and
  the [comparison dashboard][comparison-url] for published results.

## Status

tellus is under active development and its API is still settling.

Both extensions have landed: event-sourced actors and serializable actor refs, so actors on
different nodes message and watch each other through the same API. Both are feature gated behind
`persistence` and `cluster`, so tellus stays purely local and free of their dependencies by
default. Remoting is clustered: nodes join through seed addresses, the member list is gossiped, and
a pluggable failure detector plus downing provider replace vanished members with synthesized
terminated signals, while a node shutting down cleanly announces its own departure.

The open items on the remoting side are listed under the limitations in
[docs/cluster.md](docs/cluster.md), the main one being a cluster-wide receptionist on top of the
per-node discovery registry.

## Documentation

How tellus works internally, top-down with links into the implementation:

- [docs/actors.md](docs/actors.md): the core, from the `Actor` trait down to the run loop.
- [docs/persistence.md](docs/persistence.md): event-sourced persistence, from the `EventSourced`
  trait to recovery, fencing and schema evolution.
- [docs/cluster.md](docs/cluster.md): the `cluster` feature, from cluster membership and gossip
  to the wire model, in particular which of the core guarantees carry over the network and which
  weaken.

## Development

The [justfile](justfile) defines the usual tasks; `just all` runs check, fmt, lint, test and doc,
each across the feature combinations. Formatting uses nightly rustfmt options, which `just fmt`
takes care of. The `tellus-persistence-postgres` tests run against a container, so `just all`
needs Docker.

Regression benchmarks (criterion) run with `just bench`: messaging, and persistence against an
in-memory store. On CI every pull request is benchmarked against its merge base and the comparison
posted as a comment, and every commit on `main` is tracked over time on the [benchmark
dashboard][benchmarks-url].

The benchmarks against kameo and ractor run with `just comparison`; they are excluded from
per-pull-request CI and published to the [comparison dashboard][comparison-url] on version tags
and manual runs.

The cluster demo runs with `just cluster-demo-up`, which needs Docker; it is a soak test rather
than a gate, so it is excluded from `just all` and from CI.

## License

This code is open source software licensed under the
[Apache 2.0 License](http://www.apache.org/licenses/LICENSE-2.0.html).
