# tellus

**Actors for Rust, on solid ground.**

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
trees, death watch with an ordering guarantee and optional event-sourced persistence. Inspired by
Carl Hewitt's [Actor Model](https://en.wikipedia.org/wiki/Actor_model) and strongly influenced by
[Akka](https://akka.io).

tellus is available on [crates.io](https://crates.io/crates/tellus):

```sh
cargo add tellus
```

## Packages

- [`tellus`](tellus): the actor framework itself. See [its README](tellus/README.md) for highlights,
  getting started and the core concepts.
- [`tellus-persistence-postgres`](tellus-persistence-postgres): PostgreSQL event and snapshot
  stores for tellus persistence. The contract test suite any store implementation must pass ships
  with `tellus` itself, behind the `persistence-tests` feature.
- [`tellus-comparison`](tellus-comparison): messaging benchmarks comparing tellus against
  [kameo](https://crates.io/crates/kameo) and [ractor](https://crates.io/crates/ractor). See
  [its README](tellus-comparison/README.md) for the methodology, fairness rules and caveats and the
  [comparison dashboard][comparison-url] for published results.

## Roadmap

tellus is under active development and its API is still settling.

Event-sourced persistence has landed, feature gated behind `persistence`, so tellus stays purely
local and free of its dependencies by default.

Remoting is next, feature gated the same way: serializable actor refs, so actors on different nodes
message and watch each other through the very same API.

## Documentation

How tellus works under the hood, top-down with links into the implementation:

- [docs/actors.md](docs/actors.md): the core, from the `Actor` trait down to the run loop.
- [docs/persistence.md](docs/persistence.md): event-sourced persistence, from the `EventSourced`
  trait to recovery, fencing and schema evolution.

## Development

The [justfile](justfile) defines the usual tasks; `just all` runs check, fmt, lint, test and doc.
Formatting uses nightly rustfmt options, which `just fmt` takes care of. The
`tellus-persistence-postgres` tests run against a container, so `just all` needs Docker.

Regression benchmarks (criterion) run with `just bench`: messaging, and persistence against an
in-memory store. On CI every pull request is benchmarked against its merge base and the comparison
posted as a comment, and every commit on `main` is tracked over time on the
[benchmark dashboard][benchmarks-url].

The benchmarks against kameo and ractor run with `just comparison`; they are excluded from
per-pull-request CI and published to the [comparison dashboard][comparison-url] on version tags
and manual runs.

## License

This code is open source software licensed under the
[Apache 2.0 License](http://www.apache.org/licenses/LICENSE-2.0.html).
