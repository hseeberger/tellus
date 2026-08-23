# waltz

[![license][license-badge]][license-url]
[![build][build-badge]][build-url]
[![benchmarks][benchmarks-badge]][benchmarks-url]
[![comparison][comparison-badge]][comparison-url]

[license-badge]: https://img.shields.io/github/license/hseeberger/waltz
[license-url]: https://github.com/hseeberger/waltz/blob/main/LICENSE
[build-badge]: https://img.shields.io/github/actions/workflow/status/hseeberger/waltz/ci.yml
[build-url]: https://github.com/hseeberger/waltz/actions/workflows/ci.yml
[benchmarks-badge]: https://img.shields.io/badge/benchmarks-dashboard-informational
[benchmarks-url]: https://hseeberger.github.io/waltz/dev/bench/
[comparison-badge]: https://img.shields.io/badge/comparison-dashboard-informational
[comparison-url]: https://hseeberger.github.io/waltz/comparison/

An actor framework for Rust, built on [Tokio](https://tokio.rs): typed messages, supervision
trees and death watch with an ordering guarantee. Inspired by Carl Hewitt's
[Actor Model](https://en.wikipedia.org/wiki/Actor_model) and strongly influenced by
[Akka](https://akka.io).

## Packages

- [`waltz`](waltz): the actor framework itself. See [its README](waltz/README.md) for highlights,
  getting started and the core concepts.
- [`waltz-comparison`](waltz-comparison): messaging benchmarks comparing waltz against
  [kameo](https://crates.io/crates/kameo) and [ractor](https://crates.io/crates/ractor). See
  [its README](waltz-comparison/README.md) for the methodology, fairness rules and caveats and the
  [comparison dashboard][comparison-url] for published results.

## Roadmap

waltz is under active development and its API is still settling.

Two extensions are in the works, both feature gated, so waltz stays purely local and free of their
dependencies by default:

- persistence: event sourced actors, where commands are handled against the current state, the
  events they cause are appended to an event store and only then applied, and the state is
  recovered by replay, optionally shortcut by snapshots.
- remoting: serializable actor refs, so actors on different nodes message and watch each other
  through the very same API.

## Documentation

How waltz works under the hood, top-down with links into the implementation:

- [docs/actors.md](docs/actors.md): the core, from the `Actor` trait down to the run loop.

## Development

The [justfile](justfile) defines the usual tasks; `just all` runs check, fmt, lint, test and doc.
Formatting uses nightly rustfmt options, which `just fmt` takes care of.

Messaging throughput benchmarks (criterion) run with `just bench`. On CI every pull request is
benchmarked against its merge base and the comparison posted as a comment, and every commit on
`main` is tracked over time on the [benchmark dashboard][benchmarks-url].

The benchmarks against kameo and ractor run with `just comparison`; they are excluded from
per-pull-request CI and published to the [comparison dashboard][comparison-url] on version tags
and manual runs.

## License

This code is open source software licensed under the
[Apache 2.0 License](http://www.apache.org/licenses/LICENSE-2.0.html).
