# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.2.2](https://github.com/hseeberger/tellus/compare/tellus-v0.2.1...tellus-v0.2.2) - 2026-09-04

### Other

- update Cargo.toml dependencies

## [0.2.1](https://github.com/hseeberger/tellus/compare/tellus-v0.2.0...tellus-v0.2.1) - 2026-08-30

### Fixed

- harden backoff config, recovery drop and bench sizing

### Other

- harden core and persistence guarantees
- cut nesting and duplication in core and persistence
- pin bench runtime threads and raise sampling

## [0.2.0](https://github.com/hseeberger/tellus/compare/tellus-v0.1.0...tellus-v0.2.0) - 2026-08-27

### Added

- add event-sourced persistence
- add hotpath-based profiling behind hotpath feature

### Other

- use independent versions per crate
- install via cargo add instead of pinned versions
- refresh docs after persistence landed

## [0.1.0](https://github.com/hseeberger/tellus/releases/tag/tellus-v0.1.0) - 2026-08-24

### Other

- rename ferrier to tellus
