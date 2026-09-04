# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.2](https://github.com/hseeberger/tellus/compare/tellus-persistence-postgres-v0.1.1...tellus-persistence-postgres-v0.1.2) - 2026-09-04

### Other

- updated the following local packages: tellus, tellus

## [0.1.1](https://github.com/hseeberger/tellus/compare/tellus-persistence-postgres-v0.1.0...tellus-persistence-postgres-v0.1.1) - 2026-08-30

### Fixed

- harden backoff config, recovery drop and bench sizing

### Other

- harden core and persistence guarantees

## [0.1.0](https://github.com/hseeberger/tellus/releases/tag/tellus-persistence-postgres-v0.1.0) - 2026-08-27

### Added

- add event-sourced persistence

### Other

- use independent versions per crate
- install via cargo add instead of pinned versions
- publish tellus-persistence-postgres to crates.io
- refresh docs after persistence landed
