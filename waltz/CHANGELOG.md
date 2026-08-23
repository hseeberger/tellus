# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.1](https://github.com/hseeberger/waltz/compare/waltz-v0.1.0...waltz-v0.1.1) - 2026-08-23

### Added

- add with_* setters to ActorConfig and RestartPolicy
- add request-response via ask and reply_to
- core actors

### Fixed

- correct misleading unreachable message in scatter_gather
- reject unknown fields in config deserialization
- report termination instead of full mailbox while terminating
- honor parent stop arriving during a restart's stop_children

### Other

- add release-plz workflow and publish waltz crate
- order core items by visibility tier
- shorten root watch error message
- replace deprecated Atomic::fetch_update with try_update
- add examples from counter to device manager
- close coverage gaps in quota, termination and watch tests
- make ClosedMailbox::take_watchers consume the mailbox
- route Backoff::default through the validating constructor
- switch from log/logforth to tracing
