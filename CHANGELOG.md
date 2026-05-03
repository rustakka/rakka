# Changelog

All notable changes to this project are documented here. The format is
based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and
this project adheres to [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added
- `ai-skills/` — vendor-neutral skill bundle for AI coding assistants
  working on **projects that depend on rakka**. Six skills covering
  actor design, testing, troubleshooting, cluster, persistence, and
  Python bindings. Distributed alongside the repo; does not affect
  rakka's internal development workflow.
- Project hygiene: `LICENSE` (Apache-2.0), `CONTRIBUTING.md`,
  `CODE_OF_CONDUCT.md`, `SECURITY.md`, GitHub issue + PR templates,
  Dependabot configuration.

## [0.2.1] — 2026-04

### Changed
- Rename umbrella crate to `rakka-rs` on crates.io (Cargo's
  `package =` alias keeps the import name `rakka`). The short name
  `rakka` is owned by an unrelated, dormant crate.
- Developer-experience polish — umbrella feature flags, crate
  metadata, CI docs flow.

### Fixed
- CI release pipeline: throttle crates.io publishes and retry on
  rate-limit 429s.
- Correct full crates.io publish order in dependency order.
- Restrict PyPI upload to Python artifacts only.

## [0.2.0]

### Added
- Full Akka.NET parity sweep — every subsystem the umbrella claims
  has a working Rust implementation: core, supervision, dispatch,
  mailboxes, FSM, event stream, coordinated shutdown, remote, cluster,
  cluster-tools, cluster-sharding, cluster-metrics, distributed-data,
  persistence (with sql / redis / mongodb / cassandra / aws / azure
  adapters), persistence-query, streams, coordination, discovery, di,
  hosting, telemetry, dashboard.
- First-class Python bindings (`pip install rakka`) — Actor base
  class, async + blocking ask/tell, dispatcher strategies
  (`python-pinned`, `python-subinterpreter-pool`, `python-nogil`,
  `python-subprocess`), C-extension compatibility registry.
- Cross-runtime profiler — `cargo run -p rakka-profiler` and
  `python -m rakka.profiler` emit a shared JSON schema so Rust and
  Python paths can be compared directly.
- Release pipeline: tag-driven publishes to crates.io and PyPI,
  GitHub Releases with `rakka-dashboard` + `rakka-profiler` binaries
  for Linux x86_64 and macOS aarch64.

### Changed
- Project rename from `rustakka` → `rakka` across crates, modules,
  documentation, and the published Python package.

[Unreleased]: https://github.com/rustakka/rakka/compare/v0.2.1...HEAD
[0.2.1]: https://github.com/rustakka/rakka/compare/v0.2.0...v0.2.1
[0.2.0]: https://github.com/rustakka/rakka/releases/tag/v0.2.0
