# Changelog

All notable changes to this project are documented here. The format is
based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and
this project adheres to [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Fixed
- `pyproject.toml`: explicitly include `LICENSE` in the maturin sdist so
  PyPI's strict `License-File` metadata check passes. The `0.1.0` PyPI
  upload published all four wheels but rejected the sdist with
  `400 License-File LICENSE does not exist in distribution file`.
  Wheels cover Linux x64 (manylinux + musllinux), macOS universal2, and
  Windows x64 — sdist install (e.g. aarch64 Linux) requires the next
  release.

## [0.1.0] — 2026-05-03

### Changed
- **Project rename: `rakka` → `atomr`.** The umbrella crate, every
  `rakka-*` workspace member, the Python package, the binary names
  (`atomr-dashboard`, `atomr-profiler`), and the AI-skills plugin all
  ship under the `atomr` name. The repository moves from
  `github.com/rustakka/rakka` to `github.com/rustakka/atomr` (the GitHub
  redirect keeps old links working). The publish-name reset to `0.1.0`
  reflects that this is a new identity on crates.io and PyPI.

### Added
- `ai-skills/` — vendor-neutral skill bundle for AI coding assistants
  working on **projects that depend on atomr**. Six skills covering
  actor design, testing, troubleshooting, cluster, persistence, and
  Python bindings. Distributed alongside the repo; does not affect
  atomr's internal development workflow.
- Project hygiene: `LICENSE` (Apache-2.0), `CONTRIBUTING.md`,
  `CODE_OF_CONDUCT.md`, `SECURITY.md`, GitHub issue + PR templates,
  Dependabot configuration.

### Pre-rename history (published as `rakka-rs` / `rakka-*`)

The releases below were published under the prior `rakka-rs` and
`rakka-*` crate names, and as the `rakka` PyPI package. They remain
installable from the registries but are not maintained going forward;
new development happens under the `atomr` name.

#### [0.2.1] — 2026-04

##### Changed
- Renamed umbrella crate to `rakka-rs` on crates.io (Cargo's `package =`
  alias kept the import name `rakka`). The short name `rakka` was owned
  by an unrelated, dormant crate.
- Developer-experience polish — umbrella feature flags, crate metadata,
  CI docs flow.

##### Fixed
- CI release pipeline: throttle crates.io publishes and retry on
  rate-limit 429s.
- Correct full crates.io publish order in dependency order.
- Restrict PyPI upload to Python artifacts only.

#### [0.2.0]

##### Added
- Full Akka.NET parity sweep — every subsystem the umbrella claims has
  a working Rust implementation: core, supervision, dispatch, mailboxes,
  FSM, event stream, coordinated shutdown, remote, cluster,
  cluster-tools, cluster-sharding, cluster-metrics, distributed-data,
  persistence (with sql / redis / mongodb / cassandra / aws / azure
  adapters), persistence-query, streams, coordination, discovery, di,
  hosting, telemetry, dashboard.
- First-class Python bindings (`pip install rakka`) — Actor base class,
  async + blocking ask/tell, dispatcher strategies (`python-pinned`,
  `python-subinterpreter-pool`, `python-nogil`, `python-subprocess`),
  C-extension compatibility registry.
- Cross-runtime profiler — `cargo run -p rakka-profiler` and
  `python -m rakka.profiler` emit a shared JSON schema so Rust and
  Python paths can be compared directly.
- Release pipeline: tag-driven publishes to crates.io and PyPI, GitHub
  Releases with `rakka-dashboard` + `rakka-profiler` binaries for Linux
  x86_64 and macOS aarch64.

##### Changed
- Project rename from `rustakka` → `rakka` across crates, modules,
  documentation, and the published Python package.

[Unreleased]: https://github.com/rustakka/atomr/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/rustakka/atomr/releases/tag/v0.1.0
[0.2.1]: https://github.com/rustakka/atomr/compare/v0.2.0...v0.2.1
[0.2.0]: https://github.com/rustakka/atomr/releases/tag/v0.2.0
