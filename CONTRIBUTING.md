# Contributing to rakka

Thanks for your interest in contributing! rakka is a native-Rust
runtime for actor-based concurrent and distributed systems, with
first-class Python bindings. This guide covers how to get a working
checkout, the bar for changes, and how releases happen.

## Code of conduct

Participation in this project is governed by the
[Contributor Covenant](CODE_OF_CONDUCT.md). By contributing you agree
to abide by its terms.

## Reporting issues

- **Bugs:** open an issue using the *Bug report* template. Include a
  minimal reproducing snippet and the output of `rustc --version` and
  (if relevant) `python --version`.
- **Security vulnerabilities:** see [SECURITY.md](SECURITY.md). Do
  **not** open a public issue.
- **Feature requests / design discussion:** open an issue using the
  *Feature request* template. For substantial design changes, prefer
  a short proposal in the issue first; we'll iterate before code.

## Development setup

```bash
# Rust workspace
cargo build --workspace
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
cargo fmt --all -- --check

# Python bindings (requires maturin + a Python dev toolchain)
python -m venv .venv && source .venv/bin/activate
pip install maturin
maturin develop --release
pytest python/tests -v

# Docs (mkdocs-material)
pip install mkdocs-material
mkdocs serve
```

The repo uses the `xtask` pattern for project-level tasks:

```bash
cargo xtask audit       # crate boundaries + workspace invariants
cargo xtask profile     # run the cross-runtime profiler
cargo xtask verify      # the 1.0-rc readiness gate
cargo xtask bump        # bump workspace version (used by the release flow)
```

Toolchain pinning lives in `rust-toolchain.toml`; CI uses the same
versions.

## Pull requests

A good rakka PR:

1. **Stays focused.** One feature, one fix, one refactor — not all three.
2. **Includes tests.** New behavior gets a unit test or an integration
   test under the relevant `crates/*/tests/` or `python/tests/`. Bug
   fixes get a regression test.
3. **Passes the local gate** before pushing:
   ```bash
   cargo fmt --all -- --check
   cargo clippy --workspace --all-targets -- -D warnings
   cargo test --workspace
   ```
4. **Updates docs** if it changes a user-visible API. The `docs/` tree
   is the source of truth; rustdoc is the line-level reference.
5. **Updates the changelog.** Add an `[Unreleased]` entry to
   `CHANGELOG.md` describing the user-visible change.
6. **Follows commit conventions.** We use Conventional Commits style:
   `feat:`, `fix:`, `chore:`, `docs:`, `ci:`, `refactor:`, `test:`.
   Subject ≤ 72 chars; the body explains *why*.

CI runs `fmt`, `clippy`, `cargo test --workspace`, an audit/regression
check, persistence integration tests, and a docs build. PRs are not
merged until CI is green.

## Workspace layout

| Path | What's there |
|---|---|
| `crates/` | Rust workspace — one crate per subsystem (`rakka-core`, `rakka-cluster`, …) |
| `crates/py-bindings/` | PyO3 bridge crates |
| `python/rakka/` | Python package (the facade over the native extension) |
| `python/tests/` | Python integration tests |
| `examples/` | Runnable Rust examples |
| `benches/` | Criterion benches |
| `scripts/` | Cross-runtime tooling |
| `docs/` | mkdocs-material source |
| `xtask/` | Cargo xtask (audit, profile, bump, verify) |
| `ai-skills/` | Skills for AI coding assistants working on consumer projects |

## Releases

Releases are tagged from `main`. Pushing a tag matching `v*` (e.g.
`v0.3.0`, `v1.0.0-rc.1`) fires `.github/workflows/release.yml` which:

1. Runs the `cargo xtask verify` readiness gate.
2. Builds release binaries for Linux x86_64 and macOS aarch64.
3. Creates the GitHub Release with binaries attached.
4. Publishes every Rust crate to crates.io in dependency order.
5. Publishes Python wheels to PyPI.

The version-bump-and-tag step is automated by
`.github/workflows/version-bump.yml`. Maintainers trigger it via
`workflow_dispatch`.

## License

By contributing you agree that your contributions are licensed under
the [Apache License 2.0](LICENSE).
