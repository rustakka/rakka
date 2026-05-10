# Release pipeline

> **See also:** [release-process.md](release-process.md) — the
> operator-facing reference (how to ship, conventional-commit rules,
> trampoline architecture, troubleshooting). This document focuses on
> workflow internals: jobs, matrix entries, build commands.

`/.github/workflows/release.yml` ships atomr to three places on every
`v*` tag:

1. **GitHub Releases** — pre-built `atomr-dashboard` and `atomr-profiler`
   binaries plus all built Python wheels.
2. **crates.io** — every publishable Rust crate, in dependency order.
3. **PyPI** — platform-specific wheels (Linux x86_64/aarch64, Linux musl
   x86_64, macOS universal2, Windows x86_64) and an sdist.

## Triggering

There are three paths into this pipeline; they all converge on the
same publish jobs.

* **Direct tag push** (`git push origin vX.Y.Z`) — fires
  `on: push: tags`. Use this when a human is cutting a release
  outside of the auto-bump flow.
* **Auto-bump trampoline** — `version-bump.yml` runs on every push
  to `main` and decides a SemVer bump from Conventional-Commit
  subjects (`feat:` → minor, `fix:`/`perf:`/`revert:` → patch,
  `!:`/`BREAKING CHANGE` → major; everything else — including
  `build:`, `chore:`, `docs:`, `ci:`, `test:`, `refactor:`,
  `style:` — is `skip`). When it decides to bump, it commits the
  version change, tags it, pushes, **and then explicitly dispatches
  `release.yml`** via `gh workflow run release.yml --ref vX.Y.Z
  -f dry_run=false`. The explicit dispatch is required because tag
  events authored by the default `GITHUB_TOKEN` do not fire downstream
  workflows.
* **Manual `workflow_dispatch`** — choose `dry_run=true` for a
  rehearsal that publishes to TestPyPI and runs `cargo publish --dry-run`.
  Toggle `skip_python` / `skip_crates` to ship to only one registry.
  A manual dispatch with `dry_run=false` against a `v*` tag ref also
  performs a real publish (this is the same path the trampoline takes).

### What gets published when

| Trigger | verify | binaries | wheels | GitHub Release | crates.io | PyPI |
|---|---|---|---|---|---|---|
| `push` on `v*` tag | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| `workflow_dispatch` ref=`v*` `dry_run=false` | ✓ | ✓ | ✓ | ✓ | ✓ (unless `skip_crates`) | ✓ (unless `skip_python`) |
| `workflow_dispatch` `dry_run=true` | ✓ | ✓ | ✓ | — | dry-run only | TestPyPI |

The publish jobs guard on `startsWith(github.ref, 'refs/tags/v')`, so
a `workflow_dispatch` against a branch ref will only run the verify
gate and (optionally) dry-run jobs — never a real publish.

## What gets built

### Binaries (`build-binaries`)

Cross-compiled for:

| OS | Target | Notes |
|---|---|---|
| Ubuntu | `x86_64-unknown-linux-gnu` | native cargo |
| Ubuntu | `aarch64-unknown-linux-gnu` | via `cross` |
| macOS | `x86_64-apple-darwin` | native cargo |
| macOS | `aarch64-apple-darwin` | native cargo |
| Windows | `x86_64-pc-windows-msvc` | native cargo |

### Wheels (`build-wheels`)

Built via `PyO3/maturin-action`. The action runs each target inside the
appropriate `manylinux` / `musllinux` container; the action's
`--interpreter` flag builds a wheel per CPython ABI (3.10 – 3.13).

| OS | Target | Wheel tag |
|---|---|---|
| Ubuntu | `x86_64-unknown-linux-gnu` | `manylinux_2_17_x86_64` |
| Ubuntu | `aarch64-unknown-linux-gnu` | `manylinux_2_17_aarch64` |
| Ubuntu | `x86_64-unknown-linux-musl` | `musllinux_1_2_x86_64` |
| macOS | `universal2-apple-darwin` | `macosx_*_universal2` (fat: x86_64 + arm64) |
| Windows | `x86_64-pc-windows-msvc` | `win_amd64` |

### sdist (`build-sdist`)

A single source distribution `atomr-X.Y.Z.tar.gz`, used by PyPI for
platforms that have no pre-built wheel.

## Required secrets / config

| Secret | Where | Used by |
|---|---|---|
| `CRATES_IO_TOKEN` | repo `Settings → Secrets → Actions` | `publish-crates` |
| PyPI Trusted Publisher | configured on PyPI itself, **not** as a GitHub secret | `publish-pypi` |

### PyPI Trusted Publishing setup

Trusted publishing avoids long-lived API tokens. One-time setup:

1. Create the project on https://pypi.org/manage/projects/ (or run a
   manual upload first).
2. Go to *Manage → Publishing → Add a new publisher → GitHub*.
3. Fill in:
   * Owner: `<your-gh-org>`
   * Repository: `atomr`
   * Workflow name: `release.yml`
   * Environment: `pypi`
4. Repeat for TestPyPI with environment `testpypi` if you want
   dry-run uploads.

The `publish-pypi` job already declares `permissions: id-token: write`
and `environment: pypi` so the OIDC handshake works once you've
registered the publisher.

If you'd rather use an API token, replace the
`pypa/gh-action-pypi-publish` action's `with:` block with:

```yaml
with:
  packages-dir: upload
  password: ${{ secrets.PYPI_API_TOKEN }}
  skip-existing: true
```

## Crates published

The `publish-crates` job walks every publishable crate in dependency
order. Adding a new crate? Slot it into the earliest layer whose
prerequisites have already been published, and pin its intra-workspace
deps with `{ workspace = true }` (NOT a hand-written `version = "..."`
literal) so the next bump doesn't leave a stale pin behind.

Current order (top to bottom):

1. `atomr-config`
2. `atomr-core`
3. `atomr-serialization-hyperion`
4. `atomr-macros`, `atomr-testkit`
5. `atomr-remote`, `atomr-remote-serial`
6. `atomr-persistence`, `atomr-streams`
7. `atomr-coordination`, `atomr-discovery`, `atomr-di`
8. `atomr-cluster`
9. `atomr-persistence-tck`, `atomr-persistence-query`
10. `atomr-hosting`
11. `atomr-distributed-data`, `atomr-distributed-data-lmdb`
12. `atomr-cluster-tools`, `atomr-cluster-metrics`
13. `atomr-persistence-query-inmemory`, `atomr-persistence-sql`
14. `atomr-persistence-redis`, `atomr-persistence-mongodb`
15. `atomr-persistence-cassandra`, `atomr-persistence-aws`
16. `atomr-persistence-azure`
17. `atomr-cluster-sharding`
18. `atomr-patterns`
19. `atomr-telemetry`
20. `atomr-dashboard`
21. `atomr` (umbrella)
22. `atomr-profiler` (binary + lib; published last so its `atomr` dep is already on the index)

Workspace members deliberately excluded: `xtask`, `examples/*`,
`benches/*` (all carry `publish = false`), and the `crates/py-bindings/*`
crates (they ship as a single `atomr` PyPI wheel via `maturin`, not
individual crates.io publishes).

## Cross-publishing constraints

* **crates.io publishes are sequential** — every dependent crate
  must wait for its dependencies to be visible. The `publish-crates`
  job orders them deliberately; if you add a new crate, slot it into
  the matching layer of that block.
* **`already uploaded` is treated as success** — re-tagging the same
  version (after fixing one mid-pipeline crate) is cheap; previously-
  uploaded crates skip in <1s.
* **Rate limiting** — each successful publish sleeps 30s; `429 Too
  Many Requests` triggers exponential backoff up to 6 attempts. With
  ~22 crates this caps the publish-crates job around 15 min.
* **Wheel ABI tags** are baked in by maturin from the build container,
  so each matrix entry produces a different wheel tag. If you need
  more (e.g. PyPy), add another matrix line.
* **Universal2 macOS wheels** cover both Intel and Apple Silicon in a
  single artifact; that's why we don't run separate macOS x86_64 and
  aarch64 wheel builds.
* **musllinux** is Alpine-friendly; if you don't ship to Alpine, drop
  that matrix row to halve Linux build time.

## Verifying a release locally

Dry-run a release by triggering the workflow manually:

```
gh workflow run release.yml -f dry_run=true
```

This runs the verify gate, builds every binary + wheel, and uploads
to TestPyPI (`https://test.pypi.org/p/atomr`) without touching crates.io
or production PyPI. The artifacts also land on the workflow's
*Artifacts* panel so you can download and smoke-test before tagging.
