# Release pipeline

`/.github/workflows/release.yml` ships atomr to three places on every
`v*` tag:

1. **GitHub Releases** — pre-built `atomr-dashboard` and `atomr-profiler`
   binaries plus all built Python wheels.
2. **crates.io** — every publishable Rust crate, in dependency order.
3. **PyPI** — platform-specific wheels (Linux x86_64/aarch64, Linux musl
   x86_64, macOS universal2, Windows x86_64) and an sdist.

## Triggering

* **Tag push** (`git push origin vX.Y.Z`) — runs the full pipeline.
  `version-bump.yml` does this automatically on every `main` push that
  contains `feat:` / `fix:` / `BREAKING CHANGE` Conventional-Commit
  subjects (or a `Release-As: x.y.z` trailer).
* **Manual** (`workflow_dispatch`) — choose `dry_run=true` for a
  rehearsal that publishes to TestPyPI and runs `cargo publish --dry-run`.
  Toggle `skip_python` / `skip_crates` to ship to only one registry.

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

## Cross-publishing constraints

* **crates.io publishes are sequential** — every dependent crate
  must wait for its dependencies to be visible. The `publish-crates`
  job orders them deliberately; if you add a new crate, slot it into
  the matching layer of that block.
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
