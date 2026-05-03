# Security Policy

## Reporting a vulnerability

We take security issues seriously. **Please do not open a public
GitHub issue for security vulnerabilities.** Instead, report them
privately so we can investigate and ship a fix before disclosure.

### How to report

Use GitHub's private vulnerability reporting:

> **Repository → Security → Report a vulnerability**

Or email the maintainers (see commit history for current
maintainers' addresses) with the subject line
`[security] rakka: <one-line summary>`.

In your report, please include:

1. A description of the issue and its impact.
2. Steps to reproduce, or a minimal proof-of-concept.
3. The affected version(s) — ideally a commit SHA or a published
   crates.io / PyPI version.
4. Any mitigations you've identified.

## What to expect

- **Acknowledgement** within 3 business days.
- **Initial assessment** (severity, scope) within 7 business days.
- **Fix timeline** depends on severity:
  - Critical (RCE, sandbox escape, auth bypass): emergency patch
    release, target ≤ 14 days.
  - High (data loss, privilege escalation): patch in the next
    scheduled release, target ≤ 30 days.
  - Medium / low: rolled into the next minor release.
- **Coordinated disclosure.** We'll work with you on a disclosure
  timeline. Public advisory typically goes out alongside the patched
  release; we credit reporters by name unless they prefer otherwise.

## Supported versions

Security fixes are issued against the latest minor release on the
`0.x` line. Older minor versions receive fixes only for critical
issues, on a best-effort basis.

| Version | Status |
|---|---|
| `0.2.x` | Supported |
| `< 0.2` | End of life |

A formal LTS policy will be published when rakka reaches `1.0`.

## Scope

In scope:

- The Rust workspace crates published to crates.io
  (`rakka-rs`, `rakka-core`, `rakka-cluster*`, `rakka-persistence*`,
  `rakka-streams`, `rakka-remote`, …).
- The Python package published to PyPI (`rakka`).
- The release artifacts (`rakka-dashboard`, `rakka-profiler` binaries).
- The CI pipeline insofar as a compromise would affect published
  artifacts.

Out of scope:

- Third-party storage backends and transports (report to those
  upstreams).
- The akka.net upstream clone under `akka.net/` (developer-local; not
  shipped).
- Examples in `examples/` are illustrative and not security-hardened.

## Hardening guidance

If you are deploying rakka in a security-sensitive environment, see
`docs/observability.md` and `docs/remoting.md` for the recommended
configuration: TLS for cross-host transport, an SBR (split-brain
resolver), authenticated discovery, and telemetry export to a SIEM.
