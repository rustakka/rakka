#!/usr/bin/env python3
"""Sync + diff the upstream akka.net repository.

Responsibilities:

1. Ensure an upstream clone exists at ``./akka.net`` (never committed —
   see ``.gitignore``). Clone it on first run, fetch on subsequent runs.
2. Compute the diff since the last synced commit and group it by
   subsystem so reviewers know which rakka crate needs attention.
3. Emit a markdown report suitable for pasting into a PR description or
   piping into ``$GITHUB_STEP_SUMMARY``.

Usage::

    python scripts/sync-upstream.py               # clone/fetch + diff HEAD~200..HEAD
    python scripts/sync-upstream.py --since <sha> # diff since a specific commit
    python scripts/sync-upstream.py --depth 500   # shallow clone (CI)
    python scripts/sync-upstream.py --no-fetch    # offline; diff only
    python scripts/sync-upstream.py -o upstream-diff.md
"""

from __future__ import annotations

import argparse
import pathlib
import shutil
import subprocess
import sys
from collections import defaultdict
from typing import Dict, List, Optional, Tuple

DEFAULT_UPSTREAM = "https://github.com/akkadotnet/akka.net"
DEFAULT_PATH = "akka.net"
DEFAULT_RANGE = "HEAD~200..HEAD"

# upstream src/ prefix → rakka crate.  Kept in sync with PORTING.md.
SUBSYSTEM_MAP: List[Tuple[str, str]] = [
    ("src/core/Akka.TestKit", "rakka-testkit"),
    ("src/core/Akka.Remote", "rakka-remote"),
    ("src/core/Akka.Cluster", "rakka-cluster"),
    ("src/core/Akka.Persistence.Query", "rakka-persistence-query"),
    ("src/core/Akka.Persistence.TCK", "rakka-persistence-tck"),
    ("src/core/Akka.Persistence", "rakka-persistence"),
    ("src/core/Akka.Streams", "rakka-streams"),
    ("src/core/Akka.Coordination", "rakka-coordination"),
    ("src/core/Akka.Discovery", "rakka-discovery"),
    ("src/core/Akka/Configuration", "rakka-config"),
    ("src/core/Akka", "rakka-core"),
    ("src/contrib/cluster/Akka.Cluster.Tools", "rakka-cluster-tools"),
    ("src/contrib/cluster/Akka.Cluster.Sharding", "rakka-cluster-sharding"),
    ("src/contrib/cluster/Akka.Cluster.Metrics", "rakka-cluster-metrics"),
    ("src/contrib/cluster/Akka.DistributedData", "rakka-distributed-data"),
    ("src/contrib/dependencyinjection/Akka.DependencyInjection", "rakka-di"),
]


def classify(path: str) -> str:
    """Map an upstream file path to a rakka crate (or 'unmapped')."""
    if not path.startswith("src/"):
        return "non-src"
    for prefix, crate in SUBSYSTEM_MAP:
        if path.startswith(prefix + "/") or path == prefix:
            return crate
    return "unmapped"


def run(cmd: List[str], cwd: Optional[pathlib.Path] = None, check: bool = True) -> str:
    result = subprocess.run(
        cmd,
        cwd=str(cwd) if cwd else None,
        check=check,
        capture_output=True,
        text=True,
    )
    return result.stdout


def ensure_clone(
    path: pathlib.Path, upstream: str, depth: Optional[int], fetch: bool
) -> None:
    if not shutil.which("git"):
        raise RuntimeError("git is required but not on PATH")

    if not path.exists():
        print(f"→ cloning {upstream} → {path}", file=sys.stderr)
        cmd = ["git", "clone"]
        if depth:
            cmd += ["--depth", str(depth)]
        cmd += [upstream, str(path)]
        subprocess.check_call(cmd)
        return

    if not (path / ".git").exists():
        raise RuntimeError(
            f"{path} exists but is not a git checkout. "
            f"Remove it and rerun to re-clone."
        )

    if fetch:
        print(f"→ fetching latest in {path}", file=sys.stderr)
        # Handle both full and shallow clones gracefully.
        try:
            subprocess.check_call(
                ["git", "fetch", "--tags", "--prune", "origin"], cwd=str(path)
            )
        except subprocess.CalledProcessError:
            subprocess.check_call(
                ["git", "fetch", "--unshallow", "--tags", "origin"], cwd=str(path)
            )


def git_head(path: pathlib.Path) -> str:
    return run(["git", "rev-parse", "HEAD"], cwd=path).strip()


def diff_shortstat(path: pathlib.Path, rng: str) -> str:
    return run(
        ["git", "log", "--pretty=format:", "--shortstat", rng, "--", "src/"],
        cwd=path,
        check=False,
    )


def changed_files(path: pathlib.Path, rng: str) -> Dict[str, Tuple[int, int]]:
    """{path: (added, removed)} for files changed in the range under src/."""
    out = run(
        ["git", "log", "--pretty=format:", "--numstat", rng, "--", "src/"],
        cwd=path,
        check=False,
    )
    acc: Dict[str, Tuple[int, int]] = {}
    for line in out.splitlines():
        parts = line.split("\t")
        if len(parts) != 3:
            continue
        added, removed, fp = parts
        if added == "-" or removed == "-":
            continue  # binary
        fp = fp.strip()
        if not fp:
            continue
        a = int(added)
        r = int(removed)
        old = acc.get(fp, (0, 0))
        acc[fp] = (old[0] + a, old[1] + r)
    return acc


def render_markdown(
    upstream: str, clone: pathlib.Path, rng: str, head: str, files: Dict[str, Tuple[int, int]]
) -> str:
    by_crate: Dict[str, List[Tuple[str, int, int]]] = defaultdict(list)
    for fp, (a, r) in files.items():
        by_crate[classify(fp)].append((fp, a, r))

    total_files = len(files)
    total_added = sum(a for a, _ in files.values())
    total_removed = sum(r for _, r in files.values())

    out: List[str] = []
    out.append("# Upstream akka.net change analysis")
    out.append("")
    out.append(f"- upstream: `{upstream}`")
    out.append(f"- clone path: `{clone}`")
    out.append(f"- range: `{rng}`")
    out.append(f"- head: `{head}`")
    out.append(
        f"- totals: **{total_files}** files, +{total_added} / -{total_removed}"
    )
    out.append("")

    if not files:
        out.append("No changes in `src/` for the requested range. ✅")
        return "\n".join(out) + "\n"

    out.append("## Changes by rakka crate")
    out.append("")
    out.append("| rakka crate | files | +added | -removed |")
    out.append("|---|---|---|---|")
    for crate in sorted(by_crate, key=lambda c: -sum(a for _, a, _ in by_crate[c])):
        rows = by_crate[crate]
        added = sum(a for _, a, _ in rows)
        removed = sum(r for _, _, r in rows)
        out.append(f"| {crate} | {len(rows)} | +{added} | -{removed} |")
    out.append("")
    out.append("## File-level detail")
    out.append("")
    out.append("<details><summary>expand</summary>")
    out.append("")
    for crate in sorted(by_crate):
        out.append(f"### {crate}")
        out.append("")
        for fp, a, r in sorted(by_crate[crate]):
            out.append(f"- `{fp}` (+{a} / -{r})")
        out.append("")
    out.append("</details>")
    return "\n".join(out) + "\n"


def main(argv: Optional[List[str]] = None) -> int:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--upstream", default=DEFAULT_UPSTREAM)
    p.add_argument("--path", default=DEFAULT_PATH)
    p.add_argument("--since", default=None, help="base commit (default: HEAD~200)")
    p.add_argument("--depth", type=int, default=None, help="shallow clone depth")
    p.add_argument(
        "--no-fetch",
        dest="fetch",
        action="store_false",
        default=True,
        help="skip `git fetch` (offline)",
    )
    p.add_argument("-o", "--output", default=None, help="write markdown to FILE")
    args = p.parse_args(argv)

    repo_root = pathlib.Path(__file__).resolve().parent.parent
    clone = (repo_root / args.path).resolve()
    ensure_clone(clone, args.upstream, args.depth, args.fetch)

    rng = f"{args.since}..HEAD" if args.since else DEFAULT_RANGE
    head = git_head(clone)
    files = changed_files(clone, rng)
    md = render_markdown(args.upstream, clone, rng, head, files)

    if args.output:
        pathlib.Path(args.output).write_text(md, encoding="utf-8")
    else:
        sys.stdout.write(md)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
