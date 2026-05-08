//! `xtask` — developer tooling for the atomr workspace.
//!
//! Subcommands:
//! * `parity` — emit a presence report for the workspace crates.
//! * `audit` — count placeholder/anti-pattern sentinels per crate
//!   (baseline tracker; CI fails on regression).
//! * `profile` — run the actor perf profiler (rust only).
//! * `dashboard` — run the atomr-dashboard with embed-ui.

use std::collections::BTreeMap;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{anyhow, Context, Result};

fn main() -> Result<()> {
    let mut args = env::args().skip(1);
    let cmd = args.next().unwrap_or_else(|| "help".into());
    match cmd.as_str() {
        "parity" => parity(),
        "audit" => audit(args.collect()),
        "verify" => verify(),
        "soak" => soak(args.collect()),
        "bump" => bump(args.collect()),
        "profile" => profile(args.collect()),
        "dashboard" => dashboard(args.collect()),
        "help" | "-h" | "--help" => {
            print_help();
            Ok(())
        }
        other => Err(anyhow!("unknown xtask subcommand: {other}")),
    }
}

fn print_help() {
    println!("atomr xtask");
    println!();
    println!("USAGE:");
    println!("  cargo xtask <subcommand>");
    println!();
    println!("SUBCOMMANDS:");
    println!("  parity                          regenerate docs/reports/parity-presence.json");
    println!("  audit [--check] [--json <out>]  count anti-pattern sentinels per crate");
    println!("  verify                          run build + test + clippy + audit-check (1.0-rc gate)");
    println!("  soak [--hours <n>]              run the workspace test suite in a loop for n hours");
    println!("  bump <patch|minor|major|--pre <id>|--set <ver>>");
    println!("                                  bump workspace + python version, refresh Cargo.lock");
    println!("  profile [extra args...]         run the actor perf profiler (rust only)");
    println!("  dashboard [extra args...]       run atomr-dashboard with embed-ui + common features");
    println!("  help                            print this help");
}

fn dashboard(mut extra: Vec<String>) -> Result<()> {
    if extra.first().map(|s| s.as_str()) == Some("--") {
        extra.remove(0);
    }
    let features = std::env::var("ATOMR_DASHBOARD_FEATURES")
        .unwrap_or_else(|_| "bin,embed-ui,aggregator,metrics-prometheus".into());
    let status = Command::new(env!("CARGO"))
        .args(["run", "-q", "-p", "atomr-dashboard", "--features", &features, "--"])
        .args(&extra)
        .status()
        .context("spawning cargo run for atomr-dashboard")?;
    if !status.success() {
        return Err(anyhow!("atomr-dashboard exited with {status}"));
    }
    Ok(())
}

fn profile(mut extra: Vec<String>) -> Result<()> {
    if extra.first().map(|s| s.as_str()) == Some("--") {
        extra.remove(0);
    }
    let status = Command::new(env!("CARGO"))
        .args(["run", "--release", "-q", "-p", "atomr-profiler", "--"])
        .args(&extra)
        .status()
        .context("spawning cargo run for atomr-profiler")?;
    if !status.success() {
        return Err(anyhow!("atomr-profiler exited with {status}"));
    }
    Ok(())
}

fn bump(args: Vec<String>) -> Result<()> {
    // Phase 15.F + version-bump skill. Updates the workspace version
    // in `Cargo.toml`, the python version in `pyproject.toml`, the
    // workspace `Cargo.lock`, and prints the new version. The user
    // (or a CI hook) then commits + tags + pushes.
    let mut iter = args.into_iter();
    let arg = iter
        .next()
        .ok_or_else(|| anyhow!("usage: bump <patch|minor|major> | bump --pre <id> | bump --set <version>"))?;
    let cargo_toml = std::path::Path::new("Cargo.toml");
    let pyproject = std::path::Path::new("pyproject.toml");
    let current = read_workspace_version(cargo_toml)?;
    let next = match arg.as_str() {
        "patch" => semver_bump(&current, BumpKind::Patch)?,
        "minor" => semver_bump(&current, BumpKind::Minor)?,
        "major" => semver_bump(&current, BumpKind::Major)?,
        "--pre" => {
            let id = iter.next().ok_or_else(|| anyhow!("--pre requires <id>"))?;
            semver_bump(&current, BumpKind::Pre(id))?
        }
        "--set" => iter.next().ok_or_else(|| anyhow!("--set requires <version>"))?,
        other => return Err(anyhow!("unknown bump arg: {other}")),
    };
    println!("{} -> {}", current, next);
    write_workspace_version(cargo_toml, &next)?;
    write_workspace_deps_versions(cargo_toml, &current, &next)?;
    write_member_crate_pins(&current, &next)?;
    if pyproject.exists() {
        write_pyproject_version(pyproject, &next)?;
    }
    // Refresh Cargo.lock — `cargo metadata` is enough to rewrite it.
    let _ = Command::new(env!("CARGO")).args(["update", "--workspace"]).status();
    println!("ATOMR_NEW_VERSION={next}");
    Ok(())
}

#[derive(Debug)]
enum BumpKind {
    Patch,
    Minor,
    Major,
    Pre(String),
}

fn semver_bump(current: &str, kind: BumpKind) -> Result<String> {
    // Strip any pre-release tag for component arithmetic; preserve
    // the result of explicit --pre overrides.
    let (core, _pre) = match current.split_once('-') {
        Some((c, p)) => (c, Some(p)),
        None => (current, None),
    };
    let parts: Vec<&str> = core.split('.').collect();
    if parts.len() != 3 {
        return Err(anyhow!("version `{current}` is not MAJOR.MINOR.PATCH"));
    }
    let mut major: u64 = parts[0].parse().context("major")?;
    let mut minor: u64 = parts[1].parse().context("minor")?;
    let mut patch: u64 = parts[2].parse().context("patch")?;
    let next = match kind {
        BumpKind::Patch => {
            patch += 1;
            format!("{major}.{minor}.{patch}")
        }
        BumpKind::Minor => {
            minor += 1;
            patch = 0;
            format!("{major}.{minor}.{patch}")
        }
        BumpKind::Major => {
            major += 1;
            minor = 0;
            patch = 0;
            format!("{major}.{minor}.{patch}")
        }
        BumpKind::Pre(id) => format!("{major}.{minor}.{patch}-{id}"),
    };
    Ok(next)
}

fn read_workspace_version(path: &std::path::Path) -> Result<String> {
    let text = std::fs::read_to_string(path)?;
    // Find `[workspace.package]` block.
    let block_start = text
        .find("[workspace.package]")
        .ok_or_else(|| anyhow!("no [workspace.package] block in {}", path.display()))?;
    let block_end = text[block_start..].find("\n[").map(|i| block_start + i).unwrap_or(text.len());
    let block = &text[block_start..block_end];
    for line in block.lines() {
        let trimmed = line.trim_start();
        if let Some(rest) = trimmed.strip_prefix("version") {
            let after_eq = rest.split_once('=').map(|(_, v)| v.trim()).unwrap_or("");
            let value = after_eq.trim_matches('"').trim_matches('\'');
            return Ok(value.to_string());
        }
    }
    Err(anyhow!("no version key in [workspace.package]"))
}

fn write_workspace_version(path: &std::path::Path, version: &str) -> Result<()> {
    let text = std::fs::read_to_string(path)?;
    // Replace the first `version = "..."` after `[workspace.package]`.
    let block_start =
        text.find("[workspace.package]").ok_or_else(|| anyhow!("no [workspace.package] block"))?;
    let after_block = &text[block_start..];
    let local_idx = after_block.find("version = ").ok_or_else(|| anyhow!("no version line"))?;
    let abs = block_start + local_idx;
    let line_end = text[abs..].find('\n').map(|i| abs + i).unwrap_or(text.len());
    let new_line = format!("version = \"{version}\"");
    let mut out = String::with_capacity(text.len() + new_line.len());
    out.push_str(&text[..abs]);
    out.push_str(&new_line);
    out.push_str(&text[line_end..]);
    std::fs::write(path, out)?;
    Ok(())
}

/// Bumps the `version = "<prev>"` pin on every internal path-dep line
/// inside `[workspace.dependencies]`. The release pipeline rejects a
/// crate whose internal deps still resolve to an older version, so this
/// must move in lockstep with the workspace version.
fn write_workspace_deps_versions(path: &std::path::Path, prev: &str, next: &str) -> Result<()> {
    let text = std::fs::read_to_string(path)?;
    let block_start = match text.find("[workspace.dependencies]") {
        Some(i) => i,
        None => return Ok(()),
    };
    // The block ends at the next top-level header or EOF.
    let after = &text[block_start + "[workspace.dependencies]".len()..];
    let block_len = after.find("\n[").map(|i| i + 1).unwrap_or(after.len());
    let head = &text[..block_start];
    let block = &text[block_start..block_start + "[workspace.dependencies]".len() + block_len];
    let tail = &text[block_start + "[workspace.dependencies]".len() + block_len..];

    let needle = format!("version = \"{prev}\"");
    let replacement = format!("version = \"{next}\"");
    let mut new_block = String::with_capacity(block.len());
    for line in block.split_inclusive('\n') {
        if line.contains("path = \"crates/") && line.contains(&needle) {
            new_block.push_str(&line.replace(&needle, &replacement));
        } else {
            new_block.push_str(line);
        }
    }
    let mut out = String::with_capacity(text.len());
    out.push_str(head);
    out.push_str(&new_block);
    out.push_str(tail);
    std::fs::write(path, out)?;
    Ok(())
}

/// Walk every `crates/*/Cargo.toml` and bump any `version = "<prev>"` pin
/// that sits on the same dependency line as a `path = "../..."`. This
/// catches member crates that pin internal deps directly in their own
/// `[dependencies]` block instead of going through
/// `[workspace.dependencies]` — without this the release pipeline's
/// verify gate fails with `failed to select a version for the
/// requirement <crate> = "^<prev>"`.
fn write_member_crate_pins(prev: &str, next: &str) -> Result<()> {
    let crates_dir = std::path::Path::new("crates");
    if !crates_dir.is_dir() {
        return Ok(());
    }
    let needle = format!("version = \"{prev}\"");
    let replacement = format!("version = \"{next}\"");
    for entry in std::fs::read_dir(crates_dir)? {
        let entry = entry?;
        if !entry.file_type()?.is_dir() {
            continue;
        }
        let cargo = entry.path().join("Cargo.toml");
        if !cargo.exists() {
            continue;
        }
        let text = std::fs::read_to_string(&cargo)?;
        let mut changed = false;
        let mut out = String::with_capacity(text.len());
        for line in text.split_inclusive('\n') {
            if line.contains("path = \"../") && line.contains(&needle) {
                out.push_str(&line.replace(&needle, &replacement));
                changed = true;
            } else {
                out.push_str(line);
            }
        }
        if changed {
            std::fs::write(&cargo, out)?;
        }
    }
    Ok(())
}

fn write_pyproject_version(path: &std::path::Path, version: &str) -> Result<()> {
    let text = std::fs::read_to_string(path)?;
    let mut replaced = false;
    let mut out = String::with_capacity(text.len());
    for line in text.lines() {
        let trimmed = line.trim_start();
        if !replaced && trimmed.starts_with("version") && trimmed.contains('=') {
            out.push_str(&format!("version = \"{version}\"\n"));
            replaced = true;
        } else {
            out.push_str(line);
            out.push('\n');
        }
    }
    if !replaced {
        return Err(anyhow!("no version line in pyproject.toml"));
    }
    std::fs::write(path, out)?;
    Ok(())
}

fn soak(args: Vec<String>) -> Result<()> {
    // Phase 15.D — runs `cargo test --workspace` in a loop until the
    // requested duration expires, capturing iteration count + first
    // failure. Designed for the nightly CI job; default 1 hour to
    // keep local invocations practical.
    let mut hours: f64 = 1.0;
    let mut iter_args = args.into_iter();
    while let Some(a) = iter_args.next() {
        match a.as_str() {
            "--" => continue,
            "--hours" => {
                hours = iter_args
                    .next()
                    .ok_or_else(|| anyhow!("--hours requires a value"))?
                    .parse()
                    .context("parsing --hours")?;
            }
            other => return Err(anyhow!("unknown soak flag: {other}")),
        }
    }
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs_f64(hours * 3600.0);
    let mut iteration = 0u32;
    let mut failures = 0u32;
    println!("==> soak: running cargo test --workspace for {hours} hour(s)");
    while std::time::Instant::now() < deadline {
        iteration += 1;
        let status = Command::new(env!("CARGO"))
            .args(["test", "--workspace", "--quiet"])
            .status()
            .with_context(|| format!("spawning cargo test (iteration {iteration})"))?;
        if !status.success() {
            failures += 1;
            eprintln!("[iter {iteration}] FAILED ({status})");
        } else {
            println!("[iter {iteration}] ok");
        }
    }
    println!("soak: {iteration} iterations, {failures} failure(s) over {:.2}h", hours);
    if failures > 0 {
        return Err(anyhow!("{failures} soak iterations failed"));
    }
    Ok(())
}

fn verify() -> Result<()> {
    // Phase 15 of `docs/full-port-plan.md`. The 1.0-rc gate. Each
    // step is a hard fail.
    let cargo = env!("CARGO");
    let steps: Vec<(&str, &[&str])> = vec![
        ("cargo build --workspace", &["build", "--workspace"]),
        ("cargo test --workspace --quiet", &["test", "--workspace", "--quiet"]),
        (
            "cargo clippy --workspace --all-targets -- -D warnings",
            &["clippy", "--workspace", "--all-targets", "--", "-D", "warnings"],
        ),
    ];
    for (label, args) in &steps {
        println!("==> {label}");
        let status =
            Command::new(cargo).args(args.iter()).status().with_context(|| format!("spawning `{label}`"))?;
        if !status.success() {
            return Err(anyhow!("{label} failed: {status}"));
        }
    }
    println!("==> cargo xtask audit --check");
    audit(vec!["--check".into()])?;
    println!("\nverify: OK");
    Ok(())
}

fn parity() -> Result<()> {
    // Presence-only machine-readable report. The human-curated
    // depth-graded `docs/parity.md` is hand-maintained against the
    // 2026-04 audit baseline; Phase 0 deliberately stops auto-overwriting
    // it. A future xtask phase will compute depth grades from the audit
    // counts + LOC ratios.
    let crates_dir = Path::new("crates");
    let mut rust_crates = Vec::new();
    let mut py_crates = Vec::new();

    if crates_dir.is_dir() {
        for entry in fs::read_dir(crates_dir)? {
            let entry = entry?;
            if !entry.path().is_dir() {
                continue;
            }
            let name = entry.file_name().to_string_lossy().into_owned();
            if name == "py-bindings" {
                continue;
            }
            rust_crates.push(name);
        }
        let py_dir = crates_dir.join("py-bindings");
        if py_dir.is_dir() {
            for entry in fs::read_dir(&py_dir)? {
                let entry = entry?;
                if !entry.path().is_dir() {
                    continue;
                }
                py_crates.push(entry.file_name().to_string_lossy().into_owned());
            }
        }
    }

    rust_crates.sort();
    py_crates.sort();

    let mut json = String::from("{\n  \"rust\": [");
    for (i, c) in rust_crates.iter().enumerate() {
        if i > 0 {
            json.push(',');
        }
        json.push_str(&format!("\n    \"{c}\""));
    }
    json.push_str("\n  ],\n  \"python\": [");
    for (i, c) in py_crates.iter().enumerate() {
        if i > 0 {
            json.push(',');
        }
        json.push_str(&format!("\n    \"{c}\""));
    }
    json.push_str("\n  ]\n}\n");

    fs::create_dir_all("docs/reports")?;
    fs::write("docs/reports/parity-presence.json", json)?;
    println!(
        "wrote docs/reports/parity-presence.json ({} rust + {} python crates)",
        rust_crates.len(),
        py_crates.len()
    );
    println!("note: docs/parity.md is hand-maintained with depth grades; not regenerated.");
    Ok(())
}

// -- audit ----------------------------------------------------------------

#[derive(Default, Clone)]
struct CrateCounts {
    files: usize,
    loc: usize,
    unwrap_used: usize,
    expect_used: usize,
    panic_macro: usize,
    todo_macro: usize,
    unimplemented_macro: usize,
    box_dyn_any: usize,
    placeholder_marker: usize,
    stub_comment: usize,
    placeholder_comment: usize,
    println_macro: usize,
    eprintln_macro: usize,
    dbg_macro: usize,
}

impl CrateCounts {
    fn add(&mut self, other: &CrateCounts) {
        self.files += other.files;
        self.loc += other.loc;
        self.unwrap_used += other.unwrap_used;
        self.expect_used += other.expect_used;
        self.panic_macro += other.panic_macro;
        self.todo_macro += other.todo_macro;
        self.unimplemented_macro += other.unimplemented_macro;
        self.box_dyn_any += other.box_dyn_any;
        self.placeholder_marker += other.placeholder_marker;
        self.stub_comment += other.stub_comment;
        self.placeholder_comment += other.placeholder_comment;
        self.println_macro += other.println_macro;
        self.eprintln_macro += other.eprintln_macro;
        self.dbg_macro += other.dbg_macro;
    }
}

fn audit(args: Vec<String>) -> Result<()> {
    let mut check_mode = false;
    let mut json_out: Option<PathBuf> = None;
    let mut iter = args.into_iter();
    while let Some(a) = iter.next() {
        match a.as_str() {
            "--" => continue,
            "--check" => check_mode = true,
            "--json" => {
                json_out = Some(PathBuf::from(
                    iter.next().ok_or_else(|| anyhow!("--json requires a path argument"))?,
                ));
            }
            other => return Err(anyhow!("unknown audit flag: {other}")),
        }
    }

    let crates_dir = Path::new("crates");
    if !crates_dir.is_dir() {
        return Err(anyhow!("no crates/ directory found (cwd must be workspace root)"));
    }

    let mut per_crate: BTreeMap<String, CrateCounts> = BTreeMap::new();
    let crate_dirs = collect_crate_dirs(crates_dir)?;
    for (name, dir) in crate_dirs {
        let counts = audit_crate(&dir)?;
        per_crate.insert(name, counts);
    }

    let mut total = CrateCounts::default();
    for c in per_crate.values() {
        total.add(c);
    }

    print_audit_table(&per_crate, &total);

    if let Some(path) = json_out.as_deref() {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(path, audit_json(&per_crate, &total))?;
        println!("\nwrote {}", path.display());
    }

    if check_mode {
        let baseline_path = Path::new("docs/reports/audit-2026-04.json");
        if !baseline_path.exists() {
            return Err(anyhow!(
                "--check requires {}; run `cargo xtask audit --json {}` first",
                baseline_path.display(),
                baseline_path.display()
            ));
        }
        let baseline_text = fs::read_to_string(baseline_path)?;
        let mut regressions = Vec::new();
        for (name, counts) in &per_crate {
            let baseline = parse_json_crate(&baseline_text, name).unwrap_or_default();
            check_metric(&mut regressions, name, "unwrap_used", counts.unwrap_used, baseline.unwrap_used);
            check_metric(&mut regressions, name, "expect_used", counts.expect_used, baseline.expect_used);
            check_metric(&mut regressions, name, "panic_macro", counts.panic_macro, baseline.panic_macro);
            check_metric(&mut regressions, name, "todo_macro", counts.todo_macro, baseline.todo_macro);
            check_metric(
                &mut regressions,
                name,
                "unimplemented_macro",
                counts.unimplemented_macro,
                baseline.unimplemented_macro,
            );
            check_metric(&mut regressions, name, "box_dyn_any", counts.box_dyn_any, baseline.box_dyn_any);
            check_metric(
                &mut regressions,
                name,
                "placeholder_marker",
                counts.placeholder_marker,
                baseline.placeholder_marker,
            );
            check_metric(&mut regressions, name, "stub_comment", counts.stub_comment, baseline.stub_comment);
            check_metric(
                &mut regressions,
                name,
                "placeholder_comment",
                counts.placeholder_comment,
                baseline.placeholder_comment,
            );
            check_metric(
                &mut regressions,
                name,
                "println_macro",
                counts.println_macro,
                baseline.println_macro,
            );
            check_metric(
                &mut regressions,
                name,
                "eprintln_macro",
                counts.eprintln_macro,
                baseline.eprintln_macro,
            );
            check_metric(&mut regressions, name, "dbg_macro", counts.dbg_macro, baseline.dbg_macro);
        }
        if !regressions.is_empty() {
            eprintln!("\naudit regressions vs baseline:");
            for r in &regressions {
                eprintln!("  {r}");
            }
            return Err(anyhow!("{} audit regression(s)", regressions.len()));
        }
        println!("\naudit: no regressions vs {}", baseline_path.display());
    }

    Ok(())
}

fn check_metric(out: &mut Vec<String>, crate_name: &str, metric: &str, current: usize, baseline: usize) {
    if current > baseline {
        out.push(format!("{crate_name}: {metric} {baseline} -> {current} (+{})", current - baseline));
    }
}

fn collect_crate_dirs(crates_dir: &Path) -> Result<Vec<(String, PathBuf)>> {
    let mut out = Vec::new();
    for entry in fs::read_dir(crates_dir)? {
        let entry = entry?;
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }
        let name = entry.file_name().to_string_lossy().into_owned();
        if name == "py-bindings" {
            let py_root = path;
            for sub in fs::read_dir(&py_root)? {
                let sub = sub?;
                let p = sub.path();
                if !p.is_dir() {
                    continue;
                }
                let n = sub.file_name().to_string_lossy().into_owned();
                out.push((format!("py-bindings/{n}"), p));
            }
        } else {
            out.push((name, path));
        }
    }
    out.sort_by(|a, b| a.0.cmp(&b.0));
    Ok(out)
}

fn audit_crate(crate_dir: &Path) -> Result<CrateCounts> {
    let src = crate_dir.join("src");
    if !src.is_dir() {
        return Ok(CrateCounts::default());
    }
    let mut counts = CrateCounts::default();
    walk_rs(&src, &mut counts)?;
    Ok(counts)
}

fn walk_rs(dir: &Path, counts: &mut CrateCounts) -> Result<()> {
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            walk_rs(&path, counts)?;
            continue;
        }
        if path.extension().and_then(|s| s.to_str()) != Some("rs") {
            continue;
        }
        let text = fs::read_to_string(&path)?;
        scan_file(&text, counts);
    }
    Ok(())
}

fn scan_file(text: &str, counts: &mut CrateCounts) {
    counts.files += 1;
    let mut in_test_module = false;
    let mut depth = 0i32;
    let mut test_depth_start = i32::MAX;
    for raw_line in text.lines() {
        counts.loc += 1;
        let line = raw_line.trim_start();

        // Skip comment-only lines for code-pattern counts, but still
        // scan them for placeholder/stub markers.
        let is_comment = line.starts_with("//");

        if line.starts_with("#[cfg(test)]") {
            // Next `mod` or `fn` opens a test region; track via brace depth.
            test_depth_start = depth;
            in_test_module = true;
        }
        // Approximate brace tracking — good enough for excluding test
        // modules from anti-pattern counts.
        for ch in raw_line.chars() {
            match ch {
                '{' => depth += 1,
                '}' => {
                    depth -= 1;
                    if in_test_module && depth <= test_depth_start {
                        in_test_module = false;
                        test_depth_start = i32::MAX;
                    }
                }
                _ => {}
            }
        }

        if !is_comment {
            // Anti-pattern markers: only count in non-test code.
            if !in_test_module {
                if contains_call(line, ".unwrap(") {
                    counts.unwrap_used += 1;
                }
                if contains_call(line, ".expect(") {
                    counts.expect_used += 1;
                }
                if contains_macro(line, "panic!") {
                    counts.panic_macro += 1;
                }
                if contains_macro(line, "todo!") {
                    counts.todo_macro += 1;
                }
                if contains_macro(line, "unimplemented!") {
                    counts.unimplemented_macro += 1;
                }
                if line.contains("Box<dyn Any") {
                    counts.box_dyn_any += 1;
                }
                if contains_macro(line, "println!") {
                    counts.println_macro += 1;
                }
                if contains_macro(line, "eprintln!") {
                    counts.eprintln_macro += 1;
                }
                if contains_macro(line, "dbg!") {
                    counts.dbg_macro += 1;
                }
            }
        }

        if line.contains("__placeholder__") {
            counts.placeholder_marker += 1;
        }
        let lower = line.to_ascii_lowercase();
        if is_comment && lower.contains("// stub") {
            counts.stub_comment += 1;
        }
        if is_comment && lower.contains("// placeholder") {
            counts.placeholder_comment += 1;
        }
    }
}

fn contains_call(line: &str, needle: &str) -> bool {
    line.contains(needle)
}

fn contains_macro(line: &str, needle: &str) -> bool {
    if let Some(idx) = line.find(needle) {
        // Make sure prior char isn't an identifier char (avoid matching
        // e.g. `safe_println!` or method names).
        let before = line[..idx].chars().last();
        match before {
            None => true,
            Some(c) => !c.is_alphanumeric() && c != '_',
        }
    } else {
        false
    }
}

fn print_audit_table(per_crate: &BTreeMap<String, CrateCounts>, total: &CrateCounts) {
    let header = [
        "crate", "files", "LOC", "unwrap", "expect", "panic", "todo", "unimpl", "Box<Any", "PHldr", "stub//",
        "phldr//", "println", "eprint", "dbg",
    ];
    println!(
        "{:<32} {:>5} {:>6} {:>6} {:>6} {:>5} {:>4} {:>6} {:>7} {:>5} {:>6} {:>7} {:>7} {:>6} {:>4}",
        header[0],
        header[1],
        header[2],
        header[3],
        header[4],
        header[5],
        header[6],
        header[7],
        header[8],
        header[9],
        header[10],
        header[11],
        header[12],
        header[13],
        header[14],
    );
    for (name, c) in per_crate {
        println!(
            "{:<32} {:>5} {:>6} {:>6} {:>6} {:>5} {:>4} {:>6} {:>7} {:>5} {:>6} {:>7} {:>7} {:>6} {:>4}",
            name,
            c.files,
            c.loc,
            c.unwrap_used,
            c.expect_used,
            c.panic_macro,
            c.todo_macro,
            c.unimplemented_macro,
            c.box_dyn_any,
            c.placeholder_marker,
            c.stub_comment,
            c.placeholder_comment,
            c.println_macro,
            c.eprintln_macro,
            c.dbg_macro,
        );
    }
    println!(
        "{:<32} {:>5} {:>6} {:>6} {:>6} {:>5} {:>4} {:>6} {:>7} {:>5} {:>6} {:>7} {:>7} {:>6} {:>4}",
        "TOTAL",
        total.files,
        total.loc,
        total.unwrap_used,
        total.expect_used,
        total.panic_macro,
        total.todo_macro,
        total.unimplemented_macro,
        total.box_dyn_any,
        total.placeholder_marker,
        total.stub_comment,
        total.placeholder_comment,
        total.println_macro,
        total.eprintln_macro,
        total.dbg_macro,
    );
}

fn audit_json(per_crate: &BTreeMap<String, CrateCounts>, total: &CrateCounts) -> String {
    let mut s = String::from("{\n  \"crates\": {");
    for (i, (name, c)) in per_crate.iter().enumerate() {
        if i > 0 {
            s.push(',');
        }
        s.push_str(&format!("\n    \"{name}\": {{"));
        write_counts(&mut s, c, "      ");
        s.push_str("\n    }");
    }
    s.push_str("\n  },\n  \"total\": {");
    write_counts(&mut s, total, "    ");
    s.push_str("\n  }\n}\n");
    s
}

fn write_counts(s: &mut String, c: &CrateCounts, indent: &str) {
    let kvs = [
        ("files", c.files),
        ("loc", c.loc),
        ("unwrap_used", c.unwrap_used),
        ("expect_used", c.expect_used),
        ("panic_macro", c.panic_macro),
        ("todo_macro", c.todo_macro),
        ("unimplemented_macro", c.unimplemented_macro),
        ("box_dyn_any", c.box_dyn_any),
        ("placeholder_marker", c.placeholder_marker),
        ("stub_comment", c.stub_comment),
        ("placeholder_comment", c.placeholder_comment),
        ("println_macro", c.println_macro),
        ("eprintln_macro", c.eprintln_macro),
        ("dbg_macro", c.dbg_macro),
    ];
    for (i, (k, v)) in kvs.iter().enumerate() {
        if i > 0 {
            s.push(',');
        }
        s.push_str(&format!("\n{indent}\"{k}\": {v}"));
    }
}

fn parse_json_crate(text: &str, crate_name: &str) -> Option<CrateCounts> {
    // Tiny ad-hoc JSON parser sufficient for our flat baseline format.
    // Avoids pulling in serde_json just for the xtask.
    let key = format!("\"{crate_name}\":");
    let start = text.find(&key)?;
    let after = &text[start + key.len()..];
    let open = after.find('{')?;
    let close = after[open..].find('}')?;
    let body = &after[open + 1..open + close];
    let mut c = CrateCounts::default();
    for token in body.split(',') {
        let token = token.trim();
        if let Some((k, v)) = token.split_once(':') {
            let k = k.trim().trim_matches('"');
            let v: usize = v.trim().parse().ok()?;
            match k {
                "files" => c.files = v,
                "loc" => c.loc = v,
                "unwrap_used" => c.unwrap_used = v,
                "expect_used" => c.expect_used = v,
                "panic_macro" => c.panic_macro = v,
                "todo_macro" => c.todo_macro = v,
                "unimplemented_macro" => c.unimplemented_macro = v,
                "box_dyn_any" => c.box_dyn_any = v,
                "placeholder_marker" => c.placeholder_marker = v,
                "stub_comment" => c.stub_comment = v,
                "placeholder_comment" => c.placeholder_comment = v,
                "println_macro" => c.println_macro = v,
                "eprintln_macro" => c.eprintln_macro = v,
                "dbg_macro" => c.dbg_macro = v,
                _ => {}
            }
        }
    }
    Some(c)
}
