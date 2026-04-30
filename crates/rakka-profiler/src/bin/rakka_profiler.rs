//! CLI wrapper for the Rust-side actor profiler.
//!
//! ```text
//! rakka-profiler [--scenario tell|ask|fanout|cpu|all]
//!                   [--messages N]
//!                   [--format md|json]
//!                   [--output FILE]
//! ```
//!
//! Without any arguments it runs every scenario and prints a markdown
//! table.

use std::fs;
use std::process::ExitCode;

use rakka::prelude::*;
use rakka_profiler::{scenarios, ProfilerReport, Scenario};

#[derive(Debug, Clone)]
struct Args {
    scenario: Option<Scenario>,
    messages: Option<u64>,
    format: Format,
    output: Option<String>,
}

#[derive(Debug, Clone, Copy)]
enum Format {
    Markdown,
    Json,
}

impl Default for Args {
    fn default() -> Self {
        Self { scenario: None, messages: None, format: Format::Markdown, output: None }
    }
}

fn parse_args() -> Result<Args, String> {
    let mut a = Args::default();
    let mut it = std::env::args().skip(1);
    while let Some(arg) = it.next() {
        match arg.as_str() {
            "--scenario" => {
                let v = it.next().ok_or("--scenario needs a value")?;
                if v != "all" {
                    a.scenario = Some(
                        Scenario::parse(&v).ok_or_else(|| format!("unknown scenario: {v}"))?,
                    );
                }
            }
            "--messages" | "-n" => {
                let v = it.next().ok_or("--messages needs a value")?;
                a.messages = Some(v.parse().map_err(|e| format!("bad --messages: {e}"))?);
            }
            "--format" => {
                a.format = match it.next().as_deref() {
                    Some("json") => Format::Json,
                    Some("md") | Some("markdown") | None => Format::Markdown,
                    Some(v) => return Err(format!("unknown --format: {v}")),
                };
            }
            "--output" | "-o" => {
                a.output = Some(it.next().ok_or("--output needs a value")?);
            }
            "-h" | "--help" => {
                println!("{}", help_text());
                std::process::exit(0);
            }
            other => return Err(format!("unknown arg: {other}")),
        }
    }
    Ok(a)
}

fn help_text() -> &'static str {
    "rakka-profiler — actor memory + CPU profiler\n\
     \n\
     USAGE:\n\
     \x20 rakka-profiler [--scenario tell|ask|fanout|cpu|all]\n\
     \x20                   [--messages N]\n\
     \x20                   [--format md|json]\n\
     \x20                   [--output FILE]\n"
}

fn default_messages(s: Scenario) -> u64 {
    match s {
        Scenario::Tell => 100_000,
        Scenario::Ask => 5_000,
        Scenario::Fanout => 2_000,
        Scenario::Cpu => 10_000,
    }
}

fn main() -> ExitCode {
    let args = match parse_args() {
        Ok(a) => a,
        Err(e) => {
            eprintln!("error: {e}\n\n{}", help_text());
            return ExitCode::from(2);
        }
    };

    let rt = match tokio::runtime::Builder::new_multi_thread().enable_all().build() {
        Ok(r) => r,
        Err(e) => {
            eprintln!("tokio: {e}");
            return ExitCode::from(1);
        }
    };

    let result = rt.block_on(async move { run(args).await });
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("profiler failed: {e:?}");
            ExitCode::from(1)
        }
    }
}

async fn run(args: Args) -> anyhow::Result<()> {
    let system = ActorSystem::create("profiler", Config::empty()).await?;
    let mut report = ProfilerReport::new("rust");

    let scenarios: Vec<Scenario> = match args.scenario {
        Some(s) => vec![s],
        None => Scenario::all().to_vec(),
    };

    for s in scenarios {
        let n = args.messages.unwrap_or_else(|| default_messages(s));
        let m = match s {
            Scenario::Tell => scenarios::tell(&system, n).await?,
            Scenario::Ask => scenarios::ask(&system, n).await?,
            Scenario::Fanout => scenarios::fanout(&system, n).await?,
            Scenario::Cpu => scenarios::cpu(&system, n).await?,
        };
        report.push(m);
    }

    system.terminate().await;

    let rendered = match args.format {
        Format::Markdown => report.to_markdown(),
        Format::Json => serde_json::to_string_pretty(&report)?,
    };
    match args.output {
        Some(path) => fs::write(path, rendered)?,
        None => println!("{rendered}"),
    }
    Ok(())
}
