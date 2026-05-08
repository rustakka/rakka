//! atomr dashboard demo.
//!
//! Spins up a small actor topology, populates the streams + dead-letter
//! probes, and starts the embedded web dashboard. Browse to the printed
//! URL to see live actor tree, dead letters, and stream graphs.
//!
//! Run: `cargo run -p example-dashboard-demo`

use std::time::Duration;

use anyhow::Result;
use async_trait::async_trait;
use atomr::prelude::*;
use atomr_dashboard::{DashboardConfig, DashboardServer};
use atomr_streams::{Sink, Source};
use atomr_telemetry::TelemetryExtension;

#[derive(Debug)]
enum WorkerCmd {
    Inc,
}

struct Worker {
    n: u64,
}

#[async_trait]
impl Actor for Worker {
    type Msg = WorkerCmd;

    async fn handle(&mut self, _ctx: &mut Context<Self>, msg: WorkerCmd) {
        match msg {
            WorkerCmd::Inc => self.n += 1,
        }
    }
}

#[derive(Debug)]
enum BossMsg {
    /// Fan an `Inc` out to every live worker.
    Tick,
    /// Stop a child by name. Subsequent messages to its ref become dead letters.
    Retire(String),
}

#[derive(Default)]
struct Boss {
    workers: Vec<(String, ActorRef<WorkerCmd>)>,
}

#[async_trait]
impl Actor for Boss {
    type Msg = BossMsg;

    async fn pre_start(&mut self, ctx: &mut Context<Self>) {
        for i in 0..3 {
            let name = format!("worker-{i}");
            let r = ctx.spawn(Props::create(|| Worker { n: 0 }), &name).expect("spawn worker");
            self.workers.push((name, r));
        }
    }

    async fn handle(&mut self, ctx: &mut Context<Self>, msg: BossMsg) {
        match msg {
            BossMsg::Tick => {
                for (_, w) in &self.workers {
                    w.tell(WorkerCmd::Inc);
                }
            }
            BossMsg::Retire(name) => {
                ctx.stop_child(&name);
                // Keep the ref around so the *Tick* path keeps producing
                // deliveries — once the child drains it, those become
                // dead letters surfaced on the dashboard.
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt().with_env_filter("info,atomr=debug").try_init().ok();

    let system = ActorSystem::create("dashboard-demo", Config::empty()).await?;
    let telemetry = TelemetryExtension::new("demo-node-1", 1024).install(&system);

    let boss = system.actor_of(Props::create(Boss::default), "boss")?;

    // Periodic fan-out — keeps the actor tree busy and produces
    // dead-letters once a worker has been retired below.
    let boss_ticker = boss.clone();
    let ticker = tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_millis(250));
        loop {
            interval.tick().await;
            boss_ticker.tell(BossMsg::Tick);
        }
    });

    // After ~5s, retire one worker so the dashboard's DeadLetters page
    // shows entries.
    let boss_retire = boss.clone();
    let retirer = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(5)).await;
        boss_retire.tell(BossMsg::Retire("worker-2".into()));
        tracing::info!("retired worker-2 — subsequent ticks → dead letters");
    });

    // Streams page: launch a fresh graph every 3s so the running-graph
    // counter cycles up and down. Each graph counts ticks for ~2s.
    let streams_telemetry = telemetry.clone();
    let streams = tokio::spawn(async move {
        let mut round: u64 = 0;
        loop {
            round += 1;
            let name = format!("demo-tick-collector-{round}");
            let id = streams_telemetry.streams.start_graph(&name);
            let source = Source::tick(Duration::from_millis(50), Duration::from_millis(100), 1u64).take(20);
            let total: u64 = Sink::sum(source).await;
            streams_telemetry.streams.finish_graph(id);
            tracing::info!(graph = %name, total, "stream graph completed");
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    });

    let bind = "127.0.0.1:9100".parse()?;
    let cfg = DashboardConfig { bind, ..Default::default() };
    let server = DashboardServer::new(telemetry, cfg);
    let handle = server.start().await?;

    let addr = handle.bound_addr;
    println!();
    println!("┌─────────────────────────────────────────────────────────────");
    println!("│ atomr dashboard demo");
    println!("│   UI:        http://{addr}/");
    println!("│   API:       http://{addr}/api/snapshot");
    println!("│   actors:    http://{addr}/api/actors/tree");
    println!("│   dead lts:  http://{addr}/api/dead-letters");
    println!("│   streams:   http://{addr}/api/streams");
    println!("│   ws stream: ws://{addr}/ws");
    println!("│ Ctrl-C to stop.");
    println!("└─────────────────────────────────────────────────────────────");
    println!();

    tokio::signal::ctrl_c().await?;
    tracing::info!("shutting down");
    ticker.abort();
    retirer.abort();
    streams.abort();
    handle.shutdown().await;
    system.terminate().await;
    Ok(())
}
