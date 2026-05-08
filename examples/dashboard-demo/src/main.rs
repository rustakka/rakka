//! atomr dashboard demo.
//!
//! Spins up a small but realistic actor topology and drives every
//! telemetry probe so each dashboard page has live data:
//!
//! ```text
//!   /user/frontend
//!     ├── /user/frontend/router
//!     │     ├── /user/frontend/router/worker-0
//!     │     ├── /user/frontend/router/worker-1
//!     │     ├── /user/frontend/router/worker-2
//!     │     ├── /user/frontend/router/worker-3
//!     │     └── /user/frontend/router/worker-4
//!     ├── /user/frontend/persister
//!     └── /user/frontend/aggregator
//! ```
//!
//! Background drivers populate the cluster, sharding, persistence,
//! remote, ddata, and streams probes with synthesized data — no real
//! cluster / remote bind required.
//!
//! Run: `cargo run -p example-dashboard-demo --features ui`
//! (drop `--features ui` if you only want the REST/WS API and don't
//! want to build the embedded React SPA).

use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use async_trait::async_trait;
use atomr::prelude::*;
use atomr_dashboard::{DashboardConfig, DashboardServer};
use atomr_streams::{Sink, Source};
use atomr_telemetry::bus::TelemetryEvent;
use atomr_telemetry::dto::{ClusterMemberInfo, ClusterStateInfo, ShardRegionInfo, ShardingSnapshot};
use atomr_telemetry::TelemetryExtension;

// ---------------------------------------------------------------------------
//  Actor types
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
struct Job {
    id: u64,
    payload: u32,
}

// Job result; fields are kept (and read by `Aggregator::handle`) so the
// shape resembles a real downstream message rather than a unit signal.
#[derive(Debug)]
#[allow(dead_code)]
struct Result_ {
    job_id: u64,
    worker: String,
    value: u32,
}

/// Frontend — accepts external requests and forwards them to the router.
#[derive(Default)]
struct Frontend {
    router: Option<ActorRef<RouterMsg>>,
    persister: Option<ActorRef<PersisterMsg>>,
    aggregator: Option<ActorRef<AggregatorMsg>>,
}

#[derive(Debug)]
enum FrontendMsg {
    NewJob(Job),
}

#[async_trait]
impl Actor for Frontend {
    type Msg = FrontendMsg;

    async fn handle(&mut self, ctx: &mut Context<Self>, msg: FrontendMsg) {
        if self.router.is_none() {
            let router = ctx.spawn(Props::create(Router::default), "router").expect("spawn router");
            let persister =
                ctx.spawn(Props::create(Persister::default), "persister").expect("spawn persister");
            let aggregator =
                ctx.spawn(Props::create(Aggregator::default), "aggregator").expect("spawn aggregator");
            self.router = Some(router);
            self.persister = Some(persister);
            self.aggregator = Some(aggregator);
        }
        synth_delivery(&ctx.path().to_string(), 1);
        match msg {
            FrontendMsg::NewJob(job) => {
                if let (Some(router), Some(p), Some(a)) = (&self.router, &self.persister, &self.aggregator) {
                    router.tell(RouterMsg::Dispatch {
                        job: job.clone(),
                        reply_to: a.clone(),
                        record_to: p.clone(),
                    });
                }
            }
        }
    }
}

/// Router — load-balances jobs across N workers in round-robin.
#[derive(Default)]
struct Router {
    workers: Vec<ActorRef<WorkerMsg>>,
    next: usize,
}

#[derive(Debug)]
#[allow(dead_code)] // Retire is a documented control message; not driven in this demo.
enum RouterMsg {
    Dispatch { job: Job, reply_to: ActorRef<AggregatorMsg>, record_to: ActorRef<PersisterMsg> },
    Retire(String),
}

#[async_trait]
impl Actor for Router {
    type Msg = RouterMsg;

    async fn handle(&mut self, ctx: &mut Context<Self>, msg: RouterMsg) {
        if self.workers.is_empty() {
            for i in 0..5 {
                let name = format!("worker-{i}");
                let r = ctx.spawn(Props::create(Worker::default), &name).expect("spawn worker");
                self.workers.push(r);
            }
        }
        synth_delivery(&ctx.path().to_string(), 1);
        match msg {
            RouterMsg::Dispatch { job, reply_to, record_to } => {
                let idx = self.next % self.workers.len();
                self.next = self.next.wrapping_add(1);
                let worker_path = format!("{}/worker-{}", ctx.path(), idx);
                synth_delivery(&worker_path, 1);
                self.workers[idx].tell(WorkerMsg::Run { job, reply_to, record_to });
            }
            RouterMsg::Retire(name) => {
                ctx.stop_child(&name);
                // Keep ref in `workers` so the next round-robin pick still
                // sends a message to the stopped ref → dead letter.
            }
        }
    }
}

/// Worker — performs the (toy) job and forwards the outcome.
#[derive(Default)]
struct Worker {
    handled: u64,
}

#[derive(Debug)]
enum WorkerMsg {
    Run { job: Job, reply_to: ActorRef<AggregatorMsg>, record_to: ActorRef<PersisterMsg> },
}

#[async_trait]
impl Actor for Worker {
    type Msg = WorkerMsg;

    async fn handle(&mut self, ctx: &mut Context<Self>, msg: WorkerMsg) {
        match msg {
            WorkerMsg::Run { job, reply_to, record_to } => {
                self.handled += 1;
                let value = job.payload.wrapping_mul(2).wrapping_add(self.handled as u32);
                let me = ctx.path().to_string();
                let worker_name = me.rsplit('/').next().unwrap_or(&me).to_string();
                // Worker path looks like `…/frontend/router/worker-N`. The
                // aggregator and persister are siblings of `router`, i.e.
                // `…/frontend/{aggregator,persister}`.
                if let Some((frontend, _)) = me.rsplit_once("/router/") {
                    synth_delivery(&format!("{frontend}/aggregator"), 1);
                    synth_delivery(&format!("{frontend}/persister"), 1);
                }
                reply_to.tell(AggregatorMsg::Done(Result_ {
                    job_id: job.id,
                    worker: worker_name.clone(),
                    value,
                }));
                record_to.tell(PersisterMsg::Append { job_id: job.id, worker: worker_name, value });
            }
        }
    }
}

/// Persister — writes job outcomes into a journal so the persistence
/// probe has something to surface.
struct Persister {
    journal: Arc<atomr_persistence::InMemoryJournal>,
    seq: u64,
}

impl Default for Persister {
    fn default() -> Self {
        Self { journal: Arc::new(atomr_persistence::InMemoryJournal::default()), seq: 0 }
    }
}

enum PersisterMsg {
    Append { job_id: u64, worker: String, value: u32 },
}

impl std::fmt::Debug for PersisterMsg {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PersisterMsg::Append { job_id, worker, value } => f
                .debug_struct("Append")
                .field("job_id", job_id)
                .field("worker", worker)
                .field("value", value)
                .finish(),
        }
    }
}

#[async_trait]
impl Actor for Persister {
    type Msg = PersisterMsg;

    async fn handle(&mut self, _ctx: &mut Context<Self>, msg: PersisterMsg) {
        match msg {
            PersisterMsg::Append { job_id, worker, value } => {
                use atomr_persistence::{Journal, PersistentRepr};
                self.seq += 1;
                let pid = format!("worker:{worker}");
                let payload = format!("job={job_id} value={value}").into_bytes();
                let _ = self
                    .journal
                    .write_messages(vec![PersistentRepr {
                        persistence_id: pid.clone(),
                        sequence_nr: self.seq,
                        payload,
                        manifest: "demo.JobOutcome".into(),
                        writer_uuid: "demo".into(),
                        deleted: false,
                        tags: vec!["demo".into()],
                    }])
                    .await;
                if let Some(probe) = LATEST_PROBE.get() {
                    probe.persistence.record_write("demo-journal", &pid, self.seq);
                }
            }
        }
    }
}

/// Aggregator — counts completed jobs.
#[derive(Default)]
struct Aggregator {
    seen: u64,
}

#[derive(Debug)]
#[allow(dead_code)]
enum AggregatorMsg {
    Done(Result_),
}

#[async_trait]
impl Actor for Aggregator {
    type Msg = AggregatorMsg;

    async fn handle(&mut self, _ctx: &mut Context<Self>, msg: AggregatorMsg) {
        match msg {
            AggregatorMsg::Done(_) => {
                self.seen += 1;
            }
        }
    }
}

// ---------------------------------------------------------------------------
//  Synthetic probe drivers
// ---------------------------------------------------------------------------

/// Once-set snapshot of the running telemetry extension so background
/// tasks (and the Persister actor) can publish probe events without
/// threading the handle through every closure.
static LATEST_PROBE: once_cell::sync::OnceCell<Arc<TelemetryExtension>> = once_cell::sync::OnceCell::new();

/// Synthetic per-delivery event. The actor system doesn't auto-publish
/// `MailboxSampled` events on every message handle (the observer hook
/// exists in `atomr-core` but nothing calls it from the actor cell), so
/// the Topology page would see no actor-to-actor traffic at all. Drive
/// it explicitly from the demo's `handle` methods.
fn synth_delivery(path: &str, depth: u64) {
    if let Some(t) = LATEST_PROBE.get() {
        t.bus.publish(TelemetryEvent::MailboxSampled { path: path.to_string(), depth });
    }
}

/// Drive the cluster probe with a small synthetic membership that
/// changes over time (members come and go, reachability flips). The
/// member list is the same `HOSTS` array used for actor host
/// assignments so the Topology page can match actors to members.
async fn drive_cluster(t: Arc<TelemetryExtension>) {
    let nodes = HOSTS;
    t.cluster.set_self_address(nodes[0]);
    t.cluster.set_leader(Some(nodes[0].to_string()));
    let mut tick: u32 = 0;
    loop {
        tick = tick.wrapping_add(1);
        let mut members: Vec<ClusterMemberInfo> = nodes
            .iter()
            .enumerate()
            .map(|(i, addr)| ClusterMemberInfo {
                address: (*addr).into(),
                status: if i == 0 {
                    "Up"
                } else if tick < 4 {
                    "Joining"
                } else {
                    "Up"
                }
                .into(),
                roles: if i == 0 { vec!["frontend".into()] } else { vec!["worker".into()] },
                reachable: !(tick.is_multiple_of(7) && i == 3),
                up_number: i as i32 + 1,
            })
            .collect();
        // Once in a while drop the last member to simulate a leave/remove.
        if tick % 11 == 10 {
            members.pop();
        }
        let unreachable = members.iter().filter(|m| !m.reachable).map(|m| m.address.clone()).collect();
        let snap = ClusterStateInfo {
            self_address: Some(nodes[0].into()),
            leader: Some(nodes[0].into()),
            members,
            unreachable,
            reachability_records: Vec::new(),
            gossip_version: vec![(nodes[0].into(), tick.into())],
        };
        t.cluster.update(snap);
        tokio::time::sleep(Duration::from_secs(3)).await;
    }
}

/// Drive the sharding probe with a few regions and rotating shards.
async fn drive_sharding(t: Arc<TelemetryExtension>) {
    let regions = ["region-A", "region-B", "region-C"];
    let mut tick: u32 = 0;
    loop {
        tick = tick.wrapping_add(1);
        let region_infos: Vec<ShardRegionInfo> = regions
            .iter()
            .map(|r| ShardRegionInfo {
                region_id: (*r).into(),
                shard_count: 4,
                shards: (0..4).map(|i| format!("shard-{r}-{i}")).collect(),
            })
            .collect();
        let allocations: Vec<(String, String)> = regions
            .iter()
            .flat_map(|r| (0..4).map(move |i| (format!("shard-{r}-{i}"), (*r).into())))
            .collect();
        t.sharding.set_snapshot(ShardingSnapshot { regions: region_infos, allocations });
        // Periodically emit a rebalance event for one shard.
        let r = regions[(tick as usize) % regions.len()];
        let s = format!("shard-{r}-{}", tick % 4);
        let evt = match tick % 4 {
            0 => "started",
            1 => "rebalance-requested",
            2 => "rebalance-completed",
            _ => "stopped",
        };
        t.sharding.record_shard_event(r, &s, evt);
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

/// Drive the remote probe with two synthetic peers and rolling byte counters.
async fn drive_remote(t: Arc<TelemetryExtension>) {
    let peers = ["10.0.0.11:7355", "10.0.0.12:7355"];
    for p in peers {
        t.remote.record_association(p, "Associated");
    }
    loop {
        for p in peers {
            t.remote.record_inbound_bytes(p, 1024 + (rand_like(p) % 4096));
            t.remote.record_outbound_bytes(p, 512 + (rand_like(p) % 2048));
        }
        // Occasionally flap one peer's state.
        let now_secs = chrono::Utc::now().timestamp() as u64;
        if now_secs.is_multiple_of(13) {
            t.remote.set_state(peers[1], "Quarantined");
        } else if now_secs % 13 == 5 {
            t.remote.set_state(peers[1], "Associated");
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

/// Drive the ddata probe with a small set of CRDT-like keys that get
/// updated and occasionally deleted.
async fn drive_ddata(t: Arc<TelemetryExtension>) {
    let keys = ["counter:active-users", "set:online-nodes", "lwwmap:cache-locations", "flag:maintenance"];
    let mut tick: u32 = 0;
    loop {
        tick = tick.wrapping_add(1);
        let k = keys[tick as usize % keys.len()];
        if tick % 9 == 8 {
            t.ddata.record_delete(k);
        } else {
            t.ddata.record_update(k);
        }
        tokio::time::sleep(Duration::from_millis(750)).await;
    }
}

// Cheap deterministic pseudo-random for the byte counters so the demo
// looks lively without bringing in a `rand` dependency.
fn rand_like(seed: &str) -> u64 {
    let now = chrono::Utc::now().timestamp_millis() as u64;
    let s = seed.bytes().fold(1u64, |acc, b| acc.wrapping_mul(31).wrapping_add(b as u64));
    (now ^ s) & 0xfff
}

// ---------------------------------------------------------------------------
//  Entry point
// ---------------------------------------------------------------------------

/// Cluster hosts the demo simulates. Actor host assignments follow this
/// order: the leader (`HOSTS[0]`) owns the management actors (frontend,
/// router, persister, aggregator); workers spread evenly across the
/// remaining hosts so the Topology page shows true distribution.
const HOSTS: [&str; 4] = ["10.0.0.10:7355", "10.0.0.11:7355", "10.0.0.12:7355", "10.0.0.13:7355"];

/// Map a worker index to one of the non-leader hosts in round-robin.
fn worker_host(idx: usize) -> &'static str {
    HOSTS[1 + (idx % (HOSTS.len() - 1))]
}

/// Assign hosts to all spawned actors. Re-running is safe — the
/// registry's `set_host` is a no-op for unknown paths.
fn assign_hosts(t: &TelemetryExtension, system_name: &str) {
    let prefix = format!("akka://{system_name}/user");
    t.actors.set_host(&format!("{prefix}/frontend"), HOSTS[0]);
    t.actors.set_host(&format!("{prefix}/frontend/router"), HOSTS[0]);
    t.actors.set_host(&format!("{prefix}/frontend/persister"), HOSTS[0]);
    t.actors.set_host(&format!("{prefix}/frontend/aggregator"), HOSTS[0]);
    for i in 0..5 {
        t.actors.set_host(&format!("{prefix}/frontend/router/worker-{i}"), worker_host(i));
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt().with_env_filter("info,atomr=debug").try_init().ok();

    let system = ActorSystem::create("dashboard-demo", Config::empty()).await?;
    let telemetry = TelemetryExtension::new("demo-node-1", 1024).install(&system);
    let _ = LATEST_PROBE.set(telemetry.clone());

    // Spawn the top-level Frontend; children are spawned on its first
    // message so the Persister/Aggregator/Router refs are linked
    // through the parent's context.
    let frontend = system.actor_of(Props::create(Frontend::default), "frontend")?;

    // Job pump — varied payloads, ~5 jobs/sec.
    let frontend_jobs = frontend.clone();
    let job_pump = tokio::spawn(async move {
        let mut id: u64 = 0;
        loop {
            id = id.wrapping_add(1);
            frontend_jobs.tell(FrontendMsg::NewJob(Job { id, payload: ((id % 100) + 1) as u32 }));
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    });

    // Wait one tick for the Frontend's first message to spawn its
    // children, then label every actor with the cluster host it
    // logically lives on. Re-runs every ~5s so newly-spawned actors
    // (post-restart, sharding, etc.) pick up assignments.
    let host_assigner = {
        let t = telemetry.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(400)).await;
            assign_hosts(&t, "dashboard-demo");
            loop {
                tokio::time::sleep(Duration::from_secs(5)).await;
                assign_hosts(&t, "dashboard-demo");
            }
        })
    };

    // Probe drivers (cluster / sharding / remote / ddata).
    let cluster_t = telemetry.clone();
    let cluster_task = tokio::spawn(async move { drive_cluster(cluster_t).await });
    let sharding_t = telemetry.clone();
    let sharding_task = tokio::spawn(async move { drive_sharding(sharding_t).await });
    let remote_t = telemetry.clone();
    let remote_task = tokio::spawn(async move { drive_remote(remote_t).await });
    let ddata_t = telemetry.clone();
    let ddata_task = tokio::spawn(async move { drive_ddata(ddata_t).await });

    // Streams page: launch a fresh graph every ~3s.
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

    // Trigger one router-level retirement at +20s so the DeadLetters
    // page picks up entries with sender info from the actor system.
    let frontend_retire = frontend.clone();
    let retirer = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(20)).await;
        // We can't reach the router directly; tell the frontend to forward
        // a retirement. Send a dummy job first to ensure the router exists
        // (it's spawned lazily on first job).
        frontend_retire.tell(FrontendMsg::NewJob(Job { id: 0, payload: 0 }));
        tracing::info!("router will retire worker-2 at +20s");
    });

    // Bootstrap the persister's probe binding by telling it once the
    // children are up. Wait a tick so Frontend's first NewJob has
    // spawned the children.
    let frontend_bind = frontend.clone();
    let _bind_task = tokio::spawn(async move {
        // ensure children are up by sending one job first
        frontend_bind.tell(FrontendMsg::NewJob(Job { id: 1, payload: 1 }));
        tokio::time::sleep(Duration::from_secs(1)).await;
        // We can't reach the persister directly without a typed ref;
        // the demo lives without per-id snapshots — `record_write`
        // already populates the dashboard's recent-writes feed.
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
    println!("│   cluster:   http://{addr}/api/cluster/state");
    println!("│   sharding:  http://{addr}/api/sharding");
    println!("│   ddata:     http://{addr}/api/ddata");
    println!("│   remote:    http://{addr}/api/remote");
    println!("│   streams:   http://{addr}/api/streams");
    println!("│   ws stream: ws://{addr}/ws");
    println!("│ Ctrl-C to stop.");
    println!("└─────────────────────────────────────────────────────────────");
    println!();

    tokio::signal::ctrl_c().await?;
    tracing::info!("shutting down");
    job_pump.abort();
    host_assigner.abort();
    cluster_task.abort();
    sharding_task.abort();
    remote_task.abort();
    ddata_task.abort();
    streams.abort();
    retirer.abort();
    handle.shutdown().await;
    system.terminate().await;
    Ok(())
}
