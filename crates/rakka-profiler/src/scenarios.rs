//! Scenario implementations. Each returns a [`Measurement`].
//!
//! All scenarios take a pre-built [`ActorSystem`] from the caller so the
//! system-startup cost is attributed to setup rather than the scenario
//! itself.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use rakka::prelude::*;
use tokio::sync::oneshot;

use crate::metrics::{cpu_delta, cpu_time, peak_rss_bytes, rss_bytes, rss_delta};
use crate::report::{Measurement, Scenario};

/// Fire-and-forget throughput into a null actor.
pub async fn tell(system: &ActorSystem, n: u64) -> anyhow::Result<Measurement> {
    let counter = Arc::new(AtomicUsize::new(0));
    let counter_actor = counter.clone();
    let target = system.actor_of(
        Props::create(move || CountingActor { counter: counter_actor.clone() }),
        "profiler-tell",
    )?;

    let rss_before = rss_bytes();
    let cpu_before = cpu_time();
    let start = Instant::now();
    for i in 0..n {
        target.tell(i);
    }
    while counter.load(Ordering::Acquire) < n as usize {
        tokio::task::yield_now().await;
    }
    let elapsed = start.elapsed();
    let rss_after = rss_bytes();
    let cpu_after = cpu_time();

    Ok(Measurement::from_throughput("rust", Scenario::Tell, "default-dispatcher", n, elapsed)
        .with_memory(rss_delta(rss_before, rss_after), peak_rss_bytes())
        .with_cpu(cpu_delta(cpu_before, cpu_after)))
}

/// Sequential ask latency.
pub async fn ask(system: &ActorSystem, n: u64) -> anyhow::Result<Measurement> {
    let target = system.actor_of(Props::create(|| EchoActor), "profiler-ask")?;

    let rss_before = rss_bytes();
    let cpu_before = cpu_time();
    let start = Instant::now();
    let mut samples: Vec<Duration> = Vec::with_capacity(n as usize);
    for i in 0..n {
        let t0 = Instant::now();
        let _reply: u64 = target
            .ask_with(
                move |reply: oneshot::Sender<u64>| EchoMsg { value: i, reply },
                Duration::from_secs(5),
            )
            .await
            .map_err(|e| anyhow::anyhow!("ask failed: {e:?}"))?;
        samples.push(t0.elapsed());
    }
    let elapsed = start.elapsed();
    let rss_after = rss_bytes();
    let cpu_after = cpu_time();
    samples.sort_unstable();

    Ok(Measurement::from_throughput("rust", Scenario::Ask, "default-dispatcher", n, elapsed)
        .with_latencies(&samples)
        .with_memory(rss_delta(rss_before, rss_after), peak_rss_bytes())
        .with_cpu(cpu_delta(cpu_before, cpu_after)))
}

/// Spawn N actors then hit each with one message; measures creation cost.
pub async fn fanout(system: &ActorSystem, n: u64) -> anyhow::Result<Measurement> {
    let counter = Arc::new(AtomicUsize::new(0));
    let rss_before = rss_bytes();
    let cpu_before = cpu_time();
    let start = Instant::now();

    let mut refs = Vec::with_capacity(n as usize);
    for i in 0..n {
        let c = counter.clone();
        let r = system.actor_of(
            Props::create(move || CountingActor { counter: c.clone() }),
            &format!("profiler-fanout-{i}"),
        )?;
        refs.push(r);
    }
    for (i, r) in refs.iter().enumerate() {
        r.tell(i as u64);
    }
    while counter.load(Ordering::Acquire) < n as usize {
        tokio::task::yield_now().await;
    }
    let elapsed = start.elapsed();
    let rss_after = rss_bytes();
    let cpu_after = cpu_time();

    Ok(Measurement::from_throughput("rust", Scenario::Fanout, "default-dispatcher", n, elapsed)
        .with_memory(rss_delta(rss_before, rss_after), peak_rss_bytes())
        .with_cpu(cpu_delta(cpu_before, cpu_after)))
}

/// CPU-bound handler — each message runs a small hashing loop. Throughput
/// for this scenario reflects per-core compute rather than mailbox speed.
pub async fn cpu(system: &ActorSystem, n: u64) -> anyhow::Result<Measurement> {
    let counter = Arc::new(AtomicUsize::new(0));
    let c = counter.clone();
    let target = system.actor_of(
        Props::create(move || CpuActor { counter: c.clone() }),
        "profiler-cpu",
    )?;

    let rss_before = rss_bytes();
    let cpu_before = cpu_time();
    let start = Instant::now();
    for i in 0..n {
        target.tell(i);
    }
    while counter.load(Ordering::Acquire) < n as usize {
        tokio::task::yield_now().await;
    }
    let elapsed = start.elapsed();
    let rss_after = rss_bytes();
    let cpu_after = cpu_time();

    Ok(Measurement::from_throughput("rust", Scenario::Cpu, "cpu-bound-handler", n, elapsed)
        .with_memory(rss_delta(rss_before, rss_after), peak_rss_bytes())
        .with_cpu(cpu_delta(cpu_before, cpu_after)))
}

// --- actors -----------------------------------------------------------------

struct CountingActor {
    counter: Arc<AtomicUsize>,
}
#[async_trait]
impl Actor for CountingActor {
    type Msg = u64;
    async fn handle(&mut self, _ctx: &mut Context<Self>, _msg: u64) {
        self.counter.fetch_add(1, Ordering::Release);
    }
}

struct EchoMsg {
    value: u64,
    reply: oneshot::Sender<u64>,
}

struct EchoActor;
#[async_trait]
impl Actor for EchoActor {
    type Msg = EchoMsg;
    async fn handle(&mut self, _ctx: &mut Context<Self>, msg: EchoMsg) {
        let _ = msg.reply.send(msg.value);
    }
}

struct CpuActor {
    counter: Arc<AtomicUsize>,
}
#[async_trait]
impl Actor for CpuActor {
    type Msg = u64;
    async fn handle(&mut self, _ctx: &mut Context<Self>, seed: u64) {
        // Deterministic CPU-bound work: 4k iterations of a cheap mixer.
        let mut h: u64 = seed.wrapping_add(0x9E37_79B9_7F4A_7C15);
        for i in 0..4096u64 {
            h ^= h.wrapping_mul(0xBF58_476D_1CE4_E5B9).wrapping_add(i);
            h = h.rotate_left(27);
        }
        std::hint::black_box(h);
        self.counter.fetch_add(1, Ordering::Release);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rakka::prelude::Config;

    #[tokio::test]
    async fn tell_scenario_completes() {
        let sys = ActorSystem::create("t-tell", Config::empty()).await.unwrap();
        let m = tell(&sys, 200).await.unwrap();
        assert_eq!(m.messages, 200);
        assert!(m.throughput_msgs_per_sec > 0.0);
        sys.terminate().await;
    }

    #[tokio::test]
    async fn ask_scenario_has_latencies() {
        let sys = ActorSystem::create("t-ask", Config::empty()).await.unwrap();
        let m = ask(&sys, 50).await.unwrap();
        assert!(m.p50_ns.is_some());
        assert!(m.p95_ns.is_some());
        sys.terminate().await;
    }

    #[tokio::test]
    async fn fanout_spawns_actors() {
        let sys = ActorSystem::create("t-fanout", Config::empty()).await.unwrap();
        let m = fanout(&sys, 20).await.unwrap();
        assert_eq!(m.messages, 20);
        sys.terminate().await;
    }

    #[tokio::test]
    async fn cpu_scenario_runs() {
        let sys = ActorSystem::create("t-cpu", Config::empty()).await.unwrap();
        let m = cpu(&sys, 40).await.unwrap();
        assert!(m.elapsed_ns > 0);
        sys.terminate().await;
    }
}
