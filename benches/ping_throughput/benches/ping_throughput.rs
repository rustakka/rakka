//! Criterion bench sending N messages to a null actor.

use async_trait::async_trait;
use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use rakka::prelude::*;

struct Null;

#[async_trait]
impl Actor for Null {
    type Msg = u64;
    async fn handle(&mut self, _ctx: &mut Context<Self>, _msg: u64) {}
}

fn bench_tell_throughput(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread().enable_all().build().unwrap();
    let sys = rt.block_on(async { ActorSystem::create("bench", Config::empty()).await.unwrap() });
    let r = sys.actor_of(Props::create(|| Null), "null").unwrap();

    const N: u64 = 10_000;
    let mut g = c.benchmark_group("tell");
    g.throughput(Throughput::Elements(N));
    g.bench_function("10k-null", |b| {
        b.iter(|| {
            for i in 0..N {
                r.tell(i);
            }
        });
    });
    g.finish();

    rt.block_on(async { sys.terminate().await });
}

criterion_group!(benches, bench_tell_throughput);
criterion_main!(benches);
