//! Local in-memory storage benchmark of the actual service and CAS path.
//! Results include hashing/manifest serialization; they do not measure AWS S3.
use append_proxy::{AppendService, Config};
use bytes::Bytes;
use clap::Parser;
use object_store::memory::InMemory;
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Instant,
};

#[derive(Parser)]
struct Args {
    #[arg(long, default_value_t = 1000)]
    records: usize,
    #[arg(long, default_value_t = 16)]
    objects: usize,
    #[arg(long, default_value_t = 8)]
    concurrency: usize,
    #[arg(long, default_value_t = 65536)]
    bytes: usize,
}
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    anyhow::ensure!(
        args.records > 0
            && args.objects > 0
            && args.concurrency > 0
            && args.bytes > 0
            && args.bytes <= 8 * 1024 * 1024,
        "positive bounded arguments required"
    );
    anyhow::ensure!(
        args.records <= 100_000 && args.objects <= 100_000 && args.concurrency <= 1024,
        "benchmark scale exceeds safety limits"
    );
    let service = Arc::new(AppendService::new(
        Arc::new(InMemory::new()),
        Config {
            max_entries: args.records.min(100_000),
            ..Default::default()
        },
    )?);
    let next = Arc::new(AtomicUsize::new(0));
    let payload = Bytes::from(vec![b'x'; args.bytes]);
    let started = Instant::now();
    let mut workers = tokio::task::JoinSet::new();
    for _ in 0..args.concurrency {
        let service = service.clone();
        let next = next.clone();
        let payload = payload.clone();
        workers.spawn(async move {
            let mut latency_us = Vec::new();
            loop {
                let index = next.fetch_add(1, Ordering::Relaxed);
                if index >= args.records {
                    break;
                }
                let t = Instant::now();
                service
                    .append(
                        &format!("object-{}", index % args.objects),
                        &format!("request-{index}"),
                        payload.clone(),
                        None,
                    )
                    .await?;
                latency_us.push(t.elapsed().as_micros() as u64);
            }
            Ok::<_, append_proxy::ProxyError>(latency_us)
        });
    }
    let mut latencies = Vec::new();
    while let Some(worker) = workers.join_next().await {
        latencies.extend(worker??);
    }
    let seconds = started.elapsed().as_secs_f64();
    latencies.sort_unstable();
    let mut observed = 0;
    for index in 0..args.objects {
        if let Ok(snapshot) = service.snapshot(&format!("object-{index}")).await {
            observed += snapshot.manifest.entries.len();
        }
    }
    anyhow::ensure!(
        observed == args.records,
        "benchmark detected a missing committed request"
    );
    println!(
        "{}",
        serde_json::json!({"backend":"object_store::memory::InMemory", "scope":"actual append service; in-process storage, no network or AWS measurement", "records":args.records,"objects":args.objects,"concurrency":args.concurrency,"payload_bytes":args.bytes,"seconds":seconds,"appends_per_second":args.records as f64/seconds,"mib_per_second":(args.records*args.bytes) as f64/1048576.0/seconds,"latency_us":{"p50":latencies[latencies.len()/2],"p95":latencies[(latencies.len()*95/100).min(latencies.len()-1)],"p99":latencies[(latencies.len()*99/100).min(latencies.len()-1)]}})
    );
    Ok(())
}
