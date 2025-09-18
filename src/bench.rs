use std::time::{Instant};
use std::path::Path;
use crate::engine::Engine;

/// Simple synchronous benchmark (single-threaded) that calls Engine::set repeatedly.
/// Reports throughput and latency percentiles (p50/p95/p99).
pub fn run_bench<P: AsRef<Path>>(dir: P, ops: usize, key_prefix: &str, val_size: usize) -> anyhow::Result<()> {
    let mut engine = Engine::open(dir)?;
    let mut latencies_ms = Vec::with_capacity(ops);

    // prepare a value payload of the requested size
    let val = vec!['x' as u8; val_size];

    for i in 0..ops {
        let key = format!("{}{:08}", key_prefix, i);
        let start = Instant::now();
        engine.set(&key, &val)?;
        let dt = start.elapsed();
        latencies_ms.push(dt.as_secs_f64() * 1000.0);
        if (i+1) % 1000 == 0 {
            eprintln!("progress: {}/{}", i+1, ops);
        }
    }

    // compute stats
    latencies_ms.sort_by(|a,b| a.partial_cmp(b).unwrap());
    let sum: f64 = latencies_ms.iter().sum();
    let mean = sum / (latencies_ms.len() as f64);
    let p50 = latencies_ms[latencies_ms.len() * 50 / 100];
    let p95 = latencies_ms[latencies_ms.len() * 95 / 100];
    let p99 = latencies_ms[latencies_ms.len() * 99 / 100];
    let throughput = (ops as f64) / (latencies_ms.iter().sum::<f64>() / 1000.0);

    println!("ops: {}", ops);
    println!("value size: {} bytes", val_size);
    println!("mean latency (ms): {:.3}", mean);
    println!("p50 (ms): {:.3}", p50);
    println!("p95 (ms): {:.3}", p95);
    println!("p99 (ms): {:.3}", p99);
    println!("throughput (ops/sec): {:.1}", throughput);

    Ok(())
}
