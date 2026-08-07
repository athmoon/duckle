//! Does keeping data resident on the device across operators pay for the
//! transfer? This is the experiment the single-kernel benchmark could not
//! answer, and the one that decides whether GPU execution is a real feature.
//!
//! Chain: filter (value > t) -> project (v*2+1) -> grouped sum by key.
//! One upload, three dispatches with no host round trip between them, and a
//! download of only the group totals.
//!
//! The CPU baseline is all-cores, because the real competition is DuckDB
//! running vectorised on every core, not one thread.
//!
//!   cargo run -p duckle-gpu --release --example chain_bench

use std::time::Instant;

const N_GROUPS: usize = 64;
const THRESHOLD: u32 = 500;

fn main() {
    let Some(gpu) = duckle_gpu::Gpu::detect() else {
        println!("no GPU adapter; nothing to measure");
        return;
    };
    let cores = std::thread::available_parallelism().map(|n| n.get()).unwrap_or(1);
    println!("device : {} ({})", gpu.info().name, gpu.info().backend);
    println!("cores  : {cores}");
    println!("chain  : filter -> project -> grouped sum ({N_GROUPS} groups)\n");

    println!(
        "{:>10} {:>9} {:>10} {:>9} {:>9} {:>9} {:>8}",
        "rows", "cpu-all", "gpu-total", "up", "compute", "down", "speedup"
    );

    for n in [1_000_000usize, 2_000_000, 4_000_000] {
        if n > gpu.max_records(4) {
            println!("{n:>10}  exceeds device limit, skipped");
            continue;
        }
        let keys: Vec<u32> = (0..n as u32).map(|i| i.wrapping_mul(2654435761)).collect();
        let values: Vec<u32> = (0..n as u32).map(|i| i.wrapping_mul(40503) % 1000).collect();

        // Warm the device so shader compilation is not charged to the first size.
        let _ = gpu.filter_project_aggregate(&keys[..1024], &values[..1024], THRESHOLD, N_GROUPS);

        let t = Instant::now();
        let cpu = cpu_all_cores(&keys, &values, cores);
        let cpu_ms = t.elapsed().as_secs_f64() * 1000.0;

        let t = Instant::now();
        let (got, tm) = gpu
            .filter_project_aggregate(&keys, &values, THRESHOLD, N_GROUPS)
            .expect("gpu chain");
        let gpu_ms = t.elapsed().as_secs_f64() * 1000.0;

        // A benchmark that does not check its answer measures nothing.
        assert_eq!(got, cpu, "gpu disagreed with cpu at {n} rows");

        println!(
            "{:>10} {:>7.1}ms {:>8.1}ms {:>7.1}ms {:>7.1}ms {:>7.1}ms {:>7.2}x",
            n, cpu_ms, gpu_ms, tm.upload_ms, tm.compute_ms, tm.download_ms, cpu_ms / gpu_ms
        );
    }

    println!(
        "\nupload moves {} bytes/row; download is {} bytes total regardless of rows.",
        8,
        N_GROUPS * 4
    );
}

/// The same chain across every core, with per-thread partial sums merged at
/// the end, which is how a real parallel aggregate is written.
fn cpu_all_cores(keys: &[u32], values: &[u32], cores: usize) -> Vec<u32> {
    let n = keys.len();
    let per = n.div_ceil(cores);
    let partials: Vec<Vec<u32>> = std::thread::scope(|s| {
        let handles: Vec<_> = (0..cores)
            .map(|c| {
                let lo = (c * per).min(n);
                let hi = ((c + 1) * per).min(n);
                let k = &keys[lo..hi];
                let v = &values[lo..hi];
                s.spawn(move || {
                    duckle_gpu::filter_project_aggregate_cpu(k, v, THRESHOLD, N_GROUPS)
                })
            })
            .collect();
        handles.into_iter().map(|h| h.join().unwrap()).collect()
    });
    let mut out = vec![0u32; N_GROUPS];
    for p in partials {
        for (o, x) in out.iter_mut().zip(p) {
            *o = o.wrapping_add(x);
        }
    }
    out
}
