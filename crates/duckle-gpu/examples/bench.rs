//! Measure whether the GPU actually beats the CPU on a column-shaped hash.
//!
//! The CPU baseline is deliberately all-cores, not single-threaded. Duckle's
//! real competition for this work is DuckDB, which is vectorised across every
//! core, so timing the GPU against one core would flatter it into a speedup
//! that does not exist in the product.
//!
//! The GPU timing includes the upload and the readback. Excluding transfer is
//! the standard way GPU benchmarks lie: the data starts in host memory and the
//! answer has to come back, so both legs are part of the cost.
//!
//!   cargo run -p duckle-gpu --release --example bench

use std::time::Instant;

fn main() {
    let record_words = 8; // 32 bytes per record, a plausible key-column width
    let Some(gpu) = duckle_gpu::Gpu::detect() else {
        println!("no GPU adapter; nothing to measure");
        return;
    };
    let info = gpu.info();
    println!("device : {} ({}, {})", info.name, info.backend, info.device_type);
    println!("cores  : {}", std::thread::available_parallelism().map(|n| n.get()).unwrap_or(1));
    println!(
        "cap    : {} records at {} bytes\n",
        gpu.max_records(record_words * 4),
        record_words * 4
    );

    println!(
        "{:>12} {:>12} {:>12} {:>12} {:>10}",
        "records", "cpu-1core", "cpu-all", "gpu+xfer", "vs cpu-all"
    );

    for n_records in [100_000usize, 1_000_000, 4_000_000, 16_000_000] {
        if n_records > gpu.max_records(record_words * 4) {
            println!("{n_records:>12}  exceeds device limit, skipped");
            continue;
        }
        let data: Vec<u32> = (0..n_records * record_words)
            .map(|i| (i as u32).wrapping_mul(2654435761))
            .collect();

        // Warm the device: the first dispatch pays shader compilation, which
        // is a one-off and would otherwise be charged to the first size.
        let _ = gpu.hash_records(&data[..record_words * 1024], record_words);

        let t = Instant::now();
        let cpu1 = duckle_gpu::hash_records_cpu(&data, record_words);
        let cpu1_ms = t.elapsed().as_secs_f64() * 1000.0;

        let t = Instant::now();
        let cpu_n = hash_all_cores(&data, record_words);
        let cpun_ms = t.elapsed().as_secs_f64() * 1000.0;

        let t = Instant::now();
        let g = gpu.hash_records(&data, record_words).expect("gpu hash");
        let gpu_ms = t.elapsed().as_secs_f64() * 1000.0;

        // A benchmark that does not check its answer is measuring nothing.
        assert_eq!(g, cpu1, "gpu disagreed with cpu at {n_records} records");
        assert_eq!(cpu_n, cpu1, "threaded cpu disagreed with itself");

        println!(
            "{:>12} {:>10.1}ms {:>10.1}ms {:>10.1}ms {:>9.2}x",
            n_records,
            cpu1_ms,
            cpun_ms,
            gpu_ms,
            cpun_ms / gpu_ms
        );
    }
}

/// The same hash spread across every core, as the fair baseline.
fn hash_all_cores(data: &[u32], record_words: usize) -> Vec<u32> {
    let cores = std::thread::available_parallelism().map(|n| n.get()).unwrap_or(1);
    let n_records = data.len() / record_words;
    let per = n_records.div_ceil(cores);
    std::thread::scope(|s| {
        let handles: Vec<_> = (0..cores)
            .map(|c| {
                let lo = (c * per).min(n_records);
                let hi = ((c + 1) * per).min(n_records);
                let slice = &data[lo * record_words..hi * record_words];
                s.spawn(move || duckle_gpu::hash_records_cpu(slice, record_words))
            })
            .collect();
        handles.into_iter().flat_map(|h| h.join().unwrap()).collect()
    })
}
