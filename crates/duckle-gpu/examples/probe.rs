fn main() {
    for a in duckle_gpu::list_adapters() {
        println!("  {} | {} | {} | {} | max_buf={}MB",
            a.name, a.backend, a.device_type, a.driver, a.max_buffer_bytes / 1048576);
    }
    match duckle_gpu::Gpu::detect() {
        Some(g) => println!("\n  SELECTED: {:?}", g.info()),
        None => println!("\n  NO GPU DEVICE AVAILABLE"),
    }
}
