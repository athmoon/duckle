//! Optional GPU compute for Duckle pipeline stages.
//!
//! # Why wgpu and not CUDA
//!
//! Pipelines run on whatever machine the user has. wgpu targets Vulkan and
//! DX12 on Windows, Vulkan on Linux and Metal on macOS, across NVIDIA, AMD and
//! Intel, from one binary and with no toolkit to install. CUDA would be
//! NVIDIA-only and would need a runtime shipped alongside.
//!
//! # Why this is in-process and not a sidecar
//!
//! Duckle already runs some subsystems as sidecar binaries. That is wrong here:
//! GPU work is per-batch, so a sidecar would pay process IPC and serialisation
//! on every batch and give back more than the device wins.
//!
//! # The honest position
//!
//! A discrete GPU has to earn its place against DuckDB, which already runs
//! vectorised across every core, and it has to earn it while paying to move
//! data over PCIe in both directions. That is not a given, and it is not true
//! for every operation. So this crate is built measurement-first: it exposes
//! device detection, one real kernel, and a byte-exact CPU reference for the
//! same computation, so the speedup can be measured on the machine in question
//! rather than assumed. Nothing here is wired into stage execution until a
//! kernel is measured to win.

use std::sync::Arc;

/// What was found on this machine. Reported to the user rather than kept
/// internal: "GPU acceleration is on" is a claim they should be able to check.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GpuInfo {
    /// Adapter name as the driver reports it, e.g. "NVIDIA GeForce RTX 4090".
    pub name: String,
    /// Graphics API actually in use: Vulkan, Dx12, Metal, Gl.
    pub backend: String,
    /// DiscreteGpu, IntegratedGpu, VirtualGpu, Cpu or Other.
    pub device_type: String,
    /// Driver name and version, useful when a machine misbehaves.
    pub driver: String,
    /// Largest single storage buffer the device will bind, in bytes. This is
    /// the real cap on batch size, and it is often far below total VRAM.
    pub max_buffer_bytes: u64,
    /// Largest workgroup dispatch in one dimension.
    pub max_workgroups_per_dim: u32,
}

impl GpuInfo {
    /// Whether this is a real discrete device rather than a software or
    /// integrated fallback. An integrated GPU shares system memory with the
    /// CPU, so it rarely beats DuckDB and should not be picked silently.
    pub fn is_discrete(&self) -> bool {
        self.device_type == "DiscreteGpu"
    }
}

/// A ready compute device. Cloneable and cheap to pass around; the underlying
/// wgpu device and queue are reference counted.
#[derive(Clone)]
pub struct Gpu {
    device: Arc<wgpu::Device>,
    queue: Arc<wgpu::Queue>,
    info: GpuInfo,
}

impl std::fmt::Debug for Gpu {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Gpu").field("info", &self.info).finish()
    }
}

/// `InstanceDescriptor` has no `Default` in wgpu 30, so spell it out once.
/// PRIMARY is Vulkan and Metal here, since DX12 is not compiled in.
fn instance_descriptor() -> wgpu::InstanceDescriptor {
    wgpu::InstanceDescriptor {
        backends: wgpu::Backends::PRIMARY,
        flags: wgpu::InstanceFlags::default(),
        memory_budget_thresholds: Default::default(),
        backend_options: Default::default(),
        // Headless compute: there is no window, so no display handle.
        display: None,
    }
}

fn backend_name(b: wgpu::Backend) -> &'static str {
    match b {
        wgpu::Backend::Vulkan => "Vulkan",
        wgpu::Backend::Dx12 => "Dx12",
        wgpu::Backend::Metal => "Metal",
        wgpu::Backend::Gl => "Gl",
        wgpu::Backend::BrowserWebGpu => "WebGpu",
        wgpu::Backend::Noop => "Noop",
    }
}

fn device_type_name(t: wgpu::DeviceType) -> &'static str {
    match t {
        wgpu::DeviceType::DiscreteGpu => "DiscreteGpu",
        wgpu::DeviceType::IntegratedGpu => "IntegratedGpu",
        wgpu::DeviceType::VirtualGpu => "VirtualGpu",
        wgpu::DeviceType::Cpu => "Cpu",
        wgpu::DeviceType::Other => "Other",
    }
}

/// List every adapter the system exposes, without opening a device.
///
/// Used for reporting and for diagnosing "why is my GPU not being used": a
/// machine with a discrete card that only lists an integrated one usually has
/// a driver problem, and that is worth showing rather than hiding.
pub fn list_adapters() -> Vec<GpuInfo> {
    let instance = wgpu::Instance::new(instance_descriptor());
    pollster::block_on(instance.enumerate_adapters(wgpu::Backends::PRIMARY))
        .into_iter()
        .map(|a| {
            let info = a.get_info();
            let limits = a.limits();
            GpuInfo {
                name: info.name,
                backend: backend_name(info.backend).to_string(),
                device_type: device_type_name(info.device_type).to_string(),
                driver: format!("{} {}", info.driver, info.driver_info).trim().to_string(),
                max_buffer_bytes: limits.max_storage_buffer_binding_size as u64,
                max_workgroups_per_dim: limits.max_compute_workgroups_per_dimension,
            }
        })
        .collect()
}

impl Gpu {
    /// Open the best available compute device, or None when there is none.
    ///
    /// Returns None rather than erroring on a machine with no GPU, no driver,
    /// or a headless server: the caller's correct response is always to run on
    /// the CPU, so an absent device is not an error condition.
    pub fn detect() -> Option<Gpu> {
        pollster::block_on(Self::detect_async())
    }

    async fn detect_async() -> Option<Gpu> {
        let instance = wgpu::Instance::new(instance_descriptor());
        let adapter = instance
            .request_adapter(&wgpu::RequestAdapterOptions {
                // HighPerformance picks the discrete card on a laptop that has
                // both, which is the whole point of asking for a GPU here.
                power_preference: wgpu::PowerPreference::HighPerformance,
                force_fallback_adapter: false,
                compatible_surface: None,
                ..Default::default()
            })
            .await
            .ok()?;

        let adapter_info = adapter.get_info();
        let adapter_limits = adapter.limits();
        let (device, queue) = adapter
            .request_device(&wgpu::DeviceDescriptor {
                label: Some("duckle-gpu"),
                required_features: wgpu::Features::empty(),
                // Ask for what the adapter actually offers rather than the
                // conservative defaults, so a big card is allowed to bind big
                // buffers instead of being capped at the downlevel minimum.
                required_limits: adapter_limits.clone(),
                memory_hints: wgpu::MemoryHints::Performance,
                trace: wgpu::Trace::Off,
                experimental_features: Default::default(),
            })
            .await
            .ok()?;

        Some(Gpu {
            device: Arc::new(device),
            queue: Arc::new(queue),
            info: GpuInfo {
                name: adapter_info.name,
                backend: backend_name(adapter_info.backend).to_string(),
                device_type: device_type_name(adapter_info.device_type).to_string(),
                driver: format!("{} {}", adapter_info.driver, adapter_info.driver_info)
                    .trim()
                    .to_string(),
                max_buffer_bytes: adapter_limits.max_storage_buffer_binding_size as u64,
                max_workgroups_per_dim: adapter_limits.max_compute_workgroups_per_dimension,
            },
        })
    }

    pub fn info(&self) -> &GpuInfo {
        &self.info
    }

    /// Largest number of fixed-width records this device can hash in one
    /// dispatch. Callers must chunk to this; exceeding it is a device error,
    /// not a slow path.
    pub fn max_records(&self, record_bytes: usize) -> usize {
        let by_buffer = (self.info.max_buffer_bytes as usize) / record_bytes.max(1);
        // Output is one u32 per record, and is bound as a storage buffer too.
        let by_output = (self.info.max_buffer_bytes as usize) / 4;
        let by_dispatch = (self.info.max_workgroups_per_dim as usize).saturating_mul(WORKGROUP);
        by_buffer.min(by_output).min(by_dispatch)
    }
}

const WORKGROUP: usize = 64;

/// FNV-1a over each fixed-width record, one 32-bit hash per record.
///
/// FNV-1a rather than a cryptographic hash on purpose: this exists to measure
/// whether the device beats the CPU on a real column-shaped workload, and a
/// simple hash is the honest case. A heavier hash would flatter the GPU by
/// raising compute per byte, which would make the benchmark say more about the
/// choice of hash than about the hardware.
const HASH_SHADER: &str = r#"
struct Params {
    record_words: u32,
    n_records: u32,
};

@group(0) @binding(0) var<storage, read> data: array<u32>;
@group(0) @binding(1) var<storage, read_write> out: array<u32>;
@group(0) @binding(2) var<uniform> params: Params;

@compute @workgroup_size(64)
fn main(@builtin(global_invocation_id) gid: vec3<u32>) {
    let i = gid.x;
    if (i >= params.n_records) {
        return;
    }
    var h: u32 = 2166136261u;
    let base = i * params.record_words;
    for (var w: u32 = 0u; w < params.record_words; w = w + 1u) {
        let word = data[base + w];
        // Hash the four bytes little-endian, so the result matches a CPU
        // implementation walking the same buffer as a byte slice.
        for (var b: u32 = 0u; b < 4u; b = b + 1u) {
            let byte = (word >> (b * 8u)) & 255u;
            h = (h ^ byte) * 16777619u;
        }
    }
    out[i] = h;
}
"#;

/// Byte-exact CPU reference for [`Gpu::hash_records`].
///
/// Not a fallback that happens to be close: the tests assert the two agree
/// exactly, so a kernel that drifts is a test failure rather than a silent
/// difference in someone's output.
pub fn hash_records_cpu(data: &[u32], record_words: usize) -> Vec<u32> {
    if record_words == 0 {
        return Vec::new();
    }
    data.chunks_exact(record_words)
        .map(|rec| {
            let mut h: u32 = 2166136261;
            for word in rec {
                for b in 0..4 {
                    let byte = (word >> (b * 8)) & 0xff;
                    h = (h ^ byte).wrapping_mul(16777619);
                }
            }
            h
        })
        .collect()
}

impl Gpu {
    /// Hash each fixed-width record on the device.
    ///
    /// `data` is a flat buffer of `n_records * record_words` words. Returns one
    /// hash per record, matching [`hash_records_cpu`] exactly.
    pub fn hash_records(&self, data: &[u32], record_words: usize) -> Result<Vec<u32>, String> {
        if record_words == 0 || data.is_empty() {
            return Ok(Vec::new());
        }
        if data.len() % record_words != 0 {
            return Err(format!(
                "buffer of {} words is not a whole number of {}-word records",
                data.len(),
                record_words
            ));
        }
        let n_records = data.len() / record_words;
        let cap = self.max_records(record_words * 4);
        if n_records > cap {
            return Err(format!(
                "{n_records} records exceeds this device's limit of {cap}; chunk the batch"
            ));
        }
        pollster::block_on(self.hash_records_async(data, record_words, n_records))
    }

    async fn hash_records_async(
        &self,
        data: &[u32],
        record_words: usize,
        n_records: usize,
    ) -> Result<Vec<u32>, String> {
        use wgpu::util::DeviceExt;

        let module = self
            .device
            .create_shader_module(wgpu::ShaderModuleDescriptor {
                label: Some("fnv1a"),
                source: wgpu::ShaderSource::Wgsl(HASH_SHADER.into()),
            });

        let input = self
            .device
            .create_buffer_init(&wgpu::util::BufferInitDescriptor {
                label: Some("input"),
                contents: bytemuck::cast_slice(data),
                usage: wgpu::BufferUsages::STORAGE,
            });
        let out_bytes = (n_records * 4) as u64;
        let output = self.device.create_buffer(&wgpu::BufferDescriptor {
            label: Some("output"),
            size: out_bytes,
            usage: wgpu::BufferUsages::STORAGE | wgpu::BufferUsages::COPY_SRC,
            mapped_at_creation: false,
        });
        // A separate host-visible buffer: storage buffers cannot be mapped, so
        // the result is copied here before being read back.
        let readback = self.device.create_buffer(&wgpu::BufferDescriptor {
            label: Some("readback"),
            size: out_bytes,
            usage: wgpu::BufferUsages::COPY_DST | wgpu::BufferUsages::MAP_READ,
            mapped_at_creation: false,
        });
        let params: [u32; 2] = [record_words as u32, n_records as u32];
        let params_buf = self
            .device
            .create_buffer_init(&wgpu::util::BufferInitDescriptor {
                label: Some("params"),
                contents: bytemuck::cast_slice(&params),
                usage: wgpu::BufferUsages::UNIFORM,
            });

        let pipeline = self
            .device
            .create_compute_pipeline(&wgpu::ComputePipelineDescriptor {
                label: Some("fnv1a"),
                layout: None,
                module: &module,
                entry_point: Some("main"),
                compilation_options: Default::default(),
                cache: None,
            });
        let bind_group = self.device.create_bind_group(&wgpu::BindGroupDescriptor {
            label: None,
            layout: &pipeline.get_bind_group_layout(0),
            entries: &[
                wgpu::BindGroupEntry {
                    binding: 0,
                    resource: input.as_entire_binding(),
                },
                wgpu::BindGroupEntry {
                    binding: 1,
                    resource: output.as_entire_binding(),
                },
                wgpu::BindGroupEntry {
                    binding: 2,
                    resource: params_buf.as_entire_binding(),
                },
            ],
        });

        let mut encoder = self
            .device
            .create_command_encoder(&wgpu::CommandEncoderDescriptor { label: None });
        {
            let mut pass = encoder.begin_compute_pass(&wgpu::ComputePassDescriptor {
                label: None,
                timestamp_writes: None,
            });
            pass.set_pipeline(&pipeline);
            pass.set_bind_group(0, &bind_group, &[]);
            let groups = n_records.div_ceil(WORKGROUP) as u32;
            pass.dispatch_workgroups(groups, 1, 1);
        }
        encoder.copy_buffer_to_buffer(&output, 0, &readback, 0, out_bytes);
        self.queue.submit(Some(encoder.finish()));

        let slice = readback.slice(..);
        let (tx, rx) = std::sync::mpsc::channel();
        slice.map_async(wgpu::MapMode::Read, move |r| {
            let _ = tx.send(r);
        });
        // Drive the queue until the map completes; without this the callback
        // never fires and the read below blocks forever.
        self.device
            .poll(wgpu::PollType::Wait {
                submission_index: None,
                timeout: None,
            })
            .map_err(|e| format!("gpu poll failed: {e:?}"))?;
        rx.recv()
            .map_err(|e| format!("gpu readback channel closed: {e}"))?
            .map_err(|e| format!("gpu readback failed: {e:?}"))?;

        let view = slice
            .get_mapped_range()
            .map_err(|e| format!("gpu buffer map failed: {e:?}"))?;
        let out = bytemuck::cast_slice::<u8, u32>(&view).to_vec();
        drop(view);
        readback.unmap();
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample(n_records: usize, record_words: usize) -> Vec<u32> {
        (0..n_records * record_words)
            .map(|i| (i as u32).wrapping_mul(2654435761))
            .collect()
    }

    #[test]
    fn the_cpu_reference_is_stable_and_per_record() {
        // Two identical records must hash identically, and a one-bit change
        // must not. This pins the reference itself, so a GPU/CPU mismatch
        // later is unambiguous about which side moved.
        let a = hash_records_cpu(&[1, 2, 3, 4, 1, 2, 3, 4], 4);
        assert_eq!(a.len(), 2);
        assert_eq!(a[0], a[1]);
        let b = hash_records_cpu(&[1, 2, 3, 5], 4);
        assert_ne!(a[0], b[0]);
    }

    #[test]
    fn a_zero_width_record_yields_nothing_rather_than_dividing_by_zero() {
        assert!(hash_records_cpu(&[1, 2, 3], 0).is_empty());
    }

    /// Requires a working GPU, so it is opt-in: CI runners have none, and a
    /// test that silently passes on the CPU would be worse than no test.
    ///
    ///   DUCKLE_GPU_TEST=1 cargo test -p duckle-gpu -- --ignored --nocapture
    #[test]
    #[ignore = "needs a GPU; set DUCKLE_GPU_TEST=1"]
    fn gpu_hash_matches_the_cpu_reference_exactly() {
        if std::env::var("DUCKLE_GPU_TEST").is_err() {
            return;
        }
        let gpu = Gpu::detect().expect("no GPU adapter available");
        println!("  device: {:?}", gpu.info());
        let record_words = 8;
        for n in [1usize, 63, 64, 65, 1000, 100_000] {
            let data = sample(n, record_words);
            let got = gpu.hash_records(&data, record_words).expect("gpu hash");
            let want = hash_records_cpu(&data, record_words);
            assert_eq!(got, want, "mismatch at {n} records");
        }
        println!("  gpu matches cpu across all sizes");
    }
}
