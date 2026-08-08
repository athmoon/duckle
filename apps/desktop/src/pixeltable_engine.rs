//! On-demand provisioning of a Python with `pixeltable` for the Pixeltable
//! source and sink (#223).
//!
//! Pixeltable is a Python library, so unlike DuckDB or the Lance sidecar there
//! is no binary to fetch. We create a dedicated virtualenv with uv (which
//! brings its own standalone Python) and install `pixeltable` into it, then
//! publish that interpreter as DUCKLE_PIXELTABLE_PYTHON for the engine's
//! `resolve_pixeltable_python()`.
//!
//! Everything lives under `<app_data>/pixeltable/`, isolated from any system
//! Python, so nothing is installed into the user's environment and removing
//! that directory removes it completely.
//!
//! Deliberately NOT provisioned at startup: this is a ~1 GB dependency tree for
//! a connector most workspaces never touch, so it is fetched the first time a
//! Pixeltable node actually runs. `publish_if_present` is the cheap
//! startup-safe half that only wires up an install that already exists.

use std::path::{Path, PathBuf};

/// Windows: suppress the console window a GUI process would otherwise flash
/// when spawning uv. No-op elsewhere.
fn no_window(cmd: &mut std::process::Command) {
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        cmd.creation_flags(0x0800_0000); // CREATE_NO_WINDOW
    }
    let _ = cmd;
}

fn root(app_data: &Path) -> PathBuf {
    app_data.join("pixeltable")
}

/// The interpreter inside the provisioned venv.
pub fn python_path(app_data: &Path) -> PathBuf {
    let venv = root(app_data).join("venv");
    if cfg!(windows) {
        venv.join("Scripts").join("python.exe")
    } else {
        venv.join("bin").join("python")
    }
}

pub fn is_installed(app_data: &Path) -> bool {
    python_path(app_data).exists()
}

/// Publish an existing install as DUCKLE_PIXELTABLE_PYTHON. No network, no
/// work when absent, so it is safe on every startup.
pub fn publish_if_present(app_data: &Path) {
    let p = python_path(app_data);
    if p.exists() {
        std::env::set_var("DUCKLE_PIXELTABLE_PYTHON", &p);
    }
}

/// Create the venv and install pixeltable, returning the interpreter path.
///
/// Idempotent: an existing interpreter is published and returned without
/// touching the network, so a second Pixeltable node in the same run is free.
pub fn ensure(app_data: &Path) -> Result<PathBuf, String> {
    let python = python_path(app_data);
    if python.exists() {
        std::env::set_var("DUCKLE_PIXELTABLE_PYTHON", &python);
        return Ok(python);
    }
    // Reuse the dbt module's uv: it already handles "system uv, else a
    // previously downloaded one, else fetch it", and having two copies of that
    // logic is how they drift apart.
    let uv = crate::dbt_engine::ensure_uv(app_data)?;
    let venv = root(app_data).join("venv");
    std::fs::create_dir_all(root(app_data)).map_err(|e| e.to_string())?;

    let mut mk = std::process::Command::new(&uv);
    no_window(&mut mk);
    let out = mk
        .args(["venv", "--python", "3.12"])
        .arg(&venv)
        .output()
        .map_err(|e| format!("run uv venv: {e}"))?;
    if !out.status.success() {
        return Err(format!(
            "uv venv failed: {}",
            tail(&String::from_utf8_lossy(&out.stderr))
        ));
    }

    let mut install = std::process::Command::new(&uv);
    no_window(&mut install);
    let out = install
        .args(["pip", "install", "pixeltable"])
        .arg("--python")
        .arg(&python)
        .output()
        .map_err(|e| format!("run uv pip install: {e}"))?;
    if !python.exists() || !out.status.success() {
        return Err(format!(
            "installing pixeltable failed: {}",
            tail(&String::from_utf8_lossy(&out.stderr))
        ));
    }
    std::env::set_var("DUCKLE_PIXELTABLE_PYTHON", &python);
    Ok(python)
}

/// Keep the end of a long tool error, which is where the cause usually is.
fn tail(s: &str) -> String {
    let t = s.trim();
    t.lines().rev().take(8).collect::<Vec<_>>().join(" | ")
}
