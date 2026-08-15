//! The `serve` subcommand: a lightweight web management console for running
//! and monitoring Duckle pipelines on a server, with no desktop app.
//!
//! It hosts a small self-contained web panel (embedded HTML, no Node, no extra
//! binary) backed by a tiny std-only HTTP server, so the whole console ships
//! inside the runner you already deploy. The panel has three views:
//!   - Operations: run history across all pipelines (status, duration, rows,
//!     errors) plus per-pipeline run logs.
//!   - Pipelines:  every pipeline in the workspace with its last status and an
//!     editable interval schedule.
//!   - Run:        trigger any pipeline on demand and see the result.
//!
//! Runs execute in-process through the same engine as `duckle-runner run`, are
//! serialized by a single lock (so a manual run and a scheduled run never
//! collide on the shared workspace env), and append the same run history
//! (`<workspace>/runs/<id>.json`) and NDJSON logs (`<workspace>/logs/<id>/`)
//! the desktop and runner already write. A background scheduler triggers any
//! pipeline whose interval has elapsed. Reaching it means being able to run
//! any pipeline in the workspace, so it is open only on loopback and refuses
//! to start on any other host without a credential: see console_auth.

use duckle_duckdb_engine::{append_run_record, load_run_history, DuckdbEngine, PipelineDoc, RunRecord};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

const PANEL_HTML: &str = include_str!("panel.html");
const SIGNIN_HTML: &str = include_str!("signin.html");

use crate::audit;
use crate::console_auth;

struct ServeArgs {
    host: String,
    port: u16,
    workspace: PathBuf,
    duckdb: Option<PathBuf>,
    tick_interval: Duration,
    /// Console credential. Prefer DUCKLE_CONSOLE_TOKEN: an argument is visible
    /// to anyone who can list processes on the host.
    token: Option<String>,
}

fn parse_serve_args() -> Result<ServeArgs, String> {
    let mut host = "127.0.0.1".to_string();
    let mut port: u16 = 8080;
    let mut workspace: Option<PathBuf> = None;
    let mut duckdb: Option<PathBuf> = None;
    let mut tick_secs: Option<u64> = None;
    let mut token: Option<String> = None;
    let mut it = std::env::args().skip(2);
    while let Some(arg) = it.next() {
        let mut take = |label: &str| it.next().ok_or_else(|| format!("{} needs a value", label));
        match arg.as_str() {
            "--host" => host = take("--host")?,
            "--port" => {
                port = take("--port")?
                    .parse()
                    .map_err(|_| "--port must be a number".to_string())?
            }
            "--workspace" => workspace = Some(PathBuf::from(take("--workspace")?)),
            "--duckdb" => duckdb = Some(PathBuf::from(take("--duckdb")?)),
            "--token" => token = Some(take("--token")?),
            "--tick-interval" => {
                tick_secs = Some(
                    take("--tick-interval")?
                        .parse()
                        .map_err(|_| "--tick-interval must be a number (seconds)".to_string())?,
                )
            }
            "-h" | "--help" => {
                println!(
                    "duckle-runner serve - web management console\n\n\
                     USAGE:\n    duckle-runner serve [--host <ip>] [--port <n>] [--workspace <dir>] [--duckdb <path>] [--tick-interval <secs>]\n\n\
                     OPTIONS:\n    \
                     --host <ip>            Bind address (default 127.0.0.1; use 0.0.0.0 for remote access)\n    \
                     --port <n>             Port (default 8080)\n    \
                     --workspace <dir>      Workspace root holding pipelines, runs/, logs/ (default: current dir)\n    \
                     --duckdb <path>        DuckDB CLI (default: DUCKLE_DUCKDB_BIN, sibling bin/duckdb, or PATH)\n    \
                     --tick-interval <secs> Scheduler poll cadence in seconds (default 15; also DUCKLE_TICK_INTERVAL)\n    \
                     --token <secret>       Shared sign-in token (also DUCKLE_CONSOLE_TOKEN)\n\n\
                     On 127.0.0.1 with no accounts the console is open, because reaching it\n\
                     means already being on the machine. Any other --host REFUSES TO START\n\
                     without a credential: pass --token, set DUCKLE_CONSOLE_TOKEN, or give\n\
                     people their own with `duckle-runner console add-user <name> --role ...`.\n\
                     Put it behind a reverse proxy if you need TLS."
                );
                std::process::exit(0);
            }
            other => return Err(format!("unknown serve argument: {}", other)),
        }
    }
    let workspace = workspace.unwrap_or_else(|| PathBuf::from("."));
    // Poll cadence: --tick-interval flag > DUCKLE_TICK_INTERVAL env > 15s default.
    let tick_interval = Duration::from_secs(
        tick_secs
            .or_else(|| {
                std::env::var("DUCKLE_TICK_INTERVAL")
                    .ok()
                    .and_then(|s| s.parse().ok())
            })
            .filter(|n| *n > 0)
            .unwrap_or(15),
    );
    Ok(ServeArgs { host, port, workspace, duckdb, tick_interval, token })
}

/// Bounds how many pipelines execute at once.
///
/// Runs used to be serialized outright. The stated reason was that the
/// workspace env vars are shared, but those are set once at startup and do not
/// vary per run, so the real constraint is resources: each concurrent run gets
/// its own DUCKLE_MEMORY_LIMIT and its own DuckDB child, so N at once needs
/// roughly N times the memory and spawns N times the threads.
///
/// So the default stays 1, byte-for-byte the old behaviour, and a power user
/// with cores and memory to spare raises it. Independent DuckDB queries were
/// measured scaling about 3.8x across 8 concurrent processes on a 20-core box,
/// which is where the headroom is - not in splitting one query, which measured
/// slower.
struct RunGate {
    /// Permits currently free. Guarded by the mutex, waited on via the condvar.
    free: Mutex<usize>,
    ready: Condvar,
}

impl RunGate {
    fn new(permits: usize) -> Self {
        RunGate { free: Mutex::new(permits.max(1)), ready: Condvar::new() }
    }

    /// Block until a permit is free, then hold it until the guard drops.
    fn acquire(&self) -> RunPermit<'_> {
        let mut free = self.free.lock().unwrap_or_else(|p| p.into_inner());
        while *free == 0 {
            free = self.ready.wait(free).unwrap_or_else(|p| p.into_inner());
        }
        *free -= 1;
        RunPermit { gate: self }
    }
}

struct RunPermit<'a> {
    gate: &'a RunGate,
}

impl Drop for RunPermit<'_> {
    fn drop(&mut self) {
        let mut free = self.gate.free.lock().unwrap_or_else(|p| p.into_inner());
        *free += 1;
        // One permit freed wakes one waiter.
        self.gate.ready.notify_one();
    }
}

/// How many pipelines may run concurrently. 1 (the default) serializes them.
fn max_concurrent_runs() -> usize {
    std::env::var("DUCKLE_MAX_CONCURRENT_RUNS")
        .ok()
        .and_then(|v| v.trim().parse::<usize>().ok())
        .filter(|n| *n > 0)
        .unwrap_or(1)
}

struct State {
    workspace: PathBuf,
    duckdb: PathBuf,
    /// Bounds concurrent pipeline execution. Defaults to one at a time; raise
    /// with DUCKLE_MAX_CONCURRENT_RUNS. See [`RunGate`].
    run_lock: RunGate,
    /// Pipeline ids currently executing, so the console can show a live
    /// "Running" status (discussion #155). Populated for the duration of a run.
    running: Mutex<std::collections::HashSet<String>>,
    /// Who may call this console and what they may do. Decided once at
    /// startup, because a bind that cannot be authenticated must not serve at
    /// all rather than serve and warn.
    console: console_auth::Console,
    /// Bind host, for the cross-origin / DNS-rebind guard on state-changing
    /// routes. The web editor has had this since it shipped; the console did
    /// not, which left the default loopback console drivable by any page the
    /// operator happened to visit.
    host: String,
    /// Scheduler poll cadence (issue #135). Default 15s; overridable via
    /// --tick-interval or DUCKLE_TICK_INTERVAL.
    tick_interval: Duration,
}

pub fn run() -> Result<(), String> {
    let args = parse_serve_args()?;
    let workspace = args
        .workspace
        .canonicalize()
        .unwrap_or_else(|_| args.workspace.clone());
    let duckdb = crate::resolve_duckdb(args.duckdb.clone())?;

    // Set the workspace env once for the process; runs are serialized so these
    // stay consistent for every execution (matches the runner's run path).
    std::env::set_var("DUCKLE_DUCKDB_BIN", &duckdb);
    std::env::set_var("DUCKLE_WORKSPACE", &workspace);
    std::env::set_var("DUCKLE_LOG_DIR", workspace.join("logs"));
    apply_workspace_memory_limit(&workspace);

    // Decide who may use this console before binding anything. An exposed bind
    // with no credential is an error here, not a warning: the console can run
    // any pipeline in the workspace, so serving it to the network unauthenticated
    // is remote code execution, and a warning in a service log is not a control.
    let token = args
        .token
        .clone()
        .or_else(|| std::env::var("DUCKLE_CONSOLE_TOKEN").ok())
        .filter(|t| !t.trim().is_empty());
    let console = console_auth::Console::configure(&workspace, &args.host, token.as_deref())?;
    let console_open = console.is_open();

    let state = Arc::new(State {
        workspace: workspace.clone(),
        duckdb: duckdb.clone(),
        run_lock: RunGate::new(max_concurrent_runs()),
        running: Mutex::new(std::collections::HashSet::new()),
        console,
        host: args.host.clone(),
        tick_interval: args.tick_interval,
    });

    // Fold any pre-unification console store into schedules.json before the
    // scheduler reads it, so an existing install keeps firing across the change.
    migrate_legacy_schedules(&workspace);

    spawn_scheduler(state.clone());

    let addr = format!("{}:{}", args.host, args.port);
    let listener = TcpListener::bind(&addr).map_err(|e| format!("bind {}: {}", addr, e))?;
    eprintln!("duckle-runner: management console on http://{}", addr);
    eprintln!("duckle-runner: workspace {}", workspace.display());
    eprintln!("duckle-runner: DuckDB {}", duckdb.display());
    if console_open {
        eprintln!("duckle-runner: no token set; reachable only from this machine");
    } else {
        eprintln!("duckle-runner: sign-in required");
    }

    for stream in listener.incoming() {
        match stream {
            Ok(s) => {
                let st = state.clone();
                std::thread::spawn(move || {
                    if let Err(e) = handle(s, &st) {
                        eprintln!("duckle-runner: request error: {}", e);
                    }
                });
            }
            Err(e) => eprintln!("duckle-runner: accept error: {}", e),
        }
    }
    Ok(())
}

// ── Web editor mode (#75 phase 2 spike): serve the full frontend + an
//    HTTP command bridge so the React editor runs in a browser, backed by the
//    server-side engine/filesystem. Single-tenant, no auth (localhost / proxy).

struct WebArgs {
    host: String,
    port: u16,
    workspace: PathBuf,
    duckdb: Option<PathBuf>,
    dist: PathBuf,
    /// Editor credential. Prefer DUCKLE_CONSOLE_TOKEN over an argument, which
    /// anyone who can list processes on the host can read.
    token: Option<String>,
}

fn parse_web_args() -> Result<WebArgs, String> {
    let mut host = "127.0.0.1".to_string();
    let mut port: u16 = 8090;
    let mut workspace: Option<PathBuf> = None;
    let mut duckdb: Option<PathBuf> = None;
    let mut dist: Option<PathBuf> = None;
    let mut token: Option<String> = None;
    let mut it = std::env::args().skip(2);
    while let Some(arg) = it.next() {
        let mut take = |label: &str| it.next().ok_or_else(|| format!("{} needs a value", label));
        match arg.as_str() {
            "--host" => host = take("--host")?,
            "--port" => {
                port = take("--port")?.parse().map_err(|_| "--port must be a number".to_string())?
            }
            "--workspace" => workspace = Some(PathBuf::from(take("--workspace")?)),
            "--duckdb" => duckdb = Some(PathBuf::from(take("--duckdb")?)),
            "--dist" => dist = Some(PathBuf::from(take("--dist")?)),
            "--token" => token = Some(take("--token")?),
            "-h" | "--help" => {
                println!(
                    "duckle-runner web - serve the Duckle editor as a web app (spike)\n\n\
                     USAGE:\n    duckle-runner web --dist <dir> [--host <ip>] [--port <n>] [--workspace <dir>] [--token <secret>]\n\n\
                     Same accounts and roles as `duckle-runner serve`: open on 127.0.0.1\n\
                     with no accounts, and REFUSES TO START on any other --host without a\n\
                     credential (--token, DUCKLE_CONSOLE_TOKEN, or `console add-user`)."
                );
                std::process::exit(0);
            }
            other => return Err(format!("unknown web argument: {}", other)),
        }
    }
    Ok(WebArgs {
        host,
        port,
        workspace: workspace.unwrap_or_else(|| PathBuf::from(".")),
        duckdb,
        dist: dist.ok_or("web mode needs --dist <frontend dist dir>")?,
        token,
    })
}

struct WebState {
    workspace: PathBuf,
    duckdb: PathBuf,
    dist: PathBuf,
    /// Bind host, for the cross-origin / DNS-rebind guard on POST routes.
    host: String,
    /// Bounds concurrent runs from the browser. One at a time by default; raise
    /// with DUCKLE_MAX_CONCURRENT_RUNS. See [`RunGate`].
    run_lock: RunGate,
    /// Who may use this editor. Same policy object the console uses, so one
    /// set of accounts covers both.
    console: console_auth::Console,
}

pub fn run_web() -> Result<(), String> {
    let args = parse_web_args()?;
    let workspace = args.workspace.canonicalize().unwrap_or_else(|_| args.workspace.clone());
    // Drop the Windows extended-length prefix (\\?\) so the path the browser
    // sees and echoes back in /api/fs calls stays a plain C:\... path.
    let workspace = {
        let s = workspace.to_string_lossy().to_string();
        PathBuf::from(s.strip_prefix(r"\\?\").map(|x| x.to_string()).unwrap_or(s))
    };
    let duckdb = crate::resolve_duckdb(args.duckdb.clone())?;
    let dist = args.dist.canonicalize().map_err(|e| format!("--dist {}: {}", args.dist.display(), e))?;
    std::env::set_var("DUCKLE_DUCKDB_BIN", &duckdb);
    std::env::set_var("DUCKLE_WORKSPACE", &workspace);
    std::env::set_var("DUCKLE_LOG_DIR", workspace.join("logs"));
    apply_workspace_memory_limit(&workspace);
    // The editor writes files, edits connections and runs pipelines, so it is
    // at least as powerful as the console and gets the same rule: loopback is
    // open, anything else needs a credential before the socket is bound.
    let token = args
        .token
        .clone()
        .or_else(|| std::env::var("DUCKLE_CONSOLE_TOKEN").ok())
        .filter(|t| !t.trim().is_empty());
    let console = console_auth::Console::configure(&workspace, &args.host, token.as_deref())?;
    let console_open = console.is_open();

    let state = Arc::new(WebState {
        workspace: workspace.clone(),
        duckdb: duckdb.clone(),
        dist: dist.clone(),
        host: args.host.clone(),
        run_lock: RunGate::new(max_concurrent_runs()),
        console,
    });
    let addr = format!("{}:{}", args.host, args.port);
    let listener = TcpListener::bind(&addr).map_err(|e| format!("bind {}: {}", addr, e))?;
    eprintln!("duckle-runner: web editor on http://{}", addr);
    eprintln!("duckle-runner: workspace {}", workspace.display());
    eprintln!("duckle-runner: serving {}", dist.display());
    if console_open {
        eprintln!("duckle-runner: no token set; reachable only from this machine");
    } else {
        eprintln!("duckle-runner: sign-in required");
    }
    for stream in listener.incoming() {
        match stream {
            Ok(s) => {
                let st = state.clone();
                std::thread::spawn(move || {
                    if let Err(e) = handle_web(s, &st) {
                        eprintln!("duckle-runner: request error: {}", e);
                    }
                });
            }
            Err(e) => eprintln!("duckle-runner: accept error: {}", e),
        }
    }
    Ok(())
}

/// Exchange a token for a session cookie, for the editor.
fn web_sign_in(stream: &mut TcpStream, state: &WebState, req: &Request) -> Result<(), String> {
    let body: Value = serde_json::from_slice(&req.body).unwrap_or(json!({}));
    let token = body.get("token").and_then(|v| v.as_str()).unwrap_or("");
    match state.console.sign_in(token) {
        Some((sid, who)) => {
            audit::record(&state.workspace, Some(&who), "session.sign_in", "editor", audit::Outcome::Allowed);
            let payload = json!({ "label": who.label, "role": who.role.as_str() }).to_string();
            let head = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\
                 Set-Cookie: {}={}; HttpOnly; SameSite=Strict; Path=/\r\nConnection: close\r\n\r\n",
                payload.len(),
                console_auth::SESSION_COOKIE,
                sid
            );
            stream.write_all(head.as_bytes()).map_err(|e| e.to_string())?;
            stream.write_all(payload.as_bytes()).map_err(|e| e.to_string())
        }
        None => {
            audit::record(&state.workspace, None, "session.sign_in", "editor", audit::Outcome::Unauthenticated);
            respond_err(stream, "401 Unauthorized", "that token was not accepted")
        }
    }
}

fn handle_web(mut stream: TcpStream, state: &WebState) -> Result<(), String> {
    let req = read_request(&mut stream)?;
    // Block cross-origin / non-local state-changing POSTs (CSRF + DNS-rebind).
    if req.method == "POST" && req.path.starts_with("/api/") && !guard_local(&req, &state.host) {
        return respond_403(&mut stream, "blocked: cross-origin or non-local request");
    }
    if req.method == "POST" && req.path == "/api/session" {
        return web_sign_in(&mut stream, state, &req);
    }
    // Parse the route ONCE, here, and let both the gate and the dispatcher use
    // the result. They used to parse the path separately - the gate with
    // `starts_with("/api/cmd/connection")` and the dispatcher with
    // `trim_start_matches("/api/cmd/")` - and `trim_start_matches` strips its
    // prefix REPEATEDLY. So `/api/cmd//api/cmd/connection_decrypt_payload` was
    // not "a connection command" to the gate, which asked only for operator,
    // and was exactly `connection_decrypt_payload` to the dispatcher, which
    // decrypted the workspace's stored credentials. Two parsers over one string
    // is the bug; one parser is the fix.
    let cmd = req.path.strip_prefix("/api/cmd/");
    let fs_op = req.path.strip_prefix("/api/fs/");

    // The editor has no read-only mode: opening it means loading a workspace to
    // change it, so the whole surface needs operator. Anything touching
    // connections, which is to say credentials, needs admin.
    let needed = match cmd {
        Some(c) if c.starts_with("connection") => console_auth::Role::Admin,
        _ => console_auth::Role::Operator,
    };
    let action = if req.path.starts_with("/api/") { "editor.api" } else { "editor.open" };
    let who = state.console.identify(req.authorization.as_deref(), req.cookie.as_deref());
    let Some(who) = who else {
        audit::record(&state.workspace, None, action, &req.path, audit::Outcome::Unauthenticated);
        if req.method == "GET" && !req.path.starts_with("/api/") {
            return respond(&mut stream, "401 Unauthorized", "text/html; charset=utf-8", SIGNIN_HTML.as_bytes());
        }
        return respond_err(&mut stream, "401 Unauthorized", "sign in to use the editor");
    };
    if !who.role.allows(needed) {
        audit::record(&state.workspace, Some(&who), action, &req.path, audit::Outcome::Denied);
        return respond_403(
            &mut stream,
            &format!("this needs the {} role; you have {}", needed.as_str(), who.role.as_str()),
        );
    }
    if req.method == "POST" {
        audit::record(&state.workspace, Some(&who), action, &req.path, audit::Outcome::Allowed);
    }
    if req.method == "POST" {
        if let Some(cmd) = cmd {
            let cmd = cmd.to_string();
            // A panic inside a command (e.g. a source that misbehaves during a
            // live drift read) would otherwise unwind this connection's thread
            // and drop the socket, which the browser can only report as an
            // opaque "Failed to fetch". Catch it and answer with a real 500 the
            // editor can show.
            let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                dispatch_cmd(&mut stream, state, &cmd, &req.body)
            }));
            return match outcome {
                Ok(r) => r,
                Err(_) => respond_err(
                    &mut stream,
                    "500 Internal Server Error",
                    &format!("command '{cmd}' failed unexpectedly"),
                ),
            };
        }
        if let Some(op) = fs_op {
            return dispatch_fs(&mut stream, state, &op.to_string(), &req.body);
        }
    }
    if req.method == "POST" && req.path == "/api/run_stream" {
        return run_stream(&mut stream, state, &req.body);
    }
    if req.method == "POST" && req.path == "/api/inspect" {
        return inspect_schema(&mut stream, state, &req.body);
    }
    // Static frontend: map the URL path into the dist dir; unknown non-asset
    // paths fall back to index.html (SPA routing).
    serve_static(&mut stream, state, &req.path)
}

/// Server-side filesystem bridge for the web editor. The browser cannot touch
/// the server's disk, so the frontend's workspace file ops (read/write/list)
/// route here. Every path is confined to the workspace dir (no traversal out).
fn dispatch_fs(stream: &mut TcpStream, state: &WebState, op: &str, body: &[u8]) -> Result<(), String> {
    let args: Value = serde_json::from_slice(body).unwrap_or(Value::Null);
    let path_arg = args.get("path").and_then(|v| v.as_str()).unwrap_or("");
    let target = match confine_to_workspace(&state.workspace, path_arg) {
        Ok(p) => p,
        Err(e) => return respond_err(stream, "400 Bad Request", &e),
    };
    match op {
        "exists" => respond_json(stream, &serde_json::json!({ "exists": target.exists() })),
        "read" => match std::fs::read_to_string(&target) {
            Ok(content) => respond_json(stream, &serde_json::json!({ "content": content })),
            Err(e) => respond_err(stream, "404 Not Found", &e.to_string()),
        },
        "write" => {
            let content = args.get("content").and_then(|v| v.as_str()).unwrap_or("");
            if let Some(parent) = target.parent() {
                let _ = std::fs::create_dir_all(parent);
            }
            match std::fs::write(&target, content) {
                Ok(()) => respond_json(stream, &serde_json::json!({ "ok": true })),
                Err(e) => respond_err(stream, "500 Internal Server Error", &e.to_string()),
            }
        }
        "mkdir" => match std::fs::create_dir_all(&target) {
            Ok(()) => respond_json(stream, &serde_json::json!({ "ok": true })),
            Err(e) => respond_err(stream, "500 Internal Server Error", &e.to_string()),
        },
        "remove" => {
            let r = if target.is_dir() { std::fs::remove_dir_all(&target) } else { std::fs::remove_file(&target) };
            match r {
                Ok(()) => respond_json(stream, &serde_json::json!({ "ok": true })),
                Err(e) => respond_err(stream, "500 Internal Server Error", &e.to_string()),
            }
        }
        "readdir" => {
            let mut entries = Vec::new();
            if let Ok(rd) = std::fs::read_dir(&target) {
                for e in rd.flatten() {
                    let ft = e.file_type();
                    entries.push(serde_json::json!({
                        "name": e.file_name().to_string_lossy(),
                        "isFile": ft.as_ref().map(|t| t.is_file()).unwrap_or(false),
                        "isDirectory": ft.as_ref().map(|t| t.is_dir()).unwrap_or(false),
                    }));
                }
            }
            respond_json(stream, &Value::Array(entries))
        }
        _ => respond_err(stream, "404 Not Found", &format!("unknown fs op: {}", op)),
    }
}

/// Resolve `path` (absolute or relative) and ensure it stays inside the
/// workspace. Lexical normalization (no symlink follow needed) is enough since
/// we only ever read/write plain files the editor created.
fn confine_to_workspace(workspace: &Path, path: &str) -> Result<PathBuf, String> {
    if path.is_empty() {
        return Err("path required".into());
    }
    let raw = PathBuf::from(path.replace('\\', "/"));
    let joined = if raw.is_absolute() { raw } else { workspace.join(raw) };
    // Normalize . and .. lexically.
    let mut normalized = PathBuf::new();
    for comp in joined.components() {
        use std::path::Component::*;
        match comp {
            ParentDir => {
                normalized.pop();
            }
            CurDir => {}
            other => normalized.push(other.as_os_str()),
        }
    }
    // Compare normalized strings: tolerate \ vs /, the \\?\ prefix, and (on
    // Windows) case so the browser-built path matches the server workspace.
    let norm = |p: &Path| {
        p.to_string_lossy()
            .replace('\\', "/")
            .trim_start_matches("//?/")
            .trim_end_matches('/')
            .to_lowercase()
    };
    if !norm(&normalized).starts_with(&norm(workspace)) {
        return Err("path escapes the workspace".into());
    }
    Ok(normalized)
}

/// The body of the two connection-secret commands, split out from the socket
/// so a test can drive the real path instead of re-implementing it. Encrypting
/// is strict: a failure must surface, never fall through to writing plaintext.
fn connection_secret_cmd(workspace: &Path, cmd: &str, body: &[u8]) -> Result<String, String> {
    let args: Value = serde_json::from_slice(body).unwrap_or(Value::Null);
    let payload = args.get("payloadJson").and_then(|v| v.as_str()).unwrap_or("null");
    if cmd == "connection_encrypt_payload" {
        duckle_secrets::encrypt_payload_json(workspace, payload)
    } else {
        duckle_secrets::decrypt_payload_json(workspace, payload)
    }
}

fn dispatch_cmd(stream: &mut TcpStream, state: &WebState, cmd: &str, body: &[u8]) -> Result<(), String> {
    match cmd {
        // Drives the editor's runtime indicator offline -> ready.
        "ping" => respond_json(stream, &Value::String("pong".into())),
        // Connection secrets, encrypted at rest with the same AES-256-GCM
        // primitives and the same per-workspace key the desktop app uses, so a
        // workspace stays readable whichever edition wrote it.
        //
        // These two used to echo the payload back unchanged, which meant the
        // self-hosted web edition wrote passwords to connections/*.json in
        // clear text while the desktop encrypted them - the same product
        // quietly downgrading its own security depending on how it was
        // launched. Encrypt is strict, because failing to encrypt must never
        // fall through to writing plaintext. Decrypt is lenient by design:
        // when there is no key yet, or a field is still plain, the payload is
        // returned as-is so connections saved before this change keep opening.
        "connection_encrypt_payload" | "connection_decrypt_payload" => {
            match connection_secret_cmd(&state.workspace, cmd, body) {
                Ok(out) => respond_json(stream, &Value::String(out)),
                Err(e) => respond_err(
                    stream,
                    "500 Internal Server Error",
                    &format!("connection secrets: {e}"),
                ),
            }
        }
        // Execute a pipeline on the server engine and return the RunResult (the
        // same shape the desktop returns). The frontend reads the final result
        // from this response; live per-stage events (the Channel) are not
        // streamed in the MVP. Concurrency is bounded by run_lock (1 by default).
        "run_pipeline" => {
            let args: Value = serde_json::from_slice(body).unwrap_or(Value::Null);
            let mut doc: PipelineDoc = match serde_json::from_value(args.get("pipeline").cloned().unwrap_or(Value::Null)) {
                Ok(d) => d,
                Err(e) => return respond_err(stream, "400 Bad Request", &format!("bad pipeline: {}", e)),
            };
            // Saved Salesforce connection refs resolve server-side against this
            // workspace (#166 stage 2) - the browser never sees the secret.
            if let Err(e) = duckle_secrets::resolve_connection_refs(&state.workspace, &mut doc.nodes) {
                return respond_err(stream, "400 Bad Request", &e);
            }
            // Same placeholder resolution as /api/run (execute_one) and the
            // desktop: expand ${ENV:KEY} secrets - so a connection field stored as
            // ${ENV:...} still resolves after ref injection (#166 stage 2) - and the
            // ${date}/${datetime} builtins, before the workspace-context pass.
            let env_file = state.workspace.join("secrets.env");
            if let Err(e) = crate::apply_env_pass(&mut doc, &state.workspace, &env_file) {
                return respond_err(stream, "400 Bad Request", &e);
            }
            duckle_duckdb_engine::context::apply_time_builtins(&mut doc);
            duckle_duckdb_engine::context::apply_workspace_context(&mut doc, &state.workspace);
            let name = args.get("pipelineName").and_then(|v| v.as_str()).unwrap_or("web").to_string();
            let _guard = state.run_lock.acquire();
            let engine = DuckdbEngine::new(state.duckdb.clone());
            let result = engine.execute_pipeline_named(&doc, &name);
            match serde_json::to_value(&result) {
                Ok(v) => respond_json(stream, &v),
                Err(e) => respond_err(stream, "500 Internal Server Error", &e.to_string()),
            }
        }
        // Compile to per-stage SQL for the Plan tab.
        "compile_pipeline" => {
            let args: Value = serde_json::from_slice(body).unwrap_or(Value::Null);
            let mut doc: PipelineDoc = match serde_json::from_value(args.get("pipeline").cloned().unwrap_or(Value::Null)) {
                Ok(d) => d,
                Err(e) => return respond_err(stream, "400 Bad Request", &format!("bad pipeline: {}", e)),
            };
            duckle_duckdb_engine::context::apply_workspace_context(&mut doc, &state.workspace);
            match duckle_duckdb_engine::compile_pipeline_sql(&doc) {
                Ok(stages) => match serde_json::to_value(&stages) {
                    Ok(v) => respond_json(stream, &v),
                    Err(e) => respond_err(stream, "500 Internal Server Error", &e.to_string()),
                },
                Err(e) => respond_err(stream, "400 Bad Request", &e.to_string()),
            }
        }
        "pipeline_column_lineage" => {
            let args: Value = serde_json::from_slice(body).unwrap_or(Value::Null);
            let mut doc: PipelineDoc = match serde_json::from_value(args.get("pipeline").cloned().unwrap_or(Value::Null)) {
                Ok(d) => d,
                Err(e) => return respond_err(stream, "400 Bad Request", &format!("bad pipeline: {}", e)),
            };
            duckle_duckdb_engine::context::apply_workspace_context(&mut doc, &state.workspace);
            let engine = DuckdbEngine::new(state.duckdb.clone());
            match engine.pipeline_column_lineage(&doc) {
                Ok(result) => match serde_json::to_value(&result) {
                    Ok(v) => respond_json(stream, &v),
                    Err(e) => respond_err(stream, "500 Internal Server Error", &e.to_string()),
                },
                Err(e) => respond_err(stream, "400 Bad Request", &e.to_string()),
            }
        }
        // Trust scorecard for the open pipeline (compile + structural risks +
        // ungoverned PII). Static by default; with checkDrift it also reads each
        // source's live schema (resolving ${workspace} against this server's
        // workspace first). Matches the desktop command and the MCP tool.
        "pipeline_trust_report" => {
            let args: Value = serde_json::from_slice(body).unwrap_or(Value::Null);
            let pipeline = args.get("pipeline").cloned().unwrap_or(Value::Null);
            let check_drift = args.get("checkDrift").and_then(|v| v.as_bool()).unwrap_or(false);
            if check_drift {
                if let Ok(mut doc) = serde_json::from_value::<PipelineDoc>(pipeline.clone()) {
                    duckle_duckdb_engine::context::apply_time_builtins(&mut doc);
                    duckle_duckdb_engine::context::apply_workspace_context(&mut doc, &state.workspace);
                    let resolved = match serde_json::to_value(&doc) {
                        Ok(v) => v,
                        Err(e) => return respond_err(stream, "500 Internal Server Error", &e.to_string()),
                    };
                    let engine = DuckdbEngine::new(state.duckdb.clone());
                    let report = duckle_duckdb_engine::trust::trust_report(&resolved, Some(&engine));
                    return respond_json(stream, &report);
                }
            }
            let report = duckle_duckdb_engine::trust::trust_report(&pipeline, None);
            respond_json(stream, &report)
        }
        // Tells the browser editor which server workspace it is editing, so it
        // can auto-load it (there is no native folder picker on the web).
        "web_bootstrap" => respond_json(
            stream,
            &serde_json::json!({ "workspace": state.workspace.to_string_lossy() }),
        ),
        // The browser build skips the engine-setup gate, but answer truthfully.
        "engine_status" => respond_json(
            stream,
            &serde_json::json!([{
                "id": "duckdb",
                "name": "DuckDB",
                "description": "DuckDB engine",
                "required": true,
                "installed": true,
                "outdated": false,
                "version": "1.5.4",
                "target_version": "1.5.4",
                "path": state.duckdb.to_string_lossy(),
                "available": true,
            }]),
        ),
        // Genuinely unknown commands get a real 404 (correct HTTP semantics for
        // typos and for non-browser callers like curl/tools). Desktop-only
        // commands the shared frontend still invokes on the web build are kept
        // graceful by the web shim, which maps a 404 to a null no-op so the
        // editor keeps booting.
        _ => respond_err(stream, "404 Not Found", &format!("unknown command: {}", cmd)),
    }
}

/// Run a pipeline and STREAM its progress to the browser as Server-Sent Events:
/// each engine PipelineEvent is a `data:` line; the final RunResult is an
/// `event: result` line. The frontend turns these back into the same live
/// per-node animation the desktop gets from the Tauri Channel.
fn run_stream(stream: &mut TcpStream, state: &WebState, body: &[u8]) -> Result<(), String> {
    let args: Value = serde_json::from_slice(body).unwrap_or(Value::Null);
    let mut doc: PipelineDoc = match serde_json::from_value(args.get("pipeline").cloned().unwrap_or(Value::Null)) {
        Ok(d) => d,
        Err(e) => return respond_err(stream, "400 Bad Request", &format!("bad pipeline: {}", e)),
    };
    // Saved Salesforce connection refs resolve server-side against this
    // workspace (#166 stage 2) - the browser never sees the secret.
    if let Err(e) = duckle_secrets::resolve_connection_refs(&state.workspace, &mut doc.nodes) {
        return respond_err(stream, "400 Bad Request", &e);
    }
    // Same placeholder resolution as /api/run (execute_one) and the desktop:
    // expand ${ENV:KEY} secrets - so a connection field stored as ${ENV:...}
    // still resolves after ref injection (#166 stage 2) - and the
    // ${date}/${datetime} builtins, before the workspace-context pass.
    let env_file = state.workspace.join("secrets.env");
    if let Err(e) = crate::apply_env_pass(&mut doc, &state.workspace, &env_file) {
        return respond_err(stream, "400 Bad Request", &e);
    }
    duckle_duckdb_engine::context::apply_time_builtins(&mut doc);
    duckle_duckdb_engine::context::apply_workspace_context(&mut doc, &state.workspace);
    let name = args.get("pipelineName").and_then(|v| v.as_str()).unwrap_or("web").to_string();
    // Optional run-to-here target: when set, the engine runs only the subgraph
    // up to and including this node (partial run).
    let target = args
        .get("targetNodeId")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string());
    // SSE response head (no Content-Length; we stream until the run ends).
    let head = "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nCache-Control: no-cache\r\nConnection: close\r\n\r\n";
    stream.write_all(head.as_bytes()).map_err(|e| e.to_string())?;
    stream.flush().map_err(|e| e.to_string())?;

    let _guard = state.run_lock.acquire();
    // A second handle to the same socket for the event callback (the run is
    // synchronous, so events stream first, the result line follows).
    let mut ev = stream.try_clone().map_err(|e| e.to_string())?;
    let engine = DuckdbEngine::new(state.duckdb.clone());
    let result = engine.execute_pipeline_with_events(&doc, target.as_deref(), Some(&name), |evt| {
        if let Ok(j) = serde_json::to_string(&evt) {
            let _ = ev.write_all(format!("data: {}\n\n", j).as_bytes());
            let _ = ev.flush();
        }
    });
    let rj = serde_json::to_string(&result).unwrap_or_else(|_| "{}".to_string());
    stream
        .write_all(format!("event: result\ndata: {}\n\n", rj).as_bytes())
        .map_err(|e| e.to_string())?;
    stream.flush().map_err(|e| e.to_string())?;
    Ok(())
}

/// Web-editor autodetect (issue #148). The browser cannot read the server's
/// sources, so schema inspection routes here and drives the SAME engine.inspect
/// the desktop `autodetect_schema` command uses: real driver reads, ${ENV:...}
/// resolved engine-side, and honest errors. Without this the web editor could
/// only fall back to a fabricated col_1/col_2/col_3 schema. The response shape
/// ({ columns, sampleRows }) matches the desktop InspectionPayload exactly.
fn inspect_schema(stream: &mut TcpStream, state: &WebState, body: &[u8]) -> Result<(), String> {
    let args: Value = serde_json::from_slice(body).unwrap_or(Value::Null);
    let format = args.get("format").and_then(|v| v.as_str()).unwrap_or("");
    if format.is_empty() {
        return respond_err(stream, "400 Bad Request", "inspect: missing format");
    }
    let options = args
        .get("options")
        .cloned()
        .unwrap_or_else(|| serde_json::json!({}));
    let engine = DuckdbEngine::new(state.duckdb.clone());
    match engine.inspect(format, options) {
        Ok(insp) => respond_json(
            stream,
            &serde_json::json!({ "columns": insp.schema, "sampleRows": insp.sample_rows }),
        ),
        Err(e) => respond_err(stream, "422 Unprocessable Entity", &e.to_string()),
    }
}

fn serve_static(stream: &mut TcpStream, state: &WebState, url_path: &str) -> Result<(), String> {
    let rel = url_path.trim_start_matches('/');
    let candidate = if rel.is_empty() { state.dist.join("index.html") } else { state.dist.join(rel) };
    // Confine to the dist dir, and SPA-fallback to index.html for non-asset paths.
    let file = match candidate.canonicalize() {
        Ok(p) if p.starts_with(&state.dist) && p.is_file() => p,
        _ => state.dist.join("index.html"),
    };
    match std::fs::read(&file) {
        Ok(bytes) => respond(stream, "200 OK", web_content_type(&file), &bytes),
        Err(e) => respond_err(stream, "404 Not Found", &format!("{}: {}", file.display(), e)),
    }
}

fn web_content_type(path: &Path) -> &'static str {
    match path.extension().and_then(|e| e.to_str()).unwrap_or("") {
        "html" => "text/html; charset=utf-8",
        "js" | "mjs" => "text/javascript; charset=utf-8",
        "css" => "text/css; charset=utf-8",
        "json" => "application/json",
        "svg" => "image/svg+xml",
        "png" => "image/png",
        "jpg" | "jpeg" => "image/jpeg",
        "webp" => "image/webp",
        "ico" => "image/x-icon",
        "woff2" => "font/woff2",
        "woff" => "font/woff",
        "ttf" => "font/ttf",
        "wasm" => "application/wasm",
        "map" => "application/json",
        _ => "application/octet-stream",
    }
}

// ── HTTP (minimal, std-only) ──

struct Request {
    method: String,
    path: String,
    query: HashMap<String, String>,
    origin: Option<String>,
    host: Option<String>,
    /// `Authorization: Bearer <token>`, for API clients.
    authorization: Option<String>,
    /// Raw `Cookie` header, carrying the console's session id for browsers.
    cookie: Option<String>,
    body: Vec<u8>,
}

/// How long a single read may stall before the connection is abandoned.
///
/// Generous per read, not per request, so a slow client on a bad link is fine.
/// Without any deadline a caller could open a socket, send one byte and park
/// the thread serving it forever - and since every connection gets its own
/// `std::thread::spawn` with no ceiling, a handful of those is the whole
/// server. It matters because this runs before anyone is identified: it is the
/// one part of the console an unauthenticated caller always reaches.
const READ_TIMEOUT: Duration = Duration::from_secs(30);

/// The largest request body that will be buffered.
///
/// `Content-Length` is the caller's own claim and was believed without limit,
/// so a declared and delivered 4 GiB was read into memory before anything
/// looked at who was asking. Pipeline documents and file writes are far below
/// this; anything above it is not a console request.
const MAX_BODY: usize = 32 << 20;

fn read_request(stream: &mut TcpStream) -> Result<Request, String> {
    // Both directions: a client that stops reading must not pin the thread on
    // a blocked write either.
    let _ = stream.set_read_timeout(Some(READ_TIMEOUT));
    let _ = stream.set_write_timeout(Some(READ_TIMEOUT));
    // Read until the end of headers (\r\n\r\n), then the body by Content-Length.
    let mut buf = Vec::with_capacity(2048);
    let mut tmp = [0u8; 2048];
    let header_end;
    loop {
        let n = stream.read(&mut tmp).map_err(|e| e.to_string())?;
        if n == 0 {
            return Err("connection closed before request".into());
        }
        buf.extend_from_slice(&tmp[..n]);
        if let Some(pos) = find_subslice(&buf, b"\r\n\r\n") {
            header_end = pos;
            break;
        }
        if buf.len() > 1 << 20 {
            return Err("request headers too large".into());
        }
    }
    let head = String::from_utf8_lossy(&buf[..header_end]).to_string();
    let mut lines = head.split("\r\n");
    let request_line = lines.next().ok_or("empty request")?;
    let mut parts = request_line.split_whitespace();
    let method = parts.next().unwrap_or("GET").to_string();
    let raw_target = parts.next().unwrap_or("/").to_string();
    let (path, query) = split_query(&raw_target);

    let mut content_length = 0usize;
    let mut origin = None;
    let mut host = None;
    let mut authorization = None;
    let mut cookie = None;
    for line in lines {
        if let Some((k, v)) = line.split_once(':') {
            let key = k.trim();
            if key.eq_ignore_ascii_case("content-length") {
                content_length = v.trim().parse().unwrap_or(0);
            } else if key.eq_ignore_ascii_case("origin") {
                origin = Some(v.trim().to_string());
            } else if key.eq_ignore_ascii_case("host") {
                host = Some(v.trim().to_string());
            } else if key.eq_ignore_ascii_case("authorization") {
                authorization = Some(v.trim().to_string());
            } else if key.eq_ignore_ascii_case("cookie") {
                cookie = Some(v.trim().to_string());
            }
        }
    }
    if content_length > MAX_BODY {
        return Err(format!("request body too large ({content_length} bytes)"));
    }
    let mut body = buf[header_end + 4..].to_vec();
    while body.len() < content_length {
        let n = stream.read(&mut tmp).map_err(|e| e.to_string())?;
        if n == 0 {
            break;
        }
        body.extend_from_slice(&tmp[..n]);
    }
    body.truncate(content_length);
    Ok(Request { method, path, query, origin, host, authorization, cookie, body })
}

/// Host part of an Origin/Host header value (drop scheme, port, path, ipv6 []).
fn header_host(s: &str) -> &str {
    let s = s.trim();
    let s = s
        .strip_prefix("http://")
        .or_else(|| s.strip_prefix("https://"))
        .unwrap_or(s);
    let s = s.split('/').next().unwrap_or(s);
    if let Some(rest) = s.strip_prefix('[') {
        return rest.split(']').next().unwrap_or(rest);
    }
    s.rsplit_once(':').map(|(h, _)| h).unwrap_or(s)
}

fn is_loopback_host(h: &str) -> bool {
    matches!(h, "127.0.0.1" | "localhost" | "::1")
}

/// Whether a state-changing POST is allowed. Closes the no-auth CSRF /
/// DNS-rebinding gap that the web server otherwise has: a cross-origin Origin
/// (a random website's JS hitting localhost) is rejected, and when bound to
/// loopback the Host must be loopback too, so a DNS name rebound to 127.0.0.1
/// cannot drive the local server. A loopback bind (the default) is fully
/// guarded; a 0.0.0.0 / explicit-IP bind is an opted-in remote exposure (the
/// startup banner already warns "no authentication"), so only the cross-origin
/// check applies there.
fn guard_local(req: &Request, bind_host: &str) -> bool {
    let bound_loopback = is_loopback_host(bind_host);
    if bound_loopback {
        if let Some(h) = req.host.as_deref() {
            if !is_loopback_host(header_host(h)) {
                return false;
            }
        }
    }
    if let Some(o) = req.origin.as_deref() {
        let oh = header_host(o);
        let same_as_host = req.host.as_deref().map(header_host) == Some(oh);
        if !(is_loopback_host(oh) || oh == bind_host || same_as_host) {
            return false;
        }
    }
    true
}

fn respond_403(stream: &mut TcpStream, msg: &str) -> Result<(), String> {
    let body = msg.as_bytes();
    let head = format!(
        "HTTP/1.1 403 Forbidden\r\nContent-Type: text/plain\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    stream.write_all(head.as_bytes()).map_err(|e| e.to_string())?;
    stream.write_all(body).map_err(|e| e.to_string())?;
    Ok(())
}

fn find_subslice(hay: &[u8], needle: &[u8]) -> Option<usize> {
    hay.windows(needle.len()).position(|w| w == needle)
}

fn split_query(target: &str) -> (String, HashMap<String, String>) {
    let mut q = HashMap::new();
    let (path, qs) = match target.split_once('?') {
        Some((p, s)) => (p.to_string(), s),
        None => (target.to_string(), ""),
    };
    for pair in qs.split('&').filter(|s| !s.is_empty()) {
        let (k, v) = pair.split_once('=').unwrap_or((pair, ""));
        q.insert(url_decode(k), url_decode(v));
    }
    (path, q)
}

fn url_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'%' if i + 2 < bytes.len() => {
                let h = (hex_val(bytes[i + 1]), hex_val(bytes[i + 2]));
                if let (Some(a), Some(b)) = h {
                    out.push(a * 16 + b);
                    i += 3;
                    continue;
                }
                out.push(b'%');
                i += 1;
            }
            b'+' => {
                out.push(b' ');
                i += 1;
            }
            c => {
                out.push(c);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).to_string()
}

fn hex_val(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

fn respond(stream: &mut TcpStream, status: &str, content_type: &str, body: &[u8]) -> Result<(), String> {
    let header = format!(
        "HTTP/1.1 {}\r\nContent-Type: {}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        status,
        content_type,
        body.len()
    );
    stream.write_all(header.as_bytes()).map_err(|e| e.to_string())?;
    stream.write_all(body).map_err(|e| e.to_string())?;
    stream.flush().map_err(|e| e.to_string())
}

fn respond_json(stream: &mut TcpStream, value: &Value) -> Result<(), String> {
    respond(stream, "200 OK", "application/json", value.to_string().as_bytes())
}

fn respond_err(stream: &mut TcpStream, status: &str, msg: &str) -> Result<(), String> {
    respond(stream, status, "application/json", json!({ "error": msg }).to_string().as_bytes())
}

fn handle(mut stream: TcpStream, state: &State) -> Result<(), String> {
    let req = read_request(&mut stream)?;
    let route = (req.method.as_str(), req.path.as_str());

    // A loopback console with no token treats every caller as a local admin,
    // on the reasoning that reaching the socket means already being on the
    // machine. A browser breaks that reasoning: any page the operator visits
    // can POST to 127.0.0.1 from their machine. The editor has blocked
    // cross-origin state changes since it shipped and the console did not, so
    // `fetch('http://127.0.0.1:8080/api/run', ...)` from a random site ran a
    // workspace pipeline. Same guard, same place in the request.
    if req.method != "GET" && req.path.starts_with("/api/") && !guard_local(&req, &state.host) {
        return respond_403(&mut stream, "blocked: cross-origin or non-local request");
    }

    // Signing in is the one thing an unauthenticated caller may do.
    if route == ("POST", "/api/session") {
        return sign_in(&mut stream, state, &req);
    }

    // Everything else is identified and authorised before it is dispatched, so
    // a route cannot be reached by forgetting to check it at the call site.
    let (needed, action) = audit::requirement(&req.method, &req.path);
    let target = audit_target(&req);
    let who = state.console.identify(req.authorization.as_deref(), req.cookie.as_deref());
    let Some(who) = who else {
        audit::record(&state.workspace, None, action, &target, audit::Outcome::Unauthenticated);
        // A browser asking for the page gets the sign-in form; an API client
        // gets a 401 it can act on.
        if req.method == "GET" && (req.path == "/" || req.path == "/index.html") {
            return respond(&mut stream, "401 Unauthorized", "text/html; charset=utf-8", SIGNIN_HTML.as_bytes());
        }
        return respond_err(&mut stream, "401 Unauthorized", "sign in to use the console");
    };
    if !who.role.allows(needed) {
        audit::record(&state.workspace, Some(&who), action, &target, audit::Outcome::Denied);
        return respond_err(
            &mut stream,
            "403 Forbidden",
            &format!("this needs the {} role; you have {}", needed.as_str(), who.role.as_str()),
        );
    }
    // Reads are not recorded: they would bury the events worth seeing under a
    // dashboard that polls every few seconds. Anything that changes something,
    // and every refusal above, is.
    if req.method != "GET" {
        audit::record(&state.workspace, Some(&who), action, &target, audit::Outcome::Allowed);
    }

    if route == ("DELETE", "/api/session") {
        state.console.sign_out(req.cookie.as_deref());
        return respond_json(&mut stream, &json!({ "ok": true }));
    }
    if route == ("GET", "/api/whoami") {
        return respond_json(
            &mut stream,
            &json!({ "label": who.label, "role": who.role.as_str(), "open": state.console.is_open() }),
        );
    }

    match route {
        ("GET", "/") | ("GET", "/index.html") => {
            respond(&mut stream, "200 OK", "text/html; charset=utf-8", PANEL_HTML.as_bytes())
        }
        ("GET", "/api/summary") => respond_json(&mut stream, &api_summary(state)),
        ("GET", "/api/pipelines") => respond_json(&mut stream, &api_pipelines(state)),
        ("GET", "/api/pipeline") => match req.query.get("file") {
            Some(f) => match read_pipeline_file(state, f) {
                Ok(v) => respond_json(&mut stream, &v),
                Err(e) => respond_err(&mut stream, "404 Not Found", &e),
            },
            None => respond_err(&mut stream, "400 Bad Request", "missing file"),
        },
        ("GET", "/api/runs") => respond_json(&mut stream, &api_runs(state, req.query.get("id").map(|s| s.as_str()))),
        ("GET", "/api/log") => respond_json(&mut stream, &api_log(state, &req.query)),
        ("GET", "/api/catalog") => respond_json(&mut stream, &api_catalog(state)),
        ("GET", "/api/audit") => {
            let filter = audit::Filter {
                actor: req.query.get("actor").cloned(),
                outcome: req.query.get("outcome").cloned(),
                action: req.query.get("action").cloned(),
                // A page, not the file. The console polls, and an unbounded
                // read would grow with the log until the poll was the most
                // expensive thing the server did.
                limit: req
                    .query
                    .get("limit")
                    .and_then(|v| v.parse::<usize>().ok())
                    .unwrap_or(200)
                    .clamp(1, 1000),
            };
            match audit::read(&state.workspace, &filter) {
                Ok(page) => respond_json(&mut stream, &json!(page)),
                Err(e) => respond_err(&mut stream, "500 Internal Server Error", &e),
            }
        }
        ("POST", "/api/catalog") => {
            match duckle_duckdb_engine::catalog::build_and_save(&state.workspace) {
                Ok(_) => respond_json(&mut stream, &api_catalog(state)),
                Err(e) => respond_err(&mut stream, "500 Internal Server Error", &e),
            }
        }
        ("GET", "/api/schedules") => match load_schedules(state) {
            Ok(v) => respond_json(&mut stream, &v),
            Err(e) => respond_err(&mut stream, "500 Internal Server Error", &e),
        },
        ("POST", "/api/schedules") => {
            let body: Value = serde_json::from_slice(&req.body).unwrap_or(json!({}));
            match save_schedule(state, &body) {
                Ok(v) => respond_json(&mut stream, &v),
                Err(e) => respond_err(&mut stream, "400 Bad Request", &e),
            }
        }
        ("GET", "/api/params") => match req.query.get("file") {
            Some(f) => match discover_pipeline_params(state, f) {
                Ok(names) => respond_json(&mut stream, &json!({ "params": names })),
                Err(e) => respond_err(&mut stream, "404 Not Found", &e),
            },
            None => respond_err(&mut stream, "400 Bad Request", "missing file"),
        },
        ("POST", "/api/run") => {
            let body: Value = serde_json::from_slice(&req.body).unwrap_or(json!({}));
            let file = match body.get("file").and_then(|v| v.as_str()) {
                Some(f) => f.to_string(),
                None => return respond_err(&mut stream, "400 Bad Request", "missing file"),
            };
            let params = parse_run_params(body.get("params"));
            match execute_one(state, &file, "manual", &params) {
                Ok(v) => respond_json(&mut stream, &v),
                Err(e) => respond_err(&mut stream, "400 Bad Request", &e),
            }
        }
        _ => respond_err(&mut stream, "404 Not Found", "not found"),
    }
}

/// What the request was aimed at, for the audit log. Never the body, which can
/// hold run parameters, and never the query string wholesale.
fn audit_target(req: &Request) -> String {
    if let Some(f) = req.query.get("file").or_else(|| req.query.get("id")) {
        return f.clone();
    }
    if req.method != "GET" {
        if let Ok(body) = serde_json::from_slice::<Value>(&req.body) {
            if let Some(t) = body.get("file").or_else(|| body.get("id")).and_then(|v| v.as_str()) {
                return t.to_string();
            }
        }
    }
    req.path.clone()
}

/// Exchange a token for a session cookie.
///
/// The token arrives in the body, never in the URL: a query string reaches the
/// server log, the browser history and any proxy in between.
fn sign_in(stream: &mut TcpStream, state: &State, req: &Request) -> Result<(), String> {
    let body: Value = serde_json::from_slice(&req.body).unwrap_or(json!({}));
    let token = body.get("token").and_then(|v| v.as_str()).unwrap_or("");
    match state.console.sign_in(token) {
        Some((sid, who)) => {
            audit::record(&state.workspace, Some(&who), "session.sign_in", "-", audit::Outcome::Allowed);
            // HttpOnly so page scripts cannot read it, SameSite=Strict so
            // another site cannot ride it. Not Secure: the console is served
            // over plain HTTP behind a proxy or on localhost, and marking it
            // Secure would stop the cookie being set at all.
            let cookie = format!(
                "{}={}; HttpOnly; SameSite=Strict; Path=/",
                console_auth::SESSION_COOKIE,
                sid
            );
            let payload = json!({ "label": who.label, "role": who.role.as_str() }).to_string();
            let head = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\
                 Set-Cookie: {}\r\nConnection: close\r\n\r\n",
                payload.len(),
                cookie
            );
            stream.write_all(head.as_bytes()).map_err(|e| e.to_string())?;
            stream.write_all(payload.as_bytes()).map_err(|e| e.to_string())
        }
        None => {
            audit::record(
                &state.workspace,
                None,
                "session.sign_in",
                "-",
                audit::Outcome::Unauthenticated,
            );
            respond_err(stream, "401 Unauthorized", "that token was not accepted")
        }
    }
}

// ── Pipeline discovery ──

/// Scan the workspace for pipeline files (a `.json` with a top-level `nodes`
/// array), skipping bookkeeping folders. Returns (absolute path, id, value).
fn discover_pipelines(workspace: &Path) -> Vec<(PathBuf, String, Value)> {
    let mut out = Vec::new();
    // One walk, shared with the catalog. Each keeping its own copy of the
    // folders to skip is how the two came to disagree: the console could open
    // a pipeline in a subfolder that the workspace graph could not see, so the
    // blast radius quietly omitted it.
    for path in duckle_duckdb_engine::catalog::discover_pipeline_files(workspace) {
        let text = match std::fs::read_to_string(&path) {
            Ok(t) => t,
            Err(_) => continue,
        };
        let v: Value = match serde_json::from_str(&text) {
            Ok(v) => v,
            Err(_) => continue,
        };
        if v.get("nodes").and_then(|n| n.as_array()).is_some() {
            let id = path.file_stem().map(|s| s.to_string_lossy().into_owned()).unwrap_or_default();
            out.push((path, id, v));
        }
    }
    out.sort_by(|a, b| a.1.to_lowercase().cmp(&b.1.to_lowercase()));
    out
}

/// Map of repo item id -> human name from <workspace>/repository.json. Workspace
/// pipeline files are saved as pipelines/<id>.json with no `name` field, so the
/// dashboard must resolve the friendly name here instead of showing the internal
/// id (#108). Best-effort: a missing / unreadable repository.json yields an empty
/// map and callers fall back to the id.
fn repo_names(workspace: &Path) -> std::collections::HashMap<String, String> {
    let mut map = std::collections::HashMap::new();
    let text = match std::fs::read_to_string(workspace.join("repository.json")) {
        Ok(t) => t,
        Err(_) => return map,
    };
    let items: Vec<Value> = serde_json::from_str(&text).unwrap_or_default();
    for it in items {
        if let (Some(id), Some(name)) = (
            it.get("id").and_then(|x| x.as_str()),
            it.get("name").and_then(|x| x.as_str()),
        ) {
            if !name.trim().is_empty() {
                map.insert(id.to_string(), name.to_string());
            }
        }
    }
    map
}

/// #102: apply the workspace's saved memory cap (.duckle/settings.json
/// memory_limit_mb, set from the desktop Settings UI) as DUCKLE_MEMORY_LIMIT so
/// web-editor runs honor the same per-workspace limit. An explicit
/// DUCKLE_MEMORY_LIMIT already in the launch environment wins.
fn apply_workspace_memory_limit(workspace: &Path) {
    if std::env::var("DUCKLE_MEMORY_LIMIT").map(|v| !v.is_empty()).unwrap_or(false) {
        return;
    }
    let text = match std::fs::read_to_string(workspace.join(".duckle").join("settings.json")) {
        Ok(t) => t,
        Err(_) => return,
    };
    let v: Value = match serde_json::from_str(&text) {
        Ok(v) => v,
        Err(_) => return,
    };
    if let Some(mb) = v.get("memory_limit_mb").and_then(|x| x.as_u64()).filter(|m| *m > 0) {
        std::env::set_var("DUCKLE_MEMORY_LIMIT", format!("{}MB", mb));
    }
}

fn rel(workspace: &Path, path: &Path) -> String {
    path.strip_prefix(workspace)
        .unwrap_or(path)
        .to_string_lossy()
        .replace('\\', "/")
}

fn last_run(workspace: &Path, id: &str) -> Option<RunRecord> {
    // History is appended in order; the most recent record is last.
    load_run_history(workspace, id).into_iter().last()
}

fn api_pipelines(state: &State) -> Value {
    // A broken store must not take the pipeline list down with it; the
    // Schedules view reports the reason on its own.
    let scheds = load_schedules(state).unwrap_or_else(|_| json!({}));
    let names = repo_names(&state.workspace);
    let items: Vec<Value> = discover_pipelines(&state.workspace)
        .into_iter()
        .map(|(path, id, v)| {
            let last = last_run(&state.workspace, &id);
            let sched = scheds
                .get(&id)
                .cloned()
                .unwrap_or(json!({ "enabled": false, "intervalSeconds": 0, "intervalMinutes": 0 }));
            let running = state.running.lock().map(|s| s.contains(&id)).unwrap_or(false);
            let next_at = next_run_at(&sched, last.as_ref().map(|r| r.at.as_str()));
            let name = names
                .get(&id)
                .cloned()
                .or_else(|| {
                    v.get("name").and_then(|x| x.as_str()).map(str::trim).filter(|s| !s.is_empty()).map(str::to_string)
                })
                .unwrap_or_else(|| id.clone());
            json!({
                "file": rel(&state.workspace, &path),
                "id": id,
                "name": name,
                "nodeCount": v.get("nodes").and_then(|n| n.as_array()).map(|a| a.len()).unwrap_or(0),
                "edgeCount": v.get("edges").and_then(|e| e.as_array()).map(|a| a.len()).unwrap_or(0),
                "lastStatus": last.as_ref().map(|r| r.status.clone()),
                "lastAt": last.as_ref().map(|r| r.at.clone()),
                "lastDurationMs": last.as_ref().map(|r| r.duration_ms),
                "lastRows": last.as_ref().map(|r| r.rows),
                "schedule": sched,
                "running": running,
                "nextRunAt": next_at,
            })
        })
        .collect();
    json!({ "pipelines": items })
}

fn api_summary(state: &State) -> Value {
    let pipes = discover_pipelines(&state.workspace);
    let mut total_runs = 0u64;
    let mut ok = 0u64;
    let mut failed = 0u64;
    for (_, id, _) in &pipes {
        for r in load_run_history(&state.workspace, id) {
            total_runs += 1;
            if r.status == "ok" {
                ok += 1;
            } else {
                failed += 1;
            }
        }
    }
    json!({
        "pipelineCount": pipes.len(),
        "totalRuns": total_runs,
        "ok": ok,
        "failed": failed,
        "workspace": state.workspace.to_string_lossy(),
    })
}

/// Run history across all pipelines (or one, when `id` is given), newest first,
/// each record tagged with its pipeline id/name.
fn api_runs(state: &State, only: Option<&str>) -> Value {
    let mut rows: Vec<Value> = Vec::new();
    let names = repo_names(&state.workspace);
    for (path, id, v) in discover_pipelines(&state.workspace) {
        if let Some(want) = only {
            if want != id {
                continue;
            }
        }
        let name = names
            .get(&id)
            .cloned()
            .or_else(|| {
                v.get("name").and_then(|x| x.as_str()).map(str::trim).filter(|s| !s.is_empty()).map(str::to_string)
            })
            .unwrap_or_else(|| id.clone());
        for r in load_run_history(&state.workspace, &id) {
            rows.push(json!({
                "id": id,
                "name": name,
                "file": rel(&state.workspace, &path),
                "at": r.at,
                "status": r.status,
                "durationMs": r.duration_ms,
                "rows": r.rows,
                "nodeCount": r.node_count,
                "trigger": r.trigger,
                "error": r.error,
                "category": r.category,
            }));
        }
    }
    // RunRecord.at is RFC3339 UTC, so a string sort orders by time; newest first.
    rows.sort_by(|a, b| {
        b.get("at").and_then(|v| v.as_str()).unwrap_or("")
            .cmp(a.get("at").and_then(|v| v.as_str()).unwrap_or(""))
    });
    json!({ "runs": rows })
}

fn read_pipeline_file(state: &State, file: &str) -> Result<Value, String> {
    let path = resolve_in_workspace(&state.workspace, file)?;
    let text = std::fs::read_to_string(&path).map_err(|e| format!("read {}: {}", path.display(), e))?;
    let doc: Value = serde_json::from_str(&text).map_err(|e| format!("parse {}: {}", path.display(), e))?;
    // Confining to the workspace is not enough on its own. This route is rated
    // for viewers, and the workspace also holds `.duckle/console-users.json`
    // and `connections/*.json`, so "any JSON inside the workspace" handed the
    // lowest role the account hashes and the stored connection payloads. A
    // pipeline is the thing with a `nodes` array; anything else is refused
    // whatever its path.
    if doc.get("nodes").and_then(Value::as_array).is_none() {
        return Err(format!("{file} is not a pipeline"));
    }
    Ok(doc)
}

/// Resolve a workspace-relative path and refuse anything that escapes the
/// workspace (no `..` traversal beyond the root).
fn resolve_in_workspace(workspace: &Path, file: &str) -> Result<PathBuf, String> {
    let candidate = workspace.join(file);
    let canon = candidate.canonicalize().map_err(|_| format!("not found: {}", file))?;
    if !canon.starts_with(workspace) {
        return Err("path escapes workspace".into());
    }
    Ok(canon)
}

fn api_log(state: &State, query: &HashMap<String, String>) -> Value {
    let id = match query.get("id") {
        Some(i) => i,
        None => return json!({ "entries": [] }),
    };
    let tail: usize = query.get("tail").and_then(|t| t.parse().ok()).unwrap_or(200);
    let file = state.workspace.join("logs").join(sanitize_segment(id)).join("runtime.log");
    let text = match std::fs::read_to_string(&file) {
        Ok(t) => t,
        Err(_) => return json!({ "entries": [], "file": file.to_string_lossy() }),
    };
    let lines: Vec<&str> = text.lines().filter(|l| !l.trim().is_empty()).collect();
    let start = lines.len().saturating_sub(tail);
    let entries: Vec<Value> = lines[start..]
        .iter()
        .map(|l| serde_json::from_str::<Value>(l).unwrap_or_else(|_| json!({ "raw": l })))
        .collect();
    json!({ "entries": entries, "file": file.to_string_lossy() })
}

/// Match the engine's per-pipeline log-folder sanitization (run_log.rs).
fn sanitize_segment(name: &str) -> String {
    let s: String = name
        .chars()
        .map(|c| if c.is_alphanumeric() || c == '-' || c == '_' || c == '.' { c } else { '_' })
        .collect();
    if s.is_empty() { "pipeline".into() } else { s }
}

// ── Schedules ──

/// The workspace graph, for the console's Catalog tab.
///
/// Reads the saved graph rather than rescanning every pipeline, because the
/// dashboard polls every few seconds and a rebuild reads every pipeline file.
/// A POST to the same path rebuilds it deliberately.
fn api_catalog(state: &State) -> Value {
    use duckle_duckdb_engine::catalog;
    let cat = match catalog::load(&state.workspace) {
        Ok(Some(c)) => c,
        // Never built: do it once so the first visit shows something.
        Ok(None) => match catalog::build_and_save(&state.workspace) {
            Ok(c) => c,
            Err(e) => return json!({ "error": e }),
        },
        Err(e) => return json!({ "error": e }),
    };
    let owners = catalog::load_owners(&state.workspace).unwrap_or_default();
    let assets: Vec<Value> = cat
        .assets
        .iter()
        .map(|a| {
            json!({
                "id": a.id,
                "kind": a.kind,
                "writtenBy": cat.producers(&a.id).iter().map(|t| &t.pipeline_id).collect::<Vec<_>>(),
                "readBy": cat.consumers(&a.id).iter().map(|t| &t.pipeline_id).collect::<Vec<_>>(),
                "owner": owners.for_asset(&a.id).map(|r| r.owner.clone()),
            })
        })
        .collect();
    json!({
        "assets": assets,
        "pipelines": cat.pipelines.len(),
        "orphans": cat.orphans().iter().map(|a| &a.id).collect::<Vec<_>>(),
        "externals": cat.externals().iter().map(|a| &a.id).collect::<Vec<_>>(),
        // Carried so the tab can say the view may be incomplete rather than
        // presenting a partial graph as the whole picture.
        "unresolved": cat.unresolved.len(),
        "hasOwners": !owners.is_empty(),
    })
}

/// Where the console's own store used to live, before both products moved to
/// the workspace `schedules.json` the desktop app already used. Only read now,
/// and only to carry an existing install's schedules across once.
fn legacy_schedules_path(workspace: &Path) -> PathBuf {
    workspace.join("panel-schedules.json")
}

/// The console's view of the shared store, one entry per pipeline id:
/// `{ "enabled": bool, "intervalSeconds": n, "intervalMinutes": n, "cron": "<expr>" }`.
/// A non-empty `cron` takes precedence over the interval (#132).
///
/// `intervalSeconds` is the real stored value. `intervalMinutes` is derived and
/// kept only so an older console page still renders something sensible; it is
/// rounded and must not be written back as if it were exact, because the
/// desktop editor offers seconds as a unit and a 30-second schedule saved from
/// a minutes-only view comes back as a minute.
///
/// The store can hold several schedules for one pipeline, and file-watch
/// schedules the console cannot express at all. Those are left strictly alone:
/// this view shows the first schedule the console can represent, and a save
/// edits that same record by id rather than replacing the pipeline's entry.
/// The schedule store, or why it could not be read.
///
/// The failure is returned rather than flattened to an empty map. An empty map
/// renders as "nothing is scheduled", which is the same sentence a healthy
/// workspace with no schedules produces - so a file that would not parse was
/// indistinguishable from one that said there was nothing to do.
fn load_schedules(state: &State) -> Result<Value, String> {
    let list = duckle_duckdb_engine::schedules::load(&state.workspace).inspect_err(|e| {
        eprintln!("duckle-runner: {e}");
    })?;
    let mut out = serde_json::Map::new();
    for s in &list {
        if out.contains_key(&s.pipeline_id) {
            continue;
        }
        let (seconds, cron) = match &s.kind {
            duckle_duckdb_engine::schedules::ScheduleKind::Cron { expr } => (0, expr.clone()),
            duckle_duckdb_engine::schedules::ScheduleKind::Interval { seconds } => {
                (*seconds, String::new())
            }
            // Not expressible here; leave the pipeline looking unscheduled to
            // the console rather than misrepresenting a watch as an interval.
            duckle_duckdb_engine::schedules::ScheduleKind::FileWatch { .. } => continue,
        };
        out.insert(
            s.pipeline_id.clone(),
            json!({
                "id": s.id,
                "enabled": s.enabled,
                "intervalSeconds": seconds,
                "intervalMinutes": seconds / 60,
                "cron": cron,
            }),
        );
    }
    Ok(Value::Object(out))
}

/// Carry a pre-unification `panel-schedules.json` into the shared store.
///
/// Runs once at startup. Only pipelines with no schedule already in the shared
/// store are imported, so a workspace where the desktop app already scheduled
/// the same pipeline keeps the desktop's record rather than gaining a second
/// one. The old file is left on disk untouched: it costs nothing, and deleting
/// a user's data to tidy up is not this function's call to make.
fn migrate_legacy_schedules(workspace: &Path) {
    let legacy = legacy_schedules_path(workspace);
    let Ok(text) = std::fs::read_to_string(&legacy) else {
        return;
    };
    let Ok(Value::Object(entries)) = serde_json::from_str::<Value>(&text) else {
        return;
    };
    if entries.is_empty() {
        return;
    }
    let outcome = duckle_duckdb_engine::schedules::update(workspace, |list| {
        for (pipeline_id, cfg) in &entries {
            if list.iter().any(|s| &s.pipeline_id == pipeline_id) {
                continue;
            }
            let cron = cfg.get("cron").and_then(Value::as_str).unwrap_or("").trim();
            let minutes = cfg.get("intervalMinutes").and_then(Value::as_u64).unwrap_or(0);
            let kind = if !cron.is_empty() {
                duckle_duckdb_engine::schedules::ScheduleKind::Cron { expr: cron.to_string() }
            } else if minutes > 0 {
                duckle_duckdb_engine::schedules::ScheduleKind::Interval { seconds: minutes * 60 }
            } else {
                // Neither a cron nor a usable interval: nothing to carry over.
                continue;
            };
            list.push(duckle_duckdb_engine::schedules::Schedule {
                id: format!("panel-{pipeline_id}"),
                pipeline_id: pipeline_id.clone(),
                name: pipeline_id.clone(),
                enabled: cfg.get("enabled").and_then(Value::as_bool).unwrap_or(false),
                kind,
                last_run_at: None,
                last_run_status: None,
                last_run_duration_ms: None,
                last_run_error: None,
                next_run_at: None,
            });
        }
    });
    match outcome {
        Ok(_) => eprintln!(
            "duckle-runner: imported schedules from {} into schedules.json",
            legacy.display()
        ),
        Err(e) => eprintln!("duckle-runner: could not import {}: {e}", legacy.display()),
    }
}

/// The `cron` crate expects a 6- or 7-field expression (seconds first). Accept a
/// standard 5-field cron ("min hour dom mon dow") by prepending a "0 " seconds
/// field; pass a 6/7-field expression through. Returns None for any other field
/// count so a malformed expression is rejected rather than silently ignored.
fn normalize_cron(expr: &str) -> Option<String> {
    match expr.split_whitespace().count() {
        5 => Some(format!("0 {}", expr)),
        6 | 7 => Some(expr.to_string()),
        _ => None,
    }
}

/// The next time an enabled schedule is expected to fire, as an RFC3339 string
/// for the console to display beside "last run" (discussion #155). Cron uses the
/// exact next occurrence in local time; interval mode estimates from the last
/// run (or now) rolled forward by whole intervals. Returns None when the
/// schedule is disabled or has neither a cron nor a positive interval.
fn next_run_at(sched: &Value, last_at: Option<&str>) -> Option<String> {
    if !sched.get("enabled").and_then(Value::as_bool).unwrap_or(false) {
        return None;
    }
    let cron = sched.get("cron").and_then(Value::as_str).unwrap_or("").trim();
    if !cron.is_empty() {
        let schedule = normalize_cron(cron).and_then(|e| e.parse::<cron::Schedule>().ok())?;
        return schedule.after(&chrono::Local::now()).next().map(|dt| dt.to_rfc3339());
    }
    let interval = sched.get("intervalSeconds").and_then(Value::as_u64).unwrap_or(0);
    if interval == 0 {
        return None;
    }
    let step = chrono::Duration::seconds(interval as i64);
    let now = chrono::Utc::now();
    let mut next = last_at
        .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
        .map(|dt| dt.with_timezone(&chrono::Utc) + step)
        .unwrap_or(now + step);
    // A steady interval schedule fires every `interval`; roll past any missed
    // slots so the shown time is the next one still in the future.
    while next <= now {
        next += step;
    }
    Some(next.to_rfc3339())
}

fn save_schedule(state: &State, body: &Value) -> Result<Value, String> {
    save_schedule_at(&state.workspace, body)
}

/// The store half of saving a schedule, split out so a test can drive the same
/// code the handler runs rather than a copy of its logic.
fn save_schedule_at(workspace: &Path, body: &Value) -> Result<Value, String> {
    let id = body.get("id").and_then(|v| v.as_str()).ok_or("missing id")?;
    let enabled = body.get("enabled").and_then(|v| v.as_bool()).unwrap_or(false);
    let interval = body.get("intervalMinutes").and_then(|v| v.as_u64()).unwrap_or(0);
    let cron = body.get("cron").and_then(|v| v.as_str()).unwrap_or("").trim().to_string();
    // Validate a supplied cron expression up front so a bad one is rejected with
    // a clear message instead of silently never firing (#132).
    if !cron.is_empty()
        && normalize_cron(&cron).and_then(|e| e.parse::<cron::Schedule>().ok()).is_none()
    {
        return Err("Invalid cron expression (use 5 fields, e.g. `0 9 * * 1`)".to_string());
    }
    // Seconds are what the store holds. A console that sends only minutes is
    // still honoured, but one that echoes back the intervalSeconds it was given
    // keeps a sub-minute schedule exactly as the desktop editor set it.
    let seconds = match body.get("intervalSeconds").and_then(|v| v.as_u64()) {
        Some(s) => s,
        None => interval.saturating_mul(60),
    };
    // An enabled schedule with neither a cron nor a positive interval is not a
    // schedule. The runner skips it, but the desktop scheduler computes
    // `now + 0s` as its next run and fires it on every tick, forever. Refusing
    // it here is better than either behaviour, and better than the console's
    // empty interval box quietly becoming "run continuously".
    if enabled && cron.is_empty() && seconds == 0 {
        return Err(
            "An enabled schedule needs a cron expression or an interval greater than zero".into(),
        );
    }
    let kind = if !cron.is_empty() {
        duckle_duckdb_engine::schedules::ScheduleKind::Cron { expr: cron }
    } else {
        duckle_duckdb_engine::schedules::ScheduleKind::Interval { seconds }
    };
    let pipeline_id = id.to_string();
    duckle_duckdb_engine::schedules::update(workspace, move |list| {
        // Edit the record this pipeline already has rather than adding another,
        // so saving from the console does not quietly double a schedule the
        // desktop app created. A file-watch record is not one the console can
        // edit, so it is skipped and a new record is added alongside it.
        let existing = list.iter_mut().find(|s| {
            s.pipeline_id == pipeline_id
                && !matches!(s.kind, duckle_duckdb_engine::schedules::ScheduleKind::FileWatch { .. })
        });
        match existing {
            Some(s) => {
                s.enabled = enabled;
                s.kind = kind;
                // A changed trigger invalidates the time this process armed.
                s.next_run_at = None;
            }
            None => list.push(duckle_duckdb_engine::schedules::Schedule {
                id: format!("panel-{pipeline_id}"),
                pipeline_id: pipeline_id.clone(),
                name: pipeline_id.clone(),
                enabled,
                kind,
                last_run_at: None,
                last_run_status: None,
                last_run_duration_ms: None,
                last_run_error: None,
                next_run_at: None,
            }),
        }
    })
    .map_err(|e| format!("write schedules: {}", e))?;
    Ok(json!({ "ok": true }))
}

// ── Execution ──

/// Parse the optional `params` object from a run request into a {name: value}
/// map, keeping only non-empty string-ish values (a blank field means "use the
/// context default", so it is dropped rather than overriding with an empty value).
fn parse_run_params(v: Option<&Value>) -> HashMap<String, String> {
    let mut out = HashMap::new();
    if let Some(Value::Object(m)) = v {
        for (k, val) in m {
            let s = match val {
                Value::String(s) => s.clone(),
                Value::Null => continue,
                other => other.to_string(),
            };
            if !s.is_empty() {
                out.insert(k.clone(), s);
            }
        }
    }
    out
}

/// List the `${...}` parameters a pipeline file exposes, for the dashboard's
/// run-parameters form. Reads the file and delegates to the engine's discovery.
fn discover_pipeline_params(state: &State, file: &str) -> Result<Vec<String>, String> {
    let path = resolve_in_workspace(&state.workspace, file)?;
    let text = std::fs::read_to_string(&path).map_err(|e| format!("read {}: {}", path.display(), e))?;
    let doc: PipelineDoc =
        serde_json::from_str(&text).map_err(|e| format!("parse {}: {}", path.display(), e))?;
    Ok(duckle_duckdb_engine::context::discover_parameters(&doc))
}

/// Run one pipeline by its workspace-relative file path, end to end: resolve
/// env/time placeholders (as the runner does), execute through the engine,
/// append a run-history record, and return a result summary. Serialized by the
/// run lock so a scheduled run never overlaps a manual one.
/// Removes a pipeline id from the running set when the run ends, no matter how
/// (normal return, `?` error, or panic). Paired with the insert in execute_one.
struct RunningGuard<'a> {
    set: &'a Mutex<std::collections::HashSet<String>>,
    id: String,
}
impl Drop for RunningGuard<'_> {
    fn drop(&mut self) {
        if let Ok(mut set) = self.set.lock() {
            set.remove(&self.id);
        }
    }
}

/// Fire a scheduled pipeline, unless another Duckle process already is.
///
/// The in-memory `last_fired` / `cron_next` maps below only stop THIS process
/// double-firing. A desktop app open on the same workspace runs its own
/// scheduler and knows nothing about this one, so the guard has to live on
/// disk. Skipping is the right response to a clash: the next tick comes round
/// anyway, and two runs of one pipeline race on the sink and on the
/// `xf.incremental` watermark, which is how a load quietly skips rows.
fn run_scheduled(state: &State, id: &str, file: &str) {
    let _lock = match duckle_duckdb_engine::runlock::try_acquire(&state.workspace, id) {
        Some(l) => l,
        None => {
            eprintln!("duckle-runner: scheduled {id} already running elsewhere, skipped");
            return;
        }
    };
    match execute_one(state, file, "scheduled", &HashMap::new()) {
        Ok(v) => {
            let status = v.get("status").and_then(|s| s.as_str()).unwrap_or("?");
            eprintln!("duckle-runner: scheduled {} -> {}", id, status);
            record_schedule_outcome(
                state,
                id,
                status,
                v.get("durationMs").and_then(|d| d.as_u64()).unwrap_or(0),
                v.get("error").and_then(|e| e.as_str()).map(str::to_string),
            );
        }
        Err(e) => {
            eprintln!("duckle-runner: scheduled {} failed: {}", id, e);
            // A run that could not even start is still an outcome, and leaving
            // it out is what makes a schedule look like it never fired.
            record_schedule_outcome(state, id, "error", 0, Some(e.clone()));
            alerts_notify(state, id, "error", 0, Some(e));
        }
    }
}

/// Write a scheduled run's outcome back to the shared schedule store.
///
/// Only the desktop app used to do this, and once both products moved to one
/// `schedules.json` that left a runner-only deployment showing "never run"
/// forever while it was in fact running fine every hour - and made any
/// staleness check built on `lastRunAt` useless.
fn record_schedule_outcome(
    state: &State,
    pipeline_id: &str,
    status: &str,
    duration_ms: u64,
    error: Option<String>,
) {
    let (pipeline_id, status) = (pipeline_id.to_string(), status.to_string());
    let outcome = duckle_duckdb_engine::schedules::update(&state.workspace, move |list| {
        for s in list.iter_mut().filter(|s| s.pipeline_id == pipeline_id) {
            s.last_run_at = Some(chrono::Utc::now());
            s.last_run_status = Some(status.clone());
            s.last_run_duration_ms = Some(duration_ms);
            s.last_run_error = error.clone();
        }
    });
    if let Err(e) = outcome {
        eprintln!("duckle-runner: could not record the run against its schedule: {e}");
    }
}

/// Raise an alert for something that happened outside `execute_one`, which is
/// where the ordinary path already reports from.
fn alerts_notify(state: &State, pipeline_id: &str, status: &str, duration_ms: u64, error: Option<String>) {
    let result = duckle_duckdb_engine::RunResult {
        status: status.to_string(),
        duration_ms,
        nodes: Default::default(),
        preview: Vec::new(),
        category: error.as_deref().map(duckle_duckdb_engine::error_category::categorize_error)
            .map(str::to_string),
        error,
    };
    duckle_duckdb_engine::alerts::notify(&state.workspace, pipeline_id, &result);
}

/// A schedule came due and its pipeline is not there.
///
/// This used to do nothing at all: the fire site was `if let Some(path) =
/// pipes.get(id)`, so renaming or deleting a pipeline turned its schedule into
/// a no-op that reported nothing, forever. That is the worst shape a scheduler
/// failure can take, because everything looks healthy while the data quietly
/// stops arriving.
fn report_missing_pipeline(state: &State, id: &str) {
    let msg = format!(
        "scheduled pipeline '{id}' has no pipeline file in the workspace; \
         it was probably renamed, moved or deleted"
    );
    eprintln!("duckle-runner: {msg}");
    record_schedule_outcome(state, id, "error", 0, Some(msg.clone()));
    alerts_notify(state, id, "error", 0, Some(msg));
}

fn execute_one(
    state: &State,
    file: &str,
    trigger: &str,
    params: &HashMap<String, String>,
) -> Result<Value, String> {
    let path = resolve_in_workspace(&state.workspace, file)?;
    let text = std::fs::read_to_string(&path).map_err(|e| format!("read {}: {}", path.display(), e))?;
    let mut doc: PipelineDoc = serde_json::from_str(&text).map_err(|e| format!("parse {}: {}", path.display(), e))?;

    let id = path.file_stem().map(|s| s.to_string_lossy().into_owned()).unwrap_or_else(|| "pipeline".into());

    let _guard = state.run_lock.acquire();

    // Mark this pipeline as running for the duration of the execution so the
    // console can show a live "Running" status (discussion #155). The guard
    // clears it on every exit path, including the `?` early returns below.
    if let Ok(mut set) = state.running.lock() {
        set.insert(id.clone());
    }
    let _running = RunningGuard { set: &state.running, id: id.clone() };

    // Same placeholder resolution as `duckle-runner run`: saved Salesforce
    // connection refs first (#166 stage 2, so a connection field stored as
    // ${ENV:...} still expands), then ${ENV:KEY} secrets, then the dynamic
    // ${date}/${datetime}/... builtins.
    duckle_secrets::resolve_connection_refs(&state.workspace, &mut doc.nodes)?;
    let env_file = state.workspace.join("secrets.env");
    crate::apply_env_pass(&mut doc, &state.workspace, &env_file)?;
    duckle_duckdb_engine::context::apply_time_builtins(&mut doc);
    // Per-run input parameters from the dashboard (issue #127) override the
    // static workspace context for this run; applied before the context pass so a
    // supplied value wins and any unset ${KEY} still resolves from the context.
    duckle_duckdb_engine::context::apply_params(&mut doc, params);
    // Match the web cmd paths and headless `duckle-runner --pipeline`: resolve
    // ${workspace}/${projectroot} and workspace-relative file paths before run,
    // so file-loaded pipelines (manual /api/run + scheduled runs) work too.
    duckle_duckdb_engine::context::apply_workspace_context(&mut doc, &state.workspace);

    let engine = DuckdbEngine::new(state.duckdb.clone());
    let result = engine.execute_pipeline_named(&doc, &id);

    let _ = append_run_record(&state.workspace, &id, RunRecord::from_result(&result, trigger));
    // After the run is recorded, so an unreachable channel can never cost a
    // run its history entry, and never changes the outcome reported below.
    duckle_duckdb_engine::alerts::notify(&state.workspace, &id, &result);

    Ok(json!({
        "id": id,
        "status": result.status,
        "durationMs": result.duration_ms,
        "error": result.error,
        "nodes": result.nodes.iter().map(|(nid, st)| json!({
            "id": nid, "status": st.status, "rows": st.rows, "durationMs": st.duration_ms, "error": st.error,
        })).collect::<Vec<_>>(),
    }))
}

// ── Scheduler ──

/// Background loop: every 30s, run any enabled pipeline whose schedule is due.
/// Interval schedules are tracked in-memory from process start (first run fires
/// one interval after boot). Cron schedules are evaluated in LOCAL time so
/// "0 9 * * *" means 9am local, matching how the dashboard displays run times
/// (#132). Both keep next-run state in-memory, so a restart re-arms from the
/// next occurrence with no surprise burst of catch-up runs.
/// What this tick should do with a cron schedule, and what to arm next.
///
/// Returned rather than performed, so the decision can be tested without a
/// thread, a workspace and a wall clock.
///
/// The armed occurrence is remembered together with the expression it came
/// from. Keyed by schedule id alone, an edited cron expression did nothing
/// until the OLD occurrence came round: a schedule moved from 03:00 to 09:00
/// skipped 09:00 entirely and then fired at 03:00 the next morning, at the one
/// time it had just been moved away from.
fn cron_decision(
    armed: Option<&(String, chrono::DateTime<chrono::Local>)>,
    expr: &str,
    sched: &cron::Schedule,
    now: chrono::DateTime<chrono::Local>,
) -> (bool, Option<(String, chrono::DateTime<chrono::Local>)>) {
    let next_after_now = || sched.after(&now).next().map(|t| (expr.to_string(), t));
    match armed {
        // Armed from this very expression, and its moment has come.
        Some((e, at)) if e == expr && now >= *at => (true, next_after_now()),
        // Armed from this expression, not due yet. Left exactly as it is:
        // re-arming here would push the occurrence away on every tick.
        Some((e, at)) if e == expr => (false, Some((e.clone(), *at))),
        // Never seen before, or the expression changed underneath us. Arm what
        // it says NOW, and do not fire: the edit is not itself an occurrence.
        _ => (false, next_after_now()),
    }
}

fn spawn_scheduler(state: Arc<State>) {
    std::thread::spawn(move || {
        let mut last_fired: HashMap<String, Instant> = HashMap::new();
        // The armed occurrence AND the expression it came from. See cron_decision.
        let mut cron_next: HashMap<String, (String, chrono::DateTime<chrono::Local>)> =
            HashMap::new();
        loop {
            std::thread::sleep(state.tick_interval);
            let scheds = match load_schedules(&state) {
                Ok(v) => v,
                // Already reported to stderr by load_schedules. Firing nothing
                // is the safe answer to a store that will not parse.
                Err(_) => continue,
            };
            let obj = match scheds.as_object() {
                Some(o) => o,
                None => continue,
            };
            // Map id -> its file path for the enabled, due ones.
            let pipes: HashMap<String, PathBuf> =
                discover_pipelines(&state.workspace).into_iter().map(|(p, id, _)| (id, p)).collect();
            for (id, cfg) in obj {
                // Cron schedule (local time) takes precedence over interval when
                // set (#132). Kept separate so the interval path below is unchanged.
                {
                    let enabled = cfg.get("enabled").and_then(|v| v.as_bool()).unwrap_or(false);
                    let cron = cfg.get("cron").and_then(|v| v.as_str()).unwrap_or("").trim();
                    if enabled && !cron.is_empty() {
                        last_fired.remove(id);
                        match normalize_cron(cron).and_then(|e| e.parse::<cron::Schedule>().ok()) {
                            None => {
                                cron_next.remove(id);
                            }
                            Some(sched) => {
                                let (fire, rearm) = cron_decision(
                                    cron_next.get(id),
                                    cron,
                                    &sched,
                                    chrono::Local::now(),
                                );
                                if fire {
                                    match pipes.get(id) {
                                        Some(path) => {
                                            let file = rel(&state.workspace, path);
                                            run_scheduled(&state, id, &file);
                                        }
                                        None => report_missing_pipeline(&state, id),
                                    }
                                }
                                match rearm {
                                    Some(next) => {
                                        cron_next.insert(id.clone(), next);
                                    }
                                    // No future occurrence at all. Forgetting
                                    // it leaves the schedule armed by nothing
                                    // rather than due every tick.
                                    None => {
                                        cron_next.remove(id);
                                    }
                                }
                            }
                        }
                        continue;
                    }
                    // Not a cron schedule: drop any stale cron state and fall
                    // through to the interval logic below.
                    cron_next.remove(id);
                }
                let enabled = cfg.get("enabled").and_then(|v| v.as_bool()).unwrap_or(false);
                // Seconds, not minutes: the shared store keeps the exact value
                // the desktop editor set, and rounding it here would turn a
                // 30-second schedule into one that never fires.
                let seconds = cfg.get("intervalSeconds").and_then(|v| v.as_u64()).unwrap_or(0);
                if !enabled || seconds == 0 {
                    last_fired.remove(id);
                    continue;
                }
                let interval = Duration::from_secs(seconds);
                let due = match last_fired.get(id) {
                    Some(t) => t.elapsed() >= interval,
                    None => false, // first sighting: start the clock, fire next interval
                };
                let now = Instant::now();
                if last_fired.get(id).is_none() {
                    last_fired.insert(id.clone(), now);
                    continue;
                }
                if due {
                    // The clock is re-armed whether or not the pipeline is
                    // there. It used to be advanced only inside the match, so a
                    // missing pipeline left this schedule permanently due and it
                    // re-evaluated on every tick, silently, for as long as the
                    // process lived.
                    last_fired.insert(id.clone(), now);
                    match pipes.get(id) {
                        Some(path) => {
                            let file = rel(&state.workspace, path);
                            run_scheduled(&state, id, &file);
                        }
                        None => report_missing_pipeline(&state, id),
                    }
                }
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::{
        connection_secret_cmd, console_auth, cron_decision, migrate_legacy_schedules,
        normalize_cron, read_pipeline_file, read_request, save_schedule_at, RunGate, State,
        MAX_BODY,
    };
    use std::sync::Mutex;
    use duckle_duckdb_engine::schedules::{self, ScheduleKind};

    /// The console and the desktop app now keep one store, so a schedule saved
    /// here has to be a record the desktop reads, in the file the desktop reads.
    /// Before this, the console wrote `panel-schedules.json` and the desktop
    /// never saw it, which is why the same pipeline could end up scheduled twice.
    #[test]
    fn a_console_save_lands_in_the_store_the_desktop_reads() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path();

        save_schedule_at(
            ws,
            &serde_json::json!({ "id": "nightly-load", "enabled": true, "intervalSeconds": 90 }),
        )
        .expect("save");

        let list = schedules::load(ws).expect("the desktop can read it");
        assert_eq!(list.len(), 1);
        assert_eq!(list[0].pipeline_id, "nightly-load");
        assert!(list[0].enabled);
        // 90 seconds, not "1 minute" or "0 minutes": the console works in the
        // same units the desktop editor offers, so an interval survives a save
        // from either side unchanged.
        assert!(
            matches!(list[0].kind, ScheduleKind::Interval { seconds: 90 }),
            "interval was not stored exactly: {:?}",
            list[0].kind
        );

        // Saving again edits that record rather than adding a second one, so a
        // pipeline cannot accumulate duplicate schedules by being saved twice.
        save_schedule_at(
            ws,
            &serde_json::json!({ "id": "nightly-load", "enabled": false, "cron": "0 9 * * 1" }),
        )
        .expect("second save");
        let list = schedules::load(ws).expect("still readable");
        assert_eq!(list.len(), 1, "a second save duplicated the schedule");
        assert!(!list[0].enabled);
        assert!(matches!(&list[0].kind, ScheduleKind::Cron { expr } if expr == "0 9 * * 1"));
    }

    #[test]
    fn an_enabled_schedule_with_no_cron_and_no_interval_is_refused() {
        // The console's interval box left empty posts intervalSeconds: 0. The
        // runner skips such a schedule, but the desktop scheduler computes
        // `now + 0s` as the next run and fires it on every tick, forever.
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path();
        let err = save_schedule_at(
            ws,
            &serde_json::json!({ "id": "nightly-load", "enabled": true, "intervalSeconds": 0 }),
        )
        .expect_err("an enabled schedule with no trigger must be refused");
        assert!(err.contains("greater than zero"), "unhelpful message: {err}");

        // Disabling it is fine - that is how the console turns a schedule off.
        save_schedule_at(
            ws,
            &serde_json::json!({ "id": "nightly-load", "enabled": false, "intervalSeconds": 0 }),
        )
        .expect("a disabled schedule needs no trigger");
    }

    #[test]
    fn the_pipeline_reader_refuses_anything_that_is_not_a_pipeline() {
        // This route is rated for viewers, and the workspace also holds the
        // console account hashes and the connection files. Confining to the
        // workspace was the only check, so "any JSON under the workspace" was
        // readable by the lowest role there is.
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path();
        // A saved connection: same workspace, same "it is JSON" shape, and it
        // holds an encrypted credential payload. (The account file makes the
        // same point but Console::configure rightly refuses to start against a
        // fabricated Argon2 hash, so this is the cleaner fixture.)
        std::fs::create_dir_all(ws.join("connections")).unwrap();
        std::fs::write(
            ws.join("connections").join("prod-db.json"),
            serde_json::json!({ "name": "prod", "payload": "<ciphertext>" }).to_string(),
        )
        .unwrap();
        std::fs::create_dir_all(ws.join("pipelines")).unwrap();
        std::fs::write(
            ws.join("pipelines").join("real.json"),
            serde_json::json!({ "name": "real", "nodes": [], "edges": [] }).to_string(),
        )
        .unwrap();

        // The server canonicalises the workspace at startup, and
        // resolve_in_workspace compares canonical paths - on Windows that means
        // a \?\ prefix on both sides or neither. A test holding the raw
        // temp path would fail every lookup for the wrong reason.
        let ws_canon = ws.canonicalize().unwrap();
        let state = State {
            workspace: ws_canon.clone(),
            duckdb: std::path::PathBuf::from("duckdb"),
            run_lock: RunGate::new(1),
            running: Mutex::new(std::collections::HashSet::new()),
            console: console_auth::Console::configure(&ws_canon, "127.0.0.1", None).unwrap(),
            host: "127.0.0.1".into(),
            tick_interval: std::time::Duration::from_secs(15),
        };

        let leaked = read_pipeline_file(&state, "connections/prod-db.json");
        assert!(leaked.is_err(), "a stored connection was readable through the pipeline route");
        assert!(read_pipeline_file(&state, "pipelines/real.json").is_ok(), "a real pipeline still reads");
    }

    /// An install that already had console schedules must keep firing across
    /// the move to the shared store, and must not gain a duplicate for a
    /// pipeline the desktop app had already scheduled.
    #[test]
    fn the_old_console_store_is_carried_over_without_duplicating() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path();

        // The desktop already schedules one of these two pipelines.
        save_schedule_at(
            ws,
            &serde_json::json!({ "id": "already-known", "enabled": true, "intervalSeconds": 600 }),
        )
        .unwrap();

        std::fs::write(
            ws.join("panel-schedules.json"),
            serde_json::json!({
                "already-known": { "enabled": true, "intervalMinutes": 5, "cron": "" },
                "console-only": { "enabled": true, "intervalMinutes": 15, "cron": "" },
                "never-configured": { "enabled": false, "intervalMinutes": 0, "cron": "" },
            })
            .to_string(),
        )
        .unwrap();

        migrate_legacy_schedules(ws);

        let list = schedules::load(ws).unwrap();
        let ids: std::collections::HashSet<&str> =
            list.iter().map(|s| s.pipeline_id.as_str()).collect();
        assert!(ids.contains("console-only"), "a console schedule was lost");
        assert!(
            !ids.contains("never-configured"),
            "an entry with no cron and no interval was imported as a schedule"
        );
        assert_eq!(
            list.iter().filter(|s| s.pipeline_id == "already-known").count(),
            1,
            "the pipeline the desktop already scheduled gained a duplicate"
        );
        // ...and it kept the desktop's value rather than the console's 5 minutes.
        let known = list.iter().find(|s| s.pipeline_id == "already-known").unwrap();
        assert!(matches!(known.kind, ScheduleKind::Interval { seconds: 600 }));

        // Running again is a no-op, so a restart does not re-import.
        migrate_legacy_schedules(ws);
        assert_eq!(schedules::load(ws).unwrap().len(), list.len(), "re-imported on restart");
    }

    #[test]
    fn normalize_cron_pads_five_fields_and_validates() {
        // A standard 5-field cron gets a "0 " seconds field prepended so the
        // `cron` crate (which wants 6/7 fields) accepts it, and the result parses.
        let five = normalize_cron("0 9 * * 1").expect("5-field accepted");
        assert_eq!(five, "0 0 9 * * 1");
        assert!(five.parse::<cron::Schedule>().is_ok(), "padded expr parses");
        // A 6-field expression passes through unchanged and parses.
        let six = normalize_cron("*/30 * * * * *").expect("6-field accepted");
        assert_eq!(six, "*/30 * * * * *");
        assert!(six.parse::<cron::Schedule>().is_ok());
        // Garbage / wrong field counts are rejected (never fire silently).
        assert!(normalize_cron("not a cron").is_none());
        assert!(normalize_cron("* * *").is_none());
        assert!(normalize_cron("").is_none());
    }

    /// The web editor must encrypt connection secrets exactly like the desktop.
    ///
    /// This drives `connection_secret_cmd`, the same function the HTTP handler
    /// calls, so reverting that handler to the old echo-the-payload-back
    /// behaviour fails here. The assertion that matters is the negative one:
    /// the stored form must NOT contain the password. A round-trip assertion
    /// alone would have passed against the broken pass-through, because
    /// echoing a payload round-trips perfectly.
    #[test]
    fn web_editor_encrypts_connection_secrets() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path();
        let body = serde_json::to_vec(&serde_json::json!({
            "payloadJson": r#"{"host":"db.internal","user":"reporting","password":"hunter2"}"#
        }))
        .unwrap();

        let stored = connection_secret_cmd(ws, "connection_encrypt_payload", &body).expect("encrypts");
        assert!(
            !stored.contains("hunter2"),
            "password reached disk in clear text: {stored}"
        );
        assert!(stored.contains("enc:v1:"), "no ciphertext marker: {stored}");
        // Non-secret fields stay readable so the connection list still renders.
        assert!(stored.contains("db.internal"), "host should not be encrypted");

        let back_body =
            serde_json::to_vec(&serde_json::json!({ "payloadJson": stored })).unwrap();
        let back = connection_secret_cmd(ws, "connection_decrypt_payload", &back_body)
            .expect("decrypts");
        assert!(back.contains("hunter2"), "did not survive the round trip");
    }

    /// A workspace written before this fix holds plaintext. Opening it must
    /// keep working rather than erroring, which is why the decrypt side is
    /// deliberately lenient.
    #[test]
    fn web_editor_still_opens_legacy_plaintext_connections() {
        let tmp = tempfile::tempdir().unwrap();
        let body = serde_json::to_vec(&serde_json::json!({
            "payloadJson": r#"{"host":"db.internal","password":"hunter2"}"#
        }))
        .unwrap();
        let back = connection_secret_cmd(tmp.path(), "connection_decrypt_payload", &body)
            .expect("lenient");
        assert!(back.contains("hunter2"), "legacy plaintext must still load");
    }

    /// Editing a cron expression has to take effect before the old one fires.
    ///
    /// The armed occurrence was keyed by schedule id alone, so an edit changed
    /// nothing until the OLD occurrence came round. A schedule moved from
    /// 03:00 to 09:00 skipped 09:00 entirely and then fired at 03:00 the next
    /// morning: not merely late, but firing at the one time it had just been
    /// moved away from.
    #[test]
    fn an_edited_cron_expression_is_armed_from_the_new_one() {
        use chrono::{Datelike, TimeZone, Timelike};
        let parse = |e: &str| {
            normalize_cron(e).and_then(|x| x.parse::<cron::Schedule>().ok()).expect("bad cron")
        };
        let at = |h: u32, m: u32| {
            chrono::Local.with_ymd_and_hms(2026, 8, 15, h, m, 0).single().expect("ambiguous local time")
        };

        // 03:00 daily, first seen at 08:00: arm tomorrow, do not fire.
        let daily_3am = parse("0 3 * * *");
        let (fire, armed) = cron_decision(None, "0 3 * * *", &daily_3am, at(8, 0));
        assert!(!fire, "a schedule fired the moment it was first seen");
        let armed = armed.expect("nothing was armed");
        assert_eq!(armed.0, "0 3 * * *");
        assert_eq!(armed.1.hour(), 3, "armed at the wrong hour");

        // Now it is edited to 09:00. The next tick must re-arm from the NEW
        // expression rather than keep waiting for tomorrow's 03:00.
        let daily_9am = parse("0 9 * * *");
        let (fire, rearmed) = cron_decision(Some(&armed), "0 9 * * *", &daily_9am, at(8, 0));
        assert!(!fire, "the edit itself fired a run");
        let rearmed = rearmed.expect("nothing was armed after the edit");
        assert_eq!(rearmed.0, "0 9 * * *", "still armed by the old expression");
        assert_eq!(rearmed.1.hour(), 9, "did not re-arm from the edited expression");
        assert_eq!(rearmed.1.day(), 15, "the edit was pushed to tomorrow");

        // And at 09:00 it fires, then arms the following day.
        let (fire, next) = cron_decision(Some(&rearmed), "0 9 * * *", &daily_9am, at(9, 0));
        assert!(fire, "the edited schedule did not fire at its new time");
        let next = next.expect("nothing was armed after firing");
        assert_eq!(next.1.day(), 16, "re-armed on the same day, so it would fire twice");

        // An unchanged expression that is not due yet is left exactly alone,
        // or the occurrence would be pushed away on every tick and never come.
        let (fire, held) = cron_decision(Some(&next), "0 9 * * *", &daily_9am, at(9, 1));
        assert!(!fire);
        assert_eq!(held.unwrap().1, next.1, "an armed occurrence was moved by a tick");
    }

    /// The first thing an unauthenticated caller reaches must be bounded.
    ///
    /// `read_request` runs before anyone is identified, on a thread spawned per
    /// connection with no ceiling. It had no read deadline, so one byte and
    /// silence parked that thread for the life of the process, and it believed
    /// whatever Content-Length it was handed, so a declared body was buffered
    /// whole before anything looked at who was asking.
    #[test]
    fn an_unidentified_caller_cannot_park_a_thread_or_name_its_own_body_size() {
        use std::io::Write;
        use std::net::{TcpListener, TcpStream};

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();

        // An outsized Content-Length is refused before a byte of it is read.
        let sender = std::thread::spawn(move || {
            let mut c = TcpStream::connect(addr).unwrap();
            let _ = write!(
                c,
                "POST /api/run HTTP/1.1\r\nHost: x\r\nContent-Length: {}\r\n\r\n",
                MAX_BODY + 1
            );
            // Deliberately never sends the body: the refusal must not depend
            // on the caller actually delivering what it claimed.
            std::thread::sleep(std::time::Duration::from_millis(200));
        });
        let (mut server, _) = listener.accept().unwrap();
        let err = match read_request(&mut server) {
            Err(e) => e,
            Ok(_) => panic!("an unbounded body was accepted"),
        };
        assert!(err.contains("too large"), "wrong refusal: {err}");
        sender.join().unwrap();

        // And an ordinary request leaves the socket with a deadline on it, so
        // no later read on this connection can block forever either.
        let sender = std::thread::spawn(move || {
            let mut c = TcpStream::connect(addr).unwrap();
            let _ = write!(c, "GET /api/summary HTTP/1.1\r\nHost: x\r\n\r\n");
            std::thread::sleep(std::time::Duration::from_millis(200));
        });
        let (mut server, _) = listener.accept().unwrap();
        let req = read_request(&mut server).expect("an ordinary request was refused");
        assert_eq!(req.path, "/api/summary");
        assert!(
            server.read_timeout().unwrap().is_some(),
            "the connection has no read deadline, so a stalled caller pins the thread"
        );
        sender.join().unwrap();
    }
}
