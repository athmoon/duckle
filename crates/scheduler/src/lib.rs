//! Duckle scheduler.
//!
//! Cron- and interval-based triggers for pipelines. Schedules are
//! persisted to `<workspace>/schedules.json` so they survive restarts.
//! A single tokio task wakes every 15 seconds, decides which schedules
//! are due, and fires each as a non-blocking spawn that calls into the
//! shared `DuckdbEngine`.

use chrono::{DateTime, Local, Utc};
use cron::Schedule as CronSchedule;
use duckle_duckdb_engine::{
    append_run_record, runlock, schedules, DuckdbEngine, RunRecord, RunResult,
};
use notify::{RecommendedWatcher, RecursiveMode};
use notify_debouncer_mini::{new_debouncer, DebounceEventResult, Debouncer};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tokio::time;
use tracing::warn;

/// Default poll cadence for checking due schedules. Overridable via the
/// DUCKLE_TICK_INTERVAL env var (whole seconds, must be > 0) so sub-15s
/// real-time schedules can fire closer to their configured rate (issue #135).
const DEFAULT_TICK_INTERVAL: Duration = Duration::from_secs(15);
const WATCH_DEBOUNCE: Duration = Duration::from_secs(2);

/// Resolve the scheduler poll cadence: DUCKLE_TICK_INTERVAL (whole seconds)
/// if set and greater than 0, otherwise the 15s default.
fn tick_interval() -> Duration {
    std::env::var("DUCKLE_TICK_INTERVAL")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .filter(|n| *n > 0)
        .map(Duration::from_secs)
        .unwrap_or(DEFAULT_TICK_INTERVAL)
}

/// The schedule record and its trigger kinds live in the engine crate, because
/// `duckle-runner serve` writes the same store and a second definition of the
/// same file format is a drift waiting to happen. Re-exported so callers keep
/// naming them here.
pub use duckle_duckdb_engine::schedules::{Schedule, ScheduleKind};

#[derive(Clone)]
pub struct Scheduler {
    inner: Arc<Mutex<SchedulerInner>>,
    engine: DuckdbEngine,
    fire_tx: UnboundedSender<String>,
}

struct SchedulerInner {
    schedules: Vec<Schedule>,
    workspace_path: Option<PathBuf>,
    /// Why the store could not be read, when it could not be.
    ///
    /// Kept so the answer to "what are my schedules?" can be the truth rather
    /// than an empty list. See [`Scheduler::list`].
    load_error: Option<String>,
    /// Active file-watchers, keyed by schedule id. Holding the
    /// `Debouncer` keeps the watch alive; dropping it stops watching.
    watchers: HashMap<String, Debouncer<RecommendedWatcher>>,
    /// Receiver for file-watch fires; taken by `spawn_ticker`.
    fire_rx: Option<UnboundedReceiver<String>>,
}

/// What a schedule locks when it fires.
///
/// The pipeline, not the schedule record. The pipeline owns the sink and the
/// `xf.incremental` watermark, so it is the thing that must not run twice: two
/// schedules pointed at one pipeline and coinciding at midnight collide every
/// bit as much as two processes do. It also has to be the pipeline for the
/// lock to work across products, because the web console identifies a schedule
/// by its pipeline while this crate mints a uuid, so a record-keyed lock would
/// have the two naming different files and guarding nothing.
fn lock_key(s: &Schedule) -> &str {
    &s.pipeline_id
}

/// The answer to "may this process run that schedule right now?".
enum Claim {
    /// Yes. Dropping the payload gives the claim back.
    Ours(Option<runlock::RunLock>),
    /// No - another Duckle process is already running it.
    Taken,
}

/// Ask for the exclusive right to run `pipeline_id`.
///
/// Both of this crate's fire paths and the runner's own scheduler go through a
/// lock like this, because the in-process guards each of them keeps - a
/// semaphore here, a last-fired map there - say nothing about the other
/// process. Two schedulers on one workspace is not a misconfiguration: it is
/// what a workspace looks like mid-way through moving from a laptop to a
/// server. Firing twice means two runs into the same sink and two runs
/// advancing the same `xf.incremental` watermark, and the second is how a load
/// silently skips rows.
///
/// See [`lock_key`] for why the key is the pipeline rather than the schedule
/// record that fired.
///
/// A scheduler with no workspace is handed an unheld claim rather than a
/// refusal: there is nothing on disk for two processes to race over, and
/// `run_now` declines such a run on its own terms with a clearer message than
/// a lock could give.
fn claim_run(workspace: Option<&Path>, pipeline_id: &str) -> Claim {
    match workspace {
        None => Claim::Ours(None),
        Some(ws) => match runlock::try_acquire(ws, pipeline_id) {
            Some(lock) => Claim::Ours(Some(lock)),
            None => Claim::Taken,
        },
    }
}

impl Scheduler {
    pub fn new(engine: DuckdbEngine) -> Self {
        let (fire_tx, fire_rx) = unbounded_channel();
        Self {
            inner: Arc::new(Mutex::new(SchedulerInner {
                schedules: Vec::new(),
                workspace_path: None,
                load_error: None,
                watchers: HashMap::new(),
                fire_rx: Some(fire_rx),
            })),
            engine,
            fire_tx,
        }
    }

    /// Switch to a different workspace path. Loads schedules from the
    /// new path; computes next-run times for each; rebuilds watchers.
    pub fn set_workspace(&self, path: Option<PathBuf>) {
        let mut g = self.inner.lock().expect("scheduler poisoned");
        g.workspace_path = path;
        self.reload(&mut g);
        self.rebuild_watchers(&mut g);
    }

    /// Re-read the store, and remember it if it will not be read.
    fn reload(&self, inner: &mut SchedulerInner) {
        let Some(path) = inner.workspace_path.clone() else {
            inner.schedules = Vec::new();
            inner.load_error = None;
            return;
        };
        match schedules::load(&path) {
            Ok(mut list) => {
                for s in list.iter_mut() {
                    compute_next_run(s);
                }
                inner.schedules = list;
                inner.load_error = None;
            }
            Err(e) => {
                warn!("Failed to load schedules: {}", e);
                // Emptied rather than left as it was. This may be a different
                // workspace than the one those schedules came from, and a
                // ticker firing another workspace's schedules would be far
                // worse than firing none. Nothing is lost by it: the file is
                // never written back over, because every write re-reads under
                // a lock and fails the same way.
                inner.schedules = Vec::new();
                inner.load_error = Some(e);
            }
        }
    }

    /// Recreate file-watchers for the current schedule set. Drops all
    /// existing watchers and rebuilds from enabled FileWatch
    /// schedules.
    fn rebuild_watchers(&self, inner: &mut SchedulerInner) {
        inner.watchers.clear();
        let specs: Vec<(String, String, bool)> = inner
            .schedules
            .iter()
            .filter(|s| s.enabled)
            .filter_map(|s| match &s.kind {
                ScheduleKind::FileWatch { path, recursive } => {
                    Some((s.id.clone(), path.clone(), *recursive))
                }
                _ => None,
            })
            .collect();
        for (id, path, recursive) in specs {
            match self.make_watcher(&id, &path, recursive) {
                Ok(w) => {
                    inner.watchers.insert(id, w);
                }
                Err(e) => warn!("File-watch setup failed for {}: {}", id, e),
            }
        }
    }

    fn make_watcher(
        &self,
        schedule_id: &str,
        path: &str,
        recursive: bool,
    ) -> notify::Result<Debouncer<RecommendedWatcher>> {
        let tx = self.fire_tx.clone();
        let sid = schedule_id.to_string();
        let mut debouncer = new_debouncer(WATCH_DEBOUNCE, move |res: DebounceEventResult| {
            if let Ok(events) = res {
                if !events.is_empty() {
                    let _ = tx.send(sid.clone());
                }
            }
        })?;
        let mode = if recursive {
            RecursiveMode::Recursive
        } else {
            RecursiveMode::NonRecursive
        };
        debouncer.watcher().watch(Path::new(path), mode)?;
        Ok(debouncer)
    }

    /// The schedules, or why the store could not be read.
    ///
    /// A store that will not parse used to come back as an empty list, which
    /// reads as "you have no schedules" - the most alarming way possible to
    /// say "I could not open the file", and one that invites re-creating
    /// schedules that are still sitting on disk.
    ///
    /// While in the failed state this retries the read, so repairing the file
    /// is enough to recover without restarting the app. A healthy store is
    /// served from memory and costs nothing.
    pub fn list(&self) -> Result<Vec<Schedule>, String> {
        let mut g = self.inner.lock().expect("scheduler poisoned");
        if g.load_error.is_some() {
            self.reload(&mut g);
        }
        match &g.load_error {
            Some(e) => Err(e.clone()),
            None => Ok(g.schedules.clone()),
        }
    }

    pub fn upsert(&self, mut schedule: Schedule) -> Result<Schedule, String> {
        match &schedule.kind {
            ScheduleKind::Cron { expr } => {
                CronSchedule::from_str(expr)
                    .map_err(|e| format!("Invalid cron expression: {}", e))?;
            }
            ScheduleKind::Interval { seconds } => {
                if *seconds < 1 {
                    return Err("Interval must be at least 1 second".into());
                }
            }
            ScheduleKind::FileWatch { path, .. } => {
                if path.trim().is_empty() {
                    return Err("Watch path is required".into());
                }
            }
        }
        if schedule.id.is_empty() {
            schedule.id = uuid::Uuid::new_v4().to_string();
        }
        compute_next_run(&mut schedule);
        let mut g = self.inner.lock().expect("scheduler poisoned");
        let saved = schedule.clone();
        self.commit(&mut g, move |list| {
            match list.iter().position(|s| s.id == saved.id) {
                Some(idx) => {
                    // Upsert carries config only; preserve the existing
                    // run-history fields so a partial payload doesn't wipe
                    // last_run_* to null.
                    let prev = &list[idx];
                    let mut next = saved;
                    next.last_run_at = prev.last_run_at;
                    next.last_run_status = prev.last_run_status.clone();
                    next.last_run_duration_ms = prev.last_run_duration_ms;
                    next.last_run_error = prev.last_run_error.clone();
                    list[idx] = next;
                }
                None => list.push(saved),
            }
        })?;
        self.rebuild_watchers(&mut g);
        Ok(schedule)
    }

    pub fn delete(&self, id: &str) -> Result<(), String> {
        let mut g = self.inner.lock().expect("scheduler poisoned");
        g.watchers.remove(id);
        let id = id.to_string();
        self.commit(&mut g, move |list| list.retain(|s| s.id != id))
    }

    /// Apply a change to the shared store and adopt the result.
    ///
    /// The change runs against the list as it is on disk, not the copy this
    /// process is holding, because `duckle-runner serve` may be editing the
    /// same file. Whatever comes back becomes the in-memory state, so a
    /// schedule added by the other process shows up here rather than being
    /// overwritten on the next save.
    ///
    /// A scheduler with no workspace keeps its list in memory only; that is
    /// the pre-workspace state at startup, not an error worth surfacing.
    ///
    /// The write failing IS worth surfacing, and the caller decides how. This
    /// used to log and return nothing, so `upsert` and `delete` reported
    /// success to the UI for a schedule that never reached the disk: a full
    /// disk, a read-only workspace or a store that will not parse all looked
    /// like a save that worked, and the schedule was gone at the next restart.
    fn commit<F>(&self, inner: &mut SchedulerInner, change: F) -> Result<(), String>
    where
        F: FnOnce(&mut Vec<Schedule>),
    {
        let Some(path) = inner.workspace_path.clone() else {
            change(&mut inner.schedules);
            return Ok(());
        };
        let mut list = schedules::update(&path, change)?;
        // Next-run times are this process's own bookkeeping and are not what
        // the other process wrote, so recompute rather than trust.
        for s in list.iter_mut() {
            if s.next_run_at.is_none() {
                compute_next_run(s);
            }
        }
        inner.schedules = list;
        // The write re-read the store to apply the change, so it parses: any
        // remembered failure is stale.
        inner.load_error = None;
        Ok(())
    }

    /// Execute a schedule's pipeline right now, regardless of its
    /// timing. Updates last-run bookkeeping on completion.
    pub async fn run_now(&self, id: &str) -> Result<RunResult, String> {
        let (workspace, pipeline_id) = {
            let g = self.inner.lock().expect("scheduler poisoned");
            let s = g
                .schedules
                .iter()
                .find(|s| s.id == id)
                .ok_or_else(|| "Schedule not found".to_string())?;
            (g.workspace_path.clone(), s.pipeline_id.clone())
        };
        let workspace =
            workspace.ok_or_else(|| "No workspace set for the scheduler".to_string())?;
        // Resolve workspace context exactly like the canvas and the runner do:
        // substitute ${var} / ${context.var} (e.g. a context-based DB password),
        // inline SQL routines, and rewrite child-pipeline refs. Without this a
        // scheduled run sent the raw ${context.X} placeholder to the driver, so
        // a pipeline that ran fine from the canvas failed under a schedule with
        // auth errors like ORA-01017 (issue #32).
        let mut pipeline = duckle_duckdb_engine::context::resolve_workspace(
            &workspace,
            &pipeline_id,
            None,
        )?
        .doc;
        // Stamp the dynamic date/time builtins (${date}/${datetime}/...) at fire
        // time, so a recurring schedule writes a fresh-dated path on every run.
        duckle_duckdb_engine::context::apply_time_builtins(&mut pipeline);
        // Expand saved Salesforce connection refs into node auth props (#166
        // stage 2) BEFORE the env pass, so a connection field stored as
        // ${ENV:...} still resolves below.
        duckle_secrets::resolve_connection_refs(&workspace, &mut pipeline.nodes)?;
        // Resolve ${ENV:NAME} from the process environment so scheduled runs see
        // OS env vars just like the headless runner does (issue #137).
        duckle_duckdb_engine::context::apply_env(&mut pipeline);
        // A fresh per-run cancel scope so concurrent scheduled runs (and the
        // interactive run) don't share or reset each other's cancellation.
        let engine = self.engine.for_new_run();
        let started = Utc::now();
        // Log scheduled runs under the pipeline id (the scheduler has no
        // friendly name handy) so they still land in the per-pipeline log.
        let log_name = pipeline_id.clone();
        let result =
            tokio::task::spawn_blocking(move || engine.execute_pipeline_named(&pipeline, &log_name))
                .await
                .map_err(|e| e.to_string())?;
        self.record_run(id, started, &result);
        Ok(result)
    }

    /// Fire a schedule and make sure the outcome is recorded whichever way it
    /// goes.
    ///
    /// `run_now` only records after the pipeline has actually executed, so
    /// every `?` before that point - a pipeline file that has been renamed or
    /// deleted, a context that will not resolve, no workspace - produced a log
    /// line and nothing else. No alert, no `last_run_at`, and a schedule that
    /// reads as though it never fired at all. That is the same silence the
    /// runner's scheduler had, and it is worse here because the desktop is
    /// where someone would go looking for the reason.
    async fn fire_and_record(&self, id: &str, why: &str) {
        let started = Utc::now();
        let Err(e) = self.run_now(id).await else {
            return;
        };
        warn!("{} run {} failed: {}", why, id, e);
        // A run that never started still took time and still failed, which is
        // exactly what an operator needs to see against the schedule.
        let elapsed = Utc::now().signed_duration_since(started).num_milliseconds().max(0) as u64;
        let result = RunResult {
            status: "error".into(),
            duration_ms: elapsed,
            nodes: Default::default(),
            preview: Vec::new(),
            category: Some(
                duckle_duckdb_engine::error_category::categorize_error(&e).to_string(),
            ),
            error: Some(e),
        };
        self.record_run(id, started, &result);
    }

    fn record_run(&self, id: &str, started: DateTime<Utc>, result: &RunResult) {
        let mut g = self.inner.lock().expect("scheduler poisoned");
        let pipeline_id = g.schedules.iter().find(|s| s.id == id).map(|s| s.pipeline_id.clone());
        let (sid, status, duration, error) =
            (id.to_string(), result.status.clone(), result.duration_ms, result.error.clone());
        let saved = self.commit(&mut g, move |list| {
            if let Some(s) = list.iter_mut().find(|s| s.id == sid) {
                s.last_run_at = Some(started);
                s.last_run_status = Some(status);
                s.last_run_duration_ms = Some(duration);
                s.last_run_error = error;
                compute_next_run(s);
            }
        });
        // Nobody is waiting on this one - the run already happened - so the
        // outcome goes to the log. Run history below is written either way, so
        // the run is not lost with it.
        if let Err(e) = saved {
            warn!("Could not record the run against schedule {}: {}", id, e);
        }
        let workspace = g.workspace_path.clone();
        // Everything below talks to the disk and the network, so the lock goes
        // back first. Alert delivery waits up to ten seconds per channel, and
        // holding the scheduler's mutex across that stalls every other thing
        // that needs it - the next tick, the schedule list, an edit from the
        // UI - for as long as an unreachable webhook takes to time out.
        drop(g);

        // Append to the pipeline's run history too, and tell whoever asked to
        // be told. Alerting comes after the record so a channel that is down
        // cannot cost a run its history entry, and it never raises: see
        // duckle_duckdb_engine::alerts::notify.
        if let (Some(path), Some(pid)) = (workspace, pipeline_id) {
            let record = RunRecord::from_result(result, "scheduled");
            let _ = append_run_record(&path, &pid, record);
            duckle_duckdb_engine::alerts::notify(&path, &pid, result);
        }
    }

    /// Start the polling task and the file-watch fire listener.
    /// Returns immediately.
    pub fn spawn_ticker(&self) {
        // Cron / interval poller.
        let me = self.clone();
        tokio::spawn(async move {
            let mut tick = time::interval(tick_interval());
            tick.tick().await; // Skip the immediate tick.
            loop {
                tick.tick().await;
                me.fire_due().await;
            }
        });

        // File-watch fire listener - drains the channel watchers post to.
        let rx = {
            let mut g = self.inner.lock().expect("scheduler poisoned");
            g.fire_rx.take()
        };
        if let Some(mut rx) = rx {
            let me = self.clone();
            tokio::spawn(async move {
                while let Some(id) = rx.recv().await {
                    let me2 = me.clone();
                    tokio::spawn(async move {
                        // Watching is per process, so two Duckle processes
                        // watching one folder both see the same file land and
                        // both fire. Same clash as a cron tick, same guard.
                        let (workspace, pipeline_id) = {
                            let g = me2.inner.lock().expect("scheduler poisoned");
                            let pipeline_id = g
                                .schedules
                                .iter()
                                .find(|s| s.id == id)
                                .map(|s| lock_key(s).to_string());
                            (g.workspace_path.clone(), pipeline_id)
                        };
                        // A schedule that vanished between the file event and
                        // here has nothing to lock; run_now reports it missing.
                        let key = pipeline_id.unwrap_or_else(|| id.clone());
                        let _claim = match claim_run(workspace.as_deref(), &key) {
                            Claim::Ours(lock) => lock,
                            Claim::Taken => {
                                warn!(
                                    "Pipeline {} is already running in another process; \
                                     skipping the file-watch fire of {}",
                                    key, id
                                );
                                return;
                            }
                        };
                        me2.fire_and_record(&id, "File-watch").await;
                    });
                }
            });
        }
    }

    /// Take every schedule that is due, and claim it so nothing else takes it.
    ///
    /// Split out from `fire_due` so a test can drive the claim itself rather
    /// than a copy of it: the bug this guards against was invisible to a test
    /// that re-implemented the claiming step.
    ///
    /// Returns each due schedule's id alongside its pipeline id, because the
    /// schedule is what came due but the pipeline is what gets locked.
    fn claim_due(&self, now: DateTime<Utc>) -> Vec<(String, String)> {
        let mut g = self.inner.lock().expect("scheduler poisoned");
        let due: Vec<(String, String)> = g
            .schedules
            .iter()
            .filter(|s| s.enabled && matches!(s.next_run_at, Some(t) if t <= now))
            .map(|s| (s.id.clone(), lock_key(s).to_string()))
            .collect();
            // Claim the occurrence immediately, under the lock, by advancing
            // next_run_at to the next FUTURE time. The tick wakes every 15s and
            // run_now only recomputes next_run_at on completion (record_run);
            // without this claim a run slower than 15s gets re-fired every
            // tick. Advancing (vs clearing to None) keeps the schedule firing
            // on cadence even if this run errors before record_run.
            //
            // The claim goes to the STORE, not just to this process's copy.
            // Held in memory it was undone by the next commit for any reason
            // at all - a schedule edited in the UI, another schedule's run
            // finishing - because commit adopts the list from disk, where
            // next_run_at was still the time already claimed and therefore
            // still in the past. The run in flight was then due again on the
            // very next tick, and only the run lock stopped it, logging a
            // refusal that blamed "another process" for this one. Writing it
            // also makes the claim visible to the other process, so the lock
            // goes back to being the backstop it was meant to be.
        if !due.is_empty() {
            let claimed: Vec<String> = due.iter().map(|(id, _)| id.clone()).collect();
            if let Err(e) = self.commit(&mut g, move |list| {
                for s in list.iter_mut() {
                    if claimed.iter().any(|id| id == &s.id) {
                        claim_next_run(s, now);
                    }
                }
            }) {
                // The runs still go ahead: the lock keeps them from doubling,
                // and refusing to fire because the bookkeeping could not be
                // written would turn a full disk into a silent outage of every
                // schedule.
                warn!("Could not record the fire claim: {}", e);
                for s in g.schedules.iter_mut() {
                    if due.iter().any(|(id, _)| id == &s.id) {
                        claim_next_run(s, now);
                    }
                }
            }
        }
        due
    }

    async fn fire_due(&self) {
        let now = Utc::now();
        // Read the workspace under the same lock as the due list, so the path
        // used for the run lock is the one this tick actually decided against.
        let workspace = { self.inner.lock().expect("scheduler poisoned").workspace_path.clone() };
        let due = self.claim_due(now);
        for (id, pipeline_id) in due {
            let me = self.clone();
            let workspace = workspace.clone();
            let permit = run_permits().clone();
            tokio::spawn(async move {
                // Hold a permit for the whole run. Every schedule that comes due
                // in the same tick used to fire at once, so ten due at midnight
                // meant ten pipelines each sized for the whole machine. The
                // permit bounds that; the run still happens, it just queues.
                let _slot = permit.acquire_owned().await;
                // The semaphore above bounds this process only. Skipping on a
                // clash rather than queueing is deliberate: the next tick comes
                // round anyway, and a backlog of identical overdue runs helps
                // nobody.
                let _claim = match claim_run(workspace.as_deref(), &pipeline_id) {
                    Claim::Ours(lock) => lock,
                    Claim::Taken => {
                        warn!(
                            "Pipeline {} is already running in another process; \
                             skipping schedule {} this tick",
                            pipeline_id, id
                        );
                        return;
                    }
                };
                me.fire_and_record(&id, "Scheduled").await;
            });
        }
    }
}

/// How many scheduled pipelines may execute at once.
///
/// Set by power mode via DUCKLE_MAX_CONCURRENT_RUNS. Read once, because the
/// bound has to be a single shared semaphore for it to mean anything.
///
/// The default is deliberately generous rather than 1: firing due schedules
/// concurrently is long-standing behaviour here and some workspaces rely on
/// it. What it was missing was any ceiling at all. Each concurrent run gets
/// its own memory limit and its own DuckDB child, so the honest ceiling is a
/// function of RAM, which is why power mode asks rather than assumes.
fn run_permits() -> &'static std::sync::Arc<tokio::sync::Semaphore> {
    static PERMITS: std::sync::OnceLock<std::sync::Arc<tokio::sync::Semaphore>> =
        std::sync::OnceLock::new();
    PERMITS.get_or_init(|| {
        let n = std::env::var("DUCKLE_MAX_CONCURRENT_RUNS")
            .ok()
            .and_then(|v| v.trim().parse::<usize>().ok())
            .filter(|n| *n > 0)
            .unwrap_or(8);
        std::sync::Arc::new(tokio::sync::Semaphore::new(n))
    })
}

/// Advance next_run_at to the next occurrence strictly after `now`.
/// Used to "claim" a due schedule at dispatch so the 15s ticker can't
/// re-fire a still-running schedule. Unlike compute_next_run (which for
/// intervals is anchored on last_run_at and can still be in the past for
/// an overdue run), this is always anchored on `now`, guaranteeing a
/// future time.
fn claim_next_run(s: &mut Schedule, now: DateTime<Utc>) {
    s.next_run_at = match &s.kind {
        // Evaluate in local time (see parse_cron) and store the resulting
        // absolute instant as UTC.
        ScheduleKind::Cron { expr } => parse_cron(expr)
            .and_then(|sched| sched.after(&now.with_timezone(&Local)).next())
            .map(|dt| dt.with_timezone(&Utc)),
        ScheduleKind::Interval { seconds } => {
            Some(now + chrono::Duration::seconds(*seconds as i64))
        }
        ScheduleKind::FileWatch { .. } => None,
    };
}

fn compute_next_run(s: &mut Schedule) {
    if !s.enabled {
        s.next_run_at = None;
        return;
    }
    s.next_run_at = match &s.kind {
        ScheduleKind::Cron { expr } => parse_cron(expr)
            .and_then(|sched| sched.upcoming(Local).next())
            .map(|dt| dt.with_timezone(&Utc)),
        ScheduleKind::Interval { seconds } => {
            let base = s.last_run_at.unwrap_or_else(Utc::now);
            Some(base + chrono::Duration::seconds(*seconds as i64))
        }
        // Event-driven - no scheduled next-run time.
        ScheduleKind::FileWatch { .. } => None,
    };
}

/// The `cron` crate expects a 6- or 7-field expression (seconds first). Accept a
/// standard 5-field cron ("min hour dom mon dow") by prepending a "0 " seconds
/// field, and pass 6/7-field expressions through. Without this a hand-edited
/// 5-field expression parsed to None and the schedule silently never fired.
/// Mirrors normalize_cron in duckle-runner's serve.rs.
fn normalize_cron(expr: &str) -> Option<String> {
    match expr.split_whitespace().count() {
        5 => Some(format!("0 {}", expr)),
        6 | 7 => Some(expr.to_string()),
        _ => None,
    }
}

/// Parse a cron expression for schedule evaluation (issue #194).
///
/// Cron expressions are evaluated in the machine's LOCAL time zone, so
/// "0 0 3 * * *" means 3am where the user is, not 3am UTC. This matches how
/// the UI renders next-run times (toLocaleString) and how the web console has
/// behaved since #132. The computed instant is still stored as UTC.
fn parse_cron(expr: &str) -> Option<CronSchedule> {
    normalize_cron(expr).and_then(|e| CronSchedule::from_str(&e).ok())
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cron_parses_and_computes_next() {
        let mut s = Schedule {
            id: "t".into(),
            pipeline_id: "p1".into(),
            name: "every minute".into(),
            enabled: true,
            kind: ScheduleKind::Cron {
                expr: "0 * * * * *".into(),
            },
            last_run_at: None,
            last_run_status: None,
            last_run_duration_ms: None,
            last_run_error: None,
            next_run_at: None,
        };
        compute_next_run(&mut s);
        assert!(s.next_run_at.is_some());
        assert!(s.next_run_at.unwrap() > Utc::now());
    }

    /// Issue #194: cron must be evaluated in the machine's local time zone,
    /// not UTC. Asserting on the LOCAL hour (rather than a hardcoded UTC hour)
    /// keeps this correct on any developer machine and in CI.
    #[test]
    fn cron_fires_at_the_local_wall_clock_hour() {
        use chrono::Timelike;
        let mut s = Schedule {
            id: "t".into(),
            pipeline_id: "p1".into(),
            name: "daily 3am".into(),
            enabled: true,
            kind: ScheduleKind::Cron {
                expr: "0 0 3 * * *".into(),
            },
            last_run_at: None,
            last_run_status: None,
            last_run_duration_ms: None,
            last_run_error: None,
            next_run_at: None,
        };
        compute_next_run(&mut s);
        let next = s.next_run_at.expect("next_run_at set").with_timezone(&Local);
        assert_eq!(next.hour(), 3, "3am cron must land on 3am local, got {}", next);
        assert_eq!(next.minute(), 0);
    }

    /// The claim path (used at dispatch to stop a re-fire) must agree with
    /// compute_next_run, or a schedule fires correctly once and then re-arms
    /// in the wrong zone.
    #[test]
    fn claim_next_run_also_uses_local_time() {
        use chrono::Timelike;
        let mut s = Schedule {
            id: "t".into(),
            pipeline_id: "p1".into(),
            name: "daily 3am".into(),
            enabled: true,
            kind: ScheduleKind::Cron {
                expr: "0 0 3 * * *".into(),
            },
            last_run_at: None,
            last_run_status: None,
            last_run_duration_ms: None,
            last_run_error: None,
            next_run_at: None,
        };
        claim_next_run(&mut s, Utc::now());
        let next = s.next_run_at.expect("next_run_at set").with_timezone(&Local);
        assert_eq!(next.hour(), 3, "claim must also be local, got {}", next);
    }

    /// A hand-written 5-field cron used to parse to None, leaving next_run_at
    /// unset so the schedule silently never fired.
    #[test]
    fn five_field_cron_is_accepted_and_scheduled() {
        use chrono::Timelike;
        let mut s = Schedule {
            id: "t".into(),
            pipeline_id: "p1".into(),
            name: "daily 3am, 5-field".into(),
            enabled: true,
            kind: ScheduleKind::Cron {
                expr: "0 3 * * *".into(),
            },
            last_run_at: None,
            last_run_status: None,
            last_run_duration_ms: None,
            last_run_error: None,
            next_run_at: None,
        };
        compute_next_run(&mut s);
        let next = s.next_run_at.expect("5-field cron must schedule").with_timezone(&Local);
        assert_eq!(next.hour(), 3);
        assert_eq!(next.minute(), 0);
    }

    #[test]
    fn normalize_cron_rejects_bad_field_counts() {
        assert_eq!(normalize_cron("0 3 * * *").as_deref(), Some("0 0 3 * * *"));
        assert_eq!(normalize_cron("0 0 3 * * *").as_deref(), Some("0 0 3 * * *"));
        assert!(normalize_cron("* * *").is_none());
        assert!(normalize_cron("* * * * * * * *").is_none());
        assert!(normalize_cron("").is_none());
    }

    #[test]
    fn interval_computes_next() {
        let mut s = Schedule {
            id: "t".into(),
            pipeline_id: "p1".into(),
            name: "every 5".into(),
            enabled: true,
            kind: ScheduleKind::Interval { seconds: 300 },
            last_run_at: None,
            last_run_status: None,
            last_run_duration_ms: None,
            last_run_error: None,
            next_run_at: None,
        };
        compute_next_run(&mut s);
        let next = s.next_run_at.expect("next_run_at set");
        let now = Utc::now();
        let delta = next - now;
        assert!(delta.num_seconds() <= 301 && delta.num_seconds() >= 299);
    }

    #[test]
    fn disabled_clears_next() {
        let mut s = Schedule {
            id: "t".into(),
            pipeline_id: "p1".into(),
            name: "off".into(),
            enabled: false,
            kind: ScheduleKind::Interval { seconds: 60 },
            last_run_at: None,
            last_run_status: None,
            last_run_duration_ms: None,
            last_run_error: None,
            next_run_at: Some(Utc::now()),
        };
        compute_next_run(&mut s);
        assert!(s.next_run_at.is_none());
    }

    /// The condition the run lock exists for. `fire_due` claims an occurrence
    /// by advancing `next_run_at` under the in-process mutex, which is enough
    /// for one process and does nothing for two: the claim never reaches disk
    /// at fire time, so a desktop app and a `duckle-runner serve` daemon
    /// pointed at one workspace independently decide the same schedule is due
    /// in the same second. This asserts that decision is genuinely made twice,
    /// so the guard in `fire_due` is load-bearing rather than defensive.
    #[test]
    fn two_schedulers_on_one_workspace_both_decide_the_same_run_is_due() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path().to_path_buf();
        let engine = || DuckdbEngine::new(PathBuf::from("duckdb"));

        // The desktop app, which writes the schedule to the workspace.
        let desktop = Scheduler::new(engine());
        desktop.set_workspace(Some(ws.clone()));
        desktop
            .upsert(Schedule {
                id: String::new(),
                pipeline_id: "nightly-load".into(),
                name: "every second".into(),
                enabled: true,
                // Six fields, so the leading one is seconds: due almost at once.
                kind: ScheduleKind::Cron { expr: "* * * * * *".into() },
                last_run_at: None,
                last_run_status: None,
                last_run_duration_ms: None,
                last_run_error: None,
                next_run_at: None,
            })
            .expect("schedule rejected");

        // A runner daemon started afterwards against the same workspace, which
        // is how a workspace gets promoted from a laptop to a server.
        let daemon = Scheduler::new(engine());
        daemon.set_workspace(Some(ws.clone()));

        // Let the next-run time arrive for both.
        std::thread::sleep(Duration::from_millis(1500));
        let now = Utc::now();
        let due = |s: &Scheduler| -> Vec<String> {
            s.list().expect("schedules unreadable")
                .into_iter()
                .filter(|x| x.enabled && matches!(x.next_run_at, Some(t) if t <= now))
                .map(|x| x.id)
                .collect()
        };
        let a = due(&desktop);
        let b = due(&daemon);
        assert_eq!(a.len(), 1, "the desktop scheduler did not consider it due");
        assert_eq!(
            a, b,
            "both processes must reach the same fire decision for the lock to matter"
        );

        // And that shared decision is exactly what the lock arbitrates: the
        // first process to ask gets to run it, the second is turned away.
        // Keys come from lock_key, the same function both fire paths use, so a
        // change of mind about what gets locked fails here rather than shipping.
        let key = |s: &Scheduler, id: &str| -> String {
            lock_key(s.list().expect("schedules unreadable").iter().find(|x| x.id == id).expect("schedule vanished")).to_string()
        };
        let held = key(&desktop, &a[0]);
        let first = match claim_run(Some(&ws), &held) {
            Claim::Ours(lock) => lock.expect("a workspace was set, so a lock was due"),
            Claim::Taken => panic!("the first process could not take the run lock"),
        };
        assert!(
            matches!(claim_run(Some(&ws), &key(&daemon, &b[0])), Claim::Taken),
            "the second process was allowed to run the same pipeline"
        );

        // What is locked is the pipeline, and that is what makes the guard hold
        // across products: the web console names a schedule by its pipeline
        // while this crate mints a uuid, so a record-keyed lock would have the
        // two picking different files and guarding nothing. The ids genuinely
        // differ here, so this would catch that.
        assert_eq!(held, "nightly-load", "the lock was not keyed on the pipeline");
        assert_ne!(a[0], held, "the schedule id and pipeline id must differ here");
        drop(first);
    }

    /// A save that did not reach the disk is not a save.
    ///
    /// `commit` logged the write failure and returned nothing, so `upsert` and
    /// `delete` handed back Ok for a schedule that never reached the store. A
    /// read-only workspace, a full disk or a store that will not parse all
    /// looked exactly like success, and the schedule was simply absent at the
    /// next restart.
    #[test]
    fn a_schedule_that_could_not_be_written_is_not_reported_as_saved() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path().to_path_buf();
        // A store that will not parse. Every write is a read-modify-write, so
        // this fails the read and must not be silently overwritten either.
        std::fs::write(ws.join("schedules.json"), "{ this is not json").unwrap();

        let sched = Scheduler::new(DuckdbEngine::new(PathBuf::from("duckdb")));
        sched.set_workspace(Some(ws.clone()));

        let err = sched
            .upsert(Schedule {
                id: String::new(),
                pipeline_id: "nightly-load".into(),
                name: "nightly".into(),
                enabled: true,
                kind: ScheduleKind::Interval { seconds: 3600 },
                last_run_at: None,
                last_run_status: None,
                last_run_duration_ms: None,
                last_run_error: None,
                next_run_at: None,
            })
            .expect_err("a schedule that was never written was reported as saved");
        assert!(!err.is_empty(), "the failure has to say something");

        // And the unreadable store is left exactly as it was, rather than
        // being replaced by a list built from a failed read.
        let after = std::fs::read_to_string(ws.join("schedules.json")).unwrap();
        assert_eq!(after, "{ this is not json", "a corrupt store was overwritten");

        assert!(sched.delete("anything").is_err(), "delete reported success too");
    }

    /// "I could not read the file" must never be shown as "you have none".
    ///
    /// An unreadable store came back as an empty list, so the UI said there
    /// were no schedules - the most alarming possible way to report a parse
    /// error, and one that invites re-creating schedules that are still on
    /// disk. It also has to recover on its own once the file is repaired,
    /// because the alternative is telling someone to restart the app.
    #[test]
    fn an_unreadable_store_says_so_and_recovers_when_it_is_repaired() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path().to_path_buf();
        let sched = Scheduler::new(DuckdbEngine::new(PathBuf::from("duckdb")));

        // A workspace with a real schedule in it.
        sched.set_workspace(Some(ws.clone()));
        let saved = sched
            .upsert(Schedule {
                id: String::new(),
                pipeline_id: "nightly-load".into(),
                name: "nightly".into(),
                enabled: true,
                kind: ScheduleKind::Interval { seconds: 3600 },
                last_run_at: None,
                last_run_status: None,
                last_run_duration_ms: None,
                last_run_error: None,
                next_run_at: None,
            })
            .unwrap();
        assert_eq!(sched.list().unwrap().len(), 1);

        // Now the file is damaged - a half-written save, a bad merge.
        let good = std::fs::read_to_string(ws.join("schedules.json")).unwrap();
        std::fs::write(ws.join("schedules.json"), "[{\"id\": \"nightly").unwrap();
        sched.set_workspace(Some(ws.clone()));

        let err = sched.list().expect_err("an unreadable store was reported as no schedules");
        assert!(!err.is_empty(), "the failure has to say something");

        // And nothing fires while it cannot be read, rather than the previous
        // workspace's schedules firing against this one.
        assert!(sched.claim_due(Utc::now()).is_empty(), "a schedule fired from an unreadable store");

        // The file is left exactly as it was, so the schedules are recoverable.
        assert_eq!(
            std::fs::read_to_string(ws.join("schedules.json")).unwrap(),
            "[{\"id\": \"nightly",
            "the damaged store was overwritten"
        );

        // Repair it, and the next question gets the right answer without a
        // restart or a workspace switch.
        std::fs::write(ws.join("schedules.json"), good).unwrap();
        let back = sched.list().expect("a repaired store still reported as broken");
        assert_eq!(back.len(), 1);
        assert_eq!(back[0].id, saved.id, "the schedule came back changed");
    }

    /// A fire claim has to survive the next save, whatever caused it.
    ///
    /// `fire_due` advanced `next_run_at` to claim an occurrence, but only in
    /// this process's copy. `commit` adopts the list from disk, where the time
    /// was still the one already claimed and therefore still in the past, so
    /// any unrelated save - a schedule edited in the UI, another schedule's run
    /// finishing - put the in-flight schedule straight back into the due set.
    #[test]
    fn an_unrelated_save_does_not_make_a_running_schedule_due_again() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path().to_path_buf();
        let sched = Scheduler::new(DuckdbEngine::new(PathBuf::from("duckdb")));
        sched.set_workspace(Some(ws.clone()));

        let mut due_now = Schedule {
            id: String::new(),
            pipeline_id: "nightly-load".into(),
            name: "nightly".into(),
            enabled: true,
            kind: ScheduleKind::Interval { seconds: 3600 },
            last_run_at: None,
            last_run_status: None,
            last_run_duration_ms: None,
            last_run_error: None,
            next_run_at: None,
        };
        let running = sched.upsert(due_now.clone()).unwrap().id;
        // Make it due, the way an hour passing would.
        let now = Utc::now();
        {
            let mut g = sched.inner.lock().unwrap();
            sched
                .commit(&mut g, |list| {
                    for s in list.iter_mut() {
                        s.next_run_at = Some(now - chrono::Duration::seconds(1));
                    }
                })
                .unwrap();
        }

        // Claim it through the code a tick actually runs, not a copy of it.
        // An earlier version of this test re-implemented the claim and so
        // passed with the defect still in place.
        let claimed = sched.claim_due(now);
        assert_eq!(claimed.len(), 1, "the schedule was not due when it should have been");

        // Now something else saves, which is the step that used to undo it.
        due_now.id = String::new();
        due_now.pipeline_id = "unrelated".into();
        due_now.name = "unrelated".into();
        sched.upsert(due_now).unwrap();

        let after = sched.list().unwrap().into_iter().find(|s| s.id == running).unwrap();
        assert!(
            matches!(after.next_run_at, Some(t) if t > now),
            "the running schedule is due again after an unrelated save: {:?}",
            after.next_run_at
        );
    }

    /// A run that never gets as far as starting still has to be reported.
    ///
    /// `run_now` records only after the pipeline has executed, so every early
    /// return - a pipeline file renamed or deleted out from under a schedule,
    /// a context that will not resolve - used to leave a `warn!` in the log and
    /// nothing anywhere a person looks: the schedule kept its old green status,
    /// `last_run_at` stayed where it was, run history gained no entry and no
    /// alert went out. A schedule that stopped working looked like one that was
    /// working. This drives the failure through the fire path both triggers now
    /// use and asserts every one of those surfaces sees it.
    #[tokio::test]
    async fn a_schedule_whose_pipeline_is_gone_reports_a_failed_run() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path().to_path_buf();
        let sched = Scheduler::new(DuckdbEngine::new(PathBuf::from("duckdb")));
        sched.set_workspace(Some(ws.clone()));
        let id = sched
            .upsert(Schedule {
                id: String::new(),
                // No such file in the workspace: this is the pipeline someone
                // renamed without touching the schedule that points at it.
                pipeline_id: "nightly-load".into(),
                name: "nightly".into(),
                enabled: true,
                kind: ScheduleKind::Interval { seconds: 3600 },
                last_run_at: None,
                last_run_status: None,
                last_run_duration_ms: None,
                last_run_error: None,
                next_run_at: None,
            })
            .expect("schedule rejected")
            .id;

        sched.fire_and_record(&id, "Test").await;

        let after = sched.list().unwrap().into_iter().find(|s| s.id == id).expect("schedule vanished");
        assert_eq!(after.last_run_status.as_deref(), Some("error"));
        assert!(after.last_run_at.is_some(), "the fire left no last_run_at");
        assert!(
            after.last_run_error.is_some(),
            "the failure was not kept against the schedule"
        );

        // The same failure has to survive a restart, because the console reads
        // the store rather than this process's memory.
        let reread = schedules::load(&ws).expect("schedules did not persist");
        let stored = reread.iter().find(|s| s.id == id).expect("schedule not on disk");
        assert_eq!(stored.last_run_status.as_deref(), Some("error"));

        // And it has to reach run history, which is what the Runs view reads
        // and what the metrics textfile is derived from.
        let history = ws.join("runs").join("nightly-load.json");
        let text = std::fs::read_to_string(&history)
            .unwrap_or_else(|e| panic!("no run history at {}: {e}", history.display()));
        let records: Vec<serde_json::Value> = serde_json::from_str(&text).unwrap();
        let last = records.last().expect("run history is empty");
        assert_eq!(last["status"], "error");
        assert_eq!(last["trigger"], "scheduled");
        assert!(
            last["error"].as_str().unwrap_or("").contains("nightly-load"),
            "the record does not say which pipeline could not be loaded: {last}"
        );
    }
}
