//! The workspace schedule store, shared by everything that schedules a run.
//!
//! There used to be two stores. The desktop app kept `schedules.json`, a list
//! of records with their own ids, their own names, cron/interval/file-watch
//! kinds and run history. `duckle-runner serve` kept `panel-schedules.json`, an
//! object keyed by pipeline id holding `{enabled, intervalMinutes, cron}`.
//! Neither could see the other, so a schedule set up on the desktop was
//! invisible to the web console and the console's own list was invisible to the
//! desktop. The natural response to a console that shows nothing scheduled is
//! to schedule it again, which is how one pipeline ends up with two owners.
//!
//! This is the one store: `<workspace>/schedules.json`, in the record format,
//! which is a superset of what the console models.
//!
//! Sharing a file between processes means no writer may assume the copy it read
//! is still current. Every mutation goes through [`update`], which takes an
//! exclusive lock, re-reads from disk, applies the change to *that* list, and
//! writes it back through a temporary file. A blind write of an in-memory list
//! would silently drop whatever the other process added since, and the symptom
//! - a schedule that was definitely saved, gone an hour later - is close to
//! unfalsifiable after the fact.

use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

use crate::runlock;

/// How a schedule decides it is time to run.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ScheduleKind {
    /// Standard 5-field cron (minute hour day month weekday), or 6/7-field
    /// with a leading seconds field. Evaluated in the machine's local time
    /// zone (issue #194).
    Cron { expr: String },
    /// Fire every N seconds since last run (or app start).
    Interval { seconds: u64 },
    /// Fire when a file or folder changes (debounced ~2s).
    FileWatch {
        path: String,
        #[serde(default)]
        recursive: bool,
    },
}

/// One scheduled pipeline.
///
/// Timestamps are UTC in the file. Cron is evaluated in local time at fire
/// time, which is a display-vs-storage split, not a contradiction.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Schedule {
    pub id: String,
    pub pipeline_id: String,
    pub name: String,
    #[serde(default = "default_true")]
    pub enabled: bool,
    pub kind: ScheduleKind,
    #[serde(default)]
    pub last_run_at: Option<chrono::DateTime<chrono::Utc>>,
    #[serde(default)]
    pub last_run_status: Option<String>,
    #[serde(default)]
    pub last_run_duration_ms: Option<u64>,
    #[serde(default)]
    pub last_run_error: Option<String>,
    #[serde(default)]
    pub next_run_at: Option<chrono::DateTime<chrono::Utc>>,
}

fn default_true() -> bool {
    true
}

pub fn schedules_path(workspace: &Path) -> PathBuf {
    workspace.join("schedules.json")
}

/// The store as it is on disk right now.
///
/// A missing file is an empty list, not an error: a fresh workspace has no
/// schedules and that is not a problem to report. A file that exists but will
/// not parse IS an error, because silently treating a corrupt store as empty
/// is how a scheduler stops running everything without saying why.
pub fn load(workspace: &Path) -> Result<Vec<Schedule>, String> {
    let p = schedules_path(workspace);
    if !p.exists() {
        return Ok(Vec::new());
    }
    let content = std::fs::read_to_string(&p).map_err(|e| e.to_string())?;
    if content.trim().is_empty() {
        return Ok(Vec::new());
    }
    serde_json::from_str(&content).map_err(|e| format!("Parse schedules.json: {}", e))
}

/// Apply a change to the store and persist it, as one exclusive step.
///
/// `f` receives the list as it is on disk at this moment, not a copy the caller
/// read earlier, so a change made by the other process in between survives.
/// Returns the list as written, which callers should adopt as their new
/// in-memory state for the same reason.
pub fn update<F>(workspace: &Path, f: F) -> Result<Vec<Schedule>, String>
where
    F: FnOnce(&mut Vec<Schedule>),
{
    let _guard = lock_store(workspace)?;
    let mut list = load(workspace)?;
    f(&mut list);
    write_atomically(workspace, &list)?;
    Ok(list)
}

/// Hold the store lock, waiting briefly for the other process to finish.
///
/// Unlike a run lock, where a clash means "someone else is already doing this
/// so skip", a clash here means "someone else is mid-write, so wait": the write
/// is a few milliseconds and the caller has a change that must not be dropped.
/// The ceiling exists so a wedged holder degrades to a reported error rather
/// than a hung UI, though the kernel releases the lock on process death, so
/// reaching it at all should mean a genuinely stuck writer.
fn lock_store(workspace: &Path) -> Result<runlock::RunLock, String> {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        // A nested key: pipeline ids flatten their separators to underscores, so
        // no pipeline can ever name this lock and stall a save by running.
        if let Some(lock) = runlock::try_acquire_nested(workspace, "store", "schedules") {
            return Ok(lock);
        }
        if std::time::Instant::now() >= deadline {
            return Err("Timed out waiting to write schedules.json".into());
        }
        std::thread::sleep(std::time::Duration::from_millis(25));
    }
}

/// Write through a temporary file in the same directory, then rename over.
///
/// A reader either sees the previous store or the new one. Writing in place
/// leaves a window where the file is half-written, and the other process is
/// polling it every few seconds, so that window gets hit.
fn write_atomically(workspace: &Path, list: &[Schedule]) -> Result<(), String> {
    let p = schedules_path(workspace);
    if let Some(parent) = p.parent() {
        std::fs::create_dir_all(parent).map_err(|e| e.to_string())?;
    }
    let body = serde_json::to_string_pretty(list).map_err(|e| e.to_string())?;
    let tmp = p.with_extension("json.tmp");
    std::fs::write(&tmp, body).map_err(|e| e.to_string())?;
    // Windows refuses a rename onto an existing file, so clear the way first.
    // The store lock is what keeps this from being a window another writer can
    // fall into; a reader that lands here sees a missing file, which reads as
    // an empty store for one poll rather than as corruption.
    #[cfg(windows)]
    let _ = std::fs::remove_file(&p);
    std::fs::rename(&tmp, &p).map_err(|e| e.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn interval(pipeline: &str, seconds: u64) -> Schedule {
        Schedule {
            id: format!("id-{pipeline}"),
            pipeline_id: pipeline.into(),
            name: pipeline.into(),
            enabled: true,
            kind: ScheduleKind::Interval { seconds },
            last_run_at: None,
            last_run_status: None,
            last_run_duration_ms: None,
            last_run_error: None,
            next_run_at: None,
        }
    }

    #[test]
    fn a_missing_store_is_empty_and_a_corrupt_one_is_an_error() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path();
        assert!(load(ws).unwrap().is_empty(), "a fresh workspace has no schedules");

        std::fs::write(schedules_path(ws), b"{ not json").unwrap();
        assert!(
            load(ws).is_err(),
            "a corrupt store must be reported, not read as 'nothing scheduled'"
        );
    }

    #[test]
    fn a_write_keeps_what_the_other_process_added_in_the_meantime() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path();

        // The desktop app loads the store and holds it in memory.
        update(ws, |v| v.push(interval("nightly", 3600))).unwrap();
        let stale = load(ws).unwrap();
        assert_eq!(stale.len(), 1);

        // The web console adds one while the desktop still holds the old copy.
        update(ws, |v| v.push(interval("hourly", 60))).unwrap();

        // The desktop now records a run. It must not write `stale` back over
        // the top, which is what a blind save of in-memory state would do.
        update(ws, |v| {
            let s = v.iter_mut().find(|s| s.pipeline_id == "nightly").expect("lost the schedule");
            s.last_run_status = Some("success".into());
        })
        .unwrap();

        let after = load(ws).unwrap();
        let ids: Vec<&str> = after.iter().map(|s| s.pipeline_id.as_str()).collect();
        assert_eq!(ids, vec!["nightly", "hourly"], "a concurrent addition was dropped");
        assert_eq!(after[0].last_run_status.as_deref(), Some("success"));
    }

    #[test]
    fn the_store_survives_writers_running_at_the_same_time() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path().to_path_buf();

        // Every thread appends one schedule. With a read-modify-write under the
        // lock all of them survive; with a plain read-then-write most are lost.
        let threads: Vec<_> = (0..8)
            .map(|i| {
                let ws = ws.clone();
                std::thread::spawn(move || {
                    update(&ws, |v| v.push(interval(&format!("p{i}"), 60))).unwrap();
                })
            })
            .collect();
        for t in threads {
            t.join().unwrap();
        }

        let after = load(&ws).unwrap();
        assert_eq!(after.len(), 8, "concurrent writers lost updates");
    }
}
