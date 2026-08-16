//! `duckle-runner work` - claim items out of a batch and run them.
//!
//! A batch written by `ctl.foreach` with `dispatch: "queue"` is a list of
//! independent jobs in a file. This is the other half: a process that reads
//! that file, takes one item at a time, and runs it. Start it on one machine
//! and it is a resumable For Each. Start it on five machines pointed at the
//! same workspace and they share the work, without a queue server, a database
//! or any service between them.
//!
//! # How two workers avoid running the same item
//!
//! Each item is claimed with the same OS advisory lock a pipeline run uses
//! (`runlock`, `.duckle/locks/batch/`). The kernel releases it when the process
//! dies, so a worker that is killed mid-item leaves nothing to clean up and the
//! item becomes claimable again. There is no lease, no heartbeat and no
//! timeout, because there is nothing to expire.
//!
//! # What this guarantees, and what it does not
//!
//! **At least once, not exactly once.** The ledger line is written after the
//! item succeeds, so a worker that completes an item and dies before recording
//! it leaves that item looking undone, and another worker will run it again.
//! That is the honest trade for having no transactional store: the alternative
//! is recording before the work, which loses items instead of repeating them,
//! and a lost load is worse than a repeated one. Make the child idempotent -
//! an upsert sink rather than an append - and a repeat costs time, not
//! correctness.

use std::io::Write;
use std::path::{Path, PathBuf};

use duckle_duckdb_engine::{batch, runlock, DuckdbEngine};

/// One recorded attempt, appended after the fact.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct LedgerLine {
    v: u32,
    index: usize,
    status: String,
    at: String,
    /// Which worker ran it. Free text, for reading a ledger after the fact.
    worker: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

fn ledger_path(workspace: &Path, batch_id: &str) -> PathBuf {
    batch::batches_dir(workspace).join(format!("{batch_id}.ledger.ndjson"))
}

/// Which items of a batch are already finished.
///
/// Only successes count as done. A failed attempt is left claimable so another
/// worker, or the same one on a later pass, tries again - a batch where one
/// transient network error permanently consumed an item would be worse than
/// useless. The redrive of a genuinely broken item is a human decision, and
/// the ledger keeps the failures so there is something to look at.
fn finished(workspace: &Path, batch_id: &str) -> std::collections::HashSet<usize> {
    let mut done = std::collections::HashSet::new();
    let Ok(text) = std::fs::read_to_string(ledger_path(workspace, batch_id)) else {
        return done;
    };
    for line in text.lines() {
        if line.trim().is_empty() {
            continue;
        }
        if let Ok(l) = serde_json::from_str::<LedgerLine>(line) {
            if l.v == 1 && l.status == "ok" {
                done.insert(l.index);
            }
        }
    }
    done
}

fn record(workspace: &Path, batch_id: &str, line: &LedgerLine) -> Result<(), String> {
    let p = ledger_path(workspace, batch_id);
    if let Some(dir) = p.parent() {
        std::fs::create_dir_all(dir).map_err(|e| e.to_string())?;
    }
    let text = serde_json::to_string(line).map_err(|e| e.to_string())?;
    // One append of one short line, so concurrent workers interleave whole
    // lines rather than fragments of them.
    let mut f = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&p)
        .map_err(|e| format!("{}: {e}", p.display()))?;
    // Heal a torn tail before appending. A worker killed mid-write can leave a
    // line with no newline on it; appending straight onto that glues the two
    // together and destroys THIS record as well as the broken one, turning one
    // lost line into a second item run twice. Cheap to check, and the check is
    // what makes the ledger worth trusting.
    let needs_newline = std::fs::metadata(&p)
        .ok()
        .map(|m| m.len() > 0)
        .unwrap_or(false)
        && !ends_with_newline(&p);
    let payload = if needs_newline { format!("\n{text}\n") } else { format!("{text}\n") };
    f.write_all(payload.as_bytes()).map_err(|e| e.to_string())
}

fn ends_with_newline(p: &Path) -> bool {
    use std::io::{Read, Seek, SeekFrom};
    let Ok(mut f) = std::fs::File::open(p) else { return true };
    let Ok(len) = f.metadata().map(|m| m.len()) else { return true };
    if len == 0 {
        return true;
    }
    if f.seek(SeekFrom::End(-1)).is_err() {
        return true;
    }
    let mut last = [0u8; 1];
    match f.read_exact(&mut last) {
        Ok(()) => last[0] == b'\n',
        Err(_) => true,
    }
}

/// Every batch in the workspace, oldest first so work is taken in the order it
/// was queued rather than in whatever order the filesystem lists.
fn batches(workspace: &Path) -> Vec<(String, PathBuf)> {
    let dir = batch::batches_dir(workspace);
    let Ok(entries) = std::fs::read_dir(&dir) else {
        return Vec::new();
    };
    let mut out: Vec<(String, PathBuf)> = entries
        .flatten()
        .map(|e| e.path())
        .filter(|p| p.extension().and_then(|x| x.to_str()) == Some("ndjson"))
        .filter(|p| !p.to_string_lossy().contains(".ledger."))
        .filter_map(|p| {
            let id = p.file_stem()?.to_string_lossy().into_owned();
            Some((id, p))
        })
        .collect();
    out.sort();
    out
}

pub fn run() -> Result<i32, String> {
    let args: Vec<String> = std::env::args().skip(2).collect();
    if args.first().map(String::as_str) == Some("-h")
        || args.first().map(String::as_str) == Some("--help")
    {
        println!(
            "duckle-runner work - run queued batch items\n\n\
             USAGE:\n    \
             duckle-runner work [--workspace <dir>] [--batch <id>] [--once] [--duckdb <path>]\n\n\
             Runs items queued by a For Each set to \"Queue for workers\". Start it on\n\
             several machines pointed at one workspace and they share the batch: each\n\
             item is claimed with the same lock a pipeline run uses, so no two workers\n\
             take the same one.\n\n\
             --batch <id>   only this batch, instead of every batch in the workspace\n    \
             --once         claim and run a single item, then exit\n\n\
             Items are run AT LEAST once. The ledger is written after an item succeeds,\n\
             so a worker that finishes an item and then dies leaves it looking undone\n\
             and another worker repeats it. Make the child idempotent - an upsert sink\n\
             rather than an append - and a repeat costs time, not correctness.\n\n\
             A failed item stays claimable and is retried on a later pass; the ledger\n\
             keeps the failure so there is something to look at."
        );
        return Ok(0);
    }

    let mut workspace = PathBuf::from(".");
    let mut only_batch: Option<String> = None;
    let mut once = false;
    let mut duckdb: Option<PathBuf> = None;
    let mut it = args.iter();
    while let Some(a) = it.next() {
        match a.as_str() {
            "--workspace" => {
                workspace = PathBuf::from(it.next().ok_or("--workspace needs a value")?)
            }
            "--batch" => only_batch = Some(it.next().ok_or("--batch needs a value")?.clone()),
            "--duckdb" => duckdb = Some(PathBuf::from(it.next().ok_or("--duckdb needs a value")?)),
            "--once" => once = true,
            other => return Err(format!("unknown argument: {other}")),
        }
    }
    let workspace = std::fs::canonicalize(&workspace)
        .map_err(|e| format!("workspace {}: {e}", workspace.display()))?;
    // The engine reads this for sub-pipeline refs, incremental state and logs,
    // exactly as a normal run does.
    std::env::set_var("DUCKLE_WORKSPACE", &workspace);

    let duckdb = crate::resolve_duckdb(duckdb)?;
    let engine = DuckdbEngine::new(duckdb);
    let worker = worker_id();

    let mut ran = 0usize;
    let mut failed = 0usize;
    let mut skipped_claimed = 0usize;

    for (batch_id, path) in batches(&workspace) {
        if let Some(want) = &only_batch {
            if &batch_id != want {
                continue;
            }
        }
        let (items, unreadable) = batch::read(&path).map_err(|e| e.to_string())?;
        if unreadable > 0 {
            eprintln!(
                "duckle-runner: {} line(s) of {} could not be read and were skipped",
                unreadable,
                path.display()
            );
        }
        let done = finished(&workspace, &batch_id);

        for item in &items {
            if done.contains(&item.index) {
                continue;
            }
            // Claim it. A key of batch + index, under its own group so no
            // pipeline run can ever name it.
            let key = format!("{}-{}", batch_id, item.index);
            let claim = match runlock::try_acquire_nested(&workspace, "batch", &key) {
                Some(lock) => lock,
                None => {
                    skipped_claimed += 1;
                    continue;
                }
            };

            let label = item.item.clone().unwrap_or_else(|| item.index.to_string());
            eprintln!("duckle-runner: running {} item {}", batch_id, label);
            let started = chrono::Utc::now();
            let outcome = engine.run_batch_item(&item.child, &item.vars, item.item.as_deref());
            let (status, error) = match outcome {
                Ok(()) => {
                    ran += 1;
                    ("ok", None)
                }
                Err(e) => {
                    failed += 1;
                    eprintln!("duckle-runner: {} item {} failed: {}", batch_id, label, e);
                    ("error", Some(e.to_string()))
                }
            };
            if let Err(e) = record(
                &workspace,
                &batch_id,
                &LedgerLine {
                    v: 1,
                    index: item.index,
                    status: status.into(),
                    at: started.to_rfc3339(),
                    worker: worker.clone(),
                    error,
                },
            ) {
                // The work happened; only the record of it failed. Say so
                // loudly, because the consequence is that another worker will
                // run this item again.
                eprintln!(
                    "duckle-runner: {} item {} ran but could not be recorded ({}); \
                     it will be run again",
                    batch_id, label, e
                );
            }
            drop(claim);
            if once {
                println!("ran {ran}, failed {failed}");
                return Ok(if failed > 0 { 1 } else { 0 });
            }
        }
    }

    if ran == 0 && failed == 0 {
        if skipped_claimed > 0 {
            println!("nothing to do: {skipped_claimed} item(s) are being run by other workers.");
        } else {
            println!("nothing to do: no unfinished items in {}.", workspace.display());
        }
        return Ok(0);
    }
    println!("ran {ran}, failed {failed}");
    if skipped_claimed > 0 {
        println!("{skipped_claimed} item(s) were already claimed by other workers.");
    }
    Ok(if failed > 0 { 1 } else { 0 })
}

/// Something a human can recognise in a ledger: the machine, and the process.
fn worker_id() -> String {
    let host = std::env::var("COMPUTERNAME")
        .or_else(|_| std::env::var("HOSTNAME"))
        .unwrap_or_else(|_| "unknown".into());
    format!("{host}/{}", std::process::id())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn line(index: usize, status: &str) -> LedgerLine {
        LedgerLine {
            v: 1,
            index,
            status: status.into(),
            at: "2026-08-16T10:00:00Z".into(),
            worker: "host/1".into(),
            error: None,
        }
    }

    /// A finished item is not run twice; a failed one is retried.
    ///
    /// Treating a failure as done would let one transient network error consume
    /// an item permanently, which is a worse outcome than repeating it. The
    /// ledger keeps the failure either way so there is something to look at.
    #[test]
    fn only_successes_count_as_finished() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path();
        std::fs::create_dir_all(batch::batches_dir(ws)).unwrap();
        record(ws, "b1", &line(0, "ok")).unwrap();
        record(ws, "b1", &line(1, "error")).unwrap();
        record(ws, "b1", &line(2, "ok")).unwrap();

        let done = finished(ws, "b1");
        assert!(done.contains(&0) && done.contains(&2));
        assert!(!done.contains(&1), "a failed item must stay claimable");
        assert_eq!(done.len(), 2);
    }

    /// A ledger damaged by a crash must not lose the record of what finished.
    #[test]
    fn a_torn_ledger_line_does_not_hide_the_rest() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path();
        std::fs::create_dir_all(batch::batches_dir(ws)).unwrap();
        record(ws, "b1", &line(0, "ok")).unwrap();
        let p = ledger_path(ws, "b1");
        let mut raw = std::fs::read_to_string(&p).unwrap();
        raw.push_str("{\"v\":1,\"index\":9,\"stat");
        std::fs::write(&p, raw).unwrap();
        record(ws, "b1", &line(1, "ok")).unwrap();

        let done = finished(ws, "b1");
        assert!(done.contains(&0) && done.contains(&1), "a torn line hid a finished item");
    }

    /// The ledger of a batch is not mistaken for a batch.
    #[test]
    fn a_ledger_is_not_picked_up_as_work() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path();
        std::fs::create_dir_all(batch::batches_dir(ws)).unwrap();
        std::fs::write(batch::batch_path(ws, "b1"), "").unwrap();
        record(ws, "b1", &line(0, "ok")).unwrap();

        let found = batches(ws);
        assert_eq!(found.len(), 1, "found {found:?}");
        assert_eq!(found[0].0, "b1");
    }

    /// Two workers must not take the same item.
    #[test]
    fn a_claimed_item_is_refused_to_a_second_worker() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path();
        let key = "b1-0";
        let first = runlock::try_acquire_nested(ws, "batch", key).expect("first worker wins");
        assert!(
            runlock::try_acquire_nested(ws, "batch", key).is_none(),
            "a second worker took an item that was already being run"
        );
        drop(first);
        // ...and once that worker is gone - which is what a kill looks like,
        // since the kernel drops the lock - the item is claimable again.
        assert!(runlock::try_acquire_nested(ws, "batch", key).is_some());
    }
}
