//! Work handed out as a file, so more than one machine can get through it.
//!
//! `ctl.foreach` normally runs its per-row children inside the process that
//! reached the node. That is bounded by one machine no matter how many rows
//! there are, and it loses everything if that machine dies half way.
//!
//! With `dispatch: "queue"` the rows are written here instead, one JSON object
//! per line, and the node returns. The file is then the work: any number of
//! `duckle-runner` processes can read it, claim an item apiece through the
//! existing run lock, and run it. Nothing needs a queue server, a database or a
//! network service - a batch is a file in the workspace, which is the same
//! thing every other piece of Duckle state already is.
//!
//! # Why NDJSON, and why a version on every line
//!
//! One object per line means a worker can stream a batch of 400,000 items
//! without holding it in memory, and a half-written last line is discardable
//! rather than fatal - the failure mode of a single JSON array. `v` is on every
//! line rather than in a header because a worker may start reading at any
//! offset, and a line that cannot say what it is cannot be safely skipped.

use std::io::Write;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::EngineError;

/// One unit of work: one row of the driving query, and the child to run for it.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkItem {
    /// Format version of THIS line. See the module note.
    pub v: u32,
    /// The batch this line belongs to, repeated per line so a line stays
    /// meaningful when it is copied out of the file into a log or a message.
    pub batch: String,
    /// Position in the driving query. Ordering information, never identity:
    /// see `item`.
    pub index: usize,
    /// What this item IS, from `ctl.foreach`'s item key column. This is what
    /// makes the run name and therefore the watermark, so it is the field that
    /// decides whether two items share state. Absent when no item key was set,
    /// in which case every item of the batch is the same named run.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub item: Option<String>,
    /// The child pipeline reference, exactly as authored on the node.
    pub child: String,
    /// The `${ITER_*}` substitutions for this row.
    pub vars: std::collections::BTreeMap<String, String>,
}

pub fn batches_dir(workspace: &Path) -> PathBuf {
    workspace.join("batches")
}

pub fn batch_path(workspace: &Path, batch_id: &str) -> PathBuf {
    batches_dir(workspace).join(format!("{batch_id}.ndjson"))
}

/// A batch id that is unique per dispatch and still says what it came from.
///
/// The node id leads so a directory listing groups a node's batches together;
/// the timestamp makes two dispatches of the same node distinct. Milliseconds
/// because a fast pipeline can dispatch the same node twice in one second.
pub fn new_batch_id(node_id: &str, at: chrono::DateTime<chrono::Utc>) -> String {
    let node: String = node_id
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() || c == '-' || c == '_' { c } else { '_' })
        .collect();
    format!("{}-{}", node, at.format("%Y%m%dT%H%M%S%3f"))
}

/// Write a batch, and return where it went.
///
/// Written to a temp name and renamed, like every other store here: a worker
/// scanning the folder must never see a batch that is still being written and
/// conclude the work is smaller than it is.
pub fn write(workspace: &Path, batch_id: &str, items: &[WorkItem]) -> Result<PathBuf, EngineError> {
    let dir = batches_dir(workspace);
    std::fs::create_dir_all(&dir).map_err(|e| {
        EngineError::Config(format!("batch: cannot create {}: {}", dir.display(), e))
    })?;
    let final_path = batch_path(workspace, batch_id);
    let tmp = dir.join(format!("{batch_id}.{}.ndjson.tmp", std::process::id()));

    let mut out = String::new();
    for item in items {
        let line = serde_json::to_string(item)
            .map_err(|e| EngineError::Config(format!("batch: encode item: {e}")))?;
        out.push_str(&line);
        out.push('\n');
    }
    {
        let mut f = std::fs::File::create(&tmp).map_err(|e| {
            EngineError::Config(format!("batch: cannot write {}: {}", tmp.display(), e))
        })?;
        f.write_all(out.as_bytes())
            .map_err(|e| EngineError::Config(format!("batch: cannot write {}: {}", tmp.display(), e)))?;
    }
    if let Err(e) = std::fs::rename(&tmp, &final_path) {
        let _ = std::fs::remove_file(&tmp);
        return Err(EngineError::Config(format!(
            "batch: cannot place {}: {}",
            final_path.display(),
            e
        )));
    }
    Ok(final_path)
}

/// Read a batch back.
///
/// A line that will not parse is skipped and counted rather than failing the
/// whole read: a batch is appended to by a crashing process's last write, and
/// losing 400,000 good items to one torn line would be the worse outcome. A
/// line whose `v` this build does not know is skipped the same way, because
/// guessing at a format from the future is how corruption gets executed.
pub fn read(path: &Path) -> Result<(Vec<WorkItem>, usize), EngineError> {
    let text = std::fs::read_to_string(path).map_err(|e| {
        EngineError::Config(format!("batch: cannot read {}: {}", path.display(), e))
    })?;
    let mut items = Vec::new();
    let mut skipped = 0usize;
    for line in text.lines() {
        if line.trim().is_empty() {
            continue;
        }
        match serde_json::from_str::<WorkItem>(line) {
            Ok(item) if item.v == 1 => items.push(item),
            _ => skipped += 1,
        }
    }
    Ok((items, skipped))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn item(i: usize, name: &str) -> WorkItem {
        let mut vars = std::collections::BTreeMap::new();
        vars.insert("ITER_INDEX".to_string(), i.to_string());
        vars.insert("ITER_ITEM_TABLE_NAME".to_string(), name.to_string());
        WorkItem {
            v: 1,
            batch: "n1-20260816T101112123".into(),
            index: i,
            item: Some(name.into()),
            child: "pipelines/sync-one-table.json".into(),
            vars,
        }
    }

    #[test]
    fn a_batch_round_trips_one_line_per_item() {
        let tmp = tempfile::tempdir().unwrap();
        let items = vec![item(0, "orders"), item(1, "customers")];
        let path = write(tmp.path(), "n1-20260816T101112123", &items).unwrap();

        // One line per item, so a worker can stream rather than load.
        let raw = std::fs::read_to_string(&path).unwrap();
        assert_eq!(raw.lines().count(), 2);
        assert!(raw.lines().all(|l| l.contains("\"v\":1")), "every line must carry its version");

        let (back, skipped) = read(&path).unwrap();
        assert_eq!(skipped, 0);
        assert_eq!(back.len(), 2);
        assert_eq!(back[1].item.as_deref(), Some("customers"));
        assert_eq!(back[1].vars["ITER_ITEM_TABLE_NAME"], "customers");
    }

    /// A torn last line must cost one item, not the whole batch.
    #[test]
    fn a_damaged_line_is_skipped_and_counted() {
        let tmp = tempfile::tempdir().unwrap();
        let path = write(tmp.path(), "b", &[item(0, "orders"), item(1, "customers")]).unwrap();
        let mut raw = std::fs::read_to_string(&path).unwrap();
        raw.push_str("{\"v\":1,\"batch\":\"b\",\"index\":2,\"chi");
        std::fs::write(&path, raw).unwrap();

        let (back, skipped) = read(&path).unwrap();
        assert_eq!(back.len(), 2, "the intact items must survive a torn line");
        assert_eq!(skipped, 1);
    }

    /// A line from a future format is skipped, not guessed at.
    #[test]
    fn an_unknown_version_is_not_executed() {
        let tmp = tempfile::tempdir().unwrap();
        let path = write(tmp.path(), "b", &[item(0, "orders")]).unwrap();
        let mut raw = std::fs::read_to_string(&path).unwrap();
        let mut future = item(1, "customers");
        future.v = 99;
        raw.push_str(&serde_json::to_string(&future).unwrap());
        raw.push('\n');
        std::fs::write(&path, raw).unwrap();

        let (back, skipped) = read(&path).unwrap();
        assert_eq!(back.len(), 1, "a v99 line must not be run by a v1 worker");
        assert_eq!(skipped, 1);
    }

    /// Two dispatches of one node are two batches.
    #[test]
    fn a_batch_id_is_unique_per_dispatch_and_names_its_node() {
        use chrono::TimeZone;
        let t1 = chrono::Utc.with_ymd_and_hms(2026, 8, 16, 10, 11, 12).unwrap();
        let a = new_batch_id("foreach-1", t1);
        let b = new_batch_id("foreach-1", t1 + chrono::Duration::milliseconds(7));
        assert_ne!(a, b, "two dispatches in the same second collided");
        assert!(a.starts_with("foreach-1-"), "{a}");
        // A node id that would escape the folder cannot.
        assert!(!new_batch_id("../../etc/passwd", t1).contains('/'));
    }

    /// A worker must never see a half-written batch.
    #[test]
    fn a_batch_appears_whole_or_not_at_all() {
        let tmp = tempfile::tempdir().unwrap();
        let items: Vec<WorkItem> = (0..500).map(|i| item(i, &format!("t{i}"))).collect();
        let path = write(tmp.path(), "big", &items).unwrap();
        // Nothing is left behind mid-write, and the only file present is complete.
        let stray: Vec<_> = std::fs::read_dir(batches_dir(tmp.path()))
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_name().to_string_lossy().contains("tmp"))
            .collect();
        assert!(stray.is_empty(), "a temp batch file was left in the folder");
        assert_eq!(read(&path).unwrap().0.len(), 500);
    }
}
