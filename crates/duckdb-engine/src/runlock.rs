//! A cross-process lock for one scheduled pipeline run.
//!
//! Duckle has two schedulers: the desktop app runs one inside the Tauri
//! process, and `duckle-runner serve` runs another. Both guard themselves in
//! process - a semaphore in one, a condvar gate in the other - and neither
//! knows the other exists. Point both at the same workspace, which is exactly
//! what happens while promoting a workspace from a laptop to a server, and the
//! same schedule fires twice at the same instant: two runs writing the same
//! sink, and two runs advancing the same `xf.incremental` watermark, which is
//! how a load silently skips rows.
//!
//! The lock is the operating system's, not ours. On Windows the file is opened
//! with no sharing, so a second opener is refused; on Unix the descriptor takes
//! a non-blocking `flock`. Both are released by the kernel when the handle
//! closes, which includes the process being killed or crashing - so a run that
//! dies mid-flight cannot wedge a schedule forever, and there is no stale-lock
//! timeout to tune or get wrong.
//!
//! Acquisition never waits. A schedule that is already running elsewhere is
//! skipped for this tick rather than queued, because the next tick will come
//! around anyway and a queue of identical overdue runs is not useful.

use std::fs::{self, File, OpenOptions};
use std::path::{Path, PathBuf};

/// Held for the duration of a run. Dropping it releases the lock.
#[derive(Debug)]
pub struct RunLock {
    /// Releasing happens when this closes; nothing else is required.
    _file: File,
    key: String,
}

impl RunLock {
    /// Which run this lock covers, for logging.
    pub fn key(&self) -> &str {
        &self.key
    }
}

/// Reduce a caller-supplied name to something safe to use as a filename.
///
/// Keys are pipeline ids that reach us from a file on disk, so they are
/// sanitised rather than trusted: anything that is not plainly a name becomes
/// an underscore, which also flattens any path separator.
fn safe_name(key: &str) -> String {
    key.chars()
        .map(|c| if c.is_ascii_alphanumeric() || c == '-' || c == '_' { c } else { '_' })
        .collect()
}

/// Where a lock for `key` lives. Kept beside the workspace key material, under
/// `.duckle`, so everything the runtime owns is in one place.
fn lock_path(workspace: &Path, key: &str) -> PathBuf {
    workspace.join(".duckle").join("locks").join(format!("{}.lock", safe_name(key)))
}

#[cfg(windows)]
fn open_exclusive(path: &Path) -> std::io::Result<File> {
    use std::os::windows::fs::OpenOptionsExt;
    // share_mode(0) means no other handle may be opened while this one lives,
    // so a second process gets a sharing violation instead of a second lock.
    OpenOptions::new().create(true).write(true).share_mode(0).open(path)
}

#[cfg(unix)]
fn open_exclusive(path: &Path) -> std::io::Result<File> {
    use std::os::unix::io::AsRawFd;
    let file = OpenOptions::new().create(true).write(true).open(path)?;
    // LOCK_NB so this reports "someone else has it" rather than blocking the
    // scheduler tick behind a run that might take hours.
    let rc = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
    if rc != 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(file)
}

/// Take the lock for `key`, or return `None` when another process holds it.
///
/// A workspace that cannot be written - read-only mount, missing directory it
/// cannot create - yields `None` as well. That is deliberate: if the lock
/// cannot be taken, the run does not happen, because the failure mode of
/// running anyway is the duplicate this exists to prevent.
pub fn try_acquire(workspace: &Path, key: &str) -> Option<RunLock> {
    acquire_at(lock_path(workspace, key), key)
}

/// Take a lock that lives one level down, under `group`.
///
/// For locks that guard something other than a pipeline run and must never be
/// blocked by one. A run key cannot reach this path: separators in a key are
/// flattened to underscores, so no pipeline id can name a subdirectory.
pub fn try_acquire_nested(workspace: &Path, group: &str, key: &str) -> Option<RunLock> {
    let path = workspace
        .join(".duckle")
        .join("locks")
        .join(safe_name(group))
        .join(format!("{}.lock", safe_name(key)));
    acquire_at(path, key)
}

fn acquire_at(path: PathBuf, key: &str) -> Option<RunLock> {
    if let Some(dir) = path.parent() {
        if fs::create_dir_all(dir).is_err() {
            return None;
        }
    }
    match open_exclusive(&path) {
        Ok(file) => Some(RunLock { _file: file, key: key.to_string() }),
        // Held elsewhere, or unwritable. Either way this process does not run.
        Err(_) => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_second_acquire_is_refused_while_the_first_is_held() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path();

        let first = try_acquire(ws, "nightly-load").expect("first caller wins");
        assert_eq!(first.key(), "nightly-load");
        assert!(
            try_acquire(ws, "nightly-load").is_none(),
            "two holders of the same run lock at once"
        );

        // A different schedule is unaffected - the lock is per run, not global,
        // so unrelated pipelines still fire on time.
        let other = try_acquire(ws, "hourly-sync").expect("different key is free");
        drop(other);

        // Releasing lets the next caller through, which is what makes the
        // schedule resume on the following tick rather than stalling.
        drop(first);
        assert!(
            try_acquire(ws, "nightly-load").is_some(),
            "lock never became available again"
        );
    }

    /// The whole point of this module is holding across PROCESSES, so a
    /// same-process test proves the wrong thing. This one re-runs the test
    /// binary as a real child, has it take the lock, and checks that the
    /// parent is refused while the child lives. The two talk through marker
    /// files rather than a sleep, so a slow machine makes the test slower
    /// rather than flaky.
    #[test]
    fn a_second_os_process_is_refused_while_the_first_holds_the_lock() {
        use std::process::Command;
        use std::time::{Duration, Instant};

        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path();

        let mut child = Command::new(std::env::current_exe().unwrap())
            .args(["runlock::tests::child_holds_the_lock", "--exact", "--nocapture"])
            .env(CHILD_ENV, ws)
            .spawn()
            .expect("could not re-run the test binary as a child");

        // Wait for the child to actually be holding it. Anything else - the
        // child failing to acquire, or exiting early - is a broken test rather
        // than a passing one, so both are reported instead of timing out.
        let held = ws.join("held");
        let deadline = Instant::now() + Duration::from_secs(30);
        while !held.exists() {
            if ws.join("failed").exists() {
                let _ = child.wait();
                panic!("the child could not take the lock, so nothing was proved");
            }
            if let Ok(Some(status)) = child.try_wait() {
                panic!("the child exited ({status}) before taking the lock");
            }
            if Instant::now() > deadline {
                let _ = child.kill();
                panic!("the child never reported holding the lock");
            }
            std::thread::sleep(Duration::from_millis(20));
        }

        // The measurement: another process holds it, so this one is refused.
        assert!(
            try_acquire(ws, "cross-process").is_none(),
            "two OS processes held the same run lock at once"
        );
        // ...and it is that run that is locked, not the workspace. A second
        // schedule must still be free to fire while the first one runs.
        assert!(
            try_acquire(ws, "some-other-run").is_some(),
            "one running schedule blocked an unrelated one"
        );

        fs::write(ws.join("release"), b"").unwrap();
        let status = child.wait().unwrap();
        assert!(status.success(), "child test failed: {status}");

        // The kernel dropped it when the child's handle closed, so the next
        // tick can run. This also covers the crash case: nothing but process
        // death is needed to release, so there is no stale lock to time out.
        assert!(
            try_acquire(ws, "cross-process").is_some(),
            "the lock survived the process that held it"
        );
    }

    /// Not a test of its own - the child half of the case above, which is why
    /// it does nothing at all unless the parent asked for it by env var.
    #[test]
    fn child_holds_the_lock() {
        use std::time::{Duration, Instant};

        let Ok(ws) = std::env::var(CHILD_ENV) else { return };
        let ws = PathBuf::from(ws);
        let Some(_lock) = try_acquire(&ws, "cross-process") else {
            fs::write(ws.join("failed"), b"").unwrap();
            return;
        };
        fs::write(ws.join("held"), b"").unwrap();
        // Hold until the parent has finished measuring, with a ceiling so a
        // parent that dies cannot leave this process running forever.
        let deadline = Instant::now() + Duration::from_secs(30);
        while !ws.join("release").exists() && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(20));
        }
    }

    /// Set by the parent to put the child half into child mode.
    const CHILD_ENV: &str = "DUCKLE_RUNLOCK_CHILD_WORKSPACE";

    #[test]
    fn keys_that_look_like_paths_cannot_escape_the_lock_directory() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path();
        let lock = try_acquire(ws, "../../etc/passwd").expect("sanitised, not rejected");
        drop(lock);
        let locks = ws.join(".duckle").join("locks");
        let stray = fs::read_dir(&locks).unwrap().filter_map(|e| e.ok()).count();
        assert_eq!(stray, 1, "the lock landed outside {}", locks.display());
    }
}
