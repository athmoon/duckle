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

/// A stable 64-bit digest of a key.
///
/// FNV-1a, written out rather than taken from the standard library: the output
/// of `DefaultHasher` is explicitly not guaranteed across Rust releases, and
/// two Duckle builds sharing one workspace - a desktop app on one version and
/// a runner on another, which is the normal state mid-upgrade - must derive the
/// same lock filename or neither of them is locking anything.
fn digest(key: &str) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in key.as_bytes() {
        h ^= *b as u64;
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    h
}

/// Reduce a caller-supplied name to something safe to use as a filename.
///
/// Keys are pipeline ids that reach us from a file on disk, so they are
/// sanitised rather than trusted: anything that is not plainly a name becomes
/// an underscore, which also flattens any path separator.
///
/// The digest is what makes the result unique, and it is not decoration.
/// Sanitising alone is many-to-one: `sales.daily` and `sales_daily` both became
/// `sales_daily`, and every non-ASCII name of the same length became the same
/// run of underscores, so two unrelated pipelines shared one lock. The
/// symptom was the worst kind - a scheduled run silently skipped, with a log
/// line blaming a different pipeline that happened to be running.
fn safe_name(key: &str) -> String {
    // Enough of the original to recognise in a directory listing; the digest
    // carries the identity.
    let stem: String = key
        .chars()
        .take(48)
        .map(|c| if c.is_ascii_alphanumeric() || c == '-' || c == '_' { c } else { '_' })
        .collect();
    format!("{stem}-{:016x}", digest(key))
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
/// Hold an exclusive lock on a named store, waiting briefly for whoever has it.
///
/// Unlike a run lock, where a clash means "someone else is already doing this,
/// so skip", a clash here means "someone else is mid-write, so wait": the write
/// takes a few milliseconds and the caller is holding a change that must not be
/// dropped. The ceiling turns a wedged holder into a reported error rather than
/// a hung UI; the kernel releases the lock on process death, so reaching it at
/// all should mean a genuinely stuck writer.
///
/// Every store that two processes share goes through this - schedules.json and
/// alert-state.json today. A store that reads, modifies and writes without it
/// loses whichever writer finished first.
pub fn lock_store(workspace: &Path, name: &str) -> Result<RunLock, String> {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        // A nested key: pipeline ids flatten their separators to underscores,
        // so no pipeline can ever name this lock and stall a save by running.
        if let Some(lock) = try_acquire_nested(workspace, "store", name) {
            return Ok(lock);
        }
        if std::time::Instant::now() >= deadline {
            return Err(format!("Timed out waiting to write {name}"));
        }
        std::thread::sleep(std::time::Duration::from_millis(25));
    }
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

    /// Two different pipelines must never share one lock.
    ///
    /// Sanitising a name to a filename is many-to-one, and the collision was
    /// not exotic: `sales.daily` and `sales_daily` both became `sales_daily`,
    /// and any two non-ASCII names of equal length became the same run of
    /// underscores. The result was a scheduled run silently skipped, logged as
    /// a clash with a different pipeline that was merely running at the time.
    #[test]
    fn two_pipelines_whose_names_sanitise_alike_do_not_share_a_lock() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path();

        // The property, stated on the names themselves.
        assert_ne!(safe_name("sales.daily"), safe_name("sales_daily"));
        assert_ne!(safe_name("ventes/quotidien"), safe_name("ventes.quotidien"));
        // Non-ASCII collapsed hardest: every name of a given length was one
        // key. These share not a single ASCII character.
        assert_ne!(safe_name("日次"), safe_name("週次"));
        // And the same name is still the same lock, which is the whole point.
        assert_eq!(safe_name("sales.daily"), safe_name("sales.daily"));

        // The behaviour that follows from it: holding one leaves the other
        // free, so a schedule is not skipped because of an unrelated run.
        let held = try_acquire(ws, "sales.daily").expect("first pipeline could not lock");
        let other = try_acquire(ws, "sales_daily")
            .expect("an unrelated pipeline was blocked by a name collision");
        // ...while a genuine second attempt at the same pipeline is refused.
        assert!(try_acquire(ws, "sales.daily").is_none(), "the same pipeline locked twice");
        drop(held);
        drop(other);
    }

    /// The lock filename cannot drift between builds sharing a workspace.
    #[test]
    fn the_digest_is_fixed_rather_than_whatever_the_toolchain_hashes_to() {
        // Pinned values. DefaultHasher would satisfy the uniqueness test above
        // and still break the cross-process guarantee, because its output is
        // not stable across Rust releases - a desktop app and a runner built
        // on different toolchains would lock different files and neither would
        // be locking anything. These are FNV-1a, which is fixed forever.
        assert_eq!(digest(""), 0xcbf2_9ce4_8422_2325);
        assert_eq!(digest("a"), 0xaf63_dc4c_8601_ec8c);
        assert_eq!(digest("foobar"), 0x85944171f73967e8);
    }
}
