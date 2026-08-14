//! An append-only record of who did what through the management console.
//!
//! Run history already says a pipeline ran. It does not say who caused it, from
//! where, or whether someone tried and was refused, and those are the three
//! questions asked after an incident. This is the answer to them, kept as one
//! NDJSON line per event beside the run logs, so the same collector that ships
//! those ships this.
//!
//! Refusals are recorded as well as successes. A log that only holds what
//! worked cannot show someone probing an endpoint they have no role for, which
//! is the pattern worth seeing.

use std::io::Write;
use std::path::{Path, PathBuf};

use serde_json::json;

use crate::console_auth::{Identity, Role};

pub fn audit_path(workspace: &Path) -> PathBuf {
    workspace.join("logs").join("audit.ndjson")
}

/// How an attempt ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    Allowed,
    /// Authenticated, but the role was not enough.
    Denied,
    /// No usable credential at all.
    Unauthenticated,
}

impl Outcome {
    fn as_str(self) -> &'static str {
        match self {
            Outcome::Allowed => "allowed",
            Outcome::Denied => "denied",
            Outcome::Unauthenticated => "unauthenticated",
        }
    }
}

/// Append one event. Never fails the request it describes.
///
/// A console that refused to serve because its audit file was unwritable would
/// turn a full disk into an outage, so a write failure goes to stderr and the
/// request continues. That is a deliberate trade and the reason the file is not
/// the only record: runs are still logged in run history.
pub fn record(
    workspace: &Path,
    who: Option<&Identity>,
    action: &str,
    target: &str,
    outcome: Outcome,
) {
    let entry = json!({
        "at": chrono::Utc::now().to_rfc3339(),
        // An unauthenticated caller has no name, and inventing one would make
        // the log read as though somebody known did this.
        "actor": who.map(|w| w.label.as_str()).unwrap_or("-"),
        "role": who.map(|w| w.role.as_str()).unwrap_or("-"),
        "action": action,
        "target": target,
        "outcome": outcome.as_str(),
    });
    if let Err(e) = append(workspace, &entry.to_string()) {
        eprintln!("duckle-runner: could not write the audit log: {e}");
    }
}

fn append(workspace: &Path, line: &str) -> Result<(), String> {
    let p = audit_path(workspace);
    if let Some(dir) = p.parent() {
        std::fs::create_dir_all(dir).map_err(|e| e.to_string())?;
    }
    // One write of one short line in append mode, so concurrent writers
    // interleave whole lines rather than fragments of them.
    let mut f = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&p)
        .map_err(|e| format!("{}: {e}", p.display()))?;
    f.write_all(format!("{line}\n").as_bytes()).map_err(|e| e.to_string())
}

/// The role a route needs, and the action name recorded for it.
///
/// Kept as one table so a new route cannot quietly arrive without a decision
/// about who may call it: the dispatcher asks this for every request, and the
/// fallback is admin rather than public.
pub fn requirement(method: &str, path: &str) -> (Role, &'static str) {
    match (method, path) {
        // Reading the dashboard and its data.
        ("GET", "/") | ("GET", "/index.html") => (Role::Viewer, "console.open"),
        ("GET", "/api/summary") => (Role::Viewer, "summary.read"),
        ("GET", "/api/pipelines") => (Role::Viewer, "pipelines.list"),
        ("GET", "/api/pipeline") => (Role::Viewer, "pipeline.read"),
        ("GET", "/api/runs") => (Role::Viewer, "runs.read"),
        ("GET", "/api/log") => (Role::Viewer, "log.read"),
        ("GET", "/api/schedules") => (Role::Viewer, "schedules.read"),
        ("GET", "/api/params") => (Role::Viewer, "params.read"),
        ("GET", "/api/whoami") => (Role::Viewer, "session.whoami"),
        // Ending your own session is not a privilege. Leaving this to the
        // admin-only fallback below meant a viewer could sign in and then not
        // sign out, which the live run found before anyone else would have.
        ("DELETE", "/api/session") => (Role::Viewer, "session.sign_out"),

        // Causing work to happen, or changing when it happens.
        ("POST", "/api/run") => (Role::Operator, "pipeline.run"),
        ("POST", "/api/schedules") => (Role::Operator, "schedule.write"),

        // Anything unrecognised needs the highest role. A route added later
        // without a line here is locked down rather than left open.
        _ => (Role::Admin, "unknown"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn identity(label: &str, role: Role) -> Identity {
        Identity { label: label.into(), role }
    }

    #[test]
    fn events_are_appended_one_line_each_and_include_refusals() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path();

        let ops = identity("ops", Role::Operator);
        record(ws, Some(&ops), "pipeline.run", "nightly-load", Outcome::Allowed);
        record(ws, Some(&identity("reporting", Role::Viewer)), "pipeline.run", "nightly-load", Outcome::Denied);
        record(ws, None, "pipeline.run", "nightly-load", Outcome::Unauthenticated);

        let text = std::fs::read_to_string(audit_path(ws)).unwrap();
        let lines: Vec<&str> = text.lines().collect();
        assert_eq!(lines.len(), 3, "one line per event");

        let parsed: Vec<serde_json::Value> =
            lines.iter().map(|l| serde_json::from_str(l).expect("each line is JSON")).collect();
        assert_eq!(parsed[0]["actor"], "ops");
        assert_eq!(parsed[0]["outcome"], "allowed");
        // The refusal is the entry that matters: a log of successes alone
        // cannot show someone reaching for something they do not have.
        assert_eq!(parsed[1]["actor"], "reporting");
        assert_eq!(parsed[1]["outcome"], "denied");
        assert_eq!(parsed[2]["actor"], "-", "an unknown caller must not be given a name");
        assert_eq!(parsed[2]["outcome"], "unauthenticated");
        assert!(parsed[0]["at"].as_str().unwrap().contains('T'), "timestamp is not rfc3339");
    }

    #[test]
    fn reading_needs_less_than_running_and_unknown_routes_need_the_most() {
        assert_eq!(requirement("GET", "/api/runs").0, Role::Viewer);
        assert_eq!(requirement("POST", "/api/run").0, Role::Operator);
        // Signing yourself out is not a privileged act. Anyone who could sign
        // in must be able to sign out, whatever their role.
        assert_eq!(requirement("DELETE", "/api/session").0, Role::Viewer);
        assert_eq!(requirement("DELETE", "/api/session").1, "session.sign_out");
        assert_eq!(requirement("POST", "/api/schedules").0, Role::Operator);
        // The important one: a route nobody thought about is admin-only, so
        // adding an endpoint cannot accidentally publish it.
        assert_eq!(requirement("POST", "/api/some-future-thing").0, Role::Admin);
        assert_eq!(requirement("DELETE", "/api/pipelines").0, Role::Admin);
    }

    #[test]
    fn a_viewer_cannot_run_a_pipeline_and_an_operator_can() {
        let (needed, action) = requirement("POST", "/api/run");
        assert_eq!(action, "pipeline.run");
        assert!(!Role::Viewer.allows(needed));
        assert!(Role::Operator.allows(needed));
        assert!(Role::Admin.allows(needed));
    }
}
