//! Plans: several pipelines, in an order somebody chose.
//!
//! A schedule runs one pipeline on a clock. A plan runs many, in steps: everything inside
//! a step goes at once, and the next step waits for the one before it to finish. That is
//! the shape most nightly loads already have, written down instead of being three schedules
//! set a few minutes apart and hoped over.
//!
//! The engine can already do this with `ctl.runpipeline` and `ctl.parallelize`, and a plan
//! could have compiled to those. It does not, for one reason: a compiled plan is a single
//! run, and what an operator wants when a nightly load fails at 3am is to see *which*
//! pipeline failed, in the run history, next to every other run. So a plan drives the
//! ordinary run path once per pipeline, and what it adds is the ordering and a record of
//! the whole attempt.
//!
//! Execution takes the runner as a closure. Deciding what runs next is the part worth
//! testing, and it should be testable without a DuckDB binary, a workspace or a clock.

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::runlock;

pub fn plans_path(workspace: &Path) -> PathBuf {
    workspace.join("plans.json")
}

/// One group of pipelines that may run at the same time.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Step {
    /// What this stage of the plan is for, in the operator's words.
    #[serde(default)]
    pub name: String,
    /// Pipeline files, relative to the workspace. They have no order between them: that
    /// is what putting them in the same step means.
    pub pipelines: Vec<String>,
}

/// Several pipelines, in an order somebody chose.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Plan {
    pub id: String,
    #[serde(default)]
    pub name: String,
    pub steps: Vec<Step>,
    /// Whether a failed pipeline stops the plan.
    ///
    /// Defaults to stopping. A plan is an order, and carrying on past a failed step means
    /// running the next one against data the failed one was supposed to produce.
    #[serde(default = "default_true")]
    pub stop_on_failure: bool,
}

fn default_true() -> bool {
    true
}

impl Plan {
    /// Why this plan could not run, if it could not.
    ///
    /// Checked before saving as well as before running, so a plan that cannot work is
    /// refused at the point somebody wrote it rather than at 3am.
    pub fn problems(&self) -> Vec<String> {
        let mut out = Vec::new();
        if self.id.trim().is_empty() {
            out.push("a plan needs an id".into());
        }
        if self.steps.is_empty() {
            out.push("a plan needs at least one step".into());
        }
        for (i, step) in self.steps.iter().enumerate() {
            if step.pipelines.is_empty() {
                out.push(format!("step {} has no pipelines in it", i + 1));
            }
            for p in &step.pipelines {
                if p.trim().is_empty() {
                    out.push(format!("step {} names an empty pipeline", i + 1));
                }
            }
        }
        // The same pipeline twice in one step would run it twice at once, against itself.
        for (i, step) in self.steps.iter().enumerate() {
            let mut seen = std::collections::HashSet::new();
            for p in &step.pipelines {
                if !seen.insert(p) {
                    out.push(format!("step {} runs {} twice at the same time", i + 1, p));
                }
            }
        }
        out
    }
}

/// The bare pipeline id a plan step names, however the step was spelled.
///
/// One `plans.json` is read by two products that identify a pipeline differently. The
/// console works in workspace-relative files (`pipelines/orders.json`), because that is what
/// its run API takes. The desktop app and the engine work in bare ids (`orders`), because
/// that is what [`crate::context::resolve_workspace`] takes - it builds
/// `<workspace>/pipelines/<id>.json` itself.
///
/// Neither spelling is wrong, but a reader that understands only one turns a plan authored
/// in the other product into a plan that fails on every step. So both readers normalise
/// here, and both writers emit [`step_pipeline_file`]: tolerant readers, consistent writers.
pub fn step_pipeline_id(step: &str) -> &str {
    let s = step.trim();
    let s = s
        .strip_prefix("pipelines/")
        .or_else(|| s.strip_prefix("pipelines\\"))
        .unwrap_or(s);
    s.strip_suffix(".json").unwrap_or(s)
}

/// The workspace-relative file a plan step names, however the step was spelled.
///
/// Derived from [`step_pipeline_id`] so the two can never disagree about what a step means.
pub fn step_pipeline_file(step: &str) -> String {
    format!("pipelines/{}.json", step_pipeline_id(step))
}

/// What became of one pipeline in a plan.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct PipelineOutcome {
    pub pipeline: String,
    /// "ok", "failed", or "skipped" when an earlier step stopped the plan.
    pub status: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// What became of one step.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct StepOutcome {
    pub name: String,
    pub pipelines: Vec<PipelineOutcome>,
}

/// What became of one attempt at a plan.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct PlanRun {
    pub plan_id: String,
    /// "ok" when everything ran, "failed" when anything did.
    pub status: String,
    pub steps: Vec<StepOutcome>,
}

impl PlanRun {
    pub fn failed(&self) -> bool {
        self.status == "failed"
    }
}

/// Run a plan, one pipeline at a time through `run`.
///
/// `run` is whatever actually executes a pipeline and says whether it worked. Passing it in
/// is what lets the ordering be tested on its own: which pipelines are attempted, in what
/// order, and what happens to the rest of the plan when one of them fails, are decisions
/// that should not need a database to check.
///
/// Pipelines within a step are handed over together and in order, which is the contract the
/// caller sees. Whether the caller actually runs them at the same time is its business:
/// this decides what may overlap, not how.
pub fn execute<F>(plan: &Plan, mut run: F) -> PlanRun
where
    F: FnMut(&str) -> Result<(), String>,
{
    let mut out = PlanRun {
        plan_id: plan.id.clone(),
        status: "ok".to_string(),
        steps: Vec::new(),
    };
    let mut stopped = false;

    for step in &plan.steps {
        let mut results = Vec::new();
        for pipeline in &step.pipelines {
            if stopped {
                // Recorded rather than dropped. A plan that reports four pipelines when it
                // has six hides the two nobody looked at.
                results.push(PipelineOutcome {
                    pipeline: pipeline.clone(),
                    status: "skipped".into(),
                    error: None,
                });
                continue;
            }
            match run(pipeline) {
                Ok(()) => results.push(PipelineOutcome {
                    pipeline: pipeline.clone(),
                    status: "ok".into(),
                    error: None,
                }),
                Err(e) => {
                    out.status = "failed".into();
                    results.push(PipelineOutcome {
                        pipeline: pipeline.clone(),
                        status: "failed".into(),
                        error: Some(e),
                    });
                }
            }
        }
        // A failure stops the NEXT step, not the rest of this one: things in one step were
        // declared independent, so the others were always going to run anyway.
        if plan.stop_on_failure && results.iter().any(|r| r.status == "failed") {
            stopped = true;
        }
        out.steps.push(StepOutcome {
            name: step.name.clone(),
            pipelines: results,
        });
    }
    out
}

/// The store as it is on disk right now.
///
/// A missing file is an empty list. A file that exists and will not parse is an error,
/// because treating a corrupt store as empty is how a plan silently stops running.
pub fn load(workspace: &Path) -> Result<Vec<Plan>, String> {
    let p = plans_path(workspace);
    if !p.exists() {
        return Ok(Vec::new());
    }
    let text = std::fs::read_to_string(&p).map_err(|e| format!("read {}: {e}", p.display()))?;
    if text.trim().is_empty() {
        return Ok(Vec::new());
    }
    serde_json::from_str(&text).map_err(|e| format!("parse {}: {e}", p.display()))
}

/// Apply a change to the store and persist it, as one exclusive step.
pub fn update<F>(workspace: &Path, f: F) -> Result<Vec<Plan>, String>
where
    F: FnOnce(&mut Vec<Plan>),
{
    let _guard = runlock::lock_store(workspace, "plans")?;
    let mut list = load(workspace)?;
    f(&mut list);
    let body = serde_json::to_string_pretty(&list).map_err(|e| e.to_string())?;
    let path = plans_path(workspace);
    // Through a temporary file and a rename, so a reader never sees half a store.
    let tmp = path.with_extension("json.tmp");
    std::fs::write(&tmp, body).map_err(|e| format!("write {}: {e}", tmp.display()))?;
    std::fs::rename(&tmp, &path).map_err(|e| format!("install {}: {e}", path.display()))?;
    Ok(list)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn plan(stop: bool) -> Plan {
        Plan {
            id: "nightly".into(),
            name: "Nightly load".into(),
            stop_on_failure: stop,
            steps: vec![
                Step { name: "Extract".into(), pipelines: vec!["orders".into(), "customers".into()] },
                Step { name: "Transform".into(), pipelines: vec!["dbt".into()] },
                Step { name: "Publish".into(), pipelines: vec!["export".into()] },
            ],
        }
    }

    #[test]
    fn a_plan_runs_its_steps_in_order() {
        let mut seen = Vec::new();
        let out = execute(&plan(true), |p| {
            seen.push(p.to_string());
            Ok(())
        });
        assert_eq!(seen, ["orders", "customers", "dbt", "export"]);
        assert_eq!(out.status, "ok");
        assert_eq!(out.steps.len(), 3);
    }

    /// The reason a plan is not three schedules a few minutes apart: a step that did not
    /// work must not let the next one run against data it was supposed to produce.
    #[test]
    fn a_failure_stops_the_steps_after_it() {
        let mut seen = Vec::new();
        let out = execute(&plan(true), |p| {
            seen.push(p.to_string());
            if p == "customers" {
                return Err("connection refused".into());
            }
            Ok(())
        });

        assert_eq!(seen, ["orders", "customers"], "nothing after the failed step should run");
        assert!(out.failed());
        let statuses: Vec<&str> = out
            .steps
            .iter()
            .flat_map(|s| s.pipelines.iter())
            .map(|p| p.status.as_str())
            .collect();
        // Everything is accounted for, including what was never attempted.
        assert_eq!(statuses, ["ok", "failed", "skipped", "skipped"]);
    }

    /// Things in one step were declared independent, so a failure in one does not cancel
    /// its siblings: they were always going to run at the same time as it.
    #[test]
    fn a_failure_does_not_cancel_the_rest_of_its_own_step() {
        let mut seen = Vec::new();
        execute(&plan(true), |p| {
            seen.push(p.to_string());
            if p == "orders" {
                return Err("nope".into());
            }
            Ok(())
        });
        assert_eq!(seen, ["orders", "customers"], "the sibling should still be attempted");
    }

    #[test]
    fn a_plan_can_be_told_to_carry_on() {
        let mut seen = Vec::new();
        let out = execute(&plan(false), |p| {
            seen.push(p.to_string());
            if p == "customers" {
                return Err("nope".into());
            }
            Ok(())
        });
        assert_eq!(seen, ["orders", "customers", "dbt", "export"]);
        assert!(out.failed(), "carrying on does not make a failed plan a good one");
    }

    #[test]
    fn a_plan_that_cannot_work_says_so_before_it_is_saved() {
        let empty = Plan { id: "".into(), name: "".into(), steps: vec![], stop_on_failure: true };
        let problems = empty.problems();
        assert!(problems.iter().any(|p| p.contains("id")));
        assert!(problems.iter().any(|p| p.contains("at least one step")));

        let dupe = Plan {
            id: "x".into(),
            name: String::new(),
            stop_on_failure: true,
            steps: vec![Step { name: "s".into(), pipelines: vec!["a".into(), "a".into()] }],
        };
        assert!(
            dupe.problems().iter().any(|p| p.contains("twice")),
            "the same pipeline twice in one step would run against itself"
        );

        assert!(plan(true).problems().is_empty(), "a sound plan has nothing to report");
    }

    #[test]
    fn the_store_survives_a_round_trip() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path();
        assert!(load(ws).unwrap().is_empty(), "a fresh workspace has no plans");

        update(ws, |list| list.push(plan(true))).unwrap();
        let back = load(ws).unwrap();
        assert_eq!(back.len(), 1);
        assert_eq!(back[0], plan(true));
    }

    /// The console and the desktop app spell a step differently, and one file is read by
    /// both. A reader that understands only its own spelling fails on every step of a plan
    /// the other product wrote.
    #[test]
    fn a_step_means_the_same_pipeline_however_it_was_spelled() {
        for spelling in ["orders", "orders.json", "pipelines/orders.json", "pipelines/orders"] {
            assert_eq!(step_pipeline_id(spelling), "orders", "spelled {spelling}");
            assert_eq!(step_pipeline_file(spelling), "pipelines/orders.json");
        }
        // Written on Windows, where a hand-edited path may carry backslashes.
        assert_eq!(step_pipeline_id("pipelines\\orders.json"), "orders");
        // A name that merely contains the word survives intact: only a leading directory
        // and a trailing extension are structure, the rest is somebody's pipeline name.
        assert_eq!(step_pipeline_id("pipelines-archive"), "pipelines-archive");
        assert_eq!(step_pipeline_id("orders.json.json"), "orders.json");
    }

    /// An older store written before a field existed must still load, or upgrading breaks
    /// every plan somebody already wrote.
    #[test]
    fn a_plan_without_the_newer_fields_still_loads() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path();
        std::fs::write(
            plans_path(ws),
            r#"[{"id":"old","steps":[{"pipelines":["a"]}]}]"#,
        )
        .unwrap();

        let back = load(ws).expect("an older plan should still load");
        assert_eq!(back[0].id, "old");
        assert!(back[0].stop_on_failure, "stopping is the default, not carrying on");
        assert_eq!(back[0].steps[0].name, "");
    }
}
