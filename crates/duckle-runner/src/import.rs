//! `duckle-runner import ...` - convert a directory of legacy job files.
//!
//! The desktop app already reads one job file at a time, which is the right shape for
//! trying Duckle and the wrong shape for leaving another tool: nobody has one job, they
//! have several hundred in a repository, and converting them through a file dialog is not
//! a migration plan. This walks the tree instead and reports the whole corpus at once.
//!
//! The report matters more than the conversion. Anyone deciding whether to move needs two
//! numbers before they start - how many jobs come across clean, and which handful of
//! components account for everything that does not - and those numbers are also the
//! roadmap: the unmapped tally, sorted, is exactly the list of components worth adding
//! next. Coverage is the head of the distribution, so a corpus usually converts far better
//! than the raw component count suggests.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use duckle_duckdb_engine::talend::{self, Warning};

/// What became of one job file.
#[derive(Debug)]
pub struct JobOutcome {
    pub source: PathBuf,
    pub name: String,
    /// Where the pipeline was written. `None` when the file could not be read at all.
    pub written: Option<PathBuf>,
    pub error: Option<String>,
    /// Why this file was not a job, when it was not one. The extension is shared by
    /// routines, contexts and metadata; those parse cleanly and contain no pipeline.
    pub skipped: Option<String>,
    /// Everything a human still has to resolve, already rendered for display.
    pub warnings: Vec<String>,
    /// Components with no Duckle equivalent, and how many of each this job used.
    pub unmapped: BTreeMap<String, usize>,
}

impl JobOutcome {
    /// A job converted with nothing left to resolve. A skipped file is not a clean
    /// conversion: counting empty pipelines as successes is how a migration report ends
    /// up flattering itself.
    pub fn is_clean(&self) -> bool {
        self.error.is_none() && self.skipped.is_none() && self.warnings.is_empty()
    }
}

/// What became of the corpus.
#[derive(Debug, Default)]
pub struct BulkReport {
    pub jobs: Vec<JobOutcome>,
}

impl BulkReport {
    pub fn clean(&self) -> usize {
        self.jobs.iter().filter(|j| j.is_clean()).count()
    }

    pub fn needs_attention(&self) -> usize {
        self.jobs
            .iter()
            .filter(|j| j.error.is_none() && j.skipped.is_none() && !j.warnings.is_empty())
            .count()
    }

    pub fn failed(&self) -> usize {
        self.jobs.iter().filter(|j| j.error.is_some()).count()
    }

    /// Files that parsed but hold no pipeline. Reported so the total still adds up.
    pub fn skipped(&self) -> usize {
        self.jobs
            .iter()
            .filter(|j| j.error.is_none() && j.skipped.is_some())
            .count()
    }

    /// The machine-readable form, for a migration script or a CI job.
    pub fn to_json(&self) -> serde_json::Value {
        serde_json::json!({
            "jobs": self.jobs.iter().map(|j| serde_json::json!({
                "source": j.source.to_string_lossy(),
                "name": j.name,
                "written": j.written.as_ref().map(|p| p.to_string_lossy()),
                "error": j.error,
                "skipped": j.skipped,
                "warnings": j.warnings,
                "unmapped": j.unmapped,
            })).collect::<Vec<_>>(),
            "summary": {
                "total": self.jobs.len(),
                "clean": self.clean(),
                "needsAttention": self.needs_attention(),
                "failed": self.failed(),
                "skipped": self.skipped(),
                "unmapped": self.unmapped_totals(),
            }
        })
    }

    /// Every unmapped component across the corpus, with a total count.
    ///
    /// This is the whole point of converting in bulk: one job tells you nothing about
    /// whether a migration is viable, and a hundred tell you exactly which components to
    /// build next.
    pub fn unmapped_totals(&self) -> BTreeMap<String, usize> {
        let mut totals = BTreeMap::new();
        for job in &self.jobs {
            for (component, n) in &job.unmapped {
                *totals.entry(component.clone()).or_insert(0) += n;
            }
        }
        totals
    }
}

pub fn run() -> Result<i32, String> {
    let args: Vec<String> = std::env::args().skip(2).collect();
    let first = args.first().map(String::as_str).unwrap_or("");
    if first.is_empty() || first == "-h" || first == "--help" {
        println!(
            "duckle-runner import - convert a folder of legacy job files\n\n\
             USAGE:\n    \
             duckle-runner import <dir> [--out <dir>] [--json] [--strict]\n\n\
             Walks <dir> for job files, converts each one, and mirrors the folder\n\
             layout under --out (default ./imported). The layout is kept rather than\n\
             flattened so two jobs that share a name cannot overwrite each other.\n\n\
             Nothing is ever dropped in silence. A component with no equivalent is\n\
             imported as a placeholder and reported, an encrypted password becomes an\n\
             ${{ENV:...}} placeholder, and a file that will not parse is listed rather\n\
             than ending the run.\n\n\
             The tally at the end is the number to decide on: it says how many jobs\n\
             came across clean and which components account for everything that did\n\
             not. Sorted by count, that list is also the shortest path to a complete\n\
             migration.\n\n\
             --strict exits 1 when any job needs attention, for a CI gate.\n"
        );
        return Ok(0);
    }

    let src = PathBuf::from(first);
    let mut out = PathBuf::from("imported");
    let mut json = false;
    let mut strict = false;
    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--out" => {
                i += 1;
                out = PathBuf::from(args.get(i).ok_or("--out needs a directory")?);
            }
            "--json" => json = true,
            "--strict" => strict = true,
            other => return Err(format!("unknown option {other}")),
        }
        i += 1;
    }
    if !src.is_dir() {
        return Err(format!("{} is not a directory", src.display()));
    }

    let report = import_tree(&src, &out)?;
    if json {
        println!("{}", serde_json::to_string_pretty(&report.to_json()).unwrap());
    } else {
        print_report(&report, &out);
    }
    Ok(if strict && (report.needs_attention() > 0 || report.failed() > 0) { 1 } else { 0 })
}

fn print_report(report: &BulkReport, out: &Path) {
    for job in &report.jobs {
        match (&job.error, &job.skipped) {
            (Some(e), _) => println!("  FAILED  {}  {}", job.name, e),
            (None, Some(why)) => println!("  skip    {}  {}", job.name, why),
            (None, None) if job.warnings.is_empty() => println!("  ok      {}", job.name),
            (None, None) => {
                println!("  review  {}  ({} to resolve)", job.name, job.warnings.len())
            }
        }
        for w in &job.warnings {
            println!("            {w}");
        }
    }
    let jobs = report.clean() + report.needs_attention() + report.failed();
    println!(
        "\n{} file(s): {} held a job, {} were not jobs\n\
         {} job(s): {} clean, {} need review, {} failed -> {}",
        report.jobs.len(),
        jobs,
        report.skipped(),
        jobs,
        report.clean(),
        report.needs_attention(),
        report.failed(),
        out.display()
    );

    let totals = report.unmapped_totals();
    if !totals.is_empty() {
        // Sorted by count: the shortest path to converting the rest of the corpus.
        let mut ranked: Vec<_> = totals.iter().collect();
        ranked.sort_by(|a, b| b.1.cmp(a.1).then(a.0.cmp(b.0)));
        println!("\nno equivalent yet, most used first:");
        for (component, n) in ranked {
            println!("  {n:>4}  {component}");
        }
    }
}

/// Convert every job file under `src`, mirroring the tree under `out`.
///
/// The source layout is preserved rather than flattened, because two jobs in different
/// folders routinely share a name and flattening would have one silently overwrite the
/// other - a migration tool that loses a job is worse than one that refuses it.
pub fn import_tree(src: &Path, out: &Path) -> Result<BulkReport, String> {
    let mut files = Vec::new();
    collect_jobs(src, &mut files)?;
    files.sort();

    let mut report = BulkReport::default();
    for file in files {
        let rel = file.strip_prefix(src).unwrap_or(&file).to_path_buf();
        let name = file
            .file_stem()
            .map(|s| s.to_string_lossy().to_string())
            .unwrap_or_else(|| "job".to_string());
        report.jobs.push(convert_one(&file, &rel, &name, out));
    }
    Ok(report)
}

/// Convert one file. A failure here is recorded, never propagated: one unreadable file in
/// a corpus of hundreds must not end the migration.
fn convert_one(file: &Path, rel: &Path, name: &str, out: &Path) -> JobOutcome {
    let mut outcome = JobOutcome {
        source: file.to_path_buf(),
        name: name.to_string(),
        written: None,
        error: None,
        skipped: None,
        warnings: Vec::new(),
        unmapped: BTreeMap::new(),
    };

    let xml = match std::fs::read_to_string(file) {
        Ok(x) => x,
        Err(e) => {
            outcome.error = Some(format!("read: {e}"));
            return outcome;
        }
    };
    // A job declares itself a process. Routines are Java source that merely shares the
    // extension, and their javadoc HTML makes the XML reader fail; reporting those as
    // failed jobs makes a migration look worse than it is and sends someone chasing
    // files that were never jobs. Checking first keeps the other half of the rule intact:
    // a file that says it is a job and then will not parse is still a failure.
    if !xml.contains("ProcessType") {
        outcome.skipped = Some("not a job (no process declaration)".into());
        return outcome;
    }

    let import = match talend::import_item(&xml, name) {
        Ok(i) => i,
        Err(e) => {
            outcome.error = Some(e);
            return outcome;
        }
    };

    // A routine, a context or a metadata item parses perfectly and yields no nodes.
    // Writing an empty pipeline for it would put a file in the output that runs and does
    // nothing, and counting it as converted would inflate the only number anyone reads.
    if import.nodes.is_empty() {
        outcome.skipped = Some("holds no job (routine, context or metadata)".into());
        return outcome;
    }

    for w in &import.warnings {
        outcome.warnings.push(w.to_string());
        if let Warning::UnmappedComponent { component, .. } = w {
            *outcome.unmapped.entry(component.clone()).or_insert(0) += 1;
        }
    }

    let target = out.join(rel).with_extension("json");
    if let Some(parent) = target.parent() {
        if let Err(e) = std::fs::create_dir_all(parent) {
            outcome.error = Some(format!("create {}: {e}", parent.display()));
            return outcome;
        }
    }
    let body = match serde_json::to_string_pretty(&import.to_pipeline_json()) {
        Ok(b) => b,
        Err(e) => {
            outcome.error = Some(format!("serialise: {e}"));
            return outcome;
        }
    };
    match std::fs::write(&target, body) {
        Ok(()) => outcome.written = Some(target),
        Err(e) => outcome.error = Some(format!("write {}: {e}", target.display())),
    }
    outcome
}

/// Walk for job files. Anything that is not one is left alone, so pointing this at a
/// checkout rather than a curated folder is safe.
fn collect_jobs(dir: &Path, into: &mut Vec<PathBuf>) -> Result<(), String> {
    let entries = std::fs::read_dir(dir).map_err(|e| format!("read {}: {e}", dir.display()))?;
    for entry in entries {
        let entry = entry.map_err(|e| format!("read {}: {e}", dir.display()))?;
        let path = entry.path();
        if path.is_dir() {
            collect_jobs(&path, into)?;
        } else if path.extension().and_then(|e| e.to_str()) == Some("item") {
            into.push(path);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A job using one component that maps and one that does not, so a single fixture
    /// exercises both halves of the report.
    fn job_xml(unmapped_component: &str) -> String {
        format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<talendfile:ProcessType xmlns:talendfile="platform:/resource/org.talend.model/model/TalendFile.xsd">
  <node componentName="tFileInputDelimited" posX="100" posY="50">
    <elementParameter field="TEXT" name="UNIQUE_NAME" value="in_1"/>
    <elementParameter field="TEXT" name="FILENAME" value="&quot;/data/in.csv&quot;"/>
  </node>
  <node componentName="{unmapped_component}" posX="300" posY="50">
    <elementParameter field="TEXT" name="UNIQUE_NAME" value="odd_1"/>
  </node>
  <connection connectorName="FLOW" source="in_1" target="odd_1"/>
</talendfile:ProcessType>
"#
        )
    }

    fn write(dir: &Path, rel: &str, body: &str) -> PathBuf {
        let p = dir.join(rel);
        std::fs::create_dir_all(p.parent().unwrap()).unwrap();
        std::fs::write(&p, body).unwrap();
        p
    }

    #[test]
    fn every_job_in_the_tree_is_converted() {
        let src = tempfile::tempdir().unwrap();
        let out = tempfile::tempdir().unwrap();
        write(src.path(), "a.item", &job_xml("tSomethingOdd"));
        write(src.path(), "nested/b.item", &job_xml("tSomethingOdd"));

        let report = import_tree(src.path(), out.path()).unwrap();

        assert_eq!(report.jobs.len(), 2, "both jobs should be found");
        assert!(out.path().join("a.json").exists(), "top-level job written");
        assert!(
            out.path().join("nested/b.json").exists(),
            "nested job keeps its folder"
        );
    }

    #[test]
    fn two_jobs_with_the_same_name_do_not_overwrite_each_other() {
        // The reason the tree is mirrored rather than flattened. Losing a job silently is
        // the worst thing a migration tool can do.
        let src = tempfile::tempdir().unwrap();
        let out = tempfile::tempdir().unwrap();
        write(src.path(), "sales/load.item", &job_xml("tSomethingOdd"));
        write(src.path(), "finance/load.item", &job_xml("tSomethingOdd"));

        let report = import_tree(src.path(), out.path()).unwrap();

        assert_eq!(report.jobs.len(), 2);
        assert!(out.path().join("sales/load.json").exists());
        assert!(out.path().join("finance/load.json").exists());
    }

    #[test]
    fn the_unmapped_tally_adds_up_across_the_corpus() {
        // The number someone actually decides on: not "does this job convert" but "which
        // handful of components stand between me and moving".
        let src = tempfile::tempdir().unwrap();
        let out = tempfile::tempdir().unwrap();
        write(src.path(), "a.item", &job_xml("tSomethingOdd"));
        write(src.path(), "b.item", &job_xml("tSomethingOdd"));
        write(src.path(), "c.item", &job_xml("tOtherOdd"));

        let report = import_tree(src.path(), out.path()).unwrap();
        let totals = report.unmapped_totals();

        assert_eq!(
            totals.get("tSomethingOdd"),
            Some(&2),
            "seen once in each of two jobs"
        );
        assert_eq!(totals.get("tOtherOdd"), Some(&1));
    }

    #[test]
    fn a_job_that_needs_attention_is_counted_separately_from_a_clean_one() {
        let src = tempfile::tempdir().unwrap();
        let out = tempfile::tempdir().unwrap();
        // Only a component that maps: nothing left to resolve.
        write(
            src.path(),
            "clean.item",
            r#"<?xml version="1.0" encoding="UTF-8"?>
<talendfile:ProcessType xmlns:talendfile="platform:/resource/org.talend.model/model/TalendFile.xsd">
  <node componentName="tFileInputDelimited" posX="100" posY="50">
    <elementParameter field="TEXT" name="UNIQUE_NAME" value="in_1"/>
    <elementParameter field="TEXT" name="FILENAME" value="&quot;/data/in.csv&quot;"/>
  </node>
</talendfile:ProcessType>
"#,
        );
        write(src.path(), "messy.item", &job_xml("tSomethingOdd"));

        let report = import_tree(src.path(), out.path()).unwrap();

        assert_eq!(report.clean(), 1, "the mapped-only job is clean");
        assert_eq!(report.needs_attention(), 1, "the other has a warning");
        assert_eq!(report.failed(), 0, "neither failed to parse");
    }

    #[test]
    fn a_java_routine_is_skipped_rather_than_reported_as_a_failure() {
        // Routines share the extension but are Java source, not XML, so parsing one fails
        // on the HTML in its javadoc. Measured against a real corpus that produced four
        // alarming "FAILED" lines for files that were never jobs - which makes a
        // migration look worse than it is and sends someone chasing nothing.
        let src = tempfile::tempdir().unwrap();
        let out = tempfile::tempdir().unwrap();
        // The javadoc closes a tag it never opened, which is what actually breaks the XML
        // reader on a real routine ("expected `</String>`, but `</br>` was found"). A
        // balanced fragment parses fine and would make this test pass without the
        // discriminator existing at all.
        write(
            src.path(),
            "StringHelper.item",
            "package routines;\n\n\
             /**\n \
             * @return <String> the formatted value </br>\n \
             */\n\
             public class StringHelper {\n    \
             public static String get() { return \"x\"; }\n\
             }\n",
        );

        let report = import_tree(src.path(), out.path()).unwrap();

        assert_eq!(report.failed(), 0, "a routine is not a failed job");
        assert_eq!(report.skipped(), 1);
    }

    #[test]
    fn a_corrupt_job_is_still_a_failure() {
        // The other half of the same rule: a file that says it is a job and then will not
        // parse must be reported, or a migration silently loses it.
        let src = tempfile::tempdir().unwrap();
        let out = tempfile::tempdir().unwrap();
        write(
            src.path(),
            "truncated.item",
            r#"<?xml version="1.0" encoding="UTF-8"?>
<talendfile:ProcessType xmlns:talendfile="platform:/resource/org.talend.model/model/TalendFile.xsd">
  <node componentName="tFileInputDelimited" posX="1" posY="1"><elementParameter
"#,
        );

        let report = import_tree(src.path(), out.path()).unwrap();

        assert_eq!(report.failed(), 1, "a broken job file must be reported");
        assert_eq!(report.skipped(), 0);
    }

    #[test]
    fn one_unreadable_file_does_not_end_the_migration() {
        // A corpus of three hundred always contains something odd. Whatever it is, every
        // other job still has to come across, and the odd one still has to be accounted
        // for in the totals rather than vanishing.
        let src = tempfile::tempdir().unwrap();
        let out = tempfile::tempdir().unwrap();
        write(src.path(), "good.item", &job_xml("tSomethingOdd"));
        write(src.path(), "odd.item", "this is not xml at all <<<");

        let report = import_tree(src.path(), out.path()).unwrap();

        assert_eq!(report.jobs.len(), 2, "the odd file is still accounted for");
        assert_eq!(
            report.skipped(),
            1,
            "it never claimed to be a job, so it is not a failure"
        );
        assert!(
            out.path().join("good.json").exists(),
            "the good job still converted"
        );
    }

    #[test]
    fn an_item_file_that_holds_no_job_is_skipped_not_counted_as_clean() {
        // The extension is shared by routines, contexts and metadata, none of which are
        // jobs. They parse fine and yield nothing, so counting them as converted inflates
        // the headline with empty pipelines - measured against a real corpus, 79 of 125
        // "clean" conversions were empty. A migration number that flatters itself is
        // worse than no number.
        let src = tempfile::tempdir().unwrap();
        let out = tempfile::tempdir().unwrap();
        write(src.path(), "real.item", &job_xml("tSomethingOdd"));
        write(
            src.path(),
            "routine.item",
            r#"<?xml version="1.0" encoding="UTF-8"?>
<talendfile:ProcessType xmlns:talendfile="platform:/resource/org.talend.model/model/TalendFile.xsd">
</talendfile:ProcessType>
"#,
        );

        let report = import_tree(src.path(), out.path()).unwrap();

        assert_eq!(report.skipped(), 1, "the routine is not a job");
        assert_eq!(report.clean(), 0, "the only real job has a warning");
        assert_eq!(report.needs_attention(), 1);
        assert!(
            !out.path().join("routine.json").exists(),
            "an empty pipeline must not be written at all"
        );
    }

    #[test]
    fn files_that_are_not_jobs_are_left_alone() {
        let src = tempfile::tempdir().unwrap();
        let out = tempfile::tempdir().unwrap();
        write(src.path(), "a.item", &job_xml("tSomethingOdd"));
        write(src.path(), "notes.txt", "ignore me");
        write(src.path(), "job.properties", "also ignore me");

        let report = import_tree(src.path(), out.path()).unwrap();

        assert_eq!(report.jobs.len(), 1, "only the .item file is a job");
    }
}
