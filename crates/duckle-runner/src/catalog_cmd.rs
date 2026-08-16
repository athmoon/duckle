//! `duckle-runner catalog ...` - ask the workspace graph a question.
//!
//! The graph itself lives in the engine; this is the way to reach it from a
//! terminal or a CI job. Without a command, the whole thing would be a library
//! nobody can call, which is how a feature ships switched off.

use std::path::PathBuf;

use duckle_duckdb_engine::catalog::{self, Catalog, Impact, OwnerRule, Owners};

pub fn run() -> Result<i32, String> {
    let args: Vec<String> = std::env::args().skip(2).collect();
    let sub = args.first().map(String::as_str).unwrap_or("");
    if sub.is_empty() || sub == "-h" || sub == "--help" {
        println!(
            "duckle-runner catalog - what the workspace reads and writes\n\n\
             USAGE:\n    \
             duckle-runner catalog build   [--workspace <dir>]\n    \
             duckle-runner catalog assets  [--workspace <dir>] [--json]\n    \
             duckle-runner catalog impact  <asset> [--workspace <dir>] [--json]\n    \
             duckle-runner catalog orphans [--workspace <dir>]\n    \
             duckle-runner catalog owners  [--workspace <dir>]\n\n\
             build scans every pipeline and writes .duckle/catalog.json. The other\n\
             commands read it, and build it first if it is missing.\n\n\
             impact answers what breaks if an asset changes: the pipelines that read\n\
             it, the assets they write, and everything downstream of those. With an\n\
             owners.json it also says who to tell.\n\n\
             owners.json lives in the workspace and is meant to be committed:\n    \
             {{\"assets\": [{{\"match\": \"/lake/raw/*\", \"owner\": \"Data Platform\",\n     \
             \"contact\": \"dp@example.com\"}}], \"pipelines\": [ ... ]}}\n    \
             The first matching rule wins, so put specific patterns above general ones.\n\n\
             assets are named the way the pipelines name them, for example\n    \
             postgres://db:5432/sales.public.orders\n    \
             s3://bucket/curated/orders.parquet\n    \
             /lake/staged.parquet"
        );
        return Ok(0);
    }

    let mut workspace = PathBuf::from(".");
    let mut as_json = false;
    let mut positional: Vec<String> = Vec::new();
    let mut it = args.iter().skip(1);
    while let Some(a) = it.next() {
        match a.as_str() {
            "--workspace" => {
                workspace = PathBuf::from(it.next().ok_or("--workspace needs a value")?)
            }
            "--json" => as_json = true,
            other if other.starts_with("--") => {
                return Err(format!("unknown argument: {other}"))
            }
            other => positional.push(other.to_string()),
        }
    }

    match sub {
        "build" => {
            let cat = catalog::build_and_save(&workspace)?;
            println!(
                "{} pipelines, {} assets, {} links.",
                cat.pipelines.len(),
                cat.assets.len(),
                cat.touches.len()
            );
            report_unresolved(&cat);
            println!("Written to {}", catalog::catalog_path(&workspace).display());
            Ok(0)
        }
        "assets" => {
            let cat = load_or_build(&workspace)?;
            if as_json {
                println!("{}", serde_json::to_string_pretty(&cat.assets).map_err(|e| e.to_string())?);
                return Ok(0);
            }
            if cat.assets.is_empty() {
                println!("No assets. Are there pipelines in {}?", workspace.display());
            }
            for asset in &cat.assets {
                let writes = cat.producers(&asset.id).len();
                let reads = cat.consumers(&asset.id).len();
                println!("{:<9} {:<3}w {:<3}r  {}", asset.kind, writes, reads, asset.id);
            }
            report_unresolved(&cat);
            Ok(0)
        }
        "impact" => {
            let asset = positional
                .first()
                .ok_or("impact needs an asset, e.g. `catalog impact /lake/staged.parquet`")?;
            let cat = load_or_build(&workspace)?;
            let owners = catalog::load_owners(&workspace)?;
            let hit = cat.impact(asset, Some(&owners));
            // An asset nobody names is almost always a typo, and saying
            // "nothing is affected" would read as reassurance rather than as a
            // miss. Checked before the JSON branch as well, because that is the
            // one a CI job reads: `catalog impact <typo> --json` printed an
            // empty blast radius and exited 0, so a mistyped asset name passed
            // the gate it was added to enforce.
            let named = cat.assets.iter().any(|a| &a.id == asset);
            if as_json {
                if !named {
                    eprintln!(
                        "No pipeline names '{asset}'. Run `catalog assets` to see the names in use."
                    );
                }
                println!("{}", serde_json::to_string_pretty(&hit).map_err(|e| e.to_string())?);
                return Ok(if named { 0 } else { 1 });
            }
            if !named {
                println!("No pipeline names '{asset}'. Run `catalog assets` to see the names in use.");
                return Ok(1);
            }
            if hit.pipelines.is_empty() {
                println!("Nothing reads {asset}.");
            } else {
                println!("Changing {asset} reaches:");
                // The owner is the other half of the answer: knowing what
                // breaks is only useful alongside someone to tell.
                let owned = |o: &Option<String>| match o {
                    Some(name) => format!("  [{name}]"),
                    None => String::new(),
                };
                for p in &hit.pipelines {
                    println!("  {:>2} hop  pipeline  {}{}", p.depth, p.id, owned(&p.owner));
                }
                for a in &hit.assets {
                    println!("  {:>2} hop  asset     {}{}", a.depth, a.id, owned(&a.owner));
                }
                let contacts = contacts_for(&owners, &hit);
                if !contacts.is_empty() {
                    println!("\nTell: {}", contacts.join(", "));
                }
            }
            report_unresolved(&cat);
            Ok(0)
        }
        "owners" => {
            let cat = load_or_build(&workspace)?;
            let owners = catalog::load_owners(&workspace)?;
            if owners.is_empty() {
                println!(
                    "No {}, so nothing has an owner yet.",
                    catalog::owners_path(&workspace).display()
                );
            }
            let unowned = cat.unowned(&owners).len();
            println!("{} of {} assets have an owner.", cat.assets.len() - unowned, cat.assets.len());
            for asset in &cat.assets {
                let who = owners.for_asset(&asset.id).map(|r| r.owner.as_str()).unwrap_or("-");
                println!("  {:<20} {}", who, asset.id);
            }
            let unowned_pipelines: Vec<&str> = cat
                .pipelines
                .iter()
                .filter(|p| owners.for_pipeline(&p.id).is_none())
                .map(|p| p.id.as_str())
                .collect();
            if !unowned_pipelines.is_empty() {
                println!("\nPipelines with no owner: {}", unowned_pipelines.join(", "));
            }
            report_unresolved(&cat);
            Ok(0)
        }
        "orphans" => {
            let cat = load_or_build(&workspace)?;
            let orphans = cat.orphans();
            if orphans.is_empty() {
                println!("Everything written is read by something.");
            } else {
                println!("Written but never read here:");
                for a in orphans {
                    println!("  {:<9} {}", a.kind, a.id);
                }
                println!(
                    "\nThese may be real outputs the workspace hands to something else.\n\
                     Nothing is inferred about whether they are safe to remove."
                );
            }
            report_unresolved(&cat);
            Ok(0)
        }
        other => Err(format!("unknown catalog command: {other}")),
    }
}

/// Everyone reached, de-duplicated and in a stable order, so the line can be
/// pasted into a message rather than transcribed.
fn contacts_for(owners: &Owners, hit: &Impact) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    let mut push = |rule: Option<&OwnerRule>| {
        if let Some(r) = rule {
            let entry = match &r.contact {
                Some(c) => format!("{} <{}>", r.owner, c),
                None => r.owner.clone(),
            };
            if !out.contains(&entry) {
                out.push(entry);
            }
        }
    };
    for p in &hit.pipelines {
        push(owners.for_pipeline(&p.id));
    }
    for a in &hit.assets {
        push(owners.for_asset(&a.id));
    }
    out
}

/// Say what the graph could not see. Printed after every answer, because a
/// blast-radius result that hides its own gaps is the one that gets trusted
/// and then turns out to have missed the node that mattered.
fn report_unresolved(cat: &Catalog) {
    if cat.unresolved.is_empty() {
        return;
    }
    eprintln!(
        "\n{} source/sink node(s) could not be named, so the answers above may be incomplete:",
        cat.unresolved.len()
    );
    for u in cat.unresolved.iter().take(10) {
        eprintln!("  {} / {} ({})", u.pipeline_id, u.node_id, u.component_id);
    }
    if cat.unresolved.len() > 10 {
        eprintln!("  ... and {} more", cat.unresolved.len() - 10);
    }
}

/// Use the saved graph, building one if there is none yet, so the first
/// question anyone asks does not fail on a missing file.
fn load_or_build(workspace: &PathBuf) -> Result<Catalog, String> {
    // Rebuilds when the saved graph no longer describes the workspace, not only
    // when it is missing. Answering "what breaks if I change this" from a graph
    // built before the last three pipelines were edited is worse than taking
    // the second to rescan.
    catalog::load_or_rebuild(workspace)
}
