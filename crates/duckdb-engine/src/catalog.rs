//! What the whole workspace reads and writes, across every pipeline.
//!
//! Everything Duckle could already tell you about lineage stopped at the edge
//! of one pipeline. `pipeline_impact` inverts column lineage inside a single
//! file; trust, drift and review each take one pipeline and answer about that
//! pipeline. None of them can answer the question an owner of two hundred
//! pipelines actually asks, which is "if I drop this column, or this table
//! moves, what breaks and who do I tell?".
//!
//! This builds the missing half: the graph *between* pipelines. Each source and
//! sink node names something outside the workspace - a file, a table, a topic -
//! and two pipelines that name the same thing are connected whether or not
//! anyone drew a line between them. Collect those names and the connections
//! fall out, along with the things nobody reads and the things nobody writes.
//!
//! # Honesty about coverage
//!
//! An asset name is recovered from a node's properties, and not every connector
//! yields one: a REST source pointed at a templated URL, a component nobody has
//! taught this module about, a node left half-configured. Those are recorded in
//! [`Catalog::unresolved`] rather than dropped. A blast-radius answer that
//! quietly omits the nodes it could not read is worse than no answer, because
//! it looks complete. Anything asking this module a governance question should
//! show that list alongside the result.

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Which way data flows between a pipeline and an asset.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Direction {
    Read,
    Write,
}

/// Something outside the workspace that pipelines read or write.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Asset {
    /// Canonical name, stable enough that two pipelines naming the same thing
    /// produce the same string. This is the join key of the whole graph.
    pub id: String,
    /// Broad family: file, table, topic, collection or api.
    pub kind: String,
}

/// One pipeline touching one asset, at one node.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Touch {
    pub pipeline_id: String,
    pub node_id: String,
    pub component_id: String,
    pub asset: String,
    pub direction: Direction,
}

/// A source or sink whose target could not be named, and why.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Unresolved {
    pub pipeline_id: String,
    pub node_id: String,
    pub component_id: String,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PipelineEntry {
    pub id: String,
    pub name: String,
    pub node_count: usize,
}

/// The workspace graph as of the last build.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Catalog {
    pub pipelines: Vec<PipelineEntry>,
    pub assets: Vec<Asset>,
    pub touches: Vec<Touch>,
    /// Nodes this module could not name a target for. Never empty-by-omission:
    /// see the module docs.
    pub unresolved: Vec<Unresolved>,
}

/// One ownership rule: a glob over names, and who answers for what it matches.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OwnerRule {
    /// Glob over asset or pipeline names. `*` matches any characters,
    /// separators included, so `/lake/raw/*` covers everything beneath it.
    #[serde(rename = "match")]
    pub pattern: String,
    pub owner: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub contact: Option<String>,
}

/// Who owns what, as authored by a human.
///
/// Rules are globs rather than one entry per asset because a workspace has
/// hundreds of assets and nobody maintains a list that long: ownership is
/// really "this team owns everything under /lake/raw". The first matching rule
/// wins, so put the specific ones above the general ones - the same order a
/// reader assumes when they scan the file top to bottom.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Owners {
    #[serde(default)]
    pub assets: Vec<OwnerRule>,
    #[serde(default)]
    pub pipelines: Vec<OwnerRule>,
}

impl Owners {
    pub fn for_asset(&self, id: &str) -> Option<&OwnerRule> {
        first_match(&self.assets, id)
    }

    pub fn for_pipeline(&self, id: &str) -> Option<&OwnerRule> {
        first_match(&self.pipelines, id)
    }

    pub fn is_empty(&self) -> bool {
        self.assets.is_empty() && self.pipelines.is_empty()
    }
}

fn first_match<'a>(rules: &'a [OwnerRule], name: &str) -> Option<&'a OwnerRule> {
    rules.iter().find(|r| {
        // A pattern that will not compile matches nothing rather than
        // everything: a typo must not silently hand a team the whole workspace.
        glob::Pattern::new(&r.pattern).map(|p| p.matches(name)).unwrap_or(false)
    })
}

/// Authored, so it lives beside the pipelines and belongs in version control -
/// unlike the catalog itself, which is derived and lives under `.duckle`.
pub fn owners_path(workspace: &Path) -> PathBuf {
    workspace.join("owners.json")
}

/// Ownership rules, or an empty set when the workspace has none.
///
/// A file that will not parse is an error rather than "nobody owns anything",
/// because silently reporting every asset as unowned is indistinguishable from
/// the answer a workspace with no file at all should get.
pub fn load_owners(workspace: &Path) -> Result<Owners, String> {
    let p = owners_path(workspace);
    if !p.exists() {
        return Ok(Owners::default());
    }
    let text = std::fs::read_to_string(&p).map_err(|e| e.to_string())?;
    if text.trim().is_empty() {
        return Ok(Owners::default());
    }
    serde_json::from_str(&text).map_err(|e| format!("parse owners.json: {e}"))
}

pub fn catalog_path(workspace: &Path) -> PathBuf {
    workspace.join(".duckle").join("catalog.json")
}

impl Catalog {
    /// Pipelines that write `asset`.
    pub fn producers(&self, asset: &str) -> Vec<&Touch> {
        self.touches
            .iter()
            .filter(|t| t.asset == asset && t.direction == Direction::Write)
            .collect()
    }

    /// Pipelines that read `asset`.
    pub fn consumers(&self, asset: &str) -> Vec<&Touch> {
        self.touches
            .iter()
            .filter(|t| t.asset == asset && t.direction == Direction::Read)
            .collect()
    }

    /// Everything downstream of `asset`: the pipelines that read it, the assets
    /// those pipelines write, the pipelines that read *those*, and so on.
    ///
    /// This is the blast radius of changing or dropping something. The walk
    /// keeps a visited set because workspaces really do contain cycles - a
    /// pipeline that reads a table and writes it back is a normal incremental
    /// pattern - and a cycle must end the walk, not hang it.
    pub fn impact(&self, asset: &str, owners: Option<&Owners>) -> Impact {
        // Index once rather than rescanning the touch list per hop; a workspace
        // with hundreds of pipelines otherwise turns this quadratic.
        let mut reads_by_asset: HashMap<&str, Vec<&Touch>> = HashMap::new();
        let mut writes_by_pipeline: HashMap<&str, Vec<&Touch>> = HashMap::new();
        for t in &self.touches {
            match t.direction {
                Direction::Read => reads_by_asset.entry(&t.asset).or_default().push(t),
                Direction::Write => {
                    writes_by_pipeline.entry(&t.pipeline_id).or_default().push(t)
                }
            }
        }

        let mut seen_assets: HashSet<String> = HashSet::from([asset.to_string()]);
        let mut pipelines: BTreeMap<String, usize> = BTreeMap::new();
        let mut assets: BTreeMap<String, usize> = BTreeMap::new();
        let mut queue: VecDeque<(String, usize)> = VecDeque::from([(asset.to_string(), 0)]);

        while let Some((current, depth)) = queue.pop_front() {
            for read in reads_by_asset.get(current.as_str()).into_iter().flatten() {
                // A pipeline can be reached by several paths; keep the shortest,
                // which is the one a reader will find most believable.
                let entry = pipelines.entry(read.pipeline_id.clone()).or_insert(depth + 1);
                if *entry > depth + 1 {
                    *entry = depth + 1;
                }
                for write in writes_by_pipeline.get(read.pipeline_id.as_str()).into_iter().flatten()
                {
                    if seen_assets.insert(write.asset.clone()) {
                        assets.insert(write.asset.clone(), depth + 1);
                        queue.push_back((write.asset.clone(), depth + 1));
                    }
                }
            }
        }

        Impact {
            asset: asset.to_string(),
            pipelines: pipelines
                .into_iter()
                .map(|(id, depth)| Reached {
                    owner: owners.and_then(|o| o.for_pipeline(&id)).map(|r| r.owner.clone()),
                    id,
                    depth,
                })
                .collect(),
            assets: assets
                .into_iter()
                .map(|(id, depth)| Reached {
                    owner: owners.and_then(|o| o.for_asset(&id)).map(|r| r.owner.clone()),
                    id,
                    depth,
                })
                .collect(),
            unresolved: self.unresolved.len(),
        }
    }

    /// Assets written by some pipeline and read by none. Often a real output,
    /// sometimes a leftover nobody noticed stopped being used.
    pub fn orphans(&self) -> Vec<&Asset> {
        let read: HashSet<&str> = self
            .touches
            .iter()
            .filter(|t| t.direction == Direction::Read)
            .map(|t| t.asset.as_str())
            .collect();
        let written: HashSet<&str> = self
            .touches
            .iter()
            .filter(|t| t.direction == Direction::Write)
            .map(|t| t.asset.as_str())
            .collect();
        self.assets
            .iter()
            .filter(|a| written.contains(a.id.as_str()) && !read.contains(a.id.as_str()))
            .collect()
    }

    /// Assets that no ownership rule matches.
    ///
    /// The useful governance answer is not "here is the owner of this one
    /// thing" but "here are the forty things nobody has claimed", which is the
    /// list that gets worked through before an audit.
    pub fn unowned<'a>(&'a self, owners: &Owners) -> Vec<&'a Asset> {
        self.assets.iter().filter(|a| owners.for_asset(&a.id).is_none()).collect()
    }

    /// Assets read by some pipeline and written by none, so the workspace
    /// depends on them without producing them. These are the external contracts
    /// nobody here controls.
    pub fn externals(&self) -> Vec<&Asset> {
        let written: HashSet<&str> = self
            .touches
            .iter()
            .filter(|t| t.direction == Direction::Write)
            .map(|t| t.asset.as_str())
            .collect();
        let read: HashSet<&str> = self
            .touches
            .iter()
            .filter(|t| t.direction == Direction::Read)
            .map(|t| t.asset.as_str())
            .collect();
        self.assets
            .iter()
            .filter(|a| read.contains(a.id.as_str()) && !written.contains(a.id.as_str()))
            .collect()
    }
}

/// A node reached while walking downstream, and how many hops away it is.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Reached {
    pub id: String,
    pub depth: usize,
    /// Who to tell, when the workspace says. `None` means no rule matched,
    /// which is a real answer worth showing rather than a blank.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Impact {
    pub asset: String,
    pub pipelines: Vec<Reached>,
    pub assets: Vec<Reached>,
    /// How many source/sink nodes in the workspace could not be named at all.
    /// Carried on the answer so a caller cannot present it as exhaustive
    /// without also seeing what was missed.
    pub unresolved: usize,
}

/// Folders that hold Duckle's own output rather than pipelines. Kept identical
/// to the console's walk, because a pipeline either of them can open and the
/// other cannot is a hole in whichever answer omits it.
const NOT_PIPELINES: [&str; 7] =
    ["runs", "logs", "connections", "node_modules", ".duckle", ".git", "target"];

/// Every candidate pipeline file in the workspace.
///
/// This used to read `<workspace>/pipelines/*.json` and nothing else, while the
/// console and the desktop walk the whole workspace and skip the folders above.
/// Both of those support keeping pipelines in subfolders, so a workspace laid
/// out that way had them silently missing from the graph - and a blast radius
/// that quietly omits a pipeline is worse than no answer at all, because it
/// looks like a complete one.
pub fn discover_pipeline_files(workspace: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    let mut stack = vec![workspace.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&dir) else { continue };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
                if !NOT_PIPELINES.contains(&name) {
                    stack.push(path);
                }
            } else if path.extension().and_then(|x| x.to_str()) == Some("json") {
                out.push(path);
            }
        }
    }
    // A stable order, so a rebuild that changed nothing produces no diff.
    out.sort();
    out
}

/// Read every pipeline in the workspace and build the graph.
pub fn build(workspace: &Path) -> Result<Catalog, String> {
    let mut catalog = Catalog::default();
    let mut assets: BTreeMap<String, Asset> = BTreeMap::new();

    for path in discover_pipeline_files(workspace) {
        let Ok(text) = std::fs::read_to_string(&path) else { continue };
        let Ok(doc): Result<Value, _> = serde_json::from_str(&text) else { continue };
        let Some(nodes) = doc.get("nodes").and_then(|n| n.as_array()) else { continue };
        let pipeline_id =
            path.file_stem().map(|s| s.to_string_lossy().into_owned()).unwrap_or_default();
        let name = doc
            .get("name")
            .and_then(|n| n.as_str())
            .unwrap_or(&pipeline_id)
            .to_string();
        catalog.pipelines.push(PipelineEntry {
            id: pipeline_id.clone(),
            name,
            node_count: nodes.len(),
        });

        for node in nodes {
            let data = node.get("data").unwrap_or(&Value::Null);
            let component_id = data.get("componentId").and_then(|c| c.as_str()).unwrap_or("");
            let direction = match () {
                _ if component_id.starts_with("src.") => Direction::Read,
                _ if component_id.starts_with("snk.") => Direction::Write,
                // Transforms touch nothing outside the workspace.
                _ => continue,
            };
            let node_id = node.get("id").and_then(|i| i.as_str()).unwrap_or("").to_string();
            let props = data.get("properties").unwrap_or(&Value::Null);

            match asset_of(component_id, props) {
                Ok(asset) => {
                    catalog.touches.push(Touch {
                        pipeline_id: pipeline_id.clone(),
                        node_id,
                        component_id: component_id.to_string(),
                        asset: asset.id.clone(),
                        direction,
                    });
                    assets.entry(asset.id.clone()).or_insert(asset);
                }
                Err(reason) => catalog.unresolved.push(Unresolved {
                    pipeline_id: pipeline_id.clone(),
                    node_id,
                    component_id: component_id.to_string(),
                    reason,
                }),
            }
        }
    }

    catalog.assets = assets.into_values().collect();
    Ok(catalog)
}

/// Name the thing a source or sink node points at.
///
/// The rules follow the shapes the connector manifests actually require, most
/// specific first, and were derived by reading the required fields of all 190
/// shipped sources and sinks rather than by guessing. Template placeholders
/// such as `${date}` are deliberately kept: a daily file is one asset with a
/// date in its name, not a new asset every morning, and collapsing them is what
/// makes a dated path joinable across pipelines.
/// An address with the credential taken out of it.
///
/// Asset ids are names, and names get published. `GET /api/catalog` is rated
/// for the **viewer** role, `.duckle/catalog.json` is meant to be committed,
/// and the MCP workspace tools hand out the same strings. Two shipped shapes
/// put a password in the very field that names the server: a `uri` like
/// `mongodb://user:pass@host:27017`, and an ODBC `connectionString` ending
/// `;UID=u;PWD=p`. Neither can be the name.
///
/// Removing it also makes the name *stabler*, which is the whole job of a join
/// key: an id built from a password forks into a second asset the day that
/// password is rotated, and every impact answer spanning the rotation is then
/// wrong in a way nobody would think to check.
///
/// Two shapes are handled, because those are the two the connectors actually
/// produce. A credential passed as a query parameter is not one of them: the
/// `url` branch already drops the query string before calling this, and no
/// shipped connector puts one in an `endpoint`.
fn public_address(raw: &str) -> String {
    // `KEY=value;KEY=value` - an ODBC or JDBC connection string. Segments
    // naming a credential go entirely; what is left still names the server,
    // which is all the graph needs. The scheme and separator checks keep a
    // Hive-style path such as `/lake/dt=2026-08-15;part=1/x.parquet` out of
    // here, since it is a path and not a DSN.
    if !raw.contains("://")
        && raw.contains(';')
        && raw.contains('=')
        && !raw.contains('/')
        && !raw.contains('\\')
    {
        return raw
            .split(';')
            .filter(|seg| !seg.trim().is_empty())
            .filter(|seg| !is_dsn_credential(seg.split('=').next().unwrap_or("").trim()))
            .collect::<Vec<_>>()
            .join(";");
    }
    // `scheme://userinfo@host/tail`, or the same without a scheme. The
    // userinfo is only ever in the authority, so the search stops at the first
    // '/': a path or a query may legitimately contain '@'.
    let (prefix, rest) = match raw.split_once("://") {
        Some((scheme, rest)) => (format!("{scheme}://"), rest),
        None => (String::new(), raw),
    };
    let (authority, tail) = rest.split_at(rest.find('/').unwrap_or(rest.len()));
    let authority = authority.rsplit_once('@').map(|(_, host)| host).unwrap_or(authority);
    format!("{prefix}{authority}{tail}")
}

/// True for a connection-string key that holds a credential.
///
/// ODBC and JDBC spell these much shorter than a Duckle property key does, so
/// the engine's own [`is_secret_prop_key`] does not recognise them. A login
/// name is dropped along with the password on purpose: two pipelines reaching
/// one database under different logins are reading one asset, and keeping the
/// user in the name would split them.
fn is_dsn_credential(key: &str) -> bool {
    let k = key.to_ascii_lowercase();
    matches!(k.as_str(), "pwd" | "uid" | "usr" | "user" | "username")
        || crate::util::is_secret_prop_key(&k)
}

pub fn asset_of(component_id: &str, props: &Value) -> Result<Asset, String> {
    let s = |k: &str| -> Option<String> {
        props
            .get(k)
            .and_then(|v| match v {
                // A port is authored in the GUI as `kind: 'integer'`, so it
                // arrives as a JSON number. Reading only strings dropped it,
                // and two instances on one host then collapsed into one asset:
                // db:5432/sales and db:5433/sales were the same name.
                Value::Number(n) => Some(n.to_string()),
                other => other.as_str().map(str::to_string),
            })
            .map(|v| v.trim().to_string())
            .filter(|v| !v.is_empty())
    };
    // The connector family, used as the scheme when the target has no natural
    // one: `snk.postgres` -> `postgres`.
    let family = component_id.split('.').nth(1).unwrap_or("duckle");
    // Where the server is, when the properties say. Embedded engines have no
    // authority at all, which is correct: the file is the whole address.
    // A uri-shaped property already names its own scheme, so prefixing the
    // family would give `mongodb://mongodb://...`. Use it as the whole prefix
    // when it does, and build one from the family when it does not.
    let prefixed = |authority: &str| -> String {
        let authority = public_address(authority);
        if authority.contains("://") {
            authority.trim_end_matches('/').to_string()
        } else {
            format!("{family}://{authority}")
        }
    };
    // Join an address to the thing inside it without leaving an empty segment.
    // An absent authority is normal - an embedded database, a SaaS object with
    // no instance named - and `salesforce:///Account` would not join to the
    // same string anyone else produces.
    let addr = |authority: &str, tail: &str| -> String {
        let base = prefixed(authority);
        if base.ends_with("://") {
            format!("{base}{tail}")
        } else {
            format!("{}/{tail}", base.trim_end_matches('/'))
        }
    };
    let authority = || -> String {
        match (s("host").or_else(|| s("endpoint")).or_else(|| s("uri")).or_else(|| s("connectionString")).or_else(|| s("connect")), s("port")) {
            (Some(h), Some(p)) if !h.contains("://") && !h.contains(':') => format!("{h}:{p}"),
            (Some(h), _) => h.trim_end_matches('/').to_string(),
            _ => String::new(),
        }
    };

    // A path already carries its own scheme when it is remote (s3://, gs://,
    // sftp://), so it is used as-is; a local path is normalised to forward
    // slashes so the same file named from Windows and Linux agrees.
    if let Some(path) = s("path") {
        let kind = if path.contains("://") { "object" } else { "file" };
        return Ok(Asset { id: normalise_path(&public_address(&path)), kind: kind.into() });
    }

    // Object stores that split the address into a bucket and a key.
    if let (Some(bucket), Some(key)) = (s("bucket"), s("key")) {
        return Ok(Asset {
            id: addr(&bucket, key.trim_start_matches('/')),
            kind: "object".into(),
        });
    }

    // Kafka and friends: brokers identify the cluster, topic the stream.
    if let (Some(brokers), Some(topic)) = (s("brokers").or_else(|| s("contactPoints")), s("topic")) {
        return Ok(Asset {
            id: addr(first_host(&brokers), &topic),
            kind: "topic".into(),
        });
    }

    // Search engines address an index on an endpoint.
    if let Some(index) = s("index") {
        return Ok(Asset {
            id: addr(&authority(), &index),
            kind: "index".into(),
        });
    }

    // Document and vector stores: a collection, optionally inside a database.
    if let Some(collection) = s("collection") {
        let db = s("database").map(|d| format!("{d}.")).unwrap_or_default();
        return Ok(Asset {
            id: addr(&authority(), &format!("{db}{collection}")),
            kind: "collection".into(),
        });
    }

    // Relational: a table somewhere. `tableName` is the common spelling and
    // `table` is what the embedded vector stores use.
    if let Some(table) = s("tableName").or_else(|| s("table")) {
        let mut qualified = String::new();
        for part in [s("database"), s("schema")].into_iter().flatten() {
            qualified.push_str(&part);
            qualified.push('.');
        }
        qualified.push_str(&table);
        return Ok(Asset {
            id: addr(&authority(), &qualified),
            kind: "table".into(),
        });
    }

    // SaaS objects, where the object name is the whole target.
    if let Some(object) = s("object").or_else(|| s("objectName")) {
        return Ok(Asset { id: addr(&authority(), &object), kind: "object".into() });
    }

    // A path on a named server. FTP and SFTP give the host and the remote
    // directory or file in separate required fields, so neither half is the
    // address on its own: naming only the host made every directory on one
    // server the same asset, and joined two pipelines that share nothing but
    // the machine they log in to.
    if let Some(remote) = s("remotePath").or_else(|| s("directory")) {
        return Ok(Asset {
            id: addr(&authority(), remote.trim_start_matches('/')),
            kind: "file".into(),
        });
    }

    // A REST-shaped endpoint. Query strings are dropped: they are usually
    // paging or filter parameters, and keeping them would split one endpoint
    // into an asset per call.
    if let Some(url) = s("url") {
        let without_query = url.split('?').next().unwrap_or(&url);
        return Ok(Asset {
            id: public_address(without_query).trim_end_matches('/').to_string(),
            kind: "api".into(),
        });
    }

    // A whole database, with no finer target named. This is the honest answer
    // for a source that runs its own query: the query text is not an address,
    // and naming the database still links it to everything else on that
    // database, which is what the graph is for.
    if let Some(database) = s("database") {
        return Ok(Asset {
            id: addr(&authority(), &database),
            kind: "database".into(),
        });
    }
    if !authority().is_empty() {
        return Ok(Asset { id: prefixed(&authority()), kind: "database".into() });
    }
    // Services whose whole address is an account, project or repository, with
    // nothing finer named. Coarse on purpose: naming the service still links
    // every pipeline that uses it, which beats leaving them all unconnected.
    for key in ["account", "project", "workspace", "repo", "indexHost", "contactPoints"] {
        if let Some(v) = s(key) {
            return Ok(Asset {
                id: prefixed(first_host(&v)),
                kind: "service".into(),
            });
        }
    }

    Err(format!(
        "no target property on {component_id}; expected one of path, bucket+key, topic,          index, collection, tableName, object, url or database"
    ))
}

/// Lower-case the drive letter and use forward slashes, so `C:\data\x.csv` and
/// `c:/data/x.csv` are recognised as the same file.
fn normalise_path(path: &str) -> String {
    let unified = path.replace('\\', "/");
    let mut chars = unified.chars();
    match (chars.next(), chars.next()) {
        (Some(drive), Some(':')) if drive.is_ascii_alphabetic() => {
            format!("{}:{}", drive.to_ascii_lowercase(), chars.as_str())
        }
        _ => unified,
    }
}

/// The first host in a comma-separated broker list, so `a:9092,b:9092` and
/// `a:9092` name the same cluster.
fn first_host(brokers: &str) -> &str {
    brokers.split(',').next().unwrap_or(brokers).trim()
}

/// Build the graph and persist it, returning what was written.
pub fn build_and_save(workspace: &Path) -> Result<Catalog, String> {
    let catalog = build(workspace)?;
    let p = catalog_path(workspace);
    if let Some(dir) = p.parent() {
        std::fs::create_dir_all(dir).map_err(|e| e.to_string())?;
    }
    let body = serde_json::to_string_pretty(&catalog).map_err(|e| e.to_string())?;
    // A temp name of this writer's own. One shared `catalog.json.tmp` meant two
    // rebuilds - the console's POST /api/catalog and a `catalog build` in a
    // terminal, which is an ordinary pairing - wrote the same file, so one
    // could rename away the other's half-written bytes. No lock is needed
    // beyond this: both runs derive the same graph from the same pipelines, so
    // a complete last-writer-wins file is the right answer.
    static TMP_SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let seq = TMP_SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let tmp = p.with_extension(format!("json.{}.{seq}.tmp", std::process::id()));
    std::fs::write(&tmp, body).map_err(|e| e.to_string())?;
    // Renamed straight over, with no unlink first: see write_atomically in
    // schedules.rs for why removing the destination is both unnecessary and
    // the thing that opens a window.
    if let Err(e) = std::fs::rename(&tmp, &p) {
        let _ = std::fs::remove_file(&tmp);
        return Err(e.to_string());
    }
    Ok(catalog)
}

/// The last built graph, or None if it has never been built.
pub fn load(workspace: &Path) -> Result<Option<Catalog>, String> {
    let p = catalog_path(workspace);
    if !p.exists() {
        return Ok(None);
    }
    let text = std::fs::read_to_string(&p).map_err(|e| e.to_string())?;
    serde_json::from_str(&text).map(Some).map_err(|e| format!("parse catalog.json: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn write_pipeline(ws: &Path, id: &str, nodes: Value) {
        let dir = ws.join("pipelines");
        std::fs::create_dir_all(&dir).unwrap();
        let doc = json!({ "name": id, "nodes": nodes, "edges": [] });
        std::fs::write(dir.join(format!("{id}.json")), doc.to_string()).unwrap();
    }

    fn node(id: &str, component: &str, props: Value) -> Value {
        json!({ "id": id, "data": { "componentId": component, "properties": props } })
    }

    #[test]
    fn a_table_is_named_the_same_way_from_either_end() {
        // The whole graph hangs on two pipelines naming one thing identically,
        // so a reader and a writer of the same table must agree exactly.
        let reader = asset_of(
            "src.postgres",
            &json!({ "host": "db.internal", "port": "5432", "database": "sales", "schema": "public", "tableName": "orders" }),
        )
        .unwrap();
        let writer = asset_of(
            "snk.postgres",
            &json!({ "host": "db.internal", "port": "5432", "database": "sales", "schema": "public", "tableName": "orders" }),
        )
        .unwrap();
        assert_eq!(reader.id, writer.id);
        assert_eq!(reader.id, "postgres://db.internal:5432/sales.public.orders");
        assert_eq!(reader.kind, "table");
    }

    /// An asset id is a published name, so it must not carry a password.
    ///
    /// `GET /api/catalog` is rated for the viewer role, `.duckle/catalog.json`
    /// is committed, and the MCP tools return the same strings, so a password
    /// spliced into a name reaches all three. Both shipped shapes are covered:
    /// userinfo in a uri, and an ODBC connection string.
    #[test]
    fn an_asset_name_never_carries_the_credential_that_reached_it() {
        // src.mongodb's own placeholder is mongodb://user:pass@host:27017.
        let mongo = asset_of(
            "src.mongodb",
            &json!({ "uri": "mongodb://admin:hunter2@db.internal:27017", "database": "sales", "collection": "orders" }),
        )
        .unwrap();
        assert!(!mongo.id.contains("hunter2"), "the password is in the asset name: {}", mongo.id);
        assert_eq!(mongo.id, "mongodb://db.internal:27017/sales.orders");

        // And the same server named without a credential is the SAME asset,
        // which is the point: the name is a join key, so it cannot depend on
        // who connected or on a password that will be rotated.
        let plain = asset_of(
            "snk.mongodb",
            &json!({ "uri": "mongodb://db.internal:27017", "database": "sales", "collection": "orders" }),
        )
        .unwrap();
        assert_eq!(mongo.id, plain.id, "rotating the password forked one asset into two");

        // src.teradata's own placeholder ends ...;UID=...;PWD=...
        let odbc = asset_of(
            "src.teradata",
            &json!({ "connectionString": "DRIVER={Teradata Database ODBC Driver 17.20};DBCNAME=td.internal;UID=etl;PWD=hunter2", "database": "sales", "tableName": "orders" }),
        )
        .unwrap();
        assert!(!odbc.id.contains("hunter2"), "the password is in the asset name: {}", odbc.id);
        assert!(!odbc.id.contains("UID=etl"), "the login is in the asset name: {}", odbc.id);
        assert!(odbc.id.contains("DBCNAME=td.internal"), "the server was lost: {}", odbc.id);

        // A REST endpoint reached with basic credentials in the URL.
        let api = asset_of("src.rest", &json!({ "url": "https://svc:tok3n@api.example.com/v1/orders" })).unwrap();
        assert_eq!(api.id, "https://api.example.com/v1/orders");

        // And an sftp path, where the same shape appears in `path`.
        let sftp = asset_of("src.xml", &json!({ "path": "sftp://etl:hunter2@files.internal/in/orders.xml" })).unwrap();
        assert_eq!(sftp.id, "sftp://files.internal/in/orders.xml");
    }

    /// A pipeline the console can open must be a pipeline the graph can see.
    ///
    /// The catalog read `<workspace>/pipelines/*.json` and nothing else, while
    /// the console and the desktop walk the workspace. A pipeline in a
    /// subfolder was therefore missing from every impact answer, silently.
    #[test]
    fn pipelines_in_subfolders_are_in_the_graph_and_duckle_s_own_folders_are_not() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path();

        // The layout the flat scan saw.
        write_pipeline(ws, "flat", json!([node("a", "snk.parquet", json!({ "path": "/lake/flat.parquet" }))]));

        // A pipeline organised into a subfolder, which both editors support.
        let nested = ws.join("pipelines").join("nightly");
        std::fs::create_dir_all(&nested).unwrap();
        std::fs::write(
            nested.join("load.json"),
            json!({ "name": "load", "nodes": [node("a", "src.parquet", json!({ "path": "/lake/flat.parquet" }))], "edges": [] }).to_string(),
        )
        .unwrap();

        // And one outside the pipelines folder entirely.
        let other = ws.join("flows");
        std::fs::create_dir_all(&other).unwrap();
        std::fs::write(
            other.join("export.json"),
            json!({ "name": "export", "nodes": [node("a", "src.parquet", json!({ "path": "/lake/flat.parquet" }))], "edges": [] }).to_string(),
        )
        .unwrap();

        // Duckle's own output must not be mistaken for pipelines. A run record
        // has no nodes array, but .duckle holds documents that do.
        let hidden = ws.join(".duckle");
        std::fs::create_dir_all(&hidden).unwrap();
        std::fs::write(
            hidden.join("catalog.json"),
            json!({ "name": "not-a-pipeline", "nodes": [node("a", "snk.parquet", json!({ "path": "/lake/should-not-appear.parquet" }))], "edges": [] }).to_string(),
        )
        .unwrap();

        let cat = build(ws).unwrap();
        let ids: Vec<&str> = cat.pipelines.iter().map(|p| p.id.as_str()).collect();
        assert!(ids.contains(&"flat"), "the flat pipeline was lost: {ids:?}");
        assert!(ids.contains(&"load"), "a pipeline in a subfolder is missing from the graph: {ids:?}");
        assert!(ids.contains(&"export"), "a pipeline outside pipelines/ is missing: {ids:?}");
        assert!(!ids.contains(&"not-a-pipeline"), "walked into .duckle: {ids:?}");

        // And the point of finding them: they join.
        let hit = cat.impact("/lake/flat.parquet", None);
        let reached: Vec<&str> = hit.pipelines.iter().map(|p| p.id.as_str()).collect();
        assert!(
            reached.contains(&"load") && reached.contains(&"export"),
            "the blast radius omitted a pipeline that reads the asset: {reached:?}"
        );
    }

    /// Two directories on one FTP server are two assets.
    #[test]
    fn an_ftp_path_is_named_not_just_the_server_it_sits_on() {
        // src.ftp requires host + directory; snk.ftp requires host + remotePath.
        // Naming only the host made every path on one server one asset, so two
        // unrelated pipelines looked connected.
        let inbox = asset_of("src.ftp", &json!({ "host": "files.internal", "directory": "/in/orders" })).unwrap();
        let archive = asset_of("src.ftp", &json!({ "host": "files.internal", "directory": "/in/archive" })).unwrap();
        assert_ne!(inbox.id, archive.id, "two directories on one server were named as one asset");
        assert_eq!(inbox.id, "ftp://files.internal/in/orders");
        assert_eq!(inbox.kind, "file");

        // And a sink writing where a source reads is still the same asset.
        let written = asset_of("snk.ftp", &json!({ "host": "files.internal", "remotePath": "/in/orders" })).unwrap();
        assert_eq!(inbox.id, written.id, "a reader and a writer of one path disagree");
    }

    /// A path is not a connection string, however many '=' it contains.
    #[test]
    fn a_partitioned_path_is_left_alone() {
        // Hive-style partition names are '=' separated and a path may contain
        // ';'. Mistaking one for a DSN would rewrite the name of a real file.
        let a = asset_of("src.parquet", &json!({ "path": "/lake/dt=2026-08-15;run=1/orders.parquet" })).unwrap();
        assert_eq!(a.id, "/lake/dt=2026-08-15;run=1/orders.parquet");
    }

    /// The GUI writes a port as a number, and a number is still a port.
    #[test]
    fn two_instances_on_one_host_are_two_assets_even_when_the_port_is_a_number() {
        // manifest-synth declares port as `kind: 'integer'`, so every
        // GUI-authored node carries a JSON number here. Reading only strings
        // dropped it, and both of these collapsed onto postgres://db/sales...
        let a = asset_of(
            "src.postgres",
            &json!({ "host": "db", "port": 5432, "database": "sales", "schema": "public", "tableName": "orders" }),
        )
        .unwrap();
        let b = asset_of(
            "src.postgres",
            &json!({ "host": "db", "port": 5433, "database": "sales", "schema": "public", "tableName": "orders" }),
        )
        .unwrap();
        assert_eq!(a.id, "postgres://db:5432/sales.public.orders");
        assert_ne!(a.id, b.id, "two instances on one host were named as one asset");

        // A hand-written string port must still name the same asset as the
        // number the GUI writes, or the two authoring paths would disagree.
        let text = asset_of(
            "src.postgres",
            &json!({ "host": "db", "port": "5432", "database": "sales", "schema": "public", "tableName": "orders" }),
        )
        .unwrap();
        assert_eq!(a.id, text.id);
    }

    #[test]
    fn the_same_file_written_two_ways_is_one_asset() {
        let windows = asset_of("snk.parquet", &json!({ "path": "C:\\data\\orders.parquet" })).unwrap();
        let posix = asset_of("src.parquet", &json!({ "path": "c:/data/orders.parquet" })).unwrap();
        assert_eq!(windows.id, posix.id, "one file was counted as two assets");

        // A remote path keeps its scheme and is not a local file.
        let remote = asset_of("snk.s3", &json!({ "path": "s3://bucket/curated/orders.parquet" })).unwrap();
        assert_eq!(remote.kind, "object");
        assert_eq!(remote.id, "s3://bucket/curated/orders.parquet");
    }

    #[test]
    fn a_dated_path_stays_one_asset_rather_than_one_per_day() {
        // Collapsing the template is the point: otherwise a daily export looks
        // like a new, unread asset every morning and impact never finds it.
        let a = asset_of("snk.csv", &json!({ "path": "/exports/orders_${date}.csv" })).unwrap();
        let b = asset_of("src.csv", &json!({ "path": "/exports/orders_${date}.csv" })).unwrap();
        assert_eq!(a.id, b.id);
        assert!(a.id.contains("${date}"), "the placeholder was expanded away");
    }

    #[test]
    fn streams_and_collections_and_endpoints_are_named() {
        let topic = asset_of("src.kafka", &json!({ "brokers": "a:9092,b:9092", "topic": "orders" })).unwrap();
        assert_eq!(topic.id, "kafka://a:9092/orders");
        assert_eq!(topic.kind, "topic");

        let coll = asset_of(
            "snk.mongodb",
            &json!({ "uri": "mongodb://m:27017/", "database": "sales", "collection": "orders" }),
        )
        .unwrap();
        assert_eq!(coll.id, "mongodb://m:27017/sales.orders");

        // Paging parameters must not split one endpoint into many assets.
        let api = asset_of("src.rest", &json!({ "url": "https://api.example.com/v1/orders?page=3" })).unwrap();
        assert_eq!(api.id, "https://api.example.com/v1/orders");
    }

    /// One case per connector family, using the property sets those families
    /// actually mark required in their shipped manifests.
    ///
    /// Measured against all 190 shipped sources and sinks by their required
    /// fields alone, these rules name 184. The six they miss are `src.clipboard`
    /// and `src.webhook`, which have no external target to name at all;
    /// `src.adbc` and `src.salesforce.bulk`, which are given a raw query rather
    /// than an address; and `src.duckdb` and `src.teradata`, which require
    /// nothing and are named at run time from whichever optional database or
    /// table is actually set. That last pair is why 184 is a floor rather than
    /// the real figure: this counts only what a manifest guarantees is present.
    #[test]
    fn every_connector_family_yields_a_name() {
        let cases: Vec<(&str, Value, &str)> = vec![
            ("src.csv", json!({ "path": "/in/a.csv" }), "file"),
            ("snk.gcs", json!({ "bucket": "warehouse", "key": "/curated/a.parquet" }), "object"),
            ("src.kafka", json!({ "brokers": "a:9092", "topic": "orders" }), "topic"),
            ("src.elastic", json!({ "endpoint": "http://es:9200", "index": "orders" }), "index"),
            ("src.qdrant", json!({ "collection": "embeddings" }), "collection"),
            ("snk.postgres", json!({ "host": "db", "database": "sales", "tableName": "orders" }), "table"),
            ("snk.lancedb", json!({ "uri": "/lake/lance", "table": "vectors" }), "table"),
            ("snk.salesforce", json!({ "object": "Account" }), "object"),
            ("src.rest", json!({ "url": "https://api.example.com/orders" }), "api"),
            ("src.sqlite", json!({ "database": "/data/app.db" }), "database"),
            ("src.clickhouse", json!({ "endpoint": "http://ch:8123" }), "database"),
            ("src.snowflake", json!({ "account": "acme-eu" }), "service"),
            ("src.cassandra", json!({ "contactPoints": "c1:9042,c2:9042" }), "service"),
        ];
        for (component, props, expected_kind) in cases {
            let asset = asset_of(component, &props)
                .unwrap_or_else(|e| panic!("{component} could not be named: {e}"));
            assert_eq!(asset.kind, expected_kind, "{component} landed in the wrong family");
            assert!(!asset.id.is_empty(), "{component} produced an empty name");
            // A doubled separator means a part was formatted in as nothing,
            // which produces a name nothing else will match.
            if let Some((_, rest)) = asset.id.split_once("://") {
                assert!(!rest.contains("//"), "{component} has an empty segment: {}", asset.id);
            }
            assert!(!asset.id.ends_with('/'), "{component} name ends in a slash: {}", asset.id);
        }
    }

    /// The exact strings, for the shapes where getting them subtly wrong would
    /// still look reasonable in a list and silently fail to join.
    ///
    /// A target with no authority is named with one pair of slashes. An earlier
    /// version produced `salesforce:///Account`, which reads fine and is even
    /// valid as a URI, but is a different string from the one every other
    /// authority-less target gets, and a join key that is only nearly right is
    /// a graph with a missing edge.
    #[test]
    fn names_are_exactly_these() {
        let cases: Vec<(&str, Value, &str)> = vec![
            ("snk.salesforce", json!({ "object": "Account" }), "salesforce://Account"),
            ("src.qdrant", json!({ "collection": "embeddings" }), "qdrant://embeddings"),
            (
                "snk.postgres",
                json!({ "host": "db", "port": "5432", "database": "sales", "schema": "public", "tableName": "orders" }),
                "postgres://db:5432/sales.public.orders",
            ),
            (
                "snk.gcs",
                json!({ "bucket": "warehouse", "key": "/curated/a.parquet" }),
                "gcs://warehouse/curated/a.parquet",
            ),
            // A uri that is a local path keeps it, which is the ordinary
            // authority-less URI form and stays stable across pipelines.
            (
                "snk.lancedb",
                json!({ "uri": "/lake/lance", "table": "vectors" }),
                "lancedb:///lake/lance/vectors",
            ),
        ];
        for (component, props, expected) in cases {
            assert_eq!(asset_of(component, &props).unwrap().id, expected, "for {component}");
        }
    }

    #[test]
    fn a_node_with_no_recognisable_target_is_reported_not_dropped() {
        let err = asset_of("src.somethingnew", &json!({ "flavour": "vanilla" })).unwrap_err();
        assert!(err.contains("src.somethingnew"), "the reason must name the component: {err}");
    }

    #[test]
    fn two_pipelines_sharing_a_table_are_connected_without_anyone_linking_them() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path();
        let staged = json!({ "path": "/lake/staged.parquet" });

        write_pipeline(
            ws,
            "ingest",
            json!([
                node("n1", "src.postgres", json!({ "host": "db", "database": "sales", "tableName": "orders" })),
                node("n2", "snk.parquet", staged.clone()),
            ]),
        );
        write_pipeline(
            ws,
            "report",
            json!([
                node("n1", "src.parquet", staged),
                node("n2", "snk.csv", json!({ "path": "/out/report.csv" })),
            ]),
        );

        let cat = build(ws).unwrap();
        assert_eq!(cat.pipelines.len(), 2);
        assert_eq!(cat.assets.len(), 3, "expected orders, staged and report");
        assert!(cat.unresolved.is_empty(), "unexpected unresolved: {:?}", cat.unresolved);

        // Nothing in either pipeline file references the other. The connection
        // exists only because they name the same parquet.
        let producers = cat.producers("/lake/staged.parquet");
        let consumers = cat.consumers("/lake/staged.parquet");
        assert_eq!(producers.len(), 1);
        assert_eq!(producers[0].pipeline_id, "ingest");
        assert_eq!(consumers.len(), 1);
        assert_eq!(consumers[0].pipeline_id, "report");
    }

    #[test]
    fn impact_reaches_across_pipelines_and_reports_distance() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path();
        write_pipeline(
            ws,
            "ingest",
            json!([
                node("a", "src.postgres", json!({ "host": "db", "database": "sales", "tableName": "orders" })),
                node("b", "snk.parquet", json!({ "path": "/lake/staged.parquet" })),
            ]),
        );
        write_pipeline(
            ws,
            "enrich",
            json!([
                node("a", "src.parquet", json!({ "path": "/lake/staged.parquet" })),
                node("b", "snk.parquet", json!({ "path": "/lake/enriched.parquet" })),
            ]),
        );
        write_pipeline(
            ws,
            "report",
            json!([
                node("a", "src.parquet", json!({ "path": "/lake/enriched.parquet" })),
                node("b", "snk.csv", json!({ "path": "/out/report.csv" })),
            ]),
        );

        let cat = build(ws).unwrap();
        let hit = cat.impact("postgres://db/sales.orders", None);

        // Dropping a column in the source table reaches all three pipelines,
        // two of which never mention Postgres anywhere.
        let names: Vec<&str> = hit.pipelines.iter().map(|p| p.id.as_str()).collect();
        assert_eq!(names, vec!["enrich", "ingest", "report"]);
        let depth = |id: &str| hit.pipelines.iter().find(|p| p.id == id).unwrap().depth;
        assert_eq!(depth("ingest"), 1, "the pipeline reading it directly is one hop");
        assert_eq!(depth("enrich"), 2);
        assert_eq!(depth("report"), 3);
        assert!(hit.assets.iter().any(|a| a.id == "/out/report.csv"));
    }

    #[test]
    fn a_pipeline_that_reads_and_writes_the_same_asset_does_not_hang_the_walk() {
        // The normal incremental pattern: read a table, write it back. A walk
        // without a visited set never returns from this.
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path();
        write_pipeline(
            ws,
            "accumulate",
            json!([
                node("a", "src.parquet", json!({ "path": "/lake/running.parquet" })),
                node("b", "snk.parquet", json!({ "path": "/lake/running.parquet" })),
            ]),
        );
        let cat = build(ws).unwrap();
        let hit = cat.impact("/lake/running.parquet", None);
        assert_eq!(hit.pipelines.len(), 1);
    }

    #[test]
    fn nodes_that_cannot_be_named_are_counted_on_the_answer() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path();
        write_pipeline(
            ws,
            "partly-known",
            json!([
                node("a", "src.parquet", json!({ "path": "/lake/in.parquet" })),
                node("b", "snk.mysterybox", json!({ "wat": "?" })),
            ]),
        );
        let cat = build(ws).unwrap();
        assert_eq!(cat.unresolved.len(), 1);
        assert_eq!(cat.unresolved[0].node_id, "b");

        // The count rides along on impact, so a caller cannot show the result
        // as exhaustive without also seeing that something was missed.
        assert_eq!(cat.impact("/lake/in.parquet", None).unresolved, 1);
    }

    #[test]
    fn orphans_are_written_but_unread_and_externals_are_read_but_unwritten() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path();
        write_pipeline(
            ws,
            "one",
            json!([
                node("a", "src.csv", json!({ "path": "/in/upstream.csv" })),
                node("b", "snk.csv", json!({ "path": "/out/nobody-reads-this.csv" })),
            ]),
        );
        let cat = build(ws).unwrap();
        let orphans: Vec<&str> = cat.orphans().iter().map(|a| a.id.as_str()).collect();
        assert_eq!(orphans, vec!["/out/nobody-reads-this.csv"]);
        let externals: Vec<&str> = cat.externals().iter().map(|a| a.id.as_str()).collect();
        assert_eq!(externals, vec!["/in/upstream.csv"]);
    }

    #[test]
    fn the_first_matching_rule_wins_so_specific_beats_general() {
        // A file read top to bottom should behave the way it reads. Putting the
        // narrow rule first is how anyone carves an exception out of a broad
        // one, and last-match-wins would silently invert that.
        let owners = Owners {
            assets: vec![
                OwnerRule {
                    pattern: "/lake/raw/pii_*".into(),
                    owner: "Privacy".into(),
                    contact: Some("privacy@acme.test".into()),
                },
                OwnerRule {
                    pattern: "/lake/raw/*".into(),
                    owner: "Data Platform".into(),
                    contact: None,
                },
            ],
            pipelines: vec![OwnerRule {
                pattern: "*-ingest-*".into(),
                owner: "Ingest".into(),
                contact: None,
            }],
        };
        assert_eq!(owners.for_asset("/lake/raw/pii_customers.parquet").unwrap().owner, "Privacy");
        assert_eq!(owners.for_asset("/lake/raw/orders.parquet").unwrap().owner, "Data Platform");
        assert!(owners.for_asset("/exports/report.csv").is_none(), "matched something it should not");
        assert_eq!(owners.for_pipeline("01-ingest-orders").unwrap().owner, "Ingest");
    }

    #[test]
    fn a_pattern_that_will_not_compile_owns_nothing() {
        // The dangerous failure is the other way round: a typo that matches
        // everything would hand one team the whole workspace and read as though
        // ownership were complete.
        let owners = Owners {
            assets: vec![OwnerRule {
                pattern: "[unclosed".into(),
                owner: "Nobody".into(),
                contact: None,
            }],
            pipelines: vec![],
        };
        assert!(owners.for_asset("/lake/raw/orders.parquet").is_none());
        assert!(owners.for_asset("anything at all").is_none());
    }

    #[test]
    fn impact_says_who_to_tell_and_unowned_says_what_nobody_claims() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path();
        write_pipeline(
            ws,
            "ingest",
            json!([
                node("a", "src.postgres", json!({ "host": "db", "database": "sales", "tableName": "orders" })),
                node("b", "snk.parquet", json!({ "path": "/lake/raw/orders.parquet" })),
            ]),
        );
        write_pipeline(
            ws,
            "report",
            json!([
                node("a", "src.parquet", json!({ "path": "/lake/raw/orders.parquet" })),
                node("b", "snk.csv", json!({ "path": "/exports/report.csv" })),
            ]),
        );
        std::fs::write(
            owners_path(ws),
            json!({
                "assets": [{ "match": "/lake/raw/*", "owner": "Data Platform", "contact": "dp@acme.test" }],
                "pipelines": [{ "match": "report", "owner": "Analytics" }],
            })
            .to_string(),
        )
        .unwrap();

        let cat = build(ws).unwrap();
        let owners = load_owners(ws).unwrap();
        let hit = cat.impact("postgres://db/sales.orders", Some(&owners));

        let owner_of = |id: &str| {
            hit.pipelines
                .iter()
                .chain(hit.assets.iter())
                .find(|r| r.id == id)
                .unwrap_or_else(|| panic!("{id} was not reached"))
                .owner
                .clone()
        };
        assert_eq!(owner_of("report").as_deref(), Some("Analytics"));
        assert_eq!(owner_of("/lake/raw/orders.parquet").as_deref(), Some("Data Platform"));
        // No rule covers the ingest pipeline or the export, and saying so is
        // the point: a blank owner is a finding, not a formatting problem.
        assert_eq!(owner_of("ingest"), None);
        assert_eq!(owner_of("/exports/report.csv"), None);

        let unowned: Vec<&str> = cat.unowned(&owners).iter().map(|a| a.id.as_str()).collect();
        assert_eq!(unowned, vec!["/exports/report.csv", "postgres://db/sales.orders"]);
    }

    #[test]
    fn a_workspace_with_no_owners_file_reports_everything_unowned() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path();
        write_pipeline(ws, "p", json!([node("a", "src.csv", json!({ "path": "/in/a.csv" }))]));
        let owners = load_owners(ws).unwrap();
        assert!(owners.is_empty());
        assert_eq!(build(ws).unwrap().unowned(&owners).len(), 1);

        // A file that will not parse must not read as "nobody owns anything",
        // which is exactly what an empty result would look like.
        std::fs::write(owners_path(ws), b"{ not json").unwrap();
        assert!(load_owners(ws).is_err());
    }

    #[test]
    fn a_built_catalog_round_trips_through_disk() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path();
        assert!(load(ws).unwrap().is_none(), "nothing built yet");
        write_pipeline(ws, "p", json!([node("a", "src.csv", json!({ "path": "/in/a.csv" }))]));

        let built = build_and_save(ws).unwrap();
        let loaded = load(ws).unwrap().expect("saved catalog");
        assert_eq!(loaded.assets, built.assets);
        assert_eq!(loaded.touches, built.touches);
    }
}
