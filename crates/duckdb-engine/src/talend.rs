//! Talend job (`.item`) importer.
//!
//! Reads the XML a Talend Studio job is stored as and produces a Duckle
//! pipeline. This is an interoperability reader for a file format: it parses
//! their data, never their code, and nothing here is derived from their
//! implementation.
//!
//! Coverage is deliberately the head of the distribution rather than the whole
//! catalogue. Talend ships 900+ components, but real jobs use a couple of
//! dozen: across a 44-job corpus only 16 distinct components appeared, and the
//! three hardest of them (the mapper, the child-job call, the parallel branch)
//! already have Duckle equivalents. Everything outside the table below is
//! reported as an unmapped node, never silently dropped, because a migration
//! that quietly loses a step is worse than one that refuses it.
//!
//! Three things deliberately do NOT convert:
//!   * Encrypted passwords (`enc:system.encryption.key.v1:...`). We cannot read
//!     them and would not want to bake them into a file if we could, so the
//!     property becomes an `${ENV:...}` placeholder and the run is reported.
//!   * Repository connections (`PROPERTY_TYPE=REPOSITORY`), where the host and
//!     credentials live in a separate repository item, not in the job. The job
//!     alone does not contain enough to connect.
//!   * Java expressions in mapper outputs (`TalendDate.getCurrentDate()`,
//!     `context.getProperty(..)`). A plain `Table.Column` reference maps to a
//!     column; anything else is reported for a human to translate.

use duckle_metadata::{EdgeData, NodeData, PipelineEdge, PipelineNode, Position};
use quick_xml::events::Event;
use quick_xml::Reader;
use serde_json::{Map as JsonMap, Value as JsonValue};
use std::collections::BTreeMap;

/// A component Duckle could not translate, or a value it refused to guess.
#[derive(Debug, Clone, PartialEq)]
pub enum Warning {
    /// No Duckle equivalent for this Talend component.
    UnmappedComponent { node: String, component: String },
    /// Host/credentials live in a repository item, not in this job file.
    RepositoryConnection { node: String, component: String },
    /// Password is encrypted with a Studio key; emitted as a placeholder.
    EncryptedSecret { node: String, property: String, placeholder: String },
    /// A mapper output expression that is Java, not a column reference.
    JavaExpression { node: String, column: String, expression: String },
}

impl std::fmt::Display for Warning {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Warning::UnmappedComponent { node, component } => write!(
                f,
                "{node}: no Duckle equivalent for {component}; the node was imported as a \
                 placeholder and needs replacing by hand"
            ),
            Warning::RepositoryConnection { node, component } => write!(
                f,
                "{node} ({component}) uses a repository connection, so its host and credentials \
                 are not in this job file. Fill them in, or point the node at a saved connection"
            ),
            Warning::EncryptedSecret { node, property, placeholder } => write!(
                f,
                "{node}: {property} is encrypted with a Studio key and cannot be read. Set \
                 {placeholder} in the environment before running"
            ),
            Warning::JavaExpression { node, column, expression } => write!(
                f,
                "{node}: output column {column} is computed by Java (`{expression}`), which does \
                 not translate. Rewrite it as a SQL expression"
            ),
        }
    }
}

/// The result of reading one `.item` file.
#[derive(Debug)]
pub struct Import {
    /// Job name, taken from the file stem.
    pub name: String,
    pub nodes: Vec<PipelineNode>,
    pub edges: Vec<PipelineEdge>,
    /// Everything a human still has to resolve. Empty means a clean import.
    pub warnings: Vec<Warning>,
    /// Talend component name -> how many of them were seen.
    pub components: BTreeMap<String, usize>,
}

impl Import {
    /// Serialise to the same pipeline JSON the canvas and the runner read.
    pub fn to_pipeline_json(&self) -> JsonValue {
        serde_json::json!({
            "name": self.name,
            "nodes": self.nodes,
            "edges": self.edges,
        })
    }
}

/// One `<node>` as read from the file, before mapping.
struct RawNode {
    component: String,
    /// Talend's `UNIQUE_NAME`, e.g. `tDBInput_1`. Used as the Duckle node id
    /// because it is already unique within the job and appears verbatim in the
    /// `<connection>` elements.
    unique: String,
    params: BTreeMap<String, String>,
    /// Mapper output expressions, keyed by output column.
    mapper_out: Vec<(String, String)>,
    x: f64,
    y: f64,
}

/// Talend component -> (Duckle component id, React Flow node type).
///
/// `tDBInput`/`tDBOutput` are the modern generic forms; the concrete database
/// comes from the node's own `TYPE` parameter, so they are resolved separately
/// in [`map_component`].
fn static_map(component: &str) -> Option<(&'static str, &'static str)> {
    Some(match component {
        "tMysqlInput" => ("src.mysql", "source"),
        "tMysqlOutput" => ("snk.mysql", "sink"),
        "tOracleInput" => ("src.oracle", "source"),
        "tOracleOutput" => ("snk.oracle", "sink"),
        "tMSSqlInput" => ("src.sqlserver", "source"),
        "tMSSqlOutput" => ("snk.sqlserver", "sink"),
        "tPostgresqlInput" => ("src.postgres", "source"),
        "tPostgresqlOutput" => ("snk.postgres", "sink"),
        "tFileInputDelimited" => ("src.csv", "source"),
        "tFileOutputDelimited" => ("snk.csv", "sink"),
        "tFileInputExcel" => ("src.excel", "source"),
        "tFileOutputExcel" => ("snk.excel", "sink"),
        "tMap" => ("xf.map", "transform"),
        "tRunJob" => ("ctl.runjob", "transform"),
        "tUniqRow" => ("qa.unique", "transform"),
        "tParallelize" => ("ctl.parallelize", "transform"),
        "tConvertType" => ("xf.cast", "transform"),
        // Passes rows through and prints them, which is what tLogRow does.
        "tLogRow" => ("xf.log", "transform"),
        // A raw SQL statement against the connection, whatever the family.
        "tMysqlRow" | "tOracleRow" | "tMSSqlRow" | "tPostgresqlRow" => ("code.sql", "transform"),
        // Talend's SCD components write a type-2 dimension.
        "tMysqlSCD" | "tOracleSCD" | "tMSSqlSCD" | "tPostgresqlSCD" => ("xf.cdc.scd2", "transform"),
        _ => return None,
    })
}

/// Resolve the generic `tDBInput` / `tDBOutput` via the node's `TYPE` value.
fn map_component(raw: &RawNode) -> Option<(&'static str, &'static str)> {
    if let Some(hit) = static_map(&raw.component) {
        return Some(hit);
    }
    let family = raw.params.get("TYPE").map(|s| unquote(s).to_uppercase());
    let out = matches!(raw.component.as_str(), "tDBOutput");
    match (raw.component.as_str(), family.as_deref()) {
        ("tDBInput" | "tDBOutput", Some(fam)) => Some(match (fam, out) {
            ("MYSQL", false) => ("src.mysql", "source"),
            ("MYSQL", true) => ("snk.mysql", "sink"),
            ("ORACLE", false) => ("src.oracle", "source"),
            ("ORACLE", true) => ("snk.oracle", "sink"),
            ("MSSQL", false) => ("src.sqlserver", "source"),
            ("MSSQL", true) => ("snk.sqlserver", "sink"),
            ("POSTGRESQL", false) => ("src.postgres", "source"),
            ("POSTGRESQL", true) => ("snk.postgres", "sink"),
            _ => return None,
        }),
        _ => None,
    }
}

/// Talend stores parameter values as Java source, so a string literal arrives
/// wrapped in quotes. Strip one balanced pair; leave anything else (a bare
/// number, a `context.x` reference, an expression) untouched.
fn unquote(v: &str) -> String {
    let t = v.trim();
    if t.len() >= 2 && t.starts_with('"') && t.ends_with('"') {
        t[1..t.len() - 1].to_string()
    } else {
        t.to_string()
    }
}

/// True for a value we must not copy into a pipeline file.
fn is_encrypted(v: &str) -> bool {
    v.trim_matches('"').starts_with("enc:")
}

/// `context.foo` and `context.getProperty("foo")` become Duckle's `${foo}`, so
/// an imported job keeps using a context variable rather than freezing a value.
fn rewrite_context(v: &str) -> Option<String> {
    let t = v.trim();
    if let Some(rest) = t.strip_prefix("context.getProperty(") {
        let name = rest.trim_end_matches(')').trim().trim_matches('"');
        if !name.is_empty() {
            return Some(format!("${{{name}}}"));
        }
    }
    if let Some(name) = t.strip_prefix("context.") {
        let name = name.trim();
        if !name.is_empty() && name.chars().all(|c| c.is_alphanumeric() || c == '_') {
            return Some(format!("${{{name}}}"));
        }
    }
    None
}

/// Turn one Talend parameter into a Duckle property value, recording a warning
/// when the value cannot be carried across.
fn value_for(
    raw: &RawNode,
    key: &str,
    warnings: &mut Vec<Warning>,
) -> Option<JsonValue> {
    let raw_val = raw.params.get(key)?;
    if raw_val.trim().is_empty() {
        return None;
    }
    if is_encrypted(raw_val) {
        let placeholder = format!("${{ENV:{}_{}}}", raw.unique.to_uppercase(), key);
        warnings.push(Warning::EncryptedSecret {
            node: raw.unique.clone(),
            property: key.to_string(),
            placeholder: placeholder.clone(),
        });
        return Some(JsonValue::String(placeholder));
    }
    if let Some(ctx) = rewrite_context(raw_val) {
        return Some(JsonValue::String(ctx));
    }
    let v = unquote(raw_val);
    if v.is_empty() {
        return None;
    }
    Some(JsonValue::String(v))
}

/// Copy `(talend_key, duckle_key)` pairs into a property map.
fn copy_params(
    raw: &RawNode,
    pairs: &[(&str, &str)],
    props: &mut JsonMap<String, JsonValue>,
    warnings: &mut Vec<Warning>,
) {
    for (from, to) in pairs {
        if let Some(v) = value_for(raw, from, warnings) {
            props.insert((*to).to_string(), v);
        }
    }
}

/// Build the Duckle property map for one mapped node.
fn properties_for(
    raw: &RawNode,
    component_id: &str,
    warnings: &mut Vec<Warning>,
) -> JsonMap<String, JsonValue> {
    let mut props = JsonMap::new();
    match component_id {
        "src.mysql" | "snk.mysql" | "src.postgres" | "snk.postgres" => copy_params(
            raw,
            &[
                ("HOST", "host"),
                ("PORT", "port"),
                ("DBNAME", "database"),
                ("USER", "username"),
                ("PASS", "password"),
                ("TABLE", "tableName"),
            ],
            &mut props,
            warnings,
        ),
        "src.sqlserver" | "snk.sqlserver" => copy_params(
            raw,
            &[
                ("HOST", "host"),
                ("PORT", "port"),
                ("DBNAME", "database"),
                ("USER", "user"),
                ("PASS", "password"),
                ("DB_SCHEMA", "schema"),
                ("TABLE", "tableName"),
            ],
            &mut props,
            warnings,
        ),
        "src.oracle" | "snk.oracle" => {
            copy_params(
                raw,
                &[("USER", "user"), ("PASS", "password"), ("TABLE", "tableName")],
                &mut props,
                warnings,
            );
            // src.oracle wants one `connect` string rather than host/port/SID.
            let host = raw.params.get("HOST").map(|v| unquote(v)).unwrap_or_default();
            let port = raw.params.get("PORT").map(|v| unquote(v)).unwrap_or_default();
            let sid = raw
                .params
                .get("SID")
                .or_else(|| raw.params.get("SERVICE_NAME"))
                .or_else(|| raw.params.get("DBNAME"))
                .map(|v| unquote(v))
                .unwrap_or_default();
            if !host.is_empty() && !sid.is_empty() {
                let port = if port.is_empty() { "1521".to_string() } else { port };
                props.insert("connect".into(), JsonValue::String(format!("{host}:{port}/{sid}")));
            }
        }
        "src.csv" | "snk.csv" => {
            copy_params(raw, &[("FILENAME", "path")], &mut props, warnings);
            if let Some(sep) = value_for(raw, "FIELDSEPARATOR", warnings) {
                props.insert("delimiter".into(), sep);
            }
            // Talend counts header ROWS; Duckle asks whether there is a header.
            if let Some(h) = raw.params.get("HEADER") {
                let n: i64 = unquote(h).parse().unwrap_or(0);
                props.insert("hasHeader".into(), JsonValue::Bool(n > 0));
                if n > 1 {
                    props.insert("skipLines".into(), JsonValue::from(n - 1));
                }
            }
        }
        "src.excel" | "snk.excel" => {
            copy_params(raw, &[("FILENAME", "path")], &mut props, warnings);
        }
        "code.sql" => {
            copy_params(raw, &[("QUERY", "sql")], &mut props, warnings);
        }
        "ctl.runjob" => {
            copy_params(raw, &[("PROCESS", "pipelineRef")], &mut props, warnings);
        }
        "qa.unique" => {
            // Talend lists the key columns in a table parameter we do not walk
            // here; leaving `columns` unset makes validate fail loudly, which
            // is the right outcome for a value we cannot infer.
        }
        _ => {}
    }

    // A source with a hand-written query should carry it, whichever family.
    if component_id.starts_with("src.") {
        let query_key = if component_id == "src.mysql" || component_id == "src.postgres" {
            "sql"
        } else {
            "query"
        };
        if let Some(q) = value_for(raw, "QUERY", warnings) {
            let text = q.as_str().unwrap_or_default().trim().to_string();
            if !text.is_empty() {
                props.insert(query_key.to_string(), JsonValue::String(text));
                props.insert("mode".into(), JsonValue::String("query".into()));
            }
        }
    }
    props
}

/// Translate mapper output expressions. `Table.Column` becomes the column;
/// anything else is reported rather than guessed at.
fn mapper_expressions(raw: &RawNode, warnings: &mut Vec<Warning>) -> JsonValue {
    let mut out = JsonMap::new();
    for (col, expr) in &raw.mapper_out {
        let e = expr.trim();
        if e.is_empty() {
            continue;
        }
        let simple = e
            .split_once('.')
            .filter(|(t, c)| {
                !t.is_empty()
                    && !c.is_empty()
                    && t.chars().all(|ch| ch.is_alphanumeric() || ch == '_')
                    && c.chars().all(|ch| ch.is_alphanumeric() || ch == '_')
            })
            .map(|(_, c)| c.to_string());
        match simple {
            Some(c) => {
                out.insert(col.clone(), JsonValue::String(c));
            }
            None => warnings.push(Warning::JavaExpression {
                node: raw.unique.clone(),
                column: col.clone(),
                expression: e.to_string(),
            }),
        }
    }
    JsonValue::Object(out)
}

/// Read one Talend `.item` file.
pub fn import_item(xml: &str, job_name: &str) -> Result<Import, String> {
    let (raw_nodes, connections) = parse(xml)?;

    let mut components: BTreeMap<String, usize> = BTreeMap::new();
    for n in &raw_nodes {
        *components.entry(n.component.clone()).or_default() += 1;
    }

    let mut warnings = Vec::new();
    let mut nodes = Vec::new();

    for raw in &raw_nodes {
        // A repository connection means the credentials are not in this file.
        if raw.params.get("PROPERTY:PROPERTY_TYPE").map(|s| s.as_str()) == Some("REPOSITORY") {
            warnings.push(Warning::RepositoryConnection {
                node: raw.unique.clone(),
                component: raw.component.clone(),
            });
        }

        let (component_id, flow_type) = match map_component(raw) {
            Some(hit) => hit,
            None => {
                warnings.push(Warning::UnmappedComponent {
                    node: raw.unique.clone(),
                    component: raw.component.clone(),
                });
                // Import it as a labelled placeholder so the shape of the job
                // survives and the gap is visible on the canvas.
                nodes.push(PipelineNode {
                    id: raw.unique.clone(),
                    flow_type: Some("transform".into()),
                    position: Position { x: raw.x, y: raw.y },
                    data: node_data(format!("{} (unmapped)", raw.component), None, None),
                });
                continue;
            }
        };

        let mut props = properties_for(raw, component_id, &mut warnings);
        if component_id == "xf.map" {
            props.insert("expressions".into(), mapper_expressions(raw, &mut warnings));
        }

        nodes.push(PipelineNode {
            id: raw.unique.clone(),
            flow_type: Some(flow_type.into()),
            position: Position { x: raw.x, y: raw.y },
            data: node_data(
                raw.unique.clone(),
                Some(component_id.into()),
                Some(JsonValue::Object(props)),
            ),
        });
    }

    let known: std::collections::HashSet<&str> = nodes.iter().map(|n| n.id.as_str()).collect();
    let edges = connections
        .iter()
        .enumerate()
        // Drop dangling links rather than emit an edge to a node that is not
        // in the file; a dangling edge fails to compile with a worse message.
        .filter(|(_, c)| known.contains(c.source.as_str()) && known.contains(c.target.as_str()))
        .map(|(i, c)| PipelineEdge {
            id: format!("e{}", i + 1),
            source: c.source.clone(),
            target: c.target.clone(),
            source_handle: Some("main".into()),
            target_handle: Some("main".into()),
            edge_type: None,
            data: Some(EdgeData {
                connection_type: "main".into(),
                label: None,
                condition: None,
            }),
        })
        .collect();

    Ok(Import {
        name: job_name.to_string(),
        nodes,
        edges,
        warnings,
        components,
    })
}

/// `NodeData` has no `Default`, and an imported node only ever sets three of
/// its fields, so build it in one place rather than spelling out the rest twice.
fn node_data(label: String, component_id: Option<String>, properties: Option<JsonValue>) -> NodeData {
    NodeData {
        label,
        subtitle: None,
        component_id,
        properties,
        schema: None,
        sample_rows: None,
        disabled: None,
        alias: None,
    }
}

struct Conn {
    source: String,
    target: String,
}

/// Pull `<node>`, its `<elementParameter>`s, its mapper output entries, and the
/// `<connection>` list out of the job XML.
fn parse(xml: &str) -> Result<(Vec<RawNode>, Vec<Conn>), String> {
    let mut reader = Reader::from_str(xml);
    reader.config_mut().trim_text(true);

    let mut nodes: Vec<RawNode> = Vec::new();
    let mut conns: Vec<Conn> = Vec::new();
    let mut cur: Option<RawNode> = None;
    // Mapper entries are only outputs when we are inside <outputTables>.
    let mut in_output_table = false;
    let mut buf = Vec::new();

    loop {
        let ev = reader
            .read_event_into(&mut buf)
            .map_err(|e| format!("talend import: malformed XML at {}: {e}", reader.buffer_position()))?;
        match ev {
            Event::Eof => break,
            Event::Start(ref e) | Event::Empty(ref e) => {
                let name = e.local_name();
                let tag = String::from_utf8_lossy(name.as_ref()).to_string();
                // Values must be unescaped: Talend stores Java string literals,
                // so the quotes arrive as `&quot;` and a raw read would leave
                // `&quot;localhost&quot;` where `localhost` belongs.
                let attr = |k: &str| -> Option<String> {
                    e.attributes().flatten().find_map(|a| {
                        (a.key.local_name().as_ref() == k.as_bytes()).then(|| {
                            a.unescape_value()
                                .map(|v| v.into_owned())
                                .unwrap_or_else(|_| String::from_utf8_lossy(&a.value).to_string())
                        })
                    })
                };
                match tag.as_str() {
                    "node" => {
                        if let Some(done) = cur.take() {
                            nodes.push(done);
                        }
                        cur = Some(RawNode {
                            component: attr("componentName").unwrap_or_default(),
                            unique: String::new(),
                            params: BTreeMap::new(),
                            mapper_out: Vec::new(),
                            x: attr("posX").and_then(|v| v.parse().ok()).unwrap_or(0.0),
                            y: attr("posY").and_then(|v| v.parse().ok()).unwrap_or(0.0),
                        });
                        in_output_table = false;
                    }
                    "elementParameter" => {
                        if let (Some(n), Some(k)) = (cur.as_mut(), attr("name")) {
                            let v = attr("value").unwrap_or_default();
                            if k == "UNIQUE_NAME" {
                                n.unique = v.clone();
                            }
                            n.params.insert(k, v);
                        }
                    }
                    "outputTables" => in_output_table = true,
                    "inputTables" | "varTables" => in_output_table = false,
                    "mapperTableEntries" => {
                        if in_output_table {
                            if let (Some(n), Some(col)) = (cur.as_mut(), attr("name")) {
                                if let Some(expr) = attr("expression") {
                                    n.mapper_out.push((col, expr));
                                }
                            }
                        }
                    }
                    "connection" => {
                        if let (Some(s), Some(t)) = (attr("source"), attr("target")) {
                            conns.push(Conn { source: s, target: t });
                        }
                    }
                    _ => {}
                }
            }
            Event::End(ref e) => {
                let name = e.local_name();
                if name.as_ref() == b"node" {
                    if let Some(done) = cur.take() {
                        nodes.push(done);
                    }
                } else if name.as_ref() == b"outputTables" {
                    in_output_table = false;
                }
            }
            _ => {}
        }
        buf.clear();
    }
    if let Some(done) = cur.take() {
        nodes.push(done);
    }

    // A node with no UNIQUE_NAME cannot be referenced by a connection; fall
    // back to the component name plus an index so the import still holds.
    for (i, n) in nodes.iter_mut().enumerate() {
        if n.unique.is_empty() {
            n.unique = format!("{}_{}", n.component, i + 1);
        }
    }
    Ok((nodes, conns))
}

#[cfg(test)]
mod tests {
    use super::*;

    const JOB: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<talendfile:ProcessType xmlns:talendfile="platform:/resource/org.talend.model/model/TalendFile.xsd">
  <node componentName="tMSSqlInput" posX="100" posY="50">
    <elementParameter field="TEXT" name="UNIQUE_NAME" value="tDBInput_1"/>
    <elementParameter field="TECHNICAL" name="PROPERTY:PROPERTY_TYPE" value="REPOSITORY"/>
    <elementParameter field="TEXT" name="HOST" value="&quot;localhost&quot;"/>
    <elementParameter field="TEXT" name="PORT" value="&quot;1433&quot;"/>
    <elementParameter field="TEXT" name="DBNAME" value="&quot;AdventureWorks&quot;"/>
    <elementParameter field="TEXT" name="USER" value="&quot;sa&quot;"/>
    <elementParameter field="PASSWORD" name="PASS" value="enc:system.encryption.key.v1:AAAA"/>
    <elementParameter field="DBTABLE" name="TABLE" value="&quot;Location&quot;"/>
  </node>
  <node componentName="tMap" posX="300" posY="50">
    <elementParameter field="TEXT" name="UNIQUE_NAME" value="tMap_1"/>
    <outputTables name="dim_location">
      <mapperTableEntries name="LocationID" expression="Location.LocationID"/>
      <mapperTableEntries name="DI_Created_Date" expression="TalendDate.getCurrentDate()"/>
    </outputTables>
  </node>
  <node componentName="tSomethingExotic" posX="500" posY="50">
    <elementParameter field="TEXT" name="UNIQUE_NAME" value="tExotic_1"/>
  </node>
  <connection connectorName="FLOW" source="tDBInput_1" target="tMap_1" label="Location"/>
  <connection connectorName="FLOW" source="tMap_1" target="tExotic_1" label="out"/>
</talendfile:ProcessType>"#;

    #[test]
    fn maps_the_component_head_and_wires_the_flow() {
        let im = import_item(JOB, "dim_location").expect("parses");
        assert_eq!(im.nodes.len(), 3);
        assert_eq!(im.edges.len(), 2);
        let src = &im.nodes[0];
        assert_eq!(src.id, "tDBInput_1");
        assert_eq!(src.data.component_id.as_deref(), Some("src.sqlserver"));
        let p = src.data.properties.as_ref().unwrap();
        assert_eq!(p["host"], "localhost");
        assert_eq!(p["database"], "AdventureWorks");
        assert_eq!(p["tableName"], "Location");
    }

    #[test]
    fn an_encrypted_password_becomes_a_placeholder_not_a_guess() {
        let im = import_item(JOB, "j").unwrap();
        let p = im.nodes[0].data.properties.as_ref().unwrap();
        // Never the ciphertext, and never a blank that would look configured.
        assert_eq!(p["password"], "${ENV:TDBINPUT_1_PASS}");
        assert!(im
            .warnings
            .iter()
            .any(|w| matches!(w, Warning::EncryptedSecret { property, .. } if property == "PASS")));
    }

    #[test]
    fn a_repository_connection_is_reported_because_the_job_lacks_the_credentials() {
        let im = import_item(JOB, "j").unwrap();
        assert!(im
            .warnings
            .iter()
            .any(|w| matches!(w, Warning::RepositoryConnection { node, .. } if node == "tDBInput_1")));
    }

    #[test]
    fn an_unmapped_component_is_kept_as_a_placeholder_never_dropped() {
        let im = import_item(JOB, "j").unwrap();
        let exotic = im.nodes.iter().find(|n| n.id == "tExotic_1").expect("kept");
        assert_eq!(exotic.data.component_id, None);
        assert!(exotic.data.label.contains("unmapped"));
        assert!(im.warnings.iter().any(
            |w| matches!(w, Warning::UnmappedComponent { component, .. } if component == "tSomethingExotic")
        ));
    }

    #[test]
    fn column_refs_map_but_java_expressions_are_reported() {
        let im = import_item(JOB, "j").unwrap();
        let map = im.nodes.iter().find(|n| n.id == "tMap_1").unwrap();
        let exprs = &map.data.properties.as_ref().unwrap()["expressions"];
        assert_eq!(exprs["LocationID"], "LocationID");
        // The Java call must not be silently carried across as if it worked.
        assert!(exprs.get("DI_Created_Date").is_none());
        assert!(im.warnings.iter().any(
            |w| matches!(w, Warning::JavaExpression { column, .. } if column == "DI_Created_Date")
        ));
    }

    #[test]
    fn context_variables_survive_as_duckle_placeholders() {
        assert_eq!(rewrite_context("context.myVar").as_deref(), Some("${myVar}"));
        assert_eq!(
            rewrite_context("context.getProperty(\"vJobPID\")").as_deref(),
            Some("${vJobPID}")
        );
        assert_eq!(rewrite_context("\"literal\""), None);
    }

    /// Run the importer over a real Talend workspace and report coverage.
    ///
    /// Opt-in, because it needs a Studio workspace on disk:
    ///   DUCKLE_TALEND_CORPUS=C:/Talend/workspace cargo test -p duckle-duckdb-engine \
    ///     --lib talend -- --ignored --nocapture
    ///
    /// Synthetic fixtures prove the mapping compiles; only a real corpus shows
    /// what fraction of actual jobs survive it.
    #[test]
    #[ignore = "needs a Talend workspace; set DUCKLE_TALEND_CORPUS"]
    fn real_corpus_coverage() {
        let root = match std::env::var("DUCKLE_TALEND_CORPUS") {
            Ok(v) => std::path::PathBuf::from(v),
            Err(_) => return,
        };
        let mut items = Vec::new();
        let mut stack = vec![root];
        while let Some(dir) = stack.pop() {
            let Ok(rd) = std::fs::read_dir(&dir) else { continue };
            for e in rd.flatten() {
                let p = e.path();
                if p.is_dir() {
                    stack.push(p);
                } else if p.extension().and_then(|x| x.to_str()) == Some("item")
                    && p.to_string_lossy().contains("process")
                {
                    items.push(p);
                }
            }
        }
        assert!(!items.is_empty(), "no .item job files under the corpus root");

        let (mut jobs, mut nodes, mut mapped, mut failed) = (0usize, 0usize, 0usize, 0usize);
        let mut unmapped: BTreeMap<String, usize> = BTreeMap::new();
        for path in &items {
            let Ok(xml) = std::fs::read_to_string(path) else { continue };
            let name = path.file_stem().unwrap_or_default().to_string_lossy().to_string();
            match import_item(&xml, &name) {
                Ok(im) => {
                    jobs += 1;
                    nodes += im.nodes.len();
                    mapped += im.nodes.iter().filter(|n| n.data.component_id.is_some()).count();
                    for w in &im.warnings {
                        if let Warning::UnmappedComponent { component, .. } = w {
                            *unmapped.entry(component.clone()).or_default() += 1;
                        }
                    }
                }
                Err(e) => {
                    failed += 1;
                    eprintln!("  PARSE FAILED {}: {e}", path.display());
                }
            }
        }
        println!("\n  jobs parsed     : {jobs} of {} ({failed} failed)", items.len());
        println!("  nodes           : {nodes}");
        println!(
            "  mapped          : {mapped} ({:.1}%)",
            100.0 * mapped as f64 / nodes.max(1) as f64
        );
        println!("  unmapped kinds  : {unmapped:?}");
        assert_eq!(failed, 0, "every job in the corpus must at least parse");
    }

    #[test]
    fn the_generic_db_node_resolves_through_its_type_parameter() {
        let xml = r#"<talendfile:ProcessType xmlns:talendfile="x">
          <node componentName="tDBOutput">
            <elementParameter name="UNIQUE_NAME" value="out_1"/>
            <elementParameter name="TYPE" value="MYSQL"/>
            <elementParameter name="TABLE" value="&quot;dim&quot;"/>
          </node></talendfile:ProcessType>"#;
        let im = import_item(xml, "j").unwrap();
        assert_eq!(im.nodes[0].data.component_id.as_deref(), Some("snk.mysql"));
    }
}
