//! Where a workspace can be deployed, and how to send a pipeline there.
//!
//! Authoring happens on a laptop and running happens on a server, and until now nothing
//! joined the two: the desktop could commit and push, the server could not pull, so a
//! pipeline reached production by external CI, a cron, or somebody copying a file.
//!
//! A target is a named server plus a credential for it. The credential is an API key
//! minted by that server (`duckle-runner console key-add`), which is the right shape here:
//! it belongs to this machine rather than to a person, it carries its own role, and it can
//! be revoked centrally without touching anyone's account.
//!
//! The key is encrypted at rest with the workspace key, the same protection saved
//! connections get, and **it is never returned to the front end**. The UI needs to know
//! which targets exist, not what they are authenticated with, so listing returns names and
//! URLs only. A token that reaches the browser layer is a token in a log, a crash report
//! and a screenshot.

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

/// One deployment target, as stored.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Target {
    name: String,
    url: String,
    /// The API key, encrypted with the workspace key. Never leaves this module in clear.
    key_enc: String,
}

/// What the UI is allowed to see: which targets exist, and nothing about their keys.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TargetInfo {
    pub name: String,
    pub url: String,
}

fn targets_path(workspace: &Path) -> PathBuf {
    workspace.join(".duckle").join("deploy-targets.json")
}

fn read_targets(workspace: &Path) -> Result<Vec<Target>, String> {
    let p = targets_path(workspace);
    if !p.exists() {
        return Ok(Vec::new());
    }
    let text = std::fs::read_to_string(&p).map_err(|e| format!("read {}: {e}", p.display()))?;
    // A file that will not parse is an error rather than an empty list: silently forgetting
    // where someone deploys is worse than saying the file is broken.
    serde_json::from_str(&text).map_err(|e| format!("parse {}: {e}", p.display()))
}

fn write_targets(workspace: &Path, targets: &[Target]) -> Result<(), String> {
    let p = targets_path(workspace);
    if let Some(dir) = p.parent() {
        std::fs::create_dir_all(dir).map_err(|e| format!("create {}: {e}", dir.display()))?;
    }
    let body = serde_json::to_string_pretty(targets).map_err(|e| e.to_string())?;
    std::fs::write(&p, body).map_err(|e| format!("write {}: {e}", p.display()))
}

/// Normalise what someone typed into a base URL we can build routes on.
fn clean_url(raw: &str) -> Result<String, String> {
    let url = raw.trim().trim_end_matches('/');
    if url.is_empty() {
        return Err("a target needs a URL".into());
    }
    if !url.starts_with("http://") && !url.starts_with("https://") {
        return Err(format!("{url} needs to start with http:// or https://"));
    }
    Ok(url.to_string())
}

/// Save (or replace) a target. The key is encrypted before it touches the disk.
pub fn save_target(workspace: &Path, name: &str, url: &str, key: &str) -> Result<(), String> {
    let name = name.trim();
    if name.is_empty() {
        return Err("a target needs a name".into());
    }
    let url = clean_url(url)?;
    if key.trim().is_empty() {
        return Err("a target needs an API key from that server".into());
    }
    let wk = duckle_secrets::workspace_key(workspace, true)?;
    let key_enc = duckle_secrets::encrypt_value(&wk, key.trim())?;

    let mut targets = read_targets(workspace)?;
    targets.retain(|t| t.name != name);
    targets.push(Target { name: name.to_string(), url, key_enc });
    targets.sort_by(|a, b| a.name.cmp(&b.name));
    write_targets(workspace, &targets)
}

pub fn remove_target(workspace: &Path, name: &str) -> Result<bool, String> {
    let mut targets = read_targets(workspace)?;
    let before = targets.len();
    targets.retain(|t| t.name != name);
    let removed = targets.len() != before;
    if removed {
        write_targets(workspace, &targets)?;
    }
    Ok(removed)
}

/// Every target, without their keys.
pub fn list_targets(workspace: &Path) -> Result<Vec<TargetInfo>, String> {
    Ok(read_targets(workspace)?
        .into_iter()
        .map(|t| TargetInfo { name: t.name, url: t.url })
        .collect())
}

fn target_credential(workspace: &Path, name: &str) -> Result<(String, String), String> {
    let target = read_targets(workspace)?
        .into_iter()
        .find(|t| t.name == name)
        .ok_or_else(|| format!("no deployment target called '{name}'"))?;
    let wk = duckle_secrets::workspace_key(workspace, false)?;
    let key = duckle_secrets::decrypt_value(&wk, &target.key_enc)
        .map_err(|e| format!("could not read the key for '{name}': {e}"))?;
    Ok((target.url, key))
}

/// Ask a target who we are, which is the only way to find out before deploying whether the
/// URL is right, the server is up, and the key still works.
pub fn check_target(workspace: &Path, name: &str) -> Result<Value, String> {
    let (url, key) = target_credential(workspace, name)?;
    let resp = http_client()?
        .get(format!("{url}/api/whoami"))
        .bearer_auth(&key)
        .send()
        .map_err(|e| format!("could not reach {url}: {e}"))?;
    let status = resp.status();
    let body = resp.text().unwrap_or_default();
    if status == reqwest::StatusCode::UNAUTHORIZED {
        return Err(format!(
            "{url} did not accept this key. It may have been revoked, or it may have expired."
        ));
    }
    if !status.is_success() {
        return Err(format!("{url} answered {status}: {}", body.trim()));
    }
    Ok(serde_json::from_str(&body).unwrap_or_else(|_| json!({ "ok": true })))
}

/// Send a pipeline to a target, with the schedule it should eventually run on.
///
/// The schedule arrives switched off at the far end; that is the server's rule, not this
/// one's, and it is stated here because it is the behaviour a person is about to see.
pub fn deploy(
    workspace: &Path,
    target: &str,
    name: &str,
    pipeline: &Value,
    schedule: Option<&Value>,
) -> Result<Value, String> {
    let (url, key) = target_credential(workspace, target)?;
    let mut body = json!({ "name": name, "pipeline": pipeline });
    if let Some(s) = schedule {
        body["schedule"] = s.clone();
    }
    // Serialised here rather than through reqwest's json feature, which this build does
    // not enable and which is not worth turning on for one call.
    let payload = serde_json::to_string(&body).map_err(|e| e.to_string())?;
    let resp = http_client()?
        .post(format!("{url}/api/deploy"))
        .bearer_auth(&key)
        .header("Content-Type", "application/json")
        .body(payload)
        .send()
        .map_err(|e| format!("could not reach {url}: {e}"))?;
    let status = resp.status();
    let text = resp.text().unwrap_or_default();
    match status {
        s if s.is_success() => Ok(serde_json::from_str(&text).unwrap_or_else(|_| json!({ "ok": true }))),
        reqwest::StatusCode::UNAUTHORIZED => Err(format!(
            "{url} did not accept this key. It may have been revoked, or it may have expired."
        )),
        reqwest::StatusCode::FORBIDDEN => Err(format!(
            "this key may not deploy to {url}. Deploying needs the admin role, because a \
             deployed pipeline runs on that host; the key can be replaced with \
             `duckle-runner console key-add <label> --role admin` on the server."
        )),
        s => Err(format!("{url} answered {s}: {}", text.trim())),
    }
}

fn http_client() -> Result<reqwest::blocking::Client, String> {
    reqwest::blocking::Client::builder()
        // A deploy that hangs forever looks like a frozen app, so it fails instead.
        .timeout(std::time::Duration::from_secs(30))
        .build()
        .map_err(|e| format!("http client: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_saved_key_is_never_written_in_clear() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path();
        let key = "duckle_averysecretkeyvalue";

        save_target(ws, "prod", "https://duckle.internal", key).expect("saves");

        let written = std::fs::read_to_string(targets_path(ws)).unwrap();
        assert!(!written.contains(key), "the key was written in clear");
        assert!(written.contains("https://duckle.internal"), "the URL should be readable");
    }

    #[test]
    fn listing_targets_never_hands_out_a_key() {
        // The UI needs to know which servers exist, not what authenticates to them. A
        // token that reaches the front end is a token in a log and a screenshot.
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path();
        save_target(ws, "prod", "https://duckle.internal", "duckle_secret").expect("saves");

        let listed = list_targets(ws).expect("lists");
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].name, "prod");
        let as_json = serde_json::to_string(&listed).unwrap();
        assert!(!as_json.contains("duckle_secret"), "a key reached the UI payload");
        assert!(!as_json.contains("keyEnc"), "even the ciphertext should not travel");
    }

    #[test]
    fn a_target_is_replaced_rather_than_duplicated() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path();
        save_target(ws, "prod", "https://one.internal", "k1").unwrap();
        save_target(ws, "prod", "https://two.internal", "k2").unwrap();

        let listed = list_targets(ws).unwrap();
        assert_eq!(listed.len(), 1, "saving the same name twice made two targets");
        assert_eq!(listed[0].url, "https://two.internal");
    }

    #[test]
    fn a_url_has_to_look_like_one() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path();
        for bad in ["", "   ", "duckle.internal", "ftp://duckle.internal"] {
            assert!(
                save_target(ws, "prod", bad, "k").is_err(),
                "{bad:?} was accepted as a URL"
            );
        }
        // A trailing slash is a typo, not an error: routes are built onto this.
        save_target(ws, "prod", "https://duckle.internal/", "k").unwrap();
        assert_eq!(list_targets(ws).unwrap()[0].url, "https://duckle.internal");
    }

    #[test]
    fn a_target_with_no_key_is_refused() {
        let tmp = tempfile::tempdir().unwrap();
        assert!(save_target(tmp.path(), "prod", "https://x.internal", "  ").is_err());
    }

    #[test]
    fn removing_a_target_forgets_it() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path();
        save_target(ws, "prod", "https://x.internal", "k").unwrap();

        assert!(remove_target(ws, "prod").unwrap());
        assert!(list_targets(ws).unwrap().is_empty());
        assert!(!remove_target(ws, "prod").unwrap(), "removing twice should say so");
    }

    #[test]
    fn deploying_to_a_target_that_does_not_exist_says_which_one() {
        let tmp = tempfile::tempdir().unwrap();
        let err = deploy(tmp.path(), "staging", "p", &json!({"nodes":[]}), None).unwrap_err();
        assert!(err.contains("staging"), "unhelpful error: {err}");
    }
}
