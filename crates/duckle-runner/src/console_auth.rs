//! Who may use the management console, and what they are allowed to do.
//!
//! The console can run any pipeline in the workspace, and a pipeline can run
//! shell and SQL, so reaching it is equivalent to running code on the host. It
//! used to have no authentication at all: binding it to `0.0.0.0` printed a
//! warning and then served anyone who connected.
//!
//! Loopback is left alone. A console on `127.0.0.1` is reachable only by
//! someone who is already on the machine, and requiring a token there would
//! break the ordinary local workflow to protect against an attacker who has
//! already won. Binding anywhere else now refuses to start without a
//! credential, so the exposure that mattered cannot happen silently.
//!
//! Roles are viewer, operator and admin. The split follows what an action can
//! destroy rather than which screen it lives on: reading is viewer, causing a
//! pipeline to run or changing when it runs is operator, and touching
//! credentials or the workspace itself is admin.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use argon2::password_hash::{PasswordHash, PasswordHasher, PasswordVerifier, SaltString};
use argon2::Argon2;
use base64::Engine as _;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// What a caller is allowed to do. Ordered: each role includes the ones before.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Role {
    /// Read the dashboard, runs and logs. Changes nothing.
    Viewer,
    /// Everything a viewer can do, plus run a pipeline and set its schedule.
    Operator,
    /// Everything an operator can do, plus credentials, connections and the
    /// workspace itself.
    Admin,
}

impl Role {
    fn rank(self) -> u8 {
        match self {
            Role::Viewer => 0,
            Role::Operator => 1,
            Role::Admin => 2,
        }
    }

    /// Whether this role satisfies a requirement of `needed`.
    pub fn allows(self, needed: Role) -> bool {
        self.rank() >= needed.rank()
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Role::Viewer => "viewer",
            Role::Operator => "operator",
            Role::Admin => "admin",
        }
    }
}

/// The caller behind one request, as far as the console can tell.
#[derive(Debug, Clone)]
pub struct Identity {
    /// Shown in the audit log. Never a token or any part of one.
    pub label: String,
    pub role: Role,
}

/// One console account, as stored. The token itself is never written down.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Account {
    label: String,
    role: Role,
    /// PHC-format Argon2id hash. A token may be chosen by a person, so it is
    /// treated as a password rather than as high-entropy key material.
    token_hash: String,
}

/// The console's authentication policy for one running server.
pub struct Console {
    accounts: Vec<Account>,
    /// True when nothing is configured and the bind is loopback, which is the
    /// unchanged local case: the caller is trusted because they are already on
    /// the machine.
    open: bool,
    /// Where the session store lives, so sessions outlive the process.
    workspace: PathBuf,
    /// hash(session id) -> session, minted at sign-in and persisted, so the browser never
    /// stores the token itself and a restart does not sign everyone out.
    sessions: Mutex<HashMap<String, Session>>,
    /// sha256(token) -> identity for tokens already verified once, so an API
    /// client polling every few seconds does not pay for an Argon2 hash on
    /// every request. Only ever populated with tokens that verified.
    verified: Mutex<HashMap<[u8; 32], Identity>>,
}

/// Deliberately hand-written: a derived one would print the stored token
/// hashes, and the first place a struct like this gets printed is an error
/// message or a log line.
impl std::fmt::Debug for Console {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Console")
            .field("accounts", &self.accounts.len())
            .field("open", &self.open)
            .finish()
    }
}

pub fn accounts_path(workspace: &Path) -> PathBuf {
    workspace.join(".duckle").join("console-users.json")
}

/// Where live sessions are kept, so a restart does not sign everyone out.
pub fn sessions_path(workspace: &Path) -> PathBuf {
    workspace.join(".duckle").join("console-sessions.json")
}

/// How long a session lasts before it has to be earned again. A working day, so nobody is
/// interrupted mid-shift and nobody holds a credential for a fortnight.
const SESSION_TTL_SECS: i64 = 12 * 60 * 60;

fn now_secs() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// A live session, as stored.
///
/// Keyed by a hash of the session id rather than the id itself. The store lives in the
/// workspace, which gets backed up and copied around, and a file of working credentials
/// has no business being in one. The browser holds the id; nothing else does, so a copy of
/// this file admits nobody.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct Session {
    label: String,
    role: Role,
    /// Unix seconds. Past this the session is refused and dropped.
    expires_at: i64,
}

/// What is written down for a session id: a plain SHA-256, unsalted on purpose. Salting
/// guards low-entropy secrets against a dictionary; a 256-bit random id has no dictionary.
fn session_key(sid: &str) -> String {
    let d: [u8; 32] = Sha256::digest(sid.as_bytes()).into();
    d.iter().map(|b| format!("{b:02x}")).collect()
}

/// Read the store, dropping anything already expired. A store that will not parse is an
/// empty one here: a session is a convenience, and refusing to start the console because a
/// cache went bad would trade a small problem for a total one.
fn read_sessions_file(workspace: &Path) -> HashMap<String, Session> {
    std::fs::read_to_string(sessions_path(workspace))
        .ok()
        .and_then(|raw| serde_json::from_str(&raw).ok())
        .unwrap_or_default()
}

/// Best effort: failing to write must not fail the sign-in, it only means the session does
/// not outlive this process, which is where it used to live anyway.
fn write_sessions_file(workspace: &Path, sessions: &HashMap<String, Session>) {
    let path = sessions_path(workspace);
    if let Some(parent) = path.parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    if let Ok(json) = serde_json::to_string(sessions) {
        let tmp = path.with_extension("json.tmp");
        if std::fs::write(&tmp, json).is_ok() {
            // Rename replaces on both platforms, so a reader never sees a partial file.
            let _ = std::fs::rename(&tmp, &path);
        }
    }
}

/// Change the stored sessions as one exclusive step, and return the merged result.
///
/// Writing the whole map from a copy held in memory is what a single process wants and
/// what two processes must never do: running `serve` for operations and `web` for
/// authoring against one workspace is a normal setup, and whichever signed in last erased
/// the other's sessions. So this re-reads under a lock, applies the change to what is
/// actually on disk, and writes that back, which is exactly how the schedule store already
/// handles the same problem.
///
/// Expired sessions are dropped on the way through, so every write is also a prune and the
/// file cannot grow without bound.
///
/// One limitation, stated rather than hidden: a process still serves reads from its own
/// copy, so a session minted by the other one is not visible until this process next
/// reloads. Nothing is lost, and the recommended topology is a single console.
fn update_sessions<F>(workspace: &Path, f: F) -> HashMap<String, Session>
where
    F: FnOnce(&mut HashMap<String, Session>),
{
    // A workspace that will not take a lock must not cost anyone their session, so a
    // failure here proceeds unlocked rather than refusing to sign in.
    let _guard = duckle_duckdb_engine::runlock::lock_store(workspace, "console-sessions").ok();
    let mut stored = read_sessions_file(workspace);
    let before = stored.len();
    let now = now_secs();
    stored.retain(|_, s| s.expires_at > now);
    let pruned = stored.len() != before;
    f(&mut stored);
    if pruned || stored.len() != before {
        write_sessions_file(workspace, &stored);
    }
    stored
}

/// Read the store, dropping anything already expired and writing the pruned set back. A
/// store that will not parse is an empty one: a session is a convenience, and refusing to
/// start the console because a cache went bad trades a small problem for a total one.
fn load_sessions(workspace: &Path) -> HashMap<String, Session> {
    update_sessions(workspace, |_| {})
}

impl Console {
    /// Decide the policy for a server about to bind `host`.
    ///
    /// Returns an error, rather than a warning, when the console would be
    /// reachable off this machine with no way to tell callers apart. Refusing
    /// to start is the point: a warning is what this had before, and a warning
    /// that is printed once into a service log is not a control.
    pub fn configure(
        workspace: &Path,
        host: &str,
        token: Option<&str>,
    ) -> Result<Console, String> {
        let mut accounts = load_accounts(workspace)?;
        if let Some(t) = token {
            if t.trim().is_empty() {
                return Err("--token was given but is empty".into());
            }
            accounts.push(Account {
                label: "token".into(),
                role: Role::Admin,
                token_hash: hash_token(t)?,
            });
        }
        let loopback = is_loopback(host);
        if accounts.is_empty() && !loopback {
            // Names no subcommand: both `serve` and `web` end up here, and a
            // message telling you to run the other one is worse than none.
            return Err(format!(
                "--host {host} would expose this beyond the local machine, and it can run any \
                 pipeline in the workspace. Set a token first:\n    \
                 DUCKLE_CONSOLE_TOKEN=<secret>\n\
                 or create accounts with roles:\n    \
                 duckle-runner console add-user <label> --role viewer|operator|admin\n\
                 Accounts are read from {}",
                accounts_path(workspace).display()
            ));
        }
        Ok(Console {
            open: accounts.is_empty() && loopback,
            accounts,
            sessions: Mutex::new(load_sessions(workspace)),
            workspace: workspace.to_path_buf(),
            verified: Mutex::new(HashMap::new()),
        })
    }

    /// Whether this console admits anyone, which is only ever true on loopback
    /// with nothing configured.
    pub fn is_open(&self) -> bool {
        self.open
    }

    /// Who is calling, from the request's `Authorization` and `Cookie` headers.
    ///
    /// `None` means the request is unauthenticated and must be refused. An open
    /// console reports a local admin so callers need no special case.
    pub fn identify(&self, authorization: Option<&str>, cookie: Option<&str>) -> Option<Identity> {
        if self.open {
            return Some(Identity { label: "local".into(), role: Role::Admin });
        }
        // A session cookie first: it is the browser's normal path and costs a
        // map lookup rather than a password hash.
        if let Some(sid) = cookie.and_then(|c| cookie_value(c, SESSION_COOKIE)) {
            let key = session_key(&sid);
            let mut sessions = self.sessions.lock().ok()?;
            match sessions.get(&key) {
                Some(s) if s.expires_at > now_secs() => {
                    return Some(Identity { label: s.label.clone(), role: s.role });
                }
                // Expired, so it is not a session any more. Drop it here rather than
                // waiting for a restart, or the store only ever grows.
                Some(_) => {
                    sessions.remove(&key);
                    let ws = self.workspace.clone();
                    let k = key.clone();
                    update_sessions(&ws, |stored| {
                        stored.remove(&k);
                    });
                }
                None => {}
            }
        }
        let bearer = authorization?.strip_prefix("Bearer ")?.trim();
        self.verify_token(bearer)
    }

    /// Exchange a token for a session id to put in a cookie.
    ///
    /// The browser holds the session id, not the token, so a stored cookie
    /// cannot be replayed against the API after the server restarts.
    pub fn sign_in(&self, token: &str) -> Option<(String, Identity)> {
        let identity = self.verify_token(token)?;
        let mut raw = [0u8; 32];
        getrandom::fill(&mut raw).ok()?;
        let sid = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(raw);
        let session = Session {
            label: identity.label.clone(),
            role: identity.role,
            expires_at: now_secs() + SESSION_TTL_SECS,
        };
        let key = session_key(&sid);
        let merged = update_sessions(&self.workspace, |stored| {
            stored.insert(key, session);
        });
        *self.sessions.lock().ok()? = merged;
        Some((sid, identity))
    }

    pub fn sign_out(&self, cookie: Option<&str>) {
        if let Some(sid) = cookie.and_then(|c| cookie_value(c, SESSION_COOKIE)) {
            let key = session_key(&sid);
            let merged = update_sessions(&self.workspace, |stored| {
                stored.remove(&key);
            });
            if let Ok(mut s) = self.sessions.lock() {
                *s = merged;
            }
        }
    }

    fn verify_token(&self, token: &str) -> Option<Identity> {
        if token.is_empty() {
            return None;
        }
        let digest: [u8; 32] = Sha256::digest(token.as_bytes()).into();
        if let Some(id) = self.verified.lock().ok()?.get(&digest) {
            return Some(id.clone());
        }
        // Every account is checked even after a match, so the time taken does
        // not reveal which account a token belongs to or how many exist.
        let mut found: Option<Identity> = None;
        for account in &self.accounts {
            let Ok(parsed) = PasswordHash::new(&account.token_hash) else {
                continue;
            };
            if Argon2::default().verify_password(token.as_bytes(), &parsed).is_ok() && found.is_none()
            {
                found = Some(Identity { label: account.label.clone(), role: account.role });
            }
        }
        let identity = found?;
        self.verified.lock().ok()?.insert(digest, identity.clone());
        Some(identity)
    }
}

pub const SESSION_COOKIE: &str = "duckle_console";

/// Pull one cookie's value out of a `Cookie:` header.
fn cookie_value(header: &str, name: &str) -> Option<String> {
    header.split(';').find_map(|pair| {
        let (k, v) = pair.split_once('=')?;
        (k.trim() == name).then(|| v.trim().to_string())
    })
}

fn is_loopback(host: &str) -> bool {
    matches!(host, "127.0.0.1" | "localhost" | "::1" | "[::1]")
}

fn hash_token(token: &str) -> Result<String, String> {
    let mut salt_bytes = [0u8; 16];
    getrandom::fill(&mut salt_bytes).map_err(|e| format!("salt rng: {e}"))?;
    let salt = SaltString::encode_b64(&salt_bytes).map_err(|e| format!("salt: {e}"))?;
    Argon2::default()
        .hash_password(token.as_bytes(), &salt)
        .map(|h| h.to_string())
        .map_err(|e| format!("hash token: {e}"))
}

fn load_accounts(workspace: &Path) -> Result<Vec<Account>, String> {
    let p = accounts_path(workspace);
    if !p.exists() {
        return Ok(Vec::new());
    }
    let text = std::fs::read_to_string(&p).map_err(|e| format!("read {}: {e}", p.display()))?;
    // A console-users file that will not parse is refused rather than treated
    // as "no accounts", because that would silently reopen the console.
    let accounts: Vec<Account> = serde_json::from_str(&text)
        .map_err(|e| format!("parse {}: {e}", p.display()))?;
    for a in &accounts {
        if PasswordHash::new(&a.token_hash).is_err() {
            return Err(format!(
                "account '{}' in {} has an unreadable tokenHash; expected an Argon2 PHC string",
                a.label,
                p.display()
            ));
        }
    }
    Ok(accounts)
}

/// Write an account file for `label` with a freshly generated token, returning
/// the token so the operator can copy it. It is not stored anywhere.
pub fn add_account(workspace: &Path, label: &str, role: Role) -> Result<String, String> {
    let mut raw = [0u8; 32];
    getrandom::fill(&mut raw).map_err(|e| format!("token rng: {e}"))?;
    let token = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(raw);
    let mut accounts = load_accounts(workspace)?;
    accounts.retain(|a| a.label != label);
    accounts.push(Account { label: label.into(), role, token_hash: hash_token(&token)? });
    let p = accounts_path(workspace);
    if let Some(dir) = p.parent() {
        std::fs::create_dir_all(dir).map_err(|e| format!("create {}: {e}", dir.display()))?;
    }
    let body = serde_json::to_string_pretty(&accounts).map_err(|e| e.to_string())?;
    std::fs::write(&p, body).map_err(|e| format!("write {}: {e}", p.display()))?;
    Ok(token)
}

/// `duckle-runner console ...` - manage who may sign in.
///
/// This exists because a role model nobody can create accounts for is a role
/// model that ships switched off.
pub fn run() -> Result<i32, String> {
    let args: Vec<String> = std::env::args().skip(2).collect();
    let sub = args.first().map(String::as_str).unwrap_or("");
    if sub.is_empty() || sub == "-h" || sub == "--help" {
        println!(
            "duckle-runner console - console accounts and roles\n\n\
             USAGE:\n    \
             duckle-runner console add-user <label> --role <viewer|operator|admin> [--workspace <dir>]\n    \
             duckle-runner console list [--workspace <dir>]\n\n\
             add-user prints a freshly generated token once. It is stored only as an\n\
             Argon2 hash, so it cannot be recovered later; generate a new one instead.\n\n\
             ROLES:\n    \
             viewer     read the dashboard, runs and logs\n    \
             operator   also run pipelines and change schedules\n    \
             admin      also credentials, connections and the workspace itself"
        );
        return Ok(0);
    }

    let mut label = None;
    let mut role = None;
    let mut workspace = PathBuf::from(".");
    let mut it = args.iter().skip(1);
    while let Some(a) = it.next() {
        match a.as_str() {
            "--role" => {
                role = Some(match it.next().map(String::as_str) {
                    Some("viewer") => Role::Viewer,
                    Some("operator") => Role::Operator,
                    Some("admin") => Role::Admin,
                    other => {
                        return Err(format!(
                            "--role must be viewer, operator or admin (got {})",
                            other.unwrap_or("nothing")
                        ))
                    }
                })
            }
            "--workspace" => {
                workspace = PathBuf::from(it.next().ok_or("--workspace needs a value")?)
            }
            other if !other.starts_with("--") && label.is_none() => {
                label = Some(other.to_string())
            }
            other => return Err(format!("unknown argument: {other}")),
        }
    }

    match sub {
        "add-user" => {
            let label = label.ok_or("add-user needs a label, e.g. `console add-user ops`")?;
            let role = role.ok_or("add-user needs --role viewer|operator|admin")?;
            let token = add_account(&workspace, &label, role)?;
            println!("Added '{label}' as {}.", role.as_str());
            println!("\nToken (shown once, not stored):\n  {token}\n");
            println!("Use it as a header:  Authorization: Bearer {token}");
            println!("Accounts: {}", accounts_path(&workspace).display());
            Ok(0)
        }
        "list" => {
            let accounts = load_accounts(&workspace)?;
            if accounts.is_empty() {
                println!("No console accounts in {}.", accounts_path(&workspace).display());
            }
            for a in &accounts {
                println!("{:<24} {}", a.label, a.role.as_str());
            }
            Ok(0)
        }
        other => Err(format!("unknown console command: {other}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn binding_off_this_machine_without_a_credential_is_refused() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path();

        // The case that mattered: exposed to the network with no way to tell
        // callers apart. This used to start and print a warning.
        let err = Console::configure(ws, "0.0.0.0", None).expect_err("must refuse to start");
        assert!(err.contains("Set a token first"), "unhelpful refusal: {err}");

        // With a token it starts, and is not open.
        let console = Console::configure(ws, "0.0.0.0", Some("s3cret-token")).expect("starts");
        assert!(!console.is_open());

        // Loopback with nothing configured is unchanged: no token needed.
        let local = Console::configure(ws, "127.0.0.1", None).expect("loopback starts");
        assert!(local.is_open(), "requiring a token on localhost breaks the local workflow");
    }

    /// Sessions used to live only in the process, so every restart signed everyone out.
    /// On a rolling deployment that is every release, which is the difference between a
    /// console people keep open and one they avoid.
    #[test]
    fn a_signed_in_session_survives_a_restart() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path();

        let first = Console::configure(ws, "0.0.0.0", Some("s3cret-token")).expect("starts");
        let (sid, who) = first.sign_in("s3cret-token").expect("token signs in");
        assert_eq!(who.role, Role::Admin);
        let cookie = format!("{SESSION_COOKIE}={sid}");
        drop(first);

        // Same workspace, new process.
        let restarted = Console::configure(ws, "0.0.0.0", Some("s3cret-token")).expect("starts");
        let after = restarted
            .identify(None, Some(&cookie))
            .expect("the session should outlive the process that minted it");
        assert_eq!(after.role, Role::Admin);
        assert_eq!(after.label, who.label);
    }

    /// The session store sits in the workspace, which gets backed up and copied around, so
    /// it must not be a file of working credentials. Only a hash of the id is written; the
    /// id itself exists in the browser and nowhere else.
    #[test]
    fn the_session_store_holds_nothing_that_can_be_replayed() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path();
        let console = Console::configure(ws, "0.0.0.0", Some("s3cret-token")).expect("starts");
        let (sid, _) = console.sign_in("s3cret-token").expect("signs in");

        let written = std::fs::read_to_string(sessions_path(ws)).expect("store written");
        assert!(!written.contains(&sid), "the session id itself was written down");
        assert!(!written.contains("s3cret-token"), "the token was written down");
    }

    /// Running `serve` for operations and `web` for authoring against one workspace is a
    /// normal setup, and both keep sessions in the same file. Writing the whole map from a
    /// copy loaded at startup meant whichever signed in last erased the other's sessions.
    /// Raised by Dariusz Danielewski, who was right that a JSON file needs the same care
    /// here as anywhere else; the store this repository already uses for schedules takes a
    /// lock around read-modify-write, and this one now does too.
    #[test]
    fn two_processes_sharing_a_workspace_do_not_erase_each_others_sessions() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path();

        let ops = Console::configure(ws, "0.0.0.0", Some("s3cret-token")).expect("starts");
        let editor = Console::configure(ws, "0.0.0.0", Some("s3cret-token")).expect("starts");

        let (a, _) = ops.sign_in("s3cret-token").expect("signs in");
        let (b, _) = editor.sign_in("s3cret-token").expect("signs in");

        // A third process reads what is actually on disk.
        let fresh = Console::configure(ws, "0.0.0.0", Some("s3cret-token")).expect("starts");
        for (who, sid) in [("the ops console", &a), ("the editor", &b)] {
            assert!(
                fresh.identify(None, Some(&format!("{SESSION_COOKIE}={sid}"))).is_some(),
                "{who}'s session was erased by the other process"
            );
        }
    }

    /// Signing out has to mean signed out everywhere, not just in the process that
    /// happened to serve the request.
    #[test]
    fn signing_out_survives_a_restart_too() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path();
        let console = Console::configure(ws, "0.0.0.0", Some("s3cret-token")).expect("starts");
        let (sid, _) = console.sign_in("s3cret-token").expect("signs in");
        let cookie = format!("{SESSION_COOKIE}={sid}");
        console.sign_out(Some(&cookie));
        drop(console);

        let restarted = Console::configure(ws, "0.0.0.0", Some("s3cret-token")).expect("starts");
        assert!(
            restarted.identify(None, Some(&cookie)).is_none(),
            "a signed-out session came back after a restart"
        );
    }

    /// A session that never expires is a credential that never expires. The store also
    /// only ever grew, because nothing was dropped except by signing out.
    #[test]
    fn an_expired_session_is_refused_and_dropped() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path();
        let console = Console::configure(ws, "0.0.0.0", Some("s3cret-token")).expect("starts");
        let (sid, _) = console.sign_in("s3cret-token").expect("signs in");
        let cookie = format!("{SESSION_COOKIE}={sid}");

        // Age it past the limit by rewriting the stored expiry, which is the only thing
        // standing between a live session and a dead one.
        let raw = std::fs::read_to_string(sessions_path(ws)).unwrap();
        let mut stored: HashMap<String, Session> = serde_json::from_str(&raw).unwrap();
        for s in stored.values_mut() {
            s.expires_at = now_secs() - 1;
        }
        std::fs::write(sessions_path(ws), serde_json::to_string(&stored).unwrap()).unwrap();

        let restarted = Console::configure(ws, "0.0.0.0", Some("s3cret-token")).expect("starts");
        assert!(
            restarted.identify(None, Some(&cookie)).is_none(),
            "an expired session was admitted"
        );
        let after: HashMap<String, Session> =
            serde_json::from_str(&std::fs::read_to_string(sessions_path(ws)).unwrap()).unwrap();
        assert!(after.is_empty(), "an expired session was kept on disk");
    }

    #[test]
    fn only_the_right_token_identifies_a_caller() {
        let tmp = tempfile::tempdir().unwrap();
        let console =
            Console::configure(tmp.path(), "0.0.0.0", Some("s3cret-token")).expect("starts");

        assert!(console.identify(None, None).is_none(), "no credential was admitted");
        assert!(console.identify(Some("Bearer wrong"), None).is_none(), "wrong token admitted");
        assert!(console.identify(Some("s3cret-token"), None).is_none(), "raw header admitted");

        let who = console.identify(Some("Bearer s3cret-token"), None).expect("correct token");
        assert_eq!(who.role, Role::Admin, "--token is the operator's own credential");
    }

    #[test]
    fn a_session_cookie_stands_in_for_the_token() {
        let tmp = tempfile::tempdir().unwrap();
        let console =
            Console::configure(tmp.path(), "0.0.0.0", Some("s3cret-token")).expect("starts");

        assert!(console.sign_in("wrong").is_none(), "signed in with the wrong token");
        let (sid, who) = console.sign_in("s3cret-token").expect("sign in");
        assert_eq!(who.role, Role::Admin);
        // The session id is not the token, so a stolen cookie is not a token.
        assert_ne!(sid, "s3cret-token");

        let cookie = format!("{SESSION_COOKIE}={sid}");
        assert!(console.identify(None, Some(&cookie)).is_some(), "session not honoured");
        console.sign_out(Some(&cookie));
        assert!(console.identify(None, Some(&cookie)).is_none(), "sign-out left the session live");
    }

    #[test]
    fn accounts_carry_their_own_role_and_the_file_never_holds_the_token() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path();

        let token = add_account(ws, "reporting", Role::Viewer).expect("add account");
        let stored = std::fs::read_to_string(accounts_path(ws)).unwrap();
        assert!(!stored.contains(&token), "the token itself was written to disk");
        assert!(stored.contains("argon2"), "expected an Argon2 hash");

        let console = Console::configure(ws, "0.0.0.0", None).expect("accounts are a credential");
        let who = console
            .identify(Some(&format!("Bearer {token}")), None)
            .expect("the generated token works");
        assert_eq!(who.role, Role::Viewer);
        assert_eq!(who.label, "reporting");
        assert!(!who.role.allows(Role::Operator), "a viewer must not be able to run pipelines");
    }

    #[test]
    fn roles_include_the_ones_below_them() {
        assert!(Role::Admin.allows(Role::Operator) && Role::Admin.allows(Role::Viewer));
        assert!(Role::Operator.allows(Role::Viewer));
        assert!(!Role::Operator.allows(Role::Admin));
        assert!(!Role::Viewer.allows(Role::Operator));
    }

    #[test]
    fn an_unreadable_accounts_file_stops_the_server_rather_than_opening_it() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path();
        std::fs::create_dir_all(accounts_path(ws).parent().unwrap()).unwrap();
        std::fs::write(accounts_path(ws), b"{ not json").unwrap();
        // Treating a corrupt file as "no accounts" would silently drop the
        // console back to unauthenticated, which is the worst possible default.
        assert!(Console::configure(ws, "0.0.0.0", None).is_err());
        assert!(Console::configure(ws, "127.0.0.1", None).is_err());
    }
}
