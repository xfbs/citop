//! Shared on-disk state.
//!
//! The store is a cache of everything citop fetched, so that a new process
//! can show the previous state immediately. It is also how several citop
//! processes coordinate: before fetching something, a process *claims* it
//! in the database. Only one process can hold a claim, and something that
//! was fetched recently by anyone is not due, so no two processes make the
//! same request. Claims expire, so a crashed process cannot block others.
//!
//! Every write that changes data bumps a global sequence number (`meta.seq`)
//! and stamps the changed rows with it, so readers can cheaply ask for
//! "everything that changed since I last looked".
//!
//! It is a cache: when the schema changes, all tables are recreated instead
//! of migrated. Tokens are never stored.

use std::collections::HashSet;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use futures::future::BoxFuture;
use rusqlite::{Connection, OptionalExtension, Transaction, TransactionBehavior, params};
use serde::Serialize;
use serde::de::DeserializeOwned;
use url::Url;

use crate::http::{CachedResponse, RateLimit, ResponseCache};
use crate::model::{CommitStatus, Job, Pipeline, Project, is_busy};
use crate::provider::Discovery;

const SCHEMA_VERSION: i64 = 1;

/// How long a claim is valid. Longer than any fetch should take; if a fetch
/// takes longer, the worst case is a duplicate request.
const CLAIM_MS: i64 = 2 * 60 * 1000;
/// How often project lists are fetched again.
const REDISCOVER_MS: i64 = 10 * 60 * 1000;
/// How soon a failed project discovery is retried.
const DISCOVERY_RETRY_MS: i64 = 30 * 1000;
/// Rows not updated for this long are deleted on startup.
const PRUNE_PROJECTS_MS: i64 = 7 * 24 * 3600 * 1000;
const PRUNE_JOBS_MS: i64 = 24 * 3600 * 1000;
const PRUNE_HTTP_MS: i64 = 7 * 24 * 3600 * 1000;

const SCHEMA: &str = "
CREATE TABLE meta (key TEXT PRIMARY KEY, value INTEGER NOT NULL);
INSERT INTO meta (key, value) VALUES ('seq', 0);

-- Project lists, per configured source (filters and token can differ).
CREATE TABLE discoveries (
    scope TEXT PRIMARY KEY,
    projects TEXT,                      -- JSON [Project]; NULL until the first success
    warnings TEXT NOT NULL DEFAULT '[]',
    without_ci INTEGER NOT NULL DEFAULT 0,
    error TEXT,
    next_refresh INTEGER NOT NULL DEFAULT 0,
    claimed_by TEXT,
    claimed_until INTEGER NOT NULL DEFAULT 0,
    seq INTEGER NOT NULL DEFAULT 0
);

-- CI state per project, shared by all sources on the same instance.
CREATE TABLE projects (
    instance TEXT NOT NULL,
    project_id TEXT NOT NULL,
    default_status TEXT,                -- JSON CommitStatus or null; NULL = never fetched
    active TEXT,                        -- JSON [Pipeline]; NULL = never fetched
    error TEXT,
    busy INTEGER NOT NULL DEFAULT 0,    -- anything running: refresh at the fast rate
    fetched_at INTEGER NOT NULL DEFAULT 0,
    claimed_by TEXT,
    claimed_until INTEGER NOT NULL DEFAULT 0,
    seq INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (instance, project_id)
);
CREATE INDEX projects_seq ON projects (seq);

CREATE TABLE jobs (
    instance TEXT NOT NULL,
    project_id TEXT NOT NULL,
    pipeline_id INTEGER NOT NULL,
    jobs TEXT,                          -- JSON [Job]; NULL = never fetched
    error TEXT,
    pipeline_active INTEGER NOT NULL DEFAULT 0,
    fetched_at INTEGER NOT NULL DEFAULT 0,
    claimed_by TEXT,
    claimed_until INTEGER NOT NULL DEFAULT 0,
    seq INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (instance, project_id, pipeline_id)
);
CREATE INDEX jobs_seq ON jobs (seq);

CREATE TABLE rate_limits (
    scope TEXT PRIMARY KEY,
    lim INTEGER NOT NULL,
    remaining INTEGER NOT NULL,
    reset INTEGER,
    seq INTEGER NOT NULL
);

-- Responses for conditional requests (ETag / If-None-Match).
CREATE TABLE http_cache (
    url TEXT PRIMARY KEY,
    etag TEXT NOT NULL,
    body BLOB NOT NULL,
    next TEXT,
    stored_at INTEGER NOT NULL
);
";

pub struct Store {
    conn: Arc<Mutex<Connection>>,
    /// Identifies this process in claims.
    owner: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DiscoveryRow {
    pub seq: i64,
    /// `None` until a discovery succeeded.
    pub projects: Option<Vec<Project>>,
    pub warnings: Vec<String>,
    pub without_ci: usize,
    pub error: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ProjectRow {
    pub instance: String,
    pub project_id: String,
    pub default: Option<CommitStatus>,
    pub active: Vec<Pipeline>,
    /// Whether data was fetched successfully at least once.
    pub loaded: bool,
    pub error: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct JobsRow {
    pub instance: String,
    pub project_id: String,
    pub pipeline_id: u64,
    pub jobs: Vec<Job>,
    pub loaded: bool,
    pub error: Option<String>,
}

/// Everything that changed since some sequence number.
#[derive(Debug, Default)]
pub struct Changes {
    /// Pass this to the next [`Store::changes_since`].
    pub seq: i64,
    pub discoveries: Vec<(String, DiscoveryRow)>,
    pub projects: Vec<ProjectRow>,
    pub jobs: Vec<JobsRow>,
    pub rate_limits: Vec<(String, RateLimit)>,
}

fn now_ms() -> i64 {
    Utc::now().timestamp_millis()
}

fn to_json<T: Serialize + ?Sized>(value: &T) -> Result<String> {
    serde_json::to_string(value).context("could not serialize")
}

/// Decodes a JSON column. Undecodable data is treated as missing: it is a
/// cache, and the next fetch overwrites it.
fn from_json<T: DeserializeOwned>(json: Option<String>) -> Option<T> {
    serde_json::from_str(&json?).ok()
}

/// Increments the global sequence number and returns the new value.
fn next_seq(tx: &Transaction) -> Result<i64> {
    Ok(tx.query_row(
        "UPDATE meta SET value = value + 1 WHERE key = 'seq' RETURNING value",
        [],
        |row| row.get(0),
    )?)
}

impl Store {
    /// Opens (or creates) the store at `path`. The file is only readable by
    /// the current user, as it contains data from private projects.
    pub fn open(path: &Path) -> Result<Self> {
        if let Some(dir) = path.parent() {
            create_private_dir(dir)
                .with_context(|| format!("could not create {}", dir.display()))?;
        }
        create_private_file(path)
            .with_context(|| format!("could not create {}", path.display()))?;
        let conn =
            Connection::open(path).with_context(|| format!("could not open {}", path.display()))?;
        Self::init(conn).with_context(|| format!("could not initialize {}", path.display()))
    }

    /// A store that lives only as long as this process.
    pub fn in_memory() -> Result<Self> {
        Self::init(Connection::open_in_memory()?)
    }

    fn init(mut conn: Connection) -> Result<Self> {
        conn.busy_timeout(Duration::from_secs(5))?;
        conn.query_row("PRAGMA journal_mode = WAL", [], |_| Ok(()))?;
        conn.pragma_update(None, "synchronous", "NORMAL")?;

        let version: i64 = conn.query_row("PRAGMA user_version", [], |row| row.get(0))?;
        if version != SCHEMA_VERSION {
            let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
            // Another process may have done this while we waited for the lock.
            let version: i64 = tx.query_row("PRAGMA user_version", [], |row| row.get(0))?;
            if version != SCHEMA_VERSION {
                let tables: Vec<String> = tx
                    .prepare("SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'")?
                    .query_map([], |row| row.get(0))?
                    .collect::<rusqlite::Result<_>>()?;
                for table in tables {
                    tx.execute(
                        &format!("DROP TABLE \"{}\"", table.replace('"', "\"\"")),
                        [],
                    )?;
                }
                tx.execute_batch(SCHEMA)?;
                tx.pragma_update(None, "user_version", SCHEMA_VERSION)?;
            }
            tx.commit()?;
        }

        let now = now_ms();
        conn.execute(
            "DELETE FROM projects WHERE fetched_at < ?1",
            [now - PRUNE_PROJECTS_MS],
        )?;
        conn.execute(
            "DELETE FROM jobs WHERE fetched_at < ?1",
            [now - PRUNE_JOBS_MS],
        )?;
        conn.execute(
            "DELETE FROM http_cache WHERE stored_at < ?1",
            [now - PRUNE_HTTP_MS],
        )?;

        let nanos = Utc::now().timestamp_nanos_opt().unwrap_or_default();
        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
            owner: format!("{}-{nanos:x}", std::process::id()),
        })
    }

    /// Runs `f` on the connection in a blocking thread.
    async fn with<T, F>(&self, f: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&mut Connection, &str) -> Result<T> + Send + 'static,
    {
        let conn = self.conn.clone();
        let owner = self.owner.clone();
        tokio::task::spawn_blocking(move || {
            let mut conn = conn.lock().unwrap_or_else(|e| e.into_inner());
            f(&mut conn, &owner)
        })
        .await?
    }

    /// The current sequence number: if it did not change, nothing did.
    pub async fn seq(&self) -> Result<i64> {
        self.with(|conn, _| {
            Ok(
                conn.query_row("SELECT value FROM meta WHERE key = 'seq'", [], |row| {
                    row.get(0)
                })?,
            )
        })
        .await
    }

    /// Claims the discovery of `scope` if it is due and nobody else is doing it.
    pub async fn claim_discovery(&self, scope: &str) -> Result<bool> {
        let scope = scope.to_string();
        self.with(move |conn, owner| {
            let now = now_ms();
            conn.execute(
                "INSERT OR IGNORE INTO discoveries (scope) VALUES (?1)",
                [&scope],
            )?;
            let claimed = conn.execute(
                "UPDATE discoveries SET claimed_by = ?2, claimed_until = ?3
                 WHERE scope = ?1 AND next_refresh <= ?4 AND claimed_until < ?4",
                params![scope, owner, now + CLAIM_MS, now],
            )?;
            Ok(claimed == 1)
        })
        .await
    }

    /// Saves a discovery result. On failure, the previous project list is kept.
    pub async fn save_discovery(
        &self,
        scope: &str,
        instance: &str,
        result: Result<Discovery, String>,
    ) -> Result<()> {
        let (scope, instance) = (scope.to_string(), instance.to_string());
        self.with(move |conn, _| {
            let now = now_ms();
            let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
            let seq = next_seq(&tx)?;
            match result {
                Ok(discovery) => {
                    tx.execute(
                        "UPDATE discoveries SET projects = ?2, warnings = ?3, without_ci = ?4, error = NULL,
                             next_refresh = ?5, claimed_until = 0, seq = ?6
                         WHERE scope = ?1",
                        params![
                            scope,
                            to_json(&discovery.projects)?,
                            to_json(&discovery.warnings)?,
                            discovery.without_ci as i64,
                            now + REDISCOVER_MS,
                            seq
                        ],
                    )?;
                    let mut insert =
                        tx.prepare("INSERT OR IGNORE INTO projects (instance, project_id) VALUES (?1, ?2)")?;
                    for project in &discovery.projects {
                        insert.execute(params![instance, project.id])?;
                    }
                }
                Err(error) => {
                    tx.execute(
                        "UPDATE discoveries SET error = ?2, next_refresh = ?3, claimed_until = 0, seq = ?4
                         WHERE scope = ?1",
                        params![scope, error, now + DISCOVERY_RETRY_MS, seq],
                    )?;
                }
            }
            tx.commit()?;
            Ok(())
        })
        .await
    }

    /// The discovery of `scope`, if it changed after `seq`.
    pub async fn discovery_since(&self, scope: &str, seq: i64) -> Result<Option<DiscoveryRow>> {
        let scope = scope.to_string();
        self.with(move |conn, _| {
            Ok(conn
                .query_row(
                    "SELECT seq, projects, warnings, without_ci, error FROM discoveries
                     WHERE scope = ?1 AND seq > ?2",
                    params![scope, seq],
                    discovery_row,
                )
                .optional()?)
        })
        .await
    }

    /// Claims up to `limit` projects among `ids` that are due for a refresh:
    /// busy ones fetched before `busy_before`, others fetched before
    /// `idle_before` (both in Unix milliseconds). Returns the claimed ids.
    pub async fn claim_due_projects(
        &self,
        instance: &str,
        ids: HashSet<String>,
        busy_before: i64,
        idle_before: i64,
        limit: usize,
    ) -> Result<Vec<String>> {
        let instance = instance.to_string();
        self.with(move |conn, owner| {
            let now = now_ms();
            const DUE: &str = "claimed_until < ?2 AND fetched_at <= CASE WHEN busy THEN ?3 ELSE ?4 END";
            // Read first, so that the common case (nothing due) takes no write lock.
            let candidates: Vec<String> = conn
                .prepare_cached(&format!(
                    "SELECT project_id FROM projects WHERE instance = ?1 AND {DUE} ORDER BY fetched_at"
                ))?
                .query_map(params![instance, now, busy_before, idle_before], |row| row.get(0))?
                .filter_map(|id| id.ok().filter(|id| ids.contains(id)))
                .take(limit)
                .collect();
            if candidates.is_empty() {
                return Ok(candidates);
            }
            let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
            let mut claimed = Vec::new();
            {
                let mut claim = tx.prepare_cached(&format!(
                    "UPDATE projects SET claimed_by = ?5, claimed_until = ?6
                     WHERE instance = ?1 AND {DUE} AND project_id = ?7"
                ))?;
                for id in candidates {
                    let changed =
                        claim.execute(params![instance, now, busy_before, idle_before, owner, now + CLAIM_MS, id])?;
                    if changed == 1 {
                        claimed.push(id);
                    }
                }
            }
            tx.commit()?;
            Ok(claimed)
        })
        .await
    }

    /// Saves the result of a project refresh. Parts that failed keep their
    /// previous data, and the error is recorded.
    pub async fn save_project(
        &self,
        instance: &str,
        project_id: &str,
        default: Result<Option<CommitStatus>, String>,
        active: Result<Vec<Pipeline>, String>,
    ) -> Result<()> {
        let (instance, project_id) = (instance.to_string(), project_id.to_string());
        self.with(move |conn, _| {
            let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
            let (old_default, old_active): (Option<String>, Option<String>) = tx
                .query_row(
                    "SELECT default_status, active FROM projects WHERE instance = ?1 AND project_id = ?2",
                    params![instance, project_id],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .optional()?
                .unwrap_or_default();
            let mut errors = Vec::new();
            let default_json = match &default {
                Ok(default) => Some(to_json(default)?),
                Err(e) => {
                    errors.push(e.clone());
                    old_default
                }
            };
            let active_json = match &active {
                Ok(active) => Some(to_json(active)?),
                Err(e) => {
                    errors.push(e.clone());
                    old_active
                }
            };
            let default: Option<CommitStatus> = from_json::<Option<CommitStatus>>(default_json.clone()).flatten();
            let active: Vec<Pipeline> = from_json(active_json.clone()).unwrap_or_default();
            let busy = is_busy(default.as_ref(), &active);
            let error = (!errors.is_empty()).then(|| errors.join("; "));

            let seq = next_seq(&tx)?;
            tx.execute(
                "INSERT INTO projects (instance, project_id, default_status, active, error, busy, fetched_at, seq)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
                 ON CONFLICT (instance, project_id) DO UPDATE SET
                     default_status = ?3, active = ?4, error = ?5, busy = ?6, fetched_at = ?7,
                     claimed_until = 0, seq = ?8",
                params![instance, project_id, default_json, active_json, error, busy, now_ms(), seq],
            )?;
            tx.commit()?;
            Ok(())
        })
        .await
    }

    /// Makes a project and its jobs due for a refresh right away.
    pub async fn invalidate_project(&self, instance: &str, project_id: &str) -> Result<()> {
        let (instance, project_id) = (instance.to_string(), project_id.to_string());
        self.with(move |conn, _| {
            conn.execute(
                "UPDATE projects SET fetched_at = 0 WHERE instance = ?1 AND project_id = ?2",
                params![instance, project_id],
            )?;
            conn.execute(
                "UPDATE jobs SET fetched_at = 0 WHERE instance = ?1 AND project_id = ?2",
                params![instance, project_id],
            )?;
            Ok(())
        })
        .await
    }

    /// Claims fetching the jobs of a pipeline if they were never fetched, or
    /// the pipeline is running and they were fetched before `refresh_before`.
    pub async fn claim_jobs(
        &self,
        instance: &str,
        project_id: &str,
        pipeline_id: u64,
        pipeline_active: bool,
        refresh_before: i64,
    ) -> Result<bool> {
        let (instance, project_id) = (instance.to_string(), project_id.to_string());
        let pipeline_id = pipeline_id as i64;
        self.with(move |conn, owner| {
            let now = now_ms();
            let row: Option<(i64, i64)> = conn
                .query_row(
                    "SELECT fetched_at, claimed_until FROM jobs
                     WHERE instance = ?1 AND project_id = ?2 AND pipeline_id = ?3",
                    params![instance, project_id, pipeline_id],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .optional()?;
            // Avoid taking the write lock while someone looks at finished jobs.
            if let Some((fetched_at, claimed_until)) = row {
                let due = fetched_at == 0 || (pipeline_active && fetched_at <= refresh_before);
                if !due || claimed_until >= now {
                    return Ok(false);
                }
            }
            let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
            tx.execute(
                "INSERT OR IGNORE INTO jobs (instance, project_id, pipeline_id) VALUES (?1, ?2, ?3)",
                params![instance, project_id, pipeline_id],
            )?;
            let claimed = tx.execute(
                "UPDATE jobs SET pipeline_active = ?4, claimed_by = ?5, claimed_until = ?6
                 WHERE instance = ?1 AND project_id = ?2 AND pipeline_id = ?3 AND claimed_until < ?7
                   AND (fetched_at = 0 OR (?4 AND fetched_at <= ?8))",
                params![instance, project_id, pipeline_id, pipeline_active, owner, now + CLAIM_MS, now, refresh_before],
            )?;
            tx.commit()?;
            Ok(claimed == 1)
        })
        .await
    }

    pub async fn save_jobs(
        &self,
        instance: &str,
        project_id: &str,
        pipeline_id: u64,
        result: Result<Vec<Job>, String>,
    ) -> Result<()> {
        let (instance, project_id) = (instance.to_string(), project_id.to_string());
        let pipeline_id = pipeline_id as i64;
        self.with(move |conn, _| {
            let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
            let seq = next_seq(&tx)?;
            let now = now_ms();
            match result {
                Ok(jobs) => tx.execute(
                    "UPDATE jobs SET jobs = ?4, error = NULL, fetched_at = ?5, claimed_until = 0, seq = ?6
                     WHERE instance = ?1 AND project_id = ?2 AND pipeline_id = ?3",
                    params![instance, project_id, pipeline_id, to_json(&jobs)?, now, seq],
                )?,
                Err(error) => tx.execute(
                    "UPDATE jobs SET error = ?4, fetched_at = ?5, claimed_until = 0, seq = ?6
                     WHERE instance = ?1 AND project_id = ?2 AND pipeline_id = ?3",
                    params![instance, project_id, pipeline_id, error, now, seq],
                )?,
            };
            tx.commit()?;
            Ok(())
        })
        .await
    }

    pub async fn save_rate_limit(&self, scope: &str, rate: RateLimit) -> Result<()> {
        let scope = scope.to_string();
        self.with(move |conn, _| {
            let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
            let seq = next_seq(&tx)?;
            tx.execute(
                "INSERT INTO rate_limits (scope, lim, remaining, reset, seq) VALUES (?1, ?2, ?3, ?4, ?5)
                 ON CONFLICT (scope) DO UPDATE SET lim = ?2, remaining = ?3, reset = ?4, seq = ?5",
                params![
                    scope,
                    rate.limit as i64,
                    rate.remaining as i64,
                    rate.reset.map(|r| r.timestamp()),
                    seq
                ],
            )?;
            tx.commit()?;
            Ok(())
        })
        .await
    }

    /// Everything relevant to the given scopes and instances that changed
    /// after `seq`. Use `0` to get everything.
    pub async fn changes_since(
        &self,
        seq: i64,
        scopes: Vec<String>,
        instances: Vec<String>,
    ) -> Result<Changes> {
        self.with(move |conn, _| {
            let scopes: HashSet<String> = scopes.into_iter().collect();
            let instances: HashSet<String> = instances.into_iter().collect();
            // A read transaction gives a consistent snapshot.
            let tx = conn.transaction()?;
            let current: i64 = tx.query_row("SELECT value FROM meta WHERE key = 'seq'", [], |row| row.get(0))?;
            let mut changes = Changes {
                seq: current,
                ..Default::default()
            };

            let mut stmt = tx.prepare_cached(
                "SELECT scope, seq, projects, warnings, without_ci, error FROM discoveries WHERE seq > ?1",
            )?;
            let mut rows = stmt.query([seq])?;
            while let Some(row) = rows.next()? {
                let scope: String = row.get(0)?;
                if scopes.contains(&scope) {
                    changes.discoveries.push((
                        scope,
                        DiscoveryRow {
                            seq: row.get(1)?,
                            projects: from_json(row.get(2)?),
                            warnings: from_json(row.get(3)?).unwrap_or_default(),
                            without_ci: row.get::<_, i64>(4)?.max(0) as usize,
                            error: row.get(5)?,
                        },
                    ));
                }
            }
            drop(rows);
            drop(stmt);

            let mut stmt = tx.prepare_cached(
                "SELECT instance, project_id, default_status, active, error FROM projects WHERE seq > ?1",
            )?;
            let mut rows = stmt.query([seq])?;
            while let Some(row) = rows.next()? {
                let instance: String = row.get(0)?;
                if !instances.contains(&instance) {
                    continue;
                }
                let default_json: Option<String> = row.get(2)?;
                let active_json: Option<String> = row.get(3)?;
                changes.projects.push(ProjectRow {
                    instance,
                    project_id: row.get(1)?,
                    loaded: default_json.is_some() || active_json.is_some(),
                    default: from_json::<Option<CommitStatus>>(default_json).flatten(),
                    active: from_json(active_json).unwrap_or_default(),
                    error: row.get(4)?,
                });
            }
            drop(rows);
            drop(stmt);

            let mut stmt = tx.prepare_cached(
                "SELECT instance, project_id, pipeline_id, jobs, error FROM jobs WHERE seq > ?1",
            )?;
            let mut rows = stmt.query([seq])?;
            while let Some(row) = rows.next()? {
                let instance: String = row.get(0)?;
                if !instances.contains(&instance) {
                    continue;
                }
                let jobs_json: Option<String> = row.get(3)?;
                changes.jobs.push(JobsRow {
                    instance,
                    project_id: row.get(1)?,
                    pipeline_id: row.get::<_, i64>(2)? as u64,
                    loaded: jobs_json.is_some(),
                    jobs: from_json(jobs_json).unwrap_or_default(),
                    error: row.get(4)?,
                });
            }
            drop(rows);
            drop(stmt);

            let mut stmt =
                tx.prepare_cached("SELECT scope, lim, remaining, reset FROM rate_limits WHERE seq > ?1")?;
            let mut rows = stmt.query([seq])?;
            while let Some(row) = rows.next()? {
                let scope: String = row.get(0)?;
                if scopes.contains(&scope) {
                    let reset: Option<i64> = row.get(3)?;
                    changes.rate_limits.push((
                        scope,
                        RateLimit {
                            limit: row.get::<_, i64>(1)?.max(0) as u64,
                            remaining: row.get::<_, i64>(2)?.max(0) as u64,
                            reset: reset.and_then(|r| DateTime::from_timestamp(r, 0)),
                        },
                    ));
                }
            }
            drop(rows);
            drop(stmt);
            tx.finish()?;
            Ok(changes)
        })
        .await
    }
}

fn discovery_row(row: &rusqlite::Row) -> rusqlite::Result<DiscoveryRow> {
    Ok(DiscoveryRow {
        seq: row.get(0)?,
        projects: from_json(row.get(1)?),
        warnings: from_json(row.get(2)?).unwrap_or_default(),
        without_ci: row.get::<_, i64>(3)?.max(0) as usize,
        error: row.get(4)?,
    })
}

/// Conditional-request cache. Best effort: database errors just mean a
/// regular request is made.
impl ResponseCache for Store {
    fn load<'a>(&'a self, url: &'a Url) -> BoxFuture<'a, Option<CachedResponse>> {
        let url = url.to_string();
        Box::pin(async move {
            self.with(move |conn, _| {
                Ok(conn
                    .query_row(
                        "SELECT etag, body, next FROM http_cache WHERE url = ?1",
                        [url],
                        |row| {
                            Ok(CachedResponse {
                                etag: row.get(0)?,
                                body: row.get(1)?,
                                next: row.get(2)?,
                            })
                        },
                    )
                    .optional()?)
            })
            .await
            .ok()
            .flatten()
        })
    }

    fn store<'a>(&'a self, url: &'a Url, response: CachedResponse) -> BoxFuture<'a, ()> {
        let url = url.to_string();
        Box::pin(async move {
            let _ = self
                .with(move |conn, _| {
                    conn.execute(
                        "INSERT INTO http_cache (url, etag, body, next, stored_at) VALUES (?1, ?2, ?3, ?4, ?5)
                         ON CONFLICT (url) DO UPDATE SET etag = ?2, body = ?3, next = ?4, stored_at = ?5",
                        params![url, response.etag, response.body, response.next, now_ms()],
                    )?;
                    Ok(())
                })
                .await;
        })
    }
}

#[cfg(unix)]
fn create_private_dir(dir: &Path) -> std::io::Result<()> {
    use std::os::unix::fs::DirBuilderExt;
    std::fs::DirBuilder::new()
        .recursive(true)
        .mode(0o700)
        .create(dir)
}

#[cfg(not(unix))]
fn create_private_dir(dir: &Path) -> std::io::Result<()> {
    std::fs::create_dir_all(dir)
}

/// Creates the database file with owner-only permissions if it does not
/// exist yet. SQLite gives the WAL and SHM files the same permissions.
fn create_private_file(path: &Path) -> std::io::Result<()> {
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create(true).truncate(false);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    options.open(path).map(drop)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{PipelineKind, Status};

    fn project(id: &str) -> Project {
        Project {
            source: "s".into(),
            id: id.into(),
            path: format!("g/{id}"),
            web_url: String::new(),
            default_branch: Some("main".into()),
            archived: false,
            fork: false,
            ci_enabled: true,
        }
    }

    fn pipeline(id: u64, status: Status) -> Pipeline {
        Pipeline {
            id,
            number: None,
            name: None,
            kind: PipelineKind::Branch("main".into()),
            sha: "abc".into(),
            status,
            trigger: "push".into(),
            created_at: Utc::now(),
            updated_at: Utc::now(),
            started_at: None,
            finished_at: None,
            web_url: String::new(),
        }
    }

    fn discovery(ids: &[&str]) -> Discovery {
        Discovery {
            projects: ids.iter().map(|id| project(id)).collect(),
            warnings: vec!["w".into()],
            without_ci: 2,
        }
    }

    fn ids(ids: &[&str]) -> HashSet<String> {
        ids.iter().map(|s| s.to_string()).collect()
    }

    /// Two stores on the same file behave like two citop processes.
    fn two_stores() -> (tempfile::TempDir, Store, Store) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("sub").join("state.db");
        let a = Store::open(&path).unwrap();
        let b = Store::open(&path).unwrap();
        (dir, a, b)
    }

    #[tokio::test]
    async fn discovery_is_claimed_once() {
        let (_dir, a, b) = two_stores();
        assert!(a.claim_discovery("scope").await.unwrap());
        assert!(!b.claim_discovery("scope").await.unwrap());
        a.save_discovery("scope", "inst", Ok(discovery(&["1", "2"])))
            .await
            .unwrap();
        // Freshly discovered: not due for anyone.
        assert!(!a.claim_discovery("scope").await.unwrap());
        assert!(!b.claim_discovery("scope").await.unwrap());

        let row = b.discovery_since("scope", 0).await.unwrap().unwrap();
        assert_eq!(row.projects.unwrap().len(), 2);
        assert_eq!(row.warnings, vec!["w".to_string()]);
        assert_eq!(row.without_ci, 2);
        assert!(b.discovery_since("scope", row.seq).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn failed_discovery_keeps_projects() {
        let store = Store::in_memory().unwrap();
        store.claim_discovery("s").await.unwrap();
        store
            .save_discovery("s", "i", Ok(discovery(&["1"])))
            .await
            .unwrap();
        store
            .save_discovery("s", "i", Err("boom".into()))
            .await
            .unwrap();
        let row = store.discovery_since("s", 0).await.unwrap().unwrap();
        assert_eq!(row.projects.unwrap().len(), 1);
        assert_eq!(row.error.as_deref(), Some("boom"));
    }

    #[tokio::test]
    async fn projects_are_claimed_once_and_respect_intervals() {
        let (_dir, a, b) = two_stores();
        a.claim_discovery("s").await.unwrap();
        a.save_discovery("s", "i", Ok(discovery(&["1", "2", "3"])))
            .await
            .unwrap();
        let now = now_ms();

        // A claims two (its limit), B gets the remaining one.
        let claimed_a = a
            .claim_due_projects("i", ids(&["1", "2", "3"]), now, now, 2)
            .await
            .unwrap();
        assert_eq!(claimed_a.len(), 2);
        let claimed_b = b
            .claim_due_projects("i", ids(&["1", "2", "3"]), now, now, 10)
            .await
            .unwrap();
        assert_eq!(claimed_b.len(), 1);
        assert!(!claimed_a.contains(&claimed_b[0]));
        // Unknown projects are never claimed.
        assert!(
            b.claim_due_projects("i", ids(&["9"]), now, now, 10)
                .await
                .unwrap()
                .is_empty()
        );

        // Project 1 is now busy, 2 idle.
        a.save_project("i", "1", Ok(None), Ok(vec![pipeline(1, Status::Running)]))
            .await
            .unwrap();
        a.save_project("i", "2", Ok(None), Ok(vec![]))
            .await
            .unwrap();
        let later = now_ms() + 1;
        // Busy projects are due when fetched before `busy_before`, idle ones
        // before `idle_before`.
        let due = b
            .claim_due_projects("i", ids(&["1", "2"]), later, now - 1, 10)
            .await
            .unwrap();
        assert_eq!(due, vec!["1".to_string()]);

        // Invalidation makes a project due again immediately.
        b.invalidate_project("i", "2").await.unwrap();
        let due = a
            .claim_due_projects("i", ids(&["2"]), now - 1, now - 1, 10)
            .await
            .unwrap();
        assert_eq!(due, vec!["2".to_string()]);
    }

    #[tokio::test]
    async fn partial_failures_keep_previous_data() {
        let store = Store::in_memory().unwrap();
        let commit = CommitStatus::new(vec![pipeline(1, Status::Success)]);
        store
            .save_project(
                "i",
                "1",
                Ok(commit.clone()),
                Ok(vec![pipeline(2, Status::Running)]),
            )
            .await
            .unwrap();
        store
            .save_project("i", "1", Err("nope".into()), Ok(vec![]))
            .await
            .unwrap();

        let changes = store
            .changes_since(0, vec![], vec!["i".into()])
            .await
            .unwrap();
        let row = &changes.projects[0];
        assert_eq!(row.default, commit);
        assert!(row.active.is_empty());
        assert!(row.loaded);
        assert_eq!(row.error.as_deref(), Some("nope"));
    }

    #[tokio::test]
    async fn changes_are_incremental_and_filtered() {
        let store = Store::in_memory().unwrap();
        let scopes = || vec!["s".to_string()];
        let instances = || vec!["i".to_string()];

        store.claim_discovery("s").await.unwrap();
        store
            .save_discovery("s", "i", Ok(discovery(&["1"])))
            .await
            .unwrap();
        store
            .save_project("i", "1", Ok(None), Ok(vec![]))
            .await
            .unwrap();
        store
            .save_project("other", "1", Ok(None), Ok(vec![]))
            .await
            .unwrap();
        store
            .save_rate_limit(
                "s",
                RateLimit {
                    limit: 10,
                    remaining: 5,
                    reset: None,
                },
            )
            .await
            .unwrap();

        let all = store.changes_since(0, scopes(), instances()).await.unwrap();
        assert_eq!(all.discoveries.len(), 1);
        assert_eq!(all.projects.len(), 1, "other instances are filtered out");
        assert_eq!(all.rate_limits[0].1.remaining, 5);
        assert_eq!(store.seq().await.unwrap(), all.seq);

        let none = store
            .changes_since(all.seq, scopes(), instances())
            .await
            .unwrap();
        assert!(none.discoveries.is_empty() && none.projects.is_empty());

        assert!(store.claim_jobs("i", "1", 7, true, 0).await.unwrap());
        assert!(
            !store.claim_jobs("i", "1", 7, true, 0).await.unwrap(),
            "already claimed"
        );
        store.save_jobs("i", "1", 7, Ok(vec![])).await.unwrap();
        let jobs = store
            .changes_since(all.seq, scopes(), instances())
            .await
            .unwrap();
        assert_eq!(jobs.jobs.len(), 1);
        assert!(jobs.jobs[0].loaded);
        // Finished pipeline: jobs never become due again.
        assert!(
            !store
                .claim_jobs("i", "1", 7, false, now_ms() + 1000)
                .await
                .unwrap()
        );
        // Running pipeline: due once older than `refresh_before`.
        assert!(
            store
                .claim_jobs("i", "1", 7, true, now_ms() + 1000)
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn http_cache_round_trip() {
        let store = Store::in_memory().unwrap();
        let url = Url::parse("https://example.com/a?b=c").unwrap();
        assert!(store.load(&url).await.is_none());
        let response = CachedResponse {
            etag: "\"x\"".into(),
            body: b"[1]".to_vec(),
            next: None,
        };
        store.store(&url, response.clone()).await;
        assert_eq!(store.load(&url).await, Some(response));
    }

    #[tokio::test]
    async fn schema_mismatch_recreates_tables() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state.db");
        {
            let conn = Connection::open(&path).unwrap();
            conn.execute_batch("CREATE TABLE projects (junk TEXT); PRAGMA user_version = 999;")
                .unwrap();
        }
        let store = Store::open(&path).unwrap();
        store
            .save_project("i", "1", Ok(None), Ok(vec![]))
            .await
            .unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn database_is_private() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("citop").join("state.db");
        Store::open(&path).unwrap();
        let mode = |p: &Path| std::fs::metadata(p).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode(&path), 0o600);
        assert_eq!(mode(path.parent().unwrap()), 0o700);
    }
}
