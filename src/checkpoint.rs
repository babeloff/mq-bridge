//! Durable resume-cursor checkpoint storage for non-destructive copy sources.
//!
//! A cursor-read source persists the last successfully-sunk key so a restart resumes
//! without re-emitting already-copied rows. The backing store is selected by `checkpoint_store`:
//! the default keeps the cursor in the source datastore (a per-source `mqb_cursors_<source>`
//! collection/table); a `file:///…` URL keeps it in a local JSON file for read-only sources; and
//! a `postgres|mysql|mongodb://…` URL points it at a separate database entirely.
//!
//! Values are opaque strings; each endpoint encodes its native key (a BSON `_id`, a SQL
//! column value) into a string it can decode back.

use anyhow::{anyhow, Context};
use async_trait::async_trait;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use tokio::sync::Mutex as AsyncMutex;

/// A durable store for a single cursor position, keyed by `cursor_id` at construction.
#[async_trait]
pub trait CheckpointStore: Send + Sync {
    /// Returns the persisted cursor value, or `None` if no checkpoint exists yet.
    async fn load(&self) -> anyhow::Result<Option<String>>;
    /// Persists the cursor value, overwriting any previous position.
    async fn save(&self, value: &str) -> anyhow::Result<()>;
}

/// Returns a process-wide async lock for `path`, so concurrent saves to the same checkpoint
/// file (even from different routes) serialize their read-modify-write instead of racing.
fn path_lock(path: &Path) -> Arc<AsyncMutex<()>> {
    static LOCKS: OnceLock<Mutex<HashMap<PathBuf, Arc<AsyncMutex<()>>>>> = OnceLock::new();
    let map = LOCKS.get_or_init(|| Mutex::new(HashMap::new()));
    let mut guard = map.lock().unwrap();
    guard
        .entry(path.to_path_buf())
        .or_insert_with(|| Arc::new(AsyncMutex::new(())))
        .clone()
}

/// A file-backed checkpoint store: a single JSON object mapping cursor keys to values,
/// written atomically (unique temp file + rename). Concurrent saves to the same path are
/// serialized in-process via [`path_lock`]; cross-process sharing still relies on the atomic
/// rename. Suitable for read-only sources and dev/CLI one-offs.
pub struct FileCheckpointStore {
    path: PathBuf,
    key: String,
}

impl FileCheckpointStore {
    pub fn new(path: impl Into<PathBuf>, key: impl Into<String>) -> Self {
        Self {
            path: path.into(),
            key: key.into(),
        }
    }

    async fn read_map(&self) -> anyhow::Result<HashMap<String, String>> {
        match tokio::fs::read(&self.path).await {
            Ok(bytes) => serde_json::from_slice(&bytes).with_context(|| {
                format!("Failed to parse checkpoint file '{}'", self.path.display())
            }),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(HashMap::new()),
            Err(e) => Err(e).with_context(|| {
                format!("Failed to read checkpoint file '{}'", self.path.display())
            }),
        }
    }
}

#[async_trait]
impl CheckpointStore for FileCheckpointStore {
    async fn load(&self) -> anyhow::Result<Option<String>> {
        Ok(self.read_map().await?.get(&self.key).cloned())
    }

    async fn save(&self, value: &str) -> anyhow::Result<()> {
        // Serialize the read-modify-write so parallel saves to the same file can't lose updates.
        let lock = path_lock(&self.path);
        let _guard = lock.lock().await;

        let mut map = self.read_map().await?;
        map.insert(self.key.clone(), value.to_string());
        let bytes =
            serde_json::to_vec_pretty(&map).context("Failed to serialize checkpoint map")?;

        if let Some(parent) = self.path.parent() {
            if !parent.as_os_str().is_empty() {
                tokio::fs::create_dir_all(parent).await.ok();
            }
        }
        // Atomic write: write to a unique sibling temp file, then rename over the target. A
        // per-write suffix avoids clobbering a leftover temp from another process/crash.
        static SEQ: AtomicU64 = AtomicU64::new(0);
        let tmp = self.path.with_extension(format!(
            "tmp.{}.{}",
            std::process::id(),
            SEQ.fetch_add(1, Ordering::Relaxed)
        ));
        tokio::fs::write(&tmp, &bytes)
            .await
            .with_context(|| format!("Failed to write checkpoint temp '{}'", tmp.display()))?;
        if let Err(e) = tokio::fs::rename(&tmp, &self.path).await {
            tokio::fs::remove_file(&tmp).await.ok();
            return Err(e)
                .with_context(|| format!("Failed to commit checkpoint '{}'", self.path.display()));
        }
        Ok(())
    }
}

// --- `checkpoint_store` URL parsing and backend selection ---

/// A parsed `checkpoint_store` destination. A recognized scheme selects a file/external
/// backend; a bare name reuses the source datastore.
#[derive(Debug, Clone, PartialEq)]
pub enum CheckpointBackend {
    /// Schemeless value: reuse the source datastore; `name` is the explicit table/collection.
    Source { name: String },
    /// `file:///abs/path` — a local JSON key/value file.
    File { path: PathBuf },
    /// `postgres|postgresql|mysql|mariadb|sqlite://…[/table]` — an external SQL table.
    Sqlx { url: String, table: Option<String> },
    /// `mongodb://host/db[/collection]` — an external MongoDB collection.
    Mongo {
        url: String,
        database: String,
        collection: Option<String>,
    },
}

/// Sanitize a source table/collection into an identifier-safe token (`[^A-Za-z0-9_] -> _`).
pub fn sanitize_ident(source: &str) -> String {
    source
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

/// Default meta table/collection name for a source: `mqb_cursors_<source>`, capped to a safe
/// identifier length (63 bytes; a short hash suffix disambiguates truncated names).
pub fn default_meta_name(source: &str) -> String {
    const PREFIX: &str = "mqb_cursors_";
    const MAX: usize = 63;
    let ident = sanitize_ident(source);
    let full = format!("{PREFIX}{ident}");
    if full.len() <= MAX {
        return full;
    }
    // Truncate the source part and append a stable short hash to keep it unique (FNV-1a).
    let mut hash: u64 = 0xcbf29ce484222325;
    for b in source.as_bytes() {
        hash ^= *b as u64;
        hash = hash.wrapping_mul(0x100000001b3);
    }
    let suffix = format!("_{hash:08x}");
    let keep = (MAX - PREFIX.len() - suffix.len()).min(ident.len());
    format!("{PREFIX}{}{suffix}", &ident[..keep])
}

/// Namespaced key for a cursor within a (possibly shared) checkpoint store, so multiple sources
/// pointed at one store/file never collide: `<source>:<cursor_id>`.
pub fn checkpoint_key(source: &str, cursor_id: &str) -> String {
    format!("{}:{}", sanitize_ident(source), cursor_id)
}

/// Parse a `checkpoint_store` config value. A recognized `<scheme>:` selects a file/external
/// backend; anything else is a bare name for the source datastore (a leading `/` is stripped).
pub fn parse_checkpoint_store(spec: &str) -> anyhow::Result<CheckpointBackend> {
    let spec = spec.trim();
    if spec.is_empty() {
        return Err(anyhow!("checkpoint_store is empty"));
    }
    let scheme = spec
        .split_once(':')
        .map(|(s, _)| s.to_ascii_lowercase())
        .unwrap_or_default();
    match scheme.as_str() {
        "file" => parse_file_url(spec),
        "postgres" | "postgresql" | "mysql" | "mariadb" | "sqlite" => parse_sqlx_url(spec, &scheme),
        "mongodb" | "mongodb+srv" => parse_mongo_url(spec),
        _ => {
            // Schemeless -> source datastore, using the given name (strip a leading '/').
            let name = spec.strip_prefix('/').unwrap_or(spec).to_string();
            Ok(CheckpointBackend::Source { name })
        }
    }
}

fn parse_file_url(spec: &str) -> anyhow::Result<CheckpointBackend> {
    let url =
        url::Url::parse(spec).with_context(|| format!("Invalid file checkpoint URL '{spec}'"))?;
    let to_path = |u: &url::Url| {
        u.to_file_path().map_err(|_| {
            anyhow!("Invalid file checkpoint path in '{spec}'; use 'file:///absolute/path'")
        })
    };
    match url.host_str() {
        None | Some("") => Ok(CheckpointBackend::File {
            path: to_path(&url)?,
        }),
        Some("localhost") => {
            // Rewrite `file://localhost/…` to the empty-authority form so to_file_path works.
            let rebuilt = url::Url::parse(&spec.replacen("//localhost/", "///", 1))
                .with_context(|| format!("Invalid file checkpoint URL '{spec}'"))?;
            Ok(CheckpointBackend::File {
                path: to_path(&rebuilt)?,
            })
        }
        Some(host) => Err(anyhow!(
            "Invalid file checkpoint URL '{spec}': '{host}' is parsed as a host. Use the three-slash form, e.g. 'file:///{host}{}'.",
            url.path()
        )),
    }
}

fn path_segments(url: &url::Url) -> Vec<String> {
    url.path_segments()
        .map(|it| it.filter(|s| !s.is_empty()).map(str::to_string).collect())
        .unwrap_or_default()
}

fn parse_sqlx_url(spec: &str, scheme: &str) -> anyhow::Result<CheckpointBackend> {
    if scheme == "sqlite" {
        // SQLite URLs are file-path based; there is no path slot for a table name.
        return Ok(CheckpointBackend::Sqlx {
            url: spec.to_string(),
            table: None,
        });
    }
    let mut url =
        url::Url::parse(spec).with_context(|| format!("Invalid checkpoint URL '{spec}'"))?;
    let segments = path_segments(&url);
    match segments.len() {
        0 => Err(anyhow!(
            "checkpoint_store '{spec}' is missing a database name (e.g. postgres://host/db/table)"
        )),
        1 => Ok(CheckpointBackend::Sqlx {
            url: spec.to_string(),
            table: None,
        }),
        _ => {
            let table = segments.last().unwrap().clone();
            url.set_path(&format!("/{}", segments[..segments.len() - 1].join("/")));
            Ok(CheckpointBackend::Sqlx {
                url: url.to_string(),
                table: Some(table),
            })
        }
    }
}

fn parse_mongo_url(spec: &str) -> anyhow::Result<CheckpointBackend> {
    let mut url =
        url::Url::parse(spec).with_context(|| format!("Invalid checkpoint URL '{spec}'"))?;
    let segments = path_segments(&url);
    match segments.as_slice() {
        [] => Err(anyhow!(
            "checkpoint_store '{spec}' is missing a database name (mongodb://host/db[/collection])"
        )),
        [db] => Ok(CheckpointBackend::Mongo {
            url: spec.to_string(),
            database: db.clone(),
            collection: None,
        }),
        [db, coll] => {
            url.set_path(&format!("/{db}"));
            Ok(CheckpointBackend::Mongo {
                url: url.to_string(),
                database: db.clone(),
                collection: Some(coll.clone()),
            })
        }
        _ => Err(anyhow!(
            "checkpoint_store '{spec}' has too many path segments (expected mongodb://host/db[/collection])"
        )),
    }
}

/// Build a store for a scheme-based (file/external) backend, independent of the source datastore.
/// The schemeless `Source` variant is handled by the caller, which owns the live source connection.
pub async fn build_external_store(
    backend: CheckpointBackend,
    source_name: &str,
    cursor_id: &str,
) -> anyhow::Result<Arc<dyn CheckpointStore>> {
    match backend {
        CheckpointBackend::File { path } => Ok(Arc::new(FileCheckpointStore::new(
            path,
            checkpoint_key(source_name, cursor_id),
        ))),
        CheckpointBackend::Sqlx { url, table } => {
            #[cfg(feature = "sqlx")]
            {
                crate::endpoints::sqlx::build_sql_checkpoint_store(
                    &url,
                    table,
                    source_name,
                    cursor_id,
                )
                .await
            }
            #[cfg(not(feature = "sqlx"))]
            {
                let _ = (table, source_name, cursor_id);
                Err(anyhow!(
                    "checkpoint_store '{url}' requires the 'sqlx' feature to be enabled"
                ))
            }
        }
        CheckpointBackend::Mongo {
            url,
            database,
            collection,
        } => {
            #[cfg(feature = "mongodb")]
            {
                crate::endpoints::mongodb::build_mongo_checkpoint_store(
                    &url,
                    &database,
                    collection,
                    source_name,
                    cursor_id,
                )
                .await
            }
            #[cfg(not(feature = "mongodb"))]
            {
                let _ = (database, collection, source_name, cursor_id);
                Err(anyhow!(
                    "checkpoint_store '{url}' requires the 'mongodb' feature to be enabled"
                ))
            }
        }
        CheckpointBackend::Source { .. } => Err(anyhow!(
            "internal: Source checkpoint backend must be built by the caller"
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn file_store_round_trips_and_overwrites() {
        let dir = std::env::temp_dir().join(format!("mqb_ckpt_{}", uuid::Uuid::new_v4()));
        let path = dir.join("cursors.json");
        let store = FileCheckpointStore::new(path.clone(), "coll:cursor:c1");

        assert_eq!(store.load().await.unwrap(), None);
        store.save("oid:abc").await.unwrap();
        assert_eq!(store.load().await.unwrap(), Some("oid:abc".to_string()));
        store.save("oid:def").await.unwrap();
        assert_eq!(store.load().await.unwrap(), Some("oid:def".to_string()));

        // A second key in the same file is independent.
        let other = FileCheckpointStore::new(path, "coll:cursor:c2");
        assert_eq!(other.load().await.unwrap(), None);
        other.save("int:5").await.unwrap();
        assert_eq!(store.load().await.unwrap(), Some("oid:def".to_string()));

        tokio::fs::remove_dir_all(dir).await.ok();
    }

    #[test]
    fn parse_schemeless_is_source_datastore() {
        assert_eq!(
            parse_checkpoint_store("/my_cursors").unwrap(),
            CheckpointBackend::Source {
                name: "my_cursors".into()
            }
        );
        assert_eq!(
            parse_checkpoint_store("my_cursors").unwrap(),
            CheckpointBackend::Source {
                name: "my_cursors".into()
            }
        );
    }

    #[test]
    fn parse_file_requires_three_slashes() {
        assert_eq!(
            parse_checkpoint_store("file:///var/lib/mqb/cursors.json").unwrap(),
            CheckpointBackend::File {
                path: PathBuf::from("/var/lib/mqb/cursors.json")
            }
        );
        // localhost authority is accepted and normalized to a local path.
        assert_eq!(
            parse_checkpoint_store("file://localhost/var/lib/c.json").unwrap(),
            CheckpointBackend::File {
                path: PathBuf::from("/var/lib/c.json")
            }
        );
        // A bare host (two-slash + path) is rejected with a hint.
        assert!(parse_checkpoint_store("file://var/lib/c.json").is_err());
    }

    #[test]
    fn parse_sqlx_splits_trailing_table() {
        let b = parse_checkpoint_store("postgres://u@h:5432/mydb/cursors").unwrap();
        match b {
            CheckpointBackend::Sqlx { url, table } => {
                assert_eq!(table, Some("cursors".to_string()));
                assert!(
                    !url.contains("/cursors"),
                    "table stripped from conn url: {url}"
                );
                assert!(url.contains("/mydb"));
            }
            other => panic!("expected Sqlx, got {other:?}"),
        }
        // No trailing table -> default later.
        assert_eq!(
            parse_checkpoint_store("mysql://h/db").unwrap(),
            CheckpointBackend::Sqlx {
                url: "mysql://h/db".into(),
                table: None
            }
        );
        // SQLite is file-path based: never split a table out of the path.
        assert_eq!(
            parse_checkpoint_store("sqlite:///tmp/x.db").unwrap(),
            CheckpointBackend::Sqlx {
                url: "sqlite:///tmp/x.db".into(),
                table: None
            }
        );
    }

    #[test]
    fn parse_mongo_db_and_optional_collection() {
        let b = parse_checkpoint_store("mongodb://h:27017/mydb/cursors").unwrap();
        match b {
            CheckpointBackend::Mongo {
                url,
                database,
                collection,
            } => {
                assert_eq!(database, "mydb");
                assert_eq!(collection, Some("cursors".to_string()));
                assert!(!url.contains("/cursors"), "collection stripped: {url}");
            }
            other => panic!("expected Mongo, got {other:?}"),
        }
        assert_eq!(
            parse_checkpoint_store("mongodb://h/mydb").unwrap(),
            CheckpointBackend::Mongo {
                url: "mongodb://h/mydb".into(),
                database: "mydb".into(),
                collection: None
            }
        );
    }

    #[test]
    fn default_meta_name_is_unique_and_sanitized() {
        assert_eq!(default_meta_name("orders"), "mqb_cursors_orders");
        // Schema-qualified / odd chars are sanitized so the name stays a single identifier.
        assert_eq!(
            default_meta_name("public.orders"),
            "mqb_cursors_public_orders"
        );
        // Over-long sources are capped under the 63-char identifier limit.
        let long = "a".repeat(200);
        let name = default_meta_name(&long);
        assert!(name.len() <= 63, "capped: {} ({})", name, name.len());
        assert!(name.starts_with("mqb_cursors_"));
    }

    #[test]
    fn checkpoint_key_namespaces_by_source() {
        assert_eq!(checkpoint_key("orders", "copy-1"), "orders:copy-1");
        assert_eq!(checkpoint_key("a.b", "c1"), "a_b:c1");
    }

    // Regression: many stores writing distinct keys to one shared file concurrently must not
    // lose any update (unsynchronized read-modify-write + a shared temp name would).
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn file_store_concurrent_saves_do_not_lose_updates() {
        let dir = std::env::temp_dir().join(format!("mqb_ckpt_conc_{}", uuid::Uuid::new_v4()));
        let path = dir.join("cursors.json");
        const N: usize = 50;

        let mut handles = Vec::new();
        for i in 0..N {
            let p = path.clone();
            handles.push(tokio::spawn(async move {
                FileCheckpointStore::new(p, format!("key-{i}"))
                    .save(&format!("val-{i}"))
                    .await
                    .unwrap();
            }));
        }
        for h in handles {
            h.await.unwrap();
        }

        for i in 0..N {
            let store = FileCheckpointStore::new(path.clone(), format!("key-{i}"));
            assert_eq!(
                store.load().await.unwrap(),
                Some(format!("val-{i}")),
                "lost update for key-{i}"
            );
        }
        tokio::fs::remove_dir_all(dir).await.ok();
    }
}
