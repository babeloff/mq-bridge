//  mq-bridge
//  © Copyright 2025, by Marco Mengelkoch
//  Licensed under MIT License, see License file for more details
//  git clone https://github.com/marcomq/mq-bridge

//! Directory spool: a crash-safe FIFO queue whose backing store is a directory.
//!
//! Each message becomes a *chunk* — a payload file holding the raw
//! [`CanonicalMessage::payload`] bytes plus an optional JSON sidecar holding its metadata.
//! Chunks are named so lexical order is queue order, written through a `.tmp` name and
//! renamed into place, and (on the reading side) deleted once acknowledged. That is enough
//! to decouple a fast producer from a slow consumer across a process or language boundary
//! with no broker and no shared memory: the producer can finish and exit while the consumer
//! is still draining, and a crash on either side leaves the directory readable.
//!
//! The `file` endpoint is the sibling for a *stream* of delimited records in one file; this
//! one is for a *queue* of arbitrarily large opaque blobs, where the delimiter framing and
//! the single-writer append point would both get in the way.

use crate::models::DirSpoolConfig;
use crate::traits::{
    BoxFuture, ConsumerError, MessageConsumer, MessageDisposition, MessagePublisher,
    PublisherError, ReceivedBatch, SentBatch,
};
use crate::CanonicalMessage;
use anyhow::Context;
use async_trait::async_trait;
use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::fs::{self, File, OpenOptions};
use tokio::io::AsyncWriteExt;
use tracing::{debug, trace, warn};

/// Suffix of a chunk that is still being written. Never handed to a consumer.
const STAGING_SUFFIX: &str = ".tmp";

/// Metadata key carrying the chunk's base name (its position in the queue).
const SRC_CHUNK_KEY: &str = "mqb.src.spool_chunk";
/// Metadata key carrying the spool directory the chunk was read from.
const SRC_PATH_KEY: &str = "mqb.src.spool_path";

// --- Naming ---

/// Renders `pattern` for one chunk. Recognises `{seq}`, `{seq:0N}` / `{seq:0Nd}`,
/// `{timestamp}` (unix millis) and `{message_id}`; anything else is copied through, so an
/// unknown placeholder shows up in the file name rather than being silently dropped.
fn render_name(pattern: &str, seq: u64, message_id: u128) -> String {
    let mut out = String::with_capacity(pattern.len() + 16);
    let mut rest = pattern;
    while let Some(open) = rest.find('{') {
        out.push_str(&rest[..open]);
        let after = &rest[open + 1..];
        let Some(close) = after.find('}') else {
            // Unterminated brace: the rest is literal.
            out.push_str(&rest[open..]);
            return out;
        };
        let token = &after[..close];
        match render_placeholder(token, seq, message_id) {
            Some(value) => out.push_str(&value),
            None => {
                out.push('{');
                out.push_str(token);
                out.push('}');
            }
        }
        rest = &after[close + 1..];
    }
    out.push_str(rest);
    out
}

fn render_placeholder(token: &str, seq: u64, message_id: u128) -> Option<String> {
    let (name, spec) = match token.split_once(':') {
        Some((name, spec)) => (name, Some(spec)),
        None => (token, None),
    };
    match name {
        "seq" => Some(match spec {
            // `6`, `06` and `06d` all mean "zero-pad to six" — the leading zero is the
            // format-spec fill and the trailing `d` the printf conversion, and the issue's
            // example used both. An unparseable width pads to nothing rather than failing:
            // a bad name is easier to spot than a route that will not start.
            Some(spec) => {
                let width: usize = spec
                    .trim_end_matches('d')
                    .trim_start_matches('0')
                    .parse()
                    .unwrap_or(0);
                format!("{seq:0width$}")
            }
            None => seq.to_string(),
        }),
        "timestamp" => Some(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_millis())
                .unwrap_or(0)
                .to_string(),
        ),
        "message_id" => Some(crate::canonical_message::format_message_id(message_id)),
        _ => None,
    }
}

/// The leading run of digits in `name`, which is the chunk's sequence number under any
/// pattern that keeps `{seq}` first. Used to resume numbering after a restart.
fn leading_sequence(name: &str) -> Option<u64> {
    let digits: String = name.chars().take_while(char::is_ascii_digit).collect();
    (!digits.is_empty()).then(|| digits.parse().ok()).flatten()
}

/// Strips `.<suffix>` from `name`, returning the chunk's base name.
fn chunk_base<'a>(name: &'a str, suffix: &str) -> Option<&'a str> {
    name.strip_suffix(suffix)
        .and_then(|stem| stem.strip_suffix('.'))
}

/// Creates `path`'s directory and returns it, so both sides can be pointed at a spool that
/// does not exist yet without ordering the two processes.
async fn ensure_directory(path: &str) -> anyhow::Result<PathBuf> {
    let dir = PathBuf::from(path);
    fs::create_dir_all(&dir)
        .await
        .with_context(|| format!("Failed to create dir_spool directory: {path}"))?;
    Ok(dir)
}

/// Writes `body` to `path`, fsyncing it so the bytes survive a crash that follows the
/// rename. When `atomic`, the write lands on a `.tmp` sibling that is renamed into place
/// only once complete, so a reader listing the directory never observes a partial chunk.
async fn write_chunk_file(path: &Path, body: &[u8], atomic: bool) -> anyhow::Result<()> {
    let target = if atomic {
        let mut staging = path.as_os_str().to_os_string();
        staging.push(STAGING_SUFFIX);
        PathBuf::from(staging)
    } else {
        path.to_path_buf()
    };
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&target)
        .await
        .with_context(|| format!("Failed to open dir_spool chunk {}", target.display()))?;
    file.write_all(body)
        .await
        .with_context(|| format!("Failed to write dir_spool chunk {}", target.display()))?;
    file.sync_all()
        .await
        .with_context(|| format!("Failed to sync dir_spool chunk {}", target.display()))?;
    drop(file);
    if atomic {
        fs::rename(&target, path)
            .await
            .with_context(|| format!("Failed to finalize dir_spool chunk {}", path.display()))?;
    }
    Ok(())
}

/// fsyncs the directory so a rename is durable. Best effort: Windows cannot open a
/// directory handle this way, and a lost rename only costs a re-delivery, never data.
async fn sync_directory(dir: &Path) {
    if let Ok(handle) = File::open(dir).await {
        let _ = handle.sync_all().await;
    }
}

// --- Publisher ---

/// Writes each message to the spool directory as one payload file plus an optional JSON
/// metadata sidecar.
#[derive(Debug)]
pub struct DirSpoolPublisher {
    dir: PathBuf,
    naming_pattern: String,
    payload_suffix: String,
    metadata_suffix: Option<String>,
    atomic: bool,
    done_file: String,
    emit_done: bool,
    /// Next sequence number. Seeded past the highest number already in the directory so a
    /// restart appends to the queue instead of overwriting its head.
    seq: Arc<AtomicU64>,
}

impl DirSpoolPublisher {
    pub async fn new(config: &DirSpoolConfig) -> anyhow::Result<Self> {
        if config.naming_pattern.contains('/') || config.naming_pattern.contains('\\') {
            return Err(anyhow::anyhow!(
                "dir_spool 'naming_pattern' must name a file, not a path: {}",
                config.naming_pattern
            ));
        }
        let dir = ensure_directory(&config.path).await?;
        let payload_suffix = config.payload_suffix().to_string();
        if payload_suffix.is_empty() {
            return Err(anyhow::anyhow!(
                "dir_spool 'payload_extension' must not be empty"
            ));
        }
        if config.metadata_suffix() == Some(payload_suffix.as_str()) {
            return Err(anyhow::anyhow!(
                "dir_spool 'payload_extension' and 'metadata_extension' must differ (both are '{payload_suffix}')"
            ));
        }
        let next_seq = highest_sequence(&dir, &payload_suffix)
            .await?
            .map_or(0, |high| high + 1);
        Ok(Self {
            dir,
            naming_pattern: config.naming_pattern.clone(),
            payload_suffix,
            metadata_suffix: config.metadata_suffix().map(str::to_string),
            atomic: config.atomic,
            done_file: config.done_file.clone(),
            emit_done: config.emit_done,
            seq: Arc::new(AtomicU64::new(next_seq)),
        })
    }

    /// Writes one chunk. The sidecar is renamed into place *before* the payload, so a
    /// consumer that keys off the payload file always finds the metadata already there.
    async fn write_chunk(&self, message: &CanonicalMessage) -> anyhow::Result<String> {
        let seq = self.seq.fetch_add(1, Ordering::Relaxed);
        let base = render_name(&self.naming_pattern, seq, message.message_id);
        if let Some(suffix) = &self.metadata_suffix {
            let sidecar = serde_json::to_vec(&SidecarView {
                message_id: crate::canonical_message::format_message_id(message.message_id),
                metadata: &message.metadata,
            })
            .context("Failed to encode dir_spool metadata sidecar")?;
            let path = self.dir.join(format!("{base}.{suffix}"));
            write_chunk_file(&path, &sidecar, self.atomic).await?;
        }
        let payload_path = self.dir.join(format!("{base}.{}", self.payload_suffix));
        write_chunk_file(&payload_path, &message.payload, self.atomic).await?;
        trace!(chunk = %base, bytes = message.payload.len(), "dir_spool chunk written");
        Ok(base)
    }

    /// Creates the producer-completion sentinel. Idempotent: an existing sentinel is left
    /// alone rather than rewritten, so a second producer closing does not disturb it.
    async fn write_done(&self) -> anyhow::Result<()> {
        let path = self.dir.join(&self.done_file);
        match OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&path)
            .await
        {
            Ok(file) => {
                let _ = file.sync_all().await;
                sync_directory(&self.dir).await;
                debug!(path = %path.display(), "dir_spool done sentinel written");
                Ok(())
            }
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => Ok(()),
            Err(error) => Err(anyhow::Error::new(error).context(format!(
                "Failed to write dir_spool sentinel {}",
                path.display()
            ))),
        }
    }
}

/// On-disk shape of the metadata sidecar. Deliberately a superset of nothing else — it is
/// read back by [`read_sidecar`] and is meant to be trivially parseable from Python.
#[derive(serde::Serialize)]
struct SidecarView<'a> {
    message_id: String,
    metadata: &'a HashMap<String, String>,
}

#[derive(serde::Deserialize)]
struct SidecarOwned {
    #[serde(default)]
    message_id: Option<String>,
    #[serde(default)]
    metadata: HashMap<String, String>,
}

#[async_trait]
impl MessagePublisher for DirSpoolPublisher {
    async fn send_batch(
        &self,
        messages: Vec<CanonicalMessage>,
    ) -> Result<SentBatch, PublisherError> {
        let mut failed = Vec::new();
        for message in messages {
            if let Err(error) = self.write_chunk(&message).await {
                failed.push((message, PublisherError::Retryable(error)));
            }
        }
        // One directory fsync for the whole batch: the per-chunk renames are already
        // ordered, and this only decides how much of the tail survives a power loss.
        sync_directory(&self.dir).await;
        if failed.is_empty() {
            Ok(SentBatch::Ack)
        } else {
            Ok(SentBatch::Partial {
                responses: None,
                failed,
            })
        }
    }

    fn on_disconnect_hook(&self) -> Option<BoxFuture<'_, anyhow::Result<()>>> {
        // The sentinel means "this producer is finished", so it belongs at the one moment
        // the route can tell us that: the publisher going away.
        self.emit_done
            .then(|| Box::pin(self.write_done()) as BoxFuture<'_, anyhow::Result<()>>)
    }

    // The spool is a queue, and the sequence number a chunk gets is assigned inside
    // `send_batch`. Above `concurrency: 1` that would be worker-arrival order, so the route
    // sequences the sends and the directory listing stays source order.
    fn requires_ordered_publish(&self) -> bool {
        true
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

// --- Consumer ---

/// Reads chunks out of the spool directory in lexical (queue) order.
#[derive(Debug)]
pub struct DirSpoolConsumer {
    dir: PathBuf,
    path: String,
    payload_suffix: String,
    metadata_suffix: Option<String>,
    done_file: String,
    drain_on_read: bool,
    stop_on_done: bool,
    poll_interval: Duration,
    source_metadata: bool,
    /// Chunks a listing must skip: those handed out and not yet committed, plus — when
    /// `drain_on_read` is off, so nothing is ever deleted — those already delivered
    /// successfully. Shared with the commit closures, which run on the route's tasks and
    /// are what releases a chunk again after a nack.
    claimed: Arc<StdMutex<HashSet<String>>>,
    exit_on_empty: bool,
}

impl DirSpoolConsumer {
    pub async fn new(config: &DirSpoolConfig) -> anyhow::Result<Self> {
        Self::new_with_source_metadata(config, config.source_metadata).await
    }

    /// `source_metadata` is the effective flag: the route turns it on for an idempotent
    /// output even when the input config leaves it unset.
    pub async fn new_with_source_metadata(
        config: &DirSpoolConfig,
        source_metadata: bool,
    ) -> anyhow::Result<Self> {
        let dir = ensure_directory(&config.path).await?;
        let payload_suffix = config.payload_suffix().to_string();
        if payload_suffix.is_empty() {
            return Err(anyhow::anyhow!(
                "dir_spool 'payload_extension' must not be empty"
            ));
        }
        Ok(Self {
            dir,
            path: config.path.clone(),
            payload_suffix,
            metadata_suffix: config.metadata_suffix().map(str::to_string),
            done_file: config.done_file.clone(),
            drain_on_read: config.drain_on_read,
            stop_on_done: config.stop_on_done,
            poll_interval: Duration::from_millis(config.poll_interval_ms),
            source_metadata,
            claimed: Arc::new(StdMutex::new(HashSet::new())),
            exit_on_empty: false,
        })
    }

    /// Base names of the finalized chunks not already in flight, in queue order.
    async fn list_ready(&self, limit: usize) -> anyhow::Result<Vec<String>> {
        let mut entries = fs::read_dir(&self.dir)
            .await
            .with_context(|| format!("Failed to list dir_spool directory: {}", self.path))?;
        let mut names = Vec::new();
        while let Some(entry) = entries
            .next_entry()
            .await
            .with_context(|| format!("Failed to walk dir_spool directory: {}", self.path))?
        {
            let name = entry.file_name().to_string_lossy().into_owned();
            // `.tmp` files are mid-write, and every other extension (the sidecar, the
            // sentinel) is not itself a chunk.
            let Some(base) = chunk_base(&name, &self.payload_suffix) else {
                continue;
            };
            names.push(base.to_string());
        }
        // Filtered after the walk rather than inside it: the guard is not held across an
        // await, which is what keeps this future `Send`.
        {
            let claimed = self.claimed.lock().expect("dir_spool claim set poisoned");
            names.retain(|base| !claimed.contains(base));
        }
        // Lexical order is queue order — that is the contract `naming_pattern` documents.
        names.sort_unstable();
        names.truncate(limit);
        Ok(names)
    }

    /// Reads one chunk into a message. A missing sidecar is not an error: a payload file
    /// written by a producer that does not emit metadata is still a valid message.
    async fn read_chunk(&self, base: &str) -> anyhow::Result<CanonicalMessage> {
        let payload_path = self.dir.join(format!("{base}.{}", self.payload_suffix));
        let payload = fs::read(&payload_path).await.with_context(|| {
            format!("Failed to read dir_spool chunk {}", payload_path.display())
        })?;
        let sidecar = match &self.metadata_suffix {
            Some(suffix) => self.read_sidecar(base, suffix).await?,
            None => None,
        };
        let (message_id, metadata) = match sidecar {
            Some(SidecarOwned {
                message_id,
                metadata,
            }) => (
                message_id.and_then(|id| crate::canonical_message::message_id_from_str(&id).ok()),
                metadata,
            ),
            None => (None, HashMap::new()),
        };
        let mut message = CanonicalMessage::new(payload, message_id);
        message.metadata = metadata;
        if self.source_metadata {
            message
                .metadata
                .insert(SRC_PATH_KEY.to_string(), self.path.clone());
            message
                .metadata
                .insert(SRC_CHUNK_KEY.to_string(), base.to_string());
        }
        Ok(message)
    }

    async fn read_sidecar(&self, base: &str, suffix: &str) -> anyhow::Result<Option<SidecarOwned>> {
        let path = self.dir.join(format!("{base}.{suffix}"));
        match fs::read(&path).await {
            Ok(bytes) => serde_json::from_slice(&bytes)
                .map(Some)
                .with_context(|| format!("Failed to parse dir_spool sidecar {}", path.display())),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(anyhow::Error::new(error).context(format!(
                "Failed to read dir_spool sidecar {}",
                path.display()
            ))),
        }
    }

    /// Whether the producer has declared the stream finished.
    async fn done_present(&self) -> bool {
        fs::try_exists(self.dir.join(&self.done_file))
            .await
            .unwrap_or(false)
    }
}

/// Removes a chunk's payload and sidecar, returning whether the payload is gone. Errors are
/// logged, not propagated: the message has already been handled, so failing the commit would
/// redeliver it rather than fix the directory.
async fn remove_chunk(
    dir: &Path,
    base: &str,
    payload_suffix: &str,
    metadata_suffix: Option<&str>,
) -> bool {
    // The payload goes first — the reverse of the write order. It is what a listing keys
    // off, so once it is gone the chunk is out of the queue and a crash before the sidecar
    // delete leaves an inert orphan. The other order would leave a payload whose metadata
    // had already vanished, and a restart would redeliver it stripped.
    if !remove_file(&dir.join(format!("{base}.{payload_suffix}"))).await {
        return false;
    }
    if let Some(suffix) = metadata_suffix {
        remove_file(&dir.join(format!("{base}.{suffix}"))).await;
    }
    true
}

/// Deletes `path`, treating an already-absent file as success. Returns whether it is gone.
async fn remove_file(path: &Path) -> bool {
    match fs::remove_file(path).await {
        Ok(()) => true,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => true,
        Err(error) => {
            warn!(
                path = %path.display(),
                %error,
                "dir_spool could not delete an acknowledged chunk; it stays in the directory and will not be redelivered"
            );
            false
        }
    }
}

#[async_trait]
impl MessageConsumer for DirSpoolConsumer {
    fn set_exit_on_empty(&mut self, exit_on_empty: bool) {
        self.exit_on_empty = exit_on_empty;
    }

    // Each chunk is its own file and is deleted independently, so there is no cumulative
    // position that an out-of-order commit could advance past an unacknowledged message.
    fn commit_requires_order(&self) -> bool {
        false
    }

    async fn receive_batch(&mut self, max_messages: usize) -> Result<ReceivedBatch, ConsumerError> {
        let ready = self
            .list_ready(max_messages)
            .await
            .map_err(ConsumerError::Connection)?;

        if ready.is_empty() {
            // The sentinel only ends the stream once the queue behind it is empty, which is
            // the whole point of the two-part condition: a producer that finished long ago
            // still has its backlog drained first.
            if self.stop_on_done && self.done_present().await {
                return Err(ConsumerError::EndOfStream);
            }
            // Under `--drain` an empty batch is the exit signal, so surface it immediately
            // instead of holding the route for a poll interval it will not use.
            if !self.exit_on_empty {
                tokio::time::sleep(self.poll_interval).await;
            }
            return Ok(ReceivedBatch::empty());
        }

        let mut messages = Vec::with_capacity(ready.len());
        let mut delivered = Vec::with_capacity(ready.len());
        for base in ready {
            match self.read_chunk(&base).await {
                Ok(message) => {
                    messages.push(message);
                    delivered.push(base);
                }
                // A chunk that vanished between the listing and the read was taken by a
                // competing consumer; the rest of the batch is still good.
                Err(error) => {
                    warn!(chunk = %base, error = %error, "dir_spool skipping unreadable chunk");
                }
            }
        }
        if messages.is_empty() {
            return Ok(ReceivedBatch::empty());
        }
        self.claimed
            .lock()
            .expect("dir_spool claim set poisoned")
            .extend(delivered.iter().cloned());

        let dir = self.dir.clone();
        let payload_suffix = self.payload_suffix.clone();
        let metadata_suffix = self.metadata_suffix.clone();
        let drain_on_read = self.drain_on_read;
        let claimed = Arc::clone(&self.claimed);
        let commit: crate::traits::BatchCommitFunc = Box::new(move |dispositions| {
            Box::pin(async move {
                let mut release = Vec::new();
                for (index, base) in delivered.iter().enumerate() {
                    // A missing disposition means the route acked the whole batch.
                    let acked = !matches!(dispositions.get(index), Some(MessageDisposition::Nack));
                    if !acked {
                        // Put it back in the queue: a nack is a request to redeliver, and
                        // the chunk is still on disk to redeliver from.
                        release.push(base.clone());
                    } else if drain_on_read {
                        // Only unclaim once the payload is actually gone. A delete that
                        // failed would otherwise put the chunk back in the listing and
                        // redeliver a message the route has already handled.
                        if remove_chunk(&dir, base, &payload_suffix, metadata_suffix.as_deref())
                            .await
                        {
                            release.push(base.clone());
                        }
                    }
                    // Acked with `drain_on_read` off: the files stay, so the claim has to
                    // stay too — it is the only record that this chunk was already read.
                }
                if !release.is_empty() {
                    let mut claimed = claimed.lock().expect("dir_spool claim set poisoned");
                    for base in release {
                        claimed.remove(&base);
                    }
                }
                sync_directory(&dir).await;
                Ok(())
            })
        });

        Ok(ReceivedBatch { messages, commit })
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// The highest sequence number among the finalized chunks in `dir`, or `None` when it holds
/// no chunk this publisher would have written.
async fn highest_sequence(dir: &Path, payload_suffix: &str) -> anyhow::Result<Option<u64>> {
    let mut entries = fs::read_dir(dir)
        .await
        .with_context(|| format!("Failed to list dir_spool directory: {}", dir.display()))?;
    let mut highest = None;
    while let Some(entry) = entries.next_entry().await? {
        let name = entry.file_name().to_string_lossy().into_owned();
        let Some(base) = chunk_base(&name, payload_suffix) else {
            continue;
        };
        if let Some(seq) = leading_sequence(base) {
            highest = Some(highest.map_or(seq, |current: u64| current.max(seq)));
        }
    }
    Ok(highest)
}

#[cfg(test)]
mod tests;
