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
//! One directory takes one producer and one consumer *at a time*. Each side holds a pid
//! lock — `producer_file` and `consumer_file`, by default `PRODUCER` and `CONSUMER` — for as
//! long as the endpoint lives, which keeps a second instance in the same role out; a lock
//! whose owner is gone is broken by the next start, so a crash leaves a directory that is
//! readable *and* reusable. Production itself may span several producers in turn, so the end
//! of the stream is a separate signal: the `done_file` sentinel, written by the last
//! producer as it closes and cleared by any producer that opens the spool again.
//!
//! The `file` endpoint is the sibling for a *stream* of delimited records in one file; this
//! one is for a *queue* of arbitrarily large opaque blobs, where the delimiter framing and
//! the single-writer append point would both get in the way.

use crate::models::DirSpoolConfig;
use crate::traits::{
    BoxFuture, ConsumerError, DisconnectOutcome, MessageConsumer, MessageDisposition,
    MessagePublisher, PublisherError, ReceivedBatch, SentBatch,
};
use crate::CanonicalMessage;
use anyhow::Context;
use async_trait::async_trait;
use std::any::Any;
use std::collections::{BinaryHeap, HashMap, HashSet, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::fs::{self, File, OpenOptions};
use tokio::io::AsyncWriteExt;
use tracing::{debug, trace, warn};

/// Suffix of a chunk that is still being written. Never handed to a consumer.
const STAGING_SUFFIX: &str = ".tmp";

/// How many chunk names one directory scan keeps for later batches. A backlog is drained in
/// `ceil(depth / this)` scans rather than one scan per batch, and the cache costs about a
/// name per entry — a few megabytes at this size — no matter how deep the directory is.
const READY_CACHE_CAPACITY: usize = 65_536;

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

// --- Locks ---

/// Which end of the spool a lock belongs to.
///
/// The two ends never conflict with each other — a producer and a consumer sharing a
/// directory is what the endpoint is *for* — so each gets its own file and only collides
/// with another instance in the same role.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SpoolRole {
    Producer,
    Consumer,
}

impl SpoolRole {
    const fn label(self) -> &'static str {
        match self {
            Self::Producer => "producer",
            Self::Consumer => "consumer",
        }
    }

    /// The configured name of this role's lock file. Every instance sharing the directory
    /// has to agree on it, which is why it is configuration and not a constant.
    fn lock_file(self, config: &DirSpoolConfig) -> &str {
        match self {
            Self::Producer => &config.producer_file,
            Self::Consumer => &config.consumer_file,
        }
    }
}

/// The control files a spool holds besides its chunks: the two locks and the completion
/// sentinel.
///
/// Checked together, because they share one directory with each other and with the chunks.
/// A name that ends in the payload or sidecar extension would be handed to a consumer as a
/// message or read as one's metadata; two control files under one name would have each
/// role's lock deleting the other's; and a name with a path separator in it would escape
/// the spool. All three are configuration mistakes that are much cheaper to reject at
/// startup than to debug in a directory listing.
pub(crate) fn validate_control_files(config: &DirSpoolConfig) -> anyhow::Result<()> {
    let named = [
        ("done_file", config.done_file.as_str()),
        ("producer_file", config.producer_file.as_str()),
        ("consumer_file", config.consumer_file.as_str()),
    ];
    for (field, name) in named {
        if name.is_empty() {
            return Err(anyhow::anyhow!(
                "dir_spool '{field}' must not be empty: it names a file in the spool directory"
            ));
        }
        if name.contains('/') || name.contains('\\') {
            return Err(anyhow::anyhow!(
                "dir_spool '{field}' must name a file in the spool directory, not a path: {name}"
            ));
        }
        let payload_suffix = config.payload_suffix();
        if chunk_base(name, payload_suffix).is_some() {
            return Err(anyhow::anyhow!(
                "dir_spool '{field}' is '{name}', which ends in the payload extension \
                 '.{payload_suffix}' and would be delivered as a message. Give it a name no \
                 chunk can have."
            ));
        }
        if let Some(metadata_suffix) = config.metadata_suffix() {
            if chunk_base(name, metadata_suffix).is_some() {
                return Err(anyhow::anyhow!(
                    "dir_spool '{field}' is '{name}', which ends in the metadata extension \
                     '.{metadata_suffix}' and would be read as a chunk's sidecar. Give it a \
                     name no chunk can have."
                ));
            }
        }
        if name.ends_with(STAGING_SUFFIX) {
            return Err(anyhow::anyhow!(
                "dir_spool '{field}' is '{name}', which ends in '{STAGING_SUFFIX}' and so \
                 looks like a chunk that is still being written"
            ));
        }
    }
    // Case-insensitively, because on Windows and macOS `DONE` and `done` are one file.
    for (index, (field, name)) in named.iter().enumerate() {
        for (other_field, other_name) in named.iter().skip(index + 1) {
            if name.eq_ignore_ascii_case(other_name) {
                return Err(anyhow::anyhow!(
                    "dir_spool '{field}' and '{other_field}' are both '{name}'; the sentinel \
                     and the two locks must be three different files"
                ));
            }
        }
    }
    Ok(())
}

/// A `Pidlock` over `dir`'s file for `role`, not yet acquired. Also the handle used to ask
/// who *else* holds it.
fn role_lock(
    dir: &Path,
    role: SpoolRole,
    config: &DirSpoolConfig,
) -> anyhow::Result<pidlock::Pidlock> {
    let path = dir.join(role.lock_file(config));
    pidlock::Pidlock::new_validated(&path)
        .with_context(|| format!("Unusable dir_spool lock path {}", path.display()))
}

/// Who holds a role's lock, as far as this process can tell.
#[derive(Debug)]
enum LockHolder {
    /// Nothing live holds it: no file, or a file whose owner is gone (which reading it
    /// also cleaned up).
    Free,
    /// Held by a process that is still running.
    Pid(i32),
    /// Held by something, but the lock could not be read. Reported as held, the cautious
    /// direction: an unreadable lock must not be mistaken for a finished producer.
    Unknown,
}

impl LockHolder {
    fn is_held(&self) -> bool {
        !matches!(self, Self::Free)
    }

    fn describe(&self) -> String {
        match self {
            Self::Free => "nothing".to_string(),
            Self::Pid(pid) => format!("pid {pid}"),
            Self::Unknown => "an unreadable lock file".to_string(),
        }
    }
}

/// Reads who holds `role`'s lock on `dir`.
///
/// A lock left behind by a process that is gone reads as [`LockHolder::Free`] *and* is
/// deleted, which is what lets a crash heal instead of wedging the next start.
fn lock_holder(dir: &Path, role: SpoolRole, config: &DirSpoolConfig) -> LockHolder {
    let lock = match role_lock(dir, role, config) {
        Ok(lock) => lock,
        Err(error) => {
            warn!(dir = %dir.display(), %error, "dir_spool could not open the {} lock", role.label());
            return LockHolder::Unknown;
        }
    };
    match lock.get_owner() {
        Ok(Some(pid)) => LockHolder::Pid(pid),
        Ok(None) => LockHolder::Free,
        Err(error) => {
            warn!(
                dir = %dir.display(),
                %error,
                "dir_spool could not read the {} lock; treating it as held",
                role.label()
            );
            LockHolder::Unknown
        }
    }
}

/// Warns a non-draining reader that a draining consumer owns the spool.
///
/// Two readers that only read are fine together; a drainer alongside them deletes chunks
/// on ack, so this reader will silently miss whatever the drainer got to first. Not an
/// error — which of the two is the mistake is the operator's call — but never silent.
fn warn_if_drained(dir: &Path, config: &DirSpoolConfig) {
    if matches!(config.claim, crate::models::SpoolClaim::Off) {
        return;
    }
    let holder = lock_holder(dir, SpoolRole::Consumer, config);
    if holder.is_held() {
        warn!(
            dir = %dir.display(),
            holder = %holder.describe(),
            "dir_spool is being drained by another consumer, so this 'drain_on_read: false' reader will miss chunks it deletes"
        );
    }
}

/// Takes `role`'s lock on `dir`, or reports the conflict.
///
/// The file is created with `O_EXCL` holding this process's id, so of two processes
/// starting at once only one can believe it won, and a lock whose owner is no longer
/// running is broken and retaken. A live conflict is what `claim` decides about:
/// `Exclusive` turns it into a startup failure, `Warn` into a log line. Returns `None`
/// when no lock is held — `claim: off`, or `Warn` deferring to the live holder.
///
/// Blocking, and called straight from the endpoint constructors rather than through
/// `spawn_blocking`: `pidlock` is a blocking API, and this is one `create_new` plus one
/// small write, once per endpoint.
fn acquire_lock(
    dir: &Path,
    role: SpoolRole,
    config: &DirSpoolConfig,
) -> anyhow::Result<Option<pidlock::Pidlock>> {
    use crate::models::SpoolClaim;

    let mode = config.claim;
    if matches!(mode, SpoolClaim::Off) {
        return Ok(None);
    }
    let mut lock = role_lock(dir, role, config)?;
    match lock.acquire() {
        Ok(()) => {
            debug!(
                path = %dir.join(role.lock_file(config)).display(),
                "dir_spool {} lock taken",
                role.label()
            );
            Ok(Some(lock))
        }
        Err(pidlock::PidlockError::LockExists) => {
            let holder = lock_holder(dir, role, config).describe();
            if matches!(mode, SpoolClaim::Warn) {
                warn!(
                    dir = %dir.display(),
                    "dir_spool is already held by a {} ({holder}); running anyway because 'claim' is 'warn'",
                    role.label()
                );
                return Ok(None);
            }
            Err(anyhow::anyhow!(
                "dir_spool directory {} is already held by a {} ({holder}). \
                 Two {}s on one spool corrupt it, so this endpoint will not start. \
                 Point them at separate directories, or set 'claim' to 'warn' or 'off' if \
                 the spool really is shared. A lock left by a crash is broken automatically \
                 once its process is gone; one held by a process this machine cannot see — a \
                 spool shared across hosts or containers — has to be cleared by deleting {}.",
                dir.display(),
                role.label(),
                role.label(),
                dir.join(role.lock_file(config)).display()
            ))
        }
        Err(error) => Err(anyhow::Error::new(error).context(format!(
            "Failed to take the dir_spool {} lock in {}",
            role.label(),
            dir.display()
        ))),
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
    emit_done: crate::models::SpoolDone,
    /// Whether any chunk this publisher accepted failed to reach the disk, which makes the
    /// difference between `emit_done: success` writing the sentinel and staying quiet.
    write_failed: AtomicBool,
    /// Next sequence number. Seeded past the highest number already in the directory so a
    /// restart appends to the queue instead of overwriting its head.
    seq: Arc<AtomicU64>,
    /// The producer lock, held for as long as this publisher lives. Dropped at the defined
    /// moment the route disconnects the publisher rather than whenever the publisher
    /// happens to be freed, so the next producer in a sequence can start immediately.
    /// `None` under `claim: off`, or `claim: warn` over a spool another producer holds.
    lock: StdMutex<Option<pidlock::Pidlock>>,
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
        validate_control_files(config)?;
        // Locked before the directory is touched, so a second producer cannot be what
        // clears the sentinel below or reseeds this one's sequence.
        let lock = acquire_lock(&dir, SpoolRole::Producer, config)?;
        clear_done(&dir, &config.done_file).await;
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
            write_failed: AtomicBool::new(false),
            seq: Arc::new(AtomicU64::new(next_seq)),
            lock: StdMutex::new(lock),
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

    /// Creates the production-completion sentinel. Idempotent: an existing sentinel is
    /// left alone rather than rewritten, so a second producer closing does not disturb it.
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

    /// Whether the sentinel should go down, given how the route ended.
    ///
    /// `success` is the strict reading of "production finished": the input ran out *and*
    /// everything this producer accepted reached the disk. A shutdown or a failure leaves a
    /// stream that may be missing its tail, and saying otherwise would have a `stop_on_done`
    /// consumer treat a truncated spool as the whole of it.
    fn should_emit_done(&self, outcome: DisconnectOutcome) -> bool {
        use crate::models::SpoolDone;
        match self.emit_done {
            SpoolDone::Never => false,
            SpoolDone::End => true,
            SpoolDone::Success => {
                matches!(outcome, DisconnectOutcome::Completed)
                    && !self.write_failed.load(Ordering::Relaxed)
            }
        }
    }

    /// Closes this producer: sentinel down if it is owed, then the lock released.
    ///
    /// The sentinel goes first because, while the lock is still held, no other producer can
    /// be in `new` clearing it — which is what keeps a hand-off between two producers from
    /// losing it.
    async fn close_with(&self, outcome: DisconnectOutcome) -> anyhow::Result<()> {
        let wrote_done = if self.should_emit_done(outcome) {
            self.write_done().await
        } else {
            Ok(())
        };
        self.release_lock();
        sync_directory(&self.dir).await;
        wrote_done
    }

    /// Releases the producer lock, freeing the spool for the next producer.
    ///
    /// Idempotent, and best effort: dropping the lock deletes the file, and a file that
    /// cannot be deleted is reported rather than raised, since by this point the messages
    /// are already written. The cost of a lock left behind is bounded — the next start
    /// finds its owner gone and breaks it.
    fn release_lock(&self) {
        let released = self
            .lock
            .lock()
            .expect("dir_spool producer lock poisoned")
            .take();
        if released.is_some() {
            debug!(dir = %self.dir.display(), "dir_spool producer lock released");
        }
    }
}

/// Removes a completion sentinel left by an earlier run.
///
/// A publisher opening the spool means production is live again, so a stale sentinel has to
/// go: left in place it tells a `stop_on_done` consumer to exit the moment its queue first
/// runs dry, abandoning everything this producer is about to write. Best effort — a
/// sentinel that cannot be deleted is reported, not fatal, since the alternative is
/// refusing to produce at all.
async fn clear_done(dir: &Path, done_file: &str) {
    let path = dir.join(done_file);
    match fs::remove_file(&path).await {
        Ok(()) => debug!(
            path = %path.display(),
            "dir_spool removed a stale done sentinel: production is live again"
        ),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => warn!(
            path = %path.display(),
            %error,
            "dir_spool could not remove the stale done sentinel; a stop_on_done consumer may exit early"
        ),
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
                // Remembered even though the batch is reported as partial and the route may
                // well retry or DLQ it: this producer was handed a message it did not
                // write, so it cannot claim production succeeded.
                self.write_failed.store(true, Ordering::Relaxed);
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

    // The route calls `on_disconnect_with_outcome`; this is the fallback for a caller that
    // closes the publisher without saying why. It is read as `Stopped`, the cautious answer:
    // an unexplained close is not evidence that production finished.
    fn on_disconnect_hook(&self) -> Option<BoxFuture<'_, anyhow::Result<()>>> {
        Some(Box::pin(self.close_with(DisconnectOutcome::Stopped)))
    }

    fn on_disconnect_with_outcome(
        &self,
        outcome: DisconnectOutcome,
    ) -> Option<BoxFuture<'_, anyhow::Result<()>>> {
        // Closing is this producer's whole statement about production: the sentinel it may
        // owe a `stop_on_done` consumer, and the lock the next producer needs.
        Some(Box::pin(self.close_with(outcome)))
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
    /// Chunk names left over from the last directory scan, in queue order. Batches are
    /// served from the front of this and the directory is only walked again once it runs
    /// out, so a deep backlog is not re-listed and re-sorted for every batch.
    ready: VecDeque<String>,
    /// Chunks a nack released, waiting to be folded back into `ready`. Written by the
    /// commit closures, which run on the route's tasks, hence the shared mutex.
    requeued: Arc<StdMutex<Vec<String>>>,
    /// Directory scans performed. Only read by the tests, which assert that draining a
    /// backlog does not scan per batch; a counter bump is cheaper than a test hook.
    scans: u64,
    /// The consumer lock, released when this consumer is dropped. Only a *draining*
    /// consumer takes one — see [`DirSpoolConsumer::new_with_source_metadata`].
    _lock: Option<pidlock::Pidlock>,
    exit_on_empty: bool,
}

impl DirSpoolConsumer {
    pub async fn new(config: &DirSpoolConfig) -> anyhow::Result<Self> {
        Self::new_with_source_metadata(config, config.source_metadata).await
    }

    /// `source_metadata` is the effective flag: the route turns it on for an idempotent
    /// output even when the input config leaves it unset.
    ///
    /// Only a *draining* consumer claims the directory. `drain_on_read: false` reads
    /// without deleting, so several such readers over one spool each see every chunk once
    /// — a supported fan-out, not a conflict. Such a reader does look for a draining
    /// consumer's claim, because that one deletes chunks out from under it.
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
        validate_control_files(config)?;
        let lock = if config.drain_on_read {
            acquire_lock(&dir, SpoolRole::Consumer, config)?
        } else {
            warn_if_drained(&dir, config);
            None
        };
        Ok(Self {
            dir,
            path: config.path.clone(),
            done_file: config.done_file.clone(),
            payload_suffix,
            metadata_suffix: config.metadata_suffix().map(str::to_string),
            drain_on_read: config.drain_on_read,
            stop_on_done: config.stop_on_done,
            poll_interval: Duration::from_millis(config.poll_interval_ms),
            source_metadata,
            claimed: Arc::new(StdMutex::new(HashSet::new())),
            ready: VecDeque::new(),
            requeued: Arc::new(StdMutex::new(Vec::new())),
            scans: 0,
            _lock: lock,
            exit_on_empty: false,
        })
    }

    /// Up to `limit` base names of finalized chunks not already in flight, in queue order.
    ///
    /// Served from the cached listing, which is refilled by a directory scan only once it
    /// is exhausted. That is what keeps a drain linear in the queue depth: at
    /// `batch_size: 128` a 100k backlog costs two scans, not N scans and N sorts.
    async fn list_ready(&mut self, limit: usize) -> anyhow::Result<Vec<String>> {
        self.absorb_requeued();
        if self.ready.is_empty() {
            self.refill_ready(limit).await?;
        }
        let take = limit.min(self.ready.len());
        Ok(self.ready.drain(..take).collect())
    }

    /// Walks the directory and rebuilds the cached listing.
    ///
    /// Only the lexically smallest `capacity` names are kept, in a max-heap that evicts as
    /// it goes, so the cost is one pass and bounded memory even for a directory holding
    /// millions of chunks — the old code collected and sorted every name to then discard
    /// all but a batch's worth.
    async fn refill_ready(&mut self, limit: usize) -> anyhow::Result<()> {
        let capacity = limit.max(READY_CACHE_CAPACITY);
        // Snapshot rather than lock per entry: the guard must not be held across the
        // `next_entry` await, which is what keeps this future `Send`. The set only holds
        // chunks in flight (or, with `drain_on_read` off, already-read ones), and a refill
        // happens once per cache-full of messages, so the clone is not on the hot path.
        let claimed = self
            .claimed
            .lock()
            .expect("dir_spool claim set poisoned")
            .clone();
        let mut entries = fs::read_dir(&self.dir)
            .await
            .with_context(|| format!("Failed to list dir_spool directory: {}", self.path))?;
        self.scans += 1;
        let mut smallest: BinaryHeap<String> = BinaryHeap::new();
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
            if claimed.contains(base) {
                continue;
            }
            if smallest.len() < capacity {
                smallest.push(base.to_string());
            } else if smallest.peek().is_some_and(|worst| base < worst.as_str()) {
                smallest.pop();
                smallest.push(base.to_string());
            }
        }
        // Lexical order is queue order — that is the contract `naming_pattern` documents.
        self.ready = smallest.into_sorted_vec().into();
        Ok(())
    }

    /// Folds the chunks a nack released back into the cached listing, so a redelivery does
    /// not have to wait for the backlog ahead of it to drain and trigger the next scan.
    fn absorb_requeued(&mut self) {
        let mut returned = {
            let mut requeued = self.requeued.lock().expect("dir_spool requeue poisoned");
            if requeued.is_empty() {
                return;
            }
            std::mem::take(&mut *requeued)
        };
        returned.sort_unstable();
        returned.dedup();
        // Merged, not appended-and-resorted: both sides are already in queue order and the
        // cache can hold tens of thousands of names.
        let cached = std::mem::take(&mut self.ready);
        let mut merged = VecDeque::with_capacity(returned.len() + cached.len());
        let mut returned = returned.into_iter().peekable();
        let mut cached = cached.into_iter().peekable();
        loop {
            let pick = match (returned.peek(), cached.peek()) {
                (Some(left), Some(right)) => Some(left.cmp(right)),
                (Some(_), None) => Some(std::cmp::Ordering::Less),
                (None, Some(_)) => Some(std::cmp::Ordering::Greater),
                (None, None) => break,
            };
            match pick {
                Some(std::cmp::Ordering::Less) => merged.extend(returned.next()),
                Some(std::cmp::Ordering::Greater) => merged.extend(cached.next()),
                // Equal means a nack landed after a refill scan had already picked the
                // chunk up — it was unclaimed by then. Keep one: the cache must never hold
                // a name twice, or the chunk would be delivered twice.
                _ => {
                    merged.extend(cached.next());
                    returned.next();
                }
            }
        }
        self.ready = merged;
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

    /// Reads every chunk in `ready`, returning the messages and the base names they came
    /// from.
    ///
    /// A chunk that vanished between the listing and the read is skipped and the rest of the
    /// batch delivered. That is recovery, not a claim protocol: nothing on disk marks a
    /// chunk as taken, so under `claim: warn`/`off` two draining consumers can both read one
    /// before either deletes it, and both will deliver it. The consumer lock is what keeps
    /// that from happening; this path covers an operator or a foreign tool clearing chunks
    /// out from under a listing.
    async fn read_ready(&self, ready: Vec<String>) -> (Vec<CanonicalMessage>, Vec<String>) {
        let mut messages = Vec::with_capacity(ready.len());
        let mut delivered = Vec::with_capacity(ready.len());
        for base in ready {
            match self.read_chunk(&base).await {
                Ok(message) => {
                    messages.push(message);
                    delivered.push(base);
                }
                Err(error) => {
                    warn!(chunk = %base, error = %error, "dir_spool skipping unreadable chunk");
                }
            }
        }
        (messages, delivered)
    }

    /// Whether production has been declared finished. That is the sentinel, not the
    /// producer lock: a spool can be filled by several producers in turn, so a producer
    /// closing says nothing about whether another is coming.
    async fn done_present(&self) -> bool {
        fs::try_exists(self.dir.join(&self.done_file))
            .await
            .unwrap_or(false)
    }

    /// What to hand back when the directory holds nothing to read.
    async fn idle(&self) -> Result<ReceivedBatch, ConsumerError> {
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
        Ok(ReceivedBatch::empty())
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
        // Two attempts: a cached listing whose chunks have all been deleted under us is
        // stale, so drop it and rescan once rather than reporting an empty queue over a
        // directory that may well have filled up again.
        let mut messages = Vec::new();
        let mut delivered = Vec::new();
        for _ in 0..2 {
            let ready = self
                .list_ready(max_messages)
                .await
                .map_err(ConsumerError::Connection)?;
            if ready.is_empty() {
                return self.idle().await;
            }
            (messages, delivered) = self.read_ready(ready).await;
            if !messages.is_empty() {
                break;
            }
            self.ready.clear();
        }
        if messages.is_empty() {
            // Two listings' worth of chunks that would not read. They are still on disk, so
            // this is not the end of the stream; back off and let the next poll retry.
            if !self.exit_on_empty {
                tokio::time::sleep(self.poll_interval).await;
            }
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
        let requeued = Arc::clone(&self.requeued);
        let commit: crate::traits::BatchCommitFunc = Box::new(move |dispositions| {
            Box::pin(async move {
                let mut release = Vec::new();
                let mut redeliver = Vec::new();
                for (index, base) in delivered.iter().enumerate() {
                    // A missing disposition means the route acked the whole batch.
                    let acked = !matches!(dispositions.get(index), Some(MessageDisposition::Nack));
                    if !acked {
                        // Put it back in the queue: a nack is a request to redeliver, and
                        // the chunk is still on disk to redeliver from. It goes back into
                        // the cached listing too, so the redelivery does not wait for a
                        // whole backlog to drain first.
                        release.push(base.clone());
                        redeliver.push(base.clone());
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
                if !redeliver.is_empty() {
                    requeued
                        .lock()
                        .expect("dir_spool requeue poisoned")
                        .extend(redeliver);
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
