//  mq-bridge
//  © Copyright 2025, by Marco Mengelkoch
//  Licensed under MIT License, see License file for more details
//  git clone https://github.com/marcomq/mq-bridge
//
//! Cloud object-store endpoint (S3 / GCS / Azure Blob / R2 / ...), built on the
//! `object_store` crate (the same backend as the checkpoint store).
//!
//! - **Sink** ([`ObjectStorePublisher`]): each flushed batch is encoded (reusing the
//!   file endpoint's [`FileFormat`] codecs) and written as one immutable object at
//!   `<prefix>/[YYYY/MM/DD/]<uuidv7>.<ext>`. Objects are write-once; nothing is appended
//!   or mutated. The uuidv7 name sorts by write time; the date prefix is a readability /
//!   lifecycle-rule convenience only.
//! - **Source** ([`ObjectStoreConsumer`]): objects under `prefix` are listed in key order,
//!   fetched whole, split by `delimiter`, and emitted. Progress is a durable cursor holding
//!   the last fully-acked object key (via the external checkpoint store), so a restart
//!   resumes without re-emitting. Objects are never deleted or rewritten — resume is
//!   non-destructive, at-least-once at object granularity.

use crate::checkpoint::{self, CheckpointBackend, CheckpointStore};
use crate::endpoints::file::{encode_record, parse_delimiter, parse_message};
use crate::models::{Compression, FileFormat, ObjectStoreConfig};
#[cfg(feature = "encryption")]
use crate::support::crypto::Crypto;
use crate::support::source_ranges::{finalized_name, CoveredRanges};
use crate::traits::{
    BoxFuture, ConsumerError, MessageConsumer, MessageDisposition, MessagePublisher,
    PublisherError, ReceivedBatch, SentBatch,
};
use crate::CanonicalMessage;
use anyhow::{anyhow, Context};
use async_trait::async_trait;
use futures::StreamExt;
use object_store::{path::Path as ObjPath, ObjectStore, ObjectStoreExt, PutPayload};
use std::any::Any;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tracing::{debug, info, trace, warn};

/// Builds an `object_store` backend and its base prefix `Path` from a URL, reading
/// credentials from the environment. Shared with the object-store checkpoint backend so
/// both resolve creds identically — see [`checkpoint::object_store_backend::build_store`].
use crate::checkpoint::object_store_backend::build_store;

/// True if two object-store URLs share a scheme+host and one's path segments are a prefix
/// of the other's — i.e. a recursive `list` of one would surface objects of the other.
/// Used to reject a checkpoint location that overlaps the source prefix.
fn object_urls_overlap(a: &str, b: &str) -> bool {
    let (pa, pb) = match (url::Url::parse(a), url::Url::parse(b)) {
        (Ok(x), Ok(y)) => (x, y),
        _ => return false,
    };
    if pa.scheme() != pb.scheme() || pa.host_str() != pb.host_str() {
        return false;
    }
    let seg = |u: &url::Url| -> Vec<String> {
        u.path_segments()
            .map(|s| s.filter(|p| !p.is_empty()).map(str::to_string).collect())
            .unwrap_or_default()
    };
    let (sa, sb) = (seg(&pa), seg(&pb));
    // Segment-wise prefix match (covers equality); "data" and "database" do NOT overlap.
    sa.iter().zip(sb.iter()).all(|(x, y)| x == y)
}

/// Default object extension derived from the record format, compression and
/// encryption (e.g. `jsonl`, `jsonl.gz`, `jsonl.lz4`, `jsonl.gz.enc`). An
/// encrypted object is ciphertext, so it gets a trailing `.enc` rather than a
/// bare `.gz`/`.lz4` that tools would wrongly treat as directly decompressable.
fn extension_for(format: &FileFormat, compression: Compression, encrypted: bool) -> String {
    let base = match format {
        FileFormat::Normal | FileFormat::Json | FileFormat::Text => "jsonl",
        FileFormat::Csv => "csv",
        FileFormat::Raw => "bin",
    };
    let mut ext = match compression {
        Compression::None => base.to_string(),
        Compression::Gzip => format!("{base}.gz"),
        Compression::Lz4 => format!("{base}.lz4"),
        Compression::Zstd => format!("{base}.zst"),
    };
    if encrypted {
        ext.push_str(".enc");
    }
    ext
}

/// Rejects `compression`/`encryption` settings whose Cargo feature is missing.
fn validate_object_settings(_config: &ObjectStoreConfig) -> anyhow::Result<()> {
    #[cfg(not(feature = "compression"))]
    if _config.compression != Compression::None {
        return Err(anyhow!(
            "object_store 'compression' requires the `compression` feature"
        ));
    }
    #[cfg(not(feature = "encryption"))]
    if _config.encryption.is_some() {
        return Err(anyhow!(
            "object_store 'encryption' requires the `encryption` feature"
        ));
    }
    Ok(())
}

/// Rebuilds the covered source ranges from the object names already present under `base`.
///
/// Unlike the file sink this never deletes anything: a single PUT under the final name *is*
/// the commit here (object stores have no atomic rename), so this path never creates staging
/// objects and has no leftovers of its own to reap. Anything that is not a parseable part
/// name — including a `.stage-` object — belongs to someone else and is left untouched.
async fn recover_idempotent_object_ranges(
    store: &dyn ObjectStore,
    base: &ObjPath,
    extension: &str,
) -> anyhow::Result<CoveredRanges> {
    let mut names = Vec::new();
    let mut stream = store.list(Some(base));
    while let Some(meta) = stream.next().await {
        let key = meta?.location.to_string();
        if let Some(name) = key.rsplit('/').next() {
            names.push(name.to_string());
        }
    }
    Ok(CoveredRanges::from_finalized_names(
        names.iter().map(String::as_str),
        extension,
    ))
}

/// Splits a fetched object into record slices on `delimiter`, dropping a trailing empty
/// remainder and a stray `\r` before a `\n` delimiter (mirrors the file reader).
fn split_records<'a>(data: &'a [u8], delimiter: &[u8]) -> Vec<&'a [u8]> {
    let mut records = Vec::new();
    if delimiter.is_empty() {
        return records;
    }
    let newline = delimiter.len() == 1 && delimiter[0] == b'\n';
    let mut start = 0;
    let mut i = 0;
    while i + delimiter.len() <= data.len() {
        if &data[i..i + delimiter.len()] == delimiter {
            let mut end = i;
            if newline && end > start && data[end - 1] == b'\r' {
                end -= 1;
            }
            records.push(&data[start..end]);
            i += delimiter.len();
            start = i;
        } else {
            i += 1;
        }
    }
    if start < data.len() {
        // Trailing record with no closing delimiter.
        records.push(&data[start..]);
    }
    records
}

/// Splits and decodes an object's bytes into messages, threading CSV header state across
/// the object's lines (so the first CSV row establishes the schema).
fn split_and_parse(data: &[u8], delimiter: &[u8], format: &FileFormat) -> Vec<CanonicalMessage> {
    let mut out = Vec::new();
    let mut csv_header: Option<Vec<String>> = None;
    for record in split_records(data, delimiter) {
        if let Some(msg) = parse_message(record, format, &mut csv_header) {
            out.push(msg);
        }
    }
    out
}

fn empty_batch() -> ReceivedBatch {
    ReceivedBatch {
        messages: Vec::new(),
        commit: Box::new(|_| Box::pin(async { Ok(()) })),
    }
}

// --- Publisher (sink) ---

/// Writes each batch as one immutable object under the configured prefix.
#[derive(Clone)]
pub struct ObjectStorePublisher {
    store: Arc<dyn ObjectStore>,
    base: ObjPath,
    delimiter: Vec<u8>,
    format: FileFormat,
    #[cfg(feature = "compression")]
    compression: Compression,
    #[cfg(feature = "encryption")]
    crypto: Option<Arc<Crypto>>,
    date_partition: bool,
    extension: String,
    idempotency: bool,
    covered_ranges: Arc<Mutex<CoveredRanges>>,
}

impl ObjectStorePublisher {
    pub async fn new(config: &ObjectStoreConfig) -> anyhow::Result<Self> {
        if matches!(config.format, FileFormat::Csv) {
            // Each object is independent, so CSV would need its own header row per object.
            // Not implemented for the sink; sources can still read CSV objects.
            return Err(anyhow!(
                "object_store sink does not support the 'csv' format (per-object CSV headers are unimplemented); use jsonl/json/text/raw"
            ));
        }
        validate_object_settings(config)?;
        let (store, base) = build_store(&config.url)?;
        let store: Arc<dyn ObjectStore> = Arc::from(store);
        let delimiter = parse_delimiter(config.delimiter.as_deref())?;
        let extension = config.extension.clone().unwrap_or_else(|| {
            extension_for(
                &config.format,
                config.compression,
                config.encryption.is_some(),
            )
        });
        let covered_ranges = if config.idempotency {
            if config.date_partition {
                // `date_partition` defaults to true, so this cannot tell an explicit
                // setting from the default — keep it out of the operator's warning log.
                debug!("object_store 'idempotency' ignores 'date_partition'; part names carry the source range and are written flat under the prefix");
            }
            recover_idempotent_object_ranges(store.as_ref(), &base, &extension).await?
        } else {
            CoveredRanges::default()
        };
        info!(url = %config.url, format = ?config.format, idempotency = config.idempotency, "Object-store sink opened");
        Ok(Self {
            store,
            base,
            delimiter,
            format: config.format.clone(),
            #[cfg(feature = "compression")]
            compression: config.compression,
            #[cfg(feature = "encryption")]
            crypto: config
                .encryption
                .as_ref()
                .map(Crypto::new)
                .transpose()?
                .map(Arc::new),
            // The idempotent path names objects by source range and writes them flat under the
            // base prefix; a date layout would just be ignored, so make the override visible.
            date_partition: config.date_partition && !config.idempotency,
            extension,
            idempotency: config.idempotency,
            covered_ranges: Arc::new(Mutex::new(covered_ranges)),
        })
    }

    /// Object key for the next write: `<prefix>/[YYYY/MM/DD/]<uuidv7>.<ext>`.
    ///
    /// The uuidv7 name already sorts by write time; the optional date prefix is derived
    /// from that same id's embedded millisecond timestamp (no wall-clock dependency), so
    /// the folder and the name can never disagree.
    fn next_key(&self) -> ObjPath {
        let id = fast_uuid_v7::gen_id();
        let name = format!("{}.{}", fast_uuid_v7::format_uuid(id), self.extension);
        if self.date_partition {
            // Top 48 bits of a uuidv7 are the Unix-epoch millisecond timestamp.
            let (y, m, d) = civil_from_unix_ms((id >> 80) as u64);
            self.base
                .clone()
                .join(format!("{y:04}").as_str())
                .join(format!("{m:02}").as_str())
                .join(format!("{d:02}").as_str())
                .join(name.as_str())
        } else {
            self.base.clone().join(name.as_str())
        }
    }

    /// Compress-then-encrypt a fully built object body. The whole object is one member, so a
    /// compressed object stays a standard `.gz`/`.lz4` file and a sealed one is a single
    /// envelope — no framing, because objects are written and read whole. Both write paths go
    /// through here, so an idempotent part object is shaped exactly like an ordinary one and
    /// [`ObjectStoreConsumer::decode_object`] reads either without knowing which wrote it.
    #[allow(unused_mut)]
    fn encode_object_body(&self, mut body: Vec<u8>) -> Result<Vec<u8>, PublisherError> {
        #[cfg(feature = "compression")]
        if self.compression != Compression::None {
            body = crate::support::compression::compress_member(self.compression, &body)
                .map_err(|error| PublisherError::NonRetryable(anyhow!(error)))?;
        }
        #[cfg(feature = "encryption")]
        if let Some(crypto) = &self.crypto {
            body = crypto
                .seal(&body, b"")
                .map_err(PublisherError::NonRetryable)?;
        }
        Ok(body)
    }

    async fn send_batch_idempotent(
        &self,
        messages: Vec<CanonicalMessage>,
    ) -> Result<SentBatch, PublisherError> {
        let mut covered = self.covered_ranges.lock().await;
        let runs = covered
            .uncovered_runs(messages)
            .map_err(PublisherError::NonRetryable)?;

        for run in runs {
            let name = finalized_name(&run.source, run.start, run.end, &self.extension)
                .map_err(PublisherError::NonRetryable)?;
            let mut body = Vec::new();
            for mut message in run.messages {
                message.strip_source_metadata();
                let bytes = encode_record(&message, &self.format)
                    .map_err(|error| PublisherError::NonRetryable(anyhow!(error)))?;
                body.extend_from_slice(&bytes);
                body.extend_from_slice(&self.delimiter);
            }
            let body = self.encode_object_body(body)?;
            let key = self.base.clone().join(name.as_str());
            self.store
                .put(&key, PutPayload::from(body))
                .await
                .map_err(|error| {
                    PublisherError::Retryable(
                        anyhow!(error).context(format!("object-store put idempotent '{key}'")),
                    )
                })?;
            covered
                .insert(run.source, run.start, run.end)
                .map_err(PublisherError::NonRetryable)?;
        }
        Ok(SentBatch::Ack)
    }
}

/// Converts Unix-epoch milliseconds (UTC) to a `(year, month, day)` civil date using
/// Howard Hinnant's `civil_from_days` algorithm — avoids a date-crate dependency.
fn civil_from_unix_ms(ms: u64) -> (i64, u32, u32) {
    let days = (ms / 86_400_000) as i64;
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097; // [0, 146096]
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365; // [0, 399]
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
    let mp = (5 * doy + 2) / 153; // [0, 11]
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32; // [1, 31]
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32; // [1, 12]
    (if m <= 2 { y + 1 } else { y }, m, d)
}

#[async_trait]
impl MessagePublisher for ObjectStorePublisher {
    async fn send_batch(
        &self,
        messages: Vec<CanonicalMessage>,
    ) -> Result<SentBatch, PublisherError> {
        if messages.is_empty() {
            return Ok(SentBatch::Ack);
        }
        if self.idempotency {
            return self.send_batch_idempotent(messages).await;
        }
        let mut body = Vec::new();
        let mut failed = Vec::new();
        for mut msg in messages {
            msg.strip_source_metadata();
            match encode_record(&msg, &self.format) {
                Ok(bytes) => {
                    body.extend_from_slice(&bytes);
                    body.extend_from_slice(&self.delimiter);
                }
                Err(e) => {
                    failed.push((msg, PublisherError::NonRetryable(anyhow!(e))));
                }
            }
        }
        if body.is_empty() {
            // Every message failed to encode; nothing to write.
            return Ok(SentBatch::Partial {
                responses: None,
                failed,
            });
        }
        let body = self.encode_object_body(body)?;
        let key = self.next_key();
        self.store
            .put(&key, PutPayload::from(body))
            .await
            .map_err(|e| {
                PublisherError::Retryable(anyhow!(e).context(format!("object-store put '{key}'")))
            })?;
        trace!(key = %key, "Wrote object to object store");
        if failed.is_empty() {
            Ok(SentBatch::Ack)
        } else {
            Ok(SentBatch::Partial {
                responses: None,
                failed,
            })
        }
    }

    async fn flush(&self) -> anyhow::Result<()> {
        Ok(())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

// --- Consumer (source) ---

/// Tracks the object currently being drained and how many of its records are still
/// un-acked, so the durable cursor advances only once an object is fully consumed.
struct ObjProgress {
    key: String,
    remaining: usize,
}

/// Reads objects under a prefix in key order, resuming from a durable cursor.
pub struct ObjectStoreConsumer {
    store: Arc<dyn ObjectStore>,
    base: ObjPath,
    delimiter: Vec<u8>,
    format: FileFormat,
    #[cfg(feature = "compression")]
    compression: Compression,
    #[cfg(feature = "encryption")]
    crypto: Option<Arc<Crypto>>,
    checkpoint: Option<Arc<dyn CheckpointStore>>,
    /// Last fully-acked object key (the resume cursor).
    last_key: Arc<Mutex<Option<String>>>,
    /// Undelivered records of the current object.
    buffer: Arc<Mutex<Vec<CanonicalMessage>>>,
    /// Set while an object is in flight (buffered or awaiting commits); cleared when fully acked.
    progress: Arc<Mutex<Option<ObjProgress>>>,
    idle_delay: Duration,
    /// Reject objects larger than this many bytes rather than buffering them whole; `None` = no limit.
    max_object_bytes: Option<u64>,
    /// Consecutive decode failures on `decode_failing_key` before it is quarantined.
    decode_failures: u32,
    decode_failing_key: Option<String>,
}

/// Consecutive decode failures on one object before it is quarantined (skipped) so a
/// single poison object cannot block the source forever.
const MAX_OBJECT_DECODE_FAILURES: u32 = 5;

impl ObjectStoreConsumer {
    pub async fn new(config: &ObjectStoreConfig) -> anyhow::Result<Self> {
        validate_object_settings(config)?;
        let (store, base) = build_store(&config.url)?;
        let delimiter = parse_delimiter(config.delimiter.as_deref())?;

        // Durable resume needs an external checkpoint store: an object store has no cheap
        // per-key cursor row, so the source-datastore backend is rejected here.
        let checkpoint: Option<Arc<dyn CheckpointStore>> = match (
            &config.cursor_id,
            &config.checkpoint_store,
        ) {
            (Some(cid), Some(spec)) => match checkpoint::parse_checkpoint_store(spec)? {
                CheckpointBackend::Source { .. } => {
                    // Same misconfiguration class as the overlap check below: permanent.
                    return Err(anyhow::Error::new(ConsumerError::Permanent(anyhow!(
                            "object_store source requires an external checkpoint_store (file://, s3://, postgres://, or mongodb://); a source-datastore checkpoint is not available."
                        ))));
                }
                external => {
                    // Guard against the cursor object landing under the source prefix, where
                    // it would be listed and re-emitted as data.
                    if let CheckpointBackend::ObjectStore { url: ck_url } = &external {
                        if object_urls_overlap(&config.url, ck_url) {
                            // Misconfiguration: rebuilding the consumer reads the same config,
                            // so this must stop the route rather than reconnect forever.
                            return Err(anyhow::Error::new(ConsumerError::Permanent(anyhow!(
                                "object_store checkpoint_store '{ck_url}' overlaps the source prefix '{}'; the cursor object would be listed and re-read as data. Point checkpoint_store at a different bucket or prefix.",
                                config.url
                            ))));
                        }
                    }
                    Some(checkpoint::build_external_store(external, &config.url, cid).await?)
                }
            },
            (Some(_), None) => {
                warn!(
                    url = %config.url,
                    "object_store source has cursor_id but no checkpoint_store; resume is disabled and every restart re-emits all objects. Set an external checkpoint_store (file://, s3://, postgres://, mongodb://)."
                );
                None
            }
            (None, _) => {
                warn!(
                    url = %config.url,
                    "object_store source has no cursor_id; resume is disabled and every restart re-emits all objects."
                );
                None
            }
        };

        let last_key = match &checkpoint {
            Some(cp) => cp.load().await?,
            None => None,
        };

        info!(
            url = %config.url,
            has_checkpoint = %last_key.is_some(),
            "Object-store source connected"
        );

        Ok(Self {
            store: Arc::from(store),
            base,
            delimiter,
            format: config.format.clone(),
            #[cfg(feature = "compression")]
            compression: config.compression,
            #[cfg(feature = "encryption")]
            crypto: config
                .encryption
                .as_ref()
                .map(Crypto::new)
                .transpose()?
                .map(Arc::new),
            checkpoint,
            last_key: Arc::new(Mutex::new(last_key)),
            buffer: Arc::new(Mutex::new(Vec::new())),
            progress: Arc::new(Mutex::new(None)),
            idle_delay: Duration::from_millis(config.polling_interval_ms.unwrap_or(1000)),
            max_object_bytes: config.max_object_bytes,
            decode_failures: 0,
            decode_failing_key: None,
        })
    }

    #[cfg(test)]
    fn from_store(
        store: Arc<dyn ObjectStore>,
        base: ObjPath,
        format: FileFormat,
        checkpoint: Option<Arc<dyn CheckpointStore>>,
        last_key: Option<String>,
    ) -> Self {
        Self {
            store,
            base,
            delimiter: vec![b'\n'],
            format,
            #[cfg(feature = "compression")]
            compression: Compression::None,
            #[cfg(feature = "encryption")]
            crypto: None,
            checkpoint,
            last_key: Arc::new(Mutex::new(last_key)),
            buffer: Arc::new(Mutex::new(Vec::new())),
            progress: Arc::new(Mutex::new(None)),
            idle_delay: Duration::from_millis(10),
            max_object_bytes: None,
            decode_failures: 0,
            decode_failing_key: None,
        }
    }

    /// Fetches the next object strictly after `last` (in key order), skipping directory
    /// markers. Relies on the store listing keys in lexicographic order (S3/GCS/Azure/local
    /// /in-memory all do); `list_with_offset` also filters server-side when resuming.
    async fn next_object(&self, last: Option<&str>) -> anyhow::Result<Option<(String, Vec<u8>)>> {
        let mut stream = match last {
            Some(k) => self
                .store
                .list_with_offset(Some(&self.base), &ObjPath::from(k)),
            None => self.store.list(Some(&self.base)),
        };
        while let Some(meta) = stream.next().await {
            let meta = meta?;
            let key = meta.location.to_string();
            // Skip pseudo-directory markers; `list_with_offset` may also surface the offset key.
            if key.ends_with('/') || last == Some(key.as_str()) {
                continue;
            }
            // Refuse to materialize an over-large object; the listing already carries its size.
            // The object will not shrink, so retrying re-lists it forever: fail permanently.
            if let Some(limit) = self.max_object_bytes {
                if meta.size > limit {
                    return Err(anyhow::Error::new(ConsumerError::Permanent(anyhow!(
                        "object '{key}' is {} bytes, exceeding max_object_bytes ({limit}); refusing to buffer it whole. Raise max_object_bytes or remove the object.",
                        meta.size
                    ))));
                }
            }
            let data = self
                .store
                .get(&meta.location)
                .await?
                .bytes()
                .await?
                .to_vec();
            // Decode (decrypt/decompress) is deferred to `receive_batch` so a poison
            // object is bounded-retried then quarantined rather than blocking forever.
            return Ok(Some((key, data)));
        }
        Ok(None)
    }

    /// Bound on decompressed output.
    ///
    /// `max_object_bytes` caps the *stored* size (see `next_object`), so reusing it here
    /// would reject any object that compressed better than 1:1 — i.e. most of them. Scale
    /// it instead; the result is still bounded, so a decompression bomb cannot run away.
    #[cfg(feature = "compression")]
    fn decompressed_limit(&self) -> Option<u64> {
        const MAX_DECOMPRESSED_EXPANSION: u64 = 20;
        self.max_object_bytes
            .map(|limit| limit.saturating_mul(MAX_DECOMPRESSED_EXPANSION))
    }

    /// Decrypt-then-decompress a fetched object whole (the write path compressed first).
    #[allow(unused_variables)]
    fn decode_object(&self, key: &str, data: Vec<u8>) -> anyhow::Result<Vec<u8>> {
        #[cfg(feature = "encryption")]
        let data = if let Some(crypto) = &self.crypto {
            crypto
                .open(&data, b"")
                .with_context(|| format!("decrypt object '{key}'"))?
        } else {
            data
        };
        #[cfg(feature = "compression")]
        let data = if self.compression != Compression::None {
            crate::support::compression::decompress_all(
                self.compression,
                &data,
                self.decompressed_limit(),
            )
            .with_context(|| format!("decompress object '{key}'"))?
        } else {
            data
        };
        Ok(data)
    }

    /// Persists the resume cursor durably, then advances the in-memory cursor. The durable
    /// save happens first and its error is propagated: `last_key` only moves once progress is
    /// safely checkpointed, so a failed save re-lists the object rather than silently skipping it.
    async fn save_cursor(&self, key: &str) -> anyhow::Result<()> {
        if let Some(cp) = &self.checkpoint {
            cp.save(key)
                .await
                .with_context(|| format!("persist object-store cursor '{key}'"))?;
        }
        *self.last_key.lock().await = Some(key.to_string());
        Ok(())
    }
}

#[async_trait]
impl MessageConsumer for ObjectStoreConsumer {
    async fn receive_batch(&mut self, max_messages: usize) -> Result<ReceivedBatch, ConsumerError> {
        if max_messages == 0 {
            return Ok(empty_batch());
        }

        // Refill only when the current object is fully accounted for (buffer empty AND no
        // in-flight commits). This keeps objects from interleaving so the cursor advances
        // strictly in key order.
        {
            let buffer_empty = self.buffer.lock().await.is_empty();
            let in_flight = self.progress.lock().await.is_some();
            if buffer_empty && !in_flight {
                let last = self.last_key.lock().await.clone();
                // `from` (not `Connection`) so a permanent listing failure — e.g. an object
                // over `max_object_bytes` — is not retried as a transport blip.
                match self
                    .next_object(last.as_deref())
                    .await
                    .map_err(ConsumerError::from)?
                {
                    None => {
                        tokio::time::sleep(self.idle_delay).await;
                        return Ok(empty_batch());
                    }
                    Some((key, raw)) => {
                        // Decode here so a poison object is bounded-retried then
                        // quarantined (cursor advanced past it) rather than looping forever.
                        let data = match self.decode_object(&key, raw) {
                            Ok(data) => {
                                self.decode_failing_key = None;
                                self.decode_failures = 0;
                                data
                            }
                            Err(e) => {
                                if self.decode_failing_key.as_deref() == Some(key.as_str()) {
                                    self.decode_failures += 1;
                                } else {
                                    self.decode_failing_key = Some(key.clone());
                                    self.decode_failures = 1;
                                }
                                if self.decode_failures >= MAX_OBJECT_DECODE_FAILURES {
                                    warn!(
                                        key = %key,
                                        error = %e,
                                        "object failed to decode {MAX_OBJECT_DECODE_FAILURES} times; quarantining (advancing cursor past it)"
                                    );
                                    self.save_cursor(&key)
                                        .await
                                        .map_err(ConsumerError::Connection)?;
                                    self.decode_failing_key = None;
                                    self.decode_failures = 0;
                                    tokio::time::sleep(self.idle_delay).await;
                                    return Ok(empty_batch());
                                }
                                // Under the limit: retry the same object next poll, but do
                                // NOT surface a Connection error — that would rebuild the
                                // consumer (resetting decode_failures/decode_failing_key) and
                                // let a poison object loop forever without ever quarantining.
                                // Idle instead so the in-instance counter keeps climbing to
                                // MAX_OBJECT_DECODE_FAILURES.
                                warn!(
                                    key = %key,
                                    error = %e,
                                    attempt = self.decode_failures,
                                    max = MAX_OBJECT_DECODE_FAILURES,
                                    "object failed to decode; will retry before quarantining"
                                );
                                tokio::time::sleep(self.idle_delay).await;
                                return Ok(empty_batch());
                            }
                        };
                        let records = split_and_parse(&data, &self.delimiter, &self.format);
                        if records.is_empty() {
                            // No data records (e.g. a lone CSV header): advance past it so we
                            // don't re-list it forever, then idle.
                            self.save_cursor(&key)
                                .await
                                .map_err(ConsumerError::Connection)?;
                            tokio::time::sleep(self.idle_delay).await;
                            return Ok(empty_batch());
                        }
                        let n = records.len();
                        *self.buffer.lock().await = records;
                        *self.progress.lock().await = Some(ObjProgress { key, remaining: n });
                    }
                }
            } else if buffer_empty {
                // Buffer drained but commits for the current object are still pending; the
                // commit will clear `progress`. Idle rather than fetch the next object.
                tokio::time::sleep(self.idle_delay).await;
                return Ok(empty_batch());
            }
        }

        let batch: Vec<CanonicalMessage> = {
            let mut buffer = self.buffer.lock().await;
            let count = buffer.len().min(max_messages);
            buffer.drain(0..count).collect()
        };

        let buffer_arc = self.buffer.clone();
        let progress_arc = self.progress.clone();
        let last_key_arc = self.last_key.clone();
        let checkpoint = self.checkpoint.clone();
        let batch_for_commit = batch.clone();

        let commit = Box::new(move |dispositions: Vec<MessageDisposition>| {
            Box::pin(async move {
                // Count the leading run of acks; the first nack and everything after it is
                // requeued to the front of the buffer for at-least-once re-delivery.
                let mut leading_acks = 0usize;
                let mut requeue = Vec::new();
                let mut hit_nack = false;
                for (i, d) in dispositions.iter().enumerate() {
                    if hit_nack {
                        if let Some(m) = batch_for_commit.get(i) {
                            requeue.push(m.clone());
                        }
                        continue;
                    }
                    match d {
                        MessageDisposition::Ack | MessageDisposition::Reply(_) => leading_acks += 1,
                        MessageDisposition::Nack => {
                            hit_nack = true;
                            if let Some(m) = batch_for_commit.get(i) {
                                requeue.push(m.clone());
                            }
                        }
                    }
                }
                // Defensive: requeue any tail the dispositions didn't cover.
                if dispositions.len() < batch_for_commit.len() {
                    for m in &batch_for_commit[dispositions.len()..] {
                        requeue.push(m.clone());
                    }
                }

                if !requeue.is_empty() {
                    let mut buf = buffer_arc.lock().await;
                    let old = std::mem::take(&mut *buf);
                    let mut new = requeue;
                    new.extend(old);
                    *buf = new;
                }

                if leading_acks > 0 {
                    let mut prog = progress_arc.lock().await;
                    if let Some(p) = prog.as_mut() {
                        p.remaining = p.remaining.saturating_sub(leading_acks);
                        if p.remaining == 0 {
                            // Object fully acked. Persist the resume cursor durably BEFORE
                            // advancing: if the save fails, drop the in-flight object without
                            // advancing so it is re-listed and re-emitted (at-least-once), and
                            // surface the error rather than acking progress that isn't durable.
                            let key = p.key.clone();
                            if let Some(cp) = &checkpoint {
                                if let Err(e) = cp.save(&key).await {
                                    *prog = None;
                                    return Err(anyhow!(e).context("persist object-store cursor"));
                                }
                            }
                            *prog = None;
                            drop(prog);
                            *last_key_arc.lock().await = Some(key);
                        }
                    }
                }
                Ok(())
            }) as BoxFuture<'static, anyhow::Result<()>>
        });

        Ok(ReceivedBatch {
            messages: batch,
            commit,
        })
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::traits::{MessageConsumer, MessagePublisher};
    use object_store::memory::InMemory;

    #[test]
    fn checkpoint_overlap_detection() {
        // Same bucket, checkpoint nested under the source prefix -> overlap.
        assert!(object_urls_overlap(
            "s3://bucket/data",
            "s3://bucket/data/cursors"
        ));
        // Identical location -> overlap.
        assert!(object_urls_overlap("s3://bucket/data", "s3://bucket/data"));
        // Sibling prefixes in the same bucket -> safe.
        assert!(!object_urls_overlap(
            "s3://bucket/data",
            "s3://bucket/cursors"
        ));
        // String-prefix but distinct segment -> safe.
        assert!(!object_urls_overlap(
            "s3://bucket/data",
            "s3://bucket/database"
        ));
        // Different bucket -> safe.
        assert!(!object_urls_overlap("s3://bucket/data", "s3://other/data"));
    }

    #[test]
    fn civil_date_from_unix_ms() {
        // 2026-07-17T00:00:00Z = 1_784_246_400_000 ms.
        assert_eq!(civil_from_unix_ms(1_784_246_400_000), (2026, 7, 17));
        // Epoch.
        assert_eq!(civil_from_unix_ms(0), (1970, 1, 1));
        // A leap day: 2024-02-29T12:00:00Z.
        assert_eq!(civil_from_unix_ms(1_709_208_000_000), (2024, 2, 29));
    }

    #[test]
    fn extension_reflects_compression_and_encryption() {
        assert_eq!(
            extension_for(&FileFormat::Normal, Compression::None, false),
            "jsonl"
        );
        assert_eq!(
            extension_for(&FileFormat::Normal, Compression::Gzip, false),
            "jsonl.gz"
        );
        assert_eq!(
            extension_for(&FileFormat::Raw, Compression::Lz4, false),
            "bin.lz4"
        );
        assert_eq!(
            extension_for(&FileFormat::Normal, Compression::Zstd, false),
            "jsonl.zst"
        );
        // Encrypted objects are ciphertext -> trailing `.enc`, never a bare `.gz`.
        assert_eq!(
            extension_for(&FileFormat::Normal, Compression::Gzip, true),
            "jsonl.gz.enc"
        );
        assert_eq!(
            extension_for(&FileFormat::Raw, Compression::None, true),
            "bin.enc"
        );
    }

    fn json_msg(v: serde_json::Value) -> CanonicalMessage {
        CanonicalMessage::new(serde_json::to_vec(&v).unwrap(), None)
    }

    fn test_publisher(store: Arc<dyn ObjectStore>) -> ObjectStorePublisher {
        ObjectStorePublisher {
            store,
            base: ObjPath::from("data"),
            delimiter: vec![b'\n'],
            format: FileFormat::Normal,
            #[cfg(feature = "compression")]
            compression: Compression::None,
            #[cfg(feature = "encryption")]
            crypto: None,
            date_partition: false,
            extension: "jsonl".to_string(),
            idempotency: false,
            covered_ranges: Arc::new(Mutex::new(CoveredRanges::default())),
        }
    }

    fn kafka_message(offset: i64) -> CanonicalMessage {
        let mut message = json_msg(serde_json::json!({ "offset": offset }));
        message
            .metadata
            .insert("mqb.src.kafka_topic".into(), "orders".into());
        message
            .metadata
            .insert("mqb.src.kafka_partition".into(), "0".into());
        message
            .metadata
            .insert("mqb.src.kafka_offset".into(), offset.to_string());
        message
    }

    #[tokio::test]
    async fn idempotent_sink_recovers_finalized_objects_and_leaves_foreign_keys_alone() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let mut publisher = test_publisher(store.clone());
        publisher.idempotency = true;
        publisher
            .send_batch(vec![kafka_message(0), kafka_message(1)])
            .await
            .unwrap();
        publisher
            .send_batch(vec![kafka_message(0), kafka_message(1)])
            .await
            .unwrap();

        store
            .put(
                &ObjPath::from("data/.stage-crash"),
                PutPayload::from(Vec::new()),
            )
            .await
            .unwrap();
        let mut restarted = test_publisher(store.clone());
        restarted.idempotency = true;
        restarted.covered_ranges = Arc::new(Mutex::new(
            recover_idempotent_object_ranges(store.as_ref(), &restarted.base, "jsonl")
                .await
                .unwrap(),
        ));
        restarted
            .send_batch(vec![kafka_message(0), kafka_message(1), kafka_message(2)])
            .await
            .unwrap();

        let mut stream = store.list(Some(&ObjPath::from("data")));
        let mut names = Vec::new();
        while let Some(item) = stream.next().await {
            names.push(item.unwrap().location.to_string());
        }
        names.sort();
        assert_eq!(
            names,
            vec![
                // Not a parseable part name, and not ours to delete.
                "data/.stage-crash".to_string(),
                "data/part-orders-0-0-1.jsonl".to_string(),
                "data/part-orders-0-2-2.jsonl".to_string(),
            ]
        );
    }

    #[tokio::test]
    async fn idempotency_overrides_date_partition_rather_than_rejecting_it() {
        // `date_partition` defaults to true, so erroring would reject every default config.
        let publisher = ObjectStorePublisher::new(&ObjectStoreConfig {
            url: "memory:///data".to_string(),
            idempotency: true,
            date_partition: true,
            ..Default::default()
        })
        .await
        .expect("date_partition is overridden, not rejected");
        assert!(!publisher.date_partition);
    }

    #[cfg(feature = "compression")]
    #[tokio::test]
    async fn idempotent_parts_are_compressed_and_named_for_it() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let mut publisher = test_publisher(store.clone());
        publisher.idempotency = true;
        publisher.compression = Compression::Gzip;
        publisher.extension = extension_for(&FileFormat::Json, Compression::Gzip, false);
        publisher
            .send_batch(vec![kafka_message(0), kafka_message(1)])
            .await
            .unwrap();

        // The part name advertises the codec, and the bytes really are gzip.
        let key = ObjPath::from("data/part-orders-0-0-1.jsonl.gz");
        let stored = store.get(&key).await.unwrap().bytes().await.unwrap();
        let plain =
            crate::support::compression::decompress_all(Compression::Gzip, &stored, None).unwrap();
        assert_eq!(split_records(&plain, b"\n").len(), 2);

        // A restart parses the longer extension back out, so covered offsets are not rewritten.
        let recovered =
            recover_idempotent_object_ranges(store.as_ref(), &publisher.base, "jsonl.gz")
                .await
                .unwrap();
        let mut restarted = test_publisher(store.clone());
        restarted.idempotency = true;
        restarted.compression = Compression::Gzip;
        restarted.extension = publisher.extension.clone();
        restarted.covered_ranges = Arc::new(Mutex::new(recovered));
        restarted
            .send_batch(vec![kafka_message(0), kafka_message(1)])
            .await
            .unwrap();

        let mut stream = store.list(Some(&ObjPath::from("data")));
        let mut names = Vec::new();
        while let Some(item) = stream.next().await {
            names.push(item.unwrap().location.to_string());
        }
        assert_eq!(names, vec!["data/part-orders-0-0-1.jsonl.gz".to_string()]);
    }

    #[cfg(feature = "compression")]
    #[tokio::test]
    async fn compressed_object_round_trips() {
        for compression in [Compression::Gzip, Compression::Lz4, Compression::Zstd] {
            let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

            let mut publisher = test_publisher(store.clone());
            publisher.format = FileFormat::Raw;
            publisher.compression = compression;
            publisher.extension = extension_for(&FileFormat::Raw, compression, false);
            publisher
                .send_batch(vec![
                    json_msg(serde_json::json!({"n": 1})),
                    json_msg(serde_json::json!({"n": 2})),
                ])
                .await
                .unwrap();

            // The stored object is really compressed: it decodes to the two JSONL rows.
            let listed = store
                .list(Some(&ObjPath::from("data")))
                .next()
                .await
                .unwrap()
                .unwrap();
            let suffix = match compression {
                Compression::Gzip => ".bin.gz",
                Compression::Zstd => ".bin.zst",
                _ => ".bin.lz4",
            };
            assert!(listed.location.to_string().ends_with(suffix));
            let bytes = store
                .get(&listed.location)
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap();
            let decoded =
                crate::support::compression::decompress_all(compression, &bytes, None).unwrap();
            assert_eq!(String::from_utf8(decoded).unwrap().lines().count(), 2);

            let mut consumer = ObjectStoreConsumer::from_store(
                store,
                ObjPath::from("data"),
                FileFormat::Raw,
                None,
                None,
            );
            consumer.compression = compression;
            let batch = consumer.receive_batch(10).await.unwrap();
            assert_eq!(batch.messages.len(), 2);
            assert_eq!(batch.messages[0].payload.as_ref(), br#"{"n":1}"#);
            assert_eq!(batch.messages[1].payload.as_ref(), br#"{"n":2}"#);
            (batch.commit)(vec![MessageDisposition::Ack; 2])
                .await
                .unwrap();
        }
    }

    #[cfg(all(feature = "compression", feature = "encryption"))]
    #[tokio::test]
    async fn encrypted_object_round_trips() {
        use base64::Engine as _;

        let crypto_cfg = crate::models::EncryptionConfig {
            key: base64::engine::general_purpose::STANDARD.encode([42u8; 32]),
            ..Default::default()
        };
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

        let mut publisher = test_publisher(store.clone());
        publisher.format = FileFormat::Raw;
        publisher.compression = Compression::Gzip;
        publisher.crypto = Some(Arc::new(Crypto::new(&crypto_cfg).unwrap()));
        publisher
            .send_batch(vec![json_msg(serde_json::json!({"who": "alice"}))])
            .await
            .unwrap();

        // The stored object is ciphertext: not gzip, and no plaintext inside.
        let listed = store
            .list(Some(&ObjPath::from("data")))
            .next()
            .await
            .unwrap()
            .unwrap();
        let bytes = store
            .get(&listed.location)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert!(
            crate::support::compression::decompress_all(Compression::Gzip, &bytes, None).is_err()
        );
        assert!(!bytes.windows(5).any(|w| w == b"alice"));

        let mut consumer = ObjectStoreConsumer::from_store(
            store.clone(),
            ObjPath::from("data"),
            FileFormat::Raw,
            None,
            None,
        );
        consumer.compression = Compression::Gzip;
        consumer.crypto = Some(Arc::new(Crypto::new(&crypto_cfg).unwrap()));
        let batch = consumer.receive_batch(10).await.unwrap();
        assert_eq!(batch.messages.len(), 1);
        assert_eq!(batch.messages[0].payload.as_ref(), br#"{"who":"alice"}"#);

        // A consumer with the wrong key fails cleanly.
        let mut wrong = ObjectStoreConsumer::from_store(
            store,
            ObjPath::from("data"),
            FileFormat::Raw,
            None,
            None,
        );
        wrong.compression = Compression::Gzip;
        let wrong_cfg = crate::models::EncryptionConfig {
            key: base64::engine::general_purpose::STANDARD.encode([1u8; 32]),
            ..Default::default()
        };
        wrong.crypto = Some(Arc::new(Crypto::new(&wrong_cfg).unwrap()));
        // A wrong-key object is bounded-retried then quarantined: it never delivers
        // garbage and never surfaces a Connection error (which would rebuild the consumer,
        // reset the failure counter, and loop the poison object forever).
        for _ in 0..=MAX_OBJECT_DECODE_FAILURES {
            let batch = wrong
                .receive_batch(10)
                .await
                .expect("wrong-key object must not error out the source");
            assert!(batch.messages.is_empty());
        }
    }

    #[cfg(feature = "compression")]
    #[tokio::test]
    async fn gzip_source_quarantines_non_gzip_object() {
        // A gzip source must handle a non-gzip object cleanly (not panic): the object is
        // bounded-retried then quarantined, never delivered as data and never surfaced as a
        // Connection error that would rebuild the consumer and loop the poison object.
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        store
            .put(
                &ObjPath::from("data/not-gzip.jsonl.gz"),
                PutPayload::from(br#"{"n":1}"#.to_vec()),
            )
            .await
            .unwrap();

        let mut consumer = ObjectStoreConsumer::from_store(
            store,
            ObjPath::from("data"),
            FileFormat::Raw,
            None,
            None,
        );
        consumer.compression = Compression::Gzip;
        for _ in 0..=MAX_OBJECT_DECODE_FAILURES {
            let batch = consumer
                .receive_batch(10)
                .await
                .expect("non-gzip object must not error out the source");
            assert!(batch.messages.is_empty());
        }
    }

    #[tokio::test]
    async fn sink_writes_object_and_source_round_trips() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

        let publisher = test_publisher(store.clone());
        publisher
            .send_batch(vec![
                json_msg(serde_json::json!({"n": 1})),
                json_msg(serde_json::json!({"n": 2})),
            ])
            .await
            .unwrap();

        // One object exists under the prefix.
        let mut listed = store.list(Some(&ObjPath::from("data")));
        let first = listed.next().await.unwrap().unwrap();
        assert!(first.location.to_string().starts_with("data/"));

        let mut consumer = ObjectStoreConsumer::from_store(
            store,
            ObjPath::from("data"),
            FileFormat::Normal,
            None,
            None,
        );

        let batch = consumer.receive_batch(10).await.unwrap();
        assert_eq!(batch.messages.len(), 2);
        assert_eq!(batch.messages[0].payload.as_ref(), br#"{"n":1}"#);
        assert_eq!(batch.messages[1].payload.as_ref(), br#"{"n":2}"#);
        (batch.commit)(vec![MessageDisposition::Ack; 2])
            .await
            .unwrap();

        // Cursor advanced past the object; a further read is idle (empty).
        let drained = consumer.receive_batch(10).await.unwrap();
        assert!(drained.messages.is_empty());
    }

    #[tokio::test]
    async fn nacked_records_are_redelivered() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let publisher = test_publisher(store.clone());
        publisher
            .send_batch(vec![
                json_msg(serde_json::json!({"n": 1})),
                json_msg(serde_json::json!({"n": 2})),
            ])
            .await
            .unwrap();

        let mut consumer = ObjectStoreConsumer::from_store(
            store,
            ObjPath::from("data"),
            FileFormat::Normal,
            None,
            None,
        );

        // Ack the first, nack the second -> the second is requeued.
        let batch = consumer.receive_batch(10).await.unwrap();
        assert_eq!(batch.messages.len(), 2);
        (batch.commit)(vec![MessageDisposition::Ack, MessageDisposition::Nack])
            .await
            .unwrap();

        // The nacked record is redelivered (object not yet fully acked).
        let retry = consumer.receive_batch(10).await.unwrap();
        assert_eq!(retry.messages.len(), 1);
        assert_eq!(retry.messages[0].payload.as_ref(), br#"{"n":2}"#);
        (retry.commit)(vec![MessageDisposition::Ack]).await.unwrap();

        let drained = consumer.receive_batch(10).await.unwrap();
        assert!(drained.messages.is_empty());
    }
}
