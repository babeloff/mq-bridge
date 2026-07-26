//  mq-bridge
//  © Copyright 2025, by Marco Mengelkoch
//  Licensed under MIT License, see License file for more details
//  git clone https://github.com/marcomq/mq-bridge
use crate::canonical_message::{deserialize_u128, tracing_support::LazyMessageIds};
use crate::event_store::{EventStore, EventStoreConsumer, RetentionPolicy};
use crate::models::{Compression, FileConfig, FileConsumerMode, FileFormat};
#[cfg(feature = "encryption")]
use crate::support::crypto::Crypto;
use crate::traits::{
    ConsumerError, MessageConsumer, MessagePublisher, PublisherError, ReceivedBatch, SentBatch,
};
use crate::CanonicalMessage;
use anyhow::Context;
use async_trait::async_trait;
use once_cell::sync::Lazy;
use std::any::Any;
use std::collections::HashMap;
use std::io::Seek;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex as StdMutex, Weak};
use tokio::fs::{self, File, OpenOptions};
use tokio::io::{self, AsyncBufReadExt, AsyncSeekExt, BufReader};
use tokio::io::{AsyncWriteExt, BufWriter};
use tokio::sync::Mutex;
use tracing::{info, instrument, trace, warn};

/// A sink that writes messages to a file, one per line.
static FILE_LOCKS: Lazy<StdMutex<HashMap<String, Arc<Mutex<()>>>>> =
    Lazy::new(|| StdMutex::new(HashMap::new()));

fn get_file_lock(path: &str) -> Arc<Mutex<()>> {
    let mut locks = FILE_LOCKS.lock().unwrap();
    locks.retain(|_, v| Arc::strong_count(v) > 1);
    locks
        .entry(path.to_string())
        .or_insert_with(|| Arc::new(Mutex::new(())))
        .clone()
}

/// Appends a CSV-escaped field to `buf` without allocating for the common
/// (no special characters) case. Hot path for CSV row encoding.
fn csv_append_field(buf: &mut Vec<u8>, s: &str) {
    let bytes = s.as_bytes();
    // One byte pass instead of four `contains` scans.
    if !bytes
        .iter()
        .any(|b| matches!(b, b',' | b'"' | b'\n' | b'\r'))
    {
        buf.extend_from_slice(bytes);
        return;
    }
    buf.push(b'"');
    for &b in bytes {
        if b == b'"' {
            buf.push(b'"');
        }
        buf.push(b);
    }
    buf.push(b'"');
}

/// Appends `s` to `buf` with JSON string escaping (no surrounding quotes).
/// Fast path pushes the whole slice when it contains no characters needing
/// an escape. Hot path for decoding CSV rows into JSON objects.
fn json_append_escaped(buf: &mut String, s: &str) {
    if !s.bytes().any(|b| b < 0x20 || b == b'"' || b == b'\\') {
        buf.push_str(s);
        return;
    }
    for c in s.chars() {
        match c {
            '"' => buf.push_str("\\\""),
            '\\' => buf.push_str("\\\\"),
            '\n' => buf.push_str("\\n"),
            '\r' => buf.push_str("\\r"),
            '\t' => buf.push_str("\\t"),
            '\u{08}' => buf.push_str("\\b"),
            '\u{0C}' => buf.push_str("\\f"),
            c if (c as u32) < 0x20 => {
                use std::fmt::Write;
                let _ = write!(buf, "\\u{:04x}", c as u32);
            }
            c => buf.push(c),
        }
    }
}

fn csv_encode_row(fields: &[String]) -> Vec<u8> {
    let mut buf = Vec::new();
    for (i, f) in fields.iter().enumerate() {
        if i > 0 {
            buf.push(b',');
        }
        csv_append_field(&mut buf, f);
    }
    buf
}

/// Encodes `msg`'s JSON-object payload as a CSV row into `row_buf` (cleared first),
/// establishing the column order from its keys when `hdr` is still unset. Returns
/// `true` when this call established the header, so the caller can emit the header
/// line for a new file. Shared by the plain-append and member (compressed/encrypted)
/// write paths.
fn csv_encode_message(
    msg: &CanonicalMessage,
    hdr: &mut Option<Vec<String>>,
    row_buf: &mut Vec<u8>,
) -> Result<bool, serde_json::Error> {
    // Preferred path: borrow the keys and leave the values as unparsed
    // JSON slices, so a row costs one scan plus byte copies instead of
    // building (and re-serializing) a whole `Value` tree. Payloads with
    // escaped keys can't be borrowed, so those fall back to the tree.
    let raw_row =
        serde_json::from_slice::<HashMap<&str, &serde_json::value::RawValue>>(&msg.payload).ok();
    let parsed_row = match raw_row {
        Some(_) => None,
        None => match serde_json::from_slice::<serde_json::Value>(&msg.payload) {
            Ok(serde_json::Value::Object(obj)) => Some(obj),
            _ => None,
        },
    };
    // An object with no fields is rejected too: it carries no columns, so letting it
    // establish the header would fix an empty column set for the rest of the file.
    let no_columns = match (&raw_row, &parsed_row) {
        (Some(raw), _) => raw.is_empty(),
        (_, Some(obj)) => obj.is_empty(),
        _ => true,
    };
    if no_columns {
        return Err(serde_json::Error::io(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "CSV format requires a non-empty JSON object payload",
        )));
    }

    let mut header_established = false;
    if hdr.is_none() {
        // Sort keys so the column order is deterministic and
        // independent of serde_json's map type (BTreeMap vs the
        // IndexMap enabled by the `preserve_order` feature, which
        // `bson`/mongodb turns on under feature unification).
        let mut cols: Vec<String> = match (&raw_row, &parsed_row) {
            (Some(raw), _) => raw.keys().map(|k| (*k).to_string()).collect(),
            (_, Some(obj)) => obj.keys().cloned().collect(),
            _ => unreachable!(),
        };
        cols.sort();
        *hdr = Some(cols);
        header_established = true;
    }

    let cols = hdr.as_ref().expect("header set above");
    // Reused across the batch: rows are all the same shape, so
    // after the first one this never reallocates.
    row_buf.clear();
    row_buf.reserve(msg.payload.len());
    // Columns this payload actually supplied, for the drift check below.
    let mut matched = 0usize;
    for (i, c) in cols.iter().enumerate() {
        if i > 0 {
            row_buf.push(b',');
        }
        match (&raw_row, &parsed_row) {
            (Some(raw), _) => {
                if let Some(v) = raw.get(c.as_str()) {
                    csv_append_raw(row_buf, v);
                    matched += 1;
                }
            }
            (_, Some(obj)) => {
                if let Some(v) = obj.get(c) {
                    csv_append_value(row_buf, v);
                    matched += 1;
                }
            }
            _ => unreachable!(),
        }
    }
    if !header_established {
        // Keys the payload has beyond the ones the header covers are dropped silently, and
        // missing ones become empty fields; both mean the file's schema drifted. Counting
        // matches costs nothing extra, and the diagnostic is emitted once per process so a
        // whole drifted stream doesn't flood the log.
        let row_len = match (&raw_row, &parsed_row) {
            (Some(raw), _) => raw.len(),
            (_, Some(obj)) => obj.len(),
            _ => 0,
        };
        if matched < cols.len() || row_len > matched {
            static WARNED: AtomicBool = AtomicBool::new(false);
            if !WARNED.swap(true, Ordering::Relaxed) {
                warn!(
                    header_columns = cols.len(),
                    payload_keys = row_len,
                    matched_columns = matched,
                    "CSV payload keys differ from the established header: extra keys are dropped and missing ones written as empty fields. Logged once per process."
                );
            }
        }
    }
    Ok(header_established)
}

/// Appends one still-unparsed JSON value as a CSV field. Scalars are copied
/// straight from the source bytes; only escaped strings and nested
/// arrays/objects need any decoding.
fn csv_append_raw(buf: &mut Vec<u8>, raw: &serde_json::value::RawValue) {
    let text = raw.get();
    match text.as_bytes().first() {
        Some(b'"') => {
            let inner = &text[1..text.len() - 1];
            if !inner.as_bytes().contains(&b'\\') {
                csv_append_field(buf, inner);
            } else if let Ok(decoded) = serde_json::from_str::<String>(text) {
                csv_append_field(buf, &decoded);
            }
        }
        // Nested values are re-serialized compactly, matching the parsed path.
        Some(b'{') | Some(b'[') => {
            if let Ok(v) = serde_json::from_str::<serde_json::Value>(text) {
                csv_append_field(buf, &v.to_string());
            }
        }
        // Numbers, bools, null: never need quoting, but the scan is one pass anyway.
        _ => csv_append_field(buf, text),
    }
}

/// Appends one JSON value as a CSV field. Numbers, bools and nulls are written
/// straight into `buf` — they can never need CSV quoting, so this skips both the
/// escape scan and the `to_string` allocation that dominates numeric-heavy rows.
fn csv_append_value(buf: &mut Vec<u8>, v: &serde_json::Value) {
    use std::io::Write as _;
    match v {
        serde_json::Value::String(s) => csv_append_field(buf, s),
        serde_json::Value::Number(n) => {
            let _ = write!(buf, "{n}");
        }
        serde_json::Value::Bool(true) => buf.extend_from_slice(b"true"),
        serde_json::Value::Bool(false) => buf.extend_from_slice(b"false"),
        serde_json::Value::Null => buf.extend_from_slice(b"null"),
        // Nested arrays/objects keep their JSON spelling and do need escaping.
        other => csv_append_field(buf, &other.to_string()),
    }
}

/// Parses a single CSV line into fields. Supports quoted fields with escaped `""`.
fn parse_csv_row(line: &str) -> Vec<String> {
    let mut fields = Vec::new();
    let mut cur = String::new();
    let mut in_quotes = false;
    let mut chars = line.chars().peekable();
    while let Some(c) = chars.next() {
        if in_quotes {
            if c == '"' {
                if chars.peek() == Some(&'"') {
                    cur.push('"');
                    chars.next();
                } else {
                    in_quotes = false;
                }
            } else {
                cur.push(c);
            }
        } else if c == '"' && cur.is_empty() {
            in_quotes = true;
        } else if c == ',' {
            fields.push(std::mem::take(&mut cur));
        } else {
            cur.push(c);
        }
    }
    fields.push(cur);
    fields
}

pub(crate) fn parse_delimiter(delimiter: Option<&str>) -> anyhow::Result<Vec<u8>> {
    let bytes = match delimiter {
        Some(s) if s.starts_with("0x") => {
            let hex = s.trim_start_matches("0x");
            if hex.len() != 2 {
                return Err(anyhow::anyhow!(
                    "Hex delimiter must be 1 byte (2 hex chars)"
                ));
            }
            (0..hex.len())
                .step_by(2)
                .map(|i| u8::from_str_radix(&hex[i..i + 2], 16))
                .collect::<Result<Vec<u8>, _>>()
                .map_err(|e| anyhow::anyhow!("Invalid hex delimiter: {}", e))?
        }
        Some(s) => s.as_bytes().to_vec(),
        None => vec![b'\n'],
    };

    if bytes.is_empty() {
        return Err(anyhow::anyhow!("Delimiter cannot be empty"));
    }

    Ok(bytes)
}

async fn read_until_bytes<R: AsyncBufReadExt + Unpin>(
    reader: &mut R,
    delimiter: &[u8],
    buf: &mut Vec<u8>,
) -> std::io::Result<usize> {
    if delimiter.len() == 1 {
        return reader.read_until(delimiter[0], buf).await;
    }
    let last_byte = delimiter[delimiter.len() - 1];
    let mut total_read = 0;
    loop {
        let n = reader.read_until(last_byte, buf).await?;
        if n == 0 {
            return Ok(total_read);
        }
        total_read += n;
        if buf.len() >= delimiter.len() && &buf[buf.len() - delimiter.len()..] == delimiter {
            return Ok(total_read);
        }
    }
}

#[derive(Clone)]
pub struct FilePublisher {
    path: String,
    file_lock: Arc<Mutex<()>>,
    delimiter: Vec<u8>,
    format: FileFormat,
    #[cfg(any(feature = "compression", feature = "encryption"))]
    compression: Compression,
    #[cfg(feature = "encryption")]
    crypto: Option<Arc<Crypto>>,
    /// CSV column order, locked in by the first message written. Shared across
    /// clones of this publisher so all writers to the same file agree on it.
    csv_header: Arc<Mutex<Option<Vec<String>>>>,
}

/// Validates the `compression`/`encryption` settings shared by the file
/// publisher and consumer: both need their Cargo feature enabled.
fn validate_member_settings(config: &FileConfig) -> anyhow::Result<()> {
    // Only the feature-gated checks below read it, so it is unused with both features on.
    let _ = config;
    #[cfg(not(feature = "compression"))]
    if config.compression != Compression::None {
        return Err(anyhow::anyhow!(
            "file 'compression' requires the `compression` feature"
        ));
    }
    #[cfg(not(feature = "encryption"))]
    if config.encryption.is_some() {
        return Err(anyhow::anyhow!(
            "file 'encryption' requires the `encryption` feature"
        ));
    }
    Ok(())
}

impl FilePublisher {
    pub async fn new(config: &FileConfig) -> anyhow::Result<Self> {
        validate_member_settings(config)?;
        let path_str = &config.path;
        let path = Path::new(path_str);
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await.with_context(|| {
                format!("Failed to create parent directory for file: {:?}", parent)
            })?;
        }

        let _ = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .await
            .with_context(|| format!("Failed to open or create file for writing: {}", path_str))?;

        let file_lock = get_file_lock(path_str);
        let delimiter = parse_delimiter(config.delimiter.as_deref())?;
        let format = config.format.clone();

        info!(path = %path_str, format = ?format, "File sink opened for appending");
        Ok(Self {
            path: path_str.to_string(),
            file_lock,
            delimiter,
            format,
            #[cfg(any(feature = "compression", feature = "encryption"))]
            compression: config.compression,
            #[cfg(feature = "encryption")]
            crypto: config
                .encryption
                .as_ref()
                .map(Crypto::new)
                .transpose()?
                .map(Arc::new),
            csv_header: Arc::new(Mutex::new(None)),
        })
    }

    /// True when batches are written as self-contained members (compressed
    /// and/or encrypted) rather than plain appended lines.
    #[cfg(any(feature = "compression", feature = "encryption"))]
    fn is_member_mode(&self) -> bool {
        #[allow(unused_mut)]
        let mut member = self.compression != Compression::None;
        #[cfg(feature = "encryption")]
        {
            member |= self.crypto.is_some();
        }
        member
    }

    /// Writes one batch as a single self-contained member appended to the file.
    /// Compressed members (gzip/lz4) self-delimit, so the file stays a standard
    /// `.gz`/`.lz4` stream. An encrypted (sealed) member does not, so it is
    /// wrapped in a `[u64 be length][sealed bytes]` frame instead.
    #[cfg(any(feature = "compression", feature = "encryption"))]
    async fn send_batch_member(
        &self,
        messages: Vec<CanonicalMessage>,
    ) -> Result<SentBatch, PublisherError> {
        // Opened up front, before the CPU-bound encode/compress/seal below, and kept
        // open until the append. The order matters for throughput, not just tidiness:
        // this task is woken by the route's producer and lands in that thread's LIFO
        // slot, so any CPU it runs before its first suspension point holds the producer
        // there. `open` is a blocking-pool hop that always suspends, which is why the
        // plain path (which opens first) never stalls; building the member first cost
        // ~1.9ms of producer dead time per batch, made compressed writes ~35% slower
        // and pinned the pipeline to one core regardless of `concurrency`.
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
            .await
            .context("Failed to open file for writing batch member")?;
        // Members are concatenated, so the decoded stream is one continuous line
        // stream: the CSV header goes into the first member of a new file only.
        let is_csv = matches!(self.format, FileFormat::Csv);
        // CSV takes the file lock for the whole batch, because its header line has to land
        // in the *first* member of the file: the emptiness check, the header decision and
        // the append must stay atomic. Every other format appends self-contained records in
        // any order, so it leaves this `None` and the CPU-bound encode/compress/seal runs
        // outside the lock — the append below locks only if this is still `None`.
        let mut file_guard = if is_csv {
            Some(self.file_lock.lock().await)
        } else {
            None
        };
        let mut raw = Vec::new();
        let mut failed_messages = Vec::new();
        // Only a successful stat reporting zero bytes counts as empty. A failed stat is
        // treated as non-empty (matching the plain path's `pre_len == Some(0)`), so a
        // header can never be inserted into the middle of a file we could not measure.
        // The not-yet-created case is still empty: the append below creates the file.
        let file_is_empty = is_csv
            && match tokio::fs::metadata(&self.path).await {
                Ok(m) => m.len() == 0,
                Err(e) => e.kind() == std::io::ErrorKind::NotFound,
            };
        let mut csv_header_guard = if is_csv {
            Some(self.csv_header.lock().await)
        } else {
            None
        };
        let mut wrote_csv_header = false;
        let mut csv_row_buf: Vec<u8> = Vec::new();
        for mut msg in messages {
            msg.strip_source_metadata();
            // `Ok(None)` means the body is in the reused CSV row buffer.
            let encoded = match self.format {
                FileFormat::Csv => {
                    let hdr = csv_header_guard.as_mut().expect("csv header lock held");
                    match csv_encode_message(&msg, hdr, &mut csv_row_buf) {
                        Ok(header_established) => {
                            if header_established && file_is_empty {
                                raw.extend_from_slice(&csv_encode_row(
                                    hdr.as_ref().expect("header set above"),
                                ));
                                raw.extend_from_slice(&self.delimiter);
                                wrote_csv_header = true;
                            }
                            Ok(None)
                        }
                        Err(e) => Err(e),
                    }
                }
                ref fmt => encode_record(&msg, fmt).map(Some),
            };
            match encoded {
                Ok(Some(mut bytes)) => {
                    bytes.extend_from_slice(&self.delimiter);
                    raw.extend_from_slice(&bytes);
                }
                Ok(None) => {
                    raw.extend_from_slice(&csv_row_buf);
                    raw.extend_from_slice(&self.delimiter);
                }
                Err(e) => {
                    tracing::error!("Failed to serialize message for file sink member: {}", e);
                    failed_messages.push((msg, PublisherError::NonRetryable(anyhow::anyhow!(e))));
                }
            }
        }

        if !raw.is_empty() {
            // Every failure below leaves the member off disk (unwritten or rolled back), so a
            // CSV header established for this batch is cleared afterwards — otherwise the
            // retry would think the header was already written and emit a headerless file.
            let outcome: Result<(), PublisherError> = async {
            #[cfg(feature = "compression")]
            let raw = if self.compression != Compression::None {
                crate::support::compression::compress_member(self.compression, &raw)
                    .map_err(|e| PublisherError::NonRetryable(anyhow::anyhow!(e)))?
            } else {
                raw
            };
            #[allow(unused_mut)]
            let mut member = raw;
            #[cfg(feature = "encryption")]
            if let Some(crypto) = &self.crypto {
                let sealed = crypto
                    .seal(&member, b"")
                    .map_err(PublisherError::NonRetryable)?;
                // The consumer rejects any frame whose length prefix exceeds this cap, so a
                // batch sealing larger than it would be written but never read back. Fail
                // fast and tell the operator to shrink batch_size rather than emit a member
                // that corrupts the stream on read.
                if sealed.len() as u64 > MAX_ENCRYPTED_FRAME_BYTES {
                    return Err(PublisherError::NonRetryable(anyhow::anyhow!(
                        "encrypted batch frame is {} bytes, exceeding the {} byte cap the consumer can read; reduce batch_size",
                        sealed.len(),
                        MAX_ENCRYPTED_FRAME_BYTES
                    )));
                }
                let mut framed = Vec::with_capacity(8 + sealed.len());
                framed.extend_from_slice(&(sealed.len() as u64).to_be_bytes());
                framed.extend_from_slice(&sealed);
                member = framed;
            }

            // The member is fully built by now, so unless the batch already holds the
            // lock (CSV), it is taken here and covers only the append.
            if file_guard.is_none() {
                file_guard = Some(self.file_lock.lock().await);
            }
            // Length before the append: a failed write_all can leave a partial
            // member behind, which would corrupt the concatenated stream and get
            // compounded by the Retryable re-append. Truncate back to this
            // known-good member boundary on failure so a retry appends cleanly.
            let pre_len = file
                .metadata()
                .await
                .context("Failed to stat file before member write")?
                .len();
            // Append the whole member in one write so a concurrent reader never
            // observes a torn member (the consumer also guards against it).
            if let Err(e) = file.write_all(&member).await {
                if let Err(te) = file.set_len(pre_len).await {
                    tracing::error!(
                        "Failed to truncate file back to {} after member write error: {}",
                        pre_len,
                        te
                    );
                    // Rollback failed, so a partial member is still on disk. A Retryable
                    // re-append would concatenate onto that torn member and corrupt the
                    // whole stream, so fail permanently instead of letting a retry compound it.
                    return Err(PublisherError::NonRetryable(anyhow::Error::new(e).context(
                        "Failed to write member to file and could not truncate the partial write",
                    )));
                }
                return Err(PublisherError::Retryable(
                    anyhow::Error::new(e).context("Failed to write member to file"),
                ));
            }
            // Same rollback as the write above: a failed flush can leave a partial
            // member on disk, which a Retryable re-append would concatenate onto.
            if let Err(e) = file.flush().await {
                if let Err(te) = file.set_len(pre_len).await {
                    tracing::error!(
                        "Failed to truncate file back to {} after member flush error: {}",
                        pre_len,
                        te
                    );
                    return Err(PublisherError::NonRetryable(anyhow::Error::new(e).context(
                        "Failed to flush member to file and could not truncate the partial write",
                    )));
                }
                return Err(PublisherError::Retryable(
                    anyhow::Error::new(e).context("Failed to flush file"),
                ));
            }
            Ok(())
            }
            .await;
            if let Err(e) = outcome {
                if wrote_csv_header {
                    if let Some(hdr) = csv_header_guard.as_mut() {
                        **hdr = None;
                    }
                }
                return Err(e);
            }
        }

        if failed_messages.is_empty() {
            Ok(SentBatch::Ack)
        } else {
            Ok(SentBatch::Partial {
                responses: None,
                failed: failed_messages,
            })
        }
    }
}

/// Truncates the append-mode file back to `pre_len` (its length before the batch) so a Retryable
/// re-append starts from a clean record boundary instead of duplicating a partially written prefix.
/// When the file's CSV header was written in this batch it is rolled off with the prefix, so the
/// in-memory "header written" flag is cleared to make the retry rewrite it. `pre_len == None` (the
/// pre-batch stat failed) skips the rollback and preserves the old duplicate-on-retry behaviour.
async fn roll_back_partial_batch(
    writer: &BufWriter<File>,
    pre_len: Option<u64>,
    wrote_csv_header: bool,
    csv_header: Option<&mut tokio::sync::MutexGuard<'_, Option<Vec<String>>>>,
) {
    let Some(pl) = pre_len else { return };
    if let Err(te) = writer.get_ref().set_len(pl).await {
        tracing::error!(
            "Failed to truncate file back to {} after write error: {}",
            pl,
            te
        );
        return;
    }
    if wrote_csv_header {
        if let Some(hdr) = csv_header {
            **hdr = None;
        }
    }
}

#[async_trait]
impl MessagePublisher for FilePublisher {
    #[instrument(skip_all, fields(batch_size = messages.len()), level = "debug")]
    async fn send_batch(
        &self,
        messages: Vec<CanonicalMessage>,
    ) -> Result<SentBatch, PublisherError> {
        if messages.is_empty() {
            return Ok(SentBatch::Ack);
        }

        #[cfg(any(feature = "compression", feature = "encryption"))]
        if self.is_member_mode() {
            return self.send_batch_member(messages).await;
        }

        trace!(count = messages.len(), path = %self.path, message_ids = ?LazyMessageIds(&messages), "Writing batch to file");
        let _file_guard = self.file_lock.lock().await;

        // We open the file for every batch to ensure we are writing to the current file path.
        // This handles external file rotation/deletion (e.g. by the consumer in delete mode)
        // where the old file handle would point to a deleted inode.
        // While this has a performance cost, it ensures correctness.
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
            .await
            .context("Failed to open file for writing batch")?;

        // Length before the batch. The 1 MiB BufWriter auto-flushes full chunks mid-loop, so a
        // failure partway through a large batch can leave a partial prefix on disk; truncate back
        // to here on failure so the Retryable re-append starts clean instead of duplicating it.
        // `None` (stat failed) degrades to the old behaviour: no rollback.
        let pre_len = file.metadata().await.ok().map(|m| m.len());
        let file_is_empty = matches!(self.format, FileFormat::Csv) && pre_len == Some(0);
        // 1 MiB, not tokio's 8 KiB default: at ~100 B/record a small buffer turns a
        // bulk copy into ~13k write syscalls. Worth ~18% on file-to-file throughput.
        let mut writer = BufWriter::with_capacity(1 << 20, file);
        let mut failed_messages = Vec::new();
        // Tracks whether this batch wrote the CSV header. On rollback the header is truncated off
        // disk, so its in-memory "already written" flag must be cleared or the retry would skip it.
        let mut wrote_csv_header = false;
        let mut csv_header_guard = if matches!(self.format, FileFormat::Csv) {
            Some(self.csv_header.lock().await)
        } else {
            None
        };
        // Row buffer reused for every CSV record in this batch.
        let mut csv_row_buf: Vec<u8> = Vec::new();

        // Iterate over messages, consuming them
        for mut msg in messages {
            // Strip per-hop source/provenance keys in place — they are not
            // persisted. Done on the owned message (no payload clone); a message
            // pushed to `failed_messages` keeps its remaining fields, and the
            // dropped `mqb.src.*` keys are irrelevant to a retry on the next hop.
            msg.strip_source_metadata();
            // Carried out of the match so the `hdr` borrow ends before the write,
            // which needs the guard again for the rollback path.
            let mut csv_header_line: Option<Vec<u8>> = None;
            let serialized_msg = match self.format {
                FileFormat::Csv => {
                    let hdr = csv_header_guard.as_mut().expect("csv header lock held");
                    match csv_encode_message(&msg, hdr, &mut csv_row_buf) {
                        Ok(header_established) => {
                            if header_established && file_is_empty {
                                let mut line =
                                    csv_encode_row(hdr.as_ref().expect("header set above"));
                                line.extend_from_slice(&self.delimiter);
                                csv_header_line = Some(line);
                            }
                            Ok(None)
                        }
                        Err(e) => Err(e),
                    }
                }
                ref fmt => encode_record(&msg, fmt).map(Some),
            };
            if let Some(line) = csv_header_line {
                // Set before the write: the header is established in memory either way, so a
                // rollback has to clear it even when the write itself failed.
                wrote_csv_header = true;
                if let Err(e) = writer.write_all(&line).await {
                    roll_back_partial_batch(
                        &writer,
                        pre_len,
                        wrote_csv_header,
                        csv_header_guard.as_mut(),
                    )
                    .await;
                    return Err(PublisherError::NonRetryable(anyhow::anyhow!(e)));
                }
            }
            let serialized_msg = match serialized_msg {
                Ok(s) => s,
                Err(e) => {
                    tracing::error!("Failed to serialize message for file sink: {}", e);
                    failed_messages.push((msg, PublisherError::NonRetryable(anyhow::anyhow!(e))));
                    continue;
                }
            };

            // Write body + delimiter as one contiguous buffer so a concurrent
            // tailing reader never observes the record without its delimiter
            // (shrinks the torn-write window; the reader also guards against it).
            // `None` means the body is already in the reused CSV row buffer.
            let owned_msg;
            let record: &[u8] = match serialized_msg {
                Some(mut s) => {
                    s.extend_from_slice(&self.delimiter);
                    owned_msg = s;
                    &owned_msg
                }
                None => {
                    csv_row_buf.extend_from_slice(&self.delimiter);
                    &csv_row_buf
                }
            };
            if let Err(e) = writer.write_all(record).await {
                tracing::error!("Failed to write message to file: {}", e);
                // A buffered write failure leaves the BufWriter in an undefined state and the
                // remaining messages in this batch are unwritten. Abort so the whole batch is
                // retried rather than reusing the writer, flushing partial data, or acking
                // messages that never reached the file.
                roll_back_partial_batch(
                    &writer,
                    pre_len,
                    wrote_csv_header,
                    csv_header_guard.as_mut(),
                )
                .await;
                return Err(PublisherError::Retryable(
                    anyhow::Error::new(e).context("Failed to write message to file"),
                ));
            }
        }

        if let Err(e) = writer.flush().await {
            // A partial flush can leave part of the batch on disk; roll back so the
            // Retryable re-append doesn't duplicate the flushed prefix.
            roll_back_partial_batch(
                &writer,
                pre_len,
                wrote_csv_header,
                csv_header_guard.as_mut(),
            )
            .await;
            return Err(PublisherError::Retryable(
                anyhow::Error::new(e).context("Failed to flush file writer"),
            ));
        }
        if failed_messages.is_empty() {
            Ok(SentBatch::Ack)
        } else {
            Ok(SentBatch::Partial {
                responses: None,
                failed: failed_messages,
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

static FILE_EVENT_STORES: Lazy<Mutex<HashMap<String, Weak<EventStore>>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));

struct FileFeedState {
    /// For Consume mode: number of lines currently buffered in EventStore.
    lines_in_memory: usize,
}

/// Creates an EventStore backed by a file.
/// The EventStore acts as an in-memory buffer for the file content, allowing unified handling of Consume and Subscribe modes.
async fn create_file_event_store(
    path: &str,
    delimiter: Vec<u8>,
    format: FileFormat,
) -> anyhow::Result<Arc<EventStore>> {
    let path = path.to_string();
    // Shared state to coordinate the reader and the drop (delete) logic.
    let feed_state = Arc::new(Mutex::new(FileFeedState { lines_in_memory: 0 }));

    // Lock to serialize file modification operations
    let file_op_lock = get_file_lock(&path);

    let feed_state_clone = feed_state.clone();
    let path_clone = path.clone();
    let file_op_lock_clone = file_op_lock.clone();
    let delimiter_clone = delimiter.clone();

    let retention = RetentionPolicy {
        gc_interval: std::time::Duration::ZERO,
        ..Default::default()
    };
    // Use immediate GC for file stores to ensure files are truncated promptly on ack.

    // 1. Create EventStore with on_drop callback
    let store = Arc::new(
        EventStore::new(retention).with_drop_callback(move |events| {
            // In EventStore mode (Subscribe + Delete), we always delete.
            let count = events.len();
            if count == 0 {
                return;
            }
            let state = feed_state_clone.clone();
            let path = path_clone.clone();
            let file_op_lock = file_op_lock_clone.clone();
            let delimiter = delimiter_clone.clone();

            tokio::spawn(async move {
                // Serialize file operations to prevent race conditions between multiple GCs
                let _guard = file_op_lock.lock().await;

                {
                    let mut s = state.lock().await;
                    s.lines_in_memory = s.lines_in_memory.saturating_sub(count);
                }

                if let Err(e) = remove_lines_from_file(&path, count, &delimiter).await {
                    tracing::error!("Failed to remove lines from file {}: {}", path, e);
                    // Note: In this simplified model, if deletion fails, lines_in_memory
                    // might become out of sync, leading to reprocessing on restart.
                } else {
                    trace!("Removed {} lines from {}", count, path);
                }
            });
        }),
    );

    // 2. Spawn background reader task
    let store_weak = Arc::downgrade(&store);
    let path_clone = path.clone();
    let feed_state_clone = feed_state.clone();
    let file_op_lock_clone = file_op_lock.clone();
    let format_clone = format;

    tokio::spawn(async move {
        let mut current_sleep = std::time::Duration::from_millis(1);
        const MAX_SLEEP: std::time::Duration = std::time::Duration::from_millis(100);
        // CSV is not supported in this backend (Subscribe + delete); see FileConsumer::new.
        let mut csv_header: Option<Vec<String>> = None;

        loop {
            // Check if the store is still alive
            let store_clone = match store_weak.upgrade() {
                Some(s) => s,
                None => break, // Exit if EventStore is dropped
            };

            // Acquire file op lock first to coordinate with GC
            let file_guard = Some(file_op_lock_clone.lock().await);

            let mut state = feed_state_clone.lock().await;

            // Open file
            let file_res = OpenOptions::new().read(true).open(&path_clone).await;
            let mut file = match file_res {
                Ok(f) => f,
                Err(e) => {
                    tracing::error!("Failed to open file {}: {}", path_clone, e);
                    drop(state);
                    drop(file_guard);
                    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                    continue;
                }
            };

            // Position the reader
            // In consume mode, we skip lines that are already buffered in memory
            // because they are still in the file (until dropped).
            let mut reader = BufReader::new(file);
            let mut lines_skipped = 0;
            let mut error = false;
            let lines_to_skip = state.lines_in_memory;
            while lines_skipped < lines_to_skip {
                let mut buf = Vec::new();
                match read_until_bytes(&mut reader, &delimiter, &mut buf).await {
                    Ok(0) => break, // EOF
                    Ok(_) => lines_skipped += 1,
                    Err(e) => {
                        tracing::error!("Error skipping lines in {}: {}", path_clone, e);
                        error = true;
                        break;
                    }
                }
            }
            if error {
                drop(state);
                drop(file_guard);
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                continue;
            }
            file = reader.into_inner();

            // Release file op lock to allow publisher to write while we read
            drop(file_guard);

            // Read new lines
            let mut reader = BufReader::new(file);
            let mut lines_read = 0;
            let mut batch = Vec::with_capacity(128);

            loop {
                let mut buffer = Vec::new();
                match read_until_bytes(&mut reader, &delimiter, &mut buffer).await {
                    Ok(0) => break,
                    Ok(_) => {
                        if buffer.ends_with(&delimiter) {
                            buffer.truncate(buffer.len() - delimiter.len());
                        }
                        if delimiter.len() == 1 && delimiter[0] == b'\n' && buffer.ends_with(b"\r")
                        {
                            buffer.pop();
                        }
                        if let Some(msg) = parse_message(&buffer, &format_clone, &mut csv_header) {
                            batch.push(msg);
                        }
                        lines_read += 1;

                        state.lines_in_memory += 1;

                        if batch.len() >= 128 {
                            store_clone.append_batch(std::mem::take(&mut batch)).await;
                            batch.reserve(128);
                        }
                    }
                    Err(e) => {
                        tracing::error!("Error reading from {}: {}", path_clone, e);
                        break;
                    }
                }
            }

            if !batch.is_empty() {
                store_clone.append_batch(batch).await;
            }

            drop(state); // Release lock before sleeping

            // If we didn't read anything, sleep a bit (polling)
            if lines_read == 0 {
                tokio::time::sleep(current_sleep).await;
                current_sleep = std::cmp::min(current_sleep * 2, MAX_SLEEP);
            } else {
                current_sleep = std::time::Duration::from_millis(1);
            }
        }
    });

    Ok(store)
}

async fn remove_lines_from_file(path: &str, count: usize, delimiter: &[u8]) -> anyhow::Result<()> {
    let unique_id = fast_uuid_v7::gen_id_str();
    let temp_path = format!("{}.{}.tmp", path, unique_id);

    let file = File::open(path).await?;
    let mut reader = BufReader::new(file);
    let temp_file = File::create(&temp_path).await?;
    let mut writer = BufWriter::new(temp_file);

    let mut lines_skipped = 0;
    while lines_skipped < count {
        let mut buf = Vec::new();
        if read_until_bytes(&mut reader, delimiter, &mut buf).await? == 0 {
            break;
        }
        lines_skipped += 1;
    }

    if let Err(e) = io::copy(&mut reader, &mut writer).await {
        let _ = fs::remove_file(&temp_path).await;
        return Err(e.into());
    }

    writer.flush().await?;
    let temp_file = writer.into_inner();
    temp_file.sync_all().await?;
    drop(temp_file); // Close writer handle
    drop(reader); // Close reader handle

    fs::rename(&temp_path, path).await?;

    // Sync parent directory to ensure rename is durable
    if let Some(parent) = Path::new(path).parent() {
        if let Ok(parent_dir) = File::open(parent).await {
            let _ = parent_dir.sync_all().await;
        }
    }

    Ok(())
}
struct FileTailConsumer {
    msg_rx: async_channel::Receiver<Vec<CanonicalMessage>>,
    buffer: Vec<CanonicalMessage>,
    offset_file: Option<Arc<Mutex<tokio::fs::File>>>,
    ready: Arc<AtomicBool>,
    /// Set when a greedy fill consumed the watcher's end-of-file marker after
    /// data; the next `receive_batch` surfaces it as an empty batch so a route
    /// with `exit_on_empty` can drain-then-exit.
    pending_eof: bool,
}

fn read_until_bytes_sync<R: std::io::BufRead>(
    reader: &mut R,
    delimiter: &[u8],
    buf: &mut Vec<u8>,
) -> std::io::Result<usize> {
    if delimiter.len() == 1 {
        return reader.read_until(delimiter[0], buf);
    }
    let last_byte = delimiter[delimiter.len() - 1];
    let mut total_read = 0;
    loop {
        let n = reader.read_until(last_byte, buf)?;
        if n == 0 {
            return Ok(total_read);
        }
        total_read += n;
        if buf.len() >= delimiter.len() && &buf[buf.len() - delimiter.len()..] == delimiter {
            return Ok(total_read);
        }
    }
}

fn run_file_tail_task_sync(
    path: String,
    msg_tx: async_channel::Sender<Vec<CanonicalMessage>>,
    initial_offset: u64,
    group_id: Option<String>,
    delimiter: Vec<u8>,
    format: FileFormat,
    ready: Arc<AtomicBool>,
) {
    let mut last_position: u64 = initial_offset;
    let mut reader: Option<std::io::BufReader<std::fs::File>> = None;
    let mut current_sleep = std::time::Duration::from_millis(1);
    const MAX_SLEEP: std::time::Duration = std::time::Duration::from_millis(50);
    let mut initialized = false;
    // Tracks whether we've already emitted the empty end-of-file marker for the
    // current drained state, so we signal it once per EOF transition rather than
    // on every idle poll.
    let mut signaled_eof = false;
    const BATCH_SIZE: usize = 1024;
    let mut buf = Vec::with_capacity(1024);
    let mut csv_header: Option<Vec<String>> = None;

    loop {
        if reader.is_none() {
            let mut file = match std::fs::OpenOptions::new().read(true).open(&path) {
                Ok(f) => f,
                Err(e) => {
                    tracing::error!("Failed to open {}: {}", path, e);
                    std::thread::sleep(std::time::Duration::from_secs(1));
                    continue;
                }
            };

            if let Ok(metadata) = file.metadata() {
                if metadata.len() < last_position {
                    tracing::warn!("File {} was truncated. Resetting position to 0.", path);
                    last_position = 0;
                }
            }

            if let Err(e) = file.seek(std::io::SeekFrom::Start(last_position)) {
                tracing::error!("Failed to seek in {}: {}", path, e);
                last_position = 0; // Reset on seek failure
                if let Err(e) = file.seek(std::io::SeekFrom::Start(0)) {
                    tracing::error!("Failed to reset seek to 0 in {}: {}", path, e);
                    std::thread::sleep(std::time::Duration::from_secs(1));
                    continue;
                }
            }

            reader = Some(std::io::BufReader::with_capacity(128 * BATCH_SIZE, file));
            if !initialized {
                ready.store(true, Ordering::SeqCst);
                initialized = true;
            }
        }

        let mut batch = Vec::with_capacity(BATCH_SIZE);
        let mut lines_read_in_batch = 0;

        if let Some(r) = reader.as_mut() {
            for _ in 0..BATCH_SIZE {
                buf.clear();
                match read_until_bytes_sync(r, &delimiter, &mut buf) {
                    Ok(0) => break, // EOF
                    Ok(n) => {
                        if !buf.ends_with(&delimiter) {
                            // Torn/partial line: the writer's content reached disk
                            // ahead of its trailing delimiter. Don't advance the
                            // position or emit a message; drop the reader so the
                            // next iteration reopens and re-seeks to last_position,
                            // re-reading the line whole once the writer finishes it.
                            reader = None;
                            break;
                        }
                        last_position += n as u64;
                        buf.truncate(buf.len() - delimiter.len());
                        if delimiter.len() == 1 && delimiter[0] == b'\n' && buf.ends_with(b"\r") {
                            buf.pop();
                        }
                        if let Some(mut msg) = parse_message(&buf, &format, &mut csv_header) {
                            if group_id.is_some() {
                                msg.metadata
                                    .insert("file_offset".to_string(), last_position.to_string());
                            }
                            batch.push(msg);
                        }
                        lines_read_in_batch += 1;
                    }
                    Err(e) => {
                        tracing::error!("Error reading {}: {}", path, e);
                        reader = None; // Force reopen on next loop
                        break;
                    }
                }
            }
        }

        if !batch.is_empty() {
            if msg_tx.send_blocking(batch).is_err() {
                break; // Consumer dropped, exit thread
            }
            current_sleep = std::time::Duration::from_millis(1);
            signaled_eof = false; // data flowed; re-arm the EOF marker
        }

        if lines_read_in_batch == 0 {
            // EOF reached. Emit an empty batch once so a drained route can pause
            // or, with exit_on_empty, terminate. Re-armed when new data arrives.
            if !signaled_eof {
                if msg_tx.send_blocking(Vec::new()).is_err() {
                    break; // Consumer dropped, exit thread
                }
                signaled_eof = true;
            }
            std::thread::sleep(current_sleep);
            current_sleep = std::cmp::min(current_sleep * 2, MAX_SLEEP);
            // Invalidate reader to check for file changes (like rotation) on next poll
            reader = None;
        }
    }
}

struct FileQueueConsumer {
    msg_rx: async_channel::Receiver<Vec<CanonicalMessage>>,
    lines_in_memory: Arc<AtomicUsize>,
    path: String,
    file_lock: Arc<Mutex<()>>,
    buffer: Arc<Mutex<Vec<CanonicalMessage>>>,
    delimiter: Vec<u8>,
    ready: Arc<AtomicBool>,
    /// See [`FileTailConsumer::pending_eof`].
    pending_eof: bool,
}

#[allow(clippy::too_many_arguments)]
fn run_file_queue_task(
    path: String,
    msg_tx: async_channel::Sender<Vec<CanonicalMessage>>,
    lines_in_memory: Arc<AtomicUsize>,
    file_lock: Arc<Mutex<()>>,
    runtime_handle: tokio::runtime::Handle,
    delimiter: Vec<u8>,
    format: FileFormat,
    ready: Arc<AtomicBool>,
) {
    let mut current_sleep = std::time::Duration::from_millis(1);
    const MAX_SLEEP: std::time::Duration = std::time::Duration::from_millis(100);
    let mut initialized = false;
    // Emit the empty end-of-file marker once per drained state; see the tail task.
    let mut signaled_eof = false;
    let mut buf = Vec::new();
    let mut csv_header: Option<Vec<String>> = None;

    loop {
        buf.clear();
        let mut batch = Vec::with_capacity(128);
        let mut lines_read = 0;

        {
            let _guard = runtime_handle.block_on(file_lock.lock());
            let skip_count = lines_in_memory.load(Ordering::SeqCst);

            let file = match std::fs::OpenOptions::new().read(true).open(&path) {
                Ok(f) => f,
                Err(e) => {
                    tracing::error!("Failed to open {}: {}", path, e);
                    drop(_guard);
                    std::thread::sleep(std::time::Duration::from_secs(1));
                    continue;
                }
            };

            let mut reader = std::io::BufReader::new(file);
            let mut skipped = 0;
            let mut error = false;

            while skipped < skip_count {
                buf.clear();
                match read_until_bytes_sync(&mut reader, &delimiter, &mut buf) {
                    Ok(0) => break,
                    Ok(_) => skipped += 1,
                    Err(e) => {
                        tracing::error!("Error skipping lines in {}: {}", path, e);
                        error = true;
                        break;
                    }
                }
            }

            if !error {
                for _ in 0..128 {
                    buf.clear();
                    match read_until_bytes_sync(&mut reader, &delimiter, &mut buf) {
                        Ok(0) => break,
                        Ok(_) => {
                            if buf.ends_with(&delimiter) {
                                buf.truncate(buf.len() - delimiter.len());
                            }
                            if delimiter.len() == 1 && delimiter[0] == b'\n' && buf.ends_with(b"\r")
                            {
                                buf.pop();
                            }
                            match parse_message(&buf, &format, &mut csv_header) {
                                Some(msg) => {
                                    batch.push(msg);
                                    lines_read += 1;
                                }
                                None => {
                                    // CSV header line: remove it immediately so it never
                                    // occupies a slot in the ack/delete line accounting.
                                    if let Err(e) = runtime_handle
                                        .block_on(remove_lines_from_file(&path, 1, &delimiter))
                                    {
                                        tracing::error!(
                                            "Failed to remove CSV header line from {}: {}",
                                            path,
                                            e
                                        );
                                    }
                                }
                            }
                        }
                        Err(_) => break,
                    }
                }

                if !initialized {
                    ready.store(true, Ordering::SeqCst);
                    initialized = true;
                }
            }
        }

        if lines_read > 0 {
            lines_in_memory.fetch_add(lines_read, Ordering::SeqCst);
            if msg_tx.send_blocking(batch).is_err() {
                break;
            }
            current_sleep = std::time::Duration::from_millis(1);
            signaled_eof = false; // data flowed; re-arm the EOF marker
        } else {
            // EOF: emit an empty batch once so a drained route can pause or,
            // with exit_on_empty, terminate. Re-armed when new data arrives.
            if !signaled_eof {
                if msg_tx.send_blocking(Vec::new()).is_err() {
                    break;
                }
                signaled_eof = true;
            }
            std::thread::sleep(current_sleep);
            current_sleep = std::cmp::min(current_sleep * 2, MAX_SLEEP);
        }
    }
}

/// Reader for member-based files (compressed and/or encrypted). Such a stream
/// can't be seeked to a line boundary, so on each growth of the file it
/// re-decodes from the start and skips the records already emitted. For the
/// common write-once-then-read ETL case the file is decoded exactly once;
/// live-tailing a growing file costs a re-scan per growth (acceptable for v1).
///
/// Operational note: because each growth re-decodes from the start, tailing a
/// member stream that grows unboundedly is O(n²) in total CPU over its lifetime.
/// Size or rotate compressed/encrypted inputs (finite members, then a new file)
/// rather than appending to a single member stream indefinitely.
///
/// `make_reader` builds the decoding [`Read`](std::io::Read) chain
/// (decrypt frames and/or decompress members) over a freshly opened file.
#[cfg(any(feature = "compression", feature = "encryption"))]
fn run_file_member_consume_task_sync<F>(
    path: String,
    msg_tx: async_channel::Sender<Vec<CanonicalMessage>>,
    delimiter: Vec<u8>,
    format: FileFormat,
    ready: Arc<AtomicBool>,
    make_reader: F,
) where
    F: Fn(std::fs::File) -> Box<dyn std::io::Read>,
{
    const BATCH_SIZE: usize = 1024;
    const MAX_SLEEP: std::time::Duration = std::time::Duration::from_millis(100);
    // Consecutive decode failures at an unchanged file length before we give up and
    // close the stream (surfacing EndOfStream) rather than retrying forever or
    // silently emitting drain markers for records we can no longer reach.
    const MAX_DECODE_FAILURES: u32 = 5;
    let mut records_emitted: usize = 0;
    let mut last_len: u64 = u64::MAX; // force the first read
    let mut initialized = false;
    let mut signaled_eof = false;
    let mut current_sleep = std::time::Duration::from_millis(1);
    let mut buf = Vec::new();
    let mut decode_failures: u32 = 0;
    let mut failure_len: u64 = u64::MAX;

    loop {
        let cur_len = std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);

        // No growth since the last full pass: emit the drain marker once, then poll.
        if initialized && cur_len == last_len {
            if !signaled_eof {
                if msg_tx.send_blocking(Vec::new()).is_err() {
                    break;
                }
                signaled_eof = true;
            }
            std::thread::sleep(current_sleep);
            current_sleep = std::cmp::min(current_sleep * 2, MAX_SLEEP);
            continue;
        }

        let file = match std::fs::File::open(&path) {
            Ok(f) => f,
            Err(e) => {
                tracing::error!("Failed to open {}: {}", path, e);
                std::thread::sleep(std::time::Duration::from_secs(1));
                continue;
            }
        };
        let mut reader = std::io::BufReader::new(make_reader(file));

        // Skip records emitted on a previous pass (file re-read from the start).
        let mut csv_header: Option<Vec<String>> = None;
        let mut skipped = 0;
        let mut decode_error = false;
        while skipped < records_emitted {
            buf.clear();
            match read_until_bytes_sync(&mut reader, &delimiter, &mut buf) {
                Ok(0) => break,
                Ok(_) => skipped += 1,
                Err(e) => {
                    tracing::error!("Error decoding {}: {}", path, e);
                    decode_error = true;
                    break;
                }
            }
        }
        if decode_error {
            // Skip failed: do NOT advance records_emitted/last_len. Bound the retries
            // so permanent corruption surfaces instead of spinning forever.
            if cur_len == failure_len {
                decode_failures += 1;
            } else {
                failure_len = cur_len;
                decode_failures = 1;
            }
            if decode_failures >= MAX_DECODE_FAILURES {
                tracing::error!(
                    "Giving up decoding {} after {} failed attempts at {} bytes; closing stream",
                    path,
                    decode_failures,
                    cur_len
                );
                break;
            }
            std::thread::sleep(std::time::Duration::from_secs(1));
            continue;
        }

        let mut new_count = 0;
        let mut read_error = false;
        let mut batch = Vec::with_capacity(256);
        loop {
            buf.clear();
            match read_until_bytes_sync(&mut reader, &delimiter, &mut buf) {
                Ok(0) => break,
                Ok(_) => {
                    if !buf.ends_with(&delimiter) {
                        // Torn final member (writer mid-append): don't emit or count
                        // it; it completes and grows the file on a later poll.
                        break;
                    }
                    buf.truncate(buf.len() - delimiter.len());
                    if delimiter.len() == 1 && delimiter[0] == b'\n' && buf.ends_with(b"\r") {
                        buf.pop();
                    }
                    if let Some(msg) = parse_message(&buf, &format, &mut csv_header) {
                        batch.push(msg);
                    }
                    new_count += 1;
                    if batch.len() >= BATCH_SIZE {
                        if msg_tx.send_blocking(std::mem::take(&mut batch)).is_err() {
                            return;
                        }
                        batch = Vec::with_capacity(256);
                    }
                }
                Err(e) => {
                    // Truncated/torn member: stop; retry once the writer finishes it.
                    tracing::debug!("Partial member decode of {}: {}", path, e);
                    read_error = true;
                    break;
                }
            }
        }
        if !batch.is_empty() && msg_tx.send_blocking(batch).is_err() {
            return;
        }

        // Records actually emitted this pass must be skipped next time, even if the
        // pass then hit a decode error on the tail.
        records_emitted += new_count;
        if !initialized {
            ready.store(true, Ordering::SeqCst);
            initialized = true;
        }

        if read_error {
            // The pass ended on a decode error, not a clean EOF. Do NOT advance
            // last_len: a torn member the writer is still completing is retried on the
            // next pass (once the file grows), while permanent corruption at a fixed
            // length is bounded here so it surfaces as EndOfStream instead of silently
            // emitting drain markers for records we can no longer reach.
            if cur_len == failure_len {
                decode_failures += 1;
            } else {
                failure_len = cur_len;
                decode_failures = 1;
            }
            if decode_failures >= MAX_DECODE_FAILURES {
                tracing::error!(
                    "Giving up decoding {} after {} failed attempts at {} bytes; closing stream",
                    path,
                    decode_failures,
                    cur_len
                );
                break;
            }
            std::thread::sleep(std::time::Duration::from_secs(1));
            continue;
        }

        // Clean pass: the tail decoded to EOF, so reset the failure tracker and advance.
        decode_failures = 0;
        failure_len = u64::MAX;
        last_len = cur_len;
        if new_count > 0 {
            signaled_eof = false;
            current_sleep = std::time::Duration::from_millis(1);
        }
    }
}

/// Upper bound for one encrypted frame (one sealed batch). A larger length
/// prefix means corruption; refusing it avoids a huge bogus allocation.
#[cfg(feature = "encryption")]
const MAX_ENCRYPTED_FRAME_BYTES: u64 = 1 << 30;

/// Decodes a file of `[u64 be length][sealed member]` frames: each frame is
/// decrypted and (if configured) decompressed, and the resulting plaintext is
/// served as one continuous stream. A torn trailing frame surfaces as an
/// `UnexpectedEof` error, which the member consume task treats like a torn
/// compressed member (retried once the writer completes it).
#[cfg(feature = "encryption")]
struct EncryptedFramesReader<R: std::io::Read> {
    inner: R,
    crypto: Arc<Crypto>,
    #[cfg_attr(not(feature = "compression"), allow(dead_code))]
    compression: Compression,
    current: std::io::Cursor<Vec<u8>>,
}

#[cfg(feature = "encryption")]
impl<R: std::io::Read> EncryptedFramesReader<R> {
    fn new(inner: R, crypto: Arc<Crypto>, compression: Compression) -> Self {
        Self {
            inner,
            crypto,
            compression,
            current: std::io::Cursor::new(Vec::new()),
        }
    }

    /// Reads and decodes the next frame. `Ok(false)` = clean end of file.
    fn refill(&mut self) -> std::io::Result<bool> {
        // Read the 8-byte length prefix, distinguishing clean EOF (no bytes at
        // all) from a torn prefix (some bytes, then EOF).
        let mut len_buf = [0u8; 8];
        let mut filled = 0;
        while filled < len_buf.len() {
            let n = self.inner.read(&mut len_buf[filled..])?;
            if n == 0 {
                if filled == 0 {
                    return Ok(false);
                }
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "torn encrypted frame length prefix",
                ));
            }
            filled += n;
        }
        let len = u64::from_be_bytes(len_buf);
        if len > MAX_ENCRYPTED_FRAME_BYTES {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("encrypted frame length {len} exceeds the {MAX_ENCRYPTED_FRAME_BYTES} byte cap (corrupt file?)"),
            ));
        }
        let mut sealed = vec![0u8; len as usize];
        self.inner.read_exact(&mut sealed)?;
        let member = self
            .crypto
            .open(&sealed, b"")
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?;
        #[cfg(feature = "compression")]
        let member = if self.compression != Compression::None {
            crate::support::compression::decompress_all(self.compression, &member, None)?
        } else {
            member
        };
        self.current = std::io::Cursor::new(member);
        Ok(true)
    }
}

#[cfg(feature = "encryption")]
impl<R: std::io::Read> std::io::Read for EncryptedFramesReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        loop {
            let n = self.current.read(buf)?;
            if n > 0 || buf.is_empty() {
                return Ok(n);
            }
            if !self.refill()? {
                return Ok(0);
            }
        }
    }
}

enum ConsumerBackend {
    EventStore(EventStoreConsumer),
    Tail(FileTailConsumer),
    Queue(FileQueueConsumer),
}

/// A consumer that reads messages from a file and removes them upon commit.
pub struct FileConsumer {
    backend: ConsumerBackend,
}

impl FileConsumer {
    pub async fn new(config: &FileConfig) -> anyhow::Result<Self> {
        let delimiter = parse_delimiter(config.delimiter.as_deref())?;
        let format = config.format.clone();
        if matches!(format, FileFormat::Csv)
            && matches!(
                &config.mode,
                Some(FileConsumerMode::Subscribe { delete: true })
            )
        {
            return Err(anyhow::anyhow!(
                "FileFormat::Csv is not supported with Subscribe {{ delete: true }} mode"
            ));
        }
        validate_member_settings(config)?;
        if config.compression != Compression::None || config.encryption.is_some() {
            if !matches!(
                &config.mode,
                None | Some(FileConsumerMode::Consume { delete: false })
            ) {
                return Err(anyhow::anyhow!(
                    "file 'compression'/'encryption' is only supported with the default `consume` mode (no delete, no group_id)"
                ));
            }
            // Member-based files (compressed and/or encrypted) have no seekable
            // line offsets, so they use a dedicated reader that decodes from the
            // start of the file.
            #[cfg(any(feature = "compression", feature = "encryption"))]
            return Self::new_member_consumer(config, delimiter, format).await;
        }
        match &config.mode {
            None | Some(FileConsumerMode::Consume { delete: false }) => {
                Self::new_tail(&config.path, false, None, delimiter.clone(), format).await
            }
            Some(FileConsumerMode::Subscribe { delete: false }) => {
                Self::new_tail(&config.path, true, None, delimiter.clone(), format).await
            }
            Some(FileConsumerMode::GroupSubscribe {
                group_id,
                read_from_tail,
            }) => {
                let start_at_end = *read_from_tail;
                Self::new_tail(
                    &config.path,
                    start_at_end,
                    Some(group_id.clone()),
                    delimiter.clone(),
                    format,
                )
                .await
            }
            Some(FileConsumerMode::Consume { delete: true }) => {
                let (msg_tx, msg_rx) = async_channel::bounded(100);
                let file_lock = get_file_lock(&config.path);
                let lines_in_memory = Arc::new(AtomicUsize::new(0));
                let ready = Arc::new(AtomicBool::new(false));
                let ready_clone = ready.clone();
                let lines_clone = lines_in_memory.clone();
                let lock_clone = file_lock.clone();
                let runtime = tokio::runtime::Handle::current();
                let path_clone = config.path.clone();

                let delimiter_clone = delimiter.clone();
                let format_clone = format.clone();
                std::thread::spawn(move || {
                    run_file_queue_task(
                        path_clone,
                        msg_tx,
                        lines_clone,
                        lock_clone,
                        runtime,
                        delimiter_clone,
                        format_clone,
                        ready_clone,
                    );
                });

                info!(path = %config.path, mode = "queue (delete, optimized)", "File consumer connected");
                Ok(Self {
                    backend: ConsumerBackend::Queue(FileQueueConsumer {
                        msg_rx,
                        lines_in_memory,
                        path: config.path.clone(),
                        file_lock,
                        buffer: Arc::new(Mutex::new(Vec::new())),
                        delimiter,
                        ready,
                        pending_eof: false,
                    }),
                })
            }
            Some(FileConsumerMode::Subscribe { delete: true }) => {
                let key = format!(
                    "{}|subscribe|delete|{:?}|{:?}",
                    config.path, format, delimiter
                );

                let store = if let Some(store) = {
                    let mut stores = FILE_EVENT_STORES.lock().await;
                    stores.retain(|_, v| v.strong_count() > 0);
                    stores.get(&key).and_then(|w| w.upgrade())
                } {
                    store
                } else {
                    let created =
                        create_file_event_store(&config.path, delimiter.clone(), format).await?;
                    let mut stores = FILE_EVENT_STORES.lock().await;
                    let store = stores
                        .get(&key)
                        .and_then(|w| w.upgrade())
                        .unwrap_or_else(|| {
                            stores.insert(key.clone(), Arc::downgrade(&created));
                            created
                        });
                    store
                };

                let subscriber_id = format!("file-sub-{}", fast_uuid_v7::gen_id_str());
                info!(path = %config.path, mode = "subscribe (delete)", subscriber_id = %subscriber_id, "File consumer connected");

                Ok(Self {
                    backend: ConsumerBackend::EventStore(store.consumer(subscriber_id)),
                })
            }
        }
    }

    /// Consumer for member-based files (compressed and/or encrypted): a
    /// dedicated reader thread decodes the whole stream from the start.
    /// Restricted to the plain consume mode by the validation in `new`.
    #[cfg(any(feature = "compression", feature = "encryption"))]
    async fn new_member_consumer(
        config: &FileConfig,
        delimiter: Vec<u8>,
        format: FileFormat,
    ) -> anyhow::Result<Self> {
        let (msg_tx, msg_rx) = async_channel::bounded(100);
        let ready = Arc::new(AtomicBool::new(false));
        let ready_clone = ready.clone();

        let compression = config.compression;
        #[cfg(feature = "encryption")]
        let crypto = config
            .encryption
            .as_ref()
            .map(Crypto::new)
            .transpose()?
            .map(Arc::new);
        let make_reader = move |file: std::fs::File| -> Box<dyn std::io::Read> {
            #[cfg(feature = "encryption")]
            if let Some(crypto) = &crypto {
                return Box::new(EncryptedFramesReader::new(
                    std::io::BufReader::new(file),
                    crypto.clone(),
                    compression,
                ));
            }
            #[cfg(feature = "compression")]
            return crate::support::compression::decompress_reader(
                compression,
                std::io::BufReader::new(file),
            );
            #[cfg(not(feature = "compression"))]
            unreachable!("member consumer without compression or encryption")
        };

        let path_clone = config.path.clone();
        let format_clone = format;
        std::thread::spawn(move || {
            run_file_member_consume_task_sync(
                path_clone,
                msg_tx,
                delimiter,
                format_clone,
                ready_clone,
                make_reader,
            );
        });
        info!(path = %config.path, mode = "member consume (compressed/encrypted)", "File consumer connected");
        Ok(Self {
            backend: ConsumerBackend::Tail(FileTailConsumer {
                msg_rx,
                buffer: Vec::new(),
                offset_file: None,
                ready,
                pending_eof: false,
            }),
        })
    }

    async fn new_tail(
        path: &str,
        start_at_end: bool,
        group_id: Option<String>,
        delimiter: Vec<u8>,
        format: FileFormat,
    ) -> anyhow::Result<Self> {
        let (msg_tx, msg_rx) = async_channel::bounded(100);
        let mut initial_offset = 0;
        let ready = Arc::new(AtomicBool::new(false));
        let ready_clone = ready.clone();
        let mut offset_file = None;

        if let Some(gid) = &group_id {
            let offset_path = format!("{}.{}.offset", path, gid);
            if let Ok(content) = tokio::fs::read_to_string(&offset_path).await {
                if let Ok(pos) = content.trim().parse::<u64>() {
                    initial_offset = pos;
                    info!(
                        "Restored offset {} for group {} from {}",
                        pos, gid, offset_path
                    );
                }
            }
            let file = OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(false)
                .open(&offset_path)
                .await?;
            offset_file = Some(Arc::new(Mutex::new(file)));
        }

        if initial_offset == 0 && start_at_end {
            if let Ok(metadata) = tokio::fs::metadata(path).await {
                initial_offset = metadata.len();
            }
        }

        let path_clone = path.to_string();
        let format_clone = format;
        std::thread::spawn(move || {
            run_file_tail_task_sync(
                path_clone,
                msg_tx,
                initial_offset,
                group_id,
                delimiter,
                format_clone,
                ready_clone,
            );
        });

        info!(path = %path, mode = "tail (no-delete, optimized)", "File consumer connected");

        Ok(Self {
            backend: ConsumerBackend::Tail(FileTailConsumer {
                msg_rx,
                buffer: Vec::new(),
                offset_file,
                ready,
                pending_eof: false,
            }),
        })
    }

    /// Returns true if the consumer is ready to receive messages.
    pub fn is_ready(&self) -> bool {
        match &self.backend {
            ConsumerBackend::EventStore(_) => true,
            ConsumerBackend::Tail(c) => c.ready.load(Ordering::SeqCst),
            ConsumerBackend::Queue(c) => c.ready.load(Ordering::SeqCst),
        }
    }
}

#[async_trait]
impl MessageConsumer for FileConsumer {
    // Intentionally keeps the ordered default: the offset-tracking backend commits
    // a cumulative byte offset (the max acked `file_offset`), so out-of-order
    // commits could advance the offset past un-acked messages and lose them.
    async fn receive_batch(&mut self, max_messages: usize) -> Result<ReceivedBatch, ConsumerError> {
        match &mut self.backend {
            ConsumerBackend::EventStore(c) => c.receive_batch(max_messages).await,
            ConsumerBackend::Tail(c) => {
                // A previous greedy fill saw the end-of-file marker trailing the
                // data it returned; surface it now as an empty batch.
                if c.pending_eof {
                    c.pending_eof = false;
                    return Ok(ReceivedBatch {
                        messages: Vec::new(),
                        commit: Box::new(|_| Box::pin(async { Ok(()) })),
                    });
                }

                if c.buffer.is_empty() {
                    match c.msg_rx.recv().await {
                        // An empty batch is the watcher's end-of-file marker; fall
                        // through to return it as an empty batch.
                        Ok(batch) => c.buffer = batch,
                        Err(_) => return Err(ConsumerError::EndOfStream),
                    }
                }

                // Greedily fill buffer from channel if more messages are available
                while c.buffer.len() < max_messages {
                    match c.msg_rx.try_recv() {
                        // Stop at the end-of-file marker; remember it so the next
                        // call surfaces the empty batch after this data is served.
                        Ok(next_batch) if next_batch.is_empty() => {
                            c.pending_eof = true;
                            break;
                        }
                        Ok(mut next_batch) => c.buffer.append(&mut next_batch),
                        Err(_) => break, // Channel is empty or disconnected
                    }
                }

                let count = std::cmp::min(c.buffer.len(), max_messages);
                let messages: Vec<_> = c.buffer.drain(0..count).collect();

                let commit: crate::traits::BatchCommitFunc = if let Some(offset_file) =
                    &c.offset_file
                {
                    let offset_file = offset_file.clone();
                    let captured_messages = messages.clone();

                    Box::new(
                        move |dispositions: Vec<crate::traits::MessageDisposition>| {
                            Box::pin(async move {
                                let max_offset = dispositions
                                    .iter()
                                    .zip(captured_messages.iter())
                                    .filter_map(|(d, m)| match d {
                                        crate::traits::MessageDisposition::Ack
                                        | crate::traits::MessageDisposition::Reply(_) => m
                                            .metadata
                                            .get("file_offset")
                                            .and_then(|s| s.parse::<u64>().ok()),
                                        _ => None,
                                    })
                                    .max();

                                if let Some(offset) = max_offset {
                                    let mut file = offset_file.lock().await;
                                    if let Err(e) = file.rewind().await {
                                        tracing::error!("Failed to rewind offset file: {}", e);
                                    } else if let Err(e) = file.set_len(0).await {
                                        tracing::error!("Failed to truncate offset file: {}", e);
                                    } else if let Err(e) =
                                        file.write_all(offset.to_string().as_bytes()).await
                                    {
                                        tracing::error!("Failed to write offset file: {}", e);
                                    } else if let Err(e) = file.flush().await {
                                        tracing::error!("Failed to flush offset file: {}", e);
                                    }
                                }
                                Ok(())
                            })
                                as crate::traits::BoxFuture<'static, anyhow::Result<()>>
                        },
                    )
                } else {
                    // No-op commit since we are not deleting and no group_id to track
                    Box::new(|_dispositions: Vec<crate::traits::MessageDisposition>| {
                        Box::pin(async move { Ok(()) })
                            as crate::traits::BoxFuture<'static, anyhow::Result<()>>
                    })
                };

                Ok(ReceivedBatch { messages, commit })
            }
            ConsumerBackend::Queue(c) => {
                // A previous greedy fill saw the watcher's end-of-file marker
                // after data; surface it now as an empty batch.
                if c.pending_eof {
                    c.pending_eof = false;
                    return Ok(ReceivedBatch {
                        messages: Vec::new(),
                        commit: Box::new(|_| Box::pin(async { Ok(()) })),
                    });
                }

                {
                    let buffer = c.buffer.lock().await;
                    if buffer.is_empty() {
                        drop(buffer);
                        match c.msg_rx.recv().await {
                            // An empty batch is the watcher's end-of-file marker;
                            // fall through to return it as an empty batch.
                            Ok(b) => c.buffer.lock().await.extend(b),
                            Err(_) => return Err(ConsumerError::EndOfStream),
                        }
                    }
                }
                let mut buffer = c.buffer.lock().await;

                while buffer.len() < max_messages {
                    match c.msg_rx.try_recv() {
                        // Stop at the end-of-file marker; remember it so the next
                        // call surfaces the empty batch after this data is served.
                        Ok(b) if b.is_empty() => {
                            c.pending_eof = true;
                            break;
                        }
                        Ok(mut b) => buffer.append(&mut b),
                        Err(_) => break,
                    }
                }

                let count = std::cmp::min(buffer.len(), max_messages);
                let batch: Vec<_> = buffer.drain(0..count).collect();
                drop(buffer);

                let path = c.path.clone();
                let lock = c.file_lock.clone();
                let buffer_clone = c.buffer.clone();
                let lines_mem = c.lines_in_memory.clone();
                let batch_for_commit = batch.clone();
                let delimiter = c.delimiter.clone();

                let commit = Box::new(
                    move |dispositions: Vec<crate::traits::MessageDisposition>| {
                        Box::pin(async move {
                            let mut leading_acks = 0;
                            let mut nacked_msgs = Vec::new();
                            let mut encountered_nack = false;

                            for (i, d) in dispositions.iter().enumerate() {
                                if encountered_nack {
                                    if let Some(msg) = batch_for_commit.get(i) {
                                        nacked_msgs.push(msg.clone());
                                    }
                                    continue;
                                }
                                match d {
                                    crate::traits::MessageDisposition::Ack
                                    | crate::traits::MessageDisposition::Reply(_) => {
                                        leading_acks += 1;
                                    }
                                    crate::traits::MessageDisposition::Nack => {
                                        encountered_nack = true;
                                        if let Some(msg) = batch_for_commit.get(i) {
                                            nacked_msgs.push(msg.clone());
                                        }
                                    }
                                }
                            }

                            if !nacked_msgs.is_empty() {
                                let mut buf = buffer_clone.lock().await;
                                let old_buf = std::mem::take(&mut *buf);
                                let mut new_buf = nacked_msgs;
                                new_buf.extend(old_buf);
                                *buf = new_buf;
                            }

                            if leading_acks > 0 {
                                let _guard = lock.lock().await;
                                if let Err(e) =
                                    remove_lines_from_file(&path, leading_acks, &delimiter).await
                                {
                                    tracing::error!("Failed to remove lines from {}: {}", path, e);
                                }
                                lines_mem.fetch_sub(leading_acks, Ordering::SeqCst);
                            }
                            Ok(())
                        })
                            as crate::traits::BoxFuture<'static, anyhow::Result<()>>
                    },
                );

                Ok(ReceivedBatch {
                    messages: batch,
                    commit,
                })
            }
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// Wraps a message body for the Json/Text file formats. Generic over the payload type
/// so both formats share one struct while keeping the message_id serializer and field
/// layout (and thus the on-disk output) identical.
#[derive(serde::Serialize)]
struct RecordWrapper<'a, P: serde::Serialize> {
    #[serde(serialize_with = "crate::canonical_message::print_uuidv7")]
    message_id: u128,
    payload: P,
    metadata: &'a HashMap<String, String>,
}

/// Encodes a single message body for a non-CSV [`FileFormat`] (Raw/Normal/Json/Text).
/// Shared by the file sink and the object-store sink. CSV needs cross-record header
/// state, so it is handled inline by the file sink and rejected by the object sink.
pub(crate) fn encode_record(
    msg: &CanonicalMessage,
    format: &FileFormat,
) -> Result<Vec<u8>, serde_json::Error> {
    match format {
        FileFormat::Raw => Ok(msg.payload.to_vec()),
        // The sink format decides the encoding, not the message's origin: `normal`
        // always writes the wrapper so `message_id` and metadata survive the round
        // trip. Use `format: raw` for verbatim, unwrapped copies.
        FileFormat::Normal => serde_json::to_vec(msg),
        FileFormat::Json => {
            if let Ok(json_val) = serde_json::from_slice::<serde_json::Value>(&msg.payload) {
                serde_json::to_vec(&RecordWrapper {
                    message_id: msg.message_id,
                    payload: json_val,
                    metadata: &msg.metadata,
                })
            } else {
                serde_json::to_vec(msg)
            }
        }
        FileFormat::Text => {
            if let Ok(text) = std::str::from_utf8(&msg.payload) {
                serde_json::to_vec(&RecordWrapper {
                    message_id: msg.message_id,
                    payload: text,
                    metadata: &msg.metadata,
                })
            } else {
                serde_json::to_vec(msg)
            }
        }
        FileFormat::Csv => unreachable!("CSV is encoded by the caller, not encode_record"),
    }
}

/// Parses one file line into a message. Returns `None` for CSV header lines,
/// which establish the schema but carry no data of their own.
pub(crate) fn parse_message(
    buffer: &[u8],
    format: &FileFormat,
    csv_header: &mut Option<Vec<String>>,
) -> Option<CanonicalMessage> {
    match format {
        FileFormat::Csv => {
            let line = String::from_utf8_lossy(buffer);
            let fields = parse_csv_row(&line);
            match csv_header {
                None => {
                    *csv_header = Some(fields);
                    None
                }
                Some(cols) => {
                    // Build the JSON object bytes directly instead of constructing a
                    // serde_json::Map and re-serializing it. Avoids per-row header
                    // clones, map allocation/ordering, and a serde serialization pass.
                    let mut out = String::with_capacity(line.len() + cols.len() * 8 + 2);
                    out.push('{');
                    for (i, col) in cols.iter().enumerate() {
                        if i > 0 {
                            out.push(',');
                        }
                        out.push('"');
                        json_append_escaped(&mut out, col);
                        out.push_str("\":\"");
                        json_append_escaped(&mut out, fields.get(i).map_or("", |s| s.as_str()));
                        out.push('"');
                    }
                    out.push('}');
                    Some(CanonicalMessage::new(out.into_bytes(), None))
                }
            }
        }
        FileFormat::Raw => {
            let mut msg = CanonicalMessage::new(buffer.to_vec(), None);
            msg.metadata
                .insert("mq_bridge.original_format".to_string(), "raw".to_string());
            Some(msg)
        }
        // `json` keeps the payload as a JSON value, so it needs the whole tree.
        FileFormat::Json => {
            #[derive(serde::Deserialize)]
            struct AnyPayloadMessage {
                #[serde(deserialize_with = "deserialize_u128")]
                message_id: u128,
                payload: serde_json::Value,
                #[serde(default)]
                metadata: HashMap<String, String>,
            }

            let msg = match serde_json::from_slice::<AnyPayloadMessage>(buffer) {
                Ok(wrapper) => CanonicalMessage {
                    message_id: wrapper.message_id,
                    payload: serde_json::to_vec(&wrapper.payload)
                        .unwrap_or_default()
                        .into(),
                    metadata: wrapper.metadata,
                },
                Err(e) => raw_fallback_message(buffer, e),
            };
            Some(msg)
        }
        // `normal` and `text` want the payload as bytes, so it is decoded in one
        // pass by [`RawPayload`] rather than through a `serde_json::Value`.
        FileFormat::Normal | FileFormat::Text => {
            #[derive(serde::Deserialize)]
            struct BytePayloadMessage {
                #[serde(deserialize_with = "deserialize_u128")]
                message_id: u128,
                payload: RawPayload,
                #[serde(default)]
                metadata: HashMap<String, String>,
            }

            let msg = match serde_json::from_slice::<BytePayloadMessage>(buffer) {
                Ok(wrapper) => CanonicalMessage {
                    message_id: wrapper.message_id,
                    payload: wrapper.payload.into_bytes().into(),
                    metadata: wrapper.metadata,
                },
                Err(e) => raw_fallback_message(buffer, e),
            };
            Some(msg)
        }
    }
}

/// A line that is not the JSON envelope the format promised is kept verbatim as a
/// raw payload rather than dropped, and marked so the next hop can tell.
fn raw_fallback_message(buffer: &[u8], err: serde_json::Error) -> CanonicalMessage {
    // A file that is not JSON at all hits this for every line, so only the first
    // occurrence warns; the rest stay at debug.
    static WARNED: AtomicBool = AtomicBool::new(false);
    if !WARNED.swap(true, Ordering::Relaxed) {
        warn!(error = %err, content_length = buffer.len(), "Failed to parse file line as JSON, treating as raw. Further occurrences are logged at debug level.");
    } else {
        tracing::debug!(error = %err, content_length = buffer.len(), "Failed to parse file line as JSON, treating as raw.");
    }
    let mut msg = CanonicalMessage::new(buffer.to_vec(), None);
    msg.metadata
        .insert("mq_bridge.original_format".to_string(), "raw".to_string());
    msg
}

/// The payload of a `normal`/`text` line, decoded in a single pass.
///
/// `normal` serializes the payload as a JSON array of byte values, which is the
/// common case and the expensive one: routing it through `serde_json::Value`
/// allocates a boxed number per byte and then walks the array a second time to
/// turn it back into `Vec<u8>`. This collects those bytes straight off the
/// parser and only materializes a `Value` for payloads that are not byte arrays
/// (a `json`-format file read back as `normal`, say), which keeps the fallback
/// behaviour — render the payload as JSON text — byte-for-byte the same.
enum RawPayload {
    Bytes(Vec<u8>),
    Str(String),
    Other(serde_json::Value),
}

impl RawPayload {
    fn into_bytes(self) -> Vec<u8> {
        match self {
            RawPayload::Bytes(b) => b,
            RawPayload::Str(s) => s.into_bytes(),
            RawPayload::Other(v) => serde_json::to_vec(&v).unwrap_or_default(),
        }
    }
}

/// One element of a payload array: a byte on the fast path, anything else kept
/// as a `Value` so a non-byte array still round-trips as JSON text.
enum PayloadElement {
    Byte(u8),
    Other(serde_json::Value),
}

impl<'de> serde::Deserialize<'de> for PayloadElement {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct ElementVisitor;

        impl<'de> serde::de::Visitor<'de> for ElementVisitor {
            type Value = PayloadElement;

            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str("a JSON value")
            }

            fn visit_u64<E: serde::de::Error>(self, v: u64) -> Result<PayloadElement, E> {
                Ok(match u8::try_from(v) {
                    Ok(b) => PayloadElement::Byte(b),
                    Err(_) => PayloadElement::Other(v.into()),
                })
            }

            fn visit_i64<E: serde::de::Error>(self, v: i64) -> Result<PayloadElement, E> {
                Ok(match u8::try_from(v) {
                    Ok(b) => PayloadElement::Byte(b),
                    Err(_) => PayloadElement::Other(v.into()),
                })
            }

            fn visit_f64<E: serde::de::Error>(self, v: f64) -> Result<PayloadElement, E> {
                Ok(PayloadElement::Other(
                    serde_json::Number::from_f64(v).map_or(serde_json::Value::Null, Into::into),
                ))
            }

            fn visit_bool<E: serde::de::Error>(self, v: bool) -> Result<PayloadElement, E> {
                Ok(PayloadElement::Other(v.into()))
            }

            fn visit_str<E: serde::de::Error>(self, v: &str) -> Result<PayloadElement, E> {
                Ok(PayloadElement::Other(v.into()))
            }

            fn visit_unit<E: serde::de::Error>(self) -> Result<PayloadElement, E> {
                Ok(PayloadElement::Other(serde_json::Value::Null))
            }

            fn visit_none<E: serde::de::Error>(self) -> Result<PayloadElement, E> {
                Ok(PayloadElement::Other(serde_json::Value::Null))
            }

            fn visit_seq<A: serde::de::SeqAccess<'de>>(
                self,
                seq: A,
            ) -> Result<PayloadElement, A::Error> {
                serde::Deserialize::deserialize(serde::de::value::SeqAccessDeserializer::new(seq))
                    .map(PayloadElement::Other)
            }

            fn visit_map<A: serde::de::MapAccess<'de>>(
                self,
                map: A,
            ) -> Result<PayloadElement, A::Error> {
                serde::Deserialize::deserialize(serde::de::value::MapAccessDeserializer::new(map))
                    .map(PayloadElement::Other)
            }
        }

        d.deserialize_any(ElementVisitor)
    }
}

impl<'de> serde::Deserialize<'de> for RawPayload {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct PayloadVisitor;

        impl<'de> serde::de::Visitor<'de> for PayloadVisitor {
            type Value = RawPayload;

            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str("a byte array, a string or any JSON value")
            }

            fn visit_str<E: serde::de::Error>(self, v: &str) -> Result<RawPayload, E> {
                Ok(RawPayload::Str(v.to_string()))
            }

            fn visit_string<E: serde::de::Error>(self, v: String) -> Result<RawPayload, E> {
                Ok(RawPayload::Str(v))
            }

            fn visit_bytes<E: serde::de::Error>(self, v: &[u8]) -> Result<RawPayload, E> {
                Ok(RawPayload::Bytes(v.to_vec()))
            }

            fn visit_byte_buf<E: serde::de::Error>(self, v: Vec<u8>) -> Result<RawPayload, E> {
                Ok(RawPayload::Bytes(v))
            }

            fn visit_bool<E: serde::de::Error>(self, v: bool) -> Result<RawPayload, E> {
                Ok(RawPayload::Other(v.into()))
            }

            fn visit_u64<E: serde::de::Error>(self, v: u64) -> Result<RawPayload, E> {
                Ok(RawPayload::Other(v.into()))
            }

            fn visit_i64<E: serde::de::Error>(self, v: i64) -> Result<RawPayload, E> {
                Ok(RawPayload::Other(v.into()))
            }

            fn visit_f64<E: serde::de::Error>(self, v: f64) -> Result<RawPayload, E> {
                Ok(RawPayload::Other(
                    serde_json::Number::from_f64(v).map_or(serde_json::Value::Null, Into::into),
                ))
            }

            fn visit_unit<E: serde::de::Error>(self) -> Result<RawPayload, E> {
                Ok(RawPayload::Other(serde_json::Value::Null))
            }

            fn visit_none<E: serde::de::Error>(self) -> Result<RawPayload, E> {
                Ok(RawPayload::Other(serde_json::Value::Null))
            }

            fn visit_map<A: serde::de::MapAccess<'de>>(
                self,
                map: A,
            ) -> Result<RawPayload, A::Error> {
                serde::Deserialize::deserialize(serde::de::value::MapAccessDeserializer::new(map))
                    .map(RawPayload::Other)
            }

            /// Bytes accumulate until an element turns out not to be one; from
            /// there the array is rebuilt as a `Value` so it renders as JSON text,
            /// matching what the `Value`-based decode used to produce.
            fn visit_seq<A: serde::de::SeqAccess<'de>>(
                self,
                mut seq: A,
            ) -> Result<RawPayload, A::Error> {
                let mut bytes: Vec<u8> = Vec::with_capacity(seq.size_hint().unwrap_or(0));
                while let Some(element) = seq.next_element::<PayloadElement>()? {
                    match element {
                        PayloadElement::Byte(b) => bytes.push(b),
                        PayloadElement::Other(value) => {
                            let mut values: Vec<serde_json::Value> =
                                bytes.into_iter().map(serde_json::Value::from).collect();
                            values.push(value);
                            while let Some(rest) = seq.next_element::<PayloadElement>()? {
                                values.push(match rest {
                                    PayloadElement::Byte(b) => b.into(),
                                    PayloadElement::Other(v) => v,
                                });
                            }
                            return Ok(RawPayload::Other(serde_json::Value::Array(values)));
                        }
                    }
                }
                Ok(RawPayload::Bytes(bytes))
            }
        }

        d.deserialize_any(PayloadVisitor)
    }
}

#[cfg(test)]
mod tests;
