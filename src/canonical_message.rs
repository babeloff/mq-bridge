//  mq-bridge
//  © Copyright 2025, by Marco Mengelkoch
//  Licensed under MIT License, see License file for more details
//  git clone https://github.com/marcomq/mq-bridge

use bytes::Bytes;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use uuid::Uuid;

use crate::type_handler::KIND_KEY;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct CanonicalMessage {
    #[serde(serialize_with = "print_uuidv7", deserialize_with = "deserialize_u128")]
    pub message_id: u128,
    pub payload: Bytes,
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub metadata: HashMap<String, String>,
}

/// Reserved prefix for framework-injected **source/provenance** metadata — the
/// per-message position a consumer read from (e.g. `mqb.src.kafka_offset`,
/// `mqb.src.nats_subject`). These keys describe where a message came from on the
/// *current* hop and are deliberately **not** forwarded: every publisher strips
/// keys with this prefix when serializing metadata to the wire/store (via
/// [`CanonicalMessage::strip_source_metadata`] or [`is_source_metadata_key`]), so
/// they do not accumulate across chained endpoints (http → nats → kafka → mongodb).
/// **Any new metadata-serializing publisher must do the same.**
/// Application metadata (user headers, `reply_to`, `correlation_id`, …) is not
/// prefixed and propagates as before.
pub const SOURCE_METADATA_PREFIX: &str = "mqb.src.";

/// Whether `key` is framework-injected source metadata that must not be forwarded.
/// See [`SOURCE_METADATA_PREFIX`].
#[inline]
pub fn is_source_metadata_key(key: &str) -> bool {
    key.starts_with(SOURCE_METADATA_PREFIX)
}

/// Whether consumers should inject source/provenance metadata (`mqb.src.*`).
///
/// Off by default — the per-message origin (topic/subject/queue, offset, …) is only
/// needed when consuming a wildcard/pattern subscription and you must recover where
/// each message actually came from (e.g. dead-letter routing). Opt in by setting the
/// `MQB_SOURCE_METADATA` env var to a truthy value (`1`, `true`, `yes`, `on`). The
/// value is read once and cached. Stripping/anti-spoofing of `mqb.src.*` stays active
/// regardless, so these keys never propagate downstream. See [`SOURCE_METADATA_PREFIX`].
pub fn source_metadata_enabled() -> bool {
    #[cfg(test)]
    {
        if let Some(forced) = TEST_FORCE_SOURCE_METADATA.with(|c| c.get()) {
            return forced;
        }
    }
    use std::sync::OnceLock;
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| {
        std::env::var("MQB_SOURCE_METADATA")
            .map(|v| {
                matches!(
                    v.trim().to_ascii_lowercase().as_str(),
                    "1" | "true" | "yes" | "on"
                )
            })
            .unwrap_or(false)
    })
}

#[cfg(test)]
thread_local! {
    /// Per-thread override for [`source_metadata_enabled`] in unit tests, so tests can
    /// exercise the enabled/disabled paths deterministically without touching the
    /// process-global env var. `None` falls back to the env-derived default.
    static TEST_FORCE_SOURCE_METADATA: std::cell::Cell<Option<bool>> =
        const { std::cell::Cell::new(None) };
}

/// Guard that restores the test-only per-thread source metadata override.
// Only referenced by endpoint test modules (nats/mqtt), so it looks unused under
// feature sets that exclude them.
#[cfg(test)]
#[allow(dead_code)]
pub(crate) struct SourceMetadataTestOverride {
    previous: Option<bool>,
}

#[cfg(test)]
impl Drop for SourceMetadataTestOverride {
    fn drop(&mut self) {
        TEST_FORCE_SOURCE_METADATA.with(|c| c.set(self.previous));
    }
}

/// Force [`source_metadata_enabled`] to a value on the current thread (test-only).
#[cfg(test)]
#[must_use]
#[allow(dead_code)]
pub(crate) fn force_source_metadata_for_test(value: Option<bool>) -> SourceMetadataTestOverride {
    let previous = TEST_FORCE_SOURCE_METADATA.with(|c| {
        let prev = c.get();
        c.set(value);
        prev
    });
    SourceMetadataTestOverride { previous }
}

pub fn print_uuidv7<S>(value: &u128, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    serializer.serialize_str(fast_uuid_v7::format_uuid(*value).as_ref())
}

/// Custom deserializer for u128 that handles UUID strings, hex, and numeric formats.
pub fn deserialize_u128<'de, D>(deserializer: D) -> Result<u128, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let val = serde_json::Value::deserialize(deserializer)?;
    u128_from_json(&val).map_err(serde::de::Error::custom)
}

pub(crate) fn u128_from_json(val: &serde_json::Value) -> Result<u128, String> {
    if let Some(s) = val.as_str() {
        if let Ok(uuid) = Uuid::parse_str(s) {
            return Ok(uuid.as_u128());
        } else if s.starts_with("0x") || s.starts_with("0X") {
            if let Ok(n) =
                u128::from_str_radix(s.trim_start_matches("0x").trim_start_matches("0X"), 16)
            {
                return Ok(n);
            }
        } else if let Ok(n) = s.parse::<u128>() {
            return Ok(n);
        }
    } else if let Some(n) = val.as_u64() {
        return Ok(n as u128);
    } else if let Some(n) = val.as_i64() {
        if n < 0 {
            return Err("message_id cannot be negative".to_string());
        }
        return Ok(n as u128);
    } else if val.is_number() {
        // Fallback for large numeric literals that don't fit in u64/i64
        if let Ok(n) = serde_json::from_value::<u128>(val.clone()) {
            return Ok(n);
        }
    } else if let Some(oid) = val.get("$oid").and_then(|v| v.as_str()) {
        if let Ok(n) = u128::from_str_radix(oid, 16) {
            return Ok(n);
        }
    }
    Err("Invalid u128 format".to_string())
}

/// Parse a message id from a string, accepting the same formats as the JSON
/// deserializer: a UUID string, a `0x`-prefixed hex literal, or a decimal
/// integer. Used by the language bindings so id parsing stays identical across
/// Rust, Python, and Node.
pub fn message_id_from_str(id: &str) -> Result<u128, String> {
    u128_from_json(&serde_json::Value::String(id.to_string()))
        .map_err(|err| format!("invalid message id '{id}': {err}"))
}

/// Format a u128 message id as a canonical UUID string (the inverse of
/// [`message_id_from_str`] for UUID-shaped ids).
pub fn format_message_id(id: u128) -> String {
    fast_uuid_v7::format_uuid(id).to_string()
}

impl CanonicalMessage {
    pub fn new(payload: Vec<u8>, message_id: Option<u128>) -> Self {
        Self {
            message_id: message_id.unwrap_or_else(fast_uuid_v7::gen_id_with_sub_ms_4),
            payload: Bytes::from(payload),
            metadata: HashMap::new(),
        }
    }

    pub fn new_bytes(payload: Bytes, message_id: Option<u128>) -> Self {
        Self {
            message_id: message_id.unwrap_or_else(fast_uuid_v7::gen_id_with_sub_ms_4),
            payload,
            metadata: HashMap::new(),
        }
    }

    pub fn from_type<T: Serialize>(data: &T) -> Result<Self, serde_json::Error> {
        let bytes = serde_json::to_vec(data)?;
        Ok(Self::new(bytes, None))
    }

    pub fn from_vec(payload: impl Into<Vec<u8>>) -> Self {
        Self::new(payload.into(), None)
    }

    pub fn set_id(&mut self, id: u128) {
        self.message_id = id;
    }

    /// Remove framework-injected source/provenance metadata (`mqb.src.*`) in place.
    /// Call before serializing an outbound message to the wire/store so per-hop
    /// cursor keys don't accumulate across endpoints. See [`SOURCE_METADATA_PREFIX`].
    #[inline]
    pub fn strip_source_metadata(&mut self) {
        self.metadata.retain(|key, _| !is_source_metadata_key(key));
    }

    pub fn from_json(payload: serde_json::Value) -> Result<Self, serde_json::Error> {
        #[derive(Deserialize)]
        struct IdExtractor {
            #[serde(deserialize_with = "deserialize_u128")]
            id: u128,
        }

        let mut message_id = None;
        for key in ["message_id", "id", "_id"] {
            if let Some(v) = payload.get(key) {
                // Use from_value with a helper struct to leverage deserialize_u128
                // and produce a proper serde_json::Error on failure.
                let mut map = serde_json::Map::new();
                map.insert("id".to_string(), v.clone());
                let extractor: IdExtractor =
                    serde_json::from_value(serde_json::Value::Object(map))?;
                message_id = Some(extractor.id);
                break;
            }
        }

        let bytes = serde_json::to_vec(&payload)?;
        Ok(Self::new(bytes, message_id))
    }

    pub fn parse<T: DeserializeOwned>(&self) -> Result<T, serde_json::Error> {
        serde_json::from_slice(&self.payload)
    }

    /// Returns the payload as a UTF-8 lossy string.
    pub fn get_payload_str(&self) -> std::borrow::Cow<'_, str> {
        String::from_utf8_lossy(&self.payload)
    }

    /// Sets the payload of this message to the given string.
    pub fn set_payload_str(&mut self, payload: impl Into<String>) {
        self.payload = Bytes::from(payload.into());
    }

    pub fn with_metadata(mut self, metadata: HashMap<String, String>) -> Self {
        self.metadata = metadata;
        self
    }

    pub fn with_metadata_kv(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.metadata.insert(key.into(), value.into());
        self
    }

    pub fn with_type_key(mut self, kind: impl Into<String>) -> Self {
        self.metadata.insert(KIND_KEY.into(), kind.into());
        self
    }

    pub fn with_raw_format(mut self) -> Self {
        self.metadata
            .insert("mq_bridge.original_format".to_string(), "raw".to_string());
        self
    }
}

impl From<&str> for CanonicalMessage {
    fn from(s: &str) -> Self {
        Self::new(s.as_bytes().into(), None)
    }
}

impl From<String> for CanonicalMessage {
    fn from(s: String) -> Self {
        Self::new(s.into_bytes(), None)
    }
}

impl From<Vec<u8>> for CanonicalMessage {
    fn from(v: Vec<u8>) -> Self {
        Self::new(v, None)
    }
}

impl From<serde_json::Value> for CanonicalMessage {
    fn from(v: serde_json::Value) -> Self {
        Self::from_json(v).expect("Failed to serialize JSON value")
    }
}

/// A context object that holds metadata and identification for a message,
/// separated from the payload. Useful for typed handlers.
#[derive(Debug, Clone)]
pub struct MessageContext {
    pub message_id: u128,
    pub metadata: HashMap<String, String>,
}

impl From<CanonicalMessage> for MessageContext {
    fn from(msg: CanonicalMessage) -> Self {
        Self {
            message_id: msg.message_id,
            metadata: msg.metadata,
        }
    }
}

#[doc(hidden)]
pub mod tracing_support {
    use super::CanonicalMessage;

    /// A helper struct to lazily format a slice of message IDs for tracing.
    /// The collection and formatting only occurs if the trace is enabled.
    pub struct LazyMessageIds<'a>(pub &'a [CanonicalMessage]);

    impl<'a> std::fmt::Debug for LazyMessageIds<'a> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            let ids: Vec<String> = self
                .0
                .iter()
                .map(|m| format!("{:032x}", m.message_id))
                .collect();
            f.debug_list().entries(ids).finish()
        }
    }
}

#[doc(hidden)]
pub mod macro_support {
    use super::CanonicalMessage;
    use serde::Serialize;

    pub trait Fallback {
        fn convert(&self) -> CanonicalMessage;
    }

    impl<T: Serialize> Fallback for Wrap<T> {
        fn convert(&self) -> CanonicalMessage {
            CanonicalMessage::from_type(&self.0).expect("Serialization failed in msg! macro")
        }
    }

    pub struct Wrap<T>(pub T);

    impl<T> Wrap<T>
    where
        T: Into<CanonicalMessage> + Clone,
    {
        pub fn convert(&self) -> CanonicalMessage {
            self.0.clone().into()
        }
    }
}

/// A macro to create a `CanonicalMessage` easily.
///
/// Examples:
/// ```rust
/// use mq_bridge::msg;
///
/// let m1 = msg!("hello");
/// let m2 = msg!("hello", "greeting");
/// let m3 = msg!("hello", "kind" => "greeting");
///
/// #[derive(serde::Serialize, Clone)]
/// struct MyData { val: i32 }
/// let m4 = msg!(MyData { val: 42 }, "my_type");
/// ```
#[macro_export]
macro_rules! msg {
    ($payload:expr $(, $key:expr => $val:expr)* $(,)?) => {
        {
            #[allow(unused_imports)]
            use $crate::canonical_message::macro_support::{Wrap, Fallback};
            #[allow(unused_mut)]
            let mut message = Wrap($payload).convert();
            $(
                message = message.with_metadata_kv($key, $val);
            )*
            message
        }
    };
    ($payload:expr, $kind:expr $(,)?) => {
        {
            #[allow(unused_imports)]
            use $crate::canonical_message::macro_support::{Wrap, Fallback};
            let mut message = Wrap($payload).convert();
            message = message.with_type_key($kind);
            message
        }
    };
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn source_metadata_key_detection() {
        assert!(is_source_metadata_key("mqb.src.kafka_offset"));
        assert!(is_source_metadata_key("mqb.src.nats_subject"));
        assert!(!is_source_metadata_key("kind"));
        assert!(!is_source_metadata_key("reply_to"));
        assert!(!is_source_metadata_key("correlation_id"));
        // The reserved prefix itself is the boundary.
        assert_eq!(SOURCE_METADATA_PREFIX, "mqb.src.");
    }

    #[test]
    fn test_message_id_parsing() {
        // String UUID
        let uuid = "550e8400-e29b-41d4-a716-446655440000";
        let msg = CanonicalMessage::from_json(json!({ "id": uuid })).unwrap();
        assert_eq!(msg.message_id, 113059749145936325402354257176981405696);

        // Hex string
        let msg = CanonicalMessage::from_json(json!({ "id": "0xFF" })).unwrap();
        assert_eq!(msg.message_id, 255);

        // Numeric
        let msg = CanonicalMessage::from_json(json!({ "id": 100 })).unwrap();
        assert_eq!(msg.message_id, 100);

        // Negative numeric
        let msg_err = CanonicalMessage::from_json(json!({ "id": -1 }));
        assert!(msg_err.is_err());

        // Mongo OID
        let oid = "507f1f77bcf86cd799439011";
        let msg = CanonicalMessage::from_json(json!({ "_id": { "$oid": oid } })).unwrap();
        let expected = u128::from_str_radix(oid, 16).unwrap();
        assert_eq!(msg.message_id, expected);
    }

    #[test]
    fn test_message_id_from_str_helper() {
        // The string helper the bindings call accepts the same formats as the
        // JSON path: UUID, 0x-hex, and decimal.
        let uuid = "550e8400-e29b-41d4-a716-446655440000";
        assert_eq!(
            message_id_from_str(uuid).unwrap(),
            113059749145936325402354257176981405696
        );
        assert_eq!(message_id_from_str("0xFF").unwrap(), 255);
        assert_eq!(message_id_from_str("100").unwrap(), 100);
        assert!(message_id_from_str("not-an-id").is_err());

        // A UUID id round-trips through format_message_id unchanged.
        let id = message_id_from_str(uuid).unwrap();
        assert_eq!(format_message_id(id), uuid);
    }

    #[test]
    fn test_metadata_builder() {
        let msg = CanonicalMessage::new(b"payload".to_vec(), None)
            .with_metadata_kv("key1", "val1")
            .with_type_key("my_type");

        assert_eq!(msg.metadata.get("key1").map(|s| s.as_str()), Some("val1"));
        assert_eq!(
            msg.metadata.get("kind").map(|s| s.as_str()),
            Some("my_type")
        );
    }
}
