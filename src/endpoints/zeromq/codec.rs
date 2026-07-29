//! Backend-agnostic ZeroMQ frame codec, shared by the zmq.rs (`zeromq`) and the
//! omq.rs (`zeromq-omq`) endpoints. It operates on plain frame vectors
//! (`Vec<bytes::Bytes>`) so each backend only has to convert to/from its own
//! wire message type; the framing/format rules live here and are exercised by
//! the unit tests in `zeromq.rs`.

use crate::models::ZeroMqFormat;
use crate::traits::PublisherError;
use crate::CanonicalMessage;
use anyhow::anyhow;
use bytes::Bytes;

/// Build the wire frames for one message in `raw`/`raw_framed` mode. `raw` sends
/// the payload as a single frame; `raw_framed` prepends a JSON metadata frame so
/// the payload stays binary-safe while headers still travel. (`Json` is a
/// whole-batch format serialized by the caller, not per-message.)
pub(crate) fn encode_frames(
    message: &mut CanonicalMessage,
    format: &ZeroMqFormat,
) -> Result<Vec<Bytes>, PublisherError> {
    if matches!(format, ZeroMqFormat::RawFramed) {
        // Source/provenance keys are per-hop context and must not be forwarded.
        message.strip_source_metadata();
        let meta = serde_json::to_vec(&message.metadata)
            .map_err(|e| PublisherError::NonRetryable(anyhow!(e)))?;
        Ok(vec![Bytes::from(meta), message.payload.clone()])
    } else {
        Ok(vec![message.payload.clone()])
    }
}

/// Decode the frames of one received ZMQ message into canonical messages,
/// honouring the configured `format`. `is_sub` marks SUB traffic, whose leading
/// frame is the subscription topic rather than payload.
pub(crate) fn decode_frames(
    frames: Vec<Bytes>,
    is_sub: bool,
    format: &ZeroMqFormat,
) -> anyhow::Result<Vec<CanonicalMessage>> {
    let payload = frames.last().cloned().unwrap_or_default();
    // Only short-circuit genuinely empty single-frame inputs. Multipart Raw/RawFramed
    // messages may carry a trailing empty payload (or leading metadata) that still
    // needs to be emitted, so don't drop them here.
    if payload.is_empty() && frames.len() <= 1 {
        return Ok(vec![]);
    }
    // Only SUB traffic prepends the subscription topic as a leading frame;
    // PUSH/PULL and REP carry payload frames only, so a leading frame there is
    // not a topic. In raw_framed the leading frame is the metadata frame, not a
    // topic, so no cursor applies there either.
    let topic = if is_sub && frames.len() > 1 && !matches!(format, ZeroMqFormat::RawFramed) {
        Some(String::from_utf8_lossy(frames[0].as_ref()).into_owned())
    } else {
        None
    };
    let mut messages = match format {
        // Opaque bytes; never JSON-decode. A raw producer may send multipart, so
        // emit one message per payload frame (dropping the SUB topic frame).
        ZeroMqFormat::Raw => {
            let payload_frames = if is_sub && frames.len() > 1 {
                &frames[1..]
            } else {
                &frames[..]
            };
            payload_frames
                .iter()
                .map(|f| CanonicalMessage::new_bytes(f.clone(), None))
                .collect()
        }
        // Two-frame layout: [metadata JSON, raw payload]. Degrade to payload-only
        // when a single frame arrives (e.g. a plain raw producer).
        ZeroMqFormat::RawFramed => {
            let mut msg = CanonicalMessage::new_bytes(payload.clone(), None);
            if frames.len() > 1 {
                if let Ok(meta) = serde_json::from_slice::<std::collections::HashMap<String, String>>(
                    frames[0].as_ref(),
                ) {
                    msg.metadata = meta;
                }
            }
            vec![msg]
        }
        ZeroMqFormat::Json => {
            if let Ok(messages) = serde_json::from_slice::<Vec<CanonicalMessage>>(&payload) {
                messages
            } else if let Ok(message) = serde_json::from_slice::<CanonicalMessage>(&payload) {
                vec![message]
            } else {
                vec![CanonicalMessage::new(payload.to_vec(), None)]
            }
        }
    };
    for message in &mut messages {
        // Never let a spoofed `mqb.src.*` key in the inbound payload survive;
        // the authoritative topic cursor (SUB only) is injected below.
        message.strip_source_metadata();
    }
    // Opt-in via the MQB_SOURCE_METADATA env var; off by default.
    if crate::canonical_message::source_metadata_enabled() {
        if let Some(topic) = topic {
            for message in &mut messages {
                message
                    .metadata
                    .insert("mqb.src.zeromq_topic".to_string(), topic.clone());
            }
        }
    }
    Ok(messages)
}
