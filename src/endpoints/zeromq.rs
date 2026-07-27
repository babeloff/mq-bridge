use crate::canonical_message::tracing_support::LazyMessageIds;
use crate::models::{ZeroMqConfig, ZeroMqFormat, ZeroMqSocketType};
use crate::traits::{
    BoxFuture, ConsumerError, EndpointStatus, MessageConsumer, MessageDisposition,
    MessagePublisher, PublisherError, ReceivedBatch, SentBatch,
};
use crate::CanonicalMessage;
use anyhow::anyhow;
use async_channel::{bounded, Receiver, Sender};
use async_trait::async_trait;
use std::any::Any;
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use tokio::sync::oneshot;
use tracing::trace;
use zeromq::{Socket, SocketRecv, SocketSend, ZmqMessage};

enum SenderSocket {
    Push(zeromq::PushSocket),
    Pub(zeromq::PubSocket),
    Req(zeromq::ReqSocket),
}

enum PublisherJob {
    Send(ZmqMessage, oneshot::Sender<zeromq::ZmqResult<()>>),
    Request(ZmqMessage, oneshot::Sender<zeromq::ZmqResult<ZmqMessage>>),
}

pub struct ZeroMqPublisher {
    tx: Sender<PublisherJob>,
    expects_reply: bool,
    format: ZeroMqFormat,
}

impl ZeroMqPublisher {
    pub async fn new(config: &ZeroMqConfig) -> anyhow::Result<Self> {
        let socket_type = config.socket_type.clone().unwrap_or(ZeroMqSocketType::Push);
        let mut socket = match socket_type {
            ZeroMqSocketType::Push => {
                let mut s = zeromq::PushSocket::new();
                if config.bind {
                    s.bind(&config.url).await?;
                } else {
                    s.connect(&config.url).await?;
                }
                SenderSocket::Push(s)
            }
            ZeroMqSocketType::Pub => {
                let mut s = zeromq::PubSocket::new();
                if config.bind {
                    s.bind(&config.url).await?;
                } else {
                    s.connect(&config.url).await?;
                }
                SenderSocket::Pub(s)
            }
            ZeroMqSocketType::Req => {
                let mut s = zeromq::ReqSocket::new();
                if config.bind {
                    s.bind(&config.url).await?;
                } else {
                    s.connect(&config.url).await?;
                }
                SenderSocket::Req(s)
            }
            _ => {
                return Err(anyhow!(
                    "Unsupported socket type for publisher: {:?}",
                    socket_type
                ))
            }
        };

        let buffer_size = config.internal_buffer_size.unwrap_or(128).max(1);
        let (tx, rx) = bounded::<PublisherJob>(buffer_size);
        tokio::spawn(async move {
            while let Ok(job) = rx.recv().await {
                match job {
                    PublisherJob::Send(msg, ack_tx) => match &mut socket {
                        SenderSocket::Push(s) => {
                            let _ = ack_tx.send(s.send(msg).await);
                        }
                        SenderSocket::Pub(s) => {
                            let _ = ack_tx.send(s.send(msg).await);
                        }
                        SenderSocket::Req(_) => {
                            let err_msg = "Req socket received Send job, expected Request";
                            tracing::error!("{}", err_msg);
                            let _ = ack_tx.send(Err(zeromq::ZmqError::Other(err_msg)));
                        }
                    },
                    PublisherJob::Request(msg, reply_tx) => match &mut socket {
                        SenderSocket::Req(s) => {
                            if let Err(e) = s.send(msg).await {
                                let _ = reply_tx.send(Err(e));
                            } else {
                                let res = s.recv().await;
                                let _ = reply_tx.send(res);
                            }
                        }
                        _ => {
                            let err_msg = "Push/Pub socket received Request job, expected Send";
                            tracing::error!("{}", err_msg);
                            let _ = reply_tx.send(Err(zeromq::ZmqError::Other(err_msg)));
                        }
                    },
                }
            }
        });

        Ok(Self {
            tx,
            expects_reply: matches!(socket_type, ZeroMqSocketType::Req),
            format: config.format.clone(),
        })
    }

    /// Build the wire frames for one message in `raw`/`raw_framed` mode. `raw` sends the
    /// payload as a single frame; `raw_framed` prepends a JSON metadata frame so the
    /// payload stays binary-safe while headers still travel.
    fn frame_message(&self, message: &mut CanonicalMessage) -> Result<ZmqMessage, PublisherError> {
        if matches!(self.format, ZeroMqFormat::RawFramed) {
            // Source/provenance keys are per-hop context and must not be forwarded.
            message.strip_source_metadata();
            let meta = serde_json::to_vec(&message.metadata)
                .map_err(|e| PublisherError::NonRetryable(anyhow!(e)))?;
            let mut zmq_msg = ZmqMessage::from(bytes::Bytes::from(meta));
            zmq_msg.push_back(message.payload.clone());
            Ok(zmq_msg)
        } else {
            Ok(ZmqMessage::from(message.payload.clone()))
        }
    }

    /// Send each message as its own ZMQ message (one per message, not batched), used
    /// for `raw` and `raw_framed`. See [`frame_message`] for the per-format framing.
    async fn send_batch_raw(
        &self,
        messages: Vec<CanonicalMessage>,
    ) -> Result<SentBatch, PublisherError> {
        // Accumulate per-message outcomes so a single Retryable failure only re-sends
        // the offending message, not the whole batch (which would double-deliver the
        // ones that already succeeded).
        let mut failed = Vec::new();
        if self.expects_reply {
            let mut responses = Vec::new();
            for mut message in messages {
                let zmq_msg = match self.frame_message(&mut message) {
                    Ok(m) => m,
                    Err(e) => {
                        failed.push((message, e));
                        continue;
                    }
                };
                let (reply_tx, reply_rx) = oneshot::channel();
                if self
                    .tx
                    .send(PublisherJob::Request(zmq_msg, reply_tx))
                    .await
                    .is_err()
                {
                    failed.push((
                        message,
                        PublisherError::Retryable(anyhow!("ZeroMQ publisher task closed")),
                    ));
                    continue;
                }
                let response_zmq = match reply_rx.await {
                    Ok(Ok(r)) => r,
                    Ok(Err(e)) => {
                        failed.push((message, PublisherError::Retryable(anyhow!(e))));
                        continue;
                    }
                    Err(_) => {
                        failed.push((
                            message,
                            PublisherError::Retryable(anyhow!("ZeroMQ reply channel closed")),
                        ));
                        continue;
                    }
                };
                // REQ/REP replies are never SUB traffic, so no topic cursor applies.
                match ZeroMqConsumer::decode_batch(response_zmq, false, &self.format) {
                    Ok(decoded) => responses.extend(decoded),
                    Err(e) => failed.push((message, PublisherError::NonRetryable(anyhow!(e)))),
                }
            }
            Ok(SentBatch::Partial {
                responses: Some(responses),
                failed,
            })
        } else {
            for mut message in messages {
                let zmq_msg = match self.frame_message(&mut message) {
                    Ok(m) => m,
                    Err(e) => {
                        failed.push((message, e));
                        continue;
                    }
                };
                let (ack_tx, ack_rx) = oneshot::channel();
                if self
                    .tx
                    .send(PublisherJob::Send(zmq_msg, ack_tx))
                    .await
                    .is_err()
                {
                    failed.push((
                        message,
                        PublisherError::Retryable(anyhow!("ZeroMQ publisher task closed")),
                    ));
                    continue;
                }
                match ack_rx.await {
                    Ok(Ok(())) => {}
                    Ok(Err(e)) => failed.push((message, PublisherError::Retryable(anyhow!(e)))),
                    Err(_) => failed.push((
                        message,
                        PublisherError::Retryable(anyhow!(
                            "ZeroMQ publisher task dropped ack channel"
                        )),
                    )),
                }
            }
            if failed.is_empty() {
                Ok(SentBatch::Ack)
            } else {
                Ok(SentBatch::Partial {
                    responses: None,
                    failed,
                })
            }
        }
    }
}

#[async_trait]
impl MessagePublisher for ZeroMqPublisher {
    async fn send_batch(
        &self,
        mut messages: Vec<CanonicalMessage>,
    ) -> Result<SentBatch, PublisherError> {
        trace!(count = messages.len(), message_ids = ?LazyMessageIds(&messages), "Publishing batch of ZeroMQ messages");
        if !matches!(self.format, ZeroMqFormat::Json) {
            return self.send_batch_raw(messages).await;
        }
        // Source/provenance keys are per-hop context and must not be forwarded.
        for message in &mut messages {
            message.strip_source_metadata();
        }
        let payload =
            serde_json::to_vec(&messages).map_err(|e| PublisherError::NonRetryable(anyhow!(e)))?;
        let zmq_msg = ZmqMessage::from(bytes::Bytes::from(payload));

        if self.expects_reply {
            let (reply_tx, reply_rx) = oneshot::channel();
            self.tx
                .send(PublisherJob::Request(zmq_msg, reply_tx))
                .await
                .map_err(|_| PublisherError::Retryable(anyhow!("ZeroMQ publisher task closed")))?;
            let response_zmq = reply_rx
                .await
                .map_err(|_| PublisherError::Retryable(anyhow!("ZeroMQ reply channel closed")))?
                .map_err(|e| PublisherError::Retryable(anyhow!(e)))?;
            // REQ/REP replies are never SUB traffic, so no topic cursor applies.
            let responses = ZeroMqConsumer::decode_batch(response_zmq, false, &ZeroMqFormat::Json)
                .map_err(|e| PublisherError::NonRetryable(anyhow!(e)))?;
            Ok(SentBatch::Partial {
                responses: Some(responses),
                failed: vec![],
            })
        } else {
            let (ack_tx, ack_rx) = oneshot::channel();
            self.tx
                .send(PublisherJob::Send(zmq_msg, ack_tx))
                .await
                .map_err(|_| PublisherError::Retryable(anyhow!("ZeroMQ publisher task closed")))?;
            ack_rx
                .await
                .map_err(|_| {
                    PublisherError::Retryable(anyhow!("ZeroMQ publisher task dropped ack channel"))
                })?
                .map_err(|e| PublisherError::Retryable(anyhow!(e)))?;
            Ok(SentBatch::Ack)
        }
    }

    async fn status(&self) -> EndpointStatus {
        EndpointStatus {
            healthy: !self.tx.is_closed(),
            pending: Some(self.tx.len()),
            capacity: self.tx.capacity(),
            error: if self.tx.is_closed() {
                Some("Publisher task terminated".to_string())
            } else {
                None
            },
            ..Default::default()
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

enum ReceiverSocket {
    Pull(zeromq::PullSocket),
    Sub(zeromq::SubSocket),
    Rep(zeromq::RepSocket),
}

#[derive(Debug)]
struct ConsumerItem {
    msg: ZmqMessage,
    reply_tx: Option<oneshot::Sender<ZmqMessage>>,
}

struct BufferedMessage {
    msg: CanonicalMessage,
    reply_context: Option<ReplyContext>,
}

#[derive(Clone)]
struct ReplyContext {
    state: Arc<Mutex<BatchReplyState>>,
    index: usize,
}

struct BatchReplyState {
    tx: Option<oneshot::Sender<ZmqMessage>>,
    responses: Vec<Option<CanonicalMessage>>,
    pending: usize,
}

pub struct ZeroMqConsumer {
    rx: Receiver<Result<ConsumerItem, ConsumerError>>,
    buffer: VecDeque<BufferedMessage>,
    // Only SUB sockets prepend a subscription-topic frame; for PULL/REP a leading
    // frame is payload, not a topic, so the cursor must not be attached there.
    is_sub: bool,
    format: ZeroMqFormat,
    /// Drain mode: only then does an idle recv return without filling (empty batch).
    exit_on_empty: bool,
}

impl ZeroMqConsumer {
    pub async fn new(config: &ZeroMqConfig) -> anyhow::Result<Self> {
        let socket_type = config.socket_type.clone().unwrap_or(ZeroMqSocketType::Pull);
        let is_sub = matches!(socket_type, ZeroMqSocketType::Sub);
        let format = config.format.clone();
        let mut socket = match socket_type {
            ZeroMqSocketType::Pull => {
                let mut s = zeromq::PullSocket::new();
                if config.bind {
                    s.bind(&config.url).await?;
                } else {
                    s.connect(&config.url).await?;
                }
                ReceiverSocket::Pull(s)
            }
            ZeroMqSocketType::Sub => {
                let mut s = zeromq::SubSocket::new();
                if config.bind {
                    s.bind(&config.url).await?;
                } else {
                    s.connect(&config.url).await?;
                }
                let topic = config.topic.as_deref().unwrap_or("");
                s.subscribe(topic).await?;
                ReceiverSocket::Sub(s)
            }
            ZeroMqSocketType::Rep => {
                let mut s = zeromq::RepSocket::new();
                if config.bind {
                    s.bind(&config.url).await?;
                } else {
                    s.connect(&config.url).await?;
                }
                ReceiverSocket::Rep(s)
            }
            _ => {
                return Err(anyhow!(
                    "Unsupported socket type for consumer: {:?}",
                    socket_type
                ))
            }
        };

        let buffer_size = config.internal_buffer_size.unwrap_or(128).max(1);
        let (tx, rx) = bounded::<Result<ConsumerItem, ConsumerError>>(buffer_size);
        tokio::spawn(async move {
            loop {
                let res = match &mut socket {
                    ReceiverSocket::Pull(s) => s.recv().await.map(|msg| ConsumerItem {
                        msg,
                        reply_tx: None,
                    }),
                    ReceiverSocket::Sub(s) => s.recv().await.map(|msg| ConsumerItem {
                        msg,
                        reply_tx: None,
                    }),
                    ReceiverSocket::Rep(s) => {
                        match s.recv().await {
                            Ok(msg) => {
                                let (reply_tx, reply_rx) = oneshot::channel();
                                let item = ConsumerItem {
                                    msg,
                                    reply_tx: Some(reply_tx),
                                };
                                if tx.send(Ok(item)).await.is_err() {
                                    break;
                                }
                                // Wait for the reply from the consumer logic
                                let reply = match reply_rx.await {
                                    Ok(msg) => msg,
                                    Err(e) => {
                                        tracing::error!(
                                            "Failed to receive reply from consumer logic: {}",
                                            e
                                        );
                                        ZmqMessage::from(bytes::Bytes::from("consumer_failed"))
                                    }
                                };
                                s.send(reply).await.map(|_| ConsumerItem {
                                    msg: ZmqMessage::from(bytes::Bytes::new()),
                                    reply_tx: None,
                                }) // Dummy return to satisfy type, we loop anyway
                            }
                            Err(e) => Err(e),
                        }
                    }
                };

                // For Rep, we already handled the send inside the match. For others, we send here.
                // Actually, let's restructure to avoid the dummy return.
                if let ReceiverSocket::Rep(_) = socket {
                    if let Err(e) = res {
                        let _ = tx.send(Err(ConsumerError::Connection(anyhow!(e)))).await;
                    }
                    continue;
                }

                let item_res = res.map_err(|e| ConsumerError::Connection(anyhow!(e)));
                if tx.send(item_res).await.is_err() {
                    break;
                }
            }
        });

        Ok(Self {
            rx,
            buffer: VecDeque::new(),
            is_sub,
            format,
            exit_on_empty: false,
        })
    }

    pub(crate) fn decode_batch(
        zmq_msg: ZmqMessage,
        is_sub: bool,
        format: &ZeroMqFormat,
    ) -> anyhow::Result<Vec<CanonicalMessage>> {
        let frames = zmq_msg.into_vec();
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
                    if let Ok(meta) = serde_json::from_slice::<
                        std::collections::HashMap<String, String>,
                    >(frames[0].as_ref())
                    {
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

    async fn fill_buffer(&mut self) -> Result<(), ConsumerError> {
        // Drain mode: a brief idle timeout returns empty-handed so --drain can fire.
        let Some(item) = crate::traits::drain_gated(self.exit_on_empty, self.rx.recv()).await
        else {
            return Ok(());
        };
        let item = item.map_err(|_| ConsumerError::EndOfStream)??;
        let msgs = Self::decode_batch(item.msg, self.is_sub, &self.format)
            .map_err(|e| ConsumerError::Connection(anyhow!(e)))?;

        if let Some(tx) = item.reply_tx {
            let count = msgs.len();
            let state = Arc::new(Mutex::new(BatchReplyState {
                tx: Some(tx),
                responses: vec![None; count],
                pending: count,
            }));

            for (i, msg) in msgs.into_iter().enumerate() {
                self.buffer.push_back(BufferedMessage {
                    msg,
                    reply_context: Some(ReplyContext {
                        state: state.clone(),
                        index: i,
                    }),
                });
            }
        } else {
            for msg in msgs {
                self.buffer.push_back(BufferedMessage {
                    msg,
                    reply_context: None,
                });
            }
        }
        Ok(())
    }
}
#[async_trait]
impl MessageConsumer for ZeroMqConsumer {
    // ZeroMQ has no broker-side ack (commit only routes per-message REQ/REP
    // replies or is a no-op), so commits are order-independent.
    fn commit_requires_order(&self) -> bool {
        false
    }
    fn set_exit_on_empty(&mut self, exit_on_empty: bool) {
        self.exit_on_empty = exit_on_empty;
    }
    async fn receive_batch(&mut self, max_messages: usize) -> Result<ReceivedBatch, ConsumerError> {
        if max_messages == 0 {
            return Ok(ReceivedBatch {
                messages: Vec::new(),
                commit: Box::new(|_| Box::pin(async { Ok(()) })),
            });
        }

        if self.buffer.is_empty() {
            self.fill_buffer().await?;
        }

        let mut messages = Vec::with_capacity(max_messages);
        let mut contexts = Vec::with_capacity(max_messages);

        while messages.len() < max_messages {
            if let Some(buffered) = self.buffer.pop_front() {
                messages.push(buffered.msg);
                contexts.push(buffered.reply_context);
            } else {
                break;
            }
        }

        trace!(count = messages.len(), message_ids = ?LazyMessageIds(&messages), "Received batch of ZeroMQ messages");
        let commit = Box::new(move |dispositions: Vec<MessageDisposition>| {
            Box::pin(async move {
                for (i, ctx_opt) in contexts.into_iter().enumerate() {
                    if let Some(ctx) = ctx_opt {
                        let resp = match dispositions.get(i) {
                            Some(MessageDisposition::Reply(r)) => Some(r.clone()),
                            _ => None,
                        };

                        let mut state = ctx.state.lock().unwrap();
                        state.responses[ctx.index] = resp;
                        state.pending -= 1;

                        if state.pending == 0 {
                            if let Some(tx) = state.tx.take() {
                                let mut final_resps: Vec<CanonicalMessage> =
                                    state.responses.iter().filter_map(|r| r.clone()).collect();
                                for resp in &mut final_resps {
                                    resp.strip_source_metadata();
                                }

                                let payload = serde_json::to_vec(&final_resps).unwrap_or_default();
                                let _ = tx.send(ZmqMessage::from(bytes::Bytes::from(payload)));
                            }
                        }
                    }
                }
                Ok(())
            }) as BoxFuture<'static, anyhow::Result<()>>
        });
        Ok(ReceivedBatch { messages, commit })
    }

    async fn status(&self) -> EndpointStatus {
        EndpointStatus {
            healthy: !self.rx.is_closed(),
            pending: Some(self.rx.len() + self.buffer.len()),
            capacity: self.rx.capacity(),
            error: if self.rx.is_closed() {
                Some("Consumer task terminated".to_string())
            } else {
                None
            },
            ..Default::default()
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::ZeroMqConfig;
    use crate::CanonicalMessage;
    use tokio::time::Duration;

    #[tokio::test]
    async fn test_zeromq_push_pull() {
        let port = 5556;
        let url = format!("tcp://127.0.0.1:{}", port);

        let consumer_config = ZeroMqConfig {
            url: url.clone(),
            socket_type: Some(ZeroMqSocketType::Pull),
            bind: true,
            ..Default::default()
        };

        let publisher_config = ZeroMqConfig {
            url: url.clone(),
            socket_type: Some(ZeroMqSocketType::Push),
            bind: false,
            ..Default::default()
        };

        let mut consumer = ZeroMqConsumer::new(&consumer_config).await.unwrap();
        let publisher = ZeroMqPublisher::new(&publisher_config).await.unwrap();

        let msg = CanonicalMessage::from("hello zeromq");
        publisher.send(msg).await.unwrap();

        let received = tokio::time::timeout(Duration::from_secs(2), consumer.receive())
            .await
            .expect("Timed out waiting for message")
            .unwrap();
        assert_eq!(received.message.get_payload_str(), "hello zeromq");
    }

    #[test]
    fn decode_batch_strips_spoofed_source_metadata() {
        // A wire payload carries a reserved `mqb.src.*` key. decode_batch must drop
        // it so it can't masquerade as a framework-injected cursor downstream.
        let mut msg = CanonicalMessage::from_vec("body");
        msg.metadata
            .insert("mqb.src.kafka_offset".to_string(), "999".to_string());
        msg.metadata
            .insert("user_key".to_string(), "kept".to_string());
        let payload = serde_json::to_vec(&vec![msg]).unwrap();
        let zmq_msg = ZmqMessage::from(bytes::Bytes::from(payload));

        let decoded = ZeroMqConsumer::decode_batch(zmq_msg, false, &ZeroMqFormat::Json).unwrap();
        assert_eq!(decoded.len(), 1);
        assert!(!decoded[0].metadata.contains_key("mqb.src.kafka_offset"));
        assert_eq!(
            decoded[0].metadata.get("user_key").map(String::as_str),
            Some("kept")
        );
    }

    #[test]
    fn decode_batch_raw_returns_opaque_payload() {
        // Raw mode must hand back the frame bytes verbatim, even when they happen to
        // be valid JSON, without decoding into a CanonicalMessage.
        let json_looking = br#"[{"message_id":"x","payload":"abc"}]"#;
        let zmq_msg = ZmqMessage::from(bytes::Bytes::from(json_looking.to_vec()));

        let decoded = ZeroMqConsumer::decode_batch(zmq_msg, false, &ZeroMqFormat::Raw).unwrap();
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].payload.as_ref(), json_looking);
        assert!(decoded[0].metadata.is_empty());
    }

    #[test]
    fn decode_batch_raw_multipart_yields_one_message_per_frame() {
        // A raw producer may send a multipart message; each payload frame must become
        // its own message rather than only the last frame surfacing.
        let mut zmq_msg = ZmqMessage::from(bytes::Bytes::from_static(b"frame0"));
        zmq_msg.push_back(bytes::Bytes::from_static(b"frame1"));
        zmq_msg.push_back(bytes::Bytes::from_static(b"frame2"));

        let decoded = ZeroMqConsumer::decode_batch(zmq_msg, false, &ZeroMqFormat::Raw).unwrap();
        assert_eq!(decoded.len(), 3);
        assert_eq!(decoded[0].payload.as_ref(), b"frame0");
        assert_eq!(decoded[1].payload.as_ref(), b"frame1");
        assert_eq!(decoded[2].payload.as_ref(), b"frame2");
    }

    #[test]
    fn decode_batch_raw_sub_strips_topic_frame() {
        // For SUB sockets the leading frame is the subscription topic and must be
        // dropped; the remaining frames are payload.
        let mut zmq_msg = ZmqMessage::from(bytes::Bytes::from_static(b"my_topic"));
        zmq_msg.push_back(bytes::Bytes::from_static(b"image_bytes"));

        let decoded = ZeroMqConsumer::decode_batch(zmq_msg, true, &ZeroMqFormat::Raw).unwrap();
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].payload.as_ref(), b"image_bytes");
    }

    #[test]
    fn decode_batch_raw_framed_parses_metadata_frame() {
        // raw_framed: frame[0] is JSON metadata, frame[1] is the opaque payload.
        let meta = serde_json::json!({"kind": "jpeg", "trace_id": "abc"});
        let mut zmq_msg = ZmqMessage::from(bytes::Bytes::from(serde_json::to_vec(&meta).unwrap()));
        zmq_msg.push_back(bytes::Bytes::from_static(&[0xFF, 0xD8, 0xFF]));

        let decoded =
            ZeroMqConsumer::decode_batch(zmq_msg, false, &ZeroMqFormat::RawFramed).unwrap();
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].payload.as_ref(), &[0xFF, 0xD8, 0xFF]);
        assert_eq!(
            decoded[0].metadata.get("kind").map(String::as_str),
            Some("jpeg")
        );
        assert_eq!(
            decoded[0].metadata.get("trace_id").map(String::as_str),
            Some("abc")
        );
    }

    #[test]
    fn decode_batch_raw_framed_single_frame_degrades_to_payload() {
        // A single-frame message in raw_framed carries no metadata; treat it as payload.
        let zmq_msg = ZmqMessage::from(bytes::Bytes::from_static(b"just_payload"));
        let decoded =
            ZeroMqConsumer::decode_batch(zmq_msg, false, &ZeroMqFormat::RawFramed).unwrap();
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].payload.as_ref(), b"just_payload");
        assert!(decoded[0].metadata.is_empty());
    }

    #[tokio::test]
    async fn test_zeromq_push_pull_raw_framed() {
        let url = "tcp://127.0.0.1:5558".to_string();

        let consumer_config = ZeroMqConfig {
            url: url.clone(),
            socket_type: Some(ZeroMqSocketType::Pull),
            bind: true,
            format: ZeroMqFormat::RawFramed,
            ..Default::default()
        };
        let publisher_config = ZeroMqConfig {
            url: url.clone(),
            socket_type: Some(ZeroMqSocketType::Push),
            bind: false,
            format: ZeroMqFormat::RawFramed,
            ..Default::default()
        };

        let mut consumer = ZeroMqConsumer::new(&consumer_config).await.unwrap();
        let publisher = ZeroMqPublisher::new(&publisher_config).await.unwrap();

        let raw_bytes = vec![0xFF, 0xD8, 0xFF, 0xE0];
        let mut msg = CanonicalMessage::new(raw_bytes.clone(), None);
        msg.metadata.insert("kind".to_string(), "jpeg".to_string());
        publisher.send(msg).await.unwrap();

        let received = tokio::time::timeout(Duration::from_secs(2), consumer.receive())
            .await
            .expect("Timed out waiting for message")
            .unwrap();
        assert_eq!(received.message.payload.as_ref(), raw_bytes.as_slice());
        assert_eq!(
            received.message.metadata.get("kind").map(String::as_str),
            Some("jpeg")
        );
    }

    #[tokio::test]
    async fn test_zeromq_push_pull_raw() {
        let url = "tcp://127.0.0.1:5557".to_string();

        let consumer_config = ZeroMqConfig {
            url: url.clone(),
            socket_type: Some(ZeroMqSocketType::Pull),
            bind: true,
            format: crate::models::ZeroMqFormat::Raw,
            ..Default::default()
        };
        let publisher_config = ZeroMqConfig {
            url: url.clone(),
            socket_type: Some(ZeroMqSocketType::Push),
            bind: false,
            format: crate::models::ZeroMqFormat::Raw,
            ..Default::default()
        };

        let mut consumer = ZeroMqConsumer::new(&consumer_config).await.unwrap();
        let publisher = ZeroMqPublisher::new(&publisher_config).await.unwrap();

        // Arbitrary non-JSON binary payload, e.g. a JPEG magic header.
        let raw_bytes = vec![0xFF, 0xD8, 0xFF, 0xE0, 0x00, 0x10];
        publisher
            .send(CanonicalMessage::new(raw_bytes.clone(), None))
            .await
            .unwrap();

        let received = tokio::time::timeout(Duration::from_secs(2), consumer.receive())
            .await
            .expect("Timed out waiting for message")
            .unwrap();
        assert_eq!(received.message.payload.as_ref(), raw_bytes.as_slice());
    }
}
