use crate::canonical_message::tracing_support::LazyMessageIds;
use crate::models::{MqttConfig, MqttProtocol};
use crate::traits::{
    BoxFuture, ConsumerError, MessageConsumer, MessageDisposition, MessagePublisher,
    PublisherError, Received, ReceivedBatch, Sent, SentBatch,
};
use crate::CanonicalMessage;
use crate::APP_NAME;
use anyhow::anyhow;
use async_channel::{bounded, Receiver, Sender};
use async_trait::async_trait;
use rumqttc::v5::mqttbytes::v5::{Publish as PublishV5, PublishProperties};
use rumqttc::v5::mqttbytes::QoS as QoSV5;
use rumqttc::v5::{
    AsyncClient as AsyncClientV5, EventLoop as EventLoopV5, MqttOptions as MqttOptionsV5,
};
use rumqttc::Outgoing;
use rumqttc::Publish as PublishV3;
use rumqttc::{tokio_rustls::rustls, AsyncClient, MqttOptions, QoS, Transport};
use std::any::Any;
use std::collections::{HashMap, VecDeque};
use std::fmt::Debug;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::sync::RwLock;
use tracing::{debug, error, info, trace, warn};

/// How long the publisher waits for end-to-end broker confirmation (PUBACK for
/// QoS 1, PUBCOMP for QoS 2) before treating a publish as failed and handing it
/// back for retry. Generous enough to ride out a broker restart (the chaos
/// test) while still bounding a permanently stuck publish.
const CONFIRM_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Clone)]
pub enum Client {
    V3(AsyncClient),
    V5(AsyncClientV5),
}

fn to_qos_v5(qos: QoS) -> QoSV5 {
    match qos {
        QoS::AtMostOnce => QoSV5::AtMostOnce,
        QoS::AtLeastOnce => QoSV5::AtLeastOnce,
        QoS::ExactlyOnce => QoSV5::ExactlyOnce,
    }
}

use crate::traits::EndpointStatus;
impl Client {
    async fn ack(&self, ack: &MqttAck) -> anyhow::Result<()> {
        match (self, ack) {
            (Client::V3(c), MqttAck::V3(p)) => c.ack(p).await.map_err(|e| e.into()),
            (Client::V5(c), MqttAck::V5(p)) => c.ack(p).await.map_err(|e| e.into()),
            _ => Ok(()), // Mismatch or None (QoS 0), ignore
        }
    }

    async fn publish(
        &self,
        topic: &str,
        qos: QoS,
        message: CanonicalMessage,
    ) -> anyhow::Result<()> {
        match self {
            Client::V5(client) => {
                let mut props = PublishProperties::default();
                if let Some(rt) = message.metadata.get("reply_to") {
                    props.response_topic = Some(rt.clone());
                }
                if let Some(cd) = message.metadata.get("correlation_id") {
                    props.correlation_data = Some(cd.as_bytes().to_vec().into());
                }
                let mut user_properties: Vec<(String, String)> =
                    message.metadata.into_iter().collect();
                user_properties.push((
                    "mq_bridge.message_id".to_string(),
                    format!("{:032x}", message.message_id),
                ));
                props.user_properties = user_properties;
                client
                    .publish_with_properties(topic, to_qos_v5(qos), false, message.payload, props)
                    .await
                    .map_err(|e| e.into())
            }
            Client::V3(client) => {
                let payload = if !message.metadata.is_empty() {
                    serde_json::to_vec(&message)?
                } else {
                    message.payload.into()
                };
                client
                    .publish(topic, qos, false, payload)
                    .await
                    .map_err(|e| e.into())
            }
        }
    }

    async fn subscribe(&self, topic: &str, qos: QoS) -> anyhow::Result<()> {
        match self {
            Client::V3(client) => client.subscribe(topic, qos).await.map_err(|e| e.into()),
            Client::V5(client) => client
                .subscribe(topic, to_qos_v5(qos))
                .await
                .map_err(|e| e.into()),
        }
    }
}

/// Tracks in-flight QoS 1/2 publishes so the publisher can wait for genuine
/// end-to-end broker confirmation (PUBACK for QoS 1, PUBCOMP for QoS 2) instead
/// of returning success as soon as a message is handed to rumqttc's request
/// channel. Without this, the broker can reject a publish (e.g. `QuotaExceeded`)
/// or lose it on restart and mq-bridge would never notice — the retry
/// middleware never fires and the message is silently dropped.
///
/// Correlation relies on the event loop emitting exactly one
/// `Outgoing::Publish(pkid)` per fresh user publish, in the same FIFO order the
/// publishes are pushed onto rumqttc's request channel. [`MqttPublisher`]
/// preserves that order by registering each token under `publish_lock` right
/// before enqueuing the publish.
#[derive(Default)]
struct ConfirmState {
    /// Tokens for messages enqueued but not yet assigned a packet id by the
    /// event loop (not yet seen as `Outgoing::Publish`). FIFO.
    awaiting_pkid: VecDeque<oneshot::Sender<bool>>,
    /// Packet id -> completion token, for publishes the broker is confirming.
    inflight: HashMap<u16, oneshot::Sender<bool>>,
}

#[derive(Default)]
struct PublishConfirmer {
    state: std::sync::Mutex<ConfirmState>,
}

impl PublishConfirmer {
    /// Register a publish that is about to be enqueued. The returned receiver
    /// resolves to `true` once the broker confirms delivery, or `false` if the
    /// broker rejects it or the session is lost. Callers MUST register in the
    /// same order they enqueue publishes (see `publish_lock`).
    fn register(&self) -> oneshot::Receiver<bool> {
        let (tx, rx) = oneshot::channel();
        self.state.lock().unwrap().awaiting_pkid.push_back(tx);
        rx
    }

    /// Drop the most recently registered token. Used when the enqueue that the
    /// token was registered for failed, so no `Outgoing::Publish` will ever
    /// arrive for it (otherwise it would desync every later correlation).
    fn cancel_last(&self) {
        self.state.lock().unwrap().awaiting_pkid.pop_back();
    }

    /// The event loop assigned `pkid` to an outgoing publish.
    fn on_outgoing_publish(&self, pkid: u16) {
        if pkid == 0 {
            return; // QoS 0 — never registered, never confirmed.
        }
        let mut s = self.state.lock().unwrap();
        // A resend after a session-resume reconnect reuses the original packet
        // id; its token is already tracked, so don't consume a fresh one.
        if s.inflight.contains_key(&pkid) {
            return;
        }
        if let Some(tx) = s.awaiting_pkid.pop_front() {
            s.inflight.insert(pkid, tx);
        }
    }

    /// The broker reached a terminal state for `pkid`.
    fn settle(&self, pkid: u16, delivered: bool) {
        let tx = self.state.lock().unwrap().inflight.remove(&pkid);
        if let Some(tx) = tx {
            let _ = tx.send(delivered);
        }
    }

    /// The session was lost (fresh session on (re)connect): the broker dropped
    /// every outstanding publish, so they must all be retried.
    fn fail_all(&self) {
        let mut s = self.state.lock().unwrap();
        for tx in s.awaiting_pkid.drain(..) {
            let _ = tx.send(false);
        }
        for (_, tx) in s.inflight.drain() {
            let _ = tx.send(false);
        }
    }
}

fn should_fail_outstanding_on_connack(has_seen_connack: bool, session_present: bool) -> bool {
    has_seen_connack && !session_present
}

struct MqttState {
    client: Client,
    _stop_tx: mpsc::Sender<()>,
    is_connected: Arc<AtomicBool>,
}

pub struct MqttPublisher {
    state: Arc<RwLock<MqttState>>,
    topic: String,
    qos: QoS,
    confirmer: Arc<PublishConfirmer>,
    /// Serializes the enqueue phase across concurrent `send`/`send_batch` calls
    /// so confirmation tokens are registered in the same order publishes hit
    /// rumqttc's request channel. Only held across the (cheap) enqueue, never
    /// across confirmation waits.
    publish_lock: tokio::sync::Mutex<()>,
}

impl MqttPublisher {
    pub async fn new(config: &MqttConfig) -> anyhow::Result<Self> {
        let topic = config
            .topic
            .as_deref()
            .ok_or_else(|| anyhow!("Topic is required for MQTT publisher"))?;
        let client_id = config.client_id.clone().unwrap_or_else(|| {
            sanitize_for_client_id(&format!("{}-{}", APP_NAME, fast_uuid_v7::gen_id()))
        });

        let confirmer = Arc::new(PublishConfirmer::default());
        let state = Self::connect(config, &client_id, confirmer.clone()).await?;
        let qos = parse_qos(config.qos.unwrap_or(1));

        Ok(Self {
            state: Arc::new(RwLock::new(state)),
            topic: topic.to_string(),
            qos,
            confirmer,
            publish_lock: tokio::sync::Mutex::new(()),
        })
    }

    async fn connect(
        config: &MqttConfig,
        client_id: &str,
        confirmer: Arc<PublishConfirmer>,
    ) -> anyhow::Result<MqttState> {
        let (client, eventloop) = create_client_and_eventloop(config, client_id).await?;
        let (stop_tx, stop_rx) = mpsc::channel(1);
        let is_connected = Arc::new(AtomicBool::new(false));

        tokio::spawn(run_eventloop(
            eventloop,
            None::<Sender<MqttInternalMessage>>,
            stop_rx,
            None,
            !config.delayed_ack,
            is_connected.clone(),
            Some(confirmer),
        ));

        Ok(MqttState {
            client,
            _stop_tx: stop_tx,
            is_connected,
        })
    }

    /// Enqueue `messages` in order and return one confirmation receiver per
    /// successfully enqueued message. On the first enqueue failure, returns the
    /// receivers gathered so far plus the error (the whole batch is then retried
    /// by the caller). Only valid for QoS > 0.
    async fn enqueue_confirmed(
        &self,
        messages: &[CanonicalMessage],
    ) -> (Vec<oneshot::Receiver<bool>>, Option<anyhow::Error>) {
        let client = self.state.read().await.client.clone();
        let mut receivers = Vec::with_capacity(messages.len());
        // Hold the ordering lock across the enqueue loop so token registration
        // order matches request-channel order for pkid correlation.
        let _guard = self.publish_lock.lock().await;
        for message in messages {
            let rx = self.confirmer.register();
            let publish_future = client.publish(&self.topic, self.qos, message.clone());
            match tokio::time::timeout(Duration::from_secs(10), publish_future).await {
                Ok(Ok(_)) => receivers.push(rx),
                Ok(Err(e)) => {
                    // Enqueue failed: no Outgoing::Publish will arrive for this
                    // token, so drop it to keep correlation aligned.
                    self.confirmer.cancel_last();
                    return (
                        receivers,
                        Some(anyhow!("Failed to publish MQTT message in batch: {}", e)),
                    );
                }
                Err(_) => {
                    self.confirmer.cancel_last();
                    return (receivers, Some(anyhow!("MQTT publish timed out in batch")));
                }
            }
        }
        (receivers, None)
    }
}

/// Wait for every confirmation receiver to resolve to `true` within
/// [`CONFIRM_TIMEOUT`]. Returns `false` if any message is rejected, the session
/// is lost, the event loop is gone, or the wait times out.
async fn all_confirmed(receivers: Vec<oneshot::Receiver<bool>>) -> bool {
    let wait = async {
        for rx in receivers {
            match rx.await {
                Ok(true) => {}
                _ => return false,
            }
        }
        true
    };
    matches!(tokio::time::timeout(CONFIRM_TIMEOUT, wait).await, Ok(true))
}

/// QoS 1 PUBACK: delivery succeeded (a message reaching no subscribers is still
/// a successful delivery to the broker, not a failure to publish).
fn puback_v5_ok(reason: rumqttc::v5::mqttbytes::v5::PubAckReason) -> bool {
    use rumqttc::v5::mqttbytes::v5::PubAckReason as R;
    matches!(reason, R::Success | R::NoMatchingSubscribers)
}

/// QoS 2 PUBREC carrying a failure reason (`>= 0x80`, e.g. `QuotaExceeded`)
/// aborts the handshake — the publish is rejected.
fn pubrec_v5_failed(reason: rumqttc::v5::mqttbytes::v5::PubRecReason) -> bool {
    use rumqttc::v5::mqttbytes::v5::PubRecReason as R;
    !matches!(reason, R::Success | R::NoMatchingSubscribers)
}

/// QoS 2 PUBCOMP: the only success reason is `Success`.
fn pubcomp_v5_ok(reason: rumqttc::v5::mqttbytes::v5::PubCompReason) -> bool {
    matches!(reason, rumqttc::v5::mqttbytes::v5::PubCompReason::Success)
}

/// Build a `SentBatch::Partial` that hands every message in the batch back for
/// retry. MQTT publishing is all-or-nothing here: a batch either confirms fully
/// or is retried whole, so partial success is never reported.
fn retry_whole_batch(messages: Vec<CanonicalMessage>) -> SentBatch {
    let failed = messages
        .into_iter()
        .map(|m| {
            (
                m,
                PublisherError::Retryable(anyhow!("MQTT batch not confirmed by broker")),
            )
        })
        .collect();
    SentBatch::Partial {
        responses: None,
        failed,
    }
}

#[async_trait]
impl MessagePublisher for MqttPublisher {
    async fn send(&self, message: CanonicalMessage) -> Result<Sent, PublisherError> {
        trace!(
            message_id = %format!("{:032x}", message.message_id),
            topic = %self.topic,
            payload_size = message.payload.len(),
            "Publishing MQTT message"
        );

        // QoS 0 is fire-and-forget: there is no broker acknowledgement to wait
        // for, so enqueueing is the only confirmation available.
        if self.qos == QoS::AtMostOnce {
            let client = self.state.read().await.client.clone();
            let publish_future = client.publish(&self.topic, self.qos, message);
            return match tokio::time::timeout(Duration::from_secs(10), publish_future).await {
                Ok(Ok(_)) => Ok(Sent::Ack),
                Ok(Err(e)) => Err(PublisherError::Connection(anyhow!(
                    "Failed to publish MQTT message: {}",
                    e
                ))),
                Err(_) => Err(PublisherError::Connection(anyhow!(
                    "MQTT publish timed out"
                ))),
            };
        }

        // QoS 1/2: wait for genuine broker confirmation (PUBACK / PUBCOMP) so a
        // rejected or lost message surfaces as a retryable error instead of a
        // silent drop.
        let (receivers, enqueue_err) = self.enqueue_confirmed(std::slice::from_ref(&message)).await;
        if let Some(e) = enqueue_err {
            return Err(PublisherError::Connection(e));
        }
        if all_confirmed(receivers).await {
            Ok(Sent::Ack)
        } else {
            Err(PublisherError::Retryable(anyhow!(
                "MQTT message not confirmed by broker"
            )))
        }
    }

    async fn send_batch(
        &self,
        messages: Vec<CanonicalMessage>,
    ) -> Result<SentBatch, PublisherError> {
        trace!(count = messages.len(), topic = %self.topic, message_ids = ?LazyMessageIds(&messages), "Publishing batch of MQTT messages");

        // QoS 0: fire-and-forget, enqueue is the only confirmation.
        if self.qos == QoS::AtMostOnce {
            let client = self.state.read().await.client.clone();
            for message in &messages {
                let publish_future = client.publish(&self.topic, self.qos, message.clone());
                if let Ok(Err(_)) | Err(_) =
                    tokio::time::timeout(Duration::from_secs(10), publish_future).await
                {
                    return Ok(retry_whole_batch(messages));
                }
            }
            return Ok(SentBatch::Ack);
        }

        // QoS 1/2: enqueue in order, then wait for every message to be confirmed
        // by the broker. If enqueue fails or any confirmation does not arrive,
        // hand the whole batch back for retry rather than reporting a false Ack.
        let (receivers, enqueue_err) = self.enqueue_confirmed(&messages).await;
        if let Some(e) = enqueue_err {
            warn!(
                "MQTT batch enqueue failed, marking all {} messages for retry: {}",
                messages.len(),
                e
            );
            return Ok(retry_whole_batch(messages));
        }

        if all_confirmed(receivers).await {
            Ok(SentBatch::Ack)
        } else {
            warn!(
                "MQTT batch not confirmed by broker, marking all {} messages for retry",
                messages.len()
            );
            Ok(retry_whole_batch(messages))
        }
    }

    async fn status(&self) -> EndpointStatus {
        let state = self.state.read().await;
        let healthy = state.is_connected.load(Ordering::Relaxed);
        EndpointStatus {
            healthy,
            target: self.topic.clone(),
            error: if healthy {
                None
            } else {
                Some("Disconnected".to_string())
            },
            ..Default::default()
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

pub struct MqttConsumer(MqttListener);

impl MqttConsumer {
    pub async fn new(config: &MqttConfig) -> anyhow::Result<Self> {
        let topic = config
            .topic
            .as_deref()
            .ok_or_else(|| anyhow!("Topic is required for MQTT consumer"))?;
        let client_id = config.client_id.clone().unwrap_or_else(|| {
            sanitize_for_client_id(&format!("{}-{}", APP_NAME, fast_uuid_v7::gen_id()))
        });

        let listener = MqttListener::new(config, topic, &client_id, "consumer").await?;
        warn!("Known issue: Messages might be lost in rare cases if the MQTT broker is restarted while the consumer is running.");
        Ok(Self(listener))
    }
}

#[async_trait]
impl MessageConsumer for MqttConsumer {
    async fn receive(&mut self) -> Result<Received, ConsumerError> {
        self.0.receive().await
    }
    async fn receive_batch(&mut self, max_messages: usize) -> Result<ReceivedBatch, ConsumerError> {
        self.0.receive_batch(max_messages).await
    }
    async fn status(&self) -> crate::traits::EndpointStatus {
        self.0.status().await
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[derive(Debug)]
enum EventWrapper {
    V3(rumqttc::Event),
    V5(Box<rumqttc::v5::Event>),
}

enum EventLoop {
    V3(Box<rumqttc::EventLoop>),
    V5(Box<EventLoopV5>),
}

struct MqttInternalMessage {
    msg: CanonicalMessage,
    ack: MqttAck,
}

enum MqttAck {
    V3(PublishV3),
    V5(PublishV5),
    None,
}

struct MqttListener {
    client: Client,
    _stop_tx: mpsc::Sender<()>,
    message_rx: Receiver<MqttInternalMessage>,
    capacity: usize,
    is_connected: Arc<AtomicBool>,
    topic: String,
}

impl MqttListener {
    async fn new(
        config: &MqttConfig,
        topic: &str,
        client_id: &str,
        _client_type: &'static str,
    ) -> anyhow::Result<Self> {
        let (client, eventloop) = create_client_and_eventloop(config, client_id).await?;
        let qos = parse_qos(config.qos.unwrap_or(1));
        let queue_capacity = config.queue_capacity.unwrap_or(100);
        let (tx, rx) = bounded(queue_capacity);
        let (stop_tx, stop_rx) = mpsc::channel(1);
        let is_connected = Arc::new(AtomicBool::new(false));

        let sub_info = Some((client.clone(), topic.to_string(), qos));
        tokio::spawn(run_eventloop(
            eventloop,
            Some(tx),
            stop_rx,
            sub_info,
            !config.delayed_ack,
            is_connected.clone(),
            None,
        ));

        client.subscribe(topic, qos).await?;
        info!(topic = %topic, client_id = %client_id, "MQTT subscribed");

        Ok(Self {
            client,
            _stop_tx: stop_tx,
            message_rx: rx,
            capacity: queue_capacity,
            is_connected,
            topic: topic.to_string(),
        })
    }
}

#[async_trait]
impl MessageConsumer for MqttListener {
    async fn receive(&mut self) -> Result<Received, ConsumerError> {
        let internal = self
            .message_rx
            .recv()
            .await
            .map_err(|_| ConsumerError::EndOfStream)?;

        let message = internal.msg;
        let client = self.client.clone();
        let reply_topic = message.metadata.get("reply_to").cloned();
        let correlation_data = message.metadata.get("correlation_id").cloned();
        let ack_info = internal.ack;

        let commit = Box::new(move |disposition: MessageDisposition| {
            Box::pin(async move {
                match disposition {
                    MessageDisposition::Nack => return Ok(()),
                    MessageDisposition::Reply(resp) => {
                        handle_mqtt_reply(&client, reply_topic, correlation_data, resp).await?;
                        // Fallthrough to Ack
                    }
                    MessageDisposition::Ack => {
                        // Fallthrough to Ack
                    }
                }

                // Acknowledge the original message
                if let Err(e) = client.ack(&ack_info).await {
                    error!("Failed to ack MQTT message: {}", e);
                    return Err(anyhow!("Failed to ack MQTT message: {}", e));
                }
                Ok(())
            }) as BoxFuture<'static, anyhow::Result<()>>
        });
        Ok(Received { message, commit })
    }

    async fn receive_batch(&mut self, max_messages: usize) -> Result<ReceivedBatch, ConsumerError> {
        let mut messages = Vec::with_capacity(max_messages);
        let mut reply_infos = Vec::with_capacity(max_messages);
        let mut acks = Vec::with_capacity(max_messages);

        // Block for the first message
        match self.message_rx.recv().await {
            Ok(internal) => {
                reply_infos.push((
                    internal.msg.metadata.get("reply_to").cloned(),
                    internal.msg.metadata.get("correlation_id").cloned(),
                ));
                messages.push(internal.msg);
                acks.push(internal.ack);
            }
            Err(_) => return Err(ConsumerError::EndOfStream),
        }

        // Greedily consume more messages if they are already buffered, up to max_messages.
        while messages.len() < max_messages {
            match self.message_rx.try_recv() {
                Ok(internal) => {
                    reply_infos.push((
                        internal.msg.metadata.get("reply_to").cloned(),
                        internal.msg.metadata.get("correlation_id").cloned(),
                    ));
                    messages.push(internal.msg);
                    acks.push(internal.ack);
                }
                Err(_) => break, // Empty or Disconnected
            }
        }

        let client = self.client.clone();
        let commit = Box::new(move |dispositions: Vec<MessageDisposition>| {
            Box::pin(async move {
                // Length check to avoid silent truncation
                if dispositions.len() != reply_infos.len() || dispositions.len() != acks.len() {
                    return Err(anyhow!(
                        "MQTT batch commit: mismatched lengths: dispositions={}, reply_infos={}, acks={}",
                        dispositions.len(), reply_infos.len(), acks.len()
                    ));
                }
                let mut ack_futures = Vec::with_capacity(dispositions.len());

                for (((reply_topic, correlation_data), ack), disposition) in
                    reply_infos.into_iter().zip(acks).zip(dispositions)
                {
                    let client = client.clone();
                    ack_futures.push(async move {
                        match disposition {
                            MessageDisposition::Reply(resp) => {
                                handle_mqtt_reply(&client, reply_topic, correlation_data, resp)
                                    .await?;
                                client.ack(&ack).await.map_err(|e| {
                                    error!("Failed to ack MQTT message in batch: {}", e);
                                    anyhow!("Failed to ack MQTT message batch: {}", e)
                                })
                            }
                            MessageDisposition::Ack => client.ack(&ack).await.map_err(|e| {
                                error!("Failed to ack MQTT message in batch: {}", e);
                                e
                            }),
                            MessageDisposition::Nack => Ok(()),
                        }
                    });
                }

                futures::future::try_join_all(ack_futures).await?;
                Ok(())
            }) as BoxFuture<'static, anyhow::Result<()>>
        });
        Ok(ReceivedBatch { messages, commit })
    }

    async fn status(&self) -> crate::traits::EndpointStatus {
        let healthy = self.is_connected.load(Ordering::Relaxed);
        crate::traits::EndpointStatus {
            healthy,
            target: self.topic.clone(),
            pending: Some(self.message_rx.len()),
            capacity: Some(self.capacity),
            error: if healthy {
                None
            } else {
                Some("Disconnected".to_string())
            },
            ..Default::default()
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

async fn handle_mqtt_reply(
    client: &Client,
    reply_topic: Option<String>,
    correlation_data: Option<String>,
    resp: CanonicalMessage,
) -> anyhow::Result<()> {
    if let Some(rt) = reply_topic {
        trace!(topic = %rt, "Committing MQTT message, sending reply");
        let mut msg = resp;
        if let Some(cd) = correlation_data {
            msg.metadata.insert("correlation_id".to_string(), cd);
        }
        // Use a timeout to prevent hanging if the client buffer is full or eventloop is stuck
        match tokio::time::timeout(
            Duration::from_secs(60),
            client.publish(&rt, QoS::AtLeastOnce, msg),
        )
        .await
        {
            Ok(Err(e)) => {
                error!(topic = %rt, error = %e, "Failed to publish MQTT reply");
                return Err(anyhow::anyhow!("Failed to publish MQTT reply: {}", e));
            }
            Ok(Ok(_)) => trace!(topic = %rt, "MQTT reply published to channel"),
            Err(_) => {
                error!(topic = %rt, "Timed out publishing MQTT reply");
                return Err(anyhow::anyhow!("Timed out publishing MQTT reply to {}", rt));
            }
        }
    }
    Ok(())
}

async fn create_client_and_eventloop(
    config: &MqttConfig,
    client_id: &str,
) -> anyhow::Result<(Client, EventLoop)> {
    let (host, port) = parse_url(&config.url)?;
    let queue_capacity = config.queue_capacity.unwrap_or(100);

    let (client, eventloop) = match config.protocol {
        MqttProtocol::V5 => {
            let mut mqttoptions = MqttOptionsV5::new(client_id, host, port);
            mqttoptions
                .set_keep_alive(Duration::from_secs(config.keep_alive_seconds.unwrap_or(20)));
            mqttoptions.set_manual_acks(!config.delayed_ack);
            let default_window: u16 = match config.max_inflight {
                Some(v) => v,
                None => {
                    let capped = std::cmp::min(queue_capacity, u16::MAX as usize);
                    capped as u16
                }
            };
            mqttoptions.set_outgoing_inflight_upper_limit(default_window);
            mqttoptions.set_receive_maximum(Some(default_window));
            mqttoptions.set_max_packet_size(Some(10 * 1024 * 1024)); // Set max packet size to 10MB
            mqttoptions.set_clean_start(config.clean_session);

            if let Some(expiry) = config.session_expiry_interval {
                mqttoptions.set_session_expiry_interval(Some(expiry));
            } else if !config.clean_session {
                // If persistence is requested but no expiry set, default to 1 hour to ensure session survives disconnects.
                mqttoptions.set_session_expiry_interval(Some(3600));
            }

            if let (Some(username), Some(password)) = (&config.username, &config.password) {
                mqttoptions.set_credentials(username, password);
            }

            if config.tls.required {
                let tls_config = build_tls_config(config).await?;
                mqttoptions.set_transport(Transport::tls_with_config(tls_config.into()));
            }

            let (client, eventloop) = AsyncClientV5::new(mqttoptions, queue_capacity);
            (Client::V5(client), EventLoop::V5(Box::new(eventloop)))
        }
        MqttProtocol::V3 => {
            let mut mqttoptions = MqttOptions::new(client_id, host, port);
            mqttoptions
                .set_keep_alive(Duration::from_secs(config.keep_alive_seconds.unwrap_or(20)));
            mqttoptions.set_manual_acks(!config.delayed_ack);
            let default_window: u16 = match config.max_inflight {
                Some(v) => v,
                None => {
                    let capped = std::cmp::min(queue_capacity, u16::MAX as usize);
                    capped as u16
                }
            };
            mqttoptions.set_inflight(default_window);
            mqttoptions.set_clean_session(config.clean_session);

            if let (Some(username), Some(password)) = (&config.username, &config.password) {
                mqttoptions.set_credentials(username, password);
            }

            if config.tls.required {
                let tls_config = build_tls_config(config).await?;
                mqttoptions.set_transport(Transport::tls_with_config(tls_config.into()));
            }

            let (client, eventloop) = AsyncClient::new(mqttoptions, queue_capacity);
            (Client::V3(client), EventLoop::V3(Box::new(eventloop)))
        }
    };

    info!(url = %config.url, "MQTT client created. Eventloop will connect.");
    Ok((client, eventloop))
}

async fn run_eventloop(
    mut eventloop: EventLoop,
    message_tx: Option<Sender<MqttInternalMessage>>,
    mut stop_rx: mpsc::Receiver<()>,
    subscription_info: Option<(Client, String, QoS)>,
    manual_acks: bool,
    is_connected: Arc<AtomicBool>,
    confirmer: Option<Arc<PublishConfirmer>>,
) {
    let mut stopping = false;
    let mut has_seen_connack = false;
    // A future that is always pending until we decide to start the timeout
    let mut flush_timeout: std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> =
        Box::pin(futures::future::pending());

    loop {
        tokio::select! {
            _ = stop_rx.recv(), if !stopping => {
                stopping = true;
                // Run for a bit longer to flush outgoing messages (like ACKs or replies)
                flush_timeout = Box::pin(tokio::time::sleep(Duration::from_millis(100)));
                debug!("MQTT client dropped, flushing event loop for 100ms");
            }
            _ = &mut flush_timeout, if stopping => {
                debug!("MQTT event loop flush complete, exiting");
                break;
            }
            event_result = poll_event(&mut eventloop) => {
                match event_result {
                    Ok(event) => {
                        match event {
                            EventWrapper::V3(rumqttc::Event::Incoming(incoming)) => match incoming {
                                rumqttc::Incoming::Publish(p) => {
                                    if let Some(tx) = &message_tx {
                                        let topic = p.topic.clone();
                                        let msg = publish_to_canonical_message_v3(&p);
                                        let ack = if manual_acks && p.qos != QoS::AtMostOnce { MqttAck::V3(p) } else { MqttAck::None };
                                        let internal = MqttInternalMessage {
                                            msg, ack
                                        };
                                        trace!(message_id = %format!("{:032x}", internal.msg.message_id), %topic, "Received MQTT v3 message");
                                        if tx.send(internal).await.is_err() {
                                            break;
                                        }
                                    }
                                }
                                rumqttc::Incoming::ConnAck(ack) => {
                                    is_connected.store(true, Ordering::Relaxed);
                                    let fail_outstanding = should_fail_outstanding_on_connack(
                                        has_seen_connack,
                                        ack.session_present,
                                    );
                                    has_seen_connack = true;
                                    if !ack.session_present {
                                        // Only a post-startup fresh session implies the broker
                                        // discarded previously in-flight publishes. On the first
                                        // ConnAck, current-session publishes may already be queued
                                        // locally before the handshake completes.
                                        if fail_outstanding {
                                            if let Some(c) = &confirmer {
                                                c.fail_all();
                                            }
                                        }
                                        if let Some((client, topic, qos)) = &subscription_info {
                                            let client = client.clone();
                                            let topic = topic.clone();
                                            let qos = *qos;
                                            info!("Session not present on V3 connection, resubscribing to {}", topic);
                                            tokio::spawn(async move {
                                                if let Err(e) = client.subscribe(&topic, qos).await {
                                                    error!("Failed to resubscribe: {}", e);
                                                }
                                            });
                                        }
                                    } else {
                                        info!("Session present on V3 connection, resuming...");
                                    }
                                }
                                rumqttc::Incoming::PubAck(pa) => {
                                    if let Some(c) = &confirmer {
                                        c.settle(pa.pkid, true); // v3 PUBACK has no reason code
                                    }
                                }
                                rumqttc::Incoming::PubComp(pc) => {
                                    if let Some(c) = &confirmer {
                                        c.settle(pc.pkid, true); // v3 PUBCOMP has no reason code
                                    }
                                }
                                rumqttc::Incoming::Disconnect => {
                                    is_connected.store(false, Ordering::Relaxed);
                                }
                                _ => {}
                            },
                            EventWrapper::V3(rumqttc::Event::Outgoing(Outgoing::Publish(pkid))) => {
                                if let Some(c) = &confirmer {
                                    c.on_outgoing_publish(pkid);
                                }
                            }
                            EventWrapper::V5(event_box) => {
                                match *event_box {
                                    rumqttc::v5::Event::Incoming(rumqttc::v5::Incoming::Publish(p)) => {
                                        if let Some(tx) = &message_tx {
                                            let topic_bytes = p.topic.clone();
                                            let msg = publish_to_canonical_message_v5(&p);
                                            let ack = if manual_acks && p.qos != QoSV5::AtMostOnce { MqttAck::V5(p) } else { MqttAck::None };
                                            let internal = MqttInternalMessage {
                                                msg, ack
                                            };
                                            trace!(message_id = %format!("{:032x}", internal.msg.message_id), topic = %String::from_utf8_lossy(&topic_bytes), "Received MQTT v5 message");
                                            if tx.send(internal).await.is_err() {
                                                break;
                                            }
                                        }
                                    }
                                    rumqttc::v5::Event::Incoming(rumqttc::v5::Incoming::ConnAck(ack)) => {
                                        is_connected.store(true, Ordering::Relaxed);
                                        let fail_outstanding = should_fail_outstanding_on_connack(
                                            has_seen_connack,
                                            ack.session_present,
                                        );
                                        has_seen_connack = true;
                                        if !ack.session_present {
                                            // Only a post-startup fresh session implies the broker
                                            // discarded previously in-flight publishes. On the
                                            // first ConnAck, current-session publishes may already
                                            // be queued locally before the handshake completes.
                                            if fail_outstanding {
                                                if let Some(c) = &confirmer {
                                                    c.fail_all();
                                                }
                                            }
                                            if let Some((client, topic, qos)) = &subscription_info {
                                                let client = client.clone();
                                                let topic = topic.clone();
                                                let qos = *qos;
                                                info!("Session not present on V5 connection, resubscribing to {}", topic);
                                                tokio::spawn(async move {
                                                    if let Err(e) = client.subscribe(&topic, qos).await {
                                                        error!("Failed to resubscribe: {}", e);
                                                    }
                                                });
                                            }
                                        } else {
                                            info!("Session present on V5 connection, resuming...");
                                        }
                                    }
                                    rumqttc::v5::Event::Incoming(rumqttc::v5::Incoming::PubAck(pa)) => {
                                        if let Some(c) = &confirmer {
                                            c.settle(pa.pkid, puback_v5_ok(pa.reason));
                                        }
                                    }
                                    rumqttc::v5::Event::Incoming(rumqttc::v5::Incoming::PubRec(pr)) => {
                                        // A failed PUBREC (e.g. QuotaExceeded) aborts the QoS 2
                                        // handshake: no PUBCOMP follows, so settle it as failed now.
                                        // A successful PUBREC is not terminal — wait for PUBCOMP.
                                        if pubrec_v5_failed(pr.reason) {
                                            if let Some(c) = &confirmer {
                                                c.settle(pr.pkid, false);
                                            }
                                        }
                                    }
                                    rumqttc::v5::Event::Incoming(rumqttc::v5::Incoming::PubComp(pc)) => {
                                        if let Some(c) = &confirmer {
                                            c.settle(pc.pkid, pubcomp_v5_ok(pc.reason));
                                        }
                                    }
                                    rumqttc::v5::Event::Outgoing(Outgoing::Publish(pkid)) => {
                                        if let Some(c) = &confirmer {
                                            c.on_outgoing_publish(pkid);
                                        }
                                    }
                                    rumqttc::v5::Event::Incoming(rumqttc::v5::Incoming::Disconnect(_)) => {
                                        is_connected.store(false, Ordering::Relaxed);
                                    }
                                    _ => {}
                                }
                            }
                            _ => {}
                        }
                    }
                    Err(e) => {
                        is_connected.store(false, Ordering::Relaxed);
                        error!("MQTT EventLoop error: {}. Reconnecting...", e);
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    }
                }
            }
        }
    }
}

async fn poll_event(eventloop: &mut EventLoop) -> anyhow::Result<EventWrapper> {
    match eventloop {
        EventLoop::V3(el) => el.poll().await.map(EventWrapper::V3).map_err(|e| e.into()),
        EventLoop::V5(el) => el
            .poll()
            .await
            .map(|e| EventWrapper::V5(Box::new(e)))
            .map_err(|e| e.into()),
    }
}

fn publish_to_canonical_message_v5(p: &PublishV5) -> CanonicalMessage {
    let mut canonical_message = CanonicalMessage::new(p.payload.to_vec(), None);

    if let Some(props) = &p.properties {
        let mut metadata = std::collections::HashMap::new();
        for (key, value) in &props.user_properties {
            if key == "mq_bridge.message_id" {
                if let Ok(id) = u128::from_str_radix(value, 16) {
                    canonical_message.message_id = id;
                }
            }
            metadata.insert(key.clone(), value.clone());
        }
        if let Some(rt) = &props.response_topic {
            metadata.insert("reply_to".to_string(), rt.clone());
        }
        if let Some(cd) = &props.correlation_data {
            metadata.insert(
                "correlation_id".to_string(),
                String::from_utf8_lossy(cd).into_owned(),
            );
        }

        if !metadata.is_empty() {
            canonical_message.metadata = metadata;
        }
    }
    canonical_message
}

fn publish_to_canonical_message_v3(p: &rumqttc::Publish) -> CanonicalMessage {
    if let Ok(msg) = serde_json::from_slice::<CanonicalMessage>(&p.payload) {
        return msg;
    }
    CanonicalMessage::new(p.payload.to_vec(), None)
}

/// Sanitizes a string to be used as part of an MQTT client ID.
/// Replaces non-alphanumeric characters with a hyphen.
fn sanitize_for_client_id(input: &str) -> String {
    input
        .chars()
        .map(|c| if c.is_alphanumeric() { c } else { '-' })
        .collect()
}

async fn build_tls_config(config: &MqttConfig) -> anyhow::Result<rustls::ClientConfig> {
    let mut root_cert_store = rustls::RootCertStore::empty();
    if let Some(ca_file) = &config.tls.ca_file {
        let mut ca_buf = std::io::BufReader::new(std::fs::File::open(ca_file)?);
        let certs = rustls_pemfile::certs(&mut ca_buf).collect::<Result<Vec<_>, _>>()?;
        for cert in certs {
            root_cert_store.add(cert)?;
        }
    }

    let client_config_builder =
        rustls::ClientConfig::builder_with_provider(crate::endpoints::get_crypto_provider()?)
            .with_safe_default_protocol_versions()?
            .with_root_certificates(root_cert_store);

    let mut client_config = if config.tls.is_mtls_client_configured() {
        let cert_file = config.tls.cert_file.as_ref().unwrap();
        let key_file = config.tls.key_file.as_ref().unwrap();
        let cert_chain = load_certs(cert_file)?;
        let key_der = load_private_key(key_file)?;
        client_config_builder.with_client_auth_cert(cert_chain, key_der)?
    } else {
        client_config_builder.with_no_client_auth()
    };

    if config.tls.accept_invalid_certs {
        warn!("MQTT TLS is configured to accept invalid certificates. This is insecure and should not be used in production.");
        let mut dangerous_config = client_config.dangerous();
        let schemes = crate::endpoints::get_crypto_provider()?
            .signature_verification_algorithms
            .supported_schemes();
        let verifier = NoopServerCertVerifier {
            supported_schemes: schemes,
        };
        dangerous_config.set_certificate_verifier(Arc::new(verifier));
    }
    Ok(client_config)
}

fn load_certs(path: &str) -> anyhow::Result<Vec<rustls::pki_types::CertificateDer<'static>>> {
    let mut cert_buf = std::io::BufReader::new(std::fs::File::open(path)?);
    Ok(rustls_pemfile::certs(&mut cert_buf).collect::<Result<Vec<_>, _>>()?)
}

fn load_private_key(path: &str) -> anyhow::Result<rustls::pki_types::PrivateKeyDer<'static>> {
    let mut key_buf = std::io::BufReader::new(std::fs::File::open(path)?);
    let key = rustls_pemfile::private_key(&mut key_buf)?
        .ok_or_else(|| anyhow!("No private key found in {}", path))?;
    Ok(key)
}

/// A rustls certificate verifier that does not perform any validation.
#[derive(Debug)]
struct NoopServerCertVerifier {
    supported_schemes: Vec<rustls::SignatureScheme>,
}

impl rustls::client::danger::ServerCertVerifier for NoopServerCertVerifier {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls::pki_types::CertificateDer<'_>,
        _intermediates: &[rustls::pki_types::CertificateDer<'_>],
        _server_name: &rustls::pki_types::ServerName<'_>,
        _ocsp_response: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        self.supported_schemes.clone()
    }
}

fn parse_url(url: &str) -> anyhow::Result<(String, u16)> {
    let url = url::Url::parse(url)?;
    let host = url
        .host_str()
        .ok_or_else(|| anyhow!("No host in URL"))?
        .to_string();
    // Prefer IPv4 localhost to avoid macOS resolving `localhost` to ::1
    // which can bypass Docker Desktop port forwarding in some setups.
    let host = if host == "localhost" {
        "127.0.0.1".to_string()
    } else {
        host
    };
    let port = url
        .port()
        .unwrap_or(if url.scheme() == "mqtts" || url.scheme() == "ssl" {
            8883
        } else {
            1883
        });
    Ok((host, port))
}

fn parse_qos(qos: u8) -> QoS {
    match qos {
        0 => QoS::AtMostOnce,
        1 => QoS::AtLeastOnce,
        2 => QoS::ExactlyOnce,
        _ => QoS::AtLeastOnce,
    }
}

#[cfg(test)]
mod tests {
    use super::{should_fail_outstanding_on_connack, PublishConfirmer};

    #[tokio::test]
    async fn initial_connack_without_session_does_not_fail_current_publish() {
        let confirmer = PublishConfirmer::default();
        let rx = confirmer.register();

        let mut has_seen_connack = false;
        let fail_outstanding = should_fail_outstanding_on_connack(has_seen_connack, false);
        has_seen_connack = true;
        if fail_outstanding {
            confirmer.fail_all();
        }

        confirmer.on_outgoing_publish(42);
        confirmer.settle(42, true);

        assert!(has_seen_connack);
        assert!(rx.await.unwrap());
    }

    #[tokio::test]
    async fn later_fresh_session_fails_outstanding_publish() {
        let confirmer = PublishConfirmer::default();
        let rx = confirmer.register();

        let fail_outstanding = should_fail_outstanding_on_connack(true, false);
        assert!(fail_outstanding);
        if fail_outstanding {
            confirmer.fail_all();
        }

        assert!(!rx.await.unwrap());
    }
}
