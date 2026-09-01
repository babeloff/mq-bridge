use super::*;
use crate::models::DirSpoolConfig;
use crate::traits::{MessageConsumer, MessagePublisher};
use tempfile::tempdir;

fn config(path: &std::path::Path) -> DirSpoolConfig {
    DirSpoolConfig::new(path.to_str().unwrap())
}

fn message(payload: &str, kind: &str) -> CanonicalMessage {
    let mut msg = CanonicalMessage::from_vec(payload.as_bytes().to_vec());
    msg.metadata.insert("kind".to_string(), kind.to_string());
    msg
}

/// Drains the consumer until it reports an empty batch, acking everything it hands out.
async fn drain(consumer: &mut DirSpoolConsumer, max: usize) -> Vec<CanonicalMessage> {
    let mut collected = Vec::new();
    loop {
        let batch = consumer.receive_batch(max).await.unwrap();
        if batch.messages.is_empty() {
            return collected;
        }
        let acks = vec![MessageDisposition::Ack; batch.messages.len()];
        collected.extend(batch.messages);
        (batch.commit)(acks).await.unwrap();
    }
}

#[test]
fn render_name_supports_the_documented_placeholders() {
    assert_eq!(render_name("{seq}", 7, 0), "7");
    assert_eq!(render_name("{seq:06}", 7, 0), "000007");
    // The printf spelling from the feature request.
    assert_eq!(render_name("{seq:06d}", 7, 0), "000007");
    assert_eq!(render_name("chunk-{seq:03}", 42, 0), "chunk-042");
    // An unknown placeholder is copied through rather than dropped, so a typo is
    // visible in the file name instead of silently collapsing every chunk onto one.
    assert_eq!(render_name("{nope}-{seq:02}", 1, 0), "{nope}-01");
    assert!(render_name("{seq:04}_{timestamp}", 3, 0).starts_with("0003_"));
}

/// Pins the spelling documented in docs/CONFIGURATION.md, including the defaults an
/// omitted field falls back to.
#[test]
fn parses_the_documented_yaml_shape() {
    use crate::models::{Endpoint, EndpointType};

    let endpoint: Endpoint = serde_yaml_ng::from_str(
        r#"
dir_spool:
  path: "/tmp/video_telemetry_spool"
  naming_pattern: "{seq:06d}_{timestamp}"
  payload_extension: ".h264"
  metadata_extension: ".json"
  atomic: true
  emit_done: true
"#,
    )
    .expect("the documented publisher config must parse");
    let EndpointType::DirSpool(cfg) = endpoint.endpoint_type else {
        panic!("expected a dir_spool endpoint");
    };
    assert_eq!(cfg.path, "/tmp/video_telemetry_spool");
    assert_eq!(cfg.naming_pattern, "{seq:06d}_{timestamp}");
    // A leading dot is accepted and stripped, so `.h264` and `h264` mean the same thing.
    assert_eq!(cfg.payload_suffix(), "h264");
    assert_eq!(cfg.metadata_suffix(), Some("json"));
    assert!(cfg.emit_done);
    // Consumer-side fields keep their defaults on a publisher block.
    assert!(cfg.drain_on_read);
    assert!(!cfg.stop_on_done);
    assert_eq!(cfg.done_file, "DONE");
    assert_eq!(cfg.poll_interval_ms, 100);

    let endpoint: Endpoint = serde_yaml_ng::from_str(
        r#"
dir_spool:
  path: "/tmp/video_telemetry_spool"
  payload_extension: ".h264"
  drain_on_read: true
  stop_on_done: true
"#,
    )
    .expect("the documented consumer config must parse");
    let EndpointType::DirSpool(cfg) = endpoint.endpoint_type else {
        panic!("expected a dir_spool endpoint");
    };
    assert!(cfg.stop_on_done);
    assert_eq!(cfg.naming_pattern, "{seq:09}");
}

/// `spool` is the short spelling the feature request used; both must reach the same type.
#[test]
fn accepts_the_spool_alias() {
    use crate::models::{Endpoint, EndpointType};

    let endpoint: Endpoint =
        serde_yaml_ng::from_str("spool:\n  path: \"/tmp/s\"\n").expect("alias must parse");
    assert!(matches!(endpoint.endpoint_type, EndpointType::DirSpool(_)));
}

/// A misspelled field must name itself rather than fall through to the custom-endpoint
/// path, which would report an opaque unknown-endpoint failure instead.
#[test]
fn rejects_an_unknown_field() {
    use crate::models::Endpoint;

    let error =
        serde_yaml_ng::from_str::<Endpoint>("dir_spool:\n  path: \"/tmp/s\"\n  drain: true\n")
            .expect_err("an unknown field must be rejected");
    assert!(
        error.to_string().contains("drain"),
        "the error should name the offending field, got: {error}"
    );
}

#[test]
fn leading_sequence_reads_the_padded_prefix() {
    assert_eq!(leading_sequence("000012_1700000000000"), Some(12));
    assert_eq!(leading_sequence("000012"), Some(12));
    assert_eq!(leading_sequence("chunk-1"), None);
}

#[tokio::test]
async fn roundtrips_payload_and_metadata_through_the_spool() {
    let dir = tempdir().unwrap();
    let cfg = config(dir.path());

    let publisher = DirSpoolPublisher::new(&cfg).await.unwrap();
    publisher
        .send_batch(vec![
            message("frame-one", "video"),
            message("frame-two", "video"),
        ])
        .await
        .unwrap();

    let mut consumer = DirSpoolConsumer::new(&cfg).await.unwrap();
    let received = drain(&mut consumer, 10).await;

    assert_eq!(received.len(), 2);
    assert_eq!(&received[0].payload[..], b"frame-one");
    assert_eq!(&received[1].payload[..], b"frame-two");
    assert_eq!(received[0].metadata.get("kind").unwrap(), "video");
    // `drain_on_read` defaults to true, so an acked chunk leaves nothing behind.
    assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 0);
}

#[tokio::test]
async fn preserves_binary_payloads_byte_for_byte() {
    let dir = tempdir().unwrap();
    let cfg = config(dir.path());
    // Invalid UTF-8 plus embedded NULs and newlines: the delimiter framing the `file`
    // endpoint uses would corrupt this, which is why the spool writes whole files.
    let blob: Vec<u8> = vec![0x00, 0xff, b'\n', 0xfe, b'\r', 0x80, 0x00];

    let publisher = DirSpoolPublisher::new(&cfg).await.unwrap();
    publisher
        .send_batch(vec![CanonicalMessage::from_vec(blob.clone())])
        .await
        .unwrap();

    let mut consumer = DirSpoolConsumer::new(&cfg).await.unwrap();
    let received = drain(&mut consumer, 10).await;
    assert_eq!(received.len(), 1);
    assert_eq!(&received[0].payload[..], &blob[..]);
}

#[tokio::test]
async fn delivers_chunks_in_sequence_order_across_batches() {
    let dir = tempdir().unwrap();
    let cfg = config(dir.path());

    let publisher = DirSpoolPublisher::new(&cfg).await.unwrap();
    for index in 0..12 {
        publisher
            .send_batch(vec![message(&format!("chunk-{index}"), "seq")])
            .await
            .unwrap();
    }

    let mut consumer = DirSpoolConsumer::new(&cfg).await.unwrap();
    // A batch size below the queue depth forces several polls, which is where a naive
    // lexical sort over unpadded names would reorder 10 ahead of 2.
    let received = drain(&mut consumer, 5).await;
    let payloads: Vec<String> = received
        .iter()
        .map(|m| String::from_utf8(m.payload.to_vec()).unwrap())
        .collect();
    let expected: Vec<String> = (0..12).map(|index| format!("chunk-{index}")).collect();
    assert_eq!(payloads, expected);
}

#[tokio::test]
async fn resumes_the_sequence_after_a_publisher_restart() {
    let dir = tempdir().unwrap();
    let cfg = config(dir.path());

    let first = DirSpoolPublisher::new(&cfg).await.unwrap();
    first.send_batch(vec![message("a", "x")]).await.unwrap();
    first.send_batch(vec![message("b", "x")]).await.unwrap();
    drop(first);

    // A fresh publisher over the same directory must append, not overwrite chunk 0.
    let second = DirSpoolPublisher::new(&cfg).await.unwrap();
    second.send_batch(vec![message("c", "x")]).await.unwrap();
    drop(second);

    let mut consumer = DirSpoolConsumer::new(&cfg).await.unwrap();
    let received = drain(&mut consumer, 10).await;
    let payloads: Vec<String> = received
        .iter()
        .map(|m| String::from_utf8(m.payload.to_vec()).unwrap())
        .collect();
    assert_eq!(payloads, vec!["a", "b", "c"]);
}

#[tokio::test]
async fn never_hands_out_a_partially_written_chunk() {
    let dir = tempdir().unwrap();
    let cfg = config(dir.path());

    // Stand in for a writer that died mid-chunk: the staging file is present but was
    // never renamed into place.
    std::fs::write(dir.path().join("000000.bin.tmp"), b"half a frame").unwrap();
    let publisher = DirSpoolPublisher::new(&cfg).await.unwrap();
    publisher
        .send_batch(vec![message("whole", "x")])
        .await
        .unwrap();

    let mut consumer = DirSpoolConsumer::new(&cfg).await.unwrap();
    let received = drain(&mut consumer, 10).await;
    assert_eq!(received.len(), 1);
    assert_eq!(&received[0].payload[..], b"whole");
    // The debris is still there — it is the operator's to inspect, not ours to delete.
    assert!(dir.path().join("000000.bin.tmp").exists());
}

#[tokio::test]
async fn ends_the_stream_once_the_queue_is_empty_and_done_is_present() {
    let dir = tempdir().unwrap();
    let mut cfg = config(dir.path());
    cfg.emit_done = true;
    cfg.stop_on_done = true;

    let publisher = DirSpoolPublisher::new(&cfg).await.unwrap();
    publisher
        .send_batch(vec![message("last", "x")])
        .await
        .unwrap();
    // The route calls this hook when the publisher goes away.
    publisher.on_disconnect_hook().unwrap().await.unwrap();
    assert!(dir.path().join("DONE").exists());

    let mut consumer = DirSpoolConsumer::new(&cfg).await.unwrap();
    // The backlog is drained first: the sentinel does not cut the queue short.
    let batch = consumer.receive_batch(10).await.unwrap();
    assert_eq!(batch.messages.len(), 1);
    (batch.commit)(vec![MessageDisposition::Ack]).await.unwrap();

    match consumer.receive_batch(10).await {
        Err(ConsumerError::EndOfStream) => {}
        other => panic!("expected EndOfStream once the queue drained, got {other:?}"),
    }
}

#[tokio::test]
async fn keeps_tailing_when_stop_on_done_is_off() {
    let dir = tempdir().unwrap();
    let mut cfg = config(dir.path());
    cfg.poll_interval_ms = 1;
    std::fs::write(dir.path().join("DONE"), b"").unwrap();

    let mut consumer = DirSpoolConsumer::new(&cfg).await.unwrap();
    // An empty batch, not an end of stream: the default is a directory tail.
    let batch = consumer.receive_batch(10).await.unwrap();
    assert!(batch.messages.is_empty());
}

#[tokio::test]
async fn a_nacked_chunk_stays_on_disk_and_is_redelivered() {
    let dir = tempdir().unwrap();
    let mut cfg = config(dir.path());
    cfg.poll_interval_ms = 1;

    let publisher = DirSpoolPublisher::new(&cfg).await.unwrap();
    publisher
        .send_batch(vec![message("retry-me", "x")])
        .await
        .unwrap();

    let mut consumer = DirSpoolConsumer::new(&cfg).await.unwrap();
    let batch = consumer.receive_batch(10).await.unwrap();
    assert_eq!(batch.messages.len(), 1);
    (batch.commit)(vec![MessageDisposition::Nack])
        .await
        .unwrap();
    assert!(dir.path().join("000000000.bin").exists());

    let again = consumer.receive_batch(10).await.unwrap();
    assert_eq!(again.messages.len(), 1);
    assert_eq!(&again.messages[0].payload[..], b"retry-me");
}

#[tokio::test]
async fn without_drain_on_read_each_chunk_is_delivered_once_and_kept() {
    let dir = tempdir().unwrap();
    let mut cfg = config(dir.path());
    cfg.drain_on_read = false;
    cfg.poll_interval_ms = 1;

    let publisher = DirSpoolPublisher::new(&cfg).await.unwrap();
    publisher
        .send_batch(vec![message("archive", "x")])
        .await
        .unwrap();

    let mut consumer = DirSpoolConsumer::new(&cfg).await.unwrap();
    let received = drain(&mut consumer, 10).await;
    assert_eq!(received.len(), 1);
    // Read but not consumed: the files survive for a second reader or an operator.
    assert!(dir.path().join("000000000.bin").exists());
    assert!(dir.path().join("000000000.json").exists());
}

#[tokio::test]
async fn reads_a_bare_payload_file_written_without_a_sidecar() {
    let dir = tempdir().unwrap();
    let cfg = config(dir.path());
    // What a foreign producer (a Python script, say) would leave behind.
    std::fs::write(dir.path().join("000001.bin"), b"foreign").unwrap();

    let mut consumer = DirSpoolConsumer::new(&cfg).await.unwrap();
    let received = drain(&mut consumer, 10).await;
    assert_eq!(received.len(), 1);
    assert_eq!(&received[0].payload[..], b"foreign");
    assert!(received[0].metadata.is_empty());
}

#[tokio::test]
async fn stamps_source_metadata_when_asked() {
    let dir = tempdir().unwrap();
    let mut cfg = config(dir.path());
    cfg.source_metadata = true;

    let publisher = DirSpoolPublisher::new(&cfg).await.unwrap();
    publisher
        .send_batch(vec![message("traced", "x")])
        .await
        .unwrap();

    let mut consumer = DirSpoolConsumer::new(&cfg).await.unwrap();
    let received = drain(&mut consumer, 10).await;
    assert_eq!(
        received[0].metadata.get(SRC_CHUNK_KEY).unwrap(),
        "000000000"
    );
    assert_eq!(
        received[0].metadata.get(SRC_PATH_KEY).unwrap(),
        dir.path().to_str().unwrap()
    );
}

#[tokio::test]
async fn honours_custom_extensions_and_a_disabled_sidecar() {
    let dir = tempdir().unwrap();
    let mut cfg = config(dir.path());
    cfg.payload_extension = ".h264".to_string();
    cfg.metadata_extension = String::new();
    cfg.naming_pattern = "{seq:06d}".to_string();

    let publisher = DirSpoolPublisher::new(&cfg).await.unwrap();
    publisher
        .send_batch(vec![message("nal-unit", "video")])
        .await
        .unwrap();
    assert!(dir.path().join("000000.h264").exists());
    assert!(!dir.path().join("000000.json").exists());

    let mut consumer = DirSpoolConsumer::new(&cfg).await.unwrap();
    let received = drain(&mut consumer, 10).await;
    assert_eq!(received.len(), 1);
    assert_eq!(&received[0].payload[..], b"nal-unit");
    // No sidecar means no metadata to restore.
    assert!(received[0].metadata.is_empty());
}

#[tokio::test]
async fn rejects_a_payload_and_sidecar_sharing_one_extension() {
    let dir = tempdir().unwrap();
    let mut cfg = config(dir.path());
    cfg.payload_extension = "json".to_string();
    cfg.metadata_extension = ".json".to_string();

    let error = DirSpoolPublisher::new(&cfg).await.unwrap_err().to_string();
    assert!(error.contains("must differ"), "unexpected error: {error}");
}

/// The whole point of the endpoint: a producer that runs to completion and exits, and a
/// consumer that starts afterwards and still gets every chunk, in order.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_route_drains_a_spool_its_producer_has_already_finished_writing() {
    use crate::models::{Endpoint, EndpointType, FileConfig, FileFormat};
    use crate::route::Route;

    let dir = tempdir().unwrap();
    let spool = dir.path().join("spool");
    let sink = dir.path().join("out.jsonl");

    // Producer: fill the spool and mark it finished, then go away entirely.
    let mut producer_cfg = DirSpoolConfig::new(spool.to_str().unwrap());
    producer_cfg.emit_done = true;
    let producer = DirSpoolPublisher::new(&producer_cfg).await.unwrap();
    for index in 0..20 {
        producer
            .send_batch(vec![message(&format!("{index}"), "frame")])
            .await
            .unwrap();
    }
    producer.on_disconnect_hook().unwrap().await.unwrap();
    drop(producer);

    let mut consumer_cfg = DirSpoolConfig::new(spool.to_str().unwrap());
    consumer_cfg.stop_on_done = true;
    let route = Route::new(
        Endpoint::new(EndpointType::DirSpool(consumer_cfg)),
        Endpoint::new(EndpointType::File(FileConfig {
            path: sink.to_str().unwrap().to_string(),
            format: FileFormat::Raw,
            ..Default::default()
        })),
    )
    .with_concurrency(4)
    .with_batch_size(3);

    tokio::time::timeout(
        Duration::from_secs(10),
        route.run_until_err("spool_drain", None, None),
    )
    .await
    .expect("the route should end once DONE is reached and the queue is empty")
    .expect("the route should complete without errors");

    let written = tokio::fs::read_to_string(&sink).await.unwrap();
    let lines: Vec<&str> = written.lines().collect();
    let expected: Vec<String> = (0..20).map(|index| index.to_string()).collect();
    assert_eq!(lines, expected, "the spool must drain in queue order");
    // Every chunk was acked, so the queue is empty; only the sentinel is left.
    let leftovers: Vec<String> = std::fs::read_dir(&spool)
        .unwrap()
        .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
        .collect();
    assert_eq!(leftovers, vec!["DONE".to_string()]);
}

#[tokio::test]
async fn drains_immediately_under_exit_on_empty() {
    let dir = tempdir().unwrap();
    let mut cfg = config(dir.path());
    // Long enough that a poll-interval sleep would dominate the test's runtime.
    cfg.poll_interval_ms = 5_000;

    let mut consumer = DirSpoolConsumer::new(&cfg).await.unwrap();
    consumer.set_exit_on_empty(true);
    let started = std::time::Instant::now();
    let batch = consumer.receive_batch(10).await.unwrap();
    assert!(batch.messages.is_empty());
    assert!(
        started.elapsed() < Duration::from_secs(1),
        "a drain must not wait out the idle poll interval"
    );
}
