#![allow(dead_code)]
use std::sync::Arc;

use super::assert_permanent_consumer_error;
use mq_bridge::endpoints::nats::{NatsConsumer, NatsPublisher};
use mq_bridge::test_utils::{
    add_performance_result, run_chaos_pipeline_test, run_direct_perf_test,
    run_performance_pipeline_test, run_pipeline_test, run_test_with_docker,
    run_test_with_docker_controller, setup_logging, verify_subscriber_logic,
    PERF_TEST_MESSAGE_COUNT,
};
const CONFIG_YAML: &str = r#"
routes:
  memory_to_nats:
    concurrency: 4
    batch_size: 512
    input:
      memory: { topic: "test-in-nats" }
    output:
      middlewares:
        - retry:
            max_attempts: 20
            initial_interval_ms: 500
            max_interval_ms: 2000
      nats: { url: "nats://localhost:4222", subject: "test-stream.pipeline", stream: "test-stream" }

  nats_to_memory:
    concurrency: 4
    batch_size: 512
    input:
      nats: { url: "nats://localhost:4222", subject: "test-stream.pipeline", stream: "test-stream" }
    output:
      memory: { topic: "test-out-nats", capacity: {out_capacity} }
"#;

pub async fn test_nats_pipeline() {
    setup_logging();
    run_test_with_docker("tests/integration/docker-compose/nats.yml", || async {
        let config_yaml = CONFIG_YAML.replace(
            "{out_capacity}",
            &(PERF_TEST_MESSAGE_COUNT + 1000).to_string(),
        ); // Use a small capacity for non-perf test
        run_pipeline_test("nats", &config_yaml).await;
    })
    .await;
}

pub async fn test_nats_chaos() {
    setup_logging();
    run_test_with_docker_controller(
        "tests/integration/docker-compose/nats.yml",
        |controller| async move {
            let config_yaml = CONFIG_YAML.replace(
                "{out_capacity}",
                &(PERF_TEST_MESSAGE_COUNT + 1000).to_string(),
            );
            run_chaos_pipeline_test("nats", &config_yaml, controller, "nats").await;
        },
    )
    .await;
}

pub async fn test_nats_subscriber_logic() {
    setup_logging();
    run_test_with_docker("tests/integration/docker-compose/nats.yml", || async {
        let stream_name = format!("sub_logic_stream_{}", fast_uuid_v7::gen_id());
        let subject = format!("{}.sub_logic_{}", stream_name, fast_uuid_v7::gen_id());
        let config = mq_bridge::models::NatsConfig {
            url: "nats://localhost:4222".to_string(),
            subject: Some(subject),
            stream: Some(stream_name),
            subscriber_mode: true,
            ..Default::default()
        };

        let publisher = Arc::new(NatsPublisher::new(&config).await.unwrap());
        let sub1 = Arc::new(tokio::sync::Mutex::new(
            NatsConsumer::new(&config).await.unwrap(),
        ));
        let sub2 = Arc::new(tokio::sync::Mutex::new(
            NatsConsumer::new(&config).await.unwrap(),
        ));
        // Give subscribers time to connect and finish the subscription
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        verify_subscriber_logic(publisher, sub1, sub2).await;
    })
    .await;
}

pub async fn test_nats_performance_pipeline() {
    setup_logging();
    run_test_with_docker("tests/integration/docker-compose/nats.yml", || async {
        let config_yaml = CONFIG_YAML.replace(
            "{out_capacity}",
            &(PERF_TEST_MESSAGE_COUNT + 1000).to_string(),
        );
        run_performance_pipeline_test("nats", &config_yaml, PERF_TEST_MESSAGE_COUNT).await;
    })
    .await;
}

pub async fn test_nats_performance_direct() {
    setup_logging();
    run_test_with_docker("tests/integration/docker-compose/nats.yml", || async {
        let stream_name = "perf_nats_direct";
        let subject = "perf_nats_direct.subject";
        let config = mq_bridge::models::NatsConfig {
            url: "nats://localhost:4222".to_string(),
            ..Default::default()
        };

        let result = run_direct_perf_test(
            "NATS",
            || async {
                let mut pub_config = config.clone();
                pub_config.subject = Some(subject.to_string());
                pub_config.stream = Some(stream_name.to_string());
                Arc::new(NatsPublisher::new(&pub_config).await.unwrap())
            },
            || async {
                let mut endpoint = config.clone();
                endpoint.subject = Some(subject.to_string());
                endpoint.stream = Some(stream_name.to_string());
                Arc::new(tokio::sync::Mutex::new(
                    NatsConsumer::new(&endpoint).await.unwrap(),
                ))
            },
        )
        .await;

        add_performance_result(result);
    })
    .await;
}

pub async fn test_nats_status() {
    use mq_bridge::traits::{MessageConsumer, MessagePublisher};
    use tokio::time::{sleep, Duration};

    setup_logging();
    run_test_with_docker_controller(
        "tests/integration/docker-compose/nats.yml",
        |controller| async move {
            let stream_name = "status_nats_direct";
            let subject = "status_nats_direct.subject";
            let config = mq_bridge::models::NatsConfig {
                url: "nats://localhost:4222".to_string(),
                ..Default::default()
            };

            let mut pub_config = config.clone();
            pub_config.subject = Some(subject.to_string());
            pub_config.stream = Some(stream_name.to_string());
            let publisher = NatsPublisher::new(&pub_config).await.unwrap();

            let mut consumer_config = config.clone();
            consumer_config.subject = Some(subject.to_string());
            consumer_config.stream = Some(stream_name.to_string());
            let consumer = NatsConsumer::new(&consumer_config).await.unwrap();

            println!("[NATS] Checking initial status...");
            sleep(Duration::from_secs(2)).await;
            let pub_status = publisher.status().await;
            let con_status = consumer.status().await;
            assert!(
                pub_status.healthy,
                "Publisher should be healthy initially. Status: {:?}",
                pub_status
            );
            assert!(
                con_status.healthy,
                "Consumer should be healthy initially. Status: {:?}",
                con_status
            );
            println!("[NATS] Initial status check OK.");

            controller.stop_service("nats");
            println!("[NATS] Service 'nats' stopped. Waiting for disconnect detection...");

            let start = std::time::Instant::now();
            loop {
                if !publisher.status().await.healthy && !consumer.status().await.healthy {
                    println!("[NATS] Disconnect detected.");
                    break;
                }
                if start.elapsed() > Duration::from_secs(20) {
                    panic!("[NATS] Timeout waiting for disconnect.");
                }
                sleep(Duration::from_secs(1)).await;
            }

            controller.start_service("nats");
            println!("[NATS] Service 'nats' started. Waiting for reconnect...");

            let start = std::time::Instant::now();
            loop {
                if publisher.status().await.healthy && consumer.status().await.healthy {
                    println!("[NATS] Reconnect detected.");
                    break;
                }
                if start.elapsed() > Duration::from_secs(20) {
                    panic!("[NATS] Timeout waiting for reconnect.");
                }
                sleep(Duration::from_secs(1)).await;
            }
            println!("[NATS] Status test successful.");
        },
    )
    .await;
}

// Issue 1 (regression): reading FROM a NATS JetStream with `exit_on_empty` must drain
// every existing message and then terminate. Before the fix the consumer blocked on
// the JetStream message stream forever, so `--drain` never fired and a broker batch
// job hung after receiving all existing messages.
pub async fn test_nats_drain_exits_on_empty() {
    use mq_bridge::models::{Endpoint, EndpointType, NatsConfig, NatsDeliverPolicy};
    use mq_bridge::route::RouteOutcome;
    use mq_bridge::traits::{MessageDisposition, MessagePublisher};
    use mq_bridge::{CanonicalMessage, Route};

    setup_logging();
    run_test_with_docker("tests/integration/docker-compose/nats.yml", || async {
        const N: usize = 300;
        let stream = format!("drain_stream_{}", fast_uuid_v7::gen_id());
        let subject = format!("{stream}.data");

        // Publish N messages into the stream.
        let pub_config = NatsConfig {
            url: "nats://localhost:4222".to_string(),
            subject: Some(subject.clone()),
            stream: Some(stream.clone()),
            ..Default::default()
        };
        let publisher = NatsPublisher::new(&pub_config).await.unwrap();
        let batch: Vec<_> = (0..N)
            .map(|i| CanonicalMessage::new(format!("msg-{i}").into_bytes(), None))
            .collect();
        publisher.send_batch(batch).await.unwrap();

        // Read them all (deliver_policy = all), draining to memory.
        let in_config = NatsConfig {
            url: "nats://localhost:4222".to_string(),
            subject: Some(subject.clone()),
            stream: Some(stream.clone()),
            deliver_policy: Some(NatsDeliverPolicy::All),
            ..Default::default()
        };
        let out_topic = format!("drain_nats_out_{}", fast_uuid_v7::gen_id());
        let input = Endpoint::new(EndpointType::Nats(in_config));
        let output = Endpoint::new_memory(&out_topic, N + 100);
        let route = Route::new(input, output)
            .with_batch_size(128)
            .with_exit_on_empty(true);

        let mut verifier = route
            .connect_to_output("nats_drain_verifier")
            .await
            .unwrap();
        let collector = tokio::spawn(async move {
            let mut received = 0usize;
            while received < N {
                let item =
                    tokio::time::timeout(std::time::Duration::from_secs(15), verifier.receive())
                        .await
                        .expect("timed out draining output")
                        .expect("output stream closed early");
                received += 1;
                (item.commit)(MessageDisposition::Ack).await.unwrap();
            }
            received
        });

        let handle = route.run("nats_drain_test").await.unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(30), async {
            while handle.outcome().is_none() {
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("nats route did not exit on empty after draining");
        assert_eq!(handle.outcome(), Some(RouteOutcome::Completed));
        handle.join().await.expect("route task panicked");

        let received = tokio::time::timeout(std::time::Duration::from_secs(5), collector)
            .await
            .expect("timed out collecting output")
            .expect("collector task panicked");
        assert_eq!(received, N);
        println!("[NATS] Drain exit_on_empty test successful.");
    })
    .await;
}

/// A route whose `stream`/`subject` pair conflicts with an existing stream can never
/// succeed: every reconnect rebuilds the identical mismatch. It must terminate the route with
/// a permanent error instead of looping on the reconnect interval forever.
///
/// The reproducible form is a second stream claiming a subject the first already owns
/// (JetStream error 10065). Note that a consumer whose `filter_subject` merely falls outside
/// its stream's subjects is *accepted* by the server — it just never delivers — so that
/// variant is silent starvation rather than an error loop.
pub async fn test_nats_subject_stream_mismatch_fails_fast() {
    use mq_bridge::models::{Endpoint, EndpointType, NatsConfig};
    use mq_bridge::Route;

    setup_logging();
    run_test_with_docker("tests/integration/docker-compose/nats.yml", || async {
        let stream = format!("mismatch_stream_{}", fast_uuid_v7::gen_id());

        // Bind the stream to exactly `<stream>.data`. A consumer creates the stream with its
        // own subject verbatim (the publisher would instead register a `<stream>.>` wildcard,
        // which covers everything and cannot mismatch).
        let seed = NatsConsumer::new(&NatsConfig {
            url: "nats://localhost:4222".to_string(),
            subject: Some(format!("{stream}.data")),
            stream: Some(stream.clone()),
            ..Default::default()
        })
        .await
        .expect("create the stream via a matching consumer");
        drop(seed);

        // Ask for a *different* stream carrying the same subject. NATS refuses to create it
        // ("subjects overlap with an existing stream"), and no retry can change that.
        let in_config = NatsConfig {
            url: "nats://localhost:4222".to_string(),
            subject: Some(format!("{stream}.data")),
            stream: Some(format!("{stream}_other")),
            ..Default::default()
        };
        let route_name = format!("nats_mismatch_{}", fast_uuid_v7::gen_id());
        let input = Endpoint::new(EndpointType::Nats(in_config));
        let output = Endpoint::new_memory(&route_name, 16);
        let route = Route::new(input, output);

        assert_permanent_consumer_error(route, &route_name, "subject/stream mismatch").await;
    })
    .await;
}
