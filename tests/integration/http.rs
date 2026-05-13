#![allow(dead_code, unused)]

use mq_bridge::models::{Endpoint, EndpointType, HttpConfig, Route};
use mq_bridge::test_utils::{setup_logging, PERF_TEST_MESSAGE_COUNT};
use serde_yaml_ng;
use std::collections::HashMap;
use std::time::{Duration, Instant};

const CONFIG_YAML: &str = r#"
routes:
  memory_to_http:
    concurrency: 4
    batch_size: 128
    input:
      memory: { topic: "test-in-http" }
    output:
      http:
        url: "http://127.0.0.1:{out_port}"
        request_timeout_ms: 5000
        batch_concurrency: 4

  http_to_memory:
    concurrency: 4
    batch_size: 128
    input:
      http:
        url: "127.0.0.1:{out_port}"
        internal_buffer_size: {buffer_size}
        fire_and_forget: false
    output:
      memory: { topic: "test-out-http", capacity: {out_capacity} }
"#;

fn get_free_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().port()
}

async fn wait_for_server_ready(addr: &str, timeout: Duration) -> bool {
    let start = Instant::now();
    while start.elapsed() < timeout {
        if tokio::net::TcpStream::connect(addr).await.is_ok() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    false
}

pub async fn test_http_performance_pipeline() {
    setup_logging();
    tokio::time::timeout(Duration::from_secs(60), async {
        let in_route_name = "memory_to_http".to_string();
        let out_route_name = "http_to_memory".to_string();

        // Try a few times to avoid a TOCTOU race for an ephemeral port.
        let mut deployed_out_route: Option<Route> = None;
        let mut deployed_in_route: Option<Route> = None;
        for _attempt in 0..5 {
            let port = get_free_port();
            let config_yaml = CONFIG_YAML
                .replace("{out_port}", &port.to_string())
                .replace(
                    "{out_capacity}",
                    &(PERF_TEST_MESSAGE_COUNT + 1000).to_string(),
                )
                .replace("{buffer_size}", &(PERF_TEST_MESSAGE_COUNT * 2).to_string());
            let yaml_val: serde_yaml_ng::Value =
                serde_yaml_ng::from_str(&config_yaml).expect("Failed to parse YAML config");
            let routes_val = yaml_val.get("routes").expect("YAML must have 'routes' key");
            let routes: HashMap<String, Route> =
                serde_yaml_ng::from_value(routes_val.clone()).expect("Failed to parse routes");

            let in_route = routes[&in_route_name].clone();
            let mut out_route = routes[&out_route_name].clone(); // Make out_route mutable

            let enable_dummy_handler = std::env::var("MQB_ENABLE_DUMMY_HANDLER")
                .map(|s| s.to_lowercase() == "true")
                .unwrap_or(false);

            let handler_description = if enable_dummy_handler {
                let dummy_handler = |msg: mq_bridge::CanonicalMessage| async move {
                    // This handler does minimal work: just forward the message.
                    // It simulates the overhead of a handler without actual business logic.
                    Ok(mq_bridge::Handled::Publish(msg))
                };
                out_route = out_route.with_handler(dummy_handler);
                " (with dummy handler)"
            } else {
                ""
            };

            let in_route = routes[&in_route_name].clone();
            let out_route = routes[&out_route_name].clone();

            // Attempt to deploy the HTTP consumer (server) and probe readiness.
            match out_route.deploy(&out_route_name).await {
                Ok(_) => {
                    let addr = format!("127.0.0.1:{}", port);
                    if wait_for_server_ready(&addr, Duration::from_secs(5)).await {
                        deployed_out_route = Some(out_route);
                        deployed_in_route = Some(in_route);
                        break;
                    } else {
                        // Not ready, stop and retry with a new port
                        let _ = mq_bridge::Route::stop(&out_route_name).await;
                        tokio::time::sleep(Duration::from_millis(50)).await;
                        continue;
                    }
                }
                Err(e) => {
                    eprintln!("Failed to deploy http consumer on port {}: {}", port, e);
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    continue;
                }
            }
        }

        let in_route = deployed_in_route.expect("Failed to deploy routes after retries");
        let out_route = deployed_out_route.expect("Failed to deploy http consumer after retries");

        // Get the memory output channel that the server writes into.
        let memory_channel = out_route.output.channel().unwrap();

        // Start the publisher (memory -> HTTP) route.
        in_route
            .deploy(&in_route_name)
            .await
            .expect("Failed to deploy memory_to_http route");

        // Obtain the input memory channel and fill it with test messages.
        let in_channel = in_route.input.channel().unwrap();
        let messages = mq_bridge::test_utils::generate_test_messages(PERF_TEST_MESSAGE_COUNT);
        in_channel.fill_messages(messages).await.unwrap();

        // Poll the output channel until all messages arrive or we time out.
        // Keep this below the outer 60s timeout to allow cleanup to complete.
        let deadline = Duration::from_secs(45);
        let start = Instant::now();
        let mut last_log = Instant::now();
        let mut received = 0usize;

        while start.elapsed() < deadline {
            let batch = memory_channel.drain_messages();
            if !batch.is_empty() {
                received += batch.len();
            }
            if received >= PERF_TEST_MESSAGE_COUNT {
                break;
            }
            if last_log.elapsed() >= Duration::from_secs(5) {
                println!(
                    "Progress: {}/{} messages received",
                    received, PERF_TEST_MESSAGE_COUNT
                );
                last_log = Instant::now();
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        let duration = start.elapsed();

        // Stop both routes (Route::stop has a built-in 5 s timeout so this won't hang).
        mq_bridge::Route::stop(&in_route_name).await;
        mq_bridge::Route::stop(&out_route_name).await;

        // Reconstruct handler_description for the performance result
        let enable_dummy_handler = std::env::var("MQB_ENABLE_DUMMY_HANDLER")
            .map(|s| s.to_lowercase() == "true")
            .unwrap_or(false);
        let handler_description = if enable_dummy_handler {
            " (with dummy handler)"
        } else {
            ""
        };

        let messages_per_second = received as f64 / duration.as_secs_f64();
        mq_bridge::test_utils::add_performance_result(mq_bridge::test_utils::PerformanceResult {
            test_name: "HTTP Pipeline".to_string(),
            write_performance: messages_per_second,
            read_performance: messages_per_second,
            ..Default::default()
        });

        assert_eq!(
            received, PERF_TEST_MESSAGE_COUNT,
            "Expected {} messages, received {}",
            PERF_TEST_MESSAGE_COUNT, received
        );
    })
    .await
    .expect("HTTP pipeline test timed out");
}

#[cfg(feature = "http")]
#[tokio::test(flavor = "multi_thread")]
async fn test_http_concurrency() {
    setup_logging();
    let port = get_free_port();
    let input = Endpoint::new(EndpointType::Http(HttpConfig {
        url: format!("127.0.0.1:{}", port),
        ..Default::default()
    }));
    // Publisher needs the schema
    let sender = Endpoint::new(EndpointType::Http(HttpConfig {
        url: format!("http://127.0.0.1:{}", port),
        ..Default::default()
    }));
    let output = Endpoint::new_memory("con_out_http", 10);

    mq_bridge::test_utils::run_concurrency_test(input, output, sender).await;
}
