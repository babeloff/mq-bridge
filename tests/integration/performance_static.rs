use mq_bridge::models::{Endpoint, EndpointType, Route};
use mq_bridge::test_utils::{format_pretty, setup_logging};
use mq_bridge::Handled;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

// cargo test --release --test integration_test performance_static -- --ignored --nocapture

#[tokio::test(flavor = "multi_thread")]
#[ignore = "Performance test"]
async fn test_static_to_null_performance() {
    setup_logging();

    let counter = Arc::new(AtomicU64::new(0));
    let counter_clone = counter.clone();

    // Input: Static consumer that always produces the same string.
    let input = Endpoint::new(EndpointType::Static("benchmark_payload_data".to_string()));

    // Output: Null endpoint that discards everything.
    let output = Endpoint::new(EndpointType::Null);

    // Route with a handler that just increments a counter.
    // Now that batching is implemented, concurrency and batch_size will scale throughput.
    let route = Route::new(input, output)
        .with_handler(move |_msg| {
            counter_clone.fetch_add(1, Ordering::Relaxed);
            async move { Ok(Handled::Ack) }
        })
        .with_concurrency(8)
        .with_batch_size(1);

    let route_name = "static_perf_bench";
    println!("--- Starting Static Consumer Performance Test (10s) ---");

    route.deploy(route_name).await.unwrap();

    let start = Instant::now();
    tokio::time::sleep(Duration::from_secs(10)).await;
    let elapsed = start.elapsed();

    // Stop the route to ensure all tasks finish and we get a clean count.
    Route::stop(route_name).await;

    let total_messages = counter.load(Ordering::Relaxed);
    let msgs_per_sec = total_messages as f64 / elapsed.as_secs_f64();

    println!("\n--- Performance Results for Static Consumer ---");
    println!("{:<25} | {:<25}", "Metric", "Value");
    println!("{:-<25}-|-{:-<25}", "", "");
    println!(
        "{:<25} | {:<25}",
        "Total Messages",
        format_pretty(total_messages)
    );
    println!("{:<25} | {:<25.2?}", "Time Elapsed", elapsed);
    println!(
        "{:<25} | {:<25}",
        "Throughput (msgs/sec)",
        format_pretty(msgs_per_sec)
    );
    println!("-----------------------------------------------\n");

    // Sanity check
    assert!(total_messages > 0, "No messages were processed");
}
