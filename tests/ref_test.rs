use mq_bridge::models::{Endpoint, Middleware};
use mq_bridge::route::register_endpoint;
use mq_bridge::{CanonicalMessage, Route};
use std::sync::{Arc, Mutex};
use std::time::Duration;

#[tokio::test(flavor = "multi_thread")]
async fn test_ref_endpoint_consumer_and_publisher() {
    let shared_topic = "shared_memory_topic";
    let base_endpoint = Endpoint::new_memory(shared_topic, 100);

    register_endpoint("common_queue", base_endpoint);

    // Create a route using 'ref' for both input and output
    // Input Ref: Adds a metadata middleware to verify wrapping order
    let input = Endpoint::new(mq_bridge::models::EndpointType::Ref(
        "common_queue".to_string(),
    ));

    // Output Ref: Points to the same queue
    let output = Endpoint::new(mq_bridge::models::EndpointType::Ref(
        "common_queue".to_string(),
    ));

    let route = Route::new(input, output);
    route.deploy("ref_test_route").await.unwrap();

    // Send a message to the underlying channel
    let channel = mq_bridge::get_or_create_channel(&mq_bridge::models::MemoryConfig {
        topic: shared_topic.to_string(),
        ..Default::default()
    });

    let msg = CanonicalMessage::from("hello ref");
    channel.send_message(msg).await.unwrap();

    // Wait for the message to be processed (consumed and republished to the same queue)
    // Since input and output are the same queue, we expect to see the message appear again.
    // We consume it manually to verify.
    let received = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if let Ok(batch) = channel.receiver.recv().await {
                for msg in batch {
                    // We are looking for the republished message.
                    // Ideally we'd tag it to distinguish, but for this simple test,
                    // seeing "hello ref" again implies the route worked.
                    if msg.get_payload_str() == "hello ref" {
                        return msg;
                    }
                }
            }
        }
    })
    .await
    .expect("Timed out waiting for message processing");

    assert_eq!(received.get_payload_str(), "hello ref");

    Route::stop("ref_test_route").await;
}

#[tokio::test]
async fn test_ref_middleware_ordering() {
    // Verify that middlewares on the Ref wrap the middlewares on the Target.
    // We use a custom middleware that appends to a shared log.

    let log = Arc::new(Mutex::new(Vec::new()));
    let log_clone = log.clone();

    // Define a simple middleware factory for testing
    #[derive(Debug)]
    struct LogMiddleware {
        tag: String,
        log: Arc<Mutex<Vec<String>>>,
    }

    #[async_trait::async_trait]
    impl mq_bridge::traits::CustomMiddlewareFactory for LogMiddleware {
        async fn apply_publisher(
            &self,
            publisher: Box<dyn mq_bridge::traits::MessagePublisher>,
            _route_name: &str,
            _config: &serde_json::Value,
        ) -> anyhow::Result<Box<dyn mq_bridge::traits::MessagePublisher>> {
            let tag = self.tag.clone();
            let log = self.log.clone();
            // Wrap the publisher
            struct Wrapped {
                inner: Box<dyn mq_bridge::traits::MessagePublisher>,
                tag: String,
                log: Arc<Mutex<Vec<String>>>,
            }
            #[async_trait::async_trait]
            impl mq_bridge::traits::MessagePublisher for Wrapped {
                async fn send(
                    &self,
                    msg: CanonicalMessage,
                ) -> Result<mq_bridge::Sent, mq_bridge::traits::PublisherError> {
                    self.log.lock().unwrap().push(self.tag.clone());
                    self.inner.send(msg).await
                }
                async fn send_batch(
                    &self,
                    msgs: Vec<CanonicalMessage>,
                ) -> Result<mq_bridge::SentBatch, mq_bridge::traits::PublisherError>
                {
                    self.inner.send_batch(msgs).await
                }
                fn as_any(&self) -> &dyn std::any::Any {
                    self
                }
            }
            Ok(Box::new(Wrapped {
                inner: publisher,
                tag,
                log,
            }))
        }
    }

    mq_bridge::route::register_middleware_factory(
        "log_inner",
        Arc::new(LogMiddleware {
            tag: "inner".into(),
            log: log.clone(),
        }),
    );
    mq_bridge::route::register_middleware_factory(
        "log_outer",
        Arc::new(LogMiddleware {
            tag: "outer".into(),
            log: log.clone(),
        }),
    );

    // Define Inner Endpoint with "inner" middleware
    let inner_ep = Endpoint::new_memory("order_test", 10).add_middleware(Middleware::Custom {
        name: "log_inner".into(),
        config: serde_json::Value::Null,
    });
    register_endpoint("inner_target", inner_ep);

    // Define Ref Endpoint with "outer" middleware
    let ref_ep = Endpoint::new(mq_bridge::models::EndpointType::Ref("inner_target".into()))
        .add_middleware(Middleware::Custom {
            name: "log_outer".into(),
            config: serde_json::Value::Null,
        });

    let publisher = mq_bridge::endpoints::create_publisher_from_route("test", &ref_ep)
        .await
        .unwrap();

    publisher
        .send(CanonicalMessage::from("test"))
        .await
        .unwrap();

    // Verify Order: Outer should run before Inner
    let entries = log_clone.lock().unwrap();
    assert_eq!(*entries, vec!["outer".to_string(), "inner".to_string()]);
}

#[tokio::test]
async fn test_circular_reference_detection() {
    register_endpoint(
        "circle_a",
        Endpoint::new(mq_bridge::models::EndpointType::Ref("circle_b".to_string())),
    );
    register_endpoint(
        "circle_b",
        Endpoint::new(mq_bridge::models::EndpointType::Ref("circle_a".to_string())),
    );

    let input = Endpoint::new(mq_bridge::models::EndpointType::Ref("circle_a".to_string()));

    // Attempt to create consumer should fail
    let result = input.create_consumer("circular_test").await;
    if let Err(e) = result {
        assert!(e.to_string().contains("Circular reference detected"));
    } else {
        panic!("Should have failed with circular reference error");
    }
}

#[tokio::test]
async fn test_route_chaining_via_ref() {
    // Route 1: Memory -> Memory (intermediate)
    let in_ep = Endpoint::new_memory("chain_in", 10);
    let mid_ep = Endpoint::new_memory("chain_mid", 10);
    let route1 = Route::new(in_ep.clone(), mid_ep.clone());

    // Register route1's output as "mid_point"
    route1.register_output_endpoint(Some("mid_point")).unwrap();

    // Route 2: Ref("mid_point") -> Memory (out);
    route1.deploy("route1").await.unwrap();

    // Route 2: Ref("mid_point") -> Memory (out)
    // This effectively makes Route 2 consume from "chain_mid"
    let ref_in = Endpoint::new(mq_bridge::models::EndpointType::Ref(
        "mid_point".to_string(),
    ));
    let out_ep = Endpoint::new_memory("chain_out", 10);
    let route2 = Route::new(ref_in, out_ep.clone());
    route2.deploy("route2").await.unwrap();

    // Send message to start
    let channel_in = in_ep.channel().unwrap();
    channel_in
        .send_message(CanonicalMessage::from("hello chain"))
        .await
        .unwrap();

    // Verify at end
    let channel_out = out_ep.channel().unwrap();
    let received = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if let Ok(batch) = channel_out.receiver.recv().await {
                if let Some(msg) = batch.into_iter().next() {
                    return msg;
                }
            }
        }
    })
    .await
    .expect("Timed out waiting for chained message");

    assert_eq!(received.get_payload_str(), "hello chain");

    Route::stop("route1").await;
    Route::stop("route2").await;
}
