#![allow(dead_code)]
use mq_bridge::endpoints::file::{FileConsumer, FilePublisher};
use mq_bridge::test_utils::{setup_logging, verify_subscriber_logic};
use std::sync::Arc;

pub async fn test_file_subscriber_logic() {
    setup_logging();
    let dir = tempfile::tempdir().unwrap();
    let path = dir
        .path()
        .join("sub_logic.log")
        .to_str()
        .unwrap()
        .to_string();

    let config = mq_bridge::models::FileConfig {
        path: path.clone(),
        mode: Some(mq_bridge::models::FileConsumerMode::Subscribe { delete: true }),
        ..Default::default()
    };

    let publisher = Arc::new(FilePublisher::new(&config).await.unwrap());
    let sub1 = Arc::new(tokio::sync::Mutex::new(
        FileConsumer::new(&config).await.unwrap(),
    ));
    let sub2 = Arc::new(tokio::sync::Mutex::new(
        FileConsumer::new(&config).await.unwrap(),
    ));

    // Wait for consumers to be ready
    sub1.lock().await.wait_ready().await;
    sub2.lock().await.wait_ready().await;

    verify_subscriber_logic(publisher, sub1, sub2).await;
}
