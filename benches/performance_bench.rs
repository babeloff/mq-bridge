use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use once_cell::sync::Lazy;
use std::collections::HashMap;
use std::time::Duration;
use tokio::runtime::Runtime;
use tokio::sync::Mutex;

// Include the integration module from the tests directory so we can reuse the test logic.
#[path = "../tests/integration/mod.rs"]
mod integration; // Still needed for backend modules like kafka, nats etc.

use mq_bridge::bench_backend;
use mq_bridge::test_utils::{print_benchmark_results, PerformanceResult, PERF_TEST_CONCURRENCY};

const PERF_TEST_MESSAGE_COUNT: usize = 1000;

#[allow(unused)]
#[cfg(feature = "rustls")]
fn ensure_rustls_installed() {
    // Install the process-level provider selected by feature flags (tests/benches do this).
    #[cfg(feature = "rustls-aws-lc")]
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    #[cfg(all(feature = "rustls-ring", not(feature = "rustls-aws-lc")))]
    let _ = rustls::crypto::ring::default_provider().install_default();
}

static BENCH_RESULTS: Lazy<Mutex<HashMap<String, PerformanceResult>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));

// --- Helper Modules for Backend Setup ---

#[cfg(feature = "nats")]
pub mod nats_helper {
    use mq_bridge::endpoints::nats::{NatsConsumer, NatsPublisher};
    use mq_bridge::models::NatsConfig;
    use mq_bridge::traits::{MessageConsumer, MessagePublisher};
    use std::sync::Arc;
    use tokio::sync::Mutex;

    fn get_config(stream_name: &str, subject: &str) -> NatsConfig {
        NatsConfig {
            url: "nats://localhost:4222".to_string(),
            delayed_ack: false,
            stream: Some(stream_name.to_string()),
            subject: Some(subject.to_string()),
            ..Default::default()
        }
    }
    pub async fn create_publisher() -> Arc<dyn MessagePublisher> {
        let stream_name = "perf_nats_direct";
        let subject = "perf_nats_direct.subject";
        Arc::new(
            NatsPublisher::new(&get_config(stream_name, subject))
                .await
                .unwrap(),
        )
    }

    pub async fn create_consumer() -> Arc<Mutex<dyn MessageConsumer>> {
        let stream_name = "perf_nats_direct";
        let subject = "perf_nats_direct.subject";
        Arc::new(Mutex::new(
            NatsConsumer::new(&get_config(stream_name, subject))
                .await
                .unwrap(),
        ))
    }
}

#[cfg(feature = "mongodb")]
pub mod mongodb_helper {
    use mq_bridge::endpoints::mongodb::{MongoDbConsumer, MongoDbPublisher};
    use mq_bridge::models::MongoDbConfig;
    use mq_bridge::traits::{MessageConsumer, MessagePublisher};
    use std::sync::Arc;
    use tokio::sync::Mutex;

    fn get_config(collection_name: &str) -> MongoDbConfig {
        MongoDbConfig {
            url: "mongodb://localhost:27017".to_string(),
            database: "mq_bridge_test_db".to_string(),
            collection: Some(collection_name.to_string()),
            ..Default::default()
        }
    }
    pub async fn create_publisher() -> Arc<dyn MessagePublisher> {
        let collection_name = "perf_mongodb_direct";
        let config = get_config(collection_name);
        Arc::new(MongoDbPublisher::new(&config).await.unwrap())
    }

    pub async fn create_consumer() -> Arc<Mutex<dyn MessageConsumer>> {
        let collection_name = "perf_mongodb_direct";
        let config = get_config(collection_name);

        // Drop collection before test to ensure clean state
        let client = mongodb::Client::with_uri_str(&config.url).await.unwrap();
        client
            .database(&config.database)
            .collection::<mongodb::bson::Document>(collection_name)
            .drop()
            .await
            .ok();

        Arc::new(Mutex::new(MongoDbConsumer::new(&config).await.unwrap()))
    }
}

#[cfg(feature = "mongodb")]
pub mod mongodb_subscriber_helper {
    use mq_bridge::endpoints::mongodb::{MongoDbPublisher, MongoDbSubscriber};
    use mq_bridge::models::MongoDbConfig;
    use mq_bridge::traits::{MessageConsumer, MessagePublisher};
    use std::sync::Arc;
    use tokio::sync::Mutex;

    fn get_config(collection_name: &str) -> MongoDbConfig {
        MongoDbConfig {
            url: "mongodb://localhost:27017".to_string(),
            database: "mq_bridge_test_db".to_string(),
            collection: Some(collection_name.to_string()),
            change_stream: true,
            ..Default::default()
        }
    }
    pub async fn create_publisher() -> Arc<dyn MessagePublisher> {
        let collection_name = "perf_mongodb_sub_direct";
        let config = get_config(collection_name);
        Arc::new(MongoDbPublisher::new(&config).await.unwrap())
    }

    pub async fn create_consumer() -> Arc<Mutex<dyn MessageConsumer>> {
        let collection_name = "perf_mongodb_sub_direct";
        let config = get_config(collection_name);

        // Drop collection before test
        let client = mongodb::Client::with_uri_str(&config.url).await.unwrap();
        client
            .database(&config.database)
            .collection::<mongodb::bson::Document>(collection_name)
            .drop()
            .await
            .ok();

        Arc::new(Mutex::new(MongoDbSubscriber::new(&config).await.unwrap()))
    }
}

#[cfg(feature = "sqlx")]
pub mod sqlx_helper {
    use mq_bridge::endpoints::sqlx::{SqlxConsumer, SqlxPublisher};
    use mq_bridge::models::SqlxConfig;
    use mq_bridge::traits::{MessageConsumer, MessagePublisher};
    use std::sync::Arc;
    use tokio::sync::Mutex;

    fn get_config() -> SqlxConfig {
        SqlxConfig {
            url: "postgres://testuser:testpass@localhost:5432/testdb".to_string(),
            table: "perf_sqlx_direct".to_string(),
            auto_create_table: true,
            min_connections: Some(2),
            ..Default::default()
        }
    }

    pub async fn create_publisher() -> Arc<dyn MessagePublisher> {
        let config = get_config();
        Arc::new(SqlxPublisher::new(&config).await.unwrap())
    }

    pub async fn create_consumer() -> Arc<Mutex<dyn MessageConsumer>> {
        let config = get_config();

        // Ensure the table exists and is empty before the run.
        let publisher = SqlxPublisher::new(&config).await.unwrap();
        drop(publisher);
        sqlx::any::install_default_drivers();
        let pool = sqlx::any::AnyPoolOptions::new()
            .connect(&config.url)
            .await
            .unwrap();
        sqlx::query("DELETE FROM perf_sqlx_direct")
            .execute(&pool)
            .await
            .ok();

        Arc::new(Mutex::new(SqlxConsumer::new(&config).await.unwrap()))
    }
}

#[cfg(feature = "amqp")]
pub mod amqp_helper {
    use mq_bridge::endpoints::amqp::{AmqpConsumer, AmqpPublisher};
    use mq_bridge::models::AmqpConfig;
    use mq_bridge::traits::{MessageConsumer, MessagePublisher};
    use std::sync::Arc;
    use tokio::sync::Mutex;

    fn get_config(queue: &str) -> AmqpConfig {
        AmqpConfig {
            url: "amqp://guest:guest@localhost:5672/%2f".to_string(),
            delayed_ack: false,
            queue: Some(queue.to_string()),
            ..Default::default()
        }
    }

    pub async fn create_publisher() -> Arc<dyn MessagePublisher> {
        let queue = "perf_test_amqp_direct";
        Arc::new(AmqpPublisher::new(&get_config(queue)).await.unwrap())
    }

    pub async fn create_consumer() -> Arc<Mutex<dyn MessageConsumer>> {
        let queue = "perf_test_amqp_direct";
        Arc::new(Mutex::new(
            AmqpConsumer::new(&get_config(queue)).await.unwrap(),
        ))
    }
}

#[cfg(feature = "kafka")]
pub mod kafka_helper {
    use mq_bridge::endpoints::kafka::{KafkaConsumer, KafkaPublisher};
    use mq_bridge::models::KafkaConfig;
    use mq_bridge::traits::{MessageConsumer, MessagePublisher};
    use std::sync::Arc;
    use tokio::sync::Mutex;

    fn get_config(topic: &str) -> KafkaConfig {
        KafkaConfig {
            url: "localhost:9092".to_string(),
            group_id: Some("perf_test_group_kafka".to_string()),
            topic: Some(topic.to_string()),
            producer_options: Some(vec![
                ("queue.buffering.max.ms".to_string(), "1".to_string()), // Small linger; send_batch already enqueues a burst
                ("acks".to_string(), "1".to_string()), // Wait for leader ack, a good balance
                ("compression.type".to_string(), "snappy".to_string()), // Use snappy compression
            ]),
            ..Default::default()
        }
    }
    pub async fn create_publisher() -> Arc<dyn MessagePublisher> {
        let topic = "perf_kafka_direct";
        Arc::new(KafkaPublisher::new(&get_config(topic)).await.unwrap())
    }

    pub async fn create_consumer() -> Arc<Mutex<dyn MessageConsumer>> {
        let topic = "perf_kafka_direct";
        Arc::new(Mutex::new(
            KafkaConsumer::new(&get_config(topic)).await.unwrap(),
        ))
    }
}

#[cfg(feature = "mqtt")]
pub mod mqtt_helper {
    use super::PERF_TEST_MESSAGE_COUNT;
    use mq_bridge::endpoints::mqtt::{MqttConsumer, MqttPublisher};
    use mq_bridge::models::MqttConfig;
    use mq_bridge::traits::{MessageConsumer, MessagePublisher};
    use std::sync::Arc;
    use tokio::sync::Mutex;

    fn get_config(topic: &str, client_id: &str) -> MqttConfig {
        MqttConfig {
            url: "tcp://localhost:1883".to_string(),
            queue_capacity: Some(PERF_TEST_MESSAGE_COUNT * 4), // For batch and single
            max_inflight: Some(1000),
            qos: Some(1),
            clean_session: false,
            keep_alive_seconds: Some(60),
            topic: Some(topic.to_string()),
            client_id: Some(client_id.to_string()),
            ..Default::default()
        }
    }

    pub async fn create_publisher() -> Arc<dyn MessagePublisher> {
        let topic = "perf_mqtt_direct";
        let publisher_id = format!("pub-{}", fast_uuid_v7::gen_id());
        Arc::new(
            MqttPublisher::new(&get_config(topic, &publisher_id))
                .await
                .unwrap(),
        )
    }

    pub async fn create_consumer() -> Arc<Mutex<dyn MessageConsumer>> {
        let topic = "perf_mqtt_direct";
        let consumer_id = format!("sub-{}", fast_uuid_v7::gen_id());
        Arc::new(Mutex::new(
            MqttConsumer::new(&get_config(topic, &consumer_id))
                .await
                .unwrap(),
        ))
    }
}

#[cfg(feature = "aws")]
pub mod aws_helper {
    use aws_sdk_sns::config::Credentials;
    use mq_bridge::endpoints::aws::{AwsConsumer, AwsPublisher};
    use mq_bridge::models::AwsConfig;
    use mq_bridge::traits::{MessageConsumer, MessagePublisher};
    use std::sync::Arc;
    use tokio::sync::Mutex;

    async fn ensure_queue_exists() -> String {
        let config = aws_config::defaults(aws_config::BehaviorVersion::latest())
            .region(aws_config::Region::new("us-east-1"))
            .endpoint_url("http://localhost:4566")
            .credentials_provider(Credentials::new("test", "test", None, None, "static"))
            .load()
            .await;
        let client = aws_sdk_sqs::Client::new(&config);
        let resp = client
            .create_queue()
            .queue_name("perf-test-queue")
            .send()
            .await
            .expect("Failed to create SQS queue");
        let queue_url = resp.queue_url.expect("SQS queue URL was None");
        client
            .purge_queue()
            .queue_url(&queue_url)
            .send()
            .await
            .expect("Failed to purge SQS queue");
        queue_url
    }

    fn get_config(queue_url: Option<String>) -> AwsConfig {
        AwsConfig {
            queue_url: Some(queue_url.unwrap_or_else(|| {
                "http://localhost:4566/000000000000/perf-test-queue".to_string()
            })),
            region: Some("us-east-1".to_string()),
            endpoint_url: Some("http://localhost:4566".to_string()),
            access_key: Some("test".to_string()),
            secret_key: Some("test".to_string()),
            ..Default::default()
        }
    }

    pub async fn create_publisher() -> Arc<dyn MessagePublisher> {
        let url = ensure_queue_exists().await;
        Arc::new(AwsPublisher::new(&get_config(Some(url))).await.unwrap())
    }

    pub async fn create_consumer() -> Arc<Mutex<dyn MessageConsumer>> {
        let url = ensure_queue_exists().await;
        Arc::new(Mutex::new(
            AwsConsumer::new(&get_config(Some(url))).await.unwrap(),
        ))
    }
}

#[cfg(feature = "zeromq")]
pub mod zeromq_helper {
    use super::PERF_TEST_MESSAGE_COUNT;
    use mq_bridge::endpoints::zeromq::{ZeroMqConsumer, ZeroMqPublisher};
    use mq_bridge::models::{ZeroMqConfig, ZeroMqSocketType};
    use mq_bridge::traits::{MessageConsumer, MessagePublisher};
    use once_cell::sync::Lazy;
    use rand::RngExt;
    use std::sync::atomic::{AtomicU16, Ordering};
    use std::sync::Arc;
    use tokio::sync::Mutex;

    static PORT: Lazy<AtomicU16> = Lazy::new(|| {
        let mut rng = rand::rng();
        AtomicU16::new(rng.random_range(10000..60000))
    });

    pub async fn create_publisher() -> Arc<dyn MessagePublisher> {
        let port = PORT.load(Ordering::SeqCst);
        let config = ZeroMqConfig {
            url: format!("ipc:///tmp/mq-bridge-{}.sock", port),
            socket_type: Some(ZeroMqSocketType::Push),
            bind: false,
            internal_buffer_size: Some(PERF_TEST_MESSAGE_COUNT + 1),
            topic: None,
        };
        Arc::new(ZeroMqPublisher::new(&config).await.unwrap())
    }

    pub async fn create_consumer() -> Arc<Mutex<dyn MessageConsumer>> {
        let port = PORT.load(Ordering::SeqCst);
        let path = format!("/tmp/mq-bridge-{}.sock", port);
        let _ = std::fs::remove_file(&path);
        let config = ZeroMqConfig {
            url: format!("ipc://{}", path),
            socket_type: Some(ZeroMqSocketType::Pull),
            bind: true,
            internal_buffer_size: Some(PERF_TEST_MESSAGE_COUNT + 1),
            topic: None,
        };
        Arc::new(Mutex::new(ZeroMqConsumer::new(&config).await.unwrap()))
    }
}

#[cfg(any(feature = "ibm-mq-static", feature = "ibm-mq"))]
pub mod ibm_mq_helper {
    use mq_bridge::endpoints::ibm_mq::{IbmMqConsumer, IbmMqPublisher};
    use mq_bridge::models::IbmMqConfig;
    use mq_bridge::traits::{MessageConsumer, MessagePublisher};
    use std::sync::Arc;
    use tokio::sync::Mutex;

    pub fn get_config(queue: &str) -> IbmMqConfig {
        IbmMqConfig {
            username: Some("app".to_string()),
            password: Some("admin".to_string()),
            queue_manager: "QM1".to_string(),
            url: "localhost(1414)".to_string(),
            channel: "DEV.APP.SVRCONN".to_string(),
            queue: Some(queue.to_string()),
            ..Default::default()
        }
    }

    pub async fn create_publisher() -> Arc<dyn MessagePublisher> {
        let config = get_config("DEV.QUEUE.1");
        Arc::new(IbmMqPublisher::new(&config).await.unwrap())
    }

    pub async fn create_consumer() -> Arc<Mutex<dyn MessageConsumer>> {
        let config = get_config("DEV.QUEUE.1");
        Arc::new(Mutex::new(IbmMqConsumer::new(&config).await.unwrap()))
    }
}

pub mod memory_helper {
    use super::PERF_TEST_MESSAGE_COUNT;
    use mq_bridge::endpoints::memory::{MemoryConsumer, MemoryPublisher};
    use mq_bridge::traits::{MessageConsumer, MessagePublisher};
    use std::sync::Arc;
    use tokio::sync::Mutex;

    pub async fn create_publisher() -> Arc<dyn MessagePublisher> {
        Arc::new(MemoryPublisher::new_local(
            "perf_memory_bench",
            PERF_TEST_MESSAGE_COUNT * 2,
        ))
    }

    pub async fn create_consumer() -> Arc<Mutex<dyn MessageConsumer>> {
        Arc::new(Mutex::new(MemoryConsumer::new_local(
            "perf_memory_bench",
            PERF_TEST_MESSAGE_COUNT * 2,
        )))
    }
}

pub mod memory_subscriber_helper {
    use super::PERF_TEST_MESSAGE_COUNT;
    use mq_bridge::endpoints::memory::{MemoryPublisher, MemorySubscriber};
    use mq_bridge::models::MemoryConfig;
    use mq_bridge::traits::{MessageConsumer, MessagePublisher};
    use once_cell::sync::Lazy;
    use std::sync::Arc;
    use std::sync::Mutex as StdMutex;
    use tokio::sync::Mutex;

    // Shared state to coordinate topic name between consumer and publisher creation.
    // create_consumer is called first by the benchmark macro, so it generates the topic.
    static CURRENT_TOPIC: Lazy<StdMutex<String>> =
        Lazy::new(|| StdMutex::new("perf_memory_sub_bench_init".to_string()));

    pub async fn create_publisher() -> Arc<dyn MessagePublisher> {
        let topic = {
            let lock = CURRENT_TOPIC.lock().unwrap();
            lock.clone()
        };

        let config = MemoryConfig {
            topic,
            capacity: Some(PERF_TEST_MESSAGE_COUNT * 2),
            subscribe_mode: true,
            ..Default::default()
        };
        Arc::new(MemoryPublisher::new(&config).unwrap())
    }

    pub async fn create_consumer() -> Arc<Mutex<dyn MessageConsumer>> {
        let topic = format!("perf_memory_sub_bench_{}", fast_uuid_v7::gen_id());
        {
            let mut lock = CURRENT_TOPIC.lock().unwrap();
            *lock = topic.clone();
        }

        let config = MemoryConfig {
            topic,
            capacity: Some(PERF_TEST_MESSAGE_COUNT * 2),
            subscribe_mode: true,
            ..Default::default()
        };
        let subscriber_id = format!("sub-{}", fast_uuid_v7::gen_id());
        Arc::new(Mutex::new(
            MemorySubscriber::new(&config, &subscriber_id).unwrap(),
        ))
    }
}

pub mod file_helper {
    use mq_bridge::endpoints::file::{FileConsumer, FilePublisher};
    use mq_bridge::models::FileConfig;
    use mq_bridge::traits::{MessageConsumer, MessagePublisher};
    use once_cell::sync::Lazy;
    use std::sync::Arc;
    use std::sync::Mutex as StdMutex;
    use tokio::sync::Mutex;

    static FILE_PATH: Lazy<StdMutex<String>> = Lazy::new(|| StdMutex::new(String::new()));
    static TEMP_DIR: Lazy<StdMutex<Option<tempfile::TempDir>>> = Lazy::new(|| StdMutex::new(None));

    pub async fn create_consumer() -> Arc<Mutex<dyn MessageConsumer>> {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("bench.log");
        let path_str = path.to_str().unwrap().to_string();

        {
            let mut p_lock = FILE_PATH.lock().unwrap();
            *p_lock = path_str.clone();
            let mut t_lock = TEMP_DIR.lock().unwrap();
            *t_lock = Some(dir);
        }

        let config = FileConfig {
            path: path_str,
            ..Default::default()
        };
        Arc::new(Mutex::new(FileConsumer::new(&config).await.unwrap()))
    }

    pub async fn create_publisher() -> Arc<dyn MessagePublisher> {
        let path_str = {
            let lock = FILE_PATH.lock().unwrap();
            lock.clone()
        };
        let config = FileConfig {
            path: path_str,
            ..Default::default()
        };
        Arc::new(FilePublisher::new(&config).await.unwrap())
    }
}

pub mod file_delete_helper {
    use mq_bridge::endpoints::file::{FileConsumer, FilePublisher};
    use mq_bridge::models::FileConfig;
    use mq_bridge::traits::{MessageConsumer, MessagePublisher};
    use once_cell::sync::Lazy;
    use std::sync::Arc;
    use std::sync::Mutex as StdMutex;
    use tokio::sync::Mutex;

    static FILE_PATH: Lazy<StdMutex<String>> = Lazy::new(|| StdMutex::new(String::new()));
    static TEMP_DIR: Lazy<StdMutex<Option<tempfile::TempDir>>> = Lazy::new(|| StdMutex::new(None));

    pub async fn create_consumer() -> Arc<Mutex<dyn MessageConsumer>> {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("bench_delete.log");
        let path_str = path.to_str().unwrap().to_string();

        {
            let mut p_lock = FILE_PATH.lock().unwrap();
            *p_lock = path_str.clone();
            let mut t_lock = TEMP_DIR.lock().unwrap();
            *t_lock = Some(dir);
        }

        let config = FileConfig {
            path: path_str,
            mode: Some(mq_bridge::models::FileConsumerMode::Consume { delete: true }),
            ..Default::default()
        };
        Arc::new(Mutex::new(FileConsumer::new(&config).await.unwrap()))
    }

    pub async fn create_publisher() -> Arc<dyn MessagePublisher> {
        let path_str = {
            let lock = FILE_PATH.lock().unwrap();
            lock.clone()
        };
        let config = FileConfig {
            path: path_str,
            ..Default::default()
        };
        Arc::new(FilePublisher::new(&config).await.unwrap())
    }
}

#[cfg(feature = "http")]
pub mod http_helper {
    use mq_bridge::endpoints::http::{HttpConsumer, HttpPublisher};
    use mq_bridge::endpoints::memory::{MemoryConsumer, MemoryPublisher};
    use mq_bridge::models::HttpConfig;
    use mq_bridge::traits::{MessageConsumer, MessageDisposition, MessagePublisher};
    use once_cell::sync::Lazy;
    use std::sync::Arc;
    use std::sync::Mutex as StdMutex;
    use tokio::sync::Mutex;

    static CURRENT_URL: Lazy<StdMutex<String>> = Lazy::new(|| StdMutex::new(String::new()));

    pub async fn create_consumer() -> Arc<Mutex<dyn MessageConsumer>> {
        #[cfg(feature = "rustls")]
        crate::ensure_rustls_installed();

        let http_config = HttpConfig {
            url: "127.0.0.1:0".to_string(),
            // Sufficient internal buffer to prevent backpressure during bursts
            internal_buffer_size: Some(super::PERF_TEST_MESSAGE_COUNT * 2),
            concurrency_limit: Some(super::PERF_TEST_CONCURRENCY * 2),
            request_timeout_ms: Some(10000),
            fire_and_forget: false, // Reliable mode: wait for ack before HTTP response
            ..Default::default()
        };

        let mut http_consumer = HttpConsumer::new(&http_config)
            .await
            .expect("Failed to create HttpConsumer");
        let addr = http_consumer
            .bound_addr()
            .expect("HttpConsumer should have bound addr");
        let url = format!("http://{}", addr);

        {
            let mut lock = CURRENT_URL.lock().unwrap();
            *lock = url;
        }

        // Setup an internal memory buffer to decouple the "Write" and "Read" phases of the benchmark.
        // This allows the benchmark to finish writing all messages without deadlocking on the reader.
        let topic = format!("http_perf_buffer_{}", fast_uuid_v7::gen_id());
        let mem_config = mq_bridge::models::MemoryConfig {
            topic,
            capacity: Some(super::PERF_TEST_MESSAGE_COUNT * 10),
            ..Default::default()
        };
        let mem_publisher = MemoryPublisher::new(&mem_config).unwrap();
        let mem_consumer = MemoryConsumer::new(&mem_config).unwrap();

        // Background task to bridge Http -> Memory.
        // We only ACK the HTTP request once the message is safely accepted by the memory queue.
        tokio::spawn(async move {
            while let Ok(batch) = http_consumer.receive_batch(100).await {
                let count = batch.messages.len();
                if count > 0 && mem_publisher.send_batch(batch.messages).await.is_ok() {
                    let _ = (batch.commit)(vec![MessageDisposition::Ack; count]).await;
                }
            }
        });

        Arc::new(Mutex::new(mem_consumer))
    }

    pub async fn create_publisher() -> Arc<dyn MessagePublisher> {
        #[cfg(feature = "rustls")]
        crate::ensure_rustls_installed();
        let url = {
            let lock = CURRENT_URL.lock().unwrap();
            lock.clone()
        };
        let config = HttpConfig {
            url,
            request_timeout_ms: Some(10000),
            pool_idle_timeout_ms: Some(1000),
            tcp_keepalive_ms: Some(1000),
            ..Default::default()
        };
        Arc::new(HttpPublisher::new(&config).await.unwrap())
    }
}

#[cfg(feature = "websocket")]
pub mod websocket_helper {
    use mq_bridge::endpoints::websocket::{WebSocketConsumer, WebSocketPublisher};
    use mq_bridge::models::WebSocketConfig;
    use mq_bridge::traits::{MessageConsumer, MessagePublisher};
    use once_cell::sync::Lazy;
    use std::sync::Arc;
    use std::sync::Mutex as StdMutex;
    use tokio::sync::Mutex;

    static CURRENT_URL: Lazy<StdMutex<String>> = Lazy::new(|| StdMutex::new(String::new()));

    pub async fn create_consumer() -> Arc<Mutex<dyn MessageConsumer>> {
        let websocket_config = WebSocketConfig {
            url: "127.0.0.1:0".to_string(),
            path: Some("/bench".to_string()),
            routed_queue_capacity: Some(super::PERF_TEST_MESSAGE_COUNT * 2),
            ..Default::default()
        };

        let websocket_consumer = WebSocketConsumer::new(&websocket_config)
            .await
            .expect("Failed to create WebSocketConsumer");
        let url = websocket_consumer.url().to_string();

        {
            let mut lock = CURRENT_URL.lock().unwrap();
            *lock = url;
        }

        Arc::new(Mutex::new(websocket_consumer))
    }

    pub async fn create_publisher() -> Arc<dyn MessagePublisher> {
        let url = {
            let lock = CURRENT_URL.lock().unwrap();
            lock.clone()
        };
        Arc::new(WebSocketPublisher::new(&WebSocketConfig::new(url)))
    }
}

#[cfg(feature = "grpc")]
pub mod grpc_helper {
    use mq_bridge::endpoints::grpc::{GrpcConsumer, GrpcPublisher};
    use mq_bridge::models::GrpcConfig;
    use mq_bridge::traits::{MessageConsumer, MessagePublisher};
    use once_cell::sync::Lazy;
    use std::net::TcpListener;
    use std::sync::{Arc, Mutex as StdMutex};
    use tokio::sync::Mutex;

    static CURRENT_URL: Lazy<StdMutex<String>> = Lazy::new(|| StdMutex::new(String::new()));

    fn get_free_listener() -> std::net::TcpListener {
        TcpListener::bind("127.0.0.1:0").unwrap()
    }

    pub async fn create_consumer_with_mode(server_mode: bool) -> Arc<Mutex<dyn MessageConsumer>> {
        #[cfg(feature = "rustls")]
        crate::ensure_rustls_installed();
        let std_listener = get_free_listener();
        let port = std_listener.local_addr().unwrap().port();
        let addr = format!("127.0.0.1:{}", port);
        let url = format!("http://{}", addr);

        {
            let mut lock = CURRENT_URL.lock().unwrap();
            *lock = url.clone();
        }

        let config = GrpcConfig {
            url: url.clone(),
            server_mode,
            ..Default::default()
        };

        // If running client-mode (server_mode == false), spawn a lightweight in-process
        // Bridge server so the client can connect. This mirrors the embedded server used
        // in production endpoints but is self-contained for benches.
        if !server_mode {
            // spawn a simple bridge server that fans published messages to subscribers
            // Move the reserved std listener into the spawn so the socket stays reserved
            // until the server takes ownership.
            let std_listener = std_listener;
            tokio::spawn(async move {
                use mq_bridge::endpoints::grpc::proto;
                use proto::{BridgeMessage, PublishResponse};
                use tokio::sync::{broadcast, mpsc};
                use tokio_stream::wrappers::ReceiverStream;
                use tonic::{Request, Response, Status};

                struct BenchBridge {
                    tx: broadcast::Sender<BridgeMessage>,
                    queue_tx: mpsc::Sender<BridgeMessage>,
                }

                #[tonic::async_trait]
                impl proto::bridge_server::Bridge for BenchBridge {
                    async fn publish(
                        &self,
                        request: Request<BridgeMessage>,
                    ) -> Result<Response<PublishResponse>, Status> {
                        let msg = request.into_inner();
                        let _ = self.tx.send(msg.clone());
                        let _ = self.queue_tx.send(msg.clone()).await;
                        Ok(Response::new(PublishResponse {
                            result: Some(proto::publish_response::Result::Ack(proto::Ack {
                                id: msg.id,
                                status: 0,
                                reason: String::new(),
                                metadata: Default::default(),
                            })),
                        }))
                    }

                    type PublishBatchStream = ReceiverStream<Result<PublishResponse, Status>>;

                    async fn publish_batch(
                        &self,
                        request: Request<tonic::Streaming<BridgeMessage>>,
                    ) -> Result<Response<Self::PublishBatchStream>, Status> {
                        let mut stream = request.into_inner();
                        let (tx, rx) = mpsc::channel(32);
                        let sender = self.tx.clone();
                        let queue_tx = self.queue_tx.clone();

                        tokio::spawn(async move {
                            while let Ok(Some(msg)) = stream.message().await {
                                let id = msg.id.clone();
                                let _ = sender.send(msg.clone());
                                let _ = queue_tx.send(msg).await;
                                let resp = PublishResponse {
                                    result: Some(proto::publish_response::Result::Ack(
                                        proto::Ack {
                                            id,
                                            status: 0,
                                            reason: String::new(),
                                            metadata: Default::default(),
                                        },
                                    )),
                                };
                                if tx.send(Ok(resp)).await.is_err() {
                                    break;
                                }
                            }
                        });

                        Ok(Response::new(ReceiverStream::new(rx)))
                    }

                    type SubscribeStream = ReceiverStream<Result<BridgeMessage, Status>>;

                    async fn subscribe(
                        &self,
                        request: Request<proto::SubscribeRequest>,
                    ) -> Result<Response<Self::SubscribeStream>, Status> {
                        let topic = request.get_ref().topic.clone();
                        let mut rx = self.tx.subscribe();
                        let (tx_stream, rx_stream) = mpsc::channel(32);
                        tokio::spawn(async move {
                            loop {
                                match rx.recv().await {
                                    Ok(msg) => {
                                        let msg_topic = msg
                                            .metadata
                                            .iter()
                                            .find(|(k, _)| k.as_str() == "mq_bridge.topic")
                                            .map(|(_, v)| v.clone());
                                        if !topic.is_empty()
                                            && msg_topic.as_deref() != Some(topic.as_str())
                                        {
                                            continue;
                                        }
                                        if tx_stream.send(Ok(msg)).await.is_err() {
                                            break;
                                        }
                                    }
                                    Err(broadcast::error::RecvError::Lagged(_)) => continue,
                                    Err(broadcast::error::RecvError::Closed) => break,
                                }
                            }
                        });
                        Ok(Response::new(ReceiverStream::new(rx_stream)))
                    }

                    async fn acknowledge(
                        &self,
                        _request: Request<proto::Ack>,
                    ) -> Result<Response<proto::AckResponse>, Status> {
                        Ok(Response::new(proto::AckResponse {
                            success: true,
                            error: String::new(),
                        }))
                    }
                }

                let (tx, _) = broadcast::channel(1024);
                let (queue_tx, _rx) = mpsc::channel(16 * 1024);
                // Drain queue receiver so the send side never blocks and causes
                // backpressure that stalls publish_batch handlers during benches.
                let mut queue_rx = _rx;
                tokio::spawn(async move {
                    while let Some(_msg) = queue_rx.recv().await {
                        // intentionally drop messages
                    }
                });
                let service = BenchBridge { tx, queue_tx };
                // Convert the reserved std listener to a non-blocking tokio listener
                std_listener.set_nonblocking(true).ok();
                let listener = tokio::net::TcpListener::from_std(std_listener)
                    .expect("Failed to convert std listener to tokio listener");
                let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
                let svc = proto::bridge_server::BridgeServer::new(service);
                if let Err(e) = tonic::transport::Server::builder()
                    .serve_with_incoming(svc, incoming)
                    .await
                {
                    eprintln!("bench bridge server error: {:?}", e);
                }
            });
            // give server a small moment to bind
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        } else {
            // Release the reserved port so ServerModeConsumer::new can bind to it.
            drop(std_listener);
        }

        let cons = GrpcConsumer::new(&config).await.unwrap();
        // Allow server (if started) to stabilize before clients connect.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // Spawn diagnostics for gRPC helper so CI logs include FD counts and URL.
        let diag_url = url.clone();
        tokio::spawn(async move {
            let pid = std::process::id();
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                // compute fd count efficiently if possible
                let fd_count: Option<usize> = if let Ok(rd) = std::fs::read_dir("/proc/self/fd") {
                    Some(rd.count())
                } else {
                    None
                };

                println!(
                    "BENCH-DIAG-GRPC pid={} url={} fd_count={:?}",
                    pid, diag_url, fd_count
                );
            }
        });
        Arc::new(Mutex::new(cons))
    }

    pub async fn create_consumer() -> Arc<Mutex<dyn MessageConsumer>> {
        create_consumer_with_mode(false).await
    }

    pub async fn create_publisher() -> Arc<dyn MessagePublisher> {
        let url = {
            let lock = CURRENT_URL.lock().unwrap();
            lock.clone()
        };
        let config = GrpcConfig {
            url,
            ..Default::default()
        };
        Arc::new(GrpcPublisher::new(&config).await.unwrap())
    }
}

#[cfg(feature = "grpc")]
pub mod grpc_server_helper {
    use super::grpc_helper;
    use mq_bridge::traits::{MessageConsumer, MessagePublisher};
    use std::sync::Arc;
    use tokio::sync::Mutex;

    pub async fn create_consumer() -> Arc<Mutex<dyn MessageConsumer>> {
        // Server-mode consumer
        grpc_helper::create_consumer_with_mode(true).await
    }

    pub async fn create_publisher() -> Arc<dyn MessagePublisher> {
        grpc_helper::create_publisher().await
    }
}

fn performance_benchmarks(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    let mut group = c.benchmark_group("performance");
    // Since these are integration tests involving network/disk, we reduce sample size
    // and increase measurement time to accommodate their duration.
    group.sample_size(10);
    group.throughput(Throughput::Elements(PERF_TEST_MESSAGE_COUNT as u64));
    group.measurement_time(Duration::from_secs(10));
    group.warm_up_time(Duration::from_secs(1));

    bench_backend!(
        "aws",
        "aws",
        "tests/integration/docker-compose/aws.yml",
        aws_helper,
        group,
        &rt,
        &BENCH_RESULTS,
        PERF_TEST_MESSAGE_COUNT,
        PERF_TEST_CONCURRENCY,
        std::time::Duration::from_millis(100)
    );
    bench_backend!(
        "kafka",
        "kafka",
        "tests/integration/docker-compose/kafka.yml",
        kafka_helper,
        group,
        &rt,
        &BENCH_RESULTS,
        PERF_TEST_MESSAGE_COUNT,
        PERF_TEST_CONCURRENCY,
        std::time::Duration::from_millis(200)
    );
    bench_backend!(
        "amqp",
        "amqp",
        "tests/integration/docker-compose/amqp.yml",
        amqp_helper,
        group,
        &rt,
        &BENCH_RESULTS,
        PERF_TEST_MESSAGE_COUNT,
        PERF_TEST_CONCURRENCY,
        std::time::Duration::from_millis(100)
    );
    bench_backend!(
        "nats",
        "nats",
        "tests/integration/docker-compose/nats.yml",
        nats_helper,
        group,
        &rt,
        &BENCH_RESULTS,
        PERF_TEST_MESSAGE_COUNT,
        PERF_TEST_CONCURRENCY,
        std::time::Duration::from_millis(100)
    );
    bench_backend!(
        "mongodb",
        "mongodb",
        "tests/integration/docker-compose/mongodb.yml",
        mongodb_helper,
        group,
        &rt,
        &BENCH_RESULTS,
        PERF_TEST_MESSAGE_COUNT,
        PERF_TEST_CONCURRENCY,
        std::time::Duration::from_millis(100)
    );
    bench_backend!(
        "mongodb",
        "mongodb_subscriber",
        "tests/integration/docker-compose/mongodb.yml",
        mongodb_subscriber_helper,
        group,
        &rt,
        &BENCH_RESULTS,
        PERF_TEST_MESSAGE_COUNT,
        PERF_TEST_CONCURRENCY,
        std::time::Duration::from_millis(100)
    );
    bench_backend!(
        "sqlx",
        "postgres",
        "tests/integration/docker-compose/postgres.yml",
        sqlx_helper,
        group,
        &rt,
        &BENCH_RESULTS,
        PERF_TEST_MESSAGE_COUNT,
        PERF_TEST_CONCURRENCY,
        std::time::Duration::from_millis(100)
    );
    bench_backend!(
        "mqtt",
        "mqtt",
        "tests/integration/docker-compose/mqtt_performance.yml",
        mqtt_helper,
        group,
        &rt,
        &BENCH_RESULTS,
        PERF_TEST_MESSAGE_COUNT,
        PERF_TEST_CONCURRENCY,
        std::time::Duration::from_millis(150)
    );

    bench_backend!(
        "zeromq",
        "zeromq",
        zeromq_helper,
        group,
        &rt,
        &BENCH_RESULTS,
        PERF_TEST_MESSAGE_COUNT,
        PERF_TEST_CONCURRENCY,
        std::time::Duration::from_millis(10)
    );
    #[cfg(any(feature = "ibm-mq-static", feature = "ibm-mq"))]
    {
        // Skip when the IBM MQ client library isn't loadable (dlopen build with no
        // client installed); otherwise the helper would panic on first connect.
        if mq_bridge::endpoints::ibm_mq::ibm_mq_client_available() {
            bench_backend!(
                "",
                "ibm-mq",
                "tests/integration/docker-compose/ibm_mq.yml",
                ibm_mq_helper,
                group,
                &rt,
                &BENCH_RESULTS,
                PERF_TEST_MESSAGE_COUNT,
                PERF_TEST_CONCURRENCY,
                std::time::Duration::from_millis(100)
            );
        } else {
            eprintln!(
                "Skipping IBM MQ benchmark: client library not found. \
                 Install the IBM MQ redistributable client or set MQB_IBM_MQ_LIB."
            );
        }
    }
    bench_backend!(
        "memory",
        memory_helper,
        group,
        &rt,
        &BENCH_RESULTS,
        PERF_TEST_MESSAGE_COUNT,
        PERF_TEST_CONCURRENCY,
        std::time::Duration::from_millis(1)
    );
    bench_backend!(
        "memory_subscriber",
        memory_subscriber_helper,
        group,
        &rt,
        &BENCH_RESULTS,
        PERF_TEST_MESSAGE_COUNT,
        PERF_TEST_CONCURRENCY,
        std::time::Duration::from_millis(1)
    );
    bench_backend!(
        "file_delete",
        file_delete_helper,
        group,
        &rt,
        &BENCH_RESULTS,
        PERF_TEST_MESSAGE_COUNT,
        PERF_TEST_CONCURRENCY,
        std::time::Duration::from_millis(1000)
    );
    bench_backend!(
        "file",
        file_helper,
        group,
        &rt,
        &BENCH_RESULTS,
        PERF_TEST_MESSAGE_COUNT,
        PERF_TEST_CONCURRENCY,
        std::time::Duration::from_millis(1000)
    );
    bench_backend!(
        "http",
        "http",
        http_helper,
        group,
        &rt,
        &BENCH_RESULTS,
        PERF_TEST_MESSAGE_COUNT,
        PERF_TEST_CONCURRENCY,
        std::time::Duration::from_millis(20)
    );
    bench_backend!(
        "websocket",
        "websocket",
        websocket_helper,
        group,
        &rt,
        &BENCH_RESULTS,
        PERF_TEST_MESSAGE_COUNT,
        PERF_TEST_CONCURRENCY,
        std::time::Duration::from_millis(20)
    );
    bench_backend!(
        "grpc",
        "grpc",
        grpc_helper,
        group,
        &rt,
        &BENCH_RESULTS,
        PERF_TEST_MESSAGE_COUNT,
        PERF_TEST_CONCURRENCY,
        std::time::Duration::from_millis(20)
    );
    bench_backend!(
        "grpc",
        "grpc_server",
        grpc_server_helper,
        group,
        &rt,
        &BENCH_RESULTS,
        PERF_TEST_MESSAGE_COUNT,
        PERF_TEST_CONCURRENCY,
        std::time::Duration::from_millis(20)
    );
    // Print consolidated results
    let results = BENCH_RESULTS.blocking_lock();
    print_benchmark_results(&results, PERF_TEST_MESSAGE_COUNT);
    group.finish();
}

criterion_group!(benches, performance_benchmarks);
criterion_main!(benches);
