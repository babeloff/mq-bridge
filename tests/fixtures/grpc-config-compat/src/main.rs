use mq_bridge::models::{GrpcConfig, TlsConfig};
use std::collections::HashMap;

fn main() {
    let _config = GrpcConfig {
        url: "https://localhost:50051".to_string(),
        topic: None,
        consumer_id: None,
        timeout_ms: None,
        connect_timeout_ms: None,
        request_timeout_ms: None,
        idle_stream_timeout_ms: None,
        overall_timeout_ms: None,
        tls: TlsConfig::default(),
        server_mode: false,
        initial_stream_window_size: None,
        initial_connection_window_size: None,
        concurrency_limit_per_connection: None,
        http2_keepalive_interval_ms: None,
        http2_keepalive_timeout_ms: None,
        max_decoding_message_size: None,
        max_encoding_message_size: None,
        descriptor_set_path: None,
        descriptor_set_bytes: None,
        reflection: false,
        service_name: None,
        method_name: None,
        request: None,
        server_streaming: false,
        metadata: HashMap::new(),
        binary_metadata: HashMap::new(),
        bearer_token: None,
        api_key: None,
        api_key_name: None,
        shared: None,
    };
}
