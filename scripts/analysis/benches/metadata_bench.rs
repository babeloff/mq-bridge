use std::collections::HashMap;
use std::hint::black_box;

use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use mq_bridge::CanonicalMessage;

const HTTP_HEADERS: &[(&str, &str)] = &[
    ("content-type", "application/json"),
    ("accept", "application/octet-stream"),
    ("user-agent", "mq-bridge-bench/1.0"),
    ("host", "127.0.0.1:18080"),
    ("connection", "keep-alive"),
    ("kind", "bench.tick"),
    ("x-request-id", "018f3f5b-7cc0-7a65-8c23-123456789abc"),
    ("x-correlation-id", "018f3f5b-7cc0-7a65-8c23-abcdef012345"),
];

fn insert_http_metadata_std() -> HashMap<String, String> {
    let mut metadata = HashMap::with_capacity(HTTP_HEADERS.len() + 4);
    metadata.insert("http_method".to_string(), "POST".to_string());
    metadata.insert("http_path".to_string(), "/bench".to_string());
    metadata.insert("http_query".to_string(), "".to_string());
    metadata.insert("http_version".to_string(), "HTTP/1.1".to_string());
    for (key, value) in HTTP_HEADERS {
        metadata.insert((*key).to_string(), (*value).to_string());
    }
    metadata
}

fn make_message_with_metadata() -> CanonicalMessage {
    CanonicalMessage::new_bytes(br#"{"value":0}"#.to_vec().into(), None)
        .with_metadata(insert_http_metadata_std())
}

fn metadata_benchmarks(c: &mut Criterion) {
    let mut group = c.benchmark_group("metadata");
    group.throughput(Throughput::Elements(1));

    group.bench_function("build_http_metadata_std_hashmap", |b| {
        b.iter(|| black_box(insert_http_metadata_std()))
    });

    group.bench_function("build_canonical_message_with_metadata", |b| {
        b.iter(|| black_box(make_message_with_metadata()))
    });
    group.finish();
}

criterion_group!(benches, metadata_benchmarks);
criterion_main!(benches);
