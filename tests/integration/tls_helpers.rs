#![allow(dead_code)]

use anyhow::{Context, Result};
use std::path::{Path, PathBuf};
use std::process::Command;

/// Generate certs for a named service ("mongodb", "kafka", or "ibm-mq") using the shared script.
pub fn generate_service_certs(service: &str) -> Result<PathBuf> {
    let script = PathBuf::from("tests/integration/scripts/gen_certs.sh");
    if !script.exists() {
        anyhow::bail!("Cert script not found: {}", script.display());
    }

    // Prefer to run the script with `bash` since it uses bash-specific features.
    // Fall back to `sh` if `bash` is not available.
    let bash_status = Command::new("bash").arg(&script).arg(service).status();

    let status = match bash_status {
        Ok(s) => s,
        Err(_) => Command::new("sh")
            .arg(&script)
            .arg(service)
            .status()
            .with_context(|| format!("Failed to run cert script: {}", script.display()))?,
    };

    if !status.success() {
        anyhow::bail!("Cert script failed for {}", service);
    }

    let out = match service {
        "mongodb" => PathBuf::from("tests/integration/docker-compose/certs"),
        "kafka" => PathBuf::from("tests/integration/docker-compose/kafka-certs"),
        "ibm-mq" => PathBuf::from("tests/integration/docker-compose/ibm-mq-certs"),
        "http" => PathBuf::from("tests/integration/docker-compose/http-certs"),
        "grpc" => PathBuf::from("tests/integration/docker-compose/grpc-certs"),
        _ => unreachable!(),
    };
    Ok(out)
}

/// Build a `MongoDbConfig` with TLS enabled, pointing to `ca.pem` in `cert_dir`.
pub fn mongo_config_with_tls(
    cert_dir: &Path,
    database: impl Into<String>,
    collection: impl Into<String>,
) -> mq_bridge::models::MongoDbConfig {
    let ca = cert_dir.join("ca.pem");
    let tls = mq_bridge::models::TlsConfig::new().with_ca_file(ca.to_string_lossy());
    mq_bridge::models::MongoDbConfig {
        url: "mongodb://localhost:27017".to_string(),
        database: database.into(),
        collection: Some(collection.into()),
        tls,
        ..Default::default()
    }
}

/// Build a `KafkaConfig` with TLS enabled, pointing to `ca.pem` in `cert_dir` and the given topic.
pub fn kafka_config_with_tls(
    cert_dir: &Path,
    topic: impl Into<String>,
) -> mq_bridge::models::KafkaConfig {
    let ca = cert_dir.join("ca.pem");
    let mut cfg = mq_bridge::models::KafkaConfig::new("localhost:9093");
    cfg.topic = Some(topic.into());
    cfg.tls = mq_bridge::models::TlsConfig::new().with_ca_file(ca.to_string_lossy());
    cfg
}

/// Build an `IbmMqConfig` with TLS enabled, pointing to `ca.pem` and server certs in `cert_dir`.
pub fn ibm_mq_config_with_tls(
    cert_dir: &Path,
    queue_manager: impl Into<String>,
    channel: impl Into<String>,
) -> mq_bridge::models::IbmMqConfig {
    let ca = cert_dir.join("ca.pem");
    let mut cfg = mq_bridge::models::IbmMqConfig::new(
        "localhost(1414)",
        queue_manager.into(),
        channel.into(),
    );
    // The current IBM MQ client path accepts an MQ key repository, not a PEM CA bundle.
    // For this integration test, use TLS while skipping server certificate validation.
    cfg.tls = mq_bridge::models::TlsConfig::new()
        .with_ca_file(ca.to_string_lossy())
        .with_insecure(true);
    cfg.cipher_spec = Some("ANY_TLS12".to_string());
    cfg
}

/// Build an `HttpConfig` for a TLS-enabled server (consumer) bound to `addr`.
pub fn http_consumer_config_with_tls(
    cert_dir: &Path,
    addr: impl Into<String>,
) -> mq_bridge::models::HttpConfig {
    let (cert, key) = choose_cert_and_key(cert_dir);
    let mut cfg = mq_bridge::models::HttpConfig::new(addr.into());
    cfg.tls = mq_bridge::models::TlsConfig::new()
        .with_client_cert(cert.to_string_lossy(), key.to_string_lossy())
        .with_insecure(false);
    cfg
}

/// Build an `HttpConfig` for a TLS-enabled client (publisher) targeting `url` and trusting `ca.pem`.
pub fn http_publisher_config_with_tls(
    cert_dir: &Path,
    url: impl Into<String>,
) -> mq_bridge::models::HttpConfig {
    let ca = cert_dir.join("ca.pem");
    let mut cfg = mq_bridge::models::HttpConfig::new(url.into());
    cfg.tls = mq_bridge::models::TlsConfig::new().with_ca_file(ca.to_string_lossy());
    cfg
}

/// Build `GrpcConfig` helpers: server (with cert/key) and client (with CA).
pub fn grpc_server_config_with_tls(
    cert_dir: &Path,
    url: impl Into<String>,
) -> mq_bridge::models::GrpcConfig {
    let (cert, key) = choose_cert_and_key(cert_dir);
    let mut cfg = mq_bridge::models::GrpcConfig::new(url.into());
    cfg.server_mode = true;
    cfg.tls = mq_bridge::models::TlsConfig::new()
        .with_client_cert(cert.to_string_lossy(), key.to_string_lossy())
        .with_insecure(false);
    cfg
}

fn choose_cert_and_key(cert_dir: &Path) -> (PathBuf, PathBuf) {
    let server_crt = cert_dir.join("server.crt");
    let server_key = cert_dir.join("server.key");
    (server_crt, server_key)
}

pub fn grpc_client_config_with_tls(
    cert_dir: &Path,
    url: impl Into<String>,
) -> mq_bridge::models::GrpcConfig {
    let ca = cert_dir.join("ca.pem");
    let mut cfg = mq_bridge::models::GrpcConfig::new(url.into());
    cfg.tls = mq_bridge::models::TlsConfig::new().with_ca_file(ca.to_string_lossy());
    cfg
}
