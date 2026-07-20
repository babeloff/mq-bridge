//  mq-bridge
//  © Copyright 2025, by Marco Mengelkoch
//  Licensed under MIT License, see License file for more details
//  git clone https://github.com/marcomq/mq-bridge
pub mod canonical_message;
#[cfg(any(
    feature = "mongodb",
    feature = "sqlx",
    feature = "clickhouse",
    feature = "object-store"
))]
pub mod checkpoint;
pub mod command_handler;
pub mod connection_registry;
pub mod endpoints;
pub mod errors;
pub mod event_handler;
pub mod event_store;
pub mod extensions;
pub mod interpolation;
pub mod middleware;
pub mod models;
pub mod outcomes;
pub mod publisher;
pub mod response;
pub mod route;
#[cfg(feature = "test-utils")]
pub mod test_utils;
pub mod traits;
pub mod type_handler;

pub use anyhow;
pub use canonical_message::{CanonicalMessage, MessageContext};
pub use errors::HandlerError;
pub use models::Route;
pub use outcomes::{Handled, Received, ReceivedBatch, Sent, SentBatch};
pub use publisher::Publisher;

pub use endpoints::memory::get_or_create_channel;
pub use publisher::{get_publisher, list_publishers, register_publisher, unregister_publisher};
pub use route::{get_route, list_routes, register_endpoint, stop_route};

// Re-export the underlying driver crate for each feature-gated endpoint, so
// downstream code can depend on the exact same version mq-bridge builds against
// (and share types with it) without adding — and keeping in sync — its own
// dependency entry. Each is gated on the feature that pulls the crate in.
#[cfg(feature = "nats")]
pub use async_nats;
#[cfg(feature = "amqp")]
pub use lapin;
#[cfg(feature = "mongodb")]
pub use mongodb;
#[cfg(feature = "kafka")]
pub use rdkafka;
#[cfg(feature = "redis-streams")]
pub use redis;
#[cfg(feature = "clickhouse")]
pub use reqwest;
#[cfg(feature = "mqtt")]
pub use rumqttc;
#[cfg(feature = "websocket")]
pub use tokio_websockets;
#[cfg(feature = "zeromq")]
pub use zeromq;
#[cfg(feature = "aws")]
pub use {aws_config, aws_sdk_sns, aws_sdk_sqs};
#[cfg(feature = "grpc")]
pub use {prost, tonic};
// `sqlx` is also enabled transitively by `postgres-cdc`; the integration tests
// use this re-export instead of a duplicate dev-dependency.
#[cfg(any(feature = "ibm-mq", feature = "ibm-mq-static"))]
pub use mqi;
#[cfg(feature = "postgres-cdc")]
pub use pgwire_replication;
#[cfg(feature = "sqlx")]
pub use sqlx;

pub mod consumer {
    pub use crate::middleware::apply_middlewares_to_consumer as apply_middlewares;
}

/// The application name, derived from the package name in Cargo.toml.
pub const APP_NAME: &str = env!("CARGO_PKG_NAME");
