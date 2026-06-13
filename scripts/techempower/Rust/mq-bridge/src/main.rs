//! TechEmpower benchmark server for mq-bridge (Rust).
//!
//! Serves the JSON, Plaintext, and (optionally) the Postgres database tests on
//! `0.0.0.0:8080` through a single `http -> response` route that uses
//! mq-bridge's inline-response fast path.
//!
//! Endpoints
//! ---------
//! * `GET /json`              -> `{"message":"Hello, World!"}` (serialized per request)
//! * `GET /plaintext`         -> `Hello, World!`
//! * `GET /db`                -> one random `World` row as JSON   (Single Query)
//! * `GET /queries?queries=N` -> N random `World` rows as a JSON array (Multiple Queries)
//!
//! Design notes
//! ------------
//! * One route, no path filter. The handler dispatches on the request's
//!   `http_path` metadata, so both/all endpoints share one listener/port.
//! * The inline fast path bypasses the route worker/disposition pipeline, so the
//!   route `concurrency`/`batch_size` knobs do not gate the hot path; per-request
//!   parallelism comes from the HTTP server spawning a task per connection.
//! * Database access. mq-bridge's `sqlx` *endpoint* models a table as a message
//!   queue (INSERT publisher / polling SELECT consumer), not per-request
//!   request-reply, so it does not fit TechEmpower's random-id `SELECT`. We
//!   instead own an `sqlx::PgPool` directly in the handler and run the query
//!   there. The pool is optional: without `DATABASE_URL` the DB routes return
//!   503, so `/json` and `/plaintext` remain runnable without Postgres.
//! * Response headers: returned-message metadata becomes response headers and
//!   `http_status_code` sets the status. hyper adds `Date`/`Content-Length`.

use mq_bridge::models::{Endpoint, EndpointType, HttpConfig, ResponseConfig};
use mq_bridge::{CanonicalMessage, Handled, HandlerError, Route};
use serde::Serialize;
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;

const SERVER: &str = "mq-bridge";
const WORLD_ROWS: i32 = 10_000;
const MAX_QUERIES: i64 = 500;

#[derive(Serialize)]
struct JsonMessage {
    message: &'static str,
}

#[derive(Serialize)]
struct World {
    id: i32,
    #[serde(rename = "randomNumber")]
    random_number: i32,
}

fn random_id() -> i32 {
    fastrand::i32(1..=WORLD_ROWS)
}

fn non_retryable(e: impl std::fmt::Display) -> HandlerError {
    HandlerError::NonRetryable(anyhow::anyhow!("{e}"))
}

fn reply(body: Vec<u8>, content_type: &str) -> CanonicalMessage {
    CanonicalMessage::new(body, None)
        .with_metadata_kv("content-type", content_type)
        .with_metadata_kv("Server", SERVER)
}

fn error_reply(status: u16, body: &str) -> CanonicalMessage {
    reply(body.as_bytes().to_vec(), "text/plain")
        .with_metadata_kv("http_status_code", status.to_string())
}

/// Parse the `queries` parameter from a raw query string, clamped to 1..=500.
fn parse_queries(query: &str) -> i64 {
    query
        .split('&')
        .find_map(|pair| pair.strip_prefix("queries="))
        .and_then(|v| v.parse::<i64>().ok())
        .unwrap_or(1)
        .clamp(1, MAX_QUERIES)
}

async fn fetch_world(pool: &PgPool) -> Result<World, sqlx::Error> {
    let (id, random_number): (i32, i32) =
        sqlx::query_as("SELECT id, randomnumber FROM world WHERE id = $1")
            .bind(random_id())
            .fetch_one(pool)
            .await?;
    Ok(World { id, random_number })
}

async fn handle_request(pool: Option<PgPool>, msg: CanonicalMessage) -> Result<Handled, HandlerError> {
    let path = msg.metadata.get("http_path").map(String::as_str).unwrap_or("");

    let reply = match path {
        "/json" => {
            // TechEmpower requires per-request serialization (no pre-rendered string).
            let body = serde_json::to_vec(&JsonMessage { message: "Hello, World!" })
                .map_err(non_retryable)?;
            reply(body, "application/json")
        }
        "/plaintext" => reply(b"Hello, World!".to_vec(), "text/plain"),
        "/db" => match pool {
            None => error_reply(503, "DATABASE_URL not configured"),
            Some(pool) => {
                let world = fetch_world(&pool).await.map_err(non_retryable)?;
                reply(serde_json::to_vec(&world).map_err(non_retryable)?, "application/json")
            }
        },
        "/queries" => match pool {
            None => error_reply(503, "DATABASE_URL not configured"),
            Some(pool) => {
                let n = parse_queries(msg.metadata.get("http_query").map(String::as_str).unwrap_or(""));
                let mut worlds = Vec::with_capacity(n as usize);
                for _ in 0..n {
                    worlds.push(fetch_world(&pool).await.map_err(non_retryable)?);
                }
                reply(serde_json::to_vec(&worlds).map_err(non_retryable)?, "application/json")
            }
        },
        _ => error_reply(404, "Not Found"),
    };

    Ok(Handled::Publish(reply))
}

fn endpoint(endpoint_type: EndpointType) -> Endpoint {
    Endpoint {
        middlewares: Vec::new(),
        endpoint_type,
        handler: None,
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let listen = std::env::var("MQB_LISTEN").unwrap_or_else(|_| "0.0.0.0:8080".to_string());

    // Optional Postgres pool for the /db and /queries tests. A connection
    // failure is non-fatal: the server still serves /json and /plaintext, and
    // the DB routes return 503 until the database is reachable.
    let pool = match std::env::var("DATABASE_URL") {
        Ok(url) if !url.is_empty() => {
            let max = std::env::var("DB_MAX_CONNECTIONS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(56);
            match PgPoolOptions::new().max_connections(max).connect(&url).await {
                Ok(pool) => {
                    eprintln!("connected to Postgres (max_connections={max})");
                    Some(pool)
                }
                Err(e) => {
                    eprintln!("Postgres connection failed ({e}); /db and /queries will return 503");
                    None
                }
            }
        }
        _ => {
            eprintln!("DATABASE_URL not set; /db and /queries will return 503");
            None
        }
    };

    let mut http = HttpConfig::new(listen);
    http.method = Some("GET".to_string());
    // Raise above the expected max connection count so we never throttle.
    http.concurrency_limit = Some(65_536);
    http.internal_buffer_size = Some(16_384);
    http.inline_response_fast_path = Some(true);

    let input = endpoint(EndpointType::Http(http));
    let output = endpoint(EndpointType::Response(ResponseConfig {}));

    let handler = move |msg: CanonicalMessage| handle_request(pool.clone(), msg);
    let route = Route::new(input, output).with_handler(handler);
    let handle = route.run("techempower").await?;
    handle.join().await?;
    Ok(())
}
