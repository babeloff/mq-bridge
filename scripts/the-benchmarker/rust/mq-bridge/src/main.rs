//! the-benchmarker/web-frameworks server for mq-bridge (Rust).
//!
//! Implements the three routes the benchmark requires on `0.0.0.0:3000`:
//!
//! | Method | Path        | Response          |
//! |--------|-------------|-------------------|
//! | GET    | `/`         | 200, empty body   |
//! | GET    | `/user/:id` | 200, the `id`     |
//! | POST   | `/user`     | 200, empty body   |
//!
//! Design notes
//! ------------
//! * mq-bridge's HTTP path filter is an exact match, so it can't express the
//!   `/user/:id` path parameter. We instead use a single catch-all
//!   `http -> response` route and dispatch in the handler on the `http_method`
//!   and `http_path` request metadata, extracting `:id` as the suffix after
//!   `/user/`.
//! * The route uses the inline-response fast path: HTTP framing runs on the
//!   Tokio multi-thread runtime, one task per connection, bypassing the route
//!   worker/disposition pipeline.

use mq_bridge::endpoints::http::{HttpRequestExt, HTTP_STATUS_CODE};
use mq_bridge::models::{Endpoint, EndpointType, HttpConfig};
use mq_bridge::{CanonicalMessage, Handled, HandlerError, Route};

const USER_PREFIX: &str = "/user/";

fn empty_200() -> CanonicalMessage {
    // No body, default 200. A fresh message so no request headers are echoed.
    CanonicalMessage::new(Vec::new(), None)
}

fn text_200(body: Vec<u8>) -> CanonicalMessage {
    CanonicalMessage::new(body, None).with_metadata_kv("content-type", "text/plain")
}

fn not_found() -> CanonicalMessage {
    text_200(b"Not Found".to_vec()).with_metadata_kv(HTTP_STATUS_CODE, "404")
}

async fn handle(msg: CanonicalMessage) -> Result<Handled, HandlerError> {
    let reply = match (msg.http_method(), msg.http_path()) {
        ("GET", "/") => empty_200(),
        ("POST", "/user") => empty_200(),
        ("GET", p) => match p.strip_prefix(USER_PREFIX) {
            // GET /user/:id -> echo the id segment as the body.
            Some(id) if !id.is_empty() && !id.contains('/') => text_200(id.as_bytes().to_vec()),
            _ => not_found(),
        },
        _ => not_found(),
    };

    Ok(Handled::Publish(reply))
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let listen = std::env::var("MQB_LISTEN").unwrap_or_else(|_| "0.0.0.0:3000".to_string());

    // No method/path filter: every request reaches the handler, which routes it.
    let mut http = HttpConfig::new(listen)
        .with_inline_response_fast_path(true);
    http.concurrency_limit = Some(65_536);
    http.internal_buffer_size = Some(16_384);

    let input = Endpoint::new(EndpointType::Http(http));
    let output = Endpoint::new_response();

    let route = Route::new(input, output).with_handler(|msg| handle(msg));
    let handle = route.run("the-benchmarker").await?;
    handle.join().await?;
    Ok(())
}
