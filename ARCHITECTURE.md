# Architecture Overview

`mq-bridge` is designed as a highly extensible, protocol-agnostic message integration layer for Rust. Its architecture enables seamless bridging between diverse messaging systems, databases, and protocols, while allowing users to inject custom business logic and reliability patterns.

## Core Principles
- **Protocol Abstraction:** All business logic operates on a unified `CanonicalMessage` type, decoupling your code from specific broker or database APIs.
- **Extensibility:** New endpoints and middleware can be added with minimal effort via trait-based factories.
- **Async-First:** Built on Tokio, all I/O and processing is asynchronous and concurrency-aware.
- **Unopinionated:** The library does not enforce a specific domain or concurrency model, focusing instead on reliable, programmable data movement.

## Main Components

### 1. Route
A `Route` defines a data pipeline from one input endpoint to one output endpoint. Each route can:
- Specify concurrency and batch size
- Attach middleware for reliability, deduplication, metrics, etc.
- Attach a handler for business logic (transform, filter, respond)

### 2. Endpoint
Endpoints are protocol adapters for sources (consumers) and sinks (publishers). Supported types include Kafka, NATS, AMQP, MQTT, MongoDB, HTTP, SQLx, ZeroMQ, Files, AWS, IBM MQ, and the `memory` endpoint (in-process channels and cross-process IPC). Endpoints are created via factory functions and configured via serde (json/yml).

### 3. Middleware
Middleware wraps consumers and publishers to add cross-cutting features:
- Retries (exponential backoff)
- Dead-letter queues (DLQ) to send messages to a fallback / error endpoint
- Deduplication (sled-based)
- Metrics
- Custom user middleware

### 4. Handler
Handlers are user-defined async functions that process messages. There are two main handler types:
- **CommandHandler:** 1-to-1 or 1-to-0 transformation, can return a new message for publishing or as response.
- **EventHandler:** 1-to-N handler for event consumption. Compatible to CommandHandler, but should not return a response.
- **TypeHandler:** Strongly-typed handler, dispatches based on the `kind` metadata field and deserializes payloads.

## Memory Endpoint and IPC Transport

The `memory` endpoint is not only an in-process channel. Its `topic` field (serde alias: `url`) doubles as a **transport URL**, so the same endpoint type covers both in-process queues and cross-process IPC over Unix domain sockets / Windows named pipes.

### Transport URL schemes

| URL | Resolves to | Platform |
| --- | --- | --- |
| `my-topic` | `memory://my-topic` (no scheme = in-process, for backward compatibility) | all |
| `memory://my-topic` | In-process channel, shared by namespace within the same process | all |
| `ipc://my-queue` | Unix: `/run/mq-bridge/my-queue.sock`<br>Windows: `\\.\pipe\mq-bridge-my-queue` | Unix + Windows |
| `ipc:///var/run/my.sock` | That exact socket path (leading `/` = absolute path, note the three slashes) | Unix |
| `unix:///var/run/my.sock` | That exact socket path; the path **must** be absolute | Unix only |
| `pipe://my-pipe` | `\\.\pipe\my-pipe` — used verbatim, **no** `mq-bridge-` prefix | Windows only |

Anything else (`http://…`, an empty URL, a relative `unix://path`) is rejected at parse time.

**Named `ipc://` resolution on Unix** falls back in order, using the first writable location:
1. `/run/mq-bridge/<name>.sock` (systemd standard)
2. `$XDG_RUNTIME_DIR/mq-bridge/<name>.sock`
3. `/tmp/mq-bridge/<name>.sock` (less secure)

Because of this fallback, `ipc://name` can land in different places for different users or services. When both sides must agree deterministically, use an explicit path (`ipc:///run/myapp/queue.sock`) instead of a bare name.

### Roles: consumer is the server, publisher is the client

IPC is **unidirectional, publisher → consumer**, and the roles are fixed:

- The **consumer** binds and listens on the socket/pipe (server). It removes a stale socket file before binding, creates the parent directory with mode `0700`, and sets the socket to `0600`. It unlinks the socket on drop.
- The **publisher** connects to it (client).

So **the consumer process must be running before the publisher connects** — otherwise the publisher fails with a connection error. The consumer serves one connection at a time; if the peer disconnects, it logs a warning and waits for a new connection rather than erroring out.

### IPC requires async construction

`MemoryConsumer::new`, `MemoryPublisher::new`, and `new_local` are synchronous and only support `memory://`. Given an IPC URL they return an error ("requires async endpoint construction"). Use the async constructors:

```rust
use mq_bridge::endpoints::memory::{MemoryConsumer, MemoryPublisher};
use mq_bridge::models::MemoryConfig;

let config = MemoryConfig::new_with_url("ipc:///run/mq-bridge/orders.sock", Some(100));

// Server side (start first)
let mut consumer = MemoryConsumer::new_async(&config).await?;
// Client side, in another process
let publisher = MemoryPublisher::new_async(&config).await?;
```

Routes always build endpoints through the async factories, so an `ipc://` URL works in YAML/JSON config with no extra code:

```yaml
ipc_ingest:
  input:
    memory:
      url: "ipc:///run/mq-bridge/orders.sock"
      capacity: 256
  output:
    kafka:
      topic: "orders"
      url: "localhost:9092"
```

### Wire format

Batches are serialized with **MessagePack** (`Vec<CanonicalMessage>`) and written as length-prefixed frames: a 4-byte big-endian length followed by the payload. Frames larger than 100 MB are rejected.

### Behavioural differences vs `memory://`

- **`enable_nack` defaults to `true`** for IPC transports (`ipc://`, `unix://`, `pipe://`) and `false` for `memory://`. An explicit `enable_nack` in config always wins.
- **`subscribe_mode` is not supported** over IPC, on either side — the EventStore/broadcast backend is in-process only.
- **`request_reply` is not supported** for IPC publishers.
- **The transport does not queue.** `len()` always reports `0` and `try_recv_batch()` always returns `None`, because a socket has no inspectable backlog. `capacity` bounds the consumer-side buffer, not an internal queue.

## Batching and Concurrency


Batch processing is a core concept in mq-bridge and is required for all endpoint implementations. Every consumer and publisher must implement batch receive and batch send methods (`receive_batch`, `send_batch`).

### Why batch mode?
- Batching improves throughput and efficiency, especially for high-volume or high-latency backends.
- It enables the bridge to process messages concurrently and in parallel, reducing per-message overhead.

### Concurrency
- Each route can be configured with a `concurrency` parameter, which determines how many worker tasks will process batches in parallel.
- Batch size is also configurable per route.

### Helper Utilities
mq-bridge provides several helper functions to make implementing batching easier, especially if your endpoint only supports single-message operations:

- **`send_batch_helper`**: Calls `send` for each message in a batch and aggregates the results. Used to implement `send_batch` when only single-message sending is available.
- **`receive_batch_helper`**: Calls `receive` once and wraps the result as a batch. Used to implement `receive_batch` when only single-message receive is available.
- **`into_commit_func`**: Converts a batch commit function (`BatchCommitFunc`) into a single-message commit function (`CommitFunc`).
- **`into_batch_commit_func`**: Converts a single-message commit function into a batch commit function.

**Sample: Using `send_batch_helper`**
```rust
use mq_bridge::traits::send_batch_helper;
// Inside your MessagePublisher implementation:
async fn send_batch(&self, messages: Vec<CanonicalMessage>) -> Result<SentBatch, PublisherError> {
    send_batch_helper(self, messages, |pub_ref, msg| {
        Box::pin(pub_ref.send(msg))
    }).await
}
```

**Sample: Using `receive_batch_helper`**
```rust
// receive_batch_helper is a default method on MessageConsumer — no import needed.
// Inside your MessageConsumer implementation:
async fn receive_batch(&mut self, max_messages: usize) -> Result<ReceivedBatch, ConsumerError> {
    self.receive_batch_helper(max_messages).await
}
```

**Sample: Commit function conversion**
```rust
use mq_bridge::traits::{into_commit_func, into_batch_commit_func};
let commit: CommitFunc = into_commit_func(batch_commit_func);
let batch_commit: BatchCommitFunc = into_batch_commit_func(commit_func);
```

### How batch receive works internally

The `receive_batch` method is designed to efficiently collect a batch of messages from the underlying transport. The typical pattern is:

1. **Wait for the first message:** The consumer awaits a message from the backend (e.g., Kafka, NATS, etc.).
2. **Drain additional messages if available:** After the first message is received, the consumer immediately checks if more messages are already available (without waiting). It continues to drain messages up to the batch size or until no more are available.
3. **Return the batch:** The batch is returned as soon as either the batch size is reached or no more messages are immediately available.

This approach minimizes latency for the first message while maximizing throughput for bursts of messages.

**Pseudocode:**
```rust
async fn receive_batch(&mut self, max_messages: usize) -> Result<ReceivedBatch, ConsumerError> {
	let mut messages = Vec::with_capacity(max_messages);
	// Wait for the first message
	let first = self.inner_receive().await?;
	messages.push(first);
	// Try to drain more messages without waiting
	while messages.len() < max_messages {
		match self.try_receive_now()? {
			Some(msg) => messages.push(msg),
			None => break,
		}
	}
	Ok(ReceivedBatch { messages, commit: ... })
}
```

**Example: Receiving and committing a batch**
```rust
let batch = consumer.receive_batch(100).await?;
// Process each message in the batch...
batch.commit(vec![MessageDisposition::Ack; batch.messages.len()]).await?;
```

**Example: Sending a batch**
```rust
let messages = vec![msg1, msg2, msg3];
publisher.send_batch(messages).await?;
```

See the README and tests for more advanced batching and concurrency patterns.

## Getting Started

Below are minimal examples for the three main usage patterns in mq-bridge. For more, see the README and tests.

### 1. Typed Handler (Event-driven, Type-safe)

Use for strongly-typed, event-driven communication. Register Rust types per message `kind`; the bridge deserializes payloads automatically. Supports request-response where the protocol allows. Multiple types can be handled by a single route.

```rust
use mq_bridge::{msg, Handled, Route, publisher::Publisher, models::Endpoint};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
struct OrderPlaced {
    order_id: u64,
    amount: f64,
}

let input = Endpoint::new_memory("in", 10);
let output = Endpoint::null();
let route = Route::new(input, output)
    .add_handler("order_placed", |msg: OrderPlaced| async move {
        println!("Order #{}: ${}", msg.order_id, msg.amount);
        Ok(Handled::Ack)
    });
route.deploy("typed_handler_example").await.unwrap(); // or use route.run()

let input_publisher = Publisher::new(route.input.clone()).await.unwrap();
let event = OrderPlaced { order_id: 42, amount: 19.99 };
input_publisher.send(msg!(&event, "order_placed")).await.unwrap();
// ...
```

### 2. Compute Handler (Generic)

Use a generic handler to process, transform, or filter messages against the raw `CanonicalMessage`. One handler per-route. Suitable for pipelines, ETL, or side-effect processing.

```rust
use mq_bridge::{CanonicalMessage, Handled, Route, models::Endpoint};

let input = Endpoint::new_memory("in", 10);
let output = Endpoint::new_memory("out", 10);
let handler = |mut msg: CanonicalMessage| {
    msg.set_payload_str(format!("processed: {}", msg.get_payload_str()));
    async move { Ok(Handled::Publish(msg)) }
};
let route = Route::new(input, output).with_handler(handler);
route.deploy("compute_handler_example").await.unwrap();
// ...
```


### 3. Direct Endpoint Usage (Manual Control)

Use `send` / `send_batch` and `receive` / `receive_batch` directly on endpoints. Gives full manual control over batching, commit, concurrency, and sequencing. Useful for advanced scenarios or integration with external async runtimes.

```rust
use mq_bridge::endpoints::memory::{MemoryConsumer, MemoryPublisher};
use mq_bridge::{CanonicalMessage, traits::MessageDisposition};

let publisher = MemoryPublisher::new_local("my_topic", 100);
let mut consumer = MemoryConsumer::new_local("my_topic", 100);
let msg = CanonicalMessage::new(b"hello world".to_vec(), None);
publisher.send(msg).await.unwrap();
let received = consumer.receive().await.unwrap();
// Process the message...
// Acknowledge (required for most endpoints):
(received.commit)(MessageDisposition::Ack).await.unwrap();

// Batch variant:
let batch = consumer.receive_batch(10).await.unwrap();
batch.commit(vec![MessageDisposition::Ack; batch.messages.len()]).await.unwrap();
```

## Extending mq-bridge
- **Custom Endpoints:** Implement the `CustomEndpointFactory` trait and register your type.
- **Custom Middleware:** Implement the `CustomMiddlewareFactory` trait.
- **Typed Handlers:** Use `TypeHandler` to add new message types and logic.

## Configuration
- All routes, endpoints, and middleware are defined via YAML, JSON, or environment variables.
- See [CONFIGURATION.md](CONFIGURATION.md) for a full reference and examples.

## Example: Route Lifecycle
1. Define a route either as json or code
2. Create endpoints and apply middleware
3. Attach a handler (optional)
4. Deploy or run the route (spawns async workers)
5. Inject or receive messages
6. Route processes, transforms, and delivers messages according to config and handler logic

## More Information
- See the README for usage patterns and code examples.
- See [CONFIGURATION.md](CONFIGURATION.md) for configuration details.
- See the source code for trait definitions and extension points.
