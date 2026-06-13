"""TechEmpower JSON + Plaintext server for mq-bridge-py.

A single ``http -> response`` route is bound to ``0.0.0.0:8080`` with no path
filter; the handler dispatches on the request's ``http_path`` metadata. This
keeps both endpoints (``/json`` and ``/plaintext``) on one listener/port.

Design notes
------------
* The route uses the inline-response fast path, which bypasses the route's
  worker/disposition pipeline. Route ``concurrency``/``batch_size`` therefore do
  not gate the HTTP hot path; per-request parallelism comes from the multi-thread
  Tokio runtime (all HTTP framing + JSON (de)serialization run in Rust, off the
  GIL), while the single Python worker thread runs only the trivial dispatch.
  We keep ``concurrency: 1`` (cheapest; no route worker pool is spawned).
* ``concurrency_limit`` is a per-request semaphore (default 100) that would
  throttle TechEmpower's many connections, so we raise it above the expected
  connection count.
* The returned ``Message``'s metadata becomes the HTTP response headers, and
  hyper adds ``Date``/``Content-Length`` automatically, satisfying the header
  rules. We return a *fresh* ``Message`` so no request headers are echoed back.
"""

from __future__ import annotations

import os
import tempfile

from mq_bridge import Message, Route

LISTEN = os.environ.get("MQB_LISTEN", "0.0.0.0:8080")

CONFIG = f"""
routes:
  techempower:
    concurrency: 1
    batch_size: 512
    input:
      http:
        url: "{LISTEN}"
        method: "GET"
        concurrency_limit: 65536
        internal_buffer_size: 16384
        inline_response_fast_path: true
    output:
      response: {{}}
"""

JSON_META = {"content-type": "application/json", "Server": "mq-bridge-py"}
TEXT_META = {"content-type": "text/plain", "Server": "mq-bridge-py"}
NOT_FOUND_META = {
    "content-type": "text/plain",
    "Server": "mq-bridge-py",
    "http_status_code": "404",
}


def handle(message: Message) -> Message:
    path = message.metadata.get("http_path", "")
    if path == "/json":
        # Serialize the object per request (Rust/serde, off-GIL).
        return Message.from_json({"message": "Hello, World!"}, JSON_META)
    if path == "/plaintext":
        return Message(b"Hello, World!", TEXT_META)
    return Message(b"Not Found", NOT_FOUND_META)


def main() -> None:
    with tempfile.NamedTemporaryFile("w", suffix=".yaml", delete=False) as handle_file:
        handle_file.write(CONFIG)
        config_path = handle_file.name
    route = Route.from_yaml(config_path, "techempower").with_handler(handle)
    route.run()


if __name__ == "__main__":
    main()
