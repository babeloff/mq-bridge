# HTTP Stream Endpoint Plan

## Summary

Add a separate `http_stream` consumer endpoint later, focused on robust NDJSON ingest first.
Do not make WebSocket-style interactive replies the default, because HTTP streaming can deadlock
with clients or proxies that upload the request before reading the response.

Keep existing `http.receive_streamable` behavior as compatibility for now, but treat it as
legacy or advanced behavior once `http_stream` exists.

## Recommendation

- Add `http_stream` as a separate session-style inbound streaming endpoint.
- Make v1 ingest-only by default:
  - request stream -> many canonical messages
  - each NDJSON line -> one `CanonicalMessage`
  - one shared `http_stream_id` / `correlation_id`
  - per-item `http_stream_index`
  - response is a simple accepted/final status, not per-item replies
- Start with NDJSON as the canonical format. Consumer support can later grow to SSE if needed.
- Defer interactive streamed replies to a later explicit mode.
- Leave outbound `HttpConfig::stream_response_to` separate; it is response splitting, not inbound stream ingest.

## Why This Is Better Than The Current Flag

The current `http.receive_streamable` flag hides a different transport contract inside normal HTTP:

- normal `http`: one request body -> one canonical message -> one HTTP response
- streamable receive: one request body -> many canonical messages -> optional streamed replies

A separate endpoint makes the `n:n` behavior explicit, gives users a clearer mental model, and lets
the implementation apply tighter stream-specific safety rules without making normal HTTP harder to use.

## Main Risks

- HTTP streaming is not reliably full-duplex across all clients, proxies, and middleware.
- Inline per-item replies can deadlock if the client uploads the request body without reading the response.
- Route commit sequencing can amplify a blocked reply write: if one reply commit blocks, later commits wait.
- Raw HTTP chunks are transport boundaries, not stable content boundaries.
- Sensitive HTTP headers can leak through metadata unless filtered before messages are emitted.
- Out-of-order processing can make reply correlation ambiguous unless every reply carries an item id.

## Design Direction

Treat `http_stream` as a session endpoint:

```text
http_stream_id / correlation_id = the whole HTTP request stream
http_stream_index = one received item inside that stream
```

For v1:

- Accept `application/x-ndjson`.
- Emit one canonical message per non-empty NDJSON line.
- Generate a stream id by default; only trust client-supplied `correlation_id` according to the endpoint's auth/security policy.
- Add `http_stream_id`, `correlation_id`, `http_stream_index`, and `http_stream_format` metadata to each emitted message.
- Filter sensitive request headers from metadata, at least matching WebSocket's existing filtering of auth/cookie-style headers.
- Return an ingest response that does not depend on per-item downstream replies.

## Interactive Mode Later

If interactive replies are added later, make them explicit and not the default:

- Require a config option such as `mode: interactive`.
- Stream replies as NDJSON only initially.
- Include `http_stream_id` and `http_stream_index` in every reply frame.
- Add a timeout around reply-frame writes; slow or non-reading clients should fail the session instead of blocking indefinitely.
- Document that interactive mode requires full-duplex-capable clients and compatible proxies.
- Recommend route `concurrency: 1` for ordered request/reply streams unless clients tolerate out-of-order replies.

## Testing Targets

- NDJSON request with multiple lines emits one canonical message per line.
- Empty lines are ignored.
- All messages from one request share `http_stream_id` / `correlation_id`.
- `http_stream_index` starts at `0` and increments per emitted item.
- Sensitive headers such as `authorization`, `cookie`, `set-cookie`, `proxy-authorization`, `x-api-key`, and `session` are not propagated into metadata.
- Ingest mode does not wait for per-item replies.
- Existing `http.receive_streamable` tests still pass for compatibility.
