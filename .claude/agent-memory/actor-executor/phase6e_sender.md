---
name: Phase 6e Sender + MockClient subset
description: Patterns settled in Phase 6e — Sender struct, KafkaClient generic, pending_responses by correlation id, MockClient subset, AbstractResponse::as_any
type: project
---

Phase 6e translates `Sender.java` (1143 LOC) and the non-transactional
subset of `MockClient.java` (845 LOC) into 6 commits. Test count
1056 → 1085 (+29).

**Why:** This is the final Phase 6 sub-phase — the patterns chosen here
will affect any future producer-side or sender-related work and Phase 7
(KafkaProducer shell).

## Sender struct shape — generic over KafkaClient

```rust
pub(crate) struct Sender<C: KafkaClient> {
    client: C,                        // owned by-value (not Arc)
    accumulator: Arc<RecordAccumulator>,
    metadata: Arc<ProducerMetadata>,
    transaction_manager: Option<TransactionManager>,  // always None
    in_flight_batches: Mutex<HashMap<TopicPartition, Vec<Arc<ProducerBatch>>>>,
    pending_responses: Mutex<HashMap<i32, ResponseContext>>,  // see below
    ...
}
```

The `client` is held by-value (not `Arc<Mutex<>>`) because
`KafkaClient::poll` takes `&mut self`. Generic over `C: KafkaClient`
to allow `MockClientImpl` for tests and a production `NetworkClient`
in Phase 7. `Sender::run_once`/`run_loop` take `&mut self` to thread
the `&mut client` through.

## ResponseContext registered by correlation id (NOT via callback)

Java attaches a per-request `RequestCompletionHandler` (captured lambda
over `recordsByPartition`, `topicNames`). The handler fires synchronously
from `MockClient::poll` via `ClientResponse::on_complete()`.

The Rust translation uses a different dispatch:

```rust
struct ResponseContext {
    batches: HashMap<TopicPartition, Arc<ProducerBatch>>,
    topic_names: HashMap<Uuid, String>,
}
// Sender field: pending_responses: Mutex<HashMap<i32 /*correlation_id*/, ResponseContext>>
```

Why: the `RequestCompletionHandler` trait takes `&self`, but
`Sender::handle_produce_response` mutates `in_flight_batches` and the
pending-responses map. We could not use the callback to drive the
side-effect work without complex shared-state plumbing. Instead, we:

1. Register the `ResponseContext` keyed by correlation_id when sending.
2. After `client.poll().await` returns, walk responses, pop the
   matching context for each correlation id, and call
   `handle_produce_response(ctx, response)`.
3. Pass a `NoopHandler` to satisfy the `KafkaClient::send` callback
   slot — the `MockClient` calls it but it does nothing.

This preserves Java's "response is processed after `poll`, on the same
task" invariant exactly.

## ProduceRequestBuilder is local to sender.rs

There's no `ProduceRequest::Builder` in the Rust translation. We inlined
a `ProduceRequestBuilder` directly in sender.rs that wraps
`Mutex<Option<ProduceRequestData>>` (because `AbstractRequestBuilder::build`
takes `&self` but the data is consumed). The `take()` on first build is
fine — Java's `Builder.build` is also called once per send.

Don't promote this to `common::requests::produce_request` unless a
non-Sender caller needs it.

## AbstractResponse::as_any added in this phase

Java does `(ProduceResponse) response.responseBody()` — a direct cast on
a polymorphic field. The Rust trait `AbstractResponse` originally had
no downcast method, so I added:

```rust
fn as_any(&self) -> &dyn std::any::Any;
```

with concrete impls on `ProduceResponse`, `MetadataResponse`, and
`ApiVersionsResponse`. This is the cleanest way to recover concrete
types at the call site (`body.as_any().downcast_ref::<ProduceResponse>()`).

## MockClientImpl subset — what's translated, what's not

Translated:
- Queue-based response staging (`prepare_response`,
  `prepare_response_with_disconnect`, `respond`, `respond_with_disconnect`).
- Default-response queue (`responses` is consumed by `poll`).
- Per-node connection state (`ConnState { ready, backoff_until_ms, disconnected }`).
- `disconnect_node` — emits a `disconnected=true` ClientResponse for
  every queued request to that node.
- `check_timeout_of_pending_requests(now)` — Java's MockClient.poll
  always calls this before walking responses; needed for
  `testNoDoubleDeallocation`.
- `set_node_api_versions` (stub — no behavioral effect since Phase 6e
  doesn't exercise the api-version negotiation paths).

NOT translated (per Phase 6e NOTES.md):
- `RequestMatcher` plumbing (`prepareResponseFrom(matcher, ...)`) for
  FindCoordinator, InitProducerId, AddPartitionsToTxn, EndTxn,
  WriteTxnMarkers, TxnOffsetCommit, AddOffsetsToTxn.
- Coordinator-tracking maps.
- `respondToRequest` for out-of-order responses (not exercised by any
  non-tx case).

## Skip rationale: tests by Java case name

Listed in module rustdoc — every skipped Java case name + reason ("idempotent producer path" / "transactional path"). Each non-tx case translated.

## Hot-path allocations audit

Per-poll allocation count:
- `pending_responses.remove(&id)` — pop per response (no allocation).
- `handle_produce_response` parses `PartitionResponse` from the wire
  type via `clone()`s of `error_message`, `record_errors` —
  proportional to the number of partition responses, not records.
- `record_errors.iter().map(|e| RecordError::new(e.batch_index, e.batch_index_error_message.clone()))` —
  `Option<String>` clone per record-error (not per record).
- No `Box<dyn Future>` per send. `KafkaClient::poll` and
  `Sender::handle_responses` are concrete `async fn`.
- No `String` clone per topic-partition lookup —
  `ResponseContext::topic_names` is captured at send-time, not per
  response.
- No per-message `tokio::spawn`. The Sender is the single coroutine.

## Skip-rationale tightening (Phase 6d Round 1 lesson re-applied)

Each skipped Java SenderTest case in module rustdoc names the case
explicitly with reason. Categories:
- "Idempotent producer path" (tests driving sequence numbers, producer
  ID, epoch).
- "Transactional path" (tests driving `TransactionManager`).
- "Metrics infrastructure" (PLAN.md line 296 — out of scope).
