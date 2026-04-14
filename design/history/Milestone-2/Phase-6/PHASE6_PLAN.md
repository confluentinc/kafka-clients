# Phase 6 -- Rewrite Sender to Use KafkaClient (Eliminate ProduceClient & KafkaProduceClient)

## Goal

Rewrite `Sender` to own a `KafkaClient` implementation (concretely `NetworkClient`)
instead of the invented `ProduceClient` trait.  Delete `KafkaProduceClient`.  This
eliminates the Mutex bottleneck by matching Java's architecture where `Sender`
directly owns the client with no shared lock.

## Java Reference

### Java Sender Architecture

```
Sender (Runnable, owns KafkaClient)
  run():
    while (running):
      runOnce()
  
  runOnce():
    pollTimeout = sendProducerData(now)  // drains accumulator, groups by node, calls client.send() per node
    client.poll(pollTimeout, now)        // single call drives ALL I/O
  
  sendProduceRequest(now, destination, acks, timeout, batches):
    // Build ProduceRequestData from batches
    // Create callback: response -> handleProduceResponse(response, batches, topicNames, now)
    // client.newClientRequest(nodeId, requestBuilder, now, acks != 0, requestTimeoutMs, callback)
    // client.send(clientRequest, now)  -- NON-BLOCKING, just queues
  
  handleProduceResponse(response, batches, topicNames, now):
    // Called by NetworkClient when response arrives (via RequestCompletionHandler callback)
    // Completes ProducerBatch futures
```

Key architectural points:
1. `client.send()` is **non-blocking** -- it just queues the request
2. `client.poll()` drives **all** I/O and fires callbacks for completed responses
3. Multiple produce requests can be in-flight simultaneously to different nodes
4. `handleProduceResponse` is a **callback** invoked by `client.poll()` via `RequestCompletionHandler`
5. `Sender` does NOT await each request individually -- it batches sends then polls

### Java KafkaProducer Architecture

```
KafkaProducer
  fields:
    sender: Sender (runs on ioThread)
    metadata: ProducerMetadata (shared with Sender)
    accumulator: RecordAccumulator (shared with Sender)
  
  doSend(record):
    waitOnMetadata(topic)  // uses ProducerMetadata
    partition = partition(record, metadata)
    accumulator.append(tp, ...)
    if (result.batchIsFull || result.newBatchCreated):
      sender.wakeup()
```

## Design Decisions

### 1. Sender owns KafkaClient directly (no Arc, no Mutex)

Java: `private final KafkaClient client;` -- direct ownership, no sharing.

Rust equivalent: `Sender` owns a `Box<dyn KafkaClient>` (or generic `C: KafkaClient`).
Since `KafkaClient` is now async (Phase 5), and `Sender.run()` is an async
function spawned as a Tokio task, this works without locks.

### 2. ProduceClient trait is deleted

The `ProduceClient` trait (with `send_produce_request`, `partitions_for`) was a
deviation from Java.  It combined what should be separate concerns:
- Sending produce requests (should use `KafkaClient::send` + callback)
- Metadata fetching (should use `ProducerMetadata` / `Metadata`)

### 3. KafkaProduceClient is deleted

`KafkaProduceClient` reimplemented connection management, API version negotiation,
request serialization, and response parsing -- all of which `NetworkClient` already
does.  It is replaced by `NetworkClient` directly.

### 4. Sender.run() matches Java's runOnce() pattern

```rust
// Rust Sender::run() loop:
loop {
    if self.accumulator.is_closed().await && !self.client.has_in_flight_requests() {
        break;
    }
    let now = current_time_ms();
    let poll_timeout = self.send_producer_data(now).await;
    self.client.poll(poll_timeout, now).await;
}
```

### 5. sendProduceRequest uses RequestCompletionHandler callback

```rust
// Rust Sender::send_produce_request():
let callback: RequestCompletionHandler = Box::new(move |response: &mut ClientResponse| {
    handle_produce_response(response, &records_by_partition, &topic_names, now);
});
let client_request = self.client.new_client_request_with_timeout(
    node_id, Box::new(request_builder), now, acks != 0,
    self.request_timeout_ms, Some(callback),
);
self.client.send(client_request, now);
```

The callback completes `ProducerBatch` futures, matching Java's
`handleProduceResponse`.  Per CLAUDE.md rule 9.1, Java callbacks are translated
to closures executed after awaiting the corresponding call (here, the callback is
invoked by `client.poll()` when the response arrives).

### 6. Metadata handling moves to ProducerMetadata (Phase 7)

In this phase, metadata is still handled by `KafkaProducer.wait_on_metadata()`
using the existing approach (calling `KafkaClient` directly).  Phase 7 introduces
`ProducerMetadata` as a proper shared metadata cache matching Java's architecture.

### 7. Mock boundary for tests

Tests mock at the `KafkaClient` trait level instead of `ProduceClient`.
`MockKafkaClient` implements `KafkaClient` and can be configured to return
specific `ClientResponse` values.  This is a closer match to Java's test
infrastructure (e.g., `SenderTest` uses `MockClient`).

## Files to Delete

| File | Reason |
|------|--------|
| `src/clients/producer/kafka_produce_client.rs` | Redundant -- replaced by `NetworkClient` |

## Files to Modify

| File | Change |
|------|--------|
| `src/clients/producer/sender.rs` | Remove `ProduceClient` trait. Rewrite `Sender` to own `Box<dyn KafkaClient>` (or generic). Implement `run()` with `sendProducerData()` + `client.poll()` loop. Implement `send_produce_request()` with callback. Implement `handle_produce_response()`. |
| `src/clients/producer/kafka_producer.rs` | Remove generic `<C: ProduceClient>`. Use concrete `NetworkClient` via `KafkaClient` trait. Move metadata to be driven by the shared `Metadata` instance. Update all unit tests to mock `KafkaClient` instead of `ProduceClient`. |
| `src/clients/producer/mod.rs` | Remove `kafka_produce_client` module, `KafkaProduceClient` export, `ProduceClient` export. |
| `performance_tests/src/producer_perf.rs` | Update construction to use `NetworkClient` instead of `KafkaProduceClient`. |

## Files to Add

None in this phase (ProducerMetadata is Phase 7).

## Key Implementation Details

### Sender struct

```rust
pub struct Sender<C: KafkaClient> {
    client: C,
    accumulator: Arc<RecordAccumulator>,
    metadata: Arc<Metadata>,          // shared with KafkaProducer
    acks: i16,
    max_request_size: i32,
    request_timeout_ms: i32,
    retry_backoff_ms: i64,
    retries: i32,
    running: Arc<AtomicBool>,
    in_flight_batches: HashMap<TopicPartition, Vec<ProducerBatch>>,
}
```

### KafkaProducer struct (no longer generic over ProduceClient)

```rust
pub struct KafkaProducer {
    inner: Arc<ProducerInner>,
}

struct ProducerInner {
    config: Arc<ProducerConfig>,
    accumulator: Arc<RecordAccumulator>,
    metadata: Arc<Metadata>,
    sender_handle: Mutex<Option<JoinHandle<()>>>,
}
```

### handleProduceResponse callback pattern

The callback captures the batch map and completes each `ProducerBatch` future:

```rust
let callback: RequestCompletionHandler = {
    let batches = records_by_partition.clone();
    let topic_names = topic_names.clone();
    Box::new(move |response: &mut ClientResponse| {
        if response.was_disconnected() {
            // fail all batches with network error
        } else if let Some(body) = response.response_body() {
            // parse ProduceResponse, complete each batch
        } else {
            // acks=0: complete all batches with success
        }
    })
};
```

## Tests to Translate/Update

### Unit tests in kafka_producer.rs (update existing)

All existing tests (`test_send_and_receive_metadata`, `test_send_empty_topic_rejected`,
`test_send_multiple_records`, `test_clone_shares_state`, `test_partitions_for`,
`test_send_invalid_partition_rejected`, `test_send_negative_partition_rejected`,
`test_wait_on_metadata_times_out`, `test_metadata_fetch`,
`test_metadata_with_partition_out_of_range`, `test_topic_refresh_in_metadata`,
`test_close_when_waiting_for_metadata_update`) must be updated to use a
`MockKafkaClient` instead of `MockProduceClient`.

The `MockKafkaClient` will implement `KafkaClient` with configurable behavior
for `ready()`, `send()`, `poll()`, and metadata operations.

### Tests from Java SenderTest to translate

From `kafka/clients/src/test/java/org/apache/kafka/clients/producer/internals/SenderTest.java`,
translate the core non-transactional tests:

| Java Test | Purpose |
|-----------|---------|
| `testSimple` | Basic send and response flow |
| `testRunOnce` (non-txn path) | Verify runOnce drains + polls |
| `testSendProducerData` | Verify batch grouping by node |
| `testSendProduceRequestsWithNoBatches` | Empty batch list is no-op |
| `testMultiNodeSend` | Sends to multiple nodes in single runOnce |
| `testRetries` | Retriable errors trigger retry |
| `testMessageTooLarge` | MESSAGE_TOO_LARGE error handling |
| `testTopicAuthorizationExceptionInProduceRequest` | Auth error handling |

Tests involving `TransactionManager` are out of scope.

## Scope Limits

- No transactions (TransactionManager is null/None)
- No idempotent producer (no producer ID/epoch)
- No batch splitting on MESSAGE_TOO_LARGE (stub to fail the batch)
- No metrics/sensors (SenderMetrics)
- No guaranteeMessageOrder / partition muting
- ProducerMetadata is deferred to Phase 7 -- use existing Metadata

## Definition of Done

1. `ProduceClient` trait deleted from `sender.rs`
2. `KafkaProduceClient` deleted
3. `Sender` uses `KafkaClient::send()` + `KafkaClient::poll()` pattern
4. `handleProduceResponse` callback completes `ProducerBatch` futures
5. `KafkaProducer` is no longer generic over `ProduceClient`
6. All existing unit tests pass (updated to mock `KafkaClient`)
7. Performance test updated and compiles
8. `cargo build` succeeds
9. `cargo test` passes
10. `cargo xtask format-check` passes
11. `cargo xtask lint` passes
