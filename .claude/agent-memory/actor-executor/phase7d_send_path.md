---
name: Phase 7d — KafkaProducer send hot path
description: Translation of doSend/partition/waitOnMetadata + impl Producer; deferred sender-wakeup; double-fire fix for catch-block path
type: project
---

Phase 7d wires `KafkaProducer::send` end-to-end up to (but not
including) the broker round-trip. Several decisions to remember.

## `sender.wakeup()` is a no-op (deferred)

Java's `KafkaProducer.waitOnMetadata` calls `sender.wakeup()` between
`metadata.requestUpdateForTopic(topic)` and `metadata.awaitUpdate(...)`.
The Rust `Sender` was moved into a `tokio::spawn` task at construction
time, so the producer no longer holds a reference to the
`KafkaClient` it could call `wakeup()` on. The Phase 7d implementation
keeps `sender_wakeup` as a no-op with documented rationale: the
Sender's `run_loop` re-fetches metadata every iteration and the
`await_update` deadline is bounded by `max.block.ms`, so a missed
wake-up costs at most one Sender tick of latency, never a hang.

Resolution path (deferred): expose an
`Arc<dyn Fn() + Send + Sync>` wake handle from the Sender at
construction time, captured pre-spawn from the underlying
`KafkaClient::wakeup`. Documented in
`design/history/Milestone-1/Phase-7/NOTES.md`.

## `K: Clone, V: Clone` bound on `do_send`

`ProducerInterceptors::on_send_error` (Phase 6c) requires `K: Clone,
V: Clone` because each interceptor is invoked inside `catch_unwind`
and the previous-good-record fallback requires a clone. The do_send
path therefore inherits the bound. Hot-path types (`Vec<u8>`,
`String`, `&[u8]`) all satisfy `Clone`.

## CRITICAL: avoid double-firing interceptors on the error path

Java's `doSend` catch block fires the **user callback DIRECTLY** (not
via `appendCallbacks.onCompletion`), then fires
`interceptors.onSendError`:

```java
catch (ApiException e) {
    if (callback != null) {
        TopicPartition tp = appendCallbacks.topicPartition();
        RecordMetadata nullMetadata = new RecordMetadata(...);
        callback.onCompletion(nullMetadata, e);  // user-only!
    }
    this.errors.record();
    this.interceptors.onSendError(record, tp, e);
}
```

If you naively call `appendCallbacks.onCompletion(metadata, err)` in
the catch arm, **interceptors.onAcknowledgement fires twice** — once
inside `appendCallbacks.onCompletion` (which calls
`interceptors.onAcknowledgement`) and once via `onSendError` (which
also calls `onAcknowledgement` on each interceptor). Each interceptor
observes two error events per failed send.

The fix: extract the user callback from `append_cb.user_callback` and
fire it directly, bypassing the AppendCallbacks wrapper. A test must
pin this — assert the on_acknowledgement-with-error count is exactly
1 after a `RecordTooLarge` send. The test
`send_returns_record_too_large_and_fires_interceptor_on_send_error`
implements this regression guard.

## `AppendCallbacksImpl<K,V>` design

- One per `send` (Java parity — Java's per-call inner class also
  allocates).
- Fields: `user_callback`, `interceptors` (Arc<…>), `topic` (Arc<str>
  bump), `record_partition` (Option<i32>), `headers` (Vec clone),
  `partition` (AtomicI32), `topic_partition` (OnceLock<TopicPartition>).
- `set_partition` (called from accumulator) and `topic_partition()`
  (called from sender) race safely via the atomic.
- `topic_partition()` priority chain: `set_partition` value >
  `record_partition` > `UNKNOWN_PARTITION`. Java's `topicPartition()`
  caches the first computed value via the OnceLock — once the
  accumulator calls `set_partition`, subsequent reads return the same
  TopicPartition.

## `wait_on_metadata` translation specifics

- Returns `ClusterAndWaitTime { cluster, waited_on_metadata_ms }` to
  match Java's private inner class shape — caller needs both pieces.
- Fast path: cached metadata covers the requested partition → return
  with `waited_on_metadata_ms = 0`.
- Wait loop: `metadata.add()` then `metadata.request_update_for_topic`
  then `await_update(version, remaining_wait_ms)`. On timeout, build
  the Java-verbatim error message ("Topic X not present in metadata
  after Y ms.").
- `metadata.maybe_throw_error_for_topic(topic)` propagates topic-
  level errors (InvalidTopic, TopicAuthorization) on every iteration.

## `Producer` trait deferral pattern

`send` and `send_with_callback` are implemented; every other trait
method returns one of three `KafkaError::UnsupportedOperation` markers:
- `PHASE_7E_DEFERRED` — flush, close, partitions_for, metrics
- `PHASE_9_TXN_DEFERRED` — init/begin/commit/abort_transaction
- `TELEMETRY_DEFERRED` — client_instance_id

Pin the trait impl with a generic-bound compile-only test:

```rust
fn _phase_7d_impls_producer<K: ..., V: ..., C: ...>(p: KafkaProducer<K,V,C>) {
    fn check<K, V, P: crate::producer::Producer<K, V>>(_p: P) {}
    check::<K, V, _>(p);
}
```

If the impl is removed, the test fails to compile.

## Testing pattern: pre-populate metadata via `update_with_current_request_version`

Tests that exercise the send path through `wait_on_metadata` need a
populated metadata snapshot. Use:

```rust
let pm = ProducerMetadata::new(...).expect(...);
pm.add(topic, now);
pm.update_with_current_request_version(&build_single_topic_response(topic, n), false, now)?;
```

Then pass the populated `Some(pm)` into `KafkaProducer::new_for_test`.
For invalid-topic tests: build a `MetadataResponseTopic` with
`error_code: 17` (InvalidTopicException). The Cluster's
`invalid_topics()` set picks it up automatically.

## Hot-path allocation audit (DoD #10)

For each per-record allocation in `do_send`:
- `AppendCallbacksImpl` (Java parity — one allocation per send).
- `headers_slice: Vec<RecordHeader>` (Java parity — `headers().toArray()` copy).
- Serialized bytes from `serializer.serialize` — Vec<u8>, owned.
- Passed by `as_deref()` slice into `accumulator.append` — NO second
  copy.
- `cb_dyn: Arc<dyn AppendCallbacks>` — Arc bump.
- `Arc::clone(&self.interceptors)` — Arc bump.
- No `Box<dyn Future>` per send — `async fn` returns `impl Future`.
- No `tokio::spawn` per send — the Sender is the single coroutine.
