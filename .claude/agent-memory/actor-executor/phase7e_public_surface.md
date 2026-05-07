---
name: Phase 7e — public surface complete
description: partitioner.class factory, partitions_for, metrics stub, flush, close (idempotent + bounded), DefaultMetadataUpdater deferred to Phase 8
type: project
---

Phase 7e wires the remaining `Producer` trait methods on
`KafkaProducer`. After this phase, every non-transactional non-
telemetry method works end-to-end against `new_for_test`.

## `partitioner.class` factory pattern

Java uses reflection: `config.getConfiguredInstance(PARTITIONER_CLASS_CONFIG, Partitioner.class)`.
Rust has no reflection — instead a private `configure_partitioner`
helper maps known class strings to translated partitioner instances:

* `null` / unset / empty → `None` (built-in adaptive partitioner).
* `org.apache.kafka.clients.producer.RoundRobinPartitioner` (FQCN)
  AND `RoundRobinPartitioner` (simple-name alias) → `RoundRobinPartitioner`.
* anything else → `KafkaError::Config("Unrecognised partitioner.class: '<name>'. Supported values in Milestone-1: ...")`.

The simple-name alias is a Rust ergonomic add-on — Java users won't
spell out the FQCN as conveniently. Keep the FQCN for compatibility.

Don't use `config.get_class(...)` — its default-Null path returns Err.
Use `config.inner().originals().get(PARTITIONER_CLASS_CONFIG)` for the
"is the user explicitly setting this?" question. Then `.trim()` and
match.

## `JoinHandle` storage for idempotent async close

Phase 7c stored the `JoinHandle` as `Option<JoinHandle<()>>`. Phase 7e
needs `&self async fn close` to take the handle for awaiting. Pattern:

```rust
sender_task: std::sync::Mutex<Option<JoinHandle<()>>>,
closed: Arc<AtomicBool>,
```

Idempotency is the `closed: Arc<AtomicBool>` (compare-and-swap: first
call wins). The Mutex<Option<JoinHandle>> is the "exactly one taker"
slot — `OnceLock` is wrong here (it's "init at most once"; we need
take semantics). After close, `Drop` sees `None` and skips abort.

Tests must access `sender_task.lock().unwrap().take()` instead of
`sender_task.take()`.

## Close translation: `&self` async path can't call `Sender::initiate_close`

`Sender::initiate_close` was an instance method — but the Sender was
moved into `tokio::spawn` at construction time. The producer no
longer holds the Sender. Inline the three steps from the producer
side:

```rust
self.accumulator.close();              // pub fn on Arc<RecordAccumulator>
self.sender_running.store(false, _);   // Arc<AtomicBool> kept on producer
self.sender_wakeup();                  // Phase 7d no-op
```

Then `tokio::time::timeout(timeout, JoinHandle).await` — three
outcomes:

* `Ok(Ok(()))` — Sender exited cleanly within deadline (drain done).
* `Ok(Err(join_err))` — task panicked or was cancelled mid-drain.
  Java surfaces as `KafkaException`; Rust logs.
* `Err(_elapsed)` — deadline blown. Set `force_close=true`. We don't
  have the JoinHandle anymore (consumed by `tokio::time::timeout`),
  so the abort-on-deadline path falls through to `Drop`.

## `close(0)` force-close path

Different shape: skip the timeout-await pattern. Instead:

```rust
self.sender_force_close.store(true, Release);
self.sender_running.store(false, Release);
self.accumulator.close();
if let Some(handle) = task {
    handle.abort();
    let _ = handle.await;  // observe the cancelled handle
}
```

The trailing `let _ = handle.await` matters for tests — without it,
the JoinHandle is dropped before the spawned task observes its
cancellation, and a follow-up assertion like
`assert!(producer.accumulator.is_closed())` may race the run loop's
exit cleanup.

## `Thread.currentThread() == ioThread` has no Tokio analogue

Java's `flush()` and `close()` both check whether the caller is on
the IO thread (the spawned Sender thread) to avoid self-join
deadlocks. Tokio tasks have no `Thread.currentThread()` comparator —
`tokio::task::id()` returns a unique ID per task but there's no
public API to compare against the spawned Sender's task ID without
plumbing it through the producer. Phase 7e documents this as a
deferred Phase 8 concern: a user calling `flush().await` from inside
their `Callback` body would deadlock the runtime, not the calling
thread (Tokio's runtime is multi-task, not single-thread).

## `DefaultMetadataUpdater` is deferred to Phase 8

>300-LOC inner class on Java's NetworkClient. Drives the metadata
negotiation request/response loop; pulls in `MetadataRequest`/
`MetadataResponse` plumbing through the in-flight tracker. Translating
in isolation would touch four other modules.

Decision: defer to Phase 8 (integration testing milestone) and pair
with broker-loopback testing. Until then, `KafkaProducer::new(props)`
returns `KafkaError::UnsupportedOperation` with a Phase 8 marker,
and unit tests use `new_for_test` with `ManualMetadataUpdater`.

## `Utils.closeQuietly` chain — no Rust analogue

Java's close path calls `Utils.closeQuietly(...)` on interceptors,
metrics, serializers, partitioner, telemetry reporter. Rust analogue:
the `Serializer::close(&mut self)` and `Partitioner::close(&mut self)`
trait methods — but the producer holds them as `Box<dyn Serializer<K>>`
and `Arc<dyn Partitioner>`, where `&mut` access is impossible from
`&self async fn close`. Every Phase 6/7 plug-in has a no-op default
`close()`, so the chain is a no-op anyway. Once a non-trivial impl
lands, the close path adds the chain — likely as `Mutex<Box<dyn>>`
to enable `&mut` from `&self`.

## Hot-path allocation audit (DoD #10)

`partitions_for` allocates a `Vec<PartitionInfo>` (one Vec per call,
NOT per record — this is an introspection method, not the send path).
`flush`, `close`, `metrics` allocate nothing on the per-record hot
path.

`configure_partitioner` runs at construction, not per-send.

## Tests pattern: spawn flush, complete batch, assert promptness

`flush_waits_for_pending_record_to_complete`: append a record, spawn
the flush in a Tokio task, sleep 10ms so the spawned task observes
the begin-flush increment, then `batch.complete(0, 0)` and drive the
per-record future. The flush handle should resolve within 500ms.
Mirror of `RecordAccumulator::tests::await_flush_completion_waits_for_batch_done`
but at the producer level.

`close_with_zero_timeout_force_closes`: append + close(0), then
assert `force_close.load(Acquire) == true` and
`accumulator.is_closed() == true`. Don't assert on the producer.send
result — the run loop may finish or be aborted; both are valid.

`close_is_idempotent`: call close twice, second call must return Ok
within ~100ms (atomic-flag short-circuit). Without the `closed`
atomic, the second call would `take()` an already-`None` JoinHandle
and try to await the nothing → fast no-op anyway, but the explicit
guard makes the contract clear.
