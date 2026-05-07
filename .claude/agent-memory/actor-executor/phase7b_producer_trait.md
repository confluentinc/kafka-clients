---
name: Phase 7b — Producer trait shape decisions
description: Async-fn-in-trait choice, deferred methods, UnsupportedOperation variant — load-bearing for Phase 7c (KafkaProducer)
type: project
---

Phase 7b translated `org.apache.kafka.clients.producer.Producer<K, V>`
to a Rust trait at `src/producer/producer.rs`. Several decisions
constrain Phase 7c (KafkaProducer impl) and follow-on phases:

## Trait shape: `async fn`-in-trait, NOT dyn-compat

Used bare `async fn` signatures (Rust 1.75+) so that concrete-typed
callers — which is the primary case (`Arc<KafkaProducer<K,V>>`,
generic `fn run<P: Producer<K,V>>(p: P)`) — avoid per-call
`Pin<Box<dyn Future>>` allocation per CLAUDE.md rule 11.

**Cost**: `Box<dyn Producer<K, V>>` will not compile. This is
acceptable because the Java `Producer` interface exists primarily
for `MockProducer` substitution in tests, and Rust's idiom for
that is generic-bound dispatch (`fn run<P: Producer<K,V>>(...)`)
not dyn dispatch.

**Why**: This conflicts with making `Producer` a trait object,
but the user explicitly asked us to prefer (a) over the
associated-future-type alternative.

**How to apply**: Phase 7c's `KafkaProducer` should implement
methods directly (no boxing). `MockProducer` (out of milestone) and
any future test doubles should also impl the trait directly. Tests
that want to substitute should be generic over `P: Producer<K, V>`,
not hold `Box<dyn Producer<K, V>>`.

## `send` collapses Java's `Future<RecordMetadata>` into one async

Java's `send()` returns immediately with a `Future` that the
caller can ignore. Rust's `async fn send(...) -> Result<...>`
resolves only on broker ack — the enqueue + ack are joined.
Callers wanting fire-and-forget must `tokio::spawn` themselves.

**Why**: Keeping `send` as `async fn` (no `Pin<Box>`) requires this
collapse — there's no clean way to return both an enqueue-future
and an ack-future without erasing one of them. Documented in the
trait's module-level docs.

**How to apply**: Phase 7c's `KafkaProducer::send` impl awaits the
enqueue (RecordAccumulator append) THEN awaits the resulting
`FutureRecordMetadata`. Test code that wants Java-like
"fire-and-forget then await later" semantics should `tokio::spawn`
the future.

## Three Java methods deferred from the trait

These are NOT declared yet because they need types from
unmtranslated packages:

1. `sendOffsetsToTransaction` — needs `OffsetAndMetadata` and
   `ConsumerGroupMetadata` from `org.apache.kafka.clients.consumer`.
   **Phase 9** (transactional producer) will add the consumer
   types and reintroduce the method.

2. `registerMetricForSubscription` and
   `unregisterMetricFromSubscription` — need
   `org.apache.kafka.common.metrics.KafkaMetric`. The metrics
   surface is fully stubbed in Milestone-1; once metrics types are
   translated, these methods will be re-added with
   `UnsupportedOperation` stubs (matching `metrics()` policy).

The deferral is documented in the trait's module-level rustdoc.

## `KafkaError::UnsupportedOperation(String)` is the new stub variant

Added alongside `IllegalArgument` / `IllegalState`. Maps to
`UnsupportedOperationException` for `java_class_name()`, neither
retriable nor fatal, uses `ERR_CODE_CONFIG`. Used by:
- All transactional methods on the `Producer` trait (Milestone-1)
- `Producer::client_instance_id` (telemetry stub)
- (Future) consumer-side methods that depend on Milestone-1+
  features when they're added

## `ProducerMetrics = HashMap<String, ()>` placeholder

Public type alias for the `metrics()` return. The `()` value type
will be replaced when `MetricName` / `KafkaMetric` are translated;
existing call sites that only inspect `is_empty()` / `len()` keep
compiling. New call sites should treat it as opaque.

## Test pattern: hand-rolled `StubProducer` for trait shape verification

The test mod doesn't instantiate `KafkaProducer` (Phase 7c). It
declares a `StubProducer` (zero-sized struct), implements all
trait methods with the Milestone-1 contract, and verifies:
- Sync methods dispatch via generic `fn check<P: Producer<...>>(p)`
- Async methods dispatch through the trait
- Telemetry path's error message is asserted byte-exact

This pattern is reusable for any trait phase where the concrete
impl lands later — keep the trait file's tests focused on shape,
not behaviour.
