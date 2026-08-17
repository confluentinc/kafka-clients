# Phase P4 — Producer metrics bindings parity + final verification

Wire `Producer::metrics()` (landed Rust-side in Phases P1–P3) through every
binding backend, mirroring exactly what PR #155 (merge `e0f8165e`) did for the
consumer. No new client behaviour — this phase is a pure surface/plumbing pass
that exposes the existing registry snapshot to C, Python, and the gRPC
multilanguage harness, plus an integration test and the final `make verify`
gate.

## Java / prior-art sources

- **PR #155** (`e0f8165e`) — the consumer's metrics-through-bindings work, the
  contract this phase mirrors surface-for-surface:
  - `src/ffi/consumer.rs` — `kafka_consumer_MetricMap_t` + ~12 accessors +
    `box_metric_map` + the `MetricEntry`/`MetricMapInner` snapshot machinery.
  - `bindings/python/_confluentkafka.c` `py_Consumer_metrics`,
    `bindings/python/consumer.py` `metrics()`.
  - `multilanguage-test-server/proto/consumer_service.proto` `Metrics` RPC +
    `Metric`/`MetricList`/`MetricsResponse`.
  - `bindings/{c/grpc_server/server.cc, python/grpc_server.py,
    grpc_server_async.py, grpc_translate.py}` consumer `Metrics` handlers.
  - `tests/common/multilanguage_consumer.rs` `metrics()` forwarding +
    `metric_from_proto`; `tests/integration/multilanguage_consumer_test.rs`
    `metrics_reports_backend_registry`.
- `src/producer/producer_trait.rs:103` — `fn metrics(&self) ->
  HashMap<MetricName, Arc<KafkaMetric>>` (sync; Java `metrics()` does not block).
  Implemented by `KafkaProducer` (P1–P3 registry) and `MockProducer`
  (`mock_metrics`).

## Design decisions

### FFI: extract the machinery to `ffi::common`, keep the surfaces namespaced

- The snapshot representation (`MetricEntry`, `MetricMapInner`), the
  snapshot-building logic (`build_metric_map_inner`), and the index-walking
  accessor helpers (`metric_map_count`, `metric_map_get_name`, …,
  `metric_map_destroy`) move from `src/ffi/consumer.rs` into
  `src/ffi/common.rs` as `pub(crate)`. Both surfaces share them verbatim.
- **ABI is untouched.** Every exported `kafka_consumer_MetricMap_*` symbol keeps
  its name, signature, and behaviour; the bodies now one-line-delegate to the
  shared helpers. Verified: 19 `kafka_consumer_MetricMap` references still emit
  in the regenerated header, and the header still compiles as C11.
- The Rust-side value-kind discriminants (`0=Double, 1=String, 2=Long, 3=Int`)
  were previously the `KAFKA_CONSUMER_METRIC_VALUE_*` consumer constants.
  cbindgen never emitted them into the header (`item_types` is functions/structs/
  typedefs only), so they were never C-ABI. They are consolidated into one
  shared `crate::ffi::common::METRIC_VALUE_*` set; the consumer Rust test and the
  new producer Rust test both reference the shared set.

### MetricMap sharing decision: distinct `kafka_producer_MetricMap_t`

A **distinct producer-namespaced opaque type** (`kafka_producer_MetricMap_t`),
NOT a shared `kafka_common_MetricMap_t`. Rationale:

- The consumer already exports `kafka_consumer_MetricMap_t` (pinned by cbindgen
  + the C tests). Making both surfaces share a `kafka_common_MetricMap_t` would
  require *renaming* the consumer's exported opaque type and its ~12 accessors —
  an ABI break the task explicitly forbids.
- The task's guidance ("prefer a distinct producer-namespaced type UNLESS the
  consumer type is already structured for sharing") resolves to *distinct*: the
  consumer type is namespaced, not pre-generalised.
- Only the *internal* machinery is shared (the C-visible types cannot be), which
  is exactly the "extract the internal machinery" the task asked for. The
  producer's `extern "C"` accessors are thin delegators to the same
  `ffi::common` helpers the consumer now uses.

### MockProducer

`kafka_producer_Producer_metrics` reads through the `Producer` trait, so a
`MockProducer` returns its `mock_metrics` map (empty unless seeded via
`set_mock_metrics`, which the FFI does not expose — same stance as the consumer
FFI toward `MockConsumer`). Verified by `test_mock_producer_metrics_snapshot`.

### Proto: shared messages move to the base file

`producer_service.proto` is the base that `consumer_service.proto` already
imports for the shared messages (`KafkaError`, `Node`, `PartitionInfo`,
`Header`). protoc rejects duplicate type definitions across files in one
package, so `Metric`/`MetricList`/`MetricsResponse` are defined **once** in
`producer_service.proto` and **removed** from `consumer_service.proto`, which
inherits them via the import. This is source-only dedup — identical shapes,
identical field numbers, identical package → wire-compatible. The consumer
`Metrics` RPC still returns the same `MetricsResponse`. Consequence: the
Python/C++ servers reference these three types via the producer proto module
(`pb.*` / no using-prefix) instead of the consumer module (`cpb.*`).

The producer `Metrics` RPC takes a new `MetricsRequest { producer_id }` (the
consumer's takes `ConsumerIdRequest`).

### Python producer `metrics()` is sync on both surfaces

`metrics()` lands on `_ProducerBase` (shared by `Producer` and `AsyncProducer`)
as a plain sync method — Java `metrics()` does not block, so there is no
`_run_sync`/await wrapping. `py_Producer_metrics` in `_confluentkafka.c` mirrors
`py_Consumer_metrics` line-for-line over the `kafka_producer_MetricMap_*`
accessors, returning the same `list[dict]` shape
(`name/group/description/tags/value/kind`).

### gRPC servers + Rust harness

Producer `Metrics` handlers added to `grpc_server.py`, `grpc_server_async.py`
(sync `producer.metrics()`, no await), and `server.cc`
(`ProducerServiceImpl::Metrics` over `kafka_producer_MetricMap_*`).
`grpc_translate._metric_to_proto` is unchanged in behaviour and now shared by
both surfaces (returns `pb.Metric`). `tests/common/multilanguage_producer.rs`
`metrics()` forwards over the `Metrics` RPC (was an empty map), rebuilding each
entry as a `ClosureGauge` (snapshot semantics), with a `block()` helper and a
`metric_from_proto` mirror of the consumer backend.

## Self-review (Definition of Done)

- **DoD #1–#4 (rules / completeness / tests / blockers):** No client behaviour
  added; a pure bindings-parity pass. Every consumer metrics-binding surface has
  a producer counterpart. No TODO/FIXME left.
- **DoD #5 (tests passing):** `cargo test --features ffi --lib` → 3258 passed,
  0 failed. New Rust unit tests: `ffi::producer::tests::{
  metric_map_carries_all_value_kinds_and_tags,
  metric_map_out_of_range_accessors_are_safe, test_mock_producer_metrics_snapshot}`.
  Consumer FFI metric tests still pass (extraction regression-checked).
  Integration test `test_produce_and_check_metrics` compiles under both
  `integration-tests` and `multilanguage-tests`; execution needs the Docker
  broker harness.
- **DoD #6 (no duplication):** the extraction *removes* duplication — one
  metric-map machinery in `ffi::common`, one proto `Metric` definition.
- **DoD #7 (no non-Java structs/traits):** `MetricEntry`/`MetricMapInner` are
  FFI-marshaling helpers (no Java equivalent, required to cross the C boundary),
  identical to what the consumer FFI already had; just relocated.
- **DoD #8 (no TODO/FIXME):** none.
- **DoD #9 (`make verify`):** see the status.md P4 entry for the
  environment-blocked breakdown (cmake/Docker gaps mirror P1–P3).
- **DoD #10 (hot-path allocation audit): N/A.** `metrics()` is a per-call
  snapshot API, invoked administratively, never on the producer send path. The
  snapshot copies metric names/values into owned `CString`s once per call — that
  is the marshaling cost, not a per-record cost. No send-path allocation is
  touched by this phase.
- **DoD #11 (consumer trait surface):** N/A (producer phase). The producer trait
  `metrics()` stays a plain sync `fn`; no `#[async_trait]` bleed.

## ABI / proto compatibility statement

No existing exported C symbol changed name/signature/behaviour. No existing
proto field number changed. The proto change is a source-only relocation of
three identical messages between two files in the same package — wire-compatible.
