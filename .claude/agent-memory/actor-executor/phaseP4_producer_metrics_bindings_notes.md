---
name: phaseP4-producer-metrics-bindings
description: M12 P4 — Producer::metrics() through FFI/Python/gRPC bindings; shared metric-map machinery, proto dedup gotcha
metadata:
  type: project
---

# Phase P4 — Producer metrics bindings parity

Branch `producer-metrics`, Actor N=4. Mirrors consumer PR #155 (`e0f8165e`)
surface-for-surface to expose `Producer::metrics()` through every binding.

## Key facts / patterns

- **FFI metric-map machinery is now shared in `src/ffi/common.rs`**:
  `MetricEntry`, `MetricMapInner`, `build_metric_map_inner()`, and the
  `metric_map_*` index-walking helpers (all `pub(crate)`). Consumer and producer
  `extern "C"` accessors are thin one-line delegators casting their namespaced
  opaque `*const kafka_{consumer,producer}_MetricMap_t` to `*const MetricMapInner`.
- **Opaque types stay namespaced** (`kafka_consumer_MetricMap_t`,
  `kafka_producer_MetricMap_t`) — NOT a shared `kafka_common_MetricMap_t`,
  because the consumer type is ABI-pinned (cbindgen + C tests); sharing would
  rename its symbols. Only internal machinery is shared.
- **Value-kind consts** (`0=Double,1=String,2=Long,3=Int`) are `pub(crate) const
  crate::ffi::common::METRIC_VALUE_*` — cbindgen never emitted them (item_types =
  functions/structs/typedefs), so they are NOT C-ABI; the old
  `KAFKA_CONSUMER_METRIC_VALUE_*` consumer consts were removed and tests point at
  the shared set. Removing a `pub const` from a `pub(crate) mod` when it becomes
  test-only fires `dead_code` under `#![deny(warnings)]` — consolidate, don't
  leave orphan consts.
- **Header regen:** `touch cbindgen.toml && cargo build --features ffi --lib`
  (build.rs regenerates `target/include/confluent_kafka.h` when `ffi` on). The
  header is a build artifact (git-untracked). Add the new type to
  `cbindgen.toml [export].include`.

- **PROTO GOTCHA (cost me a build):** `consumer_service.proto` **imports**
  `producer_service.proto` and both share `package confluent.kafka.test`. Shared
  messages (KafkaError/Node/PartitionInfo/Header) live ONLY in producer proto.
  So `Metric`/`MetricList`/`MetricsResponse` must be defined ONCE in
  producer_service.proto and REMOVED from consumer_service.proto (inherited via
  import) — protoc errors "already defined in file" if duplicated across files
  in one package. Consequence: Python/C++ servers reference them via the PRODUCER
  module (`pb.*` / no using-prefix), NOT the consumer module (`cpb.*`) — had to
  flip `cpb.Metric*` → `pb.Metric*` in grpc_translate.py/grpc_server*.py.
  Field numbers unchanged → wire-compatible. protoc is vendored in the Rust build
  (`protoc_bin_vendored`) so `cargo build --features multilanguage-tests`
  regenerates; no system protoc needed (system protoc IS missing here).

- **Python producer `metrics()`** lives on `_ProducerBase` (shared by sync
  `Producer` + `AsyncProducer`) as a plain sync method — Java metrics() doesn't
  block, no `_run_sync`. `py_Producer_metrics` in `_confluentkafka.c` takes the
  `Producer*` struct (`producer->producer`), mirrors `py_Consumer_metrics`.

- **Rust multilanguage backend** `tests/common/multilanguage_producer.rs`:
  sync trait `metrics()` needs `self.block(fut)` =
  `block_in_place(|| Handle::current().block_on(fut))` (same as consumer
  backend); rebuild entries as `ClosureGauge` (snapshot, not live).

- **Integration test** `test_produce_and_check_metrics` in
  `tests/integration/producer_test.rs` — registered BOTH via
  `crate::multilanguage_test!` and in the `rust_only_fallback` mod. Value
  assertions robust after produce+flush: record-send-total>=N, batch-size-avg>0,
  request-latency-avg present, buffer-total>0 & available<=total,
  flush-time-ns-total>0. Producer client-level metrics carry only `client-id`
  tag, so name-match with `!tags.contains_key("topic")` disambiguates
  client-level from per-topic.

## Environment gate (mirrors P1-P3)
`make verify` blocked at `build-c` (cmake MISSING; system protoc MISSING; Docker
present). Runnable: `cargo xtask format-check`, `cargo xtask lint`, `cargo test
--features ffi --lib` (3258 pass), `cargo clippy --features ffi --lib`,
`--features multilanguage-tests --test integration`. C++ server.cc + Python
_confluentkafka.c are compile-pending a grpc/cmake env.
