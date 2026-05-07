# Phase 7 — Public Surface

**Goal:** Wire the Phase 6 internals into the public `KafkaProducer` API.

**Plan reference:** `design/history/Milestone-1/PLAN.md` lines 314–334.

## Sub-phase ladder

| # | Scope |
|---|---|
| 7a | `ProducerConfig` + `ProducerConfigTest` |
| 7b | `Producer` trait + gap-fill `ProducerRecordTest` / `RecordMetadataTest` against Java |
| 7c | `KafkaProducer` skeleton: constructor, validation, dependency wiring, Sender spawn, Drop/close skeleton |
| 7d | `KafkaProducer::send` hot path: serialize → interceptors → partition → accumulator |
| 7e | `KafkaProducer::flush/close/partitions_for/list_topics/metrics-stub`; transactional methods → `UnsupportedOperation` |
| 7f | `KafkaProducerTest` non-transactional translation |

Each sub-phase ends green on
`cargo build && cargo test && cargo xtask format-check && cargo xtask lint`.

## Skip list (rejected at construction or stubbed)

- `enable.idempotence=true` → `ConfigException`
- `transactional.id` set → `ConfigException`
- `security.protocol ∈ {SASL_PLAINTEXT, SASL_SSL}` → rejected (Phase 9 re-enables)
- `init/begin/commit/abortTransaction`, `sendOffsetsToTransaction` → `KafkaError::UnsupportedOperation`
- `MockProducer` — out of milestone
- `metrics()` → empty map (metric-stub pattern)
- `clientTelemetryReporter` → `None`

## Comment files

- Open: `COMMENTS.7.md`
- Resolved: `COMMENTS.DONE.7.md`
