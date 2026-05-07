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

## Phase 7c carry-over notes

Verified against `kafka/clients/src/main/java/org/apache/kafka/clients/producer/KafkaProducer.java`
during the Phase 7b Round 1 fixup (Critic 7 Suggestion 1):

- `KafkaProducer.java` defines exactly one transaction-init method:
  `public void initTransactions()` at line 648. **No** `initTransactions(boolean keepPreparedTxn)`
  overload exists in this 4.2 source.
- `prepareTransaction` does **not** exist on `KafkaProducer` — it only
  appears on `internals/TransactionManager.java:342`, which is package-
  private internal API and is not exposed on either the `Producer`
  interface or `KafkaProducer`.

Phase 7c **MUST** re-verify against `KafkaProducer.java` whether either
of these methods has appeared since (e.g. via a back-port). If yes,
they translate as inherent `impl KafkaProducer` methods (not trait
methods on `Producer`). If no, they remain absent. In either case the
Milestone-1 behavior is `KafkaError::UnsupportedOperation` per the skip
list above.
