# Phase 3: Share fetch data path

## Goal

Translate the share fetch data path — the receive-side pipeline that turns a
`ShareFetchResponse` into user-visible `ConsumerRecord`s while tracking acquired
records for acknowledgement. Governed by the receive-path zero-copy contract
(`consumer-threading.md §27`): `ShareCompletedFetch` owns one buffer, everything
downstream borrows.

## Branch

`milestone9-share-consumer`.

## Java sources

All paths relative to
`kafka/clients/src/main/java/org/apache/kafka/clients/consumer/internals/`,
submodule commit `a18251bae0b825c69794a50dffd4c3100cf5ca5b`.

- `ShareFetch.java`
- `ShareCompletedFetch.java`
- `ShareFetchBuffer.java`
- `ShareFetchCollector.java`
- `NodeAcknowledgements.java` (used by `ShareFetch.takeAcknowledgedRecords`)
- `ShareInFlightBatch.java` — `takeInFlightRecords` addition

`ShareFetchException` is the Rust unification of Java's two collector exit paths
(a bare `KafkaException` from `initialize`, and the records-branch failure).

Tests:

- `ShareCompletedFetchTest`, `ShareFetchBufferTest`, `ShareFetchCollectorTest`
- Per-record allocation-budget test (§27)

## Rust output

- `src/consumer/internals/share_fetch.rs` (`ShareFetch`)
- `src/consumer/internals/share_completed_fetch.rs` (`ShareCompletedFetch` —
  §27 zero-copy: single owned buffer moved into the cursor, `topic_arc: Arc<str>`
  cloned per record, decompress-once, key/value borrowed)
- `src/consumer/internals/share_fetch_buffer.rs`
- `src/consumer/internals/share_fetch_collector.rs`
- `src/consumer/internals/share_fetch_exception.rs`
- `src/consumer/internals/node_acknowledgements.rs`

Metrics omitted (KIP-714): metrics-manager params and call-sites dropped.

## Commits

- `507fc3f` — share consumer fetch data path (KIP-932)
- `26b943f` — fixup: corrupt-batch CRC error propagation + test-fidelity items

## Verification

- `cargo build`, `cargo test`, `cargo xtask format-check`, `cargo xtask lint` — clean.
- All 10 `ShareCompletedFetchTest`, 4 `ShareFetchBufferTest`, and
  `ShareFetchCollectorTest` methods translated.
- Per-record allocation-budget test passes (a genuine bound: ≤5/record + 120
  overhead, and ≥1/record so it can't pass vacuously).
