---
name: phase7a-design-notes
description: Milestone-8 Phase 7a design decisions and translation patterns for receive-path foundation
metadata:
  type: project
---

Milestone-8 Phase 7a — Receive-path foundation + prereqs.

**Why:** Sets up the building blocks the fetch loop (7b), topic metadata (7c), and offset fetch (7d) all depend on.

**How to apply:** Use these patterns when extending the fetch path in 7b/7c/7d. Refer to commits 518768b..f8d02b9 (8 commits).

# Key decisions

## TopicIdPartition lives in `src/common/topic_id_partition.rs`
Java: `org.apache.kafka.common.TopicIdPartition`. Mirrors TopicPartition layout (file naming, `pub` accessors, Hash by both topic_id and topic_partition). Display format is `<uuid>:<topic>-<partition>`.

## FetchRequest/Response wrappers vs auto-generated *Data
Same as MetadataRequest/Response precedent: wrap the auto-generated FetchRequestData/FetchResponseData in a thin struct + Builder. Phase 7a only translates the consumer-side surface; `forReplica` / `SimpleBuilder` / `sizeOf` / `toMessage` are deferred (server-side).

The ConcreteRequest/Response enum is intentionally NOT extended yet — the FetchRequest builder is consumed by Phase 7b's RequestManager which will send the bytes directly via NetworkClient::send.

## FetchSessionHandler API shape change vs Java
Java models Builder as inner class that mutates the outer handler on `build()`. Rust translation moves the mutation:
```rust
let mut builder = handler.new_builder();
builder.add(tp, partition_data);
let data = handler.build_request(builder);  // mutates handler
```
Inner class `FetchRequestData` renamed to `FetchSessionRequestData` in Rust to avoid collision with the auto-generated `fetch_request_data::FetchRequestData`.

## AbstractFetch is a concrete struct, not a trait
Java inheritance maps poorly to Rust. Phase 7b's FetchRequestManager composes `abstract_fetch: AbstractFetch` as a field. Protected Java fields become `pub(crate)` (matches the ConsumerMetadata/ProducerMetadata composition-over-inheritance precedent).

`prepareFetchRequests` (the largest method) is intentionally deferred to Phase 7b — Phase 7a exposes the building blocks (`create_fetch_request`, `session_handler_or_create`, `handle_fetch_success`, etc.) so 7b can either delegate or implement directly.

## §27 zero-copy contract in CompletedFetch
- `partition_data.records: Option<Vec<u8>>` is the canonical owner.
- `MemoryRecords::readable_records` is called once per CompletedFetch on first iteration (NOT per record).
- `topic: Arc<str>` is cloned cheaply per ConsumerRecord — no `String::from_utf8`.
- Headers are owned `RecordHeaders` cloned from `DefaultRecord::headers()` per the §27 milestone-8 ruling.
- Iteration is lazy via `BatchCursor`: at most one batch's records are materialized at a time (via `DefaultRecordBatch::iter_records` which returns `Vec<DefaultRecord>` for that batch only).
- No per-record `tokio::spawn`. CompletedFetch is sync end-to-end.

The per-record allocation-budget regression test lives with Phase 7b's `FetchCollector` test (since 7b exercises the full receive path end-to-end).

## Tests skipped in Phase 7a (with rationale)
- `CompletedFetchTest.testAbortedTransactionRecordsRemoved` /
  `testCommittedTransactionRecordsIncluded`: requires `ControlRecordType` / `EndTransactionMarker` / `MemoryRecords::write_end_transactional_marker`, none of which are implemented in the Rust `common::record` module yet. Aborted-transaction filtering at the CompletedFetch level IS implemented; only the end-to-end transactional test fixtures are missing.
- `CompletedFetchTest.testCorruptedMessage`: replaced by a simpler regression test (`test_key_deserialization_failure_caches_error`) that exercises the same code path without UUIDSerializer/Deserializer fixtures.
- `FetchSessionHandlerTest.testDoubleBuild`: the Rust builder is consumed by `build_request`, so calling twice is a compile-time error — enforced by the type system.

## Reaffirmed patterns from prior phases
- `#![allow(dead_code)]` at module level for new files until call sites are wired (the lib has `#![deny(warnings)]`).
- `Mutex` not `tokio::Mutex` for FetchBuffer / BufferSupplier — critical sections never await (CLAUDE.md §9).
- `await_wakeup` uses `tokio::sync::Notify::notified()` + `tokio::time::timeout` — Java's InterruptException is dropped (cancellation flows through the consumer's wakeup token per consumer-threading.md §11).
- IndexMap replaces Java's LinkedHashMap where insertion order matters for wire ordering.
- Generated PartitionData.records is `Option<Vec<u8>>` — that's the buffer the receive-path borrows from.

## Wire-format guards on tests
- Test that uses pointer-equality to verify allocation reuse (`buffer.as_ptr()`) is unreliable on the system allocator — addresses get reused. Check the pool's internal state (queue length) instead.

## Phase 7a final state
- Tests: lib 1201 passing (vs 1113 baseline = +88 new tests across 8 new files), integration 36 holding.
- No panics, no unimplemented!, no todo! in production code.
- All gates clean: build, test, format-check, lint, serial-test (13s, no hangs).
- 8 commits, ~3.3K LOC additions in source + tests.
