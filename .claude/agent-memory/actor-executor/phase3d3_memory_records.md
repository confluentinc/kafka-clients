---
name: Phase 3d-3 memory_records landing notes
description: MemoryRecords + UnalignedMemoryRecords + RecordsSend + DefaultRecordsSend translation; what landed, what's deferred for 3d-4 and Phase 5
type: project
---

Phase 3d-3 status. **Why:** Third sub-chunk of the v2 record codec; the
class is the `Records`/`TransferableRecords` concrete impl tying
together Phase 3d-1's `DefaultRecord` and 3d-2's `DefaultRecordBatch`
under one buffer. **How to apply:** When Phase 3d-4 starts, follow these
breadcrumbs to add `MemoryRecordsBuilder` and the `withRecords(...)`
factories on top.

## What landed

- `src/common/record/memory_records.rs` — `MemoryRecords` struct
  wrapping `bytes::Bytes`. Implements `BaseRecords`,
  `TransferableRecords`, `Records`. Surface: `readable_records`,
  `slice_inner`, `Records::slice` (zero-copy via `Bytes::slice`),
  `first_batch_size`, `valid_bytes`, `to_send`, `Records::batches`,
  `Records::records`, `empty()` singleton, `Display`, `PartialEq`,
  `Hash`, `Clone`. ~720 lines incl. tests.
- `src/common/record/unaligned_records.rs` — `UnalignedRecords` marker
  trait extending `TransferableRecords`. No methods (Java's
  `default toSend()` is on the concrete impl in Rust — see below).
- `src/common/record/unaligned_memory_records.rs` —
  `UnalignedMemoryRecords` wrapping `bytes::Bytes`. Implements
  `BaseRecords`, `TransferableRecords`, `UnalignedRecords`. Has
  `to_send()` and `empty()` singleton. ~110 lines incl. tests.
- `src/common/record/records_send.rs` — `RecordsSend<R: BaseRecords>`
  state-tracking core. Stores records, max_bytes_to_write, remaining,
  pending. Exposes `advance(written)` + `set_pending(pending)` for the
  Phase 5 write loop to drive. The actual `write_to(channel)` body
  lives in Phase 5. ~150 lines incl. tests.
- `src/common/record/default_records_send.rs` —
  `DefaultRecordsSend<R: TransferableRecords>` typed wrapper. Two
  constructors mirroring Java's `(T)` and `(T, int)` overloads. ~120
  lines incl. tests.

## Surprising bits worth recording

### `Records::batches()` silently truncates on corrupt batches

The trait was defined in Phase 3a as `fn batches() -> Box<dyn
Iterator<Item = Box<dyn RecordBatch + 'a>>>` — no `Result`. Java's
iterator throws `CorruptRecordException` mid-iteration; Rust can't
panic per CLAUDE.md rule 10. We `filter_map(|r| r.ok())` and stop at
the first error. **Behavioural difference:** the corruption diagnostic
is silently dropped. Callers that need it use `MemoryRecords::valid_bytes`
(which sums until corruption) or reach into the underlying
`RecordBatchIterator` directly (crate-private).

### `Records::records()` per-record allocation

`DefaultRecordBatch::iter` returns `Box<dyn Record + 'a>` borrowing from
the batch buffer. To flatten across batches we must drop each batch
between iterations, but the trait objects' lifetimes prevent that. The
workaround: per-batch, downcast each record back into a concrete
`DefaultRecord` via the trait accessors (cloning headers, slicing
key/value through `Bytes::copy_from_slice`). This means
`Records::records()` does pay a per-record allocation:

* `Vec<RecordHeader>` clone for headers
* `Bytes::copy_from_slice` for key + value (a copy)

This is the **consumer-side flat-records iterator** (broker rewriting,
metrics) and is **not on the producer hot path**. The producer goes
through `MemoryRecordsBuilder` (Phase 3d-4) and
`DefaultRecordBatch::iter` directly, both of which preserve zero-copy.

### `firstBatchSize` early-out vs `next_batch_size` validation

Java's `firstBatchSize()` has a separate `if (buffer.remaining() <
HEADER_SIZE_UP_TO_MAGIC) return null;` guard *before* calling
`nextBatchSize()`. Without it, a partial header (LOG_OVERHEAD bytes
present but not magic) would trigger SIZE-field validation in
`nextBatchSize` and could raise `CorruptRecordException` for a buffer
that simply hasn't received enough bytes yet. Mine had this bug —
caught during self-review. Fixed by replicating the early-out.

### `to_send()` lives on concrete impls, not the trait

Java declares `toSend()` as a `default` method on `UnalignedRecords`
(returns `RecordsSend<? extends BaseRecords>`) and as an override on
`AbstractRecords` (returns `DefaultRecordsSend<Records>`). In Rust,
`DefaultRecordsSend<R>` requires `R: Sized`, which trait objects
cannot satisfy. So `to_send()` lives on `MemoryRecords` and
`UnalignedMemoryRecords` directly, each returning a concretely-typed
send. Callers that need to abstract over them at the trait level can
take ownership and `match` — but the producer path knows the concrete
type, so this isn't a real friction point.

### `RecordsSend::write_to(channel)` deferred to Phase 5

The Phase 3d-3 `RecordsSend` ships only the *state-tracking core*. The
Java class's abstract subclass dispatch (`writeTo(channel,
previouslyWritten, remaining)`) is bound up with `TransferableChannel`
which lives in `common/network/*` — Phase 5 territory. We expose
`advance(written)` + `set_pending(pending)` so the Phase 5 actor can
compose the write loop without redesigning the bookkeeping. **Phase 5
follow-up:** add a trait `WriteRecordsTo` defining the per-subclass
write method, and add `RecordsSend::write_to(channel)` that runs the
loop using `advance` / `set_pending`.

## Tests deferred to Phase 3d-4 (need `MemoryRecordsBuilder`)

These Java `@Test` / `@ParameterizedTest` methods in
`MemoryRecordsTest.java` all build batches via `MemoryRecords.builder(...)`
or `MemoryRecords.withRecords(...)`. Translating them now would
require a temporary inline builder that gets ripped out in 3d-4 — not
worth the churn. They're documented per-test in the
`memory_records::tests` module's docstring.

- `testIterator` — builder API, parameterized over magic & compression
- `testHasRoomForMethod` — builder API
- `testHasRoomForMethodWithHeaders` — builder API
- `testChecksum` — `withRecords`
- `testNextBatchSize` — partially deferred. The read-path subset
  (firstBatchSize semantics) is translated; the
  builder-construction half is deferred.
- `testWithRecords` — `withRecords`
- `testUnsupportedCompress` — `withRecords` + magic-version
  compression compatibility (magic v0/v1 are out of scope per
  PLAN.md, so this test is doubly inapplicable)

## Tests deferred to Phase 6+ (need `filterTo` + nested classes)

`filterTo`, `RecordFilter`, `FilterResult` plus
`writeEndTransactionalMarker`, `withLeaderChangeMessage`, KRaft
control-record builders all need `MemoryRecordsBuilder` *and*
control-record / txn-marker plumbing that's later in the plan.

- `testFilterToPreservesPartitionLeaderEpoch`
- `testFilterToEmptyBatchRetention`
- `testEmptyBatchRetention`
- `testEmptyBatchDeletion`
- `testBuildEndTxnMarker`
- `testBaseTimestampToDeleteHorizonConversion`
- `testBuildLeaderChangeMessage`
- `testFilterToBatchDiscard`
- `testFilterToAlreadyCompactedLog`
- `testFilterToPreservesProducerInfo`
- `testFilterToWithUndersizedBuffer`
- `testFilterTo`
- `testFilterToPreservesLogAppendTime`

## Tests translated (read-path coverage)

- `testSlice` — full coverage of slicing semantics across batch
  boundaries, slice-of-slice, position+size overflow clamping
- `testSliceInvalidPosition` (with the `position > limit` boundary
  pinned in `slice_at_end_position_returns_empty`)
- `testSliceInvalidSize`
- `testSliceEmptyRecords`
- `testSliceForAlreadySlicedMemoryRecords`
- `testNextBatchSize` (read-path subset — firstBatchSize semantics)

Plus Rust-only coverage:
- Empty `MemoryRecords` is well-formed
- `slice` is zero-copy (pointer-aliasing assertion)
- `valid_bytes` truncates partial trailing batches
- Records iterator flattens across multi-batch buffers
- `Display` / `equals` / `hash` / `Clone` semantics
- `to_send()` produces a sized `DefaultRecordsSend`

## Test count delta

Baseline before 3d-3: 511 lib tests.
After 3d-3: **545 lib tests (+34: 21 in memory_records, 4 in
unaligned_memory_records, 5 in records_send, 4 in
default_records_send)**.

Generator tests unchanged at 74.

## Files added

- `src/common/record/memory_records.rs`
- `src/common/record/unaligned_records.rs`
- `src/common/record/unaligned_memory_records.rs`
- `src/common/record/records_send.rs`
- `src/common/record/default_records_send.rs`

## Files modified

- `src/common/record/mod.rs` — added 5 new module declarations + 5
  re-exports.

## Phase 3d-4 follow-ups

1. Translate `MemoryRecordsBuilder`. It will be the actual constructor
   for batches; `MemoryRecords::with_records(...)` factory variants
   will then be thin wrappers.
2. Replace the duplicated `build_batch` test helpers in
   `memory_records::tests`, `default_record_batch::tests`,
   `byte_buffer_log_input_stream::tests`, `record_batch_iterator::tests`
   with calls into the builder. Move the duplicate to a shared
   `#[cfg(test)]` module.
3. Translate the tests deferred above (the builder-API subset).

## Phase 5 follow-ups

1. Translate `TransferableChannel` in `common/network/*`.
2. Add a `Send` trait (Java `org.apache.kafka.common.network.Send`) and
   wire `RecordsSend` / `DefaultRecordsSend` to implement it.
3. Add `RecordsSend::write_to(channel)` using `advance` +
   `set_pending` to drive the loop. The subclass dispatch becomes a
   trait `WriteRecordsTo` (or a closure stored on `RecordsSend`) so
   `DefaultRecordsSend::write_to(channel, prev, rem)` delegates to
   `records.write_to(channel, prev, rem)`.
