# COMMENTS.DONE.1.md — Critic 1 review of Milestone-8 Phase-7a (resolved)

All 16 findings from `COMMENTS.1.md` have been resolved. Per-finding
resolution summary; details in the listed fixup commits.

---

## Issue 1: §27 zero-copy violation — `Arc<str>` topic name freshly allocated PER RECORD

- **Resolution**: Added `topic_arc: Arc<str>` field to `CompletedFetch`,
  initialized once in `new` / `new_full` from `partition.topic()`.
  Per-record code uses `Arc::clone(&self.topic_arc)` (atomic pointer
  bump) instead of `Arc::from(&str)` (allocation + UTF-8 copy).
- **Fix commit**: `fixup! 4526740 ... CompletedFetch §27 zero-copy + READ_COMMITTED behavior`
- **Status**: Resolved.

---

## Issue 2: §27 zero-copy violation — `DefaultRecord::clone()` per record on peek path

- **Resolution**: Restructured the per-record loop:
  - Renamed `next_fetched_record` → `advance_to_next_fetched_record`.
    Returns `Result<bool, KafkaError>`; the record stays in the cursor
    at `cursor.current_records[cursor.record_index]`.
  - Renamed `peek_last_fetched_record` → `peek_current_record`. Now
    returns `Option<(&DefaultRecord, &BatchMetadata)>` (borrowed) —
    no clone.
  - `fetch_records` reads all record fields (key/value/offset/timestamp/
    headers) inside a scoped borrow, then drops the borrow before
    advancing the cursor index.
- **Fix commit**: same as #1.
- **Status**: Resolved.

---

## Issue 3: Test coverage gap — `testCorruptedMessage` structured fields

- **Resolution**: Picked option (b): wrote two new tests that exercise
  the "first record valid, second corrupt" pattern with the cached
  exception re-raise on subsequent calls. Added a
  `new_records_with_keyed_offsets` fixture and a
  `MaybeFailingDeserializer` that fails on a specific offset.
  - `test_corrupted_message_key_fails_after_valid_record`
  - `test_corrupted_message_value_fails_after_valid_record`
- **Fix commit**: same as #1.
- **Status**: Resolved (within the limits of
  `KafkaError::Serialization(String)` — a structured
  `RecordDeserializationException` is deferred since adding it would
  touch every match site).

---

## Issue 4: Behavior mismatch — `isBatchAborted` skips `isTransactional()`

- **Resolution**: Gate the READ_COMMITTED skip on
  `batch_meta.is_transactional && !is_control_batch &&
  aborted_producer_ids.contains(...)`, matching Java's
  `AbstractRecordBatch.isTransactional` precondition on
  `isBatchAborted`.
- **Fix commit**: same as #1.
- **Status**: Resolved.

---

## Issue 5: Missing behavior — `containsAbortMarker` cleanup path

- **Resolution**: Picked option (b)/(c): documented as a known
  limitation in the module docstring AND we now return
  `KafkaError::unsupported_version` when the code encounters a control
  batch from a producer ID in `aborted_producer_ids`. This is the
  signal that the missing `ControlRecordType` translation would have
  used to drop the producer ID from the aborted set. Better to fail
  loudly than to silently drop records. Phase 7b/c will translate
  `ControlRecordType` and re-enable the cleanup path.
- **Fix commit**: same as #1.
- **Status**: Resolved (with the documented limitation; tracking via
  the module docstring and this comment).

---

## Issue 6: Missing methods on `AbstractFetch` — close-session handlers

- **Resolution**: Added two `pub(crate)` methods mirroring
  `AbstractFetch.java:281-296`:
  - `handle_close_fetch_session_success(fetch_target, request_data)`
  - `handle_close_fetch_session_failure(fetch_target, request_data, error)`
  Both call `remove_pending_fetch_request` and log at debug. Tests
  added: `test_handle_close_fetch_session_success_drops_pending`,
  `test_handle_close_fetch_session_failure_drops_pending`.
- **Fix commit**: `fixup! f8d02b9 ... AbstractFetch close-session handlers + ctor param`
- **Status**: Resolved.

---

## Issue 7: `FetchSessionHandlerTest` parameterized test coverage gaps

- **Resolution**: All missing parameter combinations now run in `for`
  loops within the existing test methods:
  - `test_id_usage_revoked_on_id_downgrade` now loops `partition=[0, 1]`.
  - `test_topic_id_replaced` now loops over all four
    `(startsWithTopicIds, endsWithTopicIds)` combinations.
  - `test_session_epoch_when_mixed_usage_of_topic_ids` now loops over
    both `startsWithTopicIds` values.
  - `test_id_usage_with_all_forgotten_partitions` now loops over both
    `useTopicIds` values.
  - Added
    `test_verify_full_fetch_response_partitions_with_topic_ids`
    translating `testVerifyFullFetchResponsePartitionsWithTopicIds`.
- **Fix commit**: `fixup! 040bdd9 ... FetchSessionHandler param tests + pub(crate) data + drop unused flag`
- **Status**: Resolved.

---

## Issue 8: `FetchBuffer::await_wakeup` — `Notify::notified()` race window

- **Resolution**: Use the prescribed `tokio::pin!(notified)` +
  `notified.as_mut().enable()` pattern. The waiter is registered
  BEFORE the second flag check; any concurrent `notify_waiters` after
  the registration is guaranteed to find us as a registered waiter.
  Added `test_await_wakeup_does_not_lose_race` to exercise the
  yield-then-wakeup ordering.
- **Fix commit**: `fixup! 3cc23a3 ... FetchBuffer race-free await_wakeup + drain-on-replace`
- **Status**: Resolved.

---

## Issue 9: `FetchBuffer::set_next_in_line_fetch(None)` doesn't drain previous

- **Resolution**: `set_next_in_line_fetch` now calls `drain()` on the
  previous `next_in_line_fetch` (if any) before replacing it. Mirrors
  Java's `retainAll` / `close` semantics. Added
  `test_set_next_in_line_fetch_drains_previous`.
- **Fix commit**: same as #8.
- **Status**: Resolved.

---

## Issue 10: `AbstractFetch::new` allocates a fresh `BufferSupplier`

- **Resolution**: `AbstractFetch::new` now takes
  `decompression_buffer_supplier: Arc<BufferSupplier>` as a constructor
  parameter (Java's 9th constructor arg). Callers that don't need
  sharing can pass `Arc::new(BufferSupplier::create())`.
- **Fix commit**: same as #6.
- **Status**: Resolved.

---

## Issue 11: `BufferSupplier::get` returns uninitialized — Java zeroed

- **Resolution**: Picked option (2): document the divergence in the
  `get()` rustdoc, including the info-leak surface (mirror of Java's
  `clear()`-without-zero semantic on a recycled buffer). The fix is
  doc-only because `BufferSupplier::get` is dead code in Phase 7a.
  Phase 7b/c callers who use the supplier for decompression will see
  the doc and decide whether they need `resize(size, 0)` semantics.
- **Fix commit**: `fixup! 518768b ... BufferSupplier::get doc clarifies Java divergence`
- **Status**: Resolved.

---

## Issue 12: `iter_records()` eagerly materializes — §27 lazy intent

- **Resolution**: Documented in the `CompletedFetch` module docstring
  (the "Known limitation (tracked for Phase 7b/c)" paragraph). Within
  a batch, `iter_records()` materializes a `Vec<DefaultRecord>` eagerly
  — to make this fully lazy at the record granularity, the underlying
  `DefaultRecordBatch` needs a streaming iterator. The cross-batch lazy
  property is preserved.
- **Fix commit**: same as #1 (the docstring update).
- **Status**: Resolved (documented as Phase 7b/c follow-up).

---

## Issue 13: `FetchSessionRequestData` is `pub` — should be `pub(crate)`

- **Resolution**: `FetchSessionRequestData` and all its fields are now
  `pub(crate)`, matching Java's package-private visibility on
  `FetchSessionHandler.FetchRequestData`. `build_request` also
  `pub(crate)` since its return type is internal. The handler itself
  remains `pub` (Java's `FetchSessionHandler` is `public`).
- **Fix commit**: same as #7.
- **Status**: Resolved.

---

## Issue 14: `FetchSessionHandler` lacks `Drop`/explicit close

- **Resolution**: Informational. Java's class is not `AutoCloseable`
  either — both rely on `prepareCloseFetchSessionRequests` being called
  before drop. Tagged for Phase 7b: the `FetchRequestManager` must
  invoke this on shutdown.
- **Status**: Tagged for Phase 7b. No 7a action.

---

## Issue 15: Doc comment on `FetchResponse::topic_ids` misleading

- **Resolution**: Rewrote the doc to be honest about what the
  implementation actually does (unconditional non-zero filter) rather
  than overstating "always returns the empty set on v12". Notes that
  Java has the same version-agnostic implementation.
- **Fix commit**: `fixup! 2c2d915 ... FetchResponse::topic_ids rustdoc accuracy`
- **Status**: Resolved.

---

## Issue 16: `new_builder_sized(_, copy_session_partitions)` silently ignored

- **Resolution**: Picked option (2): removed the parameter from the
  Rust API entirely. Documented the Java divergence in the rustdoc —
  the Rust diff path makes the flag a semantic no-op, and no Phase-7a
  call site depends on it. If a future caller needs the `true`
  behavior, the right place to implement it is in `build_request`'s
  diff loop. No internal call sites pass the parameter (it's an
  internal helper — `pub fn new_builder_sized` is exposed but unused).
- **Fix commit**: same as #7.
- **Status**: Resolved.

---

## Verification

- `cargo build` clean.
- `cargo test --lib`: 1208 passed (1201 baseline + 7 new tests).
- `cargo test --test consumer`: 36 passed (baseline preserved).
- `cargo test --lib -- --test-threads=1`: 1208 passed (no hangs).
- `cargo xtask format-check`: clean.
- `cargo xtask lint`: clean.

## §27 per-record allocation budget — post-fix audit

Confirmed by code inspection of `CompletedFetch::fetch_records`:

- ✗ `Arc::from(&str)` per record → fixed (allocate once per
  `CompletedFetch` and `Arc::clone` per record).
- ✗ `DefaultRecord::clone()` per record → fixed (read by reference
  through `peek_current_record`).
- ✓ `RecordHeaders::from_slice(record.headers())` per record — §27
  explicitly allows this in Milestone-8 (headers ownership is the
  documented tradeoff).
- ✓ User-supplied `Deserializer<T>` allocations — unavoidable.

The remaining `iter_records()` per-batch `Vec<DefaultRecord>`
materialization is acknowledged in the module docstring as a Phase
7b/c follow-up (Issue #12). It's a per-batch cost, not per-record, so
the per-record allocation budget is met.
