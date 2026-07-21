# Phase 3 review — RESOLVED (fixup on 507fc3f)

Resolution summary:
- Finding 1 (corrupt-batch propagation): CRC / batch-validation failures in
  `share_completed_fetch.rs` now map to `KafkaError::with_message(Errors::CorruptMessage, ..)`
  via the new `corrupt_record_error` helper (not `illegal_state`), so the collector's
  `is_illegal_state` escape no longer treats them as always-propagating. Added regression
  test `test_corrupt_batch_after_good_records_is_swallowed` (partition A valid + partition B
  CRC-corrupt -> Ok(fetch) with A's records, B error deferred). Non-share parallel left
  untouched (out of M11 scope) with an in-code note on the helper.
- Finding 2: `test_fetch_with_other_errors` now sweeps every Kafka error code (0..=130 via
  `Errors::for_code`) minus the handled set, asserting the catch-all IllegalState arm.
- Finding 3: added a comment in `test_fetch_normal` explaining why the isInitialized/
  isConsumed lifecycle assertions are dropped (ownership: cf moved into next-in-line slot).

Original findings below.

# Phase 3 review (commit 507fc3f — share consumer fetch data path)

Reviewed: `share_completed_fetch.rs`, `share_fetch.rs`, `share_fetch_buffer.rs`,
`share_fetch_collector.rs`, `share_fetch_exception.rs`, `node_acknowledgements.rs`,
and the `share_in_flight_batch.rs` `take_in_flight_records` addition, against the
Java sources and all three Java test files.

## Issue: CRC/corrupt-batch errors map to `illegal_state`, so the collector's `IllegalState` escape-hatch propagates them instead of swallowing (Java swallows `CorruptRecordException` when records were already collected)
- **File**: `src/consumer/internals/share_completed_fetch.rs:762-769` (also `:559-566`, `:615-629`) → surfaced via `src/consumer/internals/share_fetch_collector.rs:225-233`
- **Severity**: Behavior Mismatch
- **Java Reference**: `ShareCompletedFetch.java:392-401` (`maybeEnsureValid` throws `CorruptRecordException`); `ShareFetchCollector.java:114` (`throw new ShareFetchException(fetch, cause)`) and `:121-125` (`catch (KafkaException e) { if (fetch.isEmpty()) throw e; }`)
- **Description**: In `ShareCompletedFetch`, a failed CRC / batch validation is mapped to `KafkaError::illegal_state(...)` (the `CollectLoopError::Corrupt` route, set as the `ShareInFlightBatchException` cause at `fetch_records` lines 364-374). In Java the equivalent is `CorruptRecordException`, which is a `KafkaException` (via `RetriableException`/`ApiException`). The collector's end-of-loop error handling (`share_fetch_collector.rs:229`) keys the always-propagate decision on `matches!(&e, KafkaError::IllegalState(_))`, whose stated purpose (comment at `:227-229`) is to escape **only** Java's `IllegalStateException` (the "unexpected error code" path). Because corrupt errors are mislabeled as `IllegalState`, they hit that escape hatch and propagate as `Err(ShareFetchException)` even when records were already collected — whereas Java's `catch (KafkaException e) { if (fetch.isEmpty()) throw e; }` swallows a `CorruptRecordException` when `fetch` is non-empty and returns the good records with no error.
  - Note: deserialization failures are correctly mapped to `KafkaError::serialization` (not `IllegalState`), so they are swallowed as Java's `SerializationException` would be. The mismatch is specific to the CRC/corrupt-batch path.
- **Expected**: A CRC/corrupt-batch failure should be a Kafka-exception-equivalent (e.g. `KafkaError::with_message(Errors::CorruptMessage, ...)`, as already used for the `CORRUPT_MESSAGE` init path at `share_fetch_collector.rs:320-329`) so the collector's `is_illegal_state` check does NOT treat it as always-propagating; when the collector `fetch` is non-empty the corrupt error is swallowed and the good records are returned (Java behavior).
- **Actual**: The corrupt error escapes as `Err`, so the already-collected records are surfaced only as the `ShareFetch` carried inside the error (not returned normally), and the user sees the corrupt error on this poll instead of getting records now and the error deferred.
- **Concrete failure scenario**: A single partition whose fetch payload has two batches: batch 1 valid (its records deserialize and enter `fetch`), batch 2 fails `ensure_valid` (CRC) with `check_crcs = true` (the test harness default). In `collect`: iteration 1 fetches batch-1 records → `fetch` non-empty; iteration 2's `fetch_records` fails on batch 2 with an empty in-flight batch → `reject_record_batch` + `set_exception(illegal_state)`; the collector sets `deferred_error` and breaks; the end check sees `is_illegal_state == true` → returns `Err`. Java returns `Ok(fetch)` with batch-1's records and no exception.
- **Why it matters**: This is the exact contract of Java's outer catch — deliver already-collected records and defer/drop the retriable corrupt error. Breaking it means a corrupt batch anywhere after the first good batch throws to the user (dropping/deferring good records) rather than delivering them.
- **Precedent / scope note (not a false positive, but likely systemic)**: The non-share path has the identical shape — `completed_fetch.rs:786` maps CRC to `illegal_state` and `fetch_collector.rs:349` uses the same `is_illegal_state` escape. So the same divergence may exist in the already-merged non-share collector. The fix should be evaluated for both; flagging on the share code because that is what is under review. If this was a deliberate accepted convention for the non-share path, please record the rationale so the deviation is documented (DoD §1).

## Minor note (test fidelity, non-blocking): `test_fetch_with_other_errors` narrows Java's parameterized sweep to a single error
- **File**: `src/consumer/internals/share_fetch_collector.rs:626-670`
- **Severity**: Missing Requirement (minor)
- **Java Reference**: `ShareFetchCollectorTest.java:286-297` (`@ParameterizedTest` over every `Errors.values()` not in the handled set)
- **Description**: Java exercises the catch-all `IllegalStateException` arm for *all* unhandled error codes, guarding against a newly-added error being silently miscategorized. The Rust test picks one representative (`InvalidRecordState`). The `other =>` arm is trivially correct today, so this is low-risk, but it is a reduction in the defensive coverage DoD §3 asks to preserve. Consider looping over all `Errors` variants minus the handled set (as Java does) rather than a single pick.

## Minor note (test fidelity, non-blocking): `test_fetch_normal` drops the `isConsumed()` lifecycle assertions
- **File**: `src/consumer/internals/share_fetch_collector.rs:457-482`
- **Severity**: Missing Requirement (minor)
- **Java Reference**: `ShareFetchCollectorTest.java:114,117,134` (`assertTrue(isInitialized())`, `assertFalse(isConsumed())`, then `assertTrue(isConsumed())` after the second collect)
- **Description**: Java asserts the `ShareCompletedFetch` initialize→not-consumed→consumed lifecycle across the two collects. The Rust test cannot observe this directly because the `cf` is moved into the buffer's next-in-line slot; it substitutes `has_next_in_line_fetch()`. This is an acceptable structural consequence of the ownership model (documented pattern), noted only so the reduced assertion is on record.

## Confirmed correct (spot-checks that passed)
- **Acquired-record interleave** (`collect_records` / `next_fetched_record`): the peek-then-consume + `pending_record_offset` re-parse model matches Java's `lastRecord` + `records.next()` loop. The three inner-loop arms (== parse/advance/break; < skip/break; > gap/advance) and the trailing "remaining acquired become gaps" match `ShareCompletedFetch.java:205-240`. All 10 Java `ShareCompletedFetchTest` methods are translated, including the overlapping-dedup first-occurrence and odd/gap cases. Control-batch whole-batch skip is equivalent to Java's per-control-record skip (control batches contain only control records); the zero-record control-batch test deviation is sound and documented.
- **§27 zero-copy**: single owned buffer moved (not copied) into the cursor; `topic_arc: Arc<str>` cloned per record; decompress-once into `RecordSource::Owned`; key/value borrowed via `read_ref_from_buffer`. The per-record allocation-budget test is a genuine bound (≤5/record + 120 overhead; also asserts ≥1/record so it can't pass vacuously).
- **`ShareFetch::add`** reads `get_acquisition_lock_timeout_ms()` before the consuming `merge` — verified `merge` does not change the timeout, so the pre-merge read equals Java's post-merge read (carryover note #1 correct).
- **`NodeAcknowledgements`** exists in Java (used by `ShareFetch.takeAcknowledgedRecords`); the Rust type is a faithful, minimal translation (DoD §7 satisfied).
- **`ShareFetchException`** faithfully unifies Java's two exit paths (bare `KafkaException` from `initialize`, `ShareFetchException` from the records branch); callers get `cause()` + the carried `ShareFetch` via `into_parts()`.
- **`ShareFetchBuffer`**: no `MutexGuard` held across `.await` in `await_not_empty` (CLAUDE.md §9.6); `close`/`wakeup`/`buffered_partitions`/`set_next_in_line_fetch` (no-drain) match Java. All 4 Java `ShareFetchBufferTest` methods translated. (Trivial: the already-closed warn text says "share fetch buffer" vs Java's "fetch buffer" — cosmetic, not reported as a defect.)

## Verdict
Phase 3 is **substantially clean**. One real behavioral divergence (corrupt-batch
error propagation vs Java's swallow-when-records-collected) that mirrors the
non-share precedent and should be resolved or explicitly documented; two minor
test-fidelity notes. No zero-copy violations, no missing methods, no missing test
translations.
