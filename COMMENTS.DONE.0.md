# Resolved Comments for Actor 0

## [RESOLVED] Issue: `close` and `close_with_timeout` should return `Result`

- **File**: `src/clients/producer/mod.rs`
- **Severity**: Behavior Mismatch
- **Java Reference**: `kafka/clients/src/main/java/org/apache/kafka/clients/producer/KafkaProducer.java:1397-1464`
- **Description**: The Java `KafkaProducer.close(Duration)` implementation can throw both `InterruptException` and `KafkaException("Failed to close kafka producer", exception)` (lines 1458-1461). These are unchecked but recoverable exceptions. Per CLAUDE.md rule 10.2: "Return a Result when Java code throws an exception even if unchecked but recoverable." The Rust trait's `close(&mut self)` and `close_with_timeout(&mut self, timeout: Duration)` both return `()`, silently swallowing these errors.
- **Expected**: Both close methods should return `Result<(), KafkaError>` to match the Java behavior and comply with CLAUDE.md rule 10.2.
- **Actual**: Both close methods return `()`, making it impossible for callers to detect close failures.
- **Resolution**: Changed both `close` and `close_with_timeout` to return `Result<(), KafkaError>`. Added `# Errors` rustdoc sections. Fixed in commit 81e0616.

## [RESOLVED] Issue: `&mut self` for close is incompatible with `Arc<dyn Producer>` sharing

- **File**: `src/clients/producer/mod.rs`
- **Severity**: Design Flaw
- **Java Reference**: `kafka/clients/src/main/java/org/apache/kafka/clients/producer/Producer.java:40`
- **Description**: The trait doc comment (line 38) states that `&self` methods enable `Arc<dyn Producer>` sharing. However, `close(&mut self)` cannot be called through an `Arc<dyn Producer>` since `Arc` does not provide `&mut` access unless `strong_count == 1` (via `Arc::get_mut` or `Arc::try_unwrap`). This makes the trait unusable as a trait object behind `Arc` when close is needed. In Java, `close()` is called on the same shared `Producer` reference — it uses internal synchronization, not exclusive ownership. The `&mut self` approach is fundamentally at odds with the stated design goal of `Arc<dyn Producer>` sharing.
- **Expected**: Either (a) `close` should take `&self` and use interior mutability (a `closed` flag behind a `Mutex` or `AtomicBool`) to prevent concurrent use, matching how Java does it internally, or (b) the doc comment about `Arc<dyn Producer>` should be removed if the intended usage pattern is `Box<dyn Producer>` or owned access only.
- **Actual**: `close(&mut self)` and `close_with_timeout(&mut self, timeout: Duration)` require exclusive access, contradicting the `Arc<dyn Producer>` design stated in the doc.
- **Resolution**: Changed both methods from `&mut self` to `&self`. Updated the Design Notes doc comment to explain that implementors use interior mutability (AtomicBool/Mutex) to manage the closed flag. Fixed in commit 81e0616.

## [RESOLVED] Issue: Error injection uses take semantics instead of Java's persistent semantics

- **File**: `src/clients/producer/mock_producer.rs`
- **Severity**: Behavior Mismatch
- **Java Reference**: `kafka/clients/src/main/java/org/apache/kafka/clients/producer/MockProducer.java:296-298` (sendException), `350-352` (flushException), `359-361` (partitionsForException), `417-419` (closeException)
- **Description**: All four error injection fields (`send_error`, `flush_error`, `partitions_for_error`, `close_error`) used `Option::take()` in their respective methods (`send()`, `flush()`, `partitions_for()`, `close()`). This consumed the error on first use -- only the first call after setting an error would fail, and subsequent calls would succeed. In Java, these are public fields that persist until manually set to `null`.
- **Expected**: Error injection should persist until explicitly cleared, matching Java behavior.
- **Actual**: Error was consumed after first use via `Option::take()`.
- **Resolution**: Changed from `take()` to `as_ref()` + `.clone()` in all 4 error-checking paths. Updated setter doc comments to describe persistent semantics. Fixed tests to assert persistent errors (second call also fails) and verify clearing with `set_*_error(None)`. Fixed in commit 600755f.

## [RESOLVED] Issue: External test_auto_complete_mock omits clear() verification present in Java

- **File**: `tests/clients/producer/mock_producer_test.rs`
- **Severity**: Missing Requirement
- **Java Reference**: `kafka/clients/src/test/java/org/apache/kafka/clients/producer/MockProducerTest.java:81-82`
- **Description**: The Java `testAutoCompleteMock` test (line 73-83) sends one record, then calls `producer.clear()` and asserts `producer.history().size() == 0`. The Rust external test sent two records and verified history had both, but never called `clear()` and never verified that clearing the history works.
- **Expected**: The external `test_auto_complete_mock` should mirror the Java test: send one record, check history contains it, call `producer.clear()`, assert history is empty.
- **Actual**: `clear()` was never called in the external test.
- **Resolution**: Restructured the test to match the Java flow: send one record, verify history contains it, call `clear()`, assert history is empty. Kept an additional second-record send to verify history rebuilds after clear. Fixed in commit 31e3b66.

## [RESOLVED] Issue: `clear()` incorrectly resets offset counters (divergence from Java)

- **File**: `src/clients/producer/mock_producer.rs`
- **Severity**: Behavior Mismatch
- **Java Reference**: `kafka/clients/src/main/java/org/apache/kafka/clients/producer/MockProducer.java:490-497` (clear method) and line 62 (offsets field)
- **Description**: The Rust `clear()` method called `inner.offsets.clear()`, which resets the per-topic-partition offset counter to zero. However, Java's `clear()` does **not** clear the `offsets` map -- it only clears `sent`, `uncommittedSends`, `completions`, `consumerGroupOffsets`, `uncommittedConsumerGroupOffsets`, and sets `sentOffsets = false`. The `offsets` map (which tracks `nextOffset` per TopicPartition) is intentionally preserved across `clear()` calls in Java. This means in Java, if you send a record (offset 0), call `clear()`, then send another record to the same partition, the second record gets offset 1. In Rust, the second record would incorrectly get offset 0 again.
- **Expected**: `clear()` should NOT call `inner.offsets.clear()`. Remove the `inner.offsets.clear();` line from the method to match Java semantics.
- **Actual**: `inner.offsets.clear()` resets all offset counters, causing post-clear sends to restart at offset 0 instead of continuing the sequence.
- **Resolution**: Removed the `inner.offsets.clear()` line from `clear()`. Updated the docstring to note that offsets are intentionally preserved. Also renamed and fixed the internal `test_clear_resets_offsets` test to `test_clear_preserves_offsets`, asserting offset continues from 2 (not 0) after clear. Fixed in commit 41ed6aa.

## [RESOLVED] Issue: Dropped offset assertion for second record hides the `clear()` bug above

- **File**: `tests/clients/producer/mock_producer_test.rs`
- **Severity**: Missing Requirement
- **Java Reference**: N/A (the second record send is extra Rust-only test code)
- **Description**: The original test (commit 96db03e) asserted `assert_eq!(1, metadata2.unwrap().offset(), "Offset should be 1")` for the second record. The fixup (commit 31e3b66) moved the second send after `clear()` but dropped the offset assertion entirely. If the offset assertion had been kept, it would expose the `clear()` bug above. The code was dead weight: it sent a second record, ignored the offset, and only checked history length.
- **Expected**: The second-record section should either assert `offset == 1` (after fixing `clear()`) or be removed entirely to match the Java test exactly.
- **Actual**: The second record's offset was never checked, hiding the behavioral divergence in `clear()`.
- **Resolution**: Removed the extra post-clear second-record code entirely, matching Java's `testAutoCompleteMock` exactly: send one record, verify metadata (offset + topic), verify history, clear, verify history is empty. Fixed in commit 41ed6aa.

## [RESOLVED] Issue: `kafka_producer_send` does not set `*out_future` to null on error

- **File**: `src/ffi/producer.rs`
- **Severity**: Bug
- **Lines**: 319-334
- **Description**: On both error paths in `kafka_producer_send` -- the `build_record` error return (line 322) and the `producer_send` error return (line 333) -- the function returns an error code but never writes `std::ptr::null_mut()` to `*out_future`. A C caller who does not pre-initialize `*out_future` to NULL before the call will have a garbage/stale value in `*out_future` when the function returns an error. The test `test_send_after_close_returns_error` (line 1577) asserts `future.is_null()` which only passes because the test variable was initialized to `null_mut()` before the call -- it does not actually verify the function's behavior.
- **Expected**: On all error paths, explicitly set `unsafe { *out_future = std::ptr::null_mut(); }` before returning the error code, matching the pattern used in `kafka_future_get` (lines 483-485) which correctly sets `*out_metadata = std::ptr::null_mut()` on error.
- **Actual**: `*out_future` is left unmodified on error, making the test pass only by coincidence of pre-initialization.
- **Resolution**: Added explicit `*out_future = std::ptr::null_mut()` on both error paths in `kafka_producer_send`. Updated `test_send_after_close_returns_error` to use a non-null sentinel value (`0xDEAD_BEEF`) to verify the function actively sets the pointer to null rather than relying on pre-initialization. Updated docstring to document the null-on-error behavior. Fixed in commit 3166af8.

## [RESOLVED] Issue: `kafka_producer_send_batch` does not null-initialize `out_futures` on partial failure

- **File**: `src/ffi/producer.rs`
- **Severity**: Design Flaw
- **Lines**: 376-401
- **Description**: When `kafka_producer_send_batch` fails mid-way through the batch (e.g., at record `i=2` of 5), the slots `out_futures[2]` through `out_futures[4]` are left with whatever values the C caller had there (likely uninitialized). Although the documentation says "futures for successfully sent records prior to the error are still valid," a C caller performing cleanup has no way to know how many records succeeded unless they also track the return-code/index separately.
- **Expected**: At minimum, initialize `out_futures[i]` through `out_futures[count-1]` to null on any failure.
- **Actual**: Remaining slots are left uninitialized on partial failure; the only way a C caller can avoid accessing garbage pointers is to pre-initialize the entire array to NULL before the call.
- **Resolution**: Added null-fill loops for `out_futures[i..count]` on all three failure paths in `kafka_producer_send_batch` (null topic, build_record error, producer_send error). Added `test_send_batch_partial_failure_nulls_remaining_slots` which tests both mid-batch failure (at index 1 of 3) and first-record failure (at index 0 of 2), using non-null sentinel values to verify active null-filling. Updated docstring to document the null-fill behavior. Fixed in commit 3166af8.
