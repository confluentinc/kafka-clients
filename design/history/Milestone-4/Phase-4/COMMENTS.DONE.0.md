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