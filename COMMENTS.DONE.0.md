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
