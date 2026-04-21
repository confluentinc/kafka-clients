# Resolved: 60dd558 - Add producer foundation types (Milestone 4, Phase 1)

Resolved in fixup commit cfbfc0b.

## Issue: test_timeout does not test the same scenario as Java's RecordSendTest.testTimeout
- **File**: `src/clients/producer/future_record_metadata.rs`
- **Severity**: Behavior Mismatch
- **Java Reference**: `kafka/clients/src/test/java/org/apache/kafka/clients/producer/RecordSendTest.java:47-63`
- **Description**: Java's `testTimeout` creates one `FutureRecordMetadata`, times out on it via `future.get(5, TimeUnit.MILLISECONDS)`, then completes the underlying `ProduceRequestResult` and verifies that the *same* future now reports `isDone() == true` and `future.get().offset()` returns the correct value. The Rust test cannot do this because `get(self)` consumes the future. After the timeout, the original `(sender, future)` pair is lost and the test creates a completely new `(sender2, future2)` pair, testing an independent scenario. The key Java behavior being verified -- that a timed-out future resolves correctly once the underlying result completes -- is never tested.
- **Expected**: The test should verify the same future can be resolved after a timeout. This is directly related to `get()` taking `self` instead of `&mut self` (see next issue).
- **Actual**: Test creates an entirely new, unrelated future after the timeout and tests that instead.
- **Resolution**: Changed `get()` to `&mut self` and rewrote `test_timeout` to use the same future after timeout, verifying the same sender/future pair resolves correctly.

## Issue: FutureRecordMetadata::get() takes self, deviating from plan and Java semantics
- **File**: `src/clients/producer/future_record_metadata.rs`
- **Severity**: Design Flaw
- **Java Reference**: `kafka/clients/src/main/java/org/apache/kafka/clients/producer/internals/FutureRecordMetadata.java:61-66`
- **Description**: The Phase 1 plan specifies `async fn get(&mut self) -> Result<RecordMetadata, KafkaError>`, but the implementation uses `get(self)` which consumes the future. Java's `Future.get()` does not consume the future -- it can be called multiple times (e.g., first with a timeout, then indefinitely). The consuming signature makes the future single-use, which prevents retry-after-timeout patterns and forces the `test_timeout` test to create a new future. The `is_done()` method also becomes useless after calling `get()` since the struct no longer exists.
- **Expected**: `pub async fn get(&mut self) -> Result<RecordMetadata, KafkaError>` as specified in the plan, allowing the future to be reused after timeout.
- **Actual**: `pub async fn get(self) -> Result<RecordMetadata, KafkaError>` -- consumes the future, making it single-use.
- **Resolution**: Changed to `&mut self`. Internal representation uses `Option<oneshot::Receiver>` with `poll_fn` to poll in-place without consuming the receiver on timeout. Results are cached after first completion via `Option<Result<RecordMetadata, KafkaError>>`. Added `test_get_multiple_calls` to verify caching behavior.

## Issue: FutureRecordMetadataTest.java tests not translated and not explained
- **File**: `src/clients/producer/future_record_metadata.rs`
- **Severity**: Missing Requirement
- **Java Reference**: `kafka/clients/src/test/java/org/apache/kafka/clients/producer/internals/FutureRecordMetadataTest.java:38-64`
- **Description**: Java has `FutureRecordMetadataTest.java` with two tests: `testFutureGetWithSeconds` and `testFutureGetWithMilliSeconds`. These test the `chain()` mechanism for batch splitting. While `chain()` is intentionally deferred from Phase 1 (the plan omits it), the DoD requires that skipped tests be explicitly explained: "Never skip a test that is present in the Java codebase ... explain why they are not relevant and why they can be skipped." No such explanation is provided in the code or the test file.
- **Expected**: A comment in the test file explaining that `FutureRecordMetadataTest.java` tests are intentionally deferred because they exercise the `chain()` method which is out of scope for Phase 1, or a reference to the plan decision.
- **Actual**: No mention of the skipped tests anywhere.
- **Resolution**: Added a comment block in the test module explaining the deferred tests and their dependency on `chain()`.

## Issue: ProducerRecord and Header use panic for recoverable input validation in public API
- **File**: `src/clients/producer/record.rs`
- **Severity**: Design Flaw
- **Java Reference**: `kafka/clients/src/main/java/org/apache/kafka/clients/producer/ProducerRecord.java:69-78`
- **Description**: CLAUDE.md rule 10 states: "Avoid `panic` for public API, use it only if there's no way to recover from a particular error" and "Return a `Result` when Java code throws an exception even if unchecked but recoverable." Java's `ProducerRecord` constructor throws `IllegalArgumentException` for null topic, negative partition, and negative timestamp -- all recoverable errors. The Rust translation uses `assert!` (panic) for these validations in 7 places across `new()`, `with_all_fields()`, `with_partition()`, `with_timestamp()`, and `Header::new()`.
- **Expected**: Constructors/builders should return `Result<Self, KafkaError>` for invalid inputs, matching CLAUDE.md's error handling rules.
- **Actual**: Uses `assert!` which panics on invalid input in public API.
- **Resolution**: All constructors/builders now return `Result<Self, KafkaError>`. Added `KafkaError::illegal_argument()` convenience constructor using `Errors::InvalidConfig` as the error code. All tests updated to use `is_err()`/`unwrap()` instead of `should_panic`/`catch_unwind`. `RecordMetadata` now derives `Clone` to support result caching.
