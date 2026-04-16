# Phase 4 Review -- Critic 0

Review of commit `ea1d30d` (Phase 4: Implement Producer API Types).

## Issue 1: ProduceRequestResult.set() immediately unblocks waiters -- breaks two-phase set/done protocol

- **File**: `src/clients/producer/internals/produce_request_result.rs`
- **Severity**: Bug
- **Java Reference**: `ProduceRequestResult.java:69-82`
- **Description**: In Java, `set()` writes volatile fields and `done()` calls `latch.countDown()` to unblock waiters. Waiters calling `await()` are blocked on the latch, so they cannot observe the result until `done()` is explicitly called. This separation is critical because `ProducerBatch.completeFutureAndFireCallbacks()` calls `set()` first, then executes user callbacks in a loop, then calls `done()`. External waiters (e.g. `flush()` calling `await()`) are intentionally blocked until after all callbacks have been invoked.

  In Rust, `set()` calls `self.tx.send_modify(...)` on a `tokio::sync::watch` channel, which immediately updates the value AND notifies all receivers. The `done()` method is then a no-op (just an assert). This means `await_completion()` can return as soon as `set()` is called, before `done()` is called and before callbacks are executed.

  The code comment "Store the result but don't notify yet (done() does that)" is incorrect -- `send_modify` does notify.

- **Expected**: Waiters should not be unblocked until `done()` is called, matching Java's `CountDownLatch` semantics. A possible fix: use a two-value watch `(Option<ProduceResult>, bool)` where the bool is the "done" flag, and have `await_completion` wait for `done == true`. Alternatively, store the result in a separate `Mutex<Option<ProduceResult>>` (not the watch channel) in `set()`, and only send via the watch channel in `done()`.
- **Actual**: `set()` immediately unblocks all waiters, violating the two-phase protocol.

## Issue 2: ProduceRequest.get_error_response returns a response for acks=0 instead of None

- **File**: `src/common/requests/produce_request.rs`
- **Severity**: Behavior Mismatch
- **Java Reference**: `ProduceRequest.java:164-166`
- **Description**: In Java, `getErrorResponse()` returns `null` when `acks == 0` because the producer does not expect any response. In Rust, the method returns `ConcreteResponse::Produce(ProduceResponse::new(response_data))` with empty partition data instead. The `ConcreteRequest::get_error_response()` return type is `ConcreteResponse` (not `Option<ConcreteResponse>`), preventing the null return.

  This behavioral difference will affect how the `Sender` handles error responses for fire-and-forget produce requests (acks=0). The Java `Sender` checks for a `null` response and skips response handling entirely; the Rust equivalent will incorrectly attempt to process an empty response.

- **Expected**: Change `get_error_response` return type to `Option<ConcreteResponse>` across the request framework, and return `None` for Produce requests with acks=0.
- **Actual**: Always returns a `ConcreteResponse`, deviating from Java semantics.

## Issue 3: ProduceRequest.get_error_response omits error message on partition responses

- **File**: `src/common/requests/produce_request.rs`
- **Severity**: Behavior Mismatch
- **Java Reference**: `ProduceRequest.java:175-183`
- **Description**: Java's `getErrorResponse()` creates an `ApiError` from the throwable and sets both the error code AND the error message on each `PartitionProduceResponse` via `.setErrorMessage(apiError.message())` and `.setErrorCode(apiError.error().code())`. The Rust version only sets the error code via `ppr.set_error_code(error.code())`. The error message is never set on the partition response, meaning clients examining the wire response will not see an error description.

- **Expected**: Set the error message on each partition response, e.g. `ppr.set_error_message(Some(error.message().to_string()))`.
- **Actual**: Only the error code is set; the error message field is left at its default (empty/null).

## Issue 4: ProduceRequestBuilder.build_version() skips record validation

- **File**: `src/common/requests/produce_request.rs`
- **Severity**: Missing Requirement
- **Java Reference**: `ProduceRequest.java:68-74`
- **Description**: Java's `Builder.build(short version)` calls `ProduceRequest.validateRecords(version, partitionProduceData.records())` for every partition before constructing the request. This validates: (1) at least one record batch exists per partition, (2) record batch magic is V2, (3) ZStandard compression is not used before version 7, (4) exactly one record batch per partition. The Rust `build_version()` does none of this validation, and the `validate_records` static method is completely absent from the codebase.

- **Expected**: Implement `validate_records` and call it from `build_version()`.
- **Actual**: No validation performed; invalid records would be sent to the broker.

## Issue 5: ProduceResponse.PartitionResponse missing currentLeader field

- **File**: `src/common/requests/produce_response.rs`
- **Severity**: Missing Requirement
- **Java Reference**: `ProduceResponse.java:167, 188-204`
- **Description**: Java's `PartitionResponse` includes a `currentLeader` field of type `ProduceResponseData.LeaderIdAndEpoch`, which is used by the producer to discover the current leader when a `NOT_LEADER_OR_FOLLOWER` error is returned. The Rust `PartitionResponse` struct omits this field entirely. This field is included in `equals()`, `hashCode()`, and `toString()` in Java, and is used in all constructors.

- **Expected**: Add a `current_leader` field (or equivalent) to `PartitionResponse`.
- **Actual**: Field is missing. When the producer receives a produce response with leader info, it will not be available via this type.

## Issue 6: ProduceRequestResult.error() returns Option of String instead of a proper error type

- **File**: `src/clients/producer/internals/produce_request_result.rs`
- **Severity**: Design Flaw
- **Java Reference**: `ProduceRequestResult.java:51,69,175`
- **Description**: In Java, `errorsByIndex` is `Function<Integer, RuntimeException>` -- it returns a `RuntimeException` (which can be any Kafka exception like `RecordTooLargeException`, `InvalidRecordException`, etc.). The `FutureRecordMetadata.valueOrError()` wraps this in an `ExecutionException`. This preserves the exception type so callers can match on it.

  In Rust, the error function signature is `Arc<dyn Fn(i32) -> Option<String>>` -- it returns only the error message string, losing the error type entirely. When `FutureRecordMetadata` is implemented (Phase 5), it will need to propagate a typed error (e.g., `KafkaError`) to the caller, not just a string message. Returning `Option<String>` means the error type information (retriable, fatal, error code) is lost.

- **Expected**: The error function should return `Option<KafkaError>` or a similar typed error, not `Option<String>`.
- **Actual**: Returns `Option<String>`, losing error type information needed by downstream consumers.
