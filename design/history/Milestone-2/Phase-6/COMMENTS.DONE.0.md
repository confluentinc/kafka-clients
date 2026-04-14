# Phase 6 — Actor 0 Resolved Comments

## Resolved: Missing version_mismatch branch in handle_produce_response
- **File**: `src/clients/producer/sender.rs`
- **Severity**: Behavior Mismatch
- **Fix**: Added `response.version_mismatch().is_some()` check between the `was_disconnected()` and `has_response()` branches in `handle_produce_response`. When a version mismatch occurs, all batches are now failed with `ErrorCode::UnsupportedVersion`, matching Java Sender.java:594-598 behavior. Also added the `UnsupportedVersion` variant to `ErrorCode` in `src/errors/mod.rs` and mapped `Errors::UnsupportedVersion` in `errors_to_error_code`. A dedicated test `test_version_mismatch_fails_batch` was added to verify this branch.

## Resolved: test_acks_zero_success does not exercise the acks=0 code path
- **File**: `src/clients/producer/sender.rs`
- **Severity**: Missing Requirement (test coverage gap)
- **Fix**: Fixed `MockKafkaClient.poll()` to check `request.expect_response()`. When `expect_response` is false (acks=0), the mock now creates a `ClientResponse` with `response_body: None`, which causes `handle_produce_response` to enter the `else` branch (acks=0 path). The test now also asserts that the completed metadata has `offset() == 0`, verifying the acks=0 logic path is actually exercised.
