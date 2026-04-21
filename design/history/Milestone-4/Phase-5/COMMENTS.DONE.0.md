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

## [RESOLVED] Issue: `kafka_producer_send` null-parameter guard does not null-initialize `*out_future`

- **File**: `src/ffi/producer.rs`
- **Severity**: Bug
- **Lines**: 316-318
- **Description**: The fixup commit (3166af8) correctly added `*out_future = std::ptr::null_mut()` on the `build_record` error path and the `producer_send` error path, and updated the documentation to state "On error, `*out_future` is set to null." However, the early null-parameter guard at line 316 (`if producer.is_null() || topic.is_null() || out_future.is_null()`) returns an error code without setting `*out_future` to null when `out_future` is non-null but `producer` or `topic` is null. This violates the documented contract. The same pattern applied to `kafka_producer_send_batch` at line 382 for `out_futures`.
- **Expected**: When the guard detects `producer.is_null() || topic.is_null()` but `out_future` is non-null, set `*out_future = std::ptr::null_mut()` before returning the error code. Check `out_future.is_null()` first (returning immediately since it cannot be written to), then handle remaining null-parameter cases with proper null-initialization.
- **Actual**: `*out_future` was left unmodified when the null-parameter guard fired with a non-null `out_future`. Tests passed by coincidence of pre-initialization to `null_mut()`.
- **Resolution**: Restructured the guard in both `kafka_producer_send` and `kafka_producer_send_batch` to check the output pointer first (can't write to it, just return error), then null-initialize the output before returning for remaining null cases. Updated `test_send_null_producer`, `test_send_null_topic`, and `test_send_batch_null_params` to use non-null sentinel values instead of `null_mut()` pre-initialization, proving the functions actively null the output pointers. Fixed in commit f02fd97.
