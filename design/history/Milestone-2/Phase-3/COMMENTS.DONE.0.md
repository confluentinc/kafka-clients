# Resolved Critic Issues -- Phase 3 (Commits 114f0a3, 1520d0d)

## RESOLVED: Memory permit leak in RecordAccumulator -- batch header overhead counted per record

- **File**: `src/clients/producer/accumulator.rs` and `src/clients/producer/batch.rs`
- **Fix**: Removed batch header overhead (61 bytes) from `estimate_record_size()`, which now returns only the record-level size. Added `permits_acquired` field to `ProducerBatch` to track total semaphore permits acquired per batch. The sender now releases `permits_acquired()` instead of `written_bytes()`, ensuring acquired == released. Added `batch_header_overhead()` static method for callers that need the batch overhead separately.
- **Tests**: Added `test_memory_permits_balanced_after_multi_record_batch` to verify no permits leak for a 10-record batch. Updated `test_estimate_record_size_excludes_batch_overhead` to verify the estimate no longer includes the 61-byte batch header.

## RESOLVED: Append errors silently swallowed as "batch full"

- **File**: `src/clients/producer/batch.rs` and `src/clients/producer/accumulator.rs`
- **Fix**: Changed `ProducerBatch::try_append()` return type from `Option<SendFuture>` to `Result<Option<SendFuture>, KafkaError>`. `Ok(None)` = batch full/closed (capacity exhaustion), `Err(e)` = input validation error (e.g. invalid negative timestamp). Updated all callers in accumulator.rs to propagate errors and release memory permits on failure. Updated sender.rs to use `permits_acquired()` for memory release.
- **Tests**: Added `test_append_with_invalid_timestamp_returns_error` (batch.rs) and `test_append_invalid_timestamp_returns_error` (accumulator.rs) to verify error propagation. Updated all existing tests for the new `Result<Option<...>>` return type.
