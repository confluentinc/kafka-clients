## [RESOLVED] Issue: Error injection uses take semantics instead of Java's persistent semantics

- **File**: `src/clients/producer/mock_producer.rs`
- **Severity**: Behavior Mismatch
- **Java Reference**: `kafka/clients/src/main/java/org/apache/kafka/clients/producer/MockProducer.java:296-298` (sendException), `350-352` (flushException), `359-361` (partitionsForException), `417-419` (closeException)
- **Description**: All four error injection fields (`send_error`, `flush_error`, `partitions_for_error`, `close_error`) used `Option::take()` in their respective methods (`send()`, `flush()`, `partitions_for()`, `close()`). This consumed the error on first use -- only the first call after setting an error would fail, and subsequent calls would succeed. In Java, these are public fields that persist until manually set to `null`.
- **Expected**: Error injection should persist until explicitly cleared, matching Java behavior.
- **Actual**: Error was consumed after first use via `Option::take()`.
- **Resolution**: Changed from `take()` to `as_ref()` + `.clone()` in all 4 error-checking paths. Updated setter doc comments to describe persistent semantics. Fixed tests to assert persistent errors (second call also fails) and verify clearing with `set_*_error(None)`. Fixed in commit 600755f.