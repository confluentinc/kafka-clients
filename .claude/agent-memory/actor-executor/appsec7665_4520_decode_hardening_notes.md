---
name: appsec7665-4520-decode-hardening-notes
description: Loop 78 (APPSEC-7665/NONJAVACLI-4520) fetch-decode hardening — traps found translating ByteBufferLogInputStream, bounding decompression, and testing "a wire length sizes no allocation"
metadata:
  type: project
---

Loop 78 hardened the consumer's fetch decode (plan
`design/current/appsec-7665-4520-decode-hardening.md`, §6 records every
discrepancy). Reusable, non-obvious findings:

1. **Java's `firstBatchSize()` is not `hasNext()`.** `MemoryRecords.firstBatchSize()`
   returns null below `HEADER_SIZE_UP_TO_MAGIC` (17) bytes *before* reading the
   size; `batches().iterator().hasNext()` = `nextBatch() != null` runs
   `nextBatchSize()` directly, which throws for a corrupt size from 12 bytes on.
   **How to apply:** `has_complete_first_batch` must call
   `ByteBufferLogInputStream::next_batch_size`, not `first_batch_size`;
   `fetch_collector::test_initialize_corrupt_batch_size_propagates_corrupt_record_error`
   (12-byte buffer) pins it.

2. **`BatchIterator` has a producer-side consumer:** `ProduceRequest::validate_records`.
   Changing batch-iteration semantics (ending at v0/v1 batches) silently changed
   its Java messages. It now asks the stream Java's `hasNext()` questions
   directly. **How to apply:** before changing an iterator, grep every caller,
   including request validation, not just the consumer path.

3. **A reader-level error is unrecognisable once a record parser wraps it.**
   `DefaultRecord::read_from_stream` turns any io error into a record error, so
   the owned `iter_records` path cannot tell "snappy reader refused a block over
   the limit" from corruption. **How to apply:** enforce a stream limit where
   the caller can observe it (`take(limit + 1)` and `limit() == 0`), not inside
   a reader whose errors get re-wrapped.

4. **`Error::unsupported_version(msg)` is `Error::KafkaError` with code
   `UNSUPPORTED_VERSION`, NOT the `Error::UnsupportedVersion` variant.** Assert
   `err.error() == Errors::UnsupportedVersion`, never `matches!(.., UnsupportedVersion(_))`.

5. **Kafka error `Display` prefixes the class name** (`InvalidRecordError: ...`);
   Java's wrappers append `e.getMessage()`. Use `.message()` when building a
   Java-shaped wrapper message.

6. **Testing "a declared length sizes no allocation":** `test_alloc_tracker`
   now has `AllocTrackingGuard::max_allocation()` (largest single alloc/realloc
   target on this thread). Decoder internals (gzip window, zstd window) pollute
   it, so for a growth-policy assertion drive the bounded reader with a custom
   endless `Read`, not a real codec.

7. **Snappy expansion bound:** densest element is a 3-byte copy of 64 bytes, so
   a valid block decompresses to at most `len * 64 / 3`; a header declaring more
   fails decompression anyway, so rejecting it early is outcome-preserving.

8. **Formatting without touching untracked workspace files:**
   `rustfmt --edition 2024 <changed files>` matches `cargo xtask format-check`
   (style edition from Cargo's 2024) — see [[format-fix-without-cargo-fmt]].
   Clippy's `type_complexity` fires on `[(&str, Box<dyn Fn(..)>); N]` test
   tables; use non-capturing closures coerced to a `fn(..)` type alias.

Round 2 (Critic 78 finding + four notes):

9. **Java's `ByteUtils.readVarint(InputStream)` reads end-of-stream as a
   continuation byte.** `(byte) in.read()` is `-1` at EOF (every codec stream is
   a `ChunkedBytesStream`), so after five reads it throws
   `IllegalArgumentException` and `StreamRecordIterator.readNext` reports
   `Incorrect declared batch size, premature EOF reached` — not an
   `EOFException` / "Failed to decompress record stream". The method's javadoc
   mentions `DataInput`, which is stale (4.3.1 has no `readVarint(DataInput)`);
   a Manager brief repeated that premise. **How to apply:** check a brief's
   claim about Java behaviour against the source before implementing it; in
   Rust, `read_exact`'s `UnexpectedEof` in a stream varint maps to the
   premature-EOF text.

10. **Two checks raising one message: test the earlier one where the later one
    cannot fire.** The control-batch D3 check in `contains_abort_marker` was
    hidden by the install-point D3 check (same text). Only a non-ABORT control
    batch whose producer id is aborted makes the earlier check decisive (else it
    is skipped as aborted). **How to apply:** for any duplicated check, build the
    fixture that bypasses the later one, then mutation-test with `if false && ..`.

11. **The SASL client authenticator's receive was the one production
    `NetworkReceive` outside `KafkaChannel`'s `max_receive_size`** (pre-auth).
    Now capped at 524288 (`BrokerSecurityConfigs.DEFAULT_SASL_SERVER_MAX_RECEIVE_SIZE`,
    Java's broker-side policy). **How to apply:** in a receive-cap review, grep
    `NetworkReceive::with_source|new()` outside tests.

12. Three wrappers in `completed_fetch.rs` (`peek_current_record`,
    `contains_abort_marker`, the headers wrap) had item 5's `Display` prefix;
    Critic named two. **How to apply:** after fixing one instance of a
    formatting defect, grep the file for the rest (`cause: {}` with `e`).
    See [[workflow-autosquash-append-only-logs]] for committing the round.
