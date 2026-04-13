---
name: Metadata wait_on_metadata patterns
description: Patterns for reviewing Java waitOnMetadata() translation — caching, partition-aware retry, error propagation, integration test compatibility
type: feedback
---

Key patterns found reviewing KafkaProducer.waitOnMetadata() translation:

1. **Metadata caching**: Java's waitOnMetadata checks cached Metadata before network call. Rust implementations that always call partitions_for() on every send() add 1 RTT per record. Look for cached partition counts at the KafkaProducer level.

2. **Partition-aware retry**: Java's waitOnMetadata accepts a partition parameter and retries until `partitionsCount >= partition`. Rust implementations that separate partition validation from metadata wait miss partition-expansion scenarios.

3. **Topic-level error propagation**: Java's Metadata class tracks topic-level errors from MetadataResponse (UNKNOWN_TOPIC_OR_PARTITION, TOPIC_AUTHORIZATION_FAILED) and throws them via `maybeThrowExceptionForTopic()`. Rust fetch_partitions that only iterates partitions without checking `topic_metadata.error_code` silently swallows errors, causing confusing TimeoutError instead of proper error codes.

4. **Integration test compatibility**: Adding waitOnMetadata to send() changes the error reporting path. Tests that expect send() to succeed and errors to come back through SendFuture will break -- the error now comes from send() itself. Always check integration tests when moving metadata lookup earlier in the pipeline.

5. **MetadataRequestBuilder::new_with_version trap**: This sets both min and max version to the same value, so `oldest_allowed_version()` == the negotiated version. Not a bug but can appear as one at first glance.

**Why:** These patterns recur when translating Java's Metadata/waitOnMetadata flow because the Java architecture uses a shared Metadata cache + async update via Sender, while Rust implementations tend to inline the metadata fetch directly.

**How to apply:** When reviewing any waitOnMetadata translation, check all 4 patterns above. Pattern 3 (error propagation) combined with pattern 4 (test compatibility) causes the most visible breakage.
