---
name: review-m9-phase3-patterns
description: M11 Phase 3 share fetch data path — corrupt-error/illegal_state escape-hatch divergence; peek-then-reparse interleave equivalence heuristic
metadata:
  type: project
---

M11 Phase 3 (`507fc3f`) = ShareFetch, ShareCompletedFetch, ShareFetchBuffer,
ShareFetchCollector, ShareFetchException, NodeAcknowledgements (blocker),
ShareInFlightBatch.take_in_flight_records. Reviewed substantially clean; ONE real
behavioral divergence + two minor test-fidelity notes.

**THE finding — corrupt-batch error classification vs the collector escape-hatch.**
Java `FetchCollector`/`ShareFetchCollector.collect` wrap the loop in
`catch (KafkaException e) { if (fetch.isEmpty()) throw e; }` — a retriable
`CorruptRecordException` (which IS a KafkaException) is SWALLOWED when records were
already collected, so good records are delivered and the error deferred. The Rust
collectors defer the error and, at loop end, do
`is_illegal_state = matches!(&e, KafkaError::IllegalState(_)); if is_illegal_state || fetch.is_empty() { return Err }`.
The escape hatch is meant to fire ONLY for Java's `IllegalStateException`
("unexpected error code" arm). BUT `ShareCompletedFetch` maps CRC/`ensure_valid`
failures to `KafkaError::illegal_state(...)` (the "...is invalid, cause:" message).
So corrupt-batch errors get mislabeled IllegalState → escape hatch fires → propagate
even when `fetch` is non-empty. Divergence: Java returns Ok(records); Rust returns Err.
Reachable with one partition, batch1 good + batch2 CRC-fail, check_crcs=true (harness
default). Deserialization failures are correctly `KafkaError::serialization` (not
IllegalState) so they're swallowed correctly — mismatch is SPECIFIC to CRC/corrupt.
**Systemic:** identical shape in the merged non-share path — `completed_fetch.rs:786`
maps CRC→illegal_state, `fetch_collector.rs:349` has the same is_illegal_state escape.
So this is likely a pre-existing false-negative in the non-share collector too. Fix:
map CRC to `KafkaError::with_message(Errors::CorruptMessage, ...)` (already used for the
CORRUPT_MESSAGE init path) so it's a KafkaException-equivalent, not IllegalState.

**Audit heuristic that found it:** when a collector has an `is_X` escape hatch keyed on
one KafkaError variant to model "not-a-KafkaException escapes", trace EVERY producer of
that variant. If a Java-KafkaException-subclass (CorruptRecordException,
SerializationException) is mapped to the escaping variant upstream, the hatch
mis-fires. Grep the upstream `fetchRecords`/`ensure_valid`/deserialize error
constructors and classify each against the Java exception hierarchy
(Retriable/ApiException/KafkaException = swallowable; IllegalStateException = escapes).

**Interleave equivalence (non-finding, verified correct).** ShareCompletedFetch uses a
"peek record → store `pending_record_offset` → consume from cursor → re-parse on `==`
match" model instead of Java's borrowed `lastRecord`. Verified same-record identity
(parse_pending_record re-reads from the stored byte offset, `&self`, cursor already
advanced but unused). The 3 inner-loop arms (== parse+advance+break; < skip+break;
> gap+advance-and-continue) match Java's shared-trailing-advance structure. Control
batches skipped whole at header level = Java's per-control-record skip (control batches
are all-control). Don't reject the restructure on textual shape; trace arm-by-arm.

**Minor test-fidelity notes (non-blocking):** (1) `test_fetch_with_other_errors` picks
ONE unhandled error vs Java `@ParameterizedTest` over all `Errors.values()` minus
handled — narrows the guard against a new miscategorized error (recurring "Actor writes
weaker tests" pattern from Phase 1). (2) `test_fetch_normal` drops Java's
isInitialized/isConsumed lifecycle asserts because `cf` is moved into the buffer —
acceptable ownership consequence, substituted has_next_in_line_fetch().

**Clean:** §27 zero-copy (single moved buffer, Arc<str> topic, decompress-once, borrowed
key/value, real alloc-budget test with ≥1/rec floor so non-vacuous); ShareFetch.add
pre-merge timeout read == Java post-merge (merge doesn't touch timeout); ShareFetchBuffer
no guard across await; NodeAcknowledgements/ShareFetchException faithful. All 3 Java test
files' methods translated.
