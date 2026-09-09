# COMMENTS.DONE.70 — Critic 70 producer send-path items (Actor 67, branch p7-producer-fixes)

Resolved the producer send-path blockers + two notes assigned to me. The other
Critic-70 findings (F1 offset payload accessors, F2 consumer async rustdoc, N1
partitioner=, N3 InvalidOffset wording, N5 mock empty-cluster partition) are NOT
mine and stay open for their owners.

## B1 — send path silently DROPPED ProducerRecord headers — RESOLVED
End-to-end: kafka_producer_ProducerRecord_t gained `headers` + `header_count` (new
kafka_producer_ProducerRecordHeader_t struct); build_record_headers threads them
into send_batch / send_batch_async (+ the mock branches); the C-ext
ProducerRecord_init accepts a `headers` kwarg; _native_record passes
record.headers(). Header values are copied once at the C→Rust boundary (the core's
RecordHeaders owns its values; key/value record bytes stay zero-copy) — C45.
Tests: Rust test_send_batch_carries_headers_and_tombstone, C
test_send_batch_headers_and_tombstone, Python TestSendPathHeadersAndTombstone.

## B2 — tombstone (value=None) sent as b"" — RESOLVED
The native ctor now accepts a None value (value_len == -1, as for a null key);
_native_record drops the b"" substitution. Tests assert the null value reaches the
FFI struct (sync + async) and the mock preserves it.

## N2 — RecordMetadata.UNKNOWN_PARTITION absent — RESOLVED
Added `UNKNOWN_PARTITION = -1` (Java public constant) as a class attribute.

## N4 — MockProducer skips base __init__ — RESOLVED
MockProducer / AsyncMockProducer ctors call Producer.__init__ / AsyncProducer.__init__
(initialise base _ProducerState; the abstract-base guard does not fire on the mock).

gRPC note: the producer proto has no headers field, so the gRPC producer arm cannot
forward headers (separate proto change); the stale grpc_translate.py comment was
corrected. C45 records the header-value-copy boundary + the proto gap.
