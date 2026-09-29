# Critic 69 review of `1a0edabd` — resolved

## Issue: removeAll `all()` cause is unreachable from C/Python, so the member's error is lost — FIXED (documentation)
- **File**: `src/ffi/admin.rs` (`kafka_admin_RemoveMembersFromConsumerGroupResult_all` rustdoc), `src/ffi/common.rs`
- **Severity**: Behavior Mismatch (Low)
- **Java Reference**: `kafka/clients/src/main/java/org/apache/kafka/clients/admin/RemoveMembersFromConsumerGroupResult.java` (`all()`, `new KafkaException("Encounter exception when trying to remove: " + entry.getKey(), exception)`); `KafkaAdminClientTest.java:7700-7702`
- **Description**: The Rust core now matches Java: `all()` returns `Error::KafkaError` (code
  `UNKNOWN_SERVER_ERROR`) with the member's error as its `source()`. Java callers recover
  that member's error with `getCause()`, which the KafkaAdminClientTest assertion does. The
  C API has no accessor for an error's cause, and `kafka_common_Error_message` returns only
  the outer message. So for C, the gRPC server and Python, this change replaces an
  observable member error code (for example `UNKNOWN_MEMBER_ID`) with `-1` and nothing more.
  The information is still inside the handle but cannot be read.

  This bears on the Actor's claim that the Python-visible `-1` is "the Java-faithful
  outcome". It is faithful in the error's *kind*, but not in what the caller can observe:
  Java's result still exposes the cause, and ours no longer does. The new rustdoc also tells
  a C reader "the member's own error is its cause" without saying that C cannot read it.
- **Expected**: At minimum, the `_all` rustdoc (which becomes the C header) should say that
  the cause is not exposed across the C boundary. The Java-complete fix is a
  `kafka_common_Error_cause(const Error_t*)` accessor that returns a borrowed error or NULL.
  That is a new FFI surface, so it should probably be its own approved follow-up rather
  than being folded into this commit.
- **Actual**: The doc promises a cause that C cannot see. On a real broker, a removeAll
  partial failure reaches Python and gRPC as code `-1`, and the per-member error cannot be
  recovered.

- **Resolution**: Minimum fix per the Manager: documentation only, no new symbol.
  The rustdoc (C header) of `kafka_admin_RemoveMembersFromConsumerGroupResult_all`
  now states that the member's own error is the cause, that this C API cannot read
  an error's cause, and that from C, Python and gRPC a removeAll partial failure
  therefore surfaces as a bare Kafka error with code -1 (`UNKNOWN_SERVER_ERROR`)
  and Java's message. The removeAll sentence of
  `kafka_admin_AdminClient_remove_members_from_consumer_group_callback_t` says the
  same and points at `_all`. A `kafka_common_Error_cause` accessor is left as a
  possible separately approved follow-up.

## Issue: stale test-section comment still claims per-key delivery for the three RPCs — FIXED
- **File**: `src/ffi/admin.rs:28003-28013` (test module, "Phase E — end-to-end real-error-per-key proof")
- **Severity**: Design Flaw (Low, documentation)
- **Java Reference**: n/a
- **Description**: The section header lists `alterConsumerGroupOffsets`,
  `deleteConsumerGroupOffsets` and `removeMembersFromConsumerGroup` among the RPCs whose
  tests "assert the REAL typed error … surfaces per key, not the generic
  `admin_async_per_key_op` safety-net fallback". After this commit, those three tests
  (`*_async_fires_one_callback_with_the_whole_request_outcome`) assert the opposite: one
  callback carrying the whole-request error. None of the three uses
  `admin_async_per_key_op` any more.
- **Expected**: Narrow the header to the four map-shaped RPCs, and note that the three
  single-future RPCs assert one whole-request callback.
- **Actual**: The comment describes the removed per-key model for these three RPCs.

- **Resolution**: The "Phase E" test-section header now splits the family: the four
  map-shaped RPCs deliver per key through `admin_async_per_key_op` and assert the real
  typed error per key; the three single-future RPCs do not use that helper and assert
  one callback carrying the whole-request error.

