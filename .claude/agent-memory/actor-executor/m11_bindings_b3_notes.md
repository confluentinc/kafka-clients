---
name: m11-bindings-b3-notes
description: M11 admin bindings B3 (electLeaders, alter/listPartitionReassignments, listOffsets) — partition keys as parallel arrays, explicit boolean discriminants for Java Optionals, result shape follows the future shape
metadata:
  type: project
---

Admin C FFI + Python bindings, slice B3, landed on `dev/admin-bindings`. Builds
on [[m11_bindings_b0_b1_notes]] and [[m11_bindings_b2_notes]].

**Why:** B4–B6 repeat this shape. These four points are not visible from the
code once it works, and two of them were judgement calls that deviate from the
brief.

**How to apply:** read alongside the B0/B1 and B2 notes before the next admin
slice.

## Partition keys: parallel arrays, not a handle

The whole crate's FFI takes `TopicPartition` inputs as parallel
`topics[]` / `partitions[]` arrays (`read_topic_partitions` in
`src/ffi/consumer.rs`, `read_records_to_delete` in `admin.rs`) and returns map
keys as `_get_topic(i)` / `_get_partition(i)`.
`kafka_consumer_TopicPartition_t` is **output-only** — `_topic`, `_partition`,
`_destroy`, no constructor — so it cannot be reused as an input without
widening the consumer FFI. Do not introduce an admin-specific one either.

## The result handle's accessors follow Java's *future shape*

D2 says "per-key value and error", but only RPCs with one future per key can
have both. Check the Java `*Result` before choosing:

  - per-key error, no value: `ElectLeadersResult.partitions()` is
    `Map<TopicPartition, Optional<Throwable>>`;
    `AlterPartitionReassignmentsResult.values()` is `KafkaFuture<Void>`.
    → `_get_error(i)`, no `_get_value`. (Same as `CreatePartitionsResult`.)
  - single future over a whole map: `ListPartitionReassignmentsResult
    .reassignments()`. → `_get_value(i)`, **no** `_get_error`; any failure is a
    call failure. (Same as `listTopics`.)
  - one future per key with a payload: `ListOffsetsResult`. → both.

The Python drain shape follows: a per-key-error-only RPC drains to
`{key: error_or_None}`, not to an `(error, value)` pair, so `error_value_pair`
does not apply to it.

## Give every Java `Optional` an explicit boolean discriminant

`all_partitions` (null `Set` / `Optional.empty()` = every partition),
`cancel[i]` (empty `Optional` = revert the reassignment), `is_timestamp[i]`
(`forTimestamp(t)` vs the six no-argument factories). A NULL pointer or a
magic value would conflate "absent" with "present but empty".

The third one is load-bearing, not stylistic:
`KafkaAdminClient.getOffsetFromSpec` (`:5142-5156`) is **not injective** —
`forTimestamp(-2)` and `earliest()` both project to `-2` — and
`MockAdminClient.listOffsets` treats them differently (TimestampSpec throws).
So the C surface passes the six factories as their `ListOffsets` wire
sentinels (-1..-6, real Java constants, nothing invented) plus the flag. A
test asserting the two arms with the *identical* value and opposite flags is
what proves the flag earns its place.

## Panic audit is for trait methods too, not only mock drivers

The B0/B1 rule was "check inherent `MockAdminClient` methods for `panic!`
before exposing them". B3 found the same hazard in a **trait** method:
`find_partition_reassignment` panicked on Java's `RuntimeException` branch,
and `delete_topics` leaves a stale `reassignments` entry (as Java's does), so
create → alter → delete → list reached it from a legal call sequence. Its
rustdoc had asserted the branch was "internal corruption only" — do not trust
such a claim; trace whether any public method can produce the state.

The fix pattern when Java throws synchronously and the Rust signature returns
a `*Result`: complete the single future exceptionally with Java's exact
message. Precedent already in the file (`list_offsets` + `TimestampSpec`).
A test that asserts the exact message proves the branch is *reached*; one that
only checks `is_err()` would not.

## Small mechanics

  - `ListOffsetsResult` has no futures-map accessor, only
    `partition_result(tp)`. Drive the join from the *input* keys — both the
    production client (`future.all()` over the requested key set) and the mock
    seed one future per requested partition. No `pub(crate) futures()`
    accessor was needed, unlike `CreateTopicsResult`.
  - `Optional<Integer>` crosses as `bool fn(handle, int32_t *out)`, not a
    sentinel (precedent `kafka_consumer_OffsetAndMetadata_leader_epoch`).
  - Embedding a nested `KafkaError`'s text in a marshaling error: use
    `e.message()`, not `{e}` — `Display` prefixes the error kind
    (`"IllegalArgumentError: ..."`), which makes the exact-message assertion
    ugly and leaks a Rust type name into a C-visible string.
  - A `#[deny(warnings)]` crate catches a *field* transposition in an option
    builder as `unused_variable`, so the teeth check has to be a **call-site**
    argument swap between two same-typed parameters.
  - Mock drivers stay on `_MockAdminClientMixin` in Python, so they are absent
    from `AdminClient` entirely; the FFI-level "not a mock" rejection is only
    testable from C, which has one handle type for both clients.
