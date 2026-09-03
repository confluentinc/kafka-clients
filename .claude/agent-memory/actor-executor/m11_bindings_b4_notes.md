---
name: m11-bindings-b4-notes
description: M11 admin bindings B4 (the nine group RPCs) — the valid()/errors() result shape that is neither keyed nor parallel, reusing consumer-namespace types, and the Py_BuildValue arity blind spot on mock-unsupported RPCs
metadata:
  type: project
---

Admin C FFI + Python bindings, slice B4 (`listGroups`, `listConsumerGroups`,
`describeConsumerGroups`, `describeClassicGroups`, `listConsumerGroupOffsets`,
`alterConsumerGroupOffsets`, `deleteConsumerGroupOffsets`,
`deleteConsumerGroups`, `removeMembersFromConsumerGroup`), landed on
`dev/admin-bindings`. Builds on [[m11_bindings_b0_b1_notes]],
[[m11_bindings_b2_notes]] and [[m11_bindings_b3_notes]].

**Why:** B5 and B6 hit the same three traps, and two of them are silent.

**How to apply:** read alongside the B3 note before the next admin slice.

## A third result shape: `valid()` + `errors()`, neither keyed nor parallel

B3 established that accessors follow the Java future's shape. B4 adds the case
where there is **no per-key future at all**: `ListGroupsResult` /
`ListConsumerGroupsResult` hold one source future that `valid()` and `errors()`
split into a listing list and an **unkeyed** `Collection<Throwable>` of a
generally different length.

Do not name these `_count` / `_get_error(i)` — that spelling invites indexing
the errors by the listing count, which reads past the end on any partial
success. Use `_valid_count` / `_get_valid(i)` and `_error_count` /
`_get_error(i)`, and say in the rustdoc that the two are independent. Python
returns a `(valid, errors)` **tuple**, not a dict: merging them would have to
invent a key.

Check for this shape by looking for a `*Result` constructor that takes one
`KafkaFuture<Collection<Object>>` and fans it out in the constructor body.

## A single-source-future RPC with zero requested keys loses its error

`alterConsumerGroupOffsets`, `deleteConsumerGroupOffsets` and
`removeMembersFromConsumerGroup` back every per-key future with one source
future. Driving the join from the requested key set is right — but when that
set is empty the flattened result has nowhere to put a failure and it silently
disappears. Await the single `all()` future instead in that case and let the
error become the call's error, which is what Java's `all()` reports there too.
One helper (`empty_outcomes`) plus a `if keys.is_empty()` guard per submit.

`removeAll` mode is the same case by construction: Java refuses `memberResult`
outright there, so the result carries no keys at all.

## Namespace: reuse the consumer type, do not mint an admin one

Round 7 ruled that `kafka_consumer_TopicPartition_t` is mis-namespaced and must
not be propagated. The mirror-image rule bit here: `OffsetAndMetadata` really
**is** `org.apache.kafka.clients.consumer`, so `kafka_admin_OffsetAndMetadata_t`
would be the wrong name. Resolution:

  - **C**: no such handle. Flatten the three fields into indexed accessors on
    `kafka_admin_OffsetAndMetadataMap_t` — the B2 `LogDirDescription.ReplicaInfo`
    precedent. The map handle itself is admin-namespaced legitimately: it is the
    admin result's value, not a Java class.
  - **Python**: import `OffsetAndMetadata` from `consumer.py`, exactly as
    `admin.py` already imports `Node`. Do not redefine it.

Java's map value is **nullable** (a requested partition the group never
committed for is present with a null value), so it needs a `has_offset(i)`
discriminant or 0 and "absent" collapse.

## The Py_BuildValue arity blind spot

`consumer_group_description_to_py` shipped with 10 format units for 11
arguments. **No test could catch it**: Java's own `MockAdminClient` throws for
`describeConsumerGroups`, so the Rust mock fails every per-group future and the
*success* branch of the drain is unreachable. The suite covered the error branch
thoroughly and never touched the other one.

This generalises: **for every RPC Java's mock leaves unsupported, the drain's
success path is dead code in the test suite.** Seven of B4's nine were in that
position. B5 (ACLs, quotas, SCRAM, tokens, features) and B6 (producers,
transactions) have many more.

**How to apply:** after writing any `*_drain`, count format units against
arguments by hand for every `Py_BuildValue`, and script the sweep:

```python
re.finditer(r'Py_BuildValue\(\s*"\(([^"]*)\)"', block)   # units = alphabetic chars
```
then count top-level commas in the argument list (remember the format string
itself is argument 0, so `args == units + 1` is correct). Prefer `'s'` over an
inline `PyUnicode_FromString(...)` + `'N'`: fewer units to miscount and no
allocation. Spell the unit-to-field correspondence out in a comment when the
tuple is long.

## Small mechanics

  - Group enums (`GroupState`, `GroupType`, `ConsumerGroupState`,
    `ClassicGroupState`) have **no** `id()` in Java, so per the B2 rule they
    cross as `toString()` names. Those names are **capitalised**
    (`"Consumer"`, `"Classic"`, `"Stable"`) and are *not* the lower-case
    `"consumer"` protocol-type string sitting next to them in `GroupListing`.
    Assert both together in tests; they are adjacent and confusable, and the
    first draft got it wrong.
  - Parse with `GroupState::parse` / `GroupType::parse` so an unrecognised name
    becomes `UNKNOWN`, as Java does, rather than being a marshaling error.
  - Ragged two-level input (`Map<String, ListConsumerGroupOffsetsSpec>`) crosses
    as `const char *const *const *topics` + `const int32_t *const *partitions` +
    per-row counts, following `alterPartitionReassignments`. In C tests the
    array-of-pointers must be declared `const char *const *const topics[1]` so
    it decays to the exact parameter type.
  - A duplicate key in a two-level request is a **marshaling error**, not a
    silent overwrite: Java takes a `Map`. (Unreachable from Python, whose dict
    cannot hold one — so it needs a C test.)
  - Seeding a group in the mock: `groupConfigs` is the only map
    `MockAdminClient.listGroups` reads, and `incrementalAlterConfigs` on a
    GROUP resource (type id 32) is its only writer.
  - The `ckr-pytest:dev` image has a **stale `_confluentkafka`** baked into
    `/venv`. Skipping `pip install` in a container run silently tests an old
    extension (symptoms: `AttributeError: no attribute X_drain`, or a hang).
    Always run cargo build + `pip install --force-reinstall` + pytest in **one**
    `docker run`.
