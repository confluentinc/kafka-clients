---
name: m11-g3-admin-elections-offsets-multilanguage
description: Milestone 11 G3 (admin multilanguage elections/reassignments/offsets) — how to decide whether a null-vs-empty distinction is actually preserved, why a named enum beats forwarding the C sentinel, the throttle recipe that makes in-flight reassignments deterministic, and the scratchpad build script that hides compile failures
metadata:
  type: project
---

Slice G3 of `design/history/Milestone-11/PLAN-multilanguage-admin.md`: the four
elections / reassignments / offsets RPCs across all four backends, 32 green
entries (140 admin entries total, 18 of 46 RPCs).

## Deciding whether a null-vs-empty distinction survives: look for the discriminant

The G1 Critic found the proto *claiming* `NewPartitions.newAssignments`
null-vs-empty was preserved when three backends collapse it. The test that
separates the real cases from the broken one is mechanical:

  - **Preserved** if the C entry point takes a dedicated `bool` argument.
    `electLeaders`/`listPartitionReassignments` take `all_partitions`
    (`read_optional_partition_set` returns without reading the arrays when set);
    `alterPartitionReassignments` takes `cancel[i]` (`read_reassignments` does not
    read the replica columns for a cancelled row). Both survive `_confluentkafka.c`
    (`"KipOiO"` / `"sipO"` parse specs) and admin.py (`partitions is None`,
    `r is None` columns), and the native builder distinguishes them too
    (`set_topic_partitions(None)` vs `Some(vec)`).
  - **Collapsed** if the FFI decides by emptiness. `NewPartitionsBuilder::build`
    does `if self.new_assignments.is_empty()`, so the C boundary cannot express
    `increaseTo(n, emptyList())` at all.

Also check the *wire* spec, since a distinction the broker cannot see is not worth
carrying: `grep nullableVersions generator/messages/<Rpc>Request.json`.
`ElectLeadersRequest.json:29` and `AlterPartitionReassignmentsRequest.json:37`
have it (the latter's `about` literally says "or null to cancel");
`CreateTopicsRequest.json:45` does not, which is why `NewTopic`'s identical caveat
is harmless and `NewPartitions`' is not.

**Preserved does not mean observable.** Distinguish three states per distinction
and say which the fixture reaches:
  - `alterPartitionReassignments` cancel: fully observable. Cancelling with
    nothing in flight → `NO_REASSIGNMENT_IN_PROGRESS`; an empty replica list would
    give `IllegalArgument`/`INVALID_REPLICA_ASSIGNMENT` instead.
  - `listPartitionReassignments` selection: fully observable *once a move is in
    flight* — absent lists it, `Some(empty)` does not.
  - `electLeaders` selection: **half** observable. `ReplicationControlManager.java:1507`
    branches on `topicPartitions() == null` and there *omits* every
    `ELECTION_NOT_NEEDED`, so explicit `{tp}` → 1 entry while absent → 0 entries
    (catches an explicit set widened to cluster-wide, the dangerous direction).
    But absent and `Some(empty)` both give 0 on a healthy cluster, so the other
    direction is unreachable. Write that down rather than implying coverage.

## Cross a *named* variant, not the C boundary's encoding, when both servers own a table

`OffsetSpec` could have crossed as the `(is_timestamp, signed value)` pair both
bindings take — the `ConfigSource`-as-enum-constant-name precedent. It crosses as
a named `OffsetSpec.Kind` + `optional timestamp` instead, because forwarding the
pair would state the six-sentinel table **once in the harness and leave it
asserted by nobody**: both servers would forward it unexamined. With a named kind,
`grpc_translate.py` reaches the sentinels through `admin.py`'s public factories and
`server.cc`'s `offset_spec_columns` writes the -1..-6 table itself, so the two
tables are independent and a disagreement is a finding. General rule: if a value's
encoding is a table each binding already owns, name the *concept* on the wire.

Keep the kind separate from the timestamp regardless — `getOffsetFromSpec` is not
injective (`forTimestamp(-2)` and `earliest()` both project to -2). Note the
broker cannot tell them apart either (probed: identical answers), so that
discriminant is only observable against `MockAdminClient`; what *is* observable is
`forTimestamp(0)` returning a real timestamp where `earliest()` returns -1.
`KIND_UNSPECIFIED = 0` plus a protocol error in all three servers, never a
defaulted variant — a defaulted `EARLIEST` would turn a dropped field into a pass.

## Probe the broker before asserting; five OffsetSpec answers were not guessable

A throwaway `__rust`-only probe test (write, run with `--nocapture`, delete) paid
for itself immediately. On a plain single-node KRaft 4.2 broker:

  - `earliest`/`latest`/`earliest_local` → real offset, `timestamp = -1`, epoch present
  - `max_timestamp`, `for_timestamp(0)` → real timestamp
  - `latest_tiered` → offset **-1** and leader epoch **None** — the only route in
    the suite to Java's `Optional.empty()` leader epoch
  - `earliest_pending_upload` → **per-partition** `UnsupportedVersion` — the only
    exercise of `ListOffsetsEntry`'s per-key error arm
  - `for_timestamp(far future)` → offset -1, epoch None

Two of those close coverage gaps nothing else could reach, and neither was
predictable from the Java source.

## The throttle recipe: in-flight reassignments are deterministic, not racy

PLAN §D3 recorded `PartitionReassignment{replicas, adding, removing}` as never
populated. It is reachable, and reliably (observed on poll attempt 0, whole
scenario ~4 s):

 1. `ClusterConfig::with_brokers(3)`, RF-1 topic.
 2. Topic configs `leader.replication.throttled.replicas=*` **and**
    `follower.replication.throttled.replicas=*`.
 3. Every broker: `leader.replication.throttled.rate=1024` and
    `follower.replication.throttled.rate=1024`.
 4. Produce ~2 MiB, then reassign. At 1 KiB/s the move lasts ~30 min.

Both halves are required — a rate with no throttled-replica list throttles
nothing. This is what `ReassignPartitionsCommand --throttle` sets. In that state
`replicas` is the union of source and target and the two deltas name one broker
each, all three different, so a transposition fails (with a completed move every
vector is empty and a transposition passes).

## The scratchpad image-build script hides compile failures

`build-grpc-images-macos.sh` pipes `docker build` through `| tail -12` and wraps
it in `if ...; then :; fi`, so a C++ compile error prints "OK" and the **stale
image survives**. Symptom: an error fragment with no "In instantiation of" line.
Run `docker build -t <tag> -f bindings/c/Dockerfile.grpc "$SCRATCH/grpc-ctx"`
directly to see real diagnostics.

Related trap: `server.cc`'s `using confluent::kafka::test::...` list is explicit
and does not glob. A missing name makes the request parameter deduce to `int`, and
every member access then fails with "request for member 'x' in 'req', which is of
non-class type 'const int'" pointing at an unrelated template — including the
shared `timeout_ms(const Req&)` helper. Add all new message names first.

## Verifying Python-side proto access without PyPI

The host has no `grpc_tools` and PyPI is behind CodeArtifact auth, so nested-enum
access and `HasField` semantics were checked by running a script *inside* the
built image (`docker run --rm --entrypoint python <img> /check.py`). Confirms
`apb.OffsetSpec.EARLIEST` works (nested enum values hoist into the message class)
and that `HasField` separates absent from present-empty for both wrapper messages.
There is no pytest in the images, so a new unit test's body has to be executed
directly rather than through pytest.

## Teeth check: mutate a shared file, rebuild only one image

Transposed `offset`/`timestamp` in `_admin_list_offsets_response` and
`adding`/`removing` in `_admin_list_partition_reassignments_response`, then
rebuilt **only** the sync Python image. Result: exactly the 4 `__grpc_python` arms
touching those encoders went red; `__grpc_python_async` stayed green *because its
image still had the unmutated file*. That is stronger than G2's version — it
proves freshness, discrimination, **and** that the two Python arms do not share a
server. Restore from a saved copy and rebuild again.
