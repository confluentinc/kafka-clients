---
name: m11-g4-admin-groups-offsets-multilanguage
description: Milestone 11 G4 (admin multilanguage groups & group offsets) — the valid()/errors() split needs a whole-value response not a per-key one, why a Java-derived field must be *checked* rather than carried, the two enums whose transposition is invisible by construction, and the image-build script's "unchanged" verdict explained
metadata:
  type: project
---

Slice G4 of `design/history/Milestone-11/PLAN-multilanguage-admin.md`: the nine
group / group-offset RPCs across all four backends. 16 scenarios → 64 entries;
204 harness admin entries total, 27 of 46 RPCs.

## `valid()` / `errors()` is a whole-value response, not a fifth shape

`ListGroupsResult` / `ListConsumerGroupsResult` split **one**
`KafkaFuture<Collection<Object>>` into `valid()` listings and an **unkeyed**
`errors()` collection. The per-key envelope cannot express it — the errors have
no key to attach to — so it is envelope addendum (a) (whole-value) with two
`repeated` fields side by side. The two lists are independent and of unrelated
length; a partial success has both non-empty, and nothing may be zipped or
indexed across them. Both bindings already do exactly this (`admin.py` returns
`([GroupListing], [KafkaError])`; the C handle has `_valid_count`/`_get_valid`
next to `_error_count`/`_get_error`), so no fifth shape was needed.

`listConsumerGroupOffsets` is the `describeLogDirs` two-level case again: per-key
by group id, value = the whole `Map<TopicPartition, OffsetAndMetadata>` where the
**value** is nullable. `has_offset` at the C boundary / inner `None` in Python.

## A field Java *derives* must be checked on decode, not just carried

Four fields are derived in Java, so the Rust constructors do not accept them:
`ConsumerGroupDescription`/`ConsumerGroupListing`'s deprecated `state()`
(= `ConsumerGroupState.parse(groupState())`), and
`GroupListing`/`ClassicGroupDescription`'s `isSimpleConsumerGroup()`
(= `group_type == Classic && protocol.isEmpty()` / `protocol.isEmpty()`).

Both bindings expose each as its own accessor, so they cross the wire — but
merely carrying them is **worthless**: `MultilanguageAdmin` rebuilds the object
from the *other* fields, re-derives these, and every scenario assertion then
passes no matter what the wire said. The fix is a consistency check at the decode
boundary (`check_derived_state`, and the two `is_simple_consumer_group`
comparisons) that fails the call when the wire value disagrees with the derived
one. General rule: **if the harness re-derives a field, assert the wire's copy
against the derivation or drop it from the wire — carrying it silently is a
false claim of coverage.**

Same reasoning for enum names: `GroupState::parse` maps anything unrecognised to
`Unknown` (faithfully — Java does too), which would absorb a garbled field into a
valid value. Reject a name that parses to `Unknown` without *being* "Unknown".

## `state()` vs `group_state()`: a transposition is invisible *by construction*

`GroupState` has 9 variants, `ConsumerGroupState` 8, and their constant names
coincide for all 8. The only separating state is `GroupState::NotReady`
(`clients/.../common/GroupState.java:60`), which parses to `Unknown`. Its javadoc
table and `groupStatesForType` (`:80-89`) list `NOT_READY` under **STREAMS
only**, and a streams group is not something `describeConsumerGroups` can return
(the coordinator's `consumerGroup(...)` answers `GROUP_ID_NOT_FOUND` for another
type). So this is stronger than "unreachable on this fixture": no cluster
configuration reaches it. A *dropped* field is still caught, by the derived-state
check above. Say which of the two you have when reporting a pair like this.

## `removeAll` is preserved, and it is Java's own emptiness rule

Unlike `NewPartitions.newAssignments`, `removeAll` passes the G3 discriminator:
the C entry points take a dedicated `bool remove_all`, `admin.py` computes
`members is None` into its own column, and the wire uses
`optional MemberToRemoveList`. But `removeAll()` *is* `members.isEmpty()` in Java
(`RemoveMembersFromConsumerGroupOptions.java:57-59`) — the emptiness test is the
contract, not a collapse, because the `Collection` constructor **throws** on an
empty collection (`:33-37`). So there are exactly two legal states.

Only **half** observable, and for a new reason: the present-but-empty state is
unreachable from a scenario because `AdminBackend` takes the *options type*, and
Rust's constructor refuses to build it. Reaching it would need a harness escape
hatch that emits raw proto, which the native arm could not join. Different cause
from `electLeaders`' half-observability (there the broker cannot tell the states
apart); same reporting duty.

## Multi-key coverage needs a purpose-built cluster, not a disclaimer

`kip848_3_broker` pins `KAFKA_OFFSETS_TOPIC_NUM_PARTITIONS=1`, so **every** group
has the same coordinator and the `CoordinatorStrategy` fan-out never has two
destinations. Overriding it to 3 (one local `fn` over the shared config) lets a
batch of groups hash to different brokers. `backend_pool` keys containers by
`(kind, broker_network)`, so a distinct config just starts its own cluster —
cheap. Pair it with an exact-key-set assertion; a one-key response cannot detect
entries keyed by loop index or a short list.

`all_of` returns `Ok` for an **empty** map and `keyed` builds its map purely from
the response's `entries`, so a backend answering with no entries passes. Added
`all_of_exactly(admin, outcomes, expected_keys, what)` and used it everywhere a
scenario asserts "the batch succeeded" (G4, plus retrofitted to the two G2/G3
sites the Critic named). Compares key **sets**, not sorted vectors —
`TopicPartition` is not `Ord` and entry order is unspecified anyway.

## Request-validation failures need one *variant*, not one level

C++ stamped `VARIANT_ILLEGAL_ARGUMENT` for a malformed `OffsetSpec`; both Python
servers raised a bare `ValueError`, which fell through `_kafka_error_to_proto`'s
generic branch to `ILLEGAL_STATE` — and `variant` is the field the Rust client
matches on. Both sites carried a comment asserting they agreed "at the same
level": true about the level, silent about the variant. Fixed with
`grpc_translate.AdminRequestError` as a **rule** (mapped to ILLEGAL_ARGUMENT),
generalising the one-off `_admin_constructor_error`. Never write "the two servers
agree" without comparing the variant.

## `describeClassicGroups` has one deterministic path here, and it is not a compromise

No classic group is creatable (`consumer-threading.md` §20), but
`GroupMetadataManager.describeGroups`
(`group-coordinator/.../GroupMetadataManager.java:735-786`) resolves through
`classicGroup(...)`, which throws `GroupIdNotFoundException` for a group that is
not a `ClassicGroup`; v6+ then reports `GROUP_ID_NOT_FOUND`. So pointing it at a
live KIP-848 group is deterministic, takes the per-key **error** arm, and pins
that the two describe RPCs are not aliases of one another.
`ConsumerProtocol::deserialize_assignment` stays unreachable end to end — only
the `isInState(STABLE)` branch (`:744-757`) populates member metadata.

## The build script's "unchanged" verdict is trustworthy — and means what it says

A comment-only edit to `server.cc` produced `OK ... (unchanged: ... fully cached,
no input differed)`. That is **not** a stale image: the log shows Step 5 (COPY
server.cc) missing "Using cache" and Step 11 (cmake build) re-running; only the
final `COPY --from=builder` layer was cached, because the compiled binary is
byte-identical. Check the per-image log in `$CTX/.build-*.log` before suspecting
staleness. Bonus: restoring the mutated Python file and rebuilding returned the
image id to the exact pre-mutation sha, which is a byte-exact restore proof.

## Teeth check

Transposed `GroupListing.group_id ↔ .protocol` and
`ConsumerGroupDescription.group_id ↔ .partition_assignor` in the **sync** Python
encoder, rebuilt only that image: exactly the 4 predicted `__grpc_python` arms
went red (the failure prints `left: "uniform"` — the assignor arriving as the
group id), while `__grpc_python_async` stayed green *on the same shared source
file* because its image still had the unmutated copy. Freshness, discrimination,
and two-independent-Python-servers, in one run.
