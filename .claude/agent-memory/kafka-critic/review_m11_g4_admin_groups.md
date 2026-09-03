---
name: review-m11-g4-admin-groups
description: M11 G4 (9 group/group-offset admin RPCs) review — reachable-by-composing-two-in-slice-RPCs gap, test pinning a DEFERRED defect, tautological assert_ne strengthening claim, and the concurrent-Actor working-tree trap
metadata:
  type: project
---

Round 15 reviewed `61857016..e336e19b` (G4: `10ee2f3d`, `8cab5d55`, `66e6ff2f`,
`2c24a674`, `fae77d7e`, `e336e19b`). Highest-quality slice of the six; the two
hardest self-critical claims were both correct.

## New finding classes worth reusing

**"Unreachable" can be false by composing two RPCs in the same slice.** Round 14's
rule asked whether a different `ClusterConfig` reaches the state. G4's gap needed
neither a new fixture nor a new API: `alterConsumerGroupOffsets` on a
never-consumed group id creates a **simple classic group**
(`OffsetMetadataManager.java:458-467` — generation `< 0` on an unknown group calls
`getOrMaybeCreateClassicGroup(id, true)`; `GenerationIdOrMemberEpoch` defaults to
`-1`, `OffsetCommitRequest.json:46`, and no admin handler sets it). That makes
`describeClassicGroups`' **value** arm, `GroupType::Classic`, and
`is_simple_consumer_group == true` all reachable — the slice recorded all three as
impossible. **Check: before accepting an unreachability record, enumerate the other
RPCs in the same slice and ask whether any produces the required server-side
state.** Third occurrence in this milestone (G2 `authorizedOperations`, G3
`describeLogDirs` fan-out, G4 here).

**A derivation check that only ever sees one value of a boolean is half-dead.**
`is_simple_consumer_group` is derived in Java, so the harness *checks* it instead
of passing it — correct design, but every group in the slice is `Consumer`-type, so
it only ever compares `false == false`. A backend hardcoding `false` is invisible.
Same shape applies to any "check the derived field" pattern: ask which values of
the derivation the fixtures actually produce.

**A test can pin a known-DEFERRED production defect and break its fix.** G4's
`remove_members_rejects_an_explicitly_empty_selection` asserts
`!matches!(err, KafkaError::IllegalArgument(_))` on the wire outcome. Java's own
outcome there *is* `IllegalArgumentException` (`LeaveGroupRequest.java:45-46`);
Rust surfaces it as `UNSUPPORTED_VERSION` only because of DEFERRED 3
(`RequestBuilder::build_version` → `io::Error` → `network_client.rs:507`). Fixing
DEFERRED 3 breaks the test. **Check: when a scenario's expected outcome coincides
with an open DEFERRED item, does it assert the invariant that survives the fix
(message text) or the defective variant?**

**A claimed strengthening entailed by adjacent assertions is not a strengthening.**
`assert_ne!(a, b)` placed after `assert_eq!(a, X)` and `assert_eq!(b, Y)` with
`X != Y` is unfalsifiable. Ask for the input the old assertions accept and the new
ones reject; if none exists the change is cosmetic. (Round 14 asked for weakenings
to be declared; G4 declared none and had none — the ledger error moved to the
over-claiming direction, plus it omitted the slice's *largest* real strengthening.)

## Process trap that nearly caused a false "fixed"

**HEAD advances and a concurrent Actor dirties the working tree mid-review.**
`git status` was clean at minute 0, then the G5 Actor committed and left
`tests/common/admin_backend.rs`, `multilanguage_admin.rs` and three
`tests/integration/*` files modified. Reading the working tree made round-14
Issue 3 (reassignment predicate) look closed — the fix exists only as uncommitted
work. **Always `git archive <sha> | tar -x -C <scratch>` at the start and derive
every line number from the snapshot; re-run `git rev-parse HEAD` + `git status`
before finalising and re-verify any line number gathered early.**

## Verified-clean areas (do not re-derive)

- `GroupState` (9 constants) vs `ConsumerGroupState` (8): names coincide for all
  eight, so a `state`/`group_state` transposition is invisible **by construction**;
  the separator `NOT_READY` is STREAMS-only (`GroupState.java:60`, `:78-89`) and
  `describeConsumerGroups` cannot return a streams group. `check_derived_state` is
  real and non-vacuous for a *dropped* field (proto `state` is non-optional → `""`
  mismatches), and the three enum decoders are fail-loud (parse-to-`Unknown`
  without spelling "Unknown" is a protocol error).
- `removeAll`: Java's `removeAll()` **is** `members.isEmpty()`
  (`RemoveMembersFromConsumerGroupOptions.java:59-61`) because the `Collection`
  ctor rejects empty (`:33-38`), so an FFI emptiness test would have been faithful
  — and the C boundary carries a dedicated `bool remove_all` anyway
  (`src/ffi/admin.rs:12268`). Present-but-empty is unconstructible through
  `AdminBackend`. Not the `NewPartitions` defect.
- Envelope shapes for all nine, checked against each Java `*Result`: `listGroups` /
  `listConsumerGroups` are three futures with an **unkeyed** `Collection<Throwable>`
  errors list (so `Listings<T>` is a sound DoD #7 addition — both bindings already
  return the pair); `alter`/`delete…Offsets` and `removeMembers` are one-future
  (wide); `deleteConsumerGroups` is per-key (narrow); `listConsumerGroupOffsets` is
  per-group-future-over-a-whole-map with a **nullable** value
  (`ListConsumerGroupOffsetsHandler.java:169-171` `offsets.put(tp, null)`).
- Three-way handler equivalence for all nine: request optionals, timeouts (no 1 ms
  skew inherited), enum-name spelling both directions, sync-vs-async Python
  token-identical. Only latent divergences: a dict-keyed
  `listConsumerGroupOffsets` encoder that silently de-dups duplicate group ids
  (`grpc_translate.py:1007-1010`) where the FFI errors, and Python response
  encoders lacking C++'s null-value branches — worsened by the servicers' `try`
  covering only the client call, not the response builder
  (`grpc_server.py:919-926`), so an encoder failure crosses as gRPC UNKNOWN with
  no `KafkaError` envelope.
- `assert_real_coordinator` (`admin_groups_test.rs:218-249`) genuinely checks
  non-empty host, `port > 0`, id ∈ same-backend `describeCluster` nodes, and
  `(host, port)` equality — it would have failed the historical
  `Node { host: "", port: -1 }`. Its gap is that `ClassicGroupDescription::coordinator()`
  is a **separate decode site** never pointed at.
- Teeth check is the strongest so far: transposing two fields in the **sync**
  Python encoder turned exactly the 4 predicted `__grpc_python` arms red while
  `__grpc_python_async` stayed green on the same shared source file — proving
  freshness, discrimination and image independence at once. The byte-exact restore
  sha is a weaker, separate claim (rebuild determinism), correctly framed as a bonus.
- `__rust` = 50, `__grpc` = 0 under `--features integration-tests` (166 tests
  total); 16 of the 50 are the two G4 files. Claim exact.

## Closed this round

Round-14 Issue 1 (Python `ILLEGAL_STATE` vs C++ `ILLEGAL_ARGUMENT`) — closed by
`2c24a674`'s `grpc_translate.AdminRequestError`, verified exhaustively (the only
`raise` statements left in the three server/translate files are the two
`AdminRequestError` sites). Round-14 Issue 4 (`all_of` cardinality) — closed by
`fae77d7e` at all five named folds, with the caveat that `all_of_exactly`'s key-set
half is tautological on the `__rust` arm (`RustNativeAdmin` builds `Outcomes` from
the *request's* keys), so the teeth live only on the container-gated gRPC arms.
