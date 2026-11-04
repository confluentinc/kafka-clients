# M7/P2a — "Consumer test-redundancy cleanup" (.NET binding)

Status: APPROVED (maintainer 2026-08-11). Labeled **M7/P2a** (a P2b is anticipated later). N=22.
Test-only, Mode A. Scoped to exactly four source-verified findings (A1.1, A1.2, A1.4, A1.6b);
~−21 tests, zero coverage loss.

---

## 0 · Identity

- **Binding:** `.NET` (`bindings/dotnet/`)
- **Milestone / Phase:** **M7 / P2a** — continuation of the test-quality arc; **test-only** (a P2b is anticipated).
- **Assigned N (monotonic):** **22**.
- **Branch:** `prashah_dev_public_consumer_tests_parity`.
- **Mode:** **A**, **test-only** — no `src` / production change, no `confluent_kafka.h` delta; changes confined to `tests/`.
- **Source of truth:** `bindings/dotnet/tests/TEST_SUITE_AUDIT.md`, findings **A1.1, A1.2, A1.4, A1.6b** — exactly these four.
- ⚠ **Working-tree constraint (Option 1, §5):** the branch carries the uncommitted PR #144 SafeHandle fix in three files — `src/…/NativeConsumer.cs`, `src/…/OperationCompletionSource.cs`, and **`tests/Confluent.Kafka.UnitTests/AssemblyInfo.cs`** (⚠ under `tests/`). Do NOT stage, revert, or touch them; leave all three ` M`.

## 1 · Rationale

Four independently source-verified redundancies: **~−21 tests, zero coverage loss**. Each deletion's
exact assertion is already covered by a retained test (named per deletion). No `src` change.

## 2 · The four changes (each with keeper-per-deletion)

### (1) A1.1 — collapse the 8× `TopicPartition(-1)` ctor guard into one value-type test

All 8 are literally `Assert.Throws<ArgumentOutOfRangeException>(() => new TopicPartition(_, -1))` with
**no consumer interaction**.

**Add** `PublicTopicPartitionTests.cs` with `NegativePartition_Throws` asserting the **superset**
(verified `TopicPartition.cs:55-56`: `ArgumentOutOfRangeException(nameof(partition), partition,
"Partition must not be negative.")`): `ex.ParamName == "partition"` **and** message contains
`"Partition must not be negative."`.

**Delete** the 8 (grep-verified):
| Test | File:line |
|---|---|
| `TopicPartition_NegativePartition_ThrowsArgumentOutOfRange` | `PublicConsumerApiTests.cs:127` |
| `Commit_NegativePartition_RejectedByTopicPartitionCtor` | `PublicConsumerCommitTests.cs:203` |
| `OffsetQuery_NegativePartition_RejectedByTopicPartitionCtor` | `PublicConsumerOffsetQueryTests.cs:331` |
| `Position_NegativePartition_ThrowsArgumentOutOfRange` (**also drop the unused `ReadyForPosition` helper**) | `PublicConsumerPositionTests.cs:178` |
| `Ops_NegativePartition_RejectedByTopicPartitionCtor` | `PublicConsumerPartitionOpsTests.cs:322` |
| `TopicPartition_NegativePartition_ThrowsArgumentOutOfRange` | `PublicConsumerSeekLagTests.cs:227` |
| `TopicPartition_NegativePartition_ThrowsArgumentOutOfRange` | `PublicSyncConsumerPreconditionTests.cs:127` |
| `QueryFamily_NegativePartition_RejectedByTopicPartitionCtor` | `PublicSyncConsumerQueryTests.cs:479` |

**Keeper:** the new `PublicTopicPartitionTests :: NegativePartition_Throws` (superset ≥ every deleted copy).

⚠ **RETAIN (do NOT delete) — a 9th negative-partition test at a DIFFERENT layer:**
`Interop/ConsumerUnsubscribeSeekGroupMetadataTests.cs:130 :: Seek_NegativePartition_ThrowsArgumentOutOfRange`.
Verified: it exercises `NativeConsumer.Seek("proof-topic", partition:-1, offset:0)` — the interop-layer
`NativeConsumer` guard ("carried unchanged from M3/P1"), `ParamName == "partition"`, a distinct code path
from the public `TopicPartition` ctor. Not one of the 8; unique interop coverage. **Delete by exact
file:method, never by name-grep** — its name matches the A1.1 family.

### (2) A1.2 — delete one byte-identical `Assign→Assignment` twin

Verified character-identical: `PublicConsumerPartitionOpsTests.cs:60 :: Assign_ThenAssignment_ReflectsExactlyTheAssignedPartitions`
and `PublicConsumerSyncReadTests.cs:73 :: Assignment_ReflectsAssign_ExactlyTheAssignedPartitions`.
**Delete** the `PartitionOpsTests` copy. **Keeper:** `PublicConsumerSyncReadTests :: Assignment_ReflectsAssign_ExactlyTheAssignedPartitions` (owns `Assignment()`).

### (3) A1.4 — consolidate the 15 "ViaInterface" upcast tests

Verified: **no explicit interface implementation** in `src/` (only XML-doc artifacts in `bin/`), so
`I… c = mock; c.Member(...)` dispatches to the identical member — a ViaInterface test's only residual
value is pinning "member X is reachable through interface Y."

**Consolidation criterion (the correctness rule — NOT a hardcoded count):** keep the *minimal* set of
broad "reachable via interface" smokes such that **every (member, interface) pair currently reached
through an interface remains reached through that interface by a retained test.** No interface-membership
guarantee lost.

**Prefer the batched smokes as keepers:** `PublicConsumerSyncReadTests :: SyncReads_ReachableViaIAsyncConsumerInterface`,
`PublicConsumerSeekLagTests :: SeekAndCurrentLag_ViaIConsumerCommon_Work`, one sync
`…_ViaIConsumerInterface` batched smoke (e.g. `SyncMockConsumer_ViaIConsumerInterface_RoundTrips`).
**Delete** the per-member singletons (≈12). The 15: (async via `IAsyncConsumer`) Poll / Position /
BeginningOffsets / Commit / CommitAsync / PartitionsFor / ListTopics / Assign, + SyncReads &
SeekAndCurrentLag via `IConsumerCommon`; (sync via `IConsumer`) Poll / Committed / PartitionsFor /
ListTopics, + `SyncMockConsumer_ViaIConsumerInterface_RoundTrips`. **Expected ≈3 keepers / ≈12
deletions — but the CRITERION governs:** the Actor maps each of the 15 to the (member, interface) pair(s)
it pins, keeps whatever minimal set covers the union, and if a pair is reached **only** by a
would-delete singleton, that singleton is **kept** (or folded into a batched keeper). Coverage-preservation
wins over the ≈12 estimate. Each deleted singleton's (member, interface) reachability must be named to a
retained smoke.

### (4) A1.6b — delete a strict-subset commit round-trip

`PublicSyncConsumerRoundTripTests :: Commit_WithOffsets_BrokerFree_Succeeds` is a strict subset of
`PublicSyncConsumerQueryTests :: Committed_AfterCommit_RoundTripsOffsetMetadataAndEpoch` (identical
MockConsumer + Assign + `Commit(42,"meta-x",7)`; the keeper adds the `Committed(...)` read-back).
**Delete** the subset. **Keeper:** `PublicSyncConsumerQueryTests :: Committed_AfterCommit_RoundTripsOffsetMetadataAndEpoch`.
**Keep** `Commit_NoOffsets_…` / `Commit_EmptyOffsets_…` (distinct paths).

## 3 · Critic mandate — "no unique coverage lost" (per deletion; maintainer-required)

For **every deletion**: **name the keeper** covering its exact assertion — no deletion lands without a
named keeper. Specifically:
- **A1.1:** the new `NegativePartition_Throws` asserts the **superset** (ParamName + message); the **9th
  interop test survives**.
- **A1.4:** the retained smokes reach **every** (member, interface) pair any deleted singleton pinned —
  build the mapping, verify the union is fully covered, flag any un-reached pair.
- **A1.2 / A1.6b:** the kept twin / superset round-trip byte-covers the deleted one.
- No `src/` or `AssemblyInfo.cs` in any commit (§5); no orphaned helpers / dead `using`s (esp. dropped
  `ReadyForPosition`); the new `PublicTopicPartitionTests` is discovered by the runner. Report
  before/after test counts as the coverage-delta receipt.

## 4 · Explicitly EXCLUDED / parked

A1.3, A1.5 ([Theory] folding), A1.6a, A1.6c, A1.7, A1.8, all of §A2 (cross-layer), all of §B
(organization/renames/moves), and the `KafkaExceptionTests` question. This phase is ONLY A1.1 / A1.2 /
A1.4 / A1.6b.

## 5 · Working-tree constraint (PR #144 — Option 1)

Commit **only** `tests/…` edits, `dotnet(M7/P2a): …` (`--no-gpg-sign`, `Co-Authored-By: Claude Opus 4.8`).
**Never** `git add`, revert, or touch the three PR #144 fix files (`src/…/NativeConsumer.cs`,
`src/…/OperationCompletionSource.cs`, **`tests/…/AssemblyInfo.cs`**). ⚠ `AssemblyInfo.cs` is under
`tests/`, so "commit only tests/" is NOT sufficient — **explicitly exclude** it. Use **per-path
`git add` / `git rm`** of exactly the edited/added/deleted test files, then `git status --short` +
`git diff --cached --name-only` before **every** commit to confirm none of the three fix files (and no
unrelated untracked file, e.g. `tests/CONSUMER_TEST_COVERAGE_COMPARISON.md`) is staged. Leave the fix
untouched (all three ` M`).

## 6 · DoD gates

- `dotnet build` — 0 warnings / 0 errors on all TFM legs (no orphaned helpers / dead `using`s — esp. the
  dropped `ReadyForPosition`; warnings-as-errors catches an unused `using`).
- `dotnet test` — green (full suite); confirm the new `PublicTopicPartitionTests` runs; report exact
  before/after test counts (the ~−21 receipt).
- `dotnet format --verify-no-changes` — clean.
- `cargo build --features ffi` — no `confluent_kafka.h` delta (test-only; confirm).
- The three PR #144 fix files remain ` M` (uncommitted, untouched) at the end.

## 7 · Risks / notes

- **The 9th interop test is the over-deletion trap** — delete by exact file:method, not name-grep.
- **A1.4 is criterion-driven, not count-driven** — if estimate vs criterion conflict, keep more.
- **`AssemblyInfo`-under-`tests` trap** — the highest-risk mistake; per-path staging + pre-commit
  `git diff --cached` guards it.
- **Zero coverage loss is the contract** — if any deletion has unique coverage (no valid keeper), it is
  **kept** and noted, not force-deleted.
- Test-only, minimal surface.

## 8 · Comment workflow & handoff (Manager)

`dotnet-actor N=22` (test files only; per-path staging with the §5 guard) → DoD + before/after counts →
`dotnet-critic N=22` (§3 mandate) → fix cycle until `COMMENTS.22.md` empty + DoD passes → archive
`COMMENTS.DONE.22.md` under `design/history/M7/P2a-test-redundancy-cleanup/`, update STATUS.md (M7/P2a
DONE), reset `COMMENTS.22.md`.
