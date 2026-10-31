# COMMENTS.22 — Critic review of M7/P2a "Consumer test-redundancy cleanup"

Branch `prashah_dev_public_consumer_tests_parity`, commits `f31c415c..HEAD`
(`bf64b2e1` A1.1, `36871668` A1.2, `23aaf3a4` A1.4, `69e3de7e` A1.6b, `05408022` STATUS).
Reviewed against the PLAN's maintainer-required "no unique coverage lost" gate.

## Verdict: CLEAN — no issues filed.

Every deletion has a named, verified keeper; no `src`/`AssemblyInfo`/PR-#144 file in any commit;
both PLAN deviations correctly apply the coverage-preservation clause; DoD met.

### (a) No PR-#144 / `src` / `AssemblyInfo` file in any commit — CONFIRMED
- `git diff f31c415c..HEAD --name-only` = 13 `tests/…/*.cs` files + `design/current/STATUS.md` only.
  No `src/…/NativeConsumer.cs`, no `src/…/OperationCompletionSource.cs`, no
  `tests/…/AssemblyInfo.cs`, no unrelated file.
- All three PR-#144 fix files remain ` M` (modified-uncommitted, untouched) in the working tree.

### (b) A1.4 — every (member, interface) pair still reached through the interface — HOLDS
15 ViaInterface tests at base; **4 retained**, **11 deleted**; each deleted pair re-covered:
- Async via `IAsyncConsumer` (deleted #1 Commit, #2 BeginningOffsets, #3 PartitionsFor, #4 ListTopics,
  #5 Assign, #6 Position, #7 Poll) → reached by interface-typed helpers whose parameter is
  `IAsyncConsumer<byte[],byte[]>` and whose body invokes the member on that param, each with live
  retained callers: `CommitOf`/`CommitOffsetsOf` (CommitTests), `AssignOf` (CommitTests:77,105),
  `BeginningOffsetsOf` (OffsetQueryTests), `PartitionsForOf`/`ListTopicsOf` (PartitionMetadataTests),
  `PositionOf` (Position/SeekLagTests), and `Poll(IAsyncConsumer<…>)` (RoundTripTests:254, ~14 live
  `[Fact]` callers). Verified each helper signature is the interface, not the concrete
  `AsyncMockConsumer`.
- Sync via `IConsumer` (deleted #12 Committed, #13 PartitionsFor, #14 ListTopics, #15 Poll) → folded
  into the extended `SyncMockConsumer_ViaIConsumerInterface_RoundTrips` (TfmSmoke), which upcasts
  `IConsumer<byte[],byte[]> consumer = mock;` and drives Poll + Commit/Committed + PartitionsFor +
  ListTopics through that interface variable. Sync Poll is additionally pinned by
  `Poll(IConsumer<byte[],byte[]>)` (SyncRoundTripTests:324, ~9 live callers — not orphaned).
- Removed helpers `TestTimeoutResult` + `Poll(Task<…>)` were private to the deleted async-Poll
  singleton; the two same-named `TestTimeoutResult` in the Interop files are per-class and unaffected.

### (c) Both deviations correct
- **Dev 1 — `ReadyForPosition` KEPT (PLAN said drop):** genuinely still called by 9 retained tests
  in PublicConsumerPositionTests (1 def + 9 live call sites). Deleting it would break the
  warnings-as-errors build. Correct; the PLAN's "now-unused" note did not hold against the file.
- **Dev 2 — `CommitAsync_ViaIConsumerCommonInterface_…` KEPT:** it is the ONLY test reaching
  `CommitAsync` through an `IConsumerCommon`-typed variable. The other three `CommitAsync` call sites
  use the concrete type (`AsyncMockConsumer` at CommitTests:148,275; `MockConsumer` at
  SyncRoundTripTests:227). Keeping it was mandatory for coverage preservation, not over-retention;
  the claim is not false.

### (d) A1.1 superset + 9th interop + A1.2 / A1.6b keepers — CONFIRMED
- A1.1: new `PublicTopicPartitionTests.NegativePartition_Throws` asserts BOTH `ParamName ==
  "partition"` AND message `"Partition must not be negative."` (matches `TopicPartition.cs:55-56`) —
  a superset of every one of the 8 deleted copies (each asserted ParamName and/or message only).
  8 deleted by exact method across the 8 named files; new test discovered + passing.
- 9th interop test `Interop/ConsumerUnsubscribeSeekGroupMetadataTests.Seek_NegativePartition_…`
  (line 130, a distinct `NativeConsumer.Seek(partition:-1)` code path) SURVIVES.
- A1.2: deleted `PartitionOpsTests.Assign_ThenAssignment_…` is byte-identical to the retained keeper
  `SyncReadTests.Assignment_ReflectsAssign_ExactlyTheAssignedPartitions`; orphaned
  `using System.Collections.Generic` correctly removed.
- A1.6b: deleted `Commit_WithOffsets_BrokerFree_Succeeds` is a strict subset of the retained
  `SyncConsumerQueryTests.Committed_AfterCommit_RoundTripsOffsetMetadataAndEpoch` (same MockConsumer +
  Assign + `Commit({tp: OffsetAndMetadata(42,"meta-x",7)})`, plus the `Committed(...)` read-back);
  siblings `Commit_NoOffsets_` / `Commit_EmptyOffsets_` retained.

### DoD — MET
- `cargo build --features ffi` — no header delta (Mode A; native up to date).
- `dotnet build` — 0 warnings / 0 errors on all TFM legs (ns2.0/net8.0/net10.0 lib; net462/net8.0/net10.0 tests).
- `dotnet test -f net10.0` — 417 passed / 0 failed / 0 skipped, stable across 4 runs (matches the
  437→417, net −20 receipt = A1.1 −7 + A1.2 −1 + A1.4 −11 + A1.6b −1; parallel host-crash mitigated by
  the PR-#144 serial flip). New `PublicTopicPartitionTests` discovered + passing.
- `dotnet format --verify-no-changes` — clean (exit 0).
- net8.0/net462 legs compile-verified (build) — execution is CI-only here (only net10.0 runtime installed).
