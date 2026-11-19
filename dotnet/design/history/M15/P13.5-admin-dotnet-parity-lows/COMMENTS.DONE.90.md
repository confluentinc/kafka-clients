# COMMENTS.DONE.90 — M15/P13.5 (Admin parity lows) resolved findings

## 90.1 Nine X2 test comments say "the value actually sent is pinned by the submit-seam tests", and no seam test pins it for those RPCs — low
- Where: commit ec2ddd7a (S1d). The claim appears in nine places:
  - the class `<summary>` of `dotnet/tests/Confluent.Kafka.UnitTests/PublicAdminNegativeTimeoutTests.cs` ("The per-RPC submit-seam tests pin the value each RPC sends");
  - `PublicAdminClusterConfigResourcesTests.NegativeTimeout_IsNotRejectedSynchronously` (DescribeCluster, ListConfigResources);
  - `PublicAdminConfigsTests.NegativeTimeout_IsNotRejectedSynchronously` (DescribeConfigs, IncrementalAlterConfigs);
  - the six negative-timeout tests in `PublicAdminP8Tests` (AbortTransaction, ForceTerminateTransaction, FenceProducers, DescribeTransactions, DescribeProducers, ListTransactions).

  Each test comment says "The value actually sent is pinned by the submit-seam tests."
- Anchor: Java `KafkaAdminClient.java:496-499` (`calcDeadlineMs` → `now + Math.max(0, optionTimeoutMs)`), and PLAN §3.4, Tests bullet 2.
  - The C ABI reads a negative `timeout_ms` as unset (the client default). It reads `0` as `now + 0`. So the clamp is what makes a negative mean "already expired", as in Java, rather than "use the default".
  - Only four seam suites send a negative `TimeoutMs`: `AdminLogDirsSubmitArgumentTests`, `AdminP4SubmitArgumentTests`, `AdminP4Stage2SubmitArgumentTests` and `AdminP5SubmitArgumentTests`.
  - Between them they cover 11 RPCs: DescribeLogDirs, AlterReplicaLogDirs, DescribeReplicaLogDirs, ListOffsets, ListPartitionReassignments, ElectLeaders, AlterPartitionReassignments, ListGroups, DescribeConsumerGroups, DescribeClassicGroups and ListConsumerGroupOffsets.
  - ListTopics is pinned behaviourally by `X2_RealClient_ANegativeTimeout_FailsExactlyAsAZeroTimeout`.
  - The seam tests for the RPCs these comments name have only `null`, `0` and positive rows:
    - `AdminP3SubmitArgumentTests.ExplicitTimeout_IsForwardedVerbatim` (0, 12_345, 23_456);
    - `AdminConfigsSubmitArgumentTests.DescribeConfigs_TimeoutMapping` and `IncrementalAlterConfigs_TimeoutMapping` (0, 4_242, 5_353);
    - the `AdminP8SubmitArgumentTests` timeout tests (500, 250, 0, and the default sentinel).
- Problem: the tests themselves conform to the plan: a public mock test may assert only that nothing throws synchronously. The comments, though, claim a guard that does not exist for 10 RPCs.
  - **Mutation.** I made a throwaway worktree at b6f71c2c, with `rust/target` symlinked, and removed it afterwards. At four call sites in `NativeAdminClient` I replaced `ToNativeTimeoutMs(options.TimeoutMs)` with `(options.TimeoutMs ?? UnsetTimeoutMs)`. The four sites are DescribeCluster, DescribeConfigs, FenceProducers and AbortTransaction. A negative timeout then crosses as itself, so it means "client default" instead of Java's "already expired".
  - **Result.** The build had 0 warnings and 0 errors. The full net10.0 suite passed 2886/2886.
  - **The mutated lines are reached.** The public mock tests drive those RPCs with `-1` through `NativeAdminClient`. So the gap is "reached but not asserted", not "never run".
  - Before X2, each RPC's "rejects a negative" test pinned its own call site. After X2, about 30 of the 44 call sites depend only on routing through the shared helper. The routing is correct today: there are 44 helper calls, every `int timeoutMs = UnsetTimeoutMs;` site is followed by one, and nothing reads `.TimeoutMs` outside the helper. But a regression at any of those sites would silently change a Java "expired" call into a default-timeout wait, with no failing test. Meanwhile nine comments tell the next reader that a test covers it.
- Fix:
  - (a) **Required.** Reword the nine comments to say what is true. For a per-RPC test, for example: "The clamp itself is pinned by `PublicAdminNegativeTimeoutTests` (the helper table and the real-client differential). This RPC routes through `ToNativeTimeoutMs`, but no submit-seam test pins the value it sends." In the class summary, replace "the per-RPC submit-seam tests pin the value each RPC sends" with the 11 RPCs that are actually pinned. Alternatively, do (b) and keep the comments.
  - (b) **Recommended; this makes the comments true.** Add a negative row that expects `0` next to each existing seam timeout test:
    - `Assert.Equal(0, CaptureDescribe(new DescribeConfigsOptions { TimeoutMs = -1 }).TimeoutMs)` in `DescribeConfigs_TimeoutMapping`, and the same for `IncrementalAlterConfigs_TimeoutMapping`;
    - a `NegativeTimeout_IsSentAsZero` theory with `-1` and `int.MinValue` for `Rpc.DescribeCluster` and `Rpc.ListConfigResources` in `AdminP3SubmitArgumentTests`;
    - one `_NegativeTimeout_IsSentAsZero` fact per P8 RPC, next to `AbortTransaction_ZeroTimeout_IsNotTheDefaultSentinel`.

    The same cheap row can go into `AdminSubmitArgumentTests` (CreateTopics), the P6 CreateAcls/DeleteAcls tests and the P7 DescribeUserScramCredentials test. Those have no false comment, but they have the same gap.
- Test: if (b) is done, re-run the four-site mutation above. It must turn the new DescribeCluster, DescribeConfigs, FenceProducers and AbortTransaction rows red, and the unmutated suite must stay green. If only (a) is done, verify that `git grep -n 'pinned by the submit-seam' -- dotnet/tests` hits only tests for the 11 seam-pinned RPCs.

**Resolution (Actor 90, fixup `070985b8`, targets ec2ddd7a):** option (b). Negative-timeout seam rows
asserting a captured `0` for `TimeoutMs` `-1` and `int.MinValue` were added for all ten RPCs:
`AdminP3SubmitArgumentTests.X2_NegativeTimeout_IsSentAsZero` (DescribeCluster, ListConfigResources),
`AdminConfigsSubmitArgumentTests.X2_DescribeConfigs_NegativeTimeout_IsSentAsZero` /
`X2_IncrementalAlterConfigs_NegativeTimeout_IsSentAsZero`, and
`AdminP8SubmitArgumentTests.X2_<Rpc>_NegativeTimeout_IsSentAsZero` for the six P8 RPCs. The nine
per-RPC comments now name the seam test that pins their RPC, and the
`PublicAdminNegativeTimeoutTests` class summary states what that class pins (the helper table and,
through ListTopics, the real-client differential) and that a given RPC's sent value is pinned only
where its seam test has a negative row. Mutation, source snapshotted with `/bin/cp` and restored from
the snapshot: the four-site mutation (DescribeCluster, DescribeConfigs, FenceProducers,
AbortTransaction) turned exactly those RPCs' 8 new rows red (net10.0 2898/2906); the same mutation
at the other six RPCs' call sites turned exactly their 12 new rows red; restored source 2906/2906.
Out of scope, not done (per the Manager): rows for CreateTopics, CreateAcls/DeleteAcls and
DescribeUserScramCredentials.

## 90.2 The test-side twin of the remark G3-10 rewrote still carries the stale Java and Rust line cites — low
- Where: `dotnet/tests/Confluent.Kafka.UnitTests/Interop/AdminLogDirsLifetimeTests.cs`, in the `<remarks>` of the `describeReplicaLogDirs` omitted-key test (the "Reachable through the MOCK ONLY" paragraph). This is pre-existing text that the phase did not touch. It is the same paragraph that G3-10 fixed in `NativeAdminClient.DescribeReplicaLogDirs`'s remarks in d2f6f656.
- Anchor: PLAN §4.1 G3-10 ("replace the stale cites with symbol names, in .NET files only"), checked against `kafka/` 4.3.1 and `rust/src/admin/mock_admin_client.rs` at HEAD. Each cite in the paragraph points at the wrong code:
  - `KafkaAdminClient.java:3066-3068` is `describeLogDirs`' `LogDirDescription` construction. The `replicaDirInfoByPartition` seeding is `:3104-3107`.
  - `:3141-3145` is the mid-loop `put`. The completion loop is `:3154-3157`.
  - `mock_admin_client.rs:1352-1355` is now the `describe_replica_log_dirs_with_options` signature. The unknown-topic `continue` is `:1358-1361`.
  - The sibling cites `:1227-1229` and `:1290-1291` are the parameter lists of `describe_log_dirs_with_options` and `alter_replica_log_dirs_with_options`.
- Problem: S2b replaced these exact cites with symbol names in production. The Actor also widened G3-10 to one unlisted twin cite, `NativeAdminClient.ListPartitionReassignments`, on the "same stale cite" rationale. This test-side restatement still points readers at unrelated lines for the claim it makes: that the mock omits replicas of unknown topics and the real client seeds every key. The same stale-cite defect is therefore half-fixed.
- Fix: comment-only, mirroring d2f6f656.
  - Replace `(mock_admin_client.rs:1352-1355)` with `MockAdminClient::describe_replica_log_dirs_with_options`.
  - Replace `(:1227-1229)` / `(:1290-1291)` with `describe_log_dirs_with_options` / `alter_replica_log_dirs_with_options`.
  - Re-point the Java cites to `KafkaAdminClient.java:3104-3107` and `:3154-3157`.

  Alternatively, cite the same Java lines that `NativeAdminClient`'s remark uses (`:3103-3106` / `:3155-3160`), so the two copies agree.
- Test: none (comment-only). Verify that `git grep -nE '[a-z_]+\.rs:[0-9]' -- dotnet/src dotnet/tests` returns no `admin` Rust line cites.

**Resolution (Actor 90, fixup `ab3537f9`, targets d2f6f656):** comment-only. The three Rust line
cites became `MockAdminClient::describe_replica_log_dirs_with_options`,
`describe_log_dirs_with_options` and `alter_replica_log_dirs_with_options`. Verification against
`kafka/` 4.3.1 showed the production copy in `NativeAdminClient.DescribeReplicaLogDirs` was off by
one as well (`:3103-3106` excludes the seeding `put` at `:3107`; `:3155-3160` misses the loop header
at `:3154` and runs into `handleResponse`'s closing brace), so BOTH copies now cite
`KafkaAdminClient.java:3104-3107` (seeds `replicaDirInfoByPartition`) and `:3154-3157` (completes
every entry); the test copy's "seeds one future" became "seeds one entry" to match those lines.
The `git grep -nE '[a-z_]+\.rs:[0-9]' -- dotnet/src dotnet/tests` check still lists pre-existing
admin Rust line cites outside this paragraph (`mock_admin_client.rs`, `ffi/admin.rs`,
`config_resource.rs`, `log_dir_description.rs`, `topic_partition_replica.rs`); per the Manager's
"no other doc sweep" they were left untouched.

---

# Review record (the Critic 90 pass, its coverage record and the fixup re-check), archived from COMMENTS.90.md at close

# COMMENTS.90 — M15/P13.5 (Admin parity lows)

Single Critic pass over `21458241..b6f71c2c`: f308d137 (S1a), a69408ac (S1b), d5fdcc66 (S1c),
ec2ddd7a (S1d), 0b0327ae (S2a), d2f6f656 (S2b docs), b6f71c2c (S2b X8).
Verdict: **2 findings, both low** (90.1 test-claim/coverage, 90.2 stale doc cite). No
production defect; every PLAN item matches its Java anchor. The coverage record is at the end.

Both findings are resolved and moved to `COMMENTS.DONE.90.md`.

---

## Coverage record (what was checked, and how)

1. **Each item against its Java anchor.** No findings.
   - **S1a f308d137:**
     - G3-6 `NewPartitionReassignment` stores `List<int>(...).AsReadOnly()` behind an `IReadOnlyList<int>` (Java `List.copyOf`).
     - G5-2/G5-3 `OffsetAndMetadata` normalises a negative epoch to null and uses it in `Equals`/`GetHashCode` (`OffsetAndMetadata.java:98-116`). The admin alter path's has-epoch flag still maps absent to -1.
     - G1-13 `ListTopicsOptions` equality covers `ListInternal` only (`:67-77`).
     - G6-4 `FeatureUpdate.ToString` uses Java enum names.
   - **S1b a69408ac:**
     - The G1-10 name-collection forms forward via `TopicCollection.OfTopicNames` (`Admin.java:212,226,295,306`).
     - G3-7: the D3 options-only overload (`:1248-1249`).
     - G3-8: `LogDirDescription` has three ctors, with -1 mapped to null and live accessors (`LogDirDescription.java:38-50`). I probed it as a newly public ctor.
     - G5-6 `RemoveAll` is internal.
     - G4-5: `FilterResult.Exception`. All 5 crefs resolve in the doc XML.
   - **S1c d5fdcc66:**
     - `_isMock` is read only at the zero-feature guard and in `ObjectName`.
     - G4-3 does `continue` on a null user (`KafkaAdminClient.java:4354-4363`).
     - G6-9: the blank check covers null.
     - The header confirms that a mock with an empty map returns null.
   - **S2b d2f6f656.** Each claim was checked against `kafka/` 4.3.1, and G4-1 was also probed.
     - G4-1: I ran a 20-line console probe against the built DLL with a real client at `127.0.0.1:1`. The Unknown-ResourceType binding faulted alone, with code 42 "Invalid ACL creation: Resource type is UNKNOWN." The sibling binding timed out (code 7). This matches `KafkaAdminClient.java:2615-2621`.
     - G5-8: Java's `ListConsumerGroupOffsetsOptions` has only `requireStable`.
     - G7-7: `DescribeProducersOptions.java:29` is `brokerId(int)`.
     - G3-10: `Admin.java:1248-1249` is the `Optional.empty()` default. Both Rust symbols exist and do what is claimed.
     - No `Admin.java:1246`, `DescribeProducersOptions.java:44`, "valid by construction" or "topic partitions set on" text remains under `dotnet/src`.
   - **Declared deviations.**
     - 1 (null binds to the options-only overload, CS8625 rather than CS0121): re-derived from C# tie-breaking; correct.
     - 2 (reflection tests select by parameter types): fine.
     - 3 (real-client outcome comparisons): `G1_10`, `G3_7` and `ListConsumerGroupOffsets_TheSingleGroupForm_ForwardsItsOptions` all compare code 7 plus `Message` under a 30 s bound. A dropped option would wait out the default and fail the bound, so they are non-vacuous.
     - 4 and 8: out of scope (X10, the known flake).
     - 5: see item 2.
     - 6 and 7: see items 4 and 1.
2. **X2 sweep.**
   - `NativeAdminClient` has 45 `ToNativeTimeoutMs(` occurrences (44 calls plus the definition) and 0 `ValidateTimeoutMs`. A structural script found no `.TimeoutMs` read outside the helper and no RPC init site without a helper call.
   - `IAdmin` has 0 `TimeoutMs` "negative" lines. The other `ArgumentOutOfRangeException` rules are intact: `PartitionSizeLimitPerResponse` ×2, electionType, `IsolationLevel`, the ListGroups filters, and `Close(TimeSpan)`.
   - The canonical text is on `CreateTopicsOptions.TimeoutMs`, with 27 inheritdocs and 16 pointers. `DescribeTopicsOptions` keeps its `PartitionSizeLimitPerResponse` "must not be negative".
   - The converted tests assert either the seam value or `Assert.Null(Record.Exception(...))`, as the plan allows. The helper table and the real-client differential exist.
   - Gap: 90.1.
3. **Surface.**
   - Between base and HEAD no `dotnet/src` file was added or removed, and no type was declared or removed. The only type-line change is `OffsetAndMetadata : IEquatable<OffsetAndMetadata>`, with no operators.
   - G3-6 returns `IReadOnlyList<int>`, stored as `ReadOnlyCollection<int>`.
   - The S1a/S1b reflection tests match PLAN §3.
4. **X6.**
   - The `dead_pinvokes.py` copy at HEAD reports 359 declared, 0 `kafka_admin_*` referenced nowhere, and 8 referenced only by tests (kept per D6).
   - `static extern` counts: 359 in `NativeMethods.Admin.cs` plus 219 in `NativeMethods.cs` = 578. That equals `NativeMethodsPrelinkTests.ExpectedImportCount` and 653 − 75.
   - All 75 deleted C# names are unique. None is mentioned in `dotnet/src`, `dotnet/tests` or `grpc-server` at HEAD; the only mentions are in two `design/history` COMMENTS files. The three crefs became `<c>native_symbol</c>`.
   - Nothing in `src` or `grpc-server` resolves exports by name (no `GetExport` or `GetMethod("…")`).
   - The Prelink and marshalling tests passed in the full 2886/2886 run of the b6f71c2c-based worktree. The mutation touched only `NativeAdminClient` timeout arguments.
5. **Mode A (§6.1 items 1–3).**
   - `git diff --name-only 21458241 b6f71c2c` touches 72 files, all under `dotnet/`.
   - The header SHA-1 `af0f16448fd7ec653174907890f0e245a6b24738` equals the PM-verified base.
   - The extern count is 578, matching the constant.
   - **X8 b6f71c2c:** the `dotnet/CLAUDE.md` diff is exactly the two D2 passages, word for word (line wrapping aside).

---

## Fixup re-check (`b6f71c2c..ab3537f9`): 070985b8 (90.1), ab3537f9 (90.2)

**Verdict: re-check clean.** Both findings are resolved, and neither fixup introduced a defect. No new findings.

- **90.1, resolved by 070985b8 (option b).**
  - The ten new seam theories assert a captured `0` for `-1` and `int.MinValue`:
    - DescribeCluster and ListConfigResources, through `AdminP3SubmitArgumentTests.Capture`, which sets `options.TimeoutMs`;
    - DescribeConfigs, through `CaptureDescribe`, which calls `admin.DescribeConfigs`;
    - IncrementalAlterConfigs, through `CaptureAlter`, which calls `admin.IncrementalAlterConfigs`, not the old AlterConfigs path;
    - the six P8 RPCs, through their existing capture helpers. Their 500, 250, 750, 900, 600 and 400 rows already show that those helpers capture the timeout slot.
  - The nine comments name existing tests. The class summary now claims only the helper table, `X2_ToNativeTimeoutMs_IsJavasCalcDeadlineClamp`, and the ListTopics real-client differential.
  - At `ab3537f9`, `pinned by the submit-seam tests` remains in four places, all for RPCs that the P5 seam suite pins with negative rows: ListGroups, DescribeConsumerGroups, DescribeClassicGroups and ListConsumerGroupOffsets. `PublicAdminReassignmentsOffsetsTests:280` makes an unrelated null-vs-empty claim.
  - **Independent mutation.** I used a throwaway worktree at `ab3537f9`, with `rust/target` symlinked, and removed it afterwards. The main tree's HEAD and working tree are unchanged. Each run was the full net10.0 suite:
    - **HEAD control:** the `NegativeTimeout` filter passed 49/49.
    - **Four-site mutation** (DescribeCluster `:1749`, DescribeConfigs `:1893`, FenceProducers `:4809`, AbortTransaction `:5249`; `ToNativeTimeoutMs(options.TimeoutMs)` replaced by `(options.TimeoutMs ?? UnsetTimeoutMs)`): **2898/2906**. Exactly those four RPCs' 8 new rows failed.
    - **Six-site mutation, run separately** (`:1821`, `:2036`, `:4890`, `:4972`, `:5082`, `:5324`): **2894/2906**. Exactly the other six RPCs' 12 new rows failed.
    - **Restored source:** **2906/2906**.
  - Considered and not filed: the summary's sentence "for an RPC without such a row, this class does not pin the value it sends" is technically too broad for ListTopics. ListTopics has no seam row, but the same summary's real-client differential pins it. The error understates coverage rather than claiming a guard that does not exist, so nothing can regress unseen because of it.
- **90.2, resolved by ab3537f9.**
  - I checked both copies against `kafka/` 4.3.1 (`git describe` = `4.3.1`):
    - `KafkaAdminClient.java:3104-3107` declares `replicaDirInfoByPartition` and seeds one `ReplicaLogDirInfo` per requested `(topic, partition)` of that broker.
    - `:3154-3157` is the loop that completes every entry's future.
    - The production copy's old `:3103-3106` / `:3155-3160` were off by one, as the Actor found. **My own 90.2 "alternatively, agree with NativeAdminClient's cites" option would have propagated that off-by-one.** The Actor's correction of both copies is the right fix.
  - The three Rust symbols exist in `rust/src/admin/mock_admin_client.rs` and behave as the text claims:
    - `describe_replica_log_dirs_with_options` does `continue` on an unknown topic (`:1360-1362`);
    - `describe_log_dirs_with_options` inserts an entry per broker (`:1232-1234`);
    - `alter_replica_log_dirs_with_options` inserts a future per replica (`:1294-1296`).
  - "Seeds one entry" now matches the cited lines.
  - No other `dotnet/` or `grpc-server` copy of these Java cites remains. Only `design/history/M15/P3-*/PLAN.md:1917` keeps the old ones, and history is not edited.
- **Fixup targets.** Both subjects match their targets byte for byte (`ec2ddd7a`, `d2f6f656`). `d2f6f656` is the commit that introduced the `:3103-3106` / `:3155-3160` cites, so `ab3537f9` targets the right commit.
