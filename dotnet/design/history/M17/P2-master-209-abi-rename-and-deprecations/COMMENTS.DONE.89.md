# COMMENTS.DONE.89 — M17/P2 resolved findings

### C89-1 [Low] `DOTNET_GRPC_SKIPS` removal checklist omits the two consumer RPCs `c0220aab` added

**Category:** fix (the comment, now). Implementing the two RPCs is a follow-up for the
transaction-parity phase.

**Where:** `rust/Makefile:214-220` (the "REMOVE DOTNET_GRPC_SKIPS … when …" comment), together with
`dotnet/grpc-server/ConsumerServiceImpl.cs` and `dotnet/grpc-server/AsyncConsumerServiceImpl.cs`.

**Evidence:**
- `c0220aab` added `rpc GroupMetadata(ConsumerIdRequest)` and
  `rpc ReleaseGroupMetadata(ReleaseGroupMetadataRequest)` at
  `rust/multilanguage-test-server/proto/consumer_service.proto:103,107`.
- Neither .NET consumer servicer overrides them: `grep GroupMetadata dotnet/grpc-server/*.cs` returns
  nothing. The generated base method therefore answers `UNIMPLEMENTED`.
- The only caller is `MultilanguageConsumer::group_metadata`
  (`rust/tests/common/multilanguage_consumer.rs:463-472`), which does
  `.expect("group_metadata RPC failed")`.
- That caller is reached only from `consume_transform_produce_with_offsets_inner`
  (`rust/tests/integration/producer_transactions_test.rs:582`, call at `:642`). That function is
  the body of `test_consume_transform_produce_with_offsets` (`:723-725`), which is the third entry
  of `DOTNET_GRPC_SKIPS` (`rust/Makefile:223`).
- The removal condition at `rust/Makefile:214-220` lists:
  - the 10 transaction P/Invokes;
  - the IProducer/IAsyncProducer surface;
  - the MockProducer controls;
  - "the five RPCs in both producer servicers".

  It does not mention the consumer servicers.
- The library side already exists: `IConsumerCommon.GroupMetadata` and `ConsumerGroupMetadata`.
  Only the two servicer overrides are missing.

**Why it's wrong:** The comment is the record of when the skip can go, and the phase's own S3 log
calls this an "open item". It also says "Deleting it is the last step of that phase". Someone who
follows the checklist literally will remove the skips once the producer work is done. The
`__grpc_dotnet` and `__grpc_dotnet_async` arms of `test_consume_transform_produce_with_offsets`
will then panic on `UNIMPLEMENTED` from `GroupMetadata`, and the checklist will have called the
phase done. This phase's merge made the checklist incomplete: the RPCs arrived with `c0220aab`.
`rust/Makefile` is allowlisted .NET wiring, so the fix belongs here.

No coverage is lost today. The arms are skipped, and Python, C and Rust still run all three tests.
That is why this finding is Low.

**Suggested fix:** Extend the removal condition at `rust/Makefile:214-220`, for example:
"…and the five RPCs in both producer servicers, **plus the `GroupMetadata` /
`ReleaseGroupMetadata` ConsumerService RPCs in both consumer servicers (`ConsumerServiceImpl`,
`AsyncConsumerServiceImpl`), which `test_consume_transform_produce_with_offsets` reaches through
`Consumer::group_metadata()`**". The paragraph at `:197-205` ("none of them are the five
transaction RPCs…") can gain the same clause.

Keep the implementation of the two overrides as the tracked transaction-parity follow-up. It is
already recorded in the STATUS M17/P2 "Open item".

**Resolution:** 35f3aac41bbff6dfa4c213b024d28b0decc8eb60 — `rust/Makefile` removal checklist and skip rationale now name the `GroupMetadata` / `ReleaseGroupMetadata` RPCs in both consumer servicers; comment-only, `make -n` output unchanged.

### C89-2 [Low] Stale `bindings/dotnet/…` cites left in allowlisted .NET wiring outside `dotnet/`

**Category:** fix.

**Where:**
- Main case: `rust/tests/common/backend_pool.rs:114` and `:117`.
- Same class, the user's call: `design/current/python-binding-send-batching.md:436,452,456,457,459,460,597`.

**Evidence:**
- At HEAD, `backend_pool.rs:114` reads "`AdminService` (see `bindings/dotnet/Dockerfile.grpc`)",
  and `:117` reads "(see `bindings/dotnet/Dockerfile.grpc.async`)". Both files now live at
  `dotnet/Dockerfile.grpc{,.async}`, and `bindings/dotnet/` holds nothing tracked.
- These two lines are the branch's own .NET content. The branch delta of `backend_pool.rs`
  (`d6bf7c76..274523ec`) equals the phase delta vs `c0220aab` line for line, except for exactly
  these two lines.
- The merge carried them over verbatim, and the D9 sweep (`16df502c`) never touched files outside
  `dotnet/`.
- The S1b gate #10 grep is scoped `-- dotnet Makefile rust/Makefile .semaphore`, so it could not
  see `rust/tests/`.

**Why it's wrong:** D9(a) is meant to rewrite doc path prefixes in tracked .NET content so that no
cite points at the pre-#210 layout. These are doc comments in .NET harness wiring, and the
allowlist already carries this file as .NET wiring. They now name files that do not exist. They
are not functional: the image is resolved through `BackendKind::Dotnet`, not this path. Hence Low.

**Suggested fix:**
- In `rust/tests/common/backend_pool.rs:114,117`, change `bindings/dotnet/Dockerfile.grpc{,.async}`
  to `dotnet/Dockerfile.grpc{,.async}`. This is doc only, the file stays within the 16-path
  allowlist, and no Mode-B path is touched.
- `design/current/python-binding-send-batching.md` is pre-existing repo-root branch content, not
  .NET's tree. Its 7 `bindings/dotnet/…` cites are stale the same way. Rewrite them in the same
  commit only if the user wants that file kept current. Otherwise leave it.

**Resolution:** acb6e44b200056d68d102a26535b29e8f73d9028 — `rust/tests/common/backend_pool.rs:114,117` doc comments now cite `dotnet/Dockerfile.grpc{,.async}`; doc-comment only. `design/current/python-binding-send-batching.md` left unchanged per the Manager triage.

## S4 critic pass record

The working file `dotnet/COMMENTS.89.md` as it stood at close (critic 89's pass over `274523ec..391c3da0`, the Manager's triage, its rule-file suggestions and its coverage record, including the re-verification of both fixups). Archived here because the working file is reset to empty at close; only its title is dropped and its headings demoted one level.

Review range: `274523ec..391c3da06386b2f95d5ced6247c989a6e28c3c59` (first-parent, 21 commits).
References used: the C ABI header `rust/target/include/confluent_kafka.h` (SHA-1
`af0f16448fd7ec653174907890f0e245a6b24738`, which is `c0220aab`'s header), Java 4.3.1 (`kafka/`
@ `26b251a451`) and 4.4.0-rc3, and Python at `c0220aab` as the parity oracle.

### Findings

_The two findings, C89-1 and C89-2, were moved to the resolved entries at the top of this file._

### Manager triage (2026-10-01, project-manager) — what the Actor fixes

- **C89-1 — APPROVED.** Extend the `DOTNET_GRPC_SKIPS` removal condition in `rust/Makefile` (and the
  matching clause near the "none of them are the five transaction RPCs" paragraph, if present) to
  name the `GroupMetadata` / `ReleaseGroupMetadata` ConsumerService RPCs in both consumer servicers.
  Comment-only. Commit as `fixup!` of `ba536330` (the commit that put this checklist in `rust/Makefile`).
  Implementing the two overrides stays the transaction-parity follow-up (already in STATUS).
- **C89-2 — APPROVED for `rust/tests/common/backend_pool.rs:114,117` ONLY** (`bindings/dotnet/Dockerfile.grpc{,.async}`
  -> `dotnet/Dockerfile.grpc{,.async}`, doc-comment only). Commit as `fixup!` of `16df502c` (the D9(a) sweep
  that should have reached it). **NOT approved:** `design/current/python-binding-send-batching.md` — repo-root,
  non-.NET, pre-existing branch content absent from `c0220aab`; out of this phase's scope, left unchanged and
  recorded for the user.
- **Rule-file suggestions** — not actionable in this phase; recorded in the PLAN by the Manager.

### Rule-file suggestions (not actionable by the Actor)

PLAN §9 items 3, 4, 9, 10 and 12 already cover most rule-file drift. Each item below is either
new, or a correction to §9.

1. **`.claude/rules/consumer-threading.md:85`** is in the root rules: the branch's §1.1 .NET
   amendment, which survives the merge unchanged. It still cites "`bindings/CLAUDE.md §2`", and
   that path is now `dotnet/.claude/rules/bindings.md §2`. This line is **not** among §9.10's 10
   rule-file lines (8 `dotnet/CLAUDE.md`, 1 `ffi-marshalling.md`, 1 `dotnet-critic.md`). It is
   root-rule text, so the user decides.
2. **`dotnet/CLAUDE.md:523`** (the §4 Disposal row) still says "the timeout is ABI-backed only for
   the **consumer** (`Consumer_close_with_timeout`)" and "the timed *consumer* close is deferred,
   §1". §9.3 does not list this site. Separately, §9.3's own line numbers are now off by one,
   because `bf249e8f` inserted the link line:
   - `:53` → `:54`
   - `:596` → `:597`
   - `:348` → `:349`
   - `:374` → `:375`
   - `:321` → `:322`
3. **`dotnet/.claude/rules/ffi-marshalling.md:1386, 1452, 1477`** (§B2 path list and ownership
   table) still name `Consumer_close_with_timeout` as path 1. This is confirmed still stale and
   already §9.4. When it is rewritten, path 1 should read `Consumer_close` → `Consumer_destroy`,
   bounded by the core's default close timeout (30 s; previously a fixed 5 s). That is the
   behaviour `36fd0187` now documents in code.
4. **Signature check, not just presence (DoD §7 / persona).** The Prelink guard (`bbba6ed7`) proves
   each EntryPoint **resolves**, not that its **signature** matches. This S4 pass scripted a
   comparison of every C# `[DllImport]` against the header prototype: arity, scalar widths, `bool`
   I1, pointer/`out`/array pointee, return type, plus each `[UnmanagedFunctionPointer]` delegate
   against its callback typedef. It also diffed the header doc comments of every EntryPoint .NET
   uses between the old and new headers, to catch ownership changes hidden behind a rename.
   Suggestions:
   - add a "signature-vs-header" step to the .NET DoD gate, or to the Critic checklist, for any
     phase that changes the ABI;
   - note two parser pitfalls: cbindgen spells nullable callbacks as inline function pointers,
     whose parameter lists contain commas; and `static extern unsafe` modifier order.
5. **Gate pathspec (process, for the next layout move).** Scope a path-drift gate to the
   **whole Mode-A allowlist**, not to `dotnet` plus build files. That is how C89-2 slipped: gate
   #10 never looked at `rust/tests/common`.

### Coverage

1. **ABI signature fidelity — clean.**
   - Scripted comparison of all **653** `dotnet/src` `[DllImport]` declarations, plus the 3
     test-side ones, against `rust/target/include/confluent_kafka.h` (826 prototypes).
     - What was compared: EntryPoint presence, Cdecl, arity, every parameter's width/class
       (`int32_t`↔`int`, `int64_t`↔`long`, `int16_t`↔`short`, `size_t`↔`UIntPtr`,
       `bool`↔`[MarshalAs(I1)]`, `T*`/`T**`↔`IntPtr`/SafeHandle/`out IntPtr`/`T[]` pointee),
       the return type (`kafka_common_ErrorCode_t` is a C `int` enum ↔ `int`), and the
       SafeHandle↔opaque-type pairing (7 handle types, all consistent).
     - Result: 0 mismatches.
     - Three parser artifacts were checked by hand:
       - `ConsumerRebalanceListener_new`: 5 = 5; its inline `on_partitions_lost` fn-pointer
         contains a comma;
       - the 2 inline `user_data_destroy` (`void(*)(void*)` ↔ `CommitUserDataDestroyCallback`);
       - `Producer_send_batch`, declared `static extern unsafe` (5 = 5; the
         `ProducerRecordNative` layout matches `kafka_producer_ProducerRecord_t` field for field,
         unchanged since `274523ec`).
   - **59** `[UnmanagedFunctionPointer]` delegate↔typedef pairs were compared (arity, return,
     widths, Cdecl), plus `RebalanceListenerCallback` ↔ the two
     `ConsumerRebalanceListener_*_callback_t`. All match.
   - All 74 renames in `91cef6a5` are exact `func-renames.txt` pairs; each preserves the method
     suffix, so no cross-wiring.
   - Header doc comments of every .NET-used EntryPoint, old (`274523ec`) vs new with the rename
     and type map applied: the only differences are "CLAUDE.md §3→§4" renumbering, so there is no
     ownership or semantics change behind any rename.
2. **Removed API vs Java 4.3.1 — clean.** Every removed member is `@Deprecated` in 4.3.1:
   - `Admin.java:889/:901/:1823/:1835`;
   - `ConsumerGroupListing.java:32`, `ListConsumerGroupsOptions.java:33`,
     `ListConsumerGroupsResult.java:30`, `ClientMetricsResourceListing.java:21`,
     `ListClientMetricsResourcesOptions.java:24`, `ListClientMetricsResourcesResult.java:30`;
   - `ConsumerGroupDescription.java:189`, `ConsumerGroupState.java:30`;
   - `Consumer.java:282`.

   No Java-deprecated member is left exposed:
   - `ConsumerGroupMetadata` ctors are `internal` (Java `:37,:51` deprecated);
   - `ConsumerRecords` ctor is `internal` (4.4 deprecation);
   - `MemberDescription`'s public ctor mirrors Java's non-deprecated canonical one (`:39`);
   - there is no `MaxlifeTimeMs` / `OffsetResetStrategy`.

   Other checks:
   - No managed member is backed by any of the 15 removed EntryPoints (Prelink 653/653).
   - The remaining close surface matches D2/D3/D4: `IConsumer.Close()` and
     `IAsyncConsumer.Close(CancellationToken)`; admin `Close(TimeSpan)` is kept and is not
     deprecated in Java.
   - Python at `c0220aab` removed exactly the same public admin names. It keeps
     `close(timeout=None)` (D3's approved divergence).
3. **Sync-Dispose teardown — clean.**
   - `NativeConsumer.Dispose` keeps the order: `TryBeginClose` latch → raw-`IntPtr`
     `ConsumerClose` (exemption comment intact, no marshaller `ObjectDisposedException` path) →
     error consumed and not rethrown → `_handle.Dispose()` → `Consumer_destroy`. Only the
     EntryPoint changed.
   - The header declares `Consumer_close` as "with the default timeout". The 30 s bound is
     documented on `IConsumer.Close`, `KafkaConsumer` and `NativeConsumer.Dispose`.
   - `SafeConsumerHandle` / `ConsumerHandle` / `CloseSync` / `DisposeAsync` are unchanged in code,
     so paths 2–5 are unchanged.
   - The producer side (`NativeProducer`, `SendCompletionPump`, `DeliveryRegistration`,
     `KafkaProducer`, `MockProducer`) has comment-only diffs for `777dfa83..HEAD`.
   - The stale rule text is suggestion 3.
4. **S1 merge integrity / Mode A — clean.**
   - All 20 whole-file paths are `git diff --quiet c0220aab HEAD`.
   - The 2 orphans (`cbindgen.toml`, `src/admin/alter_consumer_group_offsets_result.rs`) are
     absent.
   - `git diff --name-only c0220aab HEAD -- . ':(exclude)dotnet'` = exactly the 16-path allowlist.
   - Mode-B paths (`rust/src`, `build.rs`, `cbindgen.toml`, `Cargo.*`, `xtask`, `generator`,
     `multilanguage-test-server`, `python/`, `c/`) are not in that list, so they equal `c0220aab`.
   - Six allowlisted paths have a delta byte-identical to the branch's pre-existing delta
     (`d6bf7c76..274523ec`): `consumer-threading.md`, `COMMENTS.DONE.50/51`,
     `python-binding-send-batching.md`, `install-dotnet.sh`, `dependencies-macos.sh`.
   - Five harness `.rs` files match the branch delta modulo `#[allow]`→`#[expect]`:
     `backend_factory`, `callback_log` and the three macros. `backend_pool` differs only in C89-2's
     two lines.
   - `admin_backend.rs` differs only in the 14 "four→five backends" doc lines.
   - `semaphore.yml` differs only in the recorded comment renumbering.
   - Root `Makefile` / `rust/Makefile` add only .NET targets and comments. Master's targets are
     unchanged apart from the pre-existing `test-integration-perf-dotnet` line.
   - `8964a732` is 5 `allow`→`expect` lines plus 1 removed `#[allow]`.
5. **S1b plumbing — clean, apart from C89-2 (doc only).**
   - `NativeLibraryPath` = `$(MSBuildProjectDirectory)/../../../rust/target/$(CargoProfileDir)/…`.
   - grpc `ProtoRoot` → `../../rust/multilanguage-test-server/proto`.
   - Both Dockerfiles COPY `rust/target/release/…` and `rust/multilanguage-test-server/proto` into
     `/src/rust/…`, and `/src/dotnet/…` mirrors the repo.
   - Soak `build.sh` runs `cd "$ROOT/rust" && cargo`, with `LIB_DIR=$ROOT/rust/target/$PROFILE`;
     `bootstrap.sh` uses `SRC_DIR=../..`.
   - Root `Makefile`: `ROOTS = REPO_ROOT=… RUST_PROJECT_ROOT=…`, and every .NET target delegates
     with `$(MAKE) -C dotnet $(ROOTS)`.
   - `dotnet/Makefile`: `REPO_ROOT ?= $(realpath ..)`, `RUST_PROJECT_ROOT ?= $(REPO_ROOT)/rust`,
     `GRPC_NATIVE_DIR = $(RUST_PROJECT_ROOT)/target/grpc-native`, which matches the harness's
     `CARGO_MANIFEST_DIR/target/grpc-native/dotnet`.
   - Neither Makefile has a non-comment `cargo` line; all cargo work goes through
     `$(MAKE) -C $(RUST_PROJECT_ROOT)`.
   - Functional grep (csproj/props/targets/Makefile/Dockerfile/sh/yml/json, `.semaphore/*`) for
     `bindings/` or a bare root `target/`: 0 hits from this phase. The only hits are master's own
     `publish-crates-io.yml` and comments.
   - Runtime path literals in C#: none.
6. **D14 / rule-file path-only — PASS.**
   - `27b61304` = 1 × R100 (`bindings/CLAUDE.md` → `dotnet/.claude/rules/bindings.md`).
   - `777dfa83` = 743 × R100.
   - `bf249e8f` touches exactly `dotnet/CLAUDE.md` (9/8), `ffi-marshalling.md` (1/1) and
     `dotnet-critic.md` (1/1). Its `--word-diff=porcelain` is exactly 10 `bindings/CLAUDE.md` →
     `dotnet/.claude/rules/bindings.md` token pairs plus one added link line, whose target resolves.
   - No other first-parent phase commit touches `dotnet/CLAUDE.md`, `dotnet/.claude/{rules,agents}/**`,
     root `CLAUDE.md` or `.claude/{rules,agents}/**`, apart from:
     - the merge `b4f019d4` bringing master's content: `CLAUDE.md`, `admin-client.md`,
       `producer-transactions.md` and `actor-executor.md` now equal `c0220aab`;
     - `consumer-threading.md`, which equals `c0220aab` plus the pre-existing branch amendment
       (delta byte-identical).
7. **D9 sweep — clean.**
   - Classified the 323 token rewrites: `src/`→`rust/src/` 141, `bindings/dotnet/`→`dotnet/` 80,
     `tests/`→`rust/tests/` 25, `bindings/python/`→`python/` 24, `target/`→`rust/target/` 15,
     other 38 (`bindings/CLAUDE.md`, root §-renumbers, `xtask`/`cbindgen` prefixes).
   - Checked every rewritten path against the HEAD tree: 275 exist. The 21 non-existent are
     untracked build artifacts (`rust/target/…`), an untracked COMMENTS file, and one
     `~/confluent-kafka-rust/dotnet/soak` false match.
   - No binding-own `src/` was mapped to `rust/src/` (no `rust/src/Confluent…`).
   - All C# hunks in `16df502c` and `52c10a6f` are comment lines (0 non-comment lines).
   - `dotnet/design/history/**`: 0 changes after `777dfa83`.
8. **Tests — clean.**
   - Prelink reflects over `typeof(NativeMethods).Assembly` (the production declarations, DoD §12),
     with an exact 653 count as the zero-match guard.
   - D7 asserts exact messages and codes. Those messages equal Java's
     `Topic.validate`/`validateGroupInstanceId` text and `ConsumerConfig.java:727-732`.
   - R6 audit: 3 sites at a 30 s deadline, documented; no deadline changed.
   - I1 sweep fix: the prefix is restored and the two bool returns are pinned. My signature script
     independently confirms every bool return/param in all 653 has I1.
   - Deleted tests: about 15 spot-checked across `AdminP3*`/`AdminP5*` rows, `GroupMarshalConsumerState*`,
     `PublicAdmin{ListConsumerGroups*,ConsumerGroupListing,P3ShapeParity,ClusterConfigResources}`
     and `PublicSyncConsumer{Precondition,Teardown}`. Each exercised only a removed member.
   - Shared helpers keep coverage: `KeyedResultMarshal.CompleteTwoLists` via
     `TwoLists_EachWalkItsOwnCount`/`All_*`; `ListConfigResources(ClientMetrics)` via
     `PublicAdminConfigsTests:181`.
9. **`GroupMetadata` / `ReleaseGroupMetadata` — judged in C89-1.** Not a defect of this phase's
   code: the arms are skipped and no coverage is lost. The comment fix belongs in this phase,
   because the merge made the removal checklist incomplete. The servicer overrides stay a tracked
   transaction-parity follow-up.
10. **STATUS accuracy — clean.**
    - All 38 SHAs cited in the M17/P2 entry are ancestors of HEAD, and each phase SHA is cited
      against its own stage.
    - The arithmetic holds:
      - 668 = 653 + 15, and 89 = 74 + 15;
      - 2927 − 74 − 15 − 7 = 2831, and 2831 + 2 + 13 = 2846;
      - 151 = 115 + 36, and 145 = 112 + 33;
      - 16 non-dotnet paths;
      - 26 = 20 + 2 + 4.
11. **Fix re-verification (35f3aac4, acb6e44b): clean.**
    - Each fixup touches only its one file, and only comment/doc lines:
      - `rust/Makefile`, 11 added / 4 removed;
      - `backend_pool.rs`, 2 / 2.
    - The C89-1 text matches the source:
      - `consumer_service.proto:103,107` declares GroupMetadata and ReleaseGroupMetadata;
      - `dotnet/grpc-server` overrides neither (0 hits);
      - `producer_transactions_test.rs:642` reaches them via `Consumer::group_metadata()`, and
        `:723` declares `test_consume_transform_produce_with_offsets`.
    - `make -C rust -n test-integration-grpc-dotnet test-integration-grpc-dotnet-native` prints
      output byte-identical to the Makefile at 391c3da0.
    - `backend_pool.rs:114,117` now cite `dotnet/Dockerfile.grpc{,.async}`, which both exist; no
      `bindings/` path is left.
    - The non-dotnet diff vs c0220aab is the same 16 paths, with 0 Mode-B paths.
    - No new finding.

## Deleted tests (DoD #3), moved from STATUS at close

The TRX-reconciled list of the 96 executed test cases per TFM deleted by S2b–S2d, with the renamed tests. It was first recorded in the STATUS M17/P2 entry (`8a6bc75b`) and moved here when that entry was condensed at close.

```
M17/P2 S2b–S2d — deleted unit tests (dotnet/tests/Confluent.Kafka.UnitTests)
Baseline: HEAD 52c10a6f, 2927 executed per TFM (net8.0 / net10.0). Counts are TRX-reconciled per TFM.

== S2b: 53 [Fact] + 21 [Theory] data rows = 74 executed test cases deleted
   Reason (every entry): Java-deprecated API, removed in #209 / root CLAUDE.md §3
   [Theory] Confluent.Kafka.UnitTests.Interop.AdminKeySeamShapeTests.EveryReader_IsAHoistedStaticReadonlyField  -- 1 data row(s) deleted (1 of 6 rows):
              (fieldName: "ClientMetricsResourceListingValue")
   [Theory] Confluent.Kafka.UnitTests.Interop.AdminP3OperationLifetimeTests.DisposeRacingAnInFlightOperation_DefersTheNativeDestroy  -- 1 data row(s) deleted (1 of 3 rows):
              (rpc: ListClientMetricsResources)
   [Theory] Confluent.Kafka.UnitTests.Interop.AdminP3OperationLifetimeTests.EveryTrampoline_IsATotalNoThrowBoundary  -- 1 data row(s) deleted (1 of 3 rows):
              (rpc: ListClientMetricsResources)
   [Theory] Confluent.Kafka.UnitTests.Interop.AdminP3OperationLifetimeTests.InlineCallback_FreesTheGcHandleExactlyOnce  -- 1 data row(s) deleted (1 of 3 rows):
              (rpc: ListClientMetricsResources)
   [Theory] Confluent.Kafka.UnitTests.Interop.AdminP3OperationLifetimeTests.SubmitThatThrows_AbandonsTheOperationAndLeavesTheHandleReleasable  -- 1 data row(s) deleted (1 of 3 rows):
              (rpc: ListClientMetricsResources)
   [Theory] Confluent.Kafka.UnitTests.Interop.AdminP3OperationLifetimeTests.TheCallbacksErrorParameter_IsOwned_AndConsumedExactlyOnce  -- 1 data row(s) deleted (1 of 3 rows):
              (rpc: ListClientMetricsResources)
   [Fact]   Confluent.Kafka.UnitTests.Interop.AdminP3ResultMarshalTests.ListClientMetricsResources_WalksToAnEmptyCollection_OnAFreshMock
   [Theory] Confluent.Kafka.UnitTests.Interop.AdminP3SubmitArgumentTests.ExplicitTimeout_IsForwardedVerbatim  -- 2 data row(s) deleted (2 of 6 rows):
              (rpc: ListClientMetricsResources, timeoutMs: 0)
              (rpc: ListClientMetricsResources, timeoutMs: 34567)
   [Theory] Confluent.Kafka.UnitTests.Interop.AdminP3SubmitArgumentTests.NullTimeout_MapsToANegative_NotZero  -- 1 data row(s) deleted (1 of 3 rows):
              (rpc: ListClientMetricsResources)
   [Fact]   Confluent.Kafka.UnitTests.Interop.AdminP5ResultMarshalTests.ConsumerAll_ThrowsTheFirstError_WhileValidStillYieldsThePartialResults
   [Fact]   Confluent.Kafka.UnitTests.Interop.AdminP5ResultMarshalTests.ConsumerEachCountAccessor_IsAskedForItsOwnAxis
   [Theory] Confluent.Kafka.UnitTests.Interop.AdminP5ResultMarshalTests.ConsumerTwoLists_EachWalkItsOwnCount  -- 6 data row(s) deleted (whole Theory):
              (validCount: 0, errorCount: 0)
              (validCount: 0, errorCount: 2)
              (validCount: 1, errorCount: 3)
              (validCount: 2, errorCount: 0)
              (validCount: 2, errorCount: 2)
              (validCount: 3, errorCount: 1)
   [Fact]   Confluent.Kafka.UnitTests.Interop.AdminP5ResultMarshalTests.ConsumerWalk_CarriesEveryListingField
   [Fact]   Confluent.Kafka.UnitTests.Interop.AdminP5SubmitArgumentTests.Consumer_BothFilters_ReachTheSubmit_EachWithItsOwnArrayAndCount
   [Fact]   Confluent.Kafka.UnitTests.Interop.AdminP5SubmitArgumentTests.Consumer_EmptyOptions_SendTheSameTwoEmptyAxes
   [Theory] Confluent.Kafka.UnitTests.Interop.AdminP5SubmitArgumentTests.Consumer_ExplicitTimeout_IsForwardedVerbatim  -- 2 data row(s) deleted (whole Theory):
              (timeoutMs: 0)
              (timeoutMs: 45678)
   [Fact]   Confluent.Kafka.UnitTests.Interop.AdminP5SubmitArgumentTests.Consumer_NegativeTimeout_IsRejectedBeforeAnythingIsSubmitted
   [Fact]   Confluent.Kafka.UnitTests.Interop.AdminP5SubmitArgumentTests.Consumer_NullOptions_SendTwoEmptyAxes
   [Fact]   Confluent.Kafka.UnitTests.Interop.AdminP5SubmitArgumentTests.Consumer_NullTimeout_MapsToANegative_NotZero
   [Theory] Confluent.Kafka.UnitTests.Interop.AdminP5SubmitArgumentTests.Consumer_OneAxisSet_LeavesTheOtherEmpty  -- 2 data row(s) deleted (whole Theory):
              (axis: GroupStates)
              (axis: Types)
   [Fact]   Confluent.Kafka.UnitTests.Interop.AdminP5SubmitArgumentTests.Consumer_TheTwoStateSpellings_AreOneFilter_AndOnlyOneReachesTheSubmit
   [Theory] Confluent.Kafka.UnitTests.Interop.AdminP5SubmitArgumentTests.Consumer_UndefinedEnumValue_IsRejectedBeforeAnythingIsSubmitted  -- 2 data row(s) deleted (whole Theory):
              (onTheStateAxis: False)
              (onTheStateAxis: True)
   [Fact]   Confluent.Kafka.UnitTests.PublicAdminClusterConfigResourcesTests.ListClientMetricsResources_IsEmptyOnAFreshMock
   [Fact]   Confluent.Kafka.UnitTests.PublicAdminConsumerGroupListingTests.Accessors_AreJavasFive_AsProperties
   [Fact]   Confluent.Kafka.UnitTests.PublicAdminConsumerGroupListingTests.CanonicalConstructor_ReadsBackEveryAccessor
   [Fact]   Confluent.Kafka.UnitTests.PublicAdminConsumerGroupListingTests.Constructors_AreJavasThreeCurrentOnes
   [Fact]   Confluent.Kafka.UnitTests.PublicAdminConsumerGroupListingTests.Deprecation_IsCarriedAcross_AtWarningSeverity
   [Fact]   Confluent.Kafka.UnitTests.PublicAdminConsumerGroupListingTests.Equality_CoversTheFourStoredMembers
   [Fact]   Confluent.Kafka.UnitTests.PublicAdminConsumerGroupListingTests.NullGroupId_IsRejectedByEveryConstructor
   [Fact]   Confluent.Kafka.UnitTests.PublicAdminConsumerGroupListingTests.ShorterConstructors_LeaveTheOptionalsAbsent
   [Fact]   Confluent.Kafka.UnitTests.PublicAdminConsumerGroupListingTests.TheTwoStateAxes_AgreeByName_ExceptWhereConsumerGroupStateCannotFollow
   [Fact]   Confluent.Kafka.UnitTests.PublicAdminConsumerGroupListingTests.ToString_MirrorsJavasRendering
   [Fact]   Confluent.Kafka.UnitTests.PublicAdminListConsumerGroupsOptionsTests.AnUndefinedConsumerGroupStateValue_ThrowsWhenWrittenThroughStates
   [Fact]   Confluent.Kafka.UnitTests.PublicAdminListConsumerGroupsOptionsTests.AnUndefinedGroupStateValue_ProjectsToUnknown_WhenReadThroughStates
   [Fact]   Confluent.Kafka.UnitTests.PublicAdminListConsumerGroupsOptionsTests.Defaults_MatchJavasFieldInitializers
   [Fact]   Confluent.Kafka.UnitTests.PublicAdminListConsumerGroupsOptionsTests.Filters_NormalizeNullAndEmptyToTheEmptySet
   [Fact]   Confluent.Kafka.UnitTests.PublicAdminListConsumerGroupsOptionsTests.Filters_StoreADeduplicatedDefensiveCopy
   [Fact]   Confluent.Kafka.UnitTests.PublicAdminListConsumerGroupsOptionsTests.Filters_StoreAnImmutableCopy
   [Fact]   Confluent.Kafka.UnitTests.PublicAdminListConsumerGroupsOptionsTests.PublicShape_IsTheTimeoutPlusJavasThreeAccessors
   [Fact]   Confluent.Kafka.UnitTests.PublicAdminListConsumerGroupsOptionsTests.ReadingTheDeprecatedAxis_ProjectsNotReadyToUnknownAndDeduplicates
   [Fact]   Confluent.Kafka.UnitTests.PublicAdminListConsumerGroupsOptionsTests.TheDeprecations_AreMirroredAsWarnings
   [Fact]   Confluent.Kafka.UnitTests.PublicAdminListConsumerGroupsOptionsTests.TheTwoStateAxes_AreOneFilterViewedThroughTwoEnums
   [Fact]   Confluent.Kafka.UnitTests.PublicAdminListConsumerGroupsOptionsTests.TimeoutMs_IsNullableAndRoundTrips
   [Fact]   Confluent.Kafka.UnitTests.PublicAdminListConsumerGroupsOptionsTests.WritingTheDeprecatedAxis_MapsEveryMemberOntoItsNamesake
   [Fact]   Confluent.Kafka.UnitTests.PublicAdminListConsumerGroupsResultTests.All_FaultsWithTheFirstError_WhenAnyErrorOccurred
   [Fact]   Confluent.Kafka.UnitTests.PublicAdminListConsumerGroupsResultTests.All_YieldsEveryListing_WhenNoErrorOccurred
   [Fact]   Confluent.Kafka.UnitTests.PublicAdminListConsumerGroupsResultTests.Empty_SucceedsOnAllThreeAccessors
   [Fact]   Confluent.Kafka.UnitTests.PublicAdminListConsumerGroupsResultTests.Errors_YieldsEveryError_AndNeverFaults
   [Fact]   Confluent.Kafka.UnitTests.PublicAdminListConsumerGroupsResultTests.EveryAccessor_ReadsTheOneSharedSource
   [Fact]   Confluent.Kafka.UnitTests.PublicAdminListConsumerGroupsResultTests.PublicShape_IsJavasThreeAccessors_AsMethods
   [Fact]   Confluent.Kafka.UnitTests.PublicAdminListConsumerGroupsResultTests.TheDeprecation_IsMirroredAsAWarning
   [Fact]   Confluent.Kafka.UnitTests.PublicAdminListConsumerGroupsResultTests.TheTwoLists_HaveIndependentLengths
   [Fact]   Confluent.Kafka.UnitTests.PublicAdminListConsumerGroupsResultTests.Valid_YieldsThePartialResults_AndIgnoresErrors
   [Fact]   Confluent.Kafka.UnitTests.PublicAdminListConsumerGroupsTests.ListConsumerGroups_AfterDispose_Throws
   [Fact]   Confluent.Kafka.UnitTests.PublicAdminListConsumerGroupsTests.ListConsumerGroups_AFullyFilteredCall_CompletesNormally
   [Fact]   Confluent.Kafka.UnitTests.PublicAdminListConsumerGroupsTests.ListConsumerGroups_AllAgreesWithValid_WhenNothingFailed
   [Fact]   Confluent.Kafka.UnitTests.PublicAdminListConsumerGroupsTests.ListConsumerGroups_ANegativeTimeout_ThrowsRightAway
   [Fact]   Confluent.Kafka.UnitTests.PublicAdminListConsumerGroupsTests.ListConsumerGroups_AnUndefinedEnumValue_ThrowsRightAway
   [Fact]   Confluent.Kafka.UnitTests.PublicAdminListConsumerGroupsTests.ListConsumerGroups_IsReachableThroughTheInterface
   [Fact]   Confluent.Kafka.UnitTests.PublicAdminListConsumerGroupsTests.ListConsumerGroups_NoFilter_IsTheSameCallHoweverItIsSpelled
   [Fact]   Confluent.Kafka.UnitTests.PublicAdminListConsumerGroupsTests.ListConsumerGroups_ReportsEverySeededGroup
   [Fact]   Confluent.Kafka.UnitTests.PublicAdminListConsumerGroupsTests.ListConsumerGroups_TheDeprecatedStatesFilter_IsCallableEndToEnd
   [Fact]   Confluent.Kafka.UnitTests.PublicAdminListConsumerGroupsTests.ListConsumerGroups_WithNoGroups_YieldsTwoEmptyCollections
   [Fact]   Confluent.Kafka.UnitTests.PublicAdminP3ShapeParityTests.ClientMetricsResourceListing_MirrorsJavasShape
   [Fact]   Confluent.Kafka.UnitTests.PublicAdminP3ShapeParityTests.TheDeprecatedClientMetricsSurface_IsMarkedObsolete

   Renamed (not deleted; executed count unchanged) — the old name stated a count the removal falsified:
     Interop.AdminP3OperationLifetimeTests.ManyOperationsOfAllThreeKinds_LeaveTheReferenceCountBalanced  ->  Interop.AdminP3OperationLifetimeTests.ManyOperationsOfEveryKind_LeaveTheReferenceCountBalanced
     PublicAdminClusterConfigResourcesTests.TfmSmoke_TheThreeNewRpcsWorkOnThisFramework  ->  PublicAdminClusterConfigResourcesTests.TfmSmoke_TheNewRpcsWorkOnThisFramework
     PublicAdminClusterConfigResourcesTests.TheThreeRpcs_AreReachableThroughIAdmin  ->  PublicAdminClusterConfigResourcesTests.TheRpcs_AreReachableThroughIAdmin
     PublicAdminConfigsTests.ListClientMetricsResources_ReturnsASeededResource_AndListConfigResourcesAgrees  ->  PublicAdminConfigsTests.ListConfigResources_ReportsASeededClientMetricsResource
     PublicAdminP3ShapeParityTests.IAdmin_TheThreeNewRpcsAreSynchronous_WithTheJavaParameterShape  ->  PublicAdminP3ShapeParityTests.IAdmin_TheNewRpcsAreSynchronous_WithTheJavaParameterShape
     PublicAdminP3ShapeParityTests.TheTwoListResults_PublishExactlyJavasSingleAccessor  ->  PublicAdminP3ShapeParityTests.TheListResult_PublishesExactlyJavasSingleAccessor

== S2c: 7 [Fact] + 8 [Theory] data rows = 15 executed test cases deleted
   Reason (every entry): Java-deprecated API, removed in #209 / root CLAUDE.md §3
   [Theory] Confluent.Kafka.UnitTests.Interop.GroupMarshalConsumerStateTests.AName_DecodesRegardlessOfCasing  -- 4 data row(s) deleted (whole Theory):
              (name: "PREPARINGREBALANCE")
              (name: "PrEpArInGrEbAlAnCe")
              (name: "PreparingRebalance")
              (name: "preparingrebalance")
   [Fact]   Confluent.Kafka.UnitTests.Interop.GroupMarshalConsumerStateTests.ANullName_DecodesToAbsenceNotUnknown
   [Fact]   Confluent.Kafka.UnitTests.Interop.GroupMarshalConsumerStateTests.AnUndefinedValue_EncodesToNull
   [Theory] Confluent.Kafka.UnitTests.Interop.GroupMarshalConsumerStateTests.AnUnrecognisedName_DecodesToUnknown  -- 4 data row(s) deleted (whole Theory):
              (name: "")
              (name: "NotReady")
              (name: "SomeStateFromANewerBroker")
              (name: "Stable ")
   [Fact]   Confluent.Kafka.UnitTests.Interop.GroupMarshalConsumerStateTests.EveryJavaConstant_RoundTripsThroughNameAndBack
   [Fact]   Confluent.Kafka.UnitTests.Interop.GroupMarshalConsumerStateTests.TheEnum_DeclaresExactlyJavasEightConstantsInOrder
   [Fact]   Confluent.Kafka.UnitTests.Interop.GroupMarshalConsumerStateTests.TheEnum_IsMarkedObsoleteAsAWarning
   [Fact]   Confluent.Kafka.UnitTests.PublicAdminConsumerGroupDescriptionTests.State_IsAProjectionOfGroupState_NotAnIndependentMember
   [Fact]   Confluent.Kafka.UnitTests.PublicAdminConsumerGroupDescriptionTests.State_LosesNotReady_SoItCannotBeRunBackwards

   Renamed / re-scoped (not deleted; executed count unchanged) — the old name or body pinned the removed State member:
     Interop.AdminP5ResultMarshalTests.Describe_State_IsAProjectionOfTheGroupStateTheAbiNamed  ->  Interop.AdminP5ResultMarshalTests.Describe_GroupState_IsTheGroupStateTheAbiNamed  (keeps the every-GroupState-through-the-walk coverage; asserts State is absent)
     PublicAdminClassicGroupDescriptionTests.State_IsStoredAndCurrent_UnlikeTheSiblingsProjection  ->  PublicAdminClassicGroupDescriptionTests.State_IsStoredAndCurrent_NotAProjection  (sibling assertion flipped: ConsumerGroupDescription has no State)
     PublicAdminConsumerGroupDescriptionTests.State_IsObsolete_AndGroupStateAndTheClassAreNot  ->  PublicAdminConsumerGroupDescriptionTests.State_IsNotTranslated_AndGroupStateAndTheClassAreNotObsolete


== S2d: 7 [Fact] + 0 [Theory] data rows = 7 executed test cases deleted
   Reason (every entry): Java-deprecated API, removed in #209 / root CLAUDE.md §3
   [Fact]   Confluent.Kafka.UnitTests.PublicSyncConsumerTeardownTests.CloseWithTimeout_ReturnsWithoutHang
   [Fact]   Confluent.Kafka.UnitTests.PublicSyncConsumerTeardownTests.CloseWithTimeout_Zero_ReturnsWithoutHang
   [Fact]   Confluent.Kafka.UnitTests.PublicSyncConsumerTeardownTests.CloseWithTimeout_ThenClose_IsIdempotent
   [Fact]   Confluent.Kafka.UnitTests.PublicSyncConsumerPreconditionTests.Close_NegativeTimeout_ThrowsArgumentOutOfRange
   [Fact]   Confluent.Kafka.UnitTests.PublicSyncConsumerPreconditionTests.Close_NegativeTimeout_ThrownBeforeNativeCall_EvenWhenClosed
   [Fact]   Confluent.Kafka.UnitTests.PublicSyncConsumerPreconditionTests.Close_ZeroTimeout_IsValid
   [Fact]   Confluent.Kafka.UnitTests.PublicSyncConsumerPreconditionTests.Close_NegativeTimeout_LeavesTheConsumerIntactAndStillClosable
```
**Unit-test reconciliation, per TFM (net8.0 and net10.0).** 2927 at the M17/P1 close and through S2a; − 74 (S2b), − 15 (S2c), − 7 (S2d) = 2831; + 2 (Prelink) + 13 (D7) = **2846**.
