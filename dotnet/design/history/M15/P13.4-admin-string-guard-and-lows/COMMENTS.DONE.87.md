# Critic 87 — resolved findings (M15/P13.4)

Moved from `COMMENTS.87.md` by Actor 87 after the fixup was committed and verified.

---

### 87.1 G1-3's stale cite and "rather than honoured" survive in the internal twin of the fixed doc — low
- Where: `bindings/dotnet/src/Confluent.Kafka/Internal/NativeAdminClient.cs:1288-1290` (`DescribeTopics(TopicCollection, DescribeTopicsOptions?, …)`, the `PartitionSizeLimitPerResponse < 0` guard); commit 81ba43c2 fixed the public twin (`Admin/DescribeTopicsOptions.cs`) and left this one.
- Anchor: plan §4.1 (G1-3), which names exactly these two defects: "Replace the `src/ffi/admin.rs:3712` cite with the symbol `describe_topics_options`" and "Drop … 'rather than honoured', because nothing is honoured". `src/ffi/admin.rs:3712` is today a blank line between `deleteTopics` and `submit_describe_topics_by_names_entries`; the helper is `describe_topics_options` at `:4217`. Java sends the value as `ResponsePartitionLimit` (`KafkaAdminClient.java:2239`), the core sends no DescribeTopicPartitions at all (plan §2 G1-3 row / §7 G1-2).
- Problem: the comment reads "The ABI reads a negative as 'keep Java's 2000 default' (src/ffi/admin.rs:3712), so a negative would be silently reinterpreted rather than honoured". The line cite points at unrelated code, and "rather than honoured" asserts the non-negative value *is* honoured, which the G1-3 fix just documented as false ("It currently has no effect on any request"). The two sentences G1-3 corrected in the public doc now contradict their own internal restatement one call below — the same defect, in the same guard, left half-fixed.
- Fix: in that comment, replace `(src/ffi/admin.rs:3712)` with the symbol (`describe_topics_options` in `src/ffi/admin.rs`), and drop "rather than honoured" (e.g. "so a negative would be silently reinterpreted — the same reasoning as the timeout guard; see `DescribeTopicsOptions.PartitionSizeLimitPerResponse` for why the value currently has no effect"). Comment-only; no behaviour change. Optionally align the test comment `tests/Confluent.Kafka.UnitTests/PublicAdminDeleteDescribeTopicsTests.cs:318` ("Zero is passed through untouched, as Java would"), the phrase §4.1 dropped from the public doc — it is still true of the binding's guard, so this part is cosmetic.
- Test: none (comment-only). Verify with `/usr/bin/grep -rn '3712\|rather than honoured' --include='*.cs' bindings/dotnet/src` — the only remaining "rather than honoured" hits must be the four `TimeoutMs` `<exception>` docs in `Admin/IAdmin.cs` (`:106`, `:153`, `:213`, `:305`), which are correct (a timeout *is* honoured).
- Resolution: fixed in 5fa2493f (`fixup! docs(dotnet): G1-3, G1-8, G2-6 (M15/P13.4 S2)`). In the `PartitionSizeLimitPerResponse < 0` guard of `NativeAdminClient.DescribeTopics(TopicCollection, ...)`, the `(src/ffi/admin.rs:3712)` line cite is replaced by the symbol `describe_topics_options` in `src/ffi/admin.rs`, and "rather than honoured" is dropped: the comment now says a negative would be silently reinterpreted as the 2000 default (the same reasoning as the timeout guard) and points at `DescribeTopicsOptions.PartitionSizeLimitPerResponse` for why the value currently has no effect on any request. Comment-only; no behaviour or test change. The optional test comment at `PublicAdminDeleteDescribeTopicsTests.cs:318` is left unchanged (cosmetic, still true of the binding's guard). Verify grep: the only remaining `3712` / `rather than honoured` hits under `bindings/dotnet/src` are the four `TimeoutMs` `<exception>` docs in `Admin/IAdmin.cs` (`:106`, `:153`, `:213`, `:305`).

---

## Review record (Manager, at phase close — 2026-09-30)

Per D9, Actor 87 ran S1 then S2 with no user gate between them, the Manager verified each
sub-stage's gates, and Critic 87 reviewed `d24fe14e..a176b86f` **once**. G1-5 was held after
S2 for a user ruling (D8 widened from 13 to 20 files), then committed before the review.

| Scope | Commit(s) | Critic 87 | Findings |
|---|---|---|---|
| Plan (approved) | `d24fe14e` | review: no findings | none |
| S1 — Group A guard (G2-1, G3-3, G4-6, G7-1, X12) | `3f5e42b7`, `293caf17` | review: no findings | none |
| S2 — G1-6/G1-7, G1-9 | `8d3cceda` | review: no findings | none |
| S2 — G2-4 | `c5d06350` | review: no findings | none |
| S2 — G2-5 | `f521f636` | review: no findings | none |
| S2 — G2-8 | `d4902bc7` | review: no findings | none |
| S2 — docs G1-3, G1-8, G2-6 | `81ba43c2` + `fixup!` `5fa2493f` | review: 87.1 (low); re-check: CLEAN | 87.1 fixed in `5fa2493f`, comment-only |
| D8 ruling recorded in the plan | `5fc6a5fe` | review: no findings | none |
| S2 — G1-5 (20 result types) | `a176b86f` | review: no findings | none |

87.1 touched no ruled decision (D1–D9).

Final gates on `5fa2493f`:

- **Mode A.** `git diff d24fe14e..5fa2493f -- src/ cbindgen.toml generator/ build.rs Cargo.toml Cargo.lock tests/ bindings/python bindings/c`
  is empty. The header SHA-1 is `41f48ea8…`. `internal static extern` is 697 throughout.
- **Tests.** 2630 on base `d24fe14e`, 2910 after S1, 2927 at close, on both net10.0 and
  net8.0. One net8.0 run after the fixup had a single failure that was not identified; the
  next seven net8.0 runs passed 2927/2927.
- **Build and format.** 0 warnings and 0 errors on all six outputs; `dotnet format
  --verify-no-changes` is clean; grpc-server builds.
- **Local Docker gate, run on `5fa2493f` (2026-09-30, 18:55–19:08 IST).**
  - The master merge `31078aac` changed the core after P13.3's gate, so a new linux/amd64
    `.so` was built at HEAD. It contains the merge's `client.dns.lookup` strings, which the
    P13.3 `.so` (sha256 `9b35e4bc…`) lacks.
  - All 697 of the binding's entry points resolve in it.
  - The sync .NET gRPC image was rebuilt from it and carries its sha256, `4cc23659…`.
  - The `__grpc_dotnet` arms of all 13 admin families (79) plus the one
    `multilanguage_admin_test` arm: **80 passed; 0 failed**.

## Critic observations — dispositions at close

- **O1** — `design/current/STATUS.md` still named `AdminKeyStrings`. Annotated with the
  rename at close.
- **O2** — `design/current/STATUS.md` (the P1 entry) and the roadmap
  `design/current/PLAN-M15-admin-client.md` §4.3 still recorded the timing deviation. Both are
  annotated as superseded by M15/P9. (The roadmap is a deliberately untracked working file, so
  its note is local.)
- **O3** — the plan's §4.10 row said "3 mock seeding methods". There are 4; the row is
  corrected.
- **O4** — the "borrowed from the result root" family is exactly six types. See PM-1.
- **O5** — the B5-ordering witness (`handle.IsClosed` after `Dispose`) cannot see a leak of
  only the `GCHandle`. None is reachable today, because every guard sits before both. Recorded
  as a premise error against plan R2; no test change.
- **Rule suggestion S1** — when a fix corrects a public doc sentence, grep the same cite and
  phrase across `src/**` internal comments and `tests/**` in the same commit. Raised with the
  user; agents don't edit rule files.

## PM items — for the user, no Actor action

### PM-1 — six result docs say the per-key error is borrowed and never destroyed

`Admin/DescribeConfigsResult.cs:35`, `Admin/DescribeLogDirsResult.cs:36`,
`Admin/AlterConfigsResult.cs:42`, `Admin/AlterPartitionReassignmentsResult.cs:43`,
`Admin/AlterReplicaLogDirsResult.cs:34` and `Admin/DescribeReplicaLogDirsResult.cs:34` say
the per-key error is "borrowed from the result root and is never destroyed". These RPCs use
the per-key ABI since M15/P9. Its callbacks own `error`, and the header says to free it with
`kafka_common_Error_destroy`. The code already does: `KeyedResultMarshal.CompleteKey` reads it
with `KafkaException.FromHandle`, and `AdminCallbacks.CompletePerKey` destroys it when the key
cannot be resolved. So this is a doc-only defect. For `DescribeLogDirs`, nested per-log-dir
errors inside the value may be borrowed, so its sentence may be partly right. Not fixed here,
because it is outside the ruled scope.
