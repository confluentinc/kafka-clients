# COMMENTS.DONE.135 — M25/P3 (KafkaFuture types become classes)

### 135-S1-1 (Medium, test-portability): the engine-size test asserts the 64-bit size, so it fails in CI's 32-bit job

- **Where**: `dotnet/tests/Confluent.Kafka.UnitTests/KafkaFutureEngineSizeTests.cs:34-36` (`EngineBytes = 72`)
  and `:69-72` (the exact `bytes == EngineBytes * EngineCount` assertion), at 08c6e3475.
- **What**: 72 B is the size of `KafkaFutureImpl<RecordMetadata>` in a 64-bit process only. CI also runs these unit
  tests in a 32-bit process. The job "verify-dotnet (Windows x86)" (`.semaphore/semaphore.yml:562-564`) runs
  `.semaphore/verify-dotnet-windows.ps1 -Platform x86`, which builds with `-p:PlatformTarget=x86` and runs
  `Confluent.Kafka.UnitTests` on net462, net8.0 and net10.0 (`:85-96`). The class is compiled in for net8.0 and
  net10.0 (`#if NET8_0_OR_GREATER`). In a 32-bit process the object header and the method-table pointer are 4 B each
  and the six reference fields are 4 B each. So the engine is 4 + 4 + (6 × 4 + 4 + 3, rounded up to 32) = 40 B, and
  the test measures 2560 B where it expects 4608 B. It fails on both TFMs. That job is in the release tag's required
  list (`.semaphore/create-release-tag.yml:79`), and the test selection picks a new test file, so it runs on this PR
  too. The other allocation tests pass there because they assert an upper bound (`<=`), and objects are smaller in a
  32-bit process. This test is the first one that asserts an exact size. The comment's "(net8.0, net10.0)" and the
  commit message's "on net8.0 and net10.0" do not say that the figure is for 64-bit only. I could not run x86 on
  this Mac, so the 40 B is computed from the layout, not measured. The 64-bit figure I reran: 72 B on net8.0 and
  net10.0, in Release and in Debug, and under `--collect 'Code Coverage'` (the `coverage-dotnet` job), so those all
  pass.
- **Why it matters**: the test makes the "verify-dotnet (Windows x86)" job fail, and the release tag needs that job.
  S2 and S5 need this test to pass in every job before they can use it as the reference.
- **Fix** (test-only): use the frozen figure for the process's pointer size, as `SerdesTests` and
  `NativeAbiConformanceTests` already do with `Environment.Is64BitProcess` / `IntPtr.Size`:
  `Environment.Is64BitProcess ? 72 : 40`. The 32-bit figure must come from a run of the x86 job (its test output
  prints the per-engine bytes). If that run cannot be had in this step, assert only in a 64-bit process, and record
  that the 32-bit size is not pinned. Either way, the comment on `EngineBytes` names the pointer size, not only the
  TFMs.
- **Resolution** (fixup 64fd2b65d of 08c6e3475): the exact 72 B is asserted only when `Environment.Is64BitProcess` (the guard `SerdesTests` uses); a 32-bit process measures and prints the size with its bitness and asserts nothing, no 32-bit figure pinned; the `EngineBytes` comment names the 8 B pointer size and says the 32-bit size is not pinned. Both facts pass on net8.0 and net10.0 (72 B); UnitTests 9872/0/51 on each TFM.

### 135-S6-1 (Low, record): the STATUS gate line says the baseline tool changed nothing, and the entry has no Analyzer lists bullet

- **Where**: `dotnet/design/current/STATUS.md:15`, "`cargo xtask dotnet-java-name-baseline` (0 added, 0 removed)",
  at ae08a5504. The M25/P3 entry has no **Analyzer lists.** bullet; the two entries below it have one (`:22`, `:33`).
- **What**: The phase changed both lists. S4 (130879f1b) and S5 (e55f8b25d) each removed 4 baseline lines: the four
  futures' `kind` lines, and the typed futures' `missing … #allOf(KafkaFuture[])` and `#completedFuture(U)` lines.
  `java-name-baseline.txt` went from 381 to 373 lines (`git diff 20c9c27db HEAD -- dotnet/analyzers`). The allowlist
  lost the two `op_Implicit` lines (group M1) and gained the four
  `missing T:…KafkaFuture… org.apache.kafka.common.KafkaFuture#KafkaFuture()` lines under the ledger-628 comment.
  The commit bodies record this as "the 4 lines above, then 0/0". The STATUS line keeps only the "then 0/0".
- **Why**: Plan §8 lists the expected list changes, and §17 says the exact keys could not be known before the work,
  so the record is where the outcome belongs. The allowlist growth is the one allowed exception to "the lists only
  shrink" (by ruling, D3, ledger 628). A reader of "(0 added, 0 removed)" concludes that the lists did not change.
- **Fix**: Add a bullet like the earlier entries' bullets: "**Analyzer lists.** The four `kind` baseline lines and the typed
  futures' `missing … allOf` / `completedFuture` lines go (S4, S5: 8 lines). The allowlist's two `op_Implicit`
  lines (M1) go, and the four constructor lines come in under the D3 group (ledger 628)." In the gate line, write
  "(each step's lines, then 0 added, 0 removed)" or drop the parenthesis. This can go into S7's edit of the entry.
- **Resolution** (fixup a5ca71856 of ae08a5504): the entry has an **Analyzer lists.** bullet (S4 and S5 each remove the `kind` lines and the typed futures' `allOf` / `completedFuture` lines, 8 baseline lines, 381 to 373; the allowlist loses the two `op_Implicit` lines of group M1 and gains the four constructor lines under the ledger-628 comment), verified with `git diff 20c9c27db f88c9d710 -- dotnet/analyzers/`; the gate line reads "(each step's lines, above, then 0 added, 0 removed)".

### 135-S6-2 (Low, record): F5 in the STATUS follow-ups says N7 stores the null, which the entry's own correction contradicts

- **Where**: `dotnet/design/current/STATUS.md:16`, "F5 tests pinning N3, N6 and N7, a null future stored and returned
  as given". Compare `:13`: "F5's N7 part is whether to store `null` as Java does, not a missing test".
- **What**: `DescribeClientQuotasResult(null)` and `AsyncDescribeClientQuotasResult(null)` throw
  `ArgumentNullException("entities")` (`DescribeClientQuotasResult.cs:46`, `AsyncDescribeClientQuotasResult.cs:45`).
  They did so before the phase too (`!= default` at 20c9c27db). The tests pin it
  (`PublicAdminResultConstructorTests.cs:170-171`, `PublicAdminDescribeClientQuotasTests.cs:152-154`,
  `AdminP14SyncCompositionTests.cs:1379-1388`). So N7 is not "stored and returned as given". F5, as written, asks
  for a test that would fail. The bullet copies plan §17, which was written before the correction.
- **Fix**: "F5 tests pinning N3 and N6, a null future stored and returned as given. For N7: whether to store a null
  `entities` as Java does (today it throws `ArgumentNullException`, as Python raises `TypeError`)." This can also go into S7's edit.
- **Resolution** (fixup a5ca71856 of ae08a5504): F5 now asks for tests pinning N3 and N6 only; for N7 it asks whether to store a null `entities` as Java does, noting that it throws `ArgumentNullException` today, pinned by tests, as Python raises `TypeError`.
