# COMMENTS.DONE.84 — M17/P1 (producer transactions and idempotency)

Resolved review items, one section per Critic cycle. Local working file, never committed at the binding root; the Manager archives a copy under `design/history/M17/P1-producer-transactions/` at the phase close.

---

# COMMENTS.84 — M17/P1 (producer transactions and idempotency), Critic cycle 1: CP0

Critic N=84 (`dotnet-critic`). **Scope:** checkpoint CP0 only: PLAN §4.3 (CP0 row), §4.1, §4.2, §4.5, and Q18, Q20, Q22, Q26. CP0 is records only; there is no code.

**Reviewed:** the uncommitted CP0 working tree on `prashah_dev_dotnet_producer_transactions`. HEAD is still the base `76629aea`, because `git commit` is denied to agents. The tree holds the staged `design/history/M17/P1-producer-transactions/PLAN.md` and the untracked `gate/CP0.txt`, `gate/CP0-roster.txt` and `gate/CP0-failed.txt`. The proposed commit messages `commit1.msg` and `commit2.msg` are in the session scratchpad.

**Everything else in the record reproduced independently.** That covers:
- G1: the header hash, and the Mode-A diff both committed and in the worktree;
- G3, with the solution and grpc-server formats re-run;
- G4, with net10.0 re-run: 2317/2317;
- G5's two environments and G6 per file;
- both baseline lists, rebuilt byte for byte with the recorded pipelines (the roster from a fresh `--list`, the FAILED list from the stored run log): 146 + 6 = 152 = roster, total 722 = 152 + 570 filtered;
- the `.so` hash and its 18 / 836 exports, counted with `objdump -T` and no Docker;
- the image IDs, platforms and flavors;
- G7: only `PLAN.md` is staged, its blob equals the worktree file, and it was staged at 10:28:10, before the branch existed at 10:30:28;
- both commit messages, which follow the house style and parse the required trailer.

There is one finding. It concerns what the record must carry for CP7, not what CP0 measured.

⚠ Local working file, never committed (`bindings/dotnet/CLAUDE.md` §8.4; covered by the root `.gitignore`).

---

## Issue 84.1: CP7's "run alone" checks have no guard against a filter that matches nothing, and §4.5's own spelling of the arm names triggers it
- **File**: `bindings/dotnet/design/history/M17/P1-producer-transactions/gate/CP0.txt` (lines 12-15, the commands CP7 must run)
- **Severity**: Missing Requirement (Low). It cannot hide a regression, because the full-run guard still requires all six arms in the CP7 ok list. What it can do is put a false "passed when run alone" into the CP7 record.
- **Reference**: PLAN §4.5, "Gate at CP7":
  - bullet 1: a new failure "is re-run alone twice";
  - bullet 4: "The six arms also pass when run alone (`cargo test ... -- <arm-name> --exact`), each once";
  - the WARNING bullet: a filter that matches nothing prints `test result: ok. 0 passed` and exits 0.
  §4.5 step 5 spells the arms `test_{...}__grpc_dotnet{,_async}`, without their module path. libtest's `--exact` compares the filter with the full test path.
- **Description**: The WARNING's two guards cover only the full `-- __grpc_dotnet` run: the CP7 total must equal the roster count, and `grep -c` over the six names in the CP7 ok list must return 6. Nothing guards the isolated runs, and step 5's spelling, taken literally, selects nothing under `--exact`. Measured on this tree with `--list` (no Docker):
  - `cargo test --features integration-tests,multilanguage-tests --test integration -- test_transactional_records_are_visible_only_after_commit__grpc_dotnet --exact --list` prints `0 tests, 0 benchmarks`.
  - The same command with the `producer_transactions_test::` prefix prints `1 test`.
  - The short name without `--exact` prints 2 tests, because the substring filter also matches the `_async` twin. So dropping `--exact` breaks "each once"; it is not a fix.

  I also ran that short-name `--exact` command for real. It is still Docker-free, because nothing is selected. It prints `running 0 tests` and `test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 722 filtered out`, and exits 0. It would be recorded as "passes when run alone" although no test ran, which is exactly the false pass the WARNING bullet exists to prevent. `CP0.txt` records the pipelines for the two `comm` inputs but not the form of this command. CP7 executes the gate from this record.
- **Expected**: Add to `CP0.txt`, next to the two pipelines, that every CP7 isolated run (bullet 4's six runs and bullet 1's re-runs of a new failure) must:
  - name the test by its full path, exactly as `CP0-failed.txt` or the CP7 run log spells it, e.g. `cargo test --features integration-tests,multilanguage-tests --test integration -- producer_transactions_test::test_transactional_records_are_visible_only_after_commit__grpc_dotnet --exact`;
  - count as passing only if it prints `running 1 test` and `test result: ok. 1 passed`. The exit code alone is not enough.

  Commit 2 has not been made yet, so this is an edit to the untracked file before that commit, not a fixup. The plan is Manager-owned, so the Manager may instead amend §4.5 to the same effect.
- **Actual**: `CP0.txt:12-15` records only the roster and FAILED pipelines. The isolated-run form and its `1 passed` check are recorded nowhere, and the plan's step-5 spelling leads straight to the `0 passed` false pass.

---

## Rule suggestion (for the Manager, not an Actor action)

This is a candidate for the §7.2 RULE-DRAFT list handed over at CP7. `bindings/dotnet/CLAUDE.md` §7.5 (DoD) could state it once:

> A gate step that runs a filtered test selection passes only when the reported selected count is asserted: `running N tests`, `N passed`, or VSTest's `Total: N`. The exit code alone is not enough. This applies to a libtest `-- <filter>`, `--exact` or `--skip`, and to `dotnet test --filter`. A filter that selects nothing exits 0 in libtest.

PLAN §4.5's WARNING already states this for one run. 84.1 shows the same hazard reappearing one bullet later in the same gate. A standing rule would stop each plan from rediscovering it.

### Resolution of 84.1 (Manager, 2026-09-28)
- **Resolved by amending the plan**, which the finding allowed ("the Manager may
  instead amend §4.5"). The defect originates in the plan's spelling, so correcting
  the plan fixes every later reader, not only `CP0.txt`.
- **What changed** in `design/history/M17/P1-producer-transactions/PLAN.md`, all
  before the first commit, so no fixup exists:
  - §4.5 step 5 now states that each arm carries its `producer_transactions_test::`
    module path and that `--exact` needs it;
  - Gate-at-CP7 bullet 1 (re-runs of a new failure) and bullet 4 (the six isolated
    runs) now require the full path with `--exact`, and count a run only if it
    prints `running 1 test` and `test result: ok. 1 passed`;
  - the Critic's rule suggestion became RD10 in D15 and §7.2, for the CP7 hand-over.
- **Verified by the Manager with `--list`, no Docker:** the short name with
  `--exact` selects 0 tests; the full path with `--exact` selects exactly 1; the
  short name without `--exact` selects 2 (the sync arm and its `_async` twin).
- `CP0.txt` itself is unchanged: its two pipelines were already correct.

---

# COMMENTS.84 — M17/P1, Critic cycle 2: verification of the 84.1 resolution (CP0)

**Scope:** only the edits made after the CP0 review:
- the re-staged `PLAN.md`, blob `e31cb3c4` → `54ffc585`, 39 lines added and 7 removed;
- `COMMENTS.DONE.84.md`;
- the rewritten `commit1.msg`.

**84.1 is verified resolved.**
- §4.5 step 5's note, and gate bullets 1 and 4, now require the full path with `--exact`. A run counts only if it prints `running 1 test` and `test result: ok. 1 passed`.
- I extracted bullet 4's example command verbatim from the staged plan and ran it with `--list`. It selects exactly `1 test`.
- RD10 matches the suggestion.
- G7's added memory directories are accurate: untracked, not gitignored, and next to tracked neighbours.
- `commit1.msg` no longer says "verbatim", and its trailer parses.

The two items below are new. The same amendment introduced both.

## Issue 84.2: §10.2's "uncommitted" forms of G1 and G7 contradict §4.2's rows: G7 now false-fails on the plan's own text, and G1 loses untracked files and its control positive
- **File**: `bindings/dotnet/design/history/M17/P1-producer-transactions/PLAN.md`, §10.2, the execution note's last sub-bullet ("While work is uncommitted, …")
- **Severity**: Design Flaw (Low). Each part has a different effect:
  - (a) G7 fails spuriously at every uncommitted checkpoint, every time.
  - (b) The Mode-A proof gets a silent hole.
  - (c) G1 fails spuriously at CP1 if CP0 is still uncommitted.
- **Reference**:
  - §4.2's G7 row: its marker grep is path-scoped to `bindings/dotnet/src bindings/dotnet/tests bindings/dotnet/grpc-server Makefile`.
  - §6's DoD row 8: "G7's marker grep over the code paths".
  - §4.2's G1 row: "Control positive: `git diff --stat 76629aea..HEAD -- bindings/dotnet/` non-empty from CP1 on".
  - The note's own sub-bullet: "The next checkpoint may start while the previous one waits for the user's commit".
- **Description**: all three parts were measured on this tree.
  - **(a) G7 cannot pass as written.** The note says G7's marker grep "runs over `git diff 76629aea` plus the checkpoint's new files". Unlike the G1 form in the same sentence, it gives no path list. Run as written, `git diff 76629aea | grep -nE '^\+.*\b(TODO|FIXME|XXX|HACK)\b'` prints **2** lines, and both are `PLAN.md`'s own text: the G7 row, which quotes the regex, and §6's row "8 | No TODO / FIXME". `PLAN.md` is absent from `76629aea`, so it stays in `git diff 76629aea` at every checkpoint, and G7 can never "print nothing". §4.2's path-scoped form prints 0.
  - **(b) G1's worktree form is blind to new files.** `git diff` never lists untracked files: `git diff 76629aea -- <…>/gate/` prints 0 bytes although that directory holds three untracked files. Under this note, new files are untracked by design ("the Actor leaves its work unstaged and lists its new files"). So a new `tests/*.rs` would pass G1 unseen, though cargo compiles it as a new integration target (`autotests` is unset). So would a new file in `generator/messages/`, which `build.rs` feeds to the generator. The note adds "plus the checkpoint's new files" for G7 but not for G1.
  - **(c) G1's control positive is left in the committed form.** The note adapts G1's Mode-A diff but not the control positive in the same row. While HEAD is at the base, `git diff --stat 76629aea..HEAD -- bindings/dotnet/` prints 0 lines, so a CP1 run before the user commits CP0 fails §4.2's condition "non-empty from CP1 on". The note allows exactly that ordering. The worktree form over `bindings/dotnet/` is no replacement: it is already non-empty today, from the user's pre-existing persona deletions and the staged `PLAN.md`, so it would prove nothing.
- **Expected**: the note spells out each uncommitted form in full, keeping §4.2's scopes. I dry-ran each one here, and each prints nothing at CP0.
  - **G7:** both of these must print nothing. The exit code is not evidence, and the trailing `/dev/null` stops grep from reading stdin when there are no new files.
    ```
    git diff 76629aea -- bindings/dotnet/src bindings/dotnet/tests bindings/dotnet/grpc-server Makefile | grep -nE '^\+.*\b(TODO|FIXME|XXX|HACK)\b'
    git ls-files -z --others --exclude-standard -- bindings/dotnet/src bindings/dotnet/tests bindings/dotnet/grpc-server | xargs -0 grep -nHE '\b(TODO|FIXME|XXX|HACK)\b' /dev/null
    ```
  - **G1:** the note's worktree diff, **plus** `git ls-files --others --exclude-standard -- src/ cbindgen.toml generator/ tests/`, which must print nothing.
  - **G1 control positive:** `git diff --stat 76629aea -- bindings/dotnet/src bindings/dotnet/tests bindings/dotnet/grpc-server` must be non-empty from CP1 on. It prints 0 lines at CP0, and CP1's `NativeMethods.cs` edit makes it non-empty.
- **Actual**:
  - (a) G7 as written can never pass while `PLAN.md` differs from the base.
  - (b) G1 is blind to new files.
  - (c) G1's control positive is still in the committed form, which stays empty until the user's first commit.

## Issue 84.3: RD10 is missing from §6's DoD row 1, which still bounds the phase's rule changes at "RD1-RD9"
- **File**: `bindings/dotnet/design/history/M17/P1-producer-transactions/PLAN.md`, §6, DoD row 1 (line 1802 of the staged plan)
- **Severity**: Missing Requirement (Low, record consistency). This is the doc-drift class R12 names: "grep the clause's distinctive words across every document".
- **Reference**: D15's table, where RD10 was added (line 1000); §7.2 ("RD1-RD6, RD9 and RD10 against `bindings/dotnet/CLAUDE.md`").
- **Description**: The amendment adds RD10 to D15 and to §7.2, but §6 row 1 still says "Rule changes the phase needs are RULE-DRAFTs the user applies (RD1-RD9, Q12)". A DoD #1 check made against §6 would treat RD10 as outside the phase's rule changes. Q12 ("RULE-DRAFTs RD1-RD9?", line 1972) is the approved question, so leaving it as history is correct. Separately, the §7.2 draft file keeps the name `RULE-DRAFT-RD1-RD6-RD9-claude-md.md` although it will now carry RD10. That is harmless, but it is the one name that no longer describes its content.
- **Expected**: §6 row 1 reads "RD1-RD10" and is marked like the other CP0-review edits. Optionally, the §7.2 file name gains RD10.
- **Actual**: §6 row 1 says "RD1-RD9".

### Resolution of 84.2 and 84.3 (Manager, 2026-09-28)
- **84.2:** the §10.2 execution note now spells out every uncommitted-form command
  with §4.2's scopes, using the Critic's commands verbatim: G1 tracked diff, G1 new
  files (`git ls-files --others --exclude-standard`), the header hash, the G1 control
  positive scoped to `bindings/dotnet/{src,tests,grpc-server}`, and the G7 marker grep
  over tracked diffs and over new files. The note also says why the scope matters.
  Manager check on this tree: each command prints nothing today (the control positive
  is empty, as expected before CP1), while the old unscoped grep matched 2 lines in
  PLAN.md, confirming (a).
- **84.3:** §6 DoD row 1 now reads "RD1-RD10", and the §7.2 draft is renamed
  `RULE-DRAFT-RD1-RD6-RD9-RD10-claude-md.md`. Q12 keeps "RD1-RD9" as the approved
  question.
- Both edits are in the staged PLAN.md, before the first commit, so no fixup exists.

---

# COMMENTS.84 — M17/P1, Critic cycle 3: CP1

Critic N=84 (`dotnet-critic`). **Scope:** checkpoint CP1 only:
- the 18 declarations in `NativeMethods.cs` (the unstaged diff, +414/−1);
- the new S1 file `Interop/ProducerTransactionNativeMethodsTests.cs` (9 facts);
- `gate/CP1.txt` and the draft `commit-cp1.msg`;
- the PLAN §4.2 copy note (lines 1138-1145) the Manager asked about.

HEAD is still `76629aea`. CP0 and CP1 are both uncommitted, so every fix below is an edit before the commit, not a fixup.

**The 18 declarations are right.** Checked one by one against the header prototypes (h:16090-16463, h:16571-16628, h:13216, h:11872-12074):
- For all 18: the EntryPoint is the full symbol, the convention is `Cdecl`, and the return type is right.
- Parameter order, types (ffi §0.1) and names: every name is the header's, camelCased.
- `I1` is on the 8 `bool` returns and on row 10's `clear`.
- Rows 1-5 and 10-12 take `SafeProducerHandle` (sync, call-scoped ref). Rows 6-9 take a raw `IntPtr` (span-the-op ref, ffi §A2).
- Rows 6-9 reuse `ProducerCallbacks.OperationCallback`, `void (IntPtr error, IntPtr userData)`, which matches the four `*_callback_t` typedefs (h:2330-2368).
  - None of the four entry points takes a `user_data_destroy`. So by §B6's one-question rule, the completion callback owns the `GCHandle` free.
  - That is the shipped `OnOperation` path: `FreeGcHandle` runs in its `finally`, and `KafkaException.FromHandle` in `OperationCompletionSource.Complete` frees the delivered error. Flush and close work the same way.
  - No production code calls these four yet.
- B8: `kafka_producer_Producer_begin_transaction_async` is in the header and the dylib, but nothing binds it. The binding names it only in a comment (NativeMethods.cs:2734) and in S1's negative (S1:53, :217). The negative has control positives.

**Reproduced independently:**
- `cargo build --features ffi --release`: exit 0. The header SHA-256 `e8d39f09…` equals CP0's.
- `dotnet build -c Release bindings/dotnet/Confluent.Kafka.sln`: 0 W, 0 E, six TFM outputs. `dotnet format --verify-no-changes`: exit 0.
- S1 alone on net10.0: 9/9. Full suite: net10.0 2326/2326, net8.0 2326/2326 (runtime 8.0.31).
- G6: 715 (236 + 479), with `command grep` and with the wrapper. The base file holds 218. The diff adds 18 extern lines and removes none.
- G1, §10.2 uncommitted forms: the tracked diff is 0 bytes and the new-file list is empty. The control positive shows `NativeMethods.cs` +414/−1.
- G7, §10.2 uncommitted forms, both print nothing:
  - the tracked marker grep, with `command grep`: exit 1;
  - the new-file form, `xargs -0 /usr/bin/grep … /dev/null`: exit 123.
  Both control positives hold.
- `nm -gU target/release/libconfluent_kafka.dylib`: each of the 18 symbols and B8's appears exactly once. The header has 11 `kafka_producer_*transaction*` functions, as CP1.txt says.
- `commit-cp1.msg`: every claim matches the diff and CP1.txt, except "the full signature" (see 84.4). Its file list is exactly the three files, and the trailer parses.

**CP1.txt's deviations:** all accepted. That covers the three recorded ones, the class-summary edit, the extra M2 mutation, and `/usr/bin/grep` under `xargs` for G7's new-file form. The reasons are in the report to the Manager.

**Findings:** three, all Low, none a memory-safety defect. 84.6 is a plan finding for the Manager.

This is a local working file, never committed (`bindings/dotnet/CLAUDE.md` §8.4; covered by the root `.gitignore`).

---

## Issue 84.4: S1's shape sweep leaves out parameter names, so a same-typed reorder in 7 of the 18 declarations stays green, and two of its doc comments claim coverage the facts lack
- **File**: `bindings/dotnet/tests/Confluent.Kafka.UnitTests/Interop/ProducerTransactionNativeMethodsTests.cs`:
  - `:62-114`, the `s_declarations` shapes;
  - `:498-512`, `ShapeOf`;
  - `:305-309` and `:362-364`, two fact summaries.

  Also `gate/CP1.txt:206-207`.
- **Severity**: Wrong Test (Low). The declarations are correct today, names included. This is a gap in what S1 can detect, not a defect in the declarations.
- **Reference**:
  - PLAN §4.4's signature column spells every parameter name, e.g. row 13 `IntPtr (IntPtr groupId, int generationId, IntPtr memberId, IntPtr groupInstanceId)` (PLAN.md:1186).
  - §5.2's S1 headline: "the 18 declarations are exactly right" (PLAN.md:1356).
  - The header prototypes: h:16185, h:16384, h:16628, h:13216, and h:16300, h:16427, h:16463.
  - P/Invoke passes arguments by position. Where parameters share a type, the declaration's names are its only statement of which header parameter sits at which position. CP4 and CP5 will write their call sites against those names.
- **Description**: `ShapeOf` renders each parameter's type and its `out`/`ref`/`[Out]` mark, but not its name. The table's shapes have no names either. Seven rows have same-typed parameters:
  - rows 3 and 7: `topics`/`metadata` (`IntPtr[]`) and `partitions`/`leaderEpochs` (`int[]`); row 7 also has `producer`/`groupMetadata`/`userData` (`IntPtr`);
  - rows 6, 8 and 9: `producer`/`userData` (`IntPtr`);
  - row 12: `groupId`/`topic` (`IntPtr`) and `partition`/`metadataCap` (`int`);
  - row 13: `groupId`/`memberId`/`groupInstanceId` (`IntPtr`).

  I measured this in a scratch copy of the tree; the worktree was not touched. I swapped these parameter names:
  - row 13's `groupId`/`memberId`;
  - row 3's `topics`/`metadata` and `partitions`/`leaderEpochs`;
  - row 12's `groupId`/`topic`.

  The build stayed 0 W / 0 E, and S1 passed 9/9.

  Two doc comments claim more than the facts check:
  - `:305-309` says row 13's "four arguments reach the core in the header's order. Reflection cannot see the order, because `groupId`, `memberId` and `groupInstanceId` are all `IntPtr`, so each field gets a distinct value and is read back". This is wrong on both counts:
    - Reflection can see the order, through `ParameterInfo.Name`.
    - The read-back cannot. `NewGroupMetadata` (`:459-460`) passes its arguments by position in the header's order. So the fact proves that the native reads the header's order, whatever names the declaration uses. It stayed green under the swap.
  - `:362-364` says "a swapped entry point among the five would break" the code-120 row. Only a swap involving `IsTransactionAbortableError` would. The other four predicates all read `False` on code 120, so swapping `IsAuthorizationError` and `IsOutOfOrderSequenceError` leaves the row green. The EntryPoint fact (`:123-151`) catches every swap, so coverage is intact; only the claim is wrong.

  The same overclaim appears in two records:
  - `CP1.txt:206-207`: "Types, order and the out / [Out] marks are exactly §4.4's, and S1 pins them";
  - the commit message: "the full signature".

  Both hold for types, for order between parameters of different types, and for the marks. Neither holds for order among parameters of the same type.
- **Expected**:
  1. `ShapeOf` renders `parameter.Name` after each type, and every table shape carries the header's names camelCased, which is what the declarations have today. For example:
     - row 13: `"IntPtr (IntPtr groupId, int generationId, IntPtr memberId, IntPtr groupInstanceId)"`;
     - row 12: `"bool (SafeProducerHandle producer, IntPtr groupId, IntPtr topic, int partition, out long outOffset, out int outLeaderEpoch, [Out] byte[] outMetadata, int metadataCap)"`. CP1.txt's first deviation already records the `out*` names.
  2. The two doc comments claim only what their facts check:
     - `:305-309` says what the fact proves: the native reads the header's order, and the non-ASCII and null-instance-id paths work. It leaves the declaration's order to the sweep.
     - `:362-364` names only swaps that involve `IsTransactionAbortableError`, and points at the EntryPoint fact for the rest.
  3. CP1.txt records a name-swap mutation, for example row 13's `groupId`↔`memberId`. It is expected to turn only `TheEighteenDeclarations_HaveTheHeaderParameterShapes` red. Correct lines 206-207 if needed. The commit message's "the full signature" is then accurate.
- **Actual**: the three swaps above pass 9/9.

## Issue 84.5: Two comments in NativeMethods.cs contradict the header: they list `history` as having no C symbol, and say `outMetadata` always passes storage
- **File**: `bindings/dotnet/src/Confluent.Kafka/Internal/Interop/NativeMethods.cs`:
  - `:3022-3025`, the mock-helper section comment;
  - `:3094-3095`, the `MockProducerCommittedOffset` xmldoc.

  Also `PLAN.md:746-750`, D8's "Remarks updates" (a plan note for the Manager).
- **Severity**: Doc accuracy (Low). No behaviour changes.
- **Reference**:
  - h:16520: `kafka_producer_MockProducer_history_count`, already bound as `MockProducerHistoryCount` at `:2997-2998`.
  - PLAN §2.2 B5 (PLAN.md:161): "only `history_count` = committed `sent` count … proxied via `HistoryCount()`".
  - h:16608-16609 ("`out_metadata` … May be null") and h:16625 ("must be null or writable for `metadata_cap` bytes").
- **Description**:
  - (a) The section comment lists "history/uncommittedRecords" among the Java helpers that "have no C symbol (PLAN §2.2, B1-B7), so nothing is declared for them". But `history()`'s count has a symbol, and it is declared 24 lines above. The same sentence qualifies B6 ("the full offsets history") but not B5.
  - (b) "The header allows a null pointer for each of the three; this declaration always passes storage." This is true of `out long outOffset` and `out int outLeaderEpoch`, because a by-ref parameter always passes an address. It is not true of `[Out] byte[] outMetadata`: a `null` array marshals as NULL, which the header allows.
  - (c) Plan note: D8 tells CP5's public remarks to name "the Mode-B rows B1-B7" as "not exported at the C ABI". Copied as written, the remarks would say that `history()` is not exported, on the same type that exposes `HistoryCount()`.
- **Expected**:
  - (a) For example: "the `history()` and `uncommittedRecords()` lists (only `history()`'s count is exported, bound above as `MockProducerHistoryCount`)". Or drop `history` from the list.
  - (b) For example: "The header allows a null pointer for each of the three. The two scalar out-params always pass storage; `outMetadata` passes NULL only for a `null` array."
  - (c) D8's remarks instruction carries B5's and B6's qualifiers: only the count, and only the single-entry projection.
- **Actual**: as quoted.

## Issue 84.6 (plan finding for the Manager): the §4.2 copy note is correct for what it names, but it misses one of G7's escapes, and in the CP7 row its advice turns a live check into one that cannot fail
- **File**: `bindings/dotnet/design/history/M17/P1-producer-transactions/PLAN.md`:
  - `:1138-1145`, the note;
  - `:1132`, G7;
  - `:1162`, the CP7 row;
  - `:1156`, the CP1 row.
- **Severity**: Plan defect (Low).
  - (a) and (c) fail loudly.
  - (b) fails silently. CP7's second Makefile check and G10 would still catch a leftover `--skip` line, so only the stale comment block could slip through.
- **Reference**:
  - The note: "Remove those backslashes when copying from the raw file."
  - GFM tables: an escaped pipe renders as `|` inside a cell, including inside a code span. So the rendered cell shows a bare `|`.
  - In a basic regex (no `-E`), `\|` is alternation, in GNU grep and in this host's `/usr/bin/grep` (2.6.0-FreeBSD, "GNU compatible"). A bare `|` is a literal.
- **Description**: measured on this tree.
  - (a) The note names G6's `\| wc -l` and G7's `(TODO\|FIXME\|XXX\|HACK)`. G7 has a third escape, `Makefile \| grep` (`:1132`). Copied raw, everything after `--` is a pathspec, so the command prints the diff itself.
    - Today it prints 0 bytes, because HEAD is the base.
    - To see what it will print once CP1 is committed, I ran it over the worktree (`git diff 76629aea -- …`): 32,045 bytes. That is a loud false fail.

    So the note's "Left in, … the gate passes without being able to fail" describes only a partly de-escaped copy.
  - (b) The note covers only "this table". But §4.3's CP7 row (`:1162`) checks `grep -c 'producer_transactions\|THREE TESTS ARE SKIPPED\|--skip test_' Makefile` = 0, and there the backslashes are regex alternation and must stay.
    - The raw form, on today's unedited Makefile, counts **5**: lines 273, 275, 309, 310 and 311, all inside the block CP7 deletes. So the raw form is a live check.
    - The rendered form, which is also what the note's advice produces, counts **0**. So "= 0" already holds before the edit.
    - The same row's `make -n test-integration-dotnet \| grep -c -- '--skip'` needs the opposite treatment. Raw, make exits 2 ("invalid option -- c"). De-escaped, it counts 3 today.

    So one row holds two commands with opposite rules, and nothing in the plan says so.
  - (c) Two more instances, with less at stake:
    - the CP1 row's `nm -gU … \| grep -cw _<symbol>`, already run (I re-ran `nm`, and CP1.txt's counts are right);
    - §7.1 X2's C# `\|\|` (`:1841`), a compile error if copied raw.
- **Expected**:
  1. Move CP7's two checks out of the table into a list or a code block, as §10.2 did for G1 and G7, with bare pipes and `-E`:
     - `command grep -cE 'producer_transactions|THREE TESTS ARE SKIPPED|--skip test_' Makefile`: 5 today, 0 after CP7;
     - `make -n test-integration-dotnet | command grep -c -- '--skip'`: 3 today, 0 after.

     CP7's gate record then shows both the before and after values, which proves each check live.
  2. Extend the note to list every escaped command in §4.2 and §4.3: G6; G7's pipe and its pattern; CP1's `nm`; CP7's two checks. If CP7's first check stays in the table, the note says it is the exception.
- **Actual**: as above.

### Resolution of 84.4 and 84.5(a)(b) (Actor, 2026-09-28)
- **84.4, part 1:** `ShapeOf` renders each parameter as its type, its `out` / `[Out]` mark and its name. Every row of `s_declarations` carries the header's parameter names, camelCased; row 12 keeps `outOffset` / `outLeaderEpoch` / `outMetadata` (CP1.txt's first deviation). The class remarks, the table's summary and the shape fact's summary say that names are compared, and why: P/Invoke binds arguments by position.
- **84.4, part 2:** the two fact summaries claim only what their facts detect.
  - Row 13's fact says that the native reads its arguments in the header's order, that its call passes them by position and so cannot see the declaration's names, and that the names are the shape sweep's job. The fact's name is unchanged; its summary now says what "header order" covers.
  - The code-120 fact says it breaks only on a swap that involves `IsTransactionAbortableError`, and points at `TheEighteenDeclarations_BindTheirSymbols_WithCdecl`, which catches every swap.
- **84.4, part 3:** CP1.txt's first note says S1 now pins every parameter name, and no longer claims that S1 pinned the order of same-typed parameters before this round. CP1.txt records M3, the requested swap of row 13's `groupId` and `memberId` in the declaration (NativeMethods.cs:412 and :414), checked with `diff` before the run.
  - Red: exactly `TheEighteenDeclarations_HaveTheHeaderParameterShapes` (8 passed, 1 failed; "Collections differ" at pos 12, `IntPtr (IntPtr group…` expected, `IntPtr (IntPtr membe…` actual). The row-13 call fact stayed green, as its new summary says it must.
  - Control: the same mutation against the pre-fix S1 passes 9/9, which reproduces the finding.
  - Restored, `cmp` identical, then 9/9.
  - M1 and M2 were re-run on the final test file. Their red sets are the same as before: M1 turns the EntryPoint fact, the family fact and the smoke red; M2 turns the I1 sweep red. Only M2's line moved, from 3062 to 3064.
  - The commit message's "the full signature" is now accurate, and its mutation sentence adds the name swap.
- **84.5 (a):** the mock-helper section comment (NativeMethods.cs:3018-3029) says that `consumerGroupOffsetsHistory()` is exported only as the single-entry projection (B6), and `history()` only as its count, bound above as `MockProducerHistoryCount` (B5). It names the helpers with no C symbol at all: B1-B4, `uncommittedRecords` (B5), `uncommittedOffsets` (B6) and B7.
- **84.5 (b):** `MockProducerCommittedOffset`'s xmldoc (NativeMethods.cs:3096-3102) says that the two scalar out-params always pass storage, because a by-reference parameter always marshals an address, and that a null `outMetadata` array marshals as NULL, which the header allows (h:16605-16609, h:16624-16625).
- **(d), the Manager's extra item:** `ConsumerCommitAsync`'s xmldoc (NativeMethods.cs:1608) reads "the genuine `_async` declarations". The count is deleted rather than recounted (ffi §A6 round-5 rule).
- **Gates, on the final tree:** G1-G7 were re-run with the §10.2 uncommitted forms and `command grep`, and all pass.
  - G1: the tracked Mode-A diff is 0 bytes and the new-file list is empty. The header SHA-256 is `e8d39f09…`, unchanged. The control positive shows `NativeMethods.cs` +419/−2.
  - G2: 0 W / 0 E, incremental and `--no-incremental`. G3: `dotnet format` is clean.
  - G4 and G5: net10.0 and net8.0 are both 2326/2326. No fact was added.
  - G6: 715. G7: both marker greps print nothing, and both control positives hold.
- The fix round changed no declaration, and no fact's name or body. Nothing is staged, and PLAN.md's staged blob is still `ea653cbb`.

### Resolution of 84.5(c) and 84.6 (Manager, 2026-09-28)
- **84.5(c):** D8's "Remarks updates" now carries §2.2's qualifiers. B1-B4 and B7 are not exported at the C ABI. For B5 only `history()`'s count is exported, as the existing `HistoryCount()`. For B6 only a single-entry projection is exported, as `CommittedOffset`. The edit is marked in place.
- **84.6:** CP7's two `Makefile` checks moved out of §4.3's table into a list under it, written with `-E`, bare pipes and before-values. The Manager re-measured both on this tree:
  - `command grep -cE 'producer_transactions|THREE TESTS ARE SKIPPED|--skip test_' Makefile` gives 5, and must give 0 after CP7;
  - `make -n test-integration-dotnet | command grep -c -- '--skip'` gives 3, and must give 0 after CP7.

  The list also records that the recipe line calls `$(MAKE)`, so GNU make 4.4.1 executes it even under `-n`. On macOS that only prints the SKIP banner; on Linux it would run the suite. §4.2's note now lists every escaped command in the plan's tables and what a raw copy of each does: G6's pipe, G7's pipe and pattern, the CP1 row's `nm`, and §7.1 X2's `||`. It also says why CP7's checks left the table.
- **Also from this review:**
  - the Critic's three rule suggestions join D15 and §7.2 as RD11-RD13, and §6 row 1 now reads RD1-RD13;
  - D12 records the Critic's two CP4 watch items;
  - D15's file-forward list gains item 13, the ABI's null-producer / dispatcher-thread wording, for kafka-critic.
- **Where these edits live:** unlike CP0's amendments, they are in the worktree copy of PLAN.md only. The staged blob stays the CP0-reviewed `ea653cbb`, so the user's pending CP0 commit captures exactly what was reviewed. The amendments become their own docs commit after CP0's.

---

# COMMENTS.84 — M17/P1, Critic cycle 4: CP1 re-verification

**Scope:** only the edits made after the CP1 review.
- The Actor's fix round:
  - the unstaged `NativeMethods.cs` diff, now +419/−2 at 3438 lines;
  - the untracked S1 file, 588 lines;
  - `gate/CP1.txt`, 322 lines;
  - `commit-cp1.msg`.
- The Manager's amendments:
  - `PLAN.md` worktree `59308aaf` against the staged `ea653cbb`, +82/−16;
  - `commit-plan-cp1.msg`.

**Verified resolved:**
- **84.4.**
  - `ShapeOf` renders `{type} {name}`.
  - All 18 expected shapes carry the header's names, camelCased. Row 12 keeps `out*`, a recorded deviation.
  - The two re-worded fact summaries claim only what their facts detect.
  - I re-ran the mutation check in a scratch copy. M3, which swaps row 13's `groupId` and `memberId`, turns only `TheEighteenDeclarations_HaveTheHeaderParameterShapes` red: 1 failed, 8 passed.
  - The control, the pre-fix S1 against the same swap, passes 9/9.
- **84.5 (a) and (b).**
  - The section comment (`NativeMethods.cs:3018-3029`) matches the header's MockProducer exports.
  - The `MockProducerCommittedOffset` xmldoc (`:3096-3102`) matches h:16605-16609 and h:16624-16625.
- **84.5 (c).** D8 carries B5's and B6's qualifiers as §2.2's "C ABI today" column states them. `HistoryCount()` is the name of the shipped member.
- **84.6.**
  - The §4.2 note covers every `\|` in the plan. `command grep -n '\\|'` finds them on lines 1167, 1168, 1207 and 1905, and on the note's own lines.
  - Each consequence the note states is right. I tested the `-E` pattern case on GNU grep 3.12 and BSD grep 2.6.0.
  - The CP7 list's before-values re-measure at 5 and 3.
  - The `$(MAKE)` caution is right. `Makefile:293-312` is one logical recipe line that contains `$(MAKE)`. Here, `make -n` both printed that line and ran it, and the SKIP banner appeared. On Linux, the `else` branch's `cargo test` would run for real.
- **D12's two watch items** match the code.
  - They agree with `NativeProducer.cs:1384-1389`, `AbandonBeforeSubmit` / `FreeGcHandle` and `ProducerCallbacks.OnOperation`.
  - The Interlocked guard stops a second free of the same context. A callback that fires after the abandon reaches the context only through the freed `GCHandle`, so the guard does not cover that case.
  - h:16625 is the right citation for the cap.
- **RD11-RD13** match my CP1 suggestions. §6 row 1 reads RD1-RD13, and §7.2's two file names carry the right IDs. No "RD1-RD10" remains; Q12's "RD1-RD9" is the approved question.
- **Item (d).** The sentence now carries no count, as ffi §A6's round-5 rule requires. The reason the records give for the change is wrong: see 84.7.
- **Re-run, all green:**
  - cargo build (up to date);
  - header SHA-256 `e8d39f09…d111`;
  - dotnet build 0W/0E and dotnet format;
  - S1 9/9;
  - net10.0 and net8.0, 2326/2326 each;
  - G6: 715;
  - the G1 and G7 uncommitted forms, with their control positives;
  - the staged blob is still `ea653cbb`.

## Issue 84.7: `gate/CP1.txt` and `commit-cp1.msg` say CP1's four async rows made the "18 genuine `_async`" count stale, but it was already wrong at `76629aea`
- **File**:
  - `bindings/dotnet/design/history/M17/P1-producer-transactions/gate/CP1.txt:79-82`, the Files section.
  - `commit-cp1.msg` in the session scratchpad, which says: "drops its count of the genuine _async declarations, which the four added here would have staled (ffi §A6 round-5 rule)".
- **Severity**: Record accuracy (Low). The code change is right; only the stated reason is wrong. CP1.txt is one of the three files this commit carries.
- **Reference**:
  - The base tree: `76629aea:bindings/dotnet/src/Confluent.Kafka/Internal/Interop/NativeMethods.cs`.
  - ffi §A6's round-5 amendment (`ffi-marshalling.md:961-972`): no counts outside the canonical enumeration.
  - My CP1 report, where item (d) came from: "The count was already 22 at `76629aea` and is 26 now."
- **Description**:
  - The sentence contrasts ConsumerCommitAsync, an `_async` name on a sync ABI function, with "the span-the-op one the genuine `_async` declarations use".
  - At `76629aea` there are 22 such declarations, each an `internal static extern void …Async(IntPtr …, …)`: 19 consumer and 3 producer (flush, close, partitions_for).
  - The other five `_async` entry points return `IntPtr` and take a SafeHandle, so they are sync like ConsumerCommitAsync: `ConsumerCommitAsync`, `ConsumerCommitAsyncWithCallback`, `ConsumerCommitAsyncOffsetsWithCallback`, `ConsumerHandleCommitAsync` and `ConsumerHandleCommitAsyncOffsets`.
  - No reading of the base tree gives 18. All the `_async` EntryPoints come to 27, the void ones to 22, and the consumer void ones to 19.
  - So the number was stale before CP1, and CP1's four rows take it to 26. Both records say the four rows "would have staled" the number, which presents the base tree's sentence as accurate.
  - The round-5 rule requires deleting the count in either case, so only the stated cause needs fixing.
- **Expected**: both records state the measured cause. For example:
  - CP1.txt: "…now reads "the genuine <c>_async</c> declarations". The number was already stale at 76629aea (22 such declarations, 26 with CP1's four async rows), so it is deleted rather than recounted (ffi §A6's round-5 rule; fix-round item (d))."
  - commit-cp1.msg: "…drops its count of the genuine _async declarations, which was already stale (it said 18; the base had 22, and CP1 makes 26) (ffi §A6 round-5 rule)."
- **Actual**:
  - CP1.txt:80-81: "CP1's four async rows would have staled the number".
  - commit-cp1.msg: "which the four added here would have staled".

## Issue 84.8 (plan finding for the Manager): D15's file-forward item 13 says "the four async transaction entry points", but five entry points in the header carry the conflicting wording
- **File**: `bindings/dotnet/design/history/M17/P1-producer-transactions/PLAN.md` (worktree `59308aaf`), D15's file-forward list, item 13 (lines 1087-1090).
- **Severity**: Record accuracy (Low). The item is handed to `kafka-critic` through the STATUS entry (the list is "reported in the STATUS entry, not acted on"). A fix scoped to four entry points would leave the fifth doc inconsistent.
- **Reference**: `target/include/confluent_kafka.h` (SHA-256 `e8d39f09…d111`). Each of the five transaction `_async` entry points carries both sentences:

  | Entry point | Prototype | "fires on the producer's dispatcher thread" | "(null is reported through `callback`)" |
  |---|---|---|---|
  | `init_transactions_async` | h:16300 | h:16278 | h:16291 |
  | `begin_transaction_async` | h:16329 | h:16313 | h:16320 |
  | `send_offsets_to_transaction_async` | h:16384 | h:16339 | h:16353 |
  | `commit_transaction_async` | h:16427 | h:16406 | h:16418 |
  | `abort_transaction_async` | h:16463 | h:16445 | h:16454 |

  `flush_async`, `close_async` and `partitions_for_async` carry only the dispatcher sentence (h:16009, h:16026, h:16044), so the conflict is confined to these five.
- **Description**:
  - "The four" counts the binding's bound async rows (§4.4 rows 6-9), but the item concerns the ABI's docs.
  - `begin_transaction_async` is unbound in .NET by decision (B8). It still exists in the ABI, with the same two sentences.
  - My CP1 note said "the async ops", with no count.
  - `commit-plan-cp1.msg` says "the async transaction callbacks", also with no count, so it stays accurate whichever wording the plan takes.
- **Expected**: for example, "the five async transaction entry points (h:16300, h:16329, h:16384, h:16427, h:16463, including `begin_transaction_async`, which .NET leaves unbound, B8) say their callback …".
- **Actual**: "the four async transaction entry points say their callback "fires on the producer's dispatcher thread", yet a null producer is "reported through `callback`", which cannot involve that dispatcher."

### Resolution of 84.7 (Actor, 2026-09-28)
- **Measured first, at both trees (RD13),** in `NativeMethods.cs`, the file that holds the sentence:
  - void `_async` externs: `git show 76629aea:<file> | command grep -A1 'EntryPoint = "[^"]*_async' | command grep -c 'internal static extern void '`, and the same grep over the worktree file. **22** at the base (19 consumer, 3 producer) and **26** in the worktree (19 consumer, 7 producer).
  - all `_async` EntryPoints: `command grep -c 'EntryPoint = "[^"]*_async'`, 27 at the base and 31 in the worktree. The five that are not void (`ConsumerCommitAsync`, `ConsumerCommitAsyncWithCallback`, `ConsumerCommitAsyncOffsetsWithCallback`, `ConsumerHandleCommitAsync`, `ConsumerHandleCommitAsyncOffsets`) return `IntPtr` and take a SafeHandle in both trees.
  - `NativeMethods.Admin.cs`, the class's other part, holds 49 void `_async` externs in both trees; the file is unchanged since the base.
  - A declaration parser that does not rely on line adjacency gives the same numbers. They agree with the Critic's 22 and 26, and no scope gives 18.
- **`gate/CP1.txt`,** the Files section (now :79-91), states the measured cause: "The number was already stale at 76629aea, where this file declares 22 void _async externs (19 consumer, 3 producer); CP1's four async rows make 26 (19 and 7). So it is deleted rather than recounted (ffi §A6's round-5 rule; fix-round item (d))." It then quotes the command with both values, and the 27/31 and 49 figures.
- **`commit-cp1.msg`** now reads: "…xmldoc drops its count of the genuine _async declarations, which was already stale: it said 18, where the base file had 22 and CP1 makes 26 (ffi §A6 round-5 rule)."
- No code changed, so no test was re-run. Nothing is staged, and PLAN.md is untouched (staged blob `ea653cbb`).

### Resolution of 84.8 (Manager, 2026-09-28)
- D15's file-forward item 13 now names the five async transaction entry points, `begin_transaction_async` included, which .NET leaves unbound (B8). It cites both conflicting sentences by header line for each:
  - "fires on the producer's dispatcher thread": h:16278, h:16313, h:16339, h:16406, h:16445;
  - "null is reported through `callback`": h:16291, h:16320, h:16353, h:16418, h:16454;
  - the prototypes: h:16300, h:16329, h:16384, h:16427, h:16463.

  The Manager re-measured these against the header at SHA-256 `e8d39f09…d111`. The item also says that `flush_async`, `close_async` and `partitions_for_async` carry only the dispatcher sentence.
- The Critic's two cycle-4 rule suggestions are folded into RD13. A count or cause stated in a gate record, commit message or plan hand-off is measured when it is written, at the base and at the worktree, with both values quoted. A claim about the ABI is counted from the header, not from the binding's bound subset.
- `commit-plan-cp1.msg` is updated to match. The edits are still worktree-only; the staged blob stays `ea653cbb`.

---

# COMMENTS.84 — M17/P1, Critic cycle 5: CP1 final check

**Scope:** only the edits made after cycle 4.
- The Actor's 84.7 fix: `gate/CP1.txt:79-91` and `commit-cp1.msg:33-35`.
- The Manager's 84.8 fix and the RD13 extension: `PLAN.md` worktree `7c165f61`, against the cycle-4 worktree `59308aaf` and the staged `ea653cbb`.
- `commit-plan-cp1.msg`.
- `COMMENTS.DONE.84.md`.

**Verified:**
- **84.7 is resolved.**
  - I diffed both records against the Actor's backups, whose hashes match what I saw in cycle 4. Only the 84.7 passages changed.
  - I re-measured every number with a declaration parser that does not rely on line adjacency:
    - 22 void `_async` externs at `76629aea` (19 consumer, 3 producer), and 26 in the worktree (19 and 7);
    - 27 and 31 `_async` EntryPoints;
    - 5 that are not void, each returning `IntPtr` and taking a `SafeHandleZeroIsInvalid` subclass;
    - 49 void `_async` externs in `NativeMethods.Admin.cs` in both trees. That file is unchanged, and it is the class's only other part.
  - The command recorded at `CP1.txt:85-86` prints 22 when run verbatim under zsh and under bash.
  - The "18" in `STATUS.md:457` needs no action. It is the M9/P4 history entry, and the M9/P4 plan itself records "the 18 `_async` declarations" (`design/history/M9/P4/PLAN.md:371`), so the number was correct when written.
- **RD13's extension is faithful** to both of my cycle-4 suggestions, and it cites 84.4, 84.7 and 84.8.
- **`commit-plan-cp1.msg`:** the RD13 line matches the RD13 row. The item-13 line is true as written, but see 84.9.
- **`COMMENTS.DONE.84.md`:**
  - The first 33516 bytes are byte-identical to the pre-move copy: 327 lines, with the same heading lines I saw in cycle 4.
  - The moved block is byte-identical to the text I wrote in cycle 4. I extracted that text from my own transcript: 7857 bytes, 88 lines.
  - Both resolutions follow it, and each states what was done accurately.
- **Invariants:**
  - The `NativeMethods.cs` blob `a3ffb545` and the S1 file's SHA-256 `5f550522…` are unchanged since cycle 4.
  - The staged blob is `ea653cbb`, and only `PLAN.md` is staged.
  - G1 prints 0 bytes in both forms. Its control positive still shows 419 insertions and 2 deletions.
  - The header SHA-256 is `e8d39f09…d111`.
  - Both G7 greps print nothing (exit 1 and 123). Their controls still match.
  - G6 prints 715.

## Issue 84.9 (plan finding for the Manager; the error is mine, from 84.8): D15's item 13 says `flush_async`, `close_async` and `partitions_for_async` carry only the dispatcher sentence, but two of them also route a null producer through `callback`, and `FutureRecordMetadata_get_async` does the same for a null future
- **File**: `bindings/dotnet/design/history/M17/P1-producer-transactions/PLAN.md` (worktree `7c165f61`), D15's file-forward list, item 13, last sentence (lines 1094-1095).
- **Severity**: Record accuracy (Low). It is the same class and the same harm as 84.8. The item goes to `kafka-critic`, and a fix scoped by it would leave three more docs with the conflict.
- **Origin**: my cycle-4 finding 84.8. The Manager applied its wording as written.
  - 84.8 said that `flush_async`, `close_async` and `partitions_for_async` "carry only the dispatcher sentence (h:16009, h:16026, h:16044), so the conflict is confined to these five".
  - I had grepped for the exact phrase "null is reported through `callback`", which misses the variant "null reported via `callback`".
  - The archived 84.8 text and the Manager's resolution record both describe that cycle accurately, so they need no edit once this finding is archived next to them.
- **Reference**: `target/include/confluent_kafka.h`, SHA-256 `e8d39f09…d111`, which is the same at the base and in the worktree. Measured with:
  ```
  command grep -n "fires on the producer's dispatcher thread" target/include/confluent_kafka.h
  command grep -n -E 'reported (as an error )?(through|via) `callback`|null is a no-op success' target/include/confluent_kafka.h
  ```
  - The first grep prints 9 lines: the five that item 13 cites, plus the four below.
  - The second prints 11 lines. Nine of them are the null-handle clauses of those nine docs. The other two, h:16286 and h:16381, concern other conditions.

  | Entry point | Prototype | Dispatcher sentence | Null-handle sentence |
  |---|---|---|---|
  | `FutureRecordMetadata_get_async` | h:15590 | h:15579 | h:15586-15587: "`future` must be a valid handle from a send function, or null (null is reported as an error through `callback`)" |
  | `flush_async` | h:16018 | h:16009 | h:16015: "`producer` must be a valid handle, or null (null reported via `callback`)" |
  | `close_async` | h:16035 | h:16026 | h:16032: "`producer` must be a valid handle, or null (null is a no-op success)" |
  | `partitions_for_async` | h:16055 | h:16044 | h:16051: "`producer` must be a valid handle, or null (null reported via `callback`)" |
- **Description**:
  - `flush_async` and `partitions_for_async` carry item 13's conflict exactly: the callback "fires on the producer's dispatcher thread", yet a null producer is "reported via `callback`".
  - `FutureRecordMetadata_get_async` has the same shape for a null future, which names no producer: "fires on the producer's dispatcher thread", yet a null future "is reported as an error through `callback`".
  - `close_async` fits the item's sentence. It says a null producer is a no-op success, and does not say whether `callback` fires.
  - So the sentence is false for two of the three entry points it names, and the conflict reaches beyond the transaction entry points.
  - `commit-plan-cp1.msg`'s item-13 line ("On all five async transaction entry points …") stays true. It would under-describe a widened item.
- **Expected**: prefer (a), because the item is a hand-off and (a) makes it complete.
  - (a) **Widen the item.** For example: "Eight entry points carry both sentences: the five transaction ones above; `flush_async` (h:16009, h:16015) and `partitions_for_async` (h:16044, h:16051), whose null producer is "reported via `callback`"; and `FutureRecordMetadata_get_async` (h:15579, h:15586-15587), whose null future "is reported as an error through `callback`". `close_async` (h:16026, h:16032) says only that a null producer is a no-op success." Update `commit-plan-cp1.msg`'s item-13 line to match.
  - (b) **Delete the last sentence** and keep the item scoped to the five transaction entry points. A claim that is not made cannot go stale, but the hand-off stays incomplete.
- **Actual**: "`flush_async`, `close_async` and `partitions_for_async` carry only the dispatcher sentence."

### Resolution of 84.9 (Manager, 2026-09-28)
- Taken as option (a), in the worktree `PLAN.md` (blob `ff2f0224`). The staged blob stays `ea653cbb`.
- D15's file-forward item 13 now names the eight async producer entry points that carry both sentences, and cites each by header line:
  - the five transaction entry points, `begin_transaction_async` included: the dispatcher sentences at h:16278, h:16313, h:16339, h:16406, h:16445; the null-producer clauses at h:16291, h:16320, h:16353, h:16418, h:16454; the prototypes at h:16300, h:16329, h:16384, h:16427, h:16463;
  - `flush_async` (h:16009, h:16015) and `partitions_for_async` (h:16044, h:16051), whose null producer is "reported via `callback`";
  - `FutureRecordMetadata_get_async` (h:15579, h:15586-15587), whose null future "is reported as an error through `callback`".
- It also names two neighbours:
  - `close_async`, which calls a null producer "a no-op success" (h:16026, h:16032);
  - `FutureRecordMetadata_get_all_async`, which turns a null entry into a per-index `InvalidRequest` error (h:15600-15601) without saying which dispatcher fires when no entry is non-null. Neither of the finding's greps reaches it, each for its own reason:
    - its dispatcher sentence says only "the dispatcher thread" (h:15599), which the first grep's phrase does not match;
    - its null clause, "a null `futures[i]` yields an `InvalidRequest` error", matches none of the second grep's alternatives.
- Each claim the item makes that the header does *not* say something was checked with a search that does not depend on one phrasing, and each block it rests on was read whole, as RD13(a) requires:
  - `close_async` does not say whether `callback` fires for a null producer (h:16032);
  - `get_all_async` says only "the dispatcher thread", and nothing about which dispatcher fires when no entry is non-null;
  - no other producer entry point carries both sentences (the two sweeps below).

  The Actor caught errors in two earlier drafts of this resolution before archiving. The first said the item makes no absence claim, and gave only the first grep's reason for missing `get_all_async`. The second said no caller hands the callback typedefs a null handle, which their own blocks contradict, and dropped `close_async`'s null-producer scope.
- The Manager measured the set with two sweeps that do not depend on one phrasing, at header SHA-256 `e8d39f09…d111`. That is the value `gate/CP0.txt:54` measured at the base and `gate/CP1.txt:212` measured at CP1.
  - **Sweep 1:** every `/** … */` block that mentions a dispatcher, in any wording, paired with the declaration after it.
    - There are 65 header-wide: 49 admin, 12 producer and 4 consumer.
    - Of the 12 producer blocks, 8 carry both sentences and 2 are the neighbours.
    - The remaining 2, `send_with_callback` and `send_async`, require a valid producer and send their validation errors to `out_error` without invoking `callback`.
  - **Sweep 2:** every producer function block with a clause that mentions null before `callback`, 11 blocks.
    - They give the same 8.
    - The other 3, `send_with_callback`, `send_async` and `send_batch_async`, match through clauses about validation errors or about which result handle is non-null. None of them routes a null handle through `callback`.
  - **The Actor's looser pass,** any producer block containing both words, finds 17: the 11, the two neighbours, three callback typedefs (h:2189, h:2316, h:2329), and `RecordMetadata_copy` (h:15734).
    - The typedefs are callbacks rather than entry points. The nulls they mention are result handles the core delivers to them (h:2195, h:2318-2320, h:2331-2332), not a caller's null argument routed through `callback`.
    - `RecordMetadata_copy` requires a non-null handle (h:15744, h:15757).

  The blocks whose classification rests on an absence were read in full: the two neighbours, the three `send*` blocks, the three callback typedefs and `RecordMetadata_copy`. The eight rest on the quoted lines. All 27 header lines the item cites were printed and checked. `send_offsets_to_transaction_async`'s null `group_metadata` clause (h:16380-16381) does not conflict on its own. With a valid producer the dispatcher exists, and a null producer is the case h:16353 already covers. The finding classes h:16381 the same way.
- Both cycle-5 rule suggestions are folded into RD13.
  - (a): an absence claim is checked by a search that does not depend on one phrasing, such as reading every matching block whole, or it is not made.
  - (b): RD13's target cell now reads §7.4 for the test half, and §7.5 (DoD), next to RD12, for the record half.

  The row now cites 84.9.
- The Manager found a second stale count from the same amendment while fixing this one. §7's STATUS-entry list said "the file-forward list (D15, 12 items)". That was true at the CP0 review, but item 13 makes 13. The count is deleted, not updated (ffi §A6 round 5, RD13). The approval record says so, since a deletion carries no in-place marker.
- `commit-plan-cp1.msg`: the item-13 line now names the eight and the two neighbours, and notes the deleted count. The RD13 line carries the absence clause.

---

# COMMENTS.84 — M17/P1, Critic cycle 6: CP1 final check (clean)

Recorded by the Manager, 2026-09-28. Critic 84 reported no findings, so COMMENTS.84.md stayed at 0 bytes and CP1 closes.
- **Checked:**
  - the four PLAN.md hunks against the cycle-5 copy (`7c165f61` → `ff2f0224`), with the staged blob still `ea653cbb`;
  - `commit-plan-cp1.msg`;
  - this file's cycle-5 archive (bytes 44322-50936, and the 5178-byte 84.9 resolution);
  - G1, G6 and G7 in their uncommitted forms, and the invariants.
- **Item 13's absence claims** were re-measured with the Critic's own parser, which covers all 1032 header doc blocks, function-pointer typedefs included. The Critic concluded the eight entry points are exhaustive and the two neighbour claims hold.
- **Deferred to the CP7 RULE-DRAFTs:** the Critic's optional rule suggestion, as a candidate extension of RD13. When a sweep backs an exhaustiveness claim in a record, give both the matched count and the count left after reading, and make an "every block" sweep include typedef doc blocks.
- **Accepted as written:** two optional wording notes.
  - Item 13's "gives the same eight". The raw second sweep returns 11 blocks; the resolution above records both numbers.
  - `commit-plan-cp1.msg`'s "marked in place". Its one exception is the deletion that the approval record names.

---

# COMMENTS.84 — M17/P1, CP2: Manager decision on the Actor's observation (b)

Recorded by the Manager, 2026-09-29, before Critic 84's CP2 review.
- **The observation.** CP2's compiler probe (`gate/CP2.txt`) finds admin results that rebuild a failure under a new message through the 3-arg internal ctor. The rebuilt exception answers false to all five D6 predicates, and PLAN :580-581 called `FromBorrowedHandle` "the only classified construction site".
- **The decision, taken from each site's Java:**
  - `AlterConsumerGroupOffsetsResult.All()` is **fixed in CP2**. Java throws `error.exception(message)` (`AlterConsumerGroupOffsetsResult.java:76-77`), a new instance of the failure's own class (`Errors.java:462-469`), so Java's `instanceof` answers follow the failure. Before the fix, a code-30 failure read back `IsAuthorizationError == false`. The rebuild now passes the five predicates on through the eight-argument ctor.
  - `RemoveMembersFromConsumerGroupResult.All()` is **left unchanged**. Java wraps the failure in a bare `KafkaException` (`RemoveMembersFromConsumerGroupResult.java:59-60`), so five falses match Java. Its `IsRetriable` pass-through is a pre-existing divergence on a branch whose own comment says it can only find success. It is filed forward (D15 item 14), not fixed.
- **Why it is in scope.** The predicates are D6's new public API, and a wrong answer on a shipped surface is a defect in that API. The fix only carries forward values the binding already holds, and it adds no Kafka behavior.
- **The Actor's re-gate** (2026-09-29 00:35):
  - 2364/2364 on net10.0 and net8.0; net462 is build-only.
  - The probe now finds only `RemoveMembersFromConsumerGroupResult.cs(99,31)` as a 3-arg caller (3 × CS1729). The base had three sites (9 × CS7036).
  - Mutation reds: 4 for reverting to the 3-arg ctor. Every one of the 15 flag-argument swaps goes red.
- **The plan amendment is deferred.** The worktree PLAN.md belongs to the pending CP1 plan-amendment commit, so the D6 correction waits until the user's four CP0/CP1 commits exist. It is drafted as scratchpad `cp2-plan/amend-cp2.py` (F1-F5), with its dry-run diff in `cp2-plan/amend-cp2.diff`. The draft deletes the uniqueness claim (no marker), adds a D6 "Rebuilt failures" bullet, corrects DoD row 11 and the `errors.rs` citation, and adds D15 items 14-21.

---

# COMMENTS.84 — M17/P1, Critic cycle 7: CP2

Critic N=84 (`dotnet-critic`). **Scope:**
- CP2: D6's five predicates, D2's constructors, S2, and S3's CP2 half;
- the Manager's decision on observation (b), `AlterConsumerGroupOffsetsResult.All()`;
- `gate/CP2.txt` and `commit-cp2.msg`;
- the CP2 plan amendment draft, F1-F5 (`cp2-plan/amend-cp2.py` and `.diff`).

**Reviewed:** the uncommitted CP2 worktree at HEAD `5e116925`. The user's four CP0/CP1 commits exist and the index is empty, so `git diff HEAD` shows CP2 alone. Each tracked CP2 file's HEAD blob equals its `76629aea` blob. PLAN.md is committed at `ff2f0224`.

**Reproduced independently:**
- **D6's sets.** My own script follows the Java `extends` chain at ccacf8a and resolves every class: TransactionAbortable {120}; ApplicationRecoverable {22, 25, 47, 49, 82, 90}; InvalidConfiguration {17, 18, 21, 29, 30, 31, 33, 34, 35, 38, 40, 43, 53, 58, 65, 87}; Authorization {29, 30, 31, 53, 65}; OutOfOrderSequence {45, 59}. They equal the test's sets. Every xmldoc code, name, class and polarity statement agrees with Java and the header.
- **Case counts.** KafkaExceptionTests 5 → 23, ConsumerGroupMetadataTests 16, and the admin tests 6 → 10: 2326 + 38 = 2364.
- **Mutations**, in a scratch copy on net10.0, over the three CP2 classes plus one probe test (50 cases). Exactly the predicted 4 go red:
  - `FromBorrowedHandle` passing `false` for `IsOutOfOrderSequenceError` reddens the OOS sweep case and table rows 45 and 59;
  - swapping `All()`'s IC and Az arguments reddens Theory row 0.
- **Item 21's absence claim.** Deleting (not retyping) the three readers in a scratch library copy leaves only two CS1574 cref diagnostics.
- **Item 16's counts**: 0 / 29 / 14 / 5 / 1 / 1, the same at both trees.
- **The cites in items 14, 15, 17, 18, 19 and 20.** Gate observation (i)'s table is complete: every other Java cite in the two admin files matches.
- **Item 19's read, at the binding level.** A `NativeConsumer` with `group.protocol=classic` throws Code 35, all six flags false, with the exact message.
- **The amendment script.** `amend-cp2.py`, run on a scratch copy of `ff2f0224`, reproduces `PLAN.dryrun.md` byte for byte, and its hunks equal `amend-cp2.diff`.
- **`commit-cp2.msg` and note 6.** Every count, cite and mutation claim in the message agrees with the diff; its 40 swap-sweep reds are 27 Theory + 8 Fact + 5 list-test. Note 6's sizes: 341 + 49 = 390 changed production lines (242 xmldoc, 73 code), and 470 + 4 + 271 = 745 test lines.

There are two findings:
- 84.10: a Java-fidelity gap in D6's new API at a third rebuild site that the observation-(b) decision did not list;
- 84.11: the Actor's observation (k).

⚠ Local working file, never committed.

---

## Issue 84.10: `TopicMetadataAndConfig`'s accessors throw a `KafkaException` that answers false to all five D6 predicates (with `Code` 0), where Java rethrows the typed exception it holds; the observation-(b) decision and the F2 bullet miss this third rebuild site
- **File**:
  - `bindings/dotnet/src/Confluent.Kafka/Admin/TopicMetadataAndConfig.cs:124-137`: `EnsureSuccess` and its summary.
  - The plan amendment draft (a plan note for the Manager): F2's "Rebuilt failures" bullet (the D6 hunk of `cp2-plan/amend-cp2.diff`) and D15.
- **Severity**: Medium. D6's new public predicates answer differently from Java on a path Java documents. There is no memory-safety impact.
- **Reference**:
  - `CreateTopicsResult.java` at ccacf8a:
    - `:109` declares the field `private final ApiException exception;`.
    - `ensureSuccess()` (`:151-154`) is `if (exception != null) throw exception;`. It throws the held typed exception itself, not a wrapper.
  - `CreateTopicsResult.java:60-62`, repeated at `:74-76`, `:87-89` and `:100-102`: "If broker returned an error for topic configs, throw appropriate exception. For example, `TopicAuthorizationException` is thrown if user does not have permission to describe topic configs."
  - `KafkaException.cs:52-53` (the class remarks, from CP2): "Where Java code tests an error with `instanceof` one of those classes, test the matching property here."
  - The Manager's decision on observation (b) (`COMMENTS.DONE.84.md`, 2026-09-29): "a wrong answer on a shipped surface is a defect in that API. The fix only carries forward values the binding already holds".
- **Description**:
  - **What it holds.** `TopicMetadataAndConfigMarshal.CopyOut` (`:59-63`) stores the metadata failure it reads with `FromBorrowedHandle`, so that failure carries the core's classification.
  - **What it throws.** Each accessor calls `EnsureSuccess`: `TopicId`, `NumPartitions`, `ReplicationFactor` and `Config`, reached from the `CreateTopicsResult` methods of the same names (`CreateTopicsResult.cs:107-136`). `EnsureSuccess` throws `new KafkaException(_exception.Message, _exception)`. That is the public ctor, so `Code` is 0, `IsRetriable` is false and all five predicates are false.
  - **Measured** in a scratch probe (not committed):
    - A failure built with `kafka_common_Error_new(29, …)` and `FromHandle` reads Code 29, with `IsInvalidConfigurationError` and `IsAuthorizationError` true.
    - Each of the four accessors throws Code 0 with all six flags false.
    - The thrown exception's `InnerException` is the held failure, and its `Message` is the same.
  - **The consequence.** On the path where Java documents a `TopicAuthorizationException`, a caller who follows the class remarks reads `IsAuthorizationError == false`. Java's `instanceof AuthorizationException` is true there.
  - **Why the decision missed it.** This is the defect the decision fixed in `AlterConsumerGroupOffsetsResult.All()`. The decision's list came from the compiler probe of the three-argument ctor's callers, which cannot see a rebuild through a public ctor.
  - **No other site.** I swept all 62 `new KafkaException(` / `new SerializationException(` sites in `src/`; 11 take more than one argument. None of the others wraps a failure the core classified:
    - `SendCompletionPump.cs:884` and `SendAccumulator.cs:1341` pass a `KafkaException` cause through unchanged;
    - `DeliveryRegistration.cs:134` wraps only a failure that is not a `KafkaException`;
    - the rest are serde wraps or binding-invariant errors.
  - **A false remark.** The summary at `:125-126` says "Java wraps in `new KafkaException(exception)`". At ccacf8a Java does not wrap it.
  - **What predates CP2.** `Code` 0 and `IsRetriable` false predate CP2; the five predicates are new in CP2.
- **Expected**: the Manager chooses (a) or (b). Either way, F2's lead sentence changes too.
  - **(a) Fix it at CP2, as for `All()`.**
    - `EnsureSuccess` rethrows a fresh exception that carries the held failure's classification and keeps the failure as `InnerException`. For example, give the internal eight-argument ctor a trailing `Exception? innerException = null`, passed to `base(message, innerException)`, and write:
      ```csharp
      throw new KafkaException(
          _exception.Code,
          _exception.Message,
          _exception.IsRetriable,
          _exception.IsTransactionAbortableError,
          _exception.IsApplicationRecoverableError,
          _exception.IsInvalidConfigurationError,
          _exception.IsAuthorizationError,
          _exception.IsOutOfOrderSequenceError,
          _exception);
      ```
    - The summary becomes: "Java's `ensureSuccess()`, which throws the held failure itself (`CreateTopicsResult.java:151-154`). This rethrows it in a fresh `KafkaException` with the same code, message, retriable flag and hierarchy predicates, so each accessor call gets its own stack trace and the original stays available as `InnerException`."
    - Two tests in `PublicAdminCreateTopicsTests.cs`, mirroring the admin pair:
      - A Fact whose failure comes from `kafka_common_Error_new(29, …)`. For each of the four accessors it asserts Code 29, the held message, the held failure's six flags and `Assert.Same(held, thrown.InnerException)`.
      - A Theory over the three bit rows of `All_PassesEachFlagOfTheFirstFailureToItsOwnParameter`, so that a swapped argument fails.
    - F2's bullet gains a sub-bullet: "`TopicMetadataAndConfig`'s accessors pass on the five answers of the failure they hold. Java rethrows that failure itself there (`CreateTopicsResult.java:151-154`)."
  - **(b) Defer it.**
    - F2's bullet gains a sub-bullet: "`TopicMetadataAndConfig`'s accessors leave all five false, and `Code` 0 and `IsRetriable` false. They wrap the failure they hold with the public `(message, innerException)` ctor, where Java rethrows it itself (`CreateTopicsResult.java:151-154`). File-forward item 22."
    - D15 gains item 22, with the measured values, the `:60-62` javadoc and the false "Java wraps" remark.
  - **In both cases.**
    - F2's lead sentence ("An admin result that rethrows a failure under a new message answers the five as the exception Java throws at that site does:") states a universal over a set its source probe cannot enumerate. This site escapes it only because it keeps the message. Following the ffi §A6 round-5 amendment (`ffi-marshalling.md:961-972`), make the lead local, for example "Each admin result below rethrows a failure it holds:".
    - F1's "`gate/CP2.txt`'s compiler probe finds admin results that also build one" can add "and Critic finding 84.10 finds a third".
- **Actual**: as quoted, with the probe values above.

## Issue 84.11: The eight-argument ctor's summary lists its callers and omits the rebuild in `All()` (the Actor's observation (k))
- **File**: `bindings/dotnet/src/Confluent.Kafka/KafkaException.cs:141-148`.
- **Severity**: Doc accuracy (Low).
- **Reference**:
  - `ffi-marshalling.md:961-972`, the §A6 round-5 amendment: state the local fact and do not enumerate the others. "A claim that is not made cannot go stale."
  - The three-argument ctor's summary (`:122-127`) states only its own contract.
- **Description**:
  - The summary says what `FromBorrowedHandle`, `FromHandle` and the three-argument ctor pass, and that each value is either copied out of a `kafka_common_Error_t` handle or `false`.
  - It omits two callers:
    - `AlterConsumerGroupOffsetsResult.All()` (`AlterConsumerGroupOffsetsResult.cs:168-179`) calls the ctor with values copied from another `KafkaException`;
    - the new admin Theory passes it arbitrary flags.
  - By the gate's timeline the summary is older than the first of those callers: the D6 files were final at 17:33:20, the admin file at 23:25:12.
  - 84.10(a) would add a further caller.
- **Expected**:
  ```
  /// <summary>
  /// Initializes a new instance with a code, a message, the retriable flag and the answer
  /// of each other hierarchy predicate, each kept as given. It derives nothing from
  /// <paramref name="code"/>: the binding forwards codes verbatim and never branches on
  /// one (ffi §B5).
  /// </summary>
  ```
  If 84.10(a) adds the `innerException` parameter, the summary names that parameter too.
- **Actual**: as quoted.

### Resolution of 84.10 (Actor, 2026-09-29)
The Manager chose (a).
- **`KafkaException.cs`:** the eight-argument internal ctor takes a trailing `Exception? innerException = null` (:157), passed to `base(message, innerException)` (:158). Its other callers compile unchanged and pass null.
- **`Admin/TopicMetadataAndConfig.cs`, `EnsureSuccess` (:131-146):** it throws a fresh `KafkaException` through that ctor with the held failure's `Code`, `Message`, `IsRetriable` and five predicates, in parameter order, and the held failure as `innerException` (:135-144), as in the snippet above. It is fresh per throw, so a shared instance's stack trace is not overwritten. Its summary (:124-130) now reads: "Java's `ensureSuccess()`, which throws the held failure itself (`CreateTopicsResult.java:151-154`). This throws a fresh `KafkaException` with the held failure's code, message, retriable flag and hierarchy predicates, so each accessor call gets its own stack trace and the held failure stays available as `InnerException`." The "Java wraps in `new KafkaException(exception)`" remark is gone.
- **Tests, `PublicAdminCreateTopicsTests.cs` (18 → 22 cases):**
  - `MetadataAccessors_KeepTheHeldFailuresCoreClassification` (Fact, :227). The held failure is `kafka_common_Error_new(29, "topic authorization failed")` mapped through `FromHandle`, which reads it with `FromBorrowedHandle`, as `TopicMetadataAndConfigMarshal.CopyOut` does. The Fact first asserts the core's answers: Code 29, flags F F F T T F. Then, for `TopicId`, `NumPartitions`, `ReplicationFactor` and `Config`, called on the object and through the `CreateTopicsResult` methods of the same names, it asserts Code 29, the held message, the held failure's six flags and `Assert.Same(held, thrown.InnerException)`.
  - `MetadataAccessors_PassEachFlagOfTheHeldFailureToItsOwnParameter` (Theory, :282, rows 0, 1 and 2). Row b sets flag n of the held failure to bit b of n, the admin Theory's rows, so each pair of the six flag parameters differs in some row. It calls the four accessors on the object.
- **Mutations** (net10.0, filter `PublicAdminCreateTopicsTests.`, 22 cases; each restored cmp-identical):
  - (i) revert to the public `(message, inner)` ctor: 4 red, the Fact and rows 0, 1 and 2;
  - (ii) swap `IsInvalidConfigurationError` and `IsAuthorizationError`: 1 red, row 0 (code 29 answers true to both, so the Fact passes);
  - (iii) drop the inner exception: 4 red, the Fact and rows 0, 1 and 2, each at `Assert.Same`;
  - beyond the three asked for: the 15-swap sweep (35 red entries, 27 Theory and 8 Fact, 0 mismatches against the set predicted from the two parameter numbers); K1, the ctor's `: base(message, innerException)` cut to `: base(message)` (4 red over 55 cases); R1, a rewrap confined to `CreateTopicsResult`'s `Project` (1 red: the Fact, at :255, Expected 29, Actual 0).
- **The `CreateTopicsResult` path.** Its methods reach the accessors through `Apply` (`CreateTopicsResult.cs:145`) and its local async `Project` (:170-172), which awaits the topic's task and then calls the accessor. The Fact covers that path for the four methods (R1 shows its assertions there are live); the Theory calls the accessors directly. Not reached by a unit test: `CopyOut`'s own construction of the held failure from `kafka_admin_TopicMetadataAndConfig_error`. The header declares no constructor for `kafka_admin_TopicMetadataAndConfig_t`, and a grep of `src/admin/mock_admin_client.rs` for `with_error`, `complete_with_error` excluded, exits 1.
- **Re-gated** on the final bytes: G1-G7 (03:13:22-03:14:15); net10.0 and net8.0 2368/2368 (2364 before); net462 build-only; the compiler probe and the reader probe give the same errors as before. The earlier mutation sets, re-run, redden the same sets.
- **Records:** `gate/CP2.txt` (What CP2 adds, a CreateTopics tests section, the mutation section, note 12 extended for the two files outside §4.3's list, observation (b) extended, and observation (m), on the gRPC CreateTopics error arm, which now carries the held code) and `commit-cp2.msg`. PLAN.md is untouched; F1/F2 of the amendment draft are the Manager's.

### Resolution of 84.11 (Actor, 2026-09-29)
- `KafkaException.cs:141-147` is the Critic's contract-only text, extended to name the new parameter:
  ```
  /// <summary>
  /// Initializes a new instance with a code, a message, the retriable flag and the answer
  /// of each other hierarchy predicate, each kept as given, and with
  /// <paramref name="innerException"/> as its <see cref="Exception.InnerException"/>. It
  /// derives nothing from <paramref name="code"/>: the binding forwards codes verbatim and
  /// never branches on one (ffi §B5).
  /// </summary>
  ```
  It names no caller. The 0-warning build (TreatWarningsAsErrors, xmldoc generated) accepts its `cref` and `paramref`.
- The admin Theory's doc said "the internal eight-argument ctor"; it now names the ctor by signature, `KafkaException(int, string?, bool, bool, bool, bool, bool, bool, Exception?)`, as the new CreateTopics Theory's doc does.
- Observation (k) in `gate/CP2.txt` is marked resolved.

---

# COMMENTS.84 — M17/P1, CP2: Manager decisions on 84.10 and the Actor's observation (m)

Recorded by the Manager, 2026-09-29, after the Actor's 84.10 / 84.11 fix and before Critic 84's re-check.
- **84.10: option (a), fixed in CP2.** Checked against Java at ccacf8a before deciding. `CreateTopicsResult.java:109` holds `private final ApiException exception;`, and `ensureSuccess()` (`:151-154`) throws it itself, so Java's `instanceof` answers follow the held failure's class. This is the defect the observation-(b) decision fixed in `AlterConsumerGroupOffsetsResult.All()`, and it gets the same decision for the same reason: the predicates are D6's new public API, and the fix only carries forward values the binding already holds.
- **My miss.** The observation-(b) decision cleared this site because of its own "Java wraps" comment, and I did not read the Java; that comment was false. This is the Critic's rule suggestion 3 (check each binding "Java does X" remark against the Java source at the pinned commit), and it goes to the CP7 RULE-DRAFTs.
- **The plan amendment draft is revised for it** (scratchpad `cp2-plan/amend-cp2.py`, dry-run diff `cp2-plan/amend-cp2.v2.diff`):
  - F1: dated 2026-09-29; it names 84.10's site and items 14-21.
  - F2: a local lead ("Each admin result below throws a new exception built from a failure it holds:") and a `TopicMetadataAndConfig` sub-bullet.
  - F3, from the Critic's "noted, not filed": it names item 11's four Rust-side checks.
  - Item 14, from the same list: the cause Java passes, which .NET drops.
  - Item 20, from the same list: the member doc that now mixes two bases.
- **Observation (m): accepted as a parity improvement; no change.**
  - **What changed.** The gRPC server's CreateTopics error arm copies the exception's `Code` (`grpc-server/AdminServiceImpl.cs:2243-2246`, through `Translate.ToProto`, `Translate.cs:148-157`). It now reports the held failure's code where it reported 0.
  - **Why it is accepted.** The Python harness sends the held error itself (`bindings/python/grpc_translate.py:707-708`), and the Rust harness reads the arm's code (`tests/common/multilanguage_admin.rs:926-927`), so .NET now agrees with Python there. No CP2 file changes for it.
  - **Records and coverage.** The CP2 commit message says so. G10 at CP7 runs every `__grpc_dotnet` test with no `--skip` (PLAN §4.5 step 4), so any multi-language test that reaches that arm is exercised there. Whether one does is not measured here.

---

# COMMENTS.84 — M17/P1, CP2: Critic cycle 8's noted item

Recorded by the Actor, 2026-09-29, on the Manager's direction.
- **Noted, not filed; fixed in CP2 (04:21:26).** `Admin/TopicMetadataAndConfig.cs:72`, the summary of the ctor that takes the held failure, named Java's parameter type as `Throwable`; at ccacf8a it is `ApiException` (`CreateTopicsResult.java:123`; the field, `:109`). Only that line changed. The sweep of the file's other Java remarks and the re-gate are in `gate/CP2.txt` (section "Critic cycle 8"; observations (n) and (o)).
- **Observation (n): resolved in CP2 (05:06:23), on the Manager's decision.** In `Admin/TopicMetadataAndConfig.cs` the remarks (`:25-30`) now name the two cases in which Java builds the failure-holding instance (`KafkaAdminClient.java:1852-1857`): a topic-config error, for example `TOPIC_AUTHORIZATION_FAILED` (29), and a broker too old to report the metadata. They no longer give `validateOnly` as a cause; the type summary (`:20`) and the error ctor's summary (`:71`) say "accepted" where they said "created", and the remarks' first sentence says "accepted" and "accept" where it said "Creation" and "create". Doc comments only; the file keeps its 147 lines. The final re-gate (05:07:51-05:08:52) passed: 2368/2368 on net10.0 and net8.0, 0 warnings and 0 errors, format clean.
- **Observation (o): accepted by the Manager, per M15's rule** (M15/P3 PLAN `:1330-1338`). The final re-gate's net8.0 run passed on its first run.
- **Observations (p) and (q): recorded, not fixed** (the stopping rule). (p): the `<returns>` of `NumPartitions` and `ReplicationFactor` (`:98`, `:107`) say "the topic was created with", which a validate-only request does not satisfy; they describe the success path, which CP2 does not change. (q): the C header's doc of `kafka_admin_TopicMetadataAndConfig_error` (`confluent_kafka.h:2957-2960`, from `src/ffi/admin.rs:2023-2026`) says "the topic creation itself succeeded"; ABI text, for kafka-critic.
- Details are in `gate/CP2.txt` (section "Critic cycle 8"; observations (n) to (q)).

---

# COMMENTS.84 — M17/P1, Critic cycle 10: CP3

Critic N=84 (`dotnet-critic`). **Scope:** checkpoint CP3: PLAN D4 (:460-546), S4 (:1655-1675), the §4.3 CP3 row (:1334), §4.2 and RD13 (:1060), against the plan blob at 8041731b.

**Reviewed:** the uncommitted CP3 worktree on HEAD 8ccb0f8d:
- `src/Confluent.Kafka/Internal/SendCompletionPump.cs` (sha256 486b7acc…, +165/-17);
- `tests/Confluent.Kafka.UnitTests/Interop/SendCompletionPumpBarrierTests.cs` (5793532c…, 831 lines, 7 facts);
- `gate/CP3.txt` (ee445927…);
- `$S/commit-cp3.msg` (a428b03d…).

**Checked with no finding:**
- **D4's invariant.** `RunLoop` takes one FIFO entry at a time and finishes `ProcessGroup` (or the catch's `FaultGroupCompletions`) before it dequeues again. A pass never spans groups, so no `get_all` spans a barrier. `DeliveryRegistration.Fire` calls `OnCompletion` synchronously, so "callbacks returned" holds when the barrier completes.
- **The races.** `Enqueue` and `EnqueueBarrier` both enqueue under `_stopLock`, so their order is linearised by that lock. A barrier that lands between `Stop`'s `_stopping` write and its locked drain is completed by that drain. `_signal.Set` is reached only while `_stopped` is false, and `Stop` sets `_stopped` under the lock before `Dispose`.
- **The reader sweep, redone by hand.** `SendAccumulator` reads no pump state; its only use is `_pump.Enqueue` (:1302). `NativeProducer` reads `DrainedSendCount`, `WaitForQueueDrain`, `CloseGate` and `Stop`. `FaultGroupCompletions` is bounded by `Count`. Every line citation in CP3.txt's table matches the worktree.
- **Interop.** Observation (a) is confirmed by reading the code: the stop path `continue`s before `DestroyFutures`. No barrier reaches `ProcessGroup`, so no `get_all` or `destroy_all` sees count 0 or `Array.Empty`. The header's non-null `futures` requirement (:15641-15647) is therefore never at stake.
- **The tests.** The test doc comments' mutation claims match `cp3/mut/run4.reds` (25 reds; 14 of 15 mutations red). The waits are bounded, there are no timing asserts, and no test uses `WaitAsync`. The mock-FIFO claim holds: `mock_producer.rs` uses `VecDeque`, `push_back` at :1250 and `pop_front` at :321. The null-future claim matches the header. I re-ran the 20 `Interop.SendCompletion*` cases: 20/20 on net10.0 and 20/20 on net8.0.
- **The pump's xmldoc.** The `EnqueueBarrier` wording ("each delivery callback its passes invoked", plus the closed-gate carve-out) is accurate on every path.

⚠ **Numbering.** The brief said to start at 84.11, but `COMMENTS.DONE.84.md:664` already holds an "Issue 84.11" (CP2, the eight-argument ctor). This item is therefore 84.12.

---

## Issue 84.12: The barrier test class states D4's invariant with no exceptions, and two of its own tests assert the exceptions; the commit message repeats it
- **Files**:
  - `bindings/dotnet/tests/Confluent.Kafka.UnitTests/Interop/SendCompletionPumpBarrierTests.cs:33-38`, the class remarks, "The invariant (D4)";
  - `$S/commit-cp3.msg:6-9`, the same sentence.
- **Severity**: Low (a false statement in a test doc comment, RD13 / ffi §A6 round-5). It hides no production defect.
- **Reference**: RD13 (PLAN :1060): "a test's doc comment claims only what its assertions can detect". D4's stop rule (PLAN :478-480): the drain completes a queued barrier with success.
- **Description**: The class remarks say that a barrier "completes only after every group enqueued before it has had its completions read, its delivery callbacks invoked, and its TaskCompletionSources set or faulted". Two facts in the same class assert the opposite:
  - `Stop_WithABarrierQueued_…` (:327). The three sends ahead of the barrier are faulted by `DrainAndFaultRemaining`. No `get_all` reads their completions, and no delivery callback runs (residual 2). The test then asserts that the barrier `RanToCompletion` (:346), and it reads `false` for "barrier completed" at each earlier fault (:356).
  - `EnqueueBarrier_AfterCloseGate_…` (:384). The hold group ahead of the barrier is still unresolved when the test asserts `RanToCompletion` (:398).
  
  The pump's own `EnqueueBarrier` xmldoc (:327-330, :350-356) states the invariant accurately. It says "awaiters completed or faulted, and each delivery callback *its passes* invoked has returned", and it carves out the closed gate. The class remark drops both qualifiers. The commit message (:6-9) makes the same unscoped claim, "its completions read, its delivery callbacks run", in a record.
- **Fix**: Scope the class remark to the running loop, or reuse the pump's wording. Also name the two exceptions: the stop drain faults without a read or a callback and completes the barrier with success, and a closed gate returns a completed task and queues nothing. Make the same change at commit-cp3.msg:6-9.
  
  D4's own invariant sentence (PLAN :467-470) has the same gap next to its stop rule. It is plan text, so it is not filed here; it is a candidate for the amendment that already carries decision (b).

### Resolution of 84.12 (Actor, 2026-09-29)
Fixed; doc comments and the commit message draft only. The Manager asked for this fix so CP3 can close.
- **Class remarks** (`SendCompletionPumpBarrierTests.cs:33-46`): the invariant paragraph is now headed "while the pump runs" and states it in `EnqueueBarrier`'s terms: each of the group's TaskCompletionSources set or faulted, and each delivery callback its passes invoked has returned. A new paragraph states the two exceptions as local facts. `Stop`'s drain faults the sends still queued ahead of a barrier, with no `get_all` read and no delivery callback, and completes the barrier with success. After `CloseGate` or `Stop`, `EnqueueBarrier` queues nothing and returns a completed task, whether or not a group ahead of it is still unresolved. It has no counts and no uniqueness claims. The file grows 831 -> 838 lines (+7 from `:40`); sha256 `b65c7700542602c269233e5f67c2ca52912f02249254d7bd8afe4f165c23c79e`.
- **Commit message** (`$S/commit-cp3.msg:6-14`): scoped the same way.
- **Batched nit, taken**: the comment on `RunLoop`'s barrier branch (`SendCompletionPump.cs:611-613`) now says that a throwing `TrySetResult` is an out-of-memory-class case, and that the barrier then stays pending. The line count is unchanged at 1103, and the numstat is still +165 / -17. sha256 `6fda36b0e222a682083306b42b15d932f3552f117c2993ac74bed70a9f95b023`.
- **Not changed**: D4's invariant sentence in the plan (PLAN :467-470), which is frozen plan text. The Critic flagged it as a candidate for the amendment that carries decision (b).
- **Checks** (12:06:38-12:07:15, `gate/CP3.txt` section "84.12 fix"):
  - Release build: 0 warnings, 0 errors.
  - `dotnet format --verify-no-changes`: clean.
  - `SendCompletionPumpBarrierTests`: 7/7 on net10.0 and 7/7 on net8.0.
  - Both CP2 freeze checks: OK.
  - Index: empty.

---

# Critic 84 — M17/P1 CP4 review (2026-09-29)

Reviewed the CP4 worktree over HEAD 8ccb0f8d: `NativeProducer.cs`, `AsyncMockProducer.cs`, S5-S7, `gate/CP4.txt` and `$S/commit-cp4.msg`. The interop is clean against `target/include/confluent_kafka.h`:
- the send-offsets submit runs nothing that can throw after its P/Invoke returns;
- the transient `ConsumerGroupMetadata_t` is destroyed and the pins are released in `finally` blocks;
- `metadataCap == buffer.Length` holds by construction;
- the sync forms and the mock helpers pass the `SafeProducerHandle`;
- each error handle is freed once.

D2, D3, D4, D5, D8, D10 and D13 hold in the code as written. The mutation counts match `cp4/mut/*.log`. The three items below are one test claim that no assertion can detect, and two doc statements that drop a qualifier.

## Issue 84.13: The D13 test's sync half claims the empty map "reaches the core", and nothing it asserts can detect a sync short-circuit
- **File**: `bindings/dotnet/tests/Confluent.Kafka.UnitTests/Interop/ProducerTransactionCancellationTests.cs:380-384` (summary) and `:414-417` (the sync half).
- **Severity**: Low. The test claims coverage it does not have (RD13). The production code has no short-circuit today.
- **Reference**: RD13 (PLAN :1060): "a test's doc comment claims only what its assertions can detect". D13 (PLAN :970-983): no managed early return on **both** surfaces. The mock checks transaction state first (`MockProducer.java:184-188`) and returns on an empty map at `:194-196`, before `sentOffsets = true`.
- **Description**: The summary says "the sync form also reaches the core, which leaves `SentOffsets()` false as Java's mock does for an empty map". The sync call runs inside an open transaction (begin at `:393`). There, an empty map succeeds in the core and leaves `sentOffsets` false. A managed `if (snapshot.Count == 0) return;` in `SendOffsetsToTransactionWithAccumulatorDrainBound` also succeeds and also leaves it false, because `BeginTransaction` already reset it (S7's `SentOffsets_FollowsTheTransactionLifecycle` asserts false after begin). The sync assertion passes either way.
  
  The mutation run did not probe this form either. P15 (`cp4/mut/muts.py:28`) puts the early return only in `SendOffsetsToTransactionWithCallback`, so the remark's "submit count reads 0" covers the async half alone.
- **Fix**: Make the sync half observe the core, using the state the two outcomes disagree on. Call the sync `SendOffsetsToTransaction` with an empty map **outside** a transaction (after the `CommitTransaction`/`AbortTransaction`, or before `BeginTransaction`), and assert the mock's state error. The mock checks state before the empty-map return, so the call fails, while a managed short-circuit would return success. Then add the sync short-circuit as a mutation. Otherwise, drop "the sync form also reaches the core" from the summary.

## Issue 84.14: `CommitTransactionWithCallback` and `AbortTransactionWithCallback` state D4's guarantee without D5 row 4's exception, which a test in the same checkpoint asserts
- **File**: `bindings/dotnet/src/Confluent.Kafka/Internal/NativeProducer.cs:1323-1326` (commit) and `:1345-1348` (abort), the `<summary>` of the token-only overloads.
- **Severity**: Low. The xmldoc is false in one documented case. CP5 writes the public xmldoc from these workers (§7), so the missing qualifier would carry over.
- **Reference**: D5 row 4 (PLAN :547-560): a token that fires while the Task awaits the D4 barrier completes the Task **successfully**, "and only that call's callback-ordering guarantee is lost". The same pattern was filed as 84.12, where a restated invariant lost its qualifiers.
- **Description**: Both summaries say "On success the returned Task completes only after the delivery callbacks and send Tasks of every send that returned before this call have completed". `CanceledWhileAwaitingTheBarrier_CompletesTheTaskSuccessfully` (`ProducerTransactionCancellationTests.cs:244-268`) asserts the opposite for both operations: `RanToCompletion` with `callback.Returned == 0`. The `SubmitControlOperation` remarks state row 4 correctly (:1382-1386), and `AwaitCompletionBarrier` does as well. The "See SubmitControlOperation" pointer does not make the unconditional "only after" true.
- **Fix**: Qualify the sentence in both summaries, for example "…have completed, unless `cancellationToken` fires during that wait, in which case the Task still completes successfully without that guarantee (D5 row 4)". Carry the qualifier into CP5's public xmldoc.

## Issue 84.15: The transaction-region comment says the Task family returns an "already-faulted Task" for an overlapping control call; that holds only when the drain has nothing to wait for
- **File**: `bindings/dotnet/src/Confluent.Kafka/Internal/NativeProducer.cs:1070-1073`.
- **Severity**: Low. The comment is false in one path, and it is the region's summary of D10.
- **Reference**: D10 (PLAN :850-865). The Actor's own observation (h) in `gate/CP4.txt` says the Task is already faulted when it is returned only if the accumulator is idle: "only a non-idle drain makes it later". The `SubmitControlOperation` remarks (:1373-1377) tie "an already-faulted Task when this returns" to the no-accumulator path.
- **Description**: The comment says the core's rejection "surfaces verbatim — thrown by the blocking family, an already-faulted Task from the Task family, because the core fires the callback inline during the submit". When the accumulator holds records, `SubmitControlOperation` returns the pending `DrainThenSubmitControlOperation` Task. The submit, and with it the inline rejection, happens only after the drain completes. So the caller receives a Task that faults later, not one that is already faulted.
- **Fix**: Scope the sentence to match observation (h). For example: "a faulted Task from the Task family, already faulted on return when the accumulator has nothing to drain, because the core fires the callback inline during the submit".

### Resolution of 84.13 (Actor, 2026-09-29)
Fixed the Critic's way: the sync half now observes the core. The claim is kept.
- **The test** (`ProducerTransactionCancellationTests.cs`, `SendOffsetsToTransaction_WithAnEmptyMap_IsStillSubmitted`): after the in-transaction sync call (still asserting `SentOffsets()` false), the test calls `CommitTransaction()`. It then calls the sync `SendOffsetsToTransaction` with an empty map, with no transaction open, and asserts `KafkaException` with `Code == -4` and `Message == "There is no open transaction."`. The mock core runs `verify_transaction_in_flight` before its empty-map return (`mock_producer.rs:1064`, `:1070-1072`; Java `MockProducer.java:184-188`, `:194-196`), so the core rejects the call. A managed early return would succeed instead.
- **Values measured at run time** (`cp4/fix1315/probe.log`): a first run asserting `int.MinValue` read `Actual: -4`, and the message assertion passed on the next run (`probe2.log`, 1/1). They match S5's abort-with-no-transaction value.
- **Doc comment**: the summary now states only what the two halves detect. The remarks name both mutations.
- **Mutation P20** (`cp4/mut/muts.py`, next to P15): `if (snapshot.Count == 0) { return; }` in `SendOffsetsToTransactionWithAccumulatorDrainBound`, right after the snapshot. Red 1/25: "Assert.Throws() Failure: No exception was thrown", at `:425`. P15 re-run on the new test: still red 1/25, submits `Expected: 1 Actual: 0`, at `:416`. Both restored sha-identical.

### Resolution of 84.14 (Actor, 2026-09-29)
Fixed; xmldoc only. The `<summary>` of both token-only overloads, `CommitTransactionWithCallback(CancellationToken)` and `AbortTransactionWithCallback(CancellationToken)`, now ends the D4 sentence with "unless `cancellationToken` fires during that wait: the `Task` then completes successfully without that ordering (D5 row 4)". These are local facts, with no count or uniqueness claims. CP5 carries the qualifier into the public xmldoc.

### Resolution of 84.15 (Actor, 2026-09-29)
Fixed; comment only. The transaction-region comment now says the Task family returns "a faulted Task". That Task is already faulted when it is returned if the accumulator has nothing to drain, because the core fires the callback inline during the submit. While a drain is pending, the submit and the fault come after it. This matches observation (h).

### Batched wording nits (Critic, unfiled), taken with 84.13-84.15
- (n1) S5 class remarks (`ProducerTransactionDrainTests.cs`): `DrainPendingSends(TimeSpan.Zero)` forces a drain as a side effect only when the accumulator is not idle. `SendAccumulator.DrainPending` arms `_forceDrain` only inside its `while (!IsEmptyAndIdleLocked())` loop.
- (n2) Both `SendOffsetsToTransaction` xmldocs that list exceptions (the sync worker and the async token overload) now list `ArgumentOutOfRangeException`, "A key's partition is negative." `NativeConsumer.SnapshotCommitOffsets` throws it for a negative partition.
- (n3) The `MockCommittedOffset` summary now says "the newest entry for that key in Java's `MockProducer.consumerGroupOffsetsHistory()`, searched latest transaction first". That is the header's meaning (h:16595-16596), in place of "newest first".

**Checks**: see `gate/CP4.txt`, section "84.13-84.15 fix". A diff of `NativeProducer.cs` with `//` lines removed is 0 bytes against the pre-fix bytes. `AsyncMockProducer.cs` is unchanged, so the budget run and the G8 rerun were not needed.

---

# Critic 84 — M17/P1 CP4 fix-round re-review (2026-09-29)

Re-reviewed the 84.13-84.15 fix round against `$S/cp4/fix1315/base/`. 84.13, 84.14, 84.15 and n1-n3 are resolved, and their new text is true:
- `NativeProducer.cs` minus every `//` line is byte-identical to the pre-fix copy.
- P20 is red at `:425` ("No exception was thrown"), 24/25 otherwise. `probe.log` measured -4 (`Actual: -4`), and `probe2.log` passed on the message.
- `mock_producer.rs:1064`, `:1070-1072` match `MockProducer.java:188`, `:194-196`.

One record count is still wrong.

## Issue 84.16: The CP4 records say "19 production mutations run, 18 red"; the runs they list are one more, because P19 was never counted
- **Files**:
  - `bindings/dotnet/design/history/M17/P1-producer-transactions/gate/CP4.txt:210`;
  - `$S/commit-cp4.msg:46`, "19 production mutations, 18 red".
- **Severity**: Low. A false count in a record, about what the tests detect. It hides no code defect.
- **Reference**: The Production list in the same file (`gate/CP4.txt:158-204`), and `$S/cp4/mut/summary.txt`.
- **Description**: The pre-fix text said "18 run, 17 red". That count already left out P19 (run at 12:52:28, 8/25 red, listed at `:199`). The count says it treats P11 and P11b as one mutation. Under that rule, P01-P18 is 18 mutations with 17 red, which is the old text exactly. The fix added one for P20 and did not add P19. The list now holds P01-P20. That is 20 mutations and 19 red with P11/P11b counted once, or 21 runs and 20 red with them counted separately. Neither counting gives 19/18. My CP4 review said the counts matched the logs. That was wrong: I missed P19.
- **Fix**: Make it "20 production mutations run (P11 masked, P11b its unmasked form), 19 red" at `gate/CP4.txt:210`, and "20 production mutations, 19 red" at `commit-cp4.msg:46`.

### Resolution of 84.16 (Actor, 2026-09-29)
Fixed; records only, with no code change and no gate re-run.
- **Recount from the log** (`$S/cp4/mut/summary.txt`, first result per id): 21 runs and 20 red with P11b counted apart; 20 mutations and 19 red with P11b folded into P11. The file's own convention is "(P11 masked, P11b its unmasked form)", which counts them as one. The command and its output are in `gate/CP4.txt`, section "84.13-84.15 fix", item 84.16. The Critic is right that the pre-fix "18 run, 17 red" had already left out P19. My fix then added only P20.
- **`gate/CP4.txt:210`**: now "20 production mutations run (P11 masked, P11b its unmasked form), 19 red;".
- **`$S/commit-cp4.msg:46`**: now "20 production mutations, 19 red".
- **Other copies of the total**: a grep of both files for the stale count finds none. The only remaining hit is the fix section quoting the pre-fix "18 run, 17 red" as history.
- **Nit, taken**: the `NativeProducer.cs` Files line said "17 hunks (-U0)". `git diff -U0 HEAD -- bindings/dotnet/src/Confluent.Kafka/Internal/NativeProducer.cs | grep -c '^@@'` gives 15. The 17 came from a plain `diff -U0` against a `git show HEAD:` copy, which aligns hunks differently. The line now reads "15 hunks (git diff -U0 HEAD)".

# Critic 84 — M17/P1 CP4 close: the 84.16 records fix and the Manager's CP4 plan amendment (2026-09-29)

**84.16: resolved.** At `gate/CP4.txt:210` and `commit-cp4.msg:46`, the text now reads 20 production mutations and 19 red. I re-ran the recount awk from `:374-381` over `$S/cp4/mut/summary.txt`. It prints "21 runs, 20 red; ... 20 mutations, 19 red", the same as the record. On P11/P11b, the file's convention is that P11 is masked and green, so P01-P20 gives 19 red. `git diff -U0 HEAD -- NativeProducer.cs | grep -c '^@@'` gives 15, and numstat gives +821 / -15, which matches `:25`. I grepped `gate/CP4.txt`, `commit-cp4.msg`, `PLAN.md` and `gate/CP3.txt` for any other stated total. The only hit is `:372`, which quotes the pre-fix "18 run, 17 red" as history.

**Plan amendment: 4 of the 7 edits are true against the code and the records.**
- The D4 Rule's row-4 sentence. `AwaitCompletionBarrier` returns normally when the token wins (`NativeProducer.cs:1466-1481`). This agrees with D5's row (`PLAN.md:572`).
- The scoped invariant. `EnqueueBarrier` returns `Task.CompletedTask` when `_stopped` is set (`SendCompletionPump.cs:368-374`), and `CloseGate` sets `_stopped` (`:421`). `DrainAndFaultRemaining` faults the groups ahead of the barrier with no `get_all` and completes the barrier with `TrySetResult(true)` (`:939-963`).
- The `:931-954` cite. It matches HEAD's 955-line base file, where the class runs from `:931` to its closing brace at `:954`.
- The gate/stop bullet. It agrees with S4's bullet at `:1713-1714`.

In D15, items 22, 23, 24 and 26 are also true:
- `TopicMetadataAndConfig.cs:98/:107`, and the `CreateTopicsResult.cs` lines it cites.
- `CreateTopicsOptions.java:41-47`.
- The header doc `h:2957-2958`.
- CP3 note 2.
- `DrainPendingSends` ends in `?? true` (`NativeProducer.cs:1678`).

The "two `PollUntil` loops" count is measured. The only two uses are S5 `:384` and S6 `:261`. Both helpers match `SendAccumulatorTests.cs:1662-1671`: a 2 ms step, and a stop at the first observation.

## Issue 84.17: Risk R9 still says "two sanctioned windows only (§5.1 principle 3)"; the amended principle 3 now sanctions more
- **File**: `bindings/dotnet/design/history/M17/P1-producer-transactions/PLAN.md:2153` (the R9 row of the risk table).
- **Severity**: Low. The plan contradicts itself.
- **Reference**: The amended §5.1 principle 3 (`PLAN.md:1541-1554`). Before the amendment it named two windows, D10's retry loop and S5's hold. It now names four kinds: D10's loop; S5's and S4's holds; S4's direct `Wait(s_deadline)` calls; and the two `PollUntil` loops.
- **Description**: R9's mitigation cites principle 3 for "two sanctioned windows only". The amendment changed what principle 3 sanctions but left R9 as it was. So the risk table now makes a count claim that the principle it cites no longer supports (RD13).
- **Fix**: Don't restate the number. Point R9 at the principle instead, for example "No sleeps; only the timing windows §5.1 principle 3 sanctions".

## Issue 84.18: D15 item 25 asks for a pump comment that already exists
- **File**: `bindings/dotnet/design/history/M17/P1-producer-transactions/PLAN.md:1246-1248`.
- **Severity**: Low. A file-forward item that is false about the code as it stands.
- **Reference**: `SendCompletionPump.cs:611-613` (RunLoop's barrier branch, the bytes staged at CP3), and `COMMENTS.DONE.84.md:792`. There, the Actor recorded this nit as taken in the 84.12 fix round.
- **Description**: Item 25 says the barrier-branch comment "could add that the barrier stays pending if `TrySetResult` throws (OOM-class)". The comment already ends: "If TrySetResult throws (an out-of-memory-class case), the catch below runs FaultGroupCompletions, bounded by Count — 0 for a barrier — so it touches no awaiter, and the barrier stays pending." A later phase that picks up item 25 would find nothing to do.
- **Fix**: Delete item 25 under D15's round-5 rule ("delete a stale claim, do not re-word it"). Then renumber 26, or leave item 25 as a closed marker, whichever D15's convention is.

## Issue 84.19: The amended principle 3 misnames S4's timed holds and leaves out the bounded waits CP4 added, although the header note says it names them
- **Files**:
  - `PLAN.md:1547` ("S4's holds of a withheld group") and `:1549` ("S4's bounded direct `Wait(s_deadline)` calls");
  - `PLAN.md:56-57`, the header note: "§5.1 principle 3 names the bounded waits and polls that CP3 and CP4 added".
- **Severity**: Low. Both statements are false about the tests.
- **Reference**: `gate/CP3.txt` note 3 ("the holds :479 and :808"). These are now `SendCompletionPumpBarrierTests.cs:486` and `:815`, each shifted +7 by the 84.12 remarks. See also the test sources below.
- **Description**:
  - (a) S4's timed holds are `Wait(s_hold)` inside a blocking continuation (`:486`) and inside `ProbeCallback.OnCompletion` (`:815`). The class remarks say so: "A callback or continuation that blocks waits up to s_hold" (`:63-64`). S4's withheld groups, its "hold group" at `:263`, `:397` and `:474`, have no time bound. The test resolves them with `CompleteNext`. So "holds of a withheld group" names the untimed construct and not the timed one.
  - (b) CP4 added the same bounded direct waits in S5 and S6: `entered.Wait(s_deadline)` at `ProducerTransactionDrainTests.cs:330`, `:381`, `:423`, `:464` and `:501`, and at `ProducerTransactionCancellationTests.cs:154` and `:257`. It also added S6's `GatedDeliveryCallback` hold, `_release.Wait(s_hold)` at `:589`. The amended list names the direct waits for S4 only and names no S6 hold. So the header note's "names the bounded waits ... that CP4 added" is false, and read as a list of what is sanctioned, principle 3 leaves out CP4's own tests. CP4 observation (i) raised only the polls, which is probably why these were missed.
- **Fix**:
  - Reword the bullet at `:1547` to "S4's, S5's and S6's holds of a blocking delivery callback or continuation (`Wait(s_hold)`), and S5's barrier-hold windows".
  - Make `:1549` "the bounded direct `Wait(s_deadline)` calls in S4-S6".
  - Or cut the header note back to what the list covers.
  - Give no per-file counts unless they are measured.

## Noted, not filed
- (n1) The brief said the 84.16 fix added `gate/CP4.txt` lines 372-386. `diff` against `CP4.before8416.txt` shows `371a372,384`, which is 13 lines. The record is fine; only the brief's range is off.
- (n2) `gate/CP4.txt:158-159` ("Runs 12:34:31-12:36:25, then P11b, P16+F01 and P19 (12:38-12:52)") does not list the 13:10 P20 and P15 re-runs. The P20 line (`:190`) and the fix section cover them.
- (n3) The header note says "Corrections carried from the CP3 and CP4 reviews", but D15 items 22 and 23 are marked "recorded at the CP2 close".
- (n4) Item 22 says `CreateTopicsResult.cs` has "the same wording" at `:80`, `:94`, `:101`, `:110`, `:119` and `:129`. Those lines say "was created", "has been created" and "once it is created", not "created with". The point stands; only the wording differs.
- (n5) The scoped D4 invariant still says a group ahead of the barrier "has had its completions read, its delivery callbacks invoked". That does not hold if `ProcessGroup` throws past its passes. `FaultGroupCompletions` then only faults the task sources (`:638`). This is the OOM or marshalling-throw case the catch comment names, and the text predates the amendment, so I have not filed it. `EnqueueBarrier`'s xmldoc wording ("each delivery callback its passes invoked has returned") is the exact form if the Manager wants the two to match.

**Resolution (Manager, CP4 close, PLAN.md is Manager-authored):** fixed by
`$S/cp4/amend/fix1719.py`. 84.17: R9 now reads "only the windows §5.1 principle 3
sanctions". 84.18: item 25 deleted (the comment exists at `SendCompletionPump.cs:611-613`);
the former item 26 is now 25 and the header says items 22-25. 84.19: principle 3 names the
kinds — bounded `Wait(s_hold)` holds in a test continuation or callback (S4, S5, S6), bounded
direct `Wait(s_deadline)` calls (S4, S5, S6), and the `PollUntil` loops — and the header note
now says the principle "names the kinds of bounded wait and poll the tests use". Noted n3
(header now says "carried from the CP2 close and the CP3 and CP4 reviews") and n4 (item 22
quotes the three phrasings) taken too; n1, n2, n5 not acted on (records wording / pre-existing).

# Critic 84 — M17/P1 CP4 close: re-check of the 84.17-84.19 plan fix (2026-09-29)

**84.17, 84.18, n3 and n4: resolved.**
- R9 (`PLAN.md:2153`) now reads "only the windows §5.1 principle 3 sanctions" and gives no count.
- Item 25 (the pump comment) is deleted. D15 now ends at item 25, the old item 26 renumbered. A grep for "item 25", "item 26", "items 22-26" and "two sanctioned" finds only the header's "items 22-25" (`:58`).
- The header note now says "carried from the CP2 close and the CP3 and CP4 reviews".
- Item 22 now quotes the three phrasings.

**84.19: partly resolved.** The `Wait(s_hold)` bullet is true for S4 (`:486`, `:815`), S5 (`:608`) and S6 (`:589`). The `Wait(s_deadline)` bullet is true for S4 (`:200`, `:497`), S5 (`:330`, `:381`, `:423`, `:464`, `:501`) and S6 (`:154`, `:257`). The `PollUntil` bullet is true for S5 (`:384`) and S6 (`:261`). None of the three bullets states a count. One kind of window has been dropped, though.

## Issue 84.20: The rewrite drops S5's 500 ms barrier-hold window from principle 3, and S5 still cites principle 3 as its sanction
- **Files**:
  - `bindings/dotnet/design/history/M17/P1-producer-transactions/PLAN.md:1541-1554`, §5.1 principle 3.
  - `PLAN.md:56-57`, the header note: "names the kinds of bounded wait and poll the tests use".
  - `$S/cp4/amend/COMMIT_MSG`, which says "names the kinds of timing window the tests use".
- **Severity**: Low. The plan now contradicts a test that cites it.
- **Reference**:
  - `ProducerTransactionDrainTests.cs:76-78`: "The sanctioned barrier hold window (PLAN §5.1 item 3): it only gives the "no barrier" mutation room to show; the pass condition is the probe", with `s_barrierHold = 500 ms`.
  - It is used as `Assert.NotSame(commit, await Task.WhenAny(commit, Task.Delay(s_barrierHold)))` at `:387`, and the same for abort at `:467`.
  - §5.2 S5, D4 wiring: "the commit `Task` is held for a bounded window".
- **Description**:
  - HEAD's principle 3 named this window as "S5's barrier hold". The first amendment kept it as "S5's barrier holds".
  - The 84.19 rewrite replaced that bullet with "holds: a test continuation or callback that blocks on a bounded `Wait(s_hold)`". The 500 ms window is not such a hold. It is a `Task.Delay` raced against the operation's `Task`, and it sits on the test thread.
  - The only `Task.Delay` principle 3 now excuses is the `PollUntil` poll step ("Their `Task.Delay` is a poll step, not synchronization").
  - So principle 3 no longer sanctions the window that S5's comment attributes to it. The header note and COMMIT_MSG then claim a list of every kind the tests use, which does not hold without that window.
- **Fix**: Restore it as a fourth bullet, for example "S5's barrier-hold window: a bounded `Task.Delay` (500 ms) raced against the commit or abort `Task`, which gives the no-barrier mutation room to show; the pass condition is the probe". Carry the same kind into COMMIT_MSG's list.

## Noted, not filed
- (n6) The holds bullet says each `Wait(s_hold)` hold is "a window that gives a mutation room to show". The hold (60 s) is chosen to outlast `s_deadline`, so that a mutation that stalls behind it fails at the deadline (`SendCompletionPumpBarrierTests.cs:63-65`, `ProducerTransactionDrainTests.cs:68-69`). "A bound that outlasts the deadline" would be closer, but the current wording is not false.

**Resolution (Manager, CP4 close):** principle 3 lists S5's barrier-hold window again as
its own bullet (`Task.WhenAny` against `Task.Delay(s_barrierHold)`, raced on the test
thread, `ProducerTransactionDrainTests.cs:387`, `:467`); COMMIT_MSG's list names it. Noted
n6 taken: the holds bullet now says "a bound chosen to outlast `s_deadline`".

# Critic 84 — M17/P1 CP5: the production surface and xmldoc (2026-09-29)

Scope: the CP5 diff of `IProducer.cs`, `IAsyncProducer.cs`, `KafkaProducer.cs`, `AsyncKafkaProducer.cs`, `MockProducer.cs`, `AsyncMockProducer.cs` (CP5 part), `IDeliveryCallback.cs` (X5 pointer) and `KafkaException.cs` (X2 pointers). Test files are the other Critic's.

Checked and true:
- **D1.** Five members on each interface. The signatures match D1, and every `CancellationToken` defaults to `default`. `BeginTransaction` is `void` on both interfaces.
- **Forwarders.** All 20 forwarders call the right CP4 worker. Each async one calls the `(…, ct)` overload, which passes the real submit.
- **D2 order.** It holds on both surfaces: `SnapshotSendOffsets` runs first, then `ThrowIfClosed`, then the token (`SubmitControlOperation`), then the drain.
- **D8.** The helpers are public on both mocks. Validation order and messages match D8: `NativeProducer.cs:1799-1920`.
- **X2.** The literal form is exact, then the superset sentence, then the -2 note. The five extra codes {22, 25, 47, 49, 82} match `error.rs:1806-1817`, and none of them is an OOS or Authorization code.
- **X3.** It does not call sends inside a transaction unsupported. The sync interface has the "nothing is buffered" sentence.
- **X4 / X6.** Both carry D5 row 4 exactly, and neither promises ordering across close or dispose.
- **X5.** R-a and R-b agree with `SendCompletionPump.EnqueueBarrier`'s remarks.
- **X7.** The message is byte-equal to `DrainAccumulatorWithin`'s template with 30 and `beginTransaction()`.
- **X12.** The precondition is present.
- **X13.** The remarks are present.
- **Java citations.** `Producer.java:45-66`, `KafkaProducer.java:209-216`, `:635`, `:732-737` and `:754-755` all resolve.
- **Stale sentences.** None in source (`--exclude-dir=bin --exclude-dir=obj`), and no mock remark says transactions are unavailable.
- **Interop.** No P/Invoke or handle use in the CP5 files. `static extern` count is 715.

## Issue 84.21: CP5's `BeginTransaction` makes "the one identical member, Metrics" and "one shared member" false on both interfaces
- **Files**: `IProducer.cs:40` and `IAsyncProducer.cs:42` ("the one identical member, `Metrics`, is simply declared on both"); `IProducer.cs:250` and `IAsyncProducer.cs:336`, in `Metrics()`'s remarks ("one shared member does not justify introducing one").
- **Severity**: Low. Public xmldoc: a uniqueness claim and a count claim that this checkpoint made false.
- **Reference**: D1 (`PLAN.md:235-284`) declares `void BeginTransaction();` on both interfaces separately. The declarations are `IProducer.cs:279` and `IAsyncProducer.cs:384`, and their signatures are identical. ffi §A6 round-5 amendment (`bindings/dotnet/.claude/rules/ffi-marshalling.md:961-973`): no uniqueness quantifier and no count.
- **Description**: Both interfaces now declare `BeginTransaction()` with the same signature, so `Metrics` is no longer "the one identical member". Two members are now shared, so the "one shared member" rationale for having no `IProducerCommon` is false as well. CP5 edited the paragraph next to it (`IAsyncProducer.cs:46-50`) but left these sentences as they were.
- **Fix**: Delete the quantifiers and don't re-scope them (round 5). For example, the class remarks could say that the members whose signatures match are declared on both (M11/P8 D-6), with no "the one". In `Metrics()`'s remarks, drop "and one shared member does not justify introducing one", or give the reason without a count.

## Issue 84.22: `SentOffsets()` says "Only `BeginTransaction` resets it", but `Clear()` resets it too, on both mocks
- **Files**: `MockProducer.cs:290-296` and `AsyncMockProducer.cs:357-363`: "Only <c>BeginTransaction</c> resets it", and the `<returns>` "since the last <c>BeginTransaction</c>". Secondary: `CommittedOffset`'s summary "or <see langword="null"/> if none did" (`MockProducer.cs:301-302`, `AsyncMockProducer.cs:368-369`).
- **Severity**: Low. The public xmldoc is false about behaviour a caller can reach through a public member.
- **Reference**: Java `MockProducer.clear()` sets `this.sentOffsets = false` (`MockProducer.java:493`) and clears `consumerGroupOffsets` (`:495`). The core mirrors it: `mock_producer.rs:734` (`inner.sent_offsets = false`) and `:736` (`consumer_group_offsets.clear()`). `Clear()` on both mocks reaches it through `NativeProducer.MockClear` → `kafka_producer_MockProducer_clear` → `mock.clear()` (`src/ffi/producer.rs:4325`). `initTransactions` also resets the flag (`mock_producer.rs:1004`, Java `:158`), but that cannot be observed: the flag cannot be true before a successful init, and a second init is rejected first (`:991-995`).
- **Description**: Take a transaction that sends offsets and commits, so `SentOffsets()` is true. `Clear()` then makes it false, although no `BeginTransaction` ran. The same `Clear()` makes `CommittedOffset` return `null` for offsets that committed transactions did send. The plan's X11 wording ("reset only by `BeginTransaction`", `PLAN.md:2086`) comes from MPT 464, which compares begin with commit. The xmldoc turned that into an exclusive claim over every member.
- **Fix**: "`BeginTransaction` and `Clear()` reset it; a commit does not." Say the same in `<returns>`. In `CommittedOffset`, add that `Clear()` forgets the committed offsets (Java-faithful). Amend X11's wording at `PLAN.md:2086` to match.

## Issue 84.23: The X4 remarks count the exceptions to the ordering guarantee ("One exception"), then list more
- **Files**: `IAsyncProducer.cs:432` (`CommitTransaction` remarks) and `:458-459` (`AbortTransaction` remarks): "One exception: if `cancellationToken` fires …".
- **Severity**: Low. A count claim in public xmldoc that the next sentences contradict.
- **Reference**: ffi §A6 round-5 amendment (`ffi-marshalling.md:961-973`): no count of residuals outside the canonical enumeration. X5's canonical enumeration is the `IAsyncProducer` remarks (`PLAN.md:2080`).
- **Description**: Each paragraph says "One exception" for D5 row 4. It then adds a second carve-out ("nothing is promised for a call that races `Close` … or disposal") and points at three more (R-a to R-c). So the ordering guarantee has at least five exceptions, and the only number the text states is 1. This is the kind of count the round-5 rule deletes, because it goes stale when the set is re-partitioned.
- **Fix**: Delete the count and keep the fact. For example: "If `cancellationToken` fires while this call waits for those completions, the `Task` completes successfully without them."

## Noted, not filed
- (n1) X8 (`IProducer.cs` / `IAsyncProducer.cs` remarks, "reports its group-metadata or no-transaction error first"). For a *null* `groupMetadata`, the binding also reports `ArgumentNullException` before the closed check, as Java does. Only Java's generation>0 / unknown-member check is overtaken. "Its generation or no-transaction error" would be exact.
- (n2) The R-b premise, "A delivery callback runs on the producer's send-completion pump thread". `IDeliveryCallback`'s own remarks say some callbacks run on the send-batch thread. A callback there that blocks on commit or abort also deadlocks, through the D3 drain rather than the barrier. The conclusion is unchanged.
- (n3) X6 "Already canceled: … thrown before anything is done". D2's argument and closed checks run before it. That is harmless, and D5 row 1 says the same.
- (n4) `MockProducer.cs:36-37` lists the `IProducer` surface in parentheses as "(`Send` / `Flush` / `PartitionsFor` / `Close`)". The list already left out `Metrics`, and now leaves out the transaction members. Nothing in it is false.
- (n5) `IProducer.SendOffsetsToTransaction`'s summary says "The map is copied before this does anything else". The `groupMetadata` null check runs first (D2 step 1). The async twin's wording ("copied before this returns") is exact.
- (n6) Observation (b) is accepted. The source-scope grep is clean, and the only hits are in the ignored `grpc-server/bin` output.
- (n7) Records: I found no claim in `gate/CP5.txt` or `commit-cp5.msg` that is false about behaviour. The production +590/-3, the 185 total (29+19+26+26+54+2+2+27), the 38 mutations (34 production + 4 fixture), 38 doc IDs (10+20+8) and 715 all reconcile.

**Resolution (Actor, CP5 fix round):**
- 84.21: the quantifiers are deleted and not re-counted. The class remarks of both interfaces
  now read "… have different signatures on the sync vs async interface, and the members whose
  signatures match are simply declared on both (M11/P8 decision D-6)". `Metrics()`'s remarks
  drop "and one shared member does not justify introducing one" on both interfaces.
- 84.22: on both mocks, `SentOffsets()` now reads "`BeginTransaction` and `Clear()` reset it; a
  commit does not", and `<returns>` says "since the last `BeginTransaction` or `Clear()`".
  `CommittedOffset` adds "`Clear()` forgets every committed offset, as Java's `clear()` does"
  (`mock_producer.rs:734`, `:736`; `MockProducer.java:493`, `:495`). PLAN.md X11 is the
  Manager's amendment and was not edited.
- 84.23: "One exception:" is deleted at both sites in `IAsyncProducer.cs` (`CommitTransaction`
  and `AbortTransaction` remarks). The sentence stays: "If `cancellationToken` fires while this
  call waits for those completions, the `Task` completes successfully without them."
- (n1) X8 on both interfaces now says "reports its generation or no-transaction error first".
- (n2) R-b now reads "A delivery callback that blocks on `CommitTransaction` or
  `AbortTransaction` deadlocks: it runs on a thread the producer owns, and the commit or abort
  waits for that thread." There is no pump-thread claim, and it still agrees with
  `SendCompletionPump.EnqueueBarrier`'s R-b.
- (n3) X6 now says "is thrown once the argument and closed checks pass, and nothing else is
  done".
- (n5) The sync `SendOffsetsToTransaction` summary now says "The map is copied before the core
  is called".
- (n4), (n6), (n7): no change. Each was noted as not false.

# Critic 84 — M17/P1 CP5: the tests half (2026-09-29)

Scope: the eight new test files, gate/CP5.txt, `$S/cp5/mut`, `$S/cp5/r10*` and `$S/commit-cp5.msg`, as far as they concern the tests. Measured in a scratch copy of `src/` and `tests/Confluent.Kafka.UnitTests` (file sha256s equal to CP5.txt's), with `target/` symlinked. The worktree was not built.

Checked and true:
- **Coverage (DoD #3).** Every §5.3.1, §5.3.3 and §5.4 row that maps to S3, S8-S12 is translated, or has the plan's N/A reason:
  - MPT: 330/364 are async only (R-c); B1 and B3 are N/A; 638/645 are Q24.
  - KPT: 222 is partial. Every valid and invalid combination of 238/341/413 is present, and so are 1329, 1944, 1950, 2054 and 2163.
  - Python: 922-1202, 1246-1380, 1395 and 1417-1480.
  - The D6 table rows match D6's table cell for cell.
- **Flavours (§5.1 principle 1).**
  - The S3 preconditions run on all four.
  - KPT 1950 (-3) runs on both real producers, and its accepting counterpart on both mocks. This matches Java's `MockProducer.sendOffsetsToTransaction`, which has no generation check (`MockProducer.java:183-199`), so observation (g) holds.
  - The helpers and the MPT rows run on both mocks, and the S10-S12 rows on both real producers.
- **Core values.** The eight classes pass 185/185 on net10.0 in the scratch copy, so every pinned code and message was re-measured. xUnit skips one duplicate row in each flavour: KPT 413 invalidProps1 has the same input as D9 row 5 (see the notes). The 54 cases for S10 count after that skip.
- **Mutation counts.** There are 38 mutations: 34 production (PK1-5, PA1-8, PM1-5, PN1-8, PH1-4, PI1-4) and 4 fixture (F01-F04). All 38 are red. Every per-mutation count in CP5.txt equals the number of distinct red methods in `summary.txt`, and each log says "Total tests: 185". The exception is the race attribution (84.26).
- **R10.** 50/50 in `r10/run-*.log`, and S12 sync is 50/50 in `r10t/`. Each log shows "Passed: 1, … Total: 1".
- **D10 fixture fix.** `RetryUntilRefused` is correct, and I found no race left in it:
  - A probe `CommitTransaction` that holds the slot makes the worker return -2 with the core unchanged. The worker is then restarted, and `sw` is not reset.
  - A -2 on the probe can only come from the worker, because the probes run one at a time on the test thread.
  - A probe that arrives first fails -4 as an App-side invalid transition, and the state does not change.
- **Timing.** The only sleeps are D10's 50 ms step and S9's 2 ms `DriveUntilResolved` step. There are no memory witnesses in the eight files.
- **DoD #12.** Each seam replaces exactly one thing:
  - The send-offsets fake forwards to `NativeMethods.ProducerSendOffsetsToTransactionAsync`.
  - S9 fails sends through production's `ErrorNext`.
  - The adapters call only public members.
- **S3 snapshot window.** A probe shows that on the async mock the `SendOffsetsToTransaction` Task is still pending when the call returns, with the awaited send ahead of it (3/3 runs). So the doc's "before its Task is awaited" is real.

## Issue 84.24: S12's async test reads `handle.IsClosed` before the dispatcher thread's `DangerousRelease` is ordered with it; a 20 ms pause in production's release path turns it red
- **File**: `bindings/dotnet/tests/Confluent.Kafka.UnitTests/PublicProducerTransactionTeardownTests.cs:61` (`Dispose_WhileInitTransactionsIsInFlight_Returns_AndTheTaskCompletesOnce`).
- **Severity**: Medium. This is a flake in a correct-production test. The Manager's R10 standard treats any such failure as a redesign item.
- **Reference**:
  - `ProducerCallbacks.OnOperation` (`Internal/Interop/ProducerCallbacks.cs:76-95`) runs `context.Complete(error)`, which calls `TrySetException` (`OperationCompletionSource.cs:201`), then `FreeGcHandle` in `finally`, which calls `DangerousRelease` (`:290`).
  - The TCS is `RunContinuationsAsynchronously`, so the test's continuation runs on a pool thread, concurrently with the dispatcher thread's `finally`.
  - Header: the async control callback "fires on the producer's dispatcher thread".
  - Precedent: `Interop/ProducerTransactionCancellationTests.cs:219-225` and `AdminOperationLifetimeTests.cs:98-114` read `IsClosed` right after firing the trampoline *on the test thread*, so their read is ordered after the release.
- **Description**:
  - A scratch probe (3 runs) shows that `Dispose()` returns 2-104 ms after `InitTransactions()` is called, with `init.IsCompleted == false` and `handle.IsClosed == false`. `Producer_close` does not wait for the in-flight init.
  - The last reference is therefore the span-the-op one, released on the dispatcher thread *after* the Task has faulted. `IsClosed` is set by that release's CAS.
  - The test awaits the Task and then reads `IsClosed`. Nothing orders that read after the dispatcher's `finally`. The test is green today only because the dispatcher thread usually finishes a few instructions before the pool continuation reaches `:61`.
  - Demonstration, in the scratch copy only: I added `Thread.Sleep(20)` before `_handleRef?.DangerousRelease()` in `FreeGcHandle`. The async test failed with "the producer handle is still open after Dispose and the completion", and the sync twin passed. The file was then restored and `cmp`-identical.
  - An OS preemption of the dispatcher thread at that point produces the same red.
  - The sync twin (`:83`) is sound. The P/Invoke marshaller releases its reference before the worker's Task completes, and `Dispose` has returned by then.
- **Fix**: Wait for the release instead of assuming it is done. The simplest fix is a bounded poll: `PollUntil(() => handle.IsClosed)` with the 2 ms step and 30 s bound of S5's and S6's `PollUntil`, stopping at the first observation. It needs one line in §5.1 principle 3 (a Manager edit) naming S12's release poll. Keep the existing message as the failure text.

## Issue 84.25: Two error assertions check only the type, and the commit message says every one checks Code and the exact Message
- **Files**:
  - `PublicProducerTransactionConcurrencyTests.cs:89`: the S11 sync worker's outcome, `await Assert.ThrowsAsync<KafkaException>(…worker…)`, with the result discarded.
  - `PublicProducerIdempotenceTests.cs:267`: KPT 2054's init timeout, same shape.
  - `$S/commit-cp5.msg:48-49`: "Every error assertion checks Code plus the exact Message".
- **Severity**: Low.
- **Reference**: §5.1 principle 2 ("Every error assertion checks `Code` and the full `Message` with ordinal equality"); DoD §3.
- **Description**: Both outcomes are deterministic and already measured:
  - The S11 worker is inside the core when the -2 is observed, and ends with the core's timeout, Code 7, "Timeout expired after 5000ms while awaiting InitProducerId. …". The same text family is pinned in S10 and S12.
  - KPT 2054's first call is the 2000 ms timeout, which the adjacent 1329 test pins.

  Neither assertion would notice a wrong code, such as the worker failing -4 because it ran a different operation. The S11 sync test's doc ("the worker then fails with a `KafkaException`") claims only the type, so its xmldoc is accurate. The defect is the principle-2 gap and the false claim in the record.
- **Fix**:
  - At both sites, keep the returned `KafkaException` and assert `Code == 7` and the exact message: the 5000 ms form in S11 and the existing `InitTimedOut` constant in S10.
  - Alternatively, weaken `commit-cp5.msg:48`. The first fix is the one principle 2 asks for.

## Issue 84.26: CP5.txt attributes two mutation reds to the pre-fix race, but the logs show three (PN4 as well), so PN4's own count is 13, not 14
- **Files**:
  - `gate/CP5.txt:226` ("PN4 SendOffsets drops the offsets: 14").
  - `:234-237` ("Two reds were caused by the race … PH3 and PI1 … Each of those two mutations still has 4 reds that are its own").
  - `:245-247` (the restart branch "is exercised only when the race hits (1 in 60 in the probe)").
- **Severity**: Low. A false count in a record, about what the tests detect.
- **Reference**: `$S/cp5/mut/PN4.log:225-229`. `CommitTransaction_WhileInitTransactionsIsInTheCore_IsRefused_Sync` fails with `Assert.NotNull() Failure: Value is null` at `ConcurrencyTests.cs:line 88`, after 55 ms. This is the same signature as `PH3.log:160` and `PI1.log:125`. PN4 mutates `AsyncMockProducer.SendOffsetsToTransaction`, which a `KafkaProducer` test cannot reach.
- **Description**:
  - Three of the 38 harness runs (PH3, PI1 and PN4), all on the pre-fix fixture at 14:08-14:17, hit the race. So PN4 has 13 reds of its own.
  - The rate matters to the stated reason for not mutation-checking the restart branch. `race-probe.txt` shows its one hit at iteration 0, the cold first call. Every test run is such a first call. The in-suite hit rate before the fix was therefore about 3 in 38 runs (plus 1 in the 5 gate repeats), not 1 in 60.
  - At that rate, a 50-run loop over a mutated restart branch, like the R10 loop, would very likely show red. "Would turn red only by chance" understates what was available.
- **Fix**:
  - At `:226`, write "PN4 …: 14 (13 its own; see below)".
  - At `:234-237`, write "Three reds were caused by the race: the red lists for PH3, PI1 and PN4 include the S11 sync twin … PH3 and PI1 each have 4 reds of their own, PN4 has 13".
  - At `:246`, add the in-suite rate (3 of 38 harness runs) next to the probe's 1 in 60.
  - Optionally, run the restart-branch mutation through the R10 loop and record the result.

## Noted, not filed
- `gate/CP5.txt:351-352`: "(… cp5/r10/race-probe.txt; the file was deleted afterwards)". `race-probe.txt` still exists (13,083 bytes). Presumably the deleted file is the probe's source. Name it.
- §5.1 principle 3 does not list S9's `DriveUntilResolved` (a 2 ms step with a 30 s Stopwatch bound, `ErrorTests.cs:215`). S9's own bullet names the pattern, and CP5.txt says "both sanctioned" on that basis. This is the same list-completeness gap as 84.19, for the Manager's next amendment. If 84.24's poll is added, name it in the same edit.
- `PublicProducerIdempotenceTests.cs:93` and `:106`: KPT 413 invalidProps1 is textually the same row as D9 row 5 (`max.in.flight.requests.per.connection=6`). xUnit logs "Skipping test case with duplicate ID" for it in each flavour, so the row labelled KPT 413 never runs. The coverage is identical, but the label is misleading. Either comment that the row is shared or drop it.
- S12 applies the plan's "3 s + 30 s + slack" bound (43 s) to each of its two sequential waits, so the worst case is 86 s. `PublicProducerTransactionTeardownTests.cs` also has no `try/finally`, unlike S11: a failed early assertion (`:53`, `:76`) leaks a live native producer with an operation in flight.

**Resolution (Actor, CP5 fix round):**
- 84.24: S12's async test replaces the immediate `handle.IsClosed` assertion with
  `PollUntil(() => handle.IsClosed, 30 s, <the same message>)`, which uses a 2 ms step and
  stops at the first observation. The failure text is unchanged.
  - Mutation R24a (`OperationCompletionSource.FreeGcHandle` never calls `DangerousRelease`):
    red, the async test only, with "the producer handle is still open after Dispose and the
    completion" after the 30 s bound.
  - R24b (the Critic's `Thread.Sleep(20)` before the release): green, 2/2.
  - Both restores were sha-identical (9476ee04bdeb).
  - The §5.1 principle 3 line is the Manager's.
- 84.25: both sites now keep the `KafkaException` and assert Code 7 and the exact message.
  - S11 sync worker: the 5000 ms form, a new `InitTimedOut` constant.
  - KPT 2054: S10's existing 2000 ms `InitTimedOut`.
  - Both values were measured: the three changed classes passed 60/60 on their first run with
    these values (`cp5/fix2126/first.log`).
  - Re-check of "every error assertion" over the eight files: every captured `KafkaException`
    has a Code and a Message assertion. That includes the helpers `AssertCoreError`,
    `AssertCoreErrorAsync`, `AssertFails` and S9's two returns, whose callers assert both.
  - One more site was a signal-only capture: S12 sync's `refused`, which asserted only
    `NotNull`. It now asserts -2 and the D10 message too.
  - The remaining `Assert.Throws` sites are `ObjectDisposedException`,
    `OperationCanceledException` and `ArgumentNullException` preconditions, which have no Code.
    The commit message now says "Every `KafkaException` assertion checks Code plus the exact
    Message".
- 84.26: CP5.txt is corrected.
  - PN4 is 14, of which 13 are its own.
  - Three race reds: PH3, PI1 and PN4.
  - The rate: 3 of 38 harness runs pre-fix, plus 1 of the 5 gate repeats; the probe's 1 in 60
    was a longer loop with its one hit on the cold first call.
  - The restart branch is now mutation-checked. With the branch changed to `return (null,
    worker)` (`cp5/fix2126/restart/`), the S11 sync twin ran 50 separate times: 22 red, 28
    green, every red "Assert.NotNull() Failure: Value is null". The restore was sha-identical
    (e2e56cfdb66f).
- Nits:
  - CP5.txt now says the deleted file was the probe's source, `ZzProbeRace.cs`, and that
    `race-probe.txt` is its output and was kept.
  - KPT 413 invalidProps1: each invalid row now carries a source label (D9 row n / KPT 238, 341
    or 413 invalidPropsN) as a theory parameter, so D9 row 5 and KPT 413 invalidProps1 are
    separate cases and both run. xUnit reports no duplicate ID, and S10 goes from 54 to 56 cases.
  - S12: both tests wrap their body in try/finally with a bounded `Dispose`, as S11 does.
    `Dispose` is idempotent (`NativeProducer.Dispose` latch).
  - The principle 3 completeness notes (S9's `DriveUntilResolved`, S12's poll) are the
    Manager's.

# Critic 84 — M17/P1 CP5 close: re-check of the 84.21-84.23 / n1-n5 fixes and the Manager's CP5 plan amendment (2026-09-29)

**Fixes: all true, and there is no new count or uniqueness claim.** The sha256s of `IProducer.cs`, `IAsyncProducer.cs`, `MockProducer.cs` and `AsyncMockProducer.cs` match `gate/CP5.txt`.

- **84.21.** Both interfaces now say "the members whose signatures match are simply declared on both". That is true: `Metrics()` and `BeginTransaction()`. It has no quantifier, and `Metrics()`'s remarks no longer count.
- **84.22.** `SentOffsets()` and its `<returns>` now name `Clear()`. `CommittedOffset`'s "`Clear()` forgets every committed offset" is true (`mock_producer.rs:734`, `:736`; `MockProducer.java:493`, `:495`).
- **84.23.** "One exception:" is gone at both sites.
- **n1.** "generation or no-transaction error" is exact.
- **n2.** R-b now says "it runs on a thread the producer owns, and the commit or abort waits for that thread". That is true for both of the threads `IDeliveryCallback` names for the async surface (the pump and the send-batch thread, `IDeliveryCallback.cs:48-50`, `:275-276`). No async callback site runs on the caller's thread.
- **n3.** True.
- **n5.** "copied before the core is called" is true.
- **Quantifier grep.** I grepped the public xmldoc outside `Internal/` for "one shared", "one identical", "One exception", "only … member", "sole" and "every other". The remaining hits are all unrelated to the producer members: `IConsumer`, `IAsyncConsumer`, `IAdmin:44`, the Admin options classes, `IProducer.cs:185` "one shared callback instance" and `IDeliveryCallback.cs:74`. `KafkaProducer.cs:36` ("only the async Send" starts a pump) and `IAsyncProducer.cs:152` still hold. "Three cases" is at X5's canonical home.

**Plan amendment: true, except for two items.**
- **X11.** Matches the code and the new xmldoc.
- **Principle 3.** Both additions match the tests:
  - S12's `PollUntil(() => handle.IsClosed, s_releaseBound = 30 s)` uses a 2 ms `Task.Delay` step and stops at the first observation (`TeardownTests.cs:39`, `:70`, `:110-118`).
  - S9's `DriveUntilResolved` uses a 2 ms `Thread.Sleep` step bounded by `s_deadline = 30 s` and stops when `ErrorNext` returns true (`ErrorTests.cs:36`, `:205-217`).
  - Every `Thread.Sleep` / `Task.Delay` in the eight CP5 files is now named: those two, plus S11's `s_retryStep`.
- **RD4.** Re-measured: `command grep -rln 'Obsolete(' --include='*.cs' bindings/dotnet/src` gives 12 files, 10 of them under `Admin/`, plus `ConsumerGroupState.cs` and `ConsumerGroupMetadata.cs`. The same count holds with `bin`/`obj` excluded. Of the attributes' targets, only `ConsumerGroupMetadata.cs:76` and `:102` are constructors; the rest are types, methods and properties. So "first `[Obsolete]` constructors" is true.
- **Header note.** It matches the four edits.

## Issue 84.27: D8's `sentOffsets()` row still says "reset only by `BeginTransaction`", which the amended X11 now contradicts
- **File**: `bindings/dotnet/design/history/M17/P1-producer-transactions/PLAN.md:773`, the D8 table's `sentOffsets()` row: "Semantics are the core's: reset only by `BeginTransaction`, not by commit (MPT 464)."
- **Severity**: Low. The plan contradicts itself, and the row is false about the core.
- **Reference**: The amended X11 (`PLAN.md:2102`: "reset by `BeginTransaction` and by `Clear()`"). 84.22's evidence: `mock_producer.rs:734`, `MockProducer.java:493`. The new xmldoc: `MockProducer.cs:292`, `AsyncMockProducer.cs:359`.
- **Description**: The amendment fixed X11 but not D8, where the claim started. A reader of D8, the decision X11 documents, still gets the exclusive claim that 84.22 showed to be false. It is the same pattern as 84.17: an amended sentence with a stale twin elsewhere in the plan.
- **Fix**: Make D8's row say "reset by `BeginTransaction` and by `Clear()`, not by commit (MPT 464)", and mark it "⚠ amended at the CP5 review" like the other edits.

## Issue 84.28: S3's amended bullet says the snapshot is witnessed "while the async call's `Task` is still pending"; the test does not establish that, and its own "so the call drains" is false
- **Files**:
  - `PLAN.md:1706-1711`, the S3 snapshot bullet.
  - `tests/Confluent.Kafka.UnitTests/PublicProducerSendOffsetsToTransactionTests.cs:195-218`: `MutatingTheOffsetsAfterTheCallReturns_DoesNotChangeWhatIsCommitted`, whose summary says "with a send ahead of it so the call drains".
- **Severity**: Low. A record claim about what a test witnesses that the test's assertions do not detect (RD13).
- **Reference**:
  - The test awaits the send first (`await Run(subject.Send())`, `:207`). So when `SendOffsetsToTransaction` runs, the accumulator is normally empty and idle, and `DrainPendingAsync` returns `s_alreadyDrained` at once (`SendAccumulator.cs`, the `IsEmptyAndIdleLocked()` fast path).
  - After that, the returned `Task` is pending only because the core completes the async operation on a spawned task (`src/ffi/producer.rs` async control path).
  - Nothing asserts `!sent.IsCompleted` before the two mutations (`:211-213`).
- **Description**:
  - The call does not drain. The "while pending" witness is a likely scheduling outcome, not an asserted state.
  - If the core's callback won the race, the test would mutate a completed call and still pass. That makes it the same test as "after the call returns".
  - The plan bullet now records the pending witness as established.
- **Fix**, either:
  - add `Assert.False(sent.IsCompleted, …)` before the mutations and stop claiming a drain; or
  - drop "while the async call's `Task` is still pending" from the plan bullet and "so the call drains" from the test summary, leaving "after the call returns, before its `Task` is awaited" plus the submit-seam test.

## Noted, not filed
- (n8) The header note says X11 "names `Clear()` as a second reset". `InitTransactions` also resets `sent_offsets` (`mock_producer.rs:1004`), although that cannot be observed. "Names `Clear()` as a reset too" avoids the count.
- (n9) The header note puts S9's `DriveUntilResolved` and S12's poll under one "(Critic finding 84.24)". Only S12's poll comes from 84.24. S9's came from a noted item, and its in-place marker correctly cites no finding.
- (n10) Principle 3's S12 sentence ends with ";" inside the `PollUntil` bullet, and the S9 bullet follows. That is punctuation only.

**Resolution (Manager, CP5 close):** 84.27 is fixed in PLAN.md by the Manager: D8's
`sentOffsets()` row now names `Clear()` as a reset too. The Actor did not edit the plan.

**Resolution (Actor, CP5 close):** 84.28. The test half is fixed; the plan half (the S3 bullet)
was fixed by the Manager. The summary of
`MutatingTheOffsetsAfterTheCallReturns_DoesNotChangeWhatIsCommitted` now claims only what its
assertions detect: "The offsets are a snapshot taken by the time the call returns: mutating the
caller's dictionary after that (on the async mock, before the returned `Task` is awaited) does
not change what `CommittedOffset` reports after the commit." "so the call drains" and any
pending-`Task` claim are gone. No `!IsCompleted` assertion was added, because the core completes
the async operation on a spawned task, so it would race. Test code is unchanged; only the
summary changed. (n8)-(n10) are the Manager's plan wording.

# Critic 84 — M17/P1 CP5: re-check of the tests-half fixes 84.24-84.26 (2026-09-29)

No new items. Everything was measured in a fresh scratch copy of `src/` and `tests/Confluent.Kafka.UnitTests`; the worktree was not built.
- **84.24: closed.**
  - The eight classes pass 187/187 on net10.0.
  - The S12 async test now waits through `PollUntil(() => handle.IsClosed, 30 s)`. The poll uses a 2 ms step and stops at the first true reading.
  - I re-ran my `Thread.Sleep(20)` before `DangerousRelease`: green in 3 of 3 runs. The file was restored and is `cmp`-identical.
  - `mut/R24a.log` (no release) is red after the 33 s bound with the expected message. `R24b.log` has 2 of 2 passed.
  - The sync `IsClosed` check stays immediate, which is correct: the marshaller releases its reference before the worker task completes.
- **84.25: closed.**
  - Both sites now assert Code 7 and the exact message: the 5000 ms form in S11 and the 2000 ms form in KPT 2054. S12 sync's `refused` is now pinned to -2 and the D10 message.
  - All three values pass in my run.
  - Every other captured `KafkaException` has Code and exact-Message assertions, with the exception in the note below.
- **84.26: closed.**
  - `CP5.txt:226` and `:234-237` now name PH3, PI1 and PN4, with 13 reds of PN4's own. These match the logs.
  - The added "3 of 38 plus 1 of 5 repeats" matches `mut/*.log` and `gates/run1-rep-net10-5.log`. The `race-probe.txt` and `ZzProbeRace.cs` wording is fixed.
  - Restart-branch mutation: recounted from `fix2126/restart/run-*.log`. It is 22 red and 28 green, every red is `Assert.NotNull() Failure`, and `orig.cs` matches the worktree file.
- **KPT 413 invalidProps1 now runs.** The source label makes it a separate case: 2 passed cases in my run, and no duplicate-ID skip. Its input is still `max.in.flight.requests.per.connection=6` with the Java message. The totals reconcile: 2625 + 2 = 2627 (`fix2126/gates/g4-net10.log`, `g5-net8.log`).
- **No new race, hang or leak.**
  - Both S12 tests now have a `try/finally` with a bounded, idempotent `Dispose`.
  - The poll is bounded.
  - The S11 sync worker's outcome is deterministic once the -2 is observed.

Noted, not filed:
- `RecoveryCompositions_SelectTheDocumentedSets` holds ten `FailASend` results. It asserts on the code sets that its predicates select, not on each message.
  - So the resolution's "S9's two returns, whose callers assert both" is true of `ErrorNext_ReportsTheRowsClassification` only.
  - The same ten inputs are pinned with Code and exact Message by that theory, so no coverage is missing.

**Resolution (Actor, CP5 close):** no items. The noted `RecoveryCompositions` point is
accepted as stated: the resolution's "S9's two returns, whose callers assert both" holds for
`ErrorNext_ReportsTheRowsClassification`, and the composition test asserts the selected code
sets, while that theory pins the same ten inputs with Code and exact Message.


# Critic 84 — M17/P1 CP5 close: final re-check of 84.27 and 84.28 (2026-09-29)

**Clean.**
- **84.27.** D8's `sentOffsets()` row (`PLAN.md:774`) now reads "reset by `BeginTransaction` and by `Clear()`, not by commit". It is marked in place.
- **84.28, plan side.** S3's bullet no longer claims the `Task` is pending. What it now says is witnessed, after return and at the submit seam, is what the two tests assert.
- **84.28, test side.** I checked `PublicProducerSendOffsetsToTransactionTests.cs` at sha a2ff4309…. The summary now claims only that mutating the map after return, before the await, does not change `CommittedOffset`. That is what `:217-218` assert. "So the call drains" is gone.
- **Header note.** n8 is taken ("as a reset", with no count) and so is n9 (84.24 is attributed to S12's poll only).
- **`COMMIT_MSG`.** Each bullet matches the diff, and "No code changes" is true of this commit.

## Noted, not filed
- (n11) `COMMIT_MSG` and RD4 say "12 source files **already** use it". The 12 include `ConsumerGroupMetadata.cs` itself, which CP2 made obsolete, so 11 predate the phase. This is wording only; the measured count is right.

**Resolution (Manager, CP5 close):** n11 taken — RD4 and the plan-amendment COMMIT_MSG now say 12 files, 11 of them before this phase.

# Critic 84: CP6 review (gRPC servicers, D14 / V1-V5 / X14)

Reviewed the uncommitted CP6 worktree on HEAD 8ccb0f8d: `git diff -- bindings/dotnet/grpc-server`,
`gate/CP6.txt` and the commit draft `commit-cp6.msg`. The code matches D14. The five overrides in
each servicer follow the `Flush` template. The sync servicer calls the sync members and the async
one awaits the four `Task` members. Both helpers follow `grpc_translate.py:222-261`, including the
null message, the absent and present-empty `group_instance_id`, and offsets with and without
metadata and epoch. Every response type is `StatusResponse`, and the override names diff equal to
the proto's 13 rpcs in both files.

I checked it again myself. The grpc-server `--no-incremental` build is 0W/0E, and `dotnet format
--verify-no-changes` is clean. A copy of the Actor's probe run from my own scratch folder gives
46/46. `git status` did not change.

## Issue 84.29: The records give CS1998 as the reason the async `BeginTransaction` cannot be `async`, but this SDK does not report CS1998
- **Files**:
  - `gate/CP6.txt:47-49`: "an async method with no await would be CS1998, an error under TreatWarningsAsErrors".
  - `gate/CP6.txt:174-175`, observation (a): "An async method with no await would not build (CS1998 under TreatWarningsAsErrors)".
  - `$S/commit-cp6.msg` body: "because an async method with no await is CS1998, an error here".
- **Severity**: Low. The record states a cause that was not measured and is false (RD13). The code is correct either way.
- **Reference**:
  - The host SDK is 10.0.401 (`dotnet --list-sdks`). Both image routes also build with `mcr.microsoft.com/dotnet/sdk:10.0` (`Dockerfile.grpc:30`, `Dockerfile.grpc.async:35`).
  - My scratch probe (`$S/critic84-cp6/cs1998b/`) is a `Microsoft.NET.Sdk.Web` net8.0 library. It uses the repo's `.editorconfig` and the `Directory.Build.props` settings: `TreatWarningsAsErrors`, `LangVersion latest`, `AnalysisLevel latest`, `EnforceCodeStyleInBuild`, and the grpc-server's `NoWarn CA1031`.
  - An `override async Task<int>` with a try/catch and no `await` builds with 0 Warning(s) / 0 Error(s). It also builds clean with `-p:WarningsAsErrors=CS1998 -p:WarningLevel=9999`, and with `LangVersion` 12 and 13.
  - Control positive: an unused local in the same project fails the build with `error CS0168`. So warnings do become errors there, but CS1998 is never reported.
  - No log in the Actor's cp6 folder measures CS1998. The only earlier source for the claim is M8/P2 (`STATUS.md:494`, `COMMENTS.DONE.24.md:46`), which used an older SDK.
- **Description**: The Metrics shape is right, but not for this reason. D14 says "calls `BeginTransaction()` synchronously", `IAsyncProducer.BeginTransaction()` returns `void` (`IAsyncProducer.cs:382`), and `Metrics` (`AsyncProducerServiceImpl.cs:372-396`) already shows the shape. The record instead rests the choice on a compiler error that this toolchain does not produce. A later reader could take it as a build constraint and "fix" other sites to match it.
- **Fix**: In `CP6.txt` (both places) and in the commit message, drop the CS1998 clause. Give the reason as D14's wording plus the `Metrics` precedent: `BeginTransaction()` is synchronous, so the override has nothing to await. Alternatively, keep a claim only if it is measured on this SDK (it is not reported).

## Noted, not filed
- Observation (a), decided: the `Task.FromResult` shape is correct, and it catches exceptions the same way as the template. `BeginTransaction()` throws synchronously, so the `try` catches the exception and the `catch (Exception)` → `Translate.ToProto` arm returns it as a `StatusResponse`, exactly as an `async` body would. The only statement outside the `try` is `Get` (`ConcurrentDictionary.TryGetValue`), which cannot throw. That is the same as `Metrics`.
- (n1, nit) `CP6.txt` §Files says each override is 19 lines and "the four state RPCs are exactly that long". That holds for the sync servicer. In the async servicer `BeginTransaction` is 21 lines (`:232-252`, with the two-line comment).
- (n2, nit) `CP6.txt` cites the Python servicer as `grpc_server.py:245-307`. `SendOffsetsToTransaction`'s final `return pb.StatusResponse()` is at `:308`. PLAN §2.4 (`:214`) has the same range.

**Resolution (Manager, CP6 close):** 84.29 fixed — CS1998 clause dropped in CP6.txt (both sites) and commit-cp6.msg; reason is now D14 plus BeginTransaction() returning void (Metrics precedent). n1 fixed (sync four = 19 lines, async BeginTransaction 21). n2 fixed in CP6.txt (:245-308); PLAN §2.4 :214 carries the same range and is batched into the CP7 plan amendment.

# Critic 84: CP7 review (root Makefile, G1-G11, the G10 gate)

I reviewed the uncommitted CP7 worktree on HEAD 8ccb0f8d: `git diff -- Makefile`, `gate/CP7.txt`,
`CP7-roster.txt`, `CP7-failed.txt`, `$S/commit-cp7.msg`, and the raw logs in `$S/cp7/gates/` and
`$S/cp7/stale/`. I checked them against PLAN §0, §4.2, §4.3, §4.5 and §4.6.

No issue filed. Each item below is something I measured:
- **Makefile.** The diff has two hunks, +1/-24, and nothing else. The deleted lines are the HEAD
  lines :272-291 and :309-311. The new :288 keeps its `^I^I` indent and ends with a `\`. `:289 fi`
  and the `else \` line are intact. The comment above the target now ends at "(see
  .semaphore/semaphore.yml)." and reads cleanly. The before and after sha256 values match the
  record, and the before value is the HEAD blob ceb18cfe. The printed recipe passes `sh -n` before
  and after the edit.
- **§4.3 checks, re-run on Darwin.** The grep prints 0 (exit 1), and HEAD's blob gives 5 at
  :273/:275/:309-311. `make -n test-integration-dotnet | grep -c -- '--skip'` prints 0, with make
  exiting 0. The Linux branch is now `cargo test ... -- __grpc_dotnet;`. `grep -ci transact` is
  0 on every tracked Makefile. That makes "The Makefile now has 0 hits" true independent of
  phrasing.
- **G10.** The run log reads "running 152 tests" and "ok. 152 passed; 0 failed; ... 570 filtered
  out", with `DDP=[unset]` and exit 0. 152 + 570 = 722, so nothing was skipped. The ok list,
  `roster.sorted`, `roster2.sorted` and `CP7-roster.txt` each `cmp` equal to `CP0-roster.txt`
  (116 + 36 = 152). `comm -13` is empty. `comm -23` is exactly the six arms, and
  `grep -cxF -f CP0-failed` over the ok list gives 6. Each `alone/1-6.log` prints
  "running 1 test" and "1 passed ... 721 filtered out". `control-short-exact.log` prints
  "running 0 tests". `g10-nm-18.txt` is 18. The header sha256 is e8d39f09…d111 after the
  cross-build and still is now. The `.so` hash equals CP0's 7b7590a0….
- **G1-G11 against `gates.out`.** Every value matches: G2 0W/0E with the six outputs; G3 0
  bytes; G4 2627 on v10.0; G5 2627 with "(.NETCoreApp,Version=v8.0)"; G8 soak under `$NET8`,
  165/165 on each runtime; PerfV3 0W/0E; perf 25/25; G9 0W/0E, 0 bytes, 13/13; G11 from the
  repo root (`cd` in `gates.sh`), rc=0 for both; G1 0/0 with the control 34 files; G6 715; G7
  rc=1 with the control 38. The five freeze checks are OK.
- **commit-cp7.msg.** The file list is the four CP7 files. The body lines are ≤72 columns
  except the three paths. The trailer is the last line, and it ends with a newline.

## Noted, not filed
- (n1, nit) Some statements in `CP7.txt` rest on observations that no saved log records:
  - "each exits 0" for the six isolated runs, and the control's "exit 0": `alone/*.log`
    records neither the command, the exit status nor the `DOCKER_DEFAULT_PLATFORM` state;
  - the images' "amd64, CONSUMER_FLAVOR=sync/async" and "DOTNET_VERSION=8.0.31 in both":
    `g10-images.log` contains none of these;
  - "14 exited ducker* containers";
  - "ps found no chaos or integration process".

  The gate does not depend on any of them. §4.5 makes the printed lines the evidence, and those
  lines are present. The full-path filter also proves `--exact` was in effect: without it, each
  non-async name would select its `_async` twin as well, but each log shows 1 run and 721
  filtered out. Under RD13, either save those outputs or drop the statements.
- (n2, nit) `CP7.txt` stale-sweep hits 4 and 5 cite `design/current/STATUS.md:15` and `:384`.
  The Manager's M17/P1 entry, written at 16:30 after this record, moved those lines to `:32`
  and `:401`, and hit 5's "M11/P5 entry" line moved with them. The record was true at 16:13.
  If it is committed with the new STATUS entry, cite the entries by name, or mark the line
  numbers as pre-entry.
- (n3, for the Manager, not CP7's defect) The §4.5 step-2 WARNING says "macOS `nm -D` reads
  **zero** symbols from a Linux ELF". That no longer holds on this host. The host `nm -D
  --defined-only` and `xcrun llvm-nm -D --defined-only` both print 18 on the staged ELF with the
  exact selector. The gate used the in-container route, so the result stands. The claim in the
  plan (and its STATUS.md:257 source) is now false as a general statement.

---

# Critic 84: the M17/P1 close-out documents (STATUS entry, the two RULE-DRAFTs, the CP6 plan amendment)

Reviewed on HEAD 8ccb0f8d, in the worktree:
- the new STATUS entry (`design/current/STATUS.md:10-25`) and the two in-place annotations (`:32`, `:401`),
  diffed against `$S/cp7/STATUS.before.md`;
- both RULE-DRAFTs in `design/history/M17/P1-producer-transactions/`;
- the CP6 amendment, diffed against `$S/cp7/amend/PLAN.before.md`, and its `COMMIT_MSG`.

Reproduced and true (not filed):
- Mode A: the `git diff 76629aea -- src/ src/ffi/ cbindgen.toml generator/ tests/` is 0 bytes, with 0
  untracked paths there. Outside `bindings/dotnet/` the diff is `Makefile` +1/-24 and `kafka`.
- The header sha256 is e8d39f09…d111, and it is recorded in all of CP0-CP7.
- The extern count is 715, against 697 at `76629aea` (`git grep`).
- Tests: 2317 in CP0.txt, 2627 in CP5-CP7.txt.
- G10: `CP0-roster.txt` and `CP7-roster.txt` `cmp` equal. `comm -13` is empty and `comm -23` gives the six
  arms. The run log says 152 passed, and each of the six `alone/*.log` files says "running 1 test".
- The flake's budget is `2_000_000` at `ProducerSubmitHandleRefTests.cs:74`. It failed on net8.0 at CP2 and on
  net10.0 at CP4.
- The member names match `IProducer.cs:268-322`, `IAsyncProducer.cs:361-470`, both mocks' `:278-321` /
  `:345-388`, `KafkaException.cs:204-319`, `ConsumerGroupMetadata.cs:75`/`:101`, and 13/13 overrides.
- The pinned core values match CP5.txt `:150-190`. KPT 1950 has no mock check in `MockProducer.java:182-191`.
- Item (26)'s line ranges are right.
- Both annotations are true, and they keep the older entries' meaning.
- In the drafts:
  - every quoted "Current text" matches `bindings/dotnet/CLAUDE.md` / `ffi-marshalling.md` (2383 lines) at the
    stated lines, and neither rule file differs from `76629aea`;
  - the bottom-up order is consistent;
  - RD5's, RD6's and RD7c's code facts match the worktree, including the drain message verbatim, the four
    divergence markers, the 16 header predicates and the six EntryPoints;
  - there are no `[[` markers;
  - both headers follow the M15/P3 D20 format;
  - the RD2 coupling to RD3's row-480 half, RD7b and RD8 is stated in both drafts.
- The CP6 amendment is correct: `grpc_server.py:308` is `SendOffsetsToTransaction`'s `return`, and no other
  `:245-307` remains.

## Issue 84.30: Two Critic rule suggestions that the phase record sends "to the CP7 RULE-DRAFTs" are in neither draft
- **Severity**: Medium (a recorded commitment dropped silently; no behaviour impact)
- **Where**: both RULE-DRAFTs; `COMMENTS.DONE.84.md:555` and `:726`
- **Reference**: `.claude/rules/agent-roles.md` (Critic rule suggestions); PLAN §7.2
- **Description**: The archived record says:
  - `:555` (cycle 6): "**Deferred to the CP7 RULE-DRAFTs:** the Critic's optional rule suggestion, as a
    candidate extension of RD13". An exhaustiveness sweep gives the matched count and the count after reading,
    and an "every block" sweep includes typedef doc blocks.
  - `:726` (CP2): rule suggestion 3, "check each binding 'Java does X' remark against the Java source at the
    pinned commit … goes to the CP7 RULE-DRAFTs".

  Neither draft has either suggestion. A search of both files for `typedef`, `matched`, `Java does`,
  `pinned commit`, `construction form` and `retype` finds only unrelated lines (RD2's "matched by Code", RD6's
  "Java does not need"). The STATUS entry does not mention them. The Manager's own hand-off list has six such
  suggestions (the one-symbol-probe sweep, delete-not-retype, check Java remarks, sweep the rest of the file,
  store the diff command, matched/post-read counts). As it stands, a reader of the record will think these are
  waiting in a draft that does not contain them.
- **Fix**: Add them to the claude-md draft, either as an RD13 extension or as a new RD with insertion point
  and verbatim text. Otherwise, record in the draft (and in the STATUS "Rule drafts" bullet) that they are
  declined or deferred, and why.

## Issue 84.31: The STATUS entry lacks two elements PLAN §7.3 requires: the Mode-A control positive and the unassigned-code readback
- **Severity**: Low
- **Where**: `design/current/STATUS.md:13`, `:16-20`
- **Reference**: PLAN §7.3 bullets 2 and 3 (`PLAN.md:2140-2147`)
- **Description**: §7.3 asks for "the Mode-A proof **with its control positive**". The entry gives the empty
  diff but not the control. The `Makefile` +1/-24 figure is a different pathspec, so it is not the control
  either. CP7.txt records the control as `git diff --stat 76629aea -- bindings/dotnet/`, 34 files, +10048/-201.
  It measures 35 / +10071 / -203 now, because it counts this close-out's own edits. The M15/P12 entry states
  its control ("control-positive 16 files / +5116 / −32").

  §7.3 also lists "the unassigned-code readback" among the pinned values: codes 1000, 134, -2 and 40000 read
  back as `Code == -1` with all six flags false (`gate/CP2.txt:558-559`; PLAN `:1653-1655`). The entry's "Core
  values" bullet leaves it out.

  (nit) "test totals per TFM" gives net10.0 and net8.0. It does not say that net462 is built and not run
  (CP7.txt, "net462 is built in G2 and not run"), which the M15/P12 entry states.
- **Fix**: Add the control positive with its measurement point, and the unassigned-code line. Say that net462
  is build-only.

## Issue 84.32: The entry points at an archived `COMMENTS.DONE.84.md` that does not exist
- **Severity**: Low
- **Where**: `design/current/STATUS.md:10`
- **Reference**: PLAN §7.3 bullet 4; `bindings/dotnet/CLAUDE.md` §8.4
- **Description**: "record at `design/history/M17/P1-producer-transactions/COMMENTS.DONE.84.md`" does not
  resolve: the folder holds only `PLAN.md`, the two drafts and `gate/`. The only copy is the untracked
  binding-root `bindings/dotnet/COMMENTS.DONE.84.md`. The snapshot README's order (items 1-8, ending at `cp6`)
  has no step that archives it. Also, "items 84.1–84.29, all resolved" stops being true as soon as this
  review's items exist.
- **Fix**: At the close, after this review's items are resolved, copy the record into the phase folder and
  include it in the CP7 snapshot. Update the range to the final item.

## Issue 84.33: "a plan amendment at each review" is false, and the README order that the commit list defers to stops at cp6
- **Severity**: Low
- **Where**: `design/current/STATUS.md:10`
- **Reference**: `git log`; `.git/claude-m17p1-snapshots/README.md`
- **Description**:
  - The CP0 review's corrections are in the archive commit `aaa3d9e8` ("The CP0 review added five
    corrections"). There is no separate amendment for them.
  - There is no CP3 amendment either. No `cp3-plan` snapshot exists, and the CP4 amendment's header says it
    carries the CP3 review's corrections.
  - "CP3 onward … in that folder's `README.md` order": the README lists items 1-8 only. The CP6 amendment
    under review here and the CP7 commit are not in it yet.
- **Fix**: Say "a plan amendment after most reviews" (or name them: CP1, CP2, CP4, CP5, CP6). Append the
  CP6-amendment and CP7 items to the README before the hand-off.

## Issue 84.34: Both drafts cite KafkaException.cs lines from the CP2 commit, not from the shipped tree; RD9's tracked-file claim is stale
- **Severity**: Low (record claims, against the drafts' own RD13)
- **Where**:
  - claude-md draft RD2 (`:40`) and RD9 (`:193`);
  - ffi draft RD7b (`:67`)
- **Reference**: the worktree; `git diff --stat 8ccb0f8d -- bindings/dotnet/src/Confluent.Kafka/KafkaException.cs`
  gives +20 (CP5's X2 pointers)
- **Description**:
  - RD2: "D6's five properties are at `KafkaException.cs:200`, `:223`, `:257`, `:280`, `:299`". Those are the
    lines at `8ccb0f8d`. In the worktree the five `public bool Is…Error { get; }` lines are at `:204`, `:231`,
    `:269`, `:296` and `:319`. `:200` is now a `///` line.
  - RD7b: "`FromBorrowedHandle` (`KafkaException.cs:378`)". It is at `:398` now; `:378` is `/// <remarks>`.
  - RD9: "`git ls-files` lists CP0-CP3; CP4.txt is on disk, untracked". `CP3.txt` is only staged. `CP4.txt`,
    `CP5.txt`, `CP6.txt`, `CP7.txt` and the two `CP7-*.txt` files are all untracked.

  The `:74` `IsFatal` remark and every insertion point are correct.
- **Fix**: Re-measure against the tree being handed over, or say which tree each cite belongs to.

## Issue 84.35: RD4's proposed row states a factory rule that the precedent it cites does not follow
- **Severity**: Low
- **Where**: claude-md draft RD4, proposed row (`:114`)
- **Reference**: `Internal/Interop/AdminCallbacks.cs:874-885`, `:1935-1984`; `Internal/NativeAdminClient.cs:1830`,
  `:5564`; `Internal/Interop/GroupMarshal.cs:105`, `:176`, `:268`
- **Description**: The row says: "Where the binding itself constructs the type, it goes through a non-obsolete
  internal factory so `src/` needs no `CS0618` suppression". It cites "the M15 admin types … for types and
  members" as precedent.

  Those types do the opposite. `AdminCallbacks.cs:874-885` constructs the `[Obsolete]`
  `ClientMetricsResourceListing` inside `#pragma warning disable CS0618` in `src/`, and the other sites above
  suppress in the same way. For an `[Obsolete]` *type*, no factory avoids CS0618, because naming the type
  raises it. The draft's own Reason calls the rule "a preference". Only the proposed text states it
  unconditionally, and a maintainer applying the text verbatim would get a rule that shipped code violates.
- **Fix**: Scope the factory clause to an obsolete constructor or member on a non-obsolete type (the
  `ConsumerGroupMetadata` case). Say that internal use of an obsolete *type* suppresses `CS0618` locally, per
  the M15 admin precedent.

## Issue 84.36: The file-forward bullet says "plus two from the CP7 sweep" and lists one item
- **Severity**: Low
- **Where**: `design/current/STATUS.md:24-25`
- **Reference**: CP7.txt stale-sweep hits 1-5; claude-md draft "Noticed while measuring" (`:272-276`)
- **Description**: Only (26) follows. If "two" counts (26)'s two files, the number contradicts the list's
  numbering. If a (27) was intended, it is missing. The likely candidate is the draft's three stale
  `CLAUDE.md` sentences (`:46`, `:422` "admin client … still **Mode B**", `:526` "`IProducer` still
  deferred"), which appear only inside a draft, no RD covers them, and the STATUS list does not carry them.
- **Fix**: Either say "one", or add the missing item. The `CLAUDE.md` sentences belong in the file-forward
  list if they are not drafted.

## Noted, not filed
- (n1, nit) RD6's insertion "after line 764, followed by a blank line" puts the new `⚠` paragraph directly
  under `:764`, the last line of the "Null callback rejected" bullet, with no blank line. Markdown then folds
  it into that bullet as a lazy continuation. Say "after line 765", or open the block with a blank line, as
  RD7c's does.

**Resolution (Manager, CP7 close, 2026-09-29).** The CP7 section above is clean. Its nit n2 is fixed: `gate/CP7.txt` marks the two STATUS.md line numbers as pre-entry. Nit n1 is accepted as is, because the §4.5 evidence is in the logs. Note n3 is filed forward as STATUS item (28). The close-out items, each fixed in the Manager's own documents (backups in the scratchpad at `cp7/fix3036/`):
- 84.30: RD14 is added to the `CLAUDE.md` draft. It carries all six suggestions from the Manager's hand-off list, and the STATUS rule-drafts bullet names it.
- 84.31: the STATUS entry gains the control positive (`git diff --stat 76629aea -- bindings/dotnet/`: 34 files, +10048/−201, tracked only), the unassigned-code readback (`gate/CP2.txt:558-559`) and "net462 is built, not run".
- 84.32: this file is archived to `design/history/M17/P1-producer-transactions/COMMENTS.DONE.84.md` at the close, and the entry's range now reads 84.1–84.36.
- 84.33: the commit sentence now lists the plan amendments that exist (CP1, CP2, CP4 carrying CP3's items, CP5, CP6). The CP0 corrections are in the archived plan, and the README order runs through cp7 and closeout.
- 84.34: the RD2 lines are now :204/:231/:269/:296/:319 and RD7b is now :398, both from the CP6 worktree. RD9's ls-files sentence is re-measured.
- 84.35: RD4's row limits the factory rule to obsolete constructors or members of a non-obsolete type. An obsolete type is built under a local pragma pair (the `AdminCallbacks.cs:874-885` example).
- 84.36: the file-forward list is now "plus three", adding (27) for the three stale `CLAUDE.md` sentences and (28) for the `nm` warning. RD14 is listed under the rule drafts, not under file-forward.
- n1: RD6's proposed text is now preceded by a blank line as well.

# Critic 84 — M17/P1 close-out re-check of 84.30-84.36 and n1 (2026-09-29)

Diffed STATUS.md, both RULE-DRAFTs and gate/CP7.txt against `$S/cp7/fix3036/` and `$S/cp7/CP7.before.txt`.

Resolved, and the new text is true as measured:
- 84.30: RD14 exists and its four bullets carry all six suggestions in `manager-notes.md` (1+2, 6, 3+4, 5).
  Its insertion point can be applied: RD10/RD12/RD13 goes after `CLAUDE.md:1007` (the end of §7.5), and the
  "RD9 first, RD1 last" bottom-up order still holds.
- 84.31: the control positive, 34 files, +10048/-201, is `gate/CP7.txt:130-131`. It measures 35 / +10074 /
  -203 now because of the close-out edits, so "at the CP7 close" is right. The unassigned-code line matches
  `gate/CP2.txt:558-559`. "net462 is built, not run" is added.
- 84.32: the range reads 84.1-84.36. The archive copy is pending by arrangement.
- 84.33: the amendments CP1 (`eb5e3073`), CP2 (`8041731b`), cp4-plan, cp5-plan and cp6-plan exist, and none
  exists for CP0 or CP3. `aaa3d9e8`'s message lists the five CP0 corrections. The six "committed so far"
  hashes match `git log 76629aea..HEAD`.
- 84.34: `KafkaException.cs` is byte-identical to the cp5 snapshot. `:204/:231/:269/:296/:319` are the five
  `public bool Is…Error { get; }` lines, and `:398` is `FromBorrowedHandle`.
- 84.35: `AdminCallbacks.cs:874-885` is a disable/restore pair around the `ClientMetricsResourceListing`
  construction. All 9 `disable CS0618` sites in `src/` concern obsolete types, so the scoped row matches them.
- 84.36: (27)'s three `CLAUDE.md` quotes match `:46`, `:422` and `:526`. (28) matches PLAN `:1455` and CP7
  note n3.
- n1: RD6 now says "preceded and followed by a blank line".
- CP7-review n2: `gate/CP7.txt:243-249` marks both line numbers as pre-entry.

## Issue 84.37: RD14's Reason gives a false count and says each rule cites its finding, which four of the six do not
- **Severity**: Low (a record claim; no behaviour impact)
- **Where**: claude-md draft RD14, the heading and **Reason** (`:274-279`), and the proposed text (`:285-299`)
- **Reference**: `COMMENTS.DONE.84.md:72`, `:324`, `:437`, `:536`, `:555`, `:726`;
  `.git/claude-m17p1-snapshots/manager-notes.md:3-18`
- **Description**:
  - "Critic 84 made six rule suggestions during the phase" is false. The record also has the suggestion that
    became RD10 (`:72`), the three that became RD11-RD13 (`:324`), and the cycle-4 and cycle-5 suggestions
    folded into RD13 (`:437`, `:536`). Six is the number that were not in a draft yet. The heading's
    "six … rules" also sits over four bullets.
  - "Each is stated with the finding that motivated it" does not hold. Only bullet 1's first half (84.10)
    and bullet 3 (`TopicMetadataAndConfig`) name one. Delete-not-retype, the matched/post-read counts, and
    the stored diff command name none.
  - The cited sites, "the cycle-6 CP1 check and the CP2 decision on 84.10", record the deferral of only two
    of the six: #6 (`:555`) and #3 (`:726`). The other four (#1, #2, #4, #5) are only in `manager-notes.md`,
    an uncommitted file under `.git/`.
  - (nit) The insertion point puts these bullets under the RD10 block's lead-in "What a gate step, a gate
    record and a phase plan may count:". The Java-remark rule and the dry-run-diff rule are not about that.
- **Fix**: Say "Six of Critic 84's rule suggestions were not yet in a draft". Either give each bullet its
  provenance or drop "Each is stated with …". Cite `manager-notes.md` for #1, #2, #4 and #5, or say where the
  record holds them. Optionally, give RD14 its own lead-in line.

## Issue 84.38: RD9's re-measured sentence says `git ls-files` lists CP0-CP2, but it lists CP3.txt too
- **Severity**: Low
- **Where**: claude-md draft RD9 **Reason** (`:195`)
- **Reference**: `git ls-files …/gate` against `git ls-tree --name-only HEAD …/gate/`
- **Description**: `git ls-files` reads the index, so it lists the staged `CP3.txt` as well as `CP0-failed`,
  `CP0-roster` and `CP0`-`CP2`. Only HEAD (`git ls-tree HEAD`) stops at CP2. The rest of the sentence is right:
  CP3.txt is staged, and CP4-CP6, CP7, CP7-failed and CP7-roster are untracked.
- **Fix**: "HEAD tracks CP0-CP2 (`git ls-tree HEAD`), CP3.txt is staged (`git ls-files` lists it too), and
  CP4-CP7 …", or cite `git ls-tree` in place of `git ls-files`.

## Issue 84.39: The README order that the STATUS entry and the 84.33 resolution cite ends at cp7, with no closeout
- **Severity**: Low
- **Where**: `design/current/STATUS.md:10` "(cp3 through cp7, then closeout)" and "and a close-out commit";
  the 84.33 resolution, "the README order runs through cp7 and closeout"
- **Reference**: `.git/claude-m17p1-snapshots/README.md` (item 10 is `cp7`, the last); the snapshot folder has
  no `closeout/`
- **Description**: The README has no closeout item, and no closeout snapshot exists. The entry refers the user
  to an order entry that is not there yet. The 84.32 fix, which archives `COMMENTS.DONE.84.md` "at the
  close", has no snapshot to go into either.
- **Fix**: When the close-out snapshot is frozen, append its line to the README (with the archived record
  and the STATUS/draft files in its manifest), at the same step as the archive copy. No text change is
  needed if that happens.

## Issue 84.40: The unassigned-code line sits under "Core values measured at CP5", but it was measured at CP2
- **Severity**: Low (nit-level)
- **Where**: `design/current/STATUS.md:16` (the parent bullet) and `:21` (the new sub-bullet)
- **Reference**: `gate/CP2.txt:558-559`. `gate/CP5.txt` has no `40000`; a grep of `gate/*.txt` finds it only
  in CP2.txt.
- **Description**: The parent reads "**Core values measured at CP5 and pinned** (`gate/CP5.txt`)". The new
  line cites CP2.txt, so the parent's "measured at CP5" no longer covers every line beneath it.
- **Fix**: Make the parent "measured at CP2 and CP5", or move the line out from under it.

Note: once 84.37-84.40 are resolved, the STATUS range "84.1–84.36" needs to become the final item (84.32).

**Resolution (Manager, CP7 close, 2026-09-29).** Each fix removes a claim rather than re-wording it (ffi §A6 round-5). Backups are in the scratchpad at `cp7/fix3740/`.
- 84.37: RD14's heading and Reason no longer give a count or a per-rule provenance claim, and there is no "no other RD" comparative. They say the rules are Critic 84 suggestions set aside for these drafts, and that the Java-remark and exhaustiveness-count rules are also in this file. The bullets now go in a separate list with their own lead-in, "How a record and a review establish a claim:".
- 84.38: RD9 now says HEAD holds CP0-CP2, CP3.txt is staged and CP4-CP7 are untracked. There is no ls-files claim.
- 84.39: the closeout snapshot is taken at this close and appended to the README as item 11.
- 84.40: the parent bullet now reads "Core values measured and pinned (`gate/CP5.txt`; the unassigned-code readback is from `gate/CP2.txt`)".
- The STATUS range and the closeout commit message now read 84.1–84.40.

# COMMENTS.84 — M17/P1, CP7 close: re-check of the 84.37-84.40 fixes

84.37, 84.38 and 84.40 are resolved in the two files that were changed. 84.39 is deferred to the closeout snapshot
by design. The STATUS range and the commit message now read 84.1-84.40. The new RD14 Reason and lead-in, and the
new RD9 and STATUS parent-bullet text, make no false count or uniqueness claim. HEAD holds CP0-failed, CP0-roster
and CP0-CP2, CP3.txt is staged, and CP4-CP7 (with CP7-failed and CP7-roster) are untracked. The RD13 record-half
bullet is the last one in its block, so "directly after the bullet" and "after that block" agree. The two
leftovers below are copies of the same claims in files the fixes did not touch.

## Issue 84.41: STATUS still calls RD14 "Critic 84's six record and review rules", the count the 84.37 fix removed
- **Severity**: Low (a record claim; no behaviour impact)
- **Where**: `design/current/STATUS.md:23` (the "Rule drafts" bullet)
- **Reference**: the RD14 heading in the claude-md draft (`:274`), "further record and review rules from Critic
  84"; the 84.37 resolution, "RD14's heading and Reason no longer give a count"
- **Description**: 84.37 flagged the draft heading's "six … rules" because it sits over four bullets. The draft
  no longer says it, but the STATUS bullet that describes the draft still does, word for word. So the fix is
  only half done, and the entry now disagrees with the heading it summarises.
- **Fix**: Drop the count here too, for example "RD14, further record and review rules from Critic 84 (added at
  the CP7 review; the file name predates it)".

## Issue 84.42: The closeout commit message still says "the core values pinned at CP5"
- **Severity**: Low (nit-level; the same point as 84.40)
- **Where**: `$S/cp7/closeout-COMMIT_MSG`, first bullet, "the core values pinned at CP5"
- **Reference**: `design/current/STATUS.md:16` after the 84.40 fix: "measured and pinned (`gate/CP5.txt`; the
  unassigned-code readback is from `gate/CP2.txt`)"
- **Description**: The message summarises the same list that 84.40 corrected in STATUS. One of its lines comes
  from CP2, so "at CP5" is no longer true of the whole list. The message was edited in this round (for the
  84.1-84.40 range), but this phrase was not.
- **Fix**: "the core values pinned (CP5, and the CP2 unassigned-code readback)", or just "the core values
  pinned".

Noted, not filed: the new RD14 Reason says the Java-remark and exhaustiveness-count rules are "also recorded in
`COMMENTS.DONE.84.md` (the cycle-6 CP1 check and the CP2 decision on 84.10)". The two sites are listed in the
opposite order to the two rules (exhaustiveness is at cycle 6, `:555`; Java-remark is the CP2 decision, `:726`).
There is no "respectively", so nothing in it is false. Swapping one pair would stop a reader matching them the
wrong way round.

**Resolution (Manager, CP7 close, 2026-09-29).** 84.41: STATUS now reads "RD14, further record and review rules from Critic 84". 84.42: the closeout commit message now reads "the core values pinned (CP5, and the CP2 unassigned-code readback)". The order note is taken: RD14 now names the exhaustiveness-count rule first, matching the order of its two sources. The STATUS range and the commit message read 84.1–84.42. The Manager verified these three by grep, with no further review round. The phase is closed and COMMENTS.84.md is empty.
