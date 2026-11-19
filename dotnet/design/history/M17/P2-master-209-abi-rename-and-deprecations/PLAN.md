# M17/P2 (N=89): move the .NET binding onto master's #223, #209 and #210 (via rebased PR #201)

Status: **APPROVED, Revision 2 (2026-10-01), "with all PM recommendations"** — D2, D3, D4
(deferred to N≥90), D5, D6 (delete outright), D7, D9 (Rev 2), D10, D11 (extended), D13(a),
D14(a), D15(a), D16(b), D17, D18(a), D8 (user-run). D12 dropped. **DONE** (closed 2026-10-02,
S5) — see the Execution log directly below.

### Execution log (Manager checkpoints; a fresh session resumes from the last entry)

User authorizations for P2 only: (1) D14 — the dotnet-actor may `git mv bindings/CLAUDE.md
dotnet/.claude/rules/bindings.md` (zero content change) plus ONE link line in `dotnet/CLAUDE.md`;
(2) path-string-only edits to the 10 `bindings/CLAUDE.md` cite lines (8 `dotnet/CLAUDE.md`, 1
`ffi-marshalling.md`, 1 `dotnet-critic.md` persona). Every other rule-file change stays a §9
suggestion. Stage order: S0 → S1a → S1 → S1b → S2 → S3 → **PAUSE (user runs D8)** → S4 → fix
loop → S5.

- **S0 (2026-10-01, Manager): PASS.** Airlock fetch done. `origin/feat/admin-per-key-python` =
  `c0220aab7c8902e74e78f5acd099dc5eba8b12c5` (unchanged), `origin/master` =
  `c7dd21bf8c1899588069647174e4da71c48ffd9a` (unchanged); `c7dd21bf` ⊂ `c0220aab`; PR #201 not
  landed. HEAD = `refs/heads/prashah_dev_dotnet_binding` @ `274523ec`; no `r2-move`; root
  `target/` gone (D17.1); `.git/info/exclude` carries the two `/dotnet/…/local-docker-logs/`
  lines (D17.2). Pre-S1a baseline: 743 tracked under `bindings/dotnet`, 241 untracked
  (exclude-standard), 16 excluded logs, 3746 files on disk.
- **S1a (2026-10-01, dotnet-actor 89): DONE, Manager-verified.** `777dfa83` pure move (743 ×
  R100, 0 other) and `27b61304` D14 pure rename (`bindings/CLAUDE.md` →
  `dotnet/.claude/rules/bindings.md`, 1 × R100). On disk under `dotnet/`: 241 untracked + 16
  excluded logs, 3747 files; `bindings/` holds only `c/`, `python/`; both `/dotnet/…` exclude
  lines match (`check-ignore`). This PLAN now lives under `dotnet/design/history/…`.
  **S1 pre-check (Manager, read-only `merge-tree` of `27b61304` × `c0220aab`): 26 CONFLICT** —
  the same 24 content + 2 modify/delete as T2, minus the implicit-dir-rename one (D14's move
  already happened). 0 untracked-file collisions with paths the merge writes; the merge does not
  touch the locally modified `.claude/agent-memory/project-manager/MEMORY.md`.
  **Sequencing refinement (recorded, not silent):** the merge commit resolves the root `Makefile`
  minimally (master's text + our dotnet blocks unchanged); D15(a)'s conversion of root
  `Makefile` / `dotnet/Makefile` / `rust/Makefile` lands as one coherent S1b commit, so the merge
  commit's own .NET hunks stay reviewable. Within D11's accepted non-building window.
- **S1 (2026-10-01, dotnet-actor 89): DONE, Manager-verified.** Merge `b4f019d4` (parents
  `27b61304`, `c0220aab`): 26 conflicts = 20 whole-file theirs (each `diff --cached C` = 0) + 2
  orphan deletes + 4 hand-merges (semaphore comment renumbered; root `Makefile` minimal; 
  `admin_backend.rs` theirs + the "five backends" doc lines; `backend_factory.rs` union).
  Follow-up `8964a732` `fix(dotnet): #[expect] …` — master #209's `allow_attributes = "deny"`
  rejected the .NET arms' `#[allow]`; 4 allowlisted harness files, 5+/6−. Gates: #9 no orphans,
  `bindings/` 0 tracked; #8 Mode A = exactly 15 allowlisted paths, Mode-B/`python`/`c` diff vs
  `c0220aab` = 0; #1 header `af0f16448fd7ec653174907890f0e245a6b24738` ✅, nm 2/0 ✅; #7
  `cargo test` 4285 passed / 0 failed / 10 ignored; format-check, check-generated, harness
  `--tests` build ✅; lint-custom 4/6 ✅ (817/231/8373/22), the 2 Java-ref rules fail as expected
  (D8); clippy run by hand (xtask lint stops at lint-custom) clean after `8964a732`.
  Local leftovers (user's, untouched): untracked `bindings/` (old `bindings/c/tests/unity`
  submodule checkout + ignored Python build output); new `c/tests/unity` submodule not
  initialised. Unit-test baseline for gate #4: 2927/2927 per TFM at `274523ec` (STATUS M17/P1).
- **S1b part 1 (2026-10-01, dotnet-actor 89): DONE, Manager-verified.** `efce532e` (csproj
  `NativeLibraryPath` → `../../../rust/target/…`, grpc `ProtoRoot` → `rust/multilanguage-test-server`,
  both Dockerfiles with in-image layout `/src/rust/…` + `/src/dotnet/…`, soak scripts) and
  `ba536330` (D15(a): root dotnet targets keep names, delegate `$(MAKE) -C dotnet $(ROOTS)`;
  `dotnet/Makefile` on `REPO_ROOT`/`RUST_PROJECT_ROOT`(=rust/)/`GRPC_NATIVE_DIR`, every
  `RUST_PROJECT_ROOT` use audited; `rust/Makefile` +`test-integration-grpc-dotnet{,-native}`
  +`DOTNET_GRPC_SKIPS` verbatim). Gates: .sln + lib + unit tests build 0E/0W both TFMs;
  `NativeLibraryPath` under `rust/target/`; dry runs clean; #10 0 functional hits; #8 = 16 paths;
  Mode-B 0. Recorded deviations: (1) `test-integration-dotnet` now has `build-rust-all-features`
  as a prerequisite like `test-integration-c` (on non-Linux the release build runs before the
  self-skip); (2) perf targets' native build split onto its own line so `make -n` cannot start
  a benchmark. Informational `make test-dotnet`: Test Run Aborted on renamed EntryPoints (S2).
- **S1b part 2 (2026-10-01, dotnet-actor 89): DONE, Manager-verified.** `bf249e8f` — the
  user-authorized rule-file edit: 3 files, numstat 9/8 + 1/1 + 1/1; word-diff = 10 ×
  `bindings/CLAUDE.md` → `dotnet/.claude/rules/bindings.md` token pairs + the one added link line
  in `dotnet/CLAUDE.md` (after the "How it loads" paragraph). `16df502c` — D9(a) sweep: 65 files,
  278/278, all C# changes are `//` comments; root-CLAUDE.md renumber folded in (4 explicitly
  "root" sites). Deliberately **skipped** (recorded): 7 `bindings/` hits that are *recorded git
  commands pinning old commit ranges* in `STATUS.md` / `PLAN-M9` (rewriting would falsify the
  recorded proofs); 42 bare `src/`/`tests/` cites that are ambiguous; `STATUS.md:581` (already a
  non-existent path). Not edited (not authorized): `bindings.md:108` naming its own old path, and
  all other rule-file/persona path drift (§9). Builds 0E/0W (lib, UnitTests, Soak, Perf);
  grpc-server still has the known proto-removal compile errors (S3); unit tests 2927 executed,
  1905 fail on 9 renamed EntryPoints (S2).
- **S2a (2026-10-01, dotnet-actor 89): DONE, Manager-verified.** `91cef6a5` (74 EntryPoint
  renames, 14 families, scripted from `func-renames.txt`; 13 pinned test literals in
  `AdminP4ReaderWiringTests`), `1aca5292` + fixup `92e44ec0` (beyond brief, justified: the admin
  `MarshalAs(I1)` sweep silently lost 12 relocated `TopicPartitionInfo` imports — prefix added,
  415 imports restored, 2 bool returns pinned; mutation shown), `52c10a6f` (25 files / 128 comment
  lines → new names). No C# identifier renamed (none embeds a full old symbol). Gates: 668
  EntryPoints unique; 653 resolve in header + dylib; the 15 non-resolving = exactly the removed
  set (Manager re-derived with `comm`). Build 0E/0W, format clean. Unit tests 2927 executed per
  TFM, 2465 pass / 462 fail — all `EntryPointNotFound` on 5 of the 15 removed (433 of them
  `Consumer_close_with_timeout`). Rule-file sites still naming old symbols (suggestions only):
  `ffi-marshalling.md:470,474,486,836-837`. Pre-#209 stale names noted, not touched
  (`kafka_common_KafkaError_t` ~40 doc lines vs real `kafka_common_Error_t`, etc.).
- **S2b–S2d (2026-10-01, dotnet-actor 89): DONE, Manager-verified.** `c4500c3d` (S2b: 2 admin
  RPCs + 6 types + 14 P/Invokes), `e2dc9bed` (S2c: `ConsumerGroupDescription.State`,
  `ConsumerGroupState`, `GroupMarshal` consumer-state helpers), `36fd0187` (S2d: `IConsumer.Close
  (TimeSpan)` + impls + `CloseSyncWithTimeout` + P/Invoke; sync `Dispose` → `Consumer_close`,
  30 s documented). R9: every removed member confirmed `@Deprecated` in Java 4.3.1 (file:line in
  the commit bodies). No async `Close(TimeSpan)` exists (`IAsyncConsumer` has only
  `Close(CancellationToken)`) → no open decision. Deleted tests: 96 executed cases per TFM (S2b
  74 = 53 Fact + 21 rows; S2c 15 = 7 + 8; S2d 7), list in
  `scratchpad/s2bd/deleted-tests.txt`. Gates: 653 EntryPoints, unique, 0 unresolved (header +
  dylib); 0 removed names left; 0E/0W all projects both TFMs (+ net462 UnitTests); format clean;
  unit tests **2831 = 2927 − 96** per TFM, 0 failed, identical name sets. **Incident (local,
  unpushed, self-corrected):** a first S2d commit `40e0da4f` staged the tracked, locally-modified
  `.claude/agent-memory/project-manager/MEMORY.md` (path list from an unscoped `git diff`); the
  actor undid it with `git reset --soft e2dc9bed` + `restore --staged`, recommitted `36fd0187`
  with dotnet paths only; MEMORY.md working-tree content byte-identical. Verified: no
  agent-memory path in any P2 commit. Briefs now require path lists scoped to `-- dotnet …`.
  S2e candidates: `PublicSyncConsumerTeardownTests.s_deadline` / `SafeConsumerHandleTests
  .s_disposeDeadline` = 30 s (zero margin vs the new close bound); `SoakClient.cs:731`.
- **S2e–S2f (2026-10-01, dotnet-actor 89): DONE, Manager-verified (dotnet-only diff).**
  `bbba6ed7` Prelink-every-`[DllImport]` test (2 facts; discovers 653; mutation to an old name
  fails on both TFMs), `2bac94dc` D7 (13 cases, sync + async real consumers, no broker),
  `e1a9f9a7` R6 audit (no deadline changed — measured sync-Dispose ≤ 0.01 s on never-joined real
  consumers, slowest subscribe+dispose 0.60 s; D2 comments at the 30 s sites; one pre-push amend of
  this commit's message), `8a6bc75b` STATUS "M17/P2 in progress" entry with the 96-case
  deleted-test list. **Plan correction (D7):** #223 does NOT reject `group.instance.id` without
  `group.id` — the core accepts it (`consumer-null-instance-<n>`); #223's real new validation is
  an *invalid* `group.instance.id` → `KafkaException` code 40, exact message pinned. Gates: #2 653
  / Prelink green both TFMs; #3 nm 1/1/0/0, test-output dylib byte-identical to release; #4
  `make test-dotnet` rc 0, UnitTests **2846 = 2831 + 15** per TFM, SoakClient.Tests 165/165,
  0E/0W, format clean. Noted only: Perf V3 consumers sync-dispose against a live broker.
- **S3 (2026-10-01, dotnet-actor 89, two passes): DONE, Manager-verified (dotnet-only diff).**
  `3a94eb0c` grpc-server: dropped the `ListClientMetricsResources` / `ListConsumerGroups`
  overrides + `ConsumerGroupListingToProto`; **deviation (accepted):** at `TranslateAdmin.cs:1051`
  only the `State = description.State` line (+ its CS0618 pragma) went, not the translator —
  `ConsumerGroupDescriptionToProto` still serves `DescribeConsumerGroups`; proto field 6 is
  reserved and Python fills `group_state` only. `ConsumerServiceImpl.Close` → untimed `Close()`;
  async servicer comments only. `391c3da0` STATUS → "S1a–S3 done, PAUSED before S4 for D8".
  Gates: #5 `build-grpc-native-dotnet` rc 0, grpc-server 0W/0E net8.0 + net10.0, format clean;
  #6 151 listed (115 + 36; admin 79, consumer 28, producer 44), 3 skip names × 2 = 6 skipped,
  **native 145/145 × plaintext/ssl/sasl_ssl** and **container 145/145 × 3** (linux/amd64 `.so`
  cross-built in `rust/target/linux-amd64`, sha256 `b8bbc17a…` = in-image `/app` copy in both
  images, all 653 EPs resolve; `MULTILANG_BACKEND_MODE=container` needed off Linux), same 145
  names both modes; #7 `doc-check` rc 0, `check-bindings` 29/29, Python plaintext oracle 230/230
  on rerun; `test-dotnet` 2846/2846 per TFM. **Deferred flakes (not fixed):** Python oracle run 1
  had 6 `producer_transactions_test` reds (passed alone + full rerun); a 2nd instrumented container
  plaintext pass had 1 red (`…describe_consumer_groups_batches_several_groups__grpc_dotnet`,
  create-topic timeout), 3/3 alone. **Open item (transaction-parity phase):** `GroupMetadata` /
  `ReleaseGroupMetadata` (new ConsumerService RPCs in `c0220aab`) have no .NET override; reached
  only by the 6 skipped txn arms; `rust/Makefile`'s `DOTNET_GRPC_SKIPS` removal comment lists only
  the five producer RPCs. Leftovers (gitignored / user's call): `rust/target/linux-amd64` 672 MB,
  staged ELF `.so` in `rust/target/release/`, +333 anonymous Docker volumes from testcontainers.
  **PAUSED here for D8** — S4 resumes on main's SendMessage.
- **D8 (2026-10-01, user): DONE.** The user ran `(cd rust && cargo xtask fetch-java-refs)`. Tags
  4.3.1 (`26b251a4`) and 4.4.0-rc3 (`a6e87dfe`) are in `kafka/`, and the submodule pointer is
  unchanged. The Manager then re-ran the full `cargo xtask lint` from `rust/`: rc 0, lint-custom
  6/6, including the two Java-ref rules.
- **S4 (2026-10-01, dotnet-critic 89, one pass over S1a–S3): 2 findings, both approved, both
  fixed.**
  - **Clean verdicts:**
    - ABI signatures: all 653 `dotnet/src` `[DllImport]` declarations, plus 3 test-side ones,
      match the header's 826 prototypes, along with 59 callback typedefs and the
      `ProducerRecordNative` layout.
    - Rule files: path-only. `bf249e8f` is exactly 10 path swaps + 1 link line.
    - Every removed API is `@Deprecated` in Java 4.3.1.
    - Teardown after the `Consumer_close` change is safe.
    - Merge integrity: the 20 whole-file paths equal `c0220aab`; the 16-path Mode-A allowlist is
      exact; Mode B = 0.
    - S1b plumbing, the D9 sweep, the tests and STATUS are all accurate.
  - **C89-1:** `rust/Makefile`'s `DOTNET_GRPC_SKIPS` comment named only the five producer RPCs. It
    now also names the `GroupMetadata` / `ReleaseGroupMetadata` ConsumerService RPCs, missing in
    both consumer servicers, in the rationale and in the removal checklist. Fixed in fixup
    `35f3aac4`. The overrides themselves stay a transaction-parity follow-up.
  - **C89-2:** `rust/tests/common/backend_pool.rs:114,117` still pointed at
    `bindings/dotnet/Dockerfile.grpc{,.async}`. Repointed to `dotnet/…` in fixup `acb6e44b`.
  - **C89-2, other half: not approved.** The critic also asked to fix the 7 stale cites in
    `design/current/python-binding-send-batching.md`. That is a repo-root Python design doc
    outside P2's scope; it is recorded as §9 item 21 for the user.
  - Rule-file suggestions are recorded as §9 items 15–21 and were not applied.
  - The critic wrote its memory note to `rust/.claude/agent-memory/dotnet-critic/`, the wrong
    location. It is untracked and unstaged; the user decides.
  - Re-verification of both fixups by critic 89: see the next entry.
- **S4 re-verify (2026-10-02, critic 89): clean, no new finding, so the loop is complete.**
  - Both fixups touch only comments.
  - The C89-1 text is accurate (proto `:103,:107`; 0 overrides).
  - `make -n` for both dotnet arms is byte-identical to `391c3da0`.
  - The `backend_pool.rs` paths exist.
  - Mode A = 16, Mode B = 0.
  - Coverage item #11 was added to `COMMENTS.89.md`; no `### C89` entries remain.
  - **Next: S5 close, which has not run yet.**
- **S5 (2026-10-02, dotnet-actor 89): DONE — docs-only close commit, not pushed.** Last code commit
  `acb6e44b`. STATUS's M17/P2 entry is now DONE, condensed to the M17/P1 entry's size; its 96-case
  deleted-test list moved to `COMMENTS.DONE.89.md` here. This PLAN and `COMMENTS.DONE.89.md`
  (C89-1/2 resolved, plus the S4 pass record) are tracked from this commit; the binding-root
  `COMMENTS.89.md` was reset to empty. Next unused dotnet N = 90.

Manager: project-manager.
- **Rev 1** (2026-09-30): HEAD `274523ec`, `origin/master` = `383f3d30`, PR #201 tip = `3b27d2c9`.
- **Rev 2** (2026-10-01): HEAD `274523ec` (= `origin/prashah_dev_dotnet_binding`, unchanged),
  `origin/master` = `c7dd21bf`, PR #201 tip = `c0220aab` (`origin/feat/admin-per-key-python`,
  rebased; it contains `c7dd21bf`; merge-base with HEAD = `d6bf7c76`).

> ⚠ **Where this file is.** Its home is
> `bindings/dotnet/design/history/M17/P2-master-209-abi-rename-and-deprecations/PLAN.md`. An
> incident while writing Rev 2 (R2.0) moved the whole `bindings/dotnet/` tree to `dotnet/` in the
> main working tree, so right now it sits under `dotnet/design/history/…`. After the user's
> revert it is back at its home. If D13(a) is approved, S1a moves it to `dotnet/` on purpose.

**How to read Rev 2.** Everything new is in the **Revision 2** block directly below. §0–§10 are
Rev 1, kept for the record. Each superseded part carries an inline **[Rev 2: …]** marker that says
what replaces it. Nothing in Rev 1 was rewritten silently.

---

## Revision 2 (2026-10-01)

### R2.0 ⚠ Incident while writing Rev 2: the user must act first

**What happened.** This was a planning-only session. A scratch `git clone` failed with
"transport 'file' not allowed". The script chained its commands with `;`, so the next `cd <clone>`
failed too, and every following command ran **in the main working tree**:

1. `git checkout -q 274523ec` (detached), then `git checkout -q -b r2-move`.
2. `git mv bindings/dotnet dotnet`. The directory was renamed on disk, so untracked and ignored
   files moved with it.
3. Commit `1c42f4db` ("scratch: pure move bindings/dotnet -> dotnet": 743 files, all `R100`,
   0 insertions).

**State now:**
- `HEAD` is `refs/heads/r2-move` at `1c42f4db`.
- `dotnet/` exists and `bindings/dotnet/` does not.
- `prashah_dev_dotnet_binding` is untouched at `274523ec`.
- The index matches `1c42f4db`.
- Nothing was pushed.

My attempt to revert was **denied by the permission system**. I have not tried the revert again
by any other route. **The revert is the user's:**

```
git -C <repo> mv dotnet bindings/dotnet
git -C <repo> symbolic-ref HEAD refs/heads/prashah_dev_dotnet_binding
git -C <repo> diff --cached --stat        # expect: empty
git -C <repo> branch -D r2-move
git -C <repo> status --short | head -20   # expect: ' M .claude/agent-memory/project-manager/MEMORY.md' + the usual untracked files
```

The dangling commit `1c42f4db` and its reflog entries are harmless.

**What the incident touched:**
- **Plan content:** none.
- **Local exclude rules:** while the move stands, the two `.git/info/exclude` entries for
  `/bindings/dotnet/…/local-docker-logs/` no longer match. 16 log files therefore show as
  untracked until the revert.
- **Evidence reuse:** `1c42f4db`'s tree is exactly the content S1a would produce
  (`1c42f4db:dotnet` = `274523ec:bindings/dotnet` = tree `80d32b35`), so R2.3 used it read-only as
  the S1a stand-in for a `git merge-tree` run. The plan does **not** reuse the commit itself: it
  was unapproved and has no proper message or attribution. S1a re-creates it under approval.

The process lesson is now in §1.

### R2.1 Changelog: Rev 1 → Rev 2

| # | Change | Why |
|---|---|---|
| 1 | Pins moved: master `383f3d30` → `c7dd21bf` (adds #218 `fa0e6c9f`, #216 `3c222d7c`, **#210 `68961a0b`**, #217 `c7dd21bf`). PR #201 `3b27d2c9` → `c0220aab`. | PR #201 was rebased. Re-verified: same subjects; 37 commits (incl. 1 merge) became 36; `c0220aab` contains `c7dd21bf`. |
| 2 | **D1 SATISFIED** (Option A's trigger happened). Options B and C are **retired**. **D1b RETIRED**, replaced by **D18** (merge source = `c0220aab`). | The rebase that A waited for exists. |
| 3 | **New workstream: the repository restructure (#210).** New decisions **D13–D17**; new stages **S1a** (pure move) and **S1b** (path plumbing); all gates restated with their new working directories (R2.7). | #210 moves the Rust workspace to `rust/` and the bindings to `python/` and `c/`. git's directory-rename detection then moves `bindings/dotnet/**` to `dotnet/**` by itself. |
| 4 | **S0 executed** against the real merge source (R2.4). Renames 67 → **74**, broken EntryPoints 82 → **89**, families 13 → **14**. The PR #201-only bucket (37) is resolved: 30 identical + 7 renamed. Removed: 15 → 15 (same names). Arms 151/145 and the 5 compile sites are unchanged. | Rev 1 numbers were pinned to `383f3d30` + `3b27d2c9` and marked for re-derivation in S0. |
| 5 | **D9** scope widened to path drift. **D8**'s command changed. **D11** extended to the new stages. **D12** confirmed as DROP. D2–D7 and D10 stand. | #210 (paths); `c0220aab` (D12). |
| 6 | New risks **R11–R19**. **R1** and **R2** resolved. **R3** stands. | — |
| 10 | The merge recipe changed. The 20 PR #201 conflicts are resolved **whole-file**, not by "take theirs" per hunk (R2.3 ⚠, R19). | Measured: hunk-level resolution leaves stale PR #201-old imports in 3 files, one of them in Rust core. |
| 7 | §9 gains rule suggestions **7–14**. | #210 path drift; rule-loading observation (R2.5); R2.0 process lesson. |
| 8 | Evidence moved. Rev 1's scratch `scratchpad/m17p2/` **no longer exists** (it was cleared), so §2's evidence column is historical. Rev 2's evidence is in `…/ac40d0c9-…/scratchpad/abi-r2/` (index: `abi-r2/INDEX.txt`). | — |
| 9 | §1 gains the R2.0 lesson. | Incident. |

### R2.2 What #210, #216, #217 and #218 changed

`68961a0b` (#210) touches 1222 files: 1154 renames (54 of them with edits), 40 modified, 22 added
and 6 deleted.

- **Rust workspace moved to `rust/`:** `src/`, `tests/`, `xtask/`, `generator/`, `build.rs`,
  `Cargo.{toml,lock}`, `cbindgen.toml`, `rust-toolchain.toml` (1.95.0), `.cargo/config.toml` (the
  `xtask` alias) and `multilanguage-test-server/`.
- **Bindings moved:** `bindings/python` → `python/` and `bindings/c` → `c/`. Each has its own
  Makefile with `REPO_ROOT ?= $(realpath ..)`, `RUST_PROJECT_ROOT ?= $(REPO_ROOT)/rust` and
  `GRPC_NATIVE_DIR = $(RUST_PROJECT_ROOT)/target/grpc-native`. On master, `bindings/` no longer
  exists.
- **Root `Makefile` is now an orchestrator:** `REPO_ROOT = $(CURDIR)`,
  `RUST_PROJECT_ROOT = $(REPO_ROOT)/rust`, `ROOTS = REPO_ROOT=… RUST_PROJECT_ROOT=…`, and
  `$(MAKE) -C rust|python|c …`. `rust/Makefile` owns every cargo recipe, including
  `test-integration-grpc-{python,c}{,-native}`.
- **Build outputs:**
  - The header is always at `rust/target/include/confluent_kafka.h` (`build.rs`:
    `CARGO_MANIFEST_DIR/target/include`).
  - `.gitignore` changed `/target` → `/rust/target`.
  - `.dockerignore` excludes `rust/target` except `release/libconfluent_kafka.{a,so}` and the
    header. It has no `dotnet` entries.
- **CI and rules:**
  - CI runs `(cd rust && cargo xtask fetch-java-refs)`, and gains a Rust-only "Check crate package"
    block.
  - Root `CLAUDE.md` gains one sentence: "The Rust code lives in `rust/`, the bindings in
    `python/` and `c/`. Run the `cargo` commands below from `rust/`; run `make` targets from the
    repository root."
  - `admin-client.md`, `consumer-threading.md` and `producer-transactions.md` get path-only edits.
- **No ABI change:** the master header SHA-1 is `423b7a63` at both `383f3d30` and `c7dd21bf`.
- **.NET:** #210 does not mention it. It names `python/` and `c/` only.
- **The other three PRs:**
  - #216 and #218 add RFC documents (`rfc/`).
  - #217 adds a manual "Publish to crates.io" promotion: `publish-crates-io.yml`, 5 semaphore
    lines, and `rust/Cargo.lock`. No .NET impact (R2.8).

### R2.3 Merge mechanics (area A): trial merges, read-only, `git merge-tree --write-tree`

| Trial | Ours | Theirs | Result tree | `CONFLICT` lines |
|---|---|---|---|---|
| **T1**: the merge carries the move | `274523ec` | `c0220aab` | `3d0eed60` | **770**: 743 file location + 1 implicit dir rename + 2 modify/delete + 24 content |
| **T2**: pre-merge pure move (D16(b)), using `1c42f4db` as the S1a stand-in | `1c42f4db` | `c0220aab` | `e93053c4` | **27**: the **identical** 24 content conflicts + 1 implicit dir rename + 2 modify/delete |

- No `renameLimit` warnings. **One** #210 rename is not paired across the merge's base → theirs
  span: `cbindgen.toml` → `rust/cbindgen.toml` is `R100` inside #210 itself, but from `d6bf7c76`
  to `c0220aab` it shows as `D` + `A`, because #209 and PR #201-new rewrote it past git's
  similarity threshold. Our branch modified that file, so it surfaces as one of the two
  modify/delete conflicts (case 4). The resolution is unaffected. Every other modify/delete would
  have shown the same way, and there are only 2, so no other rename of a file we touched went
  unpaired.
- `dotnet/` is tree `80d32b35` in `274523ec:bindings/dotnet`, `1c42f4db:dotnet`, `T1:dotnet` and
  `T2:dotnet`. So `c0220aab` touches nothing .NET, and in both trials the move is pure.
- **Every rename case, classified:**
  1. *Master renames a directory; we modified files inside it.* The files follow the rename. The
     PR #201 old-vs-new conflicts land under `rust/…`, `python/…` and `c/…`.
  2. *Files we added under a directory master renamed.* git infers `bindings/` → `./` from
     `bindings/{python,c}` → `{python,c}` and moves `bindings/dotnet/**` → `dotnet/**`. These are
     T1's 743 "file location" notices; T2 has none because the move is already done.
  3. *A file we added that collides with a master file at the inferred target.* `bindings/CLAUDE.md`
     (ours only; master never had it) would go to `./CLAUDE.md`. That is an implicit-dir-rename
     conflict, so git leaves it at `bindings/CLAUDE.md` (**D14**).
  4. *Modify/delete orphans.* Root `cbindgen.toml` (master moved it to `rust/cbindgen.toml`, which
     already carries PR #201's change) and `src/admin/alter_consumer_group_offsets_result.rs`
     (PR #201-old, deleted in -new). Resolution: take the delete; the orphan gate (R2.7 #9) checks
     it.
  5. *Renamed-with-edits files that hold our .NET hunks.* `rust/tests/common/{backend_pool,
     callback_log}.rs` and the 3 `multilanguage_*_test_macro.rs` auto-merge. `admin_backend.rs`
     (4 hunks) and `backend_factory.rs` (1 hunk) conflict.
  6. *A rule file.* Our `consumer-threading.md` §1.1 amendment and #210's path edits auto-merge
     cleanly.
- **Content conflicts by owner.** Hunk counts were read from the conflict markers in tree `T2`.
  - **4 files carry our work:**
    - `.semaphore/semaphore.yml`: 1 hunk in the header comment that lists the blocks. Our
      "Linux amd64" / "macOS arm64" .NET items collide with master's renumbered list. Comment
      only: re-number our items into master's list by hand.
    - `Makefile`: 7 regions. Resolve per D15.
    - `rust/tests/common/admin_backend.rs`: 4 hunks, **all PR #201 old vs new** (SCRAM
      `describe` now returns the harness's `DescribeUserScramCredentialsView`, not the crate's
      `Result`, plus the import list). **Take theirs in every hunk.** Our own .NET delta in this
      file sits outside the hunks and auto-merges: 10 doc lines changing "four backends" to "five
      backends". Keep it. That delta is why the file is on the Mode-A allowlist. Checked: the
      gRPC admin backend that implements the changed trait method is shared across languages and
      lives in the take-theirs `multilanguage_admin.rs`. No .NET-only implementation exists, so
      there is no semantic conflict that merge-tree could have missed.
    - `rust/tests/common/backend_factory.rs`: 1 re-export hunk. Take the **union**: ours is
      theirs' list plus `DotnetGrpcFactory` and `DotnetAsyncGrpcFactory`, under
      `#[allow(unused_imports)]`.
  - **20 theirs** (PR #201 old vs rebased): `c/tests/test_mock_admin.c`,
    `python/{_confluentkafka.c,admin.py,grpc_server.py,grpc_server_async.py}`, 13 `rust/src/**`
    files, `rust/tests/common/multilanguage_admin.rs` and
    `rust/tests/integration/admin_scram_test.rs`.
    ⚠ **Resolve them WHOLE-FILE (`git checkout c0220aab -- <path>`), not hunk by hunk.** Measured
    by resolving every hunk to theirs and diffing against `c0220aab`: 3 of the 20 still differ
    because of **stale PR #201-old lines in regions that merged cleanly**. All are imports that
    PR #201-new deleted:
    - `rust/src/ffi/admin.rs`: `Errors` (4 lines);
    - `rust/tests/common/multilanguage_admin.rs`: `KafkaFuture` plus the
      `DescribeUserScramCredentialsResponseData` wire imports (10 lines);
    - `rust/tests/integration/admin_scram_test.rs`: `DescribeUserScramCredentialsResult` (5 lines).

    Left in place they are unused imports, which fails `cargo xtask lint` (`-D warnings`), and the
    `rust/src/ffi` one is a Rust-core diff, which trips Mode B (R2.9). The other 17 match
    `c0220aab` exactly after hunk resolution. Gate #8 catches all three either way. Evidence:
    `abi-r2/resolve/theirs-residue.txt`.
- **Untracked and ignored files.** A merge-carried move (T1) moves **tracked files only**. About
  241 untracked files, 16 excluded `local-docker-logs` files and the `bin/`/`obj/` trees would be
  left behind in a `bindings/dotnet/` holding only untracked files. A `git mv` of the directory
  carries all of them (proven by R2.0).
- Evidence: `abi-r2/mt-pr201.out` (T1), `abi-r2/mt-premove.out` (T2),
  `abi-r2/modeA-preview-T2.txt`.

### R2.4 S0 recomputed (area D): numbers and deltas vs Rev 1

Rev 1 compared mbase `d6bf7c76` → master `383f3d30`, plus a separate PR #201-only bucket. Rev 2
compares **HEAD `274523ec` → `c0220aab` directly**, which is what the merge actually delivers.

| Metric | Rev 1 | Rev 2 | Δ |
|---|---|---|---|
| Header SHA-1: HEAD / old PR #201 / mbase / master / new PR #201 | `41f48ea8` / `41f48ea8` / `6d3a2699` / `423b7a63` / — | same, plus **`af0f1644`** (`c0220aab`) | new pin |
| Header functions | 811 → 788 (−96 +73), master's own delta | **849 → 826** (−103 +80 = **80 renames + 23 true removals, 0 net-new**) | different base. By arithmetic, HEAD = mbase + 38 PR #201-old functions and `c0220aab` = master + 38 PR #201-new functions; the two sets of 38 were not compared one-to-one |
| Opaque types | — | 114 → 110: 14 renamed, 4 removed (`ConsumerGroupListing`, `ListClientMetricsResourcesResult`, `ListConsumerGroupsResult`, `CorrelationIdMismatchError`) | — |
| Rename families | 13 | **14** (+ `kafka_admin_DeleteAclsFilterResults_*` → `kafka_admin_FilterResults_*`) | +1 |
| .NET `DllImport`s in `src/` | 668 | 668 | 0 |
| … byte-identical | 527 | **557** | +30 (the PR #201-only ones that turned out identical) |
| … type-only (opaque type renamed in the signature; binary-compatible with `IntPtr`) | 22 | 22 | 0 |
| … renamed, same shape | 67 | **74** | **+7**: `kafka_common_{AclBinding,AclBindingFilter}_destroy` → `kafka_common_acl_…` and `kafka_common_ClientQuotaEntity_destroy` → `kafka_common_quota_…` (predicted by Rev 1 S0.2), plus 4 × `kafka_admin_DeleteAclsFilterResults_{count,destroy,get_binding,get_error}` → `kafka_admin_FilterResults_*` (not predicted) |
| … removed | 15 | 15, **same names** | 0 |
| … PR #201-only, unresolved | 37 | **0** (30 identical + 7 renamed) | resolved |
| … shape changed | — | **0** | — |
| **Broken** (renamed + removed) | 82 | **89** | +7 |
| Header-wide signature shape changes | — | 1: `kafka_common_RecordDeserializationError_origin` `bool(const T*, int32_t*)` → `int32_t(const T*)`, from master #209; **not used by .NET** | — |
| Callback typedefs | "25 reshaped" | HEAD → `c0220aab`: 74 → 72; **0 reshaped**, 9 type-only, 2 renamed (`FutureRecordMetadata_get{,_all}_callback_t` → `KafkaFuture_RecordMetadata_*`), 2 removed (`list_client_metrics_resources` and `list_consumer_groups` callbacks) | Rev 1's 25 were master's admin reshapes (mbase → master), which PR #201 already carried onto HEAD; .NET already consumes those shapes |
| `list_transactions` | flagged in round-70 notes | **identical** old/new (`list_transactions_async` + 8 `ListTransactionsResult_*`) | no work |
| Removed functions .NET does not use | — | sync `list_{client_metrics_resources,consumer_groups}`, `ConsumerGroupDescription_state`, `ConsumerGroupListing_state` (comments only in .NET), `CorrelationIdMismatchError_*`, `Error_correlation_id_mismatch`, `kafka_consumer_ConsumerGroupMetadata_new` | — |
| gRPC-server compile sites | 5 | **5, same sites.** Trial-merge protos (`T1`/`T2`: `rust/multilanguage-test-server/proto/*`, identical to `c0220aab`) differ from Rev 1's trial-merge protos (`a52a0275`) **in comments only** | 0 (inferred, not rebuilt; R2.13) |
| Arms (static macro count) | 152 → 151 (115 + 36), 145 executed | HEAD **152** = 116 macro invocations (admin 80 / consumer 14 / producer 22) + 36 async twins (consumer 14 + producer 22) → `c0220aab` **151** = 115 (79 / 14 / 22) + 36; **145** executed (151 − 3 skips × 2 flavors) | 0 |
| `DOTNET_GRPC_SKIPS` (3 names) | exist on master | all 3 exist in the `c0220aab` multilanguage set | 0 |
| Trial-merge conflicts | 20 | 770 merge-carried / **27** with S1a | see R2.3 |

**#209 / #223 impacts re-checked against `c0220aab`:**
- `ConsumerCloseWithTimeout` (`kafka_consumer_Consumer_close_with_timeout`): removed.
  `Consumer_close` and `Consumer_close_async` are present. **D2 (5 s → 30 s) and D3 stand.**
- `ConsumerGroupDescription.State` / `ConsumerGroupState`: ABI accessors removed; .NET derives the
  value in managed code. **D5 stands.**
- ListConsumerGroups and ListClientMetricsResources: removed (14 admin EntryPoints).
- gRPC compile breaks: 5 sites (above).
- #223: no further change. **D7 stands.**

Evidence: `abi-r2/abi.py`, `abi-r2/headers/*.h` (`gen-headers.sh`),
`abi-r2/r-head-vs-pr201new/` (`func-renames.txt` 80 lines, `funcs-removed-unrenamed.txt`,
`cbs-*.txt`, `sigs-*.txt`), `abi-r2/dn_eps.txt` (668), `abi-r2/arm-names.json`.

### R2.5 Where .NET lives (area B): inputs to D13, D14 and D17

- **Path sweep** at `274523ec` over `bindings/dotnet` and `bindings/CLAUDE.md`: **1851 hits**.
  A hit is one (line, pattern) pair, so a line matching two patterns counts twice. The 12 patterns
  are in `path_sweep.py`.

  | Category | Hits | Notes |
  |---|---|---|
  | design-history (frozen) | 1318 | |
  | design-current | 282 | |
  | build/ci | 81 | 43 self-paths, 20 `target/`, 7 `multilanguage-test-server`, 7 `bindings/python` |
  | cs-src | 61 | 41 `src/<rust>` doc cites, 9 `bindings/python`, 9 `bindings/CLAUDE.md` |
  | rule | 51 | 20 self-paths, 13 `src/<rust>`, 10 `bindings/CLAUDE.md`, 3 `target/` |
  | cs-tests | 29 | 23 `src/<rust>` |
  | md-other | 11 | |
  | cs-soak | 11 | |
  | cs-grpc-server | 5 | |
  | other | 2 | |

  Worklist: `abi-r2/sweep-dotnet-hits.txt`. Script: `abi-r2/path_sweep.py`.
- **Rulebook auto-loading (observed this session, not documented behaviour):**
  - Reading a file under `bindings/dotnet/` (before R2.0) auto-loaded `bindings/CLAUDE.md`,
    `bindings/dotnet/CLAUDE.md` **and** `bindings/dotnet/.claude/rules/ffi-marshalling.md`.
  - Reading under the moved `dotnet/` (after R2.0) auto-loaded `dotnet/CLAUDE.md` and
    `dotnet/.claude/rules/ffi-marshalling.md` **but not `bindings/CLAUDE.md`**.

  So relocation silently drops the middle rulebook (D14, R14). It also shows that nested
  `.claude/rules` *do* auto-load, which contradicts `dotnet/CLAUDE.md`'s "never rely on nested
  auto-loading" (§9 item 8).
- **Personas:**
  - The tracked source is `bindings/dotnet/.claude/agents/dotnet-{actor,critic}.md`; it moves with
    the tree.
  - The repo-root discovery copies under `.claude/agents/` are untracked, never committed, and
    unaffected by location. Their *text* cites `bindings/dotnet` paths, so they need a re-copy
    after any persona edit (user, §9).
- **`design/`:** binding-local; it moves with the tree. `design/history/**` stays frozen and keeps
  its old path text.
- **Agent memory**, in two places, and **neither is tracked:**
  - **Binding-local:** `bindings/dotnet/.claude/agent-memory/dotnet-{actor,critic}/`. Only 3
    `.gitkeep` files are tracked; the 137 + 51 memory files on disk are **untracked** and not
    ignored.
  - **Repo-root:** `.claude/agent-memory/dotnet-{actor,critic}/`, 47 + 39 files, also untracked.
    This is the one personas use when spawned from the repo root (`memory: project`).
  - A `git mv` relocation (S1a) carries the first set. A merge-carried move would **strand** it
    under `bindings/dotnet/` (R2.3), which is one more reason for D16(b). The second set is
    untouched. The duplication predates #210 (D17).
- **Local excludes:** `.git/info/exclude` has 2 entries under `/bindings/dotnet/…/local-docker-logs/`,
  which go stale under D13(a) (D17).

### R2.6 Build and native plumbing (area C), old → new

All but item 11 are **functional**: they must change for anything to build or run, whichever D13
option is chosen. Paths assume D13(a) (`dotnet/`).

| # | Site | At `274523ec` | After #210 |
|---|---|---|---|
| 1 | `src/Confluent.Kafka/Confluent.Kafka.csproj:62` `NativeLibraryPath` | `$(MSBuildProjectDirectory)/../../../../target/$(CargoProfileDir)/…` (4 levels up = repo root) | `…/../../../rust/target/$(CargoProfileDir)/…`. ⚠ Not `../../../target`: that resolves to the **stale** root `target/` and silently loads a pre-#209 library (R11) |
| 2 | `grpc-server/Confluent.Kafka.GrpcServer.csproj:69` `ProtoRoot` | `…/../../../multilanguage-test-server/proto` | `…/../../rust/multilanguage-test-server/proto` |
| 3 | `Dockerfile.grpc` / `Dockerfile.grpc.async` | `COPY target/release/libconfluent_kafka.so /src/target/release/…`, `target/include/…`, `bindings/dotnet/{Directory.Build.props,.editorconfig,Confluent.Kafka.snk,src,grpc-server}`, `multilanguage-test-server/proto`; runtime stage `COPY --from=builder /src/target/release/…` | Same files from `rust/target/…`, `dotnet/…` and `rust/multilanguage-test-server/proto`. **The in-image layout must mirror the repo layout** (`/src/rust/…`, `/src/dotnet/…`), because items 1–2 are relative paths. `.dockerignore` already re-includes the `.so` and header; nothing to change there |
| 4 | `dotnet/Makefile` | `RUST_PROJECT_ROOT ?= $(realpath ../..)` (**means the repo root**); `SOLUTION`/`SOAK_DIR`/`PERF_DIR` = `$(RUST_PROJECT_ROOT)/bindings/dotnet/…`; `grpc-native -o $(RUST_PROJECT_ROOT)/target/grpc-native/dotnet`; `cargo build --manifest-path $(RUST_PROJECT_ROOT)/Cargo.toml`; docker `-f $(RUST_PROJECT_ROOT)/bindings/dotnet/Dockerfile.grpc` | Per D15. `REPO_ROOT ?= $(realpath ..)` and `RUST_PROJECT_ROOT ?= $(REPO_ROOT)/rust` (master's meaning). Paths become `$(REPO_ROOT)/dotnet/…`; `GRPC_NATIVE_DIR = $(RUST_PROJECT_ROOT)/target/grpc-native`. Cargo runs **from `rust/`** (`$(MAKE) -C $(RUST_PROJECT_ROOT) build`), because the toolchain pin (`rust/rust-toolchain.toml`) and the `xtask` alias (`rust/.cargo/config.toml`) resolve from the **working directory**, not from `--manifest-path` (R13) |
| 5 | Root `Makefile` | `$(MAKE) -C bindings/dotnet RUST_PROJECT_ROOT=$(RUST_PROJECT_ROOT) …` (9 delegations; 14 `bindings/dotnet` mentions in all); `cargo test … -- __grpc_dotnet $(DOTNET_GRPC_SKIPS)` run **from the repo root**. In `T2` these lines merged **textually unchanged**: git rewrites paths, not file contents. After the merge they pass `RUST_PROJECT_ROOT=<repo>/rust` into a Makefile that reads it as the repo root, and run `cargo` where there is no `Cargo.toml` | Per D15: `$(MAKE) -C dotnet $(ROOTS) <target>`; cargo recipes move to `rust/Makefile`; every dotnet **target name stays** (CI calls them) |
| 6 | `rust/tests/common/backend_pool.rs:206-212` (M17/P1 native launcher, merged) | reads `MULTILANG_DOTNET`, else `root.join("target/grpc-native/dotnet/Confluent.Kafka.GrpcServer.dll")` with `root = CARGO_MANIFEST_DIR` | Already right after the merge (`root` = `rust/`). It needs item 4's `GRPC_NATIVE_DIR` under `rust/target` and nothing else |
| 7 | Soak `build.sh` / `bootstrap.sh`, perf targets | repo-root-relative paths | Re-point per the sweep (cs-soak / build/ci rows) |
| 8 | Header for any .NET tooling | `target/include/confluent_kafka.h` | `rust/target/include/confluent_kafka.h`, always, whatever `CARGO_TARGET_DIR` says. Unchanged caveat: the *library* follows `CARGO_TARGET_DIR`, so a user who sets it breaks item 1, as before |
| 9 | `xtask check_bindings` / `make check-bindings` | master's | Master's target, unchanged by us. Run from the root (R2.7 #7) |
| 10 | Root `target/` (82 GB) | ignored by `/target` | **Stale and no longer ignored** after the merge (`.gitignore` is now `/rust/target`). It shows up as untracked: a `git add -A` hazard and a stale-library hazard (D17, R11, R15) |
| 11 | `.dockerignore` | no dotnet entries (ours = master's) | Optional: exclude `dotnet/**/{bin,obj}` to shrink the build context (§9 item 13; not in P2) |

### R2.7 Gates, restated with working directories (supersedes §6 for execution)

`<repo>` is the repository root. "root" means run `make` from `<repo>`. Every `cargo` command runs
in `<repo>/rust`.

| # | Gate | Command (working dir) | Pass condition |
|---|---|---|---|
| 1 | Header | `cd rust && cargo build --features ffi --release`, then `shasum rust/target/include/confluent_kafka.h` (`<repo>`) | SHA-1 = `c0220aab`'s generated header (**`af0f1644…`**) |
| 2 | P/Invokes | `git -C <repo> grep -h -o 'EntryPoint = "[a-z_A-Z0-9]*"' -- dotnet/src \| wc -l`, then the EntryPoint-vs-header script (`abi-r2/abi.py` style). ⚠ Not `grep -c DllImport`: that counts 685 lines at HEAD, comments included | **653** EntryPoint strings (668 − 15), all unique; 0 missing from the header; 0 with any of the 14 old prefixes; 0 of the 15 removed names; the Prelink test (S2e) green on both TFMs. The 3 EntryPoints outside `src/` (unit tests and soak) are untouched by #209 and `c0220aab` (verified) |
| 3 | Native freshness | `nm -gU rust/target/release/libconfluent_kafka.dylib` (`<repo>`); the same on the container `.so`, sha256 of in-image = staged | present: `kafka_common_KafkaFuture_RecordMetadata_get`, `kafka_admin_FilterResults_count`; absent: `kafka_consumer_Consumer_close_with_timeout`, `kafka_admin_DeleteAclsFilterResults_count`. Root `target/` absent (D17.1(a)); under D17.1(b), `dotnet msbuild dotnet/src/Confluent.Kafka -getProperty:NativeLibraryPath` (SDK 10.0.302 has it) prints a path under `rust/target/` |
| 4 | Unit tests | `make test-dotnet` (root; delegates to `make -C dotnet $(ROOTS) test-dotnet`) | net8.0 + net10.0; pass count = HEAD − recorded deletions + additions, reconciled exactly; 0 failed; no "Test Run Aborted" |
| 5 | grpc-server | `make build-grpc-native-dotnet` (root) | writes `rust/target/grpc-native/dotnet/Confluent.Kafka.GrpcServer.dll`; 0 warnings / 0 errors on both TFMs; `dotnet format --verify-no-changes` clean |
| 6 | Arms | `cd rust && cargo test --features integration-tests,multilanguage-tests --test integration -- --list 2>/dev/null \| /usr/bin/grep -c '__grpc_dotnet'` (runtime count); then `make test-integration-dotnet-native` with `INTEGRATION_TEST_PROTOCOL` unset / `SSL` / `SASL_SSL` (root), plus the container arms `make test-integration-dotnet{,-ssl,-sasl-ssl}` (M8/P1 amd64 recipe, both images rebuilt) | **151** listed (115 sync + 36 async); **145** executed per protocol; all green; surplus greens reconciled too; M17/P1's oracle rule (Python green + .NET red → look at the harness/broker first) |
| 7 | Rust and harness | `cd rust && cargo build && cargo test && cargo xtask format-check && cargo xtask check-generated && cargo xtask lint`; root: `make doc-check`, `make check-bindings`, `make test-integration-python-native` (PLAINTEXT oracle) | all pass; `lint` = clippy + the 4 locally runnable lint-custom rules (D8) |
| 8 | Mode A | `git -C <repo> diff --name-only c0220aab HEAD -- . ':(exclude)dotnet'` | ⊆ the R2.9 allowlist |
| 9 | Orphans | `git -C <repo> ls-tree --name-only HEAD`; `git -C <repo> ls-tree -r --name-only HEAD -- bindings` | no root `src`, `tests`, `xtask`, `generator`, `cbindgen.toml`, `build.rs`, `Cargo.*`, `rust-toolchain.toml`, `.cargo`, `multilanguage-test-server`; `bindings/` empty or as D14 decides |
| 10 | Paths | `git -C <repo> grep -n -E 'bindings/dotnet\|\.\./\.\./\.\./\.\./target\|RUST_PROJECT_ROOT\)/bindings' -- dotnet Makefile rust/Makefile .semaphore ':(exclude)dotnet/design/history'` | 0 functional hits; doc hits only as D9 allows; rule files untouched unless the user acts |
| 11 | S1a purity | `git -C <repo> diff -M --name-status 274523ec <S1a> \| awk '$1!="R100"' \| wc -l`; `ls -d <repo>/bindings/dotnet` | 0 non-`R100` lines out of 743; directory absent. Untracked files carried: record `git -C <repo> ls-files --others --exclude-standard -- bindings/dotnet \| wc -l` **before** S1a, and expect the same number under `dotnet` after it, plus 16 until D17.2 re-points the two exclude lines. R2.0 measured 257 = 241 + 16 logs |
| 12 | CI-only (pending at close) | — | full lint-custom with the Java refs; the 3 Linux and 1 macOS dotnet jobs; the rustdoc and crate-package blocks; job durations against M17/P1's limits (R6) |

### R2.8 Root rules (area E) and CI (area G)

- **E: rules.**
  - #210 adds the one sentence quoted in R2.2 to root `CLAUDE.md` and path-only edits to three
    root rule files. Section numbering does not change beyond #209's, so Rev 1's §9 item 1
    renumber map stands.
  - Our `consumer-threading.md` §1.1 amendment auto-merges (T2: no conflict).
  - Root `CLAUDE.md` now says "run the `cargo` commands from `rust/`". The binding rulebooks still
    say `cargo build --features ffi` / `target/include/…` from the root: drift, §9 item 9.
  - Rule-change *ideas* go in §9 only, and agents edit none of them.
- **G: CI.**
  - The 4 .NET jobs run `./.semaphore/install-dotnet.sh` and root `make` targets only
    (`verify-dotnet`, `test-integration-dotnet-ssl`, `test-integration-dotnet-sasl-ssl`,
    `verify-dotnet-macos-docker`). They contain no repo paths, so they are unaffected **if D15
    keeps the target names**.
  - The semaphore conflict is one header-comment hunk; re-number our items into master's list by
    hand (R2.3).
  - Our two `.semaphore/*.sh` files contain no `bindings/` or `target/` paths.
  - `fetch-java-refs` is now `(cd rust && …)` in master's lines: take theirs.
  - **#217:** a master-only, manually triggered promotion ("Publish to crates.io" →
    `publish-crates-io.yml`). `rust/Cargo.toml`'s `include` list excludes `/src/ffi/**`, so the FFI
    is not in the published crate, and .NET builds from the repository, not from crates.io.
    **No .NET impact.**
  - Other new CI blocks are Rust-only: "Check Rust docs" and "doc-check" come from #209, and
    "Check crate package" from #210.

### R2.9 Mode A/B gate (area F), and D12

- **Mode A holds** if the Actor takes `c0220aab` **whole-file** for every non-.NET conflicted file
  (R2.3 ⚠). Verified on T2: every non-.NET content conflict is PR #201 old-vs-new. Before
  resolution, T2's diff against `c0220aab` outside `dotnet/` is 38 files: the 20 theirs, 16
  allowlisted paths and the 2 orphans (`abi-r2/modeA-preview-T2.txt`). Whole-file resolution of the
  20 and deletion of the 2 orphans leaves exactly the 16 allowlisted paths. D15(a) then adds
  `rust/Makefile` as the 17th.
- **Allowlist.** After S1–S3, `git diff --name-only c0220aab HEAD` outside `dotnet/` must be a
  subset of:
  - `bindings/CLAUDE.md` (or its D14 destination);
  - `.claude/rules/consumer-threading.md` (our §1.1);
  - `.semaphore/{semaphore.yml,install-dotnet.sh,dependencies-macos.sh}`;
  - root `Makefile` (dotnet hunks) and `rust/Makefile` (D15(a): `test-integration-grpc-dotnet{,-native}`
    plus `DOTNET_GRPC_SKIPS` only);
  - `rust/tests/common/{admin_backend,backend_factory,backend_pool,callback_log,multilanguage_admin_test_macro,multilanguage_consumer_test_macro,multilanguage_test_macro}.rs`;
  - the pre-existing branch-only `COMMENTS.DONE.{50,51}.md` and
    `design/current/python-binding-send-batching.md` (from `0204437a`, untouched by P2).
- **Mode B triggers.** Any diff vs `c0220aab` in `rust/{src,build.rs,cbindgen.toml,Cargo.*,xtask,generator,multilanguage-test-server,rust-toolchain.toml,.cargo}`,
  `python/**` or `c/**` means stop and re-present to the user.
- **`rust/Makefile` is new to the allowlist.** It is a Rust-owned file receiving Make recipes, not
  Rust code: the same class as M17/P1's `tests/common/*` harness edits. Approving D15(a) approves
  it.
- **D12 re-evaluated: CONFIRMED DROP.** `c0220aab` replaces the tuple alias that `2d9f5325`
  patched with `seed_scram_result(rows: Vec<SeedRow<'_>>)` (`rust/src/ffi/admin.rs:26068`), so our
  fix is moot. Take theirs. Clippy on the merged tree was **not run** (R2.13); gate 7 decides.

### R2.10 Decisions

**Status of Rev 1's decisions:**

| # | Rev 2 status |
|---|---|
| D1 | **SATISFIED.** A's trigger happened; B and C are retired. |
| D1b | **RETIRED**, replaced by D18. |
| D2, D3, D4, D5, D6, D7, D10 | **STAND** (re-checked in R2.4). |
| D8 | **CHANGED (command only):** the user runs `(cd rust && cargo xtask fetch-java-refs)` once before S4. |
| D9 | **CHANGED (scope):** see below. |
| D11 | **CHANGED (extended):** the non-compiling window now spans S1a → S3 (see below). |
| D12 | **CONFIRMED DROP** (R2.9). |

**D9 (changed).** Rev 1 scoped the sweep to root-`CLAUDE.md` cite drift. Rev 2 adds **path drift**:
`bindings/dotnet` → `dotnet`, `src/…` → `rust/src/…`, `target/…` → `rust/target/…`,
`bindings/python` → `python`, and `bindings/CLAUDE.md` → its D14 destination.
- (a) **Recommended.**
  - **Functional** paths (R2.6) are mandatory, in S1b.
  - **Doc** path *prefixes* in tracked non-rule .NET files (cs-src, cs-tests, cs-soak, md-other,
    `design/current`) are rewritten mechanically, also in S1b. Prefixes only; cited line numbers
    are not re-derived.
  - `design/history/**` stays frozen.
  - Rule files and personas are the user's (§9).
  - Rev 1's root-cite rule (touch only unambiguous root-`CLAUDE.md` cites) still holds.
- (b) Functional paths only; docs in a later phase.

*Consequence of (a):* one more mechanical commit in S1b. It covers about 400 sweep hits outside
history and rules (282 `design/current`, 106 C#, 13 md/other). The 81 build/ci hits are the
functional set and are done regardless.

**D11 (extended).**
- S1a breaks the .NET build: the relative paths point one level too high.
- The merge commit leaves it broken.
- S1b restores the library and unit-test build.
- The unit tests can still fail at run time on renamed EntryPoints until S2.
- S3 restores the gRPC server.

Still **accept**: nothing is pushed mid-phase. Bisection across S1a → S3 is lost, and that is
accepted.

#### D13: Where the .NET binding lives (USER)

- **(a) `dotnet/` at the repo root, as a sibling of `rust/`, `python/` and `c/`. Recommended.** It
  matches #210's layout. git's own directory-rename detection already infers it (T1). It keeps the
  relocation separable from the ABI work (S1a and S1b, apart from S2 and S3).
- (b) Keep `bindings/dotnet` by merging with `-c merge.directoryRenames=false` (not trialled).
  - `bindings/` then survives on our branch only, holding `dotnet/` and `CLAUDE.md`, unlike
    `python/` and `c/`.
  - The setting is needed for **this** merge only: afterwards the merge base for later master
    merges is past #210, so nothing re-infers the rename.
  - The S1b plumbing is needed anyway. Items 1–2 keep today's depth and only insert `rust/`
    (`../../../../rust/target`, `../../../rust/multilanguage-test-server/proto`).
  - The D14 problem disappears, because `bindings/CLAUDE.md` keeps auto-loading.
  - The cost is a permanent layout divergence from master, which every later master-side tooling
    change (Makefiles, Docker contexts, `.dockerignore`) has to work around.
- (c) Another name (`csharp/`, `bindings/dotnet` → `net/`, …). There is no precedent.

*Consequence of (a):*
- 743 tracked files and about 257 untracked ones move.
- Every functional path in R2.6 changes, and the D9 doc sweep follows.
- The plan's own home moves (`dotnet/design/history/M17/P2-…`).
- D14 becomes mandatory, because `bindings/CLAUDE.md` stops auto-loading.
- The root `CLAUDE.md` sentence should mention `dotnet/` (§9 item 7, master's file).

*Interaction with D11:* S1a is a non-building commit. See D16 for why it is still preferred.

#### D14: `bindings/CLAUDE.md` (USER; it is a rule file, so agents do not move or edit it on their own)

- **(a) A pure `git mv` to `dotnet/.claude/rules/bindings.md`, plus one explicit link from
  `dotnet/CLAUDE.md`. Recommended.** Nested `.claude/rules` were observed to auto-load for .NET work
  (R2.5), and the link covers the case where that observation does not hold. The content is
  unchanged. Only the .NET binding ever used the file: master never had it, and `python/` and `c/`
  carry no copy.
- (b) Leave it as an orphan at `bindings/CLAUDE.md` (resolving the implicit-rename conflict
  in-place) and link it from `dotnet/CLAUDE.md`. It does **not** auto-load, and `bindings/` survives
  as a one-file directory.
- (c) Root `.claude/rules/bindings.md`. It loads for every agent, including the Rust-core ones (a
  context cost), and it lands in master's lane.
- (d) Fold it into `dotnet/CLAUDE.md`. That is the biggest edit, and the separate cross-binding
  identity is lost.

*Consequence:*
- Whichever option is chosen, the cites of `bindings/CLAUDE.md` need repointing:
  - 9 C# lines and 5 `design/current` lines (under D9);
  - 10 rule-file lines (8 in `dotnet/CLAUDE.md`, 1 in `ffi-marshalling.md`, 1 in the
    dotnet-critic persona), which the user does;
  - 39 more in `design/history` stay frozen.
- Execution: the user moves it, or **explicitly authorizes** the dotnet-actor to do the pure
  `git mv` in S1a or S1 (no content change).

#### D15: Makefile topology after #210

- **(a) Master's pattern. Recommended.**
  - `dotnet/Makefile` takes `REPO_ROOT` / `RUST_PROJECT_ROOT` (R2.6 #4).
  - The root `Makefile` delegates with `$(MAKE) -C dotnet $(ROOTS) …`, keeping every target name.
  - `rust/Makefile` gains `test-integration-grpc-dotnet{,-native}` plus `DOTNET_GRPC_SKIPS`,
    mirroring the Python and C targets, and the root targets delegate there.
- (b) Keep the cargo recipes in the root `Makefile` as `cd rust && cargo test …`. That touches
  fewer files but diverges from master's orchestration, and the root `Makefile` hunk stays large.

*Consequence of (a):*
- `rust/Makefile` joins the Mode-A allowlist (R2.9).
- `RUST_PROJECT_ROOT` changes meaning inside `dotnet/Makefile` (repo root → `rust/`), so every use
  must be audited (R12).
- The 7 root-`Makefile` conflict regions resolve to master's text plus thin dotnet delegations.

#### D16: Sequencing of the move

- **(b) A pre-merge pure-move commit, S1a (`git mv bindings/dotnet dotnet`). Recommended.** It
  depends on D13(a).
  - **Verified:** it cuts the merge from 770 to **27** `CONFLICT` lines (T2), leaving only the
    real ones.
  - **Proven by R2.0:** it carries the untracked and ignored files.
  - It is reviewable as 743 × `R100`.
- (a) Let the merge carry the move (T1). The merge commit mixes 743 location resolutions with the
  real ones, and about 257 untracked files plus `bin/`/`obj/` stay stranded under
  `bindings/dotnet/` and must be moved by hand.

*Interaction with D11:* (b) adds one more non-building commit (S1a) ahead of the merge. Accepted
under D11.

#### D17: Working files the restructure exposes (USER: local state, not repo content)

1. **The stale 82 GB root `target/`.**
   - **(a) The user deletes it. Recommended.** Nothing uses it after #210: the workspace's default
     target dir is `rust/target`, and moving it there would not help, because cargo fingerprints
     record source paths, which all changed. Expect one full rebuild either way.
   - (b) Add `/target/` to `.git/info/exclude`.

   Either way it must be gone or excluded **before** S1. Otherwise it shows as untracked, and a
   wrong `NativeLibraryPath` can quietly load a pre-#209 library from it (R11, R15).
2. **`.git/info/exclude`.** Re-point the two `/bindings/dotnet/…/local-docker-logs/` entries to
   `/dotnet/…` after S1a (user; the "local notes via info/exclude" convention).
3. **Untracked files under `bindings/dotnet`** (241, plus 16 excluded logs, plus `bin/`/`obj/`).
   This includes **all** the binding-local agent memory (R2.5). S1a carries them (D16(b)).
   Nothing else to do.
4. **Agent memory** in two places, both untracked (R2.5). **Recommended: no change in P2.** Record
   it as §9 item 11. Whether to consolidate, and whether to track memory at all, is a separate
   user-owned decision.

#### D18: Merge source (replaces D1b)

- **(a) Merge `c0220aab` now. Recommended.** It contains `c7dd21bf`. The 20 PR #201 conflicts
  resolve whole-file to `c0220aab` (R2.3 ⚠), and the Mode-A gate is a plain diff against
  `c0220aab`.
- (b) Wait for PR #201 to land on master. The gate becomes `git diff origin/master`, and the wait is
  of unknown length.
- (c) Merge master `c7dd21bf` alone. Not viable: our branch carries old PR #201 code that would
  need #209's conventions re-applied (Rev 1 Option B, Mode B).

*Consequence of (a):*
- If PR #201 is amended again before landing, a later master merge brings only the delta.
- If it lands squashed with identical content, the later 3-way merge should see no change on
  those files. That is reasoned, not trialled (R16).
- S0 re-checks the tip on merge day.

### R2.11 Revised stage list (replaces §4's list; §4's S2/S3 bodies stand)

**Why restructured:** #210 adds a relocation and plumbing workstream that is independent of the
ABI work. Isolating it gives four things:
- S1a is a pure rename, reviewable as `R100` (verified 743/743).
- The merge shrinks from 770 to 27 conflicts (verified).
- Untracked files are carried (proven).
- S2 and S3, the ABI review surface, stay free of path noise.

The content of S2–S5 is unchanged apart from paths and numbers.

| Stage | Owner | Mode | Content | Gate |
|---|---|---|---|---|
| **S0** | Manager | read-only | **Done for Rev 2** at `c0220aab` (R2.3/R2.4). On merge day: `airlock git fetch origin`. If `origin/master` ≠ `c7dd21bf`, the PR #201 tip ≠ `c0220aab`, or PR #201 has landed: re-run `abi-r2/` (headers, `abi.py`, merge-tree, arms) and re-present if anything material changed. **Precondition:** R2.0 is reverted (`HEAD` = `prashah_dev_dotnet_binding` @ `274523ec`, no `r2-move`, `bindings/dotnet/` present). | — |
| **S0u** | **User** | — | Revert R2.0. Decide D13–D18. Do D17.1 and D17.2. Optionally D8 (fetch-java-refs). | — |
| **S1a** | dotnet-actor 89 | A | `git -C <repo> mv bindings/dotnet dotnet`; commit `refactor(dotnet): move bindings/dotnet to dotnet/ (#210 layout, M17/P2)`. Plus D14's `git mv` only if the user authorized it. Nothing else. | #11 |
| **S1** | dotnet-actor 89 | A | `git merge c0220aab` (D18) → 27 conflicts. **`git checkout c0220aab -- <the 20 theirs>` (whole-file, R2.3 ⚠)**; `git rm` the 2 modify/delete orphans; hand-merge the 4 ours (R2.3: semaphore comment, `Makefile` per D15, `admin_backend.rs` theirs-in-hunks + keep the "five backends" doc lines, `backend_factory.rs` union); `bindings/CLAUDE.md` per D14. Nothing is staged with `-A` / `.` (R15). | #1, #7 (Rust parts), #8, #9 |
| **S1b** | dotnet-actor 89 | A | Plumbing R2.6 #1–#7, D15 (`dotnet/Makefile`, root delegations, `rust/Makefile` dotnet targets plus `DOTNET_GRPC_SKIPS`), then the D9 doc-prefix sweep as its own commit. | #10; `dotnet build` of library and tests on both TFMs |
| **S2** | dotnet-actor 89 | A | Rev 1 §4 S2a–S2f, with R2.4 numbers: **74** renames in **14** families, 22 type-only doc touches, **15** removals, `DllImport` 668 → 653; the Prelink test; D7 if approved. | #2, #3, #4 |
| **S3** | dotnet-actor 89 | A | Rev 1 §4 S3: the 5 sites. `DOTNET_GRPC_SKIPS` now lives in `rust/Makefile` (D15(a)), with the same 3 names. | #5, #6 |
| **S4** | dotnet-critic 89 (plus kafka-critic 89 only in Mode B) | — | One Critic pass at the end. Scope: Rev 1's plus S1a purity, S1's .NET hunks and the Mode-A allowlist, S1b plumbing (in-image layout mirrors the repo, cargo from `rust/`, no stale-`target/` path), and the D14 outcome. | #8, #10, plus the review |
| **S5** | Manager | — | Rev 1 §4 S5. Archive to `dotnet/design/history/M17/P2-…/COMMENTS.DONE.89.md`. The working comment files are the binding-root `dotnet/COMMENTS.89.md` / `COMMENTS.DONE.89.md` (binding `CLAUDE.md` §8.4); never stage the latter. Memory; N=90 next. | #12 recorded as pending |

### R2.12 New and changed risks

| # | Status | Risk | Mitigation |
|---|---|---|---|
| R1 | **RESOLVED** | PR #201 rebase timing | It happened (`c0220aab`). |
| R2 | **RESOLVED** | PR #201 reshapes its 37 EntryPoints | 7 renames, 0 shape changes (R2.4). |
| R3 | STANDS | Master moves before S1 | S0 on merge day. |
| R11 | new | Relocation path breakage. Worst case is *silent*: a `NativeLibraryPath` "fixed" to the repo-root `target/` loads the stale pre-#209 library. | D17.1 (delete root `target/`); gate #3 freshness-by-symbol; gate #10. |
| R12 | new | `RUST_PROJECT_ROOT` means the repo root in our `Makefile`s but `rust/` in master's `ROOTS`. A delegation that passes `$(ROOTS)` into an unconverted `dotnet/Makefile` points `SOLUTION` at `rust/bindings/dotnet/…`. | D15(a) converts `dotnet/Makefile` to master's meaning in the same commit as the delegation; gate #4. |
| R13 | new | Cargo invoked outside `rust/` (`--manifest-path`) silently uses the default toolchain instead of `rust/rust-toolchain.toml` 1.95.0, and has no `xtask` alias. | All cargo goes via `$(MAKE) -C rust …` or `cd rust && …` (R2.6 #4, R2.7). |
| R14 | new | `bindings/CLAUDE.md` stops auto-loading for .NET work after the move (observed), so agents lose its guidance without any error. | D14 before S1a; the spawn briefs name the file explicitly until D14 lands. |
| R15 | new | The 82 GB root `target/` becomes unignored: `git status` noise, and a `git add -A` disaster. | D17.1 before S1; the briefs forbid `git add -A` / `git add .`. |
| R16 | new | PR #201 is force-pushed again, or lands, before S1. | S0 re-check; if it landed, switch D18 to `origin/master` (same gate shape). |
| R17 | new | Stale local state: `.git/info/exclude` entries, the dual agent-memory location, persona discovery copies citing old paths. | D17.2 / D17.4; §9 item 11. |
| R18 | new (process) | An unchecked `cd X; git …` chain runs in the main repo (R2.0). | §1 rule: `git -C <path>`, `&&`, explicit SHAs, no clones. |
| R19 | new | Hunk-level "take theirs" leaves **stale PR #201-old lines** in regions that merged cleanly: measured in 3 of the 20 files, 19 lines, including the Rust core `rust/src/ffi/admin.rs` (R2.3). Result: lint red (unused imports) and a spurious Mode-B trip. | Whole-file `git checkout c0220aab --` for the 20 (S1); gate #8 byte-compares every non-allowlisted path against `c0220aab`. |

### R2.13 Not verified in Rev 2

- **No real `git merge` or worktree trial merge.** A scratch clone was blocked ("transport 'file'
  not allowed"), and after R2.0 I created no worktree. The conflict sets come from
  `git merge-tree --write-tree` (T1, T2). Only the take-theirs half of the resolution was
  *simulated*, file by file from `T2`'s conflict markers (R2.3 ⚠, R19). The 4 hand-merges and
  D15's Makefile rewrite were not attempted.
- **Runtime arm count** (`cargo test -- --list`): only the static macro count was done. Gate #6
  covers it.
- **gRPC-server build against the new protos:** not rebuilt. The 5 sites are inferred from the
  protos differing only in comments from Rev 1's built oracle.
- **No build, clippy or lint-custom of the merged tree.** Only `c0220aab`'s header was generated
  (`cargo check --features ffi --lib` on an archived tree).
- **CI** (Linux container arms, macOS job, durations, R6's 30 s Dispose impact).
- **Nested `.claude/rules` auto-loading as documented harness behaviour.** It was observed twice
  this session but is not checked against docs; D14(a) adds an explicit link for that reason.
- **That a merge-carried move strands untracked files:** this follows from git semantics and was
  not run. That `git mv` carries them **was** observed (R2.0).
- **That PR #201 will not move again.**

---

## 0. Scope

> **[Rev 2: scope widened; numbers superseded.]** Master now has **six** commits we lack. Beyond
> #223 and #209 there are #218 and #216 (RFC docs), **#210** (the repository restructure, R2.2)
> and #217 (crates.io promotion, R2.8). The merge source is the rebased PR #201 `c0220aab`, which
> contains all six (D18).
> - "67 EntryPoint renames" → **74**, in 14 families (R2.4).
> - In-scope item 1 now includes the relocation and plumbing stages S1a/S1b (D13–D16).
> - Item 3's harness paths move under `rust/tests/common/`.
> - Item 4 is widened to path drift (D9).
>
> The out-of-scope list is unchanged.

Master has two commits our branch does not have yet:

| Commit | PR | What it does | .NET impact |
|---|---|---|---|
| `0085b32d` | #223 | Consumer and admin get a default `client.id`. A new constructor error when `group.instance.id` is set without a group. | None required (E5). Optional pinning test (D7). |
| `383f3d30` | #209 | Restricts the Rust public surface to Java's public API. Drops Java-deprecated API. Renames the C ABI to Java package names. Adds the "forward compatibility" rules and `cargo xtask lint-custom`. 884 files. | Large. 67 EntryPoint renames, 15 EntryPoints removed, the Java-deprecated .NET surface goes, the harness protos lose 2 RPCs and a field, and the arm count drops by 1 (E2–E4). |

**In scope:**

1. The merge of master into `prashah_dev_dotnet_binding`. Its shape depends on D1 (§3).
2. The .NET library:
   - rename 67 EntryPoint strings;
   - remove 15 EntryPoints and the managed surface built on them;
   - remove the other Java-deprecated members (`ConsumerGroupDescription.State`, `ConsumerGroupState`);
   - move sync `Dispose` off the removed `close_with_timeout`;
   - update the unit tests and XML docs.
3. The .NET gRPC server: 5 compile breaks. Plus the .NET harness bits of the shared Rust harness
   (`tests/common/backend_factory.rs`, `admin_backend.rs`, `Makefile`, `semaphore.yml`).
4. Doc and cite drift in tracked .NET non-rule files (D9).
5. STATUS, the review record and memory.

**Out of scope:**

- A Rust or C-ABI `close(CloseOptions)`. That is D4, a Mode-B follow-up.
- Any Python change.
- The `GroupMetadata` RPC and the 3 transaction skips. They stay skipped and unchanged.
- Editing rule files (§11 lists suggestions for the user).
- Anything PR #201's author owns.

---

## 1. Standing constraints (relay verbatim to every agent)

> **[Rev 2: amended.]** Once S1a lands, `bindings/dotnet/…` paths below read `dotnet/…`. The
> no-edit list gains `bindings/CLAUDE.md` at whatever destination D14 picks. Freshness by symbol is
> extended in R2.7 #3. `cargo` runs from `rust/`, and the header lives at `rust/target/include/`.
> The last bullet is new (R2.0).

- NO push, NO merge to master, NO force operations. Commit only on `prashah_dev_dotnet_binding`,
  and only inside the approved stages.
- Network git only via `/usr/local/libexec/airlock-agent/git`. Never bypass
  `protocol.ssh.allow=never` or the `insteadOf` rules any other way. `cargo xtask fetch-java-refs`
  uses plain git, so it is **not** run by agents (D8).
- Never stage `COMMENTS.DONE.<N>.md`, the repo-root `.claude/agents/dotnet-*.md` discovery copies,
  `.DS_Store`, or untracked local notes (`PR-196-overview.md`, `ADMIN-API-PARITY.md`,
  `PendingAdminClientFindingsForDotnet.md`, `Dotnet-AdminClient-Findings-Workflow/`).
  Don't touch the last two at all.
- Agents don't edit rule files: `CLAUDE.md`, `.claude/rules/*`, `bindings/**/CLAUDE.md`,
  `bindings/dotnet/.claude/rules/ffi-marshalling.md`, persona files. Collect suggestions in
  `COMMENTS.DONE.89.md`.
- No messages to anyone outside this session. Don't contact PR #201's author.
- Shell traps:
  - Export `PATH` at the start of every Bash call.
  - `grep` is ugrep, so use `/usr/bin/grep`. There is no `sed`. `cat` is bat, so use `/bin/cat`.
  - zsh: write `"${T}:path"`, never `$T:path`, which is expanded as a `:t` modifier.
  - `git rev-parse --short` with more than one rev fails.
  - `PYTHONDONTWRITEBYTECODE=1`.
  - Zero-match filters pass silently (libtest `running 0 tests`, xUnit filter). git-grep ERE has
    no `\b`, so use `-w`. `grep -c` exits 1 on zero. "Test Run Aborted" can still exit 0.
- Scratch builds only under the session scratchpad, never in the repo tree.
- Bound every tool output.
- Never touch the user's `kafka-perf` Docker container. `pgrep` for orphan native gRPC servers
  before and after every native run, and kill them.
- Rebuild the native library after the merge. A `.so` or `.dylib` built before the merge is stale
  by construction (the lesson recorded twice in M15/P13). Prove it is fresh by symbol:
  `kafka_common_KafkaFuture_RecordMetadata_get` is present and
  `kafka_consumer_Consumer_close_with_timeout` is absent.
- **[Rev 2, from R2.0] Git hygiene in the main working tree.**
  - Every git command names its repo: `git -C <repo> …`. Never `cd X; git …`.
  - Chain with `&&`, never `;`, so a failed step stops the chain.
  - Use explicit SHAs or the approved branch name, not "whatever HEAD is".
  - No clones and no worktrees of the main repo, and no branch creation, unless the stage says so.
  - Run `git -C <repo> status --short | head` before and after any mutating command.
  - Stage named paths only. Never `git add -A` or `git add .`; the stale root `target/` is
    unignored after #210 (R15).

---

## 2. Verification of main's E1–E6 (Manager, independent)

> **[Rev 2: historical.]** This section is pinned to `383f3d30` + `3b27d2c9`. Its scratch folder
> `m17p2/` **no longer exists**. Current numbers are in R2.3 and R2.4.
>
> | Item | Rev 2 status |
> |---|---|
> | **E1** | 20 conflicts → **27** with S1a (770 without), all paths now under `rust/`, `python/` and `c/`. |
> | **E1+** | `2d9f5325` is moot (D12). `ece8bb74`'s admin_backend delta is the "five backends" doc lines (R2.3). |
> | **E2** | Superseded by R2.4: 557 / 22 / 74 / 15 / 0, 14 families. The "3 more `DllImport`s in unit tests" re-verified as 3 EntryPoints outside `src/`, none affected. |
> | **E3, E5** | Stand, re-checked against `c0220aab`. |
> | **E4** | Stands: 5 sites, 151 arms, 145 executed. |
> | **E6** | The renumber map stands. The `fetch-java-refs` command changed (D8). |

Scratch evidence is in
`/private/tmp/claude-501/…/ac40d0c9-…/scratchpad/m17p2/` (`mt.out`, `abi.json`, `renames.txt`,
`dn_eps.txt`, `grpc2/`, `lint-*.log`, `ids-*.txt`, `arm-tests.txt`).

| # | Main's claim | Verdict | Evidence |
|---|---|---|---|
| E1 | The trial merge of `origin/master` into HEAD has 20 conflicts, mostly from PR #201. | **Confirmed 20. Attribution corrected.** 17 are PR #201's: 10 `src/` + 4 `bindings/python/` + `tests/common/{admin_backend,multilanguage_admin}.rs` + **`cbindgen.toml`**. 3 are ours: `Makefile` (one `.PHONY` hunk), `.semaphore/semaphore.yml` (one header-comment hunk) and **`tests/common/backend_factory.rs`** (one trivial re-export hunk). Main had `cbindgen.toml` and `backend_factory.rs` swapped. | `git merge-tree` (tree `a52a0275…`), `mt.out`. PR #201 alone against master conflicts on 18 files; the extra one is an agent-memory file (`mt-pr201.out`). `3b27d2c9` does not contain #209, and no rebased PR #201 branch exists on origin. |
| E1+ | (new) | **What our branch owns in those files.** Only one branch-only commit touches the Rust core: `2d9f5325` (a clippy `type_complexity` fix in `src/ffi/admin.rs`, on PR #201's code). The other branch-only deltas in conflicted files are .NET harness wiring: `ece8bb74` in `admin_backend.rs`, plus `backend_factory.rs`, `Makefile` and `semaphore.yml`. The remaining differences between HEAD and `3b27d2c9` came from master merges. | `git log HEAD ^origin/master ^3b27d2c9 -- src/ … bindings/python bindings/c xtask generator` returns 1 commit, and `-- tests/` returns 4 commits (all .NET harness). |
| E2 | The ABI goes from 811 to 788 functions; .NET's 668 EntryPoints = 631 on master + 37 PR #201-only. | **Confirmed. The 549 "unchanged" is refined.** 811 → 788 (96 removed, 73 added). The 668 split as **527** byte-identical + **22** whose only change is renamed opaque type names in the signature + **67** renamed + **15** removed + **37** PR #201-only. The 67 renames fall in **13 families**: the 12 opaque-type renames plus `kafka_producer_FutureRecordMetadata_*` → `kafka_common_KafkaFuture_RecordMetadata_*` (.NET uses 4 of them: `destroy`, `destroy_all`, `get`, `get_all`). Every renamed signature is identical modulo the type map. All 13 old and new types are opaque (`uint8_t _private[0]`), so the 22 type-only changes are binary-compatible with `IntPtr`. The 163 enum and `#define` lines are identical. | `abi.py` / `renames.txt` / `dn_eps.txt`. HEAD has 668 `DllImport`s in `src/`, 668 unique names. The unit tests have 3 more `DllImport`s, all unaffected. HEAD header SHA-1 = `41f48ea8…`. Main's `abi/old` = `6d3a2699…`, `abi/new` = `423b7a63…`. |
| E2+ | (new) | The removed `kafka_common_CorrelationIdMismatchError_t` and `kafka_common_Error_correlation_id_mismatch` are **not** used by .NET. 25 tracked `bindings/dotnet` files (207 lines) name an old ABI type or function, in EntryPoints, XML docs or comments. That is the doc-sweep surface. | `git grep` at HEAD. |
| E3 | The Java-deprecated .NET surface is ListConsumerGroups*, ListClientMetricsResources*, ConsumerGroupState / `ConsumerGroupDescription.State`, and consumer `Close(TimeSpan)`. Admin `Close(TimeSpan)` stays. | **Confirmed. Three additions.** In Java 4.3.1: `Admin.listConsumerGroups` is `@Deprecated(since="4.1", forRemoval=true)` (`Admin.java:889`); `listClientMetricsResources` likewise (`:1823`); `ConsumerGroupDescription.state()` and `ConsumerGroupState` are `(since="4.0", forRemoval=true)`; `Consumer.close(Duration)` is `@Deprecated` (`Consumer.java:282`, `KafkaConsumer` `since="4.1"`), replaced by `close(CloseOptions)` (`:288`). No other .NET member maps to an entry in master's `design/current/java-deprecated.txt`. Admin `Close(TimeSpan)` is not deprecated and stays. **(a)** `IConsumer.Close(TimeSpan)` (`IConsumer.cs:372`) is **not** `[Obsolete]` today, unlike the admin members. **(b)** Sync `Dispose` (`NativeConsumer.cs:3106`) calls the removed `Consumer_close_with_timeout(…, 5_000)`. Its only replacement is `Consumer_close`, whose bound is the core default `CloseOptions::DEFAULT_CLOSE_TIMEOUT_MS` = 30 s (= Java `ConsumerUtils.DEFAULT_CLOSE_TIMEOUT_MS`), so sync Dispose goes from 5 s to 30 s (D2). **(c)** Master's C ABI has no `close(CloseOptions)`, so after P2 .NET has **no** timed consumer close. Java's non-deprecated replacement is a Mode-B gap (D4). Python has no timed close either: its `close(timeout=None)` ignores `timeout` on both HEAD and master and always calls `Consumer_close_async`. | `kafka/…` 4.3.1 source; master `src/ffi/consumer.rs:4077`; `bindings/python/consumer.py:827,1054` at both refs. |
| E4 | grpc-server has 4 compile breaks; the arms go from 152 to 152 (+1/−1). | **Corrected. 5 sites and 151 arms.** Built in scratch against the **trial-merge** protos: `AdminServiceImpl.cs:572` (ListConsumerGroups), `:1284` (ListClientMetricsResources), `TranslateAdmin.cs:949` (`ConsumerGroupListingToProto`, **missed by main**), `:1051` (the client-metrics listing), and `ConsumerServiceImpl.cs:602-604` (`request.HasTimeoutMs`, because master reserves `ConsumerCloseRequest.timeout_ms`). Compiling against pure master protos is the wrong oracle: it adds 2 false breaks from PR #201's proto fields (`DescribeUserScramCredentialsEntry`, `LogDirDescription.IsCordoned`). **Arms:** master removes `test_list_client_metrics_resources_lists_subscription` and `test_ml_admin_list_groups_and_list_consumer_groups_show_live_group` and adds `test_ml_admin_list_groups_shows_live_group`. `…_filters_restrict_the_listing` **already exists** on our branch; it is not new. So the arms go 152 → **151** (116 + 36 → 115 + 36, admin 80 → 79), and **146 → 145 executed** after the 6 transaction skips. The 3 `DOTNET_GRPC_SKIPS` names still exist on master. `GroupMetadata` is used only by a skipped test. | `grpc2/` scratch build; `ids-{head,master}.txt`; `arm-tests.txt` (116 bases). |
| E5 | #223 needs no .NET change. | **Confirmed: no required change.** The default is applied in the core: consumer `consumer-<group.id>-<group.instance.id or sequence>` (Java-shaped), admin `adminclient-<n>`. **But it is observable in .NET:** `IConsumerCommon.ClientId()` (`:343`) returns it on a real consumer built without `client.id`. No .NET test assumes an empty default (`PublicConsumerClientIdTests.cs` sets explicit ids or uses the mock's `"mock-consumer"`), so nothing breaks. There is also the new `group.instance.id` validation at construction. | `git show 0085b32d` (7 files, all `src/` + one core test). |
| E6 | The root CLAUDE.md is renumbered; `lint-custom` is new. | **Confirmed, with detail.** Map: §3 FFI → §4, §3 Tests → §5, §4 → §6, §5 → §7, §6 → §8, §7 → §9, §8 → §10, §9 → §11, §10 → §12, §11 → §13, §12 → §14, §13 → §15. New §3 is "Forward compatibility", which includes "deprecated API MUST NOT be translated". **Rule-file cites that go stale:** `bindings/dotnet/CLAUDE.md` 2× §11 (`:226`, `:528`) and 1× §9.5 (`:718`); `ffi-marshalling.md` 4× §3 (`:781`, `:1825` are root §3; `:1833`, `:1865` look like `bindings/dotnet/CLAUDE.md §3`), 2× §12 (`:651`, `:719`), 1× §11 (`:1144`), 1× §9.5 (`:995`); persona `dotnet-actor.md:33` 1× §3 (probably the binding's own CLAUDE.md). **Non-rule tracked cites:** about 44, but many ".NET CLAUDE.md §3" in `src/` mean **`bindings/dotnet/CLAUDE.md` §3's idiom map**, not root, so each cite needs disambiguating. **lint-custom:** on a synthetic tree built to be the least the merge can contain, 4 of 6 rules pass with counts identical to pristine master (817 / 231 / 8367 / 22). `check-no-deprecated-translation` and `check-public-audience` cannot run locally without the `4.4.0-rc3` ref. `fetch-java-refs` uses plain git, which is not permitted here. Both trees fail those two rules the same way, so they give no PR #201 signal. CI runs `cargo xtask fetch-java-refs` before `lint` (`semaphore.yml:138`, `:222`). Master also adds `check-generated` to `verify` and a `doc-check` target plus CI block. | `CLAUDE.master.md`; `lint-master.log`, `lint-synth.log`. |

---

## 3. The PR #201 dependency, and sequencing (D1)

> **[Rev 2: D1 SATISFIED, so Option A is the path taken; B and C are RETIRED.]** PR #201 was
> rebased onto #209 (and #210, #217) as `c0220aab`. It is pushed but has not landed (D18).
> - A's "only hand-merges are the .NET harness bits" holds: 4 ours-files (R2.3), with the
>   `bindings/` → `dotnet/` move done beforehand as S1a (D16).
> - A's S1-gate path list is superseded by R2.9's, because the paths moved under `rust/`,
>   `python/` and `c/`.
> - "Take the source side verbatim" now means **whole-file** (R2.3 ⚠, R19).

**The fact that decides it.** Our Rust core is exactly master (as of `d6bf7c76`) plus PR #201
(`3b27d2c9`) plus one clippy fix (`2d9f5325`). PR #201 is not on master and does not have #209.
Every one of its 17 conflicted files needs #209's conventions applied to PR #201's code:

- renamed FFI types and functions;
- deprecated items removed;
- lint-custom compliance (no public fields, Java names, `#[doc(alias)]` markers, audience).

That is exactly the rebase PR #201's author has to do to land it.

### Option A — wait for PR #201's rebase, then one merge (**recommended**)

- When PR #201 is rebased onto #209 (pushed, or better, landed on master), merge master, plus the
  rebased PR #201 tip if it has not landed, into our branch.
- For every non-.NET file, resolution becomes "take the rebased-PR-#201 / master side verbatim".
  The only hand-merges are the .NET harness bits: `backend_factory.rs` (dotnet factories),
  `admin_backend.rs` (`ece8bb74`'s dotnet arm), `backend_pool.rs` / macros if touched, `Makefile`
  and `semaphore.yml`. All of those are small hunks.
- `2d9f5325` is dropped if the rebased code is clippy-clean, and CI makes it so.
- Rust authoring is zero, so the dotnet-actor can own the merge (Mode A, as in M17/P1), provided
  the S1 gate proves `src/`, `bindings/python`, `bindings/c`, `cbindgen.toml`, `build.rs`,
  `Cargo.*`, `generator/` and `xtask/` equal the source side exactly.
- **Cost:** a wait of unknown length (R1).

### Option B — merge now, re-applying #209 over PR #201 ourselves (not recommended)

- This is real Rust authoring across 17 files: FFI renames in `src/ffi/admin.rs` and
  `src/ffi/consumer.rs`, lint-custom compliance of PR #201's new Rust types, Python binding
  renames, and the header.
- Mode B, so actor-executor + kafka-critic, plus Python work.
- It duplicates the rebase PR #201's author must do anyway. When their rebase lands, our version
  and theirs conflict again, file by file. That is two divergent resolutions of the same
  conventions, with a real risk of shipping a C ABI that differs from what master finally has.
- The only gain is starting sooner.

### Option C — a .NET-only prep sub-stage now (optional, low value; not recommended)

- It could move sync `Dispose` to `Consumer_close` (which exists at HEAD) and remove
  `ConsumerGroupDescription.State` / `ConsumerGroupState` (pure .NET, no ABI).
- It cannot remove the ListConsumerGroups, ListClientMetricsResources or `Close(TimeSpan)`
  surface. The HEAD protos and arms still exercise them, so removing them now means interim
  `UNIMPLEMENTED` handlers plus skips that S3 then deletes: churn with no lasting value.
- Worth it only if the wait for A is long **and** the user wants the D2 behaviour change isolated
  in its own reviewed commit.

**Recommendation: A**, with S0 re-run on the day PR #201's rebase appears. If the wait passes a
horizon the user picks, re-open D1 (B, or C then A).

---

## 4. Stages (Option A)

> **[Rev 2: the stage *list* is superseded by R2.11]** (S0, S0u, S1a, S1, S1b, S2, S3, S4, S5).
> - The S2 and S3 *bodies* below stand, with R2.4's numbers and `dotnet/` paths.
> - S0 and S1 are superseded where marked.

### S0 — Pre-flight (Manager; no agent; read-only)

> **[Rev 2: steps 1–4 done for Rev 2]** against `c0220aab` (R2.3, R2.4). The step-2 prediction
> (`kafka_common_acl_…` / `kafka_common_quota_…`) was **confirmed**, and S0 also found the
> unpredicted `DeleteAclsFilterResults` → `FilterResults` family. Step 4 was re-derived by proto
> diff only, not by rebuilding (R2.13). On merge day, S0 repeats per R2.11.

1. Run `airlock git fetch origin`. Record the new `origin/master` and the PR #201 state: rebased
   tip SHA, whether it contains #209, whether it has landed.
2. Re-run the ABI analysis against the **actual** merge source. Every number in §2 is pinned to
   `383f3d30` + `3b27d2c9`. Redo the 527 / 22 / 67 / 15 / 37 split, especially for PR #201's 37
   EntryPoints. The rebase will likely rename `kafka_common_AclBinding_destroy`,
   `kafka_common_AclBindingFilter_destroy` and `kafka_common_ClientQuotaEntity_destroy` under
   `kafka_common_acl_…` and `kafka_common_quota_…`, and may change more. That is a prediction;
   verify it.
3. Run the trial merge (`git merge-tree`) and re-list the conflicts. Re-derive E1+, that is,
   branch-only deltas in conflicted files.
4. Build grpc-server in scratch against the trial-merge protos to re-derive the E4 compile-site
   list, and re-count arms from the trial-merge tree.
5. If anything differs materially from this plan, amend the plan and re-present it before S1.
   Material means: a new removed EntryPoint, a new Java-deprecated member, or a new conflict in
   a .NET-owned file.

### S1 — The merge (dotnet-actor 89; Mode A if the gate holds)

> **[Rev 2: superseded by R2.11 S1a + S1 + S1b.]**
> - The merge source is `c0220aab` (D18), preceded by the S1a pure move.
> - Resolution is whole-file for the 20 PR #201 files.
> - The gate's path list becomes R2.9's allowlist and Mode-B trigger set: `src/` →
>   `rust/src/`, `bindings/python` → `python/`, `bindings/c` → `c/`, `cbindgen.toml` / `build.rs`
>   / `Cargo.*` / `generator/` / `xtask/` → under `rust/`.
> - Cargo commands run from `rust/` (R2.7).
> - Root `cbindgen.toml` must be **absent** (an orphan), not "equal to the source side".

1. `git merge origin/master` (plus the rebased PR #201 tip if it has not landed). Take the
   source side for every file outside the .NET-owned set. Hand-merge the .NET harness hunks:
   keep the dotnet factories, arms, native launcher and Makefile/semaphore targets; take master's
   `doc-check` / `check-generated` / rustdoc CI additions.
2. The merge commit may leave `bindings/dotnet` not compiling. S2 and S3 follow immediately, and
   nothing is pushed until the phase closes (D11).
3. **Gate S1:**
   - `git diff <source> HEAD -- src/ bindings/python bindings/c cbindgen.toml build.rs Cargo.toml Cargo.lock generator/ xtask/`
     is **empty**, where `<source>` is `origin/master` if PR #201 has landed, else the merge of
     master and the rebased tip. If it is not empty, stop: that is Rust authoring, so switch to
     Mode B (§5) and re-present.
   - The regenerated header is byte-identical to the one the source side generates. Record its
     new SHA-1.
   - `cargo build`, `cargo test` (the unit and doc tests that ran before), `cargo xtask format-check`,
     `cargo xtask check-generated` and `make doc-check` pass. `cargo xtask lint` passes the 4
     locally runnable lint-custom rules and clippy; the other 2 rules are D8.
   - The shared-harness Rust (`tests/common/*`) compiles with `--features integration-tests,multilanguage-tests`.

### S2 — The .NET library (dotnet-actor 89)

Suggested commits, one concern each:

- **S2a Renames.** **[Rev 2: 74 strings in 14 families (R2.4). The 7 added are the 3
  `acl`/`quota` destroys and 4 × `DeleteAclsFilterResults_*` → `FilterResults_*`. Exact pairs:
  `abi-r2/r-head-vs-pr201new/func-renames.txt`.]** Rename the 67 EntryPoint strings across the
  13 families. Update the XML docs
  and comments that name old types or functions (25 files / 207 lines at HEAD, EntryPoints
  included). Update the 22 type-only signatures' docs where they name the type. There is no
  managed signature change, because opaque → `IntPtr` is unchanged.
- **S2b Deprecated admin surface.** Remove `IAdmin` / `KafkaAdminClient` / `MockAdminClient`
  `ListConsumerGroups` and `ListClientMetricsResources`. Remove `ConsumerGroupListing`,
  `ListConsumerGroupsOptions`, `ListConsumerGroupsResult`, `ClientMetricsResourceListing`,
  `ListClientMetricsResourcesOptions` and `ListClientMetricsResourcesResult`. Remove their
  `NativeMethods.Admin.cs` P/Invokes (the 14 admin EntryPoints among the 15),
  `AdminCallbacks` / `NativeAdminClient` / `KeyedResultMarshal` plumbing, and any
  `MockAdminClient` state only they use.
- **S2c Deprecated group state.** Remove `ConsumerGroupDescription.State`, `ConsumerGroupState`,
  the `GroupMarshal` consumer-state helpers, and doc references in `DescribeConsumerGroupsOptions`,
  `DescribeClassicGroupsOptions`, `GroupListing`, `MemberDescription` and similar (D5). Keep
  `GroupState` / `ClassicGroupState`.
- **S2d Consumer close.**
  - Remove `IConsumer.Close(TimeSpan)` and its `KafkaConsumer` / `MockConsumer` implementations,
    `NativeConsumer.CloseSyncWithTimeout`, and the `ConsumerCloseWithTimeout` P/Invoke (D3).
  - Sync `Dispose` calls `Consumer_close` (D2). Remove or rename `DefaultCloseTimeoutMilliseconds`.
  - Keep the teardown ordering exactly as ffi §B2 path 1 describes (close → destroy, caller's
    thread) apart from the entry point.
  - Leave the async `Close(CancellationToken)` and `DisposeAsync` unchanged.
- **S2e Tests.**
  - Delete the unit tests of the removed surface. Record the exact list of deleted `[Fact]`s and
    `[Theory]`s with the reason "Java-deprecated API, removed in #209 / root CLAUDE.md §3", as
    DoD #3 requires. At HEAD, 21 test and grpc files (316 lines) reference the surface.
  - Update the ones that only mention it.
  - **Add** a whole-surface resolution test: for every `DllImport` in the assembly,
    `Marshal.Prelink` succeeds against the loaded native library. Today only
    "EntryPoint is set" is asserted (`AdminNativeMethodsMarshallingTests.cs:186`). A missed rename
    still compiles and fails only at call time (R8), so this is the guard.
  - Add D7's pinning test if approved.
  - Adjust teardown tests that assumed a 5 s sync-Dispose bound.
- **S2f Docs.** STATUS.md, and the .NET README / API docs where they mention the removed
  members or old ABI names.

### S3 — gRPC server and harness (dotnet-actor 89)

- Fix the 5 sites:
  - `AdminServiceImpl.cs:572`, `:1284`: drop the two RPC implementations; the service base no
    longer has them.
  - `TranslateAdmin.cs:949`, `:1051`: drop the two translators.
  - `ConsumerServiceImpl.cs:602-604`: always call `Close()`.
- Both TFMs build with 0 warnings and 0 errors; `dotnet format --verify-no-changes` is clean.
- The arm set comes from the merged tree. `DOTNET_GRPC_SKIPS` is unchanged (verify the 3 names
  once more). **[Rev 2: re-verified in `c0220aab`. Under D15(a) the variable moves to
  `rust/Makefile` with the dotnet `cargo test` recipes.]**

### S4 — The one Critic pass (dotnet-critic 89, after S3)

Precedent: M15/P13.x and M17/P1 ran one Critic at the end. The dotnet-critic reviews S1's .NET
harness hunks and S2–S3 against:

- the **new** header, symbol by symbol for every renamed or removed EntryPoint;
- Java 4.3.1 deprecations (was anything removed that Java does *not* deprecate? was anything
  Java-deprecated left behind?);
- ffi §B2 teardown safety of the new `Dispose` path.

If S1 turned out to need authored Rust (Mode B), a **kafka-critic 89** reviews those commits as
well. Fix cycles: the Actor fixes `COMMENTS.89.md` with fixups, and the Critic re-checks only the
fixups.

**[Rev 2: scope extended (R2.11 S4).]** The review also covers:
- S1a purity;
- S1b plumbing: the in-image layout mirrors the repo, cargo runs from `rust/`, and no path reaches
  the stale root `target/`;
- the D14 outcome;
- the whole-file resolution of the 20.

### S5 — Close (Manager)

STATUS entry; archive `COMMENTS.DONE.89.md` here; reset the root `COMMENTS.89.md`; memory; N=90
next. `marked_classes.txt` does not apply. **[Rev 2: "here" and "the root" mean `dotnet/` after
S1a (binding `CLAUDE.md` §8.4).]**

---

## 5. Ownership

> **[Rev 2: amended.]**
> - S1a and S1b are dotnet-actor 89, Mode A.
> - The Mode-B trigger paths read `rust/src`, `python/`, `c/`, `rust/cbindgen.toml` (R2.9).
> - D14's `git mv` of `bindings/CLAUDE.md` is the **user's**, or the actor's only on explicit
>   authorization.
> - `rust/Makefile`'s dotnet targets (D15(a)) are dotnet-actor work, in the same class as the
>   M17/P1 `tests/common/*` harness edits.

| Work | Owner | Mode |
|---|---|---|
| S0 pre-flight | Manager | read-only |
| S1 merge, if the source-side gate holds | dotnet-actor 89 | A (take-theirs + .NET harness hunks; M17/P1 precedent for `tests/common/*` harness edits) |
| S1 merge, if **any** `src/`, `bindings/python`, `bindings/c` or `cbindgen.toml` resolution needs authored code | **actor-executor 89** for those commits, reviewed by **kafka-critic 89**; the dotnet-actor does the .NET hunks | B. Re-present to the user before starting. |
| S2, S3 | dotnet-actor 89 | A |
| S4 review | dotnet-critic 89 (plus kafka-critic 89 only in Mode B) | — |
| D4 `close(CloseOptions)` ABI | a future Mode-B phase (N ≥ 90), not P2 | B |
| Rule-file edits (§11) | the user | — |
| `fetch-java-refs` / full lint-custom | D8 | — |

---

## 6. Gates (the Actor runs them, the Manager re-verifies)

> **[Rev 2: superseded for execution by R2.7]**, which restates every gate with its working
> directory and Rev 2 numbers, and adds gates #9–#11 (orphans, paths, S1a purity).
> - Header `af0f1644…`.
> - 653 EntryPoint strings, counted as `EntryPoint = "…"`, not `DllImport` lines.
> - 14 old prefixes.
> - Mode-A proof via R2.9's allowlist.

Numbers are **as of** `383f3d30` + `3b27d2c9` and are re-derived in S0.

1. **Header.** Pre-merge SHA-1 `41f48ea8…`. Post-merge: record it, and it equals the source
   side's generated header.
2. **P/Invokes.**
   - `src/` `DllImport` count = 668 − 15 = **653**, all unique, **±** whatever S0 finds for PR
     #201's rebased 37. Plus 3 in the unit tests, unchanged.
   - A script checks that **every** EntryPoint string names a function in the new header: 0
     missing.
   - `git grep` finds 0 EntryPoints with any of the 13 old prefixes, and 0 with any of the 15
     removed names.
   - The new Prelink test (S2e) passes on both TFMs.
3. **Native library freshness.** Rebuild the `.dylib` (native) and the linux/amd64 `.so`
   (container). By `nm`: new names present, `Consumer_close_with_timeout` absent, all EntryPoints
   resolve (`LC_ALL=C` sorts). The in-image `.so` sha256 equals the staged one.
4. **Unit tests** on net8.0 and net10.0:
   - pass count = HEAD's count − the recorded deleted-test list + the added tests, reconciled
     exactly;
   - 0 failed, no "Test Run Aborted";
   - `make test-dotnet` format and lint are clean.
5. **grpc-server:** 0 warnings / 0 errors on both TFMs; `dotnet format --verify-no-changes` is clean.
6. **Arms.** Predict from the merged tree's stored list: **151** arms (115 sync + 36 async),
   **145 executed** per protocol.
   - Run PLAINTEXT, SSL and SASL_SSL, each **native** (`make test-integration-dotnet-native` + the
     protocol variable) and **container** (M8/P1 amd64 recipe, both images rebuilt).
   - All green. Reconcile surplus greens as well as reds.
   - M17/P1's oracle rule applies: if a .NET arm is red and Python is green, look at the harness
     or broker before touching C#.
7. **Rust:**
   - `cargo build`, `cargo test`, `cargo xtask format-check`, `cargo xtask check-generated` and
     `make doc-check` pass.
   - `cargo xtask lint`: clippy and the 4 runnable lint-custom rules pass locally. The other 2
     rules are D8 / CI.
   - `make test-integration-python-native` on PLAINTEXT as the harness oracle, since the shared
     harness changed.
8. **Mode A proof** (if Mode A): the S1 gate diff stays empty at the phase's HEAD. S2 and S3
   touch only `bindings/dotnet/**`, plus the .NET harness hunks.
9. **CI-only** (pending at close, M8/P1 practice): full lint-custom with the Java refs, the Linux
   dotnet jobs and the rustdoc job.

---

## 7. Decisions for the user

> **[Rev 2: per-decision statuses are in R2.10, and new decisions D13–D18 are there too.]** The
> rows below are Rev 1's text, annotated where they changed.

| # | Decision | Recommendation |
|---|---|---|
| **D1** | Sequencing (§3): A wait-then-merge / B merge-now with Mode-B Rust / C .NET-only prep first. | **A.** Re-run S0 when PR #201's rebase appears. Pick a wait horizon, after which D1 re-opens. **[Rev 2: SATISFIED. The rebase appeared (`c0220aab`); B and C are retired.]** |
| **D1b** | Under A: merge once PR #201's rebase is *pushed* (master + rebased tip), or wait until it has *landed*? | Merge on **landed** if it is within the horizon, because the S1 gate is then just `git diff origin/master`. Otherwise merge master + the rebased tip, as we did with `3b27d2c9`. **[Rev 2: RETIRED, replaced by D18 (merge `c0220aab` now). `c0220aab` already contains master `c7dd21bf`, so "master + rebased tip" reduces to one merge.]** |
| **D2** | Sync `Dispose` bound: 5 s (`close_with_timeout`, now removed) → `Consumer_close` = 30 s core default. | **Accept 30 s.** It equals Java `close()` (`ConsumerUtils.DEFAULT_CLOSE_TIMEOUT_MS`), our `DisposeAsync`, and Python's close. The 5 s was a .NET-only choice. It is a visible behaviour change: a sync Dispose can now block up to 30 s against an unreachable group coordinator. Record it in STATUS and the XML docs. |
| **D3** | `IConsumer.Close(TimeSpan)`: remove, or keep it and ignore the timeout (Python's shape)? | **Remove.** Java deprecates `close(Duration)`, the ABI is gone, and keeping a parameter that is silently ignored misrepresents the contract. Python's ignored `timeout` is a Python observation, not a precedent to copy. |
| **D4** | Java's replacement `close(CloseOptions)` has no C ABI on master. | **Out of P2.** Record it as a Mode-B follow-up (core `kafka_consumer_Consumer_close_with_options` + a CloseOptions builder, then Python and .NET), candidate N ≥ 90. The user decides whether to raise it with the core owners. |
| **D5** | `ConsumerGroupDescription.State` and `ConsumerGroupState`. | **Remove.** Java deprecates both since 4.0 for removal, master and Python removed them, and the value is derived purely in .NET, with no ABI. |
| **D6** | Removal style: delete outright, or leave `[Obsolete(error: true)]` shims? | **Delete outright.** This matches #209 and the root rule "deprecated API MUST NOT be translated", and the <1.0 API is not stable. |
| **D7** | #223: add .NET tests pinning the new default? | **Yes, cheap.** For both the sync and async real consumer built without `client.id` (no broker needed for construction): `ClientId()` starts with `consumer-<group.id>-`. Plus the new `group.instance.id` constructor validation, asserting its message, if it is reachable through the .NET config. The admin default is not exposed in .NET, so there is no admin test. |
| **D8** | Full lint-custom needs the `4.3.1` and `4.4.0-rc3` kafka refs; `fetch-java-refs` uses plain git, which agents may not run. | **The user runs `cargo xtask fetch-java-refs` once locally before S4.** The Manager then runs the full `cargo xtask lint`. The fallback is CI-only (§6.9). Agents must not replicate the fetch by hand. **[Rev 2: CHANGED, command only. It is now `(cd rust && cargo xtask fetch-java-refs)`; the `xtask` alias lives in `rust/.cargo/config.toml`.]** |
| **D9** | Cite-drift sweep scope. | Rule files: the user (§11). Tracked non-rule `bindings/dotnet` files: the Actor updates **only** cites that unambiguously mean *root* CLAUDE.md, and leaves `bindings/dotnet/CLAUDE.md §3` cites alone. `design/history/**` is frozen and not touched. **[Rev 2: CHANGED. The scope is widened to path drift: functional paths are mandatory (S1b), and doc path prefixes are rewritten mechanically. See R2.10 D9.]** |
| **D10** | The `GroupMetadata` RPC and the 3 transaction skips. | **Unchanged, out of scope.** |
| **D11** | The merge commit leaves `bindings/dotnet` non-compiling until S2 and S3. | **Accept.** Nothing is pushed mid-phase. An "evil merge" that folds S2 and S3 into the merge commit would hide the .NET review surface. **[Rev 2: CHANGED, extended. The non-compiling window is S1a → S3 (R2.10 D11).]** |
| **D12** | `2d9f5325` (our clippy fix on PR #201's code). | Drop it if the source side is clippy-clean. If it is not, stop and tell the user. Do not patch core on our branch, because that is master's or PR #201's problem. **[Rev 2: CONFIRMED DROP. `c0220aab` replaced the patched alias with `seed_scram_result(rows: Vec<SeedRow<'_>>)`; gate #7 decides clippy (R2.9).]** |

---

## 8. Risks

| # | Risk | Mitigation |
|---|---|---|
| R1 | PR #201's rebase timing is unknown, which blocks A. | The D1 horizon; C or B as fallbacks. **[Rev 2: RESOLVED. The rebase exists.]** |
| R2 | PR #201's rebase reshapes its 37 EntryPoints (renames, removals, ABI shape) beyond what is predicted. | S0 recomputes; any material delta goes back to the user. **[Rev 2: RESOLVED: 30 identical + 7 renamed, 0 shape changes (R2.4). 4 of the 7 were not predicted, but they are pure renames, not material.]** |
| R3 | Master moves again before S1. | S0 is run on the merge day, not at planning time. |
| R4 | A take-theirs resolution silently drops a branch-only change. | The only branch-only core change is `2d9f5325` (D12). The .NET harness hunks are listed in §3/A. The S1 gate diff is exact. **[Rev 2: STANDS, with its dual R19 (take-theirs keeping a stale PR #201-old change). Branch-only .NET deltas outside the hunks are now enumerated: the admin_backend "five backends" lines (R2.3).]** |
| R5 | lint-custom's 2 ref-dependent rules can't run locally, so CI can be red after push. | D8. |
| R6 | 30 s sync Dispose (D2) makes teardown tests slow, or pushes CI jobs past the time limits M17/P1 set. | S2e audits tests that sync-Dispose against unreachable brokers; measure the job durations. |
| R7 | Stale native library after the merge (recorded twice in M15/P13). | §1 and §6.3 freshness-by-symbol proof. |
| R8 | A missed EntryPoint rename compiles and fails only at call time (`EntryPointNotFoundException`). | The header-resolution script, the `nm` resolution, and the new Prelink test (§6.2–6.3). |
| R9 | Over-deletion: removing a member Java does *not* deprecate. | The Critic checks each removal against 4.3.1 `@Deprecated` and master's `java-deprecated.txt`. |
| R10 | Doc drift: XML docs and ffi rules keep describing `close_with_timeout` and the old type names. | S2a/S2f for tracked non-rule files; §11 for rule files. |

---

## 9. Rule suggestions (for the user; agents do not edit rule files)

1. **Renumbered root cites.** `bindings/dotnet/CLAUDE.md`: §11 → §13 (`:226`, `:528`), §9.5 →
   §11.5 (`:718`). `ffi-marshalling.md`: root §3 → §4 at `:781` and `:1825` (check `:1833` and
   `:1865`, which look like `bindings/dotnet/CLAUDE.md §3`), §12 → §14 (`:651`, `:719`), §11 →
   §13 (`:1144`), root §9.5 → §11.5 (`:995`). Persona: `dotnet-actor.md:33` "(CLAUDE.md §3)".
   It probably means `bindings/dotnet/CLAUDE.md`; check before changing.
2. **Deprecated API in bindings.** Add the root §3 bullet ("deprecated Java API is not
   translated") to `bindings/CLAUDE.md`, so bindings don't re-expose what the core dropped. Say
   explicitly that a binding removes, and does not `[Obsolete]`, such members while <1.0.
3. **`bindings/dotnet/CLAUDE.md` §1 (`:53`) and §4 (`:596`)** say "only `close_with_timeout`
   remains sync-only…". The function is gone; rewrite. `:348` and `:374` show `Close(TimeSpan)`
   in the sync consumer surface, and `:321` shows the admin one, which stays.
4. **`ffi-marshalling.md` §B2.** The teardown table (`:1386`, `:1452`, `:1477`) must say path 1 is
   `Consumer_close` → `Consumer_destroy`. The ownership table (`:470`, `:474`, `:486`, `:836-837`)
   uses `FutureRecordMetadata_*` and `kafka_consumer_PartitionInfoList_t`, which are now
   `kafka_common_KafkaFuture_RecordMetadata_*` and `kafka_common_PartitionInfoList_t`.
5. **New FFI naming in the persona and Critic checklists.** Types are prefixed by the Java
   *package* (`kafka_common_acl_…`, `kafka_common_security_auth_…`, `kafka_common_quota_…`), and
   Java generics are spelled `KafkaFuture_RecordMetadata`. The dotnet-critic should verify
   EntryPoints against the header, not against memory of the old names.
6. **Oracle rule for proto-driven compile checks** (a process lesson from E4). Compile the
   grpc-server against the **trial-merge** protos, never against pure master protos, when the
   branch carries unmerged PR content.

**[Rev 2: items 7–14 added. Paths are as they will be under D13(a). Items 1–6 still apply; read
`bindings/dotnet/` as `dotnet/`.]**

7. **Root `CLAUDE.md`'s new #210 sentence** names only `rust/`, `python/` and `c/` (*"The Rust code
   lives in `rust/`, the bindings in `python/` and `c/`…"*). If D13(a) is taken, it should name
   `dotnet/` too. This is master's file, so raise it upstream or edit locally; that is the user's
   call.
8. **`dotnet/CLAUDE.md` "How it loads"** says working under the binding "stacks three rulebooks:
   root → `bindings/CLAUDE.md` → this file", and "never rely on nested auto-loading". After the
   move, **both halves are wrong** (R2.5, observed):
   - `bindings/CLAUDE.md` no longer loads (two rulebooks, not three, unless D14 restores it);
   - nested `dotnet/.claude/rules/*.md` *do* auto-load.

   Rewrite it to match D14's outcome, and keep the explicit links as the belt-and-braces.
9. **Path drift in the binding rulebooks.**
   - `dotnet/CLAUDE.md`:
     - §1 (`src/ffi/*.rs` → `target/include/confluent_kafka.h`);
     - §2 (the `bindings/dotnet/` layout tree);
     - §6.3 steps 2–4 (`src/ffi/<area>.rs`, `cbindgen.toml`, `cargo build --features ffi`, and
       `target/include/…`, now under `rust/`, with cargo run from `rust/`);
     - §7.1/§7.2 (the build pipeline and the commands table);
     - §8.4 (`bindings/dotnet/COMMENTS.<N>.md`, agent-memory and `design/` paths).
   - `ffi-marshalling.md`: `src/ffi/…` cites → `rust/src/ffi/…`, and `bindings/python/…` cites →
     `python/…`.
10. **`bindings/CLAUDE.md` cites.** Repoint them to D14's destination:
    - 10 rule-file lines (8 in `dotnet/CLAUDE.md`, 1 in `ffi-marshalling.md`, 1 in
      `dotnet-critic.md`), which are the user's;
    - 9 C# lines and 5 `design/current` lines, under D9.
11. **Agent memory has two homes, and neither is tracked** (R2.5, D17.4):
    - binding-local `dotnet/.claude/agent-memory/` (137 + 51 files);
    - repo-root `.claude/agent-memory/dotnet-*` (47 + 39 files).

    Personas with `memory: project` spawned from the repo root use the latter. Decide which home
    is canonical and whether either should be tracked. Today neither survives a fresh clone.
12. **Persona paths.** The tracked personas (`dotnet/.claude/agents/dotnet-{actor,critic}.md`)
    cite `bindings/dotnet` (5 and 4 lines). Update them, then **re-copy** them to the repo-root
    discovery copies (binding `CLAUDE.md` §8.4). The copies are snapshots and keep the old paths
    until then.
13. **Optional `.dockerignore`.** Add `dotnet/**/bin` and `dotnet/**/obj` so the gRPC image build
    context does not upload .NET build output. Master's file lists only `rust/target` and
    `kafka/`. This is not a correctness issue.
14. **Process rule from R2.0** (for `agent-roles.md` or the Manager persona): agents never run git
    against the main working tree except `git -C <repo>` with explicit SHAs, chained with `&&`.
    A scratch copy that cannot be made (a clone transport refused) is a **stop**, not a fallback
    to the main tree. §1's new bullet carries it for P2.

**[S4 addendum, 2026-10-01: items 15–21 come from the dotnet-critic 89 pass. These are
suggestions only. Per the coordinator's instruction, no rule file was edited for them.]**

15. **`.claude/rules/consumer-threading.md:85`** still cites `bindings/CLAUDE.md §2`. Under D14 that
    file is now `dotnet/.claude/rules/bindings.md`. This is a repo-root rule file in the Mode-A
    allowlist, but D14's path-only authorization did not cover it, so the user decides.
16. **`dotnet/CLAUDE.md:523`**: the §4 Disposal row still names `Consumer_close_with_timeout`,
    which #209 removed. The live sync consumer close is `Consumer_close` (30 s core default). This
    is the same family as item 3, but it is a different line.
17. **Item 3's line numbers are off by one** after bf249e8f added the link line. The corrected
    cites are `:54`, `:597`, `:349`, `:375` and `:322` (previously `:53`, `:596`, `:348`, `:374`
    and `:321`).
18. **Item 4, more specifically.** In `ffi-marshalling.md:1386,1452,1477`, path 1 of §B2's
    teardown table should read `Consumer_close` → `Consumer_destroy` (core default 30 s), not
    `Consumer_close_with_timeout`.
19. **Add an ABI signature check against the header to the DoD / dotnet-critic checklist.**
    Prelink proves only that every `EntryPoint` *resolves*; it cannot catch a parameter-list drift.
    S4 had to verify all 653 + 3 declarations and 59 callback typedefs by script
    (`sig_check.py`). That check should be a named gate, not an ad-hoc one.
20. **Scope path-drift gates to the whole Mode-A allowlist, not just `dotnet/`.** C89-2 found
    `rust/tests/common/backend_pool.rs` still naming `bindings/dotnet/Dockerfile.grpc{,.async}`.
    Nothing broke only because both callers happen to be cfg/container-gated. The D9 sweep's
    `bindings/dotnet` grep stopped at `dotnet/`.
21. **Not a rule file, but recorded so it is not lost.** `design/current/python-binding-send-batching.md`
    has 7 stale `bindings/dotnet` cites. It is a repo-root Python design doc that P2 merged
    whole-file. The S4 triage rejected editing it as out of P2's scope, so the user decides.

---

## 10. Out of scope (deliberately not added)

- A `close(CloseOptions)` ABI or managed surface (D4).
- Python binding changes. `bindings/python`'s ignored `close(timeout)` is only an observation.
- Any change to PR #201's content beyond take-theirs. **[Rev 2: "take-theirs" means
  whole-file `c0220aab` (R2.3 ⚠).]**
- **[Rev 2]** Consolidating or tracking agent memory (D17.4, §9 item 11), editing personas (§9
  item 12), and any `python/` or `c/` relocation follow-ups. Master owns those.
- `GroupMetadata`, and the transaction skips (D10).
- Rule-file edits (§9).
