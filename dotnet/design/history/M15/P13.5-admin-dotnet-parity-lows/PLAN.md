# M15/P13.5: .NET Admin parity lows (no core or ABI change)

> **APPROVED by the user on 2026-10-02.** Written by the Manager the same day. All of §8 is
> approved as recommended: D1 (a), D3 (a), D4 (a), D5 (a), D6 as §4.1. **D2 is approved**, so
> X8 is in scope for S2b with the exact §8 text. The Actor starts S1. The Critic runs once,
> after S2 (§5), as in P13.4.

**N = 90.** The highest used binding N is 89 (M17/P2, closed at `10812926`). There is no
`COMMENTS.9x.md` anywhere in the repo, and `STATUS.md:40` says "Next unused dotnet N = 90".
Branch `prashah_dev_dotnet_binding`. **Base = HEAD `21458241`** at drafting. **Mode A**: C#
only, under `dotnet/`. No change to Rust, the C ABI, the header, Python or C. Every item was
checked against that rule, and none needed dropping (§2).

**Source.** The clean-slate .NET AdminClient parity audit at `de51d14b` (2026-09-30), plus the
re-check at HEAD (scratchpad `recheck/result-{A..E}.tsv`). P13.4 (N=87) fixed 14 of the audit's
findings. This phase takes the 21 IDs below and edits neither the audit files, the
`Dotnet-AdminClient-Findings-Workflow/` folder, nor `PendingAdminClientFindingsForDotnet.md`.

---

## 0. Scope

| ID | Finding (short) | Slice |
|---|---|---|
| G3-6 | `NewPartitionReassignment.TargetReplicas` hands out its mutable `List<int>` | S1a |
| G5-2 | `OffsetAndMetadata` has no value equality | S1a |
| G5-3 | `OffsetAndMetadata.LeaderEpoch` keeps a negative epoch; Java reports empty | S1a |
| G1-13 | `ListTopicsOptions` has no `Equals`/`GetHashCode` | S1a |
| G6-4 | `FeatureUpdate.ToString` prints the C# enum name | S1a |
| G3-7 | Java's zero-arg and options-only `listPartitionReassignments` are not writable | S1b |
| G1-10 | No name-collection overloads of `DeleteTopics` / `DescribeTopics` | S1b |
| G3-8 | `LogDirDescription` ctor is internal; Java's three are public | S1b |
| G5-6 | `RemoveMembersFromConsumerGroupResult.RemoveAll` is public; Java's is private | S1b |
| G4-5 | `DeleteAclsResult.FilterResult.Error` should be `Exception` | S1b |
| G4-3 | `DescribeUserScramCredentials` throws on a null user; Java skips it | S1c |
| G6-9 | Null feature name gets its own message; Java's is the blank-name one | S1c |
| G6-1 | Mock `UpdateFeatures` rejects an empty map; Java's mock accepts it | S1c |
| X9 | The closed-client `ObjectDisposedException` names the internal `NativeAdminClient` | S1c |
| X2 | A negative `TimeoutMs` throws; Java clamps it to 0 | S1d |
| X6 | Unreferenced `kafka_admin_*` P/Invoke declarations | S2a |
| G4-1, G5-8, G7-7, G3-10 | Stale or wrong doc sentences and cites | S2b (docs) |
| X8 | `dotnet/CLAUDE.md` still says Admin is not exposed / Mode B | S2b (D2 approved) |

**Explicitly out:** G1-2, G1-4, G2-2, G2-3, G3-9, G5-5, G7-6, G4-7, G6-5 (each needs core or
ABI work); PR #219; the X10 null-input per-key family; X1, X4, X7, G5-4, G7-5, G6-8, G4-8; every
Python half.

---

## 1. Standing constraints (relay verbatim to the Actor and the Critic)

1. **Cadence (D1).** Two sub-stages, S1 (code) then S2 (cleanup and docs). The Actor finishes
   each one (build, tests, self-review, commits), and the PM checks its gates (§6.1). **No
   Critic runs between sub-stages, and there is no user gate between them.** `dotnet-critic` 90
   reviews `<base>..HEAD` **once, after S2** (§5). If it files findings, the Actor fixes them
   with `fixup!` commits and the Critic re-checks **only those fixups**.
2. Stage files by explicit path only. Never `git add -A`, `.` or `commit -a`. Never stage
   `dotnet/COMMENTS.90.md` / `COMMENTS.DONE.90.md`, this PLAN, or unrelated untracked files. Do
   not push or squash.
3. **Ground truth** is the C ABI header plus the Kafka 4.3.1 Java public API under `kafka/`
   (`dotnet/CLAUDE.md` §8.2). Each item's Java anchor is in §3–§4.
4. **Environment traps** (each has faked a PASS before):
   - Prefix every shell command with
     `export PATH="/usr/bin:/bin:/usr/local/bin:/opt/homebrew/bin:$HOME/.cargo/bin:$HOME/.dotnet:$PATH"`.
   - Use `~/.dotnet/dotnet`. Never `cd`; use `git -C` and absolute paths.
   - `grep` is ugrep: use `/usr/bin/grep` or `git grep`. There is no `sed`: use `awk` or python.
     `cat` is bat: use `/bin/cat`. zsh does not word-split an unquoted `$var`. Set
     `PYTHONDONTWRITEBYTECODE=1`.
   - A filter that matches zero tests still passes: always report `Passed:` / `Total:`.
   - `Test Run Aborted` means a crash, whatever the exit code.
   - A failed build followed by `--no-build` prints a stale `Passed!`: assert `0 Error(s)`.
   - Cargo runs from `rust/` only: `cargo build --features ffi`. The header is under
     `rust/target/include`.
   - Bound every tool's output (`| head`). Grep `rust/src/ffi/admin.rs` and the header; never
     read them whole.
5. **Exact messages** (DoD §3). Every new or rewritten exception assertion checks the message,
   plus `ParamName` for `Argument*Exception`. Use `StartsWith(message)` for `ArgumentException`,
   because .NET appends ` (Parameter '…')`.
6. **Reflection pins every public-surface change** (§4). A wrong C# signature compiles and passes
   every behavioural test (the M15/P1 lesson).
7. **One regression test per behaviour change**, named after the finding ID.
8. Cite code by **symbol name**, not line number (P13.3 D17). Java cites keep `File.java:line`,
   checked against `kafka/`.
9. **No new type** (DoD §7). The only new non-public member planned is one private field on
   `NativeAdminClient` (§3.3).

---

## 2. Re-verification and flags

All 21 IDs reproduce at HEAD, per the re-check TSVs. None needs a core or ABI change, so
**nothing is dropped**. Four premises in the brief need correcting or extending:

- **F1, G5-6: the brief's "nothing in `dotnet/` uses it" is false.**
  `grpc-server/AdminServiceImpl.cs` (`RemoveMembersFromConsumerGroup`) branches on
  `result.RemoveAll`, and `grpc-server` has no `InternalsVisibleTo`. The fix still stays
  .NET-only: the servicer reads `options.RemoveAll` instead. That member is public, like Java's
  `options.removeAll()`, and has the same value, because the result is built from the same member
  set. So `grpc-server` is touched, and the Docker regression (§6.2) covers it.
- **F2, G4-5.** `grpc-server/AdminServiceImpl.cs` (`FilterResultsToProto`) reads the renamed
  property too. It gets a one-word edit in the same commit.
- **F3, G4-3 and G6-9 versus the recorded X10 carry-over.** P13.4 §7 and `STATUS.md` ("X10's
  other members, which throw synchronously for a null where Java fails per key (D1)") list both
  of these as X10 members. Neither one actually is:
  - in Java, G4-3 **skips** a null user (`KafkaAdminClient.java:4357-4360`), with no per-key
    failure;
  - G6-9 is Java's own **synchronous** `IllegalArgumentException` (`:4597-4599`).

  So fixing them does **not** reverse P13.4 D1, and the per-key pattern stays out. But the close-out must
  **explicitly supersede** that STATUS line, removing G4-3 and G6-9 from X10's member list, so
  the Critic does not read them as a D1 reversal.
- **F4, G6-1 supersedes an in-code "intentional" note.** The `UpdateFeatures` remarks say the
  guards "also hold on a mock … as they did before". P13.3 §2 #19 recorded this as "noted only",
  not as a decision. The user has approved the change, and the remark is rewritten in the same
  commit. **G6-1's blank-name half is not in the brief**, because Java's mock never checks names.
  The null and blank guards therefore stay on both paths. The null guard is needed in any case,
  since a null name cannot be pinned.

Other couplings the Actor must expect:
- **X6** has to lower `NativeMethodsPrelinkTests.ExpectedImportCount` (653 today) by exactly the
  number of declarations deleted.
- **G3-7 and G1-10** add overloads, so a single literal `null` becomes ambiguous (CS0121). At
  HEAD this affects 4 `ListPartitionReassignments(null)` test sites (`PublicAdminReassignmentsOffsetsTests`)
  and 2 `DeleteTopics`/`DescribeTopics(null!)` sites. See D3.
- **X2** leaves `Close(TimeSpan)` alone. Its negative throw stays, because Java's
  `close(Duration)` also throws. Some IAdmin `<exception>` blocks mix `TimeoutMs` with other
  conditions (for example `IsolationLevel`, or `PartitionSizeLimitPerResponse` at IAdmin
  `DescribeTopics`). In those blocks, delete only the timeout clause and keep the block.

---

## 3. S1: code (four commits, in this order)

### 3.1 S1a: value types: G3-6, G5-2/G5-3, G1-13, G6-4
- **G3-6.** Store the copy as a `ReadOnlyCollection<int>` (`new List<int>(src).AsReadOnly()`),
  which is Java's `List.copyOf` (`NewPartitionReassignment.java:32-40`). The declared type stays
  `IReadOnlyList<int>`. This is the G1-9 `TopicCollection` pattern from `8d3cceda`.
  *Test:* the returned object is not a `List<int>`; `((ICollection<int>)x).IsReadOnly` is true;
  mutating the caller's source list after construction changes nothing.
- **G5-3.** Normalise a negative epoch to `null` in the ctor. Java's getter does the same
  (`OffsetAndMetadata.java:98-101`), and .NET has no other accessor of the raw value. `ToString`
  then prints `leaderEpoch=null` for it.
  *Tests:* `-1` and `int.MinValue` give `null`; `0` stays `0`; the `ToString` row. Consumer
  check: `NativeConsumer` / `NativeConsumerHandle` / the `AlterConsumerGroupOffsets` path send
  `LeaderEpoch ?? -1`, so a negative epoch still reaches native as `-1`, which is what Java sends.
  The full consumer suite must stay green. Do **not** touch `ListOffsetsResultInfo.LeaderEpoch`:
  it is a different type, and its `-1` test pin stays.
- **G5-2.** Make the class `IEquatable<OffsetAndMetadata>`, overriding `Equals(object)` and
  `GetHashCode` over `Offset`, `Metadata` and the normalised `LeaderEpoch`
  (`OffsetAndMetadata.java:105-116`). Add no operators; this follows the `MemberToRemove` /
  `RecordsToDelete` precedent.
  *Tests:* `(5,"m",null)` equals `(5,"m",-1)`; each field difference makes the two unequal; the
  hash is consistent; reflection pins the interface.
- **G1-13.** Override `Equals` / `GetHashCode` over **`ListInternal` only**. Java's `equals`
  ignores `timeoutMs` (`ListTopicsOptions.java:67-77`). This is the `DescribeProducersOptions` /
  `ListTransactionsOptions` precedent, with the inclusion or exclusion stated in the xmldoc.
  *Test:* two options that differ only in `TimeoutMs` are equal.
- **G6-4.** Add a private `JavaName(UpgradeType)` switch, giving `UNKNOWN`, `UPGRADE`,
  `SAFE_DOWNGRADE` and `UNSAFE_DOWNGRADE`, with the numeric fallback the G2-8 pattern from
  `d4902bc7` uses (`FeatureUpdate.java:28-32`, `:108-110`).
  *Test:* all four exact renderings.

### 3.2 S1b: admin surface: G3-7, G1-10, G3-8, G5-6, G4-5
- **G3-7.** On `IAdmin`, `KafkaAdminClient` and `MockAdminClient`, give `partitions` a `= null`
  default **and** add `ListPartitionReassignments(ListPartitionReassignmentsOptions options)`,
  which forwards `(null, options)` (`Admin.java:1193`, `:1248`). Then `()` and positional `(o)`
  both compile. A runtime-null `options` forwards unchanged, meaning defaults, like every other
  options parameter. Handle the literal-`null` ambiguity per D3.
  *Tests:* reflection pins the overload and `HasDefaultValue`/`DefaultValue == null` on all
  three types; `()` and `(o)` behave as `(null, o)` on the mock.
- **G1-10.** On `IAdmin` and both clients, add
  `DeleteTopics(IReadOnlyCollection<string> topicNames, DeleteTopicsOptions? options = null)` and
  the matching `DescribeTopics`. This is Java's `Admin.java:212,226,295,306`, and
  `IReadOnlyCollection<string>` is the existing IAdmin precedent for name collections. Each
  forwards `TopicCollection.OfTopicNames(topicNames)`. A null `topicNames` throws
  `ArgumentNullException` with `ParamName "topicNames"` before it forwards. The IAdmin remark
  that says these overloads do not exist is rewritten.
  *Tests:* the mock deletes or describes by name exactly as the `TopicCollection` form does;
  null → `ParamName`; reflection pins. The 2 `(null!)` test sites get a `(TopicCollection)` cast.
- **G3-8.** Make the ctors public with Java's three arities (`LogDirDescription.java:38,42,46`),
  using the D4 shape, and replace the contradicted "consistent with the sibling admin result
  types" rationale. `ReplicaInfo` already has a public ctor.
  *Tests:* reflection pins that public ctors exist with Java's arities; values round-trip;
  `-1` maps to `null` (D4).
- **G5-6.** Make `RemoveAll` `internal` (`RemoveMembersFromConsumerGroupResult.java:113` is
  `private`). The servicer reads `options.RemoveAll` instead (F1).
  *Test:* reflection shows no public `RemoveAll` on the result type. The options type's public
  `RemoveAll` is unchanged.
- **G4-5.** Rename `FilterResult.Error` to `Exception` (`DeleteAclsResult.java:58`). Update the
  servicer (F2) and every test and cref. This is a pre-1.0 rename.
  *Test:* reflection finds `Exception` and no `Error`.

### 3.3 S1c: preconditions: G4-3, G6-9, G6-1, X9
Add **one** `private readonly bool _isMock` to `NativeAdminClient`. `Create` sets it false and
`CreateMock` sets it true. G6-1 and X9 both read it, and nothing else does.
- **G4-3.** Skip a null user in the managed request loop instead of throwing. Cite
  `KafkaAdminClient.java:4357-4360`. `[null]` then becomes an empty request, which describes
  every user. Java gets the same result: it sends an empty non-null list, and the broker treats
  empty as all (`ScramImage.java:86-87`). The FFI's `read_strings` also skips NULL entries.
  Rewrite the stale "stricter than Java" remark.
  *Tests (submit seam):* `["alice", null]` submits count 1 with `"alice"`; `[null]` submits
  count 0; there is no throw. The `"The users must not contain a null element."` assertion is
  removed.
- **G6-9.** Delete the separate null-name check. `IsBlank(null)` already returns true, so a null
  key now gets Java's `"Provided feature can not be empty."`
  (`KafkaAdminClient.java:4597-4599`, `Utils.isBlank(null)`).
  *Test:* a custom `IReadOnlyDictionary` that yields a null key gives that exact message and
  `ParamName "featureUpdates"`. No test pins the old message today.
- **G6-1.** Skip only the empty-map guard, and only when `_isMock` is set
  (`MockAdminClient.java:1286-1300`). The real client keeps Java's rejection, `"Feature updates
  can not be null or empty."` (`:4590-4592`). Rewrite the remark (F4).
  *Tests:* the mock with an empty map does not throw, `Values` is empty and `All()` succeeds; the
  real client (bad bootstrap) still throws the exact message. Update `PublicAdminP7Tests` (the
  mock rejection pin). If a seam test drove the real-path guard through `CreateMock`, re-point it
  at `Create`.
- **X9.** `ThrowIfClosed` throws `ObjectDisposedException(_isMock ? nameof(MockAdminClient) :
  nameof(KafkaAdminClient))`. The channel stays `ObjectDisposedException`, by design.
  *Test:* `ObjectName` on both flavors after `Dispose`.

### 3.4 S1d: X2, a negative `TimeoutMs` sends 0
Replace `ValidateTimeoutMs(int?, string)` with
`private static int ToNativeTimeoutMs(int? timeoutMs) => timeoutMs is null ? UnsetTimeoutMs : Math.Max(0, timeoutMs.Value)`,
which is Java's `calcDeadlineMs` clamp (`KafkaAdminClient.java:496-499`). The FFI passes 0
through, and the core computes `now + 0`. Do this as **one mechanical sweep** in this commit,
inventoried by **phrase**, not by cross-reference (the P13.4 D8 lesson). Counts at HEAD:
- **45** `ValidateTimeoutMs(` call sites in `NativeAdminClient.cs`;
- **45** source doc lines matching `TimeoutMs` together with `negative` (43 in `IAdmin.cs`,
  2 in `NativeAdminClient.cs`);
- **18** `Admin/*Options.cs` files with "Must not be negative" on `TimeoutMs`;
- **46** test lines in **20** files, found with
  `git grep -nE 'must not be negative; leave it null|TimeoutMs = -' -- tests grpc-server`.

The new wording, stated once and pointed at from everywhere else: *"A negative value is sent as
0, Java's `Math.max(0, timeoutMs)`: the call is already expired and fails through its result
with the core's timeout error."*

Tests:
- a table test of the helper: `null` → -1, `0` → 0, `-1` → 0, `int.MinValue` → 0, and `5` → 5;
- each converted test asserts that the captured timeout is 0 (submit-seam tests), or that no
  synchronous throw happens (public mock tests);
- **one real-client differential test:** on a `KafkaAdminClient` with a bad bootstrap, a
  negative `TimeoutMs` faults with the **same `Code` and `Message`** as `TimeoutMs = 0`, and that
  `Code` is the timeout code. This asserts the message without hard-coding a core string.

If the zero-timeout call does not fault deterministically within the test bound, fall back to
the seam assertion and record it as R2.

**Exclusions.** Do not touch `Close(TimeSpan)`, or any other negative-value rule such as
`PartitionSizeLimitPerResponse` or `IsolationLevel`.

---

## 4. S2: cleanup and docs (cheap, no deep reading)

### 4.1 S2a: X6, delete unreferenced `kafka_admin_*` declarations
Inventory: copy `Dotnet-AdminClient-Findings-Workflow/scripts/dead_pinvokes.py` to your
scratchpad, remap its two `bindings/dotnet` constants to `dotnet`, and run it from the repo
root. Do **not** edit the original. At HEAD it reports 389 declared, **75 unreferenced** and
**8 tests-only**.

Delete the 75 per D6, and lower `ExpectedImportCount` by exactly the number deleted. The Prelink
test (R8 guard, `bbba6ed7`) must stay green. Paste the script's before and after output into
the commit message.

### 4.2 S2b: docs, minimal edits, last, doc-only commit
- **G4-1** (IAdmin `CreateAcls` remark): ANY/MATCH are rejected at construction; a binding with
  an Unknown-coded field constructs, and fails its own Task, as in Java
  (`KafkaAdminClient.java:2615-2621`).
- **G5-8** (IAdmin `ListConsumerGroupOffsets(string)`): drop the "topic partitions set on
  options are ignored" clause.
- **G7-7** (`DescribeProducers` remark): cite `DescribeProducersOptions.java:29`.
- **G3-10:** replace the stale cites with symbol names, in .NET files only. The sites are the
  `DescribeReplicaLogDirs` remarks in `NativeAdminClient`, the IAdmin
  `ListPartitionReassignments` doc, and the `NativeMethods.Admin.cs` cite. If X6 deleted that
  last declaration, its cite went with it.
- **X8 (D2 approved):** a separate one-commit edit of exactly the two passages in §8 D2,
  word for word. This is the only rule-file edit the user has authorized in this phase.

---

## 5. The one Critic pass (after S2)

`dotnet-critic` 90 reviews `<base>..HEAD` once and writes to `dotnet/COMMENTS.90.md`, taking
the exclusive lock. It checks:
1. Each item against its Java anchor (§3–§4), including the D-shapes as ruled.
2. The **X2 sweep**: re-run the phrase inventory. No stale "must not be negative" text may
   remain on `TimeoutMs`; the other negative rules and `Close(TimeSpan)` must be intact; every
   converted test must assert something.
3. The **surface**: the reflection tests match §3 exactly; there is no new type; the declared
   return types of G3-6 and G1-10 are as specified.
4. **X6**: the script reports 0 unreferenced `kafka_admin_*`; the Prelink count is consistent;
   no live declaration was removed.
5. **Mode A**: §6.1 items 1–3.

**The Critic must NOT flag:** G6-1's blank-name half (F4); the out-of-scope list (§7); the
X10 family; the Python halves; `ListOffsetsResultInfo`'s `-1` epoch.

---

## 6. Gates

### 6.1 Per sub-stage (the Actor runs them; the PM verifies them before S2)
1. `cargo build --features ffi` from `rust/`. The header SHA-1 must match base.
2. Mode A: `git diff --stat <base>..HEAD -- . ':!dotnet'` is **empty**. `<base>` is the
   **actual parent of S1's first commit**, and the Actor states the SHA it used.
3. `internal static extern` count: **653** through S1, then **653 − k** after S2a, matching
   `ExpectedImportCount`.
4. `~/.dotnet/dotnet build dotnet/Confluent.Kafka.sln` (Debug): `0 Warning(s)`, `0 Error(s)`.
   `grpc-server` builds.
5. Filtered `dotnet test -f net10.0 --no-build` on the touched classes, with explicit
   `Passed:` counts.
6. **Full suite on net10.0 and net8.0**: `Failed: 0` and no abort. Totals are compared against
   a baseline the Actor runs on base before any edit. Do not take it from memory.
7. `dotnet format --verify-no-changes` on the solution and on `grpc-server`.

### 6.2 Final
8. Critic 90 is clean (§5).
9. **Docker regression** (PM, at close, if `docker info` succeeds). `grpc-server` changed (F1,
   F2) and every RPC's timeout path changed, so run **all admin `__grpc_dotnet` arms**, in
   plaintext container mode. Count them with `-- --list`; P13.4 had 80. Use the M17/P2 macOS
   recipe:
   - cross-build the amd64 `.so`;
   - stage it;
   - build the images with `DOCKER_DEFAULT_PLATFORM=linux/amd64`;
   - run with `MULTILANG_BACKEND_MODE=container`.

   First check `git diff <last-gate>..HEAD -- rust/src rust/Cargo.lock` for a stale `.so`, and
   prove the image carries the fresh one. If Docker is down, mark the gate **CI-pending**.
10. Close-out (PM):
    - update `STATUS.md`, including the F3 supersession and "next N = 91";
    - archive `COMMENTS.DONE.90.md` (or `COMMENTS.90.md` as a clean-pass record) and this plan;
    - reset `dotnet/COMMENTS.90.md`;
    - update memory.

---

## 7. Out of scope (observed, not added)

- **G3-6's siblings** (D5). Thirteen other public getters hand out a mutable backing `List`,
  `HashSet` or array:
  - `PartitionReassignment` ×3;
  - `TopicDescription`;
  - `TopicPartitionInfo` ×2;
  - `ConsumerGroupDescription`, `ClassicGroupDescription` and `MemberAssignment`;
  - `TransactionDescription`;
  - `DescribeProducersResult`;
  - `RemoveMembersFromConsumerGroupOptions`;
  - the arrays in `UserScramCredentialsDescription`, `ClientQuotaAlteration`,
    `ClientQuotaFilter` and `TokenInformation`.
- `OffsetAndMetadata.ToString` field order (.NET prints offset, metadata, epoch; Java prints
  offset, epoch, metadata). It makes no Java-parity claim, and `PublicConsumerOffsetValueTypeTests`
  pins it.
- G6-1's blank-name half on the mock (F4).
- Dead P/Invokes outside the `kafka_admin_` prefix, and the test-only managed helper
  `DeleteAclsResultMarshal.FilterResultsReader` (D6).
- P13.4 PM-1 (six stale "borrowed error" result docs).

---

## 8. Decisions for the user

| # | Decision | Options | Recommendation |
|---|---|---|---|
| **D1** | **Cadence** | as §1.1 vs a user gate between S1 and S2 | **As §1.1.** This is the P13.4 shape: two sub-stages, the PM checks the gates between them, one Critic pass at the end, and fixups only on re-check. |
| **D2** | **X8 rulebook edit** (`dotnet/CLAUDE.md`, a rule file) | approve the exact text below vs leave it | **APPROVED (2026-10-02)**: a separate commit in S2b. **`:59-60`**, replacing "Admin / transactions are **not** exposed yet.": *"The admin client is exposed as `IAdmin` (`KafkaAdminClient` / `MockAdminClient`, M15); producer transactions are **not** exposed yet."* **`:423-424`**, replacing "The **admin client** (`IAdminClient`) is still **Mode B** — sketched once its C ABI lands (§6.3).": *"The **admin client** shipped in M15 as `IAdmin` (`KafkaAdminClient` / `MockAdminClient`), a Mode A port over the `kafka_admin_*` ABI; its decisions are in `design/current/STATUS.md`."* |
| **D3** | **G3-7 / G1-10 literal-`null` ambiguity** | (a) as briefed: accept CS0121 on a single literal `null`, rewrite the 4 + 2 in-tree sites to `()` or a cast; (b) G3-7 default only, with no options-only overload (positional `(o)` would then need `options:`) | **(a).** It is compile-time only and pre-1.0, and (a) is the only way Java's options-only form is writable positionally. |
| **D4** | **G3-8 constructor shape** | (a) three public overloads with Java's exact arities (2/4/5), `long?` sizes, Java's `-1` (`UNKNOWN_VOLUME_BYTES`) mapped to `null`, and `ArgumentNullException` for a null `replicaInfos`; (b) one ctor with optional parameters (the `OffsetAndMetadata` ctor-merge precedent) | **(a).** It gives exactly Java's arities and no 3-argument form Java lacks. The `-1` mapping keeps a Java-ported `(…, -1, -1)` faithful (`LogDirDescription.java:49-50`). The null check follows `PartitionReassignment`. |
| **D5** | **G3-6 siblings** (§7) | (a) G3-6 only, as briefed; (b) plus the 3 `PartitionReassignment` lists; (c) all 13 getters | **(a)**, as the brief requires, with the list recorded as a follow-up. Note the N=82 lesson: the named fix leaves the same defect behind green tests. (c) is cheap mechanically, but needs a per-type Java check. |
| **D6** | **X6 scope** | as §4.1 vs also the dead non-`kafka_admin_` declarations | **As §4.1.** Delete the 75 `kafka_admin_*` declarations. Keep the 8 tests-only ones, **unless one is "dead too"**: its only referrer is a test that exists solely to exercise that declaration. Any such deletion is listed in the commit message with its test. Other prefixes are reported, not deleted. |

---

## 9. Risks

- **R1, `OffsetAndMetadata` equality reaches the consumer.** Value equality could change a
  test that relied on reference identity. Mitigation: the full suite on both TFMs.
- **R2, the X2 real-client test may be timing-sensitive.** Mitigation: the differential design,
  with the seam fallback in §3.4.
- **R3, the `_isMock` flip can re-route existing seam tests.** A test that drove the real-path
  empty-map guard through `CreateMock` now sees no throw. Mitigation: §3.3's re-point
  instruction.
- **R4, X6 can shrink reflection sweeps silently** (the M17/P2 prefix-sweep lesson).
  Mitigation: the Prelink count is pinned, and the Actor reports the before and after counts
  of `AdminNativeMethodsMarshallingTests` and `CommonNativeMethodsMarshallingTests`.
- **R5, base drift.** If the branch advances before S1, re-derive §2 and the §3.4 counts first.
  HEAD is `21458241` at drafting.
