# M15/P13.4 — .NET Admin: one C-string guard for every admin string, plus ten low parity fixes

> **Approved by the user on 2026-09-30**, with all nine decisions D1–D9 taken as
> recommended in §8: D1 (a), D2, D3 out, D4 in, D5 (a), D6 as in §4.7, D7 (a), D8 all 13
> files, D9 cadence (no user gate between S1 and S2; one `dotnet-critic` 87 pass after
> S2; the Critic re-checks only the fixup commits). Written by the Manager on 2026-09-30.

**N = 87** (highest used in the binding is 86, M15/P13.3; nothing ≥ 87 exists under
`bindings/dotnet/`, `design/` or `.claude/agent-memory/`). Branch
`prashah_dev_dotnet_binding`. **Base = HEAD `de51d14b`**, unchanged since the audit.
**Mode A**: C# only, from the header down. No Rust, ABI, header, Python or C change is
planned, and none is needed (§7 lists the one Rust-side observation, G1-2, as out of
scope).

**Source.** A clean-slate static audit at `de51d14b`, stored under the session
scratchpad at `parity-de51d14b/` (`G1.json`…`G7.json`, `cross.json`, and
`overrides.json` for the final severities). This phase edits none of those files. Every
finding was re-verified at HEAD (§2). Line numbers below are HEAD's.

---

## 0. Scope

Fifteen findings. Group A is five medium findings with one root cause. Group B is ten
low ones.

| ID | Sev | Finding (short) | Sub-stage |
|---|---|---|---|
| **G2-1** | med | IAC config name/value pinned unchecked → NUL truncates | S1 |
| **G3-3** | med | ElectLeaders / ListPartitionReassignments topics, AlterReplicaLogDirs path unchecked | S1 |
| **G4-6** | med | DescribeAcls filter, DescribeClientQuotas filter, DescribeUserScramCredentials users (+ result lookup) unchecked | S1 |
| **G7-1** | med | ForceTerminateTransaction id, AbortTransaction topic, ListTransactions pattern unchecked | S1 |
| **X12** | med | Alter/DeleteConsumerGroupOffsets, RemoveMembers, CreateDelegationToken strings unchecked | S1 |
| **G1-3** | low | `PartitionSizeLimitPerResponse` doc implies an effect; stale `admin.rs` cite | S2 (docs) |
| **G1-5** | low | Stale "all Tasks complete at the same instant" remark (4 named; 13 at HEAD — D8) | S2 (docs) |
| **G1-6** | low | `TopicMetadataAndConfig.EnsureSuccess` wraps → top-level `Code` 0 | S2 |
| **G1-7** | low | Its doc says Java wraps; Java rethrows | S2 (with G1-6) |
| **G1-8** | low | Null topic name throws synchronously; Java fails that key only | S2 (docs, per D1) |
| **G1-9** | low | `TopicIds()` / `TopicNames()` hand out the mutable backing `List` | S2 |
| **G2-4** | low | `AlterConfigOp` accepts an undefined `AlterConfigOpType` | S2 |
| **G2-5** | low | `Config` lacks Java's map semantics and `equals`/`hashCode`/`toString` | S2 |
| **G2-6** | low | `IAdmin.DescribeCluster` says "Two" nullable awaitables; there are three | S2 (docs) |
| **G2-8** | low | Config types' `ToString` claims Java parity but prints C# enum names | S2 |

**Python halves are out of scope**, as briefed. They are noted per finding in §3 and §4
and are never planned.

---

## 1. Standing constraints (relay verbatim to the Actor and the Critic)

1. **Cadence (proposed as D9).** Two sub-stages, S1 then S2. The Actor finishes each one
   (build, tests, self-review, commits) and the PM checks that sub-stage's gates (§6.1).
   **No Critic runs between sub-stages.** `dotnet-critic` 87 reviews the whole range
   `<base>..HEAD` **once, after S2** (§5). If it files findings, the Actor fixes them with
   `fixup!` commits and the Critic re-checks **only those fixups**, until nothing is left.
2. Stage files by explicit path only (never `git add -A`, `.` or `commit -a`). Never stage
   the binding-root `COMMENTS.87.md` / `COMMENTS.DONE.87.md` (`bindings/dotnet/CLAUDE.md`
   §8.4), this draft, or unrelated untracked files. Do not push or squash. Do not edit the
   audit folder.
3. **Critic ground truth**: the C ABI header plus the Kafka 4.3.1 Java public API under
   `kafka/` (`bindings/dotnet/CLAUDE.md` §8.2). Each finding's parity anchor is in §3–§4.
4. **Environment traps** (each has faked a PASS before):
   - Prefix every shell command with
     `export PATH="/usr/bin:/bin:/usr/local/bin:/opt/homebrew/bin:$HOME/.cargo/bin:$HOME/.nix-profile/bin:$HOME/.dotnet:$PATH"`.
   - Use `~/.dotnet/dotnet` for dotnet.
   - `grep` is ugrep: use `/usr/bin/grep`. There is no `sed`: use `awk` or python. `cat` is
     bat: use `/bin/cat`. The shell is zsh: quote globs, and remember an unquoted `$var`
     is not word-split. Set `PYTHONDONTWRITEBYTECODE=1`.
   - A filter that matches zero tests still passes: always report `Passed:` / `Total:`.
   - `Test Run Aborted` means a crash, whatever the exit code.
   - A failed build followed by `--no-build` prints a stale `Passed!`: assert `0 Error(s)`.
   - Always `cargo build --features ffi`, never a bare `cargo build`.
   - Bound every tool's output. Grep `src/ffi/admin.rs` and the header; never read them
     whole.
5. **Exact messages** (DoD §3). Every new or rewritten exception assertion checks the
   message, plus `ParamName` for `Argument*Exception`. For `ArgumentException`, use
   `StartsWith(message)`, because .NET appends ` (Parameter '…')`.
6. **Reflection pins every public-surface change** (§4.10). A wrong C# signature compiles
   and passes every behavioural test (the M15/P1 lesson).
7. **Doc-only edits go last** in S2 (the M15/P13.3 lesson). Cite code by **symbol name**,
   not by line number (P13.3 D17). Java cites keep `File.java:line`, checked against
   `kafka/`.

---

## 2. Re-verification at HEAD `de51d14b`

**All 15 findings reproduce.** No line moved by more than 3 from the audit. Deltas:

| ID | Reproduces at | Delta from the brief |
|---|---|---|
| G2-1 | `NativeAdminClient.cs:2208`, `:2221` pin; only `:2124` validates (the resource name) | none |
| G3-3 | `:2749` ElectLeaders, `:5417` ListPartitionReassignments, `:2475` log dir | none |
| G4-6 | DescribeAcls `:3764/3766/3767`; DescribeClientQuotas `ClientQuotaMarshal.Pin` (`:73`) → `Set` `:296/298`; SCRAM users `:4105` → `PinNames` (`:6357`, pins at `:6369`); result lookup `DescribeUserScramCredentialsResult.cs:92` | **The quota filter's entity type (`:296`) is also unchecked**, not only the match name |
| G7-1 | `:5323`, `:5250`, `:5102` | none |
| X12 | `:3102/3108/3112`, `:3254/3259`, `:3447/3448/3458`, `:4296/4297`, `DelegationTokenMarshal.cs:208-209` | none |
| G1-3 | `DescribeTopicsOptions.cs:52-73`; the cited `src/ffi/admin.rs:3712` is now `describe_topics_options` (~`:4218`) | The core has **no** `response_partition_limit` reference in `src/` at all: it never sends DescribeTopicPartitions, so the value reaches no request. The header does not document the parameter. |
| G1-5 | `CreateTopicsResult.cs:37-40`, `DeleteTopicsResult.cs:40-43`, `CreatePartitionsResult.cs:34-37`, `DeleteRecordsResult.cs:43-46` | **The same paragraph is on 9 more result types** (§4.2). Deleting only the 4 named would leave **7** of those 9 saying "the same recorded limitation as `CreateTopicsResult`", pointing at a deleted paragraph. The other 2 (`AlterClientQuotasResult`, `DeleteAclsResult`) point at `CreateAclsResult`'s, which is itself one of the 7. All 13 RPCs bind a per-key `*_async` entry point whose header callback fires "once per key, as that key's own future resolves". → **D8** |
| G1-6/7 | `Admin/TopicMetadataAndConfig.cs:124-136`; Java `CreateTopicsResult.java:151-154` is `throw exception;` | No existing test pins the wrapping (grep: no `new TopicMetadataAndConfig(KafkaException)` in tests) |
| G1-8 | `NewTopic` ctor; `DistinctNames` (`NativeAdminClient.cs:6715-6736`); `TopicCollection.OfTopicNames` accepts a null element, which the RPC then rejects | none |
| G1-9 | `TopicCollection.cs:123`, `:149`; Java `TopicCollection.java:58-59`, `:77-78` | none |
| G2-4 | `Admin/AlterConfigOp.cs:58-62`; pinned by `Interop/AdminConfigsLifetimeTests.cs:153-178` (`UnknownOpTypeCode_DrivesTheRealInlineCallbackPath`) | The internal sentinel `ZeroOpSentinelOpType = -1` (`NativeAdminClient.cs:70`) must also be unreachable from the public ctor. `grpc-server/TranslateAdmin.cs:397` casts proto op types unchecked, so an undefined value now throws there synchronously. No harness scenario sends one (grep `tests/`: 0). |
| G2-5 | `Admin/Config.cs:39-82` | No test relies on duplicate `Entries` or on `Get(null)` throwing (grep: 0) |
| G2-6 | **`Admin/IAdmin.cs:279-280`** (the brief says `IAdmin.cs`) | none |
| G2-8 | `ConfigResource.cs:144-145`, `Admin/AlterConfigOp.cs:92-94`, `Admin/ConfigEntry.cs:265-292` | **`ConfigEntry.ConfigSynonym.ToString` (`:502-504`) has the same defect and is embedded in `ConfigEntry`'s rendering**, so it is in scope. Six .NET-rendering assertions pin today's output (§4.9). Two test strings that look similar, `AdminConfigsMarshalTests.cs:164` and `AdminP9PerKeyStage2Tests.cs:133`, are **core** messages; they must not change. |

### 2.1 Group A — the mechanical inventory (the load-bearing list)

Enumeration command. The Actor runs it at S1 start and again at S1 end, and the Critic
re-runs it:

```
cd bindings/dotnet/src/Confluent.Kafka && /usr/bin/grep -rn 'Utf8Marshal\.Pin(\|PinName(\|PinNames(\|PinPrincipals(' --include='*.cs' . \
  | /usr/bin/grep -v -e NativeProducer -e NativeConsumer -e ConsumerCallbacks -e ProducerSendMarshal -e 'Utf8Marshal.cs' \
  | /usr/bin/grep -v ':\s*///'
```

At HEAD this prints **70 lines**:
- 1 comment line (`AclRowMarshal.cs:85`);
- 3 helper definitions (`PinName`, `PinNames`, `PinPrincipals`);
- 2 helper internals (`AclRowMarshal.cs:87`, `NativeAdminClient.cs:6369`);
- **64 call sites**, classified in the four tables below.

The only other C-string encoder in `src/` is `PinnedTopicCache` (producer, out of scope).
SCRAM passwords and salts cross as length-prefixed bytes, so they are exempt.

**Already guarded (21, unchanged). These are per-key key strings.** CreateTopics name
(`NewTopicMarshal.cs:54`, guarded at `:1053`), CreatePartitions `:1520`, DeleteRecords
`:1687`, DescribeConfigs `:1997`, IAC resource name `:2194`, AlterReplicaLogDirs topic
`:2469`, DescribeReplicaLogDirs `:2614`, AlterPartitionReassignments `:2930`,
DescribeProducers `:4992`, ListOffsets `:5586`, ListConsumerGroupOffsets group id `:6225`,
`Submit` `:6932` (via `DistinctNames`), UpdateFeatures / FenceProducers /
DescribeTransactions `PinNames` `:4717/4819/4899`, Create/DeleteAcls rows
`AclRowMarshal.cs:371/373/374`, AlterClientQuotas entity `ClientQuotaMarshal.cs:393/398`,
AlterUserScramCredentials user `AlterUserScramCredentialsMarshal.cs:154`.

**Exempt (5). The binding's own enum-derived wire names, never user text.**
ListTransactions state names `:5161`; ListGroups states/types `:5718`, `:5720`;
ListConsumerGroups states/types `:5858`, `:5859`.

**Construction config (2) → D3.** `NativeAdminClient.Create` `:926`, `:927`. The same
pattern is in `NativeProducer.cs:199-200` and `NativeConsumer.cs:372-373`.

**To guard (36 call sites → 22 site groups).** Site groups A16–A22 are **not in the
brief**: the sweep found them.

| # | Finding | RPC / surface | Strings | Call sites | Java anchor (4.3.1) |
|---|---|---|---|---|---|
| A1 | G2-1 | incrementalAlterConfigs | config name, value | `:2208`, `:2221` | `KafkaAdminClient.java:2857` |
| A2 | G3-3 | alterReplicaLogDirs | log-dir path | `:2475` | `:2930` |
| A3 | G3-3 | electLeaders | topic | `:2749` | `:3886` |
| A4 | G3-3 | listPartitionReassignments | topic | `:5417` | `:4074` |
| A5 | G4-6 | describeAcls | filter name, principal, host | `:3764/3766/3767` | `:2570` |
| A6 | G4-6 | describeClientQuotas | component entity type, match name | `ClientQuotaMarshal.cs:296/298` | `:4286` |
| A7 | G4-6 | describeUserScramCredentials | user names | `:4105` | `:4345` |
| A8 | G4-6 | `DescribeUserScramCredentialsResult.Description(userName)` | lookup name | `DescribeUserScramCredentialsResult.cs:92` | `DescribeUserScramCredentialsResult.java:114-125` |
| A9 | G7-1 | forceTerminateTransaction | transactional id | `:5323` | `:4862` |
| A10 | G7-1 | abortTransaction | topic | `:5250` | `:4842` |
| A11 | G7-1 | listTransactions | `FilteredTransactionalIdPattern` | `:5102` | `:4880` |
| A12 | X12 | alterConsumerGroupOffsets | group id, topics, offset metadata | `:3102/3108/3112` | `:4250` |
| A13 | X12 | deleteConsumerGroupOffsets | group id, topics | `:3254/3259` | `:3787` |
| A14 | X12 | removeMembersFromConsumerGroup | group id, reason, group instance ids | `:3447/3448/3458` | `:4219` |
| A15 | X12 | createDelegationToken | owner type/name, renewer types/names | `:4296/4297`, `:4293` → `DelegationTokenMarshal.cs:208/209` | `:3290` |
| **A16** | sweep | createTopics | `NewTopic.Configs` names, values | `NewTopicMarshal.cs:77/89` | `:1782` |
| **A17** | sweep | alterClientQuotas | op keys (quota names) | `ClientQuotaMarshal.cs:423` | `:4314` |
| **A18** | sweep | describeDelegationToken | owner types/names | `:4511` → `DelegationTokenMarshal.cs:208/209` | `:3407` |
| **A19** | sweep | listGroups | `ListGroupsOptions.ProtocolTypes` | `:5719` | `:3480` |
| **A20** | sweep | listConsumerGroupOffsets | spec topic names | `:6244` | `:3745` |
| **A21** | sweep | `MockAdminClient.SetFeatureLevels` | feature names | `:7024` | `MockAdminClient.java:188` (Builder) |
| **A22** | sweep | `MockAdminClient.Update{Beginning,End,ConsumerGroup}Offsets` | topics | `:7078` (`SeedOffsets`) | `MockAdminClient.java:1486/1490/1494` |

The call-site count is 36 (A1–A22 plus the two shared `DelegationTokenMarshal` sites):
21 + 5 + 2 + 36 = 64.

---

## 3. S1 — Group A: one rule for every admin C string (5 medium)

**Rule.** Every user-supplied string that the admin client pins as a NUL-terminated C
string is checked by one shared validator. The validator rejects a NUL or an unpaired
UTF-16 surrogate. `null` passes, and each site keeps its own null precondition. `""`
passes, since it is a valid C string.

**Error channel (D2).** Synchronous `ArgumentException`, the same channel as the existing
key check.
- It is thrown in each RPC's **precondition block**, before the operation is rooted and
  before anything is pinned or `DangerousAddRef`'d (ffi §B5 order). So nothing needs
  unwinding.
- `ParamName` is the public parameter that carries the string. For a string held in an
  `*Options` member, it is `options`, following the existing precedent (the
  `DescribeDelegationTokenOptions.Owners` null check blames `nameof(options)`).
- Every rejection uses one shared message (D2 also covers the rewording).

**Why this rule, and the Java anchor for all of A1–A22.** Java never truncates: every
string is length-prefixed on the wire, so the broker sees the full string and either
honours or rejects it. The binding can only hand the ABI a C string, and the ABI stops
at the first NUL (`CStr::from_ptr`). So `"a\0b"` silently acts on `"a"`: a different
group, topic, user, transaction, config value or log dir. Java has no rejection to
mirror. The sync precondition is therefore a recorded .NET-only deviation (DoD §7), and
it is the one that keeps each request's target equal to the caller's.

Python agrees on the *channel*: `PyArg` `s`/`z` raise a synchronous `ValueError` on an
embedded NUL for G2-1, G3-3, G7-1 and X12's group id. Python's gaps (G7-1's pattern,
X12's nested rows, G4-6) are out of scope.

### 3.1 Changes

1. **`Internal/AdminKeyStrings.cs`**.
   - Rewrite the scope note (`:45-50`). Scope becomes every admin C string: request keys,
     request fields, `*Options` strings, the SCRAM result lookup and the mock's seeding
     methods. Out of scope: producer and consumer strings, the construction config (D3),
     and the binding's own enum-derived wire names.
   - The note must give **both** reasons: collapsing keys hang a countdown (round-70 F4),
     and truncation sends the request to the wrong target (this phase).
   - Rename the type and the message constant per D2.
2. **`Internal/NativeAdminClient.cs`**. Add the check for A1–A7 and A9–A22 in each
   method's existing precondition loop. Group the checks with the existing null checks, so
   every string is validated before `GCHandle.Alloc`.
   - Marshaller-fed rows (A6, A15, A16, A17, A18) are validated in the RPC over the
     caller's input, **not** inside `ClientQuotaMarshal`, `DelegationTokenMarshal` or
     `NewTopicMarshal`. Those marshallers run after rooting.
   - For A12, validate `OffsetAndMetadata.Metadata` itself (null passes), not the
     `?? string.Empty` substitute.
3. **`Admin/DescribeUserScramCredentialsResult.cs` (A8)**. Check the name in the
   synchronous `Description(string)` guard, next to the existing `ArgumentNullException`.
   It must not go in the `async` `DescriptionCore`, where the throw would become a faulted
   `Task` (ffi §A5).
4. **`Admin/IAdmin.cs` + `Admin/MockAdminClient.cs`**. Add or extend the
   `<exception cref="ArgumentException">` doc on every newly guarded RPC and seeding
   method. `IAdmin.cs:1186` (DescribeUserScramCredentials) today says only "contains a
   null element". Keep `KafkaAdminClient`'s and `MockAdminClient`'s `inheritdoc` intact.
5. The rename touches `KafkaAdminClient.cs:197` and `MockAdminClient.cs:209`. It changes
   no behaviour there.

### 3.2 Tests

- **`PublicAdminKeyStringTests.cs`** (renamed per D2). Add one row per guarded string
  role to `s_sites`. Each row calls the public surface on a `MockAdminClient` with a bad
  string and asserts a sync `ArgumentException`, the shared message (`StartsWith`) and
  `ParamName`, for both bad kinds (NUL, lone surrogate).
  - The inventory above gives **37 new rows** over A1–A7 and A9–A22: 2+1+1+1+3+2+1 +
    1+1+1+3+2+3+4 + 2+1+2+1+1+1+3. So 29 → **66**.
  - Pin the final row count, and the distinct-RPC/surface count, in
    `EveryGuardedStringHasARow`. If the Actor's count differs, it reconciles the
    difference in the commit message.
- **A8** gets its own test, because it is a result method, not an RPC. The mock faults
  this RPC ("Not implemented yet"), but the sync guard fires before `_views` is awaited,
  so the test needs no populated result: a bad name throws `ArgumentException`
  (`userName`) synchronously.
- **Over-rejection guard.** Extend `AKeyStringThatCrossesUnchanged_IsAccepted` to the new
  rows: `"délété"`, `"topic-🎈"` (a valid surrogate pair) and `"日本語"` raise no
  synchronous exception. A later asynchronous mock fault is fine.
- **B5-ordering guard, one test per shape** (6 shapes): direct request field (A1 value),
  `*Options` string (A11), marshaller-fed row (A6 match name), `PinPrincipals` row (A18),
  result lookup (A8), mock seeding (A22). After each rejection:
  - a follow-up valid call on the same client succeeds;
  - `Dispose` leaves `handle.IsClosed == true`, since a leaked `DangerousAddRef` or
    `GCHandle` would keep it open. This follows the
    `TwoKeysThatWouldCollapseAtTheAbi_…_AndTheClientStaysUsable` precedent.
- **`Interop/AdminKeyStringsTests.cs`**. Update the message constant and the rename. The
  unit truth-table (NUL, lone high, lone low, valid pair, empty, null) is unchanged.
- **Mutation checks** (recorded in the S1 commit message). For the six shape
  representatives above, delete the guard, run the filtered test, observe the named
  row(s) fail with the reason, and restore. Each row also has a vacuity guard: it asserts
  the **shared message**, so a site that throws `ArgumentException` for another reason
  (for example a null check with the same `ParamName`) cannot pass the row.

### 3.3 S1 commits (suggested)

1. `refactor(dotnet): widen the admin C-string guard's scope and message (M15/P13.4 S1)`.
   Rename, message and scope note only. Existing tests are updated for the new text. No
   new guard sites.
2. `fix(dotnet): guard every admin C string against NUL / lone surrogates (M15/P13.4 S1; G2-1, G3-3, G4-6, G7-1, X12)`.
   A1–A22, the IAdmin docs and the tests. The commit message carries the 70-line
   inventory reconciliation and the mutation evidence.

---

## 4. S2 — Group B: ten lows

Code changes come first (4.3–4.9). Doc-only changes come last (4.1, 4.2, 4.5b, 4.8).

### 4.1 G1-3 — `PartitionSizeLimitPerResponse` doc (docs)
- **Anchor:** `DescribeTopicsOptions.java:28`, `:57-69`; Java sends it as
  DescribeTopicPartitions' `ResponsePartitionLimit` (`KafkaAdminClient.java:2239`).
- **Change** (`Admin/DescribeTopicsOptions.cs:52-73`):
  - Say that the core currently sends no DescribeTopicPartitions request (by-name describe
    uses Metadata), so the value **has no effect on any request today**.
  - Replace the `src/ffi/admin.rs:3712` cite with the symbol `describe_topics_options`
    (`src/ffi/admin.rs`).
  - Drop "Zero is passed through untouched, as Java would" and "rather than honoured",
    because nothing is honoured.
  - Keep the negative-value `ArgumentOutOfRangeException` and its "stricter than Java"
    note. Behaviour is unchanged, as briefed.
- The core ignoring the value is **G1-2**, a Rust-core gap (§7). The doc should say
  "currently", so that it goes stale loudly rather than silently when G1-2 is fixed.
- No test (doc only).

### 4.2 G1-5 — stale "same instant" remark (docs; scope per D8)
- **Anchor:** Java returns one independent `KafkaFuture` per key (e.g.
  `CreateTopicsResult.java`'s `Map<String, KafkaFuture<TopicMetadataAndConfig>>`). Each
  RPC's header `kafka_admin_AdminClient_<rpc>_callback_t` is documented as firing "once
  per key, as that key's own future resolves".
- **Change.** Delete the "Deviation from Java, recorded." paragraph:
  - on the four named types;
  - under D8, also on `CreateAclsResult`, `DeleteAclsResult`, `AlterClientQuotasResult`,
    `DescribeConfigsResult`, `DescribeLogDirsResult`, `DescribeConsumerGroupsResult`,
    `DescribeClassicGroupsResult`, `DeleteConsumerGroupsResult` and
    `ListConsumerGroupOffsetsResult`.

  All 13 bind a per-key `*_async` entry point (verified in `NativeMethods.Admin.cs`).
  Before editing, the Actor re-greps `same instant\|resolves every key together\|timing independence`
  over `src/` and reports the count. `Internal/ListenerRegistration.cs:72` is an
  unrelated use of "same instant" and stays.
- No test (doc only).

### 4.3 G1-6 + G1-7 — `EnsureSuccess` keeps the error's identity fields
- **Anchor:** `CreateTopicsResult.java:151-154` (`throw exception;`). The stored exception
  is built at `KafkaAdminClient.java:1852-1857` from `Errors.forCode(topicConfigErrorCode)`
  or an `UnsupportedVersionException`.
- **Change** (`Admin/TopicMetadataAndConfig.cs:124-136`), shape per D7. Recommended: throw
  `new KafkaException(_exception.Code, _exception.Message, _exception.IsRetriable, _exception)`
  through the existing internal 4-arg ctor (`KafkaException.cs:131`). This keeps `Code`,
  `IsRetriable` and `Message` at the top level, keeps the stored exception as
  `InnerException`, and gives each accessor call a fresh stack trace.
- G1-7: rewrite the doc to say Java rethrows the stored exception **itself**, and record
  the .NET shape (same identity fields, fresh stack trace, original as inner) as the
  deliberate deviation D7 chooses.
- **Tests** (new, in `PublicAdminTopicResultConstructionTests.cs` or a sibling). Build
  `new TopicMetadataAndConfig(new KafkaException(code, msg, isRetriable: true))` with a
  **non-zero** code and `isRetriable: true`, so both assertions differ from a default
  `KafkaException` (Code 0, false). For all four accessors (`Config`, `TopicId`,
  `NumPartitions`, `ReplicationFactor`), assert:
  - `Code` and `IsRetriable` equal the stored ones;
  - `Message` is exact;
  - `InnerException` is the stored instance (`Assert.Same`) under the recommended shape.
  - **Mutation:** reverting to `new KafkaException(msg, inner)` fails the `Code` and
    `IsRetriable` assertions.

### 4.4 G1-9 — read-only `TopicIds()` / `TopicNames()`
- **Anchor:** `TopicCollection.java:58-59`, `:77-78` (`Collections.unmodifiableCollection`).
- **Change** (`TopicCollection.cs`). Build a `ReadOnlyCollection<T>` over the copied list
  once, in the ctor, and return it from `:123` and `:149`. **The declared return type
  stays `IReadOnlyCollection<T>`: no signature change.**
- **Tests:**
  - `names.TopicNames() is List<string>` is false;
  - `((ICollection<string>)names.TopicNames()).Add("x")` throws `NotSupportedException`
    and the contents are unchanged;
  - the same two for `TopicIds()`.
  - **Mutation:** reverting to `=> _topicNames` makes the cast succeed and fails the test.

### 4.5 G1-8 — null topic name (behaviour per D1)
- **Anchor:** `KafkaAdminClient.java:1739` (`topicNameIsUnrepresentable`: null or empty),
  used at `:1787` (createTopics), `:1924` (deleteTopics by name) and `:2334` (describeTopics
  by name). It fails only that key's future, with `InvalidTopicException`.
- **4.5a (only if D1 = per-key failure):** not planned. See D1 for why.
- **4.5b (recommended, D1 = document; docs, last).** Add a "stricter than Java" note on:
  - the `NewTopic` ctors' `ArgumentNullException` (Java's `NewTopic` does not null-check
    `name`);
  - `TopicCollection.OfTopicNames`, where a null element is accepted, as in Java, and then
    rejected by the RPC;
  - `IAdmin.CreateTopics` / `DeleteTopics` / `DescribeTopics`' `<exception>` docs
    (`Admin/IAdmin.cs:87`, `:132`, `:166`).

  Each note cites the Java lines above and names the binding-wide pattern (audit X10) as
  the open decision. `""` already matches Java (per-key) and gets no note. No behaviour
  change and no test change.

### 4.6 G2-4 — reject an undefined `AlterConfigOpType`
- **Anchor:** `AlterConfigOp.java:46-67` (`OpType` is closed: SET 0, DELETE 1, APPEND 2,
  SUBTRACT 3), `:83-89` (`forId`), `:91` (ctor). Java cannot express an undefined op type.
  Its only way to reach one is a null op type, which NPEs in `createRequest` and fails
  that Call as "Internal error sending …" (`KafkaAdminClient.java:1300-1309`).
- **Change** (`Admin/AlterConfigOp.cs:58-62`):
  `if (!Enum.IsDefined(typeof(AlterConfigOpType), opType)) throw new ArgumentOutOfRangeException(nameof(opType), opType, "opType must be a defined AlterConfigOpType member.");`.
  Add the `<exception>` doc. `ConfigResourceType` *normalizes* to `Unknown` instead
  (M15/P13.2). That option does not exist here, because Java's `OpType` has no UNKNOWN.
  Record the asymmetry in the remarks.
- **Tests (new):**
  - `(AlterConfigOpType)99` and `(AlterConfigOpType)(-1)` (the internal zero-op sentinel)
    each throw. Assert `ParamName == "opType"`, the message with `StartsWith`, and
    `ActualValue`.
  - All four defined members construct.
- **Existing test rewrite, `AdminConfigsLifetimeTests.UnknownOpTypeCode_DrivesTheRealInlineCallbackPath`.**
  Its purpose, proving the ABI's inline submit-failure callback releases the `GCHandle`
  and the span-the-op reference exactly once, is still valuable. Only the public ctor can
  no longer produce its input.
  - **Keep the test.** Drive the same path through the existing internal seam
    `IncrementalAlterConfigs(configs, options, NativeIncrementalAlterConfigsSubmit submit)`
    (`NativeAdminClient.cs:2090`). Pass a lambda that sets `opTypes[0] = 99` and then
    forwards every argument to the **real** `NativeMethods.AdminClientIncrementalAlterConfigsAsync`.
    The op in the dictionary is a valid `Set`.
  - All of its assertions stay: faulted with no await, `"op type"` in the message, and
    `handle.IsClosed` after `Dispose`.
  - **Tighten** `Contains("op type")` to the core's exact text. The format string is
    `unknown AlterConfigOp op type id {op_code} at index {i}` (`read_alter_config_ops`,
    `src/ffi/admin.rs`). Copy the rendered message from an actual run rather than
    composing it, in case the error type adds a prefix.
  - Update the remarks to say why the seam is used.
  - **Vacuity guard:** if the lambda forwarded the op unchanged, the "already faulted"
    assertion fails.
- Python's `test_admin.py:905` pins Python's still-permissive behaviour. That is out of
  scope; noted in §7.

### 4.7 G2-5 — `Config` map semantics and value members (shape per D6)
- **Anchor:** `Config.java:36-76`:
  - a `HashMap` keyed by name, `put`, so the last entry wins;
  - `entries()` is `unmodifiableCollection(values())`;
  - `get(null)` returns null;
  - `equals` is map equality; `hashCode` is `entries.hashCode()`;
  - `toString` is `"Config(entries=" + entries.values() + ")"`.
- **Change** (`Admin/Config.cs`), as recommended in D6:
  - **Entries.** One entry per name, and the last occurrence wins. It keeps the **first**
    occurrence's position, so the ABI's sorted order holds when there are no duplicates.
    `Entries` stays `IReadOnlyCollection<ConfigEntry>` and is read-only (wrapped, so it
    cannot be cast back to a `List`).
  - **`Get(string? name)`.** A null returns `null`, as Java does. The parameter annotation
    becomes nullable.
  - **`Equals(object?)`.** The same name set, each mapping to an `Equals` entry, ignoring
    order.
  - **`GetHashCode()`.** Order-insensitive, mirroring `AbstractMap`: the sum of
    `StringComparer.Ordinal.GetHashCode(name) ^ entry.GetHashCode()`.
  - **`ToString()`.** `Config(entries=[e1, e2])` in `Entries` order, each entry rendered
    by `ConfigEntry.ToString()`.
  - **No `IEquatable<Config>`.** This follows the `NewTopic` precedent (M15/P13.2 G1-7:
    overrides only).
  - The ctor still rejects a null `entries` or a null element with
    `ArgumentNullException`. That is stricter than Java's NPE, and unchanged.
- **Tests:**
  - duplicate names: count 1, the last value, the first position, and `Get` returns the
    last;
  - `Get(null)` is null;
  - two **distinct** instances with the same entries in different order are `Equals` and
    have the same hash;
  - differing in one value makes them unequal (so neither reference equality nor a
    constant-true `Equals` can pass);
  - exact `ToString`.
  - **Reflection:** `Equals`, `GetHashCode` and `ToString` are declared on `Config`, and
    `NullabilityInfoContext` reports `Get`'s parameter as nullable (net8.0+).

### 4.8 G2-6 — "Two" → "Three" (docs)
- **Anchor:** `KafkaAdminClient.java:2512-2536`. The controller is null when
  `NO_CONTROLLER_ID`, `clusterId` can be null on the Metadata fallback, and
  `authorizedOperations` is null when not requested.
- **Change:** `Admin/IAdmin.cs:279-280` says "Three of them" and names `Controller`,
  `ClusterId` and `AuthorizedOperations`, consistent with `DescribeClusterResult.cs:30`.
  The Actor greps for any other "Two of them" copy. No test.

### 4.9 G2-8 — config `ToString` (option per D5)
- **Anchor:** `ConfigResource.java:120-122` (`type=TOPIC`), `AlterConfigOp.java:119-124`
  (`opType=SET`), `ConfigEntry.java:183-194` and `:285-290` (`ConfigSynonym`). Java string
  concatenation renders `null` as `"null"`.
- **Recommended (D5 = Java names).** Render Java's constant names in
  `ConfigResource.ToString`, `AlterConfigOp.ToString`, `ConfigEntry.ToString` and
  `ConfigEntry.ConfigSynonym.ToString`:
  - `ConfigResourceType` → `UNKNOWN`/`TOPIC`/`BROKER`/`BROKER_LOGGER`/`CLIENT_METRICS`/`GROUP`;
  - `AlterConfigOpType` → `SET`/`DELETE`/`APPEND`/`SUBTRACT`;
  - `ConfigSource` → the 9 `DYNAMIC_…`/`STATIC_BROKER_CONFIG`/`DEFAULT_CONFIG`/`UNKNOWN`;
  - `ConfigType` → `UNKNOWN`/`BOOLEAN`/…/`PASSWORD`;
  - a null value or documentation renders as `null`.

  Implement each mapping as an explicit `switch` in a **private static** method on the
  type that renders it, with no new type (DoD §7). `ConfigSynonym` reuses
  `ConfigEntry`'s. An undefined `ConfigSource`/`ConfigType` (the public 8-arg
  `ConfigEntry` ctor does not normalize) falls back to the numeric value, and the fallback
  is documented. `ConfigResourceType` is normalized in its ctor, and `AlterConfigOpType`
  is rejected after G2-4, so neither can be undefined here.
- **Existing assertions that change (6):**
  - `PublicAdminP3ShapeParityTests.cs:340`, `:403`;
  - `Interop/AdminConfigsMarshalTests.cs:309`;
  - `PublicAdminConfigsShapeParityTests.cs:306`;
  - `PublicAdminShapeParityTests.cs:255` (and its continuation line).

  **Not** `AdminConfigsMarshalTests.cs:164` or `AdminP9PerKeyStage2Tests.cs:133`. Those
  are core messages, which render `type=Topic` from Rust.
- **New tests.** Build each enum's expected table **by hand, from the Java constants**,
  not derived from PascalCase in the test, and loop over `Enum.GetValues`. The test fails
  if a member is added unmapped. Also test null value and null documentation rendering as
  `null`, the undefined fallback, and a sensitive value still rendering `Redacted`.
- **Alternative (D5 = drop the claim).** Change the four doc lines to "a diagnostic
  rendering in Java's layout; enum members print their .NET names and a null prints
  empty". No code or test change.

### 4.10 Public API changes (pre-publish; each is pinned by a test)

| Finding | Change | Kind | Pin |
|---|---|---|---|
| Group A | About 20 RPCs, 3 mock seeding methods and `DescribeUserScramCredentialsResult.Description` newly throw `ArgumentException` for a NUL / lone-surrogate string | behavioural contract (+ xmldoc) | `PublicAdminKeyStringTests` rows |
| Group A (D2) | The shared message text changes, "key" → "string" | behavioural (message) | exact-message rows |
| G1-6 | `TopicMetadataAndConfig` accessors throw a `KafkaException` whose `Code` / `IsRetriable` equal the stored error's | behavioural | §4.3 |
| G1-9 | `TopicIds()` / `TopicNames()` return a read-only wrapper. **Declared type unchanged** | behavioural | §4.4 |
| G2-4 | `AlterConfigOp(ConfigEntry, AlterConfigOpType)` throws `ArgumentOutOfRangeException` | behavioural (+ xmldoc) | §4.6 |
| G2-5 | `Config.Equals` / `GetHashCode` / `ToString` **overrides added**; `Get(string)` → **`Get(string?)`**; `Entries` de-duplicated (last wins) | **surface** + behavioural | reflection + §4.7 |
| G2-8 (D5) | `ToString` of `ConfigResource`, `AlterConfigOp`, `ConfigEntry`, `ConfigSynonym` prints Java names and `null` | behavioural (diagnostic) | §4.9 |
| G1-3, G1-5, G1-7, G1-8 (D1), G2-6 | docs only | — | — |

No type, member or signature is removed. The extern count stays **697**.

### 4.11 S2 commits (suggested)
1. `fix(dotnet): G1-6/G1-7, G1-9 topic value types (M15/P13.4 S2)`
2. `fix(dotnet): G2-4, G2-5, G2-8 config value types (M15/P13.4 S2)`
3. `docs(dotnet): G1-3, G1-5, G1-8, G2-6 (M15/P13.4 S2)`. This commit is last and
   doc-only.

---

## 5. The one Critic pass (after S2)

`dotnet-critic` 87 reviews **`<base>..HEAD`**, the whole phase, in one pass. It writes to
`bindings/dotnet/COMMENTS.87.md`, taking the exclusive lock. Its scope:

1. **Inventory reconciliation.** Re-run the §2.1 command. Every one of the 64 call sites
   must match its class, and each "to guard" site must be validated **before** rooting
   (ffi §B5). Any site the table misses is a finding.
2. **Channel and message.** Sync `ArgumentException`, the shared message, the right
   `ParamName`, null and `""` still passing, and no over-rejection of valid surrogate
   pairs.
3. **Group B against its Java anchors** (§4). This covers the D5/D6/D7 shapes as ruled,
   and the rewritten lifetime test still driving the **real** ABI inline-failure path.
4. **Doc-only sentences.** Each re-cited or rewritten sentence must still say what the
   cited symbol or Java line says (the P13.3 lesson).
5. **Surface.** §4.10 must match the reflection tests. There must be no new type (DoD §7),
   no `IEquatable<Config>`, and an unchanged declared return type for G1-9.
6. **Mode A.** No Rust, header, Python or C diff; externs 697; `COMMENTS.DONE.87.md` not
   staged.

**Things the Critic must NOT flag:**
- the 5 exempt wire-name pins;
- the construction config, if D3 rules it out;
- the two core messages that render `type=Topic`;
- the Python halves;
- the out-of-scope siblings in §7.

---

## 6. Gates

### 6.1 Per sub-stage (the Actor runs them; the PM verifies them before the next sub-stage)
1. `cargo build --features ffi` (Debug) first. The header must be byte-identical to base.
2. Mode A: `git diff <base>..HEAD -- src/ cbindgen.toml generator/ build.rs Cargo.toml Cargo.lock tests/ bindings/python bindings/c`
   must be **empty**. `<base>` is the **actual parent of S1's first commit**, and the
   Actor states the SHA it used (the M15/P12 base-drift lesson).
3. `internal static extern` count **697 → 697**.
4. `~/.dotnet/dotnet build Confluent.Kafka.sln` (Debug): `0 Warning(s)`, `0 Error(s)`, six
   TFM outputs.
5. Filtered `dotnet test -f net10.0 --no-build` on the touched classes, with explicit
   `Passed:` counts and no `Test Run Aborted`.
6. **Full suite, net10.0 and net8.0**, Debug: `Failed: 0`, no abort. Report totals against
   the **baseline the Actor records at S1 start**. That baseline is run on base before any
   edit. It is not taken from memory (last recorded: 2630 at the P13.3 close, with one
   test commit since).
7. `dotnet format Confluent.Kafka.sln --verify-no-changes`. `grpc-server` must build, and
   must be format-clean if touched.
8. **S1 only:** the §2.1 command's output, reconciled line by line in the commit message
   (70 lines, 64 call sites, 0 unguarded user-string sites apart from the D3 outcome).

### 6.2 Final
9. Critic 87 is clean (§5).
10. **Docker regression** (PM, at close, if `docker info` succeeds). S1 edits the
    precondition blocks of about 20 RPCs, so run the **admin `__grpc_dotnet` arms of every
    admin family** as a regression. P13.3 had 43 + 36 = 79; count with `-- --list` and
    run by exact names. Stage a **fresh** linux/amd64 `.so` and prove it
    (`nm -D` in `rust:1-bookworm`, compare the in-image sha256), then rebuild the sync
    .NET image. No scenario exercises NUL strings, and adding scenarios is cross-binding
    harness work, out of scope. If Docker is down: **CI-pending**, flagged.
11. Close-out (PM): update `design/current/STATUS.md` (including its `AdminKeyStrings`
    reference, `:84`); archive `COMMENTS.DONE.87.md` (or `COMMENTS.87.md` as a clean-pass
    record) and this plan here; reset the binding-root `COMMENTS.87.md`.

---

## 7. Out of scope (sibling observations, deliberately not added)

- **Python halves** of every finding. Of note: G2-4's permissive behaviour is pinned by
  `test_admin.py:905`, and G2-5/G2-8 have Python-style semantics.
- **G1-2** (medium): the core ignores `partition_size_limit_per_response` and never sends
  DescribeTopicPartitions. This is a **Rust-core dependency**. G1-3 only makes the .NET
  doc honest about it.
- **X10's other members** (G2-9, G3-1, G4-3, G5-1, G6-3, G6-9, G7-2): synchronous null
  rejection where Java fails per key. See D1.
- **Construction-config strings** for admin, producer and consumer, if D3 rules the admin
  case out. The fix belongs to one cross-client rule.
- **Other types whose `ToString` claims Java parity** but prints C# enum names, such as
  `ResourcePattern.ToString` (66 files claim Java `toString()` parity). `ConsumerGroupListing`
  already prints Java names. Only the four config types are in scope.
- **Core messages** that render `ConfigResource(type=Topic, …)` in Rust's style, where
  Java would say `TOPIC`. This is Rust-side.
- The `PartitionSizeLimitPerResponse` negative-value rejection, whose original rationale
  goes away once nothing is honoured. Behaviour is kept, as briefed.

---

## 8. Decisions for the user

| # | Decision | Options | Recommendation |
|---|---|---|---|
| **D1** | **G1-8: null topic name** | (a) document the sync throw as a recorded precondition; (b) fail only that key's Task, for G1-8 alone; (c) do X10 binding-wide | **(a).** (b) makes topics the only family that fails a null per key, while 7 sibling findings keep throwing, and it fights the shapes: `Dictionary` keys cannot be null, and `NewTopic`'s ctor would also have to accept a null name. (c) is 8 findings and out of scope. (a) closes G1-8 exactly as the audit's own fix line allows, and leaves X10 as one future binding-wide decision. |
| **D2** | **Group A error channel, null handling, message** | Channel: sync `ArgumentException` vs a faulted Task. Null: skip vs reject. Message: keep "An admin request **key** …" vs "An admin request **string** …". Name: keep `AdminKeyStrings` vs rename to `AdminStrings`. | **Sync `ArgumentException`**, thrown before rooting (ffi §B5), with `ParamName` = the carrying parameter (`options` for options members). It matches the existing key check and Python's sync `ValueError`, and Java has no channel to mirror. **Null and `""` pass**; each site keeps its own null rule. **Reword "key" → "string"**: the message is user-visible, and "key" is wrong for a config value, a reason or a log dir. **Rename the type to `AdminStrings`** (internal, about 27 mechanical sites); the test files are renamed to match. The A8 result lookup uses the same channel. The Java-exact alternative, a "No such user" fault, would need a message synthesized in managed code, which the binding never does. |
| **D3** | **Admin construction config** (`NativeAdminClient.Create` `:926-927`) | in vs out | **Out.** Producer (`NativeProducer.cs:199-200`) and consumer (`NativeConsumer.cs:372-373`) pin their config identically. Guarding admin alone would make `AdminClient` reject a config dictionary the other two clients accept. Record it as one cross-client follow-up. |
| **D4** | **Mock seeding methods** (A21, A22) | in vs out | **In.** The rule is "every string the admin client pins". They are admin surface, the cost is 4 rows, and leaving them out would need its own exemption in the scope note. |
| **D5** | **G2-8: config `ToString`** | (a) render Java constant names and `null`; (b) drop the "matching Java" claim | **(a).** N=82 already made these same `ToString`s Java-faithful (lowercase bools), and `ConsumerGroupListing` prints Java names, so (a) completes an established direction. The cost is 4 private switch methods and 6 updated assertions. Choose (b) if you would rather not grow the Java-name pattern before the other ~60 types are decided. |
| **D6** | **G2-5: `Config` equality semantics** | as §4.7 vs variants (keep duplicates in `Entries`; order-sensitive equality; add `IEquatable<Config>`) | **As §4.7.** Map semantics: last wins, first position kept; `Get(null)` → null; order-insensitive `Equals`/`GetHashCode` over (name, entry), mirroring `AbstractMap`; `Config(entries=[…])`; overrides only (the `NewTopic` precedent). Java's `HashMap` order is unspecified, so the rendered order is .NET's own `Entries` order; that is documented. |
| **D7** | **G1-6: rethrow shape** | (a) a fresh `KafkaException(code, message, isRetriable, inner: stored)`; (b) `throw _exception;` (Java's identity); (c) `ExceptionDispatchInfo.Capture(_exception).Throw()` | **(a).** It keeps `Code`, `IsRetriable` and `Message` (the finding's substance), keeps the stored error as `InnerException`, and gives each accessor call its own stack trace, which the current doc already promises. (b) and (c) rethrow one shared instance, so concurrent accessor calls race on its `StackTrace`, and (b) overwrites it on every call. Java identity is not a .NET contract anyone can observe usefully. |
| **D8** | **G1-5 scope** | the 4 named result types vs all 13 | **All 13.** It is the same sentence with the same stale claim. Seven of the nine siblings cross-reference `CreateTopicsResult`'s paragraph, which the named fix deletes, and the other two point at `CreateAclsResult`'s. The M15 N=82 lesson applies: fixing only what a narrower review named leaves the rest behind green tests. Doc-only. |
| **D9** | **Cadence** | as §1.1 vs a user gate between S1 and S2, or you adjudicate Critic findings before any fix | **As §1.1.** The PM checks the gates between sub-stages, one Critic pass runs at the end, and the Critic re-checks only the fixups. This is the P13.3 shape that worked. |

---

## 9. Risks

- **R1: over-rejection.** A guard that rejects valid text (surrogate pairs, non-ASCII).
  Mitigation: the accept test extended to every new row (§3.2).
- **R2: guard placed after rooting.** It would leak the `GCHandle` / handle reference on
  throw. Mitigation: the 6 per-shape `IsClosed`-after-`Dispose` tests plus Critic item 1.
- **R3: inventory drift.** A new pin site lands later without a guard. Mitigation: the
  pinned row count and the scope note naming the rule. A later phase could promote the §2.1
  command into a test, but that is not planned.
- **R4: `Config.Entries` de-duplication.** It changes what a caller that passes
  duplicates sees. No in-tree caller does (grep), and the API is pre-publish.
- **R5: Docker.** Its availability is unknown. If it is down, the regression is
  CI-pending (§6.2 item 10).
- **R6: base drift.** If the branch advances before S1 starts, re-derive the §2.1 counts
  and line numbers first. HEAD is `de51d14b` at drafting.
