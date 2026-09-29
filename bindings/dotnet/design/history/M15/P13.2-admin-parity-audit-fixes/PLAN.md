# M15/P13.2 — .NET Admin: close ten parity-audit findings (configs, ACLs/quotas/SCRAM, topic types, `TopicPartition`)

**N = 85.** Branch `prashah_dev_dotnet_binding`. **Base = HEAD `7e550b95`** (M15/P13.1
closed; 8 commits ahead of origin, not pushed — leave it that way).
**Mode A** (C#-only). **APPROVED 2026-09-29.** D1, D2 and D4–D13 are approved exactly as
recommended; **D3 was changed by the user** (no Rust doc fix and no note to anyone — the
two header inaccuracies are recorded as known, with no follow-up assigned; see D3 and
§1.4). Four checkpoints run back to back; `dotnet-critic` 85 reviews **after each
checkpoint** until clean before the next starts (D6).

N derivation: unfiltered `find -name 'COMMENTS*.md'` over the repo (excluding `target/`,
`kafka/`) → highest used = 84 (`bindings/dotnet/COMMENTS.84.md`,
`bindings/dotnet/COMMENTS.DONE.84.md`, and the archived
`design/history/M15/P13.1-group-offsets-single-callback-abi/COMMENTS.DONE.84.md`).
Nothing ≥ 85 exists anywhere.

Source data: the user-owned audit baseline
`Dotnet-AdminClient-Findings-Workflow/baseline/76629aea/` (`G1.json`…`G4.json`,
`known.json`) with `overrides.json` applied (G2-1 high→medium, G4-2 high→medium).
**Neither that folder nor `PendingAdminClientFindingsForDotnet.md` is edited by this
phase.** The snapshot is older than HEAD, so every finding was re-verified at `7e550b95`
(§2); line numbers below are HEAD's.

---

## 0. Scope

| ID | Sev | RPC(s) | Finding | Approach | CP |
|---|---|---|---|---|---|
| **F1** | med | incrementalAlterConfigs | Zero-op resource completed as success locally | Send the core's no-op sentinel row; delete local completion | CP1 |
| **G2-1** | med | describeConfigs, incrementalAlterConfigs | Undefined `ConfigResourceType` not normalized → answer lost / hang + leak | **Normalize** to `Unknown` in the `ConfigResource` ctor (user's direction) | CP1 |
| **G2-4** | low | describeConfigs, incrementalAlterConfigs | Java's public 8-arg `ConfigEntry` ctor is `internal` | Publish it (user decided); update the shape test and the in-code note | CP1 |
| **G4-1** | med | deleteAcls | `FilterResult` drops the binding when the ACL has an error | Read the binding unconditionally; one `(AclBinding?, KafkaException?)` ctor | CP2 |
| **G4-2** | med | describeUserScramCredentials | Users silently de-duplicated | Pass the list through verbatim (null-element handling unchanged — G4-3 is out) | CP2 |
| **G4-4** | med | alterClientQuotas | Empty entity rejects the whole call synchronously | Drop the managed check; send a count-0 row with a **non-null** types pointer | CP2 |
| **G1-4** | low | createTopics, createPartitions, listOffsets (+deleteRecords via `TopicPartition`) | Java-legal inputs rejected | `NewTopic` accepts −1; `IncreaseTo(n, null)` accepted; null `OffsetSpec` per D2; `TopicPartition` part → G3-4 | CP3 |
| **G1-6** | low | createTopics, deleteTopics, describeTopics | Topic `*Result` types not user-constructible | Public ctors mirroring Java's `protected` ones (shape per D5) | CP3 |
| **G1-7** | low | createTopics, deleteTopics, describeTopics | `Uuid` constants/`RandomUuid`/`CompareTo`; `NewTopic` equality/`ToString` | Add them (RandomUuid per D4) | CP3 |
| **G3-4** | med | alterPartitionReassignments, listPartitionReassignments, electLeaders | Invalid partitions fail synchronously in the `TopicPartition` ctor | **Relax the ctor** (user's direction); per-call-site guards per §3.10 / D1 | CP4 |

---

## 1. Standing constraints (relay verbatim to Actor + Critic)

1. **Cadence — RULED 2026-09-29 (D6).** P13.1's cadence: normal repo prose conventions
   (no "terse / token-constrained" rule); four
   checkpoints run back to back with **no user gate** between them; `dotnet-critic` 85
   reviews **after each checkpoint** and the fix → re-review cycle repeats until the
   Critic reports nothing, then the next checkpoint starts.
   1. `dotnet-actor` 85 implements CPk and commits.
   2. `dotnet-critic` 85 reviews that checkpoint's commits (and the premises they rely
      on) and writes `bindings/dotnet/COMMENTS.85.md` (exclusive lock).
   3. The Actor fixes findings with `fixup!` commits referencing the original commit, and
      moves resolved items to `COMMENTS.DONE.85.md` (exclusive lock).
   4. Repeat 2–3 until clean; then CPk+1.
2. One checkpoint = one commit (+ its fixups) = one resumable Actor session.
3. Stage by explicit path only (never `git add -A` / `.` / `commit -a`). Never stage the
   binding-root `COMMENTS.DONE.85.md` (`bindings/dotnet/CLAUDE.md` §8.4), this phase's
   uncommitted working files, or unrelated untracked files. Do not push. Do not edit
   `PendingAdminClientFindingsForDotnet.md` or `Dotnet-AdminClient-Findings-Workflow/`.
   Do not squash.
4. **Critic ground truth**: the C ABI header + the Kafka Java public API
   (`bindings/dotnet/CLAUDE.md` §8.2). **Two header sentences are known to be wrong or
   ambiguous and this phase deliberately does not rely on them** — the Critic must not
   flag the C# for contradicting them:
   - **G4-1:** the header's `kafka_admin_DeleteAclsResult_get_binding` /
     `kafka_admin_DeleteAclsFilterResults_get_binding` docs say binding and error are
     "complementary … precisely one of them is non-null". The getter bodies
     (`src/ffi/admin.rs:15581-15594`, `:15728-15745`) return the entry's binding
     independent of its error, and the core stores both
     (`src/admin/kafka_admin_client.rs:1776-1782`, `FilterResult::new(binding, error)`),
     exactly as Java does (`KafkaAdminClient.java:2704-2707`). Python already reads both
     independently (`bindings/python/admin.py` ~2232-2241). Known header inaccuracy,
     out of scope, no follow-up assigned (D3).
   - **G4-4:** the async header's "an alteration with **no entity types**" (≈h:9440) means
     a **NULL** `entity_types[i]` pointer: `read_client_quota_alterations`
     (`src/ffi/admin.rs:14999-15006`) rejects only `types.is_null()`; a non-null pointer
     with `entity_counts[i] == 0` parses to an empty `ClientQuotaEntity` (`:15013` →
     `read_client_quota_entity`, `:14931-14945`, loop over `0..0`). The Actor's first
     CP2 test pins that empirically through the real ABI. Known header ambiguity, out of
     scope, no follow-up assigned (D3).
5. **Env traps** (they silently fake a PASS): `dotnet` = `~/.dotnet/dotnet`; prefix
   `export PATH="/usr/bin:/bin:/usr/local/bin:/opt/homebrew/bin:$HOME/.nix-profile/bin:$HOME/.cargo/bin:$HOME/.dotnet:$PATH"`
   if `git`/`cargo`/`sed` look missing; `grep` may be ugrep → use `/usr/bin/grep`; no
   `sed` → `awk`; `cat` may be `bat` → `/bin/cat`; zsh does not word-split unquoted
   `$var` and aborts on unmatched globs; a zero-match test filter reports 0 tests as a
   pass → always report `Passed:` counts; `Test Run Aborted` is the crash signal (exit
   codes vary); a failed build + `--no-build` prints a stale `Passed!` → assert
   `0 Error(s)`; build and test in the **same** configuration; never a bare
   `cargo build` (always `--features ffi`); bound every tool's output; grep
   `src/ffi/admin.rs` and `target/include/confluent_kafka.h`, never read them whole.
6. **Exact messages** (DoD §3): every new or rewritten assertion on an exception asserts
   the message (and `ParamName` for `Argument*Exception`). Core/mock messages are
   copied from the core, never paraphrased.
7. Reflection tests pin every public-surface change (§5) — the M15/P1 lesson: a widened
   or wrong C# signature compiles and passes every behavioural test.

---

## 2. Re-verification at HEAD `7e550b95` (brief vs. tree)

All ten findings **still reproduce**. Deltas from the brief / the audit record:

| ID | Reproduces | What changed or was wrong |
|---|---|---|
| F1 | yes — `NativeAdminClient.cs:2112-2135` (`keysWithNoRequest`), `:2166`, `:2217` (`SetPendingCallbacks(keys.Count - keysWithNoRequest.Count)`); `AdminOperation.cs:383-445` | The sentinel is in the header (≈h:4884-4889) and code (`src/ffi/admin.rs:4725-4731`, `distinct_config_resources` `:4774-4799` counts sentinel rows). **Sentinel rows skip op-type parsing**, so the `-1` op code Python sends is ignored, not rejected. **5 tests** touch the behaviour, not 4 (§3.1). A 6th stale comment: `NativeAdminClient.cs` ~2126-2129 says "the header skips a row whose … config name is NULL" — the header now says the opposite. |
| G2-1 | yes — `ConfigResource.cs:80-85` stores the raw value; keys de-duped by `EqualityComparer<ConfigResource>.Default` (`NativeAdminClient.cs:83-84`, `DistinctResources` `:6241-6258`) | None. `ConfigResourceMarshal.TypeFromId` (`Internal/Interop/ConfigResourceMarshal.cs` ~66-77) already normalizes the *callback* side with `Enum.IsDefined`; the ctor is the missing half. |
| G2-4 | yes — `Admin/ConfigEntry.cs:53-66` (recorded gap), `:150-168` (`internal` 8-arg ctor); `PublicAdminShapeParityTests.cs:107-117` | Java's `ConfigSynonym` ctor is **package-private** (`ConfigEntry.java:243`), so .NET's `internal ConfigSynonym` ctor already matches — a user passes an empty synonyms list, as in Java. Java does **not** null-check `synonyms`; .NET does (D8). |
| G4-1 | yes — `Internal/Interop/DeleteAclsResultMarshal.cs:79-97` (aggregate) and `:123-139` (per-key); `Admin/DeleteAclsResult.cs:93-118` | FFI doc lines moved: `src/ffi/admin.rs:15570-15575` and `:15722-15724` (brief: ~15322 / ~15469). Java's `FilterResult` ctor is **package-private**, so the new ctor is `internal` — **no public-surface change** for G4-1. The core can hold `binding == None` (`DeleteAclsResponse::acl_binding(..).ok()`), so the reader must accept (null binding, non-null error). |
| G4-2 | yes — `NativeAdminClient.cs:3966-3968` (`DistinctNames(users, …)`), helper `:6481-6501` | The broker returns **one** row per unique user with `DUPLICATE_RESOURCE` "Cannot describe SCRAM credentials for the same user twice in a single request: alice" (`ScramImage.java:79,126-128`), so the response has no duplicate keys — no duplicate-key hazard in `DescribeUserScramCredentialsViews`. The core mock does not implement this RPC ("Not implemented yet", `mock_admin_client.rs:1772-1784`), so the broker outcome is Docker-only. |
| G4-4 | yes — `NativeAdminClient.cs:3860-3865` | **ABI half verified by code reading** (constraint 4 above) — the audit's "likely" is now "yes, with a non-null pointer". `ClientQuotaMarshal.AlterationRows.Set` (`Internal/Interop/ClientQuotaMarshal.cs` ~372-392) today pins a **zero-length** `IntPtr[]`; `GCHandle.AddrOfPinnedObject` on an empty array is non-null only by **undocumented** runtime behaviour (`ffi-marshalling.md` §A4, ~683-689) → an explicit non-null placeholder is required (§3.6). Also stale: the `NativeMethods.Admin.cs` ~3585-3588 remark and the `NativeAdminClient.cs` ~3811-3824 remark ("Two of them are reachable…"). |
| G1-4 | yes — `Admin/NewTopic.cs:71-89`; `Admin/NewPartitions.cs:108-113`; `NativeAdminClient.cs:5334-5339` | **The ABI folds every negative** `num_partitions` / `replication_factor` into "unset" (`src/ffi/admin.rs:1250-1255`). So −1 is wire-identical to Java (Java sends −1 = `NO_NUM_PARTITIONS`), but −2… is **not representable**: Java sends it and the broker rejects it, the ABI would silently create with broker defaults (D7). |
| G1-6 | yes — `Admin/CreateTopicsResult.cs:51` (`internal`), `DeleteTopicsResult.cs:58` / `DescribeTopicsResult.cs:59` (`private`), all `sealed` | None. All 46 .NET `*Result` types are `sealed`; `DeleteRecordsResult` / `ListOffsetsResult` already have `public` ctors (the precedent for D5). |
| G1-7 | yes — `Uuid.cs:39` (`IEquatable<Uuid>` only, `Zero` only); `Admin/NewTopic.cs` (no overrides) | No ABI function generates a `Uuid` (`grep -i random` in the header: 0 relevant hits); the text form is already C# (`Uuid.cs` `ToString`/`Parse`). No code depends on `NewTopic` reference equality or on `Uuid` ordering (grep: 0 hits). |
| G3-4 | yes — `TopicPartition.cs:46-61` | Shape change from the audit's framing: **every** .NET site already rejects a null topic with its own guard, so relaxing the ctor changes nothing for null topics (the C ABI cannot carry one — D1). The substantive change is **negative partitions**: no admin RPC has a negative guard besides the ctor, so relaxing sends them to the core/broker exactly as Java does; the consumer keeps its own guards. Relaxing also removes a latent throw in result trampolines (§3.10). |

---

## 3. Per-finding plan

### 3.1 F1 — send the zero-op sentinel row (CP1, Mode A)

**Change** (`Internal/NativeAdminClient.cs` `IncrementalAlterConfigs`):
- For each key whose op collection is empty, emit **one sentinel row**:
  `resourceTypes[i] = (int)key.Type`, `resourceNames[i] = pinned name`,
  `configNames[i] = IntPtr.Zero`, `configValues[i] = IntPtr.Zero`, `opTypes[i] = -1`
  (Python's `(type, name, None, None, -1)` in `_incremental_alter_configs_keys_and_spec`;
  the op code is not read on a sentinel row — `src/ffi/admin.rs:4725-4731`).
- `operation.SetPendingCallbacks(keys.Count)` — the ABI now fires once per key
  (`distinct_config_resources` includes sentinel rows).
- Delete `keysWithNoRequest`, `SetKeysWithNoRequest`, and the whole divergence remark
  (≈:2040-2084); replace with a short remark citing the header sentinel sentence and
  Python. Fix the stale "header skips a row whose … config name is NULL" comment.
- `Internal/AdminOperation.cs`: delete `_keysWithNoRequest`, `SetKeysWithNoRequest`,
  `CompleteKeysWithNoRequest` and the `OnAllCallbacksComplete` override;
  `VoidKeyedAdminOperation` returns to a plain specialization (keep its type-level remark).

**Behaviour change (user-visible, Java-faithful):** a zero-op resource is now answered by
the core/broker — an absent topic faults (`"No such topic as {name}"` from the mock,
`UNKNOWN_TOPIC_OR_PARTITION` from a broker), an unauthorized one faults, `ValidateOnly`
validates, and a submit failure faults it with every other key.

**Tests (5 existing + 2 new):**

| Test | Disposition |
|---|---|
| `PublicAdminConfigsTests.IncrementalAlterConfigs_ZeroOpsAgainstAnAbsentResource_SucceedsLocally_AKnownDivergence` | **Invert + rename** (e.g. `…_ZeroOpsAgainstAnAbsentResource_FaultsExactlyLikeANonEmptyOne`): both (A) zero ops and (B) one op fault with `KafkaException` message `"No such topic as public-cfg-never-created"`. Delete the "expected to go RED" remark. |
| `PublicAdminConfigsTests.IncrementalAlterConfigs_AResourceWithNoOps_CompletesSuccessfully` | **Keep assertions** (existing topic → the core applies an empty op list → success); rewrite the remark: success now comes from the core, not local completion. |
| `PublicAdminConfigsTests.IncrementalAlterConfigs_OnlyZeroOpResources_CompletesSuccessfully` | **Keep assertions**; fix the summary ("no rows at all" → one sentinel row per resource). |
| `Interop/AdminConfigsSubmitArgumentTests.IncrementalAlterConfigs_AResourceWithNoOps_ContributesNoRow` | **Invert + rename** `…_ContributesOneSentinelRow`: `captured.Count == 1`, `ResourceNames == ["cfg-topic"]`, `ConfigNames == [null]`, config value NULL, `OpTypes == [-1]`; the key is **not** completed after the submit returns (pending on its callback); then fire the callback via `AdminCallbacks.IncrementalAlterConfigs` and assert it resolves. The `n == 0` submit-boundary assertion it carried moves to an **empty-map** test (add one if none exists for IAC — grep found none in `AdminP9PerKeyStage2/3Tests`). |
| `Interop/AdminConfigsLifetimeTests.ASubmitFailure_FaultsEveryNamedResource_AndLeavesAZeroOpResourceSuccessful` | **Invert + rename** `ASubmitFailure_FaultsEveryResource_IncludingAZeroOpOne`: two callbacks fired (code 61, `"submit failed"`), both keys fault with that exact message, `All()` faults. |
| **New** `IncrementalAlterConfigs_MixedOps_RowShapeAndCallbackCount` | Resource A (2 ops) + B (0 ops) + C (1 op) → rows `[A, A, B, C]`, config names `["a1", "a2", null, "c1"]`; fire exactly 2 callbacks → the third key is still pending (**the equivalent-mutant guard**: a leftover `keys.Count - zeroOps` countdown would resolve early — P13.1 lesson); fire the third → all resolve. |
| **New** `IncrementalAlterConfigs_ZeroOpsWithValidateOnly_IsSentAndAnswered` | Captured submit shows `validateOnly == true` and one sentinel row (the mock ignores `validate_only`, so the outcome itself is Docker-only). |

**Docs:** the `design/current/STATUS.md` "§15 Mode-B gap list" entries (~:46, ~:207,
~:215) and `PLAN-M15-admin-client.md` §15 are updated by the Manager at close
(Step 7), not by the Actor.

### 3.2 G2-1 — normalize undefined `ConfigResourceType` (CP1, Mode A)

**Change** (`ConfigResource.cs`): `Type = IsDefined(type) ? type : ConfigResourceType.Unknown`,
via a private static helper (explicit `switch` over the six members, or
`Enum.IsDefined(typeof(ConfigResourceType), type)` as `ConfigResourceMarshal.TypeFromId`
already does — Actor's choice, both are netstandard2.0-safe). Update the ctor xmldoc: an
undefined value (only reachable by an unchecked `int` cast) becomes `Unknown`, as Java's
`Type.forId` does (`ConfigResource.java:43-59`), as the ABI does
(`enum_code_or_unknown` + `ConfigResourceType::for_id`, `src/ffi/admin.rs:4723`) and as
Python does (`admin.py:379-398`). Normalizing on the one ctor serves both the key side
and the callback side.

**Tests (new):**
- Theory over `64, 1, 3, 5, -1, 255, int.MaxValue` → `Type == Unknown`; a loop over
  `Enum.GetValues(typeof(ConfigResourceType))` → each member preserved.
- `new ConfigResource((ConfigResourceType)64, "x")` equals and hashes like
  `(Unknown, "x")`; `ToString()` is exactly `"ConfigResource(type=Unknown, name='x')"`.
- `MockAdminClient.DescribeConfigs([(64,"x")])`: `Values` contains the key and it faults
  with the mock's exact message for the `Unknown` type (`get_resource_description`'s
  `_ =>` arm, `Error::unsupported_version("Not implemented yet")` — Actor copies the
  text from the core) — **not** "…contained no entry for …".
- Colliding keys `(64,"x")` + `(Unknown,"x")`: `DescribeConfigs` submits **one** resource
  (captured count 1) and the task settles within `TestTimeout` (no hang); the op's
  GCHandle / client ref are released (the existing lifetime-assert helper). The IAC
  dictionary initializer collapses them to one entry by construction.

`grpc-server/TranslateAdmin.cs:374,400` needs no change (it benefits).

### 3.3 G2-4 — publish `ConfigEntry`'s 8-arg ctor (CP1, Mode A)

**Change** (`Admin/ConfigEntry.cs`): `internal` → `public` on the
`(string name, string? value, ConfigSource source, bool isSensitive, bool isReadOnly,
IReadOnlyList<ConfigSynonym> synonyms, ConfigType type, string? documentation)` ctor.
Replace the "Recorded gap … raised rather than decided" remark (~:53-66) and the ctor's
"internal here per the recorded gap" summary (~:135-137) with: published in M15/P13.2 by
maintainer decision (2026-09-29). Keep `ConfigSynonym`'s ctor `internal` (Java's is
package-private). Null `synonyms` per D8.

**Tests:** replace `PublicAdminShapeParityTests.ConfigEntry_PublishesOnlyTheConstructorJavaHas`
with `ConfigEntry_PublishesBothConstructorsJavaHas`: exactly **two** public ctors; the
2-arg `(string, string)` and the 8-arg with exact parameter **types and names** in Java's
order. Behaviour: `IsDefault` derives from `source` (`DefaultConfig` → true); an entry
built with the public ctor `Equals` / hashes like an equal one; `name == null` →
`ArgumentNullException` `ParamName == "name"`; null `synonyms` per D8 (exact message).

### 3.4 G4-1 — keep the binding on a failed ACL delete (CP2, Mode A)

**Change:**
- `Admin/DeleteAclsResult.cs`: replace the two exclusive `internal` ctors with one
  `internal FilterResult(AclBinding? binding, KafkaException? error)` (Java:
  package-private `FilterResult(AclBinding, ApiException)`). Rewrite the "Exactly one of
  the two is non-null" remark: both may be non-null (a matched ACL whose delete failed),
  citing `KafkaAdminClient.java:2704-2707`. Property docs: `Binding` is "the matched
  ACL" (null only if the core could not decode it); `Error` unchanged.
- `Internal/Interop/DeleteAclsResultMarshal.cs` — **both** readers
  (`FilterResultsReader` ~:79-97, `FilterResultsPerKeyReader` ~:123-139): read the error;
  read the binding **unconditionally**; `readBinding` is called only when the binding
  pointer is non-null; (null, null) stays the malformed-row `KafkaException`
  (existing message); build `new FilterResult(binding, error)`.
- `DeleteAclsResult.All()`-style aggregate (~:75-85): unchanged (throws the first error,
  as Java).

**Tests:** `Interop/AdminP6ResultMarshalTests.InnerEntries_AreBindingXorError_OnACompletedFilter`
→ **rewrite** as `InnerEntries_CarryTheBindingAlongsideTheError…`: the failed entry has
**both** `Binding == Binding("failed")` and `Error` (code 42, `"could not delete"`); order
preserved. Add a (null binding, error) row → `Binding == null`, no throw; keep the
(null, null) malformed-row assertion with its exact message. The same three rows for the
per-key reader. The fixture gains an "add binding with inner error" helper. Any test
using the old ctors is updated.

**Rust half (FFI docs):** out of scope — recorded as a known header inaccuracy with no
follow-up (D3, as changed by the user).

### 3.5 G4-2 — pass SCRAM describe users through (CP2, Mode A)

**Change** (`NativeAdminClient.cs` ~:3966-3968): replace `DistinctNames(users, …)` with a
pass-through copy that keeps today's null-element check and **its exact message**
(`"The users must not contain a null element."`, `ParamName == "users"`) — that is G4-3
territory and stays unchanged. Update the method remark (duplicates reach the broker,
which answers `DUPLICATE_RESOURCE`, `ScramImage.java:126-128`; Java sends them verbatim,
`KafkaAdminClient.java:4345-4388`; Python likewise, `admin.py:3396-3401`).

**Tests (new):** captured submit for `["alice", "alice"]` → count 2, names
`["alice", "alice"]` in order; `["bob", "alice", "bob"]` → 3 rows in order; through
`MockAdminClient` a duplicated list does not throw synchronously and faults with the
mock's `"Not implemented yet"`. No existing test pins the de-dup (grep).

### 3.6 G4-4 — send an empty quota entity (CP2, Mode A)

**Change:**
- `NativeAdminClient.cs` ~:3860-3865: delete the `Entries.Count == 0` rejection. The
  repeated-entity rejection stays (two empty entities are a repeated entity — exact
  existing message with `ClientQuotaEntity(entries={})`, Actor copies it from
  `ClientQuotaEntity.ToString`).
- `Internal/Interop/ClientQuotaMarshal.cs` `AlterationRows.Set`: when the entity is
  empty, pin a **1-element placeholder** `IntPtr[]` for both `EntityTypes[i]` and
  `EntityNames[i]` (call-scoped, through the existing `PinArray`, released in `Dispose`)
  with `EntityCounts[i] = 0` — a documented non-null pointer rather than
  `AddrOfPinnedObject` of an empty array (`ffi-marshalling.md` §A4 ~683-689). A NULL
  inner pointer would fail the **whole call** with "quota alteration at index {row} has
  no entity types" (`src/ffi/admin.rs:14999-15006`) — the exact outcome this finding
  removes.
- Rewrite the `NativeAdminClient.cs` ~:3811-3824 and `NativeMethods.Admin.cs` ~:3585-3588
  remarks (one managed ABI-rejection check remains, not two).

**Tests:**
- `PublicAdminAlterClientQuotasTests.Preconditions_AreRejectedBeforeTheNativeCall`: delete
  the `empty` block (the "…with no entity types." assertion); the rest unchanged.
- **New** captured-submit: `[valid, empty]` → 2 rows; row 1 `EntityCounts == 0` **and**
  `EntityTypes[1] != IntPtr.Zero` (the pointer assertion is the point).
- **New** through the real ABI (`MockAdminClient`): `[valid, empty]` → **two** per-key
  results, each faulting with the mock's own `"Not implement yet"` (sic —
  `mock_admin_client.rs:1766`), **not** a whole-call "has no entity types" failure. This
  is the empirical pin for constraint 4's G4-4 note.
- **New** `[empty, empty]` → `ArgumentException` repeated-entity message.

### 3.7 G1-4 — accept Java-legal `NewTopic` / `IncreaseTo` inputs (CP3, Mode A)

- **`NewTopic`** (`Admin/NewTopic.cs:71-89`): accept `-1` for `numPartitions` and
  `replicationFactor` in the `(string, int?, short?)` ctor (and therefore the
  `(string, int, short)` one); **store −1** (not null), so `Equals`/`ToString` keep
  Java's `Optional.of(-1)` ≠ `Optional.empty()` distinction (§3.9). Values `< -1` per D7
  (recommended: still rejected, new messages e.g. `"Number of partitions must be -1 (the
  broker default) or non-negative; pass null to use the broker default."` — Actor fixes
  the exact text, the test asserts it). Marshalling is unchanged (−1 reaches
  `kafka_admin_NewTopic_new`, which maps it to unset — wire −1, identical to Java).
- **`NewPartitions.IncreaseTo(int, IReadOnlyList<IReadOnlyList<int>>?)`**: nullable
  parameter; `null` ⇒ `new NewPartitions(totalCount, null)` (Java: `increaseTo(n, null)`
  ≡ `increaseTo(n)`). The null-**inner**-list rejection stays.
- **Null `OffsetSpec`** in `ListOffsets` (`NativeAdminClient.cs:5334-5339`): per D2
  (recommended: keep the rejection and strengthen the test with its exact message).

**Tests:** `PublicAdminCreateTopicsTests.NewTopicPreconditions_RejectWhatTheAbiWouldReinterpret`
→ rewrite: −1 accepted for both, `-2` rejected with the new exact messages and
`ParamName`s. **New** `MockAdminClient.CreateTopics([NewTopic("t", -1, -1)])` succeeds and
`NumPartitions("t")` / `ReplicationFactor("t")` equal the mock's defaults
(`mock_admin_client.rs:706-709`, `default_partitions` / `default_replication_factor` —
values read from the core). Captured `NewTopic_new` args carry `-1`.
`PublicAdminP2bShapeParityTests.cs:267` (`Assert.Throws<ArgumentNullException>(() =>
NewPartitions.IncreaseTo(3, null!))`) → **invert**: `IncreaseTo(3, null).Assignments`
is null and `ToString()` equals `IncreaseTo(3).ToString()`. Reflection: the parameter is
annotated nullable (`NullabilityInfoContext`, net8.0+). `ListOffsets_RejectsANullSpecAndANullMap`
gains the exact message `"The offset spec for 't-0' must not be null."` (if D2 = keep).

### 3.8 G1-6 — user-constructible topic results (CP3, Mode A)

Shape per D5 (recommended: stay `sealed`, add `public` ctors):

| Type | Java (`protected`) | .NET public ctor |
|---|---|---|
| `CreateTopicsResult` | `(Map<String, KafkaFuture<TopicMetadataAndConfig>> futures)` `:35` | `(IReadOnlyDictionary<string, Task<TopicMetadataAndConfig>> futures)` — rename today's `values` param; `ArgumentNullException` on null (Java NPEs on first use) |
| `DeleteTopicsResult` | `(Map<Uuid, KafkaFuture<Void>> topicIdFutures, Map<String, KafkaFuture<Void>> nameFutures)` `:34` | `(IReadOnlyDictionary<Uuid, Task>? topicIdFutures, IReadOnlyDictionary<string, Task>? nameFutures)` — Java's two checks as `ArgumentException`, messages verbatim: `"topicIdFutures and nameFutures cannot both be specified."` / `"topicIdFutures and nameFutures cannot both be null."` |
| `DescribeTopicsResult` | `(Map<Uuid, KafkaFuture<TopicDescription>> topicIdFutures, Map<String, KafkaFuture<TopicDescription>> nameFutures)` `:37` | `(IReadOnlyDictionary<Uuid, Task<TopicDescription>>? topicIdFutures, IReadOnlyDictionary<string, Task<TopicDescription>>? nameFutures)` — same two checks/messages; `AllTopicIds`/`AllTopicNames` use `EqualityComparer<Uuid>.Default` / `StringComparer.Ordinal` for a user-built result; the internal factories keep passing the bridge comparer through a private ctor |

Parameter types equal the public accessor types (the M15/P1 rule: the accessor is the
contract). The `DeleteTopicsResult`/`DescribeTopicsResult` "invariant is structural
rather than checked" remarks are replaced (the checks are now live).

**Tests:** reflection per type — `IsSealed`, exactly one public ctor, parameter types and
names; both `ArgumentException` messages; a fabricated result round-trips
(`CreateTopicsResult` → `NumPartitions("t") == 3`, `Values["t"]` completes, `All()`
completes; `DeleteTopicsResult` by ids → `All()` faults with a supplied fault;
`DescribeTopicsResult` by names → `AllTopicNames()` returns the map, `AllTopicIds()` is
null).

### 3.9 G1-7 — `Uuid` and `NewTopic` value semantics (CP3, Mode A)

**`Uuid`** (`Uuid.cs`):
- `public static Uuid One` (Java `ONE_UUID`, `new Uuid(0, 1)`), `public static Uuid
  MetadataTopicId` (`METADATA_TOPIC_ID == ONE_UUID`), `public static
  IReadOnlyCollection<Uuid> Reserved` = `{Zero, One}` (the binding's standing
  `IReadOnlySet` substitute — netstandard2.0 has no `IReadOnlySet<T>`; same note as
  `ListTopicsResult.cs:86`).
- `public static Uuid RandomUuid()` per D4 (recommended: C#, Java's loop — regenerate
  while `Reserved.Contains(u) || u.ToString().StartsWith("-")`, `Uuid.java:76-83` — over
  a v4 random source matching `java.util.UUID.randomUUID()`'s version/variant bits;
  an `internal` overload taking the random source is the test seam, and the public
  method calls it with the real source so the two cannot diverge — DoD §12).
- `IComparable<Uuid>`: Java's `compareTo` exactly (`Uuid.java:175-190`) — **signed**
  `long` compare of `MostSignificantBits`, then `LeastSignificantBits`, returning
  exactly `1` / `-1` / `0`.

**`NewTopic`** (`Admin/NewTopic.cs`): `Equals(object?)` / `GetHashCode()` / `ToString()`
mirroring `NewTopic.java:149-174`: name (ordinal), `NumPartitions`, `ReplicationFactor`
(nullable equality — `-1` ≠ `null`, as `Optional.of(-1)` ≠ `Optional.empty()`),
`ReplicasAssignments` (content equality: same keys, per-key `SequenceEqual`),
`Configs` (content equality, ordinal). Hash is order-independent over both maps. `ToString`
is Java's exact format: `(name=t, numPartitions=3, replicationFactor=1,
replicasAssignments=null, configs=null)`, `default` for an unset count/factor, maps as
`{0=[1, 2]}` / `{k=v}`. `Configs` is settable, so the hash can change after mutation —
as in Java; document it.

**Tests:**
- `CompareTo` with high-bit values: `(long.MinValue,0) < (-1,0) < (0,0) < (1,0) <
  (long.MaxValue,0)`; LSB tie-break `(0,-1) < (0,0) < (0,1)`; exact return values;
  `CompareTo == 0 ⇔ Equals`; sorting a list.
- `One.ToString() == "AAAAAAAAAAAAAAAAAAAAAQ"`, `MetadataTopicId == One`, `Reserved`
  is exactly `{Zero, One}`.
- `RandomUuid`: 1000 iterations never reserved, never `'-'`-leading, version nibble 4,
  IETF variant; the seam fed `[Zero, One, (0xF800000000000000, 1), valid]` returns
  `valid` after consuming all four (0xF8… base64-encodes to a leading `-`).
- `NewTopic`: equal pairs for each ctor form, inequality per field, `-1` vs `null`,
  dictionaries with different insertion order equal and hash-equal, exact `ToString`
  strings (single-entry maps so the order is fixed).
- Reflection: `typeof(Uuid).GetInterfaces()` contains `IComparable<Uuid>`; the three new
  static members' types.

### 3.10 G3-4 — relax the `TopicPartition` ctor (CP4, Mode A)

**Call-site survey (every place a `TopicPartition` crosses into native code, HEAD).**
`NAC` = `Internal/NativeAdminClient.cs`, `NC` = `Internal/NativeConsumer.cs`.

*How a null topic is marshalled today:* every site pins the topic with
`Utf8Marshal.Pin` → `PinnedUtf8String` → `Encoding.UTF8.GetBytes(null)`, which throws
`ArgumentNullException` before the P/Invoke — and **every** site also has its own
explicit `Topic is null` guard that does not depend on the ctor. So `IntPtr.Zero` never
reaches the ABI as a topic, and no UB / panic is reachable today, including through
`default(TopicPartition)`.

| Surface | Native fn(s) | .NET null-topic guard | .NET negative-partition guard | ABI: NULL topic | ABI: negative partition | Java | Classification |
|---|---|---|---|---|---|---|---|
| Admin `deleteRecords` | `…_delete_records_async` | `NAC:1603` | **none** (ctor only) | silently **skipped** (`read_records_to_delete`, `src/ffi/admin.rs:1541-1566`) | passed to core | no client check (`KafkaAdminClient.java:3275-3287`) | null: **ABI-cannot-represent** (keep guard); negative: **pass through** |
| Admin `electLeaders` | `…_elect_leaders_async` | `DistinctPartitions` `NAC:6218` | none | skipped (`read_topic_partitions` `:7872`) | passed to broker | no client check (`:3886-3897`) | same |
| Admin `alterPartitionReassignments` | `…_alter_partition_reassignments_async` | `NAC:2812` | none | skipped (`read_reassignments` `:7944`) | **core fails that key**: `InvalidTopicError` "The given partition index {p} is not valid." (`kafka_admin_client.rs:3993-3998`) | per-key `InvalidTopicException` for null topic **and** negative partition (`:3936-3941`) | null: keep guard (D1); negative: **core validates** |
| Admin `listPartitionReassignments` | `…_list_partition_reassignments_async` | `DistinctPartitions` | none | skipped | **core fails the whole future**, same message (`kafka_admin_client.rs:4036-4052`) | whole future fails (`:4080-4089`) | null: keep guard (D1); negative: **core validates** |
| Admin `listOffsets` | `…_list_offsets_async` | `NAC:5326` | none | skipped (`read_offset_specs` `:8014`) | passed to core | no client check | null: keep; negative: pass |
| Admin `describeProducers` | `…_describe_producers_async` | `DistinctPartitions` | none | skipped | passed | no client check | same |
| Admin `abortTransaction` | `…_abort_transaction_async` | `NAC:5035` | none | **whole call** fails "abort transaction topic must not be null" | passed | — | same |
| Admin `alter`/`delete`/`listConsumerGroupOffsets` | `…_async` (3 fns) | `NAC:2997`, `:3153`, `:5978` | none | alter: whole call "topic at index {i} must not be null"; delete/list: skipped | passed | no client check | same |
| Admin mock seeds | `kafka_admin_MockAdminClient_update_{beginning,end,consumer_group}_offsets` | `SeedOffsets` `NAC:6817` | none | skipped | stored as-is | — | same |
| Consumer `assign`/`pause`/`resume`/`seekTo*`, `committed`, `beginning`/`endOffsets`, `commit*Offsets`, `offsetsForTimes`, `seek*`, `currentLag`, `position`, `ConsumerHandle_*`, `MockConsumer_rebalance` | `kafka_consumer_*` | `SnapshotPartitions` `NC:3875-3900`, `SnapshotCommitOffsets` `:3750-3790`, `SnapshotTimestamps` `:3960-3990`, `NC:592-620`, `:913-938`, `NativeConsumerHandle.ValidatePartition` `:362-374` | **yes** — the binding's own guards (`ArgumentOutOfRangeException`), independent of the ctor | **UB** (`CStr::from_ptr` unconditionally, `src/ffi/consumer.rs:1434-1449`; header "a valid C string") | passed to core, which answers like Java (assign accepts; pause/seek → IllegalState "No current assignment for partition …"; position → IllegalState) | assign rejects null/empty topic (`AsyncKafkaConsumer.java:1886-1890`) | null: **ABI-cannot-represent** (keep guard); negative: **unchanged** — the consumer guards stay (out of scope, §9) |
| Producer | — | — | — | no `TopicPartition` crosses the ABI (`ProducerRecord.Partition` is `int?`) | — | — | unaffected |
| gRPC servicer | `grpc-server/Translate.cs:202-203` `Tp()` | proto3 strings are never null | — | — | today throws inside the handler; after, reaches the binding | — | benefits |

The core's own `TopicPartition` holds its topic as `Arc<str>` (`src/common/topic_partition.rs:29`)
— there is **no null topic anywhere below the binding**. The sibling
`TopicPartitionReplica` (`TopicPartitionReplica.cs:64-68`) already does exactly what this
plan proposes: rejects a null topic (Java's `requireNonNull`, `TopicPartitionReplica.java:34`)
and accepts a negative partition.

**Change:**
- `TopicPartition.cs`: delete both throws; the ctor stores whatever it is given, as Java
  does (`TopicPartition.java:32-35`). Nullability annotation per D10 (rec: keep `string`).
  `ToString` per D13 (rec: `"null-5"`, Java's string concatenation). Rewrite the remarks
  (`:26-35`: `default` is no longer the only way to a null topic) and the `<exception>`
  docs (`:43-45`).
- **Every existing null-topic guard stays** (D1), with its existing exception type and
  message; only the comments that justify it "because of `default(TopicPartition)`"
  (`NAC:1601`, `:2810`, `:5324`, `:5976`, `:6815`; `Admin/IAdmin.cs:1396`) are widened to
  "a null topic (constructible, as in Java, or `default`) cannot cross the C ABI".
- **No admin negative-partition guard is added** — the value now reaches the core, which
  answers per key (APR) / whole future (LPR) with Java's message, or the broker answers.
- Result read-back: relaxing also removes a **latent throw** — `new TopicPartition(...)`
  is called on native data in trampolines (`AdminCallbacks.cs:601, 755, 3514, 3706`,
  `TopicPartitionListMarshal.cs:79`, `OffsetMapMarshalShared.cs:39`, `LogDirMarshal.cs:146`,
  `TransactionDescriptionMarshal.cs:87`). `AdminCallbacks.cs:755` echoes the input key, so
  relaxing only the guards (not the ctor) would throw inside the no-throw boundary.
- Stale docs that relied on the ctor throwing: `grpc-server/CallbackLog.cs:334-348` — keep
  the `Partition >= 0` guard (it fixes the cross-backend log shape), rewrite its
  justification (the "Java-parity design" claim is false: Java accepts −1 and its own
  producer failure path builds `new TopicPartition(topic, UNKNOWN_PARTITION)`,
  `KafkaProducer.java:1620`); `ffi-marshalling.md` ~333-336 and ~400 (the "ctor rejects
  −1 → throws inside the swallow boundary" rationale) per D12; `NC:3860-3870`
  (`SnapshotPartitions` doc). `design/current/STATUS.md:1034, 1055, 1247` are updated by
  the Manager at close.

**Tests:**
- `PublicTopicPartitionTests.NegativePartition_Throws` (`:42`) → **invert**:
  `new TopicPartition("t", -1)` → `Partition == -1`, `ToString() == "t--1"` (Java);
  `new TopicPartition(null!, 0)` → `Topic == null`, equals `default(TopicPartition)`,
  `GetHashCode` does not throw, `ToString()` per D13. Rewrite the class remark (`:20-38`,
  "a negative partition can never be smuggled into …").
- **New, admin, through the mock** (`MockAdminClient`): APR `{(t,-1), (t,0)}` for an
  existing topic → no synchronous throw; `Values[(t,-1)]` faults with the core mock's
  `UnknownTopicOrPartition` default message (`mock_admin_client.rs:1409-1431` — Java's
  `MockAdminClient` shape; the Actor copies the text from the core), `(t,0)` succeeds —
  proving the negative key round-trips the callback echo (`AdminCallbacks.cs:755`).
- **New, admin, real client with no broker** (the `PublicAdminTeardownTests` /
  `PublicAdminTfmSmokeTests` fixture pattern): LPR `{(t,-1)}` → whole task faults with
  `"The given partition index -1 is not valid."` (code 17) without any network; APR
  `{(t,-1)}` → that key faults with the same message. Both are client-side in the core
  (`kafka_admin_client.rs:3993-3998`, `:4046-4049`), so they need no broker. If the
  fixture cannot express it, these move to D9's harness follow-up and the plan's
  Docker note.
- **New, consumer**: now that the consumer negative-partition guards are reachable, one
  direct test per guard family (`SnapshotPartitions`, `SnapshotCommitOffsets`,
  `SnapshotTimestamps`, `Seek`/`SeekWithMetadata`/`CurrentLag`, `Position`,
  `NativeConsumerHandle.ValidatePartition`) asserting `ArgumentOutOfRangeException`,
  `ParamName` and message, and that no native call was made.
  `PublicConsumerCommitCallbackTests.CommitAsync_NegativePartition_ThrowsArgumentOutOfRangeBeforeAnyNativeCall`
  (`:334`) gains its `ParamName` / message assertion (it now passes through the guard, not
  the ctor).
- Existing null-topic guard tests stay green unchanged (they use `default(TopicPartition)`):
  `AdminP4SubmitArgumentTests.ElectLeaders_RejectsANullTopic_BeforeTheNativeCall`,
  `AdminP4Stage2SubmitArgumentTests.BothRpcs_RejectANullTopic_BeforeTheNativeCall`,
  `AdminP5SubmitArgumentTests.Offsets_BadArguments_AreRejectedBeforeAnythingIsSubmitted`,
  `PublicAdminListConsumerGroupOffsetsTests.ListConsumerGroupOffsets_RejectsAMalformedRequest`,
  `PublicAdminListPartitionsRecordsTests.Preconditions_AreRejectedBeforeAnyNativeCall`,
  `PublicAdminP8Tests.AbortTransaction_RejectsANullSpec`, and the consumer's null-topic
  tests. **Add** one variant per admin guard family using `new TopicPartition(null!, 0)`
  instead of `default` (the new way to reach it), asserting the same message.

---

## 4. Checkpoints

| CP | Findings | Why grouped | Blast radius |
|---|---|---|---|
| **CP1 — configs** | F1, G2-1, G2-4 | Same files and tests; F1 and G2-1 both change the IAC key → callback count | IAC, describeConfigs |
| **CP2 — ACLs / quotas / SCRAM** | G4-1, G4-2, G4-4 | One family, each contained to one marshaller / RPC | deleteAcls, describeUserScramCredentials, alterClientQuotas |
| **CP3 — topic value types & results** | G1-4, G1-6, G1-7 | Pure managed value types + public ctors; no ABI change | createTopics / createPartitions / delete/describeTopics / listOffsets inputs |
| **CP4 — `TopicPartition`** | G3-4 (+ G1-4's `TopicPartition` part) | Cross-cutting: Admin **and** consumer/producer call sites; last so its wider Critic review does not hold the others | every surface that marshals a `TopicPartition` |

---

## 5. Public API surface changes (each pinned by reflection)

| Finding | Change | Reflection test |
|---|---|---|
| G2-4 | `ConfigEntry` 8-arg ctor `internal` → `public` | exactly 2 public ctors, exact param types/names/order |
| G1-4 | `NewPartitions.IncreaseTo(int, IReadOnlyList<IReadOnlyList<int>>?)` — nullable annotation | `NullabilityInfoContext` reports the param nullable (net8.0+) |
| G1-4 | `NewTopic(string, int?, short?)` accepts −1 (contract/xmldoc, no signature change) | behavioural (exact messages) |
| G1-6 | `CreateTopicsResult`, `DeleteTopicsResult`, `DescribeTopicsResult` public ctors | `IsSealed`, one public ctor each, exact param types/names |
| G1-7 | `Uuid.One`, `Uuid.MetadataTopicId`, `Uuid.Reserved`, `Uuid.RandomUuid()`, `IComparable<Uuid>` | member types + interface list |
| G1-7 | `NewTopic.Equals/GetHashCode/ToString` overrides | `DeclaringType == typeof(NewTopic)` for the three |
| G2-1 | `ConfigResource` ctor normalizes (contract/xmldoc) | behavioural |
| G3-4 | `TopicPartition` ctor no longer throws; `ToString` of a null topic → `"null-{p}"` (D13); annotation per D10 | behavioural (§3.10) + a reflection check that the ctor's `topic` parameter / `Topic` property annotation is what D10 rules |
| G4-1 | none public (`FilterResult` ctor stays `internal`, as Java's is package-private) | — |

---

## 6. Gates

Per checkpoint:
1. `cargo build --features ffi` (Debug) first. Header byte-identical to HEAD's.
2. Mode A: `git diff 7e550b95..HEAD -- src/ cbindgen.toml generator/ bindings/python bindings/c` **empty** (D3 ruled: no Rust change of any kind in this phase).
3. `internal static extern` count **700 → 700** at every checkpoint (all needed ABI functions are already declared).
4. `~/.dotnet/dotnet build Confluent.Kafka.sln` (Debug): `0 Warning(s)`, `0 Error(s)`, six TFM outputs.
5. Filtered `dotnet test -f net10.0 --no-build` on the touched classes: explicit `Passed:` counts, `Test Run Aborted` = 0.
6. `dotnet format Confluent.Kafka.sln --verify-no-changes` (and `grpc-server` if touched).

Final (CP4), in addition:
7. Full `dotnet test -f net10.0` **and** `-f net8.0`, Debug, `Failed: 0`, `Test Run Aborted` = 0, totals reported (baseline **2345/2345** on both TFMs at `7e550b95`).
8. grpc-server builds; `dotnet format` clean.
9. **Docker**: `docker info` first. If up, stage a **fresh** linux/amd64 `.so` (prove freshness — P13.1 lesson), rebuild the .NET gRPC image, and run the existing admin `__grpc_dotnet` families this phase touches as a regression — `admin_cluster_configs_test` (8), `admin_acls_test` (6), `admin_quotas_test` (4), `admin_scram_test` (3), `admin_topics_test` (10), `admin_partitions_records_test` (8), `admin_elections_reassignments_offsets_test` (9) — 48 scenarios. CP4 changes no consumer/producer marshalling code (their guards stay), so the consumer/producer suites are optional regression only (`Translate.Tp()` is shared by every servicer). Report `N passed; 0 failed` per filter. No existing scenario exercises the new edge cases (grep), and adding scenarios is cross-binding harness work (D9). If Docker is down: **CI-pending**, flagged.
   > **Close-out correction (2026-09-30).** The per-family counts above are off by one in every family. `--list` shows 7 / 5 / 3 / 2 / 9 / 7 / 8, so the total is **41, not 48**, with the same arm count on `__rust`, `__grpc_python` and `__grpc_dotnet`. The run on `5cc92f66` passed 41/41.
   >
   > The optional regressions were also run, and passed. The other admin families' `__grpc_dotnet` arms passed 38/38. The non-admin sync `__grpc_dotnet` arms passed 34/34; the three `producer_transactions_test` arms were excluded, because they fail with a known `Unimplemented`.
   >
   > §3.1's "`PLAN-M15-admin-client.md` §15" is also wrong. That section lives in `design/history/M15/P3-cluster-configs-logdirs/PLAN.md`, and it was annotated there.
10. Critic 85 clean on CP4.

---

## 7. Decisions — **RULED 2026-09-29**

D1, D2 and D4–D13 approved exactly as recommended below. **D3 was changed by the user**
(see D3). The recommendations are kept verbatim as the record of what was ruled.

- **D1 — a null topic, which no C ABI entry can represent (risk 1(c)).** After relaxing,
  `new TopicPartition(null, p)` is constructible (as in Java), but the admin ABI silently
  **skips** a NULL-topic row (or fails the whole call, for `abortTransaction` /
  `alterConsumerGroupOffsets`), the consumer ABI dereferences it (**UB**), and the core's
  `TopicPartition` has no null form (`Arc<str>`). Java's answer differs per RPC: APR →
  per-key `InvalidTopicException` "The given topic name 'null' cannot be represented in a
  request."; LPR → the same on the whole future; consumer `assign` →
  `IllegalArgumentException` "Topic partitions to assign to cannot have null or empty
  topic"; most others have no client check. **Rec: (a) keep every existing binding-side
  null-topic guard, unchanged** (synchronous `ArgumentException` / `ArgumentNullException`,
  existing messages), recorded as an ABI-cannot-represent residual. This is today's
  behaviour for `default(TopicPartition)` and matches the `TopicPartitionReplica`
  precedent. Alternatives: (b) marshal null as `""` so the core answers — gives Java's
  exact text for consumer `assign` only, gives `''` instead of `'null'` for APR/LPR
  (`topic_name_is_unrepresentable` tests for empty, `kafka_admin_client.rs:1081`), sends a
  real `""` topic to the broker for the rest, and conflates null with empty; (c) Mode B:
  carry NULL through the ABI as a per-key invalid-topic error — the core type cannot hold
  it, so a large change; (d) complete the key locally with Java's exception — binding
  logic (`bindings/CLAUDE.md` §2.6), and exactly the local-completion pattern F1 removes.
  Python sends the literal topic `"None"` (`str(None)`), which is its own divergence (§9).
- **D2 — null `OffsetSpec` in `ListOffsets`.** Java silently queries LATEST
  (`getOffsetFromSpec` falls through, `KafkaAdminClient.java:5176-5192`); Python fails
  with `AttributeError`; .NET throws `ArgumentException` today.
  **Rec: (a) keep the rejection** as a documented precondition and assert its exact
  message. Java's LATEST is an `instanceof` fall-through on `null`, not a documented
  contract; the .NET `OffsetSpec` hierarchy is deliberately closed so that fall-through
  cannot be reached by a wrong kind (`SentinelFor` remark); and Python also rejects.
  (b) map `null` → LATEST (−1) — Java-faithful but enshrines an accident.
- **D3 — G4-1's Rust FFI-doc half.** The "complementary / exactly one non-null" docs at
  `src/ffi/admin.rs:15570-15575` and `:15722-15724` (and the `get_result_error` /
  `get_error` "or null when that entry carries a deleted binding instead" wording) are
  false against the getters' own bodies; the G4-4 async header's "an alteration with no
  entity types" is ambiguous (the code rejects only a NULL `entity_types[i]`).
  **RULED — CHANGED BY THE USER: out of scope for P13.2.** Neither item is fixed, bundled
  or sent anywhere; both are recorded here as **known header inaccuracies with no
  follow-up action assigned**. The §1.4 Critic briefing stays, so the correct .NET fix is
  not flagged against the wrong header comments. The .NET work never depended on D3.
  (The recommendation had been a docs-only note to the ABI owner; alternatives were a
  root `actor-executor` + `kafka-critic` docs-only step, or leaving it.)
- **D4 — `Uuid.RandomUuid()`.** No ABI entry exists. **Rec: implement in C#** — `Uuid` is
  a managed value type whose text form (`ToString`/`Parse`) is already C#, so a generator
  is scaffolding of the same kind, not Kafka behaviour (`bindings/CLAUDE.md` §2.6 is
  about Kafka logic). Random source: `RandomNumberGenerator` with Java's
  `UUID.randomUUID()` version-4 / IETF-variant bits, plus Java's reserved/`'-'` loop.
  Alternatives: (b) a Mode-B `kafka_common_Uuid_random_uuid(int64_t*, int64_t*)` over the
  core's `Uuid::random_uuid` (`src/common/uuid.rs:58`) — one ABI fn for a pure value
  helper, and Python has no `Uuid` type to share it; (c) omit and record.
- **D5 — G1-6 shape.** **Rec: stay `sealed`, add `public` ctors** (the audit's suggestion).
  Every .NET `*Result` is `sealed` (46/46) and `DeleteRecordsResult` /
  `ListOffsetsResult` already expose `public` ctors; none of the accessors is `virtual`,
  so unsealing with `protected` ctors (Java's literal shape) would let a user subclass
  but override nothing. Fabricating a result for a mock — the Java use case — only needs
  the ctor.
- **D6 — cadence** (§1.1). **Rec: P13.1's** — four checkpoints back to back, no user gate,
  Critic after each checkpoint until clean, normal prose conventions. **Ruled as
  recommended** (§1.1).
- **D7 — `NewTopic` values below −1.** The ABI folds every negative into "unset"
  (`src/ffi/admin.rs:1250-1255`): Java sends `-5` and the broker rejects it; the ABI
  would silently create the topic with broker defaults. **Rec: accept exactly −1**
  (Java's `NO_NUM_PARTITIONS` / `NO_REPLICATION_FACTOR`, wire-identical), **keep
  rejecting < −1** as an ABI-cannot-represent precondition. Alternative: accept all
  negatives (Python's behaviour — it inherits the silent reinterpretation; noted as a
  Python sibling) or a Mode-B ABI change to carry the raw value.
- **D8 — null `synonyms` in the published `ConfigEntry` ctor.** Java stores `null`
  (`synonyms()` then returns null); .NET's `Synonyms` is non-nullable. **Rec: keep
  `ArgumentNullException`** (documented stricter precondition). Alternative: normalize
  `null` → empty (changes `Equals` relative to Java's `Objects.equals(null, [])`).
- **D9 — Docker coverage of the new edge cases.** **Rec: no new multilanguage scenarios
  in P13.2** (they need the Rust harness + both servicers — a cross-binding item); run
  the existing touched families as regression (§6.9). Record F1 / G4-2 / G4-4 /
  G3-4-against-a-broker as a follow-up harness item.
- **D10 — nullability annotation on `TopicPartition`.** Relaxing makes a null topic
  constructible. (a) Annotate honestly: ctor `string? topic`, property `string? Topic` —
  with `<Nullable>enable</Nullable>` + `TreatWarningsAsErrors` (`Directory.Build.props:11,17`)
  every unguarded `.Topic` use in `src/`, `grpc-server/`, `tests/`, `soak/` becomes a
  build error to fix, and every user with nullable enabled gets warnings for a value the
  client never *returns* null. (b) **Rec: keep `string`** on both; the ctor accepts null at
  runtime without throwing (as `default(TopicPartition)` already yields a null `Topic`
  behind the same annotation), documented in the ctor xmldoc. Rationale: a null topic
  cannot cross the ABI (D1), so every value the binding hands back has a non-null topic;
  the annotation describes that, while the ctor stays Java-permissive.
- **D11 — consumer negative-partition guards.** They stay (out of scope: stricter than
  Java for `assign`, where Java accepts a negative partition), become reachable once the
  ctor stops throwing, and get direct tests (§3.10). **Rec: keep; record as a consumer
  parity sibling (§9).**
- **D12 — `ffi-marshalling.md` edit.** Lines ~333-336 and ~400 justify the producer
  failure-placeholder rule by "`TopicPartition`'s ctor rejects a negative partition". The
  rule (reuse the placeholder machinery) stays right; its rationale becomes false.
  **Rec: authorize a direct edit** of those lines in CP4 (the P13.1 D5 precedent);
  otherwise STATUS records it as stale.
- **D13 — `TopicPartition.ToString()` with a null topic.** Java prints `"null-5"`
  (`topic + "-" + partition`); .NET prints `"-5"` today (for `default`). **Rec: match
  Java** (`"null-5"`), since the xmldoc promises "the Java-style `topic-partition`
  string" and a null topic is now reachable through the ctor.

---

## 8. Risks

- **R1 F1 callback count.** The count moves from `keys − zeroOps` to `keys`; a leftover
  subtraction is an equivalent mutant unless a test leaves one key pending (§3.1 new test).
- **R2 F1 × G2-1.** Before G2-1, `(64,"x")` + `(Unknown,"x")` arm 2 but the ABI fires 1
  (hang). Both land in CP1; the G2-1 collision test runs on the F1 path.
- **R3 G4-4 empty-array pointer.** Relying on `AddrOfPinnedObject` of an empty array would
  work today and break silently on a runtime change; the pointer assertion pins the
  placeholder.
- **R4 header vs code (G4-1, G4-4).** The Critic's ground truth is the header, which is
  wrong/ambiguous at exactly the two points this phase depends on (§1.4, D3).
- **R5 G3-4 cross-cutting.** The survey (§3.10) found the ctor is the *only* barrier
  against negative partitions on all 10 admin RPCs and the 3 mock seeds — relaxing sends
  them to the core at once. That is Java-faithful, but a site whose ABI mis-handles a
  negative value would now be reachable; none was found (all "passed to core/broker").
  Null topics stay behind the per-site guards (D1) — a new site added later without a
  guard would hit a silent skip (admin) or UB (consumer), so the Critic checks each guard
  still precedes `Utf8Marshal.Pin`.
- **R5b annotation ripple.** If D10 = (a), CP4 grows by every unguarded `.Topic` use under
  `TreatWarningsAsErrors`; budget CP4 accordingly or split it.
- **R6 Broker-only outcomes.** F1 (absent/unauthorized/`ValidateOnly`), G4-2
  (`DUPLICATE_RESOURCE`), G4-4 (`INVALID_REQUEST` "Invalid empty client quota entity",
  `ClientQuotaControlManager.java:313`) are unreachable in-process (the core mock
  implements none of them) — covered by row-shape + real-ABI tests only (D9).
- **R7 Messages** are core-owned; any paraphrase in a test is a DoD §3 defect.
- **R8 Docker** may be down → final gate closes CI-pending.

---

## 9. Out of scope (sibling observations, not added)

- **G4-3** (null SCRAM user element) — explicitly out; G4-2 keeps today's null handling.
- **N3** (`AlterClientQuotasResult`, `AlterUserScramCredentialsResult`,
  `DescribeClientQuotasResult` public ctors) — the obvious sibling of G1-6.
- **N5** (`DeleteAclsResult.FilterResult.Error` → `Exception` naming) — same type as G4-1.
- **`Uuid.toArray` / `toList`** (`Uuid.java:192-208`) — Java static helpers G1-7 does not list.
- **`ListConfigResources` with an undefined `ConfigResourceType`** (`TranslateAdmin.cs:416`)
  — the G2-1 sibling on the type-filter input (not a `ConfigResource`).
- **Python `NewTopic` negatives** — Python passes `-5` through and inherits the ABI's
  silent "unset" reinterpretation (D7).
- **Python null topic** — admin RPCs coerce `(str(topic), int(partition))`, so a `None`
  topic is sent as the real topic `"None"` (`admin.py:2955` and siblings) (D1).
- **Consumer negative-partition guards** (`NC:3893`, `:3773`, `:3980`, `:592-620`,
  `:913-938`, `NativeConsumerHandle.cs:362-374`) are stricter than Java for `assign`
  (D11).
- **`TopicPartitionReplica`** already matches Java (null topic rejected, negative
  partition accepted) — no change.
- **Rust FFI test `admin.rs` ~25220** ("The two are complementary") pins the false G4-1
  claim for a constructed case — a known inaccuracy, out of scope with no follow-up (D3).
- Any Rust change; the gRPC servicers' behaviour; the audit baseline and
  `PendingAdminClientFindingsForDotnet.md` (user-owned).
