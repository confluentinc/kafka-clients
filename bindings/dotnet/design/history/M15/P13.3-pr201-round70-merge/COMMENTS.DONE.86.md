# Critic 86 — resolved findings (M15/P13.3)

Moved from `COMMENTS.86.md` by Actor 86 after each fixup was committed and verified.

---

### 86.1 — minor — Three comments still call the header's deleteAcls `get_binding` text "a known inaccuracy" and quote wording the merged header no longer has — and CP6 now cites the symbol for that quote.

**Where:**
- `src/Confluent.Kafka/Internal/Interop/NativeMethods.Admin.cs:3404-3407` (`DeleteAclsResultGetBinding` doc)
- `src/Confluent.Kafka/Internal/Interop/NativeMethods.Admin.cs:3421-3424` (`DeleteAclsResultGetResultError` doc)
- `src/Confluent.Kafka/Internal/Interop/DeleteAclsResultMarshal.cs:143-145` (not in the CP6 commit message's list)

```csharp
/// … The header's "null when that
/// entry carries an exception instead" and "complementary" wording
/// (header, <c>kafka_admin_DeleteAclsResult_get_binding</c>) is a known inaccuracy the reader
/// does not rely on (PLAN D3) …
```

**Evidence:**
- **Header** (merged), `kafka_admin_DeleteAclsResult_get_binding`: "As in Java's `FilterResult`,
  the binding and `kafka_admin_DeleteAclsResult_get_result_error` are **not** exclusive: Java
  builds each entry with the matched binding and, when deleting it failed, the exception too, so
  a failed entry has both." Neither "complementary" nor "carries an exception instead" occurs
  anywhere in the merged header.
- **Plan §1.6** (and §4.15, row `6d86635d`): "the P13.2 'known inaccuracy' G4-1 … is **fixed**
  by `6d86635d`: the header now says a failed deletion carries both."
- CP6 replaced the old line cite with `(header, <c>kafka_admin_DeleteAclsResult_get_binding</c>)`,
  so the sentence now names the one symbol whose doc says the opposite of what it is quoted as
  saying.

**Why it is wrong:** the code is right (it reads both handles independently, as the header now
documents), but the three comments state the ground truth backwards. A reviewer who follows the
new symbol cite finds the header agreeing with the reader, and a reader who trusts the comment
believes the header is still wrong — the exact G4-1 confusion P13.2 recorded and this merge
closed. `DeleteAclsResultMarshal.cs:143-145` makes the same claim ("The header's `get_binding`
docs call the two 'complementary'; that sentence is a known inaccuracy") and was missed by the
CP6 sweep.

**Fix:** in all three places, drop the "known inaccuracy" / "whatever the header's `get_binding`
text says" wording and state that the header agrees — e.g. "(header,
`kafka_admin_DeleteAclsResult_get_binding`: the binding and the result error are not exclusive,
as in Java's `FilterResult`)". Keep the Java cite (`KafkaAdminClient.java:2705-2708`) and the
G4-1 reference. Comment-only; no test change.

**Resolved:** `413dfb10` — all three comments now say the header agrees (`kafka_admin_DeleteAclsResult_get_binding`: the binding and the result error are not exclusive, as in Java's `FilterResult`) and that .NET reads the two independently. The Java cite (`KafkaAdminClient.java:2705-2708`) and the G4-1 reference are kept; the P13.2 `PLAN D3` cite is dropped, since that ruling recorded the header text as wrong. A sweep of the binding's `.cs` files and non-rule docs outside `design/history/` and `design/current/` finds no other copy. `design/current/STATUS.md` still carries the old D3 sentence; it is PM-owned and was left alone.

---

### 86.2 — nit — The `alterClientQuotas` submit-seam doc says four of its seven arrays are arrays-of-arrays; the header has five.

**Where:** `src/Confluent.Kafka/Internal/NativeAdminClient.cs:680` (the `NativeAlterClientQuotasSubmit` remarks)

```csharp
/// Four of the seven arrays are arrays-of-arrays: row <c>i</c>'s entity is
/// <c>entityCounts[i]</c> <c>(type, name)</c> pairs and its ops are <c>opCounts[i]</c>
/// triples, …
/// (header, <c>kafka_admin_AdminClient_alter_client_quotas</c>).
```

**Evidence:** header, `kafka_admin_AdminClient_alter_client_quotas_async` (and the sync twin):
the seven arrays are `entity_types`, `entity_names`, `entity_counts`, `op_keys`, `op_values`,
`op_has_values`, `op_counts`; five of them are `* const *` arrays-of-arrays (`entity_types`,
`entity_names`, `op_keys`, `op_values`, `op_has_values`), and the same doc goes on to call the
ops "triples" (three inner arrays) beside the entity "pairs" (two).

**Why it is wrong:** pre-existing (older than the merge, as the Actor noted), but CP6 re-cited
this sentence to the header symbol, so it now reads as header-verified. The marshalling itself is
correct; only the count is wrong.

**Fix:** "Five of the seven arrays are arrays-of-arrays". Comment-only.

**Resolved:** `cfb71be6` — now "Five of the seven arrays", checked against both header declarations (`kafka_admin_AdminClient_alter_client_quotas_async` and its sync twin) and the P/Invoke `AdminClientAlterClientQuotasAsync` (five `IntPtr[]`, two `int[]`). Two older copies of the same miscount in `Internal/Interop/ClientQuotaMarshal.cs` (the `PinAlterations` and `AlterationRows` summaries, not named in the finding) were corrected in the same commit.

---

## Review record (Manager, at phase close — 2026-09-30)

Per the user's ruling (PLAN §1.1, §8 D1 amendment), Actor 86 ran CP1→CP6 back to back, the
Manager verified each checkpoint's gates, and Critic 86 reviewed CP0–CP6 **once**, at HEAD
`05c19635`. The review fit in one Critic session; no continuation session was needed.

| Scope | Commit(s) | Critic 86 | Findings |
|---|---|---|---|
| CP0 — merge of PR #201 head `3b27d2c9` | `16d2b4c5` | review: no findings | none (one conflict, `actor-executor/MEMORY.md`, both lines kept; the tree matches `git merge-tree` apart from that file) |
| Plan (approved, with the ruling) | `19e408d0` | review: no findings | none |
| CP1 — F5, F10 | `378859e1` | review: no findings | none |
| CP2 — F6, F4 audit | `e2422f0e` | review: no findings | none |
| CP3 — F8, F7 | `661ce15e` | review: no findings | none |
| CP4 — (c), §4.8, D15, D16 | `7e8d6bdf` | review: no findings | none |
| CP5 — D11, D12, D13 | `eee5057a` | review: no findings | none |
| CP6 — D17 | `05c19635` + `fixup!` `413dfb10`, `cfb71be6` | review: 86.1 (minor), 86.2 (nit); re-review: CLEAN | 86.1 fixed in `413dfb10`, 86.2 in `cfb71be6`. Both are comment-only. |

Both `fixup!` commits target CP6's feature commit. Neither finding touched a ruled decision
(D1–D18). Both were stale claims that CP6's cite refresh made look header-verified.

Final gates on `cfb71be6`:

- **Mode A.** `git diff 16d2b4c5..cfb71be6 -- src/ cbindgen.toml generator/ build.rs Cargo.toml Cargo.lock tests/ bindings/python bindings/c`
  is empty. The only Rust in the phase is the merge, which brings PR #201's code in unchanged.
  The header SHA-1 is `41f48ea8…`. `internal static extern` went from 700 to 697 (CP5: +1 `Error_cause`, +1 `Node_is_fenced`, +1 `TopicMetadataAndConfig_config`,
  −6 flat `TopicMetadataAndConfig_config_*`).
- **Tests.** net10.0 and net8.0 both pass 2630/2630. Before the merge it was 2434/2434 (+196).
- **Local Docker gate, run on `cfb71be6` (2026-09-30, 13:31–13:41 IST).**
  - A fresh linux/amd64 `.so` exports `kafka_common_Error_cause`. Before the merge it did not.
  - All 697 of the binding's entry points resolve in it.
  - The sync .NET and sync Python gRPC images were rebuilt from it, and both carry the same `.so`
    (sha256 `9b35e4bc…`).
  - Touched families, all three arms: 129 passed; 0 failed. That includes the 4
    `list_transactions_*` `__grpc_dotnet` scenarios, F5's only per-broker proof.
  - Other admin `__grpc_dotnet` arms: 36 passed; 0 failed.
  - Non-admin sync `__grpc_dotnet` arms: 34 passed; 0 failed.

## PM items — header/core text, no Actor action

Merged-header text that disagrees with other merged-header text (plan §9 R10). No Rust change
is allowed in this phase; these are for the PM to raise with the user. The .NET side already
follows the correct reading in both cases.

### PM-1 — `kafka_admin_RemoveMembersFromConsumerGroupResult_all` still says the C API cannot read a cause

- **Header, `kafka_admin_RemoveMembersFromConsumerGroupResult_all`:** "The member's own error
  (for example `UNKNOWN_MEMBER_ID`) is the cause of the returned error, as in Java — but **this C
  API cannot read an error's cause** … so from C (and from the Python and gRPC layers built on
  it) a `removeAll` partial failure surfaces as a bare Kafka error with code -1 … and the member's
  own error code is not observable".
- **Header, `kafka_common_Error_cause`** (new in round 70): "Returns the error that caused this
  one … for example … the `removeMembersFromConsumerGroup` remove-all wrap".
- The first doc predates `kafka_common_Error_cause` and was not updated with it. .NET reads the
  cause (CP5 D11) and says so in its own P/Invoke remark. (Found by the CP5 Actor; confirmed.)

### PM-2 — `kafka_admin_AdminClient_alter_user_scram_credentials` still says `alter_client_quotas` rejects a duplicate entity

- **Header, `kafka_admin_AdminClient_alter_user_scram_credentials`, "# Duplicate users":** "This
  differs deliberately from `kafka_admin_AdminClient_alter_client_quotas`, which *rejects* a
  duplicate entity: a quota entity is a compound key … so a C caller cannot re-derive which of its
  parallel rows the surviving outcome describes".
- **Header, `kafka_admin_AdminClient_alter_client_quotas`:** "A repeated entity **across**
  alterations is sent as Java sends it — both alterations reach the broker, in order — and … the
  result holds one entry for that entity"; and **`_async`:** "A repeated entity across
  alterations is sent twice, as in Java, and is one key".
- The SCRAM doc's contrast is the pre-round-70 behaviour. .NET follows the quota docs (CP3 F7).
  (Found by the CP6 Actor; confirmed.)
