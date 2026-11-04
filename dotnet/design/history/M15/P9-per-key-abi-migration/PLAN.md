# M15/P9 — .NET Admin: migrate to the per-key independent-callback ABI (PR #201)

**Status:** APPROVED by Pranav. Actor 79 may start CP1. No code touched yet.
**Agent number:** N=79 (Actor + Critic).
**Note:** M15/P9 was already reserved in the roadmap for "tracked gaps, no RPC work" —
this phase absorbs that scope (CP9 = `LogDirDescription.IsCordoned`, already on that
list) plus the PR #201 ABI-break migration, which the roadmap predates.
**Branch:** `prashah_dev_dotnet_binding` (has local merge `2926f6d4` of `pr-201`; not pushed).

---

## 1 · What changed, and what it breaks

PR #201 converted **28 of 49** `kafka_admin_AdminClient_*_async` entry points from
"one aggregate callback carrying a flattened `*Result_t` table" to **N independent
callbacks, one per key**, fired as that key's own future resolves — possibly
concurrently, on different threads, in any order (`admin_async_per_key_op`,
`src/ffi/admin.rs:927`). The new callback shape carries **no result root**:
`(key-field(s)…, value?, error, user_data)`.

The other 21 are untouched (Java holds one `KafkaFuture<Map/Collection>`), including
**`remove_members_from_consumer_group`** and **`list_transactions`** — do not touch.

All 28 have live .NET call sites today. Nothing is greenfield.

### 1.1 Corrections to the brief (verified against the tree — these change the plan)

1. **`alter_consumer_group_offsets` and `delete_consumer_group_offsets` ARE already
   implemented in .NET** — the brief says they are not. They route through
   `AdminCallbacks.CompleteAggregateRpc` (shape 3, `AdminCallbacks.cs:2980`, `:3004`),
   because Java's `AlterConsumerGroupOffsetsResult` / `DeleteConsumerGroupOffsetsResult`
   hold **one** `KafkaFuture<Map<TopicPartition, Errors>>`. So they are **not**
   zero-risk net-new work; they are the **highest-risk** item in the phase, because the
   ABI now fans out per key while Java's shape stays a single aggregate future. That
   needs a **new** shape (per-key fan-in → one aggregate `Task`) which no existing
   walker provides. Moved to its own checkpoint (CP7).

2. **The duplicate-key gap is already largely closed.** `NativeAdminClient` de-duplicates
   the key axis at every keyed submit site before the P/Invoke — `DistinctPartitions`,
   `DistinctBindings`, `DistinctFilters`, `DistinctConfigResources` and inline
   `HashSet` guards (`NativeAdminClient.cs:1028`, `:2197`, `:2433`, `:3693`, `:5986`,
   `:6016`, `:6319`, `:6347`, …), each preserving request order and citing Java's
   `Map`/`Set` parameter. So Rust's positional `claimed` bookkeeping is a safety net
   here, not a contract .NET relies on. It still needs a **per-RPC confirmation sweep**
   (CP8), but it is not expected to force occurrence-aware tracking in
   `KeyedAdminOperation`.

3. **The callback count is NOT uniformly the key count or the array length**, and this
   is the single most dangerous detail in the migration. `incremental_alter_configs`
   passes **one row per operation** but Rust fires **once per distinct resource**
   (`distinct_config_resources`, `admin.rs:6199`; header text at `:6160`). Meanwhile
   `create_topics` fires "exactly `count` times … **minus NULL entries**"
   (`admin.rs:3776`). The countdown in §2.1 must therefore be derived **per RPC from
   that RPC's own ABI doc comment**, never from a rule of thumb.

4. **Three per-key values are still index-addressed `*Result_t` tables**, delivered
   one-entry-per-callback: `describe_producers` → `DescribeProducersResult_t`,
   `describe_transactions` → `DescribeTransactionsResult_t`, `fence_producers` →
   `FenceProducersResult_t`. Their existing `Func<IntPtr,int,TValue>` readers and
   existing `*Result_destroy` P/Invokes are reusable nearly verbatim, reading index 0.
   The other 25 get a flat per-key value handle (or none).

5. **All 11 new destroy fns + `LogDirDescription_is_cordoned` exist in Rust and are
   absent from `NativeMethods.Admin.cs`** (verified by grep). Confirms scope.

6. ⚠ **FALSIFIED DURING CP1 — "old paths still live" is FALSE at HEAD.** The plan
   assumed the 28 existing call sites keep working until migrated, so every
   checkpoint could gate on a green `dotnet test`. They do **not** work: the merged
   `pr-201` native invokes .NET's **3-argument** aggregate callbacks with the new
   **4-argument** per-key signature (verified: `AdminCallbacks.cs:98`
   `CreateTopicsCallback(IntPtr result, IntPtr error, IntPtr userData)` against
   `admin.rs:3764` `create_topics_callback_t = fn(*const c_char, *mut
   TopicMetadataAndConfig_t, *mut Error_t, *mut c_void)`). The trampoline reads the
   key `char*` as the result root, calls `*Result_count` on it, and the core
   **aborts** on a non-unwinding panic in the callback dispatcher — taking the test
   host down while `dotnet test` still **exits 0** (environment trap #6; `Test Run
   Aborted` was the only signal on every run).

   Measured at CP1: of 86 admin test classes, 79 measured individually, **26 crash**;
   the filtered run excluding those 26 still aborted, so **≥27**. Proven pre-existing
   by stashing all CP1 changes, rebuilding clean (`0 Error(s)`), and reproducing
   `PublicAdminCreateTopicsTests` identically.

   **Consequence: a green full `dotnet test` is not an achievable checkpoint gate
   until CP2–CP7 have migrated all 28 RPCs.** Each checkpoint turns some classes
   green. §3's gate is re-baselined accordingly (see §3's gate note) — **pending
   Pranav's approval**, since it changes the plan's DoD.

### 1.2 The .NET breakage, by site

| Site | What breaks |
|---|---|
| `KeyedResultMarshal.Complete<TKey,TValue>` and `Complete<TKey>` | Both walk a `*Result_t` root that the new callbacks never pass. Dead for all 28. The shape-3/3b/3c entries (`CompleteAggregate`, `CompleteList`, `CompleteTwoLists`) stay — 21 RPCs still use them. |
| `AdminCallbacks.CompleteKeyed<TKey,TValue>` (15 callers) / `CompleteKeyedVoid<TKey>` (11 callers) | The two shared trampoline bodies every affected RPC delegates to. |
| `AdminOperation.FreeGcHandle()` | A **one-shot** `Interlocked.Exchange` flag freeing the rooting `GCHandle` *and* releasing the span-the-op `SafeAdminHandle` ref in the single callback that is assumed to happen. With N concurrent callbacks this must become a **countdown released by the last of N**. Freeing early = use-after-free of the `GCHandle` target, plus a racing `Dispose()` destroying the client under in-flight callbacks (`AdminClient_destroy` is not ref-counted and does not drain). |
| `KeyedAdminOperation<TKey,TValue>` | The "one TCS per key, created before the P/Invoke" half is **correct and unchanged** — the new ABI assumes exactly that. Only completion-delivery changes: one walk resolving N → N independent partial resolutions. `FailUncompleted()` and `CompleteKeysWithNoRequest()` must move to countdown-zero. |
| Per-key `error` | Ownership **flips borrowed → owned**: `KafkaException.FromBorrowedHandle` → `FromHandle`. There is no shared root to die with it. |
| Per-key `value` | Independently owned → destroy per callback in a `finally` (11 new destroys + the 3 reused `*Result_destroy`). |
| Owned struct-pointer keys | New: `create_acls`/`delete_acls` key on `AclBinding_t`/`AclBindingFilter_t`, `alter_client_quotas` on `ClientQuotaEntity_t`. The callback must destroy the **key** via its existing `kafka_common_*_destroy`. |
| `AdminOperation.cs` `KeyedAdminOperation` remarks | Factually wrong now ("per-key timing independence is not preserved… nothing observable depends on this… Python sibling has the identical limitation"). Correct **tersely**, per §5. |

---

## 2 · Design

### 2.0 New shape family: **Shape 4 — independent per-key callbacks**

Three sub-shapes, discriminated (as always in this binding) **by the Java return
type, not by the ABI accessor set**:

- **4a — value-carrying, one `Task` per key.** Java: `Map<K, KafkaFuture<V>>`, `V` non-`Void`.
  Callback `(key…, value, error, ud)`. Resolves exactly that key's TCS.
  *(the 15 RPCs on shape 1 today)*
- **4b — void, one `Task` per key.** Java: `Map<K, KafkaFuture<Void>>`. Callback
  `(key…, error, ud)`; a null error **is** the success value. *(the 11 on shape 2 today)*
- **4c — per-key fan-in, ONE aggregate `Task`.** Java: **one**
  `KafkaFuture<Map<K, Errors>>`. Callback `(topic, partition, error, ud)` where the
  error is the map **VALUE**, not a fault (the `ElectLeaders` lesson, measured at 10
  failing tests in M15/P4 — see `KeyedResultMarshal` remarks). Accumulate; complete the
  single awaiter when the last key arrives. *(the 2 group-offsets RPCs)*

### 2.1 `AdminOperation`: countdown, not a flag

```
SetPendingCallbacks(int n)   // BEFORE the P/Invoke, with everything else the callback needs
ReleaseOne()                 // Interlocked.Decrement; at 0 → the latched free
AbandonBeforeSubmit()        // the latched free directly (native never ran)
```

- Keep **one** `int` release latch guarding the actual free (`GCHandle.Free` +
  `DangerousRelease`) so the countdown path and `AbandonBeforeSubmit` cannot double-free.
  The countdown is separate from the latch.
- **`n` is the number of callbacks native will make** — derived per RPC from its ABI doc
  comment (§1.1 item 3). Not `_perKey.Count`, not blindly the array length.
- **`n == 0` is a new leak class the countdown introduces.** An empty key collection
  means zero callbacks, so the `GCHandle` is never freed and `AdminClient_destroy` is
  deferred forever. Handle it explicitly: short-circuit before allocating the `GCHandle`
  and return an already-resolved result, or free immediately after the submit returns.
  Must be decided once, in one place, and covered by a test per affected RPC.
- `FailUncompleted()` / `CompleteKeysWithNoRequest()` run **at countdown zero only**.
  Running them per callback would fault keys whose callbacks simply have not arrived yet.
- Row-flattened RPCs (`incremental_alter_configs`) have keys with **zero** rows and
  therefore zero callbacks — excluded from `n`, resolved locally
  (`SetKeysWithNoRequest` already models this; only its firing point moves).

### 2.2 Thread-safety obligations (new)

Callbacks for one operation can run **concurrently on different threads**. Therefore:

- Every mutation of shared per-operation state must be thread-safe. `TaskCompletionSource.TrySet*`
  already is. The 4c accumulator is **not** by default — it needs a lock or interlocked
  completion counter.
- The no-throw boundary must be **per callback**, so one key's marshalling failure cannot
  strand the other N−1 (the existing per-key `catch` in `KeyedResultMarshal.Complete`
  already has the right instinct; it must survive the rewrite).
- `RunContinuationsAsynchronously` stays mandatory and gets sharper: N awaiters can now be
  released from N different core threads.

---

## 3 · Checkpoints

Commit-sized and resumable mid-phase. RPC coverage is enumerated so the full 28 is
visible before approval.

⚠ **Gate (re-baselined after CP1's finding — §1.1 item 6). DECIDED: approved by
Pranav.** The original gate was "`dotnet build` + `dotnet test` green". That is
unachievable until all 28 RPCs are migrated, because the 28 unmigrated call sites
**abort the test host** at HEAD. The per-checkpoint gate for **CP2–CP6** is:

  1. `0 Error(s)` on all TFMs, Debug **and** Release (net462 is build-verified only —
     no `mono` on this machine);
  2. the checkpoint's **own** new tests pass, with the **count** asserted (trap #5);
  3. **no new crashing test class** versus the recorded baseline, checked by grepping
     `Test Run Aborted` explicitly (trap #6);
  4. `dotnet format --verify-no-changes` clean.

⚠ **Crash-count metric definition — pin this, do not re-derive it per checkpoint.**
Measure with **one `dotnet test --filter` invocation per admin test class**, and
compare **abort sets**, not just totals. A single grouped invocation is NOT
equivalent: an aborting class masks every class after it in the same run, which
silently *under*-counts. CP2 reported 22 from a coarser grouping; re-measured by the
per-class method it was **25**. Authoritative baselines by the pinned method:
**CP2 = 25, CP3 = 20, CP4 = 21, CP5 = 20.** Any checkpoint reporting a delta must
state it against the per-class method.

**Diff the abort SET, not the total.** CP5 is the worked example: total unchanged at
20, but the set moved −1 (`PublicAdminDeleteAclsTests`, CP5's own RPC, now 10/10)
/ +1 (`PublicAdminAlterConsumerGroupOffsetsTests`). That +1 is **pre-existing**, and
was settled **in the source, not by re-measuring** — an arity-mismatch crash is
nondeterministic, so a re-run proves nothing. Proof: native
`alter_consumer_group_offsets_callback_t` (`admin.rs:13125`) is 4-arg;
`AdminCallbacks.AlterConsumerGroupOffsetsCallback` is 3-arg and byte-identical at CP3;
neither CP4's nor CP5's diff mentions it. It is CP7's own RPC and clears there.
**Prefer source-level proof over re-measurement for any crash-set delta.**

The full-suite-green gate returns at **CP7**, once the last of the 28 is migrated.

| CP | Scope | RPCs (count) |
|---|---|---|
| **CP1** | **Shared infra, purely additive** — `AdminOperation` countdown + release latch + `n==0` rule (§2.1); per-key resolution entry points on `KeyedAdminOperation` / `VoidKeyedAdminOperation`; new per-key helpers in `KeyedResultMarshal` (shape-3/3b/3c entries untouched); the 4c fan-in operation type; the 11 new destroy `[DllImport]`s + `DeleteAclsFilterResults_t`. Old paths still live → green. | 0 |
| **CP2** | **Pilot, end to end** — one 4a and one 4b, both plain string keys. Proves the shape + the test pattern before it is replicated 26×. | `create_topics`, `delete_topics_by_name` (2) |
| **CP3** | 4a, scalar/string keys | `describe_topics_by_name`, `describe_topics_by_id`, `describe_configs` (`(i32 type, name)`), `describe_log_dirs` (`i32` broker), `describe_consumer_groups`, `describe_classic_groups`, `list_consumer_group_offsets`, `describe_transactions`, `fence_producers` (9) |
| **CP4** | 4a, composite keys decomposed to scalars | `delete_records` (`topic,partition`), `list_offsets` (`topic,partition`), `describe_producers` (`topic,partition`), `describe_replica_log_dirs` (`topic,partition,broker`) (4) |
| **CP5** | 4a, **owned struct-pointer key** + new value type | `delete_acls` (`AclBindingFilter_t` key → destroy in `finally`; `DeleteAclsFilterResults_t` value) (1) |
| **CP6** | 4b, remainder — incl. the two remaining owned struct-pointer keys and the row-flattened case | `delete_topics_by_id`, `create_partitions`, `incremental_alter_configs` (**distinct-resource count**, `SetKeysWithNoRequest`), `alter_replica_log_dirs`, `delete_consumer_groups`, `alter_user_scram_credentials`, `update_features`, `alter_partition_reassignments`, `create_acls` (`AclBinding_t` key), `alter_client_quotas` (`ClientQuotaEntity_t` key) (10) |
| **CP7** | **4c fan-in → one aggregate `Task`** (§2.0). Per-key error is a map **value**, not a fault. Needs the thread-safe accumulator. `remove_members_from_consumer_group` is the 29th RPC folded in by decision (§3.0.2) and carries the mode-dependent `n` + dual-mode contract of §3.0.3. | `alter_consumer_group_offsets`, `delete_consumer_group_offsets`, `remove_members_from_consumer_group` (3) |
| **CP8** | **Sweep + cleanup.** (a) Confirm per-RPC, for all 28, that the callback count matches the countdown and that the key axis is de-duplicated (§1.1 items 2–3). (b) Delete `KeyedResultMarshal.Complete` (both overloads) and `AdminCallbacks.CompleteKeyed`/`CompleteKeyedVoid` if now unreferenced; remove the 28 dead delegate typedefs. (c) Terse correction of the stale `KeyedAdminOperation` remarks. (d) One-bullet shape-4 entry, edited directly into `ffi-marshalling.md` (Pranav authorized the direct edit here). (e) Re-arm the per-key value-destroy guard: under shape 4 a wrong/missing destroy **leaks** instead of aborting, so the old injection guards are toothless (§3.0). Needs a mechanism a managed test can actually observe — e.g. destroy-call counting via the existing marshal seam, as CP1's 4a test already does. | 0 |
| **CP9** | **`LogDirDescription.IsCordoned`** — decided IN (§3.2). Additive, unrelated to the ABI break: add `[DllImport]` for `kafka_admin_LogDirDescription_is_cordoned`, wire the property, one test. Runs last so any regression up to CP8 is unambiguously attributable to the ABI migration, not this. | 0 (net-new property) |

**Total: 28 RPCs** (2 + 9 + 4 + 1 + 10 + 2) **+ 1 net-new property (CP9).**

### 3.0 Findings resolved during execution (no further decision needed)

- **CP3 — `describeTransactions` / `fenceProducers` per-key value readers are
  untestable from managed code. RESOLVED: accept as-is; nothing to fix.** Their
  value is read at index 0 of the reused `*Result_t`, but the Rust mock faults every
  transactional id with `"Not implemented yet"`, so no value is reachable from a
  managed test — failure paths are covered, value readers are not. **This is
  Java-faithful, not a gap to close:** Java's own `MockAdminClient` throws
  `UnsupportedOperationException("Not implemented yet")` for both
  (`kafka/clients/src/test/java/org/apache/kafka/clients/admin/MockAdminClient.java:1376`,
  `:1396`), and `admin-client.md` §9 requires the Rust mock to mirror Java's mock
  method-for-method. Adding mock support would make it **diverge**; injecting
  accessors would invent test surface. Do not re-raise.
- **CP3 — `AdminConfigsMarshalTests`' injection guard lost its teeth.** Under the old
  shape a wrong reader double-freed and aborted the host (detectable); under shape 4a
  it **leaks**, which no managed assertion sees. This generalizes: a missing or wrong
  per-key value destroy now leaks silently across all 28 RPCs. **Tracked as a CP8
  task** (see CP8 (e)), not a blocker — it is already Critic target 4.
- **CP3 — Rust core doc/impl discrepancy, informational.**
  `describe_consumer_groups_async` / `describe_classic_groups_async` doc comments say
  "once per **distinct** group id" but their bodies pass the non-deduplicated
  `ids.clone()` to `admin_async_per_key_op`, which fires once per *occurrence*.
  Harmless for .NET (every submit site de-duplicates first), but the doc comment is
  the stated contract that `n` is derived from, so it should be corrected on the Rust
  side. Out of scope here — this phase authors no Rust.

### 3.0.2 ⚠ BLOCKER FOUND BEFORE CP7 — a 29th converted RPC. NEEDS PRANAV.

**`remove_members_from_consumer_group` IS converted.** The task brief listed it among
the 21 untouched with the explicit note "Java holds one `KafkaFuture<Map<…>>`, not
per-key — **do not touch**". That is wrong, verified in the merged core:

- `admin.rs:13628+` branches on `remove_all`. When **false** it calls
  `admin_async_per_key_op` over de-duplicated group instance ids and fires
  `callback(id_c.as_ptr(), error, ud)` — **once per distinct id**. When **true** it
  calls `admin_async_future_op` and fires **exactly once** with a **NULL** key
  (`callback(std::ptr::null(), error, ud)`), because in `removeAll` mode Java's
  `memberResult` is not applicable and `all()` is the only observable
  (`admin.rs:13612-13622`).
- The typedef (`admin.rs:13616`) is `fn(*const c_char, *mut Error_t, *mut c_void)`.

**Why every automated check missed it — this is the important part.** Its arity is
**unchanged, 3 → 3**, and .NET's `RemoveMembersFromConsumerGroupCallback(IntPtr
result, IntPtr error, IntPtr userData)` (`AdminCallbacks.cs:475`) still compiles. Only
the **meaning of the first parameter** changed — result-root → key-string. So the
compiler cannot catch it, and the arity-diff sweep that found the other 28 cannot
either. The trampoline reads a `char*` as a result root and calls `*Result_count` on
it: the same abort as every other unmigrated site. It is **1 of the 3 classes still
aborting after CP6**.

**The brief was also internally inconsistent**: it used "Java holds one aggregate
future" as the do-not-touch criterion here, while listing
`alter_consumer_group_offsets` / `delete_consumer_group_offsets` — whose Java shape is
*identical* (`KafkaFuture<Map<K, Errors>>`) — as converted. All three are the same
family.

**Consequences:**
1. It is **shape 4c** (per-key callbacks → one aggregate `Task`), the same family as
   CP7's two, so it belongs **in CP7** — the fan-in type and the tests are shared.
   Doing it separately would pay for that infra twice.
2. It needs something no other RPC in the phase does: a **dual-mode** contract, with a
   **sentinel key** mapping for the single NULL-key `removeAll` callback so the caller
   can await the operation and see its error despite there being no per-member future.
3. **CP7's "full-suite-green returns" gate cannot be met without it.**

**DECIDED (Pranav): option (a) — folded into CP7 as the 29th RPC**, sharing the fan-in
infra with the other two. CP7 is therefore **3 RPCs**, and its full-suite-green gate
stands. Phase total: **29 RPCs.** The dual-mode/sentinel contract was delegated to the
Manager and is specified in §3.0.3 — the Actor implements that, it does not redesign it.

### 3.0.3 `remove_members_from_consumer_group` — the decided dual-mode contract

**There is NO sentinel key, and no new public surface.** The existing .NET
`RemoveMembersFromConsumerGroupResult` already pins the contract, and it composes with
the new ABI unchanged:

- `RemoveAll => _memberInfos.Count == 0` — Java's private `removeAll()`
  (`RemoveMembersFromConsumerGroupResult.java:113-115`, `memberInfos.isEmpty()`).
- `MemberResult(member)` already **throws**
  `ArgumentException("The method: memberResult is not applicable in 'removeAll' mode")`
  when `RemoveAll`. Java does the same. Unchanged.
- `All()` already iterates the **resolved map** in removeAll mode, written in Java's
  exact shape.

So map the ABI's two modes straight onto the one aggregate `Task`:

| Native | Fan-in action |
|---|---|
| `remove_all=true` → **one** callback, **NULL** key, NULL error | complete the aggregate Task with an **EMPTY** map |
| `remove_all=true` → **one** callback, **NULL** key, non-NULL error | **fault** the aggregate Task with that error |
| `remove_all=false` → one callback **per distinct group instance id** | accumulate `(key, error-as-VALUE)`; complete the aggregate Task at countdown zero |

This reproduces today's observable behaviour exactly: in removeAll mode `All()` iterates
an empty map and can only find success, so a removeAll failure reaches the caller
**only** by faulting the await — which is precisely what the existing type remarks
already describe. `MemberResult` keeps throwing. Nothing public changes.

⚠ **`n` is MODE-DEPENDENT — the first and only such RPC in the phase.** `n = 1` when
`remove_all` is true; `n = ` the distinct-group-instance-id count when false (the core
de-duplicates, and its doc says "once per distinct group instance id, **skipping NULL
entries**"). A NULL id therefore yields **no** callback, so .NET must either reject
NULL ids up front or exclude them from `n` — otherwise the countdown over-arms and the
`GCHandle` leaks forever.

⚠ **Per-key error is the map VALUE, not a fault** (Java `Map<MemberIdentity, Errors>`) —
same as CP7's other two.

⚠ The existing type remarks say "in removeAll mode the ABI **result handle** always
carries zero rows". Under the new ABI there is no result handle at all. Correct that
sentence **tersely** (§5) — do not rewrite the block.

### 3.0.4 New environment trap found at CP7 — a stale NATIVE faking a CRASH

⚠ **`cargo build --features ffi` builds only `target/debug`, but a Release
`dotnet build` copies `target/release`.** At CP7 that meant the Release leg ran a
**pre-PR-#201 native from Sep 22**, and aborted with `AccessViolationException` in
`AdminCallbacks.OnCreateTopics` → `CompletePerKey` — i.e. blaming an RPC migrated back
at CP1/CP2 and green in Debug. Fix: `cargo build --features ffi --release` as well,
then rebuild.

This is the **inverse** of the documented traps. Those describe a stale *managed*
binary faking a **pass**; this is the native half faking a **crash**, which is worse,
because it sends you debugging code that is correct. **Build both native
configurations before trusting any Release result**, and add this to every brief.

Also at CP7: the zsh word-split trap (#4) struck again — a 92-class abort sweep
`for c in $classes` ran one iteration whose filter matched zero tests and exited 0,
measuring nothing. Caught and discarded. Prefer one combined filter: when nothing
aborts, nothing can be masked, which is strictly stronger than a per-class loop.

**Rust doc/impl discrepancy, informational (third of its kind).**
`alter_consumer_group_offsets_async` / `delete_consumer_group_offsets_async` doc
comments claim the callback fires "once per **distinct** `(topic, partition)` pair",
but both bodies pass the non-deduplicated `read_topic_partitions` result — the true
count is once per **non-NULL occurrence**. Mitigated .NET-side (alter's keys come from
a dictionary; CP7 added an inline de-dup to delete), so `n = keys.Count` holds. Same
class as §3.0's `describe_consumer_groups` and §3.0.1's `delete_acls` doc bugs. Out of
scope — this phase authors no Rust — but all three should be corrected core-side.

### 3.0.1 Carry-forwards established by CP5 (apply in CP6 and CP7)

- **Owned struct-pointer keys route through a WRAPPER, not a fourth parameter** —
  `AdminCallbacks.CompletePerKeyOwnedKey<TKey,TValue>` (added CP5), so a borrowed-key
  RPC cannot accidentally acquire a key destroy. `CompletePerKey`'s own `finally`
  destroys value+error only when `!resolved` and **never** the key — which is why the
  wrapper exists. CP6's `create_acls` (`AclBinding_t`) and `alter_client_quotas`
  (`ClientQuotaEntity_t`) both route through it with their own `destroyKey`.
- **CP1 declared the 11 destroys but NOT the accessors** on the new value types
  (`DeleteAclsFilterResults` needed `_count` / `_get_binding` / `_get_error` added in
  CP5). Check for the same gap on any CP6/CP7 value type before starting.
- ⚠ **A CP6/CP7 test that assumes a SYNCHRONOUS submit failure passes vacuously.** The
  Rust mock returns per-key **failed futures**, not a failed submit
  (`mock_admin_client.rs:1726`), so completions arrive **asynchronously on the
  dispatcher thread**. CP5 had to re-point two hand-fired trampolines at the real ABI
  *and* make them wait. Also: unmatched keys are faulted with a **synthesized code-0**
  error, so `ThrowsAsync<KafkaException>` alone proves nothing — assert the code and
  the exact message, and mutation-verify both directions.
- **`delete_acls_async`'s doc comment contradicts its body** (says "exactly `count`
  times"; the body de-duplicates first). Harmless here since .NET passes
  `DistinctFilters`, but a caller trusting the doc would over-arm its countdown.
  Rust-side, out of scope — same class as §3.0's `describe_consumer_groups` doc bug.

### 3.0.5 CP8 residue — the one open follow-up (NOT a defect)

CP8 removed 57 orphaned statics plus the two dead result-root walkers, proving
non-reference **by the compiler** (`TreatWarningsAsErrors` + `EnforceCodeStyleInBuild`
make IDE0051/IDE0052 build errors, so a private orphan cannot survive), not by grep.

**~20 further orphans are dead in `src` but pinned alive by guard tests** —
`AdminP4ReaderWiringTests`' closure scan, `AdminP4ResultMarshalTests`,
`PublicAdminCreateTopicsTests`. Removing them means also deleting ~20 wiring-guard
tests that now guard dead readers. CP8 deliberately stopped at the compiler-provable
boundary rather than gut those suites under the token constraint.

**DECIDED (Pranav): remove BOTH** — the dead readers and their now-pointless
wiring-guard cases — as one small mechanical checkpoint (**CP8b**), reusing CP8's
enumeration rather than re-deriving it. No Critic pass on CP8b.

⚠ **The hazard in CP8b is over-deletion.** `AdminP4ReaderWiringTests`' closure scan and
the P4 result-marshal suites still protect the **live shape-3/3b/3c readers that 21
unmigrated RPCs depend on**. Remove only the cases pinning *dead* readers; the guard
must keep guarding the live ones. Likewise `PublicAdminCreateTopicsTests` is a public-API
class with live coverage — remove cases, not the class, unless a class is left empty.

Also noted by CP8: **no dead delegate typedefs exist.** All 47 are live, because
CP2–CP7 changed arity in place rather than adding new ones. §6's risk mitigation ("a
leftover reference then fails to compile") therefore rests on the *readers*, not the
typedefs.

### 3.1 Explicitly out of scope

- All 21 unconverted RPCs, including `remove_members_from_consumer_group` and
  `list_transactions`. Zero changes.
- All synchronous (non-`_async`) admin entry points.
- `describe_user_scram_credentials` RESOURCE_NOT_FOUND handling (PR #201's change there
  was reverted upstream).

### 3.2 `LogDirDescription.IsCordoned` — **decided: included as CP9**

`kafka_admin_LogDirDescription_is_cordoned` is newly exported and closes a tracked .NET
gap. Pranav chose to include it rather than defer. Placed last (CP9, after the CP8 sweep)
so it stays isolated from the ABI-break work — any regression detected through CP8
remains unambiguously attributable to the migration, not to this unrelated addition.

---

## 4 · Review cadence — **single Critic pass at the end**

Per your instruction, this **overrides** the standard Manager loop in `agent-roles.md`:

- Actor 79 works CP1 → CP9, committing per checkpoint. **No Critic between checkpoints.**
- Critic 79 runs **once**, after CP9 is complete — but only when Pranav explicitly
  says to start it. Finishing CP9 does **not** auto-trigger Critic 79; the Manager
  waits for that go-ahead even if all checkpoints are green.
- Then the normal comment loop applies: `COMMENTS.79.md` → Actor fixes → Critic
  re-review, until no approved issues remain.

### 4.1 Highest-value Critic targets (stated now so the single pass is aimed)

1. **Countdown correctness per RPC** — `n` vs. the ABI's documented callback count;
   the `n == 0` path; `FailUncompleted`/`CompleteKeysWithNoRequest` firing only at zero.
2. **Premature or missing `GCHandle` / `SafeAdminHandle` release** — early = UAF + a
   `Dispose` destroying the client under in-flight callbacks; late = permanent leak with
   no managed symptom (the M9/P8 precedent: caught only by a differential test).
3. **Per-key `error` ownership flip** — a surviving `FromBorrowedHandle` leaks; a
   `FromHandle` left on a still-borrowed path double-frees and aborts the process.
4. **Per-key value + owned-key destroys** on every path, including the throwing one.
5. **CP7's shape**: per-key error as map value (not a fault), and accumulator thread safety.
6. **Concurrency**: per-callback no-throw boundary; no shared mutable state without a lock.

---

## 5 · Token-conservation constraint (binding on Actor 79 and Critic 79)

Pranav is extremely short on tokens. Both agents are instructed:

- Focus **solely on code change and behavior correctness**.
- Do **not** write or expand verbose comments, docstrings, or prose documentation. A
  short one-liner is acceptable only where it is genuinely load-bearing for an
  ownership or thread-safety invariant.
- Do **not** reproduce this codebase's existing habit of multi-paragraph XML-doc
  `<remarks>` blocks on new code. The stale-remarks correction in CP8 is a **terse
  edit**, not a rewrite.
- Critic findings: the defect, the site, the consequence. No essays.

---

## 6 · Risks

| Risk | Mitigation |
|---|---|
| Countdown off by one for some RPC → silent UAF or permanent leak, no managed symptom | Derive `n` per RPC from its ABI doc comment; CP8 sweep; differential release tests (M9/P8 pattern) |
| CP7 mis-shaped as per-key faults instead of map values | Called out in §2.0; the `ElectLeaders`/M15/P4 precedent is already documented in `KeyedResultMarshal` |
| A converted RPC missed → it still compiles against a stale delegate and fails at runtime with `EntryPointNotFoundException` or silent wrong data | CP8 removes the dead typedefs; a leftover reference then fails to compile |
| Concurrent callbacks expose latent non-thread-safe state | §2.2; Critic target 6 |
| `pr-201` is not on `master` and may change upstream | This work is local to `prashah_dev_dotnet_binding`; re-verify the ABI if `pr-201` is rebased |
