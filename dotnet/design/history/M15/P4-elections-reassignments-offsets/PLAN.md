# M15/P4 — Elections, reassignments, offsets (4 RPCs, one phase)

**Status:** ✅ **APPROVED 2026-09-10** by the maintainer. Single phase, single
directory, **N = 70**; the actor-critic loop is authorized to start. Critic 70
reviews at each of §3's TWO internal stage boundaries.
**Agent number: N = 70.** Comment files `COMMENTS.70.md` / `COMMENTS.DONE.70.md`.
Verified free: **0** files matching `COMMENTS*70.md` against a control-positive of
**2** for `COMMENTS*69.md`; **0** occurrences of `N=70` in the tracked `STATUS.md`
against a control-positive of **1** for `N=69`. The roadmap's §8.1 ledger
(`design/current/PLAN-M15-admin-client.md:807`) also says **70**, and the phase
table no longer carries an `N` column at all (**0** rows match the old
`| **M15/Pn** | <digits> |` form against a control of **10** phase rows) — the
single-source fix made during P3 scoping held, so there is no stale number here.

**Parent roadmap:** the M15 admin roadmap under `bindings/dotnet/design/current/`
— ⚠ **deliberately UNTRACKED**; never `git add` it.

**Base:** `719b3b42` (M15/P3, squashed and pushed; branch 0 ahead / 0 behind).

**Mode:** **A, strictly.** No Rust, no new ABI function, no `cbindgen.toml` or
`generator/` change, **for any reason**. All four entry points and every accessor
this phase reads are already exported — verified in §1.

**Branch:** `prashah_dev_dotnet_admin`. Do NOT create a branch, merge, or rebase.

⚠ **ONE PHASE, ONE AGENT NUMBER, ONE DIRECTORY (maintainer instruction).** No
P4a/P4b. §3's internal stages are stages *within this phase* — commit and Critic
round at each boundary, exactly as P3 ran — **not** sub-phases. Do not mint a
second agent number and do not propose a split.

---

## 1 · The ABI read — every accessor set, checked against §4.4

| RPC | Result type | Accessors (verbatim from the header) | §4.4 shape | Key | Value | Walker unchanged? |
|---|---|---|---|---|---|---|
| `electLeaders` | `ElectLeadersResult` | `count`, `get_topic`, `get_partition`, `get_error`, `destroy` | ⚠ **the ABI set says "shape 2"; Java says otherwise** — see §1.1 | composite `TopicPartition` | ⚠ **the ERROR is the value** (`KafkaException?`) | **YES** — `CompleteAggregate`, error accessor supplied as the **value** reader |
| `alterPartitionReassignments` | `AlterPartitionReassignmentsResult` | `count`, `get_topic`, `get_partition`, `get_error`, `destroy` | **2** | composite `TopicPartition` | — void | **YES** — `Complete<TKey>` |
| `listPartitionReassignments` | `ListPartitionReassignmentsResult` | `count`, `get_topic`, `get_partition`, `get_value`, `destroy` | **3** | composite `TopicPartition` | `PartitionReassignment` | **YES** — `CompleteAggregate` |
| `listOffsets` | `ListOffsetsResult` | `count`, `get_topic`, `get_partition`, `get_error`, `get_value`, `destroy` | **1** | composite `TopicPartition` | `ListOffsetsResultInfo` | **YES** — `Complete<TKey, TValue>` |

**The roadmap's "partition-keyed throughout" is correct** — all four key on
`TopicPartition`, composed from `get_topic(i)` + `get_partition(i)`. P2b's
`DeleteRecordsKey` (`AdminCallbacks.cs:401`,
`Func<IntPtr, int, TopicPartition>`) is the existing precedent for that reader
shape.

### 1.1 ⚠⚠ THE HEADLINE — two RPCs with BYTE-IDENTICAL accessor sets and DIFFERENT Java shapes

`ElectLeadersResult` and `AlterPartitionReassignmentsResult` expose **exactly the
same five functions** — `count`, `get_topic`, `get_partition`, `get_error`,
`destroy`. Verified by diffing the two accessor lists with the type prefix
stripped: **identical**.

But their Java shapes are not the same:

| | Java | Meaning of `get_error(i)` |
|---|---|---|
| `alterPartitionReassignments` | `Map<TopicPartition, KafkaFuture<Void>>` + `all()` (`:45`, `:52`) | a **per-key failure** — that partition's `Task` **faults** |
| `electLeaders` | **`KafkaFuture<Map<TopicPartition, Optional<Throwable>>>`** + `all()` (`:47`, `:54`) | **the VALUE** — one future over a map; a partition's entry *is* its optional error |

Java's own javadoc on `partitions()` states it: *"If the election succeeded then
the value for a topic partition will be the empty Optional. Otherwise the election
failed and the Optional will be set with the error."*

⚠⚠ **This breaks the classification method that has worked for three phases.**
Since P2 the method has been "enumerate the accessor set, classify against §4.4".
Here that method returns the **wrong answer** for `electLeaders`: the set says
shape 2, and shape 2 would fault the key. **The accessor set is necessary but not
sufficient. The Java accessor is a co-equal input, not a cross-check.**

**Consequences, all mandatory:**

  - `electLeaders` goes through **`CompleteAggregate<TopicPartition, KafkaException?>`**,
    with `get_error(i)` read by the **value** reader — because Java's map value
    type *is* the optional error. `Optional<Throwable>` → `KafkaException?` is the
    natural mapping, the same substitution P3 made for `OptionalLong` → `long?`.
  - It must **NOT** go through `Complete<TKey>`. That would convert a
    per-partition election failure into a **faulted `Task`**, changing both the
    error semantics and the future arity (N futures instead of one).
  - **The per-partition error is still `const`/BORROWED** → `FromBorrowedHandle`,
    never destroyed — same as every other per-key error site.
  - **A test must pin that a non-null per-partition error becomes a MAP VALUE, not
    a fault**, and that `electLeaders` routes through the aggregate walker. This
    is the phase's single most important test: a future maintainer looking only at
    the accessor set will "correct" it to `Complete<TKey>` and the suite must go
    red.
  - `ElectLeadersResult.all()` is **not** a plain `all_of`: Java iterates the map
    and completes exceptionally with the **first present** `Optional`
    (`ElectLeadersResult.java:56-68`). Translate that loop, do not substitute an
    aggregate-fault helper.

### 1.2 No walker seam change

All four RPCs fit the four shipped callables — `Complete<TKey,TValue>` (`:211`),
`Complete<TKey>` (`:283`), `CompleteAggregate<TKey,TValue>` (`:326`),
`CompleteList<TValue>` (`:398`) — **unchanged**. P4 adds **no** new callable and
must not modify `Accessors`.

⚠ That makes P4 the **first phase since P1 with no mechanism change at all**. The
risk therefore moves entirely to **shape fidelity and input marshalling**, which
is where §1.1 and §2 concentrate.

---

## 2 · §9.1 item 9 (KEY-SET vs ROW-SET) — checked PRE-WRITE, and DISCHARGED

Item 9 requires this before writing, not in review. Result: **no zero-row-key
hazard in any P4 RPC, and no Mode-B gap.**

**`alterPartitionReassignments`** was the flagged candidate, because Java's input
is `Map<TopicPartition, Optional<NewPartitionReassignment>>` where
`Optional.empty()` means *"cancel this reassignment"* — exactly the kind of value
that can vanish across a flattened encoding. It does not, for two independent
reasons:

  1. **The request is NOT row-flattened over a collection.** The ABI takes
     parallel arrays where **entry `i` is one caller key** — `topics[i]`,
     `partitions[i]` — so every key contributes exactly one row. A key that
     flattens to zero rows is **not expressible**. This is structurally unlike
     `incrementalAlterConfigs` (P3's 69.6), whose rows were `(key, element)` pairs.
  2. **`Optional.empty()` has a DEDICATED channel** — a `const bool *cancel`
     array. The header states the design intent verbatim: *"`cancel[i] != false`
     **reverts** the reassignment of that partition — Java's empty `Optional`
     (`Admin.java:1142-1143`) — and `target_replicas[i]` /
     `target_replica_counts[i]` are then not read. **A separate flag rather than a
     NULL replica pointer, so cancelling stays distinct from "present but empty",
     which Java rejects.**"*

**So the ABI already encodes the three-way distinction** Java needs: *cancel* ·
*reassign to a non-empty list* · *present-but-empty (rejected)*. Nothing is lost.

**Still required of the binding:** `cancel` and `target_replicas` must be driven
from the **`Optional`'s presence**, never from an empty list. A C# input of
`IReadOnlyDictionary<TopicPartition, NewPartitionReassignment?>` where `null` =
cancel maps cleanly; **a `?? Array.Empty<int>()` anywhere on this path is the
defect**, because it collapses cancel into present-but-empty, which Java rejects.
**A test must prove cancel and empty-list produce different calls** — the same
null-vs-empty discipline as P2b's `NewPartitions` (roadmap §7 gate 6).

The other three inputs are also one-row-per-key (`electLeaders`,
`listOffsets`) or take no key array at all (`listPartitionReassignments`), so item
9 is satisfied trivially for them.

---

## 3 · Internal staging — TWO stages, and why

**Recommendation: two internal stages, one commit and one Critic round each.**
Not sub-phases (§0) — stages inside this phase, the P3 pattern.

| Stage | Content | Why here | Boundary |
|---|---|---|---|
| **1** | The shared `TopicPartition` key reader · **`ElectLeaders`** · **`AlterPartitionReassignments`** · types `ElectionType`, `NewPartitionReassignment` | ⚠ **The accessor-set twins land TOGETHER, in a small diff.** §1.1's hazard is not visible in either RPC alone — it is visible in the *pair*. The test that distinguishes them is the stage's deliverable. This is P3 Stage 1's logic: land the risky thing first, small. | Full gate; commit; Critic round |
| **2** | **`ListPartitionReassignments`** · **`ListOffsets`** · types `PartitionReassignment`, `OffsetSpec` (7 kinds), `IsolationLevel`, nested `ListOffsetsResultInfo` | The value-heavy pair. Zero shape risk (plain shapes 3 and 1); the risk is **input encoding** — 7 `OffsetSpec` kinds behind 6 sentinels plus a flag (§4.2). | Full gate; commit; Critic round |

**Why two and not three:** P4 is ~4 RPCs and ~18 public types against P3's 8 and
~28, with **no seam change**. Three boundaries would cost more than they buy.
**Why not zero:** §1.1's hazard is subtle enough that reviewing it inside a
4-RPC diff, alongside `ListOffsets`' 7-way input encoding, would dilute it.

---

## 4 · Per-RPC surface

⚠ **D18 governs every accessor below: an unforced getter is a PROPERTY**; the
method form only where `CS0102` forces it (a same-named `public static` factory).
`OffsetSpec`'s factories make that collision live — see §4.2.

### 4.1 Stage 1

**`ElectLeaders`** — `ElectLeadersResult.java:47, :54`:

```csharp
public sealed class ElectLeadersResult
{
    public Task<IReadOnlyDictionary<TopicPartition, KafkaException?>> Partitions();  // partitions()
    public Task All();                                                               // all()
}
```
Input: `elect_leaders_async(admin, election_type, all_partitions, topics, partitions, count, timeout_ms, …)`. Java's `electLeaders(ElectionType, Set<TopicPartition>)` accepts a **null** set meaning *all partitions* — carried by the `all_partitions` bool. **Null-vs-empty matters again**: a null set (all) and an empty set are different requests.

**`ElectionType`** — root namespace (Java `org.apache.kafka.common.ElectionType`,
D13). `PREFERRED((byte) 0)`, `UNCLEAN((byte) 1)` (`ElectionType.java:27`); the
enum's underlying values **must** be those codes, they cross the ABI as `int32_t`.

**`AlterPartitionReassignments`** — `AlterPartitionReassignmentsResult.java:45, :52`:

```csharp
public sealed class AlterPartitionReassignmentsResult
{
    public IReadOnlyDictionary<TopicPartition, Task> Values { get; }   // values()
    public Task All();                                                 // all() — KafkaFuture<Void>
}
```
**`NewPartitionReassignment`** — `NewPartitionReassignment.java:32, :38`: ctor
taking `List<Integer> targetReplicas`, accessor `targetReplicas()`. Java's ctor
**rejects an empty list**; mirror that.

### 4.2 Stage 2

**`ListPartitionReassignments`** — `ListPartitionReassignmentsResult.java:40`, a
**single** accessor. Do not add a second view:

```csharp
public sealed class ListPartitionReassignmentsResult
{
    public Task<IReadOnlyDictionary<TopicPartition, PartitionReassignment>> Reassignments();
}
```
**`PartitionReassignment`** — `PartitionReassignment.java:32, :41, :49, :57`:
ctor `(replicas, addingReplicas, removingReplicas)` plus those three accessors.
ABI reads them through six flattened accessors (`replica`/`replica_count`,
`adding_replica`/`adding_replica_count`, `removing_replica`/`removing_replica_count`)
— **no child handle per list**, the same flattened pattern as P3's `ReplicaInfo`.

**`ListOffsets`** — `ListOffsetsResult.java:41, :54`:

```csharp
public sealed class ListOffsetsResult
{
    public Task<ListOffsetsResultInfo> PartitionResult(TopicPartition partition);          // :41
    public Task<IReadOnlyDictionary<TopicPartition, ListOffsetsResultInfo>> All();          // :54

    // NESTED per D17 — Java declares it `public static class` at :70
    public sealed class ListOffsetsResultInfo
    {
        public long Offset { get; }        // offset()      :82
        public long Timestamp { get; }     // timestamp()   :86
        public int? LeaderEpoch { get; }   // leaderEpoch() :90 — Optional<Integer>
    }
}
```
  - ⚠ **`ListOffsetsResultInfo` is NESTED**, like P3's `ReplicaLogDirInfo` (D17).
    **The roadmap's P4 row lists it as a flat type — that is stale; Java wins.**
  - `partitionResult(partition)` is a **third** accessor beyond the usual
    values/all pair, and Java **throws** for a partition that was not requested —
    mirror that, do not return null.
  - **`leaderEpoch()` is `Optional<Integer>` → `int?`.** The ABI signals absence
    **structurally**, not by sentinel: `bool ..._leader_epoch(info, int32_t *out_epoch)`
    returns `false` when Java's is `Optional.empty()`. ⚠ **Use the bool.** Do not
    invent a `-1` sentinel — contrast P3's `total_bytes`, where `-1` *was* the
    documented sentinel. Different RPCs, different conventions.

**`OffsetSpec`** — ⚠ **Java has SEVEN kinds, not one** (`OffsetSpec.java:26-32`):
`EarliestSpec`, `LatestSpec`, `MaxTimestampSpec`, `EarliestLocalSpec`,
`LatestTieredSpec`, `EarliestPendingUploadSpec`, `TimestampSpec`, exposed through
the factories `latest()`, `earliest()`, `forTimestamp(long)`, `maxTimestamp()`,
`earliestLocal()`, `latestTiered()`, `earliestPendingUpload()` (`:47-101`). **The
roadmap's row says only `OffsetSpec`; this is more surface than it implies.**

The ABI encodes them as `is_timestamp[i]` + `spec_timestamps[i]`, header verbatim:
*"When `is_timestamp[i]` is true the spec is `OffsetSpec.forTimestamp(spec_timestamps[i])`
**for any value at all**; otherwise `spec_timestamps[i]` selects one of the six
no-argument factories through the `ListOffsets` wire sentinel … `-1` = `latest()`,
`-2` = `earliest()`, `-3` = `maxTimestamp()`, `-4` = `earliestLocal()`,
`-5` = `latestTiered()`, `-6` = `earliestPendingUpload()`. Any other value with
`is_timestamp[i]` false is rejected."*

⚠⚠ **The flag is load-bearing and the header says why:** *"that projection is not
injective — `forTimestamp(-2)` and `earliest()` both yield `-2`, yet Java treats
them differently up to that point."* **A test must pin `forTimestamp(-2)` against
`earliest()` producing different calls.** Collapsing the flag is a silent
wrong-answer defect.

⚠ **D18 / `CS0102` applies here.** If `OffsetSpec` exposes a static factory
`Latest()` *and* an instance accessor of the same name, the property form is
forbidden — this is precisely the `RecordsToDelete.BeforeOffset` situation. Choose
deliberately and **record the reason at the site**.

⚠ **An unrecognised sentinel with `is_timestamp` false is REJECTED by the ABI, and
that rejection fires the callback INLINE** on the calling thread (the header names
it among the inline triggers for this entry point). `RunContinuationsAsynchronously`
is load-bearing, and the `GCHandle` must be freed exactly once on that path. **A
test must drive it** — an integration test never reaches it.

**`IsolationLevel`** — root namespace (Java `org.apache.kafka.common`).
`READ_UNCOMMITTED((byte) 0)`, `READ_COMMITTED((byte) 1)` (`IsolationLevel.java:22`);
underlying values must be those ids. Java's `ListOffsetsOptions` default is
`READ_UNCOMMITTED`; any other id is rejected by the ABI.

---

## 5 · Mode-B gap list — carried forward from P3, not restarted

**P4 inherits `design/history/M15/P3-cluster-configs-logdirs/PLAN.md` §15 with its
two entries** (`LogDirDescription.isCordoned()`; the zero-operation resource in
`incrementalAlterConfigs`). **Add to that list; do not restart it.** Its two
entries are unchanged by P4 and are not P4's to close.

⚠ **P4's scoping identified NO new Mode-B gap** — §2's `Optional.empty()` risk is
discharged by the ABI's dedicated `cancel` flag, and every P4 accessor needed is
exported. If Stage 1 or 2 turns one up: **implement the most honest Mode-A
behaviour, document the divergence at the site, add the entry as you implement it,
and continue.** Escalate only if no honest Mode-A behaviour exists. P3's zero-op
workaround is the template — a workaround carrying its **own tripwire**, written
to go RED when the gap closes.

---

## 6 · Tests

Vehicle: **`MockAdminClient`**, plus the **direct submit with a capturing
callback** harness where the mock cannot produce a shape.

⚠ **Verify each RPC's mock support before relying on it.** P2b found
`createPartitions` and non-empty `deleteRecords` unimplemented; P3 found all eight
implemented. **Do not assume either way** — check `src/admin/mock_admin_client.rs`
and assert the exact *"Not implemented yet"* message where that is what Java's mock
does (`admin-client.md` §9).

### 6.1 Non-negotiable, inherited

1. **Per-type reflection walk over every new/changed public signature — not
   sampling.** The standard is P3's: a full exported-surface diff between commits
   with a control-positive. C# upcasts and widens silently.
2. **Borrowed-vs-owned `KafkaError`, by injection.** All four P4 RPCs have a
   `const`/**borrowed** per-key `get_error(i)` → `FromBorrowedHandle`, never
   destroyed; the callback's `error` parameter is **owned** → `FromHandle`.
   ⚠ The direction has now flipped three times across P3's stages — do not reason
   from the last thing you wrote.
3. **Span-the-op `DangerousAddRef`** on every submit; the **differential** check
   (no op in flight → `Dispose` releases; in flight → it does not; completes → it
   then does).
4. **`GCHandle` freed exactly once on every path**, including the inline path
   §4.2 makes reachable by ordinary bad input.
5. **D20 — reflection surface-set assertions filter by `BindingFlags` +
   `CompilerGeneratedAttribute` ONLY, never a name predicate**; extra names go in
   the expected set. ⚠ The rule-file text is **still unapplied** — verified: **0**
   `CompilerGeneratedAttribute` and no `§0.4` in
   `bindings/dotnet/.claude/rules/ffi-marshalling.md`, against a control-positive
   `§0.3` present. **It binds P4's own tests regardless.**
6. **Trap 11 / §9.1 item 7** — an injection **result** is not evidence unless the
   build reported **`0 Error(s)`**. It fired live in P3.
7. ⚠ **§9.1 item 10 — a named injection is a claim to be MEASURED, not repeated.**
   **This applies to every injection named in this plan and in any brief the
   Manager writes.** P3's 69.7 named three trigger edits; two were measured
   harmless. **Never write an unmeasured danger model into a code comment.**
8. ⚠✅ **The prose sweep is MECHANICAL, and covers text ADDED *or FALSIFIED***
   (widened by maintainer ruling 2026-09-10, from finding 70.1).

   **Two passes, and the second is the one that was missing:**

   a. **Added.** Grep the comment lines this commit *adds* for universal
      quantifiers — "only", "any", "every", "always", "never", "exactly",
      "cannot", "the one place" — and check each against code in the same commit.
   b. ⚠ **Falsified.** Then ask: **"does anything I changed make an existing,
      untouched comment ELSEWHERE in the codebase now false?"**

   **A commit's semantic footprint includes every doc block whose TRUTH VALUE the
   change affects — not just the lines the diff touches.** This is the doc analog
   of a regression test: new code can break an old *claim* exactly as easily as it
   can break an old *test*, and nothing but a deliberate look catches it.

   **Proof by counterexample — 70.1.** The falsifying change was **100% in
   `AdminCallbacks.cs`**; the falsified prose was **100% in the untouched
   `KeyedResultMarshal.cs`** (0 diff lines, against a +231/−12 control on
   `AdminCallbacks.cs`). **A lines-added sweep is structurally blind to that split,
   by construction** — it is not a diligence failure, it is a scope gap.

   **How to run pass (b) cheaply:** for each *concept* the commit changes (a
   shape, an ownership rule, a routing decision), grep the codebase for prose
   asserting the old invariant — not for the lines you wrote. If a claim was
   written against one example and stated as if universal, the new example is
   exactly what falsifies it.

9. ⚠✅ **A NEW READER BUILT ON A SHARED KEY/VALUE FACTORY MUST BE ADDED TO THE
   WIRING GUARD IN THE SAME COMMIT THAT INTRODUCES IT.** Adopted 2026-09-10 from
   finding 70.12. *(Standing rule; sits alongside P3 `PLAN.md` §9.1's items 9 and
   10, which the briefs cite by those numbers.)*

   **Why it needs to be a checklist item and not a habit:** readers built from the
   same factory — `TopicPartitionKey`, `BorrowedOptionalError` and their kin — are
   **layout-compatible with their siblings**, so a *wrong* wiring returns the
   *right* answer and **no behavioural test can see it**. `AdminP4ReaderWiringTests`
   exists precisely to close that gap (it was built for finding 70.2), and Stage 2
   reopened it by adding two `TopicPartitionKey`-built readers
   (`AdminCallbacks.cs:827` `ListPartitionReassignmentsKey`, `:872` `ListOffsetsKey`)
   without extending the guard.

   **Measured, not argued:** cross-wiring either new reader to `deleteRecords`'
   accessors left **1297/1297 green**, while a reachability control (a throw
   inserted at each) confirmed both readers genuinely execute (**10 RED / 2 RED**).
   The guard file was **absent from the Stage-2 diff entirely** — 0 occurrences,
   against a control-positive of 1 for `AdminCallbacks.cs` in the same range.

   **How to apply (pre-write, by the Actor — not post-hoc by the Critic):** when a
   commit adds a reader built on a shared factory, add its `[InlineData]` row **and**
   its entry to the reader set the guard tracks, in that same commit. **Then re-run
   the cross-wire injection and confirm it goes RED** — a grown file is not a closed
   guard; only the injection proves closure.

### 6.2 P4-specific

  - ⚠⚠ **`ElectLeaders` routes through the AGGREGATE walker and a non-null
    per-partition error becomes a MAP VALUE, not a fault** (§1.1). **The phase's
    most important test.** Pin the routing too, so "correcting" it to
    `Complete<TKey>` goes red.
  - **`ElectLeaders.All()` completes exceptionally with the FIRST present
    `Optional`**, matching `ElectLeadersResult.java:56-68` — not a generic
    aggregate fault.
  - **The accessor-set twins are distinguishable**: an assertion that
    `ElectLeaders` and `AlterPartitionReassignments`, despite identical ABI
    accessor sets, produce **different result shapes** (one future vs. per-key
    futures).
  - **`alterPartitionReassignments` cancel-vs-empty produce DIFFERENT calls**
    (§2): `null`/`Optional.empty()` sets `cancel[i]`; an empty target-replica list
    is **rejected**, as Java rejects it.
  - **`electLeaders` null-vs-empty partition set**: null → `all_partitions` true;
    empty → a request for no partitions. Different calls.
  - ⚠ **`forTimestamp(-2)` and `earliest()` produce different calls** (§4.2) —
    the non-injective-projection test.
  - **All six `OffsetSpec` sentinels round-trip** (`-1`…`-6`), and an unrecognised
    sentinel with `is_timestamp` false **drives the inline callback path** with the
    `GCHandle` freed exactly once.
  - **`leaderEpoch` absence is read from the BOOL**, not a sentinel — include an
    epoch of `-1` that is genuinely present, to prove it is not implemented as
    "negative → null".
  - **`partitionResult(unrequested)` throws**, matching Java.
  - **`ListOffsetsResultInfo` is nested with the D18 property form**; a reflection
    assertion, since the roadmap says otherwise.
  - **`ElectionType` / `IsolationLevel` id round-trips** (0/1 each).
  - **P1–P3 regression**: their tests pass **unmodified**; `Accessors` gained no
    field; no new walker callable exists.

### 6.3 Cross-cutting

  - **TFM-matrix smoke** on net462 (via netstandard2.0), net8.0, net10.0.
  - **DoD §10 (hot-path allocation audit): N/A** — Admin is batch/administrative
    (`admin-client.md` §10). **State it; never skip silently.**
  - **DoD §11: N/A** to `IAdmin`, spirit verified — every new RPC is a plain sync
    `fn` returning a `*Result`; only `Close` returns `Task`.

---

## 7 · Definition of Done

```
cargo build --features ffi                       # header MUST be byte-identical (Mode-A proof)
dotnet build -c Release --no-incremental          # 0W/0E across all 6 TFM outputs
dotnet test -f net10.0 && dotnet test -f net8.0   # confirm the COUNT; grep 'Test Run Aborted'
dotnet format --verify-no-changes
cargo xtask format-check && cargo xtask lint      # from the REPO ROOT
```

**Test floor: 1178** (P3's close). Run the **whole** gate at **each** stage
boundary, not only at the end.

**Mode-A proof, every round, with a control-positive in the same command:**
```
git diff 719b3b42..HEAD -- src/ cbindgen.toml target/include/confluent_kafka.h generator/   # EMPTY
git diff --stat 719b3b42..HEAD -- bindings/dotnet/                                          # NON-empty
```

---

## 8 · Commit hygiene

  - One commit per §3 stage; `fixup!` referencing it when closing a comment.
  - **Never `git add -A`.** Never `git add` `COMMENTS.70.md` /
    `COMMENTS.DONE.70.md`, the repo-root `.claude/agents/dotnet-*.md`, anything
    under `.claude/agent-memory/`, or `design/current/PLAN-M15-admin-client.md`
    (untracked but **not** gitignored — one `git add -A` from being committed).
  - **Rule files are never edited by an automated agent** — `CLAUDE.md`,
    `bindings/dotnet/CLAUDE.md`, and everything under any `.claude/rules/`
    **including `ffi-marshalling.md`**. Draft candidate insertions as a separate
    file for the maintainer, as P3's `RULE-DRAFT-D20-ffi-marshalling.md` did.
  - **No squash, no rebase** — the maintainer does that after review.

---

## 9 · The rule that produced this milestone's real defects

⚠ **THE PLAN IS NOT REVIEW GROUND TRUTH.** P1's sketch was wrong twice, P2a's a
third time, P2b's G2 remedy a fourth; P3's scoping corrected four roadmap claims
and its rounds produced five instances of *a conclusion resting on an unverified
premise* — **one from every role in the loop**, caught only by measurement.

**A `*Result`'s public accessor signature is the contract for the SHAPE THE BINDING
PUBLISHES**, not the private field it is derived from — and **quoting a signature is
not enough**: Java has no nullable-reference annotations, so **read the javadoc AND
the body**.

⚠⚠ **BUT — for a ROUTING decision, read the STORED FIELD, not the public accessor.
Amended 2026-09-10 from finding 70.9; the sentence above, taken alone, is BACKWARDS
for exactly one bound RPC.**

`createTopics` is that RPC, and it is the only one:

  - `CreateTopicsResult.java:33` — the **stored** field is
    `Map<String, KafkaFuture<TopicMetadataAndConfig>>` → **shape 1**.
  - `:43-45` — the **public** accessor is `Map<String, KafkaFuture<Void>> values()`,
    narrowed by **`thenApply(v -> null)`** → the **shape-2 signature**.

**Route by the stored field's future type**, which may differ from what the public
accessor's signature implies. **Sweep result, run twice independently (Critic 70,
then Actor 70 rather than citing it): all 16 bound results checked; `createTopics`
is the ONLY one with a `thenApply`-narrowed accessor.** It is a single member, not
a family — recorded here so no later phase re-derives it.

**The two rules do not conflict; they answer different questions.** *What does the
binding publish?* → the public accessor. *Which walker callable does it route
through?* → the stored field.

⚠⚠ **New for P4, from §1.1: the ABI accessor set is not sufficient either.**
`ElectLeaders` and `AlterPartitionReassignments` prove it. **Both the header and
the Java accessor must agree before a shape is settled; where they appear to
disagree, Java defines the shape and the header defines the mechanics.**

Where this plan and the Java source disagree, **the Java source wins and this plan
is the defect** — report it rather than implementing it.

---

## 10 · ⚠ Environment traps — these fabricate FALSE PASSES

1. **`PATH` is clobbered** — prepend
   `export PATH="/usr/bin:/bin:/usr/local/bin:/opt/homebrew/bin:$HOME/.cargo/bin:$PATH"`
   to every Bash call. `command -v` is not reliable.
2. **`sed` does not exist** — use `awk 'NR>=A && NR<=B'`.
3. **`grep` may be `ugrep`** — emits *nothing* on patterns it rejects. Use
   `/usr/bin/grep` for evidence.
4. **`cat` may be a missing `bat` alias** — `cat > f <<'EOF'` silently writes a
   **0-byte file**. Use `/bin/cat`.
5. **zsh** does not word-split unquoted vars; unquoted globs abort the command.
6. **A zero-match test filter exits 0.** Confirm the COUNT.
7. ⚠ **An ABORTED `dotnet test` exits 0** — `Test Run Aborted` is the only signal.
8. **`dotnet build -c Release` + `dotnet test --no-build` runs DEBUG binaries.**
9. ⚠⚠ **A FAILED BUILD + `--no-build` prints `Passed!` off the STALE binary** —
   assert **`0 Error(s)`** before trusting any test or injection result. Observed
   live in P3, one day after being written down.
10. **`ls … | awk` exits 0 on empty input**, so `||` never fires — use `test -f`.
11. **Every negative claim needs a control-positive in the same command.**
    ⚠ **A FILTERED grep is not a count** — `grep X | grep -i "kw"` can silently
    drop the one line that refutes it. For a negative count, grep **unfiltered**
    first and inspect what a filter would have excluded. This produced one of P3's
    five unverified-premise instances.

### 10.1 FALSE-FAIL traps — do not burn a round

  - `ConsumerPollBridgeTests.ResultBridge_RunsContinuationsAsynchronously_OffTheCompletingThread`
    (`26761aa4`) and
    `ConsumerRebalanceListenerBridgeTests.DisposeAsync_WithLiveRegistration_ReturnsWithoutHanging`
    (`b25bf7f0`) — assert `completingThreadId != continuationThreadId`, unsound
    because managed thread ids are **recycled**.
  - `ProducerSubmitHandleRefTests.SubmitVoidOperation_WhenAddRefThrows_DoesNotRootTheCompletionContext`
    (`ae72b65e`) — a marginal **allocation-budget** flake (seen at 2,139,104 B vs
    a 2,000,000 B budget); passes on re-run and 3/3 in isolation.

  - `PublicProducerDeliveryCallbackAllocationBudgetTests.CallbackSend_AddsOnlyTheRegistration`
    — a marginal **allocation-budget** flake (seen at 79 B against a 64 B budget),
    found during P4 Stage 2's review. **Phase-unrelated**: **0** producer files
    appear in P4's whole 36-file diff (control-positive: 36 total).

**None is in P4's scope.** If one goes red it is **not** a P4 regression — re-run,
confirm, move on.

---

## 11 · Phase execution log (Manager-maintained)

⚠ **Durable hand-off state** — the phase must be resumable from the repo alone.
Findings live in `COMMENTS.70.md` (open) / `COMMENTS.DONE.70.md` (closed); **never
`git add` either**. This log records what those cannot: sequencing and
authorization.

### Stage 1 — `367ed559`, review in flight

**`367ed559`** on `719b3b42`. **1178 → 1231** tests both TFMs (+53), 18 files.
Manager-verified: **0** files outside `bindings/dotnet/` against an 18-file
control; Mode A **0** core-tree lines; **0** `COMMENTS`/`agent-memory`/
`.claude/rules` paths in the commit.

**The central hazard shipped correctly.** `ElectLeaders` routes through
`CompleteAggregate<TopicPartition, KafkaException?>` with `get_error(i)` as the
**value** reader; `AlterPartitionReassignments` through `CompleteKeyedVoid` where
the same accessor faults the key. `All()` translates Java's first-present-`Optional`
loop, not `allOf`. **No new walker callable and no new `Accessors` field.**

**Critic 70 round 1: 0 High, 2 Medium, 4 Low.** All five Actor injections
re-measured and held exactly (10/2/3/1 RED + host abort); reflection walk confirmed
**purely additive** (988 → 1022, every line `>`); both recorded deviations judged
sound.

#### ⚠ A plan obligation that was NOT fully satisfiable, and was split correctly

§6.2 asked one test to pin both (a) a non-null per-partition error becoming a **map
value** and (b) `electLeaders` **routing** through the aggregate walker. **(a) is
unobtainable through the mock**: the Rust mock fails `electLeaders` outright,
faithfully mirroring Java's `MockAdminClient.java:797`
`UnsupportedOperationException("Not implemented yet")` — **Manager-verified at both
the Java line and the Rust mirror**. So no `ElectLeadersResult_t` carrying a
per-partition error exists without a broker.

The Actor measured (a) over the **byte-identical twin root**
(`AlterPartitionReassignmentsResult_t`) and pinned (b) separately. **The Critic
verified the substitution is sound for the claim it was made for.** Recorded
because the plan's wording implied a single test that cannot exist.

#### The two Mediums

  - **70.1 — the walker's own remarks assert something P4 falsifies, and the
    staleness points AT the central hazard.** `KeyedResultMarshal.cs:49-54` /
    `:298-305` generalize from `ListTopicsResult` (`get_error` → **0** hits,
    control `get_value` → 1) to shape 3 as a *category*; **`ElectLeadersResult`
    declares `get_error` (2 hits) while being shape 3.** The walker is at **0**
    diff lines against a 313-line `AdminCallbacks.cs` control — the amendment
    landed only on the copy. ⚠ **It tells a maintainer that a result with
    `get_error` cannot be shape 3 — precisely the "correction" injection 1 shows
    costs 10 RED.**
  - **70.2 — `electLeaders`' own walk readers are unguarded.** Swapping
    `FromBorrowedHandle` → `FromHandle` at `AdminCallbacks.cs:716`, and replacing
    `ElectLeadersKey` with a constant, both leave **1231/1231 PASS**; the same swap
    at `KeyedResultMarshal.cs:249` **aborts the host** (control-positive). The test
    exercises a hand-written **mirror**, not production's lambda, **while its
    remark claims the direction was measured "here"** — the
    claim-doesn't-match-what-is-pinned class (69.2 / 69.6).

#### Process notes worth keeping

  - ⚠ **Trap 7 fired LIVE twice this round** — both aborted runs printed
    `Passed! - Failed: 0` (at 134 and 374 tests). The `0 Error(s)`-first discipline
    is what kept them interpretable.
  - ⚠ **The Mode-A control must be chosen so it would actually be non-empty.** The
    P3 range is **not** a valid control for a Mode-A diff, because it is *also*
    empty; the Critic used `95b3a7cc` (95 files) instead. A control that cannot
    fail proves nothing.
  - ✅ **The Critic DROPPED its own candidate rule after checking its premise** —
    it found `PLAN.md:331-332` already scopes the prose sweep to "newly added
    comment lines", making 70.1 a scope miss *inside* an existing instruction
    rather than a missing rule. **This is the exact discipline P3's final round
    failed** (a rule proposed on a premise that `bindings/dotnet/CLAUDE.md:552`
    refuted). Recorded as a positive.

### Manager-carried, for phase close

**Candidate ruling (NOT adopted, raised by Critic 70):** widen **§6.1 item 8** —
the mechanical prose sweep — from "newly **added** comment lines" to "added **or
falsified**". Origin: 70.1 was invisible to the sweep because the falsified text
was **pre-existing and untouched** by the commit that falsified it. Rule files are
off limits to automated agents, so this is the maintainer's to adopt; it is a
**`PLAN.md` §6.1 edit**, which is Manager-owned, so it can be applied here if ruled.

### ⏸ Stage 2 NOT started

`ListPartitionReassignments` + `ListOffsets`. Starts only after Stage 1 closes with
no open approved issue.

### ⚠ Manager-owned plan defect — §6.2's `ElectLeaders` test obligation was UNSATISFIABLE

**Recorded as a plan-wording defect on the Manager, NOT an Actor deviation.**
Ruled 2026-09-10.

§6.2 asked a **single** test to pin two different things: (a) a non-null
per-partition error becomes a **map value**, and (b) `electLeaders` **routes**
through the aggregate walker. **(a) cannot be obtained through the mock at all** —
the Rust mock fails `electLeaders` outright, faithfully mirroring Java's
`MockAdminClient` `UnsupportedOperationException("Not implemented yet")`, so no
`ElectLeadersResult_t` carrying a per-partition error exists without a broker.

**The Actor was right to split it** — a byte-identical-twin-root test for the value
claim, a separate routing test — and the Critic verified the substitution sound.
**The Manager should have anticipated the mock limitation during the scoping pass**,
where every other RPC's mock support *was* checked. §6 already carries the standing
instruction *"Verify each RPC's mock support before relying on it… Do not assume
either way"* — this plan failed its own instruction.

**Generalization for later phases:** when a test obligation names two properties,
check that a **single** artifact can exhibit both. If the mock cannot produce the
shape one property needs, the obligation is two tests, and the plan should say so
rather than leaving the Actor to discover it and justify a split.

### ✅ Critic 70's rule-drop discipline — noticed and valued

Critic 70 drafted a candidate rule, **checked its premise against the cited
document before proposing it**, found `PLAN.md:331-332` already scoped the sweep,
and **withdrew it** — re-characterising 70.1 as a scope *miss inside an existing
instruction* rather than a missing instruction.

**This is the exact discipline P3's final round failed**, where a rule was proposed
on the ground that a phrase appeared in no table while
`bindings/dotnet/CLAUDE.md:552` carried it verbatim — and which would have been
adopted on stated confidence alone. Recorded as a positive, and relayed to the
Critic.

### Stage 1 fix cycle — `c5d81916`; the widened sweep found MORE on its first use

**`c5d81916`** (`fixup!` → `367ed559`). **1236** tests both TFMs. All **7** findings
closed in `COMMENTS.DONE.70.md`, none disputed. Mode A **0** lines over the core
paths.

#### ⚠✅ §6.1 item 8's widening paid for itself IMMEDIATELY — on the very fixup that motivated it

Applied for the first time, to the commit that caused the widening, the
**falsified** pass found **two further instances of the same defect class** that
finding 70.1 had not named:

  - **The shape-1 accessor set was stated as universally
    `count`/`get_key`/`get_value`/`get_error`.** Manager-verified false in **two**
    distinct ways: `DeleteRecordsResult` has **neither** (`get_key` 0,
    `get_value` 0), and `DescribeLogDirsResult` / `DescribeReplicaLogDirsResult`
    have **no `get_key`** (0) while having `get_value` (1) — against a
    control-positive of 1/1 on `CreateTopicsResult`.
  - **"Three result shapes"** sat over a now **four**-item list, since P3 added
    sub-shape 3b.

**Both were pre-existing text in files this fixup did not otherwise need to
touch.** The lines-added pass could not have seen either. This is the strongest
possible evidence for the widening: it did not merely close 70.1, it found the
same class twice more on first use.

**Shape 2's "no `_get_value`" statement was re-checked and HOLDS** —
Manager-verified independently rather than accepted: **0** across all five members
(`DeleteTopics`, `CreatePartitions`, `AlterConfigs`, `AlterReplicaLogDirs`,
`AlterPartitionReassignments`), control-positive **1** on `DescribeConfigsResult`.

#### The bar for closing 70.1/70.2 is higher than "the named sites are fixed"

  - **70.1** — the Critic must verify the **corrected invariant** holds across
    every shape bound in P1–P4, not merely that the three named sentences changed.
    The replacement statement is *"shape membership is decided by the **Java return
    type**; the header decides the **mechanics**"* — the question is whether that
    is true generally, or just true of the cases caught this round.
  - **70.2** — the fix changed **shared machinery**, not only a test:
    `AdminCallbacks.BorrowedOptionalError` and `TopicPartitionKey` are new shared
    factories other RPCs' readers may now route through. Three things to confirm:
    (a) the twin-root test drives **production's own body**, not a second mirror
    under a new name; (b) `AdminP4ReaderWiringTests` — which reads captured
    `DllImport` `EntryPoint`s and asserts no two readers share a set — **actually
    throws** when a refactor stops capturing; (c) the factories still run at static
    init with **no per-walk allocation**.

#### ⚠ §13 trap 7 fired LIVE again — third-or-fourth confirmed sighting

Another aborted run printing `Passed!` alongside the abort. **The trap is doing
exactly the job it exists for.** No action needed beyond the record — but the
repeat sightings are why §9.1 item 7 is a **hard requirement, not advisory**: at
this frequency, an advisory would have been skipped by now.

#### Closed — do NOT re-litigate

The **§6.1 item 8 widening** (Manager-applied), the **§6.2 plan-defect
attribution** (Manager's, not the Actor's), and the **Mode-A-control observation**
(settled; no process change — the `bindings/dotnet/` diffstat control is never
empty and does discriminate) are all decided. If raised fresh, point at these
entries rather than re-deriving them.

### Stage 1 round 2 — 7 findings re-verified closed, 3 new Lows (70.8/70.9/70.10)

#### ⚠✅ The invariant now has a STATED, SWEEP-VERIFIED EXCEPTION — a stronger position than a corrected sentence

**70.9 — `createTopics` is the one RPC where the routing rule is undecidable from
the PUBLIC accessor.** Manager-verified line-for-line:

  - `CreateTopicsResult.java:33` — the **stored** field is
    `Map<String, KafkaFuture<TopicMetadataAndConfig>>` → **shape 1**.
  - `:43-45` — the **public** accessor is `Map<String, KafkaFuture<Void>> values()`,
    narrowed by `thenApply(v -> null)` → the **shape-2 signature**.

⚠ **`PLAN.md` §9 currently tells the reader to prefer the public accessor, which is
exactly backwards for this one case.** The fix is not to reverse the general rule
but to **name the exception**: route by the **stored field's** future type, which
may differ from what the public accessor's signature implies — and cite the sweep
(**16 bound results checked; `createTopics` the only one with a `thenApply`-narrowed
accessor**) so a reader need not re-derive it.

**This is the durable improvement of the round.** The taxonomy doc no longer merely
states a corrected general rule — it **names its one known edge case**, so the next
phase cannot be silently wrong about it again.

**70.8 — the counter-example list was one short, and the FRAMING was slanted.**
`DescribeConfigsResult` also lacks `get_key` (it has `get_key_name` /
`get_key_type` instead) — Manager-verified: `get_key` **0**, `get_key_name` **1**,
against a control-positive of `get_key` **1** on `CreateTopicsResult` and
`DescribeTopicsResult`. With the full sweep, **4 of the 6 bound shape-1 members
lack `get_key`**. ⚠ **So "has `get_key`" is the MINORITY case.** Restate the framing
accordingly — appending one more name to an already-slanted sentence would leave
the emphasis wrong, which is how the original became false.

**70.10 — the wiring test's comment names the wrong mechanism.** It credits the
`?? throw` at `:148-151`; the Critic's IL probe on net10.0 shows a static lambda
compiles to `<>c` (**not** null), so that throw never fires — `Assert.NotEmpty` at
`:163` is what actually catches the refactor. Correct the comment to the true
mechanism.

#### ✅ §9.1 item 10 applied to the ACTOR's claim — the pattern to keep repeating

The Critic's **fifth** re-measurement broke the shared `TopicPartitionKey` body and
measured **11 RED across both donor RPCs**. The Actor's reasoning — *"it is caught
there, therefore it is caught here too"* — was a **reasonable inference and still an
assertion**.

⚠ **Shared-code claims about OTHER call sites get their own injection, not
inherited confidence.** That is precisely §9.1 item 10 turned on the Actor's own
claim rather than on a brief's, and it is the generalization worth carrying: when a
fix consolidates behaviour into shared machinery, each *donor* site needs its own
measurement, because "the shared body is guarded" does not establish "every caller
routes through it".

### Stage 1 final fix pass — `89e9e468`; and 70.10 became a BETTER result than the finding

**`89e9e468`** (`fixup!` → `367ed559`). All three Lows closed. **Scope confirmed
doc-only** — 2 files (`KeyedResultMarshal.cs` +39/−4, entirely inside doc comments;
`AdminP4ReaderWiringTests.cs`), and the only non-`///`/non-`//` added lines are a
**string-literal continuation inside an exception message**. No control flow, no
assertion logic. **The narrow re-check scope therefore stands** — no widening.

#### ⚠✅ 70.10's outcome upgraded the CLAIM, not just the comment — the round's best result

The finding said the `?? throw` at `:148-151` was dead and `Assert.NotEmpty` at
`:163` was doing the work. The Actor's own probe measured **both branches as exact
complements**:

  - a static **lambda** → compiles to `<>c` (**not** null) → trips
    **`Assert.NotEmpty`**;
  - a static **method group** → **null** → trips the **`?? throw`**.

⚠ **So the `?? throw` is not dead code needing resuscitation — it is a LIVE branch
guarding a DIFFERENT FAILURE SHAPE than the one 70.10 pointed at.** The fix turned
an apparent **redundancy** into a confirmed **case-split**.

**That is categorically different from patching a wrong comment**, and it is the
distinction worth carrying: when a guard *looks* dead, the measurement that proves
it dead and the measurement that proves it guards another case are the same
experiment — **run it before deleting anything.** An "optional ask" to make a
supposedly-dead path reachable turned out to need **no code change at all**,
because the thing that looked broken was not.

#### ✅ 70.9's sweep was RE-RUN, not cited — skepticism applied to agreement

The Actor checked **all 16 bound results itself** for the `thenApply`-narrowing
property rather than trusting the Critic's count, and got the same answer: **one
member, not a family.**

⚠ **That is the correct level of skepticism applied to a finding it was already
inclined to agree with.** This milestone's five unverified-premise instances were
all cases where someone accepted a claim they agreed with. **Re-verify agreement,
not only disagreement** — agreement is where verification feels least necessary and
is therefore skipped.

#### ✅ `PLAN.md` §9 now carries the exception (Manager-applied, 2026-09-10)

The Actor flagged that §9 still pointed readers at the **public accessor** for
routing — the thing 70.9 disproved for `createTopics` — and **correctly declined to
edit it**, since `PLAN.md` is the Manager's artifact. **Now fixed**: §9 states both
rules and separates the questions they answer — *what does the binding publish?* →
the public accessor; *which walker callable does it route through?* → the **stored
field** — with the 16-result sweep cited.

⚠ **Left unfixed, this would have been the exact defect shape the whole round was
chasing: the plan and the code disagreeing about the one case that matters.**

### ✅ STAGE 1 CLOSED (2026-09-10)

**Chain: `367ed559` → `c5d81916` → `89e9e468`.** 4 review rounds, **10 findings**
(7 round-1 + 3 round-2 doc-accuracy), **0 High throughout**, all closed.
**1236/1236** both TFMs. Manager-verified: Mode A **0** core-tree lines against a
control-positive of 21 files / +3938 / −61; **0** open findings in
`COMMENTS.70.md`.

### Stage 2 — `ListPartitionReassignments` + `ListOffsets`

**Clean start confirmed:** **0** `ListOffsets*` and **0**
`ListPartitionReassignments*` files under `Admin/`, against a control-positive of
**2** `ElectLeaders*` files from Stage 1.

⚠ **The false parallel was real and worth pre-empting.** "Reassignments" in the
name does **not** imply Stage 1's shape — Manager-verified from the header:

| Result | `get_error` | `get_value` | Shape |
|---|---|---|---|
| `AlterPartitionReassignmentsResult` (Stage 1) | **1** | **0** | **2** — per-key void |
| `ListPartitionReassignmentsResult` (Stage 2) | **0** | **1** | **3** — aggregate, no per-key error |
| `ListOffsetsResult` (Stage 2) | **1** | **1** | **1** — per-key value |

**Three different shapes across two similarly-named RPC families.** This is the
same class as Stage 1's identical-accessor-set twins, inverted: there the *sets*
matched and the *shapes* differed; here the *names* match and both the sets and the
shapes differ.

### Standing carry-forward into Stage 2 (not merely logged — binding)

  - **§6.1 item 8 (widened)** — sweep for comment lines **added OR falsified**.
    Paid for itself twice on first use.
  - ⚠ **§9.1 item 10 applies to AGREEMENT, not just disagreement.** Actor 70 re-ran
    the Critic's own 70.9 sweep on a finding it already agreed with, rather than
    citing it. **Re-verify claims you agree with** — this milestone's entire run of
    unverified-premise instances were cases where someone accepted a claim
    *because* they agreed with it, which is exactly where verification feels least
    necessary.
  - ⚠ **A guard that "looks dead" needs the same measurement as one that looks
    broken, before either conclusion is written down** (70.10). The experiment that
    proves a branch dead and the one that proves it guards another case are the
    **same experiment** — run it before asserting either.
  - **`PLAN.md` §9** now separates *what the binding publishes* (public accessor)
    from *which callable routes it* (**stored field**), with `createTopics` named as
    the sole exception from a 16-result sweep. **Point agents at §9 directly rather
    than letting Stage 2 re-derive it.**

### Stage 2 — `cbc99b67`, review in flight

**`cbc99b67`** on `89e9e468`. **1297/1297** both TFMs (Stage-1 floor 1236).
Manager-verified: Mode A **0** core-tree lines for the **whole phase**
(`719b3b42..HEAD`) against a control-positive of 36 files / +7612 / −63.

#### ✅ RULED 2026-09-10 — `OffsetSpec`'s CLOSED hierarchy is a deliberate, maintainer-approved divergence from Java (DoD §7)

C#'s `OffsetSpec` hierarchy is **closed/sealed**; Java's is an **open**
`public class OffsetSpec`. **This is approved as built. Do not reopen it, and do
not ask for it to mirror Java's extensibility.**

**The evidence, Manager-verified at the source.**
`KafkaAdminClient.java:5176` `getOffsetFromSpec` tests six named subtypes with
`instanceof` and then ends:

```java
        }
        return ListOffsetsRequest.LATEST_TIMESTAMP;
    }
```

**An unconditional fallthrough — no exception, no log.** So in Java today, an
incorrectly-extended or future-unknown `OffsetSpec` subclass is **silently
reinterpreted as "give me the latest offset."** That is a live
silent-wrong-answer footgun in the Java client, not a hypothetical.

**The tradeoff, stated plainly for whoever reads this later:**

  - **Gained:** the silent-fallthrough class is eliminated **entirely** — an
    unknown spec cannot be silently reinterpreted, because it cannot be
    constructed.
  - **Cost:** a genuine future `OffsetSpec` addition (a new Kafka version, or a
    Rust-core addition) needs a **binding change** rather than being silently
    absorbed. ⚠ That cost is the *point*: "silently absorbed" is precisely the
    behaviour being rejected, and a compile error at the binding is strictly more
    visible than a wrong offset at runtime.

**Settled — do not let a reviewer re-litigate it as a shape defect.** If raised,
point here.

#### Stage 2 review — what Critic 70 must establish independently

  - **Re-verify the shape table from the header** — three-way re-confirmation
    (Manager, Actor, Critic) is the standard this phase has held throughout.
    Neither the Actor's numbers nor the Manager's are a substitute.
  - ⚠ **The transposition injection (`-4` ↔ `-5` sentinels) is the single most
    important result to re-run.** It failed **only its own two theory cases — 2 RED
    out of 1297**. That is the strongest available evidence for **"no sampling"**
    on `OffsetSpec`'s seven kinds: a sampled check would very plausibly have missed
    a two-sentinel swap entirely.
  - ⚠ **A process gap the Actor caught in ITSELF, worth generalizing:** a
    revert-verification **grep** for one injection found nothing *because that
    injection added no marker* — the injection was still live, and only a real
    **accessor-count check** caught it. **A marker grep is not a revert.** The
    Critic must check whether **any other injection this round has the same blind
    spot**; absence of a marker is not evidence of absence of an injection.
  - **All seven `OffsetSpec` kinds walked explicitly** in the reflection, seam and
    round-trip tests — confirm **none are sampled or grouped**.
  - **Spot-check the two "already resolved" discrepancies** rather than accepting
    "consistent with Stage 1" as self-certifying: `IsolationLevel` validated
    managed-side (matching Stage 1's `ElectionType` precedent), and the
    `DistinctPartitions` dedup shared with Stage 1.
  - **No new Mode-B gap** — confirm against P3's §15 list, which should remain at
    its existing two entries, unchanged.

### Stage 2 round 4 — 1 Medium (70.12), 1 Low (70.11). Both guard/doc-only.

**70.12 [Medium] — the 70.2 class, REOPENED BY NEW CODE.** Now a standing
checklist item: **§6.1 item 9**. Manager-verified: the guard file is **absent from
the Stage-2 diff** (0 hits, control-positive 1 for `AdminCallbacks.cs`), and the two
new `TopicPartitionKey`-built readers are at `AdminCallbacks.cs:827`
(`ListPartitionReassignmentsKey`) and `:872` (`ListOffsetsKey`).

⚠ **The lesson is that a guard built to close a class does not extend itself.**
70.2 produced `AdminP4ReaderWiringTests`; Stage 2 added two readers of exactly the
shape it guards and the file never grew. **The fix is the checklist item, not the
two `[InlineData]` rows** — the rows close this instance; the item closes the class.
**And the closure proof is the re-run cross-wire injection going RED, not the file
growing.**

**70.11 [Low] — a stale count inherited via `<inheritdoc>`.**
`AdminCallbacks.cs:962-964` says *"the two shape-3 aggregate walks"*; there are
**three** call sites. Manager-verified with a caution worth recording: a raw grep
for `CompleteAggregateRpc` returns **4** — **1 declaration (`:1278`) + 3 call sites
(`:1325`, `:1361`, `:1384`)**. ⚠ A narrowing filter (`CompleteAggregateRpc<`)
returns **1**, the declaration, and would have been *more* wrong than the raw count.
**The line-numbered listing is what settled it** — a small live instance of *"a
filtered grep is not a count"*, in both directions. Fix at the source so every
`<inheritdoc>` site downstream reads correctly — **fix the class, not the
instance**, as 70.1 required.

#### ✅ Two results earned this round

  - **Three-way independent shape-table confirmation — Actor, Manager, Critic —
    landed IDENTICAL numbers all three times.** The discipline is now
    **load-bearing rather than aspirational**: it is what would have caught a
    Stage-2 false parallel, and it reported clean because there was none.
  - ⚠ **The Critic swept its own injections for the marker-grep blind spot it was
    asked to check, found 4 of 5 carry NO marker, and correctly reframed that as
    "the normal case" rather than a red flag.** **Record the reframing, not just the
    sweep:** the fix is **NOT** "always add a marker". The fix is **verify by
    repository state, not by grep** — an accessor count, a build, a test outcome.
    A marker is one convenient way to make a revert checkable; **its absence is
    unremarkable, and treating marker-presence as the standard would replace a real
    check with a ritual.**

### Stage 2 final fix pass — `fca02254`; and §6.1 item 9 became STRUCTURAL

**`fca02254`** (`fixup!` → `cbc99b67`). **1300/1300** both TFMs. Scope **doc/test-only**:
`AdminCallbacks.cs` **0** non-comment lines; `AdminP4ReaderWiringTests.cs` 10.
Manager-verified: Mode A **0** core-tree lines for the whole phase against a
control-positive of 36 files / +7709 / −63.

#### ⚠✅ The Actor turned a PROCESS obligation into a STRUCTURAL one — beyond what was asked

Two `[InlineData]` rows were sized. The Actor also added
**`TheTrackedSet_CoversEveryFactoryBuiltReader`** — a **mechanical completeness
assertion** discovering every `AdminCallbacks` field whose delegate **closes over a
`DllImport`** (what factories do; hand-written `static` lambdas do not) and
asserting it equals the tracked guard set.

**That converts §6.1 item 9 from "remember to add a row" into "a missing row fails
the build on its own."** It verified the discovered set **empirically** against the
real six rather than assuming it from the predicate, and proved the assertion
bites: a seventh factory-built reader added without a guard row → **1 RED**.

⚠ **A checklist item that depends on memory is one a future phase will miss — this
is the strictly better form, and it is the right generalization of 70.12.** The
rows closed the instance; the checklist item closed the class; **the completeness
assertion closes the class mechanically.**

#### ⚠⚠ COUNT DISCIPLINE — a filtered grep can UNDER-count as easily as over-count

Two live instances **in this phase, one in each direction**, both Manager-made:

| Attempt | Returned | Truth | Why it was wrong |
|---|---|---|---|
| `grep -c "= TopicPartitionKey("` | **0** | 5 call sites | The declarations **wrap** — the factory call sits on the line *after* the `=` |
| `grep -c "CompleteAggregateRpc<"` | **1** | 3 call sites | Matched only the **declaration**, which carries the generic parameters |
| `grep -c "CompleteAggregateRpc"` | **4** | 3 call sites | Included the declaration |

**The line-numbered listing is authoritative**, and **declaration must be separated
from call sites** before any count is quoted. The correct reading:
`TopicPartitionKey(` → 6 = 1 declaration (`:338`) + **5** call sites (`:506`,
`:753`, `:809`, `:828`, `:873`); `BorrowedOptionalError(` → 2 = 1 declaration
(`:316`) + **1** call site (`:793`); **six factory-built readers**, matching the
Actor's claim.

⚠ **"A filtered grep is not a count" is now proven in BOTH directions.** A
narrowing filter under-counted to 0 and to 1; an unfiltered one over-counted to 4.
Neither is a count — **only the listing is.**

#### 70.11's resolution — de-numbered rather than re-numbered

The fix **removed the count entirely** from the per-field comment rather than
updating "two" → "three", because **`<inheritdoc>` copies prose verbatim**, so a
hardcoded count in a base doc goes stale on **every future inheritor** — which is
literally how Stage 2's own field inherited the wrong count.

⚠ **And the Actor deliberately LEFT ALONE a sibling comment** (*"the two
sub-shape-3b walks"*) after checking it against the same listing and confirming it
still holds — **it did not "fix" it reflexively just because it shared a similar
shape.** That restraint is correct *provided the count is right*, which is why the
Critic was asked to re-derive it rather than accept it.

### ✅ STAGE 2 CLOSED — Critic 70 round 5. **M15/P4 COMPLETE.**

#### The discovery predicate was STRESS-TESTED, not merely spot-checked

The Critic did not only check for over-broadness — it **built a concrete
under-broad case it expected to slip past** the scan (a factory whose lambda
captures only managed wrappers) and **measured it: 1 RED, caught.**

**The reason is structural, not incidental:** Roslyn hoists all locals and
parameters of one scope into a **single `<>c__DisplayClass`**, so any factory that
takes its accessors **as parameters** necessarily has them in the same display
class its lambda closes over. **That is a proof of completeness for this
codebase's shapes — not an absence of counterexamples.**

⚠ **Record the caveat exactly, because the guarantee is conditional:**
completeness rests on **"factories take their accessors as parameters."** If a
future phase writes a factory that closes over its accessors **indirectly** — via a
helper object rather than raw parameters — **the guarantee must be re-derived, not
assumed to still hold.**

#### 70.13 [Low, record-keeping] — folded into this archive, no separate fix cycle

Two corrections to `COMMENTS.DONE.70.md`, both Manager-verified, **conclusion
unaffected**:

  - **The sweep count understates itself:** `:327` reads *"21 added comment
    lines"*; the measured figure is **48**.
  - **The `CompleteAggregateRpc` citations are `+7` stale against HEAD.** The
    record cites `:1278` (declaration) and `:1325`; HEAD has **`:1285`** and
    **`:1332` / `:1368` / `:1391`** — verified by line-numbered listing, exactly
    seven lines' drift.

⚠⚠ **This is the milestone's running lesson appearing one last time, in the
record-keeping ABOUT that lesson: an evidence record's own counts and line
citations go stale the moment code moves under them — exactly as inline comments
do.** A closed finding is not a frozen fact. **Prefer a stable anchor (a symbol
name, a quoted line) over a line number in any record meant to outlive the commit
that produced it.**

#### The count-discipline lesson, final form

**A filtered grep can UNDER-count or OVER-count. Only a line-numbered listing is a
count**, and **declaration must be separated from call sites** before any figure is
quoted. Proven three times this phase, all Manager-made, in both directions:
`= TopicPartitionKey(` → **0** (declarations wrap); `CompleteAggregateRpc<` → **1**
(matched only the declaration); bare `CompleteAggregateRpc` → **4** (included it).

#### §10.1 gains a second false-flake

`PublicProducerDeliveryCallbackAllocationBudgetTests.CallbackSend_AddsOnlyTheRegistration`
(79 B vs a 64 B budget) — **0** producer files in P4's 36-file diff. Added so a
future round does not burn a cycle on it, as with the Stage-1 producer flake.
