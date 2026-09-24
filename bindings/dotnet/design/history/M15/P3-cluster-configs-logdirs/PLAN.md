# M15/P3 — Cluster, configs, log dirs (8 RPCs, one phase)

**Status:** ✅ **APPROVED 2026-09-09** by the maintainer. Design decisions
**D11-D15 are RULED** (2026-09-09, §2) and must NOT be revisited by an Actor or
Critic. Agent number **N = 69**; the actor-critic loop is authorized to start.

**Agent number: N = 69.** Comment files are `COMMENTS.69.md` /
`COMMENTS.DONE.69.md`.

**Parent roadmap:** the M15 admin roadmap under `bindings/dotnet/design/current/`
(approved 2026-09-08, decisions D1–D6). ⚠ **Deliberately UNTRACKED** — still the
governing roadmap, still to be kept updated, but **never committed**, so it is
absent from a fresh clone. Named by description rather than by path so this
tracked file carries no citation that cannot resolve.

**Base:** P2b's final commit `0fc2ca9f`. ⚠ **Not `f24add9e`, not `6aa5fc4a`, not
`139b7064`** — P1 was squashed to `6aa5fc4a`, P2a to `139b7064`, P2b to
`0fc2ca9f`. Mode-A proof runs against **`0fc2ca9f`**.

**Mode:** **A**. No Rust, no new ABI function, no `cbindgen.toml` change. All
eight entry points and every accessor this phase reads are already exported —
verified in §1. The one Java member with no ABI accessor (`isCordoned`) is
**ruled out of scope**, not worked around (§7).

**Branch:** `prashah_dev_dotnet_admin`. Do NOT create a branch, merge, or rebase.

⚠ **Authorization is per-phase.**

---

## 0 · Roadmap corrections — found during this scoping, APPLIED to the roadmap on disk

Four stale claims, all verified with control-positive greps, all now fixed in
`design/current/PLAN-M15-admin-client.md` (**which stays UNTRACKED — never
`git add` it**). Recorded here because this tracked file is the durable record
and the roadmap is not.

1. ⚠ **The P3 row carried `N = 68`, colliding with the closed P2b record**
   (`COMMENTS.DONE.68.md` already exists under
   `design/history/M15/P2b-list-partitions-records/`). The amendment below the
   table was already correct — P3…P9 = 69…75 — and the table simply had not been
   updated.

   **Root cause: a table of *literal* agent numbers that must be hand-edited at
   every split, and has now gone stale once per split.** Fixed structurally per
   **D11**: the `N` column is **removed from the table**, and a single
   `§8.1 Agent numbering` block above it states the **rule** plus the ledger it
   derives. One source, not ten.

2. **§8's sizing note — "1–2 in P1, 3 in P2, 5 in P3, 4 in P5, 6 in P8" — is a
   *result-shape first-appearance* note, not a phase-size budget.** Shape 5 does
   first appear in P3; the row lists **8 RPCs**. The sentence has been reworded so
   it cannot be read as a size claim again.

3. ⚠ **The P3 row listed `ClusterDescription` as a type to create. Java has no
   such class, and neither does the Rust core.** `find kafka -name
   "ClusterDescription.java"` → **0**, control-positive `find kafka -name
   "TopicDescription.java"` → **1**; `src/admin/cluster_description.rs`
   **absent**, control-positive `src/admin/topic_description.rs` **present**.
   Java's `DescribeClusterResult` exposes four futures directly
   (`DescribeClusterResult.java:49, :59, :66, :74`) and the Rust core mirrors it
   exactly (`src/admin/describe_cluster_result.rs:46, :53, :58, :66`). Creating a
   public `ClusterDescription` would be a `definition-of-done.md` §7 violation.
   Removed from the row (**D12**).

4. **The P2b row said "991 tests".** That was the round-2 figure; two more landed
   in the final round
   (`design/history/M15/P2b-list-partitions-records/COMMENTS.DONE.68.md:141-142`).
   HEAD is **993** (`design/current/STATUS.md:19`). Corrected.

---

## 1 · The ABI read — every accessor set, checked against §4.4

Per the roadmap's own instruction ("treat each phase's ABI read as LOAD-BEARING,
not as a formality"), here is every P3 `*Result_t` accessor set, read from
`target/include/confluent_kafka.h`, with its §4.4 shape and whether the shipped
walker can express it **unchanged**.

| RPC | Result type | Accessors (verbatim from the header) | §4.4 shape | Key | Value | Walker unchanged? |
|---|---|---|---|---|---|---|
| `describeCluster` | `DescribeClusterResult` | `node_count`, `get_node`, `controller`, `authorized_operation_count`, `has_authorized_operations`, `authorized_operation`, `cluster_id`, `destroy` | **5** (first use) | — none | — | **N/A — not a table walk** |
| `listConfigResources` | `ListConfigResourcesResult` | `count`, `get_type`, `get_name`, `destroy` | **matches none — sub-shape 3b** | — none | `ConfigResource` | **NO — one new callable** |
| `listClientMetricsResources` | `ListClientMetricsResourcesResult` | `count`, `get_name`, `destroy` | **matches none — sub-shape 3b** | — none | `ClientMetricsResourceListing` | **NO — same new callable** |
| `describeConfigs` | `DescribeConfigsResult` | `count`, `get_key_type`, `get_key_name`, `get_value`, `get_error`, `destroy` | **1** | composite `ConfigResource` | `Config` (handle) | **YES** |
| `incrementalAlterConfigs` | **`AlterConfigsResult`** | `count`, `get_key_type`, `get_key_name`, `get_error`, `destroy` | **2** | composite `ConfigResource` | — void | **YES** |
| `describeLogDirs` | `DescribeLogDirsResult` | `count`, `get_broker`, `get_value`, `get_error`, `destroy` | **1** | scalar `int` | `LogDirDescriptionMap` (nested map handle) | **YES** |
| `alterReplicaLogDirs` | `AlterReplicaLogDirsResult` | `count`, `get_topic`, `get_partition`, `get_broker_id`, `get_error`, `destroy` | **2** | composite `TopicPartitionReplica` (3 parts) | — void | **YES** |
| `describeReplicaLogDirs` | `DescribeReplicaLogDirsResult` | `count`, `get_topic`, `get_partition`, `get_broker_id`, `get_value`, `get_error`, `destroy` | **1** | composite `TopicPartitionReplica` (3 parts) | `ReplicaLogDirInfo` (handle) | **YES** |

### 1.1 Five of eight RPCs need NO seam change

⚠ **`incrementalAlterConfigs` has no ABI symbol under that name** — verified,
`grep -c "kafka_admin_IncrementalAlterConfigs" target/include/confluent_kafka.h`
→ **0**, control-positive
`grep -c "kafka_admin_CreateTopicsResult_get_value"` → **1**. The result type is
`kafka_admin_AlterConfigsResult_t`, which is **correct and faithful**: Java's
signature is `AlterConfigsResult incrementalAlterConfigs(...)`
(`Admin.java:501, :530`). **Name the C# type `AlterConfigsResult`.**

Its key is **composite** (`get_key_name(i)` + `get_key_type(i)` → a
`ConfigResource`) and its value axis is shape 2 (void) — a combination §4.4 does
not name. **It nonetheless fits the shipped walker unchanged**, because P2a
generalized the key axis to `Func<IntPtr, int, TKey>` (result handle + index) and
P2b's `DeleteRecords` already proved that form on a composite key:

```csharp
internal static void Complete<TKey>(
    IntPtr result, Accessors accessors,
    VoidKeyedAdminOperation<TKey> operation,
    Func<IntPtr, int, TKey> readKey)
```

So `(get_key_name(i), get_key_type(i)) -> ConfigResource` and
`(get_topic(i), get_partition(i), get_broker_id(i)) -> TopicPartitionReplica` are
both expressible with **no change to `KeyedResultMarshal`, `Accessors`, or any
operation type**. Same for the three shape-1 rows via `Complete<TKey, TValue>`.

**This is P2a's and P2b's seam work paying off, and it is why an 8-RPC phase is
tractable at all.**

### 1.2 The one real seam change: sub-shape 3b, aggregate-over-a-**collection**

`ListConfigResourcesResult` and `ListClientMetricsResourcesResult` match **none**
of §4.4's six shapes:

  - **No `get_error` of any kind** — like shape 3, and for the same reason. The
    header states it for both: *"Java has a single future here, so any failure is
    a call failure and is returned."*
  - **No `get_key`** — and, unlike sub-shape 1c, no composite key either. There
    is no key at all. Java's accessors are
    `KafkaFuture<Collection<ConfigResource>>` (`ListConfigResourcesResult.java:42`)
    and `KafkaFuture<Collection<ClientMetricsResourceListing>>`
    (`ListClientMetricsResourcesResult.java:45`) — **one future over an ordered
    LIST**, not over a map.

Shape 3's `CompleteAggregate<TKey, TValue>` builds a `Dictionary<TKey, TValue>`
and cannot express this. Forcing one — e.g.
`IReadOnlyDictionary<ConfigResource, bool>` — would invent a non-Java public
surface (DoD §7). `ListClientMetricsResourcesResult` makes that plainest: its
only per-index accessor is `get_name(i)`, and the listing **is** the name.

### 1.3 `DescribeCluster` is not a table walk at all

Shape 5 reads a handful of scalars and borrowed children off **one** root, so it
needs **no** `KeyedResultMarshal` callable — it gets its own marshaller, exactly
as `TopicDescriptionMarshal` is `describeTopics`'.

⚠ **`authorizedOperations()` is genuinely nullable, and the ABI says so with a
separate gate**, not with a count of zero. Header, verbatim on
`authorized_operation_count`: *"0 covers both 'the broker did not report them'
(Java yields null) and 'reported, but none authorized'; use
`kafka_admin_DescribeClusterResult_has_authorized_operations` to tell them
apart."* The Rust core agrees:
`pub fn authorized_operations(&self) -> KafkaFuture<Option<BTreeSet<AclOperation>>>`
(`src/admin/describe_cluster_result.rs:66`). **Reading the count alone and
skipping the gate collapses null into empty — a silent behavioural divergence
with no failing test.** `controller()` is nullable too (header: *"or null if
there is none (Java's `controller()` yields null)"*;
`src/admin/describe_cluster_result.rs:53` → `KafkaFuture<Option<Node>>`).

---

## 2 · DESIGN DECISIONS — RULED 2026-09-09

### D11 — P3 ships as a **SINGLE phase**, N = 69 ✅ ruled 2026-09-09

A three-way split (P3a/P3b/P3c) was proposed and **rejected**. P3 is one phase,
one actor-critic round, all 8 RPCs.

**Numbering is therefore unchanged from the roadmap amendment: P3 = 69,
P4…P9 = 70…75. Nothing shifts.**

**The numbering RULE is adopted** in place of the table's literal numbers:
*a sub-phase takes the next free agent number in sequence; every later phase
shifts by the number of extra sub-phases; the phase LABEL never renumbers.*
Applied ledger: P1=66, P2a=67, P2b=68, P3=69, P4=70, P5=71, P6=72, P7=73, P8=74,
P9=75.

⚠ **D6 pre-sanctions a mid-flight split, so no new maintainer ruling would be
needed if this round proves too large.** That escape hatch exists and §3
deliberately keeps it open by sequencing the work into three internally
green, committable stages. **Do not pre-emptively take it** — run the phase as
one round, and raise a split only if the round is actually running long.

### D12 — **No public `ClusterDescription` type** ✅ ruled 2026-09-09

`DescribeClusterResult` exposes four `Task`s directly, mirroring Java's four
`KafkaFuture` accessors. An **internal** payload type carrying the four
marshalled values into `SingleAdminOperation<T>` is fine and invisible; a
**public** `ClusterDescription` is a DoD §7 violation (§0 item 3).

### D13 — `ConfigResource` and `TopicPartitionReplica` at the **root** namespace ✅ ruled 2026-09-09

Both are `org.apache.kafka.common.*` types
(`common.config.ConfigResource`, `common.TopicPartitionReplica`), so both go in
the root `Confluent.Kafka` namespace — **not** under `Admin/`. This follows D10's
`TopicCollection` ruling and matches the shipped `Node.cs`, `AclOperation.cs`,
`TopicPartition.cs`, `Uuid.cs`, all at the root, while `Config.cs` /
`ConfigEntry.cs` (Java `clients.admin`) sit under `Admin/`.

⚠ **This diverges from `confluent-kafka-dotnet`, which puts `ConfigResource` in
`Confluent.Kafka.Admin`.** The divergence was flagged and **accepted**: D1
already established that M15 follows the **Java** shape rather than ckd's.

### D14 — sub-shape 3b gets its **own callable**, with no `Accessors` ✅ ruled 2026-09-09

Per P2b's general rule — *when a defect is "shape is encoded in whether a field is
null", make each shape a distinct callable, never add another nullable
discriminator*. See §4.1.

**⚠ Nothing in P3 may add a nullable field to `Accessors`.** `Accessors` came out
of P2b **smaller** — `count` plus a required, non-nullable `getError` — and must
stay that way. A plan instruction to the contrary is a plan defect; the P2b Actor
correctly refused exactly that (the G2 deviation) and was upheld. P3 is designed
so no such refusal is needed.

### D15 — `LogDirDescription.isCordoned()`: **ship WITHOUT it, Mode A, gap tracked** ✅ ruled 2026-09-09

**This is a ruled deviation, not an oversight and not a defect. See §7 for the
full record — read it before reviewing `LogDirDescription`.**

### D16 — `ConfigResourceType` at the root; `ConfigSource` / `ConfigType` stay **nested** ✅ ruled 2026-09-09

Java's `ConfigResource.Type` cannot keep that name nested in C# without colliding
with `System.Type` at every unqualified use site, so it is flattened to
**`ConfigResourceType`** at the root namespace, beside `ConfigResource` (D13).
By contrast `ConfigEntry.ConfigSource` and `ConfigEntry.ConfigType` **can** nest
cleanly and **stay nested**, matching Java exactly.

### D17 — `ReplicaLogDirInfo` stays **nested** inside `DescribeReplicaLogDirsResult`, with Java-bean `Get*` prefixes ✅ ruled 2026-09-09

Java nests it (`DescribeReplicaLogDirsResult.java:89-117`) and its accessors are
`getCurrentReplicaLogDir()` / `getCurrentReplicaOffsetLag()` /
`getFutureReplicaLogDir()` / `getFutureReplicaOffsetLag()` — the only Java-bean
accessors in M15. **The roadmap row listed it flat; Java wins.**

---

## 3 · Internal sequencing — three stages, each independently green and committable

**The risk lands first.** Five of eight RPCs need no mechanism change (§1.1), and
the single genuine seam change is `CompleteList`. So the walker edit and
everything that depends on it goes in Stage 1, in a small diff, exactly as P1
landed the bridge with one RPC to prove it.

| Stage | Content | Why here | Green boundary |
|---|---|---|---|
| **1** | `CompleteList` (§4.1) · `DescribeCluster` (§4.2) · `ListConfigResources` + `ListClientMetricsResources` (§4.3) · types `ConfigResource`, `ConfigResourceType`, `ClientMetricsResourceListing` | **The only stage that edits `KeyedResultMarshal`.** A defect in the shared mechanism is then found in a 3-RPC diff, not an 8-RPC one. Also lands `ConfigResource`, which Stage 2 keys on. | Build + full suite green; `AdminKeySeamShapeTests` extended and green; commit. |
| **2** | `DescribeConfigs` · `IncrementalAlterConfigs` · `ConfigEntry` **extension** (`Source`/`Type`/`Documentation`/`Synonyms`) + `ConfigEntry.ConfigSource` / `.ConfigType` / `.ConfigSynonym` · `AlterConfigOp` + `AlterConfigOpType` | Zero walker change. Reopens **P1's reviewed `ConfigEntry`** and carries the widest input in M15 (5 parallel arrays). Depends on Stage 1's `ConfigResource`. | Build + full suite green; P1's existing `ConfigEntry` reflection assertions pass **unmodified**; commit. |
| **3** | `DescribeLogDirs` · `AlterReplicaLogDirs` · `DescribeReplicaLogDirs` · types `TopicPartitionReplica`, `LogDirDescription`, `ReplicaInfo`, `DescribeReplicaLogDirsResult.ReplicaLogDirInfo` | Zero walker change. Independent of Stage 2 — shares no type with it. Carries the D15 deviation (§7) and the deepest value tree. | Build + full suite green; commit. |

⚠ **Each stage must reach a genuinely green, committable state** — build, full
test suite with the count confirmed, `dotnet format`, and the Mode-A proof. That
is what keeps D11's escape hatch open: if the round runs long, a split at a stage
boundary is still available. **It is an escape hatch, not the plan.**

⚠ **Stage 2 owes Stage 1 a test it cannot write.** `ListClientMetricsResources`
can only be exercised against an **empty** mock until `IncrementalAlterConfigs`
exists, because the Rust mock creates a client-metrics resource as a *side
effect* of an `incrementalAlterConfigs` against a `CLIENT_METRICS` resource
(`src/admin/mock_admin_client.rs`, test
`incremental_alter_configs_client_metrics_creates_resource` at `:2500`).
**Stage 2 must add the seeded-listing test** — alter a `CLIENT_METRICS` resource,
then assert both `ListClientMetricsResources` and `ListConfigResources` filtered
to `CLIENT_METRICS` return it. **Do not let this drop**; it is the one
cross-stage debt.

---

## 4 · Stage 1 — the seam change and the non-keyed roots

### 4.1 `CompleteList` — the sub-shape-3b callable (D14)

One new method on `KeyedResultMarshal`, additive, touching no existing call site:

```csharp
internal static void CompleteList<TValue>(
    IntPtr result,
    CountAccessor count,
    SingleAdminOperation<IReadOnlyCollection<TValue>> operation,
    Func<IntPtr, int, TValue> readValue)
```

  - **No `Accessors` parameter** — these results declare no `get_error` at all
    (D14), exactly as `CompleteAggregate` takes none.
  - **No key reader** — there is no key.
  - **Any failure faults the one task**: a throw from `readValue` propagates to
    the trampoline's no-throw boundary, which faults the single awaiter.
  - **Order is preserved as the ABI delivers it.** `ListConfigResources` entries
    are documented *"sorted by (type id, name)"*, `ListClientMetricsResources`
    *"sorted by name"*. Java returns a `Collection`, so order is not a contract —
    but do not shuffle it, and do not sort again.
  - Reuses `SingleAdminOperation<TValue>` and therefore `AdminOperation`'s
    `GCHandle` / `SetHandleRef` / `AbandonBeforeSubmit` machinery **unchanged**.
    That base class is the part P1 got right across three review rounds —
    inherit it, do **not** reimplement or "simplify" it.

⚠ **Blast radius — this WILL turn assertions in
`tests/Confluent.Kafka.UnitTests/Interop/AdminKeySeamShapeTests.cs` RED**, which
pins the walker's exact method surface. **That is expected, not a regression** —
the identical thing happened when P2b added its overload and broke that file's
`.Single(m => m.Name == nameof(Complete))` assertion. Extend the assertions to
cover the new callable; **do not weaken them**, and do not re-word P2a/P2b's
forward-looking seam comments to pre-empt the change (re-wording a
forward-looking comparative is what `ffi-marshalling.md §A6`'s round-5 amendment
warns produces the next stale sentence).

### 4.2 `DescribeCluster` (shape 5)

`DescribeClusterResult.java:49, :59, :66, :74`:

```csharp
public sealed class DescribeClusterResult
{
    public Task<IReadOnlyCollection<Node>> Nodes();                         // nodes()
    public Task<Node?> Controller();                                        // controller()
    public Task<string> ClusterId();                                        // clusterId()
    public Task<IReadOnlyCollection<AclOperation>?> AuthorizedOperations(); // authorizedOperations()
}
```

  - **Both nullabilities are contract, not edge cases** (§1.3).
  - Java's `authorizedOperations()` is a `Set<AclOperation>`; `IReadOnlySet<T>`
    post-dates the netstandard2.0 floor, so `IReadOnlyCollection<AclOperation>`
    is the mapping — the substitution already in `bindings/dotnet/CLAUDE.md §4`'s
    idiom map and already used by `ListTopicsResult.Names()`.
  - **Reuse the shipped `Node` and `AclOperation`, both at the root namespace —
    do not declare second copies.** A `NodeMarshal` and an `AclOperation` decode
    path already exist; check whether either is directly reusable before writing
    a new one.

**One source, four projections.** The RPC completes **one**
`SingleAdminOperation<T>` over an **internal** payload carrying all four
marshalled values (D12), and the four public `Task`s derive from it.

  - ⚠ **Follow `ListTopicsResult`'s `Project` helper precedent**
    (`Admin/ListTopicsResult.cs`): an `async` method, **not** `ContinueWith`, so
    a faulted projection carries the same `KafkaException` as the source rather
    than an extra `AggregateException` wrapper.
  - ⚠ **Repeated calls must return the same `Task` instance**, because Java's
    `nodes()` returns the same `KafkaFuture` object every call. Each projection
    must be created once and cached. **Flag the unobserved-faulted-task
    consideration at the site** (four eagerly-created tasks all fault together if
    the call fails) and state which way it was resolved.
  - **Recorded deviation (`definition-of-done.md` §7):** Java's four futures are
    four independent fields; here they derive from one. This is the same
    already-recorded deviation as the roadmap's §4.3 — the ABI settles all four
    together and has no `KafkaFuture` type to express independent timing.
    Deriving them is *stronger* than Java: four projections of one source cannot
    report four different cluster states.

`DescribeClusterOptions` (POCO per D5): `TimeoutMs`,
`IncludeAuthorizedOperations`, `IncludeFencedBrokers` — verify each against
`DescribeClusterOptions.java` before writing.

### 4.3 `ListConfigResources` and `ListClientMetricsResources`

**`ConfigResource`** — new public type, **root namespace** (D13). From
`ConfigResource.java:53, :71, :81, :88, :96, :101, :113, :120`:

```csharp
public sealed class ConfigResource
{
    public ConfigResource(ConfigResourceType type, string name);
    public ConfigResourceType Type { get; }
    public string Name { get; }
    public bool IsDefault();
    // plus Equals / GetHashCode / ToString — value equality is part of Java's shape
}
```

⚠ **Value equality is load-bearing, not decorative.** `ConfigResource` is the
dictionary **key** for Stage 2's `DescribeConfigsResult` and `AlterConfigsResult`.
Getting `Equals`/`GetHashCode` wrong produces a result map whose keys the caller
cannot look up. **Stage 1 lands it and tests it; Stage 2 depends on it.**

**`ConfigResourceType`** — new public enum, **root namespace** (D16). Java's ids
(`ConfigResource.java:36-41`) are `GROUP(32)`, `CLIENT_METRICS(16)`,
`BROKER_LOGGER(8)`, `BROKER(4)`, `TOPIC(2)`, `UNKNOWN(0)`, and the header agrees
verbatim (*"2 = TOPIC, 4 = BROKER, 8 = BROKER_LOGGER, 16 = CLIENT_METRICS,
32 = GROUP"*).

  - **The C# enum's underlying values MUST be those ids** — they cross the ABI in
    both directions as `int32_t`.
  - `get_type(i)` returns **`-1` when the index is out of range** — a value no
    Java `Type` has. **Do not map it to `UNKNOWN` (which is `0`)**; the loop is
    bounded by `count`, so it is unreachable, and a defensive throw is the right
    treatment, mirroring `KeyedResultMarshal.ReadStringKey`.

**`ClientMetricsResourceListing`** — new public type, `Admin/`. From
`ClientMetricsResourceListing.java:25, :29, :34, :42, :47`: a constructor taking
`string name`, a `Name` accessor (see the correction below), plus
`Equals`/`GetHashCode`/`ToString`.

⚠⚠ **CORRECTION (2026-09-09, Manager-owned — the FIFTH plan defect in M15, found
by Actor 69 and verified).** This paragraph originally read *"`public string
Name()` (a **method** in Java — mirror it, as `DeletedRecords.LowWatermark()` was
mirrored in P2b)"*. **That citation was backwards.** The shipped member is
`public long LowWatermark { get; }` — a **property**
(`bindings/dotnet/src/Confluent.Kafka/Admin/DeletedRecords.cs:58`; grep: property
form **1** hit, method form **0**).

The shipped tree's real rule, now that both P2b types have been read rather than
recalled:

  - `DeletedRecords.LowWatermark` is a **property** — the *unforced* case,
    following `bindings/dotnet/CLAUDE.md §3`'s "non-blocking getter → sync
    property" row.
  - `RecordsToDelete.BeforeOffset()` is a **method** — and that is **forced by the
    language**, not a convention: a `public static RecordsToDelete
    BeforeOffset(long)` factory (`RecordsToDelete.cs:49`) and an instance accessor
    of the same name (`:62`) cannot coexist as a static method plus an instance
    property (`CS0102`). Already recorded in the tracked
    `design/current/STATUS.md` ("**forced by the language**, probed and confirmed").

So the shipped convention for an **unforced** admin value-type getter is a
**property**, and `ClientMetricsResourceListing.Name` / `ConfigResource.Type` /
`.Name` / `.IsDefault` are all unforced. **Actor 69 shipped them as properties and
was right to** — the plan was the defect. Do not "fix" them back.

**The two results** — each has **exactly one** public accessor in Java. Do not
add a second view:

```csharp
public sealed class ListConfigResourcesResult
{
    public Task<IReadOnlyCollection<ConfigResource>> All();               // :42
}
public sealed class ListClientMetricsResourcesResult
{
    public Task<IReadOnlyCollection<ClientMetricsResourceListing>> All(); // :45
}
```

⚠ **`listClientMetricsResources` is `[Obsolete]`.** `Admin.java:1821-1824` and
`:1833-1836`: `@Deprecated(since = "4.1", forRemoval = true)`, *"Use
listConfigResources(Set, ListConfigResourcesOptions) instead"*. The C# members
must carry `[Obsolete("...")]` with the same replacement guidance.
**`TreatWarningsAsErrors` is on**, so every internal call site — the `IAdmin`
declaration, `KafkaAdminClient`'s forwarder, `MockAdminClient`'s, and every test —
needs `#pragma warning disable CS0618` / `restore` scoped as tightly as possible.
**This is the most likely source of a build break in Stage 1**; do not suppress
project-wide.

**Input:**

  - `list_config_resources(admin, resource_types, count, timeout_ms, …)` — one
    `const int32_t*` array. ⚠ Header: *"pass NULL or `count == 0` for Java's
    empty set, which means **every supported type**"*, matching `Admin.java:1812`'s
    no-arg default. **Empty must not be rejected** — it is the "all types"
    request. A `?? throw` or an emptiness guard here is the defect.
  - `list_client_metrics_resources(admin, timeout_ms, …)` — no arrays at all.

---

## 5 · Stage 2 — configs (the borrowed-handle value tree)

### 5.1 ⚠ `ConfigEntry` is REOPENED — the P1-foundation edit

The shipped `bindings/dotnet/src/Confluent.Kafka/Admin/ConfigEntry.cs:46` is the
**P1 flattened subset**: `Name` (`:87`), `Value` (`:93`), `IsDefault` (`:96`),
`IsSensitive` (`:102`), `IsReadOnly` (`:105`) — and nothing else, because
`describeTopics`' `TopicMetadataAndConfig` exposes only flattened `config_*`
accessors. Java has **four more**:

| Java | Line | ABI source (net-new) |
|---|---|---|
| `ConfigSource source()` | `ConfigEntry.java:95` | `kafka_admin_ConfigEntry_source` |
| `List<ConfigSynonym> synonyms()` | `:126` | `synonym_count` / `synonym_name` / `synonym_value` / `synonym_source` |
| `ConfigType type()` | `:133` | `kafka_admin_ConfigEntry_type` |
| `String documentation()` | `:140` | `kafka_admin_ConfigEntry_documentation` |

**All four must be added.** Additive, so source-compatible — but it edits P1's
reviewed foundation and must be kept tight.

⚠ **`ConfigEntry.cs:34` and `:67` already carry a recorded deviation** about
which constructor parameters exist. **Read it before touching the file and do not
invalidate it** — extend the record if the new members change its scope, rather
than deleting it. Quantifier drift in exactly this kind of remarks block produced
P1's finding 8 and M14/P1's five-cycle tail.

### 5.2 ⚠ `ConfigSource` / `ConfigType` cross the ABI as **strings**, not ids

**This is the most surprising thing in P3 and the easiest to get wrong.**

```c
const char *kafka_admin_ConfigEntry_source(const kafka_admin_ConfigEntry_t *entry);
const char *kafka_admin_ConfigEntry_type  (const kafka_admin_ConfigEntry_t *entry);
const char *kafka_admin_ConfigEntry_synonym_source(const kafka_admin_ConfigEntry_t *entry, int32_t index);
```

Header, verbatim: *"Returns the config source as Java's enum constant name
(borrowed), e.g. `"DYNAMIC_TOPIC_CONFIG"` or `"DEFAULT_CONFIG"`.
`ConfigEntry.ConfigSource` **has no numeric id in Java**, so the name is the
contract."* Same sentence shape for `type` (*e.g. `"STRING"` or `"UNKNOWN"`*).

⚠ **Contrast every other enum in P3**, which crosses as an `int32_t` wire code
(`ConfigResourceType`, `AlterConfigOpType`, `AclOperation`). **Two conventions
coexist in this phase; mixing them up is the defect to guard against.**

  - The decode is a **string → enum** mapping. Do not invent numeric ids and do
    not rely on `Enum.Parse` matching by accident — assert the mapping explicitly.
  - **Both enums have an `UNKNOWN` member** (`ConfigEntry.java:224` and `:200`),
    the correct landing place for an unrecognised name — that is what Java's own
    `UNKNOWN` is for. **A throw here would be a behavioural divergence**: a broker
    introducing a new source would fail the whole describe instead of degrading.
  - Members, from Java, in declaration order — ⚠ **read `ConfigEntry.java:199-224`
    and take the lists from there, not from this table** (§12):
    - `ConfigType` (`:199-210`): `UNKNOWN`, `BOOLEAN`, `STRING`, `INT`, `SHORT`,
      `LONG`, `DOUBLE`, `LIST`, `CLASS`, `PASSWORD`.
    - `ConfigSource` (`:215-224`): `DYNAMIC_TOPIC_CONFIG`,
      `DYNAMIC_BROKER_LOGGER_CONFIG`, `DYNAMIC_BROKER_CONFIG`,
      `DYNAMIC_DEFAULT_BROKER_CONFIG`, `DYNAMIC_CLIENT_METRICS_CONFIG`,
      `DYNAMIC_GROUP_CONFIG`, `STATIC_BROKER_CONFIG`, `DEFAULT_CONFIG`, `UNKNOWN`.
  - **Both stay nested inside `ConfigEntry`** (D16), matching Java.

### 5.3 `ConfigSynonym` is a nested class with **no handle**

`grep -o "kafka_admin_ConfigSynonym_[a-z_0-9]*"` over the header returns **zero
matches**, control-positive `kafka_admin_ConfigEntry_name` → present. There is no
`ConfigSynonym_t`: synonyms are read through **three parallel indexed accessors
on the parent entry**, bounded by `synonym_count`.

So `ConfigEntry.ConfigSynonym` is a POCO assembled from those, mirroring Java
(`:252` `name()`, `:259` `value()`, `:266` `source()`, plus
`Equals`/`GetHashCode`/`ToString` — **the value-equality members are part of
Java's public shape; do not drop them**).

⚠ **Header: *"Synonyms keep Java's precedence order and are not sorted."***
Precedence order is semantically meaningful — it is why `synonyms()` returns a
`List` and not a `Set`. **Do not sort, reorder, or de-duplicate.**

⚠ `synonym_value(i)` returns null *"if the value is null **or** `index` is out of
range"* — an overloaded null, the same class as `deleteRecords`' `-1` (P2b §3).
The loop is bounded by `synonym_count`, so out-of-range is unreachable; a null
inside the bound is a **genuine null value** and must round-trip as `null`, not
`""`.

### 5.4 `DescribeConfigs` — surface, key reader, value tree

`DescribeConfigsResult.java:43, :50`:

```csharp
public sealed class DescribeConfigsResult
{
    public IReadOnlyDictionary<ConfigResource, Task<Config>> Values { get; }   // values()
    public Task<IReadOnlyDictionary<ConfigResource, Config>> All();            // all()
}
```

⚠ **`All()` here carries the whole map**; `AlterConfigsResult.all()` (§5.5) is
`KafkaFuture<Void>`. The two are one line apart in shape and easy to swap.

**Key reader** — the `DeleteRecordsKey` pattern with two accessors instead of
two, hoisted as a `static readonly` field beside its accessor set (as
`AdminKeySeamShapeTests.EveryReader_IsAHoistedStaticReadonlyField` requires):

```csharp
internal static readonly Func<IntPtr, int, ConfigResource> DescribeConfigsKey =
    static (result, index) => new ConfigResource(
        (ConfigResourceType)NativeMethods.DescribeConfigsResultGetKeyType(result, index),
        KeyedResultMarshal.ReadStringKey(NativeMethods.DescribeConfigsResultGetKeyName(result, index)));
```

⚠ **The comparer is a required constructor argument on `KeyedAdminOperation`,
never inferred.** ⚠ **Note an inconsistency already in the tree**:
`DescribeTopicsResult` is built with
`OfTopicNames(operation.Tasks, operation.KeyComparer)` while `DeleteRecordsResult`
is built with `new DeleteRecordsResult(operation.Tasks)` and **no comparer**.
Pick one deliberately for the new results and say which and why; do not copy
whichever example happened to be open.

**Value tree** — `get_value(i)` returns a **borrowed `const kafka_admin_Config_t*`**.
A new `ConfigMarshal.CopyOut(IntPtr)` walks
`Config_entry_count` → `Config_get_entry(i)` → the nine `ConfigEntry_*`
accessors → `synonym_count` → the three `synonym_*` accessors.

⚠ **Every string is borrowed from the one result root and dies with it.** The
returned `Config` must be **fully owned managed state** before the trampoline's
`finally` destroys the root (ffi §B4 / CLAUDE.md §6.4). Nothing native-backed —
no `IntPtr`, no lazily-read accessor, no cached handle — may survive on the public
object. All strings are the NUL-terminated, callee-owned form (ffi §B3 row 2).

  - `ConfigEntry_value` is nullable (*"sensitive configs come back null"*) →
    `string?`, preserved as `null`.
  - `ConfigEntry_documentation` is nullable (*"or null when the broker did not
    report it"*) → `string?`, preserved as `null`.
  - Entries are documented *"sorted by name"*. **Reuse the shipped `Config` type
    unchanged** (`Admin/Config.cs:64` `Entries`, `:74` `Get(string)` mirroring
    Java's `Config.get(String)`) — only `ConfigEntry` grows.
  - `kafka_admin_Config_find_entry` mirrors `Config.get(String)`, but since the
    whole `Config` is copied out eagerly the managed `Config.Get(name)` already
    satisfies it. **Bind it only if a concrete use appears**; note the deliberate
    omission in the self-review rather than leaving it unexplained.

**Input:** two parallel arrays (`resource_types`, `resource_names`) plus
`include_synonyms`, `include_documentation`. Header: *"An entry with a NULL name
is skipped"* — silently. **Guard at the C# boundary** (ffi §B5, validate before
any pin/marshal) rather than letting the ABI swallow it.
`DescribeConfigsOptions`: `TimeoutMs`, `IncludeSynonyms`, `IncludeDocumentation` —
verify against `DescribeConfigsOptions.java`.

### 5.5 `IncrementalAlterConfigs` — surface and the five-array input

`AlterConfigsResult.java:39, :46`:

```csharp
public sealed class AlterConfigsResult
{
    public IReadOnlyDictionary<ConfigResource, Task> Values { get; }   // values()
    public Task All();                                                 // all() — KafkaFuture<Void>
}
```

Routes through `Complete<TKey>` with a `VoidKeyedAdminOperation<ConfigResource>` —
**no value reader, because there is no `_get_value`**.

**`AlterConfigOp`** (`AlterConfigOp.java:46-100`): constructor
`(ConfigEntry configEntry, AlterConfigOpType opType)`, `ConfigEntry()` and
`OpType()` as **methods** (Java's are methods — mirror them), plus
`Equals`/`GetHashCode`/`ToString`. Op ids are **wire codes** crossing as `int32_t`:
`SET(0)`, `DELETE(1)`, `APPEND(2)`, `SUBTRACT(3)` (`:50, :54, :60, :67`), matching
the header. **Unlike §5.2's enums, these are ids, not names.**

```c
kafka_admin_AdminClient_incremental_alter_configs_async(
    admin, resource_types, resource_names, config_names, config_values, op_types,
    count, timeout_ms, validate_only, callback, user_data);
```

Header, verbatim: *"Java's `Map<ConfigResource, Collection<AlterConfigOp>>`
becomes five parallel arrays, **one row per operation**: row `i` applies
`(config_names[i] -> config_values[i], op_types[i])` to the resource
`(resource_types[i], resource_names[i])`. Rows naming the same resource are
grouped in order."*

Four hazards, each needing an explicit test:

1. **"Rows naming the same resource are grouped in order."** Emit each resource's
   rows contiguously and in the caller's op order. Do not interleave resources.
2. **A NULL `config_values` entry is the null value `DELETE` uses** — `null` is
   meaningful and must reach the ABI as `null`. ⚠ **A `?? string.Empty` anywhere
   on this path is the defect** — the same null-vs-empty class as P2b's
   `NewPartitions` gate (roadmap §7 gate 6).
3. **"A row with a NULL resource name or config name is skipped"** — silently.
   Guard at the C# boundary.
4. ⚠⚠ **"An unknown op-type code fails the whole call with an illegal-argument
   error"**, and the async doc adds that trigger to the **inline** callback path:
   *"It runs synchronously on the calling thread, before this function returns,
   when the RPC cannot be submitted at all (a NULL `admin` handle, **or an
   unknown `AlterConfigOp.OpType` code**)."*

   **This is exactly the case the roadmap's §4.5 correction told later phases to
   look for — and here the extra trigger IS in the header, on this entry point's
   own doc.** Cite the header for it (**not** `src/ffi/admin.rs:62`, which covers
   the family-wide claim cbindgen does not emit). Two consequences:
   `RunContinuationsAsynchronously` is load-bearing on ordinary bad input, and the
   `GCHandle` free must be correct on the inline path. **A test must drive the
   inline path** — an integration test never reaches it.

`AlterConfigsOptions`: `TimeoutMs`, `ValidateOnly` — verify against
`AlterConfigsOptions.java`.

---

## 6 · Stage 3 — log dirs (scalar key, nested-table value)

### 6.1 `TopicPartitionReplica` — new public type, **root namespace** (D13)

From `TopicPartitionReplica.java:33, :39, :43, :47, :52, :66, :78`:

```csharp
public sealed class TopicPartitionReplica
{
    public TopicPartitionReplica(string topic, int partition, int brokerId);
    public string Topic();      // topic()      — METHODS in Java
    public int Partition();     // partition()
    public int BrokerId();      // brokerId()
    // plus Equals / GetHashCode / ToString
}
```

⚠ **Value equality is load-bearing** — this keys **both**
`AlterReplicaLogDirsResult` and `DescribeReplicaLogDirsResult`. Java's `hashCode`
is at `:52`; mirror the fields it combines.

⚠ **Java's `TopicPartitionReplica` has NO constructor-time validation** — a
negative partition is constructible. The Rust core notes this and guards against
the consequence at `src/admin/mock_admin_client.rs:1305-1315`. **Mirror Java: do
not add validation the Java type does not have** (DoD §7). Guard on the *submit*
path instead (§6.4).

### D18 — method vs property for admin value types ✅ RULED 2026-09-09

**An unforced getter is a PROPERTY. The method form is used ONLY where `CS0102`
forces it** — the `RecordsToDelete.BeforeOffset` static-factory collision being the
sole known case.

**So the `// METHODS in Java` comments in the sketch above are WRONG and are
superseded.** `Topic`, `Partition` and `BrokerId` are **properties**. Same for
`ReplicaInfo`'s `Size` / `OffsetLag` / `IsFuture` (§6.2) and `AlterConfigOp`'s
`ConfigEntry` / `OpType` (§5.5). Nothing in Stage 3 re-litigates this.

What is now established, by reading the shipped tree rather than recalling it:

  - `bindings/dotnet/CLAUDE.md §3`'s idiom map says **non-blocking getter → sync
    property**, and that is what the *unforced* P2b type did
    (`DeletedRecords.LowWatermark` is a **property**, `DeletedRecords.cs:58`).
  - The one shipped **method**-form sibling, `RecordsToDelete.BeforeOffset()`, is
    **forced by `CS0102`** — a same-named `public static` factory (`:49`) cannot
    coexist with an instance property. Recorded in the tracked `STATUS.md`.
  - **`TopicPartitionReplica` has no such forcing**: Java declares **0** `public
    static` members on it (control-positive: 8 `public` members total), so nothing
    stops the property form.
  - Counterweight: the shipped `IConsumerCommon` deliberately uses **methods** for
    `Assignment()` / `Subscription()` / `Paused()`, on FDG grounds — each does a
    P/Invoke, can throw, and returns a fresh snapshot. That reasoning does **not**
    reach a pure managed value type like `TopicPartitionReplica`, which is why the
    two families can legitimately differ — but it is why the rule needs stating
    rather than assuming.

**Sketch code blocks elsewhere in this plan that spell these as `Foo()` are stale
against D18; the ruling wins.** Actor 69 shipped `ConfigResource` and
`ClientMetricsResourceListing` as properties in Stage 1 before the ruling landed —
that is now retroactively correct and must not be "fixed" back.

### 6.2 `DescribeLogDirs` — scalar key, nested-table value

`DescribeLogDirsResult.java:41, :50`:

```csharp
public sealed class DescribeLogDirsResult
{
    public IReadOnlyDictionary<int, Task<IReadOnlyDictionary<string, LogDirDescription>>> Descriptions { get; }  // descriptions()
    public Task<IReadOnlyDictionary<int, IReadOnlyDictionary<string, LogDirDescription>>> AllDescriptions();     // allDescriptions()
}
```

**The key is `int32_t get_broker(i)` alone** — the first bare-scalar key in M15;
every prior key was a string, a base64-parsed `Uuid`, or a composite. The shipped
seam accepts `Func<IntPtr, int, int>` unchanged.

**The value marshaller — three levels, one root:**

```
DescribeLogDirsResult_get_value(result, i)   -> borrowed LogDirDescriptionMap_t*
  LogDirDescriptionMap_count / _get_key(j) / _get_value(j)  -> borrowed LogDirDescription_t*
    LogDirDescription_error(desc)            -> borrowed const KafkaError_t*   ⚠ see below
    LogDirDescription_total_bytes / _usable_bytes           -> int64_t, -1 == empty OptionalLong
    LogDirDescription_replica_count
    LogDirDescription_replica_topic / _partition / _size / _offset_lag / _is_future
```

⚠⚠ **`LogDirDescription_error` returns `const kafka_common_KafkaError_t*` — a
SECOND borrowed error, nested inside the value tree, distinct from the per-key
`DescribeLogDirsResult_get_error(i)`.** Both are borrowed;
`KafkaException.FromBorrowedHandle` for both; **neither is ever destroyed**. The
only owned error in the RPC is the callback's `error` parameter. **This is the
highest double-free risk in P3**, because a reviewer scanning for "the error
accessor" finds only one of the two.

The two mean different things and must not be conflated:

  - `DescribeLogDirsResult_get_error(i)` — **that broker's** query failed; the
    per-key `Task` faults.
  - `LogDirDescription_error(desc)` — **that log directory** is in error while
    the broker's query succeeded; it is a *field* on the description (Java:
    `ApiException error()`, `LogDirDescription.java:61`), and the per-key `Task`
    **succeeds** carrying a description whose `Error` is non-null. **Faulting the
    task here would be a behavioural divergence.**

⚠ **`total_bytes` / `usable_bytes`: `-1` is Java's empty `OptionalLong`** —
header verbatim. `OptionalLong` has no netstandard2.0 equivalent, so the mapping
is **`long?`** with `-1` → `null`. **Confirm the substitution against
`bindings/dotnet/CLAUDE.md §4`'s idiom map and add a row if it is not there.**

⚠ **Contrast P2b's `deleteRecords`, where `-1` was NOT a sentinel** because it is
a legitimate low watermark. Here `-1` **is** the documented sentinel and a volume
size cannot legitimately be negative. **Opposite rules — do not carry P2b's habit
over without re-reading the header.**

**`LogDirDescription` and `ReplicaInfo`** — new public types, `Admin/`:

```csharp
public sealed class LogDirDescription
{
    public KafkaException? Error();                                          // :61 (ApiException)
    public IReadOnlyDictionary<TopicPartition, ReplicaInfo> ReplicaInfos();  // :69
    public long? TotalBytes();                                               // :78 (OptionalLong)
    public long? UsableBytes();                                              // :87 (OptionalLong)
    // ⚠ NO IsCordoned — see §7, D15. Its absence is deliberate and ruled.
}

public sealed class ReplicaInfo
{
    public ReplicaInfo(long size, long offsetLag, bool isFuture);
    public long Size();       // :38
    public long OffsetLag();  // :48
    public bool IsFuture();   // :59
    // plus ToString (:64)
}
```

⚠ **There is no `kafka_admin_ReplicaInfo_t` handle** — verified,
`grep -o "kafka_admin_ReplicaInfo_[a-z_0-9]*"` returns **zero matches**,
control-positive `kafka_admin_LogDirDescription_total_bytes` → present. Replicas
are read through the six flattened indexed accessors on the parent description.
Each `(topic, partition)` pair reassembles into the shipped `TopicPartition` —
**reuse it**, and verify its `Equals`/`GetHashCode` before keying a dictionary on
it.

⚠ Java's `error()` returns an `ApiException`; `KafkaException` is the established
mapping (P1's `TopicMetadataAndConfig` precedent) — **confirm against that
precedent rather than introducing a new exception type.**

**Input:** `describe_log_dirs(admin, brokers, count, timeout_ms, …)` — one array
of broker ids. Header: *"`DescribeLogDirsOptions` has no other field in Java"*, so
the options POCO is `TimeoutMs` only. Verify against
`DescribeLogDirsOptions.java`.

### 6.3 `AlterReplicaLogDirs` and `DescribeReplicaLogDirs`

`AlterReplicaLogDirsResult.java:67, :75` and
`DescribeReplicaLogDirsResult.java:41, :48`:

```csharp
public sealed class AlterReplicaLogDirsResult
{
    public IReadOnlyDictionary<TopicPartitionReplica, Task> Values { get; }   // values()
    public Task All();                                                        // all() — KafkaFuture<Void>
}

public sealed class DescribeReplicaLogDirsResult
{
    public IReadOnlyDictionary<TopicPartitionReplica, Task<ReplicaLogDirInfo>> Values { get; }  // values()
    public Task<IReadOnlyDictionary<TopicPartitionReplica, ReplicaLogDirInfo>> All();           // all()

    // NESTED, per D17 — Java nests it at :89-117
    public sealed class ReplicaLogDirInfo
    {
        public string? GetCurrentReplicaLogDir();   // :89
        public long GetCurrentReplicaOffsetLag();   // :96
        public string? GetFutureReplicaLogDir();    // :104
        public long GetFutureReplicaOffsetLag();    // :112
        // plus ToString  :117
    }
}
```

⚠ **`ReplicaLogDirInfo` is the only type in M15 with Java-bean `Get*`
accessors** (D17). Keep the prefixes and the nesting. Both `*LogDir` accessors are
nullable — a replica with no future move has a null future log dir; preserve
`null`, do not substitute `""`.

**Key reader** — the `DeleteRecordsKey` pattern with three accessors:

```csharp
internal static readonly Func<IntPtr, int, TopicPartitionReplica> DescribeReplicaLogDirsKey =
    static (result, index) => new TopicPartitionReplica(
        KeyedResultMarshal.ReadStringKey(NativeMethods.DescribeReplicaLogDirsResultGetTopic(result, index)),
        NativeMethods.DescribeReplicaLogDirsResultGetPartition(result, index),
        NativeMethods.DescribeReplicaLogDirsResultGetBrokerId(result, index));
```

`AlterReplicaLogDirs` gets the identical reader over its own accessors.

### 6.4 Input marshalling — parallel arrays with silent-skip rules

```c
alter_replica_log_dirs   (admin, topics, partitions, broker_ids, log_dirs, count, timeout_ms, …)
describe_replica_log_dirs(admin, topics, partitions, broker_ids,           count, timeout_ms, …)
```

Follow the inlined rooting/pin/submit sequence `deleteRecords` uses, including
`GCHandle.Alloc` → `SetGcHandle` → `DangerousAddRef` → `SetHandleRef` →
`submit(...)` → `catch { AbandonBeforeSubmit(); throw; }` → unpin in `finally`.

⚠ **Both entry points SILENTLY SKIP bad rows.** Header: *"An entry with a NULL
topic or NULL log dir is skipped"* (alter) and *"An entry with a NULL topic is
skipped"* (describe). A skipped row produces **no result entry**, so its `Task`
would hang were it not for `KeyedAdminOperation.FailUncompleted`. **Guard at the
C# boundary** so the caller gets an `ArgumentException` naming the offending
entry.

⚠ **`describeReplicaLogDirs`' result count is NOT guaranteed to equal the request
count.** The header's `count` doc says so, and the Rust mock omits replicas of
unknown topics outright (`src/admin/mock_admin_client.rs:1356-1359`, mirroring
Java's `if (topicMetadata != null)` guard). **This is the one RPC in M15 where
`FailUncompleted` fires on a normal, non-error path** — a describe of a
non-existent topic. Test it explicitly and make sure the message is
comprehensible.

---

## 7 · ⚠⚠ RULED DEVIATION — `LogDirDescription.IsCordoned()` is NOT implemented (D15)

**Read this before reviewing `LogDirDescription`. It is a closed maintainer
decision, dated and evidenced, NOT a defect to raise and NOT an oversight in the
C# work.**

### The facts, each independently checkable

| Layer | Status | Evidence |
|---|---|---|
| **Java** | **HAS IT** | `public boolean isCordoned()`, `LogDirDescription.java:94` |
| **Rust core** | **HAS IT** | `pub fn is_cordoned(&self) -> bool`, `src/admin/log_dir_description.rs:113`; populated from the wire at `src/admin/kafka_admin_client.rs:2109`; unit-tested by `cordoned_flag_is_carried`, `src/admin/log_dir_description.rs:143` |
| **C ABI** | **DOES NOT EXPORT IT** | `grep -ci "cordoned" target/include/confluent_kafka.h` → **0**, against a control-positive `grep -c "kafka_admin_LogDirDescription_total_bytes"` → **1** |

**The blocker is the missing ABI accessor, not the C# work.** The value is
carried all the way to the FFI boundary and dropped there. There is nothing for
the binding to read.

### The ruling

**Ruled 2026-09-09 by the maintainer: ship P3 WITHOUT `IsCordoned`, stay Mode A,
track the gap.** The alternative — landing a
`kafka_admin_LogDirDescription_is_cordoned` accessor as a small Rust-core slice
first — was considered and **not taken**. No Rust is authored in this phase.

### What that requires of the implementation

  - **The C# `LogDirDescription` must NOT fake it.** No stubbed property, no
    `false` default. `false` would read as *"this log dir is not cordoned"* when
    the truth is *"the binding cannot know"* — a wrong answer is worse than an
    absent one. **Absence is the honest encoding.**
  - **DoD §2 ("are all methods from the translated classes implemented?") will
    legitimately flag this.** The self-review must state it explicitly, cite the
    three rows above, and name the ruling — not skip the item silently.
  - **A Critic that raises this is doing its job correctly.** This section exists
    so the evidence is already on the table when it does. The correct disposition
    is *closed by D15*, not *fixed*.
  - **P9 close-out carries it forward** as a known, tracked milestone gap, to be
    re-opened if and when the ABI accessor lands.

---

## 8 · Test vehicle — all 8 RPCs are exercisable against `MockAdminClient`

Verified in `src/admin/mock_admin_client.rs`: `describe_cluster` (`:1088`),
`describe_configs` (`:1117`), `incremental_alter_configs` (`:1147`),
`list_config_resources` (`:1165`), `list_client_metrics_resources` (`:1206`),
`describe_log_dirs` (`:1224`), `alter_replica_log_dirs` (`:1282`) and
`describe_replica_log_dirs` (`:1345`) are **all real in-memory implementations**
— **none** is a *"Not implemented yet"* stub. This is a materially better
position than P2b, where `createPartitions` and non-empty `deleteRecords` both
were.

**Seeding needs no new ABI function:**

  - `MockAdminClient_new(numBrokers)` places every partition leader and the
    controller on broker 0, and seeds every broker's log dirs with
    `DEFAULT_LOG_DIRS = ["/tmp/kafka-logs"]`
    (`src/admin/mock_admin_client.rs:95, :242-243`).
  - **`CreateTopics` — already bound in P1** — populates `partition_log_dirs`
    (`:382-399`), which `describe_log_dirs` reads.

So the log-dir loop is fully reachable from the shipped surface: create a topic →
`DescribeLogDirs` sees its replicas → `AlterReplicaLogDirs` to `/tmp/kafka-logs`
succeeds → `DescribeReplicaLogDirs` reports the move.

⚠ **`set_broker_log_dirs` is Rust-only and NOT exported.** The method exists at
`src/admin/mock_admin_client.rs:325`, and
`grep -c "MockAdminClient.*log_dir" target/include/confluent_kafka.h` → **0**,
control-positive `grep -c "kafka_admin_MockAdminClient_new"` → **2**. So .NET
cannot seed a *custom* log dir — and does not need to. **Do not request an ABI
seeding function**; that would be a second Mode-B item for no coverage gain.

Reachable error behaviours worth asserting, from the mock's own code — **assert
the exact messages, not just `is_err()`** (`definition-of-done.md` §3):

  - `alter_replica_log_dirs` to a dir not in the broker's list →
    `KafkaStorageError`, *"Log directory {dir} is offline"* (`:1301-1306`).
  - `alter_replica_log_dirs` for an unknown broker/topic/partition →
    `ReplicaNotAvailable`, *"Can't find {replica}"* (`:1294-1299`, `:1317-1324`).
  - `describe_replica_log_dirs` for an unknown topic → the replica is **omitted
    from the result** (`:1356-1359`) — the §6.4 `FailUncompleted` case.

---

## 9 · Tests

Vehicle: **`MockAdminClient`**, no broker — except where a shape must be driven
through a result root the mock cannot produce, which uses the **direct submit
with a capturing callback** harness P1 established
(`Interop/AdminKeyedResultMarshalTests.cs`) and P2b reused.

**P2b's closing count is the floor: `dotnet test -c Release` is at 993 passed /
0 failed on net10.0 AND net8.0** (`design/current/STATUS.md:19`). Confirm the
**count**, never the exit code.

### 9.1 Non-negotiable, inherited from P1's three review rounds

1. **Reflection assertions on every new public signature, sensitivity proven by
   re-injection.** ⚠ **C# upcasts and widens silently** — P1's three shape defects
   survived a green build, 883 green tests **and a 0-High first Critic pass**. A
   behavioural test cannot catch a widened signature.
2. **Borrowed-vs-owned `KafkaError`, by injection.** Per-key `get_error(i)` is
   `const`/**borrowed** → `FromBorrowedHandle`, **never destroyed**; the
   callback's `error` **parameter** is **owned** → `FromHandle`. Same C type;
   const-ness is the only signal.
   ⚠ **P3 has THREE distinct cases, not one:**
     - Stage 1's RPCs have **no per-key `get_error` at all**, so the *only* error
       they see is the owned callback parameter → **`FromBorrowedHandle` there
       would be the mistake**, the inverse of every prior phase's hazard.
     - Stages 2 and 3's keyed RPCs have the usual borrowed per-key error.
     - Stage 3 has a **second borrowed error nested in the value tree**
       (`LogDirDescription_error`, §6.2). **Inject a double-free at BOTH Stage-3
       sites** — a single-site test passes while the other aborts the host, and an
       aborted run still exits 0.
3. **Span-the-op `DangerousAddRef` on every new submit** — `AdminClient_destroy`
   still has **no refcount and no drain**. The **differential** check per RPC: no
   op in flight → `Dispose` releases; op in flight → it does **not**; op completes
   → it then does. **All three cases.**
4. **`GCHandle` freed exactly once on every path**, including the inline-callback
   path — which §5.5 hazard 4 makes reachable by ordinary bad input.
5. ✅ **D19 (RULED 2026-09-09) — EVERY admin RPC's options and timeout are
   asserted at the SUBMIT SEAM. This is a checklist item, not a judgment call.**
   The Rust `MockAdminClient` **ignores `_options` for every admin RPC**
   (`src/admin/mock_admin_client.rs:1088`), so **no behavioural test can ever see
   them** — a swapped boolean pair or a dropped timeout ships fully green, proven
   by injection in Stage 1 round 1. Applies to every RPC in every remaining stage.
   Stage 2 is where it bites hardest: `describe_configs` carries **two** booleans
   (`include_synonyms`, `include_documentation`) and `incremental_alter_configs`
   carries `validate_only` **plus** a five-array row-per-operation input whose row
   **grouping** and **null-vs-empty `config_values`** are equally invisible to the
   mock. All of it gets submit-seam assertions.
6. ✅ **D20 (RULED 2026-09-09) — a reflection surface-set assertion is filtered by
   `BindingFlags` and `CompilerGeneratedAttribute` ONLY, NEVER by a name
   predicate**; any extra members go in the expected set. Origin: the same
   name-filter blindness occurred **twice inside Stage 1** (§14 defect 4).
7. ✅ **Injection evidence requires a successful build.** Per §13 trap 11, a
   failed build plus `--no-build` prints `Passed!` off a stale binary. **Every
   "goes red / stays green" claim MUST state `0 Error(s)` first, or it is not
   evidence.**
8. ✅ **The author-side prose sweep is MECHANICAL** — grep newly added comment
   lines for universal quantifiers ("only", "any", "every", "always", "never",
   "exactly", "cannot", "the one place") and check each against code in the same
   commit. Stage 1's numbers: a read-through found **1 of 3**; the grep found all
   of them.
9. ⚠✅ **KEY-SET vs ROW-SET: when a per-key result's key set comes from the
   CALLER'S MAP but the ABI request is ROW-FLATTENED, the plan MUST state what
   happens to a key that flattens to ZERO rows.** Adopted 2026-09-09 from Critic
   69 finding 69.6.

   The hazard: `KeyedAdminOperation` pre-registers one awaitable **per caller
   key**, while the ABI is fed **one row per (key, element)** pair. A key whose
   collection is empty therefore contributes **no row**, the result table carries
   no entry for it, and `FailUncompleted` (`AdminOperation.cs:277-291`) faults it —
   even where Java completes it **successfully**, because Java's future map is
   built per key and its request carries the key itself.

   **This is not specific to `incrementalAlterConfigs`.** It recurs wherever a
   `Map<K, Collection<V>>` or `Map<K, V>` input is flattened. **Stage 3's
   `alterReplicaLogDirs` (`Map<TopicPartitionReplica, String>`) has the identical
   shape and MUST be checked against this item before it is written**, not
   rediscovered in review.

   **How to apply:** for each row-flattened RPC, state (a) whether a zero-row key
   is expressible, (b) what Java does with it, and (c) what the binding does —
   and if the binding completes it **locally**, enumerate the cases where local
   completion **diverges** from Java's broker round-trip, with evidence. A test
   covering a zero-row key must exercise the **success** path; one that reaches
   its assertion through the submit-failure path passes for the wrong reason.
10. ⚠✅ **A NAMED INJECTION IS A CLAIM TO BE MEASURED, NOT REPEATED.** Adopted
    2026-09-09 — the Actor's own formulation, and the natural companion to item 7.

    When a finding, a brief, or a plan **names** the edit that would break an
    invariant ("moving X into the `finally` would…"), that named edit is a
    **hypothesis about the danger model**. Drive it. Do not repeat it downstream,
    and above all **do not write it into a code comment** — a comment stating an
    unmeasured danger model launders it into permanent, citable fact.

    69.7 is the case in point. Its **conclusion** was right, but **two of the
    three placements it named as the realistic trigger are not dangerous** —
    measured, each on a build reporting `0 Error(s)`. See §14's placement table.

    Item 7 says an injection *result* you did not establish is not evidence. This
    says an injection *premise* you did not establish is not evidence either. The
    two together: **neither end of an injection claim survives on assertion.**

### 9.2 Stage 1

  - **`CompleteList` produces a list, not a map**, and preserves delivery order.
  - **`CompleteList` has no error channel** — a reflection assertion in
    `AdminKeySeamShapeTests` in the style of
    `TheAggregateWalker_HasNoPerKeyErrorChannel`, so the absence stays structural.
  - **`Accessors` is unchanged** — re-assert
    `Accessors_CarryNeitherAKeyNorAValueAccessor` and that `Accessors` gained no
    field (D14).
  - ⚠ **`AuthorizedOperations()` distinguishes null from empty**, driven through
    the direct-callback harness so both `has_authorized_operations == false`
    (→ `null`) and `true` with count 0 (→ empty collection) are exercised. **This
    is the test that discriminates a correct implementation from one that reads
    the count alone.**
  - **`Controller()` yields `null`** on a null controller pointer — not a faulted
    task, not a default `Node`.
  - **Shape-5 projections agree**: all four `Task`s derive from one completion,
    and repeated calls return the **same** `Task` instance.
  - **`ListConfigResources` with an empty/absent type filter** means "every
    supported type" and is **not** rejected.
  - **`ConfigResourceType` id round-trip**: each of 2/4/8/16/32 maps to the right
    member and back.
  - **`ListClientMetricsResources` is `[Obsolete]`** — a reflection assertion on
    the `IAdmin` member, so a later refactor cannot silently drop Java's
    deprecation.
  - **`ConfigResource` value equality**: equal `(type, name)` are equal with equal
    hash codes; same name under two types are not.

### 9.3 Stage 2

  - ⚠ **The value tree is fully copied out before the root dies.** Complete a
    `DescribeConfigs` through the direct-callback harness, destroy the result
    root, **then** read every field of the returned `Config` — including a
    synonym's name/value/source. **This is the test that catches a lazily-held
    borrowed pointer, and nothing else will.**
  - **Synonym precedence order is preserved** — assert against a known input
    order, not a sorted expectation.
  - **Null round-trips as `null`, not `""`**: a null synonym value, a null
    `ConfigEntry.Value` (sensitive config), a null `ConfigEntry.Documentation`.
  - **`ConfigSource` / `ConfigType` decode from Java's enum constant NAMES**
    (§5.2), one assertion per member, plus **an unrecognised name maps to
    `UNKNOWN`** rather than throwing.
  - ⚠ **`AlterConfigOpType` decodes from wire IDS** (0/1/2/3), not names. **The
    two conventions coexisting in one phase is the thing to pin.**
  - **Composite `ConfigResource` key round-trip**: two resources of the same type
    with different names, and the same name under two types, land in distinct
    entries.
  - **`DescribeConfigsResult.All()` carries the map; `AlterConfigsResult.All()`
    is a bare `Task`** — a reflection assertion, since these are one line apart.
  - **Five-array flattening**: a resource with three ops emits three contiguous
    rows in caller order with the resource repeated; two resources do not
    interleave.
  - ⚠ **A `DELETE` with a null value reaches the ABI as null**; a `SET` with `""`
    reaches it as `""`. Different requests.
  - ⚠ **An unknown op-type code drives the INLINE callback path** (§5.5 hazard 4):
    the callback runs on the submitting thread before the entry point returns,
    every key faults, and the `GCHandle` is freed exactly once.
  - **`ConfigEntry`'s P1 members are unchanged** — the five shipped properties
    keep their exact signatures and nullability, proven by P1's existing
    reflection assertions passing **unmodified**.
  - **The Stage-1 debt**: `ListClientMetricsResources` returns a seeded listing
    after an `IncrementalAlterConfigs` against a `CLIENT_METRICS` resource, and
    `ListConfigResources` filtered to `CLIENT_METRICS` agrees (§3).

### 9.4 Stage 3

  - ⚠ **The nested map is fully copied out before the root dies.** Same harness
    shape as §9.3: destroy the root, **then** read the log-dir key, the
    description's `Error()`, `TotalBytes()`/`UsableBytes()`, and every
    `ReplicaInfo`.
  - ⚠ **`LogDirDescription.Error()` non-null does NOT fault the per-key task**
    (§6.2). Drive both: a broker-level `get_error(i)` (task faults) and a
    dir-level `LogDirDescription_error` (task succeeds, `Error()` non-null).
  - **`TotalBytes()` / `UsableBytes()`: `-1` → `null`**, a real value round-trips,
    and ⚠ **include a `0` case** so "-1 → null" is not accidentally implemented as
    "falsy → null".
  - **Scalar `int` broker key** round-trips, including broker `0`.
  - **3-part composite key** round-trips: same topic+partition on two brokers,
    same topic+broker on two partitions, same partition+broker on two topics — all
    distinct entries.
  - **`TopicPartitionReplica` value equality**: equal triples equal with equal
    hashes; each field differing makes them unequal.
  - ⚠ **`DescribeReplicaLogDirs` of an unknown topic**: the replica is omitted and
    the per-key `Task` faults through `FailUncompleted` with a comprehensible
    message (§6.4). **The one normal-path `FailUncompleted` in M15.**
  - **Exact mock error messages** — *"Log directory {dir} is offline"* and
    *"Can't find {replica}"* (§8).
  - **A null topic or null log dir is rejected at the C# boundary** with an
    `ArgumentException` naming the entry — not silently skipped by the ABI.
  - **`ReplicaLogDirInfo` nesting and `Get*` prefixes** match Java (D17) — a
    reflection assertion, since it is the one Java-bean type in M15.
  - ⚠ **D15**: a test pins that `LogDirDescription` has **no** `IsCordoned`
    member — so a later well-meaning refactor cannot add a fake one — **and the
    self-review states the gap** with §7's three evidence rows.

### 9.5 Cross-cutting

  - **P1/P2a/P2b regression**: their tests pass **unmodified** except for the
    `AdminKeySeamShapeTests` assertions the new callable legitimately extends, and
    one earlier defect re-injected still goes red.
  - **TFM-matrix smoke** on net462 (via netstandard2.0), net8.0, net10.0.
  - **DoD §10 (hot-path allocation audit): N/A** — Admin is batch/administrative
    with no per-record path (`admin-client.md` §10). **State it; never skip
    silently.**
  - **DoD §11: N/A** to `IAdmin`, spirit verified — every new RPC is a plain sync
    `fn` returning a `*Result`; only `Close` returns `Task`; no `async` in the
    marshallers other than `ListTopicsResult`-style projection helpers.
  - ⚠ **DoD §2 needs an explicit statement about `IsCordoned`** (§7). A silently
    missing Java member is exactly what DoD §2 exists to catch.

---

## 10 · Definition of Done

```
cargo build --features ffi            # header MUST be byte-identical (Mode-A proof)
dotnet build -c Release --no-incremental   # 0W/0E across all 6 TFM outputs
dotnet test -f net10.0 && dotnet test -f net8.0
dotnet format --verify-no-changes
cargo xtask format-check && cargo xtask lint     # from the REPO ROOT (false-fails from bindings/dotnet)
```

Plus no `TODO`/`FIXME`, Apache-2.0 header on every new file, §9's tests green
with **counts confirmed**.

**Run the full gate at each of §3's three stage boundaries, not only at the end.**
That is what keeps a late split available.

**Mode-A proof, every round:**
`git diff 0fc2ca9f..HEAD -- src/ cbindgen.toml target/include/confluent_kafka.h generator/`
**empty**, with a **control-positive** over `bindings/dotnet/` in the same command
so an empty result cannot be a broken pathspec; header hash byte-identical
(P1/P2a/P2b all closed at SHA-256 `45912ea9…`). A genuine ABI gap **STOPS the
phase and escalates to the Manager** as a Rust-core dependency — `dotnet-actor`
does not author Rust and does not invent a managed workaround. ⚠ **`isCordoned` is
NOT such a gap to escalate — it is already ruled (§7, D15).**

---

## 11 · Commit hygiene

  - Incremental commits, at minimum one per §3 stage; `fixup!` referencing the
    original when closing a comment.
  - **Never `git add -A`.**
  - **Do NOT commit:** `COMMENTS.69.md` / `COMMENTS.DONE.69.md` (local working
    files — only the Manager's archived copy under
    `design/history/M15/P3-cluster-configs-logdirs/` is tracked); the repo-root
    `.claude/agents/dotnet-*.md` discovery copies; anything under
    `.claude/agent-memory/`; `bin`/`obj`; `.DS_Store`.
  - ⚠ **`design/current/PLAN-M15-admin-client.md` stays UNTRACKED.** It is
    untracked but **not gitignored**, so it is one `git add -A` from being
    committed.

---

## 12 · The rule that produced P1's only real defects

⚠ **THE PLAN IS NOT REVIEW GROUND TRUTH.** P1's plan sketch was wrong **twice**,
P2a's a third time, P2b's G2 remedy a fourth — and §0 of this document corrects
**four** roadmap claims, one of which (`ClusterDescription`) would have created a
public type Java does not have. This plan also contradicts the roadmap once more,
on `ReplicaLogDirInfo`'s nesting (D17).

**A `*Result`'s public accessor signature is the contract, not the private field
it is derived from.** Every signature in §4, §5 and §6 was quoted from a Java
**public accessor** with a line citation, precisely so it can be checked.
**Where this plan and the Java source disagree, the Java source wins and this
plan is the defect** — report it rather than implementing it.

⚠ **And quoting the signature is not enough.** P2a's third plan defect was
declaring `AllTopicNames()`/`AllTopicIds()` non-nullable; Java has no
nullable-reference annotations, so **return-nullability lives only in the javadoc
and the method body**. For every accessor in this phase: **read the javadoc AND
the body**, not just the declaration.

The Critic's ground truth is the **C ABI header** + the **Java public API shape**
(`bindings/dotnet/CLAUDE.md §8.2`) — not Rust internals, not Java implementation
logic, and not this document.

---

## 13 · ⚠ Environment traps — these fabricate FALSE PASSES

A check can look green **because the tool never ran**.

1. **`PATH` is clobbered.** `git`, `cargo`, `sed` all appear absent. Start every
   Bash call with
   `export PATH="/usr/bin:/bin:/usr/local/bin:/opt/homebrew/bin:$HOME/.cargo/bin:$PATH"`.
   `command -v` is **not** reliable here.
2. **`grep` may be `ugrep`** — it rejects some patterns and emits *nothing*, so a
   pipeline reads as a pass. Use `/usr/bin/grep` for anything relied on as
   evidence.
3. **`sed` does not exist** — use `awk 'NR>=A && NR<=B'` for line ranges.
4. **`cat` is shadowed by a missing `bat` alias** — `cat > f <<'EOF'` silently
   writes a **0-byte file and continues**. Use `/bin/cat`.
5. **This is zsh** — unquoted `$var` is not word-split; unquoted globs abort the
   command with "no matches found". Always quote.
6. **A test filter matching zero tests exits 0** printing `0 passed`. **Confirm
   the expected COUNT.**
7. ⚠ **An ABORTED test run ALSO exits 0** — P1's discovery. A double-free aborts
   the test host and `dotnet test` still returns 0; **`Test Run Aborted` in the
   output is the only signal.** Never certify a run by its exit code. **P3 has
   three borrowed-error sites (§9.1 item 2), so it has three independent ways to
   produce exactly this false pass.**
8. **`dotnet build -c Release` followed by `dotnet test --no-build` silently runs
   Debug binaries** — P2a saw three injections read `Passed!` without ever being
   applied. Unlike the other traps, this one makes a *correct* guard look broken
   and a *broken* one look fine.
9. **`ls … | awk` prints nothing and exits 0 on no input**, so a `||` fallback
   never fires — use `test -f` to check existence.
10. **Every negative claim needs a control-positive in the same command.** A
    "symbol absent" result and a broken pathspec are indistinguishable otherwise.
    Five of this plan's findings (§0.3, §1.1, §5.3, §6.2's `ReplicaInfo` note, §7)
    are negatives and each is paired with one.
11. ⚠⚠ **A FAILED BUILD MAKES AN INJECTION PROBE PRINT `Passed!` — assert
    `0 Error(s)` before trusting ANY injection result.** Found in Stage 1's round-2
    fix cycle (2026-09-09): two of the Actor's injection probes **did not compile**,
    and `dotnet test --no-build` then reported a bogus `Passed!` off the **stale**
    binary.

    **This is a DIFFERENT trap from trap 8.** Trap 8 is a Release/Debug mismatch —
    a binary was built, just the wrong one. Here **no new binary was produced at
    all**, so the probe measured the pre-injection code.

    It is the worst-placed false pass in the phase because it corrupts
    **falsification evidence**, which is this phase's main instrument: "the
    injection stayed green" is the *conclusion you are testing for*, and it is also
    exactly the shape a failed build produces. The two are indistinguishable
    without checking the build.

    **Requirement for every remaining stage: any "the injection goes red / stays
    green" claim MUST state that the build succeeded first.** A probe whose build
    status is unstated is not evidence.

### 13.1 A FALSE-FAIL trap — do not burn a round on it

Two **pre-existing consumer** tests assert
`completingThreadId != continuationThreadId`, which is **unsound** because managed
thread ids are **recycled**:

  - `ConsumerPollBridgeTests.ResultBridge_RunsContinuationsAsynchronously_OffTheCompletingThread`
    (last touched by `26761aa4`, M6) — seen failing with both ids equal to 30.
  - `ConsumerRebalanceListenerBridgeTests.DisposeAsync_WithLiveRegistration_ReturnsWithoutHanging`
    (last touched by `b25bf7f0`, M9/P6).

A **third**, observed during P3's own Stage-1 review round (2026-09-09) and added
here so the next round does not spend time on it:

  - `ProducerSubmitHandleRefTests.SubmitVoidOperation_WhenAddRefThrows_DoesNotRootTheCompletionContext`
    — a marginal **allocation-budget** flake (seen at 2,139,104 B against a
    2,000,000 B budget). It passed on re-run and 3/3 in isolation. **Not a P3
    regression**: the file is absent from P3's Stage-1 commit (grep of
    `git show --name-only 44b0b4c7` → **0** hits, against a 26-file
    control-positive) and was last touched by `ae72b65e`.

**None of these three files is in P3's scope.** If one goes red it is **not** a P3
regression — re-run, confirm, move on. Fixing them is a separate slice: the first
two are consumer-side, the third is a producer alloc budget that wants widening or
a stabler measurement.

---

## 14 · Phase execution log (Manager-maintained)

⚠ **Durable hand-off state.** This section exists so the phase can be picked up
from the repo alone. The Manager died once mid-round (API error, Critic 69
round 2); nothing that matters may live only in an agent's context.

**Findings live in `bindings/dotnet/COMMENTS.69.md` (open) and
`COMMENTS.DONE.69.md` (closed).** Both are local working files — **never
`git add` either**. This log records only what those files cannot: sequencing,
authorization state, and what is blocked on whom.

### Commits so far (base `0fc2ca9f` = P2b close)

| SHA | What | Gate |
|---|---|---|
| `44b0b4c7` | **Stage 1** — `CompleteList` + `DescribeCluster` + `ListConfigResources` + `ListClientMetricsResources` | 1049/1049 both TFMs, `Test Run Aborted` 0, Mode A 0 lines (control 26 files) |
| `6ab8a5bf` | `fixup!` → `44b0b4c7` — Critic round-1 findings 69.1/69.2/69.3 | 1065/1065 both TFMs, Mode A 0 lines (control 28 files, +4096/−48) |

**Do NOT squash or rebase** — the maintainer does that after their own review.

### Stage 1 review rounds

  - **Round 1** — Critic 69 on `44b0b4c7`: **0 High / 1 Medium / 2 Low**, no
    memory-safety defect. All three accepted by Actor 69, none disputed, all
    closed into `COMMENTS.DONE.69.md`.
  - **Round 2** — Critic 69 on `44b0b4c7..6ab8a5bf` plus a whole-Stage-1
    reflection walk: **0 High / 0 Medium / 2 Low** (69.4, 69.5). No memory-safety
    and no public-shape defect. The reflection walk was **machine-checked, not
    read**: `diff(api@44b0b4c7, api@6ab8a5bf)` **empty** at 1200 lines each,
    against a 98-line control-positive versus the stage base. All three demanded
    re-verifications were re-run rather than accepted, and matched the Actor's
    claims exactly (2 failed / 6 failed / control holds).

    The first attempt terminated on an API error before writing any findings; the
    same Critic was **resumed**, not restarted, and it removed its throwaway
    worktree and released its lock on completion (verified: `wt69r2` → 0 hits
    against a 1-worktree control).

    **Maintainer ruling: fix both Lows, then Stage 1 closes — no third Critic
    round**, both being test/doc-only with no production risk. The closure record
    in `COMMENTS.DONE.69.md` must carry the evidence, since that waiver replaces
    a review round.

  - **Round-2 fix cycle** — fixup `11a24a34`. Both findings closed.

### ✅ STAGE 1 IS CLOSED (2026-09-09)

**Final Stage-1 range: `0fc2ca9f..11a24a34`** — `44b0b4c7` + fixups `6ab8a5bf`,
`11a24a34`. **Do NOT squash or rebase**; the maintainer does that after review.

Closure evidence, each re-verified by the Manager against the repo:

  - `11a24a34` touches **3 test files and ZERO production files**
    (`bindings/dotnet/src/` → **0**, against a 3-file control-positive). The
    escalation condition ("tell me if the 69.4 fix leaves the seam test") was met
    but **benignly**: the 69.4 fix *is* confined to `AdminKeySeamShapeTests.cs`;
    the commit spans three files only because 69.5's two instances live in the
    other two.
  - **Mode A holds across the whole stage**: `0` core-tree diff lines, control
    positive 28 files / +4117 / −48.
  - **69.4 genuinely fixed**: the assertion is now
    `.Where(method => !method.IsDefined(typeof(CompilerGeneratedAttribute), inherit: false))`
    over `GetMethods(NonPublic | Static)` (`:233`) — **the name predicate is
    gone**, `ReadStringKey` sits in the expected set (`:245`), and the disproven
    "for no gain" justification was **deleted, not re-scoped**. The one surviving
    `StartsWith("Complete")` (`:214`) is the remark explaining the *earlier*
    version, which is correct to keep.
  - **All five findings 69.1–69.5 are in `COMMENTS.DONE.69.md` with evidence**;
    `COMMENTS.69.md` has **0** open findings; no stale lock; main tree pristine
    (0 modified tracked `.cs` against a 1-file control); 1 worktree.

**Record-keeping precedent set here, worth keeping:** the Actor recorded 69.3's
remedy as **superseded, not wrong-when-given**, and stated the supersession
rather than rewriting the round-1 entry. **A later round must not retroactively
edit an earlier finding's record** — the history of what was believed when is
part of the evidence.

**Evidence that the prose sweep must stay MECHANICAL, with numbers:** round 1's
read-through found **1 of 3**. Round 2's grep over every comment line added since
`44b0b4c7` — control-positive **240** added comment lines — produced **20** hits:
2 licence noise, 16 verified true, and the 2 false ones were exactly 69.5(a) and
(b). Keep it mechanical for Stages 2 and 3; the ratio is a property of
read-throughs, not of effort.

### ✅ ALL THREE RULINGS APPROVED 2026-09-09 — Stage 2 AUTHORIZED

The park is lifted. The rulings are now **D18** (method vs property — §6.1),
**D19** (options at the submit seam — §9.1 item 5) and **D20** (reflection
surface-set assertions — §9.1 item 6). The suspensions on §5.5 / §6.1 / §6.2 are
**lifted**, and §4.3's backwards citation is corrected in place.

⚠ **D20 was raised against `bindings/dotnet/.claude/rules/ffi-marshalling.md`,
which IS a rule file** — it lives literally under a `.claude/rules/` directory,
and `bindings/dotnet/CLAUDE.md:7` and `:832` cite it by that path as the authority
for boundary rules, the same delegating relationship root `CLAUDE.md` has with
`agent-roles.md`. **Automated agents do not edit it.** The Manager has drafted the
insertion text and handed it to the maintainer to apply; D20 binds this phase
through §9.1 item 6 regardless of when the rule file is updated.

⚠⚠ **UNRESOLVED, and it lands in P9 — flag before P9 starts.** The M15 roadmap's
§10 schedules *"`ffi-marshalling.md` **Part C · Admin**"* as a **P9 deliverable**,
i.e. it tasks an agent with authoring content into a file agents are forbidden to
edit. That is a genuine conflict in the milestone plan, not a misreading. It needs
a maintainer decision — most likely "the Manager drafts Part C and the maintainer
applies it" — **before P9 begins**, not during.

### ✅ AUTHORIZATION STATE — Stage 2 AUTHORIZED 2026-09-09 (all three rulings adopted)

All three rulings raised at the Stage-1 boundary were **approved**. They are now
**D18**, **D19** and **D20**; the park is lifted and Stage 2 is running. Retained
below is *why* each was raised, since the origin is the evidence.

**D18 (was ruling 1) — method vs property.** ✅ *Unforced getter → **property**;
method form only where `CS0102` forces it.* Origin: this plan's §4.3 cited
`DeletedRecords.LowWatermark()` as a **method** precedent; shipped is
`public long LowWatermark { get; }`, a **property** (`DeletedRecords.cs:58`;
property form 1 hit, method form 0) — a Manager defect, not an Actor one.
`RecordsToDelete.BeforeOffset()` is a method only because a same-named
`public static` factory (`:49`) collides; `TopicPartitionReplica` declares **0**
`public static` members, so nothing forces it there. §5.5 / §6.1 / §6.2 are
**un-suspended** and §4.3 is corrected. Settled before Stage 3 lands
`TopicPartitionReplica` and `ReplicaInfo`.

**D19 (was ruling 2) — options at the submit seam**, now **§9.1 checklist item 5**,
a checklist item rather than a judgment call. The Rust `MockAdminClient` **ignores
`_options` for every admin RPC** (`src/admin/mock_admin_client.rs:1088`), so option
flags and `timeoutMs` are behaviourally untestable **permanently** — proven by
injection in round 1 (a swapped boolean pair and a dropped timeout both ran fully
green).

**D20 (was ruling 3) — reflection surface-set assertions** filter by `BindingFlags`
+ `CompilerGeneratedAttribute` **only, never by a name predicate**; extra names go
in the expected set. Now **§9.1 checklist item 6**. Raised by Critic 69 in round 2
after the same defect class occurred **twice inside one stage** (§14 defect 4).

⚠ **D20's home is a RULE FILE and the Manager did not edit it.**
`bindings/dotnet/.claude/rules/ffi-marshalling.md` sits literally under a
`.claude/rules/` directory, and `bindings/dotnet/CLAUDE.md:7` / `:832` cite it by
that path as the authority for boundary rules. Insertion text was drafted and
handed to the maintainer. **D20 binds this phase through §9.1 item 6 regardless of
when the rule file is updated** — the Actor already implemented it in `11a24a34`.

### Standing requirement added mid-phase — author-side prose sweep

**At every stage boundary the Actor must sweep the prose it wrote in that same
change** — remarks, xmldoc, comments — for quantifiers and uniqueness claims
("the only", "the one place", "exactly N", "always", "never") that a later stage
falsifies. **A reviewer cannot flag prose that did not exist at review time**, so
author-side sweeping is the only thing that catches this class, and a passing
review round does **not** imply that round's own new text was reviewed.

Precedent from Actor 69's round-1 fix, which caught its own
`"the phase's only two-boolean entry point"` — false in Stage 2, where
`describe_configs` takes `include_synonyms` **and** `include_documentation`. Per
`ffi-marshalling.md §A6`'s round-5 amendment the remedy is to **DELETE** the
over-broad quantifier, not re-scope it; a re-worded comparative is how the next
stale sentence gets written.

### Plan defects found so far (all Manager-owned, all corrected in place)

1. §4.3's `DeletedRecords.LowWatermark()` citation — **backwards**; corrected,
   and it is the origin of pending ruling 1.
2. §4.3 listed `[Obsolete]` only on the RPC members; Java also deprecates three
   **types** (`ListClientMetricsResourcesResult.java:30`,
   `ListClientMetricsResourcesOptions.java:24`,
   `ClientMetricsResourceListing.java:21`). Actor 69 marked all four — more
   Java-faithful than the plan.
3. §9.2 asked for the `has_authorized_operations == false` branch via the mock;
   the mock cannot produce it (`mock_admin_client.rs:1106` always completes
   `Some(BTreeSet::new())`). Driven by an injectable gate parameter instead.
4. The plan predicted `AdminKeySeamShapeTests` would go red on `CompleteList`.
   **It did not** — `CompleteOverloads()` filtered on `Name == nameof(Complete)`,
   so a differently-*named* callable was invisible to it.

   ⚠⚠ **This class then occurred a SECOND time inside the same stage** (Critic 69
   finding 69.4). The round-1 fix asserted the callable surface as a set — but
   filtered it `.Where(name.StartsWith("Complete"))`, i.e. reproduced the same
   name-based mistake one width wider, in a test whose own remark *documents the
   first occurrence*. Proven: `WalkSomething` lands **green**, control
   `CompleteSomething` goes **red**. Its stated justification for not widening —
   that doing so "would drag in `ReadStringKey` and compiler-generated members" —
   was **measurably false**: `GetMethods(NonPublic | Static)` returns five methods,
   none compiler-generated, because the lambda display class is a **nested type**
   that `GetMethods` on the containing type never returns.

   **The durable rule: a surface-set assertion filters by `BindingFlags` +
   `CompilerGeneratedAttribute`, never by a name predicate; extra names go in the
   expected set.** Raised as pending ruling 3 for `ffi-marshalling.md`. Note the
   Critic **revised its own round-1 recommendation** here on new measurement
   (`COMMENTS.69.md:257-259`) — round 1 had said "narrow the sentence, not the
   test", which it withdrew as leaving a live hole.

5. Two universals introduced **by the round-1 fixup itself** were falsified by code
   in the same commit (finding 69.5), even though that commit is where the
   author-side prose sweep was introduced. The sweep caught one claim and missed
   two — evidence it must be **mechanical** (grep newly added prose for "only",
   "any", "every", "always", "never", "exactly", "cannot") rather than a
   read-through.

### Stage 2 — `45680c3c`, review in flight

**`45680c3c`** — "DescribeConfigs + IncrementalAlterConfigs, and `ConfigEntry`
completed", on `11a24a34`. **1120** tests both TFMs (Stage-1 floor 1065).
Manager-verified: Mode A **0** core-tree lines against a control-positive of 41
files / +7848 / −78; **0** `COMMENTS`, **0** `agent-memory`, **0** `.claude/rules/`
paths in the range against that same 41-file control.

Six injections claimed by the Actor, all handed to Critic 69 for re-running rather
than acceptance, each required to state its build status per §13 trap 11: null
`config_values` coalesced to empty → RED; rows reversed within a resource → RED;
the two `describeConfigs` booleans swapped → 2 RED; **borrowed per-resource error
freed as owned → host abort**; `IsDefault` no longer derived from `Source` → RED;
the 8-arg ctor made public → **P1's assertion RED**.

⚠ **Ownership direction INVERTS between stages, and it is easy to carry the wrong
habit forward.** Stage 1's three RPCs have **no per-key `get_error` at all**, so
their only error is the callback's **owned** parameter (`FromHandle`). Stage 2's
two RPCs have a `const`/**borrowed** `get_error(i)` (`FromBorrowedHandle`, never
destroyed). Stage 3 has **two** borrowed sites — the per-key error *and* the nested
`LogDirDescription_error`.

### ⚠ ESCALATED, MAINTAINER-OWNED, OPEN — the 8-arg `ConfigEntry` constructor

**Disposition is the same as D15: a known open decision, NOT a defect to file.**
Neither Actor nor Critic may pre-empt it in either direction. A finding that
*depends* on how it lands must say so explicitly rather than assume an outcome.

Facts, each Manager-verified:

  - **Java publishes TWO public constructors** — `ConfigEntry.java:44` (2-arg) and
    `:59` (the full one).
  - **C# publishes ONE.** The 8-arg is `internal` at `ConfigEntry.cs:150` (there is
    also an `internal` 5-arg at `:121`).
  - **P1's guard** `PublicAdminShapeParityTests.cs:107-117`
    (`ConfigEntry_PublishesOnlyTheConstructorJavaHas`) asserts
    `Assert.Single(publicCtors)` then `Assert.Equal(2, parameters.Length)`.
  - ⚠ **That guard file is UNTOUCHED across the entire P3 range** — **0** diff
    lines against a control-positive of **494** diff lines on `ConfigEntry.cs`.
    So the boundary condition held: `ConfigEntry` was extended without editing the
    assertion that constrains it.

The Actor handled this correctly — it hit the boundary condition, **stopped**,
recorded the gap in the type's remarks as a DoD §2 item, and demonstrated the
guard is **live rather than vacuous** (injection 6 turns it red).

⚠ **A Manager verification miss worth recording, because the fix is a habit.**
Checking "is P1's guard untouched?", I located the guard by grepping for
`Assert.Single` + `ConfigEntry` and got **`PublicAdminConfigsShapeParityTests.cs`**
with **425** diff lines — an apparent contradiction of the Actor's claim. It was a
false alarm: that file is **new in Stage 2** (`git cat-file -e 0fc2ca9f:<path>`
fails), so its "diff" is simply its own creation. P1's actual guard is
`PublicAdminShapeParityTests.cs`, which is pre-existing and has 0 diff lines.
**Lesson: when checking whether a pre-existing file changed, first prove it EXISTED
at the baseline.** A name-similar file created by the change under review will
otherwise read as a large diff and manufacture a contradiction.

### ⚠⚠ §13 trap 11 OBSERVED LIVE — one day after it was written down

Stage 2's **injection 4** (freeing the borrowed per-resource error as owned)
aborted the test host, and the run printed:

> `Passed! - Failed: 0, Passed: 102`

**over a crashed host.** Reading that banner instead of grepping `Test Run
Aborted` would have recorded a deliberate **double-free** as "the injection stayed
green" — i.e. as evidence that the guard does not work, when in fact the guard
fired and the host died.

This is trap 11's exact predicted failure mode, observed within a day of the trap
being written, and it is the strongest available justification for §9.1 item 7:
**an injection result is not evidence unless the run's build reported `0 Error(s)`
AND `Test Run Aborted` was explicitly grepped.** Neither the exit code nor the
summary banner can be trusted on this path.

### Stage 2 review — Critic 69 round 3: 0 High, 1 Medium, 0 Low

**69.6 [Medium] — a resource mapped to an EMPTY `AlterConfigOp` collection is
faulted, where Java completes it successfully.** Manager-verified against the Java
source before dispatching the fix:

  - **Java builds the future map per RESOURCE, not per row.**
    `KafkaAdminClient.java:2889-2896`:
    `for (ConfigResource resource : resources) futures.put(resource, new KafkaFutureImpl<>());`
    and `createRequest` passes
    `new IncrementalAlterConfigsRequest.Builder(resources, configs, …)` — the
    resource collection travels **alongside** the configs map.
  - **`AdminOperation.cs:277-291` `FailUncompleted`** faults any key whose task is
    not completed, with *"The {0} result contained no entry for '{1}'."* A zero-op
    resource contributes no row, so it has no entry, so it is faulted.
  - **The covering test passes for the WRONG REASON.** Its helper drives
    completion through `AdminCallbacks.IncrementalAlterConfigs(IntPtr.Zero,
    CapturedError(), …)` — the **submit-failure** path, where *every* awaitable
    faults — and then asserts `Assert.NotNull(entry.Value.Exception)` for each. The
    zero-op key's fault is therefore indistinguishable from the blanket failure.

  ⚠⚠ **CORRECTION (2026-09-09) — the miscounted grep was the COORDINATOR'S, and
  an earlier revision of this entry MISATTRIBUTED it to the Critic. Retracted.**

  **What the Critic actually filed** was *"Java has no path that fails a resource
  merely for carrying zero operations"*, which its citations support. It never
  claimed an `isEmpty()` count. Verified: `COMMENTS.69.md` contains exactly **two**
  `isEmpty` mentions, both at **lines 780-781, inside Round 4** — they *are* its
  rebuttal of this misattribution. **Round 3 (lines 397-612), which filed 69.6,
  contains ZERO**, against a 2-hit whole-file control-positive.

  **The chain, recorded in full because every link is a reusable lesson:** the
  coordinator ran `grep isEmpty | grep -i "config\|alter"`, which **silently
  dropped** `KafkaAdminClient.java:2883` because that line contains neither word;
  reported "0 hits, control 40" as verified fact; the Manager independently found
  `:2883` and correctly flagged the count — **but attributed it to "the finding's
  claim"** rather than to the relay, and then **propagated that misattribution
  into the round-4 Critic brief**, where the Critic rebutted it with evidence.

  The underlying substance is unchanged: `:2883`'s `if
  (!unifiedRequestResources.isEmpty())` guards whether any resource needs the
  *unified least-loaded-broker* request path, **not** whether a resource's op
  collection is empty — so no empty-collection guard exists and 69.6 stands.

  ⚠ **This is the same class that had to be publicly withdrawn from `STATUS.md`
  in P2b** — a claim reported as coming from a reviewer that the reviewer never
  made. It is the **third** instance this phase of a conclusion resting on a
  miscounted grep, and the **first authored by the coordinator** rather than by an
  Actor or Critic. Recorded without softening, per the standing rule: **never
  attribute a recommendation or an argument to a Critic, Actor, or document unless
  it literally appears there** — and a filtered grep is not a count, because the
  filter can drop the very line that refutes it.

### Stage 2 fix cycle — `1b0fec85`; and the SECOND escalated Mode-B gap

**`1b0fec85`** (`fixup!` → `45680c3c`). **1123** tests both TFMs. Mode A **0**
core-tree lines, control-positive 42 files / +8092 / −78. All six findings
69.1–69.6 in `COMMENTS.DONE.69.md`; **0** open in `COMMENTS.69.md`.

Manager-verified: `CompleteKeysWithNoRequest` has exactly **one** definition
(`AdminOperation.cs:356`) and exactly **one** call site
(`AdminCallbacks.cs:697`) — 2 occurrences repo-wide, so the "single caller,
no-op for every other shape-2 RPC" claim holds.

#### ⚠⚠ ESCALATED, MAINTAINER-OWNED, OPEN — zero-op resources are dropped by the FFI row encoding

**The ask: a way to express a resource carrying ZERO operations across the ABI.**

Manager-verified evidence, both citations checked directly:

  - **`src/ffi/admin.rs:4233-4262`** builds the resource map from **rows alone**:
    a `for i in 0..n` loop ending in
    `out.entry(ConfigResource::new(resource_type, resource_name)).or_default().push(AlterConfigOp::new(...))`.
    A resource with zero rows **never enters `out`** and is therefore never sent.
  - **`src/admin/mock_admin_client.rs:629-636`** resolves the topic and returns
    `UnknownTopicOrPartition` (*"No such topic as {}"*) **before**
    `apply_alter_ops`. **So the Rust core would fail an absent resource
    correctly.** The divergence is introduced *solely* by the FFI row encoding.

⚠⚠ **This is DIFFERENT IN KIND from D15 — do not assume D15's outcome carries
over.** D15 **omits a feature** (`IsCordoned` is absent, which is honest absence).
This one returns **success where both Java and the Rust core return an error** —
a **wrong answer**, not a missing one. That distinction may change how the
maintainer rules.

**Until ruled: do not design around it, and do not start any Rust work.**

#### Stage 3 is gated on this ruling, not only on Stage 2 closing

Stage 3's **`alterReplicaLogDirs`** (`Map<TopicPartitionReplica, String>`) has the
**identical key-set-vs-row-set shape** (§9.1 item 9), so how this is ruled shapes
how Stage 3 is written. **Stage 3 does not start until Stage 2 closes AND the
maintainer has ruled.**

#### Two process results worth keeping

  - **The mechanical prose sweep deleted 1 of 21 claims PRE-COMMIT** — an
    "exactly the cases" exhaustiveness claim over the divergence set that the
    Actor could not discharge. That is the sweep catching an author-side claim
    **before review**, which is exactly what it exists for (§9.1 item 8). Contrast
    Stage 1, where a read-through caught 1 of 3 and the other two reached review.
  - **The grep-count correction was taken.** Nothing in the Actor's record cites
    the miscounted `isEmpty()` figure; it re-verified
    `KafkaAdminClient.java:2889-2896` and `:2902` itself. A corrected citation
    propagated instead of a repeated one.

### Stage 2 round 4 — 69.6 CLOSED, one new 69.7 [Low → graded UP]

Critic 69 verified the success-branch-only placement **against the Java source
rather than accepting the reasoning**: `KafkaAdminClient.java:2893-2895`, `:2902`
and `:2922-2924` (`handleFailure` → `completeAllExceptionally`) together show that
completing on the failure branch would diverge **from** Java, not toward it.

**69.7 — graded UP from Low on CONSEQUENCE, not likelihood** (maintainer, and the
same reasoning applied to 69.4). Under the injected edit, an
`incrementalAlterConfigs` that **failed at submit** reports **success** for a
zero-op resource — a wrong answer with the whole suite green at 1123/0. The edit
that causes it is **ordinary tidy-up**: moving `CompleteKeysWithNoRequest()` beside
`FailUncompleted()` in the `finally`, or hoisting it out of the `if`/`else`. The
site already carries a comment explaining why it is deliberately not on the failure
branch — **but a comment is not a guard**, and this phase's recurring pattern is
that unpinned invariants eventually get violated. Pinned with a test that fails
when the call is moved to the failure branch.

**Two positives worth keeping:**

  - The Critic **verified the placement reasoning against Java** instead of
    accepting a plausible explanation — the same standard it applied to its own
    round-1 recommendation when it revised it.
  - The Actor **refused an exhaustiveness claim where one would have been
    natural**: *"this is a list of what was found, not a claim that nothing else
    follows from the root."* That is §A6 discipline applied **pre-emptively**
    rather than under correction. Round 4's sweep found **26** quantifier-bearing
    lines out of 150 added, **none false**.

### ✅ STAGE 2 IS CLOSED (2026-09-09)

**Stage 2 chain: `45680c3c` → `1b0fec85` → `0d1587be`.** **1124** tests both TFMs.
**Do NOT squash or rebase.**

Manager-verified closure evidence:

  - `0d1587be` is **test-only**: **0** files under `bindings/dotnet/src/`, against
    a control-positive of **1** total file
    (`Interop/AdminConfigsLifetimeTests.cs`).
  - **Neither escalation was pre-empted**: `ConfigEntry.cs` has **0** diff lines
    across `45680c3c..HEAD`, against a control-positive of 6 files / +330 / −6.
  - **Mode A across all of P3**: **0** core-tree lines, control-positive 42 files
    / +8172 / −78.
  - **All seven findings 69.1–69.7 in `COMMENTS.DONE.69.md`; 0 open.**
  - Tree pristine, 1 worktree, 0 modified tracked `.cs`.

#### ⚠⚠ THE ROUND'S MOST IMPORTANT OUTCOME — a finding's DANGER MODEL was wrong, and measurement caught it

69.7's **conclusion** was right — the invariant is real and worth pinning. But the
edits it named as the realistic trigger are **not dangerous**. The Actor drove all
three placements, each on a build reporting `0 Error(s)`:

| # | Placement | Result |
|---|---|---|
| **A** | moved into the `finally`, beside `FailUncompleted` | **GREEN** |
| **B** | hoisted out of the `if`/`else`, **after** it | **GREEN** |
| **C** | hoisted out of the `if`/`else`, **before** it | **RED** |

**A and B are unobservable** because `FailAll` has already completed those keys, so
`TrySetResult` cannot take effect. **Only C changes an answer.**

So what is actually pinned is the **ORDER relative to `FailAll`**, not the lexical
position of the call. The test's remark states exactly that
(`AdminConfigsLifetimeTests.cs:197`, verbatim: *"What is pinned is the ORDER
relative to `FailAll`, not the lexical position of the call — measured, not
assumed."*).

⚠ **The wrong danger model was propagated by the Critic AND by the coordinator AND
by this Manager** — it was relayed as the justification for grading 69.7 up, and I
passed it to the Actor in the fix brief in that form. The **grading-up was still
correct**, because C is real and ships a wrong answer green. The *mechanism* was
not.

⚠⚠ **The Actor REFUSED to write the brief's wording into the tree**, because doing
so would have laundered an unmeasured danger model into a permanent code comment.
That refusal is the single most valuable act of the round: a comment asserting
"moving this into the `finally` would break it" would have been **false, durable,
and citable**, and every later reader would have inherited it.

#### The pattern, now complete: no role is exempt; only measurement is

This is the **fourth** instance this phase of a conclusion resting on an unverified
premise, and the first where the premise came from a **Critic finding** rather than
from a grep:

  1. **Actor miscounts** — 69.5, two universals falsified by code in the same commit.
  2. **Coordinator miscounts** — the `isEmpty` retraction above (a filtered grep
     that dropped the refuting line).
  3. **Manager misattributes** — repeating (2) as the Critic's claim, then
     propagating it into a brief.
  4. **Critic mis-models** — 69.7's danger model, right conclusion, wrong mechanism.

**Every role in the loop has now produced one.** The only thing that has caught all
four is **measurement**, which is why §9.1 items 7 and 10 are requirements rather
than advice.

#### Also recorded

The Actor's mechanical prose sweep caught **2 of its own 11** quantifier hits and
narrowed them **pre-commit** — over-claims it introduced *while fixing an
over-claim class*. The sweep working on the author who best understands the hazard
is the strongest evidence yet that it must stay **mechanical** rather than become a
careful read-through.

### ⏸ PARKED after Stage 2 — Stage 3 gated on ONE remaining condition

**Two conditions gate Stage 3; exactly one is now satisfied.**

  - ✅ **Stage 2 has closed.**
  - ❌ **The maintainer has NOT ruled on the Mode-B zero-op gap.** Stage 3's
    `alterReplicaLogDirs` shares the key-set-vs-row-set shape (§9.1 item 9), so
    the ruling shapes how it is written.

The **8-arg `ConfigEntry` constructor** ruling is also still open.

**Do not start Stage 3, do not pre-write it, do not refactor speculatively, and do
not make plan edits that presuppose either ruling.** The correct action while
gated is to remain idle.

---

## 15 · Mode-B gap list (consolidated) — DEFERRED, none closed in P3

✅ **MAINTAINER RULING 2026-09-09: P3 is strictly Mode A. Every gap requiring
Mode B is LISTED and deferred — none is closed in this phase.**

  - **No Rust, no new ABI function, no `cbindgen.toml` or `generator/` change, for
    any reason, in any stage.** The Mode-A proof — empty core-tree diff **plus a
    control-positive** — must hold at every commit through phase close.
  - ⚠ **A Mode-B gap encountered mid-stage does NOT stop the phase. This
    SUPERSEDES the earlier stop-and-escalate instruction.** The correct action is:
    implement the most honest Mode-A behaviour available, **document the divergence
    at the site**, add an entry here, and continue.
  - **Escalate only if there is NO honest Mode-A behaviour** — i.e. every option
    would silently report a wrong answer with no way to pin it.
  - **Stage 2's pattern is the template:** a workaround that carries its **own
    tripwire**, written to go **RED when the gap closes**.

**This section is a deliverable, not a scratch list. It is carried into the P9
close-out.** Every entry states: the Java contract · whether the Rust core already
implements it · the exact missing ABI capability · the C# workaround and its
divergences · the test that pins it.

### Gap 1 — `LogDirDescription.isCordoned()` (D15) · Stage 3

| | |
|---|---|
| **Java contract** | `public boolean isCordoned()` — `LogDirDescription.java:94` |
| **Rust core** | ✅ **Already implements it** — `pub fn is_cordoned(&self) -> bool`, `src/admin/log_dir_description.rs:113`; populated from the wire at `kafka_admin_client.rs:2109`; unit-tested by `cordoned_flag_is_carried` at `:143` |
| **Missing ABI capability** | an accessor exposing the flag, e.g. `kafka_admin_LogDirDescription_is_cordoned`. Verified absent: `grep -ci "cordoned"` over the header → **0**, against a control-positive `kafka_admin_LogDirDescription_total_bytes` → **1** |
| **C# workaround** | **None — the member is OMITTED.** ⚠ Deliberately not faked: no stub, no `false` default, because `false` asserts *"not cordoned"* when the truth is *"cannot know"*. **Absence is the honest encoding.** |
| **Divergence** | `LogDirDescription` is knowingly incomplete against Java. **DoD §2 will legitimately flag it** — the self-review must say so and cite this entry. ⚠ **Second-order consequence, recorded:** Java's three constructors are **public** (`:38`, `:42`, `:46`), and the two shorter ones default `isCordoned` to `false`. The C# constructor is therefore **`internal`** — a public one would either take a parameter the type cannot store or silently bake in the very `false` this decision exists to avoid. Only the result marshaller builds one. |
| **Test that pins it** | ✅ **LANDED (Stage 3)** — `PublicAdminLogDirsShapeParityTests.LogDirDescription_HasNoCordonedMember_AndTheSweepThatProvesItFindsTheOthers`. It sweeps every member at every accessibility (including backing fields) for the substring `Cordon`, sweeps the whole exported surface so the name cannot reappear under another owner, and asserts the constructor is not public. ⚠ It carries its **own positive control** — the same walk must still find `TotalBytes` and `Error` — because an absence assertion over a member walk is vacuously true if the walk finds nothing. |

### Gap 2 — a resource carrying ZERO operations in `incrementalAlterConfigs` · Stage 2

| | |
|---|---|
| **Java contract** | The future map is built **per resource** — `KafkaAdminClient.java:2889-2896`, `for (ConfigResource resource : resources) futures.put(resource, new KafkaFutureImpl<>())` — and `createRequest` passes `new IncrementalAlterConfigsRequest.Builder(resources, configs, …)`, so **the resource itself travels in the request** and a zero-op resource is a legal no-op alter that Java completes **successfully**. |
| **Rust core** | ✅ **Would behave correctly** — `src/admin/mock_admin_client.rs:629-636` resolves the topic and returns `UnknownTopicOrPartition` (*"No such topic as {}"*) **before** `apply_alter_ops`. The divergence is introduced **solely by the FFI row encoding**. |
| **Missing ABI capability** | **a way to express a resource carrying zero operations.** `src/ffi/admin.rs:4233-4262` builds the resource map from **rows alone** (`out.entry(ConfigResource::new(..)).or_default().push(..)` inside a `for i in 0..n` loop), so a resource with no rows **never enters the map and is never sent**. |
| **C# workaround** | `CompleteKeysWithNoRequest`, called from `CompleteKeyedVoid`'s **success branch only** (`AdminOperation.cs:356` → single call site `AdminCallbacks.cs:697`). Completes zero-row keys locally so they succeed, matching Java's ordinary-case outcome. |
| **Divergences** | Reproduces Java for the ordinary case, but **not** where the broker would have had something to say — a resource that **does not exist**, an **authorization failure**, or **`validate_only = true`**. In those cases C# reports success where Java (and the Rust core) report an error. |
| **Test that pins it** | the **A/B divergence test**: the same absent resource **succeeds** with zero ops and **faults** with one op. ⚠ **Expected to go RED when this gap closes**, and that expectation is stated at the site — not only in a report — so a future reader does not read it as a regression. Plus the 69.7 order-invariant test, which pins that the local completion happens **after** `FailAll`, never before. |

⚠⚠ **Gap 2 is DIFFERENT IN KIND from Gap 1.** Gap 1 **omits a feature** — honest
absence, and the caller can see nothing is there. Gap 2 returns **success where
Java and the Rust core both return an error** — a **wrong answer**, which the
caller cannot distinguish from a real one. **Do not let Gap 1's "omit it honestly"
disposition be read as a precedent for Gap 2's class.**

### Gap 3+ — Stage 3 outcome: NO new Mode-B gap

Stage 3 (`describeLogDirs` / `alterReplicaLogDirs` / `describeReplicaLogDirs`) added
**no** entry here. Every capability the three RPCs need is exported by the header, so
the whole stage is Mode A and Gap 1 remains the only `isCordoned`-shaped omission.
Two things that *look* like gaps were each checked and are not:

  - **`describeReplicaLogDirs` can fault a requested key** (`FailUncompleted`) when the
    result omits it. This is **not** an ABI gap: the header's own
    `..._count` docs state that Java's real client seeds a future per requested replica
    (`KafkaAdminClient.java:3066-3068`) and completes every one (`:3141-3145`), and the
    Rust core does the same — so against a real client the path never fires. Only
    `MockAdminClient` omits replicas of unknown topics
    (`mock_admin_client.rs:1352-1355`), which is what makes the path reachable in a
    broker-free test at all. The fault is deliberate over a locally-fabricated default,
    for Gap 1's reason: a default `ReplicaLogDirInfo` is indistinguishable from the real
    client's answer for a replica the broker genuinely does not host, so completing
    locally would invent data. Pinned by
    `AdminLogDirsLifetimeTests.AReplicaTheMockOmits_FaultsThatKeyWithAMessageNamingIt`.
  - **`alterReplicaLogDirs` cannot hit Gap 2's zero-row shape.** Checked *before* the RPC
    was written, per §9.1 item 9: Java's input is `Map<K, V>`, not
    `Map<K, Collection<V>>`, so every key carries exactly one value and produces exactly
    one row. `count` always equals the key count; nothing is completed locally and no
    divergence arises. The only way a key could still lose its row — a null topic or null
    log dir, which the ABI *silently skips* — is turned into an `ArgumentException` at the
    C# boundary instead.

**One measured TEST-COVERAGE gap, which is not a Mode-B gap and is deliberately not
listed above.** The nested `kafka_admin_LogDirDescription_error` is exported and read
correctly, but no broker-free path produces a *non-null* one: the Rust mock builds every
description with `LogDirDescription::new(None, …)` and carries `existing.error().cloned()`
forward, always `None` (`mock_admin_client.rs:1251-1262`), and a `LogDirDescription_t` is
reachable only through a `describeLogDirs` result root. **Measured, not assumed:** a
destroy-after-read injected at that site left the suite green, while a control-positive
throwing on a null pointer at the same site turned exactly the two `describeLogDirs` tests
red — proving the site executes and the pointer is null every time. The write-up lives once
in `AdminLogDirsMarshalTests.TheNestedDirectoryError_IsAFieldOnASuccessfulDescription`,
which pins the *shape* instead (a description carrying an error is an ordinary value that
faults nothing), with the structural no-native-state sweep covering the field itself.

### Stage 3 — `a432e62c`, review in flight

**`a432e62c`** — "describeLogDirs, alterReplicaLogDirs and describeReplicaLogDirs",
on `0d1587be`. **1178** tests both TFMs (floor 1124). Manager-verified: **0** files
outside `bindings/dotnet/` against a 21-file control; Mode A **0** core-tree lines
against a control-positive of 57 files / +12302 / −78; `ConfigEntry.cs` at **0**
diff lines, so the open ctor ruling was not pre-empted.

#### ⚠⚠ THE STAGE'S HEADLINE IS A NEGATIVE RESULT — a green injection that means UNREACHABILITY, not safety

Three runs, not two:

| Run | Injection | Result |
|---|---|---|
| 1 | per-key `*Result_get_error(i)` destroyed after read | **HOST ABORT** at exit code 0, summary still reading `Passed!` |
| 2 | nested `LogDirDescription_error` destroyed after read | **GREEN** |
| 3 | **control-positive** — throw-if-null at that same nested site | **RED, exactly 2 tests** |

**Run 3 is what makes run 2 interpretable.** It proves the nested site *does*
execute on every walk and the pointer is *always* null. So run 2's green is a
**measured test-coverage gap**, not a verified site.

⚠ **A two-run version of this experiment would have concluded "both sites safe" —
the exact false conclusion run 3 prevents.** Record this as the phase's clearest
illustration of why a control-positive is not ceremony: without it, an *absence of
signal* is indistinguishable from a *negative result*.

**Mechanism, Manager-verified independently:** **0** call sites in the core pass
`Some(...)` for the `LogDirDescription` error, against a control-positive of **11**
total construction sites (`mock_admin_client.rs:1251`, `log_dir_description.rs:135`,
`ffi/admin.rs:20144` / `:20157` / `:20167`, and the rest). No mock-reachable path
yields a non-null nested error.

⚠ **§9.1 item 10 vindicated at the level of EVIDENCE QUALITY, not merely
correctness.** The injections were **named in the Manager's brief**; the Actor
**measured rather than repeated**; and the answer **differed from what the brief
implied**. The Actor then **demoted three comment claims it had written before the
measurement** — including one in `LogDirMarshal`'s remarks asserting that *both*
injections were decisive. That is the rule working on its author's own prose,
which is the hardest case.

### ✅ STAGE 3 CLOSED — Critic 69 round 5: **0 findings**. M15/P3 COMPLETE.

**Phase chain:** `44b0b4c7` → `6ab8a5bf` → `11a24a34` → `45680c3c` → `1b0fec85`
→ `0d1587be` → `a432e62c`. **1178** tests both TFMs. Mode A for the **whole
phase**: 0 core-tree lines, control-positive 57 files / +12302 / −78.

#### ⚠⚠ A CANDIDATE RULING WAS REJECTED AS A FALSE POSITIVE — the FIFTH instance of this phase's signature defect

The Critic proposed requiring that a quoted phrase appear literally in the
document it cites, on the ground that `"non-blocking getter → sync property"` is
quoted across several files but *"those words are not in it and there is no
table."*

**There is a table, and the row exists.** Manager-verified:
`bindings/dotnet/CLAUDE.md:552` reads verbatim

    | Non-blocking **getter** | sync **property** |

inside a markdown table under `### Sync vs async — the governing rule` (`:534`).
Control-positive: **2** table rows contain "getter", against **49** total table
rows in the file. The Critic cited **`:147`** — an incidental inline code comment
(`getters → properties`) — and missed `:552`, the actual governing row. The
quoted phrase is a fair paraphrase, differing only in markdown emphasis and an
arrow.

**So the citations in those 7 files are correct, the rule is unnecessary, and
adopting it would have driven churn across 7 files in 4 phases to fix nothing.**

⚠ **Not filed against the Critic.** It declined to file this against Stage 3 —
the right call — and raised it as a **candidate** rather than acting on it. **The
process worked; only the premise was wrong.**

**The tally, now five, one distinctive thing about each:**

| # | Role | Where it landed |
|---|---|---|
| 1 | **Actor** (69.5) | two universals in **code comments**, falsified by code in the same commit |
| 2 | **Coordinator** | a **filtered grep** reported as a count, silently dropping the refuting line |
| 3 | **Manager** | a **misattribution** — flagging (2) correctly but pinning it on the Critic, then propagating it into a brief |
| 4 | **Critic** (69.7) | a **danger model** — right conclusion, wrong mechanism |
| 5 | **Critic** (this) | a **proposed RULE** |

⚠ **The fifth is the worst place for it, because a rule propagates by design.**
The first four cost a comment, a citation, an attribution and a brief; a rule
would have cost 7 files across 4 phases. **Every role in the loop has now produced
one. Only measurement has caught any of them.**

#### The round's genuine highlights

  - **The three-run asymmetry, with the inference STATED rather than gestured at:**
    run 3 red proves the nested site executes; run 2 green then proves nothing
    reached it with a non-null pointer, since a non-null one freed as owned is
    exactly what aborted the host in run 1. Together: **the site runs, always with
    null — unreachability, not safety.**
  - **Two injections that test the GUARDS rather than the code** — a fake public
    accessor → RED on the census sweep; a vacuous D15 absence walk → RED. Checking
    that the tests **would fail if they should**, which is the right instinct after
    a phase in which two tests passed for the wrong reason (69.2, 69.6).
  - **The discriminating public-shape detail:** Java's `ReplicaInfo` has **no**
    `equals`/`hashCode`, and the binding correctly omits them — the **opposite**
    call from Stage 2's `ConfigEntry`, where Java has them and the binding added
    them. **Both right, and opposite.** That is precisely what a per-type
    reflection walk buys over sampling.
  - **§9.1 item 9's pre-write check was performed and recorded** at
    `NativeAdminClient.cs:1683-1689` — and the Actor went further, finding the
    residual row-loss path (the ABI skips a NULL log dir) and closing it with an
    `ArgumentException`. The item did its job: cheap, not rediscovered in review.
