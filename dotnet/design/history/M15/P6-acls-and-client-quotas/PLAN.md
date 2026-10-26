# M15 / P6 — ACLs & client quotas (.NET binding)

> **Status:** 🟡 **DRAFT — pending maintainer approval. Implementation NOT authorized.**
> **Agent number:** **N = 76** (re-derived from the filesystem; §0.1)
> **Mode:** **A** — verified, not inherited (§0.2). Zero Rust-core change, zero
> C-ABI change, zero generated-header change.
> **Branch:** work continues on `prashah_dev_dotnet_binding`. Do **NOT** create a
> branch, do **NOT** merge, rebase or squash.
> **Scope:** `CreateAcls`, `DescribeAcls`, `DeleteAcls`, `DescribeClientQuotas`,
> `AlterClientQuotas` — 5 RPCs, 18 net-new C# types.
> **Roadmap row:** `design/current/PLAN-M15-admin-client.md` §8, M15/P6.

Bookkeeping (agent-number rule, environment traps, DoD gates, comment-file
mechanics) lives in the roadmap — this plan cites it rather than restating it.
What follows is overwhelmingly the ABI read, the shapes, and the type design.

---

## §0 — Ledger and mode (terse)

### §0.1 Agent number: **N = 76**, re-derived from the filesystem

Per roadmap §8.1 (*"re-derive from `find -name 'COMMENTS*.md'`, never from this
table alone"*). Repo-wide enumeration across **both** `bindings/dotnet/` and the
repo root returns a maximum of **75** (M15/P5, closed and archived at
`design/history/M15/P5-groups-and-group-offsets/COMMENTS.DONE.75.md`). **No
`COMMENTS.76.*` exists anywhere.** Cross-space check: the repo-root Rust sequence
maxes at 64, `design/history/` at 48. **76 is free in both spaces**, and matches
the roadmap ledger's prediction. Nothing downstream shifts.

Comments land in `bindings/dotnet/COMMENTS.76.md`; resolved items move to
`COMMENTS.DONE.76.md`. Neither is ever `git add`ed (roadmap §12).

### §0.2 Mode A — verified

| Check | Evidence |
|---|---|
| All 5 RPCs exist in the Rust core | `src/admin/mod.rs:534` `create_acls`, `:539` `describe_acls`, `:544` `delete_acls`, `:550` `describe_client_quotas`, `:560` `alter_client_quotas` |
| All 5 have sync **and** `_async` C entry points | `confluent_kafka.h:7986/8023`, `8071/8105`, `8141/8176`, `8225/8258`, `8321/8359` — 10 declarations |
| All 5 `*Result_t` typedefs + `_destroy` present | `h:1040/1047/1054/1061/1068`; destroys at `h:7682/7715/7815/7897/7942` |
| All 5 callback typedefs present | `h:1078/1090/1102/1115/1128` |
| Working tree carries no Rust/ABI edit | `git status --porcelain -- src/ cbindgen.toml generator/ target/include/` → **empty** |

**Conclusion: the whole delta is C# under `bindings/dotnet/`.** The per-phase
Mode-A proof obligation (roadmap §2) is discharged at phase end by re-running
that `git status` / `git diff` and confirming the header hash is byte-identical.

⚠ **Build with `cargo build --features ffi`.** A bare `cargo build` exits 0 while
producing a symbol-less dylib and overwriting the good header.

---

## §1 — THE ABI READ (roadmap §8: *"load-bearing, not a formality"*)

### §1.0 The five accessor sets, classified against §4.4's six shapes

| RPC | C accessor set (`h:` lines) | Shape the accessors *name* | What Java's result type *publishes* (stored field) |
|---|---|---|---|
| `createAcls` | `count` :7646, `get_binding` :7659, `get_error` :7671, `destroy` :7682 | **2** | `Map<AclBinding, KafkaFuture<Void>>` `CreateAclsResult.java:30` + `all()` :47 — **genuine shape 2** |
| `describeAcls` | `count` :7692, `get_binding` :7704, `destroy` :7715 — **no `get_error`** | **3b** | `KafkaFuture<Collection<AclBinding>>` `DescribeAclsResult.java:30`; only `values()` :39, **no `all()`** — genuine 3b |
| `deleteAcls` | `count` :7725, `get_filter` :7736, `get_error` :7754, **`get_result_count`** :7767, **`get_binding(i,j)`** :7784, **`get_result_error(i,j)`** :7803, `destroy` :7815 | **NONE** | `Map<AclBindingFilter, KafkaFuture<FilterResults>>` `DeleteAclsResult.java:81` + `all() → KafkaFuture<Collection<AclBinding>>` :99 |
| `describeClientQuotas` | `count` :7825, `get_entity` :7836, **`get_quota_count`** :7848, **`get_quota_key(i,j)`** :7864, **`get_quota_value(i,j,out)`** :7884, `destroy` :7897 — **no `get_error`** | **3** | `KafkaFuture<Map<ClientQuotaEntity, Map<String,Double>>>` `DescribeClientQuotasResult.java:31`; only `entities()` :46 |
| `alterClientQuotas` | `count` :7907, `get_entity` :7918, `get_error` :7931, `destroy` :7942 | **2** | `Map<ClientQuotaEntity, KafkaFuture<Void>>` `AlterClientQuotasResult.java:31` + `all()` :52 — **genuine shape 2** |

**Unlike P5, the accessor sets do not lie here.** All five name the shape Java
publishes. P5's stored-field discipline (§1.4 there) still applies as a *check*,
and it passes for all five — each row above cites the stored field, not the
accessor set. The hazard this phase carries is different, and it is in the two
findings below.

### §1.1 Finding N1 — **the key axis is a marshalled object graph** (new)

Three of the five RPCs key their per-key map on a **borrowed child handle**, not
on a string or a scalar tuple:

- `createAcls` → `const kafka_common_AclBinding_t *` (`h:7659`)
- `alterClientQuotas` → `const kafka_common_ClientQuotaEntity_t *` (`h:7918`)
- `describeClientQuotas` → same entity handle as the aggregate map's key (`h:7836`)

Every prior key in M15 was `string` (P1, P5), `Uuid` (P2a) or a
scalar composite read from two accessors (`TopicPartition`, P2b/P4). This is the
first key that is a **rebuilt object graph**, and it has a consequence the walker
seam cannot enforce:

> **`AclBinding` and `ClientQuotaEntity` are used as `IReadOnlyDictionary` keys.
> Without value `Equals`/`GetHashCode` mirroring Java's, every per-key lookup a
> user performs misses silently** — `values()[binding]` throws
> `KeyNotFoundException` for a binding the user just submitted, and the tests
> would pass if they only ever enumerate the dictionary.

Java defines both: `ClientQuotaEntity.equals` `:61` / `hashCode` `:69` over the
entries map; `AclBinding.equals`/`hashCode` over `(pattern, entry)`. Mirror them,
including the nested `ResourcePattern` / `AccessControlEntry` (and the filter
counterparts, which are `DeleteAclsResult`'s keys).

**The walker seam is NOT affected.** `KeyedResultMarshal.Complete<TKey,TValue>`
(`:279`) already takes `readKey` as a reader over `(result, index)` — the 1c
generalization. A handle-typed key is *"the reader's business, not the walker's"*
(`KeyedResultMarshal.cs:270-276`). **No `KeyedResultMarshal` edit is required for
N1.**

### §1.2 Finding N2 — **`DeleteAclsResult` is a two-level nested result, and it uses the error channel BOTH ways** (new)

`DeleteAclsResult` matches **none** of §4.4's six shapes, nor sub-shape 1c, nor
3b. It is shape 1 whose per-key **value is itself an indexed list**, each element
carrying binding-**xor**-error:

```
count                       -> filters                       (outer)
  get_filter(i)             -> AclBindingFilter     KEY
  get_error(i)              -> the FILTER's future failing   FAULT  (borrowed)
  get_result_count(i)       -> |FilterResults.values()|      (inner)
    get_binding(i, j)       -> AclBinding                    VALUE
    get_result_error(i, j)  -> FilterResult.error()          VALUE  (borrowed)
```

Two things make this genuinely new, both stated verbatim in the header:

1. **The two error accessors are independent, and only one is a fault.**
   `get_error(i)` is *"the filter's future failing, meaning nothing was deleted
   for it"* (`h:7744-7747`) → **faults that filter's `Task`**. `get_result_error(i,j)`
   is *"Java's `FilterResult.error()`: the filter matched this ACL but deleting it
   failed … independent of `..._get_error`"* (`h:7793-7796`) → **a stored value
   inside the successfully-completed `FilterResults`**. This is the first RPC in
   M15 to use the borrowed-error channel as a *fault* and as a *value*
   simultaneously. `KeyedResultMarshal.cs:171-175` already warns that const-ness
   answers ownership and nothing else, and that fault-vs-value is answered by the
   Java return type — P6 is the phase where one RPC needs both answers at once.
2. **`get_binding(i,j)` and `get_result_error(i,j)` are complements**
   (`h:7775-7777`: *"precisely one of them is non-null"*), exactly as
   `get_value`/`get_error` are at the outer level of shape 1.

**Does N2 need a new walker callable, and therefore its own sub-phase?**

**No — and this is the phase's central technical judgment.** The inner walk fits
entirely inside a `readValue` reader, because readers are already typed
`(IntPtr result, int index) -> TValue` and the walker is explicitly agnostic to
what the reader does with the root. So `DeleteAcls` is expressed as:

```
KeyedResultMarshal.Complete<AclBindingFilter, DeleteAclsFilterResults>(
    result, s_deleteAclsAccessors, operation,
    readKey:   (r, i) => AclBindingFilterMarshal.Read(DeleteAclsResult_get_filter(r, i)),
    readValue: (r, i) => ReadFilterResults(r, i));   // ← the (i, j) walk lives HERE
```

`KeyedResultMarshal.cs` is **579 lines with 5 callables** (`Complete<TKey,TValue>`
:279, `Complete<TKey>` :351, `CompleteAggregate<TKey,TValue>` :407,
`CompleteList<TValue>` :479, `CompleteTwoLists<TFirst,TSecond>` :554 — P5's).
**P6 adds ZERO callables and edits ZERO existing ones.**

Contrast with P2's D7 split, which *is* the bar for a sub-phase: P2 had to
**generalize the walker's own type parameters** (`<TValue>` → `<TKey,TValue>`) and
restructure its value axis, i.e. edit P1's reviewed foundation. P6 does not touch
the foundation at all. The new work is confined to per-RPC readers and to two new
key types' equality contracts.

**Therefore P6 is neither "mechanical repetition" nor a sub-phase candidate: it
is one phase carrying two new mechanisms that live entirely in leaf code.**

⚠ **The one condition that would change this answer.** If, mid-implementation, the
Actor finds `Complete<TKey,TValue>`'s signature *cannot* express the nested value
without a signature change, that is a foundation edit and becomes an **escalation
to the Manager**, not a unilateral refactor. Per roadmap §8 this is exactly the
"assuming mechanical repetition is how a phase discovers mid-implementation that
it must rewrite the reviewed foundation" failure — surfacing it early is the
mitigation, absorbing it silently is the defect.

### §1.3 Finding N3 — **the roadmap's type list names a class Java does not have**

The M15/P6 roadmap row lists **`DeletedAcl`**. There is no such Java class:

```
kafka/clients/src/main/java/org/apache/kafka/clients/admin/DeletedAcl.java  -> ABSENT
kafka/.../clients/admin/DeleteAclsResult.java                              -> PRESENT
```

Java's nested types are **`DeleteAclsResult.FilterResult`** (`:39`, holding
`binding()` `:51` / `exception()` `:58`) and **`DeleteAclsResult.FilterResults`**
(`:66`, holding `values()` `:76`). `DeletedAcl` is ecosystem
(confluent-kafka-dotnet / librdkafka) vocabulary, and `bindings/CLAUDE.md §2.1`
forbids adopting it.

**This is the same class of roadmap error as P3's `ClusterDescription` removal.**
Correction applied in §3.2: P6 ships `FilterResult` / `FilterResults`, **not**
`DeletedAcl`. The roadmap row is corrected in P9's doc-sync, not edited here.

### §1.4 Finding N4 — **seven parallel arrays, not eight**

The roadmap row says *"8 parallel arrays"*. The header says **seven**, twice
verbatim — *"The bindings cross as seven parallel arrays rather than as handles"*
(`h:7949`) and *"The filters cross as seven parallel arrays"* (`h:8122`) — and the
declarations confirm it: `resource_types`, `resource_names`, `pattern_types`,
`principals`, `hosts`, `operations`, `permission_types`, then `count` (a scalar,
not an array). **Seven.** The eighth was presumably `count`.

Harmless as a count, but it matters as *design input*: seven arrays is exactly one
row per `AclBinding` field, which is what makes the single row-projector in §4.1
possible.

### §1.5 The four input marshalling shapes (this phase has four, not one)

| RPC | Input shape | Precedent |
|---|---|---|
| `create_acls` / `delete_acls` | **7 flat parallel arrays** + `count` (`h:7986`, `h:8141`) — byte-identical signatures | `alterPartitionReassignments` (P4) |
| `describe_acls` | **7 scalars** (`h:8071`) — Java takes one filter, not a collection (`h:8040-8041`) | none; trivially simpler |
| `describe_client_quotas` | **3 flat arrays** + `count` + `bool strict` (`h:8225`) | P3 config-resource arrays |
| `alter_client_quotas` | **ragged 2-level: 3 outer `char***`, 1 `double**`, 1 `bool**`, 2 per-row `int32_t*` counts** (`h:8321`) | `listConsumerGroupOffsets` (P5 D32) |

---

## §2 — THE TWO NULL-VS-ABSENT MODELS (the roadmap's second flagged hazard)

The roadmap says *"both families turn on a null-vs-absent distinction"*. They do —
but they are **two different distinctions with two different arities**, and
conflating them is the phase's likeliest silent defect.

### §2.1 ACL family: **binary** — `null` = wildcard, `""` = a literal empty name

The split is **by type**, not by field:

**Concrete types (`AclBinding`, `ResourcePattern`, `AccessControlEntry`) — nothing is nullable.**
Header, on every string accessor: *"Never null on a binding"* (`h:7443`, `:7467`,
`:7478`). And the enums are constrained: *"A binding never carries ANY
(`ResourcePattern` rejects it)"* (`h:7432`), *"never carries ANY or MATCH … those
are filter-only pattern types"* (`h:7456-7457`), same for operation (`h:7493`) and
permission type (`h:7506`). The `create_acls` entry point enforces this: *"ANY (1)
is rejected, as Java's `ResourcePattern` constructor rejects it"* (`h:7963-7973`),
and *"a NULL entry is rejected"* for names, principals and hosts.

**Filter types (`AclBindingFilter`, `ResourcePatternFilter`, `AccessControlEntryFilter`) — strings are nullable, enums use `Any`/`Match` sentinels.**
Header: *"or null when the filter matches any resource name (Java's null name).
**Null is distinct from a pointer to the empty string, which filters on the name
`""`**"* (`h:7528-7530`), and the same for principal (`h:7553`) and host
(`h:7564`). Enum-side, a filter *"may also carry ANY=1"* (`h:7517`) and MATCH=2
(`h:7542`), and *"no combination is rejected: Java's filter constructors accept
ANY and MATCH, which is what a filter is for"* (`h:8061-8063`).

**Design consequence — the marshalling rule:**

```
C# string? field   ->  null  ->  IntPtr.Zero                    (wildcard)
                   ->  ""    ->  pointer to a pinned "\0"       (literal empty name)
                   ->  "x"   ->  pointer to pinned "x\0"
```

This is ffi §A4's absent-vs-empty sentinel discipline, but on the **string** axis
rather than the byte axis: `Utf8Marshal.Pin(null)` must yield `IntPtr.Zero` and
`Utf8Marshal.Pin("")` must yield a **non-null** pointer to a NUL byte. A helper
that maps both to `IntPtr.Zero` turns a filter on `""` into a match-everything
filter, which **deletes ACLs the user did not ask to delete** on `deleteAcls`.
That is the highest-severity behavioural defect available in this phase and it
gets its own test (§5, T-N1).

### §2.2 Quota family: **ternary** — and Java encodes the third state as a *null `Optional`*

`ClientQuotaFilterComponent` carries `Optional<String> match` with **three**
states, documented verbatim at `ClientQuotaFilterComponent.java:33-36`:

> *"@param match if present, the name that's matched exactly / if empty, matches
> the default name / **if null**, matches any specified name"*

| Java construction | `match` field | ABI `match_types[i]` (`h:8205-8207`) |
|---|---|---|
| `ofEntity(type, name)` `:51` | `Optional.of(name)` | **0 = EXACT** — match `match_names[i]` exactly |
| `ofDefaultEntity(type)` `:61` | `Optional.empty()` | **1 = DEFAULT** — the built-in default entity |
| `ofEntityType(type)` `:71` | **`null`** (the reference) | **2 = SPECIFIED** — any *named* entity of the type |

The header states exactly why the discriminant exists: *"DEFAULT and SPECIFIED
both carry no name, so a null name alone could not tell them apart"*
(`h:8208-8211`).

**Design consequence:** a C# `string?` **cannot** model this — it has two states
and collapses DEFAULT into SPECIFIED, silently turning "the default user's quota"
into "every named user's quota". The ABI already supplies the honest model, so the
C# type carries the discriminant explicitly (§4.5, **D37**).

**Two more ternaries in the same family**, both already solved by explicit ABI
flags — mirror them, do not re-derive:

- **`ClientQuotaEntity.entries()` values are nullable** — a null value is the
  built-in default entity for that type. Header: *"null … **if the entry names the
  built-in default entity** for its type … A name of `""` is a real, distinct name
  and comes back as a pointer to an empty string"* (`h:7622-7628`), and on the
  write side *"A NULL `entity_names[i][j]` is Java's null map value: the built-in
  default entity for that type, **which is not the same as omitting the type and
  not the same as the name `""`**"* (`h:8287-8290`). → C#
  `IReadOnlyDictionary<string, string?>`.
- **`ClientQuotaAlteration.Op.value()` is a boxed `Double`** (`:30`, `:53`) — null
  means **remove** the quota, not "set to 0". ABI: `op_has_values[i][j] == false`
  *"is Java's `Op(key, null)`: **remove** that quota rather than set it. The flag
  is required because every `double`, including 0, is a legal quota value, so no
  sentinel could carry the distinction"* (`h:8291-8295`). → C# `double?`.
  Symmetrically on the read side, `get_quota_value` is the milestone's second
  `bool`-returning out-param accessor, *"as
  `kafka_admin_ListOffsetsResultInfo_leader_epoch` does"* (`h:7874-7876`).

---

## §3 — Types: what ships, and where

### §3.1 Placement follows the shipped convention, not the roadmap §6 sketch

The roadmap §6 sketch shows `Admin/Options/` and `Admin/Results/` subfolders.
**They do not exist**: `Admin/` is flat (82 `.cs` files, zero subdirectories), and
the `common`-package value types sit at the project root
(`AclOperation.cs`, `ConfigResource.cs`, `ElectionType.cs`, `GroupState.cs`).
Follow the shipped convention (P5 D23 / P3 D13/D16).

| Java package | → namespace | → folder |
|---|---|---|
| `org.apache.kafka.common.{acl,resource,quota}` | `Confluent.Kafka` | project root |
| `org.apache.kafka.clients.admin` | `Confluent.Kafka.Admin` | `Admin/` (flat) |

### §3.2 The 18 net-new types (control-negative `ZZZControl` → 0 confirms the sweep discriminates)

**Root namespace `Confluent.Kafka` — 12 net-new:**

| Type | Java | Shape |
|---|---|---|
| `ResourceType` | `common/resource/ResourceType` | enum; `Unknown=0, Any=1, Topic=2, Group=3, Cluster=4, TransactionalId=5, DelegationToken=6, User=7` (`h:7429-7430`) |
| `PatternType` | `common/resource/PatternType` | enum; `Unknown=0, Any=1, Match=2, Literal=3, Prefixed=4` (`h:7453-7454`) |
| `AclPermissionType` | `common/acl/AclPermissionType` | enum; `Unknown=0, Any=1, Deny=2, Allow=3` (`h:7503-7504`) |
| `ResourcePattern` | `common/resource/ResourcePattern` | `(ResourceType, string Name, PatternType)`; ctor **rejects** `Any` type, `Any`/`Match` pattern; value equality |
| `ResourcePatternFilter` | `common/resource/ResourcePatternFilter` | `(ResourceType, string? Name, PatternType)`; accepts everything; value equality |
| `AccessControlEntry` | `common/acl/AccessControlEntry` | `(string Principal, string Host, AclOperation, AclPermissionType)`; ctor rejects `Any`; value equality |
| `AccessControlEntryFilter` | `common/acl/AccessControlEntryFilter` | `(string? Principal, string? Host, AclOperation, AclPermissionType)`; value equality |
| `AclBinding` | `common/acl/AclBinding` | `(ResourcePattern Pattern, AccessControlEntry Entry)`; **value equality — N1** |
| `AclBindingFilter` | `common/acl/AclBindingFilter` | `(ResourcePatternFilter PatternFilter, AccessControlEntryFilter EntryFilter)`; **value equality — N1** |
| `ClientQuotaEntity` | `common/quota/ClientQuotaEntity` | `IReadOnlyDictionary<string,string?> Entries`; consts `User`/`ClientId`/`Ip` (`:33-35`); `IsValidEntityType` (`:37`); **value equality — N1** |
| `ClientQuotaFilterComponent` | `common/quota/ClientQuotaFilterComponent` | 3 static factories + explicit discriminant (**D37**) |
| `ClientQuotaFilter` | `common/quota/ClientQuotaFilter` | `Contains` `:49` / `ContainsOnly` `:59` / `All` `:66`; `Components` `:73`, `Strict` `:80` |
| `ClientQuotaAlteration` (+ nested `Op`) | `common/quota/ClientQuotaAlteration` | `(ClientQuotaEntity Entity, IReadOnlyCollection<Op> Ops)`; `Op(string Key, double? Value)` |

**`Confluent.Kafka.Admin` — 5 results + 5 options + 2 nested:**

`CreateAclsResult`, `DescribeAclsResult`, `DeleteAclsResult` (+ nested
`FilterResult`, `FilterResults` — **not** `DeletedAcl`, §1.3),
`DescribeClientQuotasResult`, `AlterClientQuotasResult`; and `CreateAclsOptions`,
`DescribeAclsOptions`, `DeleteAclsOptions`, `DescribeClientQuotasOptions`,
`AlterClientQuotasOptions`.

**Reused unchanged: `AclOperation`** — already at
`src/Confluent.Kafka/AclOperation.cs`, namespace `Confluent.Kafka`, with all 16
members `Unknown=0 … TwoPhaseCommit=15` including `Any=1`, matching `h:7488-7491`
exactly. **No edit required.**

### §3.3 `AclBinding` is FLAT at the ABI and NESTED in Java — the binding restores the nesting

There is **no** `kafka_common_AccessControlEntry_*` and **no**
`kafka_common_ResourcePattern_*` in the header (verified: 0 declarations each).
`AclBinding_t` exposes **7 flat accessors** — but every one of them is documented
*in terms of the Java nesting*: `resource_type` returns
*"`pattern().resourceType().code()`"* (`h:7429`), `principal` returns
*"`entry().principal()`"* (`h:7467`), and so on for all seven.

So the C ABI flattened a two-level Java structure, and restoring it is layer 4's
job (`bindings/CLAUDE.md §1.2`) — the same call as D3 (`TopicCollection`) and D5
(`*Options`). **Ship the nesting**: `AclBinding.Pattern.Name`, not
`AclBinding.ResourceName`. The reader assembles `ResourcePattern` from accessors
1-3 and `AccessControlEntry` from accessors 4-7.

---

## §4 — Per-RPC surface and the marshalling design

### §4.1 The 7-array destructuring — ONE row-projector serves three RPCs

`create_acls` (`h:7986`) and `delete_acls` (`h:8141`) have **byte-identical**
signatures; `describe_acls` (`h:8071`) is the same seven fields as **scalars**.
One projection rule therefore covers all three:

```
row i  <-  AclBinding                 |  AclBindingFilter
  [0] resource_types[i]   int32   <-  Pattern.Type                | PatternFilter.Type
  [1] resource_names[i]   char*   <-  Pattern.Name  (never null)  | PatternFilter.Name  (NULL ok)
  [2] pattern_types[i]    int32   <-  Pattern.PatternType         | PatternFilter.PatternType
  [3] principals[i]       char*   <-  Entry.Principal (never null)| EntryFilter.Principal (NULL ok)
  [4] hosts[i]            char*   <-  Entry.Host      (never null)| EntryFilter.Host      (NULL ok)
  [5] operations[i]       int32   <-  Entry.Operation             | EntryFilter.Operation
  [6] permission_types[i] int32   <-  Entry.PermissionType        | EntryFilter.PermissionType
```

**Implementation — `Internal/Interop/AclRowMarshal.cs`** (internal, `unsafe`):

- A single `Pin(count)` scope allocating **three `int[]`** (blittable, pinned via
  `GCHandle.Alloc(Pinned)`) and **four `IntPtr[]`** of per-string pinned buffers.
- Strings go through the §2.1 rule: `null → IntPtr.Zero`, `"" → non-null pointer
  to a NUL byte`. This is the one place the rule is implemented, so it cannot
  diverge between the concrete and filter paths.
- **Everything unpins in a `finally` after the submit returns.** The ABI copies
  the rows out during the call — the `create_acls` doc says the arrays are read
  within the call and the result is materialised before the callback
  (`h:7953-7958`) — so this is a **call-scoped pin** (ffi §A4), *not* the deferred
  producer-send pin. Do **not** hold a pin across the `Task`.
- `describe_acls` reuses the same projector for a **single row** and passes the
  seven values as scalars — no arrays, no pinning beyond the three strings.

**Precondition validation happens in C#, before any pin** (ffi §A5/§B5). The ABI
*does* reject ANY/MATCH on `create_acls` — but it reports that by **firing the
callback synchronously on the calling thread** (`h:8004-8007`), which is the
§4.5-case-2 path. Validating in the C# constructors instead (`ResourcePattern`
rejecting `Any`, `AccessControlEntry` rejecting `Any`) is both Java-faithful
(Java's constructors throw) and keeps the inline-callback path off the happy path.

### §4.2 `alter_client_quotas` — the ragged 2-level marshalling

Seven pointers, of which **four are arrays-of-arrays** (`h:8321-8332`):

```
entity_types   char*** [count][entity_counts[i]]   <- alteration i's entity, (type, name) pairs
entity_names   char*** [count][entity_counts[i]]   <- NULL entry = default entity (§2.2)
entity_counts  int32*  [count]
op_keys        char*** [count][op_counts[i]]
op_values      double**[count][op_counts[i]]
op_has_values  bool**  [count][op_counts[i]]       <- false = Op(key, null) = REMOVE
op_counts      int32*  [count]
```

Follows P5's D32 `listConsumerGroupOffsets` precedent — reuse that marshaller's
shape. Two ABI-side rejections to surface as C# preconditions (again, to keep the
inline-callback path off the happy path):

- **A repeated entity type within one alteration, or a repeated entity across
  alterations, is rejected** (`h:8300-8302`) — *"Java keys both by a `Map`, so a
  duplicate could only be silently dropped."* The header explains at `h:8304-8311`
  why this diverges from `alter_user_scram_credentials`; that reasoning is for P7,
  not P6, but do not "fix" the asymmetry.
- **An alteration with no entity types is rejected** (`h:8342`).

⚠ `[MarshalAs(UnmanagedType.I1)]` on `bool validate_only` **and** on the `bool`
return of `get_quota_value` (roadmap §7 gate 2). `op_has_values` crosses as a
`bool*` buffer — marshal it as a **`byte[]` of 0/1**, not a `bool[]`, since .NET's
`bool` is 4 bytes in the default marshaller and 1 byte only in a blittable
context.

### §4.3 Result shapes → C# surface

```csharp
// shape 2, handle-typed key (N1)
public sealed class CreateAclsResult {
    public IReadOnlyDictionary<AclBinding, Task> Values { get; }   // Java values() :40
    public Task All();                                             // Java all()    :47
}

// sub-shape 3b — one future over a collection; Java has NO all()
public sealed class DescribeAclsResult {
    public Task<IReadOnlyCollection<AclBinding>> Values();          // Java values() :39
}

// NEW: two-level nested, inner error-as-value (N2)
public sealed class DeleteAclsResult {
    public sealed class FilterResult {                              // Java :39
        public AclBinding? Binding { get; }                         // :51  — xor with Error
        public KafkaException? Error { get; }                       // :58  — Java exception()
    }
    public sealed class FilterResults {                             // Java :66
        public IReadOnlyList<FilterResult> Values { get; }          // :76
    }
    public IReadOnlyDictionary<AclBindingFilter, Task<FilterResults>> Values { get; } // :91
    public Task<IReadOnlyCollection<AclBinding>> All();             // :99
}

// shape 3 (aggregate), handle-typed key, nested value
public sealed class DescribeClientQuotasResult {
    public Task<IReadOnlyDictionary<ClientQuotaEntity,
                IReadOnlyDictionary<string, double>>> Entities();   // Java entities() :46
}

// shape 2, handle-typed key (N1)
public sealed class AlterClientQuotasResult {
    public IReadOnlyDictionary<ClientQuotaEntity, Task> Values { get; }  // Java values() :45
    public Task All();                                                   // Java all()    :52
}
```

**`FilterResult.Error` is a `KafkaException` built with
`KafkaException.FromBorrowedHandle`, never `FromHandle`** (roadmap §4.2 fact 2,
risk 1). It is a *stored value*, so it is constructed and kept — which makes this
the one place in P6 where a borrowed error outlives the walk. That is fine and
required: `FromBorrowedHandle` **copies** code/message/flags out and retains no
pointer, exactly like every other reader (`KeyedResultMarshal.cs:177-182`). The
*handle* still dies with the root.

### §4.4 `DeleteAclsResult.All()` — derive it, do not re-walk

Java `:99-121` is explicit, and it is **not** `allOf` + "no value":

```java
allOf(futures.values()).thenApply(v -> getAclBindings(futures));
// getAclBindings: for each filter's FilterResults, for each FilterResult:
//     if (result.exception() != null) throw result.exception();
//     acls.add(result.binding());
```

So `All()`:
1. awaits every per-filter `Task` (a **filter-level** failure faults it), then
2. iterates every `FilterResult` in filter order and **throws the first inner
   exception it meets**, otherwise accumulates the binding.

⚠ **A filter-level success with an inner `FilterResult.error()` still faults
`All()`, while `Values[filter]` completes successfully.** Those two are *supposed*
to disagree — it is the whole point of the two channels — and it is the
discriminating test (§5, T-N2). Also note Java's comment at `:97-98`: *"if the
filters don't match any ACLs, this is **not** considered an error"* — an empty
result is a successful empty collection, never a fault.

### §4.5 `ClientQuotaFilterComponent` — the discriminant, and a recorded deviation

**D37** (below) settles the public shape. Implementation:

```csharp
public enum ClientQuotaMatchType { Exact = 0, Default = 1, Specified = 2 }  // == ABI match_types

public sealed class ClientQuotaFilterComponent {
    public static ClientQuotaFilterComponent OfEntity(string entityType, string entityName);  // Java :51
    public static ClientQuotaFilterComponent OfDefaultEntity(string entityType);              // Java :61
    public static ClientQuotaFilterComponent OfEntityType(string entityType);                 // Java :71
    public string EntityType { get; }                          // Java entityType() :78
    public ClientQuotaMatchType MatchType { get; }             // the ABI discriminant
    public string? MatchName { get; }                          // non-null iff MatchType == Exact
}
```

No public constructor (Java's is `private` `:39`), so the illegal fourth state
(`Exact` with a null name) is **unrepresentable**, and `OfEntity` keeps Java's
`Objects.requireNonNull(entityName)` as an `ArgumentNullException`.

---

## §5 — Tests (the phase-specific ones; the standing battery is roadmap §9)

Every roadmap §9 mandatory test applies per phase and is not restated. These are
the P6-specific additions, each pinning a finding above.

| # | Test | Pins |
|---|---|---|
| **T-N1** | `AclBindingFilter` with `Name = ""` and with `Name = null` produce **different** submitted rows — asserted at the **submit seam** (the pinned pointer is `IntPtr.Zero` vs non-null), not through the mock. Must be shown to go RED if the marshaller collapses both to `IntPtr.Zero`. | §2.1 — the deletes-too-much defect |
| **T-N2** | A `deleteAcls` result with **filter-level success + an inner `FilterResult.error()`**: `Values[filter]` **completes**, its `FilterResults` carries the error as a value, and `All()` **faults** with that exact message. This is the test that discriminates a correct two-channel implementation from one that conflates them. | §1.2, §4.4 |
| **T-N3** | The three `ClientQuotaFilterComponent` factories produce `match_types` **0 / 1 / 2** — three distinct submitted values. A `string?`-based model collapses 1 and 2 and turns this red. | §2.2 |
| **T-N4** | `ClientQuotaAlteration.Op(key, null)` submits `op_has_values[i][j] == false`, and `Op(key, 0.0)` submits `true` with value `0.0`. Distinct outcomes, not one. | §2.2 |
| **T-N5** | `ClientQuotaEntity` entry with a **null name** vs `""` vs `"x"` — three distinct submitted rows, and on the read path three distinct `Entries` values (`null` ≠ `""`). | §2.2, `h:7622-7628` |
| **T-N6** | **Dictionary-key round-trip**: construct an `AclBinding` / `ClientQuotaEntity` equal-by-value to one the result returned, and look it up in `Values` — it must hit. Mutation check: remove `GetHashCode` and confirm it goes RED. | §1.1 — the silent-miss defect |
| **T-N7** | `DescribeAclsResult` exposes **`Values()` and no `All()`** (Java has none), and `DeleteAclsResult.All()` on a no-match result is a **successful empty collection**, not a fault (`DeleteAclsResult.java:97-98`). | §1.0, §4.4 |
| **T-N8** | `ResourcePattern`/`AccessControlEntry` constructors **throw** on `Any` (and `Match` for pattern type) with Java's message asserted as a string (DoD §3); the filter constructors **accept** them. | §2.1, `h:7432/7456/7493/7506` |
| **T-N9** | **Wiring guard** (P5 §6.3 / finding 70.12): `CreateAclsResult` and `AlterClientQuotasResult` have byte-identical accessor sets (`count`/`get_X`/`get_error`/`destroy`), so a cross-wired reader returns a plausible answer. Every reader added here gets an `[InlineData]` row + a guard entry **in the same commit**, with a deliberate cross-wire injection confirmed RED before committing. | §1.0 |
| **T-N10** | **Per-key error never destroyed** — verify by **injection** on `DeleteAcls`, which has **two** borrowed-error accessors (`get_error`, `get_result_error`). Destroy each deliberately, confirm RED, revert. A double free aborts the process; no managed assertion catches it. | roadmap risk 1 |

**Mock coverage — check before relying on it.** P5's experience (six of nine RPCs
with no mock coverage; `list_groups`' mock ignoring every filter) means the Actor
must **first** verify what `src/admin/mock_admin_client.rs` actually implements
for these five RPCs, and assert filters/inputs **at the submit seam** wherever the
mock ignores them. Report the finding in the first commit body.

---

## §6 — Coordination (VERBATIM from the maintainer; §3 of P5 is the precedent)

1. **Exactly ONE Critic review pass for the whole phase, run only after the Actor
   has fully finished implementing all five RPCs.** No mid-phase or interim Critic
   reviews. This mirrors the M15/P5 ruling (roadmap D6: *"P5 ships as ONE phase,
   no internal sub-stages, Critic once at the end"*) — the identical discipline
   applies to P6. Do **not** propose a sub-phase split (e.g. ACLs vs quotas) as a
   way to get more than one Critic pass; if the ABI-read exercise genuinely
   surfaces a new mechanism that needs its own sub-phase, flag it explicitly as an
   **escalation to the maintainer** rather than deciding it yourself.
2. **The Actor MAY use internal checkpoints / resumable stages for its own
   progress tracking** (e.g. landing one RPC per session/checkpoint, the way Actor
   75 did for M15/P5 after repeatedly dying to context exhaustion) — this is fine
   and even encouraged, since the maintainer is short on tokens and wants
   resilience against context/autocompact exhaustion. **But a checkpoint is a
   resume point, not a Critic-reviewable stage — it must never trigger an interim
   Critic pass.**
3. **The plan document itself prioritizes CODE AND LOGIC content over process
   narrative.** Bookkeeping sections are pointers to the roadmap, not restatements.

**Agent numbers:** Actor = `dotnet-actor` N=76; Critic = `dotnet-critic` N=76.
**Personas:** `dotnet-actor` / `dotnet-critic` — **never** `actor-executor` /
`kafka-critic`, which are Rust-shaped and review against the wrong ground truth.
**Fix cycles** after the single first review are unbounded and unstaged, until
`COMMENTS.76.md` is empty.

### §6.1 Suggested checkpoint boundaries (Actor's own bookkeeping only)

Ordered so the new mechanisms land late, on top of proven readers:
`AclBinding`/filter type family + `AclRowMarshal` → `CreateAcls` (shape 2, N1) →
`DescribeAcls` (3b) → quota type family → `AlterClientQuotas` (shape 2, N1) →
`DescribeClientQuotas` (shape 3 + nested value) → **`DeleteAcls` last** (N2, the
only genuinely new result arity).

### §6.2 Environment traps — VERBATIM in every Actor/Critic brief

This sandbox has a broken shell init that fabricates **false passes**:

1. **`PATH` is clobbered.** Every Bash call must start with
   `export PATH="/usr/bin:/bin:/usr/local/bin:/opt/homebrew/bin:$HOME/.cargo/bin:$PATH"`.
   `command -v` is **not** reliable here.
2. **`grep` is aliased to `ugrep`** — rejects some patterns and emits nothing,
   reading as a pass. Use `/usr/bin/grep` for anything relied on as evidence.
3. **`sed` may be missing** — use `awk` or `/usr/bin/sed` explicitly.
4. **`cat` is shadowed by a missing `bat` alias** — `cat > file <<'EOF'` silently
   writes a 0-byte file. Use `/bin/cat`.
5. **This is zsh** — unquoted `$var` is not word-split; unquoted globs abort with
   "no matches found". Always quote.
6. **A test filter matching zero tests exits 0** — always confirm the expected
   **count**, never the exit code.

**Standing context-budget carry-overs (P5 §3.A):** grep rather than read the
generated header (it is ~15k lines); **range-read this plan, never whole-file
it**; defer per-RPC Java-source reads until that RPC is being written; build with
`cargo build --features ffi`; **bound every test invocation's output** (an
unbounded `dotnet test` is ~1.16 MB in one call).

---

## §7 — Design decisions (continuing P5's numbering; P5 ended at D34)

**D35 — `DeleteAclsResult` ships `FilterResult` / `FilterResults`, NOT `DeletedAcl`.**
Java has no `DeletedAcl` class (§1.3); the nested types are `FilterResult` `:39`
and `FilterResults` `:66`. `DeletedAcl` is ckd/librdkafka vocabulary and
`bindings/CLAUDE.md §2.1` forbids adopting it. The roadmap row is corrected in P9
doc-sync.

**D36 — restore the Java nesting over the flat ABI.** `AclBinding` /
`AclBindingFilter` ship as `(Pattern, Entry)` pairs with real `ResourcePattern` /
`AccessControlEntry` types, although the ABI exposes only 7 flat accessors (§3.3).
Same call as D3 (`TopicCollection`) and D5 (`*Options`): the C ABI flattened a
shape it cannot express, and layer 4 restores it. Every ABI accessor doc already
names the Java path it flattens (`h:7429`, `:7467`), so the mapping is mechanical.

**D37 — `ClientQuotaFilterComponent` exposes an explicit `MatchType` discriminant
rather than mirroring Java's `Optional<String> match()`. RECORDED DEVIATION.**
Java's accessor `:88` returns an `Optional<String>` that is itself nullable — a
three-state encoding C# has no idiom for. `string?` has two states and would
collapse DEFAULT into SPECIFIED (§2.2), which is a behavioural defect, not a
stylistic one. The ABI already models it with an explicit discriminant
(`h:8205-8211`), and Java's three static factories remain the **only** public
construction path, so the shape a user writes is identical to Java's. Record at
the site per `definition-of-done.md` §7.

**D38 — validate ACL/quota preconditions in C#, before any pin.** The ABI rejects
ANY/MATCH, NULL names and duplicate entities by **firing the callback
synchronously on the calling thread** (`h:8004-8007`, `h:8241-8242`, `h:8342`) —
the roadmap §4.5 case-2 path. Java's own constructors throw for the same inputs,
so C#-side validation is both Java-faithful and keeps the inline-callback path off
the happy path (ffi §A5/§B5). **The case-2 path must still be tested** (roadmap §9
requires the `GCHandle` free on both inline paths) — validation does not remove
that obligation, it only makes the path exceptional.

**D39 — `AclBinding`, `AclBindingFilter` and `ClientQuotaEntity` implement value
equality.** Not a style choice: all three are `IReadOnlyDictionary` keys on the
public surface (§1.1), and reference equality makes every user lookup miss
silently. Mirror Java's `equals`/`hashCode` (`ClientQuotaEntity.java:61/:69`),
including the nested components.

**Open for the maintainer: none.** D35–D39 are all settled by evidence cited
above. The single item the Actor may hit mid-phase and must escalate rather than
absorb is §1.2's walker-signature condition.

---

## §8 — Definition of Done

Roadmap §9 in full, per phase. P6-specific additions:

- **Mode-A proof:** `git diff` over `src/ src/ffi/ cbindgen.toml
  target/include/confluent_kafka.h generator/` empty; header hash byte-identical.
- **Exhaustiveness walk:** the 5 RPCs checked off against `src/admin/mod.rs`'s 46
  methods — a walk, not a recollection.
- **`MockAdminClient` audit** for these 5 RPCs against
  `src/admin/mock_admin_client.rs` per `admin-client.md` §9: a method Java's own
  mock implements must be implemented; only a method Java's mock leaves as
  `UnsupportedOperationException` may surface an unsupported error, each citing the
  exact Java line.
- **DoD §10 (hot-path allocation audit): N/A** — stated explicitly, never silently
  skipped (`admin-client.md` §10).
- **DoD §11:** N/A to `IAdmin`, but verify the 5 new methods stayed plain `fn`,
  only `Close` returns `Task`, and no `async` bled into the marshallers.
- **Shape justification per RPC:** one line in each commit body citing the **Java
  file and line of the stored field** (§1.0's table is the source). Five lines,
  five cites — the Critic verifies all five.
- Gates are **CI-only** for Docker/rustfmt/clippy in this sandbox; do not block
  loop closure on them.

---

## §9 — Risks specific to P6

| # | Risk | Mitigation |
|---|---|---|
| 1 | **`null` vs `""` collapsed in the filter marshaller** → `deleteAcls` matches everything and deletes ACLs the user did not request. Passes every round-trip test. | §2.1's single-site rule; **T-N1** with a RED-confirmed injection. |
| 2 | **DEFAULT vs SPECIFIED collapsed** in `ClientQuotaFilterComponent` → "the default user's quota" silently becomes "every named user's quota". | **D37**'s explicit discriminant; **T-N3**. |
| 3 | **Handle-typed keys without value equality** → every `Values[key]` lookup misses. Invisible to enumeration-only tests. | **D39**; **T-N6** with a `GetHashCode`-removal mutation check. |
| 4 | **The two `deleteAcls` error channels conflated** → either an inner failure silently disappears from `All()`, or a matched-but-undeleted ACL wrongly faults its filter's `Task`. | §1.2/§4.4; **T-N2**, the discriminating test. |
| 5 | **Two borrowed-error accessors on one RPC** → double free, process abort, no managed assertion sees it. | `FromBorrowedHandle` only; **T-N10** injection on *both* accessors. |
| 6 | **Cross-wired reader** between `CreateAclsResult` and `AlterClientQuotasResult` (byte-identical accessor sets) returns a plausible answer. | **T-N9** wiring guard + injection, same commit. |
| 7 | **Walker signature turns out insufficient for the nested value** → a mid-phase rewrite of P1's reviewed foundation. | §1.2's escalation condition, flagged up front rather than discovered late. |

---

## §10 — Approval

This plan is a **draft**. Implementation is **not** authorized until the
maintainer approves it. On approval the Manager archives it at this path and
spawns `dotnet-actor` N=76; `dotnet-critic` N=76 is spawned **once**, only after
the Actor reports all five RPCs complete and green (§6).
