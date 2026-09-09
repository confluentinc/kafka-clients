# M15 — .NET Admin client binding (`IAdmin` / `KafkaAdminClient` / `MockAdminClient`)

**Status:** **APPROVED 2026-09-08.** All six design decisions ruled (§1, D1–D6)
and the plan approved by the maintainer. The actor–critic loop is authorized.

⚠ **Authorization is per-phase, not milestone-wide.** **M15/P1 only** is
authorized as of 2026-09-08. Each subsequent phase needs its own explicit
go-ahead from the maintainer. This is deliberate and is §8's *"Why P1 is scoped
this way"* argument enforced as process: P1 lands the shared mechanism that
P2…P8 merely repeat, so the maintainer reviews **real code** at the P1 boundary
before authorizing the rest. **Do not treat approval of this roadmap as approval
to start P2.**

Per-phase plans are split out under
`bindings/dotnet/design/history/M15/<Phase>/PLAN.md` as each is authorized.

**Scope:** restore the Java `org.apache.kafka.clients.admin.Admin` shape in
idiomatic C# on top of the **already-shipped** Admin C ABI (PR #148) — the .NET
counterpart of what `bindings/python/admin.py` did for Python, but targeting a
**more Java-faithful shape** than Python chose (§5.3).

**Mode:** **A** (`bindings/dotnet/CLAUDE.md §6.1`) for every phase — verified in
§2, not assumed.

**Agent numbers:** **N = 66 … 74** (next free in the binding's own sequence;
highest used is 65 — M11/P3.1, closed). See §12.

**Milestone number:** **M15** — the next unused in the binding's own sequence
(`bindings/dotnet/design/history/` holds M0…M14; M14/P2 is the newest closed
phase). The binding keeps its own numbering, independent of the repo-root Rust
`design/`.

**Location note.** This is a multi-phase roadmap, so it lives in
`design/current/` rather than a single `design/history/<M>/<P>/`, following the
`PLAN-M9-consumer-callback-parity.md` precedent. On approval the Manager splits
it: one `PLAN.md` per phase under `bindings/dotnet/design/history/M15/<Phase>/`,
per `bindings/dotnet/CLAUDE.md §8.4`.

---

## 0 · Rules-vs-tree reconciliation (do this before any code)

Two governing documents make claims that the current tree falsifies. Per root
`CLAUDE.md` (*"Any change to this prompt is to be avoided by automatic agents"*),
these are **raised, not edited** — the fix goes through the `agent-roles.md`
process, and lands as a P9 doc-sync deliverable.

### 0.1 `.claude/rules/admin-client.md` §11 — obsolete in every premise

| §11 records | Current state |
|---|---|
| *"the only FFI is `src/ffi/producer.rs`"* | `src/ffi/consumer.rs`, `src/ffi/consumer_handle.rs` **and** `src/ffi/admin.rs` all exist and are merged |
| *"the only Python binding is `producer.py`"* | `bindings/python/consumer.py` and `bindings/python/admin.py` (4 062 lines) both exist |
| *"there is no `_ProducerBase`/`AsyncProducer` split"* | `admin.py` ships `Admin`/`AsyncAdmin` bases with `_run_sync`/`_run_async` |
| *"the plan's 'async C API'/'mirror consumer' wording is a design conflict to be resolved with the Manager"* | Resolved and shipped in PR #148 |
| Says nothing about .NET | The .NET binding did not exist when §11 was written |

**Suggested update: delete or archive §11.** Left as-is it will send a future
agent to resolve a conflict that no longer exists. **No action taken here.**

### 0.2 `bindings/dotnet/CLAUDE.md §3:422` — stale Mode classification

> *"The **admin client** (`IAdminClient`) is still **Mode B** — sketched once its
> C ABI lands (§6.3)."*

The C ABI **has** landed (§2), so Admin is **Mode A**, and D1 has settled the type
name — so the concrete replacement can be stated now. **Proposed replacement text**
(to be applied in P9, when it is true of shipped code — not now):

> The **admin client** (`IAdmin`) is **Mode A** — its C ABI landed with PR #148
> (`src/ffi/admin.rs`: 48 RPC entry points + `close`, each with a sync and an
> `_async` form), so the binding is C#-only. Shipped in M15: `IAdmin` with
> `KafkaAdminClient` / `MockAdminClient` and 46 Java-shaped RPC methods under
> `Confluent.Kafka.Admin`. Unlike the producer and consumer there is **no
> sync/async interface pair** — Java's `Admin` methods do not block, so each is a
> plain sync method returning a `*Result` holding one `Task<T>` per key, and only
> `Close` returns a `Task` (see the §4 **admin per-key result** divergence).

Two further spots need the same sweep: §2's file map shows `Admin/` as a
hypothetical, and §5's index needs a Part C row once §10 lands.

---

## 1 · DESIGN DECISIONS — ruled 2026-09-08

All six were **ruled by the maintainer on 2026-09-08**, each confirming the
default this plan proposed. They are recorded here as **D1–D6** and referenced by
that number throughout.

**The reasoning below is kept deliberately, not trimmed as spent scaffolding.**
Three of these decisions (D1, D2, D3) put .NET at odds with either
confluent-kafka-dotnet or the Python sibling, so a future reviewer diffing the
bindings will land on them and ask why. The answer needs to be *here*, not
reconstructible only from a chat transcript.

| # | Decision | Ruling |
|---|---|---|
| D1 | Interface name | **`IAdmin`** (not `IAdminClient`); impls `KafkaAdminClient` + `MockAdminClient` |
| D2 | One interface or a pair | **ONE `IAdmin`**; no `IAsyncAdmin` |
| D3 | `TopicCollection` | **Restore the Java type**; 46 C# methods, not Python's 48 |
| D4 | `MockAdminClient` timing | **Lands in P1** |
| D5 | `*Options` | **C# POCOs**, one per Java class, nullable ⇒ Java defaults |
| D6 | Phase count | **9 phases**; P5 may split into P5a/P5b if the round runs long |

### D1 — Interface name: **`IAdmin`** ✅ ruled 2026-09-08

Java's **interface** is `Admin`; `AdminClient` is a separate abstract class with
the static `create()`; the concrete impl is `KafkaAdminClient`.

- `bindings/CLAUDE.md §2.1` is firm: *"Do not adopt the language's existing
  ecosystem Kafka client shape."* confluent-kafka-dotnet's name is
  `IAdminClient` — adopting it is the thing that rule forbids.
- `bindings/dotnet/CLAUDE.md §4` maps Java `Producer` → `IProducer`, `Consumer` →
  `IConsumer`: mirror the Java name, add the C# `I` prefix (CA1715), change
  nothing else. Applied mechanically, Java `Admin` → **`IAdmin`**.
- But `bindings/dotnet/CLAUDE.md §3:422` already *writes* `IAdminClient`, in the
  stale sentence §0.2 is rewriting anyway.

**Ruled: `IAdmin`**, with `KafkaAdminClient` + `MockAdminClient` as the impls
(both Java's own class names, both matching `src/admin/`).

⚠ **This was flagged as a one-way door and ruled as one** — it is the public type
name, and `IAdminClient` is what a .NET developer arriving from ckd will expect.
The ruling accepts that friction deliberately: `bindings/CLAUDE.md §2.1` makes
ecosystem-shape adoption the one thing a binding may not do, and the whole point
of the four-layer model is that a name is recognizable *across* bindings. Do not
re-open this as an ergonomics question later — the ergonomic cost was known and
priced in.

### D2 — One interface, not a pair: **ONE `IAdmin`** ✅ ruled 2026-09-08

Producer and Consumer each ship a pair because their Java methods **block**, so
both mappings are defensible and the pair genuinely serves two audiences.
**Admin's methods do not block** (§5.2), so there is exactly one faithful
mapping and a second interface would be a **synonym**, not a choice — it would
double the surface while carrying no additional meaning.

**Ruled: ONE `IAdmin`.** No `IAsyncAdmin`.

This is the plan's sharpest divergence from the Python sibling, which ships
**four** classes (`AdminClient`/`AsyncAdminClient` + two mocks). The divergence
is *downstream* of a different earlier choice, not a disagreement about C#:
Python made its Admin methods **block** and return plain dicts, and a blocking
API genuinely does need an async twin to serve asyncio users. .NET's methods do
not block, so the fork Python needed never arises. Full argument in §5.3.

⚠ **The maintainer has since worked through the Python `Admin`/`AsyncAdmin` split
against this single-`IAdmin` design in detail, and endorses it** — this is a
reviewed, affirmed decision, not a default that merely went un-objected-to. A
future reviewer who notices the binding asymmetry (producer/consumer have pairs,
admin does not) should read §5.2 for why, and should not treat the asymmetry as
an oversight to "fix".

### D3 — `TopicCollection`: **restore the Java type** ✅ ruled 2026-09-08

Java has ONE `deleteTopics(TopicCollection)` and ONE
`describeTopics(TopicCollection)`. The C ABI cannot express the names-xor-ids
union safely, so it ships **split entry points** (`delete_topics` /
`delete_topics_by_ids`, `describe_topics` / `describe_topics_by_ids`) — that is
why 46 Rust trait methods become 48 C entry points. **Python kept the split** and
exposes 48 methods, rebuilding no `TopicCollection`.

**Ruled: restore `TopicCollection`; ship 46 C# methods**, each selecting its ABI
entry point from which factory built the collection.

The reasoning that decided it, kept because it is the general principle for every
future ABI-flattened union: **the C ABI split the method because C cannot hold the
invariant, not because Java's shape is wrong.** A binding that inherits the split
has let a layer-3 limitation leak into layer 4 — the exact failure
`bindings/CLAUDE.md §1.2` describes the four-layer model to prevent. C# *can* hold
it: a sealed class with a private constructor and two static factories
(`TopicCollection.OfTopicNames` / `.OfTopicIds`) makes names-xor-ids unbreakable
at **compile time**, strictly better than the runtime check Java's own
`TopicCollection` performs. Recorded as a deliberate divergence from the Python
sibling, justified by `bindings/CLAUDE.md §2.1` (target Java, not a sibling
binding) and §1.2 (*"rich-OO languages do substantial rebuilding"*).

### D4 — `MockAdminClient` lands in **P1** ✅ ruled 2026-09-08

`kafka_admin_MockAdminClient_new(int32_t num_brokers)` returns the **same**
`kafka_admin_AdminClient_t*` opaque type as the real ctor, so the entire RPC
surface works against it with zero extra ABI surface, and
`bindings/dotnet/CLAUDE.md §7.3` makes broker-free `Mock*` tests the *only*
unit-test vehicle.

**Ruled: `MockAdminClient` lands in P1.** Deferring it would mean deferring every
test in the milestone's riskiest phase.

### D5 — `*Options` as **C# POCOs** ✅ ruled 2026-09-08

Java has ~61 `*Options` classes. The ABI flattened them to scalar parameters —
there are **zero** `kafka_admin_*Options_t` handles (verified). Python flattened
them further, to keyword arguments (`timeout=`, `validate_only=`).

**Ruled: restore them** as plain C# POCOs, one per Java class, nullable with
`null` ⇒ Java defaults.

Same principle as D3: they are cheap (a handful of properties each), and omitting
them would bake the ABI's flattening into the public surface — precisely what
layer 4 exists to undo. The Python comparison does not transfer, because the
divergence is about *host idiom*, not about shape discipline: Python's keyword
arguments are that language's natural spelling of an options bag, whereas C# has
no equivalent idiom, so in C# the options object **is** the idiomatic form. Both
bindings are being idiomatic about the same Java shape.

### D6 — **9 phases** ✅ ruled 2026-09-08

§8 lays out **9** (8 build + 1 close-out). Python used 8 slices for the same
surface. .NET must additionally rebuild ~44 `*Result` classes, ~61 `*Options`
classes and ~67 data types — the D3/D5 rebuilding cost, arriving as schedule.

**Ruled: 9 phases** as laid out, **with P5 explicitly sanctioned to split into
P5a/P5b if the round runs long** (it carries nine RPCs and ten data types — see
§8). Splitting further is safe and needs no new ruling; merging is not
recommended, because each phase's size is what keeps a defect in the shared
mechanism reviewable.

---

## 2 · Mode A holds — verified, not assumed

`bindings/dotnet/CLAUDE.md §6.1`'s gate asks: *is the feature already exposed at
the C ABI?* For Admin, **yes**, on every axis:

| Check | Evidence |
|---|---|
| Admin C ABI present and merged | `src/ffi/admin.rs` (23 747 lines); reachable from `master` (`4e0c768c`, `3f13934a`, `03212264`) |
| Header carries the symbols | **477** distinct `kafka_admin_*` functions; **113** `kafka_admin_*_t` typedefs (66 opaque structs + 47 callback typedefs) |
| Every RPC has sync **and** async entry points | **48** RPCs + `close` = **49** base names × 2 = **98** entry points; **47** callback typedefs (the two by-name/by-id pairs share one each) |
| Rust core surface complete | `src/admin/mod.rs` declares **46** sync RPC methods + `async fn close`; `src/admin/` holds 167 files |
| A mock exists at the ABI | `kafka_admin_MockAdminClient_new(int32_t)` → `kafka_admin_AdminClient_t*` + 5 seeding functions |
| Python sibling already shipped on it | `bindings/python/admin.py`, 46/46 RPCs, zero omitted |
| `KafkaFuture::join_map_results` landed | `src/common/kafka_future.rs:192` |

**Conclusion: no Rust work, no new C ABI function, no `cbindgen.toml` change.**
The whole delta is C# under `bindings/dotnet/`.

**Mode-A proof obligation, every phase** (the M14/P1 and M13/P1 precedent):
`git diff <base>..HEAD -- src/ src/ffi/ cbindgen.toml target/include/confluent_kafka.h generator/`
must be **empty**, and the generated header hash byte-identical. A phase that
cannot show this has silently become Mode B and must stop — per
`bindings/dotnet/CLAUDE.md §8.1`, authoring Rust is the root `actor-executor`'s
job, not `dotnet-actor`'s. A genuine ABI gap is a **Rust-core dependency**
escalated to the Manager; it is *not* a licence to invent a managed workaround.

### Scope exclusions (absent from the Rust core, therefore out of scope here)

Verified absent from `src/admin/`: share-group RPCs (KIP-932), streams-group
RPCs, `describeMetadataQuorum`, `unregisterBroker`, the telemetry family
(`clientInstanceId` / `metrics` / `register|unregisterMetricForSubscription`),
and the deprecated `alterConfigs` (its `AlterConfigsResult` type exists, used by
`incrementalAlterConfigs`). **The binding does not add what the core lacks**
(`bindings/CLAUDE.md §2.6`). Two in-scope RPCs are Java-deprecated and ported
anyway for parity: `listConsumerGroups` and `listClientMetricsResources`.

---

## 3 · What's reusable vs. genuinely new

### Reuse unchanged

- **`Utf8Marshal`** — every Admin string is UTF-8, **NUL-terminated**,
  callee-owned (ffi §B3 row 2). There are **no** length-delimited slices in
  Admin; that form is the consumer fetch-batch path only.
- **`KafkaException`** — the flat error type and the
  precondition-vs-operational two-surface model (ffi §A5/§B5), verbatim.
- **`SafeHandleZeroIsInvalid`** — base for the new handles.
- **The Properties pattern** — `AdminClientProperties_{new,put,from_configs,destroy}`
  is byte-for-byte `ProducerProperties_*` / `ConsumerProperties_*`;
  `SafeAdminPropertiesHandle` is `SafeProducerPropertiesHandle` with three
  symbols changed.
- **Category-3 borrow-root marshalling** — `MetricMapMarshal`,
  `PartitionInfoListMarshal`, `TopicPartitionInfoMapMarshal` are the exact
  template for every `*ResultMarshal`.
- **The hookless one-shot completion bridge** — `OperationCompletionSource` and
  the consumer's `*_async` submit helpers (span-the-op `DangerousAddRef`,
  `GCHandle` as `user_data`, freed by the callback, `AbandonBeforeSubmit` on a
  submit throw). Admin is the **same** ffi §B6 family (§4.5).
- **`Node`, `TopicPartition`, `OffsetAndMetadata`, `KafkaException`** — already
  public in `Confluent.Kafka`; Admin **reuses**, never re-declares. This mirrors
  Python, which imports `Node`/`OffsetAndMetadata` from `consumer` and
  `KafkaError` from `producer` for exactly this reason.

### Copy-and-adapt

- The consumer's `*_async` submit helper → an Admin equivalent. **Drop** the
  single-op-in-flight assumption (Admin has no access guard and permits
  concurrent ops). **Keep** the span-the-op `AddRef`, the `GCHandle` discipline
  and `AbandonBeforeSubmit`.
- `TopicPartitionInfoMapMarshal` → `KeyedResultMarshal` (§4.3).

### Genuinely new

- **The per-key result bridge** (§4) — the one real design problem.
- **`KafkaException.FromBorrowedHandle`** — a *non-destroying* sibling of
  `FromHandle`. Mandatory: per-key errors are **borrowed** (§4.2), and the
  existing `FromHandle` destroys in its `finally`. Reusing it is a double-free.
- **~44 `*Result` C# classes** (one per `*Result_t`, plus the RPCs that have no
  result handle at all — §4.4).
- **~61 `*Options` C# POCOs** (D5).
- **~67 Admin data types** — the same set Python rebuilt.
- **`TopicCollection`** (D3) — Java puts it in `common`, so it goes at the root
  namespace, not under `Admin/`.
- **A public `Admin/` folder + `Confluent.Kafka.Admin` namespace** — the first
  topical folder in this binding (`bindings/dotnet/CLAUDE.md §2` anticipates it
  by name).
- **`ffi-marshalling.md` Part C · Admin** — §10.

**Only two input handle types exist** (`NewTopic_t`, `NewPartitions_t`). Every
other input crosses as **parallel arrays** — config resources as
`(int32_t* types, char** names, int32_t count)`; ACLs as **eight** parallel
arrays with no `AclBinding_t` at all. So the C# `AclBinding` / `ConfigResource`
classes are pure managed types that *destructure* at the call site — which makes
P6/P7 substantially lighter than their type counts suggest.

---

## 4 · THE ONE REAL DESIGN PROBLEM — per-key futures across a flattened ABI

### 4.1 The shape mismatch

| Layer | Shape |
|---|---|
| **Java** | `createTopics(...)` returns **immediately** with `CreateTopicsResult` holding `Map<String, KafkaFuture<TopicMetadataAndConfig>>` — one future **per topic**, resolving independently |
| **Rust core** | Preserved: `fn create_topics(&self, …) -> CreateTopicsResult` holding one `KafkaFuture<T>` per key (`admin-client.md` §1/§5) |
| **C ABI** | **Flattened away.** There is **no `kafka_admin_*Future*` type anywhere in the header** — verified. `src/ffi/admin.rs:83-84` says so outright: *"only independent per-key *timing* is lost (which C cannot express without a `KafkaFuture` type)"* |
| **.NET** | Must **restore** the Java shape: a sync method returning a `*Result` holding one `Task<T>` per key |

### 4.2 What the ABI hands back

Each multi-key `*Result_t` is a **flattened, index-addressed, fully-settled
table** — a Category-3 owned borrow-root in ffi §B2's taxonomy:

```c
int32_t     kafka_admin_CreateTopicsResult_count  (const kafka_admin_CreateTopicsResult_t *result);
const char *kafka_admin_CreateTopicsResult_get_key(const kafka_admin_CreateTopicsResult_t *result, int32_t index);
const kafka_admin_TopicMetadataAndConfig_t *
            kafka_admin_CreateTopicsResult_get_value(const kafka_admin_CreateTopicsResult_t *result, int32_t index);
const kafka_common_KafkaError_t *
            kafka_admin_CreateTopicsResult_get_error(const kafka_admin_CreateTopicsResult_t *result, int32_t index);
void        kafka_admin_CreateTopicsResult_destroy(kafka_admin_CreateTopicsResult_t *result);
```

Six facts, each load-bearing and each pinned by the header's own doc comments:

1. **`get_value` and `get_error` are exact complements** — *"null if that topic
   failed"* / *"null if that topic was created successfully"*. Exactly one is
   non-null per index. Java's per-key granularity, preserved.
2. **Per-key errors are BORROWED, and must not be destroyed** — verbatim:
   *"The pointer is borrowed from the result handle — read it with the
   `kafka_common_KafkaError_*` accessors, but do **not** destroy it."*
   ⚠ **This is why `KafkaException.FromHandle` cannot be used here.** It
   `_destroy`s in its `finally`. A naive reuse is a double-free — a process
   abort, invisible to every managed assertion.
   ⚠ **Const-ness is the ONLY ownership signal, and the same C type appears on
   both sides of it**: `_get_error(i)` returns `const …KafkaError_t*`
   (**borrowed**), while the callback's `error` parameter is a non-const
   `…KafkaError_t*` that the callback **owns and must free**. This is the single
   most likely source of a leak-or-double-free in this binding.
3. **A per-key failure is not a call failure.** Verbatim on the callback typedef:
   *"A per-topic failure arrives inside `result`, not as `error`."* Both the sync
   return value and the async `error` parameter stay null for a partially-failed
   batch.
4. **The result is fully materialized when the callback fires.**
   `admin.rs:3197-3206` harvests every per-key `KafkaFuture` into
   `KafkaFuture::join_map_results`, and `admin.rs:809` awaits it *before*
   enqueuing the completion job. `join_map_results` deliberately does **not**
   short-circuit: `map.insert(key.clone(), future.get().await)` — no `?` — which
   is exactly why it exists alongside `join_map`. Its loop is **sequential**, so
   total latency is bounded by the slowest key.
5. **Entries are sorted by key** — deterministic ordering, free for .NET.
6. **For the by-id variants the key is a base64 topic-id string**, not a binary
   UUID. `TopicCollection.OfTopicIds` must round-trip through that encoding.

### 4.3 The bridge

```
C# CreateTopics(topics, options)                    ← plain sync method, returns immediately
  ├─ validate preconditions (ffi §B5) — before any pin/marshal
  ├─ build kafka_admin_NewTopic_t[] (NewTopic_new / _put_config / _set_replicas_assignment)
  ├─ create ONE TaskCompletionSource<T> PER KEY, up front,
  │    each with RunContinuationsAsynchronously
  ├─ GCHandle.Alloc(the per-key TCS map)  →  user_data     ← publish BEFORE the call (§4.5)
  ├─ DangerousAddRef on SafeAdminHandle   →  held submit → callback (span-the-op)
  ├─ kafka_admin_AdminClient_create_topics_async(…, s_createTopicsCb, ud)
  ├─ destroy the NewTopic_t[] (input handles: the ABI copies out, caller retains ownership)
  └─ return new CreateTopicsResult(perKeyTasks)     ← Java's shape, restored

… later, on ONE OF THREE THREADS (§4.5):
s_createTopicsCb(result, error, ud):                ← fires exactly once
  ├─ try { …                                        ← total no-throw boundary
  │    if (error != IntPtr.Zero)                    ← OWNED: FromHandle destroys it
  │        → fault EVERY per-key TCS with FromHandle(error)
  │    else for i in 0 .. count-1:
  │        key = Utf8Marshal.PtrToString(get_key(i))              [borrowed → copy]
  │        err = get_error(i)
  │        if (err != IntPtr.Zero)
  │            tcs[key].TrySetException(FromBorrowedHandle(err))  [BORROWED → never destroy]
  │        else
  │            tcs[key].TrySetResult(Marshal(get_value(i)))       [borrowed → copy out]
  │  } finally { CreateTopicsResult_destroy(result); FreeGcHandle(ud); DangerousRelease(); }
  └─ any TCS left uncompleted is faulted, so no Task can ever hang
```

**Why one aggregate callback serves N per-key Tasks:** the callback receives the
result with *every* key already resolved (fact 4), so it has all the information
needed to complete all N `TaskCompletionSource`s in one pass. Creating the TCSs
*before* the submit is what lets the C# method return the `*Result`
synchronously — matching Java, where the futures also exist before any response
has arrived.

**⚠ The one honest deviation, to be documented on the public surface:** per-key
**granularity** is fully preserved (each `Task` carries exactly that key's value
or that key's error), but per-key **timing independence** is not — all N `Task`s
complete at the same instant, because the ABI resolved them together. In Java a
fast topic's future can complete before a slow one's. Nothing observable depends
on this for correctness (`All()` and per-key `await` behave identically), but it
is a real difference from Java and must not be papered over. Python has the
identical limitation for the identical reason. Recorded per
`definition-of-done.md` §7.

**Rejected alternative** (same reasoning the Rust FFI used, recorded so it is not
re-opened casually): expose the `KafkaFuture` itself as a C handle. Truest to
Java, but needs one opaque future type per distinct `T` across 46 result types —
a combinatorial explosion. **Revisit only if a concrete need for per-key timing
appears.**

### 4.4 The result-shape taxonomy — SIX shapes, not one

§4.2 describes the *representative* shape. Admin actually has six, and
`KeyedResultMarshal` must handle all of them. Getting this wrong is the most
likely way a later phase silently invents a non-Java surface.

| # | Java result shape | ABI | C# `*Result` |
|---|---|---|---|
| 1 | `Map<K, KafkaFuture<V>>` | 5 fns (`count`/`get_key`/`get_value`/`get_error`/`destroy`) | `IReadOnlyDictionary<K, Task<V>>` — e.g. `CreateTopicsResult`, `DescribeTopicsResult` |
| 2 | `Map<K, KafkaFuture<Void>>` | **4 fns — NO `get_value`** | `IReadOnlyDictionary<K, Task>` — e.g. `DeleteTopicsResult`, `CreatePartitionsResult`, `IncrementalAlterConfigsResult`, `AlterConsumerGroupOffsetsResult` |
| 3 | one `KafkaFuture<Map<K,V>>` | one result handle, no per-key errors | one `Task<IReadOnlyDictionary<K,V>>` — e.g. `ListTopicsResult`; a failure faults the **whole** task |
| 4 | `valid()` + unkeyed `errors()` | two parallel lists | two `Task`s — e.g. `ListGroupsResult` (`Valid()` / `Errors()`) |
| 5 | several independent futures over one payload | one result handle | one `*Result` exposing several `Task`s over one marshalled object — e.g. `DescribeClusterResult` (`Nodes()`/`Controller()`/`ClusterId()`/`AuthorizedOperations()`) |
| 6 | every public future is `KafkaFuture<Void>` | **NO result handle at all** | a `*Result` whose only member is `All()` — e.g. `AbortTransactionResult`, `ForceTerminateTransactionResult` |

Shape 2's ABI reason, verbatim from the header: *"There is no `_get_value`:
Java's per-key future is `KafkaFuture<Void>`, so a null error **is** the success
value."* Shape 6 means those RPCs' async callbacks carry only an error — success
is a null error, and there is nothing to destroy.

**There are 44 `*Result_t` typedefs and exactly 44 `*Result_destroy` functions**
— one-to-one, no gaps. (`kafka_admin_ListOffsetsResultInfo_t` is *not* a result
handle despite the name; it is a borrowed child value type and correctly has no
destroy.)

### 4.5 Threading — Admin is the CONSUMER's family, but LESS ordered

The header documents the callback's thread precisely, and there are **three**
cases — one more than a first read suggests:

1. **The handle's dispatcher thread** — the normal path. Each `AdminClient` owns
   one (`"kafka-admin-callback-dispatcher"`, `admin.rs:255`), created implicitly
   by `_new` and torn down by `_destroy`. **There is no
   `kafka_admin_AdminClient_poll`** — this is a push model; the caller never
   drives it.
2. **Synchronously on the calling thread, before the entry point returns.**
   For `create_topics_async` and `close_async` the header documents exactly one
   trigger: *"when the RPC cannot be submitted at all (a NULL `admin` handle)."*

   ⚠ **But the family-wide trigger set is larger, and the extra triggers are
   ordinary bad input** — an unparseable base64 topic id, an unknown
   `AlterConfigOp` op-type, an unknown isolation level. The source of that claim
   is the FFI module doc, **not** the generated header:
   `src/ffi/admin.rs:62` — *"…is plain bad input, not only a programming error,
   so a caller must not assume the entry point has returned by the time the
   callback runs."* cbindgen does not emit `//!` module docs, so this sentence
   **does not appear in `confluent_kafka.h`**.

   ⚠⚠ **Correction applied 2026-09-08 (Critic 66, Observation).** An earlier
   draft attributed that sentence to the header. It is not there. The engineering
   conclusion is unchanged — the header independently states callbacks *"are not
   guaranteed to be serialised on one thread"* and *"do not hold a lock across
   this call and re-acquire it in the callback"* — but the **citation** was
   wrong, and a Critic whose ground truth is the header (§8.2) cannot verify a
   header claim that isn't in the header. **Cite `src/ffi/admin.rs:62` for the
   bad-input triggers and the header for everything else.** When a later phase
   binds an entry point that parses a topic id or an enum code, re-read that
   entry point's own header doc rather than assuming the family-wide wording.
3. **A tokio worker thread**, if the dispatcher's completion queue is
   unreachable — i.e. a panic in an earlier callback killed the dispatcher.

> *"So callbacks are not guaranteed to be serialised on one thread. Do not hold a
> lock across this call and re-acquire it in the callback, and publish everything
> the callback needs (including `user_data`) before calling rather than after."*

Consequences, all mandatory:

- **`RunContinuationsAsynchronously` on every TCS** (ffi §B7). Case 2 makes this
  sharper than for the consumer: without it, an awaiter's continuation runs
  **synchronously inside your own P/Invoke call**, on ordinary bad input.
- **Total no-throw callback body** — a managed exception unwinding into Rust is
  UB, and there is no caller frame to catch it (ffi §B6).
- **No managed lock spanning submit-and-callback**, and **publish `user_data`
  before the call** — the header says both, and case 2 makes the deadlock real.
- **Do not assume serialisation.** Unlike the consumer's single dispatcher,
  Admin explicitly disclaims it. Keep per-op state in the `GCHandle`, not in
  shared fields.

**`user_data` free site — settled by ffi §B6's decisive question**
(*"does this entry point take a `user_data_destroy`?"*): **no admin entry point
does** — a header-wide grep returns **0**, and no `user_data_destroy` typedef
exists for admin at all (the consumer has one, for its multi-shot listener). So
Admin is the **hookless one-shot per-operation** family (ffi §B6 first Rule): the
**callback is the sole owner** of the `GCHandle` free, on every path including
both inline cases. The only other free site is `AbandonBeforeSubmit`, reachable
only when the submitting P/Invoke threw so native never ran.

**⚠ `AdminClient_destroy` has NO refcount and NO drain — the binding must supply
the safety.** `admin.rs:550-568` shuts down the runtime, drops the client, drops
`completion_tx`, and **detaches** the dispatcher join handle (*"do NOT join —
outstanding completion jobs may still hold a cloned `completion_tx`"*). The
header states: *"Destroying concurrently with an in-flight `_async` operation is
a C lifetime precondition the caller must uphold."*

This is **structurally the same hazard as the consumer UAF** already fixed with
span-the-op `AddRef`, except the consumer ABI ref-counts internally and Admin's
does not. So the span-the-op `DangerousAddRef` on `SafeAdminHandle` (held
submit → callback, released in `FreeGcHandle`) is not a nicety — it is **the**
mechanism that makes `Dispose` racing an in-flight op safe rather than a
use-after-free. **Highest-risk item in the milestone; P1 must land it with the
differential test (§9).**

---

## 5 · THE SYNC-VS-ASYNC SHAPE — resolved

### 5.1 The decision

**Every one of the 46 RPC methods is a plain synchronous C# method returning a
`*Result` object that holds one `Task<T>` per key. Only `Close` returns a
`Task`.** There is no `IAsyncAdmin`, and no method carries an `Async` suffix.

```csharp
namespace Confluent.Kafka.Admin;

public interface IAdmin : IDisposable, IAsyncDisposable
{
    CreateTopicsResult   CreateTopics  (IEnumerable<NewTopic> newTopics, CreateTopicsOptions?   options = null);
    DeleteTopicsResult   DeleteTopics  (TopicCollection topics,          DeleteTopicsOptions?   options = null);
    ListTopicsResult     ListTopics    (                                 ListTopicsOptions?     options = null);
    DescribeTopicsResult DescribeTopics(TopicCollection topics,          DescribeTopicsOptions? options = null);
    // … all 46, same shape: sync fn → *Result holding Task<T> per key …

    Task Close(TimeSpan timeout);          // Java's close(Duration) BLOCKS → Task (§4 Disposal row)
}

public sealed class CreateTopicsResult                 // Java CreateTopicsResult
{
    public IReadOnlyDictionary<string, Task> Values { get; }  // Java values() — Map<String, KafkaFuture<Void>>
    public Task<Config>  Config           (string topic);   // Java config(String)      — thenApply
    public Task<Uuid>    TopicId          (string topic);   // Java topicId(String)     — thenApply
    public Task<int>     NumPartitions    (string topic);   // Java numPartitions       — thenApply
    public Task<int>     ReplicationFactor(string topic);   // Java replicationFactor   — thenApply
    public Task          All              ();               // Java all()               — allOf
}
```

⚠ **Two corrections applied 2026-09-08 after Critic 66 findings 1 and 2 — this
sketch was wrong in its first draft, and P2…P8 would have copied it.** Both were
caught only by checking the Java source directly, which is why the P1 checkpoint
exists:

  - **`Values` is `Task`, not `Task<TopicMetadataAndConfig>`.**
    `CreateTopicsResult.java:33` holds a **private**
    `Map<String, KafkaFuture<TopicMetadataAndConfig>>`, but `:43-48` publishes
    `Map<String, KafkaFuture<Void>> values()`, deliberately erasing the metadata
    via `thenApply(v -> null)`. The metadata is reachable **only** through the
    four typed accessors. Publishing the private map would widen Java's public
    surface — the opposite of restoring it.
  - **`ReplicationFactor` is `int`, not `short`.** Java's *result* side is
    `KafkaFuture<Integer> replicationFactor(String)` (`:104`), `int
    replicationFactor()` (`:141`), ctor `(Uuid, int, int, Config)` (`:115`); the
    ABI agrees (`int32_t`). `short` is the **request** side
    (`NewTopic.replicationFactor()`) and does not transfer to the result.

**General rule this yields for P2…P8:** a Java `*Result`'s **public accessor
signature** is the contract, not the private field it is derived from. Read the
accessor, not the field.

### 5.2 Justification

`bindings/dotnet/CLAUDE.md §4`'s governing rule says: decide **per method from
the Java implementation**, and names three triggers that force async — *blocks*,
*returns `Future<T>`*, *takes a completion callback*. Admin is the first client
where those triggers must be read carefully rather than mechanically.

`Admin.createTopics()` **does not block**: it hands work to the background thread
and returns instantly with a result object (`admin-client.md` §1, derived from
`KafkaAdminClient` itself, not the Javadoc). Trigger 1 does not fire. Nor does
trigger 3 — there is no callback parameter. Trigger 2 — *returns `Future<T>`* —
is the interesting one: `createTopics` does **not** return a `KafkaFuture`; it
returns a `CreateTopicsResult` that *contains* `KafkaFuture`s, one per key. The
`Task` mapping therefore belongs on the **futures inside the result**, not on the
method.

Mapping it to `async Task<CreateTopicsResult> CreateTopics(...)` would (a) invent
blocking Java does not have, (b) collapse N per-key futures into one aggregate —
exactly what `admin-client.md` §5 forbids (*"Do NOT flatten per-key futures into
one aggregate future on the public surface"*), and (c) make it impossible to
await one topic without awaiting all of them.

This is also why **D2 ruled one interface**. The producer/consumer pairs exist
because their Java methods block, leaving a genuine choice. Admin's do not, so a
second interface would be a synonym.

**Which ABI entry point does a sync C# method call?** The **`_async`** one,
always. The sync ABI twin *"blocks until every per-topic future has resolved"* —
calling it from a sync C# method would make `CreateTopics` block, the very
contract violation above; wrapping it in `Task.Run` would be sync-over-async,
forbidden by ffi §B7. Using `_async` avoids both. **The 46 sync ABI entry points
are simply unused by this binding** — exactly as Python leaves them for C
consumers.

⚠ **Note the inversion, because it reads backwards at first glance:** the *sync*
C# methods drive the *`_async`* ABI entry points. Not a contradiction — "sync"
describes the C# method's own return behaviour (it returns without waiting), and
the `_async` ABI is what *makes* that possible.

**`Close`** is the one exception, exactly as `admin-client.md` §1 says: Java's
`close(Duration)` joins the background thread, so it blocks, so trigger 1 fires →
`Task Close(TimeSpan)`, driven by `kafka_admin_AdminClient_close_async`. Name
mirrors Java, no `Async` suffix. Follow Python's `_close_ms` convention: a null
timeout maps to `-1`, which the ABI reads as Java's no-arg `close()`.

### 5.3 ⚠ This is a DELIBERATE divergence from the Python sibling

Python did **not** restore this shape. `bindings/python/admin.py`'s
`create_topics` **blocks** and returns a plain
`dict[str, TopicMetadataAndConfig | KafkaError]`. It creates **no** futures at
all — `concurrent.futures` is never imported — and it ships **no `*Result`
classes** (the only `class *Result*` in 4 062 lines is the Java *value* type
`ListOffsetsResultInfo`). Because its methods block, Python then needed the
`AdminClient` / `AsyncAdminClient` split to serve asyncio users.

**.NET does not copy that — ruled 2026-09-08 (D2).** Two reasons, both from the
rulebook:

- `bindings/CLAUDE.md §2.1` — target the **Java** shape. Java's `createTopics`
  does not block and does not return a dict.
- `bindings/CLAUDE.md §1.2` — *"Reconstruction effort scales with distance from
  C: … rich-OO languages do substantial rebuilding."* C# has `Task<T>`,
  `TaskCompletionSource<T>` and cheap value types; it can restore per-key futures
  faithfully where Python's chosen idiom did not.

The result: **.NET's Admin surface is strictly more Java-faithful than Python's**,
and needs one interface where Python needed two.

⚠ **This divergence is endorsed, not merely tolerated.** The maintainer worked
through the Python `Admin`/`AsyncAdmin` split against this single-`IAdmin` design
in detail before ruling D2. So the two bindings are *knowingly* asymmetric here,
and a future reviewer must not "converge" them by giving .NET a blocking API or an
`IAsyncAdmin`, nor read the asymmetry as one binding having drifted. Record the
divergence in each phase's close-out so the finding surfaces with its rationale
attached.

What .NET **does** borrow from Python is the **mechanism**, not the shape: the
per-RPC `(submit, resolve, free)` spec triple is a good factoring, and its
handling of a callback arriving after teardown is worth copying.

---

## 6 · File & type layout

```
bindings/dotnet/src/Confluent.Kafka/
├─ Admin/                                    ← NEW public topical folder, namespace Confluent.Kafka.Admin
│  ├─ IAdmin.cs · KafkaAdminClient.cs · MockAdminClient.cs
│  ├─ NewTopic.cs · NewPartitions.cs · TopicListing.cs · TopicDescription.cs
│  ├─ Config.cs · ConfigEntry.cs · ConfigResource.cs · AlterConfigOp.cs
│  ├─ TopicMetadataAndConfig.cs · TopicPartitionInfo.cs
│  ├─ ConsumerGroupDescription.cs · MemberDescription.cs · MemberAssignment.cs · …
│  ├─ AclBinding.cs · AccessControlEntry.cs · ResourcePattern.cs · …
│  ├─ Options/     ← one file per Java *Options class (~61)
│  └─ Results/     ← one file per Java *Result class  (~44 + the no-handle ones)
├─ TopicCollection.cs · Uuid.cs              ← Java puts these in `common` → root namespace
└─ Internal/
   ├─ AdminOperation.cs                      ← per-op state: per-key TCS map + GCHandle + AddRef
   └─ Interop/
      ├─ NativeMethods.Admin.cs              ← partial class; keeps the 218-decl file navigable
      ├─ SafeAdminHandle.cs                  ← Category 1 (ffi §B2); ReleaseHandle → AdminClient_destroy
      ├─ SafeAdminPropertiesHandle.cs        ← Category 1, short-lived
      ├─ AdminCallbacks.cs                   ← the static readonly Cdecl delegates (47)
      ├─ KeyedResultMarshal.cs               ← the shared walker for all SIX result shapes (§4.4)
      └─ <PerType>Marshal.cs                 ← TopicDescriptionMarshal, ConfigMarshal, …
```

Rationale: `bindings/dotnet/CLAUDE.md §2` — public API at or below the project
root, topical folders once a family grows, `Internal/` the only non-public
folder, `unsafe` only under `Internal/Interop/`. `Admin/` is explicitly
sanctioned there (*"a public `Admin/` folder is expected and correct"*), citing
ckd's 107-public-type `Admin/` as precedent. `NativeMethods` stays **one class**
(CA1060) split across `partial` files.

**`SafeHandle` mapping** (ffi §B2 categories):

| Handle | Category | Wrapped? |
|---|---|---|
| `AdminClient_t` (real **and** mock — same type) | 1 · client | ✅ `SafeAdminHandle` |
| `AdminClientProperties_t` | 1 · config, short-lived | ✅ `SafeAdminPropertiesHandle` |
| `NewTopic_t`, `NewPartitions_t` | input, caller-retained | destroyed in a `finally` after the submit; not wrapped |
| every `*Result_t` | 3 · owned **borrow-root** | destroyed in the callback's `finally`; not wrapped (read-and-free within one call) |
| the callback's `error` param (non-const) | 2 · flat transient, **owned** | `FromHandle` (destroys) |
| `get_error(i)` (const) | 4 · **borrowed** | `FromBorrowedHandle` — **never** destroyed |
| every `get_value(i)` / `get_key(i)` / child handle / `const char*` | 4 · borrowed view | **never** freed; copied out before the root's `_destroy` |

---

## 7 · Gates that are easy to forget

Mechanical items with a silent failure mode. Check every phase.

1. **`EntryPoint` on every `[DllImport]`** whose C# name drops the
   `kafka_admin_` prefix — otherwise `EntryPointNotFoundException` at **runtime**,
   not compile time (ffi §0.1).
2. **`[MarshalAs(UnmanagedType.I1)]` on every `bool`** — Admin has many
   (`validate_only`, `retry_on_quota_violation`, `include_synonyms`,
   `include_documentation`, `has_assignments`, …). A missing `I1` marshals a
   4-byte Win32 `BOOL` and silently corrupts the next argument.
3. **Negative `timeout_ms` means *unset*** (client default applies), not "zero
   timeout". A C# `TimeSpan?` of `null` must map to a negative, not to `0`.
4. **Java enums cross as `int32_t` id codes** (`ConfigResource.Type.id()`,
   `IsolationLevel.id()`, `AclOperation`, `PatternType`, …). Unrecognised codes
   become `UNKNOWN` at the ABI and are rejected by the broker — so the C# enum's
   numeric values must match Java's, not be auto-assigned.
5. **`NewTopic.set_replicas_assignment` switches constructor semantics** — once
   called, the entry uses Java's replicas-assignment ctor and `num_partitions` /
   `replication_factor` are **not sent**. The C# `NewTopic` must model the
   either/or, not let both be set.
6. **`NewPartitions.has_assignments` is an explicit discriminant, not inferred
   from count.** `increaseTo(n, emptyList())` is legal Java and is a *different*
   wire request from `increaseTo(n)`; the broker rejects present-but-empty with
   `INVALID_REPLICA_ASSIGNMENT`. A C# `IReadOnlyList<...>?` must distinguish
   null from empty.
7. **`MockAdminClient` seeding semantics differ per method** — the three offset
   setters **merge**; `set_feature_levels` **replaces**. Mirrors Java.
8. **The mock seeding functions return an error if the handle is not a mock** —
   check the returned `KafkaError*`, do not ignore it.
9. **`MockAdminClient_new` returns NULL** if `num_brokers < 1` (Java's `build()`
   throw, expressed in the FFI idiom). Map to an exception, do not deref.
10. **Destroy the input `NewTopic_t[]` after the submit** — the ABI copies out
    and *"the caller retains ownership"*. Forgetting leaks per call.
11. **`dotnet format` and the xmldoc gate** — `TreatWarningsAsErrors` + the
    emitted `Confluent.Kafka.xml` catch broken `<see cref>`s; a new public folder
    is where those break first.
12. **Run `cargo xtask format-check` from the REPO ROOT** — it false-fails from
    `bindings/dotnet`.

---

## 8 · Phase breakdown

Nine phases, each independently reviewable by one `dotnet-actor` /
`dotnet-critic` round, each a **vertical slice** (P/Invoke → marshaller →
public type → tests) that leaves the binding green.

| Phase | N | Name | Content |
|---|---|---|---|
| **M15/P1** | 66 | **Foundation + the per-key bridge, proven on one RPC** | `SafeAdminHandle` (+ the span-the-op AddRef, §4.5), `SafeAdminPropertiesHandle`, `NativeMethods.Admin.cs` core block, `KafkaAdminClient` / `MockAdminClient` construction, `Close`/`Dispose`/`DisposeAsync`, `IAdmin` skeleton, `AdminOperation`, `KeyedResultMarshal` (shapes 1 **and** 2), **`KafkaException.FromBorrowedHandle`**, and **`CreateTopics` end-to-end** (`NewTopic`, `CreateTopicsOptions`, `CreateTopicsResult`, `TopicMetadataAndConfig`, `Config`, `ConfigEntry`). **The bridge is the deliverable; `CreateTopics` is its proof.** |
| **M15/P2** | 67 | **Topics & partitions** | `DeleteTopics` (shape 2 + `TopicCollection`), `ListTopics` (shape 3), `DescribeTopics`, `CreatePartitions`, `DeleteRecords`. Types: `TopicCollection`, `Uuid` (+ base64 id round-trip), `TopicListing`, `TopicDescription`, `TopicPartitionInfo`, `NewPartitions`, `RecordsToDelete`, `DeletedRecords`, `AclOperation`. Completes result shapes 1–3. |
| **M15/P3** | 68 | **Cluster, configs, log dirs** | `DescribeCluster` (**shape 5**), `DescribeConfigs`, `IncrementalAlterConfigs`, `ListConfigResources`, `ListClientMetricsResources`, `DescribeLogDirs`, `AlterReplicaLogDirs`, `DescribeReplicaLogDirs`. Types: `ConfigResource`(+`Type`), `ConfigSynonym`, `AlterConfigOp`(+`OpType`), `TopicPartitionReplica`, `ReplicaInfo`, `LogDirDescription`, `ReplicaLogDirInfo`, `ClientMetricsResourceListing`, `ClusterDescription`. |
| **M15/P4** | 69 | **Elections, reassignments, offsets** | `ElectLeaders`, `AlterPartitionReassignments`, `ListPartitionReassignments`, `ListOffsets`. Types: `ElectionType`, `IsolationLevel`, `NewPartitionReassignment`, `PartitionReassignment`, `OffsetSpec`, `ListOffsetsResultInfo`. Partition-keyed throughout. |
| **M15/P5** | 70 | **Groups & group offsets** | `ListGroups` (**shape 4**), `ListConsumerGroups`, `DescribeConsumerGroups`, `DescribeClassicGroups`, `ListConsumerGroupOffsets`, `AlterConsumerGroupOffsets`, `DeleteConsumerGroupOffsets`, `DeleteConsumerGroups`, `RemoveMembersFromConsumerGroup`. Types: `GroupListing`, `ConsumerGroupListing`, `ConsumerGroupDescription`, `ClassicGroupDescription`, `MemberDescription`, `MemberAssignment`, `ListConsumerGroupOffsetsSpec`, `MemberToRemove`, `GroupState`, `GroupType`. **Largest phase — D6 pre-sanctions a P5a/P5b split if the round runs long; no new ruling needed.** |
| **M15/P6** | 71 | **ACLs & client quotas** | `CreateAcls`, `DescribeAcls`, `DeleteAcls`, `DescribeClientQuotas`, `AlterClientQuotas`. Types: `AclBinding`, `AclBindingFilter`, `AccessControlEntry`(+`Filter`), `ResourcePattern`(+`Filter`), `ResourceType`, `PatternType`, `AclPermissionType`, `DeletedAcl`, `ClientQuotaEntity`, `ClientQuotaFilter`(+`Component`), `ClientQuotaAlteration`(+`Op`). ⚠ Inputs are **8 parallel arrays**, no `AclBinding_t` handle — the C# types destructure at the call site. Both families turn on a **null-vs-absent** distinction. |
| **M15/P7** | 72 | **SCRAM, delegation tokens, features** | `DescribeUserScramCredentials`, `AlterUserScramCredentials`, `CreateDelegationToken`, `RenewDelegationToken`, `ExpireDelegationToken`, `DescribeDelegationToken`, `DescribeFeatures`, `UpdateFeatures`. Types: `ScramMechanism`, `ScramCredentialInfo`, `UserScramCredentialUpsertion`/`Deletion`, `UserScramCredentialsDescription`, `KafkaPrincipal`, `TokenInformation`, `DelegationToken`, `UpgradeType`, `FeatureUpdate`, `FinalizedVersionRange`, `SupportedVersionRange`, `FeatureMetadata`. Independent of P6 — no shared types. |
| **M15/P8** | 73 | **Producers & transactions** | `DescribeProducers`, `DescribeTransactions`, `AbortTransaction` (**shape 6**), `ForceTerminateTransaction` (shape 6), `FenceProducers`, `ListTransactions`. Types: `TransactionState`, `ProducerState`, `PartitionProducerState`, `TransactionDescription`, `TransactionListing`, `ProducerIdAndEpoch`, `AbortTransactionSpec`. Richest per-key shapes — deliberately last, once the pattern is settled. |
| **M15/P9** | 74 | **Doc-sync, DoD sweep, close-out** | `ffi-marshalling.md` **Part C · Admin** (§10); the §0 doc fixes; STATUS.md; the milestone-wide sweep — exhaustive-RPC-coverage **walk** against the Rust `Admin` trait's 46 methods, TFM-matrix smoke, allocation-audit N/A statement, `MockAdminClient` fidelity audit against `src/admin/mock_admin_client.rs`. |

**Why P1 is scoped this way.** The bridge (§4) is the whole risk of the
milestone. Landing it with exactly one RPC means a defect in the shared mechanism
is found once, in a 1-RPC diff, rather than 46 times. Every later phase is then
mechanical repetition of a reviewed pattern — which is what makes P2…P8 safely
large. The six result shapes are deliberately spread so each first appears in a
phase small enough to review: 1–2 in P1, 3 in P2, 5 in P3, 4 in P5, 6 in P8.

---

## 9 · Test plan & Definition of Done

Per `bindings/dotnet/CLAUDE.md §7.4/§7.5` and root
`.claude/rules/definition-of-done.md`. **One DoD, applied per phase.**

### Per-phase gates

```
cargo build --features ffi            # native + header — must produce an IDENTICAL header (Mode-A proof)
dotnet build -c Release --no-incremental   # 0W/0E across all 6 TFM outputs
dotnet test -f net10.0 && dotnet test -f net8.0
dotnet format --verify-no-changes
cargo xtask format-check && cargo xtask lint     # from the REPO ROOT
```

### Test vehicle: `MockAdminClient`, no broker

All unit tests run against `MockAdminClient` (D4), seeded via
`MockAdminClient_new(numBrokers)`, `set_feature_levels`, `timeout_next_request`,
`update_beginning_offsets`, `update_end_offsets`,
`update_consumer_group_offsets`.

### Mandatory tests

**The per-key bridge (P1, re-checked per phase):**
- A multi-key call where **some keys succeed and some fail** — each `Task`
  carries *its own* outcome. **This is the test that discriminates a correct
  implementation from one that faults everything on any failure.**
- `All()` faults when any key fails, succeeds when all succeed.
- Awaiting **one** key's `Task` works without awaiting the others.
- A top-level submit failure (non-null `error`) faults **every** per-key `Task`
  — and leaves none hanging.
- Error **message and code** asserted, not just `is_err` (DoD §3).
- **Shape 2 (void) specifically**: success is a *null error*, and the `Task`
  completes rather than carrying a value.

**Memory safety (the §4.5 items — highest risk):**
- **Differential ref-count test** (the M9/P8 precedent): with no op in flight
  `Dispose` releases the native handle immediately; with one in flight it does
  **not**; completing the op then releases it. A single-case assertion cannot
  tell a working ref-count from a permanently-unbalanced one.
- `Dispose` racing an in-flight `_async` op does not crash — the ABI does **not**
  protect this, so the test proves the *binding's* guarantee.
- Each `*Result_t` root destroyed **exactly once**, including on the throwing
  path.
- A per-key `KafkaError` is **never** destroyed — verify by **injection**
  (destroy it deliberately, confirm the test goes red, revert). A double-free
  aborts the process and no managed assertion catches it.
- `GCHandle` freed exactly once per op, including **both** inline-callback paths
  (§4.5 case 2 — reachable on ordinary bad input, e.g. an unparseable base64
  topic id).
- The **synchronous-callback** case does not deadlock and does not run the
  awaiter's continuation inside the P/Invoke (proves
  `RunContinuationsAsynchronously`).
- Aggressive GC during an in-flight op does not collect the delegate.
- Double-`Dispose` safe; post-`Dispose` call → `ObjectDisposedException`.

**Marshalling:**
- Non-ASCII topic / config value / error message round-trips (guards `LPStr`).
- Every `bool` parameter correct (guards a missing `MarshalAs(I1)`).
- `null` timeout → negative `timeout_ms` (client default), **not** 0.
- `NewPartitions` null-vs-empty assignments produce different requests (§7 gate 6).
- Preconditions (`ArgumentNullException` / `ArgumentOutOfRangeException`) fire
  **before** any P/Invoke — mandatory, the ABI does not validate (ffi §B5).

**Shape fidelity:**
- Each phase's RPC set checked off against `src/admin/mod.rs`'s 46 methods by an
  **exhaustiveness walk**, not a recollection.
- `MockAdminClient` audited against `src/admin/mock_admin_client.rs` per
  `admin-client.md` §9: a method Java's own mock **implements** must be
  implemented, and only methods Java's mock leaves as
  `UnsupportedOperationException` may surface an unsupported error — each such
  site citing the exact Java line. *"No in-scope test exercises it"* is **not** a
  licence to stub.

**Cross-cutting:**
- TFM-matrix smoke: construct a `MockAdminClient`, run one RPC, close — on
  **net462** (via netstandard2.0), **net8.0**, **net10.0**.
- **DoD §10 (hot-path allocation audit): N/A**, stated explicitly per
  `admin-client.md` §10 — Admin calls are batch/administrative with no per-record
  path. **Stated, never silently skipped.**
- **DoD §11 (consumer trait surface check): N/A** to `IAdmin`, but its spirit
  applies — verify the 46 methods stayed plain `fn`, only `Close` returns `Task`,
  and no `async` bled into the marshallers.
- No `TODO`/`FIXME`; Apache-2.0 header on every new file.

---

## 10 · `ffi-marshalling.md` Part C · Admin — what it must cover (P9)

The rulebook is Part 0 (shared) + Part A (producer) + Part B (consumer), *"each
client reads as one complete, self-contained story"*. Admin needs its own Part C
on the same principle:

- **§C1 Thread model** — no .NET pump; the ABI pushes; **three** callback threads
  and the explicit *non*-serialisation guarantee (§4.5). This is where Admin
  differs from the consumer, which *is* serialised.
- **§C2 Handle ownership** — the category table in §6, plus the ⚠
  **destroy-is-not-ref-counted** rule.
- **§C3 Strings** — NUL-terminated callee-owned only; no length-delimited form.
- **§C4 The per-key result bridge** — §4 promoted to a rule, including all six
  result shapes and the named anti-pattern *"reusing `FromHandle` on a per-key
  error"*.
- **§C5 Error model** — top-level `error` = submit failure (**owned**); per-key
  error = that key's outcome (**borrowed**). The two must never be conflated —
  the direct analogue of §B5's two-channel commit-callback rule.
- **§C6 Callbacks** — the hookless one-shot family; `static readonly` Cdecl
  delegates; total no-throw; `GCHandle` freed by the callback (sole owner, since
  there is no `user_data_destroy`); publish-before-call; no lock across submit.
- **§C7 Async completion** — N `TaskCompletionSource`s per op, all with
  `RunContinuationsAsynchronously`; the timing-independence deviation (§4.3).

**Part C is a P9 deliverable**, authored from what P1–P8 actually shipped so it
documents reality rather than intent. But **P1 must not ship without §C2/§C4/§C6
drafted**, since those are the rules P2–P8 copy.

---

## 11 · Risks

| # | Risk | Mitigation |
|---|---|---|
| 1 | **Per-key error double-free.** Reusing `FromHandle` on a *borrowed* per-key error aborts the process; const-ness is the only signal, and the same C type is owned elsewhere. | `FromBorrowedHandle` is a P1 deliverable; named anti-pattern in Part C §C4/§C5; verified **by injection**. |
| 2 | **`Dispose` racing an in-flight op.** The ABI explicitly does **not** protect this (no refcount, no drain). | Span-the-op `DangerousAddRef` in P1 + the differential ref-count test (§9). |
| 3 | **Inline callback on ordinary bad input** (§4.5 case 2) — a missing `RunContinuationsAsynchronously` runs user continuations inside the P/Invoke; a lock spanning submit+callback self-deadlocks. | Both rules are P1 deliverables with a dedicated test; stated in Part C §C1/§C7. |
| 4 | **The six result shapes get collapsed into one** and a later phase invents a non-Java surface. | Shapes enumerated in §4.4; spread deliberately across phases so each first appears in a small diff (§8). |
| 5 | **Volume.** ~44 Result + ~61 Options + ~67 data types + 46 RPCs. | Mechanism proven once in P1; P2–P8 repeat a reviewed pattern. Split any phase that runs long (P5 flagged). |
| 6 | **Silent Mode-B drift** — a phase "fixes" an ABI gap in Rust. | Per-phase `git diff` + header-hash proof (§2). A real gap escalates to the Manager. |
| 7 | **Shape drift toward ckd** (107 public types in its `Admin/` make copying tempting) **or toward Python** (whose Admin shape is deliberately *not* the target — §5.3). | `bindings/CLAUDE.md §2.1` is firm: target Java. D1 and §5.3 settle the two places the pull is strongest, both ruled 2026-09-08. |
| 8 | **Timing-independence deviation goes undocumented** and is later filed as a bug. | Documented on the public surface and in Part C §C7 (§4.3), per `definition-of-done.md` §7. |

---

## 12 · Agent numbers & mechanics

- **N = 66 … 74**, one per phase (§8). Highest used in the binding's sequence is
  **65** (M11/P3.1, closed — all nine findings resolved). **Nothing is in
  flight.** Follows the M9/P5–P9 precedent of one `N` per phase.
- **Personas:** `dotnet-actor` / `dotnet-critic` — **not** the root
  `actor-executor` / `kafka-critic`, which are Rust-translation-shaped and do not
  know P/Invoke (`bindings/dotnet/CLAUDE.md §8.1`). Both persona files are
  registered at the repo root and are **byte-identical** to the binding-local
  sources (verified), so no re-copy is needed unless a persona is edited.
- **Comments:** `bindings/dotnet/COMMENTS.<N>.md` → `COMMENTS.DONE.<N>.md`, both
  **local working files, never committed at the binding root**. The single
  tracked record is the Manager's archived copy at
  `bindings/dotnet/design/history/M15/<Phase>/COMMENTS.DONE.<N>.md`.
- **Review ground truth:** the **C ABI header** + the **Kafka Java public API
  shape** — not Rust internals, not Java implementation logic (§8.2).
- **Branch:** to be named by the maintainer; working assumption is a new branch
  off the current `prashah_dev_dotnet_admin`.

### ⚠ Environment traps — put verbatim in every Actor/Critic brief

This sandbox has a broken shell init that fabricates **false passes**:

1. **`PATH` is clobbered.** `git`, `cargo`, `sed` all appear absent. Every Bash
   call must start with
   `export PATH="/usr/bin:/bin:/usr/local/bin:/opt/homebrew/bin:$HOME/.cargo/bin:$PATH"`.
   `command -v` is **not** reliable here.
2. **`grep` is aliased to `ugrep`** — rejects some patterns and emits nothing,
   reading as a pass. Use `/usr/bin/grep` for anything relied on as evidence.
3. **`sed` may be missing** — use `awk` or `/usr/bin/sed` explicitly.
4. **`cat` is shadowed by a missing `bat` alias** — `cat > file <<'EOF'` silently
   writes a 0-byte file. Use `/bin/cat`.
5. **This is zsh** — unquoted `$var` is not word-split; unquoted globs like
   `--include=*.cs` abort the command with "no matches found". Always quote.
6. **A test filter matching zero tests exits 0.** Always confirm the expected
   **count**, never the exit code.
