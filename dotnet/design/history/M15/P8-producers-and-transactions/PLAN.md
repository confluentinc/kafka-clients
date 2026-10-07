# M15/P8 — Producers & Transactions (.NET Admin binding)

> **Roadmap row:** `design/current/PLAN-M15-admin-client.md` §8 (untracked — never `git add` it).

**Status:** **APPROVED / AUTHORIZED** 2026-09-22. D46 and D47 **RULED** as recommended (§6). Actor N=78 authorized; Critic runs exactly once at the end (§8.1).
**Agent number:** **N = 78** (re-derived from the filesystem: repo-wide `COMMENTS*.md` max is 77; no `COMMENTS.78.*` exists. Matches the ledger.)
**Base:** `f58bf93d` on `prashah_dev_dotnet_binding` (P7 squashed by the maintainer; base of that squash `88ead79b`).
**Mode:** **A** — verified, see §0.2.
**Scope:** the **last six** Admin RPCs. Milestone goes 40/46 → **46/46**.

---

## 0. Verified preconditions

### 0.1 The six RPCs, confirmed absent

`IAdmin.cs` declares **42** methods today. Each of the six below returns **0** grep hits in it, against control-positives `CreateAcls`=3, `DescribeFeatures`=3, `ListGroups`=8.

All twelve C entry points exist (`<rpc>` and `<rpc>_async`) plus six `_callback_t` typedefs. All six are on the Rust `Admin` trait as a defaulted convenience overload + a required `_with_options` form (`src/admin/mod.rs:374-461`); there are no other overloads.

### 0.2 Mode A proof (sound form — **NOT** the gitignored header path)

```
git diff --name-only 88ead79b..HEAD | grep -v '^bindings/dotnet/'   → EMPTY
git diff --name-only 88ead79b..HEAD | grep -c '^bindings/dotnet/'   → 50   (control-positive)
git status --porcelain -- src/ cbindgen.toml generator/             → EMPTY
```

⚠ Do **not** prove Mode A by `git diff` over `target/include/confluent_kafka.h`. `target/` is gitignored (`.gitignore:1`), so that path can never show a diff and the claim is vacuous. The Actor repeats the proof above at phase end, or regenerates with `cargo build --features ffi` and hash-compares.

---

## 1. Shape classification — **the header defines the mechanics, Java defines the shape**

Taxonomy reference: roadmap §4.4 (six shapes + sub-shape 1c).

| # | RPC | Java result class | **Stored field** | **Published surface** | ABI accessor set | Shape |
|---|---|---|---|---|---|---|
| 1 | `describeProducers` | `DescribeProducersResult` | `Map<TopicPartition, KafkaFuture<PartitionProducerState>>` `:30` | `partitionResult(TopicPartition)` `:36` (**throws IAE**), `all()` `:45` | `count`/`get_topic`/`get_partition`/`get_error` + **nested** `get_producer_count(i)` and 6 × `get_*(i,j)` /`destroy` | **1**, key sub-shape **1c** (composite `TopicPartition`), value is a **P6-style nested `(i,j)` list** |
| 2 | `describeTransactions` | `DescribeTransactionsResult` | `Map<CoordinatorKey, KafkaFuture<TransactionDescription>>` `:28` | `description(String)` `:43` (**throws IAE**), `all()` `:62` | `count`/`get_transactional_id`/`get_error` + 6 scalar value fields + **nested** `get_topic_partition_count(i)`, `get_topic_partition_{topic,partition}(i,j)`/`destroy` | **1**, `string` key, **nested `(i,j)` inside the value** |
| 3 | `abortTransaction` | `AbortTransactionResult` | `Map<TopicPartition, KafkaFuture<Void>>` `:28` — **retained, never published** | **`all()` `:41` and nothing else** | **NO result handle.** Callback is `(kafka_common_Error_t*, void*)` — `h:1390` | **6** |
| 4 | `forceTerminateTransaction` | **`TerminateTransactionResult`** | `KafkaFuture<Void>` `:27` — a **scalar** future, not a map | **`result()` `:36` and nothing else** | **NO result handle.** `h:1403` | **6**, ⚠ **but the member is `result()`, not `all()`** — see D46 |
| 5 | `fenceProducers` | `FenceProducersResult` | `Map<CoordinatorKey, KafkaFuture<ProducerIdAndEpoch>>` `:33` | `fencedProducers()` `:43` → `Map<String, KafkaFuture<Void>>`, `producerId(String)` `:53`, `epochId(String)` `:60` (**both throw IAE**), `all()` `:67` | `count`/`get_transactional_id`/`get_error`/`get_producer_id`/`get_epoch_id`/`destroy` | **1 stored, published as 2 + two derived per-key value futures** — see §1.1 |
| 6 | `listTransactions` | `ListTransactionsResult` | `KafkaFuture<Map<Integer, KafkaFutureImpl<Collection<TransactionListing>>>>` `:35` | `all()` `:48`, `byBrokerId()` `:68`, `allByBrokerId()` `:92` — **all three mint fresh futures per call** | `count`/`get_broker_id`/`get_error`/`get_listing_count(i)`/`get_{transactional_id,producer_id,state}(i,j)`/`destroy` | **matches NO §4.4 shape** — see §1.2 |

### 1.1 `fenceProducers` — the THIRD sighting of stored-vs-published, and the sharpest

P4 swept 16 results twice and concluded `createTopics` was the sole case. P5 found a second (`ListConsumerGroupOffsetsResult`'s package-private field). **`FenceProducersResult` is the third, and it differs from both:** the stored value type `ProducerIdAndEpoch` is *never published as itself*, yet the ABI **does** export it (`get_producer_id`, `get_epoch_id`). So the binding must marshal the full value and then publish **four** derived views:

- `fencedProducers()` `:44-47` — builds a **brand-new map every call**, values are `thenApply(p -> null)`, i.e. the payload is **erased**.
- `producerId(id)` `:54` → `findAndApply(id, p -> p.producerId)`; `epochId(id)` `:61` → `p -> p.epoch`.
- `findAndApply` `:71-78` re-mints the `CoordinatorKey` and **throws `IllegalArgumentException`** (`:74-77`) for an id that was not requested — ⚠ **documented nowhere**; the javadoc at `:50-52`/`:57-59` is silent. Trust the code.
- `all()` `:68` — `allOf(...)`, no payload.

`ProducerIdAndEpoch` lives in **`org.apache.kafka.common.utils`**, not `admin`, and exposes **public fields** (`producerId: long` `:24`, `epoch: short` `:25`) with **no accessors** — which is why Java writes `p -> p.producerId` rather than a method ref. Note the width: `KafkaFuture<Long>` and `KafkaFuture<Short>` → `Task<long>` / `Task<short>`.

### 1.2 `listTransactions` — one ABI table, **three different published shapes**

The ABI gives a single table keyed by broker, with a **per-broker error** and a nested listing list. The header states the mapping to Java's three views itself (`h:10208-10213`):

> *"A per-broker error is what Java's `byBrokerId()` view keeps and its `all()` / `allByBrokerId()` views discard, so a listing that succeeded on one broker and failed on another reports both here."*

So from one marshalled snapshot the `*Result` publishes:

| Java accessor | Java type | shape | derivation |
|---|---|---|---|
| `byBrokerId()` `:68` | `KafkaFuture<Map<Integer, KafkaFuture<Collection<TransactionListing>>>>` | **1** (per-broker task faults with that broker's error) | direct from the table |
| `allByBrokerId()` `:92` | `KafkaFuture<Map<Integer, Collection<TransactionListing>>>` | **3** | **first** broker error faults the whole task (`:106-107`) |
| `all()` `:48` | `KafkaFuture<Collection<TransactionListing>>` | aggregate | `allByBrokerId().thenApply(flatten)` `:49-55`, inherits first-error |

⚠ `allByBrokerId` `:107` guards with `!allFuture.isDone()`, so listings arriving **after** the first failure are silently dropped. Reproduce the semantics (first error wins), not the race.

Listings are **sorted by transactional id then producer id** at the ABI (`h:10236-10238`) although Java's value is an unordered `Collection` — so a .NET order assertion is stable, but must not be presented as a Java guarantee.

### 1.3 Structural verdict — **ONE phase, ZERO walker callables, ZERO walker edits**

The sub-phase bar (P2's D7, restated every phase since) is *"does it edit the reviewed foundation?"* — not *"is the mechanism new?"*

`KeyedResultMarshal.cs` is at **`src/Confluent.Kafka/Internal/Interop/KeyedResultMarshal.cs`**, **598 lines, 5 callables** at `:298` `Complete<TKey,TValue>`, `:370` `Complete<TKey>`, `:426` `CompleteAggregate`, `:498` `CompleteList`, `:573` `CompleteTwoLists`. ⚠ **Re-verify, never copy** — this plan is the *third* to find both the path and the line numbers moved since the prior phase's plan.

Routing, all with existing machinery:

| RPC | route | foundation change |
|---|---|---|
| `describeProducers` | `Complete<TopicPartition, PartitionProducerState>`; key via the **existing** `AdminCallbacks.TopicPartitionKey(getTopic, getPartition)` (`AdminCallbacks.cs:522`, already used by `DeleteRecords` `:690` and `ElectLeaders` `:1456`); nested `(i,j)` producer walk lives **inside the `readValue` reader** | none |
| `describeTransactions` | `Complete<string, TransactionDescription>`; nested `(i,j)` partition walk inside `readValue` | none |
| `abortTransaction` | **bypasses KRM** — no result handle; a single void completion (P7 precedent: four RPCs called `SingleAdminOperation<T>.SetResult` directly) | none |
| `forceTerminateTransaction` | same | none |
| `fenceProducers` | `Complete<string, ProducerIdAndEpoch>`; the four views are built in the `*Result` class from the completed per-key tasks | none |
| `listTransactions` | `Complete<int, IReadOnlyCollection<TransactionListing>>` (`TKey = int`, reader `(r,i) => …GetBrokerId(r,i)`); the other two views derive in the `*Result` class | none |

⚠ **Escalation condition, flagged up front (P6 §1.2 precedent):** if the Actor finds that `Complete<TKey,TValue>`'s signature **cannot** express one of these — most plausibly `listTransactions`' `int` key or the nested value readers — **that IS a foundation edit. Escalate; do not absorb it.** Adding a callable is not automatically an escalation; *editing an existing one* is.

⚠ **Do NOT add a `CompleteSingle`/`CompleteVoid` callable "for symmetry"** with the two shape-6 RPCs. P7 rejected exactly this: the body would be the call it wraps, and the walker's value is that each callable names a distinct result **arity**.

### 1.4 Self-consistency check (the P7 plan-defect guard)

P7's plan classified `describeUserScramCredentials` as shape 1 while its own §4 published surface had **no per-key `Task`** — an internal contradiction that cost a round. **Cross-check applied to every row above: §1's shape and §3's published surface agree.** Where they could disagree (`fenceProducers` stored 1 / published 2, `listTransactions` three views), §1 says so explicitly and **§3's surface — derived from Java's public accessors — wins.**

---

## 2. Net-new C# types (12)

| Type | Namespace | Java source | Notes |
|---|---|---|---|
| `TransactionState` | root | `admin.TransactionState` | enum, **8** constants — see §4.1 |
| `ProducerState` | `Admin` | `admin.ProducerState` | 6 fields, **2 optional** |
| `DescribeProducersResult.PartitionProducerState` | **nested** | `DescribeProducersResult:61` | nested in Java → nested here (D17 precedent) |
| `TransactionDescription` | `Admin` | `admin.TransactionDescription` | 7 fields, **1 optional**, holds `Set<TopicPartition>` |
| `TransactionListing` | `Admin` | `admin.TransactionListing` | 3 fields |
| `AbortTransactionSpec` | `Admin` | `admin.AbortTransactionSpec` | 4 fields; `producerEpoch` is **`short`** |
| `DescribeProducersResult` / `DescribeTransactionsResult` / `AbortTransactionResult` / `TerminateTransactionResult` / `FenceProducersResult` / `ListTransactionsResult` | `Admin` | — | see §3 |
| 6 × `*Options` | `Admin` | — | see §4.2 |

**`ProducerIdAndEpoch` is NOT created.** It lives in `org.apache.kafka.common.utils` and is *never published* by `FenceProducersResult` — every accessor erases or projects it (§1.1). Creating a C# type for it would add a struct that no public signature mentions (DoD §7). It exists only as an internal marshalling carrier. ⚠ The roadmap row lists it as a type to create; that row is **wrong** — the same class of error as P3's `ClusterDescription` and P6's `DeletedAcl`. Corrected here; the roadmap is fixed in P9 doc-sync, **never mid-phase**.

⚠ The roadmap row also omits **`TerminateTransactionResult`** entirely and names no result type for `forceTerminateTransaction`. Corrected here.

### 2.1 Field widths — three inconsistencies in Java, mirror them exactly

| Field | `ProducerState` | `TransactionDescription` | `AbortTransactionSpec` | ABI |
|---|---|---|---|---|
| producer epoch | `int` `:51` | `int` `:64` | **`short`** `:49` | `int32_t` everywhere; the abort input **rejects** an out-of-16-bit value rather than truncating (`src/ffi/admin.rs:17868-17870`, message `"producer epoch {n} does not fit in a 16-bit epoch"`) |

Do not "harmonize" these. `ProducerState.producerEpoch()` is `int`, `AbortTransactionSpec.producerEpoch()` is `short`.

`TransactionDescription.transactionTimeoutMs()` is **`long`** `:68` though the wire field is an int. `TransactionListing`'s accessor is **`state()`** while its field is `transactionState` `:44` — mirror the **accessor** name.

### 2.2 The three optional fields cross as `bool`-returning out-param accessors

P7's `finalized_features_epoch` pattern. Verified in the header:

| Java | type | ABI |
|---|---|---|
| `ProducerState.coordinatorEpoch()` | `OptionalInt` `:67` | `bool …_get_coordinator_epoch(result, i, j, out)` |
| `ProducerState.currentTransactionStartOffset()` | `OptionalLong` `:63` | `bool …_get_current_transaction_start_offset(result, i, j, out)` — *"returns false when Java's `OptionalLong` is empty (no transaction in progress)"* |
| `TransactionDescription.transactionStartTimeMs()` | `OptionalLong` `:72` | `bool …_get_transaction_start_time_ms(result, i, out)` `h:10058` |

C# model: `int?` / `long?`. ⚠ **`false` must leave the out-param untouched and produce `null`, not `0` or `-1`.** These are the three most likely silent defects in the phase: the scalar siblings (`producer_id`, `producer_epoch`, `last_sequence`, `last_timestamp`) all return **`-1`** on out-of-range, so a reader that treats `-1` as "absent" for the optionals *and* as a value for the scalars will look consistent and be wrong.

⚠ C `bool` is **1 byte** — `[MarshalAs(UnmanagedType.I1)]`, precedent `NativeMethods.Admin.cs:26-27, 196-197`.

---

## 3. Published C# surface

Precedent settled by reading the shipped tree, not by rule-recall: a **non-async** member returning a materialized dictionary is a **property** (`AlterConfigsResult.Values => _values` `:83`, `AlterPartitionReassignmentsResult.Values` `:84`, `AlterUserScramCredentialsResult.Values` `:53`); anything that must `await` is necessarily a **method** (`ListGroupsResult.All()` `:113` / `.Valid()` `:141` / `.Errors()` `:156`; `AlterConsumerGroupOffsetsResult.PartitionResult(…)` `:92`). D18 ("an unforced getter is a property") is therefore satisfied by the async/non-async split, not overridden by it.

```
DescribeProducersResult
    Task<PartitionProducerState> PartitionResult(TopicPartition partition)   // throws ArgumentException for an unrequested partition
    Task<IReadOnlyDictionary<TopicPartition, PartitionProducerState>> All()
    sealed class PartitionProducerState { IReadOnlyList<ProducerState> ActiveProducers { get; } }

DescribeTransactionsResult
    Task<TransactionDescription> Description(string transactionalId)          // throws ArgumentException
    Task<IReadOnlyDictionary<string, TransactionDescription>> All()

AbortTransactionResult
    Task All()                                                               // sole member

TerminateTransactionResult
    Task Result()                                                            // sole member — see D46

FenceProducersResult
    IReadOnlyDictionary<string, Task> FencedProducers { get; }               // payload erased, mirroring thenApply(p -> null)
    Task<long>  ProducerId(string transactionalId)                           // throws ArgumentException
    Task<short> EpochId(string transactionalId)                              // throws ArgumentException
    Task All()

ListTransactionsResult
    Task<IReadOnlyCollection<TransactionListing>> All()
    Task<IReadOnlyDictionary<int, Task<IReadOnlyCollection<TransactionListing>>>> ByBrokerId()
    Task<IReadOnlyDictionary<int, IReadOnlyCollection<TransactionListing>>> AllByBrokerId()
```

**Exception mapping.** Java throws `IllegalArgumentException` from `partitionResult` `:38-41`, `description` `:46-49` and `findAndApply` `:74-77`, each with an exact message. The binding's established mapping is `ArgumentException` (P5 precedent for the minting results). **Assert the exact message text** (DoD §3) — reproduce Java's wording, e.g. ``"TransactionalId `x` was not included in the request"``.

⚠ **Java's aggregate accessors wrap differently and inconsistently:** `DescribeProducersResult.all()` wraps in **`KafkaException`** `:54`, `DescribeTransactionsResult.all()` wraps in a plain **`RuntimeException`** `:71`. Do not unify. Record the divergence at the site if the binding cannot express one of them.

---

## 4. Input marshalling

### 4.1 `TransactionState` crosses as a **`toString()` display string**, and it is a writer as well as a reader

`admin/TransactionState.java:25-32` — exactly 8 constants. The string that crosses the ABI is the **ctor argument returned by `toString()`** (`:43-46`), **not** `Enum.name()`:

| constant | wire string |
|---|---|
| `ONGOING` | `Ongoing` |
| `PREPARE_ABORT` | `PrepareAbort` |
| `PREPARE_COMMIT` | `PrepareCommit` |
| `COMPLETE_ABORT` | `CompleteAbort` |
| `COMPLETE_COMMIT` | `CompleteCommit` |
| `EMPTY` | `Empty` |
| `PREPARE_EPOCH_FENCE` | `PrepareEpochFence` |
| `UNKNOWN` | `Unknown` |

⚠ **A C# enum's default `ToString()` yields `PREPARE_ABORT`, which is the WRONG wire string.** Both directions need the explicit table — P5's `GroupState` precedent. There is **no public `name()` accessor** in Java; the display string is reachable only through `toString()`.

Reader contract, from `TransactionState.parse` `:48-49`: `NAME_TO_ENUM.getOrDefault(name, UNKNOWN)` — **never throws, never returns null, case-sensitive**. The header agrees (`h:10643-10645`): an unrecognised name *"becomes `UNKNOWN` rather than a marshaling error"*, and `"ongoing"` silently becomes `UNKNOWN`.

⚠ **Non-finding, recorded so nobody "fixes" it:** the broker-side `coordinator.transaction.TransactionState` has a **9th** constant `DEAD` (`:78`) with no client-side counterpart. `parse("Dead")` → `UNKNOWN`. **Do not add a `Dead` member** — the client enum is the binding's contract.

### 4.2 Options — defaults where the neutral value is not the C# default

| Options class | own fields | default | hazard |
|---|---|---|---|
| `DescribeProducersOptions` | `OptionalInt brokerId` `:27` | `empty()` = "query the partition leader" | ABI `has_broker_id: bool` + `broker_id: i32` (`src/ffi/admin.rs:19180-19181`). Model as `int?`. Java has **no un-set path** (no `brokerId(OptionalInt)` overload) — C# `int?` is strictly more capable; that is fine, do not remove the setter. `equals`/`hashCode` `:39`/`:48` **include** `timeoutMs`. |
| `DescribeTransactionsOptions` | none | — | timeout only |
| `AbortTransactionOptions` | none | — | timeout only; **no class javadoc at all** |
| `TerminateTransactionOptions` | none | — | timeout only. ⚠ **`ForceTerminateTransactionOptions` does not exist** — repo-wide grep 0 hits vs control `TerminateTransactionOptions` 12 |
| `FenceProducersOptions` | none | — | timeout only |
| `ListTransactionsOptions` | 4 filters | see below | the phase's landmine cluster |

`AbstractOptions.timeoutMs` is a **boxed `Integer`, default `null`** = "use the AdminClient default" (`:27`, javadoc `:30-31`). ⚠ The ABI takes `int32_t timeout_ms` where **negative** means the client default (`h:19167` and five siblings). **A C# `int` defaulting to `0` means "0 ms", not "default".** Model as `int?` and pass `-1` when null — `KafkaAdminClient.java:4865-4867` shows Java itself forwards `timeoutMs` only if non-null.

`ListTransactionsOptions` — **four filters, three different neutral encodings**:

| filter | Java default | ABI | C# |
|---|---|---|---|
| `filteredStates` `:30` | `emptySet()` | `states`/`state_count`; *"An empty or NULL array means every state"* (`h:10643-10646`) | ⚠ **empty and null are genuinely identical here and both mean ALL.** A collection defaulting to empty is **SAFE**. Stated explicitly to prevent over-correcting from P5's `ListConsumerGroupOffsetsSpec` landmine, where empty≠null. There is no way to express "match zero states". |
| `filteredProducerIds` `:31` | `emptySet()` | `producer_ids`/`producer_id_count`; same conflation (`h:10647-10648`) | same — safe |
| `filteredDuration` `:33` | **`-1L`** | `duration_ms: i64`, *"Negative means no duration filter"* (`h:10649-10651`) | ⚠ **a C# `long` defaulting to `0` is a real "longer than 0 ms" filter, not neutral. Default to `-1`.** |
| `filteredTransactionalIdPattern` `:34` | **`null`** (no initializer) | `transactional_id_pattern: *const c_char`; ⚠ *"NULL and `\"\"` are distinct: an empty pattern is a legal value the broker evaluates"* | ⚠ **`string?`; a `string.Empty`-for-null normalisation changes the request.** The P6 ACL-filter hazard, same shape. |

⚠ **Javadoc/code disagreement:** `ListTransactionsOptions:119-120` says the pattern filter's "empty means no filter"; the field has no initializer so the default is **`null`**. The consumer handles both (`internals/ListTransactionsHandler.java:78`). **Trust the code: the getter is nullable.** (Third sighting in M15 of a Kafka comment contradicting its code — after P5/D33 and P7/77.4.)

⚠ `filteredStates()` `:93` and `filteredProducerIds()` `:103` return the **live mutable `HashSet`**, not a copy. The C# POCO should expose read-only views; note the deviation.

⚠ `ListTransactionsOptions.equals`/`hashCode` `:138`/`:149` deliberately **exclude** `timeoutMs`, while `DescribeProducersOptions` `:44`/`:49` **includes** it. Mirror each as written; do not unify.

### 4.3 Per-RPC input shapes

| RPC | arrays | scalars | discriminants | rejects |
|---|---|---|---|---|
| `describe_producers` `src/ffi/admin.rs:19176` | **2 parallel** (`topics`, `partitions`), one `count` | `timeout_ms` | **`has_broker_id: bool`** | — (NULL array → empty; NULL entry skipped) |
| `describe_transactions` `:19284` | **1** string array | `timeout_ms` | none | — |
| `abort_transaction` `:19382` | **0** | `topic`, `partition`, `producer_id`, `producer_epoch`, `coordinator_epoch`, `timeout_ms` | none | ⚠ **NULL `topic`** → `"abort transaction topic must not be null"`; ⚠ **`producer_epoch` outside 16 bits** → `"producer epoch {n} does not fit in a 16-bit epoch"` |
| `force_terminate_transaction` `:19488` | **0** | `transactional_id`, `timeout_ms` | none | ⚠ **NULL `transactional_id`** → `"transactional id must not be null"` |
| `fence_producers` `:19587` | **1** string array | `timeout_ms` | none | — |
| `list_transactions` `:19699` | **2 independent, TWO SEPARATE COUNTS** | `duration_ms`, `transactional_id_pattern`, `timeout_ms` | `duration_ms` sentinel; NULL-vs-`""` on the pattern | — |

⚠ **`list_transactions`' two-count transposition hazard is real and the Rust-side protection stops at the ABI.** The Rust code deliberately pairs each array with its own count as a tuple so that swapping them is a *compile error* (`src/ffi/admin.rs:17923-17932`): *"with `producer_id_count > state_count` it would read past the end of a caller-supplied array."* Across the ABI these are **four adjacent loose parameters** (two pointers, two `int`s) and a transposition is an **out-of-bounds native read**, not an exception. **Required: one C# helper per filter that passes `(array, array.Length)` together.** Do not thread four loose arguments through the call site.

⚠ **Two entry points fire the completion callback SYNCHRONOUSLY ON THE CALLING THREAD for bad input**, not just for a NULL handle: `abort_transaction` and `force_terminate_transaction` unwrap their marshalling `Result` *inside* the submit closure (`:19439`, `:19537`), so `admin_async_future_op:767-774` completes inline. This is the §4.5 case-2 path and P6's D38 situation: **validate in C# before any pin**, and ensure the `GCHandle`/TCS are published *before* the P/Invoke, because the delegate may run re-entrantly on the current thread before the call returns.

---

## 5. TESTABILITY — the phase's concentrated risk

### 5.1 Mock coverage is **0 of 6** — worse than any prior M15 phase

All six are hard-stubbed in `src/admin/mock_admin_client.rs`, each returning `Error::unsupported_version("Not implemented yet")`:

| RPC | lines | failure granularity |
|---|---|---|
| `describe_producers_with_options` | `:981-998` (error `:994`) | **per requested partition** |
| `abort_transaction_with_options` | `:1000-1013` (`:1011`) | single future |
| `describe_transactions_with_options` | `:1015-1032` (`:1028`) | **per transactional id** |
| `fence_producers_with_options` | `:1034-1051` (`:1047`) | **per transactional id** |
| `list_transactions_with_options` | `:1053-1061` (`:1059`) | ⚠ **top-level — the WHOLE call fails**, `*out_result` untouched |
| `force_terminate_transaction_with_options` | `:1063-1089` | delegates to `fence_producers`, inherits `:1047` |

These are faithful translations of Java's own `UnsupportedOperationException` (`admin-client.md` §9), **not** Rust gaps. All six **ignore `_options`** (`:984`, `:1003`, `:1018`, `:1037`, `:1053`); `force_terminate` reads `timeout_ms` at `:1078-1080` but forwards it into `fence_producers`, which discards it — unobservable end-to-end.

The mock's `State` (`:114-160`) holds **no producer or transaction state of any kind**, and only **six** `kafka_admin_MockAdminClient_*` C exports exist (`new`, `timeout_next_request`, `set_feature_levels`, `update_beginning_offsets`, `update_end_offsets`, `update_consumer_group_offsets`) — **none seeds transaction state**. `timeout_next_requests` is never read in the range `981-1089`.

**Consequence:** the richest value surface in the milestone — `ProducerState`'s 6 fields (2 optional), `TransactionDescription`'s 7 (1 optional + a nested partition set), `TransactionListing`'s 3, **two nested `(i,j)` walks**, and **three presence discriminants** — has **no mock-reachable happy path at all**. A transposed field, a dropped optional, or an off-by-one in a nested walk **ships green**.

### 5.2 Required test strategy (see D47)

1. **Error-path tests against the real mock** for all six — port the assertions already in `bindings/c/tests/test_mock_admin.c:5858-6046`, which cover exactly this RPC set: row counts, `"Not implemented yet"` messages, sorted-row ordering, `-1` sentinels (`ProducerIdAndEpoch.NONE`), empty-batch-is-empty-success, `list_transactions`' non-null error with `result` left NULL, and both abort-path marshalling rejections.
2. **Injected-accessor tests for every value reader** (P7 precedent), covering all ~20 fields, both nested walks, and each optional in **both** present and absent states.
3. ⚠ **Every injected test needs a reachability control-positive** proving it drives the **production** reader and not a harness double. This is the P3-Stage-3 three-run lesson: a green injection is uninterpretable without a control that goes **RED**. A two-run experiment there would have concluded "both sites safe" when one was merely unreachable.
4. **Submit-seam assertions for every option** on all six RPCs — the mock discards options, so behavioural assertion is impossible *by construction*. `src/ffi/admin.rs:17930-17932` says so: *"the compiler is the only available check."* Every prior admin phase rediscovered this.
5. **Reflection assertions on the public surface.** P1's lesson: a green build plus hundreds of green tests did not catch three public-API shape defects, because C# upcasts and widens silently. Pin `TerminateTransactionResult.Result`'s name, the `int?`/`long?` optional returns, `Task<short>` on `EpochId`, and the property-vs-method split in §3.
6. ⚠ Surface-set assertions must filter by `BindingFlags` + `CompilerGeneratedAttribute`, **never by a name predicate**. That defect occurred twice inside one P3 stage, the second time in a test whose own remark documented the first.

### 5.3 A NON-finding, measured — P7's 77.1 does **not** recur here

77.1 [HIGH] was: `UpdateFeatures({})` reported success where **Java throws** (`KafkaAdminClient.java:4591`), because a shape-1/2 bridge mints zero TCS entries for an empty input so the whole-call error has nowhere to land. My memory flagged this as generalizing to *"most of P8"*.

**Checked, and it does not.** None of the three collection-taking RPCs has an empty guard — `describeProducers` `:4824-4830`, `describeTransactions` `:4833-4839`, `fenceProducers` `:4889-4895` each build a future from the collection and call `invokeDriver` with no validation. Java itself **succeeds** on an empty collection, which is what the ABI implements (`test_mock_admin.c:5902-5908`, `:5972-5977`). **Adding a Java-less precondition would be the defect.**

⚠ The *bridge mechanism* still holds, though: with zero keys, a whole-call error (NULL handle, closed client) has nowhere to land and `All()` completes successfully. The managed `ThrowIfClosed()` precondition established by 77.1's fix (`NativeAdminClient.cs:4251-4260`, after `ThrowIfClosed()` and **before** `timeoutMs`/any pin) is what covers this — keep it, do not extend it into an empty-input throw.

### 5.4 `forceTerminateTransaction` discards producerId/epoch — Java does too, so this is **not** an ABI gap

`KafkaAdminClient.java:4862-4877` implements `forceTerminateTransaction` by **delegating to `fenceProducers`** on a singleton set, then `:4875` takes `fenceResult.fencedProducers().get(transactionalId)` — which has already gone through `thenApply(p -> null)` (`FenceProducersResult.java:46`). **Java discards the payload itself.** The ABI's lack of a result handle matches. Recorded so a reviewer does not read it as a Mode-B gap.

---

## 6. Decisions

**D46 — `TerminateTransactionResult.Result()` vs `.All()`. ✅ RULED 2026-09-22: align with Java — ship `Result()`.** Java's sole public member is **`result()`** (`:36`), not `all()`. Roadmap §4.4 defines shape 6 as *"a `*Result` whose only member is `All()`"* and its §8 row calls `ForceTerminateTransaction` shape 6 — so the roadmap contradicts Java here; correct the roadmap in P9, never mid-phase.
*Tradeoff:* five of the six results answer `.All()` and the sixth does not, which is genuinely surprising to a .NET caller, and `Result` collides conceptually with `Task.Result` (a blocking property). Ruling `.All()` would buy uniformity at the cost of inventing a name Java does not have — which `bindings/CLAUDE.md §2.1` forbids.

**D47 — Test strategy under 0/6 mock coverage. ✅ RULED 2026-09-22: HARDEN — adopt §5.2 in full.** Injected-accessor tests for every value reader, **each with a reachability control-positive**, plus submit-seam option assertions and reflection surface pins. This is mandatory, not advisory.
*Tradeoff:* this is materially more test code than P6/P7 and injected tests can drift into testing the harness (which is precisely why item 3 is non-negotiable). The cheaper alternative — error-path tests only — leaves ~20 value fields, 2 nested walks and 3 presence discriminants completely unguarded, on the richest value surface in the milestone. P5's precedent for a weaker version of this problem (7 of 9 stubbed) was **accepted risk, documented in §6.0**; P8 is 6 of 6, so I recommend hardening rather than repeating.

**D48 — `TransactionState` bidirectional name table** (§4.1). Explicit table both directions; never `Enum.ToString()`/`Enum.Parse`. Reader mirrors `parse`: case-sensitive, unknown → `Unknown`, never throws. No `Dead` member. *Settled by evidence; no ruling needed.*

**D49 — `FencedProducers` is a property, `ProducerId`/`EpochId`/`All` are methods** (§3). Follows the shipped split: non-async materialized dictionary → property (`AlterConfigsResult.Values`); anything awaiting or taking an argument → method. *Settled by in-repo precedent.*

**D50 — no `ProducerIdAndEpoch` C# type** (§2). Never published by any Java accessor; a public type for it would violate DoD §7. Internal marshalling carrier only.

**D51 — mirror Java's field widths and its two inconsistencies exactly** (§2.1): `producerEpoch` is `int` on `ProducerState`/`TransactionDescription` and `short` on `AbortTransactionSpec`; `transactionTimeoutMs` is `long`; `TransactionListing`'s accessor is `State` though its field is `transactionState`. Do not harmonize.

**D52 — reproduce Java's two different aggregate wrapper exceptions** (`KafkaException` vs `RuntimeException`, §3) or record the divergence at the site if the binding cannot express one.

**Carried gaps, unchanged — P8 adds none.** `LogDirDescription.isCordoned()` (P3/D15) and D44 (`DescribeUserScramCredentialsResult`'s three collapsed `RESOURCE_NOT_FOUND` behaviours) remain the milestone's only two tracked Mode-B gaps, both owned by P9. **Mode-B gap list stays at 2.**

---

## 7. Suggested checkpoint order

New mechanisms last, prerequisite closure first (the P5/P6 lesson: **read the last checkpoint's commit before ranking the remaining work**).

| # | Chunk | Why here |
|---|---|---|
| 1 | `TransactionState` + `ProducerState` + `TransactionListing` + `TransactionDescription` + `AbortTransactionSpec` + the 6 `*Options` | pure managed, no ABI; unblocks everything |
| 2 | `forceTerminateTransaction` (`TerminateTransactionResult`) | simplest ABI call in the phase: 2 scalars, no result handle |
| 3 | `abortTransaction` (`AbortTransactionResult`) | scalars + the two input rejections + the sync-fire path |
| 4 | `fenceProducers` | first table walk; smallest accessor set; establishes the minting/throwing views |
| 5 | `describeTransactions` | string key + first nested `(i,j)` walk |
| 6 | `describeProducers` | composite key **and** nested `(i,j)`, 6 value fields, 2 optionals |
| 7 | `listTransactions` + phase DoD sweep | the three-view result and the two-count marshalling hazard, once the seam harness exists |

---

## 8. Coordination — **maintainer constraints, relayed VERBATIM**

These are part of the **approved plan**, not preconditions that expire at approval. An agent proposing an interim review gets this section as the citation.

1. **The Critic runs exactly ONCE, at the very end of the whole phase's implementation.** No interim or mid-phase reviews, no per-checkpoint reviews, no per-RPC reviews.
2. **The Actor MAY use internal checkpoints or resumable substages for its own progress tracking** — this is allowed and encouraged given P5's crash history and P6/P7's successful checkpoint model — **but a checkpoint boundary must never trigger an extra Critic pass.** Checkpoints are resume points, not review gates.
3. **Do not over-invest effort in the N=* agent-numbering ledger.** A quick filesystem check to pick the next free number is sufficient — do not spend cycles reconciling stale roadmap tables, writing corrections, or producing ledger-hygiene prose. Move on quickly.
4. **The maintainer is very short on tokens.** The plan document itself must prioritize code and logic content (Java shapes, ABI accessor sets, marshalling decisions, per-RPC design) over process narrative. Keep coordination/process sections lean — the substance is the per-RPC technical content, not paragraphs about workflow.
5. Relay all of the above, verbatim, to whichever Actor and Critic you eventually spawn for this phase.

Standing constraint carried from P6/P7: **keep XML/inline comments LIGHT** — the plan carries the rationale; code comments are minimal pointers, not restatements. Public members still need enough xmldoc to pass the `TreatWarningsAsErrors` + `Confluent.Kafka.xml` gate: **one concise line plus the Java cite**, no multi-paragraph remarks blocks.

After the Actor reports all six RPCs complete and green, `dotnet-critic` **N=78** runs once; fix cycles after that are unbounded and continue the **existing** Actor session via `SendMessage` rather than a fresh spawn.

Both agents put full status in their **final assistant message** — `SendMessage` to the Manager has not landed reliably in P3–P7.

---

## 9. Method reminders that earned their place

1. **The header defines the mechanics; Java defines the shape.** Accessor-set identity is evidence in neither direction (P4 found identical sets with different shapes; P5 found identical sets with the same shape). Open the Java result class every time.
2. **For a routing decision read the STORED FIELD, not the public accessor** — and then **cross-check that §1's shape matches §3's published surface.** P7 followed the field rule, quoted the right evidence, and still classified against it.
3. **Read the method BODY, not the javadoc.** Three sightings in M15 now (P5/D33, P7/77.4, §4.2 here).
4. **A filtered grep is not a count.** Only a line-numbered listing is; separate declarations from call sites.
5. **Before reporting a zero, run a control-positive.** A zero from a wrong path looks identical to a real zero.
6. **A named injection is a claim to be MEASURED, not repeated** — and an injection *result* needs a `0 Error(s)` build behind it. Neither end survives on assertion.
7. **Re-verify file paths and line numbers; never copy them from a prior plan.** `KeyedResultMarshal.cs` has now moved twice.
8. **Sweep prose added *or falsified* by the diff, mechanically.** Grep for *only / any / every / always / never / exactly / cannot / the one place*.

### Environment traps (relay into every session brief)

`PATH` may be clobbered (a cargo on disk can look absent); `sed`/`cat` may be aliased or missing — prefer the Read tool and `grep -n`; zsh does not word-split unquoted variables; a zero-match libtest run still exits 0. Plus two .NET-specific false-PASS traps: **an ABORTED `dotnet test` exits 0** (only `Test Run Aborted` in the output betrays it), and **`dotnet build -c Release` + `dotnet test --no-build` runs the DEBUG binaries** — three P2a injections read `Passed!` without ever being applied.
