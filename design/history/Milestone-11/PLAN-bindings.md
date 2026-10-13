# Milestone 11 — Admin client C FFI + Python bindings

> Continuation of `Milestone-11/PLAN.md`, whose scope banner deferred all
> bindings work ("Rust core + tests ONLY"). The Rust core is complete (46/46
> in-scope RPCs). This document plans the deferred C/Python layers only.
>
> Read `PLAN.md`'s "C FFI conventions to follow" and "Python bindings
> conventions to follow" sections alongside this — they are still the design
> reference, minus the two stale caveats corrected below.

## 0. Prerequisite: two documents need refreshing

Both describe the tree as it stood before the consumer bindings landed. They
should be updated before implementation starts, so that guidance and code agree.

### 0.1 `.claude/rules/admin-client.md` §11

§11 was written when the consumer bindings were still unmerged. It records that
the branch had no consumer FFI, no `consumer.py` and no async C dispatcher, and
that `src/ffi/producer.rs` was synchronous; on that basis it directs an
implementer to mirror the producer's synchronous patterns and to treat the
plan's async wording as an unresolved design conflict.

PR #116 has since merged, so the current state differs on each point:

| §11 records | Current state (`origin/master` `3e84b9b`) |
|---|---|
| no `src/ffi/consumer.rs` | present, ~3785 lines, ~135 exported fns |
| no `consumer.py` | present, ~754 lines |
| no async C dispatcher | `src/ffi/common.rs`: `CompletionJob`, `spawn_dispatcher` |
| producer synchronous only | producer also has 6 `_async` fns and a dispatcher thread |
| no `_ProducerBase`/`AsyncProducer` split | `producer.py` provides all three |
| no `_run_sync`/`_run_async` | present in both `producer.py` and `consumer.py` |

**Suggested update:** note that the reviewed async dispatcher now exists in
`src/ffi/common.rs` and should be reused, and that the **consumer** binding
shape is the one to mirror — Admin is RPC-oriented rather than per-record, so
the consumer is the closer precedent. The accompanying caution against
introducing an unreviewed async dispatcher can be retired, since a shared one
is now in place.

### 0.2 `PLAN.md`'s bindings caveats

The two `> DEFERRED` notes at the "C FFI conventions" and "Python bindings
conventions" headings state that PR #116 is unmerged. That clause can be
removed; the conventions themselves remain accurate.

## 1. What is reusable, and what is genuinely new

### Reuse unchanged — no edits to existing files except one `mod` line

- **All of `src/ffi/common.rs`**: `kafka_common_KafkaError_t` + its five
  functions, `box_error`, `CompletionJob`, `spawn_dispatcher`,
  `enqueue_or_run_inline`, `OperationCallbackFn` / `OperationCompletion` /
  `OperationCallbackTarget`. The dispatcher queue is a channel of type-erased
  `Box<dyn FnOnce() + Send>`, so **adding 46 new result shapes requires zero
  changes to `common.rs`** — each op captures its own C fn-pointer, `user_data`
  and owned result handle into its closure.
  Do **not** define a second error type; cbindgen would emit a duplicate.
- `kafka_common_Node_t` + getters, and `kafka_consumer_PartitionInfoList_t` /
  `box_partition_info_list` — cross-module import is already precedented
  (`src/ffi/producer.rs` imports from `ffi::consumer` for `partitions_for`).
- `_confluentkafka.c`'s `fire_handle_cb` generic trampoline body, plus
  `node_to_py` / `partition_info_to_py`.
- Python `KafkaError` (`consumer.py` already does `from producer import
  KafkaError`).
- `src/ffi/mod.rs` gains exactly one `pub(crate) mod admin;` line.

### Copy-and-adapt

- `async_value_op` / `async_void_op` from `src/ffi/consumer.rs` →
  `admin_async_value_op` / `admin_async_void_op`. **Simpler than the
  consumer's**: `Admin` is `Send + Sync` and its methods take `&self`, so the
  consumer's `UnsafeCell` + `AtomicU64` single-owner guard (which mirrors
  Java's `KafkaConsumer.acquire()`) is unnecessary, and there is no `wakeup()`
  so there is no abort path. Drop the guard and release-ordering logic
  entirely.
  **Keep** the consumer's critical invariant: build the C handles inside the
  *completion closure on the dispatcher thread*, never inside the spawned task
  — raw pointers are `!Send` and would make the future `!Send`.
- `AdminHandle { kind: AdminKind, completion_tx, dispatcher, runtime,
  runtime_handle }`, `AdminKind::{Kafka(KafkaAdminClient),
  Mock(Box<MockAdminClient>)}`. Multi-thread runtime, for the reason recorded
  in `producer.rs` (a current-thread runtime only progresses inside
  `block_on`, so the admin background task would stall).
- Python: `consumer.py`'s per-method `(submit, resolve, free)` spec triples and
  `_run_sync` / `_run_async` → `_AdminBase` / `AdminClient` /
  `MockAdminClient` / `AsyncAdminClient` / `AsyncMockAdminClient`.
  Both sync and async Python classes drive the **`_async`** C entry points, as
  producer and consumer do, so no host thread parks inside a native
  `block_on` and Python signal handlers keep running.

### Net-new

- ~46 opaque `kafka_admin_*Result_t` handles with `_count` / `_get_key(i)` /
  `_get_value(i)` / `_get_error(i)` / `_destroy`, following the existing
  `kafka_consumer_OffsetMap_t` shape.
- Options marshaling: a `kafka_admin_AdminClientProperties_t` string-map handle
  (following `kafka_consumer_ConsumerProperties_t`) for construction, plus
  per-RPC option setters where an RPC has non-default options.
- `TopicCollection` input: topic-names **xor** topic-ids with an "exactly one
  non-null" invariant (`admin-client.md` §5). Either two entry points per RPC
  or one discriminated `#[repr(C)]` input struct. Prefer two entry points —
  it keeps the invariant unrepresentable-when-violated rather than
  runtime-checked.
- `Uuid` and `Config` / `ConfigEntry` C representations (no precedent exists).
- Python `admin.py`, and `pyproject.toml` `py-modules += "admin"` (without
  this the module is not installed and tests fail with `ImportError`).
- C tests `bindings/c/tests/test_kafka_admin.c` / `test_mock_admin.c`;
  Python `bindings/python/test/unit/test_admin.py`.

## 2. The one real design problem: per-key results across C

Admin's Rust surface is deliberately **one `KafkaFuture<T>` per key**
(`admin-client.md` §1/§5: "do NOT flatten per-key futures … callers rely on
per-key granularity"). C has no `KafkaFuture` equivalent.

**`KafkaFuture::join_map` exists but is NOT sufficient as-is.** Verified in
`src/common/kafka_future.rs` (`JoinMapFuture::get`):

```rust
map.insert(key.clone(), future.get().await?);   // `?` short-circuits
```

It abandons every remaining key on the first error and yields only that error.
Admin callers need **per-key** outcomes — `createTopics` where topic A succeeds
and topic B fails must report both.

**Recommendation — flatten at the FFI boundary with a new collect-all join:**

1. Add `KafkaFuture::join_map_results(entries) -> KafkaFuture<HashMap<K,
   Result<T, KafkaError>>>` (or an FFI-local equivalent) that awaits every
   future and records each outcome instead of short-circuiting. Small, testable,
   and the natural counterpart to the existing `join_map`.
2. Each `*_async` entry point awaits that once and delivers **one** opaque
   result handle whose `_get_value(i)` / `_get_error(i)` expose the per-key
   outcome. Per-key *data* is fully preserved; only independent per-key
   *timing* is lost.
3. Python drains it to a `dict[key, value | KafkaError]` via the existing
   `*_drain` pattern.

This reuses the entire established pattern end to end and matches librdkafka's
C admin API, which likewise delivers one event per admin call with per-topic
results inside.

Rejected alternative: expose `KafkaFuture<T>` itself as
`kafka_admin_KafkaFuture_t` (mirroring `kafka_producer_FutureRecordMetadata_t`).
Truest to Java, but needs one opaque future type per distinct `T` across 46
result types — a combinatorial explosion in the cbindgen allowlist and in
Python. Revisit only if a concrete need for per-key timing appears.

## 3. Scoping reality, and a decision to take first

Consumer: ~30 trait methods → ~135 exported C functions, 3785 lines.
Admin: **46 methods, 46 result types**, with richer payloads. A naive
full-surface port with both sync and `_async` variants is plausibly 8–12k lines
of `src/ffi/admin.rs` alone.

**Decision to take before any code (D1 below): do C sync variants exist?**
`PLAN.md` specifies both a bare blocking name and an `_async` variant per
method, per producer/consumer convention. But Python drives *only* the `_async`
entry points, so the sync variants serve C consumers exclusively. Dropping them
nearly halves the surface. Recommendation: **ship `_async` only in the first
slice**, add sync variants later if a C consumer asks — additive, non-breaking.

## 4. Phasing

Follows `PLAN.md`'s bindings-slice grouping (which deliberately merges the
finer Rust-core phases, because a full header + Python-module diff per 1–4
method phase would dwarf the work). Each slice is Rust FFI → C tests → Python
→ Python tests, and is independently shippable.

| Slice | RPCs | Why this boundary |
|---|---|---|
| **B0 — Foundation** | none | `AdminHandle`, `admin_async_value_op`, options/properties marshaling, `join_map_results`, `mod.rs` + cbindgen wiring, `admin.py` skeleton, one smoke test each side. No RPCs — proves the plumbing before 46 result types pile on. |
| **B1 — Topics & partitions** | `createTopics`, `deleteTopics`, `listTopics`, `describeTopics`, `createPartitions`, `deleteRecords` | The surface every user hits first, and the one that exercises per-key batch results (`createTopics`) and `TopicCollection` (names xor ids). Design problems surface here or nowhere. |
| **B2 — Cluster, configs, log dirs** | `describeCluster`, `describeConfigs`, `incrementalAlterConfigs`, `listConfigResources`, `describeLogDirs`, `alterReplicaLogDirs`, `describeReplicaLogDirs`, `listClientMetricsResources` | Introduces `Config`/`ConfigEntry` marshaling; reuses `Node`. |
| **B3 — Elections, reassignments, offsets** | `electLeaders`, `alterPartitionReassignments`, `listPartitionReassignments`, `listOffsets` | Partition-keyed throughout. Passes partitions as parallel `topics[]`/`partitions[]` arrays and returns `_get_topic(i)`/`_get_partition(i)`, as `deleteRecords` already does — *not* the consumer's `TopicPartition` handle, which is output-only and, per CLAUDE.md §3, misnamed (`TopicPartition` is `org.apache.kafka.common`, so it should be `kafka_common_TopicPartition_t`). Reusing it would propagate that namespace error into a second public C API. |
| **B4 — Groups & offsets** (= `PLAN.md` slice A) | the 9 Tier-2 group RPCs | Natural "group administration" boundary. |
| **B5a — ACLs and quotas** (= first half of slice B) | `createAcls`, `describeAcls`, `deleteAcls`, `describeClientQuotas`, `alterClientQuotas` | Both domains are type-heavy and both turn on a null-versus-absent distinction C cannot carry in a pointer alone. New `kafka_common_*` types: `AclBinding`, `AclBindingFilter`, `ClientQuotaEntity`, plus the four ACL enums as `code()` values and the wire match-type constants. |
| **B5b — SCRAM, delegation tokens, features** (= second half of slice B) | `describeUserScramCredentials`, `alterUserScramCredentials`, `describeDelegationToken`, `createDelegationToken`, `renewDelegationToken`, `expireDelegationToken`, `describeFeatures`, `updateFeatures` | The remaining Tier-3 phases 3–5. Independent of B5a: no shared C types. |
| **B6 — Producers & transactions** (= slice C) | `describeProducers`, `describeTransactions`, `abortTransaction`, `forceTerminateTransaction`, `listTransactions`, `fenceProducers` | The richest per-key shapes (`fenceProducers` → `ProducerIdAndEpoch` per id, `describeProducers` per partition) — deliberately last, once the batch-result pattern is settled. |

**B5 was split in two (Manager, after B4).** As originally tabled it was 13 RPCs
across five unrelated domains, each needing new C types with no precedent — roughly
twice B2's type-design load in one slice. B5a is ACLs + client quotas (both
type-heavy, both with null-versus-absent distinctions); B5b is SCRAM + delegation
tokens + features. The two halves share no C types, so the boundary costs nothing.

`MockAdminClient` bindings ride along in each slice (mirroring
`test_mock_consumer.c` / `MockConsumer`), because the C and Python unit tests
need the mock to test without a broker.

Multilanguage/gRPC harness wiring stays **out of scope**, per `PLAN.md` — it
was wired last for producer and consumer alike, well after the bindings were
stable.

## 5. Per-slice checklist (gates that are easy to forget)

1. `cbindgen.toml` `[export] include` — append **every** new opaque type *and*
   every new `*_callback_t`. It is a 36-entry explicit allowlist; anything
   missing is silently omitted from the generated header and the C test fails
   with an incomplete-type error. There is no `.h` in the repo —
   `target/include/confluent_kafka.h` is generated by `build.rs` under
   `--features ffi`.
2. `pyproject.toml` `py-modules += "admin"` (once, in B0).
3. `bindings/c/CMakeLists.txt` — `add_executable` +
   `target_include_directories` + `target_link_libraries` + `add_test`, three
   to four lines per test binary, fully manual.
4. Callback obligation: every `_async` entry point must fire its callback
   exactly once on every path, including post-teardown — that is what
   `enqueue_or_run_inline` is for. A dropped callback hangs the Python
   `threading.Event` in `_run_sync` forever.
5. Ownership: exactly one of `(result_handle, error_handle)` is non-null and
   the callee owns and frees it. Sub-handles from getters are **borrowed
   `*const`**, valid only until the parent's `_destroy`.
6. `make verify` before calling a slice done — it runs `build format-check lint
   test`, where `test` = `test-multilanguage test-c test-python`.
   Note it does **not** include `test-rust`; run `cargo test` separately.
7. Submodule: `bindings/c/tests/unity` is not initialized in a fresh checkout —
   `make submodules` first.

## 6. Definition of Done

Full `.claude/rules/definition-of-done.md`, with the adjustments already
recorded in `PLAN.md` and `admin-client.md` §10: **#10 (hot-path allocation
audit) is N/A** — Admin is batch/administrative, no per-record path; state
this explicitly per slice rather than skipping silently. **#11 (consumer trait
surface check) does not apply** to Admin's trait, but its spirit does: per-RPC
methods stay plain `fn`, only `close()` is `async fn`.

Additionally, per slice:
- C tests cover both `MockAdminClient` and (where a broker fixture exists) the
  real client, including at least one **partial-failure** batch case — the
  whole point of per-key errors.
- Python tests mirror `test_consumer.py` conventions (`asyncio_mode = "auto"`,
  flat `test_*` functions) and include the two lifetime/interrupt cases that
  file establishes: handle-lifetime after GC, and SIGINT interrupting a sync
  call with the client still usable afterwards.
- **Request-direction pinning, sharpened after B5b.** A mock that throws **per
  key** still echoes the key set back, so the key columns *are* pinned by any
  pre-existing per-key test; only the **payload** columns are dead. A mock that
  fails the **whole call** echoes nothing, so every column is dead. Both cases
  still owe the extracted row-builder a direct unit test — the discriminants
  live in the payload — but do not claim "no test pins any column" when the key
  columns are covered.
- **"Mock implemented" does not mean "request observable."** The discard audit
  is per *field*, not per RPC: `MockAdminClient.createDelegationToken` is fully
  implemented yet ignores `options.owner()` entirely (it uses
  `renewers().get(0)`), so the owner needed an options-level test even though the
  RPC has end-to-end coverage. Check each option field of each new RPC.
- **Before writing a drain, check whether the mock has state to seed.** If Java's
  `MockAdminClient` has a `Builder` setter or an `update*` method for it, export
  the equivalent: it beats substituting a `_to_*`-converter test because it
  exercises the C builder too. Cite the Java setter lines in the rustdoc
  (B5b's `kafka_admin_MockAdminClient_set_feature_levels` is the precedent).
- **A literal translation of a Java `get(0)` / `get(key)` that can throw becomes
  a completed-exceptionally future in Rust, never an index panic.** Java's throw
  is a catchable `RuntimeException`; a Rust panic unwinds across `extern "C"`
  (every FFI path runs `submit` inline on the calling thread) and aborts the
  process. `MockAdminClient::create_delegation_token` is the case that caught
  this.

## 7. Decisions taken (Manager, 2026-08-06)

User direction: *"Keep it according to the existing code and as compliant with
Java as possible."* That resolves D1–D3.

- **D1 — Ship both sync and `_async` C variants.** Consistency with the existing
  code decides this: every blocking-in-Java producer/consumer method exposes
  both a bare blocking name and an `_async` variant, and `PLAN.md` specifies the
  same for Admin. (An earlier draft of this plan suggested `_async`-only to
  reduce surface area; consistency with the established convention takes
  priority.) Bare name = sync (`block_on`, joining all per-key futures — the C
  equivalent of Java's `result.all().get()`); `_async` = callback-based.
- **D2 — Results: one flattened result handle per RPC, with accessors mirroring
  the Java result type.** One opaque `kafka_admin_*Result_t` per RPC, delivered
  through one callback and freed with `_destroy`. The *rest* of the accessor
  set follows whatever the Java result actually carries — do not assume a
  keyed shape:

  | Java result shape | C accessors |
  |---|---|
  | `Map<K, KafkaFuture<V>>` | `_count` / `_get_key(i)` / `_get_value(i)` / `_get_error(i)` |
  | `Map<K, KafkaFuture<Void>>` / `Map<K, Optional<Throwable>>` | `_count` / `_get_key(i)` / `_get_error(i)` — no value |
  | one `KafkaFuture<Map<K, V>>` for the whole listing | `_count` / `_get_key(i)` / `_get_value(i)`; failure is the call's error |
  | one future fanned into a listing **plus an unkeyed error collection** of a different length (`ListGroupsResult.valid()`/`errors()`) | `_valid_count` / `_get_valid(i)` **and** `_error_count` / `_get_error(i)` — no `_count`, no `_get_key`, because the two sequences are not co-indexed |
  | **every** future the Java result exposes publicly is a `KafkaFuture<Void>` | **no result handle at all** — success is a null return / a null `error` in the callback, following `kafka_admin_AdminClient_close_async`'s callback shape |

  **Fifth rule — when the per-key value `V` is itself a collection**, the four
  rows above do not say whether `_get_value(i)` should return a minted handle
  or whether the collection should be flattened into a second index. Both
  answers are already in the tree, so the rule is fixed here before B6:

  > Flatten to a second index (`_get_<x>_count(i)` / `_get_<x>(i, j)`) when the
  > collection's **element** is scalar-only. Mint a `*_t` value handle,
  > returned by `_get_value(i)`, as soon as that element **itself contains a
  > collection** — because the handle gives the inner collection an index space
  > starting at 0, whereas flattening would need a third index.
  >
  > **Two index levels is the limit — per handle, not per RPC.** A
  > `_get_x(i, j, k)` signature is where flattening stops being readable in C
  > and the accessor count starts multiplying. Minting a handle is precisely
  > the move that *resets* the budget, so the mechanical form of the rule is:
  > **flatten while the remaining depth is ≤ 2 from the current handle; mint
  > when it would exceed that.** An RPC may therefore address three or more
  > levels in total, as `describeLogDirs` does across three handles.

  All three shipped precedents fall out of it mechanically:

  - `describeLogDirs` (B2) is `Map<Integer, KafkaFuture<Map<String,
    LogDirDescription>>>`, and `LogDirDescription` carries
    `Map<TopicPartition, ReplicaInfo>` on top of `error()` / `totalBytes()` /
    `usableBytes()`. Fully flattened that is
    `_get_replica_topic(i, j, k)` — three levels. So B2 minted
    `kafka_admin_LogDirDescriptionMap_t` **and**
    `kafka_admin_LogDirDescription_t`, and flattened the replica map onto the
    latter at a single index.
  - `deleteAcls` (B5a) is `Map<AclBindingFilter, KafkaFuture<FilterResults>>`,
    and `FilterResults` is a list and nothing else, whose element `FilterResult`
    is a two-field union (binding **xor** exception) with no collection inside.
    Two levels suffice, so B5a shipped `_get_result_count(i)` /
    `_get_binding(i, j)` / `_get_result_error(i, j)` with no new handle.
  - `describeTopics` (B1) is the third worked example, and the one that shows
    the rule applying **recursively**. `V = TopicDescription` carries
    `List<TopicPartitionInfo>`, whose element carries `List<Node>` (replicas,
    ISR, ELR) — so the element is *not* scalar-only and fully flattening would
    need `_get_partition_replica(i, j, k)`. B1 accordingly shipped
    `_get_value(i)` → `kafka_admin_TopicDescription_t` →
    `_partition(j)` → `kafka_admin_TopicPartitionInfo_t` → flattened nodes
    (`src/ffi/admin.rs`), each handle spending at most two index levels. The
    main rule predicts exactly that shape.

  Rejected the alternative discriminator "mint when the value is a named Java
  type users hold, flatten when it is an anonymous list wrapper": `FilterResults`
  and `LogDirDescription` are both named, public, user-visible Java classes, so
  that test does not separate the two precedents. Index depth does, and it is
  the property that actually makes the C surface unusable.

  Two clarifications the rule is **not** about, so B6 does not over-apply it:

  - A `V` that is a *single record* of scalars is not a collection at all. Keep
    flattening its fields onto index `i` — `_get_<field>(i)` — as
    `OffsetAndMetadata` (B4) and `ReplicaLogDirInfo` (B2) already do. B6's
    `fenceProducers` (`Map<String, KafkaFuture<ProducerIdAndEpoch>>`) is this
    case, not the collection case.
  - A record with scalars *and* one collection **whose element is scalar-only**,
    keyed directly by the result (not nested inside another collection), still
    flattens: the scalars sit at `i` and the collection at `(i, j)`, which is
    two levels. B6's `describeTransactions` (`TransactionDescription`: scalars
    plus `Set<TopicPartition>`, and a `TopicPartition` is two scalars) is this
    case. `describeProducers` (`PartitionProducerState` → `List<ProducerState>`,
    all scalars) is the plain flatten case.

    The "element is scalar-only" qualifier is load-bearing, not decoration:
    without it this clarification contradicts the main rule on
    `TopicDescription`, which is *also* "a record with scalars and one
    collection, keyed directly by the result" and correctly did **not** flatten,
    because a `TopicPartitionInfo` element contains three more collections.

  **The void-result row (added for B6; restated by payload type after the
  round-11 review.)** The discriminator is the **generic parameter of the
  futures the Java result exposes**, not a judgement about whether the result
  "carries anything useful":

  > **Every public future on the result is a `KafkaFuture<Void>` → no result
  > handle. Any public future carries a value → a result handle, however small
  > that value is.**

  Stated that way the rule is mechanically checkable from the Java result class
  alone — read the generic parameters of its public accessors — and it needs no
  appeal to intent. Both B6 void results satisfy it exactly, each exposing one
  `Void` future and keeping its per-key map private:

  - `AbortTransactionResult` exposes exactly one method, `all() -> KafkaFuture<Void>`;
    its per-partition `Map<TopicPartition, KafkaFuture<Void>>` is `private`, and
    the RPC takes exactly one `AbortTransactionSpec`, so there is one key by
    construction.
  - `TerminateTransactionResult` exposes exactly one method,
    `result() -> KafkaFuture<Void>`, over a `private final KafkaFuture<Void>`.

  The rationale for the shape is unchanged: "one opaque result handle per RPC"
  exists to carry per-key data and errors across a boundary with no
  `KafkaFuture`; with a `Void` payload there is nothing to carry, so a handle
  whose only method is `_destroy` is ceremony plus a leak to get wrong. The
  precedent is already in the module: `close_async`'s error-only callback and
  `admin_sync_value_op` with `T = ()`.

  The payload-type phrasing also explains B5b's opposite decision without
  appealing to judgement: `RenewDelegationTokenResult` and
  `ExpireDelegationTokenResult` each expose a `KafkaFuture<Long>` (the new expiry
  timestamp), so they take a handle. Same "one whole-call future, no per-key
  granularity" shape; different payload type; different answer.

  Per-key *data and errors* are preserved wherever Java expresses them — only
  independent per-key *timing* is lost, which C has no `KafkaFuture` to convey.
  Requires the collect-all join in §2 (`join_map` short-circuits). Rejected the
  one-opaque-future-per-`T` alternative: 46 result types would explode the
  cbindgen allowlist and the Python layer for a capability no caller has asked
  for.

  *(Amended again after B5a, for the collection-valued-`V` rule above: the
  first four rows silently assumed a scalar or record `V`, and B2 and B5a had
  answered the collection case two different ways without either being written
  down. Both shipped shapes are preserved — the rule is a codification, not a
  change.)*

  *(Amended after B3. The original wording said "per-key value **and** error"
  unconditionally, which is unsatisfiable for three of B3's four RPCs —
  `electLeaders` carries no per-key value, `listPartitionReassignments` no
  per-key error — and would recur in B4 and B6. Following Java's result shape
  is the more faithful reading, is what B3 shipped, and still lets a C caller
  reach every outcome Java can.)*

  **Addendum (2026-09-15) — superseded for the Topics-family `_async` entry
  points.** D2's "one flattened result handle per RPC, delivered through one
  callback" is no longer the shape for the seven Topics-family async entry
  points: `kafka_admin_AdminClient_create_topics_async`,
  `_delete_topics_async`, `_delete_topics_by_ids_async`,
  `_describe_topics_async`, `_describe_topics_by_ids_async`,
  `_create_partitions_async`, `_delete_records_async`. Their callback now
  fires **once per key, independently, as that key's own future resolves** —
  restoring the per-key *timing* granularity this section's closing paragraph
  said C had no way to convey (it does, once the callback itself is
  per-key rather than per-batch). There is no flattened result handle on this
  path at all; each key's value (if any) arrives as its own small owned
  handle (`kafka_admin_TopicMetadataAndConfig_t` / `kafka_admin_TopicDescription_t`
  / the new `kafka_admin_DeletedRecords_t`), freed independently of any
  `*Result_t`. The **synchronous** entry points for these same seven RPCs are
  unaffected — they still return one flattened `kafka_admin_*Result_t`, a
  faithful translation of Java's synchronous-style `KafkaFuture.allOf(...).get()`
  usage, per `admin-client.md` §1. Every other RPC's `_async` entry point is
  also unaffected and still follows D2 as written above.

  The shared mechanism (`admin_async_per_key_op` in `src/ffi/admin.rs`,
  registering `KafkaFuture::when_complete` per key instead of joining via
  `KafkaFuture::join_map_results`) is designed to extend to the remaining
  per-key RPCs in later phases; see the plan this addendum was written
  against for the phase list. Motivation: the joined shape did not match
  Java's per-key `KafkaFuture` contract (a slow or failed key held up every
  other key in the same call), nor real `confluent_kafka`'s
  `AdminClient.create_topics()`, which this repo's own
  `bindings/python/test/performance/performance_common.py` already assumes.

  **Addendum (2026-09-15, Phase B) — also superseded for the Configs-family
  `_async` entry points.** `kafka_admin_AdminClient_describe_configs_async`
  and `kafka_admin_AdminClient_incremental_alter_configs_async` now fire
  their callback once per **resource** (`ConfigResource`), independently, via
  the same `admin_async_per_key_op` mechanism, reusing the topics addendum's
  reasoning verbatim. Two differences worth recording since this is the
  mechanism's first reuse against a non-string key:
    - `ConfigResource` is a composite key (a type code plus a name), delivered
      as two parameters — `(resource_type: i32, resource_name: *const char)`
      — rather than a single opaque key handle, matching how the pre-existing
      *flattened* `DescribeConfigsResult`/`AlterConfigsResult` accessors
      already expose this same key
      (`kafka_admin_DescribeConfigsResult_get_key_type`/`_get_key_name`) —
      the per-key callback did not invent a new representation for it.
    - `incrementalAlterConfigs`'s C rows are one per *operation*
      (`read_alter_config_ops`'s existing flattening — several config ops can
      target the same resource), but Java's future is one per *resource*
      (`KafkaAdminClient.java:2870`, iterating `configs.keySet()`). A new
      `distinct_config_resources` helper de-dupes the flat rows down to the
      resource set before fan-out, computed independently of the (fallible)
      op-type parse so a bad op-type code still fans its error out over every
      named resource rather than none. `describeConfigs`'s rows are already
      one per resource, so it needs no such de-duplication.
  `kafka_admin_Config_t` gains a standalone owned-handle destructor
  (`kafka_admin_Config_destroy`), the Configs-family analog of
  `kafka_admin_TopicMetadataAndConfig_destroy` — same dual-provenance pattern
  (borrowed from a flattened `*Result_t` vs. owned from the per-key callback).
  The **synchronous** entry points for both RPCs are unaffected, per the same
  rule as the topics addendum.

  **Addendum (2026-09-15, Phase C) — also superseded for the Log-dirs-family
  `_async` entry points.** `kafka_admin_AdminClient_describe_log_dirs_async`,
  `_alter_replica_log_dirs_async` and `_describe_replica_log_dirs_async` now
  fire their callback once per **broker id** (`describeLogDirs`) or per
  **replica** (`alterReplicaLogDirs`/`describeReplicaLogDirs`), independently,
  via the same `admin_async_per_key_op` mechanism, reusing the Topics/Configs
  addenda's reasoning verbatim. Notes specific to this family:
    - `describeLogDirs`'s key is a plain broker id (`i32`), delivered as a
      single scalar parameter, no composite-key handling needed.
    - `alterReplicaLogDirs`/`describeReplicaLogDirs`'s key is a
      `TopicPartitionReplica` — a *three*-part composite key — delivered as
      `(topic: *const char, partition: i32, broker_id: i32)`, extending the
      Configs addendum's two-part `ConfigResource` precedent by one field and
      matching how the pre-existing *flattened*
      `AlterReplicaLogDirsResult`/`DescribeReplicaLogDirsResult` accessors
      already expose this same key (`_get_topic`/`_get_partition`/
      `_get_broker_id`).
    - Per §D2's fifth rule (mint a handle when the element itself contains a
      collection), `describeLogDirs`'s per-broker value
      (`Map<String, LogDirDescription>`) already had a nested handle
      (`kafka_admin_LogDirDescriptionMap_t`) minted for the flattened sync
      result path. Phase C changes only the OUTER per-broker delivery; that
      inner handle type is reused as-is, just delivered individually and
      owned (a new `kafka_admin_LogDirDescriptionMap_destroy`) instead of
      embedded and borrowed inside a `kafka_admin_DescribeLogDirsResult_t` —
      the same dual-provenance pattern as `kafka_admin_Config_t` /
      `kafka_admin_Config_destroy` in the Phase B addendum.
      `kafka_admin_ReplicaLogDirInfo_t` gets the same treatment
      (`kafka_admin_ReplicaLogDirInfo_destroy`).
    - `describeReplicaLogDirs` against `MockAdminClient` surfaced a real gap
      in `admin_async_per_key_op` itself (COMMENTS.67): the mock
      (`MockAdminClient.describeReplicaLogDirs`, mirroring Java's own mock's
      `MockAdminClient.java:1112`) skips a replica of a topic it does not
      know entirely, rather than reporting an error for it, so the Rust core
      never creates a future for that key at all. Combined with the per-key
      `Future`-dict-returned-immediately contract — every requested key
      already has a caller-side `Future`/`Promise` before the native call
      runs — that key's `Future` never resolved: an initial version of this
      phase documented this as an accepted, Java-faithful mock limitation,
      but a Critic review correctly rejected that framing. Java's own mock
      never makes the "every key gets a `Future`" promise in the first
      place — a missing key is simply absent from the returned `Map`,
      detectable immediately by a Java caller — so the hang was entirely a
      consequence of *this port's own wrapping design*, not something
      inherited faithfully from Java.

      **Fixed generically in `admin_async_per_key_op`**, not per-RPC: after
      `submit` succeeds, any key present in the caller's `keys` list but
      absent from the returned `entries` now gets an explicit synthetic
      error (`Error::local_illegal_state`), fired synchronously before the
      real entries are registered — extending the existing total-submission-
      failure fan-out to also cover this partial-success gap. Since
      `admin_async_per_key_op` is shared by every per-key RPC (Phases A-C so
      far, D-G to come), this closes the gap for all of them at once; Phases
      A and B's RPCs are unaffected in practice because none of their
      `entries` builders can currently produce fewer entries than `keys`
      (verified: full Rust/Python/C suites unchanged after the fix). The new
      bound `K: PartialEq` on `admin_async_per_key_op` is satisfied by every
      existing key type (`String`, `TopicPartition`, `ConfigResource`, `i32`,
      `TopicPartitionReplica`), all already `Eq + Hash` for their `HashMap`
      usage elsewhere.
      `bindings/python/test/unit/test_admin.py`'s
      `test_describe_replica_log_dirs_omits_unknown_topics` was rewritten
      (COMMENTS.DONE.67) to assert the unknown-topic replica's `Future`
      resolves with an explicit `KafkaError` within a bounded timeout,
      replacing its old "proves the pending state" framing. Against a real
      broker every requested key always resolves with a value regardless
      (`KafkaAdminClient.java:3066-3068`, `:3141-3145`).
  The **synchronous** entry points for all three RPCs are unaffected, per the
  same rule as the Topics/Configs addenda.

  **Addendum (2026-09-15, Phase D) — also superseded for the
  Partitions/offsets-family `_async` entry points.**
  `kafka_admin_AdminClient_alter_partition_reassignments_async` and
  `_list_offsets_async` now fire their callback once per **partition**
  (`TopicPartition`), independently, via the same `admin_async_per_key_op`
  mechanism, reusing the Topics/Configs/Log-dirs addenda's reasoning
  verbatim. Notes specific to this family:
    - The key is a `TopicPartition` (a two-part composite: topic name plus
      partition id), delivered as `(topic: *const char, partition: i32)` —
      the same shape `delete_records` (Phase A) already established for this
      exact key type — matching how the pre-existing *flattened*
      `AlterPartitionReassignmentsResult`/`ListOffsetsResult` accessors
      already expose it (`_get_topic`/`_get_partition`). No new key
      representation was invented.
    - `alterPartitionReassignments`'s per-partition future is
      `KafkaFuture<Void>`, so its callback has no value parameter — the same
      shape as `alter_replica_log_dirs` (Phase C).
    - `listOffsets`'s per-partition future is `KafkaFuture<ListOffsetsResultInfo>`.
      Per §D2's existing (already-minted) handle rule, the flattened sync
      result already had a `kafka_admin_ListOffsetsResultInfo_t` handle type;
      Phase D reuses `ListOffsetsResultInfoInner` as-is, just delivered
      individually and owned (a new `kafka_admin_ListOffsetsResultInfo_destroy`)
      instead of embedded and borrowed inside a `kafka_admin_ListOffsetsResult_t`
      — the same dual-provenance pattern as `kafka_admin_Config_t` (Phase B)
      and `kafka_admin_LogDirDescriptionMap_t` (Phase C).
    - `listOffsets` is the first RPC in this file whose real (non-mock)
      `Admin` implementation resolves its per-key futures through the
      `AdminApiDriver`/`PartitionLeaderStrategy` machinery (`admin-client.md`
      §2) rather than a single `Call` — partitions sharing a leader tend to
      resolve together in practice on a real broker. This does not change
      the per-key *contract* (`Admin::list_offsets` still returns one
      `KafkaFuture` per partition, and `admin_async_per_key_op` still
      registers one independent `when_complete` per entry); it only means a
      genuinely independent completion *timing* between two partitions of
      this RPC is harder to demonstrate end-to-end than for a `Call`-based
      RPC. The direct `admin_async_per_key_op`-level Rust tests (driving two
      hand-built `KafkaFutureImpl<ListOffsetsResultInfo>` instances, one
      resolved and one left pending) are unaffected by this, since they
      exercise the mechanism directly rather than through a real
      leader-lookup round trip.
    - Both RPCs' `keys` (for `admin_async_per_key_op`'s fan-out) are computed
      independently of their respective fallible per-entry parses
      (`read_reassignments`'s empty-replica-list check, `read_offset_specs`'s
      sentinel check, and `list_offsets_options`'s isolation-level check) —
      the same "keys computed independently of the fallible parse" pattern
      `delete_topics_by_ids_entries` established in Phase A — so a
      marshaling failure still fans an explicit error out to every requested
      key rather than leaving any of them without a callback.
  The **synchronous** entry points for both RPCs are unaffected, per the same
  rule as the Topics/Configs/Log-dirs addenda.

  **Addendum (2026-09-16, Phase E) — also superseded for the
  Consumer-groups-family `_async` entry points.**
  `kafka_admin_AdminClient_describe_consumer_groups_async`,
  `_describe_classic_groups_async`, `_list_consumer_group_offsets_async`,
  `_alter_consumer_group_offsets_async`, `_delete_consumer_group_offsets_async`,
  `_delete_consumer_groups_async` and
  `_remove_members_from_consumer_group_async` now fire their callback once per
  key, independently, via the same `admin_async_per_key_op` mechanism, reusing
  the Topics/Configs/Log-dirs/Partitions-offsets addenda's reasoning verbatim.
  This family splits into two distinct Java key shapes, unlike the four prior
  phases which were each uniform:
    - **Map-of-groups** (a top-level `Map<String, KafkaFuture<...>>`, one
      independent future per group id): `describeConsumerGroups`,
      `describeClassicGroups`, `listConsumerGroupOffsets` and
      `deleteConsumerGroups`. The key is a group id (`*const char`), the same
      shape `create_topics`/`delete_consumer_groups` already established for a
      plain string key.
    - **Single-group, many-sub-key** (ONE Java `KafkaFutureImpl` underlying
      every per-key view via `whenComplete`/`thenApply` —
      `AlterConsumerGroupOffsetsResult.partitionResult`,
      `DeleteConsumerGroupOffsetsResult.partitionResult`,
      `RemoveMembersFromConsumerGroupResult.memberResult`):
      `alterConsumerGroupOffsets` and `deleteConsumerGroupOffsets` (keyed by
      `TopicPartition`, the same `(topic, partition)` shape as
      `alter_partition_reassignments`/`list_offsets`), and
      `removeMembersFromConsumerGroup` (keyed by group instance id). Because
      every per-key view derives from the *same* source future, all of a
      call's keys necessarily resolve at the exact same instant — there is no
      genuine temporal independence to demonstrate for these three, unlike the
      four map-of-groups RPCs above.
  Notes specific to this family:
    - `describeConsumerGroups`'s per-group future is
      `KafkaFuture<ConsumerGroupDescription>` and `describeClassicGroups`'s is
      `KafkaFuture<ClassicGroupDescription>`. Per §D2's existing handle rule,
      both flattened sync results already had `kafka_admin_ConsumerGroupDescription_t`
      / `kafka_admin_ClassicGroupDescription_t` handle types; Phase E reuses
      each `Inner` struct as-is, just delivered individually and owned (new
      `kafka_admin_ConsumerGroupDescription_destroy` /
      `kafka_admin_ClassicGroupDescription_destroy`) instead of embedded and
      borrowed inside the flattened result — the same dual-provenance pattern
      as `kafka_admin_ListOffsetsResultInfo_t` (Phase D).
    - `listConsumerGroupOffsets`'s per-group future is
      `KafkaFuture<Map<TopicPartition, OffsetAndMetadata>>` — a *nested* value,
      like `describeLogDirs` (Phase C). The existing flattened
      `kafka_admin_OffsetAndMetadataMap_t` handle is reused as-is under the
      same dual-provenance pattern, with a new
      `kafka_admin_OffsetAndMetadataMap_destroy`.
    - `deleteConsumerGroups`'s per-group future, and all three
      single-group-many-sub-key RPCs' per-sub-key futures, are
      `KafkaFuture<Void>`, so those four callbacks have no value parameter —
      the same shape as `alter_partition_reassignments` (Phase D).
    - `alterConsumerGroupOffsets` and `deleteConsumerGroupOffsets` with an
      empty offsets/partitions argument, and `removeMembersFromConsumerGroup`
      in `removeAll` mode (or with an empty non-`removeAll` member list, which
      Java's options constructor itself rejects synchronously), have **no
      per-key slot at all** — unlike the map-of-groups RPCs and unlike Phases
      A/D's per-key RPCs, whose empty-input case is simply "zero keys, zero
      callbacks" with no whole-call observable lost. Here Java's own `all()` is
      the *only* observable in that case, and the per-key delivery model has no
      channel to carry it: the callback fires zero times rather than reporting
      the whole-call outcome. This is a deliberate, documented limitation (see
      the callback typedef doc comments in `src/ffi/admin.rs` and the
      `admin.py` module docstring), not an oversight.
    - All seven RPCs' `keys` (for `admin_async_per_key_op`'s fan-out) are
      computed independently of their respective fallible parses, the same
      "keys computed independently of the fallible parse" pattern established
      in Phase A and continued through Phase D — so a marshaling failure still
      fans an explicit error out to every requested key (when any exist)
      rather than leaving one without a callback.
  The **synchronous** entry points for all seven RPCs are unaffected, per the
  same rule as the Topics/Configs/Log-dirs/Partitions-offsets addenda.

  **Addendum (2026-09-16, Phase F) — also superseded for the
  ACLs/quotas/features-family `_async` entry points.**
  `kafka_admin_AdminClient_create_acls_async`, `_delete_acls_async`,
  `_alter_client_quotas_async`, `_alter_user_scram_credentials_async` and
  `_update_features_async` now fire their callback once per key,
  independently, via the same `admin_async_per_key_op` mechanism. Unlike
  Phase E's split, all five of this family's Java `*Result` types are
  genuine `Map<K, KafkaFuture<V>>` — none derives multiple per-key views from
  one shared future — so there is no single-group-many-sub-key wrinkle here;
  the interesting variation this phase adds is in the *key* shape itself:
    - `createAcls` is keyed by `AclBinding`, whose constituent
      `ResourcePattern`/`AccessControlEntry` **validate** (an ANY resource
      type, an ANY/MATCH pattern type, etc. are rejected). A malformed row
      therefore cannot be represented as a real `AclBinding` for the per-key
      fan-out, so the key delivered to the callback is the raw `AclBindingKey`
      tuple `(resource_type, resource_name, pattern_type, principal, host,
      operation, permission_type)` — computed independently of the validated
      parse (the by-now-standard "keys computed independently of the fallible
      parse" pattern from Phase A onward), never through `AclBinding`'s
      validating constructors — packaged into an **owned**
      `kafka_common_AclBinding_t` handle (freed by a new
      `kafka_common_AclBinding_destroy`), the same opaque type the existing
      flattened sync result (`kafka_admin_CreateAclsResult_get_binding`)
      already exposes borrowed — the dual-provenance pattern from
      `kafka_admin_ListOffsetsResultInfo_t` (Phase D) applied to a *key*
      rather than a value for the first time in this series.
    - `deleteAcls` is keyed by `AclBindingFilter`, whose constructors are
      infallible (a filter's ANY/MATCH values and nullable strings are the
      whole point of a filter), so no raw-tuple stand-in is needed there — the
      real `AclBindingFilter` doubles as the key, delivered as an owned
      `kafka_common_AclBindingFilter_t` handle (new
      `kafka_common_AclBindingFilter_destroy`), same dual-provenance pattern.
      Its value, `FilterResults` (a `List<FilterResult>`, one row per matched
      ACL, each carrying either a binding or its own exception), is a
      genuinely *nested* value like `describeLogDirs` (Phase C) and
      `listConsumerGroupOffsets` (Phase E) — but unlike those, there was no
      existing "whole nested value" handle type to reuse under dual
      provenance, since the old flattened sync result embedded its nested
      rows directly (`kafka_admin_DeleteAclsResult_get_result_count`/
      `_get_binding`/`_get_result_error`, addressed by the *outer* filter
      index) rather than through a handle representing one filter's whole
      `FilterResults`. Phase F mints that handle for the first time,
      `kafka_admin_DeleteAclsFilterResults_t` (reusing the existing
      `DeleteAclsFilterResultInner` per-row struct, still shared with the
      synchronous path's own nested rows), with its own
      `kafka_admin_DeleteAclsFilterResults_destroy`.
    - `alterClientQuotas` is keyed by `ClientQuotaEntity`. Unlike
      `AclBinding`, `ClientQuotaEntity::new` does not validate at all (it is a
      bare `HashMap<String, Option<String>>` wrapper), so — like
      `AclBindingFilter` — the real type doubles as the key with no raw-tuple
      stand-in, computed independently of `read_client_quota_alterations`'s
      validated (and duplicate-rejecting) parse. Delivered as an owned
      `kafka_common_ClientQuotaEntity_t` handle (new
      `kafka_common_ClientQuotaEntity_destroy`), the same opaque type the
      existing flattened sync result already exposes borrowed.
    - `alterUserScramCredentials` and `updateFeatures` are both keyed by a
      plain string (username / feature name respectively) with a
      `KafkaFuture<Void>` value — the same shape `delete_consumer_groups`
      (Phase E) already established for a plain string key with no handle to
      free; nothing new here beyond reusing that shape twice more.
    - `updateFeatures` is the one RPC across all six phases so far whose
      top-level `Admin` method call is itself fallible (Java's real
      `KafkaAdminClient.updateFeatures` throws `IllegalArgumentException` for
      an empty update map — `src/admin/mod.rs`'s `update_features` returns
      `Result<UpdateFeaturesResult, Error>`, not a bare `UpdateFeaturesResult`,
      to carry it). With a non-empty map this behaves like every other
      fallible-submission RPC (the shared error fans out to every key via
      `admin_async_per_key_op`'s existing "submission failed" branch). With an
      **empty** map there are zero keys, so — like Phase E's
      `alterConsumerGroupOffsets`/`removeMembersFromConsumerGroup` empty-input
      cases — the callback fires zero times and this rejection has no channel
      to travel through at all; a documented limitation (see the callback
      typedef's doc comment in `src/ffi/admin.rs` and `update_features`'s
      docstring in `admin.py`), not an oversight.
  The **synchronous** entry points for all five RPCs are unaffected, per the
  same rule as every prior addendum in this section.

  **Addendum (2026-09-17, Phase G — final phase; completes the 28-RPC
  rollout) — superseded for the producers/transactions-family `_async` entry
  points.** `kafka_admin_AdminClient_describe_producers_async`,
  `_describe_transactions_async` and `_fence_producers_async` now fire their
  callback once per key, independently, via the same `admin_async_per_key_op`
  mechanism. All three of their Java `*Result` types are genuine
  `Map<K, KafkaFuture<V>>` (keyed by `TopicPartition` for describeProducers, by
  the transactional id — Java's `CoordinatorKey` — for the other two), so there
  is no single-key-many-view wrinkle. Two things are new to this phase:
    - **The per-key value reuses the flattened result handle carrying a single
      key**, rather than a dedicated value handle. Every prior rich-value phase
      (Config in B, LogDirDescriptionMap in C, ConsumerGroupDescription in E,
      DeleteAclsFilterResults in F) had — or minted — a *standalone* value
      handle that the synchronous flattened result also exposed via
      `_get_value`. These three RPCs are the only converted ones whose sync
      result flattens the value directly into indexed getters with **no**
      standalone value handle, and whose Java value types
      (`PartitionProducerState`, `TransactionDescription`, `ProducerIdAndEpoch`)
      have no C handle of their own. Rather than add new C types (DoD #7) or
      destabilise the stable, tested synchronous getter API by refactoring it
      onto a new value handle, the per-key callback boxes a single-key
      `DescribeProducersResult_t` / `DescribeTransactionsResult_t` /
      `FenceProducersResult_t` (one entry, readable at index 0) as the value,
      freed by the *same* `_destroy` the synchronous path uses. This is
      dual-provenance-safe like `kafka_admin_ListOffsetsResultInfo_t` (Phase D),
      but with both provenances **owned** (sync multi-row, async single-row) and
      one fresh box per firing — no borrowed aliasing at all. The Python drains
      (`_drain_partition_producer_state` etc.) reuse the sync path's own
      `DescribeProducersResult_drain` + `_to_describe_producers` unpacker, taking
      the single value out of the resulting one-entry dict.
    - **`fenceProducers`' per-key future needed a crate-internal accessor.**
      Java exposes the per-id future only through its `producerId(id)` /
      `epochId(id)` / `fencedProducers()` projections; the flattened sync path
      joins the first two back into one `ProducerIdAndEpoch`. Per-key delivery
      needs the whole `ProducerIdAndEpoch` as one future, so
      `FenceProducersResult` gains a `pub(crate) fn futures()` accessor (the FFI
      reads the underlying map those projections are built from — not a new
      public API, and not visible to Java-mirroring callers).
    - **`listTransactions` is deliberately LEFT JOINED** — the one RPC in the
      B6 slice not converted, and the final joined exception of the whole
      rollout. Java's `ListTransactionsResult` is a single
      `KafkaFuture<Map<Integer, KafkaFuture<Collection<TransactionListing>>>>`
      fanned across brokers via `byBrokerId()`/`all()`/`allByBrokerId()`, with
      **no** per-transactional-id future map — the same category as `listTopics`
      / `listGroups`, which §D2's original wording and Phase A left joined. It
      keeps its whole-call `fire_handle_cb` callback and its `_run_sync` /
      `_run_async` Python path; converting it would mean inventing a per-key
      shape Java's result does not have.
  The **synchronous** entry points for all three converted RPCs are unaffected,
  per the same rule as every prior addendum. This completes the per-key reversal:
  every admin RPC whose Java `*Result` is a genuine per-key `KafkaFuture` map is
  now delivered per key; the RPCs that remain joined (the `list*` /
  `describeCluster` family and `listTransactions`) are exactly those whose Java
  `*Result` exposes a single whole-call future rather than a per-key map.
- **D3 — Slice granularity: seven slices as tabled in §4**, B0 first.
- **D4 — `admin-client.md` §11 and `PLAN.md`'s caveats.** Updating rules files is
  outside the Actor's remit — those changes go through the `agent-roles.md`
  process. **Manager guidance for this milestone:** §11 predates the PR #116
  merge and its premises no longer describe the tree (see §0.1). Implementation
  should mirror the **consumer** FFI/Python patterns and reuse
  `src/ffi/common.rs`'s dispatcher; §11's direction to mirror the synchronous
  producer, and its caution about introducing an async dispatcher, no longer
  apply. If a review raises §11, this section is the reference. The §11 update
  itself is tracked separately and does not block implementation.
