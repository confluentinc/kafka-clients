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
  | one `KafkaFuture<Void>` for the whole call, and nothing else on the Java result | **no result handle at all** — success is a null return / a null `error` in the callback, following `kafka_admin_AdminClient_close_async`'s callback shape |

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

  **The void-result row (added for B6.)** `abortTransaction` and
  `forceTerminateTransaction` are the only two of the 46 RPCs whose Java result
  carries no data at all: `AbortTransactionResult` exposes exactly one method,
  `all() -> KafkaFuture<Void>`, and `TerminateTransactionResult` exposes
  `result() -> KafkaFuture<Void>`. Neither exposes per-key granularity a caller
  could reach either — the abort result's per-partition map is private and the
  RPC takes exactly one spec, so there is one key by construction. "One opaque
  result handle per RPC" exists to carry per-key data and errors across a
  boundary with no `KafkaFuture`; with nothing to carry, a handle whose only
  method is `_destroy` is ceremony plus a leak to get wrong. The precedent for
  the shape is already in the module: `close_async`'s error-only callback and
  `admin_sync_value_op` with `T = ()`. Note this is *not* the same as B5b's
  decision to give the two `KafkaFuture<Long>` results a handle each — there the
  future has a value to deliver.

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
