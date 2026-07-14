# Milestone 11: AdminClient (Rust core + C FFI + Python bindings)

## Goal

Translate `org.apache.kafka.clients.admin.Admin` (`KafkaAdminClient`,
`MockAdminClient`) from Java to Rust, then build a C FFI (sync + async) and
Python bindings (sync + async, over the async C API) on top — completing
the third and last major Java client surface after Producer and Consumer.

Java source root: `kafka/clients/src/main/java/org/apache/kafka/clients/admin/`
(production, ~150 files) and
`kafka/clients/src/test/java/org/apache/kafka/clients/admin/` (tests,
`KafkaAdminClientTest.java` is 11,752 lines), at submodule commit
`a18251bae0b825c69794a50dffd4c3100cf5ca5b`.

**Status: plan only, not yet executed** — recorded here "for the moment"
ahead of kickoff so the scope/tiering decisions below are pinned down before
any Actor/Critic work starts.

## Key architecture decision: Admin methods are sync-returning-futures, not async

Every Java `Admin` RPC method (`createTopics`, `deleteTopics`,
`listOffsets`, ...) **returns immediately** with a `*Result` object wrapping
one `KafkaFuture<T>` per key (e.g. one per topic for `createTopics`). The
network I/O happens later on `KafkaAdminClient`'s background thread; the
*caller* decides whether/when to block, via `.get()`/`.all().get()`. Per
CLAUDE.md §9.1 ("if a method is blocking in Java it should be async in
Rust"), these methods are **not** blocking in Java, so they must **not**
become `async fn` in Rust — they stay plain sync functions that enqueue a
`Call`/driver invocation onto the background task and return a result
struct holding one `KafkaFuture<T>`-equivalent handle per key.
`src/common/kafka_future.rs` (already translated from Java's `KafkaFuture`)
is the type to reuse here.

The one exception is `close()`: Java's `close(Duration timeout)` blocks
joining the background thread (CLAUDE.md §9.4: `thread.join()` → must
actually `.await` in Rust), so `close()` is the only `async fn` on the
`Admin` trait.

```rust
pub trait Admin: Send + Sync + 'static {
    fn create_topics(&self, topics: &[NewTopic], options: CreateTopicsOptions) -> CreateTopicsResult;
    fn delete_topics(&self, topics: TopicCollection, options: DeleteTopicsOptions) -> DeleteTopicsResult;
    fn describe_cluster(&self, options: DescribeClusterOptions) -> DescribeClusterResult;
    // ... all other RPCs: sync, return a *Result holding KafkaFuture<T> per key ...

    async fn close(&self, timeout: Duration);   // blocks in Java -> must await in Rust

    fn metrics(&self) -> HashMap<MetricName, Metric>;   // non-blocking accessor stays sync
}
```

This nuance is easy to miss by copying the Consumer trait's
`#[async_trait]`-everything shape (`consumer-threading.md` §2), because the
overall picture (background thread, retries, futures) looks similar. The
distinguishing signal is *where* the blocking happens: Consumer's `poll()`
itself performs I/O and blocks; Java's `Admin.createTopics()` hands work to
a background thread and returns instantly — blocking is opt-in, at the
`KafkaFuture.get()` call site, which in Rust means "the caller awaits/polls
the future when it chooses to," not "the method is `async`."

This will be written down as a new rules file,
`.claude/rules/admin-client.md` (mirroring `consumer-threading.md`'s role
for Consumer), during Phase 1, so every subsequent Actor/Critic loop across
many phases has a durable reference instead of re-deriving it.

## Dispatch engine: preserve Java's two-pattern split

`KafkaAdminClient` uses two distinct mechanisms, both translated per
CLAUDE.md's "preserve original architecture" rule:

1. **`Call`/retry-list** (`internals`: `Call`, `NodeProvider` variants
   `ControllerNodeProvider`/`LeastLoadedNodeProvider`/
   `ConstantNodeIdProvider`) — a simple per-request retry unit with
   `tries`/`deadlineMs`, used for most single-request RPCs (topics,
   configs, log dirs, elections, reassignments).
2. **`AdminApiDriver`/`AdminApiHandler`/`AdminApiLookupStrategy`** — a
   multi-step engine for RPCs needing a coordinator or per-partition-leader
   lookup before the real request can be sent (group/offset/transaction
   RPCs via `CoordinatorStrategy`, `listOffsets`/`describeProducers` via
   `PartitionLeaderStrategy`, broker-list lookups via
   `StaticBrokerStrategy`/`AllBrokersStrategy`).

Both are driven by **one `tokio::spawn`ed background task per `AdminClient`
instance** (mirrors `AdminClientRunnable`, simpler than Consumer's
`RequestManagers` list — Java has one thread, one pending-calls queue, no
separate managers). Reuse `NetworkClient`/`KafkaClient`
(`src/network_client.rs`, `src/kafka_client.rs`) exactly as Producer's
`Sender` and Consumer's `NetworkClientDelegate` already do. A thin
`AdminMetadataManager` (bootstrap + controller/broker list refresh) is the
Admin-specific analog of `ConsumerMetadata`.

## Module layout (new)

```
src/admin/
  mod.rs                     # Admin trait, new_admin_client() factory -> Box<dyn Admin>
  admin_client_config.rs     # AdminClientConfig
  kafka_admin_client.rs      # KafkaAdminClient: owns NetworkClient + bg task handle
  mock_admin_client.rs       # MockAdminClient: in-memory fake, immediately-ready futures
  new_topic.rs, new_partitions.rs, config.rs, config_entry.rs, alter_config_op.rs,
  topic_description.rs, topic_listing.rs, records_to_delete.rs, deleted_records.rs, ...
      # one file per Java POJO, re-exported from admin:: per CLAUDE.md naming conventions
  options/                   # one file per *Options class, re-exported from admin::
  internals/
    call.rs                  # Call, CallsInFlight, NodeProvider variants
    admin_api_driver.rs, admin_api_handler.rs, admin_api_future.rs,
    admin_api_lookup_strategy.rs, coordinator_strategy.rs,
    static_broker_strategy.rs, all_brokers_strategy.rs, partition_leader_strategy.rs,
    coordinator_key.rs, admin_metadata_manager.rs, admin_bootstrap_addresses.rs,
    admin_client_runnable.rs  # the tokio::spawn'ed background task
    <per-RPC>_handler.rs       # only for RPCs using the AdminApiDriver pattern
```

New wire-protocol request wrapper types go in `src/common/requests/` next to
the existing `metadata_request.rs`-style files (generated
`*RequestData`/`*ResponseData` structs already exist for every admin RPC via
`generator/messages/*.json` + `build.rs`, so this is "write the typed
wrapper," not "write the wire format"). Several RPCs reuse wrappers
Producer/Consumer already built: `MetadataRequest` (backs
`listTopics`/`describeTopics`/`describeCluster`), `FindCoordinatorRequest`,
`OffsetFetchRequest`/`OffsetCommitRequest` (back
`listConsumerGroupOffsets`/`alterConsumerGroupOffsets`), `ListOffsetsRequest`.

## Scope: priority tiers

| Tier | RPC groups | Notes |
|---|---|---|
| **0 — Foundation** | `Admin` trait, `AdminClientConfig`, `Call`/retry engine, `AdminApiDriver` engine, background task, `MockAdminClient` shell, `admin-client.md` rules file | Delivered together with Tier 1 Phase 1 — a dispatch engine needs a real RPC to prove itself against |
| **1 — Topics/Partitions/Cluster/Configs** | createTopics, deleteTopics, listTopics, describeTopics, createPartitions, deleteRecords, describeCluster, describeConfigs, incrementalAlterConfigs, alterReplicaLogDirs, describeLogDirs, describeReplicaLogDirs, listConfigResources, electLeaders, alterPartitionReassignments, listPartitionReassignments, listOffsets | Most commonly used surface; first use of `AdminApiDriver`/`PartitionLeaderStrategy` (listOffsets) |
| **2 — Consumer groups & offsets** | describeConsumerGroups, listConsumerGroups, listGroups (unified, filters by type), describeClassicGroups, listConsumerGroupOffsets, alterConsumerGroupOffsets, deleteConsumerGroupOffsets, deleteConsumerGroups, removeMembersFromConsumerGroup | First heavy use of `CoordinatorStrategy` |
| **3 — ACLs, quotas, SCRAM, tokens, transactions, features** | createAcls/describeAcls/deleteAcls, describeClientQuotas/alterClientQuotas, describeUserScramCredentials/alterUserScramCredentials, delegation tokens (create/renew/expire/describe), describeProducers/describeTransactions/abortTransaction/forceTerminateTransaction/listTransactions/fenceProducers, describeFeatures/updateFeatures, listClientMetricsResources | `AclBinding`/`AclBindingFilter`/`ResourcePattern` (Java `common.acl`/`common.resource`) may not exist in the Rust tree yet — verify at Tier 3 kickoff and translate if missing |
| **4 — Deferred, revisit later** | Streams Groups (describeStreamsGroups, list/alter/deleteStreamsGroupOffsets, deleteStreamsGroups), Share Groups / KIP-932 (describeShareGroups, alter/list/deleteShareGroupOffsets, deleteShareGroups), KRaft raft-voter admin (addRaftVoter, removeRaftVoter, describeMetadataQuorum, unregisterBroker) | Streams isn't otherwise translated in this repo; Share Groups are already out of scope on the Consumer side (`consumer-threading.md` §20); raft-voter ops are broker/controller cluster-membership, not typical client usage |

Tests follow the same tiering: `KafkaAdminClientTest.java` (11,752 lines)
and the matching `internals/*HandlerTest.java` files are translated
incrementally, split by which tier/phase owns each RPC — not as one
11k-line file in one sitting. Dedicated files (`NewTopicTest`, `ConfigTest`,
`TopicCollectionTest`, `ScramMechanismTest`, `GroupListingTest`,
`ConsumerGroupDescriptionTest`, `ListGroupsOptionsTest`, per-`*ResultTest`
files, etc.) are each assigned to the tier owning that class. Per
`definition-of-done.md` #3, `@RepeatedTest` becomes loops, error messages
are asserted (not just `is_err()`), and wire types get byte-level encoding
tests.

## Vertical slicing within a tier

Confirmed with the user: vertical, not horizontal. For each RPC group, one
phase goes Rust core + unit tests → C sync FFI → C async FFI → Python sync
→ Python async, end to end, before starting the next group. This differs
from how Producer/Consumer were historically built (fully horizontal by
layer) but is the right call here because Admin's RPC surface is far more
fragmented (~40+ in-scope RPCs vs. Producer's single send path) — a
horizontal pass would mean nothing is usable end-to-end until dozens of
RPCs are done, and binding-layer design problems (e.g. how to shape a
per-key batch callback for `createTopics`) would surface very late.

## Phases (Tier 1 — first to execute)

| # | Phase | Scope | Depends on |
|---|---|---|---|
| 1 | **Foundation + Topics CRUD** | `Admin` trait, dispatch engine (`Call` + `AdminApiDriver`), `MockAdminClient`, `admin-client.md` rules file, `createTopics`/`deleteTopics`/`listTopics`/`describeTopics` — Rust → C sync/async FFI → Python sync/async, `MockAdminClient` exposed through FFI too (mirrors `MockProducer`/`MockConsumer`) | — |
| 2 | **Partitions & records** | `createPartitions`, `deleteRecords` — same vertical (Rust→C→Python, sync+async) | Phase 1 |
| 3 | **Cluster & configs** | `describeCluster`, `describeConfigs`, `incrementalAlterConfigs`, `listConfigResources` | Phase 1 |
| 4 | **Log dirs** | `alterReplicaLogDirs`, `describeLogDirs`, `describeReplicaLogDirs` | Phase 1 |
| 5 | **Elections, reassignment, offsets** | `electLeaders`, `alterPartitionReassignments`, `listPartitionReassignments`, `listOffsets` — first real exercise of `AdminApiDriver` + `PartitionLeaderStrategy` | Phase 1 |

Tier 2 and 3 phase breakdowns will be drawn up the same way at kickoff of
each tier (grouping by shared handler/strategy, e.g. all
`CoordinatorStrategy`-based group RPCs together), rather than fully
enumerated now.

## C FFI conventions to follow (already established by producer/consumer)

- Opaque type `kafka_admin_AdminClient_t`; `kafka_admin_MockAdminClient_t`
  for the mock, matching `kafka_producer_KafkaProducer_t`/
  `kafka_producer_MockProducer_t`.
- Every network-touching method gets **both** a bare blocking name (calls
  `runtime.block_on(...)`, joining all per-key futures — the C equivalent of
  Java's `result.all().get()` convenience) and an `_async` suffixed
  callback-based variant, per existing convention (no separate "sync"
  suffix — the bare name *is* sync).
- Multi-key results (e.g. `createTopics` → one future per topic) use a
  **batch callback** carrying an array of `{key, error}` pairs, following
  the `kafka_producer_KafkaProducer_send_batch_callback_t` precedent named
  in CLAUDE.md's C FFI conventions section.
- Async completions are dispatched through the existing shared
  `CompletionJob`/dispatcher-thread machinery in `src/ffi/common.rs` — reuse
  `spawn_dispatcher` and the `sync_void_op`/`async_void_op`/`async_value_op`
  helper shapes from `src/ffi/consumer.rs`, add `admin`-specific ones only
  where the shape genuinely differs (batch-keyed results).
- New file `src/ffi/admin.rs`, registered in `src/ffi/mod.rs`, gated by the
  existing `ffi` feature.
- `cbindgen.toml`'s `[export] include` allow-list gets each new opaque
  type/callback typedef name appended per phase, or it won't appear in the
  generated header.
- C tests: `bindings/c/tests/test_kafka_admin.c` / `test_mock_admin.c`,
  Unity framework, wired into `bindings/c/CMakeLists.txt` the same way as
  the existing `test_kafka_producer.c`/`test_mock_producer.c`.

## Python bindings conventions to follow

- Not PyO3 — Python binds through the hand-written C extension
  (`bindings/python/_confluentkafka.c`) exactly as producer/consumer do.
  New `admin.py` with `_AdminBase` (shared state/validation),
  `AdminClient`/`MockAdminClient` (sync) and `AsyncAdminClient`/
  `AsyncMockAdminClient` (asyncio-native), mirroring `producer.py`'s
  `_ProducerBase`/`Producer`/`AsyncProducer` split.
- Confirmed pattern to reuse as-is: **both** sync and async Python classes
  call the *same* `kafka_admin_AdminClient_*_async` C entry points — they
  only differ in how they wait for the callback (`_run_sync` blocks a
  `threading.Event`; `_run_async` bridges to an `asyncio.Future` via
  `loop.call_soon_threadsafe`). This is the "Python bindings using the
  async C API" the user asked for, and the existing `_run_sync`/`_run_async`
  helpers in `producer.py`/`consumer.py` can likely be reused near-verbatim
  rather than reinvented for Admin.
- New `_confluentkafka.c` glue functions per method, registered in a new
  `AdminNativeMethods` table, following the existing
  `kafka_admin_AdminClient_X_async` → `py_AdminClient_X_async` →
  `"AdminClient_X_async"` naming chain.
- Tests: `bindings/python/test/unit/test_admin.py`, pytest, matching
  `test_producer.py`/`test_consumer.py` conventions
  (`asyncio_mode = "auto"`).
- Multilanguage/gRPC harness wiring (`multilanguage-test-server/`) is
  explicitly **out of scope** for this milestone — historically that was
  wired in last, well after both C and Python bindings were stable, for
  producer and consumer alike; revisit as a later milestone if requested.

## Definition of Done per phase

Full `.claude/rules/definition-of-done.md` checklist applies, with one
adjustment: **item #10 (hot-path allocation audit) does not apply** —
`AdminClient` calls are batch/administrative, not per-record, so there is
no hot-path allocation budget to audit (note this explicitly in each
phase's self-review rather than silently skipping it). Item #11 (Consumer
trait surface check) does not apply to Admin's trait (see the
sync-vs-async design decision above) but the same *spirit* applies: verify
`Admin`'s per-RPC methods stay plain `fn`, only `close()` is `async fn`,
and no `#[async_trait]` bleeds into anything Admin-adjacent that doesn't
need it (e.g. the internal `Call`/driver types, which are plain structs
driven by the background task, not traits).

## Execution cadence

Per `.claude/rules/agent-roles.md`, executed as Manager coordinating
Actor/Critic pairs, one pair per phase: spawn `actor-executor` to
implement, then `kafka-critic` to review; iterate via
`COMMENTS.<N>.md`/`COMMENTS.DONE.<N>.md` until clean; commit; move to the
next phase. Runs under agent number **N=1** (Actor/Critic 0 was already
used for the earlier message-layer work per project memory).

Given the size of this effort (5 phases in Tier 1 alone, 3 more tiers to
follow), execution proceeds **one phase at a time with a check-in after
each phase** (build/test/lint results + what was translated) rather than
running the whole multi-tier plan unsupervised. Tier boundaries are natural
pause points to confirm the next tier's phase breakdown before continuing.

## Verification

Per phase: `cargo build`, `cargo test` (targeted to the new module plus
full suite before closing the phase), `cargo xtask lint`, `cargo xtask
format-check`; for FFI phases additionally the CMake/CTest Unity suite in
`bindings/c/`; for Python phases additionally `pytest
bindings/python/test/unit/test_admin.py`. Before closing each *tier*, run
the full `make verify` (per CLAUDE.md's `Development Workflow` and DoD
item #9) to catch cross-cutting regressions in producer/consumer from
shared-code changes (e.g. `KafkaError`, `NetworkClient`, `kafka_future.rs`).
