# Milestone 11: AdminClient (Rust core translation)

> ## SCOPE FOR THIS TASK: Rust core + tests ONLY — bindings deferred
>
> **Confirmed by the user (2026-07-15):** this task implements the Admin
> client in the Rust client **only** — translating from Java, plus unit
> tests and real-broker integration tests, and verifying they work. **No C
> FFI and no Python bindings are built in this task, for any phase across
> Tiers 1–3.** Every phase's deliverable is **Rust core + unit tests +
> integration tests**; the FFI/Python "vertical slice" is dropped for this
> task.
>
> The C FFI conventions and Python bindings conventions sections below are
> **retained as design reference for a separate future bindings task/
> milestone** — do not delete that design work, but its **execution is
> deferred**. When that future task starts, the async C dispatcher it needs
> already exists (unmerged) in **PR #116 "C and python consumer bindings"
> (branch `dev/c_and_python_consumer_bindings`, `src/ffi/common.rs`)** —
> reuse it, do not reinvent it. The bindings-slice A/B/C grouping below also
> applies only to that future task.

> ## PROGRESS STATUS (paused here — resume anytime)
>
> **Tier 1 (all 5 phases) is COMPLETE** as of 2026-07-17: Topics CRUD,
> Partitions & records, Cluster & configs, Log dirs, and Elections/
> reassignment/offsets are all implemented, unit-tested, real-broker-
> integration-tested, and Critic-clean. See `design/history/Milestone-11/
> Phase-{1,2,3,4,5}/` for each phase's archived review record, and
> `design/current/status.md` for the current translated-surface summary.
>
> **RESUMED 2026-07-29.** Tier 2 Phase 1 ("Group listing & describe") is now
> COMPLETE: `list_groups`, `list_consumer_groups`, `describe_consumer_groups`
> (dual-protocol), `describe_classic_groups` — Rust core + unit tests +
> real-broker integration tests, Critic-clean after one fix cycle. First real
> use of `CoordinatorStrategy`; landed the `ConsumerProtocol` /
> `consumer-threading.md` §20 carve-out prerequisite. Review record archived at
> `design/history/Milestone-11/Tier2-Phase-1/COMMENTS.DONE.1.md`;
> `design/current/status.md` has the surface summary.
>
> **Tier 2 Phase 2 "Group offsets" is now COMPLETE (2026-07-29):**
> `list_consumer_group_offsets`, `alter_consumer_group_offsets`,
> `delete_consumer_group_offsets` — Rust core + unit tests + real-broker
> integration tests, Critic-clean after one fix cycle. Note: the PLAN's
> `deleteConsumerGroupOffsets` "-1 OffsetCommit sentinel" claim was WRONG —
> Kafka 4.2 uses a dedicated `OffsetDelete` RPC (apiKey 47, v0), which was
> added as net-new wire work. Review record archived at
> `design/history/Milestone-11/Tier2-Phase-2/COMMENTS.DONE.1.md`.
>
> **Tier 2 Phase 3 "Group / member deletion" is now COMPLETE (2026-07-29):**
> `delete_consumer_groups`, `remove_members_from_consumer_group` (specific
> members or `remove_all`) — Rust core + unit tests + real-broker integration
> tests, Critic-clean after one fix cycle. Net-new `DeleteGroups`/`LeaveGroup`
> wire wrappers; `remove_all` describes-then-leaves with Java-faithful deadline
> handling. Review record at
> `design/history/Milestone-11/Tier2-Phase-3/COMMENTS.DONE.1.md`.
>
> **TIER 2 IS COMPLETE (all 3 phases).**
>
> **Tier 3 Phase 1 "ACLs" is COMPLETE (2026-07-29):** `create_acls`,
> `describe_acls`, `delete_acls` + all `common.acl`/`common.resource`
> primitives + the authorizer-enabled broker fixture (finding #11) — Rust core
> + unit tests + real-broker integration tests, Critic-clean after one fix
> cycle. Reused the Tier-2 `AclOperation`/`AclPermissionType`/`from_32_bit_field`.
> Review record at `design/history/Milestone-11/Tier3-Phase-1/COMMENTS.DONE.1.md`.
>
> **Tier 3 Phase 2 "Client quotas" is COMPLETE (2026-07-29):**
> `describe_client_quotas`, `alter_client_quotas` + `common.quota` primitives —
> Rust core + unit tests + real-broker integration tests, Critic-clean after one
> fix cycle. `ClientQuotaEntity` models the nullable/default entity name
> faithfully. Review record at
> `design/history/Milestone-11/Tier3-Phase-2/COMMENTS.DONE.1.md`.
>
> **Tier 3 Phase 3 "SCRAM credentials" is DEFERRED (2026-07-29)** — needs a new
> PBKDF2 crypto crate (`pbkdf2`+`hmac`+`sha2`, or `ring`) for `ScramFormatter.hi()`
> (finding #4), changing `Cargo.toml`. Per CLAUDE.md §1.2 the user must approve
> the dependency first; "continue the implementation" was NOT explicit approval.
> Per the user's direction, Phases 4–7 (no new crate) run FIRST; SCRAM resumes
> only on an explicit relayed crate approval — do not touch `Cargo.toml` until then.
>
> **Tier 3 Phase 4 "Delegation tokens" is COMPLETE (2026-07-29):**
> `create/renew/expire/describe_delegation_token` + `KafkaPrincipal`/
> `DelegationToken`/`TokenInformation` — Rust core + unit tests (against the mock,
> per finding #10), Critic-CLEAN on first pass (no fix cycle). Integration
> deferred (admin client has no SASL support yet — a separate feature). Review
> record at `design/history/Milestone-11/Tier3-Phase-4/COMMENTS.DONE.1.md`.
>
> **Tier 3 Phase 5 "Features" is COMPLETE (2026-07-29):** `describe_features`
> (reuses `ApiVersions`), `update_features` (net-new `UpdateFeatures` wire) +
> `FeatureMetadata`/`FeatureUpdate`/version-range POJOs — Rust core + unit tests
> (full 1:1 parity) + real-broker integration, Critic-CLEAN on first pass. Mock
> has real version-bounds validation (finding #9). Review record at
> `design/history/Milestone-11/Tier3-Phase-5/COMMENTS.DONE.1.md`.
>
> **Tier 3 Phase 6 "Producers & transactions" is COMPLETE (2026-07-29):**
> `describe_producers`, `abort_transaction`, `describe_transactions`,
> `fence_producers`, `list_transactions`, `force_terminate_transaction` —
> Rust core + unit tests (byte-level wire vectors, parameterized→loops) +
> real-broker integration, Critic-CLEAN on first pass (no fix cycle). First
> real use of `StaticBrokerStrategy` and `AllBrokersStrategy` (dynamic key
> discovery). **Design-gap verdict:** `AllBrokersStrategy` FITS the Tier-1
> `AdminApiLookupStrategy`/`LookupResult`/`AdminApiDriver` cleanly with NO
> foundation changes — dynamic per-broker keys flow via `completed_keys`
> (sentinel) + `mapped_keys` (new broker ids). `CoordinatorStrategy` reused
> generic over `CoordinatorType::Transaction` (not GROUP-hardcoded), confirmed
> by Critic. 2984 lib tests pass. Documented skips (both Critic-confirmed
> legitimate): `ListTransactionsHandlerTest.testBuildRequestWithDurationFilter`
> case 3 (generator omits below-min-version fields rather than throwing
> `UnsupportedVersion` — pre-existing generator-wide behavior, worth a separate
> future ticket), and the ongoing-transaction integration scenarios (no
> transactional producer API exists in the Rust `Producer` trait yet — kept as
> compiling `#[ignore]` skeletons). Review record at
> `design/history/Milestone-11/Tier3-Phase-6/`.
>
> **Next: Tier 3 Phase 7 "Client metrics"** (`listClientMetricsResources` —
> small; reuses the Tier-1 `ListConfigResources` wire path filtered to
> `CLIENT_METRICS`, zero new wire types). SCRAM (Phase 3) revisited only on an
> explicit relayed crate approval. Tier 4 remains out of scope.
>
> (Historical note: an earlier 2026-07-17 pause said "zero uncommitted Tier 2
> work" — that was superseded; a prior Actor session had in fact landed
> substantial Tier 2 Phase 1 work that was finished and committed on resume.)
>
> One thing to re-confirm at resume time, since it may have changed by
> then: whether PR #116 (`dev/c_and_python_consumer_bindings`) has merged to
> `master`, since a couple of Tier 1 findings referenced it (the async
> dispatcher for the future bindings task). Does not block Tier 2/3
> Rust-core work either way.

## Goal

Translate `org.apache.kafka.clients.admin.Admin` (`KafkaAdminClient`,
`MockAdminClient`) from Java to Rust, with unit tests and real-broker
integration tests — completing the third major Java client surface after
Producer and Consumer. (C FFI + Python bindings are a separate future task,
see the scope banner above.)

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

> **Deferred for this task (see scope banner):** the full vertical slice
> below (Rust → C → Python) describes the *eventual* end state. For THIS
> task, each phase stops at **Rust core + unit tests + integration tests**.
> The C/Python portions of every slice are deferred to the future bindings
> task.

Confirmed with the user: vertical, not horizontal. For each RPC group, one
phase goes Rust core + unit tests → C sync FFI → C async FFI → Python sync
→ Python async, end to end, before starting the next group. This differs
from how Producer/Consumer were historically built (fully horizontal by
layer) but is the right call here because Admin's RPC surface is far more
fragmented (~40+ in-scope RPCs vs. Producer's single send path) — a
horizontal pass would mean nothing is usable end-to-end until dozens of
RPCs are done, and binding-layer design problems (e.g. how to shape a
per-key batch callback for `createTopics`) would surface very late.

### Amendment (Tier 2/3): coarser bindings slices than Rust-core phases

> **Deferred for this task (see scope banner).** This bindings-slice grouping
> applies to the future bindings task only; no bindings are built now.

The literal "one phase = one vertical slice" rule holds for Tier 1, but
several Tier 2/3 phases are too small (1–4 methods, no shared handler) to
justify a full C-header + Python-module diff each — the bindings ceremony
would dwarf the translation work, and five near-identical tiny bindings PRs
add no user value. **Keep the Rust-core + unit-test granularity exactly as
phased below** (one Actor/Critic loop per phase — that is where
behavioral-parity review matters most and smaller units review better). For
the **C-FFI/Python bindings layer only**, merge into three bindings-slices:

- **Bindings-slice A** = Tier 2 Phases 1–3 combined (all consumer-group /
  offset RPCs). The natural "group administration" boundary users think in.
- **Bindings-slice B** = Tier 3 Phases 1–5 combined (ACLs, quotas, SCRAM,
  delegation tokens, features) — all small, independent, low-call-volume
  admin surfaces.
- **Bindings-slice C** = Tier 3 Phases 6–7 combined (transactions/producers
  + `listClientMetricsResources`) — transactions is the one Tier 3 area with
  real per-key batch-result shapes (`fenceProducers` returns
  `ProducerIdAndEpoch` per id, `describeProducers` per-partition) worth its
  own bindings attention; the one-method client-metrics RPC folds in rather
  than getting a standalone PR.

Each bindings-slice still runs sync-then-async, C-then-Python. Only the
*grouping* of which Rust-core phases feed one bindings pass changes. This is
a deviation from the letter (not the spirit) of the vertical-slicing
decision — confirm with the user before Tier 2 kickoff.

## Phases (Tier 1 — first to execute)

| # | Phase | Scope | Depends on |
|---|---|---|---|
| 1 | **Foundation + Topics CRUD** | `Admin` trait, dispatch engine (`Call` + `AdminApiDriver`), `MockAdminClient`, `admin-client.md` rules file, `createTopics`/`deleteTopics`/`listTopics`/`describeTopics` — Rust core + unit tests + integration tests (FFI/Python deferred, see scope banner) | — |
| 2 | **Partitions & records** | `createPartitions`, `deleteRecords` — same vertical (Rust→C→Python, sync+async) | Phase 1 |
| 3 | **Cluster & configs** | `describeCluster`, `describeConfigs`, `incrementalAlterConfigs`, `listConfigResources` | Phase 1 |
| 4 | **Log dirs** | `alterReplicaLogDirs`, `describeLogDirs`, `describeReplicaLogDirs` | Phase 1 |
| 5 | **Elections, reassignment, offsets** | `electLeaders`, `alterPartitionReassignments`, `listPartitionReassignments`, `listOffsets` — first real exercise of `AdminApiDriver` + `PartitionLeaderStrategy` | Phase 1 |

Tier 2 and Tier 3 phase breakdowns follow (grouped by shared
handler/strategy). Tier 4 remains deferred, unchanged.

## Cross-cutting prerequisites & findings (Tier 2/3)

Read before the Tier 2/3 phase tables — these are dependencies and risks
that cut across multiple phases, confirmed by reading the Java source
(`KafkaAdminClient.java`, `MockAdminClient.java`, the `internals/*Handler`
and `*Strategy` files) directly.

1. **`AclOperation`/`AclPermissionType` are a Tier 1 dependency, not just
   Tier 3.** `DescribeClusterResult` and `TopicDescription` (both Tier 1)
   return `Set<AclOperation>` for `authorizedOperations`, and
   `KafkaAdminClient` calls `AdminUtils.validAclOperations` for those Tier 1
   RPCs. `AdminUtils.validAclOperations` needs `Utils.from32BitField`
   (`common/utils/Utils.java:1385`), absent from `src/common/utils/`.
   **Tier 1 kickoff action:** land `common::acl::AclOperation` /
   `AclPermissionType` (the two enums only, not `AclBinding` /
   `AclBindingFilter`) and `common::utils::from_32_bit_field` in Tier 1. If
   Tier 1 shipped without them, the first Tier 2 phase must add them.

2. **`GroupState`, `GroupType`, `ClassicGroupState`** (`org.apache.kafka.common.*`,
   not `admin.*`) are small enums that do not exist in `src/common/` yet
   (`common/GroupState.java`, `GroupType.java`, `ClassicGroupState.java`).
   Needed from Tier 2 Phase 1 (`listGroups`, `describeConsumerGroups`); add
   them there if Tier 1 did not.

3. **`ConsumerProtocol` is a hard dependency for `describeConsumerGroups`'
   classic-group fallback and `describeClassicGroups`** — and requires an
   explicit amendment to `consumer-threading.md` §20. Both
   `DescribeConsumerGroupsHandler.handledClassicGroupResponse` and
   `DescribeClassicGroupsHandler.handleResponse` call
   `ConsumerProtocol.deserializeAssignment(...)` to parse a classic member's
   raw assignment bytes into `Set<TopicPartition>`. §20 currently lists
   `ConsumerProtocol` as out-of-scope classic-assignor machinery — that
   blanket exclusion is wrong for Admin. The generated wire structs it needs
   (`ConsumerProtocolAssignment` / `ConsumerProtocolSubscription`) already
   exist under `generator/messages/`. **Recommendation:** translate the full
   `ConsumerProtocol` class (all methods per DoD #2) into
   `src/consumer/internals/consumer_protocol.rs`, plus the two
   `ConsumerPartitionAssignor.{Assignment,Subscription}` data holders (NOT
   the assignor trait, which stays out of scope), and import them from the
   Admin handlers. This is a required, documented amendment to §20 — call it
   out in the phase self-review and add a short §20 carve-out note; not a
   silent addition.

4. **SCRAM salted-password hashing (`alterUserScramCredentials`) needs a new
   crypto crate — ASK BEFORE ADDING (CLAUDE.md §1.2).**
   `KafkaAdminClient.getSaltedPassword` calls `ScramFormatter.hi(password,
   salt, iterations)` = real PBKDF2 (RFC 5802 `Hi`) via
   `PBKDF2WithHmacSHA256`/`SHA512`. Nothing under `common.security.scram`
   exists. Candidate crates: `pbkdf2` + `hmac` + `sha2`, or `ring`. Scope is
   narrow: only `ScramFormatter.hi()` and the internal `ScramMechanism`
   name→algorithm mapping — not a full SASL/SCRAM client. Resolve the crate
   decision **before** starting Rust-core work in Tier 3 Phase 3 (it changes
   `Cargo.toml`).

5. **`KafkaPrincipal`, `DelegationToken`, `TokenInformation`** (all outside
   `admin/`) don't exist and are required for the delegation-token RPC group
   (`common/security/auth/KafkaPrincipal.java`,
   `common/security/token/delegation/{DelegationToken,TokenInformation}.java`).
   All self-contained POJOs.

6. **`common::utils::ProducerIdAndEpoch` doesn't exist.**
   `FenceProducersResult` is keyed by it. The Producer module uses raw
   `(i64, i16)` inline, never the standalone POJO. Tier 3 Phase 6 is the
   first consumer of the real type — translate
   `common/utils/ProducerIdAndEpoch.java` into
   `src/common/utils/producer_id_and_epoch.rs`.

7. **`common.acl` / `common.resource` / `common.quota` types are confirmed
   absent** (~1900 lines of small Java across 8 acl/resource + 4 quota
   files): `acl/{AclBinding, AclBindingFilter, AclOperation,
   AclPermissionType, AccessControlEntry, AccessControlEntryFilter,
   AccessControlEntryData}`, `resource/{Resource, ResourcePattern,
   ResourcePatternFilter, PatternType, ResourceType}`,
   `quota/{ClientQuotaEntity, ClientQuotaFilter, ClientQuotaFilterComponent,
   ClientQuotaAlteration}`. Admin's own `ScramMechanism` /
   `ScramCredentialInfo` (under `admin/`) are self-contained and separate
   from finding #4's crypto class.

8. **`ForwardingAdmin.java`** is a broker-plugin delegate (envelope
   forwarding for broker-side-originated `Admin` calls), not a surface
   applications construct. **Add to the out-of-scope list alongside Tier 4**
   (broker/controller-side, not typical client usage).

9. **Java's own `MockAdminClient` is intentionally incomplete for much of
   Tier 3 — mirror it exactly, do not "fix" it.** These methods
   `throw UnsupportedOperationException("Not implemented yet")` in Java:
   `describeClientQuotas`, `alterClientQuotas`, `describeUserScramCredentials`,
   `alterUserScramCredentials`, `describeProducers`, `describeTransactions`,
   `abortTransaction`, `forceTerminateTransaction`, `listTransactions`,
   `fenceProducers`. **Fully implemented** in the Java mock (translate the
   real logic): `describeFeatures`/`updateFeatures` (real upgrade/downgrade
   validation), `listClientMetricsResources`,
   `createDelegationToken`/`renewDelegationToken`/`expireDelegationToken`/
   `describeDelegationToken` (in-memory token list, `KafkaPrincipal.USER_TYPE`
   validation). For the unsupported methods the Rust mock returns a
   `KafkaError` "unsupported" variant (NOT a panic — CLAUDE.md §10.1), as an
   explicit documented deviation, not invented mock behavior.

10. **Delegation tokens have zero client-side unit-test coverage in Java** —
    no `KafkaAdminClientTest` cases, nothing across `clients/src/test/java`.
    Tier 3 Phase 4 will have thinner-than-usual test parity; compensate with
    our own unit tests written against `MockAdminClient`'s real delegation
    implementation as the behavioral reference, and lean on integration
    tests. Note this explicitly per DoD #3.

11. **ACL integration tests need a new authorizer-enabled broker fixture.**
    `tests/common/kafka_cluster.rs` / `cluster_config.rs` have no authorizer
    config; ACL RPCs are no-ops without `authorizer.class.name`. Add a new
    `ClusterConfig` variant (mirroring the KIP-848 property-override pattern
    in `consumer_test.rs`):
    `KAFKA_AUTHORIZER_CLASS_NAME=org.apache.kafka.metadata.authorizer.StandardAuthorizer`
    + `KAFKA_SUPER_USERS=User:ANONYMOUS` so ACL checks don't lock the test
    client out. Needed before Tier 3 Phase 1's integration tests.

## Phases (Tier 2 — Consumer groups & offsets)

Dispatch-mechanism ground truth (from `KafkaAdminClient.java`):

| RPC | Mechanism | Lookup strategy |
|---|---|---|
| `listGroups` | plain `Call` (broker enumeration via `LeastLoadedNodeProvider` metadata fetch + one `Call` per broker with `ConstantNodeIdProvider`) | none — hand-rolled, **not** `AllBrokersStrategy` |
| `listConsumerGroups` (deprecated) | same hand-rolled broker-enumeration `Call` pattern | none |
| `describeConsumerGroups` | `AdminApiDriver` | `CoordinatorStrategy(GROUP)` |
| `describeClassicGroups` | `AdminApiDriver` (`Batched`) | `CoordinatorStrategy(GROUP)` |
| `listConsumerGroupOffsets` | `AdminApiDriver` | `CoordinatorStrategy(GROUP)` |
| `alterConsumerGroupOffsets` | `AdminApiDriver` | `CoordinatorStrategy(GROUP)` |
| `deleteConsumerGroupOffsets` | `AdminApiDriver` | `CoordinatorStrategy(GROUP)` |
| `deleteConsumerGroups` | `AdminApiDriver` (`DeleteGroupsHandler` base + thin `DeleteConsumerGroupsHandler` subclass) | `CoordinatorStrategy(GROUP)` |
| `removeMembersFromConsumerGroup` | `AdminApiDriver` (`Batched`) | `CoordinatorStrategy(GROUP)` |

Note: `listGroups`/`listConsumerGroups` do **not** exercise
`AllBrokersStrategy` (its first real use is Tier 3 `listTransactions`) —
don't plan them assuming shared infra.

| # | Phase | Scope | Depends on |
|---|---|---|---|
| 1 | **Group listing & describe** | `listGroups`, `listConsumerGroups` (deprecated), `describeConsumerGroups`, `describeClassicGroups` — first use of `CoordinatorStrategy` and the shared broker-enumeration `Call` idiom | Tier 1 foundation |
| 2 | **Group offsets** | `listConsumerGroupOffsets`, `alterConsumerGroupOffsets`, `deleteConsumerGroupOffsets` — all `CoordinatorStrategy(GROUP)` on reused `OffsetFetch`/`OffsetCommit` wire types | T2 P1, Tier 1 |
| 3 | **Group / member deletion** | `deleteConsumerGroups`, `removeMembersFromConsumerGroup` | T2 P1, Tier 1 |

### Tier 2, Phase 1 — Group listing & describe

Grouped because all four are the first real exercise of `CoordinatorStrategy`
(the describe pair) and the shared broker-enumeration `Call` idiom (the list
pair), and the describe pair share the `ConsumerProtocol`/`AdminUtils`
prerequisites (finding #3).

- **RPCs:** `listGroups(ListGroupsOptions)` (unified, filter by
  type/state/protocol); `listConsumerGroups(ListConsumerGroupsOptions)`
  (deprecated since 4.1, consumer-groups-only); `describeConsumerGroups(...)`
  (tries `ConsumerGroupDescribeRequest` KIP-848 first, falls back per-group
  to `DescribeGroupsRequest` on `UNSUPPORTED_VERSION`/`GROUP_ID_NOT_FOUND`);
  `describeClassicGroups(...)` (always `DescribeGroupsRequest`).
- **Java main sources:** `KafkaAdminClient.java` (`listGroups` ~3467,
  `listConsumerGroups` ~3631, `describeConsumerGroups` ~3574,
  `describeClassicGroups` ~3847). Options/Result:
  `List{Groups,ConsumerGroups}{Options,Result}`,
  `Describe{Consumer,Classic}Groups{Options,Result}`. Data POJOs:
  `GroupListing`, `ConsumerGroupListing`, `ConsumerGroupDescription`,
  `ClassicGroupDescription`, `MemberDescription`, `MemberAssignment`.
  `internals/DescribeConsumerGroupsHandler.java` (392 lines, dual-protocol),
  `internals/DescribeClassicGroupsHandler.java` (193),
  `internals/CoordinatorStrategy.java`, `internals/CoordinatorKey.java`,
  `internals/AdminUtils.java` (`validAclOperations`). Prerequisites:
  `ConsumerProtocol` + `ConsumerPartitionAssignor.{Assignment,Subscription}`
  (finding #3); `common/{acl/AclOperation, GroupState, GroupType,
  ClassicGroupState}`, `Utils.from32BitField` (findings #1/#2). Wire (net
  new): `ListGroupsRequest`/`Response`,
  `ConsumerGroupDescribeRequest`/`Response`, `DescribeGroupsRequest`/`Response`
  (`MetadataRequest` reused from Tier 1).
- **Java tests:** `KafkaAdminClientTest` slices — `testListGroups*` (7),
  `testListConsumerGroups*` (incl. `...Deprecated*`, ~13),
  `testDescribeConsumerGroup*`/`testDescribe{Multiple,NonConsumer,OldAndNew}*`/
  `testDescribeGroupsWithBothUnsupportedApis` (verify
  `testDescribeConsumerGroupConfigs` isn't misfiled configs coverage),
  `testDescribeClassicGroups*` (3). Dedicated:
  `ListGroupsOptionsTest`, `ListConsumerGroupsOptionsTest`,
  `GroupListingTest`, `ConsumerGroupListingTest`,
  `ConsumerGroupDescriptionTest`, `MemberDescriptionTest`.
  `internals/DescribeConsumerGroupsHandlerTest`,
  `internals/CoordinatorStrategyTest` (foundational — translate once here,
  first real caller). Verify whether Tier 1 already translated
  `internals/AdminApiDriverTest`; if so, add only new `CoordinatorStrategy`
  cases, don't retranslate.
- **Rust modules (`src/admin/`):** `group_listing.rs`,
  `consumer_group_listing.rs`, `consumer_group_description.rs`,
  `classic_group_description.rs`, `member_description.rs`,
  `member_assignment.rs`; `options/{list_groups, list_consumer_groups,
  describe_consumer_groups, describe_classic_groups}_options.rs`;
  `{list_groups, list_consumer_groups, describe_consumer_groups,
  describe_classic_groups}_result.rs`;
  `internals/{describe_consumer_groups_handler,
  describe_classic_groups_handler, coordinator_strategy, coordinator_key,
  admin_utils}.rs`. `src/common/{group_state, group_type,
  classic_group_state, acl/acl_operation}.rs` (only the enum, re-exported as
  `common::acl::AclOperation`; rest of `common::acl` arrives in T3 P1).
  `src/consumer/internals/consumer_protocol.rs` + the `Assignment`/
  `Subscription` structs (likely a new
  `src/consumer/consumer_partition_assignor.rs`, with a comment that the
  assignor trait stays out of scope). `src/common/requests/{list_groups,
  consumer_group_describe, describe_groups}_{request,response}.rs`.
- **Test plan.** *Unit:* mirror the Java list 1:1; re-verify
  `@RepeatedTest`→loop at translation time; assert exact error messages on
  every `handleGroupError` branch; byte-level wire tests (not just
  round-trip) for all three new request/response types. Explicit
  classic-fallback state-machine test: mock `UNSUPPORTED_VERSION` on
  `ConsumerGroupDescribeRequest`, assert the retry uses `DescribeGroupsRequest`
  and that the more-informative `ConsumerGroupDescribe` error message is
  preserved if the classic retry also returns `GROUP_ID_NOT_FOUND` (mirrors
  `groupIdNotFoundErrorMessages`). *Integration:* new
  `tests/integration/admin_groups_test.rs` against a real 4.2.0 broker
  (reuse `cluster_config_with_kip848()`): start real KIP-848
  `AsyncKafkaConsumer`s, then (a) `list_groups`/`list_consumer_groups` assert
  IDs appear with `GroupType::Consumer`/`GroupState::Stable`; (b)
  `describe_consumer_groups` on a live group asserts member count + assignment
  partitions; (c) describe a nonexistent id → `GROUP_ID_NOT_FOUND`; (d)
  classic-fallback path only if the codebase can create a classic consumer —
  else note it's unit-testable only.
- **Dependencies:** Tier 1 foundation. No dependency on other Tier 2 phases.

### Tier 2, Phase 2 — Group offsets

All three are `CoordinatorStrategy(GROUP)` handlers built on reused
`OffsetFetchRequest`/`OffsetCommitRequest` — "wire reused request builders
into the Admin driver," not "build new wire types."

- **RPCs:** `listConsumerGroupOffsets(Map<String, ListConsumerGroupOffsetsSpec>,
  ...)` (batched multi-group `OffsetFetch` with per-group fallback);
  `alterConsumerGroupOffsets(String, Map<TopicPartition, OffsetAndMetadata>,
  ...)`; `deleteConsumerGroupOffsets(String, Set<TopicPartition>, ...)`.
- **Java main sources:** `KafkaAdminClient.java` (~3732 / ~4237 / ~3774).
  `{List,Alter,Delete}ConsumerGroupOffsets{Options,Result}`,
  `ListConsumerGroupOffsetsSpec`.
  `internals/{ListConsumerGroupOffsets, AlterConsumerGroupOffsets,
  DeleteConsumerGroupOffsets}Handler.java`. Reused wire:
  `OffsetFetchRequest`/`Response`, `OffsetCommitRequest`/`Response`.
  **Confirm at translation time:** `DeleteConsumerGroupOffsetsHandler` uses
  `OffsetCommitRequest` with a `-1` offset sentinel to signal deletion (a
  real deviation from a naive "delete = separate RPC" assumption).
- **Java tests:** `KafkaAdminClientTest` — `testOffsetCommit*` (3);
  `testListConsumerGroupOffsets*` incl. `testBatchedListConsumerGroupOffsets*`
  (the batching-fallback variants); `testAlterConsumerGroupOffsets*` (incl.
  FindCoordinator retriable/non-retriable);
  `testDeleteConsumerGroupOffsets*`. Dedicated:
  `DeleteConsumerGroupOffsetsResultTest`.
  `internals/{ListConsumerGroupOffsets, AlterConsumerGroupOffsets,
  DeleteConsumerGroupOffsets}HandlerTest`.
- **Rust modules:** `src/admin/options/{list_consumer_group_offsets,
  alter_consumer_group_offsets, delete_consumer_group_offsets}_options.rs`;
  `src/admin/{list_consumer_group_offsets_result,
  alter_consumer_group_offsets_result, delete_consumer_group_offsets_result,
  list_consumer_group_offsets_spec}.rs`;
  `src/admin/internals/{list_consumer_group_offsets,
  alter_consumer_group_offsets, delete_consumer_group_offsets}_handler.rs`.
- **Test plan.** *Unit:* direct translation; special attention to
  `testBatchedListConsumerGroupOffsetsWithNo{FindCoordinator,OffsetFetch}Batching`
  — these exercise `CoordinatorStrategy.disableBatch()`
  (`AdminApiDriver.java:278`), easy to under-test if the Rust
  `CoordinatorStrategy` doesn't carry a mutable "batching disabled" flag.
  Assert exact retriable-vs-non-retriable error messages
  (`COORDINATOR_LOAD_IN_PROGRESS` retry vs `GROUP_AUTHORIZATION_FAILED`
  fail). Confirm at kickoff whether Consumer's existing `OffsetFetch`/
  `OffsetCommit` wire tests cover the exact field combos Admin uses
  (`requireStable`, group-level vs top-level errors) or Admin needs its own.
  *Integration:* new `tests/integration/admin_group_offsets_test.rs` — (a)
  consume+commit, `list_consumer_group_offsets` matches; (b) stop consumer,
  `alter_consumer_group_offsets`, restart, assert resume from altered offset;
  (c) `delete_consumer_group_offsets` on inactive group, assert gone; (d)
  alter/delete on an active group asserts the expected error (verify exact
  behavior against the real broker).
- **Dependencies:** Tier 2 Phase 1 (`CoordinatorStrategy` infra), Tier 1.

### Tier 2, Phase 3 — Group / member deletion

Both are `CoordinatorStrategy(GROUP)` membership-mutation RPCs; each needs
one net-new wire wrapper (`DeleteGroupsRequest`, `LeaveGroupRequest`).

- **RPCs:** `deleteConsumerGroups(Collection<String>, ...)`;
  `removeMembersFromConsumerGroup(String,
  RemoveMembersFromConsumerGroupOptions)` (specific members, or all via
  `removeAll()`).
- **Java main sources:** `KafkaAdminClient.java` (~3757 / ~4206).
  `DeleteConsumerGroups{Options,Result}`,
  `RemoveMembersFromConsumerGroup{Options,Result}`, `MemberToRemove`.
  `internals/DeleteGroupsHandler.java` (145, abstract base doing the real
  `DeleteGroupsRequest`/`Response` handling) + `DeleteConsumerGroupsHandler.java`
  (39, thin subclass overriding only `apiName()`/`displayName()`).
  `internals/RemoveMembersFromConsumerGroupHandler.java` (149, uses
  `LeaveGroupRequest`/`Response`, keyed by `MemberIdentity`).
- **Java tests:** `KafkaAdminClientTest` — `testDeleteConsumerGroups*`
  (incl. `...WithOlderBroker`), `testRemoveMembersFromGroup*` (note the
  private helper `testRemoveMembersFromGroup(reason, expectedReason)`
  parameterizes the three `...Reason` tests — translate as a helper fn, not
  three copies). Dedicated:
  `RemoveMembersFromConsumerGroupOptionsTest`,
  `RemoveMembersFromConsumerGroupResultTest` (no dedicated
  `DeleteConsumerGroups*Test` — inline in `KafkaAdminClientTest`).
  `internals/{DeleteConsumerGroups, DeleteGroups,
  RemoveMembersFromConsumerGroup}HandlerTest`.
- **Rust modules:** `src/admin/{delete_consumer_groups_options,
  delete_consumer_groups_result, remove_members_from_consumer_group_options,
  remove_members_from_consumer_group_result, member_to_remove}.rs`;
  `src/admin/internals/{delete_groups_handler, delete_consumer_groups_handler,
  remove_members_from_consumer_group_handler}.rs`;
  `src/common/requests/{delete_groups, leave_group}_{request,response}.rs`
  (both net new; message specs exist under `generator/messages/`).
- **Test plan.** *Unit:* the "reason truncation" test
  (`testRemoveMembersFromGroupTruncatesReason`) needs an exact-truncated-string
  assertion, not just presence. Byte-level wire tests for `DeleteGroups*`
  and `LeaveGroup*` (incl. `MemberIdentity` sub-struct encoding).
  *Integration:* extend `admin_groups_test.rs` (Phase 1's file) — (a) empty
  group → `delete_consumer_groups`, assert gone; (b) delete a group with
  active members asserts the expected non-retriable error (verify exact type
  against the broker); (c) `remove_members_from_consumer_group` for one
  member asserts rebalance + reassignment; (d) `remove_all=true` empties the
  group.
- **Dependencies:** Tier 2 Phase 1 (`CoordinatorStrategy`), Tier 1.

## Phases (Tier 3 — ACLs, quotas, SCRAM, tokens, transactions, features)

Dispatch-mechanism ground truth (from `KafkaAdminClient.java`):

| RPC | Mechanism | Lookup strategy |
|---|---|---|
| `describeAcls`/`createAcls`/`deleteAcls` | plain `Call` | `LeastLoadedBrokerOrActiveKController` |
| `describeClientQuotas`/`alterClientQuotas` | plain `Call` | `LeastLoadedNodeProvider` |
| `describeUserScramCredentials` | plain `Call` | `LeastLoadedNodeProvider` |
| `alterUserScramCredentials` | plain `Call` | `ControllerNodeProvider` |
| delegation tokens (create/renew/expire/describe) | plain `Call` | `LeastLoadedNodeProvider` |
| `describeFeatures` | plain `Call` | `ConstantNodeIdProvider` (if `nodeId` set) or `LeastLoadedBrokerOrActiveKController` |
| `updateFeatures` | plain `Call` | verify provider at translation time |
| `describeProducers` | `AdminApiDriver` | `StaticBrokerStrategy` (if `brokerId` set) or `PartitionLeaderStrategy` (reused) |
| `describeTransactions` | `AdminApiDriver` | `CoordinatorStrategy(TRANSACTION)` |
| `abortTransaction` | `AdminApiDriver` | `PartitionLeaderStrategy` (reused) |
| `forceTerminateTransaction` | plain `Call` (verify — small method) | — |
| `listTransactions` | `AdminApiDriver` | `AllBrokersStrategy` — **first real use anywhere** |
| `fenceProducers` | `AdminApiDriver` | `CoordinatorStrategy(TRANSACTION)` |
| `listClientMetricsResources` | plain `Call`, reuses `ListConfigResourcesRequest` filtered to `CLIENT_METRICS` — **no new wire type** | `LeastLoadedNodeProvider` |

| # | Phase | Scope | Depends on |
|---|---|---|---|
| 1 | **ACLs** | ACL/resource primitives + `createAcls`/`describeAcls`/`deleteAcls` | Tier 1 |
| 2 | **Client quotas** | `describeClientQuotas`/`alterClientQuotas` | Tier 1 |
| 3 | **SCRAM credentials** | `describeUserScramCredentials`/`alterUserScramCredentials` — **needs crypto-crate decision (finding #4)** | Tier 1 |
| 4 | **Delegation tokens** | create/renew/expire/describe — **zero Java unit tests (finding #10)** | Tier 1 |
| 5 | **Features** | `describeFeatures`/`updateFeatures` | Tier 1 |
| 6 | **Producers & transactions** | `describeProducers`/`describeTransactions`/`abortTransaction`/`forceTerminateTransaction`/`listTransactions`/`fenceProducers` — first `StaticBrokerStrategy` + `AllBrokersStrategy` use | Tier 1 P5, Tier 2 P1 |
| 7 | **Client metrics** | `listClientMetricsResources` | Tier 1 P3 |

### Tier 3, Phase 1 — ACLs

- **RPCs:** `createAcls(Collection<AclBinding>, ...)`,
  `describeAcls(AclBindingFilter, ...)`,
  `deleteAcls(Collection<AclBindingFilter>, ...)`.
- **Java main sources:** `KafkaAdminClient.java` (`describeAcls` ~2558,
  `createAcls` ~2594, `deleteAcls` ~2654). `{Create,Describe,Delete}Acls{Options,Result}`
  (incl. `DeleteAclsResult.{FilterResults,FilterResult}`). Prerequisites
  (finding #7): `common/acl/*`, `common/resource/*` — re-verify whether
  `AclOperation`/`AclPermissionType` landed in Tier 1 (finding #1); if so
  only the remaining 10 files. Wire (net new):
  `{Describe,Create,Delete}AclsRequest`/`Response` (specs exist).
- **Java tests:** `KafkaAdminClientTest` — `testDescribeAcls`,
  `testCreateAcls`, `testCreateAclsToController`, `testDeleteAcls`,
  `testDeleteAclsToController`. Dedicated (physically under `.../common/acl/`
  even where they cover `resource` classes — a Java layout quirk, don't nest
  Rust modules to match): `AclBindingTest`, `AclOperationTest`,
  `AclPermissionTypeTest`, `ResourcePatternFilterTest`, `ResourcePatternTest`,
  `ResourceTypeTest`. No `AclBindingFilterTest`/`AccessControlEntry*Test`/
  `PatternTypeTest` exist (note as "not present in Java" per DoD #3, don't
  invent).
- **Rust modules:** `src/common/acl/{acl_binding, acl_binding_filter,
  access_control_entry, access_control_entry_filter, acl_operation,
  acl_permission_type}.rs`; `src/common/resource/{resource, resource_pattern,
  resource_pattern_filter, pattern_type, resource_type}.rs`;
  `src/admin/{create_acls, describe_acls, delete_acls}_{options,result}.rs`;
  `src/common/requests/{describe_acls, create_acls,
  delete_acls}_{request,response}.rs`.
- **Test plan.** *Unit:* translate all six dedicated test files 1:1 + the
  five `KafkaAdminClientTest` methods; byte-level wire tests for all three
  new wrappers. Assert with real error messages: `AclBindingFilter::is_unknown()`
  short-circuit (`describeAcls` on an unknown filter completes exceptionally
  with `InvalidRequestException` and **no** `Call` enqueued — assert the
  future fails without a network call, mirroring `KafkaAdminClient.java:2559`);
  `createAcls` per-binding indefinite-field rejection (other valid bindings in
  the same batch still succeed). *Integration:* new
  `tests/integration/admin_acls_test.rs`, requires the authorizer-enabled
  `ClusterConfig` fixture (finding #11) — (a) `create_acls` → `describe_acls`
  round-trip; (b) non-matching filter returns empty; (c) `delete_acls` then
  `describe_acls` shows gone; (d) end-to-end authorizer check: a gated
  operation fails with `TopicAuthorizationException` without the ACL, then
  succeeds after `create_acls`.
- **Dependencies:** Tier 1. Independent of other Tier 3 phases (plain `Call`).

### Tier 3, Phase 2 — Client quotas

- **RPCs:** `describeClientQuotas(ClientQuotaFilter, ...)`,
  `alterClientQuotas(Collection<ClientQuotaAlteration>, ...)`.
- **Java main sources:** `KafkaAdminClient.java` (~4273 / ~4301).
  `{Describe,Alter}ClientQuotas{Options,Result}`. Prerequisites (finding #7):
  `common/quota/{ClientQuotaEntity, ClientQuotaFilter,
  ClientQuotaFilterComponent, ClientQuotaAlteration}`. Wire (net new):
  `{Describe,Alter}ClientQuotasRequest`/`Response`.
- **Java tests:** `KafkaAdminClientTest` — `testDescribeClientQuotas`,
  `testEqualsOfClientQuotaFilterComponent` (the only coverage for that
  class), `testAlterClientQuotas`. **No dedicated per-class test files exist**
  for the four `common/quota/*` classes (upstream gap, not a deliberate skip
  — note per DoD #3). Compensate with our own round-trip/equals unit tests.
- **Rust modules:** `src/common/quota/{client_quota_entity,
  client_quota_filter, client_quota_filter_component,
  client_quota_alteration}.rs`; `src/admin/{describe_client_quotas,
  alter_client_quotas}_{options,result}.rs`;
  `src/common/requests/{describe_client_quotas,
  alter_client_quotas}_{request,response}.rs`.
- **Test plan.** *Unit:* the two `KafkaAdminClientTest` methods + new
  construction/equality tests for the quota POJOs (call out "new test, no
  Java original"). Byte-level wire tests incl. `ClientQuotaFilterComponent`
  match-type encoding (`EXACT`/`DEFAULT`/`ANY`). *Integration:* new
  `tests/integration/admin_quotas_test.rs` — (a) `alter_client_quotas` sets a
  byte-rate quota, `describe_client_quotas` round-trips; (b) removal (Java =
  `Op` with `null` value) no longer reported; (c) entity-type filter returns
  only matching entities.
- **Dependencies:** Tier 1 only.

### Tier 3, Phase 3 — SCRAM credentials

- **RPCs:** `describeUserScramCredentials(List<String>, ...)`,
  `alterUserScramCredentials(List<UserScramCredentialAlteration>, ...)`.
- **Java main sources:** `KafkaAdminClient.java` (~4332 / ~4378 + private
  `getScramCredentialUpsertion`/`getScramCredentialDeletion`/`getSaltedPassword`
  ~4501–4517, the crypto call site). `Describe/AlterUserScramCredentials{Options,Result}`,
  `UserScramCredentialsDescription`, `UserScramCredentialAlteration` (abstract
  base) + `UserScramCredentialUpsertion`/`UserScramCredentialDeletion`,
  `ScramCredentialInfo`, `admin/ScramMechanism` (self-contained). Prerequisite
  (finding #4): PBKDF2 crate + narrow translation of
  `common/security/scram/internals/ScramFormatter.hi()` and the internal
  `ScramMechanism` name→algorithm mapping. **Ask before adding the crate.**
  Wire (net new): `{Describe,Alter}UserScramCredentialsRequest`/`Response`.
- **Java tests:** `KafkaAdminClientTest` — `testDescribeUserScramCredentials`,
  `testAlterUserScramCredentialsUnknownMechanism`,
  `testAlterUserScramCredentials` (no `throws` — may run real PBKDF2
  synchronously; ensure the Rust equivalent actually exercises the crypto
  path). Dedicated: `ScramMechanismTest`,
  `DescribeUserScramCredentialsResultTest`. No `internals/*HandlerTest` (plain
  `Call`).
- **Rust modules:** `src/admin/{describe_user_scram_credentials_options,
  describe_user_scram_credentials_result, user_scram_credentials_description,
  alter_user_scram_credentials_options, alter_user_scram_credentials_result,
  user_scram_credential_alteration, user_scram_credential_upsertion,
  user_scram_credential_deletion, scram_credential_info, scram_mechanism}.rs`;
  `src/common/security/scram/internals/{scram_formatter,
  scram_mechanism}.rs` (the *internal* mechanism, distinct from
  `admin::ScramMechanism`);
  `src/common/requests/{describe_user_scram_credentials,
  alter_user_scram_credentials}_{request,response}.rs`.
- **Test plan.** *Unit:* the three `KafkaAdminClientTest` methods +
  `ScramMechanismTest` + `DescribeUserScramCredentialsResultTest`. Add a
  focused test asserting `hi()` output against a known RFC 5802 / Java-reference
  vector (salt, password, iterations → salted-password bytes) — the one place
  byte-level vectors matter most, since a subtly wrong PBKDF2 silently produces
  credentials the broker rejects at auth time, not alter time. *Integration:*
  new `tests/integration/admin_scram_test.rs` — (a) upsert (SCRAM_SHA_256,
  iterations=8192), describe, assert mechanism+iterations round-trip (never
  assert the salted password, which the broker never returns); (b) actually
  SASL/SCRAM-authenticate a client with the created credential — the real
  correctness signal for finding #4; (c) delete, assert (b)'s auth now fails.
- **Dependencies:** Tier 1. **Blocking:** resolve the crate ask before
  Rust-core work (it changes `Cargo.toml`).

### Tier 3, Phase 4 — Delegation tokens

- **RPCs:** `createDelegationToken(...)`, `renewDelegationToken(byte[], ...)`,
  `expireDelegationToken(byte[], ...)`, `describeDelegationToken(...)`.
- **Java main sources:** `KafkaAdminClient.java` (~3277 / ~3326 / ~3360 /
  ~3394). `{Create,Renew,Expire,Describe}DelegationToken{Options,Result}`.
  Prerequisites (finding #5): `common/security/auth/KafkaPrincipal`,
  `common/security/token/delegation/{DelegationToken,TokenInformation}`. Wire
  (net new): `{Create,Renew,Expire,Describe}DelegationTokenRequest`/`Response`
  (specs present).
- **Java tests:** **none anywhere in `clients/src/test/java`** (finding #10).
  `MockAdminClient.java` (~641–722) has full real logic for all four —
  primary behavioral reference. Note explicitly in the DoD write-up that unit
  tests are new, written against the mock.
- **Rust modules:** `src/common/security/auth/kafka_principal.rs`;
  `src/common/security/token/delegation/{delegation_token,
  token_information}.rs`; `src/admin/{create,renew,expire,describe}_delegation_token_{options,result}.rs`;
  `src/common/requests/{create,renew,expire,describe}_delegation_token_{request,response}.rs`.
- **Test plan.** *Unit* (against the mock): (a) non-`User`-type renewer
  principal → `InvalidPrincipalTypeException`-equivalent, exact message; (b)
  renew/expire with unknown HMAC → `DelegationTokenNotFoundException`-equiv;
  (c) `expire` with `expiry_time_period_ms = -1` (Java's expire-immediately
  sentinel) removes the token; (d) describe with `owners` filter returns only
  matches. Byte-level wire tests for all four (raw HMAC bytes + principal
  strings — a wire bug is security-relevant). *Integration:* new
  `tests/integration/admin_delegation_tokens_test.rs`, requires SASL-enabled
  broker (delegation tokens pair with SCRAM — verify the test image, possibly
  reuse Phase 3's SCRAM fixture): (a) create → describe lists it; (b) renew →
  later expiry; (c) expire → no longer listed; (d) stretch: authenticate a
  client using the token as SCRAM credentials.
- **Dependencies:** Tier 1. Independent of other Tier 3 phases.

### Tier 3, Phase 5 — Features

- **RPCs:** `describeFeatures(...)`, `updateFeatures(Map<String,
  FeatureUpdate>, ...)`.
- **Java main sources:** `KafkaAdminClient.java` (~4520 / ~4575).
  `Describe/UpdateFeatures{Options,Result}`, `FeatureMetadata`,
  `FinalizedVersionRange`, `SupportedVersionRange`, `FeatureUpdate` (carries
  `UpgradeType`: `UPGRADE`/`SAFE_DOWNGRADE`/`UNSAFE_DOWNGRADE`/`UNKNOWN`).
  `describeFeatures` reuses `ApiVersionsRequest`/`Response` (verify a wrapper
  already exists from Producer/Consumer handshake work — if so, zero new wire
  type here); `updateFeatures` needs new `UpdateFeaturesRequest`/`Response`.
- **Java tests:** `KafkaAdminClientTest` — private parameterized helper
  `testUpdateFeatures` driving `testUpdateFeaturesDuringSuccess`,
  `...TopLevelError`, `...HandleNotControllerException` (both
  `@ParameterizedTest` over API version → loop over the version range),
  `...ShouldFailRequestForEmptyUpdates`, `...ForInvalidFeatureName`,
  `...WhenDowngradeFlagIsNotSetDuringDeletion`; `testDescribeFeatures{Success,
  Failure,WithNodeSuccess,WithNodeFailure}`. No dedicated per-class files.
- **Rust modules:** `src/admin/{describe_features_options,
  describe_features_result, feature_metadata, finalized_version_range,
  supported_version_range, update_features_options, update_features_result,
  feature_update}.rs`; `src/common/requests/update_features_{request,response}.rs`
  (only if not already present).
- **Test plan.** *Unit:* parameterized-over-API-version tests become loops
  over the exact bounds Java uses (read `@ValueSource`/`@ParameterizedTest`
  source at translation time — don't guess).
  `...WhenDowngradeFlagIsNotSetDuringDeletion` needs an exact-error-message
  assertion (client-side validation, not wire). **MockAdminClient parity
  (finding #9):** Java's mock has *real* upgrade/downgrade version-bounds
  validation — translate it faithfully, do not stub (the one Tier 3 group
  where the mock isn't a stub). *Integration:* new
  `tests/integration/admin_features_test.rs` — (a) `describe_features` asserts
  a finalized feature (`metadata.version`) with a sane range; (b)
  `update_features` beyond max supported → expected error; (c) if the image
  allows, a real low-risk upgrade + re-describe (may scope down to (a)+(b) —
  verify at kickoff).
- **Dependencies:** Tier 1. Independent of other Tier 3 phases.

### Tier 3, Phase 6 — Producers & transactions

One phase (six tightly-coupled transaction-domain RPCs), but Rust-core
sub-sliced into three commits/PRs (not separate phases): (a)
`describeProducers`+`abortTransaction` (reuse `PartitionLeaderStrategy`, zero
new strategy infra); (b) `describeTransactions`+`fenceProducers`
(`CoordinatorStrategy(TRANSACTION)` — new coordinator type, same class as
Tier 2); (c) `listTransactions`+`forceTerminateTransaction`
(`AllBrokersStrategy`, genuinely new).

- **RPCs:** `describeProducers(Collection<TopicPartition>, ...)`;
  `describeTransactions(Collection<String>, ...)`;
  `abortTransaction(AbortTransactionSpec, ...)`;
  `forceTerminateTransaction(String, ...)`; `listTransactions(...)` (first
  real `AllBrokersStrategy` user); `fenceProducers(Collection<String>, ...)`
  (returns new `ProducerIdAndEpoch` per id).
- **Java main sources:** `KafkaAdminClient.java` (~4811 / ~4820 / ~4829 /
  ~4849 / ~4867 / ~4876). `DescribeProducers{Options,Result}` (nested
  `PartitionProducerState`), `ProducerState`,
  `DescribeTransactions{Options,Result}`, `TransactionDescription`,
  `TransactionState`, `AbortTransaction{Options,Result,Spec}`,
  `TerminateTransaction{Options,Result}`, `ListTransactions{Options,Result}`,
  `TransactionListing`, `FenceProducers{Options,Result}`.
  `internals/{DescribeProducers, AbortTransaction, DescribeTransactions,
  FenceProducers, ListTransactions}Handler.java`,
  `internals/StaticBrokerStrategy.java` (65, net new),
  `internals/AllBrokersStrategy.java` (213, net new), reused
  `PartitionLeaderStrategy`/`PartitionLeaderCache`. Prerequisite (finding #6):
  `common/utils/ProducerIdAndEpoch`. Wire (net new):
  `{DescribeProducers, DescribeTransactions,
  ListTransactions}Request`/`Response`; **confirm at translation time** what
  `abortTransaction`/`forceTerminateTransaction`/`fenceProducers` use (likely
  `WriteTxnMarkersRequest`/`InitProducerIdRequest` — read the handler bodies).
- **Java tests:** `KafkaAdminClientTest` — `testDescribeProducers*`
  (incl. `Timeout` parameterized-bool → loop, `RetryAfterDisconnect`),
  `testDescribeTransactions*`, `testAbortTransaction*`,
  `testForceTerminateTransaction*`, `testListTransactions`,
  `testFenceProducers`. Dedicated: `ListTransactionsResultTest`.
  `internals/{AbortTransaction, DescribeProducers, DescribeTransactions,
  FenceProducers, ListTransactions}HandlerTest`,
  `internals/AllBrokersStrategyTest` + `AllBrokersStrategyIntegrationTest`,
  `internals/PartitionLeaderStrategyTest`/`IntegrationTest` (add only net-new
  cases, e.g. `StaticBrokerStrategy` fallback, if Tier 1 already covered the
  rest).
- **Rust modules:** `src/admin/{describe_producers_options,
  describe_producers_result, producer_state, describe_transactions_options,
  describe_transactions_result, transaction_description, transaction_state,
  abort_transaction_options, abort_transaction_result, abort_transaction_spec,
  terminate_transaction_options, terminate_transaction_result,
  list_transactions_options, list_transactions_result, transaction_listing,
  fence_producers_options, fence_producers_result}.rs`;
  `src/admin/internals/{describe_producers_handler, abort_transaction_handler,
  describe_transactions_handler, fence_producers_handler,
  list_transactions_handler, static_broker_strategy,
  all_brokers_strategy}.rs`; `src/common/utils/producer_id_and_epoch.rs`;
  `src/common/requests/{describe_producers, describe_transactions,
  list_transactions}_{request,response}.rs` (+ whatever the abort/terminate/
  fence RPCs turn out to need).
- **Test plan.** *Unit:* direct translation, sub-sliced per (a)/(b)/(c).
  `testDescribeProducersTimeout(boolean)` → loop over both, asserting the
  distinct "timeout in metadata lookup" vs "timeout waiting for producer
  state" messages if Java distinguishes them.
  `AllBrokersStrategyTest`/`IntegrationTest` get full attention — the
  strategy's debut. **Design-gap flag:** `AllBrokersStrategy` fans out to all
  known brokers with *no per-key lookup RPC* (more like the hand-rolled
  `listGroups` enumeration than `CoordinatorStrategy`). If the Tier 1
  `AdminApiLookupStrategy` trait can't cleanly express "no per-key lookup,
  just get the broker list," that's a foundation design gap surfaced at Tier
  3 — **flag it loudly, don't paper over with a hack**; may need a Tier 1
  amendment. Byte-level wire tests for the three new request/response types.
  *Integration:* new `tests/integration/admin_transactions_test.rs`, requires
  a real transactional producer (reuse `producer_test.rs` transactional setup
  if present): (a) uncommitted txn → `describe_producers` reports in-flight
  producer state; (b) `describe_transactions` → `TransactionState::Ongoing`;
  (c) `abort_transaction` (correct `AbortTransactionSpec` from (a)) →
  follow-up describe shows `CompleteAbort`/`Empty`; (d) `list_transactions`
  shows the id while ongoing, filtered by state; (e) `fence_producers` →
  new `ProducerIdAndEpoch`, then old producer's `send`/`commit` fails with
  `ProducerFencedException`; (f) `force_terminate_transaction` as a simpler
  recovery path, same fencing assertion.
- **Dependencies:** Tier 1 P5 (`PartitionLeaderStrategy`/Cache); Tier 2 P1's
  `CoordinatorStrategy` (reused with `TRANSACTION` — **verify at review that
  the Rust `CoordinatorStrategy` was written generic over coordinator type,
  not hardcoded to `GROUP`**).

### Tier 3, Phase 7 — Client metrics

Its own tiny phase (no shared handler; piggybacks entirely on Tier 1
infra). Folds into Bindings-slice C.

- **RPCs:** `listClientMetricsResources(ListClientMetricsResourcesOptions)`.
- **Java main sources:** `KafkaAdminClient.java` (~4922–4935; reuses
  `ListConfigResourcesRequest.Builder` filtered to
  `ConfigResource.Type.CLIENT_METRICS.id()` — **zero new wire types**, that
  wrapper is Tier 1 Phase 3 scope). `ListClientMetricsResources{Options,Result}`,
  `ClientMetricsResourceListing`.
- **Java tests:** `KafkaAdminClientTest` — `testListClientMetricsResources`,
  `...Empty`, `...NotSupported` (older broker `UNSUPPORTED_VERSION` — assert
  the exact resulting error). Note `testDescribeClientMetricsConfigs` (~2199)
  is a separate Tier-1 `describeConfigs`-on-`CLIENT_METRICS` test — don't
  conflate; verify Tier 1 Phase 3 picks it up.
- **Rust modules:** `src/admin/{list_client_metrics_resources_options,
  list_client_metrics_resources_result, client_metrics_resource_listing}.rs`.
  No new `internals/` or `requests/` files.
- **Test plan.** *Unit:* the three listed methods. *Integration:* likely a
  small addition to `admin_features_test.rs` (Phase 5's file) — configure a
  client-metrics subscription if the image supports it; if not practically
  testable, fall back to unit-only and note it (don't write a vacuous test).
- **Dependencies:** Tier 1 Phase 3 (`listConfigResources` wire wrapper).

## Out of scope (added to the Tier 4 deferral list)

`ForwardingAdmin.java` (finding #8) — a broker-plugin envelope-forwarding
delegate, not a surface applications construct. Deferred alongside Tier 4
for the same rationale (broker/controller-side, not typical client usage).

## C FFI conventions to follow (already established by producer/consumer)

> **DEFERRED — not executed in this task.** Retained as design reference for
> the future bindings task. Note: the async C dispatcher / `CompletionJob` /
> `spawn_dispatcher` machinery and `src/ffi/common.rs` referenced below do
> **not** exist on `master`/this branch yet — they live in unmerged PR #116
> (`dev/c_and_python_consumer_bindings`). The future task must reuse PR #116's
> `src/ffi/common.rs`, not reinvent it. There is currently no consumer FFI;
> the only merged precedent is the synchronous producer FFI (`block_on`).

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

> **DEFERRED — not executed in this task.** Retained as design reference for
> the future bindings task. Note: `consumer.py` and the consumer `_run_sync`/
> `_run_async` helpers referenced below do **not** exist yet (they are part
> of unmerged PR #116); only `producer.py` exists on this branch. Reuse
> PR #116's work when the future task starts.

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

Per phase (this task): `cargo build`, `cargo test` (targeted to the new
module plus full suite before closing the phase), `cargo xtask lint`, `cargo
xtask format-check`, and the phase's real-broker integration tests. The
CMake/CTest Unity suite and `pytest ...test_admin.py` runs are **deferred**
along with the FFI/Python layers (see scope banner) — they belong to the
future bindings task. Before closing each *tier*, run
the full `make verify` (per CLAUDE.md's `Development Workflow` and DoD
item #9) to catch cross-cutting regressions in producer/consumer from
shared-code changes (e.g. `KafkaError`, `NetworkClient`, `kafka_future.rs`).
