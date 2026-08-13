# Current Status: Milestones 1-10 complete; Milestone 11 tracks — Producer Transactions (complete) and AdminClient (Tier 1 complete)

<!-- Two workstreams both carry the "Milestone 11" label; both summaries kept below. -->


> **Current state (2026-07-17):** Milestone 11 AdminClient **Tier 1 is fully
> complete** (Phases 1–5: topics CRUD, partitions & records, cluster & configs,
> log dirs, elections/reassignments/offsets — all Rust core + unit + real-broker
> integration tests, 2355 lib tests passing). See the "Milestone 11 — AdminClient"
> sections below for the per-phase detail. Tier 2 (consumer groups & offsets) is
> next. Scope for this task is **Rust core only** — C FFI / Python bindings are
> deferred to a separate future task.
>
> The sections immediately below (Milestone 1 / Milestone 3) are **historical and
> stale** — they predate the Producer, Consumer and Admin milestones and are left
> as-is rather than rewritten with unverified detail.

## (Historical) Milestone 3 Complete (SSL + SASL Authentication)


**Last verified:** 2026-08-06 (Milestone 11 Phase 8)

| | |
|---|---|
| Complete | Milestones 1-10 (see `design/history/MILESTONES.md`) |
| In progress | Milestone 11 — producer idempotence and transactions (Phase 8, the last phase) |
| Source | 263 files, ~176 600 lines under `src/` |
| Tests | ~2 666 passing (lib + protocol/message + consumer + producer suites), 3 `#[ignore]`d |
| Java base | Apache Kafka 4.2.0 (`kafka/` submodule at `a18251b`) |

Breakdown by area. File counts are exact; line counts are rounded to the nearest
hundred **deliberately** — an exact figure here was invalidated twice inside Milestone
11 Phase 8 by later commits in the same phase, once by a 22-line doc comment, so the
precision was costing review cycles without buying anything. Re-derive with:

```sh
for d in common consumer producer ffi; do
  echo "$d $(find src/$d -name '*.rs' | wc -l) $(find src/$d -name '*.rs' -exec cat {} + | wc -l)"
done
echo "root $(find src -maxdepth 1 -name '*.rs' | wc -l) $(find src -maxdepth 1 -name '*.rs' -exec cat {} + | wc -l)"
```

| Area | Files | Lines |
|---|---|---|
| `src/common/` | 148 | ~47 600 |
| `src/consumer/` | 64 | ~64 500 |
| `src/producer/` | 23 | ~42 200 |
| `src/ffi/` | 4 | ~7 800 |
| root client layer (`src/*.rs`) | 22 | ~14 300 |

`src/producer/` roughly tripled over Milestone 11 (15 894 → ~42 200 lines): the
`TransactionManager` and its dependency closure, the transactional `Sender` loop
and public producer API, plus the translated `TransactionManagerTest` and
`SenderTest` suites, which are the larger half.

All three `#[ignore]`d tests are reproducers for open defects, not gaps in
translation, and each is tracked in `design/history/Milestone-11/PLAN.md` §9 with a
fix direction:

  - `test_transactional_unknown_producer_handling_when_retention_limit_reached` —
    §9.25, an empty batch pool on the transactional log-truncation retry.
  - `test_init_producer_id_request_versions` — §9.1, the code generator omitting
    Java's non-default-at-unsupported-version guard. Systemic across all 197
    generated message types, so it predates Milestone 11.
  - `test_build_is_repeatable` — §9.30, `ProduceRequestBuilder::build_version`
    draining its builder where Java's `Builder.build` does not. Latent rather than
    live; the section carries the reachability derivation.

(`test_too_large_batches_are_safely_removed` was the third until loop 50 fixed
§9.18, the split-on-`MESSAGE_TOO_LARGE` panic on the write path. It now runs.)

(Separately, the integration suite `#[ignore]`s 10 tests that need harness
capabilities the pooled cluster does not expose, such as shutting down a broker;
each says so at its definition.)

Plus 197 wire-protocol message types generated at build time from the official
JSON definitions.

Beyond the Rust crate: a C FFI, a CPython extension with sync and asyncio
wrappers, and a gRPC harness that runs one shared set of integration scenarios
against native Rust, Python, and C backends.

## ⚠ Note on this document

Everything below this heading describes **Milestones 1 and 3 only** and was
written when those were the whole project. It is accurate for the network,
protocol, and security layers it covers, but it is **not** a statement of current
scope — it predates the producer (Milestone 2), the C FFI and bindings
(Milestones 4, 9, 10), the performance work (Milestone 5), the multilanguage
harness (Milestone 6), the translation agent (Milestone 7), and the entire
consumer (Milestone 8), which is now the largest module in the crate.

For current scope and progress use `design/history/MILESTONES.md` and the
per-phase `design/history/Milestone-N/**/PLAN.md` files. For performance, use
`design/current/client-comparison-results.md`, which is kept current.

## Completed Components (Milestones 1 and 3 — historical detail)

### Layer 1 — Core Protocol Types (5 classes) ✓
- **Node** (`common/node.rs`): Kafka broker representation
- **TopicPartition** (`common/topic_partition.rs`): Topic/partition identifier
- **Cluster** (`common/cluster.rs`): Cluster metadata with node collection
- **ApiKeys** (`common/protocol/api_keys.rs`): Enum of all Kafka API request types
- **Errors** (`common/protocol/errors.rs`): 134 Kafka error codes with retriable/fatal classification

### Layer 2 — Wire Protocol (5 classes) ✓
- **Readable** (`common/protocol/readable.rs`): Trait for reading wire protocol data
- **Writable** (`common/protocol/writable.rs`): Trait for writing wire protocol data
- **ByteBufferAccessor** (`common/protocol/byte_buffer_accessor.rs`): Buffer implementation for Readable/Writable
- **Message** (`common/protocol/message.rs`): Core versioned serialization trait (size, add_size, read, write, unknown_tagged_fields, duplicate)
- **ApiMessage** (`common/protocol/message.rs`): Extends Message with api_key()

### Layer 3 — Request/Response Framework ✓
- **AbstractRequest** (`common/requests/abstract_request.rs`): ConcreteRequest enum dispatching to all request types
- **AbstractResponse** (`common/requests/abstract_response.rs`): ConcreteResponse enum dispatching to all response types
- **RequestHeader** (`common/requests/request_header.rs`): Request header serialization
- **ResponseHeader** (`common/requests/response_header.rs`): Response header parsing
- **ApiVersionsRequest/Response** (`common/requests/api_versions_*.rs`): API version negotiation
- **MetadataRequest/Response** (`common/requests/metadata_*.rs`): Cluster metadata discovery
- **SendBuilder** (`common/requests/send_builder.rs`): Zero-copy request serialization

### Layer 4 — Network Transport ✓
- **TransportLayer** (`common/network/transport_layer.rs`): Async transport trait
- **PlaintextTransportLayer** (`common/network/plaintext_transport_layer.rs`): TCP transport with Tokio
- **Send/Receive** (`common/network/send.rs`, `receive.rs`): Wire-level send/receive abstractions
- **NetworkSend/NetworkReceive** (`common/network/network_send.rs`, `network_receive.rs`): Node-addressed send/receive
- **ByteBufferSend** (`common/network/byte_buffer_send.rs`): Buffer-backed send implementation

### Layer 5 — Selector & KafkaChannel ✓
- **Selector** (`common/network/selector.rs`): Non-blocking I/O multiplexer (Java NIO Selector → Tokio)
- **KafkaChannel** (`common/network/kafka_channel.rs`): Per-connection state machine
- **PlaintextChannelBuilder** (`common/network/plaintext_channel_builder.rs`): Plaintext channel factory
- **Selectable** (`common/network/selectable.rs`): Selector trait for testability

### Layer 6 — Network Client ✓
- **NetworkClient** (`clients/network_client.rs`): Core client with connection management, request/response dispatch, metadata updates
- **KafkaClient** (`clients/kafka_client.rs`): KafkaClient trait (Java interface → Rust trait)
- **Metadata** (`clients/metadata.rs`): Thread-safe metadata cache with epoch tracking
- **MetadataSnapshot** (`clients/metadata_snapshot.rs`): Immutable cluster metadata snapshot
- **NodeApiVersions** (`clients/node_api_versions.rs`): Per-node API version tracking
- **ApiVersions** (`clients/api_versions.rs`): Thread-safe API version registry
- **ClusterConnectionStates** (`clients/cluster_connection_states.rs`): Connection state machine with exponential backoff
- **InFlightRequests** (`clients/in_flight_requests.rs`): In-flight request tracking
- **ClientRequest/ClientResponse** (`clients/client_request.rs`, `client_response.rs`): Request/response wrappers
- **NetworkClientUtils** (`clients/network_client_utils.rs`): Blocking utility functions
- **MockSelector** (`common/network/mock_selector.rs`): Test-only mock for Selector

### Milestone 3 — SSL + SASL Authentication (6 Phases) ✓

#### Phase 1 — Security & Config Types ✓
- **SecurityProtocol** (`common/security/auth/security_protocol.rs`): PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL
- **SslConfig** (`common/config/ssl_configs.rs`): SSL/TLS configuration (truststore, keystore, PEM certs)
- **SaslConfig** (`common/config/sasl_configs.rs`): SASL configuration (mechanism, JAAS, credentials)
- **SslClientAuth** (`common/config/ssl_client_auth.rs`): Client auth mode enum
- **ListenerName** (`common/network/listener_name.rs`): Listener name wrapper

#### Phase 2 — SSL/TLS Transport ✓
- **SslFactory** (`common/security/ssl/ssl_factory.rs`): TLS configuration factory using rustls
- **SslTransportLayer** (`common/network/ssl_transport_layer.rs`): Async TLS transport via tokio-rustls
- **SslChannelBuilder** (`common/network/ssl_channel_builder.rs`): TLS channel factory

#### Phase 3 — SASL Handshake/Authenticate Request/Response ✓
- **SaslHandshakeRequest/Response** (`common/requests/sasl_handshake_*.rs`): SASL mechanism negotiation
- **SaslAuthenticateRequest/Response** (`common/requests/sasl_authenticate_*.rs`): SASL auth exchange

#### Phase 4 — SASL Client Authenticator ✓
- **SaslClientAuthenticator** (`common/security/authenticator/sasl_client_authenticator.rs`): Full SASL PLAIN authenticator state machine
- **Authenticator trait refactored** to accept transport layer for SASL support

#### Phase 5 — Integration & Wiring ✓
- **ChannelBuilders** (`common/network/channel_builders.rs`): Factory function `client_channel_builder()` dispatching on SecurityProtocol
- **SaslChannelBuilder** (`common/network/sasl_channel_builder.rs`): SASL channel factory (SASL_PLAINTEXT and SASL_SSL)

#### Phase 6 — Integration Tests ✓
- **SecureKafka** custom testcontainers image for SSL/SASL Docker containers
- **test_certs** utility for programmatic certificate generation (rcgen)
- **SecurityMode** enum for cluster configuration
- 5 integration tests: SSL, SASL_PLAINTEXT, SASL_SSL, wrong credentials, unsupported mechanism

### Layer 8 — Integration Tests ✓
- **Test infrastructure**: ClusterConfig, ClusterPool (shared containers), KafkaCluster (Docker wrapper), TestContext (per-test isolation)
- **integration_connection_test.rs**: TCP connect, ApiVersions request/response, full connection flow (3 tests)
- **integration_api_versions_test.rs**: Error checking, expected APIs, version ranges, metadata API range (4 tests)
- **integration_metadata_test.rs**: Brokers, controller, specific topic, all topics (4 tests)
- Uses Kafka 4.2.0 via testcontainers with atexit cleanup

### Supporting Infrastructure ✓
- **Uuid** (`common/uuid.rs`): 128-bit UUID with base64 URL encoding and signed comparison matching Java
- **varint** (`common/protocol/varint.rs`): Protocol Buffers varint/varlong encoding (unsigned and zig-zag)
- **MessageSizeAccumulator** (`common/protocol/message_size_accumulator.rs`): Two-pass size tracking
- **ObjectSerializationCache** (`common/protocol/object_serialization_cache.rs`): Two-pass serialization cache
- **MessageUtil** (`common/protocol/message_util.rs`): Helpers (to_byte_buffer_accessor, compare_raw_tagged_fields)
- **ClusterResourceListeners** (`common/internals/cluster_resource_listeners.rs`): Listener collection
- **Code generator** (`generator/`): Generates Rust structs from 197 JSON message specs

### Generated Message Types ✓
- 197 message types auto-generated from JSON specs (build.rs)
- All implement Message trait with read/write/size/add_size
- Top-level types implement ApiMessage with api_key()
- Flexible version support, tagged fields, nullable fields, array bounds checking
- 3 test-only message types in `generator/test-messages/` (SimpleExampleMessage, NullableStructMessage, SimpleArraysMessage)

## Test Coverage
- **579 unit tests** + **16 integration tests** (595 total) all passing
- Integration tests run against Kafka 4.2.0 in Docker (feature-gated: `--features integration-tests`)
- SSL/SASL integration tests use custom SecureKafka Docker image with JAAS config and PEM certificates
- Comprehensive coverage matching Java test suites
- 36+ Critic review issues resolved across multiple review rounds

## Build System
- `generator/messages/`: 197 production JSON message specs → `OUT_DIR/generated/`
- `generator/test-messages/`: 3 test JSON specs → `OUT_DIR/test_generated/` (included via `tests/common/mod.rs`)
- Generated code includes: structs, Message impl, ApiMessage impl, Eq/Hash/Display, builder setters
- `cargo xtask format`: Formats source code (generated files are already formatted by the generator)
- `cargo xtask check-generated`: Validates generated code formatting
- `cargo xtask lint` / `cargo xtask lint-fix`: Clippy with warnings as errors

## Key Java Source Reference
- Layer 3: `kafka/clients/src/main/java/org/apache/kafka/common/requests/`
- Layer 4-5: `kafka/clients/src/main/java/org/apache/kafka/common/network/`
- Layer 6: `kafka/clients/src/main/java/org/apache/kafka/clients/`

---

# Milestone 11 — AdminClient (Tier 1 Phase 1) ✓ (2026-07-16)

> Note: the sections above this line are stale (they predate the Producer and
> Consumer milestones). This section documents only the Milestone 11 Tier 1
> Phase 1 delta. Scope for this task is **Rust core + tests only — no C FFI /
> Python bindings** (deferred to a separate future task; see
> `design/history/Milestone-11/PLAN.md` scope banner). Plan and phase
> breakdown for all of Tiers 1–3 live there.

## Phase 1 — Foundation + Topics CRUD ✓

Translated `org.apache.kafka.clients.admin` foundation and the four topic-CRUD
RPCs, Rust core + unit tests + real-broker integration tests, all green.

- **`Admin` trait + `new_admin_client()` factory** (`src/admin/mod.rs`): per-RPC
  methods are plain sync `fn` returning a `*Result` holding one `KafkaFuture<T>`
  per key; only `close()` is `async fn` (the sole blocking-in-Java method). No
  `#[async_trait]` bleed into per-RPC methods or internal types. Design rules
  captured in `.claude/rules/admin-client.md`.
- **Dispatch engine** (`src/admin/internals/`): `Call` + `NodeProvider` retry
  engine (`call.rs`), `AdminMetadataManager` (`admin_metadata_manager.rs`),
  `AdminUtils` (`admin_utils.rs`), and a single-`tokio::spawn` generic
  `AdminClientRunnable<C: KafkaClient>` background task (`admin_client_runnable.rs`),
  mirroring the producer's `Sender<C>`. Reuses `NetworkClient`/`KafkaClient`.
- **`KafkaAdminClient`** (`kafka_admin_client.rs`), **`MockAdminClient`**
  (`mock_admin_client.rs`, faithful in-memory), **`AdminClientConfig`**.
- **RPCs**: `create_topics`, `delete_topics`, `list_topics`, `describe_topics`
  (both by-name and by-id, via the Metadata-API path — see accepted deviations).
- **POJOs / Options / Results**: `NewTopic`, `TopicListing`, `TopicDescription`,
  `Config`, `ConfigEntry` (+ `ConfigSource`/`ConfigSynonym`/`ConfigType`), and
  `{Create,Delete,List,Describe}Topics{Options,Result}`.
- **New common types**: `common::acl::{AclOperation, AclPermissionType}`,
  `common::TopicCollection`, `common::TopicPartitionInfo`,
  `common::utils::{from_32_bit_field, to_32_bit_field}`; `KafkaFuture` extended
  to be completable (`KafkaFutureImpl<T>` + `all_of`/`then_apply`/`then_apply_try`/
  `join_map`); `KafkaError::ThrottlingQuotaExceeded` added (quota carry-forward).
- **Wire wrappers**: `CreateTopics`/`DeleteTopics` request+response (new
  `ConcreteRequest`/`ConcreteResponse` enum variants; `Metadata` reused for
  list/describe).

### Accepted deviations (documented, Critic-approved)
- `describe_topics` uses the **Metadata-API fallback** (Java's
  `generateDescribeTopicsCallWithMetadataApi` / `handleDescribeTopicsByIds`),
  not `DescribeTopicPartitions` cursor pagination — behavior-faithful for both
  by-name and by-id.
- `TopicDescription`'s `PartialEq` excludes `topic_id` — matches Java's `equals`.
- id-XOR-name modeled as a closed enum (`TopicCollection`) — Java's input is
  already a sealed id-XOR-name type; no lost behavior.
- `KafkaFuture::join_map` is a new combinator (no Java equivalent) required by
  the get-driven future model; error propagation matches `all_of`.

### Tests
- **Rust lib suite: 2117 passing** (24 in `admin::kafka_admin_client`).
- **Integration**: `tests/integration/admin_topics_test.rs` — 4/4 green against
  a real Kafka 4.2.0 broker via testcontainers (create→list/describe; describe
  nonexistent → `UNKNOWN_TOPIC_OR_PARTITION`; delete→gone; partition/RF
  round-trip).
- Translated `KafkaAdminClientTest` slices incl. describe-by-ids, quota-retry
  (WhenEnabled / DontRetryWhenDisabled, create+delete), NOT_CONTROLLER retry,
  partial-response, retry-backoff — with exact error-message assertions.
- `cargo build` / `cargo xtask format-check` / `cargo xtask lint`: clean.
- DoD #10 (hot-path allocation audit) N/A for Admin (batch/administrative, not
  per-record). One Critic review round: 7 findings (0 blocker, 4 should-fix, 3
  minor), all resolved; see `design/history/Milestone-11/Phase-1/COMMENTS.DONE.1.md`.

## Phase 2 — Partitions & records ✓ (2026-07-17)

Translated `create_partitions` and `delete_records`, Rust core + unit tests +
real-broker integration tests, all green.

- **`create_partitions`** (`CreatePartitionsRequest`, plain `Call` on the
  controller) and **`delete_records`**.
- **`AdminApiDriver` engine pulled forward from Phase 5**: Java's
  `delete_records` genuinely dispatches via `AdminApiDriver` +
  `PartitionLeaderStrategy` (not a plain `Call`), so the multi-step
  lookup→fulfillment dispatch engine was built now: `internals/{admin_api_driver,
  admin_api_handler, admin_api_lookup_strategy, admin_api_future,
  api_request_scope, partition_leader_strategy, partition_leader_cache,
  delete_records_handler}.rs`, plus `Call::set_maybe_retry_fn`/`MaybeRetryOutcome`
  and `NodeProvider::ConstantNodeId(i32)`. Runs on the existing single bg task.
  **This engine now also backs Tier 1 Phase 5 and all of Tier 2's
  CoordinatorStrategy / Tier 3.**
- **POJOs / Options / Results**: `NewPartitions`, `RecordsToDelete`,
  `DeletedRecords`, `{CreatePartitions,DeleteRecords}{Options,Result}`.
- **Wire wrappers**: `CreatePartitions`/`DeleteRecords` request+response.

### Accepted deviations (documented, Critic-approved)
- `ApiRequestScope` modeled as a closed enum (`SingleLookup | Fulfillment`) vs
  Java's open interface — sufficient for current RPCs; revisit at Tier 2's
  `CoordinatorStrategy` if a per-group scope key is needed.
- `Batched`/`Unbatched` handler split folded into one shape (batching preserved
  for `delete_records`).
- `testDeleteRecordsMultipleSends`: substituted a per-broker fatal partition
  error for Java's SASL-auth connection error (Rust `MockClient::authentication_error`
  always returns `None`); same multi-send code path exercised.
- Java's 917-line `AdminApiDriverTest` not translated wholesale; the driver's
  two defining branches (fulfillment→unmap→re-lookup, disconnect→retry-lookup)
  are covered by dedicated driver-level unit tests added in the fix cycle.

### Tests
- **Rust lib suite: 2179 passing.**
- **Integration**: `tests/integration/admin_partitions_records_test.rs` — 5/5
  green against a real broker (create-partitions increases count; decreasing
  fails; delete-records advances low-water-mark; offset-out-of-range fails;
  nonexistent-partition fails).
- `cargo build` / `format-check` / `lint`: clean. DoD #10 N/A.
- Critic: 1 should-fix (untested driver branches), resolved in fix cycle then
  re-verified clean; see `design/history/Milestone-11/Phase-2/COMMENTS.DONE.1.md`.

## Phase 3 — Cluster & configs ✓ (2026-07-17)

Translated `describe_cluster`, `describe_configs`, `incremental_alter_configs`,
`list_config_resources`, Rust core + unit tests + real-broker integration
tests, all green.

- **`describe_cluster`** (`DescribeClusterRequest`, `NodeProvider::LeastLoadedBrokerOrActiveKController`;
  `authorized_operations` decoded via `from_32_bit_field` + `AclOperation`,
  reused from Phase 1).
- **`describe_configs`** and **`incremental_alter_configs`** with Java's
  **per-resource-type routing**: broker / broker-logger resources route to that
  specific broker node, topic/other to the controller / least-loaded node.
- **`list_config_resources`** (`ListConfigResourcesRequest`) — this wire wrapper
  is reused later by Tier 3's `listClientMetricsResources`.
- **New types**: `common::config::ConfigResource` (+ its resource-type enum),
  `AlterConfigOp` (+ `OpType`), `Describe{Cluster,Configs}{Options,Result}`,
  `AlterConfigsOptions`/`AlterConfigsResult`, `ListConfigResources{Options,Result}`
  (reusing Phase 1's `Config`/`ConfigEntry`).
- **Wire wrappers**: `DescribeCluster`, `DescribeConfigs`, `IncrementalAlterConfigs`,
  `ListConfigResources` request+response.

### Tests
- **Rust lib suite: 2246 passing.**
- **Integration**: `tests/integration/admin_cluster_configs_test.rs` — 5/5 green
  against a real broker (describe-cluster nodes/controller/id; describe-configs
  broker + topic; list-config-resources; incremental-alter-configs SET+DELETE).
- `cargo build` / `format-check` / `lint`: clean. DoD #10 N/A.
- Critic: 2 should-fix (both `MockAdminClient` — `describe_cluster` timeout never
  decremented; three config methods wrongly stubbed as "unsupported" when Java's
  mock fully implements them), resolved in fix cycle then re-verified clean.
  `admin-client.md` §9 clarified (mock-unsupported rule applies only to methods
  Java's own mock leaves unimplemented). See
  `design/history/Milestone-11/Phase-3/COMMENTS.DONE.1.md`.

## Phase 4 — Log dirs ✓ (2026-07-17)

Translated `describe_log_dirs`, `alter_replica_log_dirs`, `describe_replica_log_dirs`,
Rust core + unit tests + real-broker integration tests, all green.

- **`describe_log_dirs`** (`DescribeLogDirsRequest`, per-broker fan-out — one
  `Call` per requested broker), **`alter_replica_log_dirs`**
  (`AlterReplicaLogDirsRequest`, replica→logdir assignments routed per
  destination broker), **`describe_replica_log_dirs`** (built on
  `DescribeLogDirsRequest`, reshaped into `ReplicaLogDirInfo` current/future dir).
- **New types**: `common::TopicPartitionReplica`, `LogDirDescription` +
  `ReplicaInfo`, `Describe{LogDirs,ReplicaLogDirs}Options`/`Result`,
  `AlterReplicaLogDirsOptions`/`Result`. Wire wrappers: `DescribeLogDirs`,
  `AlterReplicaLogDirs` request+response.

### Tests
- **Rust lib suite: 2289 passing.**
- **Integration**: `tests/integration/admin_log_dirs_test.rs` — 4/4 green against
  a real broker, including a genuine **cross-directory replica move** exercised
  via a dedicated broker fixture with two `KAFKA_LOG_DIRS` (not a documented
  limitation — a real move).
- `cargo build` / `format-check` / `lint`: clean. DoD #10 N/A.
- Critic: **clean on first pass — no fix cycle needed.** See
  `design/history/Milestone-11/Phase-4/COMMENTS.DONE.1.md`.

## Phase 5 — Elections, reassignment, offsets ✓ (2026-07-17)

Translated `elect_leaders`, `alter_partition_reassignments`,
`list_partition_reassignments`, `list_offsets`, Rust core + unit tests +
real-broker integration tests, all green. **This completes Tier 1.**

- **`elect_leaders`** (`ElectLeadersRequest`, controller `Call`),
  **`alter_partition_reassignments`** / **`list_partition_reassignments`**
  (controller `Call`, cancel-via-`Optional.empty` preserved), and **`list_offsets`**
  — the canonical `AdminApiDriver` + `PartitionLeaderStrategy` user, wired via a
  new `internals::ListOffsetsHandler` (lookup partition leader → per-leader
  `ListOffsetsRequest` fulfillment, leader-unmap + re-lookup on stale-leader).
- **Reuse (DoD #6)**: the existing Consumer-side `ListOffsetsRequest`/`Response`
  wire wrapper was reused, not duplicated. New: `common::ElectionType`,
  `admin::OffsetSpec`, `NewPartitionReassignment`, `PartitionReassignment`, the
  Options/Result types; wire wrappers `ElectLeaders`,
  `AlterPartitionReassignments`, `ListPartitionReassignments`.

### Tests
- **Rust lib suite: 2355 passing.**
- **Integration**: `tests/integration/admin_elections_reassignments_offsets_test.rs`
  — 3/3 green against a real broker, including a genuine **cross-broker partition
  reassignment** on a real 3-broker cluster (elect-preferred-leaders;
  list-offsets earliest/latest/max-timestamp; alter+list reassignments).
- `cargo build` / `format-check` / `lint`: clean. DoD #10 N/A.
- Critic: clean (2 LOW observations, both adjudicated non-blocking). See
  `design/history/Milestone-11/Phase-5/COMMENTS.DONE.1.md`.

## Tier 1 — COMPLETE ✓ (Phases 1–5)

All of Milestone 11 Tier 1 is implemented, tested (unit + real-broker
integration), and Critic-clean: **topics CRUD; partitions & records; cluster &
configs; log dirs; elections, reassignments & offsets.** The two admin dispatch
patterns are both in place and exercised — the plain `Call`/`NodeProvider` retry
engine and the multi-step `AdminApiDriver` + `PartitionLeaderStrategy` lookup→
fulfillment engine. Next: **Tier 2 (consumer groups & offsets)**, starting with
Phase 1 "Group listing & describe" (which lands the `ConsumerProtocol` /
`consumer-threading.md` §20 amendment prerequisite).

## Tier 2 Phase 1 — Group listing & describe ✓ (2026-07-29)

Translated `list_groups`, `list_consumer_groups` (deprecated),
`describe_consumer_groups` (dual-protocol), and `describe_classic_groups`,
Rust core + unit tests + real-broker integration tests, all green. **First
real use of `CoordinatorStrategy` and the shared broker-enumeration `Call`
idiom.**

- **`list_groups` / `list_consumer_groups`**: hand-rolled broker-enumeration —
  `LeastLoadedNodeProvider` metadata fetch, then one `Call` per broker with
  `ConstantNodeIdProvider` (NOT `AllBrokersStrategy`). Older-broker downgrade
  of the states/types filters preserved (CLASSIC-only filter omitted vs.
  CONSUMER/SHARE-only surfacing `UnsupportedVersion`).
- **`describe_consumer_groups`**: `AdminApiDriver` + `CoordinatorStrategy(GROUP)`,
  tries `ConsumerGroupDescribeRequest` (KIP-848) first and falls back per-group
  to `DescribeGroupsRequest` on `UNSUPPORTED_VERSION`/`GROUP_ID_NOT_FOUND`,
  preserving the more-informative CGD error message across the fallback.
- **`describe_classic_groups`**: `AdminApiDriver` (Batched) +
  `CoordinatorStrategy(GROUP)`, always `DescribeGroupsRequest`.
- **Prerequisites landed**: `ConsumerProtocol`
  (`src/consumer/internals/consumer_protocol.rs`, all public methods) +
  `ConsumerPartitionAssignor.{Assignment,Subscription}` data holders (assignor
  trait stays out of scope) — the documented `consumer-threading.md` §20
  carve-out for Admin (PLAN finding #3). `common::{GroupState, GroupType,
  ClassicGroupState, ConsumerGroupState}`.
- **New types**: `admin::{GroupListing, ConsumerGroupListing,
  ConsumerGroupDescription, ClassicGroupDescription, MemberDescription,
  MemberAssignment}`, the four `*Options`/`*Result` pairs,
  `internals::{CoordinatorKey, CoordinatorStrategy,
  DescribeConsumerGroupsHandler, DescribeClassicGroupsHandler}`. Wire wrappers
  (net new): `ListGroups`, `ConsumerGroupDescribe`, `DescribeGroups`
  request+response.

### Tests
- **Rust lib suite: 2497 passing.**
- **Integration**: `tests/integration/admin_groups_test.rs` — 3/3 green against
  a real 4.2.0 broker using a live KIP-848 consumer fixture (list/list_consumer
  surface the group as `Consumer`/`Stable`; describe reports the member owning
  all partitions; describe-nonexistent-group).
- `cargo build` / `format-check` / `lint`: clean. DoD #10 N/A (Admin is
  batch/administrative, no per-record hot path).
- Critic: 3 issues on first pass (all DoD test/completeness gaps — classic-group
  test coverage, list-groups filter/older-broker wiring, `ConsumerProtocol`
  missing methods), all fixed and re-verified clean on the fix cycle. See
  `design/history/Milestone-11/Tier2-Phase-1/COMMENTS.DONE.1.md`.

## Tier 2 Phase 2 — Group offsets ✓ (2026-07-29)

Translated `list_consumer_group_offsets`, `alter_consumer_group_offsets`,
`delete_consumer_group_offsets`, Rust core + unit tests + real-broker
integration tests, all green. All three are `CoordinatorStrategy(GROUP)`
handlers on the `AdminApiDriver`.

- **`list_consumer_group_offsets`**: batched multi-group `OffsetFetch` with
  per-group fallback; the batching-downgrade path wires
  `AdminApiLookupStrategy::disable_batch()` (defaulted trait method,
  overridden by `CoordinatorStrategy`) into `AdminApiDriver.on_failure`,
  triggered before the generic `UnsupportedVersion` branch on the
  `NoBatchedOffsetFetch`/`NoBatchedFindCoordinators` messages.
- **`alter_consumer_group_offsets`**: reuses `OffsetCommitRequest`.
- **`delete_consumer_group_offsets`**: **PLAN correction** — Kafka 4.2 does
  NOT use an `OffsetCommit` `-1` sentinel; it uses a dedicated `OffsetDelete`
  RPC (`ApiKeys.OFFSET_DELETE`=47, v0-only, non-flexible). Net-new wire wrapper
  `common::requests::OffsetDelete{Request,Response}` added with all
  `ConcreteRequest`/`ConcreteResponse` enum arms wired and byte-level
  known-vector tests.
- **Reuse (DoD #6)**: `OffsetFetchRequest`/`Response` and
  `OffsetCommitRequest`/`Response` reused from the Consumer side, not
  duplicated. New: `admin::{ListConsumerGroupOffsetsSpec,
  ListConsumerGroupOffsetsResult, AlterConsumerGroupOffsetsResult,
  DeleteConsumerGroupOffsetsResult}`, the three `*Options`, and the three
  `internals` handlers.

### Tests
- **Rust lib suite: 2562 passing.**
- **Integration**: `tests/integration/admin_group_offsets_test.rs` — 4/4 green
  against a real 4.2.0 broker (list matches committed; alter+restart resumes
  from altered offset; delete on inactive group removes it; delete on active
  group fails `GROUP_SUBSCRIBED_TO_TOPIC`).
- `cargo build` / `format-check` / `lint`: clean. DoD #10 N/A.
- Critic: 1 DoD #3 gap on first pass (`requireStable` option→wire propagation
  untested) fixed + re-verified clean; retriable-test fold documented. See
  `design/history/Milestone-11/Tier2-Phase-2/COMMENTS.DONE.1.md`.

## Tier 2 Phase 3 — Group / member deletion ✓ (2026-07-29)

Translated `delete_consumer_groups` and `remove_members_from_consumer_group`
(specific members or `remove_all`), Rust core + unit tests + real-broker
integration tests, all green. Both are `CoordinatorStrategy(GROUP)` handlers.
**This completes Tier 2.**

- **`delete_consumer_groups`**: `DeleteGroupsHandler` (base, real
  `DeleteGroups` work + `api_name`/`display_name`) + `DeleteConsumerGroupsHandler`
  thin-subclass factory — the Java abstract-base/subclass split modeled via
  composition.
- **`remove_members_from_consumer_group`**: `LeaveGroupRequest` keyed by
  `MemberIdentity`; `remove_all` first describes the group to enumerate members,
  then drives `LeaveGroup`. The describe step uses the client's default API
  timeout and the `LeaveGroup` driver deadline is recomputed fresh inside the
  describe-completion callback (matching Java's `whenComplete` → `invokeDriver`).
- **New wire wrappers (net new)**: `common::requests::DeleteGroups{Request,Response}`
  (apiKey 42), `LeaveGroup{Request,Response}` (apiKey 13, incl. `MemberIdentity`/
  `MemberResponse`), with byte-level known-vector tests for both non-flexible and
  flexible (v5, incl. the KIP-800 `Reason` field) framing. Reason truncation at
  255 chars ported as `common::requests::join_group_request::maybe_truncate_reason`
  (only that helper of `JoinGroupRequest` was needed; the full class is not yet
  translated).
- **New types**: `admin::{MemberToRemove, DeleteConsumerGroupsResult,
  RemoveMembersFromConsumerGroupResult}`, the two `*Options`, the three
  `internals` handlers. `MockAdminClient` returns "Not implemented yet"
  (faithful to Java's mock, which throws `UnsupportedOperationException`).
- `ExponentialBackoff` gained a `#[derive(Clone)]` (pure additive; reused across
  the two chained `remove_all` drivers).

### Tests
- **Rust lib suite: 2625 passing.**
- **Integration**: `tests/integration/admin_groups_test.rs` (extended) — green
  against a real 4.2.0 broker: delete empty group; delete live group →
  non-retriable `NON_EMPTY_GROUP`; remove one static member → reassignment;
  `remove_all` empties the group. (Admin `LeaveGroup` removal of KIP-848 members
  requires static members with `group.instance.id`.)
- `cargo build` / `format-check` / `lint`: clean. DoD #10 N/A.
- Critic: 2 issues on first pass (removeAll deadline behavior mismatch;
  LeaveGroup flexible-framing byte-test gap), both fixed + re-verified clean.
  See `design/history/Milestone-11/Tier2-Phase-3/COMMENTS.DONE.1.md`.

## Tier 2 — COMPLETE ✓ (Phases 1–3)

All of Milestone 11 Tier 2 is implemented, tested (unit + real-broker
integration), and Critic-clean: **group listing & describe; group offsets;
group/member deletion.** First real use of `CoordinatorStrategy(GROUP)` and the
`AdminApiDriver` coordinator-lookup engine. Next: **Tier 3** (ACLs, quotas,
SCRAM, delegation tokens, features, producers/transactions, client metrics),
starting with Phase 1 "ACLs".

## Tier 3 Phase 1 — ACLs ✓ (2026-07-29)

Translated `create_acls`, `describe_acls`, `delete_acls`, Rust core + unit
tests + real-broker integration tests, all green. All three are plain `Call`
RPCs (`LeastLoadedBrokerOrActiveKController`), independent of the rest of Tier 3.

- **ACL/resource primitives (net new)**: `common::acl::{AclBinding,
  AclBindingFilter, AccessControlEntry, AccessControlEntryFilter,
  AccessControlEntryData (pub(crate))}` and `common::resource::{Resource,
  ResourcePattern, ResourcePatternFilter, PatternType, ResourceType}`. Reused
  the Tier-2-landed `common::acl::{AclOperation, AclPermissionType}` (added only
  `Display` impls) and `common::utils::from_32_bit_field` — no duplication.
- **RPCs**: `describe_acls` short-circuits an unknown `AclBindingFilter` to an
  `InvalidRequest` future with NO `Call` enqueued (Java `KafkaAdminClient.java:2559`);
  `create_acls` rejects an indefinite binding per-binding (other valid bindings
  in the batch still succeed); `create_acls`/`delete_acls` translate
  `handleNotControllerError` (clear controller + request update + retry).
- **New types**: `admin::{Create,Describe,Delete}Acls{Options,Result}` incl.
  `DeleteAclsResult::{FilterResult, FilterResults}`. `MockAdminClient` returns
  "Not implemented yet" per key (faithful to Java's mock throwing
  `UnsupportedOperationException`). Wire wrappers (net new):
  `common::requests::{Create,Describe,Delete}Acls{Request,Response}` with all
  enum arms wired and hand-computed byte-level request vectors.
- **New test fixture**: `cluster_config::authorizer_single_broker()`
  (`StandardAuthorizer` + `KAFKA_SUPER_USERS=User:ANONYMOUS`) — the first
  authorizer-enabled broker fixture (finding #11).

### Tests
- **Rust lib suite: 2709 passing.**
- **Integration**: `tests/integration/admin_acls_test.rs` — 4/4 green against a
  real 4.2.0 broker with the authorizer fixture (create→describe round-trip;
  non-matching filter empty; delete→describe gone; DENY-rule authorizer
  round-trip). Note: a live `TopicAuthorizationException` end-to-end assertion
  isn't provocable because the only authenticatable principal (anonymous
  PLAINTEXT) is a super user — documented fixture limitation, adjudicated
  defensible.
- Six dedicated primitive test files translated 1:1 (`AclBindingTest`,
  `AclOperationTest`, `AclPermissionTypeTest`, `ResourcePatternFilterTest`,
  `ResourcePatternTest`, `ResourceTypeTest`); `AclBindingFilterTest`/
  `AccessControlEntry*Test`/`PatternTypeTest` don't exist in Java (not invented).
- `cargo build` / `format-check` / `lint`: clean. DoD #10 N/A.
- Critic: 1 LOW DoD #3 gap on first pass (`AclOperationTest`/`AclPermissionTypeTest`
  reduced to spot-checks) fixed + re-verified clean (exhaustive `INFOS`-table
  loops, all 16 ops + 4 perms pinned). See
  `design/history/Milestone-11/Tier3-Phase-1/COMMENTS.DONE.1.md`.

## Tier 3 Phase 2 — Client quotas ✓ (2026-07-29)

Translated `describe_client_quotas` and `alter_client_quotas`, Rust core + unit
tests + real-broker integration tests, all green. Both are plain `Call` RPCs
(`LeastLoadedNodeProvider`), independent of the rest of Tier 3.

- **Quota primitives (net new)**: `common::quota::{ClientQuotaEntity,
  ClientQuotaFilter, ClientQuotaFilterComponent, ClientQuotaAlteration (+ nested
  Op)}`. `ClientQuotaEntity.entries` is `HashMap<String, Option<String>>` —
  `None` faithfully represents Java's null entity name (the built-in DEFAULT
  quota entity), distinct from `Some("")` (an entity literally named `""`);
  round-trips null↔wire-null end-to-end.
- **Match-type tri-state**: a `ClientQuotaMatch { Exact(String), Default, Any }`
  enum models Java's `Optional<String> match` (justified DoD #7 helper); wire
  codes `EXACT=0` / `DEFAULT=1` / `SPECIFIED(any)=2` verified against the spec.
- **Removal semantics**: `ClientQuotaAlteration.Op.value: Option<f64>`, `None` =
  removal → encoded `remove=true` + `value=0.0`; `Some(0.0)` (set-to-zero) stays
  distinct from removal on the wire.
- **New types**: `admin::{Describe,Alter}ClientQuotas{Options,Result}`. Wire
  wrappers (net new): `common::requests::{Describe,Alter}ClientQuotas{Request,Response}`
  with all enum arms wired and hand-computed byte-level vectors. `MockAdminClient`
  returns "Not implement yet" (Java mock's exact typo preserved) per §9.

### Tests
- **Rust lib suite: 2747 passing.**
- **Integration**: `tests/integration/admin_quotas_test.rs` — 3/3 green against a
  real 4.2.0 broker (alter→describe byte-rate round-trip; `Op(None)` removal no
  longer reported; entity-type filter returns only matching entities).
- No dedicated Java per-class test files exist for the four `common/quota/*`
  classes (upstream gap, recorded per DoD #3) — compensated with labeled
  "new test, no Java original" equals/hashCode/round-trip POJO tests.
- `cargo build` / `format-check` / `lint`: clean. DoD #10 N/A.
- Critic: 1 Behavior Mismatch on first pass (`ClientQuotaEntity` couldn't
  represent the null/default entity name — wire-incompatible) fixed +
  re-verified clean. See
  `design/history/Milestone-11/Tier3-Phase-2/COMMENTS.DONE.1.md`.

> **Tier 3 Phase 3 (SCRAM credentials) is DEFERRED** (2026-07-29): it needs a
> new PBKDF2 crypto crate (`Cargo.toml` change, CLAUDE.md §1.2) not yet
> approved. Phases 4–7 (which need no new crate) proceed first; SCRAM resumes
> on explicit crate approval.

## Tier 3 Phase 4 — Delegation tokens ✓ (2026-07-29)

Translated `create_delegation_token`, `renew_delegation_token`,
`expire_delegation_token`, `describe_delegation_token`, Rust core + unit tests,
all green. All four are plain `Call` RPCs (`LeastLoadedNodeProvider`),
independent of other Tier 3 phases.

- **Prerequisites (net new)**: `common::security::auth::KafkaPrincipal`
  (`USER_TYPE`, `ANONYMOUS`, eq/hash by type+name),
  `common::security::token::delegation::{DelegationToken, TokenInformation}`.
  `DelegationToken::hmac_as_base64_string` uses a from-scratch RFC 4648 standard
  (padded, `+/`) base64 encoder (the existing uuid encoder is URL-safe).
- **New types**: `admin::{Create,Renew,Expire,Describe}DelegationToken{Options,Result}`.
  Wire wrappers (net new): the four `common::requests::*DelegationToken{Request,Response}`
  with all enum arms wired and hand-computed byte-level vectors (raw HMAC bytes +
  principal strings). Deprecated Java `maxlifeTimeMs` aliases ported as
  `#[deprecated]` for DoD #2 completeness.
- **`MockAdminClient`**: FULL in-memory logic translated faithfully from Java's
  mock (finding #9/#10 — Java's mock implements these, so they are NOT stubbed):
  in-memory token list, `USER_TYPE` renewer validation, `-1` expire sentinel,
  owners describe-filter.

### Tests
- **Rust lib suite: 2808 passing.**
- **Unit tests are NEW, written against `MockAdminClient` as the behavioral
  reference (finding #10 — there are ZERO Java client-side delegation-token unit
  tests)**: non-`User` renewer → `InvalidPrincipalType`; unknown-HMAC renew/expire
  → `DelegationTokenNotFound`; `-1` expire removes; owners filter; plus
  byte-level wire vectors for all four RPCs and `KafkaPrincipalTest` coverage.
- **Integration deferred (documented)**: delegation-token creation requires a
  SASL-authenticated connection, but `AdminClientConfig`/`from_config` have no
  SASL support yet (hardcode `PLAINTEXT`) — SASL-in-admin-client is a separate
  feature. The behavioral contract is covered by the mock unit tests per finding
  #10. Nothing broken registered in `main.rs`.
- `cargo build` / `format-check` / `lint`: clean. DoD #10 N/A.
- Critic: **CLEAN on first pass — no fix cycle needed** (MockAdminClient fidelity
  and base64 encoder both verified). See
  `design/history/Milestone-11/Tier3-Phase-4/COMMENTS.DONE.1.md`.

## Tier 3 Phase 5 — Features ✓ (2026-07-29)

Translated `describe_features` and `update_features`, Rust core + unit tests +
real-broker integration tests, all green. Both plain `Call` RPCs, independent of
other Tier 3 phases.

- **`describe_features`**: reuses the existing `ApiVersionsRequest`/`Response`
  wrapper (zero new wire type; DoD #6). Node provider `ConstantNodeIdProvider`
  when `options.node_id()` is set, else `LeastLoadedBrokerOrActiveKController`.
  Decodes supported + finalized features + epoch into `FeatureMetadata`.
- **`update_features`**: controller `Call` (`ControllerNodeProvider`, NOT_CONTROLLER
  → clear-controller + refresh + retry); net-new `UpdateFeaturesRequest`/`Response`
  wire wrapper (all enum arms wired; hand-computed byte vectors; v0 `allow_downgrade`
  vs v1+ `upgrade_type`, v2 empty per-feature results). Returns
  `Result<UpdateFeaturesResult, KafkaError>` — the one admin RPC with this shape,
  because Java `updateFeatures` throws `IllegalArgumentException` SYNCHRONOUSLY
  (pre-enqueue) for empty/blank inputs, and `FeatureUpdate::new` throws for the
  deletion-without-downgrade-flag case (CLAUDE.md §10.2). Critic-confirmed faithful.
- **New types**: `admin::{FeatureMetadata, FeatureUpdate (+ UpgradeType:
  UPGRADE/SAFE_DOWNGRADE/UNSAFE_DOWNGRADE/UNKNOWN), FinalizedVersionRange,
  SupportedVersionRange, Describe/UpdateFeatures{Options,Result}}`.
- **`MockAdminClient`**: REAL upgrade/downgrade version-bounds validation
  translated faithfully (finding #9 — the mock is NOT a stub here), seeded
  feature-level maps, `InvalidRequestException`-wrapped errors.

### Tests
- **Rust lib suite: 2857 passing.**
- Full 1:1 parity on all 10 Java `KafkaAdminClientTest` feature slices:
  `testUpdateFeatures*` (`@ValueSource(shorts={1,2})` → loop over `[1,2]`;
  Success/TopLevelError/HandleNotControllerException/ShouldFailForEmptyUpdates/
  ForInvalidFeatureName/WhenDowngradeFlagNotSetDuringDeletion with exact
  client-side messages) and `testDescribeFeatures{Success,Failure,WithNodeSuccess,
  WithNodeFailure}`; plus POJO tests, byte-level `UpdateFeatures` vectors, and
  mock validation tests.
- **Integration**: `tests/integration/admin_features_test.rs` — 2/2 green against
  a real 4.2.0 broker: (a) `describe_features` sane finalized/supported range;
  (b) `update_features` beyond max rejected. A real upgrade case is out of scope
  (feature levels are cluster-wide/persistent).
- `cargo build` / `format-check` / `lint`: clean. DoD #10 N/A.
- Critic: **CLEAN on first pass — no fix cycle needed** (Result-signature
  faithfulness and mock validation fidelity both verified). See
  `design/history/Milestone-11/Tier3-Phase-5/COMMENTS.DONE.1.md`.

## Tier 3 Phase 6 — Producers & transactions ✓ (2026-07-29)

RPCs: `describe_producers`, `abort_transaction`, `describe_transactions`,
`fence_producers`, `list_transactions`, `force_terminate_transaction`. The
largest remaining Tier-3 phase; Rust core sub-sliced into 3 commits
(describeProducers+abortTransaction; describeTransactions+fenceProducers;
listTransactions+forceTerminateTransaction).

- New POJOs/results/options: `ProducerState`, `TransactionState`,
  `TransactionDescription`, `TransactionListing`, `AbortTransactionSpec`, and the
  `Describe{Producers,Transactions}`/`Abort/Terminate/Fence/ListTransactions`
  `{Options,Result}` families.
- New internals handlers: `Describe{Producers,Transactions}Handler`,
  `AbortTransactionHandler`, `FenceProducersHandler`, `ListTransactionsHandler`.
- New lookup strategies (first use anywhere): `StaticBrokerStrategy` (fallback
  when `describeProducers` sets a broker id) and `AllBrokersStrategy` (fan-out to
  all brokers via dynamic per-broker key discovery, backing `listTransactions`).
- Reused: `CoordinatorStrategy` generic over `CoordinatorType::Transaction`
  (describeTransactions/fenceProducers); `PartitionLeaderStrategy`
  (describeProducers/abortTransaction). Wire prerequisites landed in `f03a133`.
- `forceTerminateTransaction` delegates to `fenceProducers` (matches Java).

### Design-gap verdict — AllBrokersStrategy FITS CLEANLY
No Tier-1 foundation change was needed. Dynamically-discovered per-broker keys
flow through `LookupResult` (`completed_keys` sentinel + one `mapped_keys` entry
per broker); the `AdminApiDriver` already fulfills them. Proven end-to-end by the
translated `AllBrokersStrategyTest` + `AllBrokersStrategyIntegrationTest`.

### Tests
- **Rust lib suite: 2984 passing, 0 failed.**
- 1:1 `KafkaAdminClientTest` slices (`testDescribeProducers*` incl.
  `Timeout(boolean)`→loop and `RetryAfterDisconnect`, `testDescribeTransactions*`,
  `testAbortTransaction*`, `testForceTerminateTransaction*`, `testListTransactions`,
  `testFenceProducers`), `ListTransactionsResultTest`, and all internals handler
  tests. Byte-level wire vectors for the new request/response types.
- **Integration**: `tests/integration/admin_transactions_test.rs` — 5 green / 1
  ignored against a real 4.2.0 broker, verified running the FULL file together at
  both `--test-threads=1` and `--test-threads=2` (list_transactions
  AllBrokersStrategy debut; describe_producers; describe_transactions unknown-id
  error; fence_producers + force_terminate PID allocation). Ongoing-transaction
  scenarios kept as one `#[ignore]`d skeleton — no transactional producer API
  exists in the Rust `Producer` trait yet.
  - Post-handoff fixup `2105176`: `test_list_transactions_returns_empty_when_none_active`
    was non-hermetic (asserted GLOBAL emptiness while fence/force-terminate siblings
    sharing the pooled broker left persistent `Empty`-state transactional IDs). Fixed
    by giving it a dedicated isolated `ClusterConfig`. RPC was correct; test-only fix.
- Documented skip: `ListTransactionsHandlerTest.testBuildRequestWithDurationFilter`
  case 3 — the message generator omits below-min-version fields rather than
  throwing `UnsupportedVersion` (pre-existing generator-wide behavior; worth a
  separate follow-up ticket). Critic-confirmed legitimate.
- `cargo build` / `format-check` / `lint`: clean. DoD #10 N/A (not a hot path).
- Critic: **CLEAN on first pass — no fix cycle needed.** See
  `design/history/Milestone-11/Tier3-Phase-6/` (`REVIEW.md` + `COMMENTS.DONE.1.md`).

## Tier 3 Phase 7 — Client metrics ✓ (2026-07-29)

RPC: `listClientMetricsResources(ListClientMetricsResourcesOptions)`.

- New: `ClientMetricsResourceListing` (name-only POJO, `Display` = Java
  `toString`), `ListClientMetricsResourcesResult` (`all()` →
  `KafkaFuture<Vec<ClientMetricsResourceListing>>`),
  `ListClientMetricsResourcesOptions`.
- Reuses the Tier-1 `ListConfigResourcesRequest.Builder` filtered to
  `ConfigResourceType::ClientMetrics.id()` (id 16); response filtered to
  CLIENT_METRICS and mapped `.name()` → listing. **Zero new wire types**
  (matches `KafkaAdminClient.java` ~4922). Plain sync `fn` on the trait.
- `MockAdminClient` got the real Java in-memory logic (finding #9, not a stub):
  maps `client_metrics_configs.keys()` → listings, mirroring
  `MockAdminClient.java` ~1431.

### Tests
- **Rust lib suite: 2993 passing, 0 failed.**
- All three `KafkaAdminClientTest` methods 1:1 (`testListClientMetricsResources`,
  `...Empty`, `...NotSupported` with exact `UnsupportedVersion` +
  `"The version of API is not supported."` assertions). Not conflated with the
  Tier-1 `testDescribeClientMetricsConfigs`.
- **Integration**: `tests/integration/admin_cluster_configs_test.rs` — new test
  creates a real KIP-714 client-metrics subscription, lists it, asserts, deletes;
  green against apache/kafka:4.2.0.
- `cargo build` / `format-check` / `lint`: clean. DoD #10 N/A.
- Critic: **CLEAN on first pass — no fix cycle needed.** See
  `design/history/Milestone-11/Tier3-Phase-7/` (`REVIEW.md` + `COMMENTS.DONE.1.md`).

## Tier 3 Phase 3 — SCRAM credentials ✓ (2026-07-30)

RPCs: `describeUserScramCredentials` (`LeastLoadedNodeProvider`),
`alterUserScramCredentials` (`ControllerNodeProvider`). The final in-scope phase.

- Crypto dependency (CLAUDE.md §1.2, user-approved OPTION A): `aws-lc-rs`, declared
  `default-features = false, features = ["aws-lc-sys","alloc"]` so the normal-edges
  dependency graph is byte-identical to before — **zero new compiled crates**
  (aws-lc-rs was already in-tree via rustls). Critic-verified.
- `ScramFormatter::hi()` = RFC 5802 `Hi` via `aws_lc_rs::pbkdf2::derive`
  (SHA-256→32B, SHA-512→64B, single output block). Narrow translation — NOT a full
  SASL/SCRAM client. Internal + admin `ScramMechanism`, `ScramCredentialInfo`,
  `UserScramCredential{Alteration,Upsertion,Deletion}` (abstract base → closed enum),
  `UserScramCredentialsDescription`, both `{Options,Result}`, and net-new
  `{Describe,Alter}UserScramCredentials` wire types.
- `MockAdminClient` returns `KafkaError::unsupported_version("Not implemented yet")`
  for both (finding #9 — Java's mock throws `UnsupportedOperationException`); unit
  tests run against the network-mocked `KafkaAdminClient`.

### Tests
- **Rust lib suite: 3029 passing, 0 failed.**
- 1:1: `testDescribeUserScramCredentials`, `testAlterUserScramCredentialsUnknownMechanism`,
  `testAlterUserScramCredentials` (real PBKDF2), `ScramMechanismTest`,
  `DescribeUserScramCredentialsResultTest`. Byte-level wire vectors for both new types.
- **Crypto byte-vector test** (the phase's most important): `hi()` asserted against
  RFC 7914 §11 (SHA-256, c=1) and a Python-hashlib cross-check (SHA-512, c=4096);
  Critic INDEPENDENTLY recomputed both — exact match. Covers the INT(1) big-endian
  counter and the iteration loop.
- **Integration**: `tests/integration/admin_scram_test.rs` — upsert SCRAM-SHA-256
  (it=8192) → describe (mechanism+iterations round-trip; never the salted password)
  → delete → assert gone; self-scoped username (global-state-hazard compliant); green
  against apache/kafka:4.2.0. SASL-auth step scoped out (no SASL client on this
  branch — same gap as Phase 4), documented.
- `cargo build` / `format-check` / `lint`: clean. DoD #10 N/A.
- Critic: **CLEAN on first pass — no fix cycle needed.** See
  `design/history/Milestone-11/Tier3-Phase-3/` (`REVIEW.md` + `COMMENTS.DONE.1.md`).

## Milestone 11 (AdminClient) — IN-SCOPE WORK COMPLETE ✓ (2026-07-30)

All 46/46 in-scope Admin RPCs translated: Tier 1 (17), Tier 2 (9), Tier 3
Phases 1–7 (20). Rust core + unit tests + real-broker integration throughout; every
phase Critic-reviewed to clean. 3029 lib tests passing.

**Out of scope (unchanged):** C FFI / Python bindings — a separate future task
(reuse the async dispatcher in PR #116); and Tier 4 — Streams groups, Share
groups/KIP-932, KRaft raft-voter admin (`addRaftVoter`/`removeRaftVoter`/
`describeMetadataQuorum`/`unregisterBroker`), and `ForwardingAdmin` (broker-plugin
delegate). Deferred for the reasons in `design/history/Milestone-11/PLAN.md`.
