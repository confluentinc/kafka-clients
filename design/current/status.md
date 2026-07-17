# Current Status: Milestone 3 Complete (SSL + SASL Authentication)

Milestone 1 (8 layers) + Milestone 3 (6 phases) complete. SSL/TLS encryption and SASL PLAIN authentication fully implemented with integration tests against real Kafka 4.2.0 broker. 454+ unit tests + 16 integration tests passing.

## Completed Components

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
