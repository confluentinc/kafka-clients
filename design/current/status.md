# Current Status: Milestones 1-10 complete; Milestone 11 in progress

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

  - `test_too_large_batches_are_safely_removed` — §9.18, the
    split-on-`MESSAGE_TOO_LARGE` panic on the write path.
  - `test_transactional_unknown_producer_handling_when_retention_limit_reached` —
    §9.25, an empty batch pool on the transactional log-truncation retry.
  - `test_init_producer_id_request_versions` — §9.1, the code generator omitting
    Java's non-default-at-unsupported-version guard. Systemic across all 197
    generated message types, so it predates Milestone 11.

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
