# Current Status: Milestone 1 Complete (Layers 1-8)

All 8 layers complete: core types, wire protocol, 197 generated message types, request/response framework, network transport, selector/channel, network client, and integration tests against a real Kafka 4.2.0 broker. 447 unit tests + 11 integration tests passing.

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
- **447 unit tests** + **11 integration tests** (458 total) all passing
- Integration tests run against Kafka 4.2.0 in Docker (feature-gated: `--features integration-tests`)
- Comprehensive coverage matching Java test suites
- 36 Critic review issues resolved across multiple review rounds

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
