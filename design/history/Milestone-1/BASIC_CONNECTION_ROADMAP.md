# Basic Kafka Connection Implementation Roadmap

## Overview

This document outlines the minimal set of classes needed to implement basic TCP connection to Kafka brokers and send/receive requests without SSL/SASL authentication.

## Status: COMPLETE

All 8 layers implemented and verified with 447 unit tests + 11 integration tests against Kafka 4.2.0.

## Critical Path Implementation (8 Layers)

### Layer 1 — Core Protocol Types (5 classes) ✓ COMPLETE
Foundation types representing Kafka cluster entities.

- ✓ `Node` (`common/node.rs`) — Kafka broker representation
- ✓ `TopicPartition` (`common/topic_partition.rs`) — Topic/partition identifier
- ✓ `Cluster` (`common/cluster.rs`) — Cluster metadata with node collection
- ✓ `ApiKeys` (`common/protocol/api_keys.rs`) — Enum of all Kafka API request types
- ✓ `Errors` (`common/protocol/errors.rs`) — 134 Kafka error codes with retriable/fatal classification

### Layer 2 — Wire Protocol (5 classes) ✓ COMPLETE
Binary serialization/deserialization framework.

- ✓ `Readable` (`common/protocol/readable.rs`) — Trait for reading wire protocol data
- ✓ `Writable` (`common/protocol/writable.rs`) — Trait for writing wire protocol data
- ✓ `ByteBufferAccessor` (`common/protocol/byte_buffer_accessor.rs`) — Buffer implementation for Readable/Writable
- ✓ `Message` (`common/protocol/message.rs`) — Core versioned serialization trait
- ✓ `ApiMessage` (`common/protocol/message.rs`) — Extends Message with api_key()

### Layer 3 — Request/Response Framework (8 classes) ✓ COMPLETE
High-level request/response abstraction.

- ✓ `RequestHeader` (`common/requests/request_header.rs`) — Request header with correlation ID, client ID
- ✓ `ResponseHeader` (`common/requests/response_header.rs`) — Response header with correlation ID
- ✓ `AbstractRequest` (`common/requests/abstract_request.rs`) — ConcreteRequest enum dispatching to all request types
- ✓ `AbstractResponse` (`common/requests/abstract_response.rs`) — ConcreteResponse enum dispatching to all response types
- ✓ `ApiVersionsRequest` (`common/requests/api_versions_request.rs`) — Query broker API versions
- ✓ `ApiVersionsResponse` (`common/requests/api_versions_response.rs`) — Broker API version information
- ✓ `MetadataRequest` (`common/requests/metadata_request.rs`) — Request cluster metadata
- ✓ `MetadataResponse` (`common/requests/metadata_response.rs`) — Cluster metadata (topics, partitions, leaders)

### Layer 4 — Network Transport (7 classes) ✓ COMPLETE
Low-level TCP I/O and framing.

- ✓ `TransportLayer` (`common/network/transport_layer.rs`) — Async transport trait
- ✓ `PlaintextTransportLayer` (`common/network/plaintext_transport_layer.rs`) — TCP transport with Tokio
- ✓ `Send` (`common/network/send.rs`) — Trait for outgoing data
- ✓ `Receive` (`common/network/receive.rs`) — Trait for incoming data
- ✓ `NetworkSend` (`common/network/network_send.rs`) — Node-addressed outgoing frame
- ✓ `NetworkReceive` (`common/network/network_receive.rs`) — Node-addressed incoming frame
- ✓ `ByteBufferSend` (`common/network/byte_buffer_send.rs`) — Buffer-backed send implementation

### Layer 5 — Channel & Selection (5 classes) ✓ COMPLETE
Non-blocking I/O channel management (Java NIO Selector → Tokio).

- ✓ `Selectable` (`common/network/selectable.rs`) — Selector trait for testability
- ✓ `Selector` (`common/network/selector.rs`) — Non-blocking I/O multiplexer
- ✓ `KafkaChannel` (`common/network/kafka_channel.rs`) — Per-connection state machine
- ✓ `ChannelBuilder` (`common/network/channel_builder.rs`) — Factory trait for creating channels
- ✓ `PlaintextChannelBuilder` (`common/network/plaintext_channel_builder.rs`) — Plaintext channel factory

### Layer 6 — Client Infrastructure (11 classes) ✓ COMPLETE
High-level client connection and request management.

- ✓ `KafkaClient` (`clients/kafka_client.rs`) — KafkaClient trait
- ✓ `NetworkClient` (`clients/network_client.rs`) — Main KafkaClient implementation with DefaultMetadataUpdater
- ✓ `ClientRequest` (`clients/client_request.rs`) — Wrapper for outgoing requests
- ✓ `ClientResponse` (`clients/client_response.rs`) — Wrapper for received responses
- ✓ `InFlightRequests` (`clients/in_flight_requests.rs`) — Track pending requests with send_completed flag
- ✓ `ClusterConnectionStates` (`clients/cluster_connection_states.rs`) — Connection state machine with exponential backoff
- ✓ `Metadata` (`clients/metadata.rs`) — Thread-safe metadata cache with epoch tracking
- ✓ `MetadataSnapshot` (`clients/metadata_snapshot.rs`) — Immutable cluster metadata snapshot
- ✓ `NodeApiVersions` (`clients/node_api_versions.rs`) — Per-node API version information
- ✓ `ApiVersions` (`clients/api_versions.rs`) — Thread-safe API version registry
- ✓ `NetworkClientUtils` (`clients/network_client_utils.rs`) — Blocking utility functions
- ✓ `MockSelector` (`common/network/mock_selector.rs`) — Test-only mock for Selector

### Layer 8 — Integration Tests ✓ COMPLETE
End-to-end verification against Kafka 4.2.0 in Docker.

- ✓ Test infrastructure: ClusterConfig, ClusterPool, KafkaCluster, TestContext
- ✓ `integration_connection_test.rs` — TCP connect, ApiVersions handshake, full flow (3 tests)
- ✓ `integration_api_versions_test.rs` — Error checking, expected APIs, version ranges (4 tests)
- ✓ `integration_metadata_test.rs` — Brokers, controller, specific topic, all topics (4 tests)
- ✓ Container cleanup via atexit hook (`docker rm -f`)

## Implementation Phases (all complete)

### Phase 1: Wire Protocol Foundation ✓
Layers 1-2 + code generator. 197 message types auto-generated from JSON specs.

### Phase 2: Network Transport ✓
Layers 4-5. Java NIO Selector → Tokio with non-blocking I/O.

### Phase 3: Request/Response Framework ✓
Layer 3. ConcreteRequest/ConcreteResponse enums, request builders, response parsers.

### Phase 4: Client Infrastructure ✓
Layer 6. NetworkClient, Metadata, ClusterConnectionStates, InFlightRequests.

### Phase 5: Integration Testing ✓
Layer 8. Docker-based tests against Kafka 4.2.0 verifying end-to-end:
- Connect to broker via TCP
- Send ApiVersionsRequest, receive and parse ApiVersionsResponse
- Send MetadataRequest, receive and parse MetadataResponse
- Verify broker information, controller, topic metadata

## Test Coverage

- **447 unit tests** — comprehensive coverage matching Java test suites
- **11 integration tests** — feature-gated (`--features integration-tests`), require Docker
- **36 Critic review issues** resolved across multiple review rounds

## Rust-Specific Adaptations

### Java NIO → Tokio Mapping
- `java.nio.channels.Selector` → `Selector` using Tokio `readable().await` + `try_read()`
- `java.nio.channels.SocketChannel` → `tokio::net::TcpStream`
- `java.nio.ByteBuffer` → `Vec<u8>` with position tracking

### Concurrency
- Java callbacks → `Box<dyn FnOnce(&mut ClientResponse) + Send>`
- `CompletableFuture` → `tokio::spawn` for detached async work
- Java inner classes → separate structs or inlined fields
- Thread safety: `Mutex<MetadataInner>`, `RwLock` for ApiVersions

### Error Handling
- Java checked exceptions → `Result<T, KafkaError>`
- Retriable errors → `KafkaError::is_retriable()`
- Fatal errors → `KafkaError::is_fatal()`
