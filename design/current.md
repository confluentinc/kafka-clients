# Current Design

This document captures the current architecture and design of the Confluent Kafka Rust client.
It is intended to be updated after each manager agent loop completes a milestone or phase.

**Last updated:** 2026-04-10
**Status:** Milestone 1 complete, Milestone 3 (SSL/SASL) in progress

---

## Overview

A Rust Kafka client translated from the Java Kafka client (Apache Kafka 4.2), preserving the same
architecture and logical structure while adapting to Rust idioms. All I/O is async via Tokio.

**Crate stats:** ~100 source files, ~13,000 lines of library code, 447 unit tests + 11 integration tests.

---

## Layered Architecture

```
+-----------------------------------------------------------+
|  Client Layer (src/clients/)                              |
|  KafkaClient trait, NetworkClient<S, H>,                  |
|  Metadata, ApiVersions, InFlightRequests                  |
+-----------------------------------------------------------+
                          |
+-----------------------------------------------------------+
|  Request/Response Layer (src/common/requests/)            |
|  RequestBuilder trait, ConcreteRequest/ConcreteResponse,   |
|  RequestHeader, ResponseHeader, SendBuilder               |
+-----------------------------------------------------------+
                          |
+-----------------------------------------------------------+
|  Protocol Layer (src/common/protocol/)                    |
|  Readable/Writable traits, Message trait,                 |
|  ByteBufferAccessor, varint encoding                      |
+-----------------------------------------------------------+
                          |
+-----------------------------------------------------------+
|  Network Layer (src/common/network/)                      |
|  Selector (Selectable trait), KafkaChannel,               |
|  TransportLayer trait, ChannelBuilder trait,               |
|  Authenticator trait                                       |
+-----------------------------------------------------------+
                          |
+-----------------------------------------------------------+
|  Security Layer (src/common/security/, config/)           |
|  SecurityProtocol, SslFactory, SslConfig, SaslConfig      |
+-----------------------------------------------------------+
```

---

## Network Stack

### TransportLayer trait (`common/network/transport_layer.rs`)

Core abstraction bridging Java NIO and Tokio async I/O. Object-safe trait
(`Box<dyn TransportLayer>`) for runtime dispatch.

**Key methods:** `read`, `write`, `write_vectored`, `handshake`, `finish_connect`, `disconnect`

**Implementations:**
- `PlaintextTransportLayer` -- wraps `tokio::net::TcpStream`
- `SslTransportLayer` -- wraps `tokio_rustls::TlsStream<TcpStream>`, state machine: Handshaking -> Ready -> Closed

### Selector (`common/network/selector.rs`)

Multiplexes I/O across multiple `KafkaChannel` instances. Implements the `Selectable` trait.

**NIO to Tokio mapping:**
- Java `Selector.select()` -> sequential try_read/try_write + tokio timeout
- Java `SelectionKey` -> eliminated; channels keyed by string ID in HashMap
- Wakeup via `tokio::sync::Notify`
- Idle timeout via `IdleExpiryManager`

**Poll cycle:**
1. Iterate all channels, attempt non-blocking I/O (connect/read/write)
2. If no progress and timeout > 0, sleep with `Notify` wakeup
3. Return completed sends/receives and disconnected channels

### KafkaChannel (`common/network/kafka_channel.rs`)

Per-connection state machine combining transport, authentication, and I/O buffers.

**Contains:**
- `Box<dyn TransportLayer>` -- the underlying transport
- `Box<dyn Authenticator>` -- authentication handler
- `Option<NetworkSend>` / `Option<NetworkReceive>` -- I/O buffers
- `ChannelState` -- lifecycle: NotConnected -> Authenticate -> Ready -> LocalClose
- `ChannelMuteState` -- read muting for backpressure

### ChannelBuilder trait (`common/network/channel_builder.rs`)

Factory for creating `KafkaChannel` instances from a `TcpStream`.

**Implementations:**
- `PlaintextChannelBuilder` -- creates plaintext channels
- `SslChannelBuilder` -- creates SSL/TLS channels with rustls

### Network I/O Primitives

- `KafkaSend` trait (`send.rs`) -- async send; impl: `ByteBufferSend`
- `Receive` trait (`receive.rs`) -- async receive; impl: `NetworkReceive`
- `NetworkSend` (`network_send.rs`) -- wraps `KafkaSend` with destination ID
- `NetworkReceive` (`network_receive.rs`) -- size-delimited receive (4-byte header + payload)

---

## Protocol Framework

### Serialization Traits (`common/protocol/`)

- `Readable` -- deserialize Kafka types from byte streams
- `Writable` -- serialize Kafka types to byte streams
- `ByteBufferAccessor` -- concrete impl of both traits over `&mut [u8]`
- `Message` -- versioned serialization: `size()`, `add_size()`, `write()`, `read()`
- `ApiMessage` -- extends Message with `api_key()`

Two-pass serialization: calculate size first, then serialize.

### Wire Format

- Big-endian for all multi-byte integers
- Varint/varlong: Protocol Buffers unsigned and zig-zag signed encoding
- Strings: i16 length prefix (standard) or varint(len+1) (flexible)
- Bytes: i32 length prefix (standard) or varint(len+1) (flexible)
- Arrays: i32 length prefix (standard) or varint(len+1) (flexible)
- Tagged fields at end of struct in flexible versions

---

## Request/Response Framework (`common/requests/`)

### RequestBuilder trait (`abstract_request.rs`)

Constructs requests at a specific API version. Replaces Java's `AbstractRequest.Builder`.

Methods: `api_key()`, `oldest_allowed_version()`, `latest_allowed_version()`, `build()`, `build_version()`

### Dispatch Enums

- `ConcreteRequest` -- enum variants: ApiVersions, Metadata, SaslHandshake, SaslAuthenticate
- `ConcreteResponse` -- matching response enum

### Implemented Request Types

| Request | Response | Purpose |
|---------|----------|---------|
| `ApiVersionsRequest` | `ApiVersionsResponse` | API version negotiation |
| `MetadataRequest` | `MetadataResponse` | Cluster metadata discovery |
| `SaslHandshakeRequest` | `SaslHandshakeResponse` | SASL mechanism negotiation |
| `SaslAuthenticateRequest` | `SaslAuthenticateResponse` | SASL authentication exchange |

### Supporting Types

- `RequestHeader` / `ResponseHeader` -- Kafka frame headers
- `SendBuilder` -- constructs `NetworkSend` from header + body (zero-copy)

---

## Client Layer (`src/clients/`)

### KafkaClient trait (`kafka_client.rs`)

Defines the client interface: `is_ready`, `ready`, `send`, `poll`, `disconnect`,
`least_loaded_node`, `in_flight_request_count`, etc.

### NetworkClient<S: Selectable, H: HostResolver> (`network_client.rs`)

Core async client implementation, generic over Selectable (for test injection via MockSelector)
and HostResolver.

**Manages:**
- `InFlightRequests` -- per-destination request queue
- `ClusterConnectionStates<H>` -- connection state machine with exponential backoff
- `ApiVersions` -- cached per-node API version registry
- Metadata updates and recovery

### Connection State Machine

```
Disconnected -> Connecting -> CheckingApiVersions -> Ready
                    \                                  /
                     \-> AuthenticationFailed ---------/
```

### Request/Response Flow

1. `NetworkClient::send(ClientRequest)` -- queues request
2. `RequestBuilder::build_version()` -> `ConcreteRequest`
3. `Message::write()` -> bytes, wrapped in `SendBuilder` -> `ByteBufferSend` -> `NetworkSend`
4. `Selector::poll()` drives I/O via `KafkaChannel` -> `TransportLayer`
5. Response: `TransportLayer::read()` -> `NetworkReceive` -> `Message::read()` -> `ClientResponse`
6. `RequestCompletionHandler` callback invoked

### Supporting Types

- `ClientRequest` / `ClientResponse` -- request/response wrappers with metadata
- `RequestCompletionHandler` -- `Box<dyn FnOnce(&mut ClientResponse) + Send>`
- `Metadata` -- thread-safe metadata cache with epoch tracking
- `MetadataSnapshot` -- immutable cluster metadata snapshot
- `NodeApiVersions` -- per-node API version tracking

---

## Security

### SecurityProtocol (`common/security/auth/security_protocol.rs`)

```
PLAINTEXT | SSL | SASL_PLAINTEXT | SASL_SSL
```

### SSL/TLS

- `SslFactory` (`security/ssl/ssl_factory.rs`) -- TLS configuration factory using rustls
- `SslConfig` (`config/ssl_configs.rs`) -- certificate/key/truststore configuration
- `SslTransportLayer` -- async TLS transport via tokio-rustls
- `SslChannelBuilder` -- TLS channel factory
- Supports TLS 1.2 and 1.3

### SASL (in progress)

- `SaslConfig` (`config/sasl_configs.rs`) -- mechanism and JAAS configuration
- `SaslHandshakeRequest/Response` -- mechanism negotiation
- `SaslAuthenticateRequest/Response` -- authentication exchange
- SASL authenticator implementation: **not yet implemented**

### Authenticator trait (`network/authenticator.rs`)

- `PlaintextAuthenticator` -- no-op for PLAINTEXT connections
- SASL authenticator -- to be implemented

---

## Code Generation

### Pipeline

1. JSON specifications in `generator/messages/` (197 Kafka protocol definitions from Apache Kafka 4.2)
2. Generator code in `generator/src/` parses specs and emits Rust code
3. `build.rs` invokes generator at build time
4. Output: `OUT_DIR/generated/` -- one `.rs` file per message type

### Generated Artifacts

- Concrete data structs (e.g. `ApiVersionsRequestData`, `MetadataResponseData`)
- `Message` trait implementations (versioned read/write/size)
- `ApiMessage` implementations with `api_key()`
- Builder setters, Eq/Hash/Display derives
- 3 test-only message types from `generator/test-messages/`

---

## Key Dependencies

| Crate | Version | Purpose |
|-------|---------|---------|
| tokio | 1.x | Async runtime (net, io-util, rt, macros, time, sync) |
| tokio-rustls | 0.26 | Async TLS |
| rustls | 0.23 | TLS implementation (TLS 1.2 + 1.3) |
| rustls-pemfile | 2 | PEM file parsing |
| webpki-roots | 0.26 | Root CA certificates |
| indexmap | 2 | Ordered HashMap |
| uuid | 1 | UUID generation (v4) |
| rand | 0.9 | Randomization |
| serde / serde_json | 1.0 | Serialization for config and generator |
| log | 0.4 | Logging facade |

---

## Test Infrastructure

### Unit Tests (447 tests)

- Message serialization/deserialization round-trips
- Protocol encoding (varint, flexible versions, tagged fields)
- Generated message type validation
- Network client with MockSelector

### Integration Tests (11 tests, feature-gated)

- Require `--features integration-tests` and Docker
- Use `testcontainers` with Kafka 4.2.0
- Test real TCP connections, ApiVersions, Metadata queries
- Infrastructure: `ClusterConfig`, `ClusterPool`, `KafkaCluster`, `TestContext`

---

## Milestone Progress

### Milestone 1 -- Basic PLAINTEXT Connection (complete)

8 layers: core types, wire protocol, generated messages, request/response framework,
network transport, selector/channel, network client, integration tests.

### Milestone 2 -- Producer with Batching (not started)

Producer that accumulates messages into batches for Produce RPC.

### Milestone 3 -- SSL + SASL Authentication (in progress)

- Phase 1: SSL/TLS dependencies and ChannelBuilder refactoring (done)
- Phase 2: SslTransportLayer and SslChannelBuilder (done)
- Phase 3: SASL handshake/authenticate request/response types (done)
- Remaining: SASL authenticator implementation, integration tests with SSL+SASL
