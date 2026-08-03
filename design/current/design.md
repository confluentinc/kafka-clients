# Current Design

This document captures the current architecture and design of the Confluent Kafka Rust client.
It is intended to be updated after each manager agent loop completes a milestone or phase.

**Last updated:** 2026-08-03
**Status:** Milestones 1-10 complete; Milestone 11 (producer idempotence and
transactions) in progress.

> **⚠ Scope of this document.** The architecture described below — network stack,
> protocol framework, request/response layer, security, code generation — is
> still accurate and still the foundation of the crate. But this document covers
> **only Milestones 1 and 3**. It does not describe the producer, the consumer
> (now the largest module), the C FFI, the language bindings, or the
> multilanguage test harness. Do not read it as a scope or progress statement.
>
> Current scope: `design/history/MILESTONES.md` plus the per-phase
> `design/history/Milestone-N/**/PLAN.md` files.
> Current performance: `design/current/client-comparison-results.md` (kept
> current).
> Current module layout: `design/current/structure.md`.

---

## Overview

A Rust Kafka client translated from the Java Kafka client (Apache Kafka 4.2), preserving the same
architecture and logical structure while adapting to Rust idioms. All I/O is async via Tokio.

**Crate stats (2026-08-03):** 252 source files, ~146 000 lines under `src/`, ~2 264 passing tests, plus 197 generated wire-protocol message types. Per-area breakdown in `design/current/status.md`.

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
- `SaslChannelBuilder` -- creates SASL-authenticated channels (SASL_PLAINTEXT, SASL_SSL)

**Factory:**
- `client_channel_builder()` (`channel_builders.rs`) -- dispatches on `SecurityProtocol` to create the appropriate builder

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

### SASL (complete)

- `SaslConfig` (`config/sasl_configs.rs`) -- mechanism and JAAS configuration
- `SaslHandshakeRequest/Response` -- mechanism negotiation
- `SaslAuthenticateRequest/Response` -- authentication exchange
- `SaslClientAuthenticator` (`security/authenticator/sasl_client_authenticator.rs`) -- full PLAIN mechanism state machine

### Authenticator trait (`network/authenticator.rs`)

- `PlaintextAuthenticator` -- no-op for PLAINTEXT and SSL connections
- `SaslClientAuthenticator` -- full SASL PLAIN authentication with handshake/authenticate exchange

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
| rcgen | 0.13 | Self-signed certificate generation (dev-only) |

---

## Test Infrastructure

### Unit Tests (579 tests)

- Message serialization/deserialization round-trips
- Protocol encoding (varint, flexible versions, tagged fields)
- Generated message type validation
- Network client with MockSelector

### Integration Tests (16 tests, feature-gated)

- Require `--features integration-tests` and Docker
- Use `testcontainers` with Kafka 4.2.0
- Test real TCP connections, ApiVersions, Metadata queries
- SSL/SASL tests: SSL, SASL_PLAINTEXT, SASL_SSL, auth failure, unsupported mechanism
- Infrastructure: `ClusterConfig`, `ClusterPool`, `KafkaCluster`, `TestContext`, `SecureKafka` (custom image), `test_certs` (cert generation)

---

## Milestone Progress

> Only Milestones 1-3 are described here. Milestones 4-10 are complete and are
> documented in `design/history/MILESTONES.md`; Milestone 11 is in progress with
> its plan at `design/history/Milestone-11/PLAN.md`.

### Milestone 1 -- Basic PLAINTEXT Connection (complete)

8 layers: core types, wire protocol, generated messages, request/response framework,
network transport, selector/channel, network client, integration tests.

### Milestone 2 -- Producer with Batching (complete)

Producer that accumulates messages into batches for Produce RPC. Delivered;
`src/producer/` is now 21 files / ~15 900 lines.

### Milestone 3 -- SSL + SASL Authentication (complete)

- Phase 1: Security & config types (SecurityProtocol, SslConfig, SaslConfig)
- Phase 2: SSL/TLS transport (SslFactory, SslTransportLayer, SslChannelBuilder)
- Phase 3: SASL handshake/authenticate request/response types
- Phase 4: SASL client authenticator state machine
- Phase 5: ChannelBuilders factory for security protocol dispatch
- Phase 6: SSL and SASL PLAIN integration tests with custom Docker image
