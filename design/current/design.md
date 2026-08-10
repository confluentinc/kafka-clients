# Current Design

This document captures the current architecture and design of the Confluent Kafka Rust client.
It is intended to be updated after each manager agent loop completes a milestone or phase.

**Last updated:** 2026-08-03
**Status:** Milestones 1-10 complete; Milestone 11 (producer idempotence and
transactions) in its last phase — the `TransactionManager` closure, the
transactional `Sender` loop, the public producer transaction API and
`MockProducer`'s transactional surface are translated, with `TransactionManagerTest`
fully covered and broker integration tests for commit visibility, abort discard,
epoch bump and consume-transform-produce.

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

---

## Milestone 11 — AdminClient (Tier 1 Phase 1, 2026-07-16)

> The design notes above are stale (they predate Producer/Consumer/Admin).
> This section captures the AdminClient design decisions. Full rules live in
> `.claude/rules/admin-client.md`; the multi-tier plan in
> `design/history/Milestone-11/PLAN.md`. **Scope for this task: Rust core +
> tests only — C FFI / Python bindings are deferred to a separate future
> task** (that future task should reuse PR #116's `src/ffi/common.rs` async
> dispatcher).

**Key architecture decision — sync-returning-futures, not async.** Unlike the
Consumer (whose `poll()` itself blocks and is therefore `async`), every Java
`Admin` RPC returns immediately with a `*Result` wrapping one `KafkaFuture<T>`
per key; the network I/O happens on a background task and the caller opts into
blocking at `KafkaFuture.get()`. So Admin's per-RPC methods are plain sync
`fn` in Rust; only `close()` (which joins the background task in Java) is
`async fn`. No `#[async_trait]` on per-RPC methods or internal types.

**Dispatch + background task.** One `tokio::spawn` per client instance runs a
generic `AdminClientRunnable<C: KafkaClient>` (mirrors the producer `Sender<C>`),
driving a `Call`/`NodeProvider` retry engine over the shared `NetworkClient`.
`AdminMetadataManager` handles bootstrap + controller/broker refresh. (The
`AdminApiDriver`/lookup-strategy engine for coordinator/partition-leader RPCs
arrives in later tiers; Phase 1's topic RPCs use the plain `Call` path.)

**Completable `KafkaFuture`.** `KafkaFuture` was extended from pre-resolved-only
to fully completable (`KafkaFutureImpl<T>`: complete / complete_exceptionally /
when_complete + `all_of` / `then_apply` / `then_apply_try` / `join_map`),
underpinning the per-key result model.

**Phase 1 RPCs**: `create_topics`, `delete_topics`, `list_topics`,
`describe_topics` (by-name and by-id via the Metadata API). Quota-exceeded
retries carry `ThrottlingQuotaExceededException`/`throttleTimeMs` forward and
re-complete on final timeout, matching Java's `maybeCompleteQuotaExceededException`.

**Phase 2 (2026-07-17) — `create_partitions`, `delete_records`, and the
`AdminApiDriver` engine.** `create_partitions` is a plain controller `Call`.
`delete_records` required the second dispatch pattern, so the `AdminApiDriver` /
`AdminApiHandler` / `AdminApiLookupStrategy` engine (with `PartitionLeaderStrategy`
+ `PartitionLeaderCache`) was pulled forward from the originally-planned Phase 5:
a two-stage lookup→fulfillment driver that resolves per-partition leaders, batches
fulfillment requests by node, and unmaps + re-looks-up keys on stale-leader /
disconnect errors (via the new `Call::set_maybe_retry_fn` / `MaybeRetryOutcome`
hook). It runs on the same single bg task (no per-key/request `tokio::spawn`) and
now underpins Tier 1 Phase 5, all of Tier 2's `CoordinatorStrategy`, and Tier 3.

**Phase 3 (2026-07-17) — cluster & config administration.** `describe_cluster`,
`describe_configs`, `incremental_alter_configs`, `list_config_resources`, all on
the plain `Call` path. Key design point preserved from Java: **per-resource-type
routing** — `describe_configs`/`incremental_alter_configs` send broker /
broker-logger resources to that specific broker node and topic/other resources to
the controller or least-loaded node, rather than a single node. `describe_cluster`
decodes `authorized_operations` via `common::utils::from_32_bit_field` +
`common::acl::AclOperation`. Introduced `common::config::ConfigResource` and
`admin::AlterConfigOp`; the `ListConfigResourcesRequest` wire wrapper is shared
with Tier 3's future `listClientMetricsResources`.

**Phase 4 (2026-07-17) — log directories.** `describe_log_dirs` (per-broker
fan-out), `alter_replica_log_dirs` (replica→logdir assignments routed per
destination broker), `describe_replica_log_dirs` (built on `DescribeLogDirsRequest`,
reshaped into current/future-dir `ReplicaLogDirInfo`). All plain `Call` path.
Introduced `common::TopicPartitionReplica` and `admin::LogDirDescription`/`ReplicaInfo`.
The integration suite exercises a genuine cross-directory replica move via a
broker fixture configured with two `KAFKA_LOG_DIRS`.

**Phase 5 (2026-07-17) — elections, reassignments, offsets (completes Tier 1).**
`elect_leaders`, `alter_partition_reassignments`, `list_partition_reassignments`
on the plain controller `Call` path; `list_offsets` on the `AdminApiDriver` +
`PartitionLeaderStrategy` engine (built in Phase 2) via a new `ListOffsetsHandler`
— the plan's canonical first-class AdminApiDriver user. The Consumer module's
existing `ListOffsetsRequest`/`Response` wrapper was reused (no duplicate wire
type). Introduced `common::ElectionType`, `admin::OffsetSpec`,
`NewPartitionReassignment`/`PartitionReassignment`.

**Tier 1 complete.** Both admin dispatch patterns are implemented and exercised:
(1) the `Call`/`NodeProvider` retry engine for single-request RPCs, and (2) the
multi-step `AdminApiDriver`/`AdminApiHandler`/`AdminApiLookupStrategy` +
`PartitionLeaderStrategy` lookup→fulfillment engine for per-leader RPCs. Tier 2
(consumer groups & offsets) will add `CoordinatorStrategy` on top of the same
engine.
