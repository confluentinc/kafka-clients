# Milestone 1 — End-to-End KafkaProducer (Fresh Implementation)

**Status:** DRAFT — pending user approval
**Date drafted:** 2026-04-29
**Branch:** `fresh-impl`

## Goal

Translate the Java code required to make `KafkaProducer` send records to a real Kafka broker end-to-end, starting from an essentially empty `src/lib.rs` (only the license header and `#![deny(warnings)]`). The producer must be able to:

1. Construct from `Properties`-equivalent config (`ProducerConfig`).
2. Resolve metadata for one or more bootstrap servers.
3. Negotiate `ApiVersions` with each broker.
4. Serialize keys/values, accumulate records into `MemoryRecords` v2 batches, optionally compress.
5. Send `ProduceRequest` v9+ over a Tokio-based `Selector` and demultiplex `ProduceResponse`.
6. Surface partition acks via `Future<RecordMetadata>` (Rust-async) and the user `Callback`.
7. Be exercised by an integration test that produces messages to a real Kafka broker (via the existing `tests/common/kafka_cluster.rs` Testcontainers setup).

## Out of Scope (per user)

| Area | Status | Mitigation |
|---|---|---|
| `common/metrics/` (Sensor/Metrics/MetricConfig/Reporters) | **SKIP** | Stub call sites with `// metric stub` no-ops. Wire later as a separate milestone. Do *not* translate `Sensor`, `Metrics`, `KafkaMetric`, or `*MetricsRegistry` classes. |
| `TransactionManager` and idempotence-EOS branch | **SKIP** | All `if (transactionManager != null)` Java branches take the non-transactional path. `acks=all`, `enable.idempotence=false` is the default and only supported mode this milestone. Producer config validation must reject `enable.idempotence=true` and `transactional.id` with a clear error, not silently degrade. |
| `KafkaConsumer` and `consumer/` package | SKIP | Producer-only. |
| `MockProducer` | SKIP for now | Optional follow-up; not required for end-to-end production sends. |
| `Telemetry` (`common/telemetry/`) | SKIP | All `clientTelemetryReporter` references stubbed to `None`. |
| SASL / OAUTHBEARER / Kerberos / SCRAM | SASL/PLAIN + SASL_SSL included (Phase 9). SCRAM, OAUTHBEARER, Kerberos rejected with `ConfigError`. | Phase 9 is scoped to PLAIN mechanism only — sufficient for CCloud/EC2. |
| `Cluster.bootstrap` partial leader info | Translate (needed) | |
| Legacy v0/v1 records (`AbstractLegacyRecordBatch`, `LegacyRecord`) | SKIP | v2 records only — Kafka 4.2 brokers do not require legacy. |
| Coverage tooling beyond what `xtask coverage` already provides | SKIP | |
| `MockClient`, `NioEchoServer`, broker-side test fixtures | SKIP | Use real `testcontainers` broker for integration; in-process mocking only where tested in Java is for unit tests of `Sender`/`NetworkClient`. |

## Repository Cleanup (already executed in this turn)

- Deleted `design/history/Milestone-2/` (user-approved).
- Deleted `design/history/Milestone-5/` (user-approved).
- Deleted root-level `COMMENTS.0.md`, `COMMENTS.DONE.0.md`, `COMMENTS.DONE.1.md.lock` (per user feedback memory: comment files belong under `design/history/Milestone-X/Phase-Y/`, never the repo root).
- Created `design/history/Milestone-1/`.

## Recommendations Awaiting User Decision

These were in the user's "Q1, Q2, Q3, Q5" list. **Recommendations included for explicit approval at plan-review time:**

1. **`marked_classes.txt` / `remaining_classes.txt` (root):** **Recommend DELETE.** They are stale snapshots from an abandoned plan; they will be superseded by this PLAN.md and the per-phase plans.
2. **`metrics.jsonl` (root):** **Recommend MOVE** to `target/` or **DELETE.** It is generated output from the prior performance test run, not source. If kept it should be `.gitignore`d.
3. **`tests/common/` scaffolding:** **Recommend KEEP.** `cluster_config.rs`, `cluster_pool.rs`, `kafka_cluster.rs`, `test_certs.rs`, `test_context.rs` are reusable Testcontainers infrastructure and we will need them to land the integration test in Phase 8. Re-validate they still compile against the rebuilt crate at end of Phase 1; fix any breakage there.
4. **`tests/integration/performance_test.rs`:** **Recommend KEEP** but leave `#[cfg(...)]`-disabled until Phase 8. It already references the API surface we are about to build (`KafkaProducer`, `ProducerConfig`, `ProducerRecord`, `ByteArraySerializer`) — so it doubles as a north-star sanity check on the API shape we land in Phase 7/8.
5. **Root `Makefile`:** **Recommend KEEP.** Already a thin wrapper around `cargo xtask`.

## Phase Overview

The work is broken into 8 phases. Each phase ends with a green `cargo build && cargo test && cargo xtask format-check && cargo xtask lint`. Phases 1–6 are unit-testable in isolation. Phase 7 wires the surface together. Phase 8 is the real-broker integration test.

| # | Phase | LOC est. (Java) | Why this slice |
|---|---|---|---|
| 1 | Foundations: errors, time, config, byte utils, headers | ~1.5k | Everything depends on these and they have low fan-out. |
| 2 | Wire-protocol generator extension + message-spec types we need | ~2k generated | All requests/responses need this before we can write a single byte. |
| 3 | Records (v2 batches), compression, serialization | ~3k | Needed by both `MemoryRecordsBuilder` (write path) and `ProduceRequest` framing. |
| 4 | Cluster model: `Node`, `Cluster`, `PartitionInfo`, `TopicPartition`, `MetadataSnapshot`, `Metadata` | ~1.5k | The Sender and `BuiltInPartitioner` both need a stable view of the cluster. |
| 5 | Network layer: Tokio `Selector`, `KafkaChannel` (plaintext + TLS), `NetworkClient`, `InFlightRequests`, `ApiVersions`, `ClusterConnectionStates` | ~3.5k | Largest single phase; isolated from producer-specific logic. |
| 6 | Producer internals (no `KafkaProducer` shell): `BufferPool`, `ProducerBatch`, `ProduceRequestResult`, `FutureRecordMetadata`, `RecordAccumulator`, `BuiltInPartitioner`, `RoundRobinPartitioner`, `IncompleteBatches`, `ProducerInterceptors`, `ProducerMetadata`, `Sender` | ~4k | All build on Phases 3–5. `Sender` is the loop driver. |
| 7 | Public surface: `Partitioner`, `Callback`, `ProducerInterceptor`, `RecordMetadata`, `ProducerRecord`, `Producer` trait, `KafkaProducer`, `ProducerConfig` | ~2k | Thin wrapper that ties it all together. |
| 8 | Integration test: produce N messages to Testcontainers Kafka, assert delivery via `kafka-console-consumer` or via the Java client side-by-side | small | Real broker round-trip. |
| 9 | SASL/PLAIN + SASL_SSL: `SaslChannelBuilder`, `SaslClientAuthenticator`, wire messages, CCloud smoke test | ~1k | Required for EC2/CCloud testing against real authenticated brokers. |

## Phase 1 — Foundations

**Goal:** Lay down the leaf-of-tree utilities and error types so every later phase has a stable target.

**Java classes to translate:**
- `common/KafkaException.java`
- `common/errors/*` (only the producer-relevant subset: `ApiException`, `RetriableException`, `AuthenticationException`, `AuthorizationException`, `TimeoutException`, `RecordTooLargeException`, `SerializationException`, `InvalidTopicException`, `ProducerFencedException`, `InterruptException`, `DisconnectException`, `NetworkException`, `UnknownServerException`, `UnsupportedVersionException`, `InvalidMetadataException`, `LeaderNotAvailableException`, `NotLeaderOrFollowerException`, `UnknownTopicOrPartitionException`, `TopicAuthorizationException`, `TransactionAbortedException`, `ClusterAuthorizationException`, `OutOfOrderSequenceException`, `UnknownProducerIdException`, `InvalidProducerEpochException`, `CorruptRecordException`, `InvalidRecordException`)
- `common/utils/Time.java`, `SystemTime.java`, `MockTime.java`, `Timer.java`, `LogContext.java`, `Utils.java` (the slice we actually use), `ByteUtils.java`, `Crc32C.java`, `ExponentialBackoff.java`, `ExponentialBackoffManager.java`, `Exit.java` (panic-on-exit only), `ProducerIdAndEpoch.java`
- `common/utils/BufferSupplier.java`, `ByteBufferInputStream.java`, `ByteBufferOutputStream.java`, `ChunkedBytesStream.java`
- `common/header/Header.java`, `Headers.java`, `header/internals/RecordHeader.java`, `RecordHeaders.java`
- `common/Uuid.java` (use `uuid` crate but match Kafka's most-significant-bits / least-significant-bits ordering — see CLAUDE.md rule about `i64` for comparisons)
- `common/config/ConfigDef.java`, `ConfigException.java`, `AbstractConfig.java`, `TopicConfig.java` (constants only), `SslConfigs.java` (constants), `SaslConfigs.java` (constants — for rejection in validator)
- `common/internals/Topic.java` (topic-name validation)

**Skip explicitly:**
- All `metrics/` packages.
- `JaasConfig`, `KafkaPrincipal`, `KafkaPrincipalBuilder` (out of TLS-only scope).
- `ImplicitLinkedHashCollection`, `KafkaCompletableFuture` (replace with Rust idioms — `tokio::sync::oneshot` / `Vec` / `IndexMap`).
- `KafkaThread`, `ThreadUtils` (Tokio runtime replaces them).
- Legacy `Java.java` JVM-version helpers.

**Tests to translate:**
- `ByteUtilsTest`, `Crc32CTest`, `ExponentialBackoffTest`, `ExponentialBackoffManagerTest`, `TimerTest`, `TimeTest`, `MockTimeTest`, `UtilsTest` (subset matching translated functions), `ChecksumsTest`, `ChunkedBytesStreamTest`, `ByteBufferInputStreamTest`, `ByteBufferOutputStreamTest`, `RecordHeadersTest`, `RecordHeaderTest`, `UuidTest`, `ConfigDefTest` (subset relevant to ProducerConfig keys), `TopicTest` (validation rules), `LogContextTest`.

**Module layout (under `src/`):**
```
src/
  lib.rs                       (declares modules)
  common/
    mod.rs
    errors.rs                  (the unified KafkaError enum + helpers)
    kafka_exception.rs         (re-export shim)
    uuid.rs
    header/
      mod.rs
      header.rs
      internals/{record_header,record_headers}.rs
    config/
      mod.rs
      config_def.rs
      config_exception.rs
      abstract_config.rs
      topic_config.rs
      ssl_configs.rs
      sasl_configs.rs
    utils/
      mod.rs
      time.rs system_time.rs mock_time.rs timer.rs
      log_context.rs utils.rs byte_utils.rs crc32c.rs
      exponential_backoff.rs exponential_backoff_manager.rs
      buffer_supplier.rs byte_buffer_input_stream.rs byte_buffer_output_stream.rs
      chunked_bytes_stream.rs
      producer_id_and_epoch.rs exit.rs
    internals/
      mod.rs
      topic.rs
```

**DoD additions for this phase:**
- The unified `KafkaError` enum exposes `is_retriable()`, `is_fatal()`, `code()` (matching librdkafka error codes where possible). Document any code-mapping decisions in a comment block.
- All translated tests pass.
- No use of `Mutex<i64>` for shared counters that must be hot-path safe (use `AtomicI64`).
- `cargo xtask lint` and `format-check` are clean.

---

## Phase 2 — Wire Protocol & Message Generation

**Goal:** Generate the Rust types for every protocol message KafkaProducer touches, and translate the runtime side of the protocol (the encoders/decoders the generator emits calls to).

**Strategy:** The `generator/` crate already exists and produces Rust modules for the JSON specs in `generator/messages/`. Verify what the current generator emits, fix gaps, then drive it to produce the message types we need.

**Java classes to translate:**
- `common/protocol/ApiKeys.java`, `Errors.java`, `ApiMessage.java`, `Message.java`, `Readable.java`, `Writable.java`, `ByteBufferAccessor.java`, `MessageUtil.java`, `MessageSizeAccumulator.java`, `ObjectSerializationCache.java`, `SendBuilder.java`, `DataOutputStreamWritable.java`
- `common/protocol/types/*` (only what generated code uses — `Type`, `Field`, `Schema`, `Struct`, `ArrayOf`, `CompactArrayOf`, `TaggedFields`, `RawTaggedField`, `RawTaggedFieldWriter`, `BoundField`, `SchemaException`)
- `common/requests/AbstractRequest.java`, `AbstractResponse.java`, `AbstractRequestResponse.java`, `RequestHeader.java`, `ResponseHeader.java`, `RequestUtils.java`, `RequestContext.java` (only what NetworkClient touches), `CorrelationIdMismatchException.java`
- Per-API request/response wrappers we need this milestone:
  - `ApiVersionsRequest.java`, `ApiVersionsResponse.java`
  - `MetadataRequest.java`, `MetadataResponse.java`
  - `ProduceRequest.java`, `ProduceResponse.java`
  - `FindCoordinatorRequest.java`, `FindCoordinatorResponse.java` (light — used only if we extend later; stub if not needed for end-to-end produce)
  - `InitProducerIdRequest/Response` — **SKIP** (idempotence/transactions out of scope)

**Generator work:**
- Confirm `generator/src/message/` already produces the per-field flexible-versions overrides correctly (CLAUDE.md rule: `field_flexible_versions(field, msg_flex)`, never raw message-level value). If not, fix it. Add tests in `generator/` if missing.
- Confirm nullable string/bytes default handling matches CLAUDE.md (empty `Some(...)` unless `"default": "null"` is explicit).
- Confirm `i64` is used for fields that are compared as signed (producer IDs, offsets, Uuid most/least bits).

**Tests to translate:**
- `RequestResponseTest.java` (the relevant subset — Produce, Metadata, ApiVersions request/response round-trips and **byte-vector-level encoding** tests, per DoD line 3 last bullet).
- `MessageTest.java` from `generator/` if present, or add new generator round-trip tests for our message set.
- `ByteBufferAccessorTest.java`, `MessageUtilTest.java`.

**Module layout:**
```
src/common/
  protocol/
    mod.rs
    api_keys.rs errors.rs api_message.rs message.rs
    readable.rs writable.rs byte_buffer_accessor.rs
    message_util.rs message_size_accumulator.rs object_serialization_cache.rs
    send_builder.rs data_output_stream_writable.rs
    types/
      mod.rs type.rs field.rs schema.rs struct.rs array_of.rs compact_array_of.rs
      tagged_fields.rs raw_tagged_field.rs raw_tagged_field_writer.rs
      bound_field.rs schema_exception.rs
  requests/
    mod.rs
    abstract_request.rs abstract_response.rs abstract_request_response.rs
    request_header.rs response_header.rs request_utils.rs
    correlation_id_mismatch_exception.rs
    api_versions_request.rs api_versions_response.rs
    metadata_request.rs metadata_response.rs
    produce_request.rs produce_response.rs
  message/  (generated; included via build.rs)
    mod.rs (generated)
```

**DoD additions:**
- Each translated request/response has a byte-vector encoding test against a known-good vector captured from the Java client (we'll capture via `kafka/clients` with a tiny gradle test that prints `Bytes.toHexString` — store the hex strings as test fixtures).
- Header `ClientId` field uses length-prefixed encoding even in flexible versions (Java overrides flexibleVersions to "none" — verify our generator handles this).

---

## Phase 3 — Records, Compression, Serialization

**Goal:** Build the v2 record format end-to-end so we can construct a `MemoryRecords` byte-buffer that the broker will accept.

**Java classes to translate:**
- `common/record/RecordVersion.java`, `TimestampType.java`, `CompressionType.java`, `ControlRecordType.java`
- `common/record/Record.java`, `RecordBatch.java`, `MutableRecordBatch.java`, `AbstractRecordBatch.java`
- `common/record/DefaultRecord.java`, `DefaultRecordBatch.java`
- `common/record/SimpleRecord.java`, `BaseRecords.java`, `Records.java`, `AbstractRecords.java`, `TransferableRecords.java`
- `common/record/MemoryRecords.java`, `MemoryRecordsBuilder.java`
- `common/record/RecordValidationStats.java`, `RecordBatchIterator.java`, `LogInputStream.java`, `ByteBufferLogInputStream.java`
- `common/record/CompressionRatioEstimator.java`
- `common/record/UnalignedRecords.java`, `UnalignedMemoryRecords.java`, `RecordsSend.java`, `DefaultRecordsSend.java`
- `common/compress/Compression.java`, `NoCompression.java`, `GzipCompression.java`, `SnappyCompression.java`, `Lz4Compression.java`, `ZstdCompression.java` (use `flate2` / `snap` / `lz4_flex` / `zstd` crates already in Cargo.toml)
- `common/compress/Lz4BlockInputStream.java`, `Lz4BlockOutputStream.java`, `GzipOutputStream.java` (Kafka's framed Lz4 differs from raw lz4 — must reproduce framing exactly)
- `common/serialization/Serializer.java`, `Deserializer.java`, `Serdes.java`, `ByteArraySerializer.java`, `ByteArrayDeserializer.java`, `StringSerializer.java`, `StringDeserializer.java`, `IntegerSerializer.java`, `LongSerializer.java`, `BytesSerializer.java`, `BytesDeserializer.java`, `ByteBufferSerializer.java`, `ByteBufferDeserializer.java`, `UUIDSerializer.java`, `UUIDDeserializer.java`, `ShortSerializer.java`, `ShortDeserializer.java`, `FloatSerializer.java`, `FloatDeserializer.java`, `DoubleSerializer.java`, `DoubleDeserializer.java`, `BooleanSerializer.java`, `BooleanDeserializer.java`, `VoidSerializer.java`, `VoidDeserializer.java`

**Skip explicitly:**
- `AbstractLegacyRecordBatch`, `LegacyRecord`, `SimpleLegacyRecord*Test` (legacy v0/v1 — out of scope).
- `FileRecords`, `FileLogInputStream`, `RemoteLogInputStream`, `UnalignedFileRecords` (broker-side / consumer-side; not used by producer).
- `EndTransactionMarker`, `ControlRecordUtils` (transaction markers — txns out of scope, but keep the enum stub for `ControlRecordType` because record-batch parsing references it).

**Tests to translate:**
- `DefaultRecordTest`, `DefaultRecordBatchTest`, `MemoryRecordsTest`, `MemoryRecordsBuilderTest`, `CompressionRatioEstimatorTest`, `ByteBufferLogInputStreamTest`, `BufferSupplierTest`, `ControlRecordTypeTest`
- All serializer/deserializer round-trip tests (`StringSerializerTest`, `IntegerSerializerTest`, etc. — they're in `common/serialization/`).

**DoD additions (zero-copy hot path, per CLAUDE.md rule 12):**
- `MemoryRecordsBuilder::append` writes serialized bytes **directly into the batch buffer** — no intermediate `Vec<u8>` per record.
- Batch finalization (`build`) computes CRC + writes the header in place; it does not copy the already-written record bytes.
- The eventual `Send` to the wire uses `IoSlice` / `write_vectored` (will be exercised in Phase 5; the contract here is that `MemoryRecords::buffers()` returns slices, not a flattened buffer).
- Byte-level encoding test: build a batch with two known records, assert the bytes equal a hex fixture captured from the Java client.

---

## Phase 4 — Cluster Model & Metadata

**Goal:** Translate the data structures that represent the cluster topology and the `Metadata` cache that `KafkaProducer` reads from.

**Java classes:**
- `common/Node.java`, `TopicPartition.java`, `TopicIdPartition.java`, `TopicCollection.java`, `PartitionInfo.java`, `TopicPartitionInfo.java`, `Cluster.java`, `ClusterResource.java`, `ClusterResourceListener.java`
- `clients/MetadataSnapshot.java`, `Metadata.java`, `MetadataRecoveryStrategy.java`
- `clients/producer/internals/ProducerMetadata.java`
- `common/internals/ClusterResourceListeners.java`
- `clients/StaleMetadataException.java`
- `clients/CommonClientConfigs.java`, `clients/ClientDnsLookup.java`, `clients/ClientUtils.java` (the parts ProducerConfig depends on — bootstrap parsing, DNS lookup helpers)
- `clients/HostResolver.java`, `DefaultHostResolver.java`

**Tests:**
- `MetadataTest.java`, `MetadataSnapshotTest.java`, `ProducerMetadataTest.java`, `ClusterTest` (if present), `ClientUtilsTest.java`, `CommonClientConfigsTest.java`.

**DoD additions:**
- Topic / partition identifiers used as `HashMap` keys in hot paths use `Arc<str>` (CLAUDE.md rule 11 hot-path optimization). `TopicPartition::topic()` returns `&str` not `&String`.

---

## Phase 5 — Network Layer

**Goal:** Tokio-based replacement for Java's NIO `Selector` + the full `NetworkClient` machinery.

**Java classes:**
- `common/network/Selectable.java`, `Selector.java`, `KafkaChannel.java`, `ChannelState.java`, `TransportLayer.java`, `PlaintextTransportLayer.java`, `SslTransportLayer.java`, `ChannelBuilder.java`, `ChannelBuilders.java`, `PlaintextChannelBuilder.java`, `SslChannelBuilder.java`, `ChannelMetadataRegistry.java`, `ListenerName.java`, `ConnectionMode.java`
- `common/network/Send.java`, `ByteBufferSend.java`, `NetworkSend.java`, `NetworkReceive.java`, `Receive.java`, `TransferableChannel.java`, `InvalidReceiveException.java`
- `common/network/CipherInformation.java`, `ClientInformation.java`, `ServerConnectionId.java`
- `common/security/auth/SecurityProtocol.java` (PLAINTEXT, SSL only — others rejected)
- `clients/ClientRequest.java`, `ClientResponse.java`, `KafkaClient.java`, `NetworkClient.java`, `NetworkClientUtils.java`, `InFlightRequests.java`, `ClusterConnectionStates.java`, `ConnectionState.java`, `LeastLoadedNode.java`, `MetadataUpdater.java`, `ManualMetadataUpdater.java`, `RequestCompletionHandler.java`
- `clients/ApiVersions.java`, `NodeApiVersions.java`

**Tokio-specific structure:**
- `Selector` becomes a struct that owns a `tokio::sync::Mutex<HashMap<NodeId, KafkaChannel>>` and a per-channel send/recv loop spawned with `tokio::spawn`.
- Each `KafkaChannel` holds a `tokio::net::TcpStream`. SSL channels additionally hold a raw `rustls::ClientConnection` driven via `read_tls`/`write_tls` (mirroring Java's `SSLEngine.wrap`/`unwrap` decoupling). **Do NOT use `tokio_rustls::TlsStream`** — it couples crypto with TCP I/O on a single task and breaks the architectural mirror with Java's `SslTransportLayer`.
- Wire reads use length-prefix framing (4-byte big-endian size header) — `tokio::io::AsyncReadExt::read_exact`.
- Wire writes use vectored I/O (`writev_all` / `write_vectored`) so framing header + payload are not concatenated (CLAUDE.md rule 12).
- **Cancellation safety (CLAUDE.md rule 9.6):** the read loop must NOT share a `tokio::select!` arm with state mutations. Pattern: dedicated read task per channel, write side holds an `mpsc::UnboundedSender<Send>`. Never hold a `MutexGuard` across `.await`.
- `NetworkClient::poll` becomes `async fn poll(&mut self, timeout_ms: i64) -> Vec<ClientResponse>` driven by `select!` over (a) the channels' inbound message channels and (b) a sleep-deadline.

**Skip:**
- `SaslChannelBuilder`, `Authenticator`, `PlaintextAuthenticator`, `SaslClientAuthenticator`, `ReauthenticationContext`, `DelayedResponseAuthenticationException`.
- All Kerberos / OAUTHBEARER / SCRAM under `common/security/`.

**Tests:**
- `SelectorTest` (subset — connect, send, receive, disconnect, multi-connection, idle expiry).
- `NetworkClientTest` (the producer-relevant cases: connect-before-send, request-correlation, timeout-on-send, version-negotiation hand-off).
- `InFlightRequestsTest`, `ClusterConnectionStatesTest`, `NodeApiVersionsTest`, `ApiVersionsTest`.
- `ServerConnectionIdTest`, `ChannelBuildersTest`.
- `SslTransportLayerTest` subset — SSL handshake against a self-signed cert (use `rcgen` already in dev-dependencies).
- We can use the existing test scaffolding under `tests/common/` — `test_certs.rs` already builds rcgen certs.

**DoD additions:**
- A loopback test sends a `MetadataRequest` to a tiny in-process echo server and decodes the response via the generated `MetadataResponse` type — round-trip green.
- TLS handshake test connects to a self-signed broker.
- A "connection close mid-request" test asserts the in-flight request is failed with `DisconnectException`-equivalent error.

---

## Phase 6 — Producer Internals

**Goal:** All the pieces between `KafkaProducer.send` and the wire — accumulator, batches, sender loop.

**Java classes:**
- `clients/producer/internals/BufferPool.java`, `ProducerBatch.java`, `ProduceRequestResult.java`, `FutureRecordMetadata.java`, `IncompleteBatches.java`, `ProducerInterceptors.java`, `RecordAccumulator.java`, `BuiltInPartitioner.java`, `Sender.java`
- `clients/producer/internals/ErrorLoggingCallback.java`
- `clients/producer/internals/ProducerMetadata.java` (already in Phase 4 — re-confirm wiring)
- `clients/producer/RoundRobinPartitioner.java`, `Partitioner.java`, `Callback.java`, `ProducerInterceptor.java`, `RecordMetadata.java`, `BufferExhaustedException.java`

**Skip:**
- `KafkaProducerMetrics`, `ProducerMetrics`, `SenderMetricsRegistry`, `TransactionalRequestResult`, `TransactionManager`, `TxnPartitionEntry`, `TxnPartitionMap`, `PreparedTxnState`.

**Tokio-specific structure:**
- `Sender` runs as a `tokio::task` (one per producer instance), driving the loop `RecordAccumulator::ready` -> `drain` -> `NetworkClient::send` -> `poll` -> partition-batch completion. It must `.await` actual response handles, not poll a flag (CLAUDE.md rule 9.4).
- `FutureRecordMetadata` wraps a `tokio::sync::oneshot::Receiver<Result<RecordMetadata, KafkaError>>`. `KafkaProducer::send` returns this directly so callers can `.await` it.
- The user `Callback` is invoked **at the same lifecycle point** as Java's `completeFutureAndFireCallbacks` — i.e. inside the sender task, after the partition's record is acknowledged/failed, before completing the future (CLAUDE.md rule 9.5).
- **No per-message `tokio::spawn`** on the send path (CLAUDE.md rule 11). The accumulator's append path is purely synchronous (just buffer copy). The sender task is the single coroutine.
- `BufferPool` uses a `tokio::sync::Notify` for "buffer became free" signals.

**Tests:**
- `BufferPoolTest`, `ProducerBatchTest`, `RecordAccumulatorTest`, `SenderTest`, `BuiltInPartitionerTest`, `IncompleteBatchesTest` (if exists), `ProducerInterceptorsTest`, `FutureRecordMetadataTest`, `RoundRobinPartitionerTest`, `RecordSendTest`, `ProducerMetadataTest`, `ErrorLoggingCallbackTest` (if present).
- Sender tests will need a `MockNetworkClient` shim. We translate the relevant parts of `MockClient.java` for unit testing scope **only** (with a comment stating it's not the full Java MockClient).

**DoD additions:**
- Hot-path allocation audit: `KafkaProducer::send` does not clone the topic name. Header values are not copied. The future return is not boxed (`-> impl Future<Output = ...>` or a concrete struct).

---

## Phase 7 — Public Surface

**Goal:** Wire everything into `KafkaProducer`.

**Java classes:**
- `clients/producer/Producer.java` (trait), `KafkaProducer.java`, `ProducerConfig.java`, `ProducerRecord.java`, `RecordMetadata.java`, `Callback.java`, `Partitioner.java`, `ProducerInterceptor.java`

**Skip:**
- `KafkaProducer.send`'s transaction methods (`initTransactions`, `beginTransaction`, `commitTransaction`, `abortTransaction`, `sendOffsetsToTransaction`) — return `Err(KafkaError::UnsupportedOperation)` with a clear message. Document this explicitly; do not silently no-op.
- `MockProducer` — out of scope this milestone.
- `PreparedTxnState` — out of scope.

**Tests:**
- `KafkaProducerTest.java` — translate **only the non-transactional cases**. Skip transactional tests with a clear comment listing each one.
- `ProducerConfigTest`, `ProducerRecordTest`, `RecordMetadataTest`.

**DoD additions:**
- `KafkaProducer::new(config)` validates and rejects `enable.idempotence=true`, `transactional.id` set, `security.protocol` other than PLAINTEXT/SSL.
- Public API takes borrowed args where possible (CLAUDE.md rule 12). `ProducerRecord` does **not** clone the value byte slice. The serialized bytes are written directly into the accumulator's batch buffer.

---

## Phase 8 — Integration Test

**Goal:** Send N records to a real broker, assert delivery.

**Work:**
- Re-enable `tests/integration/performance_test.rs` (or a slimmed-down `producer_smoke_test.rs`).
- Use `tests/common/kafka_cluster.rs` (already present) for a Testcontainers Kafka broker.
- Send 1000 records, assert all 1000 acks, then consume them via the Java `kafka-console-consumer` (invoked via `Command`) or via a thin in-test consumer using only the wire types we already translated (no full `KafkaConsumer` translation).
- TLS variant: same test against a TLS broker using `test_certs.rs`.

**DoD additions:**
- Test runs under `cargo test --features integration-tests`.
- Test asserts: all sent records get a `RecordMetadata` with the same partition the partitioner selected; offsets are monotonic per-partition.
- Test runs cleanly **3 times in a row** — flakiness is a blocker.

---

## Phase 9 — SASL/PLAIN + SASL_SSL

**Goal:** Enable `security.protocol=SASL_SSL` with `sasl.mechanism=PLAIN` so the producer can connect to CCloud and real brokers requiring authentication. Scope is intentionally narrow: PLAIN mechanism only, no SCRAM, no Kerberos, no OAUTHBEARER.

**Reference:** `master` branch has a working implementation of exactly this scope — use it as the primary reference alongside the Java source.

**Java classes to translate:**
- `common/network/SaslChannelBuilder.java` — creates a `KafkaChannel` with either plaintext or TLS transport + `SaslClientAuthenticator`
- `common/security/authenticator/SaslClientAuthenticator.java` — PLAIN-only state machine: `SendApiVersionsRequest → ReceiveApiVersionsResponse → SendHandshakeRequest → ReceiveHandshakeResponse → SendPlainToken → ReceiveResponse → Complete`
- `common/security/ssl/SslFactory.java` / `DefaultSslEngineFactory.java` — already partially covered by `SslChannelBuilder` in Phase 5; extend to load CA cert from `ssl.ca.location` env-style config

**Skip:**
- SCRAM, OAUTHBEARER, Kerberos/GSSAPI — reject with `ConfigError("Unsupported SASL mechanism: ...")`
- Server-side: `KafkaPrincipal`, `KafkaPrincipalBuilder`, `LoginManager`, JAAS server contexts
- Re-authentication (methods exist on the `Authenticator` trait as no-ops; leave as-is)

**Wire messages (generate from existing JSON specs in `generator/messages/`):**
- `SaslHandshakeRequest.json` / `SaslHandshakeResponse.json`
- `SaslAuthenticateRequest.json` / `SaslAuthenticateResponse.json`

**Config changes:**
- `ProducerConfig` / `KafkaProducer::new` must accept `SASL_PLAINTEXT` and `SASL_SSL` in addition to `PLAINTEXT` and `SSL` (remove the Phase 7 rejection for SASL_*)
- Accept `sasl.mechanism` (default `PLAIN`), `sasl.jaas.config` or separate `sasl.username` / `sasl.password` keys
- `SecurityProtocol` enum extended with `SaslPlaintext` and `SaslSsl` variants

**Integration test (`tests/integration/ssl_sasl_test.rs`):**
Translate the 5 cases from master:
1. SSL connection (TLS-only, self-signed cert via `rcgen`)
2. SASL_PLAINTEXT + PLAIN credentials
3. SASL_SSL (TLS + PLAIN)
4. Auth failure with wrong credentials → `AuthenticationException`
5. Unsupported mechanism → `UnsupportedSaslMechanismException`

**CCloud smoke test (env-var driven, optional gate):**
Wire `tests/integration/performance_test.rs` (already has env-var support from `dev/milestone-5`) to accept `SECURITY_PROTOCOL`, `SASL_MECHANISM`, `SASL_USERNAME`, `SASL_PASSWORD`, `SSL_CA_LOCATION`. Test skips if `SASL_USERNAME` is not set, so it runs only when an EC2/CCloud environment is configured.

**DoD additions:**
- Auth failure surfaces as `KafkaError::Authentication` with a message matching Java's error string
- SASL handshake sends correct mechanism name in the `SaslHandshakeRequest`; token format matches RFC 4616 (`\0username\0password`)
- All 5 integration test cases green against a Testcontainers broker with SASL configured

---

## Risks & Mitigations

| Risk | Mitigation |
|---|---|
| Wire-protocol byte-vector divergence from Java client (off-by-one in flexible versions, varint encoding, tagged fields) | Capture hex fixtures from the Java client and assert bytes literally — do not rely on round-trip tests alone (see DoD line 3 last bullet). This is enforced in Phase 2 and Phase 3. |
| LZ4 framing difference: Kafka's framed LZ4 is **not** the standard LZ4 frame format | Translate `Lz4BlockInputStream` / `Lz4BlockOutputStream` literally. Test against a Java-produced LZ4 batch byte fixture. Do not use `lz4_flex::frame` blindly. |
| Tokio cancellation deadlocking the send path | Apply CLAUDE.md rule 9.6 strictly: dedicated tasks for I/O, no `select!` over operations with side effects, drop `MutexGuard` before `.await`. Phase-5 tests must include a "request-in-flight + producer dropped" case. |
| `MetadataRequest` topic-list being too large for varint encoding | Honor `max.request.size` and reject early — same as Java. |
| `BufferPool` waiting indefinitely under back-pressure | Match Java's `max.block.ms` semantics: wait with timeout, surface `BufferExhaustedException`. |
| Generator gaps (per-field flexibleVersions, nullable defaults) | Identify and fix in Phase 2 before any request type is generated. Add generator-level unit tests. |
| Real-broker test flakiness from Testcontainers cold-start | Reuse the existing `cluster_pool.rs` warm-pool pattern. |

## Workflow

The Manager will execute the agent loop **per phase**. Each phase ends with:
- A clean `cargo build && cargo test && cargo xtask format-check && cargo xtask lint`.
- Critic-approved `COMMENTS.N.md` empty.
- `COMMENTS.DONE.N.md` archived to `design/history/Milestone-1/Phase-K/`.
- Plan file for the **next** phase committed before spawning that phase's Actor.

Agent numbers will be assigned sequentially per phase: Phase 1 → N=1, Phase 2 → N=2, ..., Phase 9 → N=9.

## Approval Checklist

Before this plan is executed, please confirm:

- [ ] The 8-phase decomposition matches your priorities (or tell me to merge/split phases).
- [ ] The skip list (metrics / transactions / consumer / SASL / legacy records / MockProducer) is correct.
- [ ] The `marked_classes.txt`, `remaining_classes.txt`, `metrics.jsonl`, `tests/common/`, `tests/integration/performance_test.rs`, `Makefile` recommendations above (delete / move / keep) are approved or amended.
- [ ] The integration-test approach (Testcontainers Kafka, sent-then-consumed via Java console consumer) is acceptable, or you'd prefer a pure-Rust round-trip (which would expand scope to a minimal Fetch path).
