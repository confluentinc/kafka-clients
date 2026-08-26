# Milestone 13 — Phase 6 residual-file sweep

Full walk of every Java file changed `4.2.0..4.3.1` under
`clients/src/main/java/org/apache/kafka/{clients,common}` (176 entries) and the
test tree (`clients/src/test`, 117 entries, restricted to translated classes),
classified as:

- **(a) covered** — the 4.3.1 delta was applied (or confirmed a no-op) by a
  Phase 0–5 commit;
- **(b) untranslated** — the class has no Rust counterpart per PLAN §1.1 scope,
  so there is nothing to drift;
- **(c) deleted / moved-out** — the file left the client in 4.3.1;
- **(d) UNCOVERED in-scope residue** — an in-scope delta not yet applied.

**Result: zero (d) residues.** Every in-scope delta was applied by Phases 0–5;
the sweep applied no new code. The (d)-candidate list called out in the Phase-6
task (`ClientUtils`, `Metadata`, `KafkaFutureImpl`, `AuthenticationException`,
`RecordHeaders`, `SaslConfigs`, `BrokerSecurityConfigs`, `Bytes`/`Utils`/
`BytesUtils`, `feature/BaseVersionRange`) was investigated individually — all
resolved to (a), (b), or (c); see the "(d)-candidate adjudications" section.

## Counts (main code, 176 files)

| Classification | Count |
|---|---|
| (a) covered by Phase 0–5 | 66 |
| (b) untranslated area (§1.1) | 76 |
| (c) deleted / moved-out | 34 |
| (d) uncovered in-scope residue | 0 |

(The 33 `common/record/*` → `common/record/internal/*` renames are counted once
each under (a) — they were applied crate-wide by the Phase-1 module move. The
`R051`/`R052` git rename pairings are artifacts pairing an unrelated deleted file
with an unrelated added file; each half is classified on its own merits below.)

## Main code — classification

### (a) Covered by Phase 0–5

**Phase 1 — common (record move + small deltas):**

- 33 `common/record/*` → `common/record/internal/*` class renames
  (`AbstractLegacyRecordBatch`, `AbstractRecordBatch`, `AbstractRecords`,
  `BaseRecords`, `ByteBufferLogInputStream`, `CompressionRatioEstimator`,
  `CompressionType`, `ControlRecordType`, `ControlRecordUtils`, `DefaultRecord`,
  `DefaultRecordBatch`, `DefaultRecordsSend`, `EndTransactionMarker`,
  `FileLogInputStream`, `FileRecords`, `LegacyRecord`, `LogInputStream`,
  `MemoryRecords`, `MemoryRecordsBuilder`, `MultiRecordsSend`,
  `MutableRecordBatch`, `PartialDefaultRecord`, `Record`, `RecordBatch`,
  `RecordBatchIterator`, `RecordVersion`, `Records`, `RecordsSend`,
  `RemoteLogInputStream`, `SimpleRecord`, `TransferableRecords`,
  `UnalignedFileRecords`, `UnalignedMemoryRecords`, `UnalignedRecords`) —
  KAFKA-20128; content unchanged, `pub(crate)` module move.
- `common/record/internal/package-info.java` (new, via R051 dst) — Phase 1
  carried package docs.
- `common/record/package-info.java` (M) — Phase 1 (`TimestampType` stays public).
- `common/record/internal/ControlRecordType.java` (R068) — Phase 1 KAFKA-10863
  (generated `ControlRecordTypeSchema`).
- `common/GroupState.java` — annotation-only (`@InterfaceStability.Evolving`
  removed); no Rust analogue. Phase 1 (documented in commit `952b6320`).
- `common/PartitionInfo.java` — javadoc addition ("preferred replica is the head
  of the list"); applied to `src/common/partition_info.rs`. Phase 1 (`952b6320`).
- `common/TopicPartitionInfo.java` — javadoc `Node#noNode()`→`null`; Rust doc
  already reads "or `None`". Phase 1.
- `common/utils/ProducerIdAndEpoch.java` — `record.internal` import move only.
  Phase 1.
- `common/protocol/Readable.java`, `Writable.java`, `SendBuilder.java` —
  `record`→`record.internal` import moves only. Phase 1.
- `common/header/internals/RecordHeaders.java` — `record.internal` import move
  only. Phase 1.
- `common/feature/BaseVersionRange.java` — comment on `toMap()` insertion order;
  Rust folds into `SupportedVersionRange` whose `Display` uses a fixed format
  (not map iteration), so ordering is moot. Phase 1 skip-with-reason.
- `common/requests/FetchRequest.java`, `FetchResponse.java`, `ProduceRequest.java`,
  `ProduceResponse.java`, `ListOffsetsResponse.java`,
  `OffsetsForLeaderEpochResponse.java`, `RequestUtils.java`,
  `InitProducerIdRequest.java`, `TxnOffsetCommitRequest.java` — `record.internal`
  import moves, one javadoc `@link` path (`FetchResponse`), one whitespace
  continuation-indent (`TxnOffsetCommitRequest`); no behavioral Rust change.
  Phase 1 (`952b6320`).

**Phase 2 — producer:**

- `producer/MockProducer.java`, `ProducerConfig.java`, `KafkaProducer.java`,
  `internals/TransactionManager.java`, `internals/ProducerBatch.java`,
  `internals/Sender.java`, `internals/RecordAccumulator.java`,
  `internals/ProduceRequestResult.java`, `BufferExhaustedException.java`,
  `RecordMetadata.java`, `internals/TransactionalRequestResult.java`,
  `internals/TxnPartitionEntry.java` — Phase 2 (2PC-revert internals + await
  overload + `SENDER_TIMEOUT_MSG`/`INIT_TXN_TIMEOUT_MSG` message assertions).
- `producer/Producer.java` — javadoc `@see`→`{@link}` only; N/A rustdoc.
  Doc-equivalent, no Rust change (transitively part of Phase 2's producer sweep).

**Phase 3 — consumer offsets/commit:**

- `consumer/internals/CommitRequestManager.java` (KAFKA-20165 `OffsetFetchResult`),
  `OffsetFetcherUtils.java`, `OffsetsRequestManager.java`,
  `OffsetsForLeaderEpochUtils.java` (import-only, skip),
  `common/requests/OffsetFetchRequest.java` (`request_all_offsets`),
  `OffsetFetchResponse.java` (import-only, skip),
  `common/requests/ConsumerGroupHeartbeatResponse.java` (KIP-1251 5-liner).

**Phase 4 — consumer rebalance/poll:**

- `consumer/ConsumerRecord.java`, `ConsumerRecords.java`, `KafkaConsumer.java`
  (javadoc-only, N/A), `MockConsumer.java`;
  `consumer/internals/AbstractFetch.java`, `AbstractHeartbeatRequestManager.java`,
  `AbstractMembershipManager.java`, `AsyncKafkaConsumer.java`,
  `CompletedFetch.java`, `ConsumerMembershipManager.java`,
  `ConsumerNetworkThread.java`, `ConsumerUtils.java` (javadoc, N/A),
  `Fetch.java` (folded into `ConsumerRecords`; skip), `FetchCollector.java`,
  `FetchMetricsManager.java`, `Fetcher.java`, `MemberStateListener.java`,
  `SubscriptionState.java`, `WakeupTrigger.java` (rotating-token model; skip);
  events `ApplicationEvent.java`, `ApplicationEventProcessor.java`,
  `ApplyAssignmentEvent.java` (A), `AsyncPollEvent.java`, `BackgroundEvent.java`,
  `PartitionsAssignedEvent.java` (A), `PartitionsRemovedEvent.java` (R064 rename
  of `ConsumerRebalanceListenerCallbackNeededEvent`).
- `clients/ClientUtils.java` (−27, drops unused `createNetworkClient` overload;
  no Rust factory overload) and `clients/Metadata.java` (import moves/javadoc) —
  Phase 4 recorded skips, N/A.

**Phase 5 — admin:**

- `admin/Admin.java` (javadoc), `admin/DeleteConsumerGroupsResult.java`,
  `admin/KafkaAdminClient.java`, `admin/LogDirDescription.java` (KIP-1066
  `isCordoned`), `admin/internals/AdminApiDriver.java` (KAFKA-20673).

**Phase 0 (transitive):** the `common/record/internal/*` renames above were
enabled by the Phase-0 spec-corpus sync + submodule bump; no separate main-code
file is owned solely by Phase 0.

### (b) Untranslated areas (§1.1) — no Rust counterpart

- **Share consumer (KIP-932):** `consumer/KafkaShareConsumer.java`,
  `MockShareConsumer.java`, `internals/ShareCompletedFetch.java`,
  `ShareConsumeRequestManager.java`, `ShareConsumerImpl.java`, `ShareFetch.java`,
  `ShareFetchCollector.java`, `ShareMembershipManager.java`,
  `ShareSessionHandler.java`, `internals/events/ShareAcknowledgementEvent.java`,
  `SharePollEvent.java`, `common/requests/ShareFetchResponse.java`,
  `ShareRequestMetadata.java`.
- **Streams-integration consumer internals:**
  `internals/StreamsGroupHeartbeatRequestManager.java`,
  `StreamsMembershipManager.java`, `StreamsRebalanceData.java`,
  `internals/events/StreamsTasksAssignedEvent.java` (A).
- **Classic consumer / Coordinator:** `internals/ClassicKafkaConsumer.java`,
  `ConsumerCoordinator.java`, `internals/OffsetFetcher.java`.
- **OAuth (`oauthbearer`, incl. KAFKA-18608 client assertion):**
  `security/oauthbearer/BrokerJwtValidator.java`,
  `ClientCredentialsJwtRetriever.java`, `DefaultJwtRetriever.java`,
  `JwtBearerJwtRetriever.java`, `OAuthBearerValidatorCallbackHandler.java`,
  `internals/secured/ClientAssertionRequestFormatter.java` (A),
  `ClientCredentialsRequestFormatterFactory.java` (A), `ConfigOrJaas.java` (A),
  `ClientSecretRequestFormatter.java` (R062 rename of
  `ClientCredentialsRequestFormatter`), `HttpJwtRetriever.java`,
  `assertion/AssertionSupplierFactory.java` (A), `AssertionUtils.java`,
  `CloseableSupplier.java` (R052 dst), `DefaultAssertionCreator.java`.
- **Telemetry (KIP-714):** `telemetry/internals/ClientTelemetryReporter.java`,
  `ClientTelemetryUtils.java`, `common/requests/PushTelemetryRequest.java`.
- **Schema runtime (`protocol/types` beyond minimal `types.rs`):**
  `protocol/types/ArrayOf.java`, `BoundField.java`, `CompactArrayOf.java`,
  `NullableSchema.java` (A), `Schema.java`, `Type.java`, `common/protocol/Protocol.java`,
  `common/protocol/ApiKeys.java` (its only delta is the `Schema.Visitor`
  buffer-detector refactor + RECORDS-type imports — the Rust `ApiKeys` is
  generated and has no schema-walking `hasBuffer` detector).
- **`ConfigDef` framework:** `common/config/ConfigDef.java`.
- **Compression hierarchy:** `common/compress/Compression.java`,
  `GzipCompression.java`, `Lz4BlockOutputStream.java`, `Lz4Compression.java`,
  `NoCompression.java`, `SnappyCompression.java`, `ZstdCompression.java`.
- **Monolithic `Utils`/`Bytes`/`Shell` + KIP-1247:** `common/utils/Utils.java`,
  `Bytes.java`, `Shell.java`, `common/utils/internals/BytesUtils.java` (A),
  `common/utils/package-info.java`.
- **`ProducerInterceptors`:** `producer/internals/ProducerInterceptors.java`
  (`record.internal` import move only; class untranslated).
- **Serialization framework:** `common/serialization/ListSerializer.java`,
  `ListDeserializer.java` (no Rust counterpart; the consumer's `Deserializer<T>`
  trait is a separate zero-copy design, §27).
- **OAuth-only config keys:** `common/config/SaslConfigs.java` (the delta is the
  `SASL_OAUTHBEARER_EXPECTED_{AUDIENCE,ISSUER}` doc + LOW→HIGH importance change;
  those OAuth constants are not present in `src/common/config/sasl_configs.rs`),
  `common/config/internals/BrokerSecurityConfigs.java` (same OAuth importance
  change; no Rust `broker_security_configs`).

### (c) Deleted / moved-out in 4.3.1

- `common/record/RecordValidationStats.java` (D) — moved to the storage module;
  no Rust counterpart present (Phase 1 confirmed absent).
- `common/utils/Scheduler.java` (D), `common/utils/SystemScheduler.java`
  (R051 src) — Scheduler abstraction, no Rust counterpart (untranslated Utils).
- `common/utils/MappedIterator.java` (R052 src) — no Rust counterpart.
- `consumer/internals/events/StreamsOnTasksAssignedCallbackNeededEvent.java` (D)
  — Streams, untranslated.
- The 33 old `common/record/*.java` paths (R0xx src halves) — superseded by their
  `common/record/internal/*` destinations under (a).

### (d)-candidate adjudications

Each (d)-candidate from the Phase-6 task, individually investigated:

| Candidate | Verdict |
|---|---|
| `clients/ClientUtils.java` | (a) Phase 4 skip: drops an unused `createNetworkClient` overload; Rust has no such factory overload. |
| `clients/Metadata.java` | (a) Phase 4 skip: `record.internal` import + javadoc only. |
| `common/internals/KafkaFutureImpl.java` | (a) doc-only typo `dependants`→`dependents`; the word is absent from `src/common/kafka_future.rs`. No change. |
| `common/errors/AuthenticationException.java` | (a) javadoc `<ul><li>` HTML fix; the Rust `authentication_error.rs` is a different typed-error design with no matching rustdoc list. No change. |
| `common/header/internals/RecordHeaders.java` | (a) Phase 1: `record.internal` import move only. |
| `common/config/SaslConfigs.java` | (b) OAuth `EXPECTED_{AUDIENCE,ISSUER}` importance LOW→HIGH + doc; those OAuth keys are not translated. |
| `common/config/internals/BrokerSecurityConfigs.java` | (b) same OAuth importance change; no Rust counterpart file. |
| `common/utils/{Bytes,Utils}.java`, `internals/BytesUtils.java` | (b) monolithic Utils/Bytes + KIP-1247; untranslated. Confirmed no translated helper drifted (`ProducerIdAndEpoch` was the only touched Utils-adjacent file and its delta is an import move). |
| `common/feature/BaseVersionRange.java` | (a) Phase 1 skip: `toMap()` ordering comment; Rust `SupportedVersionRange::Display` uses a fixed format independent of `to_map()`, so the concern does not apply. |

## Item 2 — `ProtocolRoundTripConsistencyTest` (+180, new in 4.3.1)

**Disposition: skip-with-reason.**

The test (`common/message/ProtocolRoundTripConsistencyTest.java`) cross-validates
the **generated** message serializer (`AllTypeMessageData.write/read/size`)
against the **runtime `Schema`/`Struct` serializer**
(`AllTypeMessageData.SCHEMA_0`, `new Struct(schema).set(...)`, `struct.writeTo`,
`Schema.read`, `ObjectSerializationCache`), asserting they produce byte-identical
output and round-trip to equal values.

Its premise — "the generated serializer produces the same bytes as an
*independent* runtime schema serializer" — is **unrepresentable in Rust**,
because Rust deliberately has **no runtime `Schema`/`Struct` serialization path**:

- PLAN §1.1 excludes the Schema runtime (`protocol/types` beyond the minimal
  `types.rs`); Phase 1's `Type.java` skip-with-reason records that
  `src/common/protocol/types.rs` is **metadata-only** — a `SchemaType` tag enum +
  `Field`/`Schema` field-lookup for introspection, with no `Struct`,
  `write_to`, or `Schema.read` runtime serializer.
- Rust therefore has exactly **one** serialization path (the generated
  `write`/`read`/`size`), and no second independent serializer to diff against.
- `AllTypeMessage.json` is a Java **test-only** message spec
  (`clients/src/test/resources/common/message/`) not present in the Rust
  generator corpus (`generator/test-messages/` holds `SimpleExampleMessage`,
  `NullableStructMessage`, `SimpleArraysMessage`, `SimpleRecordsMessage`).

The generated path's own write→read round-trip correctness is already covered in
Rust by `tests/common/message/message_serialization_test.rs` (per-message-type
round-trips) and the dedicated per-message tests
(`simple_example_message_test.rs`, etc.), plus the byte-level known-vector tests
added across Milestone 11 for net-new wire types. The one thing
`ProtocolRoundTripConsistencyTest` adds over those — a cross-check against a
*second* serializer — has no Rust analogue by design.

## Item 4 — Test-side sweep

Same `4.2.0..4.3.1` walk over `clients/src/test`, restricted to test files of
**translated** classes. Per-phase Recorded skips (PLAN §Phase 1–5) already cover
the substantive cases; this sweep confirms completeness and adds no new residue.

**Handled by a phase (translated + delta applied, or Recorded skip):**

- Phase 5: `KafkaAdminClientTest`, `MockAdminClient`, `AdminApiDriverTest`,
  `ListConsumerGroupOffsetsHandlerTest`, `PartitionLeaderStrategyIntegrationTest`.
- Phase 4: `ConsumerRecordsTest`, `KafkaConsumerTest`, `AsyncKafkaConsumerTest`,
  `ConsumerHeartbeatRequestManagerTest`, `ConsumerMembershipManagerTest`,
  `CompletedFetchTest`, `FetchCollectorTest`, `FetchRequestManagerTest`,
  `FetchTest` (A), `FetcherTest`, `WakeupTriggerTest`,
  `ApplicationEventProcessorTest` (all with PLAN §Phase-4 Recorded skips for the
  Share/Streams/classic-facade/`Fetch.add` cases).
- Phase 3: `CommitRequestManagerTest`, `OffsetsRequestManagerTest`,
  `OffsetFetcherTest` (classic, §20 skip), `OffsetFetchRequestTest`/
  `OffsetFetchResponseTest` (import-only skip).
- Phase 2: `KafkaProducerTest` (incl. new `INIT_TXN_TIMEOUT_MSG` assertion —
  realized in Rust as `error.message().contains(INIT_TXN_TIMEOUT_MSG)`),
  `MockProducerTest`, `SenderTest`, `TransactionManagerTest` (incl. new
  `SENDER_TIMEOUT_MSG` assertion, Phase-2 commit `fad98fca`).
- Phase 1: `FeaturesTest` (skip-with-reason — `Features` collection untranslated),
  `SupportedVersionRangeTest` (`mkMap`→`Map.of` cosmetic; Rust `test_from_to_map`
  present), `ControlRecordTypeTest` (D old → A `record/internal/`), the 20
  `common/record/*Test` + fixtures (`ArbitraryMemoryRecords`,
  `InvalidMemoryRecordsProvider`, `BufferSupplierTest`) renamed to
  `common/record/internal/` — transitively the Phase-1 record move.

**Pure `record`→`record.internal` import-move test deltas (transitively Phase 1):**
`RecordAccumulatorTest` (9/9), `ProducerBatchTest` (8/8), `RecordSendTest`,
`FutureRecordMetadataTest`, `SendBuilderTest`, `RecordsSerdeTest`,
`ProduceRequestTest`, `ProduceResponseTest`, `RequestResponseTest`,
`RequestTestUtils` — every +/- line is an import move; no behavioral test change.

**Cosmetic-only refactors (no behavioral change):** `ApiVersionsResponseTest`
(`Utils.mkMap`→`Map.of`).

**`test/TestUtils.java` (+10):** adds the
`assertFutureThrowsWithMessageContaining` helper, used by the new
`KafkaProducerTest`/`TransactionManagerTest` assertions above; realized in Rust
inline via `error.message().contains(...)` (Rust idiom — no shared helper). N/A.

**Untranslated-area test files (§1.1 — skip):**
`AcknowledgementCommitCallbackHandlerTest` (Share), `RequestManagersTest` (its
only delta is a new `StreamsRebalanceData` ctor arg — Streams), `ConfigDefTest`
(ConfigDef), `ProtocolSerializationTest` / `TypeTest` (Schema runtime),
`ListDeserializerTest` / `ListSerializerTest` (serialization framework),
`BytesTest` / `FixedOrderMapTest` / `UtilsTest` (Utils), plus all
`Share*`/`Streams*`/`*Coordinator*`/`*Assignor*`/oauth/telemetry/compress test
files filtered out of scope.

**Deleted test files (c):** `common/utils/MappedIteratorTest.java`,
`common/utils/MockScheduler.java`, `common/record/ControlRecordTypeTest.java`
(superseded by `record/internal/ControlRecordTypeTest.java`).

**New test resources:** `AllTypeMessage.json` (Java test-only spec, backing the
skipped `ProtocolRoundTripConsistencyTest`; not added to the Rust corpus),
`log4j2.yaml` (Java test logging config; N/A).
