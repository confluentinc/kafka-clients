# M21/P1 — producer `RecordMetadata`, `ProducerRecord` validation, Java's constant names (N=105)

**Status:** done (2026-11-04). Ruled 2026-11-04 (ledger 277–287, 304–311); squashed at Pranav's
request into `916ffb4e`, which reached master in #259 (`cd83931b7`). The evidence is the M21/P1 entry
in `dotnet/design/current/STATUS.md`.
**Label:** M21/P1, N=105. N=105 was unused (no `dotnet/COMMENTS.105.md`, no `M21/`, STATUS's top entry N=104).
**Branch:** `prashah_dev_dotnet_cffi_port`, from `51f453af` (pushed). Commits on this branch; no new
branch, worktree or squash.
**Mode:** A (.NET only). Q1–Q3 are follow-ups for later (ledger 304–306).
**Split (D11, ledger 287):** A3 + R2 #2 moved to `dotnet/design/history/M21/P2-producer-headers/PLAN.md` (N=106).

Cite prefixes: `src/` = `dotnet/src/Confluent.Kafka/`, `tests/` = `dotnet/tests/Confluent.Kafka.UnitTests/`,
`grpc/` = `dotnet/grpc-server/`, `J/` = `kafka/clients/src/main/java/org/apache/kafka/`, `JT/` =
`kafka/clients/src/test/java/org/apache/kafka/`, `py/` = `python/confluent_kafka/`, `pyt/` =
`python/test/`, `pyc` = `python/_confluentkafka.c`, `BH` = `rust/target/include/confluent_kafka_bindings.h`
(built 2026-10-30; no `rust/src/ffi` change since `3616e6eb`). Cites opened at `51f453af` on 2026-11-04.

---

## 1. Scope

**In:** A1 (`RecordMetadata`'s members, R2 #1's public constructor, `UNKNOWN_PARTITION`), the gRPC
server's real sizes, A2 (`ProducerRecord`'s three Java checks), and D5: Java's names for every public
.NET member that translates a Java `public static final` field or a Java enum constant.
**Out:** A3 and R2 #2 (P2); every other `FOLLOWUPS-REVIEW.md` item; the consumer's partition guards
(row 157); the config-class constants (row 271; R3d answered, ledger 307); the shared harness size
check (Q1, ledger 304). No allocation-budget constant moves in P1.

---

## 2. Facts

### 2.1 The fact refresh (ledger 277, all nine approved)

1. Python's gRPC server drops producer headers too (`python/grpc_translate.py:233-259`, stale
   docstring `:243-248`); only C forwards them (`c/grpc_server/server.cc:910-955`). (P2.)
2. Java's and Python's `MockProducer` never seal headers (`J/clients/producer/MockProducer.java:614-615`;
   `pyt/unit/test_headers.py:620-629`). (P2.)
3. `Headers.Wrap` can stay non-copying over the receive path's per-record list
   (`src/Internal/Interop/ConsumerRecordsMarshal.cs:235-247`); only `Headers.Empty` breaks. (P2.)
4. A1 needs 2 imports, not 4: Java's and Python's classes compute `hasOffset` / `hasTimestamp`
   (`J/clients/producer/RecordMetadata.java:63-65`, `:79-81`; `py/producer/record_metadata.py:77-88`).
5. `tests/PublicProducerRecordTests.cs:47` pins the old partition text.
6. Python uses the OptionsBuilder for every record (`pyc:2291-2316`). (P2.)
7. The core copies header values three times. (P2; Q2.)
8. The harness comments `rust/tests/integration/producer_test.rs:564-569`, `:3011-3014` are stale. (Q1.)
9. The Java placeholder is a send-time `ApiException`'s; Java checks topic, timestamp, partition in that order.

Line drift (§2.2 of the evaluation): `src/Internal/Interop/ProducerSendMarshal.cs:94-117` → `:97-120`;
`py/producer/producer_record.py:170-177` → `:169-178`; `RecordHeaders.java:154-173` → `:153-177`;
`RecordHeader.java:68-87` → `:67-89`; `header.py:99-125` → `:99-126`; `headers.py:177-193` → `:177-195`;
`rust/src/ffi/producer/producer_record.rs:724-745` → `:724-748`; `rust/src/ffi/common/header/mod.rs:356-362`
→ `:354-363`; `dotnet/CLAUDE.md:201-208` → `:201-209`.

### 2.2 Found after the rulings (reported to Pranav, ledger 281 and 285)

1. **.NET's `TopicPartition` is a `readonly struct`** (`src/Common/TopicPartition.cs:24`), so a
   `TopicPartition` parameter cannot be null. Ruled (row 309): the constructor takes
   `TopicPartition?`, so the null case exists as in Java and Python, and D4's throw applies.
2. **Six Java enums override `toString()`**, so Java prints a display name there, not the constant:
   `TransactionState` (`J/clients/admin/TransactionState.java:48-50`), `ClassicGroupState`
   (`J/common/ClassicGroupState.java:58-60`), `IsolationLevel` (lowercase constant,
   `J/common/IsolationLevel.java:50-52`), `TimestampType` (`J/common/record/TimestampType.java:47-49`),
   `GroupState` (`J/common/GroupState.java:94-96`), `GroupType` (`J/common/GroupType.java:57-59`).
   Today's PascalCase member names happen to equal those display names for five of them, and the gRPC
   server relies on it: "The enum members are spelled as Java's toString(), so Enum.ToString() is the
   wire spelling." (`grpc/TranslateAdmin.cs:1286-1287`; the name tables `:1289-1311`). After D5, a C#
   enum's `ToString()` prints the Java constant (`STABLE`) and cannot be overridden, so (a) every text
   the binding prints or sends for these six uses an internal map to Java's `toString()` text, and
   (b) the enum's own `ToString()` differs from Java's for these six: a recorded deviation (C# cannot
   override an enum's `ToString`), open for Pranav (a public helper would be additive later).

---

## 3. Decisions (all ruled; ledger rows in brackets)

| D | Ruling | Detail |
|---|---|---|
| D1 [278] | 4 properties | `HasOffset` (`Offset != -1`), `HasTimestamp` (`Timestamp != -1`), `SerializedKeySize`, `SerializedValueSize` (Java `RecordMetadata.java:63-106`; Python `record_metadata.py:77-103`). |
| D2 [279] | 2 imports | `kafka_producer_RecordMetadata_serialized_key_size_err` (BH:53037), `_serialized_value_size_err` (BH:53053); `RecordMetadataMarshal.CopyOut` (`src/Internal/Interop/RecordMetadataMarshal.cs:27-39`) reads 6 values. |
| D3 [280] | Java's public constructor | `public RecordMetadata(TopicPartition topicPartition, long baseOffset, int batchIndex, long timestamp, int serializedKeySize, int serializedValueSize)`: offset `baseOffset == -1 ? -1 : baseOffset + batchIndex` (`RecordMetadata.java:49-57`). Stores the `TopicPartition` (a struct, §2.2 #1); `Topic` / `Partition` read it. The internal 4-argument constructor (`src/Producer/RecordMetadata.cs:25`) is replaced by an internal one taking the combined offset (as Python's `_from_ffi`, `record_metadata.py:59-75`); the 4 test sites move (`tests/PublicKafkaFutureTests.cs:665`, `PublicAsyncKafkaFutureTests.cs:740`, `PublicNonGenericKafkaFutureTests.cs:201`, `dotnet/tests/Confluent.Kafka.GrpcServer.UnitTests/LoggingDeliveryCallbackTests.cs:38`). |
| D4 [281, 309] | (b)(i), with `TopicPartition?` | The public constructor takes `TopicPartition? topicPartition` and stores it as given; `Topic`, `Partition` and `ToString` throw `InvalidOperationException("topicPartition must not be null")` when it is null. The internal delivery-path constructor always has a value. Tests: a null argument constructs; each of the three members throws that type and message. (Row 309 supersedes the keep-the-struct handling first committed in `a042cdef`.) |
| D5 [285] | Java's names, scope (3) | §3.1. |
| D6 [282] | sizes -1 | the placeholder (`src/Internal/DeliveryState.cs:273`) becomes Java's `(tp, -1, -1, NO_TIMESTAMP, -1, -1)` (`J/clients/producer/KafkaProducer.java:1134`). |
| D7 [283] | real sizes | `grpc/Translate.cs:322-331` sends `SerializedKeySize` / `SerializedValueSize`, as Python (`python/grpc_translate.py:226-227`) and C (`c/grpc_server/server.cc:411-412`); the comment at `:322` goes. |
| D9 [284] | Java's texts and order | "Topic cannot be null.", "Invalid timestamp: %d. Timestamp should always be non-negative or null.", "Invalid partition: %d. Partition number should always be non-negative or null.", checked topic → timestamp → partition (`J/clients/producer/ProducerRecord.java:72-79`; Python `producer_record.py:169-178`). A negative timestamp now throws; the remark `src/Producer/ProducerRecord.cs:28-29` goes; the `<exception>` docs follow. |
| D10 [286] | `ArgumentNullException` / `ArgumentOutOfRangeException` with Java's texts | null topic: `ArgumentNullException(nameof(topic), "Topic cannot be null.")`; negative timestamp / partition: `ArgumentOutOfRangeException(nameof(timestamp|partition), value, <Java's text>)`. `ArgumentOutOfRangeException.Message` appends "(Parameter …)" and "Actual value was …", so tests assert the text before them (the gRPC server already strips them, `dotnet/CLAUDE.md:1282-1284`). |

### 3.1 D5: the inventory (a complete sweep of `src/Confluent.Kafka` outside `Internal/`, 2026-11-04)

**Renamed: public `const`s (8, plus the new one).** `ConsumerRecord.NoTimestamp` → `NO_TIMESTAMP`
(`src/Consumer/ConsumerRecord.cs:40`); `KafkaPrincipal.UserType` → `USER_TYPE`
(`src/Common/Security/Auth/KafkaPrincipal.cs:31`); `ResourcePattern.WildcardResource` →
`WILDCARD_RESOURCE` (`src/Common/Resource/ResourcePattern.cs:39`); `ClientQuotaEntity.User` /
`ClientId` / `Ip` → `USER` / `CLIENT_ID` / `IP` (`src/Common/Quota/ClientQuotaEntity.cs:51-57`);
`MockAdminClient.DefaultClusterId` and `AsyncMockAdminClient.DefaultClusterId` → `DEFAULT_CLUSTER_ID`
(`src/Admin/MockAdminClient.cs:41`, `AsyncMockAdminClient.cs:43`); new `RecordMetadata.UNKNOWN_PARTITION`.

**Renamed: static properties that stand for Java constants (10).** `Uuid.Zero` / `One` /
`MetadataTopicId` / `Reserved` → `ZERO_UUID` / `ONE_UUID` / `METADATA_TOPIC_ID` / `RESERVED`
(`src/Common/Uuid.cs:48-60`; Java `Uuid.java:41-56`; Python `uuid.py:96-105`); `KafkaPrincipal.Anonymous`
→ `ANONYMOUS` (`KafkaPrincipal.cs:58`; Python `kafka_principal.py:67-68`); `ResourcePatternFilter.Any`,
`AclBindingFilter.Any`, `AccessControlEntryFilter.Any` → `ANY` (`src/Common/Resource/ResourcePatternFilter.cs:36`,
`src/Common/Acl/AclBindingFilter.cs:38`, `AccessControlEntryFilter.cs:44`); `MockAdminClient.DefaultLogDirs`
and `AsyncMockAdminClient.DefaultLogDirs` → `DEFAULT_LOG_DIRS` (`MockAdminClient.cs:47`,
`AsyncMockAdminClient.cs:49`; Python `admin/_mock_core.py:516-517`).

**Renamed: the members of the 19 public enums that translate a Java enum** (Java's constant names,
read from the Java sources):

| .NET enum (file) | Java | Members after the rename |
|---|---|---|
| `FeatureUpdate.UpgradeType` (`src/Admin/FeatureUpdate.cs:29`) | `FeatureUpdate.UpgradeType` | `UNKNOWN`, `UPGRADE`, `SAFE_DOWNGRADE`, `UNSAFE_DOWNGRADE` |
| `TransactionState` (`src/Admin/TransactionState.cs:36`) | `TransactionState` (overrides `toString`) | `ONGOING`, `PREPARE_ABORT`, `PREPARE_COMMIT`, `COMPLETE_ABORT`, `COMPLETE_COMMIT`, `EMPTY`, `PREPARE_EPOCH_FENCE`, `UNKNOWN` |
| `ConfigEntry.ConfigType` (`src/Admin/ConfigEntry.cs:318`) | `ConfigEntry.ConfigType` | `UNKNOWN`, `BOOLEAN`, `STRING`, `INT`, `SHORT`, `LONG`, `DOUBLE`, `LIST`, `CLASS`, `PASSWORD` |
| `ConfigEntry.ConfigSource` (`:358`) | `ConfigEntry.ConfigSource` | `DYNAMIC_TOPIC_CONFIG`, `DYNAMIC_BROKER_LOGGER_CONFIG`, `DYNAMIC_BROKER_CONFIG`, `DYNAMIC_DEFAULT_BROKER_CONFIG`, `DYNAMIC_CLIENT_METRICS_CONFIG`, `DYNAMIC_GROUP_CONFIG`, `STATIC_BROKER_CONFIG`, `DEFAULT_CONFIG`, `UNKNOWN` |
| `AlterConfigOpType` (`src/Admin/AlterConfigOpType.cs:30`) | `AlterConfigOp.OpType` | `SET`, `DELETE`, `APPEND`, `SUBTRACT` |
| `ScramMechanism` (`src/Admin/ScramMechanism.cs:26`) | `ScramMechanism` | `UNKNOWN`, `SCRAM_SHA_256`, `SCRAM_SHA_512` |
| `ClassicGroupState` (`src/Common/ClassicGroupState.cs:40`) | `ClassicGroupState` (overrides) | `UNKNOWN`, `PREPARING_REBALANCE`, `COMPLETING_REBALANCE`, `STABLE`, `DEAD`, `EMPTY` |
| `IsolationLevel` (`src/Common/IsolationLevel.cs:22`) | `IsolationLevel` (overrides) | `READ_UNCOMMITTED`, `READ_COMMITTED` |
| `TimestampType` (`src/Common/Record/TimestampType.cs:23`) | `TimestampType` (overrides) | `NO_TIMESTAMP_TYPE`, `CREATE_TIME`, `LOG_APPEND_TIME` |
| `ConfigResourceType` (`src/Common/Config/ConfigResourceType.cs:31`) | `ConfigResource.Type` | `UNKNOWN`, `TOPIC`, `BROKER`, `BROKER_LOGGER`, `CLIENT_METRICS`, `GROUP` |
| `GroupState` (`src/Common/GroupState.cs:38`) | `GroupState` (overrides) | `UNKNOWN`, `PREPARING_REBALANCE`, `COMPLETING_REBALANCE`, `STABLE`, `DEAD`, `EMPTY`, `ASSIGNING`, `RECONCILING`, `NOT_READY` |
| `RecordDeserializationException.DeserializationExceptionOrigin` (`src/Common/Errors/RecordDeserializationException.cs:56`) | same | `KEY`, `VALUE` |
| `PatternType` (`src/Common/Resource/PatternType.cs:22`) | `PatternType` | `UNKNOWN`, `ANY`, `MATCH`, `LITERAL`, `PREFIXED` |
| `ResourceType` (`src/Common/Resource/ResourceType.cs:22`) | `ResourceType` | `UNKNOWN`, `ANY`, `TOPIC`, `GROUP`, `CLUSTER`, `TRANSACTIONAL_ID`, `DELEGATION_TOKEN`, `USER` |
| `ElectionType` (`src/Common/ElectionType.cs:34`) | `ElectionType` | `PREFERRED`, `UNCLEAN` |
| `AclOperation` (`src/Common/Acl/AclOperation.cs:24`) | `AclOperation` | `UNKNOWN`, `ANY`, `ALL`, `READ`, `WRITE`, `CREATE`, `DELETE`, `ALTER`, `DESCRIBE`, `CLUSTER_ACTION`, `DESCRIBE_CONFIGS`, `ALTER_CONFIGS`, `IDEMPOTENT_WRITE`, `CREATE_TOKENS`, `DESCRIBE_TOKENS`, `TWO_PHASE_COMMIT` |
| `AclPermissionType` (`src/Common/Acl/AclPermissionType.cs:21`) | `AclPermissionType` | `UNKNOWN`, `ANY`, `DENY`, `ALLOW` |
| `GroupType` (`src/Common/GroupType.cs:35`) | `GroupType` (overrides) | `UNKNOWN`, `CONSUMER`, `CLASSIC`, `SHARE`, `STREAMS` |
| `GroupMembershipOperation` (`src/Consumer/GroupMembershipOperation.cs:21`) | `CloseOptions.GroupMembershipOperation` | `LEAVE_GROUP`, `REMAIN_IN_GROUP`, `DEFAULT` |

Numeric values do not change.

**Kept in .NET casing (no Java constant behind them).** `ClientQuotaMatchType` and its members `Exact`,
`Default`, `Specified` (`src/Common/Quota/ClientQuotaMatchType.cs:22`; a .NET-only helper of
`ClientQuotaFilterComponent`, `tests/PublicNamespaceLayoutTests.cs:284`); `Serdes.String`, `ByteArray`,
`Int32`, `Int64`, `Double`, `Guid`, `Null` (`src/Common/Serialization/Serdes.cs:26-53`: Java's `Serdes`
has static methods, not fields); the static holder classes' methods `UpgradeTypes.FromCode`,
`ScramMechanisms.MechanismName` / `FromType` / `FromMechanismName`, `GroupStateExtensions.GroupStatesForType`
(Java methods); the `==` / `!=` operators; the two `internal` enums under `Internal/`; every type name.

**Strings an enum name becomes.** The rename must not change a wire string, and every text the
binding prints must equal Java's. The S1 inventory covers at least: the private `JavaName` maps that
become `ToString()` or stay (`src/Admin/FeatureUpdate.cs:120`, `AlterConfigOp.cs:102`,
`ConfigEntry.cs:241`, `:255`, `src/Common/Config/ConfigResource.cs:91`); `src/Admin/GroupListing.cs:136-138`;
`src/Common/Quota/ClientQuotaFilterComponent.cs:127` (kept enum, unchanged); `grpc/TranslateAdmin.cs:839-840`,
`:882`, `:1286-1311` (the wire names, which must stay Java's `toString()` for the six of §2.2 #2);
the `Enum.GetNames` / `GetValues` / `IsDefined` sites in `src`, the gRPC server and the tests; every
`nameof`, interpolation and `ToString()` of a renamed member in `src`, `grpc-server`, `soak`, `tests`
and `tests/Performance`; and the test expectations of enum names (for example
`tests/PublicAdminP8TypesTests.cs:51`, `tests/Interop/GroupMarshalClassicStateTests.cs:58`), which
become Java's constant names. An internal map gives Java's `toString()` text for the six enums of
§2.2 #2, read from each Java source; no public member is added.

---

## 4. Steps (one commit each)

- **S0 — rules** (markdown only; path check, ledger 58). Drafts R1–R4 below, applied verbatim.
- **S1 — D5 renames** (the Actor may split it in two, S1a the `const`s and static properties, S1b
  the enum members). The naming rules: `dotnet/.editorconfig:162-168`, with `EnforceCodeStyleInBuild`
  and `TreatWarningsAsErrors` (`dotnet/Directory.Build.props:16-17`), and any analyzer (CA1707) that
  flags an underscore: the narrowest change that admits Java's names and keeps every other name
  checked, stated in the commit message. Every reference in `src`, `grpc-server`, `soak`, `tests`
  and `tests/Performance` follows. Tests: the existing suites, with the enum-name expectations at
  Java's constant names; a test per §2.2 #2 enum that the binding's printed or wire text is Java's
  `toString()` (the gRPC name tables round-trip every member); `tests/PublicNamespaceLayoutTests.cs`
  stays green.
- **S2 — A1 core** (D1–D4, D6). Tests: new `tests/PublicRecordMetadataTests.cs` with Java's
  `testConstructionWithMissingBatchIndex` and `testConstructionWithBatchIndexOffset`
  (`JT/clients/producer/RecordMetadataTest.java:29`, `:46`), Python's `test_record_metadata_surface`
  (`pyt/unit/test_producer_types.py:155`: `UNKNOWN_PARTITION == -1`, `HasOffset` / `HasTimestamp`,
  "foo-2@7") and `test_record_metadata_from_ffi_is_what_the_constructor_builds` (`:249`); deliveries
  on `MockProducer` and `AsyncMockProducer` read sizes 0 and `HasTimestamp == false` (the core mock,
  `rust/src/producer/mock_producer.rs:1398`); a failed delivery's placeholder has sizes -1; a broker
  test in `dotnet/tests/Confluent.Kafka.IntegrationTests/KafkaProducerBrokerTests.cs` asserts the real
  serialized sizes and `HasTimestamp`.
- **S3 — A1 gRPC** (D7). `TranslateTests` asserts the sizes; `LoggingDeliveryCallbackTests.cs:38` uses
  the new constructor.
- **S4 — A2** (D9, D10). `tests/PublicProducerRecordTests.cs`: Java's `testInvalidRecords`
  (`JT/clients/producer/ProducerRecordTest.java:55`) with the messages asserted (DoD §3; Python's
  `test_invalid_records`, `test_producer_types.py:56`); both negative reports the timestamp; `:35-37`
  and `:47` updated.
- **S5 — comment-only commit** (ledger 59).
- **S6 — the STATUS entry.**

### 4.1 Rule drafts (S0)

- **R1** — `dotnet/CLAUDE.md` §3, the `RecordMetadata` block of the sketch (`:211-216`) becomes:
  ```csharp
  public sealed class RecordMetadata {            // NON-generic (no K/V on the ack); getters → properties
      public const int UNKNOWN_PARTITION = -1;
      public RecordMetadata(TopicPartition topicPartition, long baseOffset, int batchIndex, long timestamp,
          int serializedKeySize, int serializedValueSize);   // Java's; offset = baseOffset + batchIndex unless -1
      public bool HasOffset { get; }
      public long Offset { get; }
      public bool HasTimestamp { get; }
      public long Timestamp { get; }
      public int SerializedKeySize { get; }
      public int SerializedValueSize { get; }
      public string Topic { get; }
      public int Partition { get; }
  }
  ```
- **R2** — the "Clipped to today's ABI" paragraph (`:292-302`): delete "`RecordMetadata`'s
  serialized-size / `has*` accessors," from its list of omitted members.
- **R3** — the §3 idiom-map table (after the `method send, flush, poll` row, `:619`) gains the row:
  `| a public static final field or an enum constant (UNKNOWN_PARTITION, ResourceType.ANY) | Java's own name and spelling: RecordMetadata.UNKNOWN_PARTITION, Uuid.ZERO_UUID, ResourceType.ANY; numeric values unchanged. A member with no Java constant behind it keeps .NET casing (ClientQuotaMatchType, Serdes). A C# enum's ToString() prints the Java constant; where Java's enum overrides toString() (GroupState, ClassicGroupState, GroupType, TransactionState, TimestampType, IsolationLevel) the text the binding prints or sends is Java's, from an internal map, and the enum's own ToString() is a recorded deviation | ledger 285 |`
  (with the names in backticks, as the table's other rows).
- **R4** — §7.2's C# style paragraph (`:1214-1218`): after "PascalCase," add "Java's names for the
  constants and enum members of §3's idiom map (ledger 285),".

## 5. Tests not translated, with the reason

None in P1. Java's `ProducerRecordTest.testEqualsAndHashCode` and `RecordHeadersTest` are P2's.

## 6. Gates (every commit; macOS, Release)

- `dotnet build -c Release` of the solution, the gRPC server, the soak and the perf projects: 0 warnings, 0 errors.
- `dotnet format --verify-no-changes` (and the soak's two checks when `dotnet/soak/` is touched).
- UnitTests on net8.0 and net10.0 with `-c Release`, counts reported against the previous commit's;
  `Test Run Aborted` grepped; a filter that matches nothing is a failure; never `--no-build` after a
  failed build.
- The gRPC server unit tests (`make -C dotnet test-grpc-server-dotnet`) and the soak tests.
- Once, at the end, with Docker (`RUST_TEST_THREADS=6`): `make verify-dotnet-macos-docker` and the
  IntegrationTests.
- Every staged path under `dotnet/`, so `git commit --no-verify` (ledger 58), the gate results in the
  message, ending `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.
- Pranav's no-header perf A/B, base `51f453af` against P1's tip (ledger 29); the PM does not run it.

## 7. Rulings

Pranav, 2026-11-04, verbatim (three replies to the coordinator's summaries of the evaluation):
1. "D4: (b) / D11: Two phases / D13: (a) / D20: (a) / D21: (a) / D23: (i) / D24: (c) (If python does exactly the same) / D27: (a) / D5: Use what java has / D10: ArgumentNullException and ArgumentOutOfRangeException carrying Java's texts / D18: Java's way / Q1, Q2, Q3 for later"
2. "group b table : agree / D24 (c) : Lets do python way / D5: (b) / D4(b) : (i)"
3. "#1 its a yes on 9 items / #2 (3) / #3 record R3d as answered"

The coordinator's readings: D4 (b)(i) = store the `TopicPartition`, throw
`InvalidOperationException("topicPartition must not be null")`; D5 (b) scope (3) = Java's names for
every public member that translates a Java `public static final` field or enum constant, in P1; R3d
answered (a), record only. Ledger rows 277–307 (this phase: 277–287, 304, 307).

Pranav, 2026-11-04, on the phase's open items, verbatim: "I agree R1-R4. For D4, lets take option
(b). For #3 lets do (a), #4 agree. #5 Okay." Ledger 308–311:
- R1–R4 approved (row 308); R1's constructor line takes `TopicPartition?`.
- D4 (b): `TopicPartition?`, the ruled throw on `Topic`, `Partition`, `ToString` (row 309; §2.2 #1's
  keep-the-struct handling is superseded).
- #3 (a): the six enums' own `ToString()` deviation is accepted; a public helper for Java's
  `toString()` text is a follow-up (row 310).
- #4 and #5: the texts that now match Java beyond the rulings stand, and no `.editorconfig` change
  (row 311).
