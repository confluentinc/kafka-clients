# M22/P2 — config constants, disposing the given serializers, and constructor arguments only (N=109)

**Status:** done (2026-11-06). S0–S7 committed on `prashah_dev_dotnet_config_constants`, `b39e6900d` through the S7
close-out commit, then moved onto master `9e7d8962d` and squashed into one commit (the originals are in the local tag
`m22p2-before-squash`), not pushed; the record is the STATUS entry (`dotnet/design/current/STATUS.md`,
Milestone 22 / Phase 2) and the two `COMMENTS.DONE` files in this folder. Every decision was ruled (§12; ledger
365–394); ledger 395 and 396 were added during the phase: 395 Agreed (b), 396 Pending.
**Mode:** A (.NET only): no C ABI change. One tooling step (S1) is Rust (`rust/xtask`), done by the root
`actor-executor` and reviewed by `kafka-critic` with the full pre-commit hook (`dotnet/CLAUDE.md` §8.1).
**Base:** `prashah_dev_dotnet_typed_config` at `3f7aa45f5` (M22/P1's squash `6bc16018e` plus the merge of master
`439a55567`, #308 and #315). It is pushed (`origin/prashah_dev_dotnet_typed_config` = `3f7aa45f5`) and not merged into
master. No merge, rebase or cherry-pick is in progress; the only tracked changes are three agent-memory files.
Every cite below is a line at `3f7aa45f5`.
**N:** 109. No `COMMENTS.109.md` or `COMMENTS.DONE.109.md` exists at the repo root or in `dotnet/` (checked).
**Branch:** `prashah_dev_dotnet_config_constants`, stacked on `3f7aa45f5`, created only after the plan is ruled.
**Sources:** ledger rows 130, 253, 271, 285, 355, 356 (Agreed); rows 318–341 (M21/P3, abandoned) for facts
only; `CONFIG-ONLY-CONSTRUCTOR-DOTNET-EVALUATION.md` (repo root, untracked): its outcome note is Part 3's
ruling, and its §2 facts, §3.5 and D7 are re-checked here (§1.2) rather than taken as true. Four read-only
research passes read the generator, the Java sources, the .NET code and Python's serde lifecycle. I opened
every cite this plan leans on myself, and ran three compile probes in the session scratchpad (§1.2, F24).

Cite prefixes: `src/` = `dotnet/src/Confluent.Kafka/`, `tests/` = `dotnet/tests/Confluent.Kafka.UnitTests/`,
`J/` = `kafka/clients/src/main/java/org/apache/kafka/`, `JT/` = `kafka/clients/src/test/java/org/apache/kafka/`,
`py/` = `python/confluent_kafka/`, `pyt/` = `python/test/unit/`, `core/` = `rust/src/`, `X/` = `rust/xtask/src/`,
`DC` = `dotnet/CLAUDE.md`, `FM` = `dotnet/.claude/rules/ffi-marshalling.md`, `PC` = root `CLAUDE.md`,
"Python Binding Conventions".

---

## 0. In one paragraph

Three parts in one phase. **Part 1:** Java's eight config classes and three config enums, which Python
translates and .NET lacks. The eight classes are generated from `kafka/` by a C# renderer added to
`X/config_consts.rs`, next to its Python renderer. The renderer keeps each field's declared Java type,
because M22/P1's typed configuration refuses an `int` on a SHORT key. The three enums and their Java methods
are hand-written, as Python's are. **Part 2:** the four real clients dispose the serializers and
deserializers they were given, after the destroy. A failure is traced and never rethrown, and the mocks never
dispose. This is Python's way, and it differs from Java only in Java's rethrow. **Part 3:** Pranav's ruling
that serializers are constructor arguments only, recorded as an Agreed ledger row and as rule text, with the
deviation from Java's and Python's constructors. The same rule edit fixes the stale default-interface-method
claim at DC:600-601.

## 1. Scope

### 1.1 In and out

**In:**

- **Part 1.** `ProducerConfig`, `ConsumerConfig`, `AdminClientConfig` (each with the three constants Java inherits
  from `AbstractConfig`), `TopicConfig`, `SaslConfigs`, `SslConfigs`, `SecurityConfig`, `LogLevelConfig`: their
  `public static final` fields, minus Java's deprecated ones. `SecurityProtocol`, `SslClientAuth`, `GroupProtocol`,
  with Java's methods. The C# renderer in `X/config_consts.rs`. The rows in `PublicNamespaceLayoutTests`.
  Tests (§7).
- **Part 2.** Disposal of the given serializers on `KafkaProducer` / `AsyncKafkaProducer`, and of the given
  deserializers on `KafkaConsumer` / `AsyncKafkaConsumer`, on every teardown path and on a failed construction,
  with their XML docs. Tests (§7).
- **Part 3.** The ledger row (365, Agreed) and its rule text (S0), the DC:600-601 correction, and the proposed
  update of row 356's note.

**Out, each with its reason:**

- Java's config-class methods `configNames()`, `configDef()`, `main` (`J/clients/producer/ProducerConfig.java:752`,
  `:761`, `:770` and their consumer and admin twins), `ConsumerConfig.appendDeserializerToConfig` (`:790`),
  `SaslConfigs.addClientSaslSupport` (`:387`), `SslConfigs.addClientSslSupport` (`:128`), and the config classes'
  public constructors (`ProducerConfig.java:734`, `:743`; `ConsumerConfig.java:849`, `:858`;
  `AdminClientConfig.java:325`): not translated, as Python does not translate them (PC Scope: "translated for their
  `public static final` constants only"; "except a method that takes a `ConfigDef`").
- `CommonClientConfigs` (no `@InterfaceAudience.Public`, `J/clients/CommonClientConfigs.java:39`) and
  `ShareConsumerConfig` (with the share consumer), as in Python.
- Any setter on `ProducerConfig` / `ConsumerConfig` for a reflection-filled setting: Part 3 says "No setter is part
  of this phase".
- A config route for serializers (`key.serializer` resolved, configured or defaulted): ruled out by Part 3.
- Switching the binding's own code, gRPC server, soak, perf or README to the constants (D9).
- `IAsyncDisposable` on serializers (D17), and any gate between `Dispose` and a running `Poll` / `Send` (D18).

### 1.2 Fact refresh

Each starting fact of the brief, confirmed or corrected, then the facts this plan adds. "Confirmed" means the
claim and its cite hold at `3f7aa45f5`.

| # | The brief's claim | Verdict | Cite, correction or addition |
|---|---|---|---|
| F1 | Python translates the `public static final` constants of the eight classes, and the three enums in full | Confirmed | `py/producer/producer_config.py`, `py/consumer/consumer_config.py`, `py/admin/admin_client_config.py`, `py/common/config/{topic_config,sasl_configs,ssl_configs,security_config,log_level_config}.py`; enums `py/common/security/auth/security_protocol.py`, `py/common/config/ssl_client_auth.py`, `py/consumer/group_protocol.py` |
| F2 | All of them are generated from the Java sources by `X/config_consts.rs`, the enum files included | **Corrected** | Only the eight classes are generated: `SPECS` lists them (`X/config_consts.rs:1074-1123`), one `.py` and one `.pyi` each, 16 files, header "GENERATED by `cargo xtask generate-error-codes`" (`:1299`). The three enum files are **hand-written**: no GENERATED header and no `.pyi`. Nothing under `X/` mentions the three enums, and `py_value` cannot render an enum-valued constant ("no Python form", `:1195`) |
| F3 | `ShareConsumerConfig` is left out with the share consumer; `CommonClientConfigs` because Kafka 4.4 does not make it `@InterfaceAudience.Public` | Confirmed | `CommonClientConfigs.java:39` has no annotation; the eight classes and three enums all have it (`ProducerConfig.java:55`, `ConsumerConfig.java:64`, `AdminClientConfig.java:45`, `TopicConfig.java:32`, `SaslConfigs.java:25`, `SslConfigs.java:28`, `SecurityConfig.java:25`, `LogLevelConfig.java:27`, `SecurityProtocol.java:29`, `SslClientAuth.java:28`, `GroupProtocol.java:31`). Python pins both exclusions (`pyt/test_config_classes.py:113`, `:119`) |
| F4 | Ledger 271's counts 62 / 72 / 39 / 72 / 119 / 47 / 2 / 7 | Confirmed (recount) | 420 constants, unchanged. Of them `_DOC`: 8 / 9 / 6 / 35 / 47 / 19 / 1 / 0 = 125. The three client classes include `AbstractConfig`'s `AUTOMATIC_CONFIG_PROVIDERS_PROPERTY`, `CONFIG_PROVIDERS_CONFIG`, `CONFIG_PROVIDERS_DOC` (`J/common/config/AbstractConfig.java:67-70`; `py/producer/producer_config.py:102-103`) |
| F5 | .NET has none of these classes or enums | Confirmed | No such type in `dotnet/` (src, tests, gRPC server, soak, perf, tools). Only private or internal key constants: `src/Internal/NativeProducer.cs:42` (`MaxBlockMsConfig`), `src/Internal/NativeConsumer.cs:39` (`GroupIdConfig`), the serde key sets (`src/Internal/ClientConfigs.cs:32-46`), the generated maps (`src/Internal/ConfigTypes.g.cs`) |
| F6 | M22/P1's C# renderer is in `X/config_types.rs`, output `src/Internal/ConfigTypes.g.cs`, kept current by `check-generated` | Confirmed | `OUTPUT_CS` `X/config_types.rs:40`, `render_cs` `:700-784`, `all_outputs` `:793-797` (it already appends `config_consts::outputs`), `generate` `:799-805`, `check_up_to_date` `:808-817`; `check-generated` → `check_generated` (`X/main.rs:131`) → `config_types::check_up_to_date` (`:606`) |
| F7 | Ledger 355: `config_consts.rs`'s C# emitter will be larger | Confirmed, made concrete | `config_consts.rs` is 1,435 lines with no C# renderer. Its Python render is ~170 lines (`outputs` `:1246-1343`, `py_value` `:1162-1197`, `py_str` `:1144-1159`, `wrap` `:1226-1243`). **Its field model does not keep the declared Java type**: `Field{name, init, public, static_final}` (`member`, `:731-817`), so `short`, `int` and `long` all become one `Value::Int` (`:881-886`). C# needs the declared type (D21), so the parser grows too |
| F8 | Java closes both serializers, the given ones included, at `close()` and when the constructor fails, and rethrows the first failure as "Failed to close kafka producer"; the consumer does the same through `Deserializers.close` | Confirmed, with refinements | **Close:** after the I/O thread join (`J/clients/producer/KafkaProducer.java:1494-1526`): interceptors, metrics, `keySerializerPlugin`, `valueSerializerPlugin`, partitioner (`:1528-1533`); the rethrow only when `swallowException` is false (`:1537-1541`); each failure logged at **ERROR** by `Utils.closeQuietly` (`J/common/utils/Utils.java:1143-1152`). **Constructor failure:** `appendSerializerToConfig` and `new ProducerConfig(...)` run before the `try` (`:319-321`, `try` at `:360`), so a configuration error closes nothing; the catch (`:519-524`) closes the serializers only once they were wrapped (`:404`, `:412`). **Consumer:** `closeQuietly(deserializers, …)` after the network thread (`J/clients/consumer/internals/AsyncKafkaConsumer.java:1665`) and metrics (`:1678`), at `:1679`; rethrow "Failed to close kafka consumer" (`:1685-1689`); the constructor's catch (`:596-603`) closes them only after `:496`; `Deserializers.close` closes key then value with no identity check (`J/clients/consumer/internals/Deserializers.java:77-89`) |
| F9 | Python closes both, the given ones included, also on a failed construction; logs a failure and never raises | Confirmed, with refinements | `close_if_defined` logs at **WARNING** with `exc_info`, "Exception while closing serde %r", logger `confluent_kafka` (`py/common/serialization/_supply.py:53`, `:81-92`); `close()` is called synchronously, never awaited. **When:** last, after the completion thread is joined and the handle destroyed (`py/producer/producer.py:686-691`; `py/producer/_base.py:563-567`), the consumer after its uses and the destroy (`py/consumer/_base.py:700-718`). **Failed construction:** `prepare()` runs before the guarded region (`py/producer/_base.py:456`), so a configuration error closes nothing; a core failure closes both (`:461-474`; consumer `py/consumer/_base.py:581-601`). A close the consumer refuses (another thread, or inside a callback) closes nothing (`py/consumer/consumer.py:716-732`) |
| F10 | Neither language's mocks close them | Confirmed | Java `MockProducer.close` (`J/clients/producer/MockProducer.java:438-444`), `MockConsumer.close` (`J/clients/consumer/MockConsumer.java:655-657`, `:674-676`; it takes no deserializers); Python `py/producer/_mock_core.py:482-487`, `py/consumer/_mock_core.py:665-671` |
| F11 | .NET never disposes them | Confirmed | No `Dispose` reaches a serde field; `ISerializer<T>`, `IDeserializer<T>` and `ISerde<T>` are not `IDisposable`. **Added:** no serializer or deserializer class in the repo implements `IDisposable` (src, tests, gRPC server, soak, perf, tools), so Part 2 changes no in-repo caller's behaviour |
| F12 | The Java tests are `KafkaProducerTest.testSerializerClose`, the serializer part of `testConstructorFailureCloseResource`, and the consumer's equivalent if one exists | **Partly corrected** | `testSerializerClose` (`JT/clients/producer/KafkaProducerTest.java:603-619`): two `MockSerializer`s, `CLOSE_COUNT` unchanged while open, `+2` after close. **`testConstructorFailureCloseResource` (`:560-577`) has no serializer part**: it breaks construction with an invalid interceptor class, passes `ByteArraySerializer`s, and asserts only `MockMetricsReporter`'s counts and the message. **No consumer equivalent exists**: `KafkaConsumerTest.testConstructorClose` (`JT/clients/consumer/KafkaConsumerTest.java:607-626`) asserts the reporter only, and no test asserts a deserializer's close count |
| F13 | Python's tests, by name | Confirmed | `pyt/test_kafka_producer.py`: `test_serializer_close` `:184`, `test_given_serializers_are_closed_at_close_but_not_configured` `:626`, `test_a_failed_construction_closes_the_serializers_built` `:635`, `test_a_failing_configure_…` `:674`, `test_async_producer_closes_its_serializers` `:717`, `test_a_serializer_close_that_raises_is_logged_not_raised` `:738`, its async twin `:752`. `pyt/test_kafka_consumer.py`: `:2142`, `:2167`, `:2177`, `:2197`, `:2297`, `:2311`, `:2327`. Also `pyt/test_serde_supply.py:246`, `:251`, `:260`; `pyt/test_producer_family.py:2823` (a cancelled close disposes after its teardown) |
| F14 | A failing `Dispose` written to "one `Trace.TraceWarning` (Python's way, and decision 20's convention)" | **Corrected** | Decision 20 writes one **`Trace.TraceError`** (ledger 20; `src/Internal/DeliveryState.cs:217`; FM §A6). `TraceWarning` is what the binding writes for the finalizer leak (`src/Internal/Interop/SafeClientHandle.cs:71`) and for Java's close-in-callback warning (`src/Internal/NativeProducer.cs:670`). Python logs WARNING (F9); Java ERROR (F8). D15 picks Python's level |
| F15 | `Poll` deserializes outside the in-flight use (ledger 130 (a)), so `Dispose` can run while another thread's `Poll` is deserializing (row 339) | Confirmed; widened | Sync: the use covers only `ConsumerPoll`; `CopyOut` runs after it (`src/Internal/NativeConsumer.cs:402-416`). Async: `ExitUse` before `CopyOut` (`src/Internal/NativeConsumer.Async.cs:1267-1281`). **Python has the same window** (`resolve(payload)` after `with self._use()`, `py/consumer/_base.py:1055-1097`; no guard). **Java does not**: a `close()` from another thread fails in `acquire()`. **The producer has the window in all three:** .NET `Send` serializes before `EnterUse` (`src/Producer/KafkaProducer.cs:96-108`; `src/Internal/NativeProducer.cs:222`), Python before `_send_use` (`py/producer/producer.py:517`, `:539`), and Java's `close()` does not wait for a `send` serializing on another thread |
| F16 | The Rust core made the same constructor choice | Confirmed | `core/producer/kafka_producer.rs:858-867` (and the doc at `:918-921`); `core/consumer/internals/async_kafka_consumer.rs:3480-3495`; `ProducerConfig::set_partitioner` `core/producer/producer_config.rs:1698` |
| F17 | DC's claim that a default interface method can be added later is wrong (CS8701) | Confirmed | DC:600-601. Probe (scratchpad `probes/Dim`, SDK 10.0.400): an interface member with a body fails on the netstandard2.0 asset with CS8701 "Target runtime doesn't support default interface implementation."; net8.0 builds. DC:801-802 already states the floor's limit for the listener |
| F18 | — | Added: Java's constants | Non-`String` public constants: `ConsumerConfig` `ASSIGN_FROM_SUBSCRIBED_ASSIGNORS` (`List<String>`, `:71`), five `int`s (`DEFAULT_MAX_POLL_RECORDS` `:100`, …) and two `boolean`s (`:356`, `:389`); `AdminClientConfig.DEFAULT_METADATA_RECOVERY_REBOOTSTRAP_TRIGGER_MS` (`long`, `:155`); `SaslConfigs`: four `double`, five `long`, two `short` (`DEFAULT_LOGIN_REFRESH_MIN_PERIOD_SECONDS` `:102`, `DEFAULT_LOGIN_REFRESH_BUFFER_SECONDS` `:110`), three `int`, two `boolean`; `SslConfigs` `RECONFIGURABLE_CONFIGS`, `NON_RECONFIGURABLE_CONFIGS` (`Set<String>`, `:150`, `:162`); `LogLevelConfig.VALID_LOG_LEVELS` (`Set<String>`, `:68`). **Not compile-time constants in Java** (JLS §15.29): `ConsumerConfig.DEFAULT_GROUP_PROTOCOL` (a method chain, `:121`), `DEFAULT_GROUP_REMOTE_ASSIGNOR` (`null`, `:129`), `DEFAULT_ISOLATION_LEVEL` (`:382`), `DEFAULT_SHARE_ACQUIRE_MODE` (`:416`), `SslConfigs.DEFAULT_SSL_KEYMANGER_ALGORITHM` and `DEFAULT_SSL_TRUSTMANAGER_ALGORITHM` (JDK calls, `:106`, `:111`; Python hard-codes `SunX509` / `PKIX`, `X/config_consts.rs:38-41`), and the four collections. **Deprecated:** `ProducerConfig.PARTITIONER_ADPATIVE_PARTITIONING_ENABLE_CONFIG` (`:107-108`), `TopicConfig.MESSAGE_DOWNCONVERSION_ENABLE_CONFIG` and `_DOC` (`:273-281`), left out through `design/current/java-deprecated.txt:48`, `:51`, `:52`, written by `cargo xtask java-deprecated` from `DEPRECATION_REFS = ["4.3.1", "4.4.0-rc3"]` (`X/java.rs:95`); the generator ignores the annotation itself (`X/config_consts.rs:595-613`, `:1205`) |
| F19 | — | Added: Python's class shapes and tests | The three client classes' `__init__` raises `TypeError("ProducerConfig is not instantiable: its ConfigDef machinery is not translated; a configuration is a dict of its keys")` (`py/producer/producer_config.py:106-108`); the other five construct (`__slots__ = ()`, Java's implicit default constructor). A `Set` is a `frozenset`, a `List` a tuple, `null` is `None`; the `.pyi` gives `int` for Java's `short` and `long`. `pyt/test_config_classes.py`: `:61` (the only Java-source check: a regex over `public static final String X = "literal";`, deprecated names absent), `:77`, `:105`, `:113`, `:119`, `:125`, `:134`, `:138`, `:148`, `:166`, `:188`; `pyt/test_config_class_ports.py:90`, `:98` |
| F20 | — | Added: the three enums in Java and Python | `SecurityProtocol(short id, String name)` (`J/common/security/auth/SecurityProtocol.java:32-61`): `names()` `:66`, `forId(short)` returns `null` on a miss (`:70`), `forName(String)` is `valueOf(name.toUpperCase(Locale.ROOT))` (`:75`, so an unknown name throws `IllegalArgumentException` "No enum constant org.apache.kafka.common.security.auth.SecurityProtocol.X", a `null` an NPE); no `toString` override. `SslClientAuth` (`J/common/config/SslClientAuth.java:30-52`): `VALUES = List.of(values())`, `forConfig` (`null` → `NONE`, case-insensitive match, unknown → `null`), and `toString` **overridden** to lower case. `GroupProtocol(String name)` (`J/clients/consumer/GroupProtocol.java:34-57`): `of(String)` as `forName`; no override. Python: `SecurityProtocol(IntEnum)` by id, `for_id` → `None`, `for_name` → `TypeError` for `None`, `IllegalArgumentError` with Java's text; `SslClientAuth(Enum)` by name, `VALUES` tuple, `for_config`, `__str__` lower case; `GroupProtocol(Enum)`, `of` as `for_name`. **No Java test of the three enums exists** anywhere under `kafka/` |
| F21 | — | Added: what Python's own code uses | Literal keys everywhere: the library (`py/producer/_base.py:120-121`, `py/consumer/_base.py:162-167`, `py/_config.py:78-80`), the tests (157 lines of `"bootstrap.servers"` in 41 files), the gRPC servers, soak, examples. The constants appear only in `pyt/test_config_classes.py` and `pyt/test_config_class_ports.py` (28 lines, e.g. `:61` `BOOTSTRAP = {ProducerConfig.BOOTSTRAP_SERVERS_CONFIG: …}`). Java's own Javadoc examples use literal keys too |
| F22 | — | Added: .NET precedents | Constants: `public const` for primitives and strings (`src/Producer/RecordMetadata.cs:30`, `src/Consumer/ConsumerRecord.cs:40`, `src/Common/Security/Auth/KafkaPrincipal.cs:31`, `src/Admin/MockAdminClient.cs:41`); a static get-only property for an object or a collection (`src/Common/Uuid.cs:48-60`, `RESERVED` an `IReadOnlyCollection<Uuid>` over a `ReadOnlyCollection`; `KafkaPrincipal.cs:58`; `MockAdminClient.cs:47`, `DEFAULT_LOG_DIRS` an `IReadOnlyList<string>`). An enum's Java integer field is its underlying value (`src/Common/Acl/AclOperation.cs:24`, `src/Common/Resource/ResourceType.cs:22`, `ScramMechanism : byte`). A Java enum's static methods sit in a plural static class beside it, not as extensions (`ScramMechanisms` `src/Admin/ScramMechanism.cs:42`, `UpgradeTypes` `src/Admin/FeatureUpdate.cs:134`, whose summary says "Java's `code()` needs no counterpart: a cast yields it", `:131-132`); extension methods sit in `*Extensions` (`GroupStateExtensions`, `src/Common/GroupState.cs:84`). Java's `toString` overrides are internal maps (`src/Internal/Interop/GroupMarshal.cs:133-159`, `TransactionMarshal.cs:29-31`) |
| F23 | — | Added: layout test | Every exported type needs a row in `JavaCounterparts` (`tests/PublicNamespaceLayoutTests.cs:41-329`; checked by `:342-354`); a .NET-only type a `=` row next to its pair (`("ScramMechanisms", "=ScramMechanism")`, `:224`); a Java row's source must exist in `kafka/` (`:424-447`). So a public type added without its row fails the unit tests |
| F24 | — | Added: probes (scratchpad, SDK 10.0.400) | **Probe `Shape`**, with the repo's `.editorconfig` and `Directory.Build.props` settings (`EnforceCodeStyleInBuild`, `TreatWarningsAsErrors`, `AnalysisLevel` latest, `dotnet/Directory.Build.props:16-18`; `GenerateDocumentationFile`, `src/Confluent.Kafka.csproj:16`) on netstandard2.0, net8.0 and net10.0: a class with a private constructor (sealed or not), a class with Java's implicit public constructor (sealed or not), `const short`, `const double`, `public const string? X = null`, `enum : short`, and a static method returning `SecurityProtocol?` all build with 0 warnings; no CA1052 or naming rule fires (IDE1006's `pascal_case` checks only the first letter and CA1707 is off, `design/current/STATUS.md:51`). A public member in a `.g.cs` file without `///` fails with CS1591, so generated public code needs docs. **Probe `App`:** `new ProducerConfig()` against a private constructor fails with CS0122 |
| F25 | — | Added: .NET teardown | Producer: `CloseWithin` (`src/Internal/NativeProducer.cs:901-915`) → `CloseAndTearDown` (`:958-973`) → `TearDown` (`:1006-1012`: join the drain thread, wait for the uses, `CloseExplicitly`); a close from a callback queues `close_with_timeout_cb(0)` and leaves `TearDown` to the drain thread after its loop (`:932-956`; `src/Internal/CompletionThread.cs:147-172`); `Dispose` / `DisposeAsync` `:402-448`. Consumer: `CloseSync` (`src/Internal/NativeConsumer.cs:1348-1400`), `ReopenIfRefused` (`:1402-1417`), `Dispose` (`:1883-1922`), `DisposeInsideACallback` (`:1924-1936`; the async consumers run `DisposeOnThePumpAsync` on a pool thread), `TearDown` (`:1971-1986`); async `CloseOnceAsync` (`src/Internal/NativeConsumer.Async.cs:1374-1420`), `DisposeOnThePumpAsync` (`:1422-1465`). Finalizer: no destroy (`src/Internal/Interop/SafeClientHandle.cs:55-80`). The serde fields are set before the native client (`src/Producer/KafkaProducer.cs:64-66`, `src/Consumer/KafkaConsumer.cs:71-73` and the async twins); after that, `ClientConfigs.Prepare` can throw `ConfigException` (`NativeProducer.cs:97`, `NativeConsumer.cs:156`) and the native construction can fail (`src/Internal/Interop/ConfigMarshal.cs:56-75`; `NativeProducer.cs:101-107`, `:502-517`; `NativeConsumer.cs:160-166`; `NativeConsumer.Async.cs:654-672`). The mocks' `Close` / `Dispose` forward to the same native cores (`src/Producer/MockProducer.cs:115`, `:118`, `:154`; `src/Consumer/MockConsumer.cs:170`, `:173`, `:328` and the async twins), so disposal must be registered by the four real clients only |
| F26 | — | Added: where .NET writes key literals | UnitTests 236 occurrences of the eight common keys in 69 files; IntegrationTests 5 / 1; GrpcServer.UnitTests 3 / 1 (proto maps); gRPC server 0 (its keys come from the proto, `dotnet/grpc-server/Translate.cs:48-56`); soak 61 / 5 (and a config file, `dotnet/soak/SoakClient/SoakConfig.cs:225`); perf 28 / 7 (PerfV2 is ckd's client with librdkafka keys); tools 5 / 1; README 4. M22/P1's ports of Java's config-class tests write literals (`tests/PublicClientConfigTests.cs:89-110`), where Java's tests and Python's port use the constants |

## 2. Parity anchor

| # | Item | Python | Java | .NET today |
|---|---|---|---|---|
| A1 | The eight config classes | generated constants (F1, F2) | `public static final` fields | none |
| A2 | Their construction | the client classes raise `TypeError`; the other five construct (F19) | public constructors; implicit default ones | — |
| A3 | Their methods | not translated | `configNames`, `configDef`, `main`, `append…ToConfig`, `add…Support` | — |
| A4 | Constant types | `str`, `int` (for `short`, `int`, `long`), `float`, `bool`, `None`, `tuple`, `frozenset` | declared Java types | — |
| A5 | Deprecated constants | left out (F18) | `@Deprecated` | — |
| A6 | The three enums | hand-written, methods on the enum (F2, F20) | enums with fields and static methods | none |
| A7 | Own code uses the constants | no (F21) | no (Javadoc examples) | — |
| A8 | Given serdes closed at close | yes, last, WARNING, never raised (F9) | yes, after the I/O join, ERROR, rethrown (F8) | never (F11) |
| A9 | On a failed construction | a core failure closes both; a config error closes nothing (F9) | only after they were wrapped; a config error closes nothing (F8) | never |
| A10 | Mocks | never close (F10) | never close (F10) | never |
| A11 | Same instance as key and value | closed twice | closed twice | — |
| A12 | An async `close` | never awaited (F9) | `Closeable` is synchronous | — |
| A13 | Dispose vs a running (de)serialize | not guarded, consumer and producer (F15) | consumer impossible; producer not guarded | — |
| A14 | Config-only constructors | yes, with a config route and a bytes default | `(Map)`, `(Properties)` | none (Part 3 keeps it so) |

**Ceiling.** New public surface: eight classes of constants, three enums, three static classes beside them. No
other public member changes; Part 2 changes behaviour, not signatures.

**NOT adding:** config objects, constructors or setters on the config classes; `configNames()` / `configDef()`;
a config route or `Configure`; `IAsyncDisposable` handling; a gate around (de)serialization; constants for
`CommonClientConfigs` or `ShareConsumerConfig`; any use of the constants in the binding's own code.

## 3. Decisions

Grouped as the brief asks: the facts; the picks that are Python's way, in one table; the items with no Python
way or a pick away from it, one at a time; the "keep .NET" picks; Part 3's ruling; the S0 approval.

### 3.1 Group 1 — the fact refresh (one approval; ledger 366)

Approve §1.2 F1–F26 as the facts this plan stands on. F2, F12 and F14 correct the brief; F7, F15 and F18 change
details of D1, D18, D20 and D21.

### 3.2 Group 2 — Python's way (one table; ledger 367–385)

| D | Item | Python | Java | .NET pick | Breaking |
|---|---|---|---|---|---|
| D1 | How the eight classes are written | generated from `kafka/` (`X/config_consts.rs`) | source | generated by a C# renderer added to `X/config_consts.rs`, fed into `all_outputs` (`X/config_types.rs:793-797`), so `generate-error-codes` writes them and `check-generated` checks them; Python's output byte-identical. S1, Rust lane. **Same as Python** | no |
| D2 | How the three enums are written | hand-written (F2) | source | hand-written, as Python's (S2). **Same as Python** | no |
| D3 | Which fields | every `public static final` field of the eight, `AbstractConfig`'s three flattened into the client classes, `_DOC`s included, deprecated ones left out through `java-deprecated.txt` (F18) | — | the same 420 fields. **Same as Python** | no |
| D4 | Namespaces and files | the module of the Java package | packages | `Confluent.Kafka.Producer.ProducerConfig` (`src/Producer/ProducerConfig.g.cs`), `Consumer.ConsumerConfig`, `Admin.AdminClientConfig`, `Common.Config.{TopicConfig, SaslConfigs, SslConfigs, SecurityConfig, LogLevelConfig}` (`src/Common/Config/*.g.cs`), `Common.Config.SslClientAuth`, `Common.Security.Auth.SecurityProtocol`, `Consumer.GroupProtocol` (each enum in `<Name>.cs` with its static class, DC §2); rows in `PublicNamespaceLayoutTests`. **Same as both** (ledger 253) | no |
| D5 | The three client classes' shape | not instantiable (`TypeError`, F19) | public constructors, a non-final class | an ordinary, non-static, **unsealed** class with a **private parameterless constructor**, so `new ProducerConfig()` is CS0122 (F24); nothing outside can derive from it today. When the public constructor and setters arrive (Part 3), adding them is additive; the private constructor may stay or go (removing a private member is invisible), and sealing or not is decided then without a break, as nothing can derive before. Python's refusal, at compile time. **Same as Python** (deviation from Java's public constructors, as Python's). Alternative: `sealed` now (unsealing later is non-breaking too) | no |
| D6 | The five key-name classes' shape | construct (`__slots__ = ()`) | implicit public default constructor, not final | an unsealed class with Java's implicit public constructor (F24: no analyzer objects). **Same as both**. Alternative: `static` classes, the usual .NET form for constants holders, which refuses `new TopicConfig()` (Python allows it) | no |
| D7 | Enum values and Java's fields | `SecurityProtocol` an `IntEnum` by id; the others by name (F20) | `short id`, `String name` fields | `enum SecurityProtocol : short { PLAINTEXT = 0, SSL = 1, SASL_PLAINTEXT = 2, SASL_SSL = 3 }`; `SslClientAuth { REQUIRED, REQUESTED, NONE }` and `GroupProtocol { CLASSIC, CONSUMER }` in Java's order. Java's `id` is the cast, its `name` field `ToString()` (each equals the constant's name), as `UpgradeTypes` documents (F22). **Same as Python** | no |
| D8 | The enums' Java methods | on the enum (F20) | static methods | in a static class beside each enum (D22): `Names()` (`IReadOnlyList<string>`, Java order); `ForId(short)` → `SecurityProtocol?`, `null` on a miss; `ForName(string)` and `GroupProtocols.Of(string)` case-insensitive (`ToUpperInvariant`, Java's `Locale.ROOT`), an unknown name `ArgumentException` with Java's text "No enum constant org.apache.kafka.common.security.auth.SecurityProtocol.X" (`…clients.consumer.GroupProtocol.X`), `null` `ArgumentNullException` (Python `TypeError`, Java NPE; the error table, DC §3); `SslClientAuths.VALUES` (`IReadOnlyList<SslClientAuth>`, Java order); `ForConfig(string?)` → `SslClientAuth?`: `null` → `NONE`, case-insensitive match, unknown → `null`. **Same as both** | no |
| D9 | The binding's own code, tests, README, gRPC server, soak, perf | literals (F21) | literals in Javadoc | not switched: the library keeps its private literals, the harness its literals and proto keys (F26), the README its literal example. Only the tests that translate Java's config-class tests use the constants, as Java's tests and Python's port do: `tests/PublicClientConfigTests.cs:89-110` and its siblings, plus the new constants tests (§7). **Same as Python** | no |
| D10 | Tests of the constants | `pyt/test_config_classes.py`, `pyt/test_config_class_ports.py` (F19) | none for the enums (F20) | translated (§7), with Python's Java-source check as a unit test over `kafka/`, besides `check-generated`. **Same as Python** | no |
| D11 | What is disposed | each given serde (`close_if_defined`) | each given serializer (`closeQuietly`) | each serializer or deserializer given to `KafkaProducer`, `AsyncKafkaProducer`, `KafkaConsumer` or `AsyncKafkaConsumer` that implements `IDisposable` (`Serdes` members do not). **Same as both** | behaviour (D19) |
| D12 | When, and on which path | once, last: after the join and the destroy (F9) | after the I/O join | once, as step 6 of DC §4 **Close/Dispose**, after the destroy, on the thread that destroyed: `Close()`, `Close(TimeSpan)`, `Close(CloseOptions)`, `Dispose`, `DisposeAsync` on the closing thread; a close from a delivery callback on the producer's drain thread after its deferred teardown (Python: the completion thread, `py/producer/producer.py:291-295`); the async consumers' `Dispose` inside a callback (FM §B2 path 2) on the pool thread after the deferred teardown. The async producer's `Close` reads its token only before it starts (`src/Internal/NativeProducer.cs:884-899`): a token that fires later changes nothing and the serializers are disposed after the teardown, as Python's cancelled close disposes them after its teardown (`pyt/test_producer_family.py:2823`); a token already cancelled at the call closes and disposes nothing. A close that fails in the core still disposes (Python's `finally`; Java's `closeQuietly`). A close that is refused and leaves the client open disposes nothing (Python, F9): a cross-thread `Close`, a `Close` inside a callback, the core's −2. A second close disposes nothing more. Finalization disposes nothing (no destroy either). **Same as Python** | behaviour |
| D13 | Order; one instance as key and value | key, then value; twice | key, then value; twice | key, then value; an instance given as both is disposed twice, which `IDisposable`'s contract makes harmless. **Same as both** | — |
| D14 | A constructor that fails after taking them | a core failure disposes both; `prepare()`'s error nothing (F9) | only after they were wrapped; a config error nothing (F8) | a native construction failure (the core refuses the configuration, or the class, view or notify setup fails, F25) disposes both, key then value, before the exception propagates; a `ConfigException` from the binding's own check, or an `ArgumentNullException`, disposes nothing. **Same as Python** (Java differs only for failures before it wraps them) | behaviour |
| D15 | A failing `Dispose` | logged at WARNING, "Exception while closing serde %r", never raised (F9) | logged at ERROR, the first rethrown as "Failed to close kafka producer" / "…consumer" (F8) | caught; the other one still disposed; one `Trace.TraceWarning` per failure, Python's text: "Exception while closing serde {serde}: {exception}"; never rethrown. Python's level, not decision 20's `TraceError` (F14). **Same as Python**; away from Java's rethrow | — |
| D16 | The mocks | never close (F10) | never close | `MockProducer`, `AsyncMockProducer`, `MockConsumer`, `AsyncMockConsumer` never dispose them, on `Close` or `Dispose`. **Same as both** | no |
| D17 | `IDisposable` only, or `IAsyncDisposable` too | `close()` synchronous, never awaited (F9) | `Closeable` synchronous | `IDisposable` only, on every path, `DisposeAsync` included; a serializer that is only `IAsyncDisposable` is not disposed, and the XML docs say so. **Same as both**. Alternative, with no Java or Python way: `DisposeAsync` prefers `IAsyncDisposable.DisposeAsync`, the others `Dispose` (two behaviours by path) | — |
| D18 | `Dispose` while another thread (de)serializes | not guarded (F15) | consumer impossible, producer not guarded | recorded, no gate, as M21/P3's row 339 recorded it: a gate would bring back ledger 130's deadlock for a deserializer that disposes its consumer. FM §B2 says so (R11). **Same as Python** | — |
| D19 | The commit type | — | — | `feat(dotnet)!`: a user who shares one `IDisposable` serializer between clients, or reuses it after closing the client, sees it disposed | — |

### 3.3 Group 3 — one ruling each

**D20 — `const` or not (ledger 386).** No Python way: Python's `Final` is a typing marker, and every value is read
at run time. Java's `javac` inlines a compile-time constant (a `final` primitive or `String` whose initializer is a
constant expression, JLS §15.29) into the caller's class file and reads every other field at run time; C#'s
`const` and a static property behave the same way.
- (a) Java's split: a `const` where Java's field is a compile-time constant, a static get-only property otherwise
  (F22's form for objects and collections). By F18 that is 10 fields: `ConsumerConfig.DEFAULT_GROUP_PROTOCOL`,
  `DEFAULT_GROUP_REMOTE_ASSIGNOR` (`string?`, `null`), `DEFAULT_ISOLATION_LEVEL`, `DEFAULT_SHARE_ACQUIRE_MODE`,
  `SslConfigs.DEFAULT_SSL_KEYMANGER_ALGORITHM`, `DEFAULT_SSL_TRUSTMANAGER_ALGORITHM`, and the four collections; S1
  reports the exact set. A later Kafka change of such a default then reaches a .NET caller without a recompile,
  as it reaches a Java caller. The generator tracks constant-ness while it evaluates (F7).
- (b) every scalar a `const`, the collections properties: simpler; a changed default reaches a caller only when it
  recompiles, where Java's caller gets it at once for those 6.
- (c) every field a static property: no inlining at all, and no field usable in a `switch` label or an attribute,
  where Java's compile-time constants are usable in a `case` label.

**Pick (a).**

**D21 — the C# type of each constant (ledger 387).** No Python way: Python has one `int` (A4).
- (a) Java's declared type: `string`, `int`, `long`, `short`, `double`, `bool`, `List<String>` →
  `IReadOnlyList<string>`, `Set<String>` → `IReadOnlyCollection<string>` (D26). S1 extends the field model to keep
  the declared type (F7) and stops generation on any other type.
- (b) the evaluated value's kind, as Python's stub (`short` and `long` become `int`).

(b) breaks the constants against M22/P1's typed configuration. A SHORT key takes only a `short`
(ledger 352), and the two SHORT keys are exactly the ones with `short` Java defaults
(`sasl.login.refresh.buffer.seconds`, `sasl.login.refresh.min.period.seconds`,
`src/Internal/ConfigTypes.g.cs:110-111`; F18). Under (b),
`configs[SaslConfigs.SASL_LOGIN_REFRESH_MIN_PERIOD_SECONDS] = SaslConfigs.DEFAULT_LOGIN_REFRESH_MIN_PERIOD_SECONDS`
would throw `ConfigException`. **Pick (a).**

**D22 — the static classes beside the enums (ledger 388).** No Python way: Python puts the methods on the enum,
which C# cannot. The DC §2 precedent is a plural static class for static methods, and `*Extensions` for
extension methods (F22). Java's methods here are static and take a `short` or a `string`, not the enum.
- (a) `SecurityProtocols` (`Names()`, `ForId`, `ForName`), `SslClientAuths` (`VALUES`, `ForConfig`),
  `GroupProtocols` (`Of`), plain static methods, each in its enum's file, with a `=` layout row.
- (b) `SecurityProtocolExtensions`, … as non-extension statics: the name would then promise extension methods
  that do not exist.

**Pick (a).**

**D23 — the XML docs on the generated members (ledger 389).** No Python way; CS1591 requires them (F24).
- (a) One summary line per member that names Java's field, "Java's `ProducerConfig.LINGER_MS_CONFIG`.", and one
  per class, "Java's `org.apache.kafka.clients.producer.ProducerConfig`: its `public static final` fields."
  DC §4's comment rule 4 allows naming a class or member.
- (b) For a `_CONFIG` key, the summary is its `_DOC` text. That means new HTML-to-XML-doc conversion in the
  generator (`<code>`, `<ul>`, entities), and it doubles the generated text.

**Pick (a).**

### 3.4 Group 4 — keep .NET's way (ledger 390–392)

| D | Python | Java | .NET (kept) | Clean Python way? |
|---|---|---|---|---|
| D24 `SslClientAuth`'s text | `__str__` lower case (`required`) | `toString` lower case | `ToString()` prints the constant (`REQUIRED`), ledger 285's recorded deviation for an enum that overrides `toString`; the binding never prints or sends it, so no internal map is added (F22) | no: a C# enum cannot override `ToString`, and a public replacement would need a name Java does not have |
| D25 Collections | `tuple`, `frozenset` | `List.of`, `Set.of` | `IReadOnlyList<T>` / `IReadOnlyCollection<T>` static properties over a `ReadOnlyCollection<T>` in Java's source order (`Uuid.RESERVED`, `DEFAULT_LOG_DIRS`, F22); `IReadOnlySet` does not exist on netstandard2.0 (DC §3) | no: these are the idiom map's types |
| D26 Generated file names | `producer_config.py` (+ `.pyi`) | — | `ProducerConfig.g.cs`, as `Internal/ConfigTypes.g.cs`, so a reader and `dotnet format` see the generated marker | no |

### 3.5 Part 3 — constructor arguments only (Pranav's ruling, 2026-11-06; ledger 365, Agreed)

Recorded as ruled, not a question. Pranav's ruling of 2026-11-06, following Emanuele's proposal (the evaluation's
option A):
- Java's config-only constructors, `KafkaProducer(Map)`, `KafkaProducer(Properties)`, `KafkaConsumer(Map)` and
  `KafkaConsumer(Properties)`, are not translated. The Rust core made the same choice (F16).
- Serializers and deserializers are constructor arguments only, set up by the user before they are passed, as Java
  allows: Java never calls `configure()` on a serializer it is given.
- Nothing is built by reflection. `key.serializer` and its peers stay neither checked nor sent (ledger 356).
- Why (Emanuele): a C# constructor cannot be async, and Java calls a possibly blocking `configure()` on the classes
  it builds by reflection. Avoiding reflection also keeps trimming and AOT open.
- Later, each setting that Java fills by reflection (`partitioner.class`, `interceptor.classes`, `metric.reporters`,
  `config.providers`, the SASL and OAuth handler classes, `ssl.engine.factory.class`) gets a setter on
  `ProducerConfig` / `ConsumerConfig` taking an object the user built, as the Rust core's
  `ProducerConfig::set_partitioner` does. No setter is part of this phase.

What this phase does with it: the ledger row (365), the rule text R1, R2 and R6–R7 (§6.1) with the recorded
deviation from Java's public constructors and from Python's config route and bytes default, R3's CS8701
correction (F17), and the proposed note on row 356 (§6.2). D5's private constructor keeps the config object
and its setters additive.

### 3.6 S0 approval (ledger 393) and logistics (ledger 394)

**D27.** Approve R1–R11 (§6.1) and the row 356 note (§6.2) verbatim, as drafted for the picks. A different ruling
on D5, D12, D15, D17, D20, D21 or D25 changes the clause that names it; I redraft before S0.
**D28.** N=109; the stacked branch; S1 in the Rust lane (root `actor-executor`, `kafka-critic`, the full hook,
comments in the repo-root `COMMENTS.109.md`); S2–S7 by `dotnet-actor` / `dotnet-critic` (`dotnet/COMMENTS.109.md`);
a Critic pass after each Actor commit; squashing decided by Pranav at the end.

## 4. What the steps build

- **`X/config_consts.rs`** (S1): the field model gains the declared Java type and, under D20 (a), a compile-time
  constant flag; a C# renderer writes eight `.g.cs` files (D4, D5, D6, D20, D21, D23, D25, D26). It escapes C#
  string literals (quotes, backslashes, control characters), prints each `double` so C# reads back Java's value,
  and adds the eight files to `outputs()`, so `all_outputs` (`X/config_types.rs:793-797`) feeds both `generate` and
  `check_up_to_date`. It gets an xtask unit test for the C# output, like `test_cs_output_is_generated_and_checked`
  (`X/config_types.rs:976`). The Python output stays byte-identical.
- **`src/Common/Security/Auth/SecurityProtocol.cs`, `src/Common/Config/SslClientAuth.cs`, `src/Consumer/GroupProtocol.cs`**
  (S2): each enum and its static class (D7, D8, D22).
- **Disposal** (S4, S5): an internal helper doing Python's `close_if_defined` in .NET terms (`is IDisposable` →
  `Dispose()` in a `try`, D15). Each real client hands its two serdes to its native core when it builds it, and
  the mocks hand none (F25). The core disposes them after `CloseExplicitly` in `TearDown`, and on a failed native
  construction (D14). A `ConfigException` from `Prepare` is raised before the core takes them, so nothing is
  disposed for it. The Actor picks the shape and keeps it to these points.

## 5. Mode A

The C ABI is untouched: constants are values in the binding, and disposal is managed-only. The one non-.NET change
is the generator (S1), Rust tooling that adds no Kafka behaviour.

## 6. Steps (one commit each)

- **S0 — rules** (markdown only): R1–R11 verbatim; the row 356 note (§6.2) in the untracked ledger.
- **S1 — the generated constants** (Rust lane; D1, D3–D6, D20, D21, D23, D25, D26): §4's generator change; the eight
  `.g.cs` files; and the eight `JavaCounterparts` rows (`tests/PublicNamespaceLayoutTests.cs`). The rows are an
  eight-line C# test edit, made here so that the unit tests stay green at this commit (F23); `dotnet-critic`
  reviews those lines.
- **S2 — the enums** (D2, D7, D8, D22, D24): three files, six layout rows, and the enum tests (§7).
- **S3 — the constants tests** (D9, D10): §7's constants tests; the Java-port test rows switched to the constants.
- **S4 — the producer disposes its serializers** (D11–D17, D19; `feat(dotnet)!`): `KafkaProducer` and
  `AsyncKafkaProducer`, their XML docs (constructors, `Close`, `Dispose`), tests (§7).
- **S5 — the consumer disposes its deserializers** (D11–D19; `feat(dotnet)!`): `KafkaConsumer` and
  `AsyncKafkaConsumer`, their XML docs, tests (§7).
- **S6 — comment-only commit** (ledger 59).
- **S7 — close-out**: the STATUS entry; `design/current` updated; `COMMENTS.DONE.109.md` (and the S1 Rust-lane one)
  archived here; the ledger statuses; this plan and its folder committed with the phase (DC §8.4).

### 6.1 S0 rule drafts (written at `3f7aa45f5`; each replaces exactly the text quoted)

**R1 — DC §3, the producer sketch's comment (`DC:284`).** Replace
```
// Real clients take the serializers — Java KafkaProducer(Map, Serializer<K>, Serializer<V>).
```
with
```
// Real clients take the serializers — Java KafkaProducer(Map, Serializer<K>, Serializer<V>). Java's (Map) and
// (Properties) constructors are not translated, and a given serializer that is IDisposable is disposed at the
// producer's teardown (§4 Serializers, Close/Dispose).
```

**R2 — DC §3, the async consumer sketch's comment (`DC:435`).** Replace
```
    // 3-param ctor (decision A) — Java KafkaConsumer(Map, Deserializer<K>, Deserializer<V>), KafkaConsumer.java:601
```
with
```
    // 3-param ctor (decision A) — Java KafkaConsumer(Map, Deserializer<K>, Deserializer<V>), KafkaConsumer.java:609.
    // Java's (Map) and (Properties) constructors are not translated (§4 Serializers).
```
(`:601` is stale: the `(Map, D, D)` constructor is `J/clients/consumer/KafkaConsumer.java:609`.)

**R3 — DC §3, the Serdes deviations' headers bullet (`DC:600-601`).** Replace
```
- **Headers overload deferred** (Java's `default T deserialize(String, Headers, byte[])`) —
  addable later non-breakingly as a C# default-interface-method forwarding to the header-less form.
```
with
```
- **Headers overload deferred** (Java's `default T deserialize(String, Headers, byte[])`, ledger 153). A C#
  default interface method does not compile while netstandard2.0 is a target (CS8701), so a member added to
  `IDeserializer<T>` or `ISerializer<T>` breaks every implementer; its shape is decided when it is added.
```

**R4 — DC §3, the `ISerde<T>` bullet (`DC:610-611`).** Replace
```
  `Serde<T>` uses `serializer()`/`deserializer()` accessors + `Closeable`; ours extends both
  directly (stateless serdes, nothing to close).
```
with
```
  `Serde<T>` uses `serializer()`/`deserializer()` accessors + `Closeable`; ours extends both
  directly. Java's `close()` is `IDisposable`: the built-in serdes are stateless and not `IDisposable`, and a
  user's serde that is `IDisposable` is disposed by the client it was given to (§4 **Close/Dispose**).
```

**R5 — DC §3, the idiom map's constants row (`DC:631`).** Replace the row's last cell separator
"`… and the enum's own `ToString()` is a recorded deviation | ledger 285 |`" with
"`… and the enum's own `ToString()` is a recorded deviation. The eight config classes (`ProducerConfig`,
`ConsumerConfig`, `AdminClientConfig`, `TopicConfig`, `SaslConfigs`, `SslConfigs`, `SecurityConfig`,
`LogLevelConfig`) carry only Java's `public static final` fields, generated from `kafka/` (`*.g.cs`,
`cargo xtask generate-error-codes`), each with Java's declared type: a Java compile-time constant is a `const`,
any other field a static get-only property, a `List` an `IReadOnlyList`, a `Set` an `IReadOnlyCollection`. Their
Java methods and the three client classes' constructors are not translated (M22/P2) | ledger 285; M22/P2 |`".

**R6 — DC §4, the "Serializers" row (`DC:673`).** Replace
"See §3 for the sketch + the deliberate deviations. | ✅ M6/P1a (foundation) · ✅ M6/P1b (typed consumers) · ✅ M11/P5 (typed producer) |"
with
"See §3 for the sketch + the deliberate deviations. **Constructor arguments only** (ledger 365): Java's config-only constructors `KafkaProducer(Map)`, `KafkaProducer(Properties)`, `KafkaConsumer(Map)` and `KafkaConsumer(Properties)` are not translated, and nothing is built by reflection: the user builds and sets up each serializer and deserializer and passes it, as Java allows (Java never calls `configure()` on one it is given), and `key.serializer` and its peers are neither checked nor sent (§4 **Config**). Each setting Java fills by reflection (`partitioner.class`, `interceptor.classes`, `metric.reporters`, `config.providers`, the SASL and OAuth handler classes, `ssl.engine.factory.class`) gets, when it is translated, a setter on `ProducerConfig` / `ConsumerConfig` that takes an object the user built, as the Rust core's `ProducerConfig::set_partitioner` does. *(Deviation from Java's public constructors, and from Python's, whose configuration route resolves and configures `key.serializer` and falls back to a bytes serde. Why: a C# constructor cannot be async, and Java calls a possibly blocking `configure()` on the classes it builds by reflection; without reflection, trimming and native AOT stay open. The Rust core makes the same choice.)* **Disposal** (M22/P2): the four real clients dispose each serializer or deserializer they were given that is `IDisposable`, once, key then value, after the destroy (§4 **Close/Dispose**, step 6), and when the native client cannot be built; a throwing `Dispose` is written to one `Trace.TraceWarning` and never rethrown, as Python logs and never raises. An `IAsyncDisposable`-only serde is not disposed. The mocks never dispose them, as Java's and Python's mocks. | ✅ M6/P1a (foundation) · ✅ M6/P1b (typed consumers) · ✅ M11/P5 (typed producer) · ✅ M22/P2 (constructor arguments only; disposal) |"

**R7 — DC §4, the "Config" row (`DC:674`).** Replace
"The five own-role serializer keys (`key.serializer`, `value.serializer`, `serializer.encoding`, `key.serializer.encoding`, `value.serializer.encoding`; consumer: the deserializer ones) are neither checked nor sent, as in Python."
with
"The five own-role serializer keys (`key.serializer`, `value.serializer`, `serializer.encoding`, `key.serializer.encoding`, `value.serializer.encoding`; consumer: the deserializer ones) are neither checked nor sent, as in Python; nothing reads them, since a serializer is a constructor argument only (§4 **Serializers**, ledger 365)."

**R8 — DC §4, **Close/Dispose** (`DC:1030-1035`).** Replace
```
5. destroy on the closing thread.

The admin mocks' `Close` does nothing (decision 21); their `Dispose` runs these steps. A
close from a callback is deferred to the drain thread, after its drain. A SafeHandle reached by
finalization does not destroy: it leaks and writes one `Trace` warning (Java and Python have no
client finalizer).
```
with
```
5. destroy on the closing thread;
6. dispose the serializers or deserializers the producer or consumer was given that are `IDisposable`,
   key then value, on the same thread (§4 **Serializers**).

The admin mocks' `Close` does nothing (decision 21); their `Dispose` runs these steps. The producer and
consumer mocks never dispose their serializers or deserializers, as Java's and Python's mocks. A
close from a callback is deferred to the drain thread, after its drain, and step 6 runs with the deferred
steps. A close that is refused and leaves the client open disposes nothing. A SafeHandle reached by
finalization does not destroy: it leaks and writes one `Trace` warning (Java and Python have no
client finalizer), and disposes nothing.
```

**R9 — FM §A2, the "Teardown order" bullet (`FM:511-518`).** After the sentence ending "so the destroy runs on the
closing thread and never inside `__execute_callbacks`." (`FM:513-514`) insert:
" The serializers the producer was given are then disposed on the same thread (`dotnet/CLAUDE.md` §4
**Close/Dispose**, step 6)."

**R10 — FM §B2, the path list's row 1 (`FM:1552`).** Replace
"→ release the `SafeHandle` → destroy | the closing thread |"
with
"→ release the `SafeHandle` → destroy → dispose the deserializers the consumer was given (`dotnet/CLAUDE.md` §4 **Close/Dispose**, step 6) | the closing thread |"

**R11 — FM §B2, the cases: a new bullet after "**A live `ConsumerHandle`** does not defer the destroy (Category 6)." (`FM:1600`):**
```
      - **A `Poll` still deserializing on another thread** is not waited for: `Poll` deserializes after its
        use ends (ledger 130), so a `Dispose` from another thread can dispose the deserializers while it runs,
        as Python's `close()` can (ledger 384). A gate would bring back ledger 130's deadlock for a deserializer
        that disposes its consumer.
```

### 6.2 Row 356's note (untracked ledger; applied in S0 with D27)

Replace, in row 356's "Ruling and notes":
"Consequence: the encoding keys are not listed as unused, as in Python; M21/P3's configuration-route serdes will read them from the user's dictionary later."
with
"Consequence: the encoding keys are not listed as unused, as in Python. (2026-11-06, ledger 365: there is no configuration route, so nothing will read them; the five keys stay neither checked nor sent.)"

## 7. Tests

DoD §3: every Java test of the translated behaviour, each named as translated or not, with its reason; texts
asserted, not just types. Python's tests are translated where they test behaviour .NET now has.

**Part 1 — the constants** (`tests/PublicConfigClassesTests.cs`, S3; Python's `pyt/test_config_classes.py`)
- `:61` `test_every_literal_constant_matches_java`: over the eight classes, Python's regex over each Java file under
  `kafka/` (the root the layout test already reads), each match equal to the .NET member's value by reflection, or
  absent when `java-deprecated.txt` lists it.
- `:77` `test_evaluated_constants`: Python's spot checks with .NET's types: the evaluated expressions,
  `DEFAULT_LOGIN_REFRESH_MIN_PERIOD_SECONDS` a `short`, `DEFAULT_KERBEROS_MIN_TIME_BEFORE_RELOGIN` a `long`, a
  `double`, a `bool`, `DEFAULT_GROUP_REMOTE_ASSIGNOR` `null`, the collections' types and contents, `SunX509` / `PKIX`;
  and, under D20 (a), which members are `const` (reflection: `IsLiteral`) and which properties.
- `:105` deprecated constants absent; `:113` and `:119` no exported `CommonClientConfigs` or `ShareConsumerConfig`.
- `:125` the three client classes have no public constructor (reflection); Python's `TypeError` text has no .NET
  form (CS0122 is compile-time).
- `:134` the five key-name classes construct.
- `:138` `test_modules`: covered by the layout rows (S1, S2).
- .NET's own: the typed defaults pass M22/P1's check on their keys (`ClientConfigs.Prepare`): the two `short`s on
  the two SHORT keys, an `int` (`DEFAULT_MAX_POLL_RECORDS` on `max.poll.records`), a `long`, a `double`, a `bool`.
- The xtask unit test of the C# output (S1).

**Part 1 — the Java ports** (S3): M22/P1's rows of `ProducerConfigTest`, `ConsumerConfigTest` and
`AdminClientConfigTest` (`tests/PublicClientConfigTests.cs:89-110` and the rest of that file's ports) and Python's
`pyt/test_config_class_ports.py:90`, `:98` write their keys with the constants, as Java and Python write them.

**Part 1 — the enums** (`tests/PublicConfigEnumsTests.cs`, S2): Python's `:148` `test_ssl_client_auth`, `:166`
`test_security_protocol`, `:188` `test_group_protocol`: members and order, values (`(short)SecurityProtocol.SSL == 1`),
`VALUES`, `ForConfig` (`null`, mixed case, unknown), `ForId` hit and miss, `ForName` / `Of` case-insensitive with
Java's exact text and `ArgumentNullException` for `null`, `Names()` in order; the keyword-only checks do not apply.
`ToString()` is `REQUIRED` (D24). Java has no tests of the three (F20).

**Part 2 — the producer** (`tests/PublicSerializerDisposalTests.cs`, S4; each test on `KafkaProducer` and
`AsyncKafkaProducer`)
- Java `KafkaProducerTest.testSerializerClose` (`:603-619`) and Python `:184`: two counting `IDisposable`
  serializers, not disposed while open, once each after `Close`, `Close(TimeSpan)`, `Dispose` and `DisposeAsync`.
- Python `:626`: `Close` twice disposes once each (its "not configured" half does not apply: no `Configure`).
- Python `:635`: a native construction failure (an unresolvable bootstrap host, as Python's) disposes both given
  serializers once; the config-route halves do not apply (ledger 365).
- Python `:717`: the async producer disposes them.
- Python `:738`, `:752`: a throwing `Dispose` (key in one, value in the other): both attempted, one
  `TraceWarning` with the text (a `TraceListener`), nothing thrown from `Close`.
- Python `pyt/test_producer_family.py:2823`: on `AsyncKafkaProducer`, a token that fires during `Close` leaves
  the disposal after the teardown, not before; a token cancelled before the call disposes nothing.
- Not translated: Java's `testConstructorFailureCloseResource` (`:560-577`; no serializer part, and .NET has no
  interceptors, F12); Python's `:674` (a failing `configure`), `pyt/test_keyword_callables.py:121` (config route).
- .NET's own: a `ConfigException` and an `ArgumentNullException` dispose nothing; one instance as key and value is
  disposed twice; key before value (a shared log); a `Close` from a delivery callback disposes on the drain thread
  after the deferred teardown (thread name asserted); an `IAsyncDisposable`-only serializer is not disposed; the two
  mocks never dispose, on `Close` and `Dispose`.

**Part 2 — the consumer** (`tests/PublicDeserializerDisposalTests.cs`, S5; each on `KafkaConsumer` and
`AsyncKafkaConsumer`)
- Java: none exists (F12).
- Python `pyt/test_kafka_consumer.py:2167` (given ones disposed at close, once), `:2177` (a native construction
  failure, Python's empty bootstrap, disposes the given ones), `:2297` / `:2311` (a throwing `Dispose`, traced,
  not raised), `:2327` (the async consumer). Not translated: `:2142`, `:2197` (config route, `configure`).
- .NET's own: a refused close (cross-thread `Close` during an op; `Close` inside a sync listener) disposes nothing,
  and a later `Dispose` disposes once; the async consumers' `Dispose` inside a callback disposes after the deferred
  teardown; a cross-thread `Dispose` during a `Poll` disposes after the teardown; the two mocks never dispose.
- Python's `pyt/test_serde_supply.py:246`, `:251`, `:260` → the internal helper's tests (`tests/Internal/`): it
  calls `Dispose`, traces and does not rethrow, and does nothing for a serde that is not `IDisposable`.
- D18's window is recorded, not tested: a test would need to time a `Dispose` into a running deserializer.

## 8. Gates

DC §7.5, per commit and at the end:
- **S1 (Rust lane):** the full pre-commit hook (`.githooks/pre-commit`: build, test, format, lint, `check-generated`)
  with `RUST_TEST_THREADS=6` if its integration stage runs here; `git diff --exit-code -- python/` after
  `cargo xtask generate-error-codes` (Python unchanged); and, because the step adds public C# types, `dotnet build`
  of the library on netstandard2.0, net8.0 and net10.0 and the unit tests on net8.0 and net10.0.
- **S0, S2–S7 (dotnet only):** `dotnet build` of the five targets (solution, gRPC server and its tests, soak and its
  tests, PerfV3, PackageSmoke) on the three TFMs; the unit tests on net8.0 and net10.0; `dotnet format
  --verify-no-changes`; then a `--no-verify` commit. S4 and S5 also run `make -C dotnet test-grpc-server-dotnet`,
  the soak tests and the integration tests with Docker.
- **Close:** `make verify-dotnet-macos-docker` once (`RUST_TEST_THREADS=6`); after a push, CI (net462 in the Windows
  block runs the constants and disposal tests on .NET Framework).
- **No perf A/B:** `Send` and `Poll` are unchanged; Part 2 runs at construction and teardown only.

## 9. Risks and open questions

- **R1 — the behaviour change (D19).** A user who shares an `IDisposable` serializer between clients, or keeps
  using it after closing the client, sees it disposed. Java and Python close it too; the XML docs and R6 say so.
- **R2 — the window (D18).** A `Dispose` from another thread can dispose a deserializer that a `Poll` is still
  running, and a producer's `Close` a serializer that a `Send` is still running; recorded, as Python has both.
- **R3 — the generator's C# literals.** 125 `_DOC` strings with quotes, backslashes and HTML go into C# literals;
  S1's xtask test and `check-generated` pin them, and S3's literal check reads them back against `kafka/`.
- **R4 — JDK defaults.** `DEFAULT_SSL_KEYMANGER_ALGORITHM` / `DEFAULT_SSL_TRUSTMANAGER_ALGORITHM` are Java's JDK
  defaults (`SunX509`, `PKIX`) hard-coded as Python's; a JVM with other defaults would differ, which no .NET user sees.
- **R5 — the cross-lane rows in S1.** Eight C# lines in a Rust-lane commit; `dotnet-critic` reviews them.
- **R6 — Java's typos are kept** (`DEFAULT_SSL_KEYMANGER_ALGORITHM`), as Java's names (ledger 285).
- **R7 — the future config object.** D5 leaves the three client classes without a public constructor; the phase that
  adds Part 3's setters decides the constructor's shape (Java's `ProducerConfig(Map)`) and how a client takes it.
- **R8 — an `IAsyncDisposable`-only serde** is not disposed (D17); documented.
- **Q1 — squash** at the end: Pranav's call.

## 10. Logistics

- N=109 (no `COMMENTS.109.md` / `COMMENTS.DONE.109.md` at the repo root or in `dotnet/`).
- Branch `prashah_dev_dotnet_config_constants` from `prashah_dev_dotnet_typed_config` at `3f7aa45f5` (or its later
  tip), created after the ruling; it lands after M22/P1, which it stacks on.
- S1: root `actor-executor` and `kafka-critic`, the full hook, comments in the repo-root `COMMENTS.109.md`. S2–S7:
  `dotnet-actor` and `dotnet-critic`, comments in `dotnet/COMMENTS.109.md`. A Critic pass after each Actor commit.

## 11. Proposed ledger rows

Added to `DOTNET-PORT-DECISIONS.md` (repo root, untracked) on 2026-11-06: row 365 Agreed (Part 3); rows 366–394
Pending, one per decision: 366 F1–F26; 367–385 D1–D19; 386–389 D20–D23; 390–392 D24–D26; 393 D27 (S0 and the
row 356 note); 394 D28 (logistics).

## 12. Rulings

Pranav, 2026-11-06, verbatim:
1. Part 3 (constructor arguments only), before this plan: ledger 365, Agreed.
2. After his questions on D5 (Java's public non-final classes with public constructors; Rust's `pub struct` with
   `pub fn new`; Python's public class whose `__init__` raises): "okay I agree with all the rulings." Ledger
   366–394, Agreed as picked: F1–F26; D1–D19; D20–D23 (a); D24–D26; D27 (R1–R11 and the row 356 note verbatim);
   D28. D5 stands as picked: a public, unsealed, non-static class whose only constructor is private.
3. After the phase (master had merged M22/P1 as `9e7d8962d`, #318): rebase or merge onto master, "squash all the
   changes to a single commit", and "Change author email to prashah@confluent.io".
4. Ledger 395: "Lets do (b) with 9-character table", then, after dotnet-critic 109-L395-1 (`ToUpperInvariant` maps `ſ`
   only under ICU): "yes make it 10-character … lets make .NET 10 character to align completely with java and python".
   The three enum lookups map the 10 letters whose Java and Python upper case is ASCII (ı, ß, ſ, ﬀ ﬁ ﬂ ﬃ ﬄ ﬅ ﬆ) before
   `ToUpperInvariant` (D8 amended), folded into the single commit.
5. Rebased onto master `96b786709` (#316, errors follow Java's exception hierarchy), with the lookups' and tests'
   exception types moved to #316's classes.
6. After the rebase: the producers refuse a null serializer with #316's `ConfigException` and Java's text (ledger 397,
   Pranav's request), and the consumers' null-deserializer message becomes Kafka 4.4's (ledger 398, "lets go with (a)"),
   as two follow-up commits on the branch.
