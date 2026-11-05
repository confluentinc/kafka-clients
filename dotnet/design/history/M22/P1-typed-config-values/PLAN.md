# M22/P1 — typed config values for the producer, consumer and admin clients (N=108)

**Status:** done (2026-11-05). S0–S8 committed on `prashah_dev_dotnet_typed_config` and squashed into one commit (the 17
originals, `7365b1b13` … `59af784cd`, are in the local tag `m22p1-before-squash`), not pushed; the record is the STATUS entry
(`dotnet/design/current/STATUS.md`, Milestone 22 / Phase 1) and the two `COMMENTS.DONE` files in
this folder. Every decision was ruled (§11); ledger 362–364 were added during the phase and Agreed (§11).
**Mode:** A (.NET only): the C ABI already takes the configuration, as text. If D8 is (a), one
tooling step (S1) is Rust (`rust/xtask`), done by the root `actor-executor` and reviewed by
`kafka-critic` (`dotnet/CLAUDE.md` §8.1). Mode B (a typed C ABI) is weighed in §5 and is not
proposed for this phase.
**Base:** master `95aa08565` (#314 merged). Every cite below was opened at that commit.
**N:** 108 proposed. N=107 belongs to M21/P3 (serializers from the configuration), which Pranav
abandoned on 2026-11-05; ledger rows 335–341 name it, so reusing 107 would mix the two phases'
records. No `COMMENTS.107/108` file exists in `dotnet/` or at the repo root.
**Sources:** `CONFIG-TYPED-VALUES-DOTNET-EVALUATION.md` (repo root, untracked; "the evaluation"),
re-checked here (§1.2), never taken as true. Ledger rows 271 (config constants), 272 (null value),
273 (`Double.toString`), 276 (null in `NewTopic.Configs`), 285 (Java's names). Rows 318–341
(M21/P3, abandoned) are context only: M22/P1 touches the same code at the serializer keys
(rows 325, 326; D11 here), and implements none of them.

Cite prefixes: `src/` = `dotnet/src/Confluent.Kafka/`, `tests/` = `dotnet/tests/Confluent.Kafka.UnitTests/`,
`grpc/` = `dotnet/grpc-server/`, `J/` = `kafka/clients/src/main/java/org/apache/kafka/`, `JT/` =
`kafka/clients/src/test/java/org/apache/kafka/`, `py/` = `python/confluent_kafka/`, `pyt/` =
`python/test/unit/`, `ffi/` = `rust/src/ffi/`, `core/` = `rust/src/`, `X/` = `rust/xtask/src/`,
`DC` = `dotnet/CLAUDE.md`, `FM` = `dotnet/.claude/rules/ffi-marshalling.md`, `PC` = root
`CLAUDE.md`, "Python Binding Conventions". BH (`rust/target/include/confluent_kafka_bindings.h`) is
an untracked build artifact dated 2026-10-30, older than the base, so ABI facts cite `ffi/`.

---

## 0. In one paragraph

Java's clients take `Map<String, Object>` / `Properties`, and Python's take one
`configs: dict[str, Any]`. .NET's six real clients take `IReadOnlyDictionary<string, string>`.
Python reaches Java's behaviour over the same string-only C ABI by checking each value's type
against the key's Java `ConfigDef` type and converting it to Java's text before the call
(`py/_config.py:182-219`), a recorded deviation that "goes when the FFI takes typed values"
(PC, Implementation over the FFI, exception 2). This plan does the same in .NET: one
`IReadOnlyDictionary<string, object?> configs` on the six constructors, a generated C# copy of
Python's key-type table, one internal helper doing `prepare()`'s work, and one marshaller in place
of the three copied loops. The parameter change breaks every caller at compile time (168 sites,
all in this repo, all mechanical; no package has been published by automation, §10 Q1).

## 1. Scope

### 1.1 In and out

**In:**

- The six constructors: `KafkaProducer` (`src/Producer/KafkaProducer.cs:49-52`),
  `AsyncKafkaProducer` (`AsyncKafkaProducer.cs:51-54`), `KafkaConsumer` (`src/Consumer/KafkaConsumer.cs:56-59`),
  `AsyncKafkaConsumer` (`AsyncKafkaConsumer.cs:58-61`), `KafkaAdminClient` (`src/Admin/KafkaAdminClient.cs:45`),
  `AsyncKafkaAdminClient` (`AsyncKafkaAdminClient.cs:48`), with their XML docs. Each has exactly one
  constructor; no static factory takes a configuration (`IAdmin` has no `Create`).
- The three copied checks and marshal loops: `src/Internal/NativeProducer.cs:93-124` and `:507-553`,
  `NativeConsumer.cs:154-183` and `:207-253`, `NativeAdminClient.cs:75-94` and `:118-165`.
- The two managed reads: `max.block.ms` (`NativeProducer.cs:555-559`, used for the in-callback
  waits, FM §A7) and "group.id defined" (`NativeConsumer.cs:182`, used for ledger 105's check).
- Ledger 272's null rule (Agreed 2026-10-28, follow-up open): this phase closes it.
- Java's `Double.toString` / `Float.toString` / `BigDecimal.toString` text, only in the binding's own error messages (D16).
- `ConfigException` (D6), if ruled.
- Every caller in the repo (§8), and the rule text that names the string form (S0).

**Out, each with its reason:**

- M21/P3 (serializers and deserializers from the configuration): abandoned by Pranav on
  2026-11-05. D11 decides only what the binding does with the serializer keys when instances are
  given, which is the only case .NET has.
- `NewTopic.Configs`, `MockAdminClient` topic configs and `DefaultGroupConfigs`, `MetricName.Tags`:
  Java types them `Map<String, String>`, so they stay `<string, string>`. Ledger 276 already ruled
  the null in `NewTopic.Configs`.
- Config constants (`ProducerConfig.LINGER_MS_CONFIG`, …; ledger 271): a decision (D10), not
  assumed. The pick keeps them for M22/P2.
- Row 273's audit of the `ToString`s that print a `double` (`ClientQuotaAlteration.cs:146` and
  others): a decision (D16). The pick leaves it as its own follow-up.
- The mocks: none takes a configuration (`MockProducer.cs:57`, `MockConsumer.cs:62`,
  `MockAdminClient.cs:61`, `:77`, and their async peers).
- The gRPC harness protos: they carry `map<string, string>` (`producer_service.proto:107`,
  `consumer_service.proto:169`, `admin_service.proto:351`, `chaos_service.proto:90`, `:110`). A string
  is valid for every key type, so the servicers keep working; typed values over the harness would
  change every language's backend.
- A typed C ABI (Mode B, §5).

### 1.2 Re-check of the evaluation at `95aa08565`

Read by four read-only research passes plus my own spot checks and two scratch compile probes
(probe 4 and probe 5, net10.0, outside the repo). "Confirmed" means the claim and its cite hold;
every correction or addition carries its cite.

| # | The evaluation's claim (its §) | Verdict | Cite, correction or addition |
|---|---|---|---|
| F1 | The C ABI takes config as strings only (§2, §5) | Confirmed; lines corrected | Producer `ffi/producer/producer_config.rs:145-171` (fn :147, `map_strings` :157, `ProducerConfig::new` :158, doc "a live map of C strings" :142). Consumer `ffi/consumer/consumer_config.rs:132-151` (fn :134); its doc differs: "`props` must be null or a valid map of `char *` keys and values" (:128-130). Admin `ffi/admin/admin_client_config.rs:111-129` (fn :113). `map_strings` `ffi/util.rs:671`. **Added:** no typed write exists anywhere: the config handles export only `new`, `destroy`, `config_names`, the typed getters `get_short/int/long/double/boolean/string/list`, `unused`, `ignore`, `log_unused` (and the producer's `set_partitioner`). |
| F2 | A NULL key or value reads as `""` (§2) | Confirmed; line corrected | `c_str_to_string` is `ffi/util.rs:97-111`. **Added:** `kafka_Map_put` stores raw pointers and compares keys by pointer identity (`ffi/util.rs:730-733`), so two equal keys from different buffers become two entries and the later wins. A `Dictionary` cannot produce that. |
| F3 | The .NET checks and the three copied loops (§2) | Confirmed; lines corrected | Consumer loop `NativeConsumer.cs:207-253` (not `:251`), admin loop `NativeAdminClient.cs:118-165` (not `:163`), admin checks `:75-94`. The loops differ only in the config/client functions and the admin's `FreeError` call form. Externs: `kafka_Map_new_err` / `_put_err` / `_destroy_err` (`src/Internal/Interop/NativeMethods.cs:820-824`, `:123-124`), `kafka_producer_ProducerConfig_new` (:826-827), `kafka_consumer_ConsumerConfig_new` (:272-273), `kafka_admin_AdminClientConfig_new` (`NativeMethods.AdminClient.cs:25-26`). |
| F4 | The null-value message and `ParamName` (§2) | Confirmed, with doc wording added | Both checks use `ParamName` "config". XML docs: "A config value is null." (`KafkaProducer.cs:47`, `AsyncKafkaProducer.cs:49`); "A configuration value is null." (`KafkaAdminClient.cs:43`, `AsyncKafkaAdminClient.cs:46`); the consumers say "values are strings." (`KafkaConsumer.cs:41`, `AsyncKafkaConsumer.cs:43`). |
| F5 | Only two managed reads of a value (§2) | Confirmed | `MaxBlockMsOf` (`NativeProducer.cs:555-559`; key :42, default 60000 :44, stored `_maxBlockMs` :71/:78) and `ContainsKey("group.id")` (`NativeConsumer.cs:182`, key :39, stored `_groupIdDefined` :68/:129). The dictionary is not kept after construction. |
| F6 | Python's `prepare()` (§2, §4) | Confirmed; details added | `_SERDE_KEYS` `py/_config.py:77-84`, `check_type` :96-136, `check_null` :139-153, `_core_str` :156, `convert_to_string` :168-179, `prepare` :182-219. **Added:** BOOLEAN takes only `bool` (an `int` 1 is refused); `bytes` is refused everywhere; an `int` on a DOUBLE key is sent as `3`, where Java's `convertToString` of the parsed value is `3.0`; a list on a key not in the table is sent as `[a, b]` (`java_str`), not joined; a **given** serde key is skipped entirely (no null check, no type check, not sent), while an own-role serde key that is not given is type-checked and then dropped (:208-218); `ConfigError` renders the value with Python's spellings (`True`, `None`, `py/common/config/config_error.py:60`). |
| F7 | The generator `X/config_types.rs` (§2) | Confirmed; detail added | Output `:38`; run by `cargo xtask generate-error-codes` (`X/main.rs:44`, `:560`); checked by `check-generated` through `config_types::check_up_to_date` (`X/main.rs:606`, `config_types.rs:668`). Counts confirmed: 335 keys (PRODUCER 120, CONSUMER 120, ADMIN 95), 122 null rules (46, 44, 32). **Added:** `render` is ~65 lines (`:568-633`) and `all_outputs` (`:653-657`) feeds both `generate` and `check_up_to_date`, so a second renderer is one function plus one list entry. The `None` spelling is applied while the custom validators' messages are **evaluated** (`:552-553`), not when rendered, so a C# table needs a second evaluation with Java's `null`. |
| F8 | The string type dates from M11/P1 with no recorded reason (§2) | Confirmed; provenance found | `M11/P1-producer-foundation/PLAN.md:18`, `:30`, `:32`. The DC §4 "Config" row (`DC:672`) and FM §0.1's `ProducerProperties_put` bullet (`FM:101-102`) come from `592b3f5db` (2026-08-12, "dotnet: scaffold the .NET binding — folder layout & project setup", Co-Authored-By Claude, branch `prashah_dev_dotnet_binding_producer`), with no rationale. That row says "coerce non-string values to `str`", so it assumed typed values; the `<string, string>` parameter contradicted it from the first constructor. `ProducerProperties_*` exists nowhere in `rust/src`. |
| F9 | The core's `ConfigDef` lacks some Java keys (§2) | Confirmed; made exact | The core has **no CLASS type** (`core/common/config/config_def.rs:60-99`), so every Java CLASS key is missing (9 producer, 8 consumer, 6 admin), and so are `ssl.keystore.password`, `ssl.key.password`, `ssl.truststore.password`, `security.providers`, `config.providers`, `enable.metrics.push`, `metric.reporters`, `interceptor.classes`, and the consumer's `partition.assignment.strategy` (`core/producer/producer_config.rs:3340-3354`, `core/consumer/consumer_config.rs:3078-3091`, `core/admin/admin_client_config.rs:1309-1319`; these tests assert samples, not complete lists). |
| F10 | .NET has no `ConfigException`; a core config error is `KafkaException` −10 "(the error table, `KafkaException.cs:173-180`)" (§2) | Confirmed; corrected | −10 has **no case** in that switch; it takes the default arm (`src/Common/KafkaException.cs:179`). −10 is `kafka_common_Error_CONFIG` (`ffi/common.rs:2125`). The namespace `Confluent.Kafka.Common.Config` **does** exist (`ConfigResource`, `ConfigResourceType`). Tests asserting −10: `tests/PublicAdminTeardownTests.cs:163`, `tests/Interop/ProducerConfigMarshalTests.cs:33` (used :46, :62), `tests/PublicAdminClientCreateTests.cs:38` (:110), `tests/KafkaExceptionCauseTests.cs:34` (:58). |
| F11 | "Behind `AbstractConfig::new(…, ConfigMap, …)`" (§0, §5) | Corrected | `AbstractConfig::new` is `pub(crate)` and test-only (`core/common/config/abstract_config.rs:92`). The clients go through `with_hooks` (:125) / `with_hooks_and_log_warnings` (:147) and a private `build(originals: ConfigMap, …)` (producer `:1174`, consumer `:1050`, admin `:471`). `ParsedValue` (`config_def.rs:152`) and `parse_type` (:737-820) are as stated; the core's `Double.toString` is `java_double_to_string` (`config_def.rs:916`). |
| F12 | The core's text parsing (§5) | Confirmed | Trim `config_def.rs:828`; booleans ignoring case (:948); `parse_java_double` (:888-910) takes `1E-05`, `NaN`, `Infinity` and refuses `nan`, `inf`, hex; lists split as Java's `\s*,\s*` with limit −1 (:848-861), keeping empty elements. |
| F13 | Tests that pin today's null behaviour (§6) | Confirmed; lines corrected | `tests/PublicAdminTeardownTests.cs:242-255` (value :246-248, map :254), `tests/Interop/ProducerConfigMarshalTests.cs:113-118` (map) and `:121-133` (value, message :132), `tests/Interop/ConsumerConfigMarshalTests.cs:51-54` and `:57-66` (type only). |
| F14 | 168 client constructions outside `src/` (§6) | Confirmed; one class missed | UnitTests 133 sites / 55 files, IntegrationTests 10 / 1, GrpcServer.UnitTests 4 / 2, grpc-server 10 / 8, Performance 6 / 3 (PerfV3 only; PerfV2 is ckd's client), soak 3 / 1, PackageSmoke 2 / 1. **Missed:** 38 direct `NativeProducer/NativeConsumer/NativeAdminClient.Create` calls in 11 unit-test files, which take the configuration too. §8 has the detail. |
| F15 | Version `4.4.0-alpha.1`; publication not checked (§6, §14) | Confirmed; resolved as far as the repo can say | Nothing pushes a package: `.semaphore/release-nupkg.sh:18-20` "Nothing is published: the release is uploaded to nuget.org by hand, from the tag build's artifacts", and the release blocks run only on a `dotnet/v…` tag (`.semaphore/semaphore.yml:1378`). No `dotnet/*` tag exists locally. A hand upload cannot be ruled out offline (Q1). |
| F16 | "Every number must be formatted with `CultureInfo.InvariantCulture`: under `de-DE`, `0.5` prints `0,5`" (§9) | Confirmed; widened to integers | Under `sv-SE` a negative integer prints with U+2212 (`−1`), through `ToString`, `Convert.ToString` and interpolation alike (probe 5), and the core's parsers expect ASCII `-`. Every number, integral or not, goes through the invariant culture. |
| F17 | "`value.GetType().FullName` (`System.Int64`)" for the message (§9) | Corrected | `FullName` of a generic type is assembly-qualified (``System.Collections.Generic.List`1[[System.Int32, System.Private.CoreLib, Version=10.0.0.0, …]]``); `Type.ToString()` gives ``System.Collections.Generic.List`1[System.Int32]`` and equals `FullName` for a non-generic type (probe 5). |
| F18 | "netstandard2.0 / net462 … expected identical, not run" (§14) | Corrected (from the .NET documentation; not run here) | On .NET Framework `double.ToString()` prints 15 significant digits and the `"R"` format is documented not to round-trip in some cases; .NET Core 3.0 made both shortest round-trip. CI's Windows block runs the unit tests on net462, net8.0 and net10.0 (`.semaphore/semaphore.yml:451-482`, `tests/Confluent.Kafka.UnitTests.csproj:17`), so a conversion that leans on the runtime's formatting fails there, not on this Mac. DC §7.5 does not mention that block (stale, outside this phase). |
| F19 | The parameter-type options (§7) | Confirmed; two more ruled out | Probe 4: `IEnumerable<KeyValuePair<string, object?>>` refuses a `Dictionary<string, string>` too (CS1503: `KeyValuePair` is a struct, so there is no variance); a target-typed `new() { … }` never works for an interface parameter (CS0144); an `IDictionary<string, object?>` variable is CS1503 against `IReadOnlyDictionary<string, object?>`, as today. |
| F20 | Java's integer rules (§3, §8) | Confirmed; one consequence made explicit | SHORT refuses an `Integer` (`J/common/config/ConfigDef.java:744-750`) and LONG refuses a `Short` (:752-760). So under Java's rule a C# `int` literal for a SHORT key is refused (D2). |
| F21 | Java's non-string key (§1) | Confirmed; path difference added | The `Map` path: `Utils.castToStringObjectMap` throws `ConfigException(String.valueOf(key), value, "Key must be a string.")` (`J/common/utils/Utils.java:1521`), so a `null` key prints `null`. The `Properties` path: `propsToMap` throws `ConfigException("One or more keys is not a string.")` (`Utils.java:1492`), the form `KafkaProducerTest.testConstructorWithNotStringKey` asserts (`JT/clients/producer/KafkaProducerTest.java:580`). |
| F22 | — (not in the evaluation) | Added | Java ignores a serializer key when an instance is given: `ProducerConfig.appendSerializerToConfig` overwrites it with the instance's class (`J/clients/producer/ProducerConfig.java:713-727`), and the constructor calls `config.ignore(KEY_SERIALIZER_CLASS_CONFIG)` and never configures the instance (`J/clients/producer/KafkaProducer.java:398-410`; consumer `J/clients/consumer/internals/Deserializers.java:49-63`). So the user's value is never type-checked, and the encoding keys, which only a configured `StringSerializer` reads, are logged as unused. .NET always gives instances. |
| F23 | — | Added | `group.id = ""` fails the consumer's construction with `InvalidGroupIdError` "The configured group.id should not be an empty string or whitespace." (`core/consumer/internals/async_kafka_consumer.rs:3851-3857`). So "defined" changes only for a `null` value. |
| F24 | — | Added | Java's `null` and Python's "not set" differ for a key with a non-null default and no validator: `ssl.endpoint.identification.algorithm` (STRING, default `"https"`, no validator, `J/common/config/SslConfigs.java:115`, `:145`). Java's `null` turns hostname verification off; Python's and this plan's `null` keeps `https`. This is the deviation ledger 272 accepted, now with a named case (D5). |

## 2. Parity anchor

What Python does, what Java does, and what .NET does today, item by item. Every decision in §3 is
measured against this table.

| # | Item | Python | Java | .NET today |
|---|---|---|---|---|
| A1 | Parameter | one `configs: dict[str, Any]` (PC Configuration, `CLAUDE.md:436`; `rfc/apis/python.md:194`) | `Map<String, Object> configs` and `Properties` (`KafkaProducer.java:302`, `:319-321`, `:331`, `:346-347`; `KafkaConsumer.java:559`, `:572`, `:590-593`, `:609-612`; `Admin.java:135-136`, `:145-146`) | `IReadOnlyDictionary<string, string> config` (F3) |
| A2 | Values a key accepts | `check_type` (`py/_config.py:96-136`): BOOLEAN `bool`; STRING/PASSWORD `str`; INT/SHORT/LONG any `int`; DOUBLE `int`/`float`; LIST `list`/`tuple`; CLASS a class; `str` for every type | `parseType` (`ConfigDef.java:703-791`): BOOLEAN `Boolean`; STRING `String`; PASSWORD `Password`/`String`; INT `Integer`; SHORT `Short`; LONG `Integer`/`Long`; DOUBLE any `Number`, `BigInteger` and `BigDecimal` included; LIST any `List`; CLASS `Class`; `String` for every type | `string` only |
| A3 | Wrong type | `ConfigError` (−10), "Invalid value v for configuration k: Expected …" (`config_error.py:60`) | `ConfigException`, same text (`ConfigException.java:39-40`) | impossible |
| A4 | Text sent to the core | `convert_to_string` (`py/_config.py:168-179`), `java_str` and `_double_to_string` (`py/_java.py:59`, `:87`) | none (parsed in process; `convertToString` `ConfigDef.java:806`) | the given string |
| A5 | `null` value | the key's null rule (`check_null`, `py/_config.py:139-153`), else left out (:208-218) | the validator runs on `null` (`ConfigDef.java:552-554`), else the key holds `null` | `ArgumentException` for every `null` (F4) |
| A6 | `null` map | `TypeError("configs must not be None")` (`py/_config.py:199`) | NPE (`Utils.java:1511`; `ProducerConfig.java:713`) | `ArgumentNullException` (`NativeProducer.cs:97-100`) |
| A7 | Non-string or `null` key | `ConfigError`, "Key must be a string.", every key before any value (`py/_config.py:202-204`) | `ConfigException`, same text (F21) | impossible, except a `null` key from a custom `IReadOnlyDictionary` (unchecked) |
| A8 | Serializer keys with an instance given | skipped; own-role keys dropped (`py/_config.py:77-84`, `:208-218`) | the class keys ignored; encoding keys unused (F22) | all sent; the core lists them as unused |
| A9 | Unknown keys | sent; the core logs one INFO line (PC, `CLAUDE.md:438`) | kept; logged as unused (`AbstractConfig.java:392-397`) | sent (same as both) |
| A10 | Value checks (ranges, valid strings, number text) | the core's | `ConfigDef`'s | the core's (same as Python) |
| A11 | Type of a core config error | `ConfigError` (−10, `py/common/config/config_error.py:42`) | `ConfigException` | `KafkaException`, `Code` −10 (F10) |
| A12 | "group.id defined" | `configs.get("group.id") is not None` (`py/consumer/_base.py:636`) | `Optional.ofNullable(group.id)`; `throwIfGroupIdNotDefined` (`AsyncKafkaConsumer.java:1293-1298`) | `ContainsKey` (F5) |
| A13 | `max.block.ms` for the binding's own waits | read from the converted text (`py/producer/_base.py:494-496`) | not applicable (its own waits) | parsed from the string (F5) |
| A14 | Number text | locale-free (`str`, `repr`) | locale-free (`toString`) | the caller's string |
| A15 | Config constants | generated (`py/producer/producer_config.py`, ledger 271) | `public static final` | none |

**Ceiling.** The public surface after this phase: the six constructors' one parameter, typed
`IReadOnlyDictionary<string, object?>`, and at most one new public type, `ConfigException` (D6),
which both Java and Python have. Nothing else public.

**NOT adding:** a `Properties` form or a second dictionary overload (D1); config classes,
`ConfigDef` or typed getters; config constants (unless D10 folds them in); range or valid-string
checks in .NET (A10 stays the core's); a `Password` type; serde `Configure`, or serializers from
the configuration (M21/P3); a typed C ABI (§5); an `IConfiguration` binder or ckd-style config
builders (DC §3 "Do NOT build"); a migration helper such as `ToConfig()`; a .NET-side unused-key
warning; typed values through the gRPC harness protos.

## 3. Decisions

Grouped for a fast ruling: one approval for the facts, one table for the picks that are Python's
way, then the items that need one ruling each, then the picks that keep .NET's way.

### 3.1 Group 1 — the fact refresh (one approval; ledger 342) — ruled as picked (2026-11-05)

Approve §1.2 F1–F24 as the facts this plan stands on. None of them changes the evaluation's
recommendation; F16, F17, F18, F22 and F24 change details of D5, D7, D11, D15 and D16 below.

### 3.2 Group 2 — Python's way (one table; ledger 343–351) — every row ruled as picked (2026-11-05)

| D | Python | Java | .NET today | Change (pick) | Breaking |
|---|---|---|---|---|---|
| D1 parameter type (**Ruled as picked**) | one `dict[str, Any]` (A1) | `Map<String, Object>`, plus `Properties` | `IReadOnlyDictionary<string, string>` | replace it with one `IReadOnlyDictionary<string, object?>` on the six constructors; no overload | yes: 168 sites + 38 internal calls, all mechanical (§8) |
| D12 parameter name (**Ruled as picked**) | `configs` | `configs` (`Map`), `properties` / `props` | `config` | `configs` | named arguments only |
| D3 LIST values (**Ruled as picked**) | `list`, `tuple`, or `str` (`py/_config.py:125`) | any `List`, or a `String` (`ConfigDef.java:768-776`) | n/a | an `IList` (an array, `List<T>`, `ReadOnlyCollection<T>`, `ImmutableList<T>`) or a `string`; a set or a lazy `IEnumerable` is refused with "Expected a comma separated list.", as Java refuses a `Set`; elements unchecked, each converted by D15, a `null` element sent as `null` (Python's `_core_str`), joined with `,` | no |
| D4 CLASS values (**Ruled as picked**) | a class, sent as `module.qualname`, or `str` (`py/_config.py:129`, `:177-178`) | a `Class` or a `String` (`ConfigDef.java:777-782`) | n/a | a `System.Type`, sent as its `FullName`, or a `string`; anything else "Expected a Class instance or class name." The core has no CLASS key (F9), so the text is never loaded or parsed | no |
| D5 `null` value (**Ruled as picked**) | the null rule, else not set (A5) | the validator, else `null` | `ArgumentException` | ledger 272 as agreed: a key whose validator refuses `null` throws `ConfigException` with Java's message (122 rules, Java's `null` spelling, D7); any other `null` leaves the key out; never a `NULL` pointer (F2). F24's case is the recorded deviation | the three tests of F13 |
| D6 error type (**Ruled as picked**) | `ConfigError`, −10 | `ConfigException` | `KafkaException` −10, the default arm (F10) | `Confluent.Kafka.Common.Config.ConfigException : KafkaException`, Java's three constructors `(message)`, `(name, value)`, `(name, value, message)`, `Code` −10, not retriable; the error table maps the core's −10 to it too, so one `catch` covers the binding's and the core's config errors (the third typed subclass under decision 14, which allows them) | an exact-type `Assert.Throws<KafkaException>` on −10 (F10's four files); `catch (KafkaException)` unchanged |
| D13 keys (**Ruled as picked**) | "Key must be a string.", all keys before any value (A7) | same (F21, `Map` path) | n/a | a `null` key (only a custom `IReadOnlyDictionary` can yield one) throws `ConfigException` "Invalid value {v} for configuration null: Key must be a string."; every key is checked before any value | no |
| D14 the two managed reads (**Ruled as picked**) | `group.id` defined = not `None`; `max.block.ms` from the converted text (A12, A13) | `Optional.ofNullable` | `ContainsKey`; the string parsed | "defined" = present and not `null`; `max.block.ms` read from D15's text, so an `int` or a `long` works | no |
| D15 the text sent (**Ruled as picked**) | `convert_to_string` / `_core_str` / `java_str` (A4) | `convertToString(parseType(v))` for a defined key; `String.valueOf` for the rest | the given string | the exact text the core parses to the value Java's `parseType` gives, not Java's `convertToString` text (revised 2026-11-05 with D2's conversion): a `string` unchanged (the core trims, as Java's `parseType`); `bool` as `true` / `false`; a number by D2's table (integers and `decimal` as their invariant digits, a `double` as invariant "G17", a `float` or `Half` widened to `double` first, `NaN` / `Infinity` / `-Infinity` / `-0` as literals); a list's elements by these same rules, joined with `,`; a `System.Type` as its `FullName`. A key not in the table, which the core never parses: the same rules for scalars, a collection as `[a, b]` and a dictionary as `{k=v}` (Python's `java_str`), any other object by its `ToString()` in the invariant culture. The value the core holds and every output it shows (its messages, the configuration log) are Java's, because the core prints parsed values (D2). Same as Python, except a `float` on a DOUBLE key, widened first as Java's `Float.doubleValue()` (0.1f holds `0.10000000149011612`), which Python cannot express | no |

D1's alternatives, for the record (measured, evaluation §13 and probe 4): (b) keep the string
form and add `<string, object?>` overloads: `null` and a target-typed argument are ambiguous
(CS0121), six more constructors, and a second form Python merged away (A1); (c) the non-generic
`System.Collections.IDictionary`: untyped keys, so "Key must be a string." becomes reachable, and a
variable typed `IReadOnlyDictionary<string, string>` still fails (CS1503); (d)
`IEnumerable<KeyValuePair<string, object?>>`: fails like (a) (F19). A caller who writes
`Dictionary<string, object>` (non-nullable values) compiles with warning CS8620, which is an error
under `TreatWarningsAsErrors`; the docs show `Dictionary<string, object?>`.

### 3.3 Group 3 — one ruling each

**D2 — integer and numeric widths (ledger 352). Ruled as picked** (2026-11-05, the amended pick below). Java accepts exact classes; Python has one `int`
and accepts it for INT, SHORT and LONG, leaving the range to the core
(`pyt/test_config.py:103`, `2**63-1` for INT). C# has Java's distinctions, so .NET can follow
either.
- (a) Java's classes, mapped to C#'s: BOOLEAN `bool`; INT `int`; SHORT `short`; LONG `int` or
  `long`; DOUBLE any C# number: `sbyte`, `byte`, `short`, `ushort`, `int`, `uint`, `long`, `ulong`,
  `float`, `double`, `decimal`, `System.Numerics.BigInteger`, and on the net8.0 and net10.0 assets
  `Half`, `Int128` and `UInt128`, sent as a text the core parses to the `double` Java's
  `Number.doubleValue` gives (below).
  Java's DOUBLE arm takes any `Number` (`J/common/config/ConfigDef.java:761-767`), `BigInteger` and
  `BigDecimal` included; `BigInteger` and `decimal` take their roles. `char`, `TimeSpan` and enums
  are refused everywhere (Java and Python refuse their equivalents).
- (b) Python's: any integral C# type for INT, SHORT and LONG; the core range-checks the text
  ("Not a number of type INT").
- (c) any integral type whose value fits the key's width.

**The rule for a C# number type Java has no class for** (`byte`, `ushort`, `uint`, `ulong`, `Half`,
`Int128`, `UInt128`): refused on INT, SHORT and LONG, which take exact classes, and accepted on
DOUBLE, which takes any number. `BigInteger` and `decimal` get the same answer from Java's own
classes: Java's INT, SHORT and LONG arms refuse a `BigInteger` or a `BigDecimal` (only `Integer`,
`Short`, `Integer`/`Long` pass, `ConfigDef.java:736-760`), and DOUBLE takes both.

**One exception: `nint` and `nuint`** (boxed, `System.IntPtr` and `System.UIntPtr`). (i) Numbers by
the rule, accepted on DOUBLE; (ii) refused everywhere. Pick **(ii)**: a boxed `nint` cannot be told
from an `IntPtr` handle, on netstandard2.0 and .NET Framework `IntPtr` is not a numeric type at all
(so the asset would decide the answer), and a pointer-sized value in a configuration is almost
certainly a mistake. The message is "Expected value to be a double, but it was a System.IntPtr".

**Where the types exist.** `Half` needs `#if NET5_0_OR_GREATER`, `Int128` and `UInt128`
`#if NET7_0_OR_GREATER`. Probe 6 (netstandard2.0, net8.0, net10.0): both symbols are defined on the
net8.0 and net10.0 builds and not on netstandard2.0, where an unguarded `System.Half` or
`System.Int128` is CS0234; `System.Numerics.BigInteger` and `System.IntPtr` compile on all three
with no package reference. So the netstandard2.0 asset cannot name the three and refuses them as
any unknown type ("…but it was a System.Half"). Only an app on .NET 5, 6 or 7 (all out of support)
gets that asset with those types; .NET Framework has none of them, and a net8.0+ app gets the
net8.0 or net10.0 asset.

**The conversion: the exact digits; the core rounds** (Pranav, 2026-11-05: "have the PM switch
D2's conversion to sending the exact digits"; Python's way, which sends `str(int)` for any `int`).
Java's `doubleValue()` rounds to the nearest `double`, ties to even, and gives ±Infinity past the
range (Javadoc; no JDK run here, §10 R10). The binding does not round. It sends a text the core's
`parse_java_double` (`core/common/config/config_def.rs:883-912`) reads, and that function ends in
Rust's `str::parse::<f64>` (`:906`), which rounds correctly, half to even, and returns infinity
past the range (probe 7: `"9007199254740995"` → `9007199254740996`; 401 digits → `inf`).

| C# value on a DOUBLE key | Text sent |
|---|---|
| `sbyte` … `ulong`, `BigInteger`, `Int128`, `UInt128` | `ToString(CultureInfo.InvariantCulture)`: digits, a leading `-` |
| `decimal` | `ToString(CultureInfo.InvariantCulture)`: exact digits (`0.1`, `-12.50`); never an exponent |
| `double` | `"G17"` in the invariant culture, on every runtime; `NaN`, `Infinity`, `-Infinity` and a negative zero (`-0`) written as literals |
| `float`, `Half` | widened to `double` (exact: every binary32 and binary16 value is a binary64 value, Java's `Float.doubleValue()`), then as a `double` |

- **Why "G17" for `double`, and one format for all runtimes.** "R" is shortest round-trip only since
  .NET Core 3.0; on .NET Framework it is documented to fail to round-trip in some cases, and
  "G17" is the documented workaround (F18). Seventeen significant digits identify every `double`, and
  the core parses them correctly, so the text is longer than Java's (`0.1` goes as
  `0.10000000000000001`) and the value is the same. The literals for the special values and `-0`
  keep the text independent of the runtime's culture symbols and of .NET Framework's printing of a
  negative zero, which is not verified here.
- **Every text is one `parse_java_double` accepts**: an optional sign, then `NaN`, `Infinity`, or a
  first character that is a digit or `.` followed only by digits, `.`, `e`, `E`, `+`, `-` (`:899-905`).
  Invariant integers, invariant decimals and invariant "G17" (`1E+21`, `1.0000000000000001E-05`)
  stay inside that set; a culture's U+2212 minus would not (F16).
- **Probe 7** (a netstandard2.0 / net8.0 / net10.0 library with this rule; .NET 8.0.30 and
  10.0.11, arm64; the netstandard2.0 build run on .NET 10): 71,323 texts per build (35,040 on the
  netstandard2.0 build, which cannot name `Half`, `Int128`, `UInt128`) parsed by a verbatim copy of
  `parse_java_double` at `95aa08565`: 0 rejected, 0 wrong. Every `double`, `float` and `Half` came
  back bit for bit (20,017 doubles from random bit patterns plus `-0`, `NaN`, the infinities,
  `double.Epsilon`, `MaxValue`, `1e23`); every integer and `decimal` came back as the correctly
  rounded `double`, the same value .NET Core's IEEE-correct `double.Parse` gives for the same digits
  (5,006 `BigInteger`s including 2^53+3, −(2^53+1), 2^64+2049, ±10^400; 5,005 `decimal`s; the
  `ulong` 2^63+1025 that a convert-as-signed sequence rounds wrongly).
- **Why the casts are not used** (probe 6, kept for the record): `(double)BigInteger` truncates
  (30,073 of 40,009 wrong; 2^53+3 → `…994`), `(double)decimal` is off by one unit in the last place in
  13,292 of 50,008, and `(double)Int128` / `UInt128` in 919 of 58,390 on .NET 8 (0 on .NET 10). With
  the digits sent, these casts never decide a value, on any runtime.
- **What the user sees is the parsed value, not the text.** The core's validators print the parsed
  value (`Range`, `config_def.rs:1078-1095`, through `config_error(name, Some(value), …)`), and its
  `Display` prints a `Double` with Java's `Double.toString` (`:247-262`); the INFO dump of the
  configuration prints the parsed `values` (`core/common/config/abstract_config.rs:340-352`); the
  unused-keys line prints names only (`:316-329`); the C getters (`kafka_producer_ProducerConfig_get_double`
  and the others) return parsed values. The raw text survives in two places only: `originals()`
  (`core/producer/producer_config.rs:1588-1594`), which reaches a custom partitioner's `configure`
  (`core/producer/kafka_producer.rs:1142-1144`), set by `kafka_producer_ProducerConfig_set_partitioner`,
  which .NET does not call; and the consumer's group-protocol check, which only asks whether the
  original text is blank (`core/consumer/consumer_config.rs:1204-1206`). A text the core cannot parse
  would be echoed in "Not a number of type DOUBLE", but every text above parses. So the value, the
  core's messages and the configuration log match Java's for every number type.
- **Not picked:** rounding in .NET with exact integer arithmetic and sending Java's
  `Double.toString` of the result (this plan's earlier pick): the same values, with rounding code in
  the binding.

Pick **(a)**, with the rule, `nint`/`nuint` (ii) and the exact digits above: Java decides content
(PC rule 1), and Python's rule is only its type system's limit. Consequences:
- `["batch.size"] = 16384L` is refused, as in Java.
- `["sasl.login.refresh.min.period.seconds"] = 60` (an `int` literal on a SHORT key) is refused with
  "Expected value to be a 16-bit integer (short), but it was a System.Int32", as Java refuses an
  `Integer` there (F20), so the caller writes `(short)60` or `"60"`. Two keys are SHORT, in all three
  clients (`sasl.login.refresh.buffer.seconds`, `sasl.login.refresh.min.period.seconds`;
  `py/_config_types.py:96-97`). Python accepts both.
- A `BigInteger` on an INT key is refused with "Expected value to be a 32-bit integer, but it was a
  System.Numerics.BigInteger" (Java: "…but it was a java.math.BigInteger"), and with "…a 64-bit
  integer (long), …" on a LONG key. Python accepts any `int` there.
- A `BigInteger`, `decimal`, `Int128` or `UInt128` on a DOUBLE key is sent as its digits and the
  core holds the nearest `double` (2^53+3 holds `9007199254740996`; 10^400 holds `Infinity`, which
  the key's validator then judges and prints as `Infinity`, as in Java).
- An `int` on a DOUBLE key goes as `3`, same as Python (F6); the core holds and prints `3.0`, as Java.
- `nint` and `nuint` are refused on every key.

**D8 — where the C# key-type table comes from (ledger 353). Ruled as picked** (2026-11-05).
- (a) Generated: a C# renderer in `X/config_types.rs` writes `src/Internal/ConfigTypes.g.cs`
  (internal: three key → type maps and three null-rule maps). It joins `all_outputs`, so
  `generate-error-codes` writes it and `check-generated` keeps it equal to Java's `ConfigDef` on
  every Kafka upgrade, with Python's table. The custom validators' messages are evaluated a second
  time with Java's `null` (F7). Cost: Rust in the root `actor-executor` / `kafka-critic` lane, and
  the commit runs the full pre-commit hook; `check-generated` (a Rust-side check) then covers one
  `dotnet/` file.
- (b) Hand-written C# (produced once, then edited by hand), plus a unit test that reads Python's
  generated `py/_config_types.py` and asserts the same keys, types and rules (Java's `null` where
  Python says `None`). Dotnet lane only; a Kafka upgrade fails that test until someone edits the
  C# by hand; a `dotnet/` test then reads a `python/` file.
- (c) Hand-written C# checked by a test against `kafka/` itself, as `PublicNamespaceLayoutTests`
  checks namespaces (`tests/PublicNamespaceLayoutTests.cs:423-447`). That test is a plain regex
  search; checking `ConfigDef` would mean a second reader of `define(...)` calls, constants,
  `withClientSslSupport` and the custom validators (742 lines in the xtask), in C#.

Pick **(a)**: one reader of Java's `ConfigDef`, Python's way, and the renderer is small (F7).

**D9 — Mode A, with the deviation recorded (ledger 354). Ruled as picked** (2026-11-05). (a) Mode A, recording the deviation
from DC's "no Kafka logic" law with Python's end condition (S0 R4): "goes when the C ABI takes
typed config values"; (b) Mode A, and also put Mode B (§5) to the core owners as a proposal, so
both bindings can later drop their tables; (c) Mode B first, blocking this phase on the core.
Pick **(a)**, with (b) as Pranav's call: §5 shows what Mode B would remove and what it would not.

**D10 — config constants, ledger 271 (ledger 355). Ruled as picked** (2026-11-05). Ruled 2026-10-28 as "a phase of its own after
M20/P1", generated from the Java sources as Python's are. Python's RFC example pairs them with
typed values (`ProducerConfig.LINGER_MS_CONFIG: 5`, `rfc/apis/python.md:192-229`).
- (a) M22/P2, right after this phase: a C# emitter in `X/config_consts.rs` (its rendering is
  inline Python, `:1246+`, so a C# emitter is larger than D8's), 420 constants and the enums
  `SslClientAuth`, `SecurityProtocol`, `GroupProtocol`, in the M20/P1 namespaces.
- (b) fold it into M22/P1: one Rust-lane step for both renderers, a phase twice the size.
- (c) leave it on the follow-up list.

Pick **(a)**: it is what ledger 271 already says, and this phase stays one reviewable change.

**D11 — the serializer keys when instances are given (ledger 356). Ruled (a), Python's way**
(Pranav, 2026-11-05: "Regarding D11 lets go with (a)"; reason: "we will eventually do M21/P3
later"). Not the pick below: the five own-role serializer keys never reach the core, and the two
class keys are neither checked nor sent. Consequences: the three encoding keys
(`serializer.encoding`, `key.serializer.encoding`, `value.serializer.encoding`; consumer: the
deserializer ones) are not listed as unused, as in Python; M21/P3's configuration-route serdes
will read them from the user's dictionary later. The other role's keys, and every key on the
admin client, still reach the core. The analysis before the ruling, kept for the record:

.NET always gives instances.
Java ignores the class keys and lists the encoding keys as unused (F22); Python skips the given
keys and drops all five own-role keys (`py/_config.py:77-84`), because its string serdes read the
encoding keys from the typed copy; .NET today sends all five, listed as unused.
- (a) Python's: the five own-role keys never reach the core, and the two class keys are not
  checked.
- (b) Java's, for a given instance: `key.serializer` / `value.serializer` (consumer:
  `key.deserializer` / `value.deserializer`) are not checked and not sent; `serializer.encoding`,
  `key.serializer.encoding`, `value.serializer.encoding` (consumer: the deserializer ones) reach the
  core and are listed as unused, as Java lists them, because nothing in .NET reads them.
- (c) today's: all five reach the core; the class keys are checked as CLASS.

The pick was **(b)**: the class keys match both Java and Python; for the encoding keys, the unused
line is true in .NET, where the serdes have no `Configure`. Superseded by the ruling above, which
takes (a) because M21/P3 will come back and its serdes will read the encoding keys.

**D16 — Java's value text in the binding's own messages, and ledger 273 (ledger 357). Ruled as picked** (2026-11-05). Narrowed
2026-11-05 by D2's conversion: the text sent to the core needs no `Double.toString` any more. Java's
text is needed only where the binding itself prints the user's value: its wrong-type, null-rule and
"Key must be a string." messages, "Invalid value {v} for configuration {k}: …", where Java prints
`String.valueOf(value)` (`J/common/config/ConfigException.java:39-40`), e.g. a `42.5` on an INT key.
The text printed, per C# type:

| C# value | Java class | Printed | Same as Java? |
|---|---|---|---|
| `bool` | `Boolean` | `true` / `false` | yes |
| `sbyte`, `short`, `int`, `long`, `BigInteger` | `Byte`, `Short`, `Integer`, `Long`, `BigInteger` | invariant digits | yes |
| `double` | `Double` | Java's `Double.toString` (the JDK 19 form Python's `_double_to_string` follows, `py/_java.py:59-84`, with its two-digit subnormal rule): `1.0E7`, `100.0` | yes |
| `float` | `Float` | Java's `Float.toString`: `0.1f` as `0.1`, not `0.10000000149011612` | yes |
| `decimal` | `BigDecimal` | Java's `BigDecimal.toString`: plain digits, or scientific when the adjusted exponent is below −6 (`0.0000001m` as `1E-7`, `0.00000010m` as `1.0E-7`), as .NET would not print it | yes, by porting that one rule |
| `char`, `string` | `Character`, `String` | itself | yes |
| a list, array or set; a dictionary | `List` / `Set`; `Map` | `[a, b]`; `{k=v}`, elements by these rules (Python's `java_str`) | yes for collections; a C# array prints `[a, b]` where Java prints `[Ljava.lang.String;@1b6d3586`, recorded |
| `byte`, `ushort`, `uint`, `ulong`, `Int128`, `UInt128`, `nint`, `nuint` | none | invariant digits (.NET's own) | no Java class |
| `Half` | none | .NET's own invariant text (`0.5`) | no Java class |
| `TimeSpan`, an enum, any other object | none, or not the same class | .NET's own `ToString()`, invariant culture | no Java class |

- (a) An internal helper for these messages: Java's `Double.toString` and `Float.toString`, and
  `BigDecimal.toString`'s scientific rule for `decimal`. It should give the same text on every
  runtime, net462 included: take the runtime's round-trip digits only after checking that they parse
  back, else 17 digits; on .NET Framework a message may then show more digits than Java's (§10 R11).
  Row 273's audit of the `ToString`s that print a `double` (`ClientQuotaAlteration.cs:146`, whose
  doc already notes Java's `1.0` against .NET's `1`) stays its own follow-up and can reuse the
  helper.
- (b) (a), plus row 273 folded in.
- (c) (a) without the `BigDecimal` rule: a `decimal` prints .NET's plain digits, a recorded
  difference in a message only.

Pick **(a)**: the messages read as Java's, consistent with D7; row 273 changes printed text on other
types and needs its own audit.

**D19 — the phase's logistics (ledger 358). Ruled as picked** (2026-11-05). N=108; a branch from master at the base (or later
master, re-checked); if D8 is (a), S1 is Rust: the root `actor-executor` writes it, `kafka-critic`
reviews it with comments in the repo-root `COMMENTS.108.md`, and the commit runs the full pre-commit
hook; S2–S8 are the `dotnet-actor`'s, reviewed by the `dotnet-critic` in `dotnet/COMMENTS.108.md`;
a Critic pass after each Actor commit (agent-roles). Pick: as written.

### 3.4 Group 4 — keep .NET's way (ledger 359–360) — both rows ruled as picked (2026-11-05)

| D | Python | Java | .NET (kept) | Clean Python way? |
|---|---|---|---|---|
| D7 message text (**Ruled as picked**) | `True`, `None`, `builtins.float` (`config_error.py:60`, `py/_config.py:89`) | `true`, `null`, `java.lang.Long` | Java's spellings, as .NET prints them elsewhere (`Header.ToString`'s `null`, `src/Common/Header/Header.cs:94-98`; the bool casing fixed in N=82): "Invalid value true for configuration retries: Expected value to be a 32-bit integer, but it was a System.Boolean". The type is `value.GetType().ToString()` (F17). A double in the message is Java's `Double.toString` (D16) | **Yes**: C#'s own spellings (`True`, `False`, `null`), as Python uses its host's. Leave it open if wanted |
| D17 `null` map (**Ruled as picked**) | `TypeError("configs must not be None")` | NPE | `ArgumentNullException(nameof(configs))`, Python's check in .NET's idiom (FM §A5) | no: it is the same check |

### 3.5 S0 approval (ledger 361) — ruled: R1–R12 approved verbatim (2026-11-05)

Approve R1–R12 of §6.1 verbatim, as drafted for the picks above, with D11's ruled (a) and D2's
exact-digits conversion already in R4. A different ruling on D2, D4, D6, D8 or D12 changes the clause
that names it; I redraft before S0.

## 4. What the steps build

- **`src/Internal/ConfigTypes.g.cs`** (D8): `Producer`, `Consumer`, `Admin` maps, key →
  `ConfigType` (`Boolean`, `String`, `Password`, `Int`, `Short`, `Long`, `Double`, `List`,
  `Class`), and the three null-rule maps, key → (named or plain, Java's message).
- **`src/Internal/JavaText.cs`** (D2, D15, D16): two jobs. (1) The text sent to the core (D15,
  D2's table): invariant digits for integers and `decimal`, invariant "G17" for a `double` (a
  `float` or `Half` widened first), the special values and `-0` as literals; no rounding code, the
  core rounds. (2) Java's `String.valueOf` text for the binding's own messages (D16): Java's
  `Double.toString`, `Float.toString` and `BigDecimal.toString`, `[a, b]` / `{k=v}` for
  collections, all in the invariant culture.
- **`src/Internal/ClientConfigs.cs`**, `Prepare(configs, client)`: Python's `prepare()` in Java's
  order: the `null` map (D17); every key (D13); then each entry in enumeration order: an own-role
  serializer key skipped, neither checked nor sent (D11 (a)); a `null` by the null rule (D5); the type check for a key in the table (D2–D4),
  with `ConfigException` and D7's text; the text (D15). It returns the ordered key/text pairs; it
  keeps no copy of the dictionary (Python's typed copy feeds serde `configure()`, which .NET does not
  have).
- **`src/Internal/Interop/ConfigMarshal.cs`**: one function that pins the pairs, builds the
  `kafka_Map_t`, calls the client's config constructor and frees in `finally`, replacing the three
  loops (F3).
- **`src/Common/Config/ConfigException.cs`** (D6), and the −10 case in `KafkaException.cs:173-180`.
- `NativeProducer` reads `max.block.ms` from the text pairs; `NativeConsumer` sets
  `_groupIdDefined` from "present and not `null`" (D14).

The binding's code becomes Kafka logic of the kind Python carries, and no more: the type check, the
null rule, the text. Ranges, valid strings, number text, unknown keys and every cross-key rule stay
the core's.

## 5. Mode A vs Mode B

**Mode A (this plan).** The ABI exists (F1), so DC §6.1 says Mode A. The binding takes on the type
check and the text, which DC's one law ("holds no Kafka logic", `DC:15-22`, from the same 2026-08-12
scaffold as F8) forbids. Python's binding carries the same code under a recorded deviation with an
end condition (PC, exception 2, `CLAUDE.md:493`); this plan records the same deviation in DC §4
"Config" (S0 R4), with the same end condition. The cost in .NET: one generated table (D8), three
internal files (§4), and one more place that must follow Java's `ConfigDef` on a Kafka upgrade
(`check-generated` covers the table under D8 (a)).

**Mode B (not proposed for this phase).** A typed C ABI, at sketch level:

```
kafka_common_config_ConfigValue_t *kafka_common_config_ConfigValue_boolean(int8_t v);
… _short(int16_t), _int(int32_t), _long(int64_t), _double(double),
  _string(const char *), _list(const kafka_List_t *values), _null(void),
  _other(const char *class_name)       // any other object: its class, for the message
void kafka_common_config_ConfigValue_destroy(kafka_common_config_ConfigValue_t *);
kafka_producer_ProducerConfig_t *kafka_producer_ProducerConfig_with_values(
    const kafka_Map_t *values /* const char * -> ConfigValue_t * */, kafka_common_Error_t **out_err);
(and the consumer's and the admin client's)
```

The core would add a public typed constructor over its private `build(ConfigMap)` (F11), with a
public value type that is a struct with constructors, not an enum carrying values inline (root
`CLAUDE.md` §3).
- It would remove: both bindings' type tables and checks (`py/_config.py:96-179`, Python's
  generated table, and everything in §4 but `ConfigMarshal`), PC exception 2, and the null
  deviation. Java's `null` would be kept (F24), and a list element containing a comma would stay one
  element.
- It would not fix: the keys the core does not define (F9: every CLASS key, three passwords,
  `enable.metrics.push`, …) stay unchecked unless the core's `ConfigDef` learns them; and the type
  names in the core's messages would be Java's (`java.lang.Long` for a C# `long`).
- It costs: core API and ABI design and review (root lane and the core owners), a PC change and
  Python's migration, then a smaller .NET change.

So Mode B is the core owners' call and worth raising only to retire two tables; D9 asks whether to
raise it. This phase does not wait for it.

## 6. Steps (one commit each)

- **S0 — rules** (markdown only): R1–R12 below, verbatim.
- **S1 — the key-type table** (D8). Under (a), in the Rust lane: the C# renderer in
  `X/config_types.rs`, `src/Internal/ConfigTypes.g.cs` generated, an xtask unit test for the C#
  output (Java's `null` in the three custom-validator messages), `check-generated` green. Under (b):
  the C# file plus its test against `py/_config_types.py`.
- **S2 — `ConfigException`** (D6), and −10 → `ConfigException` in the error table; the −10 tests of
  F10 updated where they assert the exact type.
- **S3 — `JavaText`** (D2's text, D15, D16), with its tests (§7).
- **S4 — `ClientConfigs.Prepare`** (D2–D5, D11, D13, D15), with the helper-level tests (§7). Internal
  and unused until S5.
- **S5 — the switch** (D1, D12, D14): the six constructors and their XML docs (F4's lines); one
  `ConfigMarshal` in place of the three loops and checks (F3); the two managed reads; every caller
  in §8; the three tests of F13 rewritten for D5 and D17.
- **S6 — client-level tests** (§7: the constructor-level ports, the smoke per client and flavour,
  `max.block.ms` and `group.id`).
- **S7 — comment-only commit** (ledger 59).
- **S8 — close-out**: the STATUS entry; `design/current` updated; `COMMENTS.DONE.108.md` archived
  here; the ledger rows' statuses; this plan and its folder committed with the phase (DC §8.4).

### 6.1 S0 rule drafts (written at `95aa08565`; each replaces exactly the text quoted)

**R1 — DC §3, the producer sketch (`DC:285-286`).** Replace
```
    public AsyncKafkaProducer(IReadOnlyDictionary<string, string> config,
        ISerializer<TKey> keySerializer, ISerializer<TValue> valueSerializer);
```
with
```
    public AsyncKafkaProducer(IReadOnlyDictionary<string, object?> configs,
        ISerializer<TKey> keySerializer, ISerializer<TValue> valueSerializer);   // typed values, §4 Config
```

**R2 — DC §3, the async consumer sketch (`DC:435-436`).** Replace
```
    public AsyncKafkaConsumer(IReadOnlyDictionary<string, string> config,
        IDeserializer<TKey> keyDeserializer, IDeserializer<TValue> valueDeserializer);
```
with
```
    public AsyncKafkaConsumer(IReadOnlyDictionary<string, object?> configs,
        IDeserializer<TKey> keyDeserializer, IDeserializer<TValue> valueDeserializer);   // typed values, §4 Config
```

**R3 — DC §3, the sync consumer sketch (`DC:506-507`).** Replace
```
    public KafkaConsumer(IReadOnlyDictionary<string, string> config,
        IDeserializer<TKey> keyDeserializer, IDeserializer<TValue> valueDeserializer);  // 3-param (decision A)
```
with
```
    public KafkaConsumer(IReadOnlyDictionary<string, object?> configs,
        IDeserializer<TKey> keyDeserializer, IDeserializer<TValue> valueDeserializer);  // 3-param (decision A); typed values, §4 Config
```

**R4 — DC §4, the "Config" row (`DC:672`).** Replace
"| **Config** | `IReadOnlyDictionary<string,string>` → per-entry `ProducerProperties_put` (consumer: `ConsumerProperties_put`); keys are **Java dotted names** (`bootstrap.servers` required); coerce non-string values to `str`; classic-/consumer-only keys accepted silently. | wiring the constructor |"
with
"| **Config** | One `IReadOnlyDictionary<string, object?> configs` on each `Kafka*` client constructor: Java's `Map<String, Object>`, with no `Properties` form, as Python merges it. Keys are **Java dotted names** (`bootstrap.servers` required). A value is a C# value of a type Java's `ConfigDef.parseType` accepts for the key's type (`bool`; `int` for INT, `short` for SHORT, `int` or `long` for LONG; any number but `nint` / `nuint` for DOUBLE, `BigInteger` included; an `IList` for LIST; a `System.Type` for CLASS) or the string Java would parse. The binding checks the type against the key's Java `ConfigDef` type (`Internal/ConfigTypes.g.cs`, generated from `kafka/` by `cargo xtask generate-error-codes`) and sends a text the core parses to the value Java's `parseType` gives (integers and `decimal` as digits, a `double` as "G17"; the core rounds); a wrong type, a `null` the key's validator refuses or a `null` key throws `ConfigException` (code −10) with Java's message, before any P/Invoke. Any other `null` leaves the key out, so the core's default applies. The value itself (ranges, valid strings, number text) is the core's to check, and the core's errors are `ConfigException` too; unknown keys reach the core, which logs them as unused. The five own-role serializer keys (`key.serializer`, `value.serializer`, `serializer.encoding`, `key.serializer.encoding`, `value.serializer.encoding`; consumer: the deserializer ones) are neither checked nor sent, as in Python. *(Deviation from the one law: the type check and the text are Kafka logic, here because the C ABI takes every value as a C string and only the binding sees the object's type, as root `CLAUDE.md`'s Python conventions record for Python (Implementation over the FFI, exception 2); and a `null` the validator accepts is "not set" where Java holds `null` (ledger 272). Goes when: the C ABI takes typed config values.)* | wiring the constructor (M22/P1) |"

**R5 — DC §4, the "Error granularity" row (`DC:673`).** Replace
"thrown by `Poll` for a key or value that fails to deserialize when no record precedes it (decision 11). | if catch-by-type is needed |"
with
"thrown by `Poll` for a key or value that fails to deserialize when no record precedes it (decision 11). The third is **`ConfigException : KafkaException`** (M22/P1), Java's `common.config` class, code −10: thrown by the client constructors for a configuration value the binding refuses, and the type of the core's −10 (§4 **Config**). | if catch-by-type is needed |"

**R6 — DC §3, the idiom map: a new row after "| `String` topic / config | UTF-8, hand-marshalled | ffi §A3/§B3 |" (`DC:634`):**
"| `Map<String, Object>` / `Properties` (a client's configuration) | one `IReadOnlyDictionary<string, object?> configs` per `Kafka*` constructor; a value of the C# type Java's `parseType` accepts for the key, or Java's text; a `null` meets the key's validator or is not set | §4 **Config**; ffi §0.1, §A5 |"

**R7 — DC §2, the `Internal/` bullet (`DC:166-169`).** Replace
"`_cb` → `TaskCompletionSource` adapters, ffi §A7/§B7), config → properties marshalling."
with
"`_cb` → `TaskCompletionSource` adapters, ffi §A7/§B7), the client configuration's type check and
  text (§4 **Config**)."

**R8 — FM §0.1 (`FM:101-102`).** Replace
"  - Prefer `ProducerProperties_put` / `ConsumerProperties_put` over `_from_configs`
    (avoids marshalling a `const char *const *`)."
with
"  - A client's configuration crosses as one `kafka_Map_t` of NUL-terminated UTF-8 keys and values
    (`kafka_Map_new_err`, `kafka_Map_put_err`), read by the client's config constructor
    (`kafka_producer_ProducerConfig_new`, `kafka_consumer_ConsumerConfig_new`,
    `kafka_admin_AdminClientConfig_new`) and freed, with the config, after the client's
    constructor. The ABI takes text only, so each value is checked and converted to Java's text
    first (`dotnet/CLAUDE.md` §4 **Config**). A `NULL` value reads as `""`
    (`rust/src/ffi/util.rs:97-111` at `95aa08565`), so a key that is not set is left out, never
    passed as `NULL`. One marshaller builds the map for the three clients."

**R9 — FM §A5, a new bullet after the Precondition bullet (after `FM:769`, "across FFI is UB."):**
"  - **Configuration errors are not preconditions.** A value of a type its key does not accept, a
    `null` its key's validator refuses, or a `null` key throws `ConfigException`
    (`Confluent.Kafka.Common.Config`, code −10) with Java's message, before any P/Invoke; the core's
    own configuration errors (ranges, valid strings, number text) arrive as the same type
    (`dotnet/CLAUDE.md` §4 **Config**). A `null` map stays `ArgumentNullException`."
**and Tests required, a new bullet after the one at `FM:799-800`:**
"  - A config value of the wrong type and a `null` its validator refuses → `ConfigException` with
    Java's exact message, before any native call; a configuration the core refuses →
    `ConfigException` too."

**R10 — FM §B5, the same bullet after the Precondition bullet (after `FM:1908`, "which the closed
check keeps from reaching C."):** R9's first bullet, verbatim. **And Tests required, after the
bullet at `FM:2053-2054`:** R9's test bullet, verbatim.

**R11 — FM §B5, the `GroupMetadata()` bullet (`FM:1909`).** Replace
"  - **`GroupMetadata()` without `group.id` (ledger 105).**"
with
"  - **`GroupMetadata()` without `group.id` (ledger 105; a `null` value counts as without, as in
    Python, M22/P1).**"

**R12 — DC §4, the "Error granularity" row's sibling in the idiom map: none.** The `KafkaException`
hierarchy row (`DC:621`) already says "Typed subclasses may be added later; they must derive from
`KafkaException`", which covers D6. Nothing to change; listed so the Critic does not look for it.

## 7. Tests

DoD §3: every Java test of the translated behaviour, each named as translated or not with its
reason; error text asserted, not just the type. Python's tests are translated where they test
behaviour .NET now has. Placement: helper-level tests in `tests/Internal/ClientConfigsTests.cs`
and `tests/Internal/JavaTextTests.cs`; client-level ones in `tests/PublicClientConfigTests.cs`.

**Java, `JT/common/config/ConfigDefTest.java`**
- `testBasicTypes` (:55): translated at the helper level with .NET's values: `"1   "` (INT) sent
  unchanged, `2` (`int`) for LONG accepted as `2`, `" a , b, c"` (LIST) unchanged, `42.5` (DOUBLE)
  as `42.5`, `"java.lang.String"` (CLASS) unchanged, `"true"` / `"FalSE"` / `"TRUE"` unchanged,
  `"password"` (PASSWORD) unchanged. The parsed values are the core's, not asserted here.
- `testBadInputs` (:123): the binding's half with .NET's exact messages: INT `42.5`,
  `long.MaxValue` (a `long`), `new object()`; LONG, DOUBLE, STRING `new object()`; LIST `53` and
  `new object()`. The text half (`"hello"`, `"42.5"`, `"9223372036854775807"`, its `"00"` form,
  `"truee"`, `"fals"`) through a client constructor, asserting the core's message ("Not a number of
  type INT", "Expected value to be either true or false"). `"ClassDoesNotExist"` (CLASS): not
  translated, since .NET loads no class and the core has no CLASS key (F9).
- `testValidateCannotParse` (:341): "Invalid value non_integer for configuration batch.size: Not a
  number of type INT" through the producer constructor.
- `testConvertValueToStringBoolean` (:594), `…Short` (:600), `…Int` (:606), `…Long` (:612),
  `…Double` (:618: `3.125`, `1.7976931348623157E308`, `1.024E8`, `-1.024E8`), `…String` (:627),
  `…List` (:640), `…Class` (:646), `…NestedClass` (:655, a nested type's `FullName` with `+`),
  `…NullType` (:663): translated in two halves. The value half through a config handle: the text
  D15 sends is read back with the config's typed getter as Java's parsed value (`true`, `32767`, …,
  each `double` bit for bit). The text half against D16's message helper: the four `Double`
  vectors print as Java's `Double.toString` (`1.024E8`, not .NET's `102400000`), a list as `[a, bc,
  d]` where Java's `String.valueOf` prints it. `…Password` (:633): the raw-string half only; .NET has
  no `Password` type, so the `[hidden]` half does not apply.
- `testValidators` (:164), `testNullDefault` (:98), `testNullDefaultWithValidator` (:198),
  `testListValidatorAnyNonDuplicateValues` (:771), `testListValidatorIn` (:824): their `null` half is
  the null-rule matrix below; the value half is the core's `ConfigDef`, not translated here.
- The other 30 tests (`testInvalidDefault`, `testMissingRequired`, `testDefinedTwice`,
  `testGroupInference`, `testParseForValidate`, `testValidate*`, the docs and RST tests, the unit
  and list-size tests, …): not translated; they test building a `ConfigDef`, which .NET does not do.

**Java, `JT/common/utils/UtilsTest.java`**: `testCastToStringObjectMapNonStringKey` (:1009) → D13's
`null` key; `testCastToStringObjectMap` (:996) → typed values survive `Prepare`; the four
`testPropsToMap*` (:916-971) not translated: no `Properties` form (D1).

**Java, the config classes** (through the constructors, as Python's
`pyt/test_config_class_ports.py:91`, `:98`): `ProducerConfigTest` `:91`, `:101`, `:122`, `:157`
(rejections) and `:144` (valid strategy); `ConsumerConfigTest` `:146`, `:157`, `:228`, `:240` (the
invalid row); `AdminClientConfigTest` `:43`. Translated beyond Python, because they pass typed values
and the core raises them at construction: `ConsumerConfigTest.testUnsupportedConfigsWithConsumerGroupProtocol`
(:255) with `int` values for `heartbeat.interval.ms` and `session.timeout.ms`
(`core/consumer/consumer_config.rs:1214`; the `partition.assignment.strategy` row is translated only
if the core raises it, since the core does not define that key, F9), and
`ProducerConfigTest.testUpperboundCheckOfEnableIdempotence` (:180) and
`testTwoPhaseCommitIncompatibleWithTransactionTimeout` (:196) with `bool` and `int` values, each only
if S6 shows the core raises Java's message at construction; otherwise skipped with that reason, as
Python skips them. The others are skipped with Python's reasons (`test_config_class_ports.py:27-45`):
`appendSerializerToConfig` (.NET always takes instances), the parsed-config reads (.NET exposes no
parsed config), the properties-file checks.

**Java, the clients**: `KafkaProducerTest` `:745`, `:754`, `:762` and `KafkaConsumerTest` `:713`,
`:724`, `:735` (socket buffer sizes) with `int` values; `KafkaProducerTest.testDeliveryTimeoutAndLingerMsConfig`
(:2843) with `int` values; `KafkaAdminClientTest.testDefaultApiTimeoutAndRequestTimeoutConflicts`
(:155) with `int` values. Not translated: `KafkaProducerTest.testNoSerializerProvided` (:538; .NET
requires instances), `testConstructorWithNotStringKey` (:580; the `Properties` path, F21; D13's
`null` key stands for it), `testUnusedConfigs` (`KafkaProducerTest:2466`, `KafkaConsumerTest:3963`;
they read `unused()`, and the core's INFO line is not observable from a .NET test, as Python checks it
only in a subprocess, `pyt/test_config.py:255`).

**Python** (`pyt/`)
- `test_config.py`: `:48`, `:90`, `:160` (as the `testConvertValueToString*` halves above), `:184`
  (with D15's text: a float as "G17", an `int` on a DOUBLE key as `3`), `:200` and `:309` (as D11 (a):
  the five own-role keys dropped, the other role's sent), `:216`, `:233`, `:273`, `:299`, `:340`
  translated; `:103` translated with D2's rule (the out-of-range value as text,
  since a `long` on an INT key is refused); `:290` → D13; `:346` → the S1 table test. Not
  translated: `:111` (Python's own number-parser helpers), `:129` and `:326` (serdes from the
  configuration, M21/P3), `:255` (a subprocess log check), `:360`, `:371`, `:386` (Python's
  `Duration` helper; .NET takes `TimeSpan`).
- `test_config_matrix.py` (7 tests, `:74` … `:148`): all, over the C# table, for the three clients;
  the smoke tests on all six clients, with `TYPED_KEYS` (`:122`) in .NET's types (`(short)60` for
  the SHORT key, D2).
- `test_config_null.py` (7 tests, `:50` … `:120`): all.
- `test_kafka_producer.py` `:278`, `:288`, `:488`; `test_admin_client.py` `:96`, `:215`, `:236`
  (`:210` → D13); `test_admin_java_ports.py:413`; `test_producer_family.py:370`;
  `test_errors.py:424` (`ConfigException`'s constructors, Java's spellings);
  `test_common_types.py:502-526` (`java_str` and the `Double.toString` vectors, including the
  subnormals, `1.0E23` and `-0.0`, against D16's message helper); `test_none_is_null_producer_common.py:356` (the `null` map, on
  all six). Not translated: `test_kafka_producer.py:704`, `test_kafka_consumer.py:2226`,
  `test_serde_supply.py:128`, `:138` (M21/P3).

**.NET's own**
- Culture: under `de-DE` and `sv-SE` as the current culture, `0.5` and `-1` are sent as `0.5` and `-1`.
- D2's text, per type, at the helper level (`JavaTextTests`), exact strings: `sbyte` … `ulong`,
  `BigInteger`, `Int128`, `UInt128` (`#if NET7_0_OR_GREATER`) as invariant digits (`-5`,
  `18446744073709551615`, 401 digits for 10^400); `decimal` as its digits (`0.1`, `-12.50`); a
  `double` as "G17" (`0.10000000000000001`, `1E+21`); `NaN`, `Infinity`, `-Infinity` and `-0` as
  those literals; a `float` (`0.1f` → `0.10000000149011612`) and a `Half` (`#if NET5_0_OR_GREATER`;
  `Half.Epsilon` → `5.9604644775390625E-08`) widened to `double` first. Every text matches
  `parse_java_double`'s grammar (sign, digits, `.`, `e`/`E`, `+`/`-`, `NaN`, `Infinity`).
- The core rounds, through a real config handle (Interop tests, with a test-only declaration of
  `kafka_producer_ProducerConfig_get_double`): on `sasl.kerberos.ticket.renew.window.factor` (DOUBLE,
  no validator in the core, `core/common/config/sasl_configs.rs:406`), the `BigInteger` 2^53+3 reads
  back as `9007199254740996` (the cast gives `…994`); −(2^53+1) as `-9007199254740992` (a tie, to
  even); 2^64+2049 as `18446744073709555712`; 10^400 as `+Infinity`; the `decimal`
  `2090639523243.3333527855107988m` as the nearest `double` (the cast is one unit off); the `ulong`
  2^63+1025 as 2^63+2048 (a convert-as-signed sequence gives 2^63); the `Int128`
  31640652437294794598055815133 as `3.1640652437294794E28` (the .NET 8 cast gives `…803E28`).
- The `double` format round-trips through the core on every TFM: a few thousand doubles (random bit
  patterns, the subnormals, `MaxValue`, `-0`, `NaN`, the infinities) and floats sent through a config
  handle read back bit for bit. The unit tests run on net462 in CI's Windows block (x64 and x86), so
  this pins "G17" and the literals on .NET Framework (F18).
- D16's message text: a `42.5` on an INT key → "Invalid value 42.5 for configuration batch.size:
  Expected value to be a 32-bit integer, but it was a System.Double"; `1e7` prints `1.0E7`; `0.1f`
  prints `0.1`; `0.0000001m` prints `1E-7`; a `BigInteger` its digits; a `string[]` `[a, b]`.
- `max.block.ms` given as `int` and as `long` bounds an in-callback wait (the existing string test,
  typed).
- `group.id = null` is "not defined": `GroupMetadata()` throws code 24 with Java's message
  (ledger 105).
- A core −10 (`batch.size = "abc"`) arrives as `ConfigException` from all six constructors, and
  `catch (KafkaException)` catches it.
- The three tests of F13, rewritten: a `null` with no refusing validator constructs; a refusing one
  throws `ConfigException` with Java's message; the `null` map keeps `ArgumentNullException`.
- D11 (a), at the helper level: on a producer, none of `key.serializer`, `value.serializer`,
  `serializer.encoding`, `key.serializer.encoding`, `value.serializer.encoding` is sent, and
  `key.serializer = 5` (not a CLASS value) does not throw; on a consumer, the five deserializer keys
  likewise. The other role's keys still reach the core: a producer's `key.deserializer` and a
  consumer's `key.serializer` are sent, and on the admin client all ten are sent; a constructor smoke
  test per client shows the same.
- D2's refusals, with the exact message: `BigInteger` on INT ("Expected value to be a 32-bit
  integer, but it was a System.Numerics.BigInteger") and on LONG ("…a 64-bit integer (long), but it
  was a System.Numerics.BigInteger"); `uint` on LONG; `Int128` on LONG (net8+); `nint` on DOUBLE
  ("Expected value to be a double, but it was a System.IntPtr") and `nuint` on INT.

## 8. Caller migration

| Where | Sites / files | Change | Mechanical? |
|---|---|---|---|
| `tests/` (unit) | 133 / 55, plus 38 `Native*.Create` calls in 11 files | ~72 client-config dictionaries become `Dictionary<string, object?>` (the ~31 `NewTopic` config and tag maps stay); helpers `UnreachableProducers.cs:44-48`, `PublicConsumerApiTests.cs:292`, `PublicAdminNonNumericBrokerNameTests.cs:189`, `PublicAdminNegativePartitionTests.cs:115`, `PublicAdminP14SyncCloseTests.cs:178` | yes, except F13's three tests and F10's exact-type asserts |
| `dotnet/tests/Confluent.Kafka.IntegrationTests/` | 10 / 1 | the `Configs()` helper in `KafkaProducerBrokerTests.cs` | yes |
| `dotnet/grpc-server/` | 10 / 8 | `new Dictionary<string, string>(request.Config)` in the 8 servicers; the chaos factories' `Func<IReadOnlyDictionary<string, string>, …>` type; mock selection reads strings (`IsEmptyConfig`, e.g. `ProducerServiceImpl.cs:204`; `TranslateAdmin.SelectsMock`, :55), so it keeps reading the proto's string map | small, not purely textual |
| gRPC server unit tests | 4 / 2 | empty dictionaries (`ChaosConsumerTests.cs:339`, `:341`; `ChaosProducerTests.cs:302`, `:304`) | yes |
| `dotnet/tests/Performance` (PerfV3) | 6 / 3 | dictionary type; PerfV2 is ckd's client and is untouched | yes |
| `dotnet/soak` | 3 / 1 | `SoakClient.cs:256`, `:260`, `:1144`; its own string reads (`pconf["client.id"]` :255, `cconf.TryGetValue("group.id")` :259) and the properties-file parser (`SoakConfig.cs:214`) stay string-typed and convert at the constructor | small |
| `dotnet/tools/PackageSmoke` | 2 / 1 | `Program.cs:92`, `:95`, `:98`, `:104`, `:114` | yes |
| `dotnet/README.md` | 2 examples | `:45`, `:62`; one example shows a typed value (`["linger.ms"] = 5`) | yes |

## 9. Gates

DC §7.5, per commit and at the end:
- **S1 (Rust, if D8 (a)):** the full pre-commit hook (`.githooks/pre-commit`: build, test, format,
  lint, `check-generated`), with `RUST_TEST_THREADS=6` if its integration stage runs on this Mac.
- **S0, S2–S8 (dotnet only):** `dotnet build` over netstandard2.0, net8.0 and net10.0; the unit
  tests on net8.0 and net10.0; `dotnet format --verify-no-changes`; then a `--no-verify` commit, as
  for every dotnet-only commit. S5 also builds the gRPC server, soak, PerfV3 and PackageSmoke, runs
  `make -C dotnet test-grpc-server-dotnet`, and runs the integration tests with Docker
  (`make test-integration-dotnet`).
- **Close:** `make verify-dotnet` (Linux, Docker); after the push, CI's Windows block runs the unit
  tests on net462 (the "G17" round-trip through the core and D16's message vectors on .NET Framework, F18).
- **No perf A/B:** nothing on the send or poll path changes; construction only.

## 10. Risks and open questions

- **Q1 — who D1's break reaches.** No package was ever pushed by automation (F15). If anyone
  uploaded a `Confluent.Kafka` 4.4.0-alpha by hand to nuget.org, its users break too; Pranav knows,
  the repo cannot tell.
- **R1 — .NET Framework formatting** (F18): the text sent no longer depends on it ("G17" is the
  documented round-trip format there, and the special values and `-0` are literals), and the
  round-trip test runs on net462 in CI's Windows block; not runnable on this Mac. `-0` on .NET
  Framework, and its culture's symbols for `NaN` and the infinities, are not verified, which is why
  the helper writes all four as literals.
- **R2 — `Double.toString` in messages only** (D16): at a power-of-two boundary, or on .NET
  Framework, a message may print one or more digits more than Java's; the value sent is never
  affected. The vectors include Python's subnormal and large cases.
- **R3 — the null deviation** (F24): a `null` for a key with a non-null default and no validator
  keeps the default where Java holds `null`; recorded in R4, removed only by Mode B.
- **R4 — list elements with commas**: the core splits them (a string ABI; Python too).
- **R5 — keys the core does not define** (F9): for them the binding's type check is the only
  check, as in Python.
- **R6 — SHORT literals** (D2 (a)): `60` for a SHORT key is refused; the message names the type.
- **R10 — Java's `doubleValue()` rounding is taken from its Javadoc; no JDK was run.**
  `BigInteger.doubleValue()` and `BigDecimal.doubleValue()` are documented to round to nearest and
  give ±Infinity past the range. The core's parse rounds correctly (Rust's `str::parse::<f64>`,
  probe 7: `9007199254740995` → `…996`, 401 digits → `inf`), and agreed with .NET Core's IEEE-correct
  `double.Parse` on every one of probe 7's 10,011 integer and `decimal` texts. If Java rounded
  differently in some case, the core would hold a neighbouring `double` there.
- **R11 — what probe 7 did not cover**: it ran on arm64, .NET 8 and 10 only, with a copy of
  `parse_java_double`, not the core itself. .NET Framework's "G17" and x64/x86 are covered only by
  the round-trip test in CI's Windows block, through the real core.
- **R12 — the three newer types on the netstandard2.0 asset.** An app on .NET 5–7 gets that asset
  and its `Half`, `Int128` or `UInt128` is refused as an unknown type; only out-of-support runtimes
  are affected (D2).
- **R7 — D6's type change**: an exact-type assert or a `GetType() == typeof(KafkaException)` on a
  −10 path breaks; S2 sweeps `src/`, `grpc/` and the tests for both.
- **R8 — the lane split** (D8 (a), D19): S1 needs the root personas and the hook's run time before
  the dotnet steps can start.
- **R9 — M21/P3**: D11 is ruled (a), so the encoding keys never reach the core now; M21/P3's
  configuration-route serdes will read them from the user's dictionary when it comes back.
- **Q2 — Mode B** (D9 (b)): raise it with the core owners or not.
- **Q3 — DC §7.5** omits CI's Windows block (F18): a one-line rule fix, outside this phase; say if
  it should ride with S0.

## 11. Rulings

Pranav, 2026-11-05, verbatim, relayed by the coordinator:
1. "have the PM switch D2's conversion to sending the exact digits". A direction on D2's
   conversion, applied in D2, D15 and D16 (ledger 351, 352, 357); those decisions stay Pending.
2. "Regarding D11 lets go with (a)", with the reason given earlier the same day: "we will
   eventually do M21/P3 later". Ledger 356, Agreed: (a), not the pick (b).
3. "I agree with all the pending rows with PM picks." Ledger 342–355 and 357–361, Agreed as picked:
   the facts (342); D1, D12, D3, D4, D5, D6, D13, D14 (343–350); D15, D2 and D16 as amended after
   direction 1 (351, 352, 357); D8, D9, D10, D19 (353–355, 358); D7, D17 (359, 360); and the S0
   drafts R1–R12, verbatim as they stand in §6.1 (361).
4. "Rows 362-364 agree with PM's picks." Ledger 362–364, Agreed as picked.
5. "Squash the changes to single commit like earlier". The 17 commits are squashed into one; the
   originals are in the local tag `m22p1-before-squash`.

Every decision is ruled; the phase is done (STATUS, Milestone 22 / Phase 1).
