# Plan: .NET verifiable clients for Kafka's system tests (M24/P1)

Status: **Done 2026-11-17** (approved 2026-11-16 by Pranav). Rulings in §9, ledger rows 551–564; the evidence is in
`dotnet/design/current/STATUS.md` (M24/P1) and `COMMENTS.DONE.130.md` beside this plan. Built on master `81ddfa9c0`. Manager N=130
(129 is the paused M23/P7). Branch `prashah_dev_dotnet_verifiable_clients`, cut from `master` at `1a2495f62`.
Mode A: no C ABI change, no library change, no Python change. The plan goes in with the phase (`dotnet/CLAUDE.md` §8.4).

**Goal.** .NET ports of Apache Kafka's `org.apache.kafka.tools.VerifiableProducer` and `VerifiableConsumer`, and
of their rate limiter `org.apache.kafka.server.util.ThroughputThrottler`, over the .NET binding. Kafka's ducktape
system tests (`kafkatest`) then run the .NET client in place of the Java tools, through
`kafkatest.services.verifiable_client.VerifiableClientApp`. The Python tools in `python/verifiable_clients/`
(#276, `c30733560`) are the sibling and the reference; the Rust tools in `rust/tools/verifiable-clients/` settle
what Java leaves open, as they did for Python.

**Scope exception.** Kafka's `tools` module is outside "client only". The Rust milestone recorded a one-off
exception for these two tools (`design/history/Milestone-14-verifiable-clients/PLAN.md` §2.1) and Python followed
it. This plan asks for the same exception for .NET (§6, F8). `VerifiableShareConsumer` stays out, as in Rust and
Python.

**The contract is stdout, not the API.** `verifiable_client.py:47-58` (in `kafka/tests/kafkatest/services/`):
one JSON object per line on stdout, logs on stderr. The services parse each line with `json.loads` and skip
what does not parse (`verifiable_producer.py:324-331`, `verifiable_consumer.py:461-464`).

---

## 1. Summary

| Group | Items | Lean |
|---|---|---|
| 1. Fact refreshes, approved as one | F1–F8 | approve |
| 2. Python's way, one table | V1–V25 | Python's way each |
| 3. One at a time | Q1–Q7 | Q1 A · Q2 A · Q3 A · Q4 A · Q5 A · Q6 A · Q7 A |
| 4. Keep .NET, one item | K1–K10 | keep |

No `CLAUDE.md` rule blocks Java or Python parity here. The rules that touch the tools (constructor-only serdes,
ledger 365; the comment rule, ledger 38; the §2 namespace rule) agree with Python's choices.

Effort: CP1 ≈ 1.5–2 days, CP2 ≈ 1 day, plus the local ducktape run of Q6 (≈ half a day with the cluster build).

---

## 2. Facts (group 1)

- **F1. M24 is free.** No `dotnet/design/history/M24*` exists on any branch (`git log --all -- 'dotnet/design/history/M24*'`
  is empty); only M23/P2 and M23/P3's plans mention "M24/P1" as an alternative that was not taken. N=130; ledger
  rows start at 551 (534 is the highest row; the paused M23/P7 reserved 535–550). Re-check both at approval.
- **F2. The core is still PLAIN-only for SASL and PEM-only for TLS**, so Python's "what cannot pass" list holds for
  .NET too, verified, not assumed: `rust/src/producer/producer_config.rs:1736` ("only PLAIN is supported by this
  client"), `rust/src/common/config/ssl_configs.rs:83-89` (the default keystore type is PEM; the rustls factory
  rejects JKS and PKCS12), `rust/src/common/config/sasl_configs.rs:23-27`. `rust/src/common/security/scram/` is
  the admin API's `ScramMechanism`, not an authenticator. The .NET binding adds no SSL or SASL limit of its own:
  `sasl.jaas.config` is a PASSWORD key, and a string is accepted for it (§4 **Config**).
- **F3. Every client API the tools call exists in .NET** (§4 table). No C ABI and no library change is needed for
  the tools themselves.
- **F4. Two mock members Python's tests use have no .NET counterpart**: `MockProducer.history()` (.NET has only
  `HistoryCount()`, `MockProducer.cs:182-186`, a recorded deviation: "the native mock holds the records
  serialized") and `MockConsumer.schedule_poll_task` (.NET has only `ScheduleNopPollTask()`,
  `MockConsumer.cs:326-328`; the C ABI has only `kafka_consumer_MockConsumer_schedule_nop_poll_task`, so a
  `SchedulePollTask` would be Mode B). Q3 handles them in the tests, with no library change.
- **F5. Python has 115 test cases**: 114 functions, the consumer's signal test parametrized twice. `test_common.py`
  15, `test_throughput_throttler.py` 12, `test_verifiable_producer.py` 38, `test_verifiable_consumer.py` 49 (+1).
  The Rust tool has 65 (`throughput_throttler.rs` 10, `verifiable_producer.rs` 25, `verifiable_consumer.rs` 30),
  all unit tests, no subprocess test.
- **F6. A send blocked on metadata is released by a close from another thread, by the docs.** `IProducer.cs:369-372`:
  "A `Send` whose record is still waiting for metadata or buffer memory when the producer closes fails with the
  `KafkaException` 'Producer closed while send in progress'. Its delivery callback does not run." The sync close
  submits the native close before it waits for the calls in progress (`NativeProducer.cs:1067-1121`:
  `SubmitClose`, then `TearDown` → `_uses.WaitForNoneThenMarkDestroyed()`). The 2026-08 note that "a concurrent
  close cannot wake a send" describes the pre-M19/P1 ABI. The producer's hook relies on this; CP1's first step
  proves it with the translated subprocess test (R1).
- **F7. The `kafka/` checkout is 4.4.0-rc4.** Its consumer service passes `--close-timeout`
  (`verifiable_consumer.py:447-448`) and repeats `--session-timeout` 1000 times (`:440`,
  `"--session-timeout %s" % sec*1000`). Python's README says the 4.4 tests have not been run
  (`python/verifiable_clients/README.md:32-35`).
- **F8. The ducker-ak nodes are `eclipse-temurin:17-jdk-jammy`** (Ubuntu 22.04, `kafka/tests/docker/ducker-ak:50`),
  and the image's `apt install` line (`kafka/tests/docker/Dockerfile:63`) installs no ICU. A .NET app that needs
  ICU stops at start when it is missing, so the tools run with invariant globalization (Q4).

---

## 3. Parity anchor (the ceiling)

Anything not in these tables is not built. "Same as Python" means the .NET code does what the Python line does.

### 3.1 Production code

| Python (`python/verifiable_clients/`) | Java | .NET (`dotnet/tools/VerifiableClients/`) |
|---|---|---|
| `_common.py:51` `OUTPUT_LOCK` (re-entrant) | `synchronized (System.out)` | the output's lock, a `Monitor` (re-entrant): same as both |
| `_common.py:54-56` `now_millis` | `System.currentTimeMillis()` | `DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()` |
| `_common.py:59-63` `event` (timestamp, name, then fields) | `@JsonPropertyOrder({"timestamp","name"})` | the event writer: `timestamp`, `name`, then the fields in Java's order |
| `_common.py:66-86` `print_json`, `:89-98` `_discard_stdout` | `printJson` (`VerifiableProducer.java:487-493`) | `JsonOutput.Print` (V18, Q5) |
| `_common.py:101-109` `configure_logging` | log4j to the log file | stderr logger, `VERIFIABLE_CLIENT_LOG_LEVEL` (V19) |
| `_common.py:112-128` `ArgumentParserError`, `ArgumentParser` | argparse4j | internal `ArgumentParserException`, hand-rolled `ArgumentParser` (V5, V6, K2) |
| `_common.py:131-141` `load_props` (ISO 8859-1) | `Utils.loadProps` | `LoadProps` (V17) |
| `_common.py:148-237` `parse_properties` and helpers | `java.util.Properties.load` | `ParseProperties` and helpers (V17) |
| `_common.py:240-247` `_join_surrogates` | (UTF-16 strings) | none: C# strings are UTF-16, as Java's (K9) |
| `_common.py:250-291` `ShutdownHook` | `Runtime.addShutdownHook` | `ShutdownHook` (V13, Q1) |
| `throughput_throttler.py:40-119` | `ThroughputThrottler.java:55-137` | `ThroughputThrottler` (V20) |
| `verifiable_producer.py:53-62` `_class_name`, `_message` | `getClass().toString()`, `getMessage()` | V8, V9 |
| `verifiable_producer.py:67-88` five events | `ProducerEvent` subclasses (`:328-485`) | one method per event (V21) |
| `verifiable_producer.py:91-199` `VerifiableProducer` (`send`, callback, `get_value`, `get_key`, `close`, `run`) | `:95-536` | `VerifiableProducer` (`Send`, `PrintInfoCallback`, `GetValue`, `GetKey`, `Close`, `Run`) |
| `verifiable_producer.py:202-285` `arg_parser`, `parse_args`, `create_from_args` | `argParser` (`:109-200`), `createFromArgs` (`:222-276`) | `ArgParser`, `ParseArgs`, `CreateFromArgs` |
| `verifiable_producer.py:288-352` `main` (`close_and_summarize`, `stop`) | `main` + hook (`:538-574`) | `Main` (V12) |
| `verifiable_consumer.py:67-87` constants (`MIN_STARTUP_VISIBLE_SECONDS`, `_POLL_TIMEOUT`, assignor names, `"classic"`) | `ConsumerConfig.DEFAULT_GROUP_PROTOCOL`, assignor `getName()` | same constants (V15, V22) |
| `verifiable_consumer.py:90-97` `group_protocol_of` | `GroupProtocol.of` | the library's `GroupProtocolMethods.Of` (K1) |
| `verifiable_consumer.py:103-152` eight events and the summaries | `ConsumerEvent` subclasses | one method per event (V21) |
| `verifiable_consumer.py:155-292` `VerifiableConsumer` (`has_message_limit`, `is_finished`, `on_records_received`, `on_complete`, listener, `_commit_sync`, `run`, `close`) | `:110-280` | `VerifiableConsumer : IOffsetCommitCallback, IConsumerRebalanceListener` with the same members |
| `verifiable_consumer.py:295-411` `arg_parser`, `parse_args`, `create_from_args` | `argParser` (`:521-650`), `createFromArgs` (`:655-735`) | `ArgParser`, `ParseArgs`, `CreateFromArgs` |
| `verifiable_consumer.py:414-435` `main` | `main` (`:737-757`) | `Main` |

### 3.2 Events (field order is Java's; the services read the starred fields)

| Event | Fields after `timestamp`, `name` |
|---|---|
| `startup_complete`*, `shutdown_complete`* (both tools), `shutdown_requested` (consumer) | none |
| `producer_send_success`* | `key`, `value`*, `topic`, `partition`, `offset` |
| `producer_send_error`* | `key`, `value`*, `topic`, `exception`, `message` (null when none) |
| `tool_data` | `sent`, `acked`, `target_throughput`, `avg_throughput` |
| `partitions_assigned`*, `partitions_revoked`* | `partitions` `[{topic, partition}]` |
| `records_consumed`* | `count` (the whole poll), `partitions` `[{topic, partition, count, minOffset, maxOffset}]` |
| `record_data` (only with `--verbose`) | `key`, `value`, `topic`, `partition`, `offset` |
| `offsets_committed`* | `offsets` `[{topic, partition, offset}]`, `error` (left out when null), `success` |

### 3.3 Tests

The .NET tests translate Python's 115 cases one for one (names in PascalCase), in
`dotnet/tools/VerifiableClients/VerifiableClients.Tests/`. Python's field-order assertions become exact-string assertions, which
covers the Rust tool's JSON vectors without extra tests. Two Rust-only tests are added, because a .NET `int`
wraps as Java's does and a Python `int` cannot: `get_key_counter_wraps_on_overflow` and
`on_records_received_consumed_count_wraps_on_overflow`. Total: 117.

| Python file (cases) | .NET file | Adaptations |
|---|---|---|
| `test_common.py` (15) | `CommonTests.cs` | `joins_surrogate_pairs` and `keeps_a_lone_surrogate` assert the UTF-16 result, which needs no joining (K9). `shutdown_hook_survives_a_second_signal_during_the_first` tests a Python re-entrancy hazard that .NET's signal thread cannot hit; it becomes "a second SIGTERM while the hook runs is ignored, the hook runs once and the exit code is 0" (subprocess). `ignores_signals_after_the_first` calls the hook's signal method directly. `print_json` tests use an injected writer (Q2). |
| `test_throughput_throttler.py` (12) | `ThroughputThrottlerTests.cs` | none |
| `test_verifiable_producer.py` (38) | `VerifiableProducerTests.cs` | The six tests that read `mock.history()` read a recording producer over `MockProducer` (Q3). `invalid_acks_choice` asserts one text (V5). The negative-timestamp test asserts .NET's text, "…non-negative or null." (K3). Script tests run `dotnet VerifiableProducer.dll` (K7); the two signal tests skip on Windows (K8). |
| `test_verifiable_consumer.py` (49 + 1) | `VerifiableConsumerTests.cs` | `_CommitScript`, `_StateAtClose`, `_WakeupCounter` and `schedule_poll_task` become one delegating consumer over `MockConsumer` (Q3). `_FakeClock` and the startup-floor monkeypatch become the internal clock and sleep parameters (Q2). `AddRecord` takes UTF-8 bytes (a recorded .NET deviation, `MockConsumer.cs:35-39`). The classic-protocol test asserts `UnsupportedOperationException` (K3). |

### 3.4 .NET precedents to mirror

- `dotnet/soak/SoakClient` + `SoakClient.Tests`: an Exe over the public API, `net8.0;net10.0`, internal types with
  `InternalsVisibleTo` keyed to the shared strong name (`SoakClient.csproj:30-62`).
- `dotnet/tests/Performance/PerformanceCommon/PerfSignals.cs:42-43`: `PosixSignalRegistration` for SIGINT and
  SIGTERM with `Cancel = true`.
- `dotnet/tests/Performance/Confluent.Kafka.PerformanceTests/PerfV3Process.cs:43-177`: a test that runs a sibling
  Exe as `dotnet <path>/bin/<config>/<tfm>/<name>.dll`, with `DOTNET_HOST_PATH`.
- `dotnet/Dockerfile.grpc`: a Linux publish with `libconfluent_kafka.so` copied next to the app.

### 3.5 Not adding

- No library change: no `MockProducer.History()`, no `MockConsumer.SchedulePollTask`, no C ABI entry point (Q3).
- No `VerifiableShareConsumer`, no classic group protocol, no transactional message copier, no `--broker-list`.
- No async flavour of the tools, no extra CLI option, event or field.
- No NuGet package for the CLI or JSON (V5, Q5); no package beyond the three test packages the other test projects
  already use (xunit 2.9.3, Microsoft.NET.Test.Sdk 17.14.1, xunit.runner.visualstudio 2.8.2).
- No Native AOT publish, no Docker image, no make target for the Linux publish (a README recipe, as Python's).
- No ducktape job in CI, no `deploy` script for `VerifiableClientApp`.
- No JVM-halt emulation (`Environment.Exit` from the hook).

---

## 4. .NET client API check

All under `dotnet/src/Confluent.Kafka/`.

| Tool needs (Java) | .NET | Cite |
|---|---|---|
| `new KafkaProducer<>(props, ser, ser)` | `KafkaProducer(IReadOnlyDictionary<string, object?>, ISerializer<TKey>, ISerializer<TValue>)` | `Producer/KafkaProducer.cs:86-89` |
| `producer.send(record, callback)` | `KafkaFuture<RecordMetadata> Send(ProducerRecord, ICallback)` | `Producer/KafkaProducer.cs:118`; contract `Producer/IProducer.cs:289-316` |
| `Callback.onCompletion` | `ICallback.OnCompletion(RecordMetadata, Exception?)` | `Producer/ICallback.cs:81` |
| `producer.close()` | `Close()`; a blocked `Send` fails on close | `Producer/KafkaProducer.cs:183`; `Producer/IProducer.cs:349-372` |
| `new ProducerRecord<>(topic, null, createTime, k, v)`, `(topic, k, v)` | both constructors; negative timestamp throws Java's message | `Producer/ProducerRecord.cs:123`, `:170`, `:86` |
| `RecordMetadata.topic/partition/offset` | `Topic()`, `Partition()`, `Offset()` | `Producer/RecordMetadata.cs:87-116` |
| `new KafkaConsumer<>(props, de, de)`; classic refused | constructor; `UnsupportedOperationException` for `classic` | `Consumer/KafkaConsumer.cs:89`; doc `:70-73` |
| `consumer.subscribe(List.of(topic), this)` | `Subscribe(IReadOnlyCollection<string>, IConsumerRebalanceListener)` | `Consumer/KafkaConsumer.cs:118`; `Consumer/IConsumer.cs:125` |
| `ConsumerRebalanceListener` (revoked, assigned; lost by default) | `IConsumerRebalanceListener` (`:79`, `:90`); lost through `ConsumerRebalanceListenerExtensions` unless the listener implements `IConsumerRebalanceListenerWithPartitionsLost` | `Consumer/IConsumerRebalanceListener.cs:70-124` |
| `consumer.poll(Duration.ofMillis(Long.MAX_VALUE))` | `Poll(TimeSpan)`; `TimeSpan.MaxValue` gives 922 337 203 685 477 ms | `Consumer/KafkaConsumer.cs:111`; `Internal/NativeConsumer.cs:1517-1525` |
| `ConsumerRecords.partitions/records(tp)/count` | `Partitions()`, `Records(TopicPartition)`, `Count()` | `Consumer/ConsumerRecords.cs:132`, `:99`, `:152` |
| `consumer.commitSync(offsets)` | `Commit(IReadOnlyDictionary<TopicPartition, OffsetAndMetadata>)` | `Consumer/KafkaConsumer.cs:159`; `Consumer/IConsumer.cs:394` |
| `consumer.commitAsync(offsets, this)` | `CommitAsync(offsets, IOffsetCommitCallback)` | `Consumer/IConsumer.cs:460`; callback `Consumer/IOffsetCommitCallback.cs:56` |
| `consumer.wakeup()` from the hook thread | `Wakeup()`: any thread, a no-op once closed | `Consumer/KafkaConsumer.cs:233`; `Consumer/IConsumerCommon.cs:34-39` |
| `consumer.close(CloseOptions.timeout(Duration))` | `Close(CloseOptions)`, `CloseOptions.Timeout(TimeSpan?)` | `Consumer/KafkaConsumer.cs:218`; `Consumer/CloseOptions.cs:36` |
| `new OffsetAndMetadata(offset)` | `OffsetAndMetadata(long)` | `Consumer/OffsetAndMetadata.cs:82` |
| `GroupProtocol.of(name)` | `GroupProtocolMethods.Of(string)`, Java's message | `Consumer/GroupProtocol.cs:38-64` |
| `WakeupException`, `FencedInstanceIdException` | `Confluent.Kafka.Common.Errors.WakeupException`, `…FencedInstanceIdException` | `Common/Errors/WakeupException.cs:30`, `Common/Errors/FencedInstanceIdException.cs:25` |
| `StringSerializer`, `StringDeserializer` | same names | `Common/Serialization/StringSerializer.cs:32`, `StringDeserializer.cs:31` |
| `KafkaException()` with a null message | empty `Message` | `Common/KafkaException.cs:38-44` |

Mock members Python's tests use:

| Python | .NET | Gap and mode |
|---|---|---|
| `MockProducer(auto_complete=…)` | `MockProducer(ser, ser, autoComplete)` (`Producer/MockProducer.cs:65-68`) | none |
| `error_next(e=…)` | `ErrorNext(Exception?)` (`:179`) | none |
| `set_send_exception(…)` | `SetSendException(Exception?)` (`:260`) | none |
| `closed()` | `Closed()` (`:199`) | none |
| `history()` | `HistoryCount()` only (`:186`) | typed history missing; a library `History()` would be Mode A but re-decides a recorded deviation. Tests use a recording producer (Q3) |
| `MockConsumer(offset_reset_strategy=…)` | `MockConsumer(de, de, autoOffsetReset)` (`Consumer/MockConsumer.cs:70-73`) | none |
| `schedule_poll_task(task=…)` | `ScheduleNopPollTask()` only (`:328`) | missing; Mode B (the C ABI has only the nop form). Tests use a delegating consumer (Q3) |
| `rebalance(new_assignment=…)` | `Rebalance(IReadOnlyCollection<TopicPartition>)` (`:363`) | none |
| `update_beginning_offsets(new_offsets=…)` | `UpdateBeginningOffset(topic, partition, offset)` (`:252`) | per partition; none |
| `add_record(record=…)` | `AddRecord(topic, partition, offset, byte[]?, byte[]?)` (`:296`) | bytes, a recorded deviation; none |
| `set_poll_exception(exception=…)` | `SetPollException(KafkaException?)` (`:305`) | none |
| `wakeup()` then `poll` throws | `Wakeup()` (`:207`) | none |

---

## 5. Design

### 5.1 Location and project shape (V4, K4–K6)

```
dotnet/tools/VerifiableClients/
├─ README.md                                   ← the tools, the tests, the ducktape run (V24)
├─ VerifiableClients/VerifiableClients.csproj  ← class library, internal types, namespace Confluent.Kafka.Tools
│    Common.cs (JsonOutput, ArgumentParser, ArgumentParserException, LoadProps/ParseProperties, ShutdownHook,
│               logging) · ThroughputThrottler.cs · VerifiableProducer.cs · VerifiableConsumer.cs
├─ VerifiableProducer/VerifiableProducer.csproj  ← Exe; Program.Main → VerifiableProducer.Main(args)
├─ VerifiableConsumer/VerifiableConsumer.csproj  ← Exe; Program.Main → VerifiableConsumer.Main(args)
└─ VerifiableClients.Tests/VerifiableClients.Tests.csproj
```

- `net8.0;net10.0`, `IsPackable=false`, `GenerateDocumentationFile=false`, the shared `Directory.Build.props`
  (style, warnings as errors, analyzers, strong name), `InvariantGlobalization=true` on the two Exes (Q4).
  `ProjectReference` to `src/Confluent.Kafka`, which copies the native library to the output.
- All four projects join `Confluent.Kafka.sln`: they need no package the solution lacks, which is the reason soak
  and grpc-server stay out.
- The Apache 2.0 header of the other .NET files ("Copyright 2025 Confluent Inc."), as Python's files carry.
- Java's names in PascalCase (`GetValue`, `HasMessageLimit`, `OnRecordsReceived`, `CommitSync`, `CreateFromArgs`),
  the comment rule (ledger 38) extended to `tools/`: a concise comment for each recorded deviation, no Java or
  Python line numbers.

### 5.2 Sync clients (V1)

The tools use `KafkaProducer` / `KafkaConsumer` and the mocks through `IProducer<string, string>` /
`IConsumer<string, string>`. The listener runs inside `Poll` on the main thread and the delivery callback on the
producer's completion thread, as Java's run on its consumer thread and I/O thread.

### 5.3 CLI (V5, V6)

Hand-rolled, as Rust's (`Milestone-14 PLAN §2.3`); no new dependency. It does what Python's argparse use does:
options with metavar, help, default, `int`/`long` type, `choices` and `required`; a required one-member
mutually-exclusive group (`--bootstrap-server`); `store_true` flags; `--opt=value`; a value that looks like a
negative number (`--acks -1`); the last repeated option wins (F7); no abbreviations (`_common.py:124`);
`-h`/`--help` prints help and exits 0; positional tokens are unrecognized. Errors are checked in argparse's order:
an error met while reading the arguments first, then the required options in declaration order, then the group,
then unrecognized arguments (CPython 3.10's `parse_known_args` and `parse_args`).

**How much text fidelity the harness needs: none.** The services pass well-formed arguments and never read stderr;
only the JSON on stdout and the process's life matter. The texts are for people and for the translated tests.

### 5.4 JSON output (V18, V21, Q5)

`System.Text.Json.Utf8JsonWriter`, in the shared framework of `net8.0` and `net10.0` (no package). Each event is
one method that writes `timestamp`, `name` and Java's fields in Java's order, compact, `null` written as `null`,
`error` left out when null. The output writes the line plus `"\n"` through one UTF-8 (no BOM) writer over
`Console.OpenStandardOutput()`, flushes it, and holds a re-entrant lock across the write. An `IOException` (a
closed pipe) marks the output broken and every later line is dropped, as Python points stdout at the null device.
A value the writer refuses (a non-finite double) prints Java's "Bad data can't be written as json: …" line.
Numbers are written culture-invariant; `avg_throughput` keeps a `.0` on a whole number, as Java's and Python's do.

### 5.5 Properties file and config values (V16, V17)

`LoadProps` reads the file as ISO 8859-1 (`Encoding.Latin1`), and `ParseProperties` is `java.util.Properties.load`:
`=`, `:` and whitespace separators, `#`/`!` comments at the start of a natural line, continuation lines with
their leading whitespace dropped, the escapes, a malformed `\uXXXX` throwing (which ends the tool, as Java's
`IllegalArgumentException` does), the value trimmed, the later line winning. An unreadable file is an
`ArgumentParserException` with the I/O message.

The configuration is built as Python's: `bootstrap.servers`, `acks` (the string), `retries` `"0"`; the consumer's
`group.protocol`, `group.remote.assignor` or `partition.assignment.strategy`, `session.timeout.ms`, `group.id`,
`group.instance.id`, `enable.auto.commit` (a `bool`), `auto.offset.reset`; the file's entries merged. Every value
is a string from the file or a Java-typed value, and `dotnet/CLAUDE.md` §4 **Config** accepts both: "a C# value
of a type Java's `ConfigDef.parseType` accepts for the key's type … or the string Java would parse". The
serializers go to the constructors (two `StringSerializer` / `StringDeserializer` instances each, as Python makes
two); `key.serializer` and its peers are neither checked nor sent.

### 5.6 Shutdown (V12–V15, Q1)

- `ShutdownHook(Action hook, string name)` registers SIGINT and SIGTERM with `PosixSignalRegistration`, keeps both
  registrations alive for the process, and sets `Cancel = true` so the runtime does not end the process. The
  first signal sets an event; later ones are ignored. A dedicated thread named as Java's
  (`verifiable-producer-shutdown-hook`, `verifiable-consumer-shutdown-hook`) waits on the event and runs `hook`
  once; an exception there is logged to stderr with its stack trace. `Finish()`, called by the main thread at its
  end, ignores later signals and waits for a hook that has started.
- **Producer.** `stop` sets the stop flag, wakes the throttler and runs `CloseAndSummarize`; the main thread runs
  `CloseAndSummarize` after `Run`. A lock and a flag make it run once; the second caller waits for the first.
  `CloseAndSummarize` closes the producer (the hook's close releases a send blocked on metadata, F6), prints
  `shutdown_complete`, then `tool_data`.
- **Consumer.** The hook prints `shutdown_requested` and calls `Wakeup()` in a `finally`, so a failed print cannot
  skip the wakeup. `Run` catches `WakeupException` quietly and any other exception with an error log; its
  `finally` closes with `CloseOptions.Timeout(closeTimeoutMs)`, logs a close failure, keeps `startup_complete`
  visible for at least 3 s, then prints `shutdown_complete`.
- **Exit codes.** 0 after a normal end or a signal; 1 after an argument error (usage and `prog: error: msg` on
  stderr) or any other exception escaping `Main` (stack trace on stderr). `Main` catches it, since .NET's
  unhandled-exception path aborts the process (exit 134 on Linux).
- **Windows.** `PosixSignalRegistration` maps only console events there (Ctrl+C to SIGINT, console shutdown to
  SIGTERM); there is no `kill -TERM`. The tools build and run there; ducktape never does. The signal tests skip
  on Windows (K8).

### 5.7 ThroughputThrottler (V20)

A line-for-line translation of `ThroughputThrottler.java:55-137`: `float` for the elapsed seconds (C#'s `float` is
Java's, so Python's `_float32` has no counterpart), `long` nanoseconds from `Stopwatch.GetTimestamp()`, and
Java's `synchronized` / `wait` / `notifyAll` as `lock` / `Monitor.Wait` / `Monitor.PulseAll` on a private lock
object. The timed path clears the wakeup flag after its wait; the zero-target path keeps it set, as Java's.

### 5.8 Logging (V19)

The tools' own lines (the consumer's "Error during processing, terminating consumer process: ", a hook failure,
a close failure) go to stderr in Python's layout, at `WARNING` and above by default,
`VERIFIABLE_CLIENT_LOG_LEVEL` changing it. The core's logs go to stderr through `env_logger`, set by `RUST_LOG`
(`rust/src/ffi/common.rs:97-106`); the README says so.

### 5.9 Deployment to the ducker-ak nodes (Q4)

1. **The Linux native library**: Python's recipe as it stands (`python/verifiable_clients/README.md:41-52`), an
   `ubuntu:22.04` container building `cargo build -p confluent-kafka --release --lib --features ffi` into
   `rust/target-linux/release/libconfluent_kafka.so`. One build serves both bindings.
2. **Self-contained publish on the host**, for the host's architecture (`linux-arm64` or `linux-x64`), into a
   directory under the Kafka checkout's `tests/` (mounted as `/opt/kafka-dev/tests/` on every node):

   ```
   for tool in VerifiableProducer VerifiableConsumer; do
     dotnet publish dotnet/tools/VerifiableClients/$tool -c Release -f net8.0 -r linux-arm64 --self-contained \
       -p:NativeLibraryPath="$PWD/rust/target-linux/release/libconfluent_kafka.so" \
       -p:NativeLibraryFileName=libconfluent_kafka.so \
       -o <kafka>/tests/dotnet-clients
   done
   ```

   The two `-p:` global properties override the library project's host-native copy (`Confluent.Kafka.csproj`,
   the `NativeLibraryPath` property and the `<Content>` item), so the Linux `.so` lands next to the apphosts and no
   ICU, runtime or `LD_LIBRARY_PATH` is needed on the node. If the override does not take (R2), publish without
   it, delete the host library from the output, and copy the `.so` in, as `Dockerfile.grpc` does.
3. **Globals** (`<kafka>/tests/dotnet-globals.json`):

   ```json
   {
     "VerifiableProducer": {
       "class": "kafkatest.services.verifiable_client.VerifiableClientApp",
       "exec_cmd": "/opt/kafka-dev/tests/dotnet-clients/VerifiableProducer",
       "pids": "pgrep -f /opt/kafka-dev/tests/dotnet-clients/VerifiableProducer",
       "kill_signal": 15
     },
     "VerifiableConsumer": {
       "class": "kafkatest.services.verifiable_client.VerifiableClientApp",
       "exec_cmd": "/opt/kafka-dev/tests/dotnet-clients/VerifiableConsumer",
       "pids": "pgrep -f /opt/kafka-dev/tests/dotnet-clients/VerifiableConsumer",
       "kill_signal": 15
     }
   }
   ```

   `exec_cmd` starts with the apphost, not `env`, so `VerifiableClientApp`'s default pattern
   (`pgrep -f '<exec_cmd>'`, `verifiable_client.py:318`) would work too; `pids` is spelled out for symmetry with
   Python's globals. The apphost is the .NET process itself, so the signal reaches the tool.

### 5.10 Tests (V23, K7, K8)

- `VerifiableClients.Tests` references the library project; the subprocess tests run
  `dotnet <repo>/dotnet/tools/VerifiableClients/<Tool>/bin/<Config>/<tfm>/<Tool>.dll` with `DOTNET_HOST_PATH`
  (PerfV3Process's way), against `localhost:1`, with Python's timeouts (60 s per process).
- Signals are sent with the `kill` command (`kill -TERM <pid>`), so the test project needs no P/Invoke. A small
  `[UnixFact]` attribute (a `FactAttribute` subclass setting `Skip` on Windows) marks the signal tests; no
  package.
- `make -C dotnet test-verifiable-clients-dotnet` runs the project alone, for both TFMs. Being in the solution,
  the project also runs in `make -C dotnet test-dotnet`, hence in `make verify-dotnet` (Linux CI) and in
  `make test-dotnet-macos-docker` (macOS CI; its filter, `Makefile:342`, excludes only the integration tests).
  Python's run in `make -C python test` (`python/Makefile:43-51`), so on Linux CI only (its macOS job runs
  `test-unit`). The subprocess and floor tests add about a minute per TFM.

### 5.11 Docs (V24)

`dotnet/tools/VerifiableClients/README.md`, laid out as Python's: what the tools are; running the tests; building
the Linux native and publishing (§5.9); the globals; running one case at a time with `ducktape` on `ducker01` and
`--collect-only`; logging (`VERIFIABLE_CLIENT_LOG_LEVEL`, `RUST_LOG`); and "what cannot pass yet", re-verified for
.NET (F2):

- the classic group protocol (only `group_protocol=consumer` parametrizations; the tool's default `classic` fails
  at construction with `UnsupportedOperationException`);
- SSL: the client reads PEM only and the tests generate JKS stores;
- SASL other than PLAIN (GSSAPI, SCRAM), and SASL_SSL and the delegation-token test, whose JAAS config only a JVM
  reads from `KAFKA_OPTS`;
- the transactional message copier and older Kafka releases, which use Java tools the globals do not replace;
- the `client/pluggable_test.py` case, which passes no `--group-protocol` and so runs `classic`.

The `dotnet/README.md` gets one line pointing at it.

### 5.12 Local ducktape run (Q6)

Lean: at close-out, against Kafka 4.4 (the `kafka/` checkout's 4.4.0-rc4, in a separate clone so Gradle's output
stays out of the reference tree), a named smoke set, each `group_protocol=consumer` and ISOLATED_KRAFT:
`client/consumer_test.py` `test_broker_rolling_bounce`, `test_consumer_bounce` (clean shutdown, `bounce_mode=all`)
and `test_fencing_static_consumer` (one parametrization; it needs the 3 s floor), and the producer-led
`client/compression_test.py` `test_compressed_topic` (VerifiableProducer with Java's ConsoleConsumer). The exact
parametrizations are taken from `ducktape --collect-only` and recorded with the results in the STATUS entry. A
failure the Java tools also show, or that the Python tools show over the same core, is recorded, not fixed here.

---

## 6. Decisions

### Group 1: fact refreshes (one approval)

F1–F8 in §2, plus the scope exception: these two tools and the throttler are in scope for .NET as they were for
Rust (`Milestone-14 PLAN §2.1`) and Python.

### Group 2: Python's way (one table)

| # | Item | Python | Java | .NET (change) |
|---|---|---|---|---|
| V1 | Client flavour | sync `KafkaProducer` / `KafkaConsumer` (`verifiable_producer.py:282`, `verifiable_consumer.py:407`) | sync (`VerifiableProducer.java:273`, `VerifiableConsumer.java:720`) | sync `KafkaProducer` / `KafkaConsumer`: same as both |
| V2 | Shared pieces | written once in `_common.py` (`:15-20`) | each tool its own `printJson` and hook | one `Common.cs`: same as Python |
| V3 | ThroughputThrottler home | beside the tools (`throughput_throttler.py:15-19`) | `server-common` | beside the tools: same as Python (and Rust) |
| V4 | Project shape | two scripts plus shared modules (`README.md:60-69`) | one tools jar, two main classes, picked by `kafka-run-class.sh <class>` | a library plus two thin Exes, `VerifiableProducer` and `VerifiableConsumer` (§5.1): same as Python, and Rust's lib plus two bins. Alternative: one Exe taking the tool name first, as Java's `kafka-run-class.sh` (fewer projects; a leading token in `exec_cmd`) |
| V5 | CLI error texts | argparse's (`test_verifiable_producer.py:282-311`; `test_verifiable_consumer.py:536-556`) | argparse4j's, as the Rust tool reproduces them ("argument --topic is required", "unrecognized arguments: '--bogus'", `verifiable_producer.rs:481-549`) | argparse's texts in their Python 3.10 form (the ducker nodes' Python; 3.12 quotes the choices, `test_verifiable_producer.py:284-287`): same as Python. The harness reads neither (§5.3). Alternative: argparse4j's, same as Java and Rust |
| V6 | No arguments; argument errors | help, exit 0; usage plus `prog: error: msg` on stderr, exit 1 (`verifiable_producer.py:291-302`) | `printHelp`, exit 0; `handleError`, exit 1 (`VerifiableProducer.java:540-571`) | same as both, plus `-h`/`--help`, `--opt=value`, last repeated wins, no abbreviations (§5.3) |
| V7 | Other startup errors | traceback, exit 1 | stack trace, exit 1 | stack trace on stderr, exit 1: same as both (`Main` catches, §5.6) |
| V8 | `exception` field | `"class " + module.qualname` (`verifiable_producer.py:53-56`) | `exception.getClass().toString()` (`VerifiableProducer.java:438`) | `"class " + e.GetType().FullName`: same as Python (the binding's own class name; .NET cannot name Java's class for a .NET exception) |
| V9 | `message` field | `str(e) or None` (`:59-62`) | `getMessage()`, null kept | `null` when `Message` is empty (`KafkaException()` has an empty `Message`, `KafkaException.cs:38-44`): same as Python |
| V10 | `avg_throughput` with zero elapsed | 0.0 (`:327-331`) | Infinity, or NaN with nothing acked (`:562`) | 0.0: same as Python (and Rust) |
| V11 | A record that cannot be built (negative create time) | a `producer_send_error`, not counted (`:133-138`) | thrown outside the `try`, ends the producing thread (`:283-292`) | same as Python (and Rust) |
| V12 | The producer's hook | stop flag, throttler wakeup, close, summary; run once by the hook or the main thread (`:310-352`) | stop flag, close, summary in the hook; the JVM then halts (`:553-565`) | same as Python: the hook closes the producer, so a blocked send is released (F6) |
| V13 | Hook structure | the handler only sets an event; the hook runs once on its own thread; later signals ignored; the main thread waits for it (`_common.py:250-291`) | the JVM runs the hook on its thread at SIGINT/SIGTERM and at exit | same as Python, over `PosixSignalRegistration` (Q1) |
| V14 | Consumer close that throws | logged, `shutdown_complete` still printed (`verifiable_consumer.py:269-276`) | ends `run()` with no `shutdown_complete` (`VerifiableConsumer.java:258-262`) | same as Python (and Rust) |
| V15 | Startup floor | `startup_complete` visible ≥ 3 s (`:79-87`, `:277-279`) | none | 3 s: same as Python (and Rust, `verifiable_consumer.rs:87`) |
| V16 | Serdes | given to the constructors (`verifiable_producer.py:266-267`, `:282-283`) | `key.serializer` config plus the constructor (`VerifiableProducer.java:246-249`, `:272-273`) | constructors only, two instances each: same as Python (the only .NET form, ledger 365) |
| V17 | Properties parser | `java.util.Properties` in full: ISO 8859-1, continuations, escapes, trimmed value (`_common.py:131-237`) | `Utils.loadProps` | same as both (§5.5) |
| V18 | Output | one compact line, flushed per line, re-entrant lock, a broken pipe dropped (`_common.py:48-98`) | `println` under `synchronized (System.out)`; `PrintStream` sets its error flag | same as Python (§5.4) |
| V19 | Logging | stderr, `WARNING`, `VERIFIABLE_CLIENT_LOG_LEVEL` (`_common.py:101-109`) | log4j to the service's log file | same as Python; the core's through `RUST_LOG` (§5.8) |
| V20 | Throttler interrupts | `InterruptedException` paths dropped (`throughput_throttler.py:55-58`) | `catch (InterruptedException)` (`ThroughputThrottler.java:92`, `:118`) | dropped: same as Python (and Rust). .NET has `Thread.Interrupt`, but nothing here interrupts a thread |
| V21 | Event form | one function per event, returning a dict (`verifiable_producer.py:67-88`) | one class per event, serialized by Jackson | one method per event writing its line: same as Python |
| V22 | Poll timeout | `timedelta.max` (`verifiable_consumer.py:74-77`) | `Duration.ofMillis(Long.MAX_VALUE)` (`VerifiableConsumer.java:242`) | `TimeSpan.MaxValue` (≈ 29 000 years): same as Python in intent; `Long.MAX_VALUE` ms does not fit a `TimeSpan`, as it does not fit Python's `timedelta` either |
| V23 | Tests and wiring | 115 cases, their own target inside `make -C python test` (`python/Makefile:43-51`, `:97-104`) | none (ducktape only) | the 115 translated plus two Rust overflow tests (§3.3); `test-verifiable-clients-dotnet`; in the solution, so inside `test-dotnet` (§5.10) |
| V24 | Docs | README beside the tools (`README.md:1-128`) | none | README beside the tools, ".NET" list re-verified (§5.11) |
| V25 | Deprecated `--producer.config` / `--consumer.config` | notice on stdout, file still loaded (`verifiable_producer.py:268-271`) | same (`VerifiableProducer.java:257-259`) | same as both |

### Group 3: one at a time

**Q1. The signal mechanism.** Python: `signal.signal(SIGINT/SIGTERM, handler)`, the handler only setting an event
(`_common.py:263-277`). Java: `Runtime.addShutdownHook`, run by the JVM on SIGINT, SIGTERM and SIGHUP and at a
normal exit, then the JVM halts (`VerifiableProducer.java:553`, `VerifiableConsumer.java:747`). .NET has no direct
equivalent of either.

- **A (lean).** `PosixSignalRegistration` for SIGINT and SIGTERM, `Cancel = true`, the registrations held for the
  process (§5.6). This is Python's design: a flag-only handler, a hook thread, the main thread waiting, exit 0.
  Precedent: `PerfSignals.cs:42-43`.
- **B.** `Console.CancelKeyPress` plus `AppDomain.ProcessExit` (`SoakSignals.cs:33-48`). On SIGTERM the runtime
  runs `ProcessExit` and then ends the process whatever the main thread is doing, which is Java's halt; the hook
  has to finish inside the handler, and the consumer's main thread could be cut off before `shutdown_complete`.
- **C.** A for the signals, plus `ProcessExit` running the hook at a normal exit, as the JVM does. Python's
  explicit "the main thread runs it at its end" already covers that; C adds a second path for no gain.

**Q2. Test seams.** Python patches module globals: `monkeypatch` replaces `print_json`, `time` and
`MIN_STARTUP_VISIBLE_SECONDS`, and `capsys` captures stdout (`test_verifiable_consumer.py:51-53`, `:328-339`,
`:363-399`). Java's `VerifiableConsumer` takes the `PrintStream` it prints to (`VerifiableConsumer.java:110-118`);
its producer prints to `System.out`. C# cannot patch a method, and `Console.SetOut` is process-wide while xUnit
runs test classes in parallel.

- **A (lean).** Both tools take the output (the `JsonOutput` over a `TextWriter`), as Java's consumer takes its
  `PrintStream`; `Main` passes stdout. The consumer has an internal constructor taking the floor, a monotonic clock
  and a sleep function, the default ones used by `CreateFromArgs`. Same as Java for the consumer's output; a
  deviation from both for the producer's (it prints to stdout in Java and Python), for testability.
- **B, Python's way.** Global stdout, captured with `Console.SetOut` in tests that run in one non-parallel xUnit
  collection; the floor a static field the tests set to zero. Works, but serializes those tests and leaves a
  mutable static in the tool.

**Q3. The two mock gaps (F4).** Python's tests read `MockProducer.history()` and schedule poll tasks; Python's
mocks are pure-Python translations of Java's, which have both (`MockProducer.history()`,
`MockConsumer.schedulePollTask(Runnable)`). .NET's mocks are over the native mocks and lack both
(`MockProducer.cs:182-186`, `MockConsumer.cs:326-328`).

- **A (lean).** Test-only decorators in the test project: a recording `IProducer<string, string>` that keeps each
  `ProducerRecord` and forwards to a `MockProducer`, and a delegating `IConsumer<string, string>` over a
  `MockConsumer` with hooks on `Poll` (a queue of tasks run first, Java's `schedulePollTask` semantics),
  `Commit` (scripted errors, Python's `_CommitScript`), `Close` (state at close) and `Wakeup` (a counter). No
  library change.
- **B.** Add `MockProducer.History()` (Mode A, re-deciding the recorded `HistoryCount()` deviation) and
  `MockConsumer.SchedulePollTask(Action)` (Mode B: a new C ABI entry point, a Rust-core task first). Library
  parity with Python and Java, but new public surface justified only by these tests; better as its own phase.
- **C.** A now; B listed as a follow-up for the mock-parity backlog.

**Q4. Deployment.** Python: the interpreter is on the node (Python 3.10), so its build artifacts are copied into
`tests/python-clients/` and run with `LD_LIBRARY_PATH` (`README.md:37-91`). Java: the tools ship in the Kafka build
the nodes run. No .NET runtime is on the nodes, so there is no Python's way to copy.

- **A (lean).** Self-contained publish for the host's `linux-<arch>`, with the `ubuntu:22.04` `.so` from Python's
  recipe and `InvariantGlobalization=true` (§5.9). Nothing installed on the nodes; about 70 MB per architecture.
- **B.** Framework-dependent publish, the .NET 8 runtime installed on every node (a `deploy` script running
  `apt-get install dotnet-runtime-8.0`, or a runtime unpacked into the mounted directory), `exec_cmd`
  `dotnet /opt/kafka-dev/tests/dotnet-clients/VerifiableProducer.dll`. Smaller, but it changes the nodes or needs
  network at deploy.
- **C.** Native AOT. Not possible yet: the library is not marked AOT-compatible on master (the work is on the
  unmerged `prashah_dev_dotnet_aot_reflection`), warnings are errors, and AOT for Linux has to be compiled on
  Linux. Revisit once that branch lands.

**Q5. The JSON writer.** Python: `json.dumps(…, separators=(",", ":"), allow_nan=False)`, which escapes non-ASCII
(`_common.py:78`). Java: Jackson, which writes non-ASCII as UTF-8 and a NaN as `"NaN"`. The services decode both.

- **A (lean).** `Utf8JsonWriter` with `JavaScriptEncoder.UnsafeRelaxedJsonEscaping`: non-ASCII written as UTF-8
  (same as Java; a character outside the BMP is written as a `\uXXXX` pair, as Python writes it), `"+<>&'` unescaped (same as both), and a non-finite double refused into the "Bad data" line
  (same as Python).
- **B.** `Utf8JsonWriter` with its default encoder: non-ASCII escaped (same as Python), but also `+`, `<`, `>`,
  `&` and `'` as `\uXXXX` (as neither).
- **C.** A hand-written writer reproducing `json.dumps` exactly. More code for no reader.

**Q6. A local ducktape run in the DoD, and which Kafka.** Python ran Kafka 4.3.1's system tests
(`README.md:32-35`); Java's tools are what the tests are written for.

- **A (lean).** Yes, at close-out, the smoke set of §5.12 against 4.4 (the `kafka/` checkout's version, the
  binding's version line, and the only release whose consumer service passes `--close-timeout`, which no
  non-Java tool has run).
- **B.** The same set against 4.3.1, comparable with Python's run.
- **C.** Both.
- **D.** No run in the DoD; the README recipe only, the run left to whoever needs it.

**Q7. Phasing.** Rust split the tools into two phases (`Milestone-14 PLAN §6-7`); Python landed them in one PR
(#276).

- **A (lean).** One phase, M24/P1, N=130, two checkpoints, each followed by Critic 130: CP1 the shared pieces, the
  throttler, the producer and its tests; CP2 the consumer, its tests, the README and the Q6 run.
- **B.** Two phases, M24/P1 (N=130) and M24/P2 (N=131).
- **C.** One phase with no checkpoint, as Python's one PR.

### Group 4: keep .NET (one approval)

- **K1.** `GroupProtocolMethods.Of` from the library (`GroupProtocol.cs:52-64`), Java's `GroupProtocol.of` with
  Java's message, instead of Python's own copy (`verifiable_consumer.py:90-97`).
- **K2.** `ArgumentParserException`, argparse4j's name (Python's `ArgumentParserError`); internal.
- **K3.** The library's own texts in assertions: `ProducerRecord`'s "…non-negative or null." (`ProducerRecord.cs:86`,
  Python says "None"), and `UnsupportedOperationException` for `classic` (`KafkaConsumer.cs:70-73`).
- **K4.** The tools' types are internal, in `Confluent.Kafka.Tools` (Java's `org.apache.kafka.tools` mapped as §2
  maps packages), the throttler beside them as Python and Rust keep it; `PrintInfoCallback` is a nested class, as
  Java's, since an `ICallback` needs a class (the library has no delegate factory for it).
- **K5.** `net8.0;net10.0`, strong-named and analyzed through `Directory.Build.props`, the .NET files' license
  header, as `SoakClient`.
- **K6.** In `Confluent.Kafka.sln` (soak and grpc-server stay out only for their extra packages).
- **K7.** A separate test project, as `SoakClient.Tests`; subprocess tests run the sibling Exe's `.dll` from its
  `bin/`, as `PerfV3Process`.
- **K8.** Signals sent with the `kill` command; the signal tests skip on Windows through a `[UnixFact]` attribute.
- **K9.** C# strings are UTF-16, as Java's: no `_join_surrogates`; a lone surrogate stays as in Java.
- **K10.** C# `int` wraps as Java's: `keyCounter` and `consumedMessages` overflow as in Java (the two Rust tests).

---

## 7. Phase: checkpoints, gates, close-out

**CP1 — shared pieces, throttler, producer (dotnet-actor 130, then dotnet-critic 130).**
1. S1, the risk spike, committed with its tests: the four projects and the solution wiring, a minimal producer
   `Main`, and the three subprocess tests that retire R1–R3: SIGTERM releases a blocked send (Python's
   `test_script_sigterm_releases_a_blocked_send`), exit 0 on SIGTERM while throttled, and the Linux publish of §5.9
   started once (`ldd`-free: the apphost runs `--help` in an `ubuntu:22.04` container).
2. S2 `Common.cs` and `CommonTests` (15).
3. S3 `ThroughputThrottler` and its tests (12).
4. S4 `VerifiableProducer` complete, its tests (38 + 1), `test-verifiable-clients-dotnet`.

**CP2 — consumer, docs, the run (dotnet-actor 130, then dotnet-critic 130).**
1. S5 `VerifiableConsumer` and its tests (49 + 1 + 1).
2. S6 README and the `dotnet/README.md` line.
3. S7 the Q6 run, results in the STATUS entry.
4. S8 the comment-only commit for the phase's own files (comment rule 8).

**Gates per commit** (no Rust or C change, so Pranav's rule applies: build, format and tests, then `--no-verify`):
`dotnet build` of the solution with 0 warnings; `dotnet format Confluent.Kafka.sln --verify-no-changes`;
`make -C dotnet test-verifiable-clients-dotnet` on net8.0 and net10.0. **Close gate:** `make test-dotnet-macos-docker`
locally (the full unit suite, the new project included, both TFMs) and Linux CI's `make verify-dotnet` after the
push. DoD §10 (hot path) and §11 (consumer trait surface): N/A, stated in the self-review; the tools add no library
surface.

**Close-out (Manager).** STATUS entry (commits, gates with counts and commands, Critic summary, the ducktape
results, follow-ups); `COMMENTS.DONE.130.md` archived beside this plan; `COMMENTS.130.md` reset; the rulings as
ledger rows from 551; the plan committed with the phase.

---

## 8. Risks and open questions

- **R1.** The hook's close must release a send blocked on metadata (F6). If it does not, the producer's SIGTERM
  waits up to `max.block.ms`; the fallback is the hook not closing (Java's JVM halt has no .NET twin without
  `Environment.Exit`). S1 settles it before any other work.
- **R2.** The `-p:NativeLibraryPath` override of §5.9 relies on global properties flowing to the referenced
  library project. Fallback: the `Dockerfile.grpc` copy-in.
- **R3.** ICU and other node libraries: `InvariantGlobalization=true` removes ICU; the self-contained runtime needs
  only what `ubuntu:22.04` ships (libstdc++, zlib). The library makes no culture-dependent call (no
  `GetCultureInfo`, `new CultureInfo` or `CurrentCulture` under `src/Confluent.Kafka`).
- **R4.** The Linux `.so` must carry the codecs `compression_test.py` uses (gzip, snappy, lz4, zstd); #293 made the
  codecs optional features. Check the default feature set of Python's build command before the run.
- **R5.** Signal tests in CI on Linux and macOS (Python's run on Linux only). Generous timeouts; a flake is
  recorded, not retried silently.
- **R6.** The analyzers (`CKRDA*`, through `Directory.Build.props`) may report on the tools' Java-named members;
  `.editorconfig` may need a `tools/` section, decided in S1.
- **R7.** The 4.4 system tests have never run with a non-Java client; a failure may be the core's. Q6 records such
  failures; it does not fix them here.
- **O1.** Approve the scope exception (group 1).
- **O2.** Q6: is a ducktape run part of the DoD, and against which Kafka?
- **O3.** Q7: one phase with checkpoints (N=130 only), or two phases (N=131 reserved too)?
- **O4.** V5: Python's argparse texts, or argparse4j's as the Rust tool?

---

## 9. Rulings

Pranav, 2026-11-16, relayed by the coordinator: "I agree on all. F1-F8 agreed. V1-V25 agreed. Q1-Q7 agreed with
PM's lean." Ledger rows 551–561, Agreed.

- **Group 1:** F1–F8 agreed, and the tools-module scope exception (O1).
- **Group 2:** V1–V25 agreed as written; V5 is argparse's error texts in their Python 3.10 form (O4).
- **Group 3:** every item takes A: Q1 `PosixSignalRegistration`; Q2 the output injected, plus internal clock and
  sleep parameters; Q3 test-only wrappers around the mocks; Q4 self-contained `linux-<arch>` publish; Q5
  `Utf8JsonWriter` with relaxed escaping; Q6 the four-case smoke run against Kafka 4.4 at close-out (O2); Q7 one
  phase, two checkpoints, N=130 only (O3).
- **Group 4:** K1–K10 agreed.

Pranav, 2026-11-17, relayed by the coordinator, on the three questions raised during the phase; each takes its
recommendation (a). Ledger rows 562–564, Agreed.

- **The README publish recipe (S6):** `dotnet publish … --artifacts-path <dir>` with the two `-p:` overrides of
  §5.9, so the repo's `bin/` and `obj/` stay untouched.
- **The error-log tests (S5):** the two tests run in their own collection, which runs alone and swaps
  `Console.Error`, as `ChaosRecordOutcomesTests`' `ConsoleCollection` does; no code change.
- **The quiet wakeup (S5):** Python's way. A `Log.Debug` helper in `Common.cs`, and "Caught WakeupException because
  consumer is shutdown, ignore and terminate." logged at DEBUG where the consumer catches the wakeup (§5.6, §5.8).
