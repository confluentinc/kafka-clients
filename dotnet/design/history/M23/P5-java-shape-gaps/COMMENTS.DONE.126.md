# COMMENTS.DONE.126 — resolved findings (Critic 126, M23/P5)

## 126-S2-1 — `AsyncAdmin.Create`'s remark states a name clash that does not exist

- **Severity:** Low (public doc gives a wrong rule; no behaviour change)
- **Commit:** `f4de62b60` (S2)
- **Where:** `dotnet/src/Confluent.Kafka/Admin/IAsyncAdmin.cs:1398-1401`
- **Problem:** The remark on `AsyncAdmin.Create` says "From code in a `Confluent.Kafka.*` namespace, write
  `IAsyncAdmin.Create` (C# 14) or the full name `Confluent.Kafka.Admin.AsyncAdmin.Create`." That copies the `Admin`
  remark, but the clash D15 rules on is only for the name `Admin`, which is also the namespace
  `Confluent.Kafka.Admin`. There is no namespace `AsyncAdmin`, so `AsyncAdmin.Create(…)` works from any namespace once
  `using Confluent.Kafka.Admin;` is there. The remark tells a pre-C# 14 user in a `Confluent.Kafka.*` namespace that
  the short form does not work, which is false. Plan §3.3 / §4 S2 ask for this remark only on `Admin.Create`.
- **Evidence:** a scratch library compiled against `dotnet/src/Confluent.Kafka/bin/Release/netstandard2.0/Confluent.Kafka.dll`
  (SDK 10.0.400, `using Confluent.Kafka.Admin;`, `namespace Confluent.Kafka.Scratch;`):
  `AsyncAdmin.Create(c)` gives `0 Error(s)`; `Admin.Create(c)` in the same file gives
  `error CS0234: The type or namespace name 'Create' does not exist in the namespace 'Confluent.Kafka.Admin'`.
  The commit message says the same thing: only `Admin.Create` names the namespace (CS0234).
- **Expected fix:** remove the `<remarks>` from `AsyncAdmin.Create`, or say that `AsyncAdmin.Create` works from every
  namespace and only `Admin` clashes with the namespace. Leave the `Admin.Create` remark as it is.
- **Resolved in 5bcc82c2b:** removed the `<remarks>` from `AsyncAdmin.Create`; `Admin.Create`'s remark is unchanged.

## 126-S7-1 — Low (test-coverage) — the "no entry" test checks nothing in about 40% of runs

- **File:** `dotnet/tests/Confluent.Kafka.UnitTests/PublicConsumerRecordsTests.cs:237-269`
  (`AFailingDeserializer_GivesNoEntry_ToAPartitionWithNoRecordReturned`); the code it is the only pin for is
  `dotnet/src/Confluent.Kafka/Internal/Interop/ConsumerRecordsMarshal.cs:152` (`if (records.Count > 0)` in
  `AddPartition`).
- **Problem:** the test's main assertions run only when the core reads `records-a-0` before `records-a-1`. When it
  reads `records-a-1` first, the poll throws, and the test checks only `error.TopicPartition()` and returns at `:263`.
  The core's order is random for each `MockConsumer`: the mock keeps its records in a `std::collections::HashMap`
  (`rust/src/consumer/mock_consumer.rs:31`), and each poll goes through `self.records.keys()` (`:784`, "Test ordering
  does not depend on this"). Each `HashMap` gets its own `RandomState`. `AddPartition`'s `Count > 0` matters only on
  this path (earlier partitions returned records, and the next one fails on its first record). The mock never gives a
  partition with no records on the success path (`results.entry(tp).or_default().push(rec)`), so no other test pins
  it. If the check is removed, the test fails only when the branch with the assertions runs.
- **Evidence:** a scratch program (outside the repo) linked against the built `Confluent.Kafka.dll` ran this test's
  setup with 40 new `MockConsumer`s in one process: `threw=16 returned=24`. In 16 of 40 runs the test skipped every
  assertion about the partitions.
- **Expected fix:** make it deterministic. Either repeat the setup with a new `MockConsumer` until the poll returns
  records, up to a bound (for example 64 tries), and fail if it never does, then assert as now. Or (with S8's
  test-side import of `kafka_consumer_ConsumerRecords_with_next_offsets`) build the native batch in a fixed
  partition order and call `ConsumerRecordsMarshal.CopyOut` on it directly.
- **Resolved in 7001759e8:** the test now tries new `MockConsumer`s, at most 50, until the core reads `records-a-0` before the failing `records-a-1`, then asserts as before; it fails with `Assert.Fail` if no consumer does. A mutation check (`Count > 0` changed to `Count >= 0` in `AddPartition`) makes it fail.

## Decisions and deviations made during execution (Manager 126)

- **D10, a null partition.** `TopicPartition` is a `readonly struct`, so the plan's "`Records(null)` for a partition
  returns an empty list" cannot occur: `Records(null)` binds to `Records(string)` and throws Java's message.
  `Records(default(TopicPartition))` and a partition with a null topic return the empty list (S7).
- **D15, the name clash.** The in-namespace `Admin.UpgradeType.*` / `Admin.OpType.*` lines name the type in full,
  `Confluent.Kafka.Admin.UpgradeType.…`, as `dotnet/CLAUDE.md` §2 names `TimeoutException`; the idiom map's
  example `Admin.UpgradeType.UPGRADE` stays true as written. Only `Admin.Create` carries the "use `IAdmin.Create`"
  remark: no namespace is named `AsyncAdmin` (126-S2-1).
- **S2.** `Create` checks `conf` for null itself, so `ParamName` is `conf`. Its doc says `bootstrap.servers` or
  `bootstrap.controllers` is required. `DeleteRecordsResult` / `AsyncDeleteRecordsResult` take Java's `futures`.
- **S5.** `dotnet/README.md` loses its sentence "The value comes before the key …", false after the swap.
  `Ctor_NegativePartition` used a form Java lacks; `testInvalidRecords` covers its check.
- **S7.** `Records(topic)` follows the `Partitions()` order (the poll's), as Java's map iteration does, not the
  partition number. The core mock's partition order is random per instance; the tests read the order the poll
  gives (126-S7-1).
- **S8.** The marshal keeps its `count <= 0` branch: it returns `EMPTY`, or the next offsets with no records,
  without reading partitions (D13 holds). One P/Invoke (`kafka_consumer_ConsumerRecords_next_offsets_err`) and
  one map per poll.
- **S9.** A listener whose `OnPartitionsLost` did anything but call Revoked took the `…WithPartitionsLost`
  interface (the three gRPC listeners, `DelegateListener`, the four explicit no-op implementations); one that only
  repeated Revoked dropped it. SerdeGuard's helpers moved to `Internal/DefaultMethodGuard.cs`; the serde messages
  are unchanged.
- **Java tests skipped, with the reason in the file:** `OffsetAndMetadataTest`'s three `Serializable` tests;
  `ConsumerRecordsTest.testNextOffsetsLogsErrorPeriodicallyWhenConstructedWithDeprecatedConstructor` (the deprecated
  constructor is not translated).
- **Flakes seen in the gates (all passed alone or on a full rerun; none in code this phase changed):** the macOS
  60 s teardown stall (`PublicSyncConsumerTimedOverloadTests`, `PublicConsumerCheckOrderTests`,
  `PublicAsyncConsumerOverlappingSubscribeTests`, `SafeProducerHandleTests.CreateAndDisposeMany`,
  `PublicProducerSerializerFailureTests` on teardown) and `ProducerSubmitHandleRefTests`' 2 MB memory budget.
