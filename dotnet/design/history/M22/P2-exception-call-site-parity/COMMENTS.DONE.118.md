# Critic 118 (resolved) — call-site exception parity (`PLAN.md`)

Scope: `git diff -- dotnet` (on top of `84f60f1a1`) plus the untracked
`src/Confluent.Kafka/Internal/NullConfigValues.cs`,
`tests/Confluent.Kafka.UnitTests/NullConfigValuesTests.cs` and
`tests/Confluent.Kafka.UnitTests/PublicConsumerPartitionArgumentTests.cs`. Reference: the C ABI
header (`rust/target/include/confluent_kafka_bindings.h`, "BH") and Apache Kafka 4.4 in `kafka/`.

## 1. F9: a nested serializer-failure callback clears the "inside a callback" flag, so a later `Flush`/`Close` in the outer callback waits for itself (Low)

`Internal/FutureFailure.cs` `PendingCallbacks.Call` (lines 94-105) sets `_inside.Value = true`
and, in its `finally`, sets it to `false` instead of restoring the previous value. `Call` is
synchronous, so the `AsyncLocal` write in its `finally` changes the caller's execution context.

Sequence:

1. Async failure callback A has awaited once (its `ended` task is now in `_running`).
2. A sends a second record whose serializer throws an `ApiException`, with an async callback.
3. The nested `Call` sets `_inside` to `false` in A's context.
4. A then awaits `producer.Flush()` (or calls `Close`/`Dispose`). `Snapshot()` sees
   `_inside == false`, includes A's own `ended` task, and A waits for itself forever.

Java runs this callback synchronously inside `doSend`, so a `flush()` there never waits for
itself. The class's own doc says "A wait made from inside one of them does not wait for them, or it
would wait for itself."

Fix: save the previous value and restore it:

```csharp
bool outer = _inside.Value;
_inside.Value = true;
try { … } finally { _inside.Value = outer; }
```

Add a test: a callback that yields, sends a failing record, then awaits `Flush` and completes
within the deadline.

## 2. The mock `Seek(tp, negative offset)` makes a check that Java's `MockConsumer` does not (Low; pre-existing, in a branch this diff rewrote)

`Internal/NativeConsumer.cs` `CheckSeekArguments(string, int, long)` (lines 989-1002) runs
`RequireSeekOffset(offset)` on the mock branch, after `RequireAssignedTopic`. Java's
`MockConsumer.seek(TopicPartition, long)` is `ensureNotClosed(); subscriptions.seek(partition,
offset);`. `SubscriptionState.seek` builds `new FetchPosition(offset)`, which makes no check, so:

- on an **assigned** partition, Java's mock accepts a negative offset; .NET throws
  `IllegalArgumentException("seek offset must not be a negative number")`;
- on an **unassigned** partition, Java's mock throws `IllegalStateException("No current assignment
  for partition t-0")`; .NET throws the `IllegalArgumentException`.

The core mock follows Java (`rust/src/consumer/mock_consumer.rs` `seek_with_offset` has no offset
check), so dropping the binding's check on the mock gives Java's behaviour. The new comment above
the method ("Java's MockConsumer checks nothing before ensureNotClosed()") is accurate, but the
mock checks nothing after it either. The tests that pin the current behaviour
(`PublicConsumerSeekLagTests:265-276`, `PublicConsumerApiTests:115-126` on `AsyncMockConsumer` with
an unassigned `t-0`) would need to expect Java's `IllegalStateException`. If this is out of the
plan's scope, record it in the "Call-site checks that differ from Java" section instead.

## 3. `dotnet/CLAUDE.md` "Negative partitions" misstates `position` and the mock's `currentLag` (Low, docs)

`dotnet/CLAUDE.md:697-699` says that for `seek`, `position`, `pause`, `resume` and `currentLag` the
core answers `IllegalStateException` "No current assignment for partition t--1". The code and
`PublicConsumerPartitionArgumentTests` follow Java instead, and differ from that text in two places:

- `position` throws "You can only check the position for partitions assigned to this consumer."
  (`AsyncKafkaConsumer.position` checks `isAssigned` first), as `IllegalArgumentException` on the
  mocks (`MockConsumer.position`).
- The mocks' `currentLag` answers `0` (`MockConsumer.currentLag`'s "assume caught up" branch).

The plan's F11 row carries the same wording. Correct the rulebook text so a later review does not
"fix" the code back to it.

## 4. F12: no test for the changed lookup behaviour with a negative partition (Low, missing test)

The deleted `ConsumerNegativePartitionGuardTests` included
`SnapshotTimestamps_OffsetsForTimes_RejectsANegativePartition_BeforeAnyNativeCall`. After this
change, `offsetsForTimes`, `beginningOffsets` and `endOffsets` pass a negative partition to the
core, but no test exercises them with one. `PublicConsumerPartitionArgumentTests` covers `assign`,
`commit` and `committed` (mocks) and `assign` (`KafkaConsumer`); `NegativePartitionCalls` covers
the owner-check order only. The plan's F12 is "As F11", which includes "Test that the class and
message are Java's".

Add a mock case so the core's answer is pinned. Java's `MockConsumer.beginningOffsets` /
`endOffsets` throw `IllegalStateException` "The partition t--1 does not have a beginning offset." /
"… does not have an end offset.", and its `offsetsForTimes` throws `UnsupportedOperationException`
"Not implemented yet.". Assert whatever the core gives, and record any difference.

## Resolution

1. Fixed: `PendingCallbacks.Call` saves and restores the previous flag. Test
   `PublicProducerSerializerFailureTests.ApiException_ANestedFailingSendInsideAnAsyncCallback_LetsItsFlushComplete`
   (an async callback yields, sends a failing record, then awaits `Flush`). With the old reset the test hangs.
2. Fixed: the mock branch of `CheckSeekArguments(string, int, long)` makes no offset check; the core mock answers.
   `Seek_NegativeOffset_OnTheMock_UnassignedPartition_ThrowsIllegalState` (SeekLag and Api tests:
   `IllegalStateException` "No current assignment for partition t-0"),
   `Seek_NegativeOffset_OnTheMock_AssignedPartition_IsAccepted`, and the interop
   `Seek_NegativeOffset_OnTheMock_ThrowsTheUnassignedPartitionsIllegalState`. The real consumers and
   `ConsumerHandle` keep the negative-offset check.
3. Fixed: the `dotnet/CLAUDE.md` "Negative partitions" bullet and the plan's F11 row now give `position`'s
   message and classes and the mocks' `currentLag` of 0.
4. Fixed: `PublicConsumerPartitionArgumentTests.OnTheMocks_TheOffsetLookups_AnswerANegativePartition_AsJavasMock`.
   The core gives Java's mock answers on both mocks: `IllegalStateException` "The partition t--1 does not have a
   beginning offset." / "... an end offset.", `UnsupportedOperationException` "Not implemented yet.". No difference
   to record; the CLAUDE.md bullet lists them.

Rule suggestion taken: the `AsyncLocal` restore rule is in `ffi-marshalling.md` §A7's in-callback bullet.
