# Critic 113: resolved findings

## 1. Medium: `NoOffsetForPartitionException` from the core always gets the plural message, so the mock consumer's message is not Java's

- **Where:** `dotnet/src/Confluent.Kafka/Internal/Interop/ErrorPayloadMarshal.cs`,
  `ReadNoOffsetForPartitionException`:

  ```csharp
  NoOffsetForPartitionException built = new NoOffsetForPartitionException(partitions);
  return new NoOffsetForPartitionException(default(CoreError), built.Message, innerException, built.Partitions);
  ```

  The core's message (`message`) is dropped and replaced by the `(Collection)` constructor's text.
  The test `TheMockConsumer_RaisesNoOffsetForPartition_WithItsPartition` pins the replacement.
- **Reference:** Java has two constructors with two messages:
  - `NoOffsetForPartitionException(TopicPartition)` gives
    `"Undefined offset with no reset policy for partition: " + partition`.
  - `NoOffsetForPartitionException(Collection)` gives
    `"Undefined offset with no reset policy for partitions: " + partitions`.

  Java uses both. `MockConsumer#resetOffsetPosition` and `OffsetFetcherUtils` throw the singular
  form. `SubscriptionState#resetInitializingPositions` throws the collection form. The core keeps
  that split: `ConsumerNoOffsetForPartitionError::new(tp)` writes the singular message and is called
  from `mock_consumer.rs` and `offset_fetcher_utils.rs`, while `with_partitions` writes the plural one
  and is called from `subscription_state.rs`. So the message the core reports is already Java's for
  whichever constructor Java used at that site.
- **Failure scenario:** on the .NET `MockConsumer` (core-backed) with `auto.offset.reset=none`,
  `Poll` raises an error whose message is
  `"Undefined offset with no reset policy for partitions: [t-0]"`. Java's `MockConsumer` says
  `"Undefined offset with no reset policy for partition: t-0"`. The same happens on the real
  consumer's `OffsetFetcherUtils` path. With several partitions, the rebuilt message also follows
  the C list's order, while the core's own message sorts them.
- **Why the plan led here:** D7 assumes the core only builds this error through the `(Collection)`
  constructor ("builds its message from the partitions, as Java's `(Collection<TopicPartition>)`
  constructor does"). The core shows otherwise. Python's `_CORE_KWARGS["NoOffsetForPartitionError"]`
  makes the same rebuild. Python's mock is pure Python, so only its real-client path shows the
  difference there.
- **Fix:** keep the core message, as every other reader except `QuotaViolation` does:
  `new NoOffsetForPartitionException(default(CoreError), message, innerException, new HashSet<TopicPartition>(partitions))`.
  Change the mock-consumer test to expect Java's singular text,
  `"Undefined offset with no reset policy for partition: " + NonAsciiTopic + "-0"`. In the seam test,
  assert that the core message passes through (`"core text"`), and keep the check that
  `Partitions` is a set. Update D7's sentence about `NoOffsetForPartitionException` to match.
  The QuotaViolation rebuild is unaffected: Java's `getMessage()` is null there, and the core message
  is not Java's text.

- **Resolution:** `ReadNoOffsetForPartitionException` now keeps the core's message and builds
  `Partitions` as a `HashSet`. `TheMockConsumer_RaisesNoOffsetForPartition_WithItsPartition` expects
  Java's singular text. The seam test is renamed
  `NoOffsetForPartition_ReadsItsPartitionsAsASet_AndKeepsTheCoreMessage`. It asserts that
  `"core text"` passes through, that `Partitions` is an `ISet`, and that a duplicate partition in
  the C list is folded. D7 and the Phase 4 table in `PLAN-error-hierarchy-parity.md` now say that a
  message is rebuilt only where the core's cannot be Java's (`QuotaViolationException`). The rule
  suggestion for `dotnet/CLAUDE.md` is left to the owner, as instructed.

## 2. Low: `TopicAuthorization_KeepsAGivenMessage_AndAnOwnedErrorIsFreedByTheRead` cannot fail on its second claim

- **Where:** `dotnet/tests/Confluent.Kafka.UnitTests/ErrorPayloadReaderTests.cs`. The test calls
  `FromFfiError(error, owned: true)` and asserts only the message and the topics.
- **Reference:** the plan's Phase 4 test bullet asks that each test check "that the error handle is
  freed exactly once".
- **Failure scenario:** if `FromFfiError` stopped destroying an owned error, the test would still
  pass, because nothing observes the leak. A double free would crash, but the name promises that the
  error is freed, and the test cannot show that. The other tests check what can be observed: they
  read twice through `owned: false`, then destroy once, so a destroyed view or a double free aborts
  the run.
- **Fix:** rename the test so it claims only what it checks (for example
  `TopicAuthorization_KeepsAGivenMessage_ThroughAnOwnedRead`). The "freed exactly once" property is
  shown by the read-twice-then-destroy pattern, not by this test.

- **Resolution:** renamed to `TopicAuthorization_KeepsAGivenMessage_ThroughAnOwnedRead`. No cheap
  observation of the free exists. The read-twice-then-destroy tests already cover it.
