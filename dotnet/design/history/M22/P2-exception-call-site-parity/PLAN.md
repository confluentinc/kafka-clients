# .NET exception call-site parity

**Status:** APPROVED. Kind C (F3, F6, F25) is dropped: none of those differs from Java. F25's call-time
throw is still recorded (§4 item 3).

**Goal:** wherever Java throws an exception from a public method, the .NET binding throws the same
Java-named class, at the same point (at the call or inside the returned future), with Java's message.
Errors the Rust core returns already have their Java class (the error-hierarchy parity work); this plan
covers the checks the binding makes itself.

**Mode:** .NET only. Nothing under `rust/`, `c/` or `python/` changes, and the C header does not
change. A difference that cannot be removed without changing one of those is not fixed: it is
recorded in `dotnet/CLAUDE.md` (§4 below), so the binding's rulebook names every remaining
difference from Java.

**Source:** a method-by-method audit of the producer, consumer, admin and common public API against
Java 4.4, plus the open review comments on the pull request. Each item gives the Java member that
decides the behaviour; the Actor re-reads it before changing anything.

---

## 1 · Decisions

- **D1 (ruled): only .NET changes.** Items that need the core, the C ABI or Python are recorded,
  not fixed.
- **D2 (ruled): a `null` config value follows Java per key.** Java's `ConfigDef.parseValue` runs
  the key's validator on `null`: a key whose validator rejects it throws `ConfigException` with
  the validator's message (`Range`: "Value must be non-null"; `ValidString`: "String must be one
  of: …"; `CaseInsensitiveValidString`: "String must be one of (case insensitive): …"; a
  `ValidList` that refuses nulls: "Configuration '<key>' values must not be null."). Any other
  `null` is treated as not set, as Python does. The table of null-rejecting keys is a .NET source
  file checked in once. A unit test reads the generated Python table
  (`python/confluent_kafka/_config_types.py`, read only) and fails if the two differ, so the .NET
  table cannot drift without a failing test. No generator changes.
- **D3 (ruled): an admin RPC with a null topic name or group id inside an argument keeps a
  call-time throw**, as Python does, and `dotnet/CLAUDE.md` records it. Java fails that key's
  future with `InvalidTopicException` / `InvalidGroupIdException` instead. 
- **D4: a .NET-only precondition stays `ArgumentNullException` / `ArgumentOutOfRangeException`**
  (`dotnet/CLAUDE.md` §A5), but only where Java throws `NullPointerException` or nothing. Where
  Java throws a Java-named class for the same input, .NET throws that class.
- **D5: a nullable result the C ABI can return is typed nullable** (review comments). Java's
  `FeatureMetadata` / `TransactionDescription` futures never complete with `null`, but the C ABI
  can return a null handle and the binding then completes the future with `null`, as Python
  completes it with `None`. The public types say so (`FeatureMetadata?`, `TransactionDescription?`)
  instead of hiding it behind `null!`, and the XML docs state when it happens.
- **D6: unpaired surrogates encode as Java's `String.getBytes(UTF_8)` does**, as `?` (`0x3F`), in
  `StringSerde` and in every string the binding marshals to the C ABI (topics, header keys, group
  ids, admin strings). Today `StringSerde` writes `U+FFFD` and the marshaller throws
  `ArgumentException`. A NUL character still cannot cross the C ABI (C strings): that stays a
  call-time `ArgumentException` and is recorded.

---

## 2 · Items to fix

Paths are relative to `dotnet/src/Confluent.Kafka/`. "Java" is under
`kafka/clients/src/main/java/org/apache/kafka/`.

All items are done in one phase. The **Kind** column says what each item is:

- **A — a different class from Java's** where Java throws for that input (13 items). These are the
  call-site mismatches the audit was looking for.
- **B — .NET throws where Java does not** (5 items).
- **C — consistency with the binding's own rule** (F3, F6, F25): dropped, because none of them
  differs from Java.
- **D — not a call-site exception difference** (5 items): the open review comments (F7–F10) and a
  documentation fix (F5).

### Producer, common types, review comments

| # | Kind | Java member and condition | Java | .NET today | Fix |
|---|---|---|---|---|---|
| F1 | A | `RecordHeaders.add` with a null key | `IllegalArgumentException` "key cannot be null." | `ArgumentNullException` (`Common/Header/Headers.cs` `CheckKey`) | Throw `IllegalArgumentException` with Java's message. Check `RecordHeader`'s own constructor too |
| F2 | B | `MockProducer.errorNext(null)` | completes the send successfully | `ArgumentNullException` (`Internal/NativeProducer.cs` `MockErrorNext`) | A `null` completes the next send, as Java's |
| F4 | B | `MockProducer` `partitionsFor`, `metrics`, `completeNext`, `errorNext`, `clear` and the history getters after `close()` | no throw (Java's mock checks for close only in `send`, `flush` and the transaction methods) | `IllegalStateException` "MockProducer is already closed." | Drop the closed check on those methods if the mock's native handle outlives `close()`. If it does not, record it (§4) |
| F5 | D | `close(Duration)` with a negative timeout | `IllegalArgumentException` | `IllegalArgumentException` (correct), but `IProducer.cs` and `IAsyncProducer.cs` document `ArgumentOutOfRangeException` | Fix the XML docs |
| F7 | D | `StringSerializer` with an unpaired surrogate (review comment) | `?` (`0x3F`) | `U+FFFD` (`Internal/Serialization/StringSerde.cs`) | D6: a cached UTF-8 encoding with `EncoderReplacementFallback("?")`; wire tests for an unpaired high and low surrogate |
| F8 | D | A string marshalled to the C ABI with an unpaired surrogate | encoded as `?` | `ArgumentException` (`Internal/Interop/ProducerSendMarshal.cs`, `Internal/AdminStrings.cs`, the other UTF-8 marshallers) | D6: encode with the same fallback. NUL stays `ArgumentException`, recorded |
| F9 | D | `send` with an async callback when the serializer throws an `ApiException` (review comments) | `flush()` and `close()` return only after the callbacks of the records they cover | `FutureFailure.SendAsync` completes the first stage without registering the callback task, so `Flush` / `Close` can return first (`Producer/AsyncKafkaProducer.cs`) | Register the managed failure's callback task with the producer's drain, so `Flush` and `Close` wait for it as they wait for native completions. Test: a slow async callback on a serializer failure holds `Flush` |
| F10 | D | `describeFeatures` / `describeTransactions` results (review comments) | — | `null!` returned as a non-nullable value (`Internal/Interop/FeatureMetadataMarshal.cs`, `TransactionDescriptionMarshal.cs`) | D5: `FeatureMetadata?` / `TransactionDescription?` through the converter, the per-id futures, `All()`'s dictionary and both sync and async results, with the docs |

### Consumer

| # | Kind | Java member and condition | Java | .NET today | Fix |
|---|---|---|---|---|---|
| F11 | A | `seek`, `position`, `pause`, `resume`, `currentLag` with a negative partition | `IllegalStateException` "No current assignment for partition t--1" (`seek`, `pause`, `resume`, the real consumers' `currentLag`); `position`: "You can only check the position for partitions assigned to this consumer.", `IllegalStateException` on the real consumers, `IllegalArgumentException` on the mocks; the mocks' `currentLag` returns 0 | `ArgumentOutOfRangeException` (`Internal/NativeConsumer.cs` `RequireTopicPartition`, and the collection checks) | Drop the binding's partition check and let the core answer. Test that the class and message are Java's. If the core gives something else, keep the call as it is and record it |
| F12 | B | `assign`, `committed`, `beginningOffsets`, `endOffsets`, `offsetsForTimes`, `commitSync(offsets)` with a negative partition | accepted (the core or the broker decides) | `ArgumentOutOfRangeException` | As F11 |
| F13 | A | `seek`, `position`, `pause`, `resume`, `currentLag` with a null topic | `IllegalStateException` "No current assignment for partition null-<p>" | `ArgumentNullException` / `ArgumentException` "Topic names must not be null." | A null topic can never be assigned, so the binding throws `IllegalStateException` with Java's message (Java's `TopicPartition.toString()` prints `null-<p>`) |
| F14 | A | `subscribe(null)` on a consumer without `group.id` | `InvalidGroupIdException` (Java checks the group id before the null) | `IllegalArgumentException` | Check the group id first, with Java's message |
| F15 | A | `seek(tp, negative offset)`, `seekToBeginning(null)`, `seekToEnd(null)` on a closed consumer or while another thread holds it | `IllegalArgumentException` (checked before `acquire()`) | `IllegalStateException` / `ConcurrentModificationException` | Run the argument checks before the closed and owner checks, in Java's order |
| F16 | B | `metrics()` while another thread is inside the consumer | no throw (no `acquire()`) | `ConcurrentModificationException`; `dotnet/CLAUDE.md` justifies it as "as Python's `metrics()` does", which is false | Take only the handle-lifetime use, not the owner check. Correct the rule text |
| F17 | B | `enforceRebalance()` / `(reason)` after `close()` or from another thread | no throw (it only logs) | `IllegalStateException` / `ConcurrentModificationException`, against `dotnet/CLAUDE.md` ("never throws") | After close: return. From another thread: no owner check, as F16 |
| F18 | A | `KafkaConsumer(config, null, …)` (null deserializer), and the mock and async constructors | uses `key.deserializer` / `value.deserializer`; `ConfigException` "Missing required configuration \"key.deserializer\" which has no default value." when it is missing | `ArgumentNullException` | .NET config takes no classes, so the key is never set: throw Java's `ConfigException` with Java's message. Record that the config route does not exist (§4) |
| F19 | A | Consumer `null` config value | per key (D2) | plain `ArgumentException` (`Internal/NativeConsumer.cs`) | D2 |

### Admin, and the shared config table

| # | Kind | Java member and condition | Java | .NET today | Fix |
|---|---|---|---|---|---|
| F20 | A | `RemoveMembersFromConsumerGroupResult.memberResult(null)` | `IllegalArgumentException` (the removeAll-mode message, or "Member null was not included in the original request") | `ArgumentNullException` (`Admin/RemoveMembersFromConsumerGroupResult.cs`) | Java's class and messages, in Java's order |
| F21 | A | `AlterConsumerGroupOffsetsResult.partitionResult` / `DeleteConsumerGroupOffsetsResult.partitionResult` with a null topic | `IllegalArgumentException` (in the future for Alter, at the call for Delete) | plain `ArgumentException` at the call | Java's class, message and point |
| F22 | A | `DescribeUserScramCredentialsResult.description(null)` | a future failed with `ResourceNotFoundException` "No such user: null" | `ArgumentNullException` at the call | Return a failed future, as Java |
| F23 | A | `MockAdminClient.Builder` `controller(int)`, `numBrokers` shrinking below the log-dir count, `build()` with no brokers, `addTopic` with a leader without log dirs | `IndexOutOfBoundsException` at the call | `IllegalArgumentException` from the core mock | Where the builder's state is in managed code, check the index there and throw `IndexOutOfBoundsException` with the JDK's message. Where it is only in the core, record it |
| F24 | A | `MockAdminClient.createDelegationToken` without a renewer | `IndexOutOfBoundsException` at the call | `IllegalArgumentException` in the future, from the core mock | Check the renewers in the binding and throw at the call, with the JDK's message |
| F26 | A | `Admin.create` with a `null` config value | per key (D2) | plain `ArgumentException` (`Internal/NativeAdminClient.cs`) | D2, with the producer's (`Internal/NativeProducer.cs`) in the same change |

---

## 3 · Not changed: aligned already, or recorded already

Closed clients throwing `IllegalStateException` at the call; negative timeouts on the consumer's
query methods; the `CreateTopicsResult` accessors for a topic not in the request; the consumer's
reentrancy `ConcurrentModificationException`; `commitAsync(null)`; the core mock's unimplemented
RPCs failing each key's future; .NET-only checks of undefined enum values; `NullPointerException`
as `ArgumentNullException`. The `Uuid.Parse` message for bad base64 differs from Java's decoder
text, but the class is Java's; it is left as it is.

## 4 · Recorded, not fixed (need the core, the C ABI or Python)

Each gets one bullet in `dotnet/CLAUDE.md`, next to the rule it qualifies, saying what differs,
why, and what would remove it:

1. A NUL character in a string argument throws `ArgumentException` at the call (C strings cannot
   carry it); Java sends it and the broker rejects the topic.
2. `NewTopic` with a partition count or replication factor below -1 throws
   `ArgumentOutOfRangeException` at construction; Java's future fails with
   `InvalidPartitionsException` / `InvalidReplicationFactorException`. The C ABI reads any negative
   as "unset", so passing the value through would create the topic with broker defaults.
3. Admin null topic names and group ids throw at the call (D3).
4. The producer's `Flush`, `PartitionsFor` and `Metrics`, and the consumer's `Metrics`, throw
   `IllegalStateException` after `close()`, because the native handle is gone; Java answers. (And
   F4, if the mock's handle does not outlive `close()`.)
5. `MockAdminClient.setFetchesRemainingUntilVisible` on an unknown topic throws
   `IllegalArgumentException` (the core mock); Java's throws `RuntimeException`. (And F23's cases
   that live only in the core.)
6. The consumer has no `key.deserializer` / `value.deserializer` route (F18).
7. Any of F11 and F12 where the core does not answer as Java does.

## 5 · Process and tests

- One phase, one Actor/Critic loop (Actor and Critic 118), one commit by the Manager as the binding
  owner (no co-author trailer); nothing is pushed.
- Every fix gets a unit test asserting the exact class, message and point (call or future), sync
  and async where both exist.
- The Actor and Critic use filtered `dotnet test` runs; the Manager runs the full gate once, before
  the commit.
- The same commit adds the `dotnet/CLAUDE.md` bullets for the recorded items (§4).
