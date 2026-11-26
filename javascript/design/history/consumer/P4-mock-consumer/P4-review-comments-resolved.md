# P4: the issues the review resolved (Critic 153)

Copied at the end of P4 (2026-11-27) from `COMMENTS.DONE.153.md` at the repository root: the issues Critic 153
found in its first review of P4, each with the fix that resolved it, in issue order. The review's round 2 confirmed
all five fixes and found no new issue. It also judged the one fix-up line outside its target (the count comment of
`test/lint/exception_word.test.js`, made stale by its target's own change) a one-off, not a precedent.

## Issue 153-1: On a MockConsumer, `assign` and `subscribe` turn a null partition or topic into a blank topic, which the core's mock keeps
- **Commit**: `b50f0fc17` javascript: add MockConsumer over the FFI's mock, with its tests (the mock runs the `assign` and `subscribe` bodies `c13a488dd` moved)
- **File**: `javascript/src/internal/consumer_calls.ts:190` (`partitionPairs`' `blankNull`), `:300` (`topic ?? ''`), `:322`; `javascript/src/consumer/mock_consumer.ts:207,216`
- **Severity**: Behavior Mismatch
- **Reference**: Java `MockConsumer#assign` and `#subscribe(Collection)`, through `SubscriptionState#assignFromUser` (`manualSubscribedTopics.add(partition.topic())`) and `#subscribe(Set, Optional)`; P4.md §1 ("Where the FFI's mock differs from Java's, the binding passes it through, a test pins it, and NOTES.md and the package docs list it") and §8 risk 4.
- **Description**: The shared bodies rewrite a null partition, or a null topic, as a blank topic, so that a `KafkaConsumer` gets the core's message for it (NOTES.md, "The handle": AsyncKafkaConsumer's "cannot contain null or empty topic" and "cannot have null or empty topic" checks). The core's mock makes no such check (`MockConsumer::subscribe_internal_topics`, and `assign` through `SubscriptionState::assign_from_user`), so on a mock the blank topic is stored. A probe of the built package at `fc0a5f6a0`:
  - `mock.assign([null])` resolves, and `mock.assignment()` is `[TopicPartition('', 0)]`, printed `-0`. Java's mock throws `NullPointerException` at `partition.topic()` in `assignFromUser`.
  - `mock.assign([new TopicPartition({ topic: null, partition: 3 })])` assigns `('', 3)`. Java's mock assigns `null-3`.
  - `mock.subscribe(['a', null])` resolves, and `mock.subscription()` is `{'', 'a'}`. Java's mock keeps `{null, 'a'}`.

  The mock's other calls send a null element through as NULL, and the FFI refuses it: `rebalance([null])` rejects with `TypeError` "new_assignment must not contain null", and `losePartitions([null])`, `pause([null])` and `seekToBeginning([null])` reject the same way. NOTES.md's mock section, the package docs and PLAN §6 don't mention the blank topic on the mock, and no test pins it.
- **Expected**: On a mock, a null partition or topic given to `assign` or `subscribe` doesn't become a partition or topic the test never gave. Either it goes to the FFI as the mock's other calls send theirs (the FFI's `TypeError` then answers a null partition, as Java's `NullPointerException` does; a null topic comes under asks 7 and 10), or the mapping stays, and the difference is recorded in NOTES.md, the package docs and PLAN §6 and pinned by a test (P4.md §8 risk 4). The mapping is P2's code that item 1 only moved, so per P4.md §3 the change is a new commit, not a fix-up of the move.
- **Actual**: `assign([null])` succeeds with a partition `-0` the test never gave, and a null topic becomes `''`. None of this is recorded or pinned.
- **Fixed**: `1248c2831` (a new commit: the blank-topic rewrite is P2's code the move only moved). The shared `assign` and `subscribe` bodies keep it behind a `blankNull` flag, which `KafkaConsumer` sets and `MockConsumer` doesn't; on a mock the null goes to the FFI as NULL. Tests pin `assign([null])` ("partitions must not contain null"), a null-topic partition ("topic must not be null") and `subscribe(['a', null])` ("topics must not contain null"), and that nothing was assigned or subscribed to. NOTES.md, the package docs and PLAN §6 (ask 10, beside ask 7) record it.

## Issue 153-2: More places where the FFI's mock differs from Java's mock, none recorded or pinned
- **Commit**: `b50f0fc17` (the behaviours), `6e8ee7aa2` (NOTES.md, the package docs, PLAN §6)
- **File**: `javascript/design/current/consumer/NOTES.md:262`; `javascript/src/consumer/index.ts:41-60`; `javascript/design/current/consumer/PLAN.md:518-529`; `javascript/test/unit/consumer/mock_consumer.test.js`
- **Severity**: Missing Requirement
- **Reference**: P4.md §8 risk 4 ("Each one found gets a pinned test, a `NOTES.md` line and a PLAN §6 ask"); Java `MockConsumer#partitionsFor`, `#currentLag`, `#subscribe(Collection)`, `#assign`, `#seekToBeginning`, `#seekToEnd` and `#poll`; JS conventions, Types (`NullPointerException` becomes `TypeError`).
- **Description**: A probe of the built mock at `fc0a5f6a0` found these differences beyond asks 5 and 8 to 11:
  - `partitionsFor(null)`, and `{ topic: null, timeout: 1 }`, reject with `TypeError` "topic must not be null". Java's mock answers `[]` (`partitions.getOrDefault(topic, Collections.emptyList())`). The binding's notes record the same case for `MockProducer` as a deviation. The consumer's mock has no such record.
  - `currentLag(null)` rejects with `TypeError` "topic_partition must not be null". Java's mock answers `OptionalLong.of(0L)`, since `endOffsets.containsKey(null)` is false.
  - `subscribe({ topics: null })`, `assign({ partitions: null })`, `seekToBeginning(null)` and `seekToEnd(null)` reject with `IllegalArgumentError` and Java's `KafkaConsumer` messages: "Topic collection to subscribe to cannot be null", "Topic partitions collection to assign to cannot be null" and "Partitions collection cannot be null". Java's mock throws `NullPointerException` from `new HashSet<>(topics)`, `new HashSet<>(partitions)` and `requestOffsetReset`'s `partitions.forEach`, which is a `TypeError` here. Ask 10 records the null pattern's message, the same family. These four aren't recorded.
  - A negative timeout: `poll(-1)`, `position({ partition, timeout: -1 })`, `commitSync({ timeout: -1 })` and the other timed forms reject with "Timeout must not be negative" from the FFI's `ms()`, and `lastPollTimeout()` stays `null`. Java's mock ignores each of these timeouts, and its `poll` records the timeout as `lastPollTimeout`. The binding's behaviour is the conventions' rule (Signatures, Q35). The rule's deviation names only Java's `KafkaConsumer` behaviour: "Invalid negative timeout -1", or waiting forever. Its producer bullet records `MockProducer`'s Java side separately, but nothing does that for the consumer's mock.
- **Expected**: Each case gets a test that pins today's answer, a line in NOTES.md's "Where the FFI's mock differs from Java's" and in the package docs' list, and, for the FFI's refusals, a PLAN §6 ask or an extension of ask 10, as P4.md §8 risk 4 asks.
- **Actual**: None of them is recorded or pinned.
- **Fixed**: `3904862d0` (fix-up of `b50f0fc17`: the pinned tests) and `38acc374c` (fix-up of `6e8ee7aa2`: NOTES.md, the package docs, PLAN §6 ask 10 extended and ask 12 added). No behaviour changes.

## Issue 153-3: The mock copies P3's and the producer mock's code (DoD #6)
- **Commit**: `b50f0fc17` javascript: add MockConsumer over the FFI's mock, with its tests
- **File**: `javascript/src/consumer/mock_consumer.ts:110-118` (`offsetEntries`), `:79` (`isErrorOrNull`); `javascript/src/internal/metadata.ts:71-77` (`NodeArgs`)
- **Severity**: Design Flaw
- **Reference**: definition-of-done.md #6; P4.md §1 item 9 (`tp_long_map_from_js`, "which the `update*Offsets` calls reuse"); 152-3's precedent (`longMap`), and `fc0a5f6a0`, which dropped the mock's own copy of `isIterable` for this reason.
- **Description**:
  - `offsetEntries` is `offsetsForTimes`' entries mapping (`consumer_calls.ts:640-653`) with other names. Both call `topicPartitionLongMap.take`, refuse a null value with a `TypeError`, and build the same `[[topic, partition] | null, bigint]` entries that `tp_long_map_from_js` reads. The C side shares the reader, while the TypeScript side copies the code that builds its input.
  - `isErrorOrNull` (`mock_consumer.ts:79`) is `mock_producer.ts:77`, word for word.
  - `NodeArgs` (`metadata.ts:71`) is `NodeFields` (`metadata.ts:24`) field for field, in the same file.
- **Expected**: One helper builds both entry lists, taking the parameter's name and the word for its value (offset, timestamp). One `isErrorOrNull` lives in `src/internal/` and serves both mocks. One interface describes a node's fields.
- **Actual**: Each of the three exists twice.
- **Fixed**: `3904862d0` (fix-up of `b50f0fc17`: `NodeArgs` gives way to `NodeFields`), `c13037179` (a new commit: `longEntries` in consumer_calls.ts builds the entries for `offsetsForTimes` and the three `update*Offsets`) and `30aa2cd55` (a new commit: throwable.ts' `isErrorOrNull`, the third copy, is exported and serves both mocks).

## Issue 153-4: The "one call at a time" difference is only partly listed and pinned
- **Commit**: `b50f0fc17` (the test), `6e8ee7aa2` (the package docs)
- **File**: `javascript/test/unit/consumer/mock_consumer.test.js:764`; `javascript/src/consumer/index.ts:43-49`
- **Severity**: Test Gap
- **Reference**: P4.md §2 ("one call at a time, a mock member during a pending `poll` included, and the ignored setters of ask 8, pinned"); Java `MockConsumer#assignment`, `#subscription` and `#paused` (`synchronized`), and `#groupMetadata` (not synchronized).
- **Description**:
  - The test pins one of ask 8's six setters, `updateEndOffsets`. Nothing pins `updateBeginningOffsets`, `updateDurationOffsets`, `setPollException`, `setOffsetsException` or `scheduleNopPollTask`. When the core ignores a call to one of the two error setters, the error the binding built for it (its `InjectedError`) is dropped too, and released only at the next mock call. No test covers that path.
  - The package docs list the calls that throw `ConcurrentModificationError` while another call on the mock is pending: `addRecord()`, `setMaxPollRecords()`, `updatePartitions()` and the waiting methods. A probe at `fc0a5f6a0` (each call made right after `poll(0)`, until one was refused) shows that `assignment()`, `subscription()`, `paused()` and `groupMetadata()` throw it too. Java's mock waits for those, being `synchronized`, or, for `groupMetadata()`, answers at once. `metrics()`, `wakeup()`, `closed()` and `lastPollTimeout()` were never refused in 20000 rounds. Neither the package docs' list nor any test mentions the four getters.
- **Expected**: The window test, or one beside it, covers each ignored setter whose effect can be seen (the offsets read back, a poll that doesn't reject) and the four getters, and the package docs' list names the getters.
- **Actual**: As described above.
- **Fixed**: `3904862d0` (fix-up of `b50f0fc17`: tests of `updateBeginningOffsets`, `updateDurationOffsets`, `setPollException` and `setOffsetsException` ignored during a pending poll, beside the existing `updateEndOffsets` one; of the four getters refused; and a memory probe that the ignored error setter's error is let go at the next mock call) and `38acc374c` (fix-up of `6e8ee7aa2`: the package docs and NOTES.md name the getters; PLAN §6 ask 13). `scheduleNopPollTask` has no effect a test can see, so it is not covered.

## Issue 153-5: `rebalance`'s and `losePartitions`' TSDoc says they throw, and puts Java's `@throws` in `@remarks`
- **Commit**: `b50f0fc17` javascript: add MockConsumer over the FFI's mock, with its tests
- **File**: `javascript/src/consumer/mock_consumer.ts:468-530`
- **Severity**: Missing Requirement
- **Reference**: JS conventions, Errors ("A method that returns a Promise never throws: it rejects") and Documentation (the tags are `@throws` naming the error classes, and one `@remarks` for the JavaScript notes); Java `MockConsumer#losePartitions` (`@throws IllegalStateException if any partition is not currently assigned`). Every other Promise method of the package writes its errors as "rejects with it" (`src/consumer/consumer.ts`, `src/producer/producer.ts`).
- **Description**: `rebalance` and `losePartitions` return Promises and never throw: the unit test checks their argument errors with `assert.rejects`. Their TSDoc still says "`@throws` `IllegalArgumentError` when the given parameters are not ..." and "`@throws` `TypeError` when ...". `losePartitions` also moves Java's `@throws` into its `@remarks` ("The Promise rejects with `IllegalStateError` if any partition is not currently assigned"), so TypeDoc's Throws section leaves out Java's contract.
- **Expected**: `@throws` `IllegalStateError` "rejects with it if any partition is not currently assigned", and the argument errors written as rejections, as the `Consumer` interface writes them.
- **Actual**: As described above.
- **Fixed**: `3904862d0` (fix-up of `b50f0fc17`): `rebalance`'s and `losePartitions`' argument errors are written as rejections, and Java's `@throws IllegalStateException` is `losePartitions`' `@throws IllegalStateError` "rejects with it if any partition is not currently assigned", out of `@remarks`; within the doc-size limit. The Critic's cosmetic note (the count comment in `test/lint/exception_word.test.js`) is fixed in the same fix-up.
