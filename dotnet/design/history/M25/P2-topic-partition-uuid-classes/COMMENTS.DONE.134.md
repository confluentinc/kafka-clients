# COMMENTS.DONE.134 — M25/P2 (TopicPartition and Uuid are classes)

### 134-S2-1 (Low, test-coverage): the ListPartitionReassignments shape test no longer checks the map's value, and its comment is wrong

- **Where**: `dotnet/tests/Confluent.Kafka.UnitTests/PublicAdminP4Stage2ShapeParityTests.cs:100-107`
  (`ListPartitionReassignmentsResult_PublishesExactlyOneAccessor`).
- **What**: the comment says "Nullability positions: 0 = the KafkaFuture (a struct, so oblivious), 1 = the
  dictionary, 2 = its value", and the loop checks positions 1 and 2. That was true while `TopicPartition` was a
  struct, which takes no position. Now the `TopicPartition` key is position 2 and the `PartitionReassignment`
  value is position 3. The flags of `ListPartitionReassignmentsResult.Reassignments()`'s return at 709a9e966 are
  `0,1,1,1`, so the loop now checks the key and never the value. The test still passes, so the commit fixed the
  two `PublicAdminP4ShapeParityTests` positions that failed and missed this one.
- **Evidence** (`git archive` trees, net8.0): change `Reassignments()` to return
  `KafkaFuture<IReadOnlyDictionary<TopicPartition, PartitionReassignment?>>` (flags become `0,1,1,2`; build 0W/0E).
  At 709a9e966 all 168 `~ShapeParity` tests pass. At the parent 88f1196fe the same change fails
  `ListPartitionReassignmentsResult_PublishesExactlyOneAccessor` (1 of 168). So S2 removed the only check that
  this value is non-null. Java's `ListPartitionReassignmentsResult#reassignments` is a
  `KafkaFuture<Map<TopicPartition, PartitionReassignment>>` whose values are never null, and the test's own name
  ("exactly one accessor") and comment state that it checks the value.
- **Fix**: fix the comment ("2 = its TopicPartition key, 3 = its value") and check positions 1 to 3, as the commit
  did in `PublicAdminP4ShapeParityTests` (lines 90-95 and 113-118). No other `NullableAnnotation.Flag(…, n)` call
  in the unit tests reads a position after a `TopicPartition` (the others read position 0 or 1 of a type with no
  `TopicPartition` before them, or use `NullabilityInfoContext`).

Fixed in b31be1fdd: the comment names position 2 as the TopicPartition key and 3 as the value, and the loop checks positions 1 to 3 (a nullable-value mutant now fails the test on net8.0).

### 134-S4-1 (Low, order): a real `Commit` with a null key is refused before the owner check and the group check

- **Where**: `dotnet/src/Confluent.Kafka/Internal/NativeConsumer.cs:1264-1265` (`SnapshotCommitOffsetsWhenOpen` →
  `CheckWhenOpen(() => OffsetsSnapshot.Of(offsets))`), with the new null-key check at
  `Internal/OffsetsSnapshot.cs:70`. Every real form goes through it (`NativeConsumer.cs:500, 1083, 1095`,
  `NativeConsumer.Async.cs:459, 488, 673, 684`).
- **What**: on the real consumers `CheckWhenOpen` runs before the owner check (it is for what Java checks before
  `acquire()`). Java checks only the map itself there: `AsyncKafkaConsumer#commitSync(Map, Duration)` and
  `#commitAsync(Map, OffsetCommitCallback)` copy it with `new HashMap<>(offsets)`, which accepts a null key. Then
  `acquireAndEnsureOpen()` runs, then `commit(CommitEvent)` calls `throwIfGroupIdNotDefined()`, and only the
  background thread reads the key. So Java gives `ConcurrentModificationException` while another thread is inside,
  and `InvalidGroupIdException` without `group.id`. .NET gives `ArgumentNullException("offsets")` in both cases.
  §4.1: "Checks keep their place in Java's order". The commit applies this rule to C10 (`Committed` without
  `group.id` gives the group error first, `CommittedPartitions`) but not to C9. The binding already keeps Java's
  group-first order for `Commit` with a negative timeout (`ThrowIfNoGroupIdBefore`).
- **Evidence** (probe, real `KafkaConsumer`): without `group.id`, `Commit({null: 6})`, `Commit(…, 1s)` and
  `CommitAsync(…, null)` give `ArgumentNullException [offsets]`, while `Commit({np-0: 6})` gives
  `InvalidGroupIdException`. With a `Poll(3s)` running on another thread, `Commit({null: 6})` gives
  `ArgumentNullException`, while `Commit({np-0: 6})`, `Pause({null})` and `SeekToBeginning({null})` give
  `ConcurrentModificationException`.
- **Fix**: on the real consumers, answer a null key after the use is entered and after the group check, as C10
  does: `CheckWhenOpen` keeps the null-map check (and today's null-topic answer). A null key gives no snapshot, and
  inside `CheckInAUse` the binding throws `GroupIdMissing ? GroupIdNotDefined() : new ArgumentNullException(…)`.
  The mocks keep their order: Java's `MockConsumer` runs only `ensureNotClosed()` before `putAll`. Add the
  no-`group.id` case to `Commit_ANullKey_…` and a C9 row to the owner matrix (134-S4-2). If the PM keeps the
  current order instead, record it in S8 and say why C9 and C10 differ.

Fixed in bbc0f7f18: on the real consumers a null commit key gives no snapshot before the owner check and is answered inside the use, after the group check (and after commitSync(Map, Duration)'s timeout check): InvalidGroupIdException without group.id, Java's "Invalid negative timeout" for a negative timeout, else the ArgumentNullException; the mocks keep their order. Tests: Commit_ANullKey_WithoutAGroupId_IsTheGroupError_BeforeTheNullCheck and Commit_ANullKey_WithANegativeTimeout_IsJavasTimeoutError_BeforeTheNullCheck (KafkaConsumer, AsyncKafkaConsumer) and the C9 rows of 134-S4-2; the old order (snapshot before the owner check) fails 6 tests.

### 134-S4-2 (Low, test-coverage): no test pins the owner order of the null-partition answers

- **Where**: `NativeConsumer.cs:2776-2778` (`SnapshotSeekPartitions`, real path: `?? CheckInAUse<…>(() => throw
  new IllegalStateException(NoCurrentAssignment(null)))`). The matrices without null-partition rows are
  `PublicConsumerCheckOrderTests.cs:127-161` (`WhileAnotherThreadIsInside_ANullArgument_…`) and `:187-240`
  (`OnTheAsyncMock_WhileARebalanceRefusesOtherThreads_…`).
- **What**: the commit puts the real `SeekToBeginning` / `SeekToEnd` null answer "after the owner check too" with
  code written only for it, and the deviation list says "C4 real: null check after the owner check". No test runs a
  null partition while another thread is inside. The new tests check only the closed-first order
  (`OnAClosedConsumer_ANullPartition_…`). The check-order matrices list only null-topic and null-collection calls.
- **Evidence**: mutant M1 removes the `CheckInAUse` wrapper (`?? throw new IllegalStateException(...)`). Build
  0W/0E. The whole `~Consumer` set passes: 1949 passed, 0 failed, 2 skipped, of 1951. Reachability control, with a
  `Poll(3s)` running on another thread: under M1 `SeekToBeginning({null})` and `SeekToEnd({null})` give
  `IllegalStateException "No current assignment for partition null"`. At HEAD they give
  `ConcurrentModificationException`. So the behaviour changes and no test sees it. For comparison, M3 (D17 without
  `!IsMock`), M4 (no `Array.Resize` for skipped nulls) and M5 (no de-duplication in `NotAssignedToLose`) each fail
  2 of the 59 new tests.
- **Fix**: add null-partition rows to the real-consumer owner matrix: `Pause`, `Resume`, `SeekToBeginning`,
  `SeekToEnd`, `Position` (both forms), `Seek` (both forms), `CurrentLag`, `Committed`, `OffsetsForTimes`,
  `BeginningOffsets`, `EndOffsets`, and `Commit` after 134-S4-1. Each expects `ConcurrentModificationException`,
  except `Seek(null, negative offset)` and `Seek(null, null OffsetAndMetadata)`, which Java checks before
  `acquire()`. Add the same rows to the async-mock refusal test.

Fixed in bbc0f7f18: NullArgumentCalls gains 24 null-partition rows (Pause, Resume, SeekToBeginning, SeekToEnd, Position x2, Seek x5, CurrentLag, Committed x2, OffsetsForTimes x2, BeginningOffsets x2, EndOffsets x2, Commit x2, CommitAsync x2) and NullAsyncCallbackCalls one (CommitAsync(null key, async callback)); the real-consumer matrix expects ConcurrentModificationException except Seek(null, negative offset) (IAE) and Seek(null, null) (ANE offsetAndMetadata), the async-mock refusal expects it everywhere but CurrentLag(null), which returns its Task as for a null topic. Mutant M1 (no CheckInAUse in SnapshotSeekPartitions) now fails both real-consumer cases.

### 134-S4-3 (Low, order): a real `OffsetsForTimes` refuses a null key before Java's negative-target check

- **Where**: `NativeConsumer.cs:2890-2911` (`SnapshotTimestamps`, the new `tp is null` branch).
- **What**: `AsyncKafkaConsumer#offsetsForTimes(Map, Duration)` checks every value at the call, after
  `acquireAndEnsureOpen()` and before the event exists. It throws `IllegalArgumentException("The target time for
  partition " + entry.getKey() + " is " + value + ". The target time cannot be negative.")`, and a null key prints
  as `null`. Only the background thread reads the key (`OffsetFetcherUtils#topicsForPartitions`). The binding does
  not check negatives (the core does, after the snapshot), so the new null-key check comes first. This is the
  pattern the Actor records for C13 (a core check that Java runs first), but it is not in the deviation list.
- **Evidence** (probe, real `KafkaConsumer`): `OffsetsForTimes({np-0: -1})` gives the core's
  `IllegalArgumentException "The target time for partition np-0 is -1. …"`. `{null: -1}` and `{null: 5, np-0: -1}`
  both give `ArgumentNullException [timestampsToSearch]`. Java gives the IAE (for `null`, then for `np-0`).
- **Fix**: on the real consumers, when a key is null, first scan the values in iteration order and throw Java's
  IAE text for the first negative one (`JavaText.ValueOf(key)`), as `RequireSeekOffset` already repeats Java's
  pre-check for `Seek`. Otherwise refuse the null key. Or record it in S8 next to C13. Fix the type doc too: the
  `OffsetsForTimes` summary says "A negative timestamp is passed through, not rejected", but the core rejects
  `-1` (an older doc error, visible here).

Fixed in bbc0f7f18: a real OffsetsForTimes that meets a null key first scans the map in its order and throws Java's IAE for the first negative target time (key printed with JavaText.ValueOf, so "null"), then the ArgumentNullException; the IConsumer/IAsyncConsumer docs drop "A negative timestamp is passed through" and list the IAE (on the async consumer the core's IAE faults the Task, the binding's null-key one is thrown at once, as C10's group answer). Test: OffsetsForTimes_ANullKey_ANegativeTargetTime_IsJavasIllegalArgumentException_First (3 maps x 3 timeouts); removing the scan fails it.

### 134-S4-4 (Low, plan gap): a real `BeginningOffsets` / `EndOffsets` / `OffsetsForTimes` with a zero timeout never sees Java's NPE

- **Where**: `NativeConsumer.cs:1190-1196` (`LookUpOffsets`) and `:1150-1158` (`OffsetsForTimes(…, timeout)`),
  with their async twins. These are rows C11 and C12, which the plan files as case 3 ("background NPE").
- **What**: with `Duration.ZERO`, Java's `AsyncKafkaConsumer#beginningOrEndOffset` only `add`s the event and returns
  `new HashMap<>()`. `Collectors.toMap` accepts the null key, and the background NPE completes a future nobody
  waits on. `#offsetsForTimes` returns `listOffsetsEvent.emptyResults()`, which maps every key to `null`, the null
  key included. So Java answers without an exception: `{}`, and `{null=null, …}`. .NET throws
  `ArgumentNullException`. For `BeginningOffsets` / `EndOffsets` this is §4.1 case 5 (Java accepts and .NET can
  return an empty map). For `OffsetsForTimes` it is case 4 (a .NET dictionary cannot hold the null key). Neither is
  case 3, and neither is in the plan or in the deviation list.
- **Evidence** (probe): `BeginningOffsets({np-0}, TimeSpan.Zero)` returns 0 entries and
  `OffsetsForTimes({np-0: 1}, TimeSpan.Zero)` returns `{np-0=null}` (the core follows Java's zero path). With a
  null element or key, both throw `ArgumentNullException [partitions]` / `[timestampsToSearch]`.
- **Fix**: either answer a zero-timeout `BeginningOffsets` / `EndOffsets` with a null element by returning an empty
  map with no native call (Java's answer; the event Java adds fails whole in the background, so nothing is sent),
  and record `OffsetsForTimes(…, Zero)` as a case-4 deviation. Or record both in S8. Add a test for whichever is
  chosen.

Fixed in bbc0f7f18: with a zero timeout (one that reaches the core as 0 ms, as for any partition) a real BeginningOffsets/EndOffsets with a null element returns a new empty map with no native call (Java's new HashMap<>()); OffsetsForTimes keeps the ArgumentNullException after the negative-target scan, as a .NET dictionary cannot hold Java's emptyResults() null key: a case-4 difference for S8. Tests: BeginningAndEndOffsets_ANullElement_WithAZeroTimeout_IsJavasEmptyMap and OffsetsForTimes_ANullKey_WithAZeroTimeout_IsArgumentNullException; docs of the four timed lookups name the empty answer.

### 134-S4-5 (Low, plan row): P1 and P2 do not "already behave": the null key is refused before Java's transaction checks

- **Where**: `NativeProducer.Transactions.cs:239` (`WithTransactionOffsets` → `OffsetsSnapshot.Of`, before the
  native call that runs the transaction checks). These are S5 rows, but this step claims they already behave.
- **What**: plan P1 says "in Java's place (after those checks)". `KafkaProducer#sendOffsetsToTransaction` runs
  `throwIfInvalidGroupMetadata`, `throwIfNoTransactionManager` and `throwIfProducerClosed`, then dereferences the
  key (`TopicPartition::topic`). So a non-transactional producer gives Java's `IllegalStateException` first. Java's
  `MockProducer#sendOffsetsToTransaction` runs `verifyNotClosed`, `verifyNotFenced`,
  `verifyTransactionsInitialized` and `verifyTransactionInFlight`, then the injected exception, before its
  `putAll`. The binding answers the null key before the core runs any of these. (For a transactional producer
  outside a transaction the order is right: Java's dereference comes before `TransactionManager`'s state check.)
- **Evidence** (probes): a real producer without `transactional.id` gives `ArgumentNullException [offsets]` for
  `{null: 6}` and `IllegalStateException "Transactional method invoked on a non-transactional producer."` for
  `{t-0: 6}`. `MockProducer` before `InitTransactions` gives the ANE for `{null: 6}` and "MockProducer hasn't been
  initialized for transactions." for `{t-0: 6}`. After `InitTransactions` it gives the ANE and "There is no open
  transaction."
- **Fix** (S5): the binding cannot see the core's transaction state, so most likely record both in S8 (P1 for the
  non-transactional producer, P2 for the mock's verify checks and injected exception), and correct the "already
  behave" claim. Moving the P1 check after the native call is not possible. If the producer knows binding-side that
  no `transactional.id` is set, it could raise that error first instead.

Fixed in 0caf625ef (S5): offsets with a null key cross as an empty map, for which the core makes exactly the checks Java makes before it reads a key (KafkaProducer: group metadata, transaction manager, closed; MockProducer: closed, fenced, initialized, in a transaction, then the injected exception) and returns; the binding then throws the ArgumentNullException (sync) or faults the Task with it (async). Note: on the default idempotent producer without transactional.id Java also gives the NPE, since its transaction manager exists and "Transactional method invoked on a non-transactional producer." comes from TransactionManager after the dereference; only enable.idempotence=false gives Java's IllegalStateException first, which the test pins. Tests: PublicProducerNullPartitionTests (4 theories x 2 clients); answering the null key before the core fails 6 of them.

### 134-S4-7 (Low, Java behaviour): the no-timeout `BeginningOffsets` / `EndOffsets` miss Java's zero path when `default.api.timeout.ms=0`

- **Where**: `NativeConsumer.cs:1416-1422` (`SnapshotOffsetPartitions` / `IsZeroTimeout`, reached from
  `LookUpOffsets:1189` and `NativeConsumer.Async.cs:867` with `timeout = null` for the no-timeout forms,
  `IConsumer.cs:639, 685` and their `IAsyncConsumer` twins).
- **What**: Java's `beginningOffsets(Collection)` / `endOffsets(Collection)` call the timed form with
  `defaultApiTimeoutMs` (`Duration.ofMillis(config.getInt(DEFAULT_API_TIMEOUT_MS_CONFIG))`, `atLeast(0)`), so with
  `default.api.timeout.ms=0` they take the same `timeout.isZero()` branch S4-4 handles: `new HashMap<>()` without
  reading an element. `IsZeroTimeout(null)` is false, so .NET refuses the null element. The binding's own non-null
  answer is the empty map (the core takes its zero path), so .NET now differs from itself as well.
- **Evidence** (probe, real `KafkaConsumer`, `default.api.timeout.ms=0`): `BeginningOffsets({np-0})` → 0 entries;
  `BeginningOffsets({null})` and `EndOffsets({null})` → `ArgumentNullException [partitions]`. With an explicit
  `TimeSpan.Zero` both give 0 entries. `OffsetsForTimes({np-0: 1})` (no timeout) → `{np-0=null}`, i.e. Java's
  `emptyResults()` too, so its null-key ANE is the same case-4 difference as the explicit zero.
- **Fix**: read the setting at construction as `group.id` is read (`NativeConsumer.cs:358`,
  `ConfigMarshal.TextOf(prepared, …)`) and treat the no-timeout forms as a zero timeout when it is `0`; add the
  no-timeout rows to `BeginningAndEndOffsets_ANullElement_WithAZeroTimeout_IsJavasEmptyMap`. Or record it in S8 next
  to the S4-4 case-4 entry ("a zero timeout, given or from `default.api.timeout.ms`"). The S8 entry for
  `OffsetsForTimes` should name the default either way.

Fixed in 673edaa46: the real consumers read default.api.timeout.ms at construction (Java's trimmed int text, as the core accepted it, via JavaText.Trim + int.TryParse) and the no-timeout BeginningOffsets/EndOffsets take S4-4's zero branch when it is 0; test BeginningAndEndOffsets_ANullElement_WithoutATimeout_AndAZeroDefaultApiTimeout_IsJavasEmptyMap (sync/async, "0", " +0\t", "-0", int 0; {np-0, null}, {null}, {np-0} all empty); mutants (never zero / always zero) 8 and 2 red. OffsetsForTimes keeps the case-4 ANE (S8 names the default).

### 134-S7-1 (Low, test): the RandomUuid seam cannot tell value membership from reference membership

`dotnet/tests/Confluent.Kafka.UnitTests/PublicUuidTests.cs:463` (`RandomUuid_Seam_SkipsReservedAndDashContainingCandidates`)
feeds `Uuid.ZERO_UUID` and `Uuid.ONE_UUID` themselves. Now that `Uuid` is a class, `RandomUuid` skipping those two
*instances* is not the same as skipping the *values*. PLAN §3.2 names exactly this risk ("let `RandomUuid` return
`(0, 0)`"). Mutant: in `Uuid.RandomUuid(Func<Uuid>)`, replace `s_reserved.Contains(uuid)` with
`(ReferenceEquals(uuid, ZERO_UUID) || ReferenceEquals(uuid, ONE_UUID))`. The 70 tests of `PublicUuidTests`,
`PublicAdminNullTopicIdTests` and `PublicTopicPartitionTests` all pass (net10.0; unmutated control 70/70). The code is
right: `ReadOnlyCollection.Contains` uses `EqualityComparer<Uuid>.Default`, which compares values. The real generator
also cannot draw `(0, 0)` or `(0, 1)`, because the version and variant bits are set. That is why this is Low.
**Fix:** in the queue, use fresh `new Uuid(0L, 0L)` and `new Uuid(0L, 1L)` in place of the two constants (or add
them beside the constants).

Fixed in 3529b8f73: the seam test feeds fresh new Uuid(0L, 0L) and new Uuid(0L, 1L) objects (asserted NotSame as the constants) in place of ZERO_UUID and ONE_UUID; the reference-equality mutant (ReferenceEquals against the two constants in RandomUuid's loop) now turns RandomUuid_Seam_SkipsReservedAndDashContainingCandidates red (net10.0).

### 134-S4-6 (Low, records): deviations S8 has to record that are missing from the Actor's list

- **Where**: `NativeConsumer.cs:2805` (`SnapshotPausePartitions`), `:2764-2780` (`SnapshotSeekPartitions`),
  `SnapshotAssignPartitions` (mock path). The test that pins the first one is
  `PublicConsumerNullPartitionTests.cs:110` (`PauseAndResume_…_AndNothingIsPaused`).
- **What**, two Java-observable differences the list does not have:
  1. **C3 and C4: partitions before the null are not paused or reset.** Java goes through the caller's collection
     in order: `MockConsumer#pause` (`subscriptions.pause`, then `paused.add`),
     `ApplicationEventProcessor#process(PausePartitionsEvent)`, and `SubscriptionState#requestOffsetReset`. So
     `Pause({np-0, null})` pauses `np-0`, then throws. .NET pauses nothing, and the test asserts that. On the mocks
     this also differs from .NET's own answer for a non-null partition that is not assigned. Probe:
     `MockConsumer.Pause({np-0, other-7})` gives the ISE and `Paused() = {np-0}`, but `Pause({np-0, null})` gives
     the ISE and `Paused() = {}`. This is necessary under §4.1 ("before any P/Invoke"), like C2's
     `committed.clear()`, but it is not recorded.
  2. **C2 on a subscribed mock.** `MockConsumer#assign` calls `committed.clear()`, then
     `SubscriptionState#assignFromUser`, whose `setSubscriptionType` throws `IllegalStateException "Subscription
     to topics, partitions and pattern are mutually exclusive"` before any element is read. Probe: on a subscribed
     `MockConsumer`, `Assign({np-0})` gives that ISE (from the core), but `Assign({null})` gives
     `ArgumentNullException [partitions]`. The C2 deviation names only the `committed.clear()`.
- **Fix**: add both to the S8 list, and name the C3/C4 side effect in the test's comment ("a recorded
  difference", as `BeginningOffsets_ANullElement_OnTheMocks_MakesNoNativeCall` does).

Recorded in 090022b4f: dotnet/CLAUDE.md §3: 'A refused call does nothing' (Pause/Resume/SeekToBeginning/SeekToEnd pause or reset none before the null; MockConsumer.Assign keeps its committed offsets) and 'A null partition's error comes first' (a subscribed MockConsumer's Assign gives the ArgumentNullException before Java's mutually-exclusive IllegalStateException). The Pause/Resume test's comment names the recorded difference (202dac86d).

### 134-S4-8 (Low, records): a refused null-key commit does not run the commit callbacks Java's `commit()` runs first

- **Where**: `NativeConsumer.cs:1269-1299` (`CommitArguments`, `SnapshotCommitOffsetsWhenOpen`, `NullCommitKey`)
  and the async `CommitAsyncWithCallback` forms.
- **What**: Java's `commit(CommitEvent)` runs `throwIfGroupIdNotDefined()` and then
  `offsetCommitCallbackInvoker.executeCallbacks()` before the event reaches the background thread that reads the key.
  So a `commitSync` / `commitAsync` with a null key first runs the callbacks of earlier `commitAsync` calls. The
  binding answers the null key with no core call, so they stay queued until the next call that delivers them. This is
  the S4-6 kind (a side effect before the null that §4.1's "before any P/Invoke" does not repeat), and no list has it.
- **Evidence** (probe, real `KafkaConsumer` with `group.id`): `CommitAsync({}, cb)` → `cb` not yet run;
  `Commit({null: 1})` → ANE, `cb` still 0 runs; `Commit({})` → `cb` 1 run. A second `CommitAsync({}, cb2)` followed by
  `Commit({np-0: 1}, 300 ms)` (TimeoutException) runs `cb2`.
- **Fix**: add it to the S8 list with S4-6. (The S5 technique, an empty map sent to the core and the null key
  answered after it, would run them, but it would move the S4-1 checks into the core; not worth it.) Also add the
  three orders ruled below, and the async-producer Task fault of 0caf625ef.

Recorded in 090022b4f: dotnet/CLAUDE.md §3 'A refused call does nothing': on a real consumer a Commit/CommitAsync with a null key runs none of the queued CommitAsync callbacks Java's commit() runs first. The three rulings are recorded too: a timeout under 1 ms is zero ('A zero timeout'); a Java check before an answer thrown at the call is thrown at the call (the five-cases bullet: AsyncKafkaConsumer's group error and negative-target error with a null key); the two CommitAsync(offsets, IAsyncOffsetCommitCallback) orders ('A null partition's error comes first'); and the async producers' Task fault for SendOffsetsToTransaction (the five-cases bullet; ffi-marshalling.md §A5).

### 134-S6-R (records): the S6 differences S8 has to record, verified, in one place

Beside 134-S4-6 and 134-S4-8. Items marked **(new)** are missing from the Actor's list.

1. **Case 4 (an `ArgumentNullException` where Java accepts the null):** A5 on the mocks (Java's mock looks the null up in
   `reassignments`, finds nothing and skips it); A8 on the mocks with one group spec (Java's mock filters with
   `topicPartitions.contains(key)`, which never matches); A11 (Java's `putAll` stores the null key; .NET seeds nothing,
   the map's other keys included, which `MockSeeding_ANullKey_…_AndSeedsNothing` pins); A16 (Java stores the map as
   given).
2. **Thrown at the call where Java fails later:** A3 (Java's request build NPEs inside the network send, which ends the
   admin thread), A6, A7 and **A8 on the real clients (new)**: Java's `ListConsumerGroupOffsetsHandler` constructor
   only stores the specs, and the group's future fails with the NPE when `buildBatchedRequest` reads the partition, as
   for A6 and A7.
3. **A12:** a future already failed with Java's `IllegalArgumentException`, where Java fails it only once the call ends,
   with the call's own error if the call failed. This is the existing "Admin null names" deviation, now for a null
   partition too.
4. **A14 and A15 on a client-built Result:** Java's text as `IllegalArgumentException`, where Java's `get(null)` on the
   immutable map of `PartitionLeaderStrategy.PartitionLeaderFuture` NPEs first. For A15 this is every Java Result,
   because its constructor is package-private. A user-built `ListOffsetsResult`, and Java's mock (a `HashMap`), give
   Java's `IllegalArgumentException`, as .NET does.
5. **The mocks refuse the argument before Java's mock throws `UnsupportedOperationException`:** A1 (a non-empty map), A3,
   A6 (Java's "Not implement yet"), A7, A9, A10, and **(new)** A8 with more than one group spec, and A2 when an entry
   with a timestamp spec comes before the null key in the map's order. Java's `MockAdminClient#listConsumerGroupOffsets`
   throws "Not implemented yet" at the call before it reads any spec when `groupSpecs.size() != 1`, and
   `#listOffsets` throws "Not implement yet" at the call for a `TimestampSpec` entry. Probe (`MockAdminClient`):
   `{g1: [np-0, null], g2: (unset)}` gives `ArgumentNullException [groupSpecs]`; without the null the .NET mock returns
   futures failed with `UnsupportedOperationException "Not implemented yet"`.
6. **A5 order on the real clients:** every element is checked before anything is sent, so a null after an empty-topic
   or negative-partition element is `ArgumentNullException`. Java's validation loop returns the Result, failed with
   `InvalidTopicException`, at the first bad element and never reads the null. Probe: `[("", 0), null]` gives
   `ArgumentNullException [partitions]`; `[("", 0)]` gives a future failed with `InvalidTopicException "The given topic
   name '' cannot be represented in a request."`. The same already holds for a null topic.
7. **(new) A4 on the mocks: the entries before the null key are not applied.** Java's
   `MockAdminClient#alterPartitionReassignments` loops the map. Each entry before the null key stores (or cancels) its
   reassignment, then `partition.topic()` NPEs. The binding refuses before the call, so nothing is applied (the 134-S4-6
   kind). Probe: a mock with topic `np` (one partition, three brokers), `AlterPartitionReassignments({np-0: [1, 2],
   null: null})` (an `EntryMap`, in that order) gives `ArgumentNullException [reassignments]`, and
   `ListPartitionReassignments()` then has 0 entries; the same call without the null leaves 1. In Java a
   `LinkedHashMap` in that order (or a `HashMap` whose bucket 0 holds np-0 first) leaves np-0 stored. Java's real
   client reads every key at the call before it sends, so the real clients match. Optional: pin it in
   `AlterPartitionReassignments_ANullKey_IsArgumentNullException` on the mocks ("…AndNothingIsApplied"), as the seeding
   test does.
8. **(new) The recorded options order now covers an element or key too.** STATUS (M23/P6) records "when options and
   another argument are both null, the other argument's error comes first". With a null options and a null element,
   A3, A6, A7 and A8 report the element (`[partitions]`, `[offsets]`, `[groupSpecs]`), where Java's
   `options.timeoutMs()` NPE at the call comes first and the element fails only later. For A3, Python also checks the
   options first. A1, A2, A4, A5, A9 and A10 match Java, whose key NPE comes before the options. The tests pin the
   .NET order (`Options.Null` rows). S8 should say that the rule includes elements.
9. **134-S4-4 / 134-S4-7:** the `OffsetsForTimes` case-4 entry says "a zero timeout, given or from
   `default.api.timeout.ms`". This is the Actor's item, and it is right: the probe for 134-S4-7 showed `{np-0=null}` for
   the no-timeout form at 0.

Recorded in 090022b4f: dotnet/CLAUDE.md §3: case 4 (A5 and A8 mocks with one spec, A11 seeding nothing, A16) in 'A null partition Java keeps'; thrown at the call where Java fails later (A3, A6-A8 real) in 'Admin null names' and case 3; A12 and A14/A15 in 'Admin null names'; the mocks refusing before Java's UnsupportedOperationException (A1, A3, A6, A7, A9, A10, A8 with several specs, A2 with a timestamp spec first), the A5 order and the options-vs-element order in 'A null partition's error comes first'; A4 on the mocks in 'A refused call does nothing'; the zero timeout from default.api.timeout.ms in 'A null partition Java keeps' (OffsetsForTimes) and 'A zero timeout'.

### 134-S7-R (records): the S7 differences S8 has to record, verified, in one place

Beside 134-S4-6, 134-S4-8 and 134-S6-R. Items marked **(new)** are missing from the Actor's list.

1. **Sorting with a null id (the Actor's item 1, verified; acceptable under D4).** PLAN §3.2 and D4 say "sorting
   with `Comparer<Uuid>.Default` still works". That is wrong, and S8 must not copy it into STATUS or `D/CLAUDE.md`.
   Probe on net8.0 (8.0.30) and net10.0 (10.0.15), list `[(1,0), null, (-1,0)]`:
   - `List.Sort()`, `Array.Sort(array)`, `List.BinarySearch(null)` and also `List.Sort(Comparer<Uuid>.Default)`
     throw `InvalidOperationException` "Failed to compare two elements in the array.", with an inner
     `ArgumentNullException` whose `ParamName` is `other`. The BCL sort helper treats a null comparer and the default
     comparer the same way: it calls `CompareTo` itself.
   - LINQ `OrderBy`, `SortedSet<Uuid>` and a wrapping comparer (`Comparer<Uuid>.Create`, or a lambda over
     `Comparer<Uuid>.Default.Compare`) call `Compare` and put null first.
   - `List.Sort()` accepts some lists that hold a null without calling `CompareTo`: `[null, x]`, or nulls only. Java's
     sort NPEs on both of those.
   - net462 cannot run here (no Mono). The .NET Framework `GenericArraySortHelper<T>.Sort` takes the same direct
     `CompareTo` path for a null or default comparer and wraps the exception the same way, and CI's Windows net462
     leg runs `Sorting_ANullElement_Throws_ButTheDefaultComparerOrdersNullFirst`.

   Java's `Collections.sort` NPEs. Python raises too, but with `AttributeError` ("'NoneType' object has no attribute
   '_most_significant_bits'", from `Uuid._compare`), not `TypeError`. Only two Nones give `TypeError` (probe:
   `sorted([hi, None, lo])` and `sorted([None, hi])` give `AttributeError`; `sorted([None, None])` gives
   `TypeError`). So a throwing sort is Java's and Python's answer, and D4 holds. S8 should record the throwing sort
   and the three non-throwing routes above.
2. **D15 and `TopicMetadataAndConfig` (the Actor's item 2, verified).** `admin/create_topics_result.pyi` types the
   topic id as `Uuid` in the constructor (line 33), the getter (line 37) and the two `topic_id` refinements (lines
   63 and 65). Only the implementation `create_topics_result.py` says `Uuid | None` (constructor and getter). D15's
   rule ("as Python's stubs") therefore gives `Uuid`, which is what the commit keeps, and D15's example list is
   wrong for this member. S8 should record the rule's answer and drop that example from the ledger row 623 text.
3. **(new) D15 gives way to an implemented .NET interface.** `CompareTo(Uuid?)` and `Equals(Uuid?)` (and `==` /
   `!=`, which follow `Equals`) are annotated nullable, although Python's `__lt__(self, other: Uuid)` is not.
   `IComparable<T>.CompareTo(T?)` and `IEquatable<T>.Equals(T?)` force the annotation: a probe class with
   `CompareTo(X other)` / `Equals(X other)` gets CS8767 on both, which is an error under warnings-as-errors. PLAN
   §3.2 already sketches `Uuid?`, and the shape test pins it. S8's D15 bullet should name this exception, so that a
   later review does not flag it (`CompareTo(null)` still throws, D4).
4. **The closed check before the null id (the Actor's item 3, verified).** Java's real client gives no closed
   answer here:
   - U2 NPEs at `topicId.equals(Uuid.ZERO_UUID)` before `AdminClientRunnable#call` sees the closing client.
   - U3 fails the null key's future with `InvalidTopicException` ("The given topic id 'null' cannot be represented
     in a request.") and the others with `IllegalStateException` "Cannot accept new calls when AdminClient is
     closing." through `call.handleFailure`.
   - Java's mock has no closed state (`close(Duration)` is empty), so it answers as when open.

   .NET throws that `IllegalStateException` at the call on all four clients, before it reads the ids. This is
   Python's order: `admin.py` `delete_topics` calls `_check_not_closed()` before `topic_collection_target`. It is
   the existing closed-first deviation, and S8 should say that it also covers a null id.
   `AClosedClient_ReportsItsClosedCheck_BeforeANullId` pins it (mutant: `ThrowIfClosed()` dropped from
   `CheckTopicIds` turns 4 rows red).
5. **(new) A refused call changes nothing (the 134-S4-6 / 134-S6-R item 7 kind).**
   - Java's mock deletes (`deleteTopics`) or describes (`describeTopics`, which decrements
     `fetchesRemainingUntilVisible`) the other ids and fails only the null key's future. A pending
     `timeoutNextRequests` is consumed by that call.
   - Java's real `describeTopics` still sends the request for the other ids.
   - .NET refuses the whole call. The other ids are untouched, and a pending `TimeoutNextRequest` stays for the next
     call.

   Probe (`MockAdminClient`, net8.0 and net10.0): create `t1`, call `TimeoutNextRequest(1)`, then
   `DeleteTopics(OfTopicIds({id, null}))` gives `ArgumentNullException [topics]`. The next
   `DeleteTopics(OfTopicIds({id})).All().Get()` then fails with `ExecutionException(TimeoutException "The mock timed
   out the request.")`, and `t1` is still described. In Java the first call takes the timeout (both keys' futures
   fail with `TimeoutException`), and the second deletes `t1`. `MockDeleteTopics_ANullId_DeletesNothing` pins the
   "deletes nothing" part. For completeness: U3 on the real clients is the "Admin null names" case (Java fails the
   key's future), and U2 on the real clients is case 3. So S8's case-4 list ("U2 and U3 mocks") plus the widened
   "Admin null names" bullet cover all four clients.

Recorded in 090022b4f: dotnet/CLAUDE.md §3: 'Sorting a null Uuid' (the throwing sorts and the three non-throwing routes; D4's sorting sentence corrected in STATUS); the D15 rule's answer for TopicMetadataAndConfig (Uuid) and the IEquatable/IComparable exception in the five-cases bullet, both corrections also in the STATUS entry; the closed check before a null id in 'Admin null names'; a refused DeleteTopics/DescribeTopics changing nothing (the other ids, a pending TimeoutNextRequest) in 'A refused call does nothing'.

### 134-S8-1 (Low, records): a refused mock `Rebalance` or `Assign` skips more of Java's work than the rules say

- **Where**: `dotnet/CLAUDE.md:933-944` (**A refused call does nothing**). Also the comment above
  `PublicConsumerNullPartitionTests.Rebalance_ANullElement_IsArgumentNullException_AndTheAssignmentIsKept` (lines 639-640).
- **What**:
  1. **C13 is not recorded.** PLAN §4.2 C13 marks it "*(deviation: Java's side effects before its NPE are not
     repeated)*", and this bullet is the place for that kind, but it does not name `Rebalance`. Java's
     `MockConsumer#rebalance` clears the buffered records, then calls `onPartitionsRevoked(removed)`, then calls
     `SubscriptionState#assignFromSubscribed`. That calls `PartitionStates#set`, which runs `map.clear()` before
     `update` NPEs on `tp.topic()`. So Java's mock is left with no assignment.
     Probe (`MockConsumer`, net10.0): subscribe with a listener, `Rebalance({np-0})`, `AddRecord(np-0)`, then
     `Rebalance({np-1, null})` gives `ArgumentNullException [newAssignment]`. The listener got no
     `OnPartitionsRevoked`, `Assignment()` is still `[np-0]`, and `Poll` still returns the record. Java's
     listener gets `onPartitionsRevoked([np-0])`, the records are cleared, and `assignment()` is empty.
     The test comment ("refuses it before any side effect, so the assignment is kept") does not say that Java's is
     not kept.
  2. **`Assign` also skips Java's change of the subscription type.** The bullet names only `committed.clear()`.
     Java's `SubscriptionState#assignFromUser` calls `setSubscriptionType(USER_ASSIGNED)` before the loop that NPEs.
     So a fresh Java mock is left in manual-assignment mode, and its next `subscribe` throws
     `IllegalStateException` "Subscription to topics, partitions and pattern are mutually exclusive".
     Probe: on a fresh `MockConsumer`, `Assign({null})` gives the ANE, and `Subscribe({np})` then succeeds.
     Control: after a successful `Assign({np-0})` the same `Subscribe` gives that ISE, so the core mock does track
     the mode.
- **Fix**: add both to the bullet. For example: "`MockConsumer.Rebalance` clears no records, calls no
  `OnPartitionsRevoked` and keeps the assignment. Java's mock clears the records, calls `onPartitionsRevoked` for
  the removed partitions and is left with no assignment." And: "`MockConsumer.Assign` keeps the mock's committed
  offsets and its subscription type. Java's mock clears the offsets and switches to manual assignment first."
  Also name the `Rebalance` difference in the test comment ("a recorded difference"), as the Pause test does.

Fixed in a3b381014: dotnet/CLAUDE.md §3 'A refused call does nothing' now says the consumer mocks' Assign keeps the committed offsets and the subscription type (a later Subscribe succeeds; Java's mock switches to manual assignment first, so its next subscribe throws the 'mutually exclusive' IllegalStateException), and that the consumer mocks' Rebalance clears no records, calls no OnPartitionsRevoked and keeps the assignment (Java's mock clears the records, calls onPartitionsRevoked for the removed partitions and is left with no assignment). The Rebalance test's comment names the difference ('a recorded difference').

### 134-S8-2 (Low, records): two of Java's answers in **Admin null names** are stated wrongly

- **Where**: `dotnet/CLAUDE.md:883-884` and `:889-892`.
- **What**:
  1. **`DescribeProducersResult`.** "as Java's Result over a `HashMap` (its mock's) does" holds only for
     `ListOffsetsResult`. Java's `DescribeProducersResult` constructor is package-private.
     `KafkaAdminClient#describeProducers` builds the Result over `PartitionLeaderFuture#all()`, whose map comes from
     `Collectors.toUnmodifiableMap`. `MockAdminClient#describeProducers` throws `UnsupportedOperationException`
     "Not implemented yet". So every Java `DescribeProducersResult` throws an NPE on `partitionResult(null)`. The
     `IllegalArgumentException` with Java's text is Python's answer (PLAN A15, D8; 134-S6-R item 4: "For A15 this is
     every Java Result").
  2. **"Java's real client reads the ids first".** It reads the partitions first too. Java's real client makes no
     closed check at the call. On a closing client, `DeleteRecords` and `ListOffsets` (the immutable map of
     `PartitionLeaderFuture`), `AlterPartitionReassignments` (`entry.getKey().topic()`), `ListPartitionReassignments`
     (the validation loop), `DescribeProducers` and `AbortTransaction` still NPE at the call. .NET, like Python's
     `_check_not_closed()`, reports the closed client instead. `AClosedClient_ReportsItsClosedCheck_BeforeANullPartition`
     pins that on all four clients. A null name or group id is the same kind of case: Java fails that key's future and
     the closing error fails the others.
- **Fix**: (1) "`ListOffsetsResult.PartitionResult` of a null partition throws Java's `IllegalArgumentException`, as
  Java's Result over a `HashMap` (a user's, or its mock's) does. Java's client-built Result throws
  `NullPointerException` first, from its immutable map. `DescribeProducersResult.PartitionResult` throws the same
  class with Java's text, as Python's does. Every Java `DescribeProducersResult` throws `NullPointerException`." (2)
  "Java's real client reads the argument first: an NPE at the call, or that key's failed future."

Fixed in a3b381014: dotnet/CLAUDE.md §3 'Admin null names': ListOffsetsResult answers as Java's Result over a HashMap (a user's or its mock's); DescribeProducersResult throws IllegalArgumentException with Java's text as Python's does, while every Java DescribeProducersResult throws NullPointerException (only the real client builds one, over an immutable map); and Java's real client makes no closed check at the call and reads the argument first (an NPE at the call, or that key's failed future). Also: 'Both are sealed classes, as Java's' is now 'classes, as Java's (sealed, where Java's Uuid is not final)', in CLAUDE.md and STATUS, and the two long sentences are split at their semicolons.
