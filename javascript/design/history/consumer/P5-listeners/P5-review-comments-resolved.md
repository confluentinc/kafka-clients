# P5: the issues the review resolved (Critic 154)

Copied at the end of P5 (2026-11-27) from `COMMENTS.DONE.154.md` at the repository root: the issues Critic 154
found in its review of P5, each with the fix that resolved it, in issue order. The review took three rounds. Round 1
found 154-1 to 154-6. Round 2 confirmed their fixes and found 154-7, a leak the round-1 code already had, and
154-8. Round 3 confirmed both fixes and found no new issue.

## Issue 154-1: A failed `subscribe` with a listener frees the listener the mock still holds, and `rebalance()` then crashes
- **Commit**: `25596a713` javascript: the consumer's rebalance listeners, their reports and the window
- **File**: `javascript/native/consumer.c:1016` (`subscribe_on_complete`, the release at line 1026); `javascript/test/memory/consumer_callbacks.test.js:47`
- **Severity**: Bug
- **Reference**: P5.md §1, Lifetimes, and §8 risk 3; the FFI's doc of `kafka_consumer_Consumer_subscribe_with_topics_listener` (`rust/target/include/confluent_kafka_bindings.h:37751`: "its `self` must stay alive until a later successful `subscribe_*` replaces it, or the consumer is destroyed"); Java's `SubscriptionState#subscribe`, which calls `registerRebalanceListener` before `setSubscriptionType`, and `MockConsumer#rebalance`
- **Description**: `subscribe_on_complete` releases the subscribe's `Listener` whenever the core answers with an error. But the core's mock registers the listener before the check that fails: `MockConsumer::subscribe_internal_topics` calls `SubscriptionState::subscribe_with_topics`, which runs `register_rebalance_listener(listener)` and then `set_subscription_type(..)?`, in Java's order. The mock's `rebalance()` and `lose_partitions()` read the listener from `SubscriptionState`. So after a subscribe refused with "Subscription to topics, partitions and pattern are mutually exclusive", the core calls the freed `Listener` (`listener_invoke` reads `l->c`):
  ```js
  const mock = new MockConsumer({ offsetResetStrategy: 'earliest' });
  await mock.assign([new TopicPartition({ topic: 't', partition: 0 })]);
  await mock.subscribe({ topics: ['t'], callback: { onPartitionsRevoked() {}, onPartitionsAssigned() {} } })
    .catch(() => {}); // IllegalStateError
  await mock.rebalance([]); // the process dies
  ```
  - It exits 139 (SIGSEGV) in 4 runs of 4, and 138 (SIGBUS) under `MallocScribble=1`. `consumerStats()` shows the listener freed (made 1, freed 1) before the rebalance.
  - The same happens with `subscribe({ topics, callback: L1 })` and a `rebalance`, then a refused `subscribe({ pattern, callback: L2 })`, then a `rebalance`: exit 139.
  - There, Java's mock calls the new listener's `onPartitionsRevoked`, then throws `IllegalArgumentException` "Attempt to dynamically assign partitions while manual assignment in use".
  - No test meets this path. The memory test's "failed" case is a `KafkaConsumer` subscribe that the core refuses before it registers anything (`topics: ['']`), and its mock case fails `assign`.
  - On a `KafkaConsumer` the release is safe today, because the core calls the listener from its app-side copy, which is written only on success (`AsyncKafkaConsumer::subscribe_internal_topics`).
- **Expected**: As the FFI's doc says, a listener given to a subscribe that the core ran stays alive until a later successful subscribe replaces it, a `KafkaConsumer`'s `close()` returns, or the consumer is destroyed. That includes the failed subscribe's own listener, so the mock calls it as Java's does. NOTES.md (line 383) and P5.md say that a failure keeps the failed subscribe's listener too. A memory probe covers a failed mock subscribe, both after `assign()` and topics then pattern, followed by `rebalance()` and `losePartitions()`.
- **Actual**: The failed subscribe's listener is freed while the core's mock still holds it. The next `rebalance()` or `losePartitions()` uses freed memory and crashes the process.
- **Resolution**: Fixed by 6a94c533a (fixup of 25596a713: a failed subscribe the core answered keeps its listener, chained with the consumer's, until a later successful subscribe, a KafkaConsumer's close() or the destroy; a call refused before the core ran lets go of its own; memory probes for a failed mock subscribe after assign() and topics then pattern, each followed by rebalance() and losePartitions()) and 8dabd82fb (fixup of 2135c3365: NOTES.md).

## Issue 154-2: A callback that throws or rejects with `null` is reported as a success
- **Commit**: `25596a713` javascript: the consumer's rebalance listeners, their reports and the window (the carried value), and `e5ae3678b` (the commit callbacks)
- **File**: `javascript/src/internal/consumer_calls.ts:327` (`completion`); `javascript/src/internal/errors.ts:170`
- **Severity**: Bug
- **Reference**: P5.md §1: "A throw, or a rejection, is the callback's error: it crosses the core as a `LocalCallbackError` that carries the value ... so the value comes back as the same object", and the "as itself" rows of its errors' table; JS conventions, Errors (line 122); in Java, `throw null` throws `NullPointerException`, so Java's call fails
- **Description**: `fromNative` returns a carried value as it is, so a carried `null` comes back as `null`. `completion()`'s `done(error)` then reads `error !== null` as "no error". So in the rows where the value comes back with no layer around it, a callback that throws `null`, or rejects with it, makes the call resolve:
  - On a mock, `commitAsync({ offsets, callback: () => { throw null; } })` resolves, and so does a callback that returns `Promise.reject(null)`. Throwing `undefined` or `0` rejects, as it should.
  - On a mock, `rebalance()` resolves when `onPartitionsAssigned` throws `null`.
  - On a mock, `losePartitions([tp])` resolves when `onPartitionsLost` rejects with `null`, although the core stopped at the error: the partition is still assigned afterwards.
  - On a `KafkaConsumer` against a broker, `commitSync()` resolves after running a queued commit callback that throws `null`.
- **Expected**: The call rejects with `null`, the value the callback threw, as it does for any other value. `done` needs a way to tell a carried `null` from no error, for example the native side passing whether there is one.
- **Actual**: The call resolves. The callback's failure is lost, and the caller can't tell that the commit, the rebalance or the loss stopped.
- **Resolution**: Fixed by 6a94c533a (fixup of 25596a713: fromNative gives a carried null as the private CARRIED_NULL, which callbackError puts back as null; completion() is unchanged; the listener rows test null), 4666d3011 (fixup of e5ae3678b: the commit callback rows test null, failing on a resolution), 47386e9cc (fixup of a07db258f: the mock tests throw and reject null and undefined) and 8dabd82fb (NOTES.md).

## Issue 154-3: Contexts kept until the destroy keep their consumer alive: refused or pending commit callbacks, and a mock's listener, leak
- **Commit**: `e5ae3678b` javascript: the consumer's commit callbacks; and `25596a713` (the mock's listener, kept until the destroy)
- **File**: `javascript/native/consumer.c:571` (`commit_callback_new`, whose context is freed only by its report or at line 2341) and `:1491`; `javascript/native/consumer.c:94` and `:2207` (the mock's `Listener`)
- **Severity**: Design Flaw
- **Reference**: P5.md §1, Lifetimes ("A commit callback is kept until it reported, or until the destroy"; "A `MockConsumer` keeps it until the destroy"), and §8 risk 3 ("one dropped too late is a leak"); the core's `commit_async_internal` (`rust/src/consumer/internals/async_kafka_consumer.rs:6521`), which registers the callback only after `ensure_open()` and `commit_inner()` succeed; in Java, a refused `commitAsync`'s callback and a closed mock's listener are garbage
- **Description**: Each context holds the user's function by a strong `napi_ref`, which is a GC root. A commit callback the core never runs, and a mock's listener, are freed only by the destroy. The destroy runs from the handle object's finalizer, so it never comes while the function can reach that object. The rule is followed as written; the leak is in how "kept" is held. Three probes (`--expose-gc`, deltas of `consumerStats()`):
  1. When the core refuses a `commitAsync({ callback })` (busy, closed, no `group.id`, a null map), it never runs the callback, but the callback's context stays. 200 calls made while a `poll()` was pending, each refused with `ConcurrentModificationError`, left 200 contexts alive after the poll, until the destroy.
  2. If that callback references its consumer, as a callback that does anything usually does, the destroy never comes. A closed `KafkaConsumer` given a refused `commitAsync({ callback })` that captures it is never freed (`consumersFreed` +0; +1 with a callback that captures nothing). The same happens after `close({ timeout: 0 })` with a `commitAsync({ offsets, callback })` still pending (+0, against +1).
  3. A closed `MockConsumer` whose listener references the mock is never freed. This is consumer-threading.md §31's pattern, `await mock.commitSync()` in `onPartitionsRevoked`: 0 mocks of 3 freed, with 3 listeners alive, against 3 of 3 freed for a listener that doesn't reference its mock.

  The memory tests pass because their callbacks reference nothing.
- **Expected**: The core never runs a refused commit's callback, so its JS function can go once the commit's completion reports an error. The small C context may stay until the destroy if the FFI's doc requires it ("once passed to a `commit_async_*`, until `onComplete` fired"), or the FFI's owner can document that a failed `commit_async_*` never fires `onComplete`. A function kept for a closed client (a mock's listener, a commit still pending at close) is held through the handle object, an edge the GC can collect with the cycle, and not by a strong native reference. Memory probes use callbacks that reference their consumer.
- **Actual**: Memory grows by one context per refused commit for as long as the consumer is open, and a closed consumer or mock is never freed when its callback references it.
- **Resolution**: Fixed by 6a94c533a (fixup of 25596a713: listener functions held through the handle object's `callbacks` object, read through weak references; a probe of closed mocks and a KafkaConsumer whose listeners reference them), 4666d3011 (fixup of e5ae3678b: a commit callback's function held the same way and dropped once it reported or its commit failed, through its own completion commit_async_on_complete; its context held by the core's registration until the report or the destroy, and by the call until its answer; a trampoline with no function reports at once, running no JS; probes of 200 refused commits, a closed consumer's refused commit and a commit pending at close(0), each callback referencing its consumer) and 8dabd82fb (NOTES.md says why).

## Issue 154-4: The memory test's count of a pending commit callback passes with 0
- **Commit**: `e5ae3678b` javascript: the consumer's commit callbacks
- **File**: `javascript/test/memory/consumer_callbacks.test.js:156`
- **Severity**: Test Gap
- **Reference**: P5.md §2 (`test/memory/`) and §8 risk 3 ("a listener dropped too early is a use-after-free")
- **Description**: `assert.ok(out.counts.kafkaPending <= 1)` also passes when the pending commit callback's context has already been freed (0), which is the use-after-free that this probe exists to catch. With no broker, nothing runs the callback between `commitAsync()` and `live()`, and the count is 1 in 5 runs of 5.
- **Expected**: `assert.equal(out.counts.kafkaPending, 1)`.
- **Actual**: `<= 1`.
- **Resolution**: Fixed by 4666d3011 (fixup of e5ae3678b): assert.equal(out.counts.kafkaPending, 1).

## Issue 154-5: The timed-out close's cause has the core's message, not Java's, and the test doesn't say so
- **Commit**: `e5ae3678b` javascript: the consumer's commit callbacks
- **File**: `javascript/test/unit/consumer/kafka_consumer.test.js:1109`; `javascript/design/current/consumer/NOTES.md`
- **Severity**: Test Gap
- **Reference**: Java's `AsyncKafkaConsumer#close(Duration,GroupMembershipOperation,boolean)`, `#awaitPendingAsyncCommitsAndExecuteCommitCallbacks` and `ConsumerUtils#getResult(Future,long)`; P3's 152-5 (the tests that assert the core's text where Java's differs say so)
- **Description**: Checked against Java, as the brief asked. The shape is Java's:
  - With a zero timer, Java's close times out on the last pending async commit, where `getResult` throws `new TimeoutException(e)`.
  - It keeps that error as the first one, since the auto-commit's own failure is swallowed in `commitSyncAllConsumed`.
  - It throws `KafkaException("Failed to close kafka consumer", <it>)`.
  - The binding's cause is Kafka's `TimeoutError` from `common/errors` (probed: `instanceof` it, and not the root's `TimeoutError`).

  The message differs. Java's is the `toString()` of the JDK's message-less `TimeoutException`, "java.util.concurrent.TimeoutException". The core's is its own `PENDING_ASYNC_COMMIT_TIMEOUT_MSG`, "Timed out waiting for last pending async commit to complete". The test pins the core's text under the comment "as Java's close records the timeout of the last pending async commit", without saying the text is the core's, and checks no class. NOTES.md doesn't record the difference.
- **Expected**: The test says the text is the core's and Java's is "java.util.concurrent.TimeoutException", as P3's timeout tests do, and asserts that the cause is a `common/errors` `TimeoutError`. NOTES.md records the difference, or PLAN §6 gets an ask if the user wants Java's text.
- **Actual**: The core's text is pinned as if it were Java's, and the cause's class isn't checked.
- **Resolution**: Fixed by 4666d3011 (fixup of e5ae3678b: the test says the text is the core's and Java's is "java.util.concurrent.TimeoutException", and asserts the cause is common/errors' TimeoutError) and 8dabd82fb (fixup of 2135c3365: NOTES.md records the difference).

## Issue 154-6: The docs say a mock refuses its own calls inside its callbacks, but six of them are silently ignored
- **Commit**: `2135c3365` javascript: the consumer's notes, package docs and API report for listeners and commit callbacks
- **File**: `javascript/src/consumer/index.ts:104`; `javascript/design/current/consumer/NOTES.md:379`
- **Severity**: Behavior Mismatch
- **Reference**: P5.md §1 (the mock's window, ask 16); P4's ask 8 (`with_mock_void`), which the first bullet of the same list states ("ignored", while another call on the mock is pending)
- **Description**: The new bullet says that inside a listener or commit callback the mock runs, "its other waiting `Consumer` methods reject with `UnsupportedOperationError`, and `poll()`, `subscribe()`, `assign()`, `unsubscribe()`, `subscription()` and the mock's own calls reject or throw with `ConcurrentModificationError`". NOTES.md says that those calls "are refused with `ConcurrentModificationError`". A probe inside a mock's `onPartitionsAssigned`, run by `rebalance()`, found otherwise:
  - `updateEndOffsets()`, `updateBeginningOffsets()`, `setPollException()` and `scheduleNopPollTask()` return normally and are dropped. After the rebalance, `endOffsets([tp])` says "The partition test-0 does not have an end offset." and `beginningOffsets([tp])` "... does not have a beginning offset.". By P4's list, `updateDurationOffsets()` and `setOffsetsException()` are dropped the same way.
  - `addRecord()`, `setMaxPollRecords()`, `rebalance()` and `losePartitions()` do throw or reject with `ConcurrentModificationError`.
  - `closed()`, `lastPollTimeout()`, `metrics()` and `wakeup()` answer.
  - `close()` rejects with `ConcurrentModificationError`, not with the `UnsupportedOperationError` the sentence implies for the other waiting methods.

  So a test that seeds offsets from a mock's listener gets nothing, and no error, where the docs promise a `ConcurrentModificationError` (and Java's mock applies the offsets). The mock's window test covers none of the mock's own calls.
- **Expected**: The bullet and NOTES.md say which of the mock's own calls throw, which are ignored (the six) and which answer, and that `close()` is refused with `ConcurrentModificationError`. A test pins the ignored ones inside the window.
- **Actual**: "the mock's own calls reject or throw with `ConcurrentModificationError`".
- **Resolution**: Fixed by 47386e9cc and b1aed6f03 (fixups of a07db258f: a test inside the mock's listener pins the six ignored setters, the refused addRecord(), setMaxPollRecords(), updatePartitions(), rebalance(), losePartitions() and close(), and the answering closed(), lastPollTimeout(), metrics() and wakeup()) and 8dabd82fb (fixup of 2135c3365: the package docs and NOTES.md list them).

## Issue 154-7: The value a callback throws is held by a strong native reference until a later sweep, so a closed consumer whose last callback threw a value that reaches it is never freed
- **Commit**: `25596a713` javascript: the consumer's rebalance listeners, their reports and the window (`callback_value_error`, which carries the value on the consumer's `injected` list); the fix-ups `6a94c533a` and `4666d3011` leave it as it was
- **File**: `javascript/native/consumer.c:354` (`callback_value_error`), the sweeps at `:254` (`mock_of`), `:532` (`listener_invoke`) and `:621` (`commit_on_complete`), and the release at `:2486` (the destroy); `javascript/native/mock_values.c:162` (`injected_error_carry`'s strong `napi_create_reference(env, holder, 1, ...)`)
- **Severity**: Design Flaw
- **Reference**: 154-3 and its Expected (a value kept for a closed client is held through the handle object, not by a strong native reference; memory probes with callbacks that reference their consumer); P5.md §8 risk 3 ("one dropped too late is a leak")
- **Description**: The fix-ups hold the listener's and commit callback's functions through the handle object. But the values those callbacks throw or reject with still cross through the carrier: an `InjectedError` on the consumer's `injected` list, which holds the value by a strong `napi_ref`, a GC root. The list lets go of a value only in a sweep or at the destroy. A sweep runs in a mock call (`mock_of`) or in the next listener or commit callback run, and the destroy runs from the handle object's finalizer. So when the last callback a consumer runs throws a value that reaches the consumer, the destroy never comes. That is the case for a value that references the consumer, and for an `Error` made inside a callback that closes over its consumer, since V8's captured stack trace holds the callback's function and so its closure. Probes (`--expose-gc`, deltas of `consumerStats()`):
  - **`KafkaConsumer`, no broker.** The probe runs `commitAsync({ offsets: new TopicPartitionMap(), callback })`, where the callback is `({ exception }) => { if (exception === null) throw new Error('could not record the commit of ' + kc.groupMetadata().groupId()); }`, then `commitSync({ offsets: new TopicPartitionMap() })`, which rejects with that error as itself, then `close({ timeout: 0 })`. Result: 0 consumers freed of 1, and 1 carried value alive. With a pre-made error that references nothing: 1 of 1 freed, 0 alive.
  - **`MockConsumer`.** A listener's `onPartitionsAssigned` throws `{ mock }`, or a `new Error(...)` made inside the listener, so `rebalance()` rejects with it as itself, and the mock is then closed. Result: 0 mocks of 3 freed, 3 carried values alive. With a pre-made error: 3 of 3 freed.
  - **A random mix.** A random mix of P5's mock operations, under `MallocScribble=1` (no crash), leaves 2 mocks of 30 unfreed for some seeds, with their listeners and commit contexts. One mock call after each close, `mock.closed()`, which sweeps the list, frees all 30 for the same seeds.

  The memory tests' thrown values don't reach their consumer, so they pass.
- **Expected**: A carried value doesn't stay behind a strong native reference once the core has delivered it. Either the value is held through the handle object, with the `InjectedError` keeping a weak reference, as `Held` does for the functions; or the list is swept in the completions that deliver an error (at least `close_on_complete`). Either way, a closed consumer whose last callback threw a value that reaches it is collected. A memory probe throws such a value from a listener and from a commit callback. P4's injected errors (`setPollException()` and the others) share this list. A change to the shared carrier or to P2's or P3's completions is a new commit, not a fix-up.
- **Actual**: The value stays strongly held until a later sweep or the destroy, and a closed consumer whose last callback threw a value that reaches it is never freed.
- **Resolution**: Fixed by c19b52443 (new commit): the carrier (native/mock_values.c) takes a `keeper`, holding the value as its element through a weak reference; the consumer passes its handle object's `carried` object, so a carried value is collected with the handle object, while P4's injected mock errors still come back as the same instance, once; the producer's mock passes none. Probes: a mock's listener and commit callback and a KafkaConsumer's commit callback throwing an Error made inside them or a value referencing the consumer, all freed after close (fails without the fix: 0 of 5 freed); a broker probe of a KafkaConsumer listener's thrown Error, freed. NOTES.md's paragraph on GC roots covers carried values.

## Issue 154-8: `subscribe_on_complete` copies P2's test of a call the core never ran
- **Commit**: `6a94c533a` fixup! javascript: the consumer's rebalance listeners, their reports and the window
- **File**: `javascript/native/consumer.c:1110` (`subscribe_on_complete`), a copy of `javascript/native/consumer.c:2346` (`close_on_complete`, from P2's `4ce77642f`)
- **Severity**: Design Flaw
- **Reference**: `.claude/rules/definition-of-done.md` #6; the round-2 brief ("DoD #6")
- **Description**: The fix's `refused` test is P2's, line for line: the `kafka_common_Error__enum_err` read of the code through `error_warn`, then `code == kafka_common_Error_LOCAL_CONCURRENT_MODIFICATION || code == kafka_common_Error_LOCAL_NULL_POINTER`. Both copies decide whether the core ran the call, and so a lifetime: whether the close released the listener (151-1 was a bug in exactly this decision), and whether the subscribe's listener may be freed (154-1). A code added to one has to be added to the other.
- **Expected**: One helper, such as `static int refused_before_core(napi_env env, Consumer* c, const kafka_common_Error_t* error)`, used by both completions. It changes P2's `close_on_complete`, so it is a new commit, not a fix-up.
- **Actual**: Two copies.
- **Resolution**: Fixed by 1f4072b7f (new commit): refused_before_core, one helper that close_on_complete and subscribe_on_complete both call.
