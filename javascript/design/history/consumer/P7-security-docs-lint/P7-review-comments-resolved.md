# P7: the issues the review resolved (Critic 156)

Copied at the end of P7 (2026-11-28) from `COMMENTS.DONE.156.md` at the repository root: the issues Critic 156
found in its review of P7, each with the fix that resolved it, in issue order. The review took seven rounds.
- Round 1 found 156-1 to 156-5, among them that ask 17's premise was wrong: Java's own `close()` also runs
  `onPartitionsLost` after `unsubscribe()`.
- Round 2 confirmed those fixes and found 156-6 and 156-7. Round 3 found no new issue.
- The final gate's first run then failed a P5 commit-callback test over SSL, a race in the test. Rounds 4 to 6
  reviewed its fixes and found 156-8 to 156-10, and round 7 found no new issue.

## Issue 156-1: Ask 17's premise is wrong: Java's own `close()` after `unsubscribe()` runs `onPartitionsLost` too
- **Commit**: `27be0a9ee` javascript: the consumer's broker tests over SSL and SASL_SSL, and its security cases (the test); `c8bb8c669` javascript: the consumer in the README, the migration guide and the CHANGELOG, and examples/consumer.js (the docs)
- **File**: `javascript/test/integration/consumer.test.js:498-544`, `javascript/README.md:1097-1099`, `javascript/CHANGELOG.md:383-384`
- **Severity**: Behavior Mismatch
- **Reference**: `org.apache.kafka.clients.consumer.internals.AbstractMembershipManager#clearAssignmentAndLeaveGroup` and `#clearAssignment`; `org.apache.kafka.clients.consumer.internals.AsyncKafkaConsumer#unsubscribe`, `#resetGroupMetadata` and `#runRebalanceCallbacksOnClose`; P7.md items 4 and 16; PLAN §6 ask 17
- **Description**: The pinned test, its name and comments, the README's Limitations bullet and the CHANGELOG's deviation entry all say that Java's `close()` runs no listener once `unsubscribe()` has run `onPartitionsRevoked`, because `leaveGroup` clears the assignment snapshot with `notifyAssignmentChange(Collections.emptySet())`. Java's code path does not clear it:
  - For a member in the group, `leaveGroup(true)` runs the revoke callback and then `clearAssignmentAndLeaveGroup()`. That method calls `subscriptions.unsubscribe()` first, which sets the subscription type to `NONE`, and only then `clearAssignment()`. So `clearAssignment()`'s guard `subscriptions.hasAutoAssignedPartitions()` is false, and its `notifyAssignmentChange(Collections.emptySet())` is skipped.
  - The other `notifyAssignmentChange(emptySet())` is in `leaveGroup`'s `isNotInGroup()` branch, which a member in the group doesn't take. So `AsyncKafkaConsumer`'s `groupAssignmentSnapshot` keeps the revoked partitions.
  - `AsyncKafkaConsumer#unsubscribe` ends with `resetGroupMetadata()`, which sets the generation to `-1`.
  - `close()` always runs `runRebalanceCallbacksOnClose()`. The snapshot isn't empty and `memberEpoch` is `-1`, so it calls `invokePartitionsLost(...)`. `SubscriptionState#unsubscribe` doesn't clear the registered listener, so the user's `onPartitionsLost` runs.
  - The core translates the same order (`clear_assignment_and_leave_group` in `rust/src/consumer/internals/consumer_membership_manager.rs`: `subs.unsubscribe()`, then `clear_assignment()` with the same `has_auto_assigned_partitions()` guard). That is why the core calls `onPartitionsLost` here: it does what Java does.

  I probed Java's own client: the `kafka-clients-4.3.1.jar` the test broker's image ships, with the KIP-848 protocol, against a plaintext broker. The probe subscribes with a listener, polls until assigned, then calls `unsubscribe()` and `close()`. All 3 runs printed `after unsubscribe(): [assigned [t-0], revoked [t-0]]` and `after close(): [assigned [t-0], revoked [t-0], lost [t-0]]`. These are the calls the pinned test asserts for this client. The 4.4.0 sources in `kafka/` have the same code.

  This premise first came from P5's review (the broker probe saw the core's calls, and the Java side was read from `leaveGroup` alone). P7.md item 4 and PLAN §6 ask 17 carry it, so the Manager's files need the correction too.
- **Expected**:
  - The test keeps its assertions, but its name and comments describe them as Java's behaviour: Java's `close()` runs `onPartitionsLost` here too. A core that stops doing so is the one that should fail it.
  - The README's Limitations bullet and the CHANGELOG's deviation entry go, since this is no difference from Java.
  - Ask 17 is corrected in PLAN §6 and withdrawn before it reaches the FFI's owner. Acted on, it would make the core differ from Java.
- **Actual**:
  - The test is named "...where Java's close() runs no listener". Its comments say "Java's: [['assigned', [tp]], ['revoked', [tp]]], and no more." and "a core that does as Java does fails it on purpose".
  - The README says "where Java's `close()` runs no listener".
  - The CHANGELOG lists the behaviour under "Deviations from Java's consumer".
- **Fixed in**: `1f92325e9` (fixup of `27be0a9ee`: the test renamed to "... as Java's close() does", its comments citing `AbstractMembershipManager#clearAssignmentAndLeaveGroup`, `#clearAssignment`, `AsyncKafkaConsumer#unsubscribe` and `#runRebalanceCallbacksOnClose`, the assertions kept) and `5e3011dbd` (fixup of `c8bb8c669`: the README's Limitations bullet and the CHANGELOG's deviation entry removed). I read the Java code path myself and confirmed it. NOTES.md never called it a difference. Note: `1f92325e9` was committed with only its subject line (`git commit --fixup` with an empty message); its reason is here.

## Issue 156-2: The README's second anti-pattern describes a hazard the consumer as built doesn't have
- **Commit**: `c8bb8c669` javascript: the consumer in the README, the migration guide and the CHANGELOG, and examples/consumer.js
- **File**: `javascript/README.md:952-956`
- **Severity**: Behavior Mismatch (the docs against the code)
- **Reference**: P7.md item 12, and the docs being accurate to the code as built; `NOTES.md`, "Liveness"; JS conventions, Future improvements (Q49)
- **Description**: The bullet says "a closed consumer that is garbage collected while one of its calls still has an answer queued runs that answer's JavaScript during the collection, and may hold the thread for up to 5 seconds while the client stops". In this binding neither can happen.
  - Each waiting call's context holds its `done` function by a strong reference: `call_ctx_new`, and the `napi_create_reference(env, ..., 1, &ctx->done)` calls of `js_consumer_poll`, `subscribe_ctx_new`, `js_consumer_commit_async` and `js_consumer_close` in `native/consumer.c`. Only the call's completion deletes that reference.
  - `done` reaches the handle object through its closure (`completion(start)` in `src/internal/consumer_calls.ts`: the executor keeps `start`, and `start` keeps the handle). So the handle object, whose finalizer destroys the consumer, stays reachable until the answer has run.
  - That is the "lean" of the conventions' Q49 entry, "a reference that each pending call holds", which the code already has.
  - The finalizer sets `c->queue.finalizing = 1` before `consumer_destroy`, so nothing the destroy completes runs JavaScript (`void_on_complete` and the others test `finalizing`; `NOTES.md`, "Liveness").

  I probed the built package with `node --expose-gc`. Each round closes a consumer, makes a `position()` call after `close()`, drops the call and the consumer, leaves the answer queued (no turn of the event loop), and runs two collections.
  - In all 5 rounds, the consumer was freed +0 during the collections, and +1 only after the answer ran on the event loop.
  - The call's `.catch` never ran during a collection.
  - Control: a closed consumer with no call pending is freed by a collection (+1).
- **Expected**: The README says what the consumer does: a consumer with a call still waiting isn't collected until that call's answer has run, so no JavaScript runs during a collection. The advice to await every call can stay, for the reason the Commits section gives (a call not awaited keeps the consumer busy). P7.md item 12 and PLAN risk 9 asked for this anti-pattern, so the Manager settles the wording with the user.
- **Actual**: The bullet presents the Q49 hazard as this client's behaviour.
- **Fixed in**: `733cc93b2` (fixup of `c8bb8c669`): the bullet now says a consumer with a call still waiting stays reachable until that call's answer has run, so no JavaScript runs during a collection, and keeps the advice to await every call, for the reason the Commits section gives. Checked: `call_ctx_new` and the `napi_create_reference(..., 1, &ctx->done)` of `js_consumer_poll`, `subscribe_ctx_new`, `js_consumer_commit_async` and `js_consumer_close`; `completion(start)` in `src/internal/consumer_calls.ts`; `consumer_finalize` sets `queue.finalizing` before `consumer_destroy`.

## Issue 156-3: `usage.ts` compiles no `MockConsumer` form of the `Consumer` methods, 7 `KafkaConsumer` forms, and 15 value-class methods
- **Commit**: `b5807ae69` javascript: every form of the consumer's public API in the typing test
- **File**: `javascript/test/typing/usage.ts:1460-1567`
- **Severity**: Test Gap
- **Reference**: P7.md item 5 and §3 ("with each of its overload signatures compiled at least once"); JS conventions, Tests ("every overload signature compiles and works")
- **Description**: The new section makes the `Consumer` calls through `everyConsumerForm(consumer: Consumer<string, number>)`. Each call there resolves to the interface's signature in `dist/consumer/consumer.d.ts`, not to the class's.

  Passing a `KafkaConsumer` and a `MockConsumer` to that function only checks that each class is assignable to `Consumer`. TypeScript checks overloaded methods loosely here: method parameters are bivariant, and each target signature needs only one related source signature. A zero-argument overload (`commitSync()`, `commitAsync()`, `listTopics()`, `close()`) is related to every target signature with the same return type. So the commit message's "each class's own signatures must satisfy every overload the interface declares" doesn't hold.

  I measured it with the TypeScript compiler API, resolving every call and `new` of `usage.ts` (outside the `@ts-expect-error` lines) to its declaration:
  - **`MockConsumer`**: 60 declared signatures are never reached. These are every `Consumer` form on a `MockConsumer`: all six `subscribe`, all six `commitSync` and all four `commitAsync`; `seek`, `seekToBeginning`, `seekToEnd`, `position`, `committed`, `partitionsFor`, `listTopics`, `pause`, `resume`, `offsetsForTimes`, `beginningOffsets`, `endOffsets` and `currentLag`; `assignment`, `subscription`, `paused`, `groupMetadata`, `metrics`, `wakeup` and `unsubscribe`; `assign({ partitions })`, `poll({ timeout })`, and both `close` forms.
  - **`KafkaConsumer`**: 7 are never reached: `seekToBeginning(partitions)`, `seekToEnd({ partitions })`, `committed(partitions)`, `pause(partitions)`, `resume({ partitions })`, `beginningOffsets({ partitions, timeout })` and `endOffsets({ partitions })`.
  - **The value classes**: 15 methods are never called: `OffsetAndMetadata`'s `toString` and `toJSON`; `OffsetAndTimestamp`'s `toString`, `hashCode` and `toJSON`; `SubscriptionPattern`'s `toString`, `hashCode` and `toJSON`; `ConsumerGroupMetadata`'s `toString` and `toJSON`; `GroupProtocol`'s `toString` and `ordinal`; and `GroupMembershipOperation`'s `name`, `toString` and `toJSON`.
  - **The consumer's errors**: `CommitFailedError`, `RetriableCommitFailedError` and `InvalidOffsetError` aren't constructed at all, if the heading's "every form of the consumer's public API" covers them.

  Two mutations show what the gap lets through:
  - I deleted `commitSync(timeout)`, `commitSync(offsets)`, `commitAsync(callback)` and `listTopics(timeout)` from `MockConsumer`. Deleted in `src/consumer/mock_consumer.ts`, the library still builds (`implements Consumer` passes). Deleted in `dist/consumer/mock_consumer.d.ts`, `usage.ts` still type-checks (exit 0), while a user's `mock.commitSync(offsets)`, `mock.commitSync(1000)` and `mock.listTopics(1000)` fail with TS2769.
  - I narrowed the bare forms of `MockConsumer`'s `pause`, `seekToEnd` and `committed` to `TopicPartition[]`. `usage.ts` still type-checks, and a user's `mock.pause(new Set([tp]))` doesn't. `check-api` flags only a change from the committed report, not a form that was wrong from the start.

  Every one of these forms compiles today when called on the concrete classes (a probe file with each call: exit 0). So this is missing coverage, not a type defect.
- **Expected**:
  - Each form is called on a `MockConsumer`-typed receiver and on a `KafkaConsumer`-typed one, as the earlier `consumerUsage()` section does for some `KafkaConsumer` forms.
  - A form that takes an iterable is called with one that isn't an array.
  - The listed value-class methods are compiled.
- **Actual**: As listed above.
- **Fixed in**: `bfccadb75` (fixup of `b5807ae69`): every `Consumer` form called on a `KafkaConsumer`-typed and a `MockConsumer`-typed receiver (a `Set` where an iterable is taken), the 15 value-class methods, `CommitFailedError`'s and `RetriableCommitFailedError`'s constructor forms, and `InvalidOffsetError` refused as abstract. Shown to bite: deleting `MockConsumer`'s `commitSync(offsets)` from `dist/consumer/mock_consumer.d.ts` in a scratch copy fails `usage.ts` with TS2769; the earlier `usage.ts` passed with the same deletion.

## Issue 156-4: P7.md item 6's check of every consumer export's TSDoc against its Javadoc wasn't done, and a spot-checked member misses part of Java's contract
- **Commit**: `b5807ae69` javascript: every form of the consumer's public API in the typing test (the typing and TypeDoc commit; no commit in the range touches `src/`)
- **File**: `javascript/src/consumer/consumer.ts:319-345` (`poll`), `javascript/src/consumer/consumer.ts:1609-1633` (`close(options)`)
- **Severity**: Missing Requirement
- **Reference**: P7.md item 6; `javascript/CLAUDE.md` line 188 ("Java's contract comes first") and line 189; `org.apache.kafka.clients.consumer.KafkaConsumer#poll(Duration)` and `#close(CloseOptions)`
- **Description**: The Actor reports a spot check only (`wakeup`, `close` and the listener docs). Two members show what the full check would find.
  - **`poll`**: Java's Javadoc says "Note that this method may block beyond the timeout in order to execute custom ConsumerRebalanceListener callbacks." It also says "The last consumed offset can be manually set through seek(...) or automatically set as the last committed offset for the subscribed list of partitions". The TSDoc has neither sentence, although a listener's Promise holds `poll()` past its timeout in this client too (line 148).
  - **`close(options)`**, one of the spot-checked members: Java's Javadoc has three passages the TSDoc lacks:
    - "If auto-commit is enabled, this will commit the current offsets if possible within the timeout."
    - "The actual maximum wait time is bounded by the request.timeout.ms setting ... Note that the execution time of callbacks (such as OffsetCommitCallback and ConsumerRebalanceListener) does not consume time from the close timeout."
    - "This close operation will attempt all shutdown steps even if one of them fails ..."

    The check has to answer whether the core keeps "callbacks don't consume the close timeout". If it doesn't, the difference goes in `NOTES.md` and `@remarks` (line 189).

  Both TSDocs are well under 150% of the Javadoc's words, so the size limit didn't force these cuts.
- **Expected**: Every consumer export is checked against its Javadoc, as item 6 says. Each missing contract sentence is restored, or its difference recorded.
- **Actual**: Only a spot check was made, and it missed `close(options)`'s missing passages.
- **Fixed in**: `4fbbbec61` (a new commit: the TSDoc is earlier phases' code). Every public member of src/consumer was checked against its Javadoc; the commit body lists what was restored. The core keeps the close() callbacks sentence for the step it covers: `close_internal` caps the timeout at request.timeout.ms, and `leave_group_on_close`'s wait is not bounded by the close deadline. So no new deviation.

## Issue 156-5: The security tests have two copies of the poll-until-rejected loop
- **Commit**: `27be0a9ee` javascript: the consumer's broker tests over SSL and SASL_SSL, and its security cases
- **File**: `javascript/test/integration/_security.js:348-357`, `javascript/test/integration/sasl.test.js:265-276`
- **Severity**: Design Flaw (DoD #6, low)
- **Reference**: `.claude/rules/definition-of-done.md` #6; the precedent of P5's 154-8
- **Description**: The shared helper `pollFailingAuthentication` inlines a loop: a 60 s deadline, `poll(500)`, an assertion that no record came back, and a return of the first rejection. `pollUntilRejected` in `sasl.test.js` repeats that loop line for line. This commit adds both.
- **Expected**: One `pollUntilRejected(consumer, what)` in `_security.js`. `pollFailingAuthentication` calls it after its `assign()`, and `sasl.test.js` imports it.
- **Actual**: Two copies.
- **Fixed in**: `3a1369bc7` (fixup of `27be0a9ee`): `pollUntilRejected(consumer, what)` lives in `_security.js`; `pollFailingAuthentication` calls it after its `assign()`, and `sasl.test.js` imports it.

## Issue 156-6: The TSDoc check still misses two passages of Java's contract
- **Commit**: `4fbbbec61` javascript: the consumer's TSDoc carries the parts of Java's contract it had dropped
- **File**: `javascript/src/consumer/consumer.ts:229-258` (`subscribe({ pattern })`), `javascript/src/consumer/consumer.ts:1190-1194` (`partitionsFor({ topic, timeout })`)
- **Severity**: Missing Requirement (low; the rest of 156-4 is resolved)
- **Reference**: P7.md item 6; issue 156-4 ("each missing contract sentence is restored, or its difference recorded"); `org.apache.kafka.clients.consumer.KafkaConsumer#subscribe(SubscriptionPattern)` and `#partitionsFor(String,Duration)`
- **Description**: The commit says every public member of `src/consumer` was checked against its Javadoc. I compared each `Consumer` overload's TSDoc with the Javadoc of its Java form, sentence by sentence. Two passages are still missing:
  - **`subscribe({ pattern })`** (Java's `subscribe(SubscriptionPattern)`) lacks the short-hand paragraph that the commit restored on `subscribe({ topics })`. The paragraph says: "This is a short-hand for subscribe(... ConsumerRebalanceListener), which uses a no-op listener. If you need the ability to seek to particular offsets, you should prefer ..., since group rebalances will cause partition offsets to be reset. You should also provide your own listener if you are doing your own offset management since the listener gives you an opportunity to commit offsets before a rebalance finishes."
  - **`partitionsFor({ topic, timeout })`**'s `@returns` stops at "...auto-created in the background." It lacks the sentence that follows in Java and that the untimed form keeps: "Callers should not assume the topic does not exist based solely on an empty result."

  The size limit didn't force either cut. In a scratch copy with both passages restored (the pattern one linking `subscribe({ pattern, callback })`), ESLint, `doc-size` included, still passes on `consumer.ts`.
- **Expected**: Both passages are restored.
- **Actual**: Both are still missing.
- **Fixed in**: `0b6551118`, a new commit (the lines blamed to earlier phases, `dd2046786` and `1513ab98c`, not to `4fbbbec61`): `subscribe({ pattern })` gets the short-hand paragraph, linking `subscribe({ pattern, callback })`, and the timed `partitionsFor({ topic, timeout })`'s `@returns` gets "Callers should not assume the topic does not exist based solely on an empty result." ESLint, `doc-size` included, passes.

## Issue 156-7: After the autosquash, the typing commit's message still states the claim 156-3 showed false
- **Commit**: `b5807ae69` javascript: every form of the consumer's public API in the typing test, with its fix-up `bfccadb75`
- **File**: the commit message of `b5807ae69`
- **Severity**: Missing Requirement (low)
- **Reference**: issue 156-3; P7.md §4 (the branch is squashed); the `amend!` rewords `0bab3ef0b`, `967cba1a7` and `8bdedbcda`, made for the same reason
- **Description**: A `fixup!` keeps its target's message, so after the autosquash the typing commit still says that `usage.ts` compiles "the forms of the Consumer interface through a function that both KafkaConsumer and MockConsumer are passed to (each class's own signatures must satisfy every overload the interface declares)". 156-3 showed that mechanism doesn't hold. The code that lands now calls each form on the class itself (`everyKafkaConsumerForm`, `everyMockConsumerForm`), and adds the value-class methods and the errors' constructors, none of which the message mentions.

  I checked this with a trial `git rebase --autosquash -i ba988a002` of `967cba1a7` in a scratch clone, with the hooks as configured. It stops nowhere, leaves the tip's tree (`e17ae841b`) in six commits, and the typing commit's message is the original one, word for word. The other two amended messages read correctly.
- **Expected**: An `amend!` reword of `b5807ae69`, as was done for `27be0a9ee` and `c8bb8c669`. Its message describes what lands: each form called on the interface, on a `KafkaConsumer` and on a `MockConsumer` (a `Set` where an iterable is taken), plus the value-class methods and the errors' constructor forms.
- **Actual**: The squashed message keeps the parenthetical claim.
- **Fixed in**: `7ff816cf4`, `amend!` reword of `b5807ae69`: the message now says each form is called on a Consumer-typed receiver, on a KafkaConsumer and on a MockConsumer (a Set where an iterable is taken), and adds the value-class methods, the errors' constructor forms and the abstract `InvalidOffsetError`. No line starts with `#`. A trial autosquash gives the typing commit this message.

## Issue 156-8: The rewritten commit-callback test still asserts a timing, its new comment misstates the code path, and it dropped a check the contract supports
- **Commit**: `888a2e01a` javascript: the commit-callback broker test asserts the contract, not a timing
- **File**: `javascript/test/integration/consumer.test.js:968-990`
- **Severity**: Test Gap
- **Reference**:
  - `org.apache.kafka.clients.consumer.internals.AsyncKafkaConsumer#commitAsync(Optional,OffsetCommitCallback)` and `#commit(CommitEvent)`;
  - the core's `register_async_commit` and `submit_commit` (`rust/src/consumer/internals/async_kafka_consumer.rs`);
  - JS conventions line 65 ("runs inside the next waiting call").
- **Description**: The diagnosis is right: a later `commitAsync()` runs the callbacks queued so far, in Java's `commit()` and in the core's `submit_commit`. But the fix has three problems.
  - **The final assertion still depends on a timing.**
    - The test commits `ok`, then `failed`, then an empty map last. Java's `commit()` returns `completedFuture(null)` for empty offsets, and `commitAsync` stores that already-completed future as `lastPendingAsyncCommit`. The core's `register_async_commit` stores `PendingAsyncCommit::completed()` "without chaining behind earlier in-flight commits: a later `commitSync` / `close` does not wait for them".
    - So `commitSync()` waits for neither earlier commit. It runs the `ok` and `failed` callbacks only if their answers came back during the 1 s idle wait.
    - I ran the test's sequence against my broker with no idle wait:
      - Java's own client (the 4.3.1 jar), 10 runs of 10: after `commitSync()` only `empty` had run, and still only `empty` after a second `commitSync()`.
      - This client: 9 runs of 10 `[empty]`, 1 run `[empty, ok]`.
      - With the 1 s wait, all three had run in 10 runs of 10, in both clients.
    - So "after commitSync(), all three ran" holds only because both answers arrive within that second. This is the slow side of the race the commit set out to remove.
  - **The new comment is false for this sequence.** "commitSync() waits for the commits before it and runs their callbacks": here it waits for nothing.
  - **A check the contract supports was dropped.**
    - Java and the core run the queued callbacks before they register the new commit. So no commit's callback can run inside its own `commitAsync()`, and `empty`'s cannot run before the next waiting call.
    - The old `deepEqual(seen, [])` caught a callback run inside its own call. The new count check doesn't: an `empty` callback run inline would pass.
    - In 40 runs (both orders below), no callback ran inside its own call.
- **Expected**:
  - The final assertion doesn't rest on the idle wait. Either:
    - commit the empty map first, then `ok` and `failed`. `commitSync()` then waits for the last pending commit, and all three run inside it, deterministically: 10 runs of 10 with no wait in Java's client and in this one; or
    - keep the order, and end with waiting calls in a deadline loop until the three have run.
  - The idle check stays.
  - Before the idle wait, `seen` doesn't hold `empty` (no commit's callback runs inside its own `commitAsync()`).
  - The comment says what the code does.
- **Actual**: The final assertion relies on the 1 s idle wait, the comment claims `commitSync()` waits for the earlier commits, and nothing checks that a callback doesn't run inside its own call.
- **Related**: `callbackErrorFrom`'s `commitAsync()` case (`consumer.test.js:1009-1031`) has the same race's other side. Its next `commitAsync()` rejects with the callback's error only if the first commit's answer came back within its fixed 1.5 s wait. The `commitSync()`, `poll()` and `close()` cases wait for the commit themselves.
- **Fixed in**: `38b5a972e`, a new commit (the change reorders P5's lines, `e5ae3678b`, as well as `888a2e01a`'s, so it is no fix-up of one target). The review's way (1): the empty map is committed first, then ok and failed, so `commitSync()` waits for failed's pending commit and runs all three callbacks. The test checks that failed's callback hasn't run before the idle wait, keeps the idle check, and its comments say what the code does. Related: `8b0fd161c`, a new commit: `callbackErrorFrom`'s `commitAsync()` case repeats `commitAsync()` in a 30-second deadline loop until one rejects, and the fixed 1.5-second wait goes (the other cases wait for the commit themselves).

## Issue 156-9: The own-call check covers only the last commit, not the empty map's, whose inline run the P5 test caught
- **Commit**: `38b5a972e` javascript: the commit-callback broker test commits the empty map first
- **File**: `javascript/test/integration/consumer.test.js:968-982`
- **Severity**: Test Gap (low; the rest of 156-8 is resolved)
- **Reference**: issue 156-8 ("no commit's callback runs inside its own `commitAsync()`"); `org.apache.kafka.clients.consumer.internals.AsyncKafkaConsumer#commitAsync(Optional,OffsetCommitCallback)`; the core's `register_async_commit`
- **Description**:
  - The test now checks, before the idle wait, that `failed`'s callback hasn't run. That guards against a `commitAsync()` that waits for its own answer. It does not guard against the regression the P5 test caught: an empty map's callback run inside its own `commitAsync()`.
  - That is the plausible inline case. An empty map's commit completes at once in Java (`completedFuture(null)`, whose `whenComplete` only enqueues the callback) and in the core (`register_async_commit` enqueues it, and nothing runs it until the next waiting call).
  - With the empty map now committed first, its callback may legitimately run inside the next `commitAsync()`. So an inline run inside its own call shows only in `seen` right after that first call, which nothing reads.
  - That check is deterministic. In my probe of this order (empty, then `ok`, then `failed`, no wait), `seen` was `[]` after the first `commitAsync()` in 10 runs of 10, in Java's client and in this one. `empty` then ran inside the second call each time.
- **Expected**: `assert.deepEqual(seen, [])` right after the empty map's `commitAsync()`, beside the `failed` check: no commit's callback runs inside its own call, the first one included.
- **Actual**: Only the last commit's own-call run is checked.
- **Fixed in**: `0289ee0c2`, fix-up of `38b5a972e`: `assert.deepEqual(seen, [])` right after the empty map's `commitAsync()`, with a comment saying its commit completes at once but its callback waits for a later waiting call. The line above the insertion is `38b5a972e`'s; the line below is P5's (`e5ae3678b`), unchanged. A trial autosquash applies it with no stop and keeps the tip's tree.

## Issue 156-10: The new `assert.deepEqual(seen, [])` fails the type check of the tests, so the gate fails
- **Commit**: `0289ee0c2` fixup! javascript: the commit-callback broker test commits the empty map first
- **File**: `javascript/test/integration/consumer.test.js:974` (the error is reported at `:985`)
- **Severity**: Bug (a required check fails; the test itself passes)
- **Reference**: `javascript/Makefile` `typecheck-tests` (`scripts/typecheck_tests.js`), which the root `test-javascript-unit` runs, and so the root `make verify`, and which `make -C javascript test` runs, and so `make verify-javascript`; PLAN.md, "Commits and gate" ("Every Actor commit, fix-ups included, passes the quick checks", `typecheck-tests` among them); `node_modules/@types/node/assert.d.ts` (20.19.41), where `strict.deepEqual` is `typeof deepStrictEqual`, declared `asserts actual is T`
- **Description**:
  - The test file takes `assert` from `node:assert/strict`, where `deepEqual` is `deepStrictEqual`, a TypeScript assertion function. So after `assert.deepEqual(seen, [])`, `tsc` narrows `seen`, a `const`, to `never[]` for the rest of the block.
  - The `failed` check below it then destructures an element: `seen.some(([what]) => what === 'failed')`. With the element typed `never`, `tsc --strict` reports `test/integration/consumer.test.js(985,29): error TS2488: Type 'never' must have a '[Symbol.iterator]()' method that returns an iterator.`
  - `node scripts/typecheck_tests.js` exits 1 at `0289ee0c2`, in the checkout and in a snapshot of the commit, with "1 finding(s) in the test files". At `8b0fd161c`, whose tree differs only by these 3 lines, it exits 0 with "no finding in the test files".
  - P5's test didn't hit this: after its `assert.deepEqual(seen, [])` it only spread `seen` (`[...seen].sort()`), which needs no element type.
  - At run time the check is right: the test passed 5 runs of 5 against a broker at the tip, and ESLint and Prettier pass on the file.
- **Expected**: An empty-list check that doesn't narrow `seen`, so `typecheck-tests` passes. For example `assert.deepEqual([...seen], []);`: with only that change, `typecheck_tests.js` exits 0 with "no finding in the test files", and a failure still prints what ran.
- **Actual**: `typecheck-tests` fails, and with it `make test-javascript-unit`, the root `make verify` and `make verify-javascript`.
- **Fixed in**: `67890a259`, fix-up of `0289ee0c2` (which folds into `38b5a972e`): the check asserts on a copy, `assert.deepEqual([...seen], [])`, so `seen` keeps its type; `typecheck-tests` finds nothing in the test files. `0289ee0c2` alone fails `typecheck-tests` (TS2488 at consumer.test.js:985), as its snapshot check showed: I committed it without running `typecheck-tests` first. A trial autosquash stops nowhere and keeps the tip's tree.
