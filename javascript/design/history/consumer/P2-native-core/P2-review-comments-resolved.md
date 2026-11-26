# P2: the issues the review resolved (Critic 151)

Copied at the end of P2 (2026-11-26) from `COMMENTS.DONE.151.md` at the repository root: the issues Critic 151
found in its review of P2, each with the fix that resolved it, in issue order. The review's round 2 confirmed every
fix and found no new issue. Issue 151-3 is resolved as a documented deviation, as in Python (producer DECISIONS 2),
which the Manager decided; P2.md's "Result" says so for the user.

## Issue 151-1: A `close(null)` the FFI refuses is recorded as a close: Node is let go, and the finalizer destroys the open consumer
- **Commit**: `dd2046786` javascript: the consumer, over the C FFI with bytes only (NOTES.md: `9bfaa639f`)
- **File**: `javascript/native/consumer.c:803-815` (`close_on_complete`); `javascript/design/current/consumer/NOTES.md:31-32` and `:83-85`
- **Severity**: Bug
- **Reference**: Java `AsyncKafkaConsumer#close(CloseOptions)`: `option.timeout()` dereferences the null before `acquire()`, so the consumer stays open. P2.md item 7 ("the finalizer destroys only a closed consumer (lines 155, 184)"). JS conventions, Lifetime: "No finalizer closes or destroys a client that's still open", and a client keeps Node running "until `close()` closes it". `kafka_consumer_KafkaConsumer_destroy`'s doc: it "drops the consumer (which closes it if `close` was never called)".
- **Description**: `close({ option: null })`, or a bare `close(null)`, sends a NULL `CloseOptions` to `kafka_consumer_Consumer_close_with_options_cb`. Its `close_options_ref` refuses the NULL before the core runs, and the completion gets `LocalNullPointerError("options must not be null")`. `close_on_complete` treats every outcome except `LOCAL_CONCURRENT_MODIFICATION` as "closed" and calls `cbq_closed`. That turns the handle object's reference weak and drops Node's open hold, although the core consumer is still open. Probes on the built package, broker-less (`127.0.0.1:1`):
  - A child that only runs `c.close({ option: null }).catch(...)` prints `assignment() after: 0` (no closed error, so the consumer is open). It then exits on its own with status 0. An open consumer should keep Node running until it is killed, as the unit test's own `open` case shows with `SIGTERM`.
  - With `--expose-gc`, an open consumer that is dropped and collected adds 0 to `consumerStats().consumersFreed`. A consumer whose `close(null)` was refused, dropped and collected, adds 1, the same as a closed one. So `consumer_finalize` ran `consumer_destroy`, and with it `KafkaConsumer_destroy_err`, on a consumer the core never closed. That destroy then closes it, from a finalizer.

  The unit test at `kafka_consumer.test.js:799-802` asserts only the rejection. The memory test 'the test-only switch of the copy path arms and disarms…' closes the consumer again right after its refused `close(null)`. So neither test sees the problem. NOTES.md says only the busy refusal "closed nothing", and that the finalizer "destroys only a consumer the core reported closed". The null refusal is a second case where nothing closed, and the core never reported this consumer closed.
- **Expected**: A close the FFI refuses before the core runs closes nothing. That covers `LOCAL_NULL_POINTER` for the NULL options as well as the busy `LOCAL_CONCURRENT_MODIFICATION`. No `cbq_closed`, `closedNow` is false, and the consumer keeps holding Node and is never destroyed by a collection until a later close succeeds. A test pins this: in the child-process test, the child keeps running after a refused `close({ option: null })`; or, in a memory probe, a consumer dropped after the refused close leaves `consumersFreed` unchanged. NOTES.md names both refusals that close nothing.
- **Actual**: The refused `close(null)` lets Node exit with the consumer still open. It also makes the open consumer collectable, and its finalizer then destroys, and so closes, it. `debug('KafkaConsumer closed')` is logged for it too.

Fixed in `4ce77642f`: a close refused for NULL options (LOCAL_NULL_POINTER) is no close, as the busy one; a child test and a memory probe pin it, and NOTES.md names both refusals.

## Issue 151-2: `testMetricsRemovedOnClose` is weakened on a premise that is false for its own configuration
- **Commit**: `dd2046786` javascript: the consumer, over the C FFI with bytes only
- **File**: `javascript/test/unit/consumer/kafka_consumer.test.js:138-141` (the note) and `:517-531` (the test)
- **Severity**: Test Gap
- **Reference**: Java `KafkaConsumerTest#testMetricsRemovedOnClose` and `assertMetricsMap(false)`: once the `count` meta-metric is removed, `assertEquals(List.of(), actual, "The consumer should have removed its metrics on close(), but there are metrics remaining")`. The Actor's departure 9. DoD #3 (a test checks the same behaviour as Java's).
- **Description**: The note says "after close() the core keeps the coordinator's metrics in the consumer's registry, where Java removes every metric but the "count" meta-metric". On that basis the port checks only that the `consumer-metrics` group is gone. The premise holds only when a `group.id` is set. Java's test and this port both build the consumer without one (`{ 'group.protocol': 'consumer', 'bootstrap.servers': 'localhost:9999' }`). With that configuration the core leaves exactly the `count` meta-metric after `close()`: a probe of the built package, running the test's exact steps 20 times, gives `20 x ["kafka-metrics-count:count"]`. The 17 `consumer-coordinator-metrics` stay after `close()` only with `'group.id': 'g'`, which I also measured. So Java's full assertion holds here, and the port checks less than Java for a wrong reason. A regression that left, for example, the fetch-manager metrics registered after `close()` would pass this test.
- **Expected**: The port asserts Java's condition: after `close()`, the metric names are exactly `['kafka-metrics-count:count']`. If the core's behaviour with a `group.id` (coordinator metrics kept after `close()`) should be recorded, the note says that it applies with a `group.id`, a case Java's test doesn't cover.
- **Actual**: `assert.deepEqual(after.filter((n) => n.startsWith('consumer-metrics:')), [])` and "count is present", under a note whose premise is wrong for this configuration.

Fixed in `678d56ace`: the port asserts Java's full condition (only the count meta-metric is left after close()), and the wrong note is gone.

## Issue 151-3: The FFI's null refusals come before Java's earlier checks, and `pause`/`resume` give a null-topic `TopicPartition` a `TypeError`; neither is recorded
- **Commit**: `dd2046786` javascript: the consumer, over the C FFI with bytes only
- **File**: `javascript/src/consumer/kafka_consumer.ts:243-358`; `javascript/design/current/consumer/NOTES.md:24-29`; `javascript/design/current/consumer/PLAN.md` §6
- **Severity**: Behavior Mismatch. This needs the user's decision: record it as a deviation with an FFI ask, or change it.
- **Reference**:
  - Java `AsyncKafkaConsumer#subscribeInternal` and `#subscribeToRegex` run `acquireAndEnsureOpen()`, then `throwIfGroupIdNotDefined()`, then the null checks. `#assign`, `#pause` and `#resume` run `acquireAndEnsureOpen()` first. `SubscriptionState#pause` and `#resume` call `assignedState(tp)`.
  - JS conventions, Errors: "all checks run in Java's order (Q36)".
  - Python's conventions record the same deviation under "Order": "a consumer without `group.id` reports a null collection where Java reports `InvalidGroupIdException`".
  - `kafka_common_TopicPartition_new`'s header doc: "WARNING: `NULL` `topic` panics; Java stores `null`."
- **Description**: The binding passes a null collection or pattern to the FFI, which refuses it at the boundary (`kafka_consumer_Consumer_subscribe_with_topics_cb`: `if topics.is_null() { fail(...) }`). That happens before the core's `acquire()`, closed check and group check. Probes on the built package:
  - With no `group.id`, `subscribe({ topics: null })` gives `IllegalArgumentError: Topic collection to subscribe to cannot be null`, and `subscribe({ pattern: null })` gives `IllegalArgumentError: Topic pattern to subscribe to cannot be null`. Java throws `InvalidGroupIdException` first, and the binding does too for `topics: [null]`, `[' ']` and `[]`.
  - On a closed consumer, `subscribe({ topics: null })`, `subscribe({ pattern: null })` and `assign({ partitions: null })` give those `IllegalArgumentError`s, and `pause({ partitions: null })` gives `TypeError: The partitions to pause must be nonnull`. Java throws `IllegalStateException("This consumer has already been closed.")`.
  - While a `poll` is pending, `subscribe({ topics: null })` and `assign({ partitions: null })` give the `IllegalArgumentError`s. Java throws `ConcurrentModificationException`.
  - `pause` or `resume` of `new TopicPartition({ topic: null, partition: 0 })`, which Java and the JS class both allow, gives `TypeError: topic must not be null` from the FFI's `TopicPartition_new`. Java throws `IllegalStateException("No current assignment for partition null-0")`. `assign` maps the same case to a blank topic to get Java's message, but `pause` and `resume` don't, and NOTES.md mentions only a null element for them.

  None of this is in NOTES.md, the TSDoc, PLAN §6's FFI asks or a test. The binding can't put the checks in Java's order without repeating a precondition, so the fix belongs to the FFI.
- **Expected**: The user decides. At a minimum: NOTES.md, and the TSDoc `@remarks` where it fits, record this as a deviation with its Why and Goes when, as Python's conventions record theirs. PLAN §6 gains the FFI ask: refuse the NULL only after the core's earlier checks, and accept a null topic. A test pins today's order.
- **Actual**: None of it is documented. The tests cover the null cases only on an idle, open consumer with a `group.id`.

Fixed in `789d73e01`: recorded as a deviation in NOTES.md (Why, Goes when) and in the @remarks of subscribe, assign and pause (resume's is at the doc-size limit), and pinned by a test of today's order and messages. The FFI ask goes to PLAN §6 through the Manager.

## Issue 151-4: The port of `testOperationsBySubscribingConsumerWithDefaultGroupId` leaves out its constructor half, which P2.md gives to P2
- **Commit**: `dd2046786` javascript: the consumer, over the C FFI with bytes only
- **File**: `javascript/test/unit/consumer/kafka_consumer.test.js:307-313`
- **Severity**: Test Gap
- **Reference**: Java `KafkaConsumerTest#testOperationsBySubscribingConsumerWithDefaultGroupId` starts with `assertThrows(InvalidConfigurationException.class, () -> newConsumer(groupProtocol, null, Optional.of(true)))`, which means no `group.id` and `enable.auto.commit=true`. P2.md §2: "the constructor, `subscribe` and `assign` halves of … whose commit halves P3 adds".
- **Description**: The port's comment says "Its subscribe half: the commit half needs the commit calls". Java's test also opens with a constructor assertion that needs no commit call, and the port leaves it out. P3 will add only the commit halves, so it would stay missing. `consumer_config.test.js` `testOverrideEnableAutoCommit` (line 99) asserts the same error, so the behaviour is tested, but not in this Java test's port.
- **Expected**: The port asserts that `new KafkaConsumer({ configs: configs(null, { 'enable.auto.commit': true }) })` throws `InvalidConfigurationError` "enable.auto.commit cannot be set to true when default group id (null) is used.", unwrapped as in Java. Or its comment says where that half is tested.
- **Actual**: The port has only the subscribe half, and its comment misdescribes what is left for P3.

Fixed in `d5c899c8c`: the port asserts the constructor half (InvalidConfigurationError with Java's message), and its comment names what is left.

## Issue 151-5: `Consumer.poll`'s TSDoc paraphrases the text the conventions prescribe, and leaves out the timeout's errors
- **Commit**: `dd2046786` javascript: the consumer, over the C FFI with bytes only
- **File**: `javascript/src/consumer/consumer.ts:202-247`
- **Severity**: Missing Requirement (low). The wording half changes nothing a user sees, so it may be the user's call.
- **Reference**:
  - JS conventions, Serialization: "The consumer's docs and the README say: "A key or value from `poll()` points into the memory the client fetched, and a value you keep holds that whole fetch in memory. To keep a value for long, copy it with `Buffer.from(value)`.""
  - Java `KafkaConsumer#poll(Duration)`: `@throws java.lang.ArithmeticException if the timeout is greater than Long.MAX_VALUE milliseconds`.
  - The same duration helper's errors, as its siblings document them: `Producer.close` and `KafkaFuture.get` have `@throws TypeError` for a null timeout and `@throws RangeError` for `NaN`, `-Infinity`, years, months or weeks, and values beyond `Number.MAX_SAFE_INTEGER`.
- **Description**: The `@remarks` says "Each key and value is a `Buffer` over the memory the client fetched, not a copy, and a value you keep holds that whole fetch in memory: to keep a value for long, copy it with `Buffer.from(value)`." That is not the conventions' sentence. The producer quotes its own prescribed text word for word (`producer.ts:143`). Java's `ArithmeticException` row is also dropped without its JavaScript form. `poll` throws `RangeError` in that case (`poll({ timeout: NaN })` gives "timeout must be a number of milliseconds, not NaN", tested), and `TypeError('timeout must not be null')` for a null timeout (tested). Neither error is documented.
- **Expected**: The conventions' sentence as written, plus `@throws` rows for the `RangeError` (the JavaScript form of Java's `ArithmeticException` case) and for the null timeout's `TypeError`, as `Producer.close` has.
- **Actual**: A paraphrase, and no row for the `RangeError` or the null timeout's `TypeError`.

Fixed in `516881bc8`: the conventions' two sentences are quoted word for word in poll's @remarks and the package docs, and poll documents its timeout's RangeError and TypeError.

## Issue 151-6: `native_registry.test.js`'s comment still counts 10 registrations, adding up to the old 17
- **Commit**: `dd2046786` javascript: the consumer, over the C FFI with bytes only
- **File**: `javascript/test/lint/native_registry.test.js:37-40`
- **Severity**: Test Gap (a stale comment only)
- **Reference**: P2.md item 18: "with the counts its tests expect, as `508b3da9d` did for `adminStats`". In `508b3da9d`, the same hunk updates this comment along with the count ("11 registrations (… faultNext and adminStats, the test-only ones)").
- **Description**: The expected count is now 19, but the comment above it still reads "10 registrations (probe, headerStats, producerStats, errorStats, recordSpy, metricsStats and faultNext, the test-only ones), 3 uses, 4 src/ files". That adds up to 17 and leaves out `consumerStats` and `consumerRefuseExternalBuffers`.
- **Expected**: "12 registrations (…, faultNext, consumerStats and consumerRefuseExternalBuffers, the test-only ones), 3 uses, 4 src/ files."
- **Actual**: The old comment.

Fixed in `d590a5676`: the comment counts 12 registrations, consumerStats and consumerRefuseExternalBuffers included.
