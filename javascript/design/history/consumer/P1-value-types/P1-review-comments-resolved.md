# P1: the issues the review resolved (Critic 150)

Copied at the end of P1 (2026-11-25) from `COMMENTS.DONE.150.md` at the repository root: the issues Critic 150
found in its three rounds of review, each with the fix that resolved it.

## Issue 150-1: `records({ partition })` doesn't check the partition's type, so `TopicPartitionMap.get()` reads it
- **Commit**: `fb63c9e0f` javascript: the consumer's value classes
- **File**: `javascript/src/consumer/consumer_records.ts:162-166`
- **Severity**: Bug
- **Reference**: `javascript/CLAUDE.md`, JavaScript Binding Conventions, Class family ("Where Java takes a value class, JavaScript takes only an instance of it, and a plain object throws an error with a hint") and "TypeScript first" (the run-time type checks stay for a plain JavaScript caller); "The binding's notes", Java shape, JS idiom, Errors ("A value of the wrong JS type throws `TypeError`"); `org.apache.kafka.clients.consumer.ConsumerRecords#records(TopicPartition)`; P1.md item 8.
- **Description**: The partition form reads its value with `RECORDS_FORMS.value(options, 'partition')` and passes it straight to `this.#map().get(partition)`. The value is never checked. `TopicPartitionMap.get` then reads it as its own argument. A plain object is taken as `get()`'s options object, and any other wrong value fails `get()`'s form check. So a plain JavaScript caller gets an error about a method and a parameter (`get()`, `key`) they never used, of the wrong class, and one plain object is accepted without any error. The topic form checks its type (`TypeError('topic must be a string')`), but the partition form doesn't. Probed against the built `dist/` at `e631c46b9`, with `recs` holding one record for `t-0`:
  - `recs.records({ partition: 'x' })` throws `IllegalArgumentError("get() takes one of (key); got a string")`.
  - `recs.records({ partition: { topic: 't', partition: 0 } })` (the usual mistake: a plain object for a `TopicPartition`) throws `IllegalArgumentError("get() takes one of (key); got (topic, partition)")`, with no hint.
  - `recs.records({ partition: { key: tp } })` returns `t-0`'s records, because `get()` reads `{ key: tp }` as `get({ key })`.

  No test gives `records` a wrong-typed `partition` by name. The bare form is covered (`records(1)`), but that path never reaches `get()`.
- **Expected**: Before it reads the map, `records({ partition })` refuses a value that is neither a `TopicPartition` nor `null` with a `TypeError` naming `partition`, as the topic form does for `topic`. `src/internal/payload.ts` already has this check: `topicPartition.take(value, 'partition')` throws `TypeError('partition must be a TopicPartition')`. A test covers a string and a plain object given as `partition`, each with `// @ts-expect-error` and the exact message.
- **Actual**: The value goes unchecked into `TopicPartitionMap.get()`. A wrong type comes back as `IllegalArgumentError` naming `get()` and `key`, and a plain object with a `key` property is read as `get()`'s options object.


- **Fixed** in `b74649bd7` (fixup of `fb63c9e0f`): `records({ partition })` checks its value with the payload `topicPartition` kind before it reads the map, so a wrong type throws `TypeError('partition must be a TopicPartition')`; a test covers a string, two plain objects and `null`.

## Issue 150-2: `CloseOptions.timeout()` and `withTimeout()` TSDoc says a fractional timeout throws `RangeError`
- **Commit**: `fb63c9e0f` javascript: the consumer's value classes (TSDoc that `8f6299335`, P1's Comments-rule commit, left as it was)
- **File**: `javascript/src/consumer/close_options.ts:93-94` and `:147-148`
- **Severity**: Bug (TSDoc; low)
- **Reference**: `javascript/CLAUDE.md`, JavaScript Binding Conventions, Signatures ("`NaN`, and a finite value above `Number.MAX_SAFE_INTEGER`, throw `RangeError`, and fractions are rounded down"), and Documentation; "The binding's notes", Comments, point 1; the constructor's own `@throws` at `close_options.ts:59-61`.
- **Description**: The static `CloseOptions.timeout({ timeout })` and the setter `withTimeout({ timeout })` both document `@throws RangeError when the timeout is not a whole count of milliseconds a number holds`. Read as written, any timeout that isn't a whole number of milliseconds throws. But fractions are rounded down, as the conventions say and the class's own test asserts (`CloseOptions.timeout({ timeout: 250.7 }).timeout()` is `250`, `close_options.test.js`). The doc also doesn't name the cases that do throw: it never says that a `Temporal.Duration` with years, months or weeks is refused. Probed against the built `dist/` at `8f6299335`:
  - `timeout({ timeout: 250.7 })` gives 250, and `withTimeout({ timeout: 0.5 })` gives 0, with no error.
  - `NaN` throws `RangeError("timeout must be a number of milliseconds, not NaN")`, and `2 ** 60` throws `RangeError("timeout of 1152921504606847000 ms is above Number.MAX_SAFE_INTEGER ms; pass Infinity for no limit")`.
  - `withTimeout({ timeout: '5' })` throws `TypeError("timeout must be a number of milliseconds or a Temporal.Duration")`, which neither doc lists.

  The same class's constructor documents the same conversion correctly (lines 59-61). So do the producer's `close()` (`src/producer/producer.ts:340-342`) and `KafkaFuture.get()` (`src/common/kafka_future.ts:511-513`).
- **Expected**: Both docs state the cases the constructor states: `RangeError` when the timeout is `NaN`, `-Infinity`, beyond `Number.MAX_SAFE_INTEGER` milliseconds, or a `Temporal.Duration` with years, months or weeks. They also give the `TypeError` for a timeout that is neither a number nor a `Temporal.Duration`, or is `undefined`. No wording says that a fraction throws.
- **Actual**: Both docs say a timeout that is "not a whole count of milliseconds a number holds" throws `RangeError`. That contradicts the rounding the client does and its test, and the `TypeError` isn't listed.


- **Fixed** in `cd701ffe9` (fixup of `fb63c9e0f`): both TSDocs now give the constructor's cases, `RangeError` for `NaN`, `-Infinity`, beyond `Number.MAX_SAFE_INTEGER` ms or a `Temporal.Duration` with years, months or weeks, and `TypeError` for a timeout that is neither a number nor a `Temporal.Duration`, or is `undefined`; nothing says a fraction throws.
