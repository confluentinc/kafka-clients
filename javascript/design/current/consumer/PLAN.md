# JavaScript (Node.js) client — the consumer

Status: **approved** by the user on 2026-11-25, with the decisions of §8, and corrected the same day (P1's tests
and linter work, `poll(long)`, `doc-check`, the CHANGELOG, and the per-commit checks the user decided). P1 is
**done** (2026-11-25): its phase plan, `P1.md` beside this file, was approved with its choices 2(a) to 9(a), and
its "Result" section has the commits, the review and the gate. Choice 5(a) moved the `Consumer` interface from P1
to P2. P2 is **done** (2026-11-26): its phase plan, `P2.md`, was approved with its choices 2(a) to 8(a), and its
"Result" section has the commits, the review and the gate. P3 is **done** (2026-11-26): its phase plan, `P3.md`, was
approved with its choices 2(a) and 3(a), and its "Result" section has the commits, the review and the gate. P4 is
**done** (2026-11-27): its phase plan, `P4.md`, was approved with its choice 2(a), and its "Result" section has the
commits, the review and the gate. P5 is **done** (2026-11-27): its phase plan, `P5.md`, was approved with its choices
2(a) and 3(a), and its "Result" section has the commits, the review and the gate. The user then chose (a) in its
"For the user" list: `commitAsync({ offsets, callback })`'s `callback` stays non-null. P6 is **done**
(2026-11-27): its phase plan, `P6.md`, was approved with its choices 2(a), done as the merge `34f8de73b` of
PR #312's `c0adc2cf5`, 3(a) and 4(b), and its "Result" section has the commits, the review and the gate. The
user accepted its deviation 1b and left 1a, the float's signaling NaN, open for their team. P7's phase plan,
`P7.md`, was approved on 2026-11-28 with its choices 2(a), 3(a) and 4(b), and P7 is in progress.

- Branch `dev/javascript-consumer-bindings`, on PR #312's head `f62d5fec9` (`origin/dev/javascript-producer-bindings`),
  an ancestor of the branch, and since 2026-11-27 merged with its newer `c0adc2cf5` (`34f8de73b`, P6.md, choice
  2(a)). The user moved the branch there on 2026-11-25 (from `208abeb64`); P1's commits came
  through unchanged, under new hashes.
- Rules: the base holds `origin/kraina-js-claude-rules` up to `31910fe32`. The rules branch is at `aa551943e`, three
  commits ahead (lines 97, 101, 106, 131 and 160 of its file); the consumer follows them where they apply (P2.md,
  choice 8(a)).
- Agent numbers 150–157, one per phase (decision 17).
- The owner deleted an earlier draft of this plan. This one is written fresh and doesn't use it.
- Line numbers below are those of `javascript/CLAUDE.md` at `208abeb64`. At `8a25205a3` they are the same up to line
  218; the row of old line 219 is gone, so old lines 220 and 221 are 219 and 220, and the notes start at 227.

## Words used in this plan

- **Core**: the Rust client in `rust/`, which does all the Kafka work.
- **FFI** (foreign function interface): the C functions the core exports, declared in
  `rust/target/include/confluent_kafka_bindings.h`. Every non-Rust client calls only these. An **entry point** is
  one of them.
- **Addon**: the C code in `javascript/native/` that Node loads and that calls the FFI.
- **`_cb` twin**: the non-blocking form of an entry point. It returns at once and reports its result later, through
  a completion. **`_err` twin**: a form with an error slot, which also catches a crash inside the core (a Rust
  *panic*) and reports it as an error.
- **Callback queue** and **drain**: the core queues every completion and user callback of a client; the addon runs
  them on Node's main thread by calling the client's `__execute_callbacks` (one drain).
- **Thread-safe function**: Node's way for another thread to ask the main thread to run something. The core's
  "the queue isn't empty" signal reaches the main thread through one per client.
- **Promise**: JavaScript's value for a result that comes later, read with `await`.
- **Rebalance**: the group coordinator moving partitions between consumers. The **listener** is the user's
  `ConsumerRebalanceListener`, told before partitions are taken away and after they are given.
- **Commit callback**: the user's `OffsetCommitCallback`, told when a `commitAsync` finishes.
- **Callback window**: the time during which a listener or commit callback has started and hasn't reported back.
- **`ConsumerHandle`**: the core's side door into a consumer that is busy inside `poll()`; calls made from a
  listener go through it.
- **Zero copy**: handing JavaScript the fetched bytes without copying them. An **external buffer** is a Node
  `Buffer` whose memory belongs to the core; Node tells the addon when it's garbage collected.
- **Finalizer**: the addon function Node calls when a JavaScript object is garbage collected.
- **Serde**: a serializer or a deserializer (value to bytes, bytes to value).
- **Data class**: a class whose Java getters become read-only fields (`record.offset`, not `record.offset()`).
- **Mock**: `MockConsumer`, the test double users run without a broker.
- **gRPC conformance backend**: a small server that lets the Rust test suite drive the JavaScript client, so the
  same multi-language test runs against every language.
- **Verifiable client**: a program that speaks the Kafka system-test protocol (one JSON event per line), used for
  chaos and soak runs.
- **Actor / Critic**: the agent that writes the code and the agent that reviews it (`.claude/rules/agent-roles.md`).
  A **fixup commit** fixes an earlier commit of the same phase.

## 1. Goal and scope

**Goal.** Add the consumer to the Node.js client: Java's `Consumer` interface, `KafkaConsumer`, `MockConsumer` and
every public type they reach, built on the C FFI exactly as the producer is, and shaped by the JS conventions. The
milestone is done when:
- `examples/consumer.js` reads back the record that `examples/producer.js` wrote;
- the Java tests of every translated class pass, translated with Java's messages;
- the Rust multi-language consumer tests pass against the JavaScript gRPC backend;
- a verifiable consumer runs in the chaos harness.

**In scope:**
- Value classes, in plain TypeScript (line 160): `ConsumerRecord`, `ConsumerRecords`, `OffsetAndTimestamp`,
  `SubscriptionPattern`, `ConsumerGroupMetadata`, `CloseOptions` with `GroupMembershipOperation`, `GroupProtocol`.
- `ConsumerConfig`: its constants, the configs type and the key table, from the existing xtask generators.
- The interfaces the user implements: `ConsumerRebalanceListener`, with its defaults helper, and
  `OffsetCommitCallback`.
- The client family: the `Consumer` interface, `KafkaConsumer` with the JavaScript-only `create()` (line 156), and
  `MockConsumer` over the FFI's mock.
- The deserializers: the `Deserializer` interface and the built-in classes (decision 1).
- Unit, broker and type tests; the README, the migration guide and TypeDoc; the example.
- The gRPC `ConsumerService` and `verifiable_clients/verifiable_consumer.js`.

**Already there, and reused:** `OffsetAndMetadata` (with its test), `TopicPartition`, `TopicPartitionMap`,
`TopicPartitionSet`, `PartitionInfo`, `Node`, `MetricNameMap`, the plain TypeScript `Header`, `RecordHeader` and
`Headers` (line 135) with the `headers` converter of `src/internal/payload.ts`, the generated error classes
(`WakeupError`, `InvalidGroupIdError`, `RecordDeserializationError`, `CommitFailedError` and the others), and the
addon's queue and error code. `native/headers.c` now only copies headers into the core at `send()`, so the copy out
of a consumed record is new in P2 (4.2).

**Out of scope:**
- What the JS conventions leave out: the share consumer, assignors, interceptors, partitioners, `ConfigDef` and
  `CommonClientConfigs` (line 24); `run()`, an async iterator, an event emitter and `connect`/`disconnect`
  (line 26); `AbortSignal` (line 151); clients in worker threads (line 153).
- `subscribe(Pattern)`: only `SubscriptionPattern` is taken, and a `RegExp` throws (line 76).
- Every deprecated member: `close(Duration)`, the `ConsumerGroupMetadata` constructors, `ConsumerRecords(Map)`,
  `MockConsumer(OffsetResetStrategy)`. Kafka 4.4's `Consumer` has no `poll(long)`, only `poll(Duration)`.
- Producer transactions, which the producer left out of the preview. `groupMetadata()` is built, but nothing hands
  it to a producer yet.
- Members the FFI has no entry point for (§6): left out and listed in the module docs (line 161).
- No edits to the JS conventions (lines 1–224 of `javascript/CLAUDE.md`), root `CLAUDE.md` or `.claude/rules/`.
  Agents write only in "The binding's notes".
- No FFI or core change. The only Rust changes are the xtask config generators (P1) and the Rust test harness of
  the gRPC arm (P8).
- No change to the producer's serde code before P6, which applies decision 1(a).

## 2. Ground truth and rules

These decide every question, in this order:
1. The JS conventions, `javascript/CLAUDE.md` lines 11–223 at `8a25205a3`, and the rules branch's newer lines where
   they apply (P2.md, choice 8(a)). Before each phase, fetch `origin/kraina-js-claude-rules` and diff it against the
   branch (producer DECISIONS 1). A change stops the phase until I've checked what it touches.
2. The C ABI in `rust/target/include/confluent_kafka_bindings.h`.
3. Java 4.4: `kafka/clients/src/main/java/org/apache/kafka/clients/consumer/` and its tests.
4. Root `CLAUDE.md` where the JS conventions are silent, and `.claude/rules/consumer-threading.md` §31 and §41 for
   what the core does in the callback window.
5. How the producer read the rules, in `javascript/design/current/producer/DECISIONS.md`:
   - 1–4: the rules come from PR #327; a gap is checked against a similar decided case first; the terms; a
     conventions conflict is implemented as written unless that breaks the client;
   - 16–18: what a serde config function's keys do, the string factory, the mock over the FFI;
   - 21 is superseded: the rules settled headers on 2026-11-24 (lines 135–138, Q31), as DECISIONS 21 records;
   - 33–35: a call on a closed client holds Node, callbacks run one at a time, no never-closed warning.

   The consumer follows each by analogy and says so where it does.
6. Python's consumer (`python/confluent_kafka/consumer/` and its tests): a reference only, used where the JS
   conventions say "as in Python".

Terms are those of producer DECISIONS 3: a "Not decided yet" row stops the work, and a **blocker** is something
that needs a C FFI or core change, which agents never make.

## 3. The C surface used

All of it is in the header today. Names drop the `kafka_consumer_` prefix. The binding calls the `_cb` form of a
waiting call and the `_err` twin of everything else (lines 57, 64).

| Phase | Entry points |
|---|---|
| P2 | `ConsumerConfig_new`; `KafkaConsumer_new` with NULL deserializers; `KafkaConsumer__as_Consumer_err`, `KafkaConsumer_destroy_err`; `Consumer__set_callbacks_notify_err`, `Consumer__execute_callbacks_err`; `Consumer_poll_cb`; `ConsumerRecords_partitions_err`, `_records_with_partition_err`, `_next_offsets_err`, `_destroy_err`; the `ConsumerRecord_*_err` accessors; `subscribe_with_topics_cb`, `subscribe_with_pattern_cb`, `SubscriptionPattern_new_err`, `assign_cb`, `unsubscribe_cb`; `assignment`, `subscription`, `paused`; `pause_cb`, `resume_cb`; `wakeup_err`, `metrics_err`; `close_cb`, `close_with_options_cb`, the `CloseOptions_*` functions; for a record's headers, `ConsumerRecord_headers_err`, then `kafka_common_header_internals_RecordHeaders__as_Headers_err`, `kafka_common_header_Headers_to_array_err`, the `kafka_List_*_err` reads, `kafka_common_header_internals_RecordHeader__as_Header_err` and `kafka_common_header_Header_key_err` / `_value_err` |
| P3 | the four `commit_sync*_cb` forms, `commit_async_cb`, `committed(_with_timeout)_cb`, `group_metadata`, `ConsumerGroupMetadata_*_err`, `position(_with_timeout)_cb`, `seek_with_offset_cb`, `seek_with_offset_and_metadata_cb`, `seek_to_beginning_cb`, `seek_to_end_cb`, `beginning_offsets`, `end_offsets` and `offsets_for_times` (each with `_with_timeout`) `_cb`, `OffsetAndTimestamp_*`, `partitions_for(_with_timeout)_cb`, `list_topics(_with_timeout)_cb` and `current_lag_cb` |
| P4 | `MockConsumer_new`, `__as_Consumer_err`, `destroy_err`, `add_record`, `ConsumerRecordOptionsBuilder_*`, `ConsumerRecord_with_options_err`, `set_poll_error_err`, `set_offsets_error_err`, `update_beginning_offsets_err`, `update_end_offsets_err`, `update_duration_offsets_err`, `update_partitions`, `set_max_poll_records`, `schedule_nop_poll_task_err`, `closed_err`, `last_poll_timeout_err`, `rebalance_cb`, `lose_partitions_cb` |
| P5 | `ConsumerRebalanceListener_new_err`, `_destroy_err`; `subscribe_with_topics_listener_cb`, `subscribe_with_pattern_listener_cb`; `OffsetCommitCallback_new_err`, `_destroy_err`; `commit_async_with_callback_cb`, `commit_async_with_offsets_callback_cb`; `Consumer__set_callback_result_err`; `Consumer_handle_err` and the `ConsumerHandle_*` forms (`_cb` for the waiting calls; `assignment`, `paused`, `group_metadata` and `wakeup` synchronous) |
| P6 | none: the deserializers run in TypeScript, and the core keeps NULL deserializers (line 125) |

## 4. Design

### 4.1 Layout

The producer's layout (JS conventions, Layout and modules), one Java class per file:
- `src/consumer/`: `consumer.ts` (the interface; Java's `Consumer` has no statics, so there's no runtime object, as
  for `Producer`), `kafka_consumer.ts`,
  `mock_consumer.ts`, `consumer_record.ts`, `consumer_records.ts`, `offset_and_timestamp.ts`,
  `subscription_pattern.ts`, `consumer_group_metadata.ts`, `close_options.ts`, `group_protocol.ts`,
  `consumer_config.ts` (generated), `consumer_rebalance_listener.ts`, `offset_commit_callback.ts`, all exported
  from `src/consumer/index.ts`, which is already the package's `./consumer` export.
- `src/common/serialization/`: the `Deserializer` interface and the built-in classes (P6).
- `src/internal/`: `consumer_rebalance_listener_defaults.ts`; `deserializer_defaults.ts`, `deserialize_batch.ts`,
  `builtin_serde.ts` and `client_construction.ts` (P6); and the code shared with the producer (`config.ts`,
  `serde.ts`, `lifecycle.ts` and the rest), extended rather than copied.
- `native/`: `consumer.c` (the client, `poll()` and the records) and, if that reads better, `mock_consumer.c`,
  registered in `addon.c` and `binding.gyp`. They reuse `callback_queue.c`, `error.c`, `error_payload.c` and
  `record.c`. `headers.c` gains the copy out of a consumed record (4.2), taking the reading code of the test-only
  `record_spy.c`, which then calls it. Code that both clients need, such as the producer mock's injected-error
  carrier in `producer.c`, moves to a shared file instead of being copied (DoD #6). P2's test-only switch and
  read-back for the copy path (4.2) are built as `record_spy.c` is.
- Tests in `test/unit/consumer/`, `test/integration/`, `test/typing/` and `test/memory/`; `examples/consumer.js`;
  `grpc-server/`; `verifiable_clients/verifiable_consumer.js`.

### 4.2 Records and zero copy (P2)

- `poll()` resolves to a plain TypeScript `ConsumerRecords` (line 160). The addon builds it when the poll
  completes: per partition, the records and the next offsets, in the core's order.
- `ConsumerRecord` is a data class (decision 10): `topic`, `partition`, `offset`, `timestamp`, `timestampType`,
  `serializedKeySize`, `serializedValueSize`, `headers`, `key`, `value`, `leaderEpoch` and `deliveryCount` are
  fields. Java's `long`s are `bigint`s (line 93). One topic string is made per partition, not per record.
- Keys and values aren't copied (line 133): each is a `Buffer` over the core's memory, made with
  `napi_create_external_arraybuffer`. The records belong to the batch handle that the poll's completion hands over,
  so the addon counts what still needs the batch, its live Buffers and the `Headers` not yet copied (below), and
  destroys the batch when the count reaches zero. The docs say what follows: keeping one key alive keeps that whole
  poll's batch in memory. The addon doesn't call `napi_adjust_external_memory` (line 134).
- On Electron, which refuses external buffers, the addon copies each key and value instead, when Node-API answers
  `napi_no_external_buffers_allowed` (line 133). Headers are copied everywhere, so the limitation covers only keys
  and values. A copy holds nothing native, so on that path only the `Headers` not yet copied keep the batch alive.
  Plain Node never gives that answer, so P2 tests the copy path through a test-only switch. A real Electron run
  stays out of CI.
- A batch may outlive its consumer. The FFI's records handle owns its records and the buffers behind their keys
  and values (the fetch buffer or, for a compressed batch, each record's own), and holds nothing of the consumer
  (`rust/src/ffi/consumer/consumer_records.rs`). So a Buffer kept after `close()`, or after the consumer is
  collected, stays valid. P2's broker memory probes check it (P2.md, choice 6(a)); running them under ASan is a
  Future improvement, drafted on the rules branch (`bcaac2f6b`).
- Each record has its own `Headers`, never `null`, empty when the record has none, which the app or a deserializer
  can change, a change touching only that record (line 137). On its first read or change, not at `poll()`, it
  copies its list out of the headers that `ConsumerRecord_headers_err` lends out `const`: each key becomes a string,
  and each value a new `Buffer` with its own copy, or `null`. That copy is the one addon call outside the clients
  (line 160). Until then the `Headers` keeps the batch alive, and after it, it holds nothing native. No
  `RecordHeaders_with_record_headers_err` handle is made (decision 2). A record with no headers has nothing to
  copy, but the addon learns that only by looking, a few calls per record, since the FFI has no header count. P2
  looks only on the copy path (P2.md, choice 5(a)), which copies every key and value anyway, so there a record with
  no headers holds nothing, and plain Node pays nothing.
- The mock's records hold the user's own JavaScript keys and values, not bytes (4.6).

### 4.3 Waiting calls, the queue and liveness

- A method returns a Promise if and only if its entry point has a `_cb` twin (line 64). For the consumer that's
  `poll`, `subscribe`, `assign`, `unsubscribe`, `pause`, `resume`, every commit, `committed`, `position`, the
  seeks, the offset and metadata queries, `currentLag` and `close`. `assignment()`,
  `subscription()`, `paused()`, `groupMetadata()`, `metrics()` and `wakeup()` are synchronous.
- One thread-safe function per consumer, signalled by the core's notify hook. Each drain runs the queued
  completions on the main thread (line 146), with the producer's `callback_queue.c`. Drains don't nest.
- One call at a time, the core's busy error passed through unchanged; `wakeup()`, `metrics()` and the callback
  window are the exceptions (line 152). `close()` during a pending `poll()` fails.
- An open consumer keeps Node running, and `close()` lets it exit (line 158). A call on a closed consumer still
  holds Node until it settles, as line 158 now says (producer DECISIONS 33).
- The binding keeps no closed flag (line 184). The finalizer calls `_destroy` only for a closed consumer, and never
  while a report is pending (lines 150, 155). A consumer that's never closed runs until the process exits, with no
  warning (line 155, decision 6).
- The preview's `ExperimentalWarning` is sent once per process, shared with the producer
  (`src/internal/lifecycle.ts`); the mock stays silent (line 186).
- Every example and test sets `group.protocol: 'consumer'` (line 141). With `classic`, the core's
  `UnsupportedOperationError` comes through unchanged, and a test asserts its message.

### 4.4 Listeners, commit callbacks and the window (P5)

- A listener is an object with `onPartitionsRevoked({ partitions })`, `onPartitionsAssigned({ partitions })` and
  the optional `onPartitionsLost({ partitions })`. The binding always gives the FFI three functions: a missing
  `onPartitionsLost` runs `ConsumerRebalanceListenerDefaults`, which calls `onPartitionsRevoked`, checked on each
  call (line 56). A missing required method is passed as NULL to `ConsumerRebalanceListener_new_err`, whose
  `LocalNullPointerError` becomes a `TypeError` (line 57).
- An `OffsetCommitCallback` is a function, `({ offsets, exception }) => unknown`, as the producer's `Callback` is.
- A callback may return a Promise. The binding waits for it with `Promise.resolve`, then reports through
  `Consumer__set_callback_result_err`. A throw or rejection crosses as a `LocalCallbackError`, and the delivering
  call fails with it, with Java's wrapping where the core applies it ("User rebalance callback throws an error",
  "Failed to close kafka consumer") (line 148). The core wraps a `LocalCallbackError` even where Java would leave a
  `KafkaException` alone, so the binding puts the user's value back where Java has it (P5.md has the table).
- Calls made while a report is pending (line 149; `consumer-threading.md` §31, §41): the binding counts pending
  reports. The methods the `ConsumerHandle` has go through it: `commitSync`, `commitAsync`, the seeks, `pause`,
  `resume`, `position`, `committed`, the offset and metadata queries and `currentLag` through its `_cb` forms, and
  `assignment()`, `paused()`, `groupMetadata()` and `wakeup()` through its synchronous ones. `metrics()` goes to
  the consumer, which allows it. Every other method has no handle form, goes to the consumer, and the core's busy
  check refuses it with `ConcurrentModificationError` (decision 8). P5.md has the table, where the core's longer
  message shows, and what a mock's handle lacks (ask 16).
- A callback that never settles has no timeout, and `wakeup()` doesn't end it (line 150). The docs say so.
- A listener's registration lives as long as the FFI's docs say, and a pending report keeps its consumer from being
  destroyed (line 150; P5.md).
- `commitAsync`'s callback runs inside the next waiting call (line 65).

### 4.5 Deserializers and `create()` (P6)

- A deserializer is an object `{ deserialize({ topic, headers, data }), close?() }` (line 126). It may return a
  Promise, and a plain value adds no microtask (line 127; decision 4).
- With no deserializer, keys and values are Buffers (lines 55, 125), and a deserializer isn't called for a `null`
  key or value.
- The built-in deserializers follow decision 1.
- The config keys `key.deserializer` and `value.deserializer` take `({ configs, isKey }) => deserializer`. A
  constructor argument wins. The constructor takes only synchronous functions, and `create()` also takes ones that
  return a Promise (lines 131, 156). The core never sees the client's own serde keys. Every other key reaches it,
  the ones a config function reads included, and it lists the ones it doesn't use, as the rules branch's line 132
  says (P2.md §5; P6.md, choice 4(b); ask 18). A config function gets the configs with the `client.id` the parsed
  config generated, after the core has checked the config, as Java's serdes are configured (P6.md, choice 3(a)).
- In those keys, a class name, a serde object and a class each throw `ConfigError` with line 131's hint, which
  names the class, as in "did you mean `() => new StringDeserializer()`?". A function that returns a Promise
  throws `ConfigError` in the constructor, with a message that points to `create()` (line 131). P6 tests each of
  them, with the exact messages; its phase plan fixes how the hint names the class for a class name and for a serde
  object.
- `await KafkaConsumer.create(...)` awaits each config function in turn, the key's first. A failure gives
  `KafkaError('Failed to construct kafka consumer')` with the cause, and closes the deserializers already built
  (lines 156, 157).
- A failing deserializer makes `poll()` reject with `RecordDeserializationError` (already generated), and the
  binding seeks back so that the record isn't skipped, as in Python (line 130). A failing `close()` goes to
  `process.emitWarning` (line 126).

### 4.6 The mock (P4)

- `MockConsumer` wraps the FFI's mock (line 160) and shares the consumer's code through `__as_Consumer`.
- `addRecord(record)` builds a C record with `ConsumerRecordOptionsBuilder` and `ConsumerRecord_with_options_err`,
  carrying what the FFI's mock reads: topic, partition, offset and leader epoch. The addon keeps the app's record
  through one reference until the mock is destroyed, as `add_record`'s doc requires, and its key and value cross
  as the `void *` addresses of that holder's two tags, as the producer mock's do. `poll()` hands back the very
  record objects, as Java's mock returns the records it was given (P4.md).
- Injected errors (`setPollException`, `setOffsetsException`) come back as the very same objects, through the
  producer mock's injected-error carrier.
- `rebalance()` and `losePartitions()` return Promises, since they have `_cb` twins. From P5 they run the
  listener.

### 4.7 Errors, timeouts and checks

- Errors are the core's, converted by the existing `error.c` and `error_payload.c`. The binding makes no checks of
  its own except the temporary `group.id` one: `groupMetadata()` and `committed(null)` without `group.id` throw
  `InvalidGroupIdError` with Java's message (lines 119, 183).
- A `null` timeout throws `TypeError('timeout must not be null')`. A negative one goes to the FFI, which rejects it
  with `IllegalArgumentError('Timeout must not be negative')` (lines 79–83). A `CloseOptions` whose timeout isn't
  set reaches the FFI without one, so `close` waits up to Java's default, 30 seconds (decision 7).
- A crash the core catches comes back unchanged. The closed getters are decision 11.

## 5. Phases

Each phase gets its own Actor and Critic number, a short phase plan approved before it starts (decision 18), and
the full gate before its hand-off (§7).

| Phase | N | Delivers | Needs |
|---|---|---|---|
| P1 Value types and config | 150 | the plain TypeScript types, `ConsumerConfig`, the listener and commit-callback interfaces | nothing: Node is installed (decision 15) |
| P2 Native core, bytes only | 151 | the `Consumer` interface, create, `poll()` and the records, subscriptions, `close()` | P1 (decisions 2, 3 and 7 are made, and the plain `Headers` it needs is in the base) |
| P3 Offsets and commits | 152 | commits without callbacks, positions, seeks, offset and metadata queries | P2 |
| P4 MockConsumer | 153 | the mock over the FFI, without listeners | P3 |
| P5 Listeners and commit callbacks | 154 | the listener, `commitAsync` callbacks, the callback window | P4 |
| P6 Deserializers and `create()` | 155 | the `Deserializer` interface, the built-ins, the config keys, `create()` | P5; the reviewer's answer on Q50, or line 127 as written (decision 4) |
| P7 Broker tests, typing, docs | 156 | the remaining broker tests, type tests, docs, linter coverage, measurements | P6 |
| P8 gRPC and verifiable consumer | 157 | `ConsumerService`, the `__grpc_js` consumer arm, the verifiable consumer | P7; the admin branch landed (decision 14) |

**P1 — value types and config (N=150).** No native code.
- `ConsumerRecord`: a data class, added to the linter's list (`DATA_CLASSES` in `scripts/lint/java_ts.js`); Java's
  three public constructors as one constructor, by the Signatures rules. Its headers go through the `headers`
  converter of `src/internal/payload.ts` (line 138): a given `Headers` is kept as it is, `null` throws
  `IllegalArgumentError('Headers cannot be null')`, Header objects and `[key, value]` pairs make a new `Headers`,
  and the short form uses `new Headers()`.
- `ConsumerRecords`, with Java's methods, `[Symbol.iterator]` and `empty()`. `records` returns a new array, for a
  topic too (P1.md, choice 7(a)).
- `OffsetAndTimestamp`, `SubscriptionPattern` and `GroupProtocol`.
- `ConsumerGroupMetadata`: Java's constructors are deprecated, so a private constructor and one `@internal` static
  factory (line 48), for the addon.
- `CloseOptions` and `GroupMembershipOperation`: Java's statics, `with*` methods and getters, and the options
  classes' constructor taking each setter's key, through `src/internal/options.ts`, copied unchanged from the admin
  branch to the same path (decision 14; `P1.md`).
- `ConsumerConfig`: its constants, configs type and key table, by adding the consumer to
  `rust/xtask/src/config_consts/js.rs` and `rust/xtask/src/config_types/js.rs`. The cargo gates apply, and
  `cargo xtask check-generated` must pass. The configs type types `key.deserializer` and `value.deserializer` as
  plain keys, since line 131's function type needs the `Deserializer` interface, which comes in P6; P6 gives them
  that type.
- Shared code: `show`, Java's `toString()` of a key or value, moves from `src/producer/producer_record.ts` to
  `src/internal/java_string.ts`, so `ProducerRecord` and `ConsumerRecord` share it (DoD #6).
- The linter: `scripts/lint/rules/overload_shape.js` gets `ac3334412`'s three changes, word for word, with its test
  in `test/lint/overload_shape.test.js`, and two `LEFT_OUT` entries for the `subscribe(Pattern…)` forms (line 76);
  `scripts/lint/rules/type_mapping.js` gets `ac3334412`'s two bare-names hunks, word for word (P1.md, choice 6(a));
  `scripts/lint/java_ts.js` gets `ConsumerRecord` in `DATA_CLASSES` and `OffsetCommitCallback` in `FUNCTION_TYPES`.
  `eslint-baseline.txt` loses the `KafkaProducer(Properties,Serializer,Serializer)` line the copied change makes
  stale, and gets five commented lines for shapes the conventions decide and the linter doesn't model:
  `ConsumerRecords#iterator`, `CloseOptions`' keyed constructor and the listener's three methods (P1.md, choice
  8(a)), the last three with `test/lint/runner.test.js` comparing each finding key once (P1.md, choice 9(a)). `lint-mapping.json` gets the names Java doesn't have: `toJSON` on the value classes and enums,
  `ConsumerRecords.[Symbol.iterator]` and `ConsumerConfigs`. `java-index.json` doesn't change: it already has every
  class P1 needs.
- `ConsumerRebalanceListener`, with `src/internal/consumer_rebalance_listener_defaults.ts`, and
  `OffsetCommitCallback`.
- The docs and tests write one-value calls in the named form, such as `CloseOptions.timeout({ timeout: 5000 })`,
  and each method's tests check the bare form once (decision 19).
- Tests from Java: `ConsumerRecordTest` (2), `ConsumerRecordsTest` (7 of 8: the eighth,
  `testNextOffsetsLogsErrorPeriodicallyWhenConstructedWithDeprecatedConstructor`, tests the deprecated
  `ConsumerRecords(Map)`), `CloseOptionsTest` (4) and `ConsumerGroupMetadataTest` (2 of 5: `testAssignmentConstructor`
  and `testGroupIdConstructor`, through `internalCreate`; the three null checks test the deprecated constructors, so
  they're skipped, with the reason in the test file).
- Tests from the core's translations, in `rust/src/consumer/`: for the classes Java doesn't test,
  `offset_and_timestamp.rs` (6), `subscription_pattern.rs` (3) and `group_protocol.rs` (4 of 5); and, as extras
  for classes Java does test, `close_options.rs` (3) and `consumer_group_metadata.rs` (1, `test_display`).
  - Of `group_protocol.rs`, `test_display_lowercase` is left out (the core prints lower case, Java prints
    `CLASSIC`), `test_of_invalid` asserts Java's message, not the core's, and `test_from_str_roundtrip` becomes the
    test of `of`'s bare form.
  - Of `consumer_group_metadata.rs`, two tests repeat Java's, and two test Rust-only code
    (`test_display_through_trait_object`, `test_copy_of`).
- In all, 15 tests from Java and 17 from the core. Also tests of `ConsumerRecord`'s headers (kept, made from pairs,
  the exact errors) and type tests for the new shapes.

**P2 — native core, bytes only (N=151).**
- The `Consumer` interface, moved from P1 beside `KafkaConsumer`, which implements it, as the producer's `Producer`
  landed with `KafkaProducer` (P1.md, choice 5(a)), with the members left out listed in its docs. P2.md is the
  phase plan, with the linter probe's findings (its §6). What P1's checks found, as the user decided it on
  2026-11-26:
  - the interface grows phase by phase, as the admin branch's `Admin` did: P2 has P2's members only, and the
    members P3, P5 and P6 add are baseline lines until then, with no stubs (P2.md, choice 2(a)). This replaces
    the 42 Promise-returning signatures keeping their findings until P3;
  - Java's `KafkaConsumer` and `MockConsumer` name `subscribe`'s listener `listener`, where the interface says
    `callback`: P2 copies the admin branch's `java_ts.js` changes word for word (now `fc45e8ba7` and `bdc5bf9b7`,
    once `ac3334412` and `fc71b3525`), with `35eff7d6d`'s `valueOf` change and test (choice 3(a));
  - the type rows for `listTopics` and `currentLag` (the admin branch's `508b3da9d` and `35eff7d6d`, once
    `ce6493d9e` and `3462a2ebc`) move to P3, with those methods;
  - the left-out `Pattern` forms: `type_mapping.js` skips the `LEFT_OUT` forms, with a test (choice 4(a)), and
    `LEFT_OUT` gets `KafkaConsumer`'s entries, `MockConsumer`'s in P4.
- `native/consumer.c`: construction (`ConsumerConfig_new`, then `KafkaConsumer_new` with NULL deserializers, then
  `__as_Consumer_err`), the notify hook, the drains and the finalizer (4.3); `close()` and `close(options)`
  (decision 7).
- `poll()` and the records (4.2), with their headers copied out on first use (line 137).
- `subscribe` (topics or a `SubscriptionPattern`), `assign`, `unsubscribe`, `assignment()`, `subscription()`,
  `paused()`, `pause`, `resume`, `wakeup()` and `metrics()`, and what the three getters do on a closed consumer
  (decision 11).
- The preview warning; an interim refusal of deserializer arguments and config keys (decision 13).
- Tests: `ConsumerConfigTest`, 9 of 17 ported through `new KafkaConsumer`, as `SaslConfigsTest` was for the producer
  (producer DECISIONS 28); 30 broker-less `KafkaConsumerTest` cases for these methods (P2.md §2); one broker round
  trip, with the JavaScript producer writing and the JavaScript consumer reading; the batch's lifetime in the broker
  memory probes (P2.md, choice 6(a)).
- A test of the copy path that Electron needs (4.2): a broker memory probe in
  `test/integration/memory_probes.test.js`, with a test-only switch that makes the addon act as if Node-API had
  refused external buffers. It checks that:
  - the keys and values are copies, compared by address, as `recordSpy()` does for the producer;
  - the copies don't count toward the batch: once nothing but copies is left, the batch is destroyed, though the
    test still holds every key and value. That's right after `poll()` for records with no headers (4.2, P2.md
    choice 5(a)), and right after the last record's `Headers` is read otherwise;
  - the copies stay valid after that, and after `close()`, under the probes' poisoned allocator;
  - without the switch, the same records keep their batch until their Buffers are collected.
- The switch and the read-back are built and named as the producer's test-only native code is: registered in
  `addon.c` with the rest, always built and doing nothing until a test arms them, declared in
  `src/internal/native.ts` as "Test-only", listed with their test in `check-native-registry`'s `TEST_ONLY`, and
  reached through `dist/internal/native`, as `recordSpy()` and `faultNext()` are. A mutant in
  `test/memory_mutation/` (a copy that still holds the batch) shows the probe fails. A real Electron run stays out
  of CI.

**P3 — offsets and commits (N=152).**
- `commitSync` (four forms), `commitAsync()` without a callback, `committed`, `groupMetadata()` with the temporary
  `group.id` check, `position`, `seek` (to an offset or an `OffsetAndMetadata`), `seekToBeginning`, `seekToEnd`,
  `beginningOffsets`, `endOffsets`, `offsetsForTimes`, `partitionsFor`, `listTopics` and `currentLag`;
  `enforceRebalance` is dropped (decision 9, reversed by the user on 2026-11-26).
- The 14 methods join both `Consumer` and `KafkaConsumer`, removing 28 of the 30 baseline lines (`enforceRebalance`'s
  two stay, for the dropped method); the type rows for `listTopics` and `currentLag` come with them; `commitAsync`'s
  two callback forms are `overload-missing` until P5.
- Tests: the matching `KafkaConsumerTest` cases, with exact messages (6, and the commit halves of the two
  default-group-id cases P2 starts); 13 of the 14 broker-backed `KafkaConsumerTest` cases, in
  `test/integration/kafka_consumer_broker.test.js` (P3.md, choice 2(a)), and broker round trips for commits,
  `committed`, seeks and `position`.

**P4 — MockConsumer (N=153).**
- 4.6, with every method of the FFI's mock. The members the FFI lacks are left out and listed (§6).
- Tests: the ten `MockConsumerTest` cases without a listener (P4.md, choice 2(a)): `testSimpleMock`,
  `testConsumerRecordsIsEmptyWhenReturningNoRecords`, `shouldNotClearRecordsForPausedPartitions`,
  `endOffsetsShouldBeIdempotent`, `testDurationBasedOffsetReset`, `testRe2JPatternSubscription` (without its
  `subscribe(pattern, null)` assertion), `shouldReturnMaxPollRecords`, `testLosePartitionsRemovesFromAssignment`,
  `testLosePartitionsThrowsIfNotAssigned` and `testLosePartitionsClearsOnlyLostRecords`.

**P5 — listeners, commit callbacks and the window (N=154).**
- 4.4.
- Tests: the three `MockConsumerTest` cases with a listener (`testRebalanceListener`,
  `testLosePartitionsCallsOnPartitionsLost`, `testLosePartitionsThenRebalance`; P4.md, choice 2(a)), and
  `testRe2JPatternSubscription`'s `subscribe(pattern, null)` assertion; of `KafkaConsumerTest`, the broker-backed
  `testSubscriptionOnInvalidTopic`, which subscribes with a listener (P3.md, choice 2(a)), and Java's listener in
  `verifyPollTimesOutDuringMetadataUpdate`, which its port left out (Java's other cases with a listener run the
  classic protocol or script a `MockClient`, and are left out with their reasons); the two tests
  `consumer-threading.md` §31 requires (a `commitSync` inside `onPartitionsRevoked` succeeds, and `poll()` stays
  pending until the listener's Promise settles); one test for each row of the window's table (P5.md).

**P6 — deserializers and `create()` (N=155).** Decision 1 is (a); decision 4 waits on the reviewer.
- 4.5. The built-in classes, with Java's test vectors in `test/java_vectors.json`. The interim refusal goes.
- Under decision 1(a), the producer side too: the ten serializer classes, `KafkaProducer.create()`, and the removal
  of `stringSerializer()`, with the producer's docs, example and tests.
- Tests: the twelve `SerializationTest` cases in scope (`allSerdesShouldRoundtripInput`,
  `allSerdesShouldSupportNull`, the three `floatDeserializerShouldThrowSerializationExceptionOn*` cases,
  `floatSerdeShouldPreserveNaNValues`, `testSerializeVoid`, `testDeserializeVoid`,
  `voidDeserializerShouldThrowOnNotNullValues`, `testBooleanSerializer`, `testBooleanDeserializer`,
  `booleanDeserializerShouldThrowOnEmptyInput`), and the deserializer cases of `KafkaConsumerTest`. None of
  `ConsumerConfigTest` is left for P6: its two deserializer cases test `appendDeserializerToConfig`, which isn't
  translated (P2.md §2). Left out, with the reasons in the test file: the 19 `listSerde*` cases, `ListSerializerTest`
  and `ListDeserializerTest` (the List serdes are left out, line 128); `testSerdeFromUnknown` and
  `testSerdeFromNotNull` (`Serdes` isn't translated); `stringSerdeShouldSupportDifferentEncodings` and
  `stringSerdeConfigureThrowsOnUnknownEncoding` (UTF-8 only, no `configure()`); `stringDeserializerSupportByteBuffer`
  (the ByteBuffer serdes are left out).

**P7 — broker tests, typing, docs and linter coverage (N=156).**
- The broker tests of P3 to P6, also over the SSL and SASL_SSL listeners
  (`make -C javascript test-integration-ssl` and `test-integration-sasl`).
- `tsc --strict` type tests, TypeDoc and the API report.
- Linter coverage: each check that applies to the consumer covers its code, for example `zero_copy` on the receive
  path, `cb_twin_only`, `waiting_call_promise`, `single_drainer`, `notify_hook`, `tsfn_lifetime`,
  `native_wrappers`, `default_helpers`, `data_objects`, `no_closed_flag`, `ownership`, `out_err` and
  `java_completeness`. A check that knows only the producer is extended, with tests in `test/lint/`.
- Memory probes. Two measurements, reported and not acted on: the per-record cost of freezing records (Q9), and of
  external buffers against copies (Q24).
- The README, the consumer's part of the migration guide, the CHANGELOG's consumer section (P1 to P6 write none),
  `examples/consumer.js` and the doc-examples test. The docs list the anti-patterns the JS conventions name (Q46).

**P8 — gRPC backend and verifiable consumer (N=157).** After the admin branch has landed (decision 14).
- `ConsumerService` (30 RPCs, `rust/multilanguage-test-server/proto/consumer_service.proto`) in
  `javascript/grpc-server/`.
- A `__grpc_js` arm in `multilanguage_consumer_test!` (`rust/tests/common/multilanguage_consumer_test_macro.rs`), a real consumer in
  `JsGrpcFactory` (`rust/tests/common/backend_factory.rs`, a stub today), and `JS_GRPC_SKIPS` in `rust/Makefile`
  trimmed to what's left out. The cargo gates apply.
- `javascript/verifiable_clients/verifiable_consumer.js`, beside `verifiable_producer.js`, with Python's
  `python/verifiable_clients/verifiable_consumer.py` as the reference, and its chaos runs.

### 5.1 The Java tests

| Java test | Cases | Where |
|---|---|---|
| `ConsumerRecordTest` | 2 | P1, all |
| `ConsumerRecordsTest` | 8 | P1, 7 (the eighth tests the deprecated `ConsumerRecords(Map)`) |
| `CloseOptionsTest` | 4 | P1, all |
| `ConsumerGroupMetadataTest` | 5 | P1, 2: the two getter tests, through `internalCreate`; the three null checks are skipped, as they test the deprecated constructors |
| `OffsetAndMetadataTest` | — | already ported |
| `ConsumerConfigTest` | 17 | P2, 9 through `new KafkaConsumer`; the other 8 skipped, with their reasons (P2.md §2) |
| `KafkaConsumerTest` | 135 | P2 to P6: P2 30, P3 19, P5 1, P6 1, and 84 left out with reasons (P2.md §2; P3.md, choice 2(a)); P7 reruns the broker cases over SSL and SASL_SSL. Python's port is the starting point (36 broker-less, 14 broker-backed, 85 left out with reasons), and each case is re-checked against Java and the FFI |
| `MockConsumerTest` | 13 | P4 (10) and P5 (3) (P4.md, choice 2(a)) |
| `SerializationTest` | 36 | P6, the 12 in scope |
| `ListSerializerTest`, `ListDeserializerTest` | — | left out: the List serdes are |

## 6. FFI gaps: asks to the owner, not Actor work

The binding adds no entry points (line 161). These members are left out, and listed in the module docs, until the
FFI has them:
1. `clientInstanceId(Duration)`, on the consumer and the mock.
2. `registerMetricForSubscription` and `unregisterMetricFromSubscription`, and the mock's `addedMetrics()`.
3. The mock's `schedulePollTask(Runnable)`, `setClientInstanceId`, `disableTelemetry` and `injectTimeoutException`.

Fifteen FFI and core behaviours the binding lives with for now:

4. A negative `CloseOptions` timeout is read as zero (`duration_ms()` in `rust/src/ffi/consumer/close_options.rs`),
   where Java throws "The timeout cannot be negative." It's already a listed deviation (line 81). Ask: Java's check
   in the FFI.
5. The closed synchronous getters (`assignment`, `subscription`, `paused`, `groupMetadata`) panic in the core, so
   the error's message gains the FFI's panic prefix (decision 11). Ask: report them with `precondition::fail`
   (`rust/src/ffi/precondition.rs`), which passes Java's error through unchanged. (P4.md expected the mock's
   `metrics()` once closed to panic too; it does not: see ask 11.)
6. `groupMetadata()` without `group.id` panics in the core too; the binding's temporary check (line 119) catches it
   first. Ask: report `InvalidGroupIdError` through the error slot, so that the temporary check can go.
7. The FFI refuses a NULL collection, pattern or `CloseOptions` at its boundary, before the core's group, closed and
   busy checks, which Java runs first: with no `group.id`, `subscribe({ topics: null })` gives
   `IllegalArgumentError` where Java throws `InvalidGroupIdException`, and a closed or busy consumer gives it where
   Java throws `IllegalStateException` or `ConcurrentModificationException`. `kafka_common_TopicPartition_new`
   refuses a NULL topic, which Java's `TopicPartition` holds, so `pause` and `resume` of a null-topic partition give
   `TypeError` where Java says "No current assignment for partition null-0". Recorded as a deviation, as in Python
   (P2.md, issue 151-3). P3's calls do the same with a null partition, collection or map (`position(null)` gives
   `TypeError`, its message naming the C parameter, where Java says "You can only check the position for partitions
   assigned to this consumer."). The FFI also checks a negative timeout before the core's closed and group checks
   (P3.md, issue 152-2, accepted by the user on 2026-11-26 as a known difference), and its busy check before
   `seek`'s negative offset, which Java checks first. Ask: refuse a
   NULL and a negative timeout only after the core's earlier checks, and accept a null topic.
8. The mock's setters that have no error slot (`update_beginning_offsets`, `update_end_offsets`,
   `update_duration_offsets`, `set_poll_error`, `set_offsets_error`, `schedule_nop_poll_task`) log and ignore a call
   made while another call on the mock is pending (`with_mock_void` in `rust/src/ffi/consumer/mock_consumer.rs`),
   where Java's synchronized mock waits and applies it (P4.md). Ask: report the refusal through the `_err` twin's
   error slot.
9. A NULL value in a map of `TopicPartition` to `int64_t *` is read as 0 (`map_topic_partition_i64` in
   `rust/src/ffi/common/topic_partition.rs`), where Java's mock keeps the null and later reports the partition as
   having no offset. The binding refuses a `null` offset with `TypeError`, as P3's `null` timestamp (P4.md). Ask:
   refuse a NULL value, or read it as absent.
10. The mock refuses nulls Java's mock accepts: a NULL topic or list in `update_partitions`, which Java's mock stores,
    and NULL options in `close_with_options_cb` on the mock's view, which Java's mock ignores, give `TypeError`
    (P4.md), as the producer mock's `partitionsFor(null)` does. Found while building P4: a `PartitionInfo` given to
    `update_partitions` with a null topic, replica list, replica or node host, which Java's `PartitionInfo` and
    `Node` hold, gets `TypeError` from `PartitionInfo_with_offline_replicas` and `Node_with_rack_is_fenced`; and a
    NULL pattern in `subscribe_with_pattern_cb` on the mock's view is refused at the FFI's boundary with the
    consumer's "Topic pattern to subscribe to cannot be null", where Java's mock says "Topic pattern cannot be null".
    Also: on the mock's view, `partitions_for_cb` and `current_lag_cb` refuse a NULL topic or partition, where Java's
    mock answers `[]` and `0`, and a NULL collection given to `subscribe_with_topics_cb`, `assign_cb`,
    `seek_to_beginning_cb` or `seek_to_end_cb` gets `KafkaConsumer`'s `IllegalArgumentError`, where Java's mock throws
    `NullPointerException` (a `TypeError` here). A null topic given to `assign_cb` or `subscribe_with_topics_cb` on the
    mock's view is refused too (`TopicPartition_new`, and the list's NULL element), where Java's mock stores it; the
    binding sends it as NULL there, keeping the blank topic of ask 7 for `KafkaConsumer`. Ask: accept them where Java's
    mock does, and let the mock report its own null pattern and null collections.
11. Found while building P4: `kafka_consumer_Consumer_metrics` reads the metrics through the consumer's handle, never
    through the mock, so on a closed mock it answers an empty view, where Java's mock throws "This consumer has
    already been closed." (and the core's mock would panic with that message). Ask: have
    `kafka_consumer_Consumer_metrics` on a closed mock report Java's error, as `MockConsumer#metrics` throws it.
12. Found in P4's review: on the mock's view, the FFI's `ms()` refuses a negative timeout ("Timeout must not be
    negative") from `poll_cb` and every timed `_cb` twin, as on a `KafkaConsumer` (the conventions' rule, Signatures,
    Q35), and such a `poll` does not record its timeout. Java's mock ignores every timeout and records each poll's as
    `lastPollTimeout`. Ask: let the mock take a negative timeout as Java's does.
13. Found in P4's review: the getters `assignment`, `subscription`, `paused` and `group_metadata` on the mock's view
    take the single-owner flag, so they fail with the `ConcurrentModificationException` translation while another call
    is pending, where Java's synchronized mock waits for them and its unsynchronized `groupMetadata()` answers at once.
    Ask: read them as `closed` and `last_poll_timeout` are read, through the handle.
14. Found while planning P5: the FFI refuses a NULL commit callback with `TypeError` ("callback must not be null";
    `require_ref` in `commit_callback_registration`, `rust/src/ffi/consumer/offset_commit_callback.rs`), where Java's
    `commitAsync(callback)` and `commitAsync(offsets, callback)` take `null` and log a failed commit. The binding
    sends `commitAsync({ callback: null })` to `commit_async_cb`, which is Java's `commitAsync()`, so only
    `commitAsync({ offsets, callback: null })` meets it (P5.md, choice 2). Ask: take a NULL callback as Java's null.
    Until then that form types its `callback` non-null, so `tsc` refuses the `null` (the user's choice (a) of
    2026-11-27, P5.md); the type becomes `OffsetCommitCallback | null` once the FFI takes one.
15. Found while planning P5: a commit callback gets an empty map where Java's gets `null`: for a failed commit, and
    for an empty map committed, which Java's `commit()` answers with `completedFuture(null)` (`register_async_commit`
    in `rust/src/consumer/internals/async_kafka_consumer.rs`). The binding passes `null` there for now (P5.md,
    choice 3). Ask: pass NULL offsets there.
16. Found while planning P5: while the mock runs a listener or commit callback (`rebalance_cb`,
    `lose_partitions_cb`, `commit_async_*_cb`), it holds its single-owner flag
    (`kafka_consumer_MockConsumer_rebalance_cb` in `rust/src/ffi/consumer/mock_consumer.rs`), so a call the
    `ConsumerHandle` has no form of gets `ConcurrentModificationError`, and six of the mock's setters are ignored, as
    ask 8 says. The mock's handle runs only the commits, `currentLag` and the synchronous reads: its other async
    operations fail with `UnsupportedOperationError` ("ConsumerHandle async operations are not supported on a
    MockConsumer handle; drive the MockConsumer directly."; `ConsumerHandle::async_state` in
    `rust/src/consumer/internals/async_kafka_consumer.rs`). Java's synchronized mock is reentrant and takes them all.
    Ask: let the mock's handle reach the mock for every operation, and the mock's own calls pass while its callback
    runs.
17. Found in P5's review, by a broker probe: after `unsubscribe()` has run `onPartitionsRevoked`, the core's
    `close()` runs `onPartitionsLost` for the same partitions. Java's `AbstractMembershipManager#leaveGroup` clears
    the assignment snapshot (`notifyAssignmentChange(Collections.emptySet())`), so
    `AsyncKafkaConsumer#runRebalanceCallbacksOnClose` finds nothing to revoke and calls no listener. The binding
    passes it through; no test pins it yet (P7's broker tests can). Ask: clear the assignment snapshot when the member
    leaves, as Java does.
18. Found while planning P6: the copy a client makes of its config handle (`copy_for_client` in
    `rust/src/producer/producer_config.rs` and `rust/src/consumer/consumer_config.rs`, which
    `kafka_producer_KafkaProducer_new` and `kafka_consumer_KafkaConsumer_new` build the client from) parses the
    originals again with an empty record of the keys used, and each client logs its unused keys from that copy. So
    neither client's unused-keys line can leave out a key that only a serde config function read, where Java's
    `originals()` marks it used, and an `ignore()` on the handle (`kafka_producer_ProducerConfig_ignore`,
    `kafka_consumer_ConsumerConfig_ignore`) can't help (P6.md, choice 4(b)). Ask: keep the handle's `used` set in
    the copy. Once it does, the binding marks the keys with `ignore`, an additive change.

## 7. Execution: Actor and Critic

**Roles:**
- Manager: this session (the project-manager agent).
- Actor: `actor-executor`, with the phase's number N.
- Critic: `kafka-critic`, with the same N, writing `COMMENTS.N.md` at the repository root under an exclusive lock.
  It reviews every Actor commit against Java in `kafka/`, the JS conventions and the header, reports only real
  issues, and changes no code.
- Loop, per `agent-roles.md`: Actor, then Critic, then my summary, then the Actor fixes (fixup commits naming the
  commit and the comment, comments moved to `COMMENTS.DONE.N.md`), then the Critic again, until `COMMENTS.N.md` is
  empty and the gate passes.

**Commits and gate:**
- `git commit` with the hooks on (no `--no-verify`, no breakglass), explicit paths only, with no AI co-author
  trailer (decision 16). A hook that blocks a commit or fails stops the work, and I report it. Nothing is pushed.
  Nobody touches `git stash`, switches branches or rebases.
- Every Actor commit, fix-ups included, passes the quick checks, from the root:
  `make test-javascript-unit lint-javascript` (the static, unit, memory and typing tests, `check-api`,
  `typecheck-tests`, the rule linter and its tests, and `format-check`). When Rust changes (P1, P8), also, from
  `rust/`: `cargo build`, `cargo test` for the touched code, `cargo xtask format-check`, `cargo xtask lint`,
  `cargo xtask lint-custom` and `cargo xtask check-generated`.
- The full `make verify-javascript` runs once per round: on the last commit of the Actor's build round, and on the
  last commit of each fix round, before the Critic reviews. A failure there is fixed before the review, and the
  Critic reviews only a state that passed it. The user decided these per-commit checks on 2026-11-25, in place of a
  full `make verify-javascript` on every commit.
- Before a phase's hand-off, the full gate: from `rust/`, `cargo xtask format-check`, `cargo xtask lint` and
  `cargo xtask lint-custom`; `make -C rust doc-check`, as the root Makefile has no `doc-check` target; from the
  root, `make verify` and `make verify-javascript`; and the phase's broker tests
  (`make -C javascript test-integration`). I report every result, including failures caused by the environment.

**Before each phase:**
- Check that `origin/dev/javascript-producer-bindings` is still an ancestor of the branch (`f62d5fec9` on
  2026-11-26). If it moved, stop and report: the user moves the branch.
- Fetch `origin/kraina-js-claude-rules` and diff its conventions against the branch's (§2).
- Check whether the admin branch has landed (decision 14).

**Environment notes for the subagents:**
- Node v22.23.5 and npm 10.9.9 in `/opt/homebrew/bin`, on `PATH`; `javascript/Makefile`'s `NPM_NODE_GYP` fallback
  finds npm's own node-gyp (decision 15).
- Put `~/.rustup/toolchains/1.95.0-aarch64-apple-darwin/bin` first on `PATH`, because the Nix `cargo` has no clippy.
- Use `PIP_INDEX_URL=https://pypi.org/simple` for anything Python.
- Never use `git stash`, and kill processes only by PID.
- Use one shared scratch broker. The brief says the Docker VM has 8 GB; on 2026-11-25 it reported about 15.6 GiB.
- Stage explicit paths only.

**Baseline at `8a25205a3`**, after the move onto `f62d5fec9` (`make verify-javascript`, 2026-11-25, passed): static
75, unit 578 and 1 skipped, memory 21, typing 10, verifiable-clients 127, grpc-server 13, integration 77 and 12
skipped (the SSL and SASL suites), stress 2, lint 440.

**Baseline at `208abeb64`** (`make verify-javascript`, 2026-11-25, 11:24 to 11:42, exit 0):
- Builds: `make -C rust build-all-features` and `make -C javascript build` (node-gyp 11.5.0) passed.
- Tests, by jest project: `static` 74 (10 suites), `unit` 519 passed and 1 skipped of 520 (33), `memory` 19 (6),
  `typing` 10 (2), `verifiable-clients` 127 (6), `grpc-server` 13 (1), `integration` 73 passed and 12 skipped
  of 85 (6), `stress` 2 (1) and `lint` 439 (46). None failed.
- `check-api`: the interface matches `api/` (the baseline of `4.4.0-beta.1`). `typecheck-tests`: 1454 findings in
  85 files outside the test directories, which it doesn't fail on, and none in the test files. The 21 rule-lint
  checks and `format-check` (275 files) passed.
- Skipped, all for the environment: the unit test of `await using` needs Node 24, and this is Node 22; the 12
  integration tests are the SSL and SASL suites, which need brokers that aren't set up here; and
  `test-integration-grpc` runs only on Linux, while this host is Darwin.
- The error lines in the output (callback errors, Node-API conversion errors, the gRPC server's "unexpected
  failure") come from tests that provoke them and pass. Nothing failed, so nothing is marked as code.

**Hand-off after each phase:**
- Update `javascript/design/current/consumer/` (this plan's status and the phase's notes) and "The binding's
  notes" in `javascript/CLAUDE.md`, never the conventions.
- Copy `COMMENTS.DONE.N.md` to `javascript/design/history/consumer/P<k>-<name>/`, with a clear name, then reset
  `COMMENTS.N.md`.
- Check that `design/current/marked_classes.txt` lists the translated classes (it lists every consumer class
  already), and edit nothing there.
- Report to the user.

## 8. Decisions

The user approved these on 2026-11-25, each as recommended, with the choices noted. Two wait on others: row 4 on
the reviewer (Q50), and the merge order in row 14 on the user. Row 5's conventions row (line 220 at `8a25205a3`) is
still parked. Row 6 was settled by PR #312's conventions on 2026-11-25 (line 155).

| # | Decision | Decided (2026-11-25) | Not chosen |
|---|---|---|---|
| 1 | **The built-in serdes.** Line 128 says they are Java's classes, built with `new`, UTF-8 only for String and UUID, with "no serde factories". The producer shipped the factory `stringSerializer()` with Java's six charsets (producer DECISIONS 17, made while the rules named that factory). Lines 25, 64 and 156 also list `KafkaProducer.create()`, which the producer doesn't have. | **(a)** Both clients follow line 128. P6 builds the ten deserializer classes and the ten serializer classes, removes `stringSerializer()` and its charsets (a preview interface change, which line 197 allows during development), updates the producer's docs, example and tests, and adds `KafkaProducer.create()`. Java's round-trip tests need both halves. | (b) Factories for both clients, which needs the rules' owner to change line 128. (c) Classes in the consumer only, the producer keeping its factory for now, so the two clients differ. |
| 2 | **Can a consumed record's `Headers` be changed?** Settled by line 137 (Q31, decided on 2026-11-24). | Yes. The `Headers` copies its list out of the record's lent headers on the first read or change, as line 137 says (4.2): no `RecordHeaders_with_record_headers_err` handle, `record.headers` is never `null`, and the native record stays alive until the copy. | This plan's first recommendation (a copy on the first change only, reads going to the lent headers); a copy at every poll; read-only headers. |
| 3 | **Header pairs, and `Header` validity.** Settled by lines 135 and 138 (Q31, Q60). | Plain TypeScript classes, the core getting a copy only at `send()` (line 135). Pairs wherever Java takes `Headers` or `Iterable<Header>`, typed `Iterable<Header \| readonly [string, Uint8Array \| null]>`, with the exact messages `TypeError('headers[1] key must be a string; got number')`, "Header cannot be null." and "Null header keys are not permitted"; a given `Headers` is kept (line 138). A `Header` the app has read stays valid however its `Headers` changes (line 135). The Electron limitation covers only consumed keys and values (4.2). | Java's `Headers` or `Iterable<Header>` only, with no pairs. |
| 4 | **Must every serde method be asynchronous?** Row at line 220 (Q50; line 219 at `8a25205a3`), still open. | The user asks the reviewer. If there's no answer by P6, P6 implements line 127 as written, as the producer does, and both clients change together if the answer is "all asynchronous". | Hold P6 until the answer comes. |
| 5 | **Schema Registry's config functions.** Row at line 221 (line 220 at `8a25205a3`), still parked. | Only the generic config-function mechanism (producer DECISIONS 16); no Schema Registry code and no reading of its keys. | — |
| 6 | **The never-closed warning.** Row at line 219 (Q21, still open), and line 155. Settled since: at `8a25205a3`, line 155 says nothing warns, and the row is gone. | None, as for the producer (producer DECISIONS 35, the owner's choice of 2026-11-20): an open consumer keeps Node running, so the warning could never fire. No (conventions conflict) mark is needed any more. | Wait for the design discussion. |
| 7 | **`CloseOptions` without a timeout.** Line 79 says a `null` timeout means the default; line 171 says to send it as `-1`, which `duration_ms()` reads as zero (line 81), a close that doesn't wait. Read literally, the rule breaks `close()` (producer DECISIONS 4's exception). | Build the C options as Java does, as Python and .NET do: with a timeout, `CloseOptions_new_with_timeout_err(ms)` and then `CloseOptions_with_group_membership_operation_err` if the operation isn't the default; without one, `CloseOptions_with_operation_err(op)`, which leaves the timeout unset, so `close` waits up to Java's default of 30 seconds. Marked (conventions conflict) in the binding's notes, citing lines 79, 81 and 171. | Send `-1` as line 171 reads, and `close(options)` without a timeout doesn't wait. |
| 8 | **Which calls the callback window refuses.** Line 149 names `poll`, `subscribe`, `assign` and `close`. `unsubscribe` and `subscription()` have no `ConsumerHandle` form either, so the core refuses them too. | Follow the FFI and Python's list (producer DECISIONS 2): document and test every refused method; `metrics()` and `wakeup()` work. The exact routing, including where the core's longer handle message shows, goes in P5's phase plan. | — |
| 9 | **`enforceRebalance` and the mock's `shouldRebalance()` / `resetShouldRebalance()`.** Java's `AsyncKafkaConsumer.enforceRebalance` only logs that it's unsupported, and Python drops all three. The JS conventions have no such rule, and the FFI has all three. | Drop them, as Python does: reversed by the user on 2026-11-26 ("remove it entirely as python"). `enforceRebalance()` and `enforceRebalance(reason)` aren't translated, and the mock's two methods serve only them. The P3 follow-up (`P3-followup.md`) removed what P3 had built. | Translate them: `enforceRebalance()` and `enforceRebalance(reason)` return Promises (their `_cb` twins), the two mock methods are synchronous (the first choice, 2026-11-25). |
| 10 | **Which consumer classes are data classes.** Line 46 lists "the records, `TopicPartition`, `OffsetAndMetadata`, `RecordMetadata`, `Node` and the admin description classes". | `ConsumerRecord` only. `ConsumerRecords`, `OffsetAndTimestamp`, `SubscriptionPattern` and `ConsumerGroupMetadata` keep methods. | Ask the rules' owner to add `OffsetAndTimestamp`, which matches `OffsetAndMetadata`. |
| 11 | **Q61: the closed getters' error.** Line 184 says it isn't checked yet. It is now: `assignment()`, `subscription()`, `paused()` and `groupMetadata()` on a closed consumer throw `IllegalStateError('Rust panic caught at the FFI boundary in <function>: This consumer has already been closed.')`. | Accept it for the preview: the binding's notes record it, tests assert the exact text, and FFI ask 5 goes to the owner. | Hold P2's hand-off until the FFI passes Java's message through. |
| 12 | **The FFI gaps of §6.** | Leave those members out, list them in the module docs, and send asks 1–6 to the FFI's owner. | Wait for the FFI before the affected phases. |
| 13 | **Starting before decisions 1 and 4.** | Yes: P1 to P5 go ahead. Until P6, a deserializer argument or config key is refused with an explicit `UnsupportedOperationError` (root `CLAUDE.md` §7), tested, never released, and removed in P6. | Wait for both before P2. |
| 14 | **The admin branch.** `origin/dev/javascript-admin-client` (`2ae721f97`, 18 commits on `208abeb64`; since 2026-11-25 `0cd327cf5`, 20 commits on `f62d5fec9`) edits the same shared files: `native/addon.c`, `binding.gyp`, `util.h` and `producer.c`; `src/internal/args.ts`, `config.ts`, `native.ts`, `java_object.ts` and others; 16 linter rules and `scripts/lint/java_ts.js` (`DATA_CLASSES`); the xtask config generators; `grpc-server/`; the generated API, index and baseline files; the root, `javascript/` and `rust/` Makefiles; the docs; and "The binding's notes" (not the conventions). It adds `src/internal/options.ts` (`applyOptionKeys`). | The user picks the merge order. Where both need a helper, the consumer follows the admin branch's shape, and the second branch to land removes the duplicate (DoD #6). P1 copies `options.ts` unchanged, to the same path (`P1.md`). P8 waits for the admin branch. Nobody rebases on their own. | Base this branch on the admin branch now, which ties the consumer to an unmerged branch. |
| 15 | **The environment.** Node and npm weren't installed, so `make verify-javascript` failed at `build-javascript`. | Done: Node v22.23.5 and npm 10.9.9 are in `/opt/homebrew/bin` (§7). | — |
| 16 | **Commit trailer and hooks.** The session's reminder asks for an AI co-author line. | None, as the approved producer plan says, and the hooks stay on: no `--no-verify`, no breakglass. | Add the line. |
| 17 | **Agent numbers.** | 150 to 157, one per phase, as the Manager's rules ask. | 150 for the whole milestone, as the producer used 100. |
| 18 | **Phase plans.** | A short plan per phase for your approval before it starts, as the Manager's rules ask. | Waive them, and approve all eight phases with this plan. |
| 19 | **How one-value calls are written.** Line 73 lets a one-parameter call take the value on its own, `poll(100)`, or the options object, `poll({ timeout: 100 })`. | The named form in the consumer's docs, examples and tests; each method's tests also check the bare form once, since it's part of the API. Both forms stay, as line 73 requires, so no rule changes. The named-only change is a future improvement (§10). Decided by the user on 2026-11-25. | The bare form in the docs, as line 73's examples write it. |

## 9. Risks

1. The admin branch and this one edit the same shared files; whichever lands second has merge work (decision 14).
2. PR #312 has been force-pushed twice in two days, and the base moved again, by five commits, on 2026-11-25
   (`675c9a99d` to `208abeb64`), then was rewritten the same day (`f62d5fec9`), and the user moved the branch onto
   it. If the base moves, work stops until the user moves the branch.
3. Resolved on 2026-11-25: Node is installed (decision 15).
4. One kept key or value Buffer keeps its whole poll batch in memory (4.2). Documented, and measured in P7.
5. A batch outlives its consumer by the FFI's design (4.2), but no test checks it yet. P2's broker memory probes do,
   under the allocator's poisoning; under ASan they're a Future improvement (P2.md, choice 6).
6. The mock keeps each added key and value until it's destroyed (4.6). Fine for a test double, and documented.
7. Building, freezing and wrapping records costs time per record (Q9, Q24). Measured in P7, not acted on.
8. A listener that never settles holds the rebalance and the call that delivered it, with no timeout (line 150).
9. Two future improvements the consumer reaches aren't built (producer DECISIONS 3): a closed consumer collected
   with an answer still queued runs that answer during garbage collection (Q49), and a call inside a callback,
   after its first `await`, that waits for that callback hangs (Q46). The docs list both.
10. The rows still parked in the conventions (decisions 4 to 6) may be decided against the recommendation, which
    reshapes P2 or P6.
11. Python's split of `KafkaConsumerTest` is only a starting point. Python's mock is pure Python, while this one
    wraps the FFI, so each case is re-checked.
12. P6 changes the producer's serde shape too (decision 1(a)): `stringSerializer()` goes, so the producer's docs,
    example and tests change in the same phase.
13. The copy path that Electron needs runs only under P2's test switch, which gives the answer Electron's Node-API
    gives. No CI job runs Electron (4.2).

## 10. Future improvements

Decided, and not built in this milestone:
- Every call names its arguments, one-value calls too: `poll(100)` and the producer's `send(record)` go, and only
  the named form, such as `poll({ timeout: 100 })`, stays. Not built: it needs a change to line 73, which belongs to
  the rules branch. Line 73 says a released bare value keeps its form, so the bare forms have to be dropped before
  they're released; nothing is released yet (`javascript/CHANGELOG.md`: `4.4.0-beta.1`, not published). The user
  decided it on 2026-11-25. Until then the consumer keeps both forms, as line 73 requires (decision 19).
- Refusing a call made inside a callback after the callback's first `await`, when the call waits for that same
  callback (Q46, line 213). In a send callback, `flush()`, `close()`, the transaction calls and awaiting a later
  record's future wait for that same callback, and the core refuses them only before the callback's first `await`.
  From that point the producer can no longer tell it's inside the callback, so these calls can hang, as line 149
  already warns. The consumer reaches the same case (risk 9). The fix carries an "inside this callback" mark across
  `await`s with Node's `AsyncLocalStorage`, so the binding throws instead, as Python does with a `ContextVar`. A
  spike measures its cost first, since send callbacks run once per record (line 213). The user asked on 2026-11-25
  for it to be fixed last, after P8. Not built until then: line 213 lists it as a future improvement, which an
  agent builds only once a rule takes its place, and that rule belongs to the rules branch.
