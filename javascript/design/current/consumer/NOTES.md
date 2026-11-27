# The consumer's addon notes

These are the consumer's parts of "The binding's notes" (`javascript/CLAUDE.md`, from line 227), written here as that
section would hold them, for the rules' owner to move there when they choose (P2.md, choice 7(a)). They describe what
the JS conventions leave to the binding, as the producer's notes do, and point to the conventions' group that states a
rule rather than repeat it. "Line N" is line N of `javascript/CLAUDE.md` at `8a25205a3`.

## The handle

- `native/consumer.c` holds one `Consumer` per `KafkaConsumer`: the owned class handle (`kafka_consumer_KafkaConsumer_t`,
  built from `ConsumerConfig_new` and `KafkaConsumer_new` with NULL deserializers), its borrowed
  `KafkaConsumer__as_Consumer_err` view, through which every call is made, the reentrancy handle
  (`Consumer_handle_err`, destroyed after the class handle), through which the window's calls go (below), and the
  producer's callback queue (`callback_queue.c`, `CallbackQueue`). The configuration's `kafka_Map_t` is built by `config_map_build`
  (`native/values.c`), and parsed into a configuration handle by `native/config.c`, which the producer's constructor
  uses too (below, "The deserializers and the configuration").
- Every waiting call goes through its `_cb` twin (JS conventions, Futures and waiting): `poll_cb`,
  `subscribe_with_topics_cb`, `subscribe_with_pattern_cb`, `assign_cb`, `unsubscribe_cb`, `pause_cb`, `resume_cb`,
  `close_cb` and `close_with_options_cb`, and the offset calls' twins (below). The three getters and
  `groupMetadata()` go through their fallible forms (`kafka_consumer_Consumer_assignment`, `_subscription`, `_paused`,
  `_group_metadata`), `wakeup()` and `metrics()` through their `_err` twins. Each `_cb` call has its own context
  (`CallCtx`, `PollCtx`, `CloseCtx`), the completion's opaque, freed by the completion.
- A list the addon builds with `kafka_List_new` (the topics, the partitions) only borrows its elements: the topics'
  strings and the `TopicPartition`s are freed after the call (`list_free`, `tp_item_destroy`), which copied them. A
  list the core returns owns its elements: destroying it frees them (`tp_list_take_to_js`, the subscription's strings).
- A null where Java takes a collection, a pattern or an option crosses as NULL, and the FFI's error is the answer (JS
  conventions, Errors: the binding repeats no precondition). A null topic, or a null partition in `assign`, crosses as a
  blank topic, so the core answers with Java's message for a null or empty one ("Topic collection to subscribe to cannot
  contain null or empty topic", "Topic partitions to assign to cannot have null or empty topic"), as Python's binding
  sends them; a null element in `pause` or `resume` crosses as NULL, which the FFI refuses ("partitions must not contain
  null").
- One call at a time (JS conventions, Threads and callbacks): the binding keeps no count and no lock; the core's busy
  error, "KafkaConsumer is not safe for multi-threaded access.", comes back as it is. A close refused before the core runs
  closes nothing, so it does not record the consumer closed (`close_on_complete`): the busy refusal
  (`LOCAL_CONCURRENT_MODIFICATION`), and the FFI's refusal of NULL options, `close({ option: null })`
  (`LOCAL_NULL_POINTER`, "options must not be null").
- A RegExp given as a pattern is refused with a `TypeError` that names `SubscriptionPattern` (JS conventions,
  Signatures, Q15).

## The deserializers and the configuration

- A consumer runs its deserializers in JavaScript, inside `poll()` (line 124), over the records the core returns with
  NULL deserializers: `deserializeBatch` (`src/internal/deserialize_batch.ts`) calls the key's, then the value's, each
  only for a key or value that is not null, given the record's own `Headers` (its lazy copy, line 135) and the
  `Buffer` over the fetched bytes, and builds the record again with what they returned
  (`ConsumerRecord.internalWithKeyValue`), sharing its `Headers`. A plain result adds no microtask: a batch whose
  deserializers return plain values settles `poll()` inside the native completion. A thenable is awaited, record by
  record (line 127), and a `then` that cannot be read is the deserializer's failure. A consumer without deserializers
  builds the batch as before, with no pass over its records. While a deserializer's Promise, or a seek back, is awaited,
  the core's poll has already answered, so the core takes another call of the consumer's meanwhile, where Java's
  `poll()` holds the consumer until it returns: `position()` answers past the whole batch, and `assignment()`,
  `pause()`, `resume()` and a second `poll()` go through *(deviation, accepted by the user on 2026-11-27. Why: the
  deserializers run in JavaScript after the core's poll. Goes when: the core runs the deserializers through the FFI)*.
  Python's sync `KafkaConsumer` runs its deserializers after releasing its guard (`_ConsumerState._run_sync` resolves
  after `with self._use()`), so another thread's call goes through while they run, as it does between its seeks back,
  which are separate guarded calls. Its asyncio consumer runs them on its event loop, where no other task runs
  meanwhile, so its only gaps are between its seeks back. Python refuses an `async def` deserializer, so only the wait
  for a deserializer's Promise is this binding's own. `test/integration/consumer.test.js` pins it.
- A failing deserializer ends the batch where Java's `FetchCollector` ends it, as Python's binding does: the records
  before it are returned, and `RecordDeserializationError` is the failure of a poll that has none, with Java's message
  and fields (`CompletedFetch.newRecordDeserializationException`) and the thrown value as its `cause`, `null` and
  `undefined` included. The partition cut short gets, as Java's `CompletedFetch.fetchRecords` leaves
  `nextFetchOffset`, the last returned record's offset + 1 as its next offset, or none, and the partitions after it
  none. The core has already moved past the whole batch, so before `poll()` settles each partition is sought back to
  its first record not returned (`seek_with_offset_cb`, one after another), and a seek that fails is a process
  warning *(deviation, line 130, as in Python: Java never moves past the record. Why: the deserializers run in
  JavaScript after the core's poll)*. A record that failed to deserialize is fetched again by the next poll, which
  fails on it again, as Java's cached exception does, until the app seeks past it.
- The serdes' `close()` runs once the core reports the consumer closed by that call (`closedNow`), and what it throws
  goes to `process.emitWarning`, a thrown `null` or `undefined` included. `MockConsumer` takes no deserializers, as
  Java's typed mock doesn't, and has no `create()` (line 156).
- Both clients are built in Java's order (`src/internal/client_construction.ts`): the configuration is parsed first
  (`configCreate`, `ConsumerConfig_new` or `ProducerConfig_new`), so the core's `ConfigError` comes before any serde
  config function runs; the functions get the configuration with the `client.id` it generated
  (`ConsumerConfig_get_string`, `ProducerConfig_get_string`), as Java configures a serde with its `originals()` and
  that `client.id`; and the client is built from the same handle (`KafkaConsumer_new`, `KafkaProducer_new`), which is
  freed on every path (`configFree`), its handle object's finalizer freeing one never freed. `create()` parses before
  it awaits the functions and holds the handle across its awaits. The serde config keys' own checks (a class name, a
  serde object, a class, with the hint "did you mean `() => new X()`?" naming the class) run before the parse, as Java
  reads its class names while it parses.
- The keys a serde config function reads reach the core, which lists the ones it doesn't use *(deviation: Java's
  `originals()` marks them used, so Java doesn't list them. Why: a client copies its config handle (`copy_for_client`)
  without the handle's record of the keys used, so an `ignore()` on the handle can't reach the client's unused-keys
  line. Goes when: ask 18; the binding then marks them with `ignore`, an additive change)*. The client's own serde keys
  never reach the core (line 131), and the other client's do.
- The built-in serdes (line 128) are plain TypeScript, translated from Java's classes and checked against Java's bytes
  and messages (`test/java_vectors.json`). A deserializer's messages are those of Java's `ByteBuffer` form, which
  Java's consumer calls (`DoubleDeserializer`'s names its class, where the `byte[]` form says "Deserializer").
  `StringDeserializer` and `UUIDDeserializer` read bytes as Java's `new String(bytes, UTF_8)` does
  (`decodeLikeJava`, `src/internal/encode.ts`): Node's decoder writes one U+FFFD per byte of an encoded surrogate
  (`ED A0 80`), where Java writes one for the three. `FloatSerializer` and `FloatDeserializer` cannot keep a
  signaling NaN *(deviation: a JavaScript number turns a float's signaling NaN quiet, `0x7f800001` into `0x7fc00001`.
  Why: a number is a double. Goes when: never)*: `floatSerdeShouldPreserveNaNValues` checks the payload and that the
  value stays a NaN. **(waiting for the user's team's decision)**: the code stays as built until they decide.

## The batch and its count

- `poll()`'s completion receives the `ConsumerRecords` handle, which owns every record and the bytes behind their keys
  and values, and holds nothing of the consumer (`rust/src/ffi/consumer/consumer_records.rs`). The addon wraps it in a
  `Batch`, which counts what still needs it: one reference while the records are built, one for each external Buffer
  over a key or a value, and one for each record's header handle (a `HeaderSource`, below). The last one to go destroys
  the batch (`batch_unref`, `ConsumerRecords_destroy_err`), from a Buffer's or a handle's finalizer, running no JS. So a
  Buffer kept after `close()`, or after the consumer is collected, stays valid.
- Each key and value is a Buffer over the batch's bytes, made with `napi_create_external_buffer`, the Buffer form of the
  `napi_create_external_arraybuffer` line 133 names: the same external memory, which Electron refuses the same way. An
  empty key or value is a new empty Buffer, which holds nothing. No `napi_adjust_external_memory` call is made (line
  134).
- The records are built by `ConsumerRecord.internalCreate`, an `@internal` factory that `KafkaConsumer.poll` passes to
  the addon, called once per record with the record's fields: it builds the frozen record without the constructor's
  argument checks, in the constructor's field order. One topic string is made per partition, and passed to each of its
  records.
- The poll's answer is, per partition in the core's order, `[topic, partition, records]`, and the next offsets as
  `[topic, partition, offset, leaderEpoch, metadata]` entries (`offset_map_to_js`, `native/error.c`, which an error's
  payload of the same shape uses too).

## The copy path and its switch

- Where `napi_create_external_buffer` answers `napi_no_external_buffers_allowed`, as on Electron, the addon copies each
  key and value of that batch instead (`napi_create_buffer_copy`), and a copy holds nothing of the batch. On that path
  only, the addon looks at each record's headers (P2.md, choice 5(a)): a record with none gets an empty `Headers` and no
  header handle, so a batch whose records have no headers is destroyed as soon as the poll's answer is built.
- The test-only `consumerRefuseExternalBuffers(on)` (`native/consumer.c`) makes the addon act as if Node-API had refused
  external buffers, for the batches built while it is on. Plain Node never refuses them, so the copy path is tested
  through it (`test/integration/memory_probes.test.js`). `consumerStats()` counts the consumers, batches, external
  Buffers, header handles and copies made and freed. Both are registered for the tests only, as `recordSpy()` and
  `faultNext()` are, and `check-native-registry` lists them with their test.

## The headers' copy

- A consumed record's `Headers` (`src/common/header/headers.ts`) holds the record's header handle, an external over the
  batch's `HeaderSource`, until its first read or change, when it copies its list out through
  `consumerRecordHeaders(source)`: each key a string, each value a new Buffer of its own, or null (`headers_to_js`,
  `native/headers.c`, which `record_spy.c` and an error's payload use too). It then drops the handle, and with it its
  reference on the batch. Every method of `Headers` copies first. This is the one call into the addon outside the clients
  (line 160), which `lint-mapping.json`'s `nativeCallers` lists.

## The offsets, the commits and the metadata queries

- Every one of these calls goes through its `_cb` twin, but `groupMetadata()`, which calls the fallible
  `kafka_consumer_Consumer_group_metadata` and reads the `ConsumerGroupMetadata_*_err` accessors before it frees the
  metadata. A call answered with nothing completes through `void_on_complete`; one answered with a value through its
  own completion (`committed_on_complete`, `position_on_complete`, `current_lag_on_complete`,
  `partitions_for_on_complete`, `list_topics_on_complete`, `offsets_for_times_on_complete`, `offsets_on_complete`),
  each ending its call's count as its last statement. The answer is read into JavaScript values, then `done` runs,
  which settles the Promise, and then the answer is freed, before any `.then` runs: the owned map of `committed`,
  `offsetsForTimes`, `beginningOffsets`, `endOffsets` and `listTopics` (with its lists of partitions). The owned list
  of `partitionsFor` is freed as it is read (`partitions_take_to_js`), before `done` runs, and so is an error.
- What a call takes is built for it and destroyed after it, since a list or map the addon builds with `kafka_List_new`
  or `kafka_Map_new` only borrows its items: the `TopicPartition` of `seek`, `position` and `currentLag`, the
  `OffsetAndMetadata` of `seek` (`OffsetAndMetadata_with_leader_epoch_metadata`, `-1` for no leader epoch, line 172),
  the partitions of the seeks and queries, the commit's map of `TopicPartition` to `OffsetAndMetadata` and
  `offsetsForTimes`' map of `TopicPartition` to `int64_t` (`native/values.c`: `offset_and_metadata_from_js`,
  `offset_map_from_js`, `tp_long_map_from_js`, shared by the mock and `commitAsync(offsets, callback)`).
- The answers' maps are read by shared readers: `offset_map_to_js` (`native/error.c`, the poll's next offsets and an
  error's payload too) reads `committed`'s, a NULL value, a partition with no committed offset, as `null`;
  `long_map_to_js` (`native/error.c`, an error's payload too) the offset lookups'; `partitions_to_js` and
  `node_to_js` (`native/producer.c`) the partitions of `partitionsFor` and `listTopics`, for both clients, and
  `partitionInfoOf` (`src/internal/metadata.ts`) builds the `PartitionInfo`s from them for both.
- `offsets` and `timestampsToSearch` are `TopicPartitionMap`s only, and a `Map` gets `payload.ts`' hint, given by
  name or bare (line 62). A `null` timestamp in `timestampsToSearch` is a `TypeError`, "timestampsToSearch must not
  contain a null timestamp", as Java's unboxing throws `NullPointerException`: the binding's run-time type check,
  since a `null` has no `int64` to cross as, and the FFI reads a NULL value as 0 (`map_topic_partition_i64`).
- `commitSync(null)` is the form error: a bare `null` fits both its one-parameter forms (line 73).
- *(deviation, as in Python)* `enforceRebalance()` and `enforceRebalance(reason)` aren't translated. Why: under
  KIP-848, the only protocol here, Java's `AsyncKafkaConsumer` only logs them as unsupported. Goes when: the client
  supports the classic protocol.

## The `group.id` check (line 119, choice 3(a))

The class records at construction whether `group.id` was given, a value that is neither `null` nor `undefined`
(`#groupIdDefined`), and `groupMetadata()` and `committed(null)` check it before calling the FFI, throwing or
rejecting with `InvalidGroupIdError` and Java's message ("To use the group management or offset commit APIs, you must
provide a valid group.id in the consumer configuration."). It is the binding's only check of its own (line 119), and
it keeps no other state. **(deviation)** It runs before the core's closed and busy checks, which Java runs first
(`acquireAndEnsureOpen()` before `throwIfGroupIdNotDefined()`): a closed or busy consumer without `group.id` throws
`InvalidGroupIdError` from `groupMetadata()` and `committed(null)` where Java throws `IllegalStateException` or
`ConcurrentModificationException`. Why: the binding keeps no closed flag (line 184), and the core makes the
`groupMetadata()` check with a panic, which reaches JavaScript as `IllegalStateError` with the FFI's prefix; for
`committed(null)` the FFI refuses the NULL before the core's group check. Goes when: the FFI returns
`InvalidGroupIdError` (PLAN §6, ask 6) and refuses the NULL after the core's checks (ask 7). With a `group.id`,
`committed(null)` gets the FFI's `TypeError("partitions must not be null")`, after nothing, as below. A closed
consumer's `groupMetadata()` throws decision 11's text, `IllegalStateError('Rust panic caught at the FFI boundary in
kafka_consumer_Consumer_group_metadata: This consumer has already been closed.')`.

## The order of the null checks *(deviation)*

A null argument the FFI refuses at its boundary, and a negative timeout (below), are refused before the core's own
checks, which Java runs first
(`AsyncKafkaConsumer#subscribeInternal`, `#subscribeToRegex`, `#assign`, `#pause`, `#resume`: `acquireAndEnsureOpen()`,
then for a subscription `throwIfGroupIdNotDefined()`, then the null check):

- `subscribe({ topics: null })` and `subscribe({ pattern: null })` reject with their `IllegalArgumentError`s ("Topic
  collection to subscribe to cannot be null", "Topic pattern to subscribe to cannot be null") on a consumer without
  `group.id`, a closed one, or one with a `poll()` pending, where Java throws `InvalidGroupIdException`,
  `IllegalStateException("This consumer has already been closed.")` or `ConcurrentModificationException`. A null
  topic inside the collection crosses as a blank topic and meets the core's checks in Java's order.
- `assign({ partitions: null })`, `pause({ partitions: null })` and `resume({ partitions: null })` do the same on a
  closed or busy consumer.
- `pause` or `resume` of a `TopicPartition` whose topic is null rejects with the FFI's `TypeError("topic must not be
  null")` (`kafka_common_TopicPartition_new` refuses a NULL topic), where Java throws `IllegalStateException("No
  current assignment for partition null-0")`.
- `position(null)`, `currentLag(null)` and `seek({ partition: null, ... })` reject with the FFI's `TypeError`s
  ("partition must not be null", "topic_partition must not be null": the FFI names its own parameters) on a closed or
  busy consumer, where Java reports those first, and then, for `position`, "You can only check the position for
  partitions assigned to this consumer."; a `TopicPartition` whose topic is null gets "topic must not be null" there
  too, as in `pause`.
- `committed({ partitions: null })` on a consumer with a `group.id` rejects with the FFI's `TypeError("partitions must
  not be null")` on a closed or busy consumer; without one, the binding's `InvalidGroupIdError` comes first (above).
- `beginningOffsets(null)` and `endOffsets(null)` reject with the FFI's `TypeError("Partitions cannot be null")`,
  `offsetsForTimes(null)` with `TypeError("Timestamps to search cannot be null")`, Java's own messages, and
  `partitionsFor(null)` with `TypeError("topic must not be null")`, on a closed or busy consumer, where Java checks
  those first. A null timestamp in `offsetsForTimes` is the binding's run-time type check (above), made before the
  call reaches the FFI.
- `seek` with a negative offset on a busy consumer rejects with `ConcurrentModificationError`, where Java's `seek`
  checks the offset first, before `acquireAndEnsureOpen()`: the FFI's busy check runs before the core's offset check.
  On a closed consumer the offset check comes first, as in Java.
- A negative timeout is refused by the FFI's `ms()` (`rust/src/ffi/consumer/mod.rs`) inside each `_cb` twin's task,
  after the busy check but before the core's closed and `group.id` checks, with `IllegalArgumentError("Timeout must
  not be negative")`: both timed `commitSync` forms, `position`, `committed`, `partitionsFor`, `listTopics`,
  `offsetsForTimes`, `beginningOffsets` and `endOffsets` with `timeout: -1` reject with it on a closed consumer, and
  the two `commitSync` forms and `committed` on one without `group.id`, where Java reports the closed consumer
  (`acquireAndEnsureOpen()`) or `InvalidGroupIdException` (`throwIfGroupIdNotDefined()`) first. A busy consumer gets
  the busy error first, as in Java. `poll` keeps Java's order: Java's `poll` builds its `Timer`, and so checks the
  timeout, before `acquireAndEnsureOpen()`. The user accepted this order on 2026-11-26 as a known difference (P3.md,
  issue 152-2); 151-3's order, above, is unchanged and still the user's question.

Java checks these before its closed and busy checks too, so their order is Java's: `commitSync({ offsets: null })`
(Java's `new HashMap<>(null)`, the FFI's `TypeError("offsets must not be null")`), `seekToBeginning(null)` and
`seekToEnd(null)` (Java's and the FFI's `IllegalArgumentError("Partitions collection cannot be null")`) and
`seek({ offsetAndMetadata: null })` (Java's `offsetAndMetadata.offset()`, the FFI's `TypeError("offset_and_metadata
must not be null")`).

Why: the binding repeats no precondition (line 183), so it cannot run these checks itself in Java's order, and the
FFI refuses the NULL, checks a busy consumer and checks the timeout before the core runs. Python's conventions
record the same deviation ("Order": a consumer without `group.id` reports a null collection where Java reports
`InvalidGroupIdException`). Goes when: the FFI refuses the NULL and the negative timeout only after the core's
closed, busy and group checks, and `TopicPartition_new` takes a null topic. The tests pin
today's order and messages (`test/unit/consumer/kafka_consumer.test.js`, "the null checks come before the closed,
busy and group checks" and "the null checks of the offset calls come before the closed and busy checks, and the
group.id check before all").

## Liveness

- As the producer's ("Threads and liveness"): a consumer keeps Node running from its creation until the core reports it
  closed, and a call made on it holds Node until its answer ran, a call on a closed consumer included (line 158).
  `cbq_call_begin` is the statement right before each `_cb` call, and `cbq_call_end` the last statement of its
  completion, on every path.
- The binding keeps no closed flag (line 184). The handle object's finalizer destroys only a consumer the core reported
  closed, never one whose close was refused (above) (`consumer_finalize`, `KafkaConsumer_destroy_err`), outside a drain, running no JS; an open one is never
  destroyed by a finalizer, and a consumer that's never closed gets no warning (line 155). `consumer_execute` is the
  queue's only executor, and `js_consumer_create`'s failure paths the only other place that destroys a class handle,
  before a handle object exists (`check-single-drainer`).
- The preview warning is the producer's (`warnPreview`, `src/internal/lifecycle.ts`): once per process.

## `close(option)` (decision 7) **(conventions conflict)**

Line 79 says that a `null` `CloseOptions` timeout means the default, while line 171 says a `Duration` left out, or
`null` where Java accepts it, is sent as `-1`, which the FFI's `duration_ms()` reads as zero (line 81): read literally,
`close(CloseOptions.groupMembershipOperation(...))` would not wait at all. The code builds the C options as Java builds
them, as Python and .NET do (`close_options_build`, `native/consumer.c`): with a timeout,
`CloseOptions_new_with_timeout_err(ms)` and then `CloseOptions_with_group_membership_operation_err` when the operation
isn't the default; without one, `CloseOptions_with_operation_err(op)`, which leaves the timeout unset, so `close` waits
up to Java's default of 30 seconds. `close()` itself is `close_cb`. A negative timeout still goes to the FFI, which reads
it as zero (line 81's deviation). The wording proposed to the rules' owner: line 171's `Duration` row gains "except a
`CloseOptions` timeout, which is set only when given".

## The closed getters (decision 11)

`assignment()`, `subscription()` and `paused()` on a closed consumer throw the core's panic, caught by the FFI:
`IllegalStateError('Rust panic caught at the FFI boundary in kafka_consumer_Consumer_assignment: This consumer has
already been closed.')`, with `_subscription`, `_paused` or `_group_metadata` in place of `_assignment`. Accepted for the preview; the
tests assert the text, and PLAN §6's ask 5 goes to the FFI's owner. The waiting calls on a closed consumer reject with
Java's own `IllegalStateError('This consumer has already been closed.')`. A second `close()` is harmless, and
`metrics()` answers after `close()`, the view outliving the consumer.

## The mock

- `MockConsumer` (`src/consumer/mock_consumer.ts`) wraps the FFI's `MockConsumer` (JS conventions, Implementation over
  the FFI, Q62): `native/consumer.c` builds it with `MockConsumer_new` and holds it in the same `Consumer` struct as a
  `KafkaConsumer`, over its `MockConsumer__as_Consumer_err` view, through one construction helper for both
  (`consumer_handle_new`, as the producer's `producer_handle_new`). So its `Consumer` methods go through the same `_cb`
  twins and drain, with the same counts and lifetime (above): no closed flag, `close()` keeps the handle, the
  finalizer destroys only a mock the core reported closed (`MockConsumer_destroy_err`), the close completion deciding
  as for a `KafkaConsumer`, and a mock keeps Node running from its creation until `close()`. It emits no preview
  warning. Its own waiting calls, `rebalance` and `losePartitions`, go through `rebalance_cb` and
  `lose_partitions_cb` (`tp_call`, completed by `void_on_complete`); its other calls through their fallible or `_err`
  forms, which answer inside the call.
- One copy of the bodies: `src/internal/consumer_calls.ts` holds the `Consumer` methods' bodies, each taking the
  native handle, which `KafkaConsumer` and `MockConsumer` both call. The temporary `group.id` check stays
  `KafkaConsumer`'s: the mock passes the flag as set, since Java's mock answers `groupMetadata()` with its dummy
  (`dummy.group.id`, generation 1, member `1`) and has no such check.
- The records are the app's own. `addRecord(record)` builds a C record through `ConsumerRecordOptionsBuilder` and
  `ConsumerRecord_with_options_err` carrying only what the FFI's mock reads: the topic, partition, offset and leader
  epoch (the setter not called for none). Its key and value are the addresses of a `MockRecord`'s two tags
  (`native/mock_values.c`, shared with the producer's mock), which the mock keeps without reading, and the
  `MockRecord` holds the app's `ConsumerRecord`, and so its key, value and headers, by one reference. A record the
  FFI refuses (a partition not assigned, a closed mock) is released at once; one the mock took is held until the
  destroy, as `add_record`'s doc requires, since the core's copies of it (its records, those a rebalance cleared,
  those a poll returned) share the tags. The C record is destroyed right after the call, which copied it.
- A mock's poll: the completion finds each record's `MockRecord` from its key (`mock_record_to_js`) and returns the
  app's own record, as Java's mock returns the records it was given; the batch makes no external buffer and no header
  source, so it is destroyed once read. The next offsets are read as a `KafkaConsumer`'s are.
- An error given to `setPollException` or `setOffsetsException` crosses as a `LocalCallbackError` whose opaque is an
  `InjectedError` holding the value (`native/mock_values.c`, the carrier a callback's thrown value crosses through
  too, below), and `native/error.c` hands the very same object back
  where the core reports it: `poll()` rejects with it once, and `beginningOffsets()` or `endOffsets()` with the
  offsets error once, as Java's mock throws and clears them. Each mock call first lets go of the errors the core holds
  no copy of any more (`mock_of`, `injected_errors_sweep`); the destroy lets go of the rest.
- `updatePartitions` builds each `PartitionInfo` and its nodes through Java's six-parameter constructor
  (`PartitionInfo_with_offline_replicas_err`, `Node_with_rack_is_fenced_err`, `partition_info_list_from_js` in
  `native/values.c`, from `partitionInfoArgs` in `src/internal/metadata.ts`, the reverse of `partitionInfoOf`); the
  FFI copies them, and they are destroyed after the call. `partitionsFor` and `listTopics` read them back as equal
  `PartitionInfo`s. The `update*Offsets` calls build their maps with `tp_long_map_from_js`, from the entries
  `longEntries` (`src/internal/consumer_calls.ts`) builds, as `offsetsForTimes` does. `lastPollTimeout()` crosses as
  a bigint, `-1` for Java's null and `Long.MAX_VALUE` read as `Infinity`.
- The checks are the FFI's and the core's, with Java's messages where the core has them: a record of a partition not
  assigned, a partition lost that isn't assigned, `setMaxPollRecords(0)`, an invalid or null strategy, a partition
  with no offset to reset to, and `offsetsForTimes()`, "Not implemented yet.". The binding's own are the form and
  run-time type checks (`exception` a `KafkaError` or `null`, `record` a `ConsumerRecord`, `newOffsets` a
  `TopicPartitionMap`, `partitions` an iterable of `PartitionInfo`) and the `null` offset below.

### Where the FFI's mock differs from Java's *(deviation)*

The binding passes each through (lines 119 and 184), a test pins it (`test/unit/consumer/mock_consumer.test.js`),
and the package docs list it. Why: the binding repeats no precondition and keeps no state. Goes when: the FFI's mock
does as Java's does (PLAN §6, asks 8 to 13).

- One call at a time: the FFI's mock shares the consumer's single-owner flag, so a mock call made while another call
  on the mock is pending, a `poll` included, fails with `ConcurrentModificationError` ("KafkaConsumer is not safe for
  multi-threaded access."), where Java's synchronized mock waits. So do the getters `assignment()`,
  `subscription()`, `paused()` and `groupMetadata()`, which Java's mock waits for or, for `groupMetadata()`, answers at
  once (ask 13). `closed()` and `lastPollTimeout()` are read through the handle and answer while a call is pending, and
  so do `metrics()` and `wakeup()`. The setters with no error slot (`updateBeginningOffsets`,
  `updateEndOffsets`, `updateDurationOffsets`, `setPollException`, `setOffsetsException`, `scheduleNopPollTask`) are
  logged by the core and ignored instead (`with_mock_void`), where Java's mock applies them (ask 8); the error an
  ignored error setter was given is let go at the next mock call, as the core dropped it. The FFI's mock
  answers a poll at once, so the window is short, and the test finds it by trying a call the FFI refuses.
- A `null` offset in `newOffsets` is a `TypeError`, "newOffsets must not contain a null offset", the binding's
  run-time type check, as for a `null` timestamp in `offsetsForTimes`: the FFI reads a NULL value as 0 (`map_topic_partition_i64`), where
  Java's mock keeps the null and later reports the partition as having no offset (ask 9).
- Nulls Java's mock stores or ignores get the FFI's `TypeError`s, their messages naming the C parameters: a `null`
  topic or partitions given to `updatePartitions`, a `null` element there, and a `PartitionInfo` with a null topic,
  replica list, replica or node host, which `PartitionInfo_with_offline_replicas_err` and
  `Node_with_rack_is_fenced_err` refuse ("topic must not be null", "partitions must not contain null", "replicas must
  not be null", "host must not be null"); and `close({ option: null })` ("options must not be null"), which closes
  nothing (ask 10).
- `subscribe({ pattern: null })` rejects with the FFI's "Topic pattern to subscribe to cannot be null", the
  consumer's message, where Java's mock says "Topic pattern cannot be null" (ask 10).
- The `Consumer` methods' null arguments get the FFI's errors, as on `KafkaConsumer` (the order of the null checks,
  above): `partitionsFor(null)` rejects with `TypeError` "topic must not be null" and `currentLag(null)` with
  `TypeError` "topic_partition must not be null", where Java's mock answers `[]` and `0`; `subscribe({ topics: null })`,
  `assign({ partitions: null })`, `seekToBeginning(null)` and `seekToEnd(null)` reject with `IllegalArgumentError` and
  Java's `KafkaConsumer` messages ("Topic collection to subscribe to cannot be null", "Topic partitions collection to
  assign to cannot be null", "Partitions collection cannot be null"), where Java's mock throws `NullPointerException`
  (ask 10).
- A null partition, or a null topic, given to `assign()` or `subscribe()` goes to the FFI as NULL, as the mock's
  other calls send theirs, and the FFI refuses it with `TypeError` ("partitions must not contain null", "topic must
  not be null", "topics must not contain null"): Java's mock throws `NullPointerException` for a null partition and
  stores a null topic. The blank topic a `KafkaConsumer` sends instead, so that the core answers with Java's message
  (the handle, above), is `KafkaConsumer`'s alone: Java's `AsyncKafkaConsumer` makes that check, and Java's mock
  makes none (`consumer_calls.ts`, the `blankNull` flag of `assign` and `subscribe`; asks 7 and 10).
- A negative timeout is refused by the FFI's `ms()` with `IllegalArgumentError` "Timeout must not be negative", the
  conventions' rule (Signatures, Q35), from `poll` and every timed form, and such a `poll` leaves `lastPollTimeout()`
  as it was; Java's mock ignores the timeouts and records each poll's (ask 12).
- `metrics()` on a closed mock answers an empty map: the FFI reads it through the consumer's handle, which outlives
  the close, never through the mock, whose `metrics()` would panic. Java's mock throws "This consumer has already been
  closed." (ask 11).
- The order of the null checks (above) holds for the mock too: `addRecord(null)` on a closed mock is the FFI's
  `TypeError` ("record must not be null"), where Java's mock reports the closed mock first; `rebalance(null)` and
  `losePartitions(null)` get the FFI's `TypeError`s ("new_assignment must not be null", "partitions_lost must not be
  null"), Java's `NullPointerException`s.

### Left out

- For want of an FFI entry point (PLAN §6, items 1 to 3): `clientInstanceId`, `setClientInstanceId`,
  `disableTelemetry`, `injectTimeoutException`, `schedulePollTask`, `registerMetricForSubscription`,
  `unregisterMetricFromSubscription` and `addedMetrics`.
- *(deviation, as in Python)* `enforceRebalance()`, `enforceRebalance(reason)`, `shouldRebalance()` and
  `resetShouldRebalance()`, by the user's decision of 2026-11-26. Why: under KIP-848 Java's consumer only logs
  `enforceRebalance` as unsupported, and the two helpers only track it. Goes when: the client supports the classic
  protocol.
- Java's `subscribe(Pattern…)` forms, as on `KafkaConsumer`; and the deprecated `MockConsumer(OffsetResetStrategy)` and
  `close(Duration)`.

## The listeners, the commit callbacks and the window

- A rebalance listener crosses as a `Listener` (`native/consumer.c`), the `self_` of its
  `ConsumerRebalanceListener_new_err` registration: it holds the three functions src/ built for its methods
  (`listenerRuns`, `src/internal/consumer_calls.ts`), each calling the user's method with `{ partitions }`, a
  `TopicPartitionSet` read from the borrowed, sorted list (`tp_list_to_js`, `native/values.c`, whose loop
  `tp_list_take_to_js` shares), and the listener as `this`. A missing `onPartitionsRevoked` or
  `onPartitionsAssigned` goes to the FFI as NULL, whose `LocalNullPointerError` ("on_partitions_revoked must not be
  null", the C parameter's name) is a `TypeError` (line 57); `onPartitionsLost` is always a function, which runs
  `ConsumerRebalanceListenerDefaults` with the user's object as `this` when it lacks one, looked up at each call (line
  56). A null listener goes to the FFI as NULL, which refuses it with Java's "RebalanceListener cannot be null", before
  its check of the topics; a listener that is no object is a `TypeError`. The registration is copied by the
  subscribe and destroyed after it.
- An `OffsetCommitCallback` crosses as a `CommitCallback`, the `self_` of its `OffsetCommitCallback_new_err`
  registration, holding src/'s function (`commitRun`), which calls the user's function with `{ offsets, exception }`,
  the offsets read from the borrowed map (`offset_map_to_js`) and the borrowed error built as an error.
  `commitAsync({ callback: null })` and `commitAsync(null)` are Java's `commitAsync()`, through `commit_async_cb`
  (P5.md, choice 2(a)); `commitAsync({ offsets, callback: null })` goes to the FFI, which refuses the NULL with
  `TypeError("callback must not be null")` *(deviation: Java takes a null callback and logs a failed commit. Why: the
  FFI refuses a NULL callback. Goes when: it takes one, PLAN §6 ask 14)*. On a `KafkaConsumer`, a callback's
  `offsets` is `null` for a failed commit and for an empty map committed, where the FFI hands over an empty map
  (choice 3(a)) *(temporary. Why: the FFI passes an empty map where Java's `whenComplete` passes null. Goes when: the
  FFI passes NULL, ask 15)*; on a mock it is the map committed, as Java's mock passes it.
- **The run and the report.** The core calls a listener's method or a commit callback inside the drain of the waiting
  call that delivers it (`listener_invoke`, which the three method pointers hand their run to, and
  `commit_on_complete`). Each runs its JS function once and reports once, through
  `Consumer__set_callback_result_err` (`check-completion-order`): at once when src/'s function returned `undefined`,
  Java's `void`; or, for any other result, which src/ wrapped with `Promise.resolve`, once that Promise settled, a
  throw being src/'s `Promise.reject` (`Deferred`, `deferred_new`, `deferred_report`). The addon registers both
  handlers at once, so no rejection is left unhandled, and holds them strongly until the report, so a garbage
  collection never reports a Promise early. A throw or a rejection, whatever its value, crosses the core as a
  `LocalCallbackError` carrying it, through the carrier of the mocks' injected errors (`injected_error_carry`,
  `native/mock_values.c`, which holds the value in a one-element array, since Node-API 9 references objects only),
  and `native/error.c` hands it back under `carried`, so it comes back as the same value wherever the core reports
  it (line 122). Each run lets go of the carried values the core holds no copy of any more.
- **The window** (line 149, PLAN §4.4 and decision 8). From a method's or callback's invocation until its report
  (`Consumer.reports`), every call the `ConsumerHandle` has goes through the handle (`window_of`), made with the
  consumer (`Consumer_handle_err`) and destroyed after it: the `_cb` forms of `commitSync`, `commitAsync`, `seek`,
  `seekToBeginning`, `seekToEnd`, `pause`, `resume`, `position`, `committed`, `partitionsFor`, `listTopics`,
  `offsetsForTimes`, `beginningOffsets`, `endOffsets` and `currentLag`, each counted with `cbq_call_begin` and
  completed by the consumer's own completion function, and the sync forms of `assignment()`, `paused()`,
  `groupMetadata()` and `wakeup()`. `metrics()` goes to the consumer, which answers, and every other call to the
  consumer, which refuses `poll`, `subscribe`, `assign`, `unsubscribe`, `close` and `subscription()` with
  `ConcurrentModificationError("KafkaConsumer is not safe for multi-threaded access.")` *(deviation, as in Python:
  Java's listener may call them. Why: the handle has no form of them, KIP-1306. Goes when: it has)*. Any call made
  while a report is pending goes to the handle, a timer's included, as JavaScript can't tell which code made it. The
  core checks a handle call when the call runs, so a call a callback makes without awaiting it may run after the
  report and get the handle's longer busy message (line 152).
- **The mock's window** *(deviation, PLAN §6 ask 16)*: the mock's handle isn't gated, as Java's mock has no
  `acquire()`, but of its async operations only the commits and `currentLag` work; the others fail with the core's
  `UnsupportedOperationError("ConsumerHandle async operations are not supported on a MockConsumer handle; drive the
  MockConsumer directly.")`; its sync reads (`assignment()`, `paused()`, `groupMetadata()`) answer. The calls the
  handle doesn't have meet the mock's single-owner flag, which `rebalance()`, `losePartitions()` and `commitAsync()`
  hold while the callback runs, where Java's synchronized mock takes all of them: `poll()`, `subscribe()`,
  `assign()`, `unsubscribe()`, `subscription()` and `close()` are refused with `ConcurrentModificationError`, and so
  are the mock's own `addRecord()`, `setMaxPollRecords()`, `updatePartitions()`, `rebalance()` and
  `losePartitions()`; the six setters with no error slot (`updateBeginningOffsets()`, `updateEndOffsets()`,
  `updateDurationOffsets()`, `setPollException()`, `setOffsetsException()`, `scheduleNopPollTask()`) are logged by the
  core and ignored, as while any other call is pending (ask 8); `closed()`, `lastPollTimeout()`, `metrics()` and
  `wakeup()` answer. Why: the FFI's mock handle does not drive the mock. Goes when: it does.
- **Lifetimes**, as the FFI's subscribe docs give them. The consumer holds the listeners the core may hold
  (`Consumer.listener`, chained by `Listener.next`); a subscribe holds its own until the core answers it
  (`SubscribeCtx`, `subscribe_on_complete`): a success replaces them all, a listener-less one included; a failure the
  core answered keeps the consumer's and adds its own, which the core may hold too (the mock registers it before the
  check that fails, as Java's `SubscriptionState#subscribe` does, and its `rebalance()` calls it), until a later
  successful subscribe; a call refused before the core ran (busy, or a NULL the FFI refuses) and a `KafkaConsumer`'s
  empty topic list (`unsubscribe()`, which registers none) keep the consumer's and let go of their own; `assign` and
  `unsubscribe` keep them. A `KafkaConsumer` lets go of them once a `close()` the core ran returns
  (`close_on_complete`); a mock keeps them until the destroy, as its `rebalance()` and `losePartitions()` still call
  them after `close()`, as Java's mock does. A commit callback's context is held by the core's registration until it
  reported, or until the destroy, as the FFI's doc of the callback requires, and by its commit's call until its
  answer (`Consumer.commits`); its function goes once it reported, or once its commit failed, which the core never
  runs it for (it registers the callback only once the commit is under way), so a refused commit holds no JS.
- **How the JS values are held** (`Held`): every listener's and commit callback's function is an element of the
  handle object's `callbacks` object, and every value the core carries for the consumer (what a callback threw or
  rejected with, an error given to the mock's `setPollException()` or `setOffsetsException()`) an element of its
  `carried` object (`native/mock_values.c`, the carrier's `keeper`), each read through a weak reference. A strong
  native reference is a GC root: a callback that reaches its consumer, as one that commits does, or a value that
  does, as an `Error` made inside a callback that closes over it does (its captured stack holds the callback),
  would keep a closed consumer, or a mock whose listener calls it, alive for good. Held through the handle object,
  they live while the consumer is open (the queue holds the handle object strongly until the close) or while
  anything else reaches it, and the GC collects them with it; the destroy, from the handle object's finalizer, runs
  no JS, and a value the core reads after it is its plain error. The producer's mock keeps its injected errors by a
  strong reference, as before. Each
  pending report holds the listener and the consumer's handle object, so the finalizer cannot run until it is made.
  The destroy runs the callbacks still pending and waits for their reports: one it runs reports at once, running no
  JS, and before it the addon reports, as fulfilled, those still waiting for a Promise, which no JS will settle
  (`deferred_report_all`, an environment teardown's case); no report is made once the destroy has returned
  (`Consumer.destroyed`), and a later one is dropped, the addon's memory for the consumer staying until then (a
  `Deferred` holds a unit of the struct). A callback that never settles has no timeout, and `wakeup()` doesn't end
  it: the waiting call that ran it waits too (line 150).
- **The errors** (line 122), put back where Java's `AsyncKafkaConsumer` has them by one conversion
  (`callbackError`, `src/internal/consumer_calls.ts`), which every consumer call's rejection goes through:
  `fromNative` (`src/internal/errors.ts`) remembers each error the core built around a carried value with that
  `LocalCallbackError`'s message (`carriedCause`). The core already has Java's layers, with a `LocalCallbackError` where
  Java has the value, and its own wrapping where Java's `maybeWrapAsKafkaException` would leave a `KafkaException`
  alone, since a `LocalCallbackError` sits beside `KafkaException` (`rust/src/common/local_callback_error.rs`). So a
  layer around a carried value is dropped when the value is a `KafkaError`; a layer built from its cause alone, whose
  message is the core's text of that cause (`LocalCallbackError: <message>`), is a `KafkaError` built from the value,
  its message the value's `toString()` (line 121); `close()`'s own layer, "Failed to close kafka consumer", is kept,
  and the rule applies to its cause. The outcome, pinned by `test/integration/consumer.test.js`:
  - a listener's error from `poll()` and `unsubscribe()`: a `KafkaError` as itself, anything else as the `cause` of
    `KafkaError("User rebalance callback throws an error")`;
  - from `close()`: the `cause` of `KafkaError("Failed to close kafka consumer")`, a `KafkaError` as itself and
    anything else inside a `KafkaError` built from it;
  - a commit callback's from `commitSync()` and `commitAsync()`: as itself; from `poll()`: a `KafkaError` as itself,
    anything else inside a `KafkaError` built from it; from `close()`: as itself, the `cause` of "Failed to close
    kafka consumer";
  - the mock's listener's and commit callback's from `rebalance()`, `losePartitions()` and `commitAsync()`: as itself,
    as Java's mock calls them directly.
- **A timed-out close's cause** *(deviation)*: when `close()` times out on the last pending async commit, as a
  `close({ timeout: 0 })` with a commit still pending does, its cause is `common/errors`' `TimeoutError` with the
  core's text, "Timed out waiting for last pending async commit to complete", where Java's is the `toString()` of a
  `TimeoutException` with no message, "java.util.concurrent.TimeoutException". Why: the core's message. Goes when:
  the core gives Java's.
- **A thrown `null`**: a callback that throws `null`, or rejects with it, fails its call with `null`, as with any
  other value. `fromNative` gives a carried `null` as a private symbol (`CARRIED_NULL`, `src/internal/errors.ts`),
  since the completions read `null` as no error, and `callbackError` puts the `null` back where the call rejects.
