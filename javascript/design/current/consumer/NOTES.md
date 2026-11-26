# The consumer's addon notes

These are the consumer's parts of "The binding's notes" (`javascript/CLAUDE.md`, from line 227), written here as that
section would hold them, for the rules' owner to move there when they choose (P2.md, choice 7(a)). They describe what
the JS conventions leave to the binding, as the producer's notes do, and point to the conventions' group that states a
rule rather than repeat it. "Line N" is line N of `javascript/CLAUDE.md` at `8a25205a3`.

## The handle

- `native/consumer.c` holds one `Consumer` per `KafkaConsumer`: the owned class handle (`kafka_consumer_KafkaConsumer_t`,
  built from `ConsumerConfig_new` and `KafkaConsumer_new` with NULL deserializers), its borrowed
  `KafkaConsumer__as_Consumer_err` view, through which every call is made, and the producer's callback queue
  (`callback_queue.c`, `CallbackQueue`). The configuration's `kafka_Map_t` is built by `config_map_build`
  (`native/values.c`), which the producer's constructor calls too.
- Every waiting call goes through its `_cb` twin (JS conventions, Futures and waiting): `poll_cb`,
  `subscribe_with_topics_cb`, `subscribe_with_pattern_cb`, `assign_cb`, `unsubscribe_cb`, `pause_cb`, `resume_cb`,
  `close_cb` and `close_with_options_cb`, and the offset calls' twins (below). The three getters and
  `groupMetadata()` go through their fallible forms (`kafka_consumer_Consumer_assignment`, `_subscription`, `_paused`,
  `_group_metadata`), `wakeup()` and `metrics()` through their `_err` twins. Each `_cb` call has its own context
  (`VoidCtx`, `ValueCtx`, `PollCtx`, `CloseCtx`), the completion's opaque, freed by the completion.
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
  each ending its call's count as its last statement. The answer is read, then freed, before the Promise settles: the
  owned map of `committed`, `offsetsForTimes`, `beginningOffsets`, `endOffsets` and `listTopics` (with its lists of
  partitions) and the owned list of `partitionsFor`.
- What a call takes is built for it and destroyed after it, since a list or map the addon builds with `kafka_List_new`
  or `kafka_Map_new` only borrows its items: the `TopicPartition` of `seek`, `position` and `currentLag`, the
  `OffsetAndMetadata` of `seek` (`OffsetAndMetadata_with_leader_epoch_metadata`, `-1` for no leader epoch, line 172),
  the partitions of the seeks and queries, the commit's map of `TopicPartition` to `OffsetAndMetadata` and
  `offsetsForTimes`' map of `TopicPartition` to `int64_t` (`native/values.c`: `offset_and_metadata_from_js`,
  `offset_map_from_js`, `tp_long_map_from_js`, shared for the mock and `commitAsync(offsets, callback)` to come).
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
- `enforceRebalance()` and `enforceRebalance(reason)` resolve once the core logged "Operation not supported in new
  consumer group protocol", as Java's `AsyncKafkaConsumer` does (decision 9), on a closed consumer too, where Java
  makes no check either.

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

A null argument the FFI refuses at its boundary is refused before the core's own checks, which Java runs first
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

Java checks these before its closed and busy checks too, so their order is Java's: `commitSync({ offsets: null })`
(Java's `new HashMap<>(null)`, the FFI's `TypeError("offsets must not be null")`), `seekToBeginning(null)` and
`seekToEnd(null)` (Java's and the FFI's `IllegalArgumentError("Partitions collection cannot be null")`) and
`seek({ offsetAndMetadata: null })` (Java's `offsetAndMetadata.offset()`, the FFI's `TypeError("offset_and_metadata
must not be null")`).

Why: the binding repeats no precondition (line 183), so it cannot run these checks itself in Java's order, and the
FFI refuses the NULL, and checks a busy consumer, before the core runs. Python's conventions record the same deviation ("Order": a consumer without
`group.id` reports a null collection where Java reports `InvalidGroupIdException`). Goes when: the FFI refuses the
NULL only after the core's closed, busy and group checks, and `TopicPartition_new` takes a null topic. The tests pin
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
