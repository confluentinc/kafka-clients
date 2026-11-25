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
  `close_cb` and `close_with_options_cb`. The three getters go through their fallible forms
  (`kafka_consumer_Consumer_assignment`, `_subscription`, `_paused`), `wakeup()` and `metrics()` through their `_err`
  twins. Each `_cb` call has its own context (`VoidCtx`, `PollCtx`, `CloseCtx`), the completion's opaque, freed by the
  completion.
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
  error, "KafkaConsumer is not safe for multi-threaded access.", comes back as it is. A close the core refuses for it
  (`LOCAL_CONCURRENT_MODIFICATION`) closed nothing, so it does not record the consumer closed (`close_on_complete`).
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

## Liveness

- As the producer's ("Threads and liveness"): a consumer keeps Node running from its creation until the core reports it
  closed, and a call made on it holds Node until its answer ran, a call on a closed consumer included (line 158).
  `cbq_call_begin` is the statement right before each `_cb` call, and `cbq_call_end` the last statement of its
  completion, on every path.
- The binding keeps no closed flag (line 184). The handle object's finalizer destroys only a consumer the core reported
  closed (`consumer_finalize`, `KafkaConsumer_destroy_err`), outside a drain, running no JS; an open one is never
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
already been closed.')`, with `_subscription` or `_paused` in place of `_assignment`. Accepted for the preview; the
tests assert the text, and PLAN §6's ask 5 goes to the FFI's owner. The waiting calls on a closed consumer reject with
Java's own `IllegalStateError('This consumer has already been closed.')`. A second `close()` is harmless, and
`metrics()` answers after `close()`, the view outliving the consumer.
