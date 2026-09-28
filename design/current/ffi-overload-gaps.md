# Missing FFI and core pieces — for the owner

Things the rules (CLAUDE.md §2/§3 and Python Binding Conventions) require that the FFI or the Rust
core does not provide yet.

Owner policy (2026-09-25): this PR adds **no new FFI entry points**. Python forms map to the FFI the
Rust FFI rules generate (master's, plus the 23 entry points this branch already added, which are
ported). A method or overload form whose entry point exists in neither is **not generated** now and
is listed here, to be added in a later PR. Minor FFI changes, updates and fixes are fine in this PR.

## Python forms skipped because their FFI entry point is missing

Known at the owner's confirmation; P4 and P5 complete the list by applying the rules to every class.

| Python form (Java overload) | FFI entry point the rules name | Python in this PR |
|---|---|---|
| `commit(offsets=…, timeout=…)` (`commitSync(Map, Duration)`, `commitSync(Duration)`) | a timed `commit_sync` form | no `timeout=` |
| `committed(partitions=…, timeout=…)`, `position(…, timeout=…)`, `beginning_offsets(…, timeout=…)`, `end_offsets(…, timeout=…)`, `offsets_for_times(…, timeout=…)`, `partitions_for(…, timeout=…)`, `list_topics(timeout=…)` (the `Duration` overloads) | the timed forms | no `timeout=` |
| `client_instance_id(timeout=…)` on producer and consumer | `kafka_{producer,consumer}_*_client_instance_id` | method not generated; on `MockProducer` / `MockConsumer` also the helpers serving only it (`set_client_instance_id`, `inject_timeout_exception`, `disable_telemetry`) |
| `register_metric_for_subscription(metric=…)` / `unregister_metric_from_subscription(metric=…)` | the KIP-1076 entry points (`kafka_producer_Producer_register_metric_for_subscription`, `kafka_consumer_Consumer_register_metric_for_subscription`, …) | methods not generated; `MockProducer.added_metrics()` / `MockConsumer.added_metrics()` not generated either |
| `AsyncKafkaProducer.send(record=…, callback=…)` (`async def`, Java's `send(record, Callback)`) | `kafka_producer_Producer_send_with_callback_async` | **kept** (post-phase item): the batching engine sends through `kafka_producer_Producer_send_batch` and runs the callback itself |
| `AsyncKafkaConsumer.current_lag()` (`async def`, ruling 30) | `kafka_consumer_Consumer_current_lag_async` | not generated on the async class (nor on `AsyncMockConsumer`); the sync `KafkaConsumer.current_lag()` stays |
| `AsyncKafkaConsumer.close(timeout=…)` (`async def`, Java's `@Deprecated close(Duration)`) | `kafka_consumer_Consumer_close_with_timeout_async` | **kept** (P5 post-phase item): both consumers call `kafka_consumer_Consumer_close_with_option(_async)` with `DEFAULT`, which is what Java's `close(Duration)` does (`close(CloseOptions.timeout(timeout))`); the plain `close_with_timeout` blocks inside the FFI and cannot deliver the listener's `on_partitions_revoked` on the caller's thread |
| Share-consumer family (`KafkaShareConsumer`, `MockShareConsumer`, async peers) | `kafka_consumer_ShareConsumer_*` | family not generated |

## One-time FFI renames (minor FFI fixes, done in P2/P4/P5)

Generation looks up only the entry-point name CLAUDE.md §2/§3 derive, so these existing entry points
get their derived name. On master's names the old name stays as a deprecated alias; branch-only names
were never released and are ported under the derived name only.

| Today | Derived name | Where it lives |
|---|---|---|
| `kafka_consumer_Consumer_seek_with_metadata` (+ `_async`) | `kafka_consumer_Consumer_seek_with_offset_and_metadata` | master — keep old as deprecated alias |
| `kafka_consumer_Consumer_commit_sync_offsets` (+ `_async`) | `kafka_consumer_Consumer_commit_sync_with_offsets` | master — keep old as deprecated alias |
| `kafka_consumer_Consumer_close_options` (+ `_async`) | `kafka_consumer_Consumer_close_with_option` | branch only — port under the derived name |
| `kafka_consumer_MockConsumer_set_poll_exception`, `..._set_offsets_exception` | `..._set_poll_error`, `..._set_offsets_error` (§3: no "exception" in C names) | branch only — port only the conforming names |

Done in P2: `close_options` → `close_with_option` (+ `_async`); `set_offsets_exception` → `set_offsets_error`; `set_poll_exception` dropped. **Signature change on a master entry point:** `kafka_consumer_MockConsumer_set_poll_error(consumer, message)` always injected `illegal_state`; it now takes `(consumer, code, message)` like `set_offsets_error`, as Java's `setPollException(KafkaException)` takes any error. No deprecated alias is possible for a signature change; master's C test was updated.

Also done in P2: the branch's timed producer close is ported under the derived names. The branch had folded Java's `close(Duration)` into master's `kafka_producer_Producer_close` / `_close_async` by adding a `timeout_ms` argument (`-1` = untimed). Master's two signatures are restored unchanged, and `close(Duration)` is `kafka_producer_Producer_close_with_timeout(producer, timeout_ms, out_error)` plus `kafka_producer_Producer_close_with_timeout_async(producer, timeout_ms, callback, user_data)`, as CLAUDE.md §2 derives for an overload that only adds a parameter and as the consumer's `Consumer_close_with_timeout` already is. A negative `timeout_ms` is rejected with Java's "The timeout cannot be negative.". Python's `close()` calls `close`, `close(timeout=…)` calls `close_with_timeout`.

P4/P5 complete this list by deriving every form's name; any further mismatch is added here.

P5 (consumer): done — `kafka_consumer_Consumer_seek_with_offset_and_metadata(_async)` and
`kafka_consumer_Consumer_commit_sync_with_offsets(_async)` are the entry points; the old names stay
as aliases documented **Deprecated** (cbindgen emits no attribute), and the C tests and the C gRPC
server use the new ones. Every other consumer form's name is in the header: `subscribe(_async)`,
`subscribe_pattern_async`, `assign(_async)`, `unsubscribe(_async)`, `poll(_async)`,
`commit_sync(_async)`, `commit_async`, `commit_async_with_callback`, `seek(_async)`,
`seek_to_beginning(_async)` / `seek_to_end(_async)`, `position(_async)`, `committed(_async)`,
`metrics`, `partitions_for(_async)`, `list_topics(_async)`, `paused`, `pause(_async)` /
`resume(_async)`, `offsets_for_times(_async)`, `beginning_offsets(_async)` / `end_offsets(_async)`,
`current_lag`, `group_metadata`, `close(_async)`, `close_with_option(_async)`, `wakeup`,
`KafkaConsumer_new`. Three names are used as the header has them, not renamed:

- Java's `subscribe(Collection, ConsumerRebalanceListener)` / `subscribe(SubscriptionPattern,
  ConsumerRebalanceListener)`: master's `subscribe_with_listener(_async)` /
  `subscribe_pattern_with_listener_async` run the listener on the dispatcher thread, so the
  binding calls this branch's `subscribe_caller_thread_listener_async` /
  `subscribe_pattern_caller_thread_listener_async` (the pending-callback queue), which the Threads
  rule needs.
- Java's `commitAsync(Map, OffsetCommitCallback)`: `commit_async_offsets_with_callback`, the
  spelling CLAUDE.md §3 itself cites (§2 would give `commit_async_with_offsets_callback`).
- The `kafka_consumer_ConsumerHandle_*` reentrancy entry points (`seek_with_metadata`,
  `commit_sync_offsets`, `commit_async_offsets`) keep their names: the handle has no Java class.

Minor FFI fixes in P5: `kafka_common_Error_new` builds the class of a client-side (negative) id
(the four payload-only classes, -21/-24/-26/-27, still map to `UNKNOWN_SERVER_ERROR`);
`kafka_consumer_MockConsumer_set_poll_error` / `_set_offsets_error` take a `clear` flag
(`(consumer, clear, code, message)`, a second signature change to master's `set_poll_error` after
P2's) and `MockConsumer::set_poll_error` / `set_offsets_error` take `Option<Error>`;
`kafka_consumer_Consumer_close_with_timeout` rejects a negative timeout on a `KafkaConsumer` with
"The timeout cannot be negative." instead of clamping it (a mock closes); and once a
pending-callback notify is registered a commit callback runs on the caller's thread — inside a
synchronous call on that call's thread, inside an `_async` operation queued as pending method 3 and
run by the thread that acks it — rather than on the dispatcher thread (C47; without a notify, C
callers keep the dispatcher thread).

P4 (producer): every other producer form's derived name is in the header — `send` /
`send_with_callback` (+ `send_async`), `flush`, `partitions_for`, `metrics`, `close` /
`close_with_timeout`, the five transaction ops, `KafkaProducer_new` — so the producer needs no
rename. Minor FFI fixes in P4: `close_with_timeout` (+ `_async`) rejects a negative timeout for a
`KafkaProducer` handle only (Java's `MockProducer.close(Duration)` never reads it; Critic 73
R2-N2), and the three blocking sends (`send_batch`, and after Critic 75 F3 `send` and
`send_with_callback`) no longer hold the producer lock across the send, so a close can start while a
send waits for metadata.

## Rust-core gaps

| # | What | Rule it breaks | Needed in the core | Python today |
|---|---|---|---|---|
| 1 | Header keys and values are copied once per record at the C→Rust boundary: `build_record_headers` (`src/ffi/producer.rs`) decodes each key into a `String` and copies each value, because the core's `RecordHeader` owns both. (Before P4's `e889d115` the core's borrowed `send` then dropped them: no header reached the broker from the C or Python `KafkaProducer`.) | CLAUDE.md §12 (no copy of key/value/header bytes on the send path) | `RecordHeader` must borrow its key and value instead of owning `String` / `Option<Vec<u8>>`, through the send path | The C extension borrows the header keys and values; the Rust FFI copies each key and value once per record, and the headers reach the broker |
| 2 | No `LocalUnsupportedOperation` error | §4 rule 2 (Java built-in exceptions keep Java's name) | A `LocalUnsupportedOperation` error with its own FFI id; mocks return it instead of `unsupported_version` | `UnsupportedOperationException` arrives as `UnsupportedVersionError` |
| 3 | `Consumer::current_lag()` always returns `None` (`async_kafka_consumer.rs:2819`, "not yet wired"); the Java-faithful `current_lag_async()` is not on the `Consumer` trait and has no FFI entry | §4 async-ness rule (ruling 30: `async def` iff Java waits) and Java's `currentLag` result | Add `current_lag_async` to the `Consumer` trait and one FFI entry; the sync `KafkaConsumer.current_lag()` waits on it like `position()` | `current_lag()` is a plain call that always returns `None` |
| 4 | `MockConsumer::set_poll_error` / `set_offsets_error` take a plain `Error`, so a pending injected error cannot be cleared; Java's `setPollException(null)` / `setOffsetsException(null)` clear it (`MockConsumer.java:344-350`) | §4 (a mock implements what Java's mock implements) | Core setters taking `Option<Error>` (or a clear method), and a `clear` flag on `kafka_consumer_MockConsumer_set_poll_error` / `_set_offsets_error` in the shape of `kafka_producer_MockProducer_set_commit_transaction_error(producer, clear, error_code, error_message)` **Done in P5** (the minor FFI fixes above): the core setters take `Option<Error>` and both entry points take the `clear` flag; Python's `MockConsumer` (a Python translation, row 14) clears on `set_poll_exception(exception=None)` / `set_offsets_exception(exception=None)` as Java does |
| 5 | A delivery failure's metadata crosses the FFI as null (the `ProducerBatch`-level value), losing the partition the batch had | Threads and callbacks (Java's `AppendCallbacks` hands the callback `RecordMetadata(tp, -1, -1, NO_TIMESTAMP, -1, -1)` with the chosen partition) | Deliver the placeholder metadata with the batch's partition on every failure, as for a synchronous `ApiException` | Builds Java's placeholder with the record's own partition, or -1 |
| 6 | `ProducerConfig` validates only the keys it parses (Java's `ConfigDef` validators applied in P4, Critic 74 N8); the Java-defined keys it neither parses nor validates: `client.dns.lookup`, `compression.{gzip,lz4,zstd}.level`, `metadata.recovery.strategy`, `metadata.recovery.rebootstrap.trigger.ms`, `socket.connection.setup.timeout{,.max}.ms`, and the list keys' `ValidList` (`bootstrap.servers`, `metric.reporters`, `config.providers`) | Configuration (a bad value raises `ConfigError` with Java's message) | Parse and validate them as Java's `ProducerConfig` does | Python coerces their types; a bad value within the type is accepted |
| 7 | The batching engine hands records to `kafka_producer_Producer_send_batch` one by one on its send thread, each waiting up to `max.block.ms` for an unknown topic's metadata | Behaviour of Java's `close()` / `flush()`, which do not wait for sends blocked in `waitOnMetadata` | A send that does not serialize the metadata waits (or fails the rest of a batch once one record's wait expires for its topic) | With an unreachable broker an untimed `close()` / `flush()` can wait `max.block.ms` per accumulated record; `close(timeout=…)` is bounded |
| 8 | No FFI to run a built-in serde natively (P3 item 26) | Serialization ("the client recognizes a built-in by identity and runs it natively") | A serializer / deserializer kind on the producer and consumer handles | The built-ins run as Python callables |
| 9 | `kafka_producer_Producer_send_batch` builds a `String` topic per record (`CStr::to_string_lossy().into_owned()`), as the core `ProducerRecord`'s topic is a `String` | CLAUDE.md §11 (an identifier cloned per message should be a cheap `Arc<str>` clone) | A `ProducerRecord` topic the send path can share (`Arc<str>`), interned per topic by the FFI | The C extension borrows the topic without a copy; the Rust side allocates it once per record |
| 10 | `Consumer::group_metadata()` returns a stub (`groupId=""`, generation -1) on a consumer without a `group.id` | Java's `groupMetadata()` calls `throwIfGroupIdNotDefined()` (`InvalidGroupIdException`) | A `Result` from `group_metadata`, or the check in the FFI | The binding checks `group.id` and raises `InvalidGroupIdError` with Java's message |
| 11 | `ConsumerConfig` accepts `enable.auto.commit=true` without a `group.id`, and `send.buffer.bytes` / `receive.buffer.bytes` below -1 | Configuration (Java: `InvalidConfigurationException("enable.auto.commit cannot be set to true when default group id (null) is used.")`; `ConfigException` from `atLeast(-1)`) | `ConsumerConfig.maybeOverrideEnableAutoCommit` and the `ConfigDef` validators of the keys it parses | Accepted |
| 12 | The core's `ConsumerHandle` (the reentrancy path a listener or commit callback uses while the outer call holds the consumer) has no `subscribe`, `unsubscribe`, `poll`, `commitAsync(callback)`, `partitionsFor`, `listTopics`, `metrics`, `groupMetadata`, `currentLag` or `close` | Threads and callbacks ("a listener may call back into its consumer"; Java's `acquire()` is reentrant for the calling thread) | Those operations on the handle, and FFI entries for them | From inside a callback they raise `ConcurrentModificationError`; `assignment`, `subscription`, `paused`, `assign`, `seek`, `seek_to_*`, `pause` / `resume`, `position`, `committed`, `*_offsets`, `offsets_for_times`, `commit` and `commit_nowait()` without a callback work. While an `AsyncConsumer.commit_nowait()` still finishes in a task (gap 19), `assignment()` / `subscription()` / `paused()` read through the handle and `metrics()` / `group_metadata()` raise `ConcurrentModificationError` |
| 13 | A `KafkaConsumer`'s commit callback that raises: the core has already continued past the invocation when the FFI trampoline returns | Java's `OffsetCommitCallbackInvoker.executeCallbacks()` lets it propagate out of the delivering `poll()` / `commitSync()` / `close()` | An FFI path returning the callback's error to the delivering call | Logged (`MockConsumer`, a Python translation, propagates it as Java does) |
| 14 | The FFI `MockConsumer`: `add_record` carries only topic, partition, offset and serialized key/value (no decoded record, timestamp, headers or leader epoch); its `ConsumerHandle` rejects the blocking calls; there is no `schedulePollTask(Runnable)`; `MockConsumer_new` falls back to `latest` for a strategy Java rejects | Implementation over the FFI (the mock "calls the FFI") and Java's `MockConsumer` behaviour | Those pieces | `MockConsumer` is a direct Python translation of Java's (P5 post-phase item) |
| 15 | A completed `commitAsync`'s callback is queued from a task the core spawns, not inline as Java's `whenComplete` on an already-completed future | Threads and callbacks (the callback runs inside the next call on the consumer) | Queue it inline when the commit's future is already complete | A `commit_nowait()` right after an immediately completed one may not deliver its callback yet; `commit()` / `poll()` / `close()` do |
| 16 | The FFI's `ConsumerRecords` has no next-offsets accessor (`kafka_consumer_ConsumerRecords_*` exposes count / get / is_empty / destroy only), though the core's `ConsumerRecords.next_offsets` holds Java's value (Critic 76 F5) | Java's `ConsumerRecords.nextOffsets()` (`FetchCollector`: `nextInLineFetch.nextFetchOffset()`, past trailing control records and a compacted tail, and present for a partition whose poll only advanced past them) | A next-offsets accessor on the FFI `ConsumerRecords` | `next_offsets()` is each partition's last returned offset + 1 with that record's leader epoch; a trailing transaction marker is not skipped and a marker-only partition is absent (documented on `next_offsets()`; `test_next_offsets_skip_the_transaction_marker` is skipped with this reason) |
| 17 | The binding deserializes after the core's poll has moved the positions past the whole batch (Critic 76 N3) | Java deserializes inside `FetchCollector` before it moves the position | Deserialization inside the core's fetch collection (gap 8's native serdes, or a deserializer the core calls), or a poll that moves the positions only past the records the embedder accepts | After a failing deserializer the binding seeks each partition back to its first record not returned; until that seek lands, a background auto-commit (`enable.auto.commit=true`, timer-driven on the core's task) can commit offsets past records never returned. The window is the seek's round trip; no simple fix exists without one of those pieces |
| 18 | The core's reentrant `ConsumerHandle` entry points (`kafka_consumer_ConsumerHandle_*`) are synchronous only (Critic 76 N4) | Threads and callbacks (an `async def` listener's awaited consumer calls should not block the event loop) | `_async` forms of the `ConsumerHandle` entry points | On `AsyncKafkaConsumer`, a listener's `await consumer.commit()` / `position()` / `committed()` / … runs synchronously on the loop's thread and blocks every other task for its round trip (up to `default.api.timeout.ms`); documented on `AsyncConsumer.subscribe` and `ConsumerRebalanceListener` |
| 19 | `kafka_consumer_Consumer_commit_async*` have no `_async` form, and the core's `commitAsync` delivers queued listener callbacks while it waits for its offsets (`consumer-threading.md` §31; Java's does not) (Critic 76 B1) | Threads and callbacks (a listener runs on the caller's thread; a waiting call drains the queue) | An `_async` form of the three entry points, so `commit_nowait()` waits on the dispatcher like the other waiting calls | While a listener is registered, `commit_nowait()` runs the synchronous call on a per-consumer helper thread while the calling thread drains the queue (commit callbacks the core runs on the helper are handed to the calling thread); on `AsyncConsumer` a coroutine listener method queued then is awaited in a task the next awaited call waits for. The FFI rustdoc of the three entry points states the rule for C callers |
| 20 | A config-route deserializer's `configure(configs, is_key)` does not get the generated `client.id` (Critic 76 F6, `configurableObjectsShouldSeeGeneratedClientId`); the producer's serializers are in the same position | Java's `ConsumerConfig` generates the client id before any configurable is built and passes it in `originals(client.id)` | The core's generated client id before the core consumer is built (or the binding generating it and passing it to the core) | `configure` gets the configs the user gave; `client.id` is there only if the user set it |
| 21 | The core's `ConsumerHandle` has no `metrics` / `groupMetadata` (with row 19: Critic 76 R2-N1) | Threads and callbacks (the plain reads on the async consumer behave alike while a `commit_nowait()` still finishes; Java's `commitAsync` has returned by then) | `metrics` and `group_metadata` on the `ConsumerHandle`, with FFI entries | On `AsyncConsumer`, while a `commit_nowait()` finishes in a task (a coroutine listener it delivered is awaited), `metrics()` / `group_metadata()` raise `ConcurrentModificationError`, where `assignment()` / `subscription()` / `paused()` read through the handle; documented on both methods and on `commit_nowait()` |
