# P3: the issues the review resolved (Critic 152)

Copied at the end of P3 (2026-11-26) from `COMMENTS.DONE.152.md` at the repository root: the issues Critic 152
found in its review of P3, each with the fix that resolved it, in issue order, with the Manager's decisions. The
review's round 2 confirmed the first five fixes and found 152-6; round 3 confirmed 152-6's fix and found no new
issue, only a clause of one test comment that was wrong about Java, never filed, which `be17e1f19` reworded and a
last read confirmed. Issue 152-2 is resolved as a documented deviation of the same kind as P2's 151-3, which the
Manager decided by analogy; P3.md's "Result" says so for the user.

## Issue 152-1: commitAsync()'s broker read-back cannot fail, and the test waits out a 30-second deadline
- **Commit**: `8f316efaf` javascript: the consumer's offset and commit calls against a broker
- **File**: `javascript/test/integration/consumer.test.js:237-249`
- **Severity**: Test Gap
- **Reference**: P3.md §2, `consumer.test.js`: "`commitSync` and `commitAsync` read back by `committed`"
- **Description**: The test commits offset 1 with `commitSync({ offsets })` before the consumer's first poll, so that
  poll starts at the committed offset 1: only the records at offsets 1 and 2 are left, and `pollFor(consumer, 3)`
  (line 237) polls until its 30-second deadline for a third record that never comes (it asserts no count, so the test
  goes on). `commitSync()` then commits 3, which line 239 asserts. After `seek(2)`, one `poll` and `commitAsync()`, the
  loop at line 247 waits for a committed offset of 3, the value `commitSync()` already committed, so its first
  `committed()` read ends it whatever `commitAsync()` did: a `commitAsync()` that commits nothing passes. (And if that
  one 500 ms poll returns no record, `commitAsync()` commits 2, and the outcome depends on whether the loop's first read
  comes before that commit lands.) Run against a scratch broker, this test alone took 33.7 s and the whole file (its 6
  tests) 39.7 s.
- **Expected**: `commitAsync()` commits an offset other than the one already committed, and the loop waits for that
  one, so the read-back fails when `commitAsync()` commits nothing; `pollFor` asks for the records that can come (2
  here), so the test ends without waiting out its deadline.
- **Actual**: the `commitAsync()` read-back is vacuous, and every run spends 30 s in `pollFor`.
- **Fixed**: in `f462b0c2e` (fixup of `8f316efaf`): `pollFor` asks for the 2 records that can come, and `commitAsync()` commits position 1 after a seek back, so the read-back waits for 1 and fails when `commitAsync()` commits nothing (checked with a no-op in its place: `3n !== 1n`); the test takes about 9 s.

## Issue 152-2: A negative timeout is refused before the closed and group checks, which Java runs first; not recorded
- **Commit**: `05a99ab3f` (commitSync, position, committed) and `1513ab98c` (partitionsFor, listTopics,
  offsetsForTimes, beginningOffsets, endOffsets); `3ba87a4b8` (NOTES.md)
- **File**: `javascript/design/current/consumer/NOTES.md:126` (the order section),
  `javascript/test/unit/consumer/kafka_consumer.test.js:829` (the order test)
- **Severity**: Behavior Mismatch
- **Reference**: `AsyncKafkaConsumer#commitSync(Optional,Duration)`, `#position(TopicPartition,Duration)`,
  `#committed(Set,Duration)`, `#partitionsFor(String,Duration)`, `#listTopics(Duration)`,
  `#offsetsForTimes(Map,Duration)` and `#beginningOrEndOffset`, each starting with `acquireAndEnsureOpen()`
  (`commitSync` and `committed` then `throwIfGroupIdNotDefined()`); P3.md §1 ("NOTES.md lists them all, with a test
  that pins the order"); `javascript/CLAUDE.md` line 80
- **Description**: The FFI's `ms()` (`rust/src/ffi/consumer/mod.rs`) checks the timeout inside each `_cb` twin's
  task: after the busy check (`run_cb`'s `acquire`), but before the core method, so before the core's closed and group
  checks. Probed on the built package (`dist/`):
  - a closed consumer: `commitSync({ timeout: -1 })`, `commitSync({ offsets, timeout: -1 })`,
    `position({ partition, timeout: -1 })`, `committed({ partitions, timeout: -1 })`,
    `partitionsFor({ topic, timeout: -1 })`, `listTopics({ timeout: -1 })`,
    `offsetsForTimes({ timestampsToSearch, timeout: -1 })`, `beginningOffsets(...)` and `endOffsets(...)` each reject
    with `IllegalArgumentError: Timeout must not be negative`, while `commitSync({ timeout: 0 })` on the same consumer
    rejects with `IllegalStateError: This consumer has already been closed.`, which is Java's answer for -1 too;
  - no `group.id`: `commitSync({ timeout: -1 })`, `commitSync({ offsets, timeout: -1 })` and
    `committed({ partitions, timeout: -1 })` reject with the same `IllegalArgumentError`, where Java throws
    `InvalidGroupIdException` (the binding rejects with `InvalidGroupIdError` for `timeout: 0`);
  - a busy consumer gets the busy error first, as in Java.

  NOTES.md's order section lists the FFI's null checks and `seek`'s negative offset, not this; line 80's deviation and
  the six methods' `@remarks` ("where Java makes no check and waits without a time limit") describe an open consumer;
  no test pins it. `poll` is not affected: Java's `poll` builds its `Timer`, and so checks the timeout, before
  `acquireAndEnsureOpen()`.
- **Expected**: recorded beside 151-3's order in NOTES.md (and in the methods' `@remarks` where the doc-size limit
  leaves room), pinned in the order test, and covered by the FFI ask (PLAN §6, ask 7).
- **Actual**: an undocumented difference from Java's order, on a closed consumer and on one without `group.id`.
- **Fixed**: in `2677b6802` (fixup of `05a99ab3f`): the order test pins `timeout: -1` on a closed consumer and on one without `group.id` for every affected form, `poll` keeping Java's order; the timed forms' `@remarks` say so where the doc-size limit leaves room (not `listTopics` or `beginningOffsets`); and in `6e9de9829` (fixup of `3ba87a4b8`): `NOTES.md`'s order section covers the FFI's timeout check, the forms and `poll`.

## Issue 152-3: P3 copies P2's call context and payload.ts's long-map reader (DoD #6)
- **Commit**: `05a99ab3f` (`ValueCtx`, `value_ctx_new`) and `1513ab98c` (`longMap`)
- **File**: `javascript/native/consumer.c:863-882`, `javascript/src/consumer/kafka_consumer.ts:269-273`
- **Severity**: Design Flaw
- **Reference**: definition-of-done.md #6; P3.md §1 item 3 ("`internal/payload.ts`: reused as it is (...
  `topicPartitionLongMap` ...)")
- **Description**: `ValueCtx` (consumer.c:863) is P2's `VoidCtx` (consumer.c:79) field for field (`Consumer* c;
  napi_ref done;`), and `value_ctx_new` (consumer.c:868) is `void_ctx_new` (consumer.c:464) line for line; only the
  comment on what `done` receives differs, which C cannot check through the `void *` opaque anyway. In TypeScript,
  `longMap` (kafka_consumer.ts:269) is the loop of `topicPartitionLongMap.core` (payload.ts:264-270), which reads the
  same `[topic, partition, offset]` entries `long_map_to_js` writes, for an error's payload as for the offset lookups:
  the C side shares `long_map_to_js` (the Actor's item 2), while the TypeScript side copies its reader.
- **Expected**: one context type and constructor for both kinds of completion (P2's, renamed if needed), and
  `beginningOffsets` / `endOffsets` building their answer with `topicPartitionLongMap.core`.
- **Actual**: two copies of each.
- **Fixed**: in `2677b6802` (fixup of `05a99ab3f`): one `CallCtx` and `call_ctx_new` serve both kinds of completion, and `beginningOffsets` / `endOffsets` build their maps with `topicPartitionLongMap.core`; `6e9de9829` renames the context in `NOTES.md`.

## Issue 152-4: enforceRebalance() on a busy consumer rejects, where Java's does not: NOTES.md implies parity, and the busy test leaves it out
- **Commit**: `1513ab98c` javascript: the consumer's offset and metadata queries and enforceRebalance; `3ba87a4b8`
  (NOTES.md)
- **File**: `javascript/design/current/consumer/NOTES.md:105-107`,
  `javascript/test/unit/consumer/kafka_consumer.test.js:1289-1313`
- **Severity**: Test Gap
- **Reference**: `AsyncKafkaConsumer#enforceRebalance()` and `#enforceRebalance(String)` (no
  `acquireAndEnsureOpen()`); P3.md §2 ("one call at a time, with the core's busy message"); `javascript/CLAUDE.md`
  line 152; consumer-threading.md §1
- **Description**: `enforce_rebalance_cb` and `enforce_rebalance_with_reason_cb` go through `run_cb`, whose `acquire`
  refuses a call while another is pending (the core takes `&mut self`, consumer-threading.md §1). Probed: with a
  `poll()` pending, `enforceRebalance()` and `enforceRebalance({ reason: 'r' })` reject with
  `ConcurrentModificationError: KafkaConsumer is not safe for multi-threaded access.`, where Java's
  `AsyncKafkaConsumer.enforceRebalance` only logs and returns. The refusal follows line 152, but NOTES.md says both
  "resolve once the core logged ..., as Java's AsyncKafkaConsumer does (decision 9), on a closed consumer too, where
  Java makes no check either", and the method's `@remarks` say it "only logs ..., as Java's consumer does, and so
  resolves", which reads as no check at all; and the busy test lists every other P3 call but not `enforceRebalance`,
  so the refusal is not pinned.
- **Expected**: NOTES.md says that, unlike Java's, it is refused while another call is pending (line 152), and the
  busy test includes `enforceRebalance()`.
- **Actual**: the busy refusal is neither recorded nor tested.
- **Fixed**: in `2ea549c8e` (fixup of `1513ab98c`): `enforceRebalance`'s `@remarks` say it is refused while another call is pending, and the busy test includes both forms; and in `6e9de9829` (fixup of `3ba87a4b8`): `NOTES.md` says so.

## Issue 152-5: Tests assert the core's timeout messages where Java's differ, without saying so
- **Commit**: `05a99ab3f` (commitSync) and `1513ab98c` (partitionsFor, listTopics)
- **File**: `javascript/test/unit/consumer/kafka_consumer.test.js:915-920`, `:1154-1170`
- **Severity**: Test Gap
- **Reference**: `javascript/CLAUDE.md`, "Assert the core's exact error messages in tests"; the core's own records in
  `rust/src/consumer/internals/async_kafka_consumer.rs` (the "RECORDED DEVIATION" on the commitSync timeout text;
  "Java: `throw new TimeoutException();`" on `listTopics` with a zero timeout)
- **Description**: `commitSync({ offsets, timeout: 300 })` asserts "Timeout of 300ms expired before successfully
  committing offsets", the core's text (the core records that Java's `commitSync` attaches no message of its own);
  `listTopics({ timeout: 0 })` asserts "Timeout of 0ms expired before all topics' metadata could be listed", where
  Java's `AsyncKafkaConsumer.listTopics` throws `new TimeoutException()`, with no message; `partitionsFor` and
  `listTopics` assert "Timeout expired while waiting for TopicMetadata" and "... AllTopicsMetadata", where Java's
  `TopicMetadataRequestManager` says "Timeout expired while fetching topic metadata". None of the tests says the
  message differs from Java's ("No broker: the core's timeouts." names the timeouts the core's), while the same file
  says so elsewhere ("the core's message for a negative one", "read as zero by the FFI, where Java throws").
- **Expected**: a short note at each that the text is the core's and differs from Java's.
- **Actual**: no note.
- **Fixed**: in `2677b6802` (the commitSync assertion) and `2ea549c8e` (partitionsFor and listTopics): a note at each that the text is the core's and differs from Java's.

## Manager's notes (round 1, 2026-11-26)

All five issues go to the Actor, each fixed as its **Expected** says, with these decisions:

- **152-1**: as Expected. The read-back must fail when `commitAsync()` commits nothing, and `pollFor` asks only for
  the records that can come.
- **152-2**: a documented deviation of the same kind as 151-3, decided by analogy (the binding repeats no
  precondition, line 183, so it can't reorder the FFI's checks). `NOTES.md`'s order section covers the FFI's argument
  checks, not only its null checks: add this case, naming the forms (both timed `commitSync` forms, `position`,
  `committed`, `partitionsFor`, `listTopics`, `offsetsForTimes`, `beginningOffsets`, `endOffsets`) and that `poll`
  keeps Java's order. Pin it in the order test: a closed consumer, and one without `group.id`, given `timeout: -1`.
  Add it to the methods' `@remarks` only where the doc-size limit leaves room. The Manager adds it to PLAN §6's ask 7
  at the wrap-up.
- **152-3**: as Expected: one context type and constructor for the completions that answer with an error or a value
  (P2's, renamed if that reads better, with its comment saying what `done` receives in each case), and
  `beginningOffsets` / `endOffsets` building their answer with `topicPartitionLongMap.core`.
- **152-4**: as Expected: `NOTES.md` and `enforceRebalance`'s `@remarks` say it is refused while another call is
  pending, as line 152 makes every guarded call and as the core guards it (consumer-threading.md §1), where Java's
  `AsyncKafkaConsumer.enforceRebalance` takes no `acquire()`; the busy test includes both forms.
- **152-5**: as Expected: a short note at each of those assertions that the text is the core's and differs from
  Java's.

Two small items from the Critic's report, not numbered issues, for the same round:
- `NOTES.md`: an answer map is freed right after `done` runs (the Promise already settled, before any `.then` runs),
  not "before the Promise settles". Make the sentence say what the code does.
- The `commitAsync(undefined)` test also asserts the `TypeError`'s message.
- **Done**: all five issues as decided above, and the two small items in `2677b6802` (the `commitAsync(undefined)` message) and `6e9de9829` (`NOTES.md`'s sentence on when an answer is freed).

## Issue 152-6: partitionsFor's and listTopics' timeout assertions flake under load, as commitSync's did
- **Commit**: `2ea549c8e` fixup! javascript: the consumer's offset and metadata queries and enforceRebalance (the
  assertions come from `1513ab98c`; `89c86b6c6` handles the same race for `commitSync` only)
- **File**: `javascript/test/unit/consumer/kafka_consumer.test.js:1194-1206`
- **Severity**: Test Gap
- **Reference**: `89c86b6c6`; the core's `TopicMetadataRequestManager`
  (`rust/src/consumer/internals/topic_metadata_request_manager.rs`, which completes an expired request with "Timeout
  expired while fetching topic metadata") and `CompletableEventReaper`
  (`rust/src/consumer/internals/events/completable_event_reaper.rs`, which tracks the `TopicMetadata` and
  `AllTopicsMetadata` events too); Java's `TopicMetadataRequestManager` and `CompletableEventReaper#reap` race the
  application thread the same way
- **Description**: `partitionsFor({ topic })` and `listTopics()` (`default.api.timeout.ms` 300, no broker) assert
  exactly the core's app-side texts, "Timeout expired while waiting for TopicMetadata" and "... AllTopicsMetadata"
  (`submit_and_await`'s message, not re-worded as `committed` and the offset lookups re-word theirs). But the same
  race `89c86b6c6` found for `commitSync` reaches them, with one more party: at the deadline the background task's
  topic-metadata manager, or the reaper, can complete the event first, and `await_event_completion` then returns that
  error. A probe of the built package (fresh consumers against `127.0.0.1:1`, the test's configuration, six to eight
  at a time under CPU load) gave, over three runs of 120, 90 and 160 calls each:
  - `partitionsFor`: "Timeout expired while fetching topic metadata" 12 times in 370 (7, 0 and 5);
  - `listTopics`: the same text 2 times in 370 (1, 0 and 1);
  - `commitSync({ offsets, timeout: 300 })`, the case `89c86b6c6` fixed: the reaper's text 4 times in 370.

  So the `partitionsFor` assertion fails about three times as often as the one `89c86b6c6` relaxed. The note
  `2ea549c8e` added ("Java's TopicMetadataRequestManager says 'Timeout expired while fetching topic metadata' for
  both") also reads as if the core never gives that text, while its own topic-metadata manager does.
- **Expected**: both assertions take the core's other texts for that timeout, the topic-metadata manager's
  "Timeout expired while fetching topic metadata" (Java's text too) and the reaper's
  "<event> was <n> ms past its expiration of <deadline>", as `89c86b6c6` does for `commitSync`, and the note says why.
- **Actual**: two exact-text assertions that fail under load.
- **Fixed**: in `8c8def551` (fixup of `1513ab98c`): both assertions take the usual text, exact, or the topic-metadata manager's "Timeout expired while fetching topic metadata" or the reaper's "<event> was <n> ms past its expiration of <deadline>", and the note says why.

## Manager's notes (round 2, 2026-11-26)

- **152-6**: to the Actor, as Expected: one fixup of the commit that wrote those assertions (`1513ab98c`). Both
  assertions accept the core's other texts for that timeout, the topic-metadata manager's and the reaper's, as
  `89c86b6c6` does for `commitSync`, and the usual text stays exact where it comes. The note says why, and no longer
  reads as if the core never gives the topic-metadata manager's text.
- **Done**: 152-6 as decided, in `8c8def551`.
