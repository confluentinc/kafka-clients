# Critic 109: review of the Phase 0 rule text (`dotnet/CLAUDE.md`, `dotnet/.claude/rules/ffi-marshalling.md`) — resolved

Moved from `COMMENTS.109.md` after the fix; each finding ends with how and where it was resolved.

## 1. Medium: the admin client's closed-call behaviour is described as Java's, but Java fails the futures (unrecorded deviation)

- **Files:** `dotnet/CLAUDE.md` §3, idiom row `IllegalArgumentException` / `IllegalStateException`
  (line 621); `dotnet/.claude/rules/ffi-marshalling.md` §C1, **Teardown** (lines 2453-2456).
- **Passages:**
  - CLAUDE.md: "A closed client throws `IllegalStateException` with its Java message: "Cannot perform
    operation after producer has been closed", "This consumer has already been closed.", "Cannot accept
    new calls when AdminClient is closing.""
  - ffi §C1: "After it, an RPC throws `IllegalStateException` (Java's "Cannot accept new calls when
    AdminClient is closing.", §A5), and a second `Close` does nothing."
- **What is wrong:** Java's admin client does not throw this from an RPC.
  `org.apache.kafka.clients.admin.KafkaAdminClient.AdminClientRunnable#call(Call, long)` calls
  `call.handleFailure(new IllegalStateException("Cannot accept new calls when AdminClient is closing."))`.
  So the RPC returns its Result, and each future fails with that exception. The producer
  (`throwIfProducerClosed`) and the consumer (`AsyncKafkaConsumer`'s closed check) do throw, so the
  sentence holds for those two only.

  Root `CLAUDE.md` records exactly this for Python as a deviation (Behaviour with no other home: "the
  admin client's RPCs included *(deviation: Java's `KafkaAdminClient` returns the Result and fails its
  futures with that `IllegalStateException`. Why: the handle is freed …)*").

  The .NET rule states the same behaviour as Java's, with no deviation. CLAUDE.md §4 requires each
  divergence to be recorded. Plan D20 ("as Java's and Python's do") and plan §7 miss it too.

  Also, "After it" in §C1 refers back to the negative-timeout check, not to `Close`.
- **Fix:** keep the ruled behaviour (D20) and record it as a deviation. In ffi §C1: "After `Close`, an RPC
  throws `IllegalStateException` with Java's message "Cannot accept new calls when AdminClient is
  closing." *(deviation: Java's `KafkaAdminClient` returns the Result and fails its futures with that
  exception; Python's admin client raises it at the call, as here, because the native handle is
  freed)*." In the CLAUDE.md §3 row, after the three messages: "(the admin client throws at the call,
  where Java fails the RPC's futures: ffi §C1)". Manager: add the same line to plan §7.

Resolved: recorded as a deviation in both places, worded as root `CLAUDE.md`'s Python rule. `dotnet/CLAUDE.md` §3, the `IllegalArgumentException` / `IllegalStateException` row: "The admin client throws it at the call *(deviation, as Python's: Java's `KafkaAdminClient` returns the Result and fails its futures with it; the native handle is gone after `Close`, so the call has no Result to return; ffi §C1)*". ffi §C1 **Teardown**: "After `Close`, an RPC throws `IllegalStateException` with Java's message "Cannot accept new calls when AdminClient is closing." (§A5) *(deviation, as Python's: …)*", which also fixes "After it". The Manager updated plan D20 and §7.

## 2. Low: "Java throws nothing → `ArgumentOutOfRangeException`" contradicts the negative-timeout rule for the consumer's unchecked lookups

- **Files:** `dotnet/CLAUDE.md` §3, the same idiom row (line 621); `dotnet/.claude/rules/ffi-marshalling.md`
  §B5, **Precondition** (lines 1893-1896).
- **Passages:**
  - CLAUDE.md: "A .NET-only precondition (Java throws `NullPointerException` or nothing) keeps
    `ArgumentNullException` / `ArgumentOutOfRangeException`".
  - ffi §B5: "A .NET-only precondition, where Java throws `NullPointerException` or nothing, keeps
    `ArgumentNullException` (null topic/partition/config) or `ArgumentOutOfRangeException`."
- **What is wrong:** `Committed`, `PartitionsFor`, `ListTopics`, `OffsetsForTimes`, `BeginningOffsets`
  and `EndOffsets` make no sign check in Java: root `CLAUDE.md` Timeouts says Java's
  `CompletableEvent.calculateDeadlineMs` turns a negative timeout into no deadline. So by these two
  sentences, a negative `Committed(TimeSpan)` is a .NET-only precondition and throws
  `ArgumentOutOfRangeException`. Two other passages say it throws `IllegalArgumentException`:
  - CLAUDE.md §3's sketch comments (lines 417, 497: "every TimeSpan: negative →
    IllegalArgumentException");
  - ffi §B5's preceding sentence ("Where Java makes no check, a negative timeout throws it with the
    core's own `_with_timeout` message, "Timeout must not be negative"").

  Judgment call (a) itself is sound:
  - the core rejects a negative value as −3 "Timeout must not be negative" (`ms`,
    `rust/src/ffi/consumer/mod.rs:594-600`, used by every `*_with_timeout` form), so the error table
    gives `IllegalArgumentException` whenever the value reaches the core;
  - Python raises `IllegalArgumentError` with the same text;
  - an `ArgumentOutOfRangeException` would give one condition two classes, which D20 exists to prevent.

  The general sentence still needs the exception. Plan §7's ".NET-only preconditions" bullet has the
  same wording.
- **Fix:** qualify both sentences: "A .NET-only precondition (Java throws `NullPointerException` or
  nothing, other than a negative timeout, which the core rejects itself and so throws
  `IllegalArgumentException` with the core's message, ffi §B5) keeps …". Manager: record judgment
  call (a) in plan D20/§7, so the plan and the rules agree.

Resolved: both general sentences now carve out the negative timeout, naming the lookups as plan §7 does. `dotnet/CLAUDE.md` §3 row: "… keeps `ArgumentNullException` / `ArgumentOutOfRangeException`, except a negative timeout on a consumer lookup Java does not check (`Committed`, `PartitionsFor`, `ListTopics`, `OffsetsForTimes`, `BeginningOffsets`, `EndOffsets`): it throws `IllegalArgumentException` with the core's "Timeout must not be negative", the error the core's `_with_timeout` forms return (ffi §B5)". ffi §B5 **Precondition**: the "Where Java makes no check" sentence names the same six lookups and the core's −3, and the general sentence now reads "Any other .NET-only precondition …".

## 3. Low: D23's fatal-class gap is not recorded, and "as the core's `handle_api_error`" claims more than the binding does

- **File:** `dotnet/CLAUDE.md` §3, the serializer bullet (lines 323-332).
- **Passage:** "A serializer's `ApiException` is not thrown: as the core's `handle_api_error`, the
  callback gets placeholder metadata (offset -1) and the exception, and the future fails (on
  `IAsyncProducer` the first stage completes). Any other exception is rethrown unchanged. *(Python's
  `send()` raises every serializer error.)*"
- **What is wrong:** the core's `handle_api_error` (`rust/src/producer/kafka_producer.rs:2572-2581`)
  first runs `maybe_transition_to_error_state` and only then the callback. Java's `doSend` catch does
  the same (`transactionManager.maybeTransitionToErrorState`). The binding serializes before the core
  sees the record, so it reproduces only the callback and the failed future.

  Plan D23 and §7 record the rest as a remaining deviation: "a serializer that throws one of Java's
  five fatal producer classes (`ProducerFencedException`, …) does not make the producer fatal … Python
  has the same gap". Phase 0 records D23 in the rule text, but only the Python difference made it in.
  A reader of "as the core's `handle_api_error`" would expect the state transition too.
- **Fix:** add after the `ApiException` sentence: "The core never sees the error, so Java's
  `maybeTransitionToErrorState` does not run: a serializer that throws one of Java's fatal producer
  classes (`ProducerFencedException`, …) does not make the producer fatal, as in Python (a recorded
  deviation; the transactional case belongs to the transactions step)." Alternatively, narrow the
  phrase to "as Java's `doSend` and the core's `handle_api_error` do for the callback and the future".

Resolved: `dotnet/CLAUDE.md` §3, the serializer bullet, is narrowed to "as Java's `doSend` and the core's `handle_api_error` do for the callback and the future". It adds: "The core never sees the error, so the binding cannot run Java's `maybeTransitionToErrorState`: a serializer that throws one of Java's fatal producer classes (`ProducerFencedException`, …) does not make the producer fatal, as in Python (the transactional case belongs to the transactions step)". This matches plan §7.

## 4. Low: the §B5 Why cites a root `CLAUDE.md` line that says the opposite of the new text, and the §A5 Why calls `TypeError` a Java-named error

- **File:** `dotnet/.claude/rules/ffi-marshalling.md`, §B5 **Why** (lines 2000-2002) and §A5 **Why**
  (lines 770-771).
- **Passages:**
  - §B5: "Python its Java-named errors (`IllegalArgumentError`, and `TypeError` for Java's
    `NullPointerException`; root `CLAUDE.md:431` at `3f8a5bc6`)".
  - §A5: "Python its Java-named errors (`TypeError` for Java's `NullPointerException`)".
- **What is wrong:** judgment call (c) is right for today's rules, but its citation is not. Line 431 of
  root `CLAUDE.md` at `3f8a5bc6` (`git show 3f8a5bc6:CLAUDE.md | sed -n 431p`) is exception 4 of
  Implementation over the FFI as it read then: "`partitions_for` -> `NullPointerError("topic cannot be
  null")` … `pause` -> `NullPointerError…`". The cite supports the old text and contradicts the new.

  The new claim matches the current root `CLAUDE.md`:
  - Types, the `Objects.requireNonNull` row: "there is no `NullPointerError` class" (line 389);
  - exception 4 (line 495);
  - `python/confluent_kafka/admin/create_topics_result.py:31`.

  In §A5, `TypeError` is given as the example of Python's "Java-named errors"; it is Python's built-in.
- **Fix:**
  - §B5: cite the current text without the stale pin, e.g. "root `CLAUDE.md`, Python Binding
    Conventions: Types (the `Objects.requireNonNull` row) and Implementation over the FFI,
    exception 4".
  - §A5: "Python its Java-named errors (`IllegalArgumentError`, `IllegalStateError`) and `TypeError`
    for Java's `NullPointerException`", to match §B5.

Resolved: ffi §B5 **Why** drops the stale `CLAUDE.md:431` pin and cites "root `CLAUDE.md`, Python Binding Conventions: Types, the `Objects.requireNonNull` row, and Implementation over the FFI, exception 4". Both §B5 and §A5 **Why** now read "Python its Java-named errors (`IllegalArgumentError`, `IllegalStateError`) and `TypeError` for Java's `NullPointerException`".

## Unfiled observation: ffi §0.1's `int8_t` predicate example

- **File:** `dotnet/.claude/rules/ffi-marshalling.md` §0.1, **Tests required**.
- **What is wrong:** the example named `is_retriable_error`. D5 removes that read, so a rule should
  not name it as a predicate the binding calls.

Resolved: the example now names `is_api_error`, which D18 keeps (`kafka_common_Error_is_api_error_err`
returns `int8_t` in the header).

## 5. Low: ffi §B5 still sends a consumer's "null topic" to `ArgumentNullException`, where Java, Python and the core throw `IllegalArgumentException`

- **File:** `dotnet/.claude/rules/ffi-marshalling.md` §B5. Two passages:
  - the **Precondition** bullet (lines 1896-1898): "Any other .NET-only precondition, where Java
    throws `NullPointerException` or nothing, keeps `ArgumentNullException` (null
    topic/partition/config) or `ArgumentOutOfRangeException`."
  - **Tests required** (line 2041): "Null topic/partition → `ArgumentNullException`; …"
- **What is wrong:** for the consumer, a null topic is mostly a Java `IllegalArgumentException`
  site, which D20 sends to the D15 class with Java's message. `AsyncKafkaConsumer` throws:
  - "Topic collection to subscribe to cannot be null" for a null `Subscribe` collection
    (`subscribeInternal`);
  - "Topic collection to subscribe to cannot contain null or empty topic" for a null or blank
    topic in it;
  - "Topic partitions collection to assign to cannot be null" and "Topic partitions to assign to
    cannot have null or empty topic" (`assign`);
  - "RebalanceListener cannot be null" (`subscribe(Collection, ConsumerRebalanceListener)`).

  Python raises `IllegalArgumentError` for each of these (`subscribe_topics` and
  `subscribe_callback` in `python/confluent_kafka/consumer/_base.py:163-165`). It sends a `None`
  topic to the core as `""`, and the core returns −3 with Java's text
  (`rust/src/consumer/internals/async_kafka_consumer.rs:4889-4893`, `:5100-5104`).

  ffi §A5 was updated for the producer's equivalent ("a null topic ("Topic cannot be null.")"),
  but §B5 still gives "null topic" as an `ArgumentNullException` example, and the test line asks
  for it unconditionally. That is also today's code: `SnapshotTopics` throws
  `ArgumentNullException` for a null collection and a plain `ArgumentException` for a null element,
  while a blank topic gets the core's −3 `IllegalArgumentException`. So the passage keeps the
  split D20 removes: one condition with two types. A Phase 3 Actor following the example would
  keep it. The NPE sites the example can mean are real: `PartitionsFor(null)` (Python:
  `TypeError("topic must not be None")`), `Pause` / `Resume` (`requireNonNull`),
  `OffsetsForTimes`, and `BeginningOffsets` / `EndOffsets` ("Partitions cannot be null").
- **Fix:** in the Precondition bullet, add the consumer's Java `IllegalArgumentException` sites to
  the "throw it with Java's message" list: `Subscribe`'s and `Assign`'s null collection, a null
  or blank topic in them, and a null listener, each with the Java text above. Restrict the
  `ArgumentNullException` examples to Java's NPE sites: `PartitionsFor`'s null topic, `Pause` /
  `Resume`'s and the offset lookups' null collection, and a null config. Split the test line the
  same way: "a null `Subscribe` / `Assign` collection, a null topic in it, or a null listener →
  `IllegalArgumentException` with Java's message; a null `PartitionsFor` topic or `Pause`
  collection → `ArgumentNullException`".

Resolved: ffi §B5 **Precondition** now lists the consumer's Java `IllegalArgumentException` sites under "throw it with Java's message", each with Java's text (checked against `AsyncKafkaConsumer`): a null `Subscribe` collection or a null or blank topic in it; a null or empty `SubscriptionPattern` (`throwIfSubscriptionPatternIsInvalid`); a null `Assign` collection or a null or blank topic in one of its partitions; a null listener; a null `SeekToBeginning` / `SeekToEnd` collection ("Partitions collection cannot be null", which Python also raises as `IllegalArgumentError`). The `ArgumentNullException` examples are now only Java's NPE sites: a null `PartitionsFor` topic; a null `Pause` / `Resume` (`requireNonNull`), `Committed` (`partitions.isEmpty()`), `BeginningOffsets` / `EndOffsets` (`requireNonNull`) collection or `OffsetsForTimes` map (`requireNonNull`); a null config. The **Tests required** line is split the same way. `dotnet/CLAUDE.md` §3's row names no consumer null topic or partition, so it is unchanged.
