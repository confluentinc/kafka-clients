# Python binding rules — decisions for the owner

**Owner, 2026-09-25: three general rules APPROVED; they settle most items below.**
- Rule 1 — Java decides the content (classes, methods, parameters, types, defaults, behaviour); Python
  decides only the form, through the idiom translations listed in the rules; deliberate content changes
  are marked *(deviation)*. Settles 3, 4, 6, 9, 10, 11, 12, 13 (form: `get` dropped), 14, 15, 16, 19, 29;
  23 falls to the core-gap rule (the core has no interceptors, so `interceptor.classes` still raises).
- Rule 2 — Java's standard types map to Python's standard types by a fixed table. Settles 2, 5, 20
  (`Closeable`), 21, 28 (`Metric` a `Protocol`, `KafkaMetric` a class).
- Rule 3 — where Java has nothing, Python's standard convention. Settles 25 (`DeprecationWarning` +
  docstring), 26 (root exports only `Duration` + the Java built-in exception classes), 27
  (`AsyncMockShareConsumer`).
- Individually ruled: 1 (keyword `message=`), 2, 5 (`RuntimeError`), 7, 8, 17, 18, 21, 22, 24, 30,
  31, 32.
- Items 1–32 are all ruled, and item 33 is CONFIRMED with the FFI policy. The owner approved the
  resulting rules text; the Actor's nine round-5 choices wait for the post-phase review at the end.

These are the points where the CLAUDE.md §4 draft (Python binding conventions) could not follow
Java on its own, or follows the spec where the spec differs from Java. Each one needs a yes or no
from the owner. They are ordered by impact: the ones users see in their own code come first. The
last item is not a decision; it lists the spec and package changes the rules already force.

Each item gives what it is, what Java does, what the spec and the package do now, the proposed
rule, and what changes in the package if the owner picks the Java way. File references are to
`kafka/clients/src/main/java/org/apache/kafka/` (Apache Kafka 4.3.1) unless a path says otherwise.

1. **Error constructors take the message by position.** Java's exceptions have constructor
   overloads `(String message, Throwable cause)`, `(String message)`, `(Throwable cause)` and `()`
   (`common/KafkaException.java:26-38`); some also take a payload
   (`common/errors/TopicAuthorizationException.java:25-36`).
   Now: the package builds a plain `Exception(*args)`, and a payload getter on an error you built
   yourself raises `AttributeError`. §4 now makes `message` positional-only, found as the `String`
   passed unchanged to `super(...)`. Every `Throwable` becomes the keyword `cause=`, other payload
   arguments are keywords, and payload getters return Java's defaults.
   Proposed: keep the positional message; it is how every Python exception works (`KafkaError("boom")`).
   Java way (keyword-only `message=`): every `raise KafkaError("…")` becomes a `TypeError`.
   **RULED (owner, 2026-09-25): keyword `message=` for now (the Java way).** Revisit the keyword-only
   rule for a few fields (error message, `ProducerRecord` topic/partition, …) in the next phase of
   interface updates (improvements list #3). Because a keyword-only message breaks Python's default
   copy/pickle rebuild (`cls(*e.args)`), the base `KafkaError` needs one shared rebuild method that
   recreates any error from its constructor arguments by name; payload-only errors need it anyway.

2. **`uuid_serializer()` / `uuid_deserializer()` wire format.** Java's `UUIDSerializer` handles
   `java.util.UUID` and writes its dashed 36-character form
   (`common/serialization/UUIDSerializer.java:24,30,49`). Kafka's own `Uuid` prints as URL-safe
   base64 (`common/Uuid.java:123-124`).
   Now: the spec types these factories as `Serializer[Uuid]` for Kafka's `Uuid`, and the package
   writes the base64 form. Java consumers cannot read what it writes, and it cannot read what they
   write. §4 keeps this as a deviation.
   Proposed: use Java's wire format with Python's `uuid.UUID`, the equivalent of `java.util.UUID`.
   Java way: the two factories are typed `uuid.UUID`, the bytes match Java, and the §4 table row
   "never Python's `uuid.UUID`" gets an exception for `java.util.UUID`.
   **RULED (owner, 2026-09-25): A — Java's way.** `uuid_serializer()` / `uuid_deserializer()` take
   Python's `uuid.UUID` and write Java's dashed form. Owner: this should have been derived from the
   rules — a JDK type maps to its Python standard-library equivalent (`java.util.UUID` -> `uuid.UUID`).

3. **Delivery callback metadata on failure.** Java never passes `null`. It passes a `RecordMetadata`
   with -1 in every field; the topic is set, and the partition is -1 if none was chosen
   (`producer/Callback.java:26-33`, `producer/KafkaProducer.java:1589-1595`,
   `producer/MockProducer.java:650`).
   Now: the spec and the package pass `None`; §4 keeps `None` as a deviation.
   Proposed: follow Java. `has_offset()` and `offset() == -1` were kept for exactly this case.
   Java way: `DeliveryCallback = Callable[[RecordMetadata, KafkaError | None], None]`, the send path
   builds the -1 metadata on failure, and the migration notes change.

4. **Callback alias names.** Java calls these interfaces `Callback` (`producer/Callback.java:24`),
   `OffsetCommitCallback` and `AcknowledgementCommitCallback`.
   Now: the spec, the package and §4 (as a deviation) use `DeliveryCallback`, `CommitCallback` and
   `AckCommitCallback`. Any future callback interface keeps Java's name.
   Proposed: use Java's three names.
   Java way: the three aliases are renamed in exports, stubs and docs; behaviour does not change.

5. **`RuntimeException` / `Exception` values are typed `KafkaError`.** Java's
   `MockProducer.errorNext(RuntimeException e)` (`producer/MockProducer.java:585`) and the nine
   failure-injection fields (`:80-96`) accept any runtime exception. Java's own test injects an
   `IllegalArgumentException` (`clients/src/test/java/org/apache/kafka/clients/producer/MockProducerTest.java:115-116`).
   Now: the spec and §4 say `KafkaError`; the package widened this to `BaseException`.
   Proposed: `Exception` for values the user passes in (mock injection), and `KafkaError` for values
   the client hands out.
   Java way: `error_next(*, e: Exception)` and `set_*_exception(*, …: Exception | None)`.
   **RULED (owner, 2026-09-25): `RuntimeError`.** Injected values are typed `RuntimeError` (Java's
   `RuntimeException`), and `KafkaError` subclasses `RuntimeError`, as Java's `KafkaException` extends
   `RuntimeException` (the D1 item-5 principle, which had been applied only to the JDK analogs).

6. **`MockConsumer` takes deserializer arguments.** Java's mock has only `MockConsumer(String)`
   and the deprecated `MockConsumer(OffsetResetStrategy)` (`consumer/MockConsumer.java:91,99`), and
   `addRecord` takes decoded records (`:322`).
   Now: spec §6.2's signature leaves the arguments out, but its prose needs them. The package has
   `key_deserializer=` / `value_deserializer=`, and `add_record` takes serialized bytes. §4 keeps
   this as a deviation.
   Proposed: keep them, since this is how the mock tests the real deserializer path, and add them to spec §6.2.
   Java way: remove both arguments; `add_record` takes decoded values and `poll()` does not deserialize.

7. **Type of a `ProducerRecord` without a key — adopted, pending the ruling.** Java leaves `K`
   free (`producer/ProducerRecord.java:142-144`), so a keyless record fits any producer.
   §4 now makes both records covariant in `K` and `V`. A key or value that is omitted or `None`
   binds its type variable to `Never` (`typing_extensions.Never` on Python 3.10). So
   `ProducerRecord(topic="t", value="x")`, `key=None` and a tombstone `value=None` all infer and fit
   any producer under `mypy --strict`; the earlier `bytes` binding broke the keyless case.
   `MockShareConsumer()` has nothing to bind from, and the caller annotates it
   (`c: MockShareConsumer[str, str] = MockShareConsumer()`), as Java writes the type arguments.
   Proposed: confirm this.
   Alternative: leave `K` unbound. mypy then asks for an annotation, against "no code writes type parameters".
   **RULED (owner, 2026-09-25): `Never`** for an omitted or `None` key/value, with covariant records.

8. **Typing on the config route.** A serde given through `key.serializer` / `value.deserializer`
   in `configs` is invisible to the type checker. This was already so in Java, whose config route
   is unchecked. §4's binding stubs type such a client as if the serde were omitted, so
   `KafkaConsumer(configs={"value.deserializer": "app.OrderDeser"})` is `KafkaConsumer[bytes, bytes]`,
   and no annotation can correct it.
   Now: this is the trade-off for inferring every bare construction without annotations.
   Proposed: accept it, and document "pass serdes as arguments when typing matters".
   Alternative: bind an omitted serde to `Any`. Bare constructions then lose `[bytes, bytes]`, but
   the config route is no longer typed wrongly.
   **RULED (owner, 2026-09-25): A for now** — keep the draft; document "pass serdes as arguments
   when typing matters". Option C (explicit type parameters on the config route) parked as
   improvements-list #4.

9. **`client_instance_id()` has an optional timeout.** Java has only
   `clientInstanceId(Duration timeout)`, with the timeout required (`producer/Producer.java:106`,
   `consumer/Consumer.java:182`, `consumer/ShareConsumer.java:99`).
   Now: the spec and the package use `timeout=None`. §4 keeps this: `None` waits
   `default.api.timeout.ms` on a consumer and `max.block.ms` on the producer.
   Proposed: keep it, to match every other timed method.
   Java way: `client_instance_id(*, timeout: Duration)`, required.

10. **`KafkaProducer(partitioner=…)`.** Java's constructors take no partitioner
    (`producer/KafkaProducer.java:295-341`); it comes from the `partitioner.class` config key
    (`producer/ProducerConfig.java:313`).
    Now: the spec and the package have the argument; the package raises if it is given. In §4 the
    argument and the config key are two routes to one plugin; on `Kafka*` classes both raise
    `UnsupportedVersionError` until partitioners are designed.
    Proposed: keep the argument; it matches the serde routes.
    Java way: drop the argument and keep only `partitioner.class`.

11. **`MockProducer.history_count()`.** Java has only `history()` (`producer/MockProducer.java:529`).
    Now: the spec, the package and §4 (as a deviation) keep `history_count()`.
    Proposed: drop it, as `client_id()` and `pending_count()` were dropped.
    Java way: remove it; tests use `len(p.history())`.

12. **`TopicIdPartition` parameter and stub order.** Java declares
    `TopicIdPartition(Uuid topicId, TopicPartition topicPartition)` first
    (`common/TopicIdPartition.java:36`) and `(Uuid topicId, int partition, String topic)` second (`:48`).
    Now: the spec and the package put the `(topic_id, partition, topic)` form first; §4 keeps this
    as an explicit deviation.
    Proposed: follow Java.
    Java way: parameters become `topic_id, topic_partition, partition, topic` and the stubs swap.
    Every argument is a keyword, so no call breaks.

13. **Dropping Java's `get` prefix.** Java names the getters `getMostSignificantBits()` /
    `getLeastSignificantBits()` (`common/Uuid.java:87,94`).
    Now: the spec and the package use `most_significant_bits()`. §4 generalizes this into
    "drop a leading `get`, keep `is` / `has`", marked as a deviation.
    Proposed: keep it; the modern Kafka convention already has no `get` prefix.
    Java way: `get_most_significant_bits()`, `get_least_significant_bits()`.

14. **Which constants and public fields are exposed.** Java has public constants on translated
    classes:
    - `ConsumerRecords.EMPTY` (`consumer/ConsumerRecords.java:42`);
    - `ConsumerRecord.NO_TIMESTAMP` / `NULL_SIZE` (`consumer/ConsumerRecord.java:56-57`);
    - `RecordMetadata.UNKNOWN_PARTITION` (`producer/RecordMetadata.java:31`);
    - `KafkaProducer.NETWORK_THREAD_PREFIX` / `PRODUCER_METRIC_GROUP_NAME` (`producer/KafkaProducer.java:245-246`);
    - error singletons such as `DisconnectException.INSTANCE` and
      `CoordinatorNotAvailableException.INSTANCE` (`common/errors/DisconnectException.java:24`,
      `CoordinatorNotAvailableException.java:27`);
    - `Uuid`'s four (`common/Uuid.java:37-52`).

    It also has public enum fields such as `TimestampType.id` / `name` (`common/record/TimestampType.java:27`).
    Now: the spec shows only `Uuid`'s constants, and §4 follows it. The package also has
    `UNKNOWN_PARTITION` and a `TimestampType.label()`; both must go under §4 either way.
    Proposed: expose every public constant of a translated class. An enum's `id` field is its
    `IntEnum` value; other enum fields are reached through Java's own methods (`toString` → `__str__`,
    `forName` → `for_name`).
    Java way: add those constants as class attributes.

15. **`AcknowledgementMode` enum.** Java has only the internal `ShareAcknowledgementMode`, which
    wraps a nested enum `AcknowledgementMode { IMPLICIT, EXPLICIT }`
    (`consumer/internals/ShareAcknowledgementMode.java:28-41`) and is used as a config value only.
    Now: spec §5.3 defines a public `AcknowledgementMode`; §4 keeps it as a deviation.
    Proposed: drop it. Config values are strings in `configs`, as `group.protocol` is.
    Java way: remove it from spec §5.3.

16. **An injected error comes back as the same object.** Java's mock throws the very exception
    the test set (`consumer/MockConsumer.java:267-270`).
    Now: the package sends the error to the Rust core by its FFI id and builds a new object on the
    way back, so a user's own subclass returns as its nearest generated parent. A plain `KafkaError`
    cannot be injected at all today (`TypeError: KafkaError has no _ffi_id`). Giving the base the id
    −1 alone would make it return as `UnknownServerError`. §4 now keeps the injected instance, per
    injection slot, and raises that same object.
    Proposed: confirm this. It follows Java exactly.
    Package change: the mocks keep the injected instance in Python and raise it when the core reports the injected error.

17. **The header copy against CLAUDE.md §12.** §12 forbids copying key, value or header bytes on
    the send path. The package copies header values once at the C-to-Rust boundary, because the
    core's `RecordHeader` owns its value as `Option<Vec<u8>>`.
    Now: §4 no longer allows the copy, so the package breaks §12.
    Proposed: either make the core's headers borrow their value (a core change), or add a
    header-value carve-out to §12. The owner picks one.
    Java way: nothing to compare; Java keeps the same `byte[]` without copying.
    **RULED (owner, 2026-09-25): A** — §12 stays the rule, no carve-out; the copy is recorded as a
    Rust-core gap in `design/current/ffi-overload-gaps.md` (core headers must borrow their value).

18. **The runtime check mechanism in `_args.py`.** Java resolves overloads at compile time and
    has no runtime check.
    Now: §4 adds `at_least_one` next to `exactly_one`, `at_most_one` and `all_or_none`, fixes the
    order in which they are called, and adds a private `UNSET` default. `UNSET` lets "not given"
    differ from a constant default or from Java's `null` (`acknowledge(type=…)`,
    `TopicIdPartition(topic=None)`). The owner had asked to revisit this mechanism when it was implemented.
    Proposed: confirm the four helpers, the call order and `UNSET`.
    Alternative: rely on the typing stubs only, with no runtime check.
    **RULED (owner, 2026-09-25): B — one `java_forms` decorator** in `confluent_kafka/_args.py`. Each
    merged method lists Java's forms (its overloads' parameter sets) and the Java-given defaults; the
    decorator matches the set of GIVEN arguments (not `UNSET`) against the forms, raises
    `IllegalArgumentError(message="<m>() takes one of (<a>, <b>), (<c>); got (<given>)")` when none
    matches, fills the defaults, then runs the body. `UNSET` stays, only where one form requires a
    parameter another defaults. A method whose every combination is a Java form has no decorator.
    Replaces `exactly_one` / `at_most_one` / `at_least_one` / `all_or_none` and the call-order rule.

19. **Old-client callback keys in `configs`.** Java accepts unknown keys and logs them as unused
    (`AbstractConfig.logUnused()`).
    Now: the package rejects `error_cb`, `on_delivery`, `logger` and similar librdkafka keys with a
    `ConfigError` that names their replacement (`bindings/python/confluent_kafka/_config.py:264-305`).
    §4 says unknown keys are accepted and logged.
    Proposed: keep the rejection as a named deviation, since it catches ported code that would
    otherwise silently lose its callbacks.
    Java way: drop the rejection; those keys are then only logged as unused.

20. **`Closable` and `Configurable` protocol names.** Java's serdes extend `java.io.Closeable`
    (`common/serialization/Serializer.java:36`). Java's `org.apache.kafka.common.Configurable` has a
    one-argument `configure(Map)` (`common/Configurable.java:24,29`), while the Python protocol has
    the serde's two arguments.
    Now: the spec, the package and §4 use `Closable`, `Configurable` and `SerdeBase`.
    Proposed: rename `Closable` to `Closeable`, and keep `Configurable` with the serde signature.
    Java way: `Closable` becomes `Closeable`.

21. **JDK exceptions without a Python analog class.** Java throws `NoSuchElementException` from
    `TimestampType.forName` (`common/record/TimestampType.java:39`), `NullPointerException` from
    `requireNonNull`, and `UnsupportedOperationException` from the mocks
    (`consumer/MockConsumer.java:482,536`, `producer/MockProducer.java:406`).
    Now: the package uses `KeyError` and `TypeError`, and uses `NotImplementedError` or
    `UnsupportedVersionError` in different places. §4 maps to `TypeError`, `KeyError`, `IndexError`
    and `ArithmeticError`, and maps `UnsupportedOperationException` to `UnsupportedVersionError`,
    as the core does.
    Proposed: keep the §4 mapping.
    Java way: new root analogs (`NoSuchElementError`, …) with new FFI ids.
   **RULED (owner, 2026-09-25): Java names.** A Java built-in exception gets a Python class only if the
   translated Java code throws it (incl. `Objects.requireNonNull`); name Java's with `Error`, at the
   package root, subclassing `RuntimeError` (`builtins.TimeoutError` for `TimeoutException`); an FFI id
   only when the core models it (`Local*`: -2..-5). Today: `NoSuchElementError`, `NullPointerError`
   (Python-only, no id). `UnsupportedOperationException` -> `UnsupportedVersionError` because the core
   returns `unsupported_version`; a core `LocalUnsupportedOperation` is recorded as a missing piece.
   `IndexOutOfBounds`/`Arithmetic`/`ClassCast` are not defined (Java's mirrored code never throws them).

22. **Where error classes of other packages live.** Java puts `BufferExhaustedException` in
    `clients.producer` (`producer/BufferExhaustedException.java:29`), and other errors in several
    `common.*` packages.
    Now: the package and §4 put every Kafka error in `common.errors`, except the consumer package's
    errors and `ConfigError`.
    Proposed: keep this; one import path covers almost every error.
    Java way: `confluent_kafka.producer.BufferExhaustedError`, and one errors module per Java package.
    **RULED (owner, 2026-09-25): A — Java's package.** Every error lives in the module mirroring its
    Java package (`clients` dropped): `BufferExhaustedError` -> `confluent_kafka.producer`,
    `InvalidRecordError` -> `confluent_kafka.common`, and `common.requests` / `common.network` /
    `common.metrics` / `common.protocol.types` modules for the other four. Closes C3.

23. **The `interceptor.classes` config key.** Java accepts it (`consumer/ConsumerConfig.java:321`,
    `producer/ProducerConfig.java:332`).
    Now: the spec says "not accepted", the package accepts it silently, and §4 raises
    `ConfigError` (as a deviation).
    Proposed: raise `ConfigError` until interceptors are designed.
    Java way: accept the key and run interceptors, which needs the interceptor design first.

24. **Dropping `enforceRebalance` leaves `MockConsumer.should_rebalance()` with nothing to do.**
    Java's mock sets `shouldRebalance = true` only in `enforceRebalance`
    (`consumer/MockConsumer.java:697-712`).
    Now: §4, the spec and the package drop `enforce_rebalance`, since under KIP-848 it only logs,
    so `should_rebalance()` can never return true.
    Proposed: drop `should_rebalance()` and `reset_should_rebalance()` too.
    Java way: keep `enforce_rebalance()` on the mock only.
    **RULED (owner, 2026-09-25): A — drop both.** `MockConsumer` has no `should_rebalance()` and no
    `reset_should_rebalance()`: once `enforce_rebalance()` is dropped, nothing can set the flag. §4
    states the general form: a mock helper that only reads or clears state set by a dropped method is
    dropped with it *(deviation)*.

25. **How deprecated methods are marked.** Java uses `@Deprecated`, and the compiler warns
    (`consumer/Consumer.java:283`, `consumer/MockConsumer.java:90`).
    Now: the package and §4 put the note in the docstring only.
    Proposed: also add `typing_extensions.deprecated` (PEP 702), so type checkers warn.
    Package change: decorate each deprecated method and stub.

26. **What the package root exports.** Java has no counterpart; the spec leaves it open.
    Now: §4 exports only `Duration` and the JDK analogs from the root.
    Proposed: keep this, and revisit with any compatibility module.
    Alternative: also re-export everyday names (`KafkaConsumer`, `ProducerRecord`, `KafkaError`).

27. **`AsyncMockShareConsumer`.** Java has `MockShareConsumer()` (`consumer/MockShareConsumer.java:57`).
    Now: spec §6.3 lists no async mock for the share consumer; §4's family rule creates one.
    Proposed: keep it, so every family has the same shape.

28. **`Metric` and `KafkaMetric` as `Protocol`s.** Java's `KafkaMetric` is a final class with a
    public constructor (`common/metrics/KafkaMetric.java:25,42`).
    Now: spec §5.1 shows `class KafkaMetric(Metric)`; the package and §4 make both `Protocol`s and
    do not expose the constructor.
    Proposed: keep this; instances only come from `metrics()`.

29. **Member order inside a class.** Java's order puts `initTransactions` first and `send` in the
    middle (`producer/Producer.java:45-116`), and `onPartitionsRevoked` before `onPartitionsAssigned`.
    Now: spec §6 groups methods by topic, and the package follows the spec. §4 uses Java's order,
    and the interface check ignores order.
    Proposed: Java's order in generated code; the spec's grouping is documentation.
    Package change: members are reordered; nothing else changes.

30. **Async-ness follows the Rust core.** Java's `currentLag` waits on the background thread
    (`consumer/internals/AsyncKafkaConsumer.java:1505`), yet the spec makes it a plain `def`. The FFI
    has `begin_transaction_async`, yet the spec keeps `begin_transaction` a plain `def`.
    Now: §4 makes a method `async def` iff the core method is `async fn`. That reproduces the spec,
    but generation must read the Rust core, not only the FFI header.
    Proposed: keep it.
    **RULED (owner, 2026-09-25): Java decides — revisit in the second interface pass.** On the async
    classes a method is `async def` iff Java waits in it, waiting on the background thread included
    (`addAndGet`); otherwise a plain `def` on both classes. Generation reads Java only, and the
    `x_nowait()` exception disappears (Java's `commitAsync` uses `add`, not `addAndGet`). Only change
    in 4.3.1: `AsyncKafkaConsumer.current_lag()` / `AsyncMockConsumer.current_lag()` become
    `async def`, so spec §6.2 moves `current_lag` into the async group. Implementation: expose the
    core's `current_lag_async()` on the `Consumer` trait plus one FFI entry (recorded in
    `ffi-overload-gaps.md`; today's sync core `current_lag()` always returns `None`). Logged for the
    second pass as improvements item 5.

31. **The overlap fallback.** When one Java parameter name carries types Python cannot tell apart
    (`str` versus `Iterable[str]`), §4 splits it into `<name>_<type>` parameters. When the second
    type arrives later, the existing parameter keeps its name. No such case exists in 4.3.1.
    Now: §4 makes generation stop at such a site until the owner confirms.
    Proposed: confirm the rule as written.
    **RULED (owner, 2026-09-25): A — split, as discussed earlier.** Types `isinstance` cannot tell
    apart get one parameter per type, `<name>_<type>` (`topics_str=` / `topics_iterable=`), checked by
    `java_forms` like any other pair of names; when the second type arrives in a later Java release,
    the existing parameter keeps its plain name and only the new one is suffixed *(deviation)*. The
    "generation stops until the owner confirms" clause is removed. Still stop when two Java types map
    to the same Python type (Java `int` and `long` both `int`), since no suffix can tell them apart.
    Proposal for the §4 draft (owner reviews with §4): `java_forms` builds its check once when the
    class is defined, not per call (85 ns vs 550 ns per call, measured on 3.11).

32. **CLAUDE.md numbering.** The new section is item `4.`. The old `4. **Comments and documentation**`
    stays, and master already has two items numbered `3.`. Other rule files cite CLAUDE.md numbers
    (`§9.6`, `§10.4`, `§12`).
    Proposed: put the Python rules under their own heading, `## Python binding conventions`, with
    the same bullets, so no existing number moves.
    Alternative: renumber items 3–13 and update every rule file that cites them.
    **RULED (owner, 2026-09-25): A — own heading.** The Python rules move out of the numbered list
    into their own section, `## Python Binding Conventions`, placed after `## Translation Rules`; no
    existing number moves (225 files cite them). Cited as "CLAUDE.md, Python Binding Conventions".
    The pre-existing duplicate `3.` on master is left as it is.

33. **Changes the rules already force — no decision needed, listed for confirmation.**

    Spec edits:
    - `Headers` read values become `memoryview | None`, and `ProducerRecord(headers=…)` spells out
      the written type.
    - Serde parameters default to `None` in §6.1–§6.3.
    - Constructors get `self`-typed binding stubs in a fixed order: `ProducerRecord` and
      `ConsumerRecord` have four each (key and value given, or omitted/`None` → `Never`).
      (`MockConsumer` has none since ruling 6 removed its deserializer arguments; the caller annotates
      it, `c: MockConsumer[str, str] = MockConsumer(...)`.)
    - A returned `Duration` is a `float` (`CloseOptions.timeout()`).
    - `set_client_instance_id(instance_id: Uuid | None)` and `TopicIdPartition` `topic: str | None`.
    - `acknowledge`, `MockConsumer.add_record` and `MockShareConsumer.add_record` take
      `ConsumerRecord[K, V]` (ruling 6: decoded records, as Java).
    - `Uuid.to_array()` / `to_list()` exist.
    - `OffsetResetStrategy` members are in Java's order: `LATEST`, `EARLIEST`, `NONE`.
    - §5's `Duration` paragraph gains the producer's `max.block.ms` fallback.
    - `MockProducer(cluster=…, partitioner=…)` works as in Java.
    - A static factory and an instance getter that share a name (`CloseOptions.timeout`) become one
      attribute. This was an owner item; it is not a decision, because it is already Java's naming.

    Package fixes:
    - Error classes get Java's constructors (generated by `xtask`) and their payload defaults, and
      the base `KafkaError` gets `_ffi_id = -1`.
    - Serde arguments default to `None`, so a `key/value.(de)serializer` config key is used.
    - The `Headers` alias gains `| None`.
    - `_args.py` gains `at_least_one` and `UNSET`.
    - Remove `RecordMetadata.UNKNOWN_PARTITION` and `TimestampType.label()` (unless item 14 is decided the Java way).
    - `send_offsets_to_transaction(offsets: Mapping)`.
    - `interceptor.classes` raises `ConfigError`.
    - `MockProducer.client_instance_id()` raises `UnsupportedVersionError`, not `NotImplementedError`.
    - Mocks implement every Java mock method and constructor argument: `client_instance_id`,
      `schedule_poll_task`, reentrant listener calls, and a `cluster` / `partitioner` that is used
      rather than rejected.
    - An injected error is raised as the same instance (item 16).
    - The `commit_nowait()` callback runs on the caller's thread.
    - The `_args` calls follow §4's order. Today `TopicIdPartition(topic_id=…, topic="t")` raises
      "takes exactly one of partition, topic_partition; got none", where §4 gives "needs all of
      partition, topic together; got topic". `TopicIdPartition(topic_id=…, partition=0)` is accepted
      today but rejected by §4.
    - The typing fixes are package work too, not only spec edits: the binding stubs (today every
      bare construction reports `Need type annotation`), and the spelled-out `ProducerRecord`
      headers input type.
    - `KafkaError.__str__` with only a cause returns `""` today; it must return Java's
      `getMessage()`, which with only a cause is the cause's `toString()` text.
    - FFI: add the conforming entry-point names and keep the old ones as deprecated
      (`seek_with_metadata` → `seek_with_offset_and_metadata`, `commit_sync_offsets` →
      `commit_sync_with_offsets`), and add the missing timed forms (`committed`, `position`, …).
    - `OffsetResetStrategy` values become the member names, and `origin()` returns the nested enum.
    - The header copy (item 17) and the old-client key rejection (item 19) follow the owner's rulings.

    **Updated by the rulings (2026-09-25) — the list above as it now reads:**
    - `_args.py`: the `java_forms` decorator (18) replaces `at_least_one` and the other helpers and
      the call-order rule; `UNSET` stays only where one form requires a parameter another defaults.
      The `TopicIdPartition` examples still change behaviour, with `java_forms`' message.
    - Constants (14, rule 1): `RecordMetadata.UNKNOWN_PARTITION` STAYS, and Java's other public
      constants are added (`ConsumerRecords.EMPTY`, `ConsumerRecord.NO_TIMESTAMP` / `NULL_SIZE`, …);
      only `TimestampType.label()` is removed.
    - FFI names: master's FFI still has non-conforming names (`seek_with_metadata`,
      `commit_sync_offsets`); renaming them to the §2/§3 name is a minor FFI fix allowed in this PR,
      with the old name kept as a deprecated alias.
    - Header copy (17): §12 stays; Python copies header values once until the core's
      `RecordHeader` borrows (core gap 1).
    - Old-client keys (19, rule 1): accepted with Java's warn-on-unused log; the `ConfigError`
      rejection of `error_cb` / `on_delivery` / `logger` goes.
    - New spec edits from the rulings: error message is `message=` (1); `uuid_*` serdes take
      `uuid.UUID`, dashed (2); `KafkaError(RuntimeError)` (5); `NoSuchElementError` /
      `NullPointerError` at the root (21); errors in their Java package's module (22);
      `should_rebalance()` / `reset_should_rebalance()` dropped (24);
      `AsyncConsumer.current_lag()` is `async def` (30).

    **CONFIRMED (owner, 2026-09-25), with an FFI policy that replaces the "add the missing timed
    forms" line above:**
    - No new FFI entry points. Python maps its forms to the FFI generated by the Rust FFI rules
      (CLAUDE.md §2/§3), found by applying those rules.
    - Entry points this branch already added (23, e.g. `close_options`, the pending-callback queue,
      the `MockConsumer` helpers) are ported and used.
    - A Python method or overload form whose entry point exists in neither is NOT generated now; it
      is listed in `ffi-overload-gaps.md` and added in a later PR (an additive, non-breaking change,
      since every parameter is keyword-only).
    - Minor changes, updates and fixes to the FFI layer are fine in this PR (renames to the rule's
      name, the `commit_nowait()` callback thread, bug fixes).
    - This supersedes D7 (a `timeout=` whose timed entry point is missing was silently ignored) and
      the old rule that a method without core support raises the mapped error: such forms are left
      out instead. Known cases at confirmation: the `timeout=` of `commit()` (with or without `offsets=`), `committed`,
      `position`, `beginning_offsets`, `end_offsets`, `offsets_for_times`, `partitions_for`,
      `list_topics`; `client_instance_id()`; `register_metric_for_subscription()` /
      `unregister_metric_from_subscription()`; the async `current_lag()`; the share-consumer family.
      The mocks follow their base class.

## Post-phase review (owner, 2026-09-25)

The owner will relook at these after all phases (P1–P6) are done and update these specific rules
then. Until then they stay as Actor 72 wrote them in round 5; Critics do not reopen them.

1. `MockConsumer.rebalance` counts as "Java waits" because it waits on a rebalance listener it runs,
   so `AsyncMockConsumer.rebalance` can await an `async def` listener.
2. Ruling 24 widened: a mock helper that only reads, sets or clears state used only by a dropped or
   not-generated method is dropped too — `set_client_instance_id`, `inject_timeout_exception`,
   `disable_telemetry`, `added_metrics`.
3. Non-error types: ruling 22 moves only errors; `TimestampType`, `Headers`, `KafkaMetric` still fold
   into `confluent_kafka.common` (rule 1 could move them to their Java packages).
4. `partitioner.class`: a set value raises `ConfigError` (item 23's reasoning), where it raised
   `UnsupportedVersionError`.
5. `UNSET` only where one form requires a parameter another defaults, so
   `TopicIdPartition(topic_id=…, partition=0, topic=None)` is rejected although Java accepts it
   (item 18 cited it as an `UNSET` use; item 33 lists `topic: str | None`).
6. Rule 2 maps Java's `Exception` to Python's `Exception`: the callback error argument is
   `Exception | None` (was `KafkaError | None`).
7. `KafkaMetric`'s Java constructor (`MetricValueProvider`, `Time`) has no rule; unreachable today.
8. FFI-backed vs pure-Python mocks is listed per class (`MockProducer` is Python), with no general
   criterion for a future mock.
9. Enum public fields are reached through Java's methods (`__str__`, `for_name`), not attributes —
   from item 14's proposal; its "Java way" line covered constants only.
10. *(Manager, fixing Critic 72 R4-F1)* `java_forms` matches Java's overloads strictly: a left-out
    parameter must get a value that a shorter overload passes to that same overload. Visible effects,
    all Java-faithful: `acknowledge(topic, partition, offset)` without `type`,
    `commit_nowait(offsets=…)` without `callback`, `OffsetAndMetadata(offset, leader_epoch)` without
    `metadata`, `MockProducer(auto_complete=True)` alone, and `TopicAuthorizationError()` /
    `ConfigError()` are all rejected (Java has no such overload).
11. *(P3, Actor 74)* `str(e)` is Java's `getMessage()`, so `QuotaViolationError` (whose Java
    constructor passes no message and which overrides `toString()` instead) has `str(e) == ""`.
    The rule "`toString()` -> `__str__`" would give `"<class>: '<metric>' violated quota. Actual:
    …, Threshold: …"`; the Errors rule was taken as the more specific one.
12. *(P3, Actor 74)* A Java constructor argument with no Python analog is left empty:
    `InterruptException(String message)` passes `new InterruptedException()` as its cause, so
    `InterruptError(message=…).__cause__` is `None`; `Thread.currentThread().interrupt()` has no
    analog either.
13. *(P3, Actor 74)* A `Throwable` parameter in every overload (`RecordDeserializationError`'s
    `cause`) keeps the Errors rule's `cause: BaseException | None = None`, and `java_forms` counts
    it as given even at `None` (it is Java-required, and Java passes `null`). Otherwise every
    `RecordDeserializationError(…, cause=None)` would be rejected.
14. *(P3, Actor 74)* With `cause = None` always, the stub of a `(Throwable cause)` form is already
    accepted by the `(message, cause)` form's stub (`message` optional there), and mypy rejects a
    stub that can never match; such stubs are left out (`InvalidTopicError`, `RecordTooLargeError`).
15. *(P3, Actor 74)* `KafkaMetric`'s constructor takes Java's `(lock, metricName, valueProvider,
    config, time)` (item 7): `MetricValueProvider` and `Time` have no Types row and are typed
    `object`, called by Java method name (`value(config, now)`, `milliseconds()`); `lock` is used
    when it is a context manager; `is_measurable()` tests for Java's `measure(config, now)`, since
    `Measurable` is a placeholder alias of `object` and `isinstance` cannot tell.
16. *(P3, Actor 74)* `AcknowledgeType` is not generated: it is reachable only from the share
    consumer family, which is not generated (FFI), and the Scope rule adds only the `common` types
    the share consumer reaches (`TopicIdPartition`).
17. *(P3, Actor 74)* A written header that is not a `(str, bytes-like | None)` pair raises
    `TypeError` (Java's type system rules it out; no Java exception to mirror); a `None` key raises
    `NullPointerError(message="Null header keys are not permitted")` from Java's `RecordHeader`.
18. *(P3, Actor 74)* Item 5's pattern also rejects `ConfigError(name=…, value=None)`: `value` is in
    both `(name, value)` and `(name, value, message)`, so `None` reads as not given and
    `(name)` matches nothing, although Java's `new ConfigException(name, null)` is common.
19. *(P3, Actor 74)* An error the core reports that no Java constructor of its class can take
    (a `RecordDeserializationError` without its record) is built without the constructor, carrying
    the message only; `copy`/`pickle` keep it. `to_ffi_id()` of a class without an id
    (`NullPointerError`, a user's exception) is `UNKNOWN_SERVER_ERROR` (-1); a mock matches the
    error the core then reports to the injected instance by that reported id.
20. *(P3, Actor 74)* `OffsetAndMetadata`: `(offset)` folds into the earliest-declared
    `(offset, leaderEpoch, metadata)`, so that form's stub is
    `(offset, leader_epoch=None, metadata="")`. It accepts every combination, including the
    `(offset, leader_epoch)` that `java_forms` rejects, and the `(offset, metadata)` stub can never
    match (mypy rejects it). As in item 14 the subsumed stub is left out, and with one stub left
    (`typing.overload` needs two) the class has none: the type checker sees the implementation
    signature, and only `java_forms` rejects `(offset, leader_epoch)`.
21. *(P3, Actor 74)* `ConsumerRecord` has two forms (the short `(topic, partition, offset, key,
    value)` and the long `(…, headers, leaderEpoch[, deliveryCount])`), so the binding rule gives
    eight stubs, four per form, where ruling 33 lists "four each". `ProducerRecord` has four: every
    combination of its union matches one of its six constructors, so it has no form stubs, only
    the four binding stubs over the union signature.
22. *(P3, Actor 74)* A serde encoding is a Python codec name (Java's names such as `UTF-16` resolve
    too), and the bytes are Java's: unmappable characters become `?` and malformed input U+FFFD
    (Java's `REPLACE`), and `UTF-16` / `UTF-32` use Java's big-endian order and marks, where
    Python's codecs of those names use the machine's order.
23. *(P3, Actor 74)* `float_*(size=4)` keeps a 32-bit NaN's payload in the top mantissa bits of the
    Python `float` (the hardware widening would set the quiet bit), so Java's
    `floatSerdeShouldPreserveNaNValues` holds; a value beyond the 32-bit range serializes as an
    infinity (Java's `(float)` narrowing). `int_serializer` raises `OverflowError` for an `int`
    beyond its width, which Java's `Integer` / `Long` cannot hold.
24. *(P3, Actor 74)* On the config route, a class that cannot be constructed without arguments, or
    whose instance is not callable, raises `KafkaError` with Java's `Utils.newInstance` /
    `getConfiguredInstance` messages (Java throws `KafkaException` there); the rule names
    `ConfigError` only for an instance or an unresolvable name, which keep it.
25. *(P3, Actor 74)* The built-in serde classes are private (only the factories are public), so the
    config route cannot name a built-in: `key.deserializer=
    "org.apache.kafka.common.serialization.StringDeserializer"`, as a ported Java config has it,
    raises `ConfigError(… "Class … could not be found.")`. No rule maps Java's class names.
26. *(P3, Actor 74)* "The client recognizes a built-in by identity and runs it natively" is client
    work (P4/P5) and needs FFI the header does not have; in P3 every built-in is the Python
    callable, which the clients call.
27. *(P3, Actor 74)* The key types come from Java: `cargo xtask generate-error-codes` also writes
    `confluent_kafka/_config_types.py` from `ProducerConfig` / `ConsumerConfig` (every `define` /
    `defineInternal`, plus `withClientSslSupport()` / `withClientSaslSupport()`), and
    `check-generated` fails when it is stale. No rule names the generator of this table.
28. *(P3, Actor 74)* "Logged once as unused" logs the keys the client's `ConfigDef` does not define
    and no serde's `configure` read (a recording dict, as Java's `RecordingMap`), at INFO on
    `confluent_kafka.common.config`, text `These configurations '[a, b]' were supplied but are not
    used yet.` (keys sorted). Java also logs defined keys its code never reads (SASL keys under
    `PLAINTEXT`); the binding cannot see what the core reads.
29. *(P3, Actor 74)* A `None` value is left out of the core's map, so the key's default applies;
    Java parses an explicit `null` (a validator may reject it; a non-null default is not used).
30. *(P3, Actor 74)* `partitioner.class`: Java's built-in
    `org.apache.kafka.clients.producer.RoundRobinPartitioner` is accepted (the core runs it); any
    other value raises, including the core's own `ConsistentRandomPartitioner` /
    `Murmur2RandomPartitioner` names, which are not Java classes. `interceptor.classes` raises only
    when it names an interceptor: Java's default, the empty list, is accepted.
31. *(P3, Actor 74)* In `ConfigDef`'s messages the value's class is Python's `module.qualname`
    (`builtins.int`) where Java prints `java.lang.Integer`; a Python `int` beyond the key's width
    raises `Not a number of type INT` (Java's `Integer` / `Long` cannot hold it).
32. *(P3, Actor 74)* `configs` that is not a mapping raises `TypeError` (the producer raised
    `IllegalArgumentError`); a key that is not a `str` raises `ConfigError(… "Key must be a
    string.")`, as Java's `Utils.castToStringObjectMap`.
33. *(P3, Actor 74)* Item 5's pattern again: `ConsumerRecords.records(topic=None)` raises
    `java_forms`' `records() takes one of (partition), (topic); got ()`, where Java's
    `records((String) null)` throws `IllegalArgumentException("Topic must be non-null.")`.
- *(Manager, from Critic 74 N7)* The rules' nullability rule would type `CloseOptions.timeout(...)` as
  non-null, but Java's own test calls `timeout(null)`; the binding follows Java and accepts `None`.
34. *(P3, Actor 74)* The Idiom rule ("a class name's `Exception` suffix becomes `Error`, nothing
    else changes") is followed literally, so Java's two suffixless exception classes keep their
    names in Python: `InvalidRegularExpression` and `OffsetMetadataTooLarge`, with no `Error`
    suffix (`confluent_kafka.common.errors`). The Types row "a Kafka exception class → its `…Error`
    class" reads the other way; the Rust core names them `…Error`.
- *(Manager, from Critic 74 R2-N2)* The error generator types a constructor parameter `T | None` when a
  shorter Java constructor passes `null` to it (e.g. `RecordTooLargeError(message,
  record_too_large_partitions)`, `RecordDeserializationError`'s `origin` and buffers). The rules'
  nullability clause instead asks whether Java tests the field for null, which it doesn't here. Kept as
  generated (closest to Java, which itself passes null).
- *(P4, Actor 75)* The async `send(record, callback)` is kept on `AsyncKafkaProducer` /
  `AsyncMockProducer` although the `_async` form the rules derive for it,
  `kafka_producer_Producer_send_with_callback_async`, is not in the header ("an `async def` needs
  the `_async` form" would leave the async class without `callback=`). The Threads rule names the
  async callback's thread (the event loop), Java has the overload, and the binding's batching engine
  runs the callback itself. Listed in `ffi-overload-gaps.md`.
- *(P4, Actor 75)* Both `send` overloads go through the C extension's batching engine, which hands
  records to `kafka_producer_Producer_send_batch` (CLAUDE.md §11 hot path), not through the entry
  points §2/§3 derive per overload (`kafka_producer_Producer_send`, `_send_with_callback`,
  `_send_async`); the derived names are used for the existence check only. Likewise the
  constructors map to `kafka_producer_KafkaProducer_new` (the core names Java's constructor `new`;
  §2 would read `with_configs…`), the serializers running in Python.
- *(P4, Actor 75)* After `close()`, `flush()`, `metrics()` and `partitions_for()` raise
  `IllegalStateError("Cannot perform operation after producer has been closed")`, as the
  Behaviour rule says ("any other call after it raises"); Java's `KafkaProducer.flush()` and
  `metrics()` do not check for a closed producer (the binding has freed the native handle by then).
- *(P4, Actor 75)* A raising `send()` callback on `MockProducer` is logged, as the Threads rule says
  for every producer; Java's `MockProducer.Completion.complete` lets it propagate from `send()` /
  `completeNext()` / `flush()`, and the future then never completes.
- *(P4, Actor 75)* `MockProducer`'s second form, `(autoComplete, partitioner, keySerializer,
  valueSerializer)`, has every parameter required, so its binding stubs type a serializer the call
  leaves at `None` as `None` (binding `bytes`); its stub with both serializers given is subsumed by
  the first form's and is left out (items 14/20). Seven stubs in all.
- *(P4, Actor 75)* The sync producers' futures are started (`set_running_or_notify_cancel`), so
  `cancel()` returns `False`, as Java's `FutureRecordMetadata.cancel()` does; an `asyncio.Future`
  can still be cancelled by its awaiter, and the callback still runs. A callback runs before its
  future completes, as in Java's `ProducerBatch.completeFutureAndFireCallbacks` and
  `MockProducer.Completion.complete`, and `flush()` / `commit_transaction()` return after the
  callbacks of the records sent before them (Java's guarantee; the C poll thread completes records
  after the Rust flush). No rule states these.
- *(P4, Actor 75)* The core reports a bare `KafkaException` (the producer's "Failed to construct
  kafka producer") with the id of `UNKNOWN_SERVER_ERROR`, which the Errors rule's table gives to
  `UnknownServerError`; `from_ffi_error` now raises the base `KafkaError` for it, telling the two
  apart with `kafka_common_Error_is_api_error` (every client).
- *(P4, Actor 75)* Java's behaviour from a delivery callback, which no rule states: `close()` there
  is `close(0)` without a self-join (the poll thread frees the producer as it exits), and `flush()`
  raises `KafkaError("KafkaProducer.flush() invocation inside a callback is not permitted because it
  may lead to deadlock.")`.
- *(P4, Actor 75)* `KafkaProducer`'s constants (`NETWORK_THREAD_PREFIX`,
  `PRODUCER_METRIC_GROUP_NAME`) are on `AsyncKafkaProducer` too; the Class family rule names only
  the peer's constructors and methods.
- *(P4, Actor 75)* The batching engine performs Java's per-send metadata wait on its send thread,
  record by record, so with an unreachable broker an untimed `close()` / `flush()` waits up to
  `max.block.ms` per accumulated record; `close(timeout=…)` is bounded by its timeout since
  `send_batch` no longer holds the producer lock across its sends.
- *(P4, Actor 75)* The native `ProducerRecord` no longer copies the topic or the header keys (DoD
  #10, after the P3 fixup merge): it holds the topic `str` and the headers as a tuple (a tuple
  snapshot when given another sequence, so a later change to the caller's list does not reach the
  record) and points into their cached UTF-8 buffers; header values stay buffer exports. A topic or
  key with an embedded NUL still raises `ValueError`, as before. That is the C struct only: the Rust
  FFI's `send_batch` still builds a `String` topic per record (the core `ProducerRecord`'s topic is a
  `String`; Rust-core gap 9 in `ffi-overload-gaps.md`) and an owned key `String` and value copy per
  header (the core's `RecordHeader` owns both; gap 1).
- *(P4, Actor 75)* `KafkaProducerTest` cases that script a `MockClient` only to make an operation
  fail are translated against an unreachable broker (short `max.block.ms`): the timeout half of
  `testInitTransactionTimeout`, `testOnlyCanExecuteCloseAfterInitTransactionsTimeout`, the callback
  half of `testCallbackAndInterceptorHandleError`, `shouldNotInvokeFlushInCallback`, the serializer
  half of `testHeadersSuccess`; `testNullGroupMetadataInSendOffsets` / `testInvalidGeneration…` use a
  producer without a transaction, as Java checks the metadata first.
- *(P4, Actor 75)* Critic 75 F2: Java's `send()` rethrows the errors of `doSend` that are not
  `ApiException`s (`KafkaProducer.java:1069-1081`), but the batching engine hands a record to the
  Rust producer after `send()` has returned. `send()` therefore waits for its record's handover and
  raises the error the core's `send()` returns only on a transactional producer with no transaction
  started (the binding tracks `begin_transaction()` until the next `commit_transaction()` /
  `abort_transaction()`). There every send fails in Java with the transaction manager's
  `IllegalStateException`, raised with its exact message. Elsewhere the rethrown errors reach the
  callback and the future: a producer in a previous fatal or abortable error state (Java's
  `KafkaException` from `TransactionManager.maybeFailWithError`, which `commit_transaction()` then
  raises), and a send racing a `commit_transaction()` on another thread. Waiting on every
  transactional send was measured at 6.1k against 62.9k records/s (debug build, local broker, 20 000
  records of 100 bytes). The binding keeps the batched speed, which CLAUDE.md §11's hot path argues
  for; full Java fidelity would cost that factor.
- *(P4, Actor 75)* Critic 74 N8: the core's `ProducerConfig` now applies Java's `ConfigDef`
  validators to the keys it parses. Two change behaviour for C and Rust callers too: `acks` must
  match `all`, `-1`, `0` or `1` exactly (`ALL` was accepted), and an empty `transactional.id` is
  rejected ("String must be non-empty") instead of meaning "no transactional id".
- *(P4, Actor 75)* Critic 75 N1: `begin_transaction()` does not drain the batching engine's
  accumulation, as Java's `beginTransaction` does not wait (the drain held the calling thread, and
  on `AsyncKafkaProducer` the event loop, for up to `max.block.ms` per accumulated record). The
  Behaviour rule's §13 line ("async sends that returned before a transaction-control call belong
  to it") says what belongs to a call, not that every call drains, and it holds without the drain:
  with no transaction open, a transactional producer's `send()` returns only once its record is
  with the Rust producer (the F2 item above), so nothing that returned is still accumulated when a
  `begin_transaction()` that can succeed runs; the records of an open transaction stay in it and
  the `commit_transaction()` / `abort_transaction()` ending it drains them; a producer without a
  `transactional.id` fails whatever it has accumulated. The four waiting control ops, `flush()`
  and `close()` still drain. One difference remains, on a misuse: a second `begin_transaction()`
  with a transaction open raises the invalid-transition `IllegalStateError` even when a record of
  that transaction still being handed over would put the producer into an abortable error state,
  where Java, whose `send()` did that before returning, raises that state's `KafkaError`.
- *(Manager, from the spec-update Critic)* The rules' getter/setter clause says a one-argument setter
  "sets and returns the object", which fits only fluent setters. Java's `KafkaMetric.config(MetricConfig)`
  returns `void`, and the binding returns `None`. Suggested wording for the rule: "returns what Java
  returns".
- *(P5, Actor 76)* `MockConsumer` / `AsyncMockConsumer` are a direct Python translation of Java's
  `MockConsumer` (with the `SubscriptionState` and `AutoOffsetResetStrategy` parts it uses), like
  `MockProducer`, although the Implementation-over-the-FFI rule lists `MockConsumer` with the
  FFI-backed classes. The FFI mock cannot keep Java's behaviour: `add_record` carries no decoded
  record, timestamp, headers or leader epoch (Java returns the very records added, and commits the
  leader epoch it recorded), its `ConsumerHandle` rejects a listener's blocking calls, and
  `schedulePollTask(Runnable)` has no entry point (Rust-core gap 14). The mock is thread-safe the
  way Java's `synchronized` mock is (a reentrant lock; another thread waits, it does not raise
  `ConcurrentModificationError`); `wakeup()` only sets its flag.
- *(P5, Actor 76)* `close(timeout=…)` on both consumers calls `kafka_consumer_Consumer_close_with_option(_async)`
  with `DEFAULT`, as Java's `close(Duration)` is `close(CloseOptions.timeout(timeout))`: the derived
  `close_with_timeout` has no `_async` form, and its plain form blocks inside the FFI, where the
  listener's `on_partitions_revoked` cannot run on the caller's thread. The async class keeps
  `close(timeout=…)` although its `_async` entry point is missing (as P4's async `send(callback=)`).
- *(P5, Actor 76)* `subscribe(topics=…, callback=…)` / `subscribe(pattern=…, callback=…)` call this
  branch's `…_caller_thread_listener_async` entry points, not the derived
  `subscribe_with_listener(_async)` / `subscribe_pattern_with_listener_async`, which run the listener
  on the dispatcher thread (the Threads rule).
- *(P5, Actor 76)* A raising `commit_nowait()` callback is logged on `KafkaConsumer` (the FFI cannot
  return it to the delivering call, Rust-core gap 13) and propagates from `MockConsumer` (Java). A
  commit callback is a plain function: an awaitable it returns is closed and logged (the
  `KafkaConsumer`) or rejected with `TypeError` (the mock); rule 3's `async def` covers the
  listener's methods only.
- *(P5, Actor 76)* Item 5's pattern: an explicit `callback=None` reads as not given, so
  `subscribe(pattern=…, callback=None)` is Java's `subscribe(pattern)` (Java's
  `subscribe(pattern, null)` throws "RebalanceListener cannot be null") and
  `commit_nowait(offsets=…, callback=None)` is rejected by `java_forms` (Java's
  `commitAsync(offsets, null)` is accepted). The gRPC servers pass a no-op callback for a
  `CommitAsync` with offsets and no callback.
- *(P5, Actor 76)* A failing deserializer: the core has moved the positions past the whole batch
  before Python deserializes it, so the binding returns the records before the failing one (raising
  when there are none, as Java's `FetchCollector.collectFetch`) and seeks each partition back to its
  first record not returned — one `seek` per such partition. No rule states the mechanism, only the
  outcome ("leaves the position unmoved").
- *(P5, Actor 76)* The listener's `partitions` are `set[TopicPartition]` (the Types rule reads
  `KafkaConsumer`'s implementation, whose invoker builds a `SortedSet`); Java's `MockConsumer` passes
  `List`s, and the Python mock passes sets.
- *(P5, Actor 76)* `KafkaConsumer.group_metadata()` without a `group.id` raises
  `InvalidGroupIdError` (Java's `throwIfGroupIdNotDefined()`), checked by the binding: the core
  returns a stub (Rust-core gap 10). The Configuration rule names only `subscribe`, `commit` and
  `committed`.
- *(P5, Actor 76)* The not-thread-safe rule is applied to `close()` too: while one thread closes a
  `KafkaConsumer`, a call from another raises `ConcurrentModificationError` (Java's `close()` holds
  the consumer's lock), and a `close()` the FFI refuses because another thread is inside the
  consumer leaves it open and raises the same error; `wakeup()` stays callable and is a no-op once
  closed.
- *(P5, Actor 76)* `ConsumerRebalanceListener.on_partitions_lost`'s default returns what
  `on_partitions_revoked` returns (Java's returns `void`), so an `async def` `on_partitions_revoked`
  reached through the default is awaited by the async consumers; "default methods keeping their
  body" does not say what a Python default returns.
- *(P5, Actor 76)* `close()` releases the mock's listener and poll tasks too (Threads and callbacks:
  "`close()` releases every callback the client holds"), where Java's mock keeps them, so a
  `rebalance()` after `close()` (which Java's mock allows) fires no listener.
- *(P5, Actor 76)* `poll(timeout=-x)` on the `KafkaConsumer` raises Java's `Timer` message, `Invalid
  negative timeout N` (N = `Duration.toMillis()`, rounded down), and `close(…)` "The timeout cannot be
  negative."; the mocks accept a negative timeout, as Java's mock never reads it. The Timeouts rule
  says "Java's message", which is per method.
- *(Manager, from Critic 76 N2)* `kafka_consumer_MockConsumer_set_poll_error` / `_set_offsets_error`
  gained a `clear` flag in P5, a second signature change to master's entry point after P2's `(code,
  message)`. It gives C callers Java's `setPollException(null)`; the Python `MockConsumer` is now pure
  Python and doesn't use it. Keep it, or revert to one change.
- *(P5, Actor 76)* `commit_nowait()` can deliver a `ConsumerRebalanceListener` callback, where Java's
  `commitAsync` never does: Java's `AsyncKafkaConsumer.commit(CommitEvent)` only runs
  `offsetCommitCallbackInvoker.executeCallbacks()` and waits for `offsetsReady`
  (`AsyncKafkaConsumer.java:1126-1140`), while the core's `commit_async` waits for the offsets through
  `process_background_events_until`, as `consumer-threading.md` §31 requires of `commit_async`
  ("`process_background_events` is called at the TOP of every public blocking-style API"). A
  listener callback queued then runs on the caller's thread inside `commit_nowait()`, and a listener
  error it raises is raised from `commit_nowait()`, where Java would run the callback, and raise its
  error, in the next `poll()`. The core's §31 behaviour is unchanged (Manager); the binding services
  the queue during `commit_nowait()` (Critic 76 B1; Rust-core gap 19). On `AsyncConsumer` a coroutine
  listener method queued then is awaited in a task on the loop, and the next awaited call (or
  `close()`) waits for it and raises its failure.
- *(Manager, from Critic 76 R3-N1)* The Threads rule exempts only `wakeup()` from the one-thread guard, but
  Java's `AsyncKafkaConsumer.metrics()` doesn't call `acquire()` either (`AsyncKafkaConsumer.java:1288-1290`).
  So `metrics()` from another thread raises `ConcurrentModificationError` in the binding and succeeds in
  Java. Exempting it would also need a guard-free metrics read in the FFI (`kafka_consumer_Consumer_metrics`
  takes the guard, `src/ffi/consumer.rs:5799`).
