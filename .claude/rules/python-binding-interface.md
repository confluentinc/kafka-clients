# Python binding interface rules

Rules for the Python client (`confluent_kafka`) built on the Rust core's C FFI. They supplement
`CLAUDE.md` (translation rules, C FFI conventions, DoD) and the consumer / producer rule files —
when those conflict with a rule here **inside `bindings/python/` and the FFI entry points the
binding needs**, this file wins. They were derived from the User Interface Specification
(`design/current/python-client-interface-spec.md`, §3 design philosophy) and its Design Decisions
(`design/current/python-client-interface-decisions.md`, D1–D28), and from the audit base rules in
`design/current/session-analysis/spec-audit-rules.md` §0. The long-term goal is a rule set complete
enough that the Python client can be generated from the Java client mechanically; every rule here
must therefore be decidable from the Java source without judgement.

Each numbered section is one rule: the rule, **Why**, **How to apply**.

## 1. Sources of truth, in order

1. The Java client, Apache Kafka 4.3.1 (`kafka/clients/src/main/java/org/apache/kafka/clients/…`,
   `…/common/…`; the mocks `MockProducer` / `MockConsumer` are in the SAME main tree; Java's tests —
   `MockConsumerTest`, `KafkaProducerTest`, … — are under `kafka/clients/src/test/java/…`). Java is
   the functional contract.
2. The spec (`python-client-interface-spec.md`) — the surface as ruled by the owner. Where the spec
   and Java differ, the spec wins only where a numbered principle or a rule below names the
   difference; otherwise the spec is wrong and must be flagged (rule 13), not silently followed.
3. The C header generated from `src/ffi/` (`target/include/confluent_kafka.h`) — what the binding
   can call today. Missing FFI is added (rule 10), never worked around in Python.
4. `confluent-kafka-python` (`~/work/confluent-kafka-python`) — only for the differences section;
   never a design input.

The admin client (`python-admin-interface-spec.md`) is **paused**; do not implement it.

## 2. Java's name and Java's type, by default (R1)

Every method, parameter, field, return value and nested type uses Java's name (snake_cased) and
Java's type unless a transform in rule 3 or a principle in rule 4 says otherwise. Dropping a Java
method, flattening a Java object into keyword arguments, changing a return type, splitting one
overload set into two methods, moving a nested type to top level, or inventing a parameter name are
each violations unless a rule names them.

**Why:** the first audit checked the spec against its own text and let `close(group_membership=…)`,
`seek(offset: int | OffsetAndMetadata)`, `records_for_topic`, `RecordMetadata.offset() -> int | None`
through. "Python is the idiom" (principle 1) covers case, keyword-only and raising — not reshaping.

**How to apply:** open the Java file for every class you touch and compare member by member. A
Critic runs this pass (rule 14) for every class in a phase; a difference must be explained by
exactly one transform or one principle.

## 3. The complete list of allowed transforms (R2)

Anything not on this list needs a new rule first (recorded here and in the decisions doc).

1. `camelCase` → `snake_case`; `PascalCase` kept for types; Java package → Python module with the
   `clients` segment dropped (`org.apache.kafka.clients.consumer` → `confluent_kafka.consumer`).
2. **Every parameter of every method, constructor and module-level function is keyword-only**
   (`*,`). Two exemptions only: (a) callables the user writes (serializers, callbacks, listener
   methods) are called positionally; (b) a method of a Java builder whose one argument is the value
   the method is named after takes it positionally — `CloseOptions.timeout(30.0)`,
   `CloseOptions.with_group_membership_operation(op)` (admin, paused, same shape:
   `CreateTopicsOptions().timeout_ms(30000)`).
3. **Each Java overload set → ONE method or constructor** taking the union of the parameters, with
   a default for any parameter some overload omits.
4. **Java's parameter names are always kept.** Same name / different types across overloads →
   ONE parameter with the union of the mapped Python types and one `@typing.overload` stub per type
   (`MockConsumer(*, offset_reset_strategy: str | OffsetResetStrategy)` — no default, Java has no
   no-arg constructor, the enum form is deprecated and kept; `subscribe(*, pattern: SubscriptionPattern)`
   — the client-side `java.util.regex.Pattern` overloads are dropped, principle 11). Distinct Java
   names stay separate parameters (`seek(*, partition, offset=None, offset_and_metadata=None)`).
   **Overlap fallback — NOT yet reviewed by the owner:** if the mapped Python types are not disjoint
   (a `str` is an `Iterable[str]`), split into one parameter per type, Java's name with the Python
   type appended (`topics_str` / `topics_iterable`). The owner asked to review this case explicitly
   before it is applied anywhere; if a site needs it, record it in the clarifications file (rule 13)
   and do not apply it.
5. **Only Java-permitted parameter combinations are accepted** (R3). The implementation checks at
   runtime that the given arguments match exactly one Java overload and raises
   `IllegalArgumentError` naming every alternative. **Mechanism (one shape for every method, so two
   Actors cannot diverge):** a single private helper in `confluent_kafka/_args.py`,
   `exactly_one(method: str, **candidates)` / `at_most_one(method, **candidates)` /
   `all_or_none(method, **group)`, called first thing in the implementation; message forms are
   exactly `"<method>() takes exactly one of <a>, <b>; got <given or 'none'>"`,
   `"<method>() takes at most one of <a>, <b>; got <given>"`, `"<method>() needs all of <a>, <b>,
   <c> together; got <given>"`. Java has no runtime analog (the compiler resolves overloads), so this
   is the one binding-only mechanism; the owner asked to re-discuss it at implementation — record the
   helper's final shape in the clarifications file.
6. **`@typing.overload` stubs** declare the permitted forms to the type checker: one stub per form
   that differs by parameter type; overloads that differ only by an optional trailing parameter
   (listener, `Duration`) fold into one stub via `= None`; a set that differs only that way has no
   stubs. The implementation signature is the keyword-only union of the stubs. A Java getter/setter
   pair sharing one name (`timeoutMs()` / `timeoutMs(Integer)`, `NewTopic.configs`) is one method
   with two stubs: the no-argument form reads, the one-argument form sets and returns `Self`.
   Stubs and implementation must pass mypy / pyright consistency checks (rule 12).
7. `Duration` → `Duration = float | timedelta` (seconds or a `timedelta`); a negative value raises
   `IllegalArgumentError`. `poll(*, timeout: Duration)` is required; every other Java `Duration`
   overload becomes `timeout: Duration | None = None` (`None` = `default.api.timeout.ms`). A Java
   `Integer timeoutMs` stays `timeout_ms: int | None` (milliseconds) — it is not a `Duration`.
8. Collections: `Set` → `set`, `Map` → `dict`, `List` → `list`, `Collection`/`Iterable` input →
   `Iterable`, arrays → `tuple`; `Optional<T>` → `T | None` (a Java `long` with a sentinel plus a
   `hasX()` predicate is NOT an `Optional` — both methods are kept; known sites: `RecordMetadata`
   (`has_offset()`/`offset()`, `has_timestamp()`/`timestamp()`, Java `RecordMetadata.java:61-86`),
   `Node.has_rack()`/`rack()` (`Node.java:105-107`)).
9. Single-method callback interfaces (`Callback`, `OffsetCommitCallback`,
   `AcknowledgementCommitCallback`) → callables with a typed alias (`DeliveryCallback`,
   `CommitCallback`, `AckCommitCallback`); multi-method interfaces (`ConsumerRebalanceListener`)
   stay classes; `Runnable` → `Callable[[], None]`.
10. `…Exception` → `…Error` on CLASS names only; method names keep Java's word
    (`set_poll_exception`, `set_offsets_exception`).
11. `Future<T>` → `concurrent.futures.Future[T]` (sync) / `asyncio.Future[T]` (async);
    `byte[]`/`ByteBuffer` → `bytes` / `memoryview`; `Headers`/`Header` → the `Headers` alias
    (`Sequence[tuple[str, memoryview]]` read, `Iterable[tuple[str, bytes-like | None]]` written).
12. Java public mutable FIELDS (`MockProducer.sendException`) → `set_<field>(*, …)` methods,
    because no bare public attribute exists on this surface (D16); state the reason at the site.
13. **Deprecated Java methods and types are KEPT** and marked with Java's `@Deprecated` note
    (owner: revisit at implementation — list each one you implement in the clarifications file).
14. Java builders (`CloseOptions`, admin `*Options`, `MockAdminClient.Builder`) keep Java's shape:
    private or no-arg constructor as Java has it, static factories, fluent setters returning `Self`,
    getters. Never a keyword constructor in their place.
15. Accessors are METHODS (`tp.topic()`), never `@property` or bare attributes. Value types are
    immutable with value equality; those used as dict keys are hashable.
16. Where Java returns `void` and Python needs a return, return `None`; where Java's Java-7
    sentinel/exception pairing exists (`IllegalStateException` on use after close), raise the same
    JDK analog (`IllegalStateError`).

## 4. The twelve principles (spec §3) — the sanctioned deviations from literal Java

1. Java is the functional contract; Python is the idiom (case, keyword-only, raising).
2. Overload collapse (rule 3.3–3.6).
3. Keyword-only (rule 3.2).
4. Errors are raised as a typed hierarchy, caught by type (rule 5).
5. Sync and async are peers: a method is `async def` on the async class iff it blocks in Java or
   awaits the background task; otherwise a plain `def` on both. `AsyncKafkaProducer.send` is
   `async def` (Java blocks on buffer space) and returns an `asyncio.Future[RecordMetadata]`.
6. `Producer`/`Consumer`/`ShareConsumer` and their `Async*` peers are non-instantiable bases
   (`__init__` guard raising `TypeError` naming `KafkaConsumer` / `MockConsumer`); `Kafka*` adds only
   a constructor; `Mock*` adds constructor + Java's mock helpers.
7. Generics are inferred from the typed serde factories (`json_deserializer() -> Deserializer[Any]`);
   no hand-written type parameters anywhere, including tests and docs.
8. A serde is any callable of the right shape; `configure(configs, is_key)` / `close()` are honoured
   iff defined; built-ins run natively (no per-record Python call).
9. The engine underneath is async: blocking calls are Python waiting on the shared dispatcher
   (`src/ffi/common.rs` `CompletionJob` / `spawn_dispatcher`), interruptible by `wakeup()` and Ctrl+C.
10. Every client is a context manager; producer `__exit__` flushes then closes; closing twice is
    harmless; use after close raises `IllegalStateError`; the real clients expose NO closed
    accessor (only `MockProducer.closed()` / `MockConsumer.closed()`).
11. Regex subscription is broker-side RE2/J only: `subscribe(*, pattern: SubscriptionPattern)`.
12. Java `xAsync` names are not reused: `commit()` = `commitSync` (blocks / awaited),
    `commit_nowait()` = `commitAsync` (plain `def` on both classes, `on_commit=` callback).

## 5. Error model (D1, PR #156)

- One Python class per Java exception class, Java's `extends` chains, `…Error` names, generated as
  **static source plus `.pyi` stubs** into the module mirroring each Java package
  (`confluent_kafka.common.errors`, root for the JDK analogs) by extending the core's
  `cargo xtask generate-error-codes` to read the Java exception sources and cross-check them against
  the FFI `kafka_common_ErrorCode_t` enum. Any Java class without an FFI id, FFI id without a Java
  class, or differing `extends` chain **fails the build**.
- The **current** `cargo xtask generate-error-codes` (xtask/src/main.rs) only copies the 162
  `kafka_common_ErrorCode_t` ids into `bindings/python/_error_code.py` and a Rust mirror. The
  extension required here — parse the Java exception sources (`common/errors/*.java`,
  `clients/consumer/*Exception.java`, `common/config/ConfigException.java`, the JDK analogs), derive
  each class's parent from its `extends`, its abstractness from the Java `abstract` modifier, its
  module from its Java package, its `_ffi_id` from the enum, and emit `.py` + `.pyi` — is new work,
  not a configuration change. `check-generated` must fail on staleness exactly as it does today.
- Java's abstract classes are catch-only bases: constructing one raises `TypeError`. The set is
  derived from the Java `abstract` modifier, never hand-listed; in 4.3.1 it is exactly five:
  `RetriableException`, `RefreshRetriableException`, `InvalidMetadataException`,
  `ApplicationRecoverableException`, `InvalidOffsetException` (consumer package). They get no
  `_ffi_id`.
- No `code()`, no `is_retriable()` / `is_fatal()` / `txn_requires_abort()` on the Python surface. The
  FFI id lives in a private class attribute `_ffi_id`; one table `id → class` is derived from it.
  core → Python: `raise cls(message) from cause`; an unknown id raises the base `KafkaError`.
  Python → core (mock injection): `type(error)._ffi_id`.
- The cause chain is preserved with `raise … from underlying` (`e.__cause__` = Java's `getCause()`),
  including the serde's own exception inside `RecordDeserializationError`.
- JDK analogs at the package root: `IllegalStateError`, `IllegalArgumentError`,
  `ConcurrentModificationError` (subclasses of `RuntimeError`), `confluent_kafka.TimeoutError`
  (subclass of `builtins.TimeoutError`); `ConfigError` is under `KafkaError`.
- Typed payload accessors are methods: `RecordDeserializationError.topic_partition()`, `offset()`,
  `key_buffer()`, `value_buffer()`, `origin()`; `TopicAuthorizationError.unauthorized_topics()`, …
  — exactly the Java getters, backed by the FFI's `kafka_common_Error_<type>` accessors.

## 6. Module layout and class family (spec §4)

```
bindings/python/confluent_kafka/
  __init__.py                 # JDK analogs; Duration; optional re-exports (not decided — do not add)
  producer/                   # Producer, KafkaProducer, MockProducer, AsyncProducer, AsyncKafkaProducer,
                              #   AsyncMockProducer, ProducerRecord, RecordMetadata, DeliveryCallback
  consumer/                   # Consumer, KafkaConsumer, MockConsumer, Async*, ConsumerRecord(s),
                              #   ConsumerRebalanceListener, ConsumerGroupMetadata, OffsetAnd*,
                              #   CloseOptions (+ nested GroupMembershipOperation), SubscriptionPattern,
                              #   OffsetResetStrategy (deprecated, kept), CommitCallback
  common/                     # TopicPartition, TopicIdPartition, Node, PartitionInfo, Uuid, MetricName,
                              #   Metric, KafkaMetric, TimestampType, Headers
  common/errors/              # generated hierarchy (rule 5)
  common/serialization/       # Serializer / Deserializer protocols, built-in factories, Configurable,
                              #   Closable, SerdeBase
```

Async classes live in the same module as their sync peer, `Async`-prefixed. Each Java class is one
Python class; nested Java types are nested Python classes. The C extension stays one module
(`_confluentkafka`) that the package imports; it must not be the public import path.

**Why:** the spec's module table mirrors Java's package tree so the full module path is the
canonical, collision-free name (`confluent_kafka.TimeoutError` vs
`confluent_kafka.common.errors.TimeoutError`).

## 7. Callback and thread contracts

- `on_delivery` runs on the background completion (dispatcher) thread on `KafkaProducer`, on the
  event loop on `AsyncKafkaProducer`; never on the thread that called `send()`. A raising callback is
  logged, not propagated.
- `ConsumerRebalanceListener` methods and `on_commit` run on the **caller's task** inside `poll()` /
  `commit_nowait()` / `unsubscribe()` / `close()` (consumer-threading.md §31); on `AsyncKafkaConsumer`
  listener methods may be `async def` and are awaited, and the rebalance does not proceed until they
  complete. Where the current binding runs them on the dispatcher thread, that is an implementation
  gap to close, not a contract to document.
- An `async def` listener on `AsyncKafkaConsumer` may call back into the consumer (`commit`,
  `assign`, `seek`, `position`, …) through the captured `ConsumerHandle` (consumer-threading.md §41):
  the background loop keeps spinning during the callback, so the canonical
  `await consumer.commit()` inside `on_partitions_revoked` completes. The current FFI adapter's
  single per-handle dispatcher FIFO deadlocks this (D25 gaps 1 and 8); the fix is required, not a
  documented limitation.
- `wakeup()` is sync on both classes and callable from any thread or signal handler. It breaks the
  in-flight blocking call, which raises `WakeupError` (a concrete `common.errors` class, not a JDK
  analog), and the next call proceeds normally — Java's `KafkaConsumer.wakeup()` /
  `WakeupException`, implemented by the core's rotating `CancellationToken`
  (consumer-threading.md §11). Internal background-task wakes (listener-ack pokes) MUST use the
  application-event notify, never the user-facing wakeup token (§31 anti-pattern).
- `KafkaProducer` is thread-safe; `KafkaConsumer` is not — concurrent use raises
  `ConcurrentModificationError`; `wakeup()` is the one method callable from any thread.
- Every exported `*_async` FFI function's rustdoc states, self-contained, which thread its callback
  runs on (cbindgen copies only item-level rustdoc into the header; verify in
  `target/include/confluent_kafka.h`).

## 8. Serialization (spec §5.4)

`Serializer(Protocol[T_contra])` / `Deserializer(Protocol[T_co])` with `__call__(topic, data,
headers=None)`; `data` is a `memoryview` into the fetch buffer on the receive path; `None` passes
through as Java's tombstone. Built-ins: `bytes_*` (defaults), `memoryview_deserializer`,
`string_*(*, encoding="utf_8")`, `int_*(*, size=4)`, `float_*(*, size=8)`, `bool_*`, `uuid_*`,
`json_*`, each typed so `K`/`V` are inferred. Kwarg route accepts callables/instances (a class is
rejected with a redirect error); config route (`key.deserializer` dotted path or class) constructs
and calls `configure(conf, is_key)`; kwarg wins. Execution is eager on the caller's thread; a
failing deserializer makes `poll()` raise `RecordDeserializationError` with the position unmoved.

## 9. Configuration (spec §5.7)

`config: dict` of Java's dotted keys; `str`/`int`/`float`/`bool` coerced per Java's `ConfigDef`;
class-typed keys accept a dotted path or class object; unknown keys are accepted with a warn-on-unused
log (Java `logUnused()`); `group.id` optional (group APIs raise `InvalidGroupIdError` without it);
no `error_cb`, no `logger` parameter (Python `logging` on `confluent_kafka.*`); callbacks are never
config entries. (`config` vs Java's `configs`/`conf` parameter name is an open owner item — keep
`config`.)

## 10. FFI additions the binding needs

- Precedent to mirror: `src/ffi/consumer.rs` (entry points, handles, `_async` variants) and the
  shared async dispatcher in `src/ffi/common.rs` (`CompletionJob`, `spawn_dispatcher`,
  `enqueue_or_run_inline`) — reuse it, never add a second dispatcher.
- If the spec needs an entry point the header lacks, add it to `src/ffi/` following `CLAUDE.md` §3
  and the overload rule in `CLAUDE.md` §2, which decides the FFI shape — quoted, because the Python
  side must match it exactly:
    - overloads that differ **only by presence** of parameters (the `Duration`-trailing methods,
      `send(record)` / `send(record, callback)`, `commit()` / `(offsets)` / `(offsets, Duration)`)
      → **one** FFI method taking optional parameters (Java `Optional` → Rust `Option`, C null /
      sentinel); Python passes absent arguments as null. Never one entry point per presence
      combination.
    - overloads that differ by **type or name-collision** (`seek(tp, long)` / `seek(tp,
      OffsetAndMetadata)`, `subscribe(topics)` / `subscribe(SubscriptionPattern)`, `close(Duration)`
      / `close(CloseOptions)`) → one FFI method per form, named `<base>_<param1>_<param2>` (the
      intersection form keeps the bare name). Python dispatches to the matching entry point by which
      arguments were given, or by `isinstance` for a union parameter.
    - more than three parameters → a single `<base>_options(options)` FFI method taking an
      `Options` struct built through its `OptionsBuilder` (mandatory sets validated in `build()`).
- If the Rust **core** lacks the feature (e.g. `Consumer.client_instance_id`, metric registration),
  the Python method exists with the spec's signature and raises the mapped Java error the core
  would raise (an explicit `Error`, per `CLAUDE.md` §5 — never a silent no-op or a hang), and the
  gap is logged in the clarifications file (rule 13). One sanctioned exception, owner-approved in
  D7: a `timeout=` parameter on an otherwise implemented method whose FFI entry point has no timed
  form is **silently ignored** — but wherever a timed entry point exists (`close_with_timeout`, the
  `Duration` overloads) it MUST be wired, and every remaining unwired `timeout=` is listed in the
  clarifications file.
- No `TODO`/`FIXME`; no new FFI without a C test and a Python test.

## 10a. Decided behavioural contracts with no other home

- `ConsumerRecords(*, records, next_offsets=None)`: the records-only form is Java's constructor
  deprecated in 4.3.1 (KIP-1094 / KAFKA-20660), kept per rule 3.13; an instance built that way
  raises from `next_offsets()` as Java does — it cannot answer it.
- `TopicIdPartition` appears in Java only on the share-consumer surface (`ShareConsumer.commitSync`,
  `AcknowledgementCommitCallback`); with the share consumer out of scope (C1) the type is defined in
  `common/` (two-stub constructor) but referenced by no producer/consumer method.
- `memoryview_deserializer()` hands out views that borrow the fetch batch: one held view pins its
  whole batch; the default `bytes_deserializer()` copies. The retention rule is documented on the
  factory and on `ConsumerRecord.headers()` (header values are `memoryview`s).
- `RecordDeserializationError` leaves the position unmoved, so
  `seek(partition=e.topic_partition(), offset=e.offset() + 1)` skips the poison pill.
- `close()` twice is harmless; use after close raises `IllegalStateError`; the real clients have no
  `closed` accessor.

## 11. Tests (DoD #3, #5, #9)

- Translate Java's tests for every class with a Java test (`KafkaProducerTest`, `MockProducerTest`,
  `MockConsumerTest`, `ConsumerRecordsTest`, `TopicPartitionTest`, …) as pytest, skipping only those
  whose subject is out of scope with the reason stated in the test file.
- Per public method: a positional call raises `TypeError`; each illegal argument combination raises
  `IllegalArgumentError` with the alternatives named; each stub form works.
- `make verify` is `build format-check lint test check-bindings` (root `Makefile:332`), where `test`
  = `test-rust-all-features test-c test-python` and `test-python` runs `bindings/python/Makefile`'s
  `test` (pytest `test/unit`, then `test-integration-python`). A `mypy --strict` run over
  `bindings/python/confluent_kafka` does NOT exist yet — add a `typecheck` target to
  `bindings/python/Makefile`, called from its `test` target, so stub/implementation drift fails
  `make verify`; generics inference is asserted with `reveal_type`-style checks in a typing test
  module. Dev loop: `make devel-build-python` (debug profile, root `venv/`), then
  `cd bindings/python && ../../venv/bin/python -m pytest test/unit`.
- The error hierarchy test asserts, for every generated class, the parent chain against the Java
  source (both directions), and that `_ffi_id` round-trips through the FFI enum.
- `make verify` must pass (Rust, C, Python, format, lint).

## 12. Typing surface

Every public module ships `.pyi`-quality annotations (inline or stub): `@overload` stubs, `Self`,
`Protocol`s, `Generic[K, V]` on the clients and records, typed factories. `from __future__ import
annotations` everywhere; Python ≥ 3.10 (`X | None`). No `Any` on the public surface except where
Java has `Object` (`Metric.metric_value()`, `json_*`).

## 13. Never stop for input — log it

The owner's standing instruction for this implementation: **do not stop to ask**. Do everything that
does not depend on the answer; for what does, choose the most Java-faithful assumption, implement it,
and append an entry to `design/current/implementation-clarifications.md` (what, the assumption taken,
the alternatives, where in the code). Items already known to need the owner: the overlap fallback
(rule 3.4), every deprecated method kept (rule 3.13), the mechanics of combination validation
(rule 3.5), `config` vs `configs`/`conf`, any core capability gap (rule 10), and the parked
improvements list (`design/current/python-interface-improvements.md` — never apply an item from it).

## 14. Actor / Critic protocol for this work

- Actor: one phase at a time; build with `make devel-build-python`, run `make verify`; commit per
  step with a message naming the phase and the rules applied; fix `COMMENTS.<N>.md` before moving
  on, moving fixed items to `COMMENTS.DONE.<N>.md`.
- Critic: for every class the phase touched, run the Java comparison pass (rule 2) member by member;
  then the spec comparison (signatures, defaults, return types, async-ness, stubs, thread contract);
  then CLAUDE.md §3 for new FFI. Report only real findings with the Java line and the rule violated.
  A difference explained by one transform is not a finding.
- Both: never edit this file, `CLAUDE.md`, the spec or the decisions doc; propose changes in the
  clarifications file.
