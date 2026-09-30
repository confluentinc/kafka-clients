# Confluent Kafka Rust

Rust Kafka client implementation translated from the Java Kafka client (client only) with AI assistance. Keeps the same architecture, namespace, and names, adapted to Rust naming conventions.

Any change to this prompt is to be avoided by automatic agents.
Suggestions for changes are possible through the process highlighted in [agent-roles.md](.claude/rules/agent-roles.md).

**The public API in Rust is not stable in versions <1.0, no issue in changing it**

## Translation Rules
1. **Classes outside the repository**: when you find classes outside of the Kafka repository:
    1. In case code with same behaviour and equal or better performance is found in Rust standard library use that one.
    2. In case code with same behaviour and equal or better performance is found in a very popular Rust crate ask if possible to include the new dependency and then use it.
    3. As last resort implement it from scratch by translating it from latest stable OpenJDK source code, ask before doing it and
       keep the same licence: GPL + Classpath Exception (important).
2. **Naming Conventions**:
   - Java package `org.apache.kafka.message` → Rust module `message`
   - Java package `org.apache.kafka.clients.consumer` → Rust module `consumer`. `clients` MUST NOT appear in folder name or Rust module.
   - Java class names (PascalCase) → Rust struct/enum names (PascalCase)
   - Java method names (camelCase) → Rust function names (snake_case)
   - Java const CommonClientConfigs.RETRY_BACKOFF_EXP_BASE → Rust `CommonClientConfigs::RETRY_BACKOFF_EXP_BASE`
   - Each Java class MUST be in its own file, with all its nested classes and enums, but imports for the struct MUST use the parent module re-export, not the file module path. For example, `ProducerRecord` is defined in `producer_record.rs` but imported ONLY as
   `use crate::producer::ProducerRecord;` not `use crate::producer::producer_record::ProducerRecord;`. Only when there are nested classes, or nested enums, they can be available through the submodule, for example: `use crate::producer::producer_record::NestedStaticClassInJava`.
   - Constant MUST be exported only by the struct defining them. E.g.:
     `GROUP_METADATA_TOPIC_NAME` is accessible through
     `::common::internals::Topic::GROUP_METADATA_TOPIC_NAME`
   - Static functions MUST be exported through only by the struct defining them. E.g:
     `to_byte_buffer_accessor` is accessible through `::common::protocol::MessageUtil::to_byte_buffer_accessor`
   - Classes whose package contains `internal` MUST  use only `pub(crate)`
   - Java `Exception` → Rust `Error` (e.g. `TopicAuthorizationException` → `TopicAuthorizationError`)
     - they're all enums of Error
     - each error has its own file
     - they implement `ErrorHierarchy`
     - errors in packages different from `common` should have a prefix corresponding to their package to avoid conflicts like `ConsumerOffsetOutOfRange` for the `OffsetOutOfRange` error in `consumer` module.
     Java errors should all have the `Local` prefix, independently of the subpackage.
     - the word "exception" MUST never appear in Rust code, except in comments about the Java client.
   - Java `throws` / `throw` → Rust `return Err(...)` (e.g. `maybeThrowAnyException` → `maybe_return_any_error`)
   - Preserve original architecture and logical structure
   - Java `long` fields used in comparison (e.g. `Uuid`, producer IDs, offsets) must use `i64` in Rust, not `u64` — signed vs unsigned comparison produces different ordering for values with the high bit set
   - Nullable `string`/`bytes` fields in the Kafka message specs without an explicit `"default": "null"` must default to empty (`Some(String::new())` / `Some(Vec::new())`), not `None`. Only use `None` when the spec explicitly sets `"default": "null"`
   - Java interfaces become traits in Rust, except those translated to std traits or to functions, default method implementation on an interface become a default trait function implementation in Rust
    If a Java class implements multiple interfaces and that have a function in common with same signature, implement the function on the struct and delegate to it in both implementations.
    Constants that should be associated to the trait are exported through the module containing the trait, like nested structs or enums, to make sure they're dyn compatible
   - When generating wire protocol code, always use per-field `flexibleVersions` overrides via `field_flexible_versions(field, msg_flex)` in the generator — never the raw message-level value. Some fields (e.g. `ClientId` in `RequestHeader`) override to `"none"` and must always use length-prefixed encoding
   - Overloaded methods: make sure there's:
     - a method with same name (after translation) that has the intersection of parameters from all overloaded methods, if that method exists in Java.
     - additional methods with <base_name>_with_<param1_name>_<param2_name> (ALWAYS using the "with" keyword in between).
     - when parameters have the same name and different type,
     **in case if the final names would collide**, use the Rust parameter type name instead of the parameter name to discriminate, only for the parameters with same name while continue using the parameter name for rest of parameters.
     In case the difference is **only** Optional use Rust's `Option` and a single method name
     - constructor translation from Java always uses `new`, not `from`. If there are parameters use `.with_<a>_<b>_<c>(a,b,c)`, not `.new_with_<a>_<b>_<c>(a,b,c)`
     - if there a static method `from_<something>` that is translated from Java and a corresponding constructor `with_<something>` with same final signature, keep only the method `with_<something>`
     - if there are more than three parameters in the method name, add a dedicated non-exhaustive `Options` struct that is the only parameter to the method name with `_with_options` suffix. Also in case Java API makes some parameters of the intersection set optional in a later version, add this method with only the `options` parameter
     The `Options` struct must have a `OptionsBuilder` with a `new` parameterless constructor. All optional parameters have fluent setters in the builder. Finally the user calls `build` before passing the `Options` struct, there the different sets of mandatory parameters are validated and a `IllegalArgumentError` error is returned in case they weren't passed. Semantic validation is left to the method where the `Options` is passed.
     - getters and setters with same name: use `<field_name>` for the getter and `set_<field_name>` for the setter. If a method is a setter,
       use `set_<field_name>` even if there's no corresponding getter
     - examples:
       - `fooBar(a, b)`, `fooBar(a, c)` -> `foo_bar_with_b(a, b)`, `foo_bar_with_c(a, c)`
       - `fooBaz(a)`, `fooBaz(a, c)` -> `foo_baz(a)`, `foo_baz_with_c(a, c)`
       - `fooBar(a, b)`, `fooBar(a, c)`, `fooBar(a, b, c, d)`  -> `foo_bar_with_b(a, b)`, `foo_bar_with_c(a, c)`, `foo_bar_with_b_c_d(a, b, c, d)`
        - later: `foo_bar_with_b(a, b, c, d, e)` -> `foo_bar_with_options(options)`, `FooBarOptionsBuilder::new().set_a(a).build()`
        - later: `fooBar(b)`, `fooBar(c)` -> `FooBarOptionsBuilder::new().build()`
3. **C FFI Conventions**:
    - Always define types ending with '_t' for opaque or public structures
    - The crate's base error type `common::Error` -> `kafka_common_Error_t`. Note this
      is NOT Java's `KafkaException`: the handle wraps the whole flat `Error` enum and it allows to map other exceptions that aren't subclasses of `KafkaException`.
      (§10.3). Java's `KafkaException` maps to the embedded `common::KafkaError`
      struct, which never crosses the boundary on its own.
    - the word "exception" MUST never appear in C API and ffi code, except in comments about the Java client.
    - Classes whose package contains the disclamer "This module is not a supported API" MUST NOT have C bindings.
    - Predicates on `Error` keep their Rust name behind the type prefix:
      `is_retriable` -> `kafka_common_Error_is_retriable`, and likewise every
      hierarchy predicate from §10.4, e.g. `is_kafka_error` ->
      `kafka_common_Error_is_kafka_error`. A predicate added on the Rust side is
      expected on the C side too — C cannot see enum variants, so these are the
      only way a C caller can classify an error beyond its numeric code.
    - Exceptions having additional fields in Java: expose the corresponding C opaque type that can be retrieved from the `kafka_common_Error_t` like `kafka_common_Error_resource_not_found`. In case that error is not that type it returns `NULL`, otherwise the returned pointer can be used to access the additional fields with accessor functions. An example of these types and accessors is `kafka_common_ResourceNotFoundError_t` and `kafka_common_ResourceNotFoundError_resource`.
    - preserve Java namespaces in first part of the function name, skipping `clients`:
      - `org.apache.kafka.clients.producer.KafkaProducer` -> `kafka_producer_KafkaProducer_t`
      - `org.apache.kafka.clients.producer.MockProducer` -> `kafka_producer_MockProducer_t`
    - Don't check for failing programming preconditions like NULLs on required parameters
      or parameters not following the function parameters preconditions.
    - For async completion callbacks the typedef is named after the **Java** method whose result it
      carries, plus `_callback` suffix — like `kafka_producer_KafkaProducer_send_callback_t` or
      `kafka_producer_KafkaProducer_send_batch_callback_t` — not after the C entry point, which may be
      one of several overload-collapsed names sharing the typedef (e.g.
      `kafka_consumer_Consumer_commit_async_callback_t` serves both `..._commit_async_with_callback`
      and `..._commit_async_offsets_with_callback`).
    - Multi-shot registration callbacks (a callback set registered once and fired N times, e.g. a
      listener interface) are named `<Type>_<javaMethod>_callback_t` after the Java interface method
      (like `kafka_consumer_ConsumerRebalanceListener_on_partitions_revoked_callback_t`), with the
      registration's release hook named `<Type>_user_data_destroy_t`.


3. **Tests**: Keep the same tests, after translating a class, also translate and run all its corresponding tests.
4. **Comments and documentation**: Keep similar comments as the Java source,
translate javadoc to rustdoc. Never change the contract of public API.
5. **Completeness**: Don't leave any TODO or FIXME — finish everything that should be done. If a Java code path is not yet implemented, fail the affected records/operations with an appropriate `Error` — silently completing or hanging futures is worse than an explicit error.
6. **Scripts**: Use xtask Rust programs instead of shell scripts
7. **License**: All translated code, except GPL with CPE from OpenJDK, includes the Apache 2.0 license header.
    Copyright holder for Apache licensed code is Confluent Inc.
8. **Non-blocking IO**: Use non-blocking IO (Tokio) with a single Selector for multiple TCP connections, as with Java Selector class.
9. **Concurrency**: 
    1. If a method is blocking in Java it should async in Rust
    2. Translate callbacks you find in Java client to code that is executed 
       after awaiting the corresponding call in Rust.
    3. In case the original method isn't blocking to await the callback response (for example awaiting a CompletableFuture), use Tokio `task::spawn` to create a coroutine that is detached from current flow.
    4. When Java uses `thread.join()` or `Future.get()` to block until completion, the Rust translation must actually `.await` the corresponding handle — setting a flag or dropping a channel is not equivalent to joining.
    5. Translating a Java callback to async does not eliminate the callback obligation. If Java guarantees exactly-once callback invocation per record at a specific lifecycle point (e.g. `completeFutureAndFireCallbacks`), the Rust translation must invoke the equivalent at the same point — not defer it or silently drop it.
    6. **Tokio-specific pitfalls** (no Java equivalent — Java threads do not have cancellation semantics):
       - `tokio::select!` cancels the losing branch's future mid-execution. Never put operations with side effects (incrementing a counter, sending on a channel, writing to a buffer) inside a `select!` arm unless the future is cancellation-safe. Use `biased;` when ordering matters.
       - Holding a `MutexGuard` across an `.await` point deadlocks the async runtime — always drop locks before awaiting.
    7. About naming, whenever we're talking about a "thread" in Java let's use the term "task" in Rust. E.g. in log messages.
10. **Error handling**: follow [Rust guidelines](https://doc.rust-lang.org/book/ch09-03-to-panic-or-not-to-panic.html) for error handling.
    1. Avoid `panic` for public API, use it only if there's no way to recover from a particular error, such as an OOM or a
       `ArithmeticException` like division by zero.
    2. Return a `Result` when Java code throws an exception even if unchecked but recoverable.
    3. **The base error type is `Error`** (`common::Error`) — the single type every
       fallible API returns. It is a flat enum with no Java counterpart: Rust cannot
       express Java's exception hierarchy, so one enum holds both `KafkaException`'s
       subclasses and the generic `java.lang` / `java.util` runtime exceptions that
       sit beside it. Java's `KafkaException` base class maps to the `KafkaError`
       struct (error code + optional message), embedded in each specific error
       struct.
    4. Because the hierarchy is flattened, every **intermediate** (non-leaf) class in
       Java's error hierarchy MUST be recoverable as a predicate on `Error`, named
       `is_` + the class name snake_cased with the Exception suffix replaced by
       `Error` (§2). The `_error` suffix is applied uniformly — no exceptions, so
       the name is derivable from the Java class without judgement:

       | Java intermediate class     | predicate                      |
       |-----------------------------|--------------------------------|
       | `KafkaException`            | `is_kafka_error()`             |
       | `ApiException`              | `is_api_error()`               |
       | `RetriableException`        | `is_retriable_error()`         |
       | `RefreshRetriableException` | `is_refresh_retriable_error()` |
       | `InvalidMetadataException`  | `is_invalid_metadata_error()`  |
       | `AuthenticationException`   | `is_authentication_error()`    |
       | `AuthorizationException`    | `is_authorization_error()`     |

       These predicates encode Java's `extends` chain, so the family contains
       ONLY intermediate classes. A Java *static* that classifies an exception is
       NOT a predicate on `Error` and MUST NOT be added to this trait — it is
       translated as a free function in the module matching its Java home, taking
       `&Error`. The one such case is fatality: `RequestUtils.isFatalException`
       becomes `common::requests::request_utils::is_fatal_error(&Error)`, whose
       body mirrors the Java `instanceof` chain (delegating to
       `is_authentication_error` / `is_authorization_error` for the two base
       classes, matching variants for the standalone ones). It is deliberately
       NOT `Error::is_fatal_error`: fatality is not a property of an exception's
       *type* — the same class is fatal in one context and recoverable in another
       (Streams' `RecordCollectorImpl.isFatalException` and
       `TransactionManager`'s `FATAL_ERROR` state use entirely different notions),
       so it does not belong in the `extends`-encoding trait. Java itself keeps it
       a static with a single caller (`AdminMetadataManager`); the translation
       mirrors that shape. Being in an API that is not public 
       `common::requests::request_utils::is_fatal_error` is not exposed in C FFI.

       Leaf classes need no predicate — match the `Error` variant or compare the
       error code instead. Each predicate MUST:
         - document the exact set of variants it covers and cite the Java class it
           translates, because a flattened enum gives the reader no other way to
           see the hierarchy;
         - be covered by a test asserting BOTH directions over every error code
           against the Java `extends` chain (the precedent is `errors.rs`'s
           `test_retriable_errors_match_java_hierarchy`) — a sampled test cannot
           catch a code wrongly added to, or missing from, the set;
         - state its polarity relative to its siblings. These predicates are NOT
           complements of one another: `Serialization` and `Wakeup` are
           `KafkaException`s that are not `ApiException`s, so they answer `true` to
           `is_kafka_error()` and `false` to `is_api_error()`.
11. **Language-related optimizations**: When the memory can be kept on the stack even if Java code creates a new object, keep it on the stack. On hot paths (send path, batch drain, wire framing, per-record processing), also account for costs Java's JIT/GC masks but Rust makes explicit:
    - Identifiers cloned on every message (topic names, client IDs): prefer `Arc<str>` over `String` to make clones cheap
    - A single numeric field shared across tasks: prefer `AtomicI64`/`AtomicU64` over `Mutex<i64>` to avoid lock contention
    - Hot-path async dispatch: avoid `Pin<Box<dyn Future>>` per call — prefer concrete `async fn` return types or generic dispatch
    - Per-message `tokio::spawn` on the send path: avoid — use a shared completion task with a channel instead

    **"Hot path" definition**: per-record / per-message dispatch (send-path record build, batch drain, deserialize/serialize, wire framing). This does **not** include per-RPC or per-batch top-level API surfaces (e.g. the `Producer` / `Consumer` dispatch trait used at `send()` / `poll()` granularity) — there, one `Pin<Box<dyn Future>>` per call is amortized over many records and is negligible. `#[async_trait]` is acceptable for those top-level surfaces.

    Outside hot paths, prefer the simpler type (`String`, `Mutex`) unless profiling shows otherwise.
12. **Parameters and return values of public API**: Accept the most general borrowed form for input parameters. Borrow immutably, and return immutable values.
    Return a borrowed reference in case the data is still owned by the original struct (getter for example).
    When ownership is transferred to the caller prefer returning the struct (making use of RVO) over Box or Rc or Arc.
    Don't copy byte arrays holding the key, value or headers passed to ProduceRecord or received in ConsumeRecord. This zero-copy requirement extends through the entire write path: serialized bytes must be written directly into the batch buffer (no intermediate buffer), batch finalization must not copy already-serialized bytes, and wire sends must use vectored I/O (`IoSlice` / `write_vectored`) so the framing header and payload are sent without assembling a single contiguous buffer. On the receive path, the symmetric rule applies: fetched bytes are owned by one buffer in `CompletedFetch`, every downstream type borrows slices from it, and the `Deserializer<T>` trait takes `&[u8]` (sync, no `#[async_trait]`) — see `consumer-threading.md` §27.
13. **Consumer-specific rules**: see [consumer-threading.md](.claude/rules/consumer-threading.md) for `AsyncKafkaConsumer` API shape, background-task design, `wakeup()` cancellation, `SubscriptionState` ownership, group-protocol scope, receive-path zero-copy, and `ConsumerRebalanceListener` invocation thread. These rules supplement #8/#9/#11/#12 inside the consumer module.

## Python Binding Conventions

The Python client, package `confluent_kafka` in `bindings/python/`, is built only on the C FFI of §3. Its surface and implementation are generated from this section, the Java source, the C FFI header and the previous release's interface; the same inputs give the same result on every run.

- **General rules**
  - Rule 1: Java decides the content — classes, methods, parameters, types, defaults and behaviour. Python decides only the form, through the idiom translations below. A deliberate content change is marked *(deviation)*; anything else needs a new rule. Nothing comes from confluent-kafka-python or readability, and Javadoc is read only for nullability.
  - Rule 2: Java's standard types map to Python's standard types by the fixed table in Types.
  - Rule 3: where Java has nothing, Python's standard convention applies.
- **Idiom translations (rule 1)**
  - Names are snake_case for methods and parameters, PascalCase for types, a nested Java type a nested class (`CloseOptions.GroupMembershipOperation`) (`offsetsForTimes(timestampsToSearch)` -> `offsets_for_times(*, timestamps_to_search)`); a class name's `Exception` suffix becomes `Error`, nothing else changes (`setPollException` -> `set_poll_exception`).
  - A leading `get` is dropped; `is` and `has` stay (`Uuid.getMostSignificantBits()` -> `most_significant_bits()`, `hasRack()` -> `has_rack()`).
  - One Java overload set -> one method (Signatures).
  - `null` and `Optional` -> `None`; Java collections -> Python collections (Types).
  - A Java client interface -> a non-instantiable base class; an interface the client only hands out -> a `typing.Protocol`; a multi-method interface the user implements -> a class to subclass, abstract methods becoming no-ops and `default` methods keeping their body; a single-method callback interface -> a `Callable` alias of the interface's name (`Callback`, `OffsetCommitCallback`, `AcknowledgementCommitCallback`); `Runnable` -> `Callable[[], None]`.
  - `AutoCloseable` / `Closeable` -> a context manager (`__enter__`/`__exit__`, async `__aenter__`/`__aexit__`) whose exit calls `close()`, the producer flushing first.
  - `iterator()` -> `__iter__`, `count()` -> `__len__`; `equals`/`hashCode` -> `__eq__`/`__hash__`, `toString()` -> `__str__`, `Comparable` -> ordering.
  - `Future<T>` -> `concurrent.futures.Future[T]` on a sync class, `asyncio.Future[T]` on an async one (so the async `send` is awaited twice: `md = await (await p.send(record=r))`); `Duration` -> `float | timedelta`; `java.util.UUID` -> `uuid.UUID`.
- **Scope**
  - In scope: the `Producer`, `Consumer` and `ShareConsumer` families and every public type reachable from what is generated (taken, returned or thrown), recursively, plus the `common` types the share consumer reaches (`TopicIdPartition`). Only public classes outside `internals` packages and packages marked "not a supported API", and only their public members (not the package-private `KafkaConsumer.clientId()`). The Admin client is not covered: do not generate it.
  - Not translated: the config classes (`ProducerConfig`, `ConsumerConfig`, `ShareConsumerConfig`, `CommonClientConfigs` — their keys are the keys of `configs`), assignors, interceptors, partitioner implementations.
  - Nothing Java lacks is added except as a *(deviation)* or by rule 3: no `client_id()`, no producer `poll()`.
- **Modules**
  - An error lives in the module of its Java package, `clients` dropped (`BufferExhaustedError` -> `confluent_kafka.producer`, `InvalidRecordError` -> `confluent_kafka.common`, `CorrelationIdMismatchError` -> `confluent_kafka.common.requests`); a Java built-in exception class is at the root.
  - Every other type lives in `confluent_kafka.producer`, `.consumer`, `.common`, `.common.config` or `.common.serialization`, the one matching its Java package, or else the nearest one above it (`common.record.TimestampType`, `common.header.Headers` -> `confluent_kafka.common`); `Duration` is at the root.
  - One snake_case file per Java class in its module package (`consumer/close_options.py`), re-exported by the package `__init__.py` as `from .close_options import CloseOptions as CloseOptions` and listed in `__all__`; users import from the module, never the file. Internals are `_`-prefixed. By rule 3 the root exports only `Duration` and the Java built-in exception classes. `_confluentkafka` (the C extension) is never a public import path.
- **Class family**
  - A Java client interface -> a base class with all its methods; constructing it raises `TypeError("<Base> is a non-instantiable base; use <Kafka class> or <Mock class>")`.
  - `Kafka*` subclasses add only Java's public constructors; `Mock*` subclasses add Java's public constructors and the Java mock's public methods beyond the interface.
  - By rule 3, each of the nine client classes (three bases, three `Kafka*`, three `Mock*`) has an asyncio peer in the same module, `Async` + its name (`AsyncKafkaConsumer`, `AsyncMockShareConsumer`), with the same constructors and methods, except overloads left out because their entry point is missing (Implementation over the FFI).
  - By rule 3, `@Deprecated` -> a `DeprecationWarning` when the method, or a deprecated overload, is called, plus Java's deprecation note in the docstring.
  - On the async peer a method is `async def` iff Java waits in it: its Java implementation (read as for defaults) waits on the background thread (`addAndGet`), on the network or a timer, or on a rebalance listener it runs (`MockConsumer.rebalance`). Every other method is a plain `def` on both classes (`assignment()`, `begin_transaction()`, `commit_nowait()`).
  - *(deviation)* A Java `xSync` / `xAsync` pair -> `x()` / `x_nowait()`: `commitSync` -> `commit()`, `commitAsync` -> `commit_nowait()`. Java's `xAsync` names are never reused.
  - Members keep Java's declaration order (the interface's order in the base, then the Java mock's own methods); a dunder replacing a Java method takes its place, `__enter__`/`__exit__` follow `close`; the async peer uses the sync order.
  - Java getters stay methods (`tp.topic()`), never `@property`. Java static methods -> `@staticmethod`.
  - `public static final` constants -> class attributes of the same name (`RecordMetadata.UNKNOWN_PARTITION`, `ConsumerRecords.EMPTY`, `ConsumerRecord.NO_TIMESTAMP`, `DisconnectError.INSTANCE`, `Uuid.ZERO_UUID`), a constant collection as a `frozenset` (`Uuid.RESERVED`). Apart from nested types, enum members and constants there are no public attributes.
  - A public mutable field -> a setter `set_<field>(*, <field>)` and nothing else *(deviation)*: `MockProducer.sendException` -> `set_send_exception(*, send_exception)`.
  - Java type parameters -> `Generic` with Java's letters (`KafkaConsumer[K, V]`, `ConsumerRecord[K, V]`); other types are not generic. The records (`ProducerRecord`, `ConsumerRecord`) are covariant in `K` and `V`.
  - A Java enum -> `enum.IntEnum` valued by the constant's numeric `id` field when it has one (`TimestampType.CREATE_TIME = 0`), else `enum.Enum` valued by the constant's name (`LEAVE_GROUP = "LEAVE_GROUP"`); members in Java order. Its other public fields are reached through Java's own methods (`toString` -> `__str__`, `forName` -> `for_name`), not as attributes.
  - All constructors private -> `__init__` raises `TypeError` naming the static factories (`CloseOptions`). Value types and records are immutable.
  - Dropped *(deviation)*: any method whose `AsyncKafkaConsumer` body only logs that it is unsupported (`enforceRebalance`), and any mock helper that only reads, sets or clears state that exists only for a dropped or not-generated method (`MockConsumer.should_rebalance()`, `reset_should_rebalance()`).
- **Signatures**
  - *(deviation)* Every parameter of every method, constructor and module function is keyword-only (`*,`), dunders aside; a positional call is a `TypeError`. Exemptions: callables the user writes (serdes, callbacks, listener methods, and the `Partitioner` and `Cluster` a mock is given) are called positionally; in a class with a fluent setter (an instance method returning its own class), every one-argument method takes it positionally (`CloseOptions.timeout(30.0)`, `.with_group_membership_operation(op)`).
  - A parameter is named by the Java **interface** declaration when the method is declared on one (`Consumer.subscribe(topics, callback)` -> `callback`, although the implementations say `listener`), else by the class's own declaration; a type never renames it: `errorNext(RuntimeException e)` -> `error_next(*, e)`, `Uuid.fromString(String str)` -> `from_string(*, str)`.
  - One Java overload set -> one Python method (or `__init__`) taking the union of the overloads' parameters by Java name, never one method per overload: `seek(tp, long offset)`, `seek(tp, OffsetAndMetadata offsetAndMetadata)` -> `seek(*, partition, offset=None, offset_and_metadata=None)`.
  - Order: each overload keeps its own order; where that leaves a choice, the parameter of the earliest-declared overload goes first. `subscribe(topics)`, `(topics, callback)`, `(pattern, callback)`, `(pattern)` -> `topics, pattern, callback`; `TopicIdPartition(topicId, topicPartition)`, `(topicId, partition, topic)` -> `topic_id, topic_partition, partition, topic`.
  - Same position, Java types mapping to the same Python type -> one parameter named after the earliest-declared overload (`KafkaConsumer(Map configs)`, `(Properties properties)` -> `configs`); an error's message is never merged this way.
  - A parameter in every overload is required. Otherwise its default is **Java-given** when a shorter overload gives it a value — the argument it passes calling a longer public overload of the set, or the constant it assigns to that parameter's field — read in the declaring class or, for an interface method, in the `Kafka*` class following `delegate` forwards (`KafkaConsumer`'s delegate is `AsyncKafkaConsumer`, `KafkaShareConsumer`'s `ShareConsumerImpl`): `null`, `Optional.empty()` -> `None`; a literal, enum constant or `static final` constant -> its value; an empty array, list or set (`new Node[0]`, `Collections.emptySet()`) -> `()`; an empty map -> `None`, read as empty. So `OffsetAndMetadata(offset)` -> `metadata=""`, `Node(id, host, port, rack)` -> `is_fenced=False`, `acknowledge(record)` -> `type=AcknowledgeType.ACCEPT`.
  - A header parameter defaults to `()`. Any other omitted parameter — including one Java fills with a call or a computed value (`Cluster.empty()`) — defaults to `None`, meaning "not given", and every parameter defaulting to `None` is `T | None`.
  - A serializer / deserializer parameter defaults to `None`: then the config key is used if set, else `bytes_serializer()` / `bytes_deserializer()` *(deviation: Java requires one)*.
  - A form is an overload plus every overload equal to it minus trailing parameters, each folding into the earliest-declared such overload (`close()` folds into `close(Duration)`); inside a form a parameter is optional only if an overload of that form omits it.
  - `@typing.overload` stubs, one per form, exist iff the union signature accepts a combination that matches no Java overload (below), or one name carries several types, or a type variable needs binding (below); so `subscribe`, `seek`, `close`, `acknowledge` have stubs and `commit`, `Node` have none. Stubs follow the declaration order of each form's first overload, forms made only of `@Deprecated` overloads last. The implementation signature is the keyword-only union.
  - Binding, so that a construction needs no written type parameters under `mypy --strict`:
    - An omitted serde binds its type variable to `bytes`; a serde given only through the config route leaves the client typed as if it were omitted (`[bytes, bytes]`), so pass serdes as arguments when typing matters.
    - A record key or value that is omitted or `None` binds its type variable to `Never` (`typing_extensions.Never` on Python 3.10).
    - Each constructor gets one stub per combination of those parameters, annotating `self` with the bound types. In a stub a given serde, key or value has its plain type and no default; a `None` key or value is typed `None`, defaulting to `None` only where the parameter is optional (`ProducerRecord`'s key).
    - Stub order: for each Java-form stub in its order, the given sets from none, then each parameter alone in parameter order, then pairs, up to all. `KafkaConsumer` -> `(self: KafkaConsumer[bytes, bytes], *, configs)`, `(self: KafkaConsumer[K, bytes], *, configs, key_deserializer: Deserializer[K])`, `(self: KafkaConsumer[bytes, V], *, configs, value_deserializer: Deserializer[V])`, `(self, *, configs, key_deserializer: Deserializer[K], value_deserializer: Deserializer[V])`.
    - A class with nothing to bind from gets no binding stub, and the caller annotates it as Java writes the type arguments: `c: MockConsumer[str, str] = MockConsumer(offset_reset_strategy="earliest")`, like `new MockConsumer<String, String>("earliest")`. Otherwise no code, test or doc writes type parameters.
  - One Java name with several types -> one parameter typed as their union (in stub order), one stub per type, dispatched by `isinstance`: `MockConsumer(@Deprecated OffsetResetStrategy offsetResetStrategy)`, `(String offsetResetStrategy)` -> `offset_reset_strategy: str | OffsetResetStrategy`.
  - *(deviation)* Types `isinstance` cannot tell apart (a `str` is an `Iterable[str]`) -> one parameter per type, `<name>_<type>` (`topics_str`, `topics_iterable`), checked by `java_forms` like any other names; when the second type arrives in a later Java release, the existing parameter keeps its plain name and only the new one is suffixed. When one parameter's Java types map to the same Python type (`int`, `long`), generation stops with an error at that site.
  - `java_forms` works on Java's overloads, unfolded: each overload's parameter set, in Java declaration order. A set of given names matches an overload O when every name it contains is one of O's parameters and every parameter of O it leaves out gets a value that a shorter overload passes to O itself (a `null`, a constant or an empty collection, as above). Only such values are O's Java-given defaults; a default some other overload receives does not count. When several overloads match, the one with the most parameters is used (the earliest-declared on a tie), and `java_forms` fills exactly its Java-given defaults before the body runs. So `acknowledge(topic=…, partition=…, offset=…)` without `type` is rejected: Java has no such overload, and nothing passes `type` to `acknowledge(topic, partition, offset, type)`.
  - "Given" means "not left at its default". A parameter that one form requires and another defaults has the private sentinel `_args.UNSET` as its implementation-signature default, which tells "not given" from a given value equal to a default (`acknowledge(type=…)`); the stubs show the Java value.
  - A method whose union accepts a set of given names matching no overload gets the one decorator `java_forms` from `confluent_kafka/_args.py`, listing its overloads and their Java-given defaults. It matches the given names against the overloads, raises `IllegalArgumentError(message="<m>() takes one of (<a>, <b>), (<c>); got (<given>)")` listing every overload when none matches (`<m>` the method name, or the class name for a constructor; names in parameter order; `close` -> `"close() takes one of (), (timeout), (option); got (timeout, option)"`), then fills the Java-given defaults and runs the body. It builds its check once, when the class is defined. A method whose every combination matches an overload has no decorator.
  - A getter and a setter sharing one name (`timeoutMs()`, `timeoutMs(Integer)`) -> one method, two stubs: no argument reads, one argument sets and returns the object. A static factory and an instance getter sharing one name (`CloseOptions.timeout(Duration)`, `timeout()`) -> one attribute: on the class the factory, on an instance the getter.
  - Timeouts: a `Duration` present in every overload is required (`poll(*, timeout: Duration)`); one some overload omits is `timeout: Duration | None = None`, `None` calling the overload without it. A negative duration raises `IllegalArgumentError` with Java's message. A Java `Integer timeoutMs` stays `timeout_ms: int | None` (milliseconds).
  - Java `void` -> `None`.
  - Nullability: a required parameter is `T | None` when it is a record key or value (`K`, `V`), or when Java gives null a meaning for it: the method's own body (read as for defaults; not its callees) passes it to `Optional.ofNullable` or has a non-throwing null branch for it; it is stored in a field that holds `null` until this method sets it and that Java tests for null; or its Javadoc, or its getter's, allows null. A Javadoc "Non-null" wins.
  - A return is `T | None` for `Optional<T>`, for a field that can hold null by the rule above, or when its Javadoc says it may be null. An argument handed to a user callback is `T | None` when the Java code invoking the callback can pass null for it (the commit callback's offsets on failure).
  - For an interface method the Javadoc read is the interface's, or, when that only points elsewhere (`@see`, `See {@link …}`), the one it points to.
- **Types** (rule 2; input = passed by the caller; output = returned or handed to user code)

  | Java | Python |
  |---|---|
  | `boolean`; `byte`, `short`, `int`, `long`; `float`, `double` (and boxed) | `bool`; `int`; `float` |
  | `String` | `str` |
  | `Object`, a method's own type parameter | `Any` — the only `Any` on the surface besides `json_*` |
  | `byte[]`, `ByteBuffer` | `bytes` (input), `memoryview` (fetched data) |
  | `Optional<T>`, `OptionalInt`, `OptionalLong` | `T \| None` |
  | a `long` with a sentinel plus `hasX()` (`RecordMetadata.offset()`, `hasOffset()`) | both methods kept, `int` |
  | input `Collection<T>`, `Set<T>`, `Iterable<T>`; `List<T>`; `Map<K, V>` | `Iterable[T]`; `Sequence[T]`; `Mapping[K, V]` (but `configs: dict[str, Any]`) |
  | output `Set<T>`; `List<T>`; `Map<K, V>` | `set[T]`; `list[T]`; `dict[K, V]` |
  | output `Collection<T>`, `Iterable<T>` | `set[T]` if the Java implementation builds a `Set`, else `list[T]` |
  | `T[]` | `tuple[T, ...]` |
  | `? extends T`, `? super T` | `T` |
  | `java.time.Duration` | input `Duration = float \| timedelta` (seconds); output `float` seconds |
  | `java.util.UUID` | `uuid.UUID` |
  | `org.apache.kafka.common.Uuid` | `confluent_kafka.common.Uuid` |
  | `Headers`, `Iterable<Header>` *(deviation: no class)* | input `Iterable[tuple[str, bytes \| bytearray \| memoryview \| None]]`, written in full; output the alias `Headers = Sequence[tuple[str, memoryview \| None]]` |
  | `Throwable`; `Exception`; `RuntimeException` | `BaseException`; `Exception`; `RuntimeError` |
  | a Kafka exception class | its `…Error` class; `KafkaError` subclasses `RuntimeError`, as `KafkaException` extends `RuntimeException` |
  | a Java built-in exception the translated code throws (`IllegalStateException`, `IllegalArgumentException`, `ConcurrentModificationException`, `TimeoutException`, `NoSuchElementException`, `NullPointerException`) | a root class of Java's name with `Error`, subclassing `RuntimeError` (`TimeoutError` subclasses `builtins.TimeoutError`) |
  | `UnsupportedOperationException` | `UnsupportedVersionError` |
  | `Objects.requireNonNull(x, msg)` | raises `NullPointerError(message=msg)` |
  | `java.io.Closeable` of a serde; `Metric`; `KafkaMetric` | the `Closeable` protocol; a `typing.Protocol`; a class |
  | `java.util.regex.Pattern` | dropped with the overloads taking it *(deviation)* |
  | `Partitioner`, `Cluster`, `MetricConfig`, `Measurable` | placeholder aliases of `object` of the same name in their module *(deviation)*; a mock uses them as Java's mock does (Implementation over the FFI) |

- **Errors**
  - One class per Java exception class the FFI enum `kafka_common_ErrorCode_t` names, plus Java's abstract bases and the Java built-in exceptions of Types, with Java's `extends` chain; the base is `KafkaError(RuntimeError)` (`KafkaException`).
  - `cargo xtask generate-error-codes` emits them as `.py` plus `.pyi` from the Java exception sources, cross-checked against the FFI enum: a Kafka class without an id, an id without a class or a different `extends` chain fails the build (a Java built-in exception has an id only where the enum has one: `NoSuchElementError` and `NullPointerError` have none); `check-generated` fails on stale output.
  - A Java `abstract` exception is a catch-only base: constructing it raises `TypeError`; it has no FFI id.
  - Constructors collapse Java's by Signatures, with these changes:
    - In each constructor, the `String` passed unchanged to `super(...)` as the message, directly or through `this(...)`, is the keyword `message`, whatever Java calls it.
    - A `String` with the message's Java name in another constructor is the message too (`ConfigException(name, value, message)`); every other `String` keeps its Java name: `ResourceNotFoundException(message)`, `(message, cause)`, `(resource, message)`, `(resource, message, cause)` -> `ResourceNotFoundError(*, message: str, resource: str | None = None, cause: BaseException | None = None)`.
    - Every `Throwable` parameter, whatever Java calls it (`cause`, `t`, `throwable`), is the keyword `cause: BaseException | None = None`, stored as `__cause__`.
    - Java's `getCause()` is `__cause__`; for an error from the core it follows the FFI chain `kafka_common_Error_source`, including a serde's or user callback's own exception.
    - `str(e)` is Java's `getMessage()`: the string the Java constructor passes to `super(...)` (`TopicAuthorizationError(unauthorized_topics={"t"})` -> `"Not authorized to access topics: [t]"`), `""` when that is `null`, and for Java's `Throwable(Throwable)` `cause.toString()`: `"<module>.<class>"`, plus `": <message>"` when the cause has one.
    - A call giving none of the parameters is matched like any other: `KafkaError()` is Java's `KafkaException()`, while `TopicAuthorizationError()`, `ConfigError()` and `RetriableCommitFailedError()` are rejected by `java_forms`, since Java has no no-argument constructor for them.
    - `KafkaError` defines one `__reduce__` that rebuilds any error from its constructor arguments by name, so `copy` and `pickle` work.
  - Java's payload getters are methods (`RecordDeserializationError.topic_partition()`, `offset()`, `key_buffer()`, `value_buffer()`, `origin()`; `TopicAuthorizationError.unauthorized_topics()`). They return what the constructor was given, or the Java-given default `java_forms` filled for the matched overload (`TopicAuthorizationError(message="x").unauthorized_topics()` -> `set()`, since Java's `(String message)` passes `emptySet()`); an error from the core is constructed with the payload read from the FFI accessors of §3.
  - No `code()` and no `is_*` predicates: the type is the predicate (`except RetriableError`). The FFI id is the private class attribute `_ffi_id` with one `id → class` table. Core -> Python: construct the class of the id, chained from its cause; an unknown id raises `KafkaError`. Python -> core: `type(error)._ffi_id`; the base `KafkaError` carries the core's id for a bare `KafkaException`, `UNKNOWN_SERVER_ERROR` (−1), which `UnknownServerError` owns in the table.
  - An error the user injects into a mock (`set_poll_exception`, `set_send_exception`, `error_next`, …) is raised again as the very same instance; where the mock runs in the core, the binding keeps the instance per injection slot (the setter whose call then fails, not the id) and raises it when the core reports that error, the `_ffi_id` only choosing what the core reports.
- **Serialization**
  - *(deviation)* A serde is any callable. `Serializer(Protocol[T_contra])`: `__call__(topic: str, value: T_contra | None, headers: Headers | None = None) -> bytes | None`; `Deserializer(Protocol[T_co])`: `__call__(topic: str, data: memoryview | None, headers: Headers | None = None) -> T_co | None`. Headers are always passed; `None` in is Java's null and the serde decides what it maps to.
  - *(deviation)* Lifecycle is duck-typed: `configure(configs, is_key)` runs once after construction on the config route only; `close()` runs at client close, its exceptions logged, never raised; an absent method is a no-op. `Configurable` / `Closeable` are `@runtime_checkable` protocols for them, and `SerdeBase` is a no-op base with both.
  - *(deviation)* Built-in factories in `confluent_kafka.common.serialization`, matching Java's serdes byte for byte: `bytes_serializer()`, `bytes_deserializer()` (an owned copy, made lazily), `memoryview_deserializer()` (views into the fetch batch), `string_serializer(*, encoding="utf_8")`, `string_deserializer(*, encoding="utf_8")`, `int_serializer(*, size=4)`, `int_deserializer(*, size=4)` (4 or 8: `Integer`, `Long`), `float_serializer(*, size=8)`, `float_deserializer(*, size=8)` (8 or 4: `Double` canonicalizing NaN like `doubleToLongBits`, `Float` keeping raw bits), `bool_serializer()`, `bool_deserializer()`, `uuid_serializer()`, `uuid_deserializer()` (`uuid.UUID` in Java's dashed form), `json_serializer()`, `json_deserializer()`. No others; another size raises `IllegalArgumentError`.
  - Each factory is typed (`string_deserializer() -> Deserializer[str]`, `json_deserializer() -> Deserializer[Any]`) and returns a real Python callable; the client recognizes a built-in by identity and runs it natively with identical results.
  - Supply by constructor argument (any callable or instance; a class raises `IllegalArgumentError(message="<key>: pass an instance, not the class — did you forget '()'?")`) or by config key `key.serializer`, `value.deserializer`, … (a dotted path or a class: resolved, no-arg constructed, then `configure(configs, is_key)`; an instance or an unresolvable name raises `ConfigError` at construction). A given argument wins and the config key is ignored.
  - Every record goes through a deserializer; there is no raw path. Serdes run eagerly on the caller's thread inside `send()` / `poll()` and raise from that call. A failing deserializer makes `poll()` raise `RecordDeserializationError` and leaves the position unmoved: `seek(partition=e.topic_partition(), offset=e.offset() + 1)` skips the record.
- **Configuration**
  - The constructor parameter is `configs: dict[str, Any]` of Java's dotted keys. `str`, `int`, `float` and `bool` values are coerced to the key's `ConfigDef` type (`"true"` equals `True`); a bad value raises `ConfigError` with Java's message. A class-typed key takes a dotted path or a class.
  - Unknown keys are accepted and logged once as unused (Java's `logUnused()`). `interceptor.classes` and a set `partitioner.class` raise `ConfigError`: the core has no interceptors or custom partitioners. The share consumer rejects the keys Java's `ShareConsumerConfig` rejects.
  - `group.id` is optional; `subscribe`, `commit` and `committed` raise `InvalidGroupIdError` without it.
  - No config value is a callback and no constructor takes an error callback or a logger: operation errors raise or fail their future; everything else is a record on the `confluent_kafka.*` Python loggers.
- **Threads and callbacks**
  - The `send()` callback runs on a background completion thread (`KafkaProducer`) or the event loop (`AsyncKafkaProducer`), never on the caller's thread; a producer's callbacks run one at a time in completion order; a raising callback is logged, not propagated.
  - `ConsumerRebalanceListener` methods, the `commit_nowait()` callback and the share consumer's acknowledgement-commit callback run on the caller's thread, inside the waiting call that delivers them (`poll`, `commit`, `unsubscribe`, `close`, …), and the rebalance does not advance until the listener returns (`consumer-threading.md` §31). By rule 3, on the async consumer a listener method may be `async def` and is awaited on the event loop.
  - A listener may call back into its consumer (`commit`, `seek`, `assign`, `position`, …) through the core's `ConsumerHandle` (`consumer-threading.md` §31); the call succeeds while the outer call waits, on the mocks too.
  - `wakeup()` is a plain `def` on both classes, callable from any thread or signal handler: the in-flight or next waiting call raises `WakeupError`, and the call after proceeds normally (`consumer-threading.md` §11).
  - Ctrl+C during any client's waiting call re-raises `KeyboardInterrupt`, a consumer first waking itself and letting the call end; cancelling the task awaiting an async call does the same with `CancelledError`.
  - `KafkaProducer` is thread-safe. `KafkaConsumer` and `KafkaShareConsumer` are not: a call while another thread is inside the instance raises `ConcurrentModificationError`, `wakeup()` excepted. The async classes follow the same rule per event loop.
  - `close()` releases every callback the client holds.
- **Implementation over the FFI**
  - The client classes, their async peers, `MockConsumer` and `MockShareConsumer` call the FFI. Every other class — value types, records, enums, errors, `CloseOptions`, the serdes and `MockProducer` — is a direct Python translation of its Java source (`MockProducer` because Java's sends complete in the caller, which the FFI's completion dispatcher cannot give).
  - Python adds no FFI entry points. Each Java overload of an FFI-backed class calls the entry point whose name §2 (Rust name) and §3 (C prefix) derive for it; overloads §2 folds into one Rust method with `Option` parameters share one, and Python passes absent arguments as null. Generation looks up only that derived name.
  - An overload whose derived entry-point name is not in the header is not generated — a parameter, a method, or a whole class or family when all its overloads are missing — and `java_forms` and the stubs leave it out. A mock does not generate what its base class does not.
  - The Python method calls the entry point of the overload the given arguments select (by which are given, or `isinstance` for a union-typed parameter). A waiting call uses the entry point's `_async` completion form when there is one, else its plain form; an `async def` needs the `_async` form. Each `_async` function's rustdoc states, self-contained, the thread its callback runs on.
  - Within what is generated, a mock implements every method and constructor argument Java's mock implements: `MockProducer(cluster=…, partitioner=…)` uses the cluster for `partitions_for()` and the partitioner to choose partitions, calling their Java methods by snake_case name. A mock method raises only where Java's mock throws.
  - A blocking call waits on the shared completion dispatcher of `src/ffi/common.rs` (`CompletionJob`, `spawn_dispatcher`, `enqueue_or_run_inline`) — never a second dispatcher — and while waiting drains the caller-thread callback queue, running those callbacks on the waiting thread. An async call awaits an `asyncio.Future` completed through `loop.call_soon_threadsafe` and drains the same queue on the loop.
- **Behaviour with no other home**
  - `ConsumerRecords(*, records)` (Java's deprecated records-only constructor): `next_offsets()` logs a rate-limited error and returns `{}`.
  - Header values handed out are `memoryview`s (into the fetch batch for fetched records) or `None`; `headers()` is `()` when empty, never `None`. §12's no-copy rule covers header values too.
  - `ProducerRecord(value=None)` is sent as a null value (a tombstone), never as `b""`.
  - `close()` twice is harmless; any other call after it raises `IllegalStateError`, as in Java.
  - Async sends that returned before a transaction-control call belong to it (`producer-transactions.md` §13).
- **Tests and typing**
  - Translate Java's tests of every translated class (`KafkaProducerTest`, `MockConsumerTest`, `ConsumerRecordsTest`, …) to pytest under `bindings/python/test/unit`, asserting Java's messages; skip only a test whose subject is out of scope or not generated, with the reason in the test file.
  - For every public method: a positional call raises `TypeError`, every illegal combination raises `IllegalArgumentError` with the exact message, every stub works.
  - The error-hierarchy test checks each class's parent chain against the Java source in both directions and each `_ffi_id` against the FFI enum. Built-in serdes pass Java's test vectors both natively and as Python callables.
  - Full annotations (`from __future__ import annotations`, Python ≥ 3.10) and `.pyi` stubs for generated modules; `mypy --strict` over the package runs in `make verify`, with a typing test asserting inference through `assert_type`.
- **Stability**
  - The interface check compares the generated public signatures (names, parameters, defaults, types, stubs; member order aside) with the previous release's.
  - The client moves from development to preview releases to GA. During development, a new or changed rule may change existing interfaces from one PR to the next. In preview releases an interface may change even in a minor release, and the interface check only warns. After GA, a change to an existing interface fails the interface check in a minor release and warns in a major release.
  - A rule change that alters an existing interface says so; otherwise a new Java overload only adds optional keyword parameters or widens a union, and nothing existing is renamed or removed silently. An overload left out for a missing FFI entry point is added when the entry point arrives, as new optional keyword parameters or a new method — an additive, non-breaking change.
  - A method or overload Java deprecates is kept and deprecated as in Class family; a method Java removes is kept, deprecated, until the next major version.

## Agent Role

Follow the role assigned to you as described in [agent-roles.md](.claude/rules/agent-roles.md).
Use LSP plugins when available and working, notify when it's not working, avoid grepping when unnecessary.

## Source Reference
Java source in `kafka/` directory (Apache Kafka 4.3.1)

## Development Workflow
- **Build**: `cargo build`
- **Test**: `cargo test`, run the timeout tool with timeout 10s by default when checking if single tests are timing out.
- **Format**: `cargo xtask format`
- **Format Check**: `cargo xtask format-check` (CI-friendly)
- **Lint**: `cargo xtask lint` (runs clippy with warnings as errors)
- **Lint Fix**: `cargo xtask lint-fix` (automatically fix clippy warnings)
- **Check Generated**: `cargo xtask check-generated` (validates generated code formatting only)
- **Coverage (unit)**: `cargo xtask coverage` (report at coverage/html/index.html)
- **Coverage (lcov)**: `cargo xtask coverage-lcov` (writes coverage/lcov.info)
- **Coverage (all tests)**: `cargo xtask coverage-all` (requires Docker)

## Definition of Done
Follow the DoD described in [definition-of-done.md](.claude/rules/definition-of-done.md).

## Compact Instructions
Auto compact when reaching 60% of the maximum context and continue the running task

## Permissions
Read from `.claude/settings.local.json` the auto approved commands and always use those
unless it's not possible.