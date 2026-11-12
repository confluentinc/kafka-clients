# Confluent Kafka C++

The C++ binding of the Rust Kafka client, package `confluent::kafka` under `cpp/`. It is a
header-only C++20 library over the C FFI header `confluent_kafka_bindings.h`, and its surface is
Java's `org.apache.kafka.clients` API (Apache Kafka 4.4.0, the `kafka/` submodule) in C++ form.

These rules are the C++ counterpart of the root `CLAUDE.md`'s Python section. Working under
`cpp/` loads the root `CLAUDE.md` (the core, its C FFI in §4, errors in §12) and then this
file; where the two disagree inside `cpp/`, this file wins. Deep dives, when one is needed, go
to `cpp/.claude/rules/<topic>.md` and are linked from here; plans, status and the
contract-change ledger live in `cpp/design/current/`, finished milestones in
`cpp/design/history/`. The questionnaire the rules come from is
`design/current/new-language-binding-questions.md`; its answers for C++ are the Confluence page
"C++ binding: answers to the kickstart questions" (space CLIENTS, 6364529292).

Layout:

```
cpp/
  CLAUDE.md                          this file
  README.md                          toolchain floor, build, test
  CMakeLists.txt, cmake/             INTERFACE target ConfluentKafka::cpp, package config
  Makefile                           build, test, format, format-check, lint, clean
  .clang-format, .clang-tidy         §10
  include/confluent/kafka/           the library: one header per Java class (§3)
    detail/                          hand-written boundary code, not public (§1)
  test/unit/                         GoogleTest, one executable per file (§11)
  test/probe/                        the ABI probe shared library (§12)
  tools/                             checks run by CTest and the Makefile
  design/current/, design/history/   plans, status, doc-contract-changes.md
```

A rule tagged *(deviation)* changes Java's content on purpose and says **Why** and, when it is
temporary, **Goes when**. Anything else that departs from Java needs a new rule, not a comment.

## 1. Premises

1. **Java decides the content, C++ the form.** Classes, methods, parameters, types, defaults,
   behaviour and messages are Java's; the idiom rules below decide only how they are spelled.
   Nothing comes from librdkafka's `rdkafkacpp` or from another client; Javadoc is read for
   nullability only.
2. **The binding includes only `confluent_kafka_bindings.h`**, and only from
   `include/confluent/kafka/detail/ffi.hpp`, inside `extern "C"` (the header declares none).
   It adds no FFI entry point. An overload whose entry point (§8) is missing is not generated,
   and the module header's documentation block lists it with the name looked for.
3. **Every fallible call throws.** A C call passes one `out_err` slot and converts a stored error
   into the C++ exception of its id (§6); a `_cb` function is never called, so no `*Async*` peer,
   no completion queue and no sentinel exists. A method waits iff its entry point has a `_cb`
   twin in the header; its documentation says so.
4. **Scope is the Python binding's**: the `Producer`, `Consumer`, `ShareConsumer` and `Admin`
   families and every public type they reach, only `@InterfaceAudience.Public` classes outside
   `internals` and "not a supported API" packages, their public members only; errors and config
   classes as §6 and §7 say. Nothing Java lacks is added except by C++ convention: `begin()` /
   `end()`, `size()`, `operator==`, `std::hash`, `operator<<`, `to_string()`, `of()` factories.
5. **Generated from fixed inputs**: these rules, the Java source, the C header and the previous
   release's API inventory give the same result on every run. Hand-written and not generated:
   `detail/` (error conversion, RAII handles, trampolines, the completion thread) and the tests
   under `test/`. The error classes come from the C++ emitter of
   `cargo xtask generate-error-codes`; `check-generated` fails on stale output.
6. **Core and FFI asks** are recorded in `cpp/design/current/status.md`, never worked around in
   the binding.

## 2. Names

1. Package → namespace, `clients` dropped: `org.apache.kafka.clients.consumer.KafkaConsumer` →
   `confluent::kafka::consumer::KafkaConsumer`, `common.record.TimestampType` →
   `confluent::kafka::common::record::TimestampType`. `Duration` and the Java built-in errors
   (§6) are at `confluent::kafka`. `confluent::kafka::detail` is internal and undocumented.
2. `Exception` → `Error`, nothing else (`TopicAuthorizationException` →
   `TopicAuthorizationError`; `InvalidRegularExpression` keeps its name). No `Local` prefix: C++
   namespaces separate the Java built-ins from Kafka's. The word "exception" appears only in
   `std::exception` and in comments about Java.
3. Methods and parameters snake_case; a leading `get` is dropped when it is followed by what the
   method returns (`getMostSignificantBits` → `most_significant_bits`), kept when it is the
   whole name or qualified (`get`, `get_now`); `is` and `has` stay. Types PascalCase. Java's
   `xSync` / `xAsync` keep their names (`commit_sync`, `commit_async`).
4. `public static final` constants → `static constexpr` members of the same name
   (`std::string_view`, `int32_t`, `std::array`); a constant `Set` or `List` → `std::array`.
   Static methods → `static` member functions. Nested Java types → nested C++ types
   (`CloseOptions::GroupMembershipOperation`).
5. A name C++ gives by convention (§1.4, `Duration`, `detail`) is frozen once released; if Java
   later adds a public item of that name, the Java item gets the different name, chosen with
   the owner.

## 3. Headers and modules

1. One header per Java class, snake_case, in its module directory
   (`include/confluent/kafka/consumer/close_options.hpp`), each starting with the Apache 2.0
   header (§13), `#pragma once`, and including `<confluent/kafka/detail/ffi.hpp>` rather than the
   C header. One umbrella header per module (`<confluent/kafka/consumer.hpp>`) includes them
   all; users include the umbrella, a class header's name may change.
2. Header-only: every function is `inline`, a template or `constexpr`; no `.cpp` ships.
3. Enums: a Java `enum` whose constructor sets exactly one integer field → `enum class` with that
   underlying type and Java's values (`TimestampType : int32_t { CREATE_TIME = 0 }`); a Java
   `enum` with none → `enum class` whose enumerators take the values of the C header's `_e` enum
   in Java order; a class hierarchy the C API flattens into a data-carrying enum (`OffsetSpec`,
   `TopicCollection`) stays a class hierarchy, never `std::variant`. Java's methods on an enum
   are free functions beside it: `to_string(E)` found by ADL, `E_for_name(...)` for a static.
   An enum's kind, underlying type and values are frozen once released; new enumerators are
   appended.

## 4. Overloads and signatures

1. One C++ overload per Java overload, with that overload's parameters in Java's order and no
   default argument. A shorter Java overload calls the longer one with the value Java passes
   (`OffsetAndMetadata(offset)` calls `OffsetAndMetadata(offset, "")`), so Java's value is in
   code. Two overloads whose parameters map to the same C++ types at every position merge,
   named after the earliest declared (`KafkaConsumer(Map)` and `(Properties)` → `configs`);
   `int32_t` and `int64_t` stay distinct. No `*Options` struct or builder for long parameter
   lists: the C++ overload set is the Java one.
2. Public constructors stay constructors; static factories keep Java's names (`Admin::create`,
   `TopicCollection::of_topic_names`). A class without a public constructor has private
   constructors and a `detail` factory. An abstract Java class is an abstract C++ class. A static
   factory and an instance getter of one name both keep it; only an unresolvable clash takes
   Rust's `do_` prefix.
3. A getter and a setter sharing a name are two overloads: `timeout_ms()` reads,
   `timeout_ms(std::optional<int32_t>)` sets and returns what Java's setter returns.
4. Arguments are positional. "Not given" is calling the shorter overload. An explicit
   `std::nullopt` or empty `std::shared_ptr` is Java's `null`: it selects the overload taking the
   parameter, and the FFI reports Java's error where Java throws (§6.6).
5. A class Java builds with setters (`*Options`, `CloseOptions`, `ListConsumerGroupOffsetsSpec`,
   `MockAdminClient::Builder`) has a default constructor and chainable setters returning
   `*this`. The binding builds the C `<X>Options_t` with `<X>Options_new` and calls
   `set_<field>` only for the setters that ran, in last-call order.
6. Nullability: a parameter or return Java gives null a meaning for (its body, the field it is
   stored in, Java client code, or its Javadoc; "Non-null" wins), and a record key or value, is
   `std::optional<T>` for a value and a possibly empty `std::shared_ptr<T>` for an interface.
   Everything else is `T`, `const T&` or `std::string_view`, so a null cannot be written.
7. Parameters: the most general borrowed form in (`std::string_view`, `std::span<const T>`,
   `const std::map<K, V>&`, `const T&`); an interface the client keeps is a `std::shared_ptr`
   the client holds until it releases the registration. Out: a value type by value, a getter on a
   value type by `const&`. A client is move-only; a value type is copyable and movable.
8. `java.time.Duration` → `std::chrono::milliseconds`, alias `confluent::kafka::Duration`; a
   coarser duration converts implicitly, a finer one needs the caller's `duration_cast`. A
   negative duration throws `IllegalArgumentError`, Java's message where Java's method checks the
   sign, the core's `"Timeout must not be negative"` elsewhere *(deviation: the FFI's `-1` is the
   absent timeout, so a negative value cannot cross. Goes when the FFI carries "no timeout"
   separately)*. A Java `Integer timeoutMs` → `std::optional<int32_t>` milliseconds.

## 5. Types

| Java | C++ |
|---|---|
| `boolean`; `byte`, `short`, `int`, `long`; `float`, `double` | `bool`; `int8_t`, `int16_t`, `int32_t`, `int64_t`; `float`, `double` |
| `String` | `std::string_view` in, `std::string` out |
| `byte[]`, `ByteBuffer` | `std::span<const std::byte>` in, crossing as `kafka_Bytes_t` without a copy; out: a span into the fetch batch while its `ConsumerRecords` lives, or `common::utils::Bytes` (§9.3) |
| `Optional<T>`, `OptionalInt`, `OptionalLong` | `std::optional<T>` |
| `Void` as a type argument | `void` (`KafkaFuture<void>`) |
| a type parameter; `? extends T` | a template parameter with Java's letter (`KafkaProducer<K, V>`); `T` |
| `Object` | does not appear: Java's config `Map<String, Object>` merges into the config class's string constructor (§7); no `std::any` on the surface |
| `Collection<T>`, `List<T>`, `Iterable<T>`, `T[]`, `Set<T>` in | `std::span<const T>` (a `Set` with duplicates removed as `Set.copyOf` would) |
| `Map<K, V>` in | `const std::unordered_map<K, V>&`, plus an overload on `std::map` |
| `List<T>`, `T[]` out; `Set<T>` out; `Map<K, V>` out | `std::vector<T>`; `std::unordered_set<T>` (`std::set` where Java builds a `TreeSet`); `std::unordered_map<K, V>` (`std::map` where Java builds a `TreeMap`) |
| `Collection<T>`, `Iterable<T>` out | `std::unordered_set<T>` iff the Java implementation builds a `Set`, else `std::vector<T>` |
| `java.time.Duration` | `Duration` (§4.8) |
| `java.util.UUID`; `org.apache.kafka.common.Uuid` | a `confluent::kafka::Uuid` value type is not added: `UUIDSerializer` uses `common::Uuid`'s string form; `common::Uuid` is translated |
| `Headers`, `Iterable<Header>` taken | `Headers` of `confluent::kafka::common::header`, or a `std::span` of `std::pair<std::string_view, std::span<const std::byte>>` *(deviation: a convenience, as Python's pairs)* |
| `Throwable`, `Exception`, `RuntimeException` | `std::exception_ptr` for a cause; `std::exception` in a `catch` |
| a Kafka or Java built-in exception | its class (§6) |
| `Runnable` | `std::function<void()>` |
| a type not translated yet (`MetricConfig`, `Measurable`, `MetricValueProvider`, `Gauge`) | an incomplete class of Java's name in its namespace, used through `std::shared_ptr` or a reference *(deviation. Goes when it is translated)*; `Partitioner<K, V>` and `Cluster` are translated, since `ProducerConfig::set_partitioner` needs them |
| a non-Public type in a public signature (`Time`) | a `detail` type with Java's method names and no public name |

A shape not in the table stops generation with an error until a row is decided.

JDK interfaces a class implements:

| JDK interface | C++ |
|---|---|
| `Closeable`, `AutoCloseable` | the destructor calls `close()` when it was not called, catching and logging what it throws; `close()` stays public |
| `Comparable` | `operator<=>` and `operator==`; `compare_to` not generated |
| `Iterable` (`ConsumerRecords`) | `begin()` / `end()` over const iterators, `size()` added; `forEach`, `spliterator` not generated |
| `equals` / `hashCode` / `toString` | `operator==`, a `std::hash` specialisation, `to_string(const T&)` and `operator<<` printing what Java prints (a `double` as `Double.toString` since JDK 19) |
| `Future` | `KafkaFuture<T>` (§8.4) |
| `Principal` | dropped; the class keeps its own methods |
| `Serializable` | nothing; value types copy, handle holders move |

An interface not in the table stops generation with an error until a row is decided.

Interfaces Java declares:

1. A client interface (`Producer`, `Consumer`, `Admin`) → an abstract class template with pure
   virtual methods, Java's `default` methods virtual with their body; `KafkaProducer<K, V>` and
   `MockProducer<K, V>` derive from it, so a `std::unique_ptr<Producer<K, V>>` holds either.
2. A multi-method interface the user implements (`ConsumerRebalanceListener`, `Serializer<T>`,
   `Deserializer<T>`, `Partitioner<K, V>`) → an abstract class, abstract methods pure virtual,
   defaults virtual with Java's body, passed as `std::shared_ptr`.
3. A single-method `@FunctionalInterface` (`Callback`, `OffsetCommitCallback`,
   `KafkaFuture::BaseFunction`) → an abstract class too, never a bare `std::function` parameter,
   with a static `of(std::invocable auto&& f)` returning a `std::shared_ptr` to an
   implementation that calls `f`. A method Java adds later is then additive.
4. An interface the client only hands out (`ConsumerGroupMetadata`, `Metric`) → an abstract class
   with no public implementation.
5. Type parameters bind by class template argument deduction from the serde arguments: every
   `Serializer<T>` / `Deserializer<T>` declares `using value_type = T;`, and `KafkaProducer
   p(config, StringSerializer(), IntegerSerializer())` deduces `<std::string, int32_t>`. A
   deduction guide binds an omitted serde to `common::utils::Bytes`. A class with nothing to
   deduce from is written with its arguments, as Java writes `MockConsumer<String, String>`.
   The guides' bindings are frozen once released.

## 6. Errors

1. A real class hierarchy: one class per exception the FFI enum `kafka_common_Error_e` names,
   plus Java's abstract bases and the built-ins below, with Java's `extends` chain; a Java
   abstract exception is a catch-only base with a protected constructor. The base is
   `KafkaError : std::runtime_error`. No `code()` and no `is_*` predicate: the type is the
   predicate (`catch (const RetriableError&)`). The FFI id is the private static `ffi_id`, with
   one id → factory table; a Rust-only id (`LOCAL_CALLBACK`) has no class. The generator fails
   the build on a Kafka class without an id, an id without a class, or a different chain.
2. Java built-ins are root classes of Java's name with `Error`: `IllegalArgumentError :
   std::invalid_argument`, `NullPointerError : std::invalid_argument` (`LOCAL_NULL_POINTER`),
   `IndexOutOfBoundsError : std::out_of_range`; `IllegalStateError`,
   `ConcurrentModificationError`, `UnsupportedOperationError`, `TimeoutError`,
   `InterruptedError`, `ExecutionError`, `NoSuchElementError` derive from `std::runtime_error`.
3. One helper, `detail::check(out_err)`, reads the id, message, payload (through the class's
   view, `kafka_common_Error_resource_not_found_err` then its accessors) and cause chain
   (`kafka_common_Error_source_err`), frees the handle and throws the class of the id. A failed
   `KafkaFuture::get()` throws `ExecutionError` whose `cause()` is the stored error. An error on
   a path that cannot throw (a destructor, the completion thread's pump) is logged to the
   binding's log (§7.5) and freed; nothing is dropped silently.
4. A caught Rust panic arrives as `IllegalStateError` with a message beginning
   `Rust panic caught at the FFI boundary in <function>`, thrown like any other error and named
   under `@throws IllegalStateError` so a reader can tell a core bug from a contract error.
5. `what()` is Java's `getMessage()`, `""` for null; `to_string(e)` and `operator<<` are Java's
   `toString()`: `"<Class>: <message>"` with the C++ class name without namespace, or the
   class's override. Messages are Java's text verbatim, `null`, `true` and `false` included.
6. The binding makes no null check: an empty `std::shared_ptr` or `std::nullopt` crosses as
   `NULL`, and the FFI reports `NullPointerError("<parameter> must not be null")` or the error
   Java's own check throws, in Java's order.
7. `KafkaError::cause()` returns a `std::exception_ptr`, empty when there is none, built from the
   FFI chain when the exception is constructed; `std::nested_exception` is not used. Every
   trampoline catches every C++ exception, stores `std::current_exception()` behind
   `kafka_common_Error_local_callback_err(message, ptr, release_fn)`, and the binding rethrows
   that pointer with `std::rethrow_exception` when the error comes back, so a `catch (const
   MyError&)` sees the object the callback threw; an unrecognised one is thrown as `KafkaError`.
   An exception a user injects into a mock is thrown again as a copy of the same object.
8. Constructor parameters: the string Java passes to `super(...)` is `message`, every
   `Throwable` is `cause` (a `std::exception_ptr`), other strings keep Java's names; one C++
   overload per Java constructor. Payload getters are methods returning what the constructor was
   given or what the shorter Java overload passes.
9. Tests: the hierarchy test checks every class's parent chain against the Java source in both
   directions and every `ffi_id` against the FFI enum; a round-trip test throws one instance of
   every class through a mock callback and asserts type and `what()`; a message test asserts
   `what()` and `to_string()` for every class with a Java-built message.

## 7. Configuration

1. A client's configuration is a config class in Rust's shape: `ProducerConfig`,
   `ConsumerConfig`, `AdminClientConfig`, constructed from
   `const std::map<std::string, std::string>&` (Java's `Properties` form; the
   `Map<String, Object>` overload merges into it), wrapping `<X>Config_new`, so the core
   validates every value with Java's `ConfigError` messages at construction. Values are strings;
   no `std::any`, no variant, no binding-side type check. Each class carries its constants, the
   `AbstractConfig` getters the FFI exposes (`get_short`, `get_int`, `get_long`, `get_double`,
   `get_boolean`, `get_string`, `get_list`), `unused()`, `ignore(key)`, `log_unused()` and the
   static `config_names()`, and is move-only RAII over its C handle. A client constructor takes
   the class by value and keeps Java's `Map` / `Properties` constructors as overloads that build
   it and forward.
2. A key Java fills by reflection is a setter on the config class taking the interface:
   `template <class K, class V> ProducerConfig& set_partitioner(std::shared_ptr<Partitioner<K,
   V>>)`, type-erased inside and recovered when `KafkaProducer<K, V>` is built, a mismatch
   reported at construction. `partitioner.type` stays a string key. Serializers and
   deserializers are constructor arguments, never config values; `key.serializer` and the other
   serde keys reach the core as unknown keys. `interceptor.classes`, `metric.reporters` and
   `partition.assignment.strategy` have no setter until the core defines one.
3. Unknown keys are accepted; the core logs one INFO line listing them.
4. `TopicConfig`, `SaslConfigs`, `SslConfigs`, `SecurityConfig`, `LogLevelConfig`,
   `SecurityProtocol`, `SslClientAuth`, `GroupProtocol` are translated in full minus the
   `ConfigDef` methods; `CommonClientConfigs` is not translated.
5. No config value is a callback or a logger; the FFI has no log hook, so the binding offers none.
   What the binding itself logs goes to `std::clog` with a `confluent.kafka` prefix.
6. Each `Kafka*` client logs one WARNING at construction that the C++ binding is in preview and
   its API not stable before GA; the mocks log nothing *(deviation. Goes when the binding is GA)*.

## 8. Implementation over the FFI

1. FFI-calling classes: the client bases, the `Kafka*` and `Mock*` clients, the config classes,
   the admin `*Result` classes and `KafkaFuture`. The mocks always use the core's mock clients
   (`kafka_*_Mock*`); nothing of Java's `Mock*` classes is translated. Pure C++ translations,
   marshaled at the boundary: value types, records, enums, errors, the `*Options`,
   `CloseOptions`, `Header` / `Headers`, the serdes.
2. Entry point of an overload: the name the root `CLAUDE.md` §2 (Rust) and §4 (C prefix) derive,
   a static factory from the Rust type defining it; the own-name fallible function where one
   exists, else the `_err` twin; never a `_cb` twin and never a name from `confluent_kafka.h`.
   Overloads the Rust side folds into one method with `Option` parameters share one entry point
   and pass absent arguments as `NULL`.
3. Marshaling follows one conversions table, the Python one with the C++ types of §5
   substituted; the rows C++ changes: a call is the own-name function or the `_err` twin with
   one `out_err` slot and `detail::check`; a waiting call is the same, blocking the calling
   thread; a user callback is a trampoline receiving the registered `std::shared_ptr` as `self`,
   catching everything and reporting through `<Client>__set_callback_result` before it returns;
   a `Duration` is `int64_t` milliseconds, `-1` when the shorter overload omits it; an owned
   core error is converted and freed, a borrowed one converted and left to its owner. A shape
   the table lacks stops generation until a row is decided.
4. `KafkaFuture<T>` wraps the C `kafka_common_KafkaFuture_t`, owned or borrowed with its owning
   `*Result` kept alive, one type for every future handed out, `send()`'s included *(deviation,
   as Python's)*. Members where the header has the entry point: `completed_future`, `all_of`,
   `then_apply(std::shared_ptr<BaseFunction<T, U>>)`, `get()`, `get(Duration)` (a timeout throws
   `TimeoutError`), `is_done()`; `cancel`, `is_cancelled`, `is_completed_exceptionally` and
   `get_now` are binding-side over `is_done` and `get` (`cancel()` returns `false`);
   `when_complete` and `to_completion_stage` are not generated.
5. Every core handle is owned by one RAII wrapper (`detail::UniqueHandle`) that calls
   `_destroy_err` in its destructor; a borrowed C pointer never escapes a C++ object that does
   not also keep its owner alive, as a `ConsumerRecord` keeps its `ConsumerRecords`.
6. The binding reimplements no core logic and repeats no core check, with three exceptions,
   run after the call's own argument conversion and before the core's checks: the negative
   timeout (§4.8); a call on a closed client throws `IllegalStateError` with the Java client's
   closed message, the admin RPCs included, except the consumer's `wakeup()`, which does nothing
   *(deviation: `close()` frees the handle. Goes when the native object outlives `close()`)*;
   `group_metadata()` without `group.id` throws `InvalidGroupIdError` *(deviation: the core
   panics there. Goes when the FFI reports Java's error)*. `close()` twice is harmless.
7. A mock method Java's mock leaves as `UnsupportedOperationException` throws
   `UnsupportedOperationError` with Java's message, which the core's mock returns.

## 9. Threads, blocking and buffers

1. A method that blocks in Java is a blocking member function calling the C blocking form under
   its own name. Asynchrony is the user's (`std::async`, a thread), which §9.5 allows for the
   producer and the admin client.
2. `Callback::on_completion` and a future's `then_apply` function run on the client's
   completion thread: one `std::thread` per `KafkaProducer` and per `KafkaAdminClient`, started
   in the constructor, woken through `__set_callbacks_notify` (a condition-variable notify,
   nothing else) and running `__execute_callbacks` until `close()`, which joins it after
   `_destroy_err` ran what was pending. `flush()` and `close()` also run queued callbacks on the
   calling thread while they wait. A raising `send()` callback is logged by the core, not
   propagated; on `MockProducer` it propagates out of the call that completes the record.
   Listener and commit callbacks run on the caller's thread inside the delivering call (`poll`,
   `commit_sync`, `unsubscribe`, `close`), the rebalance not advancing until the listener
   returns. Serdes run on the caller's thread inside `send()` and `poll()`.
3. Zero-copy, the root `CLAUDE.md` §14: keys, values and header values cross as
   `std::span<const std::byte>` views and are never copied by the binding. A serializer's
   returned span stays valid until its next call on that thread or its destruction; the core
   copies it into the batch during the trampoline's report. `ByteArrayDeserializer` copies into
   a `std::vector<std::byte>`; `BytesDeserializer` returns `common::utils::Bytes`, which owns a
   refcount on the fetch batch through `kafka_consumer_ConsumerRecord__take_key` /
   `__take_value` (core entry points to add) and frees it with `kafka_Bytes_destroy` from its
   last copy. A core error's payload buffers are copied *(deviation: the exception outlives the
   freed handle)*.
4. Inside a listener or commit callback the Java shape works: `consumer.commit_sync()` on the
   consumer. A `thread_local` "inside this consumer's callback" flag routes the methods
   KIP-1306's `RebalanceConsumer` has (commit, seek, pause, resume, position, committed, the
   offsets queries, `partitions_for`, `list_topics`, `group_metadata`, `assignment`, `paused`,
   `wakeup`) to the `kafka_consumer_ConsumerHandle_*` blocking forms; `assign`, `subscribe`,
   `unsubscribe`, `poll` and `close` throw `ConcurrentModificationError` with `KafkaConsumer`'s
   message *(deviation. Goes when the handle has them)*. Inside a producer callback `send()`
   works, `flush()` throws Java's deadlock `KafkaError` from the core, `close()` logs Java's
   warning and closes with timeout 0, and the transaction calls time out after `max.block.ms`
   *(deviation: their completions are queued on the thread the callback blocks)*.
5. `KafkaProducer` and `KafkaAdminClient` are thread-safe; `close()` frees the handle only after
   every call using it returned, through a use count in `detail`. `KafkaConsumer` is not: the
   core raises `ConcurrentModificationError` for a concurrent call, and the binding adds no lock;
   `wakeup()` and `metrics()` are exempt. All clients are move-only.
6. `wakeup()` is `noexcept`, callable from any thread; the in-flight or next waiting consumer
   call throws `WakeupError`, the call after proceeds. It is not async-signal-safe, and no other
   interruption exists: the binding installs no signal handler and waits in no slices.
7. An exception a listener or callback raises comes back from the delivering call as Java's
   `maybeWrapAsKafkaException` returns it: from `close()` wrapped in `KafkaError("Failed to
   close kafka consumer")` with the original as `cause()`; elsewhere a `KafkaError` or a commit
   callback's exception as itself, anything else wrapped as `KafkaError("User rebalance callback
   throws an error")`.
8. `Header` and `Headers` are translated classes in `confluent::kafka::common::header`
   *(deviation: Java's concrete classes are internal)*, with `RecordHeaders`' three constructors
   as overloads; mutable until the serializers have run inside `send()`, read-only after with
   `"RecordHeaders has been closed."`; fetched headers stay writable. A serializer receives the
   record's own `Headers&`.
9. Nothing is queued by the binding: `send()` is the blocking C send, so every send that
   returned is in the transaction and `begin_transaction` waits no longer than its entry point.

## 10. Serialization

1. `Serializer<T>` and `Deserializer<T>` are synchronous abstract class templates with Java's
   methods: `serialize(topic, data)`, `serialize(topic, headers, data)` (default calls the
   first), `configure(configs, is_key)`, `close()`, virtual destructor; `deserialize(topic,
   data)` takes a `std::span<const std::byte>` into the fetch buffer and returns `T`. A
   serializer is called for `std::nullopt` too; a deserializer is not called for a null key or
   value, the record's field being `std::nullopt`. The FFI registers them through
   `<Serde>_new_async_err`; the trampoline reports through `<Serde>__set_callback_result` before
   returning.
2. The built-in serdes are Java's classes in `confluent::kafka::common::serialization`,
   constructed as Java's are (`StringSerializer()`), matching Java byte for byte:
   `ByteArray*` (`std::vector<std::byte>`), `Bytes*` (`common::utils::Bytes`), `String*` (UTF-8;
   `configure` reads Java's `serializer.encoding` keys and throws `IllegalArgumentError` for
   another encoding *(deviation: the standard library has no converter)*), `Short*`, `Integer*`,
   `Long*`, `Float*`, `Double*`, `Boolean*`, `UUID*`, `Void*`, and `ListSerializer<Inner>` /
   `ListDeserializer<Inner>` through the constructors taking the inner serde only (the
   class-name ones are the root `CLAUDE.md` §2's no-class case). `ByteBuffer*` are not
   translated *(deviation: `ByteBuffer` maps to the same types as `byte[]`)*; `Serde`, `Serdes`
   and a JSON serde do not exist. Serde class names and `value_type`s are frozen once released.

## 11. Tests

1. Java's tests of every translated class become GoogleTest under `cpp/test/unit`, one
   executable per file named after it, asserting Java's messages; `@RepeatedTest(N)` becomes a
   loop. A test is skipped only when its subject is out of scope or not generated, or needs a
   Java double the binding cannot inject (`MockClient`, `MockTime`), with the reason in the file
   and the core's translation named; one that uses the double only to make a call fail runs
   against an unreachable bootstrap address. A class Java ships without tests takes the Rust
   core's tests of it.
2. Binding-specific tests: `tools/check_ffi_symbols.py` (CTest `ffi_symbols`: every `kafka_`
   name in `include/` is declared in the C header and none is a `_cb` function); the error
   tests of §6.9; lifetime tests run under ASan and UBSan (`-DCONFLUENT_KAFKA_CPP_SANITIZE=ON`),
   once per `make verify`; an overload test per Java overload set; compile-fail tests (a private
   constructor, a copied client, a null where the type has none); serde test vectors against
   Java's.
3. GoogleTest comes from the system (`find_package(GTest CONFIG)`) or is fetched at a pinned
   release. Integration: `make verify` through `verify-cpp`; `test-integration-cpp` with `-ssl`
   and `-sasl-ssl`; a `cpp/grpc_server` backend for the multilanguage conformance suite;
   `producer-perf-test-cpp` and `consumer-perf-test-cpp` compared against the C and Python
   numbers.

## 12. Documentation and stability

1. Doxygen comments from the same translator as the rustdoc (`cargo xtask javadoc`): Java's
   contract first, then `@param`, `@return`, `@throws XError` per Javadoc `@throws`, then one
   `Translation notes` paragraph for anything about C++ (the completion thread, a span's
   lifetime, that a method waits and what runs meanwhile). At most 150% of the Javadoc's words;
   no Java file:line references, members by fully qualified name.
2. A contract change has a `WARNING:` line in the notes, which fails the lint unless the item
   has an accepted entry in `cpp/design/current/doc-contract-changes.md`, in the root
   `documentation.md` §6 format; only the user fills `Accepted`.
3. Source compatibility always; ABI stability from the first major version: no public data
   members; a stateful class holds one pointer (its C handle or a private implementation);
   enumerators are appended and users keep a `default:`; an abstract class's vtable is frozen
   at release, a later Java method appended as a virtual with a default body, and the base's
   inline constructor records the header version the subclass was compiled against, so the
   binding calls a new slot only on an object that has it; interfaces cross as
   `std::shared_ptr`, so an added method needs no new constructor.
4. The interface check: an API inventory (every public declaration, through libclang) compared
   with the previous release's; the previous release's unit tests compiled against the new
   headers (catches an ambiguous new overload); and the ABI probe (`test/probe`, a shared
   library instantiating every public class) compared with the previous release's dump through
   `abidiff`. In development a rule change may alter interfaces per PR; in preview a change
   warns; after GA a change fails in a minor and warns in a major, where the ABI report is the
   release notes' list of breaks.
5. A Java deprecation becomes `[[deprecated("<Java's note>")]]`; a removed method stays,
   deprecated, until the next major. A renamed parameter changes nothing. A return type becomes
   `std::optional` only in a major *(deviation from Python's `| None` in a minor: it changes every
   caller)*.

## 13. Build, format and lint

1. C++20, header-only, under `include/confluent/kafka`; `cpp/CMakeLists.txt` exports the
   INTERFACE target `ConfluentKafka::cpp`, which carries the include path, the C header's
   directory (SYSTEM) and the link to the cdylib cargo builds under `rust/target/<profile>/`,
   located through `RUST_PROJECT_ROOT` (default `../rust`) or `CONFLUENT_KAFKA_LIB_DIR` /
   `CONFLUENT_KAFKA_INCLUDE_DIR`; `LINK_TYPE` picks static or shared. From the repository root:
   `make build-cpp`, `make test-cpp`. Toolchain floor: GCC 11, Clang 16, MSVC 2019 16.10. No
   C++17 mode.
2. `clang-format` with `cpp/.clang-format` (`BasedOnStyle: Google`, `IndentWidth: 4`,
   `ColumnLimit: 120`; layout only, names stay Java's); `clang-tidy` with `cpp/.clang-tidy`,
   every finding an error, `readability-identifier-naming` set to the names of §2, the `google-*`
   group not enabled; `make format`, `make format-check`, `make lint` in `cpp/`. Every
   translation unit of the project builds with `-Wall -Wextra -Wpedantic -Werror` (MSVC: `/W4
   /WX /permissive-`).
3. `check-generated` covers the error emitter's output and every generated header. Released
   through a vcpkg overlay port under `cpp/vcpkg/` (headers plus a prebuilt cdylib per platform),
   versioned separately from the core, starting at a preview version; no Conan recipe.
4. Every file carries the Apache 2.0 header, copyright Confluent Inc.; code translated from
   OpenJDK keeps GPL with Classpath Exception. No TODO or FIXME: a Java path not implemented
   fails with the right `Error`, never silently.
