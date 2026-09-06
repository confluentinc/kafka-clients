# Confluent Kafka Rust

Rust Kafka client implementation translated from the Java Kafka client (client only) with AI assistance. Keeps the same architecture, namespace, and names, adapted to Rust naming conventions.

Any change to this prompt is to be avoided by automatic agents.
Suggestions for changes are possible through the process highlighted in [agent-roles.md](.claude/rules/agent-roles.md).

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
   - Java const CommonClientConfigs.RETRY_BACKOFF_EXP_BASE → Rust `common_client_configs::RETRY_BACKOFF_EXP_BASE`
   - Each Java class MUST be in its own file, but internal imports for the struct MUST use the parent module re-export, not the file module path. For example,
   `ProducerRecord` is defined in `producer_record.rs` but imported preferably as
   `use crate::producer::ProducerRecord;` not `use crate::producer::producer_record::ProducerRecord;`. Externally it's possible to use both
   - Constant MUST be exported only by the file defining them. E.g.:
     `GROUP_METADATA_TOPIC_NAME` is accessible through
     `::common::internals::topic::GROUP_METADATA_TOPIC_NAME`
   - Static functions MUST be exported only by the file defining them. E.g:
     `to_byte_buffer_accessor` is accessible through `::common::protocol::message_util::to_byte_buffer_accessor`
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
   - When generating wire protocol code, always use per-field `flexibleVersions` overrides via `field_flexible_versions(field, msg_flex)` in the generator — never the raw message-level value. Some fields (e.g. `ClientId` in `RequestHeader`) override to `"none"` and must always use length-prefixed encoding
   - Overloaded methods: make sure there's
     - a method with same name (just translated) that has the intersection of parameters from all overloaded methods, if that method exists in Java.
     - additional methods with <base_name>_<param1_name>_<param2_name> (WITHOUT additional keywords in between).
     - when parameters have the same name and different type,
     **only if the names would collide**, use the type to discriminate the method. In case the difference is **only** Optional use Rust's `Option` and a single method name
     - if there are more than three parameters in the method name, add a dedicated non-exhaustive `Options` struct with all three parameters and the options containing rest of parameters
     - getters and setters with same name: use `<field_name>` for the getter and `set_<field_name>` for the setter. If a method is a setter,
       use `set_<field_name>` even if there's no corresponding getter.
     - examples:
       - `fooBar(a, b)`, `fooBar(a, c)` -> `foo_bar_b(a, b)`, `foo_bar_c(a, c)`
       - `fooBaz(a)`, `fooBaz(a, c)` -> `foo_baz(a)`, `foo_baz_c(a, c)`
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

## Agent Role

Follow the role assigned to you as described in [agent-roles.md](.claude/rules/agent-roles.md).

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