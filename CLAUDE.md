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
   - Java `throws` / `throw` → Rust `return Err(...)` (e.g. `maybeThrowAnyException` → `maybe_return_any_error`)
   - Preserve original architecture and logical structure
3. **C FFI Conventions**:
    - Always define types ending with '_t' for opaque or public structures
    - `org.apache.kafka.common.KafkaException` -> `kafka_common_KafkaError_t`.
    - `is_retriable` -> `kafka_common_KafkaError_is_retriable`.
    - preserve Java namespaces in first part of the function name, skipping `clients`:
      - `org.apache.kafka.clients.producer.KafkaProducer` -> `kafka_producer_KafkaProducer_t`
      - `org.apache.kafka.clients.producer.MockProducer` -> `kafka_producer_MockProducer_t`
    - Don't check for failing programming preconditions like NULLs on required parameters
      or parameters not following the function parameters preconditions.


3. **Tests**: Keep the same tests, after translating a class, also translate and run all its corresponding tests.
4. **Comments and documentation**: Keep similar comments as the Java source,
translate javadoc to rustdoc. Never change the contract of public API.
5. **Completeness**: Don't leave any TODO or FIXME — finish everything that should be done
6. **Scripts**: Use xtask Rust programs instead of shell scripts
7. **License**: All translated code, except GPL with CPE from OpenJDK, includes the Apache 2.0 license header.
    Copyright holder for Apache licensed code is Confluent Inc.
8. **Non-blocking IO**: Use non-blocking IO (Tokio) with a single Selector for multiple TCP connections, as with Java Selector class.
9. **Concurrency**: 
    1. If a method is blocking in Java it should async in Rust
    2. Translate callbacks you find in Java client to code that is executed 
       after awaiting the corresponding call in Rust.
    3. In case the original method isn't blocking to await the callback response (for example awaiting a CompletableFuture), use Tokio `task::spawn` to create a coroutine that is detached from current flow.
    4. About naming, whenever we're talking about a "thread" in Java let's use the term "task" in Rust. E.g. in log messages.
10. **Error handling**: follow [Rust guidelines](https://doc.rust-lang.org/book/ch09-03-to-panic-or-not-to-panic.html) for error handling.
    1. Avoid `panic` for public API, use it only if there's no way to recover from a particular error, such as an OOM or a
       `ArithmeticException` like division by zero.
    2. Return a `Result` when Java code throws an exception even if unchecked but recoverable.
    3. Use a `KafkaError` similar to the librdkafka one with functions `is_retriable` or `is_fatal` or `txn_requires_abort()` and 
       an error code that corresponds to the Java Kafka exceptions.
11. **Language-related optimizations**: When the memory can be kept on the stack even if Java code creates a new object, keep it on the stack.
12. **Parameters and return values of public API**: Accept the most general borrowed form for input parameters. Borrow immutably, and return immutable values.
    Return a borrowed reference in case the data is still owned by the original struct (getter for example).
    When ownership is transferred to the caller prefer returning the struct (making use of RVO) over Box or Rc or Arc.
    Don't copy byte arrays holding the key, value or headers passed to ProduceRecord or received in ConsumeRecord.

## Agent Role

Follow the role assigned to you as described in [agent-roles.md](.claude/rules/agent-roles.md).

## Source Reference
Java source in `kafka/` directory (Apache Kafka 4.2)

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