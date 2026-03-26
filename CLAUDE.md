# Confluent Kafka Rust

Rust Kafka client implementation transpiled from the Java Kafka client (client only) with AI assistance. Keeps the same architecture, namespace, and names, adapted to Rust naming conventions.

Any change to this prompt is to be avoided by automatic agents.
Suggestions for changes are possible through the process highlighted in [agent-roles.md](.claude/rules/agent-roles.md).

## Translation Rules
1. **Java Standard Library**: If a Java stdlib class isn't available in the Rust standard library, or in a very popular Rust crate, implement it from scratch by transpiling from OpenJDK
2. **Rust Standard Library**: Use Rust stdlib when it provides equivalent or better functionality/performance
3. **Naming Conventions**:
   - Java package `org.apache.kafka.message` → Rust module `message`
   - Java package `org.apache.kafka.clients.consumer` → Rust module `clients::consumer`
   - Java class names (PascalCase) → Rust struct/enum names (PascalCase)
   - Java method names (camelCase) → Rust function names (snake_case)
   - Preserve original architecture and logical structure
4. **Tests**: Keep the same tests, after translating a class, also translate and run all its corresponding tests.
5. **Comments and documentation**: Keep similar comments as the Java source,
translate javadoc to rustdoc. Never change the contract of public API.
6. **Completeness**: Don't leave any TODO or FIXME — finish everything that should be done
7. **Scripts**: Use xtask Rust programs instead of shell scripts
8. **License**: All transpiled code includes Apache 2.0 license header. Copyright holder is Confluent Inc.
9. **Non-blocking IO**: Use non-blocking IO with a single Selector for multiple TCP connections, as with Java Selector class.
10. **Concurrency**: 
    1. Translate callbacks you find in Java client to code that is executed 
       after awaiting the corresponding call in Rust.
    2. In case the original method isn't blocking to await the callback response (for example awaiting a CompletableFuture), use Tokio `task::spawn` to create a coroutine that is detached from current flow.
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
- **Test**: `cargo test`
- **Format**: `cargo xtask format`
- **Format Check**: `cargo xtask format-check` (CI-friendly)
- **Check Generated**: `cargo xtask check-generated` (validates generated code formatting only)
