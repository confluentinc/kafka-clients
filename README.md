# example-confluent-kafka-rust

A high-fidelity, architecture-preserving Rust transpilation of the Apache Kafka Java client (client only), generated instruction-by-instruction from the original Java sources. This project aims to provide a native Rust Kafka client with the same API, structure, and semantics as the official Java client, following Rust idioms and conventions where appropriate.

## Project Overview

- **Source:** Transpiled from `org.apache.kafka.clients` and related Java packages (Apache Kafka 4.1)
- **Architecture:** Mirrors the Java client structure, namespaces, and logic, adapted to Rust module and naming conventions
- **Standard Library:** Java standard library features are either mapped to Rust equivalents or re-implemented from OpenJDK sources if no equivalent exists
- **Schema:** All Kafka message schemas are generated from the official JSON definitions
- **Wire Protocol:** Full support for Kafka's binary protocol, including flexible versions and tagged fields

## Directory Structure

```
src/
├── lib.rs              # Root library module
├── common/             # Common types (UUID, protocol, etc.)
├── message/            # Message schema generation
│   └── ...             # FieldType, FieldSpec, StructSpec, etc.
message-specs/          # 197 Kafka JSON RPC definitions
build.rs                # Build script (generates code, formats output)
tests/
└── generated_messages_test.rs  # Integration tests for generated messages
```

## Building the Project

This project uses a pure Rust workflow with no shell scripts. All automation is handled via [xtask](https://github.com/matklad/cargo-xtask) commands.

**Note:** The project is configured with `#![deny(warnings)]` to treat all compiler warnings as errors, ensuring code quality and maintainability.

### 1. Build the Library

```bash
cargo build
```
- Runs the code generator, formats all generated code, and builds the library.

### 2. Format All Code

```bash
cargo xtask format
```
- Formats all source and generated Rust code using `rustfmt`.

### 3. Check Formatting (CI-friendly)

```bash
cargo xtask format-check
```
- Checks formatting of all code (source + generated). Fails if any file is not properly formatted.

### 4. Check Only Generated Code Formatting

```bash
cargo xtask check-generated
```
- Checks formatting of generated files only, without modifying them. Useful for CI.

## Running Tests

```bash
cargo test
```
- Runs all unit and integration tests, including round-trip wire protocol and generated message tests.

## Running the Serialization Example

An executable example is provided to demonstrate serialization and deserialization of Kafka messages with all fields set to example values.

To run the example:

```sh
cargo run --bin serialization_example
```

This example demonstrates:
- **MetadataRequest/Response**: Basic message serialization with topics, brokers, and partitions
- **ProduceResponse**: Advanced usage with tagged fields in flexible versions (9+)
  - Each `PartitionProduceResponse` includes a `CurrentLeader` tagged field (tag 0)
  - `ProduceResponseData` includes a `NodeEndpoints` array tagged field (tag 0)
  - Shows how tagged fields are automatically serialized/deserialized in flexible versions

The output shows the serialized bytes and the deserialized structs for each message type, including proper handling of tagged fields.

## Regenerating Code

Generated code is automatically updated on build. To force regeneration:

```bash
cargo clean && cargo build
```

## License

Apache License 2.0. See [LICENSE](LICENSE) for details.
