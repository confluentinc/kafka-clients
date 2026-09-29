# Rust-Based Kafka Clients

A high-fidelity, architecture-preserving Rust translation of the Apache Kafka Java client (client only), generated instruction-by-instruction from the original Java sources. This project aims to provide a native Rust Kafka client with the same API, structure, and semantics as the official Java client, following Rust idioms and conventions where appropriate.

It also contains bindings for multiple languages, automatically generated following translation rules, optimized and reviewed for performance as well.

## Project Overview

- **Source:** Translated from `org.apache.kafka.clients` and related Java packages (Apache Kafka 4.3.1)
- **Architecture:** Mirrors the Java client structure, namespaces, and logic, adapted to Rust module and naming conventions
- **Standard Library:** Java standard library features are either mapped to Rust equivalents or re-implemented from OpenJDK sources if no equivalent exists
- **Schema:** All Kafka message schemas are generated from the official JSON definitions
- **Wire Protocol:** Full support for Kafka's binary protocol, including flexible versions and tagged fields

## Prerequisites

### Clone with submodules

```bash
git clone --recurse-submodules <repo-url>
```

Or if already cloned:

```bash
git submodule update --init
```

### Install development tools

```bash
cargo install cargo-llvm-cov grcov && rustup component add llvm-tools
```

- **cargo-llvm-cov**: LLVM source-based code coverage instrumentation
- **grcov**: HTML coverage report generator
- **llvm-tools**: LLVM binaries required by cargo-llvm-cov

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

All automation is handled via [xtask](https://github.com/matklad/cargo-xtask) commands.

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

## Code Coverage

```bash
cargo xtask coverage          # Unit tests, HTML report at coverage/html/index.html
cargo xtask coverage-lcov     # Unit tests, lcov output at coverage/lcov.info
cargo xtask coverage-all      # Unit + integration tests (requires Docker)
```

## Regenerating Code

Generated code is automatically updated on build. To force regeneration:

```bash
cargo clean && cargo build
```

## License

Apache License 2.0. See [LICENSE](LICENSE) for details.
