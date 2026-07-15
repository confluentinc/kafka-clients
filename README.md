# example-confluent-kafka-rust

A high-fidelity, architecture-preserving Rust transpilation of the Apache Kafka Java client (client only), generated instruction-by-instruction from the original Java sources. This project aims to provide a native Rust Kafka client with the same API, structure, and semantics as the official Java client, following Rust idioms and conventions where appropriate.

## Project Overview

- **Source:** Transpiled from `org.apache.kafka.clients` and related Java packages (Apache Kafka 4.2)
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

### Install a current Rust toolchain

This project requires `edition = "2024"` (Rust ≥1.85), and `cargo-llvm-cov`
requires Rust ≥1.87. Your distro's packaged Rust (e.g. Ubuntu's
`apt install cargo`/`rustc`, which ships 1.75) is too old — install Rust via
[rustup](https://rustup.rs) instead:

```bash
sudo apt install -y rustup   # or: curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
rustup default stable
```

Make sure `~/.cargo/bin` comes before any distro Rust on your `PATH`, then
verify with `cargo --version` (should be ≥1.87). If you already have
`cargo`/`rustc` installed via `apt`, remove them (`sudo apt remove cargo
rustc`) to avoid `PATH` ambiguity.

### Install development tools

```bash
cargo install cargo-llvm-cov grcov && rustup component add llvm-tools
```

- **cargo-llvm-cov**: LLVM source-based code coverage instrumentation
- **grcov**: HTML coverage report generator
- **llvm-tools**: LLVM binaries required by cargo-llvm-cov

### Additional tools for C/Python bindings and full verification

The Rust-only workflow above (`cargo build`, `cargo test`, `cargo xtask ...`)
is enough to work on the core library. The repo also ships C bindings
(`bindings/c/`) and Python bindings (`bindings/python/`), built and tested
through the top-level `Makefile`:

```bash
sudo apt install -y cmake python3-venv
make init      # one-shot: submodules + Python venv + bindings/python[dev]
make build     # builds Rust (release, with `ffi` feature), C, and Python bindings
make verify    # format-check + lint + Rust/C/Python tests (what CI/pre-commit run)
```

`make init-hooks` (run as part of `make build`) installs a pre-commit hook
(`.githooks/pre-commit`) that runs `make verify-sandbox` before every commit;
bypass with `git commit --no-verify` if needed.

Docker is required for `cargo xtask coverage-all` and for the opt-in
multilanguage integration tests (`make test-multilanguage`), which spin up
per-language gRPC server containers via testcontainers.

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
