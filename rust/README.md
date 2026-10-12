# Rust Kafka Client

The `confluent_kafka` crate: a high-fidelity, architecture-preserving Rust
translation of the Apache Kafka Java client (client only), translated from
`org.apache.kafka.clients` and related Java packages (Apache Kafka 4.3.1, whose
sources are the `kafka/` submodule at the repository root).

- **Architecture:** Mirrors the Java client structure, namespaces, and logic, adapted to Rust module and naming conventions
- **Standard Library:** Java standard library features are either mapped to Rust equivalents or re-implemented from OpenJDK sources if no equivalent exists
- **Schema:** All Kafka message schemas are generated from the official JSON definitions
- **Wire Protocol:** Full support for Kafka's binary protocol, including flexible versions and tagged fields
- **C FFI:** Behind the `ffi` feature, the crate exports the C API that the
  bindings in `../c/` and `../python/` are built on; the build writes its header
  to `target/include/confluent_kafka.h`

## Status

**Preview:** the Rust client is in preview and not yet recommended for production use. The public API is not stable before the 1.0 GA release and may change. We welcome your feedback while the design is still open.


## Directory Structure

```
src/                    # The confluent_kafka crate
├── lib.rs              # Crate root
├── *.rs                # Client layer (org.apache.kafka.clients), flat at the crate root
├── common/             # org.apache.kafka.common
├── producer/           # org.apache.kafka.clients.producer
├── consumer/           # org.apache.kafka.clients.consumer
├── admin/              # org.apache.kafka.clients.admin
└── ffi/                # C FFI (feature `ffi`)
generator/              # Wire-protocol code generator
├── src/                # Generator sources, compiled into build.rs
├── messages/           # Kafka JSON RPC definitions
├── test-messages/      # Test-only message specs
└── crate/              # Manifest for the generator's tests and CLI
build.rs                # Runs the generator; writes the C header
tests/                  # Integration and multilanguage tests
examples/
xtask/                  # Build automation (`cargo xtask ...`)
multilanguage-test-server/  # gRPC protos + server for the multilanguage tests
consumer-perf/          # Consumer benchmark harness
tools/verifiable-clients/
Makefile                # Targets the repository root's Makefile delegates to
```

`../design/current/structure.md` describes every module in detail.

## Where commands run

Run every `cargo` command **from this directory**. `rust-toolchain.toml` and
`.cargo/config.toml` (which defines the `cargo xtask` alias) are looked up from
the current directory, so `cargo --manifest-path rust/Cargo.toml` from the
repository root would use a different toolchain and lose `cargo xtask`.

## Install development tools

```bash
cargo install cargo-llvm-cov grcov && rustup component add llvm-tools
```

- **cargo-llvm-cov**: LLVM source-based code coverage instrumentation
- **grcov**: HTML coverage report generator
- **llvm-tools**: LLVM binaries required by cargo-llvm-cov

## Building

Rust automation is handled via [xtask](https://github.com/matklad/cargo-xtask) commands.

**Note:** The project is configured with `#![deny(warnings)]` to treat all compiler warnings as errors, ensuring code quality and maintainability.

### 1. Build the Library

```bash
cargo build
```
- Runs the code generator, formats all generated code, and builds the library.
  Add `--features ffi` for the C API.

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

### 5. Lint

```bash
cargo xtask lint          # clippy with warnings as errors
cargo xtask lint-custom   # the project's translation-rule checks
```

## Running Tests

```bash
cargo test --workspace
```
- Runs the unit tests and the tests that need no broker, including round-trip
  wire protocol and generated message tests.

The broker-backed suites are behind cargo features and need Docker:

```bash
cargo test --features integration-tests --test integration
cargo test --all-features -- --skip __grpc   # everything, minus the gRPC arms
```

The `__grpc_python` / `__grpc_c` arms of the multilanguage suite need the
bindings' gRPC images; the repository root's `make test-integration-python` and
`make test-integration-c` build those first.

## Make targets

`Makefile` wraps the commands above with the flags CI uses. The repository
root's Makefile delegates to it, and it can be run directly:

```bash
make build                # release build, feature ffi
make test                 # cargo test --workspace, then with feature ffi
make test-all-features    # all features, skipping the gRPC arms
make test-integration     # functional integration suite (Docker)
make test-integration-perf
make format-check lint doc-check
make clean
```

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

Apache License 2.0. See [LICENSE](../LICENSE) for details.
