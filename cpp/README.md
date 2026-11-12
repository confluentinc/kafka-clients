# Confluent Kafka C++

A header-only C++20 binding of the Confluent Kafka Rust client, built on the client's C FFI
(`confluent_kafka_bindings.h`). Its API is the Apache Kafka Java client's `org.apache.kafka.clients`
API in C++ form; the rules that derive it are in [CLAUDE.md](CLAUDE.md).

**Status: scaffold.** The build, the test framework, the formatter and the linter are in place;
the public API is not generated yet. See `design/current/status.md`.

## Toolchain

| Tool | Floor |
|---|---|
| CMake | 3.20 |
| Compiler | GCC 11, Clang 16, MSVC 2019 16.10 (C++20, no extensions) |
| Rust core | `cargo build --features ffi` under `../rust` (produces `libconfluent_kafka` and the C header) |
| Tests | GoogleTest 1.17 (system package, or fetched by CMake) |
| Format / lint | clang-format and clang-tidy 18 or later |

## Build

```sh
# From the repository root: builds the Rust core with the FFI feature, then the C++ project.
make build-cpp
make test-cpp

# From cpp/, against an already built core:
make build            # configure (Ninja) + build
make test             # ctest
make format-check     # clang-format --dry-run --Werror
make lint             # clang-tidy over the test sources
```

The CMake project locates the core through cache variables:

| Variable | Default | Meaning |
|---|---|---|
| `RUST_PROJECT_ROOT` | `../rust` | the Rust crate; the header is read from `<root>/target/include` and the library from `<root>/target/<profile>` |
| `CONFLUENT_KAFKA_INCLUDE_DIR` | derived | directory holding `confluent_kafka_bindings.h` |
| `CONFLUENT_KAFKA_LIB_DIR` | derived | directory holding `libconfluent_kafka` |
| `LINK_TYPE` | `shared` | `shared` or `static` |
| `CONFLUENT_KAFKA_CPP_SANITIZE` | `OFF` | build the tests with AddressSanitizer and UndefinedBehaviorSanitizer |
| `CONFLUENT_KAFKA_CPP_BUILD_TESTS` | `ON` when top-level | build `test/unit` and `test/probe` |

Consumers link the INTERFACE target `ConfluentKafka::cpp` (`find_package(ConfluentKafkaCpp)` after
`cmake --install`, or `add_subdirectory(cpp)`).

The Makefile variables `CLANG_FORMAT`, `CLANG_TIDY` and `CLANG_TIDY_EXTRA_ARGS` select the LLVM
binaries; `CLANG_TIDY_EXTRA_ARGS` takes, for example, `--extra-arg=-isystem<gcc include dir>` when a
clang-tidy build lacks its own resource directory.

## Layout

```
include/confluent/kafka/   the library (one header per Java class; detail/ is internal)
test/unit/                 GoogleTest, one executable per file
test/probe/                a shared library instantiating every public header (ABI baseline)
tools/                     checks run by CTest and the Makefile
design/current/            plans, status, the documentation contract-change ledger
design/history/            finished milestones
```
