# Confluent Kafka C++

A header-only C++20 binding of the Confluent Kafka Rust client, built on the client's C FFI
(`confluent_kafka_bindings.h`). Its API is the Apache Kafka Java client's `org.apache.kafka.clients`
API in C++ form; the rules that derive it are in [CLAUDE.md](CLAUDE.md).

**Status: scaffold.** The build, the test framework, the formatter and the translation linter are in
place, with the first value types and enums translated by hand to prove the linter; the client
families are not generated yet. See `design/current/status.md`.

## Toolchain

| Tool | Floor |
|---|---|
| CMake | 3.20 |
| Compiler | GCC 11, Clang 16, MSVC 2019 16.10 (C++20, no extensions) |
| Rust core | `cargo build --features ffi` under `../rust` (produces `libconfluent_kafka` and the C header) |
| Tests | GoogleTest 1.17 (system package, or fetched by CMake) |
| Format / lint | clang-format and clang-tidy 18 or later |
| Translation linter | LLVM 21 (`clang-tidy-21 libclang-21-dev libclang-cpp21-dev llvm-21-dev`), a JRE for ANTLR, `abidiff` for `abi-check` |

## Build

```sh
# From the repository root: builds the Rust core with the FFI feature, then the C++ project.
make build-cpp
make test-cpp

# From cpp/, against an already built core:
make build            # configure (Ninja) + build
make test             # ctest
make format-check     # clang-format --dry-run --Werror
make lint             # clang-tidy over the test sources, then the translation linter
make lint-tests       # the translation linter's own tests
make lint-baseline    # rewrite tools/lint/data/baseline.txt
make abi-check        # against the previous cpp/vX.Y.Z tag, or BASELINE=<rev>; STRICT=ON fails on a break
make verify           # format-check, lint, test, lint-tests, abi-check
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
tools/lint/                the translation linter: Java model, rules, clang-tidy checks, data files
tools/abi_check.cmake      the interface check against a previous release
design/current/            plans, status, the documentation contract-change ledger
design/history/            finished milestones
```
