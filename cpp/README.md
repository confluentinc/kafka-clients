# Confluent Kafka C++

A C++20 binding of the Confluent Kafka Rust client, built on the client's C FFI
(`confluent_kafka_bindings.h`) as a compiled library, shared (`libconfluent_kafka_cpp.so.0.1`) and
static, so a fix reaches programs through a package upgrade. Its API is the Apache Kafka Java client's `org.apache.kafka.clients`
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
make test-static      # the unit tests against the static library
make format-check     # clang-format --dry-run --Werror
make lint             # clang-tidy over the test sources, then the translation linter
make lint-tests       # the translation linter's own tests
make lint-baseline    # rewrite tools/lint/data/baseline.txt
make abi-check        # against the previous cpp/vX.Y.Z tag, or BASELINE=<rev>; STRICT=ON fails on a break
make verify           # format-check, lint, test, test-static, lint-tests, abi-check
```

The CMake project locates the core through cache variables:

| Variable | Default | Meaning |
|---|---|---|
| `RUST_PROJECT_ROOT` | `../rust` | the Rust crate; the header is read from `<root>/target/include` and the library from `<root>/target/<profile>` |
| `CONFLUENT_KAFKA_INCLUDE_DIR` | derived | directory holding `confluent_kafka_bindings.h` |
| `CONFLUENT_KAFKA_LIB_DIR` | derived | directory holding `libconfluent_kafka` |
| `CONFLUENT_KAFKA_NATIVE_STATIC_LIBS` | threads, `dl`, `m` | the system libraries a program linking the core's static library needs |
| `CONFLUENT_KAFKA_CPP_SANITIZE` | `OFF` | build the tests with AddressSanitizer and UndefinedBehaviorSanitizer |
| `CONFLUENT_KAFKA_CPP_BUILD_TESTS` | `ON` when top-level | build `test/unit` and `test/probe` |

The core is found as both its shared and its static library. Programs link `ConfluentKafka::cpp`
(the shared library, over the shared core) or `ConfluentKafka::cpp_static` (the static library,
over the core's static library), through `add_subdirectory(cpp)` or, installed,
`find_package(ConfluentKafkaCpp)`, which finds the core in the same prefix (or in
`ConfluentKafkaCpp_CORE_LIB_DIR`); pkg-config users take `confluent-kafka-cpp`, with `--static` for
the static library. The C header is needed only to build the library. `cmake --install` has two
components: `runtime` (the shared library and its soname link) and `development` (everything
else). `test/package` is a program built against the installed tree, run by the `package_config`
test.

## Linux packages

`make package-deb DISTRO=ubuntu:24.04` at the repository root builds, in a container of that
release, the Debian packages of `packaging/debian` into `build/deb/<distro>/<revision>/`:

| Package | Contents |
|---|---|
| `libconfluent-kafka0` | `libconfluent_kafka.so.0`, the C core (soname from `rust/build.rs` `C_ABI_MAJOR`) |
| `libconfluent-kafka-dev` | the C headers, `libconfluent_kafka.so`, `libconfluent_kafka.a`, `confluent-kafka.pc` |
| `libconfluent-kafka-cpp0.1` | `libconfluent_kafka_cpp.so.0.1` |
| `libconfluent-kafka-cpp-dev` | the C++ headers, `libconfluent_kafka_cpp.so`, `.a`, the CMake package, `confluent-kafka-cpp.pc` |

A program linked against the shared libraries takes a fix of either by a package upgrade; one
linked against the static ones keeps what it was built with. The core is staged by
`cargo xtask install-c`, which any other packaging can use too.

The Makefile variables `CLANG_FORMAT`, `CLANG_TIDY` and `CLANG_TIDY_EXTRA_ARGS` select the LLVM
binaries; `CLANG_TIDY_EXTRA_ARGS` takes, for example, `--extra-arg=-isystem<gcc include dir>` when a
clang-tidy build lacks its own resource directory.

## Layout

```
include/confluent/kafka/   the public headers (one per Java class); export.hpp marks the exports
src/                       the library's sources (one .cpp per header); detail/ is the C boundary
test/unit/                 GoogleTest, one executable per file
test/probe/                every public header compiled on its own
tools/lint/                the translation linter: Java model, rules, clang-tidy checks, data files
tools/abi_check.cmake      the interface check against a previous release
design/current/            plans, status, the documentation contract-change ledger
design/history/            finished milestones
```
