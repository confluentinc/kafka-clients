# C++ binding: status

Updated 2026-11-14.

## Done

- Scaffold on branch `esabellico/cpp_bindings_scaffold`: CMake project (`ConfluentKafka::cpp`
  INTERFACE target, install and package config, version check against `version.hpp`), Makefile
  (build, test, format, format-check, lint, clean), root `make build-cpp` / `make test-cpp`
  (standalone; not yet part of `build-all`, `test` or `verify`).
- `include/confluent/kafka/detail/ffi.hpp`: the single include of the C header, `UniqueHandle`,
  `ErrorHandle`, `ErrorSlot`.
- Tests: GoogleTest harness (`confluent_kafka_cpp_unit_test`), `ffi_handle_test`, the
  `ffi_symbols` check (`tools/check_ffi_symbols.py`: every `kafka_` name in `include/` is declared
  in the header and none is a `_cb` function), the ABI probe library under `test/probe`.
- `.clang-format`, `.clang-tidy` with the naming rules of CLAUDE.md §2.
- `CLAUDE.md` with the rules derived from the kickstart questionnaire answers (Confluence page
  6364529292).

## Translation linter (`tools/lint`)

A clang-tidy plugin that parses the Java sources with an ANTLR-generated C++ parser and checks the
headers against this file's rules; built with `-DCONFLUENT_KAFKA_CPP_LINT=ON`, or on its own with
`cmake -S cpp/tools/lint`. Spikes, each answered before the code that depends on it:

- (c) **Yes**: the grammars-v4 Java grammar (`tools/lint/grammar`, pinned) with ANTLR 4.13.2
  parses all 1340 files of `kafka/clients/src/main/java` with no syntax error
  (`javamodel_kafka_test`). 9.7 s on one thread, 5.3 s on four and no faster beyond; the
  slowest file is `KafkaAdminClient.java` (0.7 s). Full LL from the start: SLL-first gains
  nothing, as SLL fails on three files in four with this grammar. A prediction cache per thread
  is slower than the shared one. ThreadSanitizer finds nothing with 8 threads.
- Step 1 done: the Java model (`javamodel/java_model.hpp`) and `ck_java_model dump`. Every name
  of the 1340 files resolves, the `@InterfaceAudience.Public` types equal
  `design/current/interface-audience-public-4.4.txt`, and every `@Deprecated` item is in
  `design/current/java-deprecated.txt` (`javamodel_kafka_test`).
- (a) the plugin loading into `clang-tidy-21` and reading comments on template members, and
  (b) `clang-tidy-21` parsing libstdc++ 15's `<chrono> <span> <compare> <functional>`: open,
  waiting for LLVM 21 (`clang-21 clang-tidy-21 libclang-21-dev libclang-cpp21-dev llvm-21-dev`).

## Next

1. `detail::check(out_err)` and the error hierarchy: the C++ emitter of
   `cargo xtask generate-error-codes` (CLAUDE.md §6), the hierarchy, round-trip and message tests.
2. `KafkaFuture<T>`, the config classes (§7.1), `Duration` and the common value types
   (`TopicPartition`, `Node`, `Uuid`, `Header` / `Headers`).
3. The producer family: `Producer<K, V>`, `KafkaProducer`, `MockProducer`, `ProducerRecord`,
   `RecordMetadata`, `Callback`, the serdes, the completion thread; `KafkaProducerTest` and
   `MockProducerTest`.
4. The consumer family, then the admin family.
5. The rest of the translation linter (Java model, rules, checks, `abi-check` with the API
   inventory and `abidiff` of §12.4), `verify-cpp`, the integration tests and the gRPC
   conformance backend (§11.3), the vcpkg overlay port (§13.3).

## Core and FFI asks

Recorded here, not worked around in the binding (CLAUDE.md §1.6):

- A log hook in the FFI, so the binding can route the core's log records (§7.5).
- `when_complete` on `kafka_common_KafkaFuture_t` (§8.4).
- Config setters for `interceptor.classes`, `metric.reporters` and `partition.assignment.strategy`
  once the core defines the interfaces (§7.2).
- The FFI reporting `InvalidGroupIdError` for `group_metadata()` without `group.id`, and a
  separate "no timeout" value, so the binding's two remaining checks of §8.6 can go.

## Environment notes

- clang-format and clang-tidy are not pinned by the build; the Makefile takes `CLANG_FORMAT` and
  `CLANG_TIDY`. The linter will pin clang-tidy 21, the major its plugin is built for. With LLVM 19's clang-tidy built without its resource directory, pass
  `CLANG_TIDY_EXTRA_ARGS=--extra-arg=-isystem/usr/lib/gcc/x86_64-linux-gnu/15/include`.
- The C header's opaque structs use a zero-length array, so the header directory is a SYSTEM
  include; the project's own headers build with `-Wpedantic -Werror`.
