# C++ binding: status

Updated 2026-11-18.

## Done

- Scaffold on branch `esabellico/cpp_bindings_scaffold`: CMake project (`ConfluentKafka::cpp`
  INTERFACE target, install and package config, version check against `version.hpp`), Makefile
  (build, test, format, format-check, lint, clean), run as `make -C cpp <target>`, each building
  the core first (standalone; not yet part of the root `build-all`, `test` or `verify`).
- `src/detail/ffi.hpp` (moved from `include/` by the packaging step 2): the single include of the
  C header, `UniqueHandle`, `ErrorHandle`, `ErrorSlot`.
- Tests: GoogleTest harness (`confluent_kafka_cpp_unit_test`), `ffi_handle_test`, the header probe
  under `test/probe`.
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
- Step 2 done: the rules (`tools/lint/rules/`): names (§2, §3.1), the §5 type table as shape sets
  with the `decisions.txt` tag each alternative needs, the §1.4 scope (310 types from the client
  families, the config classes and the serdes, minus `data/scope-excluded.txt`), the expected
  shape of a class, and the data-file parsers; `ck_java_model scope` and `ck_java_model
  expected <FQN>`. The eleven verification classes derive with no problem except `Configurable`,
  whose `Map<String, ?>` has no §5 row, as planned. The word budget joins `expected` with the
  doc check (step 7).
- (a) **Yes**: a module built with the lint project's CMake (LLVM 21.1.8 from Ubuntu, RTTI on)
  loads with `clang-tidy-21 -load`, reads a Doxygen comment's `@param` names on a member of a
  class template, and prints parameter types as written (`int32_t`, `std::string_view`,
  `std::span<const std::byte>`), not canonically. A class's injected name prints unqualified
  (`const Box<K> &`), so the plugin builds types structurally. `find_package(LLVM)` takes no
  version (its version file wants major.minor); the major is asserted after.
- (b) **Yes**: `clang-tidy-21` parses a C++20 header including libstdc++ 15's `<chrono>`,
  `<span>`, `<compare>` and `<functional>` with no error.

- Step 3 done: the clang-tidy plugin `libConfluentKafkaTidy.so` (`tools/lint/tidy/`), loaded with
  `clang-tidy-21 -load`, with `confluent-kafka-java-class`, `-java-inheritance`, `-java-enum` and
  `-header`. They match the translation unit once, share the Java side (built once per process)
  and read their inputs from the options `ConfluentKafkaJavaRoot`, `ConfluentKafkaDataDir`,
  `ConfluentKafkaIncludeRoot`. Tests: `lint_plugin_loads`, one fixture per check under
  `tools/lint/test/fixtures` (23 expected diagnostics and the passing cases beside them, run by
  `fixture_runner` through the real clang-tidy), and `lint_binding_headers` (the binding's own
  headers report nothing). The baseline of untranslated classes comes with the surface check
  (step 5), which is the first check to report a missing class.

- Step 4 done with replacement classes (the owner's call: no class may throw while §6's error
  emitter does not exist): `TopicPartition`, `SubscriptionPattern`, `AclPermissionType` and
  `ConfigResource` with its nested `Type`, each with its module header and its tests (Java's,
  or the Rust core's where Java has none). Value types hold one `std::shared_ptr<const Impl>`
  (decided rule 8): Java's fields are final, so copies share the value. The plugin reports
  nothing on the 10 headers; four mutations of real headers (`tools/lint/test/mutations`, run by
  `fixture_runner mutate`) are each reported. A module header of a nested module
  (`common/acl.hpp`) need not be in its parent's.

- Step 5 done: `confluent-kafka-java-members`, `-java-signature` and `-surface`
  (`tidy/pairing.cpp` pairs each Java overload with the C++ function of its group, qualifier, name
  and arity that mismatches the fewest shapes, so the three checks agree on every pair; types are
  read as written by `tidy/type_reader.cpp`). `data/baseline.txt` is written by the surface check
  (`ConfluentKafkaWriteBaseline: 'true'`): 305 `missing-class` lines, the 310 in-scope types minus
  the five translated. `tools/check_ffi_symbols.py` is gone: the members check rejects a `_cb`
  reference, and the compiler a `kafka_` name the C header lacks. Readings the checks settled:
  Java's implicit default constructor is C++'s implicit one; an override takes its types from
  its base (`send(const int32_t&)` overrides `Producer<K, V>::send(const K&)`); a name a
  using-declaration brings in is read as written (`std::exception_ptr`); a missing `Comparable`
  operator is the inheritance check's finding only. Fixtures for the three checks, a fixture may
  restrict itself to the checks it breaks on purpose (`checks.txt`), and the surface check's
  translation-unit findings are expected through `probe-expectations.txt`; four more mutations
  (a parameter type, a parameter name, a missing and an extra member).

- Step 6, six of seven: `Node`, `ClusterResource`, `PatternType`, `SslClientAuth`, `MetricName`
  and `KafkaPrincipal`, each clean under the seven checks, with its tests and decisions. Found on
  the way and fixed in the linter: a Java enum may be `std::optional` (`forConfig` returns null);
  a decision line names Java's parameter (`clusterId`). `KafkaPrincipal` is the first mutable
  value type: one pointer still, but a copy clones it (its flag is set in place). `Configurable`
  waits for its row: `configure(Map<String, ?>)` has none in §5, which the linter reports.

- Step 7 done: `confluent-kafka-doc` (`tidy/doc_check.cpp`) on every public declaration: a
  comment, an `@param` per parameter and no other, `@return` for a value, `@throws XError` per
  Javadoc `@throws`, at most 150% of the Javadoc's words, one `@par Translation notes` and none
  without a Javadoc, `WARNING:` only in the notes and only with an accepted entry in
  `design/current/doc-contract-changes.md` (stale and malformed entries reported), no Java
  file:line reference, inline tag or HTML. The Javadoc side (`rules/javadoc.cpp`) ports the Rust
  counter without its renames, which never change a count; `cargo xtask javadoc --words` prints
  the Rust counts and `test/data/javadoc_words.txt` holds them for all 2629 in-scope markers,
  equal to the C++ ones (`javadoc_words_parity`; regenerate with the `javadoc_words_vector`
  target). Two counter fixes on both sides: a supertype named by a simple name is an enclosing
  class's member first (Rust missed `StreamsGroupTopologyDescription.Node`), and a code span is
  delimited by matching backtick runs, so Java's stray backtick in `PatternType.MATCH` no longer
  halves its count. A fixture per finding and three mutations of real headers.

- Step 8 done: `confluent-kafka-abi` (`tidy/abi_check.cpp`): a class with data holds one pointer
  and is not passed in registers (Clang's `canPassInRegisters`, the property a later destructor
  or copy would flip) nor an aggregate; no bit-field, virtual inheritance, unscoped or untyped
  enum, virtual outside an interface translation, overloaded virtual name. The header-version
  recorder of §12.3 waits for the first interface. `make abi-check` (`tools/abi_check.cmake`):
  the baseline from `BASELINE=` or the highest `cpp/v*` tag, extracted with `git archive`; the
  API inventories (`confluent-kafka-inventory`) compared by `ck_java_model api-diff`; `abidiff`
  on probe libraries built with `-fkeep-inline-functions`; the baseline's unit tests compiled
  against the new headers. The `abi_check` test runs it against HEAD: unchanged and an appended
  enumerator pass, an inserted enumerator and a removed overload fail.
- Step 9 done: `cpp/Makefile` `lint` (stock checks plus the plugin), `lint-plugin`,
  `lint-custom`, `lint-tests`, `lint-baseline`, `abi-check`, `verify`; root `lint-cpp`,
  `verify-cpp`; README and CLAUDE.md §12.4, §13.3. CI is not wired: a Semaphore job needs LLVM
  21 from apt.llvm.org on Ubuntu 24.04, a JRE, `abidiff` and the Rust FFI build, and runs
  `make verify-cpp`.

## Packaging: a compiled library (2026-11-18)

Upgradable Linux packages: a fix to the C++ layer or to the core reaches programs by a package
upgrade. The binding becomes a compiled library, shared and static, over the shared C core, and
both cores' static libraries ship too (CLAUDE.md §3.2, §12.3, §12.4, §13.1, §13.4, rewritten for
it). The core's soname is opt-in for packaging builds (`CONFLUENT_KAFKA_C_ABI_SONAME=1`,
`C_ABI_MAJOR` in `rust/build.rs`). Steps, one commit each:

1. The rules. Done.
2. The library: `src/`, an object library into `ConfluentKafka::cpp` (shared, soname
   `libconfluent_kafka_cpp.so.0.1`) and `::cpp_static`; the eleven classes' bodies out of their
   headers; `detail/ffi.hpp` to `src/detail/`. Done. The library has no `NEEDED` on the core yet:
   none of the translated classes calls the FFI, and `--as-needed` drops the unused link.
3. The linter: `confluent-kafka-library` (a body in a public header outside §3.2's list, a class
   or free function without `CONFLUENT_KAFKA_CPP_API`), the header check's "never the C header",
   and `confluent-kafka-source`, which takes the `_cb` rule from the members check and runs on
   each source under `src/`. Done, in a check of their own rather than in `-abi`: each fixture
   runs only its check, and the `abi` fixture's classes have no export marks. Fixtures `library`
   and `source`, three mutations of `node.hpp`. The planned C declarations in the lint probe are
   not needed: no check looks C entry points up.
4. The package config: exported targets for both libraries and the core, pkg-config, install
   components, unit tests against the static library too. Done: `ConfluentKafkaCppConfig.cmake`
   defines `ConfluentKafka::core` and `::core_static` from the package prefix (or
   `ConfluentKafkaCpp_CORE_LIB_DIR`) before the exported targets, whose names the in-tree
   targets share; the version file is `SameMinorVersion` before 1.0, as the soname;
   `confluent-kafka-cpp.pc` requires the core's `confluent-kafka.pc` (step 5). `make test-static`
   (in `verify`); the `package_config` test installs the build tree and runs `test/package`, a
   program built with `find_package`, over each library.
5. `cargo xtask install-c` (the core with its soname, links, static library, headers and
   `confluent-kafka.pc`) and the `api-compat` rule that a C ABI break raises `C_ABI_MAJOR`. Done:
   one `cargo rustc --release --features ffi --lib -- --print=native-static-libs` build in
   `target/package/` (never `target/release`, where a library with a soname would make the
   repository's own builds look for a file cargo does not write), then
   `libconfluent_kafka.so.0.<version>` with its `.so.0` and `.so` links, the static library, both
   headers and `confluent-kafka.pc`, rustc's native libraries as `Libs.private`
   (`-lgcc_s -lutil -lrt -lpthread -lm -ldl -lc` on Linux). `api-compat` refuses a C break the
   version allows when `C_ABI_MAJOR` is not raised over the baseline's (a baseline from before the
   soname has none to keep).
6. `abi-check` on the real shared library, with the `SOVERSION` rule. Done: each tree's library is
   built by its own CMake (Debug, against `CORE_LIB_DIR`) and compared by `abidiff
   --no-added-syms` with the public headers given; a break under an unchanged `SOVERSION` is a
   second finding. A header-only baseline shipped no library, so it has no ABI to compare. The
   previous tests of private code (`#include "detail/..."`) are left out. The test's cases now
   patch sources as well as headers and check why a case fails: a removed overload is a removed
   symbol under the soname `0.1`; an inserted enumerator keeps the values (the ABI) and changes
   the order (the API inventory).
7. `packaging/debian/`: one source package, `libconfluent-kafka0`, `libconfluent-kafka-dev`,
   `libconfluent-kafka-cpp0.1`, `libconfluent-kafka-cpp-dev`, built per distro release in its
   container; the upgrade checked by running a program built against the old package. Done:
   `make package-deb DISTRO=ubuntu:24.04 [DEB_REVISION=n]` at the root. The core is staged in
   `debian/core` (the binary targets' `dh_prep` clears `debian/tmp`) and copied in at install;
   the C++ packages carry the binding's version (`dh_gencontrol -v`); `shlibs` with `-V` instead
   of a symbols file, as the C exports change with the experimental FFI; line tables for the
   core's `-dbgsym`. `install-c` removes the LLVM bitcode rustc embeds in the static library
   (196 of 261 MB; `-dev` 52 → 9.7 MB). On Ubuntu 24.04: runtime 5.5 MB, C++ runtime 31 KB, core
   floor glibc 2.38; lintian reports no error or warning (`initial-upload-closes-no-bugs`
   suppressed, it is for the Debian archive; a readelf parse note on the 81 205-section static
   library is lintian's own). Checked in a fresh container: a program built with `find_package`
   against revision 1, after `apt install` of a revision 2 whose `to_string(Node)` changed,
   prints the new text from its shared build and the old from its static one, neither rebuilt.
   Not done: the crates' licenses in `debian/copyright` (they are linked into the core),
   vendoring the crates for the Debian archive, CI jobs.

## Next

1. `detail::check(out_err)` and the error hierarchy: the C++ emitter of
   `cargo xtask generate-error-codes` (CLAUDE.md §6), the hierarchy, round-trip and message tests.
2. `KafkaFuture<T>`, the config classes (§7.1), `Duration` and the common value types
   (`TopicPartition`, `Node`, `Uuid`, `Header` / `Headers`).
3. The producer family: `Producer<K, V>`, `KafkaProducer`, `MockProducer`, `ProducerRecord`,
   `RecordMetadata`, `Callback`, the serdes, the completion thread; `KafkaProducerTest` and
   `MockProducerTest`.
4. The consumer family, then the admin family.
5. `Configurable`, once §5 has a row for `Map<String, ?>`; the Semaphore job for
   `make verify-cpp`; the integration tests and the gRPC conformance backend (§11.3), the vcpkg
   overlay port (§13.3).

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
