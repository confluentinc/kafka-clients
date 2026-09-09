# P1 — Actor 64 session analysis

Phase **P1** of `PLAN-python-interface-implementation.md`: the Python error hierarchy
generator and the `confluent_kafka` package skeleton it lives in. Branch
`dev_python-interface-implementation`. Committed, not pushed.

## What was built

### 1. Error-hierarchy generator (`xtask/src/error_hierarchy.rs`, new)
Extends `cargo xtask generate-error-codes` to generate the typed Python exception
hierarchy from the Java exception sources, cross-checked against the FFI
`kafka_common_ErrorCode_t` enum (rule 5 / spec §5.5 / D1).

- Parses each Java exception source for `(name, extends, abstract, package)`,
  resolving the parent FQN package-aware via the file's `import`s (falling back to
  `java.lang.*` builtins and the same-package rule).
- Cross-checks a reviewed 161-row `BRIDGE` table (FFI-id-constant → Java-class-FQN,
  the one correspondence not mechanically derivable — "wire code vs FFI id", D1)
  against the FFI enum, then builds the full class graph and validates it. The
  build **fails** on: a bridge id absent from the enum; an enum id (other than
  `NONE`) with no bridge class; two ids mapping to one class; an abstract class
  carrying an id; a concrete class with no id; an unresolvable parent.
- Emits static `.py` + `.pyi` into the module mirroring each Java package. Each
  class carries Java's parent, a docstring citing the Java class, and (concrete
  only) `_ffi_id: ClassVar[int] = <value>  # kafka_common_ErrorCode_<ID>`; abstract
  classes get a `type(self) is <Base>` `__init__` guard raising `TypeError` and no
  id. `check-generated` now fails on staleness of these files too.
- Wired into `generate_error_codes()` and `check_error_codes_up_to_date()` in
  `xtask/src/main.rs`; the legacy `_error_code.py` is still generated and its
  docstring now marks it superseded for the package.

### 2. `confluent_kafka` package skeleton (`bindings/python/confluent_kafka/`, new)
Per rule 6 / spec §4 module layout:

- `__init__.py` — JDK analogs (imported from the generated root module),
  `Duration = float | timedelta`, no client re-exports (undecided, spec §4).
- `_args.py` — `exactly_one` / `at_most_one` / `all_or_none` (rule 3.5) with the
  exact message forms, raising `IllegalArgumentError`.
- `common/__init__.py`, `common/serialization/__init__.py` (empty skeleton),
  `producer/__init__.py` (skeleton), `consumer/__init__.py` (re-exports the
  consumer-package generated errors), `common/config/__init__.py` (re-exports
  `ConfigError`).
- `common/errors/_base.py` — hand-written `KafkaError` base (message + Java-style
  `__str__`, no `code()` / `is_retriable()` / `is_fatal()` /
  `txn_requires_abort()`).
- `common/errors/__init__.py` — the runtime mapping: `KafkaError`, the whole
  generated hierarchy, a lazily-built `_BY_FFI_ID` (`id → class`, over all four
  generated packages, exposed via module `__getattr__`), `from_ffi_error(handle,
  *, cause=None)` (reads code+message via the C extension, unknown id → base
  `KafkaError`, supports `raise ... from cause`), and `to_ffi_id(error)` for mock
  injection.

### 3. Generated output (committed, produced by the generator)
- `confluent_kafka/common/errors/_generated.{py,pyi}` — 145 `common.errors`
  classes + the 5 abstract bases (4 here + 1 in consumer) + the 6 sibling-package
  classes folded in (C3).
- `confluent_kafka/common/config/_generated_errors.{py,pyi}` — `ConfigError`.
- `confluent_kafka/consumer/_generated_errors.{py,pyi}` — 5 consumer classes + the
  abstract `InvalidOffsetError`.
- `confluent_kafka/_generated_errors.{py,pyi}` — the 4 JDK analogs.

### 4. Build / test integration
- `pyproject.toml` — registers the package in setuptools `packages` (legacy
  `py-modules` kept), adds `mypy` to the `dev` extra.
- `bindings/python/Makefile` — new `typecheck` target (`mypy --strict
  confluent_kafka`), called from `test` (rule 11).
- Tests: `bindings/python/test/unit/test_errors.py` (30 tests) and `test_args.py`
  (12 tests) — see counts below.
- `xtask/src/error_hierarchy.rs` unit tests: 9 (`cargo test -p xtask`).

## Generator cross-check results (counts)

- **FFI enum ids parsed:** 162 (`kafka_common_ErrorCode_t`), of which `NONE=0` is
  the no-error sentinel with no class → **161 class-bearing ids**.
- **Java classes parsed:** 161 concrete (one per class-bearing id) + 5 abstract +
  the base `KafkaException` + intermediate `ApiException` = the full graph closes
  (every parent resolves; 0 unresolved parents).
- **Ids matched:** 161 / 161 (bijective concrete-class ↔ id).
- **Abstract classes:** exactly 5 (`RetriableException`, `RefreshRetriableException`,
  `InvalidMetadataException`, `ApplicationRecoverableException`, and the consumer
  `InvalidOffsetException`), each carrying no `_ffi_id` and raising `TypeError` on
  construction.
- **Module distribution of the 161:** 145 `common.errors`, 5 `consumer`, 1
  `common.config`, 4 JDK root, + 6 sibling-package classes folded into
  `common.errors` (C3: `InvalidRecordException`, `CorrelationIdMismatchException`,
  `InvalidReceiveException`, `QuotaViolationException`, `SchemaException`,
  `BufferExhaustedException`).

Two Java classes lack the `Exception` suffix and are handled explicitly:
`InvalidRegularExpression` → `InvalidRegularExpressionError`, `OffsetMetadataTooLarge`
→ `OffsetMetadataTooLargeError`. `CorrelationIdMismatchException extends
IllegalStateException`, so `CorrelationIdMismatchError` subclasses the root JDK analog
`IllegalStateError` (cross-module import) — Java-faithful, not `KafkaError`.

## Test counts

- **New pytest:** `test_errors.py` 30 + `test_args.py` 12 = **42 new**.
- **Full unit suite:** **396 passed, 2 skipped** (365 pre-existing + 31 collected
  from the two new files; the 42 assert-functions include parametrised classes).
- **xtask:** `cargo test -p xtask` → **33 passed** (9 new for the generator).
- **mypy --strict confluent_kafka:** clean (13 source files).

The error-hierarchy tests re-parse the Java sources independently (both directions:
Java→Python parent equals `extends`, Python→Java no invented concrete class) and
assert `_ffi_id` uniqueness + equality to the same-named `_error_code.py` constant,
so a generated-output drift fails here as well as at build time.

## Verification run

`cargo test -p xtask` ✓, `cargo run -p xtask -- generate-error-codes` ✓,
`cargo xtask format-check` ✓, `cargo xtask lint` ✓ (full 2-pass workspace),
`cargo xtask check-generated` ✓ (199 generated files), `mypy --strict` ✓,
`pytest test/unit` ✓ (396 passed / 2 skipped). Per instruction the full `make
verify` (Docker integration) was NOT run by hand; the git pre-commit hook runs it
on commit.

## Clarifications logged (`design/current/implementation-clarifications.md`)

- **C3** — the six non-`common.errors` Kafka classes fold into `common.errors`
  (spec gives no module row for their sibling packages); owner to confirm.
- **C4** — typed payload accessor **methods** (`RecordDeserializationError.
  topic_partition()`, `TopicAuthorizationError.unauthorized_topics()`, …) are
  **deferred to P4/P5**: each FFI accessor returns an opaque sub-handle whose
  sub-accessors need new `_confluentkafka.c` natives + marshalling, a large C task
  with no producer/consumer caller to exercise it yet. P1 ships the hierarchy, the
  `id → class` table, `from_ffi_error` (code+message+cause) and `to_ffi_id`.
- **C5** — the legacy flat `producer.KafkaError` / `consumer.KafkaError` coexist
  with the new typed hierarchy; removal is a P4/P5 item as those surfaces move into
  the package.

## Commits

1. `P1: Python error hierarchy generator + confluent_kafka package skeleton`
   (generator + generated output + package + runtime mapping + `_args` + pyproject
   + clarifications C3–C5).
2. `P1: tests + mypy typecheck gate for the error hierarchy` (test_errors.py,
   test_args.py, Makefile `typecheck` target, `_error_code.py` superseded-docstring).

`COMMENTS.64.md` was empty at start and end.
