---
name: ffi-java-exact-audit-round70
description: Actor 70 (PR #201) FFI Java-exactness fixes F1-F11 — reusable gotchas for per-key dedup, Python incref counts, verification commands that actually cover all targets
metadata:
  type: project
---

Round 70 (2026-09-29, branch `feat/admin-per-key-python`) made the admin FFI
deliver exactly what Java's `Admin` exposes (Error_cause, Node_is_fenced,
TopicMetadataAndConfig_config, per-key dedup, listTransactions byBrokerId
two-callback shape, updateFeatures_async returning submission errors, ...).

Gotchas worth reusing:

- **Changing how many times a per-key callback fires breaks Python refcounts
  silently.** `_confluentkafka.c` increfs the Python callback N times up front
  (`admin_incref_n`). After making `admin_async_per_key_op` fire once per
  DISTINCT key, every C-ext site that increfs by raw row count leaks on a
  duplicate. All sites were already distinct (admin.py pre-dedups) except
  alterClientQuotas, which admin.py deliberately sends undeduped — fixed with a
  frozenset-based distinct count in C. Audit every `admin_incref_n(cb, n)` when
  touching callback cardinality. No test observes the leak.
- **`cargo check --all-targets --features ffi` does NOT compile
  `tests/integration` / multilanguage helpers** (behind `integration-tests`).
  Use `--all-features` (clippy's `--all-features` does it too) after any core
  public-API change.
- Clippy that works here: PATH=nix clippy-1.97.1 + rustc-1.97.1 + cargo-1.97.1
  bins first, separate CARGO_TARGET_DIR in the scratchpad, and
  `cargo clippy --workspace --all-targets --all-features --keep-going -- -D warnings`
  so the pre-existing `seed_scram_result` type_complexity error doesn't stop
  other targets. See [[env_clippy_unavailable_nix]].
- C gRPC server (`bindings/c/grpc_server`) cannot be built on this Mac (no
  protoc / grpc_cpp_plugin): grep-review its use of changed symbols instead.
- A per-key "absent" outcome (Java map omits the key) is expressible as
  value=NULL + error=NULL; `admin_async_per_key_outcome_op` reports it as
  `None`. The C ext cannot build a LOCAL_* error (`kafka_common_Error_new`
  only maps protocol codes), so preserving Python behaviour needed an admin.py
  `KafkaError._from_parts(-4, ...)`.
- Java's Rust mock mirrors Java's `createTopics` map overwrite: a duplicate
  topic name yields ONE callback carrying TopicExists (second spec's future
  replaces the first), not the first spec's success.
- Also run `ctest` for ALL targets, not just `-R admin`, when touching a
  `kafka_common_*` symbol: test_mock_producer.c pinned GroupAuthorization's
  `""` group id.
