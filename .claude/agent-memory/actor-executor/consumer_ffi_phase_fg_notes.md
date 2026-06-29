---
name: consumer-ffi-phase-fg-notes
description: Consumer FFI Phase F/G — kafka_consumer C test + cbindgen verification, no-broker wakeup pattern
metadata:
  type: project
---

Phase F/G of consumer FFI (`design/current/consumer-ffi-plan.md`) closed out.

**Phase F was already complete** from a prior commit — all 16 consumer opaque
`_t` types + `kafka_common_Node_t` + both callback typedefs
(`Consumer_poll_callback_t`, `Consumer_op_callback_t`) were already in
`cbindgen.toml [export].include`. No edit was needed; verification only.

Header verification facts:
- cbindgen emits opaque types as `typedef struct { ... } NAME_t;` — to count a
  type's definition grep for `} NAME_t;` (the closing line), NOT
  `typedef struct NAME_t`.
- `kafka_common_KafkaError_t` appears exactly once (lives in `ffi/common.rs`,
  shared by producer + consumer — a second definition would duplicate).
- Header compiles as C: `cc -std=c11 -fsyntax-only -Wall -Wextra -I target/include <tiny.c>`.
- `unsupported_version` maps to protocol error code **35** (Errors::UnsupportedVersion).
  Classic-protocol rejection test asserts `kafka_common_KafkaError_code(err) == 35`.

**No-broker handling (the wakeup test risk):** `AsyncKafkaConsumer::new` does
NOT connect synchronously — it only spawns the bg task — so
`KafkaConsumer_new` never hangs without a broker. `subscribe` just records
intent. A wakeup-interrupted poll returns promptly: pre-arm via
`Consumer_wakeup(c)` before poll, OR fire wakeup from a helper pthread
mid-poll (wakeup bypasses the access guard). Both return null records +
non-null error. `test_kafka_consumer.c` links `pthread` explicitly in
CMakeLists (SYSTEM_LIBS is empty under shared linking).

**make verify decomposition:** `make verify = build format-check lint test`
where `test = test-rust test-integration test-c test-python`.
test-integration + test-python need Docker/broker — run the non-Docker
components individually instead: `cargo xtask format-check`, `cargo xtask lint`,
`cargo test`, and the C `ctest --test-dir bindings/c/build`. Note `cargo xtask
lint` does NOT pass `--features ffi`; run `cargo clippy --features ffi --lib --
-D warnings` separately for FFI clippy.
