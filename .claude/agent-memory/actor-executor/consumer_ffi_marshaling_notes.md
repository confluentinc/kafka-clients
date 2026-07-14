---
name: consumer-ffi-marshaling-notes
description: Consumer C FFI Phase D/E patterns in src/ffi/consumer.rs — guard, void-op helpers, map/list handle ownership, mock-vs-broker test gaps
metadata:
  type: project
---

Phase D+E of `design/current/consumer-ffi-plan.md` landed in commit 176cf598 (branch `dev/c_and_python_consumer_bindings`).

**Why:** Expose the full broker-less-testable consumer surface to C.
**How to apply:** when extending the consumer FFI (e.g. `test_kafka_consumer.c`, broker-only methods), reuse these patterns.

Key patterns in `src/ffi/consumer.rs`:
- `sync_void_op` needs a HRTB closure `for<'a> FnOnce(&'a mut dyn Consumer) -> Pin<Box<dyn Future + 'a>>`; callers wrap the body in `Box::pin(...)`. A plain `FnOnce(&mut _) -> Fut` with one type param does NOT compile (Fut can't borrow the &mut). `async_void_op` instead uses `&'static mut` (consumer is leaked) so its closures return bare `impl Future + Send`.
- Map/list handles own `Vec<Inner>` (NOT `Vec<Box<Inner>>` — clippy `box_collection`); element addresses are stable because the containing `*MapInner` is boxed and the Vec is built once in `box_*` and never mutated. Getters return `&items[i] as *const Inner as *const handle_t` (borrowed; C must not destroy sub-handles).
- Single-value `box_*` constructors (box_topic_partition etc.) were removed as dead code — no FFI fn returns a standalone single value; they only appear as borrowed sub-handles inside maps/lists. Re-add if a standalone-returning API is added.
- Non-hot-path strings cached as `CString` in the `*Inner` struct; topic/key/value/host stay ptr+len (zero-copy, NOT NUL-terminated) per §27.

Mock-vs-broker test gaps (assert mock behavior, not wire behavior):
- `ConsumerRecord::new` (what MockConsumer builds) sets serialized_key/value_size = NULL_SIZE (-1) and timestamp_type = NoTimestampType (-1) regardless of key/value presence. Only the fetch path sets real sizes. Don't assert size==len for mock records.
- `committed` returns entries only for partitions present in mock's `committed` map (populated by a prior commit).
- `update_partitions`/`set_poll_exception` are mock-only inherent methods; FFI drivers `MockConsumer_update_partitions` (builds PartitionInfo with one leader Node) and `MockConsumer_set_poll_error` (illegal_state) exposed.

cbindgen: every new opaque `_t` and `*_callback_t` MUST be appended to `cbindgen.toml` `[export].include`; plain functions returning existing types do not. Release build regenerates `target/include/confluent_kafka.h` (slow, ~1-3 min). Verify exactly one `} kafka_common_KafkaError_t;`.

ConcurrentModification error code maps to -1 (UnknownServerError) in C — see test `CONCURRENT_MODIFICATION_CODE`.
