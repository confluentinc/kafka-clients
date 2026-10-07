# Critic review of Phase A (admin per-key async callbacks, Topics family) — RESOLVED

Commits reviewed: `bba95a8b`, `2e774090`, `fb7a1889`, `9003db1a`, `b5412794`, `09f2e294`
on branch `feat/admin-per-key-callbacks-phase-a`.

Fix commit: `fixup! 2e774090 / fb7a1889: address COMMENTS.65 (test_mock_admin.c
callback signatures, create_topics dedup direction, temporal-independence test)`.

## 1. [Bug — build-breaking / DoD §9] `bindings/c/tests/test_mock_admin.c` not updated for the new callback signatures — RESOLVED

Rewrote the 5 affected callback functions (`on_create`, `on_describe`,
`on_delete`, `on_create_partitions`, `on_delete_records`) and their call sites
in `bindings/c/tests/test_mock_admin.c` to the new per-key signatures:

- `on_create(const char *key, kafka_admin_TopicMetadataAndConfig_t *value, kafka_common_Error_t *error, void *user_data)`
- `on_describe(const char *key, kafka_admin_TopicDescription_t *value, kafka_common_Error_t *error, void *user_data)` (shared by name/id)
- `on_delete(const char *key, kafka_common_Error_t *error, void *user_data)` (shared by name/id, Void-shaped)
- `on_create_partitions(const char *key, kafka_common_Error_t *error, void *user_data)` (Void-shaped)
- `on_delete_records(const char *topic, int32_t partition, kafka_admin_DeletedRecords_t *value, kafka_common_Error_t *error, void *user_data)`

Each callback struct gained generic `error_count`/`value_count` atomics
(the native callback now fires once per key, not once for the whole batch)
alongside the existing key-specific fields, and every `*_async` test now
`wait_for`s the correct per-key fired-count (2 for two-topic batches) instead
of 1.

The two "NULL handle" tests (`create_topics`, `create_partitions`,
`delete_records`) previously passed `count=0` with a NULL/empty topics array,
relying on the OLD design's "always fires exactly once, even for a
degenerate empty call" guarantee. Under the new per-key design, a null admin
fans its error out over `keys` — and `keys` is empty when the request is
also empty, so the callback correctly fires **zero** times for a truly empty
request regardless of admin validity (there is nothing to report per-key).
This is not a regression the Critic flagged; it's a natural consequence of
the per-key contract, and no real caller (Python always builds `keys` from
non-empty input before calling in) can reach it. Fixed the tests by passing a
small non-empty topics/partitions array with the NULL admin, which exercises
the actual "null admin with real keys" fan-out path — a more realistic and
more rigorous null-handle test than the original all-empty one.

**Verification added to the rollout, per the Critic's request**: built and ran
the full `bindings/c/tests` CTest suite (`cmake -S bindings/c -B
bindings/c/build -DRUST_PROJECT_ROOT=$(pwd)` against a fresh `cargo build
--release --features ffi`, then `cmake --build bindings/c/build && ctest
--output-on-failure` from `bindings/c/build`). Result:

- `test_mock_admin` (the target containing all 5 rewritten callbacks): **all
  tests pass**, including the create_topics/delete_topics/describe_topics/
  create_partitions/delete_records async + null-handle cases.
- All other CTest targets except `kafka_admin` pass unchanged
  (`mock_producer`, `kafka_producer`, `mock_consumer`, `kafka_consumer`,
  `consumer_callbacks`).
- `kafka_admin`'s one failure (`test_kafka_admin_b3_empty_batches_need_no_broker`,
  asserting `list_partition_reassignments` times out against `localhost:9092`
  with no broker) is a **pre-existing environmental artifact, not caused by
  this fix**: a real Kafka broker (`java`, PID confirmed via `lsof -iTCP:9092
  -sTCP:LISTEN`) happens to be listening on port 9092 in this shared sandbox,
  so the call succeeds instead of timing out. This RPC
  (`list_partition_reassignments`/`alter_partition_reassignments`/
  `list_offsets`) is not one of the 7 Topics-family RPCs this phase touches.

This build+run step is now part of this phase's standing verification
routine (recorded in agent-memory for later phases).

## 2. [Bug — behavior mismatch vs. Java] `create_topics` duplicate-name dedup picks the wrong spec — RESOLVED

Fixed `bindings/python/admin.py`'s `_create_topics_keys_and_spec` to be
**first-occurrence-wins**, matching Java's `KafkaAdminClient.createTopics`
(`KafkaAdminClient.java:1782-1796`, vacant-entry-only insertion) and this
crate's own `KafkaAdminClient::create_topics`
(`src/admin/kafka_admin_client.rs`, `Entry::Vacant` check):

```python
deduped = {}
for t in new_topics:
    if t.name not in deduped:
        deduped[t.name] = t
```

(previously `{t.name: t for t in new_topics}`, which is last-occurrence-wins).

Added `test_create_topics_duplicate_name_first_spec_wins` in
`bindings/python/test/unit/test_admin.py`: two `NewTopic("dup", ...)` entries
with different `num_partitions` (3 then 5); asserts the resolved metadata has
`num_partitions == 3` (the first spec), reproducing exactly the Critic's
repro case. Full `bindings/python/test/unit` suite: 365 → 366 tests (net +1
after also adding item 3's coverage), all passing; `soak/test` unaffected
(150 passing).

## 3. [Test quality] `test_create_topics_two_keys_resolve_independently` doesn't prove temporal independence — RESOLVED (fixed, not deferred)

Added a Rust-level unit test,
`ffi::admin::tests::admin_async_per_key_op_delivers_a_resolved_key_without_waiting_on_a_pending_one`
in `src/ffi/admin.rs`, that drives `admin_async_per_key_op` directly with two
hand-built `KafkaFutureImpl<i32>` futures — one already complete ("fast"),
one deliberately never completed ("slow") — and asserts the "fast" key's
callback is delivered (received on an `mpsc::Receiver` within a 5s bound)
without waiting on "slow", and that nothing further arrives within 200ms
afterward (since "slow" never resolves).

A full `Admin` trait stub was not used (the trait has 47 methods; `submit`'s
`&dyn Admin` parameter is unused by this test, so a `MockAdminClient` already
at hand stands in just to build a valid `AdminHandle`).

**Sanity-checked that the test actually catches the regression it targets**:
temporarily injected `let _ = h.runtime.block_on(future.get());` into
`admin_async_per_key_op`'s per-key loop (simulating a reintroduced join
before per-key dispatch) — the test then hung indefinitely (`cargo test`
killed by an external 25s timeout, confirming the whole test binary blocks,
not just one assertion) — then reverted the injection and confirmed the test
passes cleanly again (`cargo test --lib --features ffi
admin_async_per_key_op_delivers_a_resolved_key... ok`, 0.22s).

Full `cargo test --lib --features ffi`: 4126 → 4127 (net +1), all passing;
`cargo test --lib` (no features): 3920, unchanged; `cargo xtask format-check`
clean after `cargo xtask format`.

## Informational (no action taken, per the Critic's own note)

`bba95a8b`'s commit message description of `KafkaFutureImpl::when_complete`
as "still unused" pre-change was inaccurate (it already had a caller at
`src/admin/kafka_admin_client.rs:4321` predating this branch) — the Critic
confirmed this pre-dates the branch and is not a blocker; no action taken.
