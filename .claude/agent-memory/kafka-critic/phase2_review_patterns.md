---
name: Phase-2 review patterns
description: High-yield review areas for the wire-protocol generator + runtime translation phase
type: reference
---

Recurring concerns when reviewing Java→Rust translations of the wire-protocol
generator + runtime layer:

1. **Generator-side `field_flexible_versions(field, msg_flex)` must be applied
   in every emit path: `add_size`, `read`, `write`, AND `schema()`.** Easy to
   miss the schema_type emit. The CLAUDE.md per-field rule is locked in Phase
   2a tests (`test_field_flexible_versions_*`).

2. **`FieldType::Records` vs `FieldType::Bytes` should NOT be collapsed at
   emit time.** Records must take the zero-copy path
   (`write_byte_buffer`/`write_records`); Bytes can use `write_byte_array`.
   Phase 2d-4 collapsed both to `write_byte_array(.as_slice())` to avoid a
   borrow lint — this defeats CLAUDE.md rule 12 zero-copy on the producer
   send path.

3. **Tagged-field struct/array writes pre-allocate hard-coded buffer sizes
   (256 for structs, 1024 for arrays).** Java sizes via
   `_object.size(_cache, _version)`. The `ObjectSerializationCache` was
   designed for exactly this — generator emit should look it up. As-is, every
   produce response with `current_leader` pays an extra 256-byte alloc per
   partition.

4. **Wrapper-level byte fixtures (Phase 2e) re-use per-`*Data` fixtures via
   concatenation, not a fresh Java capture.** Acceptable when per-`*Data`
   fixtures already lock the inner bytes against Java, but a single end-to-end
   `header_v2 ++ body_v9` capture would close the loop without much effort.

5. **`schema()` emits `Type::CompactBytes`/`Type::Bytes` for array-of-struct
   fields as a placeholder.** The wire encoding (in `read`/`write`/`add_size`)
   is correct, but consumers that introspect the `Schema` (parameterized
   tests, future reflection paths) will see wrong field types. Flag for the
   phase that re-enables those tests.

6. **`KafkaError::from_code` collapses unmapped wire codes to
   `UnknownServer`.** Many codes the producer's `Sender` treats specially
   (NotEnoughReplicas=19, NotEnoughReplicasAfterAppend=20, KafkaStorageError=56,
   UnknownTopicId=100) collapse; `is_retriable` then returns false for all of
   them. The `Errors` catalogue (Phase 2c) has all 130+ variants but the
   `KafkaError` mapping is from Phase 1 and incomplete.

7. **`clearPartitionRecords()` is *not* a no-op in Java.** It sets
   `data = null` to release records memory before response arrives. If the
   Rust translation has already snapshotted `acks/timeout/transactional_id`
   into the wrapper, the records release path is straightforward — adding the
   method is a small finishing touch.

8. **`ApiKey.listeners` drift between hand-coded `ALL_API_KEYS` and generated
   `ApiMessageType.listeners()` is not asserted.** The Phase 2c hand-coded
   table is best-effort per Apache Kafka 4.2; the JSON-spec table may differ
   (post-KIP-833 ZkBroker removal). For producer-path code this is fine
   because no caller uses `listeners` for routing — but worth flagging for
   any phase that adds routing logic.

9. **`RequestUtils::serialize` does an unconditional `to_vec()` of the inline
   buffer.** Returns `Vec<u8>` to match Java's `byte[]` return; this is one
   extra copy per call. Not on the producer hot path (the Sender will go
   through `SendBuilder` directly), but worth flagging if any other call
   site appears.

10. **`expect()` panics in lookup chains for known-good API keys are
    technically reachable.** `ApiKeys::for_id(0).expect("PRODUCE")` will
    never fail because PRODUCE is in `ALL_API_KEYS`, but it's a panic on
    the public API surface that CLAUDE.md rule 10.1 advises against. A
    `OnceLock<&'static ApiKey>` or `const PRODUCE_KEY: &ApiKey = &ALL_API_KEYS[0];`
    would remove the panic without runtime cost.

11. **Verifying fixes for "field_type → emit dispatch" issues requires
    looking at the *generated* file, not just the generator source.** The
    generated `*_data.rs` lives under `target/debug/build/confluent-kafka-rust-*/out/generated/`.
    For Issue 1 (Records → write_byte_buffer) the diff in `generator/src/lib.rs`
    only shows the conditional emit; you have to grep the generated
    produce_request_data.rs to confirm `writable.write_byte_buffer(_nv.as_slice())`
    actually shows up at the records field site (and that nearby Bytes
    fields still get `write_byte_array`).

12. **Tagged-field size cache: a fresh `ObjectSerializationCache` per
    write site is fine.** Java reuses the outer cache, but each
    tagged-struct's `add_size` populates the cache and the immediately
    subsequent allocate-and-write consumes the total. The cache is not
    re-used across sites; the size pass and write pass are independent.
    Don't flag per-site fresh caches as a correctness concern.

13. **The tagged-field branch for Bytes/Records still emits
    `write_byte_array` even after Issue 1's fix.** The Issue 1 fix only
    touched the non-tagged `generate_field_write` and
    `generate_array_element_write` paths. Tagged Records would not get
    the SendBuilder zero-copy path. In Phase 2 wired specs (Produce,
    Metadata, ApiVersions) no `records` field is tagged so this is
    theoretical — but worth flagging if a future spec adds one.
