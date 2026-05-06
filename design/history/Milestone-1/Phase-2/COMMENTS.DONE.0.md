# Phase 2 Review — Resolved Items (Critic N=0)

These items from `COMMENTS.0.md` have been addressed by the Actor 0
fixup commits.

---

## MAJOR

### Issue 1: Generator emits `write_byte_array` for `FieldType::Records` — defeats SendBuilder zero-copy

- **File:** `generator/src/lib.rs:3940` (and matching `add_size` site for records)
- **Severity:** MAJOR (Performance, CLAUDE.md rule 12)
- **Java reference:** `MessageDataGenerator.generateVariableLengthWriter` emits
  `_writable.writeRecords(this.<field>)` for `FieldType::Records`, which
  dispatches into the zero-copy chunk path on `SendBuilder`. For
  `FieldType::Bytes` Java uses `_writable.writeByteArray(...)`.
- **What Rust does:** Both `Bytes | Records` collapse to
  `writable.write_byte_array(_nv.as_slice())`. On `SendBuilder` this writes
  into the inline `ByteBufferAccessor` and then copies again on flush — the
  records bytes do **not** become a separate `Arc<[u8]>` chunk.
- **Why wrong:** CLAUDE.md rule 12: "wire sends must use vectored I/O so the
  framing header and payload are sent without assembling a single contiguous
  buffer". On the producer send path this means the per-batch records bytes
  must reach the wire as their own `IoSlice`. The Phase 2c NOTES already
  acknowledged that `write_records(BaseRecords)` is deferred to Phase 3 —
  but at the **generator** level the distinction between `Bytes` and
  `Records` is being collapsed. When Phase 3 introduces `BaseRecords` /
  `MemoryRecords`, callers will look for `write_byte_buffer`/`write_records`
  emit on the records field and find `write_byte_array`. Easy to miss.
- **Suggested fix:** Have the generator emit `writable.write_byte_buffer(...)`
  (which already routes through `SendBuilder`'s chunk path) for
  `FieldType::Records` *now*. `Vec<u8>` deref-coerces to `&[u8]` already, so
  no API change is required. Phase 3 then only has to swap `Vec<u8>` for
  `BaseRecords` and add the `write_records` trait method, without revisiting
  every emit site.

**Fix applied:** Generator now emits `write_byte_buffer` for
`FieldType::Records` (both the top-level `generate_field_write` and the
`generate_array_element_write` paths). `FieldType::Bytes` still uses
`write_byte_array`. Both produce identical wire bytes — the difference
is only on `SendBuilder`. Verified: all 314 prior lib tests + the
Java-authoritative byte fixtures captured in
`src/common/message/tests.rs` continue to pass.
Commit: `d8a1886 fixup! Phase 2d: align generator emit code with Phase 2c runtime traits`.

---

### Issue 2: `ProduceRequest::clearPartitionRecords()` not translated

- **File:** `src/common/requests/produce_request.rs:56–63`
- **Severity:** MAJOR (Missing Requirement)
- **Java reference:** `kafka/clients/src/main/java/org/apache/kafka/common/requests/ProduceRequest.java`
  → `public void clearPartitionRecords()` sets `data = null` after caching
  `partitionSizes()`. Used by `Sender.completeBatch` to release the records
  buffer immediately after sending instead of holding it until response
  arrives.
- **What Rust does:** Method is omitted; the docstring on `request_data()`
  asserts it's a no-op, which is incorrect.
- **Why wrong:** Without it, the records `Vec<u8>` inside `ProduceRequestData`
  stays alive for the full request/response round-trip, doubling peak
  records memory in flight. CLAUDE.md rule 5 requires completeness. The
  existing `acks`/`timeout`/`transactional_id` snapshot fields are exactly
  the cache that Java's `clearPartitionRecords` requires, so the wrapper is
  *almost* there.
- **Suggested fix:** Add `pub fn clear_partition_records(&mut self)` that
  replaces `self.data.topic_data` with `Vec::new()` (or wraps `data` in
  `Option<ProduceRequestData>` and takes it). Update `request_data()` to
  return an error or empty data after clear, mirroring Java's `IllegalStateException`.

**Fix applied:** `data` is now `Option<ProduceRequestData>`. A new
`clear_partition_records` method sets `data = None`. `request_data()`
returns `Result<&ProduceRequestData, KafkaError>` with an
`IllegalArgument` error matching Java's `IllegalStateException` text
post-clear. Eagerly cached `partition_keys: Vec<{topic_name, topic_id,
partition_index}>` populated at construction so `error_counts` /
`get_error_response` keep working after clear (mirrors Java's lazily-
initialized `partitionSizes` cache). The infallible
`AbstractRequestResponse::data()` returns an empty stub
(`ProduceRequestData::new()`) post-clear — Java's "do not serialize
after clear" contract still applies. Two new tests cover post-clear
behaviour.
Commit: `72da60b fixup! Phase 2e: requests module — wrapper layer over generated *Data structs`.

---

### Issue 3: Generator schema emit for array-of-struct fields uses placeholder `Type::CompactBytes/Bytes`

- **Files:**
  - generated `metadata_request_data.rs:417` (`topics`)
  - generated `metadata_response_data.rs` (`brokers`, `topics`)
  - generated `produce_request_data.rs:686` (`topic_data`)
  - generated `produce_response_data.rs:1528` (`responses`)
- **Severity:** MAJOR (Behavior Mismatch — silently wrong type info)
- **Java reference:** `SchemaGenerator.generateSchemaCode` emits
  `new ArrayOf(<TopicProduceData.SCHEMA_X>)` /
  `new CompactArrayOf(...)` for `[]Struct` fields.
- **What Rust does:** `Field::with_doc("topic_data", if version >= 9 {
  Type::CompactBytes } else { Type::Bytes }, ...)`. This is the *bytes*
  type, not array-of-struct.
- **Why wrong:** `Schema::new(...)` returns a misleading shape. The
  emit-side `read`/`write`/`add_size` methods drive the wire encoding and
  are correct, so the wire bytes match Java — no immediate bug. But
  consumers like `ApiVersionsResponseTest.requestSchemas[]` (deferred per
  the actor's notes) and any future schema-based reflection (e.g.
  KafkaProtocolDescriptionGenerator) will see wrong field types. The actor
  flagged this in `phase2d_generator_runtime_gap.md` as a known limitation
  — calling out here so it doesn't get lost when the deferred tests come
  back online.
- **Suggested fix:** Extend the schema emit branch in `generator/src/lib.rs`
  to recognize `FieldType::Array(Struct(name))` and emit
  `Type::CompactArrayOf(Box::new(<NestedStruct>::schema(version)?))` /
  `Type::ArrayOf(...)` — needs a runtime `Type::ArrayOf(Box<Schema>)` /
  `Type::CompactArrayOf(Box<Schema>)` variant. Until then add a
  generator unit test that asserts the produced `Schema` has the right
  field types so the placeholder doesn't drift unnoticed.

**Fix applied:** `schema_type_for` in `generator/src/lib.rs` now emits
`Type::CompactArray(Box::new(CompactArrayOf::new(Type::Schema(Box::new(<Inner>::schema(version)?)))))`
(and the `Array`/`ArrayOf::new` non-compact variant) for
`FieldType::Array(Struct(...))`. Direct `FieldType::Struct(name)`
emits `Type::Schema(Box::new(<Name>::schema(version)?))`. Nested and
common structs gain their own `schema(version) -> Result<Schema, …>`
method (previously only top-level `*Data` structs had one). The
generated file imports `ArrayOf`/`CompactArrayOf`. New generator
test `schema_emit_for_array_of_struct_uses_array_of_schema_not_bytes`
exercises the produce_request_data top-level body. (The runtime
already had `Type::Array(Box<ArrayOf>)` and
`Type::CompactArray(Box<CompactArrayOf>)` variants — no new variants
needed.)
Commit: `d8a1886 fixup! Phase 2d: align generator emit code with Phase 2c runtime traits`.

---

## MINOR

### Issue 4: ApiKey ↔ ApiMessageType drift test omits `listeners`

- **File:** `src/common/protocol/api_keys.rs:914–953`
- **Severity:** MINOR
- **Java reference:** `ApiKeys.java` — `listeners` is sourced from
  `ApiMessageType.listeners()` (the JSON spec).
- **What Rust does:** Hand-coded `ALL_API_KEYS` listener sets are
  best-effort per Apache Kafka 4.2; the drift test compares only `id` and
  `name`, *deliberately* not `listeners`. The docstring explains the
  rationale (avoid locking generator-side reshape). Result: the two tables
  can silently disagree on `listeners` (e.g. `ZkBroker` may be present in
  hand-coded but absent in generated post-KIP-833).
- **Why it matters now:** Producer-path code does not consult `listeners`
  except in two test assertions, so functional impact is currently zero.
  But the moment any code does `api_key.in_scope(ListenerType::Broker)` for
  routing, it will use the hand-coded value, not the JSON-spec value.
- **Suggested fix:** Either delete the hand-coded `listeners` field and
  delegate `ApiKey::listeners()` to `self.message_type().listeners()` (a
  one-liner), or add an explicit "intentional drift" assertion to the test.

**Fix applied:** Critic's preferred option — deleted the
`listeners: &'static [ListenerType]` field from `ApiKey` entirely. New
`ApiKey::listeners()` getter delegates to
`self.message_type().listeners()`, and `in_scope()` calls the new
getter. `ListenerType` is re-exported from the generated
`api_message_type` module so there is a single source of truth (the JSON
spec). Drift test docstring updated. The translated `every_api_has_a_listener`
test now applies the `hasValidVersion()` guard Java does in
`ApiKeysTest#testApiScope` — APIs with no valid versions are exempt.
Commit: `30b2b3f fixup! Phase 2d: ApiKeys ↔ generated ApiMessageType drift test`.

---

### Issue 5: `ByteBufferAccessor::allocate(256)` / `allocate(1024)` for tagged-field structs/arrays

- **Files:** generated `produce_response_data.rs:624` (CurrentLeader 256-byte tmp) and `:1495` (NodeEndpoints 1024-byte tmp); pattern emitted by `generator/src/lib.rs` (struct/array tagged-field write).
- **Severity:** MINOR (Performance)
- **Java reference:** Java sizes the tagged-field struct via
  `_object.size(_cache, _version)` and pre-allocates exactly that amount.
- **What Rust does:** Emits a hard-coded `allocate(256)` (struct) or
  `allocate(1024)` (array). `ByteBufferAccessor::ensure_remaining_write`
  defensively grows past the cap, so functional output is correct. But every
  produce **response** with a `current_leader` tag pays an extra 256-byte
  alloc per partition (1024 per response for `node_endpoints`), and any
  array exceeding 1024 bytes triggers a `Vec::resize` reallocation.
- **Suggested fix:** Use the size already computed by
  `MessageSizeAccumulator` in `add_size` and pass it through
  `ObjectSerializationCache` (the identity-keyed cache exists for exactly
  this purpose). The cache can store the per-field tagged-struct size so
  `write` allocates exactly what it needs.

**Fix applied:** Tagged Bytes/Records writes no longer allocate a temp
buffer at all — the payload size (`varint(len+1) + len`) is computed
inline from constants and `byte_utils::size_of_unsigned_varint`, then
each piece is written directly into the outer `writable`. Tagged Struct
writes (nullable + non-nullable + null-default cases) and tagged Array
writes pre-compute the exact size via a fresh
`MessageSizeAccumulator` (with its own `ObjectSerializationCache` for
nested struct elements) and allocate the temp `ByteBufferAccessor` to
that size. No more `allocate(256)` or `allocate(1024)` literals in the
generator emit; verified by inspecting the regenerated
`produce_response_data.rs`. 316 lib tests pass.
Commit: `d8a1886 fixup! Phase 2d: align generator emit code with Phase 2c runtime traits`.

---

### Issue 7: `KafkaError::from_code` collapses many producer-relevant codes to `UnknownServer`

- **File:** `src/common/errors.rs:340–367`
- **Severity:** MINOR (carry-over from Phase 1; surfaces as a Phase-2 latent
  issue because `Errors::exception()` uses this)
- **Java reference:** `Errors.java` — every wire code maps to a distinct
  `ApiException` subclass, several of which the producer's `Sender`
  treats specially: `KafkaStorageException` (56, retriable),
  `NotEnoughReplicasException` (19, retriable),
  `NotEnoughReplicasAfterAppendException` (20, retriable),
  `MessageTooLargeException` (10 → already handled in
  `KafkaError::RecordTooLarge`), `InvalidRequiredAcks` (21).
- **What Rust does:** Codes 19, 20, 21, 56, 76 (UnsupportedCompressionType),
  88 (UnstableOffsetCommit), 100 (UnknownTopicId), and many others all
  collapse to `KafkaError::UnknownServer` in `from_code`. The
  `Errors::exception()` API surface looks complete, but
  `KafkaError::is_retriable` will return *false* for all of them — wrong
  for at least 19/20/56/100.
- **Why it matters:** When Phase 4's `Sender` calls
  `error.is_retriable()` to decide whether to re-enqueue, every
  `NotEnoughReplicas` partition response will be treated as a fatal error
  instead of being retried.
- **Suggested fix:** Phase 1 work, but Phase 2 should at minimum extend
  `KafkaError` (and `from_code`) to cover wire codes returned by the APIs
  Phase 2 wires up: 19, 20, 21, 56, 100. Or document explicitly that the
  collapse is intentional and Phase 4 will wire dedicated variants.

**Fix applied:** Added five wire-code variants — `NotEnoughReplicas` (19,
retriable), `NotEnoughReplicasAfterAppend` (20, retriable),
`InvalidRequiredAcks` (21, non-retriable per Java's
`InvalidConfigurationException`), `KafkaStorage` (56, retriable per
`InvalidMetadataException`), `UnknownTopicId` (100, retriable per
`InvalidMetadataException`). `is_retriable`, `code`, `java_class_name`,
`message`, and `from_code` all updated. Existing retriable-set and
from-code round-trip tests extended.
Commit: `3c352ae fixup! Phase 2e: KafkaError::InvalidRequest + ApiKey header-version helpers`.

---

## NIT

### Issue 9: `MetadataResponse::api_key()` panics on a known-good lookup

- **File:** `src/common/requests/metadata_response.rs:159` (and similarly
  `produce_response.rs:68`, `api_versions_response.rs`)
- **Severity:** NIT
- **Description:** `ApiKeys::for_id(3).expect("METADATA")`. `for_id`
  always succeeds for a wired-in API; functional impact is zero. But
  CLAUDE.md rule 10.1 says "avoid panic for public API". A compile-time
  lookup (`const METADATA: &ApiKey = &ALL_API_KEYS[3];` if order is stable,
  or a `OnceLock`) would remove the panic from the public API surface
  without hurting performance.

**Fix applied:** Each affected `api_key()` method now uses a
function-local `OnceLock<&'static ApiKey>` cache keyed on the API id.
Updated: `MetadataResponse`, `MetadataRequest`, `ProduceRequest`,
`ProduceResponse`, `ApiVersionsRequest`, `ApiVersionsResponse`. The
panic that would have fired from a public-API entry point if the
catalogue were misaligned with the API id is now gone — `expect()` is
inside the `OnceLock` initializer, which is unreachable from the public
boundary.
Commit: `72da60b fixup! Phase 2e: requests module — wrapper layer over generated *Data structs`.
