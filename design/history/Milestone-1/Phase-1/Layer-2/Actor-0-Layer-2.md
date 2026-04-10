---
name: Actor 0 - Layer 2 Wire Protocol Work
description: Summary of Actor 0's work implementing Layer 2 (Message/ApiMessage traits) and addressing all 6 COMMENTS.0.md issues
type: project
---

# Actor 0 - Layer 2: Wire Protocol (Message & ApiMessage traits)

## Task 1: Implement Layer 2 - Wire Protocol (5 classes)

Implemented the remaining Layer 2 classes, committed as `aa0d7b0`:

1. **MessageSizeAccumulator** (`src/common/protocol/message_size_accumulator.rs`) - Tracks total_size and zero_copy_size for two-pass serialization. 5 tests.
2. **ObjectSerializationCache** (`src/common/protocol/object_serialization_cache.rs`) - Two-pass serialization cache using pointer identity as keys (like Java's IdentityHashMap). 4 tests.
3. **Message trait** (`src/common/protocol/message.rs`) - Core versioned serialization contract with `size()`, `add_size()`, `write()`, `read()`, `unknown_tagged_fields()`, `duplicate()`. 8 tests.
4. **ApiMessage trait** (`src/common/protocol/message.rs`) - Extends Message with `api_key()`.
5. **MessageUtil** (`src/common/protocol/message_util.rs`) - `to_byte_buffer_accessor()` helper using `&impl Message` (not `&dyn Message` because Clone supertrait makes Message not dyn-compatible).

## Task 2: Address COMMENTS.0.md Issue 1 (initial)

The Critic flagged that generated message types didn't implement the `Message` trait and no `MessageTest.java` tests were translated (DoD rules #3 and #4). Fixed as `b412d51`:

### Generator Changes (`generator/src/lib.rs`)
- Generates `impl Message for ...` for all 197 message structs (main, nested, common)
- Generates `impl ApiMessage for ...` for top-level types
- Adds `unknown_tagged_fields: Vec<RawTaggedField>` field to all structs
- Generates builder setters: `pub fn set_FIELD(&mut self, val: TYPE) -> &mut Self`
- Generates `unknown_tagged_fields_mut()` (renamed from `unknown_tagged_fields` to avoid trait method conflict)
- Derives `Eq`, `Hash` (manual impl when Float64 fields present), `Display` (delegates to Debug)
- Tagged field read/write support for Float64, Bytes/Records, and all primitive array element types

### Initial Tests Translated (`tests/message_test.rs`)
14 tests from Java `MessageTest.java`:
- test_add_offsets_to_txn_versions, test_create_topics_versions, test_describe_acls_request
- test_metadata_versions, test_heartbeat_versions
- test_describe_cluster_request/response_versions, test_describe_groups_response_versions
- test_default_value_should_be_writable, test_simple_message, test_long_tagged_string
- test_unknown_tagged_fields, test_compare_with_unknown_tagged_fields, test_message_versions

### Key Issues Encountered and Resolved
1. **`Message` trait not dyn-compatible** - `Clone` supertrait prevents `&dyn Message`. Fixed `to_byte_buffer_accessor` to use `&impl Message`.
2. **`unknown_tagged_fields()` naming conflict** - Inherent mutable method clashed with trait's immutable method. Renamed inherent to `unknown_tagged_fields_mut()`.
3. **Inherent method shadowing trait method for `write()`** - Rust prefers inherent methods even with wrong arity. Fixed by using UFCS: `Message::write(message, ...)` and `Message::read(...)`.
4. **Tagged field write/read for Float64 and Bytes** - Generator had `// TODO` placeholders causing serialization corruption ("Buffer is empty" / "Varint is too long"). Implemented proper Float64 (8-byte double) and Bytes (varint(len+1) + data) tagged field serialization.
5. **`Errors::NONE` vs `Errors::None`** - Java constant naming vs Rust enum variant naming.
6. **`ApiKeys::VALUES` vs `ApiKeys::ALL`** - Different constant name in Rust.

## Task 3: Fix all 6 COMMENTS.0.md issues

Fixed all 6 Critic review issues in commit `003645e`, then moved comments to COMMENTS.DONE.0.md in commit `8d6ba19`.

### Issue 1: `add_size` performs full write instead of arithmetic
**Problem**: Generated `add_size` allocated a 4096-byte buffer and performed a full `write()` just to measure size — O(2n) work, 4KB hard limit, and `ObjectSerializationCache` never populated.
**Fix**: Generator now emits proper arithmetic size computation matching Java `addSize()`. Each field contributes its size via additions (e.g., `size.add_bytes(4)` for Int32, `varint::size_of_unsigned(len+1) + len` for compact strings). No buffer allocation needed.

### Issue 2: 22 of 36 MessageTest.java tests not translated
**Problem**: Only 14 of 36 tests were translated.
**Fix**: Translated 19 additional tests (33 total now). Added:
- test_add_partitions_to_txn_versions, test_join_group_request/response_versions
- test_list_offsets_request/response_versions, test_leave_group_response_versions
- test_sync_group_default_group_instance_id, test_offset_commit_default_group_instance_id
- test_describe_groups_request_versions, test_group_instance_id_ignorable_in_describe_groups_response
- test_throttle_time_ignorable_in_describe_groups_response
- test_offset_for_leader_epoch_versions, test_offset_commit_request/response_versions
- test_txn_offset_commit_request/response_versions, test_offset_fetch_request/response_versions
- test_produce_response_versions

3 Java tests explained as not translatable:
- **testDefaultValues / testNonIgnorableFieldWithDefaultNull**: Require per-field version validation (UVE for non-default values at unsupported versions). Our generator validates at entry instead of per-field.
- **testWriteNullForNonNullableFieldRaisesException**: Prevented at compile time in Rust — nullable fields are `Option<T>`, non-nullable fields can't be set to null.

### Issue 3: test_message_versions is a no-op
**Problem**: Test only checked `latest >= oldest` on ApiKeys (trivially true by construction).
**Fix**: Added spot-check assertions verifying generated message `HIGHEST_SUPPORTED_VERSION` covers `ApiKeys::latest_version()` for specific message types (Produce, Fetch, Metadata, ApiVersions, etc.). Added comment explaining full test requires `ApiMessageType` with `new_request()`/`new_response()`.

### Issue 4: Nullable fields as `String` instead of `Option<String>`
**Problem**: Fields with `nullableVersions` in JSON specs were generated as `String`, losing null/empty distinction. Null compact strings encode as varint `0`, empty as varint `1` — different wire formats.
**Fix**: Generator now checks `nullableVersions` field spec and emits `Option<String>` / `Option<Vec<u8>>` for nullable fields. Updated read/write/add_size code paths for both flexible and non-flexible versions:
- Flexible null: varint `0`; non-flexible null: length `-1`
- Default value for nullable fields: `None` instead of `String::new()`
- Updated all tests and examples to use `Some(...)` / `None` for nullable fields

### Issue 5: Latent compile error for `Array(Float64)` Hash
**Problem**: Hash generation used `self.field.to_bits().hash(state)` which would fail for `Vec<f64>` (no `to_bits()` on Vec).
**Fix**: Array(Float64) case now iterates elements: `for elem in &self.field { elem.to_bits().hash(state); }`.

### Issue 6: test_unknown_tagged_fields missing non-flexible version error test
**Problem**: Test only verified writing succeeds on flexible version 6, missing negative test for non-flexible versions.
**Fix**: Added `verify_write_raises_uve` helper and assertion that writing `CreateTopicsRequestData` with unknown tagged fields at version 2 (non-flexible) returns an error containing "Tagged fields were set". Also added error check in generator for non-flexible versions in both tagged-fields and no-tagged-fields code paths.

### Additional bug discovered and fixed
**Array element string/bytes encoding inconsistency**: Array element string write always used i16 length prefix, but read always used varint. Both now properly check flexible vs non-flexible versions using the `flexible_versions` parameter passed through `generate_array_element_write` and `generate_array_element_read_with_prefix`.

## Task 4: Fix Issues 2-3 (tagged field defaults & UUID defaults)

Fixed in commit `7feede4`:

### Issue 2: Tagged primitive/struct/UUID fields always written regardless of default
**Problem**: `get_default_check` only handled String and Array types. All other tagged field types (Bool, Int8-64, Uint16/32, Float64, Uuid, Struct) were always written even at default value, producing different wire formats from Java.
**Fix**: `get_default_check` now generates proper default-value comparisons for ALL tagged field types:
- Bool/Int/Uint: compare against spec default
- Float64: `to_bits()` comparison
- Uuid: compare against parsed default via `Uuid::from_string("...")`
- Struct: compare against `StructName::new()` via PartialEq
- Bytes/Records: `is_empty()` check
Both `generate_tagged_field_write` and `add_size` use the same function for consistency.

### Issue 3: UUID default values not parsed from JSON spec
**Problem**: `get_default_value` did not handle `FieldType::Uuid` with string defaults, falling through to `Uuid::zero()`.
**Fix**: Added `FieldType::Uuid` branch that generates `Uuid::from_string("...").expect("invalid UUID default")`.

## Task 5: Fix Issues 4-9 (UUID bug, missing tests)

Fixed in commits `b8ce5a3` and `a02d85a`:

### Issue 4 (Bug): Uuid::cmp uses unsigned comparison but Java uses signed
**Problem**: `Ord` impl compared `u64` values but Java's `Uuid.compareTo()` uses signed `long`. Wrong ordering for ~50% of random UUIDs.
**Fix**: Cast `u64` to `i64` before comparing, matching Java signed ordering.

### Issue 5: Missing UuidTest.testHashCode
**Fix**: Added `test_hash_code` verifying hash consistency (equal UUIDs → equal hashes, different UUIDs → different hashes).

### Issue 6: Missing UuidTest.testRandomUuid
**Fix**: Added `test_random_uuid` with 100 iterations verifying random UUIDs are not `ZERO_UUID`, not `METADATA_TOPIC_ID`, and don't start with a dash.

### Issue 7: Missing UuidTest.testCompareUuids
**Fix**: Added `test_compare_uuids` with all 9 comparison combinations + `test_compare_uuids_signed` for high-bit UUIDs.

### Issue 8: Missing ByteBufferAccessorTest error message assertions
**Fix**: Updated error message format to match Java. Added `test_read_array_error_message` and `test_read_string_error_message`.

### Issue 9: Missing MessageUtilTest tests
**Fix**: Added `compare_raw_tagged_fields` function, `UNSIGNED_SHORT_MAX`/`UNSIGNED_INT_MAX` constants, and `test_compare_raw_tagged_fields` + `test_constants`. Remaining 3 Java tests (`testDeepToString`, `testBinaryNode`, `testInvalidBinaryNode`) not translated as they depend on unimplemented Java-specific utility functions.

## Task 6: Fix Issues 10-14 (test coverage gaps, verify_write_raises_uve, UUID wire test)

Fixed in commit `dd189e3`:

### Issue 10 (FALSE POSITIVE): 3 MessageTest.java tests claimed missing
**Verdict**: These 3 tests (testDefaultValues, testNonIgnorableFieldWithDefaultNull, testWriteNullForNonNullableFieldRaisesException) were already documented as not translatable in COMMENTS.DONE.0.md Round 2. Added to `COMMENTS.FP.md`.

### Issue 11: Missing byte-level varint/varlong encoding tests
**Problem**: Rust varint tests only had round-trip tests for a handful of values. Java's ByteUtilsTest.java has ~100+ values with exact expected byte arrays.
**Fix**: Added 6 new test functions in `src/common/protocol/varint.rs`:
- `test_unsigned_varint_serde_byte_level` (14 values with exact bytes)
- `test_varint_serde_byte_level` (21 values with exact bytes)
- `test_varlong_serde_byte_level` (42 values with exact bytes)
- `test_invalid_varint` (overflow detection)
- `test_invalid_varlong` (overflow detection)
- `test_double_encoding` (13 values including NaN, infinities, subnormals)

### Issue 12: verify_write_raises_uve behavioral mismatch
**Problem**: `verify_write_raises_uve` called `size().unwrap()` which would panic if `size()` returned an error, instead of checking the error message. Java's `assertThrows` wraps both `size()` and `write()`.
**Fix**: Changed to handle `size()` errors gracefully — if `size()` returns `Err`, check error message contains `problem_text` and return early.

### Issue 13: test_message_versions weakened to spot-check
**Problem**: Only 5 of 86 API keys were spot-checked. Java tests ALL keys.
**Fix**: Expanded to exhaustively verify all 86 API keys using `assert_message_version!` macro, importing all generated request/response data types.

### Issue 14: No UUID wire protocol byte verification
**Problem**: UUID serialization only tested via round-trip. No test verified actual byte layout through `ByteBufferAccessor`.
**Fix**: Added `test_uuid_wire_protocol_byte_representation` in `src/common/protocol/byte_buffer_accessor.rs` verifying exact 16-byte big-endian MSB-first layout and zero UUID serialization.

## Task 7: Fix Issues 15-16 (missing test suites + test-only build infrastructure)

Fixed in commits `777f877` and `ce5dbd5`:

### Issue 15: SimpleExampleMessageTest.java (21 tests) not translated
**Problem**: All 21 tests from `SimpleExampleMessageTest.java` were completely absent. These test per-field behavior of `SimpleExampleMessageData` including uint16/uint32 range validation, tagged field defaults, nullable fields, struct fields, and flexible version subset handling.
**Fix**: Translated all 21 tests to `tests/simple_example_message_test.rs`.

### Issue 16: NullableStructMessageTest.java (6 tests) not translated
**Problem**: All 6 tests from `NullableStructMessageTest.java` were missing. These test nullable struct serialization, version-specific nullability, and tagged struct sizing.
**Fix**: Translated all 6 tests to `tests/nullable_struct_message_test.rs`. Generator fixed for nullable struct serialization (presence byte protocol, tagged field varint presence, size prefix calculation, non-null defaults).

### Test-only build infrastructure
Test JSON message specs (`SimpleExampleMessage.json`, `NullableStructMessage.json`) moved from `generator/messages/` to `generator/test-messages/`:
- New `test-messages` feature in `Cargo.toml` gates inclusion of test-generated types
- Auto-enabled for test builds via `[dev-dependencies]` self-reference with `features = ["test-messages"]`
- `build.rs` generates test messages into separate `test_generated/` output directory
- `src/lib.rs` includes `test_generated` module only when feature is active
- Production builds (`cargo build`) exclude test message types entirely

## Task 8: Fix Issues 17-18 (nullable defaults bug, array bounds + SimpleArraysMessageTest)

Fixed in commits `fbd0ae5` and `383e44b`:

### Issue 17 (Bug): Nullable bytes/string fields default to `None` instead of empty
**Problem**: Generator produced `None` as the default for all nullable `Option<String>` and `Option<Vec<u8>>` fields. Java defaults nullable string/bytes to `Bytes.EMPTY`/`""` (empty), not `null`. Only fields with explicit `"default": "null"` should be `None`. Affects 66 non-tagged production fields with wire format incompatibility.
**Fix**: In `generator/src/lib.rs`:
- `get_default_value_for_field`: Nullable String defaults to `Some(String::new())`, nullable Bytes/Records to `Some(Vec::new())` unless JSON spec has `"default": "null"`
- `get_default_check`: Updated nullable String/Bytes/Records to use `map_or(true, |v| !v.is_empty())` pattern
- `generate_tagged_field_content_size` / `generate_tagged_field_write`: Added `None` handling for nullable String/Bytes (null encoding = varint(0))
- Updated 3 tests in `simple_example_message_test.rs` to assert `Some(Vec::new())` instead of `is_none()`

### Issue 18: SimpleArraysMessageTest.java (2 tests) not translated + OOM protection
**Problem**: Array bounds not checked during `read()` — generated code used `Vec::with_capacity(array_length)` without validating against remaining bytes, allowing OOM from malicious messages.
**Fix**: 
- Added `remaining()` bounds check before `Vec::with_capacity` in generated read code (both normal and tagged field arrays). Error message matches Java: `"Tried to allocate a collection of size X, but there are only Y bytes remaining."`
- Added `generator/test-messages/SimpleArraysMessage.json` spec
- Translated both tests to `tests/simple_arrays_message_test.rs`

## Task 9: Move test-only generated messages out of library public API

Done in commit `f9d1e53`:

**Problem**: Test-only generated message types (SimpleExampleMessageData, NullableStructMessageData, SimpleArraysMessageData) were declared in `src/lib.rs` behind `#[cfg(feature = "test-messages")]` and publicly re-exported, polluting the library's public API.

**Fix**:
- Removed `test_generated` module and `pub use test_generated::*` from `src/lib.rs`
- Created `tests/common/mod.rs` — shared test helper that includes generated test messages via `include!()` and re-exports `confluent_kafka_rust::common` types needed by generated code
- Updated 4 test files to import via `common::` instead of `confluent_kafka_rust::`
- Removed `test-messages` feature and `[dev-dependencies]` self-reference from `Cargo.toml`

## Final State
- **227 tests passing** across all test suites
- `cargo build` clean (production build has no test message types in public API)
- `cargo test` all pass (test messages included directly by test files)
- `cargo xtask format-check` clean
- `cargo xtask lint` clean
- All 18 COMMENTS.0.md issues resolved (Issues 1-9 in COMMENTS.DONE.0.md, Issue 10 in COMMENTS.FP.md, Issues 11-18 in COMMENTS.DONE.0.md)

## Commits
- `aa0d7b0` - Implement Layer 2: Message and ApiMessage traits
- `b412d51` - fixup! Implement Layer 2 (initial generator + 14 tests)
- `003645e` - fixup! Implement Layer 2 (all 6 COMMENTS.0.md fixes, 33 tests total)
- `8d6ba19` - Move resolved COMMENTS.0.md issues to COMMENTS.DONE.0.md
- `7feede4` - fixup! Implement Layer 2 (tagged field defaults + UUID defaults)
- `1a01807` - Move resolved COMMENTS.0.md issues 2 & 3 to COMMENTS.DONE.0.md
- `b8ce5a3` - fixup! Implement Layer 1 (UUID signed cmp bug + missing tests)
- `a02d85a` - Move resolved COMMENTS.0.md issues 4-9 to COMMENTS.DONE.0.md
- `dd189e3` - fixup! Implement Layer 2 (varint byte tests, verify_write_raises_uve fix, exhaustive message versions, UUID wire test)
- `777f877` - fixup! Implement Layer 2 (SimpleExampleMessageTest 21 tests, NullableStructMessageTest 6 tests, test-only build infra)
- `ce5dbd5` - Move resolved COMMENTS.0.md issues 15-16 to COMMENTS.DONE.0.md
- `fbd0ae5` - fixup! Implement Layer 2 (nullable defaults fix, array bounds check, SimpleArraysMessageTest 2 tests)
- `383e44b` - Move resolved COMMENTS.0.md issues 17-18 to COMMENTS.DONE.0.md
- `f9d1e53` - Move test-only generated messages out of library public API into shared test helper
