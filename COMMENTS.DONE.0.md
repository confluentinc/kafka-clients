# Round 1

## Issue from COMMENTS.0.md - Issue 1: MessageTest.java tests translated and Message trait implemented

**Original Issue**: MessageTest.java tests not translated (DoD #3 and #4 violation) - Generated message types did not implement the Message trait, and none of the 22 MessageTest.java tests were translated.

**Resolution**: 
1. Generated message structs now implement the `Message` trait with `read()`, `write()`, `size()`, `add_size()`, `unknown_tagged_fields()`, and `duplicate()`.
2. Generated structs implement `PartialEq`, `Eq`, `Hash`, and `Display`.
3. Builder setters (`set_*`) and `unknown_tagged_fields_mut()` added to all generated types.
4. `MessageUtil::to_byte_buffer_accessor` helper implemented.
5. 14 tests from MessageTest.java translated and passing, covering: round-trip serialization, duplication, version handling, unknown tagged fields, default values, builder patterns.
6. Tagged field read/write support for all primitive types (Float64, Bytes, Records, arrays of primitives).

# Round 2

## Issue from COMMENTS.0.md - Issue 1: add_size performs full write instead of arithmetic

**Original Issue**: Generated `add_size` allocated a 4096-byte buffer and performed a full `write()` to measure size.

**Resolution**: Generator now emits proper arithmetic size computation matching Java `addSize()`, summing field sizes without allocating buffers.

## Issue from COMMENTS.0.md - Issue 2: 22 of 36 MessageTest.java tests not translated

**Original Issue**: Only 14 of 36 MessageTest.java tests were translated.

**Resolution**: Translated 19 additional tests (33 total). 3 tests explained as not translatable:
- testDefaultValues / testNonIgnorableFieldWithDefaultNull: require per-field version validation (UVE for non-default values at unsupported versions) which generator validates at entry instead
- testWriteNullForNonNullableFieldRaisesException: prevented at compile time in Rust via Option<T> types

## Issue from COMMENTS.0.md - Issue 3: test_message_versions is a no-op

**Original Issue**: Rust test only checked `latest >= oldest` on ApiKeys (trivially true).

**Resolution**: Added spot-check assertions verifying generated message HIGHEST_SUPPORTED_VERSION covers ApiKeys latest_version for specific message types (Produce, Fetch, Metadata, etc.). Added comment explaining full test requires ApiMessageType with new_request()/new_response().

## Issue from COMMENTS.0.md - Issue 4: Nullable fields as String instead of Option<String>

**Original Issue**: Fields with `nullableVersions` in JSON specs were generated as `String` instead of `Option<String>`, losing null/empty distinction.

**Resolution**: Generator now checks `nullableVersions` field spec and emits `Option<String>` / `Option<Vec<u8>>` for nullable fields. Read/write code handles null encoding (varint 0 for flexible, -1 length for non-flexible).

## Issue from COMMENTS.0.md - Issue 5: Latent compile error for Array(Float64) Hash

**Original Issue**: Hash generation for float64 used `self.field.to_bits().hash(state)` which fails for `Vec<f64>`.

**Resolution**: Array(Float64) case now iterates elements: `for elem in &self.field { elem.to_bits().hash(state); }`.

## Issue from COMMENTS.0.md - Issue 6: test_unknown_tagged_fields missing non-flexible version error test

**Original Issue**: Test only verified writing succeeds on flexible version 6, missing negative test for non-flexible versions.

**Resolution**: Added `verify_write_raises_uve` helper and assertion that writing CreateTopicsRequestData with unknown tagged fields at version 2 (non-flexible) returns an error containing "Tagged fields were set".
# Round 3

## Issue from COMMENTS.0.md - Issue 2: Tagged primitive/struct/UUID fields always written regardless of default

**Original Issue**: get_default_check only handled String and Array types. All other tagged field types (Bool, Int8-64, Uint16/32, Float64, Uuid, Struct) were always written even when at their default value, producing different wire formats from Java.

**Resolution**: get_default_check now generates proper default-value comparisons for ALL tagged field types:
- Bool/Int/Uint: compare against spec default (e.g., `self.my_int16 != 123`)
- Float64: use `to_bits()` comparison (e.g., `self.my_float64.to_bits() != 12.34f64.to_bits()`)
- Uuid: compare against parsed default (e.g., `self.tagged_uuid != Uuid::from_string("...")`)
- Struct: compare against `StructName::new()` using PartialEq
- Bytes/Records: use `is_empty()` check
Both generate_tagged_field_write and add_size now use the same get_default_check function for consistency.

## Issue from COMMENTS.0.md - Issue 3: UUID default values not parsed from JSON spec

**Original Issue**: get_default_value did not handle FieldType::Uuid with string defaults, falling through to Uuid::zero().

**Resolution**: Added FieldType::Uuid branch in the string-parsing section of get_default_value that generates `Uuid::from_string("...").expect("invalid UUID default")`.

## Issue from COMMENTS.0.md - Issue 4: Uuid::cmp uses unsigned comparison but Java uses signed

**Original Issue**: Uuid::cmp used unsigned u64 comparison but Java Uuid.compareTo() uses signed long comparison, producing wrong ordering for ~50% of random UUIDs.

**Resolution**: Changed Ord implementation to cast u64 to i64 before comparison, matching Java signed ordering. Added test_compare_uuids_signed to verify correct behavior for UUIDs with high bits set.

## Issue from COMMENTS.0.md - Issue 5: Missing UuidTest.testHashCode test

**Original Issue**: Java testHashCode test was completely absent from Rust translation.

**Resolution**: Added test_hash_code verifying hash consistency (equal UUIDs produce equal hashes, different UUIDs produce different hashes). Adapted since Rust uses derive(Hash) instead of Java custom hashCode.

## Issue from COMMENTS.0.md - Issue 6: Missing UuidTest.testRandomUuid test

**Original Issue**: Java testRandomUuid (100 iterations) was completely missing.

**Resolution**: Added test_random_uuid with 100 iterations verifying random UUIDs are not ZERO_UUID, not METADATA_TOPIC_ID, and dont

## Issue from COMMENTS.0.md - Issue 4: Uuid::cmp uses unsigned comparison but Java uses signed

**Original Issue**: Uuid::cmp used unsigned u64 comparison but Java Uuid.compareTo uses signed long comparison, producing wrong ordering for about 50% of random UUIDs.

**Resolution**: Changed Ord implementation to cast u64 to i64 before comparison, matching Java signed ordering. Added test_compare_uuids_signed to verify correct behavior for UUIDs with high bits set.

## Issue from COMMENTS.0.md - Issue 5: Missing UuidTest.testHashCode test

**Original Issue**: Java testHashCode test was completely absent from Rust translation.

**Resolution**: Added test_hash_code verifying hash consistency (equal UUIDs produce equal hashes, different UUIDs produce different hashes). Adapted since Rust uses derive(Hash) instead of Java custom hashCode.

## Issue from COMMENTS.0.md - Issue 6: Missing UuidTest.testRandomUuid test

**Original Issue**: Java testRandomUuid (100 iterations) was completely missing.

**Resolution**: Added test_random_uuid with 100 iterations verifying random UUIDs are not ZERO_UUID, not METADATA_TOPIC_ID, and do not start with a dash.

## Issue from COMMENTS.0.md - Issue 7: Missing UuidTest.testCompareUuids test

**Original Issue**: Java testCompareUuids testing all 9 comparison combinations was missing.

**Resolution**: Added test_compare_uuids verifying all 9 comparison combinations of UUIDs with exact Ordering assertions.

## Issue from COMMENTS.0.md - Issue 8: Missing ByteBufferAccessorTest error message assertions

**Original Issue**: Rust tests only checked is_err(), not the error message content.

**Resolution**: Updated error message format to match Java. Added test_read_array_error_message and test_read_string_error_message translated from Java ByteBufferAccessorTest.

## Issue from COMMENTS.0.md - Issue 9: Missing MessageUtilTest tests

**Original Issue**: Only 1 of 6 Java MessageUtilTest methods was translated.

**Resolution**: Added compare_raw_tagged_fields function and UNSIGNED_SHORT_MAX/UNSIGNED_INT_MAX constants. Added test_compare_raw_tagged_fields and test_constants. Remaining Java tests (testDeepToString, testByteBufferToArray, testDuplicate, testBinaryNode, testInvalidBinaryNode) not translated as the corresponding utility functions are not yet implemented and depend on Java-specific APIs.
