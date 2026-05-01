# Phase 3a Review — Critic N=0

## Round 2 verdict: APPROVED

Round 2 covered fixup commits `c9d89f9`, `1fdfb8c`, `9fa1f99`, `041ffca`,
`1f965ec`, `7cef0e1` resolving Issues 1–4. Re-ran DoD checks:

- `cargo build --lib` — clean, no warnings.
- `cargo test --lib` — `test result: ok. 350 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.02s` (was 349, +1 zero-copy assertion test).
- `cargo xtask format-check` — clean.
- `cargo xtask lint` — clean.
- `cargo xtask check-generated` — clean.

Per-issue verification:
- Issue 1: `record_batch.rs:170` — `offset_of_max_timestamp(&self)` no longer takes a `BufferSupplier` arg; local `BufferSupplier::create()` instantiated at line 178 and dropped at end of for-loop scope. Mirrors Java try-with-resources. No callsite passes the old arg.
- Issue 2: `simple_record.rs` — storage is `Option<Bytes>`; canonical `new` takes `Option<Bytes>` (zero-copy); `new_from_slice` documented as the copying convenience; `from_arcs` removed; `new_with_bytes_is_zero_copy` test (line 247) asserts `p_in == p_out`; `new_from_slice_copies` (line 259) asserts `p_in != p_out`. `bytes = "1"` direct dep in Cargo.toml — popular crate (justified per CLAUDE.md rule 1.2). `Record` trait return type unchanged (`Option<&[u8]>`) — no contract leak into Phase 3c/3d.
- Issue 3: `phase3a_record_base.md` — explicit deferral note for `recordKey()` citing `MemoryRecordsBuilder.java:614` and Phase 3c.
- Issue 4: `records.rs:91-99` — docstring on `slice` cites `IllegalArgumentException` and CLAUDE.md rule 10.

No new regressions. `BufferSupplier` is still legitimately used in `streaming_iterator` (parameter required, matches Java). DEFERRED-OK items 5–8 below remain tracked for Phase 3c–3d/Phase 5.

---

Scope: 4 commits since `6a5ddfe` (`0721e86`, `147a328`, `f2e9ded`, `fd21a99`).

DoD checks I re-ran (all green):
- `cargo build --lib` — finished in 13.81s, no warnings
- `cargo test --lib` — `test result: ok. 349 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.01s`
- `cargo xtask format-check` — all formatted
- `cargo xtask lint` — no clippy issues

Java vs. Rust comparisons against `kafka/clients/src/main/java/org/apache/kafka/common/record/*.java`.

Numbering follows Phase 2 convention. Severity: **BLOCKER / MAJOR / MINOR / DEFERRED-OK**.

Issues 1, 2, 3, 4 have been resolved and moved to `COMMENTS.DONE.0.md`.
This file retains only the **DEFERRED-OK** items (5–8): they require no
code change and are tracked here so future phases pick them up at the
correct phase boundary.

---

## 5. `BaseRecords::to_send` deferred — DEFERRED-OK

- **Severity:** DEFERRED-OK
- **File:** `src/common/record/base_records.rs`
- **Java reference:** `BaseRecords.java:33` — `RecordsSend<? extends BaseRecords> toSend();`

Java's `BaseRecords` interface has two methods. The Rust trait keeps only
`size_in_bytes()`; `to_send()` is deferred until Phase 3d adds
`RecordsSend`. The actor's docstring on the trait calls this out
explicitly. Acceptable per the actor-direction note that
`RecordsSend`/`DefaultRecordsSend` are 3d concerns.

---

## 6. `TransferableRecords::write_to` deferred — DEFERRED-OK

- **Severity:** DEFERRED-OK
- **File:** `src/common/record/transferable_records.rs`
- **Java reference:** `TransferableRecords.java:38`

`TransferableChannel` is a Phase 5 (network) concern. Deferring with a
docstring note is correct per CLAUDE.md DoD #7 (no inventing new types
ahead of their phase).

---

## 7. `Records::records()` is required (no default) — DEFERRED-OK

- **Severity:** DEFERRED-OK
- **File:** `src/common/record/records.rs:85`
- **Java reference:** `AbstractRecords.java:64-66, 73-91`

Java's `AbstractRecords` provides a default chained iterator over batches.
Rust's `Records::records()` is a required method without a default. The
actor's docstring explains why (self-referential per-batch iterator
requires the concrete batch type to express). Concrete Phase 3d types
(`MemoryRecords`, `FileRecords`) will provide their own. Acceptable for
a trait surface phase.

---

## 8. `CompressionType::levelValidator` deferred — DEFERRED-OK

- **Severity:** DEFERRED-OK
- **File:** `src/common/record/compression_type.rs`
- **Java reference:** `CompressionType.java:54-63, 95-97, 127-129, 188-190`

`level_validator()` returns a `ConfigDef.Validator` lambda in Java,
used by `CommonClientConfigs`/`ProducerConfig` to enforce per-codec
level ranges at config-parse time. The actor deferred this to Phase 3c
(documented in `phase3a_compression_dispatch_gap.md`) when the
`Compression` codec dispatch lands. The level constants
(`*_MIN_LEVEL`, `*_MAX_LEVEL`, `*_DEFAULT_LEVEL`) are already in place
so the Phase 3c addition is straightforward. Acceptable.

---

## Verified clean (no issue)

The following review items were checked and found correct:

- **Enum codes match Java exactly:**
  `CompressionType` ids (NONE=0, GZIP=1, SNAPPY=2, LZ4=3, ZSTD=4),
  `TimestampType` ids (NO_TIMESTAMP_TYPE=-1, CREATE_TIME=0,
  LOG_APPEND_TIME=1), `RecordVersion` magic bytes (V0=0, V1=1, V2=2),
  `ControlRecordType` type ids (ABORT=0, COMMIT=1, LEADER_CHANGE=2,
  SNAPSHOT_HEADER=3, SNAPSHOT_FOOTER=4, KRAFT_VERSION=5, KRAFT_VOTERS=6,
  UNKNOWN=-1).
- **Per-codec compression levels match Java:** GZIP (1, 9, -1), LZ4 (1,
  17, 9), ZSTD (-131072, 22, 3).
- **`MutableRecordBatch: RecordBatch`** supertrait is correctly
  declared (`mutable_record_batch.rs:26`).
- **`Records: TransferableRecords`** supertrait is correctly declared
  (`records.rs:55`).
- **`TransferableRecords: BaseRecords`** supertrait is correctly
  declared (`transferable_records.rs:35`).
- **No legacy translation:** `AbstractLegacyRecordBatch.java`,
  `LegacyRecord.java`, `EndTransactionMarker.java`,
  `ControlRecordUtils.java`, `MemoryRecords.java`,
  `MemoryRecordsBuilder.java`, `FileRecords.java`, `FileLogInputStream.java`,
  `RemoteLogInputStream.java`, `DefaultRecord.java`, `DefaultRecordBatch.java`,
  `DefaultRecordsSend.java`, `MultiRecordsSend.java`,
  `PartialDefaultRecord.java`, `RecordBatchIterator.java`,
  `UnalignedFileRecords.java`, `UnalignedMemoryRecords.java`,
  `UnalignedRecords.java`, `RecordValidationStats.java`, and
  `CompressionRatioEstimator.java` are correctly absent from the diff.
- **Apache 2.0 / Confluent Inc. headers** present on all 13 new files.
- **Magic-byte / sentinel constants** in `record_batch.rs` match Java
  (`MAGIC_VALUE_V0..V2`, `CURRENT_MAGIC_VALUE`, `NO_TIMESTAMP=-1`,
  `NO_PRODUCER_ID=-1`, `NO_PRODUCER_EPOCH=-1`, `NO_SEQUENCE=-1`,
  `NO_PARTITION_LEADER_EPOCH=-1`).
- **`Records` length constants** match Java (`OFFSET_OFFSET=0`,
  `OFFSET_LENGTH=8`, `SIZE_OFFSET=8`, `SIZE_LENGTH=4`,
  `LOG_OVERHEAD=12`, `MAGIC_OFFSET=16`, `MAGIC_LENGTH=1`,
  `HEADER_SIZE_UP_TO_MAGIC=17`).
- **`ControlRecordTypeTest` translation** covers all three Java
  scenarios (`testParseUnknownType`, `testParseUnknownVersion`,
  `testRoundTrip` over every variant including UNKNOWN — Java's
  `@EnumSource` with no `mode=EXCLUDE` does include UNKNOWN). Two
  bonus error-path tests added.
- **`parseTypeId` byte order:** Java's `ByteBuffer` defaults to
  big-endian; Rust uses `i16::from_be_bytes`. Match.
- **No unrelated standalone Java tests for the four enum modules**:
  `ls kafka/clients/src/test/java/.../record/` confirms only
  `ControlRecordTypeTest.java` exists in the four-enum scope.
- **`mod.rs` re-exports** follow the Phase 2 pattern: each translated
  type and trait is re-exported at the parent module level so
  consumers can `use crate::common::record::Foo;`.
- **`AbstractRecordBatch` and `AbstractRecords`** correctly collapse
  Java's abstract-class default bodies into trait default methods
  (`has_producer_id`, `next_offset`, `is_compressed`,
  `last_batch`, `has_matching_magic`); the documentation files preserve
  the file-per-class mapping required by CLAUDE.md rule 2.

---

Round 1 verdict: **needs minor fixes**. Two MAJOR (#1 contract divergence, #2 zero-copy regression), one MINOR-deferral that should be tracked (#3), one MINOR design call to confirm (#4), and four DEFERRED-OK items (#5–8).

Round 1 outcome: Issues 1–4 resolved by fixup commits `9fa1f99`
(Issue 1), `c9d89f9` + `1fdfb8c` (Issue 2), `1f965ec` (Issue 3),
`041ffca` (Issue 4); see `COMMENTS.DONE.0.md` for the resolutions.
DEFERRED-OK items 5–8 retained here as tracking records for later
phases.

---

## Phase 3b Round 2 verdict: APPROVED

Round 2 covered fixup commit `5dd0a73` (drop `NULL_ENTRY_VALUE` re-export
from `src/common/serialization/mod.rs`) and `a78894` (rotate Issue 9 to
DONE). Re-ran DoD checks:

- `cargo build` — clean, no warnings.
- `cargo test --lib` — `test result: ok. 416 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.01s`.
- `cargo xtask format-check` — clean.
- `cargo xtask lint` — clean.
- `cargo xtask check-generated` — clean.

Per-issue verification:
- Issue 9: `src/common/serialization/mod.rs` — `NULL_ENTRY_VALUE` token
  removed from `pub use list_serializer::{...}` line; nothing else
  changed in the fixup (1 file, 1 line modified). `grep -rn
  "NULL_ENTRY_VALUE" src/` confirms the only callsite outside its
  defining file is `list_deserializer.rs:21` which imports from
  `crate::common::serialization::list_serializer::{...}` (the defining
  file path), per CLAUDE.md rule 2. No drift.

`COMMENTS.DONE.0.md` has Issue 9 with a Resolution paragraph citing
`5dd0a73`.

---

# Phase 3b Review — Critic N=0

Scope: 2 commits since `7cef0e1` — `284528a` (serialization module) and
`1b46ef3` (tests + actor-memory notes). Issue numbering continues from 9.

DoD checks I re-ran (all green):
- `cargo build` — finished in 14.64s, no warnings.
- `cargo test --lib` — `test result: ok. 416 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.01s` (was 350 in Phase 3a, +66 new tests).
- `cargo xtask format-check` — clean.
- `cargo xtask lint` — clean.
- `cargo xtask check-generated` — clean.

## Phase 3b Round 1 verdict: APPROVED with one MINOR

The Phase 3b serialization translation is correct, faithful to Java's
wire format, and the deferrals (Headers overload, ListSerde class-FQN,
StringSerializer/UUIDSerializer charset) are all well documented in
actor-memory notes. One single MINOR rule violation was noted (Issue 9,
now resolved and moved to `COMMENTS.DONE.0.md`).

### What I verified

- **Wire-format byte-level fidelity** for every primitive vs. Java:
  - Integer (BE 4 bytes), Long (BE 8 bytes), Short (BE 2 bytes),
    Float (BE 4 bytes via `to_bits().to_be_bytes()` — matches Java's
    `floatToRawIntBits` + manual BE shift), Double (BE 8 bytes via
    `to_bits().to_be_bytes()`), Boolean (1 byte 0x00/0x01),
    ByteArray (identity), Bytes (identity over `.as_ref()`),
    ByteBuffer (Vec<u8> identity, documented Java-`ByteBuffer.flip()`
    discipline).
  - **UUID is correct** (the common mistake): Rust does
    `u.hyphenated().to_string()` → bytes (uses 36-char text form, NOT
    16-byte binary). UUIDDeserializer parses via `Uuid::parse_str`.
    Matches Java's `data.toString().getBytes(encoding)` /
    `UUID.fromString(new String(data, encoding))`.
  - String defaults to UTF-8, configurable via
    `key.serializer.encoding`/`value.serializer.encoding`/`serializer.encoding`.
    Implemented (UTF-8, UTF-16, UTF-16BE, UTF-16LE) — wider than Java's
    encoding registry but the configure-time error path matches
    (`SerializationException("Unsupported encoding ...")`).
  - Void: serialize always `Ok(None)`, deserialize errors on non-null.
- **Float NaN preservation**: `to_bits()`/`from_bits()` round-trip
  preserves the raw bit pattern (test
  `float_serde_should_preserve_nan_values` covers `0x7f80_0001` and
  `0x7f80_0002`).
- **Error message text** matches Java exactly:
  - `"Size of data received by IntegerDeserializer is not 4"`
  - `"Size of data received by LongDeserializer is not 8"`
  - `"Size of data received by ShortDeserializer is not 2"`
  - `"Size of data received by Deserializer is not 4"` (Float)
  - `"Size of data received by Deserializer is not 8"` (Double)
  - `"Size of data received by BooleanDeserializer is not 1"`
  - `"Unexpected byte received by BooleanDeserializer: {b}"`
  - `"Data should be null for a VoidDeserializer."`
  - `"Unsupported encoding {name}"`
  - `"Error parsing data into UUID"` (UUIDDeserializer)
- **Round-trip + null tests** for all 11 primitives + Boolean:
  `string_round_trip`, `short_round_trip`, `integer_round_trip`,
  `long_round_trip`, `float_round_trip`, `double_round_trip`,
  `byte_array_round_trip`, `byte_buffer_round_trip`,
  `bytes_round_trip`, `uuid_round_trip` plus `*_supports_null`
  variants. Boolean uses dedicated `boolean_serializer_true/false` +
  `boolean_deserializer_true/false` (Java's `@ParameterizedTest`
  expansion).
- **Wire-byte fixtures** (CLAUDE.md DoD #3): `integer_wire_bytes`,
  `long_wire_bytes`, `short_wire_bytes`, `boolean_wire_bytes`,
  `float_wire_bytes`, `double_wire_bytes`, `byte_array_wire_bytes`,
  `uuid_serializer_produces_36_bytes_utf8`. All hand-computed and
  verified against Java's documented BE format.
- **Hot-path zero-copy** (CLAUDE.md rule 12): every concrete
  `Serializer` overrides `serialize_to(&[u8], &mut Vec<u8>)` to use
  `extend_from_slice` / `push` / `out.put_slice`-equivalent — no
  intermediate `Vec`. Verified by `integer_serialize_to_writes_directly`,
  `integer_serialize_to_null_writes_nothing`, `long_serialize_to_appends`.
- **`ListSerializer`/`ListDeserializer` round-trip** for all primitive
  inner types + String. Byte-count fixtures (`*_byte_count_is_*`)
  verify the 1-byte strategy + 4-byte null-count + 4-byte size +
  N×element-size layout matches Java exactly. Null-entry tests cover
  both `ConstantSize` (null index list) and `VariableSize`
  (`NULL_ENTRY_VALUE` per-entry sentinel).
- **Skipped Java tests**: `ListSerializerTest.java` (10 tests,
  all `*NoArgConstructors*` / `*ClassNotFound*` — runtime
  `Class.forName` plumbing) and `ListDeserializerTest.java` (similar
  18 tests) are correctly deferred. Verified by reading each Java test
  file: every test exercises the `configure(Map)` reflection path. The
  two `assertInstanceOf(LinkedList.class, ...)` /
  `assertInstanceOf(Stack.class, ...)` tests in `SerializationTest`
  are also correctly skipped (Rust has no list-class reflection). The
  *wire-format* round-trip cases that DO translate are covered by
  Rust's `list_serde_*` tests, which faithfully replicate every
  applicable Java `SerializationTest.listSerde*` scenario.
- **`Serializer` / `Deserializer` trait shape** matches the Java
  contract minus the `Headers` overload. The Headers-omission is
  correctly justified (`Headers` trait has `&mut Self` builder methods
  that prevent dyn-compatibility) and documented in the trait doc and
  in `phase3b_serializer_design.md`. Producer Phase 5 will route
  headers through `ProducerRecord.headers()`, not through the
  serializer — contract preserved at the upper layer.
- **`Serde` trait** correctly exposes `&dyn Serializer<T>` /
  `&dyn Deserializer<T>` so callers don't need the concrete type.
- **`WrapperSerde<T, S, D>` + factory functions** mirror Java's
  `Serdes.WrapperSerde` + `Serdes.Long()` / etc. The
  `serializer_mut`/`deserializer_mut` back doors are a benign Rust
  necessity for the generic-impl path; the canonical `configure` flow
  goes through `Serde::configure` which propagates correctly.
- **Apache 2.0 + Confluent Inc.** license header present on all 31
  new files in `src/common/serialization/`.
- **No new regressions** in Phase 1/2/3a — 350 baseline + 66 new = 416.
- **No TODO/FIXME** in any of the new files. No `unimplemented!()`,
  `todo!()`, or `panic!` calls.
- **Hot-path allocation audit**: zero per-record heap allocations on
  the `serialize_to` paths for primitive serializers. The
  `ListSerializer` `VariableSize` path does allocate per non-null
  entry (intermediate `Vec<u8>` to learn length before writing
  length-prefix); the `ConstantSize` path is allocation-free. The
  variable-size path's allocation is documented in the source as
  "one allocation per non-null entry on the variable path" — and
  fixed-size lists, which are the producer hot-path candidate, avoid
  it. Acceptable for Phase 3b.

---

Issue 9 has been resolved and moved to `COMMENTS.DONE.0.md`.

---

## Verified clean (no issue) for Phase 3b

- **`InnerKind` enum is a justified Rust addition** (not in Java) —
  documented in `phase3b_list_serde_gap.md` as the substitute for
  Java's `inner.getClass()` reflection lookup. Acceptable per
  CLAUDE.md DoD #7.
- **`StringEncoding` enum is a justified Rust addition** — Rust's
  stdlib has no `Charset.forName` registry; the enum captures the
  encodings the SerializationTest exercises (UTF-8, UTF-16, BE/LE
  variants). Wider than strictly needed but harmless.
- **`StringOwnedSerializer` / `ByteArrayOwnedSerializer`** wrappers:
  benign owned-Vec / owned-String adapters for the `Serde<T>` factory
  surface where `T = String` / `Vec<u8>`. The hot-path serializers
  (`StringSerializer<&str>`, `ByteArraySerializer<&[u8]>`) remain
  zero-copy. Documented in source.
- **`Serializer<[u8]>` / `Serializer<str>`** — uses `T: ?Sized` trait
  bound correctly. Both unsized types resolve to `Option<&[u8]>` /
  `Option<&str>` for input (zero-copy reference passing).
- **`Default` impl of `UUIDSerializer` / `StringSerializer`** —
  defaults to UTF-8, matches Java's `StandardCharsets.UTF_8.name()`.
- **UUIDDeserializer error message** "Error parsing data into UUID" —
  matches Java exactly.
- **`SerializationStrategy::ordinal()`** returns a `u8` matching
  Java's `enum.ordinal()` (0 = CONSTANT_SIZE, 1 = VARIABLE_SIZE).
  Correct since `ConstantSize = 0, VariableSize = 1` discriminants.
- **`from_ordinal` error**: "Invalid serialization strategy flag value"
  matches Java's `SerializationException` text exactly.
- **ListDeserializer null handling** for `ConstantSize`: when an entry
  index is in the null-index list, the code does `continue` BEFORE
  calling `read_slice` — no bytes consumed for null entries (matches
  Java's `dis.read(payload)` skip via `continue`).
- **End-of-stream error** "End of the stream was reached prematurely"
  — matches Java's `SerializationException` text exactly.
- **Apache 2.0 / Confluent Inc.** license header present on all
  new files.
- **No `clients` in module path**: `crate::common::serialization::*`
  (Java's `org.apache.kafka.common.serialization.*` correctly mapped).
- **One Java class per Rust file**: every Java
  `org.apache.kafka.common.serialization.X.java` has a corresponding
  `src/common/serialization/x_serializer.rs` (or similar). The only
  exception is `Serdes.java` (which contains 11 inner classes); Rust
  collapses the static `WrapperSerde` + factory functions into
  `serdes.rs`, mirroring Java's "all in one file" structure.
- **Trait method names**: `serialize` / `deserialize` (Rust snake_case
  was already snake_case in Java for these). `serialize_to` is the new
  zero-copy variant.
- **`KafkaError::Serialization` variant** exists in `src/common/errors.rs:136`
  and maps to Java's `org.apache.kafka.common.errors.SerializationException`.

---

Round 1 outcome: APPROVED with one MINOR rule violation (#9). Issue 9
was mechanical (drop one identifier from a `pub use`) and did not
affect correctness; resolved by fixup `5dd0a73` and moved to
`COMMENTS.DONE.0.md`. No BLOCKER, no MAJOR.

---

# Phase 3c Review — Critic N=0

Scope: 1 commit since `a78894` — `fe88cbd` (compression module). Issue
numbering continues from 11 (10 reserved/unused).

DoD checks I re-ran (all green):
- `cargo build --lib` — clean, no warnings.
- `cargo test --lib` — `test result: ok. 446 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.25s` (was 416 in Phase 3b, +30 Phase 3c tests; matches actor's claim).
- `cargo xtask format-check` — `All code is properly formatted!`
- `cargo xtask lint` — `No lint issues found!`
- `cargo xtask check-generated` — `All generated code is properly formatted!`

## Phase 3c Round 1 verdict: APPROVED with deferrals

The Phase 3c compression translation is correct, faithful to Java's wire
format for LZ4, and the two deferrals (Snappy xerial framing, LZ4 level
knob) are well documented. No BLOCKER, no MAJOR.

### What I verified — LZ4 framing fidelity (HIGHEST PRIORITY)

I read both `Lz4BlockOutputStream.java` and `Lz4BlockInputStream.java`
line-by-line against the Rust translations:

- **Magic** `0x184D2204` little-endian → `[0x04, 0x22, 0x4D, 0x18]` on
  disk: matches (`lz4_block_output_stream.rs:39`,
  `writes_header_with_correct_magic` test).
- **FLG byte layout** (bits 0-1 reserved=0, bit 2 contentChecksum, bit 3
  contentSize, bit 4 blockChecksum, bit 5 blockIndependence=1, bits 6-7
  version=1): byte-exact match in `Flg::to_byte` /
  `Flg::from_byte`. Validation rejects `block_independence != 1`,
  `version != 1`, and `reserved != 0` with the same Java strings.
- **BD byte layout** (bits 0-3 reserved2=0, bits 4-6 blockSizeValue 4..7,
  bit 7 reserved3=0): matches in `Bd::to_byte` / `Bd::from_byte`.
  `block_maximum_size = 1 << ((2 * blockSizeValue) + 8)`: matches.
- **HC checksum** = `(XXH32(buf, seed=0) >> 8) & 0xFF`: matches.
  Coverage range = `[FLG..end-of-FD]` (offset 4, len = bufferOffset-4)
  by default; `[magic..end-of-FD]` (offset 0, len = bufferOffset) when
  `useBrokenFlagDescriptorChecksum=true`: matches in
  `lz4_block_output_stream.rs:259-263` and the input-side check in
  `lz4_block_input_stream.rs:210-216`.
- **Per-block layout** `[blockSize:4 LE][data][optional blockChecksum:4 LE]`,
  with high bit `0x80000000` = incompressible, threshold check
  `compressedLength >= bufferOffset` falls back to the raw buffer:
  byte-exact match in `Lz4BlockOutputStream::write_block`
  (`lz4_block_output_stream.rs:283-314`).
- **End-mark** is a single `0u32` LE (`writeEndMark`): matches.
- **XXH32 seed = 0** confirmed by `xxh32_known_vectors` test
  (`xxh32(&[]) == 0x02CC5D05`, the canonical empty-string XXH32 result).
- **Input-side error strings** `PREMATURE_EOS`, `NOT_SUPPORTED`,
  `BLOCK_HASH_MISMATCH`, `DESCRIPTOR_HASH_MISMATCH`: match Java's
  string literals exactly.

The full Java `Lz4ArgumentsProvider` matrix (6 payloads × 2 broken × 2
ignore × 2 blockChecksum × 2 close × 3 levels = 288 combinations) is
unrolled into Rust `for` loops in 8 separate `#[test]` functions
(`header_premature_end`, `not_supported`, `bad_frame_checksum`,
`bad_block_size`, `compression_frame_structure`, `array_backed_buffer`,
`array_backed_buffer_slice`, `skip`). Spot-checked
`compression_frame_structure`: it independently re-computes the XXH32
HC byte from the post-magic bytes and asserts equality — that
duplicate-implementation cross-check is exactly what catches a
mis-applied seed or wrong byte coverage.

### What I verified — codec dispatch and level validation

- `CompressionType::wrap_for_output` / `wrap_for_input` route to all 5
  concrete codecs (no `KafkaError::InvalidRequest` placeholders): match.
- `CompressionType::level_validator()` produces a `Box<dyn Fn(i32) -> Result<…>>`.
  - Gzip: rejects `level < 1 || level > 9` unless `level == -1`.
  - Lz4: rejects `level < 1 || level > 17` strictly.
  - Zstd: rejects `level < -131072 || level > 22` strictly.
  - None / Snappy: any level call errors. Matches Java's
    `IllegalArgumentException` modulo `KafkaError::Config` mapping.
- `GzipCompression::Builder::level(default)` is allowed even though
  `-1` is outside `[1, 9]`: matches Java's `Builder` exception (line 102
  in `GzipCompression.java`).
- `Lz4Compression::Builder::level` does NOT make the same exception
  for `default_level=9` because `9 ∈ [1, 17]` — matches Java.
- `Lz4Compression::wrap_for_output` selects broken-FD checksum iff
  `message_version == MAGIC_VALUE_V0`: matches Java's
  `Lz4Compression.wrapForOutput(buffer, magic)` which mirrors the
  V0/V1+ split.

### What I verified — CompressionRatioEstimator

- The asymmetric step constants in
  `compression_ratio_estimator.rs:30-34`:
  `IMPROVING_STEP = 0.005`, `DETERIORATE_STEP = 0.05`. Java's literals
  match. The `update_estimation` direction is correct: `observed > current`
  uses `DETERIORATE_STEP` (rapid catch-up to bad ratios), `observed <
  current` uses `IMPROVING_STEP` (slow improvement). Both branches use
  `max(_, observed)` to clamp to the observed value — matches Java
  exactly. The `testUpdateEstimation` cases all pass:
  `(0.8, 0.84) → 0.85`, `(0.6, 0.7) → 0.7`, `(0.6, 0.4) → 0.595`,
  `(0.004, 0.001) → 0.004`, all `>= observed`.
- Concurrency model: `DashMap<String, Mutex<[f32; 5]>>` behind a
  `OnceLock`. Java reads `currentEstimation` outside the synchronized
  block (line 44); Rust holds the lock for the whole read-then-write.
  More conservative than Java but functionally equivalent. Acceptable.
- The Rust translation uses unique topic names per test to avoid
  cross-test interference from the static map — necessary because
  Rust unit tests share process state. Documented in
  `update_estimation_test`.

### What I verified — tests

30 new tests, distributed:
- `gzip_compression.rs`: 3 (matches Java's 3:
  `testCompressionDecompression`, `testCompressionLevels`,
  `testLevelValidator`).
- `gzip_output_stream.rs`: 2 (Rust-only roundtrip helpers).
- `lz4_block_output_stream.rs`: 5 (Rust-only header/FLG/BD round-trips).
- `lz4_compression.rs`: 12 (matches Java's 11 modulo `testDirectBuffer`
  elision — see below — plus one extra `testCompressionLevels` split).
- `snappy_compression.rs`: 1 (matches Java's 1:
  `testCompressionDecompression`).
- `zstd_compression.rs`: 2 (matches Java's 2).
- `no_compression.rs`: 2 (1 maps to Java's
  `testCompressionDecompression`, 1 Rust-only sanity check).
- `compression_ratio_estimator.rs`: 3 (matches Java's 1
  `testUpdateEstimation` plus 2 Rust-only initial-rate / reset checks).

Total: 30. **Matches actor claim**.

`testDirectBuffer` elision: Java exercises `ByteBuffer.allocateDirect`
(off-heap). Rust's `&[u8]` covers both heap-backed and direct-equivalent
slices uniformly because Rust has no JNI/heap-vs-direct buffer
distinction. The actor's argument is valid, and `array_backed_buffer_slice`
exercises the non-zero-offset path that was the actual concern in the
Java test. Acceptable.

### What I verified — Cargo.toml and deps

`twox-hash = { version = "2", features = ["xxhash32"] }` is a new
direct dep. `cargo tree | grep twox` confirms it was already pulled in
transitively by `lz4_flex` (so promoting it to a direct dep adds zero
new ELF/object dependency surface). Per CLAUDE.md rule 1.2, this is a
popular Rust crate (xxh3/xxh32 reference impl) so the promotion is
fine. Comment in Cargo.toml lines 32-37 justifies the promotion.

### What I verified — license and rules

- Apache 2.0 / Confluent Inc. headers present on all 11 new
  `src/common/compress/*.rs` files and the new
  `compression_ratio_estimator.rs`.
- One Java class per Rust file: every Java
  `org.apache.kafka.common.compress.X.java` has a corresponding
  `src/common/compress/x_compression.rs` (or `*_stream.rs`).
- `pub` exports in `mod.rs` re-export the trait and concrete codec
  types at the parent module level (CLAUDE.md rule 2 internal-import
  pattern).

### Issues found

None. No BLOCKER, no MAJOR, no MINOR.

The two known deferrals are documented (`phase3c_snappy_framing_gap.md`
and the LZ4 level note in `phase3c_compression.md`) and tracked below.

---

## 11. Snappy uses RFC framing instead of xerial framing — DEFERRED-OK

- **Severity:** DEFERRED-OK (must resolve before Phase 5 broker integration)
- **File:** `src/common/compress/snappy_compression.rs`
- **Java reference:** `kafka/clients/src/main/java/org/apache/kafka/common/compress/SnappyCompression.java` — uses `org.xerial.snappy.SnappyOutputStream` / `SnappyInputStream`.

The `snap` crate (`snap::write::FrameEncoder` / `snap::read::FrameDecoder`)
emits the *standard Snappy framing format* (magic
`0xff 0x06 0x00 0x00 0x73 0x4e 0x61 0x50 0x70 0x59`). Java's xerial
framing uses a different magic
(`0x82, 'S', 'N', 'A', 'P', 'P', 'Y', 0x00, version, compatVersion`)
and different per-block headers.

**Wire-compat impact:** Roundtrip within this client works (the actor's
`testCompressionDecompression` round-trips successfully because writer
and reader use the same Rust framing). However, a Kafka broker reading
a `CompressionType.SNAPPY` batch produced by this client will fail to
decode the snappy frames, and vice versa.

**Acceptable for Phase 3c** because no producer is yet wired against a
broker (Phase 5). **Must be resolved before Phase 5** — the fix is
either (a) a small xerial-framing wrapper around `snap::raw::Encoder` /
`Decoder` (xerial framing is fully documented and ~80 LOC of glue), or
(b) pulling in a maintained xerial-snappy crate when one becomes
available. Tracked in `phase3c_snappy_framing_gap.md`.

---

## 12. LZ4 level parameter is validated but never lowered into the compressor — DEFERRED-OK

- **Severity:** DEFERRED-OK (Performance-only; not wire-compat)
- **File:** `src/common/compress/lz4_block_output_stream.rs:201-202`
- **Java reference:** `Lz4BlockOutputStream.java:79` — Java picks
  `fastCompressor()` for the default level and `highCompressor(level)`
  otherwise.

`lz4_flex::block::compress_into` always uses `LZ4_compress_default`,
so the `level` field on `Lz4BlockOutputStream` is recorded for API
parity but never affects compression strength. All LZ4 levels produce
decompressible output (level is a quality-vs-speed knob, not a framing
change), so this is **not a wire-compat issue** — Java brokers will
decode our LZ4 batches correctly regardless of the level we requested.

**Behavioural divergence:** Java users who set `compression.lz4.level=17`
expect a smaller payload than the default; Rust users will get a
default-level payload. Documented in
`lz4_block_output_stream.rs:198-202` with a clear `#[allow(dead_code)]`
note. **Acceptable for Phase 3c** because the level constants validate
correctly (CLAUDE.md DoD #2 — the public API contract is honored even
if the underlying compressor doesn't honor the level). A future fix is
to either swap to a level-aware crate or call into `lz4-sys` for the
high-compressor path.

---

## Verified clean (no issue) for Phase 3c

- **`twox-hash` direct dep promotion** is justified — already
  transitive via `lz4_flex` (verified with `cargo tree`).
- **`Compression::of(name)` / `Compression.NONE` static factories**
  not translated. Java's interface has these as `static` methods; Rust
  trait methods cannot be static while remaining object-safe. Callers
  go through `CompressionType::wrap_for_output` for default-level
  dispatch or `<Codec>::Builder::new().level(l).build()` for
  level-aware construction. Documented in
  `phase3c_compression.md`. Acceptable per CLAUDE.md rule on
  Java→Rust idiomatic adaptation.
- **`GzipOutputStream` size parameter** ignored (flate2 has no
  output-buffer-size knob): documented at
  `gzip_output_stream.rs:35-37`. The Rust `Builder` still passes the
  Java `8 * 1024` value for getter parity. Acceptable.
- **`Lz4Compression::wrap_for_input` splits the `BufferSupplier`**
  into two suppliers (one for `Lz4BlockInputStream`, one for the outer
  `ChunkedBytesStream`). Java shares the same supplier across both
  layers. Behavioural difference: cached buffers are not shared between
  layers, so a 64KB LZ4 buffer + a 2KB chunked buffer are pooled
  independently. **Not a correctness bug**, just slightly less buffer
  reuse on the consumer hot path. Documented at
  `lz4_compression.rs:81-95`.
- **`Lz4BlockInputStream` always copies uncompressed blocks** into the
  decompression buffer. Java slices `in` directly to avoid the copy.
  This is a **performance regression** vs. Java for incompressible
  payloads, but lifetime-correct in Rust without resorting to
  unsafe transmute or `Arc<Bytes>` rewriting. Documented at
  `lz4_block_input_stream.rs:42-46`. Acceptable for Phase 3c (consumer
  path); revisit when consumer hot-path benchmarking begins.
- **`CompressionRatioEstimator` uses `DashMap` over `RwLock<HashMap>`**:
  acceptable, `dashmap` is a popular crate and is already a direct dep
  in Cargo.toml from earlier phases. Eliminates the read-write lock
  contention that Java's `ConcurrentHashMap` avoids natively.
- **`std::mem::forget(lz4)` in the test path** (`lz4_compression.rs:376`)
  intentionally leaks the writer's owned scratch buffers (~80KB per
  test case) to prevent `Drop` from writing the end-mark. ~144 cases
  hit this path → ~11MB test-binary leak. Pragmatic and isolated to
  test code.
- **Hot-path zero-copy** (CLAUDE.md rule 12): the producer-side write
  path goes
  `BufWriter (gzip/zstd) → encoder → ByteBufferOutputStream`, which
  writes compressed bytes *directly* into the destination buffer
  (`buffer.write_buffer(&[u8])` is a single `copy_from_slice` into the
  growable `Vec<u8>`). No intermediate per-record buffer is allocated.
  LZ4's output stream accumulates uncompressed input in a
  `max_block_size` (=64KB) scratch buffer and writes compressed
  blocks directly to the sink — also matches Java's pattern. Phase 3d
  (`MemoryRecordsBuilder`) will need to reuse `ByteBufferOutputStream`
  across batches for full hot-path optimization, but that's a Phase 3d
  concern.
- **`phase3a_compression_dispatch_gap.md` marked RESOLVED**: confirmed
  in lines 1-7. Cross-references `phase3c_compression.md`.
- **No regressions in 3a/3b**: 446 = 350 (3a baseline) + 66 (3b) +
  30 (3c). Matches the test ledger.

---

Round 1 outcome: **APPROVED with deferrals**. Two DEFERRED-OK items
(#11 Snappy xerial framing must be fixed before Phase 5; #12 LZ4 level
knob is a performance-only deviation, not wire-compat). No code changes
requested. Phase 3c can advance to Phase 3d.

# Phase 3d-1 Review — Critic N=0

Reviewed commit `4401d79` (Phase 3d-1: DefaultRecord + PartialDefaultRecord).
Files: `src/common/record/default_record.rs` (+1192 LOC),
`src/common/record/partial_default_record.rs` (+506 LOC),
`src/common/record/mod.rs` (re-exports for both).

## Verification done

1. **DoD checks (timeout 240s):**
   - `cargo build` — clean.
   - `cargo test --lib` — `475 passed; 0 failed; 0 ignored; 0 measured;
     0 filtered out; finished in 0.24s`.
   - `cargo xtask format-check` — clean.
   - `cargo xtask lint` — `No lint issues found!`.
   - `cargo xtask check-generated` — clean.
2. **Test count fidelity:** Java `DefaultRecordTest.java` has 20
   `@Test` methods (14 non-Partial + 6 Partial). Rust translates
   14 → `default_record::tests` and 6 → `partial_default_record::tests`,
   plus 7 Rust-side extras in `default_record` (byte-level fixture,
   zero-copy alias assertion, Unicode header round-trip,
   `record_size_upper_bound` matches, `size_in_bytes_with_sizes`
   matches, `increment_sequence` wrap, `attributes` zero) and
   2 Rust-side extras in `partial_default_record` (round-trip metadata,
   `key()/value()/headers()` empty). Total +29 tests
   (446 → 475). Matches actor's report.
3. **Byte-level fixture audit** (`byte_level_fixture`,
   `default_record.rs:984`): hand-recomputed the 14-byte expected
   vector against the v2 spec for
   `(offset_delta=1, ts_delta=2, key="k", value="v",
   headers=[("h","vh")])`:
   body = 13 bytes → length-prefix `0x1A` (zigzag(13)=26).
   Body fields all match the literal. Literal is *not* recomputed
   from `write_to` — it's a hard-coded `&[u8]` array.
4. **Zero-copy alias assertion** (`read_from_buffer_is_zero_copy`,
   `default_record.rs:1020`): captures `bytes.as_ptr()` and
   `bytes.len()` *before* the move into `read_from_buffer`, then
   asserts the returned record's `key().as_ptr()` and
   `value().as_ptr()` fall inside the original `[start, start+len)`
   range. Test exercises the `Bytes::split_to` (refcount-bump)
   pathway. Passes.
5. **Varint behaviour spot-checks** (against `byte_utils`): zig-zag
   encoding `1 → 0x02`, `-1 → 0x01`, `100 → 0xC8 0x01`,
   `i64::MAX → 10 bytes` — all confirmed via the `byte_level_fixture`
   and `invalid_varlong` tests. `byte_utils` was already validated
   in Phase 2c.
6. **Header round-trip**: `header_key_unicode_round_trip` confirms
   the `RecordHeader::from_bytes` path. UTF-8 lossy decode matches
   `Utils.utf8` semantics.
7. **`PartialDefaultRecord` justification** (Java
   `PartialDefaultRecord.java`): Java extends `DefaultRecord` and
   overrides `key()/value()/headers()` to throw
   `UnsupportedOperationException`. Java's
   `DefaultRecord.readPartiallyFrom` returns this type. Rust's
   addition is required because `read_partially_from` mirrors the
   Java static method, and the `PartialDefaultRecord` type is the
   return type. **Justified**, not overreach.
8. **CRC**: `DefaultRecord.ensure_valid()` is a no-op (Java
   matches). The CRC lives on `DefaultRecordBatch` (Phase 3d-2). No
   redundant per-record CRC. Confirmed.
9. **Visibility audit**: `increment_sequence` is `pub(crate)`
   (`default_record.rs:559`), `record_size_upper_bound` is
   `pub(crate)` with `#[allow(dead_code)]` (`default_record.rs:505`).
   Both correct per the deferral plan.
10. **License headers**: both files carry the Apache 2.0 / Confluent
    Inc. header (CLAUDE.md rule 7).
11. **Module re-exports**: `pub use default_record::DefaultRecord`
    and `pub use partial_default_record::PartialDefaultRecord` in
    `mod.rs`. Static functions (`write_to`, `read_from_buffer`, …)
    are *not* re-exported at the parent level — accessible only
    through `record::default_record::*`. Matches CLAUDE.md rule 2.

## Findings

No new issues. The translation is faithful and covers every Java
test plus the byte-level fixture and zero-copy alias assertions
required by PLAN.md DoD.

Two design choices worth recording (not bugs):

- **`PartialDefaultRecord::key()/value()/headers()` return
  `None`/`&[]`** instead of throwing
  `UnsupportedOperationException`. The Rust author cites CLAUDE.md
  rule 10 (avoid panics in public API). The `Record` trait has
  these as required methods, so returning the absent value is
  correct: callers test `has_key()`/`has_value()` first, and the
  payload accessors then naturally yield empty. No call site
  currently distinguishes "absent because partial" from "absent
  because null". Acceptable.

- **`partial_default_record::read_partially_from_inner` allocates a
  body-sized scratch buffer** (`vec![0u8; body_size_usize]`) where
  Java skips bytes via `InputStream.skip`. Documented at
  `partial_default_record.rs:198-205`. Used only on the
  consumer/broker validation path, not the producer hot path. Not
  a CLAUDE.md rule 12 violation. Acceptable.

## Phase 3d-1 Round 1 verdict: APPROVED

No code changes requested. Phase 3d-1 can advance to Phase 3d-2
(DefaultRecordBatch).

---

# Phase 3d-2 Review — Critic N=0

Scope: 1 commit since `4401d79` — `f159d8f` (DefaultRecordBatch +
LogInputStream + iterator + RecordValidationStats). Issue numbering
continues from 13 (3a: 1–8, 3b: 9, 3c: 11–12, 3d-1: had no issues).

DoD checks I re-ran (all green):
- `cargo build` — clean.
- `cargo test --lib` — `test result: ok. 508 passed; 0 failed; 0 ignored;
  0 measured; 0 filtered out; finished in 0.25s` (was 475 in Phase 3d-1,
  +33; matches actor's report).
- `cargo xtask format-check` — `All code is properly formatted!`
- `cargo xtask lint` — `No lint issues found!`
- `cargo xtask check-generated` — `All generated code is properly formatted!`

## What I verified — wire format byte fidelity

- **Header offsets (61 bytes total):** all 13 Java field offsets match
  byte-for-byte (`BASE_OFFSET_OFFSET=0`, `LENGTH_OFFSET=8`,
  `PARTITION_LEADER_EPOCH_OFFSET=12`, `MAGIC_OFFSET=16`,
  `CRC_OFFSET=17`, `ATTRIBUTES_OFFSET=21`, `LAST_OFFSET_DELTA_OFFSET=23`,
  `BASE_TIMESTAMP_OFFSET=27`, `MAX_TIMESTAMP_OFFSET=35`,
  `PRODUCER_ID_OFFSET=43`, `PRODUCER_EPOCH_OFFSET=51`,
  `BASE_SEQUENCE_OFFSET=53`, `RECORDS_COUNT_OFFSET=57`,
  `RECORDS_OFFSET=61`). Verified via lines 74–103.
- **CRC range matches Java exactly:**
  `crc32c(&self.buffer[ATTRIBUTES_OFFSET..])` (line 181) matches
  `Crc32C.compute(buffer, ATTRIBUTES_OFFSET, length - ATTRIBUTES_OFFSET)`
  (Java DefaultRecordBatch.java:399).
- **Attribute mask layout matches Java exactly:**
  `COMPRESSION_CODEC_MASK=0x07` (bits 0-2),
  `TIMESTAMP_TYPE_MASK=0x08` (bit 3),
  `TRANSACTIONAL_FLAG_MASK=0x10` (bit 4),
  `CONTROL_FLAG_MASK=0x20` (bit 5),
  `DELETE_HORIZON_FLAG_MASK=0x40` (bit 6).
- **Big-endian for all multi-byte fields:** confirmed via
  `i64_at`/`i32_at`/`i16_at`/`write_*_at` helpers (lines 938–966) all
  use `from_be_bytes` / `to_be_bytes`.
- **Mutator CRC discipline matches Java:**
  - `set_last_offset` (Rust line 397) — only updates `BASE_OFFSET_OFFSET`
    (offset 0), which is *outside* the CRC range (CRC starts at offset
    21). No CRC recomputation needed. **Matches Java
    DefaultRecordBatch.java:367** (which also does not recompute CRC).
  - `set_partition_leader_epoch` (line 435) — only updates
    `PARTITION_LEADER_EPOCH_OFFSET` (offset 12), also outside CRC range.
    **Matches Java line 386.**
  - `set_max_timestamp` (line 403) — updates `ATTRIBUTES_OFFSET` (21)
    and `MAX_TIMESTAMP_OFFSET` (35), both inside CRC range. **Recomputes
    CRC at line 432**, matching Java line 380–381 exactly.
  - Tests `set_last_offset_rewrites_base_offset` (line 1394) and
    `set_partition_leader_epoch_rewrites_field` (line 1426) explicitly
    assert `batch.is_valid()` post-mutation — i.e., the on-disk CRC is
    still the right one for the unchanged attributes-onward bytes.
- **Decompression for all 5 codecs:** `decompression_round_trip_for_each_codec`
  (line 1549) iterates `[None, Gzip, Snappy, Lz4, Zstd]` and asserts
  3-record round-trip with mixed null keys/values for every codec.
- **Multi-batch iterator:** `iterates_two_concatenated_batches`
  (`record_batch_iterator.rs:128`) and
  `iterator_ignores_incomplete_entries` (`byte_buffer_log_input_stream.rs:203`)
  both build two concatenated batches and assert correct iteration.
- **`increment_sequence` relocation:** moved to
  `default_record_batch.rs:916`, removed from `default_record.rs`.
  `default_record.rs:49` and `partial_default_record.rs:27` import the
  new path correctly. Body unchanged — round-trip-tests in 3d-1 still
  pass (`grep -rn increment_sequence src/` shows 9 callsites, all
  consistent).
- **Module re-exports:** `DefaultRecordBatch` and `RecordValidationStats`
  re-exported at parent (`mod.rs:51, 56`); static helpers
  (`write_empty_header`, `write_header`, `increment_sequence`,
  `decrement_sequence`, `estimate_batch_size_upper_bound`,
  `size_in_bytes_records`, `size_in_bytes_simple`) are *not*
  re-exported. `byte_buffer_log_input_stream`,
  `log_input_stream`, and `record_batch_iterator` are `pub(crate)`
  modules (per CLAUDE.md rule on `internal` packages). Clean.
- **Apache 2.0 / Confluent Inc. headers** present on all 5 new files.
- **`BufferSupplierTest.java` translation:** Java's file has only one
  `@Test` (`testGrowableBuffer`); translated to
  `growable_buffer_caches_and_grows`. Six bonus Rust-side tests cover
  `NoCaching`/`Default` variants Java's test omits. Complete.
- **No regressions:** 508 = 475 (3d-1 baseline) + 33 (3d-2). Matches.
- **No TODO/FIXME** in any of the five new files. No `unimplemented!()`
  / `todo!()`.

## Issues found

Issues 13, 14, 15, 16, and 17 have been resolved and moved to
`COMMENTS.DONE.0.md`.

---

## Verified clean (no issue) for Phase 3d-2

- **Decompression covers all 5 codecs** —
  `decompression_round_trip_for_each_codec` at line 1549 explicitly
  iterates `[None, Gzip, Snappy, Lz4, Zstd]`.
- **`skip_key_value_iterator`** matches Java's contract: returns
  `PartialDefaultRecord` items for compressed batches, full
  `DefaultRecord` items for uncompressed (Java optimization is
  pointless on a slice-only buffer). Test
  `skip_key_value_iterator_yields_correct_count` (line 1601)
  exercises 4 codecs.
- **Multi-batch concatenation** —
  `record_batch_iterator::tests::iterates_two_concatenated_batches`
  (line 128) builds 2 batches at offsets 0 and 2, then iterates;
  `byte_buffer_log_input_stream::tests::iterator_ignores_incomplete_entries`
  builds 2 batches and truncates 5 bytes off the end.
- **`set_last_offset` does NOT recompute CRC**, mirroring Java exactly
  (`base_offset` is outside the CRC range). The hint "Java's
  `setLastOffset` recomputes CRC" was incorrect against the actual
  Java source (DefaultRecordBatch.java:367 only does the BASE_OFFSET
  putLong).
- **`set_partition_leader_epoch` does NOT recompute CRC**, mirroring
  Java exactly. The leader epoch field is at offset 12, also outside
  the CRC range.
- **`record_size_upper_bound` `#[allow(dead_code)]`** is exercised by
  `estimate_batch_size_upper_bound_includes_overhead` (line 1770) via
  the `estimate_batch_size_upper_bound` wrapper — so the function is
  not subtly broken.
- **`PartialEq` / `Eq` / `Hash` / `Debug` / `Display`** all derive
  consistently from the underlying `Vec<u8>`; matches Java's
  `equals()` / `hashCode()` / `toString()` semantics.
- **`is_valid()`** combines size and CRC checks; matches Java line 394.
- **`ensure_valid()`** returns distinct error messages for the two
  failure modes; both map to `KafkaError::CorruptRecord`.
- **`UncompressedIter::records: Bytes`** is the single per-iter
  allocation; per-record key/value slices alias inside it (zero-copy).
- **`CompressedIter`** correctly dispatches to
  `partial_default_record::read_partially_from` when
  `skip_key_value=true`, else `default_record::read_from_stream`.
- **`compute_attributes`** packs all 5 flags correctly (verified
  against Java DefaultRecordBatch.java:412–447).
- **`LogInputStream` trait** is `pub(crate)`, single method
  `next_batch() -> Result<Option<T>, KafkaError>`. Java is
  package-private with `nextBatch() throws IOException`. Match modulo
  Rust error handling.
- **`RecordValidationStats`** is a faithful plain-data translation of
  Java's class. `EMPTY` constant + `default()` impl. `add()` is
  in-place mutation matching Java.
- **`ByteBufferLogInputStream::next_batch_size`** correctly enforces
  the V0 minimum overhead (14) and rejects oversized batches per
  `max_message_size`. Magic byte validated against
  `0..=CURRENT_MAGIC_VALUE`. V0/V1 batches are rejected with a
  `CorruptRecord` (PLAN.md scope — legacy formats out of scope).
- **`RecordBatchIterator`** poisons on first error, matching Java's
  "throw and stop" semantics.

---

## Phase 3d-2 Round 2 verdict: APPROVED

Round 2 covered fixup commits `415b3b6`, `33b1d64`, `efe9a41`,
`98ad1ec`, `6b10c4d` resolving Issues 13–17, and rotation commit
`786b5eb`. Re-ran DoD checks:

- `cargo build` — clean.
- `cargo test --lib` — `test result: ok. 511 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.25s` (was 508).
- `cargo xtask format-check` — clean.
- `cargo xtask lint` — `No lint issues found!`.
- `cargo xtask check-generated` — `199 generated file(s)` clean.

Per-issue verification:
- **Issue 14 (`415b3b6`)** — `MutableRecordBatch::set_max_timestamp`
  now returns `Result<(), KafkaError>`; `write_header` /
  `write_empty_header` / `compute_attributes` likewise. The `assert!`
  branches are replaced with
  `Err(KafkaError::IllegalArgument(<same message text>))`. The
  internal `debug_assert!` on `buffer.len() >= position + size_in_bytes`
  in `write_header_at` is a programming-error precondition (rule 10.1
  permits debug-time panic on internal invariants). All callsites
  use `?` propagation or `.expect("test helper passes valid …")` —
  no `let _ = …` ignore patterns introduced.
- **Issue 13 (`33b1d64`)** — `write_header` is fully replaced by
  `write_header_at(buffer: &mut [u8], position: usize, …)`. The new
  function does NOT call `buffer.resize`; it operates on a pre-sized
  slice and writes only the 61 header bytes in place. The CRC is
  computed over the contiguous `[ATTRIBUTES_OFFSET..size_in_bytes)`
  range that the caller pre-populated. `write_empty_header`
  pre-grows the buffer and routes through `write_header_at`. No
  residual references to the old `write_header` API in the source
  tree (the unrelated `lz4_block_output_stream::write_header` is a
  different function). API is `pub fn`, which is correct — the
  builder in Phase 3d-4 will need it; CLAUDE.md does not require
  `pub(crate)` for non-`internal` packages.
- **Issue 17 (`efe9a41`)** — `RecordBatch::iter`,
  `RecordBatch::streaming_iterator`,
  `MutableRecordBatch::skip_key_value_iterator` now all yield
  `Result<Box<dyn Record + 'a>, KafkaError>`.
  `offset_of_max_timestamp`'s default impl propagates with `?`. All
  earlier 3a/3b/3c/3d-1 test sites either iterate happy-path with
  `.unwrap()` or are not affected. The "too little" path defers an
  error via a `pending_error` field so the last-good record is
  yielded first, then the error — mirrors Java's
  `InvalidRecordException` timing exactly. The "too many" path lets
  the underlying buffer-empty read raise `Err(CorruptRecord)`.
- **Issue 16 (`98ad1ec`)** — Two new tests:
  `invalid_record_count_too_many_compressed_terminates_iter` and
  `invalid_record_count_too_little_compressed_yields_declared_count`,
  both using GZIP. `CompressedIter::next` extends with the
  `ensure_none_remaining` semantics: probes the underlying stream
  for one extra byte after the last declared record and defers
  `Err(CorruptRecord("Incorrect declared batch size, records still
  remaining in file"))` if any bytes remain. Message text matches
  Java's.
- **Issue 15 (`6b10c4d`)** — `byte_level_fixture_two_records` builds
  two records via `build_uncompressed_batch` and asserts the entire
  83-byte sequence equals a hard-coded `&[u8]` literal. CRC literal
  `0x8CCB7CD8` is independently re-verified via
  `crc32c::crc32c(&expected[ATTRIBUTES_OFFSET..])`, NOT recomputed
  by the encoder under test. Spot-checked key bytes:
  Magic at offset 16 = `0x02`, Attributes i16 at offset 21 = `0x0000`,
  base_timestamp BE at offset 27 = `1000 = 0x03E8`,
  max_timestamp at offset 35 = `1001 = 0x03E9`, records_count at
  offset 57 = `2`. All match the v2 spec.
- **Comment rotation (`786b5eb`)** — `COMMENTS.0.md` issues section
  retains only the deferred items 5/6/7/8 and 11/12 from earlier
  phases; Issues 13–17 bodies are absent. `COMMENTS.DONE.0.md` has
  full Issue 13–17 sections with **Resolution:** paragraphs citing
  the fixup SHAs.

No new regressions: the iterator type change broke no earlier test
(verified by 511/511 pass), the `write_header_at` API is new and
hasn't leaked elsewhere, no `let _ = …` swallow patterns were
introduced.

---

## Phase 3d-2 Round 1 verdict: needs minor fixes

Issues 13 and 14 are MAJOR; 15, 16, 17 are MINOR. Issue 13 (latent
`write_header` truncation bug) should be fixed before Phase 3d-3 lands
or it will surface as a panic. Issue 14 (CLAUDE.md rule 10 violation)
is a contract concern that affects the public `MutableRecordBatch`
trait and should be fixed in 3d-2 to avoid having to break the trait
signature in a later phase.

Round 1 outcome: Issues 13–17 resolved by fixup commits `415b3b6`
(Issue 14), `33b1d64` (Issue 13), `efe9a41` (Issue 17), `98ad1ec`
(Issue 16), `6b10c4d` (Issue 15); see `COMMENTS.DONE.0.md` for the
resolutions.
