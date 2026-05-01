# Phase 3a Review — Resolved Issues (Critic N=0)

Issues 1, 2, 3, 4 from the Phase 3a Round 1 review, resolved by the
Actor and moved here per `.claude/rules/agent-roles.md`.

Source review: `COMMENTS.0.md` (now retains only DEFERRED-OK items 5–8).
Original Round 1 commits reviewed: `0721e86`, `147a328`, `f2e9ded`,
`fd21a99`.

---

## 1. `RecordBatch::offset_of_max_timestamp` adds a required `BufferSupplier` parameter — Java is no-arg

- **Severity:** MAJOR (Behavior Mismatch — public API contract)
- **File:** `src/common/record/record_batch.rs:163-177`
- **Java reference:** `RecordBatch.java:257-267` —
  `default Optional<Long> offsetOfMaxTimestamp()` (no parameters)

Java's interface body acquires its own `BufferSupplier` internally with
`try (CloseableIterator<Record> iter = streamingIterator(BufferSupplier.create())) { ... }`.
The Rust default method signature is

```rust
fn offset_of_max_timestamp(
    &self,
    decompression_buffer_supplier: &mut BufferSupplier,
) -> Result<Option<i64>, KafkaError>
```

forcing every caller to supply one. This diverges from the Java public API
contract documented in the `RecordBatch` interface — Java callers (e.g.
`MemoryRecordsBuilder`, broker-side log code) call the no-arg overload and
expect `BufferSupplier.create()` to be created internally.

**Expected:** Mirror Java's no-arg signature; instantiate
`BufferSupplier::create()` (already available at
`src/common/utils/buffer_supplier.rs`) inside the default body, similarly
to Java's try-with-resources block. The for-loop already drops the
iterator (and thus releases the borrow on the local supplier) before the
method returns, so the no-arg version is implementable.

**Actual:** Required `&mut BufferSupplier` parameter; callers cannot call
`batch.offset_of_max_timestamp()` directly.

**Resolution:** Fixup `9fa1f99` against `147a328`. Dropped the parameter
from the trait method; the default body now instantiates a local
`BufferSupplier::create()` and passes it to `streaming_iterator`,
mirroring Java's try-with-resources scope. The local is dropped when
the for-loop's borrow ends. All 350 lib tests still pass.

---

## 2. `SimpleRecord::new` heap-allocates and copies key/value bytes — Java does not

- **Severity:** MAJOR (Performance / CLAUDE.md rule 12)
- **File:** `src/common/record/simple_record.rs:52-59`
- **Java reference:** `SimpleRecord.java:36-46` —
  `Utils.wrapNullable(byte[])` is `ByteBuffer.wrap(byte[])`, a zero-copy
  view over the user's array.

Java's `SimpleRecord(long, byte[], byte[], Header[])` calls
`Utils.wrapNullable(key)` / `wrapNullable(value)` which is
`ByteBuffer.wrap(...)`. `wrap` does **not** copy the array — it produces
a `ByteBuffer` view aliasing the caller's storage. Subsequent writes
through `ByteBuffer.put` into a target buffer copy directly from the
user's bytes.

Rust's `SimpleRecord::new(... Option<&[u8]> ...)` does
`key.map(Arc::from)`. `Arc::from(&[u8])` allocates a new heap buffer and
memcpys the slice contents. So every construction path that originates
from a borrowed `&[u8]` (which is the canonical surface) pays one heap
allocation + one memcpy of the key and value bytes — exactly what
CLAUDE.md rule 12 forbids on the producer write path:

> "Don't copy byte arrays holding the key, value or headers passed to
> ProducerRecord or received in ConsumerRecord. This zero-copy
> requirement extends through the entire write path"

`SimpleRecord` is on the producer write path: Java's
`MemoryRecordsBuilder.append(SimpleRecord)` is a real call site, and
`AbstractRecords.estimateSizeInBytes(byte, CompressionType, Iterable<SimpleRecord>)`
takes them in bulk. Phase 3c will hit this when it lands.

**Expected:** Storage type compatible with zero-copy from the caller. Two
viable shapes:

1. `Option<Bytes>` (from the `bytes` crate) which has explicit zero-copy
   shared-ownership semantics, or
2. Borrow the byte slices via a lifetime — e.g.
   `pub struct SimpleRecord<'a> { key: Option<&'a [u8]>, ... }`. This
   matches Java's `ByteBuffer.wrap` view-semantics one-for-one and
   is the closest Java-equivalent.

The `from_arcs` constructor exists as a fast-path bypass but the
canonical `new` constructor is what `MemoryRecordsBuilder` would call —
the API as defined forces a copy into the canonical path.

**Actual:** `Arc::from(&[u8])` allocates and copies on every `new` call.

**Resolution:** Fixups `c9d89f9` and `1fdfb8c` against `f2e9ded`.
Storage switched from `Option<Arc<[u8]>>` to `Option<bytes::Bytes>`;
`bytes` promoted from a transitive dep to a direct dep in `Cargo.toml`.
Canonical `SimpleRecord::new` now takes `Option<Bytes>` directly —
zero-copy when the caller already owns a `Bytes`. Added
`new_from_slice` as the documented copying convenience for the
`&[u8]` case (uses `Bytes::copy_from_slice`). Dropped `from_arcs`
since the new canonical path covers its purpose. Added
`new_with_bytes_is_zero_copy` test asserting
`bytes_input.as_ptr() == record.value().as_ptr()`. Lib test count
350 (was 349, +1).

---

## 3. `ControlRecordType::recordKey()` not translated

- **Severity:** MINOR (Missing translation, deferred)
- **File:** `src/common/record/control_record_type.rs`
- **Java reference:** `ControlRecordType.java:78-86`

Java's `recordKey()` builds a `Struct` writing
`(version, type)` for the current key schema. It's used by
`MemoryRecordsBuilder.java:614` (`Struct keyStruct = type.recordKey()`)
to serialize a control record's key when a broker-side or coordinator
caller emits one.

The Rust translation skipped this method without a written deferral
note in `phase3a_compression_dispatch_gap.md` or
`phase3a_record_base.md`. It's a Phase 3c concern (since `Struct` /
`Schema` from `protocol.types` and `MemoryRecordsBuilder` both arrive
then), so the omission is acceptable, but it should be tracked
explicitly so it doesn't fall through. Either:

- Add a one-line note to `phase3a_record_base.md` listing
  `recordKey()` as deferred to Phase 3c, or
- Implement it now as a 4-byte-buffer writer
  (`[u8; CURRENT_CONTROL_RECORD_KEY_SIZE]` of `(version, type)`
  big-endian Int16s) and validate against `parse_type_id` in the same
  module's tests.

**Resolution:** Fixup `1f965ec` against `fd21a99`. Added an explicit
deferral note to `.claude/agent-memory/actor-executor/phase3a_record_base.md`
listing `recordKey()`, its only Java call site
(`MemoryRecordsBuilder.java:614`), and the Phase 3c phase target.
No code change.

---

## 4. `Records::slice` returns `Result<Box<dyn Records>, KafkaError>` — Java is non-throwing

- **Severity:** MINOR (Behavior Mismatch — error model)
- **File:** `src/common/record/records.rs:91`
- **Java reference:** `Records.java:107` — `Records slice(int position, int size);`

Java declares no `throws`; both implementations
(`MemoryRecords.slice`, `FileRecords.slice`) raise
`IllegalArgumentException` (unchecked) for invalid arguments. The Rust
trait pre-emptively forces every implementor to wrap a `Result`, which
diverges from Java's contract and adds friction at every call site.

This is a design choice rather than a strict bug — Rust commonly
prefers `Result` over panics for arg validation. But the trait surface
is a public contract, so flagging.

**Expected:** Either return `Box<dyn Records>` directly and panic on
invalid args (matching Java's unchecked exception), or document that
this is a deliberate departure from Java's error model in the trait
docstring.

**Actual:** `Result` wrapper without rationale.

**Resolution:** Fixup `041ffca` against `147a328`. Kept the `Result`
signature (matches CLAUDE.md rule 10: unchecked-but-recoverable Java
exceptions translate to `Result`, not panic). Added a docstring on
the trait method explaining the deliberate divergence and citing the
rule.

---

# Phase 3b Review — Resolved Issues (Critic N=0)

Issue 9 from the Phase 3b Round 1 review, resolved by the Actor and
moved here per `.claude/rules/agent-roles.md`.

Source review: `COMMENTS.0.md`. Original Phase 3b commits reviewed:
`284528a` (serialization module) and `1b46ef3` (tests +
actor-memory notes).

---

## 9. Re-export of `NULL_ENTRY_VALUE` constant violates CLAUDE.md rule 2

- **Severity:** MINOR
- **File:** `src/common/serialization/mod.rs:70`
- **Java reference:** N/A (rule violation, not Java drift)
- **CLAUDE.md rule:** *"Constant MUST be exported only by the file
  defining them. E.g.: `GROUP_METADATA_TOPIC_NAME` is accessible
  through `::common::internals::topic::GROUP_METADATA_TOPIC_NAME`"*

`mod.rs` line 70 contained:

```rust
pub use list_serializer::{InnerKind, ListSerializer, NULL_ENTRY_VALUE, SerializationStrategy};
```

`NULL_ENTRY_VALUE` is a `pub const i32` (defined at
`list_serializer.rs:31`) and per CLAUDE.md must NOT be re-exported at
the parent module. The pattern in `src/common/record/mod.rs`
correctly avoids re-exporting constants like `MAGIC_VALUE_V0`,
`NO_TIMESTAMP`, etc. that live in `record_batch.rs` — only types and
traits are re-exported there.

**Expected**: `pub use list_serializer::{InnerKind, ListSerializer, SerializationStrategy};`
(drop `NULL_ENTRY_VALUE`). Callers needing the constant should use
`crate::common::serialization::list_serializer::NULL_ENTRY_VALUE`.

**Actual**: Constant was re-exported, allowing
`crate::common::serialization::NULL_ENTRY_VALUE` access — non-conformant
with the rule and inconsistent with the rest of the codebase
(`record::mod.rs`, `protocol::mod.rs`).

This was internal-only; the constant has no callsite outside
`list_serializer.rs` and `list_deserializer.rs`, both of which already
import it via the file-module path
(`crate::common::serialization::list_serializer::NULL_ENTRY_VALUE`),
so the fix is mechanical and risk-free.

**Resolution:** Fixup `5dd0a73` against `284528a`. Dropped
`NULL_ENTRY_VALUE` from the `pub use list_serializer::{...}` line in
`src/common/serialization/mod.rs`; kept the type re-exports
(`InnerKind`, `ListSerializer`, `SerializationStrategy`). No callsite
needed updating since the only in-tree consumer
(`list_deserializer.rs:21`) already routed through the file-module
path. All 416 lib tests still pass; format-check, lint, and
check-generated all clean.

---

# Phase 3d-2 — Critic N=0 (Round 1) — Resolved Issues

Issues 13–17 were raised against commit `f159d8f` and resolved by the
fixup commits cited below.

## 13. `write_header` truncates pre-filled record bytes — MAJOR (latent)

- **Severity:** MAJOR (latent — not exercised by current callers,
  but a public function with a documented incorrect contract).
- **File:** `src/common/record/default_record_batch.rs:744–830`
  (`pub fn write_header`).
- **Java reference:** `DefaultRecordBatch.java:480–520` (`writeHeader`).

Java's `writeHeader` writes header bytes *in place* over a
`ByteBuffer` slot whose records are already populated between
`position + RECORD_BATCH_OVERHEAD` and `position + sizeInBytes`. The
Rust translation at line 781 did
`buffer.resize(position + RECORD_BATCH_OVERHEAD, 0)`. If the caller had
already appended records (so `buffer.len() == position + size_in_bytes`,
where `size_in_bytes > RECORD_BATCH_OVERHEAD`), this resize would shrink
the buffer and silently drop the records. The CRC compute at line 828
would then index out of bounds and panic.

Currently no caller hit this path: the only `write_header` caller was
`write_empty_header`, which always sets `size_in_bytes =
RECORD_BATCH_OVERHEAD` (records absent), so the resize was a no-op.

**Resolution:** Fixup `33b1d64` against `f159d8f`. Renamed
`write_header(buffer: &mut Vec<u8>, ...)` to
`write_header_at(buffer: &mut [u8], position: usize, ...)`. The new
function takes a pre-sized mutable slice — caller must ensure
`buffer.len() >= position + size_in_bytes` with records already
populated in `[position + RECORD_BATCH_OVERHEAD,
position + size_in_bytes)`. Only the 61 header bytes are written in
place; no resize. The precondition is verified with `debug_assert!`
(programming error, not user input — CLAUDE.md rule 10.1 allows
debug-time panic on internal invariants). `write_empty_header` is
unchanged externally — it pre-grows the buffer by 61 bytes and routes
through `write_header_at` internally. This unblocks Phase 3d-3 / 3d-4
which both need to call the header-write function after appending
records into a pre-allocated batch buffer.

---

## 14. `set_max_timestamp` panics on `NoTimestampType` — MAJOR (CLAUDE.md rule 10)

- **Severity:** MAJOR (CLAUDE.md rule 10 violation on public API).
- **File:** `src/common/record/default_record_batch.rs:411–414`.
- **Java reference:** `DefaultRecordBatch.java:425–426` (throws
  `IllegalArgumentException`).

`set_max_timestamp` is a public method on the `MutableRecordBatch`
trait. CLAUDE.md rule 10.2 states: "Return a `Result` when Java code
throws an exception even if unchecked but recoverable."
`IllegalArgumentException` for an invalid argument is recoverable —
the caller can detect it, validate, and retry. The Rust translation
`assert!(timestamp_type != TimestampType::NoTimestampType, ...)`
panicked, which is forbidden by rule 10.1. Same issue applied to
`compute_attributes`, `write_header`, and `write_empty_header` (bad
magic / bad timestamp / `NoTimestampType` were all panic-on-recoverable
paths).

**Resolution:** Fixup `415b3b6` against `f159d8f`. Converted all
`assert!` calls on user-recoverable inputs to
`return Err(KafkaError::IllegalArgument(_))` with the same message text
Java raises. Updated function signatures:
- `MutableRecordBatch::set_max_timestamp` -> `Result<(), KafkaError>`
- `write_header` / `write_empty_header` -> `Result<(), KafkaError>`
- `compute_attributes` (internal) -> `Result<u8, KafkaError>`

Validation for `magic >= CURRENT_MAGIC_VALUE` and
`base_timestamp >= 0 || base_timestamp == NO_TIMESTAMP` was hoisted out
of the (now-removed) `assert!` and surfaced as
`KafkaError::IllegalArgument`. `testSetNoTimestampTypeNotAllowed` was
converted from `#[should_panic]` to assert
`Err(KafkaError::IllegalArgument)` with a message-text contains check.
All other in-process callers updated with `?` propagation or
`.unwrap()` in tests where input is guaranteed valid.

---

## 15. `byte_level_fixture_empty_batch` has zero records — MINOR

- **Severity:** MINOR (PLAN.md DoD partial-fulfillment).
- **File:** `src/common/record/default_record_batch.rs:1697–1766`.
- **PLAN.md DoD:** "build a batch with two known records, assert the
  bytes equal a hex fixture".

The original fixture covered the 61-byte header only, with the CRC
recomputed from the same encoder under test (tautological). A bug in
the encoder's CRC byte range or polynomial would not surface.

**Resolution:** Fixup `6b10c4d` against `f159d8f`. Added new test
`byte_level_fixture_two_records` that builds an 83-byte batch with two
known records `(offset=0, ts=1000, key="k1", value="v1")` and
`(offset=1, ts=1001, key="k2", value="v2")` and asserts the entire byte
sequence equals a hard-coded `&[u8]` literal. The CRC value
(`0x8CCB7CD8`) is part of the literal; an additional assertion
independently verifies that
`crc32c::crc32c(&expected[ATTRIBUTES_OFFSET..]) == 0x8CCB7CD8` to catch
a regression in the encoder's CRC byte range. The previous empty-batch
fixture is kept for header-only coverage. Test count: 510 -> 511.

---

## 16. Compressed variants of invalid-record-count tests not translated — MINOR

- **Severity:** MINOR (test coverage gap).
- **File:** `src/common/record/default_record_batch.rs` (tests module).
- **Java reference:** `DefaultRecordBatchTest.java:238`
  (`testInvalidRecordCountTooManyCompressedV2`),
  `DefaultRecordBatchTest.java:247`
  (`testInvalidRecordCountTooLittleCompressedV2`).

Java has both uncompressed and compressed variants of the
invalid-record-count tests. The Rust translation had only the
non-compressed ones; the compressed variants exercise the
`CompressedIter` code path, which is independent from `UncompressedIter`.

**Resolution:** Fixup `98ad1ec` against `f159d8f`. Translated both
tests as
`invalid_record_count_too_many_compressed_terminates_iter` and
`invalid_record_count_too_little_compressed_yields_declared_count`.
Both build a GZIP-compressed batch via the existing
`build_compressed_batch` helper and override `RECORDS_COUNT_OFFSET` to
mismatch the actual record count. To make the "too little compressed"
test surface the corruption, also extended `CompressedIter::next` to
mirror Java's `StreamRecordIterator.ensureNoneRemaining()`: after the
last declared record is yielded, probe the underlying decompressed
stream for a single extra byte; if any bytes remain, defer a
`KafkaError::CorruptRecord` to the next `next()` call with the same
"Incorrect declared batch size, records still remaining in file"
error text Java raises. Test count: 508 -> 510.

---

## 17. Iterator silently drops the corruption signal — MINOR

- **Severity:** MINOR (behavioral divergence; documented).
- **File:** `src/common/record/default_record_batch.rs:545–551`,
  `611–614`.
- **Java reference:** `DefaultRecordBatch.java` —
  `StreamRecordIterator.next()` throws `InvalidRecordException` when
  bytes remain or the count is negative.

The `UncompressedIter` / `CompressedIter` set an internal `errored`
flag on corruption (negative `RecordCount`, surplus bytes, mid-record
decode failure) but still yielded the offending or last-good record
successfully. There was no public way for the caller to inspect the
flag. Java throws `InvalidRecordException` and propagates it.

**Resolution:** Fixup `efe9a41` against `f159d8f`. Converted
`RecordBatch::iter`, `RecordBatch::streaming_iterator`, and
`MutableRecordBatch::skip_key_value_iterator` Item types from
`Box<dyn Record + 'a>` to `Result<Box<dyn Record + 'a>, KafkaError>`.
Iterator behaviour:
- Negative `RecordCount` -> immediate `Err(CorruptRecord)`.
- Declared count too LITTLE (uncompressed and compressed) -> yield the
  declared-count of records, then the next `next()` call returns
  `Err(CorruptRecord)` (deferred via a `pending_error` field so the
  last good record still flows through).
- Declared count too MANY -> yield the actual records, then the next
  read fails on the empty buffer or stream -> `Err(CorruptRecord)`.
- Mid-record decode failure -> `Err(CorruptRecord)` with the underlying
  error wrapped in the message.

The `RecordBatch::offset_of_max_timestamp` default impl propagates
errors via `?`. All in-tree call sites updated with `r.unwrap()` or
explicit Result handling. The breaking change touched many test
sites but the surface change is small (one-character `r` ->
`r.unwrap()` in iteration loops).
