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
