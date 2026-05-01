---
name: Phase-3 review patterns
description: Translation-bug patterns I saw in Phase 3a (record-format enums + base trait layer)
type: project
---

Phase 3a delivers the read-only enum / trait surface for `common/record/` —
no concrete `MemoryRecords`, `DefaultRecord`, or codec dispatch yet. Concrete
write-path types arrive in Phase 3c/3d.

**Why:** Phase 3a is preparing the trait surface for the producer write path.
Bugs here are cheap to fix but expensive once Phase 3c/3d types start
implementing the trait surface — every wrong default method body becomes
a per-implementor override.

**How to apply:**

1. **Java interface default methods** become Rust trait default methods. Watch
   for signature changes that add parameters Java didn't have — e.g. Java's
   `default Optional<Long> offsetOfMaxTimestamp()` is no-arg (it acquires a
   `BufferSupplier.create()` internally via try-with-resources). If the Rust
   default method requires the supplier as a parameter, the public API
   contract diverges. Confirm caller-side ergonomics match Java.

2. **Zero-copy regression in helper "value types"**. CLAUDE.md rule 12 forbids
   copying key/value bytes on the producer write path. `SimpleRecord` (and
   later `MemoryRecords`, `MemoryRecordsBuilder`) are on that path. Java uses
   `Utils.wrapNullable(byte[])` = `ByteBuffer.wrap(byte[])` — view-only, no
   copy. The Rust temptation is `Arc::from(&[u8])`, which allocates +
   memcpys. Look for `Arc::from(...)` over a borrowed slice in any
   constructor that takes `Option<&[u8]>` and store its result. The fix is
   either `Bytes` (from the `bytes` crate) or storing a `&'a [u8]` view via
   a lifetime parameter on the struct.

3. **Forced `Result` on Java non-throwing methods**. Java methods with no
   `throws` declaration that internally raise unchecked exceptions
   (`IllegalArgumentException`, `IllegalStateException`) shouldn't
   automatically become `-> Result<...>` in Rust. Use `Result` only when the
   exception is genuinely recoverable; otherwise mirror Java's behavior with
   panic-on-bug or silent acceptance. The trait surface is the public API
   contract; once `Result` is in the signature, every call site downstream
   must wrap-and-unwrap. `Records::slice` is a current example.

4. **Untracked deferrals.** When a method on a translated class is skipped
   for a later phase (e.g. `ControlRecordType.recordKey()` deferred to Phase
   3c), the actor should leave a written deferral note in the
   actor-memory or the file's docstring. If only the skipped method survives
   in `git diff` (no note anywhere), it's at risk of falling through. Grep
   for skipped methods by name in Java and confirm they appear either as a
   stub returning `KafkaError::InvalidRequest` or in a `phase3?_*.md`
   actor-memory note.

5. **Trait supertrait constraints.** Java `interface A extends B` →
   Rust `trait A: B`. Verify by reading the Rust trait declaration directly.
   The four supertrait edges in record/* are:
   - `MutableRecordBatch: RecordBatch`
   - `Records: TransferableRecords`
   - `TransferableRecords: BaseRecords`
   - (`AbstractRecordBatch` collapses into trait defaults — no supertrait)

6. **Box<dyn Trait> in trait-method return types.** When a trait method
   returns `Box<dyn RecordBatch + 'a>` per item in an iterator, every
   yielded record allocates. Acceptable for trait surfaces (you don't have
   the concrete type yet); flag as a perf concern only when concrete types
   land in Phase 3c+ and persist the boxing.

7. **Java `ByteBuffer` semantics quirks.** `ByteBuffer.allocate(N).putShort(...).flip()`
   leaves position=0, limit=bytes-written. `getShort(0)` reads absolute
   offset 0 (NOT position-relative). When translating to `&[u8]`, validate
   against `key.len()` (which is the Java `remaining()` after flip).
   Default byte order of `ByteBuffer` is BIG_ENDIAN — `i16::from_be_bytes`
   is the right Rust equivalent.

8. **Constants vs. `pub fn` for level metadata.** Per-codec compression
   level metadata is config-time, not hot-path. Constants (`const FOO: i32 = ...`)
   are fine. Don't expect `pub static`. Verify the constant values against
   the Java source comments — many of them come from upstream library
   constants (e.g. `Deflater.DEFAULT_COMPRESSION = -1`, `ZSTD_minCLevel = -131072`)
   which are easy to mis-translate.

9. **Zero-copy assertion test pattern.** When fixing a zero-copy regression
   on a value type that stores `Bytes`/`Arc<[u8]>`/etc., the right test is
   pointer equality (`input.as_ptr() == record.value().as_ptr()`), NOT
   contents equality (which passes trivially even after a copy). Pair it
   with a counter-test on the copying path that asserts `!=` to lock in the
   distinction. Phase 3a Round 2 used exactly this shape
   (`new_with_bytes_is_zero_copy` / `new_from_slice_copies` in
   `simple_record.rs`).

10. **Storage-type swap regression check.** When a translation switches
    storage from `Arc<[u8]>` to `bytes::Bytes` (or back), verify the wider
    trait surface that consumes that storage didn't change shape. The
    `Record::key()` / `Record::value()` return type must stay `Option<&[u8]>`
    so downstream Phase 3c/3d implementors aren't forced to depend on
    `bytes::Bytes`. The fix lives in the value-type's `fn key(&self)` /
    `fn value(&self)` accessors (`bytes.as_deref()` works for both).
