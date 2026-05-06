---
name: Phase 6b Round 1 review-fix patterns
description: Patterns from fixing Phase 6b Round 1 critic comments — buffer ownership extraction, test cleanup, Java-binary bifurcation
type: project
---

Three patterns from Phase 6b Round 1 fixes worth remembering:

## Pattern 1: extracting `Vec<u8>` ownership through `Bytes` for the BufferPool recycle path

**Problem:** `MemoryRecordsBuilder::close()` MOVES the `Vec<u8>` out of
`buffer_stream` into `MemoryRecords` as `Bytes` (zero-copy
finalization). Phase 6d's `RecordAccumulator::deallocate(batch.buffer(),
initial_capacity)` needs the `Vec<u8>` back to recycle.

**Why:** Java's `bufferStream.buffer()` returns the same `ByteBuffer`
reference both pre- and post-build (the `ByteBuffer` is
reference-shared, no move). Rust's zero-copy contract requires the
move; we recover via `Bytes::try_into_mut() -> Vec<u8>`. Verified
zero-copy: `Bytes::from(vec).try_into_mut().into()` preserves the
original allocation pointer when uniquely owned.

**How to apply:** When a Phase 6 hot-path class owns a `Vec<u8>` that
later flows into `MemoryRecords`, expose a `buffer_owned(&mut self) ->
Vec<u8>` method that:
1. Checks `built_records.take()` first (post-build path)
2. Falls back to `buffer_stream.into_buffer()` (pre-build path)
3. Routes through a shared `finalize_recycled_buffer` helper that does
   `set_len(capacity)` so the result satisfies `BufferPool::deallocate`'s
   `len == capacity` invariant.

**Edge case I hit:** `Bytes::from(empty_vec)` returns the static `b""`
which fails `try_into_mut`. Handle empty/zero-capacity Vec explicitly
before the round-trip.

## Pattern 2: `initial_capacity` snapshot field

**Problem:** `MemoryRecordsBuilder::initial_capacity()` was implemented as
a delegate to `buffer_stream.initial_capacity()`. Post-build, the
`buffer_stream` is replaced with an empty stub (`with_capacity(0)`)
because the underlying Vec moved into `Bytes`. The accessor returned 0
post-build.

**Why:** Java's `bufferStream` retains its `ByteBuffer` reference, so
`initialCapacity()` is stable. Rust's move-out invalidates that.

**How to apply:** When an accessor that's expected to be stable
across the lifecycle delegates to a moved-out source, snapshot the
value into the wrapping struct at constructor time. Don't read
through to the moved-out backing.

## Pattern 3: bifurcate on the Java boolean, not the derived value

**Problem:** Java's `complete_future_and_fire_callbacks` has a
binary `recordExceptions == null` bifurcation. The Rust translation
collapsed this via `record_exceptions.as_ref().and_then(|f| f(i))`,
which silently flipped the success branch when the closure returned
`None` for some index.

**Why:** Java treats `null` per-index as "fire `onCompletion(null,
null)`" (error mode). The collapsed `and_then` treats it as "fire
metadata callback" (success mode). The semantic differs.

**How to apply:** When porting a Java binary `if (X == null) { ...
} else { ... }` where `X` is itself fed into a `Y = f(X)` call inside
the `else` arm — keep the bifurcation at the outer `X.is_none()`
level, not at the derived `Y.is_none()` level. Same pattern likely
applies to other producer/consumer paths (Sender response handling
with partial errors comes to mind for Phase 6e).

## Test naming for one-shot accessors

When a method has one-shot semantics (e.g., `buffer_owned()` that
returns empty on second call), name the test
`<method>_returns_owned_<what>` and add a tail assertion that
exercises the second call:

```rust
let buf = batch.buffer();
// ... assertions ...
let buf2 = batch.buffer();
assert_eq!(0, buf2.len(), "subsequent buffer() must return empty Vec");
```
