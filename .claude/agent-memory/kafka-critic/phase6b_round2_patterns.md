---
name: Phase-6b Round-2 review patterns
description: Round-2 verification patterns for ProducerBatch buffer-pool ownership transfer + bytes-crate zero-copy semantics
type: project
---

# Phase-6b Round-2 verification patterns

## Bytes::try_into_mut zero-copy claims must be verified empirically

**Claim**: `Vec<u8> -> Bytes::from(v) -> try_into_mut() -> BytesMut::into::<Vec<u8>>()` preserves the original allocation.

**Verified by minimal repro** (bytes 1.11.1):

```
step 1: vec init       len=1024 cap=1024
step 2: truncated      len=513  cap=1024
step 3: bytes len=513
step 4: bytesmut       len=513  cap=1024  (capacity preserved)
step 5: vec            len=513  cap=1024  (capacity preserved)
```

**Why:** Capacity preservation is what makes the pool's `cap == initial_capacity` check fire and route the buffer to the pooled (recycle) branch instead of the non-pooled (drop) branch. If the bytes crate ever changed this contract (e.g. shrunk capacity to len), the pool would silently lose recycle hits.

**How to apply:** When reviewing zero-copy bytes-crate claims, run a 10-line repro to confirm capacity preservation through the conversion chain. Don't trust API docs alone — capacity behavior is a documented-but-easily-changed implementation detail.

## try_into_mut Err path soundness hazard with reserve

**Pattern** (anti):
```rust
let mut owned: Vec<u8> = match res {
    Ok(bm) => bm.into(),
    Err(b) => b.to_vec(),  // <-- fresh alloc with cap == len
};
if owned.capacity() < initial_capacity {
    owned.reserve(initial_capacity - owned.capacity());  // <-- may grow to *exactly* initial_capacity
}
let cap = owned.capacity();
unsafe { owned.set_len(cap); }  // <-- exposes uninit tail if reserve grew the alloc
```

**Why:** In the `Ok(bm)` branch the SAFETY argument holds: the underlying allocation was originally `vec![0u8; size]` so bytes 0..cap were zero-initialized. In the `Err(b)` branch `to_vec()` allocates a fresh buffer (cap == len), and `reserve(N)` may allocate again — bytes len..cap of the new allocation are uninitialized. If `reserve` happens to give exactly `initial_capacity` (allocator-dependent), the pool's `cap == size` check passes and the buffer goes onto the free list — the next consumer reads uninitialized bytes (UB).

**How to apply:** When reviewing recycle/pool code that uses `unsafe set_len`, trace every allocation source. If any source can produce a `Vec` whose tail is uninit, the SAFETY argument fails.

**Common false-confidence trigger:** "the path is unreachable in steady-state" — but `unsafe` requires soundness regardless of reachability, since unreachability claims often don't hold during refactors or at error paths.

## Test fidelity: regression tests must model production lifecycle

**Pattern (anti):** A regression test for a `buffer()` accessor calls `close()` then `buffer()` directly, never calling the intermediate `records()` accessor that production callers (Sender) will invoke.

**Why:** In Rust, `MemoryRecords::buffer` is a `Bytes`. Calling `records()` clones the `Bytes` (refcount += 1). When the test never clones, `try_into_mut()` succeeds. When production calls `records()` first (to feed the wire), refcount > 1 and `try_into_mut()` returns `Err`, triggering a different code path (copy fallback). The test exercises the wrong branch.

**Symptom in tests:** assertions like `len == capacity == initial_capacity` that hold in the unique-owner Ok branch but not in the cloned Err branch.

**How to apply:** When a `Bytes`-based ownership transfer claims "zero-copy when no clones outstanding," the regression test must:
1. Construct the production-typical scenario where a clone is outstanding (call `records()` or equivalent).
2. Assert what the deallocate path actually relies on (cap match for pool branch, or correct routing to non-pooled branch).

## Snapshot fields for "would-be-zero-after-move" accessors

**Pattern (good):** When a Rust translation moves out a backing storage (e.g. `Vec` out of `ByteBufferOutputStream`) on close/build, scalar getters that previously delegated to that storage break. Solution: snapshot the value at construction time into a dedicated field.

**Example:** `MemoryRecordsBuilder::initial_buffer_capacity` snapshot, replacing `self.buffer_stream.initial_capacity()` delegation. Java works because Java's `ByteBuffer` reference is still attached to `bufferStream` post-close (only position/limit reset). Rust moves the underlying `Vec` out, leaving an empty stub — the delegation returns 0.

**How to apply:** When reviewing a Rust translation that uses `mem::replace` to move out backing storage on close/build, check every scalar accessor that delegates to that storage. Each one needs a snapshot or equivalent guard.

**Look for the bug:** "Latent post-build returns-0 bug" — a getter that worked in Java's reference model returns a wrong value in Rust's move model.
