---
name: Phase 6b Round 2 review-fix patterns
description: Patterns from fixing the `finalize_recycled_buffer` Err-branch soundness hazard and adding production-lifecycle test coverage
type: project
---

Two patterns from Phase 6b Round 2 worth carrying into Phase 6d (and
any future hot-path code that mixes `unsafe set_len` with `Bytes`/`Vec`
ownership transfer).

## Pattern 1: bifurcate `unsafe set_len` by origin

When a helper produces a `Vec<u8>` via two paths and one path is a
fresh allocation while the other reuses a known-zero-init source,
**do not share the `unsafe set_len` step** between them.

- Source from `vec![0u8; size]` (e.g. an allocator pool's initial
  buffer): `set_len(capacity)` is sound — every byte 0..cap was
  zero-initialized at allocation, subsequent writes only overwrite a
  prefix.
- Source from `Bytes::to_vec()` (a fresh copy of the payload): the
  resulting `Vec` has `cap == len` initially. After `reserve(N)` the
  bytes `len..new_cap` are **uninitialized**. `set_len(new_cap)` exposes
  UB if anything reads them (and `BufferPool::deallocate`'s
  capacity-equality routing can land the buffer on a free-list, where
  a future consumer reads it).

**How to apply:** keep the `unsafe set_len` for the in-place /
known-zero-init path. Use safe `Vec::resize(cap, 0)` for the fresh-copy
path. The zero-fill cost is acceptable on rare/cold paths; it buys
soundness.

## Pattern 2: production-lifecycle regression tests for ownership transfer

Tests that call `buffer_owned()` (or any method that uses
`Bytes::try_into_mut()`) directly after `close()` only ever exercise
the **uniquely-owned** branch. The production path almost always has a
sibling clone alive (a wire-send clone, a metadata snapshot, etc.), so
the **shared** branch is the production-typical case but goes
untested.

**How to apply:** when writing regression tests for `try_into_mut`
fallbacks, structure the test to:

1. Build the artifact (`close()` → `complete()` etc.)
2. Clone the underlying `Bytes` (e.g. via `.records()` or `.buffer().clone()`)
3. Hold that clone alive
4. Call the extraction method — `try_into_mut` will return Err and the
   fallback fires
5. Assert the post-conditions of the fallback path explicitly,
   including the tail-zero check (a soundness signal — uninit-tail UB
   would produce arbitrary bytes; the safe `resize(_, 0)` guarantees
   zeros)

The tail-zero assertion is the closest you can get in safe Rust to
"asserting no UB" — it's not a proof, but it catches the most common
regression (reverting `resize` to `set_len`).

## Where this matters next

Phase 6d's `RecordAccumulator::deallocate(batch)` is the production
caller of `batch.buffer()`. It will run after `Sender.records()` has
cloned the `Bytes` for the wire send — i.e. the Err branch is the
**default** production path, not an edge case. Any future change to
`finalize_recycled_buffer` must keep both branches sound and tested.
