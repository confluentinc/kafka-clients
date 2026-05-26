---
name: Phase 3d-4 review streaming-codec self-borrow
description: How the MemoryRecordsBuilder achieves Java's appendStream-style streaming compression in Rust without giving up zero-copy
type: project
---

The Phase 3d-4 review (Issue #20) forced the compressed-append path
to stream through the codec directly into `buffer_stream` — the
original `uncompressed_buf: Vec<u8>` accumulator was a per-batch
double-memcpy violation of PLAN.md's zero-copy DoD.

**Why:** Java's `MemoryRecordsBuilder.java:143` constructs
`appendStream = new DataOutputStream(compression.wrapForOutput
(this.bufferStream, magic))` once at builder creation. Records flow
through that codec into `bufferStream` for the entire builder
lifetime. GC papers over the fact that `appendStream` borrows
`bufferStream`. Rust has no such luxury.

**How to apply:** When translating any Java class that holds a
"persistent stream wrapping our own field" pattern (compression
codecs, buffered writers, etc.), the Rust translation needs:

1. **Box the borrowee** so its address is stable across moves of the
   outer struct. Stack-allocating `ByteBufferOutputStream` makes the
   raw pointer dangle the moment the builder is moved (e.g. returned
   by value from a constructor). Caught by SIGSEGV in
   `abort_resets_buffer_position` early in development.

2. **Erase the wrapper's lifetime to `'static`** via a careful
   `mem::transmute` (or an `&'static mut` from a `*mut`). This is a
   controlled lie — the writer is dropped before the borrowee via
   either an explicit drop in `close()`/`abort()` or an explicit
   `Drop` impl on the outer struct. Rust's default field-drop order
   is *declaration order*, which would drop `buffer_stream` before
   `append_stream` if the latter is declared after — so an explicit
   `Drop` impl is required, NOT just field reordering.

3. **Funnel all writer access through `&mut self` methods** so the
   borrow on the borrowee is always exclusive while writes happen.
   Never expose the writer to callers.

4. **Drop the writer in error paths** (close-with-no-records, abort)
   to release the self-borrow before any subsequent mutation of the
   borrowee.

This shape lets `default_record::write_to_stream(&mut
self.append_stream, ...)` work transparently for both compressed and
uncompressed paths — the only difference is `append_stream = None`
for `CompressionType::None` (writes go directly to `buffer_stream`).

Verification: the streaming property is asserted by appending
> 64 KiB of records with LZ4 (whose 64 KiB block flush guarantees
emission mid-batch) and checking that
`builder.buffer_stream.position()` advances DURING append, not just
at close. Under the old materialize-then-compress design the
position would stay at the post-construction baseline until close.
