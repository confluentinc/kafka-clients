---
name: Phase 3d-4 streaming-codec self-referential struct
description: Verified-safe pattern for storing a Box<dyn Write + 'static> writer that borrows a Box<ByteBufferOutputStream> in MemoryRecordsBuilder
type: project
---

`MemoryRecordsBuilder` carries `buffer_stream: Box<ByteBufferOutputStream>` and `append_stream: Option<Box<dyn Write + 'static>>` where the latter holds a raw pointer borrow into the former.

**Why:** Java `appendStream` field bounds its own lifetime via GC. Rust needs a self-referential equivalent because the producer hot path requires bytes to stream through the codec into the batch buffer mid-append (no per-record/per-batch intermediate `Vec<u8>`). Boxing the inner stream pins its address; Drop impl orders writer-before-stream so the borrow ends before the borrowee is freed.

**How to apply when reviewing similar patterns:**
- The captured pointer must point to the *outer struct* whose address is stable, not a slice into a Vec/etc. inside it that can realloc. Check this: `wrap_for_output(&mut self.buffer_stream, ...)` is fine; capturing `&mut buffer_stream.buffer[..]` would be a bug.
- `impl Drop` is mandatory if field-declaration order doesn't naturally drop the borrower before the borrowee. `buffer_stream` is declared first in this struct; without explicit Drop, Rust would drop it first → UB.
- Every mutation-of-stream path (`abort`, `close`, `build`'s `mem::replace`) must drop the writer first. Verified in this code at lines 530, 587, 614, 644 (`build` calls `close` first).
- Writer must never escape the struct — search for `pub fn` returning `&mut dyn Write` or exposing `append_stream`. None here.
- Project has no miri config; could not run. Pattern is defensible by inspection but miri would be future hardening.
