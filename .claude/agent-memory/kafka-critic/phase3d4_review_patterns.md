---
name: Phase 3d-4 review patterns
description: MemoryRecordsBuilder translation gotchas — streaming compression pitfall and dead-code timestamp branches
type: feedback
---

Recurring high-yield review patterns when translating Java's `MemoryRecordsBuilder` (or any class that wraps an output stream around a buffer):

**Java streaming compression vs. Rust at-close compression.**
Java's pattern is `appendStream = new DataOutputStream(compression.wrapForOutput(bufferStream, magic))` — every record write streams through the codec into the buffer. Rust translations frequently rewrite this as "accumulate into `Vec<u8>`, then compress once at close" because Rust's lifetime checker makes self-referential codec wrappers awkward. This **violates the per-record zero-copy DoD** for the producer hot path and adds a full batch-sized memcpy. The module docstring may try to argue the two shapes are equivalent — they're not. Verify by reading both `appendDefaultRecord` paths (Java line ~763 vs Rust `append_default_record`).
**Why:** PLAN.md's zero-copy DoD calls out "no per-record intermediate Vec." Anything that buffers records before the codec is a regression even if the wire output is byte-identical.
**How to apply:** When reviewing any `*RecordsBuilder` translation, locate the Java `appendStream` field and verify the Rust analogue is also a stream-wrapped codec living for the builder's lifetime — not a `Vec<u8>` field that's processed at `close()`.

**Dead-code if/else branches with identical bodies signal lost behavior.**
Pattern seen in `with_records`: `if x { NO_TIMESTAMP } else { NO_TIMESTAMP }` with a comment claiming the unused branch is "for tests that pin the value". Search the codebase for the referenced fn — if it doesn't exist, the branch is dead code and the Java behavior (`System.currentTimeMillis()` for LogAppendTime) was silently dropped.
**Why:** When a translator hits a branch they don't know how to translate (clock APIs, RNG, etc.), the easy escape is to "stub it out symmetrically and add a comment." This passes review unless the reviewer reads both branches.
**How to apply:** Any `if cond { X } else { X }` is a red flag — verify Java's two branches were *intentionally* identical, not collapsed because of a translation difficulty.

**Pointer-equality tests verify the zero-copy DoD.**
A correct test captures `buffer_stream.buffer().as_ptr()` *before* any append, pre-sizes capacity to avoid reallocation during writes, runs the appends, calls `build()`, and asserts `MemoryRecords::buffer().as_ptr() == captured_ptr`. The captured pointer must come from the underlying storage — re-fetching it from the freshly-returned MemoryRecords would be tautological.
**Why:** Without pointer equality, a "zero-copy" test only verifies the byte-content survives, not the allocation. A copy-on-build implementation passes round-trip tests.
**How to apply:** If a "zero-copy" test only checks `assert_eq!(records.buffer(), expected_bytes)` it's a tautology. Demand `as_ptr()` capture before the write phase.

**`Vec::drain(0..N)` preserves the data pointer.**
When the builder uses `initial_position > 0` to leave room for legacy headers and later wants the records to start at position 0 in the resulting `Bytes`, `Vec::drain(0..initial_position)` is the canonical move. It shifts elements left in-place (memmove within the allocation) and keeps the same backing pointer. Then `Bytes::from(Vec)` is allocation-preserving. Worth validating in tests.

**`KafkaError::IllegalState` is a client-side error.**
It maps to `IllegalStateException`, code `ERR_CODE_CONFIG`, not a wire error code. Phase 2 wired the same pattern for `IllegalArgument` — when reviewers see a new "client-side state error" variant, check `is_retriable`/`is_fatal`/`code`/`java_class_name`/`message`/`from_code` and reject any fake wire code assignment.
