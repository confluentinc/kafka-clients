---
name: Phase 3d review patterns
description: Translation gotchas for the read-side records layer (MemoryRecords, RecordsSend, iterator consistency)
type: project
---

Recurring Java→Rust translation traps that surface in Phase 3d (record-set / send-state-machine layer):

**1. Iterator-error contract drift across multiple traits.**
Java's `Records.batches()` and `RecordBatch.iterator()` BOTH throw
`CorruptRecordException` on malformed batches. When Phase 3d-2 changed
`RecordBatch::iter` to yield `Result<...>`, the upstream `Records::batches`
remained plain `Box<dyn Iterator<Item = Box<dyn RecordBatch>>>` —
silent-truncating on errors via `filter_map(Result::ok)`. Whenever an
iterator-consistency rule changes for one trait, audit ALL related traits in
the same hierarchy for the same divergence. The actor's docstring may even
acknowledge "we trade panic for silent truncation" — which is exactly the
non-fix the parent rule already rejected.

**Where to look:** `Records::batches`, `MemoryRecords::valid_bytes`'s
break-on-error pattern, any `filter_map(Result::ok)` in Phase 3d code.

**2. Marker traits as Rust adaptation for Java default-method-on-interface.**
Java's `UnalignedRecords` declares `default RecordsSend<? extends BaseRecords> toSend()`.
The Rust translation cannot put this on a trait if the return type's generic
requires `Sized` — trait objects of the marker trait can't satisfy `Sized`.
The actor's "marker trait + concrete impl `to_send`" is the right fix.
Verify: (a) the trait body is empty (or only forwarding `BaseRecords`
methods), (b) every concrete impl has its own `to_send`, (c) docstring
explains the constraint.

**3. `to_send(self)` consumes — verify Java callers don't retain.**
Java's `records.toSend()` returns a new send object while keeping the
original alive (GC'd). Rust consumes `self` for ownership clarity.
Search Java callers (`grep -rn '\.toSend()' kafka/clients/src/main/java`)
for any that use `records` after `toSend()` — if any do, the Rust API
needs to take `&self` + clone the `Bytes` internally.

**4. Boundary tests on header-size short-circuits.**
`firstBatchSize` has THREE boundary cases: `< LOG_OVERHEAD` returns None,
`< HEADER_SIZE_UP_TO_MAGIC` returns None, `== HEADER_SIZE_UP_TO_MAGIC`
returns the full declared size. Java's testNextBatchSize tests all three;
make sure Rust does too. The `<` vs `<=` distinction is the easy bug.

**5. Builder-driven test deferrals require triage.**
Many `MemoryRecordsTest.java` cases construct via
`MemoryRecords.withRecords(...)` / `MemoryRecordsBuilder`. Most are
genuine builder concerns and can defer. But spot-check that the test's
verification target isn't actually a 3d-3 concern (iterator semantics,
slicing, valid_bytes — all read-path). For `testIterator`: the metadata
verifications are builder-stamped, so deferring is fine, BUT the flat
iteration count semantic IS owned by 3d-3 and must be covered with a
hand-rolled batch.

**6. Phase 5 deferral of `write_to(channel)`.**
`TransferableChannel` lives in `common/network/*` — not yet translated.
The Phase 3d-3 `RecordsSend` correctly defers the channel body. Verify
the deferred API doesn't preclude `write_vectored` over `Bytes` slices
in Phase 5 (no `Vec<u8>` forced materialization on the public surface).
