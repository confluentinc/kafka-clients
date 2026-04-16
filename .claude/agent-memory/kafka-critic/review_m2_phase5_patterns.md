---
name: M2 Phase 5 patterns
description: TOCTOU in chain traversal snapshot-before-await, OOM handling omission, test coverage gaps in BufferPool/FutureRecordMetadata
type: project
---

Key review findings for Milestone 2 Phase 5 (Producer Internals Foundation):

1. **TOCTOU in FutureRecordMetadata chain traversal**: Snapshotting the chain via collect_chain() before awaiting results misses concurrent chain() calls that happen between the snapshot and await completion. Java avoids this by reading volatile nextRecordMetadata AFTER each result.await(). Fix: restructure to await-then-read like Java.

2. **OOM handling intentionally omitted**: Java's safeAllocateByteBuffer catches OOM and restores pool accounting. Rust's default allocator aborts on OOM, making recovery impossible. Not a bug but should be documented.

3. **testLargeAvailableMemory reduced**: Java test allocates 2GB buffers (via mock), checks arithmetic. Rust test only checks initial state. Missing allocation/deallocation coverage for large values.

4. **Notify vs Condition semantics**: tokio::sync::Notify stores one permit (unlike Java Condition which loses signals when not waiting). Per-waiter Notify instances with front-of-queue signaling preserves FIFO fairness. Multiple notify_one() calls coalesce but this is safe because waiters re-check state.

**How to apply:** When reviewing async chain/future patterns, check for snapshot-before-await anti-patterns where Java reads volatile fields after await. When reviewing resource pool patterns, check OOM recovery handling differences.
