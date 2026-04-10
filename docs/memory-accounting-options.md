# Memory Accounting: Semaphore Strategy Options

## Context

The producer uses a `tokio::sync::Semaphore` with permits equal to `buffer.memory` bytes to enforce bounded memory usage. The question is: at what granularity do we acquire and release permits?

## Option A: Per-Record Tracking (chosen)

Acquire permits for each record individually. Track total acquired permits on the batch. Release the exact sum when the batch completes.

```
Record 1 (10 bytes) → acquire(10) → Batch.add_permits(10)
Record 2 (15 bytes) → acquire(15) → Batch.add_permits(15)

Batch completes     → release(Batch.acquired_permits() = 25)
```

**Pros:**
- Accurate memory accounting — permits reflect actual usage
- No wasted permits for half-empty batches
- Better utilization when many partitions have small batches

**Cons:**
- More semaphore operations (one per record vs one per batch)
- Slightly more complexity tracking permits

**When to use:** Default choice. Per-record semaphore cost is negligible in tokio (atomic operations).

## Option B: Per-Batch Allocation (Java's approach)

Acquire `batch.size` permits when creating a new batch. No per-record acquire. Release `batch.size` when the batch completes.

```
New Batch           → acquire(batch.size = 16384)
Record 1 (10 bytes) → append (no acquire)
Record 2 (15 bytes) → append (no acquire)

Batch completes     → release(16384)
```

This matches Java's `BufferPool.allocate(batchSize)` / `BufferPool.deallocate(buffer)`.

**Pros:**
- Simpler accounting — one acquire/release per batch
- Exactly matches Java behavior
- Fewer semaphore operations under high throughput

**Cons:**
- Wastes memory budget — a batch with 1 small record reserves full `batch.size` of permits
- With 1000 partitions × 16KB batch.size = 16MB reserved even if batches are nearly empty
- Reduces effective `buffer.memory` capacity

**When to switch:** If profiling shows semaphore contention is a bottleneck at very high record rates (> 1M records/sec). Java chose this approach because JVM semaphore acquisition involves kernel futex calls, which are more expensive than tokio's atomic-based implementation.

## Decision

**Option A (per-record tracking)** is implemented. The `acquired_permits` field on `ProducerBatch` is internal — switching to Option B later requires changes only in `accumulator.rs` and `batch.rs`, with no public API impact.
