---
name: M2 Phase 4 patterns
description: Watch channel notify-on-set vs latch semantics, null-return vs empty-response, error type erasure in producer API layer
type: feedback
---

Key review patterns found in M2 Phase 4 (Producer API Types):

1. **tokio::sync::watch send_modify notifies immediately** -- When translating Java CountDownLatch two-phase set/done protocol to Rust watch channels, `send_modify` unblocks waiters immediately. The Java pattern relies on `set()` writing fields without unblocking, then `done()` unblocking. Watch channels conflate write+notify.
**Why:** ProducerBatch.completeFutureAndFireCallbacks uses the gap between set() and done() to run user callbacks before flush() returns.
**How to apply:** When Java uses CountDownLatch with separate data-write and latch-countdown steps, the Rust translation must preserve that separation (e.g., store data in a Mutex, notify via a separate Notify/oneshot).

2. **Java null return vs Rust non-optional return** -- `getErrorResponse()` returns null for acks=0 in Java. Rust return type `ConcreteResponse` cannot represent this. Need `Option<ConcreteResponse>`.
**How to apply:** When translating methods that return null in specific cases, always check whether the Rust return type can represent that.

3. **Error type erasure** -- Java `Function<Integer, RuntimeException>` preserves exception type hierarchy. Translating to `Fn(i32) -> Option<String>` loses error type info needed by downstream `FutureRecordMetadata`.
**How to apply:** When Java passes exception objects, translate to `KafkaError` or typed error, not String.
