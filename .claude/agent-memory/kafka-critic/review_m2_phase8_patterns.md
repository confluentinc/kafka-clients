---
name: M2 Phase 8 patterns
description: KafkaProducer close/flush never awaits async sender, accumulator not closed, ApiException callback contract broken
type: project
---

Phase 8 (KafkaProducer/Producer trait) core pattern: sync trait methods wrapping async operations without runtime bridging.

Key issues found:
1. **close_timeout never awaits sender_handle** -- JoinHandle stored but never .await'd, pending records silently dropped
2. **initiate_close skips accumulator.close()** -- Java calls Sender.initiateClose() which closes accumulator first; Rust only sets running=false
3. **ApiException callback contract broken** -- Java catches ApiException in doSend, invokes callback, returns FutureFailure; Rust propagates all errors via ? without callback invocation
4. **close_timeout force-close unreachable** -- After initiate_close sets running=false, the force_close branch condition can never be true
5. **flush does not actually wait** -- await_flush_completion just decrements counter, doesn't block on batch completion
6. **Duration cannot be negative** -- Rust Duration is unsigned, the < 0 check is dead code

**Why:** The fundamental issue is sync Producer trait methods trying to interact with an async sender task. Java's thread.join() has no direct equivalent when the "thread" is a tokio task and the caller is a sync context.

**How to apply:** When reviewing Java blocking operations translated to Rust with async runtime, always check: (1) is the await/join actually performed or just signaled? (2) are sync/async boundaries properly bridged? (3) are error-path callbacks honored?
