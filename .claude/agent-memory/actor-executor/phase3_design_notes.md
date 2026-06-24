---
name: phase3-design-notes
description: Milestone-8 Phase 3 MockConsumer translation — key decisions, file locations, and gotchas
metadata:
  type: project
---

Milestone-8 Phase 3 closed with `eff6200`. Translates
`org.apache.kafka.clients.consumer.MockConsumer` + `MockConsumerTest`.

**Why:** MockConsumer is the public test helper users plug in for their own
test code; ships alongside the [[phase2-design-notes]] Consumer trait.

**How to apply:**
- `MockConsumer<K, V>` lives at `crate::consumer::MockConsumer` (re-exported
  from `mock_consumer.rs`).
- `subscriptions: SubscriptionState` is plain (NOT `Arc<Mutex<...>>`) because
  the Consumer trait API is `&mut self` — the borrow checker enforces
  single-writer access statically per `consumer-threading.md` §16.
- `wakeup: AtomicBool` (SeqCst) — `Consumer::wakeup(&self)` is callable from
  any task, so interior atomicity is required even on the single-task path.
- `PollTask<K, V> = Box<dyn FnOnce(&mut MockConsumer<K, V>) + Send>`
  deviates from Java's parameterless `Runnable` — Rust closures can't
  safely capture the outer struct, so tasks receive `&mut consumer`
  explicitly. Type alias avoids `clippy::type_complexity`.
- `rebalance` is `async` because `ConsumerRebalanceListener` methods are
  `async fn` per Phase 2; tests use `#[tokio::test]`.
- `current_lag(&self, tp)` does NOT call `update_fetch_position` — the
  trait method is `&self` and the helper requires `&mut`. Java's
  `currentLag` does call `position(tp)`. Tests don't hit this edge.

**Prep:** Added `KafkaError::Wakeup(String)` + `KafkaError::wakeup(msg)`
constructor in `8cc4fcd` (Java's `WakeupException` extends KafkaException
with no protocol code; mirrors IllegalState/IllegalArgument/Serialization).

**Test location:** `tests/consumer/mock_consumer_test.rs` (public type ⇒
external integration test, not inline `#[cfg(test)] mod tests`). Module
entry in `tests/consumer/main.rs`. 8 test cases + 1 trait-object-safety
check = 9 new test fns.

**Dropped null assertions in `testRe2JPatternSubscription`:** Java's
`assertThrows(IllegalArgumentException.class, () ->
consumer.subscribe((SubscriptionPattern) null))` and the analogous null
listener case can't be expressed — `subscribe_pattern` takes
`SubscriptionPattern` by value and `subscribe_pattern_with_listener` takes
`Arc<dyn>`. Documented inline.

**ConsumerRecord is not Clone:** tests build the same record twice rather
than clone. Adding `Clone` would force a `K: Clone, V: Clone` bound on
many callers and isn't worth it for the test.

**Commit plan:** prep + 1/3 + 2/3 + 3/3, all gates green at each step.
Final counts: 1004 lib tests, 36 consumer integration tests.
