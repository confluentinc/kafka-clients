---
name: phase4-critic-round1-patterns
description: Five reusable fix patterns from Critic 44 pass 1 on Milestone-11 Phase 4 — audit the entry point not the class you touched, "deallocate later" needs a named holder, per-response error isolation, harness knobs must match Java's setup, deferrals need a real owner
metadata:
  type: feedback
---

Critic 44 pass 1 on Milestone-11 Phase 4 raised five findings. All were real. These
are the generalisable ones.

**Why:** four of the five would have been caught by applying an existing rule to a
place I had not looked, not by knowing anything new.

**How to apply:** check these before declaring a send-path or response-path phase
done.

## 1. A DoD §10 allocation audit must measure the *entry point*, not the class you edited

My drain-path budget test was sound and passed; the regression was one call above it,
in `KafkaProducer::do_send_bytes`, where `TopicPartition::new(topic.to_string(), ..)`
allocated a `String` **and** an `Arc<str>` per record. `TopicPartition::new` takes
`impl Into<Arc<str>>`, so a `&str` argument silently costs two allocations.

Rule of thumb: if the change adds a call anywhere reachable from `send()`, the budget
test goes on `send()`. Measure it as a **delta** (with vs without the feature), never
an absolute count. And mutation-check *both* ways — a single sized allocation does not
move an allocation *count*; only a per-record loop does.

## 2. "Complete now, deallocate when the response arrives" needs a named second holder

Java gets one free: the `RequestCompletionHandler` closes over `recordsByPartition`,
so `inFlightBatches.clear()` only drops the map's references. Three Java paths depend
on it (`abortBatches`'s `isInflight()` fork, `failBatch(deallocateBatch=false)`,
`abortIncompleteBatches`).

A Rust callback cannot capture `&mut self`, so if the port stores only an identity in
the pending-request map, the batch is *dropped* and the pooled buffer never returns.
Any comment saying "deallocated when the response arrives" must name the field that
keeps the batch reachable. Symptom is invisible: `available_memory` shrinks silently.

## 3. `client.poll`'s response dispatch is per-response try/catch, not `?`

`NetworkClient.completeResponses` (`NetworkClient.java:666-674`) catches and logs each
`response.onComplete()` separately. Using `?` in the Rust dispatch loop makes one
failing handler abandon the rest of the poll — their batches then wait for
`delivery.timeout.ms`. When making a response handler fallible, check where Java's
`catch` actually sits; it is usually *lower* than `Sender.run`.

## 4. Test-harness timing knobs must match Java's `setup*` exactly

`SenderTest.setupWithTransactionState` builds the accumulator with
`retryBackoffMs = 0L` and the `Sender` with `RETRY_BACKOFF_MS = 50`. My harness
collapsed them into one value, so every re-enqueued batch waited a backoff Java does
not impose — which silently makes multi-in-flight retry tests untranslatable (the
resend never appears). Read the Java setup method's argument list, not just the
constants.

## 5. An itemised deferral needs an owner whose scope actually lists the file

Naming the skipped tests is necessary but not sufficient: my record pointed at Phase
8, whose PLAN scope covers `TransactionManagerTest` only. The Critic's decisive
argument was not bookkeeping — one deferred test was the one that would have caught
finding 2. If a deferral covers tests for branches the phase just introduced,
translate them instead. Where genuinely blocked, cite the *missing surface* per test
(e.g. "the Sender's clock is `Arc<dyn Fn() -> i64>` with no `sleep`, so
`tokio::time::sleep` cannot move a test's `MockTime`"), never a phase number alone.

## Incidental

Two `.lock()` calls in one expression deadlock a non-reentrant `std::sync::Mutex`
(both guards live to the end of the statement). Cost a hung debug run.
