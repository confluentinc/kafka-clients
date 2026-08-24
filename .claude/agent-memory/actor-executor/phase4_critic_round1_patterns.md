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

## 6. Do not classify a test by grepping for a type name — the hit may be a `null`

`testProducerBatchRetriesWhenPartitionLeaderChanges` sat on the "idempotence subset"
list for two rounds because `transactionManager` appears in its body — as the literal
`null` argument to both the `RecordAccumulator` and the `Sender` constructor
(`SenderTest.java:3316`, `:3320`). It is neither idempotent nor transactional. Check
the constructor *arguments*, not the identifier. The same sweep also mis-filed
`testUnresolvedSequencesAreNotFatal`, in the other direction.

## 7. Mutation-check the doc claim, not just the assertion

A rustdoc saying "this pins arm X" is a claim the mutation check can refute. Disabling
`canRetry`'s `sequenceHasBeenReset()` arm left
`testUnknownProducerErrorShouldBeRetriedForFutureBatchesWhenFirstFails` green, because
after a sequence reset the truncation arm answers instead and both return `true`.
Java's test cannot tell them apart either — so the honest fix is to say so in the doc,
not to strengthen the test past Java. Overstating what a test covers is the same class
of defect as overstating when an effect materialises (PLAN §9.15's lesson).

## 8. A completeness claim over a list needs the command that checks it, not prose

Two review rounds were spent on one accounting comment: it lost an entry while asserting
"nothing else is owed", and its prose counts drifted from the lists beside them. What
finally held: state the scope criterion, ship the two shell commands that (a) derive the
scope set from the Java file and (b) extract the enumerated entries from the Rust file,
and show `comm` over both. Then every count is read off a list instead of maintained
next to one.

Two traps when writing such a command into the comment it checks:

  - a `sed` range whose end pattern is a phrase the embedded command itself contains
    terminates at the command, not at the block. Prefer a shape-based `grep`
    (`` `test[A-Za-z]+` \([0-9]+ ``) over a range.
  - an `awk` that tracks "current method" must reset at the method's closing brace
    (`/^    }$/{n=""}`), or helper methods after a test get attributed to it. Without
    the reset the count was 54 instead of 52.

Also: if the artifact newly claims a citation convention ("line numbers are the
declaration line"), apply it to every citation in the file by script and re-run the
audit — 32 of 32 header ranges in `sender.rs` were off by one or two lines, several
pointing at a blank line.

## 9. Java's `close()` notifies nothing — do not reach for it as a justification

`NetworkClient.close()` is `selector.close(); metadataUpdater.close();
telemetrySender.close();` (`NetworkClient.java:736-746`). It never walks
`inFlightRequests` and never calls `completeResponses`, and `Selector.close()` uses
`CloseMode.DISCARD_NO_NOTIFY` (`Selector.java:886-892`, mode at `:96`). Anything the
producer still held is abandoned; the leak is invisible only because the `BufferPool` is
built inside `KafkaProducer`'s constructor (`:438`) and dies with it.

**Why:** this was the third invented justification for the same shutdown release loop
across two review passes, and Phase 3's `close()` took three rounds for the same reason.
**How to apply:** when the honest ground is "Java leaks this and our explicit release is
a deliberate improvement", write that. Check whether Java's own shutdown path would in
fact release it before claiming that it does.

## Incidental

Two `.lock()` calls in one expression deadlock a non-reentrant `std::sync::Mutex`
(both guards live to the end of the statement). Cost a hung debug run.
