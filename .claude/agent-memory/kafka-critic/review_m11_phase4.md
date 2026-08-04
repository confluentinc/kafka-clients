---
name: review-m11-phase4
description: M11 Phase 4 (idempotent send-path integration) review — five findings; the highest-yield checks were "who is the second owner of a batch" and "where does Java actually catch this"
metadata:
  type: project
---

Critic 44 pass 1 on `84fa389..144c8a8`. Five findings. The three concurrency
priorities (rules §1/§3/§4) were **clean on exhaustive check** — the defects were
all in ownership and error-boundary translation instead.

**Where the rules-§3/§4 sweep is cheap and conclusive (do this first, then move
on):** `TransactionManager` holds no accumulator reference, so the manager → deque
lock edge cannot exist by construction; only deque → manager does. That single
observation discharges the whole ordering question. For §4, grep every `.lock()`
in the production half and check only the `let x = …lock()` bindings — statement
temporaries drop at the `;`. Note `if let Err(e) = m.lock().unwrap().f(…)` *does*
hold the guard through the `if let` body (fine when the body only logs).

**Heuristics that paid, in yield order:**

1. **"Deallocate later / complete later" needs a named second owner.** Java's
   request-completion callback closes over the `ProducerBatch`, so
   `inFlightBatches.clear()` (`Sender.java:536`) merely drops references and the
   response still deallocates. Rust's `pending_produce_responses` keeps only
   `Arc<ProduceRequestResult>` identities, so any path that removes the batch from
   `Sender::in_flight_batches` and then relies on "the response will do it" is a
   leak. Three such paths exist: `maybe_abort_batches` (new in Phase 4),
   `fail_expired_batches(.., deallocate_buffer=false)`, and the
   `MESSAGE_TOO_LARGE` split arm (missing Java's `maybeRemoveAndDeallocateBatch`).
   Whenever a comment says "the response arrives later", grep for who still holds
   the object.

2. **`NetworkClient.completeResponses` (`NetworkClient.java:666-674`) catches
   per response.** Any Rust doc claiming a completion-callback exception "escapes
   `client.poll` to `Sender.run`'s catch-and-log" is wrong. Java isolates each
   response; a Rust `?` inside a `for response in responses` loop abandons the
   rest of the poll's responses. Check this at every response-dispatch loop.

3. **A deferral's destination is a checkable claim.** "carried to Phase 8's parity
   sweep" — open the plan's Phase-8 section and read its scope. Here it named
   `TransactionManagerTest` only, and the PLAN diff never amended it, so 33 named
   `SenderTest` methods had no owner. Contrast the `RecordAccumulatorTest`
   deferral in the same commit, which names the file, the reason and Phase 6 —
   that is the bar.

4. **Cross-reference the deferred test list against the phase's new branches.**
   The single live code defect (Issue 2) is asserted by
   `SenderTest.testCancelInFlightRequestAfterFatalError` (Java 2182-2219, its
   `MatchingBufferPool.allMatch()` assertions), item 23 on the deferred list. A
   named-but-untranslated test list is a map of where the bugs are: diff it
   against the methods the phase added and look at the intersection first.

5. **A DoD clause can be satisfied "as written" while the regression lands next
   door.** DoD §10 says "any class that sits on the producer send path"; the phase
   audited `RecordAccumulator::drain` (soundly — real `#[global_allocator]`
   tracker, delta design, genuine `count > 0` liveness) and missed
   `KafkaProducer::do_send_bytes`, where `TopicPartition::new(topic.to_string(), …)`
   now allocates twice per record on the default path. When an audit is presented,
   ask which entry point it covers, not whether it is correct.

**Rust-specific alloc trap to keep re-checking:** `TopicPartition::new` takes
`impl Into<Arc<str>>`; `to_string()` into it costs two allocations and two copies.
Java's equivalent (`new TopicPartition(record.topic(), partition)`) copies no
characters, so this cost is invisible in the Java source — exactly the class
CLAUDE.md §11 exists for. The interned `Arc<str>` is already available from
`RecordAccumulator::get_or_create_topic_info`.

**Verified sound — don't re-flag:** the `0b8c3d0` `Arc::ptr_eq` routing fix
(`produce_future` is unique per batch; the drain yields ≤1 batch per partition per
`send_producer_data`, so `batches.last_mut()` is the right batch);
`MockClient::respond_to_request_at` (byte-identical to `respond_with_disconnect`,
and `make_header` preserves the original correlation id); §9.18's diagnosis;
`assign_producer_state_to_batches` vs `ProducerBatch.java:395-405`;
`with_in_flight_batch_pool`'s both-owners merge, which
`test_out_of_order_sequence_is_retried_and_bumps_the_epoch` pins with real teeth
(remove the deque half and `start_sequences_at_beginning` errors);
`run_once`'s four exits vs Phase 3's `run_sender_transaction_phase` harness (they
agree); all eight production `Caller` sites.

**Judged marginal and deliberately NOT filed** (calibration):
`maybe_transition_to_error_state` running before the user callback where Java runs
it after (unobservable — the callback is sync and cannot re-enter `send`); the
lost immediate callback on the `maybe_add_partition` failure path (Java fires it
twice, Rust once, and the batch's own callback still fires);
`test_healthy_partition_retries_during_epoch_bump` stopping short of Java's tail
(the omitted assertions are covered verbatim by its sibling and the extra
`maybeUpdateProducerIdAndEpoch` is inert).
