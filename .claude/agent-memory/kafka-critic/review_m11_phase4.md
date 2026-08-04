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

## Pass 2 — the fixes were right; three of four findings were in their records

Four findings (issues 6-9) over `144c8a8..521b36e`. All three code fixes were
mechanically correct and their pins faithful; the defects were one Java-divergent
branch the fix left behind and three record-level errors.

11. **When a fix rewrites an enumerated list, diff the old list against the new one
    mechanically — do not read it.** One `SenderTest` method
    (`testEpochBumpOnOutOfOrderSequenceForNextBatchWhenBatchInFlightFails`, Java
    1105) vanished between the pass-1 deferral list and the pass-2 accounting
    comment, while both the comment and PLAN §9.19 asserted "nothing else is owed".
    `git show <old>:file | sed -n '<range>p' | grep -o 'test[A-Za-z]*' | sort -u`
    into `comm -23` against the new range found it in one command. The tell was
    arithmetic: §9.19's "translated 28 of them" is consistent with 33 only after
    silently subtracting the lost entry (33 − 3 blocked − 1 reclassified − 1 lost).
    **A count that "works out" is evidence the author derived it from the wrong
    set.** Also count groups programmatically — the header said 28 over a list of
    32, and a group the prose called 17 had 18 entries.

12. **A fix that splits one branch into two can get one arm right and keep the
    other wrong.** Issue 2's fix correctly moved the unmute for *expired in-flight*
    batches onto the response path (Java `Sender.java:735-737`) but kept
    `else if guarantee_message_order { unmute }` for *undrained* ones, where Java's
    `failExpiredBatches` unmutes nothing. The new comment ("undrained batches never
    had a request, so no response will ever unmute them") is true and irrelevant:
    the mute belongs to whichever batch is in flight. Reachable ordering violation —
    A in flight and expired (retained, no unmute), B queued and expired (unmutes),
    C queued and not expired (drains next iteration) → two in-flight requests for
    one partition under `max.in.flight=1`. When a conditional gains an arm, ask what
    Java does in *each* arm, not whether the new arm is an improvement.

13. **A justification that names a Java method's behaviour is checkable in three
    greps, and the third rewrite is as likely to be wrong as the first.** The
    close-time buffer release claims "Java's `client.close()` … runs their
    completion callbacks with disconnected responses". `NetworkClient.close()`
    (`NetworkClient.java:736-746`) only calls `selector.close()` /
    `metadataUpdater.close()`; `Selector.close()` (`:369-385`) uses
    `CloseMode.DISCARD_NO_NOTIFY`, defined at `Selector.java:96` as "no disconnect
    notification". Same failure mode as Phase 3's three `close()` justifications:
    the Rust behaviour was fine, the reason invented. Grep the cited Java method
    body every time, even when the code is obviously harmless.

**Verified resolved in pass 2 — don't re-check:** `RecordAppendResult::topic_partition`
(refcount-only; `TopicPartition` is `{i32, Arc<str>}`; the three surviving
`topic.to_string()` sites in `kafka_producer.rs` are error paths) and its
manager-vs-no-manager delta test (isolates the right class — the only remaining
difference is `maybe_add_partition`, whose idempotent body allocates nothing);
`batches_awaiting_response`'s deallocate-exactly-once path (`complete_batch` clears
`is_inflight` first; `is_done()` excludes the split arm and `can_retry`; both
`complete*` return false onto the deallocating `else` arms);
`handle_client_responses`'s per-response catch vs `NetworkClient.java:666-674`;
the split retry backoffs (`SenderTest.java:3860` vs `:3864` — citations correct);
`MockClient::set_max_in_flight_one` vs the anonymous subclass at `:3956-3973`;
`test_producer_batch_retries_when_partition_leader_changes` vs Java 3325-3390.

## Pass 3 — one finding; the fix made its own accounting reproducible

Loop: 5 → 4 → 1. The Actor answered issue 7 not by patching the counts but by
shipping the derivation: a stated scope criterion (52 = every `SenderTest` method
whose body references a `TransactionManager`), two `awk`/`grep`/`comm` commands
inside the comment block, and every count read off the lists. **Run the shipped
commands — that is the whole review of that artifact.** Mine reproduced 52 / 54 /
empty `comm -23` / exactly the 2 out-of-scope names, and an independent script
confirmed 33+18+3=54, all names unique, no name in two groups, and all 54 entry
line numbers equal to the `public void` declaration line.

14. **A normalisation script fixes numbers, not attributions.** The one residual
    across 34 `Translated from` headers was
    `testClusterAuthorizationExceptionInInitProducerIdRequest` cited as
    `(Java 2158-2179)` — which is its *sibling*
    `…InProduceRequest` (declaration 2159), separately translated in the same file
    with that same range. A script that rewrites `(Java A-B)` per method name
    cannot catch a range belonging to a different method; only a name-against-range
    comparison can. The reusable check, which finds all of these in one command:
    build `name -> (decl_line, first '    }' at-or-after)` from the Java file by
    regex, then match every `` `SenderTest.<name>` … (Java A-B) `` in the Rust file
    against it. Adjacent tests sharing a name prefix are where this lands.

15. **Don't eyeball line numbers off a `sed` range — the off-by-one will be
    yours.** I nearly filed the restored test's `(Java 1105-1228)` as off by one
    after mis-assigning rows in `sed -n '1103,1230p'` output; `grep -n` on the
    declaration showed 1105 is correct. Locate declarations with `grep -n`, or by
    script, never by counting displayed lines.

16. **Adjudicating "no test catches this mutation": look for the loud-failure
    property, not just the pinned gate.** The `!has_inflight_batches` guard in
    `maybe_update_producer_id_and_epoch` is unreachable-when-false behind a gate
    that *is* pinned (`sender.rs` "the new batch stays queued"), Java documents the
    same redundancy in its own comment, **and** in this port the guard is what
    licenses the `&mut []` pool argument — so violating it returns `Err` from
    `start_sequences_at_beginning` (rules §7) rather than corrupting sequences. An
    unpinned defensive check is acceptable when its violation fails loudly; say so
    rather than filing a coverage gap.

**Verified resolved in pass 3 — don't re-check:** the restored test vs Java
1105-1228 (11 `assertPartitionState`s, both stale-epoch assertions, same `runOnce`
counts) and its `adjust = attempts() < retries` mutation coupling;
`fail_expired_batches` unmuting in neither arm with `Sender.java:737` the only
`unmutePartition`; the A/B/C pin (16 KiB values against a 16 KiB `batch_size` force
three batches; the second post-expiry `run_once` is the load-bearing assertion);
issue 9's Java claims (`NetworkClient.java:736-746`, `Selector.java:886-892` +
`:96`, `KafkaProducer.java:438` — all correct now); the deliberate `(Java
3325-3339)` sub-range ending at `}));`.

## Pass 4 — clean; Phase 4 closed on 5 → 4 → 1 → 0

Doc-only fix (`374ca12`). All four passes' findings were real and conceded; no false
positives across the loop.

17. **An audit's denominator is part of its claim.** The Actor did not just fix the
    header — it re-derived why the pass-2 sweep missed it (the rewrite and the audit
    shared a matcher requiring `(Java A-B)` to close the parenthesis, so the one
    header with a trailing clause inside the parens was in neither set) and
    corrected its own earlier "0 mismatches over 34" to "over 33". Verify such a
    correction by reproducing *both* matchers: broadened (paren need not close) gives
    34 citations, and counting citations whose range is not immediately followed by
    `)` gives exactly 1 — so the narrow matcher's reach was 33 and nothing else was
    hidden. That two-number check is the cheap way to confirm a sweep's coverage
    claim rather than its results.

18. **Distinguish a deliberate sub-range from a wrong one by what it annotates.**
    The single surviving mismatch against declaration..closing-brace is
    `(Java 3325-3339)` on the *helper* `update_metadata_with_leader_epochs`, whose
    doc says "mirroring the `metadataUpdateWithIds(..)` calls in …" — not a
    `Translated from` header, and the test's own header separately cites the full
    `3308-3394`. A convention statement scoped to "the `Translated from` header of
    every test" correctly excludes it. Check the annotated item's kind before
    flagging a narrow range.
