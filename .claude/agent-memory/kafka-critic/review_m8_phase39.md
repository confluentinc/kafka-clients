---
name: review-m8-phase39
description: Phase 39 integration commit+callback+failed-listener test parity — maybeWrapAsKafkaException conditional, §31 reentrancy structural gap, post-seek-poll race vs Java pause()
metadata:
  type: project
---

Phase 39 = 22 integration tests (14 runnable, 8 #[ignore] callback-reentrancy,
1 #[ignore] commit). Three files: plaintext_consumer_commit_test.rs (KIP-848
arm of PlaintextConsumerCommitTest), plaintext_consumer_callback_test.rs
(PlaintextConsumerCallbackTest), consumer_test.rs +2 failed-listener.

**maybeWrapAsKafkaException is CONDITIONAL** (ConsumerUtils.java:256): wraps
with the given message ONLY if the cause is NOT already a KafkaException; an
existing KafkaException returns unchanged. So `AsyncKafkaConsumer.java:2334`
(`maybeWrapAsKafkaException(e, "User rebalance callback throws an error")`)
replaces the message for a thrown IllegalArgumentException but NOT for a
KafkaException. Rust invoker (consumer_rebalance_listener_invoker.rs) returns
the listener's raw Err unchanged; process_background_events
(async_kafka_consumer.rs:2086) records it as-is — no wrap anywhere. Real
contract divergence on the rebalance error path; recommend small prod fix
(error path = perf-neutral). When auditing any "wrap as X" Java util, check
whether it's conditional on the cause type before flagging the Rust side.

**§31 reentrancy gap is genuinely structural even for READ-only calls.** A
driver/controller-channel does NOT rescue assignment()/position()/
beginningOffsets() from inside the listener: the listener runs inline on the
caller's task INSIDE poll()/commit_*() (§31), which holds &mut self for the
whole consumer; Box<dyn Consumer> isn't Clone, so there's no second handle.
The driver IS the blocked task → deadlock. Verdict: file as known API
limitation (Java users routinely call position()/pause()/assignment() in
onPartitionsAssigned), not under-delivery. The 8 #[ignore] stubs are correct
for the phase. Watch the growing #[ignore] pile from normalizing this gap.

**Post-seek-poll race vs Java pause():** auto-commit-on-rebalance test.
Java's listener calls consumer.pause(partitions) in onPartitionsAssigned
SPECIFICALLY because the post-seek awaitAssignment polls and would advance
the sought partition's position past the seek (tp has 1000 records) before
auto-commit captures it. The Rust mitigation "no poll between seek and
re-subscribe" misses that await_assignment AFTER re-subscribe still polls →
same race, unguarded. Contrast auto-commit-on-CLOSE: there seek is the last
op (no intervening poll) so it's deterministic/faithful. Heuristic: when a
Java rebalance test uses pause() in a callback, find the poll that pause is
guarding; if the Rust translation drops pause but keeps that poll, it's racy.

**Faithful (confirmed):** commit_metadata (leaderEpoch 15 + foo/bar +
null→empty round-trip), position_and_commit (IllegalState on unassigned
position, 0/5 progression), both async-completion ordering tests (1→2→3),
new-partitions-only (containsAll capture guard + set-equality assert mirrors
Java lines 224/241). Gating/mod-wiring/multi_thread match siblings exactly.

Java refs: ConsumerUtils.java:256 (maybeWrapAsKafkaException),
AsyncKafkaConsumer.java:2304-2339 (invokeRebalanceCallbacks),
PlaintextConsumerCommitTest.java:354-388 (pause-in-callback),
ConsumerIntegrationTest.java:153-190 (always-failed listener asserts message).
