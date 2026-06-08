---
name: review-m8-phase11-batch3
description: Patterns from reviewing Phase-11 batch 3 (test translation + fixup confirmations for Issues 10-15). Covers wakeup-on-commit_async divergence, missing resetGroupMetadata, test-skip-rationale red flags.
metadata:
  type: reference
---

# Phase-11 batch-3 review patterns

## Skip-rationale red flag: claims-coverage-via-nonexistent-path

The unsubscribe skip rationale (async_kafka_consumer.rs:3716-3723) claimed
`testGroupMetadataIsResetAfterUnsubscribe` is covered via
`state_notifier.on_member_epoch_updated(None, ...)`. **But** that method
short-circuits on `None`:

```rust
let Some(epoch) = member_epoch else { return; };
```

Always grep the claimed code path when reviewing a skip rationale. Two
failure modes:
1. The path exists but no-ops for the relevant input (this case).
2. The path doesn't exist at all (a `fn`/method named in the rationale
   that grep doesn't find).

The Java equivalent (`resetGroupMetadata`) constructs a fresh
`ConsumerGroupMetadata` with UNKNOWN values — totally different code
path from the `updateGroupMetadata(memberEpoch, memberId)` overload.

Heuristic: if a skip rationale names a specific method, open that
method's source. If the body doesn't observably write to the cache
the test asserts about, the rationale is invalid.

## Shared-helper-as-uniformity-trap: commit_inner offsets_ready wakeup

The actor justified `enable_wakeup=true` on commit_inner's preliminary
`offsets_ready_rx` wait as "makes the wakeup-observable semantic uniform
across phases". Reasonable for `commit_sync` (Java sets active task
later) — BUT commit_inner is SHARED with `commit_async`, which Java
never wakes up.

Pattern: a shared helper that bundles a wakeup-observation decision will
leak the decision to ALL callers. Always check the call sites of any
helper whose `enable_wakeup` (or any wake-observable parameter) is
hard-coded. If two callers have different contracts, the helper must
parameterize.

## Test-quality smell: parameterized variant uses wrong type

`commit_async_user_supplied_callback_with_exception_group_authz` was
named for the GroupAuthorizationException parameter, but the body uses
`KafkaError::illegal_argument("Group authorization exception")` — a
message that LOOKS like the type but is actually IllegalArgument.

Always check parameterized-test splits: the Rust variant name must map
to a distinct error variant or distinct behavior. If both variants use
the same KafkaError variant, the parameterization is fake.

Heuristic: grep `assert!(matches!(err, Kafka::Some_variant)` in both
variants. If both match the same variant, file as test-correctness gap.

## Skip rationale must enumerate Java tests not just behaviors

The actor's skip sections list rationales by feature (e.g. "metrics
deferred") but don't enumerate individual Java test names. When
checking coverage, build the diff:

```
java_tests = grep "public void test" Java.java | extract names
rust_referenced = grep "test[A-Z]\w*" src/.../foo.rs | extract names
unreferenced = java_tests - rust_referenced
```

Each name in `unreferenced` either needs a 1:1 Rust translation OR an
explicit named skip in the rationale block. "Covered by inline test X"
must name X. Vague "covered by [feature]" without a test name is a
coverage gap.

In batch 3, 9 Java tests were unreferenced (no translation, no skip):
testCloseAwaitPendingAsyncCommitIncomplete, testCloseLeavesGroupDespite*
(x2), testBeginningOffsets[Timeout|WithZero]*, testOffsetsForTimes[*],
testGroupRemoteAssignorUsedInConsumerProtocol.

## Exact-message assertion still missing for some tests

DoD §3 was reinforced in batch 1 (Issue 5) but new tests in batch 3
(e.g. `beginning_offsets_propagates_timeout`) only assert
`matches!(err, KafkaError::Timeout(_))`. Java asserts EXACT message:
`"Failed to get offsets by times in 100ms"`.

Pattern: any new test that uses `matches!(_, KafkaError::X(_))` without
inspecting the inner message has lost the message contract. Always
check the Java equivalent's `assertEquals("...", t.getMessage())` —
if it exists, the Rust must `match err { KafkaError::Timeout(msg) =>
assert_eq!(msg, "...") }`.

## Fixup-cycle pattern: dual-issue-per-commit is dense but works

batch 3 closed 6 issues in 3 fixup commits (2 issues each). Each fixup
has:
- Targeted code change for one issue
- A dedicated regression test per issue
- Cited Java-line references in the commit message AND in the in-source
  doc-comment AND in the COMMENTS.DONE.1.md entry

This 3-way citation is the gold-standard pattern — it lets the critic
verify the fix at the Java contract level without re-reading the issue
text. Recommend keeping.

## Drainer-task fixture pattern: drop-consumer-before-await is necessary

batch 3 tests use the pattern:

```rust
let drainer = tokio::spawn(async move {
    while let Some(env) = handles.app_event_rx.recv().await { ... }
});
consumer.close().await.expect("close ok");
drop(consumer); // <-- ensures drainer can exit
let _ = drainer.await;
```

Without `drop(consumer)`, the drainer waits forever on `recv()` because
the sender is still held by the consumer struct. The actor flagged this
as an open uncertainty; verified the pattern is correctly applied.

When reviewing test code that uses a drainer-task pattern, look for the
`drop(consumer)` or equivalent sender-drop before any `drainer.await`.
Tests without it will hang and the test runner will time out.

## Process notes

- "submit_and_drain" matrix audit is a fixed-effort exercise: list all
  setActiveTask sites in Java AsyncKafkaConsumer, then walk every
  call site of submit_and_drain / process_background_events_until in
  Rust and verify the `enable_wakeup` argument matches.
- "auto_commit_enabled defaults to true in fixture" — innocuous in
  isolation, but it means close-path tests enqueue an extra CommitSync
  envelope. Drainers must handle this branch.
- Phase 11 batch 3 added ~36 test functions across 3 commits, bringing
  the AsyncKafkaConsumerTest translation from ~62% to ~91% coverage by
  count (estimated). Quality varies — some are real translations, some
  are stubs (Issue 27).
