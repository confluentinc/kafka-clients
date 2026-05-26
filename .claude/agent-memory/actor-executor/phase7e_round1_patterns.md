---
name: Phase 7e Round 1 patterns
description: Patterns from the Phase 7e review fixup — JoinHandle awaited via select! for Java join() parity, FQCN-vs-simple-name test differentiation, post-fix termination-proof via external observer counter
type: feedback
---

## tokio::select! over `&mut JoinHandle` for "await with deadline" parity

When mirroring a Java `Thread.join(timeoutMs)` followed by an
unbounded `Thread.join()` after `forceClose()`, **don't** use
`tokio::time::timeout(timeout, handle)` — it consumes the handle on
the elapsed path, and you can't re-await it to confirm task
termination. Instead:

```rust
if let Some(mut handle) = task {
    tokio::select! {
        join_result = &mut handle => {
            // graceful exit before deadline
        },
        _ = tokio::time::sleep(timeout) => {
            // deadline elapsed — flip force flags, wake, abort, await
            self.force_close.store(true, Ordering::Release);
            self.wakeup();
            handle.abort();
            let _ = handle.await; // <-- the second `join()`
        },
    }
}
```

**Why:** `JoinHandle` is cancellation-safe; the losing arm of
`select!` only drops the borrow, not the task. The borrow
(`&mut handle`) preserves the handle across both arms. After
`abort()`, awaiting the cancelled handle resolves to
`JoinError::cancelled` quickly — close-return implies
task-terminated, matching Java's post-condition.

**How to apply:** any "graceful close with deadline" path in Rust
that translates Java `Thread.join(N) + Thread.join()`. Don't use
`tokio::time::timeout` over the join future — restructure as
`select!`.

## Termination-proof tests need an external observer

Asserting "the spawned task has terminated" via the JoinHandle
itself is hard because the handle is consumed by close. Instead,
add an `Arc<AtomicUsize>` counter to the test mock that increments
on every entry to the long-running poll method. The test:

1. Snapshots the counter before close.
2. Calls close.
3. Sleeps past one normal poll cadence.
4. Snapshots again — equality proves the task stopped polling.

This is more reliable than wall-clock-only timing assertions and
gives a clear failure message ("polls advanced from N to M" → task
is still alive).

## FQCN-vs-simple-name test differentiation

When two factory tests differ only in the input string ("FQCN" vs
"simple name") and both assert `is_some()`, they're functionally
duplicates. Either:

- **(b) preferred** — make the FQCN test ALSO dispatch through the
  resolved trait surface (`partitioner.partition(...)`), asserting
  the result is in valid range. The simple-name test stays
  construction-only. The two tests now exercise different layers.
- **(a) acceptable** — strip the FQCN test to construction-only and
  scope its rustdoc to "asserts FQCN parses; behavioural coverage
  in <other test>".

**Why:** option (b) catches a future regression where the FQCN
factory branch returns the wrong concrete type (e.g. a partitioner
that compiles but panics under partition() because of bad
dispatch). Option (a) leaves that gap.

## Test-fixture additions belong with their first-consumer commit

When a test needs a new field on a long-lived test mock (here:
`StubKafkaClient::polls`), the field's introduction belongs in the
SAME fixup commit as the test that motivates it — not in a
"prep" commit. This keeps each commit's purpose narrow: "fix
behaviour X + the test that pins it" reviews better than "add
counter, then later add test that uses it" because the diff
shows the assertion the counter is for.

## NOTES.md carry-over hygiene

After resolving a Round-1 finding within the same milestone (i.e.
NOT deferred to a later milestone), update `NOTES.md` with a
"(Resolved in Round 1 fixup `<sha>`)" line under the relevant
caveats section. Don't leave the original deferral note as if it
were still open — future readers will skip the section thinking
the divergence still exists. Pair it with a pointer to the test
that pins the resolution.
