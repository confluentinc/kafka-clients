---
name: Phase-7e Round-2 patterns
description: Verified-good fix shapes for Java join+force-join translation; cancellation-safety acceptance criteria for tokio::select! over JoinHandle; counter-based termination-proof tests
type: project
---

# Phase-7e Round-2 review patterns

Carried out as Critic 7 verifying Round-1 fixups for the
`KafkaProducer::close` graceful-deadline path and the FQCN partitioner
test redundancy.

## Verified-good fix: Java `ioThread.join(t)` + `forceClose()` + unbounded `join()` → `tokio::select!` over `&mut JoinHandle` and `tokio::time::sleep(t)`

**Why:** Java's two-step "graceful join then force-join" guarantees the
IO thread is terminated by the time `close()` returns. `tokio::time::timeout(t, handle).await`
consumes the JoinHandle on the elapsed path, so you can't re-await it
afterward — you'd return `Ok(())` while the spawned task is still
polling. The fix is `select!` between `&mut handle` and a parallel
sleep timer; on elapse, abort + await the cancelled handle.

**How to apply:** When reviewing translations of Java `Thread.join(t)`
followed by force-close + unbounded join, look for `tokio::time::timeout(t, handle)`.
That pattern is **always** wrong for this Java idiom because it loses
the handle. The correct shape is:

```rust
if let Some(mut handle) = task_slot.lock().unwrap().take() {
    tokio::select! {
        join_result = &mut handle => { /* graceful arm */ }
        _ = tokio::time::sleep(timeout) => {
            force_close.store(true, Release);
            wakeup();
            handle.abort();
            let _ = handle.await;  // unbounded final join
        }
    }
}
```

## Cancellation-safety acceptance criteria for `tokio::select!` over `&mut JoinHandle`

`JoinHandle` is the canonical cancellation-safe future — losing the
race only drops the borrow, not the task. `tokio::time::sleep` is also
trivially cancellation-safe. Therefore `select!` between these two is
clean per CLAUDE.md rule 9.6, **provided** neither arm body has side
effects gated on the other arm winning.

When verifying:

1. Inspect each arm expression (the `EXPR =>` part) — it must be a
   pure future evaluation with no side effects.
2. Inspect each arm body (after `=>`) — side effects there only run
   when that arm wins, which is the desired behavior.
3. `biased;` is needed only when one arm should be preferred on
   simultaneous readiness; for join-vs-deadline, the random selector
   is fine because both winning paths terminate the task correctly.

## Test-pin pattern: external `Arc<AtomicCounter>` proves spawned-task termination

**Why:** Once `close()` consumes the JoinHandle, you can't query the
task's liveness directly. The mock's internal counter (incremented on
every `poll` entry) is observable from the test holding a clone of the
counter Arc. Snapshot before close-return, sleep past the poll cadence,
snapshot after — equality proves the task has stopped.

**How to apply:** When reviewing termination tests for spawned tasks,
look for:

1. Counter on the mock (`Arc<AtomicUsize>`).
2. Counter clone captured before the mock is moved into the producer.
3. Pre-close priming (sleep so the task ticks at least once) with
   `assert!(counter.load() >= 1)` — guards against the trivial-zero
   case where the test passes for the wrong reason because nothing
   has run yet.
4. Counter equality assertion across a sleep window > 1 poll cadence
   after close returns.

Defensive priming is the load-bearing part. Without it, a fast race
makes the test green even on broken code.

## Production-vs-test field-leakage check

When a test mock gains a new field (e.g. `polls: Arc<AtomicUsize>`),
verify the field is **mock-only state**, not production state leaked
into the test path. Easy check: the field's owning struct is in a
`#[cfg(test)]` mod, and the field is incremented inside the mock's
trait impl (which only runs in test builds). If the production
`KafkaClient` impl had to grow the field, that would be a real defect.

## FQCN-vs-simple-name test redundancy resolution

When two configuration-resolution tests differ only in the literal
string (FQCN vs simple name), at least one must exercise the resolved
trait surface (e.g. dispatch through `Partitioner::partition`) so the
test isn't a near-duplicate of the other. The "construction succeeds"
assertion alone is identical between FQCN and simple-name; only the
dispatch assertion proves the resolved instance is functionally usable.
