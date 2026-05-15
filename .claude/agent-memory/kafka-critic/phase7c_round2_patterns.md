---
name: Phase-7c Round-2 review patterns
description: Verified-good fix shapes for log-vs-tracing dead-export, silent-bump warn, log_unused translation, accessor cross-doc, JoinHandle drop test
type: project
---

Verified-good Round-2 patterns from Phase 7c (KafkaProducer skeleton).

---

## 1. Java exported constant with no Tokio analogue → demote to module comment

When a Java public constant feeds a feature that has no native Tokio
equivalent (e.g. `NETWORK_THREAD_PREFIX` → Java thread name → no
`tokio::task` thread-name slot), the right move is **not** to keep the
constant as dead code. The fix shape:

1. Delete the `pub const`.
2. Replace with a module-level comment that explains:
   - What Java does with it (set IO thread name).
   - Why Rust can't do the same (Tokio tasks have no native name).
   - What the equivalent observability hook would be (`tracing::info_span!`).
   - Why this crate doesn't use it yet (uses `log`, not `tracing`).
   - What's preserved without the constant (e.g. `LogContext` already
     prefixes log lines with `[Producer clientId=...]`).
3. Confirm no `pub use` re-export remains and no other module references
   the constant (`grep -rn`).

This matches CLAUDE.md rule 5: "if a Java code path is not yet
implemented, fail the affected operations with an explicit error" —
applied analogously here as "if a Java exported symbol has no consumer,
remove it with a documented deviation note rather than leaving dead code".

**Why it works**: future Phase 7e or `tracing` adoption can re-introduce
the constant alongside its consumer. The module comment makes the
omission auditable.

---

## 2. Java `log.warn` on silent-config-fixup branches MUST be translated verbatim

Java's `KafkaProducer.configureDeliveryTimeout` emits `log.warn(...)` on
the silent-bump path (`KafkaProducer.java:584-587`) so operators see the
auto-bump in their logs. **The warn message is part of the Java
operator-facing contract** — it must be translated with matching format
string and key constants.

Verification protocol:

1. Read the Java method.
2. Find every `log.warn`, `log.error`, `log.info` call inside it.
3. Check the Rust translation has the same call at the same code-path
   location.
4. Check the format string matches verbatim (Kafka operators may grep
   their logs).
5. Check the substituted values are equivalent (e.g. Java's
   `deliveryTimeoutMs` after assignment maps to Rust's
   `linger_plus_request` after the same calculation).

This is a recurring high-yield audit on the Producer translation —
operator-facing log messages tend to slip through tests because tests
assert behaviour, not log emission.

---

## 3. `config.logUnused()` translation — verify the call site, the order, AND the touch coverage

Java line 458: `config.logUnused()` is called **after** Sender start, at
the construction tail. The Rust equivalent must:

1. Be called at the same construction-tail location (after `tokio::spawn`).
2. Read the same `originals` vs. `used` set (verified by the underlying
   `unused()` predicate already being tested in Phase 1).
3. Have all `config.get_*()` accessors above it that should mark keys as
   touched. Audit by walking the construction body and listing every
   accessor — they must precede the `log_unused` call. Otherwise users
   will see false-positive "unused config" warnings for keys the
   producer actually consumed.

**Acceptable test coverage**: if the underlying `unused()` predicate is
tested (which it is, in Phase 1's `unused_lists_keys_not_touched`), the
3-line warn-emission shim doesn't strictly need its own warn-capture
test. The behaviour-defining function is covered.

---

## 4. Coherent-accessor cross-doc rule

When a struct exposes two accessors that read the same underlying state
through different borrow shapes (e.g. `Sender::is_running(&self) -> bool`
and `Sender::running_arc(&self) -> Arc<AtomicBool>`), each accessor's
rustdoc MUST cross-reference the other and explain:

1. The borrow shape (`&Self` vs. owning `Arc` handle).
2. When to use each (in-process tests own `&Sender`; spawned-task callers
   need the `Arc` to flip the flag from outside the spawn).
3. The memory-ordering guarantee that makes the two accessors coherent
   (both `Ordering::Acquire` reads of the same atomic).

This is a defect-prevention pattern, not just a doc nicety: future
maintainers seeing only one accessor may think the other is redundant
and prune it. Cross-doc keeps the relationship explicit.

---

## 5. Strengthening a Drop-side-effect test

Pattern: `#[tokio::test]` that wants to verify `Drop` on a struct
abort()s a spawned task.

**Weak shape (Round 1)**: capture an external observer (the running
flag), drop the producer, assert the flag flipped. This proves the Drop
impl ran but says nothing about whether the spawned task actually exited.

**Strong shape (Round 2)**:

```rust
let mut producer = ...;
let running = sender_running_arc(&producer);
let handle = producer.sender_task.take().expect("...");  // steal before drop
drop(producer);                                            // assertion 1: flag flips
assert!(!running.load(Ordering::Acquire));
handle.abort();                                            // mirror Drop's own abort
match tokio::time::timeout(Duration::from_secs(1), handle).await {
    Ok(Err(e)) => assert!(e.is_cancelled(), ...),          // abort path
    Ok(Ok(())) => {},                                      // cooperative-exit path
    Err(_) => panic!("did not finish within 1s"),          // task hung
}
```

**Caveat to flag in review**: by `take()`-ing the handle before drop,
the test bypasses Drop's own `task.abort()` and calls `handle.abort()`
directly. Acceptable when the Drop impl is literally
`if let Some(task) = self.sender_task.take() { task.abort(); }` — the
test reproduces both halves separately. If the Drop impl does anything
non-trivial between `take` and `abort` (e.g. calls a callback, releases
a resource), the test must be restructured to keep the handle alive.

---

## 6. Pre-existing-recorded-only items survive archive moves

When an archive commit moves resolved sections to `COMMENTS.DONE.<N>.md`,
verify what's left in `COMMENTS.<N>.md`:

1. The current round's intentionally-non-actioned items (e.g. Phase 7c
   Nit 2 — `compression: Box<dyn Compression>` one-shot read held for a
   future phase).
2. Prior phases' recorded-only items (e.g. Phase 7a Nit 1+4 — deprecated
   typo-alias defer; unreachable branch).

This file becomes the running ledger of "intentional non-actions still
on the record" across multiple phases. Don't accidentally archive these
in a future round.
