---
name: guard-in-scrutinee-deadlock
description: Java's reentrant monitor hides a whole bug class from Java tests — MutexGuard alive in an if-let scrutinee while the arm re-locks; and how to write a test that can bound it
metadata:
  type: feedback
---

Two rules that came out of the `do_send_bytes` / `maybe_add_partition` deadlock
(Milestone 11, fix commit `3740cb3`).

**1. A lock guard created in an `if let` / `while let` / `match` scrutinee stays
alive for the whole success arm.** Edition 2024 only shortens it across the
`else`. So `if let Err(e) = m.lock().unwrap().f() { self.g(e) }` deadlocks the
moment `g` re-locks `m`. Bind the `Result` to its own `let` statement instead —
the guard then dies at the `;`.

**Why:** Java's `synchronized` monitor is **reentrant** and `std::sync::Mutex`
is not, so this bug class is invisible in the Java source *and* uncatchable by
any translated Java test. Every `synchronized` method that calls another
`synchronized` method on the same object is a candidate site.

**How to apply:** whenever a translated method holds a shared guard and the body
calls back into `self`, follow that call transitively looking for a second
`.lock()` on the same mutex. Sweep with a scrutinee-aware scan, not a
line-grep — scrutinees span lines. The producer sweep (5 prod sites) is in the
`3740cb3` commit message.

**2. `tokio::time::timeout` cannot bound a `std::sync::Mutex` deadlock.** A
thread blocked in `lock()` never yields, so the runtime never advances its
timers and the test *hangs* instead of failing. Bound such a test by running the
body on its own `std::thread` with its own current-thread runtime and waiting
with `std::sync::mpsc::Receiver::recv_timeout`. Leaking the wedged worker is
fine: libtest ends the process with `exit()` rather than joining stray threads.
See `bounded` / `bounded_block_on` in `src/producer/kafka_producer.rs` tests.

**Always verify a regression test by reverting the fix** and confirming it fails
*as a timeout*, not as a hang. Related: [[phase4_critic_round1_patterns]].
