---
name: m11-bindings-critic-round3-patterns
description: Round-3 review patterns from the M11 admin-bindings slice — timed Java joins must become tokio::time::timeout, monotonic CAS deadlines, and doc reachability claims must be proven not assumed
metadata:
  type: feedback
---

Three recurring review patterns confirmed on the M11 admin bindings slice
(Critic 1 round 3). See also [[m11_bindings_b0_b1_notes]] and
[[workflow_teeth_check_mtime]].

**1. A *timed* Java join/get is a contract, not just an await.**
CLAUDE.md §9.4 ("`thread.join()` must actually be awaited") is satisfied by an
unbounded `handle.await`, which is how the bound got dropped from
`KafkaAdminClient::close`. When the Java call is `join(ms)` / `get(timeout, unit)`,
wrap it in `tokio::time::timeout(...)`.

**Why:** the deadline a Rust I/O loop reads internally is only a *hint*, because
our `Selector::connect` awaits the TCP handshake — so any phase of a
`processRequests`-style loop that calls `client.ready(...)` can block past a
shutdown deadline, where Java's non-blocking NIO cannot. Only the caller-side
timed wait is a guarantee. Java's self-deadlock guard
(`Thread.currentThread() != thread`) usually needs no analogue in Rust because
per-`Call` hooks are sync closures, but say so explicitly rather than silently
omitting it.

**How to apply:** grep the Java method for a timeout argument on the join/get
before translating. If a rustdoc or a generated C header states a bound, the code
must enforce it — a doc promise the code cannot keep is reported as a behavior
mismatch, not a doc nit. Recover the `JoinHandle` on expiry (`timeout(dur, &mut
handle)`, `JoinHandle` is `Unpin`) and put it back so a later call can still join.

**2. Java CAS loops around a deadline/state field are usually monotonicity, not
just atomicity.** A plain `store` where Java has `compareAndSet` + a
"already earlier than requested" break arm inverts the intent (`close(60s)` after
`close(100ms)` re-widened the budget). Translate the loop shape, and note which
Java statements exist only to feed a debug log (e.g.
`newHardShutdownTimeMs = prev`) so the omission is deliberate.

**3. Doc claims about reachability get audited like code.** "Only reachable while
X is running" was wrong because the async helpers clone the completion sender
*before* `spawn` and hold it for the task's life, so handle destruction can never
disconnect the queue. Before writing a reachability parenthetical, trace who owns
the last sender/receiver. Prefer stating the real trigger over deleting the
claim when the *consequence* is still true and useful to a C reader. Verify the
wording landed in `target/include/confluent_kafka.h`, not just the source, and
check the count of the *old* phrase is 0.
