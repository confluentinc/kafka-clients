---
name: loop50-split-panic-9-18
description: Loop 50 (PLAN §9.18) — build() idempotence fix, and the three habits that found/avoided the surrounding mistakes
metadata:
  type: project
---

Loop 50 fixed PLAN §9.18 (split-on-MESSAGE_TOO_LARGE panic) on branch
`investigate/split-panic-and-version-gate`. Three transferable habits, each of which
caught something the register had missed. See [[phase8_parity_sweep_notes]] for the
sibling lesson about blockage notes expiring.

**1. A register entry's *rationale* can be stale even when its *symptom* is real.**
§9.18 said the offending move "is what makes the send path zero-copy under CLAUDE.md
§12". It had been true when written and was false by the time it was read: the type it
described had since changed representation, so the clone the move avoided had become
free. Re-derive the rationale from today's code, not from the note.
**How to apply:** when a note justifies a design by a cost, measure the cost before
accepting it. `git log -S <symbol>` on the accessor names the commit that introduced it
and usually explains the original reason.

**2. A test parked as a `#[ignore]`d reproducer stops being read as a translation.**
This one had been re-verified twice by *running* it — which proves the panic and says
nothing about whether its assertions match Java. It had the wrong rig, a missing leg,
and an invented assertion.
**How to apply:** before un-`#[ignore]`ing anything, diff it against its Java source
line by line. Treat "re-verified by running it" as evidence about the blocker only.

**3. Tests sharing a process-wide singleton must be serialised, and the check is
cheap.** `CompressionRatioEstimator` is a global keyed by topic name. Java is safe
because JUnit runs a class's methods sequentially; `cargo test` does not. The full
suite passed — the two tests happened not to overlap among thousands — while
`cargo test --lib <shared-prefix> -- --test-threads=8` failed 8/8.
**How to apply:** whenever two new tests touch the same global key, run exactly those
two under `--test-threads=N` before trusting a green full-suite run. Serialise with a
`tokio::sync::Mutex` static, not `std`, when the guard must cross `.await`
(clippy `await_holding_lock`).

**Also worth knowing:** `BufferPool` really does recycle allocations (`free:
VecDeque<Vec<u8>>`), contrary to what §9.19 claimed. And Java's `assertNotSame` on a
freshly allocated buffer is not a sound Rust test — the allocator may hand back the
address the dropped batch just freed; assert against the pool's free list instead.
