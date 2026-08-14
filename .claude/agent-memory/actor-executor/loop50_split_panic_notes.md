---
name: loop50-split-panic-9-18
description: Loop 50 (PLAN §9.18) — build() idempotence fix, and the seven habits that found the surrounding mistakes across three Critic passes
metadata:
  type: project
---

Loop 50 fixed PLAN §9.18 (split-on-MESSAGE_TOO_LARGE panic) on branch
`investigate/split-panic-and-version-gate`. Seven transferable habits, each of which caught something the register or the review
had missed — 1-3 during the fix, 4-5 from Critic 50 pass 1, 6-7 from pass 3. See [[phase8_parity_sweep_notes]] for the
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

**4. Closing a defect makes every citation of it stale — grep the whole repo, not the
files you touched.** Marking §9.18 FIXED silently falsified a `mock_client.rs` comment
and a `design/current/status.md` list, neither of which the fix touched. The
`mock_client.rs` one was worse than stale: it cited §9.18 for a workaround whose real
cause was a *different, still-live* move, so a reader following it would have deleted
something load-bearing.
**How to apply:** on closing a PLAN §, `grep -rn "§9\.N" src/ design/ tests/` and read
every hit. A citation of a resolved section reads as permission to remove the thing it
guards.

**5. When an accounting block ships its own derivation, run it — including on the work
you just added.** The block's counts (`55 pairs`, `102 headers`) were stale, and its
both-ends sweep found that loop 50's own three new rustdoc headers cited the `@Test`
annotation line where the convention is the declaration line — while the entry
citations in the same file had it right. The file disagreed with itself in a way no
reading catches.
**How to apply:** a total that still sums is not a check if a re-listing can offset a
move. Derive the decomposition, not the sum.

**6. A domain has more than one axis; probing one proves nothing about the others.**
Pass 2 fixed a sweep whose assertion ran over a stale git snapshot — the domain's
*recency*. Pass 3 found the same sweep's regex silently excluded five headers that named
the method without its class — the domain's *shape*. Fixing recency never touched shape.
**How to apply:** when a classifier defines a population, enumerate every predicate it
applies (collector, literal, optional groups, alternations, value shape, keying,
greediness, file scope) and measure each, once, in one probe. Eight axes took one script;
finding them one review-pass at a time took three.

**7. Derive every number in an artifact that claims to be derived — including the ones
you did not touch.** The same block had two hand-written counts go stale and a
hand-written shape summary go stale the moment five rows were added, each one paragraph
from a rule forbidding exactly that. If a document ships a program, its prose should
point at the transcript rather than restate it.

**Also worth knowing:** `BufferPool` really does recycle allocations (`free:
VecDeque<Vec<u8>>`), contrary to what §9.19 claimed. Java's `assertNotSame` on a
freshly allocated buffer is not a sound Rust test — the allocator may hand back the
address the dropped batch just freed; assert against the pool's free list instead. And
`ProduceRequestBuilder::build_version` drains its builder via `mem::replace` where Java's
`Builder.build` shares the reference — the only 1 of 53 builders that does; filed as
PLAN §9.30, latent because `NetworkClient::do_send` is the sole production build site.
