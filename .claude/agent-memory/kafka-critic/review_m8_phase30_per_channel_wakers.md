---
name: review-m8-phase30-per-channel-wakers
description: Milestone-8 Phase 30 Critic review — per-channel wakers (selectedKeys on tokio) replacing the readiness sweep; arming state machine, cancel-safety, armed_count, fired-queue verification heuristics
metadata:
  type: project
---

Phase 30 (N=30, commits a9cd3e2 + 0778ff2, src/common/network/selector.rs only):
replaced the Phase-24 per-WAIT readiness sweep with per-channel wakers feeding a
shared fired-queue. Reviewed CLEAN. Verification heuristics that found no defects
(reusable for any future selector-readiness change):

**Cancel-safety audit (the highest-risk surface):** The WAIT future drains the
fired-queue ONLY into selector-owned `ready_ids`/`ready_scratch`, never a
future-local. The key insight making this sound: `ready_queue.fired` is a
PERSISTENT Vec that survives across `poll()` calls — an undrained fire is drained
at the NEXT WAIT, not lost. The loop-exit salvage (re-mark leftover ready_ids
dirty) is the belt; the persistent queue is the suspenders. Audit EVERY loop-exit
path for lost drains: made_progress break, deferred_wakeup break, wakeup-not-mid-
receive break, wakeup-mid-receive (process_all=true continue), timeout-0 `_=>`
arm, error return. All covered.

**armed_count drift audit:** increment guarded by `!armed_*` in arm_channel;
decrement guarded by `if *flag` in drain_fired_queue AND by per-flag checks in
unregister_arming. No double inc/dec. has_interested_channel O(1) form
(`armed_count>0 || !interest_dirty.is_empty()`) is exactly equivalent to the old
sweep at the decision point because arm_rearm_set drains interest_dirty BEFORE
the select! decision, so it reduces to `armed_count>0`, and armed_count>0 ⟺ some
channel interested (consumed→processed→rearmed; unchanged→stays armed;
ready-during-arm→continue, never reaches decision).

**Dirty-site completeness:** re-derive from channel_interest's inputs. The 6
sites: send() Ok arm, unmute(), clear()/clear_completed_receives()/drain_
completed_receives() (ALL THREE — each leaves list empty so next clear() finds
nothing; each must dirty its own sources), connect()/register_arming. mute() and
handshake transitions need NO hook: mute=interest OFF (stale fire drained once,
attempt_read.should_read==channel_interest.want_read so no read, not re-armed);
handshake channel is ALWAYS in pass-1 processed set on the iteration its interest
changes (poll_channel_reads drives the transition AND the channel is in
ready_ids/immediately_connected).

**Stale-fire no-busy-spin is STRUCTURALLY impossible** (not just untested): a
fire is a one-shot queue entry drained once; a non-interested channel is never
re-armed → generates no further fires. So a missing explicit park-duration test
for the muted-stale-fire case is a coverage nicety, NOT a defect. Flag as
non-blocking observation only.

**Root-waker/fired Mutex:** verify `fired` and `root` never held simultaneously
and root.wake() happens OUTSIDE both locks (take-under-lock, wake-after-drop).
will_wake guard on set_root. Sound.

**Pre-existing dirty files trap:** completed_fetch.rs / fetch_collector.rs /
Cargo.toml are UNCOMMITTED working-tree diagnostics the PLAN explicitly excludes.
format-check fails on completed_fetch.rs:286 — this is NOT a phase defect. Verify
the commits touch ONLY the intended file (`git show --stat`), and that
selector.rs itself passes `rustfmt --edition 2024 --check` in isolation.

Full suite 1770 lib tests (1767+3) pass; lint clean.
