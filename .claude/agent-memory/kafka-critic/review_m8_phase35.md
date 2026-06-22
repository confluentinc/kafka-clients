---
name: review-m8-phase35
description: Phase 35 STALE-member path + heartbeat field-diff review — side-channel wedge audit method, makeHeartbeatRequest-always-calls-onHBGenerated, doc-count half-correction trap
metadata:
  type: project
---

Phase 35 translated the STALE-member family + HB field-diff (+23 tests) and made a
real Java-fidelity production fix. Reviewed clean except one cosmetic doc issue.

**The real bug (worth remembering as a translation-gap pattern):**
Java `AbstractHeartbeatRequestManager.makeHeartbeatRequest(now, ignore)` ALWAYS calls
`membershipManager().onHeartbeatRequestGenerated()` — but the Rust leave/poll-timer-expiry
path built the leave HB and forgot this call, so LEAVING never reached STALE via poll().
**Audit heuristic:** when a Java method funnels through a single helper that has a
side effect (here onHBGenerated → transitionToStale), check EVERY Rust call site that
inlines/bypasses that helper still reproduces the side effect. The normal-HB path had it;
the early-return leave path didn't.

**Side-channel wedge audit (PendingMembershipTransition::Stale + 2 bool flags):**
To prove a "set flag in sync path / clear in async tail" pair can't wedge, verify:
  1. flag set ONLY where the paired channel-send also happens (STALE only reachable from
     LEAVING+pollTimerExpired, only the leave-path produces that → always sends Stale);
  2. channel tx+rx co-owned by the SAME struct → send can't fail (rx never dropped);
  3. bg task drains the channel EVERY iteration;
  4. clear is UNCONDITIONAL and in an always-reached tail (callback errors awaited+logged,
     not propagated/`?`).
If all 4 hold → cannot wedge. This is the generalizable checklist for any
flag-set-sync/clear-async bridge translating a Java `whenComplete` chain.

**PERF-neutral confirmation pattern for new side-channel variants:** confirm it reuses
the EXISTING Fenced/Fatal mpsc + Vec-drain under a lock the bg task ALREADY takes →
no-stale case is one empty try_recv. No new await/alloc/lock per loop. Network poll not
wrapped in select! (CLAUDE.md §10).

**Doc half-correction trap (the one issue found):** Actor corrected a test-count header
("26/93 predates → ~52/84") but left the stale denominator in a LATER line
("Not translated (~67/93)"). When a phase claims to correct counts, grep ALL occurrences
of the old number (here `93`) — don't trust that the header fix covered the body. Verify
the real count with `grep -c "@Test" <JavaTest>` (MembershipManagerTest=84, HBManagerTest=31).

**toStringBase fidelity:** building `expected` from `to_string_base()` rather than a
hardcoded vector is acceptable here BECAUSE Java's own test does the same
(`target = requestState.toStringBase() + suffix`); plus it pinned the suffix verbatim and
asserted no Optional/Some leak. Not a finding.

**Test-teeth check that passed:** poll_timer_expiration drives REAL mgr.poll() and asserts
state==Stale; since on_heartbeat_request_generated is the ONLY STALE setter, deleting the
fix fails the test → genuinely pins it. Contrast: the membership-manager stale tests use a
helper that calls on_heartbeat_request_generated DIRECTLY (not via poll), so they pass even
without the production poll()-path fix — only the 2 HB-manager poll_* tests pin the prod bug.
