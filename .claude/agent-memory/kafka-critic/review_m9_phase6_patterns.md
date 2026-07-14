---
name: review-m9-phase6-patterns
description: M9 Phase 6 share-consumer core — zero-copy-vs-RENEW gap, entries() order reversal, §31 test-teeth verification
metadata:
  type: project
---

# M9 Phase 6 (ShareConsumerImpl + trait + mock + config + factory)

**Zero-copy move-out breaks RENEW (systemic pattern).** The blocker-1 fix moved
`ConsumerRecord`s out of `ShareInFlightBatch` to the user (`take_in_flight_records`,
since `ConsumerRecord` is not `Clone`, §27) and kept a parallel `in_flight_offsets:
BTreeSet<i64>`. This is correct for ACCEPT/RELEASE/REJECT but **silently breaks
RENEW**: Java's `takeAcknowledgedRecords` moves the record *object* into
`renewingRecords` (`inFlightRecords.get(offset)`), and RENEW re-delivers it on a
later poll. In Rust the object is gone, so `if let Some(record) = in_flight_records.remove(...)`
fails → no renewing entry → `has_renewals()` false → no re-delivery, and the offset
is dropped from `in_flight_offsets` so a later ACCEPT returns "record cannot be
acknowledged." The wire RENEW ack IS still sent. Verdict: **broken in production**,
not just untestable. An `#[ignore]`d test + a "resolved …object-survival" comment
masked it. **Heuristic:** whenever an actor keeps a shadow scalar set (offsets/ids)
to survive a zero-copy move, check every Java path that needed the *object* itself —
renewal/re-delivery/re-ack paths are where it breaks. CLAUDE.md §5 says such a path
must fail loud, not silently complete.

**`RequestManagers::entries()` share order reversed.** Java share ctor
(`RequestManagers.java:123-128`) builds `coordinator → shareHeartbeat →
shareMembership → shareConsume`. The actor pushed `share_consume` then
`share_heartbeat` (reversed) and the comment/commit-msg misread Java ("after
share_consume"). §10 requires phase-for-phase registration order. Also: Java adds
`shareMembershipManager` as its OWN entries() element; Rust reaches it only via
`share_heartbeat.membership_manager()` and never polls it standalone (mirrors the
consumer_membership Arc-shared/reconcile-separately pattern, but no share reconcile
call exists yet). Latent while both slots default `None` (Phase 7), but committed
wrong. **Heuristic:** always open the Java `entries()` builder and diff the literal
`list.add(...)` order; don't trust the actor's comment.

**§31 callback-drain test teeth (how to prove non-vacuous).** Two required tests:
(1) callback invoked inline on caller's task during poll — has teeth because
`#[tokio::test]` is current-thread + no yield between poll-return and the assert, so
a `tokio::spawn`ed callback would NOT have run (`calls==0`) → test fails. (2) fires
exactly-once-per-commit — has teeth because the completed list is `mem::take`-cleared
after invocation, so a re-fire on the next poll would be `calls==2`. Confirmed both
fail under the wrong impl. Minor gap: neither exercises the callback on the
records-returned poll path (cosmetic).

**Group-id validation faithfulness.** Java `maybeThrowInvalidGroupIdException` only
checks `null || isEmpty` — whitespace-only ("   ") passes the *runtime* check and is
rejected only at ConfigDef construction. So a Rust runtime guard that checks only
`is_empty()` is faithful; the `testGroupIdOnlyWhitespaces` deferral (production ctor)
does not indicate a missing runtime check. Don't flag whitespace-rejection as a
runtime gap.

**MockShareConsumer.poll ignores the wakeup flag** — this matches Java
(`MockShareConsumer.java:84-97` never throws WakeupException). Don't flag the missing
wakeup-check in the mock's poll.

**new_share_consumer returns unsupported_version** while `new_consumer`'s KIP-848 arm
builds a real AsyncKafkaConsumer — asymmetry worth tracking as a Phase-7 requirement,
not a bug. `ShareConsumerImpl::from_components` itself is a complete core.

## Phase 6 fixup RE-REVIEW (225dd26 RENEW, b637a27 entries) — one regression found

**RENEW fixup is faithful** (capture-clone-on-RENEW in `renew_records`, gated
`ConsumerRecord: Clone`; `in_flight.or(captured)` routing; re-delivers repeatedly
because each poll re-drains `in_flight_records`; `Acknowledgements::add` overwrites
so RENEW-then-ACCEPT supersedes; no `renew_records` leak because every insert has an
`acknowledged_records` twin drained by `take_acknowledged_records`).

**Fixup-over-correction regression (the real finding):** the RENEW fix also
restructured `ShareConsumerImpl::collect` to mutate `current_fetch` in place. It made
the *first* (non-renewal) branch do `self.current_fetch = fetch` **unconditionally**.
Java only assigns `currentFetch = fetch` in `poll` when `!fetch.isEmpty()`
(`ShareConsumerImpl.java:628-629`); `collect`'s first branch returns the fresh fetch
and never touches `currentFetch`. Consequence: an empty collect (no new data) now
overwrites `current_fetch` with an empty fetch, **losing `acquisition_lock_timeout_ms`**
(and `_renewed`) that Java retains in the persistent `currentFetch`. The public getter
`acquisition_lock_timeout_ms()` then returns `None` where Java returns the last
`Some(t)`. Pre-fixup code matched Java; only the renewal/`else` branch's
`mem::replace(current_fetch, empty)` was buggy — the fix over-corrected the non-renewal
branch. **Heuristic:** when a blocker fix "restructures the core path in place" to fix
one branch, diff EVERY branch against Java's assign-guard — state carried on the
persistent object (timeouts, renewals) is what gets clobbered by an unconditional
replace. The passing regression test only checked the timeout after the *first*
(non-empty) poll, so it didn't catch the empty-poll path.

**Clone bound leak check:** the public `ShareConsumer<K,V>` trait stays `Send+'static`
(no Clone); regular `Consumer`/`AsyncKafkaConsumer` untouched. But `ShareConsumerImpl`
`impl`s the trait only for `K/V: Clone`, so when Phase-7 wires `new_share_consumer` to
construct it, the factory signature must gain `K/V: Clone` — a divergence from Java's
unbounded `ShareConsumer`. Track, don't block.

**entries() order fix is correct** — `share_heartbeat` before `share_consume`, matches
`RequestManagers.java:123-128`; `share_membership: _` genuinely destructure-skipped,
consistent with `consumer_membership: _`. The standalone share-membership reconcile
driving is a REAL Phase-7 gap (skip means bg loop must drive reconcile or the group
never joins) — flag as tracked requirement.
