---
name: review-m8-phase12-patterns
description: M8 Phase 12 review patterns — production-ctor wire-up bugs: slot-skip without driver, dual-notifier on shared-state translation, separated Arc<AtomicI64> for "shared" telemetry slot
metadata:
  type: feedback
---

## When `entries()` skips a slot, verify the bg-task drives the skipped manager

Phase 12 refactored `RequestManagers.coordinator` and `RequestManagers.commit`
to `Arc`-wrapped slots, both SKIPPED from `entries()`. The bg-task picked up
`coordinator` via an explicit `coordinator_handle().lock().poll(now)` call,
but NOT `commit`. The dyn-trait `CommitRequestManager::poll` returned
`PollResult::empty()` "by design" — leaving Java's actual `poll(currentTimeMs)`
body (which drains `unsentOffsetCommits` into `UnsentRequest`s) un-translated
in production code.

**Why:** "Commit work is driven via `ApplicationEventProcessor` event arms which
call `poll_with_coordinator` directly" was the rationale — but grep showed
`poll_with_coordinator` was only called from `#[cfg(test)]` code. The AEP arms
only ENQUEUE via `commit_async_no_callback` / `commit_sync` / `fetch_offsets`;
they never drain.

**How to apply:** When the Actor refactors a `RequestManagers` slot to
`Arc<...>` and skips it from `entries()`, grep for the inherent
`poll_with_coordinator` / `drain_pending_*` / similar drainer:
- `grep -rn "<drainer_fn_name>" src/` — must show at least one production call site outside `#[cfg(test)]`.
- If the comment says "driven via event arms", verify each event arm. Calling `commit_*` from an arm does NOT drain — it enqueues. The drain still needs `poll_with_coordinator` (or its analog) to be invoked from the bg-task loop.

The symptom: `commit_sync().await` and `committed().await` hang forever because
their oneshot::Receivers never resolve.

## Dual-instance bug for "shared state" Java fields translated as separate `Arc<Mutex<...>>`

Java's `AsyncKafkaConsumer` has ONE `AtomicReference<Optional<ConsumerGroupMetadata>>`
field, written by both the ctor's initial `set(...)` and the
`memberStateListener::onMemberEpochUpdated`. The Rust translation built TWO
`ConsumerStateNotifier` instances against TWO distinct
`Arc<Mutex<Option<ConsumerGroupMetadata>>>` slots:

- Notifier #A: registered on the membership manager (bg-task drives it).
- Notifier #B: built inside `new_with_components`, slot becomes the consumer's
  public-accessor backing field.

The app-side `consumer.group_metadata()` reads #B's slot, the bg-task writes
#A's slot — observable mismatch.

**Why:** Phase-11 test rig built notifier #B inside `new_with_components` to
expose it via `consumer.state_notifier()` for test injection. Phase 12 needed
to register a notifier BEFORE bg-task spawn (per the §31 invariant "first
heartbeat cycle drives `update_group_metadata`"). Building a second notifier
locally seemed easier than restructuring the component contract — but the
"easier" path forgets that the user-visible slot needs the SAME backing Arc as
the bg-task-driven one.

**How to apply:** When the Java type has a single backing field (e.g.
`AtomicReference`, `AtomicLong`, a shared mutable slot) and the Rust
translation has two construction sites (test rig + production ctor), assume
the bug is dual-instance until proven otherwise:
- Grep for the backing-Arc construction: `Arc::new(Mutex::new(...))` or
  `Arc::new(AtomicI64::new(...))`. Expect ONE per field per consumer instance.
- If you find two, the symptoms will be "writes succeed, reads return stale"
  or vice versa — the kind of bug that no unit test catches if both halves
  hold separate state.
- Same pattern caught Phase 12 Issue 3 (`max_time_to_wait_ms`): the ctor's
  `Arc<AtomicI64>` and `ConsumerNetworkThread::new`'s `cached_max_time_to_wait_ms`
  are different cells; bg-task writes to its own.

## `let _ = (foo, bar);` to "intentionally drop" an Arc telegraphs orphaned state

A line like `let _ = (group_metadata, group_assignment_snapshot);` (Phase 12
commit 2/N, async_kafka_consumer.rs:924) with a comment "intentionally
dropped" usually means the Actor noticed they couldn't keep the Arc reachable
externally and chose to drop the local handle. If the dropped Arc is then
only reachable through a private field of a struct stored elsewhere, the
writes via that path are effectively orphaned. This is a code smell:

- The Arc shouldn't be locally constructed if it needs to be reachable from
  multiple modules.
- Restructure to build the shared state at a higher level (e.g. pass through
  a components struct) and propagate it both into the listener and into the
  user-visible accessor.

## Auth-failure no-op closures are "not just one extra round-trip"

Phase 12's `FetchRequestManager::maybe_throw_auth_failure` closure is
`|_n| Ok(())`. Java's `AbstractFetch.java:452-457` consults this BEFORE
attempting a send; Java surfaces SASL/SSL auth errors observable on the
consumer side BEFORE the broker connection has been established (delegate
caches auth state from prior attempts).

The Actor's framing "one extra round-trip per disconnected-node fetch attempt
— not a correctness bug" is incomplete: it's correct for `is_unavailable`
(send-time filter handles it) but UNDER-states the gap for
`maybe_throw_auth_failure` (auth errors stay invisible until next reconnect
attempt). For PLAINTEXT-only Phase 12, this never bites; but the comment must
acknowledge the gap is real before SSL/SASL wiring lands.

## Phase 12 commit (1-3) what to grep first

When reviewing a "translate the Java primary ctor in N slices" phase, the
high-yield greps are:

1. `grep -rn "<inherent_poll_fn>" src/ | grep -v "#\[cfg(test)" | grep -v "_request_manager.rs"`
   — if zero, the inherent function never gets called from production. Slot
   refactors that skip `entries()` without adding an explicit bg-task call
   are the highest-frequency bug class here.

2. `grep -n "Arc::new(Mutex::new\|Arc::new(AtomicI64::new\|Arc::new(AtomicBool::new" src/consumer/async_kafka_consumer.rs | wc -l`
   — count per backing Arc. If the same logical field is constructed in both
   the production ctor AND `new_with_components`, dual-instance bug.

3. `grep -n "tokio::spawn" src/consumer/async_kafka_consumer.rs` — should
   show exactly ONE per consumer instance (§10).

4. `grep -n "register_state_listener\|register_member_state_listener" src/consumer/`
   — verify (a) the listener is registered BEFORE bg-task spawn, (b) the
   listener's backing Arcs are the SAME instances the consumer's accessors
   read from.

## Round-2 (fixup) review patterns

When the Actor's fixup commit changes `&mut self` → `&self` to enable
shared-Arc dispatch, audit:
- The method body must not `.await` while holding the interior mutex
  (`consumer-threading.md` §16). Read top-to-bottom; flag if any `.await`
  appears between guard acquisition and the `Drop`/explicit `drop(guard)`.
- Count production callers via grep — `&self` allows concurrent calls
  from multiple tasks; verify only the bg-task calls it in practice.

After a "shared Arc, copy-back wiring" fix lands, also grep for stale
comments that previously justified the lack of wiring. Phase 12 round 2
left a 7-line comment at `async_kafka_consumer.rs:1092-1098` claiming
the slot "stays at 0 ... no copy-back wiring" — directly contradicting
the new fix. These doc-only contradictions are easy to miss because
they don't break the build or tests; grep keywords from the OLD comment
("stays at 0", "no copy-back", "until a future commit") before approving.

Regression tests for a shared-Arc fix should cast/unwrap through the
**production accessor** path (e.g. `consumer.state_notifier()`,
`consumer.maximum_time_to_wait_ms()`). A test that only verifies the
Arc identity via internal fields proves nothing about end-to-end
visibility — Phase 12 Issue 2's regression test correctly went
listener → notifier dispatch → consumer.group_metadata().
