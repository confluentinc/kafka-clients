---
name: m11-tier2-phase3-group-member-deletion
description: M11 Tier2 Phase3 (DeleteGroups/RemoveMembers) review — chained-driver deadline reuse bug, flexible byte-vector gap, and the clean areas
metadata:
  type: project
---

Milestone 11 Tier 2 Phase 3 (group/member deletion) review, commits `95c2957..HEAD`
(9ed0648 wire, 6bb701c RPCs, ced9e89 client tests, 4bc378e integration).

**Confirmed defect — chained-driver deadline reuse (removeAll path).**
`KafkaAdminClient.remove_members_from_consumer_group` computes one
`deadline = calc_deadline_ms(now, options.timeout(), default_api_timeout_ms)`
up-front and reuses it for BOTH the describe driver and the (callback-spawned)
LeaveGroup driver. Java differs two ways: (1) the describe step uses
`describeConsumerGroups(singleton)` → **default API timeout**, not the
removeMembers `options.timeoutMs`; (2) the LeaveGroup deadline is computed
**inside `whenComplete`, after describe finishes** (fresh window), because
`invokeDriver(handler, future, timeoutMs)` recomputes `calcDeadlineMs(now(),...)`
at call time. Rust bakes `deadline` into `AdminApiDriver::new(...)` before
describe. **Lesson: when a Java admin RPC chains a second `invokeDriver` inside a
`whenComplete`, the second driver's deadline must be recomputed at callback time,
and a nested `describeConsumerGroups(singleton)` uses default-API-timeout, not the
outer request's timeout.**

**Reported test gap — LeaveGroup flexible byte-vector.** DeleteGroups has BOTH
v0 (non-flexible) and v2 (flexible) known-vector tests; LeaveGroup only has a v3
(non-flexible) request+response vector. The `Reason` field is v5-only (flexible)
and is never byte-tested (the reason-propagation test inspects built `data()`,
not serialized bytes; MockClient never serializes the queued request). Headline
feature of the phase untested on the wire.

**Clean / not-issues (verified):**
- `ExponentialBackoff` gaining `#[derive(Clone)]` — pure additive on an
  immutable-params value object (i64/i32/f64 fields); harmless.
- `maybe_truncate_reason` uses `chars().count()`/`take(255)` (scalar values) vs
  Java UTF-16 `substring(0,255)` — documented deviation, agrees for ASCII.
- Base/subclass split: `DeleteGroupsHandler` (base, holds api_name/display_name)
  + `DeleteConsumerGroupsHandler::new` factory returning the base (with
  `#[allow(clippy::new_ret_no_self)]`) — faithful, no async_trait bleed.
- MockClient `deleteConsumerGroups`/`removeMembersFromConsumerGroup` return
  "Not implemented yet" — CORRECT: Java's MockAdminClient (lines 773/801) throws
  `UnsupportedOperationException` for both. (Verified the §9 trap does NOT apply.)
- `SimpleAdminApiFuture::handle(&key)` → `pub(crate)`, shares Arc state; chain
  fires via `Completable::set` → `on_complete` callbacks. Faithful `whenComplete`.
- removeAll describe-failure wraps message but preserves underlying error code
  (Java uses generic KafkaException) — minor, untested, not flagged.
- All `ConcreteRequest`/`ConcreteResponse` match arms wired for both new APIs;
  both genuinely net-new (no DoD #6 dup).

**Fix-cycle (798e68d + 5665418) — both items genuine & complete, re-review clean.**
- Item 1: describe now `calc_deadline_ms(now, None, default_api_timeout_ms)`;
  LeaveGroup deadline recomputed inside callback as
  `calc_deadline_ms((ctx.time_provider)(), options_timeout, default_api_timeout_ms)`.
  Matches Java invokeDriver (KAC.java:5075) `calcDeadlineMs(time.milliseconds(),...)`.
- Item 2: v5 request vector `[02 67, 02, 02 6D, 02 69, 02 72, 00, 00]` and response
  `[00 00 00 00, 00 00, 02, 02 6D, 02 69, 00 19, 00, 00]` — both hand-verified
  byte-for-byte against spec (compact strings n+1, Reason 5+, tagged fields 0x00). Correct.
- **Teeth-audit heuristic (reusable):** `invoke_driver`'s `now` arg is IGNORED
  (`maybe_send_requests(_now)`); the effective knob is the `deadline` baked into
  `AdminApiDriver::new`. The runnable's timeout phase checks `deadline_ms <= real_clock`.
  So a deadline-regression test gets teeth ONLY if it advances the mock clock past the
  buggy (baked) deadline — test 2 sleeps to 10000 > buggy 6000 so pre-fix expires the
  driver and issues NO LeaveGroup lookup (expect() panics). `request_timeout_ms()` is a
  faithful deadline proxy = min(request.timeout.ms, deadline-now) — distinguishes 5000
  vs 20000 only when request.timeout.ms doesn't cap. Both tests confirmed to have teeth.
