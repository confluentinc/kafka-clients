# Phase 8 (partial foundation) — Critic #1 resolutions

Reviewed at SHA `b73c719` by Critic agent N=2. 5 findings on the
foundation work (~750 LOC of ~3361 planned). Actor stopped early
(cited §5 — refusing to ship a half-baked `AbstractMembershipManager`
§31 handshake); Manager applied fixes directly because sub-Actor
sandbox blocks worktree writes.

## Fixed (in `cbc0809`)

### #1 (Bug) — `should_client_throttle` returned `true`
Java's `ConsumerGroupHeartbeatResponse` does NOT override
`shouldClientThrottle`; inherits `AbstractResponse` default = `false`.
Heartbeat responses are not throttled. Changed to `false`.

### #2 (Bug) — `update_heartbeat_interval_ms` didn't take `current_time_ms`
Java's `Timer.updateAndReset` self-updates from `time.milliseconds()`
before computing the new deadline. The Rust impl silently used a
stale `timer_last_update_ms` baseline (0 until the first
`can_send_request`), so direct calls from `AbstractHeartbeatRequestManager
.onResponse` (Phase 8b) would set the wrong heartbeat deadline.

Changed signature to `update_heartbeat_interval_ms(current_time_ms, interval_ms)`.
Updated both tests to call directly without a prior `can_send_request`
refresh — matching Java line-for-line.

### #3 (Diagnostic) — `Display` printed unclamped `remainingMs`
Java's `Timer.remainingMs` clamps at 0; mirror with `.max(0)`.

### #4 (Coverage gap) — No `should_client_throttle` test
Added `test_should_client_throttle_is_false_for_every_version`
iterating `v0..=v4`; locks in the Java-matching default and would
have caught Finding #1.

## Deferred

### #5 (Minor) — `Builder` doesn't expose `enableUnstableLastVersion`
Java has `Builder(data)` + `Builder(data, boolean)`. Rust only the
former. Harmless for stable v0/v1; revisit when a future test needs
the unstable variant.

## Per-deviation verdict (Critic confirmed)

- Actor's deviation 1 (10 `MemberState` variants, not 8): CORRECT —
  Java's enum has exactly 10. All `previous_valid_states` arrays match
  Java's static initializer line-by-line.
- Actor's deviation 2 (skip `Heartbeat.java`): CORRECT — `Heartbeat`
  is only constructed by classic-protocol `AbstractCoordinator`
  (verified via grep). Skipping is consistent with §20.
- Actor's deviation 3 (composition over inheritance for
  `HeartbeatRequestState`): CORRECT design pattern. The collateral
  Finding #2 was a missing `Time` parameter, not a problem with
  composition itself.

## State for Phase 8b

Foundation is now solid. Phase 8b builds:

1. `AbstractMembershipManager` (1497 LOC) — §31 bidirectional
   `oneshot::Sender` / `Receiver` handshake. THE critical correctness
   contract.
2. `ConsumerMembershipManager` (525 LOC).
3. `AbstractHeartbeatRequestManager` (524 LOC). Will call
   `HeartbeatRequestState::update_heartbeat_interval_ms(current_time_ms, ...)`
   per the corrected signature.
4. `ConsumerHeartbeatRequestManager` (346 LOC).
5. `ConsumerHeartbeatRequestManagerTest` (1218 LOC) +
   `ConsumerMembershipManagerTest` (3079 LOC) per DoD §3.
6. `RequestManagers` slot population
   (`consumer_heartbeat`, `consumer_membership`).

## Gates after fix (worktree HEAD: `cbc0809`)

- `cargo build` clean
- `cargo test --lib heartbeat`: 15 passed
- `cargo test --lib consumer_group_heartbeat`: 10 passed
- `cargo xtask format-check` clean
- `cargo xtask lint` clean
