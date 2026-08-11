# Critic 44 — Phase M4 review (commit 715618f) — RESOLVED

## Issue 1: `poll_on_close` leave-heartbeat does not record `record_heartbeat_sent_ms` — RESOLVED
- **File**: `src/consumer/internals/consumer_heartbeat_request_manager.rs:1006-1018` (`poll_on_close`)
- **Severity**: Behavior Mismatch (minor — metric divergence on close path)
- **Java Reference**: `AbstractHeartbeatRequestManager.java:231-238` (`pollOnClose`) → line 233 `makeHeartbeatRequest(currentTimeMs, true)` → line 285 `metricsManager.recordHeartbeatSentMs(currentTimeMs)`
- **Description**: Java has exactly three heartbeat send sites, all of which route through
  `makeHeartbeatRequest(currentTimeMs, ignoreResponse)` and therefore record `recordHeartbeatSentMs`:
  1. poll-timer-expired leave (`:179`, ignore=true)
  2. normal heartbeat (`:198`, ignore=false)
  3. `pollOnClose` leave heartbeat (`:233`, ignore=true)

  The Rust translation wired sites 1 and 2 but `poll_on_close` built the leave heartbeat via
  `build_heartbeat_request(true)` and never called `record_heartbeat_sent_ms(current_time_ms)`, so
  `last-heartbeat-seconds-ago` read stale on the close-path leave heartbeat.

### Resolution
- `poll_on_close`'s parameter was renamed from `_current_time_ms` to `current_time_ms` (it was unused)
  and `record_heartbeat_sent_ms(current_time_ms)` is now called in the `is_leaving_group()` branch,
  immediately after `build_heartbeat_request(true)`, mirroring the poll-timer leave path. A Java-parity
  comment cites `AbstractHeartbeatRequestManager.java:233`/`:285`.
- The pre-M4 divergences noted by the Critic (`onHeartbeatRequestGenerated()` / `resetTimer()` omitted
  in `poll_on_close`) are out of scope for this metrics phase and were intentionally left as-is.
- **Test added**: `poll_on_close_records_heartbeat_sent_ms` wires a real `HeartbeatMetricsManager`, forces
  the member to LEAVING, calls `poll_on_close(12_345)`, and asserts the recorded send timestamp moves from
  the `-1` sentinel to `12_345`. A `#[cfg(test)]` accessor `last_heartbeat_ms_for_test()` was added to
  `HeartbeatMetricsManager` to read the raw timestamp the gauge closes over (the gauge's value derivation
  is already proven by `test_heartbeat_metrics`).
- **Verification**: `cargo build`, `cargo test --lib` (2086 passed), `cargo xtask lint`, `cargo xtask
  format-check` all green.
