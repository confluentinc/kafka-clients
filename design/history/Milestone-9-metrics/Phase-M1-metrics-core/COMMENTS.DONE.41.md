# Critic 41 — Phase M1 (metrics core foundation) — RESOLVED

## Issue 1: `SensorTest.testExpiredSensor` add-on-expiry path is untranslated and untested — RESOLVED
- **Severity**: Missing Requirement (test coverage)
- **File**: `src/common/metrics/sensor.rs` (`add_with_config`, `has_expired()`).
- **Java Reference**: `kafka/clients/src/test/java/org/apache/kafka/common/metrics/SensorTest.java:126-147`
  (`testExpiredSensor`).
- **Description**: `Sensor::add_with_config` has the branch
  `if self.has_expired() { return Ok(false); }`, faithfully mirroring Java
  `Sensor.add(...)` returning `false` for an expired sensor (`Sensor.java:329-330`).
  The `add`-returns-`Ok(false)`-on-expiry contract was the behavioral core of
  `testExpiredSensor` but had no M1 test exercising it.

### Resolution
Added `test_expired_sensor` to `src/common/metrics/sensor.rs` `mod tests`,
translating `SensorTest.testExpiredSensor`.

- Substitutes the M1 stats `CumulativeCount` / `Value` for Java's `Avg` / `Meter`
  (the assertion is on the boolean return of `add`, identical for any
  `MeasurableStat` — same substitution principle already applied to
  `testIdempotentAdd` / `testSimpleStats`).
- Uses a standalone sensor (registry `None`) driven by `MockTime` with a 60s
  inactive-expiry window — exercises the identical `has_expired()` /
  `add`-returns-`false` path without needing the registry.

Assertions:
- Before expiry: `add` and `add_with_config` return `Ok(true)`; metric count == 2;
  `!has_expired()`.
- After `MockTime::sleep((60 + 1) * 1000)`: `has_expired()` is true.
- A subsequent `add(...)` and `add_with_config(...)` each return `Ok(false)`, and
  the metric count is unchanged (== 2) — the stat/metric is NOT added to an
  expired sensor.
- `record(1.0)` on the expired sensor does not gate on expiry (Java parity):
  recording resets `last_record_time`, so the sensor is no longer expired
  afterwards, and the metric count remains unchanged.

The `Ok(false)` arm of `add_with_config` is now covered; a regression that
dropped the `has_expired()` guard would be caught.

Verification: `cargo build`, `cargo test --lib` (sensor tests, incl. the new one,
all green), `cargo xtask lint` (clean), `cargo xtask format-check` (clean).
