// Copyright 2025 Confluent Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! A `Time` implementation backed by the system clock.
//!
//! Mirrors `org.apache.kafka.common.utils.SystemTime`.
//!
//! Java keeps `SystemTime` in `org.apache.kafka.common.utils`, alongside
//! `Time`; this crate hosts both under `common::metrics` because `Metrics` /
//! `Sensor` / `KafkaMetric` are their only consumers. The package drift is
//! pre-existing and shared with `ByteUtils` (`common::protocol::varint`).

use super::Time;

/// Process-wide origin for [`SystemTime::nanoseconds`], the analog of the
/// arbitrary origin `System.nanoTime()` counts from.
///
/// `Instant` deliberately exposes no epoch, so a fixed reference is needed to
/// turn it into an `i64`. Captured once on first use.
static NANO_ORIGIN: std::sync::LazyLock<std::time::Instant> = std::sync::LazyLock::new(std::time::Instant::now);

/// A `Time` implementation that uses the system clock and sleep call. Mirrors
/// `org.apache.kafka.common.utils.SystemTime`.
#[derive(Debug, Clone, Copy, Default)]
pub struct SystemTime;

impl Time for SystemTime {
    fn milliseconds(&self) -> i64 {
        use std::time::{SystemTime as StdSystemTime, UNIX_EPOCH};
        StdSystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0)
    }

    /// Monotonic, via `Instant` — NOT `SystemTime`.
    ///
    /// This used to read `SystemTime::now().duration_since(UNIX_EPOCH)`, which is
    /// wall-clock: an NTP step backwards makes a later reading smaller than an
    /// earlier one, so any `nanoseconds() - start` elapsed measurement goes
    /// negative. `Instant` cannot regress.
    fn nanoseconds(&self) -> i64 {
        NANO_ORIGIN.elapsed().as_nanos() as i64
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Regression: `nanoseconds()` must be monotonic.
    ///
    /// It previously read `SystemTime::now().duration_since(UNIX_EPOCH)`, which
    /// is wall-clock — an NTP step backwards makes a later reading smaller than
    /// an earlier one, so elapsed measurements computed as
    /// `nanoseconds() - start` go negative. Java uses `System.nanoTime()`
    /// (`SystemTime.java:41`), which is monotonic.
    ///
    /// This cannot inject a clock step, so it asserts the two properties that a
    /// wall-clock implementation would not satisfy by construction: readings
    /// never decrease, and the value is an elapsed count rather than a
    /// Unix-epoch timestamp.
    #[test]
    fn nanoseconds_is_monotonic_and_not_epoch_based() {
        let t = SystemTime;

        let mut previous = t.nanoseconds();
        for _ in 0..1_000 {
            let current = t.nanoseconds();
            assert!(current >= previous, "nanoseconds() went backwards: {current} < {previous}");
            previous = current;
        }

        // A Unix-epoch nanosecond reading is ~1.7e18 and rising. An elapsed
        // count from process start is many orders of magnitude smaller, so this
        // fails loudly if the implementation reverts to wall-clock.
        let epoch_nanos_2020: i64 = 1_577_836_800_000_000_000;
        assert!(
            t.nanoseconds() < epoch_nanos_2020,
            "nanoseconds() looks like a Unix-epoch timestamp, not an elapsed count"
        );

        // milliseconds() is deliberately still wall-clock, matching Java's
        // System.currentTimeMillis().
        assert!(t.milliseconds() > 1_577_836_800_000, "milliseconds() should remain wall-clock");
    }
}
