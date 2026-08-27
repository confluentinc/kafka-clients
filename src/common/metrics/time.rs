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

//! A minimal time source for the metrics framework, mirroring the subset of
//! `org.apache.kafka.common.utils.Time` used by `Metrics`/`Sensor`/`KafkaMetric`.

/// An interface abstracting the clock to use in unit testing classes that make
/// use of clock time. Mirrors `org.apache.kafka.common.utils.Time`.
pub trait Time: Send + Sync + 'static {
    /// The current time in milliseconds.
    fn milliseconds(&self) -> i64;

    /// A monotonic nanosecond reading. Mirrors Java's `Time.nanoseconds()`,
    /// which is `System.nanoTime()` (`SystemTime.java:41`): monotonic, with an
    /// arbitrary origin, so only differences between readings are meaningful.
    /// Do NOT treat it as a wall-clock timestamp.
    fn nanoseconds(&self) -> i64;
}

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
pub(crate) mod mock {
    use super::Time;
    use std::sync::atomic::{AtomicI64, Ordering};

    /// A manually-advanced clock for tests, mirroring
    /// `org.apache.kafka.common.utils.MockTime`. Time only advances when
    /// [`MockTime::sleep`] is called.
    #[derive(Debug)]
    pub(crate) struct MockTime {
        time_ms: AtomicI64,
        // autoTickMs is 0 in the default Java MockTime used by the metrics tests,
        // so reads never auto-advance; we keep a fixed clock here.
        nanos: AtomicI64,
    }

    impl MockTime {
        pub(crate) fn new() -> Self {
            // Java MockTime default constructor seeds with System.currentTimeMillis()
            // / nanoTime(). The metrics tests only rely on relative advances, so any
            // fixed non-zero start works. Use a fixed base for determinism.
            let base_ms = 0;
            Self { time_ms: AtomicI64::new(base_ms), nanos: AtomicI64::new(base_ms * 1_000_000) }
        }

        /// Advance the clock by `ms` milliseconds, mirroring `MockTime.sleep`.
        pub(crate) fn sleep(&self, ms: i64) {
            self.time_ms.fetch_add(ms, Ordering::SeqCst);
            self.nanos.fetch_add(ms * 1_000_000, Ordering::SeqCst);
        }
    }

    impl Time for MockTime {
        fn milliseconds(&self) -> i64 {
            self.time_ms.load(Ordering::SeqCst)
        }

        fn nanoseconds(&self) -> i64 {
            self.nanos.load(Ordering::SeqCst)
        }
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
