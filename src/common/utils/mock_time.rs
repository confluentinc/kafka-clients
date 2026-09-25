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

//! A clock that only moves when told to, translated from
//! `org.apache.kafka.common.utils.MockTime` (clients test sources).

use super::{SystemTime, Time};
use crate::common::Error;
use std::sync::atomic::{AtomicI64, Ordering};

/// A clock that you can manually advance by calling [`sleep`](MockTime::sleep).
///
/// Java's `Listener` / `addListener` and `waitObject` are not translated: they
/// wake threads blocked on a Java monitor, and no Rust caller blocks on one.
#[doc(alias = "org.apache.kafka.common.utils.MockTime")]
#[derive(Debug)]
pub(crate) struct MockTime {
    auto_tick_ms: i64,

    // Values from `nanoTime` and `currentTimeMillis` are not comparable, so we
    // store them separately to allow tests using this class to detect bugs
    // where this is incorrectly assumed to be true.
    time_ms: AtomicI64,
    high_res_time_ns: AtomicI64,
}

impl MockTime {
    #[doc(alias = "org.apache.kafka.common.utils.MockTime#MockTime")]
    pub(crate) fn new() -> Self {
        Self::with_auto_tick_ms(0)
    }

    /// Seeds the clock from the system clock, as Java seeds it from
    /// `System.currentTimeMillis()` and `System.nanoTime()`.
    #[doc(alias = "org.apache.kafka.common.utils.MockTime#MockTime")]
    pub(crate) fn with_auto_tick_ms(auto_tick_ms: i64) -> Self {
        let system = SystemTime;
        Self::with_auto_tick_ms_current_time_ms_current_high_res_time_ns(
            auto_tick_ms,
            system.milliseconds(),
            system.nanoseconds(),
        )
    }

    #[doc(alias = "org.apache.kafka.common.utils.MockTime#MockTime")]
    pub(crate) fn with_auto_tick_ms_current_time_ms_current_high_res_time_ns(
        auto_tick_ms: i64,
        current_time_ms: i64,
        current_high_res_time_ns: i64,
    ) -> Self {
        Self {
            auto_tick_ms,
            time_ms: AtomicI64::new(current_time_ms),
            high_res_time_ns: AtomicI64::new(current_high_res_time_ns),
        }
    }

    fn maybe_sleep(&self, ms: i64) {
        if ms != 0 {
            self.sleep(ms);
        }
    }

    /// Advances both clocks by `ms` milliseconds.
    #[doc(alias = "org.apache.kafka.common.utils.MockTime#sleep")]
    pub(crate) fn sleep(&self, ms: i64) {
        self.time_ms.fetch_add(ms, Ordering::SeqCst);
        self.high_res_time_ns.fetch_add(ms.saturating_mul(1_000_000), Ordering::SeqCst);
    }

    /// Sets the wall clock to `new_ms`, and the monotonic clock to the same
    /// instant in nanoseconds.
    ///
    /// Returns a `LocalIllegalArgument` error, leaving the clock unchanged, when
    /// `new_ms` is older than the current time. Java stores `new_ms` before it
    /// throws; the Rust clock rejects the value without storing it, so a test
    /// that recovers from the error still sees the time it had.
    #[doc(alias = "org.apache.kafka.common.utils.MockTime#setCurrentTimeMs")]
    pub(crate) fn set_current_time_ms(&self, new_ms: i64) -> Result<(), Error> {
        let old_ms = self.time_ms.load(Ordering::SeqCst);

        // does not allow to set to an older timestamp
        if old_ms > new_ms {
            return Err(Error::local_illegal_argument(format!(
                "Setting the time to {new_ms} while current time {old_ms} is newer; this is not allowed"
            )));
        }

        self.time_ms.store(new_ms, Ordering::SeqCst);
        self.high_res_time_ns.store(new_ms.saturating_mul(1_000_000), Ordering::SeqCst);
        Ok(())
    }
}

impl Default for MockTime {
    fn default() -> Self {
        Self::new()
    }
}

impl Time for MockTime {
    #[doc(alias = "org.apache.kafka.common.utils.MockTime#milliseconds")]
    fn milliseconds(&self) -> i64 {
        self.maybe_sleep(self.auto_tick_ms);
        self.time_ms.load(Ordering::SeqCst)
    }

    #[doc(alias = "org.apache.kafka.common.utils.MockTime#nanoseconds")]
    fn nanoseconds(&self) -> i64 {
        self.maybe_sleep(self.auto_tick_ms);
        self.high_res_time_ns.load(Ordering::SeqCst)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sleep_advances_both_clocks_independently() {
        let time = MockTime::with_auto_tick_ms_current_time_ms_current_high_res_time_ns(0, 1_000, 7);
        time.sleep(5);
        assert_eq!(time.milliseconds(), 1_005);
        // The monotonic clock keeps its own origin; it is not `milliseconds * 1e6`.
        assert_eq!(time.nanoseconds(), 5_000_007);
    }

    #[test]
    fn test_auto_tick_advances_on_every_read() {
        let time = MockTime::with_auto_tick_ms_current_time_ms_current_high_res_time_ns(2, 0, 0);
        assert_eq!(time.milliseconds(), 2);
        assert_eq!(time.milliseconds(), 4);
        assert_eq!(time.nanoseconds(), 6_000_000);
    }

    #[test]
    fn test_set_current_time_ms() {
        let time = MockTime::with_auto_tick_ms_current_time_ms_current_high_res_time_ns(0, 100, 0);
        time.set_current_time_ms(250).unwrap();
        assert_eq!(time.milliseconds(), 250);
        assert_eq!(time.nanoseconds(), 250_000_000);
    }

    #[test]
    fn test_set_current_time_ms_rejects_older_timestamp() {
        let time = MockTime::with_auto_tick_ms_current_time_ms_current_high_res_time_ns(0, 100, 0);
        let err = time.set_current_time_ms(99).unwrap_err();
        assert!(matches!(err, Error::LocalIllegalArgument(_)), "{err:?}");
        assert!(
            err.to_string()
                .contains("Setting the time to 99 while current time 100 is newer; this is not allowed"),
            "{err}"
        );
        assert_eq!(time.milliseconds(), 100);
    }
}
