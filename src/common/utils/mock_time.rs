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

//! Translation of `org.apache.kafka.common.utils.MockTime`.

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicI64, Ordering};

use crate::common::utils::Time;

/// A clock that callers manually advance via [`Time::sleep`] (or by directly
/// invoking [`MockTime::set_current_time_ms`]).
///
/// Per the Java contract, `nanoseconds()` and `milliseconds()` are stored
/// independently so tests can detect bugs that incorrectly assume they are
/// derived from a single source.
///
/// Counters are [`AtomicI64`] (per CLAUDE.md rule 11) so that multiple tasks
/// can read the clock without lock contention. The listener list, used only
/// in test scaffolding, lives behind a `Mutex` because `Vec<...>` is not
/// inherently shareable.
pub struct MockTime {
    auto_tick_ms: i64,
    time_ms: AtomicI64,
    high_res_time_ns: AtomicI64,
    listeners: Mutex<Vec<Listener>>,
}

/// A callback that fires whenever the [`MockTime`] clock advances.
pub type Listener = Arc<dyn Fn() + Send + Sync>;

impl Default for MockTime {
    fn default() -> Self {
        MockTime::new(0)
    }
}

impl MockTime {
    /// Construct a fresh [`MockTime`] starting at the current wall clock /
    /// monotonic clock with no auto-advance. Equivalent to Java's
    /// `new MockTime()`.
    #[must_use]
    pub fn arc() -> Arc<Self> {
        Arc::new(MockTime::default())
    }

    /// Mirrors `new MockTime(long autoTickMs)` — every call to
    /// [`Time::milliseconds`] / [`Time::nanoseconds`] advances the clock by
    /// `auto_tick_ms` first.
    pub fn new(auto_tick_ms: i64) -> Self {
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);
        let now_ns = std::time::Instant::now().elapsed().as_nanos() as i64;
        MockTime::with_initial(auto_tick_ms, now_ms, now_ns)
    }

    /// Mirrors `new MockTime(long autoTickMs, long currentTimeMs, long currentHighResTimeNs)`.
    pub fn with_initial(auto_tick_ms: i64, current_time_ms: i64, current_high_res_time_ns: i64) -> Self {
        MockTime {
            auto_tick_ms,
            time_ms: AtomicI64::new(current_time_ms),
            high_res_time_ns: AtomicI64::new(current_high_res_time_ns),
            listeners: Mutex::new(Vec::new()),
        }
    }

    /// Add a listener invoked whenever the clock advances. Mirrors
    /// `MockTime.addListener`.
    pub fn add_listener(&self, listener: Listener) {
        self.listeners.lock().unwrap().push(listener);
    }

    /// Force the clock to a specific wall-time. Refuses going backwards in
    /// time, matching the Java `setCurrentTimeMs` precondition.
    pub fn set_current_time_ms(&self, new_ms: i64) -> Result<(), String> {
        let old_ms = self.time_ms.swap(new_ms, Ordering::AcqRel);
        if old_ms > new_ms {
            // Restore the old value so we don't leave the clock partially
            // updated from the perspective of concurrent readers.
            self.time_ms.store(old_ms, Ordering::Release);
            return Err(format!(
                "Setting the time to {new_ms} while current time {old_ms} is newer; this is not allowed"
            ));
        }
        self.high_res_time_ns.store(new_ms.saturating_mul(1_000_000), Ordering::Release);
        self.tick();
        Ok(())
    }

    fn maybe_sleep(&self, ms: i64) {
        if ms != 0 {
            self.sleep_internal(ms);
        }
    }

    fn sleep_internal(&self, ms: i64) {
        self.time_ms.fetch_add(ms, Ordering::AcqRel);
        let added_ns = ms.saturating_mul(1_000_000);
        self.high_res_time_ns.fetch_add(added_ns, Ordering::AcqRel);
        self.tick();
    }

    fn tick(&self) {
        let listeners = self.listeners.lock().unwrap().clone();
        for listener in listeners {
            listener();
        }
    }
}

impl Time for MockTime {
    fn milliseconds(&self) -> i64 {
        self.maybe_sleep(self.auto_tick_ms);
        self.time_ms.load(Ordering::Acquire)
    }

    fn nanoseconds(&self) -> i64 {
        self.maybe_sleep(self.auto_tick_ms);
        self.high_res_time_ns.load(Ordering::Acquire)
    }

    fn sleep(&self, ms: i64) {
        self.sleep_internal(ms);
    }
}

#[cfg(test)]
mod tests {
    // Translation of `org.apache.kafka.common.utils.MockTimeTest`.
    //
    // The base class `TimeTest` has two tests, `testWaitObjectTimeout` and
    // `testWaitObjectConditionSatisfied`, which exercise `Time.waitObject` —
    // a Java-specific monitor-based primitive built on `Object.wait()` /
    // `notify()`. We do not translate `waitObject` (see
    // `crate::common::utils::time` rustdoc for rationale), so those tests
    // are intentionally not translated. The producer code uses
    // `tokio::sync::Notify` directly at the call sites instead.

    use super::*;

    /// Java: `MockTimeTest#testAdvanceClock`.
    #[test]
    fn advance_clock() {
        let time = MockTime::with_initial(0, 100, 200);
        assert_eq!(time.milliseconds(), 100);
        assert_eq!(time.nanoseconds(), 200);
        time.sleep(1);
        assert_eq!(time.milliseconds(), 101);
        assert_eq!(time.nanoseconds(), 1_000_200);
    }

    /// Java: `MockTimeTest#testAutoTickMs`.
    #[test]
    fn auto_tick_ms() {
        let time = MockTime::with_initial(1, 100, 200);
        assert_eq!(time.milliseconds(), 101);
        assert_eq!(time.nanoseconds(), 2_000_200);
        assert_eq!(time.milliseconds(), 103);
        assert_eq!(time.milliseconds(), 104);
    }

    #[test]
    fn set_current_time_ms_rejects_going_backwards() {
        let time = MockTime::with_initial(0, 100, 0);
        let err = time.set_current_time_ms(50).unwrap_err();
        assert!(err.contains("not allowed"), "got: {err}");
        assert_eq!(time.milliseconds(), 100);
    }

    #[test]
    fn listener_fires_on_tick() {
        use std::sync::atomic::AtomicUsize;
        let time = MockTime::with_initial(0, 0, 0);
        let counter = Arc::new(AtomicUsize::new(0));
        let counter_clone = counter.clone();
        time.add_listener(Arc::new(move || {
            counter_clone.fetch_add(1, Ordering::AcqRel);
        }));
        time.sleep(10);
        time.sleep(20);
        assert_eq!(counter.load(Ordering::Acquire), 2);
    }
}
