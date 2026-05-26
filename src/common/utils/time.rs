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

//! Translation of `org.apache.kafka.common.utils.Time` (interface).

use std::sync::Arc;

use crate::common::utils::Timer;

/// An interface abstracting the clock used in code that observes wall- or
/// monotonic-time. Implementations must be thread-safe — every implementation
/// in this crate is built on atomics or the system clock and can be shared
/// across `tokio` tasks.
///
/// The Java default method `Time.timer(long)` becomes a free constructor on
/// [`Timer`] (`Timer::new(time, timeout_ms)`) so that `Time` stays
/// object-safe (`Arc<dyn Time>` is the canonical handle).
///
/// **Skipped translations** vs. the Java interface:
/// * `waitObject(Object, Supplier<Boolean>, long)` — relies on
///   `Object.wait()` / `Object.notify()` for cross-thread coordination. Rust
///   uses `tokio::sync::Notify` or `std::sync::Condvar` directly at call
///   sites, so the `waitObject` indirection is not translated.
/// * `waitForFuture(Future<T>, long)` — Java `Future.get(timeout)` is
///   replaced by `tokio::time::timeout(...)` at the call site.
pub trait Time: Send + Sync {
    /// Returns the current wall-clock time in milliseconds.
    fn milliseconds(&self) -> i64;

    /// Returns the value of [`Time::nanoseconds`] converted into milliseconds.
    /// Mirrors the Java default `hiResClockMs` implementation.
    fn hi_res_clock_ms(&self) -> i64 {
        self.nanoseconds() / 1_000_000
    }

    /// Returns the high-resolution monotonic time source in nanoseconds.
    /// Mirrors `System.nanoTime()` semantics: only useful for measuring
    /// elapsed time, not synchronized with wall-clock time.
    fn nanoseconds(&self) -> i64;

    /// Sleep (advance, for [`MockTime`](crate::common::utils::MockTime)) by
    /// the given number of milliseconds.
    fn sleep(&self, ms: i64);
}

/// Construct the canonical [`SystemTime`](crate::common::utils::SystemTime)
/// handle. Mirrors the `Time.SYSTEM` static field in Java.
pub fn system_time() -> Arc<dyn Time> {
    crate::common::utils::SystemTime::instance()
}

/// Build a [`Timer`] bound to the given clock. Mirrors the `Time.timer(long)`
/// default method.
pub fn timer(time: Arc<dyn Time>, timeout_ms: i64) -> Timer {
    Timer::new(time, timeout_ms)
}
