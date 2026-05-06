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

//! Translation of `org.apache.kafka.common.utils.SystemTime`.

use std::sync::Arc;
use std::sync::OnceLock;
use std::time::{Instant, SystemTime as StdSystemTime, UNIX_EPOCH};

use crate::common::utils::Time;

/// A [`Time`] implementation that uses the system clock and `thread::sleep`.
/// Use [`SystemTime::new`] to retrieve the canonical singleton.
///
/// Internally we store the process-start [`Instant`] so that
/// [`Time::nanoseconds`] returns a monotonic, ever-increasing 64-bit value
/// matching `System.nanoTime()`'s contract.
pub struct SystemTime {
    start: Instant,
}

static SINGLETON: OnceLock<Arc<SystemTime>> = OnceLock::new();

impl SystemTime {
    /// Returns the canonical shared instance, equivalent to Java's
    /// `Time.SYSTEM` / `SystemTime.getSystemTime()`.
    pub fn instance() -> Arc<dyn Time> {
        SINGLETON.get_or_init(|| Arc::new(SystemTime { start: Instant::now() })).clone()
    }
}

impl Time for SystemTime {
    fn milliseconds(&self) -> i64 {
        // System.currentTimeMillis() — wall clock since UNIX epoch.
        match StdSystemTime::now().duration_since(UNIX_EPOCH) {
            Ok(d) => d.as_millis() as i64,
            // Pre-epoch is impossible on supported platforms but be explicit.
            Err(e) => -(e.duration().as_millis() as i64),
        }
    }

    fn nanoseconds(&self) -> i64 {
        // Match System.nanoTime(): monotonic since some fixed origin.
        self.start.elapsed().as_nanos() as i64
    }

    fn sleep(&self, ms: i64) {
        if ms > 0 {
            std::thread::sleep(std::time::Duration::from_millis(ms as u64));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn system_time_singleton_returns_same_instance() {
        let a = SystemTime::instance();
        let b = SystemTime::instance();
        assert!(Arc::ptr_eq(&a, &b));
    }

    #[test]
    fn nanoseconds_is_monotonic() {
        let t = SystemTime::instance();
        let n1 = t.nanoseconds();
        let n2 = t.nanoseconds();
        assert!(n2 >= n1, "nanoseconds went backwards: {n1} -> {n2}");
    }

    #[test]
    fn milliseconds_is_positive() {
        let t = SystemTime::instance();
        assert!(t.milliseconds() > 0);
    }
}
