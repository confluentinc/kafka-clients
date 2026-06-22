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

    /// The current time in nanoseconds.
    fn nanoseconds(&self) -> i64;
}

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

    fn nanoseconds(&self) -> i64 {
        use std::time::{SystemTime as StdSystemTime, UNIX_EPOCH};
        StdSystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos() as i64)
            .unwrap_or(0)
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
