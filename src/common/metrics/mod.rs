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

//! Metrics framework (org.apache.kafka.common.metrics).

pub mod compound_stat;
pub mod gauge;
pub(crate) mod internals;
pub mod kafka_metric;
pub mod kafka_metrics_context;
pub mod measurable;
pub mod measurable_stat;
pub mod metric_config;
pub mod metric_value_provider;
// The Java `Metrics` class lives in the `metrics` package, so the file is
// `metrics/metrics.rs` per the naming convention.
#[allow(clippy::module_inception)]
pub mod metrics;
pub mod metrics_context;
pub mod metrics_reporter;
pub mod quota;
pub mod quota_violation_error;
pub mod sensor;
pub mod stat;
pub mod stats;
pub mod time_unit;

pub use compound_stat::{CompoundStat, NamedMeasurable};
pub use gauge::Gauge;
pub use kafka_metric::{KafkaMetric, TimeSource};
pub use kafka_metrics_context::KafkaMetricsContext;
pub use measurable::Measurable;
pub use measurable_stat::MeasurableStat;
pub use metric_config::MetricConfig;
pub use metric_value_provider::{MetricValue, MetricValueProvider};
pub use metrics::Metrics;
pub use metrics_context::MetricsContext;
pub use metrics_reporter::MetricsReporter;
pub use quota::Quota;
pub use quota_violation_error::QuotaViolationError;
pub use sensor::{RecordingLevel, Sensor};
pub use stat::Stat;
pub use time_unit::TimeUnit;

/// Test-only support shared across the registry, sensor, and stats-with-registry
/// tests: a manually advanced clock that also yields a [`TimeSource`], and a
/// no-op metrics reporter.
#[cfg(test)]
pub(crate) mod test_support {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicI64, Ordering};

    use crate::common::metrics::{KafkaMetric, MetricsReporter, TimeSource};

    /// A manually advanced clock reporting POSIX milliseconds.
    ///
    /// With a non-zero auto-tick, every read of the clock advances it by that
    /// amount before reporting, so time keeps moving forward under concurrent
    /// access even without an explicit [`sleep`](MockClock::sleep). With a zero
    /// auto-tick the clock only moves when `sleep` is called.
    pub(crate) struct MockClock {
        current_ms: Arc<AtomicI64>,
        auto_tick_ms: i64,
    }

    impl MockClock {
        /// A clock starting at zero that only advances on explicit `sleep`.
        pub(crate) fn new() -> Self {
            Self { current_ms: Arc::new(AtomicI64::new(0)), auto_tick_ms: 0 }
        }

        /// A clock starting at zero that advances by `auto_tick_ms` on every read.
        pub(crate) fn with_auto_tick(auto_tick_ms: i64) -> Self {
            Self { current_ms: Arc::new(AtomicI64::new(0)), auto_tick_ms }
        }

        /// A clock starting at `start_ms` that only advances on explicit `sleep`.
        /// Some stats (notably the token bucket) fill relative to an absolute
        /// zero epoch, so tests that depend on a realistic starting instant use
        /// this rather than starting from zero.
        pub(crate) fn with_start(start_ms: i64) -> Self {
            Self { current_ms: Arc::new(AtomicI64::new(start_ms)), auto_tick_ms: 0 }
        }

        /// The current time in milliseconds, applying any auto-tick.
        pub(crate) fn milliseconds(&self) -> i64 {
            Self::read(&self.current_ms, self.auto_tick_ms)
        }

        /// Advances the clock by `ms` milliseconds.
        pub(crate) fn sleep(&self, ms: i64) {
            self.current_ms.fetch_add(ms, Ordering::SeqCst);
        }

        /// A [`TimeSource`] sharing this clock's counter, so `sleep` (and
        /// auto-tick) are observed through the returned closure.
        pub(crate) fn time_source(&self) -> TimeSource {
            let current = Arc::clone(&self.current_ms);
            let auto_tick_ms = self.auto_tick_ms;
            Arc::new(move || Self::read(&current, auto_tick_ms))
        }

        fn read(current: &AtomicI64, auto_tick_ms: i64) -> i64 {
            if auto_tick_ms == 0 {
                current.load(Ordering::SeqCst)
            } else {
                current.fetch_add(auto_tick_ms, Ordering::SeqCst) + auto_tick_ms
            }
        }
    }

    /// A metrics reporter that ignores every lifecycle callback. Installing it in
    /// a registry exercises the reporter callback paths without asserting on
    /// them.
    pub(crate) struct FakeMetricsReporter;

    impl MetricsReporter for FakeMetricsReporter {
        fn init(&self, _metrics: &[Arc<KafkaMetric>]) {}
        fn metric_change(&self, _metric: Arc<KafkaMetric>) {}
        fn metric_removal(&self, _metric: Arc<KafkaMetric>) {}
        fn close(&self) {}
    }
}
