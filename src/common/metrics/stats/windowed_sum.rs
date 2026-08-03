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

//! A sampled sum maintained over the sample windows
//! (`org.apache.kafka.common.metrics.stats.WindowedSum`).

use crate::common::metrics::stats::sampled_stat::{Sample, SampledStat, SampledStatKind};
use crate::common::metrics::{Measurable, MetricConfig, Stat};

/// A [`SampledStat`] that maintains the sum of what it has seen. This is a
/// sampled version of [`crate::common::metrics::stats::CumulativeSum`].
///
/// See also [`crate::common::metrics::stats::WindowedCount`] if you want to
/// increment the value by 1 on each recording.
pub struct WindowedSum {
    inner: SampledStat,
}

/// The per-subclass behaviour shared by `WindowedSum` (and used as the base
/// behaviour for `WindowedCount`, which records `1.0`).
pub(crate) struct WindowedSumKind;

impl SampledStatKind for WindowedSumKind {
    fn update(&self, sample: &mut Sample, _config: &MetricConfig, value: f64, _now: i64) {
        sample.value += value;
    }

    fn combine(&self, samples: &[Sample], _config: &MetricConfig, _now: i64) -> f64 {
        let mut total = 0.0;
        for sample in samples {
            total += sample.value;
        }
        total
    }

    fn is_windowed_sum(&self) -> bool {
        true
    }
}

impl WindowedSum {
    /// Create a `WindowedSum`.
    pub fn new() -> Self {
        Self { inner: SampledStat::new(0.0, Box::new(WindowedSumKind)) }
    }

    /// Consume this `WindowedSum`, yielding its backing [`SampledStat`].
    ///
    /// Used by [`crate::common::metrics::stats::Rate`] /
    /// [`crate::common::metrics::stats::Meter`], which hold the `SampledStat`
    /// directly (Java `Rate.stat` is a `SampledStat`).
    pub(crate) fn into_sampled_stat(self) -> SampledStat {
        self.inner
    }
}

impl Default for WindowedSum {
    fn default() -> Self {
        Self::new()
    }
}

impl Stat for WindowedSum {
    fn record(&self, config: &MetricConfig, value: f64, time_ms: i64) {
        self.inner.record(config, value, time_ms);
    }
}

impl Measurable for WindowedSum {
    fn measure(&self, config: &MetricConfig, now: i64) -> f64 {
        self.inner.measure(config, now)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sums_over_window() {
        let config = MetricConfig::new();
        let s = WindowedSum::new();
        s.record(&config, 1.0, 0);
        s.record(&config, 2.0, 0);
        s.record(&config, 3.0, 0);
        assert_eq!(6.0, s.measure(&config, 0));
    }
}
