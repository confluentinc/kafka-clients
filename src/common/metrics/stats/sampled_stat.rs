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

//! A statistic recorded over one or more sampled windows.
//!
//! Translated from `org.apache.kafka.common.metrics.stats.SampledStat`.

use crate::common::metrics::metric_config::DEFAULT_NUM_SAMPLES;
use crate::common::metrics::{MeasurableStat, MetricConfig, TimeUnit};

/// A single sample within a [`SampledStat`].
///
/// Fields are public because the concrete statistics update them directly, as
/// the Java subclasses do.
#[derive(Clone, Debug)]
pub struct Sample {
    /// The value a reset returns this sample to.
    pub initial_value: f64,
    /// The number of events recorded into this sample.
    pub event_count: i64,
    /// When this sample started, in POSIX milliseconds.
    pub start_time_ms: i64,
    /// When the last event was recorded, in POSIX milliseconds.
    pub last_event_ms: i64,
    /// The accumulated value.
    pub value: f64,
    /// An explicit per-sample window; `-1` means the config window applies.
    pub time_window_ms: i64,
}

impl Sample {
    /// Creates a sample with no explicit window.
    pub fn new(initial_value: f64, now: i64) -> Self {
        Self {
            initial_value,
            event_count: 0,
            start_time_ms: now,
            last_event_ms: now,
            value: initial_value,
            time_window_ms: -1,
        }
    }

    /// Creates a sample with an explicit window.
    pub fn with_window(initial_value: f64, now: i64, time_window_ms: i64) -> Self {
        Self {
            initial_value,
            event_count: 0,
            start_time_ms: now,
            last_event_ms: now,
            value: initial_value,
            time_window_ms,
        }
    }

    /// Resets this sample to its initial state, starting at `now`.
    pub fn reset(&mut self, now: i64) {
        self.event_count = 0;
        self.start_time_ms = now;
        self.last_event_ms = now;
        self.value = self.initial_value;
    }

    /// Whether this sample's window is complete at `time_ms`.
    pub fn is_complete(&self, time_ms: i64, config: &MetricConfig) -> bool {
        let window_ms = if self.time_window_ms > 0 {
            self.time_window_ms
        } else {
            config.time_window_ms()
        };
        time_ms - self.start_time_ms >= window_ms || self.event_count >= config.event_window()
    }
}

/// The shared state of a [`SampledStat`]: the ring of samples plus the window
/// configuration.
#[derive(Clone, Debug)]
pub struct SampledStatBase {
    initial_value: f64,
    current: usize,
    time_window_ms: i64,
    samples: Vec<Sample>,
}

impl SampledStatBase {
    /// Creates the base state for a sampled statistic.
    pub fn new(initial_value: f64) -> Self {
        Self {
            initial_value,
            current: 0,
            time_window_ms: -1,
            // Keep one extra placeholder for the "overlapping sample" logic.
            samples: Vec::with_capacity((DEFAULT_NUM_SAMPLES + 1) as usize),
        }
    }

    /// The samples recorded so far.
    pub fn samples(&self) -> &[Sample] {
        &self.samples
    }

    /// The samples recorded so far, mutably, for concrete statistics to update
    /// in place.
    pub fn samples_mut(&mut self) -> &mut [Sample] {
        &mut self.samples
    }
}

/// A statistic recorded over one or more windows. Each sample covers a
/// configurable window (by event count or elapsed time); the samples are
/// combined to produce the measurement.
///
/// Concrete statistics provide [`update`](SampledStat::update) and
/// [`combine`](SampledStat::combine); the recording and windowing machinery is
/// shared here.
pub trait SampledStat: MeasurableStat {
    /// The shared sample state.
    fn sampled_base(&self) -> &SampledStatBase;

    /// The shared sample state, mutably.
    fn sampled_base_mut(&mut self) -> &mut SampledStatBase;

    /// Applies `value` to the sample at `sample_index`.
    fn update(&mut self, sample_index: usize, config: &MetricConfig, value: f64, time_ms: i64);

    /// Combines all samples into a single measurement.
    fn combine(&self, config: &MetricConfig, now: i64) -> f64;

    /// Creates a fresh sample. Overridden by statistics that need richer
    /// per-sample state (such as histograms).
    fn new_sample(&self, now: i64) -> Sample {
        let base = self.sampled_base();
        if base.time_window_ms > 0 {
            Sample::with_window(base.initial_value, now, base.time_window_ms)
        } else {
            Sample::new(base.initial_value, now)
        }
    }

    /// Records a value, advancing to a new sample if the current one is
    /// complete. Backs the [`Stat::record`](crate::common::metrics::Stat::record)
    /// implementation of sampled statistics.
    fn sampled_record(&mut self, config: &MetricConfig, value: f64, time_ms: i64) {
        let mut index = self.current_sample(time_ms);
        if self.sampled_base().samples[index].is_complete(time_ms, config) {
            index = self.advance(config, time_ms);
        }
        self.update(index, config, value, time_ms);
        let sample = &mut self.sampled_base_mut().samples[index];
        sample.event_count += 1;
        sample.last_event_ms = time_ms;
    }

    /// Purges obsolete samples and combines the rest. Backs the
    /// [`Measurable::measure`](crate::common::metrics::Measurable::measure)
    /// implementation of sampled statistics.
    fn sampled_measure(&mut self, config: &MetricConfig, now: i64) -> f64 {
        self.purge_obsolete_samples(config, now);
        self.combine(config, now)
    }

    /// Advances to the next sample, recycling or creating one, and returns its
    /// index.
    fn advance(&mut self, config: &MetricConfig, time_ms: i64) -> usize {
        // Keep one extra placeholder for the "overlapping sample" logic.
        let max_samples = (config.samples() + 1) as usize;
        let current = (self.sampled_base().current + 1) % max_samples;
        self.sampled_base_mut().current = current;
        if current >= self.sampled_base().samples.len() {
            let sample = self.new_sample(time_ms);
            self.sampled_base_mut().samples.push(sample);
        } else {
            self.sampled_base_mut().samples[current].reset(time_ms);
        }
        current
    }

    /// Returns the index of the current sample, creating one if none exist.
    fn current_sample(&mut self, time_ms: i64) -> usize {
        if self.sampled_base().samples.is_empty() {
            let sample = self.new_sample(time_ms);
            self.sampled_base_mut().samples.push(sample);
        }
        self.sampled_base().current
    }

    /// Returns the oldest sample, creating one if none exist.
    fn oldest(&mut self, now: i64) -> &Sample {
        if self.sampled_base().samples.is_empty() {
            let sample = self.new_sample(now);
            self.sampled_base_mut().samples.push(sample);
        }
        let samples = &self.sampled_base().samples;
        let mut oldest = &samples[0];
        for sample in &samples[1..] {
            if sample.start_time_ms < oldest.start_time_ms {
                oldest = sample;
            }
        }
        oldest
    }

    /// Resets any samples that lack observed events within the monitored
    /// window.
    fn purge_obsolete_samples(&mut self, config: &MetricConfig, now: i64) {
        let window_ms = {
            let base = self.sampled_base();
            if base.time_window_ms > 0 {
                base.time_window_ms
            } else {
                config.time_window_ms()
            }
        };
        let expire_age = config.samples() as i64 * window_ms;
        for sample in &mut self.sampled_base_mut().samples {
            // Samples overlapping the monitored window are kept, even if they
            // started before it.
            if now - sample.last_event_ms >= expire_age {
                sample.reset(now);
            }
        }
    }

    /// Configures an explicit time window for this statistic.
    fn with_time_window(&mut self, window: i64, unit: TimeUnit) {
        self.sampled_base_mut().time_window_ms = unit.to_millis(window);
    }
}

/// Generates the [`Stat`](crate::common::metrics::Stat),
/// [`Measurable`](crate::common::metrics::Measurable), and
/// [`MeasurableStat`](crate::common::metrics::MeasurableStat) implementations
/// for a [`SampledStat`], delegating to the shared recording/measuring logic.
macro_rules! impl_sampled_stat_traits {
    ($stat:ty) => {
        impl $crate::common::metrics::Stat for $stat {
            fn record(&mut self, config: &$crate::common::metrics::MetricConfig, value: f64, time_ms: i64) {
                $crate::common::metrics::stats::sampled_stat::SampledStat::sampled_record(self, config, value, time_ms)
            }
        }

        impl $crate::common::metrics::Measurable for $stat {
            fn measure(&mut self, config: &$crate::common::metrics::MetricConfig, now: i64) -> f64 {
                $crate::common::metrics::stats::sampled_stat::SampledStat::sampled_measure(self, config, now)
            }

            fn as_any(&self) -> &dyn ::std::any::Any {
                self
            }

            fn as_any_mut(&mut self) -> &mut dyn ::std::any::Any {
                self
            }
        }

        impl $crate::common::metrics::MeasurableStat for $stat {}
    };
}

pub(crate) use impl_sampled_stat_traits;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::metrics::stats::MockTime;
    use crate::common::metrics::{Measurable, Stat, TimeUnit};

    /// A sampled statistic whose measurement is the number of active samples.
    struct SampleCount {
        base: SampledStatBase,
    }

    impl SampleCount {
        fn new() -> Self {
            Self { base: SampledStatBase::new(0.0) }
        }
    }

    impl SampledStat for SampleCount {
        fn sampled_base(&self) -> &SampledStatBase {
            &self.base
        }

        fn sampled_base_mut(&mut self) -> &mut SampledStatBase {
            &mut self.base
        }

        fn update(&mut self, sample_index: usize, _config: &MetricConfig, _value: f64, _time_ms: i64) {
            self.base.samples[sample_index].value = 1.0;
        }

        fn combine(&self, _config: &MetricConfig, _now: i64) -> f64 {
            self.base.samples().iter().map(|s| s.value).sum()
        }
    }

    impl_sampled_stat_traits!(SampleCount);

    fn config() -> MetricConfig {
        MetricConfig::new()
            .with_time_window(1, TimeUnit::Seconds)
            .with_samples(2)
            .unwrap()
    }

    // Creates a sample with events at the start and at the end, leaving the
    // clock positioned at the end.
    fn complete_sample(stat: &mut SampleCount, config: &MetricConfig, time: &MockTime) {
        stat.record(config, 1.0, time.milliseconds());
        time.sleep(config.time_window_ms() - 1);
        stat.record(config, 1.0, time.milliseconds());
        time.sleep(1);
    }

    #[test]
    fn test_sample_is_purged_if_doesnt_overlap() {
        let mut stat = SampleCount::new();
        let time = MockTime::new();
        let config = config();

        // Monitored window: 2s. Complete a sample and wait 2.5s after.
        complete_sample(&mut stat, &config, &time);
        time.sleep(2500);

        let num_samples = stat.measure(&config, time.milliseconds());
        assert_eq!(num_samples, 0.0, "Sample should be purged if doesn't overlap the window");
    }

    #[test]
    fn test_sample_is_kept_if_overlaps() {
        let mut stat = SampleCount::new();
        let time = MockTime::new();
        let config = config();

        // Monitored window: 2s. Complete a sample and wait 1.5s after.
        complete_sample(&mut stat, &config, &time);
        time.sleep(1500);

        let num_samples = stat.measure(&config, time.milliseconds());
        assert_eq!(num_samples, 1.0, "Sample should be kept if overlaps the window");
    }

    #[test]
    fn test_sample_is_kept_if_overlaps_and_extra() {
        let mut stat = SampleCount::new();
        let time = MockTime::new();
        let config = config();

        // Monitored window: 2s. Create 2 samples with gaps and measure at 2.2s
        // from the start.
        complete_sample(&mut stat, &config, &time);
        time.sleep(100);
        complete_sample(&mut stat, &config, &time);
        time.sleep(100);
        stat.record(&config, 1.0, time.milliseconds());

        let num_samples = stat.measure(&config, time.milliseconds());
        assert_eq!(num_samples, 3.0, "Sample should be kept if overlaps the window and is n+1");
    }
}
