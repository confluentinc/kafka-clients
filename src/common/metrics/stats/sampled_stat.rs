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

//! A scalar value measured over one or more sampling windows
//! (`org.apache.kafka.common.metrics.stats.SampledStat`).

use std::sync::Mutex;

use crate::common::metrics::{Measurable, MetricConfig, Stat};

/// The per-subclass behaviour of a [`SampledStat`].
///
/// Java's `SampledStat` is abstract with `update`/`combine` provided by each
/// subclass and an `initialValue` passed to the constructor. We model those
/// abstract methods as this trait object held by the concrete `SampledStat`
/// struct, plus the `initial_value` stored on the struct. `newSample` is NOT
/// virtual in Java, so it stays on `SampledStat` itself.
pub trait SampledStatKind: Send + Sync {
    /// Update the current sample with the recorded value (Java `update`).
    fn update(&self, sample: &mut Sample, config: &MetricConfig, value: f64, time_ms: i64);

    /// Combine all samples into a single measurement (Java `combine`).
    fn combine(&self, samples: &[Sample], config: &MetricConfig, now: i64) -> f64;

    /// Whether this is a `WindowedCount` (counts invocations rather than summing
    /// values). Used by [`crate::common::metrics::stats::Meter`] to decide
    /// whether the total records `1.0`, mirroring Java's
    /// `rate.stat instanceof WindowedCount` check. Defaults to `false`; only
    /// `WindowedCount` overrides it.
    fn is_windowed_count(&self) -> bool {
        false
    }

    /// Whether this is a `WindowedSum` (or a subclass of it). `Meter` only
    /// supports a `WindowedSum`/`WindowedCount` rate stat, mirroring Java's
    /// `instanceof WindowedSum` guard. Defaults to `false`.
    fn is_windowed_sum(&self) -> bool {
        false
    }
}

/// A `SampledStat` records a single scalar value measured over one or more
/// samples. Each sample is recorded over a configurable window. The window can
/// be defined by number of events or elapsed time (or both, if both are given
/// the window is complete when *either* the event count or elapsed time
/// criterion is met).
///
/// All the samples are combined to produce the measurement. When a window is
/// complete the oldest sample is cleared and recycled to begin recording the
/// next sample.
///
/// The mutable sample ring + cursor are guarded by a single [`Mutex`] mirroring
/// Java's sensor `synchronized`. This lock is taken per-fetch / per-partition,
/// never per-record on the hot path (CLAUDE.md §11/§27).
pub struct SampledStat {
    initial_value: f64,
    kind: Box<dyn SampledStatKind>,
    inner: Mutex<SampledStatInner>,
}

struct SampledStatInner {
    current: usize,
    time_window_ms: i64,
    samples: Vec<Sample>,
}

impl SampledStat {
    /// Create a `SampledStat` with the given initial value and per-subclass
    /// behaviour.
    pub fn new(initial_value: f64, kind: Box<dyn SampledStatKind>) -> Self {
        Self {
            initial_value,
            kind,
            inner: Mutex::new(SampledStatInner {
                current: 0,
                time_window_ms: -1,
                // keep one extra placeholder for "overlapping sample" (see
                // purge_obsolete_samples() logic)
                samples: Vec::with_capacity((crate::common::metrics::DEFAULT_NUM_SAMPLES + 1) as usize),
            }),
        }
    }

    /// Whether the underlying kind is a `WindowedCount` (see
    /// [`SampledStatKind::is_windowed_count`]).
    pub(crate) fn is_windowed_count(&self) -> bool {
        self.kind.is_windowed_count()
    }

    /// Whether the underlying kind is a `WindowedSum` (see
    /// [`SampledStatKind::is_windowed_sum`]).
    pub(crate) fn is_windowed_sum(&self) -> bool {
        self.kind.is_windowed_sum()
    }

    /// Allow configuring the time window for this sampled stat (Java
    /// `withTimeWindow(long window, TimeUnit unit)`; the conversion to ms is done
    /// by the caller, mirroring `Rate`'s use of `withTimeWindow`).
    pub(crate) fn with_time_window_ms(&self, window_ms: i64) {
        self.inner.lock().expect("sampled stat mutex poisoned").time_window_ms = window_ms;
    }

    /// Purge any samples that lack observed events within the monitored window.
    ///
    /// Public so `Rate::window_size` can purge before computing the window size,
    /// matching Java's `protected` visibility used by `Rate`.
    pub(crate) fn purge_obsolete_samples(&self, config: &MetricConfig, now: i64) {
        let mut inner = self.inner.lock().expect("sampled stat mutex poisoned");
        Self::purge_obsolete_samples_locked(&mut inner, self.initial_value, config, now);
    }

    /// The start time of the oldest non-purged sample (Java `oldest(now)`),
    /// used by `Rate::window_size`.
    pub(crate) fn oldest_start_time_ms(&self, now: i64) -> i64 {
        let mut inner = self.inner.lock().expect("sampled stat mutex poisoned");
        Self::oldest_locked(&mut inner, self.initial_value, now).start_time_ms
    }

    // ---- internal helpers operating on a locked inner ----

    fn new_sample(initial_value: f64, time_window_ms: i64, time_ms: i64) -> Sample {
        if time_window_ms > 0 {
            Sample::new_time_window_ms(initial_value, time_ms, time_window_ms)
        } else {
            Sample::new(initial_value, time_ms)
        }
    }

    fn current_locked(inner: &mut SampledStatInner, initial_value: f64, time_ms: i64) -> &mut Sample {
        if inner.samples.is_empty() {
            let s = Self::new_sample(initial_value, inner.time_window_ms, time_ms);
            inner.samples.push(s);
        }
        let idx = inner.current;
        &mut inner.samples[idx]
    }

    fn oldest_locked(inner: &mut SampledStatInner, initial_value: f64, now: i64) -> &Sample {
        if inner.samples.is_empty() {
            let s = Self::new_sample(initial_value, inner.time_window_ms, now);
            inner.samples.push(s);
        }
        let mut oldest = 0usize;
        for i in 1..inner.samples.len() {
            if inner.samples[i].start_time_ms < inner.samples[oldest].start_time_ms {
                oldest = i;
            }
        }
        &inner.samples[oldest]
    }

    fn advance_locked(inner: &mut SampledStatInner, initial_value: f64, config: &MetricConfig, time_ms: i64) -> usize {
        // keep one extra placeholder for "overlapping sample" (see
        // purge_obsolete_samples() logic)
        let max_samples = (config.samples() + 1) as usize;
        inner.current = (inner.current + 1) % max_samples;
        if inner.current >= inner.samples.len() {
            let s = Self::new_sample(initial_value, inner.time_window_ms, time_ms);
            inner.samples.push(s);
        } else {
            let idx = inner.current;
            inner.samples[idx].reset(time_ms);
        }
        inner.current
    }

    fn purge_obsolete_samples_locked(
        inner: &mut SampledStatInner,
        initial_value: f64,
        config: &MetricConfig,
        now: i64,
    ) {
        let window_ms = if inner.time_window_ms > 0 {
            inner.time_window_ms
        } else {
            config.time_window_ms()
        };
        let expire_age = config.samples() as i64 * window_ms;
        for sample in inner.samples.iter_mut() {
            // samples overlapping the monitored window are kept, even if they
            // started before it
            if now - sample.last_event_ms >= expire_age {
                sample.reset_to(initial_value, now);
            }
        }
    }
}

impl Stat for SampledStat {
    fn record(&self, config: &MetricConfig, value: f64, time_ms: i64) {
        let mut inner = self.inner.lock().expect("sampled stat mutex poisoned");
        let initial_value = self.initial_value;
        // Select the current sample, advancing to a fresh one if complete.
        let is_complete = {
            let sample = Self::current_locked(&mut inner, initial_value, time_ms);
            sample.is_complete(time_ms, config)
        };
        let idx = if is_complete {
            Self::advance_locked(&mut inner, initial_value, config, time_ms)
        } else {
            inner.current
        };
        let sample = &mut inner.samples[idx];
        self.kind.update(sample, config, value, time_ms);
        sample.event_count += 1;
        sample.last_event_ms = time_ms;
    }
}

impl Measurable for SampledStat {
    fn measure(&self, config: &MetricConfig, now: i64) -> f64 {
        let mut inner = self.inner.lock().expect("sampled stat mutex poisoned");
        Self::purge_obsolete_samples_locked(&mut inner, self.initial_value, config, now);
        self.kind.combine(&inner.samples, config, now)
    }
}

/// A single sample within a [`SampledStat`].
///
/// Mirrors `SampledStat.Sample`. Fields are public to the crate so subclass
/// `update`/`combine` implementations can read/write them exactly as Java does.
#[derive(Clone, Debug)]
pub struct Sample {
    /// The initial (reset) value of this sample.
    pub initial_value: f64,
    /// The number of events recorded into this sample.
    pub event_count: i64,
    /// The time the sample started.
    pub start_time_ms: i64,
    /// The time of the last event recorded into the sample.
    pub last_event_ms: i64,
    /// The accumulated value of the sample.
    pub value: f64,
    /// The per-sample time window (or `-1` to use the config's window).
    pub time_window_ms: i64,
}

impl Sample {
    /// Create a sample with no explicit time window.
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

    /// Create a sample with an explicit time window.
    pub fn new_time_window_ms(initial_value: f64, now: i64, time_window_ms: i64) -> Self {
        Self {
            initial_value,
            event_count: 0,
            start_time_ms: now,
            last_event_ms: now,
            value: initial_value,
            time_window_ms,
        }
    }

    /// Reset the sample for reuse at `now`, keeping its configured initial value.
    pub fn reset(&mut self, now: i64) {
        self.event_count = 0;
        self.start_time_ms = now;
        self.last_event_ms = now;
        self.value = self.initial_value;
    }

    /// Reset the sample to a given initial value at `now`.
    ///
    /// Java's `reset(now)` resets to the sample's stored `initialValue`; the
    /// `SampledStat`'s own `initialValue` always equals it, so passing it
    /// explicitly here keeps the two in lock-step.
    fn reset_to(&mut self, initial_value: f64, now: i64) {
        self.initial_value = initial_value;
        self.reset(now);
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::metrics::internals::metrics_utils::TimeUnit;
    use crate::common::metrics::stats::{Avg, Max, Min, WindowedCount, WindowedSum};
    use crate::common::metrics::time::mock::MockTime;
    use crate::common::metrics::{Measurable, Stat, Time};
    use std::sync::Arc;

    // A SampledStat whose measure() returns the number of samples (sum of
    // per-sample value where each sample's value is set to 1). Mirrors
    // SampledStatTest.SampleCount.
    struct SampleCountKind;
    impl SampledStatKind for SampleCountKind {
        fn update(&self, sample: &mut Sample, _config: &MetricConfig, _value: f64, _time_ms: i64) {
            sample.value = 1.0;
        }
        fn combine(&self, samples: &[Sample], _config: &MetricConfig, _now: i64) -> f64 {
            samples.iter().map(|s| s.value).sum()
        }
    }
    fn sample_count() -> SampledStat {
        SampledStat::new(0.0, Box::new(SampleCountKind))
    }

    // Creates a sample with events at the start and at the end. Positions clock
    // at the end. Mirrors SampledStatTest.completeSample.
    fn complete_sample(stat: &SampledStat, config: &MetricConfig, time: &MockTime) {
        stat.record(config, 1.0, time.milliseconds());
        time.sleep(config.time_window_ms() - 1);
        stat.record(config, 1.0, time.milliseconds());
        time.sleep(1);
    }

    // SampledStatTest.testSampleIsPurgedIfDoesntOverlap
    #[test]
    fn test_sample_is_purged_if_doesnt_overlap() {
        let config = MetricConfig::new().set_time_window(1, TimeUnit::Seconds).set_samples(2);
        let stat = sample_count();
        let time = MockTime::new();

        complete_sample(&stat, &config, &time);
        time.sleep(2500);

        let num_samples = stat.measure(&config, time.milliseconds());
        assert_eq!(0.0, num_samples, "Sample should be purged if doesn't overlap the window");
    }

    // SampledStatTest.testSampleIsKeptIfOverlaps
    #[test]
    fn test_sample_is_kept_if_overlaps() {
        let config = MetricConfig::new().set_time_window(1, TimeUnit::Seconds).set_samples(2);
        let stat = sample_count();
        let time = MockTime::new();

        complete_sample(&stat, &config, &time);
        time.sleep(1500);

        let num_samples = stat.measure(&config, time.milliseconds());
        assert_eq!(1.0, num_samples, "Sample should be kept if overlaps the window");
    }

    // SampledStatTest.testSampleIsKeptIfOverlapsAndExtra
    #[test]
    fn test_sample_is_kept_if_overlaps_and_extra() {
        let config = MetricConfig::new().set_time_window(1, TimeUnit::Seconds).set_samples(2);
        let stat = sample_count();
        let time = MockTime::new();

        complete_sample(&stat, &config, &time);
        time.sleep(100);
        complete_sample(&stat, &config, &time);
        time.sleep(100);
        stat.record(&config, 1.0, time.milliseconds());

        let num_samples = stat.measure(&config, time.milliseconds());
        assert_eq!(3.0, num_samples, "Sample should be kept if overlaps the window and is n+1");
    }

    // MetricsTest.testSampledStatReturnsNaNWhenNoValuesExist
    #[test]
    fn test_sampled_stat_returns_nan_when_no_values_exist() {
        let max = Max::new();
        let min = Min::new();
        let avg = Avg::new();
        let window_ms = 100i64;
        let samples = 2;
        let config = MetricConfig::new()
            .set_time_window(window_ms, TimeUnit::Milliseconds)
            .set_samples(samples);
        let time = MockTime::new();

        max.record(&config, 50.0, time.milliseconds());
        min.record(&config, 50.0, time.milliseconds());
        avg.record(&config, 50.0, time.milliseconds());

        time.sleep(samples as i64 * window_ms);

        assert!(max.measure(&config, time.milliseconds()).is_nan());
        assert!(min.measure(&config, time.milliseconds()).is_nan());
        assert!(avg.measure(&config, time.milliseconds()).is_nan());
    }

    // MetricsTest.testSampledStatReturnsInitialValueWhenNoValuesExist
    #[test]
    fn test_sampled_stat_returns_initial_value_when_no_values_exist() {
        let count = WindowedCount::new();
        let sampled_total = WindowedSum::new();
        let window_ms = 100i64;
        let samples = 2;
        let config = MetricConfig::new()
            .set_time_window(window_ms, TimeUnit::Milliseconds)
            .set_samples(samples);
        let time = MockTime::new();

        count.record(&config, 50.0, time.milliseconds());
        sampled_total.record(&config, 50.0, time.milliseconds());

        time.sleep(samples as i64 * window_ms);

        assert_eq!(0.0, count.measure(&config, time.milliseconds()));
        assert_eq!(0.0, sampled_total.measure(&config, time.milliseconds()));
    }

    // Sanity: the Arc<dyn Measurable> view shares state with the recording stat.
    #[test]
    fn shared_arc_reflects_records() {
        let config = MetricConfig::new();
        let stat = Arc::new(WindowedSum::new());
        let view: Arc<dyn Measurable> = Arc::clone(&stat) as Arc<dyn Measurable>;
        stat.record(&config, 3.0, 0);
        assert_eq!(view.measure(&config, 0), 3.0);
    }
}
