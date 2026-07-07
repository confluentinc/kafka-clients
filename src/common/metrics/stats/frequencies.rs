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

//! A compound statistic reporting a normalized distribution over buckets.
//!
//! Translated from `org.apache.kafka.common.metrics.stats.Frequencies`.

use std::any::Any;
use std::sync::{Arc, Mutex};

use crate::common::metrics::stats::histogram::{BinScheme, ConstantBinScheme, Histogram};
use crate::common::metrics::stats::sampled_stat::{SampledStat, SampledStatBase, impl_sampled_stat_traits};
use crate::common::metrics::stats::{Frequency, Sample};
use crate::common::metrics::{CompoundStat, Measurable, MeasurableStat, MetricConfig, NamedMeasurable, Stat};
use crate::common::utils::double_to_string;
use crate::common::{KafkaError, MetricName};

/// A [`CompoundStat`] representing a normalized distribution, with a
/// [`Frequency`] metric per bucketed value giving how often that value appears
/// relative to the total recorded.
#[derive(Debug)]
pub struct Frequencies {
    inner: Arc<Mutex<FrequenciesInner>>,
    frequencies: Vec<Frequency>,
}

impl Frequencies {
    /// Creates frequencies for a boolean sensor recording `0.0` for false and
    /// `1.0` for true. Either metric name may be omitted, but not both.
    pub fn for_boolean_values(
        false_metric_name: Option<MetricName>,
        true_metric_name: Option<MetricName>,
    ) -> Result<Self, KafkaError> {
        let mut frequencies = Vec::new();
        if let Some(name) = false_metric_name {
            frequencies.push(Frequency::new(name, 0.0));
        }
        if let Some(name) = true_metric_name {
            frequencies.push(Frequency::new(name, 1.0));
        }
        if frequencies.is_empty() {
            return Err(KafkaError::illegal_argument("Must specify at least one metric name"));
        }
        Self::new(2, 0.0, 1.0, frequencies)
    }

    /// Creates frequencies capturing values in `[min, max]` across `buckets`
    /// buckets centered on the min, max, and intermediate values.
    ///
    /// Returns [`KafkaError::IllegalArgument`] if the range or bucket count is
    /// invalid, or if a frequency's center falls outside the range.
    pub fn new(buckets: i32, min: f64, max: f64, frequencies: Vec<Frequency>) -> Result<Self, KafkaError> {
        if max < min {
            return Err(KafkaError::illegal_argument(format!(
                "The maximum value {} must be greater than the minimum value {}",
                double_to_string(max),
                double_to_string(min)
            )));
        }
        if buckets < 1 {
            return Err(KafkaError::illegal_argument("Must be at least 1 bucket"));
        }
        if (buckets as usize) < frequencies.len() {
            return Err(KafkaError::illegal_argument("More frequencies than buckets"));
        }
        for freq in &frequencies {
            if min > freq.center_value() || max < freq.center_value() {
                return Err(KafkaError::illegal_argument(format!(
                    "The frequency centered at '{}' is not within the range [{},{}]",
                    double_to_string(freq.center_value()),
                    double_to_string(min),
                    double_to_string(max)
                )));
            }
        }
        let half_bucket_width = (max - min) / (buckets - 1) as f64 / 2.0;
        let bin_scheme: Arc<dyn BinScheme> = Arc::new(ConstantBinScheme::new(
            buckets,
            min - half_bucket_width,
            max + half_bucket_width,
        )?);
        Ok(Self {
            inner: Arc::new(Mutex::new(FrequenciesInner { base: SampledStatBase::new(0.0), bin_scheme })),
            frequencies,
        })
    }
}

impl Stat for Frequencies {
    fn record(&mut self, config: &MetricConfig, value: f64, time_ms: i64) {
        self.inner
            .lock()
            .expect("frequencies lock poisoned")
            .sampled_record(config, value, time_ms);
    }
}

impl Measurable for Frequencies {
    fn measure(&mut self, config: &MetricConfig, now: i64) -> f64 {
        self.inner
            .lock()
            .expect("frequencies lock poisoned")
            .sampled_measure(config, now)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

impl MeasurableStat for Frequencies {}

impl CompoundStat for Frequencies {
    fn stats(&self) -> Vec<NamedMeasurable> {
        self.frequencies
            .iter()
            .map(|freq| {
                let measurable: Arc<Mutex<dyn Measurable>> = Arc::new(Mutex::new(FrequencyMeasurable {
                    inner: Arc::clone(&self.inner),
                    center_value: freq.center_value(),
                }));
                NamedMeasurable::new(freq.name().clone(), measurable)
            })
            .collect()
    }
}

/// The shared sampled state backing a [`Frequencies`].
#[derive(Debug)]
struct FrequenciesInner {
    base: SampledStatBase,
    bin_scheme: Arc<dyn BinScheme>,
}

impl FrequenciesInner {
    fn frequency(&mut self, config: &MetricConfig, now: i64, center_value: f64) -> f64 {
        self.purge_obsolete_samples(config, now);
        let total_count: i64 = self.base.samples().iter().map(|s| s.event_count).sum();
        if total_count == 0 {
            return 0.0;
        }
        let bin_num = self.bin_scheme.to_bin(center_value);
        let mut count = 0.0f32;
        for sample in self.base.samples() {
            count += sample.histogram.as_ref().expect("histogram sample").counts()[bin_num];
        }
        count as f64 / total_count as f64
    }

    fn total_count(&self) -> f64 {
        self.base.samples().iter().map(|s| s.event_count).sum::<i64>() as f64
    }
}

impl SampledStat for FrequenciesInner {
    fn sampled_base(&self) -> &SampledStatBase {
        &self.base
    }

    fn sampled_base_mut(&mut self) -> &mut SampledStatBase {
        &mut self.base
    }

    fn new_sample(&self, now: i64) -> Sample {
        let mut sample = Sample::new(0.0, now);
        sample.histogram = Some(Histogram::new(Arc::clone(&self.bin_scheme)));
        sample
    }

    fn update(&mut self, sample_index: usize, _config: &MetricConfig, value: f64, _time_ms: i64) {
        self.base.samples_mut()[sample_index]
            .histogram
            .as_mut()
            .expect("histogram sample")
            .record(value);
    }

    fn combine(&self, _config: &MetricConfig, _now: i64) -> f64 {
        self.total_count()
    }
}

impl_sampled_stat_traits!(FrequenciesInner);

/// A measurable reporting the frequency of one bucket, sharing the sampled
/// state of the [`Frequencies`] that produced it.
struct FrequencyMeasurable {
    inner: Arc<Mutex<FrequenciesInner>>,
    center_value: f64,
}

impl Measurable for FrequencyMeasurable {
    fn measure(&mut self, config: &MetricConfig, now: i64) -> f64 {
        self.inner
            .lock()
            .expect("frequencies lock poisoned")
            .frequency(config, now, self.center_value)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::metrics::stats::MockTime;

    const DELTA: f64 = 0.0001;

    fn config() -> MetricConfig {
        MetricConfig::new().with_event_window(50).with_samples(2).unwrap()
    }

    fn metric_name(name: &str) -> MetricName {
        MetricName::new(name, "group-id", "desc", indexmap::IndexMap::new())
    }

    fn freq(name: &str, value: f64) -> Frequency {
        Frequency::new(metric_name(name), value)
    }

    #[test]
    fn test_frequency_center_value_above_max() {
        let err = Frequencies::new(4, 1.0, 4.0, vec![freq("1", 1.0), freq("2", 20.0)]).unwrap_err();
        assert!(
            err.message().contains("is not within the range"),
            "unexpected message: {}",
            err.message()
        );
    }

    #[test]
    fn test_frequency_center_value_below_min() {
        let err = Frequencies::new(4, 1.0, 4.0, vec![freq("1", 1.0), freq("2", -20.0)]).unwrap_err();
        assert!(
            err.message().contains("is not within the range"),
            "unexpected message: {}",
            err.message()
        );
    }

    #[test]
    fn test_more_frequency_parameters_than_buckets() {
        let err = Frequencies::new(1, 1.0, 4.0, vec![freq("1", 1.0), freq("2", -20.0)]).unwrap_err();
        assert!(
            err.message().contains("More frequencies than buckets"),
            "unexpected message: {}",
            err.message()
        );
    }

    #[test]
    fn test_boolean_frequencies_strategy1() {
        let config = config();
        let time = MockTime::new();
        let mut frequencies =
            Frequencies::for_boolean_values(Some(metric_name("false")), Some(metric_name("true"))).unwrap();
        let false_metric = Arc::clone(frequencies.stats()[0].stat());
        let true_metric = Arc::clone(frequencies.stats()[1].stat());

        // Record 25 "false" and 75 "true".
        for _ in 0..25 {
            frequencies.record(&config, 0.0, time.milliseconds());
        }
        for _ in 0..75 {
            frequencies.record(&config, 1.0, time.milliseconds());
        }
        assert!((false_metric.lock().unwrap().measure(&config, time.milliseconds()) - 0.25).abs() < DELTA);
        assert!((true_metric.lock().unwrap().measure(&config, time.milliseconds()) - 0.75).abs() < DELTA);
    }

    #[test]
    fn test_boolean_frequencies_strategy2() {
        let config = config();
        let time = MockTime::new();
        let mut frequencies =
            Frequencies::for_boolean_values(Some(metric_name("false")), Some(metric_name("true"))).unwrap();
        let false_metric = Arc::clone(frequencies.stats()[0].stat());
        let true_metric = Arc::clone(frequencies.stats()[1].stat());

        // Record 40 "false" and 60 "true".
        for _ in 0..40 {
            frequencies.record(&config, 0.0, time.milliseconds());
        }
        for _ in 0..60 {
            frequencies.record(&config, 1.0, time.milliseconds());
        }
        assert!((false_metric.lock().unwrap().measure(&config, time.milliseconds()) - 0.40).abs() < DELTA);
        assert!((true_metric.lock().unwrap().measure(&config, time.milliseconds()) - 0.60).abs() < DELTA);
    }
}
