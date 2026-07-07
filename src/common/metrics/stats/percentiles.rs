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

//! A compound statistic reporting one or more percentiles.
//!
//! Translated from `org.apache.kafka.common.metrics.stats.Percentiles`.

use std::any::Any;
use std::sync::{Arc, Mutex};

use crate::common::KafkaError;
use crate::common::metrics::stats::histogram::{BinScheme, ConstantBinScheme, Histogram, LinearBinScheme};
use crate::common::metrics::stats::sampled_stat::{SampledStat, SampledStatBase, impl_sampled_stat_traits};
use crate::common::metrics::stats::{Percentile, Sample};
use crate::common::metrics::{CompoundStat, Measurable, MeasurableStat, MetricConfig, NamedMeasurable, Stat};

/// How the percentile buckets are sized across the value range.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BucketSizing {
    /// Bins of constant width.
    Constant,
    /// Bins of linearly increasing width.
    Linear,
}

/// A [`CompoundStat`] that reports one or more percentiles of the recorded
/// values.
#[derive(Debug)]
pub struct Percentiles {
    inner: Arc<Mutex<PercentilesInner>>,
    percentiles: Vec<Percentile>,
}

impl Percentiles {
    /// Creates percentiles over `[0.0, max]`.
    ///
    /// Returns [`KafkaError::IllegalArgument`] for an invalid bucket
    /// configuration.
    pub fn with_max(
        size_in_bytes: i32,
        max: f64,
        bucketing: BucketSizing,
        percentiles: Vec<Percentile>,
    ) -> Result<Self, KafkaError> {
        Self::new(size_in_bytes, 0.0, max, bucketing, percentiles)
    }

    /// Creates percentiles over `[min, max]`.
    ///
    /// Returns [`KafkaError::IllegalArgument`] for an invalid bucket
    /// configuration (fewer than two bins, or linear bucketing with a non-zero
    /// minimum).
    pub fn new(
        size_in_bytes: i32,
        min: f64,
        max: f64,
        bucketing: BucketSizing,
        percentiles: Vec<Percentile>,
    ) -> Result<Self, KafkaError> {
        let buckets = size_in_bytes / 4;
        let bin_scheme: Arc<dyn BinScheme> = match bucketing {
            BucketSizing::Constant => Arc::new(ConstantBinScheme::new(buckets, min, max)?),
            BucketSizing::Linear => {
                if min != 0.0 {
                    return Err(KafkaError::illegal_argument("Linear bucket sizing requires min to be 0.0."));
                }
                Arc::new(LinearBinScheme::new(buckets, max)?)
            },
        };
        Ok(Self {
            inner: Arc::new(Mutex::new(PercentilesInner {
                base: SampledStatBase::new(0.0),
                bin_scheme,
                buckets: buckets.max(0) as usize,
                min,
                max,
            })),
            percentiles,
        })
    }
}

impl Stat for Percentiles {
    fn record(&mut self, config: &MetricConfig, value: f64, time_ms: i64) {
        self.inner
            .lock()
            .expect("percentiles lock poisoned")
            .sampled_record(config, value, time_ms);
    }
}

impl Measurable for Percentiles {
    fn measure(&mut self, config: &MetricConfig, now: i64) -> f64 {
        self.inner
            .lock()
            .expect("percentiles lock poisoned")
            .sampled_measure(config, now)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

impl MeasurableStat for Percentiles {}

impl CompoundStat for Percentiles {
    fn stats(&self) -> Vec<NamedMeasurable> {
        self.percentiles
            .iter()
            .map(|percentile| {
                let measurable: Arc<Mutex<dyn Measurable>> = Arc::new(Mutex::new(PercentileMeasurable {
                    inner: Arc::clone(&self.inner),
                    quantile: percentile.percentile() / 100.0,
                }));
                NamedMeasurable::new(percentile.name().clone(), measurable)
            })
            .collect()
    }
}

/// The shared sampled state backing a [`Percentiles`].
#[derive(Debug)]
struct PercentilesInner {
    base: SampledStatBase,
    bin_scheme: Arc<dyn BinScheme>,
    buckets: usize,
    min: f64,
    max: f64,
}

impl PercentilesInner {
    fn value(&mut self, config: &MetricConfig, now: i64, quantile: f64) -> f64 {
        self.purge_obsolete_samples(config, now);
        self.compute_quantile(quantile)
    }

    /// Scans the histograms for the given quantile without purging; callers
    /// purge first where required.
    fn compute_quantile(&self, quantile: f64) -> f64 {
        let count: f32 = self.base.samples().iter().map(|s| s.event_count as f32).sum();
        if count == 0.0 {
            return f64::NAN;
        }
        let mut sum = 0.0f32;
        let quant = quantile as f32;
        for b in 0..self.buckets {
            for sample in self.base.samples() {
                sum += sample.histogram.as_ref().expect("histogram sample").counts()[b];
                if sum / count > quant {
                    return self.bin_scheme.from_bin(b as i32);
                }
            }
        }
        f64::INFINITY
    }
}

impl SampledStat for PercentilesInner {
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
        let bounded_value = if value > self.max {
            self.max
        } else if value < self.min {
            self.min
        } else {
            value
        };
        self.base.samples_mut()[sample_index]
            .histogram
            .as_mut()
            .expect("histogram sample")
            .record(bounded_value);
    }

    fn combine(&self, _config: &MetricConfig, _now: i64) -> f64 {
        // measure() purges before calling combine(), so scan for the median
        // directly.
        self.compute_quantile(0.5)
    }
}

impl_sampled_stat_traits!(PercentilesInner);

/// A measurable reporting one quantile, sharing the sampled state of the
/// [`Percentiles`] that produced it.
struct PercentileMeasurable {
    inner: Arc<Mutex<PercentilesInner>>,
    quantile: f64,
}

impl Measurable for PercentileMeasurable {
    fn measure(&mut self, config: &MetricConfig, now: i64) -> f64 {
        self.inner
            .lock()
            .expect("percentiles lock poisoned")
            .value(config, now, self.quantile)
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
    use crate::common::MetricName;

    fn metric_name(name: &str) -> MetricName {
        MetricName::new(name, "group", "", indexmap::IndexMap::new())
    }

    #[test]
    fn test_reports_percentile_via_stats() {
        let p50 = Percentile::new(metric_name("p50"), 50.0);
        let mut percentiles = Percentiles::new(400, 0.0, 100.0, BucketSizing::Constant, vec![p50]).unwrap();
        let p50_metric = Arc::clone(percentiles.stats()[0].stat());

        let config = MetricConfig::new();
        for value in 0..100 {
            percentiles.record(&config, value as f64, 0);
        }

        // The median of 0..100 should land near 50.
        let median = p50_metric.lock().unwrap().measure(&config, 0);
        assert!((40.0..=60.0).contains(&median), "median={median}");
    }
}
