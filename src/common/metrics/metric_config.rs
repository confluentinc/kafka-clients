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

//! Configuration values for metrics.
//!
//! Translated from `org.apache.kafka.common.metrics.MetricConfig`.

use indexmap::IndexMap;

use crate::common::KafkaError;
use crate::common::metrics::{Quota, RecordingLevel, TimeUnit};

/// The default number of samples retained by a sampled statistic.
pub const DEFAULT_NUM_SAMPLES: i32 = 2;

/// Configuration values shared by the metrics recorded under a sensor.
///
/// Fluent setters are named `with_*` because Rust cannot overload the getter
/// and setter under one identifier the way the Java source does.
#[derive(Clone, Debug)]
pub struct MetricConfig {
    quota: Option<Quota>,
    samples: i32,
    event_window: i64,
    time_window_ms: i64,
    tags: IndexMap<String, String>,
    recording_level: RecordingLevel,
}

impl Default for MetricConfig {
    fn default() -> Self {
        Self {
            quota: None,
            samples: DEFAULT_NUM_SAMPLES,
            event_window: i64::MAX,
            time_window_ms: TimeUnit::Seconds.to_millis(30),
            tags: IndexMap::new(),
            recording_level: RecordingLevel::Info,
        }
    }
}

impl MetricConfig {
    /// Creates a metric config with default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// The quota, if configured.
    pub fn quota(&self) -> Option<&Quota> {
        self.quota.as_ref()
    }

    /// Sets the quota.
    pub fn with_quota(mut self, quota: Quota) -> Self {
        self.quota = Some(quota);
        self
    }

    /// The event window size.
    pub fn event_window(&self) -> i64 {
        self.event_window
    }

    /// Sets the event window size.
    pub fn with_event_window(mut self, window: i64) -> Self {
        self.event_window = window;
        self
    }

    /// The time window size in milliseconds.
    pub fn time_window_ms(&self) -> i64 {
        self.time_window_ms
    }

    /// Sets the time window, expressed in the given unit.
    pub fn with_time_window(mut self, window: i64, unit: TimeUnit) -> Self {
        self.time_window_ms = unit.to_millis(window);
        self
    }

    /// The metric tags.
    pub fn tags(&self) -> &IndexMap<String, String> {
        &self.tags
    }

    /// Sets the metric tags.
    pub fn with_tags(mut self, tags: IndexMap<String, String>) -> Self {
        self.tags = tags;
        self
    }

    /// The number of samples retained by sampled statistics.
    pub fn samples(&self) -> i32 {
        self.samples
    }

    /// Sets the number of samples.
    ///
    /// Returns [`KafkaError::IllegalArgument`] if fewer than one sample is
    /// requested.
    pub fn with_samples(mut self, samples: i32) -> Result<Self, KafkaError> {
        if samples < 1 {
            return Err(KafkaError::illegal_argument("The number of samples must be at least 1."));
        }
        self.samples = samples;
        Ok(self)
    }

    /// The configured recording level.
    pub fn record_level(&self) -> RecordingLevel {
        self.recording_level
    }

    /// Sets the recording level.
    pub fn with_record_level(mut self, recording_level: RecordingLevel) -> Self {
        self.recording_level = recording_level;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_defaults() {
        let config = MetricConfig::new();
        assert!(config.quota().is_none());
        assert_eq!(config.samples(), 2);
        assert_eq!(config.event_window(), i64::MAX);
        assert_eq!(config.time_window_ms(), 30_000);
        assert!(config.tags().is_empty());
        assert_eq!(config.record_level(), RecordingLevel::Info);
    }

    #[test]
    fn test_builders() {
        let config = MetricConfig::new()
            .with_time_window(1, TimeUnit::Seconds)
            .with_samples(4)
            .unwrap()
            .with_event_window(50)
            .with_quota(Quota::upper_bound(5.0))
            .with_record_level(RecordingLevel::Debug);
        assert_eq!(config.time_window_ms(), 1000);
        assert_eq!(config.samples(), 4);
        assert_eq!(config.event_window(), 50);
        assert_eq!(config.quota().unwrap().bound(), 5.0);
        assert_eq!(config.record_level(), RecordingLevel::Debug);
    }

    #[test]
    fn test_samples_must_be_at_least_one() {
        let err = MetricConfig::new().with_samples(0).unwrap_err();
        assert!(
            err.message().contains("The number of samples must be at least 1."),
            "unexpected message: {}",
            err.message()
        );
    }
}
