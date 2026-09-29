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

//! Configuration values for metrics (`org.apache.kafka.common.metrics.MetricConfig`).

use std::collections::BTreeMap;

use crate::common::metrics::internals::TimeUnit;
use crate::common::metrics::{Quota, RecordingLevel};

/// Configuration values for metrics.
#[derive(Clone, Debug)]
pub struct MetricConfig {
    quota: Option<Quota>,
    samples: i32,
    event_window: i64,
    time_window_ms: i64,
    tags: BTreeMap<String, String>,
    recording_level: RecordingLevel,
}

impl Default for MetricConfig {
    fn default() -> Self {
        Self {
            quota: None,
            samples: MetricConfig::DEFAULT_NUM_SAMPLES,
            event_window: i64::MAX,
            time_window_ms: MetricConfig::DEFAULT_TIME_WINDOW_MS,
            tags: BTreeMap::new(),
            recording_level: RecordingLevel::Info,
        }
    }
}

impl MetricConfig {
    /// Default number of samples for a windowed stat.
    pub const DEFAULT_NUM_SAMPLES: i32 = 2;

    /// 30 seconds, the default time window, in milliseconds.
    const DEFAULT_TIME_WINDOW_MS: i64 = 30 * 1000;

    /// Create a `MetricConfig` with default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// The configured quota, if any.
    pub fn quota(&self) -> Option<Quota> {
        self.quota
    }

    /// Set the quota.
    pub fn set_quota(mut self, quota: Quota) -> Self {
        self.quota = Some(quota);
        self
    }

    /// The number of events in an event window.
    pub fn event_window(&self) -> i64 {
        self.event_window
    }

    /// Set the event window.
    pub fn set_event_window(mut self, window: i64) -> Self {
        self.event_window = window;
        self
    }

    /// The time window in milliseconds.
    pub fn time_window_ms(&self) -> i64 {
        self.time_window_ms
    }

    /// Set the time window in milliseconds.
    pub fn set_time_window_ms(mut self, window_ms: i64) -> Self {
        self.time_window_ms = window_ms;
        self
    }

    /// Set the time window expressed in the given unit, mirroring Java's
    /// `MetricConfig.timeWindow(long window, TimeUnit unit)`.
    pub fn set_time_window(mut self, window: i64, unit: TimeUnit) -> Self {
        self.time_window_ms = unit.to_millis(window);
        self
    }

    /// The default tags for metrics using this config.
    pub fn tags(&self) -> &BTreeMap<String, String> {
        &self.tags
    }

    /// Set the default tags.
    pub fn set_tags(mut self, tags: BTreeMap<String, String>) -> Self {
        self.tags = tags;
        self
    }

    /// The number of samples for windowed stats.
    pub fn samples(&self) -> i32 {
        self.samples
    }

    /// Set the number of samples. Panics if `samples < 1`, mirroring Java's
    /// `IllegalArgumentException` — this is a configuration-time programming
    /// error rather than a recoverable runtime condition.
    pub fn set_samples(mut self, samples: i32) -> Self {
        assert!(samples >= 1, "The number of samples must be at least 1.");
        self.samples = samples;
        self
    }

    /// The recording level.
    pub fn record_level(&self) -> RecordingLevel {
        self.recording_level
    }

    /// Set the recording level.
    pub fn set_record_level(mut self, recording_level: RecordingLevel) -> Self {
        self.recording_level = recording_level;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_match_java() {
        let c = MetricConfig::new();
        assert_eq!(c.samples(), 2);
        assert_eq!(c.event_window(), i64::MAX);
        assert_eq!(c.time_window_ms(), 30_000);
        assert_eq!(c.record_level(), RecordingLevel::Info);
        assert!(c.quota().is_none());
        assert!(c.tags().is_empty());
    }

    #[test]
    #[should_panic(expected = "The number of samples must be at least 1.")]
    fn samples_below_one_panics() {
        let _ = MetricConfig::new().set_samples(0);
    }
}
