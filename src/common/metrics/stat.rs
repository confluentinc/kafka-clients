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

//! A `Stat` is a quantity computed off the stream of updates to a sensor
//! (`org.apache.kafka.common.metrics.Stat`).

use crate::common::metrics::MetricConfig;

/// A `Stat` is a quantity such as average, max, etc that is computed off the
/// stream of updates to a sensor.
///
/// `record` takes `&self`: the simple cumulative stats use interior mutability
/// (atomics) so that recording is lock-free, which is the minimal Rust
/// equivalent of Java guarding the stat field with the sensor's `synchronized`.
pub trait Stat: Send + Sync {
    /// Record the given value.
    ///
    /// * `config` - The configuration to use for this metric
    /// * `value` - The value to record
    /// * `time_ms` - The POSIX time in milliseconds this value occurred
    fn record(&self, config: &MetricConfig, value: f64, time_ms: i64);
}
