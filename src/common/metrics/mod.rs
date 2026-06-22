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

//! The Kafka metrics framework (`org.apache.kafka.common.metrics`).
//!
//! Faithful translation of the Java metrics core: same class names, file-per-
//! class, identical metric values / names / recording-level gating. Java's
//! `synchronized` is mapped to the minimal Rust atomic/lock equivalent (atomics
//! for pure-counter stats, `Mutex` where multi-field mutation needs it). JMX is
//! replaced by the [`MetricsReporter`] trait seam.

pub mod compound_stat;
pub mod gauge;
pub mod internals;
pub mod kafka_metric;
pub mod measurable;
pub mod measurable_stat;
pub mod metric_config;
pub mod metric_value_provider;
#[allow(clippy::module_inception)]
pub mod metrics;
pub mod metrics_reporter;
pub mod quota;
pub mod sensor;
pub mod stat;
pub mod stats;
pub mod time;

pub use compound_stat::{CompoundStat, NamedMeasurable};
pub use gauge::{ClosureGauge, Gauge};
pub use kafka_metric::KafkaMetric;
pub use measurable::Measurable;
pub use measurable_stat::MeasurableStat;
// `Metric` and `MetricValue` live in `org.apache.kafka.common` (→ `common::metric`);
// re-export `MetricValue` here for convenience since the stats/providers produce it.
pub use crate::common::metric::MetricValue;
pub use metric_config::{DEFAULT_NUM_SAMPLES, MetricConfig};
pub use metric_value_provider::MetricValueProvider;
pub use metrics::Metrics;
pub use metrics_reporter::MetricsReporter;
pub use quota::Quota;
pub use sensor::{RecordingLevel, Sensor};
pub use stat::Stat;
pub use time::{SystemTime, Time};
