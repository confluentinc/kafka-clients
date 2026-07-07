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
pub mod metrics_context;
pub mod metrics_reporter;
pub mod quota;
pub mod quota_violation_error;
pub mod sensor;
pub mod stat;
pub mod time_unit;

pub use compound_stat::{CompoundStat, NamedMeasurable};
pub use gauge::Gauge;
pub use kafka_metric::KafkaMetric;
pub use kafka_metrics_context::KafkaMetricsContext;
pub use measurable::Measurable;
pub use measurable_stat::MeasurableStat;
pub use metric_config::MetricConfig;
pub use metric_value_provider::{MetricValue, MetricValueProvider};
pub use metrics_context::MetricsContext;
pub use metrics_reporter::MetricsReporter;
pub use quota::Quota;
pub use quota_violation_error::QuotaViolationError;
pub use sensor::RecordingLevel;
pub use stat::Stat;
pub use time_unit::TimeUnit;
