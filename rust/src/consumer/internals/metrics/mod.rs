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

//! Consumer metrics managers (`org.apache.kafka.clients.consumer.internals.metrics`).
//!
//! Mirrors the Java package one file per class. The package is `internals`, so
//! every type here is `pub(crate)` (CLAUDE.md §2).
//!
//! Java's abstract `RebalanceMetricsManager` has one in-scope subclass,
//! [`ConsumerRebalanceMetricsManager`], and is folded into it (the share
//! subclass is out of scope, consumer-threading.md §20). `KafkaShareConsumerMetrics`
//! and `ShareRebalanceMetricsManager` are share-consumer classes and are not
//! translated.

mod abstract_consumer_metrics_manager;
mod async_consumer_metrics;
mod consumer_rebalance_metrics_manager;
mod heartbeat_metrics_manager;
mod kafka_consumer_metrics;
mod metrics_ledger;
mod offset_commit_metrics_manager;
mod rebalance_callback_metrics_manager;
mod sensor_builder;

pub(crate) use abstract_consumer_metrics_manager::AbstractConsumerMetricsManager;
pub(crate) use async_consumer_metrics::AsyncConsumerMetrics;
pub(crate) use consumer_rebalance_metrics_manager::ConsumerRebalanceMetricsManager;
pub(crate) use heartbeat_metrics_manager::HeartbeatMetricsManager;
pub(crate) use kafka_consumer_metrics::KafkaConsumerMetrics;
pub(crate) use metrics_ledger::MetricsLedger;
pub(crate) use offset_commit_metrics_manager::OffsetCommitMetricsManager;
pub(crate) use rebalance_callback_metrics_manager::RebalanceCallbackMetricsManager;
pub(crate) use sensor_builder::SensorBuilder;
