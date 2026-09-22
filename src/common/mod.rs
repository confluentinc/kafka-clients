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

//! Common types and utilities for Kafka (org.apache.kafka.common)

pub mod acl;
mod classic_group_state;
mod cluster;
mod cluster_resource;
mod cluster_resource_listener;
pub mod compress;
pub mod config;
mod consumer_group_state;
mod election_type;
// `pub(crate)` rather than private: `kafka_error_class!` and
// `message_only_error!` expand `$crate::common::error::ErrorSource` and eight
// similar absolute paths at every one of their ~150 call sites, so the module
// has to be nameable crate-wide. The types it holds are still reached through
// the re-exports below (CLAUDE.md §2).
pub(crate) mod error;
/// Kafka's exception classes (`org.apache.kafka.common.errors`).
pub mod errors;
pub mod feature;
mod group_state;
mod group_type;
pub mod header;
pub(crate) mod internals;
mod invalid_record_error;
mod isolation_level;
mod kafka_error;
mod kafka_future;
mod local_concurrent_modification_error;
mod local_illegal_argument_error;
mod local_illegal_state_error;
mod local_timeout_error;
pub mod memory;
mod metric;
mod metric_name;
mod metric_name_template;
pub mod metrics;
pub mod network;
mod node;
mod partition_info;
pub mod protocol;
pub mod quota;
pub mod record;
pub mod requests;
pub mod resource;
pub mod security;
pub mod serialization;
mod topic_collection;
mod topic_id_partition;
mod topic_partition;
mod topic_partition_info;
mod topic_partition_replica;
pub mod utils;
mod uuid;

pub use classic_group_state::ClassicGroupState;
pub use cluster::Cluster;
pub use cluster_resource::ClusterResource;
pub use cluster_resource_listener::ClusterResourceListener;
#[allow(deprecated)]
pub use consumer_group_state::ConsumerGroupState;
pub use election_type::ElectionType;
pub use error::Error;
pub use group_state::GroupState;
pub use group_type::GroupType;
pub use invalid_record_error::InvalidRecordError;
pub use isolation_level::IsolationLevel;
// `ErrorHierarchy` is deliberately NOT re-exported: it is the mechanism behind
// `Error`'s predicates, used only inside `error.rs` and by the error payloads
// that declare their own ancestry. Callers — in-crate, external, and the C FFI
// alike — use the inherent methods on `Error`.
pub use kafka_error::KafkaError;
pub use kafka_future::KafkaFuture;
pub(crate) use kafka_future::KafkaFutureOps;
// The four JDK classes the client raises itself. Re-exported here rather than
// from `common::errors`, which is Java's `org.apache.kafka.common.errors`
// package and holds none of them.
pub use local_concurrent_modification_error::LocalConcurrentModificationError;
pub use local_illegal_argument_error::LocalIllegalArgumentError;
pub use local_illegal_state_error::LocalIllegalStateError;
pub use local_timeout_error::LocalTimeoutError;
pub use metric::{Metric, MetricValue};
pub use metric_name::MetricName;
pub use metric_name_template::MetricNameTemplate;
pub use node::Node;
pub use partition_info::PartitionInfo;
pub use protocol::{ApiKeys, ByteBufferAccessor, Errors, Readable, Writable};
pub use topic_collection::TopicCollection;
pub use topic_id_partition::TopicIdPartition;
pub use topic_partition::TopicPartition;
pub use topic_partition_info::TopicPartitionInfo;
pub use topic_partition_replica::TopicPartitionReplica;
pub use uuid::Uuid;
