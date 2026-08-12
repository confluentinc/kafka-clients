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
pub mod classic_group_state;
pub mod cluster;
pub mod cluster_resource;
pub mod cluster_resource_listener;
pub mod compress;
pub mod config;
pub mod consumer_group_state;
pub mod election_type;
pub mod feature;
pub mod group_state;
pub mod group_type;
pub mod header;
pub(crate) mod internals;
pub mod isolation_level;
pub mod kafka_error;
pub mod kafka_future;
pub mod memory;
pub mod metric;
pub mod metric_name;
pub mod metric_name_template;
pub mod metrics;
pub mod network;
pub mod node;
pub mod partition_info;
pub mod protocol;
pub mod quota;
pub mod record;
pub mod requests;
pub mod resource;
pub mod security;
pub mod serialization;
pub mod topic_collection;
pub mod topic_id_partition;
pub mod topic_partition;
pub mod topic_partition_info;
pub mod topic_partition_replica;
pub mod utils;
pub mod uuid;

pub use classic_group_state::ClassicGroupState;
pub use cluster::Cluster;
pub use cluster_resource::ClusterResource;
pub use cluster_resource_listener::ClusterResourceListener;
#[allow(deprecated)]
pub use consumer_group_state::ConsumerGroupState;
pub use election_type::ElectionType;
pub use group_state::GroupState;
pub use group_type::GroupType;
pub use isolation_level::IsolationLevel;
pub use kafka_error::{Error, KafkaGenericError};
pub use kafka_future::KafkaFuture;
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
