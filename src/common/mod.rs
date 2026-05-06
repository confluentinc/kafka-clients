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

//! Translation of `org.apache.kafka.common`.

pub mod cluster;
pub mod cluster_resource;
pub mod cluster_resource_listener;
pub mod compress;
pub mod config;
pub mod errors;
pub mod feature;
pub mod header;
pub mod internals;
pub mod kafka_exception;
pub mod message;
pub mod network;
pub mod node;
pub mod partition_info;
pub mod protocol;
pub mod record;
pub mod requests;
pub mod security;
pub mod serialization;
pub mod topic_collection;
pub mod topic_id_partition;
pub mod topic_partition;
pub mod topic_partition_info;
pub mod utils;
pub mod uuid;

pub use cluster::Cluster;
pub use cluster_resource::ClusterResource;
pub use cluster_resource_listener::ClusterResourceListener;
pub use errors::KafkaError;
pub use kafka_exception::KafkaException;
pub use node::Node;
pub use partition_info::PartitionInfo;
pub use topic_collection::{TopicCollection, TopicIdCollection, TopicNameCollection};
pub use topic_id_partition::TopicIdPartition;
pub use topic_partition::TopicPartition;
pub use topic_partition_info::TopicPartitionInfo;
pub use uuid::Uuid;
