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

#![allow(dead_code)]
//! Test utilities for creating metadata responses.
//!
//! Corresponds to `org.apache.kafka.common.requests.RequestTestUtils`.

use std::collections::HashMap;

use crate::common::Node;
use crate::common::TopicPartition;
use crate::common::Uuid;
use crate::common::internals::Topic;
use crate::common::protocol::{ApiKeys, Errors};

use super::metadata_response::AUTHORIZED_OPERATIONS_OMITTED;
use super::{MetadataResponse, PartitionMetadata, TopicMetadata};

/// Default partition metadata supplier: creates a standard `PartitionMetadata`.
fn default_partition_supplier(
    error: Errors,
    partition: &TopicPartition,
    leader_id: Option<i32>,
    leader_epoch: Option<i32>,
    replicas: Vec<i32>,
    isr: Vec<i32>,
    offline_replicas: Vec<i32>,
) -> PartitionMetadata {
    PartitionMetadata {
        error,
        topic_partition: partition.clone(),
        leader_id,
        leader_epoch,
        replica_ids: replicas,
        in_sync_replica_ids: isr,
        offline_replica_ids: offline_replicas,
    }
}

/// Functional type for supplying partition metadata in tests.
pub type PartitionMetadataSupplier =
    Box<dyn Fn(Errors, &TopicPartition, Option<i32>, Option<i32>, Vec<i32>, Vec<i32>, Vec<i32>) -> PartitionMetadata>;

/// Creates a metadata response from topic metadata.
pub fn metadata_response(
    brokers: &[Node],
    cluster_id: Option<&str>,
    controller_id: i32,
    topic_metadata_list: Vec<TopicMetadata>,
) -> MetadataResponse {
    metadata_response_with_version(
        brokers,
        cluster_id,
        controller_id,
        topic_metadata_list,
        ApiKeys::METADATA.latest_version(),
    )
}

/// Creates a metadata response from topic metadata at a specific version.
pub fn metadata_response_with_version(
    brokers: &[Node],
    cluster_id: Option<&str>,
    controller_id: i32,
    topic_metadata_list: Vec<TopicMetadata>,
    response_version: i16,
) -> MetadataResponse {
    use crate::metadata_response_data::{MetadataResponsePartition, MetadataResponseTopic};

    let mut topics = Vec::new();
    for topic_metadata in &topic_metadata_list {
        let mut response_topic = MetadataResponseTopic::new();
        response_topic.set_error_code(topic_metadata.error().code());
        response_topic.set_name(Some(topic_metadata.topic().to_string()));
        response_topic.set_topic_id(topic_metadata.topic_id());
        response_topic.set_is_internal(topic_metadata.is_internal());
        response_topic.set_topic_authorized_operations(topic_metadata.authorized_operations());

        let mut response_partitions = Vec::new();
        for pm in topic_metadata.partition_metadata() {
            let mut rp = MetadataResponsePartition::new();
            rp.set_error_code(pm.error.code());
            rp.set_partition_index(pm.partition());
            rp.set_leader_id(pm.leader_id.unwrap_or(super::metadata_response::NO_LEADER_ID));
            rp.set_leader_epoch(pm.leader_epoch.unwrap_or(super::RECORD_BATCH_NO_PARTITION_LEADER_EPOCH));
            rp.set_replica_nodes(pm.replica_ids.clone());
            rp.set_isr_nodes(pm.in_sync_replica_ids.clone());
            rp.set_offline_replicas(pm.offline_replica_ids.clone());
            response_partitions.push(rp);
        }
        response_topic.set_partitions(response_partitions);
        topics.push(response_topic);
    }

    MetadataResponse::prepare_response(
        response_version,
        0, // DEFAULT_THROTTLE_TIME
        brokers,
        cluster_id.map(|s| s.to_string()),
        controller_id,
        topics,
        AUTHORIZED_OPERATIONS_OMITTED,
    )
}

/// Creates a metadata response with the given number of nodes and topic partition counts.
pub fn metadata_update_with(num_nodes: i32, topic_partition_counts: &HashMap<String, i32>) -> MetadataResponse {
    metadata_update_with_cluster_id(
        "kafka-cluster",
        num_nodes,
        &HashMap::new(),
        topic_partition_counts,
        &|_tp: &TopicPartition| None,
    )
}

/// Creates a metadata response with the given cluster id, nodes, and topic errors/partitions.
pub fn metadata_update_with_cluster_id(
    cluster_id: &str,
    num_nodes: i32,
    topic_errors: &HashMap<String, Errors>,
    topic_partition_counts: &HashMap<String, i32>,
    epoch_supplier: &dyn Fn(&TopicPartition) -> Option<i32>,
) -> MetadataResponse {
    metadata_update_with_full(
        cluster_id,
        num_nodes,
        topic_errors,
        topic_partition_counts,
        epoch_supplier,
        &default_partition_supplier,
        ApiKeys::METADATA.latest_version(),
        &HashMap::new(),
    )
}

/// Creates a metadata response with topic IDs.
pub fn metadata_update_with_ids(
    cluster_id: &str,
    num_nodes: i32,
    topic_errors: &HashMap<String, Errors>,
    topic_partition_counts: &HashMap<String, i32>,
    epoch_supplier: &dyn Fn(&TopicPartition) -> Option<i32>,
    topic_ids: &HashMap<String, Uuid>,
) -> MetadataResponse {
    metadata_update_with_full(
        cluster_id,
        num_nodes,
        topic_errors,
        topic_partition_counts,
        epoch_supplier,
        &default_partition_supplier,
        ApiKeys::METADATA.latest_version(),
        topic_ids,
    )
}

/// Type alias for partition supplier callback to avoid overly complex type signatures.
pub type PartitionSupplier =
    dyn Fn(Errors, &TopicPartition, Option<i32>, Option<i32>, Vec<i32>, Vec<i32>, Vec<i32>) -> PartitionMetadata;

/// The most general metadata update builder used by tests.
#[allow(clippy::too_many_arguments)]
pub fn metadata_update_with_full(
    cluster_id: &str,
    num_nodes: i32,
    topic_errors: &HashMap<String, Errors>,
    topic_partition_counts: &HashMap<String, i32>,
    epoch_supplier: &dyn Fn(&TopicPartition) -> Option<i32>,
    partition_supplier: &PartitionSupplier,
    response_version: i16,
    topic_ids: &HashMap<String, Uuid>,
) -> MetadataResponse {
    let mut nodes = Vec::with_capacity(num_nodes as usize);
    for i in 0..num_nodes {
        nodes.push(Node::new(i, "localhost".to_string(), 1969 + i));
    }

    let mut topic_metadata = Vec::new();

    for (topic, &num_partitions) in topic_partition_counts {
        let mut partition_metadata = Vec::with_capacity(num_partitions as usize);
        for i in 0..num_partitions {
            let tp = TopicPartition::new(topic.clone(), i);
            let leader = &nodes[(i as usize) % nodes.len()];
            let replica_ids = vec![leader.id()];
            partition_metadata.push(partition_supplier(
                Errors::None,
                &tp,
                Some(leader.id()),
                epoch_supplier(&tp),
                replica_ids.clone(),
                replica_ids,
                Vec::new(),
            ));
        }

        let topic_id = topic_ids.get(topic).copied().unwrap_or(Uuid::zero());
        topic_metadata.push(TopicMetadata {
            error: Errors::None,
            topic: topic.clone(),
            topic_id,
            is_internal: Topic::is_internal(topic),
            partition_metadata,
            authorized_operations: AUTHORIZED_OPERATIONS_OMITTED,
        });
    }

    for (topic, error) in topic_errors {
        topic_metadata.push(TopicMetadata::new_simple(
            *error,
            topic.clone(),
            Topic::is_internal(topic),
            Vec::new(),
        ));
    }

    metadata_response_with_version(&nodes, Some(cluster_id), 0, topic_metadata, response_version)
}
