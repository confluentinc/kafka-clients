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

//! Translation of `org.apache.kafka.clients.MetadataSnapshot`.

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::sync::Arc;

use crate::common::cluster::Cluster;
use crate::common::cluster_resource::ClusterResource;
use crate::common::node::Node;
use crate::common::requests::metadata_response::{MetadataResponse, PartitionMetadata};
use crate::common::topic_partition::TopicPartition;
use crate::common::uuid::Uuid;

/// An internal immutable snapshot of nodes, topics, and partitions in the
/// Kafka cluster. This keeps an up-to-date [`Cluster`] instance which is
/// optimized for read access.
///
/// Prefer to extend the `MetadataSnapshot` API for internal client usage
/// versus the public [`Cluster`].
///
/// All inner state is immutable post-construction. The snapshot is shared
/// by cloning an [`Arc<MetadataSnapshot>`] from [`crate::metadata::Metadata`].
pub struct MetadataSnapshot {
    cluster_id: Option<String>,
    nodes: HashMap<i32, Node>,
    unauthorized_topics: HashSet<String>,
    invalid_topics: HashSet<String>,
    internal_topics: HashSet<String>,
    controller: Option<Node>,
    metadata_by_partition: HashMap<TopicPartition, PartitionMetadata>,
    topic_ids: HashMap<String, Uuid>,
    topic_names: HashMap<Uuid, String>,
    cluster_instance: Arc<Cluster>,
}

impl MetadataSnapshot {
    /// Mirrors the 8-arg Java constructor (cluster_instance built lazily).
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        cluster_id: Option<String>,
        nodes: HashMap<i32, Node>,
        partitions: Vec<PartitionMetadata>,
        unauthorized_topics: HashSet<String>,
        invalid_topics: HashSet<String>,
        internal_topics: HashSet<String>,
        controller: Option<Node>,
        topic_ids: HashMap<String, Uuid>,
    ) -> Self {
        Self::new_with_cluster(
            cluster_id,
            nodes,
            partitions,
            unauthorized_topics,
            invalid_topics,
            internal_topics,
            controller,
            topic_ids,
            None,
        )
    }

    /// Mirrors the 9-arg "visible for testing" Java constructor.
    #[allow(clippy::too_many_arguments)]
    pub fn new_with_cluster(
        cluster_id: Option<String>,
        nodes: HashMap<i32, Node>,
        partitions: Vec<PartitionMetadata>,
        unauthorized_topics: HashSet<String>,
        invalid_topics: HashSet<String>,
        internal_topics: HashSet<String>,
        controller: Option<Node>,
        topic_ids: HashMap<String, Uuid>,
        cluster_instance: Option<Arc<Cluster>>,
    ) -> Self {
        let topic_names: HashMap<Uuid, String> = topic_ids.iter().map(|(k, v)| (*v, k.clone())).collect();
        let mut metadata_by_partition: HashMap<TopicPartition, PartitionMetadata> =
            HashMap::with_capacity(partitions.len());
        for p in partitions {
            metadata_by_partition.insert(p.topic_partition.clone(), p);
        }

        let cluster_instance = match cluster_instance {
            Some(c) => c,
            None => Arc::new(Self::compute_cluster_view(
                cluster_id.as_deref(),
                &nodes,
                &metadata_by_partition,
                &unauthorized_topics,
                &invalid_topics,
                &internal_topics,
                controller.as_ref(),
                &topic_ids,
            )),
        };

        MetadataSnapshot {
            cluster_id,
            nodes,
            unauthorized_topics,
            invalid_topics,
            internal_topics,
            controller,
            metadata_by_partition,
            topic_ids,
            topic_names,
            cluster_instance,
        }
    }

    /// Mirrors the package-private `partitionMetadata(TopicPartition)`.
    pub fn partition_metadata(&self, topic_partition: &TopicPartition) -> Option<&PartitionMetadata> {
        self.metadata_by_partition.get(topic_partition)
    }

    /// Mirrors `topicIds()`.
    pub fn topic_ids(&self) -> &HashMap<String, Uuid> {
        &self.topic_ids
    }

    /// Mirrors `topicNames()`.
    pub fn topic_names(&self) -> &HashMap<Uuid, String> {
        &self.topic_names
    }

    /// Mirrors `nodeById(int)`.
    pub fn node_by_id(&self, id: i32) -> Option<&Node> {
        self.nodes.get(&id)
    }

    /// Mirrors `cluster()`. Returns the cached snapshot via `Arc::clone`
    /// so the producer hot path can read without locking.
    pub fn cluster(&self) -> Arc<Cluster> {
        Arc::clone(&self.cluster_instance)
    }

    /// Borrow the cached cluster without bumping the `Arc` refcount.
    pub fn cluster_ref(&self) -> &Cluster {
        &self.cluster_instance
    }

    /// Mirrors `leaderEpochFor(TopicPartition)` — get the leader-epoch
    /// for a partition, or `None` if unknown.
    pub fn leader_epoch_for(&self, tp: &TopicPartition) -> Option<i32> {
        self.metadata_by_partition.get(tp).and_then(|pm| pm.leader_epoch)
    }

    /// Mirrors `clusterResource()` — wraps the cluster id.
    pub fn cluster_resource(&self) -> ClusterResource {
        ClusterResource::new(self.cluster_id.clone())
    }

    /// Mirrors the package-private `mergeWith(...)`.
    ///
    /// Merges the snapshot's contents with the provided metadata,
    /// returning a new snapshot. The provided metadata is presumed to be
    /// more recent; overlapping metadata is overridden.
    ///
    /// `retain_topic` is invoked with `(topic, is_internal)` and decides
    /// whether the existing snapshot's data for that topic should carry
    /// over.
    #[allow(clippy::too_many_arguments)]
    pub fn merge_with(
        &self,
        new_cluster_id: Option<String>,
        new_nodes: HashMap<i32, Node>,
        add_partitions: Vec<PartitionMetadata>,
        add_unauthorized_topics: HashSet<String>,
        add_invalid_topics: HashSet<String>,
        add_internal_topics: HashSet<String>,
        new_controller: Option<Node>,
        add_topic_ids: HashMap<String, Uuid>,
        retain_topic: impl Fn(&str, bool) -> bool,
    ) -> MetadataSnapshot {
        let should_retain_topic = |topic: &str| -> bool { retain_topic(topic, self.internal_topics.contains(topic)) };

        let mut new_metadata_by_partition: HashMap<TopicPartition, PartitionMetadata> =
            HashMap::with_capacity(add_partitions.len());

        // Carry over existing topic-ids for retained topics.
        let mut new_topic_ids: HashMap<String, Uuid> = self
            .topic_ids
            .iter()
            .filter(|(k, _)| should_retain_topic(k))
            .map(|(k, v)| (k.clone(), *v))
            .collect();

        for partition in add_partitions {
            let topic = partition.topic().to_owned();
            new_metadata_by_partition.insert(partition.topic_partition.clone(), partition);
            // Mirror Java's behaviour: take topic id from add_topic_ids,
            // and explicitly remove if not present (so a topic that lost
            // its id in the latest response gets the cached id cleared).
            match add_topic_ids.get(&topic) {
                Some(id) => {
                    new_topic_ids.insert(topic, *id);
                },
                None => {
                    new_topic_ids.remove(&topic);
                },
            }
        }
        // Existing partitions not overridden by add_partitions and whose
        // topic should be retained are carried over.
        for (tp, pm) in &self.metadata_by_partition {
            if should_retain_topic(tp.topic()) {
                new_metadata_by_partition.entry(tp.clone()).or_insert_with(|| pm.clone());
            }
        }

        let new_unauthorized_topics =
            fill_set(add_unauthorized_topics, &self.unauthorized_topics, &should_retain_topic);
        let new_invalid_topics = fill_set(add_invalid_topics, &self.invalid_topics, &should_retain_topic);
        let new_internal_topics = fill_set(add_internal_topics, &self.internal_topics, &should_retain_topic);

        MetadataSnapshot::new(
            new_cluster_id,
            new_nodes,
            new_metadata_by_partition.into_values().collect(),
            new_unauthorized_topics,
            new_invalid_topics,
            new_internal_topics,
            new_controller,
            new_topic_ids,
        )
    }

    /// Build the cached `Cluster` view. Mirrors `computeClusterView()`.
    #[allow(clippy::too_many_arguments)]
    fn compute_cluster_view(
        cluster_id: Option<&str>,
        nodes: &HashMap<i32, Node>,
        metadata_by_partition: &HashMap<TopicPartition, PartitionMetadata>,
        unauthorized_topics: &HashSet<String>,
        invalid_topics: &HashSet<String>,
        internal_topics: &HashSet<String>,
        controller: Option<&Node>,
        topic_ids: &HashMap<String, Uuid>,
    ) -> Cluster {
        let partition_infos: Vec<crate::common::partition_info::PartitionInfo> = metadata_by_partition
            .values()
            .map(|pm| MetadataResponse::to_partition_info(pm, nodes))
            .collect();
        Cluster::new_with_topic_ids(
            cluster_id.map(str::to_owned),
            nodes.values().cloned().collect(),
            partition_infos,
            unauthorized_topics.clone(),
            invalid_topics.clone(),
            internal_topics.clone(),
            controller.cloned(),
            topic_ids.clone(),
        )
    }

    /// Mirrors the package-private `bootstrap(List<InetSocketAddress>)`.
    pub fn bootstrap(addresses: &[(String, u16)]) -> MetadataSnapshot {
        let mut nodes: HashMap<i32, Node> = HashMap::new();
        let mut node_id: i32 = -1;
        for (host, port) in addresses {
            nodes.insert(node_id, Node::new(node_id, host.clone(), *port as i32));
            node_id -= 1;
        }
        let cluster = Arc::new(Cluster::bootstrap(addresses));
        MetadataSnapshot::new_with_cluster(
            None,
            nodes,
            Vec::new(),
            HashSet::new(),
            HashSet::new(),
            HashSet::new(),
            None,
            HashMap::new(),
            Some(cluster),
        )
    }

    /// Mirrors the package-private `empty()`.
    pub fn empty() -> MetadataSnapshot {
        let cluster = Arc::new(Cluster::empty().clone());
        MetadataSnapshot::new_with_cluster(
            None,
            HashMap::new(),
            Vec::new(),
            HashSet::new(),
            HashSet::new(),
            HashSet::new(),
            None,
            HashMap::new(),
            Some(cluster),
        )
    }

    /// Read access to the cluster id (`None` when not provided in the
    /// `MetadataResponse`). Mirrors a getter that Java does not expose
    /// directly but is accessed via `clusterResource().clusterId()`.
    pub fn cluster_id(&self) -> Option<&str> {
        self.cluster_id.as_deref()
    }
}

/// Mirror of Java's private `fillSet<T>(baseSet, fillSet, predicate)`.
/// Returns a fresh set containing all of `base_set` plus any element of
/// `fill_set` for which `predicate(element)` is true.
fn fill_set(
    base_set: HashSet<String>,
    fill_set_in: &HashSet<String>,
    predicate: &impl Fn(&str) -> bool,
) -> HashSet<String> {
    let mut result = base_set;
    for element in fill_set_in {
        if predicate(element) {
            result.insert(element.clone());
        }
    }
    result
}

impl fmt::Debug for MetadataSnapshot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Mirror Java's `toString()`.
        write!(
            f,
            "MetadataSnapshot{{clusterId='{}', nodes={:?}, partitions={:?}, controller={:?}}}",
            self.cluster_id.as_deref().unwrap_or(""),
            self.nodes,
            self.metadata_by_partition.values().collect::<Vec<_>>(),
            self.controller,
        )
    }
}

impl fmt::Display for MetadataSnapshot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, f)
    }
}

#[cfg(test)]
mod tests {
    //! Translation of `org.apache.kafka.clients.MetadataSnapshotTest`.

    use super::*;
    use crate::common::protocol::Errors;

    /// Java: `testMissingLeaderEndpoint`.
    #[test]
    fn missing_leader_endpoint() {
        let topic_partition = TopicPartition::new("topic".to_owned(), 0);

        let partition_metadata = PartitionMetadata::new(
            Errors::None,
            topic_partition.clone(),
            Some(5),
            Some(10),
            vec![5, 6, 7],
            vec![5, 6, 7],
            Vec::new(),
        );

        let mut nodes_by_id: HashMap<i32, Node> = HashMap::new();
        nodes_by_id.insert(6, Node::new(6, "localhost".to_owned(), 2077));
        nodes_by_id.insert(7, Node::new(7, "localhost".to_owned(), 2078));
        nodes_by_id.insert(8, Node::new(8, "localhost".to_owned(), 2079));

        let cache = MetadataSnapshot::new(
            Some("clusterId".to_owned()),
            nodes_by_id.clone(),
            vec![partition_metadata],
            HashSet::new(),
            HashSet::new(),
            HashSet::new(),
            None,
            HashMap::new(),
        );

        let cluster = cache.cluster();
        assert!(cluster.leader_for(&topic_partition).is_none());

        let partition_info = cluster.partition(&topic_partition).expect("partition cached");
        let replicas: HashMap<i32, &Node> = partition_info.replicas().iter().map(|n| (n.id(), n)).collect();
        assert!(partition_info.leader().is_none());
        assert_eq!(replicas.len(), 3);
        assert!(replicas.get(&5).expect("replica 5").is_empty());
        assert_eq!(replicas.get(&6).map(|n| (*n).clone()), nodes_by_id.get(&6).cloned());
        assert_eq!(replicas.get(&7).map(|n| (*n).clone()), nodes_by_id.get(&7).cloned());
    }

    /// Java: `testMergeWithThatPreExistingPartitionIsRetainedPostMerge`.
    #[test]
    fn merge_with_that_pre_existing_partition_is_retained_post_merge() {
        let topic1 = "topic1";
        let topic1_partition = TopicPartition::new(topic1.to_owned(), 1);
        let partition_metadata1 = PartitionMetadata::new(
            Errors::None,
            topic1_partition.clone(),
            Some(5),
            Some(10),
            vec![5, 6, 7],
            vec![5, 6, 7],
            Vec::new(),
        );

        let mut nodes_by_id: HashMap<i32, Node> = HashMap::new();
        nodes_by_id.insert(6, Node::new(6, "localhost".to_owned(), 2077));
        nodes_by_id.insert(7, Node::new(7, "localhost".to_owned(), 2078));
        nodes_by_id.insert(8, Node::new(8, "localhost".to_owned(), 2079));

        let mut topics_ids: HashMap<String, Uuid> = HashMap::new();
        let topic1_id = Uuid::random();
        topics_ids.insert(topic1_partition.topic().to_owned(), topic1_id);

        let cache = MetadataSnapshot::new(
            Some("clusterId".to_owned()),
            nodes_by_id.clone(),
            vec![partition_metadata1],
            HashSet::new(),
            HashSet::new(),
            HashSet::new(),
            None,
            topics_ids,
        );

        let cluster = cache.cluster();
        assert_eq!(cluster.topics().count(), 1);
        assert_eq!(cluster.topic_id(topic1), topic1_id);
        assert_eq!(cluster.topic_name(topic1_id), Some(topic1));

        // Merge with a new partition for topic2.
        let topic2 = "topic2";
        let topic2_partition = TopicPartition::new(topic2.to_owned(), 2);
        let partition_metadata2 = PartitionMetadata::new(
            Errors::None,
            topic2_partition.clone(),
            Some(5),
            Some(10),
            vec![5, 6, 7],
            vec![5, 6, 7],
            Vec::new(),
        );
        let mut topics_ids2: HashMap<String, Uuid> = HashMap::new();
        let topic2_id = Uuid::random();
        topics_ids2.insert(topic2_partition.topic().to_owned(), topic2_id);
        let cache = cache.merge_with(
            Some("clusterId".to_owned()),
            nodes_by_id.clone(),
            vec![partition_metadata2],
            HashSet::new(),
            HashSet::new(),
            HashSet::new(),
            None,
            topics_ids2,
            |_, _| true,
        );
        let cluster = cache.cluster();

        assert_eq!(cluster.topics().count(), 2);
        assert_eq!(cluster.topic_id(topic1), topic1_id);
        assert_eq!(cluster.topic_name(topic1_id), Some(topic1));
        assert_eq!(cluster.topic_id(topic2), topic2_id);
        assert_eq!(cluster.topic_name(topic2_id), Some(topic2));
    }

    /// Java: `testTopicNamesCacheBuiltFromTopicIds`.
    #[test]
    fn topic_names_cache_built_from_topic_ids() {
        let mut topic_ids: HashMap<String, Uuid> = HashMap::new();
        topic_ids.insert("topic1".to_owned(), Uuid::random());
        topic_ids.insert("topic2".to_owned(), Uuid::random());

        let mut nodes = HashMap::new();
        nodes.insert(6, Node::new(6, "localhost".to_owned(), 2077));
        let cache = MetadataSnapshot::new(
            Some("clusterId".to_owned()),
            nodes,
            Vec::new(),
            HashSet::new(),
            HashSet::new(),
            HashSet::new(),
            None,
            topic_ids.clone(),
        );

        let expected_names_cache: HashMap<Uuid, String> = topic_ids.into_iter().map(|(k, v)| (v, k)).collect();
        assert_eq!(cache.topic_names(), &expected_names_cache);
    }

    /// Java: `testEmptyTopicNamesCacheBuiltFromTopicIds`.
    #[test]
    fn empty_topic_names_cache_built_from_topic_ids() {
        let topic_ids: HashMap<String, Uuid> = HashMap::new();

        let mut nodes = HashMap::new();
        nodes.insert(6, Node::new(6, "localhost".to_owned(), 2077));
        let cache = MetadataSnapshot::new(
            Some("clusterId".to_owned()),
            nodes,
            Vec::new(),
            HashSet::new(),
            HashSet::new(),
            HashSet::new(),
            None,
            topic_ids,
        );
        assert!(cache.topic_names().is_empty());
    }

    /// Java: `testLeaderEpochFor`.
    #[test]
    fn leader_epoch_for() {
        let topic_partition1 = TopicPartition::new("topic".to_owned(), 0);
        let partition_metadata1 = PartitionMetadata::new(
            Errors::None,
            topic_partition1.clone(),
            Some(5),
            Some(10),
            vec![5, 6, 7],
            vec![5, 6, 7],
            Vec::new(),
        );

        let topic_partition2 = TopicPartition::new("topic".to_owned(), 1);
        let partition_metadata2 = PartitionMetadata::new(
            Errors::None,
            topic_partition2.clone(),
            Some(5),
            None, // unknown leader epoch
            vec![5, 6, 7],
            vec![5, 6, 7],
            Vec::new(),
        );

        let mut nodes_by_id = HashMap::new();
        nodes_by_id.insert(5, Node::new(5, "localhost".to_owned(), 2077));
        nodes_by_id.insert(6, Node::new(6, "localhost".to_owned(), 2078));
        nodes_by_id.insert(7, Node::new(7, "localhost".to_owned(), 2079));

        let cache = MetadataSnapshot::new(
            Some("clusterId".to_owned()),
            nodes_by_id,
            vec![partition_metadata1, partition_metadata2],
            HashSet::new(),
            HashSet::new(),
            HashSet::new(),
            None,
            HashMap::new(),
        );

        assert_eq!(cache.leader_epoch_for(&topic_partition1), Some(10));
        assert_eq!(cache.leader_epoch_for(&topic_partition2), None);
        assert_eq!(
            cache.leader_epoch_for(&TopicPartition::new("topic_missing".to_owned(), 0)),
            None
        );
    }
}
