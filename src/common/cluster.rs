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

//! Translation of `org.apache.kafka.common.Cluster`.

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::net::SocketAddr;
use std::sync::{Arc, OnceLock};

use rand::seq::SliceRandom;

use crate::common::{ClusterResource, Node, PartitionInfo, TopicPartition, Uuid};

/// An immutable representation of a subset of the nodes, topics, and
/// partitions in the Kafka cluster.
#[derive(Clone, Debug)]
pub struct Cluster {
    is_bootstrap_configured: bool,
    nodes: Vec<Node>,
    unauthorized_topics: HashSet<Arc<str>>,
    invalid_topics: HashSet<Arc<str>>,
    internal_topics: HashSet<Arc<str>>,
    controller: Option<Node>,
    partitions_by_topic_partition: HashMap<TopicPartition, PartitionInfo>,
    partitions_by_topic: HashMap<Arc<str>, Vec<PartitionInfo>>,
    available_partitions_by_topic: HashMap<Arc<str>, Vec<PartitionInfo>>,
    partitions_by_node: HashMap<i32, Vec<PartitionInfo>>,
    nodes_by_id: HashMap<i32, Node>,
    cluster_resource: ClusterResource,
    topic_ids: HashMap<Arc<str>, Uuid>,
    topic_names: HashMap<Uuid, Arc<str>>,
}

impl Cluster {
    /// Create a new cluster with the given id, nodes, and partitions.
    /// Equivalent to Java's 5-arg `Cluster(clusterId, nodes, partitions,
    /// unauthorizedTopics, internalTopics)`.
    pub fn new(
        cluster_id: Option<String>,
        nodes: Vec<Node>,
        partitions: Vec<PartitionInfo>,
        unauthorized_topics: HashSet<String>,
        internal_topics: HashSet<String>,
    ) -> Self {
        Self::build(
            cluster_id,
            false,
            nodes,
            partitions,
            unauthorized_topics,
            HashSet::new(),
            internal_topics,
            None,
            HashMap::new(),
        )
    }

    /// Equivalent to Java's 6-arg constructor with controller.
    pub fn new_with_controller(
        cluster_id: Option<String>,
        nodes: Vec<Node>,
        partitions: Vec<PartitionInfo>,
        unauthorized_topics: HashSet<String>,
        internal_topics: HashSet<String>,
        controller: Option<Node>,
    ) -> Self {
        Self::build(
            cluster_id,
            false,
            nodes,
            partitions,
            unauthorized_topics,
            HashSet::new(),
            internal_topics,
            controller,
            HashMap::new(),
        )
    }

    /// Equivalent to Java's 7-arg constructor (adds invalid topics).
    pub fn new_with_invalid(
        cluster_id: Option<String>,
        nodes: Vec<Node>,
        partitions: Vec<PartitionInfo>,
        unauthorized_topics: HashSet<String>,
        invalid_topics: HashSet<String>,
        internal_topics: HashSet<String>,
        controller: Option<Node>,
    ) -> Self {
        Self::build(
            cluster_id,
            false,
            nodes,
            partitions,
            unauthorized_topics,
            invalid_topics,
            internal_topics,
            controller,
            HashMap::new(),
        )
    }

    /// Equivalent to Java's 8-arg constructor (adds topic ids).
    #[allow(clippy::too_many_arguments)] // Mirrors Java's 8-arg constructor.
    pub fn new_with_topic_ids(
        cluster_id: Option<String>,
        nodes: Vec<Node>,
        partitions: Vec<PartitionInfo>,
        unauthorized_topics: HashSet<String>,
        invalid_topics: HashSet<String>,
        internal_topics: HashSet<String>,
        controller: Option<Node>,
        topic_ids: HashMap<String, Uuid>,
    ) -> Self {
        Self::build(
            cluster_id,
            false,
            nodes,
            partitions,
            unauthorized_topics,
            invalid_topics,
            internal_topics,
            controller,
            topic_ids,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn build(
        cluster_id: Option<String>,
        is_bootstrap_configured: bool,
        nodes: Vec<Node>,
        partitions: Vec<PartitionInfo>,
        unauthorized_topics: HashSet<String>,
        invalid_topics: HashSet<String>,
        internal_topics: HashSet<String>,
        controller: Option<Node>,
        topic_ids: HashMap<String, Uuid>,
    ) -> Self {
        // Make a randomized copy of the nodes — matches Java's
        // `Collections.shuffle(copy)` so iteration order is randomized.
        let mut shuffled_nodes = nodes.clone();
        let mut rng = rand::rng();
        shuffled_nodes.shuffle(&mut rng);

        // Index nodes by id; pre-create the partitions-by-node map with an
        // empty vec for every node so we can append efficiently below.
        let mut nodes_by_id: HashMap<i32, Node> = HashMap::with_capacity(shuffled_nodes.len());
        let mut partitions_by_node: HashMap<i32, Vec<PartitionInfo>> = HashMap::with_capacity(shuffled_nodes.len());
        for node in &shuffled_nodes {
            nodes_by_id.insert(node.id(), node.clone());
            partitions_by_node.insert(node.id(), Vec::new());
        }

        // Index partitions by topic, topic+partition, and node. The
        // partitions_by_topic_partition map shares the same `Arc<str>` topic
        // allocation as the source `PartitionInfo`, so growing the maps is
        // refcount bumps, not allocations.
        let mut partitions_by_topic_partition: HashMap<TopicPartition, PartitionInfo> =
            HashMap::with_capacity(partitions.len());
        let mut partitions_by_topic: HashMap<Arc<str>, Vec<PartitionInfo>> = HashMap::new();

        for p in &partitions {
            let tp = TopicPartition::new(p.topic_arc().clone(), p.partition());
            partitions_by_topic_partition.insert(tp, p.clone());
            partitions_by_topic.entry(p.topic_arc().clone()).or_default().push(p.clone());

            // The leader may not be known.
            if let Some(leader) = p.leader() {
                if leader.is_empty() {
                    continue;
                }
                // If known, its node info should be available.
                let entry = partitions_by_node
                    .get_mut(&leader.id())
                    .unwrap_or_else(|| panic!("partition leader id {} not found in nodes-by-id map", leader.id()));
                entry.push(p.clone());
            }
        }

        // Build available_partitions_by_topic: copy each per-topic list,
        // skipping entries with no leader.
        let mut available_partitions_by_topic: HashMap<Arc<str>, Vec<PartitionInfo>> =
            HashMap::with_capacity(partitions_by_topic.len());
        for (topic, parts) in &partitions_by_topic {
            let any_unavailable = parts.iter().any(|p| p.leader().is_none());
            let avail = if any_unavailable {
                parts.iter().filter(|p| p.leader().is_some()).cloned().collect()
            } else {
                parts.clone()
            };
            available_partitions_by_topic.insert(topic.clone(), avail);
        }

        // Topic-id maps. Convert input `HashMap<String, Uuid>` to
        // `HashMap<Arc<str>, Uuid>` so the keys can be shared.
        let topic_ids_arc: HashMap<Arc<str>, Uuid> =
            topic_ids.into_iter().map(|(k, v)| (Arc::<str>::from(k), v)).collect();
        let topic_names: HashMap<Uuid, Arc<str>> = topic_ids_arc.iter().map(|(k, v)| (*v, k.clone())).collect();

        let unauthorized_topics: HashSet<Arc<str>> = unauthorized_topics.into_iter().map(Arc::<str>::from).collect();
        let invalid_topics: HashSet<Arc<str>> = invalid_topics.into_iter().map(Arc::<str>::from).collect();
        let internal_topics: HashSet<Arc<str>> = internal_topics.into_iter().map(Arc::<str>::from).collect();

        Cluster {
            is_bootstrap_configured,
            nodes: shuffled_nodes,
            unauthorized_topics,
            invalid_topics,
            internal_topics,
            controller,
            partitions_by_topic_partition,
            partitions_by_topic,
            available_partitions_by_topic,
            partitions_by_node,
            nodes_by_id,
            cluster_resource: ClusterResource::new(cluster_id),
            topic_ids: topic_ids_arc,
            topic_names,
        }
    }

    /// Create an empty cluster instance with no nodes and no
    /// topic-partitions. Returns a cached singleton.
    pub fn empty() -> &'static Cluster {
        static EMPTY: OnceLock<Cluster> = OnceLock::new();
        EMPTY.get_or_init(|| {
            Cluster::new_with_controller(None, Vec::new(), Vec::new(), HashSet::new(), HashSet::new(), None)
        })
    }

    /// Create a "bootstrap" cluster from the given list of host/ports.
    pub fn bootstrap(addresses: &[SocketAddr]) -> Cluster {
        let mut nodes = Vec::with_capacity(addresses.len());
        let mut node_id: i32 = -1;
        for addr in addresses {
            // Java uses `getHostString()` which returns the literal host
            // (no reverse DNS). `SocketAddr::ip().to_string()` gives the
            // numeric form; for parity with Java we pass through whatever
            // textual host/IP was used to build the SocketAddr. Callers
            // building `SocketAddr` from a hostname have already resolved.
            // For unresolved hostnames see `Cluster::bootstrap_with_hosts`.
            let host = addr.ip().to_string();
            nodes.push(Node::new(node_id, host, addr.port() as i32));
            node_id -= 1;
        }
        Cluster::build(
            None,
            true,
            nodes,
            Vec::new(),
            HashSet::new(),
            HashSet::new(),
            HashSet::new(),
            None,
            HashMap::new(),
        )
    }

    /// Create a "bootstrap" cluster from `(host, port)` pairs. This is the
    /// closer analogue of Java's `Cluster.bootstrap(List<InetSocketAddress>)`
    /// because Java's `InetSocketAddress.getHostString()` returns the
    /// original textual host (not the resolved IP), preserving DNS names
    /// for later lookup. Use this when you want to defer DNS resolution.
    pub fn bootstrap_with_hosts(hosts: &[(String, u16)]) -> Cluster {
        let mut nodes = Vec::with_capacity(hosts.len());
        let mut node_id: i32 = -1;
        for (host, port) in hosts {
            nodes.push(Node::new(node_id, host.clone(), *port as i32));
            node_id -= 1;
        }
        Cluster::build(
            None,
            true,
            nodes,
            Vec::new(),
            HashSet::new(),
            HashSet::new(),
            HashSet::new(),
            None,
            HashMap::new(),
        )
    }

    /// Return a copy of this cluster combined with `partitions`.
    pub fn with_partitions(&self, partitions: HashMap<TopicPartition, PartitionInfo>) -> Cluster {
        let mut combined: HashMap<TopicPartition, PartitionInfo> = self.partitions_by_topic_partition.clone();
        for (k, v) in partitions {
            combined.insert(k, v);
        }
        let unauthorized: HashSet<String> = self.unauthorized_topics.iter().map(|s| s.to_string()).collect();
        let invalid: HashSet<String> = self.invalid_topics.iter().map(|s| s.to_string()).collect();
        let internal: HashSet<String> = self.internal_topics.iter().map(|s| s.to_string()).collect();
        let topic_ids: HashMap<String, Uuid> = self.topic_ids.iter().map(|(k, v)| (k.to_string(), *v)).collect();
        let nodes = self.nodes.clone();
        Cluster::new_with_topic_ids(
            self.cluster_resource.cluster_id().map(str::to_owned),
            nodes,
            combined.into_values().collect(),
            unauthorized,
            invalid,
            internal,
            self.controller.clone(),
            topic_ids,
        )
    }

    /// The known set of nodes.
    pub fn nodes(&self) -> &[Node] {
        &self.nodes
    }

    /// Look up a node by id.
    pub fn node_by_id(&self, id: i32) -> Option<&Node> {
        self.nodes_by_id.get(&id)
    }

    /// Get the node by node id if the replica for the given partition is
    /// online — i.e. the node exists, the partition exists, the node is in
    /// the partition's replica list, and the node is not in the offline
    /// replica list.
    pub fn node_if_online(&self, partition: &TopicPartition, id: i32) -> Option<&Node> {
        let node = self.node_by_id(id)?;
        let info = self.partition(partition)?;
        let in_replicas = info.replicas().iter().any(|n| n == node);
        let in_offline = info.offline_replicas().iter().any(|n| n == node);
        if in_replicas && !in_offline { Some(node) } else { None }
    }

    /// The current leader for the given topic-partition, or `None` if
    /// there is no current leader.
    pub fn leader_for(&self, topic_partition: &TopicPartition) -> Option<&Node> {
        self.partitions_by_topic_partition.get(topic_partition)?.leader()
    }

    /// Metadata for the specified partition, or `None` if not known.
    pub fn partition(&self, topic_partition: &TopicPartition) -> Option<&PartitionInfo> {
        self.partitions_by_topic_partition.get(topic_partition)
    }

    /// List of partitions for the topic. Returns an empty slice if no
    /// metadata is known.
    pub fn partitions_for_topic(&self, topic: &str) -> &[PartitionInfo] {
        self.partitions_by_topic.get(topic).map(|v| v.as_slice()).unwrap_or(&[])
    }

    /// Number of partitions for the given topic, or `None` if no metadata
    /// is known.
    pub fn partition_count_for_topic(&self, topic: &str) -> Option<usize> {
        self.partitions_by_topic.get(topic).map(Vec::len)
    }

    /// List of available partitions for the topic (those with a known
    /// leader).
    pub fn available_partitions_for_topic(&self, topic: &str) -> &[PartitionInfo] {
        self.available_partitions_by_topic
            .get(topic)
            .map(|v| v.as_slice())
            .unwrap_or(&[])
    }

    /// List of partitions whose leader is the given node.
    pub fn partitions_for_node(&self, node_id: i32) -> &[PartitionInfo] {
        self.partitions_by_node.get(&node_id).map(|v| v.as_slice()).unwrap_or(&[])
    }

    /// All known topics.
    pub fn topics(&self) -> impl Iterator<Item = &str> {
        self.partitions_by_topic.keys().map(|k| k.as_ref())
    }

    /// Unauthorized topics (read-only borrow).
    pub fn unauthorized_topics(&self) -> impl Iterator<Item = &str> {
        self.unauthorized_topics.iter().map(|s| s.as_ref())
    }

    /// Invalid topics (read-only borrow).
    pub fn invalid_topics(&self) -> impl Iterator<Item = &str> {
        self.invalid_topics.iter().map(|s| s.as_ref())
    }

    /// Internal topics (read-only borrow).
    pub fn internal_topics(&self) -> impl Iterator<Item = &str> {
        self.internal_topics.iter().map(|s| s.as_ref())
    }

    /// Whether this cluster was constructed by [`Cluster::bootstrap`].
    pub fn is_bootstrap_configured(&self) -> bool {
        self.is_bootstrap_configured
    }

    /// The cluster resource (cluster id).
    pub fn cluster_resource(&self) -> &ClusterResource {
        &self.cluster_resource
    }

    /// The controller node, if one is known.
    pub fn controller(&self) -> Option<&Node> {
        self.controller.as_ref()
    }

    /// All known topic ids.
    pub fn topic_ids(&self) -> impl Iterator<Item = Uuid> + '_ {
        self.topic_ids.values().copied()
    }

    /// Topic id for the given topic name. Returns [`Uuid::ZERO_UUID`] if
    /// not known (matching Java's `getOrDefault(topic, ZERO_UUID)`).
    pub fn topic_id(&self, topic: &str) -> Uuid {
        self.topic_ids.get(topic).copied().unwrap_or(crate::common::uuid::ZERO_UUID)
    }

    /// Topic name for the given topic id, or `None` if not known.
    pub fn topic_name(&self, topic_id: Uuid) -> Option<&str> {
        self.topic_names.get(&topic_id).map(|s| s.as_ref())
    }
}

impl PartialEq for Cluster {
    fn eq(&self, other: &Self) -> bool {
        // Java equals compares: isBootstrapConfigured, nodes, unauthorizedTopics,
        // invalidTopics, internalTopics, controller, partitionsByTopicPartition,
        // clusterResource, topicIds. Note: `nodes` is a List in Java and the
        // shuffle randomizes order — Java's `List.equals` is order-dependent
        // so equality of two freshly-constructed Clusters is in principle
        // sensitive to the shuffle outcome. ClusterTest.testEquals uses a
        // single node, dodging the issue. We mirror Java's order-dependent
        // equality.
        self.is_bootstrap_configured == other.is_bootstrap_configured
            && self.nodes == other.nodes
            && self.unauthorized_topics == other.unauthorized_topics
            && self.invalid_topics == other.invalid_topics
            && self.internal_topics == other.internal_topics
            && self.controller == other.controller
            && self.partitions_by_topic_partition == other.partitions_by_topic_partition
            && self.cluster_resource == other.cluster_resource
            && self.topic_ids == other.topic_ids
    }
}

impl Eq for Cluster {}

impl fmt::Display for Cluster {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let cluster_id = match self.cluster_resource.cluster_id() {
            Some(id) => id.to_string(),
            None => "null".to_string(),
        };
        let controller = match &self.controller {
            Some(c) => c.to_string(),
            None => "null".to_string(),
        };
        let nodes_str = self.nodes.iter().map(Node::to_string).collect::<Vec<_>>().join(", ");
        let parts_str = self
            .partitions_by_topic_partition
            .values()
            .map(PartitionInfo::to_string)
            .collect::<Vec<_>>()
            .join(", ");
        write!(
            f,
            "Cluster(id = {cluster_id}, nodes = [{nodes_str}], partitions = [{parts_str}], controller = {controller})"
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn nodes_array() -> [Node; 4] {
        [
            Node::new(0, "localhost".to_string(), 99),
            Node::new(1, "localhost".to_string(), 100),
            Node::new(2, "localhost".to_string(), 101),
            Node::new(11, "localhost".to_string(), 102),
        ]
    }

    const TOPIC_A: &str = "topicA";
    const TOPIC_B: &str = "topicB";
    const TOPIC_C: &str = "topicC";
    const TOPIC_D: &str = "topicD";
    const TOPIC_E: &str = "topicE";

    #[test]
    fn test_bootstrap() {
        // Translation of ClusterTest.testBootstrap. We use the host-form
        // bootstrap (`bootstrap_with_hosts`) so the textual hostnames are
        // preserved — matching Java's `InetSocketAddress.getHostString()`.
        let ip = "140.211.11.105";
        let host = "www.example.com";
        let cluster = Cluster::bootstrap_with_hosts(&[(ip.to_string(), 9002), (host.to_string(), 9002)]);
        let mut actual: HashSet<String> = HashSet::new();
        for n in cluster.nodes() {
            actual.insert(n.host().to_string());
        }
        let expected: HashSet<String> = [ip.to_string(), host.to_string()].into_iter().collect();
        assert_eq!(actual, expected);
        assert!(cluster.is_bootstrap_configured());
    }

    #[test]
    fn test_returns_immutable_views() {
        // Translation of ClusterTest.testReturnUnmodifiableCollections.
        // In Rust, the immutability is statically enforced by `&` borrows
        // (the public API exposes `&[..]` and `impl Iterator<Item = &str>`
        // which cannot be mutated by callers). The Java test asserts
        // `UnsupportedOperationException` on `.add(...)` calls which has
        // no analogue. Instead we verify the data is reachable and
        // identical under repeated reads.
        let nodes = nodes_array();
        let all_partitions = vec![
            PartitionInfo::new(TOPIC_A, 0, Some(nodes[0].clone()), nodes.to_vec(), nodes.to_vec()),
            PartitionInfo::new(TOPIC_A, 1, None, nodes.to_vec(), nodes.to_vec()),
            PartitionInfo::new(TOPIC_A, 2, Some(nodes[2].clone()), nodes.to_vec(), nodes.to_vec()),
            PartitionInfo::new(TOPIC_B, 0, None, nodes.to_vec(), nodes.to_vec()),
            PartitionInfo::new(TOPIC_B, 1, Some(nodes[0].clone()), nodes.to_vec(), nodes.to_vec()),
            PartitionInfo::new(TOPIC_C, 0, None, nodes.to_vec(), nodes.to_vec()),
            PartitionInfo::new(TOPIC_D, 0, Some(nodes[1].clone()), nodes.to_vec(), nodes.to_vec()),
            PartitionInfo::new(TOPIC_E, 0, Some(nodes[0].clone()), nodes.to_vec(), nodes.to_vec()),
        ];
        let mut unauthorized = HashSet::new();
        unauthorized.insert(TOPIC_C.to_string());
        let mut invalid = HashSet::new();
        invalid.insert(TOPIC_D.to_string());
        let mut internal = HashSet::new();
        internal.insert(TOPIC_E.to_string());

        let cluster = Cluster::new_with_invalid(
            Some("clusterId".to_string()),
            nodes.to_vec(),
            all_partitions,
            unauthorized,
            invalid,
            internal,
            Some(nodes[1].clone()),
        );

        let invalids: HashSet<&str> = cluster.invalid_topics().collect();
        assert_eq!(invalids, [TOPIC_D].into_iter().collect());
        let internals: HashSet<&str> = cluster.internal_topics().collect();
        assert_eq!(internals, [TOPIC_E].into_iter().collect());
        let unauthorizeds: HashSet<&str> = cluster.unauthorized_topics().collect();
        assert_eq!(unauthorizeds, [TOPIC_C].into_iter().collect());

        let topics_iter: HashSet<&str> = cluster.topics().collect();
        assert!(topics_iter.contains(TOPIC_A));
        assert!(topics_iter.contains(TOPIC_B));

        // partitionsForTopic(TOPIC_A) returns 3 partitions, in some order.
        assert_eq!(cluster.partitions_for_topic(TOPIC_A).len(), 3);
        // availablePartitionsForTopic(TOPIC_B) returns the one with a
        // known leader.
        assert_eq!(cluster.available_partitions_for_topic(TOPIC_B).len(), 1);
        // partitionsForNode(NODES[1].id() == 1) — only TOPIC_D-0 has node 1
        // as leader.
        assert_eq!(cluster.partitions_for_node(nodes[1].id()).len(), 1);
    }

    #[test]
    fn test_not_equals() {
        // Translation of ClusterTest.testNotEquals.
        let cluster_id_1 = Some("clusterId1".to_string());
        let cluster_id_2 = Some("clusterId2".to_string());
        let node0 = Node::new(0, "host0".to_string(), 100);
        let node1 = Node::new(1, "host1".to_string(), 100);
        let partitions_1 = vec![PartitionInfo::new(
            "topic1",
            0,
            Some(node0.clone()),
            vec![node0.clone(), node1.clone()],
            vec![node0.clone()],
        )];
        let partitions_2 = vec![PartitionInfo::new(
            "topic2",
            0,
            Some(node0.clone()),
            vec![node1.clone(), node0.clone()],
            vec![node1.clone()],
        )];
        let unauthorized_1: HashSet<String> = ["topic1".to_string()].into_iter().collect();
        let unauthorized_2: HashSet<String> = ["topic2".to_string()].into_iter().collect();
        let invalid_1: HashSet<String> = ["topic1".to_string()].into_iter().collect();
        let invalid_2: HashSet<String> = ["topic2".to_string()].into_iter().collect();
        let internal_1: HashSet<String> = ["topic3".to_string()].into_iter().collect();
        let internal_2: HashSet<String> = ["topic4".to_string()].into_iter().collect();
        let controller_1 = Node::new(2, "host2".to_string(), 100);
        let controller_2 = Node::new(3, "host3".to_string(), 100);
        let topic_ids_1: HashMap<String, Uuid> = [("topic1".to_string(), Uuid::random())].into_iter().collect();
        let topic_ids_2: HashMap<String, Uuid> = [("topic2".to_string(), Uuid::random())].into_iter().collect();

        let cluster1 = Cluster::new_with_topic_ids(
            cluster_id_1.clone(),
            vec![node0.clone()],
            partitions_1.clone(),
            unauthorized_1.clone(),
            invalid_1.clone(),
            internal_1.clone(),
            Some(controller_1.clone()),
            topic_ids_1.clone(),
        );
        let different_topic_ids = Cluster::new_with_topic_ids(
            cluster_id_1.clone(),
            vec![node0.clone()],
            partitions_1.clone(),
            unauthorized_1.clone(),
            invalid_1.clone(),
            internal_1.clone(),
            Some(controller_1.clone()),
            topic_ids_2,
        );
        let different_controller = Cluster::new_with_topic_ids(
            cluster_id_1.clone(),
            vec![node0.clone()],
            partitions_1.clone(),
            unauthorized_1.clone(),
            invalid_1.clone(),
            internal_1.clone(),
            Some(controller_2),
            topic_ids_1.clone(),
        );
        let different_internal = Cluster::new_with_topic_ids(
            cluster_id_1.clone(),
            vec![node0.clone()],
            partitions_1.clone(),
            unauthorized_1.clone(),
            invalid_1.clone(),
            internal_2,
            Some(controller_1.clone()),
            topic_ids_1.clone(),
        );
        let different_invalid = Cluster::new_with_topic_ids(
            cluster_id_1.clone(),
            vec![node0.clone()],
            partitions_1.clone(),
            unauthorized_1.clone(),
            invalid_2,
            internal_1.clone(),
            Some(controller_1.clone()),
            topic_ids_1.clone(),
        );
        let different_unauthorized = Cluster::new_with_topic_ids(
            cluster_id_1.clone(),
            vec![node0.clone()],
            partitions_1.clone(),
            unauthorized_2,
            invalid_1.clone(),
            internal_1.clone(),
            Some(controller_1.clone()),
            topic_ids_1.clone(),
        );
        let different_partitions = Cluster::new_with_topic_ids(
            cluster_id_1.clone(),
            vec![node0.clone()],
            partitions_2,
            unauthorized_1.clone(),
            invalid_1.clone(),
            internal_1.clone(),
            Some(controller_1.clone()),
            topic_ids_1.clone(),
        );
        // For "different nodes" we use 2 nodes — the shuffle is a no-op
        // for 1-element lists in `cluster1`, but for 2 elements it can
        // produce either order. We add a partition leader so that the
        // partitions-by-node index has both entries; List equality is
        // order-dependent so this test is order-flaky in Java for >=2
        // nodes, but the comparison here is against `cluster1` which has
        // a different *length* anyway — so we're safe.
        let different_nodes = Cluster::new_with_topic_ids(
            cluster_id_1.clone(),
            vec![node0.clone(), node1.clone()],
            partitions_1.clone(),
            unauthorized_1.clone(),
            invalid_1.clone(),
            internal_1.clone(),
            Some(controller_1.clone()),
            topic_ids_1.clone(),
        );
        let different_cluster_id = Cluster::new_with_topic_ids(
            cluster_id_2,
            vec![node0.clone()],
            partitions_1.clone(),
            unauthorized_1.clone(),
            invalid_1.clone(),
            internal_1.clone(),
            Some(controller_1.clone()),
            topic_ids_1.clone(),
        );

        assert_ne!(cluster1, different_topic_ids);
        assert_ne!(cluster1, different_controller);
        assert_ne!(cluster1, different_internal);
        assert_ne!(cluster1, different_invalid);
        assert_ne!(cluster1, different_unauthorized);
        assert_ne!(cluster1, different_partitions);
        assert_ne!(cluster1, different_nodes);
        assert_ne!(cluster1, different_cluster_id);
    }

    #[test]
    fn test_equals() {
        // Translation of ClusterTest.testEquals.
        let cluster_id = Some("clusterId1".to_string());
        let node1 = Node::new(1, "host0".to_string(), 100);
        let node1_dup = Node::new(1, "host0".to_string(), 100);
        let topic_id_1 = Uuid::random();
        let partitions = vec![PartitionInfo::new(
            "topic1",
            0,
            Some(node1.clone()),
            vec![node1.clone()],
            vec![node1.clone()],
        )];
        let partitions_dup = vec![PartitionInfo::new(
            "topic1",
            0,
            Some(node1_dup.clone()),
            vec![node1_dup.clone()],
            vec![node1_dup.clone()],
        )];
        let unauthorized: HashSet<String> = ["topic1".to_string()].into_iter().collect();
        let invalid: HashSet<String> = ["topic1".to_string()].into_iter().collect();
        let internal: HashSet<String> = ["topic3".to_string()].into_iter().collect();
        let controller = Node::new(2, "host0".to_string(), 100);
        let controller_dup = Node::new(2, "host0".to_string(), 100);
        let topic_ids: HashMap<String, Uuid> = [("topic1".to_string(), topic_id_1)].into_iter().collect();
        let topic_ids_dup: HashMap<String, Uuid> = [("topic1".to_string(), topic_id_1)].into_iter().collect();

        let c1 = Cluster::new_with_topic_ids(
            cluster_id.clone(),
            vec![node1.clone()],
            partitions,
            unauthorized.clone(),
            invalid.clone(),
            internal.clone(),
            Some(controller),
            topic_ids,
        );
        let c1_dup = Cluster::new_with_topic_ids(
            cluster_id,
            vec![node1_dup],
            partitions_dup,
            unauthorized,
            invalid,
            internal,
            Some(controller_dup),
            topic_ids_dup,
        );
        assert_eq!(c1, c1_dup);
    }

    #[test]
    fn empty_is_cached() {
        let a: *const Cluster = Cluster::empty();
        let b: *const Cluster = Cluster::empty();
        assert_eq!(a, b);
        assert!(!Cluster::empty().is_bootstrap_configured());
    }

    #[test]
    fn topic_id_returns_zero_when_unknown() {
        let c = Cluster::empty();
        assert_eq!(c.topic_id("missing"), crate::common::uuid::ZERO_UUID);
        assert_eq!(c.topic_name(Uuid::random()), None);
    }

    #[test]
    fn partition_lookup_works() {
        let n0 = Node::new(0, "host".to_string(), 100);
        let parts = vec![PartitionInfo::new(
            "t",
            0,
            Some(n0.clone()),
            vec![n0.clone()],
            vec![n0.clone()],
        )];
        let cluster = Cluster::new_with_controller(None, vec![n0.clone()], parts, HashSet::new(), HashSet::new(), None);
        let tp = TopicPartition::new("t", 0);
        let info = cluster.partition(&tp).expect("partition exists");
        assert_eq!(info.partition(), 0);
        assert_eq!(info.topic(), "t");
        assert_eq!(cluster.leader_for(&tp), Some(&n0));
        assert_eq!(cluster.partition_count_for_topic("t"), Some(1));
        assert_eq!(cluster.partition_count_for_topic("missing"), None);
    }

    #[test]
    fn node_if_online_logic() {
        let n0 = Node::new(0, "h".to_string(), 100);
        let n1 = Node::new(1, "h".to_string(), 101);
        let parts = vec![PartitionInfo::new_with_offline(
            "t",
            0,
            Some(n0.clone()),
            vec![n0.clone(), n1.clone()],
            vec![n0.clone()],
            vec![n1.clone()],
        )];
        let cluster = Cluster::new_with_controller(
            None,
            vec![n0.clone(), n1.clone()],
            parts,
            HashSet::new(),
            HashSet::new(),
            None,
        );
        let tp = TopicPartition::new("t", 0);
        // n0 is in replicas and not in offline → online.
        assert_eq!(cluster.node_if_online(&tp, 0), Some(&n0));
        // n1 is in replicas but also offline → not online.
        assert_eq!(cluster.node_if_online(&tp, 1), None);
        // unknown id → None.
        assert_eq!(cluster.node_if_online(&tp, 999), None);
    }

    #[test]
    fn with_partitions_combines() {
        let n0 = Node::new(0, "h".to_string(), 100);
        let parts = vec![PartitionInfo::new(
            "t",
            0,
            Some(n0.clone()),
            vec![n0.clone()],
            vec![n0.clone()],
        )];
        let cluster = Cluster::new_with_controller(None, vec![n0.clone()], parts, HashSet::new(), HashSet::new(), None);
        let tp = TopicPartition::new("t", 1);
        let new_pi = PartitionInfo::new("t", 1, Some(n0.clone()), vec![n0.clone()], vec![n0.clone()]);
        let mut extra: HashMap<TopicPartition, PartitionInfo> = HashMap::new();
        extra.insert(tp.clone(), new_pi);
        let combined = cluster.with_partitions(extra);
        assert!(combined.partition(&tp).is_some());
        assert!(combined.partition(&TopicPartition::new("t", 0)).is_some());
    }
}
