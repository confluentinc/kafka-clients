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

//! Per-partition state in the MetadataResponse.

use std::fmt;

use super::Node;

/// This is used to describe per-partition state in the MetadataResponse.
#[derive(Clone, Debug)]
pub struct PartitionInfo {
    topic: String,
    partition: i32,
    leader: Option<Node>,
    replicas: Vec<Node>,
    in_sync_replicas: Vec<Node>,
    offline_replicas: Vec<Node>,
}

impl PartitionInfo {
    /// Creates a new `PartitionInfo` with no offline replicas.
    pub fn new(
        topic: String,
        partition: i32,
        leader: Option<Node>,
        replicas: Vec<Node>,
        in_sync_replicas: Vec<Node>,
    ) -> Self {
        Self::with_offline_replicas(topic, partition, leader, replicas, in_sync_replicas, vec![])
    }

    /// Creates a new `PartitionInfo` with offline replicas.
    pub fn with_offline_replicas(
        topic: String,
        partition: i32,
        leader: Option<Node>,
        replicas: Vec<Node>,
        in_sync_replicas: Vec<Node>,
        offline_replicas: Vec<Node>,
    ) -> Self {
        Self { topic, partition, leader, replicas, in_sync_replicas, offline_replicas }
    }

    /// The topic name.
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// The partition id.
    pub fn partition(&self) -> i32 {
        self.partition
    }

    /// The node currently acting as a leader for this partition, or `None` if there is no leader.
    pub fn leader(&self) -> Option<&Node> {
        self.leader.as_ref()
    }

    /// The complete set of replicas for this partition regardless of whether they are alive or up-to-date.
    pub fn replicas(&self) -> &[Node] {
        &self.replicas
    }

    /// The subset of the replicas that are in sync, that is caught-up to the leader and ready to
    /// take over as leader if the leader should fail.
    pub fn in_sync_replicas(&self) -> &[Node] {
        &self.in_sync_replicas
    }

    /// The subset of the replicas that are offline.
    pub fn offline_replicas(&self) -> &[Node] {
        &self.offline_replicas
    }
}

impl PartialEq for PartitionInfo {
    fn eq(&self, other: &Self) -> bool {
        self.topic == other.topic
            && self.partition == other.partition
            && self.leader == other.leader
            && self.replicas == other.replicas
            && self.in_sync_replicas == other.in_sync_replicas
            && self.offline_replicas == other.offline_replicas
    }
}

impl Eq for PartitionInfo {}

impl std::hash::Hash for PartitionInfo {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.topic.hash(state);
        self.partition.hash(state);
        self.leader.hash(state);
        self.replicas.hash(state);
        self.in_sync_replicas.hash(state);
        self.offline_replicas.hash(state);
    }
}

/// Format node ids from a slice for display.
fn format_node_ids(nodes: &[Node]) -> String {
    let mut b = String::from("[");
    for (i, node) in nodes.iter().enumerate() {
        b.push_str(node.id_string());
        if i < nodes.len() - 1 {
            b.push(',');
        }
    }
    b.push(']');
    b
}

impl fmt::Display for PartitionInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let leader_str = match &self.leader {
            Some(l) => l.id_string().to_string(),
            None => "none".to_string(),
        };
        write!(
            f,
            "Partition(topic = {}, partition = {}, leader = {}, replicas = {}, isr = {}, offlineReplicas = {})",
            self.topic,
            self.partition,
            leader_str,
            format_node_ids(&self.replicas),
            format_node_ids(&self.in_sync_replicas),
            format_node_ids(&self.offline_replicas),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_node(id: i32) -> Node {
        Node::new(id, format!("host{id}"), 9092)
    }

    #[test]
    fn test_partition_info_creation() {
        let leader = make_node(0);
        let replicas = vec![make_node(0), make_node(1), make_node(2)];
        let isr = vec![make_node(0), make_node(1)];

        let pi = PartitionInfo::new("test".to_string(), 0, Some(leader), replicas, isr);
        assert_eq!(pi.topic(), "test");
        assert_eq!(pi.partition(), 0);
        assert_eq!(pi.leader().unwrap().id(), 0);
        assert_eq!(pi.replicas().len(), 3);
        assert_eq!(pi.in_sync_replicas().len(), 2);
        assert!(pi.offline_replicas().is_empty());
    }

    #[test]
    fn test_partition_info_no_leader() {
        let pi = PartitionInfo::new("test".to_string(), 0, None, vec![], vec![]);
        assert!(pi.leader().is_none());
    }

    #[test]
    fn test_partition_info_display() {
        let leader = make_node(0);
        let replicas = vec![make_node(0), make_node(1)];
        let isr = vec![make_node(0)];
        let pi = PartitionInfo::new("test".to_string(), 0, Some(leader), replicas, isr);
        let display = pi.to_string();
        assert!(display.contains("topic = test"));
        assert!(display.contains("partition = 0"));
        assert!(display.contains("leader = 0"));
    }
}
