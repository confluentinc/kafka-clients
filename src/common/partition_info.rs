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

//! Translation of `org.apache.kafka.common.PartitionInfo`.

use std::fmt;
use std::sync::Arc;

use crate::common::Node;

/// Per-partition state, as appears in a `MetadataResponse`.
///
/// Topic name is stored as `Arc<str>` so the `Cluster` indexes that
/// re-key by topic name (`partitionsByTopic`, `availablePartitionsByTopic`)
/// share the same allocation as the per-`TopicPartition` map. See
/// CLAUDE.md rule 11 (hot-path identifier interning).
///
/// `leader` is `Option<Node>` to match Java's nullable `leader` field —
/// the metadata response may report a partition without a current leader.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct PartitionInfo {
    topic: Arc<str>,
    partition: i32,
    leader: Option<Node>,
    replicas: Vec<Node>,
    in_sync_replicas: Vec<Node>,
    offline_replicas: Vec<Node>,
}

impl PartitionInfo {
    /// Create with no offline replicas — equivalent to Java's 5-argument
    /// constructor.
    pub fn new(
        topic: impl Into<Arc<str>>,
        partition: i32,
        leader: Option<Node>,
        replicas: Vec<Node>,
        in_sync_replicas: Vec<Node>,
    ) -> Self {
        Self::new_with_offline(topic, partition, leader, replicas, in_sync_replicas, Vec::new())
    }

    /// Create with all six fields — equivalent to Java's 6-argument
    /// constructor.
    pub fn new_with_offline(
        topic: impl Into<Arc<str>>,
        partition: i32,
        leader: Option<Node>,
        replicas: Vec<Node>,
        in_sync_replicas: Vec<Node>,
        offline_replicas: Vec<Node>,
    ) -> Self {
        Self {
            topic: topic.into(),
            partition,
            leader,
            replicas,
            in_sync_replicas,
            offline_replicas,
        }
    }

    /// The topic name.
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// Borrow the topic as the shared `Arc<str>`.
    pub fn topic_arc(&self) -> &Arc<str> {
        &self.topic
    }

    /// The partition id.
    pub fn partition(&self) -> i32 {
        self.partition
    }

    /// The node currently acting as leader for this partition, or `None`
    /// if there is no leader.
    pub fn leader(&self) -> Option<&Node> {
        self.leader.as_ref()
    }

    /// The complete set of replicas for this partition regardless of whether
    /// they are alive or up-to-date.
    pub fn replicas(&self) -> &[Node] {
        &self.replicas
    }

    /// The subset of the replicas that are in sync, that is caught-up to the
    /// leader and ready to take over as leader if the leader should fail.
    pub fn in_sync_replicas(&self) -> &[Node] {
        &self.in_sync_replicas
    }

    /// The subset of the replicas that are offline.
    pub fn offline_replicas(&self) -> &[Node] {
        &self.offline_replicas
    }
}

impl fmt::Display for PartitionInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Java: "Partition(topic = %s, partition = %d, leader = %s, replicas = %s, isr = %s, offlineReplicas = %s)"
        let leader = match &self.leader {
            Some(n) => n.id_string().to_string(),
            None => "none".to_string(),
        };
        write!(
            f,
            "Partition(topic = {}, partition = {}, leader = {}, replicas = {}, isr = {}, offlineReplicas = {})",
            self.topic,
            self.partition,
            leader,
            format_node_ids(&self.replicas),
            format_node_ids(&self.in_sync_replicas),
            format_node_ids(&self.offline_replicas)
        )
    }
}

/// Mirror of Java's private `formatNodeIds` helper.
fn format_node_ids(nodes: &[Node]) -> String {
    let mut s = String::from("[");
    for (i, n) in nodes.iter().enumerate() {
        s.push_str(n.id_string());
        if i + 1 < nodes.len() {
            s.push(',');
        }
    }
    s.push(']');
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn to_string_matches_java() {
        // Translation of PartitionInfoTest.testToString.
        let topic = "sample";
        let partition = 0;
        let leader = Node::new(0, "localhost".to_string(), 9092);
        let r1 = Node::new(1, "localhost".to_string(), 9093);
        let r2 = Node::new(2, "localhost".to_string(), 9094);
        let replicas = vec![leader.clone(), r1.clone(), r2.clone()];
        let isr = vec![leader.clone(), r1.clone()];
        let offline = vec![r2.clone()];

        let info = PartitionInfo::new_with_offline(topic, partition, Some(leader.clone()), replicas, isr, offline);

        let expected = format!(
            "Partition(topic = {}, partition = {}, leader = {}, replicas = {}, isr = {}, offlineReplicas = {})",
            topic,
            partition,
            leader.id_string(),
            "[0,1,2]",
            "[0,1]",
            "[2]"
        );
        assert_eq!(info.to_string(), expected);
    }

    #[test]
    fn equals_uses_all_fields() {
        let leader = Node::new(0, "h".to_string(), 9092);
        let a = PartitionInfo::new("t", 0, Some(leader.clone()), vec![leader.clone()], vec![leader.clone()]);
        let b = PartitionInfo::new("t", 0, Some(leader.clone()), vec![leader.clone()], vec![leader.clone()]);
        assert_eq!(a, b);

        // differing topic
        let c = PartitionInfo::new("t2", 0, Some(leader.clone()), vec![leader.clone()], vec![leader.clone()]);
        assert_ne!(a, c);

        // differing leader (None vs Some)
        let d = PartitionInfo::new("t", 0, None, vec![leader.clone()], vec![leader.clone()]);
        assert_ne!(a, d);
    }

    #[test]
    fn no_offline_default_to_empty() {
        let info = PartitionInfo::new("t", 0, None, Vec::new(), Vec::new());
        assert!(info.offline_replicas().is_empty());
    }
}
