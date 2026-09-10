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

//! Leadership, replicas and ISR information for a topic partition.
//!
//! Corresponds to `org.apache.kafka.common.TopicPartitionInfo`.

use crate::common::Node;

/// A class containing leadership, replicas and ISR information for a topic
/// partition.
///
/// Corresponds to `org.apache.kafka.common.TopicPartitionInfo`.
///
/// `leader` is `None` when there is no leader (Java `null`); `elr` and
/// `last_known_elr` are `None` when the information is unavailable (the Java
/// short constructor leaves them `null`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TopicPartitionInfo {
    partition: i32,
    leader: Option<Node>,
    replicas: Vec<Node>,
    isr: Vec<Node>,
    elr: Option<Vec<Node>>,
    last_known_elr: Option<Vec<Node>>,
}

impl TopicPartitionInfo {
    /// Create an instance with the provided parameters.
    ///
    /// * `partition` - the partition id
    /// * `leader` - the leader of the partition or `None` if there is none
    /// * `replicas` - the replicas of the partition in the same order as the
    ///   replica assignment (the preferred replica is the head of the list)
    /// * `isr` - the in-sync replicas
    /// * `elr` - the eligible leader replicas
    /// * `last_known_elr` - the last known eligible leader replicas
    pub fn new_elr_last_known_elr(
        partition: i32,
        leader: Option<Node>,
        replicas: Vec<Node>,
        isr: Vec<Node>,
        elr: Vec<Node>,
        last_known_elr: Vec<Node>,
    ) -> Self {
        Self {
            partition,
            leader,
            replicas,
            isr,
            elr: Some(elr),
            last_known_elr: Some(last_known_elr),
        }
    }

    /// Create an instance without eligible-leader-replica information (the Java
    /// four-argument constructor, which leaves `elr`/`last_known_elr` `null`).
    pub fn new(partition: i32, leader: Option<Node>, replicas: Vec<Node>, isr: Vec<Node>) -> Self {
        Self { partition, leader, replicas, isr, elr: None, last_known_elr: None }
    }

    /// Return the partition id.
    pub fn partition(&self) -> i32 {
        self.partition
    }

    /// Return the leader of the partition or `None` if there is none.
    pub fn leader(&self) -> Option<&Node> {
        self.leader.as_ref()
    }

    /// Return the replicas of the partition in the same order as the replica
    /// assignment. The preferred replica is the head of the list.
    pub fn replicas(&self) -> &[Node] {
        &self.replicas
    }

    /// Return the in-sync replicas of the partition. The ordering is
    /// unspecified.
    pub fn isr(&self) -> &[Node] {
        &self.isr
    }

    /// Return the eligible leader replicas of the partition, or `None` if
    /// unavailable. The ordering is unspecified.
    pub fn elr(&self) -> Option<&[Node]> {
        self.elr.as_deref()
    }

    /// Return the last known eligible leader replicas of the partition, or
    /// `None` if unavailable. The ordering is unspecified.
    pub fn last_known_elr(&self) -> Option<&[Node]> {
        self.last_known_elr.as_deref()
    }
}

impl std::fmt::Display for TopicPartitionInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        fn join(nodes: &[Node]) -> String {
            nodes.iter().map(|n| n.to_string()).collect::<Vec<_>>().join(", ")
        }
        let leader = match &self.leader {
            Some(n) => n.to_string(),
            None => "null".to_string(),
        };
        let elr_string = self.elr.as_ref().map_or_else(|| "N/A".to_string(), |v| join(v));
        let last_known_elr_string = self.last_known_elr.as_ref().map_or_else(|| "N/A".to_string(), |v| join(v));
        write!(
            f,
            "(partition={}, leader={}, replicas={}, isr={}, elr={}, lastKnownElr={})",
            self.partition,
            leader,
            join(&self.replicas),
            join(&self.isr),
            elr_string,
            last_known_elr_string
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node(id: i32) -> Node {
        Node::new(id, "host".to_string(), 9092)
    }

    #[test]
    fn accessors_full_constructor() {
        let info = TopicPartitionInfo::new_elr_last_known_elr(
            0,
            Some(node(1)),
            vec![node(1), node(2)],
            vec![node(1)],
            vec![node(2)],
            vec![node(3)],
        );
        assert_eq!(info.partition(), 0);
        assert_eq!(info.leader(), Some(&node(1)));
        assert_eq!(info.replicas().len(), 2);
        assert_eq!(info.isr(), &[node(1)]);
        assert_eq!(info.elr(), Some(&[node(2)][..]));
        assert_eq!(info.last_known_elr(), Some(&[node(3)][..]));
    }

    #[test]
    fn short_constructor_leaves_elr_none() {
        let info = TopicPartitionInfo::new(1, None, vec![node(1)], vec![node(1)]);
        assert_eq!(info.leader(), None);
        assert_eq!(info.elr(), None);
        assert_eq!(info.last_known_elr(), None);
    }

    #[test]
    fn equality() {
        let a = TopicPartitionInfo::new(0, Some(node(1)), vec![], vec![]);
        let b = TopicPartitionInfo::new(0, Some(node(1)), vec![], vec![]);
        let c = TopicPartitionInfo::new(1, Some(node(1)), vec![], vec![]);
        assert_eq!(a, b);
        assert_ne!(a, c);
    }

    #[test]
    fn display_uses_na_when_elr_absent() {
        let info = TopicPartitionInfo::new(0, None, vec![], vec![]);
        let s = info.to_string();
        assert!(s.contains("elr=N/A"), "{s}");
        assert!(s.contains("lastKnownElr=N/A"), "{s}");
    }
}
