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

//! Translation of `org.apache.kafka.common.TopicPartitionInfo`.

use std::fmt;

use crate::common::Node;

/// Leadership, replicas, and ISR information for a topic partition.
///
/// `elr` and `last_known_elr` may be `None`, mirroring Java's nullable
/// `elr`/`lastKnownElr` fields populated only by the 6-arg constructor.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct TopicPartitionInfo {
    partition: i32,
    leader: Option<Node>,
    replicas: Vec<Node>,
    isr: Vec<Node>,
    elr: Option<Vec<Node>>,
    last_known_elr: Option<Vec<Node>>,
}

impl TopicPartitionInfo {
    /// Create a [`TopicPartitionInfo`] with full ELR information —
    /// equivalent to Java's 6-argument constructor.
    pub fn new(
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

    /// Create a [`TopicPartitionInfo`] without ELR information —
    /// equivalent to Java's 4-argument constructor.
    pub fn new_without_elr(partition: i32, leader: Option<Node>, replicas: Vec<Node>, isr: Vec<Node>) -> Self {
        Self { partition, leader, replicas, isr, elr: None, last_known_elr: None }
    }

    /// Return the partition id.
    pub fn partition(&self) -> i32 {
        self.partition
    }

    /// Return the leader of the partition, or `None` if there is none.
    pub fn leader(&self) -> Option<&Node> {
        self.leader.as_ref()
    }

    /// Replicas of the partition in the same order as the replica
    /// assignment (the preferred replica is the head of the slice).
    pub fn replicas(&self) -> &[Node] {
        &self.replicas
    }

    /// In-sync replicas of the partition (ordering unspecified).
    pub fn isr(&self) -> &[Node] {
        &self.isr
    }

    /// Eligible leader replicas of the partition, or `None` if not
    /// populated by the broker.
    pub fn elr(&self) -> Option<&[Node]> {
        self.elr.as_deref()
    }

    /// Last known eligible leader replicas, or `None` if not populated by
    /// the broker.
    pub fn last_known_elr(&self) -> Option<&[Node]> {
        self.last_known_elr.as_deref()
    }
}

fn join_nodes(nodes: &[Node]) -> String {
    let parts: Vec<String> = nodes.iter().map(|n| n.to_string()).collect();
    parts.join(", ")
}

impl fmt::Display for TopicPartitionInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let elr_string = match &self.elr {
            Some(e) => join_nodes(e),
            None => "N/A".to_string(),
        };
        let last_known_elr_string = match &self.last_known_elr {
            Some(e) => join_nodes(e),
            None => "N/A".to_string(),
        };
        let leader = match &self.leader {
            Some(n) => n.to_string(),
            None => "null".to_string(),
        };
        write!(
            f,
            "(partition={}, leader={}, replicas={}, isr={}, elr={}, lastKnownElr={})",
            self.partition,
            leader,
            join_nodes(&self.replicas),
            join_nodes(&self.isr),
            elr_string,
            last_known_elr_string
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn equals_and_accessors() {
        let n0 = Node::new(0, "h".to_string(), 9092);
        let n1 = Node::new(1, "h".to_string(), 9093);
        let a =
            TopicPartitionInfo::new_without_elr(0, Some(n0.clone()), vec![n0.clone(), n1.clone()], vec![n0.clone()]);
        let b =
            TopicPartitionInfo::new_without_elr(0, Some(n0.clone()), vec![n0.clone(), n1.clone()], vec![n0.clone()]);
        assert_eq!(a, b);
        assert_eq!(a.partition(), 0);
        assert_eq!(a.leader(), Some(&n0));
        assert_eq!(a.replicas(), &[n0.clone(), n1.clone()]);
        assert_eq!(a.isr(), std::slice::from_ref(&n0));
        assert_eq!(a.elr(), None);
        assert_eq!(a.last_known_elr(), None);
    }

    #[test]
    fn elr_constructor_populates_fields() {
        let n0 = Node::new(0, "h".to_string(), 9092);
        let info = TopicPartitionInfo::new(
            1,
            Some(n0.clone()),
            vec![n0.clone()],
            vec![n0.clone()],
            vec![n0.clone()],
            vec![n0.clone()],
        );
        assert_eq!(info.elr(), Some(&[n0.clone()][..]));
        assert_eq!(info.last_known_elr(), Some(&[n0.clone()][..]));
    }

    #[test]
    fn display_contains_partition_and_leader() {
        let n0 = Node::new(0, "h".to_string(), 9092);
        let info = TopicPartitionInfo::new_without_elr(7, Some(n0.clone()), vec![n0.clone()], vec![n0]);
        let s = info.to_string();
        assert!(s.contains("partition=7"));
        assert!(s.contains("elr=N/A"));
        assert!(s.contains("lastKnownElr=N/A"));
    }
}
