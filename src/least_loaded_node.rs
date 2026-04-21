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

//! Represents the result of finding the least loaded node in a cluster.
//!
//! Translated from `org.apache.kafka.clients.LeastLoadedNode`.

use crate::common::Node;

/// Represents the result of finding the least loaded node in a cluster.
///
/// Contains the node (if available) and whether at least one connection is ready.
#[derive(Debug, Clone)]
pub struct LeastLoadedNode {
    node: Option<Node>,
    at_least_one_connection_ready: bool,
}

impl LeastLoadedNode {
    /// Creates a new `LeastLoadedNode`.
    ///
    /// # Arguments
    /// * `node` - The least loaded node, or `None` if no node is available.
    /// * `at_least_one_connection_ready` - Whether at least one connection to a live node is ready.
    pub fn new(node: Option<Node>, at_least_one_connection_ready: bool) -> Self {
        Self { node, at_least_one_connection_ready }
    }

    /// Returns a reference to the node, if available.
    pub fn node(&self) -> Option<&Node> {
        self.node.as_ref()
    }

    /// Indicates if the least loaded node is available or at least a ready connection exists.
    ///
    /// There may be no node available while ready connections to live nodes exist. This may happen
    /// when the connections are overloaded with in-flight requests. This function takes this into
    /// account.
    pub fn has_node_available_or_connection_ready(&self) -> bool {
        self.node.is_some() || self.at_least_one_connection_ready
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_no_node_no_connection() {
        let lln = LeastLoadedNode::new(None, false);
        assert!(lln.node().is_none());
        assert!(!lln.has_node_available_or_connection_ready());
    }

    #[test]
    fn test_no_node_with_connection() {
        let lln = LeastLoadedNode::new(None, true);
        assert!(lln.node().is_none());
        assert!(lln.has_node_available_or_connection_ready());
    }

    #[test]
    fn test_with_node_no_connection() {
        let node = Node::new(0, "localhost".to_string(), 9092);
        let lln = LeastLoadedNode::new(Some(node), false);
        assert!(lln.node().is_some());
        assert!(lln.has_node_available_or_connection_ready());
    }

    #[test]
    fn test_with_node_and_connection() {
        let node = Node::new(0, "localhost".to_string(), 9092);
        let lln = LeastLoadedNode::new(Some(node), true);
        assert!(lln.node().is_some());
        assert!(lln.has_node_available_or_connection_ready());
    }
}
