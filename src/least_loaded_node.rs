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

//! Translation of `org.apache.kafka.clients.LeastLoadedNode`.
//!
//! Note: the Java type lives in `org.apache.kafka.clients` (not `common`),
//! so it sits at the crate root rather than under `common::`.

use crate::common::Node;

/// The result of [`crate::KafkaClient::least_loaded_node`]: the node that
/// should next receive a request together with a flag indicating whether
/// at least one connection is currently ready.
///
/// Mirrors the Java class. A returned `node` of `None` together with
/// `at_least_one_connection_ready == true` indicates "no node free *right
/// now*, but we have a working connection elsewhere — back off rather
/// than reconnect", per the Java rustdoc on
/// `hasNodeAvailableOrConnectionReady`.
#[derive(Debug, Clone)]
pub struct LeastLoadedNode {
    node: Option<Node>,
    at_least_one_connection_ready: bool,
}

impl LeastLoadedNode {
    /// Mirrors `new LeastLoadedNode(Node, boolean)`.
    ///
    /// Java accepts `null` for `node`; the Rust translation uses
    /// `Option<Node>` so the absence is type-checked.
    pub fn new(node: Option<Node>, at_least_one_connection_ready: bool) -> Self {
        LeastLoadedNode { node, at_least_one_connection_ready }
    }

    /// Mirrors `LeastLoadedNode.node()`. Returns `None` when no node is
    /// currently available.
    pub fn node(&self) -> Option<&Node> {
        self.node.as_ref()
    }

    /// Indicates if the least loaded node is available or at least a ready
    /// connection exists.
    ///
    /// There may be no node available while ready connections to live nodes
    /// exist. This may happen when the connections are overloaded with
    /// in-flight requests. This function takes this into account.
    ///
    /// Mirrors `LeastLoadedNode.hasNodeAvailableOrConnectionReady()`.
    pub fn has_node_available_or_connection_ready(&self) -> bool {
        self.node.is_some() || self.at_least_one_connection_ready
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn node_present() {
        let node = Node::new(7, "host".into(), 9092);
        let llm = LeastLoadedNode::new(Some(node.clone()), false);
        assert_eq!(llm.node().expect("node").id(), 7);
        assert!(llm.has_node_available_or_connection_ready());
    }

    #[test]
    fn no_node_but_connection_ready() {
        let llm = LeastLoadedNode::new(None, true);
        assert!(llm.node().is_none());
        assert!(llm.has_node_available_or_connection_ready());
    }

    #[test]
    fn no_node_and_no_ready_connection() {
        let llm = LeastLoadedNode::new(None, false);
        assert!(llm.node().is_none());
        assert!(!llm.has_node_available_or_connection_ready());
    }
}
