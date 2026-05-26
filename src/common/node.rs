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

//! Translation of `org.apache.kafka.common.Node`.

use std::fmt;
use std::sync::OnceLock;

/// Information about a Kafka node.
///
/// Equality and hashing follow the Java implementation, which compares
/// `id`, `port`, `host`, `rack`, and `is_fenced`.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Node {
    id: i32,
    id_string: String,
    host: String,
    port: i32,
    rack: Option<String>,
    is_fenced: bool,
}

impl Node {
    /// Create a node without a rack and not fenced — equivalent to Java's
    /// `Node(int id, String host, int port)`.
    pub fn new(id: i32, host: String, port: i32) -> Self {
        Self::new_with_rack(id, host, port, None)
    }

    /// Create a node with an optional rack — equivalent to Java's
    /// `Node(int id, String host, int port, String rack)`.
    pub fn new_with_rack(id: i32, host: String, port: i32, rack: Option<String>) -> Self {
        Self { id, id_string: id.to_string(), host, port, rack, is_fenced: false }
    }

    /// Create a node with all fields — equivalent to Java's
    /// `Node(int id, String host, int port, String rack, boolean isFenced)`.
    pub fn new_full(id: i32, host: String, port: i32, rack: Option<String>, is_fenced: bool) -> Self {
        Self { id, id_string: id.to_string(), host, port, rack, is_fenced }
    }

    /// Return the singleton "no node" sentinel — `id = -1`, empty host,
    /// `port = -1`. Equivalent to Java's `Node.noNode()`.
    pub fn no_node() -> &'static Node {
        static NO_NODE: OnceLock<Node> = OnceLock::new();
        NO_NODE.get_or_init(|| Node::new(-1, String::new(), -1))
    }

    /// Check whether this node is empty, which may be the case if
    /// [`Node::no_node`] is used as a placeholder in a response payload with
    /// an error.
    pub fn is_empty(&self) -> bool {
        self.host.is_empty() || self.port < 0
    }

    /// The node id of this node.
    pub fn id(&self) -> i32 {
        self.id
    }

    /// String representation of the node id. Typically the integer id is
    /// used to serialize over the wire; the string form is used as an
    /// identifier with `NetworkClient` code.
    pub fn id_string(&self) -> &str {
        &self.id_string
    }

    /// The host name for this node.
    pub fn host(&self) -> &str {
        &self.host
    }

    /// The port for this node.
    pub fn port(&self) -> i32 {
        self.port
    }

    /// True if this node has a defined rack.
    pub fn has_rack(&self) -> bool {
        self.rack.is_some()
    }

    /// The rack for this node, if any.
    pub fn rack(&self) -> Option<&str> {
        self.rack.as_deref()
    }

    /// Returns whether this node is fenced. Applies to broker nodes only;
    /// for controller quorum nodes this is always `false`.
    pub fn is_fenced(&self) -> bool {
        self.is_fenced
    }
}

impl fmt::Display for Node {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Java: host + ":" + port + " (id: " + idString + " rack: " + rack + " isFenced: " + isFenced + ")"
        // When rack is null, Java prints "null".
        match &self.rack {
            Some(rack) => write!(
                f,
                "{}:{} (id: {} rack: {} isFenced: {})",
                self.host, self.port, self.id_string, rack, self.is_fenced
            ),
            None => write!(
                f,
                "{}:{} (id: {} rack: null isFenced: {})",
                self.host, self.port, self.id_string, self.is_fenced
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_node_has_sentinel_values() {
        let n = Node::no_node();
        assert_eq!(n.id(), -1);
        assert_eq!(n.host(), "");
        assert_eq!(n.port(), -1);
        assert!(n.is_empty());
        assert!(!n.has_rack());
        assert!(!n.is_fenced());
    }

    #[test]
    fn no_node_is_cached() {
        let a: *const Node = Node::no_node();
        let b: *const Node = Node::no_node();
        assert_eq!(a, b, "no_node() must return the same cached instance");
    }

    #[test]
    fn is_empty_for_negative_port() {
        let n = Node::new(0, "host".to_string(), -1);
        assert!(n.is_empty());
    }

    #[test]
    fn is_empty_for_empty_host() {
        let n = Node::new(0, String::new(), 9092);
        assert!(n.is_empty());
    }

    #[test]
    fn id_string_caches_int() {
        let n = Node::new(42, "host".to_string(), 9092);
        assert_eq!(n.id_string(), "42");
    }

    #[test]
    fn equals_uses_all_fields() {
        let a = Node::new_full(1, "h".to_string(), 9, Some("r".to_string()), false);
        let b = Node::new_full(1, "h".to_string(), 9, Some("r".to_string()), false);
        assert_eq!(a, b);

        // differing isFenced
        let c = Node::new_full(1, "h".to_string(), 9, Some("r".to_string()), true);
        assert_ne!(a, c);

        // differing rack
        let d = Node::new_full(1, "h".to_string(), 9, Some("r2".to_string()), false);
        assert_ne!(a, d);

        // differing id
        let e = Node::new_full(2, "h".to_string(), 9, Some("r".to_string()), false);
        assert_ne!(a, e);
    }

    #[test]
    fn display_renders_null_rack() {
        let n = Node::new(0, "host".to_string(), 9092);
        assert_eq!(n.to_string(), "host:9092 (id: 0 rack: null isFenced: false)");
    }

    #[test]
    fn display_renders_rack_when_present() {
        let n = Node::new_with_rack(1, "h".to_string(), 99, Some("r1".to_string()));
        assert_eq!(n.to_string(), "h:99 (id: 1 rack: r1 isFenced: false)");
    }
}
