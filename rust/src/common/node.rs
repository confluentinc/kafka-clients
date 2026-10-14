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

//! Information about a Kafka node.

use std::fmt;

/// Information about a Kafka node.
#[derive(Clone, Debug)]
#[doc(alias = "org.apache.kafka.common.Node")]
pub struct Node {
    id: i32,
    id_string: String,
    host: String,
    port: i32,
    rack: Option<String>,
    is_fenced: bool,
}

/// A sentinel node representing no node.
static NO_NODE: std::sync::LazyLock<Node> = std::sync::LazyLock::new(|| Node::new(-1, String::new(), -1));

impl Node {
    /// Creates a new `Node` with no rack and not fenced.
    #[doc(alias = "org.apache.kafka.common.Node#Node(int,String,int)")]
    pub fn new(id: i32, host: String, port: i32) -> Self {
        Self::with_rack(id, host, port, None)
    }

    /// Creates a new `Node` with the given rack.
    #[doc(alias = "org.apache.kafka.common.Node#Node(int,String,int,String)")]
    pub fn with_rack(id: i32, host: String, port: i32, rack: Option<String>) -> Self {
        Self::with_rack_is_fenced(id, host, port, rack, false)
    }

    /// Creates a new `Node` with the given rack and fenced status.
    #[doc(alias = "org.apache.kafka.common.Node#Node(int,String,int,String,boolean)")]
    pub fn with_rack_is_fenced(id: i32, host: String, port: i32, rack: Option<String>, is_fenced: bool) -> Self {
        Self::with_rack_is_fenced_id_string(id, host, port, rack, is_fenced, id.to_string())
    }

    /// Creates a new `Node` whose [`id_string`](Self::id_string) is given
    /// explicitly instead of being derived from `id`.
    ///
    /// Java's `protected` constructor (`Node.java:54-61`, KAFKA-20246 3/N),
    /// whose only caller is the consumer's `GroupCoordinatorNode` subclass,
    /// hence crate-private here.
    #[doc(alias = "org.apache.kafka.common.Node#Node(int,String,int,String,boolean,String)")]
    pub(crate) fn with_rack_is_fenced_id_string(
        id: i32,
        host: String,
        port: i32,
        rack: Option<String>,
        is_fenced: bool,
        id_string: String,
    ) -> Self {
        Self { id, id_string, host, port, rack, is_fenced }
    }

    /// Returns a sentinel node representing no node.
    #[doc(alias = "org.apache.kafka.common.Node#noNode")]
    pub fn no_node() -> &'static Node {
        &NO_NODE
    }

    /// Check whether this node is empty, which may be the case if `no_node()` is used
    /// as a placeholder in a response payload with an error.
    #[doc(alias = "org.apache.kafka.common.Node#isEmpty")]
    pub fn is_empty(&self) -> bool {
        self.host.is_empty() || self.port < 0
    }

    /// The node id of this node.
    #[doc(alias = "org.apache.kafka.common.Node#id")]
    pub fn id(&self) -> i32 {
        self.id
    }

    /// String representation of the node id.
    /// Typically, the integer id is used to serialize over the wire, the string
    /// representation is used as an identifier with NetworkClient code.
    #[doc(alias = "org.apache.kafka.common.Node#idString")]
    pub fn id_string(&self) -> &str {
        &self.id_string
    }

    /// The host name for this node.
    #[doc(alias = "org.apache.kafka.common.Node#host")]
    pub fn host(&self) -> &str {
        &self.host
    }

    /// The port for this node.
    #[doc(alias = "org.apache.kafka.common.Node#port")]
    pub fn port(&self) -> i32 {
        self.port
    }

    /// True if this node has a defined rack.
    #[doc(alias = "org.apache.kafka.common.Node#hasRack")]
    pub fn has_rack(&self) -> bool {
        self.rack.is_some()
    }

    /// The rack for this node.
    #[doc(alias = "org.apache.kafka.common.Node#rack")]
    pub fn rack(&self) -> Option<&str> {
        self.rack.as_deref()
    }

    /// Returns whether this node is fenced.
    ///
    /// This applies to broker nodes only. For controller quorum nodes, this field
    /// is not relevant and is defined to be `false`.
    #[doc(alias = "org.apache.kafka.common.Node#isFenced")]
    pub fn is_fenced(&self) -> bool {
        self.is_fenced
    }
}

/// Java's `Node.equals` (`Node.java:147-158`) first requires
/// `getClass() == obj.getClass()`, so a `GroupCoordinatorNode` never equals a
/// plain `Node` with the same fields. Rust has no subclass to compare, so
/// `id_string` stands in for the class: a plain node's is `id.to_string()` and
/// a group coordinator's is `"+" + id`, so for equal ids the two `id_string`s
/// are equal exactly when the Java classes are. `Hash` leaves it out, as Java's
/// `hashCode` leaves the class out; equal values still hash equally.
impl PartialEq for Node {
    fn eq(&self, other: &Self) -> bool {
        self.id_string == other.id_string
            && self.id == other.id
            && self.port == other.port
            && self.host == other.host
            && self.rack == other.rack
            && self.is_fenced == other.is_fenced
    }
}

impl Eq for Node {}

impl std::hash::Hash for Node {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.host.hash(state);
        self.id.hash(state);
        self.port.hash(state);
        self.rack.hash(state);
        self.is_fenced.hash(state);
    }
}

impl fmt::Display for Node {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}:{} (id: {} rack: {:?} isFenced: {})",
            self.host, self.port, self.id_string, self.rack, self.is_fenced
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_node_creation() {
        let node = Node::new(1, "localhost".to_string(), 9092);
        assert_eq!(node.id(), 1);
        assert_eq!(node.host(), "localhost");
        assert_eq!(node.port(), 9092);
        assert_eq!(node.id_string(), "1");
        assert!(!node.has_rack());
        assert!(node.rack().is_none());
        assert!(!node.is_fenced());
    }

    #[test]
    fn test_node_with_rack() {
        let node = Node::with_rack(1, "localhost".to_string(), 9092, Some("rack1".to_string()));
        assert!(node.has_rack());
        assert_eq!(node.rack(), Some("rack1"));
    }

    #[test]
    fn test_node_with_fenced() {
        let node = Node::with_rack_is_fenced(1, "localhost".to_string(), 9092, None, true);
        assert!(node.is_fenced());
    }

    #[test]
    fn test_no_node() {
        let node = Node::no_node();
        assert_eq!(node.id(), -1);
        assert!(node.is_empty());
    }

    #[test]
    fn test_node_is_empty() {
        let empty1 = Node::new(-1, String::new(), -1);
        assert!(empty1.is_empty());

        let empty2 = Node::new(0, "host".to_string(), -1);
        assert!(empty2.is_empty());

        let not_empty = Node::new(0, "host".to_string(), 9092);
        assert!(!not_empty.is_empty());
    }

    #[test]
    fn test_node_equality() {
        let n1 = Node::new(1, "host".to_string(), 9092);
        let n2 = Node::new(1, "host".to_string(), 9092);
        let n3 = Node::new(2, "host".to_string(), 9092);
        assert_eq!(n1, n2);
        assert_ne!(n1, n3);
    }

    #[test]
    fn test_node_hash() {
        use std::collections::HashSet;
        let mut set = HashSet::new();
        set.insert(Node::new(1, "host".to_string(), 9092));
        set.insert(Node::new(1, "host".to_string(), 9092));
        assert_eq!(set.len(), 1);
    }

    /// The protected `idString` constructor (7be741d08b) keeps every other
    /// field, and an explicit `idString` makes the node differ from a plain one
    /// with the same fields, as Java's class check does.
    #[test]
    fn test_node_with_explicit_id_string() {
        let node = Node::with_rack_is_fenced_id_string(1, "host".to_string(), 9092, None, false, "+1".to_string());
        assert_eq!(node.id(), 1);
        assert_eq!(node.id_string(), "+1");
        assert_eq!(node.host(), "host");
        assert_eq!(node.port(), 9092);
        assert_eq!(node.to_string(), "host:9092 (id: +1 rack: None isFenced: false)");
        assert_ne!(node, Node::new(1, "host".to_string(), 9092));
        assert_eq!(
            node,
            Node::with_rack_is_fenced_id_string(1, "host".to_string(), 9092, None, false, "+1".to_string())
        );
    }

    #[test]
    fn test_node_display() {
        let node = Node::with_rack(1, "localhost".to_string(), 9092, Some("rack1".to_string()));
        let display = node.to_string();
        assert!(display.contains("localhost:9092"));
        assert!(display.contains("id: 1"));
    }
}
