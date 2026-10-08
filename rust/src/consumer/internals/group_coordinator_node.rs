// Copyright 2026 Confluent Inc.
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

//! The consumer's group coordinator node.
//!
//! Corresponds to `org.apache.kafka.clients.consumer.internals.GroupCoordinatorNode`
//! (KAFKA-20246 3/N, 7be741d08b).

use crate::common::{Error, Node};

/// This subclass of [`Node`] is used by the consumer for information about a
/// Kafka node which is a group coordinator. It ensures that the `idString`
/// differs from a regular node with the same node ID so that the network code
/// can maintain separate network connections to the same node as a regular
/// broker and as a group coordinator. It achieves this by ensuring that the node
/// ID is non-negative (which it must be because negative node IDs are used for
/// bootstrapping) and by prepending a '+' on the node ID to create the
/// `idString`. This maintains the requirement that the `idString` can be parsed
/// as an integer to obtain the actual node ID.
///
/// # Rust shape
///
/// Rust has no subclassing, and the Java subclass adds no state or behaviour
/// beyond its constructor: it is a [`Node`] built through `Node`'s protected
/// `idString` constructor. So this type has no instances; [`Self::new`] builds
/// and returns that `Node`. What Java's class identity decides, `Node`'s
/// equality reproduces through the `+` `id_string` (see `Node`'s `PartialEq`).
#[doc(alias = "org.apache.kafka.clients.consumer.internals.GroupCoordinatorNode")]
pub(crate) struct GroupCoordinatorNode;

impl GroupCoordinatorNode {
    /// Creates the coordinator node for broker `id`: the broker's real node id,
    /// with `idString` `"+<id>"`.
    ///
    /// Translated from `GroupCoordinatorNode(int, String, int)`
    /// (`GroupCoordinatorNode.java:32-34`).
    ///
    /// # Errors
    ///
    /// [`Error::LocalIllegalArgument`] if `id` is negative, where Java's
    /// `validateId` throws `IllegalArgumentException`.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.GroupCoordinatorNode#GroupCoordinatorNode")]
    #[expect(
        clippy::new_ret_no_self,
        reason = "Java's subclass constructor builds a Node; the Rust type has no instances (see the type docs)"
    )]
    pub(crate) fn new(id: i32, host: String, port: i32) -> Result<Node, Error> {
        let id = Self::validate_id(id)?;
        Ok(Node::with_rack_is_fenced_id_string(
            id,
            host,
            port,
            None,
            false,
            format!("+{id}"),
        ))
    }

    /// Translated from `GroupCoordinatorNode.validateId` (`GroupCoordinatorNode.java:36-41`).
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.GroupCoordinatorNode#validateId")]
    fn validate_id(id: i32) -> Result<i32, Error> {
        if id < 0 {
            return Err(Error::local_illegal_argument(
                "Node id for group coordinator node cannot be negative",
            ));
        }
        Ok(id)
    }
}

/// Translated from `GroupCoordinatorNodeTest` (7be741d08b).
#[cfg(test)]
mod tests {
    use super::*;

    /// Translated from `GroupCoordinatorNodeTest.testValidation`. Java's
    /// `assertDoesNotThrow` on the two plain `Node` constructions has no Rust
    /// analogue (`Node::new` is infallible); they are kept as constructions.
    #[test]
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.GroupCoordinatorNodeTest#testValidation")]
    fn test_validation() {
        let _ = Node::new(0, "localhost".to_string(), 9092);
        let _ = Node::new(-1, "localhost".to_string(), 9092);
        assert!(GroupCoordinatorNode::new(0, "localhost".to_string(), 9092).is_ok());
        let err = GroupCoordinatorNode::new(-1, "localhost".to_string(), 9092).unwrap_err();
        let Error::LocalIllegalArgument(e) = &err else {
            panic!("expected an illegal-argument error, got {err:?}");
        };
        assert_eq!(e.message(), "Node id for group coordinator node cannot be negative");
    }

    /// Translated from `GroupCoordinatorNodeTest.testIdString`. Java's
    /// `Integer.parseInt` becomes `str::parse::<i32>`, which also accepts the
    /// leading `+`.
    #[test]
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.GroupCoordinatorNodeTest#testIdString")]
    fn test_id_string() {
        let node0 = Node::new(0, "localhost".to_string(), 9092);
        let gc_node0 = GroupCoordinatorNode::new(0, "localhost".to_string(), 9092).unwrap();
        assert_eq!(node0.id(), gc_node0.id());
        assert_ne!(node0.id_string(), gc_node0.id_string());
        assert_eq!(0, gc_node0.id_string().parse::<i32>().unwrap());
        assert_eq!(0, node0.id_string().parse::<i32>().unwrap());

        let node1 = Node::new(1, "localhost".to_string(), 9092);
        let gc_node1 = GroupCoordinatorNode::new(1, "localhost".to_string(), 9092).unwrap();
        assert_eq!(node1.id(), gc_node1.id());
        assert_ne!(node1.id_string(), gc_node1.id_string());
        assert_eq!(1, node1.id_string().parse::<i32>().unwrap());
        assert_eq!(1, gc_node1.id_string().parse::<i32>().unwrap());
    }

    /// Java's `Node.equals` rejects a different class, so a coordinator node is
    /// not equal to the plain node of the same broker; two coordinator nodes for
    /// the same broker are. Rust-only: Java's test does not compare them.
    #[test]
    fn test_coordinator_node_differs_from_the_plain_node() {
        let node0 = Node::new(0, "localhost".to_string(), 9092);
        let gc_node0 = GroupCoordinatorNode::new(0, "localhost".to_string(), 9092).unwrap();
        assert_eq!(gc_node0.id_string(), "+0");
        assert_ne!(node0, gc_node0);
        assert_eq!(gc_node0, GroupCoordinatorNode::new(0, "localhost".to_string(), 9092).unwrap());
    }
}
