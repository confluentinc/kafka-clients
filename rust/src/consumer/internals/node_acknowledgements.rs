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

//! The acknowledgements of one partition, with the node they are sent to.
//!
//! Translated from `org.apache.kafka.clients.consumer.internals.NodeAcknowledgements`.

use crate::consumer::internals::Acknowledgements;

/// The [`Acknowledgements`] of a partition and the id of the node that
/// delivered its records, where they are sent.
// Java's `Objects.requireNonNull(acknowledgements)` is the type system's here.
#[derive(Debug)]
#[doc(alias = "org.apache.kafka.clients.consumer.internals.NodeAcknowledgements")]
pub(crate) struct NodeAcknowledgements {
    node_id: i32,
    acknowledgements: Acknowledgements,
}

impl NodeAcknowledgements {
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.NodeAcknowledgements#NodeAcknowledgements")]
    pub(crate) fn new(node_id: i32, acknowledgements: Acknowledgements) -> Self {
        Self { node_id, acknowledgements }
    }

    #[doc(alias = "org.apache.kafka.clients.consumer.internals.NodeAcknowledgements#nodeId")]
    pub(crate) fn node_id(&self) -> i32 {
        self.node_id
    }

    #[doc(alias = "org.apache.kafka.clients.consumer.internals.NodeAcknowledgements#acknowledgements")]
    pub(crate) fn acknowledgements(&self) -> &Acknowledgements {
        &self.acknowledgements
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::consumer::AcknowledgeType;

    // Java has no `NodeAcknowledgementsTest`.
    #[test]
    fn test_accessors() {
        let mut acknowledgements = Acknowledgements::empty();
        acknowledgements.add(4, Some(AcknowledgeType::Accept));
        let node_acknowledgements = NodeAcknowledgements::new(7, acknowledgements);
        assert_eq!(7, node_acknowledgements.node_id());
        assert_eq!(Some(AcknowledgeType::Accept), node_acknowledgements.acknowledgements().get(4));
    }
}
