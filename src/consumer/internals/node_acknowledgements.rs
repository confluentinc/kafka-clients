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

//! Combines [`Acknowledgements`] with the id of the node to use for
//! acknowledging.
//!
//! Corresponds to
//! `org.apache.kafka.clients.consumer.internals.NodeAcknowledgements`.

// This blocker type is required by `ShareFetch::take_acknowledged_records`
// (Phase 3). Its only other consumer, `ShareConsumeRequestManager`, arrives in
// a later phase.
#![allow(dead_code)]

use super::acknowledgements::Acknowledgements;

/// This class combines [`Acknowledgements`] with the id of the node to use for
/// acknowledging.
///
/// Corresponds to
/// `org.apache.kafka.clients.consumer.internals.NodeAcknowledgements`.
pub(crate) struct NodeAcknowledgements {
    node_id: i32,
    acknowledgements: Acknowledgements,
}

impl NodeAcknowledgements {
    /// Constructs a new `NodeAcknowledgements`.
    ///
    /// Mirrors Java's `NodeAcknowledgements(int nodeId, Acknowledgements acknowledgements)`.
    pub(crate) fn new(node_id: i32, acknowledgements: Acknowledgements) -> Self {
        // Java validates `acknowledgements != null`; this is a type-system
        // invariant in Rust.
        Self { node_id, acknowledgements }
    }

    /// Returns the node id. Mirrors Java's `int nodeId()`.
    pub(crate) fn node_id(&self) -> i32 {
        self.node_id
    }

    /// Returns the acknowledgements. Mirrors Java's `Acknowledgements acknowledgements()`.
    pub(crate) fn acknowledgements(&self) -> &Acknowledgements {
        &self.acknowledgements
    }

    /// Consumes this wrapper, returning the owned acknowledgements.
    ///
    /// Rust-only convenience for the (later-phase) request-manager path, which
    /// needs to move the acknowledgements out to build the wire request.
    pub(crate) fn into_acknowledgements(self) -> Acknowledgements {
        self.acknowledgements
    }
}
