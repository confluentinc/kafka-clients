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

//! `FindCoordinator` response handling.
//!
//! Corresponds to `org.apache.kafka.common.requests.FindCoordinatorResponse`.
//!
//! Possible error codes:
//!  - `CoordinatorLoadInProgress` (14)
//!  - `CoordinatorNotAvailable` (15)
//!  - `GroupAuthorizationFailed` (30)
//!  - `InvalidRequest` (42)
//!  - `TransactionalIdAuthorizationFailed` (53)

use std::collections::HashMap;
use std::io;

use crate::FindCoordinatorResponseData;
use crate::common::Node;
use crate::common::protocol::{ApiKeys, Errors, Readable};
use crate::find_coordinator_response_data::Coordinator;

use super::AbstractResponse;

/// A `FindCoordinator` response.
///
/// Corresponds to `org.apache.kafka.common.requests.FindCoordinatorResponse`.
#[derive(Debug, Clone)]
pub struct FindCoordinatorResponse {
    data: FindCoordinatorResponseData,
}

impl FindCoordinatorResponse {
    /// Creates a new `FindCoordinatorResponse` from the underlying data.
    pub fn new(data: FindCoordinatorResponseData) -> Self {
        Self { data }
    }

    /// Returns the API key for this response.
    pub fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::FIND_COORDINATOR
    }

    /// Returns a reference to the underlying data.
    pub fn data(&self) -> &FindCoordinatorResponseData {
        &self.data
    }

    /// Returns a mutable reference to the underlying data.
    pub(crate) fn data_mut(&mut self) -> &mut FindCoordinatorResponseData {
        &mut self.data
    }

    /// Returns the throttle time in milliseconds.
    pub fn throttle_time_ms(&self) -> i32 {
        self.data.throttle_time_ms
    }

    /// Sets the throttle time in the response.
    pub fn maybe_set_throttle_time_ms(&mut self, throttle_time_ms: i32) {
        self.data.set_throttle_time_ms(throttle_time_ms);
    }

    /// Whether the client should throttle upon receiving this response.
    ///
    /// Returns `true` for v2+.
    pub fn should_client_throttle(&self, version: i16) -> bool {
        version >= 2
    }

    /// Returns the top-level (v <= 3) error code wrapped as an [`Errors`].
    pub fn error(&self) -> Errors {
        Errors::for_code(self.data.error_code)
    }

    /// Returns `true` if the top-level (v <= 3) error code is non-zero.
    pub fn has_error(&self) -> bool {
        self.error() != Errors::None
    }

    /// Returns the `Coordinator` entry for a specific key.
    ///
    /// For v <= 3 (where the response carries a single coordinator embedded in
    /// the top-level fields), the returned coordinator is synthesized from
    /// those fields and tagged with the supplied key. For v >= 4 the
    /// coordinator with matching `key` is looked up in the `coordinators`
    /// list.
    pub fn coordinator_by_key(&self, key: &str) -> Option<Coordinator> {
        if self.data.coordinators.is_empty() {
            // version <= 3
            let mut c = Coordinator::new();
            c.set_error_code(self.data.error_code)
                .set_error_message(self.data.error_message.clone())
                .set_host(self.data.host.clone())
                .set_port(self.data.port)
                .set_node_id(self.data.node_id)
                .set_key(key.to_string());
            return Some(c);
        }
        // version >= 4
        self.data.coordinators.iter().find(|c| c.key == key).cloned()
    }

    /// Returns a [`Node`] for the single (v <= 3) coordinator embedded in the
    /// top-level fields.
    pub fn node(&self) -> Node {
        Node::new(self.data.node_id, self.data.host.clone(), self.data.port)
    }

    /// Returns the coordinator list — for v <= 3 a singleton synthesized from
    /// the top-level fields, for v >= 4 the wire `coordinators` list.
    pub fn coordinators(&self) -> Vec<Coordinator> {
        if !self.data.coordinators.is_empty() {
            return self.data.coordinators.clone();
        }
        let mut c = Coordinator::new();
        c.set_error_code(self.data.error_code)
            .set_error_message(self.data.error_message.clone())
            // Java passes null key; Rust spec defaults to empty string.
            .set_key(String::new())
            .set_node_id(self.data.node_id)
            .set_host(self.data.host.clone())
            .set_port(self.data.port);
        vec![c]
    }

    /// Returns error counts by [`Errors`].
    pub fn error_counts(&self) -> HashMap<Errors, i32> {
        let mut counts = HashMap::new();
        if !self.data.coordinators.is_empty() {
            for coordinator in &self.data.coordinators {
                AbstractResponse::update_error_counts(&mut counts, Errors::for_code(coordinator.error_code));
            }
        } else {
            AbstractResponse::update_error_counts(&mut counts, self.error());
        }
        counts
    }

    /// Parses a `FindCoordinatorResponse` from a readable buffer at the given
    /// version.
    ///
    /// # Errors
    ///
    /// Returns an error if parsing fails.
    pub fn parse(readable: &mut dyn Readable, version: i16) -> io::Result<Self> {
        let data = FindCoordinatorResponseData::read(readable, version)?;
        Ok(Self::new(data))
    }

    /// Constructs a synthetic v <= 3 response for testing.
    ///
    /// Corresponds to `FindCoordinatorResponse.prepareOldResponse(Errors, Node)`.
    pub fn prepare_old_response(error: Errors, node: &Node) -> Self {
        let mut data = FindCoordinatorResponseData::new();
        data.set_error_code(error.code())
            .set_error_message(Some(error.message().to_string()))
            .set_node_id(node.id())
            .set_host(node.host().to_string())
            .set_port(node.port());
        Self::new(data)
    }

    /// Constructs a synthetic v >= 4 response with a single coordinator entry.
    ///
    /// Corresponds to `FindCoordinatorResponse.prepareResponse(Errors, String, Node)`.
    pub fn prepare_response(error: Errors, key: impl Into<String>, node: &Node) -> Self {
        let mut data = FindCoordinatorResponseData::new();
        data.set_coordinators(vec![Self::prepare_coordinator_response(error, key, node)]);
        Self::new(data)
    }

    /// Builds a single `Coordinator` entry with the supplied error/key/node.
    pub fn prepare_coordinator_response(error: Errors, key: impl Into<String>, node: &Node) -> Coordinator {
        let mut c = Coordinator::new();
        c.set_error_code(error.code())
            .set_error_message(Some(error.message().to_string()))
            .set_key(key.into())
            .set_host(node.host().to_string())
            .set_port(node.port())
            .set_node_id(node.id());
        c
    }

    /// Constructs a synthetic v >= 4 error response with one entry per key.
    ///
    /// Corresponds to `FindCoordinatorResponse.prepareErrorResponse(Errors, List<String>)`.
    pub fn prepare_error_response(error: Errors, keys: &[String]) -> Self {
        let mut data = FindCoordinatorResponseData::new();
        let no_node = Node::no_node();
        let coordinators: Vec<Coordinator> = keys
            .iter()
            .map(|key| {
                let mut c = Coordinator::new();
                c.set_error_code(error.code())
                    .set_error_message(Some(error.message().to_string()))
                    .set_key(key.clone())
                    .set_host(no_node.host().to_string())
                    .set_port(no_node.port())
                    .set_node_id(no_node.id());
                c
            })
            .collect();
        data.set_coordinators(coordinators);
        Self::new(data)
    }
}

impl std::fmt::Display for FindCoordinatorResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Verifies that v <= 3 `prepare_old_response` synthesizes a coordinator
    /// matching the requested key when looked up via `coordinator_by_key`.
    #[test]
    fn test_prepare_old_response_coordinator_by_key() {
        let node = Node::new(1, "localhost".to_string(), 9092);
        let resp = FindCoordinatorResponse::prepare_old_response(Errors::None, &node);
        let c = resp.coordinator_by_key("g1").expect("synthesized coordinator");
        assert_eq!(c.key, "g1");
        assert_eq!(c.node_id, 1);
        assert_eq!(c.host, "localhost");
        assert_eq!(c.port, 9092);
        assert_eq!(c.error_code, Errors::None.code());
        // node() accessor returns the v <= 3 single coordinator
        let n = resp.node();
        assert_eq!(n.id(), 1);
        assert_eq!(n.host(), "localhost");
        assert_eq!(n.port(), 9092);
    }

    /// Verifies that v4+ `prepare_response` produces a coordinator addressable
    /// by `coordinator_by_key` with matching field values.
    #[test]
    fn test_prepare_response_coordinator_by_key() {
        let node = Node::new(2, "h".to_string(), 9093);
        let resp = FindCoordinatorResponse::prepare_response(Errors::None, "g1", &node);
        let c = resp.coordinator_by_key("g1").expect("present");
        assert_eq!(c.key, "g1");
        assert_eq!(c.node_id, 2);
        assert!(resp.coordinator_by_key("missing").is_none());
    }

    /// Verifies that `has_error` reads the top-level error code on v <= 3.
    #[test]
    fn test_has_error_reflects_top_level_code_on_old_response() {
        let node = Node::new(1, "h".to_string(), 9092);
        let resp = FindCoordinatorResponse::prepare_old_response(Errors::CoordinatorNotAvailable, &node);
        assert!(resp.has_error());
        assert_eq!(resp.error(), Errors::CoordinatorNotAvailable);
    }

    /// Verifies `error_counts` aggregates across v4+ coordinator entries.
    #[test]
    fn test_error_counts_v4_aggregates_per_coordinator() {
        let resp = FindCoordinatorResponse::prepare_error_response(
            Errors::CoordinatorLoadInProgress,
            &["g1".to_string(), "g2".to_string()],
        );
        let counts = resp.error_counts();
        assert_eq!(counts.get(&Errors::CoordinatorLoadInProgress).copied().unwrap_or(0), 2);
    }

    /// Verifies that `coordinators()` synthesizes a one-element list on v <= 3.
    #[test]
    fn test_coordinators_v3_synthesizes_single_element() {
        let node = Node::new(7, "h".to_string(), 9092);
        let resp = FindCoordinatorResponse::prepare_old_response(Errors::None, &node);
        let list = resp.coordinators();
        assert_eq!(list.len(), 1);
        assert_eq!(list[0].node_id, 7);
    }
}
