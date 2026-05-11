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

//! Translation of `org.apache.kafka.clients.ManualMetadataUpdater`.
//!
//! Note: the Java type lives in `org.apache.kafka.clients` (not `common`),
//! so it sits at the crate root rather than under `common::`.

use crate::MetadataUpdater;
use crate::common::Node;
use crate::common::errors::KafkaError;
use crate::common::requests::{MetadataResponse, RequestHeader};
use crate::metadata_updater::MetadataUpdaterContext;

/// A simple implementation of `MetadataUpdater` that returns the cluster
/// nodes set via the constructor or via `set_nodes`.
///
/// This is useful in cases where automatic metadata updates are not
/// required. An example is controller/broker communication.
///
/// **Java's contract**: this class is *not* thread-safe.
pub struct ManualMetadataUpdater {
    nodes: Vec<Node>,
}

impl ManualMetadataUpdater {
    /// Mirrors `new ManualMetadataUpdater()`.
    pub fn new() -> Self {
        ManualMetadataUpdater { nodes: Vec::new() }
    }

    /// Mirrors `new ManualMetadataUpdater(List<Node>)`.
    pub fn with_nodes(nodes: Vec<Node>) -> Self {
        ManualMetadataUpdater { nodes }
    }

    /// Mirrors `setNodes(List<Node>)`.
    pub fn set_nodes(&mut self, nodes: Vec<Node>) {
        self.nodes = nodes;
    }
}

impl Default for ManualMetadataUpdater {
    fn default() -> Self {
        Self::new()
    }
}

impl MetadataUpdater for ManualMetadataUpdater {
    fn fetch_nodes(&self) -> Vec<Node> {
        // Java returns a defensive copy (`new ArrayList<>(nodes)`); we
        // mirror that with `.clone()`.
        self.nodes.clone()
    }

    fn is_update_due(&self, _now: i64) -> bool {
        false
    }

    fn maybe_update(&mut self, _context: &mut dyn MetadataUpdaterContext, _now: i64) -> i64 {
        i64::MAX
    }

    fn handle_server_disconnect(&mut self, _now: i64, _node_id: i32, _maybe_auth_error: Option<KafkaError>) {
        // We don't fail the broker on failures. There should be sufficient
        // information from the NetworkClient logs to indicate the reason
        // for the failure.
    }

    fn handle_failed_request(&mut self, _now: i64, _maybe_fatal_error: Option<KafkaError>) {
        // Do nothing
    }

    fn handle_successful_response(
        &mut self,
        _request_header: &RequestHeader,
        _now: i64,
        _metadata_response: MetadataResponse,
    ) {
        // Do nothing
    }

    fn close(&mut self) {}
}

#[cfg(test)]
mod tests {
    //! `ManualMetadataUpdater.java` has no dedicated test file; the
    //! Java code path is exercised via `NetworkClient` integration
    //! tests. These tests cover the trivial accessors so a regression
    //! does not slip through.

    use super::*;
    use crate::LeastLoadedNode;
    use crate::common::requests::MetadataRequestBuilder;
    use crate::metadata_recovery_strategy::MetadataRecoveryStrategy;
    use std::sync::Arc;

    /// No-op [`MetadataUpdaterContext`] for tests of impls that do not
    /// touch the context — i.e. [`ManualMetadataUpdater`] which returns
    /// `i64::MAX` from `maybe_update`.
    struct NoopContext;
    impl MetadataUpdaterContext for NoopContext {
        fn least_loaded_node(&mut self, _now: i64, _nodes: &[Node]) -> LeastLoadedNode {
            LeastLoadedNode::new(None, false)
        }
        fn can_send_request(&self, _node_id: i32, _now: i64) -> bool {
            false
        }
        fn can_connect(&self, _node_id: i32, _now: i64) -> bool {
            false
        }
        fn is_connecting(&self, _node_id: i32) -> bool {
            false
        }
        fn initiate_connect(&mut self, _node: &Node, _now: i64) {}
        fn send_internal_metadata_request(
            &mut self,
            _builder: MetadataRequestBuilder,
            _node_id_label: Arc<str>,
            _now: i64,
        ) {
        }
        fn reconnect_backoff_ms(&self) -> i64 {
            50
        }
        fn default_request_timeout_ms(&self) -> i32 {
            30_000
        }
        fn metadata_recovery_strategy(&self) -> MetadataRecoveryStrategy {
            MetadataRecoveryStrategy::None
        }
    }

    #[test]
    fn default_has_no_nodes() {
        let updater = ManualMetadataUpdater::new();
        assert!(updater.fetch_nodes().is_empty());
        assert!(!updater.is_update_due(0));
        let mut ctx = NoopContext;
        assert_eq!(
            MetadataUpdater::maybe_update(&mut ManualMetadataUpdater::new(), &mut ctx, 0),
            i64::MAX
        );
    }

    #[test]
    fn fetch_nodes_returns_copy() {
        let nodes = vec![Node::new(1, "h".into(), 9092), Node::new(2, "h".into(), 9093)];
        let updater = ManualMetadataUpdater::with_nodes(nodes.clone());
        let fetched = updater.fetch_nodes();
        assert_eq!(fetched.len(), 2);
        assert_eq!(fetched[0].id(), 1);
        assert_eq!(fetched[1].id(), 2);
    }

    #[test]
    fn set_nodes_replaces_state() {
        let mut updater = ManualMetadataUpdater::new();
        updater.set_nodes(vec![Node::new(7, "h".into(), 1234)]);
        let fetched = updater.fetch_nodes();
        assert_eq!(fetched.len(), 1);
        assert_eq!(fetched[0].id(), 7);
    }
}
