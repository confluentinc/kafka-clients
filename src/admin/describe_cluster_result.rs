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

//! The result of `Admin::describe_cluster`.
//!
//! Corresponds to `org.apache.kafka.clients.admin.DescribeClusterResult`.

use std::collections::BTreeSet;

use crate::common::acl::AclOperation;
use crate::common::{KafkaFuture, Node};

/// The result of the `Admin::describe_cluster` call.
///
/// Corresponds to `org.apache.kafka.clients.admin.DescribeClusterResult`.
pub struct DescribeClusterResult {
    nodes: KafkaFuture<Vec<Node>>,
    controller: KafkaFuture<Option<Node>>,
    cluster_id: KafkaFuture<String>,
    authorized_operations: KafkaFuture<Option<BTreeSet<AclOperation>>>,
}

impl DescribeClusterResult {
    /// Creates a new result from the four per-attribute futures.
    pub(crate) fn new(
        nodes: KafkaFuture<Vec<Node>>,
        controller: KafkaFuture<Option<Node>>,
        cluster_id: KafkaFuture<String>,
        authorized_operations: KafkaFuture<Option<BTreeSet<AclOperation>>>,
    ) -> Self {
        Self { nodes, controller, cluster_id, authorized_operations }
    }

    /// Returns a future which yields a collection of nodes in the cluster.
    pub fn nodes(&self) -> KafkaFuture<Vec<Node>> {
        self.nodes.clone()
    }

    /// Returns a future which yields the current controller node in the cluster.
    ///
    /// The value is `None` if there is no current controller.
    pub fn controller(&self) -> KafkaFuture<Option<Node>> {
        self.controller.clone()
    }

    /// Returns a future which yields the id of the cluster.
    pub fn cluster_id(&self) -> KafkaFuture<String> {
        self.cluster_id.clone()
    }

    /// Returns a future which yields the authorized operations of the cluster.
    ///
    /// The value is `None` if the operations were not requested or the broker
    /// omitted them.
    pub fn authorized_operations(&self) -> KafkaFuture<Option<BTreeSet<AclOperation>>> {
        self.authorized_operations.clone()
    }
}
