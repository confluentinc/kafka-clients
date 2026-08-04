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

//! The result of `Admin::alter_replica_log_dirs`.
//!
//! Corresponds to `org.apache.kafka.clients.admin.AlterReplicaLogDirsResult`.

use std::collections::HashMap;

use crate::common::KafkaFuture;
use crate::common::TopicPartitionReplica;

/// The result of the `Admin::alter_replica_log_dirs` call.
///
/// To retrieve the detailed result per specified [`TopicPartitionReplica`], use
/// [`values`](Self::values). To retrieve the overall result only, use
/// [`all`](Self::all).
///
/// Corresponds to `org.apache.kafka.clients.admin.AlterReplicaLogDirsResult`.
pub struct AlterReplicaLogDirsResult {
    futures: HashMap<TopicPartitionReplica, KafkaFuture<()>>,
}

impl AlterReplicaLogDirsResult {
    /// Creates a new result from the per-replica futures.
    pub(crate) fn new(futures: HashMap<TopicPartitionReplica, KafkaFuture<()>>) -> Self {
        Self { futures }
    }

    /// Returns a map from [`TopicPartitionReplica`] to a [`KafkaFuture`] which
    /// holds the status of an individual replica movement.
    ///
    /// Awaiting a value future returns silently on success; otherwise it yields
    /// one of `ClusterAuthorizationFailed`, `InvalidTopicException`,
    /// `LogDirNotFound`, `ReplicaNotAvailable`, `KafkaStorageError`, or
    /// `UnknownServerError`.
    pub fn values(&self) -> &HashMap<TopicPartitionReplica, KafkaFuture<()>> {
        &self.futures
    }

    /// Returns a future which succeeds if all the replica movements have
    /// succeeded, otherwise yields the first error described in
    /// [`values`](Self::values).
    ///
    /// Corresponds to `AlterReplicaLogDirsResult.all`.
    pub fn all(&self) -> KafkaFuture<()> {
        KafkaFuture::all_of(self.futures.values().cloned().collect())
    }
}
