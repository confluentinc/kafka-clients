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

use std::collections::HashMap;

use crate::common::KafkaFuture;
use crate::common::TopicPartitionReplica;

/// The result of the `Admin::alter_replica_log_dirs` call.
///
/// To retrieve the detailed result per specified [`TopicPartitionReplica`], use
/// [`values`](Self::values). To retrieve the overall result only, use
/// [`all`](Self::all).
#[derive(Debug)]
#[doc(alias = "org.apache.kafka.clients.admin.AlterReplicaLogDirsResult")]
pub struct AlterReplicaLogDirsResult {
    futures: HashMap<TopicPartitionReplica, KafkaFuture<()>>,
}

impl AlterReplicaLogDirsResult {
    /// Creates a new result from the per-replica futures.
    #[doc(alias = "org.apache.kafka.clients.admin.AlterReplicaLogDirsResult#AlterReplicaLogDirsResult")]
    pub(crate) fn new(futures: HashMap<TopicPartitionReplica, KafkaFuture<()>>) -> Self {
        Self { futures }
    }

    /// Returns a map from [`TopicPartitionReplica`] to [`KafkaFuture`] which holds the status of
    /// individual replica movement. To check the result of individual replica movement, call
    /// [`KafkaFuture::get`] from the value contained in the returned map. If there is no error,
    /// it will return silently; if not, it returns an error like the following:
    ///
    /// - `Error::is_cluster_authorization_error`: Authorization failed.
    ///   (`CLUSTER_AUTHORIZATION_FAILED`, 31)
    /// - `Error::is_invalid_topic_error`: The specified topic name is too long.
    ///   (`INVALID_TOPIC_EXCEPTION`, 17)
    /// - `Error::is_log_dir_not_found_error`: The specified log directory is not found in the
    ///   broker. (`LOG_DIR_NOT_FOUND`, 57)
    /// - `Error::is_replica_not_available_error`: The replica does not exist on the broker.
    ///   (`REPLICA_NOT_AVAILABLE`, 9)
    /// - `Error::is_kafka_storage_error`: Disk error occurred. (`KAFKA_STORAGE_ERROR`, 56)
    /// - `Error::is_unknown_server_error`: Unknown. (`UNKNOWN_SERVER_ERROR`, -1)
    ///
    /// # Translation notes
    ///
    /// [`KafkaFuture::get`] returns the cause itself: it has no cancellation, interruption or
    /// execution wrapper.
    #[doc(alias = "org.apache.kafka.clients.admin.AlterReplicaLogDirsResult#values")]
    pub fn values(&self) -> &HashMap<TopicPartitionReplica, KafkaFuture<()>> {
        &self.futures
    }

    /// Returns a future which succeeds if all the replica movements have
    /// succeeded, otherwise yields the first error described in
    /// [`values`](Self::values).
    #[doc(alias = "org.apache.kafka.clients.admin.AlterReplicaLogDirsResult#all")]
    pub fn all(&self) -> KafkaFuture<()> {
        KafkaFuture::all_of(self.futures.values().cloned().collect())
    }
}
