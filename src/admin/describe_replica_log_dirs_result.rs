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

//! The result of `Admin::describe_replica_log_dirs`.
//!
//! Corresponds to `org.apache.kafka.clients.admin.DescribeReplicaLogDirsResult`.

use crate::common::requests::DescribeLogDirsResponse;
use std::collections::HashMap;

use crate::common::KafkaFuture;
use crate::common::TopicPartitionReplica;

/// The result of the `Admin::describe_replica_log_dirs` call.
///
/// Corresponds to `org.apache.kafka.clients.admin.DescribeReplicaLogDirsResult`.
pub struct DescribeReplicaLogDirsResult {
    futures: HashMap<TopicPartitionReplica, KafkaFuture<ReplicaLogDirInfo>>,
}

impl DescribeReplicaLogDirsResult {
    /// Creates a new result from the per-replica futures.
    pub(crate) fn new(futures: HashMap<TopicPartitionReplica, KafkaFuture<ReplicaLogDirInfo>>) -> Self {
        Self { futures }
    }

    /// Returns a map from replica to a future which can be used to check the log
    /// directory information of individual replicas.
    pub fn values(&self) -> &HashMap<TopicPartitionReplica, KafkaFuture<ReplicaLogDirInfo>> {
        &self.futures
    }

    /// Returns a future which succeeds if log directory information of all
    /// replicas is available.
    ///
    /// Corresponds to `DescribeReplicaLogDirsResult.all`.
    pub fn all(&self) -> KafkaFuture<HashMap<TopicPartitionReplica, ReplicaLogDirInfo>> {
        let entries: Vec<(TopicPartitionReplica, KafkaFuture<ReplicaLogDirInfo>)> =
            self.futures.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
        KafkaFuture::join_map(entries)
    }
}

/// Log directory information for a replica of a partition on a given broker.
///
/// Corresponds to `DescribeReplicaLogDirsResult.ReplicaLogDirInfo`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReplicaLogDirInfo {
    current_replica_log_dir: Option<String>,
    current_replica_offset_lag: i64,
    future_replica_log_dir: Option<String>,
    future_replica_offset_lag: i64,
}

impl ReplicaLogDirInfo {
    /// Creates a fully specified `ReplicaLogDirInfo`.
    pub(crate) fn new(
        current_replica_log_dir: Option<String>,
        current_replica_offset_lag: i64,
        future_replica_log_dir: Option<String>,
        future_replica_offset_lag: i64,
    ) -> Self {
        Self {
            current_replica_log_dir,
            current_replica_offset_lag,
            future_replica_log_dir,
            future_replica_offset_lag,
        }
    }

    /// The current log directory of the replica of this partition on the given
    /// broker. `None` if no replica is found for this partition on the broker.
    pub fn current_replica_log_dir(&self) -> Option<&str> {
        self.current_replica_log_dir.as_deref()
    }

    /// Defined as `max(HW of partition - LEO of the replica, 0)`.
    pub fn current_replica_offset_lag(&self) -> i64 {
        self.current_replica_offset_lag
    }

    /// The future log directory of the replica of this partition on the given
    /// broker. `None` if the replica of this partition is not being moved to
    /// another log directory on the given broker.
    pub fn future_replica_log_dir(&self) -> Option<&str> {
        self.future_replica_log_dir.as_deref()
    }

    /// The LEO of the replica minus the LEO of the future log of this replica in
    /// the destination log directory. `-1` if either there is no replica for
    /// this partition or the replica is not being moved to another log
    /// directory on the given broker.
    pub fn future_replica_offset_lag(&self) -> i64 {
        self.future_replica_offset_lag
    }
}

impl Default for ReplicaLogDirInfo {
    /// The no-argument default, mirroring Java's `ReplicaLogDirInfo()`:
    /// `(null, INVALID_OFFSET_LAG, null, INVALID_OFFSET_LAG)`.
    fn default() -> Self {
        Self::new(
            None,
            DescribeLogDirsResponse::INVALID_OFFSET_LAG,
            None,
            DescribeLogDirsResponse::INVALID_OFFSET_LAG,
        )
    }
}

impl std::fmt::Display for ReplicaLogDirInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.future_replica_log_dir {
            Some(future) => write!(
                f,
                "(currentReplicaLogDir={}, futureReplicaLogDir={}, futureReplicaOffsetLag={})",
                self.current_replica_log_dir.as_deref().unwrap_or("null"),
                future,
                self.future_replica_offset_lag
            ),
            None => write!(
                f,
                "ReplicaLogDirInfo(currentReplicaLogDir={})",
                self.current_replica_log_dir.as_deref().unwrap_or("null")
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_matches_java() {
        let info = ReplicaLogDirInfo::default();
        assert_eq!(info.current_replica_log_dir(), None);
        assert_eq!(info.current_replica_offset_lag(), -1);
        assert_eq!(info.future_replica_log_dir(), None);
        assert_eq!(info.future_replica_offset_lag(), -1);
    }

    #[test]
    fn display_with_future() {
        let info = ReplicaLogDirInfo::new(Some("/cur".to_string()), 0, Some("/fut".to_string()), 5);
        assert_eq!(
            info.to_string(),
            "(currentReplicaLogDir=/cur, futureReplicaLogDir=/fut, futureReplicaOffsetLag=5)"
        );
    }

    #[test]
    fn display_without_future() {
        let info = ReplicaLogDirInfo::new(Some("/cur".to_string()), 0, None, -1);
        assert_eq!(info.to_string(), "ReplicaLogDirInfo(currentReplicaLogDir=/cur)");
    }
}
