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

//! AlterReplicaLogDirs response handling.
//!
//! Corresponds to `org.apache.kafka.common.requests.AlterReplicaLogDirsResponse`.
//!
//! Possible error codes: `LogDirNotFound`, `KafkaStorageError`,
//! `ReplicaNotAvailable`, `UnknownServerError`.

use std::collections::HashMap;
use std::io;

use crate::alter_replica_log_dirs_response_data::AlterReplicaLogDirsResponseData;
use crate::common::protocol::{ApiKeys, Errors, Readable};

use super::abstract_response::update_error_counts;

/// An AlterReplicaLogDirs response.
///
/// Corresponds to `org.apache.kafka.common.requests.AlterReplicaLogDirsResponse`.
#[derive(Debug, Clone)]
pub struct AlterReplicaLogDirsResponse {
    data: AlterReplicaLogDirsResponseData,
}

impl AlterReplicaLogDirsResponse {
    /// Creates a new `AlterReplicaLogDirsResponse` from the underlying data.
    pub fn new(data: AlterReplicaLogDirsResponseData) -> Self {
        Self { data }
    }

    /// Returns the API key for this response.
    pub fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::ALTER_REPLICA_LOG_DIRS
    }

    /// Returns a reference to the underlying data.
    pub fn data(&self) -> &AlterReplicaLogDirsResponseData {
        &self.data
    }

    /// Returns a mutable reference to the underlying data.
    pub(crate) fn data_mut(&mut self) -> &mut AlterReplicaLogDirsResponseData {
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

    /// Returns the error counts aggregated across all partition results
    /// (mirrors `AlterReplicaLogDirsResponse.errorCounts`).
    pub fn error_counts(&self) -> HashMap<Errors, i32> {
        let mut counts = HashMap::new();
        for topic_result in &self.data.results {
            for partition_result in &topic_result.partitions {
                update_error_counts(&mut counts, Errors::for_code(partition_result.error_code));
            }
        }
        counts
    }

    /// Parses an `AlterReplicaLogDirsResponse` from a readable buffer at the
    /// given version.
    ///
    /// # Errors
    ///
    /// Returns an error if parsing fails.
    pub fn parse(readable: &mut dyn Readable, version: i16) -> io::Result<Self> {
        let data = AlterReplicaLogDirsResponseData::read(readable, version)?;
        Ok(Self::new(data))
    }

    /// Whether the client should throttle on this response (v1+).
    pub fn should_client_throttle(&self, version: i16) -> bool {
        version >= 1
    }
}

impl std::fmt::Display for AlterReplicaLogDirsResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "AlterReplicaLogDirsResponse(data={:?})", self.data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::alter_replica_log_dirs_response_data::{
        AlterReplicaLogDirPartitionResult, AlterReplicaLogDirTopicResult,
    };

    fn partition(index: i32, error: Errors) -> AlterReplicaLogDirPartitionResult {
        let mut p = AlterReplicaLogDirPartitionResult::new();
        p.set_partition_index(index);
        p.set_error_code(error.code());
        p
    }

    fn topic(name: &str, partitions: Vec<AlterReplicaLogDirPartitionResult>) -> AlterReplicaLogDirTopicResult {
        let mut t = AlterReplicaLogDirTopicResult::new();
        t.set_topic_name(name.to_string());
        t.set_partitions(partitions);
        t
    }

    /// Mirrors `AlterReplicaLogDirsResponseTest.testErrorCounts`.
    #[test]
    fn test_error_counts() {
        let mut data = AlterReplicaLogDirsResponseData::new();
        data.set_results(vec![
            topic("t0", vec![partition(0, Errors::LogDirNotFound), partition(1, Errors::None)]),
            topic("t1", vec![partition(0, Errors::LogDirNotFound)]),
        ]);
        let counts = AlterReplicaLogDirsResponse::new(data).error_counts();
        assert_eq!(counts.len(), 2);
        assert_eq!(counts.get(&Errors::LogDirNotFound), Some(&2));
        assert_eq!(counts.get(&Errors::None), Some(&1));
    }

    #[test]
    fn should_client_throttle_v1_threshold() {
        let response = AlterReplicaLogDirsResponse::new(AlterReplicaLogDirsResponseData::new());
        assert!(!response.should_client_throttle(0));
        assert!(response.should_client_throttle(1));
    }
}
