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

//! A description of a log directory on a particular broker.
//!
//! Corresponds to `org.apache.kafka.clients.admin.LogDirDescription`.

use std::collections::HashMap;

use crate::common::KafkaError;
use crate::common::TopicPartition;
use crate::common::requests::describe_log_dirs_response::UNKNOWN_VOLUME_BYTES;

use super::ReplicaInfo;

/// A description of a log directory on a particular broker.
///
/// Corresponds to `org.apache.kafka.clients.admin.LogDirDescription`.
#[derive(Clone, Debug)]
pub struct LogDirDescription {
    error: Option<KafkaError>,
    replica_infos: HashMap<TopicPartition, ReplicaInfo>,
    total_bytes: Option<i64>,
    usable_bytes: Option<i64>,
}

impl LogDirDescription {
    /// Creates a new `LogDirDescription` with unknown volume sizes.
    ///
    /// Corresponds to the two-argument `LogDirDescription(ApiException, Map)`
    /// constructor.
    pub fn new(error: Option<KafkaError>, replica_infos: HashMap<TopicPartition, ReplicaInfo>) -> Self {
        Self::with_volume_bytes(error, replica_infos, UNKNOWN_VOLUME_BYTES, UNKNOWN_VOLUME_BYTES)
    }

    /// Creates a new `LogDirDescription` with the given raw volume sizes. A raw
    /// value of `UNKNOWN_VOLUME_BYTES` (`-1`) maps to `None`.
    ///
    /// Corresponds to the four-argument
    /// `LogDirDescription(ApiException, Map, long, long)` constructor.
    pub fn with_volume_bytes(
        error: Option<KafkaError>,
        replica_infos: HashMap<TopicPartition, ReplicaInfo>,
        total_bytes: i64,
        usable_bytes: i64,
    ) -> Self {
        Self {
            error,
            replica_infos,
            total_bytes: (total_bytes != UNKNOWN_VOLUME_BYTES).then_some(total_bytes),
            usable_bytes: (usable_bytes != UNKNOWN_VOLUME_BYTES).then_some(usable_bytes),
        }
    }

    /// Returns the error if the log directory is offline or an error occurred,
    /// otherwise `None`.
    ///
    /// - `KafkaStorageError` — the log directory is offline.
    /// - `UnknownServerError` — the server experienced an unexpected error.
    pub fn error(&self) -> Option<&KafkaError> {
        self.error.as_ref()
    }

    /// A map from topic partition to replica information for that partition in
    /// this log directory.
    pub fn replica_infos(&self) -> &HashMap<TopicPartition, ReplicaInfo> {
        &self.replica_infos
    }

    /// The total size in bytes of the volume this log directory is on, or `None`
    /// if the broker did not return a value.
    pub fn total_bytes(&self) -> Option<i64> {
        self.total_bytes
    }

    /// The usable size in bytes of the volume this log directory is on, or
    /// `None` if the broker did not return a value.
    pub fn usable_bytes(&self) -> Option<i64> {
        self.usable_bytes
    }
}

impl std::fmt::Display for LogDirDescription {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "LogDirDescription(replicaInfos={:?}, error={:?}, totalBytes={:?}, usableBytes={:?})",
            self.replica_infos, self.error, self.total_bytes, self.usable_bytes
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::protocol::Errors;

    #[test]
    fn unknown_volume_bytes_maps_to_none() {
        let d = LogDirDescription::new(None, HashMap::new());
        assert!(d.error().is_none());
        assert_eq!(d.total_bytes(), None);
        assert_eq!(d.usable_bytes(), None);
    }

    #[test]
    fn known_volume_bytes_are_present() {
        let d = LogDirDescription::with_volume_bytes(
            Some(KafkaError::new(Errors::KafkaStorageError)),
            HashMap::new(),
            123,
            456,
        );
        assert_eq!(d.error().unwrap().error(), Errors::KafkaStorageError);
        assert_eq!(d.total_bytes(), Some(123));
        assert_eq!(d.usable_bytes(), Some(456));
    }
}
