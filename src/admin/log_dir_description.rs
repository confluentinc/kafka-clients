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

use crate::common::requests::DescribeLogDirsResponse;
use std::collections::HashMap;

use crate::common::Error;
use crate::common::TopicPartition;

use super::ReplicaInfo;

/// A description of a log directory on a particular broker.
///
/// Corresponds to `org.apache.kafka.clients.admin.LogDirDescription`.
#[derive(Clone, Debug)]
pub struct LogDirDescription {
    error: Option<Error>,
    replica_infos: HashMap<TopicPartition, ReplicaInfo>,
    total_bytes: Option<i64>,
    usable_bytes: Option<i64>,
    is_cordoned: bool,
}

impl LogDirDescription {
    /// Creates a new `LogDirDescription` with unknown volume sizes.
    ///
    /// Corresponds to the two-argument `LogDirDescription(ApiException, Map)`
    /// constructor.
    pub fn new(error: Option<Error>, replica_infos: HashMap<TopicPartition, ReplicaInfo>) -> Self {
        Self::new_total_bytes_usable_bytes_is_cordoned(
            error,
            replica_infos,
            DescribeLogDirsResponse::UNKNOWN_VOLUME_BYTES,
            DescribeLogDirsResponse::UNKNOWN_VOLUME_BYTES,
            false,
        )
    }

    /// Creates a new `LogDirDescription` with the given raw volume sizes and no
    /// cordoning. A raw value of `UNKNOWN_VOLUME_BYTES` (`-1`) maps to `None`.
    ///
    /// Corresponds to the four-argument
    /// `LogDirDescription(ApiException, Map, long, long)` constructor.
    pub fn new_total_bytes_usable_bytes(
        error: Option<Error>,
        replica_infos: HashMap<TopicPartition, ReplicaInfo>,
        total_bytes: i64,
        usable_bytes: i64,
    ) -> Self {
        Self::new_total_bytes_usable_bytes_is_cordoned(error, replica_infos, total_bytes, usable_bytes, false)
    }

    /// Creates a new `LogDirDescription` with the given raw volume sizes and
    /// cordoning flag. A raw value of `UNKNOWN_VOLUME_BYTES` (`-1`) maps to
    /// `None`.
    ///
    /// Corresponds to the five-argument
    /// `LogDirDescription(ApiException, Map, long, long, boolean)` constructor
    /// (KIP-1066).
    pub fn new_total_bytes_usable_bytes_is_cordoned(
        error: Option<Error>,
        replica_infos: HashMap<TopicPartition, ReplicaInfo>,
        total_bytes: i64,
        usable_bytes: i64,
        is_cordoned: bool,
    ) -> Self {
        Self {
            error,
            replica_infos,
            total_bytes: (total_bytes != DescribeLogDirsResponse::UNKNOWN_VOLUME_BYTES).then_some(total_bytes),
            usable_bytes: (usable_bytes != DescribeLogDirsResponse::UNKNOWN_VOLUME_BYTES).then_some(usable_bytes),
            is_cordoned,
        }
    }

    /// Returns the error if the log directory is offline or an error occurred,
    /// otherwise `None`.
    ///
    /// - `KafkaStorageError` — the log directory is offline.
    /// - `UnknownServerError` — the server experienced an unexpected error.
    pub fn error(&self) -> Option<&Error> {
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

    /// Whether this log directory is cordoned or not.
    pub fn is_cordoned(&self) -> bool {
        self.is_cordoned
    }
}

impl std::fmt::Display for LogDirDescription {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "LogDirDescription(replicaInfos={:?}, error={:?}, totalBytes={:?}, usableBytes={:?}, isCordoned={})",
            self.replica_infos, self.error, self.total_bytes, self.usable_bytes, self.is_cordoned
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::Errors;

    #[test]
    fn unknown_volume_bytes_maps_to_none() {
        let d = LogDirDescription::new(None, HashMap::new());
        assert!(d.error().is_none());
        assert_eq!(d.total_bytes(), None);
        assert_eq!(d.usable_bytes(), None);
        assert!(!d.is_cordoned());
    }

    #[test]
    fn cordoned_flag_is_carried() {
        let d = LogDirDescription::new_total_bytes_usable_bytes_is_cordoned(None, HashMap::new(), -1, -1, true);
        assert!(d.is_cordoned());
        assert_eq!(d.total_bytes(), None);
        // The four-arg constructor defaults `is_cordoned` to false.
        let d2 = LogDirDescription::new_total_bytes_usable_bytes(None, HashMap::new(), 1, 2);
        assert!(!d2.is_cordoned());
    }

    #[test]
    fn known_volume_bytes_are_present() {
        let d = LogDirDescription::new_total_bytes_usable_bytes(
            Some(Error::new(Errors::KafkaStorageError)),
            HashMap::new(),
            123,
            456,
        );
        assert_eq!(d.error().unwrap().error(), Errors::KafkaStorageError);
        assert_eq!(d.total_bytes(), Some(123));
        assert_eq!(d.usable_bytes(), Some(456));
    }
}
