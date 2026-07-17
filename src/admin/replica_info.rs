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

//! A description of a replica on a particular broker.
//!
//! Corresponds to `org.apache.kafka.clients.admin.ReplicaInfo`.

/// A description of a replica on a particular broker.
///
/// Corresponds to `org.apache.kafka.clients.admin.ReplicaInfo`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReplicaInfo {
    size: i64,
    offset_lag: i64,
    is_future: bool,
}

impl ReplicaInfo {
    /// Creates a new `ReplicaInfo`.
    pub fn new(size: i64, offset_lag: i64, is_future: bool) -> Self {
        Self { size, offset_lag, is_future }
    }

    /// The total size of the log segments in this replica in bytes.
    ///
    /// This value does not include the size of data stored in remote storage.
    pub fn size(&self) -> i64 {
        self.size
    }

    /// The lag of the log's LEO with respect to the partition's high watermark
    /// (if it is the current log for the partition) or the current replica's LEO
    /// (if it is the future log for the partition).
    pub fn offset_lag(&self) -> i64 {
        self.offset_lag
    }

    /// Whether this replica has been created by an `AlterReplicaLogDirsRequest`
    /// but has not yet replaced the current replica on the broker.
    pub fn is_future(&self) -> bool {
        self.is_future
    }
}

impl std::fmt::Display for ReplicaInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ReplicaInfo(size={}, offsetLag={}, isFuture={})",
            self.size, self.offset_lag, self.is_future
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accessors_and_display() {
        let info = ReplicaInfo::new(1234, 24, true);
        assert_eq!(info.size(), 1234);
        assert_eq!(info.offset_lag(), 24);
        assert!(info.is_future());
        assert_eq!(info.to_string(), "ReplicaInfo(size=1234, offsetLag=24, isFuture=true)");
    }
}
