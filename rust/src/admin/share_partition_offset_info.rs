// Copyright 2026 Confluent Inc.
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

//! The offset and lag information for a share-partition.

/// This class is used to contain the offset and lag information for a share-partition.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
#[doc(alias = "org.apache.kafka.clients.admin.SharePartitionOffsetInfo")]
pub struct SharePartitionOffsetInfo {
    start_offset: i64,
    leader_epoch: Option<i32>,
    lag: Option<i64>,
}

impl SharePartitionOffsetInfo {
    /// Construct a new SharePartitionOffsetInfo.
    ///
    /// # Arguments
    ///
    /// - `start_offset` - The share-partition start offset. The start offset is the earliest
    ///   offset for in-flight records being evaluated for delivery to share consumers. Some
    ///   records after the start offset may already have completed delivery.
    /// - `leader_epoch` - The optional leader epoch of the share-partition
    /// - `lag` - The optional lag for the share-partition
    #[doc(alias = "org.apache.kafka.clients.admin.SharePartitionOffsetInfo#SharePartitionOffsetInfo")]
    pub fn new(start_offset: i64, leader_epoch: Option<i32>, lag: Option<i64>) -> Self {
        Self { start_offset, leader_epoch, lag }
    }

    /// Get the start offset for the share-partition. The start offset is the earliest offset for
    /// in-flight records being evaluated for delivery to share consumers. Some records after the
    /// start offset may already have completed delivery.
    ///
    /// # Returns
    ///
    /// The start offset of the partition read by the share group.
    #[doc(alias = "org.apache.kafka.clients.admin.SharePartitionOffsetInfo#startOffset")]
    pub fn start_offset(&self) -> i64 {
        self.start_offset
    }

    /// Get the leader epoch for the partition.
    ///
    /// # Returns
    ///
    /// The leader epoch of the partition.
    #[doc(alias = "org.apache.kafka.clients.admin.SharePartitionOffsetInfo#leaderEpoch")]
    pub fn leader_epoch(&self) -> Option<i32> {
        self.leader_epoch
    }

    /// Get the lag for the partition.
    ///
    /// # Returns
    ///
    /// The lag of the partition.
    #[doc(alias = "org.apache.kafka.clients.admin.SharePartitionOffsetInfo#lag")]
    pub fn lag(&self) -> Option<i64> {
        self.lag
    }
}

impl std::fmt::Display for SharePartitionOffsetInfo {
    /// Java's `toString()`: an absent leader epoch or lag prints as `null`.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let null_or = |value: Option<i64>| value.map_or_else(|| "null".to_string(), |v| v.to_string());
        write!(
            f,
            "SharePartitionOffsetInfo{{startOffset={}, leaderEpoch={}, lag={}}}",
            self.start_offset,
            null_or(self.leader_epoch.map(i64::from)),
            null_or(self.lag),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// New test, no Java original (Java has no `SharePartitionOffsetInfoTest`):
    /// `equals` compares the three fields.
    #[test]
    fn equality_compares_every_field() {
        let info = SharePartitionOffsetInfo::new(10, Some(1), Some(5));
        assert_eq!(info, SharePartitionOffsetInfo::new(10, Some(1), Some(5)));
        assert_ne!(info, SharePartitionOffsetInfo::new(11, Some(1), Some(5)));
        assert_ne!(info, SharePartitionOffsetInfo::new(10, None, Some(5)));
        assert_ne!(info, SharePartitionOffsetInfo::new(10, Some(1), None));
    }

    /// New test, no Java original: the getters return what was passed in.
    #[test]
    fn getters_return_the_constructor_arguments() {
        let info = SharePartitionOffsetInfo::new(10, Some(1), None);
        assert_eq!(info.start_offset(), 10);
        assert_eq!(info.leader_epoch(), Some(1));
        assert_eq!(info.lag(), None);
    }

    /// New test, no Java original: `toString()` follows Java's format.
    #[test]
    fn to_string_follows_java() {
        assert_eq!(
            SharePartitionOffsetInfo::new(10, Some(1), Some(5)).to_string(),
            "SharePartitionOffsetInfo{startOffset=10, leaderEpoch=1, lag=5}"
        );
        assert_eq!(
            SharePartitionOffsetInfo::new(10, None, None).to_string(),
            "SharePartitionOffsetInfo{startOffset=10, leaderEpoch=null, lag=null}"
        );
    }
}
