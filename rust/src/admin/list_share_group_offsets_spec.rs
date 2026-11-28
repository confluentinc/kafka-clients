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

//! Specification of share group offsets to list.

use crate::common::TopicPartition;

/// Specification of share group offsets to list using `Admin::list_share_group_offsets`.
// `topic_partitions == None` includes all topic partitions of the group.
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
#[doc(alias = "org.apache.kafka.clients.admin.ListShareGroupOffsetsSpec")]
pub struct ListShareGroupOffsetsSpec {
    topic_partitions: Option<Vec<TopicPartition>>,
}

impl ListShareGroupOffsetsSpec {
    /// Creates a spec that lists all topic partitions of the group.
    #[doc(alias = "org.apache.kafka.clients.admin.ListShareGroupOffsetsSpec#ListShareGroupOffsetsSpec")]
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the topic partitions whose offsets are to be listed for a share group.
    ///
    /// # Arguments
    ///
    /// - `topic_partitions` - List of topic partitions to include, or `None` to include all
    ///   topic partitions.
    ///
    /// # Returns
    ///
    /// This ListShareGroupOffsetsSpec
    #[must_use]
    #[doc(alias = "org.apache.kafka.clients.admin.ListShareGroupOffsetsSpec#topicPartitions")]
    pub fn set_topic_partitions(mut self, topic_partitions: Option<Vec<TopicPartition>>) -> Self {
        self.topic_partitions = topic_partitions;
        self
    }

    /// Returns the topic partitions whose offsets are to be listed for a share group. `None`
    /// indicates that offsets of all partitions of the group are to be listed.
    #[doc(alias = "org.apache.kafka.clients.admin.ListShareGroupOffsetsSpec#topicPartitions")]
    pub fn topic_partitions(&self) -> Option<&[TopicPartition]> {
        self.topic_partitions.as_deref()
    }
}

impl std::fmt::Display for ListShareGroupOffsetsSpec {
    /// Java's `toString()`: the partitions as Java's `[t-0, t-1]`, or `null`.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.topic_partitions {
            Some(partitions) => write!(
                f,
                "ListShareGroupOffsetsSpec(topicPartitions=[{}])",
                partitions.iter().map(ToString::to_string).collect::<Vec<_>>().join(", ")
            ),
            None => f.write_str("ListShareGroupOffsetsSpec(topicPartitions=null)"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// New test, no Java original (Java has no `ListShareGroupOffsetsSpecTest`):
    /// a fresh spec lists every partition.
    #[test]
    fn defaults_to_all_partitions() {
        assert_eq!(ListShareGroupOffsetsSpec::new().topic_partitions(), None);
    }

    /// New test, no Java original: the setter stores the partitions, and
    /// `None` restores "all partitions".
    #[test]
    fn carries_topic_partitions() {
        let partitions = vec![TopicPartition::new("t", 1), TopicPartition::new("t", 0)];
        let spec = ListShareGroupOffsetsSpec::new().set_topic_partitions(Some(partitions.clone()));
        assert_eq!(spec.topic_partitions(), Some(partitions.as_slice()));
        assert_eq!(spec.set_topic_partitions(None).topic_partitions(), None);
    }

    /// New test, no Java original: `equals` compares the partition lists (in
    /// order, as Java's `List.equals`).
    #[test]
    fn equality_compares_the_partitions() {
        let spec =
            |partitions: Vec<TopicPartition>| ListShareGroupOffsetsSpec::new().set_topic_partitions(Some(partitions));
        let a = TopicPartition::new("t", 0);
        let b = TopicPartition::new("t", 1);
        assert_eq!(spec(vec![a.clone(), b.clone()]), spec(vec![a.clone(), b.clone()]));
        assert_ne!(spec(vec![a.clone(), b.clone()]), spec(vec![b, a.clone()]));
        assert_ne!(spec(vec![a]), ListShareGroupOffsetsSpec::new());
    }

    /// New test, no Java original: `toString()` follows Java's format.
    #[test]
    fn to_string_follows_java() {
        assert_eq!(
            ListShareGroupOffsetsSpec::new().to_string(),
            "ListShareGroupOffsetsSpec(topicPartitions=null)"
        );
        let spec = ListShareGroupOffsetsSpec::new()
            .set_topic_partitions(Some(vec![TopicPartition::new("t", 1), TopicPartition::new("u", 0)]));
        assert_eq!(spec.to_string(), "ListShareGroupOffsetsSpec(topicPartitions=[t-1, u-0])");
    }
}
