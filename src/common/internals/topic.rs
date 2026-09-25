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

#![allow(dead_code)]
//! Topic name utilities.
//!
//! Corresponds to `org.apache.kafka.common.internals.Topic`.

/// Topic name utilities.
///
/// This is a namespace for topic-related utility functions, matching the Java
/// `org.apache.kafka.common.internals.Topic` class.
pub struct Topic;

impl Topic {
    /// Consumer offsets internal topic name.
    pub const GROUP_METADATA_TOPIC_NAME: &'static str = "__consumer_offsets";

    /// Transaction state internal topic name.
    pub const TRANSACTION_STATE_TOPIC_NAME: &'static str = "__transaction_state";

    /// Share group state internal topic name.
    pub const SHARE_GROUP_STATE_TOPIC_NAME: &'static str = "__share_group_state";

    /// Cluster metadata internal topic name.
    pub const CLUSTER_METADATA_TOPIC_NAME: &'static str = "__cluster_metadata";

    /// Legal characters for Kafka topic names.
    pub const LEGAL_CHARS: &'static str = "[a-zA-Z0-9._-]";

    /// Maximum topic name length.
    const MAX_NAME_LENGTH: usize = 249;

    /// Set of internal topic names.
    const INTERNAL_TOPICS: &'static [&'static str] = &[
        Self::GROUP_METADATA_TOPIC_NAME,
        Self::TRANSACTION_STATE_TOPIC_NAME,
        Self::SHARE_GROUP_STATE_TOPIC_NAME,
    ];

    /// Returns `true` if the topic is an internal Kafka topic.
    pub fn is_internal(topic: &str) -> bool {
        Self::INTERNAL_TOPICS.contains(&topic)
    }

    /// Validates a topic name, returning an error message if invalid.
    pub fn detect_invalid_topic(name: &str) -> Option<String> {
        if name.is_empty() {
            return Some("the empty string is not allowed".to_string());
        }
        if name == "." {
            return Some("'.' is not allowed".to_string());
        }
        if name == ".." {
            return Some("'..' is not allowed".to_string());
        }
        if name.len() > Self::MAX_NAME_LENGTH {
            return Some(format!(
                "the length of '{}' is longer than the max allowed length {}",
                name,
                Self::MAX_NAME_LENGTH
            ));
        }
        if !Self::contains_valid_pattern(name) {
            return Some(format!(
                "'{}' contains one or more characters other than ASCII alphanumerics, '.', '_' and '-'",
                name
            ));
        }
        None
    }

    /// Returns `true` if the topic name is valid.
    pub fn is_valid(name: &str) -> bool {
        Self::detect_invalid_topic(name).is_none()
    }

    /// Checks if a topic name contains collision characters ('.' or '_').
    pub fn has_collision_chars(topic: &str) -> bool {
        topic.contains('_') || topic.contains('.')
    }

    /// Unifies collision characters by replacing '.' with '_'.
    pub fn unify_collision_chars(topic: &str) -> String {
        topic.replace('.', "_")
    }

    /// Returns `true` if the two topic names collide due to '.' and '_' equivalence.
    pub fn has_collision(topic_a: &str, topic_b: &str) -> bool {
        Self::unify_collision_chars(topic_a) == Self::unify_collision_chars(topic_b)
    }

    /// Valid characters for Kafka topics are ASCII alphanumerics, '.', '_', and '-'.
    fn contains_valid_pattern(topic: &str) -> bool {
        topic
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || c == b'.' || c == b'_' || c == b'-')
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_internal() {
        assert!(Topic::is_internal(Topic::GROUP_METADATA_TOPIC_NAME));
        assert!(Topic::is_internal(Topic::TRANSACTION_STATE_TOPIC_NAME));
        assert!(Topic::is_internal(Topic::SHARE_GROUP_STATE_TOPIC_NAME));
        assert!(!Topic::is_internal("my-topic"));
        assert!(!Topic::is_internal(Topic::CLUSTER_METADATA_TOPIC_NAME));
    }

    #[test]
    fn test_is_valid() {
        assert!(Topic::is_valid("valid-topic"));
        assert!(Topic::is_valid("valid_topic"));
        assert!(Topic::is_valid("valid.topic"));
        assert!(!Topic::is_valid(""));
        assert!(!Topic::is_valid("."));
        assert!(!Topic::is_valid(".."));
        assert!(!Topic::is_valid("topic with spaces"));
    }

    #[test]
    fn test_has_collision() {
        assert!(Topic::has_collision("a.b", "a_b"));
        assert!(!Topic::has_collision("a.b", "a.c"));
    }
}
