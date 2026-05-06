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

//! Translation of `org.apache.kafka.common.internals.Topic`.
//!
//! Per CLAUDE.md rule 2 ("Constants MUST be exported only by the file
//! defining them"), constants here are `pub` (and the Java `public static
//! final` fields) but are accessed via `crate::common::internals::topic::*`.

use crate::common::errors::KafkaError;

/// Internal topic name for the consumer-offsets metadata. Mirrors
/// `Topic.GROUP_METADATA_TOPIC_NAME`.
pub const GROUP_METADATA_TOPIC_NAME: &str = "__consumer_offsets";

/// Internal topic name for the transaction-state log. Mirrors
/// `Topic.TRANSACTION_STATE_TOPIC_NAME`.
pub const TRANSACTION_STATE_TOPIC_NAME: &str = "__transaction_state";

/// Internal topic name for the share-group state log. Mirrors
/// `Topic.SHARE_GROUP_STATE_TOPIC_NAME`.
pub const SHARE_GROUP_STATE_TOPIC_NAME: &str = "__share_group_state";

/// Internal topic name for the KRaft cluster-metadata log. Mirrors
/// `Topic.CLUSTER_METADATA_TOPIC_NAME`.
pub const CLUSTER_METADATA_TOPIC_NAME: &str = "__cluster_metadata";

/// Java exposes a regex `LEGAL_CHARS` for documentation purposes. The actual
/// validator does a hand-rolled byte check (see [`contains_valid_pattern`]),
/// not regex matching.
pub const LEGAL_CHARS: &str = "[a-zA-Z0-9._-]";

const INTERNAL_TOPICS: &[&str] = &[
    GROUP_METADATA_TOPIC_NAME,
    TRANSACTION_STATE_TOPIC_NAME,
    SHARE_GROUP_STATE_TOPIC_NAME,
];

const MAX_NAME_LENGTH: usize = 249;

/// Validate `topic`. Returns `Ok(())` if the name is acceptable, otherwise
/// [`KafkaError::InvalidTopic`] with the same message Java would emit
/// (`"Topic name is invalid: <reason>"`).
pub fn validate(topic: &str) -> Result<(), KafkaError> {
    validate_with_prefix(topic, "Topic name")
}

/// Like [`validate`] but with a custom prefix in the error message.
/// Mirrors `Topic.validate(String, String, Consumer<String>)` (the consumer
/// is implied — we always raise `InvalidTopicException`).
pub fn validate_with_prefix(name: &str, log_prefix: &str) -> Result<(), KafkaError> {
    if let Some(reason) = detect_invalid_topic(name) {
        Err(KafkaError::InvalidTopic(format!("{log_prefix} is invalid: {reason}")))
    } else {
        Ok(())
    }
}

/// True iff the topic name passes validation.
pub fn is_valid(name: &str) -> bool {
    detect_invalid_topic(name).is_none()
}

/// True iff `topic` is one of the known Kafka-internal topics.
pub fn is_internal(topic: &str) -> bool {
    INTERNAL_TOPICS.contains(&topic)
}

/// True iff the topic contains any character that could collide with another
/// topic when metric names normalise `.` and `_` to the same character.
pub fn has_collision_chars(topic: &str) -> bool {
    topic.contains('_') || topic.contains('.')
}

/// Replace every `.` with `_` to normalise the name for collision-detection
/// purposes. Mirrors `Topic.unifyCollisionChars`.
pub fn unify_collision_chars(topic: &str) -> String {
    topic.replace('.', "_")
}

/// True iff `topic_a` and `topic_b` collide under [`unify_collision_chars`].
pub fn has_collision(topic_a: &str, topic_b: &str) -> bool {
    unify_collision_chars(topic_a) == unify_collision_chars(topic_b)
}

/// Hand-rolled validator for the topic name character set
/// `[a-zA-Z0-9._-]`. Mirrors `Topic.containsValidPattern` (Java skips
/// `Character.isLetterOrDigit` for performance — we get the same speedup
/// for free with byte-wise comparison).
pub fn contains_valid_pattern(topic: &str) -> bool {
    topic
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || b == b'.' || b == b'_' || b == b'-')
}

fn detect_invalid_topic(name: &str) -> Option<String> {
    if name.is_empty() {
        return Some("the empty string is not allowed".to_owned());
    }
    if name == "." {
        return Some("'.' is not allowed".to_owned());
    }
    if name == ".." {
        return Some("'..' is not allowed".to_owned());
    }
    // Java compares `String.length()` (UTF-16 code units) but the Kafka topic
    // name only allows ASCII characters, so byte length matches code-unit
    // length on every valid input.
    if name.len() > MAX_NAME_LENGTH {
        return Some(format!(
            "the length of '{name}' is longer than the max allowed length {MAX_NAME_LENGTH}"
        ));
    }
    if !contains_valid_pattern(name) {
        return Some(format!(
            "'{name}' contains one or more characters other than ASCII alphanumerics, '.', '_' and '-'"
        ));
    }
    None
}

#[cfg(test)]
mod tests {
    // Translation of `org.apache.kafka.common.internals.TopicTest`.

    use super::*;

    fn random_string(len: usize) -> String {
        // Java uses `TestUtils.randomString(249)`. We mirror that with a
        // 249-character ASCII string so the validator accepts it. Any
        // single-char repeat works.
        "a".repeat(len)
    }

    /// Java: `shouldAcceptValidTopicNames`.
    #[test]
    fn accepts_valid_topic_names() {
        let max_length_string = random_string(249);
        let valid: &[&str] = &[
            "valid",
            "TOPIC",
            "nAmEs",
            "ar6",
            "VaL1d",
            "_0-9_.",
            "...",
            &max_length_string,
        ];
        for name in valid {
            validate(name).unwrap_or_else(|e| panic!("expected valid: {name} ({e})"));
        }
    }

    /// Java: `shouldThrowOnInvalidTopicNames`.
    #[test]
    fn rejects_invalid_topic_names() {
        let long_string = "a".repeat(250);
        let invalid: &[&str] = &["", "foo bar", "..", "foo:bar", "foo=bar", ".", &long_string];
        for name in invalid {
            let err = validate(name).unwrap_err();
            assert!(matches!(err, KafkaError::InvalidTopic(_)), "for {name:?}: {err}");
        }
    }

    /// Java: `shouldRecognizeInvalidCharactersInTopicNames`.
    #[test]
    fn rejects_invalid_characters() {
        let invalid_chars = [
            '/', '\\', ',', '\u{0000}', ':', '"', '\'', ';', '*', '?', ' ', '\t', '\r', '\n', '=',
        ];
        for c in invalid_chars {
            let name = format!("Is {c}illegal");
            assert!(!contains_valid_pattern(&name), "expected invalid: {name:?}");
        }
    }

    /// Java: `testTopicHasCollisionChars`.
    #[test]
    fn has_collision_chars_recognises_period_and_underscore() {
        let false_topics = ["start", "end", "middle", "many"];
        let true_topics = [
            ".start", "end.", "mid.dle", ".ma.ny.", "_start", "end_", "mid_dle", "_ma_ny.",
        ];
        for t in false_topics {
            assert!(!has_collision_chars(t), "expected no collision: {t}");
        }
        for t in true_topics {
            assert!(has_collision_chars(t), "expected collision: {t}");
        }
    }

    /// Java: `testUnifyCollisionChars`.
    #[test]
    fn unify_collision_chars_replaces_period_only() {
        assert_eq!(unify_collision_chars("topic"), "topic");
        assert_eq!(unify_collision_chars(".topic"), "_topic");
        assert_eq!(unify_collision_chars("_topic"), "_topic");
        assert_eq!(unify_collision_chars("_.topic"), "__topic");
    }

    /// Java: `testTopicHasCollision`.
    #[test]
    fn topic_has_collision_with_period_underscore_pairs() {
        let period_first_middle_last_none = [".topic", "to.pic", "topic.", "topic"];
        let underscore_first_middle_last_none = ["_topic", "to_pic", "topic_", "topic"];

        // Self-collision
        for t in period_first_middle_last_none {
            assert!(has_collision(t, t));
        }
        for t in underscore_first_middle_last_none {
            assert!(has_collision(t, t));
        }

        // Same-position collision
        for i in 0..period_first_middle_last_none.len() {
            assert!(has_collision(
                period_first_middle_last_none[i],
                underscore_first_middle_last_none[i]
            ));
        }

        // Reverse: different positions should not collide.
        let underscore_reversed: Vec<_> = underscore_first_middle_last_none.iter().rev().collect();
        for i in 0..period_first_middle_last_none.len() {
            // Special case: when both are "topic" (self), they still collide
            // — but the reversed test only checks index pairs that were not
            // matching in the forward iteration.
            let a = period_first_middle_last_none[i];
            let b = *underscore_reversed[i];
            if a == "topic" && b == "topic" {
                continue;
            }
            assert!(!has_collision(a, b), "{a} and {b} should not collide");
        }
    }

    #[test]
    fn is_internal_recognises_known_topics() {
        assert!(is_internal(GROUP_METADATA_TOPIC_NAME));
        assert!(is_internal(TRANSACTION_STATE_TOPIC_NAME));
        assert!(is_internal(SHARE_GROUP_STATE_TOPIC_NAME));
        assert!(!is_internal(CLUSTER_METADATA_TOPIC_NAME));
        assert!(!is_internal("user_topic"));
    }
}
