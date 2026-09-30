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

//! `JoinGroup` request helpers.
//!
//! Corresponds to `org.apache.kafka.common.requests.JoinGroupRequest`. Only the
//! members required by the admin client (`removeMembersFromConsumerGroup`) are
//! translated here: the classic-protocol `JoinGroupRequest` itself is out of
//! scope (see `.claude/rules/consumer-threading.md` §20). This file carries the
//! `UNKNOWN_MEMBER_ID` sentinel, the shared reason-truncation utility and the
//! `group.instance.id` validator used by `ConsumerConfig`.

use crate::common::Error;
use crate::common::errors::InvalidConfigurationError;
use crate::common::internals::Topic;

/// Translates the Java static-utility class `org.apache.kafka.common.requests.JoinGroupRequest`,
/// which has no instance state, so it becomes a unit struct hosting its
/// statics as associated items.
pub struct JoinGroupRequest;

impl JoinGroupRequest {
    /// The sentinel member id used before the broker assigns one.
    ///
    /// Corresponds to `JoinGroupRequest.UNKNOWN_MEMBER_ID`.
    pub const UNKNOWN_MEMBER_ID: &str = "";

    /// The maximum length (in characters) of a join/leave-group `reason` before it
    /// is truncated on the wire. Corresponds to the `255` literal in
    /// `JoinGroupRequest.maybeTruncateReason`.
    pub const MAX_REASON_LENGTH: usize = 255;

    /// Ensures that the provided `reason` remains within [`Self::MAX_REASON_LENGTH`]
    /// characters, truncating it if it exceeds the threshold.
    ///
    /// Corresponds to `JoinGroupRequest.maybeTruncateReason`. Java measures length
    /// in UTF-16 code units; we measure in Unicode scalar values, which agrees for
    /// the ASCII reasons the admin client produces and never splits a code point.
    pub fn maybe_truncate_reason(reason: &str) -> String {
        if reason.chars().count() > Self::MAX_REASON_LENGTH {
            reason.chars().take(Self::MAX_REASON_LENGTH).collect()
        } else {
            reason.to_string()
        }
    }

    /// Validates a `group.instance.id` with the topic-name rules.
    ///
    /// Corresponds to `JoinGroupRequest.validateGroupInstanceId`, which calls
    /// `Topic.validate(id, "Group instance id", ...)` and throws
    /// `InvalidConfigurationException` with the message
    /// `"Group instance id is invalid: <reason>"`. Here the `Topic.validate`
    /// callback form collapses into returning the error directly.
    pub fn validate_group_instance_id(id: &str) -> Result<(), Error> {
        match Topic::detect_invalid_topic(id) {
            Some(reason_invalid) => Err(Error::InvalidConfiguration(InvalidConfigurationError::new(format!(
                "Group instance id is invalid: {reason_invalid}"
            )))),
            None => Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Translated from `JoinGroupRequestTest.shouldAcceptValidGroupInstanceIds`.
    ///
    /// Java builds the max-length id with `TestUtils.randomString(249)`, i.e.
    /// 249 characters drawn from `LETTERS_AND_DIGITS`; cycling through that
    /// same alphabet gives a deterministic string of the same shape.
    #[test]
    fn should_accept_valid_group_instance_ids() {
        const LETTERS_AND_DIGITS: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
        let max_length_string: String = (0..249)
            .map(|i| LETTERS_AND_DIGITS[i % LETTERS_AND_DIGITS.len()] as char)
            .collect();
        let valid_group_instance_ids = [
            "valid",
            "INSTANCE",
            "gRoUp",
            "ar6",
            "VaL1d",
            "_0-9_.",
            "...",
            max_length_string.as_str(),
        ];

        for instance_id in valid_group_instance_ids {
            JoinGroupRequest::validate_group_instance_id(instance_id)
                .unwrap_or_else(|e| panic!("{instance_id:?} should be valid, got {e:?}"));
        }
    }

    /// Translated from `JoinGroupRequestTest.shouldThrowOnInvalidGroupInstanceIds`.
    #[test]
    fn should_throw_on_invalid_group_instance_ids() {
        let long_string = "a".repeat(250);
        let invalid_group_instance_ids = ["", "foo bar", "..", "foo:bar", "foo=bar", ".", long_string.as_str()];

        for instance_id in invalid_group_instance_ids {
            let err = JoinGroupRequest::validate_group_instance_id(instance_id).unwrap_err();
            assert!(
                matches!(err, Error::InvalidConfiguration(_)),
                "InvalidConfigurationError expected as instance id {instance_id:?} is invalid, got {err:?}"
            );
        }
    }

    /// Beyond Java's tests: the message is `Topic.validate`'s
    /// `"<thing> is invalid: <reason>"` form.
    #[test]
    fn invalid_group_instance_id_carries_java_message() {
        let err = JoinGroupRequest::validate_group_instance_id("..").unwrap_err();
        assert_eq!(err.message(), "Group instance id is invalid: '..' is not allowed");
    }

    /// A short reason is returned unchanged.
    #[test]
    fn short_reason_unchanged() {
        assert_eq!(JoinGroupRequest::maybe_truncate_reason("short reason"), "short reason");
    }

    /// A reason exactly at the limit is returned unchanged.
    #[test]
    fn reason_at_limit_unchanged() {
        let reason = "x".repeat(JoinGroupRequest::MAX_REASON_LENGTH);
        assert_eq!(JoinGroupRequest::maybe_truncate_reason(&reason), reason);
    }

    /// A reason over the limit is truncated to exactly `MAX_REASON_LENGTH`.
    #[test]
    fn long_reason_truncated() {
        let reason = "y".repeat(JoinGroupRequest::MAX_REASON_LENGTH + 16);
        let truncated = JoinGroupRequest::maybe_truncate_reason(&reason);
        assert_eq!(truncated.chars().count(), JoinGroupRequest::MAX_REASON_LENGTH);
        assert_eq!(truncated, "y".repeat(JoinGroupRequest::MAX_REASON_LENGTH));
    }
}
