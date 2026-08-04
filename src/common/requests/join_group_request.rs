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
//! `UNKNOWN_MEMBER_ID` sentinel and the shared reason-truncation utility.

/// The sentinel member id used before the broker assigns one.
///
/// Corresponds to `JoinGroupRequest.UNKNOWN_MEMBER_ID`.
pub const UNKNOWN_MEMBER_ID: &str = "";

/// The maximum length (in characters) of a join/leave-group `reason` before it
/// is truncated on the wire. Corresponds to the `255` literal in
/// `JoinGroupRequest.maybeTruncateReason`.
pub const MAX_REASON_LENGTH: usize = 255;

/// Ensures that the provided `reason` remains within [`MAX_REASON_LENGTH`]
/// characters, truncating it if it exceeds the threshold.
///
/// Corresponds to `JoinGroupRequest.maybeTruncateReason`. Java measures length
/// in UTF-16 code units; we measure in Unicode scalar values, which agrees for
/// the ASCII reasons the admin client produces and never splits a code point.
pub fn maybe_truncate_reason(reason: &str) -> String {
    if reason.chars().count() > MAX_REASON_LENGTH {
        reason.chars().take(MAX_REASON_LENGTH).collect()
    } else {
        reason.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A short reason is returned unchanged.
    #[test]
    fn short_reason_unchanged() {
        assert_eq!(maybe_truncate_reason("short reason"), "short reason");
    }

    /// A reason exactly at the limit is returned unchanged.
    #[test]
    fn reason_at_limit_unchanged() {
        let reason = "x".repeat(MAX_REASON_LENGTH);
        assert_eq!(maybe_truncate_reason(&reason), reason);
    }

    /// A reason over the limit is truncated to exactly `MAX_REASON_LENGTH`.
    #[test]
    fn long_reason_truncated() {
        let reason = "y".repeat(MAX_REASON_LENGTH + 16);
        let truncated = maybe_truncate_reason(&reason);
        assert_eq!(truncated.chars().count(), MAX_REASON_LENGTH);
        assert_eq!(truncated, "y".repeat(MAX_REASON_LENGTH));
    }
}
