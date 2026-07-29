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

//! `LeaveGroup` response handling.
//!
//! Corresponds to `org.apache.kafka.common.requests.LeaveGroupResponse`.
//!
//! Top-level error codes:
//! - `COORDINATOR_LOAD_IN_PROGRESS`
//! - `COORDINATOR_NOT_AVAILABLE`
//! - `NOT_COORDINATOR`
//! - `GROUP_AUTHORIZATION_FAILED`
//!
//! Member-level error codes:
//! - `FENCED_INSTANCE_ID`
//! - `UNKNOWN_MEMBER_ID`

use std::collections::HashMap;
use std::io;

use crate::common::protocol::{ApiKeys, Errors, Readable};
use crate::leave_group_response_data::{LeaveGroupResponseData, MemberResponse};

use super::abstract_response::update_error_counts;

/// A `LeaveGroup` response.
///
/// Corresponds to `org.apache.kafka.common.requests.LeaveGroupResponse`.
#[derive(Debug, Clone)]
pub struct LeaveGroupResponse {
    data: LeaveGroupResponseData,
}

impl LeaveGroupResponse {
    /// Creates a new `LeaveGroupResponse` from the underlying data.
    ///
    /// Mirrors Java's constructor `LeaveGroupResponse(LeaveGroupResponseData)`.
    pub fn new(data: LeaveGroupResponseData) -> Self {
        Self { data }
    }

    /// Returns the API key for this response.
    pub fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::LEAVE_GROUP
    }

    /// Returns a reference to the underlying data.
    pub fn data(&self) -> &LeaveGroupResponseData {
        &self.data
    }

    /// Returns a mutable reference to the underlying data.
    pub(crate) fn data_mut(&mut self) -> &mut LeaveGroupResponseData {
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

    /// Returns the per-member responses.
    pub fn member_responses(&self) -> &[MemberResponse] {
        &self.data.members
    }

    /// Returns the top-level error code as an [`Errors`].
    ///
    /// Mirrors Java's `LeaveGroupResponse.topLevelError()`.
    pub fn top_level_error(&self) -> Errors {
        Errors::for_code(self.data.error_code)
    }

    /// Returns the effective error: the top-level error if set, otherwise the
    /// first non-`NONE` member-level error, otherwise `NONE`.
    ///
    /// Mirrors Java's `LeaveGroupResponse.error()`.
    pub fn error(&self) -> Errors {
        Self::get_error(self.top_level_error(), &self.data.members)
    }

    fn get_error(top_level_error: Errors, member_responses: &[MemberResponse]) -> Errors {
        if top_level_error != Errors::None {
            top_level_error
        } else {
            for member_response in member_responses {
                let member_error = Errors::for_code(member_response.error_code);
                if member_error != Errors::None {
                    return member_error;
                }
            }
            Errors::None
        }
    }

    /// Returns the error counts aggregated across the top-level and member-level
    /// errors. Mirrors Java's `errorCounts`.
    pub fn error_counts(&self) -> HashMap<Errors, i32> {
        let mut counts = HashMap::new();
        update_error_counts(&mut counts, Errors::for_code(self.data.error_code));
        for member_response in &self.data.members {
            update_error_counts(&mut counts, Errors::for_code(member_response.error_code));
        }
        counts
    }

    /// Parses a `LeaveGroupResponse` from a readable buffer at the given
    /// version.
    ///
    /// # Errors
    ///
    /// Returns an error if parsing fails.
    pub fn parse(readable: &mut dyn Readable, version: i16) -> io::Result<Self> {
        let data = LeaveGroupResponseData::read(readable, version)?;
        Ok(Self::new(data))
    }

    /// Whether the client should throttle on this response (version 2+).
    pub fn should_client_throttle(&self, version: i16) -> bool {
        version >= 2
    }
}

impl std::fmt::Display for LeaveGroupResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn member(member_id: &str, group_instance_id: Option<&str>, error: Errors) -> MemberResponse {
        let mut m = MemberResponse::new();
        m.set_member_id(member_id.to_string())
            .set_group_instance_id(group_instance_id.map(str::to_string))
            .set_error_code(error.code());
        m
    }

    /// `error` returns the top-level error when set.
    #[test]
    fn error_prefers_top_level() {
        let mut data = LeaveGroupResponseData::new();
        data.set_error_code(Errors::GroupAuthorizationFailed.code());
        data.set_members(vec![member("m", None, Errors::UnknownMemberId)]);
        let response = LeaveGroupResponse::new(data);
        assert_eq!(response.top_level_error(), Errors::GroupAuthorizationFailed);
        assert_eq!(response.error(), Errors::GroupAuthorizationFailed);
    }

    /// `error` falls back to the first member error when the top level is NONE.
    #[test]
    fn error_falls_back_to_member_error() {
        let mut data = LeaveGroupResponseData::new();
        data.set_error_code(Errors::None.code());
        data.set_members(vec![
            member("m1", None, Errors::None),
            member("m2", None, Errors::FencedInstanceId),
        ]);
        let response = LeaveGroupResponse::new(data);
        assert_eq!(response.top_level_error(), Errors::None);
        assert_eq!(response.error(), Errors::FencedInstanceId);
    }

    /// `error_counts` aggregates the top-level and member-level error codes.
    #[test]
    fn error_counts_aggregates_top_and_member() {
        let mut data = LeaveGroupResponseData::new();
        data.set_error_code(Errors::None.code());
        data.set_members(vec![
            member("m1", None, Errors::None),
            member("m2", None, Errors::UnknownMemberId),
        ]);
        let response = LeaveGroupResponse::new(data);
        let counts = response.error_counts();
        // NONE appears twice: top-level + one member.
        assert_eq!(counts.get(&Errors::None).copied().unwrap_or(0), 2);
        assert_eq!(counts.get(&Errors::UnknownMemberId).copied().unwrap_or(0), 1);
    }

    /// Byte-level wire-decoding check for the v3 (non-flexible batched)
    /// response, including the `MemberResponse` sub-struct.
    /// Field-by-field (big-endian):
    ///   throttle_time_ms 0 -> 0x00 0x00 0x00 0x00
    ///   error_code 0 -> 0x00 0x00
    ///   members: array len 1 -> 0x00 0x00 0x00 0x01
    ///     member_id "m": string len 1 -> 0x00 0x01, 0x6D
    ///     group_instance_id "i": string len 1 -> 0x00 0x01, 0x69
    ///     error_code 25 (UNKNOWN_MEMBER_ID) -> 0x00 0x19
    #[test]
    fn parse_known_byte_vector_v3() {
        let bytes = vec![
            0x00, 0x00, 0x00, 0x00, // throttle_time_ms 0
            0x00, 0x00, // error_code 0
            0x00, 0x00, 0x00, 0x01, // members len 1
            0x00, 0x01, 0x6D, // member_id "m"
            0x00, 0x01, 0x69, // group_instance_id "i"
            0x00, 0x19, // error_code 25
        ];
        let mut readable = crate::common::ByteBufferAccessor::from_bytes(bytes);
        let response = LeaveGroupResponse::parse(&mut readable, 3).unwrap();
        assert_eq!(response.top_level_error(), Errors::None);
        assert_eq!(response.member_responses().len(), 1);
        assert_eq!(response.member_responses()[0].member_id, "m");
        assert_eq!(response.member_responses()[0].group_instance_id.as_deref(), Some("i"));
        assert_eq!(response.error(), Errors::UnknownMemberId);
    }

    /// `should_client_throttle` returns false below v2 and true at v2+.
    #[test]
    fn should_client_throttle_by_version() {
        let response = LeaveGroupResponse::new(LeaveGroupResponseData::new());
        assert!(!response.should_client_throttle(1));
        assert!(response.should_client_throttle(2));
    }
}
