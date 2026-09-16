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

//! `LeaveGroup` request handling.
//!
//! Corresponds to `org.apache.kafka.common.requests.LeaveGroupRequest`.
//!
//! Wraps the auto-generated [`LeaveGroupRequestData`] and exposes a
//! [`LeaveGroupRequestBuilder`] used by the admin client's
//! `removeMembersFromConsumerGroup` path.

use std::io;

use crate::LeaveGroupRequestData;
use crate::LeaveGroupResponseData;
use crate::common::protocol::{ApiKeys, Errors, Readable};
use crate::leave_group_request_data::MemberIdentity;

use super::ConcreteResponse;
use super::LeaveGroupResponse;
use super::abstract_request::{ConcreteRequest, RequestBuilder};

/// A `LeaveGroup` request.
///
/// Corresponds to `org.apache.kafka.common.requests.LeaveGroupRequest`.
#[derive(Debug, Clone)]
pub struct LeaveGroupRequest {
    data: LeaveGroupRequestData,
    version: i16,
}

impl LeaveGroupRequest {
    /// Creates a new `LeaveGroupRequest` from data and version.
    ///
    /// Mirrors Java's constructor `LeaveGroupRequest(LeaveGroupRequestData, short)`.
    pub fn new(data: LeaveGroupRequestData, version: i16) -> Self {
        Self { data, version }
    }

    /// Returns a reference to the underlying data.
    pub fn data(&self) -> &LeaveGroupRequestData {
        &self.data
    }

    /// Returns a mutable reference to the underlying data.
    pub(crate) fn data_mut(&mut self) -> &mut LeaveGroupRequestData {
        &mut self.data
    }

    /// Returns the API version of this request.
    pub fn version(&self) -> i16 {
        self.version
    }

    /// Returns the API key for this request.
    pub fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::LEAVE_GROUP
    }

    /// The leaving members. Before version 3, the request is single-member and
    /// the member is reconstructed from the top-level `member_id`. Mirrors
    /// Java's `LeaveGroupRequest.members()`.
    pub fn members(&self) -> Vec<MemberIdentity> {
        if self.version <= 2 {
            let mut member = MemberIdentity::new();
            member.set_member_id(self.data.member_id.clone());
            vec![member]
        } else {
            self.data.members.clone()
        }
    }

    /// Builds the canonical error response for this request, matching Java's
    /// `LeaveGroupRequest.getErrorResponse(int, Throwable)`: a top-level error
    /// code (throttle time is populated for version 1+).
    pub fn get_error_response(&self, throttle_time_ms: i32, error: &Errors) -> ConcreteResponse {
        let mut data = LeaveGroupResponseData::new();
        data.set_error_code(error.code());
        if self.version >= 1 {
            data.set_throttle_time_ms(throttle_time_ms);
        }
        ConcreteResponse::LeaveGroup(LeaveGroupResponse::new(data))
    }

    /// Parses a `LeaveGroupRequest` from a readable buffer at the given version.
    ///
    /// # Errors
    ///
    /// Returns an error if parsing fails.
    pub fn parse(readable: &mut dyn Readable, version: i16) -> io::Result<Self> {
        let data = LeaveGroupRequestData::read(readable, version)?;
        Ok(Self::new(data, version))
    }
}

impl std::fmt::Display for LeaveGroupRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "LeaveGroupRequest(version={}, data={})", self.version, self.data)
    }
}

/// Builder for [`LeaveGroupRequest`].
///
/// Corresponds to `LeaveGroupRequest.Builder` in Java: it carries the group id
/// and the leaving member identities, then chooses the wire shape based on the
/// negotiated version (single-member below v3, batched at v3+).
#[derive(Debug, Clone)]
pub struct LeaveGroupRequestBuilder {
    group_id: String,
    members: Vec<MemberIdentity>,
}

impl LeaveGroupRequestBuilder {
    /// Creates a builder for the given group id and leaving members.
    ///
    /// Mirrors Java's `LeaveGroupRequest.Builder(String, List<MemberIdentity>)`.
    pub fn new(group_id: String, members: Vec<MemberIdentity>) -> Self {
        Self { group_id, members }
    }

    /// Returns the group id.
    pub fn group_id(&self) -> &str {
        &self.group_id
    }

    /// Returns the leaving members.
    pub fn members(&self) -> &[MemberIdentity] {
        &self.members
    }
}

impl RequestBuilder for LeaveGroupRequestBuilder {
    fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::LEAVE_GROUP
    }

    fn oldest_allowed_version(&self) -> i16 {
        ApiKeys::LEAVE_GROUP.oldest_version()
    }

    fn latest_allowed_version(&self) -> i16 {
        ApiKeys::LEAVE_GROUP.latest_version()
    }

    fn build_version(&mut self, version: i16) -> io::Result<ConcreteRequest> {
        if version < self.oldest_allowed_version() || version > self.latest_allowed_version() {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                format!(
                    "Cannot build LeaveGroup request with version {version} (allowed range: {}..={})",
                    self.oldest_allowed_version(),
                    self.latest_allowed_version(),
                ),
            ));
        }
        // Java's `Builder` validates non-empty members in its constructor; the
        // wire-wrapper builder is infallible on construction (see the
        // `RequestBuilder` contract), so the check happens at build time.
        if self.members.is_empty() {
            return Err(io::Error::other("leaving members should not be empty"));
        }

        let mut data = LeaveGroupRequestData::new();
        data.set_group_id(self.group_id.clone());
        // Starting from version 3, all leave group requests are batched.
        if version >= 3 {
            data.set_members(self.members.clone());
        } else {
            if self.members.len() != 1 {
                return Err(io::Error::new(
                    io::ErrorKind::Unsupported,
                    format!(
                        "Version {version} leave group request only supports single member instance than {} members",
                        self.members.len()
                    ),
                ));
            }
            data.set_member_id(self.members[0].member_id.clone());
        }
        Ok(ConcreteRequest::LeaveGroup(LeaveGroupRequest::new(data, version)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn member(member_id: &str, group_instance_id: Option<&str>, reason: Option<&str>) -> MemberIdentity {
        let mut m = MemberIdentity::new();
        m.set_member_id(member_id.to_string())
            .set_group_instance_id(group_instance_id.map(str::to_string))
            .set_reason(reason.map(str::to_string));
        m
    }

    /// `build_version` at v3+ carries the batched members through unchanged.
    #[test]
    fn build_version_batched_carries_members() {
        let mut builder = LeaveGroupRequestBuilder::new("g".to_string(), vec![member("m1", Some("gii-1"), Some("r"))]);
        let version = ApiKeys::LEAVE_GROUP.latest_version();
        match builder.build_version(version).unwrap() {
            ConcreteRequest::LeaveGroup(r) => {
                assert_eq!(r.version(), version);
                assert_eq!(r.data().group_id, "g");
                assert_eq!(r.data().members.len(), 1);
                assert_eq!(r.data().members[0].member_id, "m1");
            },
            other => panic!("expected LeaveGroup, got {}", other.api_key().name()),
        }
    }

    /// `build_version` below v3 collapses a single member to the top-level
    /// `member_id`.
    #[test]
    fn build_version_single_member_below_v3() {
        let mut builder = LeaveGroupRequestBuilder::new("g".to_string(), vec![member("m1", Some("gii-1"), None)]);
        match builder.build_version(2).unwrap() {
            ConcreteRequest::LeaveGroup(r) => {
                assert_eq!(r.data().member_id, "m1");
                assert!(r.data().members.is_empty());
            },
            other => panic!("expected LeaveGroup, got {}", other.api_key().name()),
        }
    }

    /// Below v3 with multiple members, the build fails with an unsupported
    /// version error (Java throws `UnsupportedVersionException`).
    #[test]
    fn build_version_below_v3_multi_member_errors() {
        let mut builder =
            LeaveGroupRequestBuilder::new("g".to_string(), vec![member("m1", None, None), member("m2", None, None)]);
        assert!(builder.build_version(2).is_err());
    }

    /// Empty members is rejected at build time.
    #[test]
    fn build_version_empty_members_errors() {
        let mut builder = LeaveGroupRequestBuilder::new("g".to_string(), vec![]);
        assert!(builder.build_version(ApiKeys::LEAVE_GROUP.latest_version()).is_err());
    }

    /// `build_version` rejects a version outside the supported range.
    #[test]
    fn build_version_out_of_range_returns_err() {
        let mut builder = LeaveGroupRequestBuilder::new("g".to_string(), vec![member("m1", None, None)]);
        let too_new = ApiKeys::LEAVE_GROUP.latest_version() + 1;
        assert!(builder.build_version(too_new).is_err());
    }

    /// Byte-level wire-encoding check for the v3 (non-flexible batched) request,
    /// including the `MemberIdentity` sub-struct.
    /// Field-by-field (big-endian, non-flexible framing, no `Reason` before v5):
    ///   group_id "g": string len 1 -> 0x00 0x01, 0x67
    ///   members: array len 1 -> 0x00 0x00 0x00 0x01
    ///     member_id "m": string len 1 -> 0x00 0x01, 0x6D
    ///     group_instance_id "i": string len 1 -> 0x00 0x01, 0x69
    #[test]
    fn serialize_known_byte_vector_v3() {
        let mut builder = LeaveGroupRequestBuilder::new("g".to_string(), vec![member("m", Some("i"), None)]);
        let mut req = builder.build_version(3).unwrap();
        let expected: &[u8] = &[
            0x00, 0x01, 0x67, // group_id "g"
            0x00, 0x00, 0x00, 0x01, // members len 1
            0x00, 0x01, 0x6D, // member_id "m"
            0x00, 0x01, 0x69, // group_instance_id "i"
        ];
        assert_eq!(req.serialize().unwrap().into_buffer().as_slice(), expected);
    }

    /// Byte-level wire-encoding check for the v5 (flexible batched) request,
    /// exercising the compact/nullable-string framing, member/top-level tagged
    /// fields, and — the headline feature of this phase — the v5 `Reason` field
    /// (KIP-800; `LeaveGroupRequest.json`: `Reason` `versions: "5+"`,
    /// `flexibleVersions: "4+"`). Hand-computed against the spec.
    /// Field-by-field (little-detail, flexible framing uses unsigned-varint
    /// length prefixes of n+1):
    ///   group_id "g": compact string len 1 -> 0x02, then 0x67
    ///   members: compact array len 1 -> 0x02
    ///     member_id "m": compact string len 1 -> 0x02, then 0x6D
    ///     group_instance_id "i": compact (nullable) string len 1 -> 0x02, then 0x69
    ///     reason "r": compact (nullable) string len 1 -> 0x02, then 0x72
    ///     member tagged fields: 0x00
    ///   top-level tagged fields: 0x00
    #[test]
    fn serialize_known_byte_vector_v5_flexible() {
        let mut builder = LeaveGroupRequestBuilder::new("g".to_string(), vec![member("m", Some("i"), Some("r"))]);
        let mut req = builder.build_version(5).unwrap();
        let expected: &[u8] = &[
            0x02, 0x67, // group_id "g" (compact string, len n+1 = 2)
            0x02, // members compact array len 1 (=n+1)
            0x02, 0x6D, // member_id "m" (compact string)
            0x02, 0x69, // group_instance_id "i" (compact nullable string, non-null)
            0x02, 0x72, // reason "r" (compact nullable string, non-null) -- v5 field
            0x00, // member tagged fields
            0x00, // top-level tagged fields
        ];
        assert_eq!(req.serialize().unwrap().into_buffer().as_slice(), expected);
    }

    /// `get_error_response` sets a top-level error code and (v1+) throttle time.
    #[test]
    fn get_error_response_sets_top_level_error() {
        let mut data = LeaveGroupRequestData::new();
        data.set_group_id("g".to_string());
        let request = LeaveGroupRequest::new(data, ApiKeys::LEAVE_GROUP.latest_version());
        let response = request.get_error_response(42, &Errors::CoordinatorNotAvailable);
        match response {
            ConcreteResponse::LeaveGroup(r) => {
                assert_eq!(r.data().error_code, Errors::CoordinatorNotAvailable.code());
                assert_eq!(r.throttle_time_ms(), 42);
            },
            other => panic!("expected LeaveGroup response, got {}", other.api_key().name()),
        }
    }
}
