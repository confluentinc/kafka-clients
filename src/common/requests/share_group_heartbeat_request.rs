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

//! `ShareGroupHeartbeat` request handling (KIP-932).
//!
//! Corresponds to `org.apache.kafka.common.requests.ShareGroupHeartbeatRequest`.

use std::io;

use crate::common::protocol::{ApiKeys, Errors, Readable};
use crate::share_group_heartbeat_request_data::ShareGroupHeartbeatRequestData;
use crate::share_group_heartbeat_response_data::ShareGroupHeartbeatResponseData;

use super::ConcreteRequest;
use super::ConcreteResponse;
use super::RequestBuilder;
use super::ShareGroupHeartbeatResponse;

/// A member epoch of `-1` means that the member wants to leave the group.
///
/// Corresponds to `ShareGroupHeartbeatRequest.LEAVE_GROUP_MEMBER_EPOCH`.
pub const LEAVE_GROUP_MEMBER_EPOCH: i32 = -1;

/// A member epoch of `0` means that the member wants to join the group.
///
/// Corresponds to `ShareGroupHeartbeatRequest.JOIN_GROUP_MEMBER_EPOCH`.
pub const JOIN_GROUP_MEMBER_EPOCH: i32 = 0;

/// A `ShareGroupHeartbeat` request.
///
/// Corresponds to `org.apache.kafka.common.requests.ShareGroupHeartbeatRequest`.
#[derive(Debug, Clone)]
pub struct ShareGroupHeartbeatRequest {
    data: ShareGroupHeartbeatRequestData,
    version: i16,
}

impl ShareGroupHeartbeatRequest {
    /// Creates a new `ShareGroupHeartbeatRequest` from data and version.
    pub fn new(data: ShareGroupHeartbeatRequestData, version: i16) -> Self {
        Self { data, version }
    }

    /// Returns a reference to the underlying data.
    pub fn data(&self) -> &ShareGroupHeartbeatRequestData {
        &self.data
    }

    /// Returns a mutable reference to the underlying data.
    pub(crate) fn data_mut(&mut self) -> &mut ShareGroupHeartbeatRequestData {
        &mut self.data
    }

    /// Returns the API version of this request.
    pub fn version(&self) -> i16 {
        self.version
    }

    /// Returns the API key for this request.
    pub fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::SHARE_GROUP_HEARTBEAT
    }

    /// Builds the canonical error response for this request, matching Java's
    /// `ShareGroupHeartbeatRequest.getErrorResponse(throttleTimeMs, Throwable)`.
    pub fn get_error_response(&self, throttle_time_ms: i32, error: &Errors) -> ConcreteResponse {
        let mut data = ShareGroupHeartbeatResponseData::new();
        data.set_throttle_time_ms(throttle_time_ms).set_error_code(error.code());
        ConcreteResponse::ShareGroupHeartbeat(ShareGroupHeartbeatResponse::new(data))
    }

    /// Parses a `ShareGroupHeartbeatRequest` from a readable buffer at the given
    /// version.
    ///
    /// # Errors
    ///
    /// Returns an error if parsing fails.
    pub fn parse(readable: &mut dyn Readable, version: i16) -> io::Result<Self> {
        let data = ShareGroupHeartbeatRequestData::read(readable, version)?;
        Ok(Self::new(data, version))
    }
}

impl std::fmt::Display for ShareGroupHeartbeatRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ShareGroupHeartbeatRequest(version={}, data={})", self.version, self.data)
    }
}

/// Builder for [`ShareGroupHeartbeatRequest`].
///
/// Corresponds to `ShareGroupHeartbeatRequest.Builder` in Java.
#[derive(Debug, Clone)]
pub struct ShareGroupHeartbeatRequestBuilder {
    data: ShareGroupHeartbeatRequestData,
    oldest_allowed_version: i16,
    latest_allowed_version: i16,
}

impl ShareGroupHeartbeatRequestBuilder {
    /// Creates a builder wrapping the given data with the full supported
    /// version range.
    pub fn new(data: ShareGroupHeartbeatRequestData) -> Self {
        Self {
            data,
            oldest_allowed_version: ApiKeys::SHARE_GROUP_HEARTBEAT.oldest_version(),
            latest_allowed_version: ApiKeys::SHARE_GROUP_HEARTBEAT.latest_version(),
        }
    }

    /// Returns a reference to the underlying data.
    pub fn data(&self) -> &ShareGroupHeartbeatRequestData {
        &self.data
    }
}

impl RequestBuilder for ShareGroupHeartbeatRequestBuilder {
    fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::SHARE_GROUP_HEARTBEAT
    }

    fn oldest_allowed_version(&self) -> i16 {
        self.oldest_allowed_version
    }

    fn latest_allowed_version(&self) -> i16 {
        self.latest_allowed_version
    }

    fn build_version(&mut self, version: i16) -> io::Result<ConcreteRequest> {
        Ok(ConcreteRequest::ShareGroupHeartbeat(ShareGroupHeartbeatRequest::new(
            self.data.clone(),
            version,
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_constants() {
        assert_eq!(LEAVE_GROUP_MEMBER_EPOCH, -1);
        assert_eq!(JOIN_GROUP_MEMBER_EPOCH, 0);
    }

    /// Byte-level wire-encoding test against a known vector (DoD §3). Verifies
    /// the flexible v1 `ShareGroupHeartbeatRequest` body encodes exactly as the
    /// Kafka wire spec requires (compact strings, int32, compact-null fields,
    /// empty tagged-field buffer) — a wrong-but-round-trip-consistent encoding
    /// would still fail here.
    #[test]
    fn test_wire_encoding_known_vector() {
        let mut data = ShareGroupHeartbeatRequestData::new();
        data.set_group_id("g".to_string());
        data.set_member_id("m".to_string());
        data.set_member_epoch(0);
        let mut req = ConcreteRequest::ShareGroupHeartbeat(ShareGroupHeartbeatRequest::new(data, 1));
        let serialized = req.serialize().expect("serialize body");
        let expected: Vec<u8> = vec![
            0x02, 0x67, // GroupId: compact string len=1 (encoded len+1=2), 'g'
            0x02, 0x6d, // MemberId: compact string len=1, 'm'
            0x00, 0x00, 0x00, 0x00, // MemberEpoch: int32 = 0 (big-endian)
            0x00, // RackId: compact nullable string, null -> unsigned varint 0
            0x00, // SubscribedTopicNames: compact nullable array, null -> 0
            0x00, // empty tagged-field buffer
        ];
        assert_eq!(serialized.buffer(), expected.as_slice());
    }

    #[test]
    fn test_build_and_get_error_response() {
        let mut data = ShareGroupHeartbeatRequestData::new();
        data.set_group_id("G1".to_string());
        let mut builder = ShareGroupHeartbeatRequestBuilder::new(data);
        let built = builder.build_version(0).expect("build v0");
        match built {
            ConcreteRequest::ShareGroupHeartbeat(req) => {
                assert_eq!(req.version(), 0);
                let resp = req.get_error_response(100, &Errors::NotCoordinator);
                match resp {
                    ConcreteResponse::ShareGroupHeartbeat(r) => {
                        assert_eq!(r.throttle_time_ms(), 100);
                        assert_eq!(r.error(), Errors::NotCoordinator);
                    },
                    other => panic!("expected ShareGroupHeartbeat response, got {other:?}"),
                }
            },
            other => panic!("expected ShareGroupHeartbeat request, got {other:?}"),
        }
    }
}
