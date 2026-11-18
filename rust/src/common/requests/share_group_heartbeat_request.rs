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

//! `ShareGroupHeartbeat` request handling (KIP-932).
//!
//! Corresponds to `org.apache.kafka.common.requests.ShareGroupHeartbeatRequest`.

use std::fmt;
use std::io;

use crate::ShareGroupHeartbeatRequestData;
use crate::ShareGroupHeartbeatResponseData;
use crate::common::protocol::{ApiKeys, Errors, Readable};

use super::AbstractRequest;
use super::ConcreteResponse;
use super::RequestBuilder;
use super::ShareGroupHeartbeatResponse;

/// A `ShareGroupHeartbeat` request.
///
/// Corresponds to `org.apache.kafka.common.requests.ShareGroupHeartbeatRequest`.
#[derive(Debug, Clone)]
#[doc(alias = "org.apache.kafka.common.requests.ShareGroupHeartbeatRequest")]
pub struct ShareGroupHeartbeatRequest {
    data: ShareGroupHeartbeatRequestData,
    version: i16,
}

impl ShareGroupHeartbeatRequest {
    /// A member epoch of `-1` means that the member wants to leave the group.
    pub const LEAVE_GROUP_MEMBER_EPOCH: i32 = -1;

    /// A member epoch of `0` means that the member wants to join the group.
    pub const JOIN_GROUP_MEMBER_EPOCH: i32 = 0;

    /// Creates a new `ShareGroupHeartbeatRequest` from data and version.
    #[doc(alias = "org.apache.kafka.common.requests.ShareGroupHeartbeatRequest#ShareGroupHeartbeatRequest")]
    pub fn new(data: ShareGroupHeartbeatRequestData, version: i16) -> Self {
        Self { data, version }
    }

    /// Returns a reference to the underlying data.
    #[doc(alias = "org.apache.kafka.common.requests.ShareGroupHeartbeatRequest#data")]
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

    /// Builds the error response for this request: the throttle time and the
    /// top-level error code.
    ///
    /// Translates `ShareGroupHeartbeatRequest.getErrorResponse(int, Throwable)`;
    /// the caller supplies `Errors.forException(e)`.
    #[doc(alias = "org.apache.kafka.common.requests.ShareGroupHeartbeatRequest#getErrorResponse")]
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
    #[doc(alias = "org.apache.kafka.common.requests.ShareGroupHeartbeatRequest#parse")]
    pub fn parse(readable: &mut dyn Readable, version: i16) -> io::Result<Self> {
        let data = ShareGroupHeartbeatRequestData::read(readable, version)?;
        Ok(Self::new(data, version))
    }
}

impl fmt::Display for ShareGroupHeartbeatRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.data)
    }
}

/// Builder for [`ShareGroupHeartbeatRequest`].
///
/// Corresponds to `ShareGroupHeartbeatRequest.Builder` in Java.
#[derive(Debug, Clone)]
#[doc(alias = "org.apache.kafka.common.requests.ShareGroupHeartbeatRequest$Builder")]
pub struct Builder {
    data: ShareGroupHeartbeatRequestData,
}

impl Builder {
    /// Creates a builder wrapping the given data.
    #[doc(alias = "org.apache.kafka.common.requests.ShareGroupHeartbeatRequest$Builder#Builder")]
    pub fn new(data: ShareGroupHeartbeatRequestData) -> Self {
        Self { data }
    }
}

impl fmt::Display for Builder {
    /// Matches Java's `Builder.toString()`: the data.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.data)
    }
}

impl RequestBuilder for Builder {
    fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::SHARE_GROUP_HEARTBEAT
    }

    fn oldest_allowed_version(&self) -> i16 {
        ApiKeys::SHARE_GROUP_HEARTBEAT.oldest_version()
    }

    fn latest_allowed_version(&self) -> i16 {
        // Mirrors Java's `super(ApiKeys)` → `Builder(apiKey, false)` →
        // `latestVersion(false)`: released versions only
        // (producer-transactions.md §12).
        ApiKeys::SHARE_GROUP_HEARTBEAT.latest_version_enable_unstable_last_version(false)
    }

    fn build_version(&mut self, version: i16) -> io::Result<AbstractRequest> {
        // Java's `Builder.build(short)` performs no validation for this request.
        Ok(AbstractRequest::ShareGroupHeartbeat(ShareGroupHeartbeatRequest::new(
            self.data.clone(),
            version,
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::protocol::ByteBufferAccessor;
    use crate::common::requests::request_response_test::{check_error_response, check_request};

    /// `RequestResponseTest.createShareGroupHeartbeatRequest(short)`.
    fn create_share_group_heartbeat_request(version: i16) -> ShareGroupHeartbeatRequest {
        let mut data = ShareGroupHeartbeatRequestData::new();
        data.set_group_id("group".to_string())
            .set_member_id("memberid".to_string())
            .set_member_epoch(10)
            .set_rack_id(Some("rackid".to_string()))
            .set_subscribed_topic_names(Some(vec!["foo".to_string(), "bar".to_string()]));
        match Builder::new(data).build_version(version).expect("build") {
            AbstractRequest::ShareGroupHeartbeat(request) => request,
            other => panic!("expected ShareGroupHeartbeat, got {other:?}"),
        }
    }

    #[test]
    fn test_constants() {
        assert_eq!(-1, ShareGroupHeartbeatRequest::LEAVE_GROUP_MEMBER_EPOCH);
        assert_eq!(0, ShareGroupHeartbeatRequest::JOIN_GROUP_MEMBER_EPOCH);
    }

    /// The builder's range is Java's `super(ApiKeys.SHARE_GROUP_HEARTBEAT)`:
    /// oldest to the latest released version.
    #[test]
    fn test_builder_version_range() {
        let builder = Builder::new(ShareGroupHeartbeatRequestData::new());
        assert_eq!(&ApiKeys::SHARE_GROUP_HEARTBEAT, builder.api_key());
        assert_eq!(1, builder.oldest_allowed_version());
        assert_eq!(1, builder.latest_allowed_version());
    }

    /// `RequestResponseTest.testSerialization`'s `SHARE_GROUP_HEARTBEAT` arm,
    /// at every version: `checkRequest(createShareGroupHeartbeatRequest(v))`,
    /// `checkErrorResponse(request, unknownServerException)` and
    /// `checkResponse(createShareGroupHeartbeatResponse(), v)` (the response
    /// half lives in `share_group_heartbeat_response.rs`).
    #[test]
    fn test_serialization_all_versions() {
        for version in ApiKeys::SHARE_GROUP_HEARTBEAT.all_versions() {
            let request = AbstractRequest::ShareGroupHeartbeat(create_share_group_heartbeat_request(version));
            check_request(&request);
            check_error_response(&request);
        }
    }

    #[test]
    fn test_get_error_response() {
        let request = create_share_group_heartbeat_request(1);
        match request.get_error_response(100, &Errors::NotCoordinator) {
            ConcreteResponse::ShareGroupHeartbeat(response) => {
                assert_eq!(100, response.throttle_time_ms());
                assert_eq!(Errors::NotCoordinator.code(), response.data().error_code);
                assert_eq!(None, response.data().error_message);
            },
            other => panic!("expected ShareGroupHeartbeat, got {other:?}"),
        }
    }

    /// Byte-level v1 encoding, derived by hand from `ShareGroupHeartbeatRequest.json`
    /// in field order. Every version is flexible: compact strings are
    /// `length + 1` then the bytes, a compact array is `count + 1` then the
    /// elements, and the struct ends with an empty tagged-field section.
    #[test]
    fn test_v1_encoding() {
        let mut data = ShareGroupHeartbeatRequestData::new();
        data.set_group_id("g".to_string())
            .set_member_id("m".to_string())
            .set_member_epoch(3)
            .set_rack_id(None)
            .set_subscribed_topic_names(Some(vec!["t".to_string()]));
        let mut request = Builder::new(data).build_version(1).unwrap();

        let mut expected = vec![
            0x02, b'g', // GroupId
            0x02, b'm', // MemberId
        ];
        expected.extend_from_slice(&3i32.to_be_bytes()); // MemberEpoch
        expected.push(0x00); // RackId: null
        expected.extend_from_slice(&[0x02, 0x02, b't']); // SubscribedTopicNames: ["t"]
        expected.push(0x00); // tagged fields
        assert_eq!(expected, request.serialize().unwrap().into_buffer());

        let parsed = ShareGroupHeartbeatRequest::parse(&mut ByteBufferAccessor::new(expected), 1).unwrap();
        assert_eq!(request_data(&request), parsed.data());
    }

    /// A null subscription (no change since the last heartbeat) is a lone `0`.
    #[test]
    fn test_v1_encoding_null_subscription() {
        let mut data = ShareGroupHeartbeatRequestData::new();
        data.set_group_id("g".to_string())
            .set_member_id("m".to_string())
            .set_member_epoch(ShareGroupHeartbeatRequest::LEAVE_GROUP_MEMBER_EPOCH)
            .set_rack_id(Some("r".to_string()))
            .set_subscribed_topic_names(None);
        let mut request = Builder::new(data).build_version(1).unwrap();

        let mut expected = vec![0x02, b'g', 0x02, b'm'];
        expected.extend_from_slice(&(-1i32).to_be_bytes()); // MemberEpoch
        expected.extend_from_slice(&[0x02, b'r']); // RackId
        expected.push(0x00); // SubscribedTopicNames: null
        expected.push(0x00); // tagged fields
        assert_eq!(expected, request.serialize().unwrap().into_buffer());
    }

    fn request_data(request: &AbstractRequest) -> &ShareGroupHeartbeatRequestData {
        match request {
            AbstractRequest::ShareGroupHeartbeat(r) => r.data(),
            other => panic!("expected ShareGroupHeartbeat, got {other:?}"),
        }
    }
}
