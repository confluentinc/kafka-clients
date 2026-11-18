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

//! `ShareGroupDescribe` response handling (KIP-932).
//!
//! Corresponds to `org.apache.kafka.common.requests.ShareGroupDescribeResponse`.
//!
//! Possible error codes:
//!  - `GroupAuthorizationFailed`
//!  - `NotCoordinator`
//!  - `CoordinatorNotAvailable`
//!  - `CoordinatorLoadInProgress`
//!  - `InvalidRequest`
//!  - `InvalidGroupId`
//!  - `GroupIdNotFound`
//!  - `TopicAuthorizationFailed`

use std::collections::HashMap;
use std::fmt;
use std::io;

use crate::ShareGroupDescribeResponseData;
use crate::common::protocol::{ApiKeys, Errors, Readable};

use super::AbstractResponse;

/// A `ShareGroupDescribe` response.
///
/// Corresponds to `org.apache.kafka.common.requests.ShareGroupDescribeResponse`.
#[derive(Debug, Clone)]
#[doc(alias = "org.apache.kafka.common.requests.ShareGroupDescribeResponse")]
pub struct ShareGroupDescribeResponse {
    data: ShareGroupDescribeResponseData,
}

impl ShareGroupDescribeResponse {
    /// Creates a new `ShareGroupDescribeResponse` from the underlying data.
    #[doc(alias = "org.apache.kafka.common.requests.ShareGroupDescribeResponse#ShareGroupDescribeResponse")]
    pub fn new(data: ShareGroupDescribeResponseData) -> Self {
        Self { data }
    }

    /// Returns the API key for this response.
    pub fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::SHARE_GROUP_DESCRIBE
    }

    /// Returns a reference to the underlying data.
    #[doc(alias = "org.apache.kafka.common.requests.ShareGroupDescribeResponse#data")]
    pub fn data(&self) -> &ShareGroupDescribeResponseData {
        &self.data
    }

    /// Returns a mutable reference to the underlying data.
    pub(crate) fn data_mut(&mut self) -> &mut ShareGroupDescribeResponseData {
        &mut self.data
    }

    /// Returns the error counts: one per described group.
    #[doc(alias = "org.apache.kafka.common.requests.ShareGroupDescribeResponse#errorCounts")]
    pub fn error_counts(&self) -> HashMap<Errors, i32> {
        let mut counts = HashMap::new();
        for group in &self.data.groups {
            AbstractResponse::update_error_counts(&mut counts, Errors::for_code(group.error_code));
        }
        counts
    }

    /// Returns the throttle time in milliseconds.
    #[doc(alias = "org.apache.kafka.common.requests.ShareGroupDescribeResponse#throttleTimeMs")]
    pub fn throttle_time_ms(&self) -> i32 {
        self.data.throttle_time_ms
    }

    /// Sets the throttle time in the response.
    #[doc(alias = "org.apache.kafka.common.requests.ShareGroupDescribeResponse#maybeSetThrottleTimeMs")]
    pub fn maybe_set_throttle_time_ms(&mut self, throttle_time_ms: i32) {
        self.data.set_throttle_time_ms(throttle_time_ms);
    }

    /// Parses a `ShareGroupDescribeResponse` from a readable buffer at the given
    /// version.
    ///
    /// # Errors
    ///
    /// Returns an error if parsing fails.
    #[doc(alias = "org.apache.kafka.common.requests.ShareGroupDescribeResponse#parse")]
    pub fn parse(readable: &mut dyn Readable, version: i16) -> io::Result<Self> {
        let data = ShareGroupDescribeResponseData::read(readable, version)?;
        Ok(Self::new(data))
    }
}

impl fmt::Display for ShareGroupDescribeResponse {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::GroupState;
    use crate::common::requests::ConcreteResponse;
    use crate::common::requests::request_response_test::check_response;
    use crate::share_group_describe_response_data::DescribedGroup;

    /// `RequestResponseTest.createShareGroupDescribeResponse()`.
    fn create_share_group_describe_response() -> ShareGroupDescribeResponse {
        let mut group = DescribedGroup::new();
        group
            .set_group_id("group".to_string())
            .set_error_code(0)
            .set_error_message(Some(Errors::for_code(0).message().to_string()))
            .set_group_state(GroupState::Empty.to_string())
            .set_members(Vec::new());
        let mut data = ShareGroupDescribeResponseData::new();
        data.set_groups(vec![group]).set_throttle_time_ms(1000);
        ShareGroupDescribeResponse::new(data)
    }

    /// `RequestResponseTest.testSerialization`'s `checkResponse` half for
    /// `SHARE_GROUP_DESCRIBE`, at every version.
    #[test]
    fn test_serialization_all_versions() {
        let response = ConcreteResponse::ShareGroupDescribe(create_share_group_describe_response());
        for version in ApiKeys::SHARE_GROUP_DESCRIBE.all_versions() {
            check_response(&response, version);
        }
    }

    /// `RequestResponseTest#testErrorCountsIncludesNone`:
    /// `assertEquals(1, createShareGroupDescribeResponse().errorCounts().get(Errors.NONE))`.
    #[test]
    fn test_error_counts_includes_none() {
        assert_eq!(
            Some(&1),
            create_share_group_describe_response().error_counts().get(&Errors::None)
        );
    }

    #[test]
    fn test_error_counts_sum_per_group() {
        let groups = ["a", "b", "c"]
            .iter()
            .zip([Errors::None, Errors::GroupIdNotFound, Errors::GroupIdNotFound])
            .map(|(id, error)| {
                let mut group = DescribedGroup::new();
                group.set_group_id(id.to_string()).set_error_code(error.code());
                group
            })
            .collect();
        let mut data = ShareGroupDescribeResponseData::new();
        data.set_groups(groups);
        assert_eq!(
            HashMap::from([(Errors::None, 1), (Errors::GroupIdNotFound, 2)]),
            ShareGroupDescribeResponse::new(data).error_counts()
        );
    }

    /// Byte-level v1 encoding of one empty group, derived by hand from
    /// `ShareGroupDescribeResponse.json` in field order (all versions flexible).
    #[test]
    fn test_v1_encoding() {
        let mut group = DescribedGroup::new();
        group
            .set_error_code(0)
            .set_error_message(None)
            .set_group_id("g".to_string())
            .set_group_state("Empty".to_string())
            .set_group_epoch(4)
            .set_assignment_epoch(5)
            .set_assignor_name("simple".to_string())
            .set_members(Vec::new())
            .set_authorized_operations(i32::MIN);
        let mut data = ShareGroupDescribeResponseData::new();
        data.set_throttle_time_ms(6).set_groups(vec![group]);
        let mut response = ConcreteResponse::ShareGroupDescribe(ShareGroupDescribeResponse::new(data));

        let mut expected = Vec::new();
        expected.extend_from_slice(&6i32.to_be_bytes()); // ThrottleTimeMs
        expected.push(0x02); // Groups: compact array of 1
        expected.extend_from_slice(&0i16.to_be_bytes()); // ErrorCode
        expected.push(0x00); // ErrorMessage: null
        expected.extend_from_slice(&[0x02, b'g']); // GroupId
        expected.extend_from_slice(&[0x06, b'E', b'm', b'p', b't', b'y']); // GroupState
        expected.extend_from_slice(&4i32.to_be_bytes()); // GroupEpoch
        expected.extend_from_slice(&5i32.to_be_bytes()); // AssignmentEpoch
        expected.extend_from_slice(&[0x07, b's', b'i', b'm', b'p', b'l', b'e']); // AssignorName
        expected.push(0x01); // Members: compact array of 0
        expected.extend_from_slice(&i32::MIN.to_be_bytes()); // AuthorizedOperations
        expected.push(0x00); // group tagged fields
        expected.push(0x00); // response tagged fields
        assert_eq!(expected, response.serialize(1).unwrap().into_buffer());
    }
}
