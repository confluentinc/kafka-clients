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

//! `DescribeGroups` response handling.
//!
//! Corresponds to `org.apache.kafka.common.requests.DescribeGroupsResponse`.

use std::collections::HashMap;
use std::io;

use crate::common::protocol::{ApiKeys, Errors, Readable};
use crate::describe_groups_response_data::{DescribeGroupsResponseData, DescribedGroup};

use super::abstract_response::update_error_counts;

/// Sentinel authorized-operations value indicating the field was omitted.
///
/// Corresponds to `DescribeGroupsResponse.AUTHORIZED_OPERATIONS_OMITTED`
/// (`Integer.MIN_VALUE`).
pub const AUTHORIZED_OPERATIONS_OMITTED: i32 = i32::MIN;

/// The "unknown state" placeholder used in error group entries.
///
/// Corresponds to `DescribeGroupsResponse.UNKNOWN_STATE`.
pub const UNKNOWN_STATE: &str = "";

/// The "unknown protocol type" placeholder used in error group entries.
///
/// Corresponds to `DescribeGroupsResponse.UNKNOWN_PROTOCOL_TYPE`.
pub const UNKNOWN_PROTOCOL_TYPE: &str = "";

/// The "unknown protocol" placeholder used in error group entries.
///
/// Corresponds to `DescribeGroupsResponse.UNKNOWN_PROTOCOL`.
pub const UNKNOWN_PROTOCOL: &str = "";

/// A `DescribeGroups` response.
///
/// Corresponds to `org.apache.kafka.common.requests.DescribeGroupsResponse`.
#[derive(Debug, Clone)]
pub struct DescribeGroupsResponse {
    data: DescribeGroupsResponseData,
}

impl DescribeGroupsResponse {
    /// Creates a new `DescribeGroupsResponse` from the underlying data.
    pub fn new(data: DescribeGroupsResponseData) -> Self {
        Self { data }
    }

    /// Returns the API key for this response.
    pub fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::DESCRIBE_GROUPS
    }

    /// Returns a reference to the underlying data.
    pub fn data(&self) -> &DescribeGroupsResponseData {
        &self.data
    }

    /// Returns a mutable reference to the underlying data.
    pub(crate) fn data_mut(&mut self) -> &mut DescribeGroupsResponseData {
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

    /// Whether the client should throttle upon receiving this response.
    ///
    /// Returns `true` for v2+.
    pub fn should_client_throttle(&self, version: i16) -> bool {
        version >= 2
    }

    /// Returns error counts by [`Errors`], aggregated per described group.
    ///
    /// Mirrors `DescribeGroupsResponse.errorCounts`.
    pub fn error_counts(&self) -> HashMap<Errors, i32> {
        let mut counts = HashMap::new();
        for group in &self.data.groups {
            update_error_counts(&mut counts, Errors::for_code(group.error_code));
        }
        counts
    }

    /// Builds a `DescribedGroup` with only a group id and error code (and
    /// placeholder state/protocol fields).
    ///
    /// Mirrors `DescribeGroupsResponse.groupError(String, Errors)`.
    pub fn group_error(group_id: impl Into<String>, error: Errors) -> DescribedGroup {
        let mut group = DescribedGroup::new();
        group
            .set_group_id(group_id.into())
            .set_error_code(error.code())
            .set_group_state(UNKNOWN_STATE.to_string())
            .set_protocol_type(UNKNOWN_PROTOCOL_TYPE.to_string())
            .set_protocol_data(UNKNOWN_PROTOCOL.to_string())
            .set_members(Vec::new())
            .set_authorized_operations(AUTHORIZED_OPERATIONS_OMITTED);
        group
    }

    /// Parses a `DescribeGroupsResponse` from a readable buffer at the given
    /// version.
    ///
    /// # Errors
    ///
    /// Returns an error if parsing fails.
    pub fn parse(readable: &mut dyn Readable, version: i16) -> io::Result<Self> {
        let data = DescribeGroupsResponseData::read(readable, version)?;
        Ok(Self::new(data))
    }
}

impl std::fmt::Display for DescribeGroupsResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Byte-level wire-encoding check for a v0 (non-flexible) response with one
    /// NONE group. Field-by-field:
    ///   groups: int32 array len 1 -> 00 00 00 01
    ///     error_code: int16 = 0 -> 00 00
    ///     group_id "g1": int16 len 2 -> 00 02, bytes 67 31
    ///     group_state "": int16 len 0 -> 00 00
    ///     protocol_type "": int16 len 0 -> 00 00
    ///     protocol_data "": int16 len 0 -> 00 00
    ///     members: int32 array len 0 -> 00 00 00 00
    ///   (throttle_time_ms is v1+, authorized_operations is v3+, absent at v0)
    #[test]
    fn test_serialize_known_byte_vector_v0() {
        let mut data = DescribeGroupsResponseData::new();
        data.set_groups(vec![DescribeGroupsResponse::group_error("g1", Errors::None)]);
        let mut resp = crate::common::requests::ConcreteResponse::DescribeGroups(DescribeGroupsResponse::new(data));
        let expected: &[u8] = &[
            0x00, 0x00, 0x00, 0x01, // groups array len 1
            0x00, 0x00, // error_code 0
            0x00, 0x02, 0x67, 0x31, // group_id "g1"
            0x00, 0x00, // group_state ""
            0x00, 0x00, // protocol_type ""
            0x00, 0x00, // protocol_data ""
            0x00, 0x00, 0x00, 0x00, // members array len 0
        ];
        assert_eq!(resp.serialize(0).unwrap().into_buffer().as_slice(), expected);
    }

    #[test]
    fn test_group_error_fields() {
        let group = DescribeGroupsResponse::group_error("g1", Errors::GroupIdNotFound);
        assert_eq!(group.group_id, "g1");
        assert_eq!(group.error_code, Errors::GroupIdNotFound.code());
        assert_eq!(group.group_state, "");
        assert_eq!(group.authorized_operations, AUTHORIZED_OPERATIONS_OMITTED);
    }

    #[test]
    fn test_error_counts_aggregates_per_group() {
        let mut data = DescribeGroupsResponseData::new();
        data.set_groups(vec![
            DescribeGroupsResponse::group_error("g1", Errors::CoordinatorNotAvailable),
            DescribeGroupsResponse::group_error("g2", Errors::CoordinatorNotAvailable),
            DescribeGroupsResponse::group_error("g3", Errors::None),
        ]);
        let resp = DescribeGroupsResponse::new(data);
        let counts = resp.error_counts();
        assert_eq!(counts.get(&Errors::CoordinatorNotAvailable).copied().unwrap_or(0), 2);
        assert_eq!(counts.get(&Errors::None).copied().unwrap_or(0), 1);
    }

    #[test]
    fn test_should_client_throttle() {
        let resp = DescribeGroupsResponse::new(DescribeGroupsResponseData::new());
        assert!(!resp.should_client_throttle(1));
        assert!(resp.should_client_throttle(2));
    }
}
