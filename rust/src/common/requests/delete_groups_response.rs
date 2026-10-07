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

//! `DeleteGroups` response handling.
//!
//! Corresponds to `org.apache.kafka.common.requests.DeleteGroupsResponse`.
//!
//! Possible per-group error codes:
//! - `COORDINATOR_LOAD_IN_PROGRESS`
//! - `COORDINATOR_NOT_AVAILABLE`
//! - `NOT_COORDINATOR`
//! - `GROUP_AUTHORIZATION_FAILED`
//! - `INVALID_GROUP_ID`
//! - `GROUP_ID_NOT_FOUND`
//! - `NON_EMPTY_GROUP`

use std::collections::HashMap;
use std::io;

use crate::DeleteGroupsResponseData;
use crate::common::protocol::{ApiKeys, Errors, Readable};

use super::AbstractResponse;

/// A `DeleteGroups` response.
///
/// Corresponds to `org.apache.kafka.common.requests.DeleteGroupsResponse`.
#[derive(Debug, Clone)]
#[doc(alias = "org.apache.kafka.common.requests.DeleteGroupsResponse")]
pub struct DeleteGroupsResponse {
    data: DeleteGroupsResponseData,
}

impl DeleteGroupsResponse {
    /// Creates a new `DeleteGroupsResponse` from the underlying data.
    ///
    /// Mirrors Java's constructor `DeleteGroupsResponse(DeleteGroupsResponseData)`.
    #[doc(alias = "org.apache.kafka.common.requests.DeleteGroupsResponse#DeleteGroupsResponse")]
    pub fn new(data: DeleteGroupsResponseData) -> Self {
        Self { data }
    }

    /// Returns the API key for this response.
    pub fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::DELETE_GROUPS
    }

    /// Returns a reference to the underlying data.
    #[doc(alias = "org.apache.kafka.common.requests.DeleteGroupsResponse#data")]
    pub fn data(&self) -> &DeleteGroupsResponseData {
        &self.data
    }

    /// Returns a mutable reference to the underlying data.
    pub(crate) fn data_mut(&mut self) -> &mut DeleteGroupsResponseData {
        &mut self.data
    }

    /// Returns the throttle time in milliseconds.
    #[doc(alias = "org.apache.kafka.common.requests.DeleteGroupsResponse#throttleTimeMs")]
    pub fn throttle_time_ms(&self) -> i32 {
        self.data.throttle_time_ms
    }

    /// Sets the throttle time in the response.
    #[doc(alias = "org.apache.kafka.common.requests.DeleteGroupsResponse#maybeSetThrottleTimeMs")]
    pub fn maybe_set_throttle_time_ms(&mut self, throttle_time_ms: i32) {
        self.data.set_throttle_time_ms(throttle_time_ms);
    }

    /// Returns a map from each group id to its deletion error. Mirrors Java's
    /// `DeleteGroupsResponse.errors()`.
    #[doc(alias = "org.apache.kafka.common.requests.DeleteGroupsResponse#errors")]
    pub fn errors(&self) -> HashMap<String, Errors> {
        self.data
            .results
            .iter()
            .map(|result| (result.group_id.clone(), Errors::for_code(result.error_code)))
            .collect()
    }

    /// Returns the error counts aggregated across all per-group results.
    /// Mirrors Java's `errorCounts`.
    #[doc(alias = "org.apache.kafka.common.requests.DeleteGroupsResponse#errorCounts")]
    pub fn error_counts(&self) -> HashMap<Errors, i32> {
        let mut counts = HashMap::new();
        for result in &self.data.results {
            AbstractResponse::update_error_counts(&mut counts, Errors::for_code(result.error_code));
        }
        counts
    }

    /// Parses a `DeleteGroupsResponse` from a readable buffer at the given
    /// version.
    ///
    /// # Errors
    ///
    /// Returns an error if parsing fails.
    #[doc(alias = "org.apache.kafka.common.requests.DeleteGroupsResponse#parse")]
    pub fn parse(readable: &mut dyn Readable, version: i16) -> io::Result<Self> {
        let data = DeleteGroupsResponseData::read(readable, version)?;
        Ok(Self::new(data))
    }

    /// Whether the client should throttle on this response (version 1+).
    #[doc(alias = "org.apache.kafka.common.requests.DeleteGroupsResponse#shouldClientThrottle")]
    pub fn should_client_throttle(&self, version: i16) -> bool {
        version >= 1
    }
}

impl std::fmt::Display for DeleteGroupsResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::delete_groups_response_data::DeletableGroupResult;

    fn result(group_id: &str, error: Errors) -> DeletableGroupResult {
        let mut r = DeletableGroupResult::new();
        r.set_group_id(group_id.to_string()).set_error_code(error.code());
        r
    }

    /// `errors` maps each group id to its error code.
    #[test]
    fn errors_maps_group_ids() {
        let mut data = DeleteGroupsResponseData::new();
        data.set_results(vec![result("g1", Errors::None), result("g2", Errors::NonEmptyGroup)]);
        let response = DeleteGroupsResponse::new(data);
        let errors = response.errors();
        assert_eq!(errors.get("g1"), Some(&Errors::None));
        assert_eq!(errors.get("g2"), Some(&Errors::NonEmptyGroup));
    }

    /// `error_counts` aggregates the per-group error codes.
    #[test]
    fn error_counts_aggregates_results() {
        let mut data = DeleteGroupsResponseData::new();
        data.set_results(vec![
            result("g1", Errors::None),
            result("g2", Errors::None),
            result("g3", Errors::GroupIdNotFound),
        ]);
        let response = DeleteGroupsResponse::new(data);
        let counts = response.error_counts();
        assert_eq!(counts.get(&Errors::None).copied().unwrap_or(0), 2);
        assert_eq!(counts.get(&Errors::GroupIdNotFound).copied().unwrap_or(0), 1);
    }

    /// Byte-level wire-decoding check for the v0 (non-flexible) response.
    /// Field-by-field (big-endian):
    ///   throttle_time_ms 0 -> 0x00 0x00 0x00 0x00
    ///   results: array len 1 -> 0x00 0x00 0x00 0x01
    ///     group_id "g": string len 1 -> 0x00 0x01, then 0x67
    ///     error_code 68 (NON_EMPTY_GROUP) -> 0x00 0x44
    #[test]
    fn parse_known_byte_vector_v0() {
        let bytes = vec![
            0x00, 0x00, 0x00, 0x00, // throttle_time_ms 0
            0x00, 0x00, 0x00, 0x01, // results len 1
            0x00, 0x01, 0x67, // group_id "g"
            0x00, 0x44, // error_code 68
        ];
        let mut readable = crate::common::protocol::ByteBufferAccessor::new(bytes);
        let response = DeleteGroupsResponse::parse(&mut readable, 0).unwrap();
        assert_eq!(response.data().throttle_time_ms, 0);
        assert_eq!(response.data().results.len(), 1);
        assert_eq!(response.data().results[0].group_id, "g");
        assert_eq!(response.data().results[0].error_code, Errors::NonEmptyGroup.code());
    }

    /// `should_client_throttle` returns false for v0 and true for v1+.
    #[test]
    fn should_client_throttle_by_version() {
        let response = DeleteGroupsResponse::new(DeleteGroupsResponseData::new());
        assert!(!response.should_client_throttle(0));
        assert!(response.should_client_throttle(1));
    }

    fn result_with_message(group_id: &str, error: Errors, message: Option<&str>) -> DeletableGroupResult {
        let mut r = result(group_id, error);
        r.set_error_message(message.map(str::to_string));
        r
    }

    fn serialize(data: DeleteGroupsResponseData, version: i16) -> Vec<u8> {
        let mut concrete = crate::common::requests::ConcreteResponse::DeleteGroups(DeleteGroupsResponse::new(data));
        concrete.serialize(version).unwrap().into_buffer()
    }

    /// Byte-level wire-encoding check for the v3 response, which adds the
    /// per-group nullable `ErrorMessage` (KAFKA-20620). Flexible framing:
    ///   throttle_time_ms 0 -> 0x00 0x00 0x00 0x00
    ///   results: compact array len 1 -> 0x02
    ///     group_id "g": compact string -> 0x02 0x67
    ///     error_code 134 (GROUP_DELETION_FAILED) -> 0x00 0x86
    ///     error_message "x": compact nullable string -> 0x02 0x78
    ///     result tagged fields -> 0x00
    ///   top-level tagged fields -> 0x00
    #[test]
    fn serialize_known_byte_vector_v3_with_error_message() {
        let mut data = DeleteGroupsResponseData::new();
        data.set_results(vec![result_with_message("g", Errors::GroupDeletionFailed, Some("x"))]);
        let expected: &[u8] = &[
            0x00, 0x00, 0x00, 0x00, // throttle_time_ms 0
            0x02, // results compact array len 1
            0x02, 0x67, // group_id "g"
            0x00, 0x86, // error_code 134
            0x02, 0x78, // error_message "x"
            0x00, // result tagged fields
            0x00, // top-level tagged fields
        ];
        assert_eq!(serialize(data, 3).as_slice(), expected);
    }

    /// A null `ErrorMessage` (the default) encodes as the compact-nullable null
    /// marker 0x00 at v3.
    #[test]
    fn serialize_known_byte_vector_v3_null_error_message() {
        let mut data = DeleteGroupsResponseData::new();
        data.set_results(vec![result("g", Errors::None)]);
        let expected: &[u8] = &[
            0x00, 0x00, 0x00, 0x00, // throttle_time_ms 0
            0x02, // results compact array len 1
            0x02, 0x67, // group_id "g"
            0x00, 0x00, // error_code 0
            0x00, // error_message null
            0x00, // result tagged fields
            0x00, // top-level tagged fields
        ];
        assert_eq!(serialize(data, 3).as_slice(), expected);
    }

    /// Below v3 `ErrorMessage` is absent from the wire; it is `ignorable`, so a
    /// non-null value is dropped silently rather than rejected.
    #[test]
    fn serialize_known_byte_vector_v2_drops_error_message() {
        let mut data = DeleteGroupsResponseData::new();
        data.set_results(vec![result_with_message("g", Errors::GroupDeletionFailed, Some("x"))]);
        let expected: &[u8] = &[
            0x00, 0x00, 0x00, 0x00, // throttle_time_ms 0
            0x02, // results compact array len 1
            0x02, 0x67, // group_id "g"
            0x00, 0x86, // error_code 134
            0x00, // result tagged fields
            0x00, // top-level tagged fields
        ];
        assert_eq!(serialize(data, 2).as_slice(), expected);
    }

    /// Translated from `RequestResponseTest.testDeleteGroupsResponseV3PreservesErrorMessage`:
    /// a `GROUP_DELETION_FAILED` result's error message survives a round trip at
    /// the latest version. The fixture is Java's `createDeleteGroupsResponse`
    /// (one successful group plus the new failed one).
    #[test]
    fn delete_groups_response_v3_preserves_error_message() {
        let mut data = DeleteGroupsResponseData::new();
        data.set_results(vec![
            result("test-group", Errors::None),
            result_with_message("failed-group", Errors::GroupDeletionFailed, Some("plugin offline")),
        ]);
        // `RequestResponseTest.testErrorCountsIncludesNone` still counts one NONE
        // for this fixture now that it also carries the failed group.
        let counts = DeleteGroupsResponse::new(data.clone()).error_counts();
        assert_eq!(counts.get(&Errors::None).copied(), Some(1));
        assert_eq!(counts.get(&Errors::GroupDeletionFailed).copied(), Some(1));
        let version = ApiKeys::DELETE_GROUPS.latest_version();
        assert_eq!(version, 3, "DeleteGroups latest version in Kafka 4.4");

        let bytes = serialize(data, version);
        let mut readable = crate::common::protocol::ByteBufferAccessor::new(bytes);
        let parsed = DeleteGroupsResponse::parse(&mut readable, version).unwrap();
        let failed = parsed
            .data()
            .results()
            .iter()
            .find(|r| r.group_id == "failed-group")
            .expect("failed-group result");
        assert_eq!(failed.error_code, Errors::GroupDeletionFailed.code());
        assert_eq!(failed.error_message.as_deref(), Some("plugin offline"));
    }
}
