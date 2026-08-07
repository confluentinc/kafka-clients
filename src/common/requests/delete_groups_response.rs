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

use crate::common::protocol::{ApiKeys, Errors, Readable};
use crate::delete_groups_response_data::DeleteGroupsResponseData;

use super::abstract_response::update_error_counts;

/// A `DeleteGroups` response.
///
/// Corresponds to `org.apache.kafka.common.requests.DeleteGroupsResponse`.
#[derive(Debug, Clone)]
pub struct DeleteGroupsResponse {
    data: DeleteGroupsResponseData,
}

impl DeleteGroupsResponse {
    /// Creates a new `DeleteGroupsResponse` from the underlying data.
    ///
    /// Mirrors Java's constructor `DeleteGroupsResponse(DeleteGroupsResponseData)`.
    pub fn new(data: DeleteGroupsResponseData) -> Self {
        Self { data }
    }

    /// Returns the API key for this response.
    pub fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::DELETE_GROUPS
    }

    /// Returns a reference to the underlying data.
    pub fn data(&self) -> &DeleteGroupsResponseData {
        &self.data
    }

    /// Returns a mutable reference to the underlying data.
    pub(crate) fn data_mut(&mut self) -> &mut DeleteGroupsResponseData {
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

    /// Returns a map from each group id to its deletion error. Mirrors Java's
    /// `DeleteGroupsResponse.errors()`.
    pub fn errors(&self) -> HashMap<String, Errors> {
        self.data
            .results
            .iter()
            .map(|result| (result.group_id.clone(), Errors::for_code(result.error_code)))
            .collect()
    }

    /// Returns the error counts aggregated across all per-group results.
    /// Mirrors Java's `errorCounts`.
    pub fn error_counts(&self) -> HashMap<Errors, i32> {
        let mut counts = HashMap::new();
        for result in &self.data.results {
            update_error_counts(&mut counts, Errors::for_code(result.error_code));
        }
        counts
    }

    /// Parses a `DeleteGroupsResponse` from a readable buffer at the given
    /// version.
    ///
    /// # Errors
    ///
    /// Returns an error if parsing fails.
    pub fn parse(readable: &mut dyn Readable, version: i16) -> io::Result<Self> {
        let data = DeleteGroupsResponseData::read(readable, version)?;
        Ok(Self::new(data))
    }

    /// Whether the client should throttle on this response (version 1+).
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
        let mut readable = crate::common::ByteBufferAccessor::from_bytes(bytes);
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
}
