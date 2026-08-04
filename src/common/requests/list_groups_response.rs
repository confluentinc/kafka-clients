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

//! `ListGroups` response handling.
//!
//! Corresponds to `org.apache.kafka.common.requests.ListGroupsResponse`.

use std::collections::HashMap;
use std::io;

use crate::common::protocol::{ApiKeys, Errors, Readable};
use crate::list_groups_response_data::ListGroupsResponseData;

use super::abstract_response::update_error_counts;

/// A `ListGroups` response.
///
/// Corresponds to `org.apache.kafka.common.requests.ListGroupsResponse`.
#[derive(Debug, Clone)]
pub struct ListGroupsResponse {
    data: ListGroupsResponseData,
}

impl ListGroupsResponse {
    /// Creates a new `ListGroupsResponse` from the underlying data.
    pub fn new(data: ListGroupsResponseData) -> Self {
        Self { data }
    }

    /// Returns the API key for this response.
    pub fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::LIST_GROUPS
    }

    /// Returns a reference to the underlying data.
    pub fn data(&self) -> &ListGroupsResponseData {
        &self.data
    }

    /// Returns a mutable reference to the underlying data.
    pub(crate) fn data_mut(&mut self) -> &mut ListGroupsResponseData {
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

    /// Returns error counts by [`Errors`].
    ///
    /// Mirrors `ListGroupsResponse.errorCounts` — the single top-level error
    /// code.
    pub fn error_counts(&self) -> HashMap<Errors, i32> {
        let mut counts = HashMap::new();
        update_error_counts(&mut counts, Errors::for_code(self.data.error_code));
        counts
    }

    /// Parses a `ListGroupsResponse` from a readable buffer at the given
    /// version.
    ///
    /// # Errors
    ///
    /// Returns an error if parsing fails.
    pub fn parse(readable: &mut dyn Readable, version: i16) -> io::Result<Self> {
        let data = ListGroupsResponseData::read(readable, version)?;
        Ok(Self::new(data))
    }
}

impl std::fmt::Display for ListGroupsResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Byte-level wire-encoding check for a v0 (non-flexible) response with no
    /// groups and a NONE error code. Field-by-field:
    ///   error_code: int16 = 0 -> 00 00
    ///   groups: int32 array len 0 -> 00 00 00 00
    ///   (throttle_time_ms is v1+, absent at v0)
    #[test]
    fn test_serialize_known_byte_vector_v0() {
        let data = ListGroupsResponseData::new();
        let mut resp = crate::common::requests::ConcreteResponse::ListGroups(ListGroupsResponse::new(data));
        let expected: &[u8] = &[0x00, 0x00, 0x00, 0x00, 0x00, 0x00];
        assert_eq!(resp.serialize(0).unwrap().into_buffer().as_slice(), expected);
    }

    #[test]
    fn test_error_counts() {
        let mut data = ListGroupsResponseData::new();
        data.set_error_code(Errors::CoordinatorLoadInProgress.code());
        let resp = ListGroupsResponse::new(data);
        let counts = resp.error_counts();
        assert_eq!(counts.get(&Errors::CoordinatorLoadInProgress).copied().unwrap_or(0), 1);
    }

    #[test]
    fn test_should_client_throttle() {
        let resp = ListGroupsResponse::new(ListGroupsResponseData::new());
        assert!(!resp.should_client_throttle(1));
        assert!(resp.should_client_throttle(2));
    }
}
