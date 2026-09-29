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

//! ListPartitionReassignments response handling.
//!
//! Corresponds to
//! `org.apache.kafka.common.requests.ListPartitionReassignmentsResponse`.

use std::collections::HashMap;
use std::io;

use crate::ListPartitionReassignmentsResponseData;
use crate::common::protocol::{ApiKeys, Errors, Readable};

use super::AbstractResponse;

/// A ListPartitionReassignments response.
///
/// Corresponds to
/// `org.apache.kafka.common.requests.ListPartitionReassignmentsResponse`.
#[derive(Debug, Clone)]
pub struct ListPartitionReassignmentsResponse {
    data: ListPartitionReassignmentsResponseData,
}

impl ListPartitionReassignmentsResponse {
    /// Creates a new `ListPartitionReassignmentsResponse` from the underlying
    /// data.
    pub fn new(data: ListPartitionReassignmentsResponseData) -> Self {
        Self { data }
    }

    /// Returns the API key for this response.
    pub fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::LIST_PARTITION_REASSIGNMENTS
    }

    /// Returns a reference to the underlying data.
    pub fn data(&self) -> &ListPartitionReassignmentsResponseData {
        &self.data
    }

    /// Returns a mutable reference to the underlying data.
    pub(crate) fn data_mut(&mut self) -> &mut ListPartitionReassignmentsResponseData {
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

    /// Returns the error counts for this response (only the top-level error).
    ///
    /// Mirrors `ListPartitionReassignmentsResponse.errorCounts`.
    pub fn error_counts(&self) -> HashMap<Errors, i32> {
        let mut counts = HashMap::new();
        AbstractResponse::update_error_counts(&mut counts, Errors::for_code(self.data.error_code));
        counts
    }

    /// Parses a `ListPartitionReassignmentsResponse` from a readable buffer at
    /// the given version.
    ///
    /// # Errors
    ///
    /// Returns an error if parsing fails.
    pub fn parse(readable: &mut dyn Readable, version: i16) -> io::Result<Self> {
        let data = ListPartitionReassignmentsResponseData::read(readable, version)?;
        Ok(Self::new(data))
    }

    /// Whether the client should throttle on this response (always true).
    pub fn should_client_throttle(&self, _version: i16) -> bool {
        true
    }
}

impl std::fmt::Display for ListPartitionReassignmentsResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ListPartitionReassignmentsResponse(data={:?})", self.data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_counts_reports_top_level_error() {
        let mut data = ListPartitionReassignmentsResponseData::new();
        data.set_error_code(Errors::NotController.code());
        let response = ListPartitionReassignmentsResponse::new(data);
        let counts = response.error_counts();
        assert_eq!(counts.get(&Errors::NotController), Some(&1));
    }
}
