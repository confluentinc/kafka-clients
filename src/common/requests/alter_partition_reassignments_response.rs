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

//! AlterPartitionReassignments response handling.
//!
//! Corresponds to
//! `org.apache.kafka.common.requests.AlterPartitionReassignmentsResponse`.

use std::collections::HashMap;
use std::io;

use crate::AlterPartitionReassignmentsResponseData;
use crate::common::protocol::{ApiKeys, Errors, Readable};

use super::AbstractResponse;

/// An AlterPartitionReassignments response.
///
/// Corresponds to
/// `org.apache.kafka.common.requests.AlterPartitionReassignmentsResponse`.
#[derive(Debug, Clone)]
pub struct AlterPartitionReassignmentsResponse {
    data: AlterPartitionReassignmentsResponseData,
}

impl AlterPartitionReassignmentsResponse {
    /// Creates a new `AlterPartitionReassignmentsResponse` from the underlying
    /// data.
    pub fn new(data: AlterPartitionReassignmentsResponseData) -> Self {
        Self { data }
    }

    /// Returns the API key for this response.
    pub fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::ALTER_PARTITION_REASSIGNMENTS
    }

    /// Returns a reference to the underlying data.
    pub fn data(&self) -> &AlterPartitionReassignmentsResponseData {
        &self.data
    }

    /// Returns a mutable reference to the underlying data.
    pub(crate) fn data_mut(&mut self) -> &mut AlterPartitionReassignmentsResponseData {
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

    /// Returns the error counts aggregated across the top-level error and all
    /// partition results.
    ///
    /// Mirrors `AlterPartitionReassignmentsResponse.errorCounts`.
    pub fn error_counts(&self) -> HashMap<Errors, i32> {
        let mut counts = HashMap::new();
        AbstractResponse::update_error_counts(&mut counts, Errors::for_code(self.data.error_code));
        for response in &self.data.responses {
            for partition in &response.partitions {
                AbstractResponse::update_error_counts(&mut counts, Errors::for_code(partition.error_code));
            }
        }
        counts
    }

    /// Parses an `AlterPartitionReassignmentsResponse` from a readable buffer at
    /// the given version.
    ///
    /// # Errors
    ///
    /// Returns an error if parsing fails.
    pub fn parse(readable: &mut dyn Readable, version: i16) -> io::Result<Self> {
        let data = AlterPartitionReassignmentsResponseData::read(readable, version)?;
        Ok(Self::new(data))
    }

    /// Whether the client should throttle on this response (always true).
    pub fn should_client_throttle(&self, _version: i16) -> bool {
        true
    }
}

impl std::fmt::Display for AlterPartitionReassignmentsResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "AlterPartitionReassignmentsResponse(data={:?})", self.data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::alter_partition_reassignments_response_data::{
        ReassignablePartitionResponse, ReassignableTopicResponse,
    };

    #[test]
    fn error_counts_includes_top_level_and_partition_errors() {
        let mut data = AlterPartitionReassignmentsResponseData::new();
        data.set_error_code(Errors::None.code());
        let mut partition = ReassignablePartitionResponse::new();
        partition.set_partition_index(0);
        partition.set_error_code(Errors::InvalidReplicaAssignment.code());
        let mut topic = ReassignableTopicResponse::new();
        topic.set_name("A".to_string());
        topic.set_partitions(vec![partition]);
        data.set_responses(vec![topic]);
        let response = AlterPartitionReassignmentsResponse::new(data);
        let counts = response.error_counts();
        assert_eq!(counts.get(&Errors::None), Some(&1));
        assert_eq!(counts.get(&Errors::InvalidReplicaAssignment), Some(&1));
    }
}
