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

//! DeleteRecords response handling.
//!
//! Corresponds to `org.apache.kafka.common.requests.DeleteRecordsResponse`.

use std::collections::HashMap;
use std::io;

use crate::DeleteRecordsResponseData;
use crate::common::protocol::{ApiKeys, Errors, Readable};

use super::AbstractResponse;

/// A DeleteRecords response.
///
/// Corresponds to `org.apache.kafka.common.requests.DeleteRecordsResponse`.
#[derive(Debug, Clone)]
pub struct DeleteRecordsResponse {
    data: DeleteRecordsResponseData,
}

impl DeleteRecordsResponse {
    /// Sentinel low watermark returned for a partition that failed.
    ///
    /// Corresponds to `DeleteRecordsResponse.INVALID_LOW_WATERMARK`.
    pub const INVALID_LOW_WATERMARK: i64 = -1;

    /// Creates a new `DeleteRecordsResponse` from the underlying data.
    pub fn new(data: DeleteRecordsResponseData) -> Self {
        Self { data }
    }

    /// Returns the API key for this response.
    pub fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::DELETE_RECORDS
    }

    /// Returns a reference to the underlying data.
    pub fn data(&self) -> &DeleteRecordsResponseData {
        &self.data
    }

    /// Returns a mutable reference to the underlying data.
    pub(crate) fn data_mut(&mut self) -> &mut DeleteRecordsResponseData {
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

    /// Returns the error counts aggregated across all partition results.
    pub fn error_counts(&self) -> HashMap<Errors, i32> {
        let mut counts = HashMap::new();
        for topic in &self.data.topics {
            for partition in &topic.partitions {
                AbstractResponse::update_error_counts(&mut counts, Errors::for_code(partition.error_code));
            }
        }
        counts
    }

    /// Parses a `DeleteRecordsResponse` from a readable buffer at the given
    /// version.
    ///
    /// # Errors
    ///
    /// Returns an error if parsing fails.
    pub fn parse(readable: &mut dyn Readable, version: i16) -> io::Result<Self> {
        let data = DeleteRecordsResponseData::read(readable, version)?;
        Ok(Self::new(data))
    }

    /// Whether the client should throttle on this response (v1+).
    pub fn should_client_throttle(&self, version: i16) -> bool {
        version >= 1
    }
}

impl std::fmt::Display for DeleteRecordsResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "DeleteRecordsResponse(data={:?})", self.data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::delete_records_response_data::{DeleteRecordsPartitionResult, DeleteRecordsTopicResult};

    #[test]
    fn error_counts_aggregates_across_partitions() {
        let mut data = DeleteRecordsResponseData::new();
        let mut ok = DeleteRecordsPartitionResult::new();
        ok.set_partition_index(0);
        ok.set_error_code(Errors::None.code());
        let mut bad = DeleteRecordsPartitionResult::new();
        bad.set_partition_index(1);
        bad.set_error_code(Errors::OffsetOutOfRange.code());
        let mut topic = DeleteRecordsTopicResult::new();
        topic.set_name("t".to_string());
        topic.set_partitions(vec![ok, bad]);
        data.set_topics(vec![topic]);
        let response = DeleteRecordsResponse::new(data);
        let counts = response.error_counts();
        assert_eq!(counts.get(&Errors::None), Some(&1));
        assert_eq!(counts.get(&Errors::OffsetOutOfRange), Some(&1));
    }

    #[test]
    fn should_client_throttle_v1_threshold() {
        let response = DeleteRecordsResponse::new(DeleteRecordsResponseData::new());
        assert!(!response.should_client_throttle(0));
        assert!(response.should_client_throttle(1));
    }
}
