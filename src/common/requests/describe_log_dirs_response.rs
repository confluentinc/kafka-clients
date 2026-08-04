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

//! DescribeLogDirs response handling.
//!
//! Corresponds to `org.apache.kafka.common.requests.DescribeLogDirsResponse`.

use std::collections::HashMap;
use std::io;

use crate::common::protocol::{ApiKeys, Errors, Readable};
use crate::describe_log_dirs_response_data::DescribeLogDirsResponseData;

use super::abstract_response::update_error_counts;

/// The sentinel offset lag returned when a replica is not being moved or does
/// not exist (`DescribeLogDirsResponse.INVALID_OFFSET_LAG`).
pub const INVALID_OFFSET_LAG: i64 = -1;

/// The sentinel returned for total/usable volume bytes when the broker does not
/// report a value (`DescribeLogDirsResponse.UNKNOWN_VOLUME_BYTES`).
pub const UNKNOWN_VOLUME_BYTES: i64 = -1;

/// A DescribeLogDirs response.
///
/// Corresponds to `org.apache.kafka.common.requests.DescribeLogDirsResponse`.
#[derive(Debug, Clone)]
pub struct DescribeLogDirsResponse {
    data: DescribeLogDirsResponseData,
}

impl DescribeLogDirsResponse {
    /// Creates a new `DescribeLogDirsResponse` from the underlying data.
    pub fn new(data: DescribeLogDirsResponseData) -> Self {
        Self { data }
    }

    /// Returns the API key for this response.
    pub fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::DESCRIBE_LOG_DIRS
    }

    /// Returns a reference to the underlying data.
    pub fn data(&self) -> &DescribeLogDirsResponseData {
        &self.data
    }

    /// Returns a mutable reference to the underlying data.
    pub(crate) fn data_mut(&mut self) -> &mut DescribeLogDirsResponseData {
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

    /// Returns the error counts: the top-level error code plus one per
    /// per-directory result (mirrors `DescribeLogDirsResponse.errorCounts`).
    pub fn error_counts(&self) -> HashMap<Errors, i32> {
        let mut counts = HashMap::new();
        counts.insert(Errors::for_code(self.data.error_code), 1);
        for result in &self.data.results {
            update_error_counts(&mut counts, Errors::for_code(result.error_code));
        }
        counts
    }

    /// Parses a `DescribeLogDirsResponse` from a readable buffer at the given
    /// version.
    ///
    /// # Errors
    ///
    /// Returns an error if parsing fails.
    pub fn parse(readable: &mut dyn Readable, version: i16) -> io::Result<Self> {
        let data = DescribeLogDirsResponseData::read(readable, version)?;
        Ok(Self::new(data))
    }

    /// Whether the client should throttle on this response (v1+).
    pub fn should_client_throttle(&self, version: i16) -> bool {
        version >= 1
    }
}

impl std::fmt::Display for DescribeLogDirsResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "DescribeLogDirsResponse(data={:?})", self.data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::describe_log_dirs_response_data::DescribeLogDirsResult;

    #[test]
    fn error_counts_includes_top_level_and_per_dir() {
        let mut data = DescribeLogDirsResponseData::new();
        data.set_error_code(Errors::None.code());
        let mut r0 = DescribeLogDirsResult::new();
        r0.set_error_code(Errors::KafkaStorageError.code());
        r0.set_log_dir("/data0".to_string());
        let mut r1 = DescribeLogDirsResult::new();
        r1.set_error_code(Errors::None.code());
        r1.set_log_dir("/data1".to_string());
        data.set_results(vec![r0, r1]);
        let response = DescribeLogDirsResponse::new(data);
        let counts = response.error_counts();
        // top-level None + per-dir None => 2 counts for None, 1 for storage error.
        assert_eq!(counts.get(&Errors::None), Some(&2));
        assert_eq!(counts.get(&Errors::KafkaStorageError), Some(&1));
    }

    #[test]
    fn should_client_throttle_v1_threshold() {
        let response = DescribeLogDirsResponse::new(DescribeLogDirsResponseData::new());
        assert!(!response.should_client_throttle(0));
        assert!(response.should_client_throttle(1));
    }
}
