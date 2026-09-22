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

//! DeleteTopics response handling.
//!
//! Corresponds to `org.apache.kafka.common.requests.DeleteTopicsResponse`.

use std::collections::HashMap;
use std::io;

use crate::DeleteTopicsResponseData;
use crate::common::protocol::{ApiKeys, Errors, Readable};

use super::AbstractResponse;

/// A DeleteTopics response.
///
/// Corresponds to `org.apache.kafka.common.requests.DeleteTopicsResponse`.
#[derive(Debug, Clone)]
pub struct DeleteTopicsResponse {
    data: DeleteTopicsResponseData,
}

impl DeleteTopicsResponse {
    /// Creates a new `DeleteTopicsResponse` from the underlying data.
    pub fn new(data: DeleteTopicsResponseData) -> Self {
        Self { data }
    }

    /// Returns the API key for this response.
    pub fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::DELETE_TOPICS
    }

    /// Returns a reference to the underlying data.
    pub fn data(&self) -> &DeleteTopicsResponseData {
        &self.data
    }

    /// Returns a mutable reference to the underlying data.
    pub(crate) fn data_mut(&mut self) -> &mut DeleteTopicsResponseData {
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

    /// Returns the error counts aggregated across all topic responses.
    pub fn error_counts(&self) -> HashMap<Errors, i32> {
        let mut counts = HashMap::new();
        for result in &self.data.responses {
            AbstractResponse::update_error_counts(&mut counts, Errors::for_code(result.error_code));
        }
        counts
    }

    /// Parses a `DeleteTopicsResponse` from a readable buffer at the given
    /// version.
    ///
    /// # Errors
    ///
    /// Returns an error if parsing fails.
    pub fn parse(readable: &mut dyn Readable, version: i16) -> io::Result<Self> {
        let data = DeleteTopicsResponseData::read(readable, version)?;
        Ok(Self::new(data))
    }

    /// Whether the client should throttle on this response (v1+).
    pub fn should_client_throttle(&self, version: i16) -> bool {
        version >= 1
    }
}

impl std::fmt::Display for DeleteTopicsResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "DeleteTopicsResponse(data={:?})", self.data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::delete_topics_response_data::DeletableTopicResult;

    #[test]
    fn error_counts_aggregates_across_responses() {
        let mut data = DeleteTopicsResponseData::new();
        let mut ok = DeletableTopicResult::new();
        ok.set_name(Some("ok".to_string()));
        ok.set_error_code(Errors::None.code());
        let mut bad = DeletableTopicResult::new();
        bad.set_name(Some("bad".to_string()));
        bad.set_error_code(Errors::UnknownTopicOrPartition.code());
        data.set_responses(vec![ok, bad]);
        let response = DeleteTopicsResponse::new(data);
        let counts = response.error_counts();
        assert_eq!(counts.get(&Errors::None), Some(&1));
        assert_eq!(counts.get(&Errors::UnknownTopicOrPartition), Some(&1));
    }

    #[test]
    fn should_client_throttle_v1_threshold() {
        let response = DeleteTopicsResponse::new(DeleteTopicsResponseData::new());
        assert!(!response.should_client_throttle(0));
        assert!(response.should_client_throttle(1));
    }
}
