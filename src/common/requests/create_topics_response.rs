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

//! CreateTopics response handling.
//!
//! Corresponds to `org.apache.kafka.common.requests.CreateTopicsResponse`.

use std::collections::HashMap;
use std::io;

use crate::CreateTopicsResponseData;
use crate::common::protocol::{ApiKeys, Errors, Readable};

use super::AbstractResponse;

/// A CreateTopics response.
///
/// Corresponds to `org.apache.kafka.common.requests.CreateTopicsResponse`.
#[derive(Debug, Clone)]
pub struct CreateTopicsResponse {
    data: CreateTopicsResponseData,
}

impl CreateTopicsResponse {
    /// Creates a new `CreateTopicsResponse` from the underlying data.
    pub fn new(data: CreateTopicsResponseData) -> Self {
        Self { data }
    }

    /// Returns the API key for this response.
    pub fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::CREATE_TOPICS
    }

    /// Returns a reference to the underlying data.
    pub fn data(&self) -> &CreateTopicsResponseData {
        &self.data
    }

    /// Returns a mutable reference to the underlying data.
    pub(crate) fn data_mut(&mut self) -> &mut CreateTopicsResponseData {
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

    /// Returns the error counts aggregated across all topic results.
    pub fn error_counts(&self) -> HashMap<Errors, i32> {
        let mut counts = HashMap::new();
        for result in &self.data.topics {
            AbstractResponse::update_error_counts(&mut counts, Errors::for_code(result.error_code));
        }
        counts
    }

    /// Parses a `CreateTopicsResponse` from a readable buffer at the given
    /// version.
    ///
    /// # Errors
    ///
    /// Returns an error if parsing fails.
    pub fn parse(readable: &mut dyn Readable, version: i16) -> io::Result<Self> {
        let data = CreateTopicsResponseData::read(readable, version)?;
        Ok(Self::new(data))
    }

    /// Whether the client should throttle on this response (v3+).
    pub fn should_client_throttle(&self, version: i16) -> bool {
        version >= 3
    }
}

impl std::fmt::Display for CreateTopicsResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CreateTopicsResponse(data={:?})", self.data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::create_topics_response_data::CreatableTopicResult;

    #[test]
    fn error_counts_aggregates_across_topics() {
        let mut data = CreateTopicsResponseData::new();
        let mut ok = CreatableTopicResult::new();
        ok.set_name("ok".to_string());
        ok.set_error_code(Errors::None.code());
        let mut bad = CreatableTopicResult::new();
        bad.set_name("bad".to_string());
        bad.set_error_code(Errors::TopicAlreadyExists.code());
        data.set_topics(vec![ok, bad]);
        let response = CreateTopicsResponse::new(data);
        let counts = response.error_counts();
        assert_eq!(counts.get(&Errors::None), Some(&1));
        assert_eq!(counts.get(&Errors::TopicAlreadyExists), Some(&1));
    }

    #[test]
    fn should_client_throttle_v3_threshold() {
        let response = CreateTopicsResponse::new(CreateTopicsResponseData::new());
        assert!(!response.should_client_throttle(2));
        assert!(response.should_client_throttle(3));
    }
}
