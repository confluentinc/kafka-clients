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

//! DescribeConfigs response handling.
//!
//! Corresponds to `org.apache.kafka.common.requests.DescribeConfigsResponse`.

use std::collections::HashMap;
use std::io;

use crate::common::config::{ConfigResource, ConfigResourceType};
use crate::common::protocol::{ApiKeys, Errors, Readable};
use crate::describe_configs_response_data::{DescribeConfigsResponseData, DescribeConfigsResult};

use super::abstract_response::update_error_counts;

/// A DescribeConfigs response.
///
/// Corresponds to `org.apache.kafka.common.requests.DescribeConfigsResponse`.
#[derive(Debug, Clone)]
pub struct DescribeConfigsResponse {
    data: DescribeConfigsResponseData,
}

impl DescribeConfigsResponse {
    /// Creates a new `DescribeConfigsResponse` from the underlying data.
    pub fn new(data: DescribeConfigsResponseData) -> Self {
        Self { data }
    }

    /// Returns the API key for this response.
    pub fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::DESCRIBE_CONFIGS
    }

    /// Returns a reference to the underlying data.
    pub fn data(&self) -> &DescribeConfigsResponseData {
        &self.data
    }

    /// Returns a mutable reference to the underlying data.
    pub(crate) fn data_mut(&mut self) -> &mut DescribeConfigsResponseData {
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

    /// Returns a map from each described [`ConfigResource`] to its result.
    ///
    /// Corresponds to `DescribeConfigsResponse.resultMap`.
    pub fn result_map(&self) -> HashMap<ConfigResource, &DescribeConfigsResult> {
        self.data
            .results
            .iter()
            .map(|result| {
                (
                    ConfigResource::new(ConfigResourceType::for_id(result.resource_type), result.resource_name.clone()),
                    result,
                )
            })
            .collect()
    }

    /// Returns the error counts aggregated across all resource results.
    pub fn error_counts(&self) -> HashMap<Errors, i32> {
        let mut counts = HashMap::new();
        for result in &self.data.results {
            update_error_counts(&mut counts, Errors::for_code(result.error_code));
        }
        counts
    }

    /// Parses a `DescribeConfigsResponse` from a readable buffer at the given
    /// version.
    ///
    /// # Errors
    ///
    /// Returns an error if parsing fails.
    pub fn parse(readable: &mut dyn Readable, version: i16) -> io::Result<Self> {
        let data = DescribeConfigsResponseData::read(readable, version)?;
        Ok(Self::new(data))
    }

    /// Whether the client should throttle on this response (v2+).
    pub fn should_client_throttle(&self, version: i16) -> bool {
        version >= 2
    }
}

impl std::fmt::Display for DescribeConfigsResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "DescribeConfigsResponse(data={:?})", self.data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn result_map_keys_on_resource_type_and_name() {
        let mut data = DescribeConfigsResponseData::new();
        let mut topic_result = DescribeConfigsResult::new();
        topic_result.set_resource_name("t".to_string());
        topic_result.set_resource_type(ConfigResourceType::Topic.id());
        topic_result.set_error_code(Errors::None.code());
        let mut broker_result = DescribeConfigsResult::new();
        broker_result.set_resource_name("0".to_string());
        broker_result.set_resource_type(ConfigResourceType::Broker.id());
        broker_result.set_error_code(Errors::None.code());
        data.set_results(vec![topic_result, broker_result]);
        let response = DescribeConfigsResponse::new(data);
        let map = response.result_map();
        assert!(map.contains_key(&ConfigResource::new(ConfigResourceType::Topic, "t".to_string())));
        assert!(map.contains_key(&ConfigResource::new(ConfigResourceType::Broker, "0".to_string())));
    }

    #[test]
    fn error_counts_aggregates_results() {
        let mut data = DescribeConfigsResponseData::new();
        let mut ok = DescribeConfigsResult::new();
        ok.set_error_code(Errors::None.code());
        let mut bad = DescribeConfigsResult::new();
        bad.set_error_code(Errors::InvalidRequest.code());
        data.set_results(vec![ok, bad]);
        let response = DescribeConfigsResponse::new(data);
        let counts = response.error_counts();
        assert_eq!(counts.get(&Errors::None), Some(&1));
        assert_eq!(counts.get(&Errors::InvalidRequest), Some(&1));
    }

    #[test]
    fn should_client_throttle_v2_threshold() {
        let response = DescribeConfigsResponse::new(DescribeConfigsResponseData::new());
        assert!(!response.should_client_throttle(1));
        assert!(response.should_client_throttle(2));
    }
}
