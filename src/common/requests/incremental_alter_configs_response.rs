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

//! IncrementalAlterConfigs response handling.
//!
//! Corresponds to `org.apache.kafka.common.requests.IncrementalAlterConfigsResponse`.

use std::collections::HashMap;
use std::io;

use crate::common::config::{ConfigResource, ConfigResourceType};
use crate::common::protocol::{ApiKeys, Errors, Readable};
use crate::incremental_alter_configs_response_data::IncrementalAlterConfigsResponseData;

use super::abstract_response::update_error_counts;

/// An IncrementalAlterConfigs response.
///
/// Corresponds to `org.apache.kafka.common.requests.IncrementalAlterConfigsResponse`.
#[derive(Debug, Clone)]
pub struct IncrementalAlterConfigsResponse {
    data: IncrementalAlterConfigsResponseData,
}

impl IncrementalAlterConfigsResponse {
    /// Creates a new `IncrementalAlterConfigsResponse` from the underlying data.
    pub fn new(data: IncrementalAlterConfigsResponseData) -> Self {
        Self { data }
    }

    /// Returns the API key for this response.
    pub fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::INCREMENTAL_ALTER_CONFIGS
    }

    /// Returns a reference to the underlying data.
    pub fn data(&self) -> &IncrementalAlterConfigsResponseData {
        &self.data
    }

    /// Returns a mutable reference to the underlying data.
    pub(crate) fn data_mut(&mut self) -> &mut IncrementalAlterConfigsResponseData {
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

    /// Returns a map from each altered [`ConfigResource`] to its error code and
    /// message.
    ///
    /// Corresponds to `IncrementalAlterConfigsResponse.fromResponseData` (Java
    /// returns `Map<ConfigResource, ApiError>`; there is no `ApiError` type in
    /// this client, so the mapped value is the raw `(error_code, error_message)`
    /// pair the admin client turns into a `Error`).
    pub fn errors_by_resource(&self) -> HashMap<ConfigResource, (i16, Option<String>)> {
        self.data
            .responses
            .iter()
            .map(|response| {
                (
                    ConfigResource::new(
                        ConfigResourceType::for_id(response.resource_type),
                        response.resource_name.clone(),
                    ),
                    (response.error_code, response.error_message.clone()),
                )
            })
            .collect()
    }

    /// Returns the error counts aggregated across all resource responses.
    pub fn error_counts(&self) -> HashMap<Errors, i32> {
        let mut counts = HashMap::new();
        for response in &self.data.responses {
            update_error_counts(&mut counts, Errors::for_code(response.error_code));
        }
        counts
    }

    /// Parses an `IncrementalAlterConfigsResponse` from a readable buffer at the
    /// given version.
    ///
    /// # Errors
    ///
    /// Returns an error if parsing fails.
    pub fn parse(readable: &mut dyn Readable, version: i16) -> io::Result<Self> {
        let data = IncrementalAlterConfigsResponseData::read(readable, version)?;
        Ok(Self::new(data))
    }

    /// Whether the client should throttle on this response (always, v0+).
    pub fn should_client_throttle(&self, _version: i16) -> bool {
        true
    }
}

impl std::fmt::Display for IncrementalAlterConfigsResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "IncrementalAlterConfigsResponse(data={:?})", self.data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::incremental_alter_configs_response_data::AlterConfigsResourceResponse;

    fn response(name: &str, resource_type: i8, error: Errors) -> AlterConfigsResourceResponse {
        let mut r = AlterConfigsResourceResponse::new();
        r.set_resource_name(name.to_string());
        r.set_resource_type(resource_type);
        r.set_error_code(error.code());
        r
    }

    #[test]
    fn errors_by_resource_maps_type_and_name() {
        let mut data = IncrementalAlterConfigsResponseData::new();
        data.set_responses(vec![
            response("t", ConfigResourceType::Topic.id(), Errors::InvalidRequest),
            response("0", ConfigResourceType::Broker.id(), Errors::None),
        ]);
        let resp = IncrementalAlterConfigsResponse::new(data);
        let map = resp.errors_by_resource();
        assert_eq!(
            map.get(&ConfigResource::new(ConfigResourceType::Topic, "t".to_string()))
                .map(|(c, _)| *c),
            Some(Errors::InvalidRequest.code())
        );
        assert_eq!(
            map.get(&ConfigResource::new(ConfigResourceType::Broker, "0".to_string()))
                .map(|(c, _)| *c),
            Some(Errors::None.code())
        );
    }

    #[test]
    fn error_counts_aggregates_responses() {
        let mut data = IncrementalAlterConfigsResponseData::new();
        data.set_responses(vec![
            response("", ConfigResourceType::Broker.id(), Errors::NotController),
            response("t", ConfigResourceType::Topic.id(), Errors::None),
        ]);
        let resp = IncrementalAlterConfigsResponse::new(data);
        let counts = resp.error_counts();
        assert_eq!(counts.get(&Errors::NotController), Some(&1));
        assert_eq!(counts.get(&Errors::None), Some(&1));
    }
}
