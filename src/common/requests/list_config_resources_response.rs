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

//! ListConfigResources response handling.
//!
//! Corresponds to `org.apache.kafka.common.requests.ListConfigResourcesResponse`.

use std::collections::HashMap;
use std::io;

use crate::common::config::{ConfigResource, ConfigResourceType};
use crate::common::protocol::{ApiKeys, Errors, Readable};
use crate::list_config_resources_response_data::ListConfigResourcesResponseData;

use super::abstract_response::single_error_count;

/// A ListConfigResources response.
///
/// Corresponds to `org.apache.kafka.common.requests.ListConfigResourcesResponse`.
#[derive(Debug, Clone)]
pub struct ListConfigResourcesResponse {
    data: ListConfigResourcesResponseData,
}

impl ListConfigResourcesResponse {
    /// Creates a new `ListConfigResourcesResponse` from the underlying data.
    pub fn new(data: ListConfigResourcesResponseData) -> Self {
        Self { data }
    }

    /// Returns the API key for this response.
    pub fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::LIST_CONFIG_RESOURCES
    }

    /// Returns a reference to the underlying data.
    pub fn data(&self) -> &ListConfigResourcesResponseData {
        &self.data
    }

    /// Returns a mutable reference to the underlying data.
    pub(crate) fn data_mut(&mut self) -> &mut ListConfigResourcesResponseData {
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

    /// Returns the top-level error of this response.
    ///
    /// Corresponds to `ListConfigResourcesResponse.error`.
    pub fn error(&self) -> Errors {
        Errors::for_code(self.data.error_code)
    }

    /// Returns the listed config resources.
    ///
    /// Corresponds to `ListConfigResourcesResponse.configResources`.
    pub fn config_resources(&self) -> Vec<ConfigResource> {
        self.data
            .config_resources
            .iter()
            .map(|entry| {
                ConfigResource::new(ConfigResourceType::for_id(entry.resource_type), entry.resource_name.clone())
            })
            .collect()
    }

    /// Returns the error counts for this response.
    pub fn error_counts(&self) -> HashMap<Errors, i32> {
        single_error_count(Errors::for_code(self.data.error_code))
    }

    /// Parses a `ListConfigResourcesResponse` from a readable buffer at the
    /// given version.
    ///
    /// # Errors
    ///
    /// Returns an error if parsing fails.
    pub fn parse(readable: &mut dyn Readable, version: i16) -> io::Result<Self> {
        let data = ListConfigResourcesResponseData::read(readable, version)?;
        Ok(Self::new(data))
    }

    /// Whether the client should throttle on this response (always, v0+).
    pub fn should_client_throttle(&self, _version: i16) -> bool {
        true
    }
}

impl std::fmt::Display for ListConfigResourcesResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ListConfigResourcesResponse(data={:?})", self.data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::list_config_resources_response_data::ConfigResource as WireConfigResource;

    #[test]
    fn config_resources_maps_type_and_name() {
        let mut data = ListConfigResourcesResponseData::new();
        data.set_error_code(Errors::None.code());
        let mut a = WireConfigResource::new();
        a.set_resource_name("topic".to_string());
        a.set_resource_type(ConfigResourceType::Topic.id());
        let mut b = WireConfigResource::new();
        b.set_resource_name("1".to_string());
        b.set_resource_type(ConfigResourceType::Broker.id());
        data.set_config_resources(vec![a, b]);
        let response = ListConfigResourcesResponse::new(data);
        let resources = response.config_resources();
        assert_eq!(resources.len(), 2);
        assert!(resources.contains(&ConfigResource::new(ConfigResourceType::Topic, "topic".to_string())));
        assert!(resources.contains(&ConfigResource::new(ConfigResourceType::Broker, "1".to_string())));
    }

    #[test]
    fn error_reads_error_code() {
        let mut data = ListConfigResourcesResponseData::new();
        data.set_error_code(Errors::UnsupportedVersion.code());
        let response = ListConfigResourcesResponse::new(data);
        assert_eq!(response.error(), Errors::UnsupportedVersion);
    }
}
