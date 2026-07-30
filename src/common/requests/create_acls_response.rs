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

//! CreateAcls response handling.
//!
//! Corresponds to `org.apache.kafka.common.requests.CreateAclsResponse`.

use std::collections::HashMap;
use std::io;

use crate::common::protocol::{ApiKeys, Errors, Readable};
use crate::create_acls_response_data::{AclCreationResult, CreateAclsResponseData};

use super::abstract_response::update_error_counts;

/// A CreateAcls response.
///
/// Corresponds to `org.apache.kafka.common.requests.CreateAclsResponse`.
#[derive(Debug, Clone)]
pub struct CreateAclsResponse {
    data: CreateAclsResponseData,
}

impl CreateAclsResponse {
    /// Creates a new `CreateAclsResponse` from the underlying data.
    pub fn new(data: CreateAclsResponseData) -> Self {
        Self { data }
    }

    /// Returns the API key for this response.
    pub fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::CREATE_ACLS
    }

    /// Returns a reference to the underlying data.
    pub fn data(&self) -> &CreateAclsResponseData {
        &self.data
    }

    /// Returns a mutable reference to the underlying data.
    pub(crate) fn data_mut(&mut self) -> &mut CreateAclsResponseData {
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

    /// Returns the per-creation results.
    ///
    /// Mirrors `CreateAclsResponse.results()`.
    pub fn results(&self) -> &[AclCreationResult] {
        &self.data.results
    }

    /// Returns the error counts aggregated across all creation results.
    pub fn error_counts(&self) -> HashMap<Errors, i32> {
        let mut counts = HashMap::new();
        for result in &self.data.results {
            update_error_counts(&mut counts, Errors::for_code(result.error_code));
        }
        counts
    }

    /// Parses a `CreateAclsResponse` from a readable buffer at the given
    /// version.
    ///
    /// # Errors
    ///
    /// Returns an error if parsing fails.
    pub fn parse(readable: &mut dyn Readable, version: i16) -> io::Result<Self> {
        let data = CreateAclsResponseData::read(readable, version)?;
        Ok(Self::new(data))
    }

    /// Whether the client should throttle on this response (v1+).
    pub fn should_client_throttle(&self, version: i16) -> bool {
        version >= 1
    }
}

impl std::fmt::Display for CreateAclsResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CreateAclsResponse(data={:?})", self.data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_counts_aggregates_across_results() {
        let mut ok = AclCreationResult::new();
        ok.set_error_code(Errors::None.code());
        let mut bad = AclCreationResult::new();
        bad.set_error_code(Errors::SecurityDisabled.code());
        let mut data = CreateAclsResponseData::new();
        data.set_results(vec![ok, bad]);
        let response = CreateAclsResponse::new(data);
        let counts = response.error_counts();
        assert_eq!(counts.get(&Errors::None), Some(&1));
        assert_eq!(counts.get(&Errors::SecurityDisabled), Some(&1));
    }

    #[test]
    fn should_client_throttle_v1_threshold() {
        let response = CreateAclsResponse::new(CreateAclsResponseData::new());
        assert!(!response.should_client_throttle(0));
        assert!(response.should_client_throttle(1));
    }
}
