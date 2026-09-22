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

//! `InitProducerId` response handling.
//!
//! Corresponds to `org.apache.kafka.common.requests.InitProducerIdResponse`.
//!
//! Possible error codes:
//!  - `CoordinatorLoadInProgress` (14)
//!  - `CoordinatorNotAvailable` (15)
//!  - `NotCoordinator` (16)
//!  - `ClusterAuthorizationFailed` (31)
//!  - `InvalidProducerEpoch` (47) — for version <= 3
//!  - `TransactionalIdAuthorizationFailed` (53)
//!  - `ProducerFenced` (90)

use std::collections::HashMap;
use std::io;

use crate::InitProducerIdResponseData;
use crate::common::protocol::{ApiKeys, Errors, Readable};

use super::AbstractResponse;

/// An `InitProducerId` response.
///
/// Corresponds to `org.apache.kafka.common.requests.InitProducerIdResponse`.
#[derive(Debug, Clone)]
pub struct InitProducerIdResponse {
    data: InitProducerIdResponseData,
}

impl InitProducerIdResponse {
    /// Creates a new `InitProducerIdResponse` from the underlying data.
    pub fn new(data: InitProducerIdResponseData) -> Self {
        Self { data }
    }

    /// Returns the API key for this response.
    pub fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::INIT_PRODUCER_ID
    }

    /// Returns a reference to the underlying data.
    pub fn data(&self) -> &InitProducerIdResponseData {
        &self.data
    }

    /// Returns a mutable reference to the underlying data.
    pub(crate) fn data_mut(&mut self) -> &mut InitProducerIdResponseData {
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

    /// Whether the client should throttle upon receiving this response.
    ///
    /// Returns `true` for v1+.
    pub fn should_client_throttle(&self, version: i16) -> bool {
        version >= 1
    }

    /// Returns the error code wrapped as an [`Errors`].
    pub fn error(&self) -> Errors {
        Errors::for_code(self.data.error_code)
    }

    /// Returns error counts by [`Errors`].
    pub fn error_counts(&self) -> HashMap<Errors, i32> {
        let mut counts = HashMap::new();
        AbstractResponse::update_error_counts(&mut counts, self.error());
        counts
    }

    /// Parses an `InitProducerIdResponse` from a readable buffer at the given
    /// version.
    ///
    /// # Errors
    ///
    /// Returns an error if parsing fails.
    pub fn parse(readable: &mut dyn Readable, version: i16) -> io::Result<Self> {
        let data = InitProducerIdResponseData::read(readable, version)?;
        Ok(Self::new(data))
    }
}

impl std::fmt::Display for InitProducerIdResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_and_error_counts() {
        let mut data = InitProducerIdResponseData::new();
        data.set_error_code(Errors::ProducerFenced.code());
        let response = InitProducerIdResponse::new(data);

        assert_eq!(response.error(), Errors::ProducerFenced);
        let counts = response.error_counts();
        assert_eq!(counts.get(&Errors::ProducerFenced), Some(&1));
        assert_eq!(counts.len(), 1);
    }

    #[test]
    fn test_error_counts_includes_none_for_success() {
        let response = InitProducerIdResponse::new(InitProducerIdResponseData::new());
        assert_eq!(response.error(), Errors::None);
        assert_eq!(response.error_counts().get(&Errors::None), Some(&1));
    }

    #[test]
    fn test_throttle_time_round_trip() {
        let mut response = InitProducerIdResponse::new(InitProducerIdResponseData::new());
        assert_eq!(response.throttle_time_ms(), 0);
        response.maybe_set_throttle_time_ms(123);
        assert_eq!(response.throttle_time_ms(), 123);
    }

    /// Java gates client throttling on v1+.
    #[test]
    fn test_should_client_throttle() {
        let response = InitProducerIdResponse::new(InitProducerIdResponseData::new());
        assert!(!response.should_client_throttle(0));
        assert!(response.should_client_throttle(1));
        assert!(response.should_client_throttle(ApiKeys::INIT_PRODUCER_ID.latest_version()));
    }

    #[test]
    fn test_api_key() {
        let response = InitProducerIdResponse::new(InitProducerIdResponseData::new());
        assert_eq!(response.api_key(), &ApiKeys::INIT_PRODUCER_ID);
    }
}
