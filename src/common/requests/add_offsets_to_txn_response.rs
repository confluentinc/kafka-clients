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

//! `AddOffsetsToTxn` response handling.
//!
//! Corresponds to `org.apache.kafka.common.requests.AddOffsetsToTxnResponse`.
//!
//! Possible error codes:
//!  - `CoordinatorLoadInProgress` (14)
//!  - `CoordinatorNotAvailable` (15)
//!  - `NotCoordinator` (16)
//!  - `InvalidProducerIdMapping` (49)
//!  - `InvalidProducerEpoch` (47) — for version <= 1
//!  - `InvalidTxnState` (48)
//!  - `GroupAuthorizationFailed` (30)
//!  - `TransactionalIdAuthorizationFailed` (53)
//!  - `ProducerFenced` (90)

use std::collections::HashMap;
use std::io;

use crate::add_offsets_to_txn_response_data::AddOffsetsToTxnResponseData;
use crate::common::protocol::{ApiKeys, Errors, Readable};

use super::abstract_response::update_error_counts;

/// An `AddOffsetsToTxn` response.
///
/// Corresponds to `org.apache.kafka.common.requests.AddOffsetsToTxnResponse`.
#[derive(Debug, Clone)]
pub struct AddOffsetsToTxnResponse {
    data: AddOffsetsToTxnResponseData,
}

impl AddOffsetsToTxnResponse {
    /// Creates a new `AddOffsetsToTxnResponse` from the underlying data.
    pub fn new(data: AddOffsetsToTxnResponseData) -> Self {
        Self { data }
    }

    /// Returns the API key for this response.
    pub fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::ADD_OFFSETS_TO_TXN
    }

    /// Returns a reference to the underlying data.
    pub fn data(&self) -> &AddOffsetsToTxnResponseData {
        &self.data
    }

    /// Returns a mutable reference to the underlying data.
    pub(crate) fn data_mut(&mut self) -> &mut AddOffsetsToTxnResponseData {
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

    /// The error code wrapped as an [`Errors`].
    ///
    /// **Private**, unlike the equivalents on `InitProducerIdResponse` and
    /// `EndTxnResponse`: Java's `AddOffsetsToTxnResponse` has no public `error()`
    /// accessor — `TransactionManager.java:1806` reads `data.errorCode()` inline.
    /// Kept as an internal helper for [`Self::error_counts`] rather than added to
    /// the public surface (DoD §7).
    fn error(&self) -> Errors {
        Errors::for_code(self.data.error_code)
    }

    /// Returns error counts by [`Errors`].
    pub fn error_counts(&self) -> HashMap<Errors, i32> {
        let mut counts = HashMap::new();
        update_error_counts(&mut counts, self.error());
        counts
    }

    /// Parses an `AddOffsetsToTxnResponse` from a readable buffer at the given
    /// version.
    ///
    /// # Errors
    ///
    /// Returns an error if parsing fails.
    pub fn parse(readable: &mut dyn Readable, version: i16) -> io::Result<Self> {
        let data = AddOffsetsToTxnResponseData::read(readable, version)?;
        Ok(Self::new(data))
    }
}

impl std::fmt::Display for AddOffsetsToTxnResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_and_error_counts() {
        let mut data = AddOffsetsToTxnResponseData::new();
        data.set_error_code(Errors::InvalidTxnState.code());
        let response = AddOffsetsToTxnResponse::new(data);

        assert_eq!(response.error(), Errors::InvalidTxnState);
        let counts = response.error_counts();
        assert_eq!(counts.get(&Errors::InvalidTxnState), Some(&1));
        assert_eq!(counts.len(), 1);
    }

    #[test]
    fn test_error_counts_includes_none_for_success() {
        let response = AddOffsetsToTxnResponse::new(AddOffsetsToTxnResponseData::new());
        assert_eq!(response.error(), Errors::None);
        assert_eq!(response.error_counts().get(&Errors::None), Some(&1));
    }

    #[test]
    fn test_throttle_time_round_trip() {
        let mut response = AddOffsetsToTxnResponse::new(AddOffsetsToTxnResponseData::new());
        assert_eq!(response.throttle_time_ms(), 0);
        response.maybe_set_throttle_time_ms(77);
        assert_eq!(response.throttle_time_ms(), 77);
    }

    #[test]
    fn test_should_client_throttle() {
        let response = AddOffsetsToTxnResponse::new(AddOffsetsToTxnResponseData::new());
        assert!(!response.should_client_throttle(0));
        assert!(response.should_client_throttle(1));
        assert!(response.should_client_throttle(ApiKeys::ADD_OFFSETS_TO_TXN.latest_version()));
    }

    #[test]
    fn test_api_key() {
        let response = AddOffsetsToTxnResponse::new(AddOffsetsToTxnResponseData::new());
        assert_eq!(response.api_key(), &ApiKeys::ADD_OFFSETS_TO_TXN);
    }
}
