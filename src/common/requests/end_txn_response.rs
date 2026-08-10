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

//! `EndTxn` response handling.
//!
//! Corresponds to `org.apache.kafka.common.requests.EndTxnResponse`.
//!
//! Possible error codes:
//!  - `CoordinatorLoadInProgress` (14)
//!  - `CoordinatorNotAvailable` (15)
//!  - `NotCoordinator` (16)
//!  - `InvalidTxnState` (48)
//!  - `InvalidProducerIdMapping` (49)
//!  - `InvalidProducerEpoch` (47) — for version <= 1
//!  - `TransactionalIdAuthorizationFailed` (53)
//!  - `ProducerFenced` (90)

use std::collections::HashMap;
use std::io;

use crate::common::protocol::{ApiKeys, Errors, Readable};
use crate::end_txn_response_data::EndTxnResponseData;

use super::abstract_response::update_error_counts;

/// An `EndTxn` response.
///
/// Corresponds to `org.apache.kafka.common.requests.EndTxnResponse`.
#[derive(Debug, Clone)]
pub struct EndTxnResponse {
    data: EndTxnResponseData,
}

impl EndTxnResponse {
    /// Creates a new `EndTxnResponse` from the underlying data.
    pub fn new(data: EndTxnResponseData) -> Self {
        Self { data }
    }

    /// Returns the API key for this response.
    pub fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::END_TXN
    }

    /// Returns a reference to the underlying data.
    pub fn data(&self) -> &EndTxnResponseData {
        &self.data
    }

    /// Returns a mutable reference to the underlying data.
    pub(crate) fn data_mut(&mut self) -> &mut EndTxnResponseData {
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
        update_error_counts(&mut counts, self.error());
        counts
    }

    /// Parses an `EndTxnResponse` from a readable buffer at the given version.
    ///
    /// # Errors
    ///
    /// Returns an error if parsing fails.
    pub fn parse(readable: &mut dyn Readable, version: i16) -> io::Result<Self> {
        let data = EndTxnResponseData::read(readable, version)?;
        Ok(Self::new(data))
    }
}

impl std::fmt::Display for EndTxnResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_and_error_counts() {
        let mut data = EndTxnResponseData::new();
        data.set_error_code(Errors::ProducerFenced.code());
        let response = EndTxnResponse::new(data);

        assert_eq!(response.error(), Errors::ProducerFenced);
        let counts = response.error_counts();
        assert_eq!(counts.get(&Errors::ProducerFenced), Some(&1));
        assert_eq!(counts.len(), 1);
    }

    #[test]
    fn test_error_counts_includes_none_for_success() {
        let response = EndTxnResponse::new(EndTxnResponseData::new());
        assert_eq!(response.error(), Errors::None);
        assert_eq!(response.error_counts().get(&Errors::None), Some(&1));
    }

    /// v5+ (Transaction V2) carries the producer id and epoch back so the client
    /// can pick up a bumped epoch without a separate `InitProducerId`.
    #[test]
    fn test_transaction_v2_producer_state_fields() {
        let mut data = EndTxnResponseData::new();
        data.set_producer_id(99).set_producer_epoch(4);
        let response = EndTxnResponse::new(data);
        assert_eq!(response.data().producer_id, 99);
        assert_eq!(response.data().producer_epoch, 4);
    }

    #[test]
    fn test_throttle_time_round_trip() {
        let mut response = EndTxnResponse::new(EndTxnResponseData::new());
        assert_eq!(response.throttle_time_ms(), 0);
        response.maybe_set_throttle_time_ms(64);
        assert_eq!(response.throttle_time_ms(), 64);
    }

    #[test]
    fn test_should_client_throttle() {
        let response = EndTxnResponse::new(EndTxnResponseData::new());
        assert!(!response.should_client_throttle(0));
        assert!(response.should_client_throttle(1));
        assert!(response.should_client_throttle(ApiKeys::END_TXN.latest_version()));
    }

    #[test]
    fn test_api_key() {
        let response = EndTxnResponse::new(EndTxnResponseData::new());
        assert_eq!(response.api_key(), &ApiKeys::END_TXN);
    }

    /// Translated from `EndTxnResponseTest.testConstructor`.
    ///
    /// Java loops `ApiKeys.END_TXN.allVersions()` and asserts twice per version —
    /// once on the constructed response, once after a serialize/parse round trip —
    /// so both the in-memory and on-the-wire forms are checked.
    #[test]
    fn test_constructor() {
        use super::super::ConcreteResponse;

        const THROTTLE_TIME_MS: i32 = 10;

        for version in ApiKeys::END_TXN.oldest_version()..=ApiKeys::END_TXN.latest_version() {
            let mut data = EndTxnResponseData::new();
            data.set_error_code(Errors::NotCoordinator.code())
                .set_throttle_time_ms(THROTTLE_TIME_MS);
            let response = EndTxnResponse::new(data);

            let assert_shape = |response: &EndTxnResponse, stage: &str| {
                let counts = response.error_counts();
                assert_eq!(counts.len(), 1, "{stage} v{version}");
                assert_eq!(counts.get(&Errors::NotCoordinator), Some(&1), "{stage} v{version}");
                assert_eq!(response.throttle_time_ms(), THROTTLE_TIME_MS, "{stage} v{version}");
                assert_eq!(response.should_client_throttle(version), version >= 1, "{stage} v{version}");
            };

            assert_shape(&response, "constructed");

            let mut concrete = ConcreteResponse::EndTxn(response);
            let mut buffer = concrete.serialize(version).expect("serialize");
            buffer.flip();
            let parsed = EndTxnResponse::parse(&mut buffer, version).expect("parse");

            assert_shape(&parsed, "round-tripped");
        }
    }
}
