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

//! InitProducerId response handling.
//!
//! Corresponds to `org.apache.kafka.common.requests.InitProducerIdResponse`.

use std::collections::HashMap;
use std::io;

use crate::common::protocol::{ApiKeys, Errors, Readable};
use crate::init_producer_id_response_data::InitProducerIdResponseData;

use super::abstract_response::update_error_counts;

/// An InitProducerId response.
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

    /// Whether the client should throttle on this response.
    ///
    /// Mirrors `InitProducerIdResponse.shouldClientThrottle` (version >= 1).
    pub fn should_client_throttle(&self, version: i16) -> bool {
        version >= 1
    }

    /// Returns the error counts for the single top-level error.
    ///
    /// Mirrors `InitProducerIdResponse.errorCounts`.
    pub fn error_counts(&self) -> HashMap<Errors, i32> {
        let mut counts = HashMap::new();
        update_error_counts(&mut counts, Errors::for_code(self.data.error_code));
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
        write!(f, "InitProducerIdResponse(data={:?})", self.data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_counts_single_top_level() {
        let mut data = InitProducerIdResponseData::new();
        data.set_error_code(Errors::CoordinatorNotAvailable.code());
        let response = InitProducerIdResponse::new(data);
        assert_eq!(response.error_counts().get(&Errors::CoordinatorNotAvailable), Some(&1));
    }

    #[test]
    fn should_client_throttle_from_v1() {
        let response = InitProducerIdResponse::new(InitProducerIdResponseData::new());
        assert!(!response.should_client_throttle(0));
        assert!(response.should_client_throttle(1));
    }

    #[test]
    fn serialize_parse_round_trip() {
        let mut data = InitProducerIdResponseData::new();
        data.set_throttle_time_ms(3);
        data.set_producer_id(1234);
        data.set_producer_epoch(7);
        let mut concrete = super::super::ConcreteResponse::InitProducerId(InitProducerIdResponse::new(data));
        let bytes = concrete.serialize(4).unwrap();
        let mut readable = crate::common::ByteBufferAccessor::from_bytes(bytes.into_buffer());
        let parsed = InitProducerIdResponse::parse(&mut readable, 4).unwrap();
        assert_eq!(parsed.data().throttle_time_ms, 3);
        assert_eq!(parsed.data().producer_id, 1234);
        assert_eq!(parsed.data().producer_epoch, 7);
    }
}
