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

//! ListTransactions response handling.
//!
//! Corresponds to `org.apache.kafka.common.requests.ListTransactionsResponse`.

use std::collections::HashMap;
use std::io;

use crate::ListTransactionsResponseData;
use crate::common::protocol::{ApiKeys, Errors, Readable};

use super::AbstractResponse;

/// A ListTransactions response.
///
/// Corresponds to `org.apache.kafka.common.requests.ListTransactionsResponse`.
#[derive(Debug, Clone)]
pub struct ListTransactionsResponse {
    data: ListTransactionsResponseData,
}

impl ListTransactionsResponse {
    /// Creates a new `ListTransactionsResponse` from the underlying data.
    pub fn new(data: ListTransactionsResponseData) -> Self {
        Self { data }
    }

    /// Returns the API key for this response.
    pub fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::LIST_TRANSACTIONS
    }

    /// Returns a reference to the underlying data.
    pub fn data(&self) -> &ListTransactionsResponseData {
        &self.data
    }

    /// Returns a mutable reference to the underlying data.
    pub(crate) fn data_mut(&mut self) -> &mut ListTransactionsResponseData {
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
    /// `ListTransactionsResponse` does not override `shouldClientThrottle` in
    /// Java, so it inherits `AbstractResponse`'s default of `false`.
    pub fn should_client_throttle(&self, _version: i16) -> bool {
        false
    }

    /// Returns the error counts for the single top-level error.
    ///
    /// Mirrors `ListTransactionsResponse.errorCounts`.
    pub fn error_counts(&self) -> HashMap<Errors, i32> {
        let mut counts = HashMap::new();
        AbstractResponse::update_error_counts(&mut counts, Errors::for_code(self.data.error_code));
        counts
    }

    /// Parses a `ListTransactionsResponse` from a readable buffer at the given
    /// version.
    ///
    /// # Errors
    ///
    /// Returns an error if parsing fails.
    pub fn parse(readable: &mut dyn Readable, version: i16) -> io::Result<Self> {
        let data = ListTransactionsResponseData::read(readable, version)?;
        Ok(Self::new(data))
    }
}

impl std::fmt::Display for ListTransactionsResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ListTransactionsResponse(data={:?})", self.data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_counts_single_top_level() {
        let mut data = ListTransactionsResponseData::new();
        data.set_error_code(Errors::CoordinatorLoadInProgress.code());
        let response = ListTransactionsResponse::new(data);
        assert_eq!(response.error_counts().get(&Errors::CoordinatorLoadInProgress), Some(&1));
    }

    #[test]
    fn serialize_parse_round_trip() {
        let mut data = ListTransactionsResponseData::new();
        data.set_throttle_time_ms(13);
        data.set_error_code(Errors::None.code());
        let mut concrete = super::super::ConcreteResponse::ListTransactions(ListTransactionsResponse::new(data));
        let bytes = concrete.serialize(1).unwrap();
        let mut readable = crate::common::ByteBufferAccessor::new(bytes.into_buffer());
        let parsed = ListTransactionsResponse::parse(&mut readable, 1).unwrap();
        assert_eq!(parsed.data().throttle_time_ms, 13);
        assert_eq!(parsed.data().error_code, Errors::None.code());
    }

    /// Byte-level encoding vector. ListTransactions response v1 is flexible:
    ///   throttle_time_ms: int32 = 13 (00 00 00 0d)
    ///   error_code: int16 = 0 (00 00)
    ///   unknown_state_filters: compact array (empty -> 0x01)
    ///   transaction_states: compact array (empty -> 0x01)
    ///   _tagged_fields: 0x00
    #[test]
    fn serialize_known_byte_vector_v1() {
        let mut data = ListTransactionsResponseData::new();
        data.set_throttle_time_ms(13);
        data.set_error_code(Errors::None.code());
        let mut concrete = super::super::ConcreteResponse::ListTransactions(ListTransactionsResponse::new(data));
        let bytes = concrete.serialize(1).unwrap();
        let expected: &[u8] = &[
            0x00, 0x00, 0x00, 0x0d, // throttle_time_ms = 13
            0x00, 0x00, // error_code = 0
            0x01, // unknown_state_filters (empty)
            0x01, // transaction_states (empty)
            0x00, // response tagged fields
        ];
        assert_eq!(bytes.into_buffer().as_slice(), expected);
    }
}
