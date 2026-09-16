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

//! DescribeTransactions response handling.
//!
//! Corresponds to `org.apache.kafka.common.requests.DescribeTransactionsResponse`.

use std::collections::HashMap;
use std::io;

use crate::DescribeTransactionsResponseData;
use crate::common::protocol::{ApiKeys, Errors, Readable};

use super::AbstractResponse;

/// A DescribeTransactions response.
///
/// Corresponds to `org.apache.kafka.common.requests.DescribeTransactionsResponse`.
#[derive(Debug, Clone)]
pub struct DescribeTransactionsResponse {
    data: DescribeTransactionsResponseData,
}

impl DescribeTransactionsResponse {
    /// Creates a new `DescribeTransactionsResponse` from the underlying data.
    pub fn new(data: DescribeTransactionsResponseData) -> Self {
        Self { data }
    }

    /// Returns the API key for this response.
    pub fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::DESCRIBE_TRANSACTIONS
    }

    /// Returns a reference to the underlying data.
    pub fn data(&self) -> &DescribeTransactionsResponseData {
        &self.data
    }

    /// Returns a mutable reference to the underlying data.
    pub(crate) fn data_mut(&mut self) -> &mut DescribeTransactionsResponseData {
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
    /// `DescribeTransactionsResponse` does not override `shouldClientThrottle`
    /// in Java, so it inherits `AbstractResponse`'s default of `false`.
    pub fn should_client_throttle(&self, _version: i16) -> bool {
        false
    }

    /// Returns the error counts aggregated across all transaction states.
    ///
    /// Mirrors `DescribeTransactionsResponse.errorCounts`.
    pub fn error_counts(&self) -> HashMap<Errors, i32> {
        let mut counts = HashMap::new();
        for state in &self.data.transaction_states {
            AbstractResponse::update_error_counts(&mut counts, Errors::for_code(state.error_code));
        }
        counts
    }

    /// Parses a `DescribeTransactionsResponse` from a readable buffer at the
    /// given version.
    ///
    /// # Errors
    ///
    /// Returns an error if parsing fails.
    pub fn parse(readable: &mut dyn Readable, version: i16) -> io::Result<Self> {
        let data = DescribeTransactionsResponseData::read(readable, version)?;
        Ok(Self::new(data))
    }
}

impl std::fmt::Display for DescribeTransactionsResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "DescribeTransactionsResponse(data={:?})", self.data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::describe_transactions_response_data::TransactionState;

    #[test]
    fn error_counts_aggregates_states() {
        let mut s1 = TransactionState::new();
        s1.set_transactional_id("t1".to_string());
        s1.set_error_code(Errors::None.code());
        let mut s2 = TransactionState::new();
        s2.set_transactional_id("t2".to_string());
        s2.set_error_code(Errors::TransactionalIdNotFound.code());
        let mut data = DescribeTransactionsResponseData::new();
        data.set_transaction_states(vec![s1, s2]);
        let response = DescribeTransactionsResponse::new(data);
        let counts = response.error_counts();
        assert_eq!(counts.get(&Errors::None), Some(&1));
        assert_eq!(counts.get(&Errors::TransactionalIdNotFound), Some(&1));
    }

    #[test]
    fn serialize_parse_round_trip() {
        let mut data = DescribeTransactionsResponseData::new();
        data.set_throttle_time_ms(9);
        let mut concrete =
            super::super::ConcreteResponse::DescribeTransactions(DescribeTransactionsResponse::new(data));
        let bytes = concrete.serialize(0).unwrap();
        let mut readable = crate::common::ByteBufferAccessor::new(bytes.into_buffer());
        let parsed = DescribeTransactionsResponse::parse(&mut readable, 0).unwrap();
        assert_eq!(parsed.data().throttle_time_ms, 9);
    }

    /// Byte-level encoding vector. DescribeTransactions response v0 is flexible:
    ///   throttle_time_ms: int32 = 9 (00 00 00 09)
    ///   transaction_states: compact array (empty -> 0x01)
    ///   _tagged_fields: 0x00
    #[test]
    fn serialize_known_byte_vector_v0() {
        let mut data = DescribeTransactionsResponseData::new();
        data.set_throttle_time_ms(9);
        let mut concrete =
            super::super::ConcreteResponse::DescribeTransactions(DescribeTransactionsResponse::new(data));
        let bytes = concrete.serialize(0).unwrap();
        let expected: &[u8] = &[
            0x00, 0x00, 0x00, 0x09, // throttle_time_ms = 9
            0x01, // transaction_states array (empty)
            0x00, // response tagged fields
        ];
        assert_eq!(bytes.into_buffer().as_slice(), expected);
    }
}
