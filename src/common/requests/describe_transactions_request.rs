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

//! DescribeTransactions request handling.
//!
//! Corresponds to `org.apache.kafka.common.requests.DescribeTransactionsRequest`.

use std::io;

use crate::DescribeTransactionsRequestData;
use crate::DescribeTransactionsResponseData;
use crate::common::protocol::{ApiKeys, Errors, Readable};
use crate::describe_transactions_response_data::TransactionState;

use super::{ConcreteRequest, ConcreteResponse, DescribeTransactionsResponse, RequestBuilder};

/// A DescribeTransactions request.
///
/// Corresponds to `org.apache.kafka.common.requests.DescribeTransactionsRequest`.
#[derive(Debug, Clone)]
pub struct DescribeTransactionsRequest {
    data: DescribeTransactionsRequestData,
    version: i16,
}

impl DescribeTransactionsRequest {
    /// Creates a new `DescribeTransactionsRequest` from data and version.
    pub fn new(data: DescribeTransactionsRequestData, version: i16) -> Self {
        Self { data, version }
    }

    /// Returns a reference to the underlying data.
    pub fn data(&self) -> &DescribeTransactionsRequestData {
        &self.data
    }

    /// Returns a mutable reference to the underlying data.
    pub(crate) fn data_mut(&mut self) -> &mut DescribeTransactionsRequestData {
        &mut self.data
    }

    /// Returns the API version of this request.
    pub fn version(&self) -> i16 {
        self.version
    }

    /// Returns the API key for this request.
    pub fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::DESCRIBE_TRANSACTIONS
    }

    /// Creates an error response for this request.
    ///
    /// Mirrors `DescribeTransactionsRequest.getErrorResponse`.
    pub fn get_error_response(&self, throttle_time_ms: i32, error: &Errors) -> ConcreteResponse {
        let mut response = DescribeTransactionsResponseData::new();
        response.set_throttle_time_ms(throttle_time_ms);
        let mut states = Vec::new();
        for transactional_id in &self.data.transactional_ids {
            let mut state = TransactionState::new();
            state.set_transactional_id(transactional_id.clone());
            state.set_error_code(error.code());
            states.push(state);
        }
        response.set_transaction_states(states);
        ConcreteResponse::DescribeTransactions(DescribeTransactionsResponse::new(response))
    }

    /// Parses a `DescribeTransactionsRequest` from a readable buffer at the given
    /// version.
    ///
    /// # Errors
    ///
    /// Returns an error if parsing fails.
    pub fn parse(readable: &mut dyn Readable, version: i16) -> io::Result<Self> {
        let data = DescribeTransactionsRequestData::read(readable, version)?;
        Ok(Self::new(data, version))
    }
}

impl std::fmt::Display for DescribeTransactionsRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "DescribeTransactionsRequest(version={}, data={:?})", self.version, self.data)
    }
}

/// Builder for [`DescribeTransactionsRequest`].
///
/// Corresponds to `DescribeTransactionsRequest.Builder`.
#[derive(Debug, Clone)]
pub struct DescribeTransactionsRequestBuilder {
    data: DescribeTransactionsRequestData,
}

impl DescribeTransactionsRequestBuilder {
    /// Creates a builder from the given request data.
    pub fn new(data: DescribeTransactionsRequestData) -> Self {
        Self { data }
    }
}

impl RequestBuilder for DescribeTransactionsRequestBuilder {
    fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::DESCRIBE_TRANSACTIONS
    }

    fn oldest_allowed_version(&self) -> i16 {
        ApiKeys::DESCRIBE_TRANSACTIONS.oldest_version()
    }

    fn latest_allowed_version(&self) -> i16 {
        ApiKeys::DESCRIBE_TRANSACTIONS.latest_version()
    }

    fn build_version(&mut self, version: i16) -> io::Result<ConcreteRequest> {
        Ok(ConcreteRequest::DescribeTransactions(DescribeTransactionsRequest::new(
            self.data.clone(),
            version,
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn get_error_response_echoes_transactional_ids() {
        let mut data = DescribeTransactionsRequestData::new();
        data.set_transactional_ids(vec!["t1".to_string(), "t2".to_string()]);
        let request = DescribeTransactionsRequest::new(data, 0);
        let response = request.get_error_response(5, &Errors::NotCoordinator);
        let ConcreteResponse::DescribeTransactions(r) = response else {
            panic!("expected DescribeTransactions response");
        };
        assert_eq!(r.data().throttle_time_ms, 5);
        assert_eq!(r.data().transaction_states.len(), 2);
        for state in &r.data().transaction_states {
            assert_eq!(state.error_code, Errors::NotCoordinator.code());
        }
    }

    #[test]
    fn serialize_parse_round_trip() {
        let mut data = DescribeTransactionsRequestData::new();
        data.set_transactional_ids(vec!["t1".to_string()]);
        let mut builder = DescribeTransactionsRequestBuilder::new(data);
        let mut request = builder.build_version(0).unwrap();
        let bytes = request.serialize().unwrap();
        let mut readable = crate::common::ByteBufferAccessor::new(bytes.into_buffer());
        let parsed = DescribeTransactionsRequest::parse(&mut readable, 0).unwrap();
        assert_eq!(parsed.data().transactional_ids, vec!["t1".to_string()]);
    }

    /// Byte-level encoding vector. DescribeTransactions v0 is flexible:
    ///   transactional_ids: compact array (len+1 = 0x02)
    ///     compact string "t1" (0x03, 0x74 0x31)
    ///   _tagged_fields: 0x00
    #[test]
    fn serialize_known_byte_vector_v0() {
        let mut data = DescribeTransactionsRequestData::new();
        data.set_transactional_ids(vec!["t1".to_string()]);
        let mut builder = DescribeTransactionsRequestBuilder::new(data);
        let mut request = builder.build_version(0).unwrap();
        let bytes = request.serialize().unwrap();
        let expected: &[u8] = &[
            0x02, // transactional_ids array length + 1
            0x03, 0x74, 0x31, // "t1"
            0x00, // request tagged fields
        ];
        assert_eq!(bytes.into_buffer().as_slice(), expected);
    }
}
