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

//! ListTransactions request handling.
//!
//! Corresponds to `org.apache.kafka.common.requests.ListTransactionsRequest`.

use std::io;

use crate::common::protocol::{ApiKeys, Errors, Readable};
use crate::list_transactions_request_data::ListTransactionsRequestData;
use crate::list_transactions_response_data::ListTransactionsResponseData;

use super::{ConcreteRequest, ConcreteResponse, ListTransactionsResponse, RequestBuilder};

/// A ListTransactions request.
///
/// Corresponds to `org.apache.kafka.common.requests.ListTransactionsRequest`.
#[derive(Debug, Clone)]
pub struct ListTransactionsRequest {
    data: ListTransactionsRequestData,
    version: i16,
}

impl ListTransactionsRequest {
    /// Creates a new `ListTransactionsRequest` from data and version.
    pub fn new(data: ListTransactionsRequestData, version: i16) -> Self {
        Self { data, version }
    }

    /// Returns a reference to the underlying data.
    pub fn data(&self) -> &ListTransactionsRequestData {
        &self.data
    }

    /// Returns a mutable reference to the underlying data.
    pub(crate) fn data_mut(&mut self) -> &mut ListTransactionsRequestData {
        &mut self.data
    }

    /// Returns the API version of this request.
    pub fn version(&self) -> i16 {
        self.version
    }

    /// Returns the API key for this request.
    pub fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::LIST_TRANSACTIONS
    }

    /// Creates an error response for this request.
    ///
    /// Mirrors `ListTransactionsRequest.getErrorResponse`.
    pub fn get_error_response(&self, throttle_time_ms: i32, error: &Errors) -> ConcreteResponse {
        let mut response = ListTransactionsResponseData::new();
        response.set_error_code(error.code());
        response.set_throttle_time_ms(throttle_time_ms);
        ConcreteResponse::ListTransactions(ListTransactionsResponse::new(response))
    }

    /// Parses a `ListTransactionsRequest` from a readable buffer at the given
    /// version.
    ///
    /// # Errors
    ///
    /// Returns an error if parsing fails.
    pub fn parse(readable: &mut dyn Readable, version: i16) -> io::Result<Self> {
        let data = ListTransactionsRequestData::read(readable, version)?;
        Ok(Self::new(data, version))
    }
}

impl std::fmt::Display for ListTransactionsRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ListTransactionsRequest(version={}, data={:?})", self.version, self.data)
    }
}

/// Builder for [`ListTransactionsRequest`].
///
/// Corresponds to `ListTransactionsRequest.Builder`.
#[derive(Debug, Clone)]
pub struct ListTransactionsRequestBuilder {
    data: ListTransactionsRequestData,
}

impl ListTransactionsRequestBuilder {
    /// Creates a builder from the given request data.
    pub fn new(data: ListTransactionsRequestData) -> Self {
        Self { data }
    }
}

impl RequestBuilder for ListTransactionsRequestBuilder {
    fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::LIST_TRANSACTIONS
    }

    fn oldest_allowed_version(&self) -> i16 {
        ApiKeys::LIST_TRANSACTIONS.oldest_version()
    }

    fn latest_allowed_version(&self) -> i16 {
        ApiKeys::LIST_TRANSACTIONS.latest_version()
    }

    fn build_version(&mut self, version: i16) -> io::Result<ConcreteRequest> {
        Ok(ConcreteRequest::ListTransactions(ListTransactionsRequest::new(
            self.data.clone(),
            version,
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn get_error_response_carries_top_level_error() {
        let request = ListTransactionsRequest::new(ListTransactionsRequestData::new(), 0);
        let response = request.get_error_response(11, &Errors::CoordinatorLoadInProgress);
        let ConcreteResponse::ListTransactions(r) = response else {
            panic!("expected ListTransactions response");
        };
        assert_eq!(r.data().error_code, Errors::CoordinatorLoadInProgress.code());
        assert_eq!(r.data().throttle_time_ms, 11);
    }

    #[test]
    fn serialize_parse_round_trip() {
        let mut data = ListTransactionsRequestData::new();
        data.set_state_filters(vec!["Ongoing".to_string()]);
        data.set_producer_id_filters(vec![1, 2]);
        data.set_duration_filter(-1);
        let mut builder = ListTransactionsRequestBuilder::new(data);
        let mut request = builder.build_version(1).unwrap();
        let bytes = request.serialize().unwrap();
        let mut readable = crate::common::ByteBufferAccessor::from_bytes(bytes.into_buffer());
        let parsed = ListTransactionsRequest::parse(&mut readable, 1).unwrap();
        assert_eq!(parsed.data().state_filters, vec!["Ongoing".to_string()]);
        assert_eq!(parsed.data().producer_id_filters, vec![1, 2]);
        assert_eq!(parsed.data().duration_filter, -1);
    }

    /// Byte-level encoding vector. ListTransactions v1 is flexible; the body is:
    ///   state_filters: compact array (len+1 = 0x02) -> "Ongoing" (0x08 + 7 bytes)
    ///   producer_id_filters: compact array (empty -> 0x01)
    ///   duration_filter: int64 = -1 (ff ff ff ff ff ff ff ff)
    ///   _tagged_fields: 0x00
    #[test]
    fn serialize_known_byte_vector_v1() {
        let mut data = ListTransactionsRequestData::new();
        data.set_state_filters(vec!["Ongoing".to_string()]);
        data.set_duration_filter(-1);
        let mut builder = ListTransactionsRequestBuilder::new(data);
        let mut request = builder.build_version(1).unwrap();
        let bytes = request.serialize().unwrap();
        let expected: &[u8] = &[
            0x02, // state_filters array length + 1
            0x08, 0x4f, 0x6e, 0x67, 0x6f, 0x69, 0x6e, 0x67, // "Ongoing"
            0x01, // producer_id_filters array (empty)
            0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, // duration_filter = -1
            0x00, // request tagged fields
        ];
        assert_eq!(bytes.into_buffer().as_slice(), expected);
    }
}
