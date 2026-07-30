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

//! InitProducerId request handling.
//!
//! Corresponds to `org.apache.kafka.common.requests.InitProducerIdRequest`.

use std::io;

use crate::common::protocol::{ApiKeys, Errors, Readable};
use crate::common::record::RecordBatch;
use crate::init_producer_id_request_data::InitProducerIdRequestData;
use crate::init_producer_id_response_data::InitProducerIdResponseData;

use super::{ConcreteRequest, ConcreteResponse, InitProducerIdResponse, RequestBuilder};

/// An InitProducerId request.
///
/// Corresponds to `org.apache.kafka.common.requests.InitProducerIdRequest`.
#[derive(Debug, Clone)]
pub struct InitProducerIdRequest {
    data: InitProducerIdRequestData,
    version: i16,
}

impl InitProducerIdRequest {
    /// Creates a new `InitProducerIdRequest` from data and version.
    pub fn new(data: InitProducerIdRequestData, version: i16) -> Self {
        Self { data, version }
    }

    /// Returns a reference to the underlying data.
    pub fn data(&self) -> &InitProducerIdRequestData {
        &self.data
    }

    /// Returns a mutable reference to the underlying data.
    pub(crate) fn data_mut(&mut self) -> &mut InitProducerIdRequestData {
        &mut self.data
    }

    /// Returns the API version of this request.
    pub fn version(&self) -> i16 {
        self.version
    }

    /// Returns the API key for this request.
    pub fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::INIT_PRODUCER_ID
    }

    /// Creates an error response for this request.
    ///
    /// Mirrors `InitProducerIdRequest.getErrorResponse`.
    pub fn get_error_response(&self, throttle_time_ms: i32, error: &Errors) -> ConcreteResponse {
        let mut response = InitProducerIdResponseData::new();
        response.set_error_code(error.code());
        response.set_producer_id(RecordBatch::NO_PRODUCER_ID);
        response.set_producer_epoch(RecordBatch::NO_PRODUCER_EPOCH);
        response.set_throttle_time_ms(throttle_time_ms);
        ConcreteResponse::InitProducerId(InitProducerIdResponse::new(response))
    }

    /// Parses an `InitProducerIdRequest` from a readable buffer at the given
    /// version.
    ///
    /// # Errors
    ///
    /// Returns an error if parsing fails.
    pub fn parse(readable: &mut dyn Readable, version: i16) -> io::Result<Self> {
        let data = InitProducerIdRequestData::read(readable, version)?;
        Ok(Self::new(data, version))
    }
}

impl std::fmt::Display for InitProducerIdRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "InitProducerIdRequest(version={}, data={:?})", self.version, self.data)
    }
}

/// Builder for [`InitProducerIdRequest`].
///
/// Corresponds to `InitProducerIdRequest.Builder`.
#[derive(Debug, Clone)]
pub struct InitProducerIdRequestBuilder {
    data: InitProducerIdRequestData,
}

impl InitProducerIdRequestBuilder {
    /// Creates a builder from the given request data.
    pub fn new(data: InitProducerIdRequestData) -> Self {
        Self { data }
    }
}

impl RequestBuilder for InitProducerIdRequestBuilder {
    fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::INIT_PRODUCER_ID
    }

    fn oldest_allowed_version(&self) -> i16 {
        ApiKeys::INIT_PRODUCER_ID.oldest_version()
    }

    fn latest_allowed_version(&self) -> i16 {
        ApiKeys::INIT_PRODUCER_ID.latest_version()
    }

    fn build_version(&mut self, version: i16) -> io::Result<ConcreteRequest> {
        Ok(ConcreteRequest::InitProducerId(InitProducerIdRequest::new(
            self.data.clone(),
            version,
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn get_error_response_carries_none_producer() {
        let mut data = InitProducerIdRequestData::new();
        data.set_transactional_id(Some("txn".to_string()));
        let request = InitProducerIdRequest::new(data, 4);
        let response = request.get_error_response(100, &Errors::CoordinatorNotAvailable);
        let ConcreteResponse::InitProducerId(r) = response else {
            panic!("expected InitProducerId response");
        };
        assert_eq!(r.data().error_code, Errors::CoordinatorNotAvailable.code());
        assert_eq!(r.data().producer_id, RecordBatch::NO_PRODUCER_ID);
        assert_eq!(r.data().producer_epoch, RecordBatch::NO_PRODUCER_EPOCH);
        assert_eq!(r.data().throttle_time_ms, 100);
    }

    #[test]
    fn serialize_parse_round_trip() {
        let mut data = InitProducerIdRequestData::new();
        data.set_transactional_id(Some("txn".to_string()));
        data.set_transaction_timeout_ms(60000);
        data.set_producer_id(RecordBatch::NO_PRODUCER_ID);
        data.set_producer_epoch(RecordBatch::NO_PRODUCER_EPOCH);
        let mut builder = InitProducerIdRequestBuilder::new(data);
        let mut request = builder.build_version(4).unwrap();
        let bytes = request.serialize().unwrap();
        let mut readable = crate::common::ByteBufferAccessor::from_bytes(bytes.into_buffer());
        let parsed = InitProducerIdRequest::parse(&mut readable, 4).unwrap();
        assert_eq!(parsed.data().transactional_id.as_deref(), Some("txn"));
        assert_eq!(parsed.data().transaction_timeout_ms, 60000);
        assert_eq!(parsed.data().producer_id, RecordBatch::NO_PRODUCER_ID);
        assert_eq!(parsed.data().producer_epoch, RecordBatch::NO_PRODUCER_EPOCH);
    }
}
