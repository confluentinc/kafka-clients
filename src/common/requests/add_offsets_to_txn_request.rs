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

//! `AddOffsetsToTxn` request handling.
//!
//! Corresponds to `org.apache.kafka.common.requests.AddOffsetsToTxnRequest`.
//!
//! Sent by a transactional producer to register a consumer group with the
//! transaction before committing offsets on its behalf, so the transaction
//! coordinator knows to include the group's offsets topic in the transaction.

use std::io;

use crate::add_offsets_to_txn_request_data::AddOffsetsToTxnRequestData;
use crate::add_offsets_to_txn_response_data::AddOffsetsToTxnResponseData;
use crate::common::protocol::{ApiKeys, Errors, Readable};

use super::AddOffsetsToTxnResponse;
use super::ConcreteRequest;
use super::ConcreteResponse;
use super::RequestBuilder;

/// An `AddOffsetsToTxn` request.
///
/// Corresponds to `org.apache.kafka.common.requests.AddOffsetsToTxnRequest`.
#[derive(Debug, Clone)]
pub struct AddOffsetsToTxnRequest {
    data: AddOffsetsToTxnRequestData,
    version: i16,
}

impl AddOffsetsToTxnRequest {
    /// Creates a new `AddOffsetsToTxnRequest` from data and version.
    pub fn new(data: AddOffsetsToTxnRequestData, version: i16) -> Self {
        Self { data, version }
    }

    /// Returns a reference to the underlying data.
    pub fn data(&self) -> &AddOffsetsToTxnRequestData {
        &self.data
    }

    /// Returns a mutable reference to the underlying data.
    pub(crate) fn data_mut(&mut self) -> &mut AddOffsetsToTxnRequestData {
        &mut self.data
    }

    /// Returns the API version of this request.
    pub fn version(&self) -> i16 {
        self.version
    }

    /// Returns the API key for this request.
    pub fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::ADD_OFFSETS_TO_TXN
    }

    /// Builds the canonical error response for this request, matching Java's
    /// `AddOffsetsToTxnRequest.getErrorResponse(throttleTimeMs, Throwable)`.
    pub fn get_error_response(&self, throttle_time_ms: i32, error: &Errors) -> ConcreteResponse {
        let mut response = AddOffsetsToTxnResponseData::new();
        response.set_error_code(error.code()).set_throttle_time_ms(throttle_time_ms);
        ConcreteResponse::AddOffsetsToTxn(AddOffsetsToTxnResponse::new(response))
    }

    /// Parses an `AddOffsetsToTxnRequest` from a readable buffer at the given
    /// version.
    ///
    /// # Errors
    ///
    /// Returns an error if parsing fails.
    pub fn parse(readable: &mut dyn Readable, version: i16) -> io::Result<Self> {
        let data = AddOffsetsToTxnRequestData::read(readable, version)?;
        Ok(Self::new(data, version))
    }
}

impl std::fmt::Display for AddOffsetsToTxnRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "AddOffsetsToTxnRequest(version={}, data={})", self.version, self.data)
    }
}

/// Builder for [`AddOffsetsToTxnRequest`].
///
/// Corresponds to `AddOffsetsToTxnRequest.Builder` in Java.
#[derive(Debug, Clone)]
pub struct AddOffsetsToTxnRequestBuilder {
    data: AddOffsetsToTxnRequestData,
    oldest_allowed_version: i16,
    latest_allowed_version: i16,
}

impl AddOffsetsToTxnRequestBuilder {
    /// Creates a builder wrapping the given data with the full supported
    /// version range.
    pub fn new(data: AddOffsetsToTxnRequestData) -> Self {
        Self {
            data,
            oldest_allowed_version: ApiKeys::ADD_OFFSETS_TO_TXN.oldest_version(),
            // Mirrors Java's `super(ApiKeys)` → `Builder(apiKey, false)` →
            // `latestVersion(false)`: released versions only. Rules §12.
            latest_allowed_version: ApiKeys::ADD_OFFSETS_TO_TXN.latest_version_enable_unstable_last_version(false),
        }
    }

    /// Returns a reference to the underlying data.
    pub fn data(&self) -> &AddOffsetsToTxnRequestData {
        &self.data
    }
}

impl RequestBuilder for AddOffsetsToTxnRequestBuilder {
    fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::ADD_OFFSETS_TO_TXN
    }

    fn oldest_allowed_version(&self) -> i16 {
        self.oldest_allowed_version
    }

    fn latest_allowed_version(&self) -> i16 {
        self.latest_allowed_version
    }

    fn build_version(&mut self, version: i16) -> io::Result<ConcreteRequest> {
        // Java's `Builder.build(short)` performs no validation for this request.
        Ok(ConcreteRequest::AddOffsetsToTxn(AddOffsetsToTxnRequest::new(
            self.data.clone(),
            version,
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn data() -> AddOffsetsToTxnRequestData {
        let mut data = AddOffsetsToTxnRequestData::new();
        data.set_transactional_id("txn-1".to_string())
            .set_producer_id(42)
            .set_producer_epoch(7)
            .set_group_id("group-1".to_string());
        data
    }

    #[test]
    fn test_build_preserves_all_fields() {
        let mut builder = AddOffsetsToTxnRequestBuilder::new(data());
        let built = builder.build().expect("build at latest version");
        match built {
            ConcreteRequest::AddOffsetsToTxn(request) => {
                assert_eq!(request.data().transactional_id, "txn-1");
                assert_eq!(request.data().producer_id, 42);
                assert_eq!(request.data().producer_epoch, 7);
                assert_eq!(request.data().group_id, "group-1");
                assert_eq!(request.version(), ApiKeys::ADD_OFFSETS_TO_TXN.latest_version());
            },
            other => panic!("expected AddOffsetsToTxn, got {other:?}"),
        }
    }

    #[test]
    fn test_get_error_response() {
        let request = AddOffsetsToTxnRequest::new(data(), 0);
        match request.get_error_response(31, &Errors::GroupAuthorizationFailed) {
            ConcreteResponse::AddOffsetsToTxn(response) => {
                assert_eq!(response.data().error_code, Errors::GroupAuthorizationFailed.code());
                assert_eq!(response.data().throttle_time_ms, 31);
            },
            other => panic!("expected AddOffsetsToTxn response, got {other:?}"),
        }
    }

    #[test]
    fn test_serialization_round_trip_all_versions() {
        for version in ApiKeys::ADD_OFFSETS_TO_TXN.oldest_version()..=ApiKeys::ADD_OFFSETS_TO_TXN.latest_version() {
            let mut builder = AddOffsetsToTxnRequestBuilder::new(data());
            let mut built = builder.build_version(version).expect("build");
            let mut buffer = built.serialize().expect("serialize");
            buffer.flip();
            let parsed = AddOffsetsToTxnRequest::parse(&mut buffer, version).expect("parse");

            assert_eq!(parsed.version(), version);
            assert_eq!(parsed.data().transactional_id, "txn-1", "v{version}");
            assert_eq!(parsed.data().producer_id, 42, "v{version}");
            assert_eq!(parsed.data().producer_epoch, 7, "v{version}");
            assert_eq!(parsed.data().group_id, "group-1", "v{version}");
        }
    }

    #[test]
    fn test_api_key_and_version() {
        let builder = AddOffsetsToTxnRequestBuilder::new(data());
        assert_eq!(builder.api_key(), &ApiKeys::ADD_OFFSETS_TO_TXN);
        assert_eq!(builder.oldest_allowed_version(), ApiKeys::ADD_OFFSETS_TO_TXN.oldest_version());
        assert_eq!(builder.latest_allowed_version(), ApiKeys::ADD_OFFSETS_TO_TXN.latest_version());

        let request = AddOffsetsToTxnRequest::new(data(), 2);
        assert_eq!(request.api_key(), &ApiKeys::ADD_OFFSETS_TO_TXN);
        assert_eq!(request.version(), 2);
    }
}
