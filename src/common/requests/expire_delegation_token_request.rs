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

//! ExpireDelegationToken request handling.
//!
//! Corresponds to
//! `org.apache.kafka.common.requests.ExpireDelegationTokenRequest`.

use std::io;

use crate::common::protocol::{ApiKeys, Errors, Readable};
use crate::expire_delegation_token_request_data::ExpireDelegationTokenRequestData;

use super::{ConcreteRequest, ConcreteResponse, ExpireDelegationTokenResponse, RequestBuilder};

/// An ExpireDelegationToken request.
///
/// Corresponds to
/// `org.apache.kafka.common.requests.ExpireDelegationTokenRequest`.
#[derive(Debug, Clone)]
pub struct ExpireDelegationTokenRequest {
    data: ExpireDelegationTokenRequestData,
    version: i16,
}

impl ExpireDelegationTokenRequest {
    /// Creates a new `ExpireDelegationTokenRequest` from data and version.
    pub fn new(data: ExpireDelegationTokenRequestData, version: i16) -> Self {
        Self { data, version }
    }

    /// Returns a reference to the underlying data.
    pub fn data(&self) -> &ExpireDelegationTokenRequestData {
        &self.data
    }

    /// Returns a mutable reference to the underlying data.
    pub(crate) fn data_mut(&mut self) -> &mut ExpireDelegationTokenRequestData {
        &mut self.data
    }

    /// Returns the HMAC bytes.
    ///
    /// Mirrors `ExpireDelegationTokenRequest.hmac`.
    pub fn hmac(&self) -> &[u8] {
        &self.data.hmac
    }

    /// Returns the expiry time period in milliseconds.
    ///
    /// Mirrors `ExpireDelegationTokenRequest.expiryTimePeriod`.
    pub fn expiry_time_period(&self) -> i64 {
        self.data.expiry_time_period_ms
    }

    /// Returns the API version of this request.
    pub fn version(&self) -> i16 {
        self.version
    }

    /// Returns the API key for this request.
    pub fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::EXPIRE_DELEGATION_TOKEN
    }

    /// Creates an error response for this request.
    ///
    /// Mirrors `ExpireDelegationTokenRequest.getErrorResponse`.
    pub fn get_error_response(&self, throttle_time_ms: i32, error: &Errors) -> ConcreteResponse {
        ConcreteResponse::ExpireDelegationToken(ExpireDelegationTokenResponse::prepare_response(
            throttle_time_ms,
            *error,
        ))
    }

    /// Parses an `ExpireDelegationTokenRequest` from a readable buffer.
    ///
    /// # Errors
    ///
    /// Returns an error if parsing fails.
    pub fn parse(readable: &mut dyn Readable, version: i16) -> io::Result<Self> {
        let data = ExpireDelegationTokenRequestData::read(readable, version)?;
        Ok(Self::new(data, version))
    }
}

impl std::fmt::Display for ExpireDelegationTokenRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Mirrors Java's `toString`, which masks the hmac.
        let mut redacted = self.data.clone();
        redacted.hmac = Vec::new();
        write!(f, "ExpireDelegationTokenRequest(version={}, data={:?})", self.version, redacted)
    }
}

/// Builder for [`ExpireDelegationTokenRequest`].
///
/// Corresponds to `ExpireDelegationTokenRequest.Builder` in Java.
#[derive(Debug, Clone)]
pub struct ExpireDelegationTokenRequestBuilder {
    data: ExpireDelegationTokenRequestData,
    oldest_allowed_version: i16,
    latest_allowed_version: i16,
}

impl ExpireDelegationTokenRequestBuilder {
    /// Creates a builder from existing data.
    pub fn from_data(data: ExpireDelegationTokenRequestData) -> Self {
        Self {
            data,
            oldest_allowed_version: ApiKeys::EXPIRE_DELEGATION_TOKEN.oldest_version(),
            latest_allowed_version: ApiKeys::EXPIRE_DELEGATION_TOKEN.latest_version(),
        }
    }
}

impl RequestBuilder for ExpireDelegationTokenRequestBuilder {
    fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::EXPIRE_DELEGATION_TOKEN
    }

    fn oldest_allowed_version(&self) -> i16 {
        self.oldest_allowed_version
    }

    fn latest_allowed_version(&self) -> i16 {
        self.latest_allowed_version
    }

    fn build_version(&mut self, version: i16) -> io::Result<ConcreteRequest> {
        Ok(ConcreteRequest::ExpireDelegationToken(ExpireDelegationTokenRequest::new(
            self.data.clone(),
            version,
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request_data() -> ExpireDelegationTokenRequestData {
        let mut data = ExpireDelegationTokenRequestData::new();
        data.hmac = b"the-hmac".to_vec();
        data.expiry_time_period_ms = -1;
        data
    }

    #[test]
    fn accessors_return_hmac_and_period() {
        let request =
            ExpireDelegationTokenRequest::new(request_data(), ApiKeys::EXPIRE_DELEGATION_TOKEN.latest_version());
        assert_eq!(request.hmac(), b"the-hmac");
        assert_eq!(request.expiry_time_period(), -1);
    }

    #[test]
    fn get_error_response_sets_error_and_throttle() {
        let request =
            ExpireDelegationTokenRequest::new(request_data(), ApiKeys::EXPIRE_DELEGATION_TOKEN.latest_version());
        let ConcreteResponse::ExpireDelegationToken(r) =
            request.get_error_response(60, &Errors::DelegationTokenNotFound)
        else {
            panic!("expected ExpireDelegationToken response");
        };
        assert_eq!(r.error(), Errors::DelegationTokenNotFound);
        assert_eq!(r.throttle_time_ms(), 60);
    }

    #[test]
    fn serialize_parse_round_trip() {
        let version = ApiKeys::EXPIRE_DELEGATION_TOKEN.latest_version();
        let mut request =
            ConcreteRequest::ExpireDelegationToken(ExpireDelegationTokenRequest::new(request_data(), version));
        let bytes = request.serialize().unwrap();
        let mut readable = crate::common::ByteBufferAccessor::from_bytes(bytes.into_buffer());
        let parsed = ExpireDelegationTokenRequest::parse(&mut readable, version).unwrap();
        assert_eq!(parsed.data().hmac, b"the-hmac");
        assert_eq!(parsed.data().expiry_time_period_ms, -1);
    }

    /// Byte-level wire vector for v2 (flexible), including the `-1`
    /// expire-immediately sentinel encoded as an all-ones int64.
    #[test]
    fn known_wire_vector_v2() {
        let mut data = ExpireDelegationTokenRequestData::new();
        data.hmac = vec![0xDE, 0xAD];
        data.expiry_time_period_ms = -1;
        let mut request = ConcreteRequest::ExpireDelegationToken(ExpireDelegationTokenRequest::new(data, 2));
        let bytes = request.serialize().unwrap().into_buffer();
        let expected: Vec<u8> = vec![
            0x03, 0xDE, 0xAD, // hmac: compact bytes len (2 + 1) then raw bytes
            0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, // expiry_time_period_ms = -1 (int64 BE)
            0x00, // request tagged fields
        ];
        assert_eq!(bytes.as_slice(), expected.as_slice());
    }
}
