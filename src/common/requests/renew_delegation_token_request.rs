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

//! RenewDelegationToken request handling.
//!
//! Corresponds to
//! `org.apache.kafka.common.requests.RenewDelegationTokenRequest`.

use std::io;

use crate::common::protocol::{ApiKeys, Errors, Readable};
use crate::renew_delegation_token_request_data::RenewDelegationTokenRequestData;

use super::{ConcreteRequest, ConcreteResponse, RenewDelegationTokenResponse, RequestBuilder};

/// A RenewDelegationToken request.
///
/// Corresponds to
/// `org.apache.kafka.common.requests.RenewDelegationTokenRequest`.
#[derive(Debug, Clone)]
pub struct RenewDelegationTokenRequest {
    data: RenewDelegationTokenRequestData,
    version: i16,
}

impl RenewDelegationTokenRequest {
    /// Creates a new `RenewDelegationTokenRequest` from data and version.
    pub fn new(data: RenewDelegationTokenRequestData, version: i16) -> Self {
        Self { data, version }
    }

    /// Returns a reference to the underlying data.
    pub fn data(&self) -> &RenewDelegationTokenRequestData {
        &self.data
    }

    /// Returns a mutable reference to the underlying data.
    pub(crate) fn data_mut(&mut self) -> &mut RenewDelegationTokenRequestData {
        &mut self.data
    }

    /// Returns the API version of this request.
    pub fn version(&self) -> i16 {
        self.version
    }

    /// Returns the API key for this request.
    pub fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::RENEW_DELEGATION_TOKEN
    }

    /// Creates an error response for this request.
    ///
    /// Mirrors `RenewDelegationTokenRequest.getErrorResponse`.
    pub fn get_error_response(&self, throttle_time_ms: i32, error: &Errors) -> ConcreteResponse {
        ConcreteResponse::RenewDelegationToken(RenewDelegationTokenResponse::prepare_response(throttle_time_ms, *error))
    }

    /// Parses a `RenewDelegationTokenRequest` from a readable buffer.
    ///
    /// # Errors
    ///
    /// Returns an error if parsing fails.
    pub fn parse(readable: &mut dyn Readable, version: i16) -> io::Result<Self> {
        let data = RenewDelegationTokenRequestData::read(readable, version)?;
        Ok(Self::new(data, version))
    }
}

impl std::fmt::Display for RenewDelegationTokenRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Mirrors Java's `toString`, which masks the hmac.
        let mut redacted = self.data.clone();
        redacted.hmac = Vec::new();
        write!(f, "RenewDelegationTokenRequest(version={}, data={:?})", self.version, redacted)
    }
}

/// Builder for [`RenewDelegationTokenRequest`].
///
/// Corresponds to `RenewDelegationTokenRequest.Builder` in Java.
#[derive(Debug, Clone)]
pub struct RenewDelegationTokenRequestBuilder {
    data: RenewDelegationTokenRequestData,
    oldest_allowed_version: i16,
    latest_allowed_version: i16,
}

impl RenewDelegationTokenRequestBuilder {
    /// Creates a builder from existing data.
    pub fn new(data: RenewDelegationTokenRequestData) -> Self {
        Self {
            data,
            oldest_allowed_version: ApiKeys::RENEW_DELEGATION_TOKEN.oldest_version(),
            latest_allowed_version: ApiKeys::RENEW_DELEGATION_TOKEN.latest_version(),
        }
    }
}

impl RequestBuilder for RenewDelegationTokenRequestBuilder {
    fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::RENEW_DELEGATION_TOKEN
    }

    fn oldest_allowed_version(&self) -> i16 {
        self.oldest_allowed_version
    }

    fn latest_allowed_version(&self) -> i16 {
        self.latest_allowed_version
    }

    fn build_version(&mut self, version: i16) -> io::Result<ConcreteRequest> {
        Ok(ConcreteRequest::RenewDelegationToken(RenewDelegationTokenRequest::new(
            self.data.clone(),
            version,
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request_data() -> RenewDelegationTokenRequestData {
        let mut data = RenewDelegationTokenRequestData::new();
        data.hmac = b"the-hmac".to_vec();
        data.renew_period_ms = 3_600_000;
        data
    }

    #[test]
    fn get_error_response_sets_error_and_throttle() {
        let request =
            RenewDelegationTokenRequest::new(request_data(), ApiKeys::RENEW_DELEGATION_TOKEN.latest_version());
        let ConcreteResponse::RenewDelegationToken(r) =
            request.get_error_response(50, &Errors::DelegationTokenNotFound)
        else {
            panic!("expected RenewDelegationToken response");
        };
        assert_eq!(r.error(), Errors::DelegationTokenNotFound);
        assert_eq!(r.throttle_time_ms(), 50);
    }

    #[test]
    fn display_masks_hmac() {
        let request =
            RenewDelegationTokenRequest::new(request_data(), ApiKeys::RENEW_DELEGATION_TOKEN.latest_version());
        let rendered = request.to_string();
        assert!(!rendered.contains("the-hmac"), "{rendered}");
    }

    #[test]
    fn serialize_parse_round_trip() {
        let version = ApiKeys::RENEW_DELEGATION_TOKEN.latest_version();
        let mut request =
            ConcreteRequest::RenewDelegationToken(RenewDelegationTokenRequest::new(request_data(), version));
        let bytes = request.serialize().unwrap();
        let mut readable = crate::common::ByteBufferAccessor::from_bytes(bytes.into_buffer());
        let parsed = RenewDelegationTokenRequest::parse(&mut readable, version).unwrap();
        assert_eq!(parsed.data().hmac, b"the-hmac");
        assert_eq!(parsed.data().renew_period_ms, 3_600_000);
    }

    /// Byte-level wire vector for v2 (flexible). A wrong HMAC framing here is
    /// security-relevant, so the raw bytes are asserted, not just a round-trip.
    #[test]
    fn known_wire_vector_v2() {
        let mut data = RenewDelegationTokenRequestData::new();
        data.hmac = vec![0xDE, 0xAD];
        data.renew_period_ms = 1;
        let mut request = ConcreteRequest::RenewDelegationToken(RenewDelegationTokenRequest::new(data, 2));
        let bytes = request.serialize().unwrap().into_buffer();
        let expected: Vec<u8> = vec![
            0x03, 0xDE, 0xAD, // hmac: compact bytes len (2 + 1) then raw bytes
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, // renew_period_ms = 1 (int64 BE)
            0x00, // request tagged fields
        ];
        assert_eq!(bytes.as_slice(), expected.as_slice());
    }
}
