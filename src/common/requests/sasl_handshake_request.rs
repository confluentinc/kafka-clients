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

//! SASL handshake request handling.
//!
//! Corresponds to `org.apache.kafka.common.requests.SaslHandshakeRequest`.
//!
//! Request from SASL client containing client SASL mechanism.
//!
//! For interoperability with Kafka 0.9.0.x, the mechanism flow may be omitted when using GSSAPI.
//! Hence this request should not conflict with the first GSSAPI client packet. For GSSAPI, the
//! first context establishment packet starts with byte 0x60 (APPLICATION-0 tag) followed by a
//! variable-length encoded size. This handshake request starts with a request header two-byte API
//! key set to 17, followed by a mechanism name, making it easy to distinguish from a GSSAPI packet.

use std::io;

use crate::SaslHandshakeRequestData;
use crate::SaslHandshakeResponseData;
use crate::common::protocol::{ApiKeys, Errors, Readable};

use super::ConcreteRequest;
use super::ConcreteResponse;
use super::RequestBuilder;
use super::SaslHandshakeResponse;

/// A SASL handshake request.
///
/// Corresponds to `org.apache.kafka.common.requests.SaslHandshakeRequest`.
#[derive(Debug, Clone)]
pub struct SaslHandshakeRequest {
    data: SaslHandshakeRequestData,
    version: i16,
}

impl SaslHandshakeRequest {
    /// Creates a new `SaslHandshakeRequest` from data and version.
    pub fn new(data: SaslHandshakeRequestData, version: i16) -> Self {
        Self { data, version }
    }

    /// Returns a reference to the underlying data.
    pub fn data(&self) -> &SaslHandshakeRequestData {
        &self.data
    }

    /// Returns a mutable reference to the underlying data.
    pub(crate) fn data_mut(&mut self) -> &mut SaslHandshakeRequestData {
        &mut self.data
    }

    /// Returns the API version of this request.
    pub fn version(&self) -> i16 {
        self.version
    }

    /// Returns the API key for this request.
    pub fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::SASL_HANDSHAKE
    }

    /// Creates an error response for this request.
    ///
    /// The `throttle_time_ms` parameter is ignored because the SaslHandshake schema
    /// does not include a throttle time field.
    pub fn get_error_response(&self, _throttle_time_ms: i32, error: &Errors) -> ConcreteResponse {
        let mut response = SaslHandshakeResponseData::new();
        response.set_error_code(error.code());
        ConcreteResponse::SaslHandshake(SaslHandshakeResponse::new(response))
    }

    /// Parses a `SaslHandshakeRequest` from a readable buffer at the given version.
    ///
    /// # Errors
    ///
    /// Returns an error if parsing fails.
    pub fn parse(readable: &mut dyn Readable, version: i16) -> io::Result<Self> {
        let data = SaslHandshakeRequestData::read(readable, version)?;
        Ok(Self::new(data, version))
    }
}

impl std::fmt::Display for SaslHandshakeRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.data)
    }
}

/// Builder for [`SaslHandshakeRequest`].
///
/// Corresponds to `SaslHandshakeRequest.Builder` in Java.
#[derive(Debug, Clone)]
pub struct SaslHandshakeRequestBuilder {
    data: SaslHandshakeRequestData,
    oldest_allowed_version: i16,
    latest_allowed_version: i16,
}

impl SaslHandshakeRequestBuilder {
    /// Creates a new builder from the given data.
    pub fn new(data: SaslHandshakeRequestData) -> Self {
        Self {
            data,
            oldest_allowed_version: ApiKeys::SASL_HANDSHAKE.oldest_version(),
            latest_allowed_version: ApiKeys::SASL_HANDSHAKE.latest_version(),
        }
    }
}

impl RequestBuilder for SaslHandshakeRequestBuilder {
    fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::SASL_HANDSHAKE
    }

    fn oldest_allowed_version(&self) -> i16 {
        self.oldest_allowed_version
    }

    fn latest_allowed_version(&self) -> i16 {
        self.latest_allowed_version
    }

    fn build_version(&mut self, version: i16) -> io::Result<ConcreteRequest> {
        Ok(ConcreteRequest::SaslHandshake(SaslHandshakeRequest::new(
            self.data.clone(),
            version,
        )))
    }
}

impl std::fmt::Display for SaslHandshakeRequestBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_builder_version_range() {
        let data = SaslHandshakeRequestData::new();
        let builder = SaslHandshakeRequestBuilder::new(data);
        assert_eq!(*builder.api_key(), ApiKeys::SASL_HANDSHAKE);
        assert_eq!(builder.oldest_allowed_version(), ApiKeys::SASL_HANDSHAKE.oldest_version());
        assert_eq!(builder.latest_allowed_version(), ApiKeys::SASL_HANDSHAKE.latest_version());
    }

    #[test]
    fn test_builder_build() {
        let mut data = SaslHandshakeRequestData::new();
        data.set_mechanism("PLAIN".to_string());
        let mut builder = SaslHandshakeRequestBuilder::new(data);
        let request = builder.build().unwrap();
        assert_eq!(*request.api_key(), ApiKeys::SASL_HANDSHAKE);
        if let ConcreteRequest::SaslHandshake(r) = &request {
            assert_eq!(r.data().mechanism, "PLAIN");
        } else {
            panic!("Expected SaslHandshake request");
        }
    }

    #[test]
    fn test_get_error_response() {
        let data = SaslHandshakeRequestData::new();
        let request = SaslHandshakeRequest::new(data, 0);
        let response = request.get_error_response(100, &Errors::UnsupportedSaslMechanism);
        let ConcreteResponse::SaslHandshake(r) = &response else {
            panic!("Expected SaslHandshake response");
        };
        assert_eq!(r.data().error_code, Errors::UnsupportedSaslMechanism.code());
    }

    #[test]
    fn test_parse_roundtrip() {
        for version in
            SaslHandshakeRequestData::LOWEST_SUPPORTED_VERSION..=SaslHandshakeRequestData::HIGHEST_SUPPORTED_VERSION
        {
            let mut data = SaslHandshakeRequestData::new();
            data.set_mechanism("PLAIN".to_string());
            let original = SaslHandshakeRequest::new(data, version);

            let serialized = ConcreteRequest::SaslHandshake(original.clone()).serialize().unwrap();
            let mut buf = serialized;
            let parsed = SaslHandshakeRequest::parse(&mut buf, version).unwrap();
            assert_eq!(original.data().mechanism, parsed.data().mechanism);
            assert_eq!(original.version(), parsed.version());
        }
    }

    #[test]
    fn test_display() {
        let mut data = SaslHandshakeRequestData::new();
        data.set_mechanism("PLAIN".to_string());
        let request = SaslHandshakeRequest::new(data, 1);
        let display = format!("{}", request);
        assert!(display.contains("PLAIN"));
    }

    /// Translated from `RequestResponseTest.testInvalidSaslHandShakeRequest`.
    ///
    /// Serializes a SaslHandshakeRequest with mechanism "PLAIN", corrupts the mechanism
    /// string length to `i16::MAX`, and asserts that `parse_request` fails with the
    /// expected error about insufficient bytes.
    #[test]
    fn test_invalid_sasl_handshake_request() {
        use crate::common::ByteBufferAccessor;
        use crate::common::requests::ConcreteRequest;

        let mut data = SaslHandshakeRequestData::new();
        data.set_mechanism("PLAIN".to_string());
        let mut builder = SaslHandshakeRequestBuilder::new(data);
        let mut request = builder.build().unwrap();

        let serialized = request.serialize().unwrap();
        // Corrupt the length of the SASL mechanism string (i16 at offset 0)
        let mut corrupted = serialized.buffer().to_vec();
        let corrupted_len = i16::MAX.to_be_bytes();
        corrupted[0] = corrupted_len[0];
        corrupted[1] = corrupted_len[1];

        let mut buf = ByteBufferAccessor::new(corrupted);
        let err = ConcreteRequest::parse_request(request.api_key(), request.version(), &mut buf).unwrap_err();
        assert_eq!(
            err.to_string(),
            "Error reading byte array of 32767 byte(s): only 5 byte(s) available"
        );
    }
}
