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

//! SASL authenticate request handling.
//!
//! Corresponds to `org.apache.kafka.common.requests.SaslAuthenticateRequest`.
//!
//! Request from SASL client containing client SASL authentication token as defined by the
//! SASL protocol for the configured SASL mechanism.
//!
//! For interoperability with versions prior to Kafka 1.0.0, this request is used only with broker
//! version 1.0.0 and higher that support SaslHandshake request v1. Clients connecting to older
//! brokers will send SaslHandshake request v0 followed by SASL tokens without the Kafka request
//! headers.

use std::io;

use crate::common::protocol::{ApiKeys, Errors, Readable};
use crate::sasl_authenticate_request_data::SaslAuthenticateRequestData;
use crate::sasl_authenticate_response_data::SaslAuthenticateResponseData;

use super::ConcreteRequest;
use super::ConcreteResponse;
use super::RequestBuilder;
use super::SaslAuthenticateResponse;

/// A SASL authenticate request.
///
/// Corresponds to `org.apache.kafka.common.requests.SaslAuthenticateRequest`.
#[derive(Debug, Clone)]
pub struct SaslAuthenticateRequest {
    data: SaslAuthenticateRequestData,
    version: i16,
}

impl SaslAuthenticateRequest {
    /// Creates a new `SaslAuthenticateRequest` from data and version.
    pub fn new(data: SaslAuthenticateRequestData, version: i16) -> Self {
        Self { data, version }
    }

    /// Returns a reference to the underlying data.
    pub fn data(&self) -> &SaslAuthenticateRequestData {
        &self.data
    }

    /// Returns the API version of this request.
    pub fn version(&self) -> i16 {
        self.version
    }

    /// Returns the API key for this request.
    pub fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::SASL_AUTHENTICATE
    }

    /// Creates an error response for this request.
    ///
    /// The `throttle_time_ms` parameter is ignored because the SaslAuthenticate schema
    /// does not include a throttle time field.
    pub fn get_error_response(&self, _throttle_time_ms: i32, error: &Errors) -> ConcreteResponse {
        let mut response = SaslAuthenticateResponseData::new();
        response.set_error_code(error.code());
        response.set_error_message(Some(error.message().to_string()));
        ConcreteResponse::SaslAuthenticate(SaslAuthenticateResponse::new(response))
    }

    /// Parses a `SaslAuthenticateRequest` from a readable buffer at the given version.
    ///
    /// # Errors
    ///
    /// Returns an error if parsing fails.
    pub fn parse(readable: &mut dyn Readable, version: i16) -> io::Result<Self> {
        let data = SaslAuthenticateRequestData::read(readable, version)?;
        Ok(Self::new(data, version))
    }
}

/// Display implementation that redacts auth bytes for security.
///
/// Clones the data and clears auth_bytes before printing to avoid logging credentials.
impl std::fmt::Display for SaslAuthenticateRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut temp_data = self.data.clone();
        temp_data.set_auth_bytes(Vec::new());
        write!(f, "{}", temp_data)
    }
}

/// Builder for [`SaslAuthenticateRequest`].
///
/// Corresponds to `SaslAuthenticateRequest.Builder` in Java.
#[derive(Debug, Clone)]
pub struct SaslAuthenticateRequestBuilder {
    data: SaslAuthenticateRequestData,
    oldest_allowed_version: i16,
    latest_allowed_version: i16,
}

impl SaslAuthenticateRequestBuilder {
    /// Creates a new builder from the given data.
    pub fn new(data: SaslAuthenticateRequestData) -> Self {
        Self {
            data,
            oldest_allowed_version: ApiKeys::SASL_AUTHENTICATE.oldest_version(),
            latest_allowed_version: ApiKeys::SASL_AUTHENTICATE.latest_version(),
        }
    }
}

impl RequestBuilder for SaslAuthenticateRequestBuilder {
    fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::SASL_AUTHENTICATE
    }

    fn oldest_allowed_version(&self) -> i16 {
        self.oldest_allowed_version
    }

    fn latest_allowed_version(&self) -> i16 {
        self.latest_allowed_version
    }

    fn build_version(&self, version: i16) -> io::Result<ConcreteRequest> {
        Ok(ConcreteRequest::SaslAuthenticate(SaslAuthenticateRequest::new(
            self.data.clone(),
            version,
        )))
    }
}

impl std::fmt::Display for SaslAuthenticateRequestBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "(type=SaslAuthenticateRequest)")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_builder_version_range() {
        let data = SaslAuthenticateRequestData::new();
        let builder = SaslAuthenticateRequestBuilder::new(data);
        assert_eq!(*builder.api_key(), ApiKeys::SASL_AUTHENTICATE);
        assert_eq!(builder.oldest_allowed_version(), ApiKeys::SASL_AUTHENTICATE.oldest_version());
        assert_eq!(builder.latest_allowed_version(), ApiKeys::SASL_AUTHENTICATE.latest_version());
    }

    #[test]
    fn test_builder_build() {
        let mut data = SaslAuthenticateRequestData::new();
        data.set_auth_bytes(vec![1, 2, 3]);
        let builder = SaslAuthenticateRequestBuilder::new(data);
        let request = builder.build().unwrap();
        assert_eq!(*request.api_key(), ApiKeys::SASL_AUTHENTICATE);
        if let ConcreteRequest::SaslAuthenticate(r) = &request {
            assert_eq!(r.data().auth_bytes, vec![1, 2, 3]);
        } else {
            panic!("Expected SaslAuthenticate request");
        }
    }

    #[test]
    fn test_get_error_response() {
        let data = SaslAuthenticateRequestData::new();
        let request = SaslAuthenticateRequest::new(data, 0);
        let response = request.get_error_response(100, &Errors::SaslAuthenticationFailed);
        let ConcreteResponse::SaslAuthenticate(r) = &response else {
            panic!("Expected SaslAuthenticate response");
        };
        assert_eq!(r.data().error_code, Errors::SaslAuthenticationFailed.code());
        assert!(r.data().error_message.is_some());
        assert_eq!(
            r.data().error_message.as_deref().unwrap(),
            Errors::SaslAuthenticationFailed.message()
        );
    }

    /// Translated from `RequestResponseTest.testSaslAuthenticateRequestResponseToStringMasksSensitiveData`
    /// (request portion).
    ///
    /// Verifies that auth_bytes field is present but empty in the Display output,
    /// matching the Java assertion `assertTrue(requestString.contains("authBytes=[]"))`.
    #[test]
    fn test_display_redacted() {
        let mut data = SaslAuthenticateRequestData::new();
        data.set_auth_bytes(b"sensitive-auth-token-123".to_vec());
        let request = SaslAuthenticateRequest::new(data, 2);
        let display = format!("{}", request);
        // Assert the positive condition: auth_bytes is present but empty in output
        assert!(
            display.contains("auth_bytes: []"),
            "auth_bytes field should be empty in Display output, got: {}",
            display
        );
    }

    #[test]
    fn test_builder_display() {
        let data = SaslAuthenticateRequestData::new();
        let builder = SaslAuthenticateRequestBuilder::new(data);
        assert_eq!(format!("{}", builder), "(type=SaslAuthenticateRequest)");
    }

    #[test]
    fn test_parse_roundtrip() {
        for version in SaslAuthenticateRequestData::LOWEST_SUPPORTED_VERSION
            ..=SaslAuthenticateRequestData::HIGHEST_SUPPORTED_VERSION
        {
            let mut data = SaslAuthenticateRequestData::new();
            data.set_auth_bytes(vec![10, 20, 30]);
            let original = SaslAuthenticateRequest::new(data, version);

            let serialized = ConcreteRequest::SaslAuthenticate(original.clone()).serialize().unwrap();
            let mut buf = serialized;
            let parsed = SaslAuthenticateRequest::parse(&mut buf, version).unwrap();
            assert_eq!(original.data().auth_bytes, parsed.data().auth_bytes);
            assert_eq!(original.version(), parsed.version());
        }
    }

    /// Translated from `RequestResponseTest.testInvalidSaslAuthenticateRequest`.
    ///
    /// Serializes a SaslAuthenticateRequest at version 1, corrupts the auth_bytes
    /// array length (i32 at offset 0) to `i32::MAX`, and asserts that `parse_request`
    /// fails with the expected error about insufficient bytes.
    #[test]
    fn test_invalid_sasl_authenticate_request() {
        use crate::common::protocol::ByteBufferAccessor;

        let version: i16 = 1; // fixed-length encoding for simplicity
        let b: Vec<u8> = vec![
            0x11, 0x1f, 0x15, 0x2c, 0x5e, 0x2a, 0x20, 0x26, 0x6c, 0x39, 0x45, 0x1f, 0x25, 0x1c, 0x2d, 0x25, 0x43, 0x2a,
            0x11, 0x76,
        ];
        let mut data = SaslAuthenticateRequestData::new();
        data.set_auth_bytes(b);
        let request = SaslAuthenticateRequest::new(data, version);

        let serialized = ConcreteRequest::SaslAuthenticate(request.clone()).serialize().unwrap();

        // Corrupt the length of the bytes array (i32 at offset 0)
        let mut corrupted = serialized.buffer().to_vec();
        let corrupted_len = i32::MAX.to_be_bytes();
        corrupted[0] = corrupted_len[0];
        corrupted[1] = corrupted_len[1];
        corrupted[2] = corrupted_len[2];
        corrupted[3] = corrupted_len[3];

        let mut buf = ByteBufferAccessor::from_bytes(corrupted);
        let err = ConcreteRequest::parse_request(request.api_key(), request.version(), &mut buf).unwrap_err();
        assert_eq!(
            err.to_string(),
            "Error reading byte array of 2147483647 byte(s): only 20 byte(s) available"
        );
    }

    /// Translated from `RequestResponseTest.testValidTaggedFieldsWithSaslAuthenticateRequest`.
    ///
    /// Manually constructs a byte buffer with a SASL_AUTHENTICATE request at the latest
    /// version including a valid tagged field, parses it, and verifies authBytes and
    /// unknown_tagged_fields are preserved.
    #[test]
    fn test_valid_tagged_fields_with_sasl_authenticate_request() {
        use crate::common::protocol::{ByteBufferAccessor, RawTaggedField, Writable};

        let mut accessor = ByteBufferAccessor::new(11);

        // Construct a SASL_AUTHENTICATE request body
        let auth_bytes = b"test";
        accessor.write_unsigned_varint(auth_bytes.len() as u32 + 1).unwrap();
        accessor.write_byte_array(auth_bytes).unwrap();

        // Write total number of tags
        accessor.write_unsigned_varint(1).unwrap();

        // Write first tag
        let tagged_field = RawTaggedField::new(1, vec![0x1, 0x2, 0x3]);
        accessor.write_unsigned_varint(tagged_field.tag()).unwrap();
        accessor.write_unsigned_varint(tagged_field.size() as u32).unwrap();
        accessor.write_byte_array(tagged_field.data()).unwrap();

        accessor.flip();

        let latest_version = SaslAuthenticateRequestData::HIGHEST_SUPPORTED_VERSION;
        let result =
            ConcreteRequest::parse_request(&ApiKeys::SASL_AUTHENTICATE, latest_version, &mut accessor).unwrap();

        let ConcreteRequest::SaslAuthenticate(sasl_request) = &result.request else {
            panic!("Expected SaslAuthenticate request");
        };
        assert_eq!(sasl_request.data().auth_bytes, auth_bytes);
        assert_eq!(sasl_request.data().unknown_tagged_fields.len(), 1);
        assert_eq!(sasl_request.data().unknown_tagged_fields[0], tagged_field);
    }

    /// Translated from `RequestResponseTest.testInvalidTaggedFieldsWithSaslAuthenticateRequest`.
    ///
    /// Same as the valid test but with a corrupted tagged field size (`i16::MAX`),
    /// verifies that parse fails with the expected error.
    #[test]
    fn test_invalid_tagged_fields_with_sasl_authenticate_request() {
        use crate::common::protocol::{ByteBufferAccessor, RawTaggedField, Writable};

        let mut accessor = ByteBufferAccessor::new(13);

        // Construct a SASL_AUTHENTICATE request body
        let auth_bytes = b"test";
        accessor.write_unsigned_varint(auth_bytes.len() as u32 + 1).unwrap();
        accessor.write_byte_array(auth_bytes).unwrap();

        // Write total number of tags
        accessor.write_unsigned_varint(1).unwrap();

        // Write first tag
        let tagged_field = RawTaggedField::new(1, vec![0x1, 0x2, 0x3]);
        accessor.write_unsigned_varint(tagged_field.tag()).unwrap();
        accessor.write_unsigned_varint(i16::MAX as u32).unwrap(); // set wrong size for tagged field
        accessor.write_byte_array(tagged_field.data()).unwrap();

        accessor.flip();

        let latest_version = SaslAuthenticateRequestData::HIGHEST_SUPPORTED_VERSION;
        let err =
            ConcreteRequest::parse_request(&ApiKeys::SASL_AUTHENTICATE, latest_version, &mut accessor).unwrap_err();
        assert_eq!(
            err.to_string(),
            "Error reading byte array of 32767 byte(s): only 3 byte(s) available"
        );
    }
}
