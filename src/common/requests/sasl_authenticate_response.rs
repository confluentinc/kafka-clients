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

//! SASL authenticate response handling.
//!
//! Corresponds to `org.apache.kafka.common.requests.SaslAuthenticateResponse`.
//!
//! Response from SASL server which for a SASL challenge as defined by the SASL protocol
//! for the mechanism configured for the client.

use std::collections::HashMap;

use crate::common::protocol::{ApiKeys, Errors, Readable};
use crate::sasl_authenticate_response_data::SaslAuthenticateResponseData;

use super::abstract_response::DEFAULT_THROTTLE_TIME;

/// Possible error codes:
/// - [`Errors::SaslAuthenticationFailed`] (57): Authentication failed
#[derive(Debug, Clone)]
pub struct SaslAuthenticateResponse {
    data: SaslAuthenticateResponseData,
}

impl SaslAuthenticateResponse {
    /// Creates a new `SaslAuthenticateResponse` from data.
    pub fn new(data: SaslAuthenticateResponseData) -> Self {
        Self { data }
    }

    /// Returns a reference to the underlying data.
    pub fn data(&self) -> &SaslAuthenticateResponseData {
        &self.data
    }

    /// Returns the API key for this response.
    pub fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::SASL_AUTHENTICATE
    }

    /// Returns the error from this response.
    pub fn error(&self) -> Errors {
        Errors::for_code(self.data.error_code)
    }

    /// Returns the error counts for this response.
    pub fn error_counts(&self) -> HashMap<Errors, i32> {
        super::abstract_response::single_error_count(Errors::for_code(self.data.error_code))
    }

    /// Returns the error message, or `None` if there was no error.
    pub fn error_message(&self) -> Option<&str> {
        self.data.error_message.as_deref()
    }

    /// Returns the session lifetime in milliseconds.
    pub fn session_lifetime_ms(&self) -> i64 {
        self.data.session_lifetime_ms
    }

    /// Returns the SASL authentication bytes from the server.
    pub fn sasl_auth_bytes(&self) -> &[u8] {
        &self.data.auth_bytes
    }

    /// Returns the throttle time in milliseconds.
    ///
    /// Always returns [`DEFAULT_THROTTLE_TIME`] (0) because the SaslAuthenticate schema
    /// does not support throttle time.
    pub fn throttle_time_ms(&self) -> i32 {
        DEFAULT_THROTTLE_TIME
    }

    /// No-op: the SaslAuthenticate schema does not support throttle time.
    pub fn maybe_set_throttle_time_ms(&mut self, _throttle_time_ms: i32) {
        // Not supported by the response schema
    }

    /// Returns whether the client should throttle upon receiving this response.
    ///
    /// Always returns `false` for SASL authenticate responses.
    pub fn should_client_throttle(&self, _version: i16) -> bool {
        false
    }

    /// Parses a `SaslAuthenticateResponse` from a readable buffer at the given version.
    ///
    /// # Errors
    ///
    /// Returns an error if parsing fails.
    pub fn parse(readable: &mut dyn Readable, version: i16) -> std::io::Result<Self> {
        let data = SaslAuthenticateResponseData::read(readable, version)?;
        Ok(Self::new(data))
    }
}

/// Display implementation that redacts auth bytes for security.
///
/// Clones the data and clears auth_bytes before printing to avoid logging credentials.
impl std::fmt::Display for SaslAuthenticateResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut temp_data = self.data.clone();
        temp_data.set_auth_bytes(Vec::new());
        write!(f, "SaslAuthenticateResponse(data={:?})", temp_data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::requests::ConcreteResponse;

    #[test]
    fn test_error() {
        let mut data = SaslAuthenticateResponseData::new();
        data.set_error_code(Errors::SaslAuthenticationFailed.code());
        let response = SaslAuthenticateResponse::new(data);
        assert_eq!(response.error(), Errors::SaslAuthenticationFailed);
    }

    #[test]
    fn test_error_message() {
        let mut data = SaslAuthenticateResponseData::new();
        data.set_error_message(Some("auth failed".to_string()));
        let response = SaslAuthenticateResponse::new(data);
        assert_eq!(response.error_message(), Some("auth failed"));
    }

    #[test]
    fn test_error_message_null() {
        let mut data = SaslAuthenticateResponseData::new();
        data.set_error_message(None);
        let response = SaslAuthenticateResponse::new(data);
        assert_eq!(response.error_message(), None);
    }

    #[test]
    fn test_session_lifetime_ms() {
        let mut data = SaslAuthenticateResponseData::new();
        data.set_session_lifetime_ms(3600000);
        let response = SaslAuthenticateResponse::new(data);
        assert_eq!(response.session_lifetime_ms(), 3600000);
    }

    #[test]
    fn test_sasl_auth_bytes() {
        let mut data = SaslAuthenticateResponseData::new();
        data.set_auth_bytes(vec![1, 2, 3, 4]);
        let response = SaslAuthenticateResponse::new(data);
        assert_eq!(response.sasl_auth_bytes(), &[1, 2, 3, 4]);
    }

    #[test]
    fn test_error_counts() {
        let mut data = SaslAuthenticateResponseData::new();
        data.set_error_code(Errors::SaslAuthenticationFailed.code());
        let response = SaslAuthenticateResponse::new(data);
        let counts = response.error_counts();
        assert_eq!(counts.get(&Errors::SaslAuthenticationFailed), Some(&1));
    }

    #[test]
    fn test_throttle_time_ms() {
        let data = SaslAuthenticateResponseData::new();
        let response = SaslAuthenticateResponse::new(data);
        assert_eq!(response.throttle_time_ms(), DEFAULT_THROTTLE_TIME);
    }

    #[test]
    fn test_should_client_throttle() {
        let data = SaslAuthenticateResponseData::new();
        let response = SaslAuthenticateResponse::new(data);
        for version in SaslAuthenticateResponseData::LOWEST_SUPPORTED_VERSION
            ..=SaslAuthenticateResponseData::HIGHEST_SUPPORTED_VERSION
        {
            assert!(!response.should_client_throttle(version));
        }
    }

    /// Translated from `RequestResponseTest.testSaslAuthenticateRequestResponseToStringMasksSensitiveData`
    /// (response portion).
    ///
    /// Verifies that auth_bytes field is present but empty in the Display output,
    /// matching the Java assertion `assertTrue(responseString.contains("authBytes=[]"))`.
    #[test]
    fn test_display_redacted() {
        let mut data = SaslAuthenticateResponseData::new();
        data.set_auth_bytes(b"sensitive-auth-token-123".to_vec());
        let response = SaslAuthenticateResponse::new(data);
        let display = format!("{}", response);
        // Assert the positive condition: auth_bytes is present but empty in output
        assert!(
            display.contains("auth_bytes: []"),
            "auth_bytes field should be empty in Display output, got: {}",
            display
        );
    }

    #[test]
    fn test_parse_roundtrip() {
        for version in SaslAuthenticateResponseData::LOWEST_SUPPORTED_VERSION
            ..=SaslAuthenticateResponseData::HIGHEST_SUPPORTED_VERSION
        {
            let mut data = SaslAuthenticateResponseData::new();
            data.set_error_code(Errors::None.code());
            data.set_error_message(None);
            data.set_auth_bytes(vec![10, 20, 30]);
            if version >= 1 {
                data.set_session_lifetime_ms(60000);
            }
            let original = SaslAuthenticateResponse::new(data);

            let serialized = ConcreteResponse::SaslAuthenticate(original.clone()).serialize(version).unwrap();
            let mut buf = serialized;
            let parsed = SaslAuthenticateResponse::parse(&mut buf, version).unwrap();
            assert_eq!(original.data().error_code, parsed.data().error_code);
            assert_eq!(original.data().auth_bytes, parsed.data().auth_bytes);
            if version >= 1 {
                assert_eq!(original.data().session_lifetime_ms, parsed.data().session_lifetime_ms);
            }
        }
    }
}
