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

//! SASL handshake response handling.
//!
//! Corresponds to `org.apache.kafka.common.requests.SaslHandshakeResponse`.
//!
//! Response from SASL server which indicates if the client-chosen mechanism is enabled in the
//! server. For error responses, the list of enabled mechanisms is included in the response.

use std::collections::HashMap;

use crate::SaslHandshakeResponseData;
use crate::common::protocol::{ApiKeys, Errors, Readable};

use super::AbstractResponse;

/// Possible error codes:
/// - [`Errors::UnsupportedSaslMechanism`] (33): Client mechanism not enabled in server
/// - [`Errors::IllegalSaslState`] (34): Invalid request during SASL handshake
#[derive(Debug, Clone)]
#[doc(alias = "org.apache.kafka.common.requests.SaslHandshakeResponse")]
pub struct SaslHandshakeResponse {
    data: SaslHandshakeResponseData,
}

impl SaslHandshakeResponse {
    /// Creates a new `SaslHandshakeResponse` from data.
    #[doc(alias = "org.apache.kafka.common.requests.SaslHandshakeResponse#SaslHandshakeResponse")]
    pub fn new(data: SaslHandshakeResponseData) -> Self {
        Self { data }
    }

    /// Returns a reference to the underlying data.
    #[doc(alias = "org.apache.kafka.common.requests.SaslHandshakeResponse#data")]
    pub fn data(&self) -> &SaslHandshakeResponseData {
        &self.data
    }

    /// Returns a mutable reference to the underlying data.
    pub(crate) fn data_mut(&mut self) -> &mut SaslHandshakeResponseData {
        &mut self.data
    }

    /// Returns the API key for this response.
    pub fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::SASL_HANDSHAKE
    }

    /// Returns the error from this response.
    #[doc(alias = "org.apache.kafka.common.requests.SaslHandshakeResponse#error")]
    pub fn error(&self) -> Errors {
        Errors::for_code(self.data.error_code)
    }

    /// Returns the error counts for this response.
    #[doc(alias = "org.apache.kafka.common.requests.SaslHandshakeResponse#errorCounts")]
    pub fn error_counts(&self) -> HashMap<Errors, i32> {
        super::AbstractResponse::single_error_count(Errors::for_code(self.data.error_code))
    }

    /// Returns the throttle time in milliseconds.
    ///
    /// Always returns [`AbstractResponse::DEFAULT_THROTTLE_TIME`] (0) because the SaslHandshake schema
    /// does not support throttle time.
    #[doc(alias = "org.apache.kafka.common.requests.SaslHandshakeResponse#throttleTimeMs")]
    pub fn throttle_time_ms(&self) -> i32 {
        AbstractResponse::DEFAULT_THROTTLE_TIME
    }

    /// No-op: the SaslHandshake schema does not support throttle time.
    #[doc(alias = "org.apache.kafka.common.requests.SaslHandshakeResponse#maybeSetThrottleTimeMs")]
    pub fn maybe_set_throttle_time_ms(&mut self, _throttle_time_ms: i32) {
        // Not supported by the response schema
    }

    /// Returns whether the client should throttle upon receiving this response.
    ///
    /// Always returns `false` for SASL handshake responses.
    pub fn should_client_throttle(&self, _version: i16) -> bool {
        false
    }

    /// Returns the list of mechanisms enabled in the server.
    #[doc(alias = "org.apache.kafka.common.requests.SaslHandshakeResponse#enabledMechanisms")]
    pub fn enabled_mechanisms(&self) -> &[String] {
        &self.data.mechanisms
    }

    /// Parses a `SaslHandshakeResponse` from a readable buffer at the given version.
    ///
    /// # Errors
    ///
    /// Returns an error if parsing fails.
    #[doc(alias = "org.apache.kafka.common.requests.SaslHandshakeResponse#parse")]
    pub fn parse(readable: &mut dyn Readable, version: i16) -> std::io::Result<Self> {
        let data = SaslHandshakeResponseData::read(readable, version)?;
        Ok(Self::new(data))
    }
}

impl std::fmt::Display for SaslHandshakeResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SaslHandshakeResponse(data={:?})", self.data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::protocol::ByteBufferAccessor;
    use crate::common::requests::ConcreteResponse;

    #[test]
    fn test_error() {
        let mut data = SaslHandshakeResponseData::new();
        data.set_error_code(Errors::UnsupportedSaslMechanism.code());
        let response = SaslHandshakeResponse::new(data);
        assert_eq!(response.error(), Errors::UnsupportedSaslMechanism);
    }

    #[test]
    fn test_error_counts() {
        let mut data = SaslHandshakeResponseData::new();
        data.set_error_code(Errors::IllegalSaslState.code());
        let response = SaslHandshakeResponse::new(data);
        let counts = response.error_counts();
        assert_eq!(counts.get(&Errors::IllegalSaslState), Some(&1));
    }

    #[test]
    fn test_throttle_time_ms() {
        let data = SaslHandshakeResponseData::new();
        let response = SaslHandshakeResponse::new(data);
        assert_eq!(response.throttle_time_ms(), AbstractResponse::DEFAULT_THROTTLE_TIME);
    }

    #[test]
    fn test_maybe_set_throttle_time_ms_is_noop() {
        let data = SaslHandshakeResponseData::new();
        let mut response = SaslHandshakeResponse::new(data);
        response.maybe_set_throttle_time_ms(100);
        // Should still be 0 since it's a no-op
        assert_eq!(response.throttle_time_ms(), AbstractResponse::DEFAULT_THROTTLE_TIME);
    }

    #[test]
    fn test_should_client_throttle() {
        let data = SaslHandshakeResponseData::new();
        let response = SaslHandshakeResponse::new(data);
        for version in
            SaslHandshakeResponseData::LOWEST_SUPPORTED_VERSION..=SaslHandshakeResponseData::HIGHEST_SUPPORTED_VERSION
        {
            assert!(!response.should_client_throttle(version));
        }
    }

    #[test]
    fn test_enabled_mechanisms() {
        let mut data = SaslHandshakeResponseData::new();
        data.set_mechanisms(vec!["PLAIN".to_string(), "SCRAM-SHA-256".to_string()]);
        let response = SaslHandshakeResponse::new(data);
        assert_eq!(
            response.enabled_mechanisms(),
            &["PLAIN".to_string(), "SCRAM-SHA-256".to_string()]
        );
    }

    #[test]
    fn test_parse_roundtrip() {
        for version in
            SaslHandshakeResponseData::LOWEST_SUPPORTED_VERSION..=SaslHandshakeResponseData::HIGHEST_SUPPORTED_VERSION
        {
            let mut data = SaslHandshakeResponseData::new();
            data.set_error_code(Errors::None.code());
            data.set_mechanisms(vec!["PLAIN".to_string()]);
            let original = SaslHandshakeResponse::new(data);

            let serialized = ConcreteResponse::SaslHandshake(original.clone()).serialize(version).unwrap();
            let mut buf = serialized;
            let parsed = SaslHandshakeResponse::parse(&mut buf, version).unwrap();
            assert_eq!(original.data().error_code, parsed.data().error_code);
            assert_eq!(original.data().mechanisms, parsed.data().mechanisms);
        }
    }

    /// A negative INT16 element length is a null element, which Java rejects for the
    /// non-nullable `Mechanisms` array (`MessageDataGenerator.java:613-617`, via the
    /// element read at `:668-677`) rather than sizing a buffer from it. This response
    /// is decoded before SASL authentication completes. Neither version is flexible:
    ///   error_code: int16 0                -> 00 00
    ///   mechanisms: int32 array len 1      -> 00 00 00 01
    ///     element:  int16 string len < 0   -> FF FF (-1) or 80 00 (-32768)
    #[test]
    fn test_parse_rejects_negative_mechanism_length() {
        for version in
            SaslHandshakeResponseData::LOWEST_SUPPORTED_VERSION..=SaslHandshakeResponseData::HIGHEST_SUPPORTED_VERSION
        {
            for length in [[0xFF, 0xFF], [0x80, 0x00]] {
                let mut bytes = vec![0x00, 0x00, 0x00, 0x00, 0x00, 0x01];
                bytes.extend_from_slice(&length);
                let mut buf = ByteBufferAccessor::new(bytes);
                let error = SaslHandshakeResponse::parse(&mut buf, version).expect_err("a null element must not parse");
                assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
                assert_eq!(
                    error.to_string(),
                    "non-nullable field mechanisms element was serialized as null"
                );
            }

            // Zero is the boundary: an empty mechanism name is a valid, non-null element.
            let mut buf = ByteBufferAccessor::new(vec![0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00]);
            let parsed = SaslHandshakeResponse::parse(&mut buf, version).unwrap();
            assert_eq!(parsed.enabled_mechanisms(), &[String::new()]);
        }
    }
}
