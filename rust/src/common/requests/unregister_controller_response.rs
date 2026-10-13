// Copyright 2026 Confluent Inc.
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

//! UnregisterController response handling (KAFKA-20395).
//!
//! Corresponds to `org.apache.kafka.common.requests.UnregisterControllerResponse`.

use std::collections::HashMap;
use std::io;

use crate::UnregisterControllerResponseData;
use crate::common::protocol::{ApiKeys, Errors, Readable};

/// An UnregisterController response.
///
/// Corresponds to `org.apache.kafka.common.requests.UnregisterControllerResponse`.
#[derive(Debug, Clone)]
#[doc(alias = "org.apache.kafka.common.requests.UnregisterControllerResponse")]
pub struct UnregisterControllerResponse {
    data: UnregisterControllerResponseData,
}

impl UnregisterControllerResponse {
    /// Creates a new `UnregisterControllerResponse` from the underlying data.
    #[doc(alias = "org.apache.kafka.common.requests.UnregisterControllerResponse#UnregisterControllerResponse")]
    pub fn new(data: UnregisterControllerResponseData) -> Self {
        Self { data }
    }

    /// Returns the API key for this response.
    pub fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::UNREGISTER_CONTROLLER
    }

    /// Returns a reference to the underlying data.
    #[doc(alias = "org.apache.kafka.common.requests.UnregisterControllerResponse#data")]
    pub fn data(&self) -> &UnregisterControllerResponseData {
        &self.data
    }

    /// Returns a mutable reference to the underlying data.
    pub(crate) fn data_mut(&mut self) -> &mut UnregisterControllerResponseData {
        &mut self.data
    }

    /// Returns the throttle time in milliseconds.
    #[doc(alias = "org.apache.kafka.common.requests.UnregisterControllerResponse#throttleTimeMs")]
    pub fn throttle_time_ms(&self) -> i32 {
        self.data.throttle_time_ms()
    }

    /// Sets the throttle time in milliseconds.
    #[doc(alias = "org.apache.kafka.common.requests.UnregisterControllerResponse#maybeSetThrottleTimeMs")]
    pub fn maybe_set_throttle_time_ms(&mut self, throttle_time_ms: i32) {
        self.data.set_throttle_time_ms(throttle_time_ms);
    }

    /// Returns the error counts for this response.
    ///
    /// Mirrors Java, which counts the top-level error only when it is not
    /// `NONE` (`if (data.errorCode() != 0)`), so a successful response has an
    /// empty map.
    #[doc(alias = "org.apache.kafka.common.requests.UnregisterControllerResponse#errorCounts")]
    pub fn error_counts(&self) -> HashMap<Errors, i32> {
        let mut error_counts = HashMap::new();
        if self.data.error_code() != 0 {
            error_counts.insert(Errors::for_code(self.data.error_code()), 1);
        }
        error_counts
    }

    /// Parses an `UnregisterControllerResponse` from a readable buffer at the
    /// given version.
    ///
    /// # Errors
    ///
    /// Returns an error if parsing fails.
    #[doc(alias = "org.apache.kafka.common.requests.UnregisterControllerResponse#parse")]
    pub fn parse(readable: &mut dyn Readable, version: i16) -> io::Result<Self> {
        let data = UnregisterControllerResponseData::read(readable, version)?;
        Ok(Self::new(data))
    }
}

impl std::fmt::Display for UnregisterControllerResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "UnregisterControllerResponse(data={:?})", self.data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::protocol::ByteBufferAccessor;
    use crate::common::requests::ConcreteResponse;

    fn data(throttle_time_ms: i32, error: Errors, message: Option<&str>) -> UnregisterControllerResponseData {
        let mut data = UnregisterControllerResponseData::new();
        data.set_throttle_time_ms(throttle_time_ms);
        data.set_error_code(error.code());
        data.set_error_message(message.map(str::to_string));
        data
    }

    fn serialize(response: UnregisterControllerResponse) -> Vec<u8> {
        let mut concrete = ConcreteResponse::UnregisterController(response);
        concrete.serialize(0).unwrap().into_buffer()
    }

    /// Byte-level encoding test against a known vector. UnregisterController v0
    /// is flexible, so the body is:
    ///   throttle_time_ms: int32 = 10 (00 00 00 0a)
    ///   error_code: int16 = 136 (00 88), CONTROLLER_ID_NOT_REGISTERED
    ///   error_message: compact nullable string "no" (len+1 = 0x03, 0x6e 0x6f)
    ///   _tagged_fields: 0x00
    #[test]
    fn serialize_known_byte_vector_v0_with_message() {
        let response = UnregisterControllerResponse::new(data(10, Errors::ControllerIdNotRegistered, Some("no")));
        let expected: &[u8] = &[
            0x00, 0x00, 0x00, 0x0a, // throttle_time_ms = 10
            0x00, 0x88, // error_code = 136
            0x03, 0x6e, 0x6f, // error_message "no"
            0x00, // tagged fields
        ];
        assert_eq!(serialize(response).as_slice(), expected);
    }

    /// A null message is the compact-nullable zero length.
    #[test]
    fn serialize_known_byte_vector_v0_null_message() {
        let response = UnregisterControllerResponse::new(data(0, Errors::None, None));
        let expected: &[u8] = &[
            0x00, 0x00, 0x00, 0x00, // throttle_time_ms = 0
            0x00, 0x00, // error_code = 0
            0x00, // error_message null
            0x00, // tagged fields
        ];
        assert_eq!(serialize(response).as_slice(), expected);
    }

    /// Java's `createUnregisterControllerResponse` (`new
    /// UnregisterControllerResponseData()`) leaves the nullable message at its
    /// generated default. The spec has no `"default": "null"`, so that default
    /// is the empty string (CLAUDE.md §2), encoded as compact length 1.
    #[test]
    fn serialize_default_data_v0() {
        let response = UnregisterControllerResponse::new(UnregisterControllerResponseData::new());
        let expected: &[u8] = &[
            0x00, 0x00, 0x00, 0x00, // throttle_time_ms = 0
            0x00, 0x00, // error_code = 0
            0x01, // error_message ""
            0x00, // tagged fields
        ];
        assert_eq!(serialize(response).as_slice(), expected);
    }

    /// Parses the known vector back, directly and through the shared
    /// `parse_response` dispatcher (the `AbstractResponse.parseResponse` arm).
    #[test]
    fn parse_known_byte_vector_v0() {
        let bytes = vec![0x00, 0x00, 0x00, 0x0a, 0x00, 0x88, 0x03, 0x6e, 0x6f, 0x00];
        let mut readable = ByteBufferAccessor::new(bytes.clone());
        let parsed = UnregisterControllerResponse::parse(&mut readable, 0).unwrap();
        assert_eq!(parsed.throttle_time_ms(), 10);
        assert_eq!(parsed.data().error_code(), Errors::ControllerIdNotRegistered.code());
        assert_eq!(parsed.data().error_message().as_deref(), Some("no"));

        let mut readable = ByteBufferAccessor::new(bytes);
        let parsed = ConcreteResponse::parse(&ApiKeys::UNREGISTER_CONTROLLER, &mut readable, 0).unwrap();
        let ConcreteResponse::UnregisterController(parsed) = parsed else {
            panic!("expected an UnregisterController response");
        };
        assert_eq!(parsed.data().error_message().as_deref(), Some("no"));
    }

    /// `errorCounts` skips `NONE`, unlike the single-error helper most
    /// responses use.
    #[test]
    fn error_counts_skip_none() {
        assert!(
            UnregisterControllerResponse::new(data(0, Errors::None, None))
                .error_counts()
                .is_empty()
        );
        let counts = UnregisterControllerResponse::new(data(0, Errors::NotController, None)).error_counts();
        assert_eq!(counts.len(), 1);
        assert_eq!(counts.get(&Errors::NotController).copied(), Some(1));
    }

    #[test]
    fn maybe_set_throttle_time_ms_updates_data() {
        let mut response = UnregisterControllerResponse::new(data(0, Errors::None, None));
        response.maybe_set_throttle_time_ms(250);
        assert_eq!(response.throttle_time_ms(), 250);
    }
}
