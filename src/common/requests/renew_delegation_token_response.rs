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

//! RenewDelegationToken response handling.
//!
//! Corresponds to
//! `org.apache.kafka.common.requests.RenewDelegationTokenResponse`.

use std::collections::HashMap;
use std::io;

use crate::RenewDelegationTokenResponseData;
use crate::common::protocol::{ApiKeys, Errors, Readable};

use super::AbstractResponse;

/// A RenewDelegationToken response.
///
/// Corresponds to
/// `org.apache.kafka.common.requests.RenewDelegationTokenResponse`.
#[derive(Debug, Clone)]
pub struct RenewDelegationTokenResponse {
    data: RenewDelegationTokenResponseData,
}

impl RenewDelegationTokenResponse {
    /// Creates a new `RenewDelegationTokenResponse` from the underlying data.
    pub fn new(data: RenewDelegationTokenResponseData) -> Self {
        Self { data }
    }

    /// Prepares a response with the given throttle time and error.
    ///
    /// Mirrors the response constructed in
    /// `RenewDelegationTokenRequest.getErrorResponse`.
    pub fn prepare_response(throttle_time_ms: i32, error: Errors) -> Self {
        let mut data = RenewDelegationTokenResponseData::new();
        data.throttle_time_ms = throttle_time_ms;
        data.error_code = error.code();
        Self::new(data)
    }

    /// Returns the API key for this response.
    pub fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::RENEW_DELEGATION_TOKEN
    }

    /// Returns a reference to the underlying data.
    pub fn data(&self) -> &RenewDelegationTokenResponseData {
        &self.data
    }

    /// Returns a mutable reference to the underlying data.
    pub(crate) fn data_mut(&mut self) -> &mut RenewDelegationTokenResponseData {
        &mut self.data
    }

    /// Returns the top-level error.
    ///
    /// Mirrors `RenewDelegationTokenResponse.error`.
    pub fn error(&self) -> Errors {
        Errors::for_code(self.data.error_code)
    }

    /// Whether this response carries an error.
    ///
    /// Mirrors `RenewDelegationTokenResponse.hasError`.
    pub fn has_error(&self) -> bool {
        self.error() != Errors::None
    }

    /// Returns the expiry timestamp.
    ///
    /// Mirrors `RenewDelegationTokenResponse.expiryTimestamp`.
    pub fn expiry_timestamp(&self) -> i64 {
        self.data.expiry_timestamp_ms
    }

    /// Returns the throttle time in milliseconds.
    pub fn throttle_time_ms(&self) -> i32 {
        self.data.throttle_time_ms
    }

    /// Sets the throttle time in the response.
    pub fn maybe_set_throttle_time_ms(&mut self, throttle_time_ms: i32) {
        self.data.throttle_time_ms = throttle_time_ms;
    }

    /// Returns the error counts for this response.
    pub fn error_counts(&self) -> HashMap<Errors, i32> {
        AbstractResponse::single_error_count(self.error())
    }

    /// Parses a `RenewDelegationTokenResponse` from a readable buffer.
    ///
    /// # Errors
    ///
    /// Returns an error if parsing fails.
    pub fn parse(readable: &mut dyn Readable, version: i16) -> io::Result<Self> {
        let data = RenewDelegationTokenResponseData::read(readable, version)?;
        Ok(Self::new(data))
    }

    /// Whether the client should throttle on this response (v1+).
    pub fn should_client_throttle(&self, version: i16) -> bool {
        version >= 1
    }
}

impl std::fmt::Display for RenewDelegationTokenResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "RenewDelegationTokenResponse(data={:?})", self.data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prepare_response_sets_fields() {
        let response = RenewDelegationTokenResponse::prepare_response(75, Errors::DelegationTokenNotFound);
        assert_eq!(response.throttle_time_ms(), 75);
        assert_eq!(response.error(), Errors::DelegationTokenNotFound);
        assert!(response.has_error());
    }

    #[test]
    fn expiry_timestamp_accessor() {
        let mut data = RenewDelegationTokenResponseData::new();
        data.expiry_timestamp_ms = 123_456;
        let response = RenewDelegationTokenResponse::new(data);
        assert_eq!(response.expiry_timestamp(), 123_456);
        assert!(!response.has_error());
    }

    /// Byte-level wire vector for v2 (flexible).
    #[test]
    fn known_wire_vector_v2() {
        use crate::common::requests::ConcreteResponse;
        let mut data = RenewDelegationTokenResponseData::new();
        data.error_code = Errors::DelegationTokenNotFound.code(); // 62
        data.expiry_timestamp_ms = 100;
        data.throttle_time_ms = 5;
        let mut response = ConcreteResponse::RenewDelegationToken(RenewDelegationTokenResponse::new(data));
        let bytes = response.serialize(2).unwrap().into_buffer();
        let expected: Vec<u8> = vec![
            0x00, 0x3E, // error_code = 62 (int16 BE)
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x64, // expiry_timestamp_ms = 100 (int64 BE)
            0x00, 0x00, 0x00, 0x05, // throttle_time_ms = 5 (int32 BE)
            0x00, // response tagged fields
        ];
        assert_eq!(bytes.as_slice(), expected.as_slice());
    }
}
