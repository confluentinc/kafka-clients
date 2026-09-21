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

//! AlterUserScramCredentials response handling.
//!
//! Corresponds to
//! `org.apache.kafka.common.requests.AlterUserScramCredentialsResponse`.

use std::collections::HashMap;
use std::io;

use crate::AlterUserScramCredentialsResponseData;
use crate::common::protocol::{ApiKeys, Errors, Readable};

use super::AbstractResponse;

/// An AlterUserScramCredentials response.
///
/// Corresponds to
/// `org.apache.kafka.common.requests.AlterUserScramCredentialsResponse`.
#[derive(Debug, Clone)]
pub struct AlterUserScramCredentialsResponse {
    data: AlterUserScramCredentialsResponseData,
    #[allow(dead_code)]
    version: i16,
}

impl AlterUserScramCredentialsResponse {
    /// Creates a new response from data and version.
    pub fn new(data: AlterUserScramCredentialsResponseData, version: i16) -> Self {
        Self { data, version }
    }

    /// Returns the API key for this response.
    pub fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::ALTER_USER_SCRAM_CREDENTIALS
    }

    /// Returns a reference to the underlying data.
    pub fn data(&self) -> &AlterUserScramCredentialsResponseData {
        &self.data
    }

    /// Returns a mutable reference to the underlying data.
    pub(crate) fn data_mut(&mut self) -> &mut AlterUserScramCredentialsResponseData {
        &mut self.data
    }

    /// Returns the throttle time in milliseconds.
    pub fn throttle_time_ms(&self) -> i32 {
        self.data.throttle_time_ms
    }

    /// Sets the throttle time in the response.
    pub fn maybe_set_throttle_time_ms(&mut self, throttle_time_ms: i32) {
        self.data.set_throttle_time_ms(throttle_time_ms);
    }

    /// Returns the error counts aggregated for this response.
    ///
    /// Mirrors `AlterUserScramCredentialsResponse.errorCounts`: one count per
    /// user-level result error code.
    pub fn error_counts(&self) -> HashMap<Errors, i32> {
        let mut counts = HashMap::new();
        for result in &self.data.results {
            AbstractResponse::update_error_counts(&mut counts, Errors::for_code(result.error_code));
        }
        counts
    }

    /// Parses a response from a readable buffer at the given version.
    ///
    /// # Errors
    ///
    /// Returns an error if parsing fails.
    pub fn parse(readable: &mut dyn Readable, version: i16) -> io::Result<Self> {
        let data = AlterUserScramCredentialsResponseData::read(readable, version)?;
        Ok(Self::new(data, version))
    }

    /// Whether the client should throttle on this response.
    ///
    /// Mirrors `AlterUserScramCredentialsResponse.shouldClientThrottle`
    /// (always `true`).
    pub fn should_client_throttle(&self, _version: i16) -> bool {
        true
    }
}

impl std::fmt::Display for AlterUserScramCredentialsResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "AlterUserScramCredentialsResponse(data={:?})", self.data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::alter_user_scram_credentials_response_data::AlterUserScramCredentialsResult;

    #[test]
    fn error_counts_aggregate_per_result() {
        let mut data = AlterUserScramCredentialsResponseData::new();
        let mut ok = AlterUserScramCredentialsResult::new();
        ok.set_user("u0".to_string()).set_error_code(Errors::None.code());
        let mut fenced = AlterUserScramCredentialsResult::new();
        fenced.set_user("u1".to_string()).set_error_code(Errors::NotController.code());
        data.set_results(vec![ok, fenced]);
        let response = AlterUserScramCredentialsResponse::new(data, 0);
        let counts = response.error_counts();
        assert_eq!(counts.get(&Errors::None), Some(&1));
        assert_eq!(counts.get(&Errors::NotController), Some(&1));
    }

    /// Byte-level wire-decoding check for the v0 (flexible) response.
    ///   throttle_time_ms 0     -> 0x00 0x00 0x00 0x00
    ///   results: compact len 1 -> 0x02
    ///     user "u0"            -> 0x03 0x75 0x30
    ///     error_code 0         -> 0x00 0x00
    ///     error_message null   -> 0x00
    ///     tagged fields        -> 0x00
    ///   tagged fields          -> 0x00
    #[test]
    fn parse_known_byte_vector_v0() {
        let bytes = vec![
            0x00, 0x00, 0x00, 0x00, // throttle_time_ms 0
            0x02, // results len 1
            0x03, 0x75, 0x30, // user "u0"
            0x00, 0x00, // error_code 0
            0x00, // error_message null
            0x00, // result tagged fields
            0x00, // response tagged fields
        ];
        let mut readable = crate::common::ByteBufferAccessor::new(bytes);
        let parsed = AlterUserScramCredentialsResponse::parse(&mut readable, 0).unwrap();
        assert_eq!(parsed.data().results.len(), 1);
        assert_eq!(parsed.data().results[0].user, "u0");
        assert_eq!(parsed.data().results[0].error_code, 0);
    }

    #[test]
    fn always_throttles() {
        let response = AlterUserScramCredentialsResponse::new(AlterUserScramCredentialsResponseData::new(), 0);
        assert!(response.should_client_throttle(0));
    }
}
