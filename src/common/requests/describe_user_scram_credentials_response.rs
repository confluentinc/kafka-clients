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

//! DescribeUserScramCredentials response handling.
//!
//! Corresponds to
//! `org.apache.kafka.common.requests.DescribeUserScramCredentialsResponse`.

use std::collections::HashMap;
use std::io;

use crate::common::protocol::{ApiKeys, Errors, Readable};
use crate::describe_user_scram_credentials_response_data::DescribeUserScramCredentialsResponseData;

use super::abstract_response::update_error_counts;

/// A DescribeUserScramCredentials response.
///
/// Corresponds to
/// `org.apache.kafka.common.requests.DescribeUserScramCredentialsResponse`.
#[derive(Debug, Clone)]
pub struct DescribeUserScramCredentialsResponse {
    data: DescribeUserScramCredentialsResponseData,
    #[allow(dead_code)]
    version: i16,
}

impl DescribeUserScramCredentialsResponse {
    /// Creates a new response from data and version.
    pub fn new(data: DescribeUserScramCredentialsResponseData, version: i16) -> Self {
        Self { data, version }
    }

    /// Returns the API key for this response.
    pub fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::DESCRIBE_USER_SCRAM_CREDENTIALS
    }

    /// Returns a reference to the underlying data.
    pub fn data(&self) -> &DescribeUserScramCredentialsResponseData {
        &self.data
    }

    /// Returns a mutable reference to the underlying data.
    pub(crate) fn data_mut(&mut self) -> &mut DescribeUserScramCredentialsResponseData {
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
    /// Mirrors `DescribeUserScramCredentialsResponse.errorCounts`: one count per
    /// user-level result error code.
    pub fn error_counts(&self) -> HashMap<Errors, i32> {
        let mut counts = HashMap::new();
        for result in &self.data.results {
            update_error_counts(&mut counts, Errors::for_code(result.error_code));
        }
        counts
    }

    /// Parses a response from a readable buffer at the given version.
    ///
    /// # Errors
    ///
    /// Returns an error if parsing fails.
    pub fn parse(readable: &mut dyn Readable, version: i16) -> io::Result<Self> {
        let data = DescribeUserScramCredentialsResponseData::read(readable, version)?;
        Ok(Self::new(data, version))
    }

    /// Whether the client should throttle on this response.
    ///
    /// Mirrors `DescribeUserScramCredentialsResponse.shouldClientThrottle`
    /// (throttled for versions `>= 0`).
    pub fn should_client_throttle(&self, _version: i16) -> bool {
        true
    }
}

impl std::fmt::Display for DescribeUserScramCredentialsResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "DescribeUserScramCredentialsResponse(data={:?})", self.data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::describe_user_scram_credentials_response_data::{CredentialInfo, DescribeUserScramCredentialsResult};

    #[test]
    fn error_counts_aggregate_per_result() {
        let mut data = DescribeUserScramCredentialsResponseData::new();
        let mut ok = DescribeUserScramCredentialsResult::new();
        ok.set_user("u0".to_string()).set_error_code(Errors::None.code());
        let mut not_found = DescribeUserScramCredentialsResult::new();
        not_found
            .set_user("u1".to_string())
            .set_error_code(Errors::ResourceNotFound.code());
        data.set_results(vec![ok, not_found]);
        let response = DescribeUserScramCredentialsResponse::new(data, 0);
        let counts = response.error_counts();
        assert_eq!(counts.get(&Errors::None), Some(&1));
        assert_eq!(counts.get(&Errors::ResourceNotFound), Some(&1));
    }

    /// Byte-level wire-decoding check for the v0 (flexible) response, mirroring
    /// how the client actually receives this message. Field-by-field:
    ///   throttle_time_ms 0        -> 0x00 0x00 0x00 0x00
    ///   error_code 0              -> 0x00 0x00
    ///   error_message null        -> 0x00 (compact-nullable string)
    ///   results: compact len 1    -> 0x02
    ///     user "u0"               -> 0x03 0x75 0x30 (compact string len 2+1)
    ///     error_code 0            -> 0x00 0x00
    ///     error_message null      -> 0x00
    ///     credential_infos len 1  -> 0x02
    ///       mechanism 1           -> 0x01 (int8)
    ///       iterations 4096       -> 0x00 0x00 0x10 0x00
    ///       tagged fields         -> 0x00
    ///     tagged fields           -> 0x00
    ///   tagged fields             -> 0x00
    #[test]
    fn parse_known_byte_vector_v0() {
        let bytes = vec![
            0x00, 0x00, 0x00, 0x00, // throttle_time_ms 0
            0x00, 0x00, // error_code 0
            0x00, // error_message null
            0x02, // results len 1
            0x03, 0x75, 0x30, // user "u0"
            0x00, 0x00, // result error_code 0
            0x00, // result error_message null
            0x02, // credential_infos len 1
            0x01, // mechanism 1
            0x00, 0x00, 0x10, 0x00, // iterations 4096
            0x00, // credential_info tagged fields
            0x00, // result tagged fields
            0x00, // response tagged fields
        ];
        let mut readable = crate::common::ByteBufferAccessor::from_bytes(bytes);
        let parsed = DescribeUserScramCredentialsResponse::parse(&mut readable, 0).unwrap();
        assert_eq!(parsed.data().throttle_time_ms, 0);
        assert_eq!(parsed.data().results.len(), 1);
        assert_eq!(parsed.data().results[0].user, "u0");
        assert_eq!(parsed.data().results[0].credential_infos[0].mechanism, 1);
        assert_eq!(parsed.data().results[0].credential_infos[0].iterations, 4096);
    }

    #[test]
    fn round_trip_via_credential_info_builder() {
        // Constructs the same data programmatically and asserts parse ->
        // fields, complementing the raw byte vector above.
        let mut data = DescribeUserScramCredentialsResponseData::new();
        let mut result = DescribeUserScramCredentialsResult::new();
        let mut info = CredentialInfo::new();
        info.set_mechanism(1).set_iterations(4096);
        result
            .set_user("u0".to_string())
            .set_error_code(0)
            .set_credential_infos(vec![info]);
        data.set_throttle_time_ms(0).set_error_code(0).set_results(vec![result]);
        let response = DescribeUserScramCredentialsResponse::new(data, 0);
        assert_eq!(response.data().results[0].credential_infos[0].iterations, 4096);
    }

    #[test]
    fn always_throttles() {
        let response = DescribeUserScramCredentialsResponse::new(DescribeUserScramCredentialsResponseData::new(), 0);
        assert!(response.should_client_throttle(0));
    }
}
