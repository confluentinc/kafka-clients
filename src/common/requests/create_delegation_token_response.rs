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

//! CreateDelegationToken response handling.
//!
//! Corresponds to
//! `org.apache.kafka.common.requests.CreateDelegationTokenResponse`.

use std::collections::HashMap;
use std::io;

use crate::common::protocol::{ApiKeys, Errors, Readable};
use crate::common::security::auth::KafkaPrincipal;
use crate::create_delegation_token_response_data::CreateDelegationTokenResponseData;

use super::abstract_response::single_error_count;

/// A CreateDelegationToken response.
///
/// Corresponds to
/// `org.apache.kafka.common.requests.CreateDelegationTokenResponse`.
#[derive(Debug, Clone)]
pub struct CreateDelegationTokenResponse {
    data: CreateDelegationTokenResponseData,
}

impl CreateDelegationTokenResponse {
    /// Creates a new `CreateDelegationTokenResponse` from the underlying data.
    pub fn new(data: CreateDelegationTokenResponseData) -> Self {
        Self { data }
    }

    /// Prepares a full response.
    ///
    /// Mirrors `CreateDelegationTokenResponse.prepareResponse(version,
    /// throttleTimeMs, error, owner, tokenRequester, issueTimestamp,
    /// expiryTimestamp, maxTimestamp, tokenId, hmac)`.
    #[allow(clippy::too_many_arguments)]
    pub fn prepare_response(
        version: i16,
        throttle_time_ms: i32,
        error: Errors,
        owner: &KafkaPrincipal,
        token_requester: &KafkaPrincipal,
        issue_timestamp: i64,
        expiry_timestamp: i64,
        max_timestamp: i64,
        token_id: &str,
        hmac: Vec<u8>,
    ) -> Self {
        let mut data = CreateDelegationTokenResponseData::new();
        data.throttle_time_ms = throttle_time_ms;
        data.error_code = error.code();
        data.principal_type = owner.principal_type().to_string();
        data.principal_name = owner.name().to_string();
        data.issue_timestamp_ms = issue_timestamp;
        data.expiry_timestamp_ms = expiry_timestamp;
        data.max_timestamp_ms = max_timestamp;
        data.token_id = token_id.to_string();
        data.hmac = hmac;
        if version > 2 {
            data.token_requester_principal_type = token_requester.principal_type().to_string();
            data.token_requester_principal_name = token_requester.name().to_string();
        }
        Self::new(data)
    }

    /// Prepares an error response with default (empty) token fields.
    ///
    /// Mirrors `CreateDelegationTokenResponse.prepareResponse(version,
    /// throttleTimeMs, error, owner, requester)`.
    pub fn prepare_error_response(
        version: i16,
        throttle_time_ms: i32,
        error: Errors,
        owner: &KafkaPrincipal,
        token_requester: &KafkaPrincipal,
    ) -> Self {
        Self::prepare_response(
            version,
            throttle_time_ms,
            error,
            owner,
            token_requester,
            -1,
            -1,
            -1,
            "",
            Vec::new(),
        )
    }

    /// Returns the API key for this response.
    pub fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::CREATE_DELEGATION_TOKEN
    }

    /// Returns a reference to the underlying data.
    pub fn data(&self) -> &CreateDelegationTokenResponseData {
        &self.data
    }

    /// Returns a mutable reference to the underlying data.
    pub(crate) fn data_mut(&mut self) -> &mut CreateDelegationTokenResponseData {
        &mut self.data
    }

    /// Returns the top-level error.
    ///
    /// Mirrors `CreateDelegationTokenResponse.error`.
    pub fn error(&self) -> Errors {
        Errors::for_code(self.data.error_code)
    }

    /// Whether this response carries an error.
    ///
    /// Mirrors `CreateDelegationTokenResponse.hasError`.
    pub fn has_error(&self) -> bool {
        self.error() != Errors::None
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
        single_error_count(self.error())
    }

    /// Parses a `CreateDelegationTokenResponse` from a readable buffer.
    ///
    /// # Errors
    ///
    /// Returns an error if parsing fails.
    pub fn parse(readable: &mut dyn Readable, version: i16) -> io::Result<Self> {
        let data = CreateDelegationTokenResponseData::read(readable, version)?;
        Ok(Self::new(data))
    }

    /// Whether the client should throttle on this response (v1+).
    pub fn should_client_throttle(&self, version: i16) -> bool {
        version >= 1
    }
}

impl std::fmt::Display for CreateDelegationTokenResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Mirrors Java's `toString`, which redacts the token id and hmac.
        let mut redacted = self.data.clone();
        redacted.token_id = "REDACTED".to_string();
        redacted.hmac = Vec::new();
        write!(f, "CreateDelegationTokenResponse(data={redacted:?})")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prepare_response_sets_requester_only_above_v2() {
        let owner = KafkaPrincipal::new(KafkaPrincipal::USER_TYPE, "alice");
        let requester = KafkaPrincipal::new(KafkaPrincipal::USER_TYPE, "requester");

        let v2 = CreateDelegationTokenResponse::prepare_response(
            2,
            0,
            Errors::None,
            &owner,
            &requester,
            1,
            2,
            3,
            "id",
            b"hmac".to_vec(),
        );
        assert!(v2.data().token_requester_principal_name.is_empty());

        let v3 = CreateDelegationTokenResponse::prepare_response(
            3,
            0,
            Errors::None,
            &owner,
            &requester,
            1,
            2,
            3,
            "id",
            b"hmac".to_vec(),
        );
        assert_eq!(v3.data().token_requester_principal_name, "requester");
        assert_eq!(v3.data().token_requester_principal_type, "User");
    }

    #[test]
    fn has_error_reflects_error_code() {
        let owner = KafkaPrincipal::anonymous();
        let ok = CreateDelegationTokenResponse::prepare_error_response(3, 0, Errors::None, &owner, &owner);
        assert!(!ok.has_error());
        let bad = CreateDelegationTokenResponse::prepare_error_response(
            3,
            0,
            Errors::DelegationTokenAuthDisabled,
            &owner,
            &owner,
        );
        assert!(bad.has_error());
        assert_eq!(bad.error(), Errors::DelegationTokenAuthDisabled);
    }

    #[test]
    fn display_redacts_token_id_and_hmac() {
        let owner = KafkaPrincipal::new(KafkaPrincipal::USER_TYPE, "alice");
        let response = CreateDelegationTokenResponse::prepare_response(
            3,
            0,
            Errors::None,
            &owner,
            &owner,
            1,
            2,
            3,
            "secret-id",
            b"secret-hmac".to_vec(),
        );
        let rendered = response.to_string();
        assert!(rendered.contains("REDACTED"), "{rendered}");
        assert!(!rendered.contains("secret-id"), "{rendered}");
    }

    /// Byte-level wire vector for v3 (flexible). Asserts the principal strings,
    /// token id, and raw HMAC bytes are framed correctly on the wire.
    #[test]
    fn known_wire_vector_v3() {
        use crate::common::requests::ConcreteResponse;
        let owner = KafkaPrincipal::new(KafkaPrincipal::USER_TYPE, "alice");
        let response = CreateDelegationTokenResponse::prepare_response(
            3,
            4,
            Errors::None,
            &owner,
            &owner,
            1,
            2,
            3,
            "tid",
            vec![0xAB, 0xCD],
        );
        let mut response = ConcreteResponse::CreateDelegationToken(response);
        let bytes = response.serialize(3).unwrap().into_buffer();
        let expected: Vec<u8> = vec![
            0x00, 0x00, // error_code = 0 (int16 BE)
            0x05, b'U', b's', b'e', b'r', // principal_type = "User"
            0x06, b'a', b'l', b'i', b'c', b'e', // principal_name = "alice"
            0x05, b'U', b's', b'e', b'r', // token_requester_principal_type = "User" (v3)
            0x06, b'a', b'l', b'i', b'c', b'e', // token_requester_principal_name = "alice" (v3)
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, // issue_timestamp_ms = 1
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, // expiry_timestamp_ms = 2
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03, // max_timestamp_ms = 3
            0x04, b't', b'i', b'd', // token_id = "tid"
            0x03, 0xAB, 0xCD, // hmac: compact bytes len (2 + 1) then raw bytes
            0x00, 0x00, 0x00, 0x04, // throttle_time_ms = 4 (int32 BE)
            0x00, // response tagged fields
        ];
        assert_eq!(bytes.as_slice(), expected.as_slice());
    }
}
