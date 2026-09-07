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

//! DescribeDelegationToken response handling.
//!
//! Corresponds to
//! `org.apache.kafka.common.requests.DescribeDelegationTokenResponse`.

use std::collections::HashMap;
use std::io;

use crate::common::protocol::{ApiKeys, Errors, Readable};
use crate::common::security::auth::KafkaPrincipal;
use crate::common::security::token::delegation::{DelegationToken, TokenInformation};
use crate::describe_delegation_token_response_data::{
    DescribeDelegationTokenResponseData, DescribedDelegationToken, DescribedDelegationTokenRenewer,
};

use super::abstract_response::single_error_count;

/// A DescribeDelegationToken response.
///
/// Corresponds to
/// `org.apache.kafka.common.requests.DescribeDelegationTokenResponse`.
#[derive(Debug, Clone)]
pub struct DescribeDelegationTokenResponse {
    data: DescribeDelegationTokenResponseData,
}

/// The parameters of Java's four-argument `DescribeDelegationTokenResponse`
/// constructor (`DescribeDelegationTokenResponse.java:38`).
///
/// Java's three constructors (`:38`, `:69`, `:73`) share no parameter name, so
/// all four of `:38`'s parameters reach its derived name. CLAUDE.md §2 caps that
/// at three and makes this struct the method's *only* parameter, so every Java
/// parameter lives here. This struct has no Java counterpart: it exists solely
/// to satisfy that naming rule (DoD #7).
///
/// It has **no** `Default`: `version`, `throttle_time_ms` and `error` come from
/// the caller in every Java overload. Build it from
/// [`DescribeDelegationTokenResponseOptionsBuilder::new_version_throttle_time_ms_error`], whose `tokens` starts empty
/// exactly as Java's `DescribeDelegationTokenResponse(int, int, Errors)` (`:69`)
/// passes `new ArrayList<>()` on the caller's behalf.
#[derive(Debug, Clone, Copy)]
#[non_exhaustive]
pub struct DescribeDelegationTokenResponseOptions<'a> {
    /// Java's `version`.
    pub version: i16,
    /// Java's `throttleTimeMs`.
    pub throttle_time_ms: i32,
    /// Java's `error`.
    pub error: Errors,
    /// Java's `tokens`. Starts empty, as in `:69`.
    pub tokens: &'a [DelegationToken],
}

impl<'a> DescribeDelegationTokenResponseOptions<'a> {
    /// Java's defaults for every parameter beyond those the name lists.
    ///
    /// Private: per CLAUDE.md §2 the options are built through
    /// [`DescribeDelegationTokenResponseOptionsBuilder`], which is this constructor's only caller.
    fn new_version_throttle_time_ms_error(version: i16, throttle_time_ms: i32, error: Errors) -> Self {
        Self { version, throttle_time_ms, error, tokens: &[] }
    }
}

/// Fluent builder for [`DescribeDelegationTokenResponseOptions`].
///
/// Per CLAUDE.md §2 the constructor's name lists every mandatory parameter,
/// each remaining parameter starts at its Java-derived default and has a
/// fluent setter, and `build` yields the options the method takes. Like
/// [`DescribeDelegationTokenResponseOptions`] it has no Java counterpart and exists solely to satisfy
/// that naming rule (DoD #7).
pub struct DescribeDelegationTokenResponseOptionsBuilder<'a> {
    options: DescribeDelegationTokenResponseOptions<'a>,
}

impl<'a> DescribeDelegationTokenResponseOptionsBuilder<'a> {
    /// Creates the options for an error response with no tokens, as Java's
    /// `DescribeDelegationTokenResponse(int, int, Errors)` (`:69`) does.
    /// Per CLAUDE.md §2, the name lists every mandatory parameter, so a later
    /// Java version that makes one of them optional adds a differently named
    /// constructor rather than changing this one.
    pub fn new_version_throttle_time_ms_error(version: i16, throttle_time_ms: i32, error: Errors) -> Self {
        Self {
            options: DescribeDelegationTokenResponseOptions::new_version_throttle_time_ms_error(
                version,
                throttle_time_ms,
                error,
            ),
        }
    }

    /// Sets [`DescribeDelegationTokenResponseOptions::tokens`].
    pub fn set_tokens(mut self, tokens: &'a [DelegationToken]) -> Self {
        self.options.tokens = tokens;
        self
    }

    /// Returns the built options.
    pub fn build(self) -> DescribeDelegationTokenResponseOptions<'a> {
        self.options
    }
}

impl DescribeDelegationTokenResponse {
    /// Creates a new `DescribeDelegationTokenResponse` from the underlying data.
    ///
    /// Corresponds to Java's
    /// `DescribeDelegationTokenResponse(DescribeDelegationTokenResponseData)`
    /// (`DescribeDelegationTokenResponse.java:73`).
    pub fn new_data(data: DescribeDelegationTokenResponseData) -> Self {
        Self { data }
    }

    /// Builds a response from a list of delegation tokens.
    ///
    /// Corresponds to Java's `DescribeDelegationTokenResponse(int, int, Errors,
    /// List<DelegationToken>)` (`DescribeDelegationTokenResponse.java:38`). The
    /// token requester is only encoded on v3+.
    pub fn new_options(options: DescribeDelegationTokenResponseOptions<'_>) -> Self {
        let DescribeDelegationTokenResponseOptions { version, throttle_time_ms, error, tokens } = options;
        let described: Vec<DescribedDelegationToken> = tokens
            .iter()
            .map(|dt| {
                let info = dt.token_info();
                let mut ddt = DescribedDelegationToken::new();
                ddt.token_id = info.token_id().to_string();
                ddt.principal_type = info.owner().principal_type().to_string();
                ddt.principal_name = info.owner().name().to_string();
                ddt.issue_timestamp = info.issue_timestamp();
                ddt.max_timestamp = info.max_timestamp();
                ddt.expiry_timestamp = info.expiry_timestamp();
                ddt.hmac = dt.hmac().to_vec();
                ddt.renewers = info
                    .renewers()
                    .iter()
                    .map(|r| {
                        let mut renewer = DescribedDelegationTokenRenewer::new();
                        renewer.principal_name = r.name().to_string();
                        renewer.principal_type = r.principal_type().to_string();
                        renewer
                    })
                    .collect();
                if version > 2 {
                    ddt.token_requester_principal_type = info.token_requester().principal_type().to_string();
                    ddt.token_requester_principal_name = info.token_requester().name().to_string();
                }
                ddt
            })
            .collect();

        let mut data = DescribeDelegationTokenResponseData::new();
        data.throttle_time_ms = throttle_time_ms;
        data.error_code = error.code();
        data.tokens = described;
        Self::new_data(data)
    }

    /// Builds an error response with no tokens.
    ///
    /// Corresponds to Java's `DescribeDelegationTokenResponse(int, int, Errors)`
    /// (`DescribeDelegationTokenResponse.java:69`).
    pub fn new_version_throttle_time_ms_error(version: i16, throttle_time_ms: i32, error: Errors) -> Self {
        Self::new_options(
            DescribeDelegationTokenResponseOptionsBuilder::new_version_throttle_time_ms_error(
                version,
                throttle_time_ms,
                error,
            )
            .build(),
        )
    }

    /// Returns the API key for this response.
    pub fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::DESCRIBE_DELEGATION_TOKEN
    }

    /// Returns a reference to the underlying data.
    pub fn data(&self) -> &DescribeDelegationTokenResponseData {
        &self.data
    }

    /// Returns a mutable reference to the underlying data.
    pub(crate) fn data_mut(&mut self) -> &mut DescribeDelegationTokenResponseData {
        &mut self.data
    }

    /// Returns the top-level error.
    ///
    /// Mirrors `DescribeDelegationTokenResponse.error`.
    pub fn error(&self) -> Errors {
        Errors::for_code(self.data.error_code)
    }

    /// Whether this response carries an error.
    ///
    /// Mirrors `DescribeDelegationTokenResponse.hasError`.
    pub fn has_error(&self) -> bool {
        self.error() != Errors::None
    }

    /// Reconstructs the delegation tokens described in this response.
    ///
    /// Mirrors `DescribeDelegationTokenResponse.tokens`.
    pub fn tokens(&self) -> Vec<DelegationToken> {
        self.data
            .tokens
            .iter()
            .map(|ddt| {
                let renewers = ddt
                    .renewers
                    .iter()
                    .map(|r| KafkaPrincipal::new(r.principal_type.clone(), r.principal_name.clone()))
                    .collect();
                let info = TokenInformation::new_token_requester(
                    ddt.token_id.clone(),
                    KafkaPrincipal::new(ddt.principal_type.clone(), ddt.principal_name.clone()),
                    KafkaPrincipal::new(
                        ddt.token_requester_principal_type.clone(),
                        ddt.token_requester_principal_name.clone(),
                    ),
                    renewers,
                    ddt.issue_timestamp,
                    ddt.max_timestamp,
                    ddt.expiry_timestamp,
                );
                DelegationToken::new(info, ddt.hmac.clone())
            })
            .collect()
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

    /// Parses a `DescribeDelegationTokenResponse` from a readable buffer.
    ///
    /// # Errors
    ///
    /// Returns an error if parsing fails.
    pub fn parse(readable: &mut dyn Readable, version: i16) -> io::Result<Self> {
        let data = DescribeDelegationTokenResponseData::read(readable, version)?;
        Ok(Self::new_data(data))
    }

    /// Whether the client should throttle on this response (v1+).
    pub fn should_client_throttle(&self, version: i16) -> bool {
        version >= 1
    }
}

impl std::fmt::Display for DescribeDelegationTokenResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Mirrors Java's `toString`, which redacts each token id and hmac.
        let mut redacted = self.data.clone();
        for token in &mut redacted.tokens {
            token.token_id = "REDACTED".to_string();
            token.hmac = Vec::new();
        }
        write!(f, "DescribeDelegationTokenResponse(data={redacted:?})")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn token(id: &str) -> DelegationToken {
        let info = TokenInformation::new_token_requester(
            id,
            KafkaPrincipal::new(KafkaPrincipal::USER_TYPE, "alice"),
            KafkaPrincipal::new(KafkaPrincipal::USER_TYPE, "requester"),
            vec![KafkaPrincipal::new(KafkaPrincipal::USER_TYPE, "bob")],
            1,
            100,
            50,
        );
        DelegationToken::new(info, format!("hmac-{id}").into_bytes())
    }

    #[test]
    fn tokens_round_trip_through_response_v3() {
        let version = 3;
        let tokens = vec![token("id-1"), token("id-2")];
        let response = DescribeDelegationTokenResponse::new_options(
            DescribeDelegationTokenResponseOptionsBuilder::new_version_throttle_time_ms_error(version, 0, Errors::None)
                .set_tokens(&tokens)
                .build(),
        );
        assert!(!response.has_error());
        let reconstructed = response.tokens();
        assert_eq!(reconstructed, tokens);
        assert_eq!(reconstructed[0].token_info().token_requester().name(), "requester");
    }

    #[test]
    fn requester_not_encoded_below_v3() {
        let tokens = vec![token("id-1")];
        let response = DescribeDelegationTokenResponse::new_options(
            DescribeDelegationTokenResponseOptionsBuilder::new_version_throttle_time_ms_error(2, 0, Errors::None)
                .set_tokens(&tokens)
                .build(),
        );
        // On v2 the requester principal is not encoded; it decodes to empty.
        assert_eq!(response.data().tokens[0].token_requester_principal_name, "");
    }

    #[test]
    fn error_only_has_no_tokens() {
        let response = DescribeDelegationTokenResponse::new_version_throttle_time_ms_error(
            3,
            25,
            Errors::DelegationTokenAuthDisabled,
        );
        assert!(response.has_error());
        assert_eq!(response.error(), Errors::DelegationTokenAuthDisabled);
        assert_eq!(response.throttle_time_ms(), 25);
        assert!(response.tokens().is_empty());
    }

    #[test]
    fn display_redacts_token_id_and_hmac() {
        let response = DescribeDelegationTokenResponse::new_options(
            DescribeDelegationTokenResponseOptionsBuilder::new_version_throttle_time_ms_error(3, 0, Errors::None)
                .set_tokens(&[token("secret-id")])
                .build(),
        );
        let rendered = response.to_string();
        assert!(rendered.contains("REDACTED"), "{rendered}");
        assert!(!rendered.contains("secret-id"), "{rendered}");
    }

    /// Byte-level wire vector for v3 (flexible), error-only (no tokens). The
    /// empty tokens array must encode as a single compact-array length byte.
    #[test]
    fn known_wire_vector_v3_error_only() {
        use crate::common::requests::ConcreteResponse;
        let response =
            DescribeDelegationTokenResponse::new_version_throttle_time_ms_error(3, 9, Errors::DelegationTokenNotFound);
        let mut response = ConcreteResponse::DescribeDelegationToken(response);
        let bytes = response.serialize(3).unwrap().into_buffer();
        let expected: Vec<u8> = vec![
            0x00, 0x3E, // error_code = 62 (int16 BE)
            0x01, // tokens: compact array length (0 + 1) -> empty
            0x00, 0x00, 0x00, 0x09, // throttle_time_ms = 9 (int32 BE)
            0x00, // response tagged fields
        ];
        assert_eq!(bytes.as_slice(), expected.as_slice());
    }
}
