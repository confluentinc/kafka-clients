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

use crate::common::Error;
use std::collections::HashMap;
use std::io;

use crate::DescribeDelegationTokenResponseData;
use crate::common::protocol::{ApiKeys, Errors, Readable};
use crate::common::security::auth::KafkaPrincipal;
use crate::common::security::token::delegation::{DelegationToken, TokenInformation};
use crate::describe_delegation_token_response_data::{DescribedDelegationToken, DescribedDelegationTokenRenewer};

use super::AbstractResponse;

/// A DescribeDelegationToken response.
///
/// Corresponds to
/// `org.apache.kafka.common.requests.DescribeDelegationTokenResponse`.
#[derive(Clone)]
#[doc(alias = "org.apache.kafka.common.requests.DescribeDelegationTokenResponse")]
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
/// the caller in every Java overload, so [`DescribeDelegationTokenResponseOptionsBuilder::build`] returns an error if any of `version`, `throttle_time_ms`, `error` was not set.
/// Build it from [`DescribeDelegationTokenResponseOptionsBuilder::new`], whose `tokens` starts empty
/// exactly as Java's `DescribeDelegationTokenResponse(int, int, Errors)` (`:69`)
/// passes `new ArrayList<>()` on the caller's behalf.
///
/// `Debug` renders no token's id and no token's hmac, as the response built
/// from these options renders neither.
#[derive(Clone, Copy)]
#[non_exhaustive]
pub struct DescribeDelegationTokenResponseOptions<'a> {
    /// Java's `version`.
    pub(crate) version: i16,
    /// Java's `throttleTimeMs`.
    pub(crate) throttle_time_ms: i32,
    /// Java's `error`.
    pub(crate) error: Errors,
    /// Java's `tokens`. Starts empty, as in `:69`.
    pub(crate) tokens: &'a [DelegationToken],
}

/// Renders `version`, `throttle_time_ms` and `error` as the derive it replaces
/// did, and each token field by field, with its id and hmac rendered as Java's
/// `DescribeDelegationTokenResponse.toString()` renders them
/// (`DescribeDelegationTokenResponse.java:131-140`): the token id as
/// `"REDACTED"` and the hmac as empty.
///
/// This struct has no Java counterpart, so it has no Java rendering of its
/// own. It carries the tokens of the response it prepares, which never renders
/// their ids or hmacs. The derive rendered each token through
/// [`DelegationToken`]'s own `Debug`, which follows Java's
/// `DelegationToken.toString()` (`DelegationToken.java:71-76`): that hides the
/// hmac but prints the token id, so the derive printed what the response
/// itself hides.
impl std::fmt::Debug for DescribeDelegationTokenResponseOptions<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Destructured exhaustively, so a field added to the struct must be
        // considered here.
        let Self { version, throttle_time_ms, error, tokens } = self;
        // Each token renders field by field. The fields of `DelegationToken`
        // and `TokenInformation` are private to their module, so they are read
        // through accessors: a field added there is not rendered until it is
        // listed here. The two secrets are not read at all: nothing of them is
        // rendered, not even the length.
        let tokens = std::fmt::from_fn(|f| {
            f.debug_list()
                .entries(tokens.iter().map(|token| {
                    std::fmt::from_fn(move |f| {
                        let info = token.token_info();
                        let token_information = std::fmt::from_fn(|f| {
                            f.debug_struct("TokenInformation")
                                .field("owner", info.owner())
                                .field("token_requester", info.token_requester())
                                .field("renewers", &info.renewers())
                                .field("issue_timestamp", &info.issue_timestamp())
                                .field("max_timestamp", &info.max_timestamp())
                                .field("expiry_timestamp", &info.expiry_timestamp())
                                .field("token_id", &"REDACTED")
                                .finish()
                        });
                        f.debug_struct("DelegationToken")
                            .field("token_information", &token_information)
                            .field("hmac", &[0u8; 0])
                            .finish()
                    })
                }))
                .finish()
        });
        f.debug_struct("DescribeDelegationTokenResponseOptions")
            .field("version", version)
            .field("throttle_time_ms", throttle_time_ms)
            .field("error", error)
            .field("tokens", &tokens)
            .finish()
    }
}

/// Fluent builder for [`DescribeDelegationTokenResponseOptions`].
///
/// Per CLAUDE.md §2 [`Self::new`] takes no parameters, every parameter has a
/// fluent setter, and [`Self::build`] validates the mandatory ones — returning
/// [`Error::LocalIllegalArgument`] if they were not set. Like [`DescribeDelegationTokenResponseOptions`] it has no Java counterpart and
/// exists solely to satisfy that naming rule (DoD #7).
pub struct DescribeDelegationTokenResponseOptionsBuilder<'a> {
    version: Option<i16>,
    throttle_time_ms: Option<i32>,
    error: Option<Errors>,
    tokens: &'a [DelegationToken],
}

impl<'a> Default for DescribeDelegationTokenResponseOptionsBuilder<'a> {
    fn default() -> Self {
        Self::new()
    }
}

impl<'a> DescribeDelegationTokenResponseOptionsBuilder<'a> {
    /// Creates a builder with every mandatory parameter unset and every other
    /// parameter at the value Java passes on the caller's behalf.
    pub fn new() -> Self {
        Self { version: None, throttle_time_ms: None, error: None, tokens: &[] }
    }

    /// Sets [`DescribeDelegationTokenResponseOptions::version`], a mandatory parameter: [`Self::build`]
    /// panics if it was not set.
    pub fn set_version(mut self, version: i16) -> Self {
        self.version = Some(version);
        self
    }
    /// Sets [`DescribeDelegationTokenResponseOptions::throttle_time_ms`], a mandatory parameter: [`Self::build`]
    /// panics if it was not set.
    pub fn set_throttle_time_ms(mut self, throttle_time_ms: i32) -> Self {
        self.throttle_time_ms = Some(throttle_time_ms);
        self
    }
    /// Sets [`DescribeDelegationTokenResponseOptions::error`], a mandatory parameter: [`Self::build`]
    /// panics if it was not set.
    pub fn set_error(mut self, error: Errors) -> Self {
        self.error = Some(error);
        self
    }
    /// Sets [`DescribeDelegationTokenResponseOptions::tokens`].
    pub fn set_tokens(mut self, tokens: &'a [DelegationToken]) -> Self {
        self.tokens = tokens;
        self
    }

    /// Returns the built options.
    ///
    /// Per CLAUDE.md §2 the mandatory parameters are validated here rather than
    /// being named in the constructor, so a later Java version that makes one of
    /// them optional changes the set this accepts instead of adding a second
    /// constructor. Today there is one mandatory set: `version`, `throttle_time_ms`, `error`.
    ///
    /// # Errors
    ///
    /// Returns [`Error::LocalIllegalArgument`] naming the first parameter of that
    /// set which was not given a setter call. Only presence is checked here;
    /// semantic validation belongs to the method the options are passed to
    /// (CLAUDE.md §2).
    pub fn build(self) -> Result<DescribeDelegationTokenResponseOptions<'a>, Error> {
        Ok(DescribeDelegationTokenResponseOptions {
            version: self.version.ok_or_else(|| Self::missing("version"))?,
            throttle_time_ms: self.throttle_time_ms.ok_or_else(|| Self::missing("throttle_time_ms"))?,
            error: self.error.ok_or_else(|| Self::missing("error"))?,
            tokens: self.tokens,
        })
    }

    /// Builds the [`Error::LocalIllegalArgument`] naming a mandatory parameter
    /// [`Self::build`] found unset.
    fn missing(parameter: &str) -> Error {
        Error::local_illegal_argument(format!(
            "DescribeDelegationTokenResponseOptionsBuilder::build: mandatory parameter `{parameter}` was not set"
        ))
    }
}

impl DescribeDelegationTokenResponse {
    /// Creates a new `DescribeDelegationTokenResponse` from the underlying data.
    ///
    /// Corresponds to Java's
    /// `DescribeDelegationTokenResponse(DescribeDelegationTokenResponseData)`
    /// (`DescribeDelegationTokenResponse.java:73`).
    #[doc(alias = "org.apache.kafka.common.requests.DescribeDelegationTokenResponse#DescribeDelegationTokenResponse")]
    pub fn with_data(data: DescribeDelegationTokenResponseData) -> Self {
        Self { data }
    }

    /// Builds a response from a list of delegation tokens.
    ///
    /// Corresponds to Java's `DescribeDelegationTokenResponse(int, int, Errors,
    /// List<DelegationToken>)` (`DescribeDelegationTokenResponse.java:38`). The
    /// token requester is only encoded on v3+.
    #[doc(alias = "org.apache.kafka.common.requests.DescribeDelegationTokenResponse#DescribeDelegationTokenResponse")]
    pub fn with_options(options: DescribeDelegationTokenResponseOptions<'_>) -> Self {
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
        Self::with_data(data)
    }

    /// Builds an error response with no tokens.
    ///
    /// Corresponds to Java's `DescribeDelegationTokenResponse(int, int, Errors)`
    /// (`DescribeDelegationTokenResponse.java:69`).
    #[doc(alias = "org.apache.kafka.common.requests.DescribeDelegationTokenResponse#DescribeDelegationTokenResponse")]
    pub fn with_version_throttle_time_ms_error(version: i16, throttle_time_ms: i32, error: Errors) -> Self {
        Self::with_options(
            DescribeDelegationTokenResponseOptionsBuilder::new()
                .set_version(version)
                .set_throttle_time_ms(throttle_time_ms)
                .set_error(error)
                .build()
                .expect("DescribeDelegationTokenResponseOptionsBuilder::build: every mandatory parameter is set above"),
        )
    }

    /// Returns the API key for this response.
    pub fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::DESCRIBE_DELEGATION_TOKEN
    }

    /// Returns a reference to the underlying data.
    #[doc(alias = "org.apache.kafka.common.requests.DescribeDelegationTokenResponse#data")]
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
    #[doc(alias = "org.apache.kafka.common.requests.DescribeDelegationTokenResponse#error")]
    pub fn error(&self) -> Errors {
        Errors::for_code(self.data.error_code)
    }

    /// Whether this response carries an error.
    ///
    /// Mirrors `DescribeDelegationTokenResponse.hasError`.
    #[doc(alias = "org.apache.kafka.common.requests.DescribeDelegationTokenResponse#hasError")]
    pub fn has_error(&self) -> bool {
        self.error() != Errors::None
    }

    /// Reconstructs the delegation tokens described in this response.
    ///
    /// Mirrors `DescribeDelegationTokenResponse.tokens`.
    #[doc(alias = "org.apache.kafka.common.requests.DescribeDelegationTokenResponse#tokens")]
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
                let info = TokenInformation::with_token_requester(
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
    #[doc(alias = "org.apache.kafka.common.requests.DescribeDelegationTokenResponse#throttleTimeMs")]
    pub fn throttle_time_ms(&self) -> i32 {
        self.data.throttle_time_ms
    }

    /// Sets the throttle time in the response.
    #[doc(alias = "org.apache.kafka.common.requests.DescribeDelegationTokenResponse#maybeSetThrottleTimeMs")]
    pub fn maybe_set_throttle_time_ms(&mut self, throttle_time_ms: i32) {
        self.data.throttle_time_ms = throttle_time_ms;
    }

    /// Returns the error counts for this response.
    #[doc(alias = "org.apache.kafka.common.requests.DescribeDelegationTokenResponse#errorCounts")]
    pub fn error_counts(&self) -> HashMap<Errors, i32> {
        AbstractResponse::single_error_count(self.error())
    }

    /// Parses a `DescribeDelegationTokenResponse` from a readable buffer.
    ///
    /// # Errors
    ///
    /// Returns an error if parsing fails.
    #[doc(alias = "org.apache.kafka.common.requests.DescribeDelegationTokenResponse#parse")]
    pub fn parse(readable: &mut dyn Readable, version: i16) -> io::Result<Self> {
        let data = DescribeDelegationTokenResponseData::read(readable, version)?;
        Ok(Self::with_data(data))
    }

    /// Whether the client should throttle on this response (v1+).
    #[doc(alias = "org.apache.kafka.common.requests.DescribeDelegationTokenResponse#shouldClientThrottle")]
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

/// Renders exactly what the redacting [`Display`](std::fmt::Display) renders.
///
/// Java has a single `toString()`; a derived `Debug` would be a second,
/// unredacted rendering that prints each token id and hmac.
impl std::fmt::Debug for DescribeDelegationTokenResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self, f)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn token(id: &str) -> DelegationToken {
        let info = TokenInformation::with_token_requester(
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
        let response = DescribeDelegationTokenResponse::with_options(
            DescribeDelegationTokenResponseOptionsBuilder::new()
                .set_version(version)
                .set_throttle_time_ms(0)
                .set_error(Errors::None)
                .set_tokens(&tokens)
                .build()
                .unwrap(),
        );
        assert!(!response.has_error());
        let reconstructed = response.tokens();
        assert_eq!(reconstructed, tokens);
        assert_eq!(reconstructed[0].token_info().token_requester().name(), "requester");
    }

    #[test]
    fn requester_not_encoded_below_v3() {
        let tokens = vec![token("id-1")];
        let response = DescribeDelegationTokenResponse::with_options(
            DescribeDelegationTokenResponseOptionsBuilder::new()
                .set_version(2)
                .set_throttle_time_ms(0)
                .set_error(Errors::None)
                .set_tokens(&tokens)
                .build()
                .unwrap(),
        );
        // On v2 the requester principal is not encoded; it decodes to empty.
        assert_eq!(response.data().tokens[0].token_requester_principal_name, "");
    }

    #[test]
    fn error_only_has_no_tokens() {
        let response = DescribeDelegationTokenResponse::with_version_throttle_time_ms_error(
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
        let response = DescribeDelegationTokenResponse::with_options(
            DescribeDelegationTokenResponseOptionsBuilder::new()
                .set_version(3)
                .set_throttle_time_ms(0)
                .set_error(Errors::None)
                .set_tokens(&[token("secret-id")])
                .build()
                .unwrap(),
        );
        let rendered = response.to_string();
        assert!(rendered.contains("REDACTED"), "{rendered}");
        assert!(!rendered.contains("secret-id"), "{rendered}");
    }

    /// `Debug` renders exactly what `Display` does (Java has a single
    /// `toString()`): neither a token id nor an hmac, which a derived `Debug`
    /// would print as its byte values.
    #[test]
    fn debug_redacts_token_id_and_hmac() {
        let response = DescribeDelegationTokenResponse::with_options(
            DescribeDelegationTokenResponseOptionsBuilder::new()
                .set_version(3)
                .set_throttle_time_ms(0)
                .set_error(Errors::None)
                .set_tokens(&[token("secret-id")])
                .build()
                .unwrap(),
        );
        assert_eq!(response.data().tokens[0].hmac, b"hmac-secret-id");
        for rendered in [format!("{response:?}"), format!("{response:#?}")] {
            assert_eq!(rendered, response.to_string());
            assert!(rendered.contains("REDACTED"), "{rendered}");
            assert!(!rendered.contains("secret-id"), "{rendered}");
            assert!(!rendered.contains(&format!("{:?}", &b"hmac-secret-id"[..])), "{rendered}");
        }
    }

    /// New test, no Java original (Java has no such struct): `Debug` renders
    /// each token's id and hmac as Java's `toString()` renders the response's
    /// (`DescribeDelegationTokenResponse.java:131-140`), the other token fields
    /// as they are, and every other field as the derive did.
    #[test]
    fn options_debug_redacts_token_id_and_hmac() {
        const TOKEN_ID: &str = "token-id-5b19e4";
        let tokens = [token(TOKEN_ID)];
        let options = DescribeDelegationTokenResponseOptionsBuilder::new()
            .set_version(3)
            .set_throttle_time_ms(0)
            .set_error(Errors::None)
            .set_tokens(&tokens)
            .build()
            .unwrap();
        let token = &options.tokens[0];
        let info = token.token_info();
        let hmac = token.hmac();
        assert_eq!(info.token_id(), TOKEN_ID);
        assert_eq!(hmac, format!("hmac-{TOKEN_ID}").as_bytes());
        // `DelegationToken`'s own `Debug`, which the derive used for each
        // token, prints the token id: the secret this test guards is really
        // there to leak.
        assert!(format!("{token:?}").contains(TOKEN_ID));

        let rendered = format!("{options:?}");
        assert_eq!(
            rendered,
            format!(
                "DescribeDelegationTokenResponseOptions {{ version: 3, throttle_time_ms: 0, error: None, \
                 tokens: [DelegationToken {{ token_information: TokenInformation {{ owner: {:?}, \
                 token_requester: {:?}, renewers: {:?}, issue_timestamp: 1, max_timestamp: 100, \
                 expiry_timestamp: 50, token_id: \"REDACTED\" }}, hmac: [] }}] }}",
                info.owner(),
                info.token_requester(),
                info.renewers(),
            )
        );
        let pretty = format!("{options:#?}");
        assert!(pretty.contains("token_id: \"REDACTED\","), "{pretty}");
        // The pretty form of a derived byte list spreads over indented lines,
        // so check that the field is the empty list itself.
        assert!(pretty.contains("hmac: [],"), "{pretty}");
        for rendered in [rendered, pretty] {
            // The hmac's text holds the token id, so this also covers the hmac
            // rendered as text.
            assert!(!rendered.contains(TOKEN_ID), "token id leaked: {rendered}");
            assert!(!rendered.contains(&format!("{hmac:?}")), "hmac leaked: {rendered}");
            assert!(!rendered.contains(&token.hmac_as_base64_string()), "hmac leaked: {rendered}");
            // The other direction: the fields that are not secret still render.
            assert!(rendered.contains("\"alice\""), "owner missing: {rendered}");
            assert!(rendered.contains("\"requester\""), "token requester missing: {rendered}");
            assert!(rendered.contains("\"bob\""), "renewer missing: {rendered}");
        }
    }

    /// Byte-level wire vector for v3 (flexible), error-only (no tokens). The
    /// empty tokens array must encode as a single compact-array length byte.
    #[test]
    fn known_wire_vector_v3_error_only() {
        use crate::common::requests::ConcreteResponse;
        let response =
            DescribeDelegationTokenResponse::with_version_throttle_time_ms_error(3, 9, Errors::DelegationTokenNotFound);
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

    /// CLAUDE.md §2: the mandatory parameters are validated in
    /// [`DescribeDelegationTokenResponseOptionsBuilder::build`], not named in the constructor, so a
    /// builder left untouched panics naming the first one it finds unset.
    #[test]
    fn describe_delegation_token_response_options_builder_build_errors_when_no_mandatory_parameter_is_set() {
        let Err(error) = DescribeDelegationTokenResponseOptionsBuilder::new().build() else {
            panic!("build must reject the unset mandatory parameter");
        };
        assert!(matches!(error, Error::LocalIllegalArgument(_)), "{error:?}");
        assert_eq!(
            error.message(),
            "DescribeDelegationTokenResponseOptionsBuilder::build: mandatory parameter `version` was not set"
        );
    }
}
