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

/// The parameters of Java's ten-argument
/// `CreateDelegationTokenResponse.prepareResponse(...)`
/// (`CreateDelegationTokenResponse.java:42`).
///
/// The two `prepareResponse` forms (`:42`, `:69`) intersect on
/// `{version, throttleTimeMs, error, owner, tokenRequester}`, leaving five
/// parameters to reach `:42`'s derived name. CLAUDE.md §2 caps that at three
/// parameters and makes this struct the method's *only* parameter, so every
/// Java parameter lives here — the intersection members included. This struct
/// has no Java counterpart: it exists solely to satisfy that naming rule
/// (DoD #7).
///
/// It deliberately has **no** `Default`. Java's narrower `:69` overload does
/// supply the five token fields (`-1, -1, -1, "", ByteBuffer.wrap(new byte[0])`)
/// but takes `version`, `throttleTimeMs`, `error`, `owner` and `requester` from
/// its caller, so those five have no Java-derived default — a synthesised empty
/// `KafkaPrincipal` would silently attribute the token to nobody. Build it from
/// [`CreateDelegationTokenResponseOptionsBuilder::new`], setting those five and
/// overriding the token fields you need: [`CreateDelegationTokenResponseOptionsBuilder::build`] panics if any of `version`, `throttle_time_ms`, `error`, `owner`, `token_requester` was not set.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct CreateDelegationTokenResponseOptions<'a> {
    /// Java's `version`.
    pub version: i16,
    /// Java's `throttleTimeMs`.
    pub throttle_time_ms: i32,
    /// Java's `error`.
    pub error: Errors,
    /// Java's `owner`.
    pub owner: &'a KafkaPrincipal,
    /// Java's `tokenRequester`.
    pub token_requester: &'a KafkaPrincipal,
    /// Java's `issueTimestamp`. Starts as `-1`, as in `:69`.
    pub issue_timestamp: i64,
    /// Java's `expiryTimestamp`. Starts as `-1`, as in `:69`.
    pub expiry_timestamp: i64,
    /// Java's `maxTimestamp`. Starts as `-1`, as in `:69`.
    pub max_timestamp: i64,
    /// Java's `tokenId`. Starts empty, as in `:69`.
    pub token_id: &'a str,
    /// Java's `hmac`. Starts empty, as in `:69`.
    pub hmac: Vec<u8>,
}

/// Fluent builder for [`CreateDelegationTokenResponseOptions`].
///
/// Per CLAUDE.md §2 [`Self::new`] takes no parameters, every parameter has a
/// fluent setter, and [`Self::build`] validates the mandatory ones — panicking
/// if they were not set. Like [`CreateDelegationTokenResponseOptions`] it has no Java counterpart and
/// exists solely to satisfy that naming rule (DoD #7).
pub struct CreateDelegationTokenResponseOptionsBuilder<'a> {
    version: Option<i16>,
    throttle_time_ms: Option<i32>,
    error: Option<Errors>,
    owner: Option<&'a KafkaPrincipal>,
    token_requester: Option<&'a KafkaPrincipal>,
    issue_timestamp: i64,
    expiry_timestamp: i64,
    max_timestamp: i64,
    token_id: &'a str,
    hmac: Vec<u8>,
}

impl<'a> Default for CreateDelegationTokenResponseOptionsBuilder<'a> {
    fn default() -> Self {
        Self::new()
    }
}

impl<'a> CreateDelegationTokenResponseOptionsBuilder<'a> {
    /// Creates a builder with every mandatory parameter unset and every other
    /// parameter at the value Java passes on the caller's behalf.
    pub fn new() -> Self {
        Self {
            version: None,
            throttle_time_ms: None,
            error: None,
            owner: None,
            token_requester: None,
            issue_timestamp: -1,
            expiry_timestamp: -1,
            max_timestamp: -1,
            token_id: "",
            hmac: Vec::new(),
        }
    }

    /// Sets [`CreateDelegationTokenResponseOptions::version`], a mandatory parameter: [`Self::build`]
    /// panics if it was not set.
    pub fn set_version(mut self, version: i16) -> Self {
        self.version = Some(version);
        self
    }
    /// Sets [`CreateDelegationTokenResponseOptions::throttle_time_ms`], a mandatory parameter: [`Self::build`]
    /// panics if it was not set.
    pub fn set_throttle_time_ms(mut self, throttle_time_ms: i32) -> Self {
        self.throttle_time_ms = Some(throttle_time_ms);
        self
    }
    /// Sets [`CreateDelegationTokenResponseOptions::error`], a mandatory parameter: [`Self::build`]
    /// panics if it was not set.
    pub fn set_error(mut self, error: Errors) -> Self {
        self.error = Some(error);
        self
    }
    /// Sets [`CreateDelegationTokenResponseOptions::owner`], a mandatory parameter: [`Self::build`]
    /// panics if it was not set.
    pub fn set_owner(mut self, owner: &'a KafkaPrincipal) -> Self {
        self.owner = Some(owner);
        self
    }
    /// Sets [`CreateDelegationTokenResponseOptions::token_requester`], a mandatory parameter: [`Self::build`]
    /// panics if it was not set.
    pub fn set_token_requester(mut self, token_requester: &'a KafkaPrincipal) -> Self {
        self.token_requester = Some(token_requester);
        self
    }
    /// Sets [`CreateDelegationTokenResponseOptions::issue_timestamp`].
    pub fn set_issue_timestamp(mut self, issue_timestamp: i64) -> Self {
        self.issue_timestamp = issue_timestamp;
        self
    }
    /// Sets [`CreateDelegationTokenResponseOptions::expiry_timestamp`].
    pub fn set_expiry_timestamp(mut self, expiry_timestamp: i64) -> Self {
        self.expiry_timestamp = expiry_timestamp;
        self
    }
    /// Sets [`CreateDelegationTokenResponseOptions::max_timestamp`].
    pub fn set_max_timestamp(mut self, max_timestamp: i64) -> Self {
        self.max_timestamp = max_timestamp;
        self
    }
    /// Sets [`CreateDelegationTokenResponseOptions::token_id`].
    pub fn set_token_id(mut self, token_id: &'a str) -> Self {
        self.token_id = token_id;
        self
    }
    /// Sets [`CreateDelegationTokenResponseOptions::hmac`].
    pub fn set_hmac(mut self, hmac: Vec<u8>) -> Self {
        self.hmac = hmac;
        self
    }

    /// Returns the built options.
    ///
    /// Per CLAUDE.md §2 the mandatory parameters are validated here rather than
    /// being named in the constructor, so a later Java version that makes one of
    /// them optional changes the set this accepts instead of adding a second
    /// constructor. Today there is one mandatory set: `version`, `throttle_time_ms`, `error`, `owner`, `token_requester`.
    ///
    /// # Panics
    ///
    /// Panics if any parameter of that set was not given a setter call.
    pub fn build(self) -> CreateDelegationTokenResponseOptions<'a> {
        CreateDelegationTokenResponseOptions {
            version: self.version.unwrap_or_else(|| Self::missing("version")),
            throttle_time_ms: self.throttle_time_ms.unwrap_or_else(|| Self::missing("throttle_time_ms")),
            error: self.error.unwrap_or_else(|| Self::missing("error")),
            owner: self.owner.unwrap_or_else(|| Self::missing("owner")),
            token_requester: self.token_requester.unwrap_or_else(|| Self::missing("token_requester")),
            issue_timestamp: self.issue_timestamp,
            expiry_timestamp: self.expiry_timestamp,
            max_timestamp: self.max_timestamp,
            token_id: self.token_id,
            hmac: self.hmac,
        }
    }

    /// Panics naming a mandatory parameter [`Self::build`] found unset.
    fn missing(parameter: &str) -> ! {
        panic!("CreateDelegationTokenResponseOptionsBuilder::build: mandatory parameter `{parameter}` was not set");
    }
}

impl CreateDelegationTokenResponse {
    /// Creates a new `CreateDelegationTokenResponse` from the underlying data.
    pub fn new(data: CreateDelegationTokenResponseData) -> Self {
        Self { data }
    }

    /// Prepares a full response.
    ///
    /// Corresponds to Java's ten-argument
    /// `CreateDelegationTokenResponse.prepareResponse(...)`
    /// (`CreateDelegationTokenResponse.java:42`).
    pub fn prepare_response_options(options: CreateDelegationTokenResponseOptions<'_>) -> Self {
        let CreateDelegationTokenResponseOptions {
            version,
            throttle_time_ms,
            error,
            owner,
            token_requester,
            issue_timestamp,
            expiry_timestamp,
            max_timestamp,
            token_id,
            hmac,
        } = options;
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
    /// Corresponds to Java's five-argument
    /// `CreateDelegationTokenResponse.prepareResponse(int, int, Errors,
    /// KafkaPrincipal, KafkaPrincipal)` (`CreateDelegationTokenResponse.java:69`).
    pub fn prepare_response(
        version: i16,
        throttle_time_ms: i32,
        error: Errors,
        owner: &KafkaPrincipal,
        token_requester: &KafkaPrincipal,
    ) -> Self {
        Self::prepare_response_options(
            CreateDelegationTokenResponseOptionsBuilder::new()
                .set_version(version)
                .set_throttle_time_ms(throttle_time_ms)
                .set_error(error)
                .set_owner(owner)
                .set_token_requester(token_requester)
                .build(),
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

        let v2 = CreateDelegationTokenResponse::prepare_response_options(
            CreateDelegationTokenResponseOptionsBuilder::new()
                .set_version(2)
                .set_throttle_time_ms(0)
                .set_error(Errors::None)
                .set_owner(&owner)
                .set_token_requester(&requester)
                .set_issue_timestamp(1)
                .set_expiry_timestamp(2)
                .set_max_timestamp(3)
                .set_token_id("id")
                .set_hmac(b"hmac".to_vec())
                .build(),
        );
        assert!(v2.data().token_requester_principal_name.is_empty());

        let v3 = CreateDelegationTokenResponse::prepare_response_options(
            CreateDelegationTokenResponseOptionsBuilder::new()
                .set_version(3)
                .set_throttle_time_ms(0)
                .set_error(Errors::None)
                .set_owner(&owner)
                .set_token_requester(&requester)
                .set_issue_timestamp(1)
                .set_expiry_timestamp(2)
                .set_max_timestamp(3)
                .set_token_id("id")
                .set_hmac(b"hmac".to_vec())
                .build(),
        );
        assert_eq!(v3.data().token_requester_principal_name, "requester");
        assert_eq!(v3.data().token_requester_principal_type, "User");
    }

    #[test]
    fn has_error_reflects_error_code() {
        let owner = KafkaPrincipal::anonymous();
        let ok = CreateDelegationTokenResponse::prepare_response(3, 0, Errors::None, &owner, &owner);
        assert!(!ok.has_error());
        let bad =
            CreateDelegationTokenResponse::prepare_response(3, 0, Errors::DelegationTokenAuthDisabled, &owner, &owner);
        assert!(bad.has_error());
        assert_eq!(bad.error(), Errors::DelegationTokenAuthDisabled);
    }

    #[test]
    fn display_redacts_token_id_and_hmac() {
        let owner = KafkaPrincipal::new(KafkaPrincipal::USER_TYPE, "alice");
        let response = CreateDelegationTokenResponse::prepare_response_options(
            CreateDelegationTokenResponseOptionsBuilder::new()
                .set_version(3)
                .set_throttle_time_ms(0)
                .set_error(Errors::None)
                .set_owner(&owner)
                .set_token_requester(&owner)
                .set_issue_timestamp(1)
                .set_expiry_timestamp(2)
                .set_max_timestamp(3)
                .set_token_id("secret-id")
                .set_hmac(b"secret-hmac".to_vec())
                .build(),
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
        let response = CreateDelegationTokenResponse::prepare_response_options(
            CreateDelegationTokenResponseOptionsBuilder::new()
                .set_version(3)
                .set_throttle_time_ms(4)
                .set_error(Errors::None)
                .set_owner(&owner)
                .set_token_requester(&owner)
                .set_issue_timestamp(1)
                .set_expiry_timestamp(2)
                .set_max_timestamp(3)
                .set_token_id("tid")
                .set_hmac(vec![0xAB, 0xCD])
                .build(),
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

    /// CLAUDE.md §2: the mandatory parameters are validated in
    /// [`CreateDelegationTokenResponseOptionsBuilder::build`], not named in the constructor, so a
    /// builder left untouched panics naming the first one it finds unset.
    #[test]
    #[should_panic(
        expected = "CreateDelegationTokenResponseOptionsBuilder::build: mandatory parameter `version` was not set"
    )]
    fn create_delegation_token_response_options_builder_build_panics_when_no_mandatory_parameter_is_set() {
        let _ = CreateDelegationTokenResponseOptionsBuilder::new().build();
    }
}
