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

//! CreateDelegationToken request handling.
//!
//! Corresponds to
//! `org.apache.kafka.common.requests.CreateDelegationTokenRequest`.

use std::io;

use crate::common::protocol::{ApiKeys, Errors, Readable};
use crate::common::security::auth::KafkaPrincipal;
use crate::create_delegation_token_request_data::CreateDelegationTokenRequestData;

use super::{ConcreteRequest, ConcreteResponse, CreateDelegationTokenResponse, RequestBuilder};

/// A CreateDelegationToken request.
///
/// Corresponds to
/// `org.apache.kafka.common.requests.CreateDelegationTokenRequest`.
#[derive(Debug, Clone)]
pub struct CreateDelegationTokenRequest {
    data: CreateDelegationTokenRequestData,
    version: i16,
}

impl CreateDelegationTokenRequest {
    /// Creates a new `CreateDelegationTokenRequest` from data and version.
    pub fn new(data: CreateDelegationTokenRequestData, version: i16) -> Self {
        Self { data, version }
    }

    /// Returns a reference to the underlying data.
    pub fn data(&self) -> &CreateDelegationTokenRequestData {
        &self.data
    }

    /// Returns a mutable reference to the underlying data.
    pub(crate) fn data_mut(&mut self) -> &mut CreateDelegationTokenRequestData {
        &mut self.data
    }

    /// Returns the API version of this request.
    pub fn version(&self) -> i16 {
        self.version
    }

    /// Returns the API key for this request.
    pub fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::CREATE_DELEGATION_TOKEN
    }

    /// Creates an error response for this request.
    ///
    /// Mirrors `CreateDelegationTokenRequest.getErrorResponse`, which prepares a
    /// response with the `ANONYMOUS` owner and requester principals.
    pub fn get_error_response(&self, throttle_time_ms: i32, error: &Errors) -> ConcreteResponse {
        ConcreteResponse::CreateDelegationToken(CreateDelegationTokenResponse::prepare_response(
            self.version,
            throttle_time_ms,
            *error,
            &KafkaPrincipal::anonymous(),
            &KafkaPrincipal::anonymous(),
        ))
    }

    /// Parses a `CreateDelegationTokenRequest` from a readable buffer.
    ///
    /// # Errors
    ///
    /// Returns an error if parsing fails.
    pub fn parse(readable: &mut dyn Readable, version: i16) -> io::Result<Self> {
        let data = CreateDelegationTokenRequestData::read(readable, version)?;
        Ok(Self::new(data, version))
    }
}

impl std::fmt::Display for CreateDelegationTokenRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "CreateDelegationTokenRequest(version={}, data={:?})",
            self.version, self.data
        )
    }
}

/// Builder for [`CreateDelegationTokenRequest`].
///
/// Corresponds to `CreateDelegationTokenRequest.Builder` in Java.
#[derive(Debug, Clone)]
pub struct CreateDelegationTokenRequestBuilder {
    data: CreateDelegationTokenRequestData,
    oldest_allowed_version: i16,
    latest_allowed_version: i16,
}

impl CreateDelegationTokenRequestBuilder {
    /// Creates a builder from existing data.
    pub fn new(data: CreateDelegationTokenRequestData) -> Self {
        Self {
            data,
            oldest_allowed_version: ApiKeys::CREATE_DELEGATION_TOKEN.oldest_version(),
            latest_allowed_version: ApiKeys::CREATE_DELEGATION_TOKEN.latest_version(),
        }
    }
}

impl RequestBuilder for CreateDelegationTokenRequestBuilder {
    fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::CREATE_DELEGATION_TOKEN
    }

    fn oldest_allowed_version(&self) -> i16 {
        self.oldest_allowed_version
    }

    fn latest_allowed_version(&self) -> i16 {
        self.latest_allowed_version
    }

    fn build_version(&mut self, version: i16) -> io::Result<ConcreteRequest> {
        Ok(ConcreteRequest::CreateDelegationToken(CreateDelegationTokenRequest::new(
            self.data.clone(),
            version,
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::create_delegation_token_request_data::CreatableRenewers;

    fn request_data() -> CreateDelegationTokenRequestData {
        let mut data = CreateDelegationTokenRequestData::new();
        data.max_lifetime_ms = 86_400_000;
        let mut renewer = CreatableRenewers::new();
        renewer.principal_type = "User".to_string();
        renewer.principal_name = "bob".to_string();
        data.renewers = vec![renewer];
        data
    }

    #[test]
    fn get_error_response_uses_anonymous_principals() {
        let request =
            CreateDelegationTokenRequest::new(request_data(), ApiKeys::CREATE_DELEGATION_TOKEN.latest_version());
        let response = request.get_error_response(100, &Errors::DelegationTokenAuthDisabled);
        let ConcreteResponse::CreateDelegationToken(r) = response else {
            panic!("expected CreateDelegationToken response");
        };
        assert_eq!(r.data().error_code, Errors::DelegationTokenAuthDisabled.code());
        assert_eq!(r.data().principal_name, "ANONYMOUS");
        assert_eq!(r.data().principal_type, "User");
        assert_eq!(r.throttle_time_ms(), 100);
    }

    #[test]
    fn serialize_parse_round_trip() {
        let version = ApiKeys::CREATE_DELEGATION_TOKEN.latest_version();
        let mut request =
            ConcreteRequest::CreateDelegationToken(CreateDelegationTokenRequest::new(request_data(), version));
        let bytes = request.serialize().unwrap();
        let mut readable = crate::common::ByteBufferAccessor::from_bytes(bytes.into_buffer());
        let parsed = CreateDelegationTokenRequest::parse(&mut readable, version).unwrap();
        assert_eq!(parsed.data().max_lifetime_ms, 86_400_000);
        assert_eq!(parsed.data().renewers.len(), 1);
        assert_eq!(parsed.data().renewers[0].principal_name, "bob");
    }

    /// Byte-level wire vector for v3 (flexible). Exercises the null owner
    /// principal fields, one renewer, and the principal-type/name strings on
    /// the wire.
    #[test]
    fn known_wire_vector_v3() {
        let mut data = CreateDelegationTokenRequestData::new();
        // owner_principal_type / owner_principal_name default to null.
        data.max_lifetime_ms = 86_400_000;
        let mut renewer = CreatableRenewers::new();
        renewer.principal_type = "User".to_string();
        renewer.principal_name = "bob".to_string();
        data.renewers = vec![renewer];
        let mut request = ConcreteRequest::CreateDelegationToken(CreateDelegationTokenRequest::new(data, 3));
        let bytes = request.serialize().unwrap().into_buffer();
        let expected: Vec<u8> = vec![
            // owner_principal_type / owner_principal_name are nullable but have no
            // explicit `default: null` in the spec, so they default to an empty
            // string (per the message-generator rule), encoding as compact string
            // length 0 + 1 = 0x01 with no bytes — NOT the compact-null marker 0x00.
            0x01, // owner_principal_type = "" (empty compact string)
            0x01, // owner_principal_name = "" (empty compact string)
            0x02, // renewers: compact array length (1 + 1)
            0x05, b'U', b's', b'e', b'r', // principal_type = "User" (compact string len 4+1)
            0x04, b'b', b'o', b'b', // principal_name = "bob" (compact string len 3+1)
            0x00, // renewer element tagged fields
            0x00, 0x00, 0x00, 0x00, 0x05, 0x26, 0x5C, 0x00, // max_lifetime_ms = 86_400_000 (int64 BE)
            0x00, // request tagged fields
        ];
        assert_eq!(bytes.as_slice(), expected.as_slice());
    }
}
