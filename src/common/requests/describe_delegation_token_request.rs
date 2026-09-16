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

//! DescribeDelegationToken request handling.
//!
//! Corresponds to
//! `org.apache.kafka.common.requests.DescribeDelegationTokenRequest`.

use std::io;

use crate::DescribeDelegationTokenRequestData;
use crate::common::protocol::{ApiKeys, Errors, Readable};
use crate::common::security::auth::KafkaPrincipal;
use crate::describe_delegation_token_request_data::DescribeDelegationTokenOwner;

use super::{ConcreteRequest, ConcreteResponse, DescribeDelegationTokenResponse, RequestBuilder};

/// A DescribeDelegationToken request.
///
/// Corresponds to
/// `org.apache.kafka.common.requests.DescribeDelegationTokenRequest`.
#[derive(Debug, Clone)]
pub struct DescribeDelegationTokenRequest {
    data: DescribeDelegationTokenRequestData,
    version: i16,
}

impl DescribeDelegationTokenRequest {
    /// Creates a new `DescribeDelegationTokenRequest` from data and version.
    pub fn new(data: DescribeDelegationTokenRequestData, version: i16) -> Self {
        Self { data, version }
    }

    /// Returns a reference to the underlying data.
    pub fn data(&self) -> &DescribeDelegationTokenRequestData {
        &self.data
    }

    /// Returns a mutable reference to the underlying data.
    pub(crate) fn data_mut(&mut self) -> &mut DescribeDelegationTokenRequestData {
        &mut self.data
    }

    /// Returns the API version of this request.
    pub fn version(&self) -> i16 {
        self.version
    }

    /// Returns the API key for this request.
    pub fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::DESCRIBE_DELEGATION_TOKEN
    }

    /// Whether the owners list is present and empty.
    ///
    /// Mirrors `DescribeDelegationTokenRequest.ownersListEmpty`.
    pub fn owners_list_empty(&self) -> bool {
        self.data.owners.as_ref().is_some_and(std::vec::Vec::is_empty)
    }

    /// Creates an error response for this request.
    ///
    /// Mirrors `DescribeDelegationTokenRequest.getErrorResponse`.
    pub fn get_error_response(&self, throttle_time_ms: i32, error: &Errors) -> ConcreteResponse {
        ConcreteResponse::DescribeDelegationToken(DescribeDelegationTokenResponse::with_version_throttle_time_ms_error(
            self.version,
            throttle_time_ms,
            *error,
        ))
    }

    /// Parses a `DescribeDelegationTokenRequest` from a readable buffer.
    ///
    /// # Errors
    ///
    /// Returns an error if parsing fails.
    pub fn parse(readable: &mut dyn Readable, version: i16) -> io::Result<Self> {
        let data = DescribeDelegationTokenRequestData::read(readable, version)?;
        Ok(Self::new(data, version))
    }
}

impl std::fmt::Display for DescribeDelegationTokenRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "DescribeDelegationTokenRequest(version={}, data={:?})",
            self.version, self.data
        )
    }
}

/// Builder for [`DescribeDelegationTokenRequest`].
///
/// Corresponds to `DescribeDelegationTokenRequest.Builder` in Java.
#[derive(Debug, Clone)]
pub struct DescribeDelegationTokenRequestBuilder {
    data: DescribeDelegationTokenRequestData,
    oldest_allowed_version: i16,
    latest_allowed_version: i16,
}

impl DescribeDelegationTokenRequestBuilder {
    /// Creates a builder from an optional owners filter.
    ///
    /// Mirrors `DescribeDelegationTokenRequest.Builder(List<KafkaPrincipal>)`:
    /// a `None` owners filter maps to a null owners field (describe all tokens
    /// the user is authorized for), while a present filter maps each principal
    /// to a `DescribeDelegationTokenOwner`.
    pub fn new(owners: Option<&[KafkaPrincipal]>) -> Self {
        let mut data = DescribeDelegationTokenRequestData::new();
        data.owners = owners.map(|owners| {
            owners
                .iter()
                .map(|owner| {
                    let mut o = DescribeDelegationTokenOwner::new();
                    o.principal_name = owner.name().to_string();
                    o.principal_type = owner.principal_type().to_string();
                    o
                })
                .collect()
        });
        Self {
            data,
            oldest_allowed_version: ApiKeys::DESCRIBE_DELEGATION_TOKEN.oldest_version(),
            latest_allowed_version: ApiKeys::DESCRIBE_DELEGATION_TOKEN.latest_version(),
        }
    }
}

impl RequestBuilder for DescribeDelegationTokenRequestBuilder {
    fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::DESCRIBE_DELEGATION_TOKEN
    }

    fn oldest_allowed_version(&self) -> i16 {
        self.oldest_allowed_version
    }

    fn latest_allowed_version(&self) -> i16 {
        self.latest_allowed_version
    }

    fn build_version(&mut self, version: i16) -> io::Result<ConcreteRequest> {
        Ok(ConcreteRequest::DescribeDelegationToken(DescribeDelegationTokenRequest::new(
            self.data.clone(),
            version,
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_none_leaves_null_owners() {
        let builder = DescribeDelegationTokenRequestBuilder::new(None);
        assert!(builder.data.owners.is_none());
    }

    #[test]
    fn new_maps_principals() {
        let owners = vec![KafkaPrincipal::new(KafkaPrincipal::USER_TYPE, "alice")];
        let builder = DescribeDelegationTokenRequestBuilder::new(Some(&owners));
        let owners = builder.data.owners.as_ref().unwrap();
        assert_eq!(owners.len(), 1);
        assert_eq!(owners[0].principal_name, "alice");
        assert_eq!(owners[0].principal_type, "User");
    }

    #[test]
    fn owners_list_empty_only_when_present_and_empty() {
        let version = ApiKeys::DESCRIBE_DELEGATION_TOKEN.latest_version();

        let mut null_owners = DescribeDelegationTokenRequestData::new();
        null_owners.owners = None;
        assert!(!DescribeDelegationTokenRequest::new(null_owners, version).owners_list_empty());

        let mut empty_owners = DescribeDelegationTokenRequestData::new();
        empty_owners.owners = Some(Vec::new());
        assert!(DescribeDelegationTokenRequest::new(empty_owners, version).owners_list_empty());
    }

    #[test]
    fn serialize_parse_round_trip() {
        let version = ApiKeys::DESCRIBE_DELEGATION_TOKEN.latest_version();
        let owners = vec![KafkaPrincipal::new(KafkaPrincipal::USER_TYPE, "alice")];
        let mut builder = DescribeDelegationTokenRequestBuilder::new(Some(&owners));
        let mut request = builder.build_version(version).unwrap();
        let bytes = request.serialize().unwrap();
        let mut readable = crate::common::ByteBufferAccessor::new(bytes.into_buffer());
        let parsed = DescribeDelegationTokenRequest::parse(&mut readable, version).unwrap();
        let parsed_owners = parsed.data().owners.as_ref().unwrap();
        assert_eq!(parsed_owners.len(), 1);
        assert_eq!(parsed_owners[0].principal_name, "alice");
    }

    /// Byte-level wire vector for v3 (flexible) with one owner.
    #[test]
    fn known_wire_vector_v3() {
        let owners = vec![KafkaPrincipal::new(KafkaPrincipal::USER_TYPE, "alice")];
        let mut builder = DescribeDelegationTokenRequestBuilder::new(Some(&owners));
        let mut request = builder.build_version(3).unwrap();
        let bytes = request.serialize().unwrap().into_buffer();
        let expected: Vec<u8> = vec![
            0x02, // owners: compact nullable array length (1 + 1)
            0x05, b'U', b's', b'e', b'r', // principal_type = "User"
            0x06, b'a', b'l', b'i', b'c', b'e', // principal_name = "alice"
            0x00, // owner element tagged fields
            0x00, // request tagged fields
        ];
        assert_eq!(bytes.as_slice(), expected.as_slice());
    }

    /// Byte-level wire vector for v3 (flexible) with a null owners filter,
    /// which must encode as the compact-null array marker (0x00).
    #[test]
    fn known_wire_vector_v3_null_owners() {
        let mut builder = DescribeDelegationTokenRequestBuilder::new(None);
        let mut request = builder.build_version(3).unwrap();
        let bytes = request.serialize().unwrap().into_buffer();
        let expected: Vec<u8> = vec![
            0x00, // owners = null (compact-null array)
            0x00, // request tagged fields
        ];
        assert_eq!(bytes.as_slice(), expected.as_slice());
    }
}
