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

//! AlterUserScramCredentials request handling.
//!
//! Corresponds to
//! `org.apache.kafka.common.requests.AlterUserScramCredentialsRequest`.

use std::collections::BTreeSet;
use std::io;

use crate::AlterUserScramCredentialsRequestData;
use crate::AlterUserScramCredentialsResponseData;
use crate::alter_user_scram_credentials_response_data::AlterUserScramCredentialsResult;
use crate::common::protocol::{ApiKeys, Errors, Readable};

use super::{AlterUserScramCredentialsResponse, ConcreteRequest, ConcreteResponse, RequestBuilder};

/// An AlterUserScramCredentials request.
///
/// Corresponds to
/// `org.apache.kafka.common.requests.AlterUserScramCredentialsRequest`.
#[derive(Clone)]
pub struct AlterUserScramCredentialsRequest {
    data: AlterUserScramCredentialsRequestData,
    version: i16,
}

impl AlterUserScramCredentialsRequest {
    /// Creates a new request from data and version.
    pub fn new(data: AlterUserScramCredentialsRequestData, version: i16) -> Self {
        Self { data, version }
    }

    /// Returns a reference to the underlying data.
    pub fn data(&self) -> &AlterUserScramCredentialsRequestData {
        &self.data
    }

    /// Returns a mutable reference to the underlying data.
    pub(crate) fn data_mut(&mut self) -> &mut AlterUserScramCredentialsRequestData {
        &mut self.data
    }

    /// Returns the API version of this request.
    pub fn version(&self) -> i16 {
        self.version
    }

    /// Returns the API key for this request.
    pub fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::ALTER_USER_SCRAM_CREDENTIALS
    }

    /// Creates an error response for this request.
    ///
    /// Mirrors `AlterUserScramCredentialsRequest.getErrorResponse`: one errored
    /// result per distinct affected user (deletions ∪ upsertions), sorted by
    /// user name.
    pub fn get_error_response(&self, _throttle_time_ms: i32, error: &Errors) -> ConcreteResponse {
        let mut users: BTreeSet<&str> = BTreeSet::new();
        for deletion in &self.data.deletions {
            users.insert(deletion.name.as_str());
        }
        for upsertion in &self.data.upsertions {
            users.insert(upsertion.name.as_str());
        }
        let results = users
            .into_iter()
            .map(|user| {
                let mut result = AlterUserScramCredentialsResult::new();
                result
                    .set_user(user.to_string())
                    .set_error_code(error.code())
                    .set_error_message(Some(error.message().to_string()));
                result
            })
            .collect();
        let mut response = AlterUserScramCredentialsResponseData::new();
        response.set_results(results);
        ConcreteResponse::AlterUserScramCredentials(AlterUserScramCredentialsResponse::new(response, self.version))
    }

    /// Parses a request from a readable buffer at the given version.
    ///
    /// # Errors
    ///
    /// Returns an error if parsing fails.
    pub fn parse(readable: &mut dyn Readable, version: i16) -> io::Result<Self> {
        let data = AlterUserScramCredentialsRequestData::read(readable, version)?;
        Ok(Self::new(data, version))
    }

    /// Returns a copy of `data` with every upsertion's salt and salted
    /// password emptied, for rendering.
    ///
    /// Mirrors Java's private `maskData`
    /// (`AlterUserScramCredentialsRequest.java:85-91`), which
    /// `Builder.toString()` (`:45-46`) renders; Java returns the copy's
    /// `toString()`, and the caller here renders the copy.
    fn mask_data(data: &AlterUserScramCredentialsRequestData) -> AlterUserScramCredentialsRequestData {
        let mut temp_data = data.clone();
        for upsertion in &mut temp_data.upsertions {
            upsertion.set_salt(Vec::new());
            upsertion.set_salted_password(Vec::new());
        }
        temp_data
    }
}

impl std::fmt::Display for AlterUserScramCredentialsRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Mirrors Java's toString, which redacts salt/password rather than
        // printing credential secrets.
        write!(
            f,
            "AlterUserScramCredentialsRequest(version={}, deletions={}, upsertions={})",
            self.version,
            self.data.deletions.len(),
            self.data.upsertions.len()
        )
    }
}

/// Renders exactly what the redacting [`Display`](std::fmt::Display) renders.
///
/// Java has a single `toString()` (`AlterUserScramCredentialsRequest.java:96-97`);
/// a derived `Debug` would be a second, unredacted rendering that prints every
/// upsertion's salt and salted password.
impl std::fmt::Debug for AlterUserScramCredentialsRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self, f)
    }
}

/// Builder for [`AlterUserScramCredentialsRequest`].
///
/// Corresponds to `AlterUserScramCredentialsRequest.Builder`.
#[derive(Clone)]
pub struct AlterUserScramCredentialsRequestBuilder {
    data: AlterUserScramCredentialsRequestData,
    oldest_allowed_version: i16,
    latest_allowed_version: i16,
}

impl AlterUserScramCredentialsRequestBuilder {
    /// Creates a builder wrapping the given request data.
    pub fn new(data: AlterUserScramCredentialsRequestData) -> Self {
        Self {
            data,
            oldest_allowed_version: ApiKeys::ALTER_USER_SCRAM_CREDENTIALS.oldest_version(),
            latest_allowed_version: ApiKeys::ALTER_USER_SCRAM_CREDENTIALS.latest_version(),
        }
    }
}

impl RequestBuilder for AlterUserScramCredentialsRequestBuilder {
    fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::ALTER_USER_SCRAM_CREDENTIALS
    }

    fn oldest_allowed_version(&self) -> i16 {
        self.oldest_allowed_version
    }

    fn latest_allowed_version(&self) -> i16 {
        self.latest_allowed_version
    }

    fn build_version(&mut self, version: i16) -> io::Result<ConcreteRequest> {
        Ok(ConcreteRequest::AlterUserScramCredentials(
            AlterUserScramCredentialsRequest::new(self.data.clone(), version),
        ))
    }
}

/// Mirrors Java's `Builder.toString()` (`AlterUserScramCredentialsRequest.java:45-46`),
/// which returns `maskData(data)`: the data with every salt and salted password
/// emptied.
impl std::fmt::Display for AlterUserScramCredentialsRequestBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", AlterUserScramCredentialsRequest::mask_data(&self.data))
    }
}

/// Renders exactly what the redacting [`Display`](std::fmt::Display) renders.
///
/// Java has a single `toString()`; a derived `Debug` would be a second,
/// unredacted rendering that prints every salt and salted password.
impl std::fmt::Debug for AlterUserScramCredentialsRequestBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self, f)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::alter_user_scram_credentials_request_data::{ScramCredentialDeletion, ScramCredentialUpsertion};

    fn deletion(name: &str, mechanism: i8) -> ScramCredentialDeletion {
        let mut d = ScramCredentialDeletion::new();
        d.set_name(name.to_string()).set_mechanism(mechanism);
        d
    }

    fn upsertion(name: &str) -> ScramCredentialUpsertion {
        let mut u = ScramCredentialUpsertion::new();
        u.set_name(name.to_string())
            .set_mechanism(1)
            .set_iterations(4096)
            .set_salt(b"salt".to_vec())
            .set_salted_password(b"pw".to_vec());
        u
    }

    #[test]
    fn error_response_has_one_sorted_result_per_distinct_user() {
        let mut data = AlterUserScramCredentialsRequestData::new();
        data.set_deletions(vec![deletion("charlie", 1), deletion("alice", 1)]);
        data.set_upsertions(vec![upsertion("bob"), upsertion("alice")]);
        let request = AlterUserScramCredentialsRequest::new(data, 0);
        let ConcreteResponse::AlterUserScramCredentials(response) =
            request.get_error_response(0, &Errors::ClusterAuthorizationFailed)
        else {
            panic!("expected AlterUserScramCredentials response");
        };
        let users: Vec<&str> = response.data().results.iter().map(|r| r.user.as_str()).collect();
        // Distinct + sorted: alice appears once even though it is in both lists.
        assert_eq!(users, vec!["alice", "bob", "charlie"]);
        assert!(
            response
                .data()
                .results
                .iter()
                .all(|r| r.error_code == Errors::ClusterAuthorizationFailed.code())
        );
    }

    /// Distinctive secrets for the rendering tests, so an assertion cannot pass
    /// by matching some unrelated field.
    const SALT: &[u8] = b"bob-salt-3c8e5a";
    const SALTED_PASSWORD: &[u8] = b"bob-salted-pw-6d2f9b";

    /// One deletion (for `alice`) and one upsertion (for `bob`) carrying
    /// [`SALT`] and [`SALTED_PASSWORD`].
    fn data_with_secrets() -> AlterUserScramCredentialsRequestData {
        let mut upsertion = ScramCredentialUpsertion::new();
        upsertion
            .set_name("bob".to_string())
            .set_mechanism(2)
            .set_iterations(8192)
            .set_salt(SALT.to_vec())
            .set_salted_password(SALTED_PASSWORD.to_vec());
        let mut data = AlterUserScramCredentialsRequestData::new();
        data.set_deletions(vec![deletion("alice", 1)]);
        data.set_upsertions(vec![upsertion]);
        data
    }

    /// Asserts that `rendered` shows neither secret: not as the byte list a
    /// derived `Debug` prints, and not as text.
    fn assert_no_secret(rendered: &str) {
        for secret in [SALT, SALTED_PASSWORD] {
            assert!(!rendered.contains(&format!("{secret:?}")), "secret leaked: {rendered}");
            assert!(
                !rendered.contains(std::str::from_utf8(secret).unwrap()),
                "secret leaked: {rendered}"
            );
        }
    }

    /// New test, no Java original: the redacting `Display` counts the
    /// deletions and upsertions and prints no salt or salted password.
    #[test]
    fn display_redacts_salt_and_salted_password() {
        let request = AlterUserScramCredentialsRequest::new(data_with_secrets(), 0);
        let rendered = request.to_string();
        assert_eq!(
            rendered,
            "AlterUserScramCredentialsRequest(version=0, deletions=1, upsertions=1)"
        );
        assert_no_secret(&rendered);
    }

    /// New test, no Java original: `Debug` renders exactly what `Display` does
    /// (Java has a single `toString()`), never the salt or salted password a
    /// derived `Debug` would print as their byte values.
    #[test]
    fn debug_redacts_salt_and_salted_password() {
        let request = AlterUserScramCredentialsRequest::new(data_with_secrets(), 0);
        assert_eq!(request.data().upsertions[0].salt, SALT);
        assert_eq!(request.data().upsertions[0].salted_password, SALTED_PASSWORD);
        for rendered in [format!("{request:?}"), format!("{request:#?}")] {
            assert_eq!(rendered, request.to_string());
            assert!(rendered.contains("upsertions=1"), "{rendered}");
            assert_no_secret(&rendered);
        }
    }

    /// New test, no Java original: the builder renders as Java's
    /// `Builder.toString()` does, the data with every salt and salted password
    /// emptied (`maskData`), so the user, mechanism and iterations still show.
    #[test]
    fn builder_display_masks_salt_and_salted_password() {
        let builder = AlterUserScramCredentialsRequestBuilder::new(data_with_secrets());
        let rendered = builder.to_string();
        let mut masked = data_with_secrets();
        masked.upsertions[0].set_salt(Vec::new()).set_salted_password(Vec::new());
        assert_eq!(rendered, masked.to_string());
        assert!(rendered.contains("name: \"alice\""), "{rendered}");
        assert!(rendered.contains("name: \"bob\""), "{rendered}");
        assert!(rendered.contains("iterations: 8192"), "{rendered}");
        assert!(rendered.contains("salt: []"), "{rendered}");
        assert!(rendered.contains("salted_password: []"), "{rendered}");
        assert_no_secret(&rendered);
    }

    /// New test, no Java original: the builder's `Debug` renders exactly what
    /// its `Display` does, never the salt or salted password.
    #[test]
    fn builder_debug_masks_salt_and_salted_password() {
        let builder = AlterUserScramCredentialsRequestBuilder::new(data_with_secrets());
        assert_eq!(builder.data.upsertions[0].salt, SALT);
        for rendered in [format!("{builder:?}"), format!("{builder:#?}")] {
            assert_eq!(rendered, builder.to_string());
            assert!(rendered.contains("name: \"bob\""), "{rendered}");
            assert_no_secret(&rendered);
        }
    }

    #[test]
    fn serialize_parse_round_trip() {
        let version = ApiKeys::ALTER_USER_SCRAM_CREDENTIALS.latest_version();
        let mut data = AlterUserScramCredentialsRequestData::new();
        data.set_deletions(vec![deletion("d0", 2)]);
        data.set_upsertions(vec![upsertion("u0")]);
        let mut builder = AlterUserScramCredentialsRequestBuilder::new(data);
        let mut request = builder.build().unwrap();
        let bytes = request.serialize().unwrap();
        let mut readable = crate::common::ByteBufferAccessor::new(bytes.into_buffer());
        let parsed = AlterUserScramCredentialsRequest::parse(&mut readable, version).unwrap();
        assert_eq!(parsed.data().deletions.len(), 1);
        assert_eq!(parsed.data().deletions[0].name, "d0");
        assert_eq!(parsed.data().deletions[0].mechanism, 2);
        assert_eq!(parsed.data().upsertions.len(), 1);
        assert_eq!(parsed.data().upsertions[0].name, "u0");
        assert_eq!(parsed.data().upsertions[0].salt, b"salt");
        assert_eq!(parsed.data().upsertions[0].salted_password, b"pw");
    }

    #[test]
    fn known_wire_vector_deletion_and_upsertion() {
        // v0 flexible encoding: one deletion "d0"/mech 2, one upsertion "u0".
        let mut data = AlterUserScramCredentialsRequestData::new();
        data.set_deletions(vec![deletion("d0", 2)]);
        data.set_upsertions(vec![upsertion("u0")]);
        let mut builder = AlterUserScramCredentialsRequestBuilder::new(data);
        let mut request = builder.build_version(0).unwrap();
        let bytes = request.serialize().unwrap().into_buffer();
        let expected: Vec<u8> = vec![
            0x02, // deletions: compact array len 1 (1+1)
            0x03, b'd', b'0', // deletion name "d0"
            0x02, // deletion mechanism 2 (int8)
            0x00, // deletion tagged fields
            0x02, // upsertions: compact array len 1
            0x03, b'u', b'0', // upsertion name "u0"
            0x01, // upsertion mechanism 1
            0x00, 0x00, 0x10, 0x00, // iterations 4096
            0x05, b's', b'a', b'l', b't', // salt (compact bytes len 4+1)
            0x03, b'p', b'w', // salted_password (compact bytes len 2+1)
            0x00, // upsertion tagged fields
            0x00, // request tagged fields
        ];
        assert_eq!(bytes.as_slice(), expected.as_slice());
    }
}
