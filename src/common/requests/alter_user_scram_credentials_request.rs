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
#[derive(Debug, Clone)]
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

/// Builder for [`AlterUserScramCredentialsRequest`].
///
/// Corresponds to `AlterUserScramCredentialsRequest.Builder`.
#[derive(Debug, Clone)]
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
