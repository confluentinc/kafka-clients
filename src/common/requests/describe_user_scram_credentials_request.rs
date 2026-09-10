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

//! DescribeUserScramCredentials request handling.
//!
//! Corresponds to
//! `org.apache.kafka.common.requests.DescribeUserScramCredentialsRequest`.

use std::io;

use crate::common::protocol::{ApiKeys, Errors, Readable};
use crate::describe_user_scram_credentials_request_data::DescribeUserScramCredentialsRequestData;
use crate::describe_user_scram_credentials_response_data::{
    DescribeUserScramCredentialsResponseData, DescribeUserScramCredentialsResult,
};

use super::{ConcreteRequest, ConcreteResponse, DescribeUserScramCredentialsResponse, RequestBuilder};

/// A DescribeUserScramCredentials request.
///
/// Corresponds to
/// `org.apache.kafka.common.requests.DescribeUserScramCredentialsRequest`.
#[derive(Debug, Clone)]
pub struct DescribeUserScramCredentialsRequest {
    data: DescribeUserScramCredentialsRequestData,
    version: i16,
}

impl DescribeUserScramCredentialsRequest {
    /// Creates a new request from data and version.
    pub fn new(data: DescribeUserScramCredentialsRequestData, version: i16) -> Self {
        Self { data, version }
    }

    /// Returns a reference to the underlying data.
    pub fn data(&self) -> &DescribeUserScramCredentialsRequestData {
        &self.data
    }

    /// Returns a mutable reference to the underlying data.
    pub(crate) fn data_mut(&mut self) -> &mut DescribeUserScramCredentialsRequestData {
        &mut self.data
    }

    /// Returns the API version of this request.
    pub fn version(&self) -> i16 {
        self.version
    }

    /// Returns the API key for this request.
    pub fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::DESCRIBE_USER_SCRAM_CREDENTIALS
    }

    /// Creates an error response for this request.
    ///
    /// Mirrors `DescribeUserScramCredentialsRequest.getErrorResponse`: a
    /// message-level error plus one errored result per requested user.
    pub fn get_error_response(&self, throttle_time_ms: i32, error: &Errors) -> ConcreteResponse {
        let mut response = DescribeUserScramCredentialsResponseData::new();
        response
            .set_throttle_time_ms(throttle_time_ms)
            .set_error_code(error.code())
            .set_error_message(Some(error.message().to_string()));
        if let Some(users) = &self.data.users {
            let mut results = Vec::with_capacity(users.len());
            for _ in users {
                let mut result = DescribeUserScramCredentialsResult::new();
                result
                    .set_error_code(error.code())
                    .set_error_message(Some(error.message().to_string()));
                results.push(result);
            }
            response.set_results(results);
        }
        ConcreteResponse::DescribeUserScramCredentials(DescribeUserScramCredentialsResponse::new(
            response,
            self.version,
        ))
    }

    /// Parses a request from a readable buffer at the given version.
    ///
    /// # Errors
    ///
    /// Returns an error if parsing fails.
    pub fn parse(readable: &mut dyn Readable, version: i16) -> io::Result<Self> {
        let data = DescribeUserScramCredentialsRequestData::read(readable, version)?;
        Ok(Self::new(data, version))
    }
}

impl std::fmt::Display for DescribeUserScramCredentialsRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "DescribeUserScramCredentialsRequest(version={}, data={:?})",
            self.version, self.data
        )
    }
}

/// Builder for [`DescribeUserScramCredentialsRequest`].
///
/// Corresponds to `DescribeUserScramCredentialsRequest.Builder`.
#[derive(Debug, Clone)]
pub struct DescribeUserScramCredentialsRequestBuilder {
    data: DescribeUserScramCredentialsRequestData,
    oldest_allowed_version: i16,
    latest_allowed_version: i16,
}

impl DescribeUserScramCredentialsRequestBuilder {
    /// Creates a builder wrapping the given request data.
    pub fn new(data: DescribeUserScramCredentialsRequestData) -> Self {
        Self {
            data,
            oldest_allowed_version: ApiKeys::DESCRIBE_USER_SCRAM_CREDENTIALS.oldest_version(),
            latest_allowed_version: ApiKeys::DESCRIBE_USER_SCRAM_CREDENTIALS.latest_version(),
        }
    }
}

impl RequestBuilder for DescribeUserScramCredentialsRequestBuilder {
    fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::DESCRIBE_USER_SCRAM_CREDENTIALS
    }

    fn oldest_allowed_version(&self) -> i16 {
        self.oldest_allowed_version
    }

    fn latest_allowed_version(&self) -> i16 {
        self.latest_allowed_version
    }

    fn build_version(&mut self, version: i16) -> io::Result<ConcreteRequest> {
        Ok(ConcreteRequest::DescribeUserScramCredentials(
            DescribeUserScramCredentialsRequest::new(self.data.clone(), version),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::describe_user_scram_credentials_request_data::UserName;

    fn request_with_users(users: &[&str]) -> DescribeUserScramCredentialsRequest {
        let mut data = DescribeUserScramCredentialsRequestData::new();
        data.set_users(Some(
            users
                .iter()
                .map(|u| {
                    let mut name = UserName::new();
                    name.set_name((*u).to_string());
                    name
                })
                .collect(),
        ));
        DescribeUserScramCredentialsRequest::new(data, 0)
    }

    #[test]
    fn error_response_has_one_result_per_requested_user() {
        let request = request_with_users(&["u0", "u1"]);
        let ConcreteResponse::DescribeUserScramCredentials(response) =
            request.get_error_response(0, &Errors::ClusterAuthorizationFailed)
        else {
            panic!("expected DescribeUserScramCredentials response");
        };
        assert_eq!(response.data().error_code, Errors::ClusterAuthorizationFailed.code());
        assert_eq!(response.data().results.len(), 2);
        assert!(
            response
                .data()
                .results
                .iter()
                .all(|r| r.error_code == Errors::ClusterAuthorizationFailed.code())
        );
    }

    #[test]
    fn error_response_with_null_users_has_no_results() {
        let request = DescribeUserScramCredentialsRequest::new(DescribeUserScramCredentialsRequestData::new(), 0);
        let ConcreteResponse::DescribeUserScramCredentials(response) =
            request.get_error_response(0, &Errors::ClusterAuthorizationFailed)
        else {
            panic!("expected DescribeUserScramCredentials response");
        };
        assert!(response.data().results.is_empty());
    }

    #[test]
    fn serialize_parse_round_trip() {
        let version = ApiKeys::DESCRIBE_USER_SCRAM_CREDENTIALS.latest_version();
        let mut builder = DescribeUserScramCredentialsRequestBuilder::new({
            let mut data = DescribeUserScramCredentialsRequestData::new();
            data.set_users(Some(vec![{
                let mut n = UserName::new();
                n.set_name("alice".to_string());
                n
            }]));
            data
        });
        let mut request = builder.build().unwrap();
        let bytes = request.serialize().unwrap();
        let mut readable = crate::common::ByteBufferAccessor::from_bytes(bytes.into_buffer());
        let parsed = DescribeUserScramCredentialsRequest::parse(&mut readable, version).unwrap();
        let users = parsed.data().users.as_ref().unwrap();
        assert_eq!(users.len(), 1);
        assert_eq!(users[0].name, "alice");
    }

    #[test]
    fn known_wire_vector_single_user() {
        // v0 flexible encoding of a request describing one user "u0".
        let mut data = DescribeUserScramCredentialsRequestData::new();
        data.set_users(Some(vec![{
            let mut n = UserName::new();
            n.set_name("u0".to_string());
            n
        }]));
        let mut builder = DescribeUserScramCredentialsRequestBuilder::new(data);
        let mut request = builder.build_version(0).unwrap();
        let bytes = request.serialize().unwrap().into_buffer();
        let expected: Vec<u8> = vec![
            0x02, // users: compact array length (1 + 1)
            0x03, b'u', b'0', // name = "u0" (compact string len 2+1)
            0x00, // UserName tagged fields
            0x00, // request tagged fields
        ];
        assert_eq!(bytes.as_slice(), expected.as_slice());
    }
}
