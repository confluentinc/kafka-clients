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

//! `FindCoordinator` request handling.
//!
//! Corresponds to `org.apache.kafka.common.requests.FindCoordinatorRequest`.
//!
//! Wraps the auto-generated [`FindCoordinatorRequestData`] and exposes a
//! [`FindCoordinatorRequestBuilder`] that picks the right version based on
//! the broker's `ApiVersions` and handles the batched-vs-single key
//! representation introduced in v4.

use std::io;

use crate::common::Node;
use crate::common::protocol::{ApiKeys, Errors, Readable};
use crate::find_coordinator_request_data::FindCoordinatorRequestData;
use crate::find_coordinator_response_data::{Coordinator, FindCoordinatorResponseData};

use super::ConcreteRequest;
use super::ConcreteResponse;
use super::FindCoordinatorResponse;
use super::RequestBuilder;

/// Minimum version supporting batched `coordinator_keys` instead of a single
/// `key` field.
///
/// Corresponds to `FindCoordinatorRequest.MIN_BATCHED_VERSION` in Java.
pub const MIN_BATCHED_VERSION: i16 = 4;

/// Coordinator type identifier for a `FindCoordinator` request.
///
/// Corresponds to `FindCoordinatorRequest.CoordinatorType` in Java.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CoordinatorType {
    /// Group coordinator (consumer groups).
    Group,
    /// Transaction coordinator (transactional producers).
    Transaction,
    /// Share coordinator (KIP-932 share consumers).
    Share,
}

impl CoordinatorType {
    /// Returns the wire-format `i8` id for this coordinator type.
    pub fn id(self) -> i8 {
        match self {
            Self::Group => 0,
            Self::Transaction => 1,
            Self::Share => 2,
        }
    }

    /// Looks up a `CoordinatorType` by its wire id.
    ///
    /// # Errors
    ///
    /// Returns an error if the id is not recognized.
    pub fn for_id(id: i8) -> io::Result<Self> {
        match id {
            0 => Ok(Self::Group),
            1 => Ok(Self::Transaction),
            2 => Ok(Self::Share),
            other => Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("Unknown coordinator type received: {other}"),
            )),
        }
    }
}

/// A `FindCoordinator` request.
///
/// Corresponds to `org.apache.kafka.common.requests.FindCoordinatorRequest`.
#[derive(Debug, Clone)]
pub struct FindCoordinatorRequest {
    data: FindCoordinatorRequestData,
    version: i16,
}

impl FindCoordinatorRequest {
    /// Creates a new `FindCoordinatorRequest` from data and version.
    pub fn new(data: FindCoordinatorRequestData, version: i16) -> Self {
        Self { data, version }
    }

    /// Returns a reference to the underlying data.
    pub fn data(&self) -> &FindCoordinatorRequestData {
        &self.data
    }

    /// Returns a mutable reference to the underlying data.
    pub(crate) fn data_mut(&mut self) -> &mut FindCoordinatorRequestData {
        &mut self.data
    }

    /// Returns the API version of this request.
    pub fn version(&self) -> i16 {
        self.version
    }

    /// Returns the API key for this request.
    pub fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::FIND_COORDINATOR
    }

    /// Builds the canonical error response for this request, matching Java's
    /// `FindCoordinatorRequest.getErrorResponse(throttleTimeMs, Throwable)`.
    pub fn get_error_response(&self, throttle_time_ms: i32, error: &Errors) -> ConcreteResponse {
        let mut response = FindCoordinatorResponseData::new();
        if self.version >= 2 {
            response.set_throttle_time_ms(throttle_time_ms);
        }
        if self.version < MIN_BATCHED_VERSION {
            // <= v3: single coordinator embedded in the top-level fields.
            let no_node = Node::no_node();
            response
                .set_error_code(error.code())
                .set_error_message(Some(error.message().to_string()))
                .set_node_id(no_node.id())
                .set_host(no_node.host().to_string())
                .set_port(no_node.port());
        } else {
            // v4+: one Coordinator entry per requested key, all with the same error.
            let no_node = Node::no_node();
            let coordinators: Vec<Coordinator> = self
                .data
                .coordinator_keys
                .iter()
                .map(|key| {
                    let mut c = Coordinator::new();
                    c.set_key(key.clone())
                        .set_error_code(error.code())
                        .set_error_message(Some(error.message().to_string()))
                        .set_host(no_node.host().to_string())
                        .set_port(no_node.port())
                        .set_node_id(no_node.id());
                    c
                })
                .collect();
            response.set_coordinators(coordinators);
        }
        ConcreteResponse::FindCoordinator(FindCoordinatorResponse::new(response))
    }

    /// Parses a `FindCoordinatorRequest` from a readable buffer at the given
    /// version.
    ///
    /// # Errors
    ///
    /// Returns an error if parsing fails.
    pub fn parse(readable: &mut dyn Readable, version: i16) -> io::Result<Self> {
        let data = FindCoordinatorRequestData::read(readable, version)?;
        Ok(Self::new(data, version))
    }
}

impl std::fmt::Display for FindCoordinatorRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "FindCoordinatorRequest(version={}, data={})", self.version, self.data)
    }
}

/// Builder for [`FindCoordinatorRequest`].
///
/// Corresponds to `FindCoordinatorRequest.Builder` in Java.
#[derive(Debug, Clone)]
pub struct FindCoordinatorRequestBuilder {
    data: FindCoordinatorRequestData,
    oldest_allowed_version: i16,
    latest_allowed_version: i16,
}

impl FindCoordinatorRequestBuilder {
    /// Creates a builder wrapping the given data with the full supported
    /// version range.
    pub fn new(data: FindCoordinatorRequestData) -> Self {
        Self {
            data,
            oldest_allowed_version: ApiKeys::FIND_COORDINATOR.oldest_version(),
            latest_allowed_version: ApiKeys::FIND_COORDINATOR.latest_version(),
        }
    }

    /// Returns a reference to the underlying data.
    pub fn data(&self) -> &FindCoordinatorRequestData {
        &self.data
    }
}

impl RequestBuilder for FindCoordinatorRequestBuilder {
    fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::FIND_COORDINATOR
    }

    fn oldest_allowed_version(&self) -> i16 {
        self.oldest_allowed_version
    }

    fn latest_allowed_version(&self) -> i16 {
        self.latest_allowed_version
    }

    fn build_version(&mut self, version: i16) -> io::Result<ConcreteRequest> {
        // Mirrors `FindCoordinatorRequest.Builder.build(short version)`.
        if version < 1 && self.data.key_type == CoordinatorType::Transaction.id() {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                format!(
                    "Cannot create a v{version} FindCoordinator request because we require features \
                     supported only in 2 or later."
                ),
            ));
        }

        let mut data = self.data.clone();
        let batched_keys = data.coordinator_keys.len();
        if version < MIN_BATCHED_VERSION {
            if batched_keys > 1 {
                return Err(io::Error::new(
                    io::ErrorKind::Unsupported,
                    format!(
                        "Cannot create a v{version} FindCoordinator request because we require features \
                         supported only in {MIN_BATCHED_VERSION} or later."
                    ),
                ));
            }
            if batched_keys == 1 {
                let single = data.coordinator_keys[0].clone();
                data.set_key(single);
                data.set_coordinator_keys(Vec::new());
            }
        } else if batched_keys == 0 && !data.key.is_empty() {
            data.set_coordinator_keys(vec![data.key.clone()]);
            data.set_key(String::new()); // default value
        }
        Ok(ConcreteRequest::FindCoordinator(FindCoordinatorRequest::new(data, version)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Translated from `FindCoordinatorRequest.CoordinatorType.forId`
    /// (round-trip per type and error for unknown).
    #[test]
    fn test_coordinator_type_for_id_round_trip() {
        for t in [
            CoordinatorType::Group,
            CoordinatorType::Transaction,
            CoordinatorType::Share,
        ] {
            assert_eq!(t, CoordinatorType::for_id(t.id()).unwrap());
        }
        assert!(CoordinatorType::for_id(42).is_err());
    }

    /// Verifies that a v0 request carrying a `Transaction` key is rejected by
    /// the builder, mirroring Java's check in `Builder.build`.
    #[test]
    fn test_v0_transaction_request_rejected() {
        let mut data = FindCoordinatorRequestData::new();
        data.set_key_type(CoordinatorType::Transaction.id());
        data.set_key("txn-id".to_string());
        let mut builder = FindCoordinatorRequestBuilder::new(data);
        let err = builder.build_version(0).expect_err("v0 transaction key must be rejected");
        let msg = err.to_string();
        assert!(msg.contains("Cannot create a v0 FindCoordinator request"), "got: {msg}");
    }

    /// Verifies that a pre-v4 request with multiple coordinator keys is
    /// rejected, mirroring Java's `NoBatchedFindCoordinatorsException`.
    #[test]
    fn test_pre_v4_rejects_batched_keys() {
        let mut data = FindCoordinatorRequestData::new();
        data.set_key_type(CoordinatorType::Group.id());
        data.set_coordinator_keys(vec!["g1".to_string(), "g2".to_string()]);
        let mut builder = FindCoordinatorRequestBuilder::new(data);
        let err = builder.build_version(3).expect_err("pre-v4 batched keys must be rejected");
        let msg = err.to_string();
        assert!(msg.contains("4 or later"), "got: {msg}");
    }

    /// Verifies that a pre-v4 build with a single coordinator key folds it
    /// into `key` and clears `coordinator_keys` (matches Java behavior).
    #[test]
    fn test_pre_v4_folds_single_coordinator_key_into_key() {
        let mut data = FindCoordinatorRequestData::new();
        data.set_key_type(CoordinatorType::Group.id());
        data.set_coordinator_keys(vec!["only".to_string()]);
        let mut builder = FindCoordinatorRequestBuilder::new(data);
        let built = builder.build_version(3).expect("pre-v4 build with one key");
        match built {
            ConcreteRequest::FindCoordinator(req) => {
                assert_eq!(req.data().key, "only");
                assert!(req.data().coordinator_keys.is_empty());
                assert_eq!(req.version(), 3);
            },
            other => panic!("expected FindCoordinator variant, got {other:?}"),
        }
    }

    /// Verifies that v4+ promotes `key` into `coordinator_keys` when the
    /// latter is empty (matches Java behavior).
    #[test]
    fn test_v4_promotes_key_to_coordinator_keys() {
        let mut data = FindCoordinatorRequestData::new();
        data.set_key_type(CoordinatorType::Group.id());
        data.set_key("g1".to_string());
        let mut builder = FindCoordinatorRequestBuilder::new(data);
        let built = builder.build_version(4).expect("v4 build");
        match built {
            ConcreteRequest::FindCoordinator(req) => {
                assert_eq!(req.data().key, "");
                assert_eq!(req.data().coordinator_keys, vec!["g1".to_string()]);
            },
            other => panic!("expected FindCoordinator variant, got {other:?}"),
        }
    }

    /// Verifies the API key accessor.
    #[test]
    fn test_api_key() {
        let builder = FindCoordinatorRequestBuilder::new(FindCoordinatorRequestData::new());
        assert_eq!(builder.api_key(), &ApiKeys::FIND_COORDINATOR);
    }
}
