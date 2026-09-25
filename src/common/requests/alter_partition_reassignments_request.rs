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

//! AlterPartitionReassignments request handling.
//!
//! Corresponds to
//! `org.apache.kafka.common.requests.AlterPartitionReassignmentsRequest`.

use std::io;

use crate::AlterPartitionReassignmentsRequestData;
use crate::AlterPartitionReassignmentsResponseData;
use crate::common::protocol::{ApiKeys, Errors, Readable};

use super::{AlterPartitionReassignmentsResponse, ConcreteRequest, ConcreteResponse, RequestBuilder};

/// An AlterPartitionReassignments request.
///
/// Corresponds to
/// `org.apache.kafka.common.requests.AlterPartitionReassignmentsRequest`.
#[derive(Debug, Clone)]
pub struct AlterPartitionReassignmentsRequest {
    data: AlterPartitionReassignmentsRequestData,
    version: i16,
}

impl AlterPartitionReassignmentsRequest {
    /// Creates a new `AlterPartitionReassignmentsRequest` from data and version.
    pub fn new(data: AlterPartitionReassignmentsRequestData, version: i16) -> Self {
        Self { data, version }
    }

    /// Returns a reference to the underlying data.
    pub fn data(&self) -> &AlterPartitionReassignmentsRequestData {
        &self.data
    }

    /// Returns a mutable reference to the underlying data.
    pub(crate) fn data_mut(&mut self) -> &mut AlterPartitionReassignmentsRequestData {
        &mut self.data
    }

    /// Returns the API version of this request.
    pub fn version(&self) -> i16 {
        self.version
    }

    /// Returns the API key for this request.
    pub fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::ALTER_PARTITION_REASSIGNMENTS
    }

    /// Creates a top-level error response for this request.
    ///
    /// Mirrors `AlterPartitionReassignmentsRequest.getErrorResponse`, which sets
    /// only the top-level error code and message and returns no per-partition
    /// results.
    pub fn get_error_response(&self, throttle_time_ms: i32, error: &Errors) -> ConcreteResponse {
        let mut data = AlterPartitionReassignmentsResponseData::new();
        data.set_throttle_time_ms(throttle_time_ms);
        data.set_error_code(error.code());
        ConcreteResponse::AlterPartitionReassignments(AlterPartitionReassignmentsResponse::new(data))
    }

    /// Parses an `AlterPartitionReassignmentsRequest` from a readable buffer at
    /// the given version.
    ///
    /// # Errors
    ///
    /// Returns an error if parsing fails.
    pub fn parse(readable: &mut dyn Readable, version: i16) -> io::Result<Self> {
        let data = AlterPartitionReassignmentsRequestData::read(readable, version)?;
        Ok(Self::new(data, version))
    }
}

impl std::fmt::Display for AlterPartitionReassignmentsRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "AlterPartitionReassignmentsRequest(version={}, data={:?})",
            self.version, self.data
        )
    }
}

/// Builder for [`AlterPartitionReassignmentsRequest`].
///
/// Corresponds to `AlterPartitionReassignmentsRequest.Builder` in Java.
#[derive(Debug, Clone)]
pub struct AlterPartitionReassignmentsRequestBuilder {
    data: AlterPartitionReassignmentsRequestData,
    oldest_allowed_version: i16,
    latest_allowed_version: i16,
}

impl AlterPartitionReassignmentsRequestBuilder {
    /// Creates a builder from existing data.
    pub fn new(data: AlterPartitionReassignmentsRequestData) -> Self {
        Self {
            data,
            oldest_allowed_version: ApiKeys::ALTER_PARTITION_REASSIGNMENTS.oldest_version(),
            latest_allowed_version: ApiKeys::ALTER_PARTITION_REASSIGNMENTS.latest_version(),
        }
    }
}

impl RequestBuilder for AlterPartitionReassignmentsRequestBuilder {
    fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::ALTER_PARTITION_REASSIGNMENTS
    }

    fn oldest_allowed_version(&self) -> i16 {
        self.oldest_allowed_version
    }

    fn latest_allowed_version(&self) -> i16 {
        self.latest_allowed_version
    }

    fn build_version(&mut self, version: i16) -> io::Result<ConcreteRequest> {
        Ok(ConcreteRequest::AlterPartitionReassignments(
            AlterPartitionReassignmentsRequest::new(self.data.clone(), version),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::alter_partition_reassignments_request_data::{ReassignablePartition, ReassignableTopic};

    fn topic(name: &str, partitions: &[(i32, Vec<i32>)]) -> ReassignableTopic {
        let mut t = ReassignableTopic::new();
        t.set_name(name.to_string());
        t.set_partitions(
            partitions
                .iter()
                .map(|(idx, replicas)| {
                    let mut p = ReassignablePartition::new();
                    p.set_partition_index(*idx);
                    p.set_replicas(Some(replicas.clone()));
                    p
                })
                .collect(),
        );
        t
    }

    #[test]
    fn get_error_response_sets_top_level_error() {
        let mut data = AlterPartitionReassignmentsRequestData::new();
        data.set_topics(vec![topic("A", &[(0, vec![1, 2, 3])])]);
        let request = AlterPartitionReassignmentsRequest::new(data, 0);
        let response = request.get_error_response(50, &Errors::ClusterAuthorizationFailed);
        if let ConcreteResponse::AlterPartitionReassignments(r) = response {
            assert_eq!(r.data().throttle_time_ms, 50);
            assert_eq!(r.data().error_code, Errors::ClusterAuthorizationFailed.code());
            assert!(r.data().responses.is_empty());
        } else {
            panic!("expected AlterPartitionReassignments response");
        }
    }

    /// Round-trips a request through the shared `ConcreteRequest` serialize /
    /// parse path.
    #[test]
    fn serialize_parse_round_trip() {
        let mut data = AlterPartitionReassignmentsRequestData::new();
        data.set_timeout_ms(30000);
        data.set_allow_replication_factor_change(true);
        data.set_topics(vec![topic("A", &[(0, vec![1, 2, 3])])]);
        let mut request =
            ConcreteRequest::AlterPartitionReassignments(AlterPartitionReassignmentsRequest::new(data, 0));
        let bytes = request.serialize().unwrap();
        let mut readable = crate::common::ByteBufferAccessor::new(bytes.into_buffer());
        let parsed = AlterPartitionReassignmentsRequest::parse(&mut readable, 0).unwrap();
        assert_eq!(parsed.data().timeout_ms, 30000);
        assert!(parsed.data().allow_replication_factor_change);
        assert_eq!(parsed.data().topics.len(), 1);
        assert_eq!(parsed.data().topics[0].name, "A");
        assert_eq!(parsed.data().topics[0].partitions[0].replicas, Some(vec![1, 2, 3]));
    }

    /// Byte-level encoding test against a known vector. AlterPartitionReassignments
    /// v0 is a flexible version, so the body is:
    ///   timeout_ms: int32 = 100 (00 00 00 64)
    ///   topics: compact array (len+1 = 0x02)
    ///     name: compact string "A" (0x02, 0x41)
    ///     partitions: compact array (len+1 = 0x02)
    ///       partition_index: int32 = 0 (00 00 00 00)
    ///       replicas: compact int32 array (len+1 = 0x02), replica 1 (00 00 00 01)
    ///       _tagged_fields: 0x00
    ///     _tagged_fields: 0x00
    ///   _tagged_fields (allow_replication_factor_change is a tagged field, default true so omitted): 0x00
    ///   _tagged_fields: 0x00
    #[test]
    fn serialize_known_byte_vector_v0() {
        let mut data = AlterPartitionReassignmentsRequestData::new();
        data.set_timeout_ms(100);
        data.set_topics(vec![topic("A", &[(0, vec![1])])]);
        let mut request =
            ConcreteRequest::AlterPartitionReassignments(AlterPartitionReassignmentsRequest::new(data, 0));
        let bytes = request.serialize().unwrap();
        let expected: &[u8] = &[
            0x00, 0x00, 0x00, 0x64, // timeout_ms = 100
            0x02, // topics array length + 1
            0x02, 0x41, // name "A"
            0x02, // partitions array length + 1
            0x00, 0x00, 0x00, 0x00, // partition_index = 0
            0x02, // replicas array length + 1
            0x00, 0x00, 0x00, 0x01, // replica 1
            0x00, // partition tagged fields
            0x00, // topic tagged fields
            0x00, // request tagged fields
        ];
        assert_eq!(bytes.into_buffer().as_slice(), expected);
    }
}
