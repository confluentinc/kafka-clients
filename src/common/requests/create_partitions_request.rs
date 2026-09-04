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

//! CreatePartitions request handling.
//!
//! Corresponds to `org.apache.kafka.common.requests.CreatePartitionsRequest`.

use std::io;

use crate::common::protocol::{ApiKeys, Errors, Readable};
use crate::create_partitions_request_data::CreatePartitionsRequestData;
use crate::create_partitions_response_data::{CreatePartitionsResponseData, CreatePartitionsTopicResult};

use super::{ConcreteRequest, ConcreteResponse, CreatePartitionsResponse, RequestBuilder};

/// A CreatePartitions request.
///
/// Corresponds to `org.apache.kafka.common.requests.CreatePartitionsRequest`.
#[derive(Debug, Clone)]
pub struct CreatePartitionsRequest {
    data: CreatePartitionsRequestData,
    version: i16,
}

impl CreatePartitionsRequest {
    /// Creates a new `CreatePartitionsRequest` from data and version.
    pub fn new(data: CreatePartitionsRequestData, version: i16) -> Self {
        Self { data, version }
    }

    /// Returns a reference to the underlying data.
    pub fn data(&self) -> &CreatePartitionsRequestData {
        &self.data
    }

    /// Returns a mutable reference to the underlying data.
    pub(crate) fn data_mut(&mut self) -> &mut CreatePartitionsRequestData {
        &mut self.data
    }

    /// Returns the API version of this request.
    pub fn version(&self) -> i16 {
        self.version
    }

    /// Returns the API key for this request.
    pub fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::CREATE_PARTITIONS
    }

    /// Creates an error response for this request, failing every requested
    /// topic with the given error.
    ///
    /// Mirrors `CreatePartitionsRequest.getErrorResponse` (which uses
    /// `ApiError.fromThrowable`); the enum-dispatch caller supplies the mapped
    /// [`Errors`] directly.
    pub fn get_error_response(&self, throttle_time_ms: i32, error: &Errors) -> ConcreteResponse {
        let mut response = CreatePartitionsResponseData::new();
        response.set_throttle_time_ms(throttle_time_ms);
        let mut results = Vec::new();
        for topic in &self.data.topics {
            let mut result = CreatePartitionsTopicResult::new();
            result.set_name(topic.name.clone());
            result.set_error_code(error.code());
            result.set_error_message(Some(error.message().to_string()));
            results.push(result);
        }
        response.set_results(results);
        ConcreteResponse::CreatePartitions(CreatePartitionsResponse::new(response))
    }

    /// Parses a `CreatePartitionsRequest` from a readable buffer at the given
    /// version.
    ///
    /// # Errors
    ///
    /// Returns an error if parsing fails.
    pub fn parse(readable: &mut dyn Readable, version: i16) -> io::Result<Self> {
        let data = CreatePartitionsRequestData::read(readable, version)?;
        Ok(Self::new(data, version))
    }
}

impl std::fmt::Display for CreatePartitionsRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CreatePartitionsRequest(version={}, data={:?})", self.version, self.data)
    }
}

/// Builder for [`CreatePartitionsRequest`].
///
/// Corresponds to `CreatePartitionsRequest.Builder` in Java.
#[derive(Debug, Clone)]
pub struct CreatePartitionsRequestBuilder {
    data: CreatePartitionsRequestData,
    oldest_allowed_version: i16,
    latest_allowed_version: i16,
}

impl CreatePartitionsRequestBuilder {
    /// Creates a builder from existing data.
    pub fn new(data: CreatePartitionsRequestData) -> Self {
        Self {
            data,
            oldest_allowed_version: ApiKeys::CREATE_PARTITIONS.oldest_version(),
            latest_allowed_version: ApiKeys::CREATE_PARTITIONS.latest_version(),
        }
    }
}

impl RequestBuilder for CreatePartitionsRequestBuilder {
    fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::CREATE_PARTITIONS
    }

    fn oldest_allowed_version(&self) -> i16 {
        self.oldest_allowed_version
    }

    fn latest_allowed_version(&self) -> i16 {
        self.latest_allowed_version
    }

    fn build_version(&mut self, version: i16) -> io::Result<ConcreteRequest> {
        Ok(ConcreteRequest::CreatePartitions(CreatePartitionsRequest::new(
            self.data.clone(),
            version,
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::create_partitions_request_data::CreatePartitionsTopic;

    fn topic(name: &str, count: i32) -> CreatePartitionsTopic {
        let mut t = CreatePartitionsTopic::new();
        t.set_name(name.to_string());
        t.set_count(count);
        t
    }

    #[test]
    fn get_error_response_fails_every_topic() {
        let mut data = CreatePartitionsRequestData::new();
        data.set_topics(vec![topic("a", 3), topic("b", 4)]);
        let request = CreatePartitionsRequest::new(data, 3);
        let response = request.get_error_response(100, &Errors::InvalidTopicError);
        if let ConcreteResponse::CreatePartitions(r) = response {
            assert_eq!(r.data().results.len(), 2);
            assert_eq!(r.data().throttle_time_ms, 100);
            for result in &r.data().results {
                assert_eq!(result.error_code, Errors::InvalidTopicError.code());
            }
        } else {
            panic!("expected CreatePartitions response");
        }
    }

    /// Round-trips a request through the shared `ConcreteRequest` serialize /
    /// parse path, exercising the enum wiring end-to-end.
    #[test]
    fn serialize_parse_round_trip() {
        let mut data = CreatePartitionsRequestData::new();
        data.set_topics(vec![topic("round-trip-topic", 6)]);
        data.set_timeout_ms(30000);
        data.set_validate_only(true);
        let mut request = ConcreteRequest::CreatePartitions(CreatePartitionsRequest::new(data, 3));
        let bytes = request.serialize().unwrap();
        let mut readable = crate::common::ByteBufferAccessor::from_bytes(bytes.into_buffer());
        let parsed = CreatePartitionsRequest::parse(&mut readable, 3).unwrap();
        assert_eq!(parsed.data().topics.len(), 1);
        assert_eq!(parsed.data().topics[0].name, "round-trip-topic");
        assert_eq!(parsed.data().topics[0].count, 6);
        assert_eq!(parsed.data().timeout_ms, 30000);
        assert!(parsed.data().validate_only);
    }

    /// Byte-level encoding test against a known vector. CreatePartitions v3 is a
    /// flexible version, so the body is:
    ///   topics: compact array (len+1 = 0x02)
    ///     name: compact string "t" (len+1 = 0x02, 0x74)
    ///     count: int32 = 3 (00 00 00 03)
    ///     assignments: compact nullable array = null (0x00)
    ///     _tagged_fields: 0x00
    ///   timeout_ms: int32 = 100 (00 00 00 64)
    ///   validate_only: bool = false (0x00)
    ///   _tagged_fields: 0x00
    #[test]
    fn serialize_known_byte_vector_v3() {
        let mut data = CreatePartitionsRequestData::new();
        data.set_topics(vec![topic("t", 3)]);
        data.set_timeout_ms(100);
        data.set_validate_only(false);
        let mut request = ConcreteRequest::CreatePartitions(CreatePartitionsRequest::new(data, 3));
        let bytes = request.serialize().unwrap();
        let expected: &[u8] = &[
            0x02, // topics array length + 1
            0x02, 0x74, // name "t"
            0x00, 0x00, 0x00, 0x03, // count = 3
            0x00, // assignments = null
            0x00, // topic tagged fields
            0x00, 0x00, 0x00, 0x64, // timeout_ms = 100
            0x00, // validate_only = false
            0x00, // request tagged fields
        ];
        assert_eq!(bytes.into_buffer().as_slice(), expected);
    }
}
