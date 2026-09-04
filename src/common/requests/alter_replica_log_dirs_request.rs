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

//! AlterReplicaLogDirs request handling.
//!
//! Corresponds to `org.apache.kafka.common.requests.AlterReplicaLogDirsRequest`.

use std::collections::HashMap;
use std::io;

use crate::alter_replica_log_dirs_request_data::AlterReplicaLogDirsRequestData;
use crate::alter_replica_log_dirs_response_data::{
    AlterReplicaLogDirPartitionResult, AlterReplicaLogDirTopicResult, AlterReplicaLogDirsResponseData,
};
use crate::common::TopicPartition;
use crate::common::protocol::{ApiKeys, Errors, Readable};

use super::{AlterReplicaLogDirsResponse, ConcreteRequest, ConcreteResponse, RequestBuilder};

/// An AlterReplicaLogDirs request.
///
/// Corresponds to `org.apache.kafka.common.requests.AlterReplicaLogDirsRequest`.
#[derive(Debug, Clone)]
pub struct AlterReplicaLogDirsRequest {
    data: AlterReplicaLogDirsRequestData,
    version: i16,
}

impl AlterReplicaLogDirsRequest {
    /// Creates a new `AlterReplicaLogDirsRequest` from data and version.
    pub fn new(data: AlterReplicaLogDirsRequestData, version: i16) -> Self {
        Self { data, version }
    }

    /// Returns a reference to the underlying data.
    pub fn data(&self) -> &AlterReplicaLogDirsRequestData {
        &self.data
    }

    /// Returns a mutable reference to the underlying data.
    pub(crate) fn data_mut(&mut self) -> &mut AlterReplicaLogDirsRequestData {
        &mut self.data
    }

    /// Returns the API version of this request.
    pub fn version(&self) -> i16 {
        self.version
    }

    /// Returns the API key for this request.
    pub fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::ALTER_REPLICA_LOG_DIRS
    }

    /// Returns the requested destination log directory per topic partition.
    ///
    /// Mirrors `AlterReplicaLogDirsRequest.partitionDirs`.
    pub fn partition_dirs(&self) -> HashMap<TopicPartition, String> {
        let mut result = HashMap::new();
        for alter_dir in &self.data.dirs {
            for topic in &alter_dir.topics {
                for partition in &topic.partitions {
                    result.insert(TopicPartition::new(topic.name.clone(), *partition), alter_dir.path.clone());
                }
            }
        }
        result
    }

    /// Creates an error response for this request, failing every requested
    /// partition of every topic with the given error.
    ///
    /// Mirrors `AlterReplicaLogDirsRequest.getErrorResponse`.
    pub fn get_error_response(&self, throttle_time_ms: i32, error: &Errors) -> ConcreteResponse {
        let mut response = AlterReplicaLogDirsResponseData::new();
        let mut results = Vec::new();
        for alter_dir in &self.data.dirs {
            for topic in &alter_dir.topics {
                let mut topic_result = AlterReplicaLogDirTopicResult::new();
                topic_result.set_topic_name(topic.name.clone());
                let partitions = topic
                    .partitions
                    .iter()
                    .map(|partition_id| {
                        let mut partition_result = AlterReplicaLogDirPartitionResult::new();
                        partition_result.set_error_code(error.code());
                        partition_result.set_partition_index(*partition_id);
                        partition_result
                    })
                    .collect();
                topic_result.set_partitions(partitions);
                results.push(topic_result);
            }
        }
        response.set_results(results);
        response.set_throttle_time_ms(throttle_time_ms);
        ConcreteResponse::AlterReplicaLogDirs(AlterReplicaLogDirsResponse::new(response))
    }

    /// Parses an `AlterReplicaLogDirsRequest` from a readable buffer at the
    /// given version.
    ///
    /// # Errors
    ///
    /// Returns an error if parsing fails.
    pub fn parse(readable: &mut dyn Readable, version: i16) -> io::Result<Self> {
        let data = AlterReplicaLogDirsRequestData::read(readable, version)?;
        Ok(Self::new(data, version))
    }
}

impl std::fmt::Display for AlterReplicaLogDirsRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "AlterReplicaLogDirsRequest(version={}, data={:?})", self.version, self.data)
    }
}

/// Builder for [`AlterReplicaLogDirsRequest`].
///
/// Corresponds to `AlterReplicaLogDirsRequest.Builder` in Java.
#[derive(Debug, Clone)]
pub struct AlterReplicaLogDirsRequestBuilder {
    data: AlterReplicaLogDirsRequestData,
    oldest_allowed_version: i16,
    latest_allowed_version: i16,
}

impl AlterReplicaLogDirsRequestBuilder {
    /// Creates a builder from existing data.
    pub fn new(data: AlterReplicaLogDirsRequestData) -> Self {
        Self {
            data,
            oldest_allowed_version: ApiKeys::ALTER_REPLICA_LOG_DIRS.oldest_version(),
            latest_allowed_version: ApiKeys::ALTER_REPLICA_LOG_DIRS.latest_version(),
        }
    }
}

impl RequestBuilder for AlterReplicaLogDirsRequestBuilder {
    fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::ALTER_REPLICA_LOG_DIRS
    }

    fn oldest_allowed_version(&self) -> i16 {
        self.oldest_allowed_version
    }

    fn latest_allowed_version(&self) -> i16 {
        self.latest_allowed_version
    }

    fn build_version(&mut self, version: i16) -> io::Result<ConcreteRequest> {
        Ok(ConcreteRequest::AlterReplicaLogDirs(AlterReplicaLogDirsRequest::new(
            self.data.clone(),
            version,
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::alter_replica_log_dirs_request_data::{AlterReplicaLogDir, AlterReplicaLogDirTopic};

    fn topic(name: &str, partitions: Vec<i32>) -> AlterReplicaLogDirTopic {
        let mut t = AlterReplicaLogDirTopic::new();
        t.set_name(name.to_string());
        t.set_partitions(partitions);
        t
    }

    fn dir(path: &str, topics: Vec<AlterReplicaLogDirTopic>) -> AlterReplicaLogDir {
        let mut d = AlterReplicaLogDir::new();
        d.set_path(path.to_string());
        d.set_topics(topics);
        d
    }

    /// Mirrors `AlterReplicaLogDirsRequestTest.testErrorResponse`.
    #[test]
    fn test_error_response() {
        let mut data = AlterReplicaLogDirsRequestData::new();
        data.set_dirs(vec![dir("/data0", vec![topic("topic", vec![0, 1, 2])])]);
        let request = AlterReplicaLogDirsRequest::new(data, 2);
        let response = request.get_error_response(123, &Errors::LogDirNotFound);
        let ConcreteResponse::AlterReplicaLogDirs(r) = response else {
            panic!("expected AlterReplicaLogDirs response");
        };
        assert_eq!(r.data().results.len(), 1);
        let topic_response = &r.data().results[0];
        assert_eq!(topic_response.topic_name, "topic");
        assert_eq!(topic_response.partitions.len(), 3);
        for (i, partition) in topic_response.partitions.iter().enumerate() {
            assert_eq!(partition.partition_index, i as i32);
            assert_eq!(partition.error_code, Errors::LogDirNotFound.code());
        }
        assert_eq!(r.data().throttle_time_ms, 123);
    }

    /// Mirrors `AlterReplicaLogDirsRequestTest.testPartitionDir`.
    #[test]
    fn test_partition_dir() {
        let mut data = AlterReplicaLogDirsRequestData::new();
        data.set_dirs(vec![
            dir("/data0", vec![topic("topic", vec![0, 1]), topic("topic2", vec![7])]),
            dir("/data1", vec![topic("topic3", vec![12])]),
        ]);
        let request = AlterReplicaLogDirsRequest::new(data, 2);
        let expect = HashMap::from([
            (TopicPartition::new("topic", 0), "/data0".to_string()),
            (TopicPartition::new("topic", 1), "/data0".to_string()),
            (TopicPartition::new("topic2", 7), "/data0".to_string()),
            (TopicPartition::new("topic3", 12), "/data1".to_string()),
        ]);
        assert_eq!(request.partition_dirs(), expect);
    }

    /// Round-trips a request through the shared `ConcreteRequest` serialize /
    /// parse path.
    #[test]
    fn serialize_parse_round_trip() {
        let mut data = AlterReplicaLogDirsRequestData::new();
        data.set_dirs(vec![dir("/data0", vec![topic("round-trip-topic", vec![3, 4])])]);
        let mut request = ConcreteRequest::AlterReplicaLogDirs(AlterReplicaLogDirsRequest::new(data, 2));
        let bytes = request.serialize().unwrap();
        let mut readable = crate::common::ByteBufferAccessor::from_bytes(bytes.into_buffer());
        let parsed = AlterReplicaLogDirsRequest::parse(&mut readable, 2).unwrap();
        assert_eq!(parsed.data().dirs.len(), 1);
        assert_eq!(parsed.data().dirs[0].path, "/data0");
        assert_eq!(parsed.data().dirs[0].topics[0].name, "round-trip-topic");
        assert_eq!(parsed.data().dirs[0].topics[0].partitions, vec![3, 4]);
    }

    /// Byte-level encoding test against a known vector. AlterReplicaLogDirs v2
    /// is a flexible version, so the body is:
    ///   dirs: compact array (len+1 = 0x02)
    ///     path: compact string "/d" (len+1 = 0x03, 0x2f 0x64)
    ///     topics: compact array (len+1 = 0x02)
    ///       name: compact string "t" (len+1 = 0x02, 0x74)
    ///       partitions: compact array [5] (len+1 = 0x02, 0x00 00 00 05)
    ///       _tagged_fields: 0x00
    ///     _tagged_fields: 0x00
    ///   _tagged_fields: 0x00
    #[test]
    fn serialize_known_byte_vector_v2() {
        let mut data = AlterReplicaLogDirsRequestData::new();
        data.set_dirs(vec![dir("/d", vec![topic("t", vec![5])])]);
        let mut request = ConcreteRequest::AlterReplicaLogDirs(AlterReplicaLogDirsRequest::new(data, 2));
        let bytes = request.serialize().unwrap();
        let expected: &[u8] = &[
            0x02, // dirs array length + 1
            0x03, 0x2f, 0x64, // path "/d"
            0x02, // topics array length + 1
            0x02, 0x74, // name "t"
            0x02, // partitions array length + 1
            0x00, 0x00, 0x00, 0x05, // partition 5
            0x00, // topic tagged fields
            0x00, // dir tagged fields
            0x00, // request tagged fields
        ];
        assert_eq!(bytes.into_buffer().as_slice(), expected);
    }
}
