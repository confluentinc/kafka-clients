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

//! Tests for Kafka message tagged fields serialization/deserialization

use confluent_kafka::common::protocol::ByteBufferAccessor;
use confluent_kafka::fetch_request_data::{FetchRequestData, ReplicaState};

#[test]
fn test_fetch_request_with_cluster_id() {
    // Version 12 - supports ClusterId as tagged field (tag 0)
    let version = 12i16;

    let mut request = FetchRequestData::new();
    request.cluster_id = Some("test-cluster-123".to_string());
    request.max_wait_ms = 500;
    request.min_bytes = 1;
    request.max_bytes = 1024000;
    request.isolation_level = 0;
    request.session_id = 0;
    request.session_epoch = -1;

    // Serialize
    let mut buffer = ByteBufferAccessor::new(1024);
    request.write(&mut buffer, version).expect("Write failed");

    // Deserialize
    buffer.flip();
    let decoded = FetchRequestData::read(&mut buffer, version).expect("Read failed");

    // Verify
    assert_eq!(decoded.cluster_id, Some("test-cluster-123".to_string()));
    assert_eq!(decoded.max_wait_ms, 500);
    assert_eq!(decoded.min_bytes, 1);
    assert_eq!(decoded.max_bytes, 1024000);
}

#[test]
fn test_fetch_request_with_replica_state() {
    // Version 15 - supports ReplicaState as tagged field (tag 1)
    let version = 15i16;

    let mut request = FetchRequestData::new();
    request.max_wait_ms = 500;
    request.min_bytes = 1;
    request.max_bytes = 1024000;
    request.isolation_level = 0;
    request.session_id = 0;
    request.session_epoch = -1;

    // Set replica state
    let mut replica_state = ReplicaState::new();
    replica_state.replica_id = 42;
    replica_state.replica_epoch = 100;
    request.replica_state = replica_state;

    // Serialize
    let mut buffer = ByteBufferAccessor::new(1024);
    request.write(&mut buffer, version).expect("Write failed");

    // Deserialize
    buffer.flip();
    let decoded = FetchRequestData::read(&mut buffer, version).expect("Read failed");

    // Verify
    assert_eq!(decoded.replica_state.replica_id, 42);
    assert_eq!(decoded.replica_state.replica_epoch, 100);
    assert_eq!(decoded.max_wait_ms, 500);
}

#[test]
fn test_fetch_request_with_both_tagged_fields() {
    // Version 15 - supports both ClusterId and ReplicaState as tagged fields
    let version = 15i16;

    let mut request = FetchRequestData::new();
    request.cluster_id = Some("production-cluster".to_string());
    request.max_wait_ms = 1000;
    request.min_bytes = 10;
    request.max_bytes = 2048000;
    request.isolation_level = 1;
    request.session_id = 123;
    request.session_epoch = 5;

    // Set replica state
    let mut replica_state = ReplicaState::new();
    replica_state.replica_id = 99;
    replica_state.replica_epoch = 200;
    request.replica_state = replica_state;

    // Serialize
    let mut buffer = ByteBufferAccessor::new(1024);
    request.write(&mut buffer, version).expect("Write failed");

    // Deserialize
    buffer.flip();
    let decoded = FetchRequestData::read(&mut buffer, version).expect("Read failed");

    // Verify all fields
    assert_eq!(decoded.cluster_id, Some("production-cluster".to_string()));
    assert_eq!(decoded.replica_state.replica_id, 99);
    assert_eq!(decoded.replica_state.replica_epoch, 200);
    assert_eq!(decoded.max_wait_ms, 1000);
    assert_eq!(decoded.min_bytes, 10);
    assert_eq!(decoded.max_bytes, 2048000);
    assert_eq!(decoded.isolation_level, 1);
    assert_eq!(decoded.session_id, 123);
    assert_eq!(decoded.session_epoch, 5);
}

#[test]
fn test_fetch_request_empty_tagged_fields() {
    // Version 15 - but with empty/default tagged field values
    let version = 15i16;

    let mut request = FetchRequestData::new();
    request.max_wait_ms = 300;
    request.min_bytes = 1;
    request.max_bytes = 512000;

    // ClusterId is empty (default)
    // ReplicaState has default values (replica_id=-1, replica_epoch=-1)

    // Serialize
    let mut buffer = ByteBufferAccessor::new(1024);
    request.write(&mut buffer, version).expect("Write failed");

    // Deserialize
    buffer.flip();
    let decoded = FetchRequestData::read(&mut buffer, version).expect("Read failed");

    // Verify
    assert_eq!(decoded.cluster_id, None);
    assert_eq!(decoded.replica_state.replica_id, -1);
    assert_eq!(decoded.replica_state.replica_epoch, -1);
    assert_eq!(decoded.max_wait_ms, 300);
}
