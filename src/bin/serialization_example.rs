/*
 * Copyright 2025 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

//! Executable: serialize and deserialize Kafka messages (MetadataRequest, MetadataResponse, ProduceResponse).
//!
//! This example demonstrates:
//! - Basic serialization/deserialization with MetadataRequest
//! - Complex nested structures with MetadataResponse
//! - Tagged fields with ProduceResponse (flexible versions)

use confluent_kafka_rust::common::Uuid;
use confluent_kafka_rust::common::protocol::ByteBufferAccessor;
use confluent_kafka_rust::metadata_request_data::{MetadataRequestData, MetadataRequestTopic};
use confluent_kafka_rust::metadata_response_data::{
    MetadataResponseBroker, MetadataResponseData, MetadataResponsePartition, MetadataResponseTopic,
};
use confluent_kafka_rust::produce_response_data::{
    LeaderIdAndEpoch, NodeEndpoint, PartitionProduceResponse, ProduceResponseData, TopicProduceResponse,
};

fn main() {
    println!("=== MetadataRequest Example ===\n");

    // Create MetadataRequest with example values
    let mut req = MetadataRequestData::new();
    req.topics = vec![{
        let mut topic = MetadataRequestTopic::new();
        topic.topic_id = Uuid::new(0x12345678_9abc_def0, 0x1234_567890abcdef);
        topic.name = "test-topic".to_string();
        topic
    }];
    req.allow_auto_topic_creation = true;
    req.include_cluster_authorized_operations = true;
    req.include_topic_authorized_operations = true;

    let req_version: i16 = MetadataRequestData::HIGHEST_SUPPORTED_VERSION;

    // Serialize request
    let mut accessor = ByteBufferAccessor::new(1024);
    req.write(&mut accessor, req_version).expect("serialize request");
    let buffer = accessor.buffer();
    println!("Serialized MetadataRequestData ({} bytes): {:x?}", buffer.len(), buffer);

    // Deserialize request
    let mut accessor = ByteBufferAccessor::from_bytes(buffer.to_vec());
    let req2 = MetadataRequestData::read(&mut accessor, req_version).expect("deserialize request");
    println!("Deserialized MetadataRequestData: {:#?}\n", req2);

    println!("=== MetadataResponse Example ===\n");

    // Create MetadataResponse with example values
    let mut resp = MetadataResponseData::new();

    // Add brokers
    resp.brokers = vec![
        {
            let mut broker = MetadataResponseBroker::new();
            broker.node_id = 1;
            broker.host = "broker1.example.com".to_string();
            broker.port = 9092;
            broker.rack = "rack1".to_string();
            broker
        },
        {
            let mut broker = MetadataResponseBroker::new();
            broker.node_id = 2;
            broker.host = "broker2.example.com".to_string();
            broker.port = 9092;
            broker.rack = "rack2".to_string();
            broker
        },
    ];

    resp.cluster_id = "test-cluster".to_string();
    resp.controller_id = 1;

    // Add topics with partitions
    resp.topics = vec![{
        let mut topic = MetadataResponseTopic::new();
        topic.error_code = 0;
        topic.name = "test-topic".to_string();
        topic.topic_id = Uuid::new(0xabcdef12_3456_7890, 0xabcd_ef1234567890);
        topic.is_internal = false;
        topic.partitions = vec![
            {
                let mut partition = MetadataResponsePartition::new();
                partition.error_code = 0;
                partition.partition_index = 0;
                partition.leader_id = 1;
                partition.leader_epoch = 0;
                partition.replica_nodes = vec![1, 2];
                partition.isr_nodes = vec![1, 2];
                partition.offline_replicas = vec![];
                partition
            },
            {
                let mut partition = MetadataResponsePartition::new();
                partition.error_code = 0;
                partition.partition_index = 1;
                partition.leader_id = 2;
                partition.leader_epoch = 0;
                partition.replica_nodes = vec![2, 1];
                partition.isr_nodes = vec![2, 1];
                partition.offline_replicas = vec![];
                partition
            },
        ];
        topic.topic_authorized_operations = -2147483648; // Default value
        topic
    }];

    let resp_version: i16 = MetadataResponseData::HIGHEST_SUPPORTED_VERSION;

    // Serialize response
    let mut accessor = ByteBufferAccessor::new(2048);
    resp.write(&mut accessor, resp_version).expect("serialize response");
    let buffer = accessor.buffer();
    println!("Serialized MetadataResponseData ({} bytes): {:x?}", buffer.len(), buffer);

    // Deserialize response
    let mut accessor = ByteBufferAccessor::from_bytes(buffer.to_vec());
    let resp2 = MetadataResponseData::read(&mut accessor, resp_version).expect("deserialize response");
    println!("Deserialized MetadataResponseData: {:#?}\n", resp2);

    println!("=== ProduceResponse Example (with Tagged Fields) ===\n");

    // Create ProduceResponse with tagged fields
    let mut produce_resp = ProduceResponseData::new();
    produce_resp.throttle_time_ms = 100;

    // Add topic responses
    produce_resp.responses = vec![{
        let mut topic = TopicProduceResponse::new();
        topic.name = String::new(); // Empty string for flexible version
        topic.topic_id = Uuid::new(0xfedcba98_7654_3210, 0xfedc_ba9876543210);
        topic.partition_responses = vec![
            {
                let mut partition = PartitionProduceResponse::new();
                partition.index = 0;
                partition.error_code = 0;
                partition.base_offset = 1000;
                partition.log_append_time_ms = 1234567890;
                partition.log_start_offset = 0;
                partition.error_message = String::new();
                // Tagged field: CurrentLeader (tag 0 in PartitionProduceResponse)
                partition.current_leader = LeaderIdAndEpoch { leader_id: 1, leader_epoch: 5 };
                partition
            },
            {
                let mut partition = PartitionProduceResponse::new();
                partition.index = 1;
                partition.error_code = 0;
                partition.base_offset = 2000;
                partition.log_append_time_ms = 1234567891;
                partition.log_start_offset = 0;
                partition.error_message = String::new();
                partition.current_leader = LeaderIdAndEpoch { leader_id: 2, leader_epoch: 3 };
                partition
            },
        ];
        topic
    }];

    // Tagged field: NodeEndpoints (tag 0 in ProduceResponseData)
    produce_resp.node_endpoints = vec![
        NodeEndpoint {
            node_id: 1,
            host: "broker1.example.com".to_string(),
            port: 9092,
            rack: "rack1".to_string(),
        },
        NodeEndpoint {
            node_id: 2,
            host: "broker2.example.com".to_string(),
            port: 9092,
            rack: "rack2".to_string(),
        },
    ];

    let produce_version: i16 = ProduceResponseData::HIGHEST_SUPPORTED_VERSION;

    // Serialize ProduceResponse
    let mut accessor = ByteBufferAccessor::new(2048);
    produce_resp
        .write(&mut accessor, produce_version)
        .expect("serialize produce response");
    let buffer = accessor.buffer();
    println!("Serialized ProduceResponseData ({} bytes): {:x?}", buffer.len(), buffer);

    // Deserialize ProduceResponse
    let mut accessor = ByteBufferAccessor::from_bytes(buffer.to_vec());
    let produce_resp2 =
        ProduceResponseData::read(&mut accessor, produce_version).expect("deserialize produce response");
    println!("\nDeserialized ProduceResponseData: {:#?}", produce_resp2);
}
