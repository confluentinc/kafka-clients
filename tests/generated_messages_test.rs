/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#[cfg(test)]
mod tests {
    use confluent_kafka_rust::*;

    #[test]
    fn test_generated_messages_accessible() {
        // Test that generated message types are accessible
        let _produce_req = produce_request::ProduceRequestData::new();
        let _fetch_req = fetch_request::FetchRequestData::new();
        let _metadata_req = metadata_request::MetadataRequestData::new();
    }

    #[test]
    fn test_produce_request_instantiation() {
        let req = produce_request::ProduceRequestData::new();
        // Verify the struct can be created
        assert!(format!("{:?}", req).contains("ProduceRequestData"));
        assert_eq!(produce_request::ProduceRequestData::API_KEY, 0);
    }

    #[test]
    fn test_fetch_request_instantiation() {
        let req = fetch_request::FetchRequestData::new();
        assert!(format!("{:?}", req).contains("FetchRequestData"));
        assert_eq!(fetch_request::FetchRequestData::API_KEY, 1);
    }
    
    #[test]
    fn test_nested_struct_generation() {
        // Test that nested structs are properly generated
        use offset_for_leader_epoch_request::*;
        
        // Create nested struct instances
        let partition = OffsetForLeaderPartition::new();
        assert_eq!(partition.partition, 0);
        assert_eq!(partition.leader_epoch, 0);
        
        let topic = OffsetForLeaderTopic::new();
        assert_eq!(topic.topic, "");
        assert_eq!(topic.partitions.len(), 0);
        
        // Create the main request with nested data
        let mut req = OffsetForLeaderEpochRequestData::new();
        assert_eq!(req.replica_id, 0);
        assert_eq!(req.topics.len(), 0);
        
        // Add a topic with partitions
        let mut topic_with_data = OffsetForLeaderTopic::new();
        topic_with_data.topic = "test-topic".to_string();
        topic_with_data.partitions.push(partition);
        req.topics.push(topic_with_data);
        
        assert_eq!(req.topics.len(), 1);
        assert_eq!(req.topics[0].topic, "test-topic");
        assert_eq!(req.topics[0].partitions.len(), 1);
    }
    
    #[test]
    fn test_common_structs_generation() {
        // Test that commonStructs are properly generated
        use add_partitions_to_txn_request::*;
        
        let topic = AddPartitionsToTxnTopic::new();
        assert_eq!(topic.name, "");
        assert_eq!(topic.partitions.len(), 0);
        
        let mut transaction = AddPartitionsToTxnTransaction::new();
        transaction.transactional_id = "txn-1".to_string();
        transaction.producer_id = 12345;
        transaction.topics.push(topic);
        
        assert_eq!(transaction.topics.len(), 1);
    }
    
    #[test]
    fn test_uuid_type_generation() {
        // Test that UUID fields are properly generated
        use share_group_describe_response::*;
        use confluent_kafka_rust::common::Uuid;
        
        // Create a TopicPartitions with UUID
        let mut topic_partitions = TopicPartitions::new();
        assert!(topic_partitions.topic_id.is_zero());
        
        // Set a UUID value
        let uuid = Uuid::new(0x0123456789ABCDEF, 0xFEDCBA9876543210);
        topic_partitions.topic_id = uuid;
        
        assert_eq!(topic_partitions.topic_id.most_sig_bits(), 0x0123456789ABCDEF);
        assert_eq!(topic_partitions.topic_id.least_sig_bits(), 0xFEDCBA9876543210);
        assert!(!topic_partitions.topic_id.is_zero());
        
        // Test UUID string conversion (base64 URL encoding without padding)
        let uuid_str = topic_partitions.topic_id.to_string();
        assert_eq!(uuid_str, "ASNFZ4mrze_-3LqYdlQyEA");
    }
}

