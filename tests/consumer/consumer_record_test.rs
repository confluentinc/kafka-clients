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

//! Translated from `org.apache.kafka.clients.consumer.ConsumerRecordTest`.

use confluent_kafka::common::header::{Headers, RecordHeader, RecordHeaders};
use confluent_kafka::common::record::TimestampType;
use confluent_kafka::consumer::{ConsumerRecord, NO_TIMESTAMP, NULL_SIZE};

/// Translated from `ConsumerRecordTest.testShortConstructor`.
#[test]
fn test_short_constructor() {
    let topic = "topic";
    let partition = 0;
    let offset = 23;
    let key = "key";
    let value = "value";

    let record: ConsumerRecord<&str, &str> = ConsumerRecord::new(topic, partition, offset, Some(key), Some(value));

    assert_eq!(record.topic(), topic);
    assert_eq!(record.partition(), partition);
    assert_eq!(record.offset(), offset);
    assert_eq!(record.key(), Some(&key));
    assert_eq!(record.value(), Some(&value));
    assert_eq!(record.timestamp_type(), TimestampType::NoTimestampType);
    assert_eq!(record.timestamp(), NO_TIMESTAMP);
    assert_eq!(record.serialized_key_size(), NULL_SIZE);
    assert_eq!(record.serialized_value_size(), NULL_SIZE);
    assert_eq!(record.leader_epoch(), None);
    assert_eq!(record.delivery_count(), None);
    // Empty `RecordHeaders` — assert empty rather than constructing a new
    // `RecordHeaders()` and comparing (Java relies on `equals`; in Rust
    // PartialEq on RecordHeaders is also defined but checking `.to_array()`
    // emptiness avoids extra heap allocation).
    assert!(record.headers().to_array().is_empty());
}

/// Translated from `ConsumerRecordTest.testLongConstructor`.
#[test]
fn test_long_constructor() {
    let topic = "topic";
    let partition = 0;
    let offset = 23i64;
    let timestamp: i64 = 23_434_217_432_432;
    let timestamp_type = TimestampType::CreateTime;
    let key = "key";
    let value = "value";
    let serialized_key_size: i32 = 100;
    let serialized_value_size: i32 = 1142;

    let mut headers = RecordHeaders::new();
    headers
        .add(RecordHeader::new("header key".to_string(), Some(b"header value".to_vec())))
        .unwrap();

    // 11-arg constructor (no delivery count, no leader epoch)
    let record: ConsumerRecord<&str, &str> = ConsumerRecord::with_headers(
        topic,
        partition,
        offset,
        timestamp,
        timestamp_type,
        serialized_key_size,
        serialized_value_size,
        Some(key),
        Some(value),
        headers.clone(),
        None,
    );

    assert_eq!(record.topic(), topic);
    assert_eq!(record.partition(), partition);
    assert_eq!(record.offset(), offset);
    assert_eq!(record.key(), Some(&key));
    assert_eq!(record.value(), Some(&value));
    assert_eq!(record.timestamp_type(), timestamp_type);
    assert_eq!(record.timestamp(), timestamp);
    assert_eq!(record.serialized_key_size(), serialized_key_size);
    assert_eq!(record.serialized_value_size(), serialized_value_size);
    assert_eq!(record.leader_epoch(), None);
    assert_eq!(record.delivery_count(), None);
    assert_eq!(record.headers(), &headers);

    // 12-arg constructor (with leader epoch and delivery count)
    let leader_epoch: Option<i32> = Some(10);
    let delivery_count: Option<i16> = Some(1);
    let record: ConsumerRecord<&str, &str> = ConsumerRecord::with_all(
        topic,
        partition,
        offset,
        timestamp,
        timestamp_type,
        serialized_key_size,
        serialized_value_size,
        Some(key),
        Some(value),
        headers.clone(),
        leader_epoch,
        delivery_count,
    );

    assert_eq!(record.topic(), topic);
    assert_eq!(record.partition(), partition);
    assert_eq!(record.offset(), offset);
    assert_eq!(record.key(), Some(&key));
    assert_eq!(record.value(), Some(&value));
    assert_eq!(record.timestamp_type(), timestamp_type);
    assert_eq!(record.timestamp(), timestamp);
    assert_eq!(record.serialized_key_size(), serialized_key_size);
    assert_eq!(record.serialized_value_size(), serialized_value_size);
    assert_eq!(record.leader_epoch(), leader_epoch);
    assert_eq!(record.delivery_count(), delivery_count);
    assert_eq!(record.headers(), &headers);
}
