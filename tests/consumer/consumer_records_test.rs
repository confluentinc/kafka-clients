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

//! Translated from `org.apache.kafka.clients.consumer.ConsumerRecordsTest`.
//!
//! Skipped tests:
//! - `testRecordsAreImmutable` — Java asserts `UnsupportedOperationException`
//!   on calls like `records.records(tp).add(...)`, which exercises the
//!   contract that `Collections.unmodifiableList` returns are read-only.
//!   In Rust, `records_partition` returns `&[ConsumerRecord<K, V>]` and
//!   `partitions()` returns an iterator over borrowed `&TopicPartition` —
//!   neither can be mutated by construction (the borrow checker enforces
//!   immutability statically), so the test is not behaviorally relevant.
//!   We do preserve the `records.count()` and `next_offsets.size()`
//!   assertions in a focused replacement test.
//! - `testRecordsByNullTopic` — Java throws `IllegalArgumentException` when
//!   `records(null)` is called. In Rust, `records_topic` accepts
//!   `&str`, which cannot be null by the type system. There is nothing to
//!   test.

use std::collections::HashMap;

use confluent_kafka::common::TopicPartition;
use confluent_kafka::common::header::RecordHeaders;
use confluent_kafka::common::record::TimestampType;
use confluent_kafka::consumer::{ConsumerRecord, ConsumerRecordOptions, ConsumerRecords, OffsetAndMetadata};
use indexmap::IndexMap;

/// Translated from `ConsumerRecordsTest.testIterator`.
#[test]
fn test_iterator() {
    let topic = "topic";
    let record_size = 10;
    let partition_size = 15;
    let empty_partition_index = 3;
    let records = build_topic_test_records(record_size, partition_size, empty_partition_index, &[topic]);

    let mut partition_count = 0;
    let mut current_partition: i32 = -1;

    for (record_count, record) in (&records).into_iter().enumerate() {
        validate_empty_partition(record, empty_partition_index);

        if current_partition != record.partition() {
            partition_count += 1;
            current_partition = record.partition();
        }
        validate_record_payload(topic, record, current_partition, record_count as i32, record_size);
    }

    // Including empty partition
    assert_eq!(partition_size, partition_count + 1);
}

/// Translated from `ConsumerRecordsTest.testRecordsByPartition`.
#[test]
fn test_records_by_partition() {
    let topics = ["topic1", "topic2"];
    let record_size: i32 = 3;
    let partition_size = 5;
    let empty_partition_index = 2;

    let consumer_records = build_topic_test_records(record_size, partition_size, empty_partition_index, &topics);

    assert_eq!((partition_size as usize) * topics.len(), consumer_records.next_offsets().len());

    for topic in &topics {
        for partition in 0..partition_size {
            let tp = TopicPartition::new(topic.to_string(), partition);
            let records = consumer_records.records_partition(&tp);

            if partition == empty_partition_index {
                assert!(records.is_empty());
            } else {
                assert_eq!(record_size as usize, records.len());
                let last_record = records.last().unwrap();
                let expected = OffsetAndMetadata::new_leader_epoch_metadata(
                    last_record.offset() + 1,
                    last_record.leader_epoch(),
                    "",
                )
                .unwrap();
                assert_eq!(consumer_records.next_offsets().get(&tp), Some(&expected));
                for (i, record) in records.iter().enumerate() {
                    validate_record_payload(topic, record, partition, i as i32, record_size);
                }
            }
        }
    }
}

/// Translated from `ConsumerRecordsTest.testRecordsByTopic`.
#[test]
fn test_records_by_topic() {
    let topics = ["topic1", "topic2", "topic3", "topic4"];
    let record_size: i32 = 3;
    let partition_size = 10;
    let empty_partition_index = 6;
    let expected_total_record_size_of_each_topic = record_size * (partition_size - 1);

    let consumer_records = build_topic_test_records(record_size, partition_size, empty_partition_index, &topics);

    assert_eq!((partition_size as usize) * topics.len(), consumer_records.next_offsets().len());

    for topic in &topics {
        let mut record_count: i32 = 0;
        let mut partition_count: i32 = 0;
        let mut current_partition: i32 = -1;

        for record in consumer_records.records_topic(topic) {
            validate_empty_partition(record, empty_partition_index);

            if current_partition != record.partition() {
                partition_count += 1;
                current_partition = record.partition();
            }

            validate_record_payload(topic, record, current_partition, record_count, record_size);
            record_count += 1;
        }

        // Including empty partition
        assert_eq!(partition_size, partition_count + 1);
        assert_eq!(expected_total_record_size_of_each_topic, record_count);
    }
}

/// Replacement for Java's `testRecordsAreImmutable`. The original test
/// asserts `UnsupportedOperationException` on attempted mutation; in Rust,
/// immutability is a type-system guarantee. The behavioral parts that still
/// matter — that `count()` reflects the non-empty partitions and `empty()`
/// has zero records — are preserved here.
#[test]
fn test_records_count_and_empty() {
    let topic = "topic";
    let record_size: i32 = 3;
    let partition_size = 6;
    let empty_partition_index = 2;

    let records = build_topic_test_records(record_size, partition_size, empty_partition_index, &[topic]);

    assert_eq!(partition_size as usize, records.next_offsets().len());
    assert_eq!((record_size * (partition_size - 1)) as usize, records.count());

    let empty: ConsumerRecords<i32, String> = ConsumerRecords::empty();
    assert_eq!(0, empty.count());
    assert!(empty.is_empty());
}

// -------- helpers --------

fn build_topic_test_records(
    record_size: i32,
    partition_size: i32,
    empty_partition_index: i32,
    topics: &[&str],
) -> ConsumerRecords<i32, String> {
    let mut partition_to_records: IndexMap<TopicPartition, Vec<ConsumerRecord<i32, String>>> = IndexMap::new();
    let mut next_offsets: HashMap<TopicPartition, OffsetAndMetadata> = HashMap::new();
    for topic in topics {
        for i in 0..partition_size {
            let mut records: Vec<ConsumerRecord<i32, String>> = Vec::with_capacity(record_size as usize);
            if i != empty_partition_index {
                for j in 0..record_size {
                    let r: ConsumerRecord<i32, String> = ConsumerRecord::new_options(
                        *topic,
                        i,
                        j as i64,
                        ConsumerRecordOptions::new(
                            0,
                            TimestampType::CreateTime,
                            0,
                            0,
                            Some(j),
                            Some(j.to_string()),
                            RecordHeaders::new(),
                            None,
                            None,
                        ),
                    );
                    records.push(r);
                }
            }
            let tp = TopicPartition::new((*topic).to_string(), i);
            partition_to_records.insert(tp.clone(), records);
            next_offsets.insert(
                tp,
                OffsetAndMetadata::new_leader_epoch_metadata(record_size as i64, None, "").unwrap(),
            );
        }
    }
    ConsumerRecords::new_next_offsets(partition_to_records, next_offsets)
}

fn validate_empty_partition(record: &ConsumerRecord<i32, String>, empty_partition_index: i32) {
    assert_ne!(
        empty_partition_index,
        record.partition(),
        "Partition {} is not empty",
        record.partition()
    );
}

fn validate_record_payload(
    topic: &str,
    record: &ConsumerRecord<i32, String>,
    current_partition: i32,
    record_count: i32,
    record_size: i32,
) {
    assert_eq!(topic, record.topic());
    assert_eq!(current_partition, record.partition());
    assert_eq!((record_count % record_size) as i64, record.offset());
    assert_eq!(Some(&(record_count % record_size)), record.key());
    assert_eq!(Some(&(record_count % record_size).to_string()), record.value());
}
