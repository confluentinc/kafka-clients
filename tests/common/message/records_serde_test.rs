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

//! Translated from `org.apache.kafka.common.message.RecordsSerdeTest`.

use std::hash::{DefaultHasher, Hash, Hasher};

use bytes::Bytes;

use crate::common::simple_records_message_data::SimpleRecordsMessageData;
use confluent_kafka::common::compress::Compression;
use confluent_kafka::common::protocol::message_util::to_byte_buffer_accessor;
use confluent_kafka::common::protocol::{ByteBufferAccessor, Message, ObjectSerializationCache};
use confluent_kafka::common::record::{MemoryRecords, SimpleRecord};

fn hash_of<T: Hash>(val: &T) -> u64 {
    let mut hasher = DefaultHasher::new();
    val.hash(&mut hasher);
    hasher.finish()
}

/// Java: `new SimpleRecordsMessageData(readable, version)`.
fn deserialize(buf: &[u8], version: i16) -> SimpleRecordsMessageData {
    let mut accessor = ByteBufferAccessor::from_bytes(buf.to_vec());
    let mut message = SimpleRecordsMessageData::new();
    Message::read(&mut message, &mut accessor, version).unwrap();
    message
}

/// Java: `testRoundTrip`. Also asserts `size()` agrees with the bytes written, as
/// the sibling test files do — a cheap extra check Java gets from its own
/// `MessageUtil` path.
fn test_round_trip(message: &mut SimpleRecordsMessageData, version: i16) {
    let accessor = to_byte_buffer_accessor(message, version).unwrap();
    let buf = accessor.buffer();

    let mut cache = ObjectSerializationCache::new();
    let computed_size = message.size(&mut cache, version).unwrap();
    assert_eq!(buf.len(), computed_size as usize, "size() mismatch for version {version}");

    let message2 = deserialize(buf, version);
    assert_eq!(*message, message2, "round trip mismatch for version {version}");
    assert_eq!(hash_of(message), hash_of(&message2), "hash mismatch for version {version}");
}

/// Java: `testAllRoundTrips`.
fn test_all_round_trips(message: &mut SimpleRecordsMessageData) {
    for version in
        SimpleRecordsMessageData::LOWEST_SUPPORTED_VERSION..=SimpleRecordsMessageData::HIGHEST_SUPPORTED_VERSION
    {
        test_round_trip(message, version);
    }
}

/// Java: `MemoryRecords.withRecords(Compression.NONE, new SimpleRecord(..), ..)`.
fn records_of(values: &[&str]) -> Bytes {
    let records: Vec<SimpleRecord> = values
        .iter()
        .map(|value| SimpleRecord::new_with_value(Some(value.as_bytes().to_vec())))
        .collect();
    MemoryRecords::with_records(Compression::none(), &records)
        .buffer_bytes()
        .clone()
}

#[test]
fn test_serde_records() {
    let mut message = SimpleRecordsMessageData::new();
    message.topic = "foo".to_string();
    message.record_set = Some(records_of(&["foo", "bar"]));

    test_all_round_trips(&mut message);
}

#[test]
fn test_serde_null_records() {
    let mut message = SimpleRecordsMessageData::new();
    message.topic = "foo".to_string();

    // Java asserts `assertNull(message.recordSet())` on a message whose record set
    // was never set. `FieldSpec.fieldDefault` returns `"null"` unconditionally for a
    // `records` field (FieldSpec.java:453-454) — unlike `bytes`, which defaults to
    // an empty buffer unless the spec says `"default": "null"`. This assertion is
    // what pins that asymmetry in the generator.
    assert_eq!(
        message.record_set, None,
        "an unset record set must default to None, not an empty buffer"
    );

    test_all_round_trips(&mut message);
}

#[test]
fn test_serde_empty_records() {
    let mut message = SimpleRecordsMessageData::new();
    message.topic = "foo".to_string();
    message.record_set = Some(MemoryRecords::empty().buffer_bytes().clone());

    test_all_round_trips(&mut message);
}

/// Not in the Java test: guards the distinction the fix above turns on. An unset
/// record set and an explicitly empty one are different values and must encode
/// differently — null as length −1, empty as length 0 — so the two must not
/// round-trip into each other.
#[test]
fn test_null_and_empty_records_are_distinct_on_the_wire() {
    let mut null_records = SimpleRecordsMessageData::new();
    null_records.topic = "foo".to_string();

    let mut empty_records = SimpleRecordsMessageData::new();
    empty_records.topic = "foo".to_string();
    empty_records.record_set = Some(Bytes::new());

    assert_ne!(null_records, empty_records);

    for version in
        SimpleRecordsMessageData::LOWEST_SUPPORTED_VERSION..=SimpleRecordsMessageData::HIGHEST_SUPPORTED_VERSION
    {
        let null_bytes = to_byte_buffer_accessor(&mut null_records, version).unwrap().buffer().to_vec();
        let empty_bytes = to_byte_buffer_accessor(&mut empty_records, version).unwrap().buffer().to_vec();
        assert_ne!(
            null_bytes, empty_bytes,
            "null and empty record sets must encode differently at version {version}"
        );

        assert_eq!(deserialize(&null_bytes, version).record_set, None);
        assert_eq!(deserialize(&empty_bytes, version).record_set, Some(Bytes::new()));
    }
}

/// Not in the Java test: guards the **non-nullable** `records` write path. The
/// sole non-nullable `records` field in the corpus is
/// `FetchSnapshotResponse.UnalignedRecords`. Java's `write` never mutates the
/// message, so the field must survive a `write` intact, two writes must emit the
/// same bytes, and `size()` computed after a `write` must still match. Before the
/// fix the non-nullable path used `std::mem::take`, which emptied the field as a
/// side effect of serialising — a bug a single-write round-trip cannot catch (the
/// one write still produced correct bytes). This is the non-nullable analog of
/// [`test_null_and_empty_records_are_distinct_on_the_wire`].
#[test]
fn test_non_nullable_records_write_does_not_mutate_the_message() {
    use confluent_kafka::fetch_snapshot_response_data::{FetchSnapshotResponseData, PartitionSnapshot, TopicSnapshot};

    let records = records_of(&["foo", "bar"]);

    let mut partition = PartitionSnapshot::new();
    partition.index = 0;
    partition.unaligned_records = records.clone();
    let mut topic = TopicSnapshot::new();
    topic.name = "foo".to_string();
    topic.partitions = vec![partition];
    let mut message = FetchSnapshotResponseData::new();
    message.topics = vec![topic];

    let version = 0;

    // First write.
    let bytes1 = to_byte_buffer_accessor(&mut message, version).unwrap().buffer().to_vec();

    // The write must NOT have emptied the (non-nullable) records field — the exact
    // side effect `std::mem::take` produced.
    assert_eq!(
        message.topics[0].partitions[0].unaligned_records, records,
        "writing a non-nullable records field must not mutate it (no std::mem::take)"
    );

    // `size()` computed after the write must still agree with the bytes written.
    let mut cache = ObjectSerializationCache::new();
    let computed_size = message.size(&mut cache, version).unwrap();
    assert_eq!(
        bytes1.len(),
        computed_size as usize,
        "size() after write disagrees with bytes written"
    );

    // A second write of the same message must produce identical bytes.
    let bytes2 = to_byte_buffer_accessor(&mut message, version).unwrap().buffer().to_vec();
    assert_eq!(
        bytes1, bytes2,
        "a second write must emit the same bytes (write must be side-effect-free)"
    );
}
