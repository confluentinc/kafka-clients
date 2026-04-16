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

//! Tests for flexible version support and tagged fields.
//! Matches Java's MessageDataGeneratorTest patterns.

use confluent_kafka::common::protocol::{ByteBufferAccessor, RawTaggedField, Readable, Writable};

#[test]
fn test_read_unsigned_varint() {
    let mut accessor = ByteBufferAccessor::from_bytes(vec![0x00]);
    assert_eq!(accessor.read_unsigned_varint().unwrap(), 0);

    let mut accessor = ByteBufferAccessor::from_bytes(vec![0x01]);
    assert_eq!(accessor.read_unsigned_varint().unwrap(), 1);

    let mut accessor = ByteBufferAccessor::from_bytes(vec![0x7F]);
    assert_eq!(accessor.read_unsigned_varint().unwrap(), 127);

    let mut accessor = ByteBufferAccessor::from_bytes(vec![0x80, 0x01]);
    assert_eq!(accessor.read_unsigned_varint().unwrap(), 128);

    let mut accessor = ByteBufferAccessor::from_bytes(vec![0xFF, 0x01]);
    assert_eq!(accessor.read_unsigned_varint().unwrap(), 255);

    let mut accessor = ByteBufferAccessor::from_bytes(vec![0x80, 0x02]);
    assert_eq!(accessor.read_unsigned_varint().unwrap(), 256);

    // Max value (2^32 - 1)
    let mut accessor = ByteBufferAccessor::from_bytes(vec![0xFF, 0xFF, 0xFF, 0xFF, 0x0F]);
    assert_eq!(accessor.read_unsigned_varint().unwrap(), 0xFFFFFFFF);
}

#[test]
fn test_write_unsigned_varint() {
    let mut accessor = ByteBufferAccessor::new(10);
    accessor.write_unsigned_varint(0).unwrap();
    assert_eq!(accessor.buffer(), &[0x00]);

    let mut accessor = ByteBufferAccessor::new(10);
    accessor.write_unsigned_varint(1).unwrap();
    assert_eq!(accessor.buffer(), &[0x01]);

    let mut accessor = ByteBufferAccessor::new(10);
    accessor.write_unsigned_varint(127).unwrap();
    assert_eq!(accessor.buffer(), &[0x7F]);

    let mut accessor = ByteBufferAccessor::new(10);
    accessor.write_unsigned_varint(128).unwrap();
    assert_eq!(accessor.buffer(), &[0x80, 0x01]);

    let mut accessor = ByteBufferAccessor::new(10);
    accessor.write_unsigned_varint(255).unwrap();
    assert_eq!(accessor.buffer(), &[0xFF, 0x01]);

    let mut accessor = ByteBufferAccessor::new(10);
    accessor.write_unsigned_varint(256).unwrap();
    assert_eq!(accessor.buffer(), &[0x80, 0x02]);

    // Max value (2^32 - 1)
    let mut accessor = ByteBufferAccessor::new(10);
    accessor.write_unsigned_varint(0xFFFFFFFF).unwrap();
    assert_eq!(accessor.buffer(), &[0xFF, 0xFF, 0xFF, 0xFF, 0x0F]);
}

#[test]
fn test_unsigned_varint_round_trip() {
    let test_values = vec![
        0u32, 1, 127, 128, 255, 256, 16383, 16384, 2097151, 2097152, 268435455, 268435456, 0xFFFFFFFF,
    ];

    for value in test_values {
        let mut accessor = ByteBufferAccessor::new(10);
        accessor.write_unsigned_varint(value).unwrap();

        let mut read_accessor = ByteBufferAccessor::from_bytes(accessor.buffer().to_vec());
        let read_value = read_accessor.read_unsigned_varint().unwrap();

        assert_eq!(value, read_value, "Round trip failed for value {}", value);
    }
}

#[test]
fn test_raw_tagged_field_creation() {
    let field = RawTaggedField::new(5, vec![1, 2, 3, 4]);
    assert_eq!(field.tag(), 5);
    assert_eq!(field.data(), &[1, 2, 3, 4]);
    assert_eq!(field.size(), 4);
}

#[test]
fn test_raw_tagged_field_equality() {
    let field1 = RawTaggedField::new(5, vec![1, 2, 3]);
    let field2 = RawTaggedField::new(5, vec![1, 2, 3]);
    let field3 = RawTaggedField::new(6, vec![1, 2, 3]);
    let field4 = RawTaggedField::new(5, vec![1, 2, 4]);

    assert_eq!(field1, field2);
    assert_ne!(field1, field3);
    assert_ne!(field1, field4);
}

#[test]
fn test_read_unknown_tagged_field() {
    // Create a buffer with tagged field data: tag=3, size=5, data=[1,2,3,4,5]
    let mut accessor = ByteBufferAccessor::from_bytes(vec![1, 2, 3, 4, 5]);

    let unknowns = Vec::new();
    let unknowns = accessor.read_unknown_tagged_field(unknowns, 3, 5).unwrap();

    assert_eq!(unknowns.len(), 1);
    assert_eq!(unknowns[0].tag(), 3);
    assert_eq!(unknowns[0].data(), &[1, 2, 3, 4, 5]);
}

#[test]
fn test_multiple_unknown_tagged_fields() {
    let unknowns = Vec::new();

    // Add first tagged field
    let mut accessor1 = ByteBufferAccessor::from_bytes(vec![10, 20, 30]);
    let unknowns = accessor1.read_unknown_tagged_field(unknowns, 1, 3).unwrap();

    // Add second tagged field
    let mut accessor2 = ByteBufferAccessor::from_bytes(vec![40, 50]);
    let unknowns = accessor2.read_unknown_tagged_field(unknowns, 2, 2).unwrap();

    assert_eq!(unknowns.len(), 2);
    assert_eq!(unknowns[0].tag(), 1);
    assert_eq!(unknowns[0].data(), &[10, 20, 30]);
    assert_eq!(unknowns[1].tag(), 2);
    assert_eq!(unknowns[1].data(), &[40, 50]);
}

#[test]
fn test_flexible_string_encoding() {
    // In flexible versions, strings are encoded with unsigned varint length + 1
    // null string: varint(0)
    // empty string: varint(1)
    // "hello": varint(6) + "hello" bytes

    let mut accessor = ByteBufferAccessor::new(20);

    // Write flexible version string "hello"
    let s = "hello";
    accessor.write_unsigned_varint((s.len() + 1) as u32).unwrap();
    accessor.write_bytes(s.as_bytes()).unwrap();

    // Read it back
    let mut read_accessor = ByteBufferAccessor::from_bytes(accessor.buffer().to_vec());
    let length = read_accessor.read_unsigned_varint().unwrap();
    assert_eq!(length, 6);

    let string_data = read_accessor.read_array((length - 1) as usize).unwrap();
    let decoded = String::from_utf8(string_data).unwrap();
    assert_eq!(decoded, "hello");
}

#[test]
fn test_flexible_bytes_encoding() {
    // In flexible versions, bytes are encoded with unsigned varint length + 1
    // null bytes: varint(0)
    // empty bytes: varint(1)
    // [1,2,3]: varint(4) + [1,2,3]

    let mut accessor = ByteBufferAccessor::new(20);

    // Write flexible version bytes
    let data = vec![1u8, 2, 3];
    accessor.write_unsigned_varint((data.len() + 1) as u32).unwrap();
    accessor.write_bytes(&data).unwrap();

    // Read it back
    let mut read_accessor = ByteBufferAccessor::from_bytes(accessor.buffer().to_vec());
    let length = read_accessor.read_unsigned_varint().unwrap();
    assert_eq!(length, 4);

    let bytes_data = read_accessor.read_array((length - 1) as usize).unwrap();
    assert_eq!(bytes_data, vec![1u8, 2, 3]);
}

#[test]
fn test_flexible_array_encoding() {
    // In flexible versions, arrays are encoded with unsigned varint length + 1
    // empty array: varint(1)
    // [10, 20, 30]: varint(4) + elements

    let mut accessor = ByteBufferAccessor::new(20);

    // Write flexible version array of int32
    let array = vec![10i32, 20, 30];
    accessor.write_unsigned_varint((array.len() + 1) as u32).unwrap();
    for value in &array {
        accessor.write_int(*value).unwrap();
    }

    // Read it back
    let mut read_accessor = ByteBufferAccessor::from_bytes(accessor.buffer().to_vec());
    let length = read_accessor.read_unsigned_varint().unwrap();
    assert_eq!(length, 4);

    let mut result = Vec::new();
    for _ in 0..(length - 1) {
        result.push(read_accessor.read_int().unwrap());
    }
    assert_eq!(result, vec![10, 20, 30]);
}

#[test]
fn test_tagged_fields_serialization() {
    // Simulate a struct with tagged fields in flexible version
    // Format: num_tagged_fields (varint), then for each field:
    //   tag (varint), size (varint), data

    let mut accessor = ByteBufferAccessor::new(50);

    // Write 2 tagged fields
    accessor.write_unsigned_varint(2).unwrap(); // 2 tagged fields

    // Field with tag=0, data=[100, 101]
    accessor.write_unsigned_varint(0).unwrap(); // tag
    accessor.write_unsigned_varint(2).unwrap(); // size
    accessor.write_bytes(&[100, 101]).unwrap();

    // Field with tag=5, data=[200, 201, 202]
    accessor.write_unsigned_varint(5).unwrap(); // tag
    accessor.write_unsigned_varint(3).unwrap(); // size
    accessor.write_bytes(&[200, 201, 202]).unwrap();

    // Read it back
    let mut read_accessor = ByteBufferAccessor::from_bytes(accessor.buffer().to_vec());
    let num_fields = read_accessor.read_unsigned_varint().unwrap();
    assert_eq!(num_fields, 2);

    let mut unknowns = Vec::new();
    for _ in 0..num_fields {
        let tag = read_accessor.read_unsigned_varint().unwrap();
        let size = read_accessor.read_unsigned_varint().unwrap();
        unknowns = read_accessor.read_unknown_tagged_field(unknowns, tag, size).unwrap();
    }

    assert_eq!(unknowns.len(), 2);
    assert_eq!(unknowns[0].tag(), 0);
    assert_eq!(unknowns[0].data(), &[100, 101]);
    assert_eq!(unknowns[1].tag(), 5);
    assert_eq!(unknowns[1].data(), &[200, 201, 202]);
}
