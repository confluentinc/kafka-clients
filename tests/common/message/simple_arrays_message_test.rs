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

//! Integration tests for SimpleArraysMessageData.
//!
//! Translated from org.apache.kafka.common.message.SimpleArraysMessageTest

use crate::common::simple_arrays_message_data::SimpleArraysMessageData;
use confluent_kafka_rust::common::protocol::ByteBufferAccessor;

#[test]
fn test_array_bounds_checking() {
    // SimpleArraysMessageData takes 2 arrays
    let buf: Vec<u8> = vec![
        0x7f, // Set size of first array to 126 which is larger than the size of this buffer
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    ];
    let mut accessor = ByteBufferAccessor::from_bytes(buf);
    let result = SimpleArraysMessageData::read(&mut accessor, 2);
    assert!(result.is_err());
    assert_eq!(
        "Tried to allocate a collection of size 126, but there are only 7 bytes remaining.",
        result.unwrap_err().to_string()
    );
}

#[test]
fn test_array_bounds_checking_other_array() {
    // SimpleArraysMessageData takes 2 arrays
    let buf: Vec<u8> = vec![
        0x01, // Set size of first array to 0
        0x7e, // Set size of second array to 125 which is larger than the size of this buffer
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    ];
    let mut accessor = ByteBufferAccessor::from_bytes(buf);
    let result = SimpleArraysMessageData::read(&mut accessor, 2);
    assert!(result.is_err());
    assert_eq!(
        "Tried to allocate a collection of size 125, but there are only 6 bytes remaining.",
        result.unwrap_err().to_string()
    );
}
