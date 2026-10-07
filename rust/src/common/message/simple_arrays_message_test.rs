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
//! Translated from org.apache.kafka.common.message.SimpleArraysMessageTest, which also
//! covers `SimpleKeyedArraysMessage` (`generator/test-messages/SimpleKeyedArraysMessage.json`).

use crate::common::protocol::{ByteBufferAccessor, MessageUtil};
use crate::test_generated::simple_arrays_message_data::SimpleArraysMessageData;
use crate::test_generated::simple_keyed_arrays_message_data::SimpleKeyedArraysMessageData;

#[test]
#[doc(alias = "org.apache.kafka.common.message.SimpleArraysMessageTest#testArrayBoundsChecking")]
fn test_array_bounds_checking() {
    // SimpleArraysMessageData takes 2 arrays
    let buf: Vec<u8> = vec![
        0x7f, // Set size of first array to 126 which is larger than the size of this buffer
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    ];
    let mut accessor = ByteBufferAccessor::new(buf);
    let result = SimpleArraysMessageData::read(&mut accessor, 2);
    assert!(result.is_err());
    assert_eq!(
        "Tried to allocate a collection of size 126, but there are only 7 bytes remaining.",
        result.unwrap_err().to_string()
    );
}

#[test]
#[doc(alias = "org.apache.kafka.common.message.SimpleArraysMessageTest#testArrayBoundsCheckingOtherArray")]
fn test_array_bounds_checking_other_array() {
    // SimpleArraysMessageData takes 2 arrays
    let buf: Vec<u8> = vec![
        0x01, // Set size of first array to 0
        0x7e, // Set size of second array to 125 which is larger than the size of this buffer
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    ];
    let mut accessor = ByteBufferAccessor::new(buf);
    let result = SimpleArraysMessageData::read(&mut accessor, 2);
    assert!(result.is_err());
    assert_eq!(
        "Tried to allocate a collection of size 125, but there are only 6 bytes remaining.",
        result.unwrap_err().to_string()
    );
}

#[test]
#[doc(
    alias = "org.apache.kafka.common.message.SimpleArraysMessageTest#testDeclaredLengthAboveInitialCapacityStillParses"
)]
fn test_declared_length_above_initial_capacity_still_parses() {
    let count: i32 = 1010;
    let mut buf: Vec<u8> = Vec::with_capacity(4 + (count as usize * 4));
    buf.extend_from_slice(&count.to_be_bytes());
    for i in 0..count {
        buf.extend_from_slice(&i.to_be_bytes());
    }
    let mut accessor = ByteBufferAccessor::new(buf);
    let out = SimpleArraysMessageData::read(&mut accessor, 0).unwrap();
    assert_eq!(count as usize, out.sheep().len());
    assert_eq!((0..count).collect::<Vec<_>>(), *out.sheep());
}

#[test]
#[doc(
    alias = "org.apache.kafka.common.message.SimpleArraysMessageTest#testKeyedDeclaredLengthAboveInitialCapacityStillParses"
)]
fn test_keyed_declared_length_above_initial_capacity_still_parses() {
    let count: i32 = 1010;
    let mut buf: Vec<u8> = Vec::with_capacity(4 + (count as usize * 8));
    buf.extend_from_slice(&count.to_be_bytes());
    for i in 0..count {
        buf.extend_from_slice(&i.to_be_bytes());
        buf.extend_from_slice(&(i * 2).to_be_bytes());
    }
    let mut accessor = ByteBufferAccessor::new(buf);
    let out = SimpleKeyedArraysMessageData::read(&mut accessor, 0).unwrap();
    assert_eq!(count as usize, out.keyed_structs().len());
}

#[test]
#[doc(alias = "org.apache.kafka.common.message.SimpleArraysMessageTest#testArrayLengthAboveMaxIsRejected")]
fn test_array_length_above_max_is_rejected() {
    let count = MessageUtil::MAX_ARRAY_LENGTH + 1;
    let mut buf = vec![0u8; 4 + count as usize];
    buf[..4].copy_from_slice(&count.to_be_bytes());
    let mut accessor = ByteBufferAccessor::new(buf);
    let result = SimpleArraysMessageData::read(&mut accessor, 0);
    assert_eq!(
        format!(
            "Tried to read a collection of size {count}, which exceeds the maximum allowed size of {}.",
            MessageUtil::MAX_ARRAY_LENGTH
        ),
        result.unwrap_err().to_string()
    );
}

#[test]
#[doc(alias = "org.apache.kafka.common.message.SimpleArraysMessageTest#testKeyedArrayLengthAboveMaxIsRejected")]
fn test_keyed_array_length_above_max_is_rejected() {
    let count = MessageUtil::MAX_ARRAY_LENGTH + 1;
    let mut buf = vec![0u8; 4 + count as usize];
    buf[..4].copy_from_slice(&count.to_be_bytes());
    let mut accessor = ByteBufferAccessor::new(buf);
    let result = SimpleKeyedArraysMessageData::read(&mut accessor, 0);
    assert_eq!(
        format!(
            "Tried to read a collection of size {count}, which exceeds the maximum allowed size of {}.",
            MessageUtil::MAX_ARRAY_LENGTH
        ),
        result.unwrap_err().to_string()
    );
}
