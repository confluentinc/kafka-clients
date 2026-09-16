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

//! Builds a [`ByteBufferSend`] from header + body for network transmission.
//!
//! Translated from `org.apache.kafka.common.protocol.SendBuilder` in Java.
//!
//! Supports zero-copy records via scatter-gather I/O: record bytes are moved
//! into separate buffers instead of being copied into the main serialization
//! buffer. The resulting [`ByteBufferSend`] uses vectored writes to send all
//! buffers efficiently in a single system call.

use std::io;

use crate::common::Uuid;
use crate::common::Writable;
use crate::common::network::ByteBufferSend;
use crate::common::protocol::ByteUtils;
use crate::common::protocol::Message;
use crate::common::protocol::MessageSizeAccumulator;
use crate::common::protocol::ObjectSerializationCache;

use super::RequestHeader;
use super::ResponseHeader;

/// Builds network `Send` objects from protocol messages.
///
/// Serializes header + body into a size-prefixed buffer, with zero-copy
/// support for record fields via scatter-gather I/O.
pub struct SendBuilder;

impl SendBuilder {
    /// Builds a `ByteBufferSend` for a request: size-prefixed header + body.
    ///
    /// # Errors
    ///
    /// Returns an error if size calculation or serialization fails.
    pub fn build_request_send(header: &RequestHeader, api_request: &mut impl Message) -> io::Result<ByteBufferSend> {
        Self::build_send(header.data(), header.header_version(), api_request, header.api_version())
    }

    /// Builds a `ByteBufferSend` for a response: size-prefixed header + body.
    ///
    /// # Errors
    ///
    /// Returns an error if size calculation or serialization fails.
    pub fn build_response_send(
        header: &ResponseHeader,
        api_response: &mut impl Message,
        api_version: i16,
    ) -> io::Result<ByteBufferSend> {
        Self::build_send(header.data(), header.header_version(), api_response, api_version)
    }

    /// Builds a size-prefixed `ByteBufferSend` from a header message and an API message.
    ///
    /// The output format is:
    /// ```text
    /// [4-byte big-endian total_size][header bytes][api message bytes]
    /// ```
    /// Record fields are split into separate buffers for zero-copy scatter-gather I/O.
    ///
    /// # Errors
    ///
    /// Returns an error if size calculation or serialization fails.
    fn build_send(
        header: &impl Message,
        header_version: i16,
        api_message: &mut impl Message,
        api_version: i16,
    ) -> io::Result<ByteBufferSend> {
        let mut cache = ObjectSerializationCache::new();

        let mut message_size = MessageSizeAccumulator::new();
        header.add_size(&mut message_size, &mut cache, header_version)?;
        api_message.add_size(&mut message_size, &mut cache, api_version)?;

        let total_size = message_size.total_size();
        let buffer_size = (message_size.size_excluding_zero_copy() + 4) as usize;

        let mut writable = SendBuilderWritable::new(buffer_size);
        writable.write_int(total_size)?;
        // Header never has records fields, so &mut is unused here but harmless.
        // We write through the trait method which requires &mut self on Message.
        let mut header_clone = header.clone();
        header_clone.write(&mut writable, &cache, header_version)?;
        api_message.write(&mut writable, &cache, api_version)?;

        Ok(ByteBufferSend::new(writable.into_buffers()))
    }
}

/// A [`Writable`] that supports scatter-gather I/O for zero-copy records.
///
/// Regular writes accumulate into a main buffer. When [`write_records`] is called,
/// the current main buffer segment is flushed and the record bytes are stored as
/// a separate buffer, matching Java's `SendBuilder` behavior.
struct SendBuilderWritable {
    current_buffer: Vec<u8>,
    completed_buffers: Vec<bytes::Bytes>,
}

impl SendBuilderWritable {
    fn new(capacity: usize) -> Self {
        Self { current_buffer: Vec::with_capacity(capacity), completed_buffers: Vec::new() }
    }

    fn into_buffers(mut self) -> Vec<bytes::Bytes> {
        if !self.current_buffer.is_empty() {
            // `Bytes::from(Vec<u8>)` adopts the allocation — no copy.
            self.completed_buffers.push(bytes::Bytes::from(self.current_buffer));
        }
        self.completed_buffers
    }
}

impl Writable for SendBuilderWritable {
    fn write_byte(&mut self, val: i8) -> io::Result<()> {
        self.current_buffer.push(val as u8);
        Ok(())
    }

    fn write_short(&mut self, val: i16) -> io::Result<()> {
        self.current_buffer.extend_from_slice(&val.to_be_bytes());
        Ok(())
    }

    fn write_int(&mut self, val: i32) -> io::Result<()> {
        self.current_buffer.extend_from_slice(&val.to_be_bytes());
        Ok(())
    }

    fn write_long(&mut self, val: i64) -> io::Result<()> {
        self.current_buffer.extend_from_slice(&val.to_be_bytes());
        Ok(())
    }

    fn write_double(&mut self, val: f64) -> io::Result<()> {
        self.current_buffer.extend_from_slice(&val.to_be_bytes());
        Ok(())
    }

    fn write_byte_array(&mut self, arr: &[u8]) -> io::Result<()> {
        self.current_buffer.extend_from_slice(arr);
        Ok(())
    }

    fn write_unsigned_varint(&mut self, val: u32) -> io::Result<()> {
        ByteUtils::write_unsigned_varint(val, &mut self.current_buffer)
    }

    fn write_varint(&mut self, val: i32) -> io::Result<()> {
        ByteUtils::write_varint(val, &mut self.current_buffer)
    }

    fn write_varlong(&mut self, val: i64) -> io::Result<()> {
        ByteUtils::write_varlong(val, &mut self.current_buffer)
    }

    fn write_uuid(&mut self, uuid: &Uuid) -> io::Result<()> {
        self.write_long(uuid.most_sig_bits() as i64)?;
        self.write_long(uuid.least_sig_bits() as i64)?;
        Ok(())
    }

    fn write_records(&mut self, data: bytes::Bytes) -> io::Result<()> {
        if !self.current_buffer.is_empty() {
            // `Bytes::from(Vec<u8>)` adopts the allocation — no copy.
            let flushed = bytes::Bytes::from(std::mem::take(&mut self.current_buffer));
            self.completed_buffers.push(flushed);
        }
        self.completed_buffers.push(data);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::MetadataRequestData;
    use crate::common::ApiKeys;
    use crate::common::network::KafkaSend;
    use crate::common::requests::RequestHeaderOptionsBuilder;

    #[test]
    fn test_send_builder_creates_size_prefixed_buffer() {
        let header = RequestHeader::new_options(
            RequestHeaderOptionsBuilder::new()
                .set_request_api_key(&ApiKeys::METADATA)
                .set_request_version(ApiKeys::METADATA.latest_version())
                .set_client_id("test-client")
                .set_correlation_id(42)
                .build()
                .unwrap(),
        )
        .unwrap();

        let mut body = MetadataRequestData::new();

        let send = SendBuilder::build_request_send(&header, &mut body).unwrap();
        // The send should have a positive size (4-byte prefix + header + body)
        assert!(send.size() > 4);
    }
}
