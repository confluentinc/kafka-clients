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
//! This is a simplified version of the Java `SendBuilder` that handles only
//! the contiguous-buffer path (no zero-copy records). The Java version lives
//! in `org.apache.kafka.common.protocol.SendBuilder`.
//!
//! For requests/responses without record sets (i.e. everything except
//! ProduceRequest/FetchResponse), the header and body are serialized into a
//! single contiguous buffer with a 4-byte big-endian size prefix.

use std::io;

use crate::common::network::ByteBufferSend;
use crate::common::protocol::ByteBufferAccessor;
use crate::common::protocol::Message;
use crate::common::protocol::MessageSizeAccumulator;
use crate::common::protocol::ObjectSerializationCache;
use crate::common::protocol::Writable;

use super::RequestHeader;
use super::ResponseHeader;

/// Builds network `Send` objects from protocol messages.
///
/// Only the simple contiguous-buffer path is supported (no zero-copy records).
/// Serializes header + body into a single buffer with a 4-byte big-endian size prefix.
pub struct SendBuilder;

impl SendBuilder {
    /// Builds a `ByteBufferSend` for a request: size-prefixed header + body.
    ///
    /// # Errors
    ///
    /// Returns an error if size calculation or serialization fails.
    pub fn build_request_send(header: &RequestHeader, api_request: &impl Message) -> io::Result<ByteBufferSend> {
        Self::build_send(header.data(), header.header_version(), api_request, header.api_version())
    }

    /// Builds a `ByteBufferSend` for a response: size-prefixed header + body.
    ///
    /// # Errors
    ///
    /// Returns an error if size calculation or serialization fails.
    pub fn build_response_send(
        header: &ResponseHeader,
        api_response: &impl Message,
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
    ///
    /// This matches the Java `SendBuilder.buildSend` method for the contiguous-buffer path.
    ///
    /// # Errors
    ///
    /// Returns an error if size calculation or serialization fails.
    fn build_send(
        header: &impl Message,
        header_version: i16,
        api_message: &impl Message,
        api_version: i16,
    ) -> io::Result<ByteBufferSend> {
        let mut cache = ObjectSerializationCache::new();

        let mut message_size = MessageSizeAccumulator::new();
        header.add_size(&mut message_size, &mut cache, header_version)?;
        api_message.add_size(&mut message_size, &mut cache, api_version)?;

        let total_size = message_size.total_size();
        let buffer_size = (message_size.size_excluding_zero_copy() + 4) as usize;

        let mut buffer = ByteBufferAccessor::new(buffer_size);
        buffer.write_int(total_size)?;
        header.write(&mut buffer, &cache, header_version)?;
        api_message.write(&mut buffer, &cache, api_version)?;

        Ok(ByteBufferSend::new(vec![buffer.buffer().to_vec()]))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::network::KafkaSend;
    use crate::common::protocol::ApiKeys;
    use crate::metadata_request_data::MetadataRequestData;

    #[test]
    fn test_send_builder_creates_size_prefixed_buffer() {
        let header =
            RequestHeader::new(&ApiKeys::METADATA, ApiKeys::METADATA.latest_version(), "test-client", 42).unwrap();

        let body = MetadataRequestData::new();

        let send = SendBuilder::build_request_send(&header, &body).unwrap();
        // The send should have a positive size (4-byte prefix + header + body)
        assert!(send.size() > 4);
    }
}
