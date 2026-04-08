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

//! A size-delimited receive that consists of a 4-byte network-ordered size N followed by
//! N bytes of content.
//!
//! Translated from `org.apache.kafka.common.network.NetworkReceive`.

use super::invalid_receive_exception::InvalidReceiveException;
use super::receive::Receive;

use log::trace;

use std::io;

/// Source identifier used when the source is unknown.
pub const UNKNOWN_SOURCE: &str = "";

/// Value indicating no maximum size limit for receives.
pub const UNLIMITED: i32 = -1;

/// Size of the header that precedes each message (4 bytes for the i32 size).
const SIZE_LENGTH: usize = 4;

/// A size-delimited receive that consists of a 4-byte network-ordered size N followed by
/// N bytes of content.
///
/// # Wire Format
///
/// ```text
/// [4 bytes: size (big-endian i32)] [N bytes: payload]
/// ```
///
/// The receive proceeds in two phases:
/// 1. Read the 4-byte size header
/// 2. Allocate and read N bytes of payload
pub struct NetworkReceive {
    /// The source identifier for this receive.
    source: String,
    /// Buffer for reading the 4-byte size header.
    size_buf: [u8; SIZE_LENGTH],
    /// Number of bytes read into the size buffer so far.
    size_bytes_read: usize,
    /// Maximum allowed receive size. `UNLIMITED` (-1) means no limit.
    max_size: i32,
    /// The requested buffer size, or -1 if not yet known.
    requested_buffer_size: i32,
    /// The payload buffer, allocated once the size is known.
    /// `None` if not yet allocated.
    buffer: Option<Vec<u8>>,
    /// Number of bytes read into the payload buffer so far.
    buffer_bytes_read: usize,
}

impl NetworkReceive {
    /// Creates a new `NetworkReceive` with the given source and a pre-existing payload buffer.
    ///
    /// The size header is considered already read and the payload buffer is provided directly.
    /// This constructor is used when the buffer contents are already known (e.g., in tests).
    pub fn with_buffer(source: &str, buffer: Vec<u8>) -> Self {
        // When a buffer is provided, we treat the size header as fully read
        // and set the payload position to the buffer's capacity (matching Java behavior
        // where buffer.remaining() == 0 for a fully-positioned buffer).
        let buffer_len = buffer.len();
        Self {
            source: source.to_string(),
            size_buf: [0; SIZE_LENGTH],
            size_bytes_read: SIZE_LENGTH,
            max_size: UNLIMITED,
            requested_buffer_size: buffer_len as i32,
            buffer: Some(buffer),
            buffer_bytes_read: buffer_len,
        }
    }

    /// Creates a new `NetworkReceive` with the given source and no size limit.
    pub fn with_source(source: &str) -> Self {
        Self::with_max_size(UNLIMITED, source)
    }

    /// Creates a new `NetworkReceive` with the given maximum size and source.
    pub fn with_max_size(max_size: i32, source: &str) -> Self {
        Self {
            source: source.to_string(),
            size_buf: [0; SIZE_LENGTH],
            size_bytes_read: 0,
            max_size,
            requested_buffer_size: -1,
            buffer: None,
            buffer_bytes_read: 0,
        }
    }

    /// Creates a new `NetworkReceive` with unknown source and no size limit.
    pub fn new() -> Self {
        Self::with_source(UNKNOWN_SOURCE)
    }

    /// Returns the payload buffer, or `None` if it has not been allocated yet.
    pub fn payload(&self) -> Option<&[u8]> {
        self.buffer.as_deref()
    }

    /// Returns the number of bytes read so far (both size header and payload).
    pub fn bytes_read(&self) -> usize {
        if self.buffer.is_none() {
            self.size_bytes_read
        } else {
            self.buffer_bytes_read + self.size_bytes_read
        }
    }

    /// Returns the total size of the receive including payload and size buffer,
    /// for use in metrics. This is consistent with `NetworkSend::size()`.
    ///
    /// # Panics
    ///
    /// Panics if the payload buffer has not been allocated yet.
    pub fn size(&self) -> usize {
        self.buffer.as_ref().expect("payload buffer not yet allocated").len() + SIZE_LENGTH
    }
}

impl Default for NetworkReceive {
    fn default() -> Self {
        Self::new()
    }
}

impl Receive for NetworkReceive {
    fn source(&self) -> &str {
        &self.source
    }

    fn complete(&self) -> bool {
        self.size_bytes_read == SIZE_LENGTH
            && self.buffer.is_some()
            && self.buffer_bytes_read == self.buffer.as_ref().map_or(0, |b| b.len())
    }

    fn read_from(&mut self, channel: &mut dyn io::Read) -> io::Result<usize> {
        let mut total_read = 0;

        // Phase 1: Read the 4-byte size header
        if self.size_bytes_read < SIZE_LENGTH {
            let bytes_read = channel.read(&mut self.size_buf[self.size_bytes_read..SIZE_LENGTH])?;
            if bytes_read == 0 {
                // No data available — return the total bytes read so far.
                // The caller will call read_from again when more data is available.
                return Ok(total_read);
            }
            total_read += bytes_read;
            self.size_bytes_read += bytes_read;

            if self.size_bytes_read == SIZE_LENGTH {
                let receive_size = i32::from_be_bytes(self.size_buf);
                if receive_size < 0 {
                    return Err(InvalidReceiveException::new(format!("Invalid receive (size = {receive_size})")).into());
                }
                if self.max_size != UNLIMITED && receive_size > self.max_size {
                    return Err(InvalidReceiveException::new(format!(
                        "Invalid receive (size = {receive_size} larger than {max_size})",
                        max_size = self.max_size,
                    ))
                    .into());
                }
                self.requested_buffer_size = receive_size;
                if receive_size == 0 {
                    self.buffer = Some(Vec::new());
                }
            }
        }

        // Phase 2: Allocate buffer if size is known but not yet allocated
        if self.buffer.is_none() && self.requested_buffer_size != -1 {
            // Simple allocation (no memory pool for now)
            self.buffer = Some(vec![0u8; self.requested_buffer_size as usize]);
            self.buffer_bytes_read = 0;
            trace!(
                "Allocated buffer of size {} for source {}",
                self.requested_buffer_size, self.source
            );
        }

        // Phase 3: Read payload data
        if let Some(ref mut buf) = self.buffer
            && self.buffer_bytes_read < buf.len()
        {
            let bytes_read = channel.read(&mut buf[self.buffer_bytes_read..])?;
            // In Java NIO, read returning 0 on a non-blocking channel does not mean EOF.
            // We mirror that behavior: if no bytes are available, we simply return
            // the total bytes read so far. The caller will call read_from again when
            // more data is available.
            total_read += bytes_read;
            self.buffer_bytes_read += bytes_read;
        }

        Ok(total_read)
    }

    fn required_memory_amount_known(&self) -> bool {
        self.requested_buffer_size != -1
    }

    fn memory_allocated(&self) -> bool {
        self.buffer.is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::io::Cursor;

    /// Translated from `NetworkReceiveTest.testBytesRead` in
    /// `org.apache.kafka.common.network.NetworkReceiveTest`.
    #[test]
    fn test_bytes_read() {
        let mut receive = NetworkReceive::with_max_size(128, "0");
        assert_eq!(0, receive.bytes_read());

        // Simulate channel that returns a 4-byte size header indicating 128 bytes of payload
        let mut size_data = Cursor::new(128_i32.to_be_bytes().to_vec());

        let read = receive.read_from(&mut size_data).unwrap();
        assert_eq!(4, read);
        assert_eq!(4, receive.bytes_read());
        assert!(!receive.complete());

        // Simulate reading 64 bytes of payload
        let payload_part1 = vec![0xABu8; 64];
        let mut channel1 = Cursor::new(payload_part1);

        let read = receive.read_from(&mut channel1).unwrap();
        assert_eq!(64, read);
        assert_eq!(68, receive.bytes_read());
        assert!(!receive.complete());

        // Simulate reading the remaining 64 bytes of payload
        let payload_part2 = vec![0xCDu8; 64];
        let mut channel2 = Cursor::new(payload_part2);

        let read = receive.read_from(&mut channel2).unwrap();
        assert_eq!(64, read);
        assert_eq!(132, receive.bytes_read());
        assert!(receive.complete());
    }

    /// Translated from `NetworkReceiveTest.testRequiredMemoryAmountKnownWhenNotSet` in
    /// `org.apache.kafka.common.network.NetworkReceiveTest`.
    #[test]
    fn test_required_memory_amount_known_when_not_set() {
        let receive = NetworkReceive::with_source("0");
        assert!(
            !receive.required_memory_amount_known(),
            "Memory amount should not be known before read."
        );
    }

    /// Translated from `NetworkReceiveTest.testRequiredMemoryAmountKnownWhenSet` in
    /// `org.apache.kafka.common.network.NetworkReceiveTest`.
    #[test]
    fn test_required_memory_amount_known_when_set() {
        let mut receive = NetworkReceive::with_max_size(128, "0");

        // Channel provides size header indicating 64 bytes
        let mut channel = Cursor::new(64_i32.to_be_bytes().to_vec());

        receive.read_from(&mut channel).unwrap();
        assert!(
            receive.required_memory_amount_known(),
            "Memory amount should be known after read."
        );
    }

    /// Translated from `NetworkReceiveTest.testSizeWithPredefineBuffer` in
    /// `org.apache.kafka.common.network.NetworkReceiveTest`.
    #[test]
    fn test_size_with_predefined_buffer() {
        let payload_size = 8;
        let expected_total_size = 4 + payload_size; // 4 bytes for size buffer + payload size

        // Create a payload buffer with sequential byte values
        let payload_buffer: Vec<u8> = (0..payload_size as u8).collect();

        let network_receive = NetworkReceive::with_buffer("0", payload_buffer);
        assert_eq!(
            expected_total_size,
            network_receive.size(),
            "The total size should be the sum of the size buffer and payload."
        );
    }

    /// Translated from `NetworkReceiveTest.testSizeAfterRead` in
    /// `org.apache.kafka.common.network.NetworkReceiveTest`.
    #[test]
    fn test_size_after_read() {
        let payload_size: i32 = 32;
        let expected_total_size = 4 + payload_size as usize; // 4 bytes for size buffer + payload size
        let mut receive = NetworkReceive::with_max_size(128, "0");

        // Channel provides size header
        let mut channel = Cursor::new(payload_size.to_be_bytes().to_vec());

        receive.read_from(&mut channel).unwrap();
        assert_eq!(
            expected_total_size,
            receive.size(),
            "The total size should be the sum of the size buffer and receive size."
        );
    }

    /// Test that negative size in header is rejected.
    #[test]
    fn test_invalid_negative_size() {
        let mut receive = NetworkReceive::with_max_size(128, "0");
        let mut channel = Cursor::new((-1_i32).to_be_bytes().to_vec());

        let result = receive.read_from(&mut channel);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(io::ErrorKind::InvalidData, err.kind());
    }

    /// Test that size exceeding max is rejected.
    #[test]
    fn test_invalid_size_exceeding_max() {
        let mut receive = NetworkReceive::with_max_size(64, "0");
        let mut channel = Cursor::new(128_i32.to_be_bytes().to_vec());

        let result = receive.read_from(&mut channel);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(io::ErrorKind::InvalidData, err.kind());
    }

    /// Test zero-size payload (used by SASL).
    #[test]
    fn test_zero_size_payload() {
        let mut receive = NetworkReceive::with_max_size(128, "0");
        let mut channel = Cursor::new(0_i32.to_be_bytes().to_vec());

        let read = receive.read_from(&mut channel).unwrap();
        assert_eq!(4, read);
        assert!(receive.complete());
        assert_eq!(Some(&[][..]), receive.payload());
    }

    /// Test default constructor.
    #[test]
    fn test_default() {
        let receive = NetworkReceive::new();
        assert_eq!(UNKNOWN_SOURCE, receive.source());
        assert!(!receive.complete());
        assert!(!receive.required_memory_amount_known());
        assert!(!receive.memory_allocated());
    }
}
