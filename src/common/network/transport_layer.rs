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

//! Transport layer trait for underlying network communication.
//!
//! Translated from `org.apache.kafka.common.network.TransportLayer`.
//!
//! At a very basic level it is a wrapper around a TCP stream and can be used as a
//! substitute for socket channel and other network channel implementations.
//!
//! In Java this extends `ScatteringByteChannel` and `TransferableChannel`.
//! In Rust, the async read/write capabilities and the Kafka-specific methods are
//! combined into this single trait, backed by `tokio::net::TcpStream`.

use std::future::Future;
use std::io;
use std::ops;
use std::pin::Pin;

/// Interest operations for the transport layer, analogous to Java `SelectionKey` ops.
///
/// These flags control which I/O operations the selector is interested in for a channel.
/// Supports bitwise OR (`|`) to combine ops, bitwise AND (`&`) to intersect,
/// and `remove` to clear specific flags.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InterestOps(u8);

impl InterestOps {
    /// No interest operations.
    pub const NONE: InterestOps = InterestOps(0);
    /// Interested in read operations (analogous to `SelectionKey.OP_READ`).
    pub const OP_READ: InterestOps = InterestOps(1 << 0);
    /// Interested in write operations (analogous to `SelectionKey.OP_WRITE`).
    pub const OP_WRITE: InterestOps = InterestOps(1 << 2);
    /// Interested in connect operations (analogous to `SelectionKey.OP_CONNECT`).
    pub const OP_CONNECT: InterestOps = InterestOps(1 << 3);

    /// Returns `true` if the given ops are set.
    pub fn contains(self, other: InterestOps) -> bool {
        (self.0 & other.0) == other.0
    }

    /// Returns these ops with the given ops removed.
    pub fn remove(self, other: InterestOps) -> InterestOps {
        InterestOps(self.0 & !other.0)
    }

    /// Returns the raw u8 value.
    pub fn bits(self) -> u8 {
        self.0
    }
}

impl ops::BitOr for InterestOps {
    type Output = InterestOps;

    fn bitor(self, rhs: InterestOps) -> InterestOps {
        InterestOps(self.0 | rhs.0)
    }
}

impl ops::BitOrAssign for InterestOps {
    fn bitor_assign(&mut self, rhs: InterestOps) {
        self.0 |= rhs.0;
    }
}

impl ops::BitAnd for InterestOps {
    type Output = InterestOps;

    fn bitand(self, rhs: InterestOps) -> InterestOps {
        InterestOps(self.0 & rhs.0)
    }
}

/// Transport layer for underlying network communication.
///
/// Provides async read/write operations on a TCP connection along with
/// Kafka-specific connection management (handshake, interest ops, muting).
///
/// In Java, `TransportLayer` extends `ScatteringByteChannel` and `TransferableChannel`.
/// In Rust, the async I/O capabilities and the Kafka-specific methods are combined
/// into this single trait. I/O methods return boxed futures for object safety (`dyn TransportLayer`).
///
/// Per CLAUDE.md rule 8, all I/O is async using Tokio.
pub trait TransportLayer: Send {
    /// Returns `true` if the channel has completed handshake and authentication.
    fn ready(&self) -> bool;

    /// Finishes the process of connecting a socket channel.
    ///
    /// # Errors
    ///
    /// Returns an error if the connection cannot be completed.
    fn finish_connect(&mut self) -> Pin<Box<dyn Future<Output = io::Result<bool>> + Send + '_>>;

    /// Disconnects the underlying socket channel.
    fn disconnect(&mut self);

    /// Returns `true` if this channel's network socket is connected.
    fn is_connected(&self) -> bool;

    /// Performs protocol-specific handshake.
    ///
    /// This is a no-op for the non-secure PLAINTEXT implementation.
    /// For SSL, this would perform the SSL handshake.
    ///
    /// # Errors
    ///
    /// Returns an error if the handshake fails.
    fn handshake(&mut self) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send + '_>>;

    /// Adds the given interest operations.
    fn add_interest_ops(&mut self, ops: InterestOps);

    /// Removes the given interest operations.
    fn remove_interest_ops(&mut self, ops: InterestOps);

    /// Returns `true` if this channel is muted (not interested in read operations).
    fn is_mute(&self) -> bool;

    /// Returns `true` if the channel has bytes to be read in any intermediate buffers
    /// which may be processed without reading additional data from the network.
    fn has_bytes_buffered(&self) -> bool;

    /// Returns `true` if there are any pending writes that have not yet been flushed
    /// to the underlying transport.
    fn has_pending_writes(&self) -> bool;

    /// Returns `true` if the transport layer is open.
    fn is_open(&self) -> bool;

    /// Closes the transport layer.
    ///
    /// # Errors
    ///
    /// Returns an error if the close operation fails.
    fn close(&mut self) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send + '_>>;

    /// Reads data from this channel into the given buffer.
    ///
    /// # Arguments
    ///
    /// * `dst` - The buffer into which bytes are to be transferred
    ///
    /// # Returns
    ///
    /// The number of bytes read, possibly zero. Returns `Ok(0)` only to indicate
    /// EOF (remote closed the connection), consistent with Tokio's `AsyncRead`.
    ///
    /// # Errors
    ///
    /// Returns an error if the read fails.
    fn read<'a>(&'a mut self, dst: &'a mut [u8]) -> Pin<Box<dyn Future<Output = io::Result<usize>> + Send + 'a>>;

    /// Writes data to this channel from the given buffer.
    ///
    /// # Arguments
    ///
    /// * `src` - The buffer from which bytes are to be retrieved
    ///
    /// # Returns
    ///
    /// The number of bytes written, possibly zero.
    ///
    /// # Errors
    ///
    /// Returns an error if the write fails.
    fn write<'a>(&'a mut self, src: &'a [u8]) -> Pin<Box<dyn Future<Output = io::Result<usize>> + Send + 'a>>;

    /// Writes data from multiple buffers to this channel (scatter-gather write).
    ///
    /// # Arguments
    ///
    /// * `srcs` - The buffers from which bytes are to be retrieved
    ///
    /// # Returns
    ///
    /// The number of bytes written, possibly zero.
    ///
    /// # Errors
    ///
    /// Returns an error if the write fails.
    fn write_vectored<'a>(
        &'a mut self,
        srcs: &'a [io::IoSlice<'a>],
    ) -> Pin<Box<dyn Future<Output = io::Result<usize>> + Send + 'a>>;
}
