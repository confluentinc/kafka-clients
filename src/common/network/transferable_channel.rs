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

//! Translation of `org.apache.kafka.common.network.TransferableChannel`.

use std::io::{self, IoSlice};

/// Extends a gathering byte channel with the minimal set of methods
/// required by the [`super::Send`] interface. Supporting TLS and efficient
/// zero-copy transfers are the main reasons for the additional methods.
///
/// In Java this extends `GatheringByteChannel` and adds two methods:
///   * `boolean hasPendingWrites()`
///   * `long transferFrom(FileChannel, long, long)`
///
/// In Rust we keep the same shape: the gathering write is the
/// [`Self::write_vectored`] method (which mirrors `GatheringByteChannel.write(
/// ByteBuffer[])`), `has_pending_writes` mirrors `hasPendingWrites`, and
/// `transfer_from` mirrors `transferFrom`. `transfer_from` is broker-side
/// only — the producer never calls it; concrete client transports return
/// `Ok(0)` or panic if invoked. The trait keeps the method for parity with
/// the Java surface.
pub trait TransferableChannel {
    /// Write a sequence of buffers to the channel. Mirrors
    /// `GatheringByteChannel.write(ByteBuffer[] srcs)`. Returns the number
    /// of bytes written, which may be less than the total length of the
    /// buffers.
    fn write_vectored(&mut self, bufs: &[IoSlice<'_>]) -> io::Result<usize>;

    /// `true` iff there are any pending writes (e.g. buffered inside the
    /// SSL transport layer). `false` when the implementation directly
    /// writes all data to the underlying socket.
    fn has_pending_writes(&self) -> bool;

    /// Transfers `count` bytes from `file` (starting at `position`) into
    /// this channel. Mirrors Java's `transferFrom`. Broker-side only;
    /// producer-side implementations may return `Ok(0)`.
    ///
    /// Default implementation returns `Ok(0)` — the producer does not use
    /// zero-copy file transfer.
    fn transfer_from(&mut self, _file: &mut std::fs::File, _position: u64, _count: u64) -> io::Result<u64> {
        Ok(0)
    }
}
