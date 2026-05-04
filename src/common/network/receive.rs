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

//! Translation of `org.apache.kafka.common.network.Receive`.

use std::io;

/// Models the in-progress reading of data from a channel from a source
/// identified by a string id.
///
/// Mirrors the Java interface `org.apache.kafka.common.network.Receive`.
/// The Java interface extends `Closeable`; in Rust the corresponding
/// resource cleanup is achieved by `Drop`. We expose `close` as an
/// explicit method to mirror the Java surface for callers that want to
/// release resources eagerly.
pub trait Receive {
    /// The id of the source from which we are receiving data.
    /// Mirrors `Receive.source()`.
    fn source(&self) -> &str;

    /// Are we done receiving data? Mirrors `Receive.complete()`.
    fn complete(&self) -> bool;

    /// Read bytes into this receive from the given byte source. Returns
    /// the number of bytes read. Mirrors
    /// `Receive.readFrom(ScatteringByteChannel)`.
    ///
    /// In Java the channel is a `ScatteringByteChannel` (NIO). In Rust we
    /// use a `&mut dyn io::Read` so the same code can be exercised with a
    /// `Cursor<Vec<u8>>` in tests and with the Phase 5b/5c Tokio
    /// non-blocking reader in production. The non-blocking reader is
    /// expected to be wrapped in a `tokio::io::ReadBuf` adapter exposing
    /// `io::Read` semantics.
    fn read_from(&mut self, src: &mut dyn io::Read) -> io::Result<u64>;

    /// Do we know yet how much memory we require to fully read this?
    /// Mirrors `Receive.requiredMemoryAmountKnown()`.
    fn required_memory_amount_known(&self) -> bool;

    /// Has the underlying memory required to complete reading been
    /// allocated yet? Mirrors `Receive.memoryAllocated()`.
    fn memory_allocated(&self) -> bool;

    /// Release any resources held by this receive. Mirrors
    /// `Receive.close()` (inherited from `Closeable`). The default
    /// implementation is a no-op; concrete impls override if they hold
    /// pooled memory.
    fn close(&mut self) -> io::Result<()> {
        Ok(())
    }
}
