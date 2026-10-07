// Copyright 2026 Confluent Inc.
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

//! An output stream backed by a list of fixed-size chunks (KIP-1332).
//!
//! Translated from `org.apache.kafka.clients.producer.internals.ChunkedByteBufferOutputStream`
//! (Apache Kafka 4.4, KAFKA-20578).

use std::io;
use std::sync::Arc;

use bytes::Bytes;

use crate::common::Error;
use crate::producer::internals::BufferPool;

/// A `ByteBufferOutputStream` backed by a list of fixed-size chunks instead of a single
/// re-allocated buffer. Chunks are supplied by the caller (initial chunks via the constructor,
/// additional chunks via [`add_buffers`](Self::add_buffers)).
///
/// Current/temporary behavior (as in Java):
///
/// - The stream does not grow on its own: a write whose size exceeds the remaining free bytes
///   across all attached chunks fails with an `IllegalState` error, so the caller must attach
///   enough chunks before any such write. Automatic mid-write growth for compression support is
///   Java's KAFKA-20579.
/// - [`buffer`](Self::buffer) returns the written bytes as a single contiguous buffer, flattening
///   all chunks into a new buffer with an extra copy. Removing that copy (scatter-gather send) is
///   Java's KAFKA-20580 and is deliberately not done here.
///
/// # Rust representation
///
/// - **A chunk can never grow.** Java's chunks are fixed-capacity `ByteBuffer`s, which throw on
///   overflow. A Rust `Vec<u8>` would instead silently reallocate, breaking the pool's memory
///   accounting, so every chunk is held as a `Box<[u8]>`: a boxed slice has no `push` / `extend`
///   and its length is fixed for its whole life. Chunks enter as the pool's `Vec<u8>`s (whose
///   length equals their capacity, so the conversion does not reallocate) and leave as `Vec<u8>`s
///   again when they are returned to the pool.
/// - **One write position instead of one per chunk.** Java keeps a position in every
///   `ByteBuffer`. Writes only ever advance past a chunk once it is full, and
///   [`set_position`](Self::set_position) fills every chunk before the one it stops in, so every
///   chunk before the current one is always full and every chunk after it is always empty. The
///   per-chunk positions are therefore fully described by the current chunk index and the
///   position inside it, which is what this struct stores.
/// - **The flattened buffer is a [`Bytes`].** Java caches the flattened `ByteBuffer` so repeated
///   `buffer()` calls return the same instance, and `MemoryRecordsBuilder` writes the batch header
///   straight into it. Here the cache is a refcounted `Bytes`, so the builder can hand the
///   finished batch to `MemoryRecords` without a second copy (CLAUDE.md §14): the flatten is the
///   single finalisation copy, exactly as `MemoryRecordsBuilder::take_batch_data` is on the
///   single-buffer path. In-place header writes go through
///   [`rewrite_buffer`](Self::rewrite_buffer).
/// - **Dropping the stream returns its chunks.** Java relies on an explicit
///   [`deallocate`](Self::deallocate); a Rust future can be dropped at any `.await` (CLAUDE.md
///   §11.6), so `Drop` performs the same deallocation if it has not happened yet. After an
///   explicit `deallocate` the stream holds no chunks and the drop is a no-op.
#[doc(alias = "org.apache.kafka.clients.producer.internals.ChunkedByteBufferOutputStream")]
pub(crate) struct ChunkedByteBufferOutputStream {
    chunks: Vec<Box<[u8]>>,
    chunk_size: usize,
    /// The pool the chunks are returned to. `None` only in tests, as Java's `pool` may be null.
    pool: Option<Arc<BufferPool>>,
    /// Java's `currentChunkIndex`; `None` once deallocated (Java sets `currentChunk = null` and
    /// `currentChunkIndex = -1`).
    current_chunk_index: Option<usize>,
    /// The write position inside `chunks[current_chunk_index]` (Java's `currentChunk.position()`).
    current_chunk_position: usize,
    /// Set once the stream is closed for appends via [`close`](Self::close); no further writes or
    /// [`add_buffers`](Self::add_buffers) are allowed.
    closed: bool,
    /// Single-buffer view produced by `flatten()` and cached here so repeat
    /// [`buffer`](Self::buffer) calls return the same instance. To be removed once scatter-gather
    /// (KAFKA-20580) is implemented.
    flattened_buffer: Option<Bytes>,
}

#[cfg_attr(
    not(test),
    expect(dead_code, reason = "wired into the producer by Milestone 16 Phase 8")
)]
impl ChunkedByteBufferOutputStream {
    /// Constructs a chunked output stream backed by the given pre-allocated chunks. Ownership of
    /// `initial_chunks` transfers to this stream (they will be returned to the pool via
    /// [`deallocate`](Self::deallocate)).
    ///
    /// # Arguments
    ///
    /// * `initial_chunks` - pre-allocated chunks. Must be non-empty and each chunk's capacity must
    ///   equal `chunk_size`
    /// * `chunk_size` - the size of each chunk in bytes
    /// * `pool` - the buffer pool used for deallocation
    ///
    /// # Errors
    ///
    /// Returns [`Error::LocalIllegalArgument`] if `initial_chunks` is empty or a chunk's capacity
    /// differs from `chunk_size`. Java's `null` list has no Rust counterpart. On error the chunks
    /// are dropped: every chunk the pool hands out has the pool's chunk size, so this is reachable
    /// only with buffers the pool did not allocate.
    #[doc(
        alias = "org.apache.kafka.clients.producer.internals.ChunkedByteBufferOutputStream#ChunkedByteBufferOutputStream"
    )]
    pub(crate) fn new(
        initial_chunks: Vec<Vec<u8>>,
        chunk_size: usize,
        pool: Option<Arc<BufferPool>>,
    ) -> Result<Self, Error> {
        Self::validated_first_chunk(&initial_chunks, chunk_size)?;
        let chunks = initial_chunks.into_iter().map(Self::into_fixed_chunk).collect();
        Ok(Self {
            chunks,
            chunk_size,
            pool,
            current_chunk_index: Some(0),
            current_chunk_position: 0,
            closed: false,
            flattened_buffer: None,
        })
    }

    /// Validates the chunk contract: `initial_chunks` non-empty, each chunk's capacity equal to
    /// `chunk_size`. Java returns the first chunk for its `super(...)` call; Rust has no base
    /// class to hand it to, so this only validates.
    #[doc(alias = "org.apache.kafka.clients.producer.internals.ChunkedByteBufferOutputStream#validatedFirstChunk")]
    fn validated_first_chunk(initial_chunks: &[Vec<u8>], chunk_size: usize) -> Result<(), Error> {
        if initial_chunks.is_empty() {
            return Err(Error::local_illegal_argument("initialChunks must be non-empty"));
        }
        Self::validate_chunk_capacities(initial_chunks, chunk_size)
    }

    /// Validates that every chunk's capacity equals `chunk_size`, which the stream's capacity
    /// bookkeeping relies on.
    #[doc(alias = "org.apache.kafka.clients.producer.internals.ChunkedByteBufferOutputStream#validateChunkCapacities")]
    fn validate_chunk_capacities(chunks: &[Vec<u8>], chunk_size: usize) -> Result<(), Error> {
        for chunk in chunks {
            if chunk.capacity() != chunk_size {
                return Err(Error::local_illegal_argument(format!(
                    "each chunk must have capacity {}, but found a chunk of capacity {}",
                    chunk_size,
                    chunk.capacity()
                )));
            }
        }
        Ok(())
    }

    /// Turns a validated pool chunk into a fixed-length one. A `ByteBuffer`'s capacity is all
    /// writable, so the length is first raised to the capacity (a no-op for the pool's chunks,
    /// whose length already equals their capacity); `into_boxed_slice` then has no excess
    /// capacity to shed and does not reallocate.
    fn into_fixed_chunk(mut chunk: Vec<u8>) -> Box<[u8]> {
        let capacity = chunk.capacity();
        chunk.resize(capacity, 0);
        chunk.into_boxed_slice()
    }

    /// Writes one byte. Java's `write(int)`.
    ///
    /// # Errors
    ///
    /// `IllegalState` after [`deallocate`](Self::deallocate) or [`close`](Self::close), or when
    /// every attached chunk is full.
    #[doc(alias = "org.apache.kafka.clients.producer.internals.ChunkedByteBufferOutputStream#write")]
    pub(crate) fn write_byte(&mut self, b: u8) -> Result<(), Error> {
        self.ensure_not_deallocated()?;
        self.ensure_writable()?;
        let index = self.advance_while_current_chunk_full()?;
        self.chunks[index][self.current_chunk_position] = b;
        self.current_chunk_position += 1;
        Ok(())
    }

    /// Writes all of `bytes`, spilling across chunks. Java's `write(byte[], int, int)` (the slice
    /// carries the offset and length) and `write(ByteBuffer)`, which Rust does not need separately.
    ///
    /// Like Java, a write that runs out of chunk capacity part-way fails after writing the bytes
    /// that fit.
    ///
    /// # Errors
    ///
    /// `IllegalState` after [`deallocate`](Self::deallocate) or [`close`](Self::close), or when
    /// the write exceeds the remaining capacity across the attached chunks.
    #[doc(alias = "org.apache.kafka.clients.producer.internals.ChunkedByteBufferOutputStream#write")]
    pub(crate) fn write_bytes(&mut self, mut bytes: &[u8]) -> Result<(), Error> {
        self.ensure_not_deallocated()?;
        self.ensure_writable()?;
        while !bytes.is_empty() {
            let index = self.advance_while_current_chunk_full()?;
            let to_write = bytes.len().min(self.chunk_size - self.current_chunk_position);
            let start = self.current_chunk_position;
            self.chunks[index][start..start + to_write].copy_from_slice(&bytes[..to_write]);
            self.current_chunk_position += to_write;
            bytes = &bytes[to_write..];
        }
        Ok(())
    }

    /// Guards against writes (and [`add_buffers`](Self::add_buffers)) after the stream has been
    /// closed for appends via [`close`](Self::close).
    #[doc(alias = "org.apache.kafka.clients.producer.internals.ChunkedByteBufferOutputStream#ensureWritable")]
    fn ensure_writable(&self) -> Result<(), Error> {
        if self.closed {
            return Err(Error::local_illegal_state("cannot write after the stream has been closed"));
        }
        Ok(())
    }

    /// Guards against any use after [`deallocate`](Self::deallocate) has returned the chunks.
    /// Returns the current chunk index, which only exists while the stream is allocated.
    #[doc(alias = "org.apache.kafka.clients.producer.internals.ChunkedByteBufferOutputStream#ensureNotDeallocated")]
    fn ensure_not_deallocated(&self) -> Result<usize, Error> {
        self.current_chunk_index
            .ok_or_else(|| Error::local_illegal_state("operation not allowed after the stream has been deallocated"))
    }

    /// Makes room for the next write by advancing past the chunks that are already full.
    /// Returns the index of the (now non-full) current chunk.
    #[doc(
        alias = "org.apache.kafka.clients.producer.internals.ChunkedByteBufferOutputStream#advanceWhileCurrentChunkFull"
    )]
    fn advance_while_current_chunk_full(&mut self) -> Result<usize, Error> {
        while self.current_chunk_position == self.chunk_size {
            self.advance_to_next_chunk()?;
        }
        self.ensure_not_deallocated()
    }

    /// Advances the current chunk to the next pre-supplied chunk.
    #[doc(alias = "org.apache.kafka.clients.producer.internals.ChunkedByteBufferOutputStream#advanceToNextChunk")]
    fn advance_to_next_chunk(&mut self) -> Result<(), Error> {
        let index = self.ensure_not_deallocated()?;
        if index + 1 >= self.chunks.len() {
            // KAFKA-20579: with compression support, Java will grow here instead of throwing.
            return Err(Error::local_illegal_state(
                "write exceeded the stream's remaining chunk capacity",
            ));
        }
        self.current_chunk_index = Some(index + 1);
        self.current_chunk_position = 0;
        Ok(())
    }

    /// Appends pre-allocated chunks to this stream. Ownership of the chunks transfers to the
    /// stream; they will be returned to the pool via [`deallocate`](Self::deallocate).
    ///
    /// Rust shape: Java's caller keeps its reference to the list, so when `addBuffers` throws the
    /// caller still holds the chunks and its `finally` returns them to the pool
    /// (`ChunkedRecordAccumulator.append`). To keep that possible, this drains `new_chunks` only
    /// on success and leaves it untouched on error.
    ///
    /// # Errors
    ///
    /// `IllegalState` after [`deallocate`](Self::deallocate) or [`close`](Self::close);
    /// `IllegalArgument` if a chunk's capacity differs from the stream's chunk size.
    #[doc(alias = "org.apache.kafka.clients.producer.internals.ChunkedByteBufferOutputStream#addBuffers")]
    pub(crate) fn add_buffers(&mut self, new_chunks: &mut Vec<Vec<u8>>) -> Result<(), Error> {
        self.ensure_not_deallocated()?;
        self.ensure_writable()?;
        Self::validate_chunk_capacities(new_chunks, self.chunk_size)?;
        self.chunks.extend(new_chunks.drain(..).map(Self::into_fixed_chunk));
        Ok(())
    }

    /// Returns the written bytes as a single buffer. Must be called only after the stream is
    /// [closed for appends](Self::close).
    ///
    /// Currently the chunks are flattened into a single new buffer, built once and cached so
    /// repeat calls return the same instance, which `MemoryRecordsBuilder::write_default_batch_header`
    /// relies on when it writes the batch header directly into the buffer.
    ///
    /// # Errors
    ///
    /// `IllegalState` after [`deallocate`](Self::deallocate), or if the stream has not been closed
    /// for appends.
    #[doc(alias = "org.apache.kafka.clients.producer.internals.ChunkedByteBufferOutputStream#buffer")]
    pub(crate) fn buffer(&mut self) -> Result<&Bytes, Error> {
        self.ensure_buffer_readable()?;
        if self.flattened_buffer.is_none() {
            self.flattened_buffer = Some(Bytes::from(self.flatten()));
        }
        Ok(self.flattened_buffer.as_ref().expect("populated just above"))
    }

    /// Runs `f` on the flattened buffer and caches the result, so the next
    /// [`buffer`](Self::buffer) call returns the rewritten bytes.
    ///
    /// Rust-only: Java mutates the cached `ByteBuffer` that `buffer()` returns (the builder writes
    /// the batch header into it, and `abort()` repositions it). A `Bytes` is immutable, so the
    /// mutation goes through here instead:
    ///
    /// - before the first [`buffer`](Self::buffer) call, `f` runs on the freshly flattened `Vec`
    ///   before it is frozen, so the first close costs exactly the one flatten copy;
    /// - afterwards (a reopened batch being closed again), the cached `Bytes` is turned back into
    ///   a `Vec`, which reuses the allocation when nothing else still references it and copies it
    ///   otherwise. Copying is what keeps a `MemoryRecords` handed out by the earlier close
    ///   unchanged, matching the single-buffer path, which also re-copies on every close.
    ///
    /// # Errors
    ///
    /// The same as [`buffer`](Self::buffer).
    pub(crate) fn rewrite_buffer<R>(&mut self, f: impl FnOnce(&mut Vec<u8>) -> R) -> Result<R, Error> {
        self.ensure_buffer_readable()?;
        let mut flattened = match self.flattened_buffer.take() {
            Some(bytes) => Vec::from(bytes),
            None => self.flatten(),
        };
        let result = f(&mut flattened);
        self.flattened_buffer = Some(Bytes::from(flattened));
        Ok(result)
    }

    /// The preconditions shared by [`buffer`](Self::buffer) and
    /// [`rewrite_buffer`](Self::rewrite_buffer).
    fn ensure_buffer_readable(&self) -> Result<(), Error> {
        self.ensure_not_deallocated()?;
        if !self.closed {
            return Err(Error::local_illegal_state(
                "buffer() must not be called before the stream is closed for appends",
            ));
        }
        Ok(())
    }

    /// Flattens the written bytes across the data-bearing chunks into a single new buffer (an
    /// extra copy). This will be removed once scatter-gather send (KAFKA-20580) is implemented.
    ///
    /// Written bytes only live in chunks up to the current one; later chunks are untouched.
    #[doc(alias = "org.apache.kafka.clients.producer.internals.ChunkedByteBufferOutputStream#flatten")]
    fn flatten(&self) -> Vec<u8> {
        let current = self.current_chunk_index.expect("checked by ensure_buffer_readable");
        let mut flattened = Vec::with_capacity(current * self.chunk_size + self.current_chunk_position);
        for chunk in &self.chunks[..current] {
            flattened.extend_from_slice(chunk);
        }
        flattened.extend_from_slice(&self.chunks[current][..self.current_chunk_position]);
        flattened
    }

    /// Closes the stream for appends: no further writes or [`add_buffers`](Self::add_buffers) are
    /// allowed, and the fully-unused chunks are released to the pool. Idempotent.
    #[doc(alias = "org.apache.kafka.clients.producer.internals.ChunkedByteBufferOutputStream#close")]
    pub(crate) fn close(&mut self) {
        self.closed = true;
        self.release_unused_chunks();
    }

    /// Return the fully-unused chunks to the pool. The data-bearing chunks are kept until batch
    /// completion ([`deallocate`](Self::deallocate)), as they hold the in-flight data.
    #[doc(alias = "org.apache.kafka.clients.producer.internals.ChunkedByteBufferOutputStream#releaseUnusedChunks")]
    fn release_unused_chunks(&mut self) {
        // Already deallocated: nothing attached.
        let Some(current) = self.current_chunk_index else {
            return;
        };
        // Removing the released chunks from `chunks` means they are not deallocated again on
        // batch completion.
        for chunk in self.chunks.drain(current + 1..) {
            if let Some(pool) = &self.pool {
                pool.deallocate(chunk.into_vec());
            }
        }
    }

    /// Total bytes written across all chunks.
    ///
    /// # Errors
    ///
    /// `IllegalState` after [`deallocate`](Self::deallocate).
    #[doc(alias = "org.apache.kafka.clients.producer.internals.ChunkedByteBufferOutputStream#position")]
    pub(crate) fn position(&self) -> Result<usize, Error> {
        let current = self.ensure_not_deallocated()?;
        // Every chunk before the current one is full (see the struct docs).
        Ok(current * self.chunk_size + self.current_chunk_position)
    }

    /// Sets the write position, walking across pre-supplied chunks if the requested position
    /// exceeds the first chunk's capacity. Only valid before any write.
    ///
    /// Java sets each walked chunk's position before it finds out that the request exceeds the
    /// attached capacity, leaving the stream half-moved when it throws. Rust checks the capacity
    /// first and leaves the stream unchanged on error.
    ///
    /// # Errors
    ///
    /// `IllegalState` after [`deallocate`](Self::deallocate) or after any write;
    /// `IllegalArgument` if `position` exceeds the attached capacity.
    #[doc(alias = "org.apache.kafka.clients.producer.internals.ChunkedByteBufferOutputStream#position")]
    pub(crate) fn set_position(&mut self, position: usize) -> Result<(), Error> {
        let current = self.ensure_not_deallocated()?;
        if current != 0 || self.current_chunk_position != 0 {
            return Err(Error::local_illegal_state("position() can only be called before any writes"));
        }
        if position > self.chunks.len() * self.chunk_size {
            return Err(Error::local_illegal_argument(format!(
                "position {} exceeds total pre-allocated capacity",
                position
            )));
        }
        // Java's walk: take `min(remaining, capacity)` from each chunk and move to the next one
        // only while bytes remain, so a position on a chunk boundary stays in the full chunk.
        if position == 0 {
            return Ok(());
        }
        let index = (position - 1) / self.chunk_size;
        self.current_chunk_index = Some(index);
        self.current_chunk_position = position - index * self.chunk_size;
        Ok(())
    }

    /// Total capacity across all attached chunks (written + free). Every chunk has the same size,
    /// so this equals `position() + remaining()` without walking the list.
    #[doc(alias = "org.apache.kafka.clients.producer.internals.ChunkedByteBufferOutputStream#attachedCapacity")]
    pub(crate) fn attached_capacity(&self) -> Result<usize, Error> {
        self.ensure_not_deallocated()?;
        Ok(self.chunks.len() * self.chunk_size)
    }

    /// Total bytes available across the current chunk and every queued (not-yet-active) chunk.
    #[doc(alias = "org.apache.kafka.clients.producer.internals.ChunkedByteBufferOutputStream#remaining")]
    pub(crate) fn remaining(&self) -> Result<usize, Error> {
        let current = self.ensure_not_deallocated()?;
        // The current chunk's free bytes plus every later chunk, all of which are empty.
        Ok((self.chunk_size - self.current_chunk_position) + (self.chunks.len() - current - 1) * self.chunk_size)
    }

    /// Java's `limit()`: the stream has no limit of its own, so `Integer.MAX_VALUE`.
    #[doc(alias = "org.apache.kafka.clients.producer.internals.ChunkedByteBufferOutputStream#limit")]
    pub(crate) fn limit(&self) -> Result<usize, Error> {
        self.ensure_not_deallocated()?;
        Ok(i32::MAX as usize)
    }

    /// The chunk size, which is the stream's initial capacity.
    #[doc(alias = "org.apache.kafka.clients.producer.internals.ChunkedByteBufferOutputStream#initialCapacity")]
    pub(crate) fn initial_capacity(&self) -> Result<usize, Error> {
        self.ensure_not_deallocated()?;
        Ok(self.chunk_size)
    }

    /// Checks that `remaining_bytes_required` more bytes fit in the attached chunks.
    ///
    /// A single write can be split across several chunks, so the required bytes needn't be
    /// contiguous: only the total free space matters. Advancing here would waste the tail of the
    /// current chunk, so writes advance lazily and this only validates. (KAFKA-20579 will grow
    /// here.)
    ///
    /// # Errors
    ///
    /// `IllegalState` after [`deallocate`](Self::deallocate), or if the bytes do not fit.
    #[doc(alias = "org.apache.kafka.clients.producer.internals.ChunkedByteBufferOutputStream#ensureRemaining")]
    pub(crate) fn ensure_remaining(&self, remaining_bytes_required: usize) -> Result<(), Error> {
        let remaining = self.remaining()?;
        if remaining_bytes_required > remaining {
            return Err(Error::local_illegal_state(format!(
                "required {} bytes but only {} remaining across the attached chunks",
                remaining_bytes_required, remaining
            )));
        }
        Ok(())
    }

    /// Returns all pool-allocated chunks to `pool` (Java's `deallocate(BufferPool)`). Called at
    /// batch completion. Idempotent: a second call finds no chunks.
    #[doc(alias = "org.apache.kafka.clients.producer.internals.ChunkedByteBufferOutputStream#deallocate")]
    pub(crate) fn deallocate_with_pool(&mut self, pool: Option<&BufferPool>) {
        for chunk in self.chunks.drain(..) {
            if let Some(pool) = pool {
                pool.deallocate(chunk.into_vec());
            }
        }
        self.current_chunk_index = None;
        self.current_chunk_position = 0;
        self.flattened_buffer = None;
    }

    /// Returns all pool-allocated chunks to the stream's own pool. Called at batch completion.
    #[doc(alias = "org.apache.kafka.clients.producer.internals.ChunkedByteBufferOutputStream#deallocate")]
    pub(crate) fn deallocate(&mut self) {
        let pool = self.pool.clone();
        self.deallocate_with_pool(pool.as_deref());
    }

    /// The number of attached chunks (test-only: Java's tests infer it from pool accounting).
    #[cfg(test)]
    fn chunk_count(&self) -> usize {
        self.chunks.len()
    }
}

/// `io::Write` over the chunks, so `DefaultRecord::write_to` and the compression wrappers can
/// write into the stream the way they write into a `Vec<u8>`.
///
/// `io::Write::write` promises that an error means no bytes were written, so unlike
/// [`write_bytes`](ChunkedByteBufferOutputStream::write_bytes) it writes only what fits and
/// reports the shortfall as a short write; the error comes on the next call, when no capacity is
/// left. `write_all` therefore fails exactly where `write_bytes` does, with the same bytes
/// written.
impl io::Write for ChunkedByteBufferOutputStream {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }
        let remaining = self.remaining().map_err(io::Error::other)?;
        let to_write = buf.len().min(remaining.max(1));
        self.write_bytes(&buf[..to_write]).map_err(io::Error::other)?;
        Ok(to_write)
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl Drop for ChunkedByteBufferOutputStream {
    /// Rust-only: returns any chunks still attached, so a stream dropped without
    /// [`deallocate`](ChunkedByteBufferOutputStream::deallocate) (a cancelled `append` future, or
    /// a batch dropped on an error path) does not leak pool memory.
    fn drop(&mut self) {
        if !self.chunks.is_empty() {
            self.deallocate();
        }
    }
}

impl std::fmt::Debug for ChunkedByteBufferOutputStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ChunkedByteBufferOutputStream")
            .field("chunks", &self.chunks.len())
            .field("chunk_size", &self.chunk_size)
            .field("current_chunk_index", &self.current_chunk_index)
            .field("current_chunk_position", &self.current_chunk_position)
            .field("closed", &self.closed)
            .finish()
    }
}

/// Translated from `ChunkedByteBufferOutputStreamTest` (Apache Kafka 4.4, KAFKA-20578).
#[cfg(test)]
mod tests {
    use std::io::Write;

    use super::*;

    fn pool(total: i64, chunk_size: usize) -> Arc<BufferPool> {
        Arc::new(BufferPool::new_incremental_for_test(total, chunk_size))
    }

    fn chunks(pool: &BufferPool, chunk_size: usize, count: usize) -> Vec<Vec<u8>> {
        pool.try_allocate_chunks((chunk_size * count) as i32)
            .expect("the test pools are large enough")
    }

    fn stream(p: &Arc<BufferPool>, chunk_size: usize, count: usize) -> ChunkedByteBufferOutputStream {
        ChunkedByteBufferOutputStream::new(chunks(p, chunk_size, count), chunk_size, Some(Arc::clone(p))).unwrap()
    }

    fn assert_illegal_state<T: std::fmt::Debug>(result: Result<T, Error>, message: &str) {
        let err = result.expect_err("expected an IllegalState error");
        assert!(matches!(err, Error::LocalIllegalState(_)), "got {err:?}");
        assert_eq!(message, err.message());
    }

    /// The crate `Error` an `io::Write` call failed with, so its message can be asserted.
    fn io_message(err: io::Error) -> String {
        err.into_inner()
            .expect("the stream reports its errors through io::Error::other")
            .downcast::<Error>()
            .expect("the inner error is the crate Error")
            .message()
            .to_string()
    }

    const DEALLOCATED: &str = "operation not allowed after the stream has been deallocated";
    const CLOSED: &str = "cannot write after the stream has been closed";

    #[test]
    #[doc(
        alias = "org.apache.kafka.clients.producer.internals.ChunkedByteBufferOutputStreamTest#testConstructorRejectsInvalidChunks"
    )]
    fn test_constructor_rejects_invalid_chunks() {
        let chunk_size = 16;
        let p = pool(64, chunk_size);

        // Java's `null` list has no Rust counterpart; the empty list is the remaining case.
        let err = ChunkedByteBufferOutputStream::new(Vec::new(), chunk_size, Some(Arc::clone(&p))).unwrap_err();
        assert!(matches!(err, Error::LocalIllegalArgument(_)), "got {err:?}");
        assert_eq!("initialChunks must be non-empty", err.message());

        // A chunk whose capacity doesn't match chunkSize violates the contract.
        let wrong_size = vec![vec![0u8; chunk_size + 1]];
        let err = ChunkedByteBufferOutputStream::new(wrong_size, chunk_size, Some(p)).unwrap_err();
        assert!(matches!(err, Error::LocalIllegalArgument(_)), "got {err:?}");
        assert_eq!(
            "each chunk must have capacity 16, but found a chunk of capacity 17",
            err.message()
        );
    }

    #[test]
    #[doc(
        alias = "org.apache.kafka.clients.producer.internals.ChunkedByteBufferOutputStreamTest#testOperationsRejectedAfterDeallocate"
    )]
    fn test_operations_rejected_after_deallocate() {
        let chunk_size = 8;
        let p = pool(64, chunk_size);
        let mut stream = stream(&p, chunk_size, 2);
        stream.write_bytes(&[1, 2, 3]).unwrap();

        stream.deallocate();

        // Every query/write operation must reject use after deallocation.
        assert_illegal_state(stream.remaining(), DEALLOCATED);
        assert_illegal_state(stream.position(), DEALLOCATED);
        assert_illegal_state(stream.buffer().map(|b| b.len()), DEALLOCATED);
        assert_illegal_state(stream.attached_capacity(), DEALLOCATED);
        assert_illegal_state(stream.limit(), DEALLOCATED);
        assert_illegal_state(stream.initial_capacity(), DEALLOCATED);
        assert_illegal_state(stream.set_position(1), DEALLOCATED);
        assert_illegal_state(stream.ensure_remaining(1), DEALLOCATED);
        assert_illegal_state(stream.write_byte(1), DEALLOCATED);
        assert_illegal_state(stream.write_bytes(&[4]), DEALLOCATED);
        // Java's `write(ByteBuffer)`: the io::Write route.
        let io_err = stream.write(&[5]).unwrap_err();
        assert_eq!(DEALLOCATED, io_message(io_err));
        let mut extra = vec![vec![0u8; chunk_size]];
        assert_illegal_state(stream.add_buffers(&mut extra), DEALLOCATED);
        assert_eq!(1, extra.len(), "a refused add_buffers leaves the chunks with the caller");

        // Lifecycle calls stay idempotent no-ops: a second close()/deallocate() must not fail.
        stream.close();
        stream.deallocate();
        assert_eq!(64, p.available_memory());
    }

    #[test]
    #[doc(
        alias = "org.apache.kafka.clients.producer.internals.ChunkedByteBufferOutputStreamTest#testWritesDisallowedAfterClose"
    )]
    fn test_writes_disallowed_after_close() {
        let chunk_size = 16;
        let p = pool(64, chunk_size);
        let mut stream = stream(&p, chunk_size, 2);
        stream.write_bytes(&[1, 2, 3]).unwrap();

        // close() closes the stream for appends; any subsequent write must fail.
        stream.close();

        assert_illegal_state(stream.write_byte(1), CLOSED);
        assert_illegal_state(stream.write_bytes(&[4]), CLOSED);
        assert_eq!(CLOSED, io_message(stream.write(&[5]).unwrap_err()));
        // Attaching more chunks is a write-preparation step, so it is disallowed once closed too.
        let mut extra = vec![vec![0u8; chunk_size]];
        assert_illegal_state(stream.add_buffers(&mut extra), CLOSED);

        // buffer() still works after close and returns the same cached instance on repeat calls.
        let first = stream.buffer().unwrap().as_ptr();
        assert_eq!(
            first,
            stream.buffer().unwrap().as_ptr(),
            "buffer() must return the same cached instance once built"
        );

        stream.deallocate();
    }

    #[test]
    #[doc(
        alias = "org.apache.kafka.clients.producer.internals.ChunkedByteBufferOutputStreamTest#testSingleChunkWriteRoundtrip"
    )]
    fn test_single_chunk_write_roundtrip() {
        let chunk_size = 16;
        let p = pool(64, chunk_size);
        let mut stream = stream(&p, chunk_size, 1);

        let payload = [1u8, 2, 3, 4, 5];
        stream.write_bytes(&payload).unwrap();

        stream.close();
        assert_eq!(&payload[..], &stream.buffer().unwrap()[..]);

        stream.deallocate();
    }

    #[test]
    #[doc(
        alias = "org.apache.kafka.clients.producer.internals.ChunkedByteBufferOutputStreamTest#testWriteAcrossMultipleChunks"
    )]
    fn test_write_across_multiple_chunks() {
        let chunk_size = 8;
        let p = pool(64, chunk_size);
        let mut stream = stream(&p, chunk_size, 3);

        let payload: Vec<u8> = (0..20).collect();
        stream.write_bytes(&payload).unwrap();

        stream.close();
        assert_eq!(&payload[..], &stream.buffer().unwrap()[..]);

        stream.deallocate();
    }

    #[test]
    #[doc(
        alias = "org.apache.kafka.clients.producer.internals.ChunkedByteBufferOutputStreamTest#testRemainingSumsFreeBytesAcrossChunks"
    )]
    fn test_remaining_sums_free_bytes_across_chunks() {
        let chunk_size = 8;
        let p = pool(64, chunk_size);
        let mut stream = stream(&p, chunk_size, 2);

        assert_eq!(2 * chunk_size, stream.remaining().unwrap());
        stream.write_bytes(&[0u8; 3]).unwrap();
        assert_eq!(2 * chunk_size - 3, stream.remaining().unwrap());
        stream.write_bytes(&[0u8; 8]).unwrap(); // crosses into chunk 2
        assert_eq!(chunk_size - 3, stream.remaining().unwrap());

        stream.deallocate();
    }

    #[test]
    #[doc(
        alias = "org.apache.kafka.clients.producer.internals.ChunkedByteBufferOutputStreamTest#testEnsureRemainingDoesNotWasteCurrentChunk"
    )]
    fn test_ensure_remaining_does_not_waste_current_chunk() {
        let chunk_size = 8;
        let p = pool(64, chunk_size);
        let mut stream = stream(&p, chunk_size, 2);
        stream.write_bytes(&[1, 2, 3]).unwrap();

        // Requesting more than the current chunk's free bytes (5) but no more than the total free
        // bytes across chunks (13) must not skip ahead: the bytes left in the current chunk stay
        // writable.
        stream.ensure_remaining(chunk_size + 1).unwrap();
        assert_eq!(2 * chunk_size - 3, stream.remaining().unwrap());
        stream.write_bytes(&[4, 5]).unwrap();
        assert_eq!(5, stream.position().unwrap());
        assert_eq!(2 * chunk_size - 5, stream.remaining().unwrap());

        // Rust-only: the failing direction, with Java's message.
        assert_illegal_state(
            stream.ensure_remaining(2 * chunk_size - 4),
            "required 12 bytes but only 11 remaining across the attached chunks",
        );

        stream.deallocate();
    }

    #[test]
    #[doc(
        alias = "org.apache.kafka.clients.producer.internals.ChunkedByteBufferOutputStreamTest#testAddBuffersExtendsStream"
    )]
    fn test_add_buffers_extends_stream() {
        let chunk_size = 8;
        let p = pool(64, chunk_size);
        let mut stream = stream(&p, chunk_size, 1);

        // Fill the initial chunk.
        stream.write_bytes(&[0u8; 8]).unwrap();
        assert_eq!(0, stream.remaining().unwrap());

        // Extend and write more — must land in the new chunk.
        let mut extra = chunks(&p, chunk_size, 1);
        stream.add_buffers(&mut extra).unwrap();
        assert!(extra.is_empty(), "ownership of the added chunks moves to the stream");
        assert_eq!(chunk_size, stream.remaining().unwrap());

        stream.write_bytes(&[9, 9, 9]).unwrap();
        assert_eq!(chunk_size - 3, stream.remaining().unwrap());

        stream.deallocate();
    }

    #[test]
    #[doc(
        alias = "org.apache.kafka.clients.producer.internals.ChunkedByteBufferOutputStreamTest#testAddBuffersRejectsWrongSizeChunks"
    )]
    fn test_add_buffers_rejects_wrong_size_chunks() {
        let chunk_size = 8;
        let p = pool(64, chunk_size);
        let mut stream = stream(&p, chunk_size, 1);
        // A chunk whose capacity doesn't match chunkSize violates the contract, same as the
        // constructor.
        let mut wrong_size = vec![vec![0u8; chunk_size + 1]];
        let err = stream.add_buffers(&mut wrong_size).unwrap_err();
        assert!(matches!(err, Error::LocalIllegalArgument(_)), "got {err:?}");
        assert_eq!(
            "each chunk must have capacity 8, but found a chunk of capacity 9",
            err.message()
        );
        assert_eq!(1, wrong_size.len(), "a refused add_buffers leaves the chunks with the caller");

        stream.deallocate();
    }

    #[test]
    #[doc(
        alias = "org.apache.kafka.clients.producer.internals.ChunkedByteBufferOutputStreamTest#testPositionWalksAcrossChunks"
    )]
    fn test_position_walks_across_chunks() {
        let chunk_size = 4;
        let p = pool(32, chunk_size);
        let mut stream = stream(&p, chunk_size, 3);

        stream.set_position(6).unwrap(); // straddles chunk 0 (4 bytes) and chunk 1 (2 bytes)
        assert_eq!(6, stream.position().unwrap());
        assert_eq!(6, stream.remaining().unwrap());

        stream.deallocate();
    }

    #[test]
    #[doc(
        alias = "org.apache.kafka.clients.producer.internals.ChunkedByteBufferOutputStreamTest#testDeallocateReturnsAllChunks"
    )]
    fn test_deallocate_returns_all_chunks() {
        let chunk_size = 8;
        let total = 64;
        let p = pool(total, chunk_size);
        let mut stream = stream(&p, chunk_size, 3);
        // Also attach one extra chunk so deallocate must handle the added-buffer case too.
        stream.add_buffers(&mut chunks(&p, chunk_size, 1)).unwrap();
        assert_eq!(total - 4 * chunk_size as i64, p.available_memory());

        stream.deallocate();
        assert_eq!(total, p.available_memory());
    }

    /// The fully-unused chunks are returned to the pool on `close()` (the stream is closed for
    /// appends). Reading `buffer()` afterwards has no chunk-releasing side effect; the
    /// data-bearing chunks stay reserved until `deallocate()`, which must not return the
    /// already-released chunks a second time.
    #[test]
    #[doc(
        alias = "org.apache.kafka.clients.producer.internals.ChunkedByteBufferOutputStreamTest#testUnusedChunksReleasedOnCloseNotOnBuffer"
    )]
    fn test_unused_chunks_released_on_close_not_on_buffer() {
        let chunk_size = 8;
        let total = 64;
        let p = pool(total, chunk_size);
        let mut stream = stream(&p, chunk_size, 3);
        // Write into the first chunk only; chunks 2 and 3 stay unused.
        let payload = [1u8, 2, 3];
        stream.write_bytes(&payload).unwrap();
        assert_eq!(total - 3 * chunk_size as i64, p.available_memory());

        // buffer() is only valid once the stream is closed for appends.
        assert_illegal_state(
            stream.buffer().map(|b| b.len()),
            "buffer() must not be called before the stream is closed for appends",
        );

        // close() (appends done) releases the two unused chunks; a second close() is a no-op.
        stream.close();
        assert_eq!(
            total - chunk_size as i64,
            p.available_memory(),
            "the two unused chunks should return to the pool on close"
        );
        stream.close();
        assert_eq!(total - chunk_size as i64, p.available_memory());

        // Reading buffer() after close must not release the remaining data-bearing chunk.
        let built = stream.buffer().unwrap().clone();
        assert_eq!(
            total - chunk_size as i64,
            p.available_memory(),
            "buffer() must not release chunks"
        );
        assert_eq!(&payload[..], &built[..]);

        // Completion-time deallocate returns only the remaining data-bearing chunk (no double
        // free).
        stream.deallocate();
        assert_eq!(
            total,
            p.available_memory(),
            "pool must be exactly restored; released chunks must not be returned twice on completion"
        );
    }

    // ---- Rust-only tests -------------------------------------------------------------------

    /// No chunk can grow past its capacity: a write that does not fit fails with Java's message
    /// (after writing the bytes that fit, as Java does), every chunk keeps its exact length, and
    /// the pool gets back exactly the memory it lent. `write_all` through `io::Write` fails at the
    /// same point without breaking `io::Write::write`'s "error means nothing written" promise.
    #[test]
    fn test_no_chunk_grows_past_its_capacity() {
        let chunk_size = 8;
        let total = 64;
        let p = pool(total, chunk_size);
        let mut stream = stream(&p, chunk_size, 2);

        assert_illegal_state(
            stream.write_bytes(&[7u8; 17]),
            "write exceeded the stream's remaining chunk capacity",
        );
        assert_eq!(16, stream.position().unwrap(), "the bytes that fit were written");
        assert_eq!(0, stream.remaining().unwrap());
        assert_illegal_state(stream.write_byte(1), "write exceeded the stream's remaining chunk capacity");
        assert!(stream.chunks.iter().all(|c| c.len() == chunk_size), "no chunk grew");
        assert_eq!(2, stream.chunk_count());

        // io::Write: a short write while capacity remains, then an error with nothing written.
        let mut io_stream = self::stream(&p, chunk_size, 1);
        io_stream.write_bytes(&[1, 2, 3, 4, 5]).unwrap();
        assert_eq!(3, io_stream.write(&[6u8; 10]).unwrap(), "only what fits is written");
        let err = io_stream.write(&[6u8; 10]).unwrap_err();
        assert_eq!("write exceeded the stream's remaining chunk capacity", io_message(err));
        assert_eq!(8, io_stream.position().unwrap(), "the failing write wrote nothing");
        let mut third = self::stream(&p, chunk_size, 1);
        assert!(third.write_all(&[0u8; 9]).is_err());
        assert_eq!(8, third.position().unwrap());

        stream.deallocate();
        io_stream.deallocate();
        third.deallocate();
        assert_eq!(total, p.available_memory(), "every chunk returned at its original capacity");
        assert_eq!(
            4,
            p.free_size(),
            "all 4 chunks lent out went back to the free list as poolable buffers"
        );
    }

    /// `set_position` preconditions with Java's messages, including the boundary case where the
    /// position ends exactly on a chunk boundary (the next write must then advance) and the
    /// capacity check, which Rust makes before moving anything.
    #[test]
    fn test_set_position_preconditions_and_boundary() {
        let chunk_size = 4;
        let p = pool(32, chunk_size);
        let mut stream = stream(&p, chunk_size, 2);

        let err = stream.set_position(9).unwrap_err();
        assert!(matches!(err, Error::LocalIllegalArgument(_)), "got {err:?}");
        assert_eq!("position 9 exceeds total pre-allocated capacity", err.message());
        assert_eq!(0, stream.position().unwrap(), "a refused set_position moves nothing");

        stream.set_position(4).unwrap();
        assert_eq!(4, stream.position().unwrap());
        assert_eq!(4, stream.remaining().unwrap());
        assert_illegal_state(stream.set_position(1), "position() can only be called before any writes");
        stream.write_bytes(&[1, 2]).unwrap();
        assert_eq!(6, stream.position().unwrap());
        stream.close();
        assert_eq!(&[0, 0, 0, 0, 1, 2][..], &stream.buffer().unwrap()[..]);
        assert_eq!(chunk_size, stream.initial_capacity().unwrap());
        assert_eq!(i32::MAX as usize, stream.limit().unwrap());
        assert_eq!(2 * chunk_size, stream.attached_capacity().unwrap());
    }

    /// `rewrite_buffer` writes into the cached flattened buffer: the first call costs exactly the
    /// flatten, later `buffer()` calls see the rewrite, and a rewrite while an earlier view is
    /// still shared leaves that view untouched.
    #[test]
    fn test_rewrite_buffer() {
        let chunk_size = 4;
        let p = pool(32, chunk_size);
        let mut stream = stream(&p, chunk_size, 2);
        stream.write_bytes(&[1, 2, 3, 4, 5, 6]).unwrap();
        stream.close();

        stream.rewrite_buffer(|b| b[0] = 9).unwrap();
        let first = stream.buffer().unwrap().clone();
        assert_eq!(&[9, 2, 3, 4, 5, 6][..], &first[..]);

        stream.rewrite_buffer(|b| b[1] = 8).unwrap();
        assert_eq!(&[9, 8, 3, 4, 5, 6][..], &stream.buffer().unwrap()[..]);
        assert_eq!(&[9, 2, 3, 4, 5, 6][..], &first[..], "a shared earlier view is not mutated");
    }

    /// Dropping a stream that still holds chunks returns them to the pool (Rust-only: a dropped
    /// future or batch has no Java `deallocate()` call).
    #[test]
    fn test_drop_returns_chunks() {
        let chunk_size = 8;
        let total = 64;
        let p = pool(total, chunk_size);
        {
            let mut stream = stream(&p, chunk_size, 3);
            stream.write_bytes(&[1, 2, 3]).unwrap();
            assert_eq!(total - 3 * chunk_size as i64, p.available_memory());
        }
        assert_eq!(total, p.available_memory());
    }
}
