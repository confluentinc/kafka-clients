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

//! Buffered reading of a decompression stream, through a buffer taken from a
//! [`BufferSupplier`].
//!
//! Translated from `org.apache.kafka.common.utils.internals.ChunkedBytesStream`.

use std::io::{self, BufRead, Read};
use std::sync::Arc;

use crate::common::utils::internals::BufferSupplier;

/// ChunkedBytesStream is a copy of `BufferedInputStream` with the following
/// differences:
/// - Unlike `java.io.BufferedInputStream#skip(long)` this class could be
///   configured to not push skip() to input stream. We may want to avoid
///   pushing this to input stream because it's implementation maybe
///   inefficient, e.g. the case of ZstdInputStream which allocates a new
///   buffer from buffer pool, per skip call.
/// - Unlike `java.io.BufferedInputStream`, which allocates an intermediate
///   buffer, this uses a buffer supplier to create the intermediate buffer.
/// - Unlike `java.io.BufferedInputStream`, this implementation does not
///   support `InputStream#mark(int)` and `InputStream#markSupported()` will
///   return false.
///
/// Note that:
/// - this class is not thread safe and shouldn't be used in scenarios where
///   multiple threads access this.
/// - the implementation of this class is performance sensitive. Minor changes
///   such as usage of ByteBuffer instead of byte[] can significantly impact
///   performance, hence, proceed with caution.
///
/// # Translation
///
/// Java's `FilterInputStream` over an `InputStream` becomes a [`Read`] over
/// the stream `R` it reads from. It is also a [`BufRead`], as Rust's buffered
/// readers are and Java's class is not: the consumer looks for bytes left
/// after a batch's last record through [`BufRead::fill_buf`] without taking
/// them. [`Read`] has no `mark` or `reset`, so neither is translated, nor is
/// `available()`: [`Read`] has no such query to pass on to `R`, and Java's one
/// use of it, ending [`Read::read`] before the bytes asked for are read, saves
/// nothing for a caller that reads on until it has them, as every caller does
/// (Java's too: `Utils.readFully`, `DefaultRecord#readFrom(InputStream,long,long,int,Long,int)`).
///
/// The buffer goes back to the supplier when the stream is closed, as Java's
/// `close()` releases it, and dropping the stream closes it.
#[doc(alias = "org.apache.kafka.common.utils.internals.ChunkedBytesStream")]
pub(crate) struct ChunkedBytesStream<R> {
    /// The stream this one reads from: Java's `in`, the `FilterInputStream`
    /// field. `None` once closed; dropping it closes it.
    input: Option<R>,
    /// Supplies the buffer which is used as intermediate buffer to store the
    /// chunk of output data.
    buffer_supplier: Arc<BufferSupplier>,
    /// Intermediate buffer to store the chunk of output data. The
    /// ChunkedBytesStream is considered closed if this buffer is `None`.
    ///
    /// Java also keeps the `ByteBuffer` the array came from
    /// (`intermediateBufRef`), to release it; the `Vec` is released itself.
    intermediate_buf: Option<Vec<u8>>,
    /// The index one greater than the index of the last valid byte in the
    /// buffer. This value is always in the range `0` through
    /// `intermediate_buf.len()`; elements `intermediate_buf[0]` through
    /// `intermediate_buf[count - 1]` contain buffered input data obtained from
    /// the underlying input stream.
    count: usize,
    /// The current position in the buffer. This is the index of the next byte
    /// to be read from the buffer.
    ///
    /// This value is always in the range `0` through `count`. If it is less
    /// than `count`, then `intermediate_buf[pos]` is the next byte to be
    /// supplied as input; if it is equal to `count`, then the next `read` or
    /// `skip` operation will require more bytes to be read from the contained
    /// input stream.
    pos: usize,
    /// If this flag is true, we will delegate the responsibility of skipping
    /// to the source stream. This is an alternative to reading the data from
    /// source stream, storing in an intermediate buffer and skipping the
    /// values using this implementation.
    delegate_skip_to_source_stream: bool,
}

impl<R> ChunkedBytesStream<R> {
    /// Reads `input` through a buffer of `intermediate_buf_size` bytes taken
    /// from `buffer_supplier`.
    ///
    /// Java refuses a buffer that is not backed by an array starting at its
    /// offset 0; a `Vec` always is.
    #[doc(alias = "org.apache.kafka.common.utils.internals.ChunkedBytesStream#ChunkedBytesStream")]
    pub(crate) fn new(
        input: R,
        buffer_supplier: Arc<BufferSupplier>,
        intermediate_buf_size: usize,
        delegate_skip_to_source_stream: bool,
    ) -> Self {
        let mut intermediate_buf = buffer_supplier.get(intermediate_buf_size);
        // Java reads into the whole array the supplier hands out; a `Vec` is
        // read into up to its length, which the supplier leaves at 0.
        intermediate_buf.resize(intermediate_buf.capacity(), 0);
        Self {
            input: Some(input),
            buffer_supplier,
            intermediate_buf: Some(intermediate_buf),
            count: 0,
            pos: 0,
            delegate_skip_to_source_stream,
        }
    }

    /// Check to make sure that buffer has not been nulled out due to close; if
    /// not return it.
    #[doc(alias = "org.apache.kafka.common.utils.internals.ChunkedBytesStream#getBufIfOpen")]
    fn buf_if_open(&self) -> io::Result<&[u8]> {
        self.intermediate_buf.as_deref().ok_or_else(stream_closed)
    }

    /// Check to make sure that underlying input stream has not been nulled out
    /// due to close; if not return it.
    #[doc(alias = "org.apache.kafka.common.utils.internals.ChunkedBytesStream#getInIfOpen")]
    fn in_if_open(&mut self) -> io::Result<&mut R> {
        self.input.as_mut().ok_or_else(stream_closed)
    }

    /// Closes the stream: returns the buffer to its supplier, and drops the
    /// stream read from, which closes it. Every later read fails with "Stream
    /// closed"; closing again does nothing.
    #[doc(alias = "org.apache.kafka.common.utils.internals.ChunkedBytesStream#close")]
    pub(crate) fn close(&mut self) {
        if let Some(buffer) = self.intermediate_buf.take() {
            self.buffer_supplier.release(buffer);
        }
        self.input = None;
    }

    /// The stream this one reads from, `None` once closed.
    // visible for testing
    #[doc(alias = "org.apache.kafka.common.utils.internals.ChunkedBytesStream#sourceStream")]
    pub(crate) fn source_stream(&self) -> Option<&R> {
        self.input.as_ref()
    }
}

impl<R: Read> ChunkedBytesStream<R> {
    /// The next byte of data, or `None` if the end of the stream is reached,
    /// where Java returns `-1`.
    ///
    /// Method syntax finds this before [`Read::read`], which is therefore
    /// called as `Read::read(&mut stream, buf)`.
    ///
    /// # Errors
    ///
    /// This stream has been closed, or the stream it reads from failed.
    #[doc(alias = "org.apache.kafka.common.utils.internals.ChunkedBytesStream#read")]
    pub(crate) fn read(&mut self) -> io::Result<Option<u8>> {
        if self.pos >= self.count {
            self.fill()?;
            if self.pos >= self.count {
                return Ok(None);
            }
        }
        let byte = self.buf_if_open()?[self.pos];
        self.pos += 1;
        Ok(Some(byte))
    }

    /// Fills the intermediate buffer with more data. The amount of new data
    /// read is equal to the remaining empty space in the buffer. For optimal
    /// performance, read as much data as possible in this call. This method
    /// also assumes that all data has already been read in, hence
    /// `pos >= count`.
    ///
    /// Returns the number of bytes read, 0 at the end of the stream, where
    /// Java returns `-1`.
    #[doc(alias = "org.apache.kafka.common.utils.internals.ChunkedBytesStream#fill")]
    fn fill(&mut self) -> io::Result<usize> {
        // The buffer and the stream are borrowed field by field: `buf_if_open`
        // and `in_if_open` would each borrow the whole stream.
        let buffer = self.intermediate_buf.as_deref_mut().ok_or_else(stream_closed)?;
        self.pos = 0;
        self.count = self.pos;
        let input = self.input.as_mut().ok_or_else(stream_closed)?;
        let n = input.read(&mut buffer[self.pos..])?;
        if n > 0 {
            self.count = n + self.pos;
        }
        Ok(n)
    }

    /// Read bytes into `b`, reading from the underlying stream at most once if
    /// necessary. Returns 0 at the end of the stream, where Java returns `-1`.
    ///
    /// Note - Implementation copied from `BufferedInputStream`. Slight
    /// modification done to remove the mark position.
    #[doc(alias = "org.apache.kafka.common.utils.internals.ChunkedBytesStream#read1")]
    fn read1(&mut self, b: &mut [u8]) -> io::Result<usize> {
        let mut avail = self.count - self.pos;
        if avail == 0 {
            // If the requested length is at least as large as the buffer, and
            // if there is no mark/reset activity, do not bother to copy the
            // bytes into the local buffer. In this way buffered streams will
            // cascade harmlessly.
            if b.len() >= self.buf_if_open()?.len() {
                return self.in_if_open()?.read(b);
            }
            self.fill()?;
            avail = self.count - self.pos;
            if avail == 0 {
                return Ok(0);
            }
        }
        let cnt = avail.min(b.len());
        b[..cnt].copy_from_slice(&self.buf_if_open()?[self.pos..self.pos + cnt]);
        self.pos += cnt;
        Ok(cnt)
    }

    /// Skips over and discards exactly `to_skip` bytes of data from this input
    /// stream. If `to_skip` is zero, then no bytes are skipped. If `to_skip`
    /// is negative, then no bytes are skipped.
    ///
    /// This method blocks until the requested number of bytes has been
    /// skipped, end of file is reached, or an error is returned.
    ///
    /// If end of stream is reached before the stream is at the desired
    /// position, then the bytes skipped till then are returned.
    ///
    /// If an I/O error occurs, then the input stream may be in an inconsistent
    /// state. It is strongly recommended that the stream be promptly closed if
    /// an I/O error occurs.
    ///
    /// This method first skips and discards bytes in the intermediate buffer.
    /// After that, depending on the value of `delegate_skip_to_source_stream`,
    /// it either delegates the skipping of bytes to the source stream or it
    /// reads the data from input stream in chunks, copies the data into
    /// intermediate buffer and skips it.
    ///
    /// Starting JDK 12, a new method was introduced in InputStream,
    /// skipNBytes which has a similar behaviour as this method.
    ///
    /// Returns the actual number of bytes skipped, which might be zero.
    ///
    /// # Errors
    ///
    /// This stream has been closed, or the stream it reads from failed.
    #[doc(alias = "org.apache.kafka.common.utils.internals.ChunkedBytesStream#skip")]
    pub(crate) fn skip(&mut self, to_skip: i64) -> io::Result<i64> {
        self.buf_if_open()?; // Check for closed stream
        if to_skip <= 0 {
            return Ok(0);
        }

        let mut remaining = to_skip;

        // Skip bytes stored in intermediate buffer first
        let avail = (self.count - self.pos) as i64;
        let bytes_skipped = avail.min(remaining);
        self.pos += bytes_skipped as usize;
        remaining -= bytes_skipped;

        while remaining > 0 {
            if self.delegate_skip_to_source_stream {
                // Use the source stream's skip to skip the rest. A Rust stream
                // has no skip of its own, so it reads and discards what it
                // skips, as Java's `InputStream.skip` does unless overridden.
                let input = self.in_if_open()?;
                let delegate_bytes_skipped = io::copy(&mut input.take(remaining as u64), &mut io::sink())? as i64;
                if delegate_bytes_skipped == 0 {
                    // read one byte to check for EOS
                    if self.read()?.is_none() {
                        break;
                    }
                    // one byte read so decrement number to skip
                    remaining -= 1;
                }
                // Java fails with "Unable to skip exactly" when the source
                // skips more than asked or a negative count, which `take`
                // rules out.
                remaining -= delegate_bytes_skipped;
            } else {
                // skip from intermediate buffer, filling it first (if required)
                if self.pos >= self.count {
                    self.fill()?;
                    // if we don't have data in intermediate buffer after fill,
                    // then stop skipping
                    if self.pos >= self.count {
                        break;
                    }
                }
                let avail = (self.count - self.pos) as i64;
                let bytes_skipped = avail.min(remaining);
                self.pos += bytes_skipped as usize;
                remaining -= bytes_skipped;
            }
        }
        Ok(to_skip - remaining)
    }
}

impl<R: Read> Read for ChunkedBytesStream<R> {
    /// Reads bytes from this stream into `b`: Java's `read(byte[], int, int)`,
    /// whose offset and length are the slice's.
    ///
    /// This method implements the general contract of [`Read::read`]. As an
    /// additional convenience, it attempts to read as many bytes as possible
    /// by repeatedly invoking the `read` method of the underlying stream. This
    /// iterated `read` continues until the specified number of bytes have been
    /// read, or the `read` method of the underlying stream returns 0,
    /// indicating end-of-file. Java also stops when the `available` method of
    /// the underlying stream returns zero, which has no counterpart here (see
    /// [`ChunkedBytesStream`]). If the first `read` on the underlying stream
    /// returns 0 to indicate end-of-file then this method returns 0. Otherwise
    /// this method returns the number of bytes actually read.
    ///
    /// An error is returned as soon as it is met, as Java's is, also after
    /// bytes have been copied into `b` in the same call, which are then lost:
    /// [`Read`] asks that a read that fails has read nothing. Every caller
    /// here gives up on the stream at its first error, so none would read
    /// them, and none of the streams read through this fails with
    /// [`io::ErrorKind::Interrupted`], the error after which a caller reads
    /// on.
    ///
    /// # Errors
    ///
    /// This stream has been closed, or the stream it reads from failed.
    fn read(&mut self, b: &mut [u8]) -> io::Result<usize> {
        self.buf_if_open()?; // Check for closed stream
        if b.is_empty() {
            return Ok(0);
        }

        let mut n = 0;
        loop {
            let nread = self.read1(&mut b[n..])?;
            if nread == 0 {
                return Ok(n);
            }
            n += nread;
            if n >= b.len() {
                return Ok(n);
            }
        }
    }
}

impl<R: Read> BufRead for ChunkedBytesStream<R> {
    /// The buffered bytes, the buffer filled from the stream read from first
    /// when none are left, as [`ChunkedBytesStream::read`] fills it.
    fn fill_buf(&mut self) -> io::Result<&[u8]> {
        if self.pos >= self.count {
            self.fill()?;
        }
        Ok(&self.buf_if_open()?[self.pos..self.count])
    }

    fn consume(&mut self, amt: usize) {
        self.pos = (self.pos + amt).min(self.count);
    }
}

impl<R> Drop for ChunkedBytesStream<R> {
    /// Closes the stream, as try-with-resources closes Java's, so its buffer
    /// goes back to the supplier.
    fn drop(&mut self) {
        self.close();
    }
}

/// The error a closed stream fails with: Java's `IOException("Stream closed")`.
fn stream_closed() -> io::Error {
    io::Error::other("Stream closed")
}

#[cfg(test)]
mod tests {
    //! Translated from
    //! `org.apache.kafka.common.utils.internals.ChunkedBytesStreamTest`.
    //!
    //! `testInvalidInputsForMethodRead` is not translated: it checks that
    //! `read(byte[], int, int)` refuses an offset and length outside the
    //! array, which a slice cannot express.

    use super::*;
    use crate::AllocTrackingGuard;

    /// The supplier Java's tests read through: `BufferSupplier.NO_CACHING`.
    fn supplier() -> Arc<BufferSupplier> {
        Arc::new(BufferSupplier::no_caching())
    }

    /// A stream over `input` with the 10-byte buffer Java's tests use.
    fn stream(input: &[u8], delegate_skip_to_source_stream: bool) -> ChunkedBytesStream<&[u8]> {
        ChunkedBytesStream::new(input, supplier(), 10, delegate_skip_to_source_stream)
    }

    /// Fixed pseudo-random bytes, standing in for Java's `Random(1337)`: a
    /// linear congruential generator's top bytes.
    fn random_bytes(size: usize) -> Vec<u8> {
        let mut state: u32 = 1337;
        (0..size)
            .map(|_| {
                state = state.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
                (state >> 24) as u8
            })
            .collect()
    }

    /// The inputs of Java's `provideSourceBytebuffersForTest`.
    fn source_inputs() -> [Vec<u8>; 5] {
        [
            // empty byte array: Java's `ByteBuffer.allocate(2)`, two zero bytes
            vec![0; 2],
            // byte array with 1 byte
            vec![1],
            // byte array with size < intermediate buffer
            b"12345678".to_vec(),
            // byte array with size > intermediate buffer
            random_bytes(16),
            // byte array with size >> intermediate buffer
            random_bytes(100),
        ]
    }

    /// A stream that records the length of every read asked of it.
    struct RecordingSource<'a> {
        data: &'a [u8],
        reads: Vec<usize>,
    }

    impl Read for RecordingSource<'_> {
        fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
            self.reads.push(buf.len());
            self.data.read(buf)
        }
    }

    /// Translated from `testEofErrorForMethodReadFully`.
    #[test]
    fn test_eof_error_for_method_read_fully() {
        let input = [0u8; 8];
        let mut got = [0u8; 9];
        let mut is = stream(&input, false);
        assert_eq!(
            8,
            Read::read(&mut is, &mut got).unwrap(),
            "Should return 8 signifying end of input"
        );
    }

    /// Translated from `testCorrectnessForMethodReadFully`, which reads its
    /// input in two passes split at a random point: this tests the scenarios
    /// where one pass may lead to partially consumed intermediate buffer.
    /// Every split point is read here, Java's random one among them, and each
    /// pass is also checked to fill what it was given.
    #[test]
    fn test_correctness_for_method_read_fully() {
        for input in source_inputs() {
            for to_read in 0..input.len() {
                let mut got = vec![0u8; input.len()];
                let mut is = stream(&input, false);
                let first = Read::read(&mut is, &mut got[..to_read]).unwrap();
                let second = Read::read(&mut is, &mut got[to_read..]).unwrap();
                assert_eq!(
                    (to_read, input.len() - to_read),
                    (first, second),
                    "split at {to_read} of {}",
                    input.len()
                );
                assert_eq!(input, got, "split at {to_read} of {}", input.len());
            }
        }
    }

    /// Translated from `testCorrectnessForMethodReadByte`.
    #[test]
    fn test_correctness_for_method_read_byte() {
        for input in source_inputs() {
            let mut is = stream(&input, false);
            let got: Vec<u8> = (0..input.len()).map(|_| is.read().unwrap().expect("a byte")).collect();
            assert_eq!(input, got);
        }
    }

    /// Translated from `testCorrectnessForMethodRead`, which checks that
    /// `read()` returns each byte's unsigned value, as a `u8` is.
    #[test]
    fn test_correctness_for_method_read() {
        for input in source_inputs() {
            let mut is = stream(&input, false);
            let got: Vec<Option<u8>> = (0..input.len()).map(|_| is.read().unwrap()).collect();
            assert_eq!(input.iter().copied().map(Some).collect::<Vec<_>>(), got);
        }
    }

    /// Translated from `testEndOfFileForMethodRead`.
    #[test]
    fn test_end_of_file_for_method_read() {
        let input = [0u8; 2];
        let mut is = stream(&input, false);
        assert_eq!(Some(0), is.read().unwrap());
        assert_eq!(Some(0), is.read().unwrap());
        assert_eq!(None, is.read().unwrap(), "end of file for read should be None");
    }

    /// Translated from `testEndOfSourceForMethodSkip`.
    #[test]
    fn test_end_of_source_for_method_skip() {
        let input = random_bytes(16);
        for push_skip_to_source_stream in [true, false] {
            let mut is = stream(&input, push_skip_to_source_stream);
            assert_eq!(
                16,
                is.skip(17).unwrap(),
                "delegating to the source: {push_skip_to_source_stream}"
            );
        }
    }

    /// Translated from `testCorrectnessForMethodSkip`, with the cases of
    /// `provideSourceSkipValuesForTest`. The bytes read after the skip are also
    /// checked to be the input's next ones, and the stream to end after them.
    #[test]
    fn test_correctness_for_method_skip() {
        let empty_buffer = [0u8; 2];
        let one_byte_buf = [1u8];
        let buf_greater_than_intermediate_buf = random_bytes(16);
        let greater = &buf_greater_than_intermediate_buf[..];
        // (bytes to pre-read, input, bytes to skip)
        let cases: [(usize, &[u8], i64); 12] = [
            // empty source byte array
            (0, &empty_buffer, 0),
            (0, &empty_buffer, 1),
            (1, &empty_buffer, 1),
            (1, &empty_buffer, 0),
            // byte source array with 1 byte
            (0, &one_byte_buf, 0),
            (0, &one_byte_buf, 1),
            (1, &one_byte_buf, 0),
            (1, &one_byte_buf, 1),
            // byte source array with full read from intermediate buf
            (0, greater, 16),
            (16, greater, 0),
            (2, greater, 10),
            (2, greater, 8),
        ];
        for (bytes_to_pre_read, input, num_bytes_to_skip) in cases {
            for push_skip_to_source_stream in [true, false] {
                let case = format!(
                    "{bytes_to_pre_read} of {} bytes pre-read, {num_bytes_to_skip} skipped, delegating to the source: \
                     {push_skip_to_source_stream}",
                    input.len()
                );
                let after_pre_read = input.len() as i64 - bytes_to_pre_read as i64;
                let expected_inp_left_after_skip = after_pre_read - num_bytes_to_skip;
                let expected_skipped_bytes = after_pre_read.min(num_bytes_to_skip);

                let mut is = stream(input, push_skip_to_source_stream);
                for _ in 0..bytes_to_pre_read {
                    assert!(is.read().unwrap().is_some(), "Unexpected end of data. {case}");
                }

                assert_eq!(expected_skipped_bytes, is.skip(num_bytes_to_skip).unwrap(), "{case}");

                // verify that we are able to read rest of the input
                let rest: Vec<u8> = (0..expected_inp_left_after_skip)
                    .map(|_| is.read().unwrap().expect("Unexpected end of data."))
                    .collect();
                let skipped_to = bytes_to_pre_read + expected_skipped_bytes as usize;
                assert_eq!(&input[skipped_to..], &rest[..], "{case}");
                assert_eq!(None, is.read().unwrap(), "{case}");
            }
        }
    }

    /// Translated from `testEdgeCaseInputForMethodSkip`, with the cases of
    /// `provideEdgeCaseInputForMethodSkip`: a skip larger than an `int`, and a
    /// negative one, for both settings.
    #[test]
    fn test_edge_case_input_for_method_skip() {
        let buffer_length = 16;
        let input = random_bytes(buffer_length);
        let cases = [
            (i64::from(i32::MAX) + 1, true, buffer_length as i64),
            (-1, true, 0),
            (i64::from(i32::MAX) + 1, false, buffer_length as i64),
            (-1, false, 0),
        ];
        for (to_skip, delegate_skip_to_source_stream, expected) in cases {
            let mut is = stream(&input, delegate_skip_to_source_stream);
            assert_eq!(
                expected,
                is.skip(to_skip).unwrap(),
                "{to_skip} skipped, delegating to the source: {delegate_skip_to_source_stream}"
            );
        }
    }

    /// The buffer goes back to its supplier when the stream is closed, as
    /// Java's `close()` releases it, or dropped, so the supplier's next buffer
    /// of that size is that one, taken without an allocation, and only once
    /// when the stream is closed twice.
    #[test]
    fn test_close_returns_the_buffer_to_its_supplier() {
        let input = random_bytes(16);
        for closes in 0..=2 {
            let pool = Arc::new(BufferSupplier::create());
            let mut is = ChunkedBytesStream::new(&input[..], Arc::clone(&pool), 10, false);
            assert_eq!(Some(input[0]), is.read().unwrap());
            for _ in 0..closes {
                is.close();
            }
            drop(is);
            let _guard = AllocTrackingGuard::new();
            let reused = pool.get(10);
            assert_eq!(
                (0, 10),
                (AllocTrackingGuard::count(), reused.capacity()),
                "closed {closes} times, then dropped: the buffer went back"
            );
            let fresh = pool.get(10);
            assert_eq!(
                1,
                AllocTrackingGuard::count(),
                "closed {closes} times, then dropped: the buffer went back once"
            );
            drop((reused, fresh));
        }
    }

    /// A closed stream fails every read with Java's "Stream closed", although
    /// its buffer still held unread bytes, and has no stream left to read
    /// from.
    #[test]
    fn test_closed_stream_fails_every_read() {
        let input = random_bytes(16);
        let mut is = stream(&input, false);
        assert_eq!(Some(input[0]), is.read().unwrap());
        is.close();
        assert!(is.source_stream().is_none());
        assert_eq!("Stream closed", is.read().unwrap_err().to_string());
        assert_eq!("Stream closed", Read::read(&mut is, &mut [0; 4]).unwrap_err().to_string());
        assert_eq!("Stream closed", Read::read(&mut is, &mut []).unwrap_err().to_string());
        assert_eq!("Stream closed", is.skip(0).unwrap_err().to_string());
        assert_eq!("Stream closed", is.fill_buf().unwrap_err().to_string());
    }

    /// `fill_buf` shows the buffered bytes, filling the buffer first only when
    /// none are left, and `consume` takes no more than are buffered.
    #[test]
    fn test_fill_buf_and_consume() {
        let mut is = stream(b"0123456789abcdef", false);
        assert_eq!(b"0123456789", is.fill_buf().unwrap());
        assert_eq!(b"0123456789", is.fill_buf().unwrap());
        is.consume(4);
        assert_eq!(b"456789", is.fill_buf().unwrap());
        assert_eq!(Some(b'4'), is.read().unwrap());
        is.consume(100);
        assert_eq!(b"abcdef", is.fill_buf().unwrap());
        is.consume(6);
        assert_eq!(b"", is.fill_buf().unwrap());
        assert_eq!(None, is.read().unwrap());
    }

    /// A read of at least the buffer's size, with nothing buffered, goes
    /// straight to the stream read from, and a shorter one is served from the
    /// buffer, filled first: Java's `read1`. A read is served from what is
    /// buffered first, then from the stream.
    #[test]
    fn test_read_bypasses_the_buffer_when_it_would_fill_it() {
        let input = random_bytes(40);
        let source = RecordingSource { data: &input, reads: Vec::new() };
        let mut is = ChunkedBytesStream::new(source, supplier(), 10, false);
        let mut got = vec![0u8; input.len()];
        assert_eq!(12, Read::read(&mut is, &mut got[..12]).unwrap());
        assert_eq!(vec![12], is.source_stream().unwrap().reads);
        assert_eq!(3, Read::read(&mut is, &mut got[12..15]).unwrap());
        assert_eq!(vec![12, 10], is.source_stream().unwrap().reads);
        // The 7 bytes left in the buffer, then the other 18 read straight in.
        assert_eq!(25, Read::read(&mut is, &mut got[15..]).unwrap());
        assert_eq!(vec![12, 10, 18], is.source_stream().unwrap().reads);
        assert_eq!(input, got);
    }

    /// A skip past the buffer reads the stream read from directly when the
    /// stream is told to delegate it, as lz4's is, and through the buffer
    /// otherwise.
    #[test]
    fn test_skip_delegates_to_the_source_stream_when_told_to() {
        let input = random_bytes(16);
        // (whether the skip is delegated, the reads it asks of the source)
        for (delegate_skip_to_source_stream, reads) in [(true, vec![15]), (false, vec![10, 10])] {
            let source = RecordingSource { data: &input, reads: Vec::new() };
            let mut is = ChunkedBytesStream::new(source, supplier(), 10, delegate_skip_to_source_stream);
            assert_eq!(15, is.skip(15).unwrap());
            assert_eq!(
                reads,
                is.source_stream().unwrap().reads,
                "delegating to the source: {delegate_skip_to_source_stream}"
            );
            assert_eq!(Some(input[15]), is.read().unwrap());
        }
    }

    /// A stream that yields `data`, then fails.
    struct FailingSource<'a> {
        data: &'a [u8],
    }

    impl Read for FailingSource<'_> {
        fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
            if self.data.is_empty() {
                return Err(io::Error::other("source failed"));
            }
            self.data.read(buf)
        }
    }

    /// A failure of the stream read from is returned as soon as it is met, as
    /// Java's is, by a read that had already copied the buffered bytes, and by
    /// every read after it.
    #[test]
    fn test_source_failure_is_returned_at_once() {
        let input = random_bytes(5);
        let mut is = ChunkedBytesStream::new(FailingSource { data: &input }, supplier(), 10, false);
        assert_eq!("source failed", Read::read(&mut is, &mut [0; 8]).unwrap_err().to_string());
        assert_eq!("source failed", is.read().unwrap_err().to_string());
        assert_eq!("source failed", is.fill_buf().unwrap_err().to_string());
    }
}
