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

//! Translation of `org.apache.kafka.common.compress.GzipOutputStream`.
//!
//! Java's class is a thin extension of `java.util.zip.GZIPOutputStream`
//! that lets the caller pick a compression level and an output buffer size
//! beyond the JDK defaults. Our equivalent wraps `flate2::write::GzEncoder`
//! and exposes the same shape: `(out, size, level)`.

use std::io::{self, Write};

use flate2::Compression as FlateCompression;
use flate2::write::GzEncoder;

use crate::common::record::CompressionType;

/// Gzip [`Write`] adapter parameterized by output buffer size and
/// compression level. Mirrors Java's `GzipOutputStream`.
///
/// Java's class extends `GZIPOutputStream(out, size)` — the `size` is the
/// internal `Deflater` output buffer. `flate2`'s API does not expose an
/// equivalent buffer-size knob (the buffer is allocated internally and
/// sized adaptively), so the parameter is accepted for API parity but
/// ignored. The level setting is applied directly to the underlying
/// `Deflater`-equivalent.
pub struct GzipOutputStream<W: Write> {
    inner: GzEncoder<W>,
    /// Configured output buffer size. Recorded for getter parity with the
    /// Java class.
    output_buffer_size: usize,
}

impl<W: Write> GzipOutputStream<W> {
    /// Create a new gzip output stream. Mirrors Java's
    /// `GzipOutputStream(OutputStream, int, int)` constructor.
    ///
    /// `level` follows the JDK `Deflater` convention: `-1` for the default,
    /// otherwise `1..=9`. We mirror that range mapping into `flate2`:
    /// `flate2::Compression::default()` for `-1`, `flate2::Compression::new(level)`
    /// otherwise.
    pub fn new(out: W, size: usize, level: i32) -> Self {
        let flate_level = if level == CompressionType::Gzip.default_level().unwrap() {
            FlateCompression::default()
        } else {
            FlateCompression::new(level as u32)
        };
        GzipOutputStream { inner: GzEncoder::new(out, flate_level), output_buffer_size: size }
    }

    /// Returns the configured output buffer size.
    pub fn output_buffer_size(&self) -> usize {
        self.output_buffer_size
    }
}

impl<W: Write> Write for GzipOutputStream<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.inner.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

impl<W: Write> Drop for GzipOutputStream<W> {
    fn drop(&mut self) {
        // flate2's GzEncoder writes the gzip trailer on `finish()`. Drop
        // path mirrors Java's `close()` — we attempt a best-effort flush
        // so that callers using RAII observe a complete frame.
        let _ = self.inner.try_finish();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use flate2::read::GzDecoder;
    use std::io::Read;

    /// Roundtrip: write into a Vec, read it back via GzDecoder, expect the
    /// original payload.
    #[test]
    fn roundtrip_default_level() {
        let payload = b"hello kafka gzip output stream";
        let mut sink = Vec::new();
        {
            let mut gz = GzipOutputStream::new(&mut sink, 8 * 1024, CompressionType::Gzip.default_level().unwrap());
            gz.write_all(payload).unwrap();
            // Drop runs try_finish.
        }
        let mut decoder = GzDecoder::new(sink.as_slice());
        let mut decoded = Vec::new();
        decoder.read_to_end(&mut decoded).unwrap();
        assert_eq!(decoded, payload);
    }

    #[test]
    fn roundtrip_max_level() {
        let payload = b"hello kafka gzip output stream";
        let mut sink = Vec::new();
        {
            let mut gz = GzipOutputStream::new(&mut sink, 8 * 1024, 9);
            gz.write_all(payload).unwrap();
        }
        let mut decoder = GzDecoder::new(sink.as_slice());
        let mut decoded = Vec::new();
        decoder.read_to_end(&mut decoded).unwrap();
        assert_eq!(decoded, payload);
    }
}
