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

//! Translation of `org.apache.kafka.common.record.UnalignedMemoryRecords`.

use std::sync::OnceLock;

use bytes::Bytes;

use crate::common::record::{BaseRecords, TransferableRecords, UnalignedRecords};

/// Represents a memory record set which is not necessarily offset-aligned.
///
/// Mirrors Java's `UnalignedMemoryRecords`. The Java implementation wraps a
/// `ByteBuffer`; we wrap a [`bytes::Bytes`] so cloning the record set is a
/// cheap refcount bump (no payload copy) and slicing for the network send
/// path is also zero-copy.
#[derive(Clone, Debug)]
pub struct UnalignedMemoryRecords {
    buffer: Bytes,
}

impl UnalignedMemoryRecords {
    /// Construct a new instance over `buffer`. Mirrors Java's
    /// `UnalignedMemoryRecords(ByteBuffer)`.
    pub fn new(buffer: Bytes) -> Self {
        UnalignedMemoryRecords { buffer }
    }

    /// Construct over a [`Vec<u8>`] (test convenience). The vector is moved
    /// into a `Bytes` without copying.
    pub fn from_vec(vec: Vec<u8>) -> Self {
        UnalignedMemoryRecords::new(Bytes::from(vec))
    }

    /// Borrow the underlying buffer.
    ///
    /// Mirrors Java's `buffer()` (which returns `buffer.duplicate()`); both
    /// expose a read-only view into the same backing storage.
    pub fn buffer(&self) -> &Bytes {
        &self.buffer
    }

    /// The empty unaligned record set. Mirrors Java's private `EMPTY`
    /// constant — returned via [`UnalignedMemoryRecords::empty`].
    pub fn empty() -> &'static UnalignedMemoryRecords {
        static EMPTY: OnceLock<UnalignedMemoryRecords> = OnceLock::new();
        EMPTY.get_or_init(|| UnalignedMemoryRecords::new(Bytes::new()))
    }
}

impl BaseRecords for UnalignedMemoryRecords {
    fn size_in_bytes(&self) -> i32 {
        self.buffer.len() as i32
    }
}

impl TransferableRecords for UnalignedMemoryRecords {}

impl UnalignedRecords for UnalignedMemoryRecords {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_returns_zero_size_singleton() {
        let e1 = UnalignedMemoryRecords::empty();
        let e2 = UnalignedMemoryRecords::empty();
        assert_eq!(e1.size_in_bytes(), 0);
        assert_eq!(e2.size_in_bytes(), 0);
        // Singleton: must alias the same instance.
        assert!(std::ptr::eq(e1, e2));
    }

    #[test]
    fn size_in_bytes_matches_buffer_len() {
        let payload = vec![1u8, 2, 3, 4, 5, 6, 7];
        let r = UnalignedMemoryRecords::from_vec(payload.clone());
        assert_eq!(r.size_in_bytes(), payload.len() as i32);
        assert_eq!(r.buffer().as_ref(), payload.as_slice());
    }

    #[test]
    fn clone_shares_storage() {
        let r = UnalignedMemoryRecords::from_vec(vec![10u8; 1024]);
        let p1 = r.buffer().as_ptr();
        let r2 = r.clone();
        let p2 = r2.buffer().as_ptr();
        assert_eq!(p1, p2, "clone must share the same backing storage");
    }
}
