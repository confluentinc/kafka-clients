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

//! Size constants of the message format versions 0 and 1.
//!
//! Translated from the constants of
//! `org.apache.kafka.common.record.internal.LegacyRecord`
//! (`LegacyRecord.java:45-69`). **Only the constants are translated.** Kafka
//! 4.0 removed message formats v0 and v1 (KIP-724), and this client reads and
//! writes only v2, so the rest of `LegacyRecord` — the record class itself —
//! has no Rust counterpart. The constants remain because
//! [`ByteBufferLogInputStream`](super::ByteBufferLogInputStream) validates every
//! batch header against [`RECORD_OVERHEAD_V0`], the smallest overhead of any
//! message format, exactly as Java's does.

/// Length of the CRC field of a legacy record.
pub(crate) const CRC_LENGTH: i32 = 4;

/// Length of the magic byte of a legacy record.
pub(crate) const MAGIC_LENGTH: i32 = 1;

/// Length of the attributes byte of a legacy record.
pub(crate) const ATTRIBUTES_LENGTH: i32 = 1;

/// Length of the key size field of a legacy record.
pub(crate) const KEY_SIZE_LENGTH: i32 = 4;

/// Length of the value size field of a legacy record.
pub(crate) const VALUE_SIZE_LENGTH: i32 = 4;

/// The size of the record header of message format v0.
pub(crate) const HEADER_SIZE_V0: i32 = CRC_LENGTH + MAGIC_LENGTH + ATTRIBUTES_LENGTH;

/// The amount of overhead bytes in a record of message format v0 — the smallest
/// of any message format.
pub(crate) const RECORD_OVERHEAD_V0: i32 = HEADER_SIZE_V0 + KEY_SIZE_LENGTH + VALUE_SIZE_LENGTH;

#[cfg(test)]
mod tests {
    use super::*;

    /// The value Java's `ByteBufferLogInputStream` messages print
    /// (`Record size N is less than the minimum record overhead (14)`).
    #[test]
    fn test_record_overhead_v0() {
        assert_eq!(6, HEADER_SIZE_V0);
        assert_eq!(14, RECORD_OVERHEAD_V0);
    }
}
