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

//! Translation of `org.apache.kafka.common.record.RecordValidationStats`.

use std::fmt;

/// Tracks resource usage during broker record validation for eventual reporting
/// in metrics. Record validation covers integrity checks on inbound data
/// (e.g. checksum verification), structural validation to make sure that
/// records are well-formed, and conversion between record formats if needed.
///
/// Mirrors Java's `RecordValidationStats` plain data class.
#[derive(Clone, Default, PartialEq, Eq, Hash)]
pub struct RecordValidationStats {
    temporary_memory_bytes: i64,
    num_records_converted: i32,
    conversion_time_nanos: i64,
}

impl RecordValidationStats {
    /// Mirrors Java's `RecordValidationStats.EMPTY` static constant.
    pub const EMPTY: RecordValidationStats =
        RecordValidationStats { temporary_memory_bytes: 0, num_records_converted: 0, conversion_time_nanos: 0 };

    /// Construct a fully-specified stats record.
    pub fn new(temporary_memory_bytes: i64, num_records_converted: i32, conversion_time_nanos: i64) -> Self {
        RecordValidationStats { temporary_memory_bytes, num_records_converted, conversion_time_nanos }
    }

    /// Add the contents of another stats record into this one (in place).
    /// Mirrors Java's `add(RecordValidationStats)`.
    pub fn add(&mut self, other: &RecordValidationStats) {
        self.temporary_memory_bytes += other.temporary_memory_bytes;
        self.num_records_converted += other.num_records_converted;
        self.conversion_time_nanos += other.conversion_time_nanos;
    }

    /// Returns the number of temporary memory bytes allocated to process the
    /// records. This size depends on whether the records need decompression
    /// and/or conversion:
    ///
    /// - Non compressed, no conversion: zero
    /// - Non compressed, with conversion: size of the converted buffer
    /// - Compressed, no conversion: size of the original buffer after decompression
    /// - Compressed, with conversion: size of the original buffer after
    ///   decompression + size of the converted buffer uncompressed
    pub fn temporary_memory_bytes(&self) -> i64 {
        self.temporary_memory_bytes
    }

    /// Number of records converted (e.g. up-converted to a newer format).
    pub fn num_records_converted(&self) -> i32 {
        self.num_records_converted
    }

    /// Time spent on conversion, in nanoseconds.
    pub fn conversion_time_nanos(&self) -> i64 {
        self.conversion_time_nanos
    }
}

impl fmt::Display for RecordValidationStats {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "RecordValidationStats(temporaryMemoryBytes={}, numRecordsConverted={}, conversionTimeNanos={})",
            self.temporary_memory_bytes, self.num_records_converted, self.conversion_time_nanos
        )
    }
}

impl fmt::Debug for RecordValidationStats {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

#[cfg(test)]
mod tests {
    // Java has no dedicated `RecordValidationStatsTest`. The class is
    // exercised via broker-side tests outside the client module, so we test
    // the basic plain-data semantics here.

    use super::*;

    #[test]
    fn empty_constant_is_zero() {
        let s = RecordValidationStats::EMPTY;
        assert_eq!(s.temporary_memory_bytes(), 0);
        assert_eq!(s.num_records_converted(), 0);
        assert_eq!(s.conversion_time_nanos(), 0);
    }

    #[test]
    fn default_matches_empty() {
        assert_eq!(RecordValidationStats::default(), RecordValidationStats::EMPTY);
    }

    #[test]
    fn new_constructs_with_values() {
        let s = RecordValidationStats::new(1024, 5, 42_000);
        assert_eq!(s.temporary_memory_bytes(), 1024);
        assert_eq!(s.num_records_converted(), 5);
        assert_eq!(s.conversion_time_nanos(), 42_000);
    }

    #[test]
    fn add_combines_in_place() {
        let mut a = RecordValidationStats::new(100, 1, 200);
        let b = RecordValidationStats::new(50, 2, 300);
        a.add(&b);
        assert_eq!(a, RecordValidationStats::new(150, 3, 500));
    }

    #[test]
    fn display_matches_java_format() {
        let s = RecordValidationStats::new(7, 8, 9);
        assert_eq!(
            format!("{s}"),
            "RecordValidationStats(temporaryMemoryBytes=7, numRecordsConverted=8, conversionTimeNanos=9)"
        );
    }
}
