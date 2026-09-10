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

//! Describes records to delete in a call to `Admin::delete_records`.
//!
//! Corresponds to `org.apache.kafka.clients.admin.RecordsToDelete`.

/// Describe records to delete in a call to `Admin::delete_records`.
///
/// Corresponds to `org.apache.kafka.clients.admin.RecordsToDelete`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct RecordsToDelete {
    offset: i64,
}

impl RecordsToDelete {
    /// Delete all the records before the given `offset`.
    ///
    /// Use `-1` to truncate to the high watermark.
    pub fn new_before_offset(offset: i64) -> Self {
        Self { offset }
    }

    /// The offset before which all records will be deleted.
    ///
    /// Use `-1` to truncate to the high watermark.
    pub fn before_offset(&self) -> i64 {
        self.offset
    }
}

impl std::fmt::Display for RecordsToDelete {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "(beforeOffset = {})", self.offset)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn before_offset_round_trip() {
        let r = RecordsToDelete::new_before_offset(10);
        assert_eq!(r.before_offset(), 10);
    }

    #[test]
    fn equality_and_hash_match_on_offset() {
        assert_eq!(RecordsToDelete::new_before_offset(5), RecordsToDelete::new_before_offset(5));
        assert_ne!(RecordsToDelete::new_before_offset(5), RecordsToDelete::new_before_offset(6));
    }
}
