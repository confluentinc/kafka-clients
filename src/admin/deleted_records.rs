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

//! Represents information about deleted records.
//!
//! Corresponds to `org.apache.kafka.clients.admin.DeletedRecords`.

/// Represents information about deleted records.
///
/// Corresponds to `org.apache.kafka.clients.admin.DeletedRecords`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DeletedRecords {
    low_watermark: i64,
}

impl DeletedRecords {
    /// Create an instance of this class with the provided parameters.
    ///
    /// `low_watermark` is the "low watermark" for the topic partition on which
    /// the deletion was executed.
    pub fn new(low_watermark: i64) -> Self {
        Self { low_watermark }
    }

    /// Return the "low watermark" for the topic partition on which the deletion
    /// was executed.
    pub fn low_watermark(&self) -> i64 {
        self.low_watermark
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn low_watermark_round_trip() {
        assert_eq!(DeletedRecords::new(42).low_watermark(), 42);
    }
}
