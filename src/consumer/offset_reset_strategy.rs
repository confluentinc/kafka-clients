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

//! Deprecated offset reset strategy enum.
//!
//! Translated from `org.apache.kafka.clients.consumer.OffsetResetStrategy`.

use std::fmt;

/// Deprecated since Java 4.0; will be removed in a future release.
/// Not required by Kafka client users; no replacement is provided.
///
/// Corresponds to Java's deprecated
/// `org.apache.kafka.clients.consumer.OffsetResetStrategy` enum.
#[deprecated(note = "Not required by Kafka client users; no replacement is provided.")]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum OffsetResetStrategy {
    /// Reset to the latest offset.
    Latest,
    /// Reset to the earliest offset.
    Earliest,
    /// No automatic reset; raise an exception when no offset is found.
    ///
    /// Suffixed with `_` to avoid confusion with `Option::None` in pattern
    /// matches.
    None_,
}

#[allow(deprecated)]
impl fmt::Display for OffsetResetStrategy {
    /// Lower-case name matching Java's
    /// `toString().toLowerCase(Locale.ROOT)`.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Latest => "latest",
            Self::Earliest => "earliest",
            Self::None_ => "none",
        })
    }
}

#[cfg(test)]
#[allow(deprecated)]
mod tests {
    use super::*;

    #[test]
    fn test_display_lowercase() {
        assert_eq!(OffsetResetStrategy::Latest.to_string(), "latest");
        assert_eq!(OffsetResetStrategy::Earliest.to_string(), "earliest");
        assert_eq!(OffsetResetStrategy::None_.to_string(), "none");
    }
}
