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

//! The leader election type used by `Admin::elect_leaders`.
//!
//! Translated from `org.apache.kafka.common.ElectionType`.

use crate::common::Error;

/// Options for `Admin::elect_leaders`.
///
/// Translated from `org.apache.kafka.common.ElectionType`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ElectionType {
    /// Elect the preferred replica as leader.
    Preferred,
    /// Elect any in-sync replica as leader (unclean election).
    Unclean,
}

impl ElectionType {
    /// Returns the wire-protocol byte value used to encode this election type.
    ///
    /// Mirrors the public `byte value` field on Java's `ElectionType`.
    pub fn value(&self) -> i8 {
        match self {
            Self::Preferred => 0,
            Self::Unclean => 1,
        }
    }

    /// Returns the [`ElectionType`] for the given wire-protocol byte value.
    ///
    /// Mirrors `ElectionType.valueOf(byte)`.
    ///
    /// # Errors
    ///
    /// Returns an [`Error`] (invalid argument) if `value` is not a valid
    /// election type, mirroring Java's `IllegalArgumentException`.
    pub fn value_of(value: i8) -> Result<Self, Error> {
        if value == Self::Preferred.value() {
            Ok(Self::Preferred)
        } else if value == Self::Unclean.value() {
            Ok(Self::Unclean)
        } else {
            Err(Error::illegal_argument(format!(
                "Value {value} must be one of [PREFERRED, UNCLEAN]"
            )))
        }
    }

    /// All election types, in declaration order.
    ///
    /// Mirrors `ElectionType.values()`.
    pub fn values() -> [Self; 2] {
        [Self::Preferred, Self::Unclean]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn value_matches_java() {
        assert_eq!(ElectionType::Preferred.value(), 0);
        assert_eq!(ElectionType::Unclean.value(), 1);
    }

    #[test]
    fn value_of_round_trips() {
        assert_eq!(ElectionType::value_of(0).unwrap(), ElectionType::Preferred);
        assert_eq!(ElectionType::value_of(1).unwrap(), ElectionType::Unclean);
    }

    #[test]
    fn value_of_invalid_is_error() {
        let err = ElectionType::value_of(2).unwrap_err();
        assert!(err.message().contains("Value 2 must be one of [PREFERRED, UNCLEAN]"));
    }
}
