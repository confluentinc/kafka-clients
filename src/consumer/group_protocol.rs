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

//! The group protocol used by the consumer.
//!
//! Translated from `org.apache.kafka.clients.consumer.GroupProtocol`.

use std::fmt;
use std::str::FromStr;

use crate::common::KafkaError;

/// The group protocol that the consumer uses.
///
/// Corresponds to Java's `org.apache.kafka.clients.consumer.GroupProtocol`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum GroupProtocol {
    /// Classic group protocol.
    Classic,
    /// Consumer group protocol (KIP-848).
    Consumer,
}

impl GroupProtocol {
    /// The upper-case Java enum name (e.g. `"CLASSIC"`).
    ///
    /// Corresponds to Java `GroupProtocol.name`.
    pub fn name(self) -> &'static str {
        match self {
            Self::Classic => "CLASSIC",
            Self::Consumer => "CONSUMER",
        }
    }

    /// Case-insensitive lookup by string name.
    ///
    /// Corresponds to Java `GroupProtocol.of(String)`.
    ///
    /// # Errors
    ///
    /// Returns [`KafkaError::IllegalArgument`] if `name` is not a recognized
    /// group protocol (matching Java's `IllegalArgumentException` from
    /// `Enum.valueOf`).
    pub fn of(name: &str) -> Result<Self, KafkaError> {
        match name.to_ascii_uppercase().as_str() {
            "CLASSIC" => Ok(Self::Classic),
            "CONSUMER" => Ok(Self::Consumer),
            _ => Err(KafkaError::illegal_argument(format!(
                "No enum constant org.apache.kafka.clients.consumer.GroupProtocol.{name}"
            ))),
        }
    }
}

impl fmt::Display for GroupProtocol {
    /// Lower-case representation matching Java's
    /// `toString().toLowerCase(Locale.ROOT)` pattern used in
    /// `ConsumerConfig.DEFAULT_GROUP_PROTOCOL`.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Classic => "classic",
            Self::Consumer => "consumer",
        })
    }
}

impl FromStr for GroupProtocol {
    type Err = KafkaError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::of(s)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_name() {
        assert_eq!(GroupProtocol::Classic.name(), "CLASSIC");
        assert_eq!(GroupProtocol::Consumer.name(), "CONSUMER");
    }

    #[test]
    fn test_display_lowercase() {
        assert_eq!(GroupProtocol::Classic.to_string(), "classic");
        assert_eq!(GroupProtocol::Consumer.to_string(), "consumer");
    }

    #[test]
    fn test_of_case_insensitive() {
        assert_eq!(GroupProtocol::of("classic").unwrap(), GroupProtocol::Classic);
        assert_eq!(GroupProtocol::of("CLASSIC").unwrap(), GroupProtocol::Classic);
        assert_eq!(GroupProtocol::of("Classic").unwrap(), GroupProtocol::Classic);
        assert_eq!(GroupProtocol::of("consumer").unwrap(), GroupProtocol::Consumer);
        assert_eq!(GroupProtocol::of("CONSUMER").unwrap(), GroupProtocol::Consumer);
        assert_eq!(GroupProtocol::of("Consumer").unwrap(), GroupProtocol::Consumer);
    }

    #[test]
    fn test_of_invalid() {
        let err = GroupProtocol::of("invalid").unwrap_err();
        assert!(err.message().contains("GroupProtocol.invalid"), "got: {}", err.message());
    }

    #[test]
    fn test_from_str_roundtrip() {
        assert_eq!("classic".parse::<GroupProtocol>().unwrap(), GroupProtocol::Classic);
        assert_eq!("consumer".parse::<GroupProtocol>().unwrap(), GroupProtocol::Consumer);
    }
}
