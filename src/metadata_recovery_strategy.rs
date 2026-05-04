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

//! Translation of `org.apache.kafka.clients.MetadataRecoveryStrategy`.

use crate::common::errors::KafkaError;

/// Defines the strategies which clients can follow to deal with the
/// situation when none of the known nodes is available.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub enum MetadataRecoveryStrategy {
    /// Mirrors Java `NONE`.
    None,
    /// Mirrors Java `REBOOTSTRAP`.
    Rebootstrap,
}

impl MetadataRecoveryStrategy {
    /// The lowercase string identifier used in client configs. Mirrors the
    /// `name` field on the Java enum constants.
    pub fn name(&self) -> &'static str {
        match self {
            MetadataRecoveryStrategy::None => "none",
            MetadataRecoveryStrategy::Rebootstrap => "rebootstrap",
        }
    }

    /// Mirrors `MetadataRecoveryStrategy.forName(String)`. Returns
    /// `KafkaError::IllegalArgument` for unknown / null inputs.
    pub fn from_name(name: &str) -> Result<Self, KafkaError> {
        // Java does `valueOf(name.toUpperCase(Locale.ROOT))` and catches
        // `IllegalArgumentException`. We match against the canonical
        // constant names directly.
        match name.to_ascii_uppercase().as_str() {
            "NONE" => Ok(MetadataRecoveryStrategy::None),
            "REBOOTSTRAP" => Ok(MetadataRecoveryStrategy::Rebootstrap),
            other => Err(KafkaError::IllegalArgument(format!(
                "Illegal MetadataRecoveryStrategy: {other}"
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn name_matches_java_lowercase() {
        assert_eq!(MetadataRecoveryStrategy::None.name(), "none");
        assert_eq!(MetadataRecoveryStrategy::Rebootstrap.name(), "rebootstrap");
    }

    #[test]
    fn from_name_round_trip() {
        assert_eq!(
            MetadataRecoveryStrategy::from_name("none").unwrap(),
            MetadataRecoveryStrategy::None
        );
        assert_eq!(
            MetadataRecoveryStrategy::from_name("NONE").unwrap(),
            MetadataRecoveryStrategy::None
        );
        assert_eq!(
            MetadataRecoveryStrategy::from_name("rebootstrap").unwrap(),
            MetadataRecoveryStrategy::Rebootstrap
        );
        assert_eq!(
            MetadataRecoveryStrategy::from_name("REBOOTSTRAP").unwrap(),
            MetadataRecoveryStrategy::Rebootstrap
        );
    }

    #[test]
    fn from_name_unknown_is_illegal_argument() {
        let err = MetadataRecoveryStrategy::from_name("bogus").unwrap_err();
        assert!(matches!(err, KafkaError::IllegalArgument(_)));
        let msg = err.to_string();
        assert!(msg.contains("Illegal MetadataRecoveryStrategy: BOGUS"), "got: {msg}");
    }
}
