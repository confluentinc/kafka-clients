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

//! Defines the strategies which clients can follow to deal with the situation
//! when none of the known nodes is available.
//!
//! Translated from `org.apache.kafka.clients.MetadataRecoveryStrategy`.

use std::fmt;

/// Defines the strategies which clients can follow to deal with the situation
/// when none of the known nodes is available.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MetadataRecoveryStrategy {
    /// No recovery strategy.
    None,
    /// Re-bootstrap from the initial bootstrap servers.
    Rebootstrap,
}

impl MetadataRecoveryStrategy {
    /// The string name of this strategy.
    pub fn name(&self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Rebootstrap => "rebootstrap",
        }
    }

    /// Parses a strategy from its string name (case-insensitive).
    ///
    /// # Errors
    /// Returns an error if the name does not match any known strategy.
    pub fn for_name(name: &str) -> Result<Self, String> {
        match name.to_uppercase().as_str() {
            "NONE" => Ok(Self::None),
            "REBOOTSTRAP" => Ok(Self::Rebootstrap),
            _ => Err(format!("Illegal MetadataRecoveryStrategy: {}", name)),
        }
    }
}

impl fmt::Display for MetadataRecoveryStrategy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.name())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_for_name_valid() {
        assert_eq!(
            MetadataRecoveryStrategy::for_name("none").unwrap(),
            MetadataRecoveryStrategy::None
        );
        assert_eq!(
            MetadataRecoveryStrategy::for_name("NONE").unwrap(),
            MetadataRecoveryStrategy::None
        );
        assert_eq!(
            MetadataRecoveryStrategy::for_name("rebootstrap").unwrap(),
            MetadataRecoveryStrategy::Rebootstrap
        );
        assert_eq!(
            MetadataRecoveryStrategy::for_name("REBOOTSTRAP").unwrap(),
            MetadataRecoveryStrategy::Rebootstrap
        );
    }

    #[test]
    fn test_for_name_invalid() {
        let err = MetadataRecoveryStrategy::for_name("invalid").unwrap_err();
        assert_eq!(err, "Illegal MetadataRecoveryStrategy: invalid");
    }

    #[test]
    fn test_name() {
        assert_eq!(MetadataRecoveryStrategy::None.name(), "none");
        assert_eq!(MetadataRecoveryStrategy::Rebootstrap.name(), "rebootstrap");
    }
}
