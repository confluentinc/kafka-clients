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

//! Public SASL/SCRAM mechanism representation.
//!
//! Corresponds to `org.apache.kafka.clients.admin.ScramMechanism`.

/// Representation of a SASL/SCRAM Mechanism.
///
/// See [KIP-554: Add Broker-side SCRAM Config API](https://cwiki.apache.org/confluence/display/KAFKA/KIP-554%3A+Add+Broker-side+SCRAM+Config+API).
///
/// This code is duplicated in
/// `crate::common::security::scram::internals::ScramMechanism`. The type field
/// in both files must match and must not change. The type field is used both for
/// passing `ScramCredentialUpsertion` and for the internal
/// `UserScramCredentialRecord`. Do not change the type field.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ScramMechanism {
    /// An unknown / unsupported mechanism (type indicator `0`).
    Unknown,
    /// SCRAM-SHA-256 (type indicator `1`).
    ScramSha256,
    /// SCRAM-SHA-512 (type indicator `2`).
    ScramSha512,
}

impl ScramMechanism {
    /// All mechanisms, in declaration order (mirrors Java's `values()`).
    const VALUES: [ScramMechanism; 3] = [
        ScramMechanism::Unknown,
        ScramMechanism::ScramSha256,
        ScramMechanism::ScramSha512,
    ];

    /// Returns the instance corresponding to the given type indicator, otherwise
    /// [`ScramMechanism::Unknown`].
    ///
    /// Mirrors `ScramMechanism.fromType(byte)`.
    pub fn from_type(r#type: i8) -> ScramMechanism {
        for mechanism in ScramMechanism::VALUES {
            if mechanism.r#type() == r#type {
                return mechanism;
            }
        }
        ScramMechanism::Unknown
    }

    /// Returns the mechanism corresponding to the SASL SCRAM mechanism name,
    /// otherwise [`ScramMechanism::Unknown`].
    ///
    /// Mirrors `ScramMechanism.fromMechanismName(String)`.
    pub fn from_mechanism_name(mechanism_name: &str) -> ScramMechanism {
        ScramMechanism::VALUES
            .into_iter()
            .find(|mechanism| mechanism.mechanism_name() == mechanism_name)
            .unwrap_or(ScramMechanism::Unknown)
    }

    /// Returns the SASL SCRAM mechanism name (e.g. `SCRAM-SHA-256`).
    ///
    /// Derived from the variant name with `_` replaced by `-`, matching Java's
    /// `toString().replace('_', '-')`.
    ///
    /// Mirrors `ScramMechanism.mechanismName()`.
    pub fn mechanism_name(self) -> &'static str {
        match self {
            ScramMechanism::Unknown => "UNKNOWN",
            ScramMechanism::ScramSha256 => "SCRAM-SHA-256",
            ScramMechanism::ScramSha512 => "SCRAM-SHA-512",
        }
    }

    /// Returns the type indicator for this SASL SCRAM mechanism.
    ///
    /// Mirrors `ScramMechanism.type()`.
    pub fn r#type(self) -> i8 {
        match self {
            ScramMechanism::Unknown => 0,
            ScramMechanism::ScramSha256 => 1,
            ScramMechanism::ScramSha512 => 2,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Mirrors `ScramMechanismTest.testFromMechanismName`.
    #[test]
    fn test_from_mechanism_name() {
        assert_eq!(ScramMechanism::Unknown, ScramMechanism::from_mechanism_name("UNKNOWN"));
        assert_eq!(
            ScramMechanism::ScramSha256,
            ScramMechanism::from_mechanism_name("SCRAM-SHA-256")
        );
        assert_eq!(
            ScramMechanism::ScramSha512,
            ScramMechanism::from_mechanism_name("SCRAM-SHA-512")
        );
        assert_eq!(ScramMechanism::Unknown, ScramMechanism::from_mechanism_name("some string"));
        assert_eq!(ScramMechanism::Unknown, ScramMechanism::from_mechanism_name("scram-sha-256"));
    }

    #[test]
    fn from_type_round_trips() {
        assert_eq!(ScramMechanism::Unknown, ScramMechanism::from_type(0));
        assert_eq!(ScramMechanism::ScramSha256, ScramMechanism::from_type(1));
        assert_eq!(ScramMechanism::ScramSha512, ScramMechanism::from_type(2));
        assert_eq!(ScramMechanism::Unknown, ScramMechanism::from_type(9));
    }
}
