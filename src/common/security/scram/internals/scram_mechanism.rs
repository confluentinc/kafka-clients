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

//! Internal SASL/SCRAM mechanism enum.
//!
//! Corresponds to
//! `org.apache.kafka.common.security.scram.internals.ScramMechanism`.
//!
//! This code is duplicated in `crate::admin::ScramMechanism`. The type field in
//! both files must match and must not change. Only the name→mechanism mapping
//! and the hash/MAC selection that the Admin `alterUserScramCredentials` path
//! needs are translated here (a narrow translation — the full SASL/SCRAM
//! handshake in the Java class is out of scope for the admin client).

/// Internal representation of a SASL/SCRAM mechanism, selecting the hash / MAC
/// algorithm used by [`super::ScramFormatter::hi`].
///
/// Only the two supported SCRAM mechanisms exist, mirroring the Java enum which
/// has exactly two members.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ScramMechanism {
    /// SCRAM-SHA-256 (`SHA-256` / `HmacSHA256`).
    ScramSha256,
    /// SCRAM-SHA-512 (`SHA-512` / `HmacSHA512`).
    ScramSha512,
}

impl ScramMechanism {
    /// Returns the mechanism for the given mechanism name, or `None` if the name
    /// does not correspond to a SCRAM mechanism.
    ///
    /// Mirrors `ScramMechanism.forMechanismName`, which returns `null` for an
    /// unknown name.
    pub(crate) fn for_mechanism_name(mechanism_name: &str) -> Option<ScramMechanism> {
        match mechanism_name {
            "SCRAM-SHA-256" => Some(ScramMechanism::ScramSha256),
            "SCRAM-SHA-512" => Some(ScramMechanism::ScramSha512),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn for_mechanism_name_maps_supported_mechanisms() {
        assert_eq!(
            ScramMechanism::for_mechanism_name("SCRAM-SHA-256"),
            Some(ScramMechanism::ScramSha256)
        );
        assert_eq!(
            ScramMechanism::for_mechanism_name("SCRAM-SHA-512"),
            Some(ScramMechanism::ScramSha512)
        );
        assert_eq!(ScramMechanism::for_mechanism_name("UNKNOWN"), None);
        assert_eq!(ScramMechanism::for_mechanism_name("scram-sha-256"), None);
    }
}
