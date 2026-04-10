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

//! Security protocol enum for Kafka connections.
//!
//! Translated from `org.apache.kafka.common.security.auth.SecurityProtocol`.

use std::fmt;
use std::str::FromStr;

/// Defines the security protocol used for communication with Kafka brokers.
///
/// Each protocol has a permanent, immutable numeric ID that matches the
/// wire-protocol values used by the Java client.
///
/// Translated from `org.apache.kafka.common.security.auth.SecurityProtocol`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SecurityProtocol {
    /// Un-authenticated, non-encrypted channel.
    Plaintext,
    /// SSL channel.
    Ssl,
    /// SASL authenticated, non-encrypted channel.
    SaslPlaintext,
    /// SASL authenticated, SSL channel.
    SaslSsl,
}

impl SecurityProtocol {
    /// All protocol variants in declaration order.
    const ALL: [SecurityProtocol; 4] = [
        SecurityProtocol::Plaintext,
        SecurityProtocol::Ssl,
        SecurityProtocol::SaslPlaintext,
        SecurityProtocol::SaslSsl,
    ];

    /// Returns the permanent and immutable numeric ID for this security protocol.
    ///
    /// This value matches `kafka.cluster.SecurityProtocol` and must never change.
    pub fn id(self) -> i16 {
        match self {
            SecurityProtocol::Plaintext => 0,
            SecurityProtocol::Ssl => 1,
            SecurityProtocol::SaslPlaintext => 2,
            SecurityProtocol::SaslSsl => 3,
        }
    }

    /// Returns the protocol name as used in configuration (e.g. `"PLAINTEXT"`, `"SASL_SSL"`).
    pub fn name(self) -> &'static str {
        match self {
            SecurityProtocol::Plaintext => "PLAINTEXT",
            SecurityProtocol::Ssl => "SSL",
            SecurityProtocol::SaslPlaintext => "SASL_PLAINTEXT",
            SecurityProtocol::SaslSsl => "SASL_SSL",
        }
    }

    /// Looks up a security protocol by its numeric ID.
    ///
    /// Returns `None` if no protocol has the given ID.
    pub fn for_id(id: i16) -> Option<Self> {
        match id {
            0 => Some(SecurityProtocol::Plaintext),
            1 => Some(SecurityProtocol::Ssl),
            2 => Some(SecurityProtocol::SaslPlaintext),
            3 => Some(SecurityProtocol::SaslSsl),
            _ => None,
        }
    }

    /// Case-insensitive lookup by protocol name.
    ///
    /// Returns `None` if no protocol matches the given name.
    pub fn for_name(name: &str) -> Option<Self> {
        let upper = name.to_uppercase();
        Self::ALL.iter().find(|p| p.name() == upper).copied()
    }

    /// Returns the names of all security protocols.
    pub fn names() -> Vec<&'static str> {
        Self::ALL.iter().map(|p| p.name()).collect()
    }
}

impl fmt::Display for SecurityProtocol {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.name())
    }
}

impl FromStr for SecurityProtocol {
    type Err = String;

    /// Parses a security protocol from its name (case-insensitive).
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        SecurityProtocol::for_name(s)
            .ok_or_else(|| format!("No enum constant SecurityProtocol.{}", s))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_id_values() {
        assert_eq!(SecurityProtocol::Plaintext.id(), 0);
        assert_eq!(SecurityProtocol::Ssl.id(), 1);
        assert_eq!(SecurityProtocol::SaslPlaintext.id(), 2);
        assert_eq!(SecurityProtocol::SaslSsl.id(), 3);
    }

    #[test]
    fn test_name_values() {
        assert_eq!(SecurityProtocol::Plaintext.name(), "PLAINTEXT");
        assert_eq!(SecurityProtocol::Ssl.name(), "SSL");
        assert_eq!(SecurityProtocol::SaslPlaintext.name(), "SASL_PLAINTEXT");
        assert_eq!(SecurityProtocol::SaslSsl.name(), "SASL_SSL");
    }

    #[test]
    fn test_for_id_roundtrip() {
        for id in 0..=3 {
            let protocol = SecurityProtocol::for_id(id).unwrap();
            assert_eq!(protocol.id(), id);
        }
    }

    #[test]
    fn test_for_id_invalid() {
        assert_eq!(SecurityProtocol::for_id(-1), None);
        assert_eq!(SecurityProtocol::for_id(4), None);
        assert_eq!(SecurityProtocol::for_id(100), None);
    }

    #[test]
    fn test_for_name_roundtrip() {
        let names = ["PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL"];
        for name in &names {
            let protocol = SecurityProtocol::for_name(name).unwrap();
            assert_eq!(protocol.name(), *name);
        }
    }

    #[test]
    fn test_for_name_case_insensitive() {
        assert_eq!(
            SecurityProtocol::for_name("plaintext"),
            Some(SecurityProtocol::Plaintext)
        );
        assert_eq!(
            SecurityProtocol::for_name("Plaintext"),
            Some(SecurityProtocol::Plaintext)
        );
        assert_eq!(
            SecurityProtocol::for_name("sasl_ssl"),
            Some(SecurityProtocol::SaslSsl)
        );
        assert_eq!(
            SecurityProtocol::for_name("Sasl_Plaintext"),
            Some(SecurityProtocol::SaslPlaintext)
        );
    }

    #[test]
    fn test_for_name_invalid() {
        assert_eq!(SecurityProtocol::for_name("INVALID"), None);
        assert_eq!(SecurityProtocol::for_name(""), None);
    }

    #[test]
    fn test_names() {
        let names = SecurityProtocol::names();
        assert_eq!(names.len(), 4);
        assert_eq!(names[0], "PLAINTEXT");
        assert_eq!(names[1], "SSL");
        assert_eq!(names[2], "SASL_PLAINTEXT");
        assert_eq!(names[3], "SASL_SSL");
    }

    #[test]
    fn test_display() {
        assert_eq!(format!("{}", SecurityProtocol::Plaintext), "PLAINTEXT");
        assert_eq!(format!("{}", SecurityProtocol::SaslSsl), "SASL_SSL");
    }

    #[test]
    fn test_from_str() {
        assert_eq!(
            "PLAINTEXT".parse::<SecurityProtocol>().unwrap(),
            SecurityProtocol::Plaintext
        );
        assert_eq!(
            "ssl".parse::<SecurityProtocol>().unwrap(),
            SecurityProtocol::Ssl
        );
        assert!("INVALID".parse::<SecurityProtocol>().is_err());
    }

    #[test]
    fn test_equality_and_hash() {
        use std::collections::HashSet;
        let mut set = HashSet::new();
        set.insert(SecurityProtocol::Plaintext);
        set.insert(SecurityProtocol::Plaintext);
        assert_eq!(set.len(), 1);
        set.insert(SecurityProtocol::Ssl);
        assert_eq!(set.len(), 2);
    }

    #[test]
    fn test_clone_and_copy() {
        let p = SecurityProtocol::SaslPlaintext;
        let p2 = p;
        assert_eq!(p, p2);
    }
}
