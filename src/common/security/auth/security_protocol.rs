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

//! Translation of `org.apache.kafka.common.security.auth.SecurityProtocol`.
//!
//! Phase 9 adds SASL_PLAINTEXT (id 2) and SASL_SSL (id 3) variants —
//! both gated on the PLAIN mechanism only at this milestone. SCRAM,
//! OAUTHBEARER, Kerberos/GSSAPI are rejected at the config-validation
//! boundary (Phase 9b).

/// On-wire id for the `PLAINTEXT` security protocol. Mirrors
/// `SecurityProtocol.PLAINTEXT.id`. Stable as part of Kafka's wire
/// protocol — see `kafka.cluster.SecurityProtocol`.
pub const ID_PLAINTEXT: i16 = 0;

/// On-wire id for the `SSL` security protocol. Mirrors
/// `SecurityProtocol.SSL.id`.
pub const ID_SSL: i16 = 1;

/// On-wire id for the `SASL_PLAINTEXT` security protocol. Mirrors
/// `SecurityProtocol.SASL_PLAINTEXT.id`.
pub const ID_SASL_PLAINTEXT: i16 = 2;

/// On-wire id for the `SASL_SSL` security protocol. Mirrors
/// `SecurityProtocol.SASL_SSL.id`.
pub const ID_SASL_SSL: i16 = 3;

/// Translation of `org.apache.kafka.common.security.auth.SecurityProtocol`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SecurityProtocol {
    /// Un-authenticated, non-encrypted channel.
    Plaintext,
    /// SSL-encrypted channel.
    Ssl,
    /// SASL authentication on un-encrypted transport.
    SaslPlaintext,
    /// SASL authentication on SSL-encrypted transport.
    SaslSsl,
}

impl SecurityProtocol {
    /// Permanent and immutable on-wire id. Mirrors `SecurityProtocol.id`.
    pub fn id(&self) -> i16 {
        match self {
            SecurityProtocol::Plaintext => ID_PLAINTEXT,
            SecurityProtocol::Ssl => ID_SSL,
            SecurityProtocol::SaslPlaintext => ID_SASL_PLAINTEXT,
            SecurityProtocol::SaslSsl => ID_SASL_SSL,
        }
    }

    /// Name as used in client configuration. Mirrors `SecurityProtocol.name`.
    pub fn name(&self) -> &'static str {
        match self {
            SecurityProtocol::Plaintext => "PLAINTEXT",
            SecurityProtocol::Ssl => "SSL",
            SecurityProtocol::SaslPlaintext => "SASL_PLAINTEXT",
            SecurityProtocol::SaslSsl => "SASL_SSL",
        }
    }

    /// All names defined by this enum. Mirrors `SecurityProtocol.names()`.
    pub fn names() -> &'static [&'static str] {
        &["PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL"]
    }

    /// Lookup by on-wire id. Mirrors `SecurityProtocol.forId(short)`.
    pub fn for_id(id: i16) -> Option<SecurityProtocol> {
        match id {
            ID_PLAINTEXT => Some(SecurityProtocol::Plaintext),
            ID_SSL => Some(SecurityProtocol::Ssl),
            ID_SASL_PLAINTEXT => Some(SecurityProtocol::SaslPlaintext),
            ID_SASL_SSL => Some(SecurityProtocol::SaslSsl),
            _ => None,
        }
    }

    /// Case-insensitive lookup by name. Mirrors
    /// `SecurityProtocol.forName(String)`. Returns `None` for unknown
    /// names; Java throws `IllegalArgumentException` from `Enum.valueOf`,
    /// but in Rust we keep the more conservative `Option` return so
    /// callers can produce a typed [`crate::common::errors::KafkaError::Config`].
    pub fn for_name(name: &str) -> Option<SecurityProtocol> {
        match name.to_ascii_uppercase().as_str() {
            "PLAINTEXT" => Some(SecurityProtocol::Plaintext),
            "SSL" => Some(SecurityProtocol::Ssl),
            "SASL_PLAINTEXT" => Some(SecurityProtocol::SaslPlaintext),
            "SASL_SSL" => Some(SecurityProtocol::SaslSsl),
            _ => None,
        }
    }

    /// `true` iff this protocol layers SASL on top of the underlying
    /// transport. Mirrors Java's check
    /// `protocol == SASL_PLAINTEXT || protocol == SASL_SSL`.
    pub fn is_sasl(&self) -> bool {
        matches!(self, SecurityProtocol::SaslPlaintext | SecurityProtocol::SaslSsl)
    }

    /// `true` iff this protocol uses TLS for the underlying transport.
    /// Mirrors Java's check `protocol == SSL || protocol == SASL_SSL`.
    pub fn uses_ssl(&self) -> bool {
        matches!(self, SecurityProtocol::Ssl | SecurityProtocol::SaslSsl)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn id_and_name_match_java_enum() {
        assert_eq!(SecurityProtocol::Plaintext.id(), 0);
        assert_eq!(SecurityProtocol::Plaintext.name(), "PLAINTEXT");
        assert_eq!(SecurityProtocol::Ssl.id(), 1);
        assert_eq!(SecurityProtocol::Ssl.name(), "SSL");
        assert_eq!(SecurityProtocol::SaslPlaintext.id(), 2);
        assert_eq!(SecurityProtocol::SaslPlaintext.name(), "SASL_PLAINTEXT");
        assert_eq!(SecurityProtocol::SaslSsl.id(), 3);
        assert_eq!(SecurityProtocol::SaslSsl.name(), "SASL_SSL");
    }

    #[test]
    fn for_id_round_trip() {
        assert_eq!(SecurityProtocol::for_id(0), Some(SecurityProtocol::Plaintext));
        assert_eq!(SecurityProtocol::for_id(1), Some(SecurityProtocol::Ssl));
        assert_eq!(SecurityProtocol::for_id(2), Some(SecurityProtocol::SaslPlaintext));
        assert_eq!(SecurityProtocol::for_id(3), Some(SecurityProtocol::SaslSsl));
        assert_eq!(SecurityProtocol::for_id(99), None);
    }

    #[test]
    fn for_name_is_case_insensitive() {
        assert_eq!(SecurityProtocol::for_name("PLAINTEXT"), Some(SecurityProtocol::Plaintext));
        assert_eq!(SecurityProtocol::for_name("plaintext"), Some(SecurityProtocol::Plaintext));
        assert_eq!(SecurityProtocol::for_name("Ssl"), Some(SecurityProtocol::Ssl));
        assert_eq!(
            SecurityProtocol::for_name("SASL_PLAINTEXT"),
            Some(SecurityProtocol::SaslPlaintext)
        );
        assert_eq!(SecurityProtocol::for_name("sasl_ssl"), Some(SecurityProtocol::SaslSsl));
        assert_eq!(SecurityProtocol::for_name("UNKNOWN"), None);
    }

    #[test]
    fn names_lists_implemented_variants() {
        assert_eq!(SecurityProtocol::names(), &["PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL"]);
    }

    #[test]
    fn is_sasl_and_uses_ssl_predicates() {
        assert!(!SecurityProtocol::Plaintext.is_sasl());
        assert!(!SecurityProtocol::Ssl.is_sasl());
        assert!(SecurityProtocol::SaslPlaintext.is_sasl());
        assert!(SecurityProtocol::SaslSsl.is_sasl());

        assert!(!SecurityProtocol::Plaintext.uses_ssl());
        assert!(SecurityProtocol::Ssl.uses_ssl());
        assert!(!SecurityProtocol::SaslPlaintext.uses_ssl());
        assert!(SecurityProtocol::SaslSsl.uses_ssl());
    }
}
