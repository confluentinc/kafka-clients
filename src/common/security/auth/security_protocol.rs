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
//! Milestone 1 only ships `PLAINTEXT` and `SSL`. The Java enum also defines
//! `SASL_PLAINTEXT` (id `2`) and `SASL_SSL` (id `3`); those are deferred
//! to Phase 9 when SASL/Kerberos/OAuth/SCRAM channels are wired up.
//! Producer config validation must reject `SASL_PLAINTEXT` / `SASL_SSL`
//! values with a [`crate::common::errors::KafkaError::Config`] error
//! until that phase lands.

/// On-wire id for the `PLAINTEXT` security protocol. Mirrors
/// `SecurityProtocol.PLAINTEXT.id`. Stable as part of Kafka's wire
/// protocol — see `kafka.cluster.SecurityProtocol`.
pub const ID_PLAINTEXT: i16 = 0;

/// On-wire id for the `SSL` security protocol. Mirrors
/// `SecurityProtocol.SSL.id`.
pub const ID_SSL: i16 = 1;

/// Reserved on-wire id for `SASL_PLAINTEXT` (Phase 9). Constant kept so
/// config validators can reject the id explicitly.
pub const ID_SASL_PLAINTEXT_RESERVED: i16 = 2;

/// Reserved on-wire id for `SASL_SSL` (Phase 9).
pub const ID_SASL_SSL_RESERVED: i16 = 3;

/// Translation of `org.apache.kafka.common.security.auth.SecurityProtocol`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SecurityProtocol {
    /// Un-authenticated, non-encrypted channel.
    Plaintext,
    /// SSL-encrypted channel.
    Ssl,
}

impl SecurityProtocol {
    /// Permanent and immutable on-wire id. Mirrors `SecurityProtocol.id`.
    pub fn id(&self) -> i16 {
        match self {
            SecurityProtocol::Plaintext => ID_PLAINTEXT,
            SecurityProtocol::Ssl => ID_SSL,
        }
    }

    /// Name as used in client configuration. Mirrors `SecurityProtocol.name`.
    pub fn name(&self) -> &'static str {
        match self {
            SecurityProtocol::Plaintext => "PLAINTEXT",
            SecurityProtocol::Ssl => "SSL",
        }
    }

    /// All names defined by this enum. Mirrors `SecurityProtocol.names()`.
    /// Only the variants implemented in this milestone are returned —
    /// `SASL_PLAINTEXT` and `SASL_SSL` will join when Phase 9 lands.
    pub fn names() -> &'static [&'static str] {
        &["PLAINTEXT", "SSL"]
    }

    /// Lookup by on-wire id. Mirrors `SecurityProtocol.forId(short)`.
    /// Returns `None` for unknown ids and for the reserved SASL ids that
    /// are not yet implemented.
    pub fn for_id(id: i16) -> Option<SecurityProtocol> {
        match id {
            ID_PLAINTEXT => Some(SecurityProtocol::Plaintext),
            ID_SSL => Some(SecurityProtocol::Ssl),
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
            _ => None,
        }
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
    }

    #[test]
    fn for_id_round_trip() {
        assert_eq!(SecurityProtocol::for_id(0), Some(SecurityProtocol::Plaintext));
        assert_eq!(SecurityProtocol::for_id(1), Some(SecurityProtocol::Ssl));
        assert_eq!(SecurityProtocol::for_id(2), None);
        assert_eq!(SecurityProtocol::for_id(3), None);
        assert_eq!(SecurityProtocol::for_id(99), None);
    }

    #[test]
    fn for_name_is_case_insensitive() {
        assert_eq!(SecurityProtocol::for_name("PLAINTEXT"), Some(SecurityProtocol::Plaintext));
        assert_eq!(SecurityProtocol::for_name("plaintext"), Some(SecurityProtocol::Plaintext));
        assert_eq!(SecurityProtocol::for_name("Ssl"), Some(SecurityProtocol::Ssl));
        assert_eq!(SecurityProtocol::for_name("SASL_PLAINTEXT"), None);
        assert_eq!(SecurityProtocol::for_name("UNKNOWN"), None);
    }

    #[test]
    fn names_lists_implemented_variants() {
        assert_eq!(SecurityProtocol::names(), &["PLAINTEXT", "SSL"]);
    }
}
