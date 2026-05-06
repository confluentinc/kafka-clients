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

//! Translation of `org.apache.kafka.common.network.ListenerName`.

use crate::common::errors::KafkaError;
use crate::common::security::auth::SecurityProtocol;

const CONFIG_STATIC_PREFIX: &str = "listener.name";

/// Translation of `org.apache.kafka.common.network.ListenerName`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ListenerName {
    value: String,
}

impl ListenerName {
    /// Construct a new instance with the given raw value. Mirrors the Java
    /// public constructor, which requires `value` to be non-null. In Rust
    /// the type system enforces non-null; we still validate against an
    /// empty string for consistency with [`Self::normalised`].
    pub fn new(value: impl Into<String>) -> Self {
        ListenerName { value: value.into() }
    }

    /// Mirrors `ListenerName.forSecurityProtocol(SecurityProtocol)` — the
    /// listener name uses the security protocol's name verbatim.
    pub fn for_security_protocol(security_protocol: SecurityProtocol) -> Self {
        ListenerName::new(security_protocol.name())
    }

    /// Mirrors `ListenerName.normalised(String)` — uppercases the value
    /// and rejects null/empty/whitespace-only inputs.
    pub fn normalised(value: &str) -> Result<Self, KafkaError> {
        if value.trim().is_empty() {
            return Err(KafkaError::Config(
                "The provided listener name is null or empty string".to_owned(),
            ));
        }
        Ok(ListenerName::new(value.to_ascii_uppercase()))
    }

    /// Mirrors `ListenerName.value()`.
    pub fn value(&self) -> &str {
        &self.value
    }

    /// Mirrors `ListenerName.configPrefix()`.
    pub fn config_prefix(&self) -> String {
        format!("{}.{}.", CONFIG_STATIC_PREFIX, self.value.to_ascii_lowercase())
    }

    /// Mirrors `ListenerName.saslMechanismConfigPrefix(String)`.
    pub fn sasl_mechanism_config_prefix(&self, sasl_mechanism: &str) -> String {
        format!("{}{}", self.config_prefix(), Self::sasl_mechanism_prefix(sasl_mechanism))
    }

    /// Mirrors the static `ListenerName.saslMechanismPrefix(String)`.
    pub fn sasl_mechanism_prefix(sasl_mechanism: &str) -> String {
        format!("{}.", sasl_mechanism.to_ascii_lowercase())
    }
}

impl std::fmt::Display for ListenerName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ListenerName({})", self.value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn for_security_protocol_uses_name() {
        let listener = ListenerName::for_security_protocol(SecurityProtocol::Plaintext);
        assert_eq!(listener.value(), "PLAINTEXT");
        let listener = ListenerName::for_security_protocol(SecurityProtocol::Ssl);
        assert_eq!(listener.value(), "SSL");
    }

    #[test]
    fn normalised_uppercases_value() {
        let listener = ListenerName::normalised("MyCustom").expect("ok");
        assert_eq!(listener.value(), "MYCUSTOM");
    }

    #[test]
    fn normalised_rejects_blank() {
        let err = ListenerName::normalised("   ").expect_err("blank");
        assert!(matches!(err, KafkaError::Config(_)));
        let err = ListenerName::normalised("").expect_err("empty");
        assert!(matches!(err, KafkaError::Config(_)));
    }

    #[test]
    fn config_prefix_lowercases_value() {
        let listener = ListenerName::new("INTERNAL");
        assert_eq!(listener.config_prefix(), "listener.name.internal.");
    }

    #[test]
    fn sasl_mechanism_prefix_round_trip() {
        let listener = ListenerName::new("EXTERNAL");
        assert_eq!(listener.sasl_mechanism_config_prefix("PLAIN"), "listener.name.external.plain.");
        assert_eq!(ListenerName::sasl_mechanism_prefix("OAUTHBEARER"), "oauthbearer.");
    }

    #[test]
    fn equals_and_hash() {
        use std::collections::HashSet;
        let a = ListenerName::new("EXTERNAL");
        let b = ListenerName::new("EXTERNAL");
        let c = ListenerName::new("INTERNAL");
        assert_eq!(a, b);
        assert_ne!(a, c);
        let mut set = HashSet::new();
        set.insert(a);
        assert!(set.contains(&b));
        assert!(!set.contains(&c));
    }

    #[test]
    fn display_format() {
        let listener = ListenerName::new("EXTERNAL");
        assert_eq!(listener.to_string(), "ListenerName(EXTERNAL)");
    }
}
