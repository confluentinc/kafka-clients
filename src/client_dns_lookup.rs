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

//! Translation of `org.apache.kafka.clients.ClientDnsLookup`.

use std::fmt;

use crate::common::errors::KafkaError;

/// Mirrors the Java enum `org.apache.kafka.clients.ClientDnsLookup`.
///
/// The Java 4.2 enum has two variants: `USE_ALL_DNS_IPS` and
/// `RESOLVE_CANONICAL_BOOTSTRAP_SERVERS_ONLY`. There is no `DEFAULT`
/// variant in current Java; an older version had one but it has since
/// been removed.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub enum ClientDnsLookup {
    /// Use all DNS IP addresses returned for the host. After a
    /// disconnection, the next IP is tried.
    UseAllDnsIps,
    /// Resolve each bootstrap address into a list of canonical names.
    /// After bootstrap, behaves the same as `UseAllDnsIps`.
    ResolveCanonicalBootstrapServersOnly,
}

impl ClientDnsLookup {
    /// Lowercase config-name analogue of the Java enum's instance field
    /// (`use_all_dns_ips`, `resolve_canonical_bootstrap_servers_only`).
    /// This is what `toString()` returns in Java.
    pub fn as_config_str(&self) -> &'static str {
        match self {
            ClientDnsLookup::UseAllDnsIps => "use_all_dns_ips",
            ClientDnsLookup::ResolveCanonicalBootstrapServersOnly => "resolve_canonical_bootstrap_servers_only",
        }
    }

    /// Mirrors `ClientDnsLookup.forConfig(String)`. Java does
    /// `valueOf(config.toUpperCase(Locale.ROOT))` and throws
    /// `IllegalArgumentException` for unknown inputs.
    pub fn for_config(config: &str) -> Result<Self, KafkaError> {
        match config.to_ascii_uppercase().as_str() {
            "USE_ALL_DNS_IPS" => Ok(ClientDnsLookup::UseAllDnsIps),
            "RESOLVE_CANONICAL_BOOTSTRAP_SERVERS_ONLY" => Ok(ClientDnsLookup::ResolveCanonicalBootstrapServersOnly),
            other => Err(KafkaError::IllegalArgument(format!("No enum constant ClientDnsLookup.{other}"))),
        }
    }
}

impl fmt::Display for ClientDnsLookup {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_config_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn as_config_str_matches_java_to_string() {
        assert_eq!(ClientDnsLookup::UseAllDnsIps.as_config_str(), "use_all_dns_ips");
        assert_eq!(
            ClientDnsLookup::ResolveCanonicalBootstrapServersOnly.as_config_str(),
            "resolve_canonical_bootstrap_servers_only",
        );
    }

    #[test]
    fn display_matches_config_str() {
        assert_eq!(format!("{}", ClientDnsLookup::UseAllDnsIps), "use_all_dns_ips");
        assert_eq!(
            format!("{}", ClientDnsLookup::ResolveCanonicalBootstrapServersOnly),
            "resolve_canonical_bootstrap_servers_only",
        );
    }

    #[test]
    fn for_config_round_trip_uppercase() {
        assert_eq!(
            ClientDnsLookup::for_config("USE_ALL_DNS_IPS").unwrap(),
            ClientDnsLookup::UseAllDnsIps,
        );
        assert_eq!(
            ClientDnsLookup::for_config("RESOLVE_CANONICAL_BOOTSTRAP_SERVERS_ONLY").unwrap(),
            ClientDnsLookup::ResolveCanonicalBootstrapServersOnly,
        );
    }

    #[test]
    fn for_config_lowercase_accepted() {
        assert_eq!(
            ClientDnsLookup::for_config("use_all_dns_ips").unwrap(),
            ClientDnsLookup::UseAllDnsIps,
        );
        assert_eq!(
            ClientDnsLookup::for_config("resolve_canonical_bootstrap_servers_only").unwrap(),
            ClientDnsLookup::ResolveCanonicalBootstrapServersOnly,
        );
    }

    #[test]
    fn for_config_unknown_is_illegal_argument() {
        let err = ClientDnsLookup::for_config("random.value").unwrap_err();
        assert!(matches!(err, KafkaError::IllegalArgument(_)));
    }
}
