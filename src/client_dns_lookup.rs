// Copyright 2026 Confluent Inc.
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

//! Controls how the client uses DNS lookups for bootstrap servers.
//!
//! Translated from `org.apache.kafka.clients.ClientDnsLookup`.

use std::fmt;

use crate::common::Error;

/// Controls how the client uses DNS lookups (the `client.dns.lookup`
/// configuration).
///
/// Translated from the Java enum `org.apache.kafka.clients.ClientDnsLookup`,
/// which in Apache Kafka 4.3.1 has exactly two constants.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum ClientDnsLookup {
    /// `use_all_dns_ips` (the default): connect to each returned IP address in
    /// sequence until a successful connection is established. After a
    /// disconnection, the next IP is used. Once all IPs have been used once,
    /// the client resolves the IP(s) from the hostname again.
    #[default]
    UseAllDnsIps,
    /// `resolve_canonical_bootstrap_servers_only`: resolve each bootstrap
    /// address into a list of canonical names. After the bootstrap phase, this
    /// behaves the same as [`ClientDnsLookup::UseAllDnsIps`].
    ///
    /// See `ClientUtils::parse_and_validate_addresses` for how the canonical
    /// name of each resolved address is derived in this translation.
    ResolveCanonicalBootstrapServersOnly,
}

impl ClientDnsLookup {
    /// Returns the constant for the given configuration value.
    ///
    /// Translated from `ClientDnsLookup.forConfig(String)`, which is
    /// `ClientDnsLookup.valueOf(config.toUpperCase(Locale.ROOT))` — so the
    /// match is case-insensitive.
    ///
    /// # Errors
    ///
    /// Returns [`Error::LocalIllegalArgument`] if `config` does not name a
    /// constant, matching the `IllegalArgumentException` Java's `Enum.valueOf`
    /// throws (with the same message text).
    pub fn for_config(config: &str) -> Result<Self, Error> {
        let upper = config.to_uppercase();
        match upper.as_str() {
            "USE_ALL_DNS_IPS" => Ok(Self::UseAllDnsIps),
            "RESOLVE_CANONICAL_BOOTSTRAP_SERVERS_ONLY" => Ok(Self::ResolveCanonicalBootstrapServersOnly),
            _ => Err(Error::local_illegal_argument(format!(
                "No enum constant org.apache.kafka.clients.ClientDnsLookup.{upper}"
            ))),
        }
    }

    /// Parses and validates a `client.dns.lookup` configuration value.
    ///
    /// `ProducerConfig`, `ConsumerConfig` and `AdminClientConfig` all define
    /// the key identically in Java:
    ///
    /// ```java
    /// .define(CLIENT_DNS_LOOKUP_CONFIG, Type.STRING,
    ///         ClientDnsLookup.USE_ALL_DNS_IPS.toString(),
    ///         in(ClientDnsLookup.USE_ALL_DNS_IPS.toString(),
    ///            ClientDnsLookup.RESOLVE_CANONICAL_BOOTSTRAP_SERVERS_ONLY.toString()), ...)
    /// ```
    ///
    /// `ConfigDef.ValidString` is case-**sensitive**, so only the exact
    /// lower-case names are accepted here (unlike [`Self::for_config`]). This
    /// helper is the shared translation of that validator; Java has no
    /// separate method for it because `ConfigDef` performs the check.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Config`] with `ValidString`'s message for any other
    /// value.
    pub(crate) fn parse_config_value(value: &str) -> Result<Self, Error> {
        match value {
            "use_all_dns_ips" => Ok(Self::UseAllDnsIps),
            "resolve_canonical_bootstrap_servers_only" => Ok(Self::ResolveCanonicalBootstrapServersOnly),
            _ => Err(Error::config_name_value_message(
                crate::CommonClientConfigs::CLIENT_DNS_LOOKUP_CONFIG,
                value,
                "String must be one of: use_all_dns_ips, resolve_canonical_bootstrap_servers_only",
            )),
        }
    }
}

impl fmt::Display for ClientDnsLookup {
    /// Translated from `ClientDnsLookup.toString()`, which returns the enum's
    /// lower-case `clientDnsLookup` field (the configuration value).
    ///
    /// Java's implicit `Enum.name()` (the upper-case constant name, e.g.
    /// `USE_ALL_DNS_IPS`) is not translated: the Rust variant name and the
    /// derived `Debug` already identify the constant, and nothing in the
    /// client reads `name()`.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::UseAllDnsIps => "use_all_dns_ips",
            Self::ResolveCanonicalBootstrapServersOnly => "resolve_canonical_bootstrap_servers_only",
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_is_use_all_dns_ips() {
        assert_eq!(ClientDnsLookup::default(), ClientDnsLookup::UseAllDnsIps);
    }

    #[test]
    fn test_to_string_matches_java() {
        assert_eq!(ClientDnsLookup::UseAllDnsIps.to_string(), "use_all_dns_ips");
        assert_eq!(
            ClientDnsLookup::ResolveCanonicalBootstrapServersOnly.to_string(),
            "resolve_canonical_bootstrap_servers_only"
        );
    }

    #[test]
    fn test_for_config_is_case_insensitive() {
        for value in ["use_all_dns_ips", "USE_ALL_DNS_IPS", "Use_All_Dns_Ips"] {
            assert_eq!(ClientDnsLookup::for_config(value).unwrap(), ClientDnsLookup::UseAllDnsIps);
        }
        for value in [
            "resolve_canonical_bootstrap_servers_only",
            "RESOLVE_CANONICAL_BOOTSTRAP_SERVERS_ONLY",
        ] {
            assert_eq!(
                ClientDnsLookup::for_config(value).unwrap(),
                ClientDnsLookup::ResolveCanonicalBootstrapServersOnly
            );
        }
    }

    #[test]
    fn test_for_config_round_trips_to_string() {
        for lookup in [
            ClientDnsLookup::UseAllDnsIps,
            ClientDnsLookup::ResolveCanonicalBootstrapServersOnly,
        ] {
            assert_eq!(ClientDnsLookup::for_config(&lookup.to_string()).unwrap(), lookup);
        }
    }

    /// Mirrors the `IllegalArgumentException` from `Enum.valueOf` that
    /// `ClientUtilsTest.testInvalidConfig` expects.
    #[test]
    fn test_for_config_unknown_value() {
        let err = ClientDnsLookup::for_config("random.value").unwrap_err();
        match err {
            Error::LocalIllegalArgument(e) => {
                assert_eq!(
                    e.message(),
                    "No enum constant org.apache.kafka.clients.ClientDnsLookup.RANDOM.VALUE"
                );
            },
            other => panic!("Expected LocalIllegalArgument, got {other:?}"),
        }
    }

    #[test]
    fn test_parse_config_value_accepts_exact_names() {
        assert_eq!(
            ClientDnsLookup::parse_config_value("use_all_dns_ips").unwrap(),
            ClientDnsLookup::UseAllDnsIps
        );
        assert_eq!(
            ClientDnsLookup::parse_config_value("resolve_canonical_bootstrap_servers_only").unwrap(),
            ClientDnsLookup::ResolveCanonicalBootstrapServersOnly
        );
    }

    /// `ConfigDef.ValidString` is case-sensitive, so upper case is rejected at
    /// config time even though `forConfig` itself would accept it.
    #[test]
    fn test_parse_config_value_rejects_other_values() {
        for value in ["USE_ALL_DNS_IPS", "default", ""] {
            let err = ClientDnsLookup::parse_config_value(value).unwrap_err();
            assert_eq!(
                err.message(),
                format!(
                    "Invalid value {value} for configuration client.dns.lookup: String must be one of: \
                     use_all_dns_ips, resolve_canonical_bootstrap_servers_only"
                )
            );
        }
    }
}
