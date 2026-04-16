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

//! SASL configuration for Kafka connections.
//!
//! Translated from `org.apache.kafka.common.config.SaslConfigs`.
//!
//! In Java this class has 40+ constants, most for Kerberos and OAuth (out of
//! scope for the current milestone). For SASL PLAIN we only need a minimal
//! subset.
//!
//! **Excluded Java constants (out of scope):**
//! - All `SASL_KERBEROS_*` — Kerberos not supported
//! - All `SASL_OAUTHBEARER_*` — OAuth not supported
//! - All `SASL_LOGIN_REFRESH_*` — Token refresh not supported
//! - `SASL_CLIENT_CALLBACK_HANDLER_CLASS` — No pluggable callback handlers
//! - `SASL_LOGIN_CLASS` — No pluggable login implementations

// ---------------------------------------------------------------------------
// Config key constants (matching Java SaslConfigs constant values)
// ---------------------------------------------------------------------------

/// Config key: `sasl.mechanism`.
///
/// SASL mechanism used for client connections. This may be any mechanism for
/// which a security provider is available. GSSAPI is the default mechanism.
pub const SASL_MECHANISM: &str = "sasl.mechanism";

/// Config key: `sasl.jaas.config`.
///
/// JAAS login context parameters for SASL connections in the format used by
/// JAAS configuration files.
pub const SASL_JAAS_CONFIG: &str = "sasl.jaas.config";

/// The GSSAPI (Kerberos) mechanism name.
pub const GSSAPI_MECHANISM: &str = "GSSAPI";

/// Default SASL mechanism (matches Java `DEFAULT_SASL_MECHANISM`).
pub const DEFAULT_SASL_MECHANISM: &str = GSSAPI_MECHANISM;

// ---------------------------------------------------------------------------
// SaslConfig struct
// ---------------------------------------------------------------------------

/// SASL configuration for Kafka connections.
///
/// Maps to the client-relevant subset of Java's `SaslConfigs`.
/// Currently supports PLAIN mechanism only.
///
/// Translated from `org.apache.kafka.common.config.SaslConfigs`.
#[derive(Debug, Clone)]
pub struct SaslConfig {
    /// SASL mechanism. Default: `"GSSAPI"` (matches Java `DEFAULT_SASL_MECHANISM`).
    /// Corresponds to `sasl.mechanism`.
    pub mechanism: String,

    /// JAAS configuration string for embedded credentials.
    /// Corresponds to `sasl.jaas.config`.
    ///
    /// Example:
    /// ```text
    /// org.apache.kafka.common.security.plain.PlainLoginModule required
    ///     username="alice" password="secret";
    /// ```
    ///
    /// If set, `username` and `password` can be extracted from this string
    /// via [`SaslConfig::resolve_username`] and [`SaslConfig::resolve_password`].
    pub jaas_config: Option<String>,

    /// Username for PLAIN authentication.
    /// Convenience field — used when `jaas_config` is not set.
    pub username: Option<String>,

    /// Password for PLAIN authentication.
    /// Convenience field — used when `jaas_config` is not set.
    pub password: Option<String>,
}

impl Default for SaslConfig {
    fn default() -> Self {
        SaslConfig {
            mechanism: DEFAULT_SASL_MECHANISM.to_owned(),
            jaas_config: None,
            username: None,
            password: None,
        }
    }
}

impl SaslConfig {
    /// Resolve the effective username, checking the `username` field first,
    /// then parsing from `jaas_config` if present.
    pub fn resolve_username(&self) -> Option<&str> {
        if let Some(ref u) = self.username {
            return Some(u.as_str());
        }
        if let Some(ref jaas) = self.jaas_config {
            return parse_jaas_option(jaas, "username");
        }
        None
    }

    /// Resolve the effective password, checking the `password` field first,
    /// then parsing from `jaas_config` if present.
    pub fn resolve_password(&self) -> Option<&str> {
        if let Some(ref p) = self.password {
            return Some(p.as_str());
        }
        if let Some(ref jaas) = self.jaas_config {
            return parse_jaas_option(jaas, "password");
        }
        None
    }
}

/// Parses an option value from a JAAS configuration string.
///
/// JAAS config format:
/// ```text
/// <loginModuleClass> <controlFlag> (<key>=<value>)*;
/// ```
///
/// Values may be quoted with double quotes. Keys are matched case-sensitively.
///
/// Returns a reference into the original `jaas` string if found, `None` otherwise.
fn parse_jaas_option<'a>(jaas: &'a str, key: &str) -> Option<&'a str> {
    // Build the search pattern: "key="
    // We need to find the key followed by '=' in the JAAS string.
    // The key could appear after whitespace or at the beginning of options.
    let key_eq = format!("{}=", key);

    // Search for the key= pattern in the string
    let mut search_from = 0;
    while search_from < jaas.len() {
        let pos = match jaas[search_from..].find(&key_eq) {
            Some(p) => search_from + p,
            None => return None,
        };

        // Check that key= appears at a word boundary (preceded by whitespace or start of string)
        let at_boundary = pos == 0 || jaas.as_bytes()[pos - 1].is_ascii_whitespace();
        if !at_boundary {
            search_from = pos + key_eq.len();
            continue;
        }

        let value_start = pos + key_eq.len();
        if value_start >= jaas.len() {
            return None;
        }

        // Check if value is quoted
        if jaas.as_bytes()[value_start] == b'"' {
            let quote_start = value_start + 1;
            // Find closing quote
            if let Some(quote_end) = jaas[quote_start..].find('"') {
                return Some(&jaas[quote_start..quote_start + quote_end]);
            }
            // No closing quote found — malformed
            return None;
        }

        // Unquoted value: read until whitespace or semicolon
        let value_end = jaas[value_start..]
            .find(|c: char| c.is_ascii_whitespace() || c == ';')
            .map(|e| value_start + e)
            .unwrap_or(jaas.len());
        return Some(&jaas[value_start..value_end]);
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default() {
        let config = SaslConfig::default();
        assert_eq!(config.mechanism, "GSSAPI");
        assert!(config.jaas_config.is_none());
        assert!(config.username.is_none());
        assert!(config.password.is_none());
    }

    #[test]
    fn test_resolve_username_from_direct_field() {
        let config = SaslConfig { username: Some("alice".to_owned()), ..SaslConfig::default() };
        assert_eq!(config.resolve_username(), Some("alice"));
    }

    #[test]
    fn test_resolve_password_from_direct_field() {
        let config = SaslConfig { password: Some("secret".to_owned()), ..SaslConfig::default() };
        assert_eq!(config.resolve_password(), Some("secret"));
    }

    #[test]
    fn test_resolve_username_from_jaas_config() {
        let config = SaslConfig {
            jaas_config: Some(
                "org.apache.kafka.common.security.plain.PlainLoginModule required \
                 username=\"alice\" password=\"secret\";"
                    .to_owned(),
            ),
            ..SaslConfig::default()
        };
        assert_eq!(config.resolve_username(), Some("alice"));
    }

    #[test]
    fn test_resolve_password_from_jaas_config() {
        let config = SaslConfig {
            jaas_config: Some(
                "org.apache.kafka.common.security.plain.PlainLoginModule required \
                 username=\"alice\" password=\"secret\";"
                    .to_owned(),
            ),
            ..SaslConfig::default()
        };
        assert_eq!(config.resolve_password(), Some("secret"));
    }

    #[test]
    fn test_direct_fields_take_precedence_over_jaas() {
        let config = SaslConfig {
            jaas_config: Some(
                "org.apache.kafka.common.security.plain.PlainLoginModule required \
                 username=\"jaas_user\" password=\"jaas_pass\";"
                    .to_owned(),
            ),
            username: Some("direct_user".to_owned()),
            password: Some("direct_pass".to_owned()),
            ..SaslConfig::default()
        };
        assert_eq!(config.resolve_username(), Some("direct_user"));
        assert_eq!(config.resolve_password(), Some("direct_pass"));
    }

    #[test]
    fn test_resolve_from_empty_config() {
        let config = SaslConfig::default();
        assert_eq!(config.resolve_username(), None);
        assert_eq!(config.resolve_password(), None);
    }

    #[test]
    fn test_jaas_config_unquoted_values() {
        let config = SaslConfig {
            jaas_config: Some("PlainLoginModule required username=alice password=secret;".to_owned()),
            ..SaslConfig::default()
        };
        assert_eq!(config.resolve_username(), Some("alice"));
        assert_eq!(config.resolve_password(), Some("secret"));
    }

    #[test]
    fn test_jaas_config_missing_field() {
        let config = SaslConfig {
            jaas_config: Some("PlainLoginModule required username=\"alice\";".to_owned()),
            ..SaslConfig::default()
        };
        assert_eq!(config.resolve_username(), Some("alice"));
        assert_eq!(config.resolve_password(), None);
    }

    #[test]
    fn test_jaas_config_malformed_no_closing_quote() {
        let config = SaslConfig {
            jaas_config: Some("PlainLoginModule required username=\"alice password=\"secret\";".to_owned()),
            ..SaslConfig::default()
        };
        // The first quote for username opens, and the next quote is in " password=" —
        // so we get "alice password=" as the value up to the next quote.
        // This is technically malformed input; we just get whatever is between quotes.
        assert_eq!(config.resolve_username(), Some("alice password="));
    }

    #[test]
    fn test_jaas_config_empty_string() {
        let config = SaslConfig { jaas_config: Some(String::new()), ..SaslConfig::default() };
        assert_eq!(config.resolve_username(), None);
        assert_eq!(config.resolve_password(), None);
    }

    #[test]
    fn test_jaas_config_no_options() {
        let config = SaslConfig {
            jaas_config: Some("PlainLoginModule required;".to_owned()),
            ..SaslConfig::default()
        };
        assert_eq!(config.resolve_username(), None);
        assert_eq!(config.resolve_password(), None);
    }

    #[test]
    fn test_jaas_config_with_extra_whitespace() {
        let config = SaslConfig {
            jaas_config: Some("PlainLoginModule   required   username=\"bob\"   password=\"pass123\"  ;".to_owned()),
            ..SaslConfig::default()
        };
        assert_eq!(config.resolve_username(), Some("bob"));
        assert_eq!(config.resolve_password(), Some("pass123"));
    }

    #[test]
    fn test_jaas_config_key_as_substring_not_matched() {
        // Ensure that "myusername" is not matched when searching for "username"
        let config = SaslConfig {
            jaas_config: Some("PlainLoginModule required myusername=\"wrong\" username=\"right\";".to_owned()),
            ..SaslConfig::default()
        };
        assert_eq!(config.resolve_username(), Some("right"));
    }

    #[test]
    fn test_config_key_constants() {
        assert_eq!(SASL_MECHANISM, "sasl.mechanism");
        assert_eq!(SASL_JAAS_CONFIG, "sasl.jaas.config");
        assert_eq!(DEFAULT_SASL_MECHANISM, "GSSAPI");
        assert_eq!(GSSAPI_MECHANISM, "GSSAPI");
    }

    #[test]
    fn test_clone() {
        let config = SaslConfig {
            mechanism: "SCRAM-SHA-256".to_owned(),
            jaas_config: Some("ScramLoginModule required;".to_owned()),
            username: Some("user".to_owned()),
            password: Some("pass".to_owned()),
        };
        let cloned = config.clone();
        assert_eq!(config.mechanism, cloned.mechanism);
        assert_eq!(config.jaas_config, cloned.jaas_config);
        assert_eq!(config.username, cloned.username);
        assert_eq!(config.password, cloned.password);
    }
}
