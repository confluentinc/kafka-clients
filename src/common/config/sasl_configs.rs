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

#![allow(dead_code)]
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

use crate::common::config::types::Password;

// ---------------------------------------------------------------------------
// Config key constants (matching Java SaslConfigs constant values)
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// SaslConfig struct
// ---------------------------------------------------------------------------

/// Translates the Java constants class
/// `org.apache.kafka.common.config.SaslConfigs`,
/// which has no instance state, so it becomes a unit struct hosting its
/// statics as associated items.
pub struct SaslConfigs;

impl SaslConfigs {
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
    pub const DEFAULT_SASL_MECHANISM: &str = Self::GSSAPI_MECHANISM;

    /// Returns whether `key` names a SASL config that Java defines as
    /// `ConfigDef.Type.PASSWORD`, i.e. one whose value must never be rendered.
    ///
    /// Translates the `Type.PASSWORD` marking of `sasl.jaas.config` in
    /// `SaslConfigs.addClientSaslSupport` (`SaslConfigs.java:380`). Java keeps
    /// that knowledge in `ConfigDef`, which this crate does not translate, so
    /// this predicate is its minimal carrier: it lets a holder of the raw user
    /// map (`ProducerConfig`'s `originals`) hide exactly the values Java hides.
    ///
    /// Covers exactly one of the key constants defined on this struct:
    /// [`SaslConfigs::SASL_JAAS_CONFIG`]. The other `Type.PASSWORD` keys Java
    /// defines in `addClientSaslSupport` (`SaslConfigs.java:392`, `:402`) are
    /// OAuth settings, which this client does not translate.
    pub(crate) fn is_password_config(key: &str) -> bool {
        key == Self::SASL_JAAS_CONFIG
    }
}

/// SASL configuration for Kafka connections.
///
/// Maps to the client-relevant subset of Java's `SaslConfigs`.
/// Currently supports PLAIN mechanism only.
///
/// Translated from `org.apache.kafka.common.config.SaslConfigs`.
#[derive(Debug, Clone, PartialEq, Eq)]
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
    ///
    /// A [`Password`] because Java defines `sasl.jaas.config` as
    /// `ConfigDef.Type.PASSWORD` (`SaslConfigs.java:380`): the string embeds
    /// the credentials, so every rendering of it is `[hidden]`.
    pub jaas_config: Option<Password>,

    /// Username for PLAIN authentication.
    /// Convenience field — used when `jaas_config` is not set.
    pub username: Option<String>,

    /// Password for PLAIN authentication.
    /// Convenience field — used when `jaas_config` is not set.
    ///
    /// Rust-only (set programmatically, no config key is parsed into it); a
    /// [`Password`] like every other secret, so every rendering of it is
    /// `[hidden]`. [`SaslConfig::resolve_password`] yields the plaintext.
    pub password: Option<Password>,
}

impl Default for SaslConfig {
    fn default() -> Self {
        SaslConfig {
            mechanism: SaslConfigs::DEFAULT_SASL_MECHANISM.to_owned(),
            jaas_config: None,
            username: None,
            password: None,
        }
    }
}

impl SaslConfig {
    /// Parses an option value from a JAAS configuration string.
    ///
    /// JAAS config format:
    /// ```text
    /// <loginModuleClass> <controlFlag> (<key>=<value>)*;
    /// ```
    ///
    /// Values may be quoted with double or single quotes, or left bare. Whitespace
    /// is tolerated on either side of the `=` — a JAAS string legally carries
    /// `name="v"`, `name = "v"`, `name='v'`, or `name=v`.
    ///
    /// A key is recognized only at an **option start** — the beginning of the
    /// string or immediately after whitespace — and quoted regions are skipped
    /// wholesale (honoring `\`-escaped quotes), so a `key=` sequence appearing
    /// inside another option's quoted value is never mistaken for a real option
    /// (e.g. `password="username=x" username="right"` resolves `username` to
    /// `right`, not `x`). This mirrors Java's JAAS lexer, which tokenizes options
    /// rather than substring-matching. The same option-start rule rejects
    /// `serviceName=` / `foo.username=` when searching for `name=` / `username=`
    /// (the char before the substring is a letter / `.`, not whitespace).
    ///
    /// The returned slice is the raw inner content between the quotes: `\`-escapes
    /// are honored for **boundary** detection (an escaped quote neither ends the
    /// value nor misaligns later options) but are not expanded, since the value is
    /// borrowed from `jaas` rather than owned.
    ///
    /// Returns a reference into the original `jaas` string if found, `None` otherwise.
    fn parse_jaas_option<'a>(jaas: &'a str, key: &str) -> Option<&'a str> {
        let bytes = jaas.as_bytes();
        let n = bytes.len();

        // Advance past a quoted region that opens at `bytes[i]` (a quote byte),
        // honoring backslash escapes. Returns the index just past the closing
        // quote, or `n` if the quote is unterminated. Quote and backslash are
        // ASCII, and UTF-8 continuation bytes are always >= 0x80, so this never
        // lands a false match inside a multi-byte character.
        fn skip_quoted(bytes: &[u8], mut i: usize) -> usize {
            let quote = bytes[i];
            i += 1;
            while i < bytes.len() {
                if bytes[i] == b'\\' {
                    i += 2; // skip the escape and the escaped byte
                    continue;
                }
                if bytes[i] == quote {
                    return i + 1;
                }
                i += 1;
            }
            i
        }

        // Walk the string once, skipping quoted regions, and only consider a match
        // where an option can legally begin (start of string or after whitespace).
        let mut i = 0;
        while i < n {
            let c = bytes[i];

            // Skip a quoted region wholesale so `key=` inside a value cannot match.
            if c == b'"' || c == b'\'' {
                i = skip_quoted(bytes, i);
                continue;
            }

            // A key can only begin at an option start.
            let at_option_start = i == 0 || bytes[i - 1].is_ascii_whitespace();
            if at_option_start && jaas[i..].starts_with(key) {
                // Skip whitespace between the key and '='.
                let mut j = i + key.len();
                while j < n && bytes[j].is_ascii_whitespace() {
                    j += 1;
                }
                // The key must be followed by '=' (after the optional whitespace);
                // otherwise this was e.g. `namespace` starting with `name` — keep
                // scanning.
                if j < n && bytes[j] == b'=' {
                    j += 1; // past '='

                    // Skip whitespace between '=' and the value.
                    while j < n && bytes[j].is_ascii_whitespace() {
                        j += 1;
                    }
                    if j >= n {
                        return None;
                    }

                    // Quoted value: double or single quote, escape-aware.
                    let quote = bytes[j];
                    if quote == b'"' || quote == b'\'' {
                        let value_start = j + 1;
                        let mut k = value_start;
                        while k < n {
                            if bytes[k] == b'\\' {
                                k += 2; // an escaped quote does not close the value
                                continue;
                            }
                            if bytes[k] == quote {
                                return Some(&jaas[value_start..k]);
                            }
                            k += 1;
                        }
                        // No closing quote found — malformed.
                        return None;
                    }

                    // Unquoted value: read until whitespace or semicolon.
                    let value_end = jaas[j..]
                        .find(|ch: char| ch.is_ascii_whitespace() || ch == ';')
                        .map(|e| j + e)
                        .unwrap_or(n);
                    return Some(&jaas[j..value_end]);
                }
            }

            i += 1;
        }

        None
    }

    /// Resolve the effective username, checking the `username` field first,
    /// then parsing from `jaas_config` if present.
    pub fn resolve_username(&self) -> Option<&str> {
        if let Some(ref u) = self.username {
            return Some(u.as_str());
        }
        if let Some(ref jaas) = self.jaas_config {
            return SaslConfig::parse_jaas_option(jaas.value(), "username");
        }
        None
    }

    /// Resolve the effective password, checking the `password` field first,
    /// then parsing from `jaas_config` if present.
    ///
    /// Returns the **plaintext**: the authenticator must send it, just as
    /// Java's `PlainLoginModule` yields a plain `String`. Never log the result.
    pub fn resolve_password(&self) -> Option<&str> {
        if let Some(ref p) = self.password {
            return Some(p.value());
        }
        if let Some(ref jaas) = self.jaas_config {
            return SaslConfig::parse_jaas_option(jaas.value(), "password");
        }
        None
    }
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
        let config = SaslConfig { password: Some(Password::new("secret")), ..SaslConfig::default() };
        assert_eq!(config.resolve_password(), Some("secret"));
    }

    #[test]
    fn test_resolve_username_from_jaas_config() {
        let config = SaslConfig {
            jaas_config: Some(Password::new(
                "org.apache.kafka.common.security.plain.PlainLoginModule required \
                 username=\"alice\" password=\"secret\";",
            )),
            ..SaslConfig::default()
        };
        assert_eq!(config.resolve_username(), Some("alice"));
    }

    #[test]
    fn test_resolve_password_from_jaas_config() {
        let config = SaslConfig {
            jaas_config: Some(Password::new(
                "org.apache.kafka.common.security.plain.PlainLoginModule required \
                 username=\"alice\" password=\"secret\";",
            )),
            ..SaslConfig::default()
        };
        assert_eq!(config.resolve_password(), Some("secret"));
    }

    #[test]
    fn test_direct_fields_take_precedence_over_jaas() {
        let config = SaslConfig {
            jaas_config: Some(Password::new(
                "org.apache.kafka.common.security.plain.PlainLoginModule required \
                 username=\"jaas_user\" password=\"jaas_pass\";",
            )),
            username: Some("direct_user".to_owned()),
            password: Some(Password::new("direct_pass")),
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
            jaas_config: Some(Password::new("PlainLoginModule required username=alice password=secret;")),
            ..SaslConfig::default()
        };
        assert_eq!(config.resolve_username(), Some("alice"));
        assert_eq!(config.resolve_password(), Some("secret"));
    }

    #[test]
    fn test_jaas_config_missing_field() {
        let config = SaslConfig {
            jaas_config: Some(Password::new("PlainLoginModule required username=\"alice\";")),
            ..SaslConfig::default()
        };
        assert_eq!(config.resolve_username(), Some("alice"));
        assert_eq!(config.resolve_password(), None);
    }

    #[test]
    fn test_jaas_config_malformed_no_closing_quote() {
        let config = SaslConfig {
            jaas_config: Some(Password::new("PlainLoginModule required username=\"alice password=\"secret\";")),
            ..SaslConfig::default()
        };
        // The first quote for username opens, and the next quote is in " password=" —
        // so we get "alice password=" as the value up to the next quote.
        // This is technically malformed input; we just get whatever is between quotes.
        assert_eq!(config.resolve_username(), Some("alice password="));
    }

    #[test]
    fn test_jaas_config_empty_string() {
        let config = SaslConfig { jaas_config: Some(Password::new("")), ..SaslConfig::default() };
        assert_eq!(config.resolve_username(), None);
        assert_eq!(config.resolve_password(), None);
    }

    #[test]
    fn test_jaas_config_no_options() {
        let config = SaslConfig {
            jaas_config: Some(Password::new("PlainLoginModule required;")),
            ..SaslConfig::default()
        };
        assert_eq!(config.resolve_username(), None);
        assert_eq!(config.resolve_password(), None);
    }

    #[test]
    fn test_jaas_config_with_extra_whitespace() {
        let config = SaslConfig {
            jaas_config: Some(Password::new(
                "PlainLoginModule   required   username=\"bob\"   password=\"pass123\"  ;",
            )),
            ..SaslConfig::default()
        };
        assert_eq!(config.resolve_username(), Some("bob"));
        assert_eq!(config.resolve_password(), Some("pass123"));
    }

    #[test]
    fn test_jaas_config_key_as_substring_not_matched() {
        // Ensure that "myusername" is not matched when searching for "username"
        let config = SaslConfig {
            jaas_config: Some(Password::new(
                "PlainLoginModule required myusername=\"wrong\" username=\"right\";",
            )),
            ..SaslConfig::default()
        };
        assert_eq!(config.resolve_username(), Some("right"));
    }

    #[test]
    fn test_jaas_config_spaces_around_equals() {
        // A JAAS string legally carries spaces around '='.
        let config = SaslConfig {
            jaas_config: Some(Password::new(
                "PlainLoginModule required username = \"alice\" password = \"secret\";",
            )),
            ..SaslConfig::default()
        };
        assert_eq!(config.resolve_username(), Some("alice"));
        assert_eq!(config.resolve_password(), Some("secret"));
    }

    #[test]
    fn test_jaas_config_single_quoted_values() {
        let config = SaslConfig {
            jaas_config: Some(Password::new("PlainLoginModule required username='alice' password='secret';")),
            ..SaslConfig::default()
        };
        assert_eq!(config.resolve_username(), Some("alice"));
        assert_eq!(config.resolve_password(), Some("secret"));
    }

    #[test]
    fn test_jaas_config_single_quoted_with_spaces() {
        let config = SaslConfig {
            jaas_config: Some(Password::new(
                "PlainLoginModule required username = 'alice' password = 'secret';",
            )),
            ..SaslConfig::default()
        };
        assert_eq!(config.resolve_username(), Some("alice"));
        assert_eq!(config.resolve_password(), Some("secret"));
    }

    #[test]
    fn test_jaas_config_bare_value_with_spaces_around_equals() {
        let config = SaslConfig {
            jaas_config: Some(Password::new("PlainLoginModule required username = alice password = secret;")),
            ..SaslConfig::default()
        };
        assert_eq!(config.resolve_username(), Some("alice"));
        assert_eq!(config.resolve_password(), Some("secret"));
    }

    #[test]
    fn test_jaas_config_service_name_does_not_false_match() {
        // `serviceName=` must not be picked up when searching for `name`, and a
        // dotted-prefix `something.username=` must not match `username=`.
        let config = SaslConfig {
            jaas_config: Some(Password::new(
                "com.sun.security.auth.module.Krb5LoginModule required \
                 serviceName=\"kafka\" foo.username=\"wrong\" username=\"right\" \
                 password=\"pass\";",
            )),
            ..SaslConfig::default()
        };
        assert_eq!(config.resolve_username(), Some("right"));
        assert_eq!(config.resolve_password(), Some("pass"));
    }

    #[test]
    fn test_jaas_config_namespace_does_not_false_match_name() {
        // A longer key sharing a prefix (`usernamespace=`) must not match
        // `username`, because '=' does not immediately follow (after optional
        // whitespace) the matched key text.
        assert_eq!(
            SaslConfig::parse_jaas_option("m required usernamespace=\"x\";", "username"),
            None
        );
    }

    #[test]
    fn test_jaas_config_key_inside_a_quoted_value_does_not_match() {
        // A `key=` sequence appearing inside another option's quoted value must
        // not be mistaken for a real option — quoted regions are skipped
        // wholesale. Here the real `username` option must resolve to `right`,
        // not the `username=x` embedded in the password value.
        let config = SaslConfig {
            jaas_config: Some(Password::new(
                "PlainLoginModule required password=\"username=x\" username=\"right\";",
            )),
            ..SaslConfig::default()
        };
        assert_eq!(config.resolve_username(), Some("right"));
        assert_eq!(config.resolve_password(), Some("username=x"));
    }

    #[test]
    fn test_jaas_config_escaped_quote_in_value_does_not_split_or_misalign() {
        // An escaped quote inside a quoted value must not terminate the value
        // (which would truncate it) nor misalign the scan of later options. The
        // password value here contains a `\"`; `username` after it must still
        // resolve to `right`, and the password slice must include the whole
        // escaped run (raw, not expanded).
        let config = SaslConfig {
            jaas_config: Some(Password::new(
                "PlainLoginModule required password=\"a\\\"b\" username=\"right\";",
            )),
            ..SaslConfig::default()
        };
        assert_eq!(config.resolve_username(), Some("right"));
        assert_eq!(config.resolve_password(), Some("a\\\"b"));
    }

    #[test]
    fn test_config_key_constants() {
        assert_eq!(SaslConfigs::SASL_MECHANISM, "sasl.mechanism");
        assert_eq!(SaslConfigs::SASL_JAAS_CONFIG, "sasl.jaas.config");
        assert_eq!(SaslConfigs::DEFAULT_SASL_MECHANISM, "GSSAPI");
        assert_eq!(SaslConfigs::GSSAPI_MECHANISM, "GSSAPI");
    }

    #[test]
    fn test_clone() {
        let config = SaslConfig {
            mechanism: "SCRAM-SHA-256".to_owned(),
            jaas_config: Some(Password::new("ScramLoginModule required;")),
            username: Some("user".to_owned()),
            password: Some(Password::new("pass")),
        };
        let cloned = config.clone();
        assert_eq!(config.mechanism, cloned.mechanism);
        assert_eq!(config.jaas_config, cloned.jaas_config);
        assert_eq!(config.username, cloned.username);
        assert_eq!(config.password, cloned.password);
    }

    /// Every `pub const` declared on [`SaslConfigs`], paired with whether Java
    /// defines it as `ConfigDef.Type.PASSWORD` (`SaslConfigs.java:380`).
    /// [`test_password_config_table_lists_every_constant`] keeps it complete.
    macro_rules! sasl_configs_constants {
        ($($name:ident => $is_password:expr),* $(,)?) => {
            [$((stringify!($name), SaslConfigs::$name, $is_password)),*]
        };
    }

    fn sasl_configs_constants() -> [(&'static str, &'static str, bool); 4] {
        sasl_configs_constants![
            SASL_MECHANISM => false,
            SASL_JAAS_CONFIG => true,
            GSSAPI_MECHANISM => false,
            DEFAULT_SASL_MECHANISM => false,
        ]
    }

    /// Both directions over every constant: the predicate is true for exactly
    /// the keys Java types `PASSWORD` and false for every other constant.
    #[test]
    fn test_is_password_config_matches_java_password_keys() {
        let mut password_keys = Vec::new();
        for (name, value, is_password) in sasl_configs_constants() {
            assert_eq!(
                SaslConfigs::is_password_config(value),
                is_password,
                "SaslConfigs::{name} = {value:?}"
            );
            if is_password {
                password_keys.push(value);
            }
        }
        // Guard against a vacuous pass: the Java `PASSWORD` set is not empty.
        assert_eq!(password_keys, vec!["sasl.jaas.config"]);

        // Keys are matched exactly, as `ConfigDef` does.
        assert!(!SaslConfigs::is_password_config(""));
        assert!(!SaslConfigs::is_password_config("SASL.JAAS.CONFIG"));
        assert!(!SaslConfigs::is_password_config("sasl.jaas.config "));
        assert!(!SaslConfigs::is_password_config("sasl.jaas"));
    }

    /// The table above must name every `pub const` this file declares, so a
    /// constant added later cannot silently escape the exhaustive test.
    #[test]
    fn test_password_config_table_lists_every_constant() {
        let declared: Vec<&str> = include_str!("sasl_configs.rs")
            .lines()
            .filter_map(|line| line.strip_prefix("    pub const "))
            .map(|rest| rest.split(':').next().unwrap_or_default())
            .collect();
        let tabled: Vec<&str> = sasl_configs_constants().iter().map(|(name, _, _)| *name).collect();
        assert_eq!(tabled, declared);
    }

    /// The finding's requested test: `{:?}` of a config holding credentials
    /// renders neither the password nor the JAAS string, while the non-secret
    /// fields stay visible and `resolve_*` still yield the plaintext.
    #[test]
    fn test_debug_redacts_jaas_config_and_password() {
        let config = SaslConfig {
            mechanism: "PLAIN".to_owned(),
            jaas_config: Some(Password::new(
                "org.apache.kafka.common.security.plain.PlainLoginModule required \
                 username=\"jaas-user\" password=\"jaas-S3cr3t-Pa55\";",
            )),
            username: Some("direct-user".to_owned()),
            password: Some(Password::new("direct-S3cr3t-Pa55")),
        };

        let rendered = format!("{config:?}");
        assert_eq!(
            rendered,
            "SaslConfig { mechanism: \"PLAIN\", jaas_config: Some([hidden]), \
             username: Some(\"direct-user\"), password: Some([hidden]) }"
        );
        for secret in [
            "direct-S3cr3t-Pa55",
            "jaas-S3cr3t-Pa55",
            "PlainLoginModule",
            "jaas-user",
        ] {
            assert!(!rendered.contains(secret), "{secret:?} leaked: {rendered}");
        }
        let pretty = format!("{config:#?}");
        assert!(pretty.contains("mechanism: \"PLAIN\""), "{pretty}");
        assert!(pretty.contains("username: Some("), "{pretty}");
        assert!(!pretty.contains("S3cr3t"), "{pretty}");
        assert!(!pretty.contains("PlainLoginModule"), "{pretty}");

        // The authenticator still receives the plaintext.
        assert_eq!(config.resolve_username(), Some("direct-user"));
        assert_eq!(config.resolve_password(), Some("direct-S3cr3t-Pa55"));

        // Credentials carried only by the JAAS string are hidden as well, and
        // still resolve to plaintext.
        let jaas_only = SaslConfig { username: None, password: None, ..config };
        let rendered = format!("{jaas_only:?}");
        assert!(!rendered.contains("jaas-S3cr3t-Pa55"), "{rendered}");
        assert!(!rendered.contains("jaas-user"), "{rendered}");
        assert!(rendered.contains("jaas_config: Some([hidden])"), "{rendered}");
        assert_eq!(jaas_only.resolve_username(), Some("jaas-user"));
        assert_eq!(jaas_only.resolve_password(), Some("jaas-S3cr3t-Pa55"));
    }
}
