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
//! In Java this class has 40+ constants, most for Kerberos and OAuth, which
//! this client does not support: it implements SASL PLAIN only. Their keys are
//! still validated as Java's `addClientSaslSupport` defines them (see
//! [`SaslConfigs::apply_sasl_config_key`]), as crate-private constants, and
//! have no effect.
//!
//! **Excluded Java constants:**
//! - `SASL_CLIENT_CALLBACK_HANDLER_CLASS`, `SASL_LOGIN_CALLBACK_HANDLER_CLASS`,
//!   `SASL_LOGIN_CLASS`, `SASL_OAUTHBEARER_JWT_RETRIEVER_CLASS`,
//!   `SASL_OAUTHBEARER_JWT_VALIDATOR_CLASS`: their values are classes, which
//!   CLAUDE.md §2 does not allow as configuration (the Rust counterpart is a
//!   trait set through a setter), so they are unknown keys here.
//! - The `*_DOC` strings and the `DEFAULT_*` values of the unsupported keys.

use crate::common::Error;
use crate::common::config::ConfigDef;
use crate::common::config::config_def::{Range, ValidList};
use crate::common::config::types::Password;

// ---------------------------------------------------------------------------
// Config key constants (matching Java SaslConfigs constant values)
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// SaslConfigs value fields
// ---------------------------------------------------------------------------

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

    // The client keys of `SaslConfigs.addClientSaslSupport`
    // (`SaslConfigs.java:387-431`) for the mechanisms this client does not
    // support. Crate-private: they are validated but have no effect.

    /// Config key: `sasl.kerberos.service.name`.
    pub(crate) const SASL_KERBEROS_SERVICE_NAME: &str = "sasl.kerberos.service.name";
    /// Config key: `sasl.kerberos.kinit.cmd`.
    pub(crate) const SASL_KERBEROS_KINIT_CMD: &str = "sasl.kerberos.kinit.cmd";
    /// Config key: `sasl.kerberos.ticket.renew.window.factor`.
    pub(crate) const SASL_KERBEROS_TICKET_RENEW_WINDOW_FACTOR: &str = "sasl.kerberos.ticket.renew.window.factor";
    /// Config key: `sasl.kerberos.ticket.renew.jitter`.
    pub(crate) const SASL_KERBEROS_TICKET_RENEW_JITTER: &str = "sasl.kerberos.ticket.renew.jitter";
    /// Config key: `sasl.kerberos.min.time.before.relogin`.
    pub(crate) const SASL_KERBEROS_MIN_TIME_BEFORE_RELOGIN: &str = "sasl.kerberos.min.time.before.relogin";
    /// Config key: `sasl.login.refresh.window.factor`.
    pub(crate) const SASL_LOGIN_REFRESH_WINDOW_FACTOR: &str = "sasl.login.refresh.window.factor";
    /// Config key: `sasl.login.refresh.window.jitter`.
    pub(crate) const SASL_LOGIN_REFRESH_WINDOW_JITTER: &str = "sasl.login.refresh.window.jitter";
    /// Config key: `sasl.login.refresh.min.period.seconds`.
    pub(crate) const SASL_LOGIN_REFRESH_MIN_PERIOD_SECONDS: &str = "sasl.login.refresh.min.period.seconds";
    /// Config key: `sasl.login.refresh.buffer.seconds`.
    pub(crate) const SASL_LOGIN_REFRESH_BUFFER_SECONDS: &str = "sasl.login.refresh.buffer.seconds";
    /// Config key: `sasl.login.connect.timeout.ms`.
    pub(crate) const SASL_LOGIN_CONNECT_TIMEOUT_MS: &str = "sasl.login.connect.timeout.ms";
    /// Config key: `sasl.login.read.timeout.ms`.
    pub(crate) const SASL_LOGIN_READ_TIMEOUT_MS: &str = "sasl.login.read.timeout.ms";
    /// Config key: `sasl.login.retry.backoff.max.ms`.
    pub(crate) const SASL_LOGIN_RETRY_BACKOFF_MAX_MS: &str = "sasl.login.retry.backoff.max.ms";
    /// Config key: `sasl.login.retry.backoff.ms`.
    pub(crate) const SASL_LOGIN_RETRY_BACKOFF_MS: &str = "sasl.login.retry.backoff.ms";
    /// Config key: `sasl.oauthbearer.scope`.
    pub(crate) const SASL_OAUTHBEARER_SCOPE: &str = "sasl.oauthbearer.scope";
    /// Config key: `sasl.oauthbearer.client.credentials.client.id`.
    pub(crate) const SASL_OAUTHBEARER_CLIENT_CREDENTIALS_CLIENT_ID: &str =
        "sasl.oauthbearer.client.credentials.client.id";
    /// Config key: `sasl.oauthbearer.client.credentials.client.secret`, a
    /// `Type.PASSWORD`.
    pub(crate) const SASL_OAUTHBEARER_CLIENT_CREDENTIALS_CLIENT_SECRET: &str =
        "sasl.oauthbearer.client.credentials.client.secret";
    /// Config key: `sasl.oauthbearer.assertion.algorithm`.
    pub(crate) const SASL_OAUTHBEARER_ASSERTION_ALGORITHM: &str = "sasl.oauthbearer.assertion.algorithm";
    /// Config key: `sasl.oauthbearer.assertion.claim.aud`.
    pub(crate) const SASL_OAUTHBEARER_ASSERTION_CLAIM_AUD: &str = "sasl.oauthbearer.assertion.claim.aud";
    /// Config key: `sasl.oauthbearer.assertion.claim.exp.seconds`.
    pub(crate) const SASL_OAUTHBEARER_ASSERTION_CLAIM_EXP_SECONDS: &str =
        "sasl.oauthbearer.assertion.claim.exp.seconds";
    /// Config key: `sasl.oauthbearer.assertion.claim.iss`.
    pub(crate) const SASL_OAUTHBEARER_ASSERTION_CLAIM_ISS: &str = "sasl.oauthbearer.assertion.claim.iss";
    /// Config key: `sasl.oauthbearer.assertion.claim.jti.include`.
    pub(crate) const SASL_OAUTHBEARER_ASSERTION_CLAIM_JTI_INCLUDE: &str =
        "sasl.oauthbearer.assertion.claim.jti.include";
    /// Config key: `sasl.oauthbearer.assertion.claim.nbf.seconds`.
    pub(crate) const SASL_OAUTHBEARER_ASSERTION_CLAIM_NBF_SECONDS: &str =
        "sasl.oauthbearer.assertion.claim.nbf.seconds";
    /// Config key: `sasl.oauthbearer.assertion.claim.sub`.
    pub(crate) const SASL_OAUTHBEARER_ASSERTION_CLAIM_SUB: &str = "sasl.oauthbearer.assertion.claim.sub";
    /// Config key: `sasl.oauthbearer.assertion.file`.
    pub(crate) const SASL_OAUTHBEARER_ASSERTION_FILE: &str = "sasl.oauthbearer.assertion.file";
    /// Config key: `sasl.oauthbearer.assertion.private.key.file`.
    pub(crate) const SASL_OAUTHBEARER_ASSERTION_PRIVATE_KEY_FILE: &str = "sasl.oauthbearer.assertion.private.key.file";
    /// Config key: `sasl.oauthbearer.assertion.private.key.passphrase`, a
    /// `Type.PASSWORD`.
    pub(crate) const SASL_OAUTHBEARER_ASSERTION_PRIVATE_KEY_PASSPHRASE: &str =
        "sasl.oauthbearer.assertion.private.key.passphrase";
    /// Config key: `sasl.oauthbearer.assertion.template.file`.
    pub(crate) const SASL_OAUTHBEARER_ASSERTION_TEMPLATE_FILE: &str = "sasl.oauthbearer.assertion.template.file";
    /// Config key: `sasl.oauthbearer.scope.claim.name`.
    pub(crate) const SASL_OAUTHBEARER_SCOPE_CLAIM_NAME: &str = "sasl.oauthbearer.scope.claim.name";
    /// Config key: `sasl.oauthbearer.sub.claim.name`.
    pub(crate) const SASL_OAUTHBEARER_SUB_CLAIM_NAME: &str = "sasl.oauthbearer.sub.claim.name";
    /// Config key: `sasl.oauthbearer.token.endpoint.url`.
    pub(crate) const SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL: &str = "sasl.oauthbearer.token.endpoint.url";
    /// Config key: `sasl.oauthbearer.jwks.endpoint.url`.
    pub(crate) const SASL_OAUTHBEARER_JWKS_ENDPOINT_URL: &str = "sasl.oauthbearer.jwks.endpoint.url";
    /// Config key: `sasl.oauthbearer.jwks.endpoint.refresh.ms`.
    pub(crate) const SASL_OAUTHBEARER_JWKS_ENDPOINT_REFRESH_MS: &str = "sasl.oauthbearer.jwks.endpoint.refresh.ms";
    /// Config key: `sasl.oauthbearer.jwks.endpoint.retry.backoff.max.ms`.
    pub(crate) const SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MAX_MS: &str =
        "sasl.oauthbearer.jwks.endpoint.retry.backoff.max.ms";
    /// Config key: `sasl.oauthbearer.jwks.endpoint.retry.backoff.ms`.
    pub(crate) const SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MS: &str =
        "sasl.oauthbearer.jwks.endpoint.retry.backoff.ms";
    /// Config key: `sasl.oauthbearer.clock.skew.seconds`.
    pub(crate) const SASL_OAUTHBEARER_CLOCK_SKEW_SECONDS: &str = "sasl.oauthbearer.clock.skew.seconds";
    /// Config key: `sasl.oauthbearer.allow.unverified.audience` (new in 4.4).
    pub(crate) const SASL_OAUTHBEARER_ALLOW_UNVERIFIED_AUDIENCE: &str = "sasl.oauthbearer.allow.unverified.audience";
    /// Config key: `sasl.oauthbearer.expected.audience`.
    pub(crate) const SASL_OAUTHBEARER_EXPECTED_AUDIENCE: &str = "sasl.oauthbearer.expected.audience";
    /// Config key: `sasl.oauthbearer.allow.unverified.issuer` (new in 4.4).
    pub(crate) const SASL_OAUTHBEARER_ALLOW_UNVERIFIED_ISSUER: &str = "sasl.oauthbearer.allow.unverified.issuer";
    /// Config key: `sasl.oauthbearer.expected.issuer`.
    pub(crate) const SASL_OAUTHBEARER_EXPECTED_ISSUER: &str = "sasl.oauthbearer.expected.issuer";
    /// Config key: `sasl.oauthbearer.header.urlencode`.
    pub(crate) const SASL_OAUTHBEARER_HEADER_URLENCODE: &str = "sasl.oauthbearer.header.urlencode";
}

/// SASL configuration for Kafka connections.
///
/// Translated from `org.apache.kafka.common.config.SaslConfigs`. The Java class
/// hosts only the `sasl.*` config keys and defaults; the values themselves
/// travel in the `Map<String, ?>` handed to `SaslChannelBuilder.configure`.
/// The Rust struct keeps Java's name and constants, and additionally holds the
/// client-relevant subset of those values, parsed once from the client config
/// properties. The fields are crate-private: callers set them through the
/// `sasl.*` properties of a client config, as a Java caller does.
///
/// Currently supports PLAIN mechanism only.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
#[doc(alias = "org.apache.kafka.common.config.SaslConfigs")]
pub struct SaslConfigs {
    /// SASL mechanism. Default: `"GSSAPI"` (matches Java `DEFAULT_SASL_MECHANISM`).
    /// Corresponds to `sasl.mechanism`.
    pub(crate) mechanism: String,

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
    /// via [`SaslConfigs::resolve_username`] and [`SaslConfigs::resolve_password`].
    ///
    /// A [`Password`] because Java defines `sasl.jaas.config` as
    /// `ConfigDef.Type.PASSWORD` (`SaslConfigs.java:380`): the string embeds
    /// the credentials, so every rendering of it is `[hidden]`.
    pub(crate) jaas_config: Option<Password>,

    /// Username for PLAIN authentication.
    /// Convenience field — used when `jaas_config` is not set.
    pub(crate) username: Option<String>,

    /// Password for PLAIN authentication.
    /// Convenience field — used when `jaas_config` is not set.
    ///
    /// Rust-only. No config key is parsed into it; tests set it directly. A
    /// [`Password`] like every other secret, so every rendering of it is
    /// `[hidden]`. [`SaslConfigs::resolve_password`] yields the plaintext.
    pub(crate) password: Option<Password>,
}

impl Default for SaslConfigs {
    fn default() -> Self {
        SaslConfigs {
            mechanism: SaslConfigs::DEFAULT_SASL_MECHANISM.to_owned(),
            jaas_config: None,
            username: None,
            password: None,
        }
    }
}

impl SaslConfigs {
    /// Applies a single `sasl.*` configuration key/value pair to `sasl`.
    ///
    /// The `sasl.*` counterpart of
    /// [`SslConfigs::apply_ssl_config_key`](crate::common::config::SslConfigs::apply_ssl_config_key),
    /// covering the client keys Java's
    /// `SaslConfigs.addClientSaslSupport` defines (`SaslConfigs.java:387-431`).
    /// `value` is the raw property value: each key's parser trims it, as
    /// `ConfigDef.parseType` does, and an error prints it as Java's would.
    ///
    /// - `sasl.mechanism` and `sasl.jaas.config` are kept, trimmed; an empty
    ///   JAAS value means unset.
    /// - The 40 keys of the mechanisms this client does not support (Kerberos,
    ///   OAUTHBEARER, login refresh) are parsed and validated with Java's type
    ///   and validator, then logged as ignored and dropped. The two
    ///   `Type.PASSWORD` ones are never kept, so their values cannot leak.
    /// - Any other `sasl.*` key, among them the class-valued ones (see the
    ///   module docs), is accepted silently, as Java's `AbstractConfig` accepts
    ///   an unknown key.
    ///
    /// # Errors
    ///
    /// [`Error::Config`] with Java's message when a value does not parse as the
    /// key's type or fails its validator.
    pub(crate) fn apply_sasl_config_key(sasl: &mut SaslConfigs, key: &str, value: &str) -> Result<(), Error> {
        match key {
            // `Type.STRING` (`SaslConfigs.java:396`).
            SaslConfigs::SASL_MECHANISM => sasl.mechanism = ConfigDef::trim(value).to_string(),
            SaslConfigs::SASL_JAAS_CONFIG => {
                // `Type.PASSWORD` (`SaslConfigs.java:397`): `parseType` wraps the
                // trimmed value (`ConfigDef.java:724-728`).
                let trimmed = ConfigDef::trim(value);
                sasl.jaas_config = if trimmed.is_empty() {
                    None
                } else {
                    Some(Password::new(trimmed))
                };
            },
            _ => {
                if Self::validate_unsupported_client_key(key, value)? {
                    log::warn!(
                        "{key} is not supported by this client and is ignored; the only SASL mechanism it supports is PLAIN"
                    );
                }
            },
        }
        Ok(())
    }

    /// Parses and validates `value` for one of the client SASL keys this
    /// client does not support, with the type and validator
    /// `addClientSaslSupport` gives it (`SaslConfigs.java:388-430`).
    ///
    /// Returns whether `key` is one of them.
    fn validate_unsupported_client_key(key: &str, value: &str) -> Result<bool, Error> {
        match key {
            // `Type.STRING` and `Type.PASSWORD` without a validator: any value.
            SaslConfigs::SASL_KERBEROS_SERVICE_NAME
            | SaslConfigs::SASL_KERBEROS_KINIT_CMD
            | SaslConfigs::SASL_OAUTHBEARER_SCOPE
            | SaslConfigs::SASL_OAUTHBEARER_CLIENT_CREDENTIALS_CLIENT_ID
            | SaslConfigs::SASL_OAUTHBEARER_CLIENT_CREDENTIALS_CLIENT_SECRET
            | SaslConfigs::SASL_OAUTHBEARER_ASSERTION_CLAIM_AUD
            | SaslConfigs::SASL_OAUTHBEARER_ASSERTION_CLAIM_ISS
            | SaslConfigs::SASL_OAUTHBEARER_ASSERTION_CLAIM_SUB
            | SaslConfigs::SASL_OAUTHBEARER_ASSERTION_FILE
            | SaslConfigs::SASL_OAUTHBEARER_ASSERTION_PRIVATE_KEY_FILE
            | SaslConfigs::SASL_OAUTHBEARER_ASSERTION_PRIVATE_KEY_PASSPHRASE
            | SaslConfigs::SASL_OAUTHBEARER_ASSERTION_TEMPLATE_FILE
            | SaslConfigs::SASL_OAUTHBEARER_SCOPE_CLAIM_NAME
            | SaslConfigs::SASL_OAUTHBEARER_SUB_CLAIM_NAME
            | SaslConfigs::SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL
            | SaslConfigs::SASL_OAUTHBEARER_JWKS_ENDPOINT_URL
            | SaslConfigs::SASL_OAUTHBEARER_EXPECTED_ISSUER => {},
            SaslConfigs::SASL_OAUTHBEARER_ASSERTION_ALGORITHM => {
                // A trimmed `Type.STRING` checked by
                // `CaseInsensitiveValidString.in("ES256", "RS256")`, whose message
                // lists its `HashSet` in iteration order (`ConfigDef.java:1141-1145`).
                let trimmed = ConfigDef::trim(value);
                let upper = trimmed.to_uppercase();
                if upper != "ES256" && upper != "RS256" {
                    return Err(Error::config_name_value_message(
                        key,
                        trimmed,
                        "String must be one of (case insensitive): ES256, RS256",
                    ));
                }
            },
            // `Type.DOUBLE` without a validator.
            SaslConfigs::SASL_KERBEROS_TICKET_RENEW_WINDOW_FACTOR | SaslConfigs::SASL_KERBEROS_TICKET_RENEW_JITTER => {
                ConfigDef::parse_double(key, value)?;
            },
            SaslConfigs::SASL_LOGIN_REFRESH_WINDOW_FACTOR => {
                Range::between(0.5, 1.0).ensure_valid(key, ConfigDef::parse_double(key, value)?)?;
            },
            SaslConfigs::SASL_LOGIN_REFRESH_WINDOW_JITTER => {
                Range::between(0.0, 0.25).ensure_valid(key, ConfigDef::parse_double(key, value)?)?;
            },
            // `Type.SHORT`, `Range.between(0, 900)` / `(0, 3600)`.
            SaslConfigs::SASL_LOGIN_REFRESH_MIN_PERIOD_SECONDS => {
                Range::between(0, 900).ensure_valid(key, ConfigDef::parse_short(key, value)?)?;
            },
            SaslConfigs::SASL_LOGIN_REFRESH_BUFFER_SECONDS => {
                Range::between(0, 3600).ensure_valid(key, ConfigDef::parse_short(key, value)?)?;
            },
            // `Type.LONG` without a validator.
            SaslConfigs::SASL_KERBEROS_MIN_TIME_BEFORE_RELOGIN
            | SaslConfigs::SASL_LOGIN_RETRY_BACKOFF_MAX_MS
            | SaslConfigs::SASL_LOGIN_RETRY_BACKOFF_MS
            | SaslConfigs::SASL_OAUTHBEARER_JWKS_ENDPOINT_REFRESH_MS
            | SaslConfigs::SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MAX_MS
            | SaslConfigs::SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MS => {
                ConfigDef::parse_long(key, value)?;
            },
            // `Type.INT` without a validator.
            SaslConfigs::SASL_LOGIN_CONNECT_TIMEOUT_MS
            | SaslConfigs::SASL_LOGIN_READ_TIMEOUT_MS
            | SaslConfigs::SASL_OAUTHBEARER_CLOCK_SKEW_SECONDS => {
                ConfigDef::parse_int(key, value)?;
            },
            // `Type.INT`, `Range.between(0, 86400)` / `(0, 3600)`.
            SaslConfigs::SASL_OAUTHBEARER_ASSERTION_CLAIM_EXP_SECONDS => {
                Range::between(0, 86400).ensure_valid(key, ConfigDef::parse_int(key, value)?)?;
            },
            SaslConfigs::SASL_OAUTHBEARER_ASSERTION_CLAIM_NBF_SECONDS => {
                Range::between(0, 3600).ensure_valid(key, ConfigDef::parse_int(key, value)?)?;
            },
            // `Type.BOOLEAN`.
            SaslConfigs::SASL_OAUTHBEARER_ASSERTION_CLAIM_JTI_INCLUDE
            | SaslConfigs::SASL_OAUTHBEARER_ALLOW_UNVERIFIED_AUDIENCE
            | SaslConfigs::SASL_OAUTHBEARER_ALLOW_UNVERIFIED_ISSUER
            | SaslConfigs::SASL_OAUTHBEARER_HEADER_URLENCODE => {
                ConfigDef::parse_boolean(key, value)?;
            },
            // `Type.LIST`, `ValidList.anyNonDuplicateValues(true, false)`.
            SaslConfigs::SASL_OAUTHBEARER_EXPECTED_AUDIENCE => {
                ValidList::parse_any_non_duplicate_values(key, value, true)?;
            },
            _ => return Ok(false),
        }
        Ok(true)
    }

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
            return SaslConfigs::parse_jaas_option(jaas.value(), "username");
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
            return SaslConfigs::parse_jaas_option(jaas.value(), "password");
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default() {
        let config = SaslConfigs::default();
        assert_eq!(config.mechanism, "GSSAPI");
        assert!(config.jaas_config.is_none());
        assert!(config.username.is_none());
        assert!(config.password.is_none());
    }

    #[test]
    fn test_resolve_username_from_direct_field() {
        let config = SaslConfigs { username: Some("alice".to_owned()), ..SaslConfigs::default() };
        assert_eq!(config.resolve_username(), Some("alice"));
    }

    #[test]
    fn test_resolve_password_from_direct_field() {
        let config = SaslConfigs { password: Some(Password::new("secret")), ..SaslConfigs::default() };
        assert_eq!(config.resolve_password(), Some("secret"));
    }

    #[test]
    fn test_resolve_username_from_jaas_config() {
        let config = SaslConfigs {
            jaas_config: Some(Password::new(
                "org.apache.kafka.common.security.plain.PlainLoginModule required \
                 username=\"alice\" password=\"secret\";",
            )),
            ..SaslConfigs::default()
        };
        assert_eq!(config.resolve_username(), Some("alice"));
    }

    #[test]
    fn test_resolve_password_from_jaas_config() {
        let config = SaslConfigs {
            jaas_config: Some(Password::new(
                "org.apache.kafka.common.security.plain.PlainLoginModule required \
                 username=\"alice\" password=\"secret\";",
            )),
            ..SaslConfigs::default()
        };
        assert_eq!(config.resolve_password(), Some("secret"));
    }

    #[test]
    fn test_direct_fields_take_precedence_over_jaas() {
        let config = SaslConfigs {
            jaas_config: Some(Password::new(
                "org.apache.kafka.common.security.plain.PlainLoginModule required \
                 username=\"jaas_user\" password=\"jaas_pass\";",
            )),
            username: Some("direct_user".to_owned()),
            password: Some(Password::new("direct_pass")),
            ..SaslConfigs::default()
        };
        assert_eq!(config.resolve_username(), Some("direct_user"));
        assert_eq!(config.resolve_password(), Some("direct_pass"));
    }

    #[test]
    fn test_resolve_from_empty_config() {
        let config = SaslConfigs::default();
        assert_eq!(config.resolve_username(), None);
        assert_eq!(config.resolve_password(), None);
    }

    #[test]
    fn test_jaas_config_unquoted_values() {
        let config = SaslConfigs {
            jaas_config: Some(Password::new("PlainLoginModule required username=alice password=secret;")),
            ..SaslConfigs::default()
        };
        assert_eq!(config.resolve_username(), Some("alice"));
        assert_eq!(config.resolve_password(), Some("secret"));
    }

    #[test]
    fn test_jaas_config_missing_field() {
        let config = SaslConfigs {
            jaas_config: Some(Password::new("PlainLoginModule required username=\"alice\";")),
            ..SaslConfigs::default()
        };
        assert_eq!(config.resolve_username(), Some("alice"));
        assert_eq!(config.resolve_password(), None);
    }

    #[test]
    fn test_jaas_config_malformed_no_closing_quote() {
        let config = SaslConfigs {
            jaas_config: Some(Password::new("PlainLoginModule required username=\"alice password=\"secret\";")),
            ..SaslConfigs::default()
        };
        // The first quote for username opens, and the next quote is in " password=" —
        // so we get "alice password=" as the value up to the next quote.
        // This is technically malformed input; we just get whatever is between quotes.
        assert_eq!(config.resolve_username(), Some("alice password="));
    }

    #[test]
    fn test_jaas_config_empty_string() {
        let config = SaslConfigs { jaas_config: Some(Password::new("")), ..SaslConfigs::default() };
        assert_eq!(config.resolve_username(), None);
        assert_eq!(config.resolve_password(), None);
    }

    #[test]
    fn test_jaas_config_no_options() {
        let config = SaslConfigs {
            jaas_config: Some(Password::new("PlainLoginModule required;")),
            ..SaslConfigs::default()
        };
        assert_eq!(config.resolve_username(), None);
        assert_eq!(config.resolve_password(), None);
    }

    #[test]
    fn test_jaas_config_with_extra_whitespace() {
        let config = SaslConfigs {
            jaas_config: Some(Password::new(
                "PlainLoginModule   required   username=\"bob\"   password=\"pass123\"  ;",
            )),
            ..SaslConfigs::default()
        };
        assert_eq!(config.resolve_username(), Some("bob"));
        assert_eq!(config.resolve_password(), Some("pass123"));
    }

    #[test]
    fn test_jaas_config_key_as_substring_not_matched() {
        // Ensure that "myusername" is not matched when searching for "username"
        let config = SaslConfigs {
            jaas_config: Some(Password::new(
                "PlainLoginModule required myusername=\"wrong\" username=\"right\";",
            )),
            ..SaslConfigs::default()
        };
        assert_eq!(config.resolve_username(), Some("right"));
    }

    #[test]
    fn test_jaas_config_spaces_around_equals() {
        // A JAAS string legally carries spaces around '='.
        let config = SaslConfigs {
            jaas_config: Some(Password::new(
                "PlainLoginModule required username = \"alice\" password = \"secret\";",
            )),
            ..SaslConfigs::default()
        };
        assert_eq!(config.resolve_username(), Some("alice"));
        assert_eq!(config.resolve_password(), Some("secret"));
    }

    #[test]
    fn test_jaas_config_single_quoted_values() {
        let config = SaslConfigs {
            jaas_config: Some(Password::new("PlainLoginModule required username='alice' password='secret';")),
            ..SaslConfigs::default()
        };
        assert_eq!(config.resolve_username(), Some("alice"));
        assert_eq!(config.resolve_password(), Some("secret"));
    }

    #[test]
    fn test_jaas_config_single_quoted_with_spaces() {
        let config = SaslConfigs {
            jaas_config: Some(Password::new(
                "PlainLoginModule required username = 'alice' password = 'secret';",
            )),
            ..SaslConfigs::default()
        };
        assert_eq!(config.resolve_username(), Some("alice"));
        assert_eq!(config.resolve_password(), Some("secret"));
    }

    #[test]
    fn test_jaas_config_bare_value_with_spaces_around_equals() {
        let config = SaslConfigs {
            jaas_config: Some(Password::new("PlainLoginModule required username = alice password = secret;")),
            ..SaslConfigs::default()
        };
        assert_eq!(config.resolve_username(), Some("alice"));
        assert_eq!(config.resolve_password(), Some("secret"));
    }

    #[test]
    fn test_jaas_config_service_name_does_not_false_match() {
        // `serviceName=` must not be picked up when searching for `name`, and a
        // dotted-prefix `something.username=` must not match `username=`.
        let config = SaslConfigs {
            jaas_config: Some(Password::new(
                "com.sun.security.auth.module.Krb5LoginModule required \
                 serviceName=\"kafka\" foo.username=\"wrong\" username=\"right\" \
                 password=\"pass\";",
            )),
            ..SaslConfigs::default()
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
            SaslConfigs::parse_jaas_option("m required usernamespace=\"x\";", "username"),
            None
        );
    }

    #[test]
    fn test_jaas_config_key_inside_a_quoted_value_does_not_match() {
        // A `key=` sequence appearing inside another option's quoted value must
        // not be mistaken for a real option — quoted regions are skipped
        // wholesale. Here the real `username` option must resolve to `right`,
        // not the `username=x` embedded in the password value.
        let config = SaslConfigs {
            jaas_config: Some(Password::new(
                "PlainLoginModule required password=\"username=x\" username=\"right\";",
            )),
            ..SaslConfigs::default()
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
        let config = SaslConfigs {
            jaas_config: Some(Password::new(
                "PlainLoginModule required password=\"a\\\"b\" username=\"right\";",
            )),
            ..SaslConfigs::default()
        };
        assert_eq!(config.resolve_username(), Some("right"));
        assert_eq!(config.resolve_password(), Some("a\\\"b"));
    }

    fn config_error(result: Result<(), Error>) -> String {
        match result {
            Err(Error::Config(e)) => e.message().to_string(),
            other => panic!("expected a ConfigError, got {other:?}"),
        }
    }

    /// Every client key of `SaslConfigs.addClientSaslSupport`
    /// (`SaslConfigs.java:388-430`) this client does not support, with Java's
    /// type and validator: a valid value is accepted and leaves the config
    /// unchanged (nothing is kept, the two passwords included), and for each
    /// key with a type or validator, a value Java rejects fails with Java's
    /// message.
    #[test]
    fn test_unsupported_client_keys_are_validated_and_dropped() {
        const NOT_INT: &str = "Not a number of type INT";
        const NOT_LONG: &str = "Not a number of type LONG";
        const NOT_SHORT: &str = "Not a number of type SHORT";
        const NOT_DOUBLE: &str = "Not a number of type DOUBLE";
        const NOT_BOOLEAN: &str = "Expected value to be either true or false";
        /// A value Java rejects, and the reason its message gives.
        type Rejected = Option<(&'static str, &'static str)>;
        // (key, a valid value, a value Java rejects and its message)
        let keys: [(&str, &str, Rejected); 40] = [
            ("sasl.kerberos.service.name", "kafka", None),
            ("sasl.kerberos.kinit.cmd", "/usr/bin/kinit", None),
            ("sasl.kerberos.ticket.renew.window.factor", "0.8", Some(("x", NOT_DOUBLE))),
            ("sasl.kerberos.ticket.renew.jitter", "0.05", Some(("x", NOT_DOUBLE))),
            ("sasl.kerberos.min.time.before.relogin", "60000", Some(("x", NOT_LONG))),
            (
                "sasl.login.refresh.window.factor",
                "1.0",
                Some(("2", "Value must be no more than 1.0")),
            ),
            (
                "sasl.login.refresh.window.jitter",
                "0.0",
                Some(("-0.1", "Value must be at least 0.0")),
            ),
            ("sasl.login.refresh.min.period.seconds", "900", Some(("40000", NOT_SHORT))),
            (
                "sasl.login.refresh.buffer.seconds",
                "0",
                Some(("3601", "Value must be no more than 3600")),
            ),
            ("sasl.login.connect.timeout.ms", "1000", Some(("1s", NOT_INT))),
            ("sasl.login.read.timeout.ms", "1000", Some(("1s", NOT_INT))),
            ("sasl.login.retry.backoff.max.ms", "10000", Some(("1s", NOT_LONG))),
            ("sasl.login.retry.backoff.ms", "100", Some(("1s", NOT_LONG))),
            ("sasl.oauthbearer.scope", "read", None),
            ("sasl.oauthbearer.client.credentials.client.id", "id", None),
            ("sasl.oauthbearer.client.credentials.client.secret", "oauth-S3cr3t", None),
            (
                "sasl.oauthbearer.assertion.algorithm",
                " es256 ",
                Some(("HS256", "String must be one of (case insensitive): ES256, RS256")),
            ),
            ("sasl.oauthbearer.assertion.claim.aud", "aud", None),
            (
                "sasl.oauthbearer.assertion.claim.exp.seconds",
                "86400",
                Some(("86401", "Value must be no more than 86400")),
            ),
            ("sasl.oauthbearer.assertion.claim.iss", "iss", None),
            (
                "sasl.oauthbearer.assertion.claim.jti.include",
                "TRUE",
                Some(("yes", NOT_BOOLEAN)),
            ),
            (
                "sasl.oauthbearer.assertion.claim.nbf.seconds",
                "0",
                Some(("-1", "Value must be at least 0")),
            ),
            ("sasl.oauthbearer.assertion.claim.sub", "sub", None),
            ("sasl.oauthbearer.assertion.file", "/a.jwt", None),
            ("sasl.oauthbearer.assertion.private.key.file", "/k.pem", None),
            ("sasl.oauthbearer.assertion.private.key.passphrase", "pass-S3cr3t", None),
            ("sasl.oauthbearer.assertion.template.file", "/t.json", None),
            ("sasl.oauthbearer.scope.claim.name", "scope", None),
            ("sasl.oauthbearer.sub.claim.name", "sub", None),
            ("sasl.oauthbearer.token.endpoint.url", "https://x", None),
            ("sasl.oauthbearer.jwks.endpoint.url", "https://x", None),
            ("sasl.oauthbearer.jwks.endpoint.refresh.ms", "3600000", Some(("1h", NOT_LONG))),
            (
                "sasl.oauthbearer.jwks.endpoint.retry.backoff.max.ms",
                "10000",
                Some(("1s", NOT_LONG)),
            ),
            ("sasl.oauthbearer.jwks.endpoint.retry.backoff.ms", "100", Some(("1s", NOT_LONG))),
            ("sasl.oauthbearer.clock.skew.seconds", "30", Some(("1s", NOT_INT))),
            ("sasl.oauthbearer.allow.unverified.audience", "false", Some(("no", NOT_BOOLEAN))),
            (
                "sasl.oauthbearer.expected.audience",
                "a, b",
                Some((
                    "a,,b",
                    "Configuration 'sasl.oauthbearer.expected.audience' values must not be empty.",
                )),
            ),
            ("sasl.oauthbearer.allow.unverified.issuer", "true", Some(("no", NOT_BOOLEAN))),
            ("sasl.oauthbearer.expected.issuer", "https://issuer", None),
            ("sasl.oauthbearer.header.urlencode", "false", Some(("no", NOT_BOOLEAN))),
        ];
        for (key, valid, invalid) in keys {
            let mut sasl = SaslConfigs::default();
            SaslConfigs::apply_sasl_config_key(&mut sasl, key, valid)
                .unwrap_or_else(|e| panic!("{key}={valid}: {e:?}"));
            assert_eq!(sasl, SaslConfigs::default(), "{key} must not be kept");
            assert!(SaslConfigs::validate_unsupported_client_key(key, valid).unwrap(), "{key}");
            if let Some((value, message)) = invalid {
                let expected = if message.starts_with("Configuration '") {
                    message.to_string()
                } else {
                    let shown = match key {
                        // `Range` prints the parsed `double`, `CaseInsensitiveValidString`
                        // the trimmed string.
                        "sasl.login.refresh.window.factor" => "2.0",
                        _ => value,
                    };
                    format!("Invalid value {shown} for configuration {key}: {message}")
                };
                assert_eq!(
                    config_error(SaslConfigs::apply_sasl_config_key(&mut sasl, key, value)),
                    expected
                );
            }
        }
    }

    // Translations of `SaslConfigsTest` (`SaslConfigsTest.java:29-124`), which
    // parses props with `new ConfigDef().withClientSaslSupport()`; here each key
    // goes through `apply_sasl_config_key`. `testSaslLoginRefreshDefaults` has no
    // counterpart: this client keeps none of the four login-refresh values, so
    // there are no defaults to read back. For the same reason the valid-value
    // tests check that the values are accepted, not what they parse to.

    /// Applies each `(key, value)` to a fresh `SaslConfigs`, as Java's tests
    /// parse their props map, and checks nothing is kept.
    fn parse_with_client_sasl_support(props: &[(&str, &str)]) -> Result<(), Error> {
        let mut sasl = SaslConfigs::default();
        for (key, value) in props {
            SaslConfigs::apply_sasl_config_key(&mut sasl, key, value)?;
        }
        assert_eq!(sasl, SaslConfigs::default());
        Ok(())
    }

    #[test]
    #[doc(alias = "org.apache.kafka.common.config.SaslConfigsTest#testSaslLoginRefreshMinValuesAreValid")]
    fn test_sasl_login_refresh_min_values_are_valid() {
        parse_with_client_sasl_support(&[
            (SaslConfigs::SASL_LOGIN_REFRESH_WINDOW_FACTOR, "0.5"),
            (SaslConfigs::SASL_LOGIN_REFRESH_WINDOW_JITTER, "0.0"),
            (SaslConfigs::SASL_LOGIN_REFRESH_MIN_PERIOD_SECONDS, "0"),
            (SaslConfigs::SASL_LOGIN_REFRESH_BUFFER_SECONDS, "0"),
        ])
        .unwrap();
    }

    #[test]
    #[doc(alias = "org.apache.kafka.common.config.SaslConfigsTest#testSaslLoginRefreshMaxValuesAreValid")]
    fn test_sasl_login_refresh_max_values_are_valid() {
        parse_with_client_sasl_support(&[
            (SaslConfigs::SASL_LOGIN_REFRESH_WINDOW_FACTOR, "1.0"),
            (SaslConfigs::SASL_LOGIN_REFRESH_WINDOW_JITTER, "0.25"),
            (SaslConfigs::SASL_LOGIN_REFRESH_MIN_PERIOD_SECONDS, "900"),
            (SaslConfigs::SASL_LOGIN_REFRESH_BUFFER_SECONDS, "3600"),
        ])
        .unwrap();
    }

    #[test]
    #[doc(
        alias = "org.apache.kafka.common.config.SaslConfigsTest#testSaslLoginRefreshWindowFactorMinValueIsReallyMinimum"
    )]
    fn test_sasl_login_refresh_window_factor_min_value_is_really_minimum() {
        assert_eq!(
            config_error(parse_with_client_sasl_support(&[(
                SaslConfigs::SASL_LOGIN_REFRESH_WINDOW_FACTOR,
                "0.499999"
            )])),
            "Invalid value 0.499999 for configuration sasl.login.refresh.window.factor: Value must be at least 0.5"
        );
    }

    #[test]
    #[doc(
        alias = "org.apache.kafka.common.config.SaslConfigsTest#testSaslLoginRefreshWindowFactorMaxValueIsReallyMaximum"
    )]
    fn test_sasl_login_refresh_window_factor_max_value_is_really_maximum() {
        assert_eq!(
            config_error(parse_with_client_sasl_support(&[(
                SaslConfigs::SASL_LOGIN_REFRESH_WINDOW_FACTOR,
                "1.0001"
            )])),
            "Invalid value 1.0001 for configuration sasl.login.refresh.window.factor: Value must be no more than 1.0"
        );
    }

    #[test]
    #[doc(
        alias = "org.apache.kafka.common.config.SaslConfigsTest#testSaslLoginRefreshWindowJitterMinValueIsReallyMinimum"
    )]
    fn test_sasl_login_refresh_window_jitter_min_value_is_really_minimum() {
        assert_eq!(
            config_error(parse_with_client_sasl_support(&[(
                SaslConfigs::SASL_LOGIN_REFRESH_WINDOW_JITTER,
                "-0.000001"
            )])),
            "Invalid value -1.0E-6 for configuration sasl.login.refresh.window.jitter: Value must be at least 0.0"
        );
    }

    #[test]
    #[doc(
        alias = "org.apache.kafka.common.config.SaslConfigsTest#testSaslLoginRefreshWindowJitterMaxValueIsReallyMaximum"
    )]
    fn test_sasl_login_refresh_window_jitter_max_value_is_really_maximum() {
        assert_eq!(
            config_error(parse_with_client_sasl_support(&[(
                SaslConfigs::SASL_LOGIN_REFRESH_WINDOW_JITTER,
                "0.251"
            )])),
            "Invalid value 0.251 for configuration sasl.login.refresh.window.jitter: Value must be no more than 0.25"
        );
    }

    #[test]
    #[doc(
        alias = "org.apache.kafka.common.config.SaslConfigsTest#testSaslLoginRefreshMinPeriodSecondsMinValueIsReallyMinimum"
    )]
    fn test_sasl_login_refresh_min_period_seconds_min_value_is_really_minimum() {
        assert_eq!(
            config_error(parse_with_client_sasl_support(&[(
                SaslConfigs::SASL_LOGIN_REFRESH_MIN_PERIOD_SECONDS,
                "-1"
            )])),
            "Invalid value -1 for configuration sasl.login.refresh.min.period.seconds: Value must be at least 0"
        );
    }

    #[test]
    #[doc(
        alias = "org.apache.kafka.common.config.SaslConfigsTest#testSaslLoginRefreshMinPeriodSecondsMaxValueIsReallyMaximum"
    )]
    fn test_sasl_login_refresh_min_period_seconds_max_value_is_really_maximum() {
        assert_eq!(
            config_error(parse_with_client_sasl_support(&[(
                SaslConfigs::SASL_LOGIN_REFRESH_MIN_PERIOD_SECONDS,
                "901"
            )])),
            "Invalid value 901 for configuration sasl.login.refresh.min.period.seconds: Value must be no more than 900"
        );
    }

    #[test]
    #[doc(
        alias = "org.apache.kafka.common.config.SaslConfigsTest#testSaslLoginRefreshBufferSecondsMinValueIsReallyMinimum"
    )]
    fn test_sasl_login_refresh_buffer_seconds_min_value_is_really_minimum() {
        assert_eq!(
            config_error(parse_with_client_sasl_support(&[(
                SaslConfigs::SASL_LOGIN_REFRESH_BUFFER_SECONDS,
                "-1"
            )])),
            "Invalid value -1 for configuration sasl.login.refresh.buffer.seconds: Value must be at least 0"
        );
    }

    #[test]
    #[doc(
        alias = "org.apache.kafka.common.config.SaslConfigsTest#testSaslLoginRefreshBufferSecondsMaxValueIsReallyMaximum"
    )]
    fn test_sasl_login_refresh_buffer_seconds_max_value_is_really_maximum() {
        assert_eq!(
            config_error(parse_with_client_sasl_support(&[(
                SaslConfigs::SASL_LOGIN_REFRESH_BUFFER_SECONDS,
                "3601"
            )])),
            "Invalid value 3601 for configuration sasl.login.refresh.buffer.seconds: Value must be no more than 3600"
        );
    }

    /// `sasl.mechanism` and `sasl.jaas.config` are kept, trimmed; a blank JAAS
    /// value is unset. Any other `sasl.*` key, such as the class-valued
    /// `sasl.login.class`, is an unknown key: accepted, and nothing is kept.
    #[test]
    fn test_apply_sasl_config_key() {
        let mut sasl = SaslConfigs::default();
        SaslConfigs::apply_sasl_config_key(&mut sasl, "sasl.mechanism", " PLAIN ").unwrap();
        SaslConfigs::apply_sasl_config_key(
            &mut sasl,
            "sasl.jaas.config",
            " PlainLoginModule required username=\"u\" password=\"p\"; ",
        )
        .unwrap();
        assert_eq!(sasl.mechanism, "PLAIN");
        assert_eq!(
            sasl.jaas_config.as_ref().map(Password::value),
            Some("PlainLoginModule required username=\"u\" password=\"p\";")
        );
        SaslConfigs::apply_sasl_config_key(&mut sasl, "sasl.jaas.config", " ").unwrap();
        assert_eq!(sasl.jaas_config, None);

        let before = sasl.clone();
        for key in [
            "sasl.login.class",
            "sasl.client.callback.handler.class",
            "sasl.login.callback.handler.class",
            "sasl.oauthbearer.jwt.retriever.class",
            "sasl.oauthbearer.jwt.validator.class",
            "sasl.unknown.key",
        ] {
            SaslConfigs::apply_sasl_config_key(&mut sasl, key, "anything").unwrap();
            assert!(!SaslConfigs::validate_unsupported_client_key(key, "anything").unwrap(), "{key}");
        }
        assert_eq!(sasl, before);
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
        let config = SaslConfigs {
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

    /// The finding's requested test: `{:?}` of a config holding credentials
    /// renders neither the password nor the JAAS string, while the non-secret
    /// fields stay visible and `resolve_*` still yield the plaintext.
    #[test]
    fn test_debug_redacts_jaas_config_and_password() {
        let config = SaslConfigs {
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
            "SaslConfigs { mechanism: \"PLAIN\", jaas_config: Some([hidden]), \
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
        let jaas_only = SaslConfigs { username: None, password: None, ..config };
        let rendered = format!("{jaas_only:?}");
        assert!(!rendered.contains("jaas-S3cr3t-Pa55"), "{rendered}");
        assert!(!rendered.contains("jaas-user"), "{rendered}");
        assert!(rendered.contains("jaas_config: Some([hidden])"), "{rendered}");
        assert_eq!(jaas_only.resolve_username(), Some("jaas-user"));
        assert_eq!(jaas_only.resolve_password(), Some("jaas-S3cr3t-Pa55"));
    }
}
