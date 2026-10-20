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

use crate::common::Error;
use crate::common::config::abstract_config::ValuesReader;
use crate::common::config::config_def::{
    CaseInsensitiveValidString, ConfigDef, DefaultValue, Importance, Range, Type, ValidList,
};
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
#[derive(Debug, Clone)]
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
    /// The `sasl.*` settings of a configuration parsed with
    /// [`Self::add_client_sasl_support`]'s definitions: `sasl.mechanism` and
    /// `sasl.jaas.config`, the two the client's PLAIN authenticator reads. An
    /// empty `sasl.jaas.config` counts as unset.
    ///
    /// # Errors
    ///
    /// [`Error::LocalIllegalState`] if a key's definition is missing or of
    /// another type, a defect of the definition rather than of the input.
    pub(crate) fn from_values(values: &mut ValuesReader<'_>) -> Result<SaslConfigs, Error> {
        Ok(SaslConfigs {
            mechanism: values.string(SaslConfigs::SASL_MECHANISM)?.to_string(),
            jaas_config: values
                .password_or_null(SaslConfigs::SASL_JAAS_CONFIG)?
                .filter(|jaas| !jaas.value().is_empty())
                .cloned(),
            username: None,
            password: None,
        })
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
    pub(crate) fn resolve_username(&self) -> Option<&str> {
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
    pub(crate) fn resolve_password(&self) -> Option<&str> {
        if let Some(ref p) = self.password {
            return Some(p.value());
        }
        if let Some(ref jaas) = self.jaas_config {
            return SaslConfigs::parse_jaas_option(jaas.value(), "password");
        }
        None
    }
}

// Documentation of the `sasl.*` keys, Java's `SaslConfigs.*_DOC` constants,
// which Java declares public and this crate keeps private: only the
// crate-private `ConfigDef` reads them.
const SASL_MECHANISM_DOC: &str = "SASL mechanism used for client connections. This may be any mechanism for which a security \
    provider is available. GSSAPI is the default mechanism.";
const SASL_JAAS_CONFIG_DOC: &str = "JAAS login context parameters for SASL connections in the format used by JAAS \
    configuration files. JAAS configuration file format is described <a \
    href=\"https://docs.oracle.com/javase/8/docs/technotes/guides/security/jgss/tutorials/LoginConfigFile.html\">here</a>. \
    The format for the value is: <code>loginModuleClass controlFlag \
    (optionName=optionValue)*;</code>. For brokers, the config must be prefixed with listener \
    prefix and SASL mechanism name in lower-case. For example, \
    listener.name.sasl_ssl.scram-sha-256.sasl.jaas.config=com.example.ScramLoginModule \
    required;";
const SASL_KERBEROS_SERVICE_NAME_DOC: &str = "The Kerberos principal name that Kafka runs as. This can be defined either in Kafka's JAAS \
    config or in Kafka's config.";
const SASL_KERBEROS_KINIT_CMD_DOC: &str = "Kerberos kinit command path.";
const SASL_KERBEROS_TICKET_RENEW_WINDOW_FACTOR_DOC: &str = "Login thread will sleep until the specified window factor of time from last refresh to \
    ticket's expiry has been reached, at which time it will try to renew the ticket.";
const SASL_KERBEROS_TICKET_RENEW_JITTER_DOC: &str = "Percentage of random jitter added to the renewal time.";
const SASL_KERBEROS_MIN_TIME_BEFORE_RELOGIN_DOC: &str = "Login thread sleep time between refresh attempts.";
const SASL_LOGIN_REFRESH_WINDOW_FACTOR_DOC: &str = "Login refresh thread will sleep until the specified window factor relative to the \
    credential's lifetime has been reached, at which time it will try to refresh the \
    credential. Legal values are between 0.5 (50%) and 1.0 (100%) inclusive; a default value \
    of 0.8 (80%) is used if no value is specified. Currently applies only to OAUTHBEARER.";
const SASL_LOGIN_REFRESH_WINDOW_JITTER_DOC: &str = "The maximum amount of random jitter relative to the credential's lifetime that is added to \
    the login refresh thread's sleep time. Legal values are between 0 and 0.25 (25%) \
    inclusive; a default value of 0.05 (5%) is used if no value is specified. Currently \
    applies only to OAUTHBEARER.";
const SASL_LOGIN_REFRESH_MIN_PERIOD_SECONDS_DOC: &str = "The desired minimum time for the login refresh thread to wait before refreshing a \
    credential, in seconds. Legal values are between 0 and 900 (15 minutes); a default value \
    of 60 (1 minute) is used if no value is specified.  This value and  \
    sasl.login.refresh.buffer.seconds are both ignored if their sum exceeds the remaining \
    lifetime of a credential. Currently applies only to OAUTHBEARER.";
const SASL_LOGIN_REFRESH_BUFFER_SECONDS_DOC: &str = "The amount of buffer time before credential expiration to maintain when refreshing a \
    credential, in seconds. If a refresh would otherwise occur closer to expiration than the \
    number of buffer seconds then the refresh will be moved up to maintain as much of the \
    buffer time as possible. Legal values are between 0 and 3600 (1 hour); a default value of  \
    300 (5 minutes) is used if no value is specified. This value and \
    sasl.login.refresh.min.period.seconds are both ignored if their sum exceeds the remaining \
    lifetime of a credential. Currently applies only to OAUTHBEARER.";
const SASL_LOGIN_CONNECT_TIMEOUT_MS_DOC: &str = "The (optional) value in milliseconds for the external authentication provider connection \
    timeout. Currently applies only to OAUTHBEARER.";
const SASL_LOGIN_READ_TIMEOUT_MS_DOC: &str = "The (optional) value in milliseconds for the external authentication provider read \
    timeout. Currently applies only to OAUTHBEARER.";
const SASL_LOGIN_RETRY_BACKOFF_MAX_MS_DOC: &str = "The (optional) value in milliseconds for the maximum wait between login attempts to the \
    external authentication provider. Login uses an exponential backoff algorithm with an \
    initial wait based on the sasl.login.retry.backoff.ms setting and will double in wait \
    length between attempts up to a maximum wait length specified by the \
    sasl.login.retry.backoff.max.ms setting. Currently applies only to OAUTHBEARER.";
const SASL_LOGIN_RETRY_BACKOFF_MS_DOC: &str = "The (optional) value in milliseconds for the initial wait between login attempts to the \
    external authentication provider. Login uses an exponential backoff algorithm with an \
    initial wait based on the sasl.login.retry.backoff.ms setting and will double in wait \
    length between attempts up to a maximum wait length specified by the \
    sasl.login.retry.backoff.max.ms setting. Currently applies only to OAUTHBEARER.";
const SASL_OAUTHBEARER_SCOPE_DOC: &str = "<p>This is the level of access a client application is granted to a resource or API which \
    is included in the token request. If provided, it should match one or more scopes \
    configured in the identity provider.</p><p>The scope was previously stored as part of the \
    <code>sasl.jaas.config</code> configuration with the key <code>scope</code>. For backward \
    compatibility, the <code>scope</code> JAAS option can still be used, but it is deprecated \
    and will be removed in a future version.</p><p>Order of \
    precedence:</p><ul><li><code>sasl.oauthbearer.scope</code> from \
    configuration</li><li><code>scope</code> from JAAS</li></ul>";
const SASL_OAUTHBEARER_CLIENT_CREDENTIALS_CLIENT_ID_DOC: &str = "<p>The ID (defined in/by the OAuth identity provider) to identify the client requesting \
    the token.</p><p>The client ID was previously stored as part of the \
    <code>sasl.jaas.config</code> configuration with the key <code>clientId</code>. For \
    backward compatibility, the <code>clientId</code> JAAS option can still be used, but it is \
    deprecated and will be removed in a future version.</p><p>Order of \
    precedence:</p><ul><li><code>sasl.oauthbearer.client.credentials.client.id</code> from \
    configuration</li><li><code>clientId</code> from JAAS</li></ul>";
const SASL_OAUTHBEARER_CLIENT_CREDENTIALS_CLIENT_SECRET_DOC: &str = "<p>The secret (defined by either the user or preassigned, depending on the identity \
    provider) of the client requesting the token.</p><p>The client secret was previously \
    stored as part of the <code>sasl.jaas.config</code> configuration with the key \
    <code>clientSecret</code>. For backward compatibility, the <code>clientSecret</code> JAAS \
    option can still be used, but it is deprecated and will be removed in a future \
    version.</p><p>Order of \
    precedence:</p><ul><li><code>sasl.oauthbearer.client.credentials.client.secret</code> from \
    configuration</li><li><code>clientSecret</code> from JAAS</li></ul>";
const SASL_OAUTHBEARER_ASSERTION_ALGORITHM_DOC: &str = "<p>The algorithm the Apache Kafka client should use to sign the assertion sent to the \
    identity provider. It is also used as the value of the OAuth <code>alg</code> (Algorithm) \
    header in the JWT assertion.</p><p><em>Note</em>: If a value for \
    <code>sasl.oauthbearer.assertion.file</code> is provided, this configuration will be \
    ignored.</p>";
const SASL_OAUTHBEARER_ASSERTION_CLAIM_AUD_DOC: &str = "<p>The JWT <code>aud</code> (Audience) claim which will be included in the  client JWT \
    assertion created locally.</p><p><em>Note</em>: If a value for \
    <code>sasl.oauthbearer.assertion.file</code> is provided, this configuration will be \
    ignored.</p>";
const SASL_OAUTHBEARER_ASSERTION_CLAIM_EXP_SECONDS_DOC: &str = "<p>The number of seconds <em>in the future</em> for which the JWT is valid. The value is \
    used to determine the JWT <code>exp</code> (Expiration) claim based on the current system \
    time when the JWT is created.</p><p>The formula to generate the <code>exp</code> claim is \
    very simple:</p><pre>Let:\n\n  x = the current timestamp in seconds, on client\n  y = the \
    value of this configuration\n\nThen:\n\n  exp = x + y\n</pre><p><em>Note</em>: If a value \
    for <code>sasl.oauthbearer.assertion.file</code> is provided, this configuration will be \
    ignored.</p>";
const SASL_OAUTHBEARER_ASSERTION_CLAIM_ISS_DOC: &str = "<p>The value to be used as the <code>iss</code> (Issuer) claim which will be included in \
    the client JWT assertion created locally.</p><p><em>Note</em>: If a value for \
    <code>sasl.oauthbearer.assertion.file</code> is provided, this configuration will be \
    ignored.</p>";
const SASL_OAUTHBEARER_ASSERTION_CLAIM_JTI_INCLUDE_DOC: &str = "<p>Flag that determines if the JWT assertion should generate a unique ID for the JWT and \
    include it in the <code>jti</code> (JWT ID) claim.</p><p><em>Note</em>: If a value for \
    <code>sasl.oauthbearer.assertion.file</code> is provided, this configuration will be \
    ignored.</p>";
const SASL_OAUTHBEARER_ASSERTION_CLAIM_NBF_SECONDS_DOC: &str = "<p>The number of seconds <em>in the past</em> from which the JWT is valid. The value is \
    used to determine the JWT <code>nbf</code> (Not Before) claim based on the current system \
    time when the JWT is created.</p><p>The formula to generate the <code>nbf</code> claim is \
    very simple:</p><pre>Let:\n\n  x = the current timestamp in seconds, on client\n  y = the \
    value of this configuration\n\nThen:\n\n  nbf = x - y\n</pre><p><em>Note</em>: If a value \
    for <code>sasl.oauthbearer.assertion.file</code> is provided, this configuration will be \
    ignored.</p>";
const SASL_OAUTHBEARER_ASSERTION_CLAIM_SUB_DOC: &str = "<p>The value to be used as the <code>sub</code> (Subject) claim which will be included in \
    the client JWT assertion created locally.</p><p><em>Note</em>: If a value for \
    <code>sasl.oauthbearer.assertion.file</code> is provided, this configuration will be \
    ignored.</p>";
const SASL_OAUTHBEARER_ASSERTION_FILE_DOC: &str = "<p>File that contains a <em>pre-generated</em> JWT assertion.</p><p>The underlying \
    implementation caches the file contents to avoid the performance hit of loading the file \
    on each access. The caching mechanism will detect whenthe file changes to allow for the \
    file to be reloaded on modifications. This allows for &quot;live&quot; assertion rotation \
    without restarting the Kafka client.</p><p>The file contains the assertion in the \
    serialized, three part JWT format:</p><ol><li>The <em>header</em> section is a base \
    64-encoded JWT header that contains values like <code>alg</code> (Algorithm), \
    <code>typ</code> (Type, always the literal value <code>JWT</code>), etc.</li><li>The \
    <em>payload</em> section includes the base 64-encoded set of JWT claims, such as \
    <code>aud</code> (Audience), <code>iss</code> (Issuer), <code>sub</code> (Subject), \
    etc.</li><li>The <em>signature</em> section is the concatenated <em>header</em> and \
    <em>payload</em> sections that was signed using a private key</li></ol><p>See <a \
    href=\"https://datatracker.ietf.org/doc/html/rfc7519\">RFC 7519</a> and <a \
    href=\"https://datatracker.ietf.org/doc/html/rfc7515\">RFC 7515</a> for more details on \
    the JWT and JWS formats.</p><p><em>Note</em>: If a value for \
    <code>sasl.oauthbearer.assertion.file</code> is provided, all other \
    <code>sasl.oauthbearer.assertion.</code>* configurations are ignored.</p>";
const SASL_OAUTHBEARER_ASSERTION_PRIVATE_KEY_FILE_DOC: &str = "<p>File that contains a private key in the standard PEM format which is used to sign the \
    JWT assertion sent to the identity provider.</p><p>The underlying implementation caches \
    the file contents to avoid the performance hit of loading the file on each access. The \
    caching mechanism will detect when the file changes to allow for the file to be reloaded \
    on modifications. This allows for &quot;live&quot; private key rotation without restarting \
    the Kafka client.</p><p><em>Note</em>: If a value for \
    <code>sasl.oauthbearer.assertion.file</code> is provided, this configuration will be \
    ignored.</p>";
const SASL_OAUTHBEARER_ASSERTION_PRIVATE_KEY_PASSPHRASE_DOC: &str = "<p>The optional passphrase to decrypt the private key file specified by \
    <code>sasl.oauthbearer.assertion.private.key.file</code>.</p><p><em>Note</em>: If the file \
    referred to by <code>sasl.oauthbearer.assertion.private.key.file</code> is modified on the \
    file system at runtime and it was created with a <em>different</em> passphrase than it was \
    previously, the client will not be able to access the private key file because the \
    passphrase is now out of date. For that reason, when using private key passphrases, either \
    use the same passphrase each time, or&mdash;for improved security&mdash;restart the Kafka \
    client using the new passphrase configuration.</p><p><em>Note</em>: If a value for \
    <code>sasl.oauthbearer.assertion.file</code> is provided, this configuration will be \
    ignored.</p>";
const SASL_OAUTHBEARER_ASSERTION_TEMPLATE_FILE_DOC: &str = "<p>This optional configuration specifies the file containing the JWT headers and/or \
    payload claims to be used when creating the JWT assertion.</p><p>Not all identity \
    providers require the same set of claims; some may require a given claim while others may \
    prohibit it. In order to provide the most flexibility, this configuration allows the user \
    to provide the static header values and claims that are to be included in the \
    JWT.</p><p><em>Note</em>: If a value for <code>sasl.oauthbearer.assertion.file</code> is \
    provided, this configuration will be ignored.</p>";
const SASL_OAUTHBEARER_SCOPE_CLAIM_NAME_DOC: &str = "The OAuth claim for the scope is often named \"scope\", but this (optional) setting can \
    provide a different name to use for the scope included in the JWT payload's claims if the \
    OAuth/OIDC provider uses a different name for that claim.";
const SASL_OAUTHBEARER_SUB_CLAIM_NAME_DOC: &str = "The OAuth claim for the subject is often named \"sub\", but this (optional) setting can \
    provide a different name to use for the subject included in the JWT payload's claims if \
    the OAuth/OIDC provider uses a different name for that claim.";
const SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL_DOC: &str = "The URL for the OAuth/OIDC identity provider. If the URL is HTTP(S)-based, it is the \
    issuer's token endpoint URL to which requests will be made to login based on the \
    configuration in <code>sasl.oauthbearer.jwt.retriever.class</code>. If the URL is \
    file-based, it specifies a file containing an access token (in JWT serialized form) issued \
    by the OAuth/OIDC identity provider to use for authorization.";
const SASL_OAUTHBEARER_JWKS_ENDPOINT_URL_DOC: &str = "The OAuth/OIDC provider URL from which the provider's <a \
    href=\"https://datatracker.ietf.org/doc/html/rfc7517#section-5\">JWKS (JSON Web Key \
    Set)</a> can be retrieved. The URL can be HTTP(S)-based or file-based. If the URL is \
    HTTP(S)-based, the JWKS data will be retrieved from the OAuth/OIDC provider via the \
    configured URL on broker startup. All then-current keys will be cached on the broker for \
    incoming requests. If an authentication request is received for a JWT that includes a \
    \"kid\" header claim value that isn't yet in the cache, the JWKS endpoint will be queried \
    again on demand. However, the broker polls the URL every \
    sasl.oauthbearer.jwks.endpoint.refresh.ms milliseconds to refresh the cache with any \
    forthcoming keys before any JWT requests that include them are received. If the URL is \
    file-based, the broker will load the JWKS file from a configured location on startup. In \
    the event that the JWT includes a \"kid\" header value that isn't in the JWKS file, the \
    broker will reject the JWT and authentication will fail.";
const SASL_OAUTHBEARER_JWKS_ENDPOINT_REFRESH_MS_DOC: &str = "The (optional) value in milliseconds for the broker to wait between refreshing its JWKS \
    (JSON Web Key Set) cache that contains the keys to verify the signature of the JWT.";
const SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MAX_MS_DOC: &str = "The (optional) value in milliseconds for the maximum wait between attempts to retrieve the \
    JWKS (JSON Web Key Set) from the external authentication provider. JWKS retrieval uses an \
    exponential backoff algorithm with an initial wait based on the \
    sasl.oauthbearer.jwks.endpoint.retry.backoff.ms setting and will double in wait length \
    between attempts up to a maximum wait length specified by the \
    sasl.oauthbearer.jwks.endpoint.retry.backoff.max.ms setting.";
const SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MS_DOC: &str = "The (optional) value in milliseconds for the initial wait between JWKS (JSON Web Key Set) \
    retrieval attempts from the external authentication provider. JWKS retrieval uses an \
    exponential backoff algorithm with an initial wait based on the \
    sasl.oauthbearer.jwks.endpoint.retry.backoff.ms setting and will double in wait length \
    between attempts up to a maximum wait length specified by the \
    sasl.oauthbearer.jwks.endpoint.retry.backoff.max.ms setting.";
const SASL_OAUTHBEARER_CLOCK_SKEW_SECONDS_DOC: &str = "The (optional) value in seconds to allow for differences between the time of the \
    OAuth/OIDC identity provider and the broker.";
const SASL_OAUTHBEARER_ALLOW_UNVERIFIED_AUDIENCE_DOC: &str = "The (optional) setting that allows the broker to validate JWTs without verifying the \
    audience. By default the broker requires sasl.oauthbearer.expected.audience to be set \
    whenever sasl.oauthbearer.jwks.endpoint.url is configured, and rejects any JWT whose \
    \"aud\" claim does not match one of the expected audiences. When this is set to true that \
    requirement is lifted: the \"aud\" claim is no longer checked and a JWT bearing any (or \
    no) audience is accepted. Leaving the audience unverified is insecure and strongly \
    discouraged; enable it only for deployments that intentionally accept tokens regardless of \
    their intended audience. The default value is 'false'.";
const SASL_OAUTHBEARER_EXPECTED_AUDIENCE_DOC: &str = "The comma-delimited setting for the broker to use to verify that the JWT was issued for \
    one of the expected audiences. The JWT will be inspected for the standard OAuth \"aud\" \
    claim and the broker will match the value(s) from the JWT's \"aud\" claim to see if there \
    is an exact match. If there is no match, the broker will reject the JWT and authentication \
    will fail. Required if sasl.oauthbearer.jwks.endpoint.url is set, with the ability to \
    opt-out using sasl.oauthbearer.allow.unverified.audience.";
const SASL_OAUTHBEARER_ALLOW_UNVERIFIED_ISSUER_DOC: &str = "The (optional) setting that allows the broker to validate JWTs without verifying the \
    issuer. By default the broker requires sasl.oauthbearer.expected.issuer to be set whenever \
    sasl.oauthbearer.jwks.endpoint.url is configured, and rejects any JWT whose \"iss\" claim \
    does not match the expected issuer. When this is set to true that requirement is lifted: \
    the \"iss\" claim is no longer checked and a JWT bearing any (or no) issuer is accepted. \
    Leaving the issuer unverified is insecure and strongly discouraged; enable it only for \
    deployments that intentionally accept tokens regardless of which issuer minted them. The \
    default value is 'false'.";
const SASL_OAUTHBEARER_EXPECTED_ISSUER_DOC: &str = "The setting for the broker to use to verify that the JWT was created by the expected \
    issuer. The JWT will be inspected for the standard OAuth \"iss\" claim and the broker will \
    match it exactly against what is in the JWT's \"iss\" claim. If there is no match, the \
    broker will reject the JWT and authentication will fail. Required if \
    sasl.oauthbearer.jwks.endpoint.url is set, with the ability to opt-out using \
    sasl.oauthbearer.allow.unverified.issuer.";
const SASL_OAUTHBEARER_HEADER_URLENCODE_DOC: &str = "The (optional) setting to enable the OAuth client to URL-encode the client_id and \
    client_secret in the authorization header in accordance with RFC6749, see <a \
    href=\"https://datatracker.ietf.org/doc/html/rfc6749#section-2.3.1\">here</a> for more \
    details. The default value is set to 'false' for backward compatibility";

impl SaslConfigs {
    /// Defines the client `sasl.*` keys on `config`: Java's
    /// `SaslConfigs.addClientSaslSupport(ConfigDef)`, in Java's order, except
    /// the class-valued keys (CLAUDE.md §2): `sasl.client.callback.handler.class`,
    /// `sasl.login.callback.handler.class`, `sasl.login.class`,
    /// `sasl.oauthbearer.jwt.retriever.class` and
    /// `sasl.oauthbearer.jwt.validator.class`.
    ///
    /// Java names each key by a public `SaslConfigs` constant; only the ones
    /// this crate already exports are referenced by name, the others are
    /// written as literals to add no public constant.
    pub(crate) fn add_client_sasl_support(config: ConfigDef) -> ConfigDef {
        config
            .define(
                "sasl.kerberos.service.name",
                Type::String,
                DefaultValue::Null,
                Importance::Medium,
                SASL_KERBEROS_SERVICE_NAME_DOC,
            )
            .define(
                "sasl.kerberos.kinit.cmd",
                Type::String,
                "/usr/bin/kinit",
                Importance::Low,
                SASL_KERBEROS_KINIT_CMD_DOC,
            )
            .define(
                "sasl.kerberos.ticket.renew.window.factor",
                Type::Double,
                0.80,
                Importance::Low,
                SASL_KERBEROS_TICKET_RENEW_WINDOW_FACTOR_DOC,
            )
            .define(
                "sasl.kerberos.ticket.renew.jitter",
                Type::Double,
                0.05,
                Importance::Low,
                SASL_KERBEROS_TICKET_RENEW_JITTER_DOC,
            )
            .define(
                "sasl.kerberos.min.time.before.relogin",
                Type::Long,
                60_000i64,
                Importance::Low,
                SASL_KERBEROS_MIN_TIME_BEFORE_RELOGIN_DOC,
            )
            .define_with_validator(
                "sasl.login.refresh.window.factor",
                Type::Double,
                0.80,
                Range::between(0.5, 1.0),
                Importance::Low,
                SASL_LOGIN_REFRESH_WINDOW_FACTOR_DOC,
            )
            .define_with_validator(
                "sasl.login.refresh.window.jitter",
                Type::Double,
                0.05,
                Range::between(0.0, 0.25),
                Importance::Low,
                SASL_LOGIN_REFRESH_WINDOW_JITTER_DOC,
            )
            .define_with_validator(
                "sasl.login.refresh.min.period.seconds",
                Type::Short,
                60i16,
                Range::between(0, 900),
                Importance::Low,
                SASL_LOGIN_REFRESH_MIN_PERIOD_SECONDS_DOC,
            )
            .define_with_validator(
                "sasl.login.refresh.buffer.seconds",
                Type::Short,
                300i16,
                Range::between(0, 3600),
                Importance::Low,
                SASL_LOGIN_REFRESH_BUFFER_SECONDS_DOC,
            )
            .define(
                SaslConfigs::SASL_MECHANISM,
                Type::String,
                SaslConfigs::DEFAULT_SASL_MECHANISM,
                Importance::Medium,
                SASL_MECHANISM_DOC,
            )
            .define(
                SaslConfigs::SASL_JAAS_CONFIG,
                Type::Password,
                DefaultValue::Null,
                Importance::Medium,
                SASL_JAAS_CONFIG_DOC,
            )
            .define(
                "sasl.login.connect.timeout.ms",
                Type::Int,
                DefaultValue::Null,
                Importance::Low,
                SASL_LOGIN_CONNECT_TIMEOUT_MS_DOC,
            )
            .define(
                "sasl.login.read.timeout.ms",
                Type::Int,
                DefaultValue::Null,
                Importance::Low,
                SASL_LOGIN_READ_TIMEOUT_MS_DOC,
            )
            .define(
                "sasl.login.retry.backoff.max.ms",
                Type::Long,
                10_000i64,
                Importance::Low,
                SASL_LOGIN_RETRY_BACKOFF_MAX_MS_DOC,
            )
            .define(
                "sasl.login.retry.backoff.ms",
                Type::Long,
                100i64,
                Importance::Low,
                SASL_LOGIN_RETRY_BACKOFF_MS_DOC,
            )
            .define(
                "sasl.oauthbearer.scope",
                Type::String,
                DefaultValue::Null,
                Importance::Medium,
                SASL_OAUTHBEARER_SCOPE_DOC,
            )
            .define(
                "sasl.oauthbearer.client.credentials.client.id",
                Type::String,
                DefaultValue::Null,
                Importance::Medium,
                SASL_OAUTHBEARER_CLIENT_CREDENTIALS_CLIENT_ID_DOC,
            )
            .define(
                "sasl.oauthbearer.client.credentials.client.secret",
                Type::Password,
                DefaultValue::Null,
                Importance::Medium,
                SASL_OAUTHBEARER_CLIENT_CREDENTIALS_CLIENT_SECRET_DOC,
            )
            .define_with_validator(
                "sasl.oauthbearer.assertion.algorithm",
                Type::String,
                "RS256",
                CaseInsensitiveValidString::r#in(&["ES256", "RS256"]),
                Importance::Medium,
                SASL_OAUTHBEARER_ASSERTION_ALGORITHM_DOC,
            )
            .define(
                "sasl.oauthbearer.assertion.claim.aud",
                Type::String,
                DefaultValue::Null,
                Importance::Medium,
                SASL_OAUTHBEARER_ASSERTION_CLAIM_AUD_DOC,
            )
            .define_with_validator(
                "sasl.oauthbearer.assertion.claim.exp.seconds",
                Type::Int,
                300,
                Range::between(0, 86400),
                Importance::Low,
                SASL_OAUTHBEARER_ASSERTION_CLAIM_EXP_SECONDS_DOC,
            )
            .define(
                "sasl.oauthbearer.assertion.claim.iss",
                Type::String,
                DefaultValue::Null,
                Importance::Medium,
                SASL_OAUTHBEARER_ASSERTION_CLAIM_ISS_DOC,
            )
            .define(
                "sasl.oauthbearer.assertion.claim.jti.include",
                Type::Boolean,
                false,
                Importance::Medium,
                SASL_OAUTHBEARER_ASSERTION_CLAIM_JTI_INCLUDE_DOC,
            )
            .define_with_validator(
                "sasl.oauthbearer.assertion.claim.nbf.seconds",
                Type::Int,
                60,
                Range::between(0, 3600),
                Importance::Low,
                SASL_OAUTHBEARER_ASSERTION_CLAIM_NBF_SECONDS_DOC,
            )
            .define(
                "sasl.oauthbearer.assertion.claim.sub",
                Type::String,
                DefaultValue::Null,
                Importance::Medium,
                SASL_OAUTHBEARER_ASSERTION_CLAIM_SUB_DOC,
            )
            .define(
                "sasl.oauthbearer.assertion.file",
                Type::String,
                DefaultValue::Null,
                Importance::Medium,
                SASL_OAUTHBEARER_ASSERTION_FILE_DOC,
            )
            .define(
                "sasl.oauthbearer.assertion.private.key.file",
                Type::String,
                DefaultValue::Null,
                Importance::Medium,
                SASL_OAUTHBEARER_ASSERTION_PRIVATE_KEY_FILE_DOC,
            )
            .define(
                "sasl.oauthbearer.assertion.private.key.passphrase",
                Type::Password,
                DefaultValue::Null,
                Importance::Medium,
                SASL_OAUTHBEARER_ASSERTION_PRIVATE_KEY_PASSPHRASE_DOC,
            )
            .define(
                "sasl.oauthbearer.assertion.template.file",
                Type::String,
                DefaultValue::Null,
                Importance::Medium,
                SASL_OAUTHBEARER_ASSERTION_TEMPLATE_FILE_DOC,
            )
            .define(
                "sasl.oauthbearer.scope.claim.name",
                Type::String,
                "scope",
                Importance::Low,
                SASL_OAUTHBEARER_SCOPE_CLAIM_NAME_DOC,
            )
            .define(
                "sasl.oauthbearer.sub.claim.name",
                Type::String,
                "sub",
                Importance::Low,
                SASL_OAUTHBEARER_SUB_CLAIM_NAME_DOC,
            )
            .define(
                "sasl.oauthbearer.token.endpoint.url",
                Type::String,
                DefaultValue::Null,
                Importance::Medium,
                SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL_DOC,
            )
            .define(
                "sasl.oauthbearer.jwks.endpoint.url",
                Type::String,
                DefaultValue::Null,
                Importance::Medium,
                SASL_OAUTHBEARER_JWKS_ENDPOINT_URL_DOC,
            )
            .define(
                "sasl.oauthbearer.jwks.endpoint.refresh.ms",
                Type::Long,
                3_600_000i64,
                Importance::Low,
                SASL_OAUTHBEARER_JWKS_ENDPOINT_REFRESH_MS_DOC,
            )
            .define(
                "sasl.oauthbearer.jwks.endpoint.retry.backoff.max.ms",
                Type::Long,
                10_000i64,
                Importance::Low,
                SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MAX_MS_DOC,
            )
            .define(
                "sasl.oauthbearer.jwks.endpoint.retry.backoff.ms",
                Type::Long,
                100i64,
                Importance::Low,
                SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MS_DOC,
            )
            .define(
                "sasl.oauthbearer.clock.skew.seconds",
                Type::Int,
                30,
                Importance::Low,
                SASL_OAUTHBEARER_CLOCK_SKEW_SECONDS_DOC,
            )
            .define(
                "sasl.oauthbearer.allow.unverified.audience",
                Type::Boolean,
                false,
                Importance::Medium,
                SASL_OAUTHBEARER_ALLOW_UNVERIFIED_AUDIENCE_DOC,
            )
            .define_with_validator(
                "sasl.oauthbearer.expected.audience",
                Type::List,
                Vec::<String>::new(),
                ValidList::any_non_duplicate_values(true, false),
                Importance::High,
                SASL_OAUTHBEARER_EXPECTED_AUDIENCE_DOC,
            )
            .define(
                "sasl.oauthbearer.allow.unverified.issuer",
                Type::Boolean,
                false,
                Importance::Medium,
                SASL_OAUTHBEARER_ALLOW_UNVERIFIED_ISSUER_DOC,
            )
            .define(
                "sasl.oauthbearer.expected.issuer",
                Type::String,
                DefaultValue::Null,
                Importance::High,
                SASL_OAUTHBEARER_EXPECTED_ISSUER_DOC,
            )
            .define(
                "sasl.oauthbearer.header.urlencode",
                Type::Boolean,
                false,
                Importance::Low,
                SASL_OAUTHBEARER_HEADER_URLENCODE_DOC,
            )
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
