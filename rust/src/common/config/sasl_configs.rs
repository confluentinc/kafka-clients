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
//! `sasl.mechanism` keeps Java's definition and its `GSSAPI` default, but PLAIN
//! is the only mechanism this client implements: under a SASL
//! `security.protocol` the client configurations reject any other value when
//! they are built (`CommonClientConfigs::post_validate_supported_sasl_mechanism`),
//! so `sasl.mechanism=PLAIN` must be set explicitly.
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
use crate::common::security::JaasContext;

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

    /// The PLAIN mechanism name, the only one the client authenticator implements.
    pub(crate) const PLAIN_MECHANISM: &str = "PLAIN";

    /// Default SASL mechanism (matches Java `DEFAULT_SASL_MECHANISM`).
    pub const DEFAULT_SASL_MECHANISM: &str = Self::GSSAPI_MECHANISM;
}

/// SASL configuration for Kafka connections.
///
/// The Java class
/// hosts only the `sasl.*` config keys and defaults; the values themselves
/// travel in the `Map<String, ?>` handed to `SaslChannelBuilder.configure`.
/// The Rust struct keeps Java's name and constants, and additionally holds the
/// client-relevant subset of those values, parsed once from the client config
/// properties. The fields are crate-private: callers set them through the
/// `sasl.*` properties of a client config, as a Java caller does.
///
/// Currently supports PLAIN mechanism only.
#[derive(Debug)]
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
    /// Parsed, as Java's `JaasConfig` parses it, when a `SASL_PLAINTEXT` or
    /// `SASL_SSL` channel builder is created
    /// ([`JaasContext::load_client_context`]); the credentials are then the
    /// login module's `username` and `password` options
    /// ([`SaslConfigs::resolve_username`], [`SaslConfigs::resolve_password`]).
    ///
    /// A [`Password`] because Java defines `sasl.jaas.config` as
    /// `ConfigDef.Type.PASSWORD` (`org.apache.kafka.common.config.SaslConfigs#addClientSaslSupport`): the string embeds
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
    /// `sasl.jaas.config`, the two the client's PLAIN authenticator reads.
    ///
    /// `sasl.jaas.config` is kept as given, an empty value included: Java's
    /// `ConfigDef` parses one into `Password("")`, which the channel builder
    /// then rejects ([`JaasContext::load_client_context`]).
    ///
    /// # Errors
    ///
    /// [`Error::LocalIllegalState`] if a key's definition is missing or of
    /// another type, a defect of the definition rather than of the input.
    pub(crate) fn from_values(values: &mut ValuesReader<'_>) -> Result<SaslConfigs, Error> {
        Ok(SaslConfigs {
            mechanism: values.string(SaslConfigs::SASL_MECHANISM)?.to_string(),
            jaas_config: values.password_or_null(SaslConfigs::SASL_JAAS_CONFIG)?.cloned(),
            username: None,
            password: None,
        })
    }

    /// The username the PLAIN authenticator sends: the `username` field if
    /// set, else the `username` option of the login module of `jaas_context`,
    /// the context of `sasl.jaas.config` ([`JaasContext::load_client_context`]),
    /// which Java's `PlainLoginModule#initialize` puts in the client's
    /// `Subject`.
    pub(crate) fn resolve_username<'a>(&'a self, jaas_context: Option<&'a JaasContext>) -> Option<&'a str> {
        if let Some(ref u) = self.username {
            return Some(u);
        }
        jaas_context
            .and_then(|context| JaasContext::config_entry_option(context.configuration_entries(), "username", None))
    }

    /// The password the PLAIN authenticator sends: the `password` field if
    /// set, else the `password` option of the login module of `jaas_context`,
    /// as for [`Self::resolve_username`].
    ///
    /// Returns the **plaintext**: the authenticator must send it, just as
    /// Java's `PlainLoginModule` yields a plain `String`. Never log the result.
    pub(crate) fn resolve_password<'a>(&'a self, jaas_context: Option<&'a JaasContext>) -> Option<&'a str> {
        if let Some(ref p) = self.password {
            return Some(p.value());
        }
        jaas_context
            .and_then(|context| JaasContext::config_entry_option(context.configuration_entries(), "password", None))
    }
}

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
            )
            .define("sasl.kerberos.kinit.cmd", Type::String, "/usr/bin/kinit", Importance::Low)
            .define("sasl.kerberos.ticket.renew.window.factor", Type::Double, 0.80, Importance::Low)
            .define("sasl.kerberos.ticket.renew.jitter", Type::Double, 0.05, Importance::Low)
            .define("sasl.kerberos.min.time.before.relogin", Type::Long, 60_000i64, Importance::Low)
            .define_with_validator(
                "sasl.login.refresh.window.factor",
                Type::Double,
                0.80,
                Range::between(0.5, 1.0),
                Importance::Low,
            )
            .define_with_validator(
                "sasl.login.refresh.window.jitter",
                Type::Double,
                0.05,
                Range::between(0.0, 0.25),
                Importance::Low,
            )
            .define_with_validator(
                "sasl.login.refresh.min.period.seconds",
                Type::Short,
                60i16,
                Range::between(0, 900),
                Importance::Low,
            )
            .define_with_validator(
                "sasl.login.refresh.buffer.seconds",
                Type::Short,
                300i16,
                Range::between(0, 3600),
                Importance::Low,
            )
            .define(
                SaslConfigs::SASL_MECHANISM,
                Type::String,
                SaslConfigs::DEFAULT_SASL_MECHANISM,
                Importance::Medium,
            )
            .define(
                SaslConfigs::SASL_JAAS_CONFIG,
                Type::Password,
                DefaultValue::Null,
                Importance::Medium,
            )
            .define("sasl.login.connect.timeout.ms", Type::Int, DefaultValue::Null, Importance::Low)
            .define("sasl.login.read.timeout.ms", Type::Int, DefaultValue::Null, Importance::Low)
            .define("sasl.login.retry.backoff.max.ms", Type::Long, 10_000i64, Importance::Low)
            .define("sasl.login.retry.backoff.ms", Type::Long, 100i64, Importance::Low)
            .define("sasl.oauthbearer.scope", Type::String, DefaultValue::Null, Importance::Medium)
            .define(
                "sasl.oauthbearer.client.credentials.client.id",
                Type::String,
                DefaultValue::Null,
                Importance::Medium,
            )
            .define(
                "sasl.oauthbearer.client.credentials.client.secret",
                Type::Password,
                DefaultValue::Null,
                Importance::Medium,
            )
            .define_with_validator(
                "sasl.oauthbearer.assertion.algorithm",
                Type::String,
                "RS256",
                CaseInsensitiveValidString::r#in(&["ES256", "RS256"]),
                Importance::Medium,
            )
            .define(
                "sasl.oauthbearer.assertion.claim.aud",
                Type::String,
                DefaultValue::Null,
                Importance::Medium,
            )
            .define_with_validator(
                "sasl.oauthbearer.assertion.claim.exp.seconds",
                Type::Int,
                300,
                Range::between(0, 86400),
                Importance::Low,
            )
            .define(
                "sasl.oauthbearer.assertion.claim.iss",
                Type::String,
                DefaultValue::Null,
                Importance::Medium,
            )
            .define(
                "sasl.oauthbearer.assertion.claim.jti.include",
                Type::Boolean,
                false,
                Importance::Medium,
            )
            .define_with_validator(
                "sasl.oauthbearer.assertion.claim.nbf.seconds",
                Type::Int,
                60,
                Range::between(0, 3600),
                Importance::Low,
            )
            .define(
                "sasl.oauthbearer.assertion.claim.sub",
                Type::String,
                DefaultValue::Null,
                Importance::Medium,
            )
            .define(
                "sasl.oauthbearer.assertion.file",
                Type::String,
                DefaultValue::Null,
                Importance::Medium,
            )
            .define(
                "sasl.oauthbearer.assertion.private.key.file",
                Type::String,
                DefaultValue::Null,
                Importance::Medium,
            )
            .define(
                "sasl.oauthbearer.assertion.private.key.passphrase",
                Type::Password,
                DefaultValue::Null,
                Importance::Medium,
            )
            .define(
                "sasl.oauthbearer.assertion.template.file",
                Type::String,
                DefaultValue::Null,
                Importance::Medium,
            )
            .define("sasl.oauthbearer.scope.claim.name", Type::String, "scope", Importance::Low)
            .define("sasl.oauthbearer.sub.claim.name", Type::String, "sub", Importance::Low)
            .define(
                "sasl.oauthbearer.token.endpoint.url",
                Type::String,
                DefaultValue::Null,
                Importance::Medium,
            )
            .define(
                "sasl.oauthbearer.jwks.endpoint.url",
                Type::String,
                DefaultValue::Null,
                Importance::Medium,
            )
            .define(
                "sasl.oauthbearer.jwks.endpoint.refresh.ms",
                Type::Long,
                3_600_000i64,
                Importance::Low,
            )
            .define(
                "sasl.oauthbearer.jwks.endpoint.retry.backoff.max.ms",
                Type::Long,
                10_000i64,
                Importance::Low,
            )
            .define(
                "sasl.oauthbearer.jwks.endpoint.retry.backoff.ms",
                Type::Long,
                100i64,
                Importance::Low,
            )
            .define("sasl.oauthbearer.clock.skew.seconds", Type::Int, 30, Importance::Low)
            .define(
                "sasl.oauthbearer.allow.unverified.audience",
                Type::Boolean,
                false,
                Importance::Medium,
            )
            .define_with_validator(
                "sasl.oauthbearer.expected.audience",
                Type::List,
                Vec::<String>::new(),
                ValidList::any_non_duplicate_values(true, false),
                Importance::High,
            )
            .define(
                "sasl.oauthbearer.allow.unverified.issuer",
                Type::Boolean,
                false,
                Importance::Medium,
            )
            .define(
                "sasl.oauthbearer.expected.issuer",
                Type::String,
                DefaultValue::Null,
                Importance::High,
            )
            .define("sasl.oauthbearer.header.urlencode", Type::Boolean, false, Importance::Low)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The context `sasl.jaas.config` = `jaas` loads, and the username and
    /// password resolved from it.
    fn jaas_credentials(jaas: &str) -> Result<(Option<String>, Option<String>), Error> {
        let config = SaslConfigs { jaas_config: Some(Password::new(jaas)), ..SaslConfigs::default() };
        let context = JaasContext::load_client_context(&config)?;
        Ok((
            config.resolve_username(context.as_ref()).map(str::to_owned),
            config.resolve_password(context.as_ref()).map(str::to_owned),
        ))
    }

    /// The username and password `jaas` resolves to.
    fn credentials(jaas: &str) -> (Option<String>, Option<String>) {
        jaas_credentials(jaas).unwrap_or_else(|e| panic!("{jaas:?} must parse: {e:?}"))
    }

    /// The message Java's `JaasConfig` rejects `jaas` with.
    fn rejection(jaas: &str) -> String {
        let error = jaas_credentials(jaas).expect_err("Java rejects the value");
        assert!(error.is_local_illegal_argument_error(), "{jaas:?}: {error:?}");
        error.message().to_owned()
    }

    fn some(username: &str, password: &str) -> (Option<String>, Option<String>) {
        (Some(username.to_owned()), Some(password.to_owned()))
    }

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
        assert_eq!(config.resolve_username(None), Some("alice"));
    }

    #[test]
    fn test_resolve_password_from_direct_field() {
        let config = SaslConfigs { password: Some(Password::new("secret")), ..SaslConfigs::default() };
        assert_eq!(config.resolve_password(None), Some("secret"));
    }

    #[test]
    fn test_resolve_from_jaas_config() {
        assert_eq!(
            credentials(
                "org.apache.kafka.common.security.plain.PlainLoginModule required \
                 username=\"alice\" password=\"secret\";"
            ),
            some("alice", "secret")
        );
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
        let context = JaasContext::load_client_context(&config).unwrap();
        assert_eq!(config.resolve_username(context.as_ref()), Some("direct_user"));
        assert_eq!(config.resolve_password(context.as_ref()), Some("direct_pass"));
    }

    #[test]
    fn test_resolve_from_empty_config() {
        let config = SaslConfigs::default();
        assert!(JaasContext::load_client_context(&config).unwrap().is_none());
        assert_eq!(config.resolve_username(None), None);
        assert_eq!(config.resolve_password(None), None);
    }

    #[test]
    fn test_jaas_config_unquoted_values() {
        assert_eq!(
            credentials("PlainLoginModule required username=alice password=secret;"),
            some("alice", "secret")
        );
    }

    #[test]
    fn test_jaas_config_missing_field() {
        assert_eq!(
            credentials("PlainLoginModule required username=\"alice\";"),
            (Some("alice".to_owned()), None)
        );
    }

    /// A quote that is never closed takes the rest of the option with it, and
    /// Java then rejects the value: `"alice password="` is `username`'s value,
    /// `secret` an option key, and the quote before `;` a string where its
    /// `=` should be.
    #[test]
    fn test_jaas_config_malformed_no_closing_quote() {
        assert_eq!(
            rejection("PlainLoginModule required username=\"alice password=\"secret\";"),
            "Value not specified for key 'secret' in JAAS config"
        );
    }

    /// An empty `sasl.jaas.config` names no login module, which Java rejects.
    #[test]
    fn test_jaas_config_empty_string() {
        assert_eq!(rejection(""), "Login module not specified in JAAS config");
    }

    #[test]
    fn test_jaas_config_no_options() {
        assert_eq!(credentials("PlainLoginModule required;"), (None, None));
    }

    #[test]
    fn test_jaas_config_with_extra_whitespace() {
        assert_eq!(
            credentials("PlainLoginModule   required   username=\"bob\"   password=\"pass123\"  ;"),
            some("bob", "pass123")
        );
    }

    #[test]
    fn test_jaas_config_key_as_substring_not_matched() {
        // `myusername` is an option of its own, not `username`.
        assert_eq!(
            credentials("PlainLoginModule required myusername=\"wrong\" username=\"right\";")
                .0
                .as_deref(),
            Some("right")
        );
    }

    #[test]
    fn test_jaas_config_spaces_around_equals() {
        // A JAAS string legally carries spaces around '='.
        assert_eq!(
            credentials("PlainLoginModule required username = \"alice\" password = \"secret\";"),
            some("alice", "secret")
        );
    }

    #[test]
    fn test_jaas_config_single_quoted_values() {
        assert_eq!(
            credentials("PlainLoginModule required username='alice' password='secret';"),
            some("alice", "secret")
        );
    }

    #[test]
    fn test_jaas_config_single_quoted_with_spaces() {
        assert_eq!(
            credentials("PlainLoginModule required username = 'alice' password = 'secret';"),
            some("alice", "secret")
        );
    }

    #[test]
    fn test_jaas_config_bare_value_with_spaces_around_equals() {
        assert_eq!(
            credentials("PlainLoginModule required username = alice password = secret;"),
            some("alice", "secret")
        );
    }

    #[test]
    fn test_jaas_config_service_name_does_not_false_match() {
        // `serviceName` and `foo.username` are options of their own.
        assert_eq!(
            credentials(
                "com.sun.security.auth.module.Krb5LoginModule required \
                 serviceName=\"kafka\" foo.username=\"wrong\" username=\"right\" \
                 password=\"pass\";"
            ),
            some("right", "pass")
        );
    }

    #[test]
    fn test_jaas_config_namespace_does_not_false_match_name() {
        assert_eq!(credentials("m required usernamespace=\"x\";"), (None, None));
    }

    #[test]
    fn test_jaas_config_key_inside_a_quoted_value_does_not_match() {
        // A `key=` inside another option's quoted value is part of that value.
        assert_eq!(
            credentials("PlainLoginModule required password=\"username=x\" username=\"right\";"),
            some("right", "username=x")
        );
    }

    #[test]
    fn test_jaas_config_escaped_quote_in_value_does_not_split_or_misalign() {
        // An escaped quote neither ends the value nor misaligns the options
        // after it, and is decoded.
        assert_eq!(
            credentials("PlainLoginModule required password=\"a\\\"b\" username=\"right\";"),
            some("right", "a\"b")
        );
    }

    fn jaas_password(value: &str) -> Option<String> {
        credentials(&format!(
            "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"u\" password={value};"
        ))
        .1
    }

    /// Quoted values decode their escapes as Java's `JaasConfig` does. Each
    /// expected value is what `new JaasConfig("KafkaClient", jaas)` returns for
    /// the `password` option on Kafka 4.4.0, printed code point by code point.
    #[test]
    fn test_jaas_config_quoted_value_escapes_are_decoded_like_java() {
        let cases: &[(&str, &str)] = &[
            // A backslash and a double quote, escaped as Java requires.
            (r#""b\\a\"ck""#, "b\\a\"ck"),
            // An escaped single quote inside single quotes.
            (r"'it\'s'", "it's"),
            // The control-character escapes.
            (r#""\a\b\f\n\r\t\v""#, "\u{7}\u{8}\u{c}\n\r\t\u{b}"),
            // Octal escapes take three digits only when the first is 0 to 3
            // (`\477` is `\47` then `7`), stop at a non-octal digit (`\08` is
            // NUL then `8`), and any other escaped character is itself (`\8`,
            // `\q`).
            (r#""\101\0\477\8\q\08\377\400""#, "A\u{0}'78q\u{0}8\u{ff} 0"),
            (r#""tab\there""#, "tab\there"),
            // A non-ASCII character, plain and escaped.
            (r#""é\é""#, "éé"),
            // A Windows path written with doubled backslashes.
            (r#""C:\\temp\\x""#, "C:\\temp\\x"),
        ];
        for (value, expected) in cases {
            assert_eq!(jaas_password(value).as_deref(), Some(*expected), "password={value}");
        }
    }

    /// Java's `StreamTokenizer` ends a quoted string at a raw line terminator,
    /// and `JaasConfig` then rejects the whole config ("Value not specified for
    /// key 'cd' in JAAS config" on Kafka 4.4.0 for `password="ab<LF>cd"`). An
    /// escaped line terminator is part of the value, and one between options
    /// is only whitespace.
    #[test]
    fn test_jaas_config_raw_line_terminator_ends_a_quoted_value() {
        for raw in ["\n", "\r"] {
            assert_eq!(
                rejection(&format!(
                    "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"u\" \
                     password=\"ab{raw}cd\";"
                )),
                "Value not specified for key 'cd' in JAAS config",
                "{raw:?}"
            );
        }
        assert_eq!(jaas_password("\"ab\\\ncd\"").as_deref(), Some("ab\ncd"));
        assert_eq!(
            credentials("PlainLoginModule required username=\"u\"\n password=\"abcd\";"),
            some("u", "abcd")
        );
    }

    /// A backslash is not a word character, so a bare value cannot hold one:
    /// Java's tokenizer ends the value `a` there and reads `\` as an option
    /// key, which `JaasConfig` rejects as Java does.
    #[test]
    fn test_jaas_config_bare_value_with_a_backslash_is_rejected() {
        assert_eq!(
            rejection("PlainLoginModule required password=a\\b;"),
            "Value not specified for key 'null' in JAAS config"
        );
    }

    #[test]
    fn test_config_key_constants() {
        assert_eq!(SaslConfigs::SASL_MECHANISM, "sasl.mechanism");
        assert_eq!(SaslConfigs::SASL_JAAS_CONFIG, "sasl.jaas.config");
        assert_eq!(SaslConfigs::DEFAULT_SASL_MECHANISM, "GSSAPI");
        assert_eq!(SaslConfigs::GSSAPI_MECHANISM, "GSSAPI");
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
        let context = JaasContext::load_client_context(&config).unwrap();
        assert_eq!(config.resolve_username(context.as_ref()), Some("direct-user"));
        assert_eq!(config.resolve_password(context.as_ref()), Some("direct-S3cr3t-Pa55"));

        // Credentials carried only by the JAAS string are hidden as well, and
        // still resolve to plaintext.
        let jaas_only = SaslConfigs { username: None, password: None, ..config };
        let rendered = format!("{jaas_only:?}");
        assert!(!rendered.contains("jaas-S3cr3t-Pa55"), "{rendered}");
        assert!(!rendered.contains("jaas-user"), "{rendered}");
        assert!(rendered.contains("jaas_config: Some([hidden])"), "{rendered}");
        let context = JaasContext::load_client_context(&jaas_only).unwrap();
        assert_eq!(jaas_only.resolve_username(context.as_ref()), Some("jaas-user"));
        assert_eq!(jaas_only.resolve_password(context.as_ref()), Some("jaas-S3cr3t-Pa55"));
    }
}
