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

//! PLAIN-only translation of Java's
//! `org.apache.kafka.common.security.JaasConfig` /
//! `JaasContext` parsing for the `sasl.jaas.config` producer config.
//!
//! ## Why PLAIN-only?
//!
//! Java's full JAAS grammar supports any `LoginModule`, control flags
//! (`required` / `requisite` / `sufficient` / `optional`), arbitrary
//! `key=value` option pairs, multiple modules per context, and quoted
//! string values with escaped quotes. Milestone 1 only supports the
//! `PLAIN` SASL mechanism (PLAN.md:365), and the only `LoginModule`
//! that the PLAIN mechanism reads is
//! `org.apache.kafka.common.security.plain.PlainLoginModule`. The
//! canonical PLAIN configuration is:
//!
//! ```text
//! org.apache.kafka.common.security.plain.PlainLoginModule required \
//!     username="alice" \
//!     password="supersecret";
//! ```
//!
//! This module recognises that shape (with optional newlines /
//! whitespace) and rejects anything else with a clear
//! `KafkaError::Config`. The Rust translation deliberately omits the
//! tar-pit of the full JAAS grammar — non-PLAIN configs are out of
//! Milestone 1 scope and would only invite subtle parser bugs.
//!
//! ## Alternative: `sasl.username` / `sasl.password`
//!
//! As a fresh-impl convenience, the producer also accepts
//! `sasl.username` / `sasl.password` config keys directly (no JAAS
//! wrapper). These keys are not present in Java's `ProducerConfig`
//! schema; they exist to spare Rust users the JAAS-string ceremony
//! when only PLAIN is in scope. See
//! [`crate::common::config::sasl_configs::SASL_USERNAME`] /
//! [`crate::common::config::sasl_configs::SASL_PASSWORD`].

use crate::common::errors::KafkaError;
use crate::common::security::authenticator::PlainCredentials;

/// Fully-qualified Java class name of the PLAIN `LoginModule`. The
/// parser requires `sasl.jaas.config` to begin with this exact
/// identifier — anything else is rejected as an unsupported
/// configuration (Milestone 1 supports PLAIN only).
pub const PLAIN_LOGIN_MODULE: &str = "org.apache.kafka.common.security.plain.PlainLoginModule";

/// Parse a `sasl.jaas.config` value into PLAIN credentials.
///
/// Accepted format (whitespace / newlines allowed anywhere between
/// tokens):
///
/// ```text
/// org.apache.kafka.common.security.plain.PlainLoginModule <control-flag>
///     username="..." password="...";
/// ```
///
/// Where `<control-flag>` is one of (`required` / `requisite` /
/// `sufficient` / `optional`) — case insensitive — and `username` /
/// `password` are quoted strings.
///
/// Reject reasons:
/// - Module name is not `PlainLoginModule` →
///   `KafkaError::Config("...this client supports PLAIN mechanism only — see sasl.username / sasl.password as an alternative")`.
/// - Missing control flag, missing semicolon, malformed quoted values
///   → `KafkaError::Config(...)`.
/// - Missing `username` or `password` → `KafkaError::Config(...)`.
pub fn parse_plain_jaas_config(jaas_config: &str) -> Result<PlainCredentials, KafkaError> {
    let mut tokens = Tokenizer::new(jaas_config);

    // First token must be the LoginModule fully-qualified class name.
    let module = tokens
        .next_word()?
        .ok_or_else(|| KafkaError::Config("sasl.jaas.config is empty — no LoginModule specified".to_owned()))?;
    if module != PLAIN_LOGIN_MODULE {
        return Err(KafkaError::Config(format!(
            "sasl.jaas.config: unsupported LoginModule {module:?} — this client supports PLAIN mechanism only \
             (org.apache.kafka.common.security.plain.PlainLoginModule). See sasl.username / sasl.password \
             as an alternative."
        )));
    }

    // Second token: control flag — required/requisite/sufficient/optional.
    let flag = tokens.next_word()?.ok_or_else(|| {
        KafkaError::Config("sasl.jaas.config: missing control flag after PlainLoginModule".to_owned())
    })?;
    match flag.to_ascii_uppercase().as_str() {
        "REQUIRED" | "REQUISITE" | "SUFFICIENT" | "OPTIONAL" => {},
        _ => {
            return Err(KafkaError::Config(format!(
                "sasl.jaas.config: invalid control flag {flag:?} \
                 (expected one of: required, requisite, sufficient, optional)"
            )));
        },
    }

    // Subsequent tokens: key=value pairs until `;` terminator.
    let mut username: Option<String> = None;
    let mut password: Option<String> = None;
    loop {
        match tokens.next_kv_or_semicolon()? {
            KvOrSemi::Semi => break,
            KvOrSemi::Kv { key, value } => match key.as_str() {
                "username" => username = Some(value),
                "password" => password = Some(value),
                other => {
                    // Java's parser silently accepts arbitrary keys (they
                    // end up in the AppConfigurationEntry options map).
                    // The Rust translation rejects keys other than the
                    // ones PLAIN consumes, because Milestone 1 cannot
                    // route unknown options anywhere.
                    return Err(KafkaError::Config(format!(
                        "sasl.jaas.config: unsupported PLAIN option {other:?} \
                         (only username / password are accepted in Milestone 1)"
                    )));
                },
            },
        }
    }

    let username =
        username.ok_or_else(|| KafkaError::Config("sasl.jaas.config: missing username option".to_owned()))?;
    let password =
        password.ok_or_else(|| KafkaError::Config("sasl.jaas.config: missing password option".to_owned()))?;
    Ok(PlainCredentials::new(username, password))
}

/// Lightweight tokenizer for the PLAIN JAAS subset. Mirrors enough of
/// `java.io.StreamTokenizer`'s default behaviour to handle the
/// canonical PLAIN config: words (FQCN-style identifiers), quoted
/// values, `=` between key/value, `;` terminator.
struct Tokenizer<'a> {
    src: &'a str,
    pos: usize,
}

#[derive(Debug)]
enum KvOrSemi {
    Kv { key: String, value: String },
    Semi,
}

impl<'a> Tokenizer<'a> {
    fn new(src: &'a str) -> Self {
        Tokenizer { src, pos: 0 }
    }

    fn skip_whitespace(&mut self) {
        while let Some(c) = self.peek_char() {
            if c.is_whitespace() {
                self.pos += c.len_utf8();
            } else {
                break;
            }
        }
    }

    fn peek_char(&self) -> Option<char> {
        self.src[self.pos..].chars().next()
    }

    fn next_char(&mut self) -> Option<char> {
        let c = self.peek_char()?;
        self.pos += c.len_utf8();
        Some(c)
    }

    /// Read a "word" — sequence of identifier-class characters
    /// (letters, digits, `.`, `_`, `-`, `$`). Mirrors Java's
    /// `StreamTokenizer.wordChars` configuration in `JaasConfig`.
    fn next_word(&mut self) -> Result<Option<String>, KafkaError> {
        self.skip_whitespace();
        let start = self.pos;
        while let Some(c) = self.peek_char() {
            if c.is_alphanumeric() || c == '.' || c == '_' || c == '-' || c == '$' {
                self.pos += c.len_utf8();
            } else {
                break;
            }
        }
        if start == self.pos {
            return Ok(None);
        }
        Ok(Some(self.src[start..self.pos].to_owned()))
    }

    /// Read a `key=value` pair OR a `;` terminator.
    fn next_kv_or_semicolon(&mut self) -> Result<KvOrSemi, KafkaError> {
        self.skip_whitespace();
        if let Some(c) = self.peek_char()
            && c == ';'
        {
            self.next_char();
            return Ok(KvOrSemi::Semi);
        }
        let key = self
            .next_word()?
            .ok_or_else(|| KafkaError::Config("sasl.jaas.config: expected key=value or `;` terminator".to_owned()))?;
        self.skip_whitespace();
        let eq = self
            .next_char()
            .ok_or_else(|| KafkaError::Config(format!("sasl.jaas.config: missing `=` after key {key:?}")))?;
        if eq != '=' {
            return Err(KafkaError::Config(format!(
                "sasl.jaas.config: expected `=` after key {key:?}, got {eq:?}"
            )));
        }
        self.skip_whitespace();
        let value = self
            .next_quoted_or_word()?
            .ok_or_else(|| KafkaError::Config(format!("sasl.jaas.config: missing value for key {key:?}")))?;
        Ok(KvOrSemi::Kv { key, value })
    }

    /// Read either a `"..."`-quoted string (with `\"` and `\\` escapes)
    /// or an unquoted word.
    fn next_quoted_or_word(&mut self) -> Result<Option<String>, KafkaError> {
        self.skip_whitespace();
        let Some(c) = self.peek_char() else {
            return Ok(None);
        };
        if c == '"' {
            self.next_char(); // consume opening quote
            let mut value = String::new();
            loop {
                let c = self
                    .next_char()
                    .ok_or_else(|| KafkaError::Config("sasl.jaas.config: unterminated quoted string".to_owned()))?;
                if c == '"' {
                    return Ok(Some(value));
                }
                if c == '\\' {
                    let escaped = self
                        .next_char()
                        .ok_or_else(|| KafkaError::Config("sasl.jaas.config: stray `\\` at end of input".to_owned()))?;
                    value.push(escaped);
                } else {
                    value.push(c);
                }
            }
        } else {
            self.next_word()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_canonical_plain_config() {
        let jaas = r#"org.apache.kafka.common.security.plain.PlainLoginModule required username="alice" password="supersecret";"#;
        let creds = parse_plain_jaas_config(jaas).expect("canonical PLAIN config must parse");
        assert_eq!(creds.username(), "alice");
        assert_eq!(creds.password(), "supersecret");
    }

    #[test]
    fn parses_multi_line_plain_config() {
        // Real-world configs are often multi-line.
        let jaas = "org.apache.kafka.common.security.plain.PlainLoginModule required\n  \
                    username=\"alice\"\n  \
                    password=\"supersecret\";";
        let creds = parse_plain_jaas_config(jaas).expect("multi-line PLAIN config must parse");
        assert_eq!(creds.username(), "alice");
        assert_eq!(creds.password(), "supersecret");
    }

    #[test]
    fn parses_case_insensitive_control_flag() {
        for flag in ["required", "REQUIRED", "Sufficient", "OPTIONAL", "requisite"] {
            let jaas =
                format!(r#"org.apache.kafka.common.security.plain.PlainLoginModule {flag} username="u" password="p";"#);
            let creds = parse_plain_jaas_config(&jaas).unwrap_or_else(|e| panic!("flag {flag} must parse: {e:?}"));
            assert_eq!(creds.username(), "u");
        }
    }

    #[test]
    fn handles_password_with_special_chars() {
        // PLAIN passwords often contain `=`, `/`, `+` etc. (base64-style).
        let jaas = r#"org.apache.kafka.common.security.plain.PlainLoginModule required username="alice" password="abc=def/g+h";"#;
        let creds = parse_plain_jaas_config(jaas).expect("special-char password must parse");
        assert_eq!(creds.password(), "abc=def/g+h");
    }

    #[test]
    fn handles_escaped_quote_in_password() {
        let jaas =
            r#"org.apache.kafka.common.security.plain.PlainLoginModule required username="alice" password="he\"llo";"#;
        let creds = parse_plain_jaas_config(jaas).expect("escaped quote must parse");
        assert_eq!(creds.password(), "he\"llo");
    }

    #[test]
    fn rejects_non_plain_login_module() {
        let jaas = r#"org.apache.kafka.common.security.scram.ScramLoginModule required username="alice" password="supersecret";"#;
        let err = parse_plain_jaas_config(jaas).expect_err("SCRAM must be rejected");
        assert!(matches!(err, KafkaError::Config(_)));
        assert!(err.message().contains("PLAIN mechanism only"), "got: {}", err.message());
        assert!(err.message().contains("ScramLoginModule"));
    }

    #[test]
    fn rejects_missing_control_flag() {
        let jaas = "org.apache.kafka.common.security.plain.PlainLoginModule";
        let err = parse_plain_jaas_config(jaas).expect_err("missing flag must reject");
        assert!(err.message().contains("missing control flag"));
    }

    #[test]
    fn rejects_invalid_control_flag() {
        let jaas = r#"org.apache.kafka.common.security.plain.PlainLoginModule maybe username="u" password="p";"#;
        let err = parse_plain_jaas_config(jaas).expect_err("bogus flag must reject");
        assert!(err.message().contains("invalid control flag"));
        assert!(err.message().contains("maybe"));
    }

    #[test]
    fn rejects_missing_username() {
        let jaas = r#"org.apache.kafka.common.security.plain.PlainLoginModule required password="p";"#;
        let err = parse_plain_jaas_config(jaas).expect_err("missing username must reject");
        assert!(err.message().contains("missing username"));
    }

    #[test]
    fn rejects_missing_password() {
        let jaas = r#"org.apache.kafka.common.security.plain.PlainLoginModule required username="u";"#;
        let err = parse_plain_jaas_config(jaas).expect_err("missing password must reject");
        assert!(err.message().contains("missing password"));
    }

    #[test]
    fn rejects_unknown_option() {
        let jaas = r#"org.apache.kafka.common.security.plain.PlainLoginModule required username="u" password="p" tokenAuthBroker="false";"#;
        let err = parse_plain_jaas_config(jaas).expect_err("unknown opt must reject");
        assert!(err.message().contains("unsupported PLAIN option"));
        assert!(err.message().contains("tokenAuthBroker"));
    }

    #[test]
    fn rejects_unterminated_quoted_string() {
        let jaas = r#"org.apache.kafka.common.security.plain.PlainLoginModule required username="alice password="p";"#;
        // The parser interprets this as: username="alice password=" then expects more tokens
        // — eventually missing-password or terminator. Confirm it errors.
        let err = parse_plain_jaas_config(jaas).expect_err("malformed must reject");
        // Either "missing" or "unterminated" message is acceptable.
        assert!(matches!(err, KafkaError::Config(_)));
    }

    #[test]
    fn rejects_missing_semicolon() {
        let jaas = r#"org.apache.kafka.common.security.plain.PlainLoginModule required username="u" password="p""#;
        let err = parse_plain_jaas_config(jaas).expect_err("missing semicolon must reject");
        // Hit either "expected key=value or `;` terminator" or an EOF-related error.
        assert!(matches!(err, KafkaError::Config(_)));
    }

    #[test]
    fn rejects_empty_config() {
        let err = parse_plain_jaas_config("").expect_err("empty must reject");
        assert!(err.message().contains("empty"));
    }

    #[test]
    fn rejects_whitespace_only_config() {
        let err = parse_plain_jaas_config("   \n  \t  ").expect_err("whitespace-only must reject");
        assert!(err.message().contains("empty"));
    }
}
