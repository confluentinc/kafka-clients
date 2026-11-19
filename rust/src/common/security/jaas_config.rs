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

//! The parser of the `sasl.jaas.config` property.
//!
//! Translated from `org.apache.kafka.common.security.JaasConfig`. Java's class
//! extends the JDK's `javax.security.auth.login.Configuration` and builds the
//! JDK's `javax.security.auth.login.AppConfigurationEntry`, and it reads the
//! property with a `java.io.StreamTokenizer`. This module holds the part of
//! each JDK class `JaasConfig` uses: [`AppConfigurationEntry`] with its
//! [`LoginModuleControlFlag`], and a tokenizer with the syntax `JaasConfig`
//! gives its `StreamTokenizer`.
//!
//! The tokenizer implements the behaviour `java.io.StreamTokenizer` documents
//! for that syntax; it is not a translation of the JDK's source. The tests pin
//! it, character class by character class, to what Kafka 4.4.0's `JaasConfig`
//! makes of each value on JDK 25. The owner chose this over a translation of
//! OpenJDK under its GPL with the Classpath Exception (`CLAUDE.md` §1, rule
//! 1.3), so the module keeps the crate's Apache licence.

use std::collections::HashMap;
use std::fmt;
use std::iter::Peekable;
use std::str::Chars;

use crate::common::Error;

/// JAAS configuration parser that constructs a JAAS configuration object with a single
/// login context from the Kafka configuration option `sasl.jaas.config`.
///
/// The JAAS configuration file format is described in the JDK's guide to
/// `LoginConfigFile`. The format of the property value is:
///
/// ```text
/// <loginModuleClass> <controlFlag> (<optionName>=<optionValue>)*;
/// ```
#[doc(alias = "org.apache.kafka.common.security.JaasConfig")]
pub(crate) struct JaasConfig {
    login_context_name: String,
    config_entries: Vec<AppConfigurationEntry>,
}

impl fmt::Debug for JaasConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("JaasConfig")
            .field("login_context_name", &self.login_context_name)
            .field("config_entries", &self.config_entries)
            .finish()
    }
}

impl JaasConfig {
    /// Parses `jaas_config_params`, the value of `sasl.jaas.config`, into the
    /// entries of the login context `login_context_name`: one per login module,
    /// in order.
    ///
    /// The value is a sequence of `<loginModuleClass> <controlFlag>
    /// (<optionName>=<optionValue>)*;` entries. Each name and value is a word
    /// (letters, digits, `.`, `-`, `_` and `$`, not starting with a digit, `.`
    /// or `-`) or a string in double or single quotes, in which the escape
    /// sequences of a Java string literal are decoded. `//` and `/* */`
    /// comments are skipped.
    ///
    /// # Errors
    ///
    /// [`Error::is_local_illegal_argument_error`], Java's
    /// `IllegalArgumentException`, when the value names no login module, an
    /// entry has no control flag, the control flag is not `required`,
    /// `requisite`, `sufficient` or `optional` (in any case), an option has no
    /// value (an unquoted number is none), an entry is not terminated by `;`,
    /// or a login module name is empty.
    ///
    /// Java catches the `IOException` of the `StringReader` it tokenizes and
    /// throws `KafkaException("Unexpected exception while parsing JAAS
    /// config")`; reading a `&str` cannot fail, so that error has no
    /// counterpart.
    #[doc(alias = "org.apache.kafka.common.security.JaasConfig#JaasConfig")]
    pub(crate) fn new(login_context_name: &str, jaas_config_params: &str) -> Result<JaasConfig, Error> {
        let mut tokenizer = StreamTokenizer::new(jaas_config_params);
        let mut config_entries = Vec::new();
        loop {
            match tokenizer.next_token() {
                Token::Eof => break,
                token => config_entries.push(Self::parse_app_configuration_entry(&mut tokenizer, token.into_sval())?),
            }
        }
        if config_entries.is_empty() {
            return Err(Error::local_illegal_argument("Login module not specified in JAAS config"));
        }
        Ok(JaasConfig { login_context_name: login_context_name.to_owned(), config_entries })
    }

    /// The entries of the login context `name`, or `None` if `name` is not the
    /// login context this configuration was parsed for.
    #[doc(alias = "org.apache.kafka.common.security.JaasConfig#getAppConfigurationEntry")]
    pub(crate) fn get_app_configuration_entry(&self, name: &str) -> Option<&[AppConfigurationEntry]> {
        (self.login_context_name == name).then_some(self.config_entries.as_slice())
    }

    /// The control flag `flag` names, compared without case, as Java's
    /// `flag.toUpperCase(Locale.ROOT)` does: Unicode's case mapping without a
    /// locale's rules, which `str::to_uppercase` applies too.
    #[doc(alias = "org.apache.kafka.common.security.JaasConfig#loginModuleControlFlag")]
    fn login_module_control_flag(flag: Option<&str>) -> Result<LoginModuleControlFlag, Error> {
        let Some(flag) = flag else {
            return Err(Error::local_illegal_argument(
                "Login module control flag is not available in the JAAS config",
            ));
        };
        match flag.to_uppercase().as_str() {
            "REQUIRED" => Ok(LoginModuleControlFlag::Required),
            "REQUISITE" => Ok(LoginModuleControlFlag::Requisite),
            "SUFFICIENT" => Ok(LoginModuleControlFlag::Sufficient),
            "OPTIONAL" => Ok(LoginModuleControlFlag::Optional),
            _ => Err(Error::local_illegal_argument(format!(
                "Invalid login module control flag '{flag}' in JAAS config"
            ))),
        }
    }

    /// Parses the rest of the entry whose first token, the login module name,
    /// the caller has read: `login_module` is that token's `sval`, `None` for a
    /// token that is neither a word nor a quoted string.
    ///
    /// An option whose key is not a word or a quoted string has the key
    /// `None`, Java's `null` `HashMap` key, so `3="x"` is accepted.
    #[doc(alias = "org.apache.kafka.common.security.JaasConfig#parseAppConfigurationEntry")]
    fn parse_app_configuration_entry(
        tokenizer: &mut StreamTokenizer<'_>,
        login_module: Option<String>,
    ) -> Result<AppConfigurationEntry, Error> {
        let control_flag = match tokenizer.next_token() {
            Token::Eof => {
                return Err(Error::local_illegal_argument(
                    "Login module control flag not specified in JAAS config",
                ));
            },
            token => Self::login_module_control_flag(token.sval())?,
        };
        let mut options = HashMap::new();
        let last = loop {
            let token = tokenizer.next_token();
            if matches!(token, Token::Eof | Token::Ordinary(';')) {
                break token;
            }
            let key = token.into_sval();
            // Java: `nextToken() != '=' || nextToken() == TT_EOF || sval == null`.
            // The end of the value leaves `sval` null, so the last two are one test.
            let value = match tokenizer.next_token() {
                Token::Ordinary('=') => tokenizer.next_token().into_sval(),
                _ => None,
            };
            let Some(value) = value else {
                return Err(Error::local_illegal_argument(format!(
                    "Value not specified for key '{}' in JAAS config",
                    key.as_deref().unwrap_or("null")
                )));
            };
            options.insert(key, value);
        };
        if !matches!(last, Token::Ordinary(';')) {
            return Err(Error::local_illegal_argument("JAAS config entry not terminated by semi-colon"));
        }
        AppConfigurationEntry::new(login_module, control_flag, options)
    }
}

/// How a login module's success or failure affects the login: the JDK's
/// `javax.security.auth.login.AppConfigurationEntry.LoginModuleControlFlag`.
#[doc(alias = "javax.security.auth.login.AppConfigurationEntry$LoginModuleControlFlag")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LoginModuleControlFlag {
    /// The login module is required to succeed; the login continues either way.
    Required,
    /// The login module is required to succeed; a failure ends the login.
    Requisite,
    /// The login module is not required to succeed; a success ends the login.
    Sufficient,
    /// The login module is not required to succeed.
    Optional,
}

impl fmt::Display for LoginModuleControlFlag {
    /// The JDK's `toString()`: `LoginModuleControlFlag: ` and the flag's name
    /// in lower case.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let flag = match self {
            LoginModuleControlFlag::Required => "required",
            LoginModuleControlFlag::Requisite => "requisite",
            LoginModuleControlFlag::Sufficient => "sufficient",
            LoginModuleControlFlag::Optional => "optional",
        };
        write!(f, "LoginModuleControlFlag: {flag}")
    }
}

/// One login module of a JAAS login context, with its control flag and
/// options: the JDK's `javax.security.auth.login.AppConfigurationEntry`.
#[doc(alias = "javax.security.auth.login.AppConfigurationEntry")]
#[derive(Clone)]
pub(crate) struct AppConfigurationEntry {
    login_module_name: String,
    control_flag: LoginModuleControlFlag,
    /// Keyed by `Option`, because Java's `HashMap` takes a `null` key.
    options: HashMap<Option<String>, String>,
}

impl fmt::Debug for AppConfigurationEntry {
    /// Renders the option keys only: the values hold the credentials
    /// (`password`).
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut option_keys: Vec<Option<&str>> = self.options.keys().map(Option::as_deref).collect();
        option_keys.sort_unstable();
        f.debug_struct("AppConfigurationEntry")
            .field("login_module_name", &self.login_module_name)
            .field("control_flag", &self.control_flag)
            .field("option_keys", &option_keys)
            .finish()
    }
}

impl AppConfigurationEntry {
    /// The entry of the login module `login_module_name`.
    ///
    /// # Errors
    ///
    /// [`Error::is_local_illegal_argument_error`] with an empty message, the
    /// JDK's bare `IllegalArgumentException()`, if `login_module_name` is `None`
    /// or empty. The JDK also rejects a `null` control flag or options, which
    /// Rust's types rule out.
    pub(crate) fn new(
        login_module_name: Option<String>,
        control_flag: LoginModuleControlFlag,
        options: HashMap<Option<String>, String>,
    ) -> Result<AppConfigurationEntry, Error> {
        match login_module_name {
            Some(login_module_name) if !login_module_name.is_empty() => {
                Ok(AppConfigurationEntry { login_module_name, control_flag, options })
            },
            _ => Err(Error::local_illegal_argument("")),
        }
    }

    /// The name of the login module, the JDK's `getLoginModuleName()`.
    pub(crate) fn login_module_name(&self) -> &str {
        &self.login_module_name
    }

    /// The control flag of the login module, the JDK's `getControlFlag()`.
    #[cfg_attr(not(test), expect(dead_code))]
    pub(crate) fn control_flag(&self) -> LoginModuleControlFlag {
        self.control_flag
    }

    /// The options of the login module, the JDK's `getOptions()`.
    pub(crate) fn options(&self) -> &HashMap<Option<String>, String> {
        &self.options
    }
}

/// A token of the [`StreamTokenizer`]: Java's `ttype`, carrying the `sval`
/// the tokenizer sets with it.
#[derive(Debug)]
enum Token {
    /// `TT_EOF`, the end of the value.
    Eof,
    /// `TT_NUMBER`, a number, for which `sval` is null.
    Number,
    /// `TT_WORD`, a word: `sval` is the word.
    Word(String),
    /// A quoted string, whose `ttype` is its quote character: `sval` is its
    /// body, with its escape sequences decoded.
    Quoted(String),
    /// Any other character, which is `ttype`; `sval` is null.
    Ordinary(char),
}

impl Token {
    /// Java's `sval` after this token.
    fn sval(&self) -> Option<&str> {
        match self {
            Token::Word(sval) | Token::Quoted(sval) => Some(sval),
            Token::Eof | Token::Number | Token::Ordinary(_) => None,
        }
    }

    /// Java's `sval` after this token, owned.
    fn into_sval(self) -> Option<String> {
        match self {
            Token::Word(sval) | Token::Quoted(sval) => Some(sval),
            Token::Eof | Token::Number | Token::Ordinary(_) => None,
        }
    }
}

/// A `java.io.StreamTokenizer` reading a string with the syntax `JaasConfig`
/// sets: the default syntax, `slashSlashComments(true)`,
/// `slashStarComments(true)`, and `wordChars` for `-`, `_` and `$`.
///
/// Each character has the class the syntax gives it:
///
/// - whitespace: `U+0000` to `U+0020`, which separates tokens;
/// - numeric: `0` to `9`, `.` and `-`;
/// - alphabetic: `A` to `Z`, `a` to `z`, `U+00A0` to `U+00FF`, every
///   character above `U+00FF`, and `-`, `_` and `$`;
/// - quote: `"` and `'`;
/// - comment: `/`;
/// - ordinary: every other character, a token of its own.
///
/// A numeric character starts a number: an optional `-`, then digits with at
/// most one `.`. A `-` that no digit or `.` follows is an ordinary character,
/// although it is also alphabetic. An alphabetic character starts a word,
/// which runs over the alphabetic and numeric characters after it. A quote
/// starts a string, which ends at the same quote, at a raw line terminator
/// (left to the next token) or at the end of the value; in it a `\` escapes
/// the character after it: `\a`, `\b`, `\f`, `\n`, `\r`, `\t` and `\v` are the
/// control characters `U+0007`, `U+0008`, `U+000C`, `U+000A`, `U+000D`,
/// `U+0009` and `U+000B`, one to three octal digits are the character of that
/// code (three only when the first is `0` to `3`), and any other character is
/// itself; a `\` that ends the value is `U+FFFF`, the `(char) -1` of the end
/// of Java's stream. `/*` starts a comment that ends after the next `*/`, or
/// with the value. `//` comments out the rest of its line, and so does a `/`
/// followed by any other character, `/` being the comment character, that
/// next character included, even when it is the line terminator.
struct StreamTokenizer<'a> {
    chars: Peekable<Chars<'a>>,
}

impl<'a> StreamTokenizer<'a> {
    fn new(input: &'a str) -> StreamTokenizer<'a> {
        StreamTokenizer { chars: input.chars().peekable() }
    }

    fn is_whitespace(c: char) -> bool {
        c <= ' '
    }

    fn is_numeric(c: char) -> bool {
        c.is_ascii_digit() || c == '.' || c == '-'
    }

    fn is_alphabetic(c: char) -> bool {
        c.is_ascii_alphabetic() || matches!(c, '-' | '_' | '$') || c >= '\u{a0}'
    }

    /// The next token: Java's `nextToken()`.
    fn next_token(&mut self) -> Token {
        loop {
            let Some(c) = self.chars.find(|&c| !Self::is_whitespace(c)) else {
                return Token::Eof;
            };
            if Self::is_numeric(c) {
                return self.number(c);
            }
            if Self::is_alphabetic(c) {
                let mut word = String::from(c);
                while let Some(next) = self.chars.next_if(|&n| Self::is_alphabetic(n) || Self::is_numeric(n)) {
                    word.push(next);
                }
                return Token::Word(word);
            }
            if c == '"' || c == '\'' {
                return Token::Quoted(self.quoted(c));
            }
            if c != '/' {
                return Token::Ordinary(c);
            }
            match self.chars.next() {
                Some('*') => {
                    let mut previous = None;
                    loop {
                        match self.chars.next() {
                            None => return Token::Eof,
                            Some('/') if previous == Some('*') => break,
                            next => previous = next,
                        }
                    }
                },
                // `//`, or the comment character followed by any other
                // character, which the comment takes.
                _ => while self.chars.next_if(|&n| n != '\n' && n != '\r').is_some() {},
            }
        }
    }

    /// The rest of the number `first` starts, or the ordinary character `-`.
    fn number(&mut self, first: char) -> Token {
        if first == '-' && !self.chars.peek().is_some_and(|&n| n == '.' || n.is_ascii_digit()) {
            return Token::Ordinary('-');
        }
        let mut seen_dot = first == '.';
        while let Some(&next) = self.chars.peek() {
            if next == '.' && !seen_dot {
                seen_dot = true;
            } else if !next.is_ascii_digit() {
                break;
            }
            self.chars.next();
        }
        Token::Number
    }

    /// The body of the string that `quote` opened, escapes decoded.
    fn quoted(&mut self, quote: char) -> String {
        let mut body = String::new();
        while let Some(c) = self.chars.next_if(|&n| n != '\n' && n != '\r') {
            if c == quote {
                break;
            }
            if c != '\\' {
                body.push(c);
                continue;
            }
            let Some(escaped) = self.chars.next() else {
                body.push('\u{ffff}');
                break;
            };
            body.push(match escaped {
                'a' => '\u{7}',
                'b' => '\u{8}',
                'f' => '\u{c}',
                'n' => '\n',
                'r' => '\r',
                't' => '\t',
                'v' => '\u{b}',
                '0'..='7' => self.octal_escape(escaped),
                other => other,
            });
        }
        body
    }

    /// The character of the octal escape whose first digit is `first`.
    fn octal_escape(&mut self, first: char) -> char {
        let digit = |c: char| c as u8 - b'0';
        let mut code = digit(first);
        if let Some(second) = self.chars.next_if(|n| ('0'..='7').contains(n)) {
            code = code * 8 + digit(second);
            if first <= '3'
                && let Some(third) = self.chars.next_if(|n| ('0'..='7').contains(n))
            {
                code = code * 8 + digit(third);
            }
        }
        // At most `\377`, a Latin-1 code point.
        char::from(code)
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    use LoginModuleControlFlag::{Optional, Required, Requisite, Sufficient};

    /// A login module entry as the vectors write it: its name, control flag
    /// and options, `None` being Java's `null` key.
    pub(crate) type ExpectedEntry = (
        &'static str,
        LoginModuleControlFlag,
        &'static [(Option<&'static str>, &'static str)],
    );

    /// What Kafka 4.4.0's `JaasConfig` makes of a value: its entries, or the
    /// message of the `IllegalArgumentException` it throws (empty for the
    /// JDK's bare one).
    pub(crate) type Outcome = Result<&'static [ExpectedEntry], &'static str>;

    /// The outcome of `new JaasConfig("KafkaClient", value)` on Kafka 4.4.0
    /// (JDK 25) for each value, the options sorted by key. Every exception was
    /// an `IllegalArgumentException`.
    pub(crate) const JAVA_OUTCOMES: &[(&str, Outcome)] = &[
        (
            "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"a\" password=\"b\"",
            Err("JAAS config entry not terminated by semi-colon"),
        ),
        (
            "org.apache.kafka.common.security.plain.PlainLoginModule requiredX username=\"a\" password=\"b\";",
            Err("Invalid login module control flag 'requiredX' in JAAS config"),
        ),
        (
            "org.apache.kafka.common.security.plain.PlainLoginModule username=\"a\" password=\"b\";",
            Err("Invalid login module control flag 'username' in JAAS config"),
        ),
        (
            "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"a\" password=\"b\";",
            Ok(&[(
                "org.apache.kafka.common.security.plain.PlainLoginModule",
                Required,
                &[(Some("password"), "b"), (Some("username"), "a")],
            )]),
        ),
        (
            "test.testConfigNoOptions required;",
            Ok(&[("test.testConfigNoOptions", Required, &[])]),
        ),
        (
            "  required option1=value1;",
            Err("Invalid login module control flag 'option1' in JAAS config"),
        ),
        (
            "test.loginModule option1=value1;",
            Err("Invalid login module control flag 'option1' in JAAS config"),
        ),
        (
            "loginModule required option1;",
            Err("Value not specified for key 'option1' in JAAS config"),
        ),
        (
            "test.testMissingSemicolon required option1=value1",
            Err("JAAS config entry not terminated by semi-colon"),
        ),
        (
            "test.testNumericOptionWithoutQuotes required option1=3;",
            Err("Value not specified for key 'option1' in JAAS config"),
        ),
        (
            "test.testInvalidControlFlag { option1=3;",
            Err("Login module control flag is not available in the JAAS config"),
        ),
        (
            "test.testNumericOptionWithQuotes required option1=\"3\";",
            Ok(&[("test.testNumericOptionWithQuotes", Required, &[(Some("option1"), "3")])]),
        ),
        (
            "test.testQuotedOptionValue required propName=\"prop value\" propName2=\"value1 = 1, value2 = 2\";",
            Ok(&[(
                "test.testQuotedOptionValue",
                Required,
                &[
                    (Some("propName"), "prop value"),
                    (Some("propName2"), "value1 = 1, value2 = 2"),
                ],
            )]),
        ),
        (
            "test.testQuotedOptionName required \"prop name\"=propValue;",
            Ok(&[("test.testQuotedOptionName", Required, &[(Some("prop name"), "propValue")])]),
        ),
        (
            " test.Module0 required index=Index0 module=Module0; test.Module1 required index=Index1 module=Module1; test.Module2 required index=Index2 module=Module2;",
            Ok(&[
                (
                    "test.Module0",
                    Required,
                    &[(Some("index"), "Index0"), (Some("module"), "Module0")],
                ),
                (
                    "test.Module1",
                    Required,
                    &[(Some("index"), "Index1"), (Some("module"), "Module1")],
                ),
                (
                    "test.Module2",
                    Required,
                    &[(Some("index"), "Index2"), (Some("module"), "Module2")],
                ),
            ]),
        ),
        (
            "com.sun.security.auth.module.JndiLoginModule required;",
            Ok(&[("com.sun.security.auth.module.JndiLoginModule", Required, &[])]),
        ),
        (
            "com.sun.security.auth.module.LdapLoginModule required;",
            Ok(&[("com.sun.security.auth.module.LdapLoginModule", Required, &[])]),
        ),
        (
            "\" com.sun.security.auth.module.JndiLoginModule \" required;",
            Ok(&[(" com.sun.security.auth.module.JndiLoginModule ", Required, &[])]),
        ),
        (
            "com.ibm.security.auth.module.LdapLoginModule required;",
            Ok(&[("com.ibm.security.auth.module.LdapLoginModule", Required, &[])]),
        ),
        ("", Err("Login module not specified in JAAS config")),
        ("   ", Err("Login module not specified in JAAS config")),
        ("\u{9}\u{a}\u{d} ", Err("Login module not specified in JAAS config")),
        ("// only a comment", Err("Login module not specified in JAAS config")),
        ("/* only a comment */", Err("Login module not specified in JAAS config")),
        ("/* unterminated", Err("Login module not specified in JAAS config")),
        ("m", Err("Login module control flag not specified in JAAS config")),
        ("m required", Err("JAAS config entry not terminated by semi-colon")),
        ("m required;", Ok(&[("m", Required, &[])])),
        ("m required k", Err("Value not specified for key 'k' in JAAS config")),
        ("m required k=", Err("Value not specified for key 'k' in JAAS config")),
        ("m required k=v", Err("JAAS config entry not terminated by semi-colon")),
        ("m REQUIRED;", Ok(&[("m", Required, &[])])),
        ("m Required;", Ok(&[("m", Required, &[])])),
        ("m reqUIRED;", Ok(&[("m", Required, &[])])),
        ("m requisite;", Ok(&[("m", Requisite, &[])])),
        ("m SUFFICIENT;", Ok(&[("m", Sufficient, &[])])),
        ("m optional;", Ok(&[("m", Optional, &[])])),
        ("m \"required\";", Ok(&[("m", Required, &[])])),
        ("m 'optional';", Ok(&[("m", Optional, &[])])),
        (
            "m required-x;",
            Err("Invalid login module control flag 'required-x' in JAAS config"),
        ),
        ("m 3;", Err("Login module control flag is not available in the JAAS config")),
        ("m ;", Err("Login module control flag is not available in the JAAS config")),
        ("m =;", Err("Login module control flag is not available in the JAAS config")),
        ("m \"\";", Err("Invalid login module control flag '' in JAAS config")),
        (
            "m \"re quired\";",
            Err("Invalid login module control flag 're quired' in JAAS config"),
        ),
        ("m required k=v;", Ok(&[("m", Required, &[(Some("k"), "v")])])),
        ("m required k = v ;", Ok(&[("m", Required, &[(Some("k"), "v")])])),
        ("m required k\u{a}=\u{a}v;", Ok(&[("m", Required, &[(Some("k"), "v")])])),
        ("m\u{9}required\u{9}k=v;", Ok(&[("m", Required, &[(Some("k"), "v")])])),
        ("m required k=\"v w\";", Ok(&[("m", Required, &[(Some("k"), "v w")])])),
        ("m required k='v w';", Ok(&[("m", Required, &[(Some("k"), "v w")])])),
        ("m required k='v\" w';", Ok(&[("m", Required, &[(Some("k"), "v\" w")])])),
        ("m required k=\"v' w\";", Ok(&[("m", Required, &[(Some("k"), "v' w")])])),
        ("m required k=v k=w;", Ok(&[("m", Required, &[(Some("k"), "w")])])),
        (
            "m required a=1x b=\"2\";",
            Err("Value not specified for key 'a' in JAAS config"),
        ),
        ("m required k=\"\";", Ok(&[("m", Required, &[(Some("k"), "")])])),
        ("m required k='';", Ok(&[("m", Required, &[(Some("k"), "")])])),
        ("m required \"k k\"=v;", Ok(&[("m", Required, &[(Some("k k"), "v")])])),
        ("m required 'k'='v';", Ok(&[("m", Required, &[(Some("k"), "v")])])),
        ("m required k==v;", Err("Value not specified for key 'k' in JAAS config")),
        ("m required =v;", Err("Value not specified for key 'null' in JAAS config")),
        ("m required = = \"x\";", Ok(&[("m", Required, &[(None, "x")])])),
        ("m required 3=\"x\";", Ok(&[("m", Required, &[(None, "x")])])),
        ("m required 3=\"x\" 4=\"y\";", Ok(&[("m", Required, &[(None, "y")])])),
        (
            "m required k=v n required;",
            Err("Value not specified for key 'n' in JAAS config"),
        ),
        (
            "m required k=\"x\"\"y\";",
            Err("Value not specified for key 'y' in JAAS config"),
        ),
        (
            "m required k=v1, k2=v2;",
            Err("Value not specified for key 'null' in JAAS config"),
        ),
        ("m required k=v ;", Ok(&[("m", Required, &[(Some("k"), "v")])])),
        ("m required k=v\u{a};", Ok(&[("m", Required, &[(Some("k"), "v")])])),
        ("m required k=v;\u{a}", Ok(&[("m", Required, &[(Some("k"), "v")])])),
        ("m required k=3;", Err("Value not specified for key 'k' in JAAS config")),
        ("m required k=-3;", Err("Value not specified for key 'k' in JAAS config")),
        ("m required k=-x;", Err("Value not specified for key 'k' in JAAS config")),
        ("m required k=-;", Err("Value not specified for key 'k' in JAAS config")),
        ("m required k=--x;", Err("Value not specified for key 'k' in JAAS config")),
        ("m required k=-.5;", Err("Value not specified for key 'k' in JAAS config")),
        ("m required k=.5;", Err("Value not specified for key 'k' in JAAS config")),
        ("m required k=.x;", Err("Value not specified for key 'k' in JAAS config")),
        ("m required k=1a;", Err("Value not specified for key 'k' in JAAS config")),
        ("m required k=0x10;", Err("Value not specified for key 'k' in JAAS config")),
        ("m required k=1.2.3;", Err("Value not specified for key 'k' in JAAS config")),
        ("m required k=a1;", Ok(&[("m", Required, &[(Some("k"), "a1")])])),
        ("m required k=x-y;", Ok(&[("m", Required, &[(Some("k"), "x-y")])])),
        ("m required k=x.y;", Ok(&[("m", Required, &[(Some("k"), "x.y")])])),
        ("m required k=v-;", Ok(&[("m", Required, &[(Some("k"), "v-")])])),
        ("m required k=x1.5-2;", Ok(&[("m", Required, &[(Some("k"), "x1.5-2")])])),
        ("m required -k=v;", Err("Value not specified for key 'null' in JAAS config")),
        ("m required k-1=v;", Ok(&[("m", Required, &[(Some("k-1"), "v")])])),
        (
            "1module required;",
            Err("Invalid login module control flag 'module' in JAAS config"),
        ),
        (
            "-module required;",
            Err("Invalid login module control flag 'module' in JAAS config"),
        ),
        (
            ".module required;",
            Err("Invalid login module control flag 'module' in JAAS config"),
        ),
        ("m-1.x required;", Ok(&[("m-1.x", Required, &[])])),
        ("m required k=$x;", Ok(&[("m", Required, &[(Some("k"), "$x")])])),
        ("m required k=_x;", Ok(&[("m", Required, &[(Some("k"), "_x")])])),
        ("m required k=x$_y;", Ok(&[("m", Required, &[(Some("k"), "x$_y")])])),
        (
            "com.example.Outer$Inner required;",
            Ok(&[("com.example.Outer$Inner", Required, &[])]),
        ),
        ("_m required;", Ok(&[("_m", Required, &[])])),
        ("$m required;", Ok(&[("$m", Required, &[])])),
        ("m required k=x@y;", Err("Value not specified for key 'null' in JAAS config")),
        ("m required k=x:y;", Err("Value not specified for key 'null' in JAAS config")),
        ("m required k=a\\b;", Err("Value not specified for key 'null' in JAAS config")),
        ("m required k=v\\;", Err("Value not specified for key 'null' in JAAS config")),
        ("m required k=v!;", Err("Value not specified for key 'null' in JAAS config")),
        ("m required k=v#;", Err("Value not specified for key 'null' in JAAS config")),
        ("= required;", Err("")),
        ("@ required;", Err("")),
        ("{ required;", Err("")),
        (
            "m required k=v;;",
            Err("Login module control flag not specified in JAAS config"),
        ),
        (
            "m required k=v ; ;",
            Err("Login module control flag not specified in JAAS config"),
        ),
        (
            "m required k=\"v\";extra",
            Err("Login module control flag not specified in JAAS config"),
        ),
        (
            "m required k=\"v\"; n",
            Err("Login module control flag not specified in JAAS config"),
        ),
        ("/ m required;", Err("Login module not specified in JAAS config")),
        ("m required k=a/b;", Err("JAAS config entry not terminated by semi-colon")),
        ("m required k=\"a/b\";", Ok(&[("m", Required, &[(Some("k"), "a/b")])])),
        ("m required k=v/;", Err("JAAS config entry not terminated by semi-colon")),
        ("m required // comment\u{a} k=v;", Ok(&[("m", Required, &[(Some("k"), "v")])])),
        ("m required /* c */ k=v;", Ok(&[("m", Required, &[(Some("k"), "v")])])),
        ("m /* c */ required;", Ok(&[("m", Required, &[])])),
        ("m required k=/*c*/v;", Ok(&[("m", Required, &[(Some("k"), "v")])])),
        ("m required k=v//c\u{a};", Ok(&[("m", Required, &[(Some("k"), "v")])])),
        ("m required k=v; // trailing", Ok(&[("m", Required, &[(Some("k"), "v")])])),
        ("m required k=v; /* trailing */", Ok(&[("m", Required, &[(Some("k"), "v")])])),
        ("m required k=v; /* unterminated", Ok(&[("m", Required, &[(Some("k"), "v")])])),
        (
            "m required /* unterminated",
            Err("JAAS config entry not terminated by semi-colon"),
        ),
        ("m required /* a * b / c */ k=v;", Ok(&[("m", Required, &[(Some("k"), "v")])])),
        ("m required /** doc **/ k=v;", Ok(&[("m", Required, &[(Some("k"), "v")])])),
        ("m required k=v // c\u{d};", Ok(&[("m", Required, &[(Some("k"), "v")])])),
        ("m required k=v /* c\u{d}\u{a} */;", Ok(&[("m", Required, &[(Some("k"), "v")])])),
        ("m required k=v /c\u{a};", Ok(&[("m", Required, &[(Some("k"), "v")])])),
        (
            "m required k=\"unterminated;",
            Err("JAAS config entry not terminated by semi-colon"),
        ),
        (
            "m required k=\"a\u{a}b\";",
            Err("Value not specified for key 'b' in JAAS config"),
        ),
        (
            "m required k=\"a\u{d}b\";",
            Err("Value not specified for key 'b' in JAAS config"),
        ),
        (
            "m required k=\"a\u{d}\u{a}b\";",
            Err("Value not specified for key 'b' in JAAS config"),
        ),
        ("m required k=\"a\\\u{a}b\";", Ok(&[("m", Required, &[(Some("k"), "a\u{a}b")])])),
        ("m required k=\"\\101\\n\";", Ok(&[("m", Required, &[(Some("k"), "A\u{a}")])])),
        ("m required k=\"a\\\"b\";", Ok(&[("m", Required, &[(Some("k"), "a\"b")])])),
        ("m required k='it\\'s';", Ok(&[("m", Required, &[(Some("k"), "it's")])])),
        (
            "m required k=\"\\a\\b\\f\\n\\r\\t\\v\";",
            Ok(&[("m", Required, &[(Some("k"), "\u{7}\u{8}\u{c}\u{a}\u{d}\u{9}\u{b}")])]),
        ),
        (
            "m required k=\"\\101\\0\\477\\8\\q\\08\\377\\400\";",
            Ok(&[("m", Required, &[(Some("k"), "A\u{0}'78q\u{0}8\u{ff} 0")])]),
        ),
        ("m required k=\"\\x\";", Ok(&[("m", Required, &[(Some("k"), "x")])])),
        ("m required k=\"\\\\\";", Ok(&[("m", Required, &[(Some("k"), "\\")])])),
        ("m required k=\"\\7\";", Ok(&[("m", Required, &[(Some("k"), "\u{7}")])])),
        ("m required k=\"\\77\";", Ok(&[("m", Required, &[(Some("k"), "?")])])),
        ("m required k=\"\\777\";", Ok(&[("m", Required, &[(Some("k"), "?7")])])),
        ("m required k=\"\\0\";", Ok(&[("m", Required, &[(Some("k"), "\u{0}")])])),
        ("m required k=\"abc\\\";", Err("JAAS config entry not terminated by semi-colon")),
        ("m required k=\"abc\\", Err("JAAS config entry not terminated by semi-colon")),
        ("m required k=\"a;b\";", Ok(&[("m", Required, &[(Some("k"), "a;b")])])),
        ("m required k=\"a=b\";", Ok(&[("m", Required, &[(Some("k"), "a=b")])])),
        ("m required k=\"a//b\";", Ok(&[("m", Required, &[(Some("k"), "a//b")])])),
        ("m required k=\"a/*b*/\";", Ok(&[("m", Required, &[(Some("k"), "a/*b*/")])])),
        ("\"my module\" required;", Ok(&[("my module", Required, &[])])),
        ("\"\" required;", Err("")),
        ("'m' required k=v;", Ok(&[("m", Required, &[(Some("k"), "v")])])),
        ("m required k=\u{e9};", Ok(&[("m", Required, &[(Some("k"), "\u{e9}")])])),
        (
            "m required k=\u{65e5}\u{672c};",
            Ok(&[("m", Required, &[(Some("k"), "\u{65e5}\u{672c}")])]),
        ),
        ("m required k=x\u{a0}y;", Ok(&[("m", Required, &[(Some("k"), "x\u{a0}y")])])),
        (
            "m required k=x\u{85}y;",
            Err("Value not specified for key 'null' in JAAS config"),
        ),
        (
            "m required k=x\u{7f}y;",
            Err("Value not specified for key 'null' in JAAS config"),
        ),
        (
            "m required k=x\u{9f}y;",
            Err("Value not specified for key 'null' in JAAS config"),
        ),
        ("m required k=v\u{0};", Ok(&[("m", Required, &[(Some("k"), "v")])])),
        (
            "m required k=\"\u{e9}\\\u{e9}\";",
            Ok(&[("m", Required, &[(Some("k"), "\u{e9}\u{e9}")])]),
        ),
        ("m required k=\u{1f600};", Ok(&[("m", Required, &[(Some("k"), "\u{1f600}")])])),
        (
            "m required k=\"\u{1f600}\";",
            Ok(&[("m", Required, &[(Some("k"), "\u{1f600}")])]),
        ),
        ("\u{e9}module required;", Ok(&[("\u{e9}module", Required, &[])])),
        ("/\u{a}m required;", Err("Login module not specified in JAAS config")),
        ("/ \u{a}m required;", Ok(&[("m", Required, &[])])),
        ("m required k=v /\u{a};", Err("JAAS config entry not terminated by semi-colon")),
        ("m required k=v /\u{d}\u{a};", Ok(&[("m", Required, &[(Some("k"), "v")])])),
        ("m required k=v /*/ x */;", Ok(&[("m", Required, &[(Some("k"), "v")])])),
        ("m required k=v /**/;", Ok(&[("m", Required, &[(Some("k"), "v")])])),
        ("m required k=v /* *\u{a}/ */;", Ok(&[("m", Required, &[(Some("k"), "v")])])),
        ("m required k=v /* *\u{d}/ */;", Ok(&[("m", Required, &[(Some("k"), "v")])])),
        ("m requ\u{131}red;", Ok(&[("m", Required, &[])])),
        ("m \u{17f}ufficient;", Ok(&[("m", Sufficient, &[])])),
        (
            "m REQU\u{130}RED;",
            Err("Invalid login module control flag 'REQU\u{130}RED' in JAAS config"),
        ),
        ("m required k=v\u{b};", Ok(&[("m", Required, &[(Some("k"), "v")])])),
        ("m required k=\"a\u{0}b\";", Ok(&[("m", Required, &[(Some("k"), "a\u{0}b")])])),
        ("m required k=x\u{ad}y;", Ok(&[("m", Required, &[(Some("k"), "x\u{ad}y")])])),
        ("m required k=\u{ff};", Ok(&[("m", Required, &[(Some("k"), "\u{ff}")])])),
        ("m required k=\u{100};", Ok(&[("m", Required, &[(Some("k"), "\u{100}")])])),
        ("m required k=x\u{2028}y;", Ok(&[("m", Required, &[(Some("k"), "x\u{2028}y")])])),
        (
            "m required\u{3000}k=v;",
            Err("Invalid login module control flag 'required\u{3000}k' in JAAS config"),
        ),
        ("m required k=\"\\\u{d}\";", Ok(&[("m", Required, &[(Some("k"), "\u{d}")])])),
        ("m required k='\\'';", Ok(&[("m", Required, &[(Some("k"), "'")])])),
        (
            "m required \"k\\",
            Err("Value not specified for key 'k\u{ffff}' in JAAS config"),
        ),
        (
            "m \"req\\",
            Err("Invalid login module control flag 'req\u{ffff}' in JAAS config"),
        ),
        ("m required k=\"\\1", Err("JAAS config entry not terminated by semi-colon")),
        ("m required k=-.;", Err("Value not specified for key 'k' in JAAS config")),
        ("m required k=.;", Err("Value not specified for key 'k' in JAAS config")),
        ("m required k=1.;", Err("Value not specified for key 'k' in JAAS config")),
        ("m required k=a.;", Ok(&[("m", Required, &[(Some("k"), "a.")])])),
        ("m required k=a..b;", Ok(&[("m", Required, &[(Some("k"), "a..b")])])),
        (
            "\"m\"\"n\" required;",
            Err("Invalid login module control flag 'n' in JAAS config"),
        ),
        ("m required k=\"x\"y;", Err("Value not specified for key 'y' in JAAS config")),
        ("m required k=x\"y\";", Err("Value not specified for key 'y' in JAAS config")),
        ("m required k=x'y';", Err("Value not specified for key 'y' in JAAS config")),
        ("m required k=\"a\"\u{a};", Ok(&[("m", Required, &[(Some("k"), "a")])])),
        (
            "m required k=v;m2 required;",
            Ok(&[("m", Required, &[(Some("k"), "v")]), ("m2", Required, &[])]),
        ),
        ("'' required k=v;", Err("")),
        ("m required k=\"\\u0041\";", Ok(&[("m", Required, &[(Some("k"), "u0041")])])),
        ("m required k=\"\\\\u0041\";", Ok(&[("m", Required, &[(Some("k"), "\\u0041")])])),
        ("m required k=\"\\'\\\"\";", Ok(&[("m", Required, &[(Some("k"), "'\"")])])),
        (
            "m required k=v; n required;",
            Ok(&[("m", Required, &[(Some("k"), "v")]), ("n", Required, &[])]),
        ),
        (
            "m required; n required; o required;",
            Ok(&[("m", Required, &[]), ("n", Required, &[]), ("o", Required, &[])]),
        ),
        (
            "m required k=v; n optional k=w;",
            Ok(&[
                ("m", Required, &[(Some("k"), "v")]),
                ("n", Optional, &[(Some("k"), "w")]),
            ]),
        ),
    ];

    /// Whether `actual` is the outcome `expected` describes.
    pub(crate) fn assert_entries(value: &str, actual: &[AppConfigurationEntry], expected: &[ExpectedEntry]) {
        assert_eq!(expected.len(), actual.len(), "{value:?}: {actual:?}");
        for (entry, (login_module_name, control_flag, options)) in actual.iter().zip(expected) {
            assert_eq!(*login_module_name, entry.login_module_name(), "{value:?}");
            assert_eq!(*control_flag, entry.control_flag(), "{value:?}");
            let options: HashMap<Option<String>, String> = options
                .iter()
                .map(|(key, value)| (key.map(str::to_owned), value.to_string()))
                .collect();
            assert_eq!(&options, entry.options(), "{value:?}");
        }
    }

    /// Every value of [`JAVA_OUTCOMES`] parses, or fails, as in Java: the
    /// entries, their order, the control flags and the options, or the exact
    /// error message.
    #[test]
    fn test_parse_matches_java() {
        for (value, expected) in JAVA_OUTCOMES {
            match (JaasConfig::new("KafkaClient", value), expected) {
                (Ok(config), Ok(entries)) => {
                    assert_entries(value, config.get_app_configuration_entry("KafkaClient").unwrap(), entries);
                    assert!(config.get_app_configuration_entry("Other").is_none(), "{value:?}");
                },
                (Err(error), Err(message)) => {
                    assert!(error.is_local_illegal_argument_error(), "{value:?}: {error:?}");
                    assert_eq!(*message, error.message(), "{value:?}");
                },
                (actual, expected) => panic!("{value:?}: expected {expected:?}, got {actual:?}"),
            }
        }
    }

    /// The control flags, in any case, through a word or a quoted string.
    #[test]
    fn test_login_module_control_flag() {
        for (flag, expected) in [
            ("required", Required),
            ("REQUISITE", Requisite),
            ("Sufficient", Sufficient),
            ("oPtIoNaL", Optional),
        ] {
            assert_eq!(expected, JaasConfig::login_module_control_flag(Some(flag)).unwrap());
        }
        let error = JaasConfig::login_module_control_flag(None).unwrap_err();
        assert_eq!("Login module control flag is not available in the JAAS config", error.message());
        let error = JaasConfig::login_module_control_flag(Some("requiredX")).unwrap_err();
        assert_eq!("Invalid login module control flag 'requiredX' in JAAS config", error.message());
    }

    /// The JDK's `LoginModuleControlFlag.toString()`.
    #[test]
    fn test_login_module_control_flag_to_string() {
        for (flag, expected) in [
            (Required, "LoginModuleControlFlag: required"),
            (Requisite, "LoginModuleControlFlag: requisite"),
            (Sufficient, "LoginModuleControlFlag: sufficient"),
            (Optional, "LoginModuleControlFlag: optional"),
        ] {
            assert_eq!(expected, flag.to_string());
        }
    }

    /// The JDK's `AppConfigurationEntry` rejects a `null` or empty login
    /// module name with a bare `IllegalArgumentException`.
    #[test]
    fn test_app_configuration_entry_requires_a_login_module_name() {
        for name in [None, Some(String::new())] {
            let error = AppConfigurationEntry::new(name, Required, HashMap::new()).unwrap_err();
            assert!(error.is_local_illegal_argument_error(), "{error:?}");
            assert_eq!("", error.message());
        }
        let entry = AppConfigurationEntry::new(Some(" ".to_string()), Optional, HashMap::new()).unwrap();
        assert_eq!(" ", entry.login_module_name());
        assert_eq!(Optional, entry.control_flag());
    }

    /// `{:?}` renders the option keys but none of their values, which hold
    /// the credentials.
    #[test]
    fn test_debug_hides_option_values() {
        let config = JaasConfig::new(
            "KafkaClient",
            "org.apache.kafka.common.security.plain.PlainLoginModule required \
             username=\"jaas-user\" password=\"jaas-S3cr3t\";",
        )
        .unwrap();
        for rendered in [format!("{config:?}"), format!("{config:#?}")] {
            assert!(!rendered.contains("jaas-S3cr3t"), "{rendered}");
            assert!(!rendered.contains("jaas-user"), "{rendered}");
            assert!(rendered.contains("password"), "{rendered}");
            assert!(rendered.contains("Required"), "{rendered}");
        }
    }
}
