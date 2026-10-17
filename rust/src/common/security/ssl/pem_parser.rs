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

//! Translated from `DefaultSslEngineFactory.PemParser`.

use regex::Regex;

use crate::common::security::ssl::base64::Base64;

/// Java's `\s`: the ASCII whitespace `[ \t\n\x0B\f\r]` (its default, not the Unicode one).
const WS: &str = r"[ \t\n\x0B\x0C\r]";
/// The characters of [`WS`], to put inside a bracket expression.
const WS_CHARS: &str = r" \t\n\x0B\x0C\r";
/// Java's `.`: anything but a line terminator.
const DOT: &str = r"[^\n\r\x{85}\x{2028}\x{2029}]";

/// Parser to process certificate/private key entries from PEM files.
///
/// Examples:
///
/// ```text
/// -----BEGIN CERTIFICATE-----
/// Base64 cert
/// -----END CERTIFICATE-----
///
/// -----BEGIN ENCRYPTED PRIVATE KEY-----
/// Base64 private key
/// -----END ENCRYPTED PRIVATE KEY-----
/// ```
///
/// Additional data may be included before headers, so all the entries within the PEM are
/// matched, whatever precedes the name in the label: `PemParser::new("PRIVATE KEY")` reads
/// `PRIVATE KEY`, `ENCRYPTED PRIVATE KEY`, `RSA PRIVATE KEY` and `EC PRIVATE KEY` entries.
///
/// The pattern is Java's, character for character, with `\s` and `.` spelled out because their
/// meaning differs between Java's `Pattern` and the `regex` crate's.
#[doc(alias = "org.apache.kafka.common.security.ssl.DefaultSslEngineFactory$PemParser")]
pub(crate) struct PemParser {
    name: String,
    pattern: Regex,
}

impl PemParser {
    /// Creates a parser for the entries whose label contains `name`.
    #[doc(alias = "org.apache.kafka.common.security.ssl.DefaultSslEngineFactory$PemParser#PemParser")]
    pub(crate) fn new(name: &str) -> Self {
        // `"-+%s\\s*.*%s[^-]*-+\\s+"`
        let begin_or_end_format =
            |kind: &str, name_ignore_space: &str| format!("-+{kind}{WS}*{DOT}*{name_ignore_space}[^-]*-+{WS}+");
        // `name.replace(" ", "\\s+")`
        let name_ignore_space = name.split(' ').map(regex::escape).collect::<Vec<_>>().join(&format!("{WS}+"));
        // `"\\s*[^\\r\\n]*:[^\\r\\n]*[\\r\\n]+"`
        let encoding_params = format!(r"{WS}*[^\r\n]*:[^\r\n]*[\r\n]+");
        // `"([a-zA-Z0-9/+=\\s]*)"`
        let base64_pattern = format!("([a-zA-Z0-9/+={WS_CHARS}]*)");
        let pattern_str = format!(
            "{}(?:{})*{}{}",
            begin_or_end_format("BEGIN", &name_ignore_space),
            encoding_params,
            base64_pattern,
            begin_or_end_format("END", &name_ignore_space)
        );
        Self {
            name: name.to_string(),
            // The pattern is made of the constants above and `name`, a fixed word list from the
            // callers, so it always compiles.
            pattern: Regex::new(&pattern_str).expect("the PEM entry pattern is valid"),
        }
    }

    /// The decoded entries of `pem`, in order.
    ///
    /// # Errors
    ///
    /// The message of what Java throws: `No matching <name> entries in PEM file`
    /// (`InvalidConfigurationException`) when there is no entry, or the message of the
    /// `IllegalArgumentException` that `Base64.getDecoder().decode` throws for the first
    /// entry whose body is not valid base64. The caller words the error around it.
    #[doc(alias = "org.apache.kafka.common.security.ssl.DefaultSslEngineFactory$PemParser#pemEntries")]
    pub(crate) fn pem_entries(&self, pem: &str) -> Result<Vec<Vec<u8>>, String> {
        // `pem + "\n"`: allow last newline to be omitted in value
        let pem = format!("{pem}\n");
        let mut entries = Vec::new();
        for captures in self.pattern.captures_iter(&pem) {
            // `matcher.group(1).replaceAll("\\s", "")`
            let base64: String = captures[1].chars().filter(|c| !is_java_whitespace(*c)).collect();
            entries.push(Base64::get_decoder().decode(base64.as_bytes())?);
        }
        if entries.is_empty() {
            return Err(format!("No matching {} entries in PEM file", self.name));
        }
        Ok(entries)
    }
}

/// What Java's `replaceAll("\\s", "")` removes.
fn is_java_whitespace(c: char) -> bool {
    matches!(c, ' ' | '\t' | '\n' | '\x0B' | '\x0C' | '\r')
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The output of `PemOracle`: Apache Kafka's `PemParser("PRIVATE KEY")` run on the same inputs,
    /// as `<count> [<hex of each entry>]`.
    fn run(pem: &str) -> String {
        let hex = |der: &[u8]| der.iter().map(|b| format!("{b:02x}")).collect::<String>();
        match PemParser::new("PRIVATE KEY").pem_entries(pem) {
            Ok(entries) => format!(
                "{} [{}]",
                entries.len(),
                entries.iter().map(|e| hex(e)).collect::<Vec<_>>().join(", ")
            ),
            Err(message) if message.starts_with("No matching") => "0 []".to_string(),
            Err(message) => format!("1 [IAE({message})]"),
        }
    }

    fn key(label: &str, body: &str) -> String {
        format!("-----BEGIN {label}-----\n{body}\n-----END {label}-----\n")
    }

    /// Every row is what Java answered (`PemOracle.java`, the pattern copied from
    /// `DefaultSslEngineFactory`): which texts match, and what each entry decodes to.
    #[test]
    fn test_entries_match_what_java_matches() {
        let cases: Vec<(&str, String, &str)> = vec![
            ("label PRIVATE KEY", key("PRIVATE KEY", "AQID"), "1 [010203]"),
            ("label ENCRYPTED PRIVATE KEY", key("ENCRYPTED PRIVATE KEY", "AQID"), "1 [010203]"),
            ("label RSA PRIVATE KEY", key("RSA PRIVATE KEY", "AQID"), "1 [010203]"),
            ("label EC PRIVATE KEY", key("EC PRIVATE KEY", "AQID"), "1 [010203]"),
            ("label FOO BAR PRIVATE KEY", key("FOO BAR PRIVATE KEY", "AQID"), "1 [010203]"),
            (
                "marker after text on the same line",
                format!("noise {}", key("PRIVATE KEY", "AQID")),
                "1 [010203]",
            ),
            (
                "mismatched labels",
                "-----BEGIN RSA PRIVATE KEY-----\nAQID\n-----END EC PRIVATE KEY-----\n".to_string(),
                "1 [010203]",
            ),
            ("illegal char in body", key("PRIVATE KEY", "AQ!D"), "0 []"),
            (
                "header lines",
                "-----BEGIN RSA PRIVATE KEY-----\nProc-Type: 4,ENCRYPTED\nDEK-Info: AES-128-CBC,00\n\nAQID\n-----END RSA PRIVATE KEY-----\n"
                    .to_string(),
                "1 [010203]",
            ),
            ("no END", "-----BEGIN PRIVATE KEY-----\nAQID\n".to_string(), "0 []"),
            (
                "END without final newline",
                "-----BEGIN PRIVATE KEY-----\nAQID\n-----END PRIVATE KEY-----".to_string(),
                "1 [010203]",
            ),
            (
                "odd whitespace in the name",
                "-----BEGIN   PRIVATE\tKEY-----\nAQID\n-----END PRIVATE   KEY-----\n".to_string(),
                "1 [010203]",
            ),
            (
                "lower case",
                "-----begin private key-----\nAQID\n-----end private key-----\n".to_string(),
                "0 []",
            ),
            (
                "two entries",
                format!("{}{}", key("PRIVATE KEY", "AQID"), key("PRIVATE KEY", "BAUG")),
                "2 [010203, 040506]",
            ),
            (
                "body on the BEGIN line",
                "-----BEGIN PRIVATE KEY-----AQID\n-----END PRIVATE KEY-----\n".to_string(),
                "0 []",
            ),
            (
                "empty body",
                "-----BEGIN PRIVATE KEY-----\n-----END PRIVATE KEY-----\n".to_string(),
                "1 []",
            ),
            (
                "CRLF",
                "-----BEGIN PRIVATE KEY-----\r\nAQID\r\n-----END PRIVATE KEY-----\r\n".to_string(),
                "1 [010203]",
            ),
            (
                "text after END",
                "-----BEGIN PRIVATE KEY-----\nAQID\n-----END PRIVATE KEY-----extra".to_string(),
                "0 []",
            ),
            (
                "text in the label",
                "-----BEGIN PRIVATE KEY (foo)-----\nAQID\n-----END PRIVATE KEY (foo)-----\n".to_string(),
                "1 [010203]",
            ),
            (
                "body in two lines, indented",
                "-----BEGIN PRIVATE KEY-----\n  AQID  \n\tBAUG\n-----END PRIVATE KEY-----\n".to_string(),
                "1 [010203040506]",
            ),
            (
                "certificate then key",
                format!(
                    "-----BEGIN CERTIFICATE-----\nAAAA\n-----END CERTIFICATE-----\n{}",
                    key("PRIVATE KEY", "AQID")
                ),
                "1 [010203]",
            ),
            ("unpadded body", key("PRIVATE KEY", "AQIDBA"), "1 [01020304]"),
            (
                "bad padding",
                key("PRIVATE KEY", "AQ="),
                "1 [IAE(Input byte array has wrong 4-byte ending unit)]",
            ),
            (
                "one char body",
                key("PRIVATE KEY", "A"),
                "1 [IAE(Input byte[] should at least have 2 bytes for base64 bytes)]",
            ),
            ("none", "nothing here".to_string(), "0 []"),
        ];
        for (name, pem, expected) in cases {
            assert_eq!(expected, run(&pem), "{name}");
        }
    }

    #[test]
    fn test_no_entry_is_the_message_java_throws() {
        assert_eq!(
            Err("No matching PRIVATE KEY entries in PEM file".to_string()),
            PemParser::new("PRIVATE KEY").pem_entries("nothing here")
        );
        assert_eq!(
            Err("No matching CERTIFICATE entries in PEM file".to_string()),
            PemParser::new("CERTIFICATE").pem_entries(&key("PRIVATE KEY", "AQID"))
        );
    }
}
