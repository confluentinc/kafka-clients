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

//! String-value parsing from `org.apache.kafka.common.config.ConfigDef`.
//!
//! The client configs in this crate are plain structs with named fields, not
//! a `ConfigDef` (see the module docs of `ProducerConfig` / `ConsumerConfig`),
//! so only the part of `ConfigDef.parseType` that every config shares is
//! translated here: how a `String` value is trimmed and, for a `LIST`, split.
//! Keeping it in one place is what makes `bootstrap.servers` (and every other
//! list-typed key) parse identically in the producer, consumer, admin client
//! and SSL configs, as it does in Java where all of them go through
//! `ConfigDef.parseType`.
//!
//! Of `ConfigDef`'s validators, only [`ValidList`]'s `anyNonDuplicateValues`
//! form is translated here, because every list-typed client key uses it and a
//! shared translation keeps its messages identical across the configs. The
//! scalar validators (`Range`, `ValidString`, ...) are inlined at the configs'
//! own keys.

use std::collections::HashSet;

use crate::common::Error;

/// The `String`-value half of Java's `ConfigDef.parseType`.
///
/// A unit struct because Java's methods are statics on `ConfigDef`
/// (CLAUDE.md §2: static functions are exported through the struct defining
/// them). The rest of `ConfigDef` (`define`, most validators, documentation)
/// has no counterpart: the configs validate in their own constructors, using
/// the nested [`ValidList`] for their list-typed keys.
#[doc(alias = "org.apache.kafka.common.config.ConfigDef")]
pub(crate) struct ConfigDef;

impl ConfigDef {
    /// Java's `String.trim()`, which `ConfigDef.parseType` applies to every
    /// `String` value before parsing it (`ConfigDef.java:707-709`).
    ///
    /// Unlike [`str::trim`] it strips every character `<= U+0020` (all ASCII
    /// control characters and the space) and nothing else — in particular not
    /// Unicode whitespace such as U+00A0.
    pub(crate) fn trim(value: &str) -> &str {
        value.trim_matches(|c: char| c <= ' ')
    }

    /// The `Type.LIST` arm of Java's `ConfigDef.parseType` for a `String`
    /// value (`ConfigDef.java:768-777`):
    ///
    /// ```java
    /// if (trimmed.isEmpty())
    ///     return List.of();
    /// else
    ///     return Arrays.asList(COMMA_WITH_WHITESPACE.split(trimmed, -1));
    /// ```
    ///
    /// with `COMMA_WITH_WHITESPACE = Pattern.compile("\\s*,\\s*")`. So the
    /// whole value is trimmed, then split on commas together with any
    /// surrounding regex whitespace (`[ \t\n\x0B\f\r]`). The `-1` limit keeps
    /// empty elements, trailing ones included: `"a,,b,"` is
    /// `["a", "", "b", ""]`. Whitespace inside an element is kept, so
    /// `"a b,c"` is `["a b", "c"]`.
    pub(crate) fn parse_list(value: &str) -> Vec<String> {
        let trimmed = Self::trim(value);
        if trimmed.is_empty() {
            return Vec::new();
        }
        // Splitting on ',' and stripping regex whitespace from each piece is
        // the same split: `\s*` around a comma absorbs exactly the whitespace
        // adjacent to it, and the two outer ends are already trimmed (Java's
        // `trim` strips a superset of `\s`), so stripping them is a no-op.
        trimmed
            .split(',')
            .map(|element| element.trim_matches(Self::is_regex_whitespace).to_string())
            .collect()
    }

    /// Java regex `\s`: `[ \t\n\x0B\f\r]`.
    fn is_regex_whitespace(c: char) -> bool {
        matches!(c, ' ' | '\t' | '\n' | '\u{0B}' | '\u{0C}' | '\r')
    }
}

/// Java's `ConfigDef.ValidList`: validates a parsed `LIST` value.
///
/// Only the `anyNonDuplicateValues(isEmptyAllowed, isNullAllowed)` factory is
/// translated, which is the one every client list key uses
/// (`ProducerConfig`, `ConsumerConfig`, `AdminClientConfig`, `SslConfigs`).
/// Its other factories, `in(String...)` and `in(boolean, String...)`, restrict
/// the elements to a set of valid strings (Java's embedded `ValidString`);
/// no client key this crate parses uses them, so that half is omitted.
///
/// Java's `ConfigDef.parseValue` calls `ensureValid` directly, without
/// wrapping (`ConfigDef.java:553`), and `ValidList` throws the
/// single-message `ConfigException(String)`. So the errors are
/// [`Error::config_message`], with no `Invalid value ... for configuration`
/// prefix.
#[doc(alias = "org.apache.kafka.common.config.ConfigDef$ValidList")]
pub(crate) struct ValidList {
    is_empty_allowed: bool,
    is_null_allowed: bool,
}

impl ValidList {
    /// Java's `ValidList.anyNonDuplicateValues(isEmptyAllowed, isNullAllowed)`:
    /// any elements are allowed, provided none is empty or duplicated.
    /// `is_empty_allowed` decides whether the list itself may be empty.
    #[doc(alias = "org.apache.kafka.common.config.ConfigDef$ValidList#anyNonDuplicateValues")]
    pub(crate) fn any_non_duplicate_values(is_empty_allowed: bool, is_null_allowed: bool) -> Self {
        Self { is_empty_allowed, is_null_allowed }
    }

    /// Java's `ValidList.ensureValid(name, value)`, with Java's check order:
    /// null, then an empty list, then duplicates, then each element.
    ///
    /// `None` is Java's `null` value, which reaches a validator only as a
    /// `null` default; a value parsed from a `String` is never null.
    ///
    /// # Errors
    ///
    /// [`Error::Config`] with Java's exact message when the value is null and
    /// nulls are not allowed, the list is empty and empty lists are not
    /// allowed, an element is repeated, or an element is the empty string.
    #[doc(alias = "org.apache.kafka.common.config.ConfigDef$ValidList#ensureValid")]
    pub(crate) fn ensure_valid(&self, name: &str, value: Option<&[String]>) -> Result<(), Error> {
        let Some(values) = value else {
            if self.is_null_allowed {
                return Ok(());
            }
            return Err(Error::config_message(format!(
                "Configuration '{name}' values must not be null."
            )));
        };
        if !self.is_empty_allowed && values.is_empty() {
            // `validString.validStrings` is always empty for
            // `anyNonDuplicateValues`, so the valid-values text is fixed.
            return Err(Error::config_message(format!(
                "Configuration '{name}' must not be empty. Valid values include: any non-empty value"
            )));
        }
        // `Set.copyOf(values).size() != values.size()`.
        let distinct: HashSet<&str> = values.iter().map(String::as_str).collect();
        if distinct.len() != values.len() {
            return Err(Error::config_message(format!(
                "Configuration '{name}' values must not be duplicated."
            )));
        }
        // `validateIndividualValues`, without the `ValidString` check (see
        // the type docs).
        if values.iter().any(String::is_empty) {
            return Err(Error::config_message(format!(
                "Configuration '{name}' values must not be empty."
            )));
        }
        Ok(())
    }

    /// The common call: parse `value` as a `LIST` ([`ConfigDef::parse_list`])
    /// and validate it with `anyNonDuplicateValues(is_empty_allowed, false)`,
    /// which is what `ConfigDef.parse` does for each such key.
    ///
    /// As in `ConfigDef.parseValue` (`ConfigDef.java:544-551`), duplicates are
    /// removed first, keeping the order of first occurrence, with a warning;
    /// the deduplicated list is validated and returned. The duplicate check in
    /// [`Self::ensure_valid`] therefore cannot fire here, as in Java.
    ///
    /// # Errors
    ///
    /// As [`Self::ensure_valid`].
    pub(crate) fn parse_any_non_duplicate_values(
        name: &str,
        value: &str,
        is_empty_allowed: bool,
    ) -> Result<Vec<String>, Error> {
        let original = ConfigDef::parse_list(value);
        let mut values: Vec<String> = Vec::with_capacity(original.len());
        for v in &original {
            if !values.contains(v) {
                values.push(v.clone());
            }
        }
        if values.len() != original.len() {
            log::warn!(
                "Configuration key \"{}\" contains duplicate values. Duplicates will be removed. \
                 The original value is: [{}], the updated value is: [{}]",
                name,
                original.join(", "),
                values.join(", ")
            );
        }
        Self::any_non_duplicate_values(is_empty_allowed, false).ensure_valid(name, Some(&values))?;
        Ok(values)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config_error_message(result: Result<Vec<String>, Error>) -> String {
        match result {
            Err(Error::Config(e)) => e.message().to_string(),
            other => panic!("expected a ConfigError, got {other:?}"),
        }
    }

    /// The value is trimmed as a whole and split on `\s*,\s*`. The first
    /// assertion is the `Type.LIST` case of `ConfigDefTest.testBasicTypes`
    /// (`" a , b, c"` parses to `["a", "b", "c"]`); the other `ConfigDef`
    /// types it covers have no shared parser here.
    #[test]
    fn test_parse_list() {
        assert_eq!(ConfigDef::parse_list(" a , b, c"), vec!["a", "b", "c"]);
        assert_eq!(ConfigDef::parse_list("a:1,b:2"), vec!["a:1", "b:2"]);
        assert_eq!(ConfigDef::parse_list("a:1, b:2"), vec!["a:1", "b:2"]);
        assert_eq!(ConfigDef::parse_list(" a:1 ,b:2 "), vec!["a:1", "b:2"]);
        assert_eq!(ConfigDef::parse_list("a:1 , b:2 "), vec!["a:1", "b:2"]);
        assert_eq!(ConfigDef::parse_list("a:1\t,\nb:2"), vec!["a:1", "b:2"]);
        assert_eq!(ConfigDef::parse_list("a"), vec!["a"]);
    }

    /// An empty or blank value is the empty list, not `[""]`.
    #[test]
    fn test_parse_list_empty() {
        assert!(ConfigDef::parse_list("").is_empty());
        assert!(ConfigDef::parse_list("   ").is_empty());
    }

    /// `split(trimmed, -1)` keeps empty elements, trailing ones included, and
    /// whitespace inside an element is not touched.
    #[test]
    fn test_parse_list_keeps_empty_elements_and_inner_whitespace() {
        assert_eq!(ConfigDef::parse_list("a,,b"), vec!["a", "", "b"]);
        assert_eq!(ConfigDef::parse_list("a, ,b"), vec!["a", "", "b"]);
        assert_eq!(ConfigDef::parse_list("a,"), vec!["a", ""]);
        assert_eq!(ConfigDef::parse_list(",a"), vec!["", "a"]);
        assert_eq!(ConfigDef::parse_list("a b,c"), vec!["a b", "c"]);
    }

    /// `String.trim()` strips `<= U+0020` only; `\s` does not include U+00A0.
    #[test]
    fn test_parse_list_java_whitespace_classes() {
        assert_eq!(ConfigDef::trim("\u{1}a\u{1F} "), "a");
        assert_eq!(ConfigDef::trim("\u{A0}a"), "\u{A0}a");
        assert_eq!(ConfigDef::parse_list("a,\u{A0}b"), vec!["a", "\u{A0}b"]);
    }

    /// `ValidList.anyNonDuplicateValues` rejects an empty element with
    /// Java's exact message, including one produced by blank space between
    /// commas.
    #[test]
    fn test_valid_list_rejects_empty_element() {
        for value in ["a,,b", "a, ,b", "a,", ",a"] {
            assert_eq!(
                config_error_message(ValidList::parse_any_non_duplicate_values("k", value, true)),
                "Configuration 'k' values must not be empty."
            );
        }
    }

    /// `ConfigDef.parseValue` removes repeated elements (after whitespace
    /// around the commas is stripped), keeping first-occurrence order, before
    /// validating. `",,"` dedupes to `[""]`, which fails as empty.
    #[test]
    fn test_valid_list_removes_duplicates() {
        assert_eq!(
            ValidList::parse_any_non_duplicate_values("k", "a:1,a:1", false).unwrap(),
            vec!["a:1"]
        );
        assert_eq!(ValidList::parse_any_non_duplicate_values("k", "a,a", true).unwrap(), vec!["a"]);
        assert_eq!(
            ValidList::parse_any_non_duplicate_values("k", "b, a ,b", true).unwrap(),
            vec!["b", "a"]
        );
        assert_eq!(
            config_error_message(ValidList::parse_any_non_duplicate_values("k", ",,", true)),
            "Configuration 'k' values must not be empty."
        );
    }

    /// `ensure_valid` itself still rejects duplicates, as Java's
    /// `anyNonDuplicateValues` does when called directly.
    #[test]
    fn test_valid_list_ensure_valid_rejects_duplicate() {
        let values = vec!["a".to_string(), "a".to_string()];
        match ValidList::any_non_duplicate_values(true, false).ensure_valid("k", Some(&values)) {
            Err(Error::Config(e)) => assert_eq!(e.message(), "Configuration 'k' values must not be duplicated."),
            other => panic!("expected a ConfigError, got {other:?}"),
        }
    }

    /// An empty (or blank) value is the empty list, which only
    /// `isEmptyAllowed = true` accepts.
    #[test]
    fn test_valid_list_empty_list() {
        for value in ["", "  "] {
            assert_eq!(
                config_error_message(ValidList::parse_any_non_duplicate_values("k", value, false)),
                "Configuration 'k' must not be empty. Valid values include: any non-empty value"
            );
            assert!(ValidList::parse_any_non_duplicate_values("k", value, true).unwrap().is_empty());
        }
    }

    /// A null value passes only when nulls are allowed.
    #[test]
    fn test_valid_list_null() {
        match ValidList::any_non_duplicate_values(true, false).ensure_valid("k", None) {
            Err(Error::Config(e)) => assert_eq!(e.message(), "Configuration 'k' values must not be null."),
            other => panic!("expected a ConfigError, got {other:?}"),
        }
        assert!(ValidList::any_non_duplicate_values(true, true).ensure_valid("k", None).is_ok());
    }

    /// A list of distinct non-empty elements passes and is returned parsed.
    #[test]
    fn test_valid_list_accepts_valid_list() {
        assert_eq!(
            ValidList::parse_any_non_duplicate_values("k", " a:1 , b:2", false).unwrap(),
            vec!["a:1", "b:2"]
        );
    }
}
