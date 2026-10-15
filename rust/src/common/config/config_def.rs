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

//! String-value parsing and shared validators from `org.apache.kafka.common.config.ConfigDef`.
//!
//! The client configs in this crate are plain structs with named fields, not
//! a `ConfigDef` (see the module docs of `ProducerConfig` / `ConsumerConfig`),
//! so only the parts of `ConfigDef.parseType` the configs share are
//! translated here: how a `String` value is trimmed and, for a `LIST`, split,
//! and how a number is parsed, with Java's error messages. Keeping them in one
//! place is what makes `bootstrap.servers` (and every other list-typed key)
//! parse identically in the producer, consumer, admin client and SSL configs,
//! as it does in Java where all of them go through `ConfigDef.parseType`.
//!
//! Of `ConfigDef`'s validators, [`ValidList`]'s `anyNonDuplicateValues` form
//! and [`Range`] are translated here, so their messages are identical across
//! the configs that use them. The other validators (`ValidString`, ...) are
//! inlined at the configs' own keys.

use std::collections::HashSet;

use crate::common::Error;

/// The `String`-value half of Java's `ConfigDef.parseType`.
///
/// A unit struct because Java's methods are statics on `ConfigDef`
/// (CLAUDE.md §2: static functions are exported through the struct defining
/// them). The rest of `ConfigDef` (`define`, most validators, documentation)
/// has no counterpart: the configs validate in their own constructors, using
/// the nested [`ValidList`] and [`Range`] where Java's definitions do.
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

    /// The `Type.INT` arm of Java's `ConfigDef.parseType` for a `String`
    /// value: `Integer.parseInt(trimmed)` (`ConfigDef.java:736-742`).
    ///
    /// # Errors
    ///
    /// [`Error::Config`] when the trimmed value is not an `int`, with
    /// `parseType`'s `NumberFormatException` message (`ConfigDef.java:788-789`):
    /// `Invalid value <value> for configuration <name>: Not a number of type
    /// INT`, where `<value>` is the original, untrimmed string. Java's
    /// `parseInt` also accepts non-ASCII Unicode digits; this accepts ASCII
    /// digits only.
    pub(crate) fn parse_int(name: &str, value: &str) -> Result<i32, Error> {
        Self::trim(value)
            .parse::<i32>()
            .map_err(|_| Self::not_a_number(name, value, "INT"))
    }

    /// The `Type.LONG` arm of Java's `ConfigDef.parseType` for a `String`
    /// value: `Long.parseLong(trimmed)` (`ConfigDef.java:752-760`).
    ///
    /// # Errors
    ///
    /// As [`Self::parse_int`], with `Not a number of type LONG`.
    pub(crate) fn parse_long(name: &str, value: &str) -> Result<i64, Error> {
        Self::trim(value)
            .parse::<i64>()
            .map_err(|_| Self::not_a_number(name, value, "LONG"))
    }

    /// `parseType`'s `catch (NumberFormatException e)`: `new
    /// ConfigException(name, value, "Not a number of type " + type)`, with the
    /// original value (`ConfigDef.java:788-789`).
    fn not_a_number(name: &str, value: &str, type_name: &str) -> Error {
        Error::config_name_value_message(name, value, format!("Not a number of type {type_name}"))
    }
}

/// What Java's `ConfigDef.Range` uses of the `java.lang.Number` its bounds
/// and values are: `doubleValue()` for the comparison and `toString()` for
/// the error message.
///
/// A crate-private trait rather than a type from Java: the Java parameters are
/// `Number`, which Rust has no counterpart for, and the trait lets one
/// [`Range`] serve each numeric config type.
pub(crate) trait Number: Copy {
    /// Java's `Number.doubleValue()`.
    fn double_value(self) -> f64;

    /// Java's `toString()` of the boxed value.
    fn java_to_string(self) -> String;
}

impl Number for i32 {
    fn double_value(self) -> f64 {
        f64::from(self)
    }

    fn java_to_string(self) -> String {
        self.to_string()
    }
}

impl Number for i64 {
    fn double_value(self) -> f64 {
        // Java's `Long.doubleValue()` is the same rounding conversion.
        self as f64
    }

    fn java_to_string(self) -> String {
        self.to_string()
    }
}

/// Java's `ConfigDef.Range`: a numeric range with an inclusive lower bound
/// and an optional inclusive upper bound.
///
/// Java's `ensureValid` takes an `Object` and rejects `null` with `Value must
/// be non-null`; a value parsed from a `String` property is never null, so
/// [`Self::ensure_valid`] takes the number itself.
#[doc(alias = "org.apache.kafka.common.config.ConfigDef$Range")]
pub(crate) struct Range<T> {
    min: T,
    max: Option<T>,
}

impl<T: Number> Range<T> {
    /// A numeric range that checks only the lower bound.
    #[doc(alias = "org.apache.kafka.common.config.ConfigDef$Range#atLeast")]
    pub(crate) fn at_least(min: T) -> Self {
        Self { min, max: None }
    }

    /// A numeric range that checks both the upper (inclusive) and lower bound.
    #[doc(alias = "org.apache.kafka.common.config.ConfigDef$Range#between")]
    pub(crate) fn between(min: T, max: T) -> Self {
        Self { min, max: Some(max) }
    }

    /// Java's `Range.ensureValid(name, o)` (`ConfigDef.java:1008-1016`): both
    /// bounds compared as `doubleValue()`.
    ///
    /// # Errors
    ///
    /// [`Error::Config`] with Java's message: `Invalid value <value> for
    /// configuration <name>: Value must be at least <min>`, or `... Value must
    /// be no more than <max>`.
    #[doc(alias = "org.apache.kafka.common.config.ConfigDef$Range#ensureValid")]
    pub(crate) fn ensure_valid(&self, name: &str, value: T) -> Result<(), Error> {
        let n = value.double_value();
        if n < self.min.double_value() {
            return Err(Error::config_name_value_message(
                name,
                value.java_to_string(),
                format!("Value must be at least {}", self.min.java_to_string()),
            ));
        }
        if let Some(max) = self.max
            && n > max.double_value()
        {
            return Err(Error::config_name_value_message(
                name,
                value.java_to_string(),
                format!("Value must be no more than {}", max.java_to_string()),
            ));
        }
        Ok(())
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

    fn config_error(result: Result<impl std::fmt::Debug, Error>) -> String {
        match result {
            Err(Error::Config(e)) => e.message().to_string(),
            other => panic!("expected a ConfigError, got {other:?}"),
        }
    }

    /// The `Type.INT` case of `ConfigDefTest.testBasicTypes`: `"1   "` parses
    /// to `1` and passes its `Range.between(0, 14)`.
    #[test]
    #[doc(alias = "org.apache.kafka.common.config.ConfigDefTest#testBasicTypes")]
    fn test_basic_types() {
        let value = ConfigDef::parse_int("a", "1   ").unwrap();
        assert_eq!(value, 1);
        Range::between(0, 14).ensure_valid("a", value).unwrap();
        assert_eq!(ConfigDef::parse_long("b", " -2 ").unwrap(), -2);
    }

    /// The `String` inputs of `ConfigDefTest.testBadInputs` for `Type.INT`
    /// and `Type.LONG`, with the message `parseType` gives them: the original
    /// value, untrimmed. Its non-`String` inputs (`42.5`, `new Object()`) have
    /// no counterpart, since a property value here is always a string.
    #[test]
    #[doc(alias = "org.apache.kafka.common.config.ConfigDefTest#testBadInputs")]
    fn test_bad_inputs() {
        for value in ["hello", "42.5", &i64::MAX.to_string(), " x ", ""] {
            assert_eq!(
                config_error(ConfigDef::parse_int("name", value)),
                format!("Invalid value {value} for configuration name: Not a number of type INT")
            );
        }
        for value in ["hello", "42.5", &format!("{}00", i64::MAX)] {
            assert_eq!(
                config_error(ConfigDef::parse_long("name", value)),
                format!("Invalid value {value} for configuration name: Not a number of type LONG")
            );
        }
        assert_eq!(ConfigDef::parse_int("name", "+2147483647").unwrap(), i32::MAX);
        assert_eq!(ConfigDef::parse_long("name", &i64::MIN.to_string()).unwrap(), i64::MIN);
    }

    /// The `Range.between(0, 10)` case of `ConfigDefTest.testValidators`:
    /// `1`, `5` and `9` pass, `-1` and `11` fail. Its `null` input has no
    /// counterpart: a value parsed from a property is never null. Both bounds
    /// are inclusive.
    #[test]
    #[doc(alias = "org.apache.kafka.common.config.ConfigDefTest#testValidators")]
    fn test_validators() {
        let range = Range::between(0, 10);
        for ok in [0, 1, 5, 9, 10] {
            range.ensure_valid("name", ok).unwrap();
        }
        assert_eq!(
            config_error(range.ensure_valid("name", -1)),
            "Invalid value -1 for configuration name: Value must be at least 0"
        );
        assert_eq!(
            config_error(range.ensure_valid("name", 11)),
            "Invalid value 11 for configuration name: Value must be no more than 10"
        );
    }

    /// `Range.atLeast` checks the lower bound only, inclusively, for `int`
    /// and `long` values alike.
    #[test]
    fn test_range_at_least() {
        Range::at_least(0).ensure_valid("name", 0).unwrap();
        Range::at_least(0).ensure_valid("name", i32::MAX).unwrap();
        Range::at_least(-1_i64).ensure_valid("name", -1).unwrap();
        Range::at_least(0_i64).ensure_valid("name", i64::MAX).unwrap();
        assert_eq!(
            config_error(Range::at_least(0).ensure_valid("name", i32::MIN)),
            "Invalid value -2147483648 for configuration name: Value must be at least 0"
        );
        assert_eq!(
            config_error(Range::at_least(-1_i64).ensure_valid("name", -2)),
            "Invalid value -2 for configuration name: Value must be at least -1"
        );
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
