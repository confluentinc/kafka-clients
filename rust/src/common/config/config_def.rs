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

    /// The `Type.SHORT` arm of Java's `ConfigDef.parseType` for a `String`
    /// value: `Short.parseShort(trimmed)` (`ConfigDef.java:744-750`), so a
    /// value outside `-32768..=32767` is not a number either.
    ///
    /// # Errors
    ///
    /// As [`Self::parse_int`], with `Not a number of type SHORT`.
    pub(crate) fn parse_short(name: &str, value: &str) -> Result<i16, Error> {
        Self::trim(value)
            .parse::<i16>()
            .map_err(|_| Self::not_a_number(name, value, "SHORT"))
    }

    /// The `Type.DOUBLE` arm of Java's `ConfigDef.parseType` for a `String`
    /// value: `Double.parseDouble(trimmed)` (`ConfigDef.java:761-767`).
    ///
    /// Java's grammar is accepted: an optional sign, then `NaN`, `Infinity`,
    /// or a decimal literal with an optional exponent and an optional `f`,
    /// `F`, `d` or `D` suffix. Rust's own `f64` parser also accepts `inf`,
    /// `nan` and `infinity` in any case, which Java rejects, so those are
    /// rejected here. Java's hexadecimal literals (`0x1p3`) are not accepted.
    ///
    /// # Errors
    ///
    /// As [`Self::parse_int`], with `Not a number of type DOUBLE`.
    pub(crate) fn parse_double(name: &str, value: &str) -> Result<f64, Error> {
        Self::parse_java_double(Self::trim(value)).ok_or_else(|| Self::not_a_number(name, value, "DOUBLE"))
    }

    /// `Double.parseDouble` of a trimmed string, without hexadecimal literals.
    fn parse_java_double(s: &str) -> Option<f64> {
        let (negative, unsigned) = match s.as_bytes().first() {
            Some(b'-') => (true, &s[1..]),
            Some(b'+') => (false, &s[1..]),
            _ => (false, s),
        };
        let magnitude = match unsigned {
            "NaN" => f64::NAN,
            "Infinity" => f64::INFINITY,
            _ => {
                let literal = unsigned.strip_suffix(['f', 'F', 'd', 'D']).unwrap_or(unsigned);
                // Digits, a point and an exponent only, so no second sign in
                // front and none of Rust's `inf` / `nan` spellings.
                let well_formed = literal.starts_with(|c: char| c.is_ascii_digit() || c == '.')
                    && literal
                        .bytes()
                        .all(|b| b.is_ascii_digit() || matches!(b, b'.' | b'e' | b'E' | b'+' | b'-'));
                if !well_formed {
                    return None;
                }
                literal.parse::<f64>().ok()?
            },
        };
        Some(if negative { -magnitude } else { magnitude })
    }

    /// The `Type.BOOLEAN` arm of Java's `ConfigDef.parseType` for a `String`
    /// value (`ConfigDef.java:712-723`): the trimmed value, compared with
    /// `"true"` and `"false"` ignoring case.
    ///
    /// # Errors
    ///
    /// [`Error::Config`] for any other value: `Invalid value <value> for
    /// configuration <name>: Expected value to be either true or false`, with
    /// the original, untrimmed value.
    pub(crate) fn parse_boolean(name: &str, value: &str) -> Result<bool, Error> {
        let trimmed = Self::trim(value);
        if trimmed.eq_ignore_ascii_case("true") {
            Ok(true)
        } else if trimmed.eq_ignore_ascii_case("false") {
            Ok(false)
        } else {
            Err(Error::config_name_value_message(
                name,
                value,
                "Expected value to be either true or false",
            ))
        }
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

impl Number for i16 {
    fn double_value(self) -> f64 {
        f64::from(self)
    }

    fn java_to_string(self) -> String {
        self.to_string()
    }
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

impl Number for f64 {
    fn double_value(self) -> f64 {
        self
    }

    fn java_to_string(self) -> String {
        java_double_to_string(self)
    }
}

/// Java's `Double.toString(d)`, which a `Range` message prints a `DOUBLE`
/// value and bounds with: `2.0` where Rust's `Display` prints `2`.
///
/// `NaN`, `Infinity` and `-Infinity` are spelled out; zero is `0.0` or
/// `-0.0`. A magnitude in `[10^-3, 10^7)` is printed as a decimal with at
/// least one digit after the point, any other in computerized scientific
/// notation, `<digit>.<digits>E<exponent>`, again with at least one fraction
/// digit. The digits are the shortest that identify the value, as since JDK
/// 19; Rust's formatter chooses the same digits.
fn java_double_to_string(d: f64) -> String {
    if d.is_nan() {
        return "NaN".to_string();
    }
    if d.is_infinite() {
        return if d > 0.0 { "Infinity" } else { "-Infinity" }.to_string();
    }
    if d == 0.0 {
        return if d.is_sign_negative() { "-0.0" } else { "0.0" }.to_string();
    }
    let sign = if d < 0.0 { "-" } else { "" };
    let magnitude = d.abs();
    // `{:e}` prints the shortest digits as `<digit>[.<digits>]e<exponent>`.
    let scientific = format!("{magnitude:e}");
    let (mantissa, exponent) = scientific.split_once('e').unwrap_or((&scientific, "0"));
    let exponent: i32 = exponent.parse().unwrap_or(0);
    let digits: String = mantissa.chars().filter(char::is_ascii_digit).collect();
    if (1e-3..1e7).contains(&magnitude) {
        if exponent >= 0 {
            // At most 7 integer digits in this range.
            let integer_len = exponent.unsigned_abs() as usize + 1;
            if digits.len() <= integer_len {
                format!("{sign}{digits}{}.0", "0".repeat(integer_len - digits.len()))
            } else {
                format!("{sign}{}.{}", &digits[..integer_len], &digits[integer_len..])
            }
        } else {
            let leading_zeros = exponent.unsigned_abs() as usize - 1;
            format!("{sign}0.{}{digits}", "0".repeat(leading_zeros))
        }
    } else {
        let fraction = if digits.len() > 1 { &digits[1..] } else { "0" };
        format!("{sign}{}.{fraction}E{exponent}", &digits[..1])
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

    /// The `Type.INT` and `Type.BOOLEAN` cases of `ConfigDefTest.testBasicTypes`:
    /// `"1   "` parses to `1` and passes its `Range.between(0, 14)`, and
    /// `"true"`, `"FalSE"` and `"TRUE"` are booleans. Its `Type.DOUBLE` value is
    /// a `Double` object (`42.5d`); a property here is a string, so `"42.5"`
    /// stands in for it.
    #[test]
    #[doc(alias = "org.apache.kafka.common.config.ConfigDefTest#testBasicTypes")]
    fn test_basic_types() {
        let value = ConfigDef::parse_int("a", "1   ").unwrap();
        assert_eq!(value, 1);
        Range::between(0, 14).ensure_valid("a", value).unwrap();
        assert_eq!(ConfigDef::parse_long("b", " -2 ").unwrap(), -2);
        assert_eq!(ConfigDef::parse_double("e", "42.5").unwrap(), 42.5);
        assert!(ConfigDef::parse_boolean("g", "true").unwrap());
        assert!(!ConfigDef::parse_boolean("h", "FalSE").unwrap());
        assert!(ConfigDef::parse_boolean("i", "TRUE").unwrap());
    }

    /// The `String` inputs of `ConfigDefTest.testBadInputs` for `Type.INT`,
    /// `Type.LONG`, `Type.DOUBLE` and `Type.BOOLEAN`, with the message
    /// `parseType` gives them: the original value, untrimmed. Its non-`String`
    /// inputs (`42.5`, `new Object()`) have no counterpart, since a property
    /// value here is always a string.
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
        assert_eq!(
            config_error(ConfigDef::parse_double("name", "hello")),
            "Invalid value hello for configuration name: Not a number of type DOUBLE"
        );
        for value in ["hello", "truee", "fals", " yes "] {
            assert_eq!(
                config_error(ConfigDef::parse_boolean("name", value)),
                format!("Invalid value {value} for configuration name: Expected value to be either true or false")
            );
        }
    }

    /// `Short.parseShort` rejects a value outside the `short` range as not a
    /// number.
    #[test]
    fn test_parse_short() {
        assert_eq!(ConfigDef::parse_short("name", " -32768 ").unwrap(), i16::MIN);
        assert_eq!(ConfigDef::parse_short("name", "900").unwrap(), 900);
        for value in ["40000", "-32769", "1.5", "x"] {
            assert_eq!(
                config_error(ConfigDef::parse_short("name", value)),
                format!("Invalid value {value} for configuration name: Not a number of type SHORT")
            );
        }
    }

    /// `Double.parseDouble`'s grammar: a sign, then `NaN`, `Infinity` or a
    /// decimal literal with an optional exponent and type suffix. Rust's own
    /// spellings of infinity and NaN, a second sign and a lone point are not
    /// numbers.
    #[test]
    fn test_parse_double() {
        for (value, expected) in [
            ("1.5", 1.5),
            (" 0.8 ", 0.8),
            ("-0.25", -0.25),
            ("+2", 2.0),
            ("5.", 5.0),
            (".5", 0.5),
            ("1e3", 1000.0),
            ("1.5E-2", 0.015),
            ("1e+2", 100.0),
            ("1.5d", 1.5),
            ("2F", 2.0),
            ("1e400", f64::INFINITY),
            ("Infinity", f64::INFINITY),
            ("-Infinity", f64::NEG_INFINITY),
        ] {
            assert_eq!(ConfigDef::parse_double("name", value).unwrap(), expected, "{value:?}");
        }
        assert!(ConfigDef::parse_double("name", "NaN").unwrap().is_nan());
        for value in [
            "", ".", "inf", "INF", "nan", "infinity", "+-1", "--1", "1e", "e5", "1.2.3", "1ff", "NaNd",
        ] {
            assert_eq!(
                config_error(ConfigDef::parse_double("name", value)),
                format!("Invalid value {value} for configuration name: Not a number of type DOUBLE"),
                "{value:?}"
            );
        }
        // Not Java's behaviour: Java reads a hexadecimal literal (`0x1p3` is
        // 8.0), which this parser does not (see `ConfigDef::parse_double`).
        assert_eq!(
            config_error(ConfigDef::parse_double("name", "0x1p3")),
            "Invalid value 0x1p3 for configuration name: Not a number of type DOUBLE"
        );
    }

    /// Java's `Double.toString`: always a fraction digit, decimal notation in
    /// `[10^-3, 10^7)`, computerized scientific notation outside it.
    #[test]
    fn test_java_double_to_string() {
        for (value, expected) in [
            (1.0, "1.0"),
            (2.0, "2.0"),
            (0.5, "0.5"),
            (0.25, "0.25"),
            (0.0, "0.0"),
            (-0.0, "-0.0"),
            (-0.1, "-0.1"),
            (100.0, "100.0"),
            (123.456, "123.456"),
            (0.001, "0.001"),
            (1234567.0, "1234567.0"),
            (1e7, "1.0E7"),
            (1.5e7, "1.5E7"),
            (1e-4, "1.0E-4"),
            (-2.5e-5, "-2.5E-5"),
            (1e23, "1.0E23"),
            (f64::INFINITY, "Infinity"),
            (f64::NEG_INFINITY, "-Infinity"),
            (f64::NAN, "NaN"),
        ] {
            assert_eq!(java_double_to_string(value), expected, "{value:?}");
        }
    }

    /// `Range` over `double` and `short` values prints them as Java does:
    /// `Range.between(0.5, 1.0)` rejects `2.0` with `Value must be no more than
    /// 1.0`. `NaN` passes, since neither comparison holds for it, as in Java.
    #[test]
    fn test_range_of_doubles_and_shorts() {
        let factor = Range::between(0.5, 1.0);
        factor.ensure_valid("f", 0.5).unwrap();
        factor.ensure_valid("f", 1.0).unwrap();
        factor.ensure_valid("f", f64::NAN).unwrap();
        assert_eq!(
            config_error(factor.ensure_valid("f", 2.0)),
            "Invalid value 2.0 for configuration f: Value must be no more than 1.0"
        );
        assert_eq!(
            config_error(factor.ensure_valid("f", 0.4)),
            "Invalid value 0.4 for configuration f: Value must be at least 0.5"
        );
        assert_eq!(
            config_error(Range::between(0.0, 0.25).ensure_valid("j", -0.1)),
            "Invalid value -0.1 for configuration j: Value must be at least 0.0"
        );
        assert_eq!(
            config_error(Range::between(0_i16, 900).ensure_valid("s", 901)),
            "Invalid value 901 for configuration s: Value must be no more than 900"
        );
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
