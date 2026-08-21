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

//! Auto offset reset strategy used by consumers.
//!
//! Translated from
//! `org.apache.kafka.clients.consumer.internals.AutoOffsetResetStrategy`.

use std::fmt;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::common::Error;
use crate::common::kafka_error::LocalIllegalArgumentError;

/// `ListOffsetsRequest.EARLIEST_TIMESTAMP` — the sentinel passed to the
/// broker to request the earliest available offset.
///
/// NOTE: a later phase translating `ListOffsetsRequest` will replace this
/// constant with a re-export from there. The numeric value is fixed by the
/// Kafka protocol and will not change.
pub const EARLIEST_TIMESTAMP: i64 = -2;

/// `ListOffsetsRequest.LATEST_TIMESTAMP` — the sentinel passed to the broker
/// to request the latest available offset.
///
/// NOTE: see [`EARLIEST_TIMESTAMP`].
pub const LATEST_TIMESTAMP: i64 = -1;

/// The kind of auto-offset-reset strategy.
///
/// Corresponds to Java's nested `AutoOffsetResetStrategy.StrategyType` enum.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum StrategyType {
    /// Reset to the latest available offset.
    Latest,
    /// Reset to the earliest available offset.
    Earliest,
    /// Do not reset; raise an error if no offset is found.
    ///
    /// Suffixed with `_` to avoid clash with `Option::None` in pattern matches.
    None_,
    /// Reset to a configured duration before the current timestamp.
    ByDuration,
}

impl fmt::Display for StrategyType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Latest => "latest",
            Self::Earliest => "earliest",
            Self::None_ => "none",
            Self::ByDuration => "by_duration",
        })
    }
}

/// Auto offset reset strategy used by the consumer.
///
/// Corresponds to Java's
/// `org.apache.kafka.clients.consumer.internals.AutoOffsetResetStrategy`.
#[derive(Clone, Debug)]
pub struct AutoOffsetResetStrategy {
    strategy_type: StrategyType,
    duration: Option<Duration>,
}

impl AutoOffsetResetStrategy {
    /// The "earliest" strategy.
    pub const EARLIEST: Self = AutoOffsetResetStrategy { strategy_type: StrategyType::Earliest, duration: None };

    /// The "latest" strategy.
    pub const LATEST: Self = AutoOffsetResetStrategy { strategy_type: StrategyType::Latest, duration: None };

    /// The "none" strategy.
    pub const NONE: Self = AutoOffsetResetStrategy { strategy_type: StrategyType::None_, duration: None };

    /// Returns the auto-offset-reset strategy from the given string.
    ///
    /// Accepts (case-sensitive, matching Java's `enumOptions` comparison):
    /// - `"earliest"`
    /// - `"latest"`
    /// - `"none"`
    /// - `"by_duration:<ISO-8601-duration>"` (e.g. `by_duration:PT1H`)
    ///
    /// # Errors
    ///
    /// Returns [`Error::LocalIllegalArgument`] if the input does not match
    /// one of the accepted forms or if the ISO-8601 duration cannot be parsed
    /// or is negative.
    pub fn from_string(s: &str) -> Result<Self, Error> {
        if s == "by_duration" {
            return Err(Error::local_illegal_argument(
                "<:duration> part is missing in by_duration auto offset reset strategy.",
            ));
        }
        match s {
            "earliest" => return Ok(Self::EARLIEST),
            "latest" => return Ok(Self::LATEST),
            "none" => return Ok(Self::NONE),
            _ => {},
        }
        if let Some(iso) = s.strip_prefix("by_duration:") {
            // Java (`AutoOffsetResetStrategy.java:87-96`) wraps whatever the
            // `try` block raised as the CAUSE of its own
            // `IllegalArgumentException`, and that cause is never null: it is
            // either `Duration.parse`'s failure or the
            // `IllegalArgumentException("Negative duration is not supported in
            // by_duration offset reset strategy.")` thrown at `:90` and
            // re-caught by the same broad `catch (Exception e)`. Without the
            // cause the two are indistinguishable to a caller, since the outer
            // message is identical for both.
            let duration = parse_iso8601_duration(iso).map_err(|cause| {
                Error::LocalIllegalArgument(LocalIllegalArgumentError::with_source(
                    "Unable to parse duration string in by_duration offset reset strategy.",
                    cause,
                ))
            })?;
            return Ok(Self { strategy_type: StrategyType::ByDuration, duration: Some(duration) });
        }
        Err(Error::local_illegal_argument(format!(
            "Unknown auto offset reset strategy: {s}"
        )))
    }

    /// Java: `AutoOffsetResetStrategy.Validator.ensureValid(String name, Object value)`
    /// (`AutoOffsetResetStrategy.java:158-168`).
    ///
    /// ```java
    /// public void ensureValid(String name, Object value) {
    ///     String offsetStrategy = (String) value;
    ///     try {
    ///         fromString(offsetStrategy);
    ///     } catch (Exception e) {
    ///         throw new ConfigException(name, value, "Invalid value `" + offsetStrategy +
    ///             "` for configuration " + name + ". The value must be either 'earliest', " +
    ///             "'latest', 'none' or of the format 'by_duration:<PnDTnHnMn.nS.>'.");
    ///     }
    /// }
    /// ```
    ///
    /// The whole point of the catch is to REPLACE whatever
    /// [`Self::from_string`] reports with a message that names the config key
    /// and lists the legal values — so this deliberately discards the inner
    /// error rather than propagating it. The result is a
    /// `ConfigException`-equivalent ([`Error::Config`]), which unlike
    /// [`Error::LocalIllegalArgument`] sits inside the `KafkaException` hierarchy.
    pub fn ensure_valid(name: &str, value: &str) -> Result<(), Error> {
        match Self::from_string(value) {
            Ok(_) => Ok(()),
            // Java's `catch (Exception e)` drops `e` entirely.
            Err(_) => Err(Error::config_value_message(
                name,
                value,
                format!(
                    "Invalid value `{value}` for configuration {name}. The value must be either \
                     'earliest', 'latest', 'none' or of the format 'by_duration:<PnDTnHnMn.nS.>'."
                ),
            )),
        }
    }

    /// Returns the offset reset strategy type.
    pub fn type_(&self) -> StrategyType {
        self.strategy_type
    }

    /// Returns the name of the offset reset strategy.
    ///
    /// Matches Java's `name()` which returns `type.toString()` (lower-case
    /// strategy-type identifier such as `"by_duration"`).
    pub fn name(&self) -> String {
        self.strategy_type.to_string()
    }

    /// Return the timestamp to be used for a `ListOffsetsRequest`.
    ///
    /// - [`StrategyType::Earliest`] → `EARLIEST_TIMESTAMP` (`-2`)
    /// - [`StrategyType::Latest`] → `LATEST_TIMESTAMP` (`-1`)
    /// - [`StrategyType::ByDuration`] → `now - duration` in milliseconds
    /// - [`StrategyType::None_`] → `None`
    pub fn timestamp(&self) -> Option<i64> {
        match self.strategy_type {
            StrategyType::Earliest => Some(EARLIEST_TIMESTAMP),
            StrategyType::Latest => Some(LATEST_TIMESTAMP),
            StrategyType::ByDuration => {
                let now_millis = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .ok()
                    .map(|d| d.as_millis() as i64)?;
                let dur = self.duration?;
                let dur_millis = dur.as_millis() as i64;
                Some(now_millis.saturating_sub(dur_millis))
            },
            StrategyType::None_ => None,
        }
    }

    /// The configured duration for `by_duration` strategies; `None` for all
    /// other strategies.
    pub fn duration(&self) -> Option<Duration> {
        self.duration
    }
}

impl PartialEq for AutoOffsetResetStrategy {
    fn eq(&self, other: &Self) -> bool {
        self.strategy_type == other.strategy_type && self.duration == other.duration
    }
}

impl Eq for AutoOffsetResetStrategy {}

impl std::hash::Hash for AutoOffsetResetStrategy {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.strategy_type.hash(state);
        self.duration.hash(state);
    }
}

impl fmt::Display for AutoOffsetResetStrategy {
    /// Matches Java's `toString()` format:
    /// `AutoOffsetResetStrategy{type=earliest}` or
    /// `AutoOffsetResetStrategy{type=by_duration, duration=PT1H}`
    ///
    /// We render the duration using ISO-8601 form (`PT…`) to match Java's
    /// `Duration.toString()` style as closely as is reasonable.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.duration {
            Some(d) => {
                write!(
                    f,
                    "AutoOffsetResetStrategy{{type={}, duration={}}}",
                    self.strategy_type,
                    format_iso8601_duration(d)
                )
            },
            None => write!(f, "AutoOffsetResetStrategy{{type={}}}", self.strategy_type),
        }
    }
}

/// Java's `IllegalArgumentException("Negative duration is not supported in
/// by_duration offset reset strategy.")` (`AutoOffsetResetStrategy.java:90`),
/// which `fromString`'s own `catch (Exception e)` immediately re-catches and
/// carries as the cause of its outer error.
const NEGATIVE_DURATION_MESSAGE: &str = "Negative duration is not supported in by_duration offset reset strategy.";

/// Stands in for the `java.time.format.DateTimeParseException` that
/// `Duration.parse` raises on a malformed duration, carrying that class's own
/// message. `DateTimeParseException` has no Rust counterpart in
/// [`Error`] — it is a `java.time` `RuntimeException`, outside both the
/// `KafkaException` hierarchy and the set of `java.lang` runtime errors the
/// enum models — so [`Error::local_illegal_argument`] carries it. That is a deviation
/// in the cause's *class* only; the distinction the cause exists to preserve
/// (malformed input vs. a negative duration) is in the message, and the outer
/// error the caller returns is the same class as Java's either way.
fn parse_failure() -> Error {
    Error::local_illegal_argument("Text cannot be parsed to a Duration")
}

/// Parse a subset of ISO-8601 durations: `PnDTnHnMn(.fS)?`.
///
/// Negative durations are explicitly rejected, matching Java's check after
/// `Duration.parse`. Fractional seconds are supported.
///
/// # Errors
///
/// Returns the [`Error::LocalIllegalArgument`] that Java's `try` block raises, which
/// the caller then carries as the `source` of its own error: either
/// [`NEGATIVE_DURATION_MESSAGE`] for a negative duration
/// (`AutoOffsetResetStrategy.java:90`) or [`parse_failure`]'s message standing
/// in for `Duration.parse`'s `DateTimeParseException`.
fn parse_iso8601_duration(input: &str) -> Result<Duration, Error> {
    // Reject negative durations explicitly (Java rejects them via
    // `duration.isNegative()` at `AutoOffsetResetStrategy.java:89-91`); a
    // leading '-' would otherwise be accepted by `Duration.parse`. Java's
    // message for this branch is reproduced verbatim because it is the one
    // thing that tells a negative value apart from a malformed one once both
    // are wrapped by the caller's identical outer message.
    if input.starts_with('-') {
        return Err(Error::local_illegal_argument(NEGATIVE_DURATION_MESSAGE));
    }
    let rest = input.strip_prefix('P').ok_or_else(parse_failure)?;
    if rest.is_empty() {
        return Err(parse_failure());
    }

    // Split into the date-part (before 'T') and the time-part (after 'T').
    let (date_part, time_part) = match rest.split_once('T') {
        Some((d, t)) => (d, Some(t)),
        None => (rest, None),
    };
    if date_part.is_empty() && time_part.is_none() {
        return Err(parse_failure());
    }
    if let Some(t) = time_part
        && t.is_empty()
    {
        return Err(parse_failure());
    }

    let mut total_secs: u64 = 0;
    let mut nanos: u32 = 0;

    // ---- date part: only days are accepted (P<n>D) — Kafka's spec does not
    // require years/months/weeks and Java's `Duration.parse` likewise rejects
    // those.
    if !date_part.is_empty() {
        // Expect <integer>D
        let days_str = date_part.strip_suffix('D').ok_or_else(parse_failure)?;
        let days: u64 = days_str.parse().map_err(|_| parse_failure())?;
        total_secs = total_secs
            .checked_add(days.checked_mul(86_400).ok_or_else(parse_failure)?)
            .ok_or_else(parse_failure)?;
    }

    // ---- time part: H, M, (.S)? in that order ----
    if let Some(t) = time_part {
        let mut buf = t;
        // Hours
        if let Some(idx) = buf.find('H') {
            let (head, tail) = buf.split_at(idx);
            let hours: u64 = head.parse().map_err(|_| parse_failure())?;
            total_secs = total_secs
                .checked_add(hours.checked_mul(3_600).ok_or_else(parse_failure)?)
                .ok_or_else(parse_failure)?;
            buf = &tail[1..]; // skip 'H'
        }
        // Minutes
        if let Some(idx) = buf.find('M') {
            let (head, tail) = buf.split_at(idx);
            let minutes: u64 = head.parse().map_err(|_| parse_failure())?;
            total_secs = total_secs
                .checked_add(minutes.checked_mul(60).ok_or_else(parse_failure)?)
                .ok_or_else(parse_failure)?;
            buf = &tail[1..]; // skip 'M'
        }
        // Seconds (optionally fractional)
        if let Some(idx) = buf.find('S') {
            let (head, tail) = buf.split_at(idx);
            if !tail.is_empty() && !tail[1..].is_empty() {
                // Anything after the 'S' is invalid.
                return Err(parse_failure());
            }
            // Parse seconds, possibly with fractional component.
            let (int_part, frac_part) = match head.split_once('.') {
                Some((i, f)) => (i, Some(f)),
                None => (head, None),
            };
            let secs: u64 = int_part.parse().map_err(|_| parse_failure())?;
            total_secs = total_secs.checked_add(secs).ok_or_else(parse_failure)?;
            if let Some(f) = frac_part {
                if f.is_empty() || f.len() > 9 {
                    return Err(parse_failure());
                }
                // Pad/truncate to 9 digits to convert to nanoseconds.
                let mut padded = String::with_capacity(9);
                padded.push_str(f);
                while padded.len() < 9 {
                    padded.push('0');
                }
                nanos = padded.parse().map_err(|_| parse_failure())?;
            }
            buf = "";
        }
        if !buf.is_empty() {
            return Err(parse_failure());
        }
    }

    Ok(Duration::new(total_secs, nanos))
}

/// Render a duration as `PT…` (hours/minutes/seconds) form.
///
/// We do not currently emit a `D` component because the strategies users
/// pass typically use sub-day precision and Java's `Duration.toString()` for
/// `Duration.ofHours(1)` is `PT1H` (no `P0D`). For multi-day durations the
/// output collapses everything into hours, which is still a valid ISO-8601
/// representation and matches what `Duration.toString()` does for durations
/// without separate day overflow.
fn format_iso8601_duration(d: Duration) -> String {
    let total_secs = d.as_secs();
    let nanos = d.subsec_nanos();
    let hours = total_secs / 3_600;
    let minutes = (total_secs % 3_600) / 60;
    let seconds = total_secs % 60;

    let mut out = String::from("PT");
    let mut wrote_any = false;
    if hours > 0 {
        out.push_str(&format!("{hours}H"));
        wrote_any = true;
    }
    if minutes > 0 {
        out.push_str(&format!("{minutes}M"));
        wrote_any = true;
    }
    if seconds > 0 || nanos > 0 || !wrote_any {
        if nanos == 0 {
            out.push_str(&format!("{seconds}S"));
        } else {
            // strip trailing zeros from nanoseconds, like Java does
            let nanos_str = format!("{:09}", nanos);
            let trimmed = nanos_str.trim_end_matches('0');
            out.push_str(&format!("{seconds}.{trimmed}S"));
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_iso8601_basic() {
        assert_eq!(parse_iso8601_duration("PT1H").unwrap(), Duration::from_secs(3600));
        assert_eq!(
            parse_iso8601_duration("PT2H30M").unwrap(),
            Duration::from_secs(2 * 3600 + 30 * 60)
        );
        assert_eq!(parse_iso8601_duration("P2D").unwrap(), Duration::from_secs(2 * 86_400));
        assert_eq!(parse_iso8601_duration("P1DT1H").unwrap(), Duration::from_secs(86_400 + 3600));
        assert_eq!(parse_iso8601_duration("PT1S").unwrap(), Duration::from_secs(1));
        assert_eq!(parse_iso8601_duration("PT0.5S").unwrap(), Duration::from_millis(500));
    }

    #[test]
    fn test_parse_iso8601_negative_rejected() {
        assert!(parse_iso8601_duration("-PT1H").is_err());
    }

    #[test]
    fn test_parse_iso8601_invalid() {
        assert!(parse_iso8601_duration("").is_err());
        assert!(parse_iso8601_duration("P").is_err());
        assert!(parse_iso8601_duration("PT").is_err());
        assert!(parse_iso8601_duration("foo").is_err());
        assert!(parse_iso8601_duration("PT1Xfoo").is_err());
    }

    #[test]
    fn test_from_string_simple() {
        assert_eq!(
            AutoOffsetResetStrategy::from_string("earliest").unwrap(),
            AutoOffsetResetStrategy::EARLIEST
        );
        assert_eq!(
            AutoOffsetResetStrategy::from_string("latest").unwrap(),
            AutoOffsetResetStrategy::LATEST
        );
        assert_eq!(
            AutoOffsetResetStrategy::from_string("none").unwrap(),
            AutoOffsetResetStrategy::NONE
        );
    }

    #[test]
    fn test_from_string_case_sensitive() {
        // Java's contains-check on enumOptions makes this case-sensitive.
        assert!(AutoOffsetResetStrategy::from_string("EARLIEST").is_err());
        assert!(AutoOffsetResetStrategy::from_string("LATEST").is_err());
        assert!(AutoOffsetResetStrategy::from_string("NONE").is_err());
    }

    #[test]
    fn test_from_string_by_duration_ok() {
        let s = AutoOffsetResetStrategy::from_string("by_duration:PT1H").unwrap();
        assert_eq!(s.type_(), StrategyType::ByDuration);
        assert_eq!(s.duration(), Some(Duration::from_secs(3600)));
        assert_eq!(s.name(), "by_duration");
    }

    #[test]
    fn test_from_string_by_duration_missing_part() {
        let err = AutoOffsetResetStrategy::from_string("by_duration").unwrap_err();
        assert!(err.message().contains("<:duration> part is missing"));
    }

    #[test]
    fn test_from_string_by_duration_empty_iso() {
        assert!(AutoOffsetResetStrategy::from_string("by_duration:").is_err());
    }

    #[test]
    fn test_from_string_by_duration_negative() {
        let err = AutoOffsetResetStrategy::from_string("by_duration:-PT1H").unwrap_err();
        assert!(err.message().contains("Unable to parse duration string"));
    }

    #[test]
    fn test_from_string_invalid() {
        let err = AutoOffsetResetStrategy::from_string("invalid").unwrap_err();
        assert!(err.message().contains("Unknown auto offset reset strategy"));
    }

    /// Translated from `AutoOffsetResetStrategyTest.testValidator`.
    ///
    /// The validator's whole purpose is to REPLACE `from_string`'s message with
    /// one that names the config key and lists the legal values, and to report
    /// it as a `ConfigException` (inside the `KafkaException` hierarchy) rather
    /// than an `IllegalArgumentException` (outside it).
    #[test]
    fn test_validator_ensure_valid() {
        // Every accepted form passes.
        for value in ["earliest", "latest", "none", "by_duration:PT1H", "by_duration:P2DT3H4M"] {
            AutoOffsetResetStrategy::ensure_valid("auto.offset.reset", value)
                .unwrap_or_else(|e| panic!("{value} must validate, got: {e}"));
        }

        // Every rejected form yields Java's ConfigException message verbatim.
        for value in [
            "",
            "invalid",
            "by_duration",
            "by_duration:",
            "by_duration:-PT1H",
            "earlist",
        ] {
            let err = AutoOffsetResetStrategy::ensure_valid("auto.offset.reset", value).unwrap_err_or_else_panic(value);

            // Java's `ConfigException(name, value, message)` renders as
            // "Invalid value <value> for configuration <name>: <message>".
            let expected = format!(
                "Invalid value {value} for configuration auto.offset.reset: Invalid value `{value}` \
                 for configuration auto.offset.reset. The value must be either 'earliest', \
                 'latest', 'none' or of the format 'by_duration:<PnDTnHnMn.nS.>'."
            );
            assert_eq!(expected, err.message(), "message mismatch for {value:?}");

            // Class: `ConfigException extends KafkaException`, so unlike the
            // `IllegalArgumentException` that `from_string` raises, this IS a
            // Kafka error.
            assert!(matches!(err, Error::Config(_)), "must be a config error for {value:?}: {err:?}");
            assert!(err.is_kafka_error(), "a config error is a Kafka error: {err:?}");
        }
    }

    /// Small helper so the loop above reads cleanly.
    trait UnwrapErrOrPanic {
        fn unwrap_err_or_else_panic(self, value: &str) -> Error;
    }
    impl UnwrapErrOrPanic for Result<(), Error> {
        fn unwrap_err_or_else_panic(self, value: &str) -> Error {
            match self {
                Ok(()) => panic!("{value:?} must be rejected by the validator"),
                Err(e) => e,
            }
        }
    }

    /// The validator is wired into `ConsumerConfig::from_properties`, which is
    /// Java's `ConfigDef` validation point — so a bad `auto.offset.reset` is
    /// rejected at config construction with the `ConfigException`, not later.
    #[test]
    fn test_consumer_config_rejects_invalid_auto_offset_reset() {
        use std::collections::HashMap;

        use crate::consumer::consumer_config::ConsumerConfig;

        let props = HashMap::from([
            ("bootstrap.servers".to_string(), "localhost:9092".to_string()),
            ("auto.offset.reset".to_string(), "bogus".to_string()),
        ]);
        let err = ConsumerConfig::from_properties(&props).expect_err("bogus strategy must be rejected");
        assert!(matches!(err, Error::Config(_)), "must be a config error: {err:?}");
        assert_eq!(
            "Invalid value bogus for configuration auto.offset.reset: Invalid value `bogus` for \
             configuration auto.offset.reset. The value must be either 'earliest', 'latest', 'none' \
             or of the format 'by_duration:<PnDTnHnMn.nS.>'.",
            err.message()
        );
    }

    #[test]
    fn test_from_string_empty() {
        assert!(AutoOffsetResetStrategy::from_string("").is_err());
    }

    #[test]
    fn test_timestamp_for_each_strategy() {
        assert_eq!(AutoOffsetResetStrategy::EARLIEST.timestamp(), Some(EARLIEST_TIMESTAMP));
        assert_eq!(AutoOffsetResetStrategy::LATEST.timestamp(), Some(LATEST_TIMESTAMP));
        assert_eq!(AutoOffsetResetStrategy::NONE.timestamp(), None);

        let s = AutoOffsetResetStrategy::from_string("by_duration:PT1H").unwrap();
        let now_millis = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as i64;
        let ts = s.timestamp().unwrap();
        // Should be approximately now - 1h. Allow a generous 5-second window.
        assert!(
            ts <= now_millis - 3_600_000 + 5_000 && ts >= now_millis - 3_600_000 - 5_000,
            "timestamp {ts} not within expected window of now-1h ({})",
            now_millis - 3_600_000
        );
    }

    #[test]
    fn test_equality_and_hash() {
        use std::collections::HashSet;
        let a = AutoOffsetResetStrategy::from_string("earliest").unwrap();
        let b = AutoOffsetResetStrategy::from_string("earliest").unwrap();
        let c = AutoOffsetResetStrategy::from_string("latest").unwrap();
        let d1 = AutoOffsetResetStrategy::from_string("by_duration:P2D").unwrap();
        let d2 = AutoOffsetResetStrategy::from_string("by_duration:P2D").unwrap();

        assert_eq!(a, b);
        assert_ne!(a, c);
        assert_eq!(d1, d2);
        assert_ne!(c, d1);

        let mut set = HashSet::new();
        set.insert(a.clone());
        set.insert(b);
        assert_eq!(set.len(), 1);
        set.insert(c);
        set.insert(d1);
        assert_eq!(set.len(), 3);
    }

    /// Java's `catch (Exception e)` wraps the failure as the CAUSE of its own
    /// `IllegalArgumentException` (`AutoOffsetResetStrategy.java:93-95`), and
    /// that cause is never null. Both branches share the same outer message, so
    /// the cause is the only thing that tells a malformed duration apart from a
    /// negative one.
    #[test]
    fn test_by_duration_parse_failure_carries_the_cause() {
        let err = AutoOffsetResetStrategy::from_string("by_duration:not-a-duration").unwrap_err();
        assert!(matches!(err, Error::LocalIllegalArgument(_)), "{err:?}");
        assert_eq!(
            "Unable to parse duration string in by_duration offset reset strategy.",
            err.message()
        );
        let cause = err.source().expect("Java's cause is never null on this path");
        assert_eq!("Text cannot be parsed to a Duration", cause.message());
    }

    /// The negative-duration branch: Java throws
    /// `IllegalArgumentException("Negative duration is not supported in
    /// by_duration offset reset strategy.")` at `:90` and its own broad
    /// `catch` re-wraps it, so the outer message is identical to the malformed
    /// case and only the cause distinguishes them.
    #[test]
    fn test_by_duration_negative_carries_the_negative_cause() {
        let err = AutoOffsetResetStrategy::from_string("by_duration:-PT1H").unwrap_err();
        assert_eq!(
            "Unable to parse duration string in by_duration offset reset strategy.",
            err.message()
        );
        let cause = err.source().expect("Java's cause is never null on this path");
        assert_eq!(
            "Negative duration is not supported in by_duration offset reset strategy.",
            cause.message()
        );
    }
}
