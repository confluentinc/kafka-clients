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
    /// Returns [`Error::IllegalArgument`] if the input does not match
    /// one of the accepted forms or if the ISO-8601 duration cannot be parsed
    /// or is negative.
    pub fn from_string(s: &str) -> Result<Self, Error> {
        if s == "by_duration" {
            return Err(Error::illegal_argument(
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
            let duration = parse_iso8601_duration(iso).map_err(|_| {
                Error::illegal_argument("Unable to parse duration string in by_duration offset reset strategy.")
            })?;
            return Ok(Self { strategy_type: StrategyType::ByDuration, duration: Some(duration) });
        }
        Err(Error::illegal_argument(format!("Unknown auto offset reset strategy: {s}")))
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

/// Parse a subset of ISO-8601 durations: `PnDTnHnMn(.fS)?`.
///
/// Negative durations are explicitly rejected, matching Java's check after
/// `Duration.parse`. Fractional seconds are supported.
///
/// # Errors
///
/// Returns `Err(())` if the string cannot be parsed or represents a negative
/// duration. The caller maps this to [`Error::IllegalArgument`].
fn parse_iso8601_duration(input: &str) -> Result<Duration, ()> {
    // Reject negative durations explicitly (the Java spec also rejects them
    // via `duration.isNegative()`); a leading '-' would otherwise be accepted
    // by `Duration.parse`.
    if input.starts_with('-') {
        return Err(());
    }
    let rest = input.strip_prefix('P').ok_or(())?;
    if rest.is_empty() {
        return Err(());
    }

    // Split into the date-part (before 'T') and the time-part (after 'T').
    let (date_part, time_part) = match rest.split_once('T') {
        Some((d, t)) => (d, Some(t)),
        None => (rest, None),
    };
    if date_part.is_empty() && time_part.is_none() {
        return Err(());
    }
    if let Some(t) = time_part
        && t.is_empty()
    {
        return Err(());
    }

    let mut total_secs: u64 = 0;
    let mut nanos: u32 = 0;

    // ---- date part: only days are accepted (P<n>D) — Kafka's spec does not
    // require years/months/weeks and Java's `Duration.parse` likewise rejects
    // those.
    if !date_part.is_empty() {
        // Expect <integer>D
        let days_str = date_part.strip_suffix('D').ok_or(())?;
        let days: u64 = days_str.parse().map_err(|_| ())?;
        total_secs = total_secs.checked_add(days.checked_mul(86_400).ok_or(())?).ok_or(())?;
    }

    // ---- time part: H, M, (.S)? in that order ----
    if let Some(t) = time_part {
        let mut buf = t;
        // Hours
        if let Some(idx) = buf.find('H') {
            let (head, tail) = buf.split_at(idx);
            let hours: u64 = head.parse().map_err(|_| ())?;
            total_secs = total_secs.checked_add(hours.checked_mul(3_600).ok_or(())?).ok_or(())?;
            buf = &tail[1..]; // skip 'H'
        }
        // Minutes
        if let Some(idx) = buf.find('M') {
            let (head, tail) = buf.split_at(idx);
            let minutes: u64 = head.parse().map_err(|_| ())?;
            total_secs = total_secs.checked_add(minutes.checked_mul(60).ok_or(())?).ok_or(())?;
            buf = &tail[1..]; // skip 'M'
        }
        // Seconds (optionally fractional)
        if let Some(idx) = buf.find('S') {
            let (head, tail) = buf.split_at(idx);
            if !tail.is_empty() && !tail[1..].is_empty() {
                // Anything after the 'S' is invalid.
                return Err(());
            }
            // Parse seconds, possibly with fractional component.
            let (int_part, frac_part) = match head.split_once('.') {
                Some((i, f)) => (i, Some(f)),
                None => (head, None),
            };
            let secs: u64 = int_part.parse().map_err(|_| ())?;
            total_secs = total_secs.checked_add(secs).ok_or(())?;
            if let Some(f) = frac_part {
                if f.is_empty() || f.len() > 9 {
                    return Err(());
                }
                // Pad/truncate to 9 digits to convert to nanoseconds.
                let mut padded = String::with_capacity(9);
                padded.push_str(f);
                while padded.len() < 9 {
                    padded.push('0');
                }
                nanos = padded.parse().map_err(|_| ())?;
            }
            buf = "";
        }
        if !buf.is_empty() {
            return Err(());
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
}
