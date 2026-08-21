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

//! Metrics internal helpers
//! (`org.apache.kafka.common.metrics.internals.MetricsUtils`).

use std::collections::BTreeMap;

use crate::common::Error;

/// A subset of `java.util.concurrent.TimeUnit` used by the metrics rate stats.
///
/// Only the variants the metrics framework needs are modelled; the conversion
/// factors in [`convert`] are exactly Java's `TimeUnit` semantics.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TimeUnit {
    /// Nanoseconds.
    Nanoseconds,
    /// Microseconds.
    Microseconds,
    /// Milliseconds.
    Milliseconds,
    /// Seconds.
    Seconds,
    /// Minutes.
    Minutes,
    /// Hours.
    Hours,
    /// Days.
    Days,
}

impl TimeUnit {
    /// The `name()` of the unit, matching Java's enum constant name.
    pub fn name(&self) -> &'static str {
        match self {
            TimeUnit::Nanoseconds => "NANOSECONDS",
            TimeUnit::Microseconds => "MICROSECONDS",
            TimeUnit::Milliseconds => "MILLISECONDS",
            TimeUnit::Seconds => "SECONDS",
            TimeUnit::Minutes => "MINUTES",
            TimeUnit::Hours => "HOURS",
            TimeUnit::Days => "DAYS",
        }
    }

    /// Convert a duration expressed in this unit to milliseconds, mirroring
    /// `TimeUnit.MILLISECONDS.convert(window, unit)`. Integer truncation matches
    /// Java's `long` arithmetic.
    pub fn to_millis(self, window: i64) -> i64 {
        match self {
            TimeUnit::Nanoseconds => window / 1_000_000,
            TimeUnit::Microseconds => window / 1_000,
            TimeUnit::Milliseconds => window,
            TimeUnit::Seconds => window.saturating_mul(1_000),
            TimeUnit::Minutes => window.saturating_mul(60 * 1_000),
            TimeUnit::Hours => window.saturating_mul(60 * 60 * 1_000),
            TimeUnit::Days => window.saturating_mul(24 * 60 * 60 * 1_000),
        }
    }
}

/// Convert the provided time from milliseconds to the requested time unit.
///
/// Faithful translation of `MetricsUtils.convert(long timeMs, TimeUnit unit)`.
pub fn convert(time_ms: i64, unit: TimeUnit) -> f64 {
    let time_ms = time_ms as f64;
    match unit {
        TimeUnit::Nanoseconds => time_ms * 1000.0 * 1000.0,
        TimeUnit::Microseconds => time_ms * 1000.0,
        TimeUnit::Milliseconds => time_ms,
        TimeUnit::Seconds => time_ms / 1000.0,
        TimeUnit::Minutes => time_ms / (60.0 * 1000.0),
        TimeUnit::Hours => time_ms / (60.0 * 60.0 * 1000.0),
        TimeUnit::Days => time_ms / (24.0 * 60.0 * 60.0 * 1000.0),
    }
}

/// Convert a sequence of `key, value` pairs to a tags map.
///
/// Returns an error (Java throws `IllegalArgumentException`) if the number of
/// elements is odd.
pub fn get_tags(key_value: &[&str]) -> Result<BTreeMap<String, String>, Error> {
    if !key_value.len().is_multiple_of(2) {
        return Err(Error::illegal_argument("keyValue needs to be specified in pairs"));
    }
    let mut tags = BTreeMap::new();
    let mut i = 0;
    while i < key_value.len() {
        tags.insert(key_value[i].to_string(), key_value[i + 1].to_string());
        i += 2;
    }
    Ok(tags)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pairs_to_map() {
        let tags = get_tags(&["k1", "v1", "k2", "v2"]).unwrap();
        assert_eq!(tags.get("k1").map(String::as_str), Some("v1"));
        assert_eq!(tags.get("k2").map(String::as_str), Some("v2"));
        assert_eq!(tags.len(), 2);
    }

    #[test]
    fn odd_count_is_error() {
        let err = get_tags(&["k1"]).unwrap_err();
        assert!(err.to_string().contains("keyValue needs to be specified in pairs"));
    }

    #[test]
    fn empty_is_empty_map() {
        assert!(get_tags(&[]).unwrap().is_empty());
    }
}
