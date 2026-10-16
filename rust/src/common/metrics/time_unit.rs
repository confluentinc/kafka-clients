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

/// A subset of `java.util.concurrent.TimeUnit` used by the metrics rate stats.
///
/// A JDK type, so Kafka's audience rules do not apply: it is public because
/// the public `MetricConfig::set_time_window`, `Rate` and `Meter` take it.
///
/// Only the variants the metrics framework needs are modelled; the conversion
/// factors used by the rate stats are exactly Java's `TimeUnit` semantics.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
// translates the JDK's java.util.concurrent.TimeUnit, outside Kafka's audience rules; the public MetricConfig::set_time_window, Rate and Meter take it
#[doc(alias = "rust-only")]
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
