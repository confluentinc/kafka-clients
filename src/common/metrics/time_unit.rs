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

//! Granularities for representing durations, used by the metrics windows and
//! rate statistics.

/// A unit of time at a given granularity. Provides just the conversion surface
/// the metrics framework needs (`java.util.concurrent.TimeUnit` in the source
/// exposes far more, but only unit selection and conversion are used here).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum TimeUnit {
    /// One thousandth of a microsecond.
    Nanoseconds,
    /// One thousandth of a millisecond.
    Microseconds,
    /// One thousandth of a second.
    Milliseconds,
    /// One second.
    Seconds,
    /// Sixty seconds.
    Minutes,
    /// Sixty minutes.
    Hours,
    /// Twenty-four hours.
    Days,
}

impl TimeUnit {
    /// The number of nanoseconds in one unit of this granularity.
    const fn scale_nanos(self) -> i64 {
        match self {
            Self::Nanoseconds => 1,
            Self::Microseconds => 1_000,
            Self::Milliseconds => 1_000_000,
            Self::Seconds => 1_000_000_000,
            Self::Minutes => 60_000_000_000,
            Self::Hours => 3_600_000_000_000,
            Self::Days => 86_400_000_000_000,
        }
    }

    /// The upper-case name of this unit.
    pub fn name(self) -> &'static str {
        match self {
            Self::Nanoseconds => "NANOSECONDS",
            Self::Microseconds => "MICROSECONDS",
            Self::Milliseconds => "MILLISECONDS",
            Self::Seconds => "SECONDS",
            Self::Minutes => "MINUTES",
            Self::Hours => "HOURS",
            Self::Days => "DAYS",
        }
    }

    /// Converts `source_duration`, expressed in `source_unit`, into this unit,
    /// truncating any fractional part. The result saturates to the `i64` range
    /// on overflow.
    pub fn convert(self, source_duration: i64, source_unit: TimeUnit) -> i64 {
        let scaled = (source_duration as i128 * source_unit.scale_nanos() as i128) / self.scale_nanos() as i128;
        scaled.clamp(i64::MIN as i128, i64::MAX as i128) as i64
    }

    /// Converts `duration`, expressed in this unit, into milliseconds.
    pub fn to_millis(self, duration: i64) -> i64 {
        TimeUnit::Milliseconds.convert(duration, self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_to_millis() {
        assert_eq!(TimeUnit::Seconds.to_millis(1), 1000);
        assert_eq!(TimeUnit::Seconds.to_millis(30), 30_000);
        assert_eq!(TimeUnit::Milliseconds.to_millis(5), 5);
        assert_eq!(TimeUnit::Minutes.to_millis(2), 120_000);
    }

    #[test]
    fn test_convert_between_units() {
        // Coarser -> finer.
        assert_eq!(TimeUnit::Milliseconds.convert(1, TimeUnit::Seconds), 1000);
        // Finer -> coarser truncates.
        assert_eq!(TimeUnit::Seconds.convert(1500, TimeUnit::Milliseconds), 1);
        // Same unit is identity.
        assert_eq!(TimeUnit::Seconds.convert(42, TimeUnit::Seconds), 42);
    }

    #[test]
    fn test_name() {
        assert_eq!(TimeUnit::Seconds.name(), "SECONDS");
        assert_eq!(TimeUnit::Nanoseconds.name(), "NANOSECONDS");
    }
}
