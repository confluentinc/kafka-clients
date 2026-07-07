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

//! Concrete statistics (org.apache.kafka.common.metrics.stats).

pub mod avg;
pub mod cumulative_count;
pub mod cumulative_sum;
pub mod frequencies;
pub mod frequency;
pub mod histogram;
pub mod max;
pub mod meter;
pub mod min;
pub mod percentile;
pub mod percentiles;
pub mod rate;
pub mod sampled_stat;
pub mod simple_rate;
pub mod token_bucket;
pub mod value;
pub mod windowed_count;
pub mod windowed_sum;

pub use avg::Avg;
pub use cumulative_count::CumulativeCount;
pub use cumulative_sum::CumulativeSum;
pub use frequencies::Frequencies;
pub use frequency::Frequency;
pub use histogram::{BinScheme, ConstantBinScheme, Histogram, LinearBinScheme};
pub use max::Max;
pub use meter::Meter;
pub use min::Min;
pub use percentile::Percentile;
pub use percentiles::{BucketSizing, Percentiles};
pub use rate::Rate;
pub use sampled_stat::{Sample, SampledStat, SampledStatBase};
pub use simple_rate::SimpleRate;
pub use token_bucket::TokenBucket;
pub use value::Value;
pub use windowed_count::WindowedCount;
pub use windowed_sum::WindowedSum;

#[cfg(test)]
pub(crate) use test_time::MockTime;

/// A mock clock shared by the statistics tests, mirroring Java's `MockTime`
/// closely enough to drive time-windowed behavior: a manually advanced clock
/// with no auto-tick.
#[cfg(test)]
pub(crate) mod test_time {
    use std::sync::atomic::{AtomicI64, Ordering};

    /// A manually advanced clock reporting POSIX milliseconds.
    pub(crate) struct MockTime {
        current_ms: AtomicI64,
    }

    impl MockTime {
        /// Creates a clock starting at time `0`.
        pub(crate) fn new() -> Self {
            Self::with_start(0)
        }

        /// Creates a clock starting at `start_ms`.
        pub(crate) fn with_start(start_ms: i64) -> Self {
            Self { current_ms: AtomicI64::new(start_ms) }
        }

        /// The current time in milliseconds.
        pub(crate) fn milliseconds(&self) -> i64 {
            self.current_ms.load(Ordering::SeqCst)
        }

        /// Advances the clock by `ms` milliseconds.
        pub(crate) fn sleep(&self, ms: i64) {
            self.current_ms.fetch_add(ms, Ordering::SeqCst);
        }
    }
}
