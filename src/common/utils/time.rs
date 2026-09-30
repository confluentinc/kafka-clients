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

//! The clock abstraction, translated from `org.apache.kafka.common.utils.Time`.

/// An interface abstracting the clock to use in unit testing classes that make
/// use of clock time.
///
/// Implementations of this class should be thread-safe.
///
/// Every client holds one `Arc<dyn Time>` and hands it to the components it
/// builds, as Java hands its single `Time` instance down from the
/// `KafkaProducer` / `KafkaConsumer` / `KafkaAdminClient` constructors. The
/// two readings are different clocks and must not be mixed:
/// [`milliseconds`](Time::milliseconds) is wall-clock time, used for deadlines
/// and timestamps; [`nanoseconds`](Time::nanoseconds) is monotonic, used only
/// to measure elapsed time.
///
/// Java's `sleep`, `waitObject`, `timer` and `waitForFuture` are not
/// translated. The first two block the calling thread on a monitor, which the
/// Rust client replaces with `tokio` awaits; the last two build on
/// `org.apache.kafka.common.utils.Timer` and `java.util.concurrent.Future`,
/// neither of which the client uses.
#[doc(alias = "org.apache.kafka.common.utils.Time")]
pub(crate) trait Time: Send + Sync + 'static {
    /// Returns the current time in milliseconds.
    #[doc(alias = "org.apache.kafka.common.utils.Time#milliseconds")]
    fn milliseconds(&self) -> i64;

    /// Returns the value returned by [`nanoseconds`](Time::nanoseconds)
    /// converted into milliseconds.
    #[doc(alias = "org.apache.kafka.common.utils.Time#hiResClockMs")]
    fn hi_res_clock_ms(&self) -> i64 {
        // `TimeUnit.NANOSECONDS.toMillis` truncates toward zero, as `/` does.
        self.nanoseconds() / 1_000_000
    }

    /// Returns the current value of the running process's high-resolution time
    /// source, in nanoseconds.
    ///
    /// This method can only be used to measure elapsed time and is not related
    /// to any other notion of system or wall-clock time. The value returned
    /// represents nanoseconds since some fixed but arbitrary *origin* time, so
    /// only differences between two readings are meaningful.
    #[doc(alias = "org.apache.kafka.common.utils.Time#nanoseconds")]
    fn nanoseconds(&self) -> i64;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::utils::MockTime;

    #[test]
    fn hi_res_clock_ms_is_nanoseconds_in_milliseconds() {
        let time = MockTime::with_auto_tick_ms_current_time_ms_current_high_res_time_ns(0, 0, 5_999_999);
        assert_eq!(time.hi_res_clock_ms(), 5);
    }
}
