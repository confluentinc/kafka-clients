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

//! Translated from `org.apache.kafka.server.util.ThroughputThrottler`.
//!
//! Pulled into this tool crate as a producer dependency: there is no
//! `server-common` crate to host it, and `VerifiableProducer` is its only
//! consumer here.

use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::time::{Duration, Instant};

use tokio::sync::Notify;

const NS_PER_MS: i64 = 1_000_000;
const NS_PER_SEC: i64 = 1000 * NS_PER_MS;
const MIN_SLEEP_NS: i64 = 2 * NS_PER_MS;

/// This struct helps producers throttle throughput.
///
/// If `targetThroughput >= 0`, the resulting average throughput will be
/// approximately `min(targetThroughput, maximumPossibleThroughput)`. If
/// `targetThroughput < 0`, no throttling will occur.
///
/// To use, do this between successive send attempts:
///
/// ```ignore
/// if throttler.should_throttle(amount_so_far, send_start_ms) {
///     throttler.throttle().await;
/// }
/// ```
///
/// Note that this can be used to throttle message throughput or data
/// throughput.
///
/// # Async translation
///
/// Java blocks the throttling task with `Object.wait()` on an intrinsic monitor.
/// The Rust translation is non-blocking: the timed path uses
/// [`tokio::time::sleep`] and the `targetThroughput == 0` block-until-wakeup path
/// uses a [`tokio::sync::Notify`]. The `wakeup` flag is preserved as an
/// [`AtomicBool`] so [`wakeup`](Self::wakeup) can be called from any task while
/// [`throttle`](Self::throttle) is sleeping — the exact concurrency Java's
/// `synchronized`/`notifyAll` provides.
pub struct ThroughputThrottler {
    start_ms: i64,
    sleep_time_ns: i64,
    target_throughput: f64,

    // Guarded by `synchronized (this)` in Java; single-writer here (only the
    // task driving `throttle` mutates it), but kept atomic so the whole struct
    // is `Sync` and shareable behind `&self` alongside `wakeup`.
    sleep_deficit_ns: AtomicI64,
    // Java's `boolean wakeup`: a persistent flag set true by `wakeup()` and
    // reset to false only inside the timed-sleep branch (never in the
    // `targetThroughput == 0` branch — faithful to the Java asymmetry).
    wakeup: AtomicBool,
    // Wakes a task blocked in `throttle().await`; the Rust analog of
    // `notifyAll()`. The `wakeup` flag above remains the source of truth.
    notify: Notify,
}

impl ThroughputThrottler {
    /// * `target_throughput` — Can be messages/sec or bytes/sec
    /// * `start_ms` — When the very first message is sent
    pub fn new(target_throughput: f64, start_ms: i64) -> Self {
        let sleep_time_ns = if target_throughput > 0.0 {
            (NS_PER_SEC as f64 / target_throughput) as i64
        } else {
            i64::MAX
        };
        Self {
            start_ms,
            sleep_time_ns,
            target_throughput,
            sleep_deficit_ns: AtomicI64::new(0),
            wakeup: AtomicBool::new(false),
            notify: Notify::new(),
        }
    }

    /// * `amount_so_far` — bytes produced so far if you want to throttle data
    ///   throughput, or messages produced so far if you want to throttle message
    ///   throughput.
    /// * `send_start_ms` — timestamp of the most recently sent message
    ///
    /// Returns `true` if throttling should happen.
    pub fn should_throttle(&self, amount_so_far: i64, send_start_ms: i64) -> bool {
        if self.target_throughput < 0.0 {
            // No throttling in this case
            return false;
        }

        let elapsed_sec = (send_start_ms - self.start_ms) as f32 / 1000.0;
        elapsed_sec > 0.0 && (amount_so_far as f64 / elapsed_sec as f64) > self.target_throughput
    }

    /// Occasionally blocks for small amounts of time to achieve
    /// `targetThroughput`.
    ///
    /// Note that if `targetThroughput` is 0, this will block extremely
    /// aggressively.
    pub async fn throttle(&self) {
        if self.target_throughput == 0.0 {
            // Block until woken. Java loops `while (!wakeup) this.wait()`.
            // Register the `notified()` waiter *before* checking the flag so a
            // `wakeup()` racing between the check and the await is not lost.
            loop {
                let notified = self.notify.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                if self.wakeup.load(Ordering::Acquire) {
                    return;
                }
                notified.await;
            }
        }

        // throttle throughput by sleeping, on average,
        // (1 / this.throughput) seconds between "things sent"
        let sleep_deficit_ns =
            self.sleep_deficit_ns.fetch_add(self.sleep_time_ns, Ordering::AcqRel) + self.sleep_time_ns;

        // If enough sleep deficit has accumulated, sleep a little
        if sleep_deficit_ns >= MIN_SLEEP_NS {
            let sleep_start = Instant::now();
            let mut remaining = sleep_deficit_ns;
            loop {
                // Register the `notified()` waiter *before* checking the flag so
                // a `wakeup()` racing between the check and the await is not lost
                // (same ordering as the `target == 0` path above).
                let notified = self.notify.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                if self.wakeup.load(Ordering::Acquire) || remaining <= 0 {
                    break;
                }
                let sleep_dur = Duration::from_nanos(remaining as u64);
                // Wake either when the timer elapses or when `wakeup()` fires its
                // `Notify` — the Rust equivalent of `wait(sleepMs, sleepNs)`
                // returning on timeout or `notifyAll()`. Both arms are
                // cancellation-safe (a dropped timer / dropped waiter has no side
                // effect), and the `wakeup` flag is the source of truth.
                tokio::select! {
                    _ = tokio::time::sleep(sleep_dur) => {}
                    _ = notified.as_mut() => {}
                }
                let elapsed = sleep_start.elapsed().as_nanos() as i64;
                remaining = sleep_deficit_ns - elapsed;
            }
            self.wakeup.store(false, Ordering::Release);
            self.sleep_deficit_ns.store(0, Ordering::Release);
        }
    }

    /// Wakeup the throttler if it is sleeping.
    pub fn wakeup(&self) {
        self.wakeup.store(true, Ordering::Release);
        self.notify.notify_waiters();
    }

    /// Exposes the computed per-message sleep time (nanoseconds). Test-only view
    /// of the value Java derives in the constructor.
    #[cfg(test)]
    pub(crate) fn sleep_time_ns(&self) -> i64 {
        self.sleep_time_ns
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_never_throttle_with_negative_target() {
        // targetThroughput < 0 => no throttling, regardless of amount/elapsed.
        let throttler = ThroughputThrottler::new(-1.0, 0);
        assert!(!throttler.should_throttle(0, 0));
        assert!(!throttler.should_throttle(1_000_000, 10_000));
    }

    #[test]
    fn should_not_throttle_when_no_time_has_elapsed() {
        // elapsedSec == 0 => the guard `elapsedSec > 0` is false.
        let throttler = ThroughputThrottler::new(100.0, 1000);
        assert!(!throttler.should_throttle(10, 1000));
    }

    #[test]
    fn should_throttle_when_rate_exceeds_target() {
        // start_ms=0, send_start_ms=1000 => elapsedSec=1.0.
        // rate = amount / elapsedSec.
        let throttler = ThroughputThrottler::new(100.0, 0);
        // 200 msgs in 1s => 200/s > 100/s => throttle.
        assert!(throttler.should_throttle(200, 1000));
        // 50 msgs in 1s => 50/s < 100/s => no throttle.
        assert!(!throttler.should_throttle(50, 1000));
        // Exactly at the target: 100/s is NOT > 100 => no throttle.
        assert!(!throttler.should_throttle(100, 1000));
    }

    #[test]
    fn sleep_time_is_ns_per_sec_over_target() {
        // targetThroughput > 0 => sleepTimeNs = NS_PER_SEC / target.
        let throttler = ThroughputThrottler::new(1000.0, 0);
        assert_eq!(throttler.sleep_time_ns(), NS_PER_SEC / 1000);

        let throttler = ThroughputThrottler::new(1.0, 0);
        assert_eq!(throttler.sleep_time_ns(), NS_PER_SEC);
    }

    #[test]
    fn sleep_time_is_max_when_target_not_positive() {
        // targetThroughput == 0 or < 0 => sleepTimeNs = Long.MAX_VALUE.
        assert_eq!(ThroughputThrottler::new(0.0, 0).sleep_time_ns(), i64::MAX);
        assert_eq!(ThroughputThrottler::new(-5.0, 0).sleep_time_ns(), i64::MAX);
    }

    #[tokio::test]
    async fn zero_target_blocks_until_wakeup() {
        use std::sync::Arc;

        // targetThroughput == 0 blocks in throttle() until wakeup() is called.
        let throttler = Arc::new(ThroughputThrottler::new(0.0, 0));
        let t2 = Arc::clone(&throttler);
        let handle = tokio::spawn(async move {
            t2.throttle().await;
        });
        // Give the task a moment to reach the wait, then wake it.
        tokio::task::yield_now().await;
        throttler.wakeup();
        // Must complete promptly now that the flag is set.
        tokio::time::timeout(Duration::from_secs(5), handle)
            .await
            .expect("throttle() should return after wakeup()")
            .expect("task should not panic");
    }

    #[tokio::test]
    async fn zero_target_returns_immediately_if_already_woken() {
        // Java never resets `wakeup` in the target==0 branch, so once set it
        // short-circuits every subsequent throttle().
        let throttler = ThroughputThrottler::new(0.0, 0);
        throttler.wakeup();
        tokio::time::timeout(Duration::from_secs(5), throttler.throttle())
            .await
            .expect("throttle() should return immediately when already woken");
    }

    #[test]
    fn should_throttle_with_zero_target() {
        // targetThroughput == 0 is not < 0, so the rate check applies: any
        // positive rate over positive elapsed time throttles.
        let throttler = ThroughputThrottler::new(0.0, 0);
        // 1 msg after 1s => rate 1/s > 0 => throttle.
        assert!(throttler.should_throttle(1, 1000));
        // 0 msgs => rate 0, not > 0 => no throttle.
        assert!(!throttler.should_throttle(0, 1000));
        // elapsed == 0 => guard `elapsedSec > 0` is false => no throttle.
        assert!(!throttler.should_throttle(1, 0));
    }

    #[tokio::test]
    async fn timed_path_sleeps_then_wakeup_interrupts() {
        use std::sync::Arc;

        // target == 1/sec => sleepTimeNs == 1s, so a single throttle() accrues
        // enough deficit to enter the timed-sleep branch and would sleep ~1s.
        let throttler = Arc::new(ThroughputThrottler::new(1.0, 0));
        let t2 = Arc::clone(&throttler);
        let started = Instant::now();
        let handle = tokio::spawn(async move {
            t2.throttle().await;
        });
        // Let the task reach the sleep, then wake it. If wakeup() failed to
        // interrupt the timed sleep, throttle() would run out the full ~1s;
        // the assertion is that the wakeup returns it far sooner.
        tokio::task::yield_now().await;
        throttler.wakeup();
        tokio::time::timeout(Duration::from_secs(5), handle)
            .await
            .expect("throttle() should return after wakeup()")
            .expect("task should not panic");
        assert!(
            started.elapsed() < Duration::from_millis(500),
            "wakeup() should interrupt the ~1s timed sleep promptly, took {:?}",
            started.elapsed()
        );
    }

    #[tokio::test]
    async fn timed_path_wakeup_before_throttle_is_not_lost() {
        // Regression: a wakeup() that fires before throttle() reaches its await
        // must not be lost. The timed path registers the waiter and checks the
        // flag before sleeping, so throttle() returns without sleeping at all.
        let throttler = ThroughputThrottler::new(1.0, 0);
        throttler.wakeup();
        tokio::time::timeout(Duration::from_secs(5), throttler.throttle())
            .await
            .expect("throttle() should return immediately when woken before the sleep");
    }
}
