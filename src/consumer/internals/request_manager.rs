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

//! `RequestManager` trait — the common interface every consumer request
//! manager implements (Phase 7-9: `Commit`, `Heartbeat`, `Fetch`,
//! `Offsets`, ...; Phase 6: `CoordinatorRequestManager`).
//!
//! Translated from
//! `org.apache.kafka.clients.consumer.internals.RequestManager`.

#![allow(dead_code)]

use super::network_client_delegate::PollResult;

/// The common interface for consumer request managers. Implementations
/// return a [`PollResult`] from [`Self::poll`] describing either the
/// requests they want dispatched or the time they need to wait before
/// having more work to do.
///
/// # Translation notes
///
/// - **No `#[async_trait]`** per `definition-of-done.md` §11 and Java's
///   contract: `poll` is documented as non-blocking ("no network I/O
///   occurs in this method"). Sync `fn` is the correct surface.
/// - **`Send + 'static`** so that
///   [`super::request_managers::RequestManagers::entries`] can return
///   `Vec<&mut dyn RequestManager>` and the bg task can own `Box<dyn
///   RequestManager>` references.
/// - **`&mut self`** on the mutating methods — they update the manager's
///   internal [`super::request_state::RequestState`] (backoff, attempt
///   count, etc.).
pub(crate) trait RequestManager: Send + 'static {
    /// During normal operation, a manager may need to send out network
    /// requests. Implementations return their need for network I/O by
    /// returning the requests inside a [`PollResult`]. Called from a
    /// single-threaded context (the consumer's bg task), so no
    /// synchronisation is required inside the body.
    ///
    /// **Note:** no network I/O occurs in this method. The method itself
    /// must not block for any reason. Quick execution is critical to
    /// ensure the consumer can heartbeat in a timely fashion.
    ///
    /// Java: `PollResult poll(long currentTimeMs)`.
    fn poll(&mut self, current_time_ms: i64) -> PollResult;

    /// On shutdown a manager may need to send out final network requests.
    /// Implementations can signal that by returning the close requests
    /// inside a [`PollResult`]. Default is an empty result — matches
    /// Java's `default PollResult pollOnClose(...) { return EMPTY; }`.
    ///
    /// Java: `default PollResult pollOnClose(long currentTimeMs)`.
    fn poll_on_close(&mut self, _current_time_ms: i64) -> PollResult {
        PollResult::empty()
    }

    /// Returns the maximum delay (ms) for which the application thread
    /// can safely wait before it should be responsive to results from
    /// this manager. Default `i64::MAX` matches Java's `Long.MAX_VALUE`.
    ///
    /// Java: `default long maximumTimeToWait(long currentTimeMs)`.
    fn maximum_time_to_wait(&self, _current_time_ms: i64) -> i64 {
        i64::MAX
    }

    /// Signals the manager that the consumer is closing so it can prepare
    /// for shutdown actions. Default no-op.
    ///
    /// Java: `default void signalClose()`.
    fn signal_close(&mut self) {}
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Default-method exerciser. Java's `RequestManager` interface
    /// provides default implementations for `pollOnClose`,
    /// `maximumTimeToWait`, and `signalClose`; this test pins those
    /// defaults so accidental changes are caught.
    struct DefaultsOnly;

    impl RequestManager for DefaultsOnly {
        fn poll(&mut self, _current_time_ms: i64) -> PollResult {
            PollResult::empty()
        }
    }

    #[test]
    fn default_poll_on_close_returns_empty() {
        let mut mgr = DefaultsOnly;
        let result = mgr.poll_on_close(0);
        assert_eq!(result.time_until_next_poll_ms, PollResult::WAIT_FOREVER);
        assert!(result.unsent_requests.is_empty());
    }

    #[test]
    fn default_maximum_time_to_wait_is_i64_max() {
        let mgr = DefaultsOnly;
        assert_eq!(mgr.maximum_time_to_wait(0), i64::MAX);
    }

    #[test]
    fn default_signal_close_is_noop() {
        let mut mgr = DefaultsOnly;
        mgr.signal_close();
        // No state to assert — the contract is "doesn't panic / doesn't
        // throw"; both are upheld here.
    }
}
