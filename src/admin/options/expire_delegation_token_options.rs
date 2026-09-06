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

//! Options for `Admin::expire_delegation_token`.
//!
//! Corresponds to
//! `org.apache.kafka.clients.admin.ExpireDelegationTokenOptions`.

/// Options for `Admin::expire_delegation_token`.
///
/// Corresponds to
/// `org.apache.kafka.clients.admin.ExpireDelegationTokenOptions`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExpireDelegationTokenOptions {
    expiry_time_period_ms: i64,
    timeout_ms: Option<i32>,
}

impl Default for ExpireDelegationTokenOptions {
    fn default() -> Self {
        Self { expiry_time_period_ms: -1, timeout_ms: None }
    }
}

impl ExpireDelegationTokenOptions {
    /// Creates default options (default API timeout, expire-immediately
    /// sentinel `-1`).
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the time period until the token should expire.
    ///
    /// `expiry_time_period_ms >= 0`: update the expiration timestamp to
    /// `min(now + expiry_time_period_ms, max_timestamp)`. `< 0`: expire the
    /// token immediately.
    ///
    /// Mirrors `ExpireDelegationTokenOptions.expiryTimePeriodMs`.
    #[must_use]
    pub fn expiry_time_period_ms(mut self, expiry_time_period_ms: i64) -> Self {
        self.expiry_time_period_ms = expiry_time_period_ms;
        self
    }

    /// The time period until the token should expire.
    ///
    /// Mirrors `ExpireDelegationTokenOptions.expiryTimePeriodMs`.
    pub fn get_expiry_time_period_ms(&self) -> i64 {
        self.expiry_time_period_ms
    }

    /// Set the timeout in milliseconds for this operation, or `None` to use the
    /// default API timeout for the `AdminClient`.
    #[must_use]
    pub fn set_timeout_ms(mut self, timeout_ms: Option<i32>) -> Self {
        self.timeout_ms = timeout_ms;
        self
    }

    /// The timeout in milliseconds for this operation, or `None` if the default
    /// API timeout should be used.
    pub fn timeout_ms(&self) -> Option<i32> {
        self.timeout_ms
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_and_setter() {
        assert_eq!(ExpireDelegationTokenOptions::new().get_expiry_time_period_ms(), -1);
        let options = ExpireDelegationTokenOptions::new()
            .expiry_time_period_ms(1000)
            .set_timeout_ms(Some(5000));
        assert_eq!(options.get_expiry_time_period_ms(), 1000);
        assert_eq!(options.timeout_ms(), Some(5000));
    }
}
