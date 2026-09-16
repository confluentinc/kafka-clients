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

//! Options for `Admin::renew_delegation_token`.
//!
//! Corresponds to
//! `org.apache.kafka.clients.admin.RenewDelegationTokenOptions`.

/// Options for `Admin::renew_delegation_token`.
///
/// Corresponds to
/// `org.apache.kafka.clients.admin.RenewDelegationTokenOptions`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RenewDelegationTokenOptions {
    renew_time_period_ms: i64,
    timeout_ms: Option<i32>,
}

impl Default for RenewDelegationTokenOptions {
    fn default() -> Self {
        Self { renew_time_period_ms: -1, timeout_ms: None }
    }
}

impl RenewDelegationTokenOptions {
    /// Creates default options (default API timeout, server-default renew
    /// period).
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the renew time period in milliseconds.
    ///
    /// Mirrors `RenewDelegationTokenOptions.renewTimePeriodMs`.
    #[must_use]
    pub fn set_renew_time_period_ms(mut self, renew_time_period_ms: i64) -> Self {
        self.renew_time_period_ms = renew_time_period_ms;
        self
    }

    /// The renew time period in milliseconds.
    ///
    /// Mirrors `RenewDelegationTokenOptions.renewTimePeriodMs`.
    pub fn renew_time_period_ms(&self) -> i64 {
        self.renew_time_period_ms
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
        assert_eq!(RenewDelegationTokenOptions::new().renew_time_period_ms(), -1);
        let options = RenewDelegationTokenOptions::new()
            .set_renew_time_period_ms(1000)
            .set_timeout_ms(Some(5000));
        assert_eq!(options.renew_time_period_ms(), 1000);
        assert_eq!(options.timeout_ms(), Some(5000));
    }
}
