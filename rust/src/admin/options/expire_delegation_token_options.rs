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

/// Options for `Admin::expire_delegation_token`.
#[derive(Clone, Debug)]
#[doc(alias = "org.apache.kafka.clients.admin.ExpireDelegationTokenOptions")]
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
    #[doc(alias = "org.apache.kafka.clients.admin.ExpireDelegationTokenOptions#ExpireDelegationTokenOptions")]
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the time period until the token should expire.
    ///
    /// # Arguments
    ///
    /// - `expiry_time_period_ms` - the time period until we should expire this token.
    ///   `expiry_time_period_ms` >= 0: the token will update the expiration timestamp to
    ///   min(now + `expiry_time_period_ms`, maxTimestamp); `expiry_time_period_ms` < 0: token will
    ///   be expired immediately.
    ///
    /// # Returns
    ///
    /// This `ExpireDelegationTokenOptions` instance.
    #[must_use]
    #[doc(alias = "org.apache.kafka.clients.admin.ExpireDelegationTokenOptions#expiryTimePeriodMs")]
    pub fn set_expiry_time_period_ms(mut self, expiry_time_period_ms: i64) -> Self {
        self.expiry_time_period_ms = expiry_time_period_ms;
        self
    }

    /// Returns the time period until the token should expire.
    #[doc(alias = "org.apache.kafka.clients.admin.ExpireDelegationTokenOptions#expiryTimePeriodMs")]
    pub fn expiry_time_period_ms(&self) -> i64 {
        self.expiry_time_period_ms
    }

    /// Set the timeout in milliseconds for this operation, or `None` if the default
    /// API timeout for the `AdminClient` should be used.
    ///
    /// # Arguments
    ///
    /// - `timeout_ms` - the timeout in milliseconds, or `None` for the default API timeout.
    ///
    /// # Returns
    ///
    /// This `ExpireDelegationTokenOptions` instance.
    #[must_use]
    #[doc(alias = "org.apache.kafka.clients.admin.AbstractOptions#timeoutMs")]
    pub fn set_timeout_ms(mut self, timeout_ms: Option<i32>) -> Self {
        self.timeout_ms = timeout_ms;
        self
    }

    /// Returns the timeout in milliseconds for this operation, or `None` if the
    /// default API timeout for the `AdminClient` should be used.
    #[doc(alias = "org.apache.kafka.clients.admin.AbstractOptions#timeoutMs")]
    pub fn timeout_ms(&self) -> Option<i32> {
        self.timeout_ms
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_and_setter() {
        assert_eq!(ExpireDelegationTokenOptions::new().expiry_time_period_ms(), -1);
        let options = ExpireDelegationTokenOptions::new()
            .set_expiry_time_period_ms(1000)
            .set_timeout_ms(Some(5000));
        assert_eq!(options.expiry_time_period_ms(), 1000);
        assert_eq!(options.timeout_ms(), Some(5000));
    }
}
