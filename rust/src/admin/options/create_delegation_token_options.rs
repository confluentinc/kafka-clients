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

//! Options for `Admin::create_delegation_token`.

use crate::common::security::auth::KafkaPrincipal;

/// Options for `Admin::create_delegation_token`.
#[derive(Clone, Debug)]
#[doc(alias = "org.apache.kafka.clients.admin.CreateDelegationTokenOptions")]
pub struct CreateDelegationTokenOptions {
    max_lifetime_ms: i64,
    renewers: Vec<KafkaPrincipal>,
    owner: Option<KafkaPrincipal>,
    timeout_ms: Option<i32>,
}

impl Default for CreateDelegationTokenOptions {
    fn default() -> Self {
        Self { max_lifetime_ms: -1, renewers: Vec::new(), owner: None, timeout_ms: None }
    }
}

impl CreateDelegationTokenOptions {
    /// Creates default options (default API timeout, server-default lifetime,
    /// no renewers, owner defaults to the request principal).
    #[doc(alias = "org.apache.kafka.clients.admin.CreateDelegationTokenOptions#CreateDelegationTokenOptions")]
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the principals allowed to renew the token.
    ///
    /// # Arguments
    ///
    /// - `renewers` - the principals allowed to renew the token.
    ///
    /// # Returns
    ///
    /// This `CreateDelegationTokenOptions` instance.
    #[must_use]
    #[doc(alias = "org.apache.kafka.clients.admin.CreateDelegationTokenOptions#renewers")]
    pub fn set_renewers(mut self, renewers: Vec<KafkaPrincipal>) -> Self {
        self.renewers = renewers;
        self
    }

    /// Returns the principals allowed to renew the token.
    #[doc(alias = "org.apache.kafka.clients.admin.CreateDelegationTokenOptions#renewers")]
    pub fn renewers(&self) -> &[KafkaPrincipal] {
        &self.renewers
    }

    /// Sets the owner of the token.
    ///
    /// # Arguments
    ///
    /// - `owner` - the owner of the token.
    ///
    /// # Returns
    ///
    /// This `CreateDelegationTokenOptions` instance.
    #[must_use]
    #[doc(alias = "org.apache.kafka.clients.admin.CreateDelegationTokenOptions#owner")]
    pub fn set_owner(mut self, owner: KafkaPrincipal) -> Self {
        self.owner = Some(owner);
        self
    }

    /// Returns the owner of the token, or `None` if not set.
    #[doc(alias = "org.apache.kafka.clients.admin.CreateDelegationTokenOptions#owner")]
    pub fn owner(&self) -> Option<&KafkaPrincipal> {
        self.owner.as_ref()
    }

    /// Sets the maximum lifetime of the token in milliseconds, or `-1` to use the server-side
    /// default.
    ///
    /// # Arguments
    ///
    /// - `max_lifetime_ms` - the maximum lifetime in milliseconds.
    ///
    /// # Returns
    ///
    /// This `CreateDelegationTokenOptions` instance.
    #[must_use]
    #[doc(alias = "org.apache.kafka.clients.admin.CreateDelegationTokenOptions#maxLifetimeMs")]
    pub fn set_max_lifetime_ms(mut self, max_lifetime_ms: i64) -> Self {
        self.max_lifetime_ms = max_lifetime_ms;
        self
    }

    /// Returns the maximum lifetime of the token in milliseconds.
    #[doc(alias = "org.apache.kafka.clients.admin.CreateDelegationTokenOptions#maxLifetimeMs")]
    pub fn max_lifetime_ms(&self) -> i64 {
        self.max_lifetime_ms
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
    /// This `CreateDelegationTokenOptions` instance.
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
    fn defaults() {
        let options = CreateDelegationTokenOptions::new();
        assert_eq!(options.max_lifetime_ms(), -1);
        assert!(options.renewers().is_empty());
        assert!(options.owner().is_none());
        assert_eq!(options.timeout_ms(), None);
    }

    #[test]
    fn setters() {
        let owner = KafkaPrincipal::new(KafkaPrincipal::USER_TYPE, "alice");
        let renewer = KafkaPrincipal::new(KafkaPrincipal::USER_TYPE, "bob");
        let options = CreateDelegationTokenOptions::new()
            .set_owner(owner.clone())
            .set_renewers(vec![renewer.clone()])
            .set_max_lifetime_ms(1000)
            .set_timeout_ms(Some(5000));
        assert_eq!(options.owner(), Some(&owner));
        assert_eq!(options.renewers(), &[renewer]);
        assert_eq!(options.max_lifetime_ms(), 1000);
        assert_eq!(options.timeout_ms(), Some(5000));
    }
}
