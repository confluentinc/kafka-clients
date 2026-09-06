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
//!
//! Corresponds to
//! `org.apache.kafka.clients.admin.CreateDelegationTokenOptions`.

use crate::common::security::auth::KafkaPrincipal;

/// Options for `Admin::create_delegation_token`.
///
/// Corresponds to
/// `org.apache.kafka.clients.admin.CreateDelegationTokenOptions`.
#[derive(Clone, Debug, PartialEq, Eq)]
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
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the principals allowed to renew the token.
    ///
    /// Mirrors `CreateDelegationTokenOptions.renewers`.
    #[must_use]
    pub fn renewers(mut self, renewers: Vec<KafkaPrincipal>) -> Self {
        self.renewers = renewers;
        self
    }

    /// The principals allowed to renew the token.
    ///
    /// Mirrors `CreateDelegationTokenOptions.renewers`.
    pub fn get_renewers(&self) -> &[KafkaPrincipal] {
        &self.renewers
    }

    /// Sets the owner of the token.
    ///
    /// Mirrors `CreateDelegationTokenOptions.owner`.
    #[must_use]
    pub fn owner(mut self, owner: KafkaPrincipal) -> Self {
        self.owner = Some(owner);
        self
    }

    /// The owner of the token, if set.
    ///
    /// Mirrors `CreateDelegationTokenOptions.owner`, which returns an
    /// `Optional`.
    pub fn get_owner(&self) -> Option<&KafkaPrincipal> {
        self.owner.as_ref()
    }

    /// Sets the maximum lifetime of the token in milliseconds, or `-1` to use
    /// the server-side default.
    ///
    /// Mirrors `CreateDelegationTokenOptions.maxLifetimeMs`.
    #[must_use]
    pub fn max_lifetime_ms(mut self, max_lifetime_ms: i64) -> Self {
        self.max_lifetime_ms = max_lifetime_ms;
        self
    }

    /// The maximum lifetime of the token in milliseconds.
    ///
    /// Mirrors `CreateDelegationTokenOptions.maxLifetimeMs`.
    pub fn get_max_lifetime_ms(&self) -> i64 {
        self.max_lifetime_ms
    }

    /// Sets the maximum lifetime of the token in milliseconds.
    ///
    /// Mirrors the deprecated `CreateDelegationTokenOptions.maxlifeTimeMs(long)`
    /// (deprecated since 4.0; use [`Self::max_lifetime_ms`]).
    #[deprecated(note = "use max_lifetime_ms")]
    #[must_use]
    pub fn maxlife_time_ms(self, max_lifetime_ms: i64) -> Self {
        self.max_lifetime_ms(max_lifetime_ms)
    }

    /// The maximum lifetime of the token in milliseconds.
    ///
    /// Mirrors the deprecated `CreateDelegationTokenOptions.maxlifeTimeMs()`
    /// (deprecated since 4.0; use [`Self::get_max_lifetime_ms`]).
    #[deprecated(note = "use get_max_lifetime_ms")]
    pub fn get_maxlife_time_ms(&self) -> i64 {
        self.max_lifetime_ms
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
    fn defaults() {
        let options = CreateDelegationTokenOptions::new();
        assert_eq!(options.get_max_lifetime_ms(), -1);
        assert!(options.get_renewers().is_empty());
        assert!(options.get_owner().is_none());
        assert_eq!(options.timeout_ms(), None);
    }

    #[test]
    fn setters() {
        let owner = KafkaPrincipal::new(KafkaPrincipal::USER_TYPE, "alice");
        let renewer = KafkaPrincipal::new(KafkaPrincipal::USER_TYPE, "bob");
        let options = CreateDelegationTokenOptions::new()
            .owner(owner.clone())
            .renewers(vec![renewer.clone()])
            .max_lifetime_ms(1000)
            .set_timeout_ms(Some(5000));
        assert_eq!(options.get_owner(), Some(&owner));
        assert_eq!(options.get_renewers(), &[renewer]);
        assert_eq!(options.get_max_lifetime_ms(), 1000);
        assert_eq!(options.timeout_ms(), Some(5000));
    }
}
