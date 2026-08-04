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

//! Options for `Admin::describe_delegation_token`.
//!
//! Corresponds to
//! `org.apache.kafka.clients.admin.DescribeDelegationTokenOptions`.

use crate::common::security::auth::KafkaPrincipal;

/// Options for `Admin::describe_delegation_token`.
///
/// Corresponds to
/// `org.apache.kafka.clients.admin.DescribeDelegationTokenOptions`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct DescribeDelegationTokenOptions {
    owners: Option<Vec<KafkaPrincipal>>,
    timeout_ms: Option<i32>,
}

impl DescribeDelegationTokenOptions {
    /// Creates default options (default API timeout, describe all authorized
    /// tokens).
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the owners to describe delegation tokens for.
    ///
    /// If owners is `None`, all the user-owned tokens and tokens where the user
    /// has Describe permission will be returned.
    ///
    /// Mirrors `DescribeDelegationTokenOptions.owners`.
    #[must_use]
    pub fn owners(mut self, owners: Option<Vec<KafkaPrincipal>>) -> Self {
        self.owners = owners;
        self
    }

    /// The owners to describe delegation tokens for, or `None` for all
    /// authorized tokens.
    ///
    /// Mirrors `DescribeDelegationTokenOptions.owners`.
    pub fn get_owners(&self) -> Option<&[KafkaPrincipal]> {
        self.owners.as_deref()
    }

    /// Set the timeout in milliseconds for this operation, or `None` to use the
    /// default API timeout for the `AdminClient`.
    #[must_use]
    pub fn timeout_ms(mut self, timeout_ms: Option<i32>) -> Self {
        self.timeout_ms = timeout_ms;
        self
    }

    /// The timeout in milliseconds for this operation, or `None` if the default
    /// API timeout should be used.
    pub fn timeout(&self) -> Option<i32> {
        self.timeout_ms
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults() {
        let options = DescribeDelegationTokenOptions::new();
        assert!(options.get_owners().is_none());
        assert_eq!(options.timeout(), None);
    }

    #[test]
    fn setters() {
        let owner = KafkaPrincipal::new(KafkaPrincipal::USER_TYPE, "alice");
        let options = DescribeDelegationTokenOptions::new()
            .owners(Some(vec![owner.clone()]))
            .timeout_ms(Some(5000));
        assert_eq!(options.get_owners(), Some([owner].as_slice()));
        assert_eq!(options.timeout(), Some(5000));
    }
}
