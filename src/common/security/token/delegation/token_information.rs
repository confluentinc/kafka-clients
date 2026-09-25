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

//! Delegation token details.
//!
//! Corresponds to
//! `org.apache.kafka.common.security.token.delegation.TokenInformation`.

use std::fmt;
use std::hash::{Hash, Hasher};

use crate::common::security::auth::KafkaPrincipal;

/// A class representing delegation token details.
///
/// Corresponds to
/// `org.apache.kafka.common.security.token.delegation.TokenInformation`.
#[derive(Debug, Clone)]
pub struct TokenInformation {
    owner: KafkaPrincipal,
    token_requester: KafkaPrincipal,
    renewers: Vec<KafkaPrincipal>,
    issue_timestamp: i64,
    max_timestamp: i64,
    expiry_timestamp: i64,
    token_id: String,
}

impl TokenInformation {
    /// Creates token information where the owner is also the requester.
    ///
    /// Mirrors the `TokenInformation(tokenId, owner, renewers, ...)`
    /// constructor.
    pub fn new(
        token_id: impl Into<String>,
        owner: KafkaPrincipal,
        renewers: Vec<KafkaPrincipal>,
        issue_timestamp: i64,
        max_timestamp: i64,
        expiry_timestamp: i64,
    ) -> Self {
        let owner_clone = owner.clone();
        Self::with_token_requester(
            token_id,
            owner,
            owner_clone,
            renewers,
            issue_timestamp,
            max_timestamp,
            expiry_timestamp,
        )
    }

    /// Creates token information with a distinct token requester.
    ///
    /// Mirrors the
    /// `TokenInformation(tokenId, owner, tokenRequester, renewers, ...)`
    /// constructor.
    pub fn with_token_requester(
        token_id: impl Into<String>,
        owner: KafkaPrincipal,
        token_requester: KafkaPrincipal,
        renewers: Vec<KafkaPrincipal>,
        issue_timestamp: i64,
        max_timestamp: i64,
        expiry_timestamp: i64,
    ) -> Self {
        Self {
            token_id: token_id.into(),
            owner,
            token_requester,
            renewers,
            issue_timestamp,
            max_timestamp,
            expiry_timestamp,
        }
    }

    /// Returns the token owner.
    ///
    /// Mirrors `TokenInformation.owner`.
    pub fn owner(&self) -> &KafkaPrincipal {
        &self.owner
    }

    /// Returns the token owner as a `type:name` string.
    ///
    /// Mirrors `TokenInformation.ownerAsString`.
    pub fn owner_as_string(&self) -> String {
        self.owner.to_string()
    }

    /// Returns the token requester.
    ///
    /// Mirrors `TokenInformation.tokenRequester`.
    pub fn token_requester(&self) -> &KafkaPrincipal {
        &self.token_requester
    }

    /// Returns the token requester as a `type:name` string.
    ///
    /// Mirrors `TokenInformation.tokenRequesterAsString`.
    pub fn token_requester_as_string(&self) -> String {
        self.token_requester.to_string()
    }

    /// Returns the renewers.
    ///
    /// Mirrors `TokenInformation.renewers`.
    pub fn renewers(&self) -> &[KafkaPrincipal] {
        &self.renewers
    }

    /// Returns the renewers as `type:name` strings.
    ///
    /// Mirrors `TokenInformation.renewersAsString`.
    pub fn renewers_as_string(&self) -> Vec<String> {
        self.renewers.iter().map(KafkaPrincipal::to_string).collect()
    }

    /// Returns the issue timestamp.
    ///
    /// Mirrors `TokenInformation.issueTimestamp`.
    pub fn issue_timestamp(&self) -> i64 {
        self.issue_timestamp
    }

    /// Returns the expiry timestamp.
    ///
    /// Mirrors `TokenInformation.expiryTimestamp`.
    pub fn expiry_timestamp(&self) -> i64 {
        self.expiry_timestamp
    }

    /// Sets the expiry timestamp.
    ///
    /// Mirrors `TokenInformation.setExpiryTimestamp`.
    pub fn set_expiry_timestamp(&mut self, expiry_timestamp: i64) {
        self.expiry_timestamp = expiry_timestamp;
    }

    /// Returns the token id.
    ///
    /// Mirrors `TokenInformation.tokenId`.
    pub fn token_id(&self) -> &str {
        &self.token_id
    }

    /// Returns the maximum timestamp.
    ///
    /// Mirrors `TokenInformation.maxTimestamp`.
    pub fn max_timestamp(&self) -> i64 {
        self.max_timestamp
    }

    /// Returns `true` if the given principal is the owner, the token requester,
    /// or one of the renewers.
    ///
    /// Mirrors `TokenInformation.ownerOrRenewer`.
    pub fn owner_or_renewer(&self, principal: &KafkaPrincipal) -> bool {
        &self.owner == principal || &self.token_requester == principal || self.renewers.contains(principal)
    }
}

impl fmt::Display for TokenInformation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "TokenInformation{{owner={}, tokenRequester={}, renewers={:?}, issueTimestamp={}, maxTimestamp={}, \
             expiryTimestamp={}, tokenId='{}'}}",
            self.owner,
            self.token_requester,
            self.renewers.iter().map(KafkaPrincipal::to_string).collect::<Vec<_>>(),
            self.issue_timestamp,
            self.max_timestamp,
            self.expiry_timestamp,
            self.token_id,
        )
    }
}

// Java's `equals` compares owner, tokenRequester, renewers, issueTimestamp,
// maxTimestamp, and tokenId — but NOT expiryTimestamp. Java's `hashCode`
// additionally hashes expiryTimestamp, which is inconsistent with `equals`.
// We faithfully translate `equals`, and implement `Hash` over the SAME fields
// as `eq` (excluding expiryTimestamp) so Rust's `Eq`/`Hash` contract holds
// (equal values hash equally). This is a deliberate deviation from Java's
// buggy `hashCode`; `TokenInformation` is never used as a hash-map key in the
// delegation-token flow, so the deviation is not observable.
impl PartialEq for TokenInformation {
    fn eq(&self, other: &Self) -> bool {
        self.issue_timestamp == other.issue_timestamp
            && self.max_timestamp == other.max_timestamp
            && self.owner == other.owner
            && self.token_requester == other.token_requester
            && self.renewers == other.renewers
            && self.token_id == other.token_id
    }
}

impl Eq for TokenInformation {}

impl Hash for TokenInformation {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.owner.hash(state);
        self.token_requester.hash(state);
        self.renewers.hash(state);
        self.issue_timestamp.hash(state);
        self.max_timestamp.hash(state);
        self.token_id.hash(state);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn user(name: &str) -> KafkaPrincipal {
        KafkaPrincipal::new(KafkaPrincipal::USER_TYPE, name)
    }

    #[test]
    fn owner_is_default_requester() {
        let info = TokenInformation::new("id", user("alice"), vec![user("bob")], 1, 100, 50);
        assert_eq!(info.owner(), &user("alice"));
        assert_eq!(info.token_requester(), &user("alice"));
        assert_eq!(info.owner_as_string(), "User:alice");
        assert_eq!(info.token_requester_as_string(), "User:alice");
    }

    #[test]
    fn renewers_as_string_lists_all() {
        let info = TokenInformation::new("id", user("alice"), vec![user("bob"), user("carol")], 1, 100, 50);
        assert_eq!(
            info.renewers_as_string(),
            vec!["User:bob".to_string(), "User:carol".to_string()]
        );
    }

    #[test]
    fn owner_or_renewer_matches_owner_requester_and_renewers() {
        let info = TokenInformation::with_token_requester(
            "id",
            user("alice"),
            user("requester"),
            vec![user("bob")],
            1,
            100,
            50,
        );
        assert!(info.owner_or_renewer(&user("alice")));
        assert!(info.owner_or_renewer(&user("requester")));
        assert!(info.owner_or_renewer(&user("bob")));
        assert!(!info.owner_or_renewer(&user("mallory")));
    }

    #[test]
    fn set_expiry_timestamp_updates_field() {
        let mut info = TokenInformation::new("id", user("alice"), vec![], 1, 100, 50);
        info.set_expiry_timestamp(75);
        assert_eq!(info.expiry_timestamp(), 75);
    }

    #[test]
    fn equals_ignores_expiry_timestamp() {
        let a = TokenInformation::new("id", user("alice"), vec![user("bob")], 1, 100, 50);
        let b = TokenInformation::new("id", user("alice"), vec![user("bob")], 1, 100, 999);
        assert_eq!(a, b);
    }

    #[test]
    fn differs_by_token_id() {
        let a = TokenInformation::new("id-a", user("alice"), vec![], 1, 100, 50);
        let b = TokenInformation::new("id-b", user("alice"), vec![], 1, 100, 50);
        assert_ne!(a, b);
    }
}
