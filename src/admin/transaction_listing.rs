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

//! A transaction listing, as reported by `ListTransactions`.
//!
//! Corresponds to `org.apache.kafka.clients.admin.TransactionListing`.

use crate::admin::transaction_state::TransactionState;

/// A transaction listing.
///
/// Corresponds to `org.apache.kafka.clients.admin.TransactionListing`.
/// `producer_id` is `i64` (Java `long`).
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct TransactionListing {
    transactional_id: String,
    producer_id: i64,
    transaction_state: TransactionState,
}

impl TransactionListing {
    /// Creates a new `TransactionListing`.
    pub fn new(transactional_id: impl Into<String>, producer_id: i64, transaction_state: TransactionState) -> Self {
        Self { transactional_id: transactional_id.into(), producer_id, transaction_state }
    }

    /// The transactional id of the transaction.
    pub fn transactional_id(&self) -> &str {
        &self.transactional_id
    }

    /// The producer id of the transaction.
    pub fn producer_id(&self) -> i64 {
        self.producer_id
    }

    /// The current state of the transaction.
    pub fn state(&self) -> TransactionState {
        self.transaction_state
    }
}

impl std::fmt::Display for TransactionListing {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "TransactionListing(transactionalId='{}', producerId={}, transactionState={})",
            self.transactional_id, self.producer_id, self.transaction_state,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accessors_and_equality() {
        let a = TransactionListing::new("foo", 12345, TransactionState::Ongoing);
        assert_eq!(a.transactional_id(), "foo");
        assert_eq!(a.producer_id(), 12345);
        assert_eq!(a.state(), TransactionState::Ongoing);
        assert_eq!(a, TransactionListing::new("foo", 12345, TransactionState::Ongoing));
        assert_ne!(a, TransactionListing::new("foo", 12345, TransactionState::Empty));
    }
}
