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

//! Options for `Admin::list_transactions`.
//!
//! Corresponds to `org.apache.kafka.clients.admin.ListTransactionsOptions`.

use std::collections::HashSet;

use crate::admin::TransactionState;

/// Options for `Admin::list_transactions`.
///
/// Corresponds to `org.apache.kafka.clients.admin.ListTransactionsOptions`.
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct ListTransactionsOptions {
    timeout_ms: Option<i32>,
    filtered_states: HashSet<TransactionState>,
    filtered_producer_ids: HashSet<i64>,
    filtered_duration_ms: i64,
    filtered_transactional_id_pattern: Option<String>,
}

impl ListTransactionsOptions {
    /// Creates default options: no filters, and `filtered_duration_ms == -1`
    /// (no duration filtering), matching Java's defaults.
    pub fn new() -> Self {
        Self { filtered_duration_ms: -1, ..Default::default() }
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

    /// Filter only the transactions that are in a specific set of states.
    ///
    /// Mirrors `filterStates`.
    #[must_use]
    pub fn filter_states(mut self, states: impl IntoIterator<Item = TransactionState>) -> Self {
        self.filtered_states = states.into_iter().collect();
        self
    }

    /// Filter only the transactions from producers in a specific set of
    /// producer ids.
    ///
    /// Mirrors `filterProducerIds`.
    #[must_use]
    pub fn filter_producer_ids(mut self, producer_ids: impl IntoIterator<Item = i64>) -> Self {
        self.filtered_producer_ids = producer_ids.into_iter().collect();
        self
    }

    /// Filter only the transactions running longer than the specified duration.
    ///
    /// Mirrors `filterOnDuration`.
    #[must_use]
    pub fn filter_on_duration(mut self, duration_ms: i64) -> Self {
        self.filtered_duration_ms = duration_ms;
        self
    }

    /// Filter only the transactions matching the given transactional id pattern.
    ///
    /// Mirrors `filterOnTransactionalIdPattern`.
    #[must_use]
    pub fn filter_on_transactional_id_pattern(mut self, pattern: Option<String>) -> Self {
        self.filtered_transactional_id_pattern = pattern;
        self
    }

    /// The set of states being filtered (empty means no state filter).
    ///
    /// Mirrors `filteredStates`.
    pub fn filtered_states(&self) -> &HashSet<TransactionState> {
        &self.filtered_states
    }

    /// The set of producer ids being filtered (empty means no producer id filter).
    ///
    /// Mirrors `filteredProducerIds`.
    pub fn filtered_producer_ids(&self) -> &HashSet<i64> {
        &self.filtered_producer_ids
    }

    /// The duration filter in milliseconds (negative means no duration filter).
    ///
    /// Mirrors `filteredDuration`.
    pub fn filtered_duration(&self) -> i64 {
        self.filtered_duration_ms
    }

    /// The transactional id pattern being filtered, if any.
    ///
    /// Mirrors `filteredTransactionalIdPattern`.
    pub fn filtered_transactional_id_pattern(&self) -> Option<&str> {
        self.filtered_transactional_id_pattern.as_deref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_match_java() {
        let options = ListTransactionsOptions::new();
        assert_eq!(options.timeout_ms(), None);
        assert!(options.filtered_states().is_empty());
        assert!(options.filtered_producer_ids().is_empty());
        assert_eq!(options.filtered_duration(), -1);
        assert_eq!(options.filtered_transactional_id_pattern(), None);
    }

    #[test]
    fn fluent_filters() {
        let options = ListTransactionsOptions::new()
            .filter_states([TransactionState::Ongoing])
            .filter_producer_ids([23423])
            .filter_on_duration(10)
            .filter_on_transactional_id_pattern(Some("^special-.*".to_string()));
        assert_eq!(options.filtered_states(), &HashSet::from([TransactionState::Ongoing]));
        assert_eq!(options.filtered_producer_ids(), &HashSet::from([23423]));
        assert_eq!(options.filtered_duration(), 10);
        assert_eq!(options.filtered_transactional_id_pattern(), Some("^special-.*"));
    }
}
