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

//! The state of a transaction, as reported by `DescribeTransactions`.
//!
//! Corresponds to `org.apache.kafka.clients.admin.TransactionState`.

/// The state of a transaction.
///
/// Corresponds to `org.apache.kafka.clients.admin.TransactionState`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum TransactionState {
    /// A transaction is in progress.
    Ongoing,
    /// The transaction is being aborted.
    PrepareAbort,
    /// The transaction is being committed.
    PrepareCommit,
    /// The transaction has been aborted.
    CompleteAbort,
    /// The transaction has been committed.
    CompleteCommit,
    /// No transaction is in progress.
    Empty,
    /// The producer epoch is being fenced.
    PrepareEpochFence,
    /// The transaction state could not be recognized.
    Unknown,
}

impl TransactionState {
    /// The wire/display name of this state (mirrors the Java enum's `name`
    /// field and `toString`).
    fn name(&self) -> &'static str {
        match self {
            TransactionState::Ongoing => "Ongoing",
            TransactionState::PrepareAbort => "PrepareAbort",
            TransactionState::PrepareCommit => "PrepareCommit",
            TransactionState::CompleteAbort => "CompleteAbort",
            TransactionState::CompleteCommit => "CompleteCommit",
            TransactionState::Empty => "Empty",
            TransactionState::PrepareEpochFence => "PrepareEpochFence",
            TransactionState::Unknown => "Unknown",
        }
    }

    /// Parses a state name, returning [`TransactionState::Unknown`] for any
    /// unrecognized value.
    ///
    /// Mirrors `TransactionState.parse`.
    pub fn parse(name: &str) -> TransactionState {
        match name {
            "Ongoing" => TransactionState::Ongoing,
            "PrepareAbort" => TransactionState::PrepareAbort,
            "PrepareCommit" => TransactionState::PrepareCommit,
            "CompleteAbort" => TransactionState::CompleteAbort,
            "CompleteCommit" => TransactionState::CompleteCommit,
            "Empty" => TransactionState::Empty,
            "PrepareEpochFence" => TransactionState::PrepareEpochFence,
            _ => TransactionState::Unknown,
        }
    }
}

impl std::fmt::Display for TransactionState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_round_trips_known_states() {
        for state in [
            TransactionState::Ongoing,
            TransactionState::PrepareAbort,
            TransactionState::PrepareCommit,
            TransactionState::CompleteAbort,
            TransactionState::CompleteCommit,
            TransactionState::Empty,
            TransactionState::PrepareEpochFence,
            TransactionState::Unknown,
        ] {
            assert_eq!(TransactionState::parse(&state.to_string()), state);
        }
    }

    #[test]
    fn parse_unknown_name_is_unknown() {
        assert_eq!(TransactionState::parse("NotAState"), TransactionState::Unknown);
    }

    #[test]
    fn display_matches_java() {
        assert_eq!(TransactionState::CompleteCommit.to_string(), "CompleteCommit");
        assert_eq!(TransactionState::PrepareEpochFence.to_string(), "PrepareEpochFence");
    }
}
