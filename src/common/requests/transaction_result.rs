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

//! The outcome a transaction is being driven towards.

/// Whether a transaction is being committed or aborted.
///
/// Translated from `org.apache.kafka.common.requests.TransactionResult`.
///
/// The wire representation is a single boolean (the `committed` field of
/// `EndTxnRequest`), so this maps to/from `bool` via [`Self::id`] and
/// [`Self::for_id`] rather than an integer code.
///
/// Variant order matches Java's declaration order (`ABORT`, `COMMIT`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TransactionResult {
    /// Abort the transaction; `id` is `false` on the wire.
    Abort,
    /// Commit the transaction; `id` is `true` on the wire.
    Commit,
}

impl TransactionResult {
    /// The wire value for this result.
    ///
    /// Corresponds to Java's public `id` field.
    pub fn id(&self) -> bool {
        match self {
            Self::Abort => false,
            Self::Commit => true,
        }
    }

    /// The result for a wire value.
    ///
    /// Corresponds to Java's `forId(boolean)`.
    pub fn for_id(id: bool) -> Self {
        if id { Self::Commit } else { Self::Abort }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_id() {
        assert!(!TransactionResult::Abort.id());
        assert!(TransactionResult::Commit.id());
    }

    #[test]
    fn test_for_id() {
        assert_eq!(TransactionResult::for_id(false), TransactionResult::Abort);
        assert_eq!(TransactionResult::for_id(true), TransactionResult::Commit);
    }

    #[test]
    fn test_round_trip() {
        for result in [TransactionResult::Abort, TransactionResult::Commit] {
            assert_eq!(TransactionResult::for_id(result.id()), result);
        }
    }
}
