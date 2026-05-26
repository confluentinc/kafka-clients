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

//! Names of the methods on
//! [`crate::consumer::ConsumerRebalanceListener`], used for log messages and
//! for routing events between the app thread and the background thread.
//!
//! Translated from
//! `org.apache.kafka.clients.consumer.internals.ConsumerRebalanceListenerMethodName`.

use std::fmt;

/// Compile-time enumeration of the methods on
/// [`crate::consumer::ConsumerRebalanceListener`]. Used by the
/// `ConsumerRebalanceListenerCallbackNeededEvent` (bg → app) and
/// `ConsumerRebalanceListenerCallbackCompletedEvent` (app → bg) handshake.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ConsumerRebalanceListenerMethodName {
    /// Corresponds to `ConsumerRebalanceListener::on_partitions_revoked`.
    OnPartitionsRevoked,
    /// Corresponds to `ConsumerRebalanceListener::on_partitions_assigned`.
    OnPartitionsAssigned,
    /// Corresponds to `ConsumerRebalanceListener::on_partitions_lost`.
    OnPartitionsLost,
}

impl ConsumerRebalanceListenerMethodName {
    /// Returns the fully-qualified method name, e.g.
    /// `ConsumerRebalanceListener.onPartitionsRevoked`. Mirrors Java's
    /// `fullyQualifiedMethodName()` which is used in log messages.
    pub fn fully_qualified_method_name(&self) -> &'static str {
        match self {
            Self::OnPartitionsRevoked => "ConsumerRebalanceListener.onPartitionsRevoked",
            Self::OnPartitionsAssigned => "ConsumerRebalanceListener.onPartitionsAssigned",
            Self::OnPartitionsLost => "ConsumerRebalanceListener.onPartitionsLost",
        }
    }
}

impl fmt::Display for ConsumerRebalanceListenerMethodName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.fully_qualified_method_name())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fully_qualified_names_match_java() {
        assert_eq!(
            ConsumerRebalanceListenerMethodName::OnPartitionsRevoked.fully_qualified_method_name(),
            "ConsumerRebalanceListener.onPartitionsRevoked"
        );
        assert_eq!(
            ConsumerRebalanceListenerMethodName::OnPartitionsAssigned.fully_qualified_method_name(),
            "ConsumerRebalanceListener.onPartitionsAssigned"
        );
        assert_eq!(
            ConsumerRebalanceListenerMethodName::OnPartitionsLost.fully_qualified_method_name(),
            "ConsumerRebalanceListener.onPartitionsLost"
        );
    }

    #[test]
    fn display_matches_fully_qualified_name() {
        let m = ConsumerRebalanceListenerMethodName::OnPartitionsRevoked;
        assert_eq!(format!("{}", m), "ConsumerRebalanceListener.onPartitionsRevoked");
    }
}
