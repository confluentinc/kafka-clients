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

//! Translation of `org.apache.kafka.clients.ConnectionState`.
//!
//! Note: the Java type lives in `org.apache.kafka.clients` (not `common`),
//! so it sits at the crate root rather than under `common::`.

/// The states of a node connection.
///
/// * `Disconnected` — connection has not been successfully established yet.
/// * `Connecting` — connection is under progress.
/// * `CheckingApiVersions` — connection has been established and api
///   versions check is in progress. Failure of this check will cause the
///   connection to close.
/// * `Ready` — connection is ready to send requests.
/// * `AuthenticationFailed` — connection failed due to an authentication
///   error.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionState {
    /// Mirrors `ConnectionState.DISCONNECTED`.
    Disconnected,
    /// Mirrors `ConnectionState.CONNECTING`.
    Connecting,
    /// Mirrors `ConnectionState.CHECKING_API_VERSIONS`.
    CheckingApiVersions,
    /// Mirrors `ConnectionState.READY`.
    Ready,
    /// Mirrors `ConnectionState.AUTHENTICATION_FAILED`.
    AuthenticationFailed,
}

impl ConnectionState {
    /// Mirrors `ConnectionState.isDisconnected()`.
    ///
    /// Returns true when the connection is in either the
    /// [`ConnectionState::Disconnected`] or
    /// [`ConnectionState::AuthenticationFailed`] state.
    pub fn is_disconnected(self) -> bool {
        matches!(self, ConnectionState::Disconnected | ConnectionState::AuthenticationFailed)
    }

    /// Mirrors `ConnectionState.isConnected()`.
    ///
    /// Returns true when the connection is in either the
    /// [`ConnectionState::CheckingApiVersions`] or [`ConnectionState::Ready`]
    /// state.
    pub fn is_connected(self) -> bool {
        matches!(self, ConnectionState::CheckingApiVersions | ConnectionState::Ready)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `ConnectionState.java` has no dedicated test class; the predicates
    /// are exercised through `ClusterConnectionStatesTest`. Keeping a tiny
    /// unit check for the predicate truth table mirrors the Java enum's
    /// behaviour and pins drift if the variant set ever changes.
    #[test]
    fn predicate_truth_table() {
        assert!(ConnectionState::Disconnected.is_disconnected());
        assert!(ConnectionState::AuthenticationFailed.is_disconnected());
        assert!(!ConnectionState::Connecting.is_disconnected());
        assert!(!ConnectionState::CheckingApiVersions.is_disconnected());
        assert!(!ConnectionState::Ready.is_disconnected());

        assert!(ConnectionState::CheckingApiVersions.is_connected());
        assert!(ConnectionState::Ready.is_connected());
        assert!(!ConnectionState::Disconnected.is_connected());
        assert!(!ConnectionState::Connecting.is_connected());
        assert!(!ConnectionState::AuthenticationFailed.is_connected());
    }
}
