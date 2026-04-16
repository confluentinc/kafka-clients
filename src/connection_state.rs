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

//! The states of a node connection.
//!
//! Translated from `org.apache.kafka.clients.ConnectionState`.

/// The states of a node connection.
///
/// - `Disconnected`: connection has not been successfully established yet
/// - `Connecting`: connection is under progress
/// - `CheckingApiVersions`: connection has been established and api versions check is in progress.
///   Failure of this check will cause connection to close.
/// - `Ready`: connection is ready to send requests
/// - `AuthenticationFailed`: connection failed due to an authentication error
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ConnectionState {
    Disconnected,
    Connecting,
    CheckingApiVersions,
    Ready,
    AuthenticationFailed,
}

impl ConnectionState {
    /// Returns `true` if the connection is in a disconnected state
    /// (either explicitly disconnected or authentication failed).
    pub fn is_disconnected(&self) -> bool {
        matches!(self, Self::AuthenticationFailed | Self::Disconnected)
    }

    /// Returns `true` if the connection is in a connected state
    /// (either checking API versions or ready).
    pub fn is_connected(&self) -> bool {
        matches!(self, Self::CheckingApiVersions | Self::Ready)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_disconnected_states() {
        assert!(ConnectionState::Disconnected.is_disconnected());
        assert!(ConnectionState::AuthenticationFailed.is_disconnected());
        assert!(!ConnectionState::Connecting.is_disconnected());
        assert!(!ConnectionState::CheckingApiVersions.is_disconnected());
        assert!(!ConnectionState::Ready.is_disconnected());
    }

    #[test]
    fn test_connected_states() {
        assert!(ConnectionState::CheckingApiVersions.is_connected());
        assert!(ConnectionState::Ready.is_connected());
        assert!(!ConnectionState::Disconnected.is_connected());
        assert!(!ConnectionState::Connecting.is_connected());
        assert!(!ConnectionState::AuthenticationFailed.is_connected());
    }
}
