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

//! Channel state tracking for [`KafkaChannel`](super::kafka_channel::KafkaChannel).
//!
//! Translated from `org.apache.kafka.common.network.ChannelState`.
//!
//! States for KafkaChannel:
//! - `NotConnected`: Connections are created in this state. State is updated on
//!   `TransportLayer::finish_connect()` when socket connection is established.
//!   PLAINTEXT channels transition from NotConnected to Ready, others transition
//!   to Authenticate.
//! - `Authenticate`: SSL, SASL_SSL and SASL_PLAINTEXT channels are in this state
//!   during SSL and SASL handshake.
//! - `Ready`: Connected, authenticated channels are in this state.
//! - `Expired`: Idle connections are moved to this state on idle timeout.
//! - `FailedSend`: Channels transition from Ready to FailedSend if closed due to
//!   a send failure.
//! - `AuthenticationFailed`: Channels are moved to this state if the requested SASL
//!   mechanism is not enabled in the broker or when brokers provide an error response
//!   during SASL authentication.
//! - `LocalClose`: Channels are moved to this state if close() is initiated locally.
//!
//! Typical transitions:
//! - PLAINTEXT Good path: NotConnected => Ready => LocalClose
//! - SASL/SSL Good path: NotConnected => Authenticate => Ready => LocalClose
//! - Bootstrap server misconfiguration: NotConnected, disconnected in NotConnected state
//! - Security misconfiguration: NotConnected => Authenticate => AuthenticationFailed

use std::fmt;

/// The state enum for a Kafka channel.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum State {
    /// Connections are created in this state.
    NotConnected,
    /// SSL/SASL handshake in progress.
    Authenticate,
    /// Connected and authenticated.
    Ready,
    /// Idle connection expired.
    Expired,
    /// Send failure.
    FailedSend,
    /// Authentication failed.
    AuthenticationFailed,
    /// Locally initiated close.
    LocalClose,
}

impl fmt::Display for State {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            State::NotConnected => write!(f, "NOT_CONNECTED"),
            State::Authenticate => write!(f, "AUTHENTICATE"),
            State::Ready => write!(f, "READY"),
            State::Expired => write!(f, "EXPIRED"),
            State::FailedSend => write!(f, "FAILED_SEND"),
            State::AuthenticationFailed => write!(f, "AUTHENTICATION_FAILED"),
            State::LocalClose => write!(f, "LOCAL_CLOSE"),
        }
    }
}

/// Channel state with optional error and remote address information.
///
/// For `AuthenticationFailed`, the error message describes the failure reason.
/// For other states, reusable constants are provided.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChannelState {
    state: State,
    /// The error message, if any (used for authentication failures).
    error: Option<String>,
    /// The remote address string, if known.
    remote_address: Option<String>,
}

impl ChannelState {
    /// Creates a new `ChannelState` with the given state, no error, and no remote address.
    pub fn new(state: State) -> Self {
        Self { state, error: None, remote_address: None }
    }

    /// Creates a new `ChannelState` with the given state and remote address.
    pub fn with_remote_address(state: State, remote_address: &str) -> Self {
        Self { state, error: None, remote_address: Some(remote_address.to_string()) }
    }

    /// Creates a new `ChannelState` with the given state, error message, and remote address.
    pub fn with_error(state: State, error: &str, remote_address: Option<&str>) -> Self {
        Self {
            state,
            error: Some(error.to_string()),
            remote_address: remote_address.map(|s| s.to_string()),
        }
    }

    /// Returns the state.
    pub fn state(&self) -> State {
        self.state
    }

    /// Returns the error message, if any.
    pub fn error(&self) -> Option<&str> {
        self.error.as_deref()
    }

    /// Returns the remote address, if known.
    pub fn remote_address(&self) -> Option<&str> {
        self.remote_address.as_deref()
    }
}

// Reusable constants for common states (matching Java's static final fields)

/// Not connected state.
pub const NOT_CONNECTED: ChannelState = ChannelState { state: State::NotConnected, error: None, remote_address: None };
/// Authenticate state.
pub const AUTHENTICATE: ChannelState = ChannelState { state: State::Authenticate, error: None, remote_address: None };
/// Ready state.
pub const READY: ChannelState = ChannelState { state: State::Ready, error: None, remote_address: None };
/// Expired state.
pub const EXPIRED: ChannelState = ChannelState { state: State::Expired, error: None, remote_address: None };
/// Failed send state.
pub const FAILED_SEND: ChannelState = ChannelState { state: State::FailedSend, error: None, remote_address: None };
/// Local close state.
pub const LOCAL_CLOSE: ChannelState = ChannelState { state: State::LocalClose, error: None, remote_address: None };

impl fmt::Display for ChannelState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ChannelState(state={}", self.state)?;
        if let Some(ref err) = self.error {
            write!(f, ", error={err}")?;
        }
        if let Some(ref addr) = self.remote_address {
            write!(f, ", remoteAddress={addr}")?;
        }
        write!(f, ")")
    }
}
