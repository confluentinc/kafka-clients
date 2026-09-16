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

//! Channel state tracking for [`KafkaChannel`](super::KafkaChannel).
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

use crate::common::Error;

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
            State::NotConnected => write!(f, "ChannelState::NOT_CONNECTED"),
            State::Authenticate => write!(f, "ChannelState::AUTHENTICATE"),
            State::Ready => write!(f, "ChannelState::READY"),
            State::Expired => write!(f, "ChannelState::EXPIRED"),
            State::FailedSend => write!(f, "ChannelState::FAILED_SEND"),
            State::AuthenticationFailed => write!(f, "AUTHENTICATION_FAILED"),
            State::LocalClose => write!(f, "ChannelState::LOCAL_CLOSE"),
        }
    }
}

/// Channel state with optional error and remote address information.
///
/// For `AuthenticationFailed`, the error describes the failure reason.
/// For other states, reusable constants are provided.
///
/// Java stores the *object*: `private final AuthenticationException exception`
/// (`ChannelState.java:76`), and `NetworkClient.processDisconnection` hands it
/// on to `metadataUpdater.handleServerDisconnect(.., Optional<AuthenticationException>)`
/// unchanged, so the application ultimately catches the very
/// `SaslAuthenticationException` / `SslAuthenticationException` the channel
/// raised. Storing a rendered string here instead would lose the class — and
/// therefore `is_authentication_error()`, `request_utils::RequestUtils::is_fatal_error` and the
/// wire code — and would bake the `Display` class prefix into what every
/// downstream caller treats as the *message*.
#[derive(Debug, Clone)]
pub struct ChannelState {
    state: State,
    /// The error, if any (used for authentication failures).
    error: Option<Error>,
    /// The remote address string, if known.
    remote_address: Option<String>,
}

// `Error` has no `PartialEq` (Java's `Throwable` has no `equals` either), but
// `ChannelState` is compared against the reusable constants below and in tests,
// so equality is defined structurally: same state, same remote address, and the
// same rendered error (`Throwable.toString()`, i.e. class + message).
impl PartialEq for ChannelState {
    fn eq(&self, other: &Self) -> bool {
        self.state == other.state
            && self.remote_address == other.remote_address
            && self.error.as_ref().map(Error::to_string) == other.error.as_ref().map(Error::to_string)
    }
}

impl Eq for ChannelState {}

impl ChannelState {
    /// Not connected state.
    pub const NOT_CONNECTED: ChannelState =
        ChannelState { state: State::NotConnected, error: None, remote_address: None };

    /// Authenticate state.
    pub const AUTHENTICATE: ChannelState =
        ChannelState { state: State::Authenticate, error: None, remote_address: None };

    /// Ready state.
    pub const READY: ChannelState = ChannelState { state: State::Ready, error: None, remote_address: None };

    /// Expired state.
    pub const EXPIRED: ChannelState = ChannelState { state: State::Expired, error: None, remote_address: None };

    /// Failed send state.
    pub const FAILED_SEND: ChannelState = ChannelState { state: State::FailedSend, error: None, remote_address: None };

    /// Local close state.
    pub const LOCAL_CLOSE: ChannelState = ChannelState { state: State::LocalClose, error: None, remote_address: None };

    /// Creates a new `ChannelState` with the given state, no error, and no remote address.
    pub fn new(state: State) -> Self {
        Self { state, error: None, remote_address: None }
    }

    /// Creates a new `ChannelState` with the given state and remote address.
    pub fn new_remote_address(state: State, remote_address: &str) -> Self {
        Self { state, error: None, remote_address: Some(remote_address.to_string()) }
    }

    /// Creates a new `ChannelState` with the given state, error, and remote address.
    pub fn new_error_remote_address(state: State, error: Error, remote_address: Option<&str>) -> Self {
        Self { state, error: Some(error), remote_address: remote_address.map(|s| s.to_string()) }
    }

    /// Returns the state.
    pub fn state(&self) -> State {
        self.state
    }

    /// Returns the error, if any (Java's `ChannelState.exception()`).
    pub fn error(&self) -> Option<&Error> {
        self.error.as_ref()
    }

    /// Returns the remote address, if known.
    pub fn remote_address(&self) -> Option<&str> {
        self.remote_address.as_deref()
    }
}

// Reusable constants for common states (matching Java's static final fields)

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
