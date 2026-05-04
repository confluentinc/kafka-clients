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

//! Translation of `org.apache.kafka.common.network.ChannelState`.

use crate::common::errors::KafkaError;

/// Inner state names for [`ChannelState`]. Mirrors the Java `ChannelState.State`
/// enum.
///
/// State machine summary (from the Java javadoc):
///   * `NotConnected` — created in this state. PLAINTEXT transitions to
///     `Ready`; SSL/SASL transitions to `Authenticate`.
///   * `Authenticate` — SSL handshake or SASL authentication in progress.
///   * `Ready` — connected and authenticated; healthy.
///   * `Expired` — moved here on idle timeout.
///   * `FailedSend` — moved here on send-side failure from `Ready`.
///   * `AuthenticationFailed` — moved here when SASL authentication is
///     refused by the broker. The associated [`KafkaError::Authentication`]
///     carries the failure reason.
///   * `LocalClose` — moved here when `close()` is initiated locally.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ChannelStateName {
    NotConnected,
    Authenticate,
    Ready,
    Expired,
    FailedSend,
    AuthenticationFailed,
    LocalClose,
}

/// State of a [`crate::common::network::Send`]'s underlying channel.
/// Mirrors the Java `ChannelState` value type.
///
/// Java uses six static singletons (`NOT_CONNECTED`, `AUTHENTICATE`,
/// `READY`, `EXPIRED`, `FAILED_SEND`, `LOCAL_CLOSE`) plus a constructor
/// path for `AUTHENTICATION_FAILED` that carries an
/// `AuthenticationException`. We reproduce the same shape with
/// constructors and the `not_connected()` / `ready()` / etc. constants.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChannelState {
    state: ChannelStateName,
    /// Set on `AuthenticationFailed`; `None` otherwise. Mirrors Java's
    /// `AuthenticationException exception`.
    exception: Option<KafkaError>,
    remote_address: Option<String>,
}

impl ChannelState {
    /// Construct a `ChannelState` with no exception or remote address.
    /// Mirrors `new ChannelState(State state)`.
    pub fn new(state: ChannelStateName) -> Self {
        ChannelState { state, exception: None, remote_address: None }
    }

    /// Mirrors `new ChannelState(State state, String remoteAddress)`.
    pub fn with_remote_address(state: ChannelStateName, remote_address: impl Into<String>) -> Self {
        ChannelState { state, exception: None, remote_address: Some(remote_address.into()) }
    }

    /// Mirrors `new ChannelState(State, AuthenticationException, String)`.
    /// The exception is required to be an authentication failure.
    pub fn with_exception(
        state: ChannelStateName,
        exception: KafkaError,
        remote_address: Option<String>,
    ) -> Self {
        ChannelState { state, exception: Some(exception), remote_address }
    }

    /// Mirrors `ChannelState.NOT_CONNECTED`.
    pub fn not_connected() -> Self {
        Self::new(ChannelStateName::NotConnected)
    }

    /// Mirrors `ChannelState.AUTHENTICATE`.
    pub fn authenticate() -> Self {
        Self::new(ChannelStateName::Authenticate)
    }

    /// Mirrors `ChannelState.READY`.
    pub fn ready() -> Self {
        Self::new(ChannelStateName::Ready)
    }

    /// Mirrors `ChannelState.EXPIRED`.
    pub fn expired() -> Self {
        Self::new(ChannelStateName::Expired)
    }

    /// Mirrors `ChannelState.FAILED_SEND`.
    pub fn failed_send() -> Self {
        Self::new(ChannelStateName::FailedSend)
    }

    /// Mirrors `ChannelState.LOCAL_CLOSE`.
    pub fn local_close() -> Self {
        Self::new(ChannelStateName::LocalClose)
    }

    /// Mirrors `ChannelState.state()`.
    pub fn state(&self) -> ChannelStateName {
        self.state
    }

    /// Mirrors `ChannelState.exception()`.
    pub fn exception(&self) -> Option<&KafkaError> {
        self.exception.as_ref()
    }

    /// Mirrors `ChannelState.remoteAddress()`.
    pub fn remote_address(&self) -> Option<&str> {
        self.remote_address.as_deref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn singleton_constructors_carry_state() {
        assert_eq!(ChannelState::not_connected().state(), ChannelStateName::NotConnected);
        assert_eq!(ChannelState::authenticate().state(), ChannelStateName::Authenticate);
        assert_eq!(ChannelState::ready().state(), ChannelStateName::Ready);
        assert_eq!(ChannelState::expired().state(), ChannelStateName::Expired);
        assert_eq!(ChannelState::failed_send().state(), ChannelStateName::FailedSend);
        assert_eq!(ChannelState::local_close().state(), ChannelStateName::LocalClose);
    }

    #[test]
    fn no_exception_or_address_by_default() {
        let s = ChannelState::ready();
        assert!(s.exception().is_none());
        assert!(s.remote_address().is_none());
    }

    #[test]
    fn with_remote_address_stores_address() {
        let s = ChannelState::with_remote_address(ChannelStateName::NotConnected, "127.0.0.1:9092");
        assert_eq!(s.remote_address(), Some("127.0.0.1:9092"));
        assert!(s.exception().is_none());
    }

    #[test]
    fn with_exception_stores_authentication_failure() {
        let err = KafkaError::Authentication("invalid credentials".into());
        let s = ChannelState::with_exception(
            ChannelStateName::AuthenticationFailed,
            err.clone(),
            Some("127.0.0.1:9092".to_owned()),
        );
        assert_eq!(s.state(), ChannelStateName::AuthenticationFailed);
        assert_eq!(s.exception(), Some(&err));
        assert_eq!(s.remote_address(), Some("127.0.0.1:9092"));
    }
}
