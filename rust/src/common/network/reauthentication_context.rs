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

//! Defines the context in which an [`Authenticator`](super::Authenticator) is
//! to operate when re-authenticating a channel.
//!
//! Translated from `org.apache.kafka.common.network.ReauthenticationContext`.

use super::NetworkReceive;

/// The context in which an [`Authenticator`](super::Authenticator) is to
/// operate when re-authenticating a channel.
///
/// Translated from `org.apache.kafka.common.network.ReauthenticationContext`.
///
/// Java's context also carries the `previousAuthenticator`, because Java's
/// `KafkaChannel` re-authenticates with a new authenticator built by its
/// `authenticatorCreator`, which reads the original `ApiVersionsResponse` out of
/// the previous one and closes it. This client's `KafkaChannel` holds one
/// authenticator for the life of the channel and re-authenticates with that
/// same instance, which already holds what Java reads from the previous one, so
/// there is no previous authenticator to pass.
#[doc(alias = "org.apache.kafka.common.network.ReauthenticationContext")]
pub struct ReauthenticationContext {
    network_receive: Option<NetworkReceive>,
    reauthentication_begin_nanos: i64,
}

impl ReauthenticationContext {
    /// Creates a context.
    ///
    /// `network_receive` is the applicable [`NetworkReceive`] instance, if any.
    /// For the client side this may be a response that has been partially read,
    /// an instance that has had no data read into it yet, or `None`; if it is
    /// `Some` then this is the instance that data should initially be read into
    /// during re-authentication. For the server side this is mandatory and it
    /// must contain the `SaslHandshakeRequest` that has been received on the
    /// server and that initiates re-authentication.
    ///
    /// `now_nanos` is the current time, in nanoseconds as per
    /// `Time::nanoseconds`, and is therefore only useful when compared to such a
    /// value; its absolute value is meaningless. It defines the moment when
    /// re-authentication begins.
    #[doc(alias = "org.apache.kafka.common.network.ReauthenticationContext#ReauthenticationContext")]
    pub fn new(network_receive: Option<NetworkReceive>, now_nanos: i64) -> Self {
        Self { network_receive, reauthentication_begin_nanos: now_nanos }
    }

    /// Returns the applicable [`NetworkReceive`] instance, if any, taking it out
    /// of the context: the authenticator reads into it from then on.
    #[doc(alias = "org.apache.kafka.common.network.ReauthenticationContext#networkReceive")]
    pub fn network_receive(&mut self) -> Option<NetworkReceive> {
        self.network_receive.take()
    }

    /// Returns the time when re-authentication began, in nanoseconds as per
    /// `Time::nanoseconds`.
    #[doc(alias = "org.apache.kafka.common.network.ReauthenticationContext#reauthenticationBeginNanos")]
    pub fn reauthentication_begin_nanos(&self) -> i64 {
        self.reauthentication_begin_nanos
    }
}

impl std::fmt::Debug for ReauthenticationContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReauthenticationContext")
            .field(
                "network_receive_bytes_read",
                &self.network_receive.as_ref().map(NetworkReceive::bytes_read),
            )
            .field("reauthentication_begin_nanos", &self.reauthentication_begin_nanos)
            .finish()
    }
}
