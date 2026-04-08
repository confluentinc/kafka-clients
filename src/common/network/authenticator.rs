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

//! Authentication for Kafka channels.
//!
//! Translated from `org.apache.kafka.common.network.Authenticator`.
//!
//! For security protocols PLAINTEXT and SSL, authentication is a no-op since no
//! further authentication needs to be done. For SASL_PLAINTEXT and SASL_SSL,
//! this would perform the SASL authentication.

use super::network_receive::NetworkReceive;

use std::io;

/// Authentication interface for Kafka channels.
///
/// Translated from the Java `Authenticator` interface.
///
/// Re-authentication methods have default no-op implementations since they only
/// apply to SASL connections. The `principal` and `principal_serde` methods from
/// the Java interface are omitted because they require `KafkaPrincipal` and
/// `KafkaPrincipalBuilder` which are server-side constructs not needed for the
/// client-only PLAINTEXT path.
pub trait Authenticator: Send {
    /// Implements any authentication mechanism.
    ///
    /// For security protocols PLAINTEXT and SSL, this is a no-op.
    /// For SASL_PLAINTEXT and SASL_SSL, this performs the SASL authentication.
    ///
    /// # Errors
    ///
    /// Returns an error if authentication fails due to invalid credentials or
    /// other security configuration errors, or if read/write fails due to an
    /// I/O error.
    fn authenticate(&mut self) -> io::Result<()>;

    /// Perform any processing related to authentication failure.
    ///
    /// This is invoked when the channel is about to be closed because of an
    /// authentication error thrown from a prior `authenticate()` call.
    ///
    /// Default implementation is a no-op.
    fn handle_authentication_failure(&mut self) -> io::Result<()> {
        Ok(())
    }

    /// Returns `true` if authentication is complete.
    fn complete(&self) -> bool;

    /// Begins re-authentication.
    ///
    /// For PLAINTEXT and SSL, this is a no-op since re-authentication does not
    /// apply/is not supported.
    ///
    /// Default implementation is a no-op.
    fn reauthenticate(&mut self) -> io::Result<()> {
        Ok(())
    }

    /// Return the session expiration time, if any.
    ///
    /// The value is in nanoseconds as per `Instant`. This value may be non-null
    /// only on the server-side.
    ///
    /// Default implementation returns `None`.
    fn server_session_expiration_time_nanos(&self) -> Option<u64> {
        None
    }

    /// Return the time on or after which a client should re-authenticate this
    /// session, if any.
    ///
    /// The value is in nanoseconds. This value may be non-null only on the
    /// client-side.
    ///
    /// Default implementation returns `None`.
    fn client_session_reauthentication_time_nanos(&self) -> Option<u64> {
        None
    }

    /// Return the number of milliseconds that elapsed while re-authenticating
    /// this session from the perspective of this instance, if applicable.
    ///
    /// Default implementation returns `None`.
    fn reauthentication_latency_ms(&self) -> Option<u64> {
        None
    }

    /// Return the next client-side `NetworkReceive` response that arrived during
    /// re-authentication that is unrelated to re-authentication, if any.
    ///
    /// Default implementation returns `None`.
    fn poll_response_received_during_reauthentication(&mut self) -> Option<NetworkReceive> {
        None
    }

    /// Return `true` if this is a server-side authenticator and the connected
    /// client has indicated that it supports re-authentication.
    ///
    /// Default implementation returns `false`.
    fn connected_client_supports_reauthentication(&self) -> bool {
        false
    }

    /// Close the authenticator and release any resources.
    fn close(&mut self);
}

/// Plaintext authenticator that performs no authentication.
///
/// This corresponds to the inner `PlaintextAuthenticator` class in
/// `PlaintextChannelBuilder.java`. For PLAINTEXT connections, authentication
/// is a no-op since data is sent and received without encryption or
/// authentication.
pub struct PlaintextAuthenticator;

impl PlaintextAuthenticator {
    /// Creates a new `PlaintextAuthenticator`.
    pub fn new() -> Self {
        Self
    }
}

impl Default for PlaintextAuthenticator {
    fn default() -> Self {
        Self::new()
    }
}

impl Authenticator for PlaintextAuthenticator {
    fn authenticate(&mut self) -> io::Result<()> {
        // no-op for plaintext
        Ok(())
    }

    fn complete(&self) -> bool {
        true
    }

    fn close(&mut self) {
        // no-op
    }
}
