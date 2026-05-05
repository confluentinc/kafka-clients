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

//! Translation of `org.apache.kafka.common.security.authenticator.Authenticator`.
//!
//! Phase 5b-3 ships a minimal trait surface plus the two non-SASL impls
//! that the producer needs:
//!
//! * [`PlaintextAuthenticator`] — always complete, returns the
//!   [`KafkaPrincipal::anonymous()`] principal.
//! * [`SslAuthenticator`] — always complete (SSL authentication is done
//!   in the [`crate::common::network::SslTransportLayer`] handshake);
//!   returns the peer principal extracted from the certificate chain.
//!
//! `SaslClientAuthenticator`, re-authentication state (`reauthenticate`,
//! `serverSessionExpirationTimeNanos`, `clientSessionReauthenticationTimeNanos`,
//! `pollResponseReceivedDuringReauthentication`,
//! `connectedClientSupportsReauthentication`), and `KafkaPrincipalSerde`
//! integration are deferred to Phase 9. The trait surface here exposes
//! only the methods the Phase 5b-3 [`crate::common::network::KafkaChannel`]
//! actually calls (`authenticate`, `complete`, `principal`, `close`).

use std::io;

use crate::common::network::TransportLayer;
use crate::common::security::auth::KafkaPrincipal;

/// Pluggable authenticator. Mirrors the Java `Authenticator` interface
/// minus the SASL-specific methods (deferred to Phase 9).
///
/// Phase 5b-3 only calls `authenticate` (no-op for non-SASL),
/// `complete` (always `true` once the underlying transport handshake is
/// done), and `principal` (used by `KafkaChannel::principal()` for
/// logging / metric labels).
pub trait Authenticator: std::marker::Send {
    /// Implements authentication using the configured SASL mechanism. For
    /// the non-SASL authenticators in this phase, this is a no-op (TLS
    /// authentication is performed inside the transport's `handshake`).
    fn authenticate(&mut self) -> io::Result<()>;

    /// Returns the [`KafkaPrincipal`] derived for this connection. For
    /// PLAINTEXT, this is always [`KafkaPrincipal::anonymous`]; for SSL,
    /// this is the peer principal extracted from the certificate chain.
    fn principal(&self) -> KafkaPrincipal;

    /// Returns `true` when authentication has completed. The Phase 5b-3
    /// non-SASL authenticators are complete as soon as the transport
    /// handshake has finished.
    fn complete(&self) -> bool;

    /// Releases any resources held by the authenticator. Mirrors Java's
    /// `Closeable.close()` on the `Authenticator` interface.
    fn close(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// Plaintext authenticator. Mirrors the Java
/// `PlaintextChannelBuilder.PlaintextAuthenticator` inner class.
///
/// In Java the authenticator routes through a `KafkaPrincipalBuilder` to
/// produce a `KafkaPrincipal` from the connecting peer's
/// [`std::net::SocketAddr`] + listener name. The principal is only
/// meaningful on the broker side for ACL evaluation; on the client side
/// (the only side this codebase implements in Milestone 1) the principal
/// is unused outside of logging, so we return the anonymous singleton.
///
/// **Phase 9 note**: when the broker side is brought online, this
/// authenticator should accept a `ListenerName` + a `KafkaPrincipalBuilder`
/// trait object and construct the principal from the peer
/// [`std::net::SocketAddr`] + listener name as Java does.
pub struct PlaintextAuthenticator;

impl PlaintextAuthenticator {
    /// Construct the singleton-equivalent plaintext authenticator. There
    /// is no per-channel state on the client side.
    pub fn new() -> Self {
        PlaintextAuthenticator
    }
}

impl Default for PlaintextAuthenticator {
    fn default() -> Self {
        PlaintextAuthenticator::new()
    }
}

impl Authenticator for PlaintextAuthenticator {
    fn authenticate(&mut self) -> io::Result<()> {
        // Mirror Java: `authenticate()` is a no-op for plaintext.
        Ok(())
    }

    fn principal(&self) -> KafkaPrincipal {
        // Java's client-side `PlaintextAuthenticator.principal()` throws
        // `IllegalStateException` because `listenerName == null`. Rust's
        // client-only Milestone 1 returns the anonymous principal so
        // callers (logging, metrics) get a stable value rather than a
        // panic. The behavioural difference is only observable on a
        // server-side `KafkaChannel` which Milestone 1 does not ship.
        KafkaPrincipal::anonymous()
    }

    fn complete(&self) -> bool {
        // Mirror Java: plaintext authenticator is always complete.
        true
    }
}

/// SSL authenticator. Mirrors the Java
/// `SslChannelBuilder.SslAuthenticator` inner class — SSL authentication
/// is performed inside the transport's `handshake`; this authenticator
/// only surfaces the peer principal once the handshake is done.
///
/// The principal is captured eagerly at construction-after-handshake by
/// the channel builder; this struct only stores the captured value to
/// keep the public `Authenticator::principal` method lock-free.
pub struct SslAuthenticator {
    principal: KafkaPrincipal,
}

impl SslAuthenticator {
    /// Construct from a transport. The principal is fetched eagerly via
    /// [`TransportLayer::peer_principal`]; on plaintext the result is
    /// `KafkaPrincipal::anonymous()`. The constructor is fallible
    /// because `peer_principal` is `io::Result` for the SSL impl.
    pub fn new(transport: &dyn TransportLayer) -> io::Result<Self> {
        Ok(SslAuthenticator { principal: transport.peer_principal()? })
    }
}

impl Authenticator for SslAuthenticator {
    fn authenticate(&mut self) -> io::Result<()> {
        // Mirror Java: `authenticate()` is a no-op for SSL — the TLS
        // handshake is what authenticates the channel.
        Ok(())
    }

    fn principal(&self) -> KafkaPrincipal {
        self.principal.clone()
    }

    fn complete(&self) -> bool {
        // Mirror Java: SSL authenticator is always complete.
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plaintext_authenticator_is_anonymous_and_complete() {
        let mut auth = PlaintextAuthenticator::new();
        assert!(auth.complete(), "plaintext authenticator should be complete immediately");
        assert_eq!(auth.principal(), KafkaPrincipal::anonymous());
        auth.authenticate().expect("authenticate is a no-op");
        auth.close().expect("close is a no-op");
    }
}
