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
/// minus the SASL-specific methods.
///
/// Phase 5b-3 only calls `authenticate` (no-op for non-SASL),
/// `complete` (always `true` once the underlying transport handshake is
/// done), and `principal` (used by `KafkaChannel::principal()` for
/// logging / metric labels).
///
/// `principal` takes a `&dyn TransportLayer` so SSL implementations can
/// look up the peer principal lazily (post-handshake). Java's
/// `SslAuthenticator` stores the transport reference and re-queries it
/// every call; the Rust trait re-receives the reference from the owning
/// channel because the authenticator does not own the transport.
///
/// **Phase 9b note (architectural):** SASL authenticators require a
/// `&mut dyn TransportLayer` in `authenticate()` to issue/read SASL
/// frames during the handshake — Java accomplishes this by having the
/// SASL authenticator hold the transport reference internally, but the
/// Rust translation has the channel own the transport so an extra
/// argument is needed. Rather than reshape this trait (and touch every
/// `PlaintextAuthenticator` / `SslAuthenticator` call site), Phase 9b
/// introduces a sibling [`SaslAuthenticator`] trait. The
/// [`ChannelAuthenticator`] enum then carries either kind, and
/// [`KafkaChannel`](crate::common::network::KafkaChannel) dispatches
/// through the enum.
pub trait Authenticator: std::marker::Send {
    /// Implements authentication using the configured SASL mechanism. For
    /// the non-SASL authenticators in this phase, this is a no-op (TLS
    /// authentication is performed inside the transport's `handshake`).
    fn authenticate(&mut self) -> io::Result<()>;

    /// Returns the [`KafkaPrincipal`] derived for this connection. For
    /// PLAINTEXT, this is always [`KafkaPrincipal::anonymous`]; for SSL,
    /// this is the peer principal extracted from the certificate chain
    /// at the time of the call (lazy — mirrors Java's
    /// `SslAuthenticator.principal()` which reads
    /// `transportLayer.sslSession()` on every invocation).
    fn principal(&self, transport: &dyn TransportLayer) -> KafkaPrincipal;

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

/// SASL-specific authenticator trait. Mirrors the Java
/// `Authenticator` interface's `authenticate()` method when used by
/// `SaslClientAuthenticator` — but in Rust, `authenticate()` carries an
/// explicit `&mut dyn TransportLayer` because the authenticator does not
/// own the transport (the channel does).
///
/// This is a sibling trait to [`Authenticator`] — the two are kept
/// separate so that:
/// 1. The shared [`Authenticator`] trait (used by `PlaintextAuthenticator`
///    and `SslAuthenticator`) does not need to grow a transport-bearing
///    `authenticate` signature, which would force every impl to plumb
///    transport into `authenticate` even though only SASL uses it.
/// 2. Implementations like [`crate::common::security::authenticator::SaslClientAuthenticator`]
///    can keep their natural Rust signature
///    (`authenticate(&mut self, transport: &mut dyn TransportLayer)`)
///    without the indirection of holding a transport reference inside
///    the authenticator (which would force unsafe lifetime/borrow
///    juggling for an authenticator that lives alongside the transport
///    inside the same `KafkaChannel`).
///
/// The [`ChannelAuthenticator`] enum dispatches across the two trait
/// types at the channel level.
pub trait SaslAuthenticator: std::marker::Send {
    /// Drive the SASL state machine forward, issuing/reading SASL frames
    /// on `transport` as needed. Mirrors
    /// `org.apache.kafka.common.security.authenticator.SaslClientAuthenticator.authenticate()`.
    fn authenticate(&mut self, transport: &mut dyn TransportLayer) -> io::Result<()>;

    /// Returns the [`KafkaPrincipal`] derived for this SASL session.
    /// For PLAIN that is the authenticated username; the Phase 9b
    /// translation returns [`KafkaPrincipal::anonymous`] until the
    /// principal-extraction wiring lands (server-side concern).
    fn principal(&self, transport: &dyn TransportLayer) -> KafkaPrincipal;

    /// Returns `true` when the SASL handshake has completed successfully.
    fn complete(&self) -> bool;

    /// Releases any resources held by the authenticator.
    fn close(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// Channel-side authenticator wrapper. Owned by
/// [`KafkaChannel`](crate::common::network::KafkaChannel) and dispatches
/// to either a non-SASL [`Authenticator`] (Plaintext / SSL) or a SASL
/// [`SaslAuthenticator`].
///
/// Mirrors Java's polymorphic `Authenticator` interface where every
/// concrete subclass implements the same `authenticate()` method — but
/// without forcing the Rust [`Authenticator`] trait to carry a
/// transport reference for cases that don't need one. The enum is the
/// "downcasting bridge" so the channel can write a single
/// `auth.authenticate(transport)` call site (see
/// [`Self::authenticate`]) regardless of which kind of authenticator
/// is in use.
pub enum ChannelAuthenticator {
    /// Plaintext or SSL authenticator (non-SASL — `authenticate()` is a
    /// no-op for these).
    Network(Box<dyn Authenticator>),
    /// SASL authenticator (needs a transport reference to drive the
    /// SASL frame exchange).
    Sasl(Box<dyn SaslAuthenticator>),
}

impl ChannelAuthenticator {
    /// Wrap a non-SASL [`Authenticator`] (Plaintext or SSL).
    pub fn network<A: Authenticator + 'static>(auth: A) -> Self {
        ChannelAuthenticator::Network(Box::new(auth))
    }

    /// Wrap a SASL [`SaslAuthenticator`].
    pub fn sasl<A: SaslAuthenticator + 'static>(auth: A) -> Self {
        ChannelAuthenticator::Sasl(Box::new(auth))
    }

    /// Drive authentication forward. The transport reference is only
    /// used by the SASL variant (the network variant ignores it,
    /// matching Java's no-op `authenticate()` for non-SASL).
    pub fn authenticate(&mut self, transport: &mut dyn TransportLayer) -> io::Result<()> {
        match self {
            ChannelAuthenticator::Network(a) => a.authenticate(),
            ChannelAuthenticator::Sasl(a) => a.authenticate(transport),
        }
    }

    /// Returns the principal derived for this connection. Forwards to
    /// the inner authenticator's `principal()`.
    pub fn principal(&self, transport: &dyn TransportLayer) -> KafkaPrincipal {
        match self {
            ChannelAuthenticator::Network(a) => a.principal(transport),
            ChannelAuthenticator::Sasl(a) => a.principal(transport),
        }
    }

    /// Returns `true` when authentication has completed.
    pub fn complete(&self) -> bool {
        match self {
            ChannelAuthenticator::Network(a) => a.complete(),
            ChannelAuthenticator::Sasl(a) => a.complete(),
        }
    }

    /// Releases authenticator-owned resources.
    pub fn close(&mut self) -> io::Result<()> {
        match self {
            ChannelAuthenticator::Network(a) => a.close(),
            ChannelAuthenticator::Sasl(a) => a.close(),
        }
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

    fn principal(&self, _transport: &dyn TransportLayer) -> KafkaPrincipal {
        // Java's client-side `PlaintextAuthenticator.principal()` throws
        // `IllegalStateException` because `listenerName == null`. Rust's
        // client-only Milestone 1 returns the anonymous principal so
        // callers (logging, metrics) get a stable value rather than a
        // panic. The behavioural difference is only observable on a
        // server-side `KafkaChannel` which Milestone 1 does not ship.
        // The transport ref is unused for plaintext.
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
/// The struct is stateless (mirroring Java's lazy lookup pattern):
/// `principal()` reads the peer principal off the supplied transport on
/// every call. Caching at construction would be wrong because the
/// constructor runs *before* the TLS handshake completes, so the peer
/// certificate is not yet available — Java avoids this by holding only
/// a transport reference and reading `sslSession()` on demand.
pub struct SslAuthenticator;

impl SslAuthenticator {
    /// Construct an `SslAuthenticator`. There is no per-channel state
    /// on the client side — Java's `principalBuilder`/`listenerName`
    /// fields are used only on the server (Phase 9).
    pub fn new() -> Self {
        SslAuthenticator
    }
}

impl Default for SslAuthenticator {
    fn default() -> Self {
        SslAuthenticator::new()
    }
}

impl Authenticator for SslAuthenticator {
    fn authenticate(&mut self) -> io::Result<()> {
        // Mirror Java: `authenticate()` is a no-op for SSL — the TLS
        // handshake is what authenticates the channel.
        Ok(())
    }

    fn principal(&self, transport: &dyn TransportLayer) -> KafkaPrincipal {
        // Lazy lookup mirrors Java's `SslAuthenticator.principal()`:
        // every call re-reads the SSL session, so post-handshake calls
        // see the certificate Subject DN even when pre-handshake calls
        // would have returned anonymous. `peer_principal` returns
        // anonymous when the handshake is incomplete or when no peer
        // cert was presented, matching Java's `SSLPeerUnverifiedException`
        // catch path.
        transport.peer_principal().unwrap_or_else(|_| KafkaPrincipal::anonymous())
    }

    fn complete(&self) -> bool {
        // Mirror Java: SSL authenticator is always complete.
        true
    }
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;
    use std::io::IoSlice;
    use std::net::SocketAddr;

    use crate::common::network::TransferableChannel;

    use super::*;

    /// Stub transport that returns a different principal depending on
    /// `handshake_complete` — used to verify lazy-lookup semantics on
    /// `SslAuthenticator::principal`.
    struct StubTransport {
        handshake_complete: Cell<bool>,
    }

    impl StubTransport {
        fn new() -> Self {
            StubTransport { handshake_complete: Cell::new(false) }
        }
    }

    impl TransferableChannel for StubTransport {
        fn write_vectored(&mut self, _bufs: &[IoSlice<'_>]) -> io::Result<usize> {
            Ok(0)
        }
        fn has_pending_writes(&self) -> bool {
            false
        }
    }

    impl TransportLayer for StubTransport {
        fn ready(&self) -> bool {
            true
        }
        fn finish_connect(&mut self) -> io::Result<bool> {
            Ok(true)
        }
        fn disconnect(&mut self) {}
        fn is_connected(&self) -> bool {
            true
        }
        fn is_open(&self) -> bool {
            true
        }
        fn close(&mut self) -> io::Result<()> {
            Ok(())
        }
        fn read(&mut self, _dst: &mut [u8]) -> io::Result<usize> {
            Ok(0)
        }
        fn handshake(&mut self) -> io::Result<()> {
            Ok(())
        }
        fn peer_principal(&self) -> io::Result<KafkaPrincipal> {
            // Pre-handshake: anonymous (Java would throw
            // SSLPeerUnverifiedException). Post-handshake: a stable
            // synthetic identity to prove the value is fetched lazily.
            if self.handshake_complete.get() {
                Ok(KafkaPrincipal::new("User", "CN=test"))
            } else {
                Ok(KafkaPrincipal::anonymous())
            }
        }
        fn add_interest_ops(&mut self, _ops: i32) {}
        fn remove_interest_ops(&mut self, _ops: i32) {}
        fn interest_ops(&self) -> i32 {
            0
        }
        fn is_mute(&self) -> bool {
            false
        }
        fn has_bytes_buffered(&self) -> bool {
            false
        }
        fn local_addr(&self) -> io::Result<SocketAddr> {
            Ok(SocketAddr::from(([127, 0, 0, 1], 0)))
        }
        fn peer_addr(&self) -> io::Result<SocketAddr> {
            Ok(SocketAddr::from(([127, 0, 0, 1], 9093)))
        }
    }

    #[test]
    fn plaintext_authenticator_is_anonymous_and_complete() {
        let mut auth = PlaintextAuthenticator::new();
        let transport = StubTransport::new();
        assert!(auth.complete(), "plaintext authenticator should be complete immediately");
        assert_eq!(auth.principal(&transport), KafkaPrincipal::anonymous());
        auth.authenticate().expect("authenticate is a no-op");
        auth.close().expect("close is a no-op");
    }

    /// `SslAuthenticator::principal` must re-query the transport on
    /// every call so the post-handshake principal is observed (mirrors
    /// Java's lazy `transportLayer.sslSession()` lookup).
    #[test]
    fn ssl_authenticator_principal_is_lazy() {
        let mut auth = SslAuthenticator::new();
        let transport = StubTransport::new();
        // Pre-handshake call: anonymous, just like Java's
        // `SSLPeerUnverifiedException` → fall back to anonymous.
        assert_eq!(auth.principal(&transport), KafkaPrincipal::anonymous());
        // Flip the stub's handshake state and re-query.
        transport.handshake_complete.set(true);
        assert_eq!(auth.principal(&transport), KafkaPrincipal::new("User", "CN=test"));
        auth.authenticate().expect("authenticate is a no-op");
        auth.close().expect("close is a no-op");
        assert!(auth.complete());
    }

    // ============================================================
    // ChannelAuthenticator enum dispatch (Phase 9b)
    // ============================================================

    /// Counting authenticator that records each call into a shared
    /// counter so the dispatch path can be observed independently of
    /// the side-effects of the real authenticators.
    struct CountingNetworkAuthenticator {
        authenticate_calls: std::sync::Arc<std::sync::atomic::AtomicUsize>,
        principal_calls: std::sync::Arc<std::sync::atomic::AtomicUsize>,
        complete_calls: std::sync::Arc<std::sync::atomic::AtomicUsize>,
        close_calls: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    }

    impl Authenticator for CountingNetworkAuthenticator {
        fn authenticate(&mut self) -> io::Result<()> {
            self.authenticate_calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(())
        }
        fn principal(&self, _transport: &dyn TransportLayer) -> KafkaPrincipal {
            self.principal_calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            KafkaPrincipal::anonymous()
        }
        fn complete(&self) -> bool {
            self.complete_calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            true
        }
        fn close(&mut self) -> io::Result<()> {
            self.close_calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(())
        }
    }

    /// Counting SASL authenticator — captures that the transport
    /// reference is actually threaded through.
    struct CountingSaslAuthenticator {
        authenticate_calls: std::sync::Arc<std::sync::atomic::AtomicUsize>,
        complete_after: usize,
    }

    impl SaslAuthenticator for CountingSaslAuthenticator {
        fn authenticate(&mut self, _transport: &mut dyn TransportLayer) -> io::Result<()> {
            self.authenticate_calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(())
        }
        fn principal(&self, _transport: &dyn TransportLayer) -> KafkaPrincipal {
            KafkaPrincipal::anonymous()
        }
        fn complete(&self) -> bool {
            self.authenticate_calls.load(std::sync::atomic::Ordering::SeqCst) >= self.complete_after
        }
    }

    /// The [`ChannelAuthenticator::Network`] variant routes
    /// `authenticate()` (ignoring transport), `principal()`,
    /// `complete()`, and `close()` to the inner non-SASL impl.
    #[test]
    fn channel_authenticator_network_variant_dispatches_to_inner() {
        let authenticate_calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let principal_calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let complete_calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let close_calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let mut wrapped = ChannelAuthenticator::network(CountingNetworkAuthenticator {
            authenticate_calls: std::sync::Arc::clone(&authenticate_calls),
            principal_calls: std::sync::Arc::clone(&principal_calls),
            complete_calls: std::sync::Arc::clone(&complete_calls),
            close_calls: std::sync::Arc::clone(&close_calls),
        });
        let mut transport = StubTransport::new();

        // authenticate(transport) — transport is ignored for the network variant.
        wrapped.authenticate(&mut transport).expect("authenticate");
        // principal(transport) — transport is passed through to the inner.
        let _ = wrapped.principal(&transport);
        // complete() — no transport argument.
        assert!(wrapped.complete());
        // close()
        wrapped.close().expect("close");

        assert_eq!(authenticate_calls.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert_eq!(principal_calls.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert_eq!(complete_calls.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert_eq!(close_calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    }

    /// The [`ChannelAuthenticator::Sasl`] variant must thread the
    /// transport reference through to the inner SASL impl's
    /// `authenticate(&mut dyn TransportLayer)` — the whole point of
    /// the enum.
    #[test]
    fn channel_authenticator_sasl_variant_threads_transport() {
        let authenticate_calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let mut wrapped = ChannelAuthenticator::sasl(CountingSaslAuthenticator {
            authenticate_calls: std::sync::Arc::clone(&authenticate_calls),
            complete_after: 2,
        });
        let mut transport = StubTransport::new();

        // First authenticate: not yet complete.
        assert!(!wrapped.complete());
        wrapped.authenticate(&mut transport).expect("step 1");
        assert!(!wrapped.complete());
        // Second authenticate: complete now.
        wrapped.authenticate(&mut transport).expect("step 2");
        assert!(wrapped.complete());

        assert_eq!(authenticate_calls.load(std::sync::atomic::Ordering::SeqCst), 2);
    }

    /// Ensure both variants are constructable and the enum exhausts its
    /// match arms — guards against future regressions if a third
    /// variant is added without updating dispatch.
    #[test]
    fn channel_authenticator_construction_smoke() {
        let _net = ChannelAuthenticator::network(PlaintextAuthenticator::new());
        let _net2 = ChannelAuthenticator::network(SslAuthenticator::new());

        // Phase 9b: SaslAuthenticator construction smoke (uses the
        // counting stub since we don't want to pull in the full
        // SaslClientAuthenticator constructor here).
        let auth = CountingSaslAuthenticator {
            authenticate_calls: std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            complete_after: 1,
        };
        let _sasl = ChannelAuthenticator::sasl(auth);
    }
}
