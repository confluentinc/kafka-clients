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

//! Typed authentication error carried across the transport/authenticator
//! [`io::Result`] boundary.
//!
//! Java classifies handshake failures by **exception type**:
//! `SSLException` / `AuthenticationException` are fatal authentication
//! failures (terminate without retries), whereas a plain `IOException`
//! (e.g. a TCP connection-reset during the TLS handshake) is a retriable
//! network disconnect. See `SslTransportLayer.handshake()`,
//! `KafkaChannel.prepare()` (which only catches `AuthenticationException`),
//! and `Selector` (which routes by `instanceof`).
//!
//! The Rust transport/authenticator surface returns [`io::Error`], whose
//! [`io::ErrorKind`] cannot faithfully carry "this was a genuine
//! authentication failure" — distinct failure modes collapse onto the same
//! kind (e.g. `ErrorKind::Other`). To preserve Java's "type, not kind"
//! distinction, genuine authentication failures (TLS certificate/protocol
//! rejection surfaced by rustls, SASL mechanism/credential rejection) are
//! stored as the crate's typed [`Error`] inside the [`io::Error`] payload via
//! [`authentication_io_error`]. The payload keeps the Java class the failure
//! was raised as — `SaslAuthenticationException` for rejected credentials,
//! `UnsupportedSaslMechanismException`, `IllegalSaslStateException`,
//! `SslAuthenticationException` — so it reaches the application unchanged, as
//! Java's `KafkaChannel` stores the exception object it caught. Downstream
//! callers ([`KafkaChannel::prepare`](super::KafkaChannel::prepare) and the
//! [`Selector`](super::Selector)) recover it with [`authentication_error`].
//!
//! Transient transport-level I/O errors (connection reset, broken pipe,
//! unexpected EOF) are NOT wrapped — their original [`io::ErrorKind`] is
//! preserved so the selector / network client treat them as a network
//! disconnect (retriable, reconnect with backoff), exactly as Java does.
//!
//! # Why these stay free functions
//!
//! The refactor that moved Java statics onto the struct translating their Java
//! class does not reach this file: there is no Java class here to translate.
//! Java expresses all of this through the exception **type** (`SSLException` /
//! `AuthenticationException` vs. a plain `IOException`), so the functions
//! below have no Java static to mirror and no Java class to host them. Inventing
//! a unit struct purely to carry them would add a type absent from the Java
//! codebase (`definition-of-done.md` §7) and buy nothing: the natural host would
//! be [`AuthenticationError`], which lives in `common::errors` and is the
//! *payload*, not the subject — [`is_authentication_error`] and
//! [`authentication_error`] both interrogate an [`io::Error`].
//!
//! They are reached through the `common::network` re-export, as CLAUDE.md §2
//! requires, so this module can still become private without touching them.
//!
//! Note [`is_authentication_error`] here takes `&io::Error` and is **not** the
//! CLAUDE.md §12.4 hierarchy predicate `Error::is_authentication_error()`,
//! although it answers through it.

use std::io;

use crate::common::Error;
use crate::common::errors::AuthenticationError;

/// Wraps a typed authentication failure as an [`io::Error`] whose payload is
/// `error` itself, so callers can recover it, class included, via
/// [`authentication_error`].
///
/// `error` is expected to answer `true` to
/// [`Error::is_authentication_error`]: its Java class extends
/// `AuthenticationException`. Any other error stored in an [`io::Error`] is
/// not an authentication failure for [`is_authentication_error`], exactly as
/// Java's `KafkaChannel.prepare` lets anything but an
/// `AuthenticationException` through to the selector as a disconnect.
///
/// The [`io::ErrorKind`] is set to [`io::ErrorKind::Other`]; classification
/// downstream is driven by the typed payload (mirroring Java's `instanceof`),
/// NOT by the kind.
pub fn authentication_io_error(error: Error) -> io::Error {
    debug_assert!(error.is_authentication_error(), "not an authentication error: {error}");
    io::Error::other(error)
}

/// Wraps an authentication failure raised as the base class, Java's
/// `AuthenticationException(String)`, as an [`io::Error`]: see
/// [`authentication_io_error`]. A failure Java raises as a subclass is built
/// as that subclass and passed to [`authentication_io_error`] instead.
pub fn auth_io_error(message: impl Into<String>) -> io::Error {
    authentication_io_error(Error::Authentication(AuthenticationError::new(message)))
}

/// The typed authentication failure `e` carries, if any: the [`Error`] stored
/// by [`authentication_io_error`], or `None` when `e` is not an
/// authentication failure (a transient I/O error, or a payload whose class
/// does not extend `AuthenticationException`).
///
/// This is the Rust equivalent of Java's
/// `catch (AuthenticationException e)` in `KafkaChannel.prepare()`, which
/// keeps the caught object.
pub fn authentication_error(e: &io::Error) -> Option<&Error> {
    e.get_ref()
        .and_then(|inner| inner.downcast_ref::<Error>())
        .filter(|error| error.is_authentication_error())
}

/// Returns `true` if `e` carries an authentication failure, i.e. the error is
/// a genuine authentication failure rather than a transient network
/// disconnect.
///
/// This is the Rust equivalent of Java's
/// `e instanceof AuthenticationException` check in `KafkaChannel.prepare()`
/// and `Selector`.
pub fn is_authentication_error(e: &io::Error) -> bool {
    authentication_error(e).is_some()
}

/// Like [`auth_io_error`], but carrying the error that caused the failure.
///
/// Translates the two-argument `AuthenticationException(String, Throwable)`
/// constructors. Java's `Throwable(String, Throwable)` sets `detailMessage`
/// from the *message*, so the cause is reachable through `getCause()` and does
/// NOT appear in the message text — unlike `Throwable(Throwable)`, which sets
/// `detailMessage = cause.toString()`.
pub fn auth_io_error_with_source(message: impl Into<String>, source: Error) -> io::Error {
    authentication_io_error(Error::Authentication(AuthenticationError::with_source(message, source)))
}

/// The bare message of the authentication failure `e` carries, if any.
///
/// `io::Error`'s `Display` delegates to the payload, whose own `Display` is Java's
/// `toString()` form (`"AuthenticationError: <message>"`). A caller rebuilding a
/// typed error must therefore use this rather than `e.to_string()`, or the prefix
/// is applied twice and the application sees
/// `"AuthenticationError: AuthenticationError: <reason>"`. Java rethrows the
/// exception object itself (`NetworkClientUtils#awaitReady`), so the message is
/// untouched; prefer [`authentication_error`], which hands over that object.
pub fn authentication_error_message(e: &io::Error) -> Option<&str> {
    authentication_error(e).map(Error::message)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn auth_io_error_is_recognized() {
        let e = auth_io_error("bad credentials");
        assert!(is_authentication_error(&e));
        // Display delegates to the payload, whose `Display` is Java's `toString()`
        // form (`<ClassName>: <message>`) like every other error in the crate.
        assert_eq!(e.to_string(), "AuthenticationError: bad credentials");
        // Kind is Other; classification must not rely on the kind.
        assert_eq!(e.kind(), io::ErrorKind::Other);
    }

    #[test]
    fn plain_io_error_is_not_auth() {
        let reset = io::Error::new(io::ErrorKind::ConnectionReset, "Connection reset by peer (os error 104)");
        assert!(!is_authentication_error(&reset));

        // Even an ErrorKind::Other without an AuthenticationError payload must
        // not be misclassified as an auth failure (the old ErrorKind heuristic
        // bug: it treated all Other as auth).
        let other = io::Error::other("TLS handshake failed: Connection reset by peer");
        assert!(!is_authentication_error(&other));
    }

    #[test]
    fn would_block_is_not_auth() {
        let wb = io::Error::from(io::ErrorKind::WouldBlock);
        assert!(!is_authentication_error(&wb));
    }

    /// The class the failure was raised as survives the carrier: Java's
    /// `KafkaChannel` keeps the `AuthenticationException` object it caught, so a
    /// rejected password stays a `SaslAuthenticationException` (code 58) all the
    /// way to the application.
    #[test]
    fn authentication_subclass_survives_the_carrier() {
        use crate::common::errors::{
            IllegalSaslStateError, SaslAuthenticationError, SslAuthenticationError, UnsupportedSaslMechanismError,
        };
        use crate::common::protocol::Errors;

        for (error, code) in [
            (
                Error::SaslAuthentication(SaslAuthenticationError::new("bad password")),
                Errors::SaslAuthenticationFailed,
            ),
            (
                Error::UnsupportedSaslMechanism(UnsupportedSaslMechanismError::new("not enabled")),
                Errors::UnsupportedSaslMechanism,
            ),
            (
                Error::IllegalSaslState(IllegalSaslStateError::new("unexpected")),
                Errors::IllegalSaslState,
            ),
            (
                Error::SslAuthentication(SslAuthenticationError::new("handshake")),
                Errors::InvalidConfig,
            ),
        ] {
            let carried = authentication_io_error(error.clone());
            assert!(is_authentication_error(&carried), "{error}");
            let recovered = authentication_error(&carried).expect("typed payload");
            assert_eq!(recovered.to_string(), error.to_string());
            assert_eq!(recovered.error(), code, "{error}");
            assert_eq!(authentication_error_message(&carried), Some(error.message()));
        }
    }

    /// A typed payload whose class does not extend `AuthenticationException` is
    /// not an authentication failure: Java's `KafkaChannel.prepare` lets it
    /// through to the selector, which treats it as a disconnect.
    #[test]
    fn non_authentication_payload_is_not_auth() {
        use crate::common::errors::NetworkError;

        let e = io::Error::other(Error::Network(NetworkError::new("broker went away")));
        assert!(!is_authentication_error(&e));
        assert!(authentication_error(&e).is_none());
    }
}
