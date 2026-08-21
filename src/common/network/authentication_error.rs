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
//! wrapped in an [`AuthenticationError`] and stored inside the [`io::Error`]
//! payload via [`auth_io_error`]. Downstream callers
//! ([`KafkaChannel::prepare`](super::kafka_channel::KafkaChannel::prepare)
//! and the [`Selector`](super::selector::Selector)) recover the distinction
//! with [`is_authentication_error`].
//!
//! Transient transport-level I/O errors (connection reset, broken pipe,
//! unexpected EOF) are NOT wrapped — their original [`io::ErrorKind`] is
//! preserved so the selector / network client treat them as a network
//! disconnect (retriable, reconnect with backoff), exactly as Java does.

use std::io;

use crate::common::errors::AuthenticationError;

/// Wraps a genuine authentication failure as an [`io::Error`] whose payload is
/// an [`AuthenticationError`], so callers can recover the typed distinction
/// via [`is_authentication_error`].
///
/// The [`io::ErrorKind`] is set to [`io::ErrorKind::Other`]; classification
/// downstream is driven by the typed payload (mirroring Java's `instanceof`),
/// NOT by the kind.
pub fn auth_io_error(message: impl Into<String>) -> io::Error {
    // The payload is the crate's single translation of
    // `org.apache.kafka.common.errors.AuthenticationException`
    // ([`crate::common::errors::AuthenticationError`]), which satisfies
    // `io::Error::other`'s `std::error::Error` bound. It used to be a second,
    // network-local struct of the same name — one Java class, one Rust type
    // (`definition-of-done.md` §6).
    io::Error::other(AuthenticationError::new(message))
}

/// Returns `true` if `e` carries an [`AuthenticationError`] payload, i.e. the
/// error is a genuine authentication failure rather than a transient network
/// disconnect.
///
/// This is the Rust equivalent of Java's
/// `e instanceof AuthenticationException` check in `KafkaChannel.prepare()`
/// and `Selector`.
/// The bare message of the [`AuthenticationError`] payload `e` carries, if any.
///
/// `io::Error`'s `Display` delegates to the payload, whose own `Display` is Java's
/// `toString()` form (`"AuthenticationError: <message>"`). A caller rebuilding the
/// typed error must therefore use this rather than `e.to_string()`, or the prefix
/// is applied twice and the application sees
/// `"AuthenticationError: AuthenticationError: <reason>"`. Java rethrows the
/// exception object itself (`NetworkClientUtils.java:86-87`), so the message is
/// untouched.
pub fn authentication_error_message(e: &io::Error) -> Option<&str> {
    e.get_ref()
        .and_then(|inner| inner.downcast_ref::<AuthenticationError>())
        .map(AuthenticationError::message)
}

pub fn is_authentication_error(e: &io::Error) -> bool {
    e.get_ref().is_some_and(|inner| inner.is::<AuthenticationError>())
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
}
