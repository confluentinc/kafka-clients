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

//! Translation of
//! `org.apache.kafka.common.security.authenticator.SaslClientAuthenticator`.
//!
//! Phase 9a deliverable. PLAIN-only state machine. SCRAM, OAUTHBEARER,
//! Kerberos/GSSAPI are explicitly out of scope and rejected by config
//! validation (Phase 9b).
//!
//! ## State machine
//!
//! Translation of Java's
//! `SaslClientAuthenticator.SaslState`:
//!
//! ```text
//! SendApiVersionsRequest → ReceiveApiVersionsResponse
//!                       → SendHandshakeRequest
//!                       → ReceiveHandshakeResponse
//!                       → SendPlainToken (sends the RFC 4616 \0user\0pass)
//!                       → ReceiveAuthenticateResponse
//!                       → Complete | Failed
//! ```
//!
//! Re-authentication states from Java
//! (`REAUTH_PROCESS_ORIG_APIVERSIONS_RESPONSE`,
//! `REAUTH_SEND_HANDSHAKE_REQUEST`, `REAUTH_RECEIVE_HANDSHAKE_OR_OTHER_RESPONSE`,
//! `REAUTH_INITIAL`) are deferred: re-authentication is out of scope for
//! Milestone 1 per PLAN.md:367 and Phase 5b-3's
//! `Authenticator::reauthenticate` no-op.
//!
//! ## Translation differences from Java
//!
//! - Java uses `javax.security.sasl.SaslClient` from JCA. For PLAIN only,
//!   the SASL exchange is a single client-initiated token of the form
//!   `\0username\0password` (RFC 4616). The Rust translation inlines this
//!   token format because we do not need a pluggable JCA-style SASL
//!   provider for PLAIN alone.
//! - Java uses a `Subject` populated via JAAS to carry the credentials.
//!   The Rust translation accepts an explicit [`PlainCredentials`] struct
//!   — JAAS parsing is deferred to Phase 9b.
//! - Java uses a reserved correlation-id range (`MIN_RESERVED..=MAX_RESERVED`)
//!   to disambiguate SASL request/response pairs from in-flight Kafka
//!   requests. Phase 9a translates this verbatim since the constants and
//!   `isReserved` predicate are part of the public surface.
//! - Java's `authenticate()` is a sync, non-blocking step function called
//!   repeatedly from the Selector loop. Phase 9a mirrors this: the state
//!   machine advances one step per `authenticate()` call, returns
//!   `Ok(())` when no further progress can be made without I/O, and
//!   completes when the state reaches `Complete`.

use std::collections::VecDeque;
use std::io;

use crate::common::errors::KafkaError;
use crate::common::message::sasl_authenticate_request_data::SaslAuthenticateRequestData;
use crate::common::message::sasl_handshake_request_data::SaslHandshakeRequestData;
use crate::common::network::network_receive::NetworkReceive;
use crate::common::network::send::Send as KafkaSend;
use crate::common::network::transferable_channel::TransferableChannel;
use crate::common::network::transport_layer::{OP_READ, OP_WRITE, TransportLayer};
use crate::common::network::{ByteBufferSend, Receive};
use crate::common::protocol::byte_buffer_accessor::ByteBufferAccessor;
use crate::common::protocol::{ApiKey, ApiKeys, Errors};
use crate::common::requests::{
    AbstractRequest, AbstractResponse, ApiVersionsRequest, ApiVersionsResponse, RequestHeader, SaslAuthenticateRequest,
    SaslAuthenticateResponse, SaslHandshakeRequest, SaslHandshakeResponse, parse_response,
};

/// Inclusive lower bound of the SASL-reserved correlation id range.
/// Mirrors Java's `SaslClientAuthenticator.MIN_RESERVED_CORRELATION_ID`.
pub const MIN_RESERVED_CORRELATION_ID: i32 = i32::MAX - 7;

/// Inclusive upper bound. Mirrors `SaslClientAuthenticator.MAX_RESERVED_CORRELATION_ID`.
pub const MAX_RESERVED_CORRELATION_ID: i32 = i32::MAX;

/// Predicate translation of Java's `SaslClientAuthenticator.isReserved(int)`.
pub fn is_reserved(correlation_id: i32) -> bool {
    correlation_id >= MIN_RESERVED_CORRELATION_ID
}

/// Sentinel meaning "this broker speaks pre-1.0 SASL — no Kafka header
/// wraps the SASL tokens." Mirrors
/// `SaslClientAuthenticator.DISABLE_KAFKA_SASL_AUTHENTICATE_HEADER = -1`.
const DISABLE_KAFKA_SASL_AUTHENTICATE_HEADER: i16 = -1;

/// PLAIN credentials. Owned by the authenticator. The byte form is
/// constructed lazily inside [`SaslClientAuthenticator::build_plain_token`]
/// so we keep the secret out of the type's `Debug` output.
#[derive(Clone)]
pub struct PlainCredentials {
    username: String,
    password: String,
}

impl PlainCredentials {
    /// Construct PLAIN credentials. Mirrors Java's
    /// `Subject.getPublicCredentials(String.class)` +
    /// `getPrivateCredentials(String.class)` indirection — Phase 9a
    /// accepts them as explicit fields instead of going through JAAS.
    pub fn new(username: impl Into<String>, password: impl Into<String>) -> Self {
        PlainCredentials { username: username.into(), password: password.into() }
    }

    /// Borrow the username. Used for the SASL handshake mechanism log.
    pub fn username(&self) -> &str {
        &self.username
    }
}

/// Hand-emitted `Debug` impl that masks the password (CLAUDE.md credential
/// handling). Username is preserved — it is not sensitive on its own and
/// it's useful for diagnostic logs.
impl std::fmt::Debug for PlainCredentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PlainCredentials")
            .field("username", &self.username)
            .field("password", &"<redacted>")
            .finish()
    }
}

/// SASL state. Translation of Java's
/// `SaslClientAuthenticator.SaslState`. Re-authentication states are
/// omitted; if re-authentication is added in a future milestone, model
/// them as a separate state set rather than overloading the initial path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SaslState {
    /// Send the initial `ApiVersionsRequest` (always v0 — mirrors Java's
    /// "always use version 0 request since brokers treat requests with
    /// schema exceptions as GSSAPI tokens").
    SendApiVersionsRequest,
    /// Awaiting `ApiVersionsResponse`.
    ReceiveApiVersionsResponse,
    /// Send `SaslHandshakeRequest` with the chosen mechanism.
    SendHandshakeRequest,
    /// Awaiting `SaslHandshakeResponse`.
    ReceiveHandshakeResponse,
    /// Send the SASL PLAIN initial token (`\0user\0pass`) wrapped in a
    /// `SaslAuthenticateRequest` (when the broker speaks
    /// `SASL_AUTHENTICATE`) or as a raw size-prefixed payload (legacy
    /// brokers).
    SendInitialToken,
    /// Awaiting `SaslAuthenticateResponse` (or a raw legacy token).
    ReceiveAuthenticateResponse,
    /// Authentication completed successfully.
    Complete,
    /// Authentication failed. The error is captured on the authenticator
    /// itself; further calls to `authenticate()` return the same error.
    Failed,
}

/// Outgoing SASL message in flight on the wire. Mirrors Java's
/// `Send netOutBuffer` field.
struct PendingSend {
    /// Wrapping payload (handles partial writes across multiple
    /// `authenticate()` calls).
    inner: ByteBufferSend,
    /// `RequestHeader` we expect back in the matching response. `None`
    /// for legacy raw SASL tokens (no Kafka header).
    correlation_header: Option<RequestHeader>,
}

/// Client-side SASL authenticator. PLAIN-only. Translation of
/// `org.apache.kafka.common.security.authenticator.SaslClientAuthenticator`.
///
/// The authenticator owns the credentials, the in-flight send buffer, the
/// in-flight receive buffer, and the negotiated `SaslAuthenticate` /
/// `SaslHandshake` versions. It does not own the
/// [`TransportLayer`] — Java's design passes the transport via the
/// `KafkaChannel` to keep the authenticator and the transport lifetime
/// independent. Phase 9a's `authenticate(transport)` mirrors this.
///
/// `Debug` is hand-emitted to redact the embedded
/// [`PlainCredentials`] password and the in-flight send buffer (which
/// may carry the PLAIN token bytes mid-flight).
pub struct SaslClientAuthenticator {
    /// Channel id (mirrors Java's `String node`). Used in error messages
    /// and in the `NetworkReceive` source label.
    node: String,
    /// Configured client id (mirrors Java's `configs.get(CLIENT_ID_CONFIG)`).
    client_id: String,
    /// Chosen SASL mechanism (PLAIN only in Phase 9a; rejected at the
    /// validation boundary otherwise).
    mechanism: String,
    /// Credentials. Loaded from `sasl.jaas.config` /
    /// `sasl.username`/`sasl.password` (Phase 9b).
    credentials: PlainCredentials,

    /// Current state.
    state: SaslState,
    /// Captured error if `state == Failed`. Distinct field from the
    /// state itself because Java carries the throwable separately.
    failure: Option<KafkaError>,

    /// In-flight send buffer (Java: `Send netOutBuffer`).
    pending_send: Option<PendingSend>,
    /// In-flight receive buffer (Java: `NetworkReceive netInBuffer`).
    pending_receive: Option<NetworkReceive>,
    /// Most recent request header for which we are waiting on a response.
    /// Mirrors Java's `RequestHeader currentRequestHeader`.
    current_request_header: Option<RequestHeader>,

    /// Negotiated SaslAuthenticate version, or
    /// `DISABLE_KAFKA_SASL_AUTHENTICATE_HEADER` if the broker doesn't
    /// support the `SASL_AUTHENTICATE` API key (legacy brokers).
    sasl_authenticate_version: i16,
    /// Negotiated SaslHandshake version (defaults to 1 — the minimum
    /// since 1.0.0).
    sasl_handshake_version: i16,

    /// Correlation-id ring counter. Mirrors Java's `int correlationId`.
    correlation_id: i32,

    /// Optional `Send` queued so other connections from the
    /// `Selector` can poll us for outgoing bytes. Currently unused in
    /// Phase 9a because the channel's KafkaChannel hosts the
    /// authenticator directly — Java has the same shape.
    #[allow(dead_code)]
    deferred_sends: VecDeque<ByteBufferSend>,
}

/// Hand-emitted `Debug` impl that masks the password and the in-flight
/// send buffer (the PLAIN token may be mid-flight there).
impl std::fmt::Debug for SaslClientAuthenticator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SaslClientAuthenticator")
            .field("node", &self.node)
            .field("client_id", &self.client_id)
            .field("mechanism", &self.mechanism)
            .field("credentials", &self.credentials)
            .field("state", &self.state)
            .field("failure", &self.failure)
            .field("pending_send", &self.pending_send.as_ref().map(|_| "<in-flight>"))
            .field("pending_receive", &self.pending_receive.is_some())
            .field("current_request_header", &self.current_request_header.is_some())
            .field("sasl_authenticate_version", &self.sasl_authenticate_version)
            .field("sasl_handshake_version", &self.sasl_handshake_version)
            .field("correlation_id", &self.correlation_id)
            .finish()
    }
}

impl SaslClientAuthenticator {
    /// Construct a new PLAIN authenticator. Mirrors the Java constructor
    /// `SaslClientAuthenticator(Map configs, AuthenticateCallbackHandler,
    /// String node, Subject, String servicePrincipal, String host, String
    /// mechanism, TransportLayer, Time, LogContext)`.
    ///
    /// Phase 9a omits:
    /// - `AuthenticateCallbackHandler` — PLAIN's only callbacks (Name,
    ///   Password) are inlined.
    /// - `Subject` / `servicePrincipal` — PLAIN does not need a Kerberos
    ///   service principal.
    /// - `host` — only relevant for GSSAPI.
    /// - `Time` — only relevant for re-authentication session expiry
    ///   (deferred).
    /// - `LogContext` — Rust `tracing` carries the equivalent.
    ///
    /// Returns `KafkaError::Config` for any mechanism other than `PLAIN`.
    pub fn new(
        node: impl Into<String>,
        client_id: impl Into<String>,
        mechanism: impl Into<String>,
        credentials: PlainCredentials,
    ) -> Result<Self, KafkaError> {
        let mechanism = mechanism.into();
        if mechanism != "PLAIN" {
            return Err(KafkaError::Config(format!(
                "Unsupported SASL mechanism: {mechanism}. Phase 9 supports only PLAIN."
            )));
        }
        Ok(SaslClientAuthenticator {
            node: node.into(),
            client_id: client_id.into(),
            mechanism,
            credentials,
            state: SaslState::SendApiVersionsRequest,
            failure: None,
            pending_send: None,
            pending_receive: None,
            current_request_header: None,
            sasl_authenticate_version: DISABLE_KAFKA_SASL_AUTHENTICATE_HEADER,
            sasl_handshake_version: 1, // Minimum since 1.0.0
            correlation_id: MIN_RESERVED_CORRELATION_ID,
            deferred_sends: VecDeque::new(),
        })
    }

    /// Current SASL state (read-only; used by tests and `complete()`).
    pub fn state(&self) -> SaslState {
        self.state
    }

    /// Returns the captured failure (if `state == Failed`), else `None`.
    pub fn failure(&self) -> Option<&KafkaError> {
        self.failure.as_ref()
    }

    /// `true` iff the authenticator has reached the `Complete` state.
    /// Mirrors Java's `SaslClientAuthenticator.complete()`.
    pub fn complete(&self) -> bool {
        self.state == SaslState::Complete
    }

    /// Returns the next correlation id in the SASL-reserved range,
    /// wrapping back to [`MIN_RESERVED_CORRELATION_ID`] when the range
    /// is exhausted. Mirrors Java's
    /// `SaslClientAuthenticator.nextCorrelationId()`. Pub for tests.
    pub fn next_correlation_id(&mut self) -> i32 {
        if !is_reserved(self.correlation_id) {
            self.correlation_id = MIN_RESERVED_CORRELATION_ID;
        }
        let id = self.correlation_id;
        self.correlation_id = self.correlation_id.wrapping_add(1);
        id
    }

    /// Build the SASL PLAIN token per RFC 4616:
    /// `\0username\0password` (one literal NUL before username, one
    /// between, no trailing NUL). Inlined here because we do not pull
    /// in a JCA-style SASL provider for PLAIN — the token is a
    /// straightforward byte concatenation.
    fn build_plain_token(&self) -> Vec<u8> {
        let username = self.credentials.username.as_bytes();
        let password = self.credentials.password.as_bytes();
        let mut token = Vec::with_capacity(2 + username.len() + password.len());
        token.push(0u8);
        token.extend_from_slice(username);
        token.push(0u8);
        token.extend_from_slice(password);
        token
    }

    /// Drive the SASL state machine forward, mirroring Java's
    /// `SaslClientAuthenticator.authenticate()`. Returns `Ok(())` when:
    /// - the current step's I/O has been issued (send buffered, response
    ///   awaited);
    /// - or there's nothing to do because the authenticator is already
    ///   `Complete` or in a state that needs more network bytes.
    ///
    /// Returns `Err(io::Error::other(KafkaError::Authentication(...)))`
    /// on authentication failure, with the error captured in
    /// [`Self::failure`].
    ///
    /// The Java version is called repeatedly from the Selector's main
    /// poll loop; the Rust translation expects the same pattern (call
    /// after each `transport.read()` cycle).
    pub fn authenticate(&mut self, transport: &mut dyn TransportLayer) -> io::Result<()> {
        // Java's `authenticate()` opens with:
        //   if (netOutBuffer != null && !flushNetOutBufferAndUpdateInterestOps())
        //       return;
        // Mirror that: finish flushing any pending send before advancing.
        if self.pending_send.is_some() && !self.flush_pending_send(transport)? {
            return Ok(());
        }

        loop {
            // The state machine is intentionally a simple match (no
            // fall-through). Each branch performs one logical step:
            // either queues a send or attempts to read+parse a response.
            // We continue looping while a state transition happens
            // without I/O blocking (e.g. just-sent → expect-response
            // immediately), and bail with `Ok(())` when further progress
            // requires a real network read.
            let started_state = self.state;
            match self.state {
                SaslState::SendApiVersionsRequest => {
                    // Mirror Java: "Always use version 0 request since
                    // brokers treat requests with schema exceptions as
                    // GSSAPI tokens". Build the request directly — no
                    // builder needed, we know the version is 0.
                    let data = crate::common::message::api_versions_request_data::ApiVersionsRequestData {
                        client_software_name: self.client_id.clone(),
                        client_software_version: env!("CARGO_PKG_VERSION").to_owned(),
                        unknown_tagged_fields: Vec::new(),
                    };
                    let req = ApiVersionsRequest::new(data, 0);
                    self.queue_request(transport, ApiKeys::for_id(18).expect("API_VERSIONS"), 0, &req)?;
                    self.state = SaslState::ReceiveApiVersionsResponse;
                },
                SaslState::ReceiveApiVersionsResponse => {
                    // Drive the receive forward. Returns Ok(None) if
                    // more bytes are needed.
                    match self.receive_response(transport)? {
                        None => return Ok(()),
                        Some(response) => {
                            // Cast the boxed dyn response back to
                            // `ApiVersionsResponse`. This is the same
                            // shape as `NetworkClient.parseResponse`
                            // re-wraps the parsed response.
                            let api_versions_response =
                                response.as_any().downcast_ref::<ApiVersionsResponse>().ok_or_else(|| {
                                    io::Error::other(KafkaError::Authentication(
                                        "expected ApiVersionsResponse from SASL handshake start".to_owned(),
                                    ))
                                })?;
                            self.set_sasl_versions_from_api_versions(api_versions_response);
                            self.state = SaslState::SendHandshakeRequest;
                            // Fall through (Java does this with a
                            // labelled fallthrough); continue the loop.
                        },
                    }
                },
                SaslState::SendHandshakeRequest => {
                    let data = SaslHandshakeRequestData {
                        mechanism: self.mechanism.clone(),
                        unknown_tagged_fields: Vec::new(),
                    };
                    let req = SaslHandshakeRequest::new(data, self.sasl_handshake_version);
                    self.queue_request(
                        transport,
                        ApiKeys::for_id(17).expect("SASL_HANDSHAKE"),
                        self.sasl_handshake_version,
                        &req,
                    )?;
                    self.state = SaslState::ReceiveHandshakeResponse;
                },
                SaslState::ReceiveHandshakeResponse => match self.receive_response(transport)? {
                    None => return Ok(()),
                    Some(response) => {
                        let handshake_response =
                            response.as_any().downcast_ref::<SaslHandshakeResponse>().ok_or_else(|| {
                                io::Error::other(KafkaError::Authentication(
                                    "expected SaslHandshakeResponse during SASL handshake".to_owned(),
                                ))
                            })?;
                        self.handle_sasl_handshake_response(handshake_response)?;
                        self.state = SaslState::SendInitialToken;
                    },
                },
                SaslState::SendInitialToken => {
                    let token = self.build_plain_token();
                    if self.sasl_authenticate_version == DISABLE_KAFKA_SASL_AUTHENTICATE_HEADER {
                        // Legacy broker — raw size-prefixed token, no
                        // `SaslAuthenticateRequest` wrapper. Translation
                        // of Java's
                        // `Send = ByteBufferSend.sizePrefixed(tokenBuf)`.
                        let send = ByteBufferSend::size_prefixed(bytes::Bytes::from(token));
                        self.pending_send = Some(PendingSend { inner: send, correlation_header: None });
                        if !self.flush_pending_send(transport)? {
                            return Ok(());
                        }
                    } else {
                        let data = SaslAuthenticateRequestData { auth_bytes: token, unknown_tagged_fields: Vec::new() };
                        let req = SaslAuthenticateRequest::new(data, self.sasl_authenticate_version);
                        self.queue_request(
                            transport,
                            ApiKeys::for_id(36).expect("SASL_AUTHENTICATE"),
                            self.sasl_authenticate_version,
                            &req,
                        )?;
                    }
                    self.state = SaslState::ReceiveAuthenticateResponse;
                },
                SaslState::ReceiveAuthenticateResponse => {
                    if self.sasl_authenticate_version == DISABLE_KAFKA_SASL_AUTHENTICATE_HEADER {
                        // Legacy: just read the raw size-prefixed
                        // response (no Kafka header). PLAIN's success
                        // response is empty — receipt is enough.
                        match self.receive_raw_token(transport)? {
                            None => return Ok(()),
                            Some(_token) => {
                                // PLAIN sends no further challenge; we're done.
                                self.state = SaslState::Complete;
                                transport.remove_interest_ops(OP_WRITE);
                            },
                        }
                    } else {
                        match self.receive_response(transport)? {
                            None => return Ok(()),
                            Some(response) => {
                                let auth_response =
                                    response.as_any().downcast_ref::<SaslAuthenticateResponse>().ok_or_else(|| {
                                        io::Error::other(KafkaError::Authentication(
                                            "expected SaslAuthenticateResponse during SASL auth".to_owned(),
                                        ))
                                    })?;
                                self.handle_sasl_authenticate_response(auth_response)?;
                                // PLAIN single-step exchange: no more
                                // challenges to respond to.
                                self.state = SaslState::Complete;
                                transport.remove_interest_ops(OP_WRITE);
                            },
                        }
                    }
                },
                SaslState::Complete => return Ok(()),
                SaslState::Failed => {
                    // Java throws `IllegalStateException` here. We
                    // surface the captured failure as `io::Error::other`
                    // wrapping a `KafkaError::Authentication`, which the
                    // upper layer (`KafkaChannel::prepare`) recognises
                    // by content.
                    let err = self
                        .failure
                        .clone()
                        .unwrap_or_else(|| KafkaError::Authentication("SASL handshake already failed".to_owned()));
                    return Err(io::Error::other(err));
                },
            }

            // If the state did not change in this iteration, we cannot
            // make further progress without I/O — return and wait for
            // the next poll. Java's switch uses fall-through; ours uses
            // an explicit loop with the same termination signal.
            if self.state == started_state {
                return Ok(());
            }
        }
    }

    /// Compute and pin the negotiated SASL_AUTHENTICATE and
    /// SASL_HANDSHAKE versions from a freshly received
    /// `ApiVersionsResponse`. Mirrors Java's
    /// `setSaslAuthenticateAndHandshakeVersions(ApiVersionsResponse)`.
    fn set_sasl_versions_from_api_versions(&mut self, response: &ApiVersionsResponse) {
        let sasl_authenticate_key = ApiKeys::for_id(36).expect("SASL_AUTHENTICATE");
        if let Some(api_version) = response.api_version(36) {
            // Pin to the maximum of (broker's max, our max). Java uses
            // `Math.min(api_version.maxVersion(), latestVersion())`.
            self.sasl_authenticate_version = api_version.max_version.min(sasl_authenticate_key.latest_version());
        }
        let sasl_handshake_key = ApiKeys::for_id(17).expect("SASL_HANDSHAKE");
        if let Some(api_version) = response.api_version(17) {
            self.sasl_handshake_version = api_version.max_version.min(sasl_handshake_key.latest_version());
        }
    }

    /// Translation of Java's `handleSaslHandshakeResponse`. Inspects the
    /// handshake response's error code and either transitions through
    /// or transitions to `Failed` with the appropriate error message.
    fn handle_sasl_handshake_response(&mut self, response: &SaslHandshakeResponse) -> io::Result<()> {
        let error = response.error();
        if error == Errors::None {
            return Ok(());
        }
        let mechanism = self.mechanism.clone();
        // Render the enabled-mechanisms list in the same shape as Java's
        // `List<String>.toString()` — `[m1, m2]` — to keep test
        // assertions parity with Java error strings.
        let enabled = format!("{:?}", response.response_data().mechanisms);
        let err = match error {
            Errors::UnsupportedSaslMechanism => KafkaError::Authentication(format!(
                "Client SASL mechanism '{mechanism}' not enabled in the server, enabled mechanisms are {enabled}"
            )),
            Errors::IllegalSaslState => KafkaError::Authentication(format!(
                "Unexpected handshake request with client mechanism {mechanism}, enabled mechanisms are {enabled}"
            )),
            other => KafkaError::Authentication(format!(
                "Unknown error code {other:?}, client mechanism is {mechanism}, enabled mechanisms are {enabled}"
            )),
        };
        self.state = SaslState::Failed;
        self.failure = Some(err.clone());
        Err(io::Error::other(err))
    }

    /// Translation of Java's `receiveToken`'s error-handling for
    /// `SaslAuthenticateResponse`: on a non-`NONE` error code, throw
    /// the `SaslAuthenticationException` with the broker's error message.
    fn handle_sasl_authenticate_response(&mut self, response: &SaslAuthenticateResponse) -> io::Result<()> {
        let error = response.error();
        if error == Errors::None {
            return Ok(());
        }
        // Prefer the broker-provided message; fall back to the canonical
        // wire-code message if the broker omitted it.
        let message = response
            .error_message()
            .map(|m| m.to_owned())
            .unwrap_or_else(|| format!("{error:?}"));
        let err = KafkaError::Authentication(message);
        self.state = SaslState::Failed;
        self.failure = Some(err.clone());
        Err(io::Error::other(err))
    }

    /// Encode a request with its header into a `Send` queued on
    /// `pending_send`, then immediately attempt to flush. Mirrors Java's
    /// `send(send)` followed by `flushNetOutBufferAndUpdateInterestOps`.
    fn queue_request(
        &mut self,
        transport: &mut dyn TransportLayer,
        api_key: &'static ApiKey,
        version: i16,
        request: &dyn AbstractRequest,
    ) -> io::Result<()> {
        let correlation_id = self.next_correlation_id();
        let header = RequestHeader::new(api_key, version, &self.client_id, correlation_id);
        let body = request.serialize_with_header(&header).map_err(io::Error::other)?;
        // Size-prefix it for the wire: ByteBufferSend automatically
        // emits a 4-byte big-endian length header in front of `payload`.
        let send = ByteBufferSend::size_prefixed(bytes::Bytes::from(body));
        self.pending_send = Some(PendingSend { inner: send, correlation_header: Some(header) });
        let _ = self.flush_pending_send(transport)?;
        Ok(())
    }

    /// Attempt to flush the pending send. Returns `Ok(true)` when the
    /// send completed (the buffer is drained and removed); `Ok(false)`
    /// when more bytes remain to be written. Mirrors Java's
    /// `flushNetOutBufferAndUpdateInterestOps`.
    fn flush_pending_send(&mut self, transport: &mut dyn TransportLayer) -> io::Result<bool> {
        let Some(pending) = self.pending_send.as_mut() else {
            return Ok(true);
        };
        // ByteBufferSend.write_to → channel.write_vectored. Pass the
        // transport as a `&mut dyn TransferableChannel` — the supertrait
        // bound on TransportLayer makes this safe (`as` coercion).
        let _written = pending.inner.write_to(transport as &mut dyn TransferableChannel)?;
        if pending.inner.completed() {
            // Drain the send; latch its correlation header so the next
            // `receive_response` knows what to expect.
            let drained = self.pending_send.take().expect("pending_send still Some");
            if let Some(header) = drained.correlation_header {
                self.current_request_header = Some(header);
            }
            transport.remove_interest_ops(OP_WRITE);
            Ok(true)
        } else {
            // Keep OP_WRITE armed so the Selector wakes us when there's
            // capacity for more bytes.
            transport.add_interest_ops(OP_WRITE);
            Ok(false)
        }
    }

    /// Read the size-prefixed framed response and parse it against the
    /// latched request header. Returns `Ok(None)` when more bytes are
    /// needed. Mirrors Java's `receiveKafkaResponse()` minus
    /// re-authentication branches.
    fn receive_response(
        &mut self,
        transport: &mut dyn TransportLayer,
    ) -> io::Result<Option<Box<dyn AbstractResponse>>> {
        let Some(bytes) = self.receive_raw_token(transport)? else {
            return Ok(None);
        };
        let header = self.current_request_header.take().ok_or_else(|| {
            io::Error::other(KafkaError::Authentication(
                "received SASL response with no pending request header".to_owned(),
            ))
        })?;
        let mut accessor = ByteBufferAccessor::wrap(bytes);
        let response = parse_response(&mut accessor, &header).map_err(io::Error::other)?;
        // Keep OP_READ armed — the next state may also need to read.
        transport.add_interest_ops(OP_READ);
        Ok(Some(response))
    }

    /// Lower-level read: drives the NetworkReceive forward and returns
    /// the payload bytes when the size-prefixed frame has been fully
    /// received; returns `Ok(None)` if more bytes are needed.
    fn receive_raw_token(&mut self, transport: &mut dyn TransportLayer) -> io::Result<Option<Vec<u8>>> {
        let receive = self
            .pending_receive
            .get_or_insert_with(|| NetworkReceive::with_source(self.node.clone()));
        // The transport layer's read() returns Ok(0) on WouldBlock and
        // surfaces EOF as Err(UnexpectedEof) — both behaviours we forward
        // verbatim.
        let _read = receive.read_from(&mut WrapTransportRead::new(transport))?;
        if !receive.complete() {
            transport.add_interest_ops(OP_READ);
            return Ok(None);
        }
        let payload = self
            .pending_receive
            .take()
            .expect("pending_receive present")
            .take_payload()
            .ok_or_else(|| {
                io::Error::other(KafkaError::Authentication("SASL receive completed without payload".to_owned()))
            })?
            .to_vec();
        Ok(Some(payload))
    }
}

/// Wrapper that adapts `&mut dyn TransportLayer` to `&mut dyn io::Read`
/// so [`NetworkReceive::read_from`] can drive it. The `io::Read` trait
/// is intentionally not a supertrait of `TransportLayer` (Java's
/// `TransportLayer` extends `ScatteringByteChannel`, not
/// `InputStream`), so we synthesise the adapter here for the SASL
/// receive path.
struct WrapTransportRead<'a> {
    inner: &'a mut dyn TransportLayer,
}

impl<'a> WrapTransportRead<'a> {
    fn new(inner: &'a mut dyn TransportLayer) -> Self {
        WrapTransportRead { inner }
    }
}

impl io::Read for WrapTransportRead<'_> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.inner.read(buf)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::RefCell;
    use std::collections::VecDeque;
    use std::net::SocketAddr;
    use std::task::{Context, Poll};

    use std::io::IoSlice;

    use crate::common::message::api_versions_response_data::{ApiVersion, ApiVersionsResponseData};
    use crate::common::message::sasl_authenticate_response_data::SaslAuthenticateResponseData;
    use crate::common::message::sasl_handshake_response_data::SaslHandshakeResponseData;
    use crate::common::protocol::byte_buffer_accessor::ByteBufferAccessor;
    use crate::common::protocol::object_serialization_cache::ObjectSerializationCache;
    use crate::common::requests::AbstractResponse;
    use crate::common::requests::ResponseHeader;
    use crate::common::security::auth::KafkaPrincipal;

    /// Mock transport: a pair of in-memory queues. Bytes written by the
    /// authenticator land on `outbound`; bytes the test pre-loads into
    /// `inbound` are returned from `read`.
    ///
    /// Behaviour mirrors Java's non-blocking NIO contract:
    /// - `write_vectored` returns the number of bytes accepted into the
    ///   `outbound` queue (always the full amount in the mock — no
    ///   partial-write simulation in Phase 9a; cover that case in 9b/9c).
    /// - `read` returns `Ok(n)` for `n > 0`, `Ok(0)` for WouldBlock when
    ///   the queue is empty, and `Err(UnexpectedEof)` when the test
    ///   explicitly sets `eof_after_drain`.
    struct MockTransport {
        outbound: RefCell<Vec<u8>>,
        inbound: RefCell<VecDeque<u8>>,
        interest_ops: i32,
        connected: bool,
        eof_after_drain: bool,
    }

    impl MockTransport {
        fn new() -> Self {
            MockTransport {
                outbound: RefCell::new(Vec::new()),
                inbound: RefCell::new(VecDeque::new()),
                interest_ops: OP_READ,
                connected: true,
                eof_after_drain: false,
            }
        }

        /// Pre-load a complete framed response (length-prefix + body) into
        /// the inbound queue.
        fn push_framed(&self, body: &[u8]) {
            let mut q = self.inbound.borrow_mut();
            for b in (body.len() as i32).to_be_bytes() {
                q.push_back(b);
            }
            q.extend(body);
        }

        fn outbound_bytes(&self) -> Vec<u8> {
            self.outbound.borrow().clone()
        }
    }

    impl TransferableChannel for MockTransport {
        fn write_vectored(&mut self, bufs: &[IoSlice<'_>]) -> io::Result<usize> {
            let mut total = 0;
            let mut out = self.outbound.borrow_mut();
            for s in bufs {
                out.extend_from_slice(s);
                total += s.len();
            }
            Ok(total)
        }

        fn has_pending_writes(&self) -> bool {
            false
        }
    }

    impl TransportLayer for MockTransport {
        fn ready(&self) -> bool {
            true
        }
        fn finish_connect(&mut self) -> io::Result<bool> {
            Ok(true)
        }
        fn disconnect(&mut self) {
            self.connected = false;
        }
        fn is_connected(&self) -> bool {
            self.connected
        }
        fn is_open(&self) -> bool {
            self.connected
        }
        fn close(&mut self) -> io::Result<()> {
            self.connected = false;
            Ok(())
        }
        fn read(&mut self, dst: &mut [u8]) -> io::Result<usize> {
            let mut q = self.inbound.borrow_mut();
            if q.is_empty() {
                if self.eof_after_drain {
                    return Err(io::Error::from(io::ErrorKind::UnexpectedEof));
                }
                return Ok(0); // WouldBlock
            }
            let mut n = 0;
            while n < dst.len() {
                match q.pop_front() {
                    Some(b) => {
                        dst[n] = b;
                        n += 1;
                    },
                    None => break,
                }
            }
            Ok(n)
        }
        fn handshake(&mut self) -> io::Result<()> {
            Ok(())
        }
        fn peer_principal(&self) -> io::Result<KafkaPrincipal> {
            Ok(KafkaPrincipal::anonymous())
        }
        fn add_interest_ops(&mut self, ops: i32) {
            self.interest_ops |= ops;
        }
        fn remove_interest_ops(&mut self, ops: i32) {
            self.interest_ops &= !ops;
        }
        fn interest_ops(&self) -> i32 {
            self.interest_ops
        }
        fn is_mute(&self) -> bool {
            self.interest_ops & OP_READ == 0
        }
        fn has_bytes_buffered(&self) -> bool {
            !self.inbound.borrow().is_empty()
        }
        fn local_addr(&self) -> io::Result<SocketAddr> {
            Ok(SocketAddr::from(([127, 0, 0, 1], 0)))
        }
        fn peer_addr(&self) -> io::Result<SocketAddr> {
            Ok(SocketAddr::from(([127, 0, 0, 1], 9092)))
        }
        fn poll_read_ready(&self, _cx: &mut Context<'_>) -> Poll<()> {
            Poll::Ready(())
        }
    }

    /// Serialise an `ApiVersionsResponse` for a SASL bootstrap exchange:
    /// includes the SASL_HANDSHAKE (17) and SASL_AUTHENTICATE (36) entries.
    fn serialize_api_versions_response(correlation_id: i32) -> Vec<u8> {
        let response = ApiVersionsResponse::new(ApiVersionsResponseData {
            error_code: 0,
            api_keys: vec![
                ApiVersion { api_key: 17, min_version: 0, max_version: 1, unknown_tagged_fields: Vec::new() },
                ApiVersion { api_key: 36, min_version: 0, max_version: 2, unknown_tagged_fields: Vec::new() },
                ApiVersion { api_key: 18, min_version: 0, max_version: 3, unknown_tagged_fields: Vec::new() },
            ],
            throttle_time_ms: 0,
            supported_features: Vec::new(),
            finalized_features_epoch: -1,
            finalized_features: Vec::new(),
            zk_migration_ready: false,
            unknown_tagged_fields: Vec::new(),
        });
        wrap_with_header(response.api_key(), 0, correlation_id, &response, 0)
    }

    fn serialize_sasl_handshake_response(
        correlation_id: i32,
        error: Errors,
        mechanisms: Vec<String>,
        version: i16,
    ) -> Vec<u8> {
        let response = SaslHandshakeResponse::new(SaslHandshakeResponseData {
            error_code: error.code(),
            mechanisms,
            unknown_tagged_fields: Vec::new(),
        });
        wrap_with_header(response.api_key(), version, correlation_id, &response, version)
    }

    fn serialize_sasl_authenticate_response(
        correlation_id: i32,
        error: Errors,
        message: Option<&str>,
        version: i16,
    ) -> Vec<u8> {
        let response = SaslAuthenticateResponse::new(SaslAuthenticateResponseData {
            error_code: error.code(),
            error_message: message.map(|s| s.to_owned()),
            auth_bytes: Vec::new(),
            session_lifetime_ms: 0,
            unknown_tagged_fields: Vec::new(),
        });
        wrap_with_header(response.api_key(), version, correlation_id, &response, version)
    }

    /// Prepend a `ResponseHeader` to a serialised response body, mirroring
    /// what Java's `NetworkClient` puts on the wire for the broker side.
    fn wrap_with_header(
        api_key: &'static ApiKey,
        api_version: i16,
        correlation_id: i32,
        response: &dyn AbstractResponse,
        body_version: i16,
    ) -> Vec<u8> {
        let body = AbstractResponse::serialize(response, body_version).expect("serialize body");
        let mut header_accessor = ByteBufferAccessor::wrap(Vec::new());
        let header_version = api_key.response_header_version(api_version);
        let header = ResponseHeader::new(correlation_id, header_version);
        let cache = ObjectSerializationCache::new();
        header.write(&mut header_accessor, &cache).expect("write response header");
        let mut combined = header_accessor.buffer().to_vec();
        combined.extend_from_slice(body.buffer());
        combined
    }

    /// Happy path: full PLAIN exchange completes.
    #[test]
    fn plain_happy_path_drives_state_machine_to_complete() {
        let mut transport = MockTransport::new();
        let mut auth = SaslClientAuthenticator::new(
            "node-0",
            "test-client",
            "PLAIN",
            PlainCredentials::new("alice", "supersecret"),
        )
        .expect("PLAIN authenticator construction");

        assert_eq!(auth.state(), SaslState::SendApiVersionsRequest);

        // Step 1: drive — should send ApiVersionsRequest, transition to
        // ReceiveApiVersionsResponse, then return Ok(()) waiting for
        // bytes.
        auth.authenticate(&mut transport).expect("step 1");
        assert_eq!(auth.state(), SaslState::ReceiveApiVersionsResponse);
        assert!(!transport.outbound_bytes().is_empty(), "ApiVersionsRequest must have been sent");

        // The request used correlation_id = MIN_RESERVED. We respond
        // with the matching header.
        let api_versions_response_bytes = serialize_api_versions_response(MIN_RESERVED_CORRELATION_ID);
        transport.push_framed(&api_versions_response_bytes);

        // Step 2: parse ApiVersionsResponse, transition through
        // SendHandshakeRequest → ReceiveHandshakeResponse.
        auth.authenticate(&mut transport).expect("step 2");
        assert_eq!(auth.state(), SaslState::ReceiveHandshakeResponse);
        assert_eq!(auth.sasl_authenticate_version, 2, "negotiated min(client=2, broker=2)");
        assert_eq!(auth.sasl_handshake_version, 1);

        // Push a successful SaslHandshakeResponse v1 (correlation_id
        // MIN+1).
        let handshake_response_bytes = serialize_sasl_handshake_response(
            MIN_RESERVED_CORRELATION_ID + 1,
            Errors::None,
            vec!["PLAIN".to_owned()],
            1,
        );
        transport.push_framed(&handshake_response_bytes);

        // Step 3: parse handshake, transition through SendInitialToken
        // → ReceiveAuthenticateResponse.
        auth.authenticate(&mut transport).expect("step 3");
        assert_eq!(auth.state(), SaslState::ReceiveAuthenticateResponse);

        // The PLAIN token is now on the wire. The exact bytes are
        // verified by the next assertion via the outbound trace shape.
        assert!(transport.outbound_bytes().windows(7).any(|w| w == b"\0alice\0"));

        // Push success response.
        let auth_response_bytes =
            serialize_sasl_authenticate_response(MIN_RESERVED_CORRELATION_ID + 2, Errors::None, None, 2);
        transport.push_framed(&auth_response_bytes);

        // Step 4: parse authenticate response, transition to Complete.
        auth.authenticate(&mut transport).expect("step 4");
        assert_eq!(auth.state(), SaslState::Complete);
        assert!(auth.complete());
    }

    /// Broker reports `UNSUPPORTED_SASL_MECHANISM` in the handshake
    /// response → state transitions to Failed with the Java-parity
    /// error message.
    #[test]
    fn handshake_unsupported_mechanism_fails_with_java_message() {
        let mut transport = MockTransport::new();
        let mut auth = SaslClientAuthenticator::new(
            "node-0",
            "test-client",
            "PLAIN",
            PlainCredentials::new("alice", "supersecret"),
        )
        .expect("authenticator");

        // Drive past ApiVersions.
        auth.authenticate(&mut transport).expect("step 1");
        transport.push_framed(&serialize_api_versions_response(MIN_RESERVED_CORRELATION_ID));
        auth.authenticate(&mut transport).expect("step 2");

        // Broker rejects PLAIN — supports only SCRAM-SHA-512.
        transport.push_framed(&serialize_sasl_handshake_response(
            MIN_RESERVED_CORRELATION_ID + 1,
            Errors::UnsupportedSaslMechanism,
            vec!["SCRAM-SHA-512".to_owned()],
            1,
        ));

        let err = auth.authenticate(&mut transport).expect_err("step 3 must fail");
        let inner = err.into_inner().expect("inner");
        let kafka_err = inner.downcast_ref::<KafkaError>().expect("KafkaError captured");
        assert!(matches!(kafka_err, KafkaError::Authentication(_)));
        assert!(kafka_err.is_fatal(), "auth failures are fatal");
        assert!(!kafka_err.is_retriable(), "auth failures are non-retriable");
        let msg = kafka_err.message();
        assert!(msg.contains("Client SASL mechanism 'PLAIN'"));
        assert!(msg.contains("not enabled in the server"));
        assert!(msg.contains("SCRAM-SHA-512"));
        assert_eq!(auth.state(), SaslState::Failed);
    }

    /// Broker reports `SASL_AUTHENTICATION_FAILED` with a message in
    /// the authenticate response → state transitions to Failed,
    /// error message is preserved.
    #[test]
    fn authenticate_failure_preserves_broker_message() {
        let mut transport = MockTransport::new();
        let mut auth = SaslClientAuthenticator::new(
            "node-0",
            "test-client",
            "PLAIN",
            PlainCredentials::new("alice", "wrong-password"),
        )
        .expect("authenticator");

        auth.authenticate(&mut transport).expect("step 1");
        transport.push_framed(&serialize_api_versions_response(MIN_RESERVED_CORRELATION_ID));
        auth.authenticate(&mut transport).expect("step 2");
        transport.push_framed(&serialize_sasl_handshake_response(
            MIN_RESERVED_CORRELATION_ID + 1,
            Errors::None,
            vec!["PLAIN".to_owned()],
            1,
        ));
        auth.authenticate(&mut transport).expect("step 3");

        // Wrong creds → broker says no.
        let broker_msg = "Authentication failed: Invalid username or password";
        transport.push_framed(&serialize_sasl_authenticate_response(
            MIN_RESERVED_CORRELATION_ID + 2,
            Errors::SaslAuthenticationFailed,
            Some(broker_msg),
            2,
        ));

        let err = auth.authenticate(&mut transport).expect_err("step 4 must fail");
        let inner = err.into_inner().expect("inner");
        let kafka_err = inner.downcast_ref::<KafkaError>().expect("KafkaError captured");
        assert!(matches!(kafka_err, KafkaError::Authentication(m) if m == broker_msg));
        assert_eq!(auth.state(), SaslState::Failed);
    }

    /// EOF mid-handshake surfaces as `UnexpectedEof` (the upper layer
    /// translates this into a channel-disconnected event).
    ///
    /// Note: the state-machine loop sends `ApiVersionsRequest` then
    /// falls through to receive on the same `authenticate()` call.
    /// When the inbound queue is empty and `eof_after_drain` is true,
    /// the receive surfaces `UnexpectedEof` immediately. This matches
    /// Java's behaviour: a peer that closes the socket right after we
    /// send the bootstrap is indistinguishable from a slow peer that
    /// then closes — both surface as `EOFException` from
    /// `channel.read() == -1`.
    #[test]
    fn eof_mid_handshake_surfaces_as_unexpected_eof() {
        let mut transport = MockTransport::new();
        transport.eof_after_drain = true;
        let mut auth = SaslClientAuthenticator::new(
            "node-0",
            "test-client",
            "PLAIN",
            PlainCredentials::new("alice", "supersecret"),
        )
        .expect("authenticator");
        let err = auth.authenticate(&mut transport).expect_err("expected EOF");
        assert_eq!(err.kind(), io::ErrorKind::UnexpectedEof);
        // State advanced past Send to Receive — request was sent, then
        // we attempted to read and hit EOF. Upper layer disconnects.
        assert_eq!(auth.state(), SaslState::ReceiveApiVersionsResponse);
        // The outbound side definitely has the framed ApiVersionsRequest.
        assert!(!transport.outbound_bytes().is_empty(), "request was sent before EOF");
    }

    /// Reject construction for unsupported mechanisms — Phase 9b config
    /// validator boundary, surfaced here at construction for defense
    /// in depth.
    #[test]
    fn reject_non_plain_mechanism_at_construction() {
        let err = SaslClientAuthenticator::new("node-0", "test", "SCRAM-SHA-512", PlainCredentials::new("a", "b"))
            .expect_err("must reject");
        assert!(matches!(err, KafkaError::Config(_)));
        assert!(err.message().contains("Unsupported SASL mechanism: SCRAM-SHA-512"));
    }

    /// Translation of Java `SaslAuthenticatorTest.testCorrelationId`:
    /// IDs must be unique within the reserved range, must all be
    /// `>= MIN_RESERVED`, and `is_reserved` returns true for each.
    #[test]
    fn next_correlation_id_stays_in_reserved_range() {
        let mut auth = SaslClientAuthenticator::new("node-0", "test", "PLAIN", PlainCredentials::new("a", "b"))
            .expect("authenticator");
        // Java: `(MAX - MIN) * 2` iterations to exhaust + wrap.
        let count = (MAX_RESERVED_CORRELATION_ID as i64 - MIN_RESERVED_CORRELATION_ID as i64) * 2;
        let mut seen = std::collections::HashSet::new();
        for _ in 0..count {
            let id = auth.next_correlation_id();
            assert!(id >= MIN_RESERVED_CORRELATION_ID, "id {id} below MIN_RESERVED");
            assert!(is_reserved(id), "id {id} not reserved");
            seen.insert(id);
        }
        // The set of distinct IDs must equal the range size.
        assert_eq!(
            seen.len(),
            (MAX_RESERVED_CORRELATION_ID - MIN_RESERVED_CORRELATION_ID + 1) as usize
        );
    }

    /// `is_reserved` boundary check.
    #[test]
    fn is_reserved_boundary() {
        assert!(!is_reserved(0));
        assert!(!is_reserved(MIN_RESERVED_CORRELATION_ID - 1));
        assert!(is_reserved(MIN_RESERVED_CORRELATION_ID));
        assert!(is_reserved(MAX_RESERVED_CORRELATION_ID));
    }

    /// RFC 4616 PLAIN token format. Exposed for direct test access via
    /// the public `build_plain_token` flow — verifies the literal byte
    /// sequence on the wire is `\0user\0pass` (one NUL before, one
    /// between, no trailing NUL).
    #[test]
    fn plain_token_rfc_4616_byte_shape() {
        let auth =
            SaslClientAuthenticator::new("node-0", "test", "PLAIN", PlainCredentials::new("alice", "supersecret"))
                .expect("authenticator");
        let token = auth.build_plain_token();
        assert_eq!(token, b"\0alice\0supersecret");
    }

    /// Tagged-field response at v2 round-trips through the
    /// authenticator — the response data includes a tagged trailer
    /// that the parser must accept and discard.
    #[test]
    fn tagged_field_round_trip_on_authenticate_v2_response() {
        use crate::common::protocol::RawTaggedField;
        let mut transport = MockTransport::new();
        let mut auth = SaslClientAuthenticator::new("node-0", "test", "PLAIN", PlainCredentials::new("alice", "p"))
            .expect("authenticator");
        auth.authenticate(&mut transport).expect("step 1");
        transport.push_framed(&serialize_api_versions_response(MIN_RESERVED_CORRELATION_ID));
        auth.authenticate(&mut transport).expect("step 2");
        transport.push_framed(&serialize_sasl_handshake_response(
            MIN_RESERVED_CORRELATION_ID + 1,
            Errors::None,
            vec!["PLAIN".to_owned()],
            1,
        ));
        auth.authenticate(&mut transport).expect("step 3");

        // Hand-craft an authenticate response with a tagged-field on the
        // trailer (v2 flexible encoding).
        let response = SaslAuthenticateResponse::new(SaslAuthenticateResponseData {
            error_code: Errors::None.code(),
            error_message: None,
            auth_bytes: Vec::new(),
            session_lifetime_ms: 0,
            unknown_tagged_fields: vec![RawTaggedField::new(7, vec![0xAB, 0xCD])],
        });
        transport.push_framed(&wrap_with_header(
            response.api_key(),
            2,
            MIN_RESERVED_CORRELATION_ID + 2,
            &response,
            2,
        ));

        auth.authenticate(&mut transport).expect("step 4");
        assert_eq!(auth.state(), SaslState::Complete);
    }

    /// `PlainCredentials::Debug` masks the password.
    #[test]
    fn plain_credentials_debug_masks_password() {
        const PWD: &str = "very-secret-password-456";
        let creds = PlainCredentials::new("alice", PWD);
        let dbg = format!("{creds:?}");
        assert!(
            !dbg.contains(PWD),
            "PlainCredentials Debug leaked password! dbg.len()={}",
            dbg.len()
        );
        assert!(dbg.contains("<redacted>"));
        assert!(dbg.contains("alice")); // username is fine
    }
}
