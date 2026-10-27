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

//! SASL client-side authenticator state machine.
//!
//! Translated from `org.apache.kafka.common.security.authenticator.SaslClientAuthenticator`.
//!
//! Implements the SASL authentication flow for the client side:
//!
//! ```text
//! SendApiVersionsRequest → ReceiveApiVersionsResponse
//!   → SendHandshakeRequest → ReceiveHandshakeResponse
//!   → Initial (send PLAIN token) → Intermediate (receive response)
//!   → ClientComplete (if using SaslAuthenticate header) → Complete
//! ```
//!
//! and the KIP-368 re-authentication flow, which begins when the broker has
//! given the session a lifetime and that lifetime is nearly over:
//!
//! ```text
//! ReauthProcessOrigApiVersionsResponse → ReauthSendHandshakeRequest
//!   → ReauthReceiveHandshakeOrOtherResponse → ReauthInitial
//!   → Intermediate → ... → Complete
//! ```
//!
//! Currently supports PLAIN mechanism only (RFC 4616); the client
//! configurations reject any other `sasl.mechanism` when they are built
//! (`CommonClientConfigs::post_validate_supported_sasl_mechanism`), so no other
//! mechanism reaches this authenticator through a client. The state machine is
//! designed for extensibility to SCRAM and other challenge-response mechanisms.
//!
//! # Non-blocking Design
//!
//! `authenticate()` is called repeatedly by `KafkaChannel::prepare()`. Each call:
//! 1. Flushes pending outbound data
//! 2. Attempts one state transition
//! 3. Returns `Ok(())` if I/O would block (partial read/write)
//! 4. Stores partial reads in `net_in_buffer` for the next call

use crate::SaslAuthenticateRequestData;
use crate::SaslHandshakeRequestData;
use crate::common::Error;
use crate::common::errors::{IllegalSaslStateError, UnsupportedSaslMechanismError};
use crate::common::network;
use crate::common::network::Authenticator;
use crate::common::network::ByteBufferSend;
use crate::common::network::NetworkReceive;
use crate::common::network::ReauthenticationContext;
use crate::common::network::Receive;
use crate::common::network::{InterestOps, TransportLayer};
use crate::common::network::{auth_io_error, authentication_io_error};
use crate::common::protocol::{ApiKeys, ByteBufferAccessor, Errors};
use crate::common::requests::AbstractRequest;
use crate::common::requests::ApiVersionsResponse;
use crate::common::requests::ConcreteResponse;
use crate::common::requests::RequestBuilder;
use crate::common::requests::SaslAuthenticateRequest;
use crate::common::requests::SaslHandshakeRequest;
use crate::common::requests::SaslHandshakeResponse;
use crate::common::requests::api_versions_request;
use crate::common::requests::{RequestHeader, RequestHeaderOptionsBuilder};

use crate::common::utils::Time;
use crate::common::utils::internals::LogContext;
use crate::kafka_debug;

use std::collections::VecDeque;
use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::Arc;

/// Internal state transitions for SASL client authentication.
///
/// The internal state transitions for initial authentication of a channel are
/// declared in order, starting with `SendApiVersionsRequest` and ending in either
/// `Complete` or `Failed`.
///
/// Re-authentication of a channel starts with the state
/// `ReauthProcessOrigApiVersionsResponse` and then flows to
/// `ReauthSendHandshakeRequest` followed by
/// `ReauthReceiveHandshakeOrOtherResponse` and then `ReauthInitial`; after that
/// the flow joins the authentication flow at the `Intermediate` state and ends at
/// either `Complete` or `Failed`.
///
/// Translated from `SaslClientAuthenticator.SaslState` in Java.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[doc(alias = "org.apache.kafka.common.security.authenticator.SaslClientAuthenticator$SaslState")]
pub enum SaslState {
    /// Initial state: client sends ApiVersionsRequest.
    SendApiVersionsRequest,
    /// Awaiting ApiVersionsResponse from server.
    ReceiveApiVersionsResponse,
    /// Received ApiVersionsResponse, send SaslHandshake request.
    SendHandshakeRequest,
    /// Awaiting SaslHandshake response from server.
    ReceiveHandshakeResponse,
    /// Initial authentication state: send first SASL token.
    Initial,
    /// Intermediate state during SASL token exchange: process challenges and send responses.
    Intermediate,
    /// Sent response to last challenge. If using SaslAuthenticate, wait for server status.
    ClientComplete,
    /// Authentication sequence complete.
    Complete,
    /// Failed authentication due to an error at some stage.
    Failed,
    /// Initial state for re-authentication: process ApiVersionsResponse from
    /// original authentication.
    ReauthProcessOrigApiVersionsResponse,
    /// Processed original ApiVersionsResponse, send SaslHandshake request as part
    /// of re-authentication.
    ReauthSendHandshakeRequest,
    /// Awaiting SaslHandshake response from server when re-authenticating, and may
    /// receive other, in-flight responses sent prior to start of re-authentication
    /// as well.
    ReauthReceiveHandshakeOrOtherResponse,
    /// Initial re-authentication state starting SASL token exchange for configured
    /// mechanism, send first token.
    ReauthInitial,
}

/// SASL client authenticator that implements the SASL PLAIN authentication flow.
///
/// Translated from `org.apache.kafka.common.security.authenticator.SaslClientAuthenticator`.
///
/// This implementation supports PLAIN mechanism only (RFC 4616). The token format
/// is `\0username\0password`. The client configurations reject any other
/// `sasl.mechanism` when they are built, so a client never sends this token
/// under another mechanism's name.
///
/// # Re-authentication
///
/// When the broker gives the session a lifetime (`connections.max.reauth.ms`,
/// KIP-368), the authenticator schedules re-authentication at a random point
/// between 85% and 95% of it, and the channel re-authenticates on its first
/// write after that point ([`KafkaChannel::maybe_begin_client_reauthentication`]).
/// Java re-authenticates with a new authenticator; this one starts over itself
/// ([`Authenticator::reauthenticate`]).
///
/// [`KafkaChannel::maybe_begin_client_reauthentication`]: crate::common::network::KafkaChannel::maybe_begin_client_reauthentication
#[doc(alias = "org.apache.kafka.common.security.authenticator.SaslClientAuthenticator")]
pub struct SaslClientAuthenticator {
    /// Current SASL state.
    state: SaslState,
    /// SASL mechanism name (e.g., "PLAIN").
    mechanism: String,
    /// Username for PLAIN authentication.
    username: String,
    /// Password for PLAIN authentication.
    password: String,
    /// The node identifier for this connection.
    node: String,
    /// The broker hostname.
    /// Used by GSSAPI/Kerberos for service principal construction; retained for
    /// future mechanism support.
    #[expect(dead_code)]
    host: String,
    /// The Kafka client ID for request headers.
    client_id: String,
    /// Correlation ID counter for the next request.
    correlation_id: i32,
    /// Version of SaslHandshake request/responses.
    sasl_handshake_version: i16,
    /// Version of SaslAuthenticate request/responses.
    /// `-1` means no SaslAuthenticate header (legacy mode).
    sasl_authenticate_version: i16,
    /// Request header for which a response from the server is pending.
    current_request_header: Option<RequestHeader>,
    /// Pending outbound data.
    net_out_buffer: Option<Box<dyn network::Send>>,
    /// Pending inbound data.
    net_in_buffer: Option<NetworkReceive>,
    /// Next SASL state to be set when outgoing writes complete.
    pending_sasl_state: Option<SaslState>,
    /// Information related to re-authentication.
    reauth_info: ReauthInfo,
    /// The clock the session's re-authentication time is computed with: the
    /// channel compares that time against the selector's clock, which is the
    /// same one.
    time: Arc<dyn Time>,
    /// Contextual log message prefix.
    log_context: LogContext,
}

impl SaslClientAuthenticator {
    /// Returns `true` if the correlation ID is reserved for SASL requests.
    #[doc(alias = "org.apache.kafka.common.security.authenticator.SaslClientAuthenticator#isReserved")]
    pub fn is_reserved(correlation_id: i32) -> bool {
        correlation_id >= SaslClientAuthenticator::SASL_CLIENT_AUTHENTICATOR_MIN_RESERVED_CORRELATION_ID
    }

    /// Sentinel version value indicating that the Kafka SASL authenticate header
    /// should not be used (legacy mode, pre-KIP-152).
    const DISABLE_KAFKA_SASL_AUTHENTICATE_HEADER: i16 = -1;

    /// The maximum reserved correlation ID for SASL requests.
    ///
    /// The reserved range of correlation IDs for SASL requests ensures that SASL
    /// requests are separated from those used in `NetworkClient` for Kafka requests.
    /// This prevents mismatched correlation IDs during re-authentication.
    pub const SASL_CLIENT_AUTHENTICATOR_MAX_RESERVED_CORRELATION_ID: i32 = i32::MAX;

    /// The minimum reserved correlation ID for SASL requests.
    ///
    /// Only one request is expected in-flight at a time during authentication,
    /// so the small range (8 values) is sufficient.
    pub const SASL_CLIENT_AUTHENTICATOR_MIN_RESERVED_CORRELATION_ID: i32 =
        Self::SASL_CLIENT_AUTHENTICATOR_MAX_RESERVED_CORRELATION_ID - 7;

    /// Creates a new `SaslClientAuthenticator`.
    ///
    /// # Arguments
    ///
    /// * `mechanism` - SASL mechanism name (e.g., "PLAIN")
    /// * `username` - Username for PLAIN authentication
    /// * `password` - Password for PLAIN authentication
    /// * `node` - Node identifier for this connection
    /// * `host` - Broker hostname
    /// * `client_id` - Kafka client ID for request headers
    /// * `time` - The clock the session's re-authentication time is computed with
    #[doc(alias = "org.apache.kafka.common.security.authenticator.SaslClientAuthenticator#SaslClientAuthenticator")]
    #[expect(clippy::too_many_arguments)]
    pub fn new(
        mechanism: &str,
        username: &str,
        password: &str,
        node: &str,
        host: &str,
        client_id: &str,
        time: Arc<dyn Time>,
        log_context: LogContext,
    ) -> Self {
        let mut authenticator = Self {
            state: SaslState::SendApiVersionsRequest,
            mechanism: mechanism.to_string(),
            username: username.to_string(),
            password: password.to_string(),
            node: node.to_string(),
            host: host.to_string(),
            client_id: client_id.to_string(),
            correlation_id: 0,
            sasl_handshake_version: 0,
            sasl_authenticate_version: SaslClientAuthenticator::DISABLE_KAFKA_SASL_AUTHENTICATE_HEADER,
            current_request_header: None,
            net_out_buffer: None,
            net_in_buffer: None,
            pending_sasl_state: None,
            reauth_info: ReauthInfo::new(),
            time,
            log_context,
        };
        authenticator.set_sasl_state(SaslState::SendApiVersionsRequest);
        authenticator
    }

    /// Returns the current SASL state.
    pub fn sasl_state(&self) -> SaslState {
        self.state
    }

    /// Returns the SASL handshake version negotiated with the broker.
    pub fn sasl_handshake_version(&self) -> i16 {
        self.sasl_handshake_version
    }

    /// Returns the SASL authenticate version negotiated with the broker.
    ///
    /// Returns `-1` if legacy mode (no SaslAuthenticate header).
    pub fn sasl_authenticate_version(&self) -> i16 {
        self.sasl_authenticate_version
    }

    /// Allocates the next correlation ID from the reserved range.
    #[doc(alias = "org.apache.kafka.common.security.authenticator.SaslClientAuthenticator#nextCorrelationId")]
    fn next_correlation_id(&mut self) -> i32 {
        if !SaslClientAuthenticator::is_reserved(self.correlation_id) {
            self.correlation_id = SaslClientAuthenticator::SASL_CLIENT_AUTHENTICATOR_MIN_RESERVED_CORRELATION_ID;
        }
        let id = self.correlation_id;
        self.correlation_id = self.correlation_id.wrapping_add(1);
        id
    }

    /// Creates the next request header for the given API key and version.
    #[doc(alias = "org.apache.kafka.common.security.authenticator.SaslClientAuthenticator#nextRequestHeader")]
    fn next_request_header(&mut self, api_key: &'static ApiKeys, version: i16) -> io::Result<RequestHeader> {
        let correlation_id = self.next_correlation_id();
        let header = RequestHeader::with_options(
            RequestHeaderOptionsBuilder::new()
                .set_request_api_key(api_key)
                .set_request_version(version)
                .set_client_id(&self.client_id)
                .set_correlation_id(correlation_id)
                .build()
                .expect("RequestHeaderOptionsBuilder::build: every mandatory parameter is set above"),
        )?;
        self.current_request_header = Some(header.clone());
        Ok(header)
    }

    /// Creates a PLAIN SASL token in RFC 4616 format: `\0username\0password`.
    ///
    /// For PLAIN, the token is always the same regardless of whether this is
    /// an initial token or a challenge response.
    #[doc(alias = "org.apache.kafka.common.security.authenticator.SaslClientAuthenticator#createSaslToken")]
    fn create_sasl_token(&self) -> Vec<u8> {
        // PLAIN token: \0<username>\0<password>
        let mut token = Vec::with_capacity(1 + self.username.len() + 1 + self.password.len());
        token.push(0);
        token.extend_from_slice(self.username.as_bytes());
        token.push(0);
        token.extend_from_slice(self.password.as_bytes());
        token
    }

    /// Sends an API request through the transport layer.
    ///
    /// Sets `net_out_buffer` and attempts to flush. On I/O error, transitions
    /// to `Failed` state.
    async fn send_request(
        &mut self,
        send: Box<dyn network::Send>,
        transport: &mut (dyn TransportLayer + Send),
    ) -> io::Result<()> {
        self.net_out_buffer = Some(send);
        match self.flush_net_out_buffer_and_update_interest_ops(transport).await {
            Ok(_) => Ok(()),
            Err(e) => {
                self.set_sasl_state(SaslState::Failed);
                Err(e)
            },
        }
    }

    /// Sends the initial SASL token (PLAIN: `\0username\0password`).
    ///
    /// Corresponds to `sendInitialToken()` and `sendSaslClientToken()` in Java.
    #[doc(alias = "org.apache.kafka.common.security.authenticator.SaslClientAuthenticator#sendInitialToken")]
    async fn send_initial_token(&mut self, transport: &mut (dyn TransportLayer + Send)) -> io::Result<()> {
        self.send_sasl_client_token(transport).await
    }

    /// Sends a SASL client token to the server.
    ///
    /// For PLAIN, this always sends the token. When `sasl_authenticate_version`
    /// is `-1` (legacy mode), the token is sent as a size-prefixed raw byte
    /// buffer. Otherwise, it's wrapped in a SaslAuthenticate request.
    ///
    /// Returns `true` if a token was sent.
    #[doc(alias = "org.apache.kafka.common.security.authenticator.SaslClientAuthenticator#sendSaslClientToken")]
    async fn send_sasl_client_token(&mut self, transport: &mut (dyn TransportLayer + Send)) -> io::Result<()> {
        let sasl_token = self.create_sasl_token();
        let send: Box<dyn network::Send> =
            if self.sasl_authenticate_version == SaslClientAuthenticator::DISABLE_KAFKA_SASL_AUTHENTICATE_HEADER {
                Box::new(ByteBufferSend::size_prefixed(bytes::Bytes::from(sasl_token)))
            } else {
                let mut data = SaslAuthenticateRequestData::new();
                data.set_auth_bytes(sasl_token);
                let request = SaslAuthenticateRequest::new(data, self.sasl_authenticate_version);
                let header = self.next_request_header(&ApiKeys::SASL_AUTHENTICATE, self.sasl_authenticate_version)?;
                let mut concrete = AbstractRequest::SaslAuthenticate(request);
                let byte_buffer_send = concrete.to_send(&header)?;
                Box::new(byte_buffer_send)
            };
        self.send_request(send, transport).await
    }

    /// Flushes the outbound buffer to the transport and updates interest ops.
    ///
    /// Returns `true` if the buffer was completely flushed.
    #[doc(
        alias = "org.apache.kafka.common.security.authenticator.SaslClientAuthenticator#flushNetOutBufferAndUpdateInterestOps"
    )]
    async fn flush_net_out_buffer_and_update_interest_ops(
        &mut self,
        transport: &mut (dyn TransportLayer + Send),
    ) -> io::Result<bool> {
        let flushed_completely = self.flush_net_out_buffer(transport).await?;
        if flushed_completely {
            transport.remove_interest_ops(InterestOps::OP_WRITE);
            if let Some(pending) = self.pending_sasl_state.take() {
                self.set_sasl_state(pending);
            }
        } else {
            transport.add_interest_ops(InterestOps::OP_WRITE);
        }
        Ok(flushed_completely)
    }

    /// Writes pending data from the outbound buffer to the transport.
    ///
    /// Returns `true` if the buffer is completely written.
    #[doc(alias = "org.apache.kafka.common.security.authenticator.SaslClientAuthenticator#flushNetOutBuffer")]
    async fn flush_net_out_buffer(&mut self, transport: &mut (dyn TransportLayer + Send)) -> io::Result<bool> {
        if let Some(ref mut buf) = self.net_out_buffer {
            if !buf.completed() {
                buf.write_to(transport).await?;
            }
            Ok(buf.completed())
        } else {
            Ok(true)
        }
    }

    /// Reads a size-delimited response or token from the transport.
    ///
    /// Returns `None` if the read is incomplete (would block), `Some(bytes)`
    /// when a complete message is available.
    #[doc(alias = "org.apache.kafka.common.security.authenticator.SaslClientAuthenticator#receiveResponseOrToken")]
    async fn receive_response_or_token(
        &mut self,
        transport: &mut (dyn TransportLayer + Send),
    ) -> io::Result<Option<Vec<u8>>> {
        if self.net_in_buffer.is_none() {
            self.net_in_buffer = Some(NetworkReceive::with_source(&self.node));
        }
        let net_in = self.net_in_buffer.as_mut().unwrap();
        net_in.read_from(transport).await?;
        if net_in.complete() {
            let payload = net_in.payload().map(|p| p.to_vec());
            self.net_in_buffer = None;
            Ok(payload)
        } else {
            Ok(None)
        }
    }

    /// Receives and parses a Kafka response (header + body).
    ///
    /// Returns `None` if the read is incomplete. On successful read, validates
    /// the correlation ID against `current_request_header`.
    #[doc(alias = "org.apache.kafka.common.security.authenticator.SaslClientAuthenticator#receiveKafkaResponse")]
    async fn receive_kafka_response(
        &mut self,
        transport: &mut (dyn TransportLayer + Send),
    ) -> io::Result<Option<ConcreteResponse>> {
        // Java's `catch (BufferUnderflowException | SchemaException |
        // IllegalArgumentException e)` covers only the *parse* of the response
        // (handled on the `parse_response` call below). `receiveResponseOrToken()`
        // throws `IOException`, which that clause does NOT catch, so a transient
        // read failure propagates as a network disconnect — retriable, reconnect
        // with backoff — rather than becoming a fatal authentication failure.
        let response_bytes = match self.receive_response_or_token(transport).await? {
            Some(bytes) => bytes,
            None => return Ok(None),
        };

        let request_header = self
            .current_request_header
            .as_ref()
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "No pending request header for SASL response"))?;
        let mut buffer = ByteBufferAccessor::new(response_bytes);
        // Java routes through `NetworkClient.parseResponse` (`:824-840`), not the
        // raw parse, so a correlation-id mismatch is converted to a
        // `SchemaException` **only** when the request used a reserved SASL
        // correlation id and the response did not; otherwise it is rethrown.
        //
        // Java then catches exactly three classes here:
        // `BufferUnderflowException | SchemaException | IllegalArgumentException`
        // (`SaslClientAuthenticator.java`). A rethrown
        // `CorrelationIdMismatchException` is an `IllegalStateException` and is
        // NOT among them, so it propagates out of `receiveKafkaResponse` without
        // failing authentication. Catching everything here — as this did before —
        // turned that case into a fatal authentication failure.
        //
        let response = match crate::NetworkClientStatics::parse_response(&mut buffer, request_header) {
            Ok(response) => response,
            Err(error) if matches!(error, Error::Schema(_) | Error::LocalIllegalArgument(_)) => {
                // Account for the fact that during re-authentication there may be
                // responses arriving for requests that were sent in the past.
                if self.reauth_info.reauthenticating() {
                    // It didn't match the current request header, so it must be
                    // unrelated to re-authentication. Save it so it can be
                    // processed later. The whole frame is kept, as Java rewinds
                    // the receive's payload.
                    self.reauth_info
                        .pending_authenticated_receives
                        .push_back(NetworkReceive::with_source_buffer(&self.node, buffer.into_buffer()));
                    return Ok(None);
                }
                kafka_debug!(
                    self.log_context,
                    "Invalid SASL mechanism response, server may be expecting only GSSAPI tokens"
                );
                self.set_sasl_state(SaslState::Failed);
                // Java: `throw new IllegalSaslStateException(msg, e)` — the
                // two-argument form, so the message is Java's literal text and the
                // cause hangs off `getCause()` rather than being appended to it.
                return Err(authentication_io_error(Error::IllegalSaslState(
                    IllegalSaslStateError::with_source(
                        "Invalid SASL mechanism response, server may be expecting a different protocol",
                        error,
                    ),
                )));
            },
            Err(error) => {
                // Outside Java's catch: propagate without touching the SASL state
                // and without classifying it as an authentication failure.
                return Err(io::Error::new(io::ErrorKind::InvalidData, error.to_string()));
            },
        };
        self.current_request_header = None;
        Ok(Some(response))
    }

    /// Receives a SASL token from the server.
    ///
    /// In legacy mode (no SaslAuthenticate header), this reads a raw size-delimited
    /// token. Otherwise, it parses a SaslAuthenticateResponse and validates the
    /// error code.
    #[doc(alias = "org.apache.kafka.common.security.authenticator.SaslClientAuthenticator#receiveToken")]
    async fn receive_token(&mut self, transport: &mut (dyn TransportLayer + Send)) -> io::Result<Option<Vec<u8>>> {
        if self.sasl_authenticate_version == SaslClientAuthenticator::DISABLE_KAFKA_SASL_AUTHENTICATE_HEADER {
            self.receive_response_or_token(transport).await
        } else {
            match self.receive_kafka_response(transport).await? {
                Some(ConcreteResponse::SaslAuthenticate(response)) => {
                    let error = response.error();
                    if error != Errors::None {
                        self.set_sasl_state(SaslState::Failed);
                        // Java: `throw errMsg == null ? error.exception() :
                        // error.exception(errMsg)` — the class the code names. A
                        // rejected password is SASL_AUTHENTICATION_FAILED, so a
                        // `SaslAuthenticationException` (code 58): a fatal
                        // authentication failure, not a retriable disconnect.
                        let error = error.error_with_optional_message(response.error_message());
                        return Err(if error.is_authentication_error() {
                            authentication_io_error(error)
                        } else {
                            // Not an `AuthenticationException`: Java's
                            // `KafkaChannel.prepare` does not catch it, so the
                            // selector closes the connection as a disconnect.
                            io::Error::other(error)
                        });
                    }
                    let session_lifetime_ms = response.session_lifetime_ms();
                    if session_lifetime_ms > 0 {
                        self.reauth_info.positive_session_lifetime_ms = Some(session_lifetime_ms);
                    }
                    Ok(Some(response.sasl_auth_bytes().to_vec()))
                },
                Some(_) => Err(auth_io_error("Expected SaslAuthenticate response")),
                None => Ok(None),
            }
        }
    }

    /// Sets the SASL state, deferring the transition if there is pending outbound data.
    #[doc(alias = "org.apache.kafka.common.security.authenticator.SaslClientAuthenticator#setSaslState")]
    fn set_sasl_state(&mut self, sasl_state: SaslState) {
        if let Some(ref buf) = self.net_out_buffer
            && !buf.completed()
        {
            self.pending_sasl_state = Some(sasl_state);
            return;
        }
        self.pending_sasl_state = None;
        self.state = sasl_state;
        kafka_debug!(self.log_context, "Set SASL client state to {:?}", sasl_state);
        if sasl_state == SaslState::Complete {
            self.reauth_info
                .set_authentication_end_and_session_reauthentication_times(self.time.nanoseconds(), &self.log_context);
            // Java also removes OP_WRITE after an authentication, and adds it
            // after a re-authentication so the write that triggered it is sent.
            // The selector here derives a channel's interest from its state
            // instead (a ready channel with a queued send is armed for write),
            // so there is no interest to update.
        }
    }

    /// Extracts SASL handshake and authenticate versions from the ApiVersionsResponse.
    ///
    /// Translated from `setSaslAuthenticateAndHandshakeVersions` in Java.
    #[doc(
        alias = "org.apache.kafka.common.security.authenticator.SaslClientAuthenticator#setSaslAuthenticateAndHandshakeVersions"
    )]
    fn set_sasl_authenticate_and_handshake_versions(&mut self, api_versions_response: &ApiVersionsResponse) {
        if let Some(auth_version) = api_versions_response.api_version(ApiKeys::SASL_AUTHENTICATE.id()) {
            self.sasl_authenticate_version = auth_version.max_version.min(ApiKeys::SASL_AUTHENTICATE.latest_version());
        }
        if let Some(hs_version) = api_versions_response.api_version(ApiKeys::SASL_HANDSHAKE.id()) {
            self.sasl_handshake_version = hs_version.max_version.min(ApiKeys::SASL_HANDSHAKE.latest_version());
        }
    }

    /// Validates the SaslHandshake response.
    ///
    /// Checks the error code and throws descriptive errors for unsupported
    /// mechanisms or illegal SASL states.
    #[doc(alias = "org.apache.kafka.common.security.authenticator.SaslClientAuthenticator#handleSaslHandshakeResponse")]
    fn handle_sasl_handshake_response(&mut self, response: &SaslHandshakeResponse) -> io::Result<()> {
        let error = response.error();
        if error != Errors::None {
            self.set_sasl_state(SaslState::Failed);
        }
        // All non-None handshake errors are AuthenticationException subclasses
        // in Java (UnsupportedSaslMechanismException / IllegalSaslStateException):
        // genuine authentication failures, fatal and not retried. Java formats
        // the mechanisms as a `List` (`[PLAIN, SCRAM-SHA-256]`) and the code by
        // its constant name (`UNKNOWN_SERVER_ERROR`).
        let enabled_mechanisms = format!("[{}]", response.enabled_mechanisms().join(", "));
        match error {
            Errors::None => Ok(()),
            Errors::UnsupportedSaslMechanism => Err(authentication_io_error(Error::UnsupportedSaslMechanism(
                UnsupportedSaslMechanismError::new(format!(
                    "Client SASL mechanism '{}' not enabled in the server, enabled mechanisms are {}",
                    self.mechanism, enabled_mechanisms
                )),
            ))),
            Errors::IllegalSaslState => Err(authentication_io_error(Error::IllegalSaslState(
                IllegalSaslStateError::new(format!(
                    "Unexpected handshake request with client mechanism {}, enabled mechanisms are {}",
                    self.mechanism, enabled_mechanisms
                )),
            ))),
            _ => Err(authentication_io_error(Error::IllegalSaslState(IllegalSaslStateError::new(
                format!(
                    "Unknown error code {}, client mechanism is {}, enabled mechanisms are {}",
                    error, self.mechanism, enabled_mechanisms
                ),
            )))),
        }
    }

    /// The main authenticate loop. This is the async implementation that is
    /// called from the `Authenticator` trait.
    async fn authenticate_impl(&mut self, transport: &mut (dyn TransportLayer + Send)) -> io::Result<()> {
        // Flush any pending outbound data first
        if self.net_out_buffer.is_some() && !self.flush_net_out_buffer_and_update_interest_ops(transport).await? {
            return Ok(());
        }

        match self.state {
            SaslState::SendApiVersionsRequest => {
                // Always use version 0 request since brokers treat requests with
                // schema exceptions as GSSAPI tokens
                let mut builder = api_versions_request::Builder::with_version(0);
                let mut request = builder.build()?;
                let header = self.next_request_header(&ApiKeys::API_VERSIONS, request.version())?;
                let send = Box::new(request.to_send(&header)?);
                self.send_request(send, transport).await?;
                self.set_sasl_state(SaslState::ReceiveApiVersionsResponse);
            },
            SaslState::ReceiveApiVersionsResponse => {
                let response = self.receive_kafka_response(transport).await?;
                if let Some(ConcreteResponse::ApiVersions(api_versions_response)) = response {
                    self.set_sasl_authenticate_and_handshake_versions(&api_versions_response);
                    self.reauth_info.api_versions_response_received_from_broker = Some(api_versions_response);
                    self.set_sasl_state(SaslState::SendHandshakeRequest);
                    // Fall through to send handshake request
                    self.send_handshake_request(transport).await?;
                    self.set_sasl_state(SaslState::ReceiveHandshakeResponse);
                } else if response.is_some() {
                    return Err(auth_io_error("Expected ApiVersions response during SASL authentication"));
                }
                // response is None -> I/O incomplete, return and try again
            },
            SaslState::SendHandshakeRequest => {
                self.send_handshake_request(transport).await?;
                self.set_sasl_state(SaslState::ReceiveHandshakeResponse);
            },
            SaslState::ReceiveHandshakeResponse => {
                let response = self.receive_kafka_response(transport).await?;
                if let Some(ConcreteResponse::SaslHandshake(handshake_response)) = response {
                    self.handle_sasl_handshake_response(&handshake_response)?;
                    self.set_sasl_state(SaslState::Initial);
                    // Fall through and start SASL authentication
                    self.send_initial_token(transport).await?;
                    self.set_sasl_state(SaslState::Intermediate);
                } else if response.is_some() {
                    return Err(auth_io_error("Expected SaslHandshake response during SASL authentication"));
                }
                // response is None -> I/O incomplete, return and try again
            },
            SaslState::Initial => {
                self.send_initial_token(transport).await?;
                self.set_sasl_state(SaslState::Intermediate);
            },
            SaslState::ReauthProcessOrigApiVersionsResponse => {
                if let Some(original) = self.reauth_info.api_versions_response_from_original_authentication.take() {
                    self.set_sasl_authenticate_and_handshake_versions(&original);
                    self.reauth_info.api_versions_response_from_original_authentication = Some(original);
                }
                self.set_sasl_state(SaslState::ReauthSendHandshakeRequest); // Will set immediately
                // Fall through to send handshake request with the latest supported version
                self.send_handshake_request(transport).await?;
                self.set_sasl_state(SaslState::ReauthReceiveHandshakeOrOtherResponse);
            },
            SaslState::ReauthSendHandshakeRequest => {
                self.send_handshake_request(transport).await?;
                self.set_sasl_state(SaslState::ReauthReceiveHandshakeOrOtherResponse);
            },
            SaslState::ReauthReceiveHandshakeOrOtherResponse => {
                let response = self.receive_kafka_response(transport).await?;
                if let Some(ConcreteResponse::SaslHandshake(handshake_response)) = response {
                    self.handle_sasl_handshake_response(&handshake_response)?;
                    self.set_sasl_state(SaslState::ReauthInitial); // Will set immediately
                    // Fall through and start SASL authentication using the
                    // configured client mechanism.
                    self.send_initial_token(transport).await?;
                    self.set_sasl_state(SaslState::Intermediate);
                } else if response.is_some() {
                    return Err(auth_io_error("Expected SaslHandshake response during SASL re-authentication"));
                }
                // response is None -> I/O incomplete, or a response to an earlier
                // request that was set aside; return and try again
            },
            SaslState::ReauthInitial => {
                self.send_initial_token(transport).await?;
                self.set_sasl_state(SaslState::Intermediate);
            },
            SaslState::Intermediate => {
                let server_token = self.receive_token(transport).await?;
                if server_token.is_some() {
                    // For PLAIN mechanism, the client is always complete after
                    // the initial token exchange. The saslClient.isComplete()
                    // check in Java always returns true for PLAIN after
                    // evaluateChallenge, and sendSaslClientToken returns false
                    // (no additional token to send), making noResponsesPending
                    // true. So for PLAIN we go directly to Complete.
                    //
                    // The CLIENT_COMPLETE state is used by challenge-response
                    // mechanisms (SCRAM) where the client sends a final response
                    // and waits for the server's acknowledgment.
                    self.set_sasl_state(SaslState::Complete);
                }
                // server_token is None -> I/O incomplete, return and try again
            },
            SaslState::ClientComplete => {
                // This state is used by challenge-response mechanisms (SCRAM)
                // where the client has sent its final token and waits for the
                // server's acknowledgment via SaslAuthenticate.
                // For PLAIN, we never enter this state.
                let server_response = self.receive_token(transport).await?;
                if server_response.is_some() {
                    self.set_sasl_state(SaslState::Complete);
                }
            },
            SaslState::Complete => {
                // Nothing to do
            },
            SaslState::Failed => {
                return Err(io::Error::other("SASL handshake has already failed"));
            },
        }
        Ok(())
    }

    /// Sends a SaslHandshake request with the negotiated version.
    #[doc(alias = "org.apache.kafka.common.security.authenticator.SaslClientAuthenticator#sendHandshakeRequest")]
    async fn send_handshake_request(&mut self, transport: &mut (dyn TransportLayer + Send)) -> io::Result<()> {
        let mut data = SaslHandshakeRequestData::new();
        data.set_mechanism(self.mechanism.clone());
        let request = SaslHandshakeRequest::new(data, self.sasl_handshake_version);
        let header = self.next_request_header(&ApiKeys::SASL_HANDSHAKE, request.version())?;
        let mut concrete = AbstractRequest::SaslHandshake(request);
        let send = Box::new(concrete.to_send(&header)?);
        self.send_request(send, transport).await
    }
}

impl Authenticator for SaslClientAuthenticator {
    fn authenticate<'a>(
        &'a mut self,
        transport: &'a mut (dyn TransportLayer + Send),
    ) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send + 'a>> {
        Box::pin(self.authenticate_impl(transport))
    }

    fn complete(&self) -> bool {
        self.state == SaslState::Complete
    }

    /// Begins re-authentication with the versions negotiated by the original
    /// authentication, then sends the SASL handshake.
    ///
    /// Java builds a new authenticator for this, which reads the original
    /// `ApiVersionsResponse` from the previous one (`reauthInfo.apiVersionsResponse()`)
    /// and closes it. This authenticator is the previous one, so it keeps that
    /// response and starts over: the session lifetime and the re-authentication
    /// time of the previous session are cleared, as they are null on a new
    /// authenticator, and the receive the channel had in progress becomes the
    /// buffer the authenticator reads into first.
    fn reauthenticate<'a>(
        &'a mut self,
        mut reauthentication_context: ReauthenticationContext,
        transport: &'a mut (dyn TransportLayer + Send),
    ) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send + 'a>> {
        Box::pin(async move {
            let api_versions_response_from_original_authentication = self
                .reauth_info
                .api_versions_response_from_original_authentication
                .take()
                .or_else(|| self.reauth_info.api_versions_response_received_from_broker.take())
                .ok_or_else(|| {
                    // Java's `Objects.requireNonNull`: the channel re-authenticates
                    // only once it is ready, so the original exchange happened.
                    io::Error::other("Cannot re-authenticate before the original authentication completed")
                })?;
            self.reauth_info
                .reauthenticating_with_api_versions_response_from_original_authentication_reauthentication_begin_nanos(
                    api_versions_response_from_original_authentication,
                    reauthentication_context.reauthentication_begin_nanos(),
                );
            self.reauth_info.positive_session_lifetime_ms = None;
            self.reauth_info.client_session_reauthentication_time_nanos = None;
            self.current_request_header = None;
            self.net_out_buffer = None;
            self.pending_sasl_state = None;
            self.net_in_buffer = reauthentication_context.network_receive();
            self.set_sasl_state(SaslState::ReauthProcessOrigApiVersionsResponse); // Will set immediately
            self.authenticate_impl(transport).await
        })
    }

    fn client_session_reauthentication_time_nanos(&self) -> Option<i64> {
        self.reauth_info.client_session_reauthentication_time_nanos
    }

    fn reauthentication_latency_ms(&self) -> Option<u64> {
        self.reauth_info.reauthentication_latency_ms()
    }

    fn poll_response_received_during_reauthentication(&mut self) -> Option<NetworkReceive> {
        self.reauth_info.poll_response_received_during_reauthentication()
    }

    fn close(&mut self) {
        // No resources to release for PLAIN mechanism.
    }
}

/// Information related to re-authentication.
///
/// Translated from Java's private inner class `SaslClientAuthenticator.ReauthInfo`.
#[doc(alias = "org.apache.kafka.common.security.authenticator.SaslClientAuthenticator$ReauthInfo")]
struct ReauthInfo {
    api_versions_response_from_original_authentication: Option<ApiVersionsResponse>,
    reauthentication_begin_nanos: i64,
    pending_authenticated_receives: VecDeque<NetworkReceive>,
    api_versions_response_received_from_broker: Option<ApiVersionsResponse>,
    positive_session_lifetime_ms: Option<i64>,
    authentication_end_nanos: i64,
    client_session_reauthentication_time_nanos: Option<i64>,
}

impl ReauthInfo {
    fn new() -> Self {
        Self {
            api_versions_response_from_original_authentication: None,
            reauthentication_begin_nanos: 0,
            pending_authenticated_receives: VecDeque::new(),
            api_versions_response_received_from_broker: None,
            positive_session_lifetime_ms: None,
            authentication_end_nanos: 0,
            client_session_reauthentication_time_nanos: None,
        }
    }

    /// Marks the start of a re-authentication, which uses the versions
    /// negotiated by the original authentication.
    #[doc(alias = "org.apache.kafka.common.security.authenticator.SaslClientAuthenticator$ReauthInfo#reauthenticating")]
    fn reauthenticating_with_api_versions_response_from_original_authentication_reauthentication_begin_nanos(
        &mut self,
        api_versions_response_from_original_authentication: ApiVersionsResponse,
        reauthentication_begin_nanos: i64,
    ) {
        self.api_versions_response_from_original_authentication =
            Some(api_versions_response_from_original_authentication);
        self.reauthentication_begin_nanos = reauthentication_begin_nanos;
    }

    /// Returns `true` if this authenticator is re-authenticating, or has
    /// re-authenticated, the channel.
    #[doc(alias = "org.apache.kafka.common.security.authenticator.SaslClientAuthenticator$ReauthInfo#reauthenticating")]
    fn reauthenticating(&self) -> bool {
        self.api_versions_response_from_original_authentication.is_some()
    }

    /// Return the (always non-null but possibly empty) NetworkReceive response that
    /// arrived during re-authentication that is unrelated to re-authentication, if
    /// any. This corresponds to a request sent prior to the beginning of
    /// re-authentication; the request was made when the channel was successfully
    /// authenticated, and the response arrived during the re-authentication
    /// process.
    #[doc(
        alias = "org.apache.kafka.common.security.authenticator.SaslClientAuthenticator$ReauthInfo#pollResponseReceivedDuringReauthentication"
    )]
    fn poll_response_received_during_reauthentication(&mut self) -> Option<NetworkReceive> {
        self.pending_authenticated_receives.pop_front()
    }

    /// Records the end of an authentication and, when the broker gave the
    /// session a lifetime, the time on or after which the client re-authenticates.
    #[doc(
        alias = "org.apache.kafka.common.security.authenticator.SaslClientAuthenticator$ReauthInfo#setAuthenticationEndAndSessionReauthenticationTimes"
    )]
    fn set_authentication_end_and_session_reauthentication_times(&mut self, now_nanos: i64, log_context: &LogContext) {
        self.authentication_end_nanos = now_nanos;
        if let Some(positive_session_lifetime_ms) = self.positive_session_lifetime_ms {
            // pick a random percentage between 85% and 95% for session re-authentication
            let pct_window_factor_to_take_network_latency_and_clock_drift_into_account = 0.85;
            let pct_window_jitter_to_avoid_reauthentication_storm_across_many_channels_simultaneously = 0.10;
            let pct_to_use = pct_window_factor_to_take_network_latency_and_clock_drift_into_account
                + rand::random::<f64>()
                    * pct_window_jitter_to_avoid_reauthentication_storm_across_many_channels_simultaneously;
            let session_lifetime_ms_to_use = (positive_session_lifetime_ms as f64 * pct_to_use) as i64;
            // Java's `Utils.msToNs` / `Math.addExact` throw here when the time
            // overflows (a lifetime of about 292 years or more, such as
            // `connections.max.reauth.ms=Long.MAX_VALUE`). The throw escapes
            // `authenticate()` as a non-authentication error, so Java's selector
            // closes the connection, and every reconnect fails the same way
            // (`SaslAuthenticatorTest.testReauthenticateWithLargeReauthValue`).
            // Such a session never needs re-authenticating, so the time saturates
            // instead and the connection is kept.
            self.client_session_reauthentication_time_nanos = Some(
                self.authentication_end_nanos
                    .saturating_add(session_lifetime_ms_to_use.saturating_mul(1_000_000)),
            );
            kafka_debug!(
                log_context,
                "Finished {} with session expiration in {} ms and session re-authentication on or after {} ms",
                self.authentication_or_reauthentication_text(),
                positive_session_lifetime_ms,
                session_lifetime_ms_to_use
            );
        } else {
            kafka_debug!(
                log_context,
                "Finished {} with no session expiration and no session re-authentication",
                self.authentication_or_reauthentication_text()
            );
        }
    }

    /// The milliseconds the re-authentication took, or `None` for the original
    /// authentication.
    #[doc(
        alias = "org.apache.kafka.common.security.authenticator.SaslClientAuthenticator$ReauthInfo#reauthenticationLatencyMs"
    )]
    fn reauthentication_latency_ms(&self) -> Option<u64> {
        self.reauthenticating().then(|| {
            ((self.authentication_end_nanos - self.reauthentication_begin_nanos).max(0) as f64 / 1000.0 / 1000.0)
                .round() as u64
        })
    }

    #[doc(
        alias = "org.apache.kafka.common.security.authenticator.SaslClientAuthenticator$ReauthInfo#authenticationOrReauthenticationText"
    )]
    fn authentication_or_reauthentication_text(&self) -> &'static str {
        if self.reauthenticating() {
            "re-authentication"
        } else {
            "authentication"
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ApiVersionsResponseData;
    use crate::SaslAuthenticateResponseData;
    use crate::SaslHandshakeResponseData;
    use crate::api_versions_response_data::ApiVersion;
    use crate::common::network::InterestOps;
    use crate::common::network::{authentication_error, is_authentication_error};
    use crate::common::protocol::Message;
    use crate::common::protocol::ObjectSerializationCache;
    use crate::common::protocol::Writable;
    use crate::common::requests::ResponseHeader;
    use crate::common::utils::{MockTime, SystemTime};

    use std::collections::VecDeque;
    use std::net::SocketAddr;

    // -----------------------------------------------------------------------
    // MockTransportLayer for testing
    // -----------------------------------------------------------------------

    /// A mock transport layer that captures writes and returns pre-programmed reads.
    struct MockTransportLayer {
        /// Pre-programmed read data, consumed in FIFO order.
        read_data: VecDeque<u8>,
        /// Captured write data.
        write_data: Vec<u8>,
        /// Interest operations currently set.
        interest_ops: InterestOps,
    }

    impl MockTransportLayer {
        fn new() -> Self {
            Self {
                read_data: VecDeque::new(),
                write_data: Vec::new(),
                interest_ops: InterestOps::NONE,
            }
        }

        /// Enqueue data that will be returned by read operations.
        fn enqueue_read_data(&mut self, data: &[u8]) {
            self.read_data.extend(data);
        }

        /// Returns all data written by the authenticator.
        fn written_data(&self) -> &[u8] {
            &self.write_data
        }
    }

    impl TransportLayer for MockTransportLayer {
        fn peer_addr(&self) -> io::Result<SocketAddr> {
            Ok("127.0.0.1:9092".parse().unwrap())
        }

        fn ready(&self) -> bool {
            true
        }

        fn finish_connect(&mut self) -> Pin<Box<dyn Future<Output = io::Result<bool>> + Send + '_>> {
            Box::pin(async { Ok(true) })
        }

        fn disconnect(&mut self) {}

        fn is_connected(&self) -> bool {
            true
        }

        fn handshake(&mut self) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send + '_>> {
            Box::pin(async { Ok(()) })
        }

        fn add_interest_ops(&mut self, ops: InterestOps) {
            self.interest_ops |= ops;
        }

        fn remove_interest_ops(&mut self, ops: InterestOps) {
            self.interest_ops = self.interest_ops.remove(ops);
        }

        fn is_mute(&self) -> bool {
            false
        }

        fn has_bytes_buffered(&self) -> bool {
            false
        }

        fn has_pending_writes(&self) -> bool {
            false
        }

        fn is_open(&self) -> bool {
            true
        }

        fn close(&mut self) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send + '_>> {
            Box::pin(async { Ok(()) })
        }

        fn readable(&self) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send + '_>> {
            Box::pin(async { Ok(()) })
        }

        fn writable(&self) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send + '_>> {
            Box::pin(async { Ok(()) })
        }

        fn poll_readable(&self, _cx: &mut std::task::Context<'_>) -> std::task::Poll<io::Result<()>> {
            std::task::Poll::Ready(Ok(()))
        }

        fn poll_writable(&self, _cx: &mut std::task::Context<'_>) -> std::task::Poll<io::Result<()>> {
            std::task::Poll::Ready(Ok(()))
        }

        fn read<'a>(&'a mut self, dst: &'a mut [u8]) -> Pin<Box<dyn Future<Output = io::Result<usize>> + Send + 'a>> {
            let available = self.read_data.len();
            let to_read = available.min(dst.len());
            if to_read == 0 {
                return Box::pin(async { Err(io::Error::from(io::ErrorKind::WouldBlock)) });
            }
            for byte in dst.iter_mut().take(to_read) {
                *byte = self.read_data.pop_front().unwrap();
            }
            Box::pin(async move { Ok(to_read) })
        }

        fn write<'a>(&'a mut self, src: &'a [u8]) -> Pin<Box<dyn Future<Output = io::Result<usize>> + Send + 'a>> {
            self.write_data.extend_from_slice(src);
            let len = src.len();
            Box::pin(async move { Ok(len) })
        }

        fn write_vectored<'a>(
            &'a mut self,
            srcs: &'a [io::IoSlice<'a>],
        ) -> Pin<Box<dyn Future<Output = io::Result<usize>> + Send + 'a>> {
            let mut total = 0;
            for slice in srcs {
                self.write_data.extend_from_slice(slice);
                total += slice.len();
            }
            Box::pin(async move { Ok(total) })
        }
    }

    // -----------------------------------------------------------------------
    // Helper functions for building serialized responses
    // -----------------------------------------------------------------------

    /// Builds a size-prefixed ApiVersionsResponse containing the given SASL versions.
    ///
    /// The response is formatted as: [4-byte size][ResponseHeader][ApiVersionsResponseData]
    fn build_api_versions_response_bytes(
        correlation_id: i32,
        sasl_handshake_max: i16,
        sasl_authenticate_max: i16,
    ) -> Vec<u8> {
        let mut data = ApiVersionsResponseData::new();
        data.set_error_code(Errors::None.code());

        let mut hs_version = ApiVersion::new();
        hs_version.set_api_key(ApiKeys::SASL_HANDSHAKE.id());
        hs_version.set_min_version(0);
        hs_version.set_max_version(sasl_handshake_max);

        let mut auth_version = ApiVersion::new();
        auth_version.set_api_key(ApiKeys::SASL_AUTHENTICATE.id());
        auth_version.set_min_version(0);
        auth_version.set_max_version(sasl_authenticate_max);

        data.set_api_keys(vec![hs_version, auth_version]);

        // Serialize header + body
        let mut response_header = ResponseHeader::with_correlation_id(correlation_id, 0); // v0 header for ApiVersions v0
        let mut cache = ObjectSerializationCache::new();
        let header_size = Message::size(response_header.data(), &mut cache, response_header.header_version()).unwrap();
        let body_size = Message::size(&data, &mut cache, 0).unwrap();
        let total_size = header_size + body_size;

        let mut buf = ByteBufferAccessor::new(Vec::with_capacity(4 + total_size as usize));
        buf.write_int(total_size).unwrap();
        let hv = response_header.header_version();
        Message::write(response_header.data_mut(), &mut buf, &cache, hv).unwrap();
        Message::write(&mut data, &mut buf, &cache, 0).unwrap();
        buf.buffer().to_vec()
    }

    /// Builds a size-prefixed SaslHandshakeResponse.
    fn build_sasl_handshake_response_bytes(
        correlation_id: i32,
        error: &Errors,
        mechanisms: Vec<String>,
        version: i16,
    ) -> Vec<u8> {
        let mut data = SaslHandshakeResponseData::new();
        data.set_error_code(error.code());
        data.set_mechanisms(mechanisms);

        let api_key = &ApiKeys::SASL_HANDSHAKE;
        let header_version = api_key.response_header_version(version);
        let mut response_header = ResponseHeader::with_correlation_id(correlation_id, header_version);

        let mut cache = ObjectSerializationCache::new();
        let header_size = Message::size(response_header.data(), &mut cache, response_header.header_version()).unwrap();
        let body_size = Message::size(&data, &mut cache, version).unwrap();
        let total_size = header_size + body_size;

        let mut buf = ByteBufferAccessor::new(Vec::with_capacity(4 + total_size as usize));
        buf.write_int(total_size).unwrap();
        let hv = response_header.header_version();
        Message::write(response_header.data_mut(), &mut buf, &cache, hv).unwrap();
        Message::write(&mut data, &mut buf, &cache, version).unwrap();
        buf.buffer().to_vec()
    }

    /// Builds a size-prefixed SaslAuthenticateResponse.
    fn build_sasl_authenticate_response_bytes(
        correlation_id: i32,
        error: &Errors,
        error_message: Option<&str>,
        auth_bytes: &[u8],
        version: i16,
    ) -> Vec<u8> {
        build_sasl_authenticate_response_bytes_with_session_lifetime(
            correlation_id,
            error,
            error_message,
            auth_bytes,
            version,
            0,
        )
    }

    /// Builds a size-prefixed SaslAuthenticateResponse granting the session
    /// `session_lifetime_ms` (0: no lifetime).
    fn build_sasl_authenticate_response_bytes_with_session_lifetime(
        correlation_id: i32,
        error: &Errors,
        error_message: Option<&str>,
        auth_bytes: &[u8],
        version: i16,
        session_lifetime_ms: i64,
    ) -> Vec<u8> {
        let mut data = SaslAuthenticateResponseData::new();
        data.set_error_code(error.code());
        data.set_error_message(error_message.map(|s| s.to_string()));
        data.set_auth_bytes(auth_bytes.to_vec());
        data.set_session_lifetime_ms(session_lifetime_ms);

        let api_key = &ApiKeys::SASL_AUTHENTICATE;
        let header_version = api_key.response_header_version(version);
        let mut response_header = ResponseHeader::with_correlation_id(correlation_id, header_version);

        let mut cache = ObjectSerializationCache::new();
        let header_size = Message::size(response_header.data(), &mut cache, response_header.header_version()).unwrap();
        let body_size = Message::size(&data, &mut cache, version).unwrap();
        let total_size = header_size + body_size;

        let mut buf = ByteBufferAccessor::new(Vec::with_capacity(4 + total_size as usize));
        buf.write_int(total_size).unwrap();
        let hv = response_header.header_version();
        Message::write(response_header.data_mut(), &mut buf, &cache, hv).unwrap();
        Message::write(&mut data, &mut buf, &cache, version).unwrap();
        buf.buffer().to_vec()
    }

    // -----------------------------------------------------------------------
    // Tests
    // -----------------------------------------------------------------------

    /// Test 1: PLAIN token generation matches RFC 4616 format.
    #[test]
    fn test_plain_token_generation() {
        let auth = SaslClientAuthenticator::new(
            "PLAIN",
            "alice",
            "secret",
            "node-0",
            "broker1",
            "client-1",
            Arc::new(SystemTime),
            LogContext::empty(),
        );
        let token = auth.create_sasl_token();
        assert_eq!(token, b"\0alice\0secret");
    }

    /// Test 2: Initial state is SendApiVersionsRequest and complete() is false.
    #[test]
    fn test_initial_state() {
        let auth = SaslClientAuthenticator::new(
            "PLAIN",
            "alice",
            "secret",
            "node-0",
            "broker1",
            "client-1",
            Arc::new(SystemTime),
            LogContext::empty(),
        );
        assert_eq!(auth.sasl_state(), SaslState::SendApiVersionsRequest);
        assert!(!auth.complete());
    }

    /// Test 3: Correlation ID management uses reserved range.
    #[test]
    fn test_correlation_id_management() {
        let mut auth = SaslClientAuthenticator::new(
            "PLAIN",
            "alice",
            "secret",
            "node-0",
            "broker1",
            "client-1",
            Arc::new(SystemTime),
            LogContext::empty(),
        );
        let id1 = auth.next_correlation_id();
        assert!(SaslClientAuthenticator::is_reserved(id1));
        assert_eq!(
            id1,
            SaslClientAuthenticator::SASL_CLIENT_AUTHENTICATOR_MIN_RESERVED_CORRELATION_ID
        );

        let id2 = auth.next_correlation_id();
        assert!(SaslClientAuthenticator::is_reserved(id2));
        assert_eq!(
            id2,
            SaslClientAuthenticator::SASL_CLIENT_AUTHENTICATOR_MIN_RESERVED_CORRELATION_ID + 1
        );
    }

    /// Test 3b: Correlation ID wraps correctly when reserved range is exhausted.
    ///
    /// Java's `int` wraps silently on overflow (`Integer.MAX_VALUE + 1` becomes
    /// `Integer.MIN_VALUE`). After wrapping, `isReserved()` returns false, so
    /// `nextCorrelationId()` resets to `SASL_CLIENT_AUTHENTICATOR_MIN_RESERVED_CORRELATION_ID`. This test
    /// ensures the Rust implementation matches this wrapping behavior in both
    /// debug and release builds.
    #[test]
    fn test_correlation_id_wraps_on_overflow() {
        let mut auth = SaslClientAuthenticator::new(
            "PLAIN",
            "alice",
            "secret",
            "node-0",
            "broker1",
            "client-1",
            Arc::new(SystemTime),
            LogContext::empty(),
        );
        // Exhaust all 8 reserved IDs
        for i in 0..8 {
            let id = auth.next_correlation_id();
            assert_eq!(
                id,
                SaslClientAuthenticator::SASL_CLIENT_AUTHENTICATOR_MIN_RESERVED_CORRELATION_ID + i
            );
            assert!(SaslClientAuthenticator::is_reserved(id));
        }
        // At this point correlation_id has wrapped past i32::MAX.
        // The 9th call should detect that the ID is no longer reserved and
        // reset to SASL_CLIENT_AUTHENTICATOR_MIN_RESERVED_CORRELATION_ID.
        let id = auth.next_correlation_id();
        assert_eq!(
            id,
            SaslClientAuthenticator::SASL_CLIENT_AUTHENTICATOR_MIN_RESERVED_CORRELATION_ID
        );
        assert!(SaslClientAuthenticator::is_reserved(id));
    }

    /// Test 4: is_reserved boundary conditions.
    #[test]
    fn test_is_reserved() {
        assert!(SaslClientAuthenticator::is_reserved(
            SaslClientAuthenticator::SASL_CLIENT_AUTHENTICATOR_MAX_RESERVED_CORRELATION_ID
        ));
        assert!(SaslClientAuthenticator::is_reserved(
            SaslClientAuthenticator::SASL_CLIENT_AUTHENTICATOR_MIN_RESERVED_CORRELATION_ID
        ));
        assert!(!SaslClientAuthenticator::is_reserved(
            SaslClientAuthenticator::SASL_CLIENT_AUTHENTICATOR_MIN_RESERVED_CORRELATION_ID - 1
        ));
        assert!(!SaslClientAuthenticator::is_reserved(0));
        assert!(!SaslClientAuthenticator::is_reserved(-1));
    }

    /// Test 5: Version negotiation extracts SASL versions from ApiVersionsResponse.
    #[test]
    fn test_version_negotiation() {
        let mut auth = SaslClientAuthenticator::new(
            "PLAIN",
            "alice",
            "secret",
            "node-0",
            "broker1",
            "client-1",
            Arc::new(SystemTime),
            LogContext::empty(),
        );

        let mut data = ApiVersionsResponseData::new();
        data.set_error_code(Errors::None.code());

        let mut hs_version = ApiVersion::new();
        hs_version.set_api_key(ApiKeys::SASL_HANDSHAKE.id());
        hs_version.set_min_version(0);
        hs_version.set_max_version(10); // Higher than latest

        let mut auth_version = ApiVersion::new();
        auth_version.set_api_key(ApiKeys::SASL_AUTHENTICATE.id());
        auth_version.set_min_version(0);
        auth_version.set_max_version(10); // Higher than latest

        data.set_api_keys(vec![hs_version, auth_version]);
        let response = ApiVersionsResponse::new(data);

        auth.set_sasl_authenticate_and_handshake_versions(&response);

        // Should be capped to latest supported version
        assert_eq!(auth.sasl_handshake_version(), ApiKeys::SASL_HANDSHAKE.latest_version());
        assert_eq!(auth.sasl_authenticate_version(), ApiKeys::SASL_AUTHENTICATE.latest_version());
    }

    /// Test 6: Successful full authentication flow with mock transport.
    #[tokio::test]
    async fn test_successful_authentication() {
        let mut auth = SaslClientAuthenticator::new(
            "PLAIN",
            "alice",
            "secret",
            "node-0",
            "broker1",
            "test-client",
            Arc::new(SystemTime),
            LogContext::empty(),
        );
        let mut transport = MockTransportLayer::new();

        // Step 1: Send ApiVersionsRequest
        auth.authenticate_impl(&mut transport).await.unwrap();
        assert_eq!(auth.sasl_state(), SaslState::ReceiveApiVersionsResponse);
        assert!(!transport.written_data().is_empty());

        // Step 2: Receive ApiVersionsResponse
        let api_versions_bytes = build_api_versions_response_bytes(
            SaslClientAuthenticator::SASL_CLIENT_AUTHENTICATOR_MIN_RESERVED_CORRELATION_ID,
            ApiKeys::SASL_HANDSHAKE.latest_version(),
            ApiKeys::SASL_AUTHENTICATE.latest_version(),
        );
        transport.enqueue_read_data(&api_versions_bytes);
        transport.write_data.clear();

        // This call receives ApiVersionsResponse and falls through to send handshake
        auth.authenticate_impl(&mut transport).await.unwrap();
        assert_eq!(auth.sasl_state(), SaslState::ReceiveHandshakeResponse);

        // Step 3: Receive SaslHandshakeResponse
        let handshake_bytes = build_sasl_handshake_response_bytes(
            SaslClientAuthenticator::SASL_CLIENT_AUTHENTICATOR_MIN_RESERVED_CORRELATION_ID + 1,
            &Errors::None,
            vec!["PLAIN".to_string()],
            auth.sasl_handshake_version(),
        );
        transport.enqueue_read_data(&handshake_bytes);
        transport.write_data.clear();

        // This call receives handshake response and falls through to send initial token
        auth.authenticate_impl(&mut transport).await.unwrap();
        assert_eq!(auth.sasl_state(), SaslState::Intermediate);

        // Step 4: Receive SaslAuthenticateResponse
        // For PLAIN, the server sends a single success response after the token.
        // The client goes directly to Complete (no CLIENT_COMPLETE intermediate
        // state, since PLAIN has no challenge-response cycle).
        let sasl_auth_bytes = build_sasl_authenticate_response_bytes(
            SaslClientAuthenticator::SASL_CLIENT_AUTHENTICATOR_MIN_RESERVED_CORRELATION_ID + 2,
            &Errors::None,
            None,
            &[],
            auth.sasl_authenticate_version(),
        );
        transport.enqueue_read_data(&sasl_auth_bytes);

        auth.authenticate_impl(&mut transport).await.unwrap();
        assert_eq!(auth.sasl_state(), SaslState::Complete);
        assert!(auth.complete());
    }

    /// Test 7: Unsupported mechanism error in handshake response.
    #[tokio::test]
    async fn test_unsupported_mechanism() {
        let mut auth = SaslClientAuthenticator::new(
            "PLAIN",
            "alice",
            "secret",
            "node-0",
            "broker1",
            "test-client",
            Arc::new(SystemTime),
            LogContext::empty(),
        );
        let mut transport = MockTransportLayer::new();

        // Send ApiVersionsRequest
        auth.authenticate_impl(&mut transport).await.unwrap();

        // Receive ApiVersionsResponse
        let api_versions_bytes = build_api_versions_response_bytes(
            SaslClientAuthenticator::SASL_CLIENT_AUTHENTICATOR_MIN_RESERVED_CORRELATION_ID,
            ApiKeys::SASL_HANDSHAKE.latest_version(),
            ApiKeys::SASL_AUTHENTICATE.latest_version(),
        );
        transport.enqueue_read_data(&api_versions_bytes);
        transport.write_data.clear();
        auth.authenticate_impl(&mut transport).await.unwrap();

        // Receive SaslHandshakeResponse with UNSUPPORTED_SASL_MECHANISM
        let handshake_bytes = build_sasl_handshake_response_bytes(
            SaslClientAuthenticator::SASL_CLIENT_AUTHENTICATOR_MIN_RESERVED_CORRELATION_ID + 1,
            &Errors::UnsupportedSaslMechanism,
            vec!["SCRAM-SHA-256".to_string(), "SCRAM-SHA-512".to_string()],
            auth.sasl_handshake_version(),
        );
        transport.enqueue_read_data(&handshake_bytes);

        let err = auth.authenticate_impl(&mut transport).await.unwrap_err();
        // An unsupported-mechanism failure is a genuine authentication failure
        // (Java: UnsupportedSaslMechanismException extends AuthenticationException)
        // and must be classified as fatal, not a retriable disconnect.
        assert!(is_authentication_error(&err));
        let error = authentication_error(&err).unwrap();
        assert!(matches!(error, Error::UnsupportedSaslMechanism(_)), "{error:?}");
        assert_eq!(error.error(), Errors::UnsupportedSaslMechanism);
        // Java's text, the mechanisms printed as a `List`.
        assert_eq!(
            error.message(),
            "Client SASL mechanism 'PLAIN' not enabled in the server, enabled mechanisms are [SCRAM-SHA-256, SCRAM-SHA-512]"
        );
        assert_eq!(auth.sasl_state(), SaslState::Failed);
    }

    /// Test 8: Authentication failure in SaslAuthenticateResponse.
    #[tokio::test]
    async fn test_auth_failure() {
        let mut auth = SaslClientAuthenticator::new(
            "PLAIN",
            "alice",
            "wrong",
            "node-0",
            "broker1",
            "test-client",
            Arc::new(SystemTime),
            LogContext::empty(),
        );
        let mut transport = MockTransportLayer::new();

        // Send ApiVersionsRequest
        auth.authenticate_impl(&mut transport).await.unwrap();

        // Receive ApiVersionsResponse
        let api_versions_bytes = build_api_versions_response_bytes(
            SaslClientAuthenticator::SASL_CLIENT_AUTHENTICATOR_MIN_RESERVED_CORRELATION_ID,
            ApiKeys::SASL_HANDSHAKE.latest_version(),
            ApiKeys::SASL_AUTHENTICATE.latest_version(),
        );
        transport.enqueue_read_data(&api_versions_bytes);
        transport.write_data.clear();
        auth.authenticate_impl(&mut transport).await.unwrap();

        // Receive SaslHandshakeResponse (success)
        let handshake_bytes = build_sasl_handshake_response_bytes(
            SaslClientAuthenticator::SASL_CLIENT_AUTHENTICATOR_MIN_RESERVED_CORRELATION_ID + 1,
            &Errors::None,
            vec!["PLAIN".to_string()],
            auth.sasl_handshake_version(),
        );
        transport.enqueue_read_data(&handshake_bytes);
        transport.write_data.clear();
        auth.authenticate_impl(&mut transport).await.unwrap();

        // Receive SaslAuthenticateResponse with error
        let sasl_auth_bytes = build_sasl_authenticate_response_bytes(
            SaslClientAuthenticator::SASL_CLIENT_AUTHENTICATOR_MIN_RESERVED_CORRELATION_ID + 2,
            &Errors::SaslAuthenticationFailed,
            Some("Authentication failed: Invalid credentials"),
            &[],
            auth.sasl_authenticate_version(),
        );
        transport.enqueue_read_data(&sasl_auth_bytes);

        let err = auth.authenticate_impl(&mut transport).await.unwrap_err();
        // A broker-side credential rejection is a genuine authentication failure
        // (Java: SaslAuthenticationException extends AuthenticationException);
        // it must be classified as fatal so it is not silently retried.
        assert!(is_authentication_error(&err));
        // Java throws `error.exception(errMsg)`: the class the code names, with
        // the broker's message.
        let error = authentication_error(&err).unwrap();
        assert!(matches!(error, Error::SaslAuthentication(_)), "{error:?}");
        assert_eq!(error.error(), Errors::SaslAuthenticationFailed);
        assert_eq!(error.message(), "Authentication failed: Invalid credentials");
        assert_eq!(auth.sasl_state(), SaslState::Failed);
    }

    /// Drives a fresh authenticator to `Intermediate` and answers its token with
    /// a SaslAuthenticate response carrying `error` and `error_message`.
    async fn authenticate_response_error(error: &Errors, error_message: Option<&str>) -> io::Error {
        let mut auth = SaslClientAuthenticator::new(
            "PLAIN",
            "alice",
            "wrong",
            "node-0",
            "broker1",
            "test-client",
            Arc::new(SystemTime),
            LogContext::empty(),
        );
        let mut transport = MockTransportLayer::new();
        auth.authenticate_impl(&mut transport).await.unwrap();
        transport.enqueue_read_data(&build_api_versions_response_bytes(
            SaslClientAuthenticator::SASL_CLIENT_AUTHENTICATOR_MIN_RESERVED_CORRELATION_ID,
            ApiKeys::SASL_HANDSHAKE.latest_version(),
            ApiKeys::SASL_AUTHENTICATE.latest_version(),
        ));
        auth.authenticate_impl(&mut transport).await.unwrap();
        transport.enqueue_read_data(&build_sasl_handshake_response_bytes(
            SaslClientAuthenticator::SASL_CLIENT_AUTHENTICATOR_MIN_RESERVED_CORRELATION_ID + 1,
            &Errors::None,
            vec!["PLAIN".to_string()],
            auth.sasl_handshake_version(),
        ));
        auth.authenticate_impl(&mut transport).await.unwrap();
        transport.enqueue_read_data(&build_sasl_authenticate_response_bytes(
            SaslClientAuthenticator::SASL_CLIENT_AUTHENTICATOR_MIN_RESERVED_CORRELATION_ID + 2,
            error,
            error_message,
            &[],
            auth.sasl_authenticate_version(),
        ));
        let err = auth.authenticate_impl(&mut transport).await.unwrap_err();
        assert_eq!(auth.sasl_state(), SaslState::Failed);
        err
    }

    /// Without an error message, Java's `error.exception()` carries the code's
    /// default text.
    #[tokio::test]
    async fn test_auth_failure_without_message_uses_the_default_text() {
        let err = authenticate_response_error(&Errors::SaslAuthenticationFailed, None).await;
        let error = authentication_error(&err).unwrap();
        assert!(matches!(error, Error::SaslAuthentication(_)), "{error:?}");
        assert_eq!(error.message(), Errors::SaslAuthenticationFailed.message());
    }

    /// A code whose class is not an `AuthenticationException` is thrown as that
    /// class in Java, which `KafkaChannel.prepare` does not catch: the selector
    /// closes the connection as a disconnect, to be retried.
    #[tokio::test]
    async fn test_non_authentication_error_code_is_not_an_authentication_failure() {
        let err = authenticate_response_error(&Errors::UnknownServerError, Some("broker trouble")).await;
        assert!(!is_authentication_error(&err));
        let error = err.get_ref().and_then(|e| e.downcast_ref::<Error>()).expect("typed payload");
        assert!(matches!(error, Error::UnknownServer(_)), "{error:?}");
        assert_eq!(error.message(), "broker trouble");
    }

    /// Test 9: Legacy raw token mode (sasl_authenticate_version == -1).
    #[tokio::test]
    async fn test_raw_token_mode() {
        let mut auth = SaslClientAuthenticator::new(
            "PLAIN",
            "alice",
            "secret",
            "node-0",
            "broker1",
            "test-client",
            Arc::new(SystemTime),
            LogContext::empty(),
        );
        let mut transport = MockTransportLayer::new();

        // Send ApiVersionsRequest
        auth.authenticate_impl(&mut transport).await.unwrap();

        // Receive ApiVersionsResponse WITHOUT SASL_AUTHENTICATE support
        // (only SASL_HANDSHAKE)
        let mut data = ApiVersionsResponseData::new();
        data.set_error_code(Errors::None.code());

        let mut hs_version = ApiVersion::new();
        hs_version.set_api_key(ApiKeys::SASL_HANDSHAKE.id());
        hs_version.set_min_version(0);
        hs_version.set_max_version(0); // Only v0

        data.set_api_keys(vec![hs_version]);

        let mut response_header = ResponseHeader::with_correlation_id(
            SaslClientAuthenticator::SASL_CLIENT_AUTHENTICATOR_MIN_RESERVED_CORRELATION_ID,
            0,
        );
        let mut cache = ObjectSerializationCache::new();
        let header_size = Message::size(response_header.data(), &mut cache, response_header.header_version()).unwrap();
        let body_size = Message::size(&data, &mut cache, 0).unwrap();
        let total_size = header_size + body_size;
        let mut buf = ByteBufferAccessor::new(Vec::with_capacity(4 + total_size as usize));
        buf.write_int(total_size).unwrap();
        let hv = response_header.header_version();
        Message::write(response_header.data_mut(), &mut buf, &cache, hv).unwrap();
        Message::write(&mut data, &mut buf, &cache, 0).unwrap();
        transport.enqueue_read_data(buf.buffer());
        transport.write_data.clear();

        auth.authenticate_impl(&mut transport).await.unwrap();
        // Should be in ReceiveHandshakeResponse (fell through from ReceiveApiVersionsResponse)
        assert_eq!(
            auth.sasl_authenticate_version(),
            SaslClientAuthenticator::DISABLE_KAFKA_SASL_AUTHENTICATE_HEADER
        );

        // Receive SaslHandshakeResponse
        let handshake_bytes = build_sasl_handshake_response_bytes(
            SaslClientAuthenticator::SASL_CLIENT_AUTHENTICATOR_MIN_RESERVED_CORRELATION_ID + 1,
            &Errors::None,
            vec!["PLAIN".to_string()],
            0,
        );
        transport.enqueue_read_data(&handshake_bytes);
        transport.write_data.clear();

        auth.authenticate_impl(&mut transport).await.unwrap();
        assert_eq!(auth.sasl_state(), SaslState::Intermediate);

        // In legacy mode, the token was sent as raw size-prefixed bytes.
        // Now provide a raw size-prefixed empty response (server "OK").
        let empty_response: Vec<u8> = 0_i32.to_be_bytes().to_vec();
        transport.enqueue_read_data(&empty_response);

        auth.authenticate_impl(&mut transport).await.unwrap();
        // In legacy mode, PLAIN goes directly to Complete
        assert_eq!(auth.sasl_state(), SaslState::Complete);
        assert!(auth.complete());
    }

    /// Test 10: Illegal SASL state error in handshake response.
    #[tokio::test]
    async fn test_handle_sasl_handshake_illegal_state() {
        let mut auth = SaslClientAuthenticator::new(
            "PLAIN",
            "alice",
            "secret",
            "node-0",
            "broker1",
            "test-client",
            Arc::new(SystemTime),
            LogContext::empty(),
        );
        let mut transport = MockTransportLayer::new();

        // Send ApiVersionsRequest
        auth.authenticate_impl(&mut transport).await.unwrap();

        // Receive ApiVersionsResponse
        let api_versions_bytes = build_api_versions_response_bytes(
            SaslClientAuthenticator::SASL_CLIENT_AUTHENTICATOR_MIN_RESERVED_CORRELATION_ID,
            ApiKeys::SASL_HANDSHAKE.latest_version(),
            ApiKeys::SASL_AUTHENTICATE.latest_version(),
        );
        transport.enqueue_read_data(&api_versions_bytes);
        transport.write_data.clear();
        auth.authenticate_impl(&mut transport).await.unwrap();

        // Receive SaslHandshakeResponse with ILLEGAL_SASL_STATE
        let handshake_bytes = build_sasl_handshake_response_bytes(
            SaslClientAuthenticator::SASL_CLIENT_AUTHENTICATOR_MIN_RESERVED_CORRELATION_ID + 1,
            &Errors::IllegalSaslState,
            vec!["PLAIN".to_string()],
            auth.sasl_handshake_version(),
        );
        transport.enqueue_read_data(&handshake_bytes);

        let err = auth.authenticate_impl(&mut transport).await.unwrap_err();
        // Java: IllegalSaslStateException extends AuthenticationException — fatal.
        assert!(is_authentication_error(&err));
        let error = authentication_error(&err).unwrap();
        assert!(matches!(error, Error::IllegalSaslState(_)), "{error:?}");
        assert_eq!(error.error(), Errors::IllegalSaslState);
        assert_eq!(
            error.message(),
            "Unexpected handshake request with client mechanism PLAIN, enabled mechanisms are [PLAIN]"
        );
        assert_eq!(auth.sasl_state(), SaslState::Failed);
    }

    /// A handshake error code other than the two Java names is reported as an
    /// `IllegalSaslStateException` naming the code by its constant.
    #[tokio::test]
    async fn test_handle_sasl_handshake_unknown_error_code() {
        let mut auth = SaslClientAuthenticator::new(
            "PLAIN",
            "alice",
            "secret",
            "node-0",
            "broker1",
            "test-client",
            Arc::new(SystemTime),
            LogContext::empty(),
        );
        let mut transport = MockTransportLayer::new();
        auth.authenticate_impl(&mut transport).await.unwrap();
        transport.enqueue_read_data(&build_api_versions_response_bytes(
            SaslClientAuthenticator::SASL_CLIENT_AUTHENTICATOR_MIN_RESERVED_CORRELATION_ID,
            ApiKeys::SASL_HANDSHAKE.latest_version(),
            ApiKeys::SASL_AUTHENTICATE.latest_version(),
        ));
        auth.authenticate_impl(&mut transport).await.unwrap();
        transport.enqueue_read_data(&build_sasl_handshake_response_bytes(
            SaslClientAuthenticator::SASL_CLIENT_AUTHENTICATOR_MIN_RESERVED_CORRELATION_ID + 1,
            &Errors::UnknownServerError,
            vec!["PLAIN".to_string(), "SCRAM-SHA-256".to_string()],
            auth.sasl_handshake_version(),
        ));

        let err = auth.authenticate_impl(&mut transport).await.unwrap_err();
        let error = authentication_error(&err).unwrap();
        assert!(matches!(error, Error::IllegalSaslState(_)), "{error:?}");
        assert_eq!(
            error.message(),
            "Unknown error code UNKNOWN_SERVER_ERROR, client mechanism is PLAIN, enabled mechanisms are [PLAIN, SCRAM-SHA-256]"
        );
        assert_eq!(auth.sasl_state(), SaslState::Failed);
    }

    /// Test 11: Parse error in receive_kafka_response sets state to Failed.
    ///
    /// When the response body is malformed (e.g., truncated), the parse error
    /// must transition the state to Failed before returning the error, matching
    /// the Java try-catch in `receiveKafkaResponse()`.
    #[tokio::test]
    async fn test_parse_error_sets_state_to_failed() {
        let mut auth = SaslClientAuthenticator::new(
            "PLAIN",
            "alice",
            "secret",
            "node-0",
            "broker1",
            "test-client",
            Arc::new(SystemTime),
            LogContext::empty(),
        );
        let mut transport = MockTransportLayer::new();

        // Send ApiVersionsRequest to advance state and set current_request_header
        auth.authenticate_impl(&mut transport).await.unwrap();
        assert_eq!(auth.sasl_state(), SaslState::ReceiveApiVersionsResponse);

        // Enqueue a size-prefixed but truncated/garbage response body.
        // The 4-byte size prefix says 4 bytes follow, but the body is garbage
        // that won't parse as a valid ApiVersionsResponse.
        let garbage: Vec<u8> = vec![
            0x00, 0x00, 0x00, 0x04, // size = 4
            0xFF, 0xFF, 0xFF, 0xFF, // garbage body
        ];
        transport.enqueue_read_data(&garbage);

        let err = auth.authenticate_impl(&mut transport).await.unwrap_err();
        assert_eq!(auth.sasl_state(), SaslState::Failed);
        assert!(
            err.to_string().contains("Invalid SASL mechanism response"),
            "Expected parse error message, got: {}",
            err
        );
        // Java throws IllegalSaslStateException (an AuthenticationException) for
        // an unparseable SASL response — a fatal authentication failure.
        assert!(is_authentication_error(&err));
        let error = authentication_error(&err).unwrap();
        assert!(matches!(error, Error::IllegalSaslState(_)), "{error:?}");
        assert_eq!(
            error.message(),
            "Invalid SASL mechanism response, server may be expecting a different protocol"
        );
        assert!(error.source().is_some(), "the parse error is the cause");
    }

    // -----------------------------------------------------------------------
    // Re-authentication (KIP-368)
    // -----------------------------------------------------------------------

    fn authenticator_with_time(time: Arc<dyn Time>) -> SaslClientAuthenticator {
        SaslClientAuthenticator::new(
            "PLAIN",
            "alice",
            "secret",
            "node-0",
            "broker1",
            "test-client",
            time,
            LogContext::empty(),
        )
    }

    /// The request headers of the size-prefixed requests `written` holds.
    fn written_request_headers(written: &[u8]) -> Vec<RequestHeader> {
        let mut headers = Vec::new();
        let mut offset = 0;
        while offset + 4 <= written.len() {
            let size = i32::from_be_bytes(written[offset..offset + 4].try_into().unwrap()) as usize;
            let mut accessor = ByteBufferAccessor::new(written[offset + 4..offset + 4 + size].to_vec());
            headers.push(RequestHeader::parse(&mut accessor).expect("request header"));
            offset += 4 + size;
        }
        headers
    }

    /// Drives `auth` through the original authentication, the broker granting
    /// the session `session_lifetime_ms`.
    async fn complete_authentication(
        auth: &mut SaslClientAuthenticator,
        transport: &mut MockTransportLayer,
        session_lifetime_ms: i64,
    ) {
        auth.authenticate_impl(transport).await.unwrap();
        transport.enqueue_read_data(&build_api_versions_response_bytes(
            SaslClientAuthenticator::SASL_CLIENT_AUTHENTICATOR_MIN_RESERVED_CORRELATION_ID,
            ApiKeys::SASL_HANDSHAKE.latest_version(),
            ApiKeys::SASL_AUTHENTICATE.latest_version(),
        ));
        auth.authenticate_impl(transport).await.unwrap();
        transport.enqueue_read_data(&build_sasl_handshake_response_bytes(
            SaslClientAuthenticator::SASL_CLIENT_AUTHENTICATOR_MIN_RESERVED_CORRELATION_ID + 1,
            &Errors::None,
            vec!["PLAIN".to_string()],
            auth.sasl_handshake_version(),
        ));
        auth.authenticate_impl(transport).await.unwrap();
        transport.enqueue_read_data(&build_sasl_authenticate_response_bytes_with_session_lifetime(
            SaslClientAuthenticator::SASL_CLIENT_AUTHENTICATOR_MIN_RESERVED_CORRELATION_ID + 2,
            &Errors::None,
            None,
            &[],
            auth.sasl_authenticate_version(),
            session_lifetime_ms,
        ));
        auth.authenticate_impl(transport).await.unwrap();
        assert!(auth.complete());
        transport.write_data.clear();
    }

    /// Begins re-authentication and answers its SaslHandshake, leaving `auth`
    /// waiting for the answer to its token. Returns that request's header.
    async fn reauthenticate_through_handshake(
        auth: &mut SaslClientAuthenticator,
        transport: &mut MockTransportLayer,
        reauthentication_context: ReauthenticationContext,
    ) -> RequestHeader {
        auth.reauthenticate(reauthentication_context, transport).await.unwrap();
        let handshake = written_request_headers(transport.written_data())
            .pop()
            .expect("a SaslHandshake");
        transport.write_data.clear();
        transport.enqueue_read_data(&build_sasl_handshake_response_bytes(
            handshake.correlation_id(),
            &Errors::None,
            vec!["PLAIN".to_string()],
            auth.sasl_handshake_version(),
        ));
        auth.authenticate_impl(transport).await.unwrap();
        assert_eq!(auth.sasl_state(), SaslState::Intermediate);
        let mut headers = written_request_headers(transport.written_data());
        assert_eq!(headers.len(), 1, "one SaslAuthenticate");
        transport.write_data.clear();
        headers.pop().unwrap()
    }

    /// Java's `ReauthInfo.setAuthenticationEndAndSessionReauthenticationTimes`:
    /// a session the broker gives a lifetime is re-authenticated on or after a
    /// random point between 85% and 95% of it, from the end of the
    /// authentication. Before this, the lifetime was parsed and dropped.
    #[tokio::test]
    async fn test_session_lifetime_schedules_reauthentication_between_85_and_95_percent() {
        let mut earliest = i64::MAX;
        let mut latest = i64::MIN;
        for _ in 0..200 {
            let time = Arc::new(MockTime::new());
            let mut auth = authenticator_with_time(time.clone());
            let mut transport = MockTransportLayer::new();
            complete_authentication(&mut auth, &mut transport, 10_000).await;

            let after = auth
                .client_session_reauthentication_time_nanos()
                .expect("a re-authentication time")
                - time.nanoseconds();
            assert!(
                (8_500_000_000..=9_500_000_000).contains(&after),
                "re-authentication {after} ns after the authentication, outside 85%..95% of 10 s"
            );
            earliest = earliest.min(after);
            latest = latest.max(after);
            // The original authentication has no re-authentication latency.
            assert_eq!(auth.reauthentication_latency_ms(), None);
        }
        // The point is jittered, so a fleet of clients does not re-authenticate
        // at once.
        assert!(latest - earliest > 100_000_000, "{earliest}..{latest}");
    }

    /// A session without a lifetime is never re-authenticated.
    #[tokio::test]
    async fn test_session_without_lifetime_is_not_reauthenticated() {
        let mut auth = authenticator_with_time(Arc::new(MockTime::new()));
        let mut transport = MockTransportLayer::new();
        complete_authentication(&mut auth, &mut transport, 0).await;
        assert_eq!(auth.client_session_reauthentication_time_nanos(), None);
    }

    /// The re-authentication flow of Java's `SaslClientAuthenticator`: a
    /// SaslHandshake at the version the original authentication negotiated, on
    /// a reserved correlation id, then the PLAIN token, then a new session
    /// lifetime. A response to a request sent before re-authentication began
    /// arrives in between and is set aside whole, for the channel to hand to
    /// the network client.
    #[tokio::test]
    async fn test_reauthentication_flow() {
        let time = Arc::new(MockTime::new());
        let mut auth = authenticator_with_time(time.clone());
        let mut transport = MockTransportLayer::new();
        complete_authentication(&mut auth, &mut transport, 10_000).await;
        time.sleep(9_600);

        let begin = time.nanoseconds();
        auth.reauthenticate(ReauthenticationContext::new(None, begin), &mut transport)
            .await
            .unwrap();
        assert!(!auth.complete());
        assert_eq!(auth.sasl_state(), SaslState::ReauthReceiveHandshakeOrOtherResponse);
        // The previous session's re-authentication time is gone with it.
        assert_eq!(auth.client_session_reauthentication_time_nanos(), None);
        let headers = written_request_headers(transport.written_data());
        assert_eq!(headers.len(), 1, "only the SaslHandshake is sent");
        assert_eq!(*headers[0].api_key(), ApiKeys::SASL_HANDSHAKE);
        assert_eq!(headers[0].api_version(), auth.sasl_handshake_version());
        assert!(SaslClientAuthenticator::is_reserved(headers[0].correlation_id()));
        let handshake_correlation_id = headers[0].correlation_id();
        transport.write_data.clear();

        // A response to an earlier request (correlation id 42) arrives first.
        let earlier_response = build_api_versions_response_bytes(42, 1, 2);
        transport.enqueue_read_data(&earlier_response);
        auth.authenticate_impl(&mut transport).await.unwrap();
        assert_eq!(auth.sasl_state(), SaslState::ReauthReceiveHandshakeOrOtherResponse);
        let set_aside = auth
            .poll_response_received_during_reauthentication()
            .expect("the earlier response is set aside");
        assert_eq!(set_aside.source(), "node-0");
        assert!(set_aside.complete());
        assert_eq!(set_aside.payload(), Some(&earlier_response[4..]));
        assert!(auth.poll_response_received_during_reauthentication().is_none());

        // The SaslHandshake response: the PLAIN token is sent.
        transport.enqueue_read_data(&build_sasl_handshake_response_bytes(
            handshake_correlation_id,
            &Errors::None,
            vec!["PLAIN".to_string()],
            auth.sasl_handshake_version(),
        ));
        auth.authenticate_impl(&mut transport).await.unwrap();
        assert_eq!(auth.sasl_state(), SaslState::Intermediate);
        let headers = written_request_headers(transport.written_data());
        assert_eq!(headers.len(), 1);
        assert_eq!(*headers[0].api_key(), ApiKeys::SASL_AUTHENTICATE);
        assert!(SaslClientAuthenticator::is_reserved(headers[0].correlation_id()));
        let written = transport.written_data().to_vec();
        assert!(
            written.windows(b"\0alice\0secret".len()).any(|w| w == b"\0alice\0secret"),
            "the PLAIN token is sent again"
        );

        // The broker accepts it and grants a new, longer session.
        time.sleep(5);
        transport.enqueue_read_data(&build_sasl_authenticate_response_bytes_with_session_lifetime(
            headers[0].correlation_id(),
            &Errors::None,
            None,
            &[],
            auth.sasl_authenticate_version(),
            20_000,
        ));
        auth.authenticate_impl(&mut transport).await.unwrap();
        assert!(auth.complete());
        assert_eq!(auth.reauthentication_latency_ms(), Some(5));
        let after = auth.client_session_reauthentication_time_nanos().expect("rescheduled") - time.nanoseconds();
        assert!((17_000_000_000..=19_000_000_000).contains(&after), "{after}");
    }

    /// Java's `KafkaChannel` hands the receive it had in progress to the
    /// authenticator, which reads the rest of it into the same buffer: the
    /// response is set aside whole rather than lost, which would leave the rest
    /// of the connection's bytes misframed.
    #[tokio::test]
    async fn test_reauthentication_continues_the_receive_in_progress() {
        let time = Arc::new(MockTime::new());
        let mut auth = authenticator_with_time(time.clone());
        let mut transport = MockTransportLayer::new();
        complete_authentication(&mut auth, &mut transport, 10_000).await;
        time.sleep(9_600);

        // The channel had read the first 7 bytes of a response.
        let earlier_response = build_api_versions_response_bytes(42, 1, 2);
        let mut partial = NetworkReceive::with_source("node-0");
        let mut first_bytes = MockTransportLayer::new();
        first_bytes.enqueue_read_data(&earlier_response[..7]);
        partial.read_from(&mut first_bytes).await.unwrap();
        assert!(!partial.complete());

        auth.reauthenticate(ReauthenticationContext::new(Some(partial), time.nanoseconds()), &mut transport)
            .await
            .unwrap();
        transport.enqueue_read_data(&earlier_response[7..]);
        auth.authenticate_impl(&mut transport).await.unwrap();

        let set_aside = auth
            .poll_response_received_during_reauthentication()
            .expect("the response in progress is set aside");
        assert_eq!(set_aside.payload(), Some(&earlier_response[4..]));
        assert_eq!(auth.sasl_state(), SaslState::ReauthReceiveHandshakeOrOtherResponse);
    }

    /// A re-authentication the broker rejects fails like an authentication:
    /// with the class the code names, as Java's `receiveToken` throws.
    #[tokio::test]
    async fn test_rejected_reauthentication_is_an_authentication_failure() {
        let time = Arc::new(MockTime::new());
        let mut auth = authenticator_with_time(time.clone());
        let mut transport = MockTransportLayer::new();
        complete_authentication(&mut auth, &mut transport, 10_000).await;
        time.sleep(9_600);

        let token = reauthenticate_through_handshake(
            &mut auth,
            &mut transport,
            ReauthenticationContext::new(None, time.nanoseconds()),
        )
        .await;
        transport.enqueue_read_data(&build_sasl_authenticate_response_bytes(
            token.correlation_id(),
            &Errors::SaslAuthenticationFailed,
            Some("Authentication failed: Invalid username or password"),
            &[],
            auth.sasl_authenticate_version(),
        ));
        let err = auth.authenticate_impl(&mut transport).await.unwrap_err();
        let error = authentication_error(&err).expect("an authentication failure");
        assert!(matches!(error, Error::SaslAuthentication(_)), "{error:?}");
        assert_eq!(error.message(), "Authentication failed: Invalid username or password");
        assert_eq!(auth.sasl_state(), SaslState::Failed);
    }

    /// A second re-authentication still uses the versions of the original
    /// authentication, as Java's new authenticator reads them from the previous
    /// one each time.
    #[tokio::test]
    async fn test_second_reauthentication() {
        let time = Arc::new(MockTime::new());
        let mut auth = authenticator_with_time(time.clone());
        let mut transport = MockTransportLayer::new();
        complete_authentication(&mut auth, &mut transport, 10_000).await;
        let handshake_version = auth.sasl_handshake_version();

        for round in 0..2 {
            time.sleep(9_600);
            let token = reauthenticate_through_handshake(
                &mut auth,
                &mut transport,
                ReauthenticationContext::new(None, time.nanoseconds()),
            )
            .await;
            transport.enqueue_read_data(&build_sasl_authenticate_response_bytes_with_session_lifetime(
                token.correlation_id(),
                &Errors::None,
                None,
                &[],
                auth.sasl_authenticate_version(),
                10_000,
            ));
            auth.authenticate_impl(&mut transport).await.unwrap();
            assert!(auth.complete(), "round {round}");
            assert_eq!(auth.sasl_handshake_version(), handshake_version, "round {round}");
            assert!(auth.client_session_reauthentication_time_nanos().is_some(), "round {round}");
        }
    }
}
