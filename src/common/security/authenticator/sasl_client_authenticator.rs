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
//! Currently supports PLAIN mechanism only (RFC 4616). The state machine is
//! designed for extensibility to SCRAM and other challenge-response mechanisms.
//!
//! # Non-blocking Design
//!
//! `authenticate()` is called repeatedly by `KafkaChannel::prepare()`. Each call:
//! 1. Flushes pending outbound data
//! 2. Attempts one state transition
//! 3. Returns `Ok(())` if I/O would block (partial read/write)
//! 4. Stores partial reads in `net_in_buffer` for the next call

use crate::common::Error;
use crate::common::network::Authenticator;
use crate::common::network::ByteBufferSend;
use crate::common::network::KafkaSend;
use crate::common::network::NetworkReceive;
use crate::common::network::Receive;
use crate::common::network::authentication_error::{auth_io_error, auth_io_error_with_source};
use crate::common::network::{InterestOps, TransportLayer};
use crate::common::protocol::{ApiKeys, ByteBufferAccessor, Errors};
use crate::common::requests::ApiVersionsRequestBuilder;
use crate::common::requests::ApiVersionsResponse;
use crate::common::requests::ConcreteRequest;
use crate::common::requests::ConcreteResponse;
use crate::common::requests::RequestBuilder;
use crate::common::requests::RequestHeader;
use crate::common::requests::SaslAuthenticateRequest;
use crate::common::requests::SaslHandshakeRequest;
use crate::common::requests::SaslHandshakeResponse;
use crate::sasl_authenticate_request_data::SaslAuthenticateRequestData;
use crate::sasl_handshake_request_data::SaslHandshakeRequestData;

use crate::common::utils::LogContext;
use crate::kafka_debug;

use std::future::Future;
use std::io;
use std::pin::Pin;

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
    SASL_CLIENT_AUTHENTICATOR_MAX_RESERVED_CORRELATION_ID - 7;

/// Returns `true` if the correlation ID is reserved for SASL requests.
pub fn is_reserved(correlation_id: i32) -> bool {
    correlation_id >= SASL_CLIENT_AUTHENTICATOR_MIN_RESERVED_CORRELATION_ID
}

/// Internal state transitions for SASL client authentication.
///
/// The states are declared in order, starting with `SendApiVersionsRequest` and
/// ending in either `Complete` or `Failed`.
///
/// Translated from `SaslClientAuthenticator.SaslState` in Java.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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
}

/// SASL client authenticator that implements the SASL PLAIN authentication flow.
///
/// Translated from `org.apache.kafka.common.security.authenticator.SaslClientAuthenticator`.
///
/// This implementation supports PLAIN mechanism only (RFC 4616). The token format
/// is `\0username\0password`.
///
/// # Re-authentication
///
/// Re-authentication states from the Java source are intentionally omitted since
/// they are out of scope for the current milestone.
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
    #[allow(dead_code)]
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
    net_out_buffer: Option<Box<dyn KafkaSend>>,
    /// Pending inbound data.
    net_in_buffer: Option<NetworkReceive>,
    /// Next SASL state to be set when outgoing writes complete.
    pending_sasl_state: Option<SaslState>,
    /// Contextual log message prefix.
    log_context: LogContext,
}

impl SaslClientAuthenticator {
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
    pub fn new(
        mechanism: &str,
        username: &str,
        password: &str,
        node: &str,
        host: &str,
        client_id: &str,
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
            sasl_authenticate_version: DISABLE_KAFKA_SASL_AUTHENTICATE_HEADER,
            current_request_header: None,
            net_out_buffer: None,
            net_in_buffer: None,
            pending_sasl_state: None,
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
    fn next_correlation_id(&mut self) -> i32 {
        if !is_reserved(self.correlation_id) {
            self.correlation_id = SASL_CLIENT_AUTHENTICATOR_MIN_RESERVED_CORRELATION_ID;
        }
        let id = self.correlation_id;
        self.correlation_id = self.correlation_id.wrapping_add(1);
        id
    }

    /// Creates the next request header for the given API key and version.
    fn next_request_header(&mut self, api_key: &'static ApiKeys, version: i16) -> io::Result<RequestHeader> {
        let correlation_id = self.next_correlation_id();
        let header = RequestHeader::new(api_key, version, &self.client_id, correlation_id)?;
        self.current_request_header = Some(header.clone());
        Ok(header)
    }

    /// Creates a PLAIN SASL token in RFC 4616 format: `\0username\0password`.
    ///
    /// For PLAIN, the token is always the same regardless of whether this is
    /// an initial token or a challenge response.
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
        send: Box<dyn KafkaSend>,
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
    async fn send_sasl_client_token(&mut self, transport: &mut (dyn TransportLayer + Send)) -> io::Result<()> {
        let sasl_token = self.create_sasl_token();
        let send: Box<dyn KafkaSend> = if self.sasl_authenticate_version == DISABLE_KAFKA_SASL_AUTHENTICATE_HEADER {
            Box::new(ByteBufferSend::size_prefixed(bytes::Bytes::from(sasl_token)))
        } else {
            let mut data = SaslAuthenticateRequestData::new();
            data.set_auth_bytes(sasl_token);
            let request = SaslAuthenticateRequest::new(data, self.sasl_authenticate_version);
            let header = self.next_request_header(&ApiKeys::SASL_AUTHENTICATE, self.sasl_authenticate_version)?;
            let mut concrete = ConcreteRequest::SaslAuthenticate(request);
            let byte_buffer_send = concrete.to_send(&header)?;
            Box::new(byte_buffer_send)
        };
        self.send_request(send, transport).await
    }

    /// Flushes the outbound buffer to the transport and updates interest ops.
    ///
    /// Returns `true` if the buffer was completely flushed.
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
    async fn receive_response_or_token(
        &mut self,
        transport: &mut (dyn TransportLayer + Send),
    ) -> io::Result<Option<Vec<u8>>> {
        if self.net_in_buffer.is_none() {
            self.net_in_buffer = Some(NetworkReceive::new_source(&self.node));
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
        let mut buffer = ByteBufferAccessor::from_bytes(response_bytes);
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
        // Java's re-authentication branch (`reauthInfo.reauthenticating()`, which
        // parks an unrelated in-flight receive in `pendingAuthenticatedReceives`
        // and returns null) has no counterpart: KIP-368 client-side
        // re-authentication is not wired in this client, so `reauthenticating()`
        // is always false and the branch is unreachable. See COMMENTS finding 25.
        let response = match crate::network_client::parse_response(&mut buffer, request_header) {
            Ok(response) => response,
            Err(error) if matches!(error, Error::Schema(_) | Error::LocalIllegalArgument(_)) => {
                kafka_debug!(
                    self.log_context,
                    "Invalid SASL mechanism response, server may be expecting only GSSAPI tokens"
                );
                self.set_sasl_state(SaslState::Failed);
                // Java: `throw new IllegalSaslStateException(msg, e)` — the
                // two-argument form, so the message is Java's literal text and the
                // cause hangs off `getCause()` rather than being appended to it.
                return Err(auth_io_error_with_source(
                    "Invalid SASL mechanism response, server may be expecting a different protocol",
                    error,
                ));
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
    async fn receive_token(&mut self, transport: &mut (dyn TransportLayer + Send)) -> io::Result<Option<Vec<u8>>> {
        if self.sasl_authenticate_version == DISABLE_KAFKA_SASL_AUTHENTICATE_HEADER {
            self.receive_response_or_token(transport).await
        } else {
            match self.receive_kafka_response(transport).await? {
                Some(ConcreteResponse::SaslAuthenticate(response)) => {
                    let error = response.error();
                    if error != Errors::None {
                        self.set_sasl_state(SaslState::Failed);
                        let err_msg = response.error_message().unwrap_or(error.message());
                        // The broker rejected the credentials. Java throws
                        // SaslAuthenticationException (an AuthenticationException):
                        // a fatal authentication failure, not a retriable disconnect.
                        return Err(auth_io_error(err_msg.to_string()));
                    }
                    Ok(Some(response.sasl_auth_bytes().to_vec()))
                },
                Some(_) => Err(auth_io_error("Expected SaslAuthenticate response")),
                None => Ok(None),
            }
        }
    }

    /// Sets the SASL state, deferring the transition if there is pending outbound data.
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
            // In the full Java implementation, this would set session
            // re-authentication times and update interest ops. For the
            // client-only PLAIN implementation, authentication is done.
        }
    }

    /// Extracts SASL handshake and authenticate versions from the ApiVersionsResponse.
    ///
    /// Translated from `setSaslAuthenticateAndHandshakeVersions` in Java.
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
    fn handle_sasl_handshake_response(&mut self, response: &SaslHandshakeResponse) -> io::Result<()> {
        let error = response.error();
        if error != Errors::None {
            self.set_sasl_state(SaslState::Failed);
        }
        // All non-None handshake errors are AuthenticationException subclasses
        // in Java (UnsupportedSaslMechanismException / IllegalSaslStateException):
        // genuine authentication failures, fatal and not retried.
        match error {
            Errors::None => Ok(()),
            Errors::UnsupportedSaslMechanism => Err(auth_io_error(format!(
                "Client SASL mechanism '{}' not enabled in the server, enabled mechanisms are {:?}",
                self.mechanism,
                response.enabled_mechanisms()
            ))),
            Errors::IllegalSaslState => Err(auth_io_error(format!(
                "Unexpected handshake request with client mechanism {}, enabled mechanisms are {:?}",
                self.mechanism,
                response.enabled_mechanisms()
            ))),
            _ => Err(auth_io_error(format!(
                "Unknown error code {:?}, client mechanism is {}, enabled mechanisms are {:?}",
                error,
                self.mechanism,
                response.enabled_mechanisms()
            ))),
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
                let mut builder = ApiVersionsRequestBuilder::for_version(0);
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
    async fn send_handshake_request(&mut self, transport: &mut (dyn TransportLayer + Send)) -> io::Result<()> {
        let mut data = SaslHandshakeRequestData::new();
        data.set_mechanism(self.mechanism.clone());
        let request = SaslHandshakeRequest::new(data, self.sasl_handshake_version);
        let header = self.next_request_header(&ApiKeys::SASL_HANDSHAKE, request.version())?;
        let mut concrete = ConcreteRequest::SaslHandshake(request);
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

    fn close(&mut self) {
        // No resources to release for PLAIN mechanism.
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api_versions_response_data::{ApiVersion, ApiVersionsResponseData};
    use crate::common::network::InterestOps;
    use crate::common::network::authentication_error::is_authentication_error;
    use crate::common::protocol::Message;
    use crate::common::protocol::ObjectSerializationCache;
    use crate::common::protocol::Writable;
    use crate::common::requests::ResponseHeader;
    use crate::sasl_authenticate_response_data::SaslAuthenticateResponseData;
    use crate::sasl_handshake_response_data::SaslHandshakeResponseData;

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
        let mut response_header = ResponseHeader::new(correlation_id, 0); // v0 header for ApiVersions v0
        let mut cache = ObjectSerializationCache::new();
        let header_size = Message::size(response_header.data(), &mut cache, response_header.header_version()).unwrap();
        let body_size = Message::size(&data, &mut cache, 0).unwrap();
        let total_size = header_size + body_size;

        let mut buf = ByteBufferAccessor::new(4 + total_size as usize);
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
        let mut response_header = ResponseHeader::new(correlation_id, header_version);

        let mut cache = ObjectSerializationCache::new();
        let header_size = Message::size(response_header.data(), &mut cache, response_header.header_version()).unwrap();
        let body_size = Message::size(&data, &mut cache, version).unwrap();
        let total_size = header_size + body_size;

        let mut buf = ByteBufferAccessor::new(4 + total_size as usize);
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
        let mut data = SaslAuthenticateResponseData::new();
        data.set_error_code(error.code());
        data.set_error_message(error_message.map(|s| s.to_string()));
        data.set_auth_bytes(auth_bytes.to_vec());

        let api_key = &ApiKeys::SASL_AUTHENTICATE;
        let header_version = api_key.response_header_version(version);
        let mut response_header = ResponseHeader::new(correlation_id, header_version);

        let mut cache = ObjectSerializationCache::new();
        let header_size = Message::size(response_header.data(), &mut cache, response_header.header_version()).unwrap();
        let body_size = Message::size(&data, &mut cache, version).unwrap();
        let total_size = header_size + body_size;

        let mut buf = ByteBufferAccessor::new(4 + total_size as usize);
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
            LogContext::empty(),
        );
        let id1 = auth.next_correlation_id();
        assert!(is_reserved(id1));
        assert_eq!(id1, SASL_CLIENT_AUTHENTICATOR_MIN_RESERVED_CORRELATION_ID);

        let id2 = auth.next_correlation_id();
        assert!(is_reserved(id2));
        assert_eq!(id2, SASL_CLIENT_AUTHENTICATOR_MIN_RESERVED_CORRELATION_ID + 1);
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
            LogContext::empty(),
        );
        // Exhaust all 8 reserved IDs
        for i in 0..8 {
            let id = auth.next_correlation_id();
            assert_eq!(id, SASL_CLIENT_AUTHENTICATOR_MIN_RESERVED_CORRELATION_ID + i);
            assert!(is_reserved(id));
        }
        // At this point correlation_id has wrapped past i32::MAX.
        // The 9th call should detect that the ID is no longer reserved and
        // reset to SASL_CLIENT_AUTHENTICATOR_MIN_RESERVED_CORRELATION_ID.
        let id = auth.next_correlation_id();
        assert_eq!(id, SASL_CLIENT_AUTHENTICATOR_MIN_RESERVED_CORRELATION_ID);
        assert!(is_reserved(id));
    }

    /// Test 4: is_reserved boundary conditions.
    #[test]
    fn test_is_reserved() {
        assert!(is_reserved(SASL_CLIENT_AUTHENTICATOR_MAX_RESERVED_CORRELATION_ID));
        assert!(is_reserved(SASL_CLIENT_AUTHENTICATOR_MIN_RESERVED_CORRELATION_ID));
        assert!(!is_reserved(SASL_CLIENT_AUTHENTICATOR_MIN_RESERVED_CORRELATION_ID - 1));
        assert!(!is_reserved(0));
        assert!(!is_reserved(-1));
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
            LogContext::empty(),
        );
        let mut transport = MockTransportLayer::new();

        // Step 1: Send ApiVersionsRequest
        auth.authenticate_impl(&mut transport).await.unwrap();
        assert_eq!(auth.sasl_state(), SaslState::ReceiveApiVersionsResponse);
        assert!(!transport.written_data().is_empty());

        // Step 2: Receive ApiVersionsResponse
        let api_versions_bytes = build_api_versions_response_bytes(
            SASL_CLIENT_AUTHENTICATOR_MIN_RESERVED_CORRELATION_ID,
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
            SASL_CLIENT_AUTHENTICATOR_MIN_RESERVED_CORRELATION_ID + 1,
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
            SASL_CLIENT_AUTHENTICATOR_MIN_RESERVED_CORRELATION_ID + 2,
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
            LogContext::empty(),
        );
        let mut transport = MockTransportLayer::new();

        // Send ApiVersionsRequest
        auth.authenticate_impl(&mut transport).await.unwrap();

        // Receive ApiVersionsResponse
        let api_versions_bytes = build_api_versions_response_bytes(
            SASL_CLIENT_AUTHENTICATOR_MIN_RESERVED_CORRELATION_ID,
            ApiKeys::SASL_HANDSHAKE.latest_version(),
            ApiKeys::SASL_AUTHENTICATE.latest_version(),
        );
        transport.enqueue_read_data(&api_versions_bytes);
        transport.write_data.clear();
        auth.authenticate_impl(&mut transport).await.unwrap();

        // Receive SaslHandshakeResponse with UNSUPPORTED_SASL_MECHANISM
        let handshake_bytes = build_sasl_handshake_response_bytes(
            SASL_CLIENT_AUTHENTICATOR_MIN_RESERVED_CORRELATION_ID + 1,
            &Errors::UnsupportedSaslMechanism,
            vec!["SCRAM-SHA-256".to_string()],
            auth.sasl_handshake_version(),
        );
        transport.enqueue_read_data(&handshake_bytes);

        let err = auth.authenticate_impl(&mut transport).await.unwrap_err();
        assert!(err.to_string().contains("PLAIN"));
        assert!(err.to_string().contains("SCRAM-SHA-256"));
        // An unsupported-mechanism failure is a genuine authentication failure
        // (Java: UnsupportedSaslMechanismException extends AuthenticationException)
        // and must be classified as fatal, not a retriable disconnect.
        assert!(is_authentication_error(&err));
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
            LogContext::empty(),
        );
        let mut transport = MockTransportLayer::new();

        // Send ApiVersionsRequest
        auth.authenticate_impl(&mut transport).await.unwrap();

        // Receive ApiVersionsResponse
        let api_versions_bytes = build_api_versions_response_bytes(
            SASL_CLIENT_AUTHENTICATOR_MIN_RESERVED_CORRELATION_ID,
            ApiKeys::SASL_HANDSHAKE.latest_version(),
            ApiKeys::SASL_AUTHENTICATE.latest_version(),
        );
        transport.enqueue_read_data(&api_versions_bytes);
        transport.write_data.clear();
        auth.authenticate_impl(&mut transport).await.unwrap();

        // Receive SaslHandshakeResponse (success)
        let handshake_bytes = build_sasl_handshake_response_bytes(
            SASL_CLIENT_AUTHENTICATOR_MIN_RESERVED_CORRELATION_ID + 1,
            &Errors::None,
            vec!["PLAIN".to_string()],
            auth.sasl_handshake_version(),
        );
        transport.enqueue_read_data(&handshake_bytes);
        transport.write_data.clear();
        auth.authenticate_impl(&mut transport).await.unwrap();

        // Receive SaslAuthenticateResponse with error
        let sasl_auth_bytes = build_sasl_authenticate_response_bytes(
            SASL_CLIENT_AUTHENTICATOR_MIN_RESERVED_CORRELATION_ID + 2,
            &Errors::SaslAuthenticationFailed,
            Some("Authentication failed: Invalid credentials"),
            &[],
            auth.sasl_authenticate_version(),
        );
        transport.enqueue_read_data(&sasl_auth_bytes);

        let err = auth.authenticate_impl(&mut transport).await.unwrap_err();
        assert!(err.to_string().contains("Authentication failed"));
        // A broker-side credential rejection is a genuine authentication failure
        // (Java: SaslAuthenticationException extends AuthenticationException);
        // it must be classified as fatal so it is not silently retried.
        assert!(is_authentication_error(&err));
        assert_eq!(auth.sasl_state(), SaslState::Failed);
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

        let mut response_header = ResponseHeader::new(SASL_CLIENT_AUTHENTICATOR_MIN_RESERVED_CORRELATION_ID, 0);
        let mut cache = ObjectSerializationCache::new();
        let header_size = Message::size(response_header.data(), &mut cache, response_header.header_version()).unwrap();
        let body_size = Message::size(&data, &mut cache, 0).unwrap();
        let total_size = header_size + body_size;
        let mut buf = ByteBufferAccessor::new(4 + total_size as usize);
        buf.write_int(total_size).unwrap();
        let hv = response_header.header_version();
        Message::write(response_header.data_mut(), &mut buf, &cache, hv).unwrap();
        Message::write(&mut data, &mut buf, &cache, 0).unwrap();
        transport.enqueue_read_data(buf.buffer());
        transport.write_data.clear();

        auth.authenticate_impl(&mut transport).await.unwrap();
        // Should be in ReceiveHandshakeResponse (fell through from ReceiveApiVersionsResponse)
        assert_eq!(auth.sasl_authenticate_version(), DISABLE_KAFKA_SASL_AUTHENTICATE_HEADER);

        // Receive SaslHandshakeResponse
        let handshake_bytes = build_sasl_handshake_response_bytes(
            SASL_CLIENT_AUTHENTICATOR_MIN_RESERVED_CORRELATION_ID + 1,
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
            LogContext::empty(),
        );
        let mut transport = MockTransportLayer::new();

        // Send ApiVersionsRequest
        auth.authenticate_impl(&mut transport).await.unwrap();

        // Receive ApiVersionsResponse
        let api_versions_bytes = build_api_versions_response_bytes(
            SASL_CLIENT_AUTHENTICATOR_MIN_RESERVED_CORRELATION_ID,
            ApiKeys::SASL_HANDSHAKE.latest_version(),
            ApiKeys::SASL_AUTHENTICATE.latest_version(),
        );
        transport.enqueue_read_data(&api_versions_bytes);
        transport.write_data.clear();
        auth.authenticate_impl(&mut transport).await.unwrap();

        // Receive SaslHandshakeResponse with ILLEGAL_SASL_STATE
        let handshake_bytes = build_sasl_handshake_response_bytes(
            SASL_CLIENT_AUTHENTICATOR_MIN_RESERVED_CORRELATION_ID + 1,
            &Errors::IllegalSaslState,
            vec!["PLAIN".to_string()],
            auth.sasl_handshake_version(),
        );
        transport.enqueue_read_data(&handshake_bytes);

        let err = auth.authenticate_impl(&mut transport).await.unwrap_err();
        assert!(err.to_string().contains("Unexpected handshake request"));
        // Java: IllegalSaslStateException extends AuthenticationException — fatal.
        assert!(is_authentication_error(&err));
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
    }
}
