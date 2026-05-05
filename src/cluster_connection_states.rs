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

//! Translation of `org.apache.kafka.clients.ClusterConnectionStates`.
//!
//! Note: the Java type lives in `org.apache.kafka.clients` (not `common`),
//! so it sits at the crate root rather than under `common::`.
//!
//! ## Hot-path key type (i32, not String)
//!
//! Java keys per-node state by `String` (the connection id). The Rust
//! translation uses `i32` everywhere — see
//! `design/history/Milestone-1/Phase-5/NOTES.md` "Hot-path identifier
//! interning". The Java string is `Integer.toString(node.id())`, so
//! using the integer directly avoids a per-call `String` clone.
//!
//! ## Authentication error storage
//!
//! Java stores the `AuthenticationException` directly on
//! `NodeConnectionState`. The Rust translation stores
//! [`KafkaError::Authentication`] inside an `Option<KafkaError>` so the
//! variant is checked at the type level.

use std::collections::{HashMap, HashSet};
use std::net::IpAddr;

use log::info;

use crate::ConnectionState;
use crate::client_utils;
use crate::common::errors::KafkaError;
use crate::common::utils::{ExponentialBackoff, LogContext};
use crate::host_resolver::HostResolver;

/// Mirrors `ClusterConnectionStates.RECONNECT_BACKOFF_EXP_BASE`.
pub const RECONNECT_BACKOFF_EXP_BASE: i32 = 2;

/// Mirrors `ClusterConnectionStates.RECONNECT_BACKOFF_JITTER`.
pub const RECONNECT_BACKOFF_JITTER: f64 = 0.2;

/// Mirrors `ClusterConnectionStates.CONNECTION_SETUP_TIMEOUT_EXP_BASE`.
pub const CONNECTION_SETUP_TIMEOUT_EXP_BASE: i32 = 2;

/// Mirrors `ClusterConnectionStates.CONNECTION_SETUP_TIMEOUT_JITTER`.
pub const CONNECTION_SETUP_TIMEOUT_JITTER: f64 = 0.2;

/// The state of our connection to each node in the cluster.
///
/// Mirrors `org.apache.kafka.clients.ClusterConnectionStates`. **Java's
/// contract**: this class is *not* thread-safe (every public method is
/// invoked under the `NetworkClient`'s exclusive ownership). The Rust
/// translation reflects that — every state-mutating method takes
/// `&mut self`.
pub struct ClusterConnectionStates {
    node_state: HashMap<i32, NodeConnectionState>,
    log_prefix: String,
    host_resolver: Box<dyn HostResolver>,
    connecting_nodes: HashSet<i32>,
    reconnect_backoff: ExponentialBackoff,
    connection_setup_timeout: ExponentialBackoff,
}

impl ClusterConnectionStates {
    /// Mirrors `new ClusterConnectionStates(long, long, long, long,
    /// LogContext, HostResolver)`.
    ///
    /// # Errors
    ///
    /// Propagates [`String`] from
    /// [`ExponentialBackoff::new`] when jitter validation fails (Java
    /// raises `IllegalArgumentException`).
    pub fn new(
        reconnect_backoff_ms: i64,
        reconnect_backoff_max_ms: i64,
        connection_setup_timeout_ms: i64,
        connection_setup_timeout_max_ms: i64,
        log_context: LogContext,
        host_resolver: Box<dyn HostResolver>,
    ) -> Result<Self, String> {
        let reconnect_backoff = ExponentialBackoff::new(
            reconnect_backoff_ms,
            RECONNECT_BACKOFF_EXP_BASE,
            reconnect_backoff_max_ms,
            RECONNECT_BACKOFF_JITTER,
        )?;
        let connection_setup_timeout = ExponentialBackoff::new(
            connection_setup_timeout_ms,
            CONNECTION_SETUP_TIMEOUT_EXP_BASE,
            connection_setup_timeout_max_ms,
            CONNECTION_SETUP_TIMEOUT_JITTER,
        )?;
        Ok(ClusterConnectionStates {
            node_state: HashMap::new(),
            log_prefix: log_context.log_prefix().to_owned(),
            host_resolver,
            connecting_nodes: HashSet::new(),
            reconnect_backoff,
            connection_setup_timeout,
        })
    }

    /// Return true iff we can currently initiate a new connection.
    /// Mirrors `ClusterConnectionStates.canConnect(String, long)`.
    pub fn can_connect(&self, id: i32, now: i64) -> bool {
        match self.node_state.get(&id) {
            None => true,
            Some(state) => {
                state.state.is_disconnected() && now - state.last_connect_attempt_ms >= state.reconnect_backoff_ms
            },
        }
    }

    /// Return true if we are disconnected from the given node and
    /// can't re-establish a connection yet. Mirrors
    /// `ClusterConnectionStates.isBlackedOut(String, long)`.
    pub fn is_blacked_out(&self, id: i32, now: i64) -> bool {
        match self.node_state.get(&id) {
            Some(state) => {
                state.state.is_disconnected() && now - state.last_connect_attempt_ms < state.reconnect_backoff_ms
            },
            None => false,
        }
    }

    /// Returns the number of milliseconds to wait, based on the
    /// connection state, before attempting to send data. Mirrors
    /// `ClusterConnectionStates.connectionDelay(String, long)`.
    pub fn connection_delay(&self, id: i32, now: i64) -> i64 {
        let state = match self.node_state.get(&id) {
            Some(s) => s,
            None => return 0,
        };
        if state.state == ConnectionState::Connecting {
            self.connection_setup_timeout_ms(id)
        } else if state.state.is_disconnected() {
            let time_waited = now - state.last_connect_attempt_ms;
            (state.reconnect_backoff_ms - time_waited).max(0)
        } else {
            // When connected, we should be able to delay indefinitely
            // since other events (connection or data acked) will cause
            // a wakeup once data can be sent.
            i64::MAX
        }
    }

    /// Return true if a specific connection establishment is currently
    /// underway. Mirrors `ClusterConnectionStates.isConnecting(String)`.
    pub fn is_connecting(&self, id: i32) -> bool {
        matches!(self.node_state.get(&id), Some(state) if state.state == ConnectionState::Connecting)
    }

    /// Check whether a connection is either being established or
    /// awaiting API version information. Mirrors
    /// `ClusterConnectionStates.isPreparingConnection(String)`.
    pub fn is_preparing_connection(&self, id: i32) -> bool {
        matches!(
            self.node_state.get(&id),
            Some(state) if state.state == ConnectionState::Connecting || state.state == ConnectionState::CheckingApiVersions
        )
    }

    /// Enter the connecting state for the given connection, moving to
    /// a new resolved address if necessary. Mirrors
    /// `ClusterConnectionStates.connecting(String, long, String)`.
    pub fn connecting(&mut self, id: i32, now: i64, host: &str) {
        let host_changed = match self.node_state.get_mut(&id) {
            Some(state) if state.host == host => {
                state.last_connect_attempt_ms = now;
                state.state = ConnectionState::Connecting;
                // Move to next resolved address, or if addresses are
                // exhausted, mark node to be re-resolved.
                state.move_to_next_address();
                self.connecting_nodes.insert(id);
                return;
            },
            Some(state) => {
                info!(
                    "{prefix}Hostname for node {id} changed from {old_host} to {host}.",
                    prefix = self.log_prefix,
                    id = id,
                    old_host = state.host,
                    host = host
                );
                true
            },
            None => false,
        };
        // Create a new NodeConnectionState if nodeState does not already
        // contain one for the specified id, or if the hostname
        // associated with the node id changed.
        let _ = host_changed; // Java: log only; replace below
        let new_state = NodeConnectionState::new(
            ConnectionState::Connecting,
            now,
            self.reconnect_backoff.backoff(0),
            self.connection_setup_timeout.backoff(0),
            host.to_owned(),
        );
        self.node_state.insert(id, new_state);
        self.connecting_nodes.insert(id);
    }

    /// Returns a resolved address for the given connection, resolving
    /// it if necessary. Mirrors
    /// `ClusterConnectionStates.currentAddress(String)`.
    ///
    /// # Errors
    ///
    /// [`KafkaError::Network`] when DNS resolution fails (Java throws
    /// `UnknownHostException`).
    pub fn current_address(&mut self, id: i32) -> Result<IpAddr, KafkaError> {
        let resolver = self.host_resolver.as_ref();
        let log_prefix = &self.log_prefix;
        let state = Self::node_state_mut_inner(&mut self.node_state, id);
        state.current_address_with(resolver, log_prefix)
    }

    /// Enter the disconnected state for the given node. Mirrors
    /// `ClusterConnectionStates.disconnected(String, long)`.
    pub fn disconnected(&mut self, id: i32, now: i64) {
        let state = Self::node_state_mut_inner(&mut self.node_state, id);
        state.last_connect_attempt_ms = now;
        Self::update_reconnect_backoff(&self.reconnect_backoff, state);
        if state.state == ConnectionState::Connecting {
            Self::update_connection_setup_timeout(&self.connection_setup_timeout, state);
            self.connecting_nodes.remove(&id);
        } else {
            Self::reset_connection_setup_timeout(&self.connection_setup_timeout, state);
            if state.state.is_connected() {
                // If a connection had previously been established, clear
                // the addresses to trigger a new DNS resolution because
                // the node IPs may have changed.
                state.clear_addresses();
            }
        }
        state.state = ConnectionState::Disconnected;
    }

    /// Indicate that the connection is throttled until the specified
    /// deadline. Mirrors
    /// `ClusterConnectionStates.throttle(String, long)`.
    pub fn throttle(&mut self, id: i32, throttle_until_time_ms: i64) {
        if let Some(state) = self.node_state.get_mut(&id) {
            // The throttle deadline should never regress.
            if state.throttle_until_time_ms < throttle_until_time_ms {
                state.throttle_until_time_ms = throttle_until_time_ms;
            }
        }
    }

    /// Return the remaining throttling delay in milliseconds if
    /// throttling is in progress. Return 0, otherwise. Mirrors
    /// `ClusterConnectionStates.throttleDelayMs(String, long)`.
    pub fn throttle_delay_ms(&self, id: i32, now: i64) -> i64 {
        match self.node_state.get(&id) {
            Some(state) if state.throttle_until_time_ms > now => state.throttle_until_time_ms - now,
            _ => 0,
        }
    }

    /// Return the number of milliseconds to wait, based on the
    /// connection state and the throttle time, before attempting to
    /// send data. Mirrors
    /// `ClusterConnectionStates.pollDelayMs(String, long)`.
    pub fn poll_delay_ms(&self, id: i32, now: i64) -> i64 {
        let throttle_delay_ms = self.throttle_delay_ms(id, now);
        if self.is_connected(id) && throttle_delay_ms > 0 {
            throttle_delay_ms
        } else {
            self.connection_delay(id, now)
        }
    }

    /// Enter the checking_api_versions state for the given node.
    /// Mirrors `ClusterConnectionStates.checkingApiVersions(String)`.
    pub fn checking_api_versions(&mut self, id: i32) {
        let state = Self::node_state_mut_inner(&mut self.node_state, id);
        state.state = ConnectionState::CheckingApiVersions;
        Self::reset_connection_setup_timeout(&self.connection_setup_timeout, state);
        self.connecting_nodes.remove(&id);
    }

    /// Enter the ready state for the given node. Mirrors
    /// `ClusterConnectionStates.ready(String)`.
    pub fn ready(&mut self, id: i32) {
        let state = Self::node_state_mut_inner(&mut self.node_state, id);
        state.state = ConnectionState::Ready;
        state.authentication_error = None;
        Self::reset_reconnect_backoff(&self.reconnect_backoff, state);
        Self::reset_connection_setup_timeout(&self.connection_setup_timeout, state);
        self.connecting_nodes.remove(&id);
    }

    /// Enter the authentication failed state for the given node.
    /// Mirrors
    /// `ClusterConnectionStates.authenticationFailed(String, long, AuthenticationException)`.
    ///
    /// `error` should carry the cause as a [`KafkaError::Authentication`]
    /// variant.
    pub fn authentication_failed(&mut self, id: i32, now: i64, error: KafkaError) {
        let state = Self::node_state_mut_inner(&mut self.node_state, id);
        state.authentication_error = Some(error);
        state.state = ConnectionState::AuthenticationFailed;
        state.last_connect_attempt_ms = now;
        Self::update_reconnect_backoff(&self.reconnect_backoff, state);
    }

    /// Return true if the connection is in the READY state and
    /// currently not throttled. Mirrors
    /// `ClusterConnectionStates.isReady(String, long)`.
    pub fn is_ready(&self, id: i32, now: i64) -> bool {
        Self::is_ready_state(self.node_state.get(&id), now)
    }

    fn is_ready_state(state: Option<&NodeConnectionState>, now: i64) -> bool {
        matches!(state, Some(s) if s.state == ConnectionState::Ready && s.throttle_until_time_ms <= now)
    }

    /// Return true if there is at least one node with connection in the
    /// READY state and not throttled. Mirrors
    /// `ClusterConnectionStates.hasReadyNodes(long)`.
    pub fn has_ready_nodes(&self, now: i64) -> bool {
        self.node_state.values().any(|state| Self::is_ready_state(Some(state), now))
    }

    /// Return true if the connection has been established. Mirrors
    /// `ClusterConnectionStates.isConnected(String)`.
    pub fn is_connected(&self, id: i32) -> bool {
        matches!(self.node_state.get(&id), Some(state) if state.state.is_connected())
    }

    /// Return true if the connection has been disconnected. Mirrors
    /// `ClusterConnectionStates.isDisconnected(String)`.
    pub fn is_disconnected(&self, id: i32) -> bool {
        matches!(self.node_state.get(&id), Some(state) if state.state.is_disconnected())
    }

    /// Return authentication error if an authentication error
    /// occurred. Mirrors
    /// `ClusterConnectionStates.authenticationException(String)`.
    pub fn authentication_error(&self, id: i32) -> Option<&KafkaError> {
        self.node_state.get(&id).and_then(|state| state.authentication_error.as_ref())
    }

    fn reset_reconnect_backoff(reconnect_backoff: &ExponentialBackoff, node_state: &mut NodeConnectionState) {
        node_state.failed_attempts = 0;
        node_state.reconnect_backoff_ms = reconnect_backoff.backoff(0);
    }

    fn reset_connection_setup_timeout(
        connection_setup_timeout: &ExponentialBackoff,
        node_state: &mut NodeConnectionState,
    ) {
        node_state.failed_connect_attempts = 0;
        node_state.connection_setup_timeout_ms = connection_setup_timeout.backoff(0);
    }

    fn update_reconnect_backoff(reconnect_backoff: &ExponentialBackoff, node_state: &mut NodeConnectionState) {
        node_state.reconnect_backoff_ms = reconnect_backoff.backoff(node_state.failed_attempts);
        node_state.failed_attempts += 1;
    }

    fn update_connection_setup_timeout(
        connection_setup_timeout: &ExponentialBackoff,
        node_state: &mut NodeConnectionState,
    ) {
        node_state.failed_connect_attempts += 1;
        node_state.connection_setup_timeout_ms = connection_setup_timeout.backoff(node_state.failed_connect_attempts);
    }

    /// Remove the given node from the tracked connection states.
    /// Mirrors `ClusterConnectionStates.remove(String)`.
    pub fn remove(&mut self, id: i32) {
        self.node_state.remove(&id);
        self.connecting_nodes.remove(&id);
    }

    /// Get the state of a given connection. Mirrors
    /// `ClusterConnectionStates.connectionState(String)`. Panics if the
    /// connection is unknown — Java throws `IllegalStateException`.
    pub fn connection_state(&self, id: i32) -> ConnectionState {
        Self::node_state_inner(&self.node_state, id).state
    }

    fn node_state_mut_inner(node_state: &mut HashMap<i32, NodeConnectionState>, id: i32) -> &mut NodeConnectionState {
        match node_state.get_mut(&id) {
            Some(s) => s,
            None => panic!("No entry found for connection {id}"),
        }
    }

    fn node_state_inner(node_state: &HashMap<i32, NodeConnectionState>, id: i32) -> &NodeConnectionState {
        match node_state.get(&id) {
            Some(s) => s,
            None => panic!("No entry found for connection {id}"),
        }
    }

    /// Get the id set of nodes which are in CONNECTING state.
    /// Mirrors the package-private `connectingNodes()` test hook.
    pub fn connecting_nodes(&self) -> &HashSet<i32> {
        &self.connecting_nodes
    }

    /// Get the timestamp of the latest connection attempt of a given
    /// node. Mirrors
    /// `ClusterConnectionStates.lastConnectAttemptMs(String)`.
    pub fn last_connect_attempt_ms(&self, id: i32) -> i64 {
        self.node_state.get(&id).map(|s| s.last_connect_attempt_ms).unwrap_or(0)
    }

    /// Get the current socket connection setup timeout of the given
    /// node. Mirrors
    /// `ClusterConnectionStates.connectionSetupTimeoutMs(String)`.
    pub fn connection_setup_timeout_ms(&self, id: i32) -> i64 {
        Self::node_state_inner(&self.node_state, id).connection_setup_timeout_ms
    }

    /// Test if the connection to the given node has reached its
    /// timeout. Mirrors
    /// `ClusterConnectionStates.isConnectionSetupTimeout(String, long)`.
    /// Panics with the Java error message if the node is not currently
    /// in `Connecting` state.
    pub fn is_connection_setup_timeout(&self, id: i32, now: i64) -> bool {
        let node_state = Self::node_state_inner(&self.node_state, id);
        if node_state.state != ConnectionState::Connecting {
            panic!("Node {id} is not in connecting state");
        }
        now - self.last_connect_attempt_ms(id) > self.connection_setup_timeout_ms(id)
    }

    /// Return the list of nodes whose connection setup has timed out.
    /// Mirrors
    /// `ClusterConnectionStates.nodesWithConnectionSetupTimeout(long)`.
    pub fn nodes_with_connection_setup_timeout(&self, now: i64) -> Vec<i32> {
        self.connecting_nodes
            .iter()
            .copied()
            .filter(|id| self.is_connection_setup_timeout(*id, now))
            .collect()
    }
}

/// The state of our connection to a node. Mirrors the private inner
/// class `ClusterConnectionStates.NodeConnectionState`.
struct NodeConnectionState {
    host: String,

    state: ConnectionState,
    authentication_error: Option<KafkaError>,
    last_connect_attempt_ms: i64,
    failed_attempts: i64,
    failed_connect_attempts: i64,
    reconnect_backoff_ms: i64,
    connection_setup_timeout_ms: i64,
    /// Connection is being throttled if `current_time < throttle_until_time_ms`.
    throttle_until_time_ms: i64,
    addresses: Vec<IpAddr>,
    address_index: i32,
    last_attempted_address: Option<IpAddr>,
}

impl NodeConnectionState {
    fn new(
        state: ConnectionState,
        last_connect_attempt_ms: i64,
        reconnect_backoff_ms: i64,
        connection_setup_timeout_ms: i64,
        host: String,
    ) -> Self {
        NodeConnectionState {
            host,
            state,
            addresses: Vec::new(),
            address_index: -1,
            authentication_error: None,
            last_connect_attempt_ms,
            failed_attempts: 0,
            failed_connect_attempts: 0,
            reconnect_backoff_ms,
            connection_setup_timeout_ms,
            throttle_until_time_ms: 0,
            last_attempted_address: None,
        }
    }

    /// Fetches the current selected IP address for this node, resolving
    /// `host` if necessary. Mirrors `currentAddress()`.
    fn current_address_with(&mut self, resolver: &dyn HostResolver, log_prefix: &str) -> Result<IpAddr, KafkaError> {
        if self.addresses.is_empty() {
            self.resolve_addresses(resolver, log_prefix)?;
        }
        // Save the address that we return so that we don't try it twice
        // in a row when we re-resolve due to disconnecting or exhausting
        // the addresses.
        let current_address = self.addresses[self.address_index as usize];
        self.last_attempted_address = Some(current_address);
        Ok(current_address)
    }

    /// Jumps to the next available resolved address for this node. If
    /// no other addresses are available, marks the list to be refreshed
    /// on the next `current_address_with()` call.
    fn move_to_next_address(&mut self) {
        if self.addresses.is_empty() {
            return; // Avoid div0. List will initialize on next currentAddress() call
        }
        self.address_index = (self.address_index + 1) % (self.addresses.len() as i32);
        if self.address_index == 0 {
            self.clear_addresses(); // Exhausted list. Re-resolve on next currentAddress() call
        }
    }

    fn resolve_addresses(&mut self, resolver: &dyn HostResolver, log_prefix: &str) -> Result<(), KafkaError> {
        // (Re-)initialize list
        let resolved = client_utils::resolve(&self.host, resolver)?;
        self.addresses = resolved;
        if log::log_enabled!(log::Level::Debug) {
            let joined = self.addresses.iter().map(|a| a.to_string()).collect::<Vec<_>>().join(",");
            log::debug!("{log_prefix}Resolved host {} to addresses {joined}", self.host);
        }
        self.address_index = 0;

        // We re-resolve DNS after disconnecting, but we don't want to
        // immediately reconnect to the address we just disconnected
        // from, in case we disconnected due to a problem with that IP
        // (such as a load balancer instance failure). Check the first
        // address in the list and skip it if it was the last address we
        // tried and there are multiple addresses to choose from.
        if self.addresses.len() > 1 && Some(self.addresses[self.address_index as usize]) == self.last_attempted_address
        {
            self.address_index += 1;
        }
        Ok(())
    }

    fn clear_addresses(&mut self) {
        self.addresses = Vec::new();
    }
}

impl std::fmt::Display for NodeConnectionState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "NodeConnectionState(state={:?}, lastConnectAttemptMs={}, failedAttempts={}, failedConnectAttempts={}, throttleUntilTimeMs={})",
            self.state,
            self.last_connect_attempt_ms,
            self.failed_attempts,
            self.failed_connect_attempts,
            self.throttle_until_time_ms
        )
    }
}

#[cfg(test)]
mod tests {
    //! Translation of `ClusterConnectionStatesTest`.
    //!
    //! Java tests use `String` node ids ("1001", "2002", "3003"). The
    //! Rust translation uses the parsed `i32` (1001, 2002, 3003) per
    //! the hot-path note at the top of this file.

    use std::net::Ipv4Addr;
    use std::sync::Arc;
    use std::sync::Mutex;

    use super::*;
    use crate::common::utils::{MockTime, Time};
    use crate::default_host_resolver::DefaultHostResolver;

    const NODE_ID_1: i32 = 1001;
    const NODE_ID_2: i32 = 2002;
    const NODE_ID_3: i32 = 3003;
    const HOST_TWO_IPS: &str = "multiple.ip.address";

    const RECONNECT_BACKOFF_MS: i64 = 10 * 1000;
    const RECONNECT_BACKOFF_MAX: i64 = 60 * 1000;
    const CONNECTION_SETUP_TIMEOUT_MS: i64 = 10 * 1000;
    const CONNECTION_SETUP_TIMEOUT_MAX_MS: i64 = 127 * 1000;

    fn initial_addresses() -> Vec<IpAddr> {
        vec![
            IpAddr::V4(Ipv4Addr::new(10, 200, 20, 100)),
            IpAddr::V4(Ipv4Addr::new(10, 200, 20, 101)),
            IpAddr::V4(Ipv4Addr::new(10, 200, 20, 102)),
        ]
    }

    fn new_addresses() -> Vec<IpAddr> {
        vec![
            IpAddr::V4(Ipv4Addr::new(10, 200, 20, 103)),
            IpAddr::V4(Ipv4Addr::new(10, 200, 20, 104)),
            IpAddr::V4(Ipv4Addr::new(10, 200, 20, 105)),
        ]
    }

    /// Translation of the inner-test class `AddressChangeHostResolver`.
    struct AddressChangeHostResolver {
        initial: Vec<IpAddr>,
        new: Vec<IpAddr>,
        state: Arc<Mutex<AddressChangeState>>,
    }

    struct AddressChangeState {
        use_new: bool,
        resolution_count: i32,
    }

    impl AddressChangeHostResolver {
        fn new(initial: Vec<IpAddr>, new: Vec<IpAddr>) -> Self {
            AddressChangeHostResolver {
                initial,
                new,
                state: Arc::new(Mutex::new(AddressChangeState { use_new: false, resolution_count: 0 })),
            }
        }

        fn handle(&self) -> Arc<Mutex<AddressChangeState>> {
            Arc::clone(&self.state)
        }
    }

    impl HostResolver for AddressChangeHostResolver {
        fn resolve(&self, _host: &str) -> Result<Vec<IpAddr>, KafkaError> {
            let mut state = self.state.lock().expect("address-change state");
            state.resolution_count += 1;
            Ok(if state.use_new {
                self.new.clone()
            } else {
                self.initial.clone()
            })
        }
    }

    fn new_states_with_resolver(resolver: Box<dyn HostResolver>) -> ClusterConnectionStates {
        ClusterConnectionStates::new(
            RECONNECT_BACKOFF_MS,
            RECONNECT_BACKOFF_MAX,
            CONNECTION_SETUP_TIMEOUT_MS,
            CONNECTION_SETUP_TIMEOUT_MAX_MS,
            LogContext::new(),
            resolver,
        )
        .expect("constructor")
    }

    fn new_states() -> ClusterConnectionStates {
        new_states_with_resolver(Box::new(DefaultHostResolver::new()))
    }

    /// Java: `testClusterConnectionStateChanges`.
    #[test]
    fn cluster_connection_state_changes() {
        let mut states = new_states();
        let time = MockTime::default();

        assert!(states.can_connect(NODE_ID_1, time.milliseconds()));
        assert_eq!(states.connection_delay(NODE_ID_1, time.milliseconds()), 0);

        // Start connecting and check state
        states.connecting(NODE_ID_1, time.milliseconds(), "localhost");
        assert_eq!(states.connection_state(NODE_ID_1), ConnectionState::Connecting);
        assert!(states.is_connecting(NODE_ID_1));
        assert!(!states.is_ready(NODE_ID_1, time.milliseconds()));
        assert!(!states.is_blacked_out(NODE_ID_1, time.milliseconds()));
        assert!(!states.has_ready_nodes(time.milliseconds()));

        let connection_delay = states.connection_delay(NODE_ID_1, time.milliseconds());
        let connection_delay_delta = (CONNECTION_SETUP_TIMEOUT_MS as f64) * CONNECTION_SETUP_TIMEOUT_JITTER;
        assert!(
            ((connection_delay - CONNECTION_SETUP_TIMEOUT_MS).abs() as f64) <= connection_delay_delta,
            "connection_delay={connection_delay}, expected≈{CONNECTION_SETUP_TIMEOUT_MS}±{connection_delay_delta}"
        );

        time.sleep(100);

        // Successful connection
        states.ready(NODE_ID_1);
        assert_eq!(states.connection_state(NODE_ID_1), ConnectionState::Ready);
        assert!(states.is_ready(NODE_ID_1, time.milliseconds()));
        assert!(states.has_ready_nodes(time.milliseconds()));
        assert!(!states.is_connecting(NODE_ID_1));
        assert!(!states.is_blacked_out(NODE_ID_1, time.milliseconds()));
        assert_eq!(states.connection_delay(NODE_ID_1, time.milliseconds()), i64::MAX);

        time.sleep(15000);

        // Disconnected from broker
        states.disconnected(NODE_ID_1, time.milliseconds());
        assert_eq!(states.connection_state(NODE_ID_1), ConnectionState::Disconnected);
        assert!(states.is_disconnected(NODE_ID_1));
        assert!(states.is_blacked_out(NODE_ID_1, time.milliseconds()));
        assert!(!states.is_connecting(NODE_ID_1));
        assert!(!states.has_ready_nodes(time.milliseconds()));
        assert!(!states.can_connect(NODE_ID_1, time.milliseconds()));

        let backoff_tolerance = (RECONNECT_BACKOFF_MS as f64) * RECONNECT_BACKOFF_JITTER;
        let current_backoff = states.connection_delay(NODE_ID_1, time.milliseconds());
        assert!(
            ((current_backoff - RECONNECT_BACKOFF_MS).abs() as f64) <= backoff_tolerance,
            "current_backoff={current_backoff}, expected≈{RECONNECT_BACKOFF_MS}±{backoff_tolerance}"
        );

        time.sleep(current_backoff + 1);
        assert!(states.can_connect(NODE_ID_1, time.milliseconds()));
    }

    /// Java: `testMultipleNodeConnectionStates`.
    #[test]
    fn multiple_node_connection_states() {
        let mut states = new_states();
        let time = MockTime::default();

        assert!(states.can_connect(NODE_ID_1, time.milliseconds()));
        assert!(states.can_connect(NODE_ID_2, time.milliseconds()));
        assert!(!states.has_ready_nodes(time.milliseconds()));

        states.connecting(NODE_ID_2, time.milliseconds(), "localhost");
        assert!(!states.has_ready_nodes(time.milliseconds()));
        time.sleep(1000);
        states.ready(NODE_ID_2);
        assert!(states.has_ready_nodes(time.milliseconds()));

        states.connecting(NODE_ID_1, time.milliseconds(), "localhost");
        assert!(states.has_ready_nodes(time.milliseconds()));
        time.sleep(1000);
        states.ready(NODE_ID_1);
        assert!(states.has_ready_nodes(time.milliseconds()));

        time.sleep(12000);

        states.disconnected(NODE_ID_2, time.milliseconds());
        assert!(states.has_ready_nodes(time.milliseconds()));
        assert!(states.is_blacked_out(NODE_ID_2, time.milliseconds()));
        assert!(!states.is_blacked_out(NODE_ID_1, time.milliseconds()));
        time.sleep(states.connection_delay(NODE_ID_2, time.milliseconds()));
        states.disconnected(NODE_ID_1, time.milliseconds() + 1);
        assert!(states.is_blacked_out(NODE_ID_1, time.milliseconds()));
        assert!(!states.is_blacked_out(NODE_ID_2, time.milliseconds()));
        assert!(!states.has_ready_nodes(time.milliseconds()));
    }

    /// Java: `testAuthorizationFailed`.
    #[test]
    fn authorization_failed() {
        let mut states = new_states();
        let time = MockTime::default();
        states.connecting(NODE_ID_1, time.milliseconds(), "localhost");
        time.sleep(100);

        states.authentication_failed(
            NODE_ID_1,
            time.milliseconds(),
            KafkaError::Authentication("No path to CA for certificate!".into()),
        );
        time.sleep(1000);
        assert_eq!(states.connection_state(NODE_ID_1), ConnectionState::AuthenticationFailed);
        assert!(states.authentication_error(NODE_ID_1).is_some());
        assert!(!states.has_ready_nodes(time.milliseconds()));
        assert!(!states.can_connect(NODE_ID_1, time.milliseconds()));

        time.sleep(states.connection_delay(NODE_ID_1, time.milliseconds()) + 1);

        assert!(states.can_connect(NODE_ID_1, time.milliseconds()));
        states.ready(NODE_ID_1);
        assert!(states.authentication_error(NODE_ID_1).is_none());
    }

    /// Java: `testRemoveNode`.
    #[test]
    fn remove_node() {
        let mut states = new_states();
        let time = MockTime::default();
        states.connecting(NODE_ID_1, time.milliseconds(), "localhost");
        time.sleep(1000);
        states.ready(NODE_ID_1);
        time.sleep(10000);
        states.disconnected(NODE_ID_1, time.milliseconds());
        states.remove(NODE_ID_1);
        assert!(states.can_connect(NODE_ID_1, time.milliseconds()));
        assert!(!states.is_blacked_out(NODE_ID_1, time.milliseconds()));
        assert_eq!(states.connection_delay(NODE_ID_1, time.milliseconds()), 0);
    }

    /// Java: `testMaxReconnectBackoff`.
    #[test]
    fn max_reconnect_backoff() {
        let mut states = new_states();
        let time = MockTime::default();
        let effective_max = ((RECONNECT_BACKOFF_MAX as f64) * (1.0 + RECONNECT_BACKOFF_JITTER)).round() as i64;

        states.connecting(NODE_ID_1, time.milliseconds(), "localhost");
        time.sleep(1000);
        states.disconnected(NODE_ID_1, time.milliseconds());

        for _ in 0..100 {
            let reconnect_backoff = states.connection_delay(NODE_ID_1, time.milliseconds());
            assert!(reconnect_backoff <= effective_max);
            assert!(!states.can_connect(NODE_ID_1, time.milliseconds()));
            time.sleep(reconnect_backoff + 1);
            assert!(states.can_connect(NODE_ID_1, time.milliseconds()));
            states.connecting(NODE_ID_1, time.milliseconds(), "localhost");
            time.sleep(10);
            states.disconnected(NODE_ID_1, time.milliseconds());
        }
    }

    /// Java: `testExponentialReconnectBackoff`.
    #[test]
    fn exponential_reconnect_backoff() {
        verify_reconnect_exponential_backoff(false);
        verify_reconnect_exponential_backoff(true);
    }

    fn verify_reconnect_exponential_backoff(enter_checking_api_version_state: bool) {
        let mut states = new_states();
        let time = MockTime::default();
        let reconnect_backoff_max_exp = (RECONNECT_BACKOFF_MAX as f64 / RECONNECT_BACKOFF_MS.max(1) as f64).ln()
            / (RECONNECT_BACKOFF_EXP_BASE as f64).ln();

        states.remove(NODE_ID_1);
        for i in 0..10 {
            states.connecting(NODE_ID_1, time.milliseconds(), "localhost");
            if enter_checking_api_version_state {
                states.checking_api_versions(NODE_ID_1);
            }
            states.disconnected(NODE_ID_1, time.milliseconds());

            let exp = (i as f64).min(reconnect_backoff_max_exp);
            let expected_backoff =
                ((RECONNECT_BACKOFF_EXP_BASE as f64).powf(exp) * RECONNECT_BACKOFF_MS as f64).round() as i64;
            let current_backoff = states.connection_delay(NODE_ID_1, time.milliseconds());
            let tol = RECONNECT_BACKOFF_JITTER * expected_backoff as f64;
            assert!(
                ((current_backoff - expected_backoff).abs() as f64) <= tol,
                "i={i}, current={current_backoff}, expected={expected_backoff}, tol={tol}"
            );
            time.sleep(states.connection_delay(NODE_ID_1, time.milliseconds()) + 1);
        }
    }

    /// Java: `testThrottled`.
    #[test]
    fn throttled() {
        let mut states = new_states();
        let time = MockTime::default();
        states.connecting(NODE_ID_1, time.milliseconds(), "localhost");
        time.sleep(1000);
        states.ready(NODE_ID_1);
        time.sleep(10000);

        assert_eq!(states.throttle_delay_ms(NODE_ID_1, time.milliseconds()), 0);

        states.throttle(NODE_ID_1, time.milliseconds() + 100);
        assert_eq!(states.throttle_delay_ms(NODE_ID_1, time.milliseconds()), 100);

        time.sleep(50);
        assert_eq!(states.throttle_delay_ms(NODE_ID_1, time.milliseconds()), 50);
        assert_eq!(states.poll_delay_ms(NODE_ID_1, time.milliseconds()), 50);

        time.sleep(50);
        assert_eq!(states.throttle_delay_ms(NODE_ID_1, time.milliseconds()), 0);
        assert_eq!(
            states.poll_delay_ms(NODE_ID_1, time.milliseconds()),
            states.connection_delay(NODE_ID_1, time.milliseconds())
        );
    }

    /// Java: `testSingleIP`. Mirrors the inline lambda host resolver.
    #[test]
    fn single_ip() {
        struct LocalhostResolver;
        impl HostResolver for LocalhostResolver {
            fn resolve(&self, host: &str) -> Result<Vec<IpAddr>, KafkaError> {
                assert_eq!(host, "localhost");
                Ok(vec![IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1))])
            }
        }
        let mut states = new_states_with_resolver(Box::new(LocalhostResolver));
        let time = MockTime::default();
        states.connecting(NODE_ID_1, time.milliseconds(), "localhost");
        let curr = states.current_address(NODE_ID_1).expect("resolves");
        states.connecting(NODE_ID_1, time.milliseconds(), "localhost");
        assert_eq!(curr, states.current_address(NODE_ID_1).expect("resolves"));
    }

    /// Java: `testMultipleIPs`.
    #[test]
    fn multiple_ips() {
        let resolver = AddressChangeHostResolver::new(initial_addresses(), new_addresses());
        let mut states = new_states_with_resolver(Box::new(resolver));
        let time = MockTime::default();

        states.connecting(NODE_ID_1, time.milliseconds(), HOST_TWO_IPS);
        let addr1 = states.current_address(NODE_ID_1).expect("resolves");
        states.connecting(NODE_ID_1, time.milliseconds(), HOST_TWO_IPS);
        let addr2 = states.current_address(NODE_ID_1).expect("resolves");
        assert_ne!(addr1, addr2);
        states.connecting(NODE_ID_1, time.milliseconds(), HOST_TWO_IPS);
        let addr3 = states.current_address(NODE_ID_1).expect("resolves");
        assert_ne!(addr1, addr3);
    }

    /// Java: `testHostResolveChange`.
    #[test]
    fn host_resolve_change() {
        let resolver = AddressChangeHostResolver::new(initial_addresses(), new_addresses());
        let handle = resolver.handle();
        let mut states = new_states_with_resolver(Box::new(resolver));
        let time = MockTime::default();

        states.connecting(NODE_ID_1, time.milliseconds(), HOST_TWO_IPS);
        let addr1 = states.current_address(NODE_ID_1).expect("resolves");

        handle.lock().expect("addr-change").use_new = true;
        // Java's test calls `connecting(..., "localhost")` but the
        // `AddressChangeHostResolver` ignores the host parameter — it
        // always returns its current `useNewAddresses` set. The
        // assertion is that switching `useNewAddresses` mid-flight
        // produces a different IP.
        states.connecting(NODE_ID_1, time.milliseconds(), "localhost");
        let addr2 = states.current_address(NODE_ID_1).expect("resolves");
        assert_ne!(addr1, addr2);
    }

    /// Java: `testNodeWithNewHostname`.
    #[test]
    fn node_with_new_hostname() {
        let resolver = AddressChangeHostResolver::new(initial_addresses(), new_addresses());
        let handle = resolver.handle();
        let mut states = new_states_with_resolver(Box::new(resolver));
        let time = MockTime::default();

        states.connecting(NODE_ID_1, time.milliseconds(), "localhost");
        let addr1 = states.current_address(NODE_ID_1).expect("resolves");

        handle.lock().expect("addr-change").use_new = true;
        states.connecting(NODE_ID_1, time.milliseconds(), HOST_TWO_IPS);
        let addr2 = states.current_address(NODE_ID_1).expect("resolves");
        assert_ne!(addr1, addr2);
    }

    /// Java: `testIsPreparingConnection`.
    #[test]
    fn is_preparing_connection() {
        let mut states = new_states();
        let time = MockTime::default();

        assert!(!states.is_preparing_connection(NODE_ID_1));
        states.connecting(NODE_ID_1, time.milliseconds(), "localhost");
        assert!(states.is_preparing_connection(NODE_ID_1));
        states.checking_api_versions(NODE_ID_1);
        assert!(states.is_preparing_connection(NODE_ID_1));
        states.disconnected(NODE_ID_1, time.milliseconds());
        assert!(!states.is_preparing_connection(NODE_ID_1));
    }

    /// Java: `testExponentialConnectionSetupTimeout`.
    #[test]
    fn exponential_connection_setup_timeout() {
        let mut states = new_states();
        let time = MockTime::default();

        assert!(states.can_connect(NODE_ID_1, time.milliseconds()));

        let max_n = ((CONNECTION_SETUP_TIMEOUT_MAX_MS as f64 / CONNECTION_SETUP_TIMEOUT_MS as f64).ln()
            / (CONNECTION_SETUP_TIMEOUT_EXP_BASE as f64).ln())
        .floor() as i32;

        for n in 0..=max_n {
            states.connecting(NODE_ID_1, time.milliseconds(), "localhost");
            assert!(states.connecting_nodes().contains(&NODE_ID_1));
            let expected = CONNECTION_SETUP_TIMEOUT_MS as f64 * (CONNECTION_SETUP_TIMEOUT_EXP_BASE as f64).powi(n);
            let tol = expected * CONNECTION_SETUP_TIMEOUT_JITTER;
            let actual = states.connection_setup_timeout_ms(NODE_ID_1) as f64;
            assert!(
                (actual - expected).abs() <= tol,
                "n={n}, actual={actual}, expected={expected}, tol={tol}"
            );
            states.disconnected(NODE_ID_1, time.milliseconds());
            assert!(!states.connecting_nodes().contains(&NODE_ID_1));
        }

        // Upper bound
        states.connecting(NODE_ID_1, time.milliseconds(), "localhost");
        let actual = states.connection_setup_timeout_ms(NODE_ID_1) as f64;
        let tol = CONNECTION_SETUP_TIMEOUT_MAX_MS as f64 * CONNECTION_SETUP_TIMEOUT_JITTER;
        assert!((actual - CONNECTION_SETUP_TIMEOUT_MAX_MS as f64).abs() <= tol);
        assert!(states.connecting_nodes().contains(&NODE_ID_1));

        states.ready(NODE_ID_1);
        let actual = states.connection_setup_timeout_ms(NODE_ID_1) as f64;
        let tol = CONNECTION_SETUP_TIMEOUT_MS as f64 * CONNECTION_SETUP_TIMEOUT_JITTER;
        assert!((actual - CONNECTION_SETUP_TIMEOUT_MS as f64).abs() <= tol);
        assert!(!states.connecting_nodes().contains(&NODE_ID_1));
        states.disconnected(NODE_ID_1, time.milliseconds());

        // ready→disconnected must not raise the timeout
        states.connecting(NODE_ID_1, time.milliseconds(), "localhost");
        let actual = states.connection_setup_timeout_ms(NODE_ID_1) as f64;
        let tol = CONNECTION_SETUP_TIMEOUT_MS as f64 * CONNECTION_SETUP_TIMEOUT_JITTER;
        assert!((actual - CONNECTION_SETUP_TIMEOUT_MS as f64).abs() <= tol);
        assert!(states.connecting_nodes().contains(&NODE_ID_1));
    }

    /// Java: `testTimedOutConnections`.
    #[test]
    fn timed_out_connections() {
        let mut states = new_states();
        let time = MockTime::default();

        states.connecting(NODE_ID_1, time.milliseconds(), "localhost");
        states.connecting(NODE_ID_2, time.milliseconds(), "localhost");

        assert_eq!(states.nodes_with_connection_setup_timeout(time.milliseconds()).len(), 0);

        time.sleep(CONNECTION_SETUP_TIMEOUT_MS / 2);
        states.connecting(NODE_ID_3, time.milliseconds(), "localhost");
        time.sleep(
            CONNECTION_SETUP_TIMEOUT_MS / 2
                + (CONNECTION_SETUP_TIMEOUT_MS as f64 * CONNECTION_SETUP_TIMEOUT_JITTER) as i64,
        );

        let timed_out = states.nodes_with_connection_setup_timeout(time.milliseconds());
        assert_eq!(timed_out.len(), 2);
        assert!(timed_out.contains(&NODE_ID_1));
        assert!(timed_out.contains(&NODE_ID_2));

        states.disconnected(NODE_ID_1, time.milliseconds());
        states.disconnected(NODE_ID_2, time.milliseconds());

        time.sleep(
            CONNECTION_SETUP_TIMEOUT_MS / 2
                + (CONNECTION_SETUP_TIMEOUT_MS as f64 * CONNECTION_SETUP_TIMEOUT_JITTER) as i64,
        );

        let timed_out = states.nodes_with_connection_setup_timeout(time.milliseconds());
        assert_eq!(timed_out.len(), 1);
        assert!(timed_out.contains(&NODE_ID_3));

        states.disconnected(NODE_ID_3, time.milliseconds());
        assert_eq!(states.nodes_with_connection_setup_timeout(time.milliseconds()).len(), 0);
    }

    /// Java: `testSkipLastAttemptedIp`.
    #[test]
    fn skip_last_attempted_ip() {
        let resolver = AddressChangeHostResolver::new(initial_addresses(), new_addresses());
        let mut states = new_states_with_resolver(Box::new(resolver));
        let time = MockTime::default();

        states.connecting(NODE_ID_1, time.milliseconds(), HOST_TWO_IPS);
        let addr1 = states.current_address(NODE_ID_1).expect("resolves");

        // Disconnect, which will trigger re-resolution with the first IP still first
        states.disconnected(NODE_ID_1, time.milliseconds());

        // Connect again, the first IP should get skipped
        states.connecting(NODE_ID_1, time.milliseconds(), HOST_TWO_IPS);
        let addr2 = states.current_address(NODE_ID_1).expect("resolves");
        assert_ne!(addr1, addr2);
    }
}
