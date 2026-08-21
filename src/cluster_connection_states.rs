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

//! The state of our connection to each node in the cluster.
//!
//! Translated from `org.apache.kafka.clients.ClusterConnectionStates`.

use std::collections::HashSet;
use std::fmt;
use std::io;
use std::net::IpAddr;

use rustc_hash::FxHashMap;

use super::ConnectionState;
use super::HostResolver;
use super::client_utils;
use crate::common::Error;
use crate::common::utils::ExponentialBackoff;
use crate::common::utils::LogContext;
use crate::kafka_info;

/// Exponential base for reconnect backoff.
pub const CLUSTER_CONNECTION_STATES_RECONNECT_BACKOFF_EXP_BASE: i32 = 2;
/// Jitter factor for reconnect backoff.
pub const CLUSTER_CONNECTION_STATES_RECONNECT_BACKOFF_JITTER: f64 = 0.2;
/// Exponential base for connection setup timeout.
pub const CLUSTER_CONNECTION_STATES_CONNECTION_SETUP_TIMEOUT_EXP_BASE: i32 = 2;
/// Jitter factor for connection setup timeout.
pub const CLUSTER_CONNECTION_STATES_CONNECTION_SETUP_TIMEOUT_JITTER: f64 = 0.2;

/// The state of our connection to each node in the cluster.
///
/// Tracks connection states, backoff timers, throttling, and DNS resolution
/// for all nodes. The type parameter `H` is the host resolver implementation.
///
/// Translated from `org.apache.kafka.clients.ClusterConnectionStates`.
pub struct ClusterConnectionStates<H: HostResolver> {
    /// Keyed by node-id string; queried multiple times per network poll per
    /// node (`is_ready` / `can_connect` / `state`). `FxHashMap` (Phase 27):
    /// Java's `String` caches its hashCode so its lookups don't rehash;
    /// Rust's default SipHash rehashes the full key per lookup. Private —
    /// never exposed. (`connecting_nodes` below IS exposed by a public
    /// accessor, so it stays on the std hasher per the boundary rule.)
    node_state: FxHashMap<String, NodeConnectionState>,
    connecting_nodes: HashSet<String>,
    reconnect_backoff: ExponentialBackoff,
    connection_setup_timeout: ExponentialBackoff,
    host_resolver: H,
    log_context: LogContext,
}

impl<H: HostResolver> ClusterConnectionStates<H> {
    /// Creates a new `ClusterConnectionStates`.
    ///
    /// # Arguments
    /// * `reconnect_backoff_ms` - Initial reconnect backoff in milliseconds.
    /// * `reconnect_backoff_max_ms` - Maximum reconnect backoff in milliseconds.
    /// * `connection_setup_timeout_ms` - Initial connection setup timeout in milliseconds.
    /// * `connection_setup_timeout_max_ms` - Maximum connection setup timeout in milliseconds.
    /// * `host_resolver` - The host resolver to use for DNS resolution.
    pub fn new(
        reconnect_backoff_ms: i64,
        reconnect_backoff_max_ms: i64,
        connection_setup_timeout_ms: i64,
        connection_setup_timeout_max_ms: i64,
        log_context: LogContext,
        host_resolver: H,
    ) -> Self {
        Self {
            reconnect_backoff: ExponentialBackoff::new(
                reconnect_backoff_ms,
                CLUSTER_CONNECTION_STATES_RECONNECT_BACKOFF_EXP_BASE,
                reconnect_backoff_max_ms,
                CLUSTER_CONNECTION_STATES_RECONNECT_BACKOFF_JITTER,
            )
            .expect("Invalid reconnect backoff jitter"),
            connection_setup_timeout: ExponentialBackoff::new(
                connection_setup_timeout_ms,
                CLUSTER_CONNECTION_STATES_CONNECTION_SETUP_TIMEOUT_EXP_BASE,
                connection_setup_timeout_max_ms,
                CLUSTER_CONNECTION_STATES_CONNECTION_SETUP_TIMEOUT_JITTER,
            )
            .expect("Invalid connection setup timeout jitter"),
            node_state: FxHashMap::default(),
            connecting_nodes: HashSet::new(),
            host_resolver,
            log_context,
        }
    }

    /// Return true if we can currently initiate a new connection. This will be the case if we are
    /// not connected and haven't been connected for at least the minimum reconnection backoff
    /// period.
    pub fn can_connect(&self, id: &str, now: i64) -> bool {
        match self.node_state.get(id) {
            None => true,
            Some(state) => {
                state.state.is_disconnected() && now - state.last_connect_attempt_ms >= state.reconnect_backoff_ms
            },
        }
    }

    /// Return true if we are disconnected from the given node and can't re-establish a connection
    /// yet.
    pub fn is_blacked_out(&self, id: &str, now: i64) -> bool {
        match self.node_state.get(id) {
            None => false,
            Some(state) => {
                state.state.is_disconnected() && now - state.last_connect_attempt_ms < state.reconnect_backoff_ms
            },
        }
    }

    /// Returns the number of milliseconds to wait, based on the connection state, before
    /// attempting to send data. When disconnected, this respects the reconnect backoff time.
    /// When connecting, return a delay based on the connection timeout. When connected, wait
    /// indefinitely (i.e. until a wakeup).
    pub fn connection_delay(&self, id: &str, now: i64) -> i64 {
        match self.node_state.get(id) {
            None => 0,
            Some(state) => {
                if state.state == ConnectionState::Connecting {
                    self.connection_setup_timeout_ms(id)
                } else if state.state.is_disconnected() {
                    let time_waited = now - state.last_connect_attempt_ms;
                    (state.reconnect_backoff_ms - time_waited).max(0)
                } else {
                    // When connected, we should be able to delay indefinitely since other events
                    // (connection or data acked) will cause a wakeup once data can be sent.
                    i64::MAX
                }
            },
        }
    }

    /// Return true if a specific connection establishment is currently underway.
    pub fn is_connecting(&self, id: &str) -> bool {
        self.node_state
            .get(id)
            .is_some_and(|state| state.state == ConnectionState::Connecting)
    }

    /// Check whether a connection is either being established or awaiting API version information.
    pub fn is_preparing_connection(&self, id: &str) -> bool {
        self.node_state.get(id).is_some_and(|state| {
            state.state == ConnectionState::Connecting || state.state == ConnectionState::CheckingApiVersions
        })
    }

    /// Enter the connecting state for the given connection, moving to a new resolved address if
    /// necessary.
    pub fn connecting(&mut self, id: &str, now: i64, host: &str) {
        if let Some(connection_state) = self.node_state.get_mut(id) {
            if connection_state.host() == host {
                connection_state.last_connect_attempt_ms = now;
                connection_state.state = ConnectionState::Connecting;
                // Move to next resolved address, or if addresses are exhausted, mark node to be
                // re-resolved
                connection_state.move_to_next_address();
                self.connecting_nodes.insert(id.to_string());
                return;
            }
            kafka_info!(
                self.log_context,
                "Hostname for node {} changed from {} to {}.",
                id,
                connection_state.host(),
                host
            );
        }

        // Create a new NodeConnectionState if node_state does not already contain one
        // for the specified id or if the hostname associated with the node id changed.
        self.node_state.insert(
            id.to_string(),
            NodeConnectionState::new(
                ConnectionState::Connecting,
                now,
                self.reconnect_backoff.backoff(0),
                self.connection_setup_timeout.backoff(0),
                host.to_string(),
            ),
        );
        self.connecting_nodes.insert(id.to_string());
    }

    /// Returns a resolved address for the given connection, resolving it if necessary.
    ///
    /// # Errors
    /// Returns an `io::Error` if the address cannot be resolved.
    pub async fn current_address(&mut self, id: &str) -> io::Result<IpAddr> {
        let state = self.node_state(id)?;
        if state.addresses.is_empty() {
            let host = state.host.clone();
            let addresses = client_utils::resolve(&host, &self.host_resolver).await?;
            let state = self.node_state(id)?;
            state.resolve_addresses_with(addresses);
        }
        let state = self.node_state(id)?;
        Ok(state.current_address())
    }

    /// Enter the disconnected state for the given node.
    pub fn disconnected(&mut self, id: &str, now: i64) {
        let node_state = self
            .node_state
            .get_mut(id)
            .unwrap_or_else(|| panic!("No entry found for connection {}", id));
        node_state.last_connect_attempt_ms = now;
        Self::update_reconnect_backoff_inner(&self.reconnect_backoff, node_state);
        if node_state.state == ConnectionState::Connecting {
            Self::update_connection_setup_timeout_inner(&self.connection_setup_timeout, node_state);
            self.connecting_nodes.remove(id);
        } else {
            Self::reset_connection_setup_timeout_inner(&self.connection_setup_timeout, node_state);
            if node_state.state.is_connected() {
                // If a connection had previously been established, clear the addresses to trigger
                // a new DNS resolution because the node IPs may have changed
                node_state.clear_addresses();
            }
        }
        node_state.state = ConnectionState::Disconnected;
    }

    /// Indicate that the connection is throttled until the specified deadline.
    pub fn throttle(&mut self, id: &str, throttle_until_time_ms: i64) {
        if let Some(state) = self.node_state.get_mut(id) {
            // The throttle deadline should never regress.
            if state.throttle_until_time_ms < throttle_until_time_ms {
                state.throttle_until_time_ms = throttle_until_time_ms;
            }
        }
    }

    /// Return the remaining throttling delay in milliseconds if throttling is in progress.
    /// Return 0 otherwise.
    pub fn throttle_delay_ms(&self, id: &str, now: i64) -> i64 {
        match self.node_state.get(id) {
            Some(state) if state.throttle_until_time_ms > now => state.throttle_until_time_ms - now,
            _ => 0,
        }
    }

    /// Return the number of milliseconds to wait, based on the connection state and the throttle
    /// time, before attempting to send data. If the connection has been established but being
    /// throttled, return throttle delay. Otherwise, return connection delay.
    pub fn poll_delay_ms(&self, id: &str, now: i64) -> i64 {
        let throttle_delay_ms = self.throttle_delay_ms(id, now);
        if self.is_connected(id) && throttle_delay_ms > 0 {
            throttle_delay_ms
        } else {
            self.connection_delay(id, now)
        }
    }

    /// Enter the checking_api_versions state for the given node.
    pub fn checking_api_versions(&mut self, id: &str) {
        let node_state = self
            .node_state
            .get_mut(id)
            .unwrap_or_else(|| panic!("No entry found for connection {}", id));
        node_state.state = ConnectionState::CheckingApiVersions;
        Self::reset_connection_setup_timeout_inner(&self.connection_setup_timeout, node_state);
        self.connecting_nodes.remove(id);
    }

    /// Enter the ready state for the given node.
    pub fn ready(&mut self, id: &str) {
        let node_state = self
            .node_state
            .get_mut(id)
            .unwrap_or_else(|| panic!("No entry found for connection {}", id));
        node_state.state = ConnectionState::Ready;
        node_state.authentication_error = None;
        Self::reset_reconnect_backoff_inner(&self.reconnect_backoff, node_state);
        Self::reset_connection_setup_timeout_inner(&self.connection_setup_timeout, node_state);
        self.connecting_nodes.remove(id);
    }

    /// Enter the authentication failed state for the given node.
    ///
    /// `error` is Java's `AuthenticationException exception` parameter
    /// (`ClusterConnectionStates.java:272`) — the object the channel raised, so
    /// the concrete subclass (`SaslAuthenticationException`,
    /// `SslAuthenticationException`, or the base class) reaches
    /// [`authentication_error`](Self::authentication_error) intact. It used to be
    /// a `String`, which flattened every failure to the base class one hop past
    /// [`ChannelState`](crate::common::network::ChannelState) and made an SSL
    /// certificate rejection indistinguishable from a rejected SASL credential.
    pub fn authentication_failed(&mut self, id: &str, now: i64, error: Error) {
        let node_state = self
            .node_state
            .get_mut(id)
            .unwrap_or_else(|| panic!("No entry found for connection {}", id));
        node_state.authentication_error = Some(error);
        node_state.state = ConnectionState::AuthenticationFailed;
        node_state.last_connect_attempt_ms = now;
        Self::update_reconnect_backoff_inner(&self.reconnect_backoff, node_state);
    }

    /// Return true if the connection is in the READY state and currently not throttled.
    pub fn is_ready(&self, id: &str, now: i64) -> bool {
        Self::is_ready_state(self.node_state.get(id), now)
    }

    fn is_ready_state(state: Option<&NodeConnectionState>, now: i64) -> bool {
        state.is_some_and(|s| s.state == ConnectionState::Ready && s.throttle_until_time_ms <= now)
    }

    /// Return true if there is at least one node with connection in the READY state and not
    /// throttled.
    pub fn has_ready_nodes(&self, now: i64) -> bool {
        self.node_state.values().any(|s| Self::is_ready_state(Some(s), now))
    }

    /// Return true if the connection has been established.
    pub fn is_connected(&self, id: &str) -> bool {
        self.node_state.get(id).is_some_and(|s| s.state.is_connected())
    }

    /// Return true if the connection has been disconnected.
    pub fn is_disconnected(&self, id: &str) -> bool {
        self.node_state.get(id).is_some_and(|s| s.state.is_disconnected())
    }

    /// Return the authentication error if an authentication error occurred.
    ///
    /// Java's `authenticationException(String id)`
    /// (`ClusterConnectionStates.java:331`), which returns the
    /// `AuthenticationException` object — hence the whole [`Error`], not its
    /// message: every caller (`NetworkClient.authenticationException` and from
    /// there the admin, consumer and producer) needs the class, not just the
    /// text.
    pub fn authentication_error(&self, id: &str) -> Option<&Error> {
        self.node_state.get(id).and_then(|s| s.authentication_error.as_ref())
    }

    /// Get the state of a given connection.
    ///
    /// # Panics
    /// Panics if no entry exists for the given connection id.
    pub fn connection_state(&self, id: &str) -> ConnectionState {
        self.node_state
            .get(id)
            .unwrap_or_else(|| panic!("No entry found for connection {}", id))
            .state
    }

    /// Get the id set of nodes which are in CONNECTING state.
    ///
    /// Visible for testing.
    pub fn connecting_nodes(&self) -> &HashSet<String> {
        &self.connecting_nodes
    }

    /// Get the timestamp of the latest connection attempt of a given node.
    pub fn last_connect_attempt_ms(&self, id: &str) -> i64 {
        self.node_state.get(id).map_or(0, |s| s.last_connect_attempt_ms)
    }

    /// Get the current socket connection setup timeout of the given node.
    ///
    /// # Panics
    /// Panics if no entry exists for the given connection id.
    pub fn connection_setup_timeout_ms(&self, id: &str) -> i64 {
        self.node_state
            .get(id)
            .unwrap_or_else(|| panic!("No entry found for connection {}", id))
            .connection_setup_timeout_ms
    }

    /// Test if the connection to the given node has reached its timeout.
    ///
    /// # Panics
    /// Panics if no entry exists for the given connection id, or if the node
    /// is not in the connecting state.
    pub fn is_connection_setup_timeout(&self, id: &str, now: i64) -> bool {
        let node_state = self
            .node_state
            .get(id)
            .unwrap_or_else(|| panic!("No entry found for connection {}", id));
        assert!(
            node_state.state == ConnectionState::Connecting,
            "Node {} is not in connecting state",
            id
        );
        now - self.last_connect_attempt_ms(id) > self.connection_setup_timeout_ms(id)
    }

    /// Return the list of nodes whose connection setup has timed out.
    pub fn nodes_with_connection_setup_timeout(&self, now: i64) -> Vec<String> {
        self.connecting_nodes
            .iter()
            .filter(|id| self.is_connection_setup_timeout(id, now))
            .cloned()
            .collect()
    }

    /// Remove the given node from the tracked connection states.
    ///
    /// The main difference between this and `disconnected` is the impact on `connection_delay`:
    /// it will be 0 after this call whereas `reconnect_backoff_ms` will be taken into account
    /// after `disconnected` is called.
    pub fn remove(&mut self, id: &str) {
        self.node_state.remove(id);
        self.connecting_nodes.remove(id);
    }

    // --- Private helpers ---

    /// Gets a mutable reference to the node state for the given id.
    fn node_state(&mut self, id: &str) -> io::Result<&mut NodeConnectionState> {
        self.node_state
            .get_mut(id)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, format!("No entry found for connection {}", id)))
    }

    /// Resets the failure count for a node and sets the reconnect backoff to the base value.
    fn reset_reconnect_backoff_inner(reconnect_backoff: &ExponentialBackoff, node_state: &mut NodeConnectionState) {
        node_state.failed_attempts = 0;
        node_state.reconnect_backoff_ms = reconnect_backoff.backoff(0);
    }

    /// Resets the failure count for a node and sets the connection setup timeout to the base
    /// value.
    fn reset_connection_setup_timeout_inner(
        connection_setup_timeout: &ExponentialBackoff,
        node_state: &mut NodeConnectionState,
    ) {
        node_state.failed_connect_attempts = 0;
        node_state.connection_setup_timeout_ms = connection_setup_timeout.backoff(0);
    }

    /// Increment the failure counter, update the node reconnect backoff exponentially.
    fn update_reconnect_backoff_inner(reconnect_backoff: &ExponentialBackoff, node_state: &mut NodeConnectionState) {
        node_state.reconnect_backoff_ms = reconnect_backoff.backoff(node_state.failed_attempts);
        node_state.failed_attempts += 1;
    }

    /// Increment the failure counter and update the node connection setup timeout exponentially.
    fn update_connection_setup_timeout_inner(
        connection_setup_timeout: &ExponentialBackoff,
        node_state: &mut NodeConnectionState,
    ) {
        node_state.failed_connect_attempts += 1;
        node_state.connection_setup_timeout_ms = connection_setup_timeout.backoff(node_state.failed_connect_attempts);
    }
}

/// The state of our connection to a node.
struct NodeConnectionState {
    host: String,
    state: ConnectionState,
    authentication_error: Option<Error>,
    last_connect_attempt_ms: i64,
    failed_attempts: i64,
    failed_connect_attempts: i64,
    reconnect_backoff_ms: i64,
    connection_setup_timeout_ms: i64,
    /// Connection is being throttled if current time < throttle_until_time_ms.
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
        Self {
            host,
            state,
            authentication_error: None,
            last_connect_attempt_ms,
            failed_attempts: 0,
            failed_connect_attempts: 0,
            reconnect_backoff_ms,
            connection_setup_timeout_ms,
            throttle_until_time_ms: 0,
            addresses: Vec::new(),
            address_index: -1,
            last_attempted_address: None,
        }
    }

    fn host(&self) -> &str {
        &self.host
    }

    /// Returns the current selected IP address for this node.
    ///
    /// The caller must ensure that addresses have been resolved before calling this method.
    fn current_address(&mut self) -> IpAddr {
        let current = self.addresses[self.address_index as usize];
        self.last_attempted_address = Some(current);
        current
    }

    /// Jumps to the next available resolved address for this node. If no other addresses are
    /// available, marks the list to be refreshed on the next `current_address()` call.
    fn move_to_next_address(&mut self) {
        if self.addresses.is_empty() {
            return; // Avoid div0. List will initialize on next current_address() call
        }

        self.address_index = (self.address_index + 1) % self.addresses.len() as i32;
        if self.address_index == 0 {
            self.clear_addresses(); // Exhausted list. Re-resolve on next current_address() call
        }
    }

    /// Sets the resolved addresses from the given list.
    fn resolve_addresses_with(&mut self, addresses: Vec<IpAddr>) {
        self.addresses = addresses;
        self.address_index = 0;

        // We re-resolve DNS after disconnecting, but we don't want to immediately reconnect to
        // the address we just disconnected from, in case we disconnected due to a problem with
        // that IP (such as a load balancer instance failure). Check the first address in the
        // list and skip it if it was the last address we tried and there are multiple addresses
        // to choose from.
        if self.addresses.len() > 1 && Some(self.addresses[self.address_index as usize]) == self.last_attempted_address
        {
            self.address_index += 1;
        }
    }

    /// Clears the resolved addresses in order to trigger re-resolving on the next
    /// `current_address()` call.
    fn clear_addresses(&mut self) {
        self.addresses.clear();
    }
}

impl fmt::Display for NodeConnectionState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "NodeConnectionState(state={:?}, lastConnectAttemptMs={}, failedAttempts={}, \
             failedConnectAttempts={}, throttleUntilTimeMs={})",
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
    use super::*;
    use std::net::Ipv4Addr;
    use std::sync::atomic::{AtomicBool, AtomicI32, Ordering};

    // --- Mock time ---

    struct MockTime {
        current_time_ms: i64,
    }

    impl MockTime {
        fn new() -> Self {
            Self { current_time_ms: 0 }
        }

        fn milliseconds(&self) -> i64 {
            self.current_time_ms
        }

        fn sleep(&mut self, ms: i64) {
            self.current_time_ms += ms;
        }
    }

    // --- Test HostResolver implementations ---

    /// A simple host resolver that always returns loopback (for single-IP tests).
    struct SingleIpHostResolver;

    impl HostResolver for SingleIpHostResolver {
        async fn resolve(&self, _host: &str) -> io::Result<Vec<IpAddr>> {
            Ok(vec![IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1))])
        }
    }

    /// A mock host resolver for testing address changes (multiple IPs).
    ///
    /// Translated from `AddressChangeHostResolver`.
    struct AddressChangeHostResolver {
        initial_addresses: Vec<IpAddr>,
        new_addresses: Vec<IpAddr>,
        use_new_addresses: AtomicBool,
        resolution_count: AtomicI32,
    }

    impl AddressChangeHostResolver {
        fn new(initial_addresses: Vec<IpAddr>, new_addresses: Vec<IpAddr>) -> Self {
            Self {
                initial_addresses,
                new_addresses,
                use_new_addresses: AtomicBool::new(false),
                resolution_count: AtomicI32::new(0),
            }
        }

        fn change_addresses(&self) {
            self.use_new_addresses.store(true, Ordering::SeqCst);
        }
    }

    impl HostResolver for AddressChangeHostResolver {
        async fn resolve(&self, _host: &str) -> io::Result<Vec<IpAddr>> {
            self.resolution_count.fetch_add(1, Ordering::SeqCst);
            if self.use_new_addresses.load(Ordering::SeqCst) {
                Ok(self.new_addresses.clone())
            } else {
                Ok(self.initial_addresses.clone())
            }
        }
    }

    // --- Test constants ---

    const RECONNECT_BACKOFF_MS: i64 = 10 * 1000;
    const RECONNECT_BACKOFF_MAX: i64 = 60 * 1000;
    const CONNECTION_SETUP_TIMEOUT_MS: i64 = 10 * 1000;
    const CONNECTION_SETUP_TIMEOUT_MAX_MS: i64 = 127 * 1000;

    const NODE_ID1: &str = "1001";
    const NODE_ID2: &str = "2002";
    const NODE_ID3: &str = "3003";
    const HOST_TWO_IPS: &str = "multiple.ip.address";

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

    fn create_single_ip_states() -> ClusterConnectionStates<SingleIpHostResolver> {
        ClusterConnectionStates::new(
            RECONNECT_BACKOFF_MS,
            RECONNECT_BACKOFF_MAX,
            CONNECTION_SETUP_TIMEOUT_MS,
            CONNECTION_SETUP_TIMEOUT_MAX_MS,
            LogContext::empty(),
            SingleIpHostResolver,
        )
    }

    fn create_multi_ip_states() -> ClusterConnectionStates<AddressChangeHostResolver> {
        ClusterConnectionStates::new(
            RECONNECT_BACKOFF_MS,
            RECONNECT_BACKOFF_MAX,
            CONNECTION_SETUP_TIMEOUT_MS,
            CONNECTION_SETUP_TIMEOUT_MAX_MS,
            LogContext::empty(),
            AddressChangeHostResolver::new(initial_addresses(), new_addresses()),
        )
    }

    /// Translated from `ClusterConnectionStatesTest.testClusterConnectionStateChanges`
    #[test]
    fn test_cluster_connection_state_changes() {
        let mut connection_states = create_single_ip_states();
        let mut time = MockTime::new();

        assert!(connection_states.can_connect(NODE_ID1, time.milliseconds()));
        assert_eq!(0, connection_states.connection_delay(NODE_ID1, time.milliseconds()));

        // Start connecting to Node and check state
        connection_states.connecting(NODE_ID1, time.milliseconds(), "localhost");
        assert_eq!(ConnectionState::Connecting, connection_states.connection_state(NODE_ID1));
        assert!(connection_states.is_connecting(NODE_ID1));
        assert!(!connection_states.is_ready(NODE_ID1, time.milliseconds()));
        assert!(!connection_states.is_blacked_out(NODE_ID1, time.milliseconds()));
        assert!(!connection_states.has_ready_nodes(time.milliseconds()));
        let connection_delay = connection_states.connection_delay(NODE_ID1, time.milliseconds());
        let connection_delay_delta =
            CONNECTION_SETUP_TIMEOUT_MS as f64 * CLUSTER_CONNECTION_STATES_CONNECTION_SETUP_TIMEOUT_JITTER;
        assert!(
            (connection_delay as f64 - CONNECTION_SETUP_TIMEOUT_MS as f64).abs() <= connection_delay_delta,
            "Expected connectionDelay ~= {} +/- {}, got {}",
            CONNECTION_SETUP_TIMEOUT_MS,
            connection_delay_delta,
            connection_delay
        );

        time.sleep(100);

        // Successful connection
        connection_states.ready(NODE_ID1);
        assert_eq!(ConnectionState::Ready, connection_states.connection_state(NODE_ID1));
        assert!(connection_states.is_ready(NODE_ID1, time.milliseconds()));
        assert!(connection_states.has_ready_nodes(time.milliseconds()));
        assert!(!connection_states.is_connecting(NODE_ID1));
        assert!(!connection_states.is_blacked_out(NODE_ID1, time.milliseconds()));
        assert_eq!(i64::MAX, connection_states.connection_delay(NODE_ID1, time.milliseconds()));

        time.sleep(15000);

        // Disconnected from broker
        connection_states.disconnected(NODE_ID1, time.milliseconds());
        assert_eq!(ConnectionState::Disconnected, connection_states.connection_state(NODE_ID1));
        assert!(connection_states.is_disconnected(NODE_ID1));
        assert!(connection_states.is_blacked_out(NODE_ID1, time.milliseconds()));
        assert!(!connection_states.is_connecting(NODE_ID1));
        assert!(!connection_states.has_ready_nodes(time.milliseconds()));
        assert!(!connection_states.can_connect(NODE_ID1, time.milliseconds()));

        // After disconnecting we expect a backoff value equal to the reconnect.backoff.ms setting
        // (plus minus 20% jitter)
        let backoff_tolerance = RECONNECT_BACKOFF_MS as f64 * CLUSTER_CONNECTION_STATES_RECONNECT_BACKOFF_JITTER;
        let current_backoff = connection_states.connection_delay(NODE_ID1, time.milliseconds());
        assert!(
            (current_backoff as f64 - RECONNECT_BACKOFF_MS as f64).abs() <= backoff_tolerance,
            "Expected reconnectBackoff ~= {} +/- {}, got {}",
            RECONNECT_BACKOFF_MS,
            backoff_tolerance,
            current_backoff
        );

        time.sleep(current_backoff + 1);
        // after waiting for the current backoff value we should be allowed to connect again
        assert!(connection_states.can_connect(NODE_ID1, time.milliseconds()));
    }

    /// Translated from `ClusterConnectionStatesTest.testMultipleNodeConnectionStates`
    #[test]
    fn test_multiple_node_connection_states() {
        let mut connection_states = create_single_ip_states();
        let mut time = MockTime::new();

        // Check initial state, allowed to connect to all nodes, but no nodes shown as ready
        assert!(connection_states.can_connect(NODE_ID1, time.milliseconds()));
        assert!(connection_states.can_connect(NODE_ID2, time.milliseconds()));
        assert!(!connection_states.has_ready_nodes(time.milliseconds()));

        // Start connecting one node and check that the pool only shows ready nodes after
        // successful connect
        connection_states.connecting(NODE_ID2, time.milliseconds(), "localhost");
        assert!(!connection_states.has_ready_nodes(time.milliseconds()));
        time.sleep(1000);
        connection_states.ready(NODE_ID2);
        assert!(connection_states.has_ready_nodes(time.milliseconds()));

        // Connect second node and check that both are shown as ready, pool should immediately
        // show ready nodes, since node2 is already connected
        connection_states.connecting(NODE_ID1, time.milliseconds(), "localhost");
        assert!(connection_states.has_ready_nodes(time.milliseconds()));
        time.sleep(1000);
        connection_states.ready(NODE_ID1);
        assert!(connection_states.has_ready_nodes(time.milliseconds()));

        time.sleep(12000);

        // disconnect nodes and check proper state of pool throughout
        connection_states.disconnected(NODE_ID2, time.milliseconds());
        assert!(connection_states.has_ready_nodes(time.milliseconds()));
        assert!(connection_states.is_blacked_out(NODE_ID2, time.milliseconds()));
        assert!(!connection_states.is_blacked_out(NODE_ID1, time.milliseconds()));
        time.sleep(connection_states.connection_delay(NODE_ID2, time.milliseconds()));
        // by the time node1 disconnects node2 should have been unblocked again
        connection_states.disconnected(NODE_ID1, time.milliseconds() + 1);
        assert!(connection_states.is_blacked_out(NODE_ID1, time.milliseconds()));
        assert!(!connection_states.is_blacked_out(NODE_ID2, time.milliseconds()));
        assert!(!connection_states.has_ready_nodes(time.milliseconds()));
    }

    /// Translated from `ClusterConnectionStatesTest.testAuthorizationFailed`
    #[test]
    fn test_authorization_failed() {
        let mut connection_states = create_single_ip_states();
        let mut time = MockTime::new();

        // Try connecting
        connection_states.connecting(NODE_ID1, time.milliseconds(), "localhost");

        time.sleep(100);

        connection_states.authentication_failed(
            NODE_ID1,
            time.milliseconds(),
            Error::SslAuthentication(crate::common::errors::SslAuthenticationError::new(
                "No path to CA for certificate!",
            )),
        );
        time.sleep(1000);
        assert_eq!(
            ConnectionState::AuthenticationFailed,
            connection_states.connection_state(NODE_ID1)
        );
        // Java stores the `AuthenticationException` object
        // (`ClusterConnectionStates.java:274`), so the subclass survives the hop:
        // a TLS certificate rejection must not read back as a SASL failure. The
        // message is the channel's own bare text, with no second class prefix.
        let stored = connection_states
            .authentication_error(NODE_ID1)
            .expect("the authentication error must be recorded");
        assert!(
            matches!(stored, Error::SslAuthentication(_)),
            "the SSL subclass must survive: {stored:?}"
        );
        assert_eq!(stored.message(), "No path to CA for certificate!");
        assert!(!connection_states.has_ready_nodes(time.milliseconds()));
        assert!(!connection_states.can_connect(NODE_ID1, time.milliseconds()));

        time.sleep(connection_states.connection_delay(NODE_ID1, time.milliseconds()) + 1);

        assert!(connection_states.can_connect(NODE_ID1, time.milliseconds()));
        connection_states.ready(NODE_ID1);
        assert!(connection_states.authentication_error(NODE_ID1).is_none());
    }

    /// Translated from `ClusterConnectionStatesTest.testRemoveNode`
    #[test]
    fn test_remove_node() {
        let mut connection_states = create_single_ip_states();
        let mut time = MockTime::new();

        connection_states.connecting(NODE_ID1, time.milliseconds(), "localhost");
        time.sleep(1000);
        connection_states.ready(NODE_ID1);
        time.sleep(10000);

        connection_states.disconnected(NODE_ID1, time.milliseconds());
        // Node is disconnected and blocked, removing it from the list should reset all blocks
        connection_states.remove(NODE_ID1);
        assert!(connection_states.can_connect(NODE_ID1, time.milliseconds()));
        assert!(!connection_states.is_blacked_out(NODE_ID1, time.milliseconds()));
        assert_eq!(0, connection_states.connection_delay(NODE_ID1, time.milliseconds()));
    }

    /// Translated from `ClusterConnectionStatesTest.testMaxReconnectBackoff`
    #[test]
    fn test_max_reconnect_backoff() {
        let mut connection_states = create_single_ip_states();
        let mut time = MockTime::new();

        let effective_max_reconnect_backoff =
            (RECONNECT_BACKOFF_MAX as f64 * (1.0 + CLUSTER_CONNECTION_STATES_RECONNECT_BACKOFF_JITTER)).round() as i64;
        connection_states.connecting(NODE_ID1, time.milliseconds(), "localhost");
        time.sleep(1000);
        connection_states.disconnected(NODE_ID1, time.milliseconds());

        // Do 100 reconnect attempts and check that MaxReconnectBackoff (plus jitter) is not
        // exceeded
        for _i in 0..100 {
            let reconnect_backoff = connection_states.connection_delay(NODE_ID1, time.milliseconds());
            assert!(
                reconnect_backoff <= effective_max_reconnect_backoff,
                "Expected reconnectBackoff {} <= {}",
                reconnect_backoff,
                effective_max_reconnect_backoff
            );
            assert!(!connection_states.can_connect(NODE_ID1, time.milliseconds()));
            time.sleep(reconnect_backoff + 1);
            assert!(connection_states.can_connect(NODE_ID1, time.milliseconds()));
            connection_states.connecting(NODE_ID1, time.milliseconds(), "localhost");
            time.sleep(10);
            connection_states.disconnected(NODE_ID1, time.milliseconds());
        }
    }

    /// Translated from `ClusterConnectionStatesTest.testExponentialReconnectBackoff`
    #[test]
    fn test_exponential_reconnect_backoff() {
        verify_reconnect_exponential_backoff(false);
        verify_reconnect_exponential_backoff(true);
    }

    /// Translated from `ClusterConnectionStatesTest.testThrottled`
    #[test]
    fn test_throttled() {
        let mut connection_states = create_single_ip_states();
        let mut time = MockTime::new();

        connection_states.connecting(NODE_ID1, time.milliseconds(), "localhost");
        time.sleep(1000);
        connection_states.ready(NODE_ID1);
        time.sleep(10000);

        // Initially not throttled.
        assert_eq!(0, connection_states.throttle_delay_ms(NODE_ID1, time.milliseconds()));

        // Throttle for 100ms from now.
        connection_states.throttle(NODE_ID1, time.milliseconds() + 100);
        assert_eq!(100, connection_states.throttle_delay_ms(NODE_ID1, time.milliseconds()));

        // Still throttled after 50ms. The remaining delay is 50ms. The poll delay should be same
        // as throttling delay.
        time.sleep(50);
        assert_eq!(50, connection_states.throttle_delay_ms(NODE_ID1, time.milliseconds()));
        assert_eq!(50, connection_states.poll_delay_ms(NODE_ID1, time.milliseconds()));

        // Not throttled anymore when the deadline is reached. The poll delay should be same as
        // connection delay.
        time.sleep(50);
        assert_eq!(0, connection_states.throttle_delay_ms(NODE_ID1, time.milliseconds()));
        assert_eq!(
            connection_states.connection_delay(NODE_ID1, time.milliseconds()),
            connection_states.poll_delay_ms(NODE_ID1, time.milliseconds())
        );
    }

    /// Translated from `ClusterConnectionStatesTest.testSingleIP`
    #[tokio::test]
    async fn test_single_ip() {
        let expected_ip = IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1));
        let host_resolver = SingleIpHostResolver;

        let mut connection_states = ClusterConnectionStates::new(
            RECONNECT_BACKOFF_MS,
            RECONNECT_BACKOFF_MAX,
            CONNECTION_SETUP_TIMEOUT_MS,
            CONNECTION_SETUP_TIMEOUT_MAX_MS,
            LogContext::empty(),
            host_resolver,
        );
        let time = MockTime::new();

        connection_states.connecting(NODE_ID1, time.milliseconds(), "localhost");
        let addr1 = connection_states.current_address(NODE_ID1).await.unwrap();
        assert_eq!(expected_ip, addr1);

        connection_states.connecting(NODE_ID1, time.milliseconds(), "localhost");
        let addr2 = connection_states.current_address(NODE_ID1).await.unwrap();
        assert_eq!(addr1, addr2);
    }

    /// Translated from `ClusterConnectionStatesTest.testMultipleIPs`
    #[tokio::test]
    async fn test_multiple_ips() {
        let mut connection_states = create_multi_ip_states();
        let time = MockTime::new();

        let resolved = client_utils::resolve(HOST_TWO_IPS, &connection_states.host_resolver)
            .await
            .unwrap();
        assert!(resolved.len() > 1);

        connection_states.connecting(NODE_ID1, time.milliseconds(), HOST_TWO_IPS);
        let addr1 = connection_states.current_address(NODE_ID1).await.unwrap();
        connection_states.connecting(NODE_ID1, time.milliseconds(), HOST_TWO_IPS);
        let addr2 = connection_states.current_address(NODE_ID1).await.unwrap();
        assert_ne!(addr1, addr2);
        connection_states.connecting(NODE_ID1, time.milliseconds(), HOST_TWO_IPS);
        let addr3 = connection_states.current_address(NODE_ID1).await.unwrap();
        assert_ne!(addr1, addr3);
    }

    /// Translated from `ClusterConnectionStatesTest.testHostResolveChange`
    #[tokio::test]
    async fn test_host_resolve_change() {
        let mut connection_states = create_multi_ip_states();
        let time = MockTime::new();

        let resolved = client_utils::resolve(HOST_TWO_IPS, &connection_states.host_resolver)
            .await
            .unwrap();
        assert!(resolved.len() > 1);

        connection_states.connecting(NODE_ID1, time.milliseconds(), HOST_TWO_IPS);
        let addr1 = connection_states.current_address(NODE_ID1).await.unwrap();

        connection_states.host_resolver.change_addresses();
        connection_states.connecting(NODE_ID1, time.milliseconds(), "localhost");
        let addr2 = connection_states.current_address(NODE_ID1).await.unwrap();

        assert_ne!(addr1, addr2);
    }

    /// Translated from `ClusterConnectionStatesTest.testNodeWithNewHostname`
    #[tokio::test]
    async fn test_node_with_new_hostname() {
        let mut connection_states = create_multi_ip_states();
        let time = MockTime::new();

        connection_states.connecting(NODE_ID1, time.milliseconds(), "localhost");
        let addr1 = connection_states.current_address(NODE_ID1).await.unwrap();

        connection_states.host_resolver.change_addresses();
        connection_states.connecting(NODE_ID1, time.milliseconds(), HOST_TWO_IPS);
        let addr2 = connection_states.current_address(NODE_ID1).await.unwrap();

        assert_ne!(addr1, addr2);
    }

    /// Translated from `ClusterConnectionStatesTest.testIsPreparingConnection`
    #[test]
    fn test_is_preparing_connection() {
        let mut connection_states = create_single_ip_states();
        let time = MockTime::new();

        assert!(!connection_states.is_preparing_connection(NODE_ID1));
        connection_states.connecting(NODE_ID1, time.milliseconds(), "localhost");
        assert!(connection_states.is_preparing_connection(NODE_ID1));
        connection_states.checking_api_versions(NODE_ID1);
        assert!(connection_states.is_preparing_connection(NODE_ID1));
        connection_states.disconnected(NODE_ID1, time.milliseconds());
        assert!(!connection_states.is_preparing_connection(NODE_ID1));
    }

    /// Translated from `ClusterConnectionStatesTest.testExponentialConnectionSetupTimeout`
    #[test]
    fn test_exponential_connection_setup_timeout() {
        let mut connection_states = create_single_ip_states();
        let time = MockTime::new();

        assert!(connection_states.can_connect(NODE_ID1, time.milliseconds()));

        // Check the exponential timeout growth
        let max_n = ((CONNECTION_SETUP_TIMEOUT_MAX_MS as f64 / CONNECTION_SETUP_TIMEOUT_MS as f64).ln()
            / (CLUSTER_CONNECTION_STATES_CONNECTION_SETUP_TIMEOUT_EXP_BASE as f64).ln()) as i32;
        for n in 0..=max_n {
            connection_states.connecting(NODE_ID1, time.milliseconds(), "localhost");
            assert!(connection_states.connecting_nodes().contains(NODE_ID1));
            let expected = CONNECTION_SETUP_TIMEOUT_MS as f64
                * (CLUSTER_CONNECTION_STATES_CONNECTION_SETUP_TIMEOUT_EXP_BASE as f64).powi(n);
            let tolerance = expected * CLUSTER_CONNECTION_STATES_CONNECTION_SETUP_TIMEOUT_JITTER;
            let actual = connection_states.connection_setup_timeout_ms(NODE_ID1) as f64;
            assert!(
                (actual - expected).abs() <= tolerance,
                "Expected connectionSetupTimeoutMs ~= {} +/- {}, got {}",
                expected,
                tolerance,
                actual
            );
            connection_states.disconnected(NODE_ID1, time.milliseconds());
            assert!(!connection_states.connecting_nodes().contains(NODE_ID1));
        }

        // Check the timeout value upper bound
        connection_states.connecting(NODE_ID1, time.milliseconds(), "localhost");
        let actual = connection_states.connection_setup_timeout_ms(NODE_ID1) as f64;
        let tolerance =
            CONNECTION_SETUP_TIMEOUT_MAX_MS as f64 * CLUSTER_CONNECTION_STATES_CONNECTION_SETUP_TIMEOUT_JITTER;
        assert!(
            (actual - CONNECTION_SETUP_TIMEOUT_MAX_MS as f64).abs() <= tolerance,
            "Expected connectionSetupTimeoutMs ~= {} +/- {}, got {}",
            CONNECTION_SETUP_TIMEOUT_MAX_MS,
            tolerance,
            actual
        );
        assert!(connection_states.connecting_nodes().contains(NODE_ID1));

        // Should reset the timeout value to the init value
        connection_states.ready(NODE_ID1);
        let actual = connection_states.connection_setup_timeout_ms(NODE_ID1) as f64;
        let tolerance = CONNECTION_SETUP_TIMEOUT_MS as f64 * CLUSTER_CONNECTION_STATES_CONNECTION_SETUP_TIMEOUT_JITTER;
        assert!(
            (actual - CONNECTION_SETUP_TIMEOUT_MS as f64).abs() <= tolerance,
            "Expected connectionSetupTimeoutMs ~= {} +/- {}, got {}",
            CONNECTION_SETUP_TIMEOUT_MS,
            tolerance,
            actual
        );
        assert!(!connection_states.connecting_nodes().contains(NODE_ID1));
        connection_states.disconnected(NODE_ID1, time.milliseconds());

        // Check if the connection state transition from ready to disconnected
        // won't increase the timeout value
        connection_states.connecting(NODE_ID1, time.milliseconds(), "localhost");
        let actual = connection_states.connection_setup_timeout_ms(NODE_ID1) as f64;
        assert!(
            (actual - CONNECTION_SETUP_TIMEOUT_MS as f64).abs() <= tolerance,
            "Expected connectionSetupTimeoutMs ~= {} +/- {}, got {}",
            CONNECTION_SETUP_TIMEOUT_MS,
            tolerance,
            actual
        );
        assert!(connection_states.connecting_nodes().contains(NODE_ID1));
    }

    /// Translated from `ClusterConnectionStatesTest.testTimedOutConnections`
    #[test]
    fn test_timed_out_connections() {
        let mut connection_states = create_single_ip_states();
        let mut time = MockTime::new();

        // Initiate two connections
        connection_states.connecting(NODE_ID1, time.milliseconds(), "localhost");
        connection_states.connecting(NODE_ID2, time.milliseconds(), "localhost");

        // Expect no timed out connections
        assert_eq!(
            0,
            connection_states.nodes_with_connection_setup_timeout(time.milliseconds()).len()
        );

        // Advance time by half of the connection setup timeout
        time.sleep(CONNECTION_SETUP_TIMEOUT_MS / 2);

        // Initiate a third connection
        connection_states.connecting(NODE_ID3, time.milliseconds(), "localhost");

        // Advance time beyond the connection setup timeout (+ max jitter) for the first two
        // connections
        time.sleep(
            CONNECTION_SETUP_TIMEOUT_MS / 2
                + (CONNECTION_SETUP_TIMEOUT_MS as f64 * CLUSTER_CONNECTION_STATES_CONNECTION_SETUP_TIMEOUT_JITTER)
                    as i64,
        );

        // Expect two timed out connections.
        let timed_out_connections = connection_states.nodes_with_connection_setup_timeout(time.milliseconds());
        assert_eq!(2, timed_out_connections.len());
        assert!(timed_out_connections.contains(&NODE_ID1.to_string()));
        assert!(timed_out_connections.contains(&NODE_ID2.to_string()));

        // Disconnect the first two connections
        connection_states.disconnected(NODE_ID1, time.milliseconds());
        connection_states.disconnected(NODE_ID2, time.milliseconds());

        // Advance time beyond the connection setup timeout (+ max jitter) for the third
        // connection
        time.sleep(
            CONNECTION_SETUP_TIMEOUT_MS / 2
                + (CONNECTION_SETUP_TIMEOUT_MS as f64 * CLUSTER_CONNECTION_STATES_CONNECTION_SETUP_TIMEOUT_JITTER)
                    as i64,
        );

        // Expect one timed out connection
        let timed_out_connections = connection_states.nodes_with_connection_setup_timeout(time.milliseconds());
        assert_eq!(1, timed_out_connections.len());
        assert!(timed_out_connections.contains(&NODE_ID3.to_string()));

        // Disconnect the third connection
        connection_states.disconnected(NODE_ID3, time.milliseconds());

        // Expect no timed out connections
        assert_eq!(
            0,
            connection_states.nodes_with_connection_setup_timeout(time.milliseconds()).len()
        );
    }

    /// Translated from `ClusterConnectionStatesTest.testSkipLastAttemptedIp`
    #[tokio::test]
    async fn test_skip_last_attempted_ip() {
        let mut connection_states = create_multi_ip_states();
        let time = MockTime::new();

        let resolved = client_utils::resolve(HOST_TWO_IPS, &connection_states.host_resolver)
            .await
            .unwrap();
        assert!(resolved.len() > 1);

        // Connect to the first IP
        connection_states.connecting(NODE_ID1, time.milliseconds(), HOST_TWO_IPS);
        let addr1 = connection_states.current_address(NODE_ID1).await.unwrap();

        // Disconnect, which will trigger re-resolution with the first IP still first
        connection_states.disconnected(NODE_ID1, time.milliseconds());

        // Connect again, the first IP should get skipped
        connection_states.connecting(NODE_ID1, time.milliseconds(), HOST_TWO_IPS);
        let addr2 = connection_states.current_address(NODE_ID1).await.unwrap();
        assert_ne!(addr1, addr2);
    }

    fn verify_reconnect_exponential_backoff(enter_checking_api_version_state: bool) {
        let mut connection_states = create_single_ip_states();
        let mut time = MockTime::new();

        let reconnect_backoff_max_exp = (RECONNECT_BACKOFF_MAX as f64 / (RECONNECT_BACKOFF_MS.max(1) as f64)).ln()
            / (CLUSTER_CONNECTION_STATES_RECONNECT_BACKOFF_EXP_BASE as f64).ln();

        connection_states.remove(NODE_ID1);
        // Run through 10 disconnects and check that reconnect backoff value is within expected
        // range for every attempt
        for i in 0..10 {
            connection_states.connecting(NODE_ID1, time.milliseconds(), "localhost");
            if enter_checking_api_version_state {
                connection_states.checking_api_versions(NODE_ID1);
            }

            connection_states.disconnected(NODE_ID1, time.milliseconds());
            // Calculate expected backoff value without jitter
            let expected_backoff = ((CLUSTER_CONNECTION_STATES_RECONNECT_BACKOFF_EXP_BASE as f64)
                .powf((i as f64).min(reconnect_backoff_max_exp))
                * RECONNECT_BACKOFF_MS as f64)
                .round() as i64;
            let current_backoff = connection_states.connection_delay(NODE_ID1, time.milliseconds());
            assert!(
                (current_backoff as f64 - expected_backoff as f64).abs()
                    <= CLUSTER_CONNECTION_STATES_RECONNECT_BACKOFF_JITTER * expected_backoff as f64,
                "Attempt {}: Expected backoff ~= {} +/- {}, got {}",
                i,
                expected_backoff,
                CLUSTER_CONNECTION_STATES_RECONNECT_BACKOFF_JITTER * expected_backoff as f64,
                current_backoff
            );
            time.sleep(connection_states.connection_delay(NODE_ID1, time.milliseconds()) + 1);
        }
    }
}
