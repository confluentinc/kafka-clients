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

//! A fake selector for testing purposes.
//!
//! Translated from `org.apache.kafka.test.MockSelector`.

use std::collections::{HashMap, HashSet};
use std::io;
use std::net::SocketAddr;

use super::ChannelState;
use super::NetworkReceive;
use super::NetworkSend;

/// A delayed receive that is delivered when a matching send completes.
///
/// Translated from `org.apache.kafka.test.DelayedReceive`.
pub struct DelayedReceive {
    source: String,
    receive: NetworkReceive,
}

impl DelayedReceive {
    /// Creates a new `DelayedReceive`.
    pub fn new(source: &str, receive: NetworkReceive) -> Self {
        Self { source: source.to_string(), receive }
    }

    /// Returns the source identifier for this delayed receive.
    pub fn source(&self) -> &str {
        &self.source
    }
}

/// A fake selector to use for testing.
///
/// Translated from `org.apache.kafka.test.MockSelector`.
///
/// # Poll cycle
///
/// Each `poll()` call clears the previous cycle's `completed_sends`,
/// `completed_receives`, `disconnected`, and `connected`. Then it processes
/// pending connections, initiated sends, and delayed receives.
///
/// This matches the Java `MockSelector` semantics:
/// - In Java, `connect()` adds to `connected` directly and `connected()`
///   returns a snapshot-and-clear. Since our `Selectable` trait returns
///   `&[String]`, we use a staging area (`pending_connected`) and clear +
///   move in `poll()` to achieve the same one-shot consumption behavior.
pub struct MockSelector {
    initiated_sends: Vec<NetworkSend>,
    completed_sends: Vec<NetworkSend>,
    completed_receives: Vec<NetworkReceive>,
    disconnected: HashMap<String, ChannelState>,
    /// Nodes that completed connection and will be visible via `connected()`.
    connected: Vec<String>,
    /// Staging area: nodes from `connect()` calls waiting to be promoted to
    /// `connected` on the next `poll()`.
    pending_connected: Vec<String>,
    delayed_receives: Vec<DelayedReceive>,
    ready: HashSet<String>,
}

impl Default for MockSelector {
    fn default() -> Self {
        Self::new()
    }
}

impl MockSelector {
    /// Creates a new `MockSelector`.
    pub fn new() -> Self {
        Self {
            initiated_sends: Vec::new(),
            completed_sends: Vec::new(),
            completed_receives: Vec::new(),
            disconnected: HashMap::new(),
            connected: Vec::new(),
            pending_connected: Vec::new(),
            delayed_receives: Vec::new(),
            ready: HashSet::new(),
        }
    }

    /// Clears all completed sends, receives, disconnections, and connections.
    pub fn clear(&mut self) {
        self.completed_sends.clear();
        self.completed_receives.clear();
        self.disconnected.clear();
        self.connected.clear();
    }

    /// Resets all state including initiated sends and delayed receives.
    pub fn reset(&mut self) {
        self.clear();
        self.initiated_sends.clear();
        self.delayed_receives.clear();
    }

    /// Simulate a server disconnect. This id will be present in `disconnected()`
    /// on the next `poll()`.
    pub fn server_disconnect(&mut self, id: &str) {
        self.disconnected
            .insert(id.to_string(), ChannelState::new(super::channel_state::State::Ready));
        self.close_channel_sync(id);
    }

    /// Simulate a server authentication failure, raising the base
    /// `AuthenticationException`.
    pub fn server_authentication_failed(&mut self, id: &str) {
        self.server_authentication_failed_with(
            id,
            crate::common::Error::Authentication(crate::common::errors::AuthenticationError::new(
                "Authentication failed",
            )),
        );
    }

    /// Simulate a server authentication failure raising a specific
    /// `AuthenticationException` subclass.
    ///
    /// `KafkaChannel.prepare` catches `SslAuthenticationException` and
    /// `SaslAuthenticationException` separately (`KafkaChannel.java:463-472`) and
    /// stores whichever it caught in the `ChannelState`, so which subclass reaches
    /// `client.authenticationException(node)` is observable behaviour; this lets a
    /// test pick it.
    pub fn server_authentication_failed_with(&mut self, id: &str, error: crate::common::Error) {
        let auth_failed =
            ChannelState::with_error_remote_address(super::channel_state::State::AuthenticationFailed, error, None);
        self.disconnected.insert(id.to_string(), auth_failed);
        self.close_channel_sync(id);
    }

    /// Since `MockSelector::connect` will always succeed and add the connection id
    /// to the connected set, we can only simulate that the connection is still
    /// pending by removing the connection id from the connected set.
    pub fn server_connection_blocked(&mut self, id: &str) {
        self.pending_connected.retain(|c| c != id);
        self.connected.retain(|c| c != id);
    }

    /// Queue a completed receive directly.
    pub fn complete_receive(&mut self, receive: NetworkReceive) {
        self.completed_receives.push(receive);
    }

    /// Queue a delayed receive that will be delivered when a matching send completes.
    pub fn delayed_receive(&mut self, receive: DelayedReceive) {
        self.delayed_receives.push(receive);
    }

    /// Clears just the completed sends list.
    pub fn clear_completed_sends(&mut self) {
        self.completed_sends.clear();
    }

    /// Clears just the completed receives list.
    pub fn clear_completed_receives(&mut self) {
        self.completed_receives.clear();
    }

    /// Mark a channel as not ready.
    pub fn channel_not_ready(&mut self, id: &str) {
        self.ready.remove(id);
    }

    /// Synchronous close implementation used internally.
    fn close_channel_sync(&mut self, id: &str) {
        // Note that there are no notifications for client-side disconnects
        self.completed_sends.retain(|s| s.destination_id() != id);
        self.initiated_sends.retain(|s| s.destination_id() != id);
        self.ready.remove(id);

        if let Some(pos) = self.connected.iter().position(|c| c == id) {
            self.connected.remove(pos);
        }
        self.pending_connected.retain(|c| c != id);
    }

    /// Completes all initiated sends by consuming them.
    fn complete_initiated_sends(&mut self) {
        let initiated: Vec<NetworkSend> = self.initiated_sends.drain(..).collect();
        for send in initiated {
            self.completed_sends.push(send);
        }
    }

    /// Completes any delayed receives whose source matches a completed send.
    fn complete_delayed_receives(&mut self) {
        let mut to_deliver = Vec::new();

        for completed_send in &self.completed_sends {
            let mut i = 0;
            while i < self.delayed_receives.len() {
                if self.delayed_receives[i].source() == completed_send.destination_id() {
                    let delayed = self.delayed_receives.remove(i);
                    to_deliver.push(delayed.receive);
                } else {
                    i += 1;
                }
            }
        }

        self.completed_receives.extend(to_deliver);
    }
}

/// Implementation of `Selectable` for `MockSelector`.
impl super::Selectable for MockSelector {
    fn connect(
        &mut self,
        id: &str,
        _address: SocketAddr,
        _peer_host: &str,
        _send_buffer_size: i32,
        _receive_buffer_size: i32,
    ) -> impl std::future::Future<Output = io::Result<()>> + Send {
        self.pending_connected.push(id.to_string());
        self.ready.insert(id.to_string());
        async { Ok(()) }
    }

    fn wakeup(&self) {}

    fn wakeup_handle(&self) -> std::sync::Arc<tokio::sync::Notify> {
        // The mock selector never blocks on real I/O, so its `poll()` does not
        // await this primitive; return an unused handle to satisfy the trait.
        std::sync::Arc::new(tokio::sync::Notify::new())
    }

    async fn close(&mut self) {}

    fn close_channel(&mut self, id: &str) -> impl std::future::Future<Output = ()> + Send {
        self.close_channel_sync(id);
        async {}
    }

    fn send(&mut self, send: NetworkSend) -> Result<(), String> {
        self.initiated_sends.push(send);
        Ok(())
    }

    fn poll(&mut self, _timeout_ms: i64) -> impl std::future::Future<Output = io::Result<()>> + Send {
        // In Java, `connected()` returns a snapshot-and-clear (each call
        // only returns connections accumulated since the last read). Since
        // our `Selectable` trait's `connected()` returns `&[String]`, we
        // replicate one-shot semantics by clearing `connected` at the start
        // of each poll and moving in the pending set.
        self.connected.clear();
        self.connected.append(&mut self.pending_connected);

        // Complete initiated sends and any delayed receives
        self.complete_initiated_sends();
        self.complete_delayed_receives();

        async { Ok(()) }
    }

    fn completed_sends(&self) -> &[NetworkSend] {
        &self.completed_sends
    }

    fn completed_receives(&self) -> Vec<&NetworkReceive> {
        self.completed_receives.iter().collect()
    }

    fn drain_completed_receives(&mut self) -> Vec<(String, Option<Vec<u8>>)> {
        // Mirror the Selector: move the receives out and take each payload Vec
        // by move (§27 Phase 20 Fix #3). Leaves the list empty so the next
        // poll's clear is a no-op.
        std::mem::take(&mut self.completed_receives)
            .into_iter()
            .map(NetworkReceive::into_source_and_payload)
            .collect()
    }

    fn disconnected(&self) -> &HashMap<String, ChannelState> {
        &self.disconnected
    }

    fn connected(&self) -> &[String] {
        &self.connected
    }

    fn mute(&mut self, _id: &str) {}

    fn unmute(&mut self, _id: &str) {}

    fn mute_all(&mut self) {}

    fn unmute_all(&mut self) {}

    fn is_channel_ready(&self, id: &str) -> bool {
        self.ready.contains(id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::network::Selectable;

    /// Phase 20 Fix #3: `drain_completed_receives` returns each receive's
    /// source and payload by move and empties the internal list, so the next
    /// poll's clear is a no-op (no double-process).
    #[test]
    fn test_drain_completed_receives_moves_and_empties() {
        let mut sel = MockSelector::new();
        sel.complete_receive(NetworkReceive::with_source_buffer("node-1", vec![1, 2, 3]));
        sel.complete_receive(NetworkReceive::with_source_buffer("node-2", vec![4, 5]));
        assert_eq!(2, sel.completed_receives().len());

        let drained = sel.drain_completed_receives();
        assert_eq!(2, drained.len());
        assert_eq!(("node-1".to_string(), Some(vec![1, 2, 3])), drained[0]);
        assert_eq!(("node-2".to_string(), Some(vec![4, 5])), drained[1]);

        // The list is now empty — a second drain (or the next poll's clear)
        // yields nothing, so the response is not double-processed.
        assert!(sel.completed_receives().is_empty());
        assert!(sel.drain_completed_receives().is_empty());
    }
}
