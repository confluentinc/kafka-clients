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

//! A network send that wraps an inner send with a destination identifier.
//!
//! Translated from `org.apache.kafka.common.network.NetworkSend`.

use super::KafkaSend;
use super::TransportLayer;

use std::future::Future;
use std::io;
use std::pin::Pin;

/// A network send that wraps an inner [`KafkaSend`] with a destination identifier.
///
/// `NetworkSend` delegates all send operations to the inner send and adds
/// a `destination_id` to identify the target broker/node.
pub struct NetworkSend {
    /// The destination identifier (typically a broker node ID).
    destination_id: String,
    /// The inner send that performs the actual data writing.
    send: Box<dyn KafkaSend>,
    /// Whether this send expects no response (producer `acks=0`).
    ///
    /// When `true`, the request is fire-and-forget: the broker never replies,
    /// so the network stack must treat the *send completing* as the terminal
    /// event. The selector uses this to break its poll loop as soon as such a
    /// send finishes writing, rather than blocking until the poll deadline
    /// waiting for a receive that will never arrive. Defaults to `false` — every
    /// other request (all consumer requests, and `acks=1`/`acks=all` produce)
    /// expects a response and is unaffected.
    fire_and_forget: bool,
}

impl NetworkSend {
    /// Creates a new `NetworkSend` with the given destination and inner send.
    ///
    /// The send defaults to expecting a response (`fire_and_forget = false`);
    /// callers that know the request is fire-and-forget (producer `acks=0`) mark
    /// it via [`set_fire_and_forget`](Self::set_fire_and_forget).
    pub fn new(destination_id: &str, send: Box<dyn KafkaSend>) -> Self {
        Self { destination_id: destination_id.to_string(), send, fire_and_forget: false }
    }

    /// Returns the destination identifier.
    pub fn destination_id(&self) -> &str {
        &self.destination_id
    }

    /// Marks whether this send is fire-and-forget (expects no response).
    pub fn set_fire_and_forget(&mut self, value: bool) {
        self.fire_and_forget = value;
    }

    /// Returns whether this send is fire-and-forget (producer `acks=0`).
    pub fn is_fire_and_forget(&self) -> bool {
        self.fire_and_forget
    }

    /// Returns a reference to the inner send.
    pub fn send(&self) -> &dyn KafkaSend {
        self.send.as_ref()
    }
}

impl KafkaSend for NetworkSend {
    fn completed(&self) -> bool {
        self.send.completed()
    }

    fn write_to<'a>(
        &'a mut self,
        channel: &'a mut dyn TransportLayer,
    ) -> Pin<Box<dyn Future<Output = io::Result<usize>> + Send + 'a>> {
        Box::pin(async { self.send.write_to(channel).await })
    }

    fn try_write_to(&mut self, channel: &mut dyn TransportLayer) -> io::Result<usize> {
        self.send.try_write_to(channel)
    }

    fn size(&self) -> usize {
        self.send.size()
    }
}
