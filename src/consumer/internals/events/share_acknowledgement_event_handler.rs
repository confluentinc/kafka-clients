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

//! `ShareAcknowledgementEventHandler` — receives
//! [`ShareAcknowledgementEvent`]s from the network task and makes them
//! available to the application task (KIP-932).
//!
//! Translated from
//! `org.apache.kafka.clients.consumer.internals.events.ShareAcknowledgementEventHandler`.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use crate::consumer::internals::events::share_acknowledgement_event::ShareAcknowledgementEvent;

/// Shared, thread-safe queue of [`ShareAcknowledgementEvent`]s.
///
/// Java holds a `BlockingQueue<ShareAcknowledgementEvent>`. The network
/// task never blocks on adds (unbounded), so a `Mutex<VecDeque<...>>`
/// behind an `Arc` reproduces the `add` / `drainTo` behaviour while
/// letting the application-task side share the same queue instance.
pub(crate) type ShareAcknowledgementEventQueue = Arc<Mutex<VecDeque<ShareAcknowledgementEvent>>>;

/// An event handler that receives [`ShareAcknowledgementEvent`]s from the
/// network task which are then made available to the application task.
///
/// Corresponds to
/// `org.apache.kafka.clients.consumer.internals.events.ShareAcknowledgementEventHandler`.
///
/// Cloning shares the same underlying queue (cheap `Arc` clone), so the
/// application task and the network task can hold independent handles to
/// the same event stream.
#[derive(Clone)]
pub(crate) struct ShareAcknowledgementEventHandler {
    event_queue: ShareAcknowledgementEventQueue,
}

impl ShareAcknowledgementEventHandler {
    /// Constructs a handler over the given shared event queue.
    ///
    /// Corresponds to Java's
    /// `ShareAcknowledgementEventHandler(BlockingQueue<ShareAcknowledgementEvent>)`.
    pub(crate) fn new(event_queue: ShareAcknowledgementEventQueue) -> Self {
        Self { event_queue }
    }

    /// Add a [`ShareAcknowledgementEvent`] to the handler.
    ///
    /// Corresponds to Java's `add(ShareAcknowledgementEvent)`.
    pub(crate) fn add(&self, event: ShareAcknowledgementEvent) {
        // The poisoned-lock case only arises if a holder panicked while
        // mutating the queue; recover the guard rather than propagate.
        let mut queue = self.event_queue.lock().unwrap_or_else(|e| e.into_inner());
        queue.push_back(event);
    }

    /// Drain all the [`ShareAcknowledgementEvent`]s from the handler.
    ///
    /// Corresponds to Java's `drainEvents()`.
    pub(crate) fn drain_events(&self) -> Vec<ShareAcknowledgementEvent> {
        let mut queue = self.event_queue.lock().unwrap_or_else(|e| e.into_inner());
        queue.drain(..).collect()
    }
}

impl Default for ShareAcknowledgementEventHandler {
    fn default() -> Self {
        Self::new(Arc::new(Mutex::new(VecDeque::new())))
    }
}
