// Copyright 2026 Confluent Inc.
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

//! `ShareAcknowledgementEventHandler` — the queue of
//! [`ShareAcknowledgementEvent`]s from the background task to the application
//! task.
//!
//! Translated from
//! `org.apache.kafka.clients.consumer.internals.events.ShareAcknowledgementEventHandler`.
//! Java wraps a `LinkedBlockingQueue` passed to its constructor; Rust owns an
//! unbounded channel, written by the background task and drained with
//! `try_recv` by the application task (`share-consumer.md` §6). The receiver
//! sits behind a `std::sync::Mutex` only so that the handler, shared by both
//! tasks, can drain through `&self`; the lock is never held across an
//! `.await`.

use std::sync::Mutex;

use tokio::sync::mpsc;

use super::ShareAcknowledgementEvent;

/// The queue of completed acknowledgements between the background task and
/// the application task.
#[doc(alias = "org.apache.kafka.clients.consumer.internals.events.ShareAcknowledgementEventHandler")]
pub(crate) struct ShareAcknowledgementEventHandler {
    event_tx: mpsc::UnboundedSender<ShareAcknowledgementEvent>,
    event_rx: Mutex<mpsc::UnboundedReceiver<ShareAcknowledgementEvent>>,
}

impl ShareAcknowledgementEventHandler {
    /// An empty queue.
    #[doc(
        alias = "org.apache.kafka.clients.consumer.internals.events.ShareAcknowledgementEventHandler#ShareAcknowledgementEventHandler"
    )]
    pub(crate) fn new() -> Self {
        let (event_tx, event_rx) = mpsc::unbounded_channel();
        Self { event_tx, event_rx: Mutex::new(event_rx) }
    }

    /// Add a [`ShareAcknowledgementEvent`] to the handler.
    ///
    /// # Arguments
    ///
    /// - `event` - A [`ShareAcknowledgementEvent`] created by the network task
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.events.ShareAcknowledgementEventHandler#add")]
    pub(crate) fn add(&self, event: ShareAcknowledgementEvent) {
        // The receiver lives in `self`, so the channel cannot be closed here.
        let _ = self.event_tx.send(event);
    }

    /// Drain all the [`ShareAcknowledgementEvent`]s from the handler.
    ///
    /// # Returns
    ///
    /// A list of [`ShareAcknowledgementEvent`]s that were drained
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.events.ShareAcknowledgementEventHandler#drainEvents")]
    pub(crate) fn drain_events(&self) -> Vec<ShareAcknowledgementEvent> {
        let mut event_rx = self.event_rx.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut events = Vec::new();
        while let Ok(event) = event_rx.try_recv() {
            events.push(event);
        }
        events
    }
}

impl Default for ShareAcknowledgementEventHandler {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for ShareAcknowledgementEventHandler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ShareAcknowledgementEventHandler").finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    /// Rust-only: Java has no test of its own for this class. Events come out
    /// in the order they were added, and a drain empties the queue.
    #[test]
    fn test_drain_events_returns_events_in_order_and_empties_the_queue() {
        let handler = ShareAcknowledgementEventHandler::new();
        assert!(handler.drain_events().is_empty());

        handler.add(ShareAcknowledgementEvent::new(HashMap::new(), false, Some(1)));
        handler.add(ShareAcknowledgementEvent::new(HashMap::new(), true, Some(2)));

        let events = handler.drain_events();
        assert_eq!(
            vec![Some(1), Some(2)],
            events
                .iter()
                .map(ShareAcknowledgementEvent::acquisition_lock_timeout_ms)
                .collect::<Vec<_>>()
        );
        assert!(!events[0].check_for_renew_acknowledgements());
        assert!(events[1].check_for_renew_acknowledgements());
        assert!(handler.drain_events().is_empty());
    }
}
