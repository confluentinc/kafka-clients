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

//! `ShareAcknowledgementCommitCallbackRegistrationEvent` — application event
//! that notifies the network task whether an acknowledgement-commit callback
//! is registered (KIP-932).
//!
//! Translated from
//! `org.apache.kafka.clients.consumer.internals.events.ShareAcknowledgementCommitCallbackRegistrationEvent`.
//!
//! Java's class `extends ApplicationEvent` (bare, non-completable). The
//! app-side dispatch lands with the share application-event processor in a
//! later phase; this phase ships the event type only.

/// Application event indicating whether an acknowledgement-commit callback
/// is registered on the consumer.
///
/// Corresponds to
/// `org.apache.kafka.clients.consumer.internals.events.ShareAcknowledgementCommitCallbackRegistrationEvent`.
pub(crate) struct ShareAcknowledgementCommitCallbackRegistrationEvent {
    is_callback_registered: bool,
}

impl ShareAcknowledgementCommitCallbackRegistrationEvent {
    /// Constructs a new [`ShareAcknowledgementCommitCallbackRegistrationEvent`].
    pub(crate) fn new(is_callback_registered: bool) -> Self {
        Self { is_callback_registered }
    }

    /// Whether an acknowledgement-commit callback is registered.
    pub(crate) fn is_callback_registered(&self) -> bool {
        self.is_callback_registered
    }
}
