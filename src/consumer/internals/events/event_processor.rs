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

//! `EventProcessor<E>` — generic event-dispatch seam.
//!
//! Translated from
//! `org.apache.kafka.clients.consumer.internals.events.EventProcessor<T>`.
//! Phase 5 ships only the trait; the implementation lands in Phase 10
//! (`ApplicationEventProcessor`) and Phase 6 (`BackgroundEventProcessor`).
//!
//! The trait is intentionally **sync** — Java's `EventProcessor.process`
//! is a `void` method called once per event from inside the network
//! thread's run loop. In Rust the equivalent is a sync `fn` called
//! inside the bg task's `run_once`. Per DoD §11 / `consumer-threading.md`,
//! `#[async_trait]` is only used on the top-level [`crate::consumer::Consumer`]
//! dispatch surface, not on per-event traits.

/// Generic event-processor seam. Phase 6 / Phase 10 supply
/// implementations for the [`super::application_event::ApplicationEvent`]
/// and [`super::background_event::BackgroundEvent`] enum types.
pub(crate) trait EventProcessor<E>: Send + 'static {
    /// Process a single event. Called once per event drained from the
    /// channel; impls dispatch to the appropriate request manager or
    /// listener.
    fn process(&mut self, event: E);
}
