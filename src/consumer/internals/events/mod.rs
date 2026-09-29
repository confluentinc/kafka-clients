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

//! Event-passing layer between the application thread and the consumer
//! background task.
//!
//! Translated from
//! `org.apache.kafka.clients.consumer.internals.events.*`. Per CLAUDE.md §2,
//! types in this module use `pub(crate)` visibility because they live in
//! the Java `internals` package.
//!
//! # Dead-code lint
//!
//! This phase ships **data + plumbing**, not behavior. The
//! [`crate::consumer::internals::events::CompletableEventReaper`] reaper,
//! channel handlers, and `WakeupTrigger` are exercised by their own tests
//! and will gain non-test callers in Phase 6 and Phase 10. The
//! module-level `#[allow(dead_code)]` keeps `cargo build` warning-free
//! until those phases land.

#![allow(dead_code)]

mod application_event;
mod application_event_handler;
mod application_event_processor;
mod background_event;
mod background_event_handler;
mod completable_event;
mod completable_event_reaper;
mod event_processor;

pub(crate) use application_event::{ApplicationEvent, ApplicationEventEnvelope, AsyncPollState};
pub(crate) use application_event_handler::ApplicationEventHandler;
pub(crate) use application_event_processor::ApplicationEventProcessor;
pub(crate) use background_event::{BackgroundEvent, BackgroundEventEnvelope};
pub(crate) use background_event_handler::BackgroundEventHandler;
pub(crate) use completable_event::{CompletableEvent, CompletableEventErasedHandle, CompletableEventHandle};
pub(crate) use completable_event_reaper::CompletableEventReaper;
pub(crate) use event_processor::EventProcessor;
