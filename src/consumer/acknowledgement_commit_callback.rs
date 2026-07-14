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

//! Callback interface notified when a share-group acknowledgement completes.
//!
//! Translated from
//! `org.apache.kafka.clients.consumer.AcknowledgementCommitCallback`.

use std::collections::{HashMap, HashSet};

use async_trait::async_trait;

use crate::common::{KafkaError, TopicIdPartition};

/// A callback interface that the user can implement to trigger custom actions
/// when an acknowledgement completes. The callback may be executed on any task
/// calling [`ShareConsumer::poll`](crate::consumer) (KIP-932).
///
/// Corresponds to Java's
/// `org.apache.kafka.clients.consumer.AcknowledgementCommitCallback`.
///
/// # Invocation model
///
/// Per `consumer-threading.md` §31, the callback executes on the **caller's
/// task** that is currently inside `poll()` — never on the background task.
/// This matches Java's contract: "The callback may be executed in any thread
/// calling `ShareConsumer.poll(Duration)`." The invocation wiring lands with
/// `ShareConsumeRequestManager` in a later phase (Phase 5/6).
///
/// # Async
///
/// `#[async_trait]` is used, mirroring the
/// [`OffsetCommitCallback`](crate::consumer::OffsetCommitCallback) precedent.
/// Java's `onComplete` is a simple `void` notification callback invoked
/// per-acknowledgement-commit (not per-record), so the one `Box<Future>` per
/// invocation is negligible — CLAUDE.md §11 only forbids `#[async_trait]` on
/// per-record hot paths. Making it `async` lets user callbacks await without
/// forcing a blocking-inside-async antipattern.
///
/// # Bounds: `Send + Sync + 'static`
///
/// The callback is stored as `Arc<dyn AcknowledgementCommitCallback>` and
/// invoked on the app task after the background task has moved on, matching
/// the `OffsetCommitCallback` "send through channel, invoke later on app side"
/// pipeline. `Arc<dyn>` sharing across tasks requires `Send + Sync`.
#[async_trait]
pub trait AcknowledgementCommitCallback: Send + Sync + 'static {
    /// A callback method the user can implement to provide asynchronous
    /// handling of acknowledgement completion. This method will be called
    /// when the acknowledgement request sent to the server has been
    /// completed.
    ///
    /// Corresponds to Java's
    /// `void onComplete(Map<TopicIdPartition, Set<Long>> offsets, Exception exception)`.
    ///
    /// Returns `()` (not `Result`) because Java's `onComplete` is `void`: the
    /// acknowledgement has already been processed and the callback cannot fail
    /// it. The `error` parameter is informational, matching Java's pattern of
    /// "exception == null means success".
    ///
    /// # Arguments
    ///
    /// * `offsets` - a map of the offsets that this callback applies to.
    /// * `error` - `Some(&error)` if the acknowledgement failed, `None` if it
    ///   completed successfully. Possible errors mirror the Java javadoc:
    ///   authorization failures, [`Errors::InvalidRecordState`], leader
    ///   changes, disconnects, wakeup, or any other unrecoverable
    ///   [`KafkaError`]. Note that even a retriable error means the
    ///   acknowledgement could not be completed and the records must be
    ///   fetched again — the callback is called after any retries.
    ///
    /// [`Errors::InvalidRecordState`]: crate::common::protocol::errors::Errors::InvalidRecordState
    async fn on_complete(&self, offsets: &HashMap<TopicIdPartition, HashSet<i64>>, error: Option<&KafkaError>);
}
