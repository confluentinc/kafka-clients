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

//! Callback interface notified when an async offset commit completes.
//!
//! Translated from `org.apache.kafka.clients.consumer.OffsetCommitCallback`.

use std::collections::HashMap;

use async_trait::async_trait;

use crate::common::{KafkaError, TopicPartition};
use crate::consumer::OffsetAndMetadata;

/// A callback interface that the user can implement to trigger custom actions
/// when a commit request completes.
///
/// Corresponds to Java's
/// `org.apache.kafka.clients.consumer.OffsetCommitCallback`.
///
/// # Invocation model
///
/// Per `consumer-threading.md` §31, the callback executes on the **caller's
/// task** that is currently inside `poll()` / `commit_*()` / `close()` —
/// never on the background task. Matches Java's contract: "the callback may
/// be executed in any thread calling poll()".
///
/// # Bounds: `Send + Sync + 'static`
///
/// The callback travels through events on the background task and is
/// invoked on the app task after the background task has already moved on.
/// `Arc<dyn>` storage is required for the symmetric "send through channel,
/// invoke later on app side" pipeline used by `process_background_events`.
/// `Box<dyn>` would forbid the clone and the design collapses.
#[async_trait]
pub trait OffsetCommitCallback: Send + Sync + 'static {
    /// A callback method the user can implement to provide asynchronous
    /// handling of commit request completion. This method will be called
    /// when the commit request sent to the server has been acknowledged.
    ///
    /// Corresponds to Java's
    /// `void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets,
    ///                  Exception exception)`.
    ///
    /// Returns `()` (not `Result`) because Java's `onComplete` is `void` and
    /// does not allow the callback to fail the commit — the commit has
    /// already happened. The `error` parameter is informational and matches
    /// Java's pattern of "exception == null means success".
    ///
    /// # Arguments
    ///
    /// * `offsets` - the offsets and associated metadata that this callback
    ///   applies to
    /// * `error` - `Some(&error)` if the commit failed, `None` if it
    ///   completed successfully
    async fn on_complete(&self, offsets: &HashMap<TopicPartition, OffsetAndMetadata>, error: Option<&KafkaError>);
}
