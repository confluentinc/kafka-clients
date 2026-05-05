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

//! Translation of `org.apache.kafka.clients.producer.Callback`.
//!
//! Java's `@FunctionalInterface Callback { void onCompletion(metadata, exception); }`
//! becomes a Rust trait object. The trait is invoked from the Sender task
//! after the broker acknowledges (or fails) the produce request,
//! mirroring `ProducerBatch.completeFutureAndFireCallbacks` in Java.
//!
//! Per CLAUDE.md rule 9.5, this callback is invoked at the same lifecycle
//! point as Java: inside the sender task, after the partition's record
//! is acknowledged/failed, before completing the user-facing future.

use crate::common::errors::KafkaError;
use crate::producer::record_metadata::RecordMetadata;

/// A callback the user can implement to allow code to execute when the
/// request is complete. Generally executes inside the producer's sender
/// task, so it should be fast.
///
/// `Send + Sync` because the sender task is independent of the calling
/// task; we move the callback into the producer batch when `send` is
/// invoked. `'static` because callbacks are stored alongside the batch
/// and outlive the immediate call frame.
pub trait Callback: Send + Sync + 'static {
    /// Invoked when the record sent to the server has been acknowledged.
    ///
    /// On error, `metadata` is `None` (Java passes a `RecordMetadata`
    /// with `-1` for all fields; Rust omits the synthetic instance and
    /// passes `None` so callers do not have to inspect every field for
    /// the sentinel). On success, `error` is `None`.
    ///
    /// Possible non-retriable error variants (the message will never be
    /// sent): `InvalidTopic`, `RecordTooLarge`, `UnknownServer`,
    /// `UnknownProducerId`, `InvalidProducerEpoch`, `Authentication`,
    /// `Authorization`.
    ///
    /// Possible retriable error variants (transient — may be covered by
    /// raising `retries`): `CorruptRecord`, `Network`,
    /// `LeaderNotAvailable`, `NotLeaderOrFollower`,
    /// `UnknownTopicOrPartition`, `KafkaStorage`, `NotEnoughReplicas`,
    /// `NotEnoughReplicasAfterAppend`, `Timeout`, `BufferExhausted`.
    fn on_completion(&self, metadata: Option<&RecordMetadata>, error: Option<&KafkaError>);
}

/// Blanket impl that lets users pass any `Fn(Option<&RecordMetadata>, Option<&KafkaError>)`
/// closure as a `Callback`, mirroring the convenience of Java 8 method
/// references / lambdas implementing the `@FunctionalInterface`.
impl<F> Callback for F
where
    F: Fn(Option<&RecordMetadata>, Option<&KafkaError>) + Send + Sync + 'static,
{
    fn on_completion(&self, metadata: Option<&RecordMetadata>, error: Option<&KafkaError>) {
        self(metadata, error);
    }
}
