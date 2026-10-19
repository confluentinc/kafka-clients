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

//! Producer callback interface.
//!
//! Translated from `org.apache.kafka.clients.producer.Callback`.

use crate::common::Error;
use crate::producer::RecordMetadata;

/// A callback interface that the user can implement to allow code to execute
/// when the request is complete. This callback will generally execute in the
/// background I/O task so it should be fast.
///
/// Java declares `Callback` as a functional interface, and a lambda is the
/// usual implementation. In Rust, [`callback::new`](new) turns a closure of
/// the method's shape into a `Callback`, inferring the closure's parameter
/// types:
///
/// ```
/// use confluent_kafka::producer::{callback, Callback};
///
/// let callback: Box<dyn Callback> = Box::new(callback::new(|metadata, error| {
///     if let Some(error) = error {
///         eprintln!("send failed: {error}");
///     } else if let Some(metadata) = metadata {
///         println!("sent at offset {}", metadata.offset());
///     }
/// }));
/// # callback.on_completion(None, None);
/// ```
///
/// Every `FnOnce(Option<&RecordMetadata>, Option<&Error>) + Send + Sync` also
/// implements `Callback` directly, but then the closure's parameter types must
/// be written out: Rust infers a closure's signature only from an `Fn` bound,
/// not from a trait it gets through a blanket impl.
///
/// [`on_completion`](Callback::on_completion) takes `self: Box<Self>`: the
/// producer invokes each record's callback exactly once, when its batch
/// completes, fails or is aborted, so the callback may consume what it
/// captures (a `oneshot::Sender`, for example).
#[doc(alias = "org.apache.kafka.clients.producer.Callback")]
pub trait Callback: Send + Sync {
    /// A callback method the user can implement to provide asynchronous
    /// handling of request completion. This method will be called when the
    /// record sent to the server has been acknowledged. When `error` is
    /// `Some`, `metadata` will contain the special -1 value for all fields. If
    /// the topic partition cannot be chosen, a -1 value will be assigned.
    ///
    /// * `metadata` - The metadata for the record that was sent (i.e. the
    ///   partition and offset). An empty metadata with -1 value for all fields
    ///   will be returned if an error occurred.
    /// * `error` - The error raised during processing of this record.
    ///   `None` if no error occurred. Possible errors include:
    ///
    ///   Non-retriable errors (fatal, the message will never be sent):
    ///   `InvalidTopicError`, `OffsetMetadataTooLarge`,
    ///   `RecordBatchTooLargeError`, `RecordTooLargeError`,
    ///   `UnknownServerError`, `UnknownProducerIdError`,
    ///   `InvalidProducerEpochError`, `AuthenticationError`,
    ///   `AuthorizationError`.
    ///
    ///   Retriable errors (transient, may be covered by increasing
    ///   `retries`): `CorruptRecordError`, `InvalidMetadataError`,
    ///   `NotEnoughReplicasAfterAppendError`, `NotEnoughReplicasError`,
    ///   `OffsetOutOfRangeError`, `TimeoutError`,
    ///   `UnknownTopicOrPartitionError`, `ProducerBufferExhaustedError`.
    #[doc(alias = "org.apache.kafka.clients.producer.Callback#onCompletion")]
    fn on_completion(self: Box<Self>, metadata: Option<&RecordMetadata>, error: Option<&Error>);
}

impl<F> Callback for F
where
    F: FnOnce(Option<&RecordMetadata>, Option<&Error>) + Send + Sync,
{
    fn on_completion(self: Box<Self>, metadata: Option<&RecordMetadata>, error: Option<&Error>) {
        (*self)(metadata, error)
    }
}

/// A [`Callback`] that calls `function`, standing in for a Java lambda
/// implementing `Callback`: `function` is
/// [`on_completion`](Callback::on_completion), and every other method keeps
/// its default. A method Java adds later as an overload of `onCompletion` gets
/// its own factory, `with_<parameter>`, taking a function of that overload's
/// shape.
// stands in for a Java lambda implementing `Callback` (CLAUDE.md §3)
#[doc(alias = "rust-only")]
pub fn new<F>(function: F) -> impl Callback
where
    F: FnOnce(Option<&RecordMetadata>, Option<&Error>) + Send + Sync,
{
    struct FunctionCallback<F>(F);

    impl<F> Callback for FunctionCallback<F>
    where
        F: FnOnce(Option<&RecordMetadata>, Option<&Error>) + Send + Sync,
    {
        fn on_completion(self: Box<Self>, metadata: Option<&RecordMetadata>, error: Option<&Error>) {
            (self.0)(metadata, error)
        }
    }

    FunctionCallback(function)
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use super::*;
    use crate::common::TopicPartition;

    #[test]
    fn new_calls_the_function_with_the_completion_arguments() {
        let seen: Arc<Mutex<Option<(i64, bool)>>> = Arc::new(Mutex::new(None));
        let sink = Arc::clone(&seen);
        let callback: Box<dyn Callback> = Box::new(new(move |metadata, error| {
            *sink.lock().unwrap() = Some((metadata.map_or(-2, RecordMetadata::offset), error.is_some()));
        }));
        let metadata = RecordMetadata::new(TopicPartition::new("t", 0), 7, 0, 0, 0, 0);
        callback.on_completion(Some(&metadata), None);
        assert_eq!(*seen.lock().unwrap(), Some((7, false)));
    }

    #[test]
    fn new_passes_the_error() {
        let seen = Arc::new(Mutex::new(None));
        let sink = Arc::clone(&seen);
        let callback: Box<dyn Callback> = Box::new(new(move |metadata, error: Option<&Error>| {
            *sink.lock().unwrap() = Some((metadata.is_some(), error.map(ToString::to_string)));
        }));
        callback.on_completion(None, Some(&Error::local_illegal_state("boom")));
        let (has_metadata, error) = seen.lock().unwrap().take().unwrap();
        assert!(!has_metadata);
        assert!(error.unwrap().contains("boom"));
    }
}
