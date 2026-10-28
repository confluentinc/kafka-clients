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

//! Consumer interceptor plugin trait.
//!
//! Translated from `org.apache.kafka.clients.consumer.ConsumerInterceptor`.

use std::collections::HashMap;

use crate::common::TopicPartition;
use crate::consumer::{ConsumerRecords, OffsetAndMetadata};

/// A plugin interface that allows you to intercept (and possibly mutate)
/// records received by the consumer. A primary use-case is for third-party
/// components to hook into the consumer applications for custom monitoring,
/// logging, etc.
///
/// This trait will get consumer config properties via
/// [`configure`](Self::configure), including the client id assigned by
/// `KafkaConsumer` if not specified in the consumer config. The interceptor
/// implementation needs to be aware that it will be sharing consumer config
/// namespace with other interceptors and serializers, and ensure that there are
/// no conflicts.
///
/// Errors raised by `ConsumerInterceptor` methods will be caught, logged, but
/// not propagated further. As a result, if the user configures the interceptor
/// with the wrong key and value type parameters, the consumer will not return
/// an error, just log the errors.
///
/// `ConsumerInterceptor` callbacks are called from the same task that invokes
/// [`Consumer::poll`](crate::consumer::Consumer::poll).
///
/// # Translation notes
///
/// The methods return no error: a panic is what Java's thrown exception
/// becomes. `KafkaConsumer` takes no interceptors yet, and an interceptor
/// implementing `ClusterResourceListener` or `Monitorable` gets no callback.
// Bounds: `Send + 'static`, not `Send + Sync`. The interceptor is stored inside
// `ConsumerInterceptors` as `Vec<Box<dyn ConsumerInterceptor<K, V>>>` with a
// single owner, so `Sync` is not required; dropping it lets users use interior
// mutability (`RefCell`, `Cell`) without a `Mutex`. The chain runs per batch
// inside `poll()`, not per record, and `#[async_trait]` would change Java's
// synchronous contract.
#[doc(alias = "org.apache.kafka.clients.consumer.ConsumerInterceptor")]
pub trait ConsumerInterceptor<K, V>: Send + 'static {
    /// This is called just before the records are returned by
    /// [`Consumer::poll`](crate::consumer::Consumer::poll).
    ///
    /// This method is allowed to modify consumer records, in which case the new
    /// records will be returned. There is no limitation on number of records
    /// that could be returned from this method. I.e., the interceptor can
    /// filter the records or generate new records.
    ///
    /// Any panic raised by this method will be caught by the caller, logged,
    /// but not propagated to the client.
    ///
    /// Since the consumer may run multiple interceptors, a particular
    /// interceptor's `on_consume()` callback will be called in the order the
    /// interceptors are configured. The first interceptor in the list gets the
    /// consumed records, the following interceptor will be passed the records
    /// returned by the previous interceptor, and so on. Since interceptors are
    /// allowed to modify records, interceptors may potentially get the records
    /// already modified by other interceptors. However, building a pipeline of
    /// mutable interceptors that depend on the output of the previous
    /// interceptor is discouraged, because of potential side-effects caused by
    /// interceptors potentially failing to modify the record and panicking. If
    /// one of the interceptors in the list panics from `on_consume()`, the panic
    /// is caught, logged, and the next interceptor is called with the records
    /// returned by the last successful interceptor in the list, or otherwise the
    /// original consumed records.
    ///
    /// # Arguments
    ///
    /// - `records` - records to be consumed by the client or records returned by
    ///   the previous interceptors in the list.
    ///
    /// # Translation notes
    ///
    /// Java returns the records; here the interceptor modifies `records` in
    /// place, or replaces the batch with `*records = new_batch;`. Since `&mut`
    /// lets a panicking interceptor leave a half-modified batch, build the new
    /// batch first and assign it last: then a panic leaves `*records` unchanged,
    /// as Java's immutable `ConsumerRecords` is. Do not `std::mem::take` the
    /// batch before work that can panic, or the next interceptor gets an empty
    /// batch.
    // The `&mut` form, rather than taking and returning ownership, avoids a
    // `K: Clone, V: Clone` bound that would otherwise propagate through
    // `ConsumerInterceptors::on_consume` into the public `Consumer<K, V>` trait.
    // Java's `ConsumerRecords` is structurally immutable (`private final`
    // fields, unmodifiable views), so in Java the "previous-good batch"
    // guarantee of `ConsumerInterceptors#onConsume` is a property of the type;
    // here it is a discipline of the interceptor. Interior-mutable state of the
    // interceptor is its own responsibility on panic, as in Java.
    #[doc(alias = "org.apache.kafka.clients.consumer.ConsumerInterceptor#onConsume")]
    fn on_consume(&self, records: &mut ConsumerRecords<K, V>);

    /// This is called when offsets get committed.
    ///
    /// Any panic raised by this method will be ignored by the caller.
    ///
    /// # Arguments
    ///
    /// - `offsets` - A map of offsets by partition with associated metadata
    #[doc(alias = "org.apache.kafka.clients.consumer.ConsumerInterceptor#onCommit")]
    fn on_commit(&self, offsets: &HashMap<TopicPartition, OffsetAndMetadata>);

    /// Configure this class with the given key-value pairs.
    ///
    /// # Arguments
    ///
    /// - `configs` - The configuration
    #[doc(alias = "org.apache.kafka.common.Configurable#configure")]
    fn configure(&mut self, configs: &HashMap<String, String>) {
        let _ = configs;
    }

    /// This is called when interceptor is closed.
    #[doc(alias = "org.apache.kafka.clients.consumer.ConsumerInterceptor#close")]
    fn close(&mut self) {}
}
