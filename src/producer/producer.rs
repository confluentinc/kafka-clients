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

//! Translation of `org.apache.kafka.clients.producer.Producer`.
//!
//! Java's `interface Producer<K, V> extends Closeable` becomes a Rust
//! trait with the same generics. Per CLAUDE.md rule 9.1 every method
//! that blocks in Java becomes `async fn` in Rust.
//!
//! # Async-fn-in-trait and dyn-compatibility
//!
//! The trait uses bare `async fn` signatures (Rust 1.75+). This is the
//! preferred shape per CLAUDE.md rule 11 — no per-call
//! `Pin<Box<dyn Future>>` allocation when callers hold a concrete
//! `KafkaProducer<K, V>` and call methods directly. The cost is that
//! the trait is **not dyn-compatible**: `Box<dyn Producer<K, V>>` does
//! not compile. This mirrors the Java surface where most callers hold
//! a concrete `KafkaProducer` and `Producer` exists primarily for
//! `MockProducer` substitution in tests. When `MockProducer` is
//! translated (out of milestone), the same trait will be used with
//! generics rather than dyn-dispatch — `fn run<P: Producer<K, V>>(p:
//! P)` instead of `fn run(p: &dyn Producer<K, V>)`.
//!
//! # `send` semantics and the Java `Future` collapse
//!
//! Java's `Future<RecordMetadata> send(...)` enqueues the record and
//! returns a `Future` that the caller can either ignore (fire and
//! forget) or block on later. The Rust analogue collapses both into
//! one `async fn` that resolves only after the broker acknowledges
//! (or fails) the record. Callers wanting fire-and-forget can
//! [`tokio::spawn`] the returned future themselves. This trade-off
//! keeps the public trait method `async fn`-shaped (no
//! `Pin<Box<dyn Future>>`) while still expressing the full
//! enqueue → ack flow as a single awaitable.
//!
//! # Milestone-1 stubs
//!
//! Every transactional method on the trait (`init_transactions`,
//! `begin_transaction`, `commit_transaction`, `abort_transaction`)
//! returns [`KafkaError::UnsupportedOperation`] in this milestone. The
//! `transactional.id` config is rejected by [`ProducerConfig`] post-
//! processing (see Phase 7a), so any caller would already have failed
//! at construction; the trait methods exist to satisfy the Java
//! interface shape and keep the rejection surface uniform.
//!
//! `client_instance_id` likewise returns [`KafkaError::UnsupportedOperation`]
//! because client-telemetry is not implemented in Milestone-1.
//!
//! # Methods deferred from this trait
//!
//! Three Java interface methods are intentionally **not** declared on
//! this trait yet, because they depend on types that have not been
//! translated:
//!
//! * `sendOffsetsToTransaction(Map<TopicPartition, OffsetAndMetadata>,
//!   ConsumerGroupMetadata)` — depends on `OffsetAndMetadata` and
//!   `ConsumerGroupMetadata` from the consumer package, neither of
//!   which has been translated yet. Phase 9 (transactional producer)
//!   will add the consumer types and reintroduce the method here.
//! * `registerMetricForSubscription(KafkaMetric)` and
//!   `unregisterMetricFromSubscription(KafkaMetric)` — depend on
//!   `org.apache.kafka.common.metrics.KafkaMetric`, which has not been
//!   translated yet. The whole metrics surface is stubbed in
//!   Milestone-1; once metrics are translated these methods will be
//!   added and gated on the same Milestone-1 stub pattern as
//!   [`Producer::metrics`].
//!
//! [`tokio::spawn`]: https://docs.rs/tokio/latest/tokio/task/fn.spawn.html
//! [`ProducerConfig`]: crate::producer::ProducerConfig

use std::collections::HashMap;
use std::time::Duration;

use crate::common::errors::KafkaError;
use crate::common::partition_info::PartitionInfo;
use crate::common::uuid::Uuid;
use crate::producer::callback::Callback;
use crate::producer::producer_record::ProducerRecord;
use crate::producer::record_metadata::RecordMetadata;

/// Placeholder return type for [`Producer::metrics`] in Milestone-1.
///
/// The Java method returns `Map<MetricName, ? extends Metric>` from
/// `org.apache.kafka.common.metrics`. The Rust translation of those
/// types is deferred — when they land, this alias will be replaced
/// with `HashMap<MetricName, KafkaMetric>` (or the equivalent owned
/// view) and existing call sites will keep compiling because they
/// only inspect `is_empty()` / `len()`.
pub type ProducerMetrics = HashMap<String, ()>;

/// The interface for [`KafkaProducer`].
///
/// Mirrors `org.apache.kafka.clients.producer.Producer<K, V>`. See
/// the module-level docs for the dyn-compatibility and `send` semantics
/// trade-offs.
///
/// The trait is `Send + Sync` so producers can be shared across
/// Tokio tasks — Java's `KafkaProducer` is documented as thread-safe
/// and the same expectation carries over.
///
/// [`KafkaProducer`]: https://kafka.apache.org/40/javadoc/org/apache/kafka/clients/producer/KafkaProducer.html
pub trait Producer<K, V>: Send + Sync {
    /// Needs to be called before any other methods when the
    /// `transactional.id` is set in the configuration.
    ///
    /// **Milestone-1**: Always returns
    /// [`KafkaError::UnsupportedOperation`] because transactions are
    /// not implemented. The `transactional.id` config key is also
    /// rejected by `ProducerConfig::new`, so a configured producer
    /// will never reach this method in normal use.
    ///
    /// See `KafkaProducer#initTransactions()`.
    ///
    /// Per CLAUDE.md rule 9.1, Java's blocking `void initTransactions()`
    /// becomes `async fn` in Rust. Milestone-1 always rejects
    /// immediately, but the signature is future-proof for when the
    /// transaction coordinator round-trip lands in Phase 9.
    fn init_transactions(&self) -> impl std::future::Future<Output = Result<(), KafkaError>> + Send;

    /// Should be called before the start of each new transaction.
    ///
    /// **Milestone-1**: Always returns
    /// [`KafkaError::UnsupportedOperation`].
    ///
    /// See `KafkaProducer#beginTransaction()`.
    fn begin_transaction(&self) -> impl std::future::Future<Output = Result<(), KafkaError>> + Send;

    /// Commits the ongoing transaction.
    ///
    /// **Milestone-1**: Always returns
    /// [`KafkaError::UnsupportedOperation`].
    ///
    /// See `KafkaProducer#commitTransaction()`.
    fn commit_transaction(&self) -> impl std::future::Future<Output = Result<(), KafkaError>> + Send;

    /// Aborts the ongoing transaction.
    ///
    /// **Milestone-1**: Always returns
    /// [`KafkaError::UnsupportedOperation`].
    ///
    /// See `KafkaProducer#abortTransaction()`.
    fn abort_transaction(&self) -> impl std::future::Future<Output = Result<(), KafkaError>> + Send;

    /// Asynchronously send a record to a topic.
    ///
    /// Java's `send()` returns immediately with a `Future`. The Rust
    /// translation collapses the enqueue and the broker-acknowledgement
    /// into one `async fn` (see module-level docs for the rationale):
    /// awaiting this method awaits both. Callers wanting fire-and-
    /// forget should `tokio::spawn` the returned future themselves.
    ///
    /// See `KafkaProducer#send(ProducerRecord)`.
    fn send(
        &self,
        record: ProducerRecord<K, V>,
    ) -> impl std::future::Future<Output = Result<RecordMetadata, KafkaError>> + Send;

    /// Asynchronously send a record with a user-supplied callback.
    ///
    /// The callback is invoked exactly once per record at the same
    /// lifecycle point as Java's `Callback.onCompletion` — after the
    /// broker has acknowledged the record (success path) or after a
    /// final non-retriable error (failure path). See
    /// [`Callback`](crate::producer::Callback) for the success/failure
    /// contract.
    ///
    /// See `KafkaProducer#send(ProducerRecord, Callback)`.
    fn send_with_callback(
        &self,
        record: ProducerRecord<K, V>,
        callback: Option<Box<dyn Callback>>,
    ) -> impl std::future::Future<Output = Result<RecordMetadata, KafkaError>> + Send;

    /// Make all buffered records immediately available to send (even
    /// if `linger.ms` is greater than 0) and block until completion of
    /// the requests associated with these records.
    ///
    /// See `KafkaProducer#flush()`.
    fn flush(&self) -> impl std::future::Future<Output = Result<(), KafkaError>> + Send;

    /// Get the partition metadata for the given topic. Java blocks
    /// while waiting for metadata; the Rust translation is `async fn`.
    ///
    /// See `KafkaProducer#partitionsFor(String)`.
    fn partitions_for(
        &self,
        topic: &str,
    ) -> impl std::future::Future<Output = Result<Vec<PartitionInfo>, KafkaError>> + Send;

    /// Get the full set of internal metrics maintained by the producer.
    ///
    /// **Milestone-1**: Returns an empty map. The proper translation
    /// of `MetricName` / `KafkaMetric` is deferred — see
    /// [`ProducerMetrics`] for the placeholder type alias.
    ///
    /// See `KafkaProducer#metrics()`.
    fn metrics(&self) -> ProducerMetrics;

    /// Determines the client's unique client instance ID used for
    /// telemetry. Java waits up to `timeout` for the producer to
    /// complete the request.
    ///
    /// **Milestone-1**: Always returns
    /// [`KafkaError::UnsupportedOperation`] with the message "Client
    /// telemetry is not implemented in Milestone-1." Client
    /// telemetry is gated on the `enable.metrics.push` config key
    /// which Phase 7a's `ProducerConfig` accepts but does not act on.
    ///
    /// See `KafkaProducer#clientInstanceId(Duration)`.
    fn client_instance_id(
        &self,
        timeout: Duration,
    ) -> impl std::future::Future<Output = Result<Uuid, KafkaError>> + Send;

    /// Close this producer, blocking until all previously-sent records
    /// complete.
    ///
    /// See `KafkaProducer#close()`.
    fn close(&self) -> impl std::future::Future<Output = Result<(), KafkaError>> + Send;

    /// Close this producer, waiting up to `timeout` for in-flight
    /// records to complete.
    ///
    /// See `KafkaProducer#close(Duration)`.
    fn close_with_timeout(&self, timeout: Duration)
    -> impl std::future::Future<Output = Result<(), KafkaError>> + Send;
}

#[cfg(test)]
mod tests {
    //! Compile-only checks for the [`Producer`] trait shape.
    //!
    //! No `KafkaProducer` exists yet (Phase 7c). These tests assert
    //! the trait declaration is well-formed and exercise the
    //! `UnsupportedOperation` error message contract via a hand-rolled
    //! stub implementation.

    use super::*;
    use crate::common::header::{Headers, RecordHeader};
    use std::sync::Arc;

    /// Minimal stub implementor used to verify the trait compiles and
    /// that callers can hold one as `&P: Producer<K, V>`. Mirrors the
    /// Milestone-1 contract: every method either returns the
    /// `UnsupportedOperation` error or a plausible empty/default value.
    struct StubProducer;

    impl Producer<Vec<u8>, Vec<u8>> for StubProducer {
        async fn init_transactions(&self) -> Result<(), KafkaError> {
            Err(KafkaError::UnsupportedOperation(MS1_TXN_MSG.into()))
        }

        async fn begin_transaction(&self) -> Result<(), KafkaError> {
            Err(KafkaError::UnsupportedOperation(MS1_TXN_MSG.into()))
        }

        async fn commit_transaction(&self) -> Result<(), KafkaError> {
            Err(KafkaError::UnsupportedOperation(MS1_TXN_MSG.into()))
        }

        async fn abort_transaction(&self) -> Result<(), KafkaError> {
            Err(KafkaError::UnsupportedOperation(MS1_TXN_MSG.into()))
        }

        async fn send(&self, _record: ProducerRecord<Vec<u8>, Vec<u8>>) -> Result<RecordMetadata, KafkaError> {
            Err(KafkaError::UnsupportedOperation("stub".into()))
        }

        async fn send_with_callback(
            &self,
            _record: ProducerRecord<Vec<u8>, Vec<u8>>,
            _callback: Option<Box<dyn Callback>>,
        ) -> Result<RecordMetadata, KafkaError> {
            Err(KafkaError::UnsupportedOperation("stub".into()))
        }

        async fn flush(&self) -> Result<(), KafkaError> {
            Ok(())
        }

        async fn partitions_for(&self, _topic: &str) -> Result<Vec<PartitionInfo>, KafkaError> {
            Ok(Vec::new())
        }

        fn metrics(&self) -> ProducerMetrics {
            ProducerMetrics::new()
        }

        async fn client_instance_id(&self, _timeout: Duration) -> Result<Uuid, KafkaError> {
            Err(KafkaError::UnsupportedOperation(MS1_TELEMETRY_MSG.into()))
        }

        async fn close(&self) -> Result<(), KafkaError> {
            Ok(())
        }

        async fn close_with_timeout(&self, _timeout: Duration) -> Result<(), KafkaError> {
            Ok(())
        }
    }

    const MS1_TXN_MSG: &str = "Transactional producer is not supported in Milestone-1.";
    const MS1_TELEMETRY_MSG: &str = "Client telemetry is not implemented in Milestone-1.";

    /// Verifies the trait compiles and a generic function over `P:
    /// Producer<K, V>` can dispatch its sync method (`metrics`).
    #[test]
    fn trait_is_implementable_and_dispatches_sync() {
        fn check<P: Producer<Vec<u8>, Vec<u8>>>(p: &P) -> Result<(), KafkaError> {
            // The only sync method on the trait — metrics returns the empty stub.
            assert!(p.metrics().is_empty());
            Ok(())
        }
        check(&StubProducer).unwrap();
    }

    /// Verifies the async-fn methods dispatch through the trait.
    #[tokio::test]
    async fn async_methods_dispatch_through_trait() {
        let p = StubProducer;

        // Every transactional method should reject in Milestone-1.
        assert!(matches!(p.init_transactions().await, Err(KafkaError::UnsupportedOperation(_))));
        assert!(matches!(p.begin_transaction().await, Err(KafkaError::UnsupportedOperation(_))));
        assert!(matches!(p.commit_transaction().await, Err(KafkaError::UnsupportedOperation(_))));
        assert!(matches!(p.abort_transaction().await, Err(KafkaError::UnsupportedOperation(_))));

        let record = ProducerRecord::<Vec<u8>, Vec<u8>>::with_partition("t", Some(0), None, None).unwrap();
        // Stub returns Err for `send` so just verify dispatch works.
        assert!(p.send(record).await.is_err());

        let record = ProducerRecord::<Vec<u8>, Vec<u8>>::with_partition("t", Some(0), None, None).unwrap();
        let cb: Box<dyn Callback> = Box::new(|_md: Option<&RecordMetadata>, _err: Option<&KafkaError>| {});
        assert!(p.send_with_callback(record, Some(cb)).await.is_err());

        // flush / partitions_for / close_with_timeout return Ok stubs.
        assert!(p.flush().await.is_ok());
        assert!(p.partitions_for("t").await.unwrap().is_empty());
        assert!(p.close_with_timeout(Duration::from_secs(1)).await.is_ok());
        assert!(p.close().await.is_ok());

        // client_instance_id is the telemetry-not-implemented path.
        let err = p.client_instance_id(Duration::from_secs(1)).await.unwrap_err();
        assert!(matches!(err, KafkaError::UnsupportedOperation(_)));
        assert_eq!(err.message(), MS1_TELEMETRY_MSG);
    }

    /// Spot check that a record with a header round-trips through
    /// `send` without compile errors — guards against a future
    /// signature regression that would force callers to clone keys/
    /// values needlessly. The stub `send` always returns Err so we
    /// check only the type.
    #[tokio::test]
    async fn send_accepts_record_with_headers() {
        let mut record = ProducerRecord::<Vec<u8>, Vec<u8>>::with_partition_and_headers(
            "t",
            Some(0),
            Some(b"key".to_vec()),
            Some(b"value".to_vec()),
            Some(vec![RecordHeader::new("h", Some(b"v"))]),
        )
        .unwrap();
        record.headers_mut().add(RecordHeader::new("k", Some(b"v"))).unwrap();
        let p = Arc::new(StubProducer);
        // Just ensure the call type-checks against the trait method.
        let _ = p.send(record).await;
    }
}
