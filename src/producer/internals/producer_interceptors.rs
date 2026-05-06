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

//! Translation of `org.apache.kafka.clients.producer.internals.ProducerInterceptors`.

#![allow(dead_code)] // Wired up in Phase 7 (KafkaProducer shell).

use std::panic::{AssertUnwindSafe, catch_unwind};

use crate::common::errors::KafkaError;
use crate::common::header::RecordHeaders;
use crate::common::record::record_batch::NO_TIMESTAMP;
use crate::common::topic_partition::TopicPartition;
use crate::producer::ProducerInterceptor;
use crate::producer::ProducerRecord;
use crate::producer::RecordMetadata;

/// A container that holds the list of [`ProducerInterceptor`] and wraps
/// calls to the chain of custom interceptors.
///
/// Mirrors `org.apache.kafka.clients.producer.internals.ProducerInterceptors`.
///
/// # Translation notes
///
/// * Java exceptions thrown by an interceptor are caught and logged; the
///   chain continues with the previous record. The Rust translation uses
///   [`std::panic::catch_unwind`] with [`AssertUnwindSafe`] to mirror
///   that behaviour. The pattern matches `ProducerBatch::
///   complete_future_and_fire_callbacks` (Phase 6b).
/// * Java requires the chain entry record to be passed by reference and
///   each interceptor returns a (possibly new) record. The Rust
///   translation requires `K: Clone, V: Clone` so we can pass a
///   throw-away clone into each interceptor — if the interceptor
///   panics, we still hold the previous good record.
/// * Java's `Plugin<T>` wrapper is a metrics shim. The Rust translation
///   skips the metrics layer (PLAN.md Phase-6 skip note); we hold the
///   interceptors directly as `Vec<Box<dyn ProducerInterceptor<K,V>>>`.
pub struct ProducerInterceptors<K, V> {
    interceptors: Vec<Box<dyn ProducerInterceptor<K, V>>>,
}

impl<K, V> ProducerInterceptors<K, V> {
    /// Construct a new container wrapping the given interceptor list.
    /// Java's second `Metrics` argument is dropped — the metrics
    /// integration is a milestone-deferred concern (PLAN.md Phase 6
    /// skip note).
    pub fn new(interceptors: Vec<Box<dyn ProducerInterceptor<K, V>>>) -> Self {
        ProducerInterceptors { interceptors }
    }

    /// True iff there are no registered interceptors. Useful on the hot
    /// path so callers can skip the indirection entirely.
    pub fn is_empty(&self) -> bool {
        self.interceptors.is_empty()
    }
}

impl<K: Clone, V: Clone> ProducerInterceptors<K, V> {
    /// Called when client sends the record to the producer, before key
    /// and value get serialized.
    ///
    /// Calls [`ProducerInterceptor::on_send`] on each interceptor in
    /// order. The `ProducerRecord` returned from the first interceptor's
    /// `on_send` is passed to the second interceptor's `on_send`, and
    /// so on. The record returned from the last interceptor is returned
    /// from this method.
    ///
    /// Mirrors Java's "exceptions are caught and ignored; the next
    /// interceptor receives the record returned by the previous
    /// successful interceptor". In Rust, panics from an interceptor's
    /// `on_send` are caught via [`catch_unwind`] and logged; the
    /// previous good record is forwarded unchanged.
    pub fn on_send(&self, record: ProducerRecord<K, V>) -> ProducerRecord<K, V> {
        let mut intercept_record = record;
        for interceptor in &self.interceptors {
            // Pass a clone into the interceptor so that, if it panics,
            // we still hold the previous good record. (Java mutates a
            // reference, so a panic leaves the reference unchanged.)
            let candidate = intercept_record.clone();
            // Capture topic/partition for the warn-log fallback in
            // case the interceptor panics — `intercept_record` gets
            // moved if we replace it on success.
            let topic = intercept_record.topic().to_string();
            let partition = intercept_record.partition();
            let result = catch_unwind(AssertUnwindSafe(|| interceptor.on_send(candidate)));
            match result {
                Ok(new_record) => intercept_record = new_record,
                Err(panic_payload) => {
                    let descr = describe_panic(panic_payload.as_ref());
                    log::warn!(
                        "Error executing interceptor onSend callback for topic: {}, partition: {:?}: {}",
                        topic,
                        partition,
                        descr,
                    );
                    // intercept_record retained from the previous
                    // iteration (or the original).
                },
            }
        }
        intercept_record
    }
}

impl<K, V> ProducerInterceptors<K, V> {
    /// Called when the record sent to the server has been acknowledged,
    /// or when sending the record fails before it gets sent to the
    /// server. Calls
    /// [`ProducerInterceptor::on_acknowledgement`] for each interceptor.
    ///
    /// This method does not propagate exceptions. Panics from any
    /// interceptor method are caught and logged via [`catch_unwind`].
    pub fn on_acknowledgement(
        &self,
        metadata: Option<&RecordMetadata>,
        exception: Option<&KafkaError>,
        headers: &RecordHeaders,
    ) {
        for interceptor in &self.interceptors {
            let result = catch_unwind(AssertUnwindSafe(|| {
                interceptor.on_acknowledgement(metadata, exception, headers);
            }));
            if let Err(panic_payload) = result {
                let descr = describe_panic(panic_payload.as_ref());
                log::warn!("Error executing interceptor onAcknowledgement callback: {}", descr);
            }
        }
    }
}

impl<K: Clone, V: Clone> ProducerInterceptors<K, V> {
    /// Called when sending the record fails inside `on_send` or before
    /// the record reaches the server. Mirrors
    /// `org.apache.kafka.clients.producer.internals.ProducerInterceptors::onSendError`.
    ///
    /// `record` may be `None` (Java passed a `null` record). If both
    /// `record` and `intercept_topic_partition` are `None`, the
    /// interceptor receives a `None` metadata.
    pub fn on_send_error(
        &self,
        record: Option<&ProducerRecord<K, V>>,
        intercept_topic_partition: Option<TopicPartition>,
        exception: &KafkaError,
    ) {
        for interceptor in &self.interceptors {
            let result = catch_unwind(AssertUnwindSafe(|| {
                // Java derives headers either from the record (with a
                // read-only snapshot) or constructs an empty one if the
                // record is null. Java also makes a copy of mutable
                // headers and marks the copy read-only — we mirror that
                // so the interceptor cannot accidentally mutate the
                // origin record's headers.
                let headers = match record {
                    Some(r) => {
                        if r.headers().is_read_only() {
                            r.headers().clone()
                        } else {
                            let mut copy = r.headers().clone();
                            copy.set_read_only();
                            copy
                        }
                    },
                    None => RecordHeaders::new(),
                };

                if record.is_none() && intercept_topic_partition.is_none() {
                    interceptor.on_acknowledgement(None, Some(exception), &headers);
                } else {
                    let tp = intercept_topic_partition.clone().unwrap_or_else(|| {
                        Self::extract_topic_partition(
                            record.expect("record was None but intercept_topic_partition was Some"),
                        )
                    });
                    let metadata = RecordMetadata::new(tp, -1, -1, NO_TIMESTAMP, -1, -1);
                    interceptor.on_acknowledgement(Some(&metadata), Some(exception), &headers);
                }
            }));
            if let Err(panic_payload) = result {
                let descr = describe_panic(panic_payload.as_ref());
                log::warn!("Error executing interceptor onAcknowledgement callback: {}", descr);
            }
        }
    }

    /// Helper. Mirrors Java's static `extractTopicPartition`.
    pub fn extract_topic_partition(record: &ProducerRecord<K, V>) -> TopicPartition {
        let partition = record.partition().unwrap_or(RecordMetadata::UNKNOWN_PARTITION);
        TopicPartition::new(record.topic_arc().clone(), partition)
    }
}

impl<K, V> ProducerInterceptors<K, V> {
    /// Closes every interceptor in the container. Mirrors Java's
    /// `close()`. Panics from individual interceptors are caught and
    /// logged.
    pub fn close(&mut self) {
        for interceptor in &mut self.interceptors {
            let result = catch_unwind(AssertUnwindSafe(|| interceptor.close()));
            if let Err(panic_payload) = result {
                let descr = describe_panic(panic_payload.as_ref());
                log::error!("Failed to close producer interceptor: {}", descr);
            }
        }
    }
}

/// Best-effort string description of a panic payload — mirrors the
/// pattern used by [`ProducerBatch::complete_future_and_fire_callbacks`].
fn describe_panic(payload: &(dyn std::any::Any + Send)) -> String {
    if let Some(s) = payload.downcast_ref::<&'static str>() {
        (*s).to_string()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        "<non-string panic payload>".to_string()
    }
}

#[cfg(test)]
mod tests {
    //! Translation of `ProducerInterceptorsTest`.

    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicI32, Ordering};

    use super::*;
    use crate::common::header::RecordHeaders;
    use crate::common::topic_partition::TopicPartition;

    /// Counters shared by all interceptor instances within a single
    /// test, mirroring the Java `ProducerInterceptorsTest` instance
    /// fields.
    #[derive(Default)]
    struct Counters {
        on_ack_count: AtomicI32,
        on_error_ack_count: AtomicI32,
        on_error_ack_with_topic_set_count: AtomicI32,
        on_error_ack_with_topic_partition_set_count: AtomicI32,
        on_send_count: AtomicI32,
    }

    /// Shared injection-flag handle. Cloned into the interceptor and
    /// the test harness so the test can flip the flags after the
    /// interceptor has been moved into the chain.
    #[derive(Default, Clone)]
    struct InjectionFlags {
        throw_on_send: Arc<AtomicBool>,
        throw_on_ack: Arc<AtomicBool>,
    }

    impl InjectionFlags {
        fn inject_on_send_error(&self, on: bool) {
            self.throw_on_send.store(on, Ordering::SeqCst);
        }

        fn inject_on_acknowledgement_error(&self, on: bool) {
            self.throw_on_ack.store(on, Ordering::SeqCst);
        }
    }

    /// Mirrors Java's `AppendProducerInterceptor` — appends a string to
    /// the record value and exercises the 2-arg
    /// `onAcknowledgement(metadata, exception)` overload (no headers).
    struct AppendProducerInterceptor {
        append_str: String,
        flags: InjectionFlags,
        counters: Arc<Counters>,
    }

    impl AppendProducerInterceptor {
        fn new(append_str: &str, flags: InjectionFlags, counters: Arc<Counters>) -> Self {
            Self { append_str: append_str.to_string(), flags, counters }
        }
    }

    impl ProducerInterceptor<i32, String> for AppendProducerInterceptor {
        fn on_send(&self, record: ProducerRecord<i32, String>) -> ProducerRecord<i32, String> {
            self.counters.on_send_count.fetch_add(1, Ordering::SeqCst);
            if self.flags.throw_on_send.load(Ordering::SeqCst) {
                panic!("Injected exception in AppendProducerInterceptor.onSend");
            }
            let new_value = record.value().map(|v| {
                let mut s = v.clone();
                s.push_str(&self.append_str);
                s
            });
            ProducerRecord::with_partition(
                record.topic_arc().clone(),
                record.partition(),
                record.key().copied(),
                new_value,
            )
            .unwrap()
        }

        fn on_acknowledgement(
            &self,
            metadata: Option<&RecordMetadata>,
            exception: Option<&KafkaError>,
            _headers: &RecordHeaders,
        ) {
            // Mirrors the Java 2-arg overload (it ignores headers).
            self.counters.on_ack_count.fetch_add(1, Ordering::SeqCst);
            if exception.is_some() {
                self.counters.on_error_ack_count.fetch_add(1, Ordering::SeqCst);
                if let Some(metadata) = metadata {
                    if metadata.topic().is_empty() {
                        // Java threw NullPointerException here — we panic to
                        // exercise the catch_unwind path.
                        panic!("Topic is null");
                    }
                    self.counters.on_error_ack_with_topic_set_count.fetch_add(1, Ordering::SeqCst);
                    if metadata.partition() >= 0 {
                        self.counters
                            .on_error_ack_with_topic_partition_set_count
                            .fetch_add(1, Ordering::SeqCst);
                    }
                }
            }
            if self.flags.throw_on_ack.load(Ordering::SeqCst) {
                panic!("Injected exception in AppendProducerInterceptor.onAcknowledgement");
            }
        }
    }

    /// Mirrors Java's `AppendNewProducerInterceptor` — exercises the
    /// 3-arg `onAcknowledgement(metadata, exception, headers)`
    /// overload. In Rust we have a single 3-arg trait method, so this
    /// is functionally identical to `AppendProducerInterceptor` for
    /// our purposes; the duplication is preserved for parity with
    /// Java's two test fixtures.
    struct AppendNewProducerInterceptor {
        append_str: String,
        flags: InjectionFlags,
        counters: Arc<Counters>,
    }

    impl AppendNewProducerInterceptor {
        fn new(append_str: &str, flags: InjectionFlags, counters: Arc<Counters>) -> Self {
            Self { append_str: append_str.to_string(), flags, counters }
        }
    }

    impl ProducerInterceptor<i32, String> for AppendNewProducerInterceptor {
        fn on_send(&self, record: ProducerRecord<i32, String>) -> ProducerRecord<i32, String> {
            self.counters.on_send_count.fetch_add(1, Ordering::SeqCst);
            if self.flags.throw_on_send.load(Ordering::SeqCst) {
                panic!("Injected exception in AppendNewProducerInterceptor.onSend");
            }
            let new_value = record.value().map(|v| {
                let mut s = v.clone();
                s.push_str(&self.append_str);
                s
            });
            ProducerRecord::with_partition(
                record.topic_arc().clone(),
                record.partition(),
                record.key().copied(),
                new_value,
            )
            .unwrap()
        }

        fn on_acknowledgement(
            &self,
            metadata: Option<&RecordMetadata>,
            exception: Option<&KafkaError>,
            _headers: &RecordHeaders,
        ) {
            self.counters.on_ack_count.fetch_add(1, Ordering::SeqCst);
            if exception.is_some() {
                self.counters.on_error_ack_count.fetch_add(1, Ordering::SeqCst);
                if let Some(metadata) = metadata {
                    if metadata.topic().is_empty() {
                        panic!("Topic is null");
                    }
                    self.counters.on_error_ack_with_topic_set_count.fetch_add(1, Ordering::SeqCst);
                    if metadata.partition() >= 0 {
                        self.counters
                            .on_error_ack_with_topic_partition_set_count
                            .fetch_add(1, Ordering::SeqCst);
                    }
                }
            }
            if self.flags.throw_on_ack.load(Ordering::SeqCst) {
                panic!("Injected exception in AppendNewProducerInterceptor.onAcknowledgement");
            }
        }
    }

    fn producer_record() -> ProducerRecord<i32, String> {
        ProducerRecord::with_partition("test", Some(0), Some(1), Some("value".to_string())).unwrap()
    }

    /// Java: `testOnSendChain`.
    #[test]
    fn on_send_chain() {
        let counters = Arc::new(Counters::default());
        let flags1 = InjectionFlags::default();
        let flags2 = InjectionFlags::default();
        let interceptor1 = Box::new(AppendProducerInterceptor::new("One", flags1.clone(), counters.clone()));
        let interceptor2 = Box::new(AppendNewProducerInterceptor::new("Two", flags2.clone(), counters.clone()));
        let interceptors: ProducerInterceptors<i32, String> =
            ProducerInterceptors::new(vec![interceptor1, interceptor2]);

        let pr = producer_record();
        // Verify that on_send mutates the record as expected.
        let intercepted = interceptors.on_send(pr.clone());
        assert_eq!(2, counters.on_send_count.load(Ordering::SeqCst));
        assert_eq!(pr.topic(), intercepted.topic());
        assert_eq!(pr.partition(), intercepted.partition());
        assert_eq!(pr.key(), intercepted.key());
        assert_eq!(intercepted.value().unwrap(), &format!("{}OneTwo", pr.value().unwrap()));

        // on_send mutates the same record the same way.
        let another = interceptors.on_send(pr.clone());
        assert_eq!(4, counters.on_send_count.load(Ordering::SeqCst));
        assert_eq!(intercepted, another);

        // Verify that if one of the interceptors throws, other
        // interceptors' callbacks are still called.
        flags1.inject_on_send_error(true);
        let part_intercept = interceptors.on_send(pr.clone());
        assert_eq!(6, counters.on_send_count.load(Ordering::SeqCst));
        assert_eq!(part_intercept.value().unwrap(), &format!("{}Two", pr.value().unwrap()));

        // Verify the record remains valid if all on_send throw.
        flags2.inject_on_send_error(true);
        let no_intercept = interceptors.on_send(pr.clone());
        assert_eq!(pr, no_intercept);

        let mut interceptors = interceptors;
        interceptors.close();
    }

    /// Java: `testOnAcknowledgementChain`.
    #[test]
    fn on_acknowledgement_chain() {
        let counters = Arc::new(Counters::default());
        let flags1 = InjectionFlags::default();
        let flags2 = InjectionFlags::default();
        let interceptor1 = Box::new(AppendProducerInterceptor::new("One", flags1.clone(), counters.clone()));
        let interceptor2 = Box::new(AppendNewProducerInterceptor::new("Two", flags2.clone(), counters.clone()));
        let interceptors: ProducerInterceptors<i32, String> =
            ProducerInterceptors::new(vec![interceptor1, interceptor2]);

        let tp = TopicPartition::new("test", 0);
        let meta = RecordMetadata::new(tp, 0, 0, 0, 0, 0);
        let headers = RecordHeaders::new();

        // Verify on_ack is called on all interceptors.
        interceptors.on_acknowledgement(Some(&meta), None, &headers);
        assert_eq!(2, counters.on_ack_count.load(Ordering::SeqCst));

        // Verify that on_acknowledgement panics do not propagate.
        flags1.inject_on_acknowledgement_error(true);
        interceptors.on_acknowledgement(Some(&meta), None, &headers);
        assert_eq!(4, counters.on_ack_count.load(Ordering::SeqCst));

        flags2.inject_on_acknowledgement_error(true);
        interceptors.on_acknowledgement(Some(&meta), None, &headers);
        assert_eq!(6, counters.on_ack_count.load(Ordering::SeqCst));

        let mut interceptors = interceptors;
        interceptors.close();
    }

    /// Java: `testOnAcknowledgementWithErrorChain`.
    #[test]
    fn on_acknowledgement_with_error_chain() {
        let counters = Arc::new(Counters::default());
        let flags1 = InjectionFlags::default();
        let flags2 = InjectionFlags::default();
        let interceptor1 = Box::new(AppendProducerInterceptor::new("One", flags1, counters.clone()));
        let interceptor2 = Box::new(AppendNewProducerInterceptor::new("Two", flags2, counters.clone()));
        let interceptors: ProducerInterceptors<i32, String> =
            ProducerInterceptors::new(vec![interceptor1, interceptor2]);
        let pr = producer_record();

        // Verify that metadata contains both topic and partition.
        interceptors.on_send_error(
            Some(&pr),
            Some(TopicPartition::new(pr.topic_arc().clone(), pr.partition().unwrap())),
            &KafkaError::Generic("Test".to_string()),
        );
        assert_eq!(2, counters.on_error_ack_count.load(Ordering::SeqCst));
        assert_eq!(2, counters.on_error_ack_with_topic_partition_set_count.load(Ordering::SeqCst));

        // Verify that metadata contains both topic and partition (because
        // the record already contains a partition).
        interceptors.on_send_error(Some(&pr), None, &KafkaError::Generic("Test".to_string()));
        assert_eq!(4, counters.on_error_ack_count.load(Ordering::SeqCst));
        assert_eq!(4, counters.on_error_ack_with_topic_partition_set_count.load(Ordering::SeqCst));

        // If producer record does not contain partition, interceptor
        // should get partition == -1.
        let record2: ProducerRecord<i32, String> =
            ProducerRecord::with_partition_and_headers("test2", None, Some(1), Some("value".to_string()), None)
                .unwrap();
        interceptors.on_send_error(Some(&record2), None, &KafkaError::Generic("Test".to_string()));
        assert_eq!(6, counters.on_error_ack_count.load(Ordering::SeqCst));
        assert_eq!(6, counters.on_error_ack_with_topic_set_count.load(Ordering::SeqCst));
        assert_eq!(4, counters.on_error_ack_with_topic_partition_set_count.load(Ordering::SeqCst));

        // If producer record does not contain partition, but
        // topic/partition is passed to on_send_error, the interceptor
        // should get a valid partition.
        let reassigned_partition = pr.partition().unwrap() + 1;
        interceptors.on_send_error(
            Some(&record2),
            Some(TopicPartition::new(record2.topic_arc().clone(), reassigned_partition)),
            &KafkaError::Generic("Test".to_string()),
        );
        assert_eq!(8, counters.on_error_ack_count.load(Ordering::SeqCst));
        assert_eq!(8, counters.on_error_ack_with_topic_set_count.load(Ordering::SeqCst));
        assert_eq!(6, counters.on_error_ack_with_topic_partition_set_count.load(Ordering::SeqCst));

        // If both record and topic/partition are null, interceptor
        // should not receive metadata.
        interceptors.on_send_error(None, None, &KafkaError::Generic("Test".to_string()));
        assert_eq!(10, counters.on_error_ack_count.load(Ordering::SeqCst));
        assert_eq!(8, counters.on_error_ack_with_topic_set_count.load(Ordering::SeqCst));
        assert_eq!(6, counters.on_error_ack_with_topic_partition_set_count.load(Ordering::SeqCst));

        let mut interceptors = interceptors;
        interceptors.close();
    }

    /// Sanity test: an interceptor that panics in `close()` is logged
    /// but does not propagate.
    #[test]
    fn close_swallows_panics() {
        struct PanicCloser;
        impl ProducerInterceptor<i32, String> for PanicCloser {
            fn on_send(&self, record: ProducerRecord<i32, String>) -> ProducerRecord<i32, String> {
                record
            }
            fn close(&mut self) {
                panic!("boom");
            }
        }
        let mut interceptors: ProducerInterceptors<i32, String> =
            ProducerInterceptors::new(vec![Box::new(PanicCloser)]);
        // Should not panic.
        interceptors.close();
    }
}
