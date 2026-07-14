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

//! A mock of the [`ShareConsumer`] interface for testing.
//!
//! Translates `org.apache.kafka.clients.consumer.MockShareConsumer` (Apache
//! Kafka 4.2). Mirrors the [`crate::consumer::MockConsumer`] precedent:
//! mock-specific configuration methods ([`MockShareConsumer::add_record`],
//! [`MockShareConsumer::set_client_instance_id`]) are inherent methods on the
//! concrete type, NOT on the [`ShareConsumer`] trait.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use indexmap::IndexMap;

use crate::common::{KafkaError, TopicPartition, Uuid};
use crate::consumer::acknowledge_type::AcknowledgeType;
use crate::consumer::acknowledgement_commit_callback::AcknowledgementCommitCallback;
use crate::consumer::internals::auto_offset_reset_strategy::AutoOffsetResetStrategy;
use crate::consumer::internals::subscription_state::SubscriptionState;
use crate::consumer::share_consumer::ShareConsumer;
use crate::consumer::{ConsumerRecord, ConsumerRecords};

/// A mock of the [`ShareConsumer`] interface you can use for testing code
/// that uses Kafka. This struct is NOT thread-safe.
pub struct MockShareConsumer<K, V> {
    /// Held directly (NOT `Arc<Mutex<...>>`) — the [`ShareConsumer`] trait API
    /// is `&mut self`, so the borrow checker enforces single-writer access
    /// (cf. [`crate::consumer::MockConsumer`], consumer-threading.md §16).
    subscriptions: SubscriptionState,
    /// Atomic so [`ShareConsumer::wakeup`] can take `&self`.
    wakeup: Arc<AtomicBool>,
    records: HashMap<TopicPartition, Vec<ConsumerRecord<K, V>>>,
    closed: bool,
    client_instance_id: Option<Uuid>,
}

impl<K, V> MockShareConsumer<K, V> {
    /// Create a new mock share consumer.
    ///
    /// Translates Java's parameterless `MockShareConsumer()` constructor.
    pub fn new() -> Self {
        Self {
            subscriptions: SubscriptionState::new(AutoOffsetResetStrategy::NONE),
            wakeup: Arc::new(AtomicBool::new(false)),
            records: HashMap::new(),
            closed: false,
            client_instance_id: None,
        }
    }

    /// Add a record to the buffer that the next [`ShareConsumer::poll`] call
    /// will return. The record's topic must already be subscribed.
    ///
    /// Translates Java's `addRecord(ConsumerRecord<K, V>)`. Returns
    /// [`KafkaError::illegal_state`] if the topic is not subscribed — Java
    /// throws `IllegalStateException` in that case.
    pub fn add_record(&mut self, record: ConsumerRecord<K, V>) -> Result<(), KafkaError> {
        self.ensure_not_closed()?;
        let tp = TopicPartition::new(record.topic().to_string(), record.partition());
        if !self.subscriptions.subscription().contains(record.topic()) {
            return Err(KafkaError::illegal_state(
                "Cannot add records for a topics that is not subscribed by the consumer",
            ));
        }
        self.records.entry(tp).or_default().push(record);
        Ok(())
    }

    /// Set the client instance ID returned by [`ShareConsumer::client_instance_id`].
    ///
    /// Translates Java's `setClientInstanceId(Uuid)`.
    pub fn set_client_instance_id(&mut self, client_instance_id: Uuid) {
        self.client_instance_id = Some(client_instance_id);
    }

    /// Java's `ensureNotClosed()`.
    fn ensure_not_closed(&self) -> Result<(), KafkaError> {
        if self.closed {
            return Err(KafkaError::illegal_state("This consumer has already been closed."));
        }
        Ok(())
    }
}

impl<K, V> Default for MockShareConsumer<K, V> {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl<K, V> ShareConsumer<K, V> for MockShareConsumer<K, V>
where
    K: Send + 'static,
    V: Send + 'static,
{
    fn subscription(&self) -> Result<HashSet<String>, KafkaError> {
        self.ensure_not_closed()?;
        Ok(self.subscriptions.subscription())
    }

    async fn subscribe(&mut self, topics: Vec<String>) -> Result<(), KafkaError> {
        self.ensure_not_closed()?;
        let set: HashSet<String> = topics.into_iter().collect();
        self.subscriptions.subscribe_topics(set, None)?;
        Ok(())
    }

    async fn unsubscribe(&mut self) -> Result<(), KafkaError> {
        self.ensure_not_closed()?;
        self.subscriptions.unsubscribe();
        Ok(())
    }

    async fn poll(&mut self, _timeout: Duration) -> Result<ConsumerRecords<K, V>, KafkaError> {
        self.ensure_not_closed()?;

        let mut results: IndexMap<TopicPartition, Vec<ConsumerRecord<K, V>>> = IndexMap::new();
        for (tp, recs) in self.records.drain() {
            if !recs.is_empty() {
                results.insert(tp, recs);
            }
        }
        Ok(ConsumerRecords::new(results, HashMap::new()))
    }

    fn acknowledge(&mut self, _record: &ConsumerRecord<K, V>) -> Result<(), KafkaError> {
        Ok(())
    }

    fn acknowledge_with_type(
        &mut self,
        _record: &ConsumerRecord<K, V>,
        _ack_type: AcknowledgeType,
    ) -> Result<(), KafkaError> {
        Ok(())
    }

    fn acknowledge_by_offset(
        &mut self,
        _topic: &str,
        _partition: i32,
        _offset: i64,
        _ack_type: AcknowledgeType,
    ) -> Result<(), KafkaError> {
        Ok(())
    }

    async fn commit_sync(
        &mut self,
    ) -> Result<HashMap<crate::common::TopicIdPartition, Option<KafkaError>>, KafkaError> {
        Ok(HashMap::new())
    }

    async fn commit_sync_timeout(
        &mut self,
        _timeout: Duration,
    ) -> Result<HashMap<crate::common::TopicIdPartition, Option<KafkaError>>, KafkaError> {
        Ok(HashMap::new())
    }

    async fn commit_async(&mut self) -> Result<(), KafkaError> {
        Ok(())
    }

    fn set_acknowledgement_commit_callback(&mut self, _callback: Option<Arc<dyn AcknowledgementCommitCallback>>) {}

    async fn client_instance_id(&mut self, _timeout: Duration) -> Result<Uuid, KafkaError> {
        match self.client_instance_id {
            Some(id) => Ok(id),
            None => Err(KafkaError::illegal_state("clientInstanceId not set")),
        }
    }

    fn acquisition_lock_timeout_ms(&self) -> Result<Option<i32>, KafkaError> {
        Ok(None)
    }

    async fn close(&mut self) -> Result<(), KafkaError> {
        self.closed = true;
        Ok(())
    }

    async fn close_timeout(&mut self, _timeout: Duration) -> Result<(), KafkaError> {
        self.closed = true;
        Ok(())
    }

    fn wakeup(&self) {
        self.wakeup.store(true, Ordering::SeqCst);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Java `MockShareConsumerTest.testSimpleMock`.
    ///
    /// `ConsumerRecord` is not `Clone`, so rather than comparing whole-record
    /// equality against the added instances (as Java's `assertEquals(rec1,
    /// iter.next())` does by object identity/equality) we assert the polled
    /// records' `(offset, key, value)` fields, which is equivalent.
    #[tokio::test]
    async fn test_simple_mock() {
        let mut consumer: MockShareConsumer<String, String> = MockShareConsumer::new();
        consumer.subscribe(vec!["test".to_string()]).await.unwrap();
        assert_eq!(0, consumer.poll(Duration::ZERO).await.unwrap().count());

        consumer.add_record(make_record("test", 0, 0, "key1", "value1")).unwrap();
        consumer.add_record(make_record("test", 0, 1, "key2", "value2")).unwrap();

        let recs = consumer.poll(Duration::from_millis(1)).await.unwrap();
        let collected: Vec<&ConsumerRecord<String, String>> = (&recs).into_iter().collect();
        assert_eq!(collected.len(), 2);
        assert_eq!(collected[0].offset(), 0);
        assert_eq!(collected[0].key(), Some(&"key1".to_string()));
        assert_eq!(collected[0].value(), Some(&"value1".to_string()));
        assert_eq!(collected[1].offset(), 1);
        assert_eq!(collected[1].key(), Some(&"key2".to_string()));
        assert_eq!(collected[1].value(), Some(&"value2".to_string()));
        assert_eq!(0, recs.next_offsets().len());
    }

    fn make_record(topic: &str, partition: i32, offset: i64, key: &str, value: &str) -> ConsumerRecord<String, String> {
        ConsumerRecord::new(topic, partition, offset, Some(key.to_string()), Some(value.to_string()))
    }
}
