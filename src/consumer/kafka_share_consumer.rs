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

//! `KafkaShareConsumer` — the top-level share-consumer facade (KIP-932).
//!
//! Translates `org.apache.kafka.clients.consumer.KafkaShareConsumer`. Like
//! Java, it is a thin facade that delegates every call to an underlying
//! [`ShareConsumer`] implementation.
//!
//! ## Delegate / creator collapse (consumer-threading.md §2)
//!
//! Java routes construction through `ShareConsumerDelegateCreator` producing a
//! `ShareConsumerDelegate` (a marker sub-interface of `ShareConsumer` adding
//! `clientId()` / `metricsRegistry()` / `kafkaShareConsumerMetrics()`). With a
//! single delegate implementation, the creator collapses to a direct
//! construction — exactly as the non-share `Consumer` path collapses
//! `ConsumerDelegate` / `ConsumerDelegateCreator` into
//! [`crate::consumer::new_consumer`]. The metrics accessors are omitted
//! (KIP-714 deferral), so no separate delegate trait is needed: the facade
//! wraps a `Box<dyn ShareConsumer<K, V>>` produced by
//! [`crate::consumer::new_share_consumer`].

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;

use crate::common::{KafkaError, TopicIdPartition, Uuid};
use crate::consumer::acknowledge_type::AcknowledgeType;
use crate::consumer::acknowledgement_commit_callback::AcknowledgementCommitCallback;
use crate::consumer::share_consumer::ShareConsumer;
use crate::consumer::{ConsumerRecord, ConsumerRecords};

/// A client that consumes records from a Kafka cluster using a share group.
///
/// See the module docs and Java's `KafkaShareConsumer`.
pub struct KafkaShareConsumer<K, V>
where
    K: Send + 'static,
    V: Send + 'static,
{
    delegate: Box<dyn ShareConsumer<K, V>>,
}

impl<K, V> KafkaShareConsumer<K, V>
where
    K: Send + Sync + 'static,
    V: Send + Sync + 'static,
{
    /// Constructs a `KafkaShareConsumer` from a configuration and explicit
    /// key/value [`Deserializer`](crate::common::serialization::Deserializer)s.
    ///
    /// Java passes deserializers via `ShareConsumerConfig` reflection; Rust
    /// takes them explicitly (matching [`crate::consumer::new_consumer`]).
    ///
    /// # Errors
    ///
    /// Returns the error from [`crate::consumer::new_share_consumer`].
    pub fn new(
        config: crate::consumer::ShareConsumerConfig,
        key_deserializer: Box<dyn crate::common::serialization::Deserializer<K>>,
        value_deserializer: Box<dyn crate::common::serialization::Deserializer<V>>,
    ) -> Result<Self, KafkaError> {
        let delegate = crate::consumer::new_share_consumer(config, key_deserializer, value_deserializer)?;
        Ok(Self { delegate })
    }

    /// Wraps an already-constructed [`ShareConsumer`] delegate. Used by tests
    /// and by callers that assemble the delegate themselves.
    pub fn from_delegate(delegate: Box<dyn ShareConsumer<K, V>>) -> Self {
        Self { delegate }
    }
}

#[async_trait]
impl<K, V> ShareConsumer<K, V> for KafkaShareConsumer<K, V>
where
    K: Send + 'static,
    V: Send + 'static,
{
    fn subscription(&self) -> Result<HashSet<String>, KafkaError> {
        self.delegate.subscription()
    }

    async fn subscribe(&mut self, topics: Vec<String>) -> Result<(), KafkaError> {
        self.delegate.subscribe(topics).await
    }

    async fn unsubscribe(&mut self) -> Result<(), KafkaError> {
        self.delegate.unsubscribe().await
    }

    async fn poll(&mut self, timeout: Duration) -> Result<ConsumerRecords<K, V>, KafkaError> {
        self.delegate.poll(timeout).await
    }

    fn acknowledge(&mut self, record: &ConsumerRecord<K, V>) -> Result<(), KafkaError> {
        self.delegate.acknowledge(record)
    }

    fn acknowledge_with_type(
        &mut self,
        record: &ConsumerRecord<K, V>,
        ack_type: AcknowledgeType,
    ) -> Result<(), KafkaError> {
        self.delegate.acknowledge_with_type(record, ack_type)
    }

    fn acknowledge_by_offset(
        &mut self,
        topic: &str,
        partition: i32,
        offset: i64,
        ack_type: AcknowledgeType,
    ) -> Result<(), KafkaError> {
        self.delegate.acknowledge_by_offset(topic, partition, offset, ack_type)
    }

    async fn commit_sync(&mut self) -> Result<HashMap<TopicIdPartition, Option<KafkaError>>, KafkaError> {
        self.delegate.commit_sync().await
    }

    async fn commit_sync_timeout(
        &mut self,
        timeout: Duration,
    ) -> Result<HashMap<TopicIdPartition, Option<KafkaError>>, KafkaError> {
        self.delegate.commit_sync_timeout(timeout).await
    }

    async fn commit_async(&mut self) -> Result<(), KafkaError> {
        self.delegate.commit_async().await
    }

    fn set_acknowledgement_commit_callback(&mut self, callback: Option<Arc<dyn AcknowledgementCommitCallback>>) {
        self.delegate.set_acknowledgement_commit_callback(callback);
    }

    async fn client_instance_id(&mut self, timeout: Duration) -> Result<Uuid, KafkaError> {
        self.delegate.client_instance_id(timeout).await
    }

    fn acquisition_lock_timeout_ms(&self) -> Result<Option<i32>, KafkaError> {
        self.delegate.acquisition_lock_timeout_ms()
    }

    async fn close(&mut self) -> Result<(), KafkaError> {
        self.delegate.close().await
    }

    async fn close_timeout(&mut self, timeout: Duration) -> Result<(), KafkaError> {
        self.delegate.close_timeout(timeout).await
    }

    fn wakeup(&self) {
        self.delegate.wakeup();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::consumer::MockShareConsumer;

    /// The facade delegates trait calls to its underlying [`ShareConsumer`].
    #[tokio::test]
    async fn facade_delegates_to_underlying_share_consumer() {
        let mock: MockShareConsumer<String, String> = MockShareConsumer::new();
        let mut consumer = KafkaShareConsumer::from_delegate(Box::new(mock));
        consumer.subscribe(vec!["t".to_string()]).await.unwrap();
        assert_eq!(consumer.subscription().unwrap(), HashSet::from(["t".to_string()]));
        assert_eq!(consumer.poll(Duration::ZERO).await.unwrap().count(), 0);
        consumer.close().await.unwrap();
        // After close the mock reports closed on the next state read.
        assert!(consumer.subscription().is_err());
    }

    struct StringDeserializer;
    impl crate::common::serialization::Deserializer<String> for StringDeserializer {
        fn deserialize(&self, _topic: &str, data: &[u8]) -> Result<String, KafkaError> {
            Ok(String::from_utf8_lossy(data).into_owned())
        }
    }

    /// `new_share_consumer` (and therefore `KafkaShareConsumer::new`) returns
    /// `unsupported_version` until the Phase-7 production pipeline is wired.
    #[test]
    fn factory_returns_unsupported_until_production_pipeline() {
        use std::collections::HashMap as StdHashMap;

        let mut props = StdHashMap::new();
        props.insert("group.id".to_string(), "g".to_string());
        props.insert("bootstrap.servers".to_string(), "localhost:9092".to_string());
        let config = crate::consumer::ShareConsumerConfig::from_properties(&props).unwrap();
        let result: Result<KafkaShareConsumer<String, String>, KafkaError> =
            KafkaShareConsumer::new(config, Box::new(StringDeserializer), Box::new(StringDeserializer));
        match result {
            Ok(_) => panic!("expected unsupported_version until Phase-7 production pipeline"),
            Err(e) => assert!(e.to_string().contains("not yet wired"), "got: {e}"),
        }
    }
}
