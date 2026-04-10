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

//! Sender background task and ProduceClient trait.
//!
//! The Sender drains batches from the RecordAccumulator and sends them
//! via the ProduceClient trait, which abstracts the network layer.
//!
//! Corresponds to org.apache.kafka.clients.producer.internals.Sender.

use crate::clients::producer::accumulator::RecordAccumulator;
use crate::clients::producer::config::{Acks, ProducerConfig};
use crate::common::TopicPartition;
use crate::errors::KafkaError;
use async_trait::async_trait;
use std::sync::Arc;
use std::time::Duration;

/// Response for a single partition in a ProduceResponse.
#[derive(Debug)]
pub struct PartitionResponse {
    /// The TopicPartition this response is for.
    pub tp: TopicPartition,
    /// The base offset assigned by the broker.
    pub base_offset: i64,
    /// The log append time assigned by the broker (-1 if not available).
    pub log_append_time: i64,
    /// Error, if the partition had one.
    pub error: Option<KafkaError>,
}

/// Partition metadata returned by the client.
#[derive(Debug, Clone)]
pub struct PartitionInfo {
    /// Topic name.
    pub topic: String,
    /// Partition number.
    pub partition: i32,
    /// Leader broker node ID, if known.
    pub leader: Option<i32>,
}

/// Trait abstracting the network layer for sending ProduceRequests.
///
/// Implementations can be the real network client or a mock for testing.
/// This is the primary mock boundary for the producer.
#[async_trait]
pub trait ProduceClient: Send + Sync + 'static {
    /// Send produce request batches to a specific broker node.
    ///
    /// Returns per-partition results (base_offset, timestamp, error).
    async fn send_produce_request(
        &self,
        node_id: i32,
        acks: Acks,
        timeout: Duration,
        batches: Vec<(TopicPartition, Vec<u8>)>,
    ) -> Result<Vec<PartitionResponse>, KafkaError>;

    /// Get partition metadata for a topic.
    async fn partitions_for(&self, topic: &str) -> Result<Vec<PartitionInfo>, KafkaError>;
}

/// Background task that drains batches from the accumulator and sends them.
///
/// Spawned as a tokio task by `KafkaProducer::new()`.
pub struct Sender<C: ProduceClient> {
    accumulator: Arc<RecordAccumulator>,
    client: Arc<C>,
    config: Arc<ProducerConfig>,
}

impl<C: ProduceClient> Sender<C> {
    /// Create a new Sender.
    pub fn new(accumulator: Arc<RecordAccumulator>, client: Arc<C>, config: Arc<ProducerConfig>) -> Self {
        Sender { accumulator, client, config }
    }

    /// Main loop: runs until the accumulator is closed and fully drained.
    pub async fn run(self) {
        loop {
            // Check for lingered batches and move them to ready.
            self.accumulator.expire_lingering_batches().await;

            // Drain all ready batches.
            let ready_batches = self.accumulator.drain().await;

            if ready_batches.is_empty() {
                if self.accumulator.is_closed().await {
                    break;
                }
                // Wait for a batch to become ready, or linger timeout.
                let linger = self.config.linger();
                let timeout = if linger.is_zero() {
                    Duration::from_millis(100)
                } else {
                    linger
                };
                self.accumulator.wait_for_batch_ready(timeout).await;
                continue;
            }

            // Send each batch. For now, use node_id=0 since we don't have
            // real metadata. In production, batches would be grouped by leader.
            let node_id = 0;
            let mut request_batches = Vec::new();
            let mut pending_batches = Vec::new();

            for mut batch in ready_batches {
                let tp = batch.tp().clone();
                let data = batch.buffer();
                request_batches.push((tp, data));
                pending_batches.push(batch);
            }

            let result = self
                .client
                .send_produce_request(node_id, self.config.acks(), self.config.request_timeout(), request_batches)
                .await;

            match result {
                Ok(responses) => {
                    // Match responses to batches and complete them.
                    for batch in pending_batches {
                        let response = responses.iter().find(|r| r.tp == *batch.tp());
                        match response {
                            Some(r) => {
                                let bytes = batch.written_bytes();
                                batch.complete(r.base_offset, r.log_append_time, r.error.as_ref());
                                self.accumulator.release_memory(bytes);
                            },
                            None => {
                                // No response for this partition — treat as success with offset 0.
                                let bytes = batch.written_bytes();
                                batch.complete(0, 0, None);
                                self.accumulator.release_memory(bytes);
                            },
                        }
                    }
                },
                Err(e) => {
                    // Network-level failure — fail all batches in this request.
                    for batch in pending_batches {
                        let bytes = batch.written_bytes();
                        batch.complete(0, 0, Some(&e));
                        self.accumulator.release_memory(bytes);
                    }
                },
            }
        }
    }
}
