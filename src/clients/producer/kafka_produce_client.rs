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

//! Real `ProduceClient` implementation backed by a Kafka `Selector`.
//!
//! This adapter bridges the async `ProduceClient` trait with the low-level
//! `Selector`-based network I/O. It manages connections, builds
//! `ProduceRequest` messages, sends them over the wire, and parses
//! `ProduceResponse` messages.
//!
//! Corresponds to the produce-request sending path in
//! `org.apache.kafka.clients.producer.internals.Sender`.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicI32, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use log::{debug, warn};
use tokio::sync::Mutex;

use crate::common::TopicPartition;
use crate::common::network::NetworkSend;
use crate::common::network::plaintext_channel_builder::PlaintextChannelBuilder;
use crate::common::network::selectable::{Selectable, USE_DEFAULT_BUFFER_SIZE};
use crate::common::network::selector::{NO_IDLE_TIMEOUT_MS, Selector};
use crate::common::protocol::{ApiKeys, ByteBufferAccessor, Errors};
use crate::common::requests::abstract_response::ConcreteResponse;
use crate::common::requests::produce_request::ProduceRequestBuilder;
use crate::common::requests::{RequestBuilder, RequestHeader};
use crate::errors::{ErrorCode, KafkaError};
use crate::produce_request_data::{PartitionProduceData, ProduceRequestData, TopicProduceData};

use super::config::Acks;
use super::sender::{PartitionInfo, PartitionResponse, ProduceClient};

/// Maximum time to wait for a single poll operation, in milliseconds.
const POLL_TIMEOUT_MS: i64 = 5000;

/// Maximum number of poll iterations before giving up.
const MAX_POLL_ITERATIONS: usize = 100;

/// A `ProduceClient` implementation that sends produce requests to a real
/// Kafka broker via a `Selector`.
///
/// This is the bridge between the high-level `KafkaProducer` and the low-level
/// network transport. It manages a single selector and connection, building
/// produce requests from raw record batch bytes and parsing the responses.
///
/// The implementation is wrapped in a `Mutex` because `Selector` requires
/// `&mut self` for most operations, and the `ProduceClient` trait takes `&self`.
pub struct KafkaProduceClient {
    inner: Mutex<KafkaProduceClientInner>,
}

struct KafkaProduceClientInner {
    /// The selector for network I/O.
    selector: Selector,
    /// Bootstrap server address.
    bootstrap_addr: SocketAddr,
    /// Client ID for request headers.
    client_id: String,
    /// Monotonically increasing correlation ID.
    correlation_counter: AtomicI32,
    /// Map from node_id (as string) to whether the connection is established.
    connected_nodes: HashMap<String, bool>,
}

impl KafkaProduceClient {
    /// Creates a new `KafkaProduceClient` that connects to the given bootstrap server.
    ///
    /// # Arguments
    ///
    /// * `bootstrap_addr` - The address of the Kafka broker
    /// * `client_id` - Client identifier for request headers
    pub fn new(bootstrap_addr: SocketAddr, client_id: &str) -> Self {
        let channel_builder = Box::new(PlaintextChannelBuilder::new(None));
        let selector = Selector::with_defaults(NO_IDLE_TIMEOUT_MS, channel_builder);

        KafkaProduceClient {
            inner: Mutex::new(KafkaProduceClientInner {
                selector,
                bootstrap_addr,
                client_id: client_id.to_string(),
                correlation_counter: AtomicI32::new(1),
                connected_nodes: HashMap::new(),
            }),
        }
    }

    /// Creates a `KafkaProduceClient` with an existing `Selector`.
    ///
    /// Useful for testing or when sharing a selector with other components.
    ///
    /// # Arguments
    ///
    /// * `selector` - Pre-configured selector
    /// * `bootstrap_addr` - The address of the Kafka broker
    /// * `client_id` - Client identifier for request headers
    pub fn with_selector(selector: Selector, bootstrap_addr: SocketAddr, client_id: &str) -> Self {
        KafkaProduceClient {
            inner: Mutex::new(KafkaProduceClientInner {
                selector,
                bootstrap_addr,
                client_id: client_id.to_string(),
                correlation_counter: AtomicI32::new(1),
                connected_nodes: HashMap::new(),
            }),
        }
    }
}

impl KafkaProduceClientInner {
    /// Returns the next correlation ID.
    fn next_correlation_id(&self) -> i32 {
        self.correlation_counter.fetch_add(1, Ordering::Relaxed)
    }

    /// Ensures the connection to the given node is established.
    ///
    /// If already connected, this is a no-op. Otherwise, initiates a connection
    /// and polls until it completes.
    async fn ensure_connected(&mut self, node_id: &str) -> Result<(), KafkaError> {
        if self.connected_nodes.get(node_id).copied().unwrap_or(false) && self.selector.is_channel_ready(node_id) {
            return Ok(());
        }

        // Initiate connection
        self.selector
            .connect(node_id, self.bootstrap_addr, USE_DEFAULT_BUFFER_SIZE, USE_DEFAULT_BUFFER_SIZE)
            .await
            .map_err(|e| {
                KafkaError::with_source(
                    ErrorCode::Network,
                    format!("Failed to initiate connection to node {}", node_id),
                    e,
                )
            })?;

        // Poll until connected
        for _ in 0..MAX_POLL_ITERATIONS {
            self.selector
                .poll(POLL_TIMEOUT_MS)
                .await
                .map_err(|e| KafkaError::with_source(ErrorCode::Network, "Poll failed during connection", e))?;

            if !self.selector.connected().is_empty() {
                self.connected_nodes.insert(node_id.to_string(), true);
                debug!("Connected to node {}", node_id);
                return Ok(());
            }

            if !self.selector.disconnected().is_empty() {
                return Err(KafkaError::new(
                    ErrorCode::Network,
                    format!(
                        "Disconnected while connecting to node {}: {:?}",
                        node_id,
                        self.selector.disconnected()
                    ),
                ));
            }
        }

        Err(KafkaError::new(
            ErrorCode::TimedOut,
            format!("Timed out waiting for connection to node {}", node_id),
        ))
    }

    /// Performs the ApiVersions handshake to determine supported produce request version.
    ///
    /// Returns the maximum supported version for the Produce API.
    async fn handshake_api_versions(&mut self, node_id: &str) -> Result<i16, KafkaError> {
        let builder = crate::common::requests::ApiVersionsRequestBuilder::new();
        let version = builder.oldest_allowed_version();
        let request = builder
            .build_version(version)
            .map_err(|e| KafkaError::with_source(ErrorCode::Network, "Failed to build ApiVersions request", e))?;

        let correlation_id = self.next_correlation_id();
        let header = RequestHeader::new(builder.api_key(), version, &self.client_id, correlation_id)
            .map_err(|e| KafkaError::with_source(ErrorCode::Network, "Failed to create request header", e))?;

        let send = request
            .to_send(&header)
            .map_err(|e| KafkaError::with_source(ErrorCode::Network, "Failed to serialize ApiVersions request", e))?;

        let network_send = NetworkSend::new(node_id, Box::new(send));
        self.selector
            .send(network_send)
            .map_err(|e| KafkaError::new(ErrorCode::Network, format!("Failed to queue ApiVersions send: {}", e)))?;

        // Poll until we receive the response
        let payload = self.poll_for_response(node_id).await?;

        let mut buffer = ByteBufferAccessor::from_bytes(payload);
        let response = ConcreteResponse::parse_response(&mut buffer, &header)
            .map_err(|e| KafkaError::with_source(ErrorCode::Network, "Failed to parse ApiVersions response", e))?;

        let ConcreteResponse::ApiVersions(avr) = response else {
            return Err(KafkaError::new(ErrorCode::Network, "Expected ApiVersions response"));
        };

        if avr.data().error_code != Errors::None.code() {
            return Err(KafkaError::new(
                ErrorCode::Network,
                format!("ApiVersions response error: {:?}", Errors::for_code(avr.data().error_code)),
            ));
        }

        let produce_version = avr
            .api_version(ApiKeys::PRODUCE.id())
            .ok_or_else(|| KafkaError::new(ErrorCode::Network, "Broker does not support Produce API"))?;

        Ok(produce_version.max_version)
    }

    /// Polls the selector until a completed receive arrives, returning the payload.
    async fn poll_for_response(&mut self, node_id: &str) -> Result<Vec<u8>, KafkaError> {
        for _ in 0..MAX_POLL_ITERATIONS {
            self.selector
                .poll(POLL_TIMEOUT_MS)
                .await
                .map_err(|e| KafkaError::with_source(ErrorCode::Network, "Poll failed", e))?;

            let receives = self.selector.completed_receives();
            if !receives.is_empty() {
                let payload = receives[0]
                    .payload()
                    .ok_or_else(|| KafkaError::new(ErrorCode::Network, "Response has no payload"))?
                    .to_vec();
                return Ok(payload);
            }

            if !self.selector.disconnected().is_empty() {
                self.connected_nodes.insert(node_id.to_string(), false);
                return Err(KafkaError::new(
                    ErrorCode::Network,
                    format!("Disconnected from node {} while waiting for response", node_id),
                ));
            }
        }

        Err(KafkaError::new(
            ErrorCode::TimedOut,
            format!("Timed out waiting for response from node {}", node_id),
        ))
    }

    /// Builds a `ProduceRequestData` from the raw batch data.
    fn build_produce_request_data(
        acks: &Acks,
        timeout: Duration,
        batches: &[(TopicPartition, Vec<u8>)],
    ) -> ProduceRequestData {
        // Group batches by topic name.
        let mut topics: HashMap<String, Vec<(i32, Vec<u8>)>> = HashMap::new();
        for (tp, data) in batches {
            topics
                .entry(tp.topic().to_string())
                .or_default()
                .push((tp.partition(), data.clone()));
        }

        let mut topic_data_list = Vec::new();
        for (topic_name, partitions) in &topics {
            let mut partition_data_list = Vec::new();
            for (partition, data) in partitions {
                let mut pd = PartitionProduceData::new();
                pd.set_index(*partition);
                pd.set_records(Some(data.clone()));
                partition_data_list.push(pd);
            }

            let mut td = TopicProduceData::new();
            td.set_name(topic_name.clone());
            td.set_partition_data(partition_data_list);
            topic_data_list.push(td);
        }

        let mut data = ProduceRequestData::new();
        let acks_value = match acks {
            Acks::None => 0,
            Acks::Leader => 1,
            Acks::All => -1,
        };
        data.set_acks(acks_value);
        data.set_timeout_ms(timeout.as_millis() as i32);
        data.set_topic_data(topic_data_list);
        data
    }

    /// Sends a produce request and returns the parsed partition responses.
    async fn send_produce(
        &mut self,
        node_id: &str,
        acks: Acks,
        timeout: Duration,
        batches: Vec<(TopicPartition, Vec<u8>)>,
    ) -> Result<Vec<PartitionResponse>, KafkaError> {
        // Ensure we're connected
        self.ensure_connected(node_id).await?;

        // Perform API version handshake to get the right produce version
        let max_produce_version = self.handshake_api_versions(node_id).await?;

        // Build the produce request data
        let data = Self::build_produce_request_data(&acks, timeout, &batches);

        // Build the request using the builder
        let request_builder = ProduceRequestBuilder::new(data);

        // Use the minimum of the broker's max version and our builder's max version
        let version = max_produce_version
            .min(request_builder.latest_allowed_version())
            .max(request_builder.oldest_allowed_version());

        let request = request_builder
            .build_version(version)
            .map_err(|e| KafkaError::with_source(ErrorCode::Network, "Failed to build ProduceRequest", e))?;

        let correlation_id = self.next_correlation_id();
        let header = RequestHeader::new(request_builder.api_key(), version, &self.client_id, correlation_id)
            .map_err(|e| KafkaError::with_source(ErrorCode::Network, "Failed to create produce request header", e))?;

        // Serialize and send
        let send = request
            .to_send(&header)
            .map_err(|e| KafkaError::with_source(ErrorCode::Network, "Failed to serialize ProduceRequest", e))?;

        let network_send = NetworkSend::new(node_id, Box::new(send));
        self.selector
            .send(network_send)
            .map_err(|e| KafkaError::new(ErrorCode::Network, format!("Failed to queue produce send: {}", e)))?;

        // For acks=0, no response is expected
        if acks == Acks::None {
            // Just poll once to send the data
            self.selector.poll(POLL_TIMEOUT_MS).await.map_err(|e| {
                KafkaError::with_source(ErrorCode::Network, "Poll failed after fire-and-forget send", e)
            })?;

            // Return success responses for all partitions
            return Ok(batches
                .into_iter()
                .map(|(tp, _)| PartitionResponse { tp, base_offset: -1, log_append_time: -1, error: None })
                .collect());
        }

        // Wait for the response
        let payload = self.poll_for_response(node_id).await?;

        // Parse the response
        let mut buffer = ByteBufferAccessor::from_bytes(payload);
        let response = ConcreteResponse::parse_response(&mut buffer, &header)
            .map_err(|e| KafkaError::with_source(ErrorCode::Network, "Failed to parse ProduceResponse", e))?;

        let ConcreteResponse::Produce(produce_response) = response else {
            return Err(KafkaError::new(ErrorCode::Network, "Expected Produce response"));
        };

        // Convert the response data to PartitionResponses
        let mut results = Vec::new();
        for topic_response in &produce_response.data().responses {
            for partition_response in &topic_response.partition_responses {
                let tp = TopicPartition::new(topic_response.name.clone(), partition_response.index);

                let error = Errors::for_code(partition_response.error_code);
                let error = if error == Errors::None {
                    None
                } else {
                    warn!("Produce error for {}: {:?} (code {})", tp, error, partition_response.error_code);
                    Some(KafkaError::new(
                        error_code_from_kafka_error(&error),
                        format!("Produce error for {}: {:?}", tp, error),
                    ))
                };

                results.push(PartitionResponse {
                    tp,
                    base_offset: partition_response.base_offset,
                    log_append_time: partition_response.log_append_time_ms,
                    error,
                });
            }
        }

        Ok(results)
    }

    /// Sends a metadata request and returns partition info for the given topic.
    async fn fetch_partitions(&mut self, topic: &str) -> Result<Vec<PartitionInfo>, KafkaError> {
        let node_id = "0";
        self.ensure_connected(node_id).await?;

        // Perform API version handshake
        let avr = self.handshake_api_versions(node_id).await?;

        // Build metadata request for the specific topic
        let builder = crate::common::requests::MetadataRequestBuilder::new_with_version(Some(&[topic]), true, avr);

        let version = builder.oldest_allowed_version();
        let request = builder
            .build_version(version)
            .map_err(|e| KafkaError::with_source(ErrorCode::Network, "Failed to build MetadataRequest", e))?;

        let correlation_id = self.next_correlation_id();
        let header = RequestHeader::new(builder.api_key(), version, &self.client_id, correlation_id)
            .map_err(|e| KafkaError::with_source(ErrorCode::Network, "Failed to create metadata request header", e))?;

        let send = request
            .to_send(&header)
            .map_err(|e| KafkaError::with_source(ErrorCode::Network, "Failed to serialize MetadataRequest", e))?;

        let network_send = NetworkSend::new(node_id, Box::new(send));
        self.selector
            .send(network_send)
            .map_err(|e| KafkaError::new(ErrorCode::Network, format!("Failed to queue metadata send: {}", e)))?;

        let payload = self.poll_for_response(node_id).await?;

        let mut buffer = ByteBufferAccessor::from_bytes(payload);
        let response = ConcreteResponse::parse_response(&mut buffer, &header)
            .map_err(|e| KafkaError::with_source(ErrorCode::Network, "Failed to parse MetadataResponse", e))?;

        let ConcreteResponse::Metadata(metadata_response) = response else {
            return Err(KafkaError::new(ErrorCode::Network, "Expected Metadata response"));
        };

        let mut partitions = Vec::new();
        for topic_metadata in &metadata_response.data().topics {
            let topic_name = topic_metadata.name.as_deref().unwrap_or("");
            for partition_metadata in &topic_metadata.partitions {
                partitions.push(PartitionInfo {
                    topic: topic_name.to_string(),
                    partition: partition_metadata.partition_index,
                    leader: if partition_metadata.leader_id >= 0 {
                        Some(partition_metadata.leader_id)
                    } else {
                        None
                    },
                });
            }
        }

        Ok(partitions)
    }
}

/// Maps a Kafka protocol `Errors` value to a client `ErrorCode`.
fn error_code_from_kafka_error(error: &Errors) -> ErrorCode {
    match error {
        Errors::UnknownTopicOrPartition => ErrorCode::UnknownTopicOrPartition,
        Errors::NotLeaderOrFollower => ErrorCode::NotLeaderOrFollower,
        Errors::MessageTooLarge | Errors::RecordListTooLarge => ErrorCode::RecordTooLarge,
        Errors::InvalidTopicException => ErrorCode::InvalidTopic,
        Errors::CorruptMessage => ErrorCode::CorruptRecord,
        Errors::TopicAuthorizationFailed => ErrorCode::TopicAuthorization,
        Errors::InvalidProducerEpoch => ErrorCode::InvalidProducerEpoch,
        Errors::TransactionalIdAuthorizationFailed => ErrorCode::TransactionalIdAuthorization,
        Errors::InvalidTxnState => ErrorCode::InvalidTxnState,
        _ => ErrorCode::Unexpected,
    }
}

#[async_trait]
impl ProduceClient for KafkaProduceClient {
    async fn send_produce_request(
        &self,
        node_id: i32,
        acks: Acks,
        timeout: Duration,
        batches: Vec<(TopicPartition, Vec<u8>)>,
    ) -> Result<Vec<PartitionResponse>, KafkaError> {
        let node_id_str = node_id.to_string();
        let mut inner = self.inner.lock().await;
        inner.send_produce(&node_id_str, acks, timeout, batches).await
    }

    async fn partitions_for(&self, topic: &str) -> Result<Vec<PartitionInfo>, KafkaError> {
        let mut inner = self.inner.lock().await;
        inner.fetch_partitions(topic).await
    }
}
