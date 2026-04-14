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

//! Producer configuration with builder pattern.
//!
//! Corresponds to org.apache.kafka.clients.producer.ProducerConfig.

use crate::errors::{ErrorCode, KafkaError};
use std::time::Duration;

/// Acknowledgement level for produce requests.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Acks {
    /// No acknowledgement (fire-and-forget).
    None,
    /// Wait for the leader to acknowledge.
    Leader,
    /// Wait for all in-sync replicas to acknowledge.
    All,
}

impl Acks {
    /// Returns the wire protocol value for this acks level.
    ///
    /// Matches Java's `ProducerConfig.ACKS_CONFIG` values:
    /// - `None` → 0
    /// - `Leader` → 1
    /// - `All` → -1
    pub fn as_i16(self) -> i16 {
        match self {
            Acks::None => 0,
            Acks::Leader => 1,
            Acks::All => -1,
        }
    }
}

/// Configuration for a KafkaProducer.
#[derive(Debug, Clone)]
pub struct ProducerConfig {
    bootstrap_servers: Vec<String>,
    batch_size: usize,
    linger: Duration,
    buffer_memory: usize,
    max_block: Duration,
    acks: Acks,
    retries: u32,
    max_in_flight_requests_per_connection: u32,
    request_timeout: Duration,
    delivery_timeout: Duration,
    retry_backoff: Duration,
}

impl ProducerConfig {
    /// Create a new configuration builder.
    pub fn builder() -> ProducerConfigBuilder {
        ProducerConfigBuilder::default()
    }

    /// Maximum size of a single batch in bytes.
    pub fn batch_size(&self) -> usize {
        self.batch_size
    }

    /// How long to wait for additional records before sending a batch.
    pub fn linger(&self) -> Duration {
        self.linger
    }

    /// Total memory available for buffering records.
    pub fn buffer_memory(&self) -> usize {
        self.buffer_memory
    }

    /// Maximum time to block when buffer memory is exhausted.
    pub fn max_block(&self) -> Duration {
        self.max_block
    }

    /// Acknowledgement level.
    pub fn acks(&self) -> Acks {
        self.acks
    }

    /// Maximum number of retries for failed requests.
    pub fn retries(&self) -> u32 {
        self.retries
    }

    /// Maximum in-flight requests per connection.
    pub fn max_in_flight_requests_per_connection(&self) -> u32 {
        self.max_in_flight_requests_per_connection
    }

    /// Request timeout.
    pub fn request_timeout(&self) -> Duration {
        self.request_timeout
    }

    /// Delivery timeout (upper bound on send latency).
    pub fn delivery_timeout(&self) -> Duration {
        self.delivery_timeout
    }

    /// Backoff between retries.
    pub fn retry_backoff(&self) -> Duration {
        self.retry_backoff
    }

    /// Bootstrap server addresses.
    pub fn bootstrap_servers(&self) -> &[String] {
        &self.bootstrap_servers
    }
}

/// Builder for `ProducerConfig`.
#[derive(Debug, Default)]
pub struct ProducerConfigBuilder {
    bootstrap_servers: Option<Vec<String>>,
    batch_size: Option<usize>,
    linger_ms: Option<u64>,
    buffer_memory: Option<usize>,
    max_block_ms: Option<u64>,
    acks: Option<Acks>,
    retries: Option<u32>,
    max_in_flight_requests_per_connection: Option<u32>,
    request_timeout_ms: Option<u64>,
    delivery_timeout_ms: Option<u64>,
    retry_backoff_ms: Option<u64>,
}

impl ProducerConfigBuilder {
    /// Set bootstrap server addresses.
    pub fn bootstrap_servers(mut self, servers: impl Into<Vec<String>>) -> Self {
        self.bootstrap_servers = Some(servers.into());
        self
    }

    /// Set the maximum batch size in bytes. Default: 16384.
    pub fn batch_size(mut self, bytes: usize) -> Self {
        self.batch_size = Some(bytes);
        self
    }

    /// Set the linger time in milliseconds. Default: 0.
    pub fn linger_ms(mut self, ms: u64) -> Self {
        self.linger_ms = Some(ms);
        self
    }

    /// Set the total buffer memory in bytes. Default: 33554432 (32MB).
    pub fn buffer_memory(mut self, bytes: usize) -> Self {
        self.buffer_memory = Some(bytes);
        self
    }

    /// Set the maximum time to block on send in milliseconds. Default: 60000.
    pub fn max_block_ms(mut self, ms: u64) -> Self {
        self.max_block_ms = Some(ms);
        self
    }

    /// Set the acknowledgement level. Default: All.
    pub fn acks(mut self, acks: Acks) -> Self {
        self.acks = Some(acks);
        self
    }

    /// Set the maximum number of retries. Default: i32::MAX (effectively infinite).
    pub fn retries(mut self, retries: u32) -> Self {
        self.retries = Some(retries);
        self
    }

    /// Set max in-flight requests per connection. Default: 5.
    pub fn max_in_flight_requests_per_connection(mut self, n: u32) -> Self {
        self.max_in_flight_requests_per_connection = Some(n);
        self
    }

    /// Set the request timeout in milliseconds. Default: 30000.
    pub fn request_timeout_ms(mut self, ms: u64) -> Self {
        self.request_timeout_ms = Some(ms);
        self
    }

    /// Set the delivery timeout in milliseconds. Default: 120000.
    pub fn delivery_timeout_ms(mut self, ms: u64) -> Self {
        self.delivery_timeout_ms = Some(ms);
        self
    }

    /// Set the retry backoff in milliseconds. Default: 100.
    pub fn retry_backoff_ms(mut self, ms: u64) -> Self {
        self.retry_backoff_ms = Some(ms);
        self
    }

    /// Build the configuration, applying defaults for unset fields.
    pub fn build(self) -> crate::errors::Result<ProducerConfig> {
        let bootstrap_servers = self
            .bootstrap_servers
            .ok_or_else(|| KafkaError::new(ErrorCode::Unexpected, "bootstrap.servers is required"))?;

        if bootstrap_servers.is_empty() {
            return Err(KafkaError::new(ErrorCode::Unexpected, "bootstrap.servers must not be empty"));
        }

        Ok(ProducerConfig {
            bootstrap_servers,
            batch_size: self.batch_size.unwrap_or(16384),
            linger: Duration::from_millis(self.linger_ms.unwrap_or(0)),
            buffer_memory: self.buffer_memory.unwrap_or(33_554_432),
            max_block: Duration::from_millis(self.max_block_ms.unwrap_or(60_000)),
            acks: self.acks.unwrap_or(Acks::All),
            retries: self.retries.unwrap_or(i32::MAX as u32),
            max_in_flight_requests_per_connection: self.max_in_flight_requests_per_connection.unwrap_or(5),
            request_timeout: Duration::from_millis(self.request_timeout_ms.unwrap_or(30_000)),
            delivery_timeout: Duration::from_millis(self.delivery_timeout_ms.unwrap_or(120_000)),
            retry_backoff: Duration::from_millis(self.retry_backoff_ms.unwrap_or(100)),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_builder_defaults() {
        let config = ProducerConfig::builder()
            .bootstrap_servers(vec!["localhost:9092".to_string()])
            .build()
            .unwrap();

        assert_eq!(config.batch_size(), 16384);
        assert_eq!(config.linger(), Duration::from_millis(0));
        assert_eq!(config.buffer_memory(), 33_554_432);
        assert_eq!(config.max_block(), Duration::from_secs(60));
        assert_eq!(config.acks(), Acks::All);
        assert_eq!(config.retries(), i32::MAX as u32);
        assert_eq!(config.max_in_flight_requests_per_connection(), 5);
    }

    #[test]
    fn test_builder_custom_values() {
        let config = ProducerConfig::builder()
            .bootstrap_servers(vec!["host1:9092".to_string(), "host2:9092".to_string()])
            .batch_size(32768)
            .linger_ms(5)
            .buffer_memory(67_108_864)
            .acks(Acks::Leader)
            .build()
            .unwrap();

        assert_eq!(config.batch_size(), 32768);
        assert_eq!(config.linger(), Duration::from_millis(5));
        assert_eq!(config.buffer_memory(), 67_108_864);
        assert_eq!(config.acks(), Acks::Leader);
    }

    #[test]
    fn test_builder_missing_bootstrap_servers() {
        let result = ProducerConfig::builder().build();
        assert!(result.is_err());
    }

    #[test]
    fn test_builder_empty_bootstrap_servers() {
        let result = ProducerConfig::builder().bootstrap_servers(Vec::<String>::new()).build();
        assert!(result.is_err());
    }
}
