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

//! Translation of `org.apache.kafka.clients.producer`.
//!
//! Phase 4b lays only the producer-side metadata cache
//! ([`internals::ProducerMetadata`]). Phase 5 wires the rest of the
//! producer (Sender, RecordAccumulator, KafkaProducer).

pub mod buffer_exhausted_error;
pub mod callback;
pub mod kafka_producer;
pub mod partitioner;
// `producer.rs` defines the `Producer` trait. The file stem matches the
// parent module name by design — Java's `Producer` interface lives at
// the top of the `producer` package, so the Rust translation does too.
// See CLAUDE.md rule 2.
#[allow(clippy::module_inception)]
pub mod producer;
pub mod producer_config;
pub mod producer_interceptor;
pub mod producer_record;
pub mod record_metadata;
pub mod round_robin_partitioner;

#[cfg(test)]
mod record_send_test;

// `internals` packages are `pub(crate)` per CLAUDE.md naming rules.
pub(crate) mod internals;

pub use buffer_exhausted_error::BufferExhaustedError;
pub use callback::Callback;
pub use kafka_producer::KafkaProducer;
pub use partitioner::Partitioner;
pub use producer::{Producer, ProducerMetrics};
pub use producer_config::ProducerConfig;
pub use producer_interceptor::ProducerInterceptor;
pub use producer_record::{ProducerRecord, ProducerRecordError};
pub use record_metadata::RecordMetadata;
pub use round_robin_partitioner::RoundRobinPartitioner;
