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

//! Producer types (org.apache.kafka.clients.producer)

mod callback;
pub(crate) mod internals;
mod kafka_producer;
#[cfg(test)]
mod mock_partitioner;
mod mock_producer;
mod partitioner;
// The `Producer` interface lives in its own `producer.rs` file per CLAUDE.md's
// "one Java class per file" rule, nested under the `producer` module that
// mirrors the `org.apache.kafka.clients.producer` package.
#[allow(clippy::module_inception)]
mod producer;
mod producer_buffer_exhausted_error;
mod producer_config;
mod producer_record;
mod record_metadata;
mod round_robin_partitioner;

pub use callback::Callback;
pub use kafka_producer::KafkaProducer;
#[cfg(test)]
pub(crate) use mock_partitioner::MockPartitioner;
pub use mock_producer::{MockProducer, MockProducerOptions, MockProducerOptionsBuilder};
pub use partitioner::Partitioner;
pub use producer::Producer;
pub use producer_buffer_exhausted_error::ProducerBufferExhaustedError;
pub use producer_config::ProducerConfig;
pub use producer_record::{ProducerRecord, ProducerRecordOptions, ProducerRecordOptionsBuilder};
pub use record_metadata::RecordMetadata;
pub use round_robin_partitioner::RoundRobinPartitioner;
