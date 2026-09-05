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

pub mod callback;
pub(crate) mod internals;
pub mod kafka_producer;
pub mod mock_producer;
pub mod producer_buffer_exhausted_error;
pub mod producer_config;
pub mod producer_record;
pub mod producer_trait;
pub mod record_metadata;

pub use callback::Callback;
pub use kafka_producer::KafkaProducer;
pub use mock_producer::MockProducer;
pub use producer_buffer_exhausted_error::ProducerBufferExhaustedError;
pub use producer_config::ProducerConfig;
pub use producer_record::{ProducerRecord, ProducerRecordOptions};
pub use producer_trait::Producer;
pub use record_metadata::RecordMetadata;
