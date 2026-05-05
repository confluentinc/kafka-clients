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
pub mod record_metadata;

// `internals` packages are `pub(crate)` per CLAUDE.md naming rules.
pub(crate) mod internals;

pub use buffer_exhausted_error::BufferExhaustedError;
pub use callback::Callback;
pub use record_metadata::RecordMetadata;
