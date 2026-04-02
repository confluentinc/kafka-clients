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

//! Kafka wire protocol serialization/deserialization.
//!
//! This module provides traits and implementations for reading and writing
//! Kafka protocol messages to/from byte streams.

pub mod api_keys;
pub mod byte_buffer_accessor;
pub mod errors;
pub mod message;
pub mod message_size_accumulator;
pub mod object_serialization_cache;
pub mod readable;
pub mod types;
pub mod varint;
pub mod writable;

pub use api_keys::ApiKeys;
pub use byte_buffer_accessor::ByteBufferAccessor;
pub use errors::Errors;
pub use message::{ApiMessage, Message};
pub use message_size_accumulator::MessageSizeAccumulator;
pub use object_serialization_cache::ObjectSerializationCache;
pub use readable::{RawTaggedField, Readable};
pub use types::{BoundField, Field, Schema, SchemaType, TaggedField};
pub use writable::Writable;
