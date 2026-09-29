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

mod api_keys;
mod api_message;
mod byte_buffer_accessor;
mod bytes_reader;
mod errors;
mod message;
mod message_size_accumulator;
mod message_util;
mod object_serialization_cache;
mod readable;
pub mod types;
mod varint;
mod writable;

pub use api_keys::ApiKeys;
pub use api_message::ApiMessage;
pub use byte_buffer_accessor::ByteBufferAccessor;
pub use bytes_reader::BytesReader;
pub use errors::Errors;
pub use message::Message;
pub use message_size_accumulator::MessageSizeAccumulator;
pub use message_util::MessageUtil;
pub use object_serialization_cache::ObjectSerializationCache;
pub use readable::Readable;
pub use types::{BoundField, Field, RawTaggedField, Schema, SchemaType, TaggedField};
pub use varint::ByteUtils;
pub use writable::Writable;
