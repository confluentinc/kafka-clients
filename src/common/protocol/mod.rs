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

//! Translation of `org.apache.kafka.common.protocol`.
//!
//! The runtime side of the protocol (`Readable`, `Writable`,
//! `ByteBufferAccessor`, `Message`, `ApiMessage`, `ApiKeys`, `Errors`, …)
//! lives at the top level. The `types` submodule contains the
//! `org.apache.kafka.common.protocol.types.*` translation (`Type`, `Schema`,
//! `Struct`, `Field`, `RawTaggedFieldWriter`, …).

pub mod api_keys;
pub mod api_message;
pub mod byte_buffer_accessor;
pub mod data_output_stream_writable;
pub mod errors;
pub mod message;
pub mod message_size_accumulator;
pub mod message_util;
pub mod object_serialization_cache;
pub mod readable;
pub mod send_builder;
pub mod types;
pub mod writable;

pub use api_keys::ApiKeys;
pub use api_message::ApiMessage;
pub use byte_buffer_accessor::{ByteBufferAccessor, SliceReadable};
pub use data_output_stream_writable::DataOutputStreamWritable;
pub use errors::Errors;
pub use message::Message;
pub use message_size_accumulator::MessageSizeAccumulator;
pub use object_serialization_cache::ObjectSerializationCache;
pub use readable::Readable;
pub use send_builder::SendBuilder;
pub use writable::Writable;
