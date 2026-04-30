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

//! Translation of `org.apache.kafka.common.protocol.types`.
//!
//! The Java package defines:
//!
//! * [`Type`] — a polymorphic encoder/decoder ladder (`INT8`, `STRING`,
//!   `COMPACT_STRING`, …). Each Java instance is a singleton anonymous
//!   subclass; we model it as a Rust enum.
//! * [`Schema`] — a sequence of [`Field`]s used to (de)serialise [`Struct`]s.
//! * [`Struct`] — a heterogenous record holding field values.
//! * [`ArrayOf`] / [`CompactArrayOf`] — array types parameterised by an
//!   element type.
//! * [`TaggedFields`] / [`RawTaggedField`] / [`RawTaggedFieldWriter`] — the
//!   "flexible versions" tagged-field machinery.
//! * [`SchemaException`] — the error thrown by `read/write/validate` when
//!   protocol parsing fails. Translated to [`KafkaError::Generic`].
//!
//! In Rust, values that the Java code passes around as `java.lang.Object`
//! are modelled as the [`Value`] enum below. Each variant maps onto a
//! distinct Java boxed type (`Boolean`, `Byte`, `Short`, `Integer`, `Long`,
//! `Double`, `String`, `byte[]`/`ByteBuffer`, `Object[]`, `Struct`,
//! `NavigableMap<Integer, Object>`).
//!
//! The minimal [`Readable`] / [`Writable`] traits in [`io`] are *temporary*:
//! they exist so that `RawTaggedFieldWriter::write_raw_tags` can target an
//! abstract sink. Phase 2c will replace them with the full
//! `org.apache.kafka.common.protocol.{Readable, Writable}` translations
//! (which also support compressed, primitive, and varint reads/writes).

pub mod array_of;
pub mod bound_field;
pub mod compact_array_of;
pub mod field;
pub mod io;
pub mod raw_tagged_field;
pub mod raw_tagged_field_writer;
pub mod schema;
pub mod schema_exception;
pub mod r#struct;
pub mod tagged_fields;
#[allow(clippy::module_inception)]
pub mod r#type;
pub mod value;

pub use array_of::ArrayOf;
pub use bound_field::BoundField;
pub use compact_array_of::CompactArrayOf;
pub use field::{Field, TaggedFieldsSection};
pub use io::{Readable, Writable};
pub use raw_tagged_field::RawTaggedField;
pub use raw_tagged_field_writer::RawTaggedFieldWriter;
pub use schema::{Schema, SchemaVisitor};
pub use schema_exception::{SchemaException, schema_exception, schema_exception_with_cause};
pub use r#struct::Struct;
pub use tagged_fields::TaggedFields;
pub use r#type::Type;
pub use value::Value;
