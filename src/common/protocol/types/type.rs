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

//! Translation of `org.apache.kafka.common.protocol.types.Type`.
//!
//! Java models each protocol type (INT8, STRING, COMPACT_STRING, ARRAY, …)
//! as an anonymous singleton subclass of `Type`. We keep that surface but
//! collapse the hierarchy into a Rust enum so dispatch is a `match` instead
//! of dynamic dispatch.

use std::collections::BTreeMap;
use std::fmt;

use crate::common::errors::KafkaError;
use crate::common::protocol::types::array_of::ArrayOf;
use crate::common::protocol::types::compact_array_of::CompactArrayOf;
use crate::common::protocol::types::raw_tagged_field::RawTaggedField;
use crate::common::protocol::types::schema::Schema;
use crate::common::protocol::types::schema_exception::schema_exception;
use crate::common::protocol::types::tagged_fields::TaggedFields;
use crate::common::protocol::types::value::Value;
use crate::common::protocol::{ByteBufferAccessor, Readable, Writable};
use crate::common::utils::byte_utils;

// `ReadBuffer<'a>` was a Phase-2b stand-in. Phase 2c replaces it with the
// proper [`ByteBufferAccessor`] (and the [`Readable`] / [`Writable`] traits).
// Internal helpers below operate on `&mut dyn Readable` / `&mut dyn Writable`
// so any conforming buffer can drive `Type::read` / `Type::write`.

/// A protocol type. Each variant maps to a Java `Type` singleton or
/// container (`ArrayOf`, `CompactArrayOf`, `TaggedFields`, `Schema`).
///
/// Methods follow the Java contract:
///
/// * [`Type::write`]   — encode `value` into `buffer`.
/// * [`Type::read`]    — decode the next value from `buffer`.
/// * [`Type::size_of`] — number of bytes [`write`](Self::write) would emit.
/// * [`Type::validate`] — type-check `value` and return it back.
/// * [`Type::is_nullable`] — whether `null`/`None` is a legal value.
#[derive(Debug, Clone, PartialEq)]
pub enum Type {
    /// `BOOLEAN`.
    Boolean,
    /// `INT8`.
    Int8,
    /// `INT16`.
    Int16,
    /// `UINT16`.
    UInt16,
    /// `INT32`.
    Int32,
    /// `UNSIGNED_INT32`.
    UnsignedInt32,
    /// `INT64`.
    Int64,
    /// `UUID`.
    Uuid,
    /// `FLOAT64`.
    Float64,
    /// `STRING`.
    String,
    /// `COMPACT_STRING`.
    CompactString,
    /// `NULLABLE_STRING`.
    NullableString,
    /// `COMPACT_NULLABLE_STRING`.
    CompactNullableString,
    /// `BYTES`.
    Bytes,
    /// `COMPACT_BYTES`.
    CompactBytes,
    /// `NULLABLE_BYTES`.
    NullableBytes,
    /// `COMPACT_NULLABLE_BYTES`.
    CompactNullableBytes,
    /// `RECORDS` — the records family is delegated to the bytes family for
    /// (de)serialisation; concrete `MemoryRecords` modelling is Phase 2c+.
    Records,
    /// `COMPACT_RECORDS`.
    CompactRecords,
    /// `VARINT`.
    Varint,
    /// `VARLONG`.
    Varlong,
    /// `ARRAY(T)` — a non-compact array.
    Array(Box<ArrayOf>),
    /// `COMPACT_ARRAY(T)`.
    CompactArray(Box<CompactArrayOf>),
    /// `TAGGED_FIELDS`.
    TaggedFields(Box<TaggedFields>),
    /// `SCHEMA` — a struct/record.
    Schema(Box<Schema>),
}

impl Type {
    /// Whether `null` is a valid value for this type. Mirrors
    /// `Type#isNullable`.
    pub fn is_nullable(&self) -> bool {
        matches!(
            self,
            Type::NullableString
                | Type::CompactNullableString
                | Type::NullableBytes
                | Type::CompactNullableBytes
                | Type::Records
                | Type::CompactRecords
        ) || matches!(self, Type::Array(a) if a.is_nullable())
            || matches!(self, Type::CompactArray(a) if a.is_nullable())
    }

    /// If this is an array type, return the element type. Mirrors
    /// `Type#arrayElementType`.
    pub fn array_element_type(&self) -> Option<&Type> {
        match self {
            Type::Array(a) => Some(a.element_type()),
            Type::CompactArray(a) => Some(a.element_type()),
            _ => None,
        }
    }

    /// Whether this is an array type. Mirrors `Type#isArray`.
    pub fn is_array(&self) -> bool {
        self.array_element_type().is_some()
    }

    /// Short identifier, e.g. `"INT8"` or `"COMPACT_STRING"`. Mirrors
    /// `DocumentedType#typeName`.
    pub fn type_name(&self) -> &'static str {
        match self {
            Type::Boolean => "BOOLEAN",
            Type::Int8 => "INT8",
            Type::Int16 => "INT16",
            Type::UInt16 => "UINT16",
            Type::Int32 => "INT32",
            Type::UnsignedInt32 => "UINT32",
            Type::Int64 => "INT64",
            Type::Uuid => "UUID",
            Type::Float64 => "FLOAT64",
            Type::String => "STRING",
            Type::CompactString => "COMPACT_STRING",
            Type::NullableString => "NULLABLE_STRING",
            Type::CompactNullableString => "COMPACT_NULLABLE_STRING",
            Type::Bytes => "BYTES",
            Type::CompactBytes => "COMPACT_BYTES",
            Type::NullableBytes => "NULLABLE_BYTES",
            Type::CompactNullableBytes => "COMPACT_NULLABLE_BYTES",
            Type::Records => "RECORDS",
            Type::CompactRecords => "COMPACT_RECORDS",
            Type::Varint => "VARINT",
            Type::Varlong => "VARLONG",
            Type::Array(_) => "ARRAY",
            Type::CompactArray(_) => "COMPACT_ARRAY",
            Type::TaggedFields(_) => "TAGGED_FIELDS",
            Type::Schema(_) => "SCHEMA",
        }
    }

    /// Validate that `value` matches this type and return it (un-modified).
    /// Mirrors `Type#validate`. Returns a [`KafkaError::Generic`] (Java
    /// `SchemaException`) on a mismatch.
    pub fn validate(&self, value: &Value) -> Result<(), KafkaError> {
        match (self, value) {
            (Type::Boolean, Value::Bool(_)) => Ok(()),
            (Type::Int8, Value::Int8(_)) => Ok(()),
            (Type::Int16, Value::Int16(_)) => Ok(()),
            (Type::UInt16, Value::UInt16(_)) => Ok(()),
            (Type::Int32, Value::Int32(_)) => Ok(()),
            (Type::UnsignedInt32, Value::UInt32(_)) => Ok(()),
            (Type::Int64, Value::Int64(_)) => Ok(()),
            (Type::Uuid, Value::Uuid(_)) => Ok(()),
            (Type::Float64, Value::Float64(_)) => Ok(()),
            (Type::String | Type::CompactString, Value::String(_)) => Ok(()),
            (Type::NullableString | Type::CompactNullableString, Value::Null) => Ok(()),
            (Type::NullableString | Type::CompactNullableString, Value::String(_)) => Ok(()),
            (Type::Bytes | Type::CompactBytes, Value::Bytes(_)) => Ok(()),
            (Type::NullableBytes | Type::CompactNullableBytes | Type::Records | Type::CompactRecords, Value::Null) => {
                Ok(())
            },
            (
                Type::NullableBytes | Type::CompactNullableBytes | Type::Records | Type::CompactRecords,
                Value::Bytes(_),
            ) => Ok(()),
            (Type::Varint, Value::Int32(_)) => Ok(()),
            (Type::Varlong, Value::Int64(_)) => Ok(()),
            (Type::Array(a), Value::Null) if a.is_nullable() => Ok(()),
            (Type::Array(a), Value::Array(items)) => {
                for item in items {
                    a.element_type().validate(item)?;
                }
                Ok(())
            },
            (Type::CompactArray(a), Value::Null) if a.is_nullable() => Ok(()),
            (Type::CompactArray(a), Value::Array(items)) => {
                for item in items {
                    a.element_type().validate(item)?;
                }
                Ok(())
            },
            (Type::TaggedFields(_), Value::TaggedFields(_)) => Ok(()),
            (Type::Schema(s), Value::Struct(st)) => s.validate_struct(st),
            // Mismatch.
            (t, v) => Err(schema_exception(format!(
                "{} is not a {}.",
                describe_value(v),
                java_value_kind(t)
            ))),
        }
    }

    /// Return the size in bytes of `value` when encoded by this type.
    /// Mirrors `Type#sizeOf`.
    pub fn size_of(&self, value: &Value) -> Result<usize, KafkaError> {
        match (self, value) {
            (Type::Boolean, _) => Ok(1),
            (Type::Int8, _) => Ok(1),
            (Type::Int16, _) => Ok(2),
            (Type::UInt16, _) => Ok(2),
            (Type::Int32, _) => Ok(4),
            (Type::UnsignedInt32, _) => Ok(4),
            (Type::Int64, _) => Ok(8),
            (Type::Uuid, _) => Ok(16),
            (Type::Float64, _) => Ok(8),

            (Type::String, Value::String(s)) => Ok(2 + s.len()),
            (Type::CompactString, Value::String(s)) => {
                let len = s.len();
                Ok(byte_utils::size_of_unsigned_varint((len + 1) as u32) + len)
            },
            (Type::NullableString, Value::Null) => Ok(2),
            (Type::NullableString, Value::String(s)) => Ok(2 + s.len()),
            (Type::CompactNullableString, Value::Null) => Ok(1),
            (Type::CompactNullableString, Value::String(s)) => {
                let len = s.len();
                Ok(byte_utils::size_of_unsigned_varint((len + 1) as u32) + len)
            },

            (Type::Bytes, Value::Bytes(b)) => Ok(4 + b.len()),
            (Type::CompactBytes, Value::Bytes(b)) => {
                Ok(byte_utils::size_of_unsigned_varint((b.len() + 1) as u32) + b.len())
            },
            (Type::NullableBytes | Type::Records, Value::Null) => Ok(4),
            (Type::NullableBytes | Type::Records, Value::Bytes(b)) => Ok(4 + b.len()),
            (Type::CompactNullableBytes | Type::CompactRecords, Value::Null) => Ok(1),
            (Type::CompactNullableBytes | Type::CompactRecords, Value::Bytes(b)) => {
                Ok(byte_utils::size_of_unsigned_varint((b.len() + 1) as u32) + b.len())
            },

            (Type::Varint, Value::Int32(v)) => Ok(byte_utils::size_of_varint(*v)),
            (Type::Varlong, Value::Int64(v)) => Ok(byte_utils::size_of_varlong(*v)),

            (Type::Array(a), Value::Null) if a.is_nullable() => Ok(4),
            (Type::Array(a), Value::Array(items)) => {
                let mut size = 4usize;
                for it in items {
                    size += a.element_type().size_of(it)?;
                }
                Ok(size)
            },
            (Type::CompactArray(a), Value::Null) if a.is_nullable() => Ok(1),
            (Type::CompactArray(a), Value::Array(items)) => {
                let mut size = byte_utils::size_of_unsigned_varint((items.len() + 1) as u32);
                for it in items {
                    size += a.element_type().size_of(it)?;
                }
                Ok(size)
            },
            (Type::TaggedFields(tf), Value::TaggedFields(map)) => tagged_fields_size(tf, map),
            (Type::Schema(s), Value::Struct(st)) => s.size_of_struct(st),

            (t, v) => Err(schema_exception(format!(
                "Cannot compute size: {} is not a {}.",
                describe_value(v),
                java_value_kind(t)
            ))),
        }
    }

    /// Encode `value` by writing primitives to `buffer`. Mirrors `Type#write`.
    pub fn write(&self, buffer: &mut dyn Writable, value: &Value) -> Result<(), KafkaError> {
        match (self, value) {
            (Type::Boolean, Value::Bool(b)) => {
                buffer.write_byte(if *b { 1 } else { 0 });
                Ok(())
            },
            (Type::Int8, Value::Int8(v)) => {
                buffer.write_byte(*v);
                Ok(())
            },
            (Type::Int16, Value::Int16(v)) => {
                buffer.write_short(*v);
                Ok(())
            },
            (Type::UInt16, Value::UInt16(v)) => {
                buffer.write_unsigned_short(*v);
                Ok(())
            },
            (Type::Int32, Value::Int32(v)) => {
                buffer.write_int(*v);
                Ok(())
            },
            (Type::UnsignedInt32, Value::UInt32(v)) => {
                buffer.write_unsigned_int(*v);
                Ok(())
            },
            (Type::Int64, Value::Int64(v)) => {
                buffer.write_long(*v);
                Ok(())
            },
            (Type::Uuid, Value::Uuid(u)) => {
                buffer.write_uuid(u);
                Ok(())
            },
            (Type::Float64, Value::Float64(v)) => {
                buffer.write_double(*v);
                Ok(())
            },

            (Type::String, Value::String(s)) => write_string_short_prefixed(buffer, s),
            (Type::CompactString, Value::String(s)) => write_string_compact(buffer, s),
            (Type::NullableString, Value::Null) => {
                buffer.write_short(-1);
                Ok(())
            },
            (Type::NullableString, Value::String(s)) => write_string_short_prefixed(buffer, s),
            (Type::CompactNullableString, Value::Null) => {
                buffer.write_unsigned_varint(0);
                Ok(())
            },
            (Type::CompactNullableString, Value::String(s)) => write_string_compact(buffer, s),

            (Type::Bytes, Value::Bytes(b)) => {
                buffer.write_int(b.len() as i32);
                buffer.write_byte_array(b);
                Ok(())
            },
            (Type::CompactBytes, Value::Bytes(b)) => {
                buffer.write_unsigned_varint((b.len() + 1) as u32);
                buffer.write_byte_array(b);
                Ok(())
            },
            (Type::NullableBytes | Type::Records, Value::Null) => {
                buffer.write_int(-1);
                Ok(())
            },
            (Type::NullableBytes | Type::Records, Value::Bytes(b)) => {
                buffer.write_int(b.len() as i32);
                buffer.write_byte_array(b);
                Ok(())
            },
            (Type::CompactNullableBytes | Type::CompactRecords, Value::Null) => {
                buffer.write_unsigned_varint(0);
                Ok(())
            },
            (Type::CompactNullableBytes | Type::CompactRecords, Value::Bytes(b)) => {
                buffer.write_unsigned_varint((b.len() + 1) as u32);
                buffer.write_byte_array(b);
                Ok(())
            },

            (Type::Varint, Value::Int32(v)) => {
                buffer.write_varint(*v);
                Ok(())
            },
            (Type::Varlong, Value::Int64(v)) => {
                buffer.write_varlong(*v);
                Ok(())
            },

            (Type::Array(a), Value::Null) if a.is_nullable() => {
                buffer.write_int(-1);
                Ok(())
            },
            (Type::Array(a), Value::Array(items)) => {
                buffer.write_int(items.len() as i32);
                for it in items {
                    a.element_type().write(buffer, it)?;
                }
                Ok(())
            },
            (Type::CompactArray(a), Value::Null) if a.is_nullable() => {
                buffer.write_unsigned_varint(0);
                Ok(())
            },
            (Type::CompactArray(a), Value::Array(items)) => {
                buffer.write_unsigned_varint((items.len() + 1) as u32);
                for it in items {
                    a.element_type().write(buffer, it)?;
                }
                Ok(())
            },
            (Type::TaggedFields(tf), Value::TaggedFields(map)) => tagged_fields_write(tf, map, buffer),
            (Type::Schema(s), Value::Struct(st)) => s.write_struct(buffer, st),

            (t, v) => Err(schema_exception(format!(
                "Cannot write: {} is not a {}.",
                describe_value(v),
                java_value_kind(t)
            ))),
        }
    }

    /// Decode the next value from `buffer`. Mirrors `Type#read`.
    pub fn read(&self, buffer: &mut dyn Readable) -> Result<Value, KafkaError> {
        match self {
            Type::Boolean => Ok(Value::Bool(buffer.read_byte()? != 0)),
            Type::Int8 => Ok(Value::Int8(buffer.read_byte()?)),
            Type::Int16 => Ok(Value::Int16(buffer.read_short()?)),
            Type::UInt16 => Ok(Value::UInt16(buffer.read_unsigned_short()?)),
            Type::Int32 => Ok(Value::Int32(buffer.read_int()?)),
            Type::UnsignedInt32 => Ok(Value::UInt32(buffer.read_unsigned_int()?)),
            Type::Int64 => Ok(Value::Int64(buffer.read_long()?)),
            Type::Uuid => Ok(Value::Uuid(buffer.read_uuid()?)),
            Type::Float64 => Ok(Value::Float64(buffer.read_double()?)),
            Type::String => {
                let length = buffer.read_short()?;
                if length < 0 {
                    return Err(schema_exception(format!("String length {length} cannot be negative")));
                }
                let length = length as usize;
                if length > buffer.remaining() {
                    return Err(schema_exception(format!(
                        "Error reading string of length {length}, only {} bytes available",
                        buffer.remaining()
                    )));
                }
                Ok(Value::String(buffer.read_string(length)?))
            },
            Type::CompactString => {
                let raw = buffer.read_unsigned_varint()? as i64 - 1;
                if raw < 0 {
                    return Err(schema_exception(format!("String length {raw} cannot be negative")));
                }
                let length = raw as usize;
                if length > i16::MAX as usize {
                    return Err(schema_exception(format!(
                        "String length {length} is larger than the maximum string length."
                    )));
                }
                if length > buffer.remaining() {
                    return Err(schema_exception(format!(
                        "Error reading string of length {length}, only {} bytes available",
                        buffer.remaining()
                    )));
                }
                Ok(Value::String(buffer.read_string(length)?))
            },
            Type::NullableString => {
                let length = buffer.read_short()?;
                if length < 0 {
                    return Ok(Value::Null);
                }
                let length = length as usize;
                if length > buffer.remaining() {
                    return Err(schema_exception(format!(
                        "Error reading string of length {length}, only {} bytes available",
                        buffer.remaining()
                    )));
                }
                Ok(Value::String(buffer.read_string(length)?))
            },
            Type::CompactNullableString => {
                let raw = buffer.read_unsigned_varint()? as i64 - 1;
                if raw < 0 {
                    return Ok(Value::Null);
                }
                let length = raw as usize;
                if length > i16::MAX as usize {
                    return Err(schema_exception(format!(
                        "String length {length} is larger than the maximum string length."
                    )));
                }
                if length > buffer.remaining() {
                    return Err(schema_exception(format!(
                        "Error reading string of length {length}, only {} bytes available",
                        buffer.remaining()
                    )));
                }
                Ok(Value::String(buffer.read_string(length)?))
            },
            Type::Bytes => {
                let size = buffer.read_int()?;
                if size < 0 {
                    return Err(schema_exception(format!("Bytes size {size} cannot be negative")));
                }
                let size = size as usize;
                if size > buffer.remaining() {
                    return Err(schema_exception(format!(
                        "Error reading bytes of size {size}, only {} bytes available",
                        buffer.remaining()
                    )));
                }
                Ok(Value::Bytes(buffer.read_array(size)?))
            },
            Type::CompactBytes => {
                let raw = buffer.read_unsigned_varint()? as i64 - 1;
                if raw < 0 {
                    return Err(schema_exception(format!("Bytes size {raw} cannot be negative")));
                }
                let size = raw as usize;
                if size > buffer.remaining() {
                    return Err(schema_exception(format!(
                        "Error reading bytes of size {size}, only {} bytes available",
                        buffer.remaining()
                    )));
                }
                Ok(Value::Bytes(buffer.read_array(size)?))
            },
            Type::NullableBytes | Type::Records => {
                let size = buffer.read_int()?;
                if size < 0 {
                    return Ok(Value::Null);
                }
                let size = size as usize;
                if size > buffer.remaining() {
                    return Err(schema_exception(format!(
                        "Error reading bytes of size {size}, only {} bytes available",
                        buffer.remaining()
                    )));
                }
                Ok(Value::Bytes(buffer.read_array(size)?))
            },
            Type::CompactNullableBytes | Type::CompactRecords => {
                let raw = buffer.read_unsigned_varint()? as i64 - 1;
                if raw < 0 {
                    return Ok(Value::Null);
                }
                let size = raw as usize;
                if size > buffer.remaining() {
                    return Err(schema_exception(format!(
                        "Error reading bytes of size {size}, only {} bytes available",
                        buffer.remaining()
                    )));
                }
                Ok(Value::Bytes(buffer.read_array(size)?))
            },
            Type::Varint => Ok(Value::Int32(buffer.read_varint()?)),
            Type::Varlong => Ok(Value::Int64(buffer.read_varlong()?)),
            Type::Array(a) => read_array_value(a.element_type(), a.is_nullable(), buffer),
            Type::CompactArray(a) => read_compact_array_value(a.element_type(), a.is_nullable(), buffer),
            Type::TaggedFields(tf) => tagged_fields_read(tf, buffer),
            Type::Schema(s) => Ok(Value::Struct(Box::new(s.read_struct(buffer)?))),
        }
    }
}

impl fmt::Display for Type {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Type::Array(a) => write!(f, "ARRAY({})", a.element_type()),
            Type::CompactArray(a) => write!(f, "COMPACT_ARRAY({})", a.element_type()),
            Type::TaggedFields(_) => write!(f, "TAGGED_FIELDS"),
            Type::Schema(s) => write!(f, "{s}"),
            _ => f.write_str(self.type_name()),
        }
    }
}

fn write_string_short_prefixed(buffer: &mut dyn Writable, s: &str) -> Result<(), KafkaError> {
    let bytes = s.as_bytes();
    if bytes.len() > i16::MAX as usize {
        return Err(schema_exception(format!(
            "String length {} is larger than the maximum string length.",
            bytes.len()
        )));
    }
    buffer.write_short(bytes.len() as i16);
    buffer.write_byte_array(bytes);
    Ok(())
}

fn write_string_compact(buffer: &mut dyn Writable, s: &str) -> Result<(), KafkaError> {
    let bytes = s.as_bytes();
    if bytes.len() > i16::MAX as usize {
        return Err(schema_exception(format!(
            "String length {} is larger than the maximum string length.",
            bytes.len()
        )));
    }
    buffer.write_unsigned_varint((bytes.len() + 1) as u32);
    buffer.write_byte_array(bytes);
    Ok(())
}

fn read_array_value(element: &Type, nullable: bool, buffer: &mut dyn Readable) -> Result<Value, KafkaError> {
    let size = buffer.read_int()?;
    if size < 0 {
        if nullable {
            return Ok(Value::Null);
        }
        return Err(schema_exception(format!("Array size {size} cannot be negative")));
    }
    let size = size as usize;
    if size > buffer.remaining() {
        return Err(schema_exception(format!(
            "Error reading array of size {size}, only {} bytes available",
            buffer.remaining()
        )));
    }
    let mut items = Vec::with_capacity(size);
    for _ in 0..size {
        items.push(element.read(buffer)?);
    }
    Ok(Value::Array(items))
}

fn read_compact_array_value(element: &Type, nullable: bool, buffer: &mut dyn Readable) -> Result<Value, KafkaError> {
    let n = buffer.read_unsigned_varint()?;
    if n == 0 {
        if nullable {
            return Ok(Value::Null);
        }
        return Err(schema_exception("This array is not nullable."));
    }
    let size = (n - 1) as usize;
    if size > buffer.remaining() {
        return Err(schema_exception(format!(
            "Error reading array of size {size}, only {} bytes available",
            buffer.remaining()
        )));
    }
    let mut items = Vec::with_capacity(size);
    for _ in 0..size {
        items.push(element.read(buffer)?);
    }
    Ok(Value::Array(items))
}

fn tagged_fields_size(tf: &TaggedFields, map: &BTreeMap<i32, Value>) -> Result<usize, KafkaError> {
    let mut size = byte_utils::size_of_unsigned_varint(map.len() as u32);
    for (tag, val) in map {
        size += byte_utils::size_of_unsigned_varint(*tag as u32);
        if let Some(field) = tf.fields().get(tag) {
            let value_size = field.r#type.size_of(val)?;
            size += value_size + byte_utils::size_of_unsigned_varint(value_size as u32);
        } else if let Value::RawTagged(rtf) = val {
            size += rtf.data().len() + byte_utils::size_of_unsigned_varint(rtf.data().len() as u32);
        } else {
            return Err(schema_exception(format!(
                "The value associated with tag {tag} must be a RawTaggedField in this version of the software."
            )));
        }
    }
    Ok(size)
}

fn tagged_fields_write(
    tf: &TaggedFields,
    map: &BTreeMap<i32, Value>,
    buffer: &mut dyn Writable,
) -> Result<(), KafkaError> {
    buffer.write_unsigned_varint(map.len() as u32);
    for (tag, val) in map {
        buffer.write_unsigned_varint(*tag as u32);
        if let Some(field) = tf.fields().get(tag) {
            let value_size = field.r#type.size_of(val)?;
            buffer.write_unsigned_varint(value_size as u32);
            field.r#type.write(buffer, val)?;
        } else if let Value::RawTagged(rtf) = val {
            buffer.write_unsigned_varint(rtf.data().len() as u32);
            buffer.write_byte_array(rtf.data());
        } else {
            return Err(schema_exception(format!(
                "The value associated with tag {tag} must be a RawTaggedField in this version of the software."
            )));
        }
    }
    Ok(())
}

fn tagged_fields_read(tf: &TaggedFields, buffer: &mut dyn Readable) -> Result<Value, KafkaError> {
    let num = buffer.read_unsigned_varint()?;
    let mut map: BTreeMap<i32, Value> = BTreeMap::new();
    let mut prev_tag: i64 = -1;
    for _ in 0..num {
        let tag = buffer.read_unsigned_varint()? as i32;
        if (tag as i64) <= prev_tag {
            return Err(schema_exception(format!("Invalid or out-of-order tag {tag}")));
        }
        prev_tag = tag as i64;
        let size = buffer.read_unsigned_varint()? as i64;
        if size < 0 {
            return Err(schema_exception(format!("field size {size} cannot be negative")));
        }
        let size = size as usize;
        if size > buffer.remaining() {
            return Err(schema_exception(format!(
                "Error reading field of size {size}, only {} bytes available",
                buffer.remaining()
            )));
        }
        if let Some(field) = tf.fields().get(&tag) {
            // Bound the inner read to `size` bytes by reading them out into
            // an owned `Vec<u8>` and decoding the value from a fresh
            // accessor. This mirrors Java's behaviour of letting `Type#read`
            // consume from the buffer up to `size` bytes.
            let inner_bytes = buffer.read_array(size)?;
            let mut inner = ByteBufferAccessor::wrap(inner_bytes);
            let value = field.r#type.read(&mut inner)?;
            if inner.remaining() > 0 {
                return Err(schema_exception(format!(
                    "Tagged field of size {size} had {} extra bytes after decoding",
                    inner.remaining()
                )));
            }
            map.insert(tag, value);
        } else {
            let bytes = buffer.read_array(size)?;
            map.insert(tag, Value::RawTagged(RawTaggedField::new(tag, bytes)));
        }
    }
    Ok(Value::TaggedFields(map))
}

/// Best-effort string description of a `Value` for error messages,
/// approximating Java's `Object#toString`.
fn describe_value(v: &Value) -> String {
    match v {
        Value::Null => "null".to_string(),
        Value::Bool(b) => b.to_string(),
        Value::Int8(n) => n.to_string(),
        Value::Int16(n) => n.to_string(),
        Value::Int32(n) => n.to_string(),
        Value::UInt16(n) => n.to_string(),
        Value::Int64(n) => n.to_string(),
        Value::UInt32(n) => n.to_string(),
        Value::Float64(n) => n.to_string(),
        Value::String(s) => s.clone(),
        Value::Bytes(_) => "ByteBuffer".to_string(),
        Value::Array(_) => "Array".to_string(),
        Value::Struct(_) => "Struct".to_string(),
        Value::Uuid(u) => u.to_string(),
        Value::TaggedFields(_) => "TaggedFields".to_string(),
        Value::RawTagged(_) => "RawTaggedField".to_string(),
    }
}

/// Java boxed-type name for use in `SchemaException` messages, matching
/// strings the Java tests assert on.
fn java_value_kind(t: &Type) -> &'static str {
    match t {
        Type::Boolean => "Boolean",
        Type::Int8 => "Byte",
        Type::Int16 => "Short",
        Type::UInt16 => "Integer (encoding an unsigned short)",
        Type::Int32 | Type::Varint => "Integer",
        Type::UnsignedInt32 => "Long (encoding an unsigned integer)",
        Type::Int64 | Type::Varlong => "Long",
        Type::Uuid => "Uuid",
        Type::Float64 => "Double",
        Type::String | Type::CompactString | Type::NullableString | Type::CompactNullableString => "String",
        Type::Bytes | Type::CompactBytes | Type::NullableBytes | Type::CompactNullableBytes => "java.nio.ByteBuffer",
        Type::Records | Type::CompactRecords => "BaseRecords",
        Type::Array(_) | Type::CompactArray(_) => "Object[]",
        Type::TaggedFields(_) => "NavigableMap",
        Type::Schema(_) => "Struct",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::common::protocol::SliceReadable;
    use crate::common::protocol::types::field::Field;
    use crate::common::protocol::types::r#struct::Struct;

    /// Round-trip helper mirroring `ProtocolSerializationTest#roundtrip`.
    fn roundtrip(t: &Type, value: &Value) -> Value {
        let mut buf = Vec::with_capacity(t.size_of(value).unwrap());
        t.write(&mut buf, value).unwrap();
        assert_eq!(buf.len(), t.size_of(value).unwrap(), "buffer should be full");
        let mut r = SliceReadable::new(&buf);
        let read = t.read(&mut r).unwrap();
        assert_eq!(r.remaining(), 0, "all bytes should be read");
        read
    }

    fn check(t: Type, v: Value, expected: &str) {
        let result = roundtrip(&t, &v);
        assert_eq!(t.to_string(), expected, "Type::Display");
        assert_eq!(v, result);
    }

    /// Mirrors `ProtocolSerializationTest.testSimple`.
    #[test]
    fn simple() {
        check(Type::Boolean, Value::Bool(false), "BOOLEAN");
        check(Type::Boolean, Value::Bool(true), "BOOLEAN");
        check(Type::Int8, Value::Int8(-111), "INT8");
        check(Type::Int16, Value::Int16(-11111), "INT16");
        check(Type::Int32, Value::Int32(-11111111), "INT32");
        check(Type::Int64, Value::Int64(-11111111111), "INT64");
        check(Type::Float64, Value::Float64(2.5), "FLOAT64");
        check(Type::Float64, Value::Float64(-0.5), "FLOAT64");
        check(Type::Float64, Value::Float64(1e300), "FLOAT64");
        check(Type::Float64, Value::Float64(0.0), "FLOAT64");
        check(Type::Float64, Value::Float64(-0.0), "FLOAT64");
        check(Type::Float64, Value::Float64(f64::MAX), "FLOAT64");
        check(Type::Float64, Value::Float64(f64::MIN_POSITIVE), "FLOAT64");
        // NaN: round-trips via bit-equality, but f64 PartialEq returns false for NaN.
        // We emulate Java's `assertEquals(Double.NaN, …)` (which is true via boxed Double.equals)
        // by comparing bit patterns.
        let mut buf = Vec::new();
        Type::Float64.write(&mut buf, &Value::Float64(f64::NAN)).unwrap();
        let mut r = SliceReadable::new(&buf);
        let v = Type::Float64.read(&mut r).unwrap();
        if let Value::Float64(d) = v {
            assert!(d.is_nan());
        } else {
            panic!("expected Float64");
        }
        check(Type::Float64, Value::Float64(f64::NEG_INFINITY), "FLOAT64");
        check(Type::Float64, Value::Float64(f64::INFINITY), "FLOAT64");

        check(Type::String, Value::String(String::new()), "STRING");
        check(Type::String, Value::String("hello".into()), "STRING");
        check(Type::String, Value::String("A\u{00ea}\u{00f1}\u{00fc}C".into()), "STRING");
        check(Type::CompactString, Value::String(String::new()), "COMPACT_STRING");
        check(Type::CompactString, Value::String("hello".into()), "COMPACT_STRING");
        check(
            Type::CompactString,
            Value::String("A\u{00ea}\u{00f1}\u{00fc}C".into()),
            "COMPACT_STRING",
        );
        check(Type::NullableString, Value::Null, "NULLABLE_STRING");
        check(Type::NullableString, Value::String(String::new()), "NULLABLE_STRING");
        check(Type::NullableString, Value::String("hello".into()), "NULLABLE_STRING");
        check(Type::CompactNullableString, Value::Null, "COMPACT_NULLABLE_STRING");
        check(
            Type::CompactNullableString,
            Value::String(String::new()),
            "COMPACT_NULLABLE_STRING",
        );
        check(
            Type::CompactNullableString,
            Value::String("hello".into()),
            "COMPACT_NULLABLE_STRING",
        );

        check(Type::Bytes, Value::Bytes(Vec::new()), "BYTES");
        check(Type::Bytes, Value::Bytes(b"abcd".to_vec()), "BYTES");
        check(Type::CompactBytes, Value::Bytes(Vec::new()), "COMPACT_BYTES");
        check(Type::CompactBytes, Value::Bytes(b"abcd".to_vec()), "COMPACT_BYTES");
        check(Type::NullableBytes, Value::Null, "NULLABLE_BYTES");
        check(Type::NullableBytes, Value::Bytes(Vec::new()), "NULLABLE_BYTES");
        check(Type::NullableBytes, Value::Bytes(b"abcd".to_vec()), "NULLABLE_BYTES");
        check(Type::CompactNullableBytes, Value::Null, "COMPACT_NULLABLE_BYTES");
        check(Type::CompactNullableBytes, Value::Bytes(Vec::new()), "COMPACT_NULLABLE_BYTES");
        check(
            Type::CompactNullableBytes,
            Value::Bytes(b"abcd".to_vec()),
            "COMPACT_NULLABLE_BYTES",
        );

        check(Type::Varint, Value::Int32(i32::MAX), "VARINT");
        check(Type::Varint, Value::Int32(i32::MIN), "VARINT");
        check(Type::Varlong, Value::Int64(i64::MAX), "VARLONG");
        check(Type::Varlong, Value::Int64(i64::MIN), "VARLONG");

        check(
            Type::Array(Box::new(ArrayOf::new(Type::Int32))),
            Value::Array(vec![Value::Int32(1), Value::Int32(2), Value::Int32(3), Value::Int32(4)]),
            "ARRAY(INT32)",
        );
        check(
            Type::Array(Box::new(ArrayOf::new(Type::String))),
            Value::Array(Vec::new()),
            "ARRAY(STRING)",
        );
        check(
            Type::Array(Box::new(ArrayOf::new(Type::String))),
            Value::Array(vec![
                Value::String("hello".into()),
                Value::String("there".into()),
                Value::String("beautiful".into()),
            ]),
            "ARRAY(STRING)",
        );
        check(
            Type::CompactArray(Box::new(CompactArrayOf::new(Type::Int32))),
            Value::Array(vec![Value::Int32(1), Value::Int32(2), Value::Int32(3), Value::Int32(4)]),
            "COMPACT_ARRAY(INT32)",
        );
        check(
            Type::CompactArray(Box::new(CompactArrayOf::new(Type::CompactString))),
            Value::Array(Vec::new()),
            "COMPACT_ARRAY(COMPACT_STRING)",
        );
        check(
            Type::CompactArray(Box::new(CompactArrayOf::new(Type::CompactString))),
            Value::Array(vec![
                Value::String("hello".into()),
                Value::String("there".into()),
                Value::String("beautiful".into()),
            ]),
            "COMPACT_ARRAY(COMPACT_STRING)",
        );
        check(
            Type::Array(Box::new(ArrayOf::nullable(Type::String))),
            Value::Null,
            "ARRAY(STRING)",
        );
        check(
            Type::CompactArray(Box::new(CompactArrayOf::nullable(Type::CompactString))),
            Value::Null,
            "COMPACT_ARRAY(COMPACT_STRING)",
        );
    }

    /// Build the populated schema/struct from `ProtocolSerializationTest#setup`.
    fn setup() -> (Schema, Struct) {
        let inner =
            Schema::new(vec![Field::no_doc("field", Type::Array(Box::new(ArrayOf::new(Type::Int32))))]).unwrap();
        let schema = Schema::new(vec![
            Field::no_doc("boolean", Type::Boolean),
            Field::no_doc("int8", Type::Int8),
            Field::no_doc("int16", Type::Int16),
            Field::no_doc("int32", Type::Int32),
            Field::no_doc("int64", Type::Int64),
            Field::no_doc("varint", Type::Varint),
            Field::no_doc("varlong", Type::Varlong),
            Field::no_doc("float64", Type::Float64),
            Field::no_doc("string", Type::String),
            Field::no_doc("compact_string", Type::CompactString),
            Field::no_doc("nullable_string", Type::NullableString),
            Field::no_doc("compact_nullable_string", Type::CompactNullableString),
            Field::no_doc("bytes", Type::Bytes),
            Field::no_doc("compact_bytes", Type::CompactBytes),
            Field::no_doc("nullable_bytes", Type::NullableBytes),
            Field::no_doc("compact_nullable_bytes", Type::CompactNullableBytes),
            Field::no_doc("array", Type::Array(Box::new(ArrayOf::new(Type::Int32)))),
            Field::no_doc("compact_array", Type::CompactArray(Box::new(CompactArrayOf::new(Type::Int32)))),
            Field::no_doc("null_array", Type::Array(Box::new(ArrayOf::nullable(Type::Int32)))),
            Field::no_doc(
                "compact_null_array",
                Type::CompactArray(Box::new(CompactArrayOf::nullable(Type::Int32))),
            ),
            Field::no_doc("struct", Type::Schema(Box::new(inner))),
        ])
        .unwrap();

        let mut st = Struct::new(schema.clone());
        st.set_by_name("boolean", Value::Bool(true)).unwrap();
        st.set_by_name("int8", Value::Int8(1)).unwrap();
        st.set_by_name("int16", Value::Int16(1)).unwrap();
        st.set_by_name("int32", Value::Int32(1)).unwrap();
        st.set_by_name("int64", Value::Int64(1)).unwrap();
        st.set_by_name("varint", Value::Int32(300)).unwrap();
        st.set_by_name("varlong", Value::Int64(500)).unwrap();
        st.set_by_name("float64", Value::Float64(0.5)).unwrap();
        st.set_by_name("string", Value::String("1".into())).unwrap();
        st.set_by_name("compact_string", Value::String("1".into())).unwrap();
        st.set_by_name("nullable_string", Value::Null).unwrap();
        st.set_by_name("compact_nullable_string", Value::Null).unwrap();
        st.set_by_name("bytes", Value::Bytes(b"1".to_vec())).unwrap();
        st.set_by_name("compact_bytes", Value::Bytes(b"1".to_vec())).unwrap();
        st.set_by_name("nullable_bytes", Value::Null).unwrap();
        st.set_by_name("compact_nullable_bytes", Value::Null).unwrap();
        st.set_by_name("array", Value::Array(vec![Value::Int32(1)])).unwrap();
        st.set_by_name("compact_array", Value::Array(vec![Value::Int32(1)])).unwrap();
        st.set_by_name("null_array", Value::Null).unwrap();
        st.set_by_name("compact_null_array", Value::Null).unwrap();

        let mut child = st.instance_by_name("struct").unwrap();
        child
            .set_by_name("field", Value::Array(vec![Value::Int32(1), Value::Int32(2), Value::Int32(3)]))
            .unwrap();
        st.set_by_name("struct", Value::Struct(Box::new(child))).unwrap();
        (schema, st)
    }

    /// Mirrors `ProtocolSerializationTest.testNulls`.
    #[test]
    fn nulls() {
        let (schema, mut st) = setup();
        let cloned_schema = schema.clone();
        let names: Vec<String> = cloned_schema.fields().iter().map(|f| f.def.name.clone()).collect();
        for name in names {
            let original = st.get_by_name(&name).unwrap().clone();
            let bound_field = schema.get_by_name(&name).unwrap().clone();
            // Try to set null; if validation fails, the type is non-nullable.
            st.set_by_name(&name, Value::Null).unwrap();
            let validate_result = st.validate();
            if validate_result.is_ok() {
                assert!(
                    bound_field.def.r#type.is_nullable(),
                    "field {name} validated null but is not nullable"
                );
            } else {
                assert!(!bound_field.def.r#type.is_nullable(), "{name} should not be nullable");
            }
            st.set_by_name(&name, original).unwrap();
        }
    }

    /// Mirrors `ProtocolSerializationTest.testDefault`.
    #[test]
    fn default_value() {
        let schema = Schema::new(vec![
            Field::with_default("field", Type::Int32, "doc", Value::Int32(42)).unwrap(),
        ])
        .unwrap();
        let st = Struct::new(schema);
        assert_eq!(st.get_by_name("field").unwrap(), &Value::Int32(42));
        st.validate().unwrap();
    }

    fn check_nullable_default(t: Type, default: Value) {
        let schema = Schema::new(vec![Field::with_default("field", t, "doc", default.clone()).unwrap()]).unwrap();
        let st = Struct::new(schema);
        assert_eq!(st.get_by_name("field").unwrap(), &default);
        st.validate().unwrap();
    }

    /// Mirrors `ProtocolSerializationTest.testNullableDefault`.
    #[test]
    fn nullable_default() {
        check_nullable_default(Type::NullableBytes, Value::Bytes(Vec::new()));
        check_nullable_default(Type::CompactNullableBytes, Value::Bytes(Vec::new()));
        check_nullable_default(Type::NullableString, Value::String("default".into()));
        check_nullable_default(Type::CompactNullableString, Value::String("default".into()));
    }

    /// Mirrors `ProtocolSerializationTest.testReadArraySizeTooLarge`.
    #[test]
    fn read_array_size_too_large() {
        let t = Type::Array(Box::new(ArrayOf::new(Type::Int8)));
        let size = 10usize;
        let mut buf = Vec::with_capacity(4 + size);
        buf.extend_from_slice(&i32::MAX.to_be_bytes());
        for i in 0..size {
            buf.push(i as u8);
        }
        let mut r = SliceReadable::new(&buf);
        assert!(t.read(&mut r).is_err(), "Array size not validated");
    }

    /// Mirrors `ProtocolSerializationTest.testReadCompactArraySizeTooLarge`.
    #[test]
    fn read_compact_array_size_too_large() {
        let t = Type::CompactArray(Box::new(CompactArrayOf::new(Type::Int8)));
        let size = 10usize;
        let mut buf = Vec::new();
        byte_utils::write_unsigned_varint(i32::MAX as u32, &mut buf);
        for i in 0..size {
            buf.push(i as u8);
        }
        let mut r = SliceReadable::new(&buf);
        assert!(t.read(&mut r).is_err(), "Array size not validated");
    }

    /// Mirrors `ProtocolSerializationTest.testReadTaggedFieldsSizeTooLarge`.
    #[test]
    fn read_tagged_fields_size_too_large() {
        let tag = 1i32;
        let t = Type::TaggedFields(Box::new(TaggedFields::from_pairs(vec![(
            tag,
            Field::no_doc("field", Type::NullableString),
        )])));
        let size_total = 10usize;
        let mut buf = Vec::with_capacity(size_total);
        let num_tagged_fields = 1u32;
        byte_utils::write_unsigned_varint(num_tagged_fields, &mut buf);
        byte_utils::write_unsigned_varint(tag as u32, &mut buf);
        byte_utils::write_unsigned_varint(i32::MAX as u32, &mut buf);
        let expected_remaining = size_total - buf.len();
        // pad to total size (rest of allocation in Java's ByteBuffer)
        while buf.len() < size_total {
            buf.push(0);
        }
        let mut r = SliceReadable::new(&buf);
        let err = t.read(&mut r).unwrap_err();
        assert_eq!(
            err.message(),
            format!(
                "Error reading field of size {}, only {expected_remaining} bytes available",
                i32::MAX
            )
        );
    }

    /// Mirrors `ProtocolSerializationTest.testReadNegativeArraySize`.
    #[test]
    fn read_negative_array_size() {
        let t = Type::Array(Box::new(ArrayOf::new(Type::Int8)));
        let size = 10usize;
        let mut buf = Vec::with_capacity(4 + size);
        buf.extend_from_slice(&(-1i32).to_be_bytes());
        for i in 0..size {
            buf.push(i as u8);
        }
        let mut r = SliceReadable::new(&buf);
        assert!(t.read(&mut r).is_err(), "Array size not validated");
    }

    /// Mirrors `ProtocolSerializationTest.testReadZeroCompactArraySize`.
    #[test]
    fn read_zero_compact_array_size() {
        let t = Type::CompactArray(Box::new(CompactArrayOf::new(Type::Int8)));
        let size = 10usize;
        let mut buf = Vec::new();
        byte_utils::write_unsigned_varint(0, &mut buf);
        for i in 0..size {
            buf.push(i as u8);
        }
        let mut r = SliceReadable::new(&buf);
        assert!(t.read(&mut r).is_err(), "Array size not validated");
    }

    /// Mirrors `ProtocolSerializationTest.testReadStringSizeTooLarge`.
    #[test]
    fn read_string_size_too_large() {
        let bytes = b"foo";
        let mut buf = Vec::with_capacity(2 + bytes.len());
        buf.extend_from_slice(&((bytes.len() as i16) * 5).to_be_bytes());
        buf.extend_from_slice(bytes);

        let mut r = SliceReadable::new(&buf);
        assert!(Type::String.read(&mut r).is_err());
        let mut r = SliceReadable::new(&buf);
        assert!(Type::NullableString.read(&mut r).is_err());
    }

    /// Mirrors `ProtocolSerializationTest.testReadNegativeStringSize`.
    #[test]
    fn read_negative_string_size() {
        let bytes = b"foo";
        let mut buf = Vec::with_capacity(2 + bytes.len());
        buf.extend_from_slice(&(-1i16).to_be_bytes());
        buf.extend_from_slice(bytes);
        let mut r = SliceReadable::new(&buf);
        assert!(Type::String.read(&mut r).is_err());
    }

    /// Mirrors `ProtocolSerializationTest.testReadBytesSizeTooLarge`.
    #[test]
    fn read_bytes_size_too_large() {
        let bytes = b"foo";
        let mut buf = Vec::with_capacity(4 + bytes.len());
        buf.extend_from_slice(&((bytes.len() as i32) * 5).to_be_bytes());
        buf.extend_from_slice(bytes);

        let mut r = SliceReadable::new(&buf);
        assert!(Type::Bytes.read(&mut r).is_err());
        let mut r = SliceReadable::new(&buf);
        assert!(Type::NullableBytes.read(&mut r).is_err());
    }

    /// Mirrors `ProtocolSerializationTest.testReadNegativeBytesSize`.
    #[test]
    fn read_negative_bytes_size() {
        let bytes = b"foo";
        let mut buf = Vec::with_capacity(4 + bytes.len());
        buf.extend_from_slice(&(-20i32).to_be_bytes());
        buf.extend_from_slice(bytes);
        let mut r = SliceReadable::new(&buf);
        assert!(Type::Bytes.read(&mut r).is_err());
    }

    /// Mirrors `ProtocolSerializationTest.testToString`.
    #[test]
    fn to_string_test() {
        let (_schema, st) = setup();
        let s = st.to_string();
        assert!(!s.is_empty(), "struct string should not be empty");
    }

    /// Mirrors `ProtocolSerializationTest.testStructEquals`.
    #[test]
    fn struct_equals() {
        let schema = Schema::new(vec![
            Field::no_doc("field1", Type::NullableString),
            Field::no_doc("field2", Type::NullableString),
        ])
        .unwrap();
        let empty1 = Struct::new(schema.clone());
        let empty2 = Struct::new(schema.clone());
        assert_eq!(empty1, empty2);

        let mut mostly = Struct::new(schema.clone());
        mostly.set_by_name("field1", Value::String("foo".into())).unwrap();
        assert_ne!(empty1, mostly);
        assert_ne!(mostly, empty1);
    }

    /// Mirrors `ProtocolSerializationTest.testReadIgnoringExtraDataAtTheEnd`.
    #[test]
    fn read_ignoring_extra_data_at_the_end() {
        let old_schema = Schema::new(vec![
            Field::no_doc("field1", Type::NullableString),
            Field::no_doc("field2", Type::NullableString),
        ])
        .unwrap();
        let new_schema = Schema::new(vec![Field::no_doc("field1", Type::NullableString)]).unwrap();
        let value = "foo bar baz";
        let mut old_format = Struct::new(old_schema.clone());
        old_format.set_by_name("field1", Value::String(value.into())).unwrap();
        old_format
            .set_by_name("field2", Value::String("fine to ignore".into()))
            .unwrap();
        let mut buf = Vec::new();
        old_format.write_to(&mut buf).unwrap();
        let mut r = SliceReadable::new(&buf);
        let new_format = new_schema.read_struct(&mut r).unwrap();
        assert_eq!(new_format.get_by_name("field1").unwrap(), &Value::String(value.into()));
    }

    /// Mirrors `ProtocolSerializationTest.testReadWhenOptionalDataMissingAtTheEndIsTolerated`.
    #[test]
    fn read_when_optional_data_missing_at_the_end_is_tolerated() {
        let old_schema = Schema::new(vec![Field::no_doc("field1", Type::NullableString)]).unwrap();
        let new_schema = Schema::with_tolerance(
            true,
            vec![
                Field::no_doc("field1", Type::NullableString),
                Field::with_default("field2", Type::NullableString, "", Value::String("default".into())).unwrap(),
                Field::with_default("field3", Type::NullableString, "", Value::Null).unwrap(),
                Field::with_default("field4", Type::NullableBytes, "", Value::Bytes(Vec::new())).unwrap(),
                Field::with_default("field5", Type::Int64, "doc", Value::Int64(i64::MAX)).unwrap(),
            ],
        )
        .unwrap();
        let value = "foo bar baz";
        let mut old_format = Struct::new(old_schema);
        old_format.set_by_name("field1", Value::String(value.into())).unwrap();
        let mut buf = Vec::new();
        old_format.write_to(&mut buf).unwrap();
        let mut r = SliceReadable::new(&buf);
        let new_format = new_schema.read_struct(&mut r).unwrap();
        assert_eq!(new_format.get_by_name("field1").unwrap(), &Value::String(value.into()));
        assert_eq!(new_format.get_by_name("field2").unwrap(), &Value::String("default".into()));
        assert_eq!(new_format.get_by_name("field3").unwrap(), &Value::Null);
        assert_eq!(new_format.get_by_name("field4").unwrap(), &Value::Bytes(Vec::new()));
        assert_eq!(new_format.get_by_name("field5").unwrap(), &Value::Int64(i64::MAX));
    }

    /// Mirrors `ProtocolSerializationTest.testReadWhenOptionalDataMissingAtTheEndIsNotTolerated`.
    #[test]
    fn read_when_optional_data_missing_at_the_end_is_not_tolerated() {
        let old_schema = Schema::new(vec![Field::no_doc("field1", Type::NullableString)]).unwrap();
        let new_schema = Schema::new(vec![
            Field::no_doc("field1", Type::NullableString),
            Field::with_default("field2", Type::NullableString, "", Value::String("default".into())).unwrap(),
        ])
        .unwrap();
        let value = "foo bar baz";
        let mut old_format = Struct::new(old_schema);
        old_format.set_by_name("field1", Value::String(value.into())).unwrap();
        let mut buf = Vec::new();
        old_format.write_to(&mut buf).unwrap();
        let mut r = SliceReadable::new(&buf);
        let err = new_schema.read_struct(&mut r).unwrap_err();
        assert!(
            err.message().contains("Error reading field 'field2':"),
            "got: {}",
            err.message()
        );
    }

    /// Mirrors `ProtocolSerializationTest.testReadWithMissingNonOptionalExtraDataAtTheEnd`.
    #[test]
    fn read_with_missing_non_optional_extra_data_at_the_end() {
        let old_schema = Schema::new(vec![Field::no_doc("field1", Type::NullableString)]).unwrap();
        let new_schema = Schema::with_tolerance(
            true,
            vec![
                Field::no_doc("field1", Type::NullableString),
                Field::no_doc("field2", Type::NullableString),
            ],
        )
        .unwrap();
        let value = "foo bar baz";
        let mut old_format = Struct::new(old_schema);
        old_format.set_by_name("field1", Value::String(value.into())).unwrap();
        let mut buf = Vec::new();
        old_format.write_to(&mut buf).unwrap();
        let mut r = SliceReadable::new(&buf);
        let err = new_schema.read_struct(&mut r).unwrap_err();
        assert!(
            err.message()
                .contains("Missing value for field 'field2' which has no default value"),
            "got: {}",
            err.message()
        );
    }

    // ---------- Byte-vector encoding tests ----------
    //
    // CLAUDE.md DoD rule 3 requires byte-level encoding tests for wire types
    // alongside round-trip tests. The hex sequences below are derived from
    // the documented Kafka wire-protocol shape.

    #[test]
    fn boolean_bytes() {
        let mut buf = Vec::new();
        Type::Boolean.write(&mut buf, &Value::Bool(true)).unwrap();
        assert_eq!(buf, [0x01]);
        let mut buf = Vec::new();
        Type::Boolean.write(&mut buf, &Value::Bool(false)).unwrap();
        assert_eq!(buf, [0x00]);
    }

    #[test]
    fn int16_be_bytes() {
        let mut buf = Vec::new();
        Type::Int16.write(&mut buf, &Value::Int16(0x0102)).unwrap();
        assert_eq!(buf, [0x01, 0x02]);
        let mut buf = Vec::new();
        Type::Int16.write(&mut buf, &Value::Int16(-1)).unwrap();
        assert_eq!(buf, [0xFF, 0xFF]);
    }

    #[test]
    fn int32_be_bytes() {
        let mut buf = Vec::new();
        Type::Int32.write(&mut buf, &Value::Int32(0x01020304)).unwrap();
        assert_eq!(buf, [0x01, 0x02, 0x03, 0x04]);
    }

    #[test]
    fn int64_be_bytes() {
        let mut buf = Vec::new();
        Type::Int64.write(&mut buf, &Value::Int64(0x0102030405060708)).unwrap();
        assert_eq!(buf, [0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08]);
    }

    #[test]
    fn string_short_prefix_bytes() {
        let mut buf = Vec::new();
        Type::String.write(&mut buf, &Value::String("abc".into())).unwrap();
        assert_eq!(buf, [0x00, 0x03, b'a', b'b', b'c']);
        let mut buf = Vec::new();
        Type::String.write(&mut buf, &Value::String(String::new())).unwrap();
        assert_eq!(buf, [0x00, 0x00]);
    }

    #[test]
    fn nullable_string_null_bytes() {
        let mut buf = Vec::new();
        Type::NullableString.write(&mut buf, &Value::Null).unwrap();
        assert_eq!(buf, [0xFF, 0xFF]); // -1 i16
    }

    #[test]
    fn compact_string_bytes() {
        let mut buf = Vec::new();
        Type::CompactString.write(&mut buf, &Value::String("abc".into())).unwrap();
        // compact: varint(len+1), so varint(4) = 0x04, then "abc"
        assert_eq!(buf, [0x04, b'a', b'b', b'c']);
    }

    #[test]
    fn compact_nullable_string_null_bytes() {
        let mut buf = Vec::new();
        Type::CompactNullableString.write(&mut buf, &Value::Null).unwrap();
        // null compact string is varint 0
        assert_eq!(buf, [0x00]);
    }

    #[test]
    fn bytes_length_prefix_bytes() {
        let mut buf = Vec::new();
        Type::Bytes.write(&mut buf, &Value::Bytes(b"\x10\x20\x30".to_vec())).unwrap();
        assert_eq!(buf, [0x00, 0x00, 0x00, 0x03, 0x10, 0x20, 0x30]);
    }

    #[test]
    fn nullable_bytes_null_bytes() {
        let mut buf = Vec::new();
        Type::NullableBytes.write(&mut buf, &Value::Null).unwrap();
        assert_eq!(buf, [0xFF, 0xFF, 0xFF, 0xFF]); // -1 i32
    }

    #[test]
    fn array_int32_bytes() {
        let mut buf = Vec::new();
        Type::Array(Box::new(ArrayOf::new(Type::Int32)))
            .write(&mut buf, &Value::Array(vec![Value::Int32(1), Value::Int32(2)]))
            .unwrap();
        // size 2 (i32 BE) then two i32 BE values
        assert_eq!(buf, [0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x02]);
    }

    #[test]
    fn compact_array_int32_bytes() {
        let mut buf = Vec::new();
        Type::CompactArray(Box::new(CompactArrayOf::new(Type::Int32)))
            .write(&mut buf, &Value::Array(vec![Value::Int32(1), Value::Int32(2)]))
            .unwrap();
        // varint(3) = 0x03 then two i32 BE values
        assert_eq!(buf, [0x03, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x02]);
    }

    #[test]
    fn null_array_bytes() {
        let mut buf = Vec::new();
        Type::Array(Box::new(ArrayOf::nullable(Type::Int32)))
            .write(&mut buf, &Value::Null)
            .unwrap();
        assert_eq!(buf, [0xFF, 0xFF, 0xFF, 0xFF]);
    }

    #[test]
    fn null_compact_array_bytes() {
        let mut buf = Vec::new();
        Type::CompactArray(Box::new(CompactArrayOf::nullable(Type::Int32)))
            .write(&mut buf, &Value::Null)
            .unwrap();
        assert_eq!(buf, [0x00]);
    }

    #[test]
    fn varint_zigzag_bytes() {
        // zig-zag(1) = 2 -> single byte 0x02
        let mut buf = Vec::new();
        Type::Varint.write(&mut buf, &Value::Int32(1)).unwrap();
        assert_eq!(buf, [0x02]);
        // zig-zag(-1) = 1 -> 0x01
        let mut buf = Vec::new();
        Type::Varint.write(&mut buf, &Value::Int32(-1)).unwrap();
        assert_eq!(buf, [0x01]);
    }

    #[test]
    fn uuid_bytes_be() {
        let u = crate::common::Uuid::new(0x0102030405060708i64, 0x090A0B0C0D0E0F10i64);
        let mut buf = Vec::new();
        Type::Uuid.write(&mut buf, &Value::Uuid(u)).unwrap();
        assert_eq!(
            buf,
            [
                0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F, 0x10,
            ]
        );
    }

    #[test]
    fn empty_tagged_fields_bytes() {
        // Empty tagged fields => varint(0)
        let t = Type::TaggedFields(Box::new(TaggedFields::from_pairs(vec![])));
        let mut buf = Vec::new();
        t.write(&mut buf, &Value::TaggedFields(BTreeMap::new())).unwrap();
        assert_eq!(buf, [0x00]);
    }

    #[test]
    fn unsigned_int32_round_trip() {
        check(Type::UnsignedInt32, Value::UInt32(0xDEAD_BEEF), "UINT32");
    }

    #[test]
    fn uint16_round_trip() {
        check(Type::UInt16, Value::UInt16(0xFFFF), "UINT16");
    }

    #[test]
    fn uuid_round_trip() {
        check(Type::Uuid, Value::Uuid(crate::common::Uuid::new(0x1234, 0x5678)), "UUID");
    }

    /// Round-trip a `TaggedFields` value with two defined tags and one
    /// raw (unknown) tag. Mirrors the integration done implicitly in the
    /// Java `RequestHeaderTest` and message-generated tests.
    #[test]
    fn tagged_fields_round_trip_defined_and_raw() {
        let t = Type::TaggedFields(Box::new(TaggedFields::from_pairs(vec![
            (1, Field::no_doc("a", Type::Int32)),
            (3, Field::no_doc("b", Type::String)),
        ])));
        let mut map = BTreeMap::new();
        map.insert(1i32, Value::Int32(42));
        map.insert(3i32, Value::String("hello".into()));
        // Tag 7 is undefined; carry as raw bytes.
        map.insert(7i32, Value::RawTagged(RawTaggedField::new(7, b"raw".to_vec())));
        let v = Value::TaggedFields(map);
        let result = roundtrip(&t, &v);
        assert_eq!(v, result);
    }

    /// Mirrors `Schema#walk` semantics: the visitor sees the schema first,
    /// then each type once (and recurses into arrays).
    #[test]
    fn schema_walk_visits_in_order() {
        struct Collector {
            names: Vec<String>,
        }
        impl super::super::schema::SchemaVisitor for Collector {
            fn visit_schema(&mut self, _: &Schema) {
                self.names.push("Schema".into());
            }
            fn visit_type(&mut self, n: &Type) {
                self.names.push(n.type_name().into());
            }
        }
        let inner_schema = Schema::new(vec![Field::no_doc("inner_int", Type::Int32)]).unwrap();
        let outer = Schema::new(vec![
            Field::no_doc("a", Type::Int8),
            Field::no_doc("b", Type::Array(Box::new(ArrayOf::new(Type::String)))),
            Field::no_doc("c", Type::Schema(Box::new(inner_schema))),
        ])
        .unwrap();
        let mut c = Collector { names: Vec::new() };
        outer.walk(&mut c);
        // Outer schema visited first, then INT8, then ARRAY -> STRING, then nested schema (which visits Schema then INT32).
        assert_eq!(c.names, vec!["Schema", "INT8", "ARRAY", "STRING", "Schema", "INT32"]);
    }
}
