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
use crate::common::utils::byte_utils;

/// Read cursor over a borrowed byte slice. Mirrors `ByteBuffer`'s
/// position/limit/remaining trio used by Java's `Type#read`.
///
/// Phase 2c will replace this with a richer `ByteBufferAccessor` that also
/// tracks limit independently of length. For now, the cursor advances
/// monotonically through the borrowed slice.
pub struct ReadBuffer<'a> {
    data: &'a [u8],
    position: usize,
}

impl<'a> ReadBuffer<'a> {
    /// Construct a cursor positioned at the start of `data`.
    pub fn new(data: &'a [u8]) -> Self {
        ReadBuffer { data, position: 0 }
    }

    /// Bytes still available in the buffer (`limit() - position()`).
    pub fn remaining(&self) -> usize {
        self.data.len() - self.position
    }

    /// Whether there are any bytes left to read.
    pub fn has_remaining(&self) -> bool {
        self.position < self.data.len()
    }

    /// Current read position.
    pub fn position(&self) -> usize {
        self.position
    }

    fn ensure(&self, n: usize) -> Result<(), KafkaError> {
        if self.remaining() < n {
            return Err(schema_exception(format!(
                "Truncated buffer: needed {n} bytes, only {} available",
                self.remaining()
            )));
        }
        Ok(())
    }

    fn read_i8(&mut self) -> Result<i8, KafkaError> {
        self.ensure(1)?;
        let v = self.data[self.position] as i8;
        self.position += 1;
        Ok(v)
    }

    fn read_i16(&mut self) -> Result<i16, KafkaError> {
        self.ensure(2)?;
        let bytes: [u8; 2] = self.data[self.position..self.position + 2].try_into().unwrap();
        self.position += 2;
        Ok(i16::from_be_bytes(bytes))
    }

    fn read_i32(&mut self) -> Result<i32, KafkaError> {
        self.ensure(4)?;
        let bytes: [u8; 4] = self.data[self.position..self.position + 4].try_into().unwrap();
        self.position += 4;
        Ok(i32::from_be_bytes(bytes))
    }

    fn read_i64(&mut self) -> Result<i64, KafkaError> {
        self.ensure(8)?;
        let bytes: [u8; 8] = self.data[self.position..self.position + 8].try_into().unwrap();
        self.position += 8;
        Ok(i64::from_be_bytes(bytes))
    }

    fn read_unsigned_varint(&mut self) -> Result<u32, KafkaError> {
        let (value, len) = byte_utils::read_unsigned_varint(&self.data[self.position..])?;
        self.position += len;
        Ok(value)
    }

    fn read_varint(&mut self) -> Result<i32, KafkaError> {
        let (value, len) = byte_utils::read_varint(&self.data[self.position..])?;
        self.position += len;
        Ok(value)
    }

    fn read_varlong(&mut self) -> Result<i64, KafkaError> {
        let (value, len) = byte_utils::read_varlong(&self.data[self.position..])?;
        self.position += len;
        Ok(value)
    }

    fn read_double(&mut self) -> Result<f64, KafkaError> {
        self.ensure(8)?;
        let v = byte_utils::read_double_at(self.data, self.position);
        self.position += 8;
        Ok(v)
    }

    fn read_bytes(&mut self, n: usize) -> Result<Vec<u8>, KafkaError> {
        self.ensure(n)?;
        let v = self.data[self.position..self.position + n].to_vec();
        self.position += n;
        Ok(v)
    }

    fn read_string(&mut self, n: usize) -> Result<String, KafkaError> {
        let bytes = self.read_bytes(n)?;
        String::from_utf8(bytes).map_err(|e| schema_exception(format!("Invalid UTF-8: {e}")))
    }
}

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

            (Type::String, Value::String(s)) => Ok(2 + s.as_bytes().len()),
            (Type::CompactString, Value::String(s)) => {
                let len = s.as_bytes().len();
                Ok(byte_utils::size_of_unsigned_varint((len + 1) as u32) + len)
            },
            (Type::NullableString, Value::Null) => Ok(2),
            (Type::NullableString, Value::String(s)) => Ok(2 + s.as_bytes().len()),
            (Type::CompactNullableString, Value::Null) => Ok(1),
            (Type::CompactNullableString, Value::String(s)) => {
                let len = s.as_bytes().len();
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

    /// Encode `value` by appending bytes to `buffer`. Mirrors `Type#write`.
    pub fn write(&self, buffer: &mut Vec<u8>, value: &Value) -> Result<(), KafkaError> {
        match (self, value) {
            (Type::Boolean, Value::Bool(b)) => {
                buffer.push(if *b { 1 } else { 0 });
                Ok(())
            },
            (Type::Int8, Value::Int8(v)) => {
                buffer.push(*v as u8);
                Ok(())
            },
            (Type::Int16, Value::Int16(v)) => {
                buffer.extend_from_slice(&v.to_be_bytes());
                Ok(())
            },
            (Type::UInt16, Value::UInt16(v)) => {
                buffer.extend_from_slice(&v.to_be_bytes());
                Ok(())
            },
            (Type::Int32, Value::Int32(v)) => {
                buffer.extend_from_slice(&v.to_be_bytes());
                Ok(())
            },
            (Type::UnsignedInt32, Value::UInt32(v)) => {
                buffer.extend_from_slice(&v.to_be_bytes());
                Ok(())
            },
            (Type::Int64, Value::Int64(v)) => {
                buffer.extend_from_slice(&v.to_be_bytes());
                Ok(())
            },
            (Type::Uuid, Value::Uuid(u)) => {
                buffer.extend_from_slice(&u.most_significant_bits().to_be_bytes());
                buffer.extend_from_slice(&u.least_significant_bits().to_be_bytes());
                Ok(())
            },
            (Type::Float64, Value::Float64(v)) => {
                byte_utils::write_double(*v, buffer);
                Ok(())
            },

            (Type::String, Value::String(s)) => write_string_short_prefixed(buffer, s),
            (Type::CompactString, Value::String(s)) => write_string_compact(buffer, s),
            (Type::NullableString, Value::Null) => {
                buffer.extend_from_slice(&(-1i16).to_be_bytes());
                Ok(())
            },
            (Type::NullableString, Value::String(s)) => write_string_short_prefixed(buffer, s),
            (Type::CompactNullableString, Value::Null) => {
                byte_utils::write_unsigned_varint(0, buffer);
                Ok(())
            },
            (Type::CompactNullableString, Value::String(s)) => write_string_compact(buffer, s),

            (Type::Bytes, Value::Bytes(b)) => {
                buffer.extend_from_slice(&(b.len() as i32).to_be_bytes());
                buffer.extend_from_slice(b);
                Ok(())
            },
            (Type::CompactBytes, Value::Bytes(b)) => {
                byte_utils::write_unsigned_varint((b.len() + 1) as u32, buffer);
                buffer.extend_from_slice(b);
                Ok(())
            },
            (Type::NullableBytes | Type::Records, Value::Null) => {
                buffer.extend_from_slice(&(-1i32).to_be_bytes());
                Ok(())
            },
            (Type::NullableBytes | Type::Records, Value::Bytes(b)) => {
                buffer.extend_from_slice(&(b.len() as i32).to_be_bytes());
                buffer.extend_from_slice(b);
                Ok(())
            },
            (Type::CompactNullableBytes | Type::CompactRecords, Value::Null) => {
                byte_utils::write_unsigned_varint(0, buffer);
                Ok(())
            },
            (Type::CompactNullableBytes | Type::CompactRecords, Value::Bytes(b)) => {
                byte_utils::write_unsigned_varint((b.len() + 1) as u32, buffer);
                buffer.extend_from_slice(b);
                Ok(())
            },

            (Type::Varint, Value::Int32(v)) => {
                byte_utils::write_varint(*v, buffer);
                Ok(())
            },
            (Type::Varlong, Value::Int64(v)) => {
                byte_utils::write_varlong(*v, buffer);
                Ok(())
            },

            (Type::Array(a), Value::Null) if a.is_nullable() => {
                buffer.extend_from_slice(&(-1i32).to_be_bytes());
                Ok(())
            },
            (Type::Array(a), Value::Array(items)) => {
                buffer.extend_from_slice(&(items.len() as i32).to_be_bytes());
                for it in items {
                    a.element_type().write(buffer, it)?;
                }
                Ok(())
            },
            (Type::CompactArray(a), Value::Null) if a.is_nullable() => {
                byte_utils::write_unsigned_varint(0, buffer);
                Ok(())
            },
            (Type::CompactArray(a), Value::Array(items)) => {
                byte_utils::write_unsigned_varint((items.len() + 1) as u32, buffer);
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
    pub fn read(&self, buffer: &mut ReadBuffer<'_>) -> Result<Value, KafkaError> {
        match self {
            Type::Boolean => Ok(Value::Bool(buffer.read_i8()? != 0)),
            Type::Int8 => Ok(Value::Int8(buffer.read_i8()?)),
            Type::Int16 => Ok(Value::Int16(buffer.read_i16()?)),
            Type::UInt16 => {
                let v = buffer.read_i16()? as u16;
                Ok(Value::UInt16(v))
            },
            Type::Int32 => Ok(Value::Int32(buffer.read_i32()?)),
            Type::UnsignedInt32 => {
                let v = buffer.read_i32()? as u32;
                Ok(Value::UInt32(v))
            },
            Type::Int64 => Ok(Value::Int64(buffer.read_i64()?)),
            Type::Uuid => {
                let msb = buffer.read_i64()?;
                let lsb = buffer.read_i64()?;
                Ok(Value::Uuid(crate::common::Uuid::new(msb, lsb)))
            },
            Type::Float64 => Ok(Value::Float64(buffer.read_double()?)),
            Type::String => {
                let length = buffer.read_i16()?;
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
                let length = buffer.read_i16()?;
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
                let size = buffer.read_i32()?;
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
                Ok(Value::Bytes(buffer.read_bytes(size)?))
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
                Ok(Value::Bytes(buffer.read_bytes(size)?))
            },
            Type::NullableBytes | Type::Records => {
                let size = buffer.read_i32()?;
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
                Ok(Value::Bytes(buffer.read_bytes(size)?))
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
                Ok(Value::Bytes(buffer.read_bytes(size)?))
            },
            Type::Varint => Ok(Value::Int32(buffer.read_varint()?)),
            Type::Varlong => Ok(Value::Int64(buffer.read_varlong()?)),
            Type::Array(a) => read_array(a.element_type(), a.is_nullable(), buffer),
            Type::CompactArray(a) => read_compact_array(a.element_type(), a.is_nullable(), buffer),
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

fn write_string_short_prefixed(buffer: &mut Vec<u8>, s: &str) -> Result<(), KafkaError> {
    let bytes = s.as_bytes();
    if bytes.len() > i16::MAX as usize {
        return Err(schema_exception(format!(
            "String length {} is larger than the maximum string length.",
            bytes.len()
        )));
    }
    buffer.extend_from_slice(&(bytes.len() as i16).to_be_bytes());
    buffer.extend_from_slice(bytes);
    Ok(())
}

fn write_string_compact(buffer: &mut Vec<u8>, s: &str) -> Result<(), KafkaError> {
    let bytes = s.as_bytes();
    if bytes.len() > i16::MAX as usize {
        return Err(schema_exception(format!(
            "String length {} is larger than the maximum string length.",
            bytes.len()
        )));
    }
    byte_utils::write_unsigned_varint((bytes.len() + 1) as u32, buffer);
    buffer.extend_from_slice(bytes);
    Ok(())
}

fn read_array(element: &Type, nullable: bool, buffer: &mut ReadBuffer<'_>) -> Result<Value, KafkaError> {
    let size = buffer.read_i32()?;
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

fn read_compact_array(element: &Type, nullable: bool, buffer: &mut ReadBuffer<'_>) -> Result<Value, KafkaError> {
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

fn tagged_fields_write(tf: &TaggedFields, map: &BTreeMap<i32, Value>, buffer: &mut Vec<u8>) -> Result<(), KafkaError> {
    byte_utils::write_unsigned_varint(map.len() as u32, buffer);
    for (tag, val) in map {
        byte_utils::write_unsigned_varint(*tag as u32, buffer);
        if let Some(field) = tf.fields().get(tag) {
            let value_size = field.r#type.size_of(val)?;
            byte_utils::write_unsigned_varint(value_size as u32, buffer);
            field.r#type.write(buffer, val)?;
        } else if let Value::RawTagged(rtf) = val {
            byte_utils::write_unsigned_varint(rtf.data().len() as u32, buffer);
            buffer.extend_from_slice(rtf.data());
        } else {
            return Err(schema_exception(format!(
                "The value associated with tag {tag} must be a RawTaggedField in this version of the software."
            )));
        }
    }
    Ok(())
}

fn tagged_fields_read(tf: &TaggedFields, buffer: &mut ReadBuffer<'_>) -> Result<Value, KafkaError> {
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
            // Bound the inner read to `size` bytes by parsing from a
            // temporary slice; this matches Java's behaviour of letting
            // the Type#read consume from the buffer up to `size`.
            let slice_end = buffer.position + size;
            let value = {
                let inner_slice = &buffer.data[buffer.position..slice_end];
                let mut inner = ReadBuffer::new(inner_slice);
                let v = field.r#type.read(&mut inner)?;
                if inner.has_remaining() {
                    return Err(schema_exception(format!(
                        "Tagged field of size {size} had {} extra bytes after decoding",
                        inner.remaining()
                    )));
                }
                v
            };
            buffer.position = slice_end;
            map.insert(tag, value);
        } else {
            let bytes = buffer.read_bytes(size)?;
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
