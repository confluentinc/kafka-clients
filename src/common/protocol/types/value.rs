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

//! A typed value representation for protocol-types.
//!
//! Java's `Type.write/read/validate/sizeOf` dispatch on `java.lang.Object`
//! and rely on runtime instance checks. Rust prefers a tagged enum, which
//! also avoids any heap allocation for the small numeric cases.
//!
//! The variants here mirror the Java boxed types each `Type` accepts:
//!
//! | Java                           | Rust [`Value`]                |
//! | ------------------------------ | ----------------------------- |
//! | `Boolean`                      | `Bool(bool)`                  |
//! | `Byte`                         | `Int8(i8)`                    |
//! | `Short`                        | `Int16(i16)`                  |
//! | `Integer`                      | `Int32(i32)`                  |
//! | `Long`                         | `Int64(i64)`                  |
//! | `Long` (UNSIGNED_INT32)        | `UInt32(u32)`                 |
//! | `Integer` (UINT16)             | `UInt16(u16)`                 |
//! | `Double`                       | `Float64(f64)`                |
//! | `String`                       | `String(String)`              |
//! | `ByteBuffer`/`byte[]`          | `Bytes(Vec<u8>)`              |
//! | `Object[]`                     | `Array(Vec<Value>)`           |
//! | `Struct`                       | `Struct(Box<Struct>)`         |
//! | `Uuid`                         | `Uuid(Uuid)`                  |
//! | `NavigableMap<Integer,Object>` | `TaggedFields(BTreeMap<…>)`   |
//! | `null`                         | `Null`                        |
//!
//! Returning `Vec<u8>` for `Bytes` rather than a borrowed slice mirrors
//! Java's `ByteBuffer.slice()` reads, which return a fresh view. Phase 2c
//! may revisit this when zero-copy `ProduceRequest` payloads come online.

use std::collections::BTreeMap;

use crate::common::Uuid;
use crate::common::protocol::types::r#struct::Struct;

/// A protocol value. The variants match the boxed types Java's `Type`
/// classes accept and produce.
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    /// `null` — only valid for nullable types.
    Null,
    /// `Boolean` (BOOLEAN).
    Bool(bool),
    /// `Byte` (INT8).
    Int8(i8),
    /// `Short` (INT16).
    Int16(i16),
    /// `Integer` (INT32, VARINT).
    Int32(i32),
    /// `Integer` (UINT16) — Java uses `Integer` for the unsigned short.
    UInt16(u16),
    /// `Long` (INT64, VARLONG).
    Int64(i64),
    /// `Long` (UNSIGNED_INT32) — Java uses a `Long` for the unsigned int.
    UInt32(u32),
    /// `Double` (FLOAT64).
    Float64(f64),
    /// `String` (STRING / COMPACT_STRING / NULLABLE_STRING /
    /// COMPACT_NULLABLE_STRING).
    String(String),
    /// `ByteBuffer` (BYTES, COMPACT_BYTES, NULLABLE_BYTES,
    /// COMPACT_NULLABLE_BYTES, RECORDS, COMPACT_RECORDS).
    Bytes(Vec<u8>),
    /// `Object[]` (ARRAY, COMPACT_ARRAY).
    Array(Vec<Value>),
    /// `Struct` (Schema-typed record).
    Struct(Box<Struct>),
    /// `Uuid` (UUID).
    Uuid(Uuid),
    /// `NavigableMap<Integer, Object>` (TAGGED_FIELDS).
    TaggedFields(BTreeMap<i32, Value>),
    /// A raw tagged field carried inside a TaggedFields map for tags the
    /// schema does not define.
    RawTagged(crate::common::protocol::types::raw_tagged_field::RawTaggedField),
}
