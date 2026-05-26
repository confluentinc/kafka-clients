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

//! Translation of `org.apache.kafka.common.protocol.types.BoundField`.
//!
//! A `BoundField` is a [`Field`] together with the [`Schema`] it belongs to
//! and its zero-based index inside that schema's field list. The Java type
//! enables fast slot lookup (`Struct.get(BoundField)`); we keep the same
//! contract by holding indices alongside the schema-identifying pointer.

use std::fmt;

use crate::common::protocol::types::field::Field;

/// A field definition bound to a particular schema.
///
/// Equality and identity in Rust differ from Java's reference equality. To
/// mimic the Java contract that the same `BoundField` cannot be reused
/// across schemas, [`crate::common::protocol::types::r#struct::Struct`]
/// validates that the `schema_id` matches before reading or writing values.
#[derive(Debug, Clone)]
pub struct BoundField {
    /// The bound field definition.
    pub def: Field,
    /// Zero-based slot in the parent schema.
    pub(crate) index: usize,
    /// Identifier of the owning schema. Each `Schema` allocates one
    /// monotonically increasing id; we compare via `==` to detect
    /// cross-schema misuse, mirroring Java's `this.schema != field.schema`.
    pub(crate) schema_id: u64,
}

impl BoundField {
    /// Construct a `BoundField`. Mirrors `BoundField(Field, Schema, int)`;
    /// `schema_id` is supplied by the parent [`super::Schema`].
    pub fn new(def: Field, schema_id: u64, index: usize) -> Self {
        BoundField { def, index, schema_id }
    }

    /// Slot index inside the parent schema.
    pub fn index(&self) -> usize {
        self.index
    }
}

impl fmt::Display for BoundField {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.def.name, self.def.r#type)
    }
}
