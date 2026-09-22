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

//! Protocol schema types for field introspection.
//!
//! Provides runtime schema metadata for Kafka protocol messages, enabling
//! field lookup by name and schema traversal.

mod raw_tagged_field;
mod schema_error;

pub use raw_tagged_field::RawTaggedField;
pub use schema_error::SchemaError;

use std::collections::HashMap;

/// The type of a field in the schema.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SchemaType {
    Boolean,
    Int8,
    Int16,
    Uint16,
    Uint32,
    Int32,
    Int64,
    Uuid,
    Float64,
    String,
    NullableString,
    CompactString,
    CompactNullableString,
    Bytes,
    NullableBytes,
    CompactBytes,
    CompactNullableBytes,
    Records,
    CompactRecords,
}

/// A field definition in a schema.
#[derive(Debug, Clone)]
pub struct Field {
    /// The name of the field (snake_case).
    pub name: &'static str,
    /// The type of the field.
    pub field_type: SchemaType,
    /// Documentation string for the field.
    pub about: &'static str,
}

/// A tagged field definition in a schema.
#[derive(Debug, Clone)]
pub struct TaggedField {
    /// The tag number.
    pub tag: i32,
    /// The name of the field (snake_case).
    pub name: &'static str,
    /// The type of the field.
    pub field_type: SchemaType,
    /// Documentation string for the field.
    pub about: &'static str,
}

/// A field definition bound to a particular schema.
#[derive(Debug, Clone)]
pub struct BoundField {
    /// The field definition.
    pub def: Field,
    /// The index of this field in the schema.
    pub index: usize,
}

/// A schema describing the fields in a Kafka protocol message at a particular version.
#[derive(Debug, Clone)]
pub struct Schema {
    /// The fields in this schema.
    fields: Vec<Field>,
    /// Lookup map from field name to index.
    fields_by_name: HashMap<&'static str, usize>,
}

impl Schema {
    /// Create a new schema from a list of fields.
    pub fn new(fields: Vec<Field>) -> Self {
        let mut fields_by_name = HashMap::with_capacity(fields.len());
        for (i, field) in fields.iter().enumerate() {
            fields_by_name.insert(field.name, i);
        }
        Schema { fields, fields_by_name }
    }

    /// Get a bound field by its name, or `None` if not found.
    pub fn get(&self, name: &str) -> Option<BoundField> {
        self.fields_by_name
            .get(name)
            .map(|&index| BoundField { def: self.fields[index].clone(), index })
    }

    /// Get a bound field by its slot index.
    pub fn get_by_index(&self, index: usize) -> Option<BoundField> {
        self.fields.get(index).map(|f| BoundField { def: f.clone(), index })
    }

    /// The fields in this schema.
    pub fn fields(&self) -> &[Field] {
        &self.fields
    }

    /// The number of fields in this schema.
    pub fn num_fields(&self) -> usize {
        self.fields.len()
    }

    /// Walk the schema with a visitor, calling `visit` for each field type.
    pub fn walk<F: FnMut(&SchemaType)>(&self, mut visitor: F) {
        for field in &self.fields {
            visitor(&field.field_type);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_schema_field_lookup() {
        let schema = Schema::new(vec![
            Field { name: "topic", field_type: SchemaType::String, about: "The topic name" },
            Field { name: "partition", field_type: SchemaType::Int32, about: "The partition index" },
            Field {
                name: "throttle_time_ms",
                field_type: SchemaType::Int32,
                about: "Duration in milliseconds for which the request was throttled",
            },
        ]);

        assert_eq!(schema.num_fields(), 3);

        let throttle = schema.get("throttle_time_ms").unwrap();
        assert_eq!(throttle.def.name, "throttle_time_ms");
        assert_eq!(throttle.def.field_type, SchemaType::Int32);
        assert_eq!(throttle.index, 2);

        assert!(schema.get("nonexistent").is_none());
    }

    #[test]
    fn test_schema_get_by_index() {
        let schema = Schema::new(vec![
            Field { name: "key", field_type: SchemaType::Bytes, about: "The key" },
            Field { name: "value", field_type: SchemaType::NullableBytes, about: "The value" },
        ]);

        let field = schema.get_by_index(0).unwrap();
        assert_eq!(field.def.name, "key");
        assert!(schema.get_by_index(2).is_none());
    }

    #[test]
    fn test_schema_walk() {
        let schema = Schema::new(vec![
            Field { name: "a", field_type: SchemaType::Int32, about: "" },
            Field { name: "b", field_type: SchemaType::Bytes, about: "" },
            Field { name: "c", field_type: SchemaType::CompactBytes, about: "" },
        ]);

        let mut types = Vec::new();
        schema.walk(|t| types.push(*t));
        assert_eq!(types, vec![SchemaType::Int32, SchemaType::Bytes, SchemaType::CompactBytes]);
    }
}
