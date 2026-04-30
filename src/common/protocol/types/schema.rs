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

//! Translation of `org.apache.kafka.common.protocol.types.Schema`.

use std::collections::HashMap;
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::common::errors::KafkaError;
use crate::common::protocol::types::bound_field::BoundField;
use crate::common::protocol::types::field::Field;
use crate::common::protocol::types::schema_exception::schema_exception;
use crate::common::protocol::types::r#struct::Struct;
use crate::common::protocol::types::r#type::{ReadBuffer, Type};
use crate::common::protocol::types::value::Value;

/// Monotonically increasing counter to identify each [`Schema`] uniquely;
/// mirrors Java's reference equality between a `Schema` and a
/// `BoundField.schema`.
static SCHEMA_ID_COUNTER: AtomicU64 = AtomicU64::new(0);

/// The schema for a compound record definition.
#[derive(Debug, Clone)]
pub struct Schema {
    id: u64,
    fields: Vec<BoundField>,
    fields_by_name: HashMap<String, usize>,
    tolerate_missing_fields_with_defaults: bool,
}

impl Schema {
    /// Construct a schema from a list of fields. Mirrors `new Schema(Field...)`.
    pub fn new(fields: Vec<Field>) -> Result<Self, KafkaError> {
        Self::with_tolerance(false, fields)
    }

    /// Construct a schema and (optionally) tolerate optional defaulted
    /// fields missing at the tail of the buffer. Mirrors
    /// `new Schema(boolean, Field...)`.
    pub fn with_tolerance(tolerate_missing_fields_with_defaults: bool, fields: Vec<Field>) -> Result<Self, KafkaError> {
        let id = SCHEMA_ID_COUNTER.fetch_add(1, Ordering::Relaxed);
        let mut bound_fields = Vec::with_capacity(fields.len());
        let mut fields_by_name: HashMap<String, usize> = HashMap::with_capacity(fields.len());
        for (i, def) in fields.into_iter().enumerate() {
            if fields_by_name.contains_key(&def.name) {
                return Err(schema_exception(format!("Schema contains a duplicate field: {}", def.name)));
            }
            fields_by_name.insert(def.name.clone(), i);
            bound_fields.push(BoundField::new(def, id, i));
        }
        Ok(Schema { id, fields: bound_fields, fields_by_name, tolerate_missing_fields_with_defaults })
    }

    /// Identifier used for cross-schema field detection.
    pub(crate) fn id(&self) -> u64 {
        self.id
    }

    /// Number of fields in this schema. Mirrors `Schema#numFields`.
    pub fn num_fields(&self) -> usize {
        self.fields.len()
    }

    /// Get a field by slot. Mirrors `Schema#get(int)`.
    pub fn get(&self, slot: usize) -> &BoundField {
        &self.fields[slot]
    }

    /// Get a field by name. Mirrors `Schema#get(String)`.
    pub fn get_by_name(&self, name: &str) -> Option<&BoundField> {
        self.fields_by_name.get(name).map(|i| &self.fields[*i])
    }

    /// All fields in declaration order. Mirrors `Schema#fields`.
    pub fn fields(&self) -> &[BoundField] {
        &self.fields
    }

    /// Whether this schema tolerates optional defaulted fields missing at
    /// the tail of the buffer. Mirrors the Java field
    /// `tolerateMissingFieldsWithDefaults`.
    pub fn tolerate_missing(&self) -> bool {
        self.tolerate_missing_fields_with_defaults
    }

    /// Walk the schema tree; mirrors `Schema#walk`. Equivalent to a
    /// pre-order traversal over types: visit the schema, then recurse into
    /// each field, recursing into array element types.
    pub fn walk(&self, visitor: &mut dyn SchemaVisitor) {
        Self::handle_node_schema(self, visitor);
    }

    fn handle_node_schema(schema: &Schema, visitor: &mut dyn SchemaVisitor) {
        visitor.visit_schema(schema);
        for f in schema.fields() {
            Self::handle_node_type(&f.def.r#type, visitor);
        }
    }

    fn handle_node_type(node: &Type, visitor: &mut dyn SchemaVisitor) {
        match node {
            Type::Schema(s) => Self::handle_node_schema(s, visitor),
            Type::Array(a) => {
                visitor.visit_type(node);
                Self::handle_node_type(a.element_type(), visitor);
            },
            Type::CompactArray(a) => {
                visitor.visit_type(node);
                Self::handle_node_type(a.element_type(), visitor);
            },
            _ => visitor.visit_type(node),
        }
    }

    // ---------- (de)serialisation helpers used by Type::Schema ----------

    /// Encode a [`Struct`] using this schema. Mirrors `Schema#write`.
    pub fn write_struct(&self, buffer: &mut Vec<u8>, st: &Struct) -> Result<(), KafkaError> {
        for field in &self.fields {
            let value = st.field_or_default(field)?;
            field
                .def
                .r#type
                .validate(value)
                .map_err(|e| wrap("writing", &field.def.name, &e))?;
            field
                .def
                .r#type
                .write(buffer, value)
                .map_err(|e| wrap("writing", &field.def.name, &e))?;
        }
        Ok(())
    }

    /// Decode a [`Struct`] using this schema. Mirrors `Schema#read`.
    pub fn read_struct(&self, buffer: &mut ReadBuffer<'_>) -> Result<Struct, KafkaError> {
        let mut values = vec![Value::Null; self.fields.len()];
        for (i, field) in self.fields.iter().enumerate() {
            if self.tolerate_missing_fields_with_defaults {
                if buffer.has_remaining() {
                    values[i] = field
                        .def
                        .r#type
                        .read(buffer)
                        .map_err(|e| wrap("reading", &field.def.name, &e))?;
                } else if field.def.has_default_value {
                    values[i] = field.def.default_value.clone();
                } else {
                    return Err(schema_exception(format!(
                        "Error reading field '{}': Missing value for field '{}' which has no default value.",
                        field.def.name, field.def.name
                    )));
                }
            } else {
                values[i] = field
                    .def
                    .r#type
                    .read(buffer)
                    .map_err(|e| wrap("reading", &field.def.name, &e))?;
            }
        }
        Ok(Struct::with_values(self.clone(), values))
    }

    /// Compute the byte size of a [`Struct`] using this schema. Mirrors
    /// `Schema#sizeOf`.
    pub fn size_of_struct(&self, st: &Struct) -> Result<usize, KafkaError> {
        let mut size = 0usize;
        for field in &self.fields {
            let value = st.field_or_default(field)?;
            size += field
                .def
                .r#type
                .size_of(value)
                .map_err(|e| wrap("computing size for", &field.def.name, &e))?;
        }
        Ok(size)
    }

    /// Validate a [`Struct`]. Mirrors `Schema#validate`.
    pub fn validate_struct(&self, st: &Struct) -> Result<(), KafkaError> {
        for field in &self.fields {
            let value = st.field_or_default(field)?;
            if let Err(e) = field.def.r#type.validate(value) {
                return Err(schema_exception(format!(
                    "Invalid value for field '{}': {}",
                    field.def.name,
                    e.message()
                )));
            }
        }
        Ok(())
    }
}

fn wrap(action: &str, field_name: &str, e: &KafkaError) -> KafkaError {
    schema_exception(format!(
        "Error {action} field '{field_name}': {}",
        if e.message().is_empty() {
            e.java_class_name()
        } else {
            e.message()
        }
    ))
}

impl PartialEq for Schema {
    fn eq(&self, other: &Self) -> bool {
        // Match Java reference equality: two `Schema`s are equal iff they
        // have the same id (i.e. they are the same instance, allocated
        // monotonically by `Schema::new`).
        self.id == other.id
    }
}

impl fmt::Display for Schema {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("{")?;
        for (i, field) in self.fields.iter().enumerate() {
            if i > 0 {
                f.write_str(",")?;
            }
            field.fmt(f)?;
        }
        f.write_str("}")
    }
}

/// Visitor for [`Schema::walk`]. Mirrors `Schema.Visitor`.
pub trait SchemaVisitor {
    /// Visit a schema; mirrors `Schema.Visitor#visit(Schema)`.
    fn visit_schema(&mut self, _schema: &Schema) {}
    /// Visit a type; mirrors `Schema.Visitor#visit(Type)`.
    fn visit_type(&mut self, _node: &Type) {}
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Java throws a `SchemaException` for duplicate field names. The Rust
    /// translation surfaces it as a [`KafkaError::Generic`].
    #[test]
    fn duplicate_field_rejected() {
        let err = Schema::new(vec![Field::no_doc("a", Type::Int32), Field::no_doc("a", Type::String)]).unwrap_err();
        assert!(err.message().contains("duplicate field"));
    }

    /// `BoundField` carries the schema id; the same `BoundField` from
    /// schema X must not be usable on schema Y. Mirrors Java's reference
    /// equality check `this.schema != field.schema`.
    #[test]
    fn cross_schema_field_rejected() {
        let s1 = Schema::new(vec![Field::no_doc("a", Type::Int32)]).unwrap();
        let s2 = Schema::new(vec![Field::no_doc("a", Type::Int32)]).unwrap();
        let f1 = s1.get_by_name("a").unwrap().clone();
        let st = crate::common::protocol::types::r#struct::Struct::new(s2);
        let err = st.get(&f1).unwrap_err();
        assert!(err.message().contains("different schema instance"));
    }
}
