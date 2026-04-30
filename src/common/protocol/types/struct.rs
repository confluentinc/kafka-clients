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

//! Translation of `org.apache.kafka.common.protocol.types.Struct`.

use std::fmt;

use crate::common::errors::KafkaError;
use crate::common::protocol::types::bound_field::BoundField;
use crate::common::protocol::types::schema::Schema;
use crate::common::protocol::types::schema_exception::schema_exception;
use crate::common::protocol::types::r#type::Type;
use crate::common::protocol::types::value::Value;

/// A `'static` reference to `Value::Null`, used by [`Struct::field_or_default`]
/// so the returned reference does not borrow from a temporary.
static NULL_REF: &Value = &Value::Null;

/// A record that can be serialised and deserialised according to a
/// pre-defined [`Schema`].
#[derive(Debug, Clone)]
pub struct Struct {
    schema: Schema,
    values: Vec<Value>,
}

impl Struct {
    /// Construct a `Struct` with all fields unset. Mirrors
    /// `new Struct(Schema)`.
    pub fn new(schema: Schema) -> Self {
        let values = vec![Value::Null; schema.num_fields()];
        Struct { schema, values }
    }

    /// Construct from a fully populated value vector. Mirrors the
    /// package-private `Struct(Schema, Object[])` used by `Schema#read`.
    pub fn with_values(schema: Schema, values: Vec<Value>) -> Self {
        Struct { schema, values }
    }

    /// The schema for this struct. Mirrors `Struct#schema`.
    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    /// Get the value for `field`, falling back to the field's default
    /// value or `null` for nullable types. Mirrors `Struct#getFieldOrDefault`.
    ///
    /// The returned reference's lifetime is bound to whichever of `self`
    /// and `field` outlives the other — the value either lives in
    /// `self.values` or in `field.def.default_value`.
    pub(crate) fn field_or_default<'a>(&'a self, field: &'a BoundField) -> Result<&'a Value, KafkaError> {
        let value = &self.values[field.index()];
        if !matches!(value, Value::Null) {
            Ok(value)
        } else if field.def.has_default_value {
            Ok(&field.def.default_value)
        } else if field.def.r#type.is_nullable() {
            // Borrow a static `Value::Null` so the returned reference can
            // outlive `field`.
            Ok(NULL_REF)
        } else {
            Err(schema_exception(format!(
                "Missing value for field '{}' which has no default value.",
                field.def.name
            )))
        }
    }

    fn validate_field(&self, field: &BoundField) -> Result<(), KafkaError> {
        if self.schema.id() != field.schema_id {
            return Err(schema_exception(format!(
                "Attempt to access field '{}' from a different schema instance.",
                field.def.name
            )));
        }
        if field.index() > self.values.len() {
            return Err(schema_exception(format!("Invalid field index: {}", field.index())));
        }
        Ok(())
    }

    /// Get a value by [`BoundField`]. Mirrors `Struct#get(BoundField)`.
    pub fn get<'a>(&'a self, field: &'a BoundField) -> Result<&'a Value, KafkaError> {
        self.validate_field(field)?;
        self.field_or_default(field)
    }

    /// Get a value by name. Mirrors `Struct#get(String)`.
    pub fn get_by_name(&self, name: &str) -> Result<&Value, KafkaError> {
        let field = self
            .schema
            .get_by_name(name)
            .ok_or_else(|| schema_exception(format!("No such field: {name}")))?;
        self.field_or_default(field)
    }

    /// Whether the schema declares a field with the given name. Mirrors
    /// `Struct#hasField`.
    pub fn has_field(&self, name: &str) -> bool {
        self.schema.get_by_name(name).is_some()
    }

    /// Set a value by [`BoundField`]. Mirrors `Struct#set(BoundField, Object)`.
    pub fn set(&mut self, field: &BoundField, value: Value) -> Result<&mut Self, KafkaError> {
        self.validate_field(field)?;
        self.values[field.index()] = value;
        Ok(self)
    }

    /// Set a value by name. Mirrors `Struct#set(String, Object)`. Returns
    /// `&mut Self` to support builder-style chaining.
    pub fn set_by_name(&mut self, name: &str, value: Value) -> Result<&mut Self, KafkaError> {
        let idx = match self.schema.get_by_name(name) {
            Some(f) => f.index(),
            None => {
                return Err(schema_exception(format!("Unknown field: {name}")));
            },
        };
        self.values[idx] = value;
        Ok(self)
    }

    /// Construct a `Struct` for the schema of a container field (struct or
    /// array of struct). Mirrors `Struct#instance(BoundField)`.
    pub fn instance(&self, field: &BoundField) -> Result<Struct, KafkaError> {
        self.validate_field(field)?;
        match &field.def.r#type {
            Type::Schema(s) => Ok(Struct::new(*s.clone())),
            Type::Array(a) => match a.element_type() {
                Type::Schema(s) => Ok(Struct::new(*s.clone())),
                _ => Err(schema_exception(format!(
                    "Field '{}' is not a container type, it is of type {}",
                    field.def.name, field.def.r#type
                ))),
            },
            Type::CompactArray(a) => match a.element_type() {
                Type::Schema(s) => Ok(Struct::new(*s.clone())),
                _ => Err(schema_exception(format!(
                    "Field '{}' is not a container type, it is of type {}",
                    field.def.name, field.def.r#type
                ))),
            },
            _ => Err(schema_exception(format!(
                "Field '{}' is not a container type, it is of type {}",
                field.def.name, field.def.r#type
            ))),
        }
    }

    /// Convenience wrapper around [`Struct::instance`] that takes a name.
    /// Mirrors `Struct#instance(String)`.
    pub fn instance_by_name(&self, name: &str) -> Result<Struct, KafkaError> {
        let field = self
            .schema
            .get_by_name(name)
            .ok_or_else(|| schema_exception(format!("No such field: {name}")))?
            .clone();
        self.instance(&field)
    }

    /// Reset every field to `Value::Null`. Mirrors `Struct#clear`.
    pub fn clear(&mut self) {
        for v in &mut self.values {
            *v = Value::Null;
        }
    }

    /// Number of bytes [`Struct::write_to`] will emit. Mirrors `Struct#sizeOf`.
    pub fn size_of(&self) -> Result<usize, KafkaError> {
        self.schema.size_of_struct(self)
    }

    /// Encode the struct into `buffer`. Mirrors `Struct#writeTo`.
    pub fn write_to(&self, buffer: &mut Vec<u8>) -> Result<(), KafkaError> {
        self.schema.write_struct(buffer, self)
    }

    /// Validate the contents of this struct against its schema. Mirrors
    /// `Struct#validate`.
    pub fn validate(&self) -> Result<(), KafkaError> {
        self.schema.validate_struct(self)
    }
}

impl PartialEq for Struct {
    fn eq(&self, other: &Self) -> bool {
        if self.schema != other.schema {
            return false;
        }
        // Same schema => same field count and order.
        for (i, field) in self.schema.fields().iter().enumerate() {
            // `field_or_default` cannot fail when both structs share the
            // same schema and the field is either populated or has a
            // default; if it does fail (missing required field), fall back
            // to direct value comparison for parity with Java's behaviour.
            let lhs = self.field_or_default(field).unwrap_or(&self.values[i]);
            let rhs = other.field_or_default(field).unwrap_or(&other.values[i]);
            if lhs != rhs {
                return false;
            }
        }
        true
    }
}

impl fmt::Display for Struct {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("{")?;
        for (i, value) in self.values.iter().enumerate() {
            let bound = self.schema.get(i);
            if i > 0 {
                f.write_str(",")?;
            }
            write!(f, "{}=", bound.def.name)?;
            if bound.def.r#type.is_array() {
                if let Value::Array(items) = value {
                    f.write_str("[")?;
                    for (j, item) in items.iter().enumerate() {
                        if j > 0 {
                            f.write_str(",")?;
                        }
                        format_value(item, f)?;
                    }
                    f.write_str("]")?;
                } else {
                    format_value(value, f)?;
                }
            } else {
                format_value(value, f)?;
            }
        }
        f.write_str("}")
    }
}

fn format_value(v: &Value, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    match v {
        Value::Null => f.write_str("null"),
        Value::Bool(b) => write!(f, "{b}"),
        Value::Int8(n) => write!(f, "{n}"),
        Value::Int16(n) => write!(f, "{n}"),
        Value::Int32(n) => write!(f, "{n}"),
        Value::UInt16(n) => write!(f, "{n}"),
        Value::Int64(n) => write!(f, "{n}"),
        Value::UInt32(n) => write!(f, "{n}"),
        Value::Float64(n) => write!(f, "{n}"),
        Value::String(s) => f.write_str(s),
        Value::Bytes(_) => f.write_str("ByteBuffer"),
        Value::Array(items) => {
            f.write_str("[")?;
            for (j, item) in items.iter().enumerate() {
                if j > 0 {
                    f.write_str(",")?;
                }
                format_value(item, f)?;
            }
            f.write_str("]")
        },
        Value::Struct(s) => write!(f, "{s}"),
        Value::Uuid(u) => write!(f, "{u}"),
        Value::TaggedFields(_) => f.write_str("TaggedFields"),
        Value::RawTagged(_) => f.write_str("RawTaggedField"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::protocol::types::array_of::ArrayOf;
    use crate::common::protocol::types::field::Field;

    fn flat_struct_schema() -> Schema {
        Schema::new(vec![
            Field::with_doc("int8", Type::Int8, ""),
            Field::with_doc("int16", Type::Int16, ""),
            Field::with_doc("int32", Type::Int32, ""),
            Field::with_doc("int64", Type::Int64, ""),
            Field::with_doc("boolean", Type::Boolean, ""),
            Field::with_doc("float64", Type::Float64, ""),
            Field::with_doc("string", Type::String, ""),
        ])
        .unwrap()
    }

    fn array_schema() -> Schema {
        Schema::new(vec![Field::with_doc(
            "array",
            Type::Array(Box::new(ArrayOf::new(Type::Array(Box::new(ArrayOf::new(Type::Int8)))))),
            "",
        )])
        .unwrap()
    }

    fn nested_child_schema() -> Schema {
        Schema::new(vec![Field::with_doc("int8", Type::Int8, "")]).unwrap()
    }

    fn nested_schema(array: Schema, child: Schema) -> Schema {
        Schema::new(vec![
            Field::with_doc("array", Type::Array(Box::new(ArrayOf::new(Type::Schema(Box::new(array))))), ""),
            Field::with_doc("nested", Type::Schema(Box::new(child)), ""),
        ])
        .unwrap()
    }

    fn populate_flat(schema: &Schema, string_value: &str) -> Struct {
        let mut s = Struct::new(schema.clone());
        s.set_by_name("int8", Value::Int8(12)).unwrap();
        s.set_by_name("int16", Value::Int16(12)).unwrap();
        s.set_by_name("int32", Value::Int32(12)).unwrap();
        s.set_by_name("int64", Value::Int64(12)).unwrap();
        s.set_by_name("boolean", Value::Bool(true)).unwrap();
        s.set_by_name("float64", Value::Float64(0.5)).unwrap();
        s.set_by_name("string", Value::String(string_value.to_string())).unwrap();
        s
    }

    /// Mirrors `StructTest.testEquals`.
    #[test]
    fn equals() {
        let schema = flat_struct_schema();
        let struct1 = populate_flat(&schema, "foobar");
        let struct2 = populate_flat(&schema, "foobar");
        let struct3 = populate_flat(&schema, "mismatching string");

        assert_eq!(struct1, struct2);
        assert_ne!(struct1, struct3);

        // Nested case.
        let array_inner_schema = array_schema();
        let child_schema = nested_child_schema();
        let nested = nested_schema(array_inner_schema.clone(), child_schema.clone());
        let array = vec![Value::Int8(1), Value::Int8(2)];
        let mut s1 = Struct::new(nested.clone());
        s1.set_by_name("array", Value::Array(array.clone())).unwrap();
        let mut child1 = Struct::new(child_schema.clone());
        child1.set_by_name("int8", Value::Int8(12)).unwrap();
        s1.set_by_name("nested", Value::Struct(Box::new(child1))).unwrap();

        let mut s2 = Struct::new(nested.clone());
        s2.set_by_name("array", Value::Array(array)).unwrap();
        let mut child2 = Struct::new(child_schema.clone());
        child2.set_by_name("int8", Value::Int8(12)).unwrap();
        s2.set_by_name("nested", Value::Struct(Box::new(child2))).unwrap();

        let mut s3 = Struct::new(nested.clone());
        let array3 = vec![Value::Int8(1), Value::Int8(2), Value::Int8(3)];
        s3.set_by_name("array", Value::Array(array3)).unwrap();
        let mut child3 = Struct::new(child_schema);
        child3.set_by_name("int8", Value::Int8(13)).unwrap();
        s3.set_by_name("nested", Value::Struct(Box::new(child3))).unwrap();

        assert_eq!(s1, s2);
        assert_ne!(s1, s3);
    }
}
