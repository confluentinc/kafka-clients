// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::fmt;

/// Represents the different field types supported in Kafka message schemas.
/// 
/// Translated from org.apache.kafka.message.FieldType
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum FieldType {
    Bool,
    Int8,
    Int16,
    Uint16,
    Int32,
    Uint32,
    Int64,
    Uuid,
    Float64,
    String,
    Bytes,
    Records,
    Struct(String),
    Array(Box<FieldType>),
}

impl FieldType {
    pub const ARRAY_PREFIX: &'static str = "[]";

    /// Parses a field type from a string representation.
    pub fn parse(s: &str) -> Result<Self, String> {
        let trimmed = s.trim();
        
        match trimmed {
            "bool" => Ok(FieldType::Bool),
            "int8" => Ok(FieldType::Int8),
            "int16" => Ok(FieldType::Int16),
            "uint16" => Ok(FieldType::Uint16),
            "int32" => Ok(FieldType::Int32),
            "uint32" => Ok(FieldType::Uint32),
            "int64" => Ok(FieldType::Int64),
            "uuid" => Ok(FieldType::Uuid),
            "float64" => Ok(FieldType::Float64),
            "string" => Ok(FieldType::String),
            "bytes" => Ok(FieldType::Bytes),
            "records" => Ok(FieldType::Records),
            _ => {
                if let Some(element_type_str) = trimmed.strip_prefix(Self::ARRAY_PREFIX) {
                    if element_type_str.is_empty() {
                        return Err(format!(
                            "Can't parse array type {}. No element type found.",
                            trimmed
                        ));
                    }
                    let element_type = Self::parse(element_type_str)?;
                    if element_type.is_array() {
                        return Err(
                            "Can't have an array of arrays. Use an array of structs containing an array instead.".to_string()
                        );
                    }
                    Ok(FieldType::Array(Box::new(element_type)))
                } else if trimmed.chars().next().map_or(false, |c| c.is_uppercase()) {
                    Ok(FieldType::Struct(trimmed.to_string()))
                } else {
                    Err(format!("Can't parse type {}", trimmed))
                }
            }
        }
    }

    /// Returns true if this is an array type.
    pub fn is_array(&self) -> bool {
        matches!(self, FieldType::Array(_))
    }

    /// Returns true if this is an array of structures.
    pub fn is_struct_array(&self) -> bool {
        if let FieldType::Array(element_type) = self {
            element_type.is_struct()
        } else {
            false
        }
    }

    /// Returns true if the serialization of this type is different in flexible versions.
    pub fn serialization_is_different_in_flexible_versions(&self) -> bool {
        matches!(
            self,
            FieldType::String
                | FieldType::Bytes
                | FieldType::Records
                | FieldType::Struct(_)
                | FieldType::Array(_)
        )
    }

    /// Returns true if this is a string type.
    pub fn is_string(&self) -> bool {
        matches!(self, FieldType::String)
    }

    /// Returns true if this is a bytes type.
    pub fn is_bytes(&self) -> bool {
        matches!(self, FieldType::Bytes)
    }

    /// Returns true if this is a records type.
    pub fn is_records(&self) -> bool {
        matches!(self, FieldType::Records)
    }

    /// Returns true if this is a floating point type.
    pub fn is_float(&self) -> bool {
        matches!(self, FieldType::Float64)
    }

    /// Returns true if this is a struct type.
    pub fn is_struct(&self) -> bool {
        matches!(self, FieldType::Struct(_))
    }

    /// Returns true if this field type is compatible with nullability.
    pub fn can_be_nullable(&self) -> bool {
        matches!(
            self,
            FieldType::String
                | FieldType::Bytes
                | FieldType::Records
                | FieldType::Struct(_)
                | FieldType::Array(_)
        )
    }

    /// Gets the fixed length of the field, or None if the field is variable-length.
    pub fn fixed_length(&self) -> Option<usize> {
        match self {
            FieldType::Bool => Some(1),
            FieldType::Int8 => Some(1),
            FieldType::Int16 => Some(2),
            FieldType::Uint16 => Some(2),
            FieldType::Int32 => Some(4),
            FieldType::Uint32 => Some(4),
            FieldType::Int64 => Some(8),
            FieldType::Uuid => Some(16),
            FieldType::Float64 => Some(8),
            _ => None,
        }
    }

    /// Returns true if this field type is variable length.
    pub fn is_variable_length(&self) -> bool {
        self.fixed_length().is_none()
    }

    /// Returns the element type for array types.
    pub fn element_type(&self) -> Option<&FieldType> {
        if let FieldType::Array(element_type) = self {
            Some(element_type)
        } else {
            None
        }
    }

    /// Returns the element name for array types.
    pub fn element_name(&self) -> Option<String> {
        self.element_type().map(|t| t.to_string())
    }

    /// Returns the struct type name for struct types.
    pub fn type_name(&self) -> Option<&str> {
        if let FieldType::Struct(name) = self {
            Some(name)
        } else {
            None
        }
    }
}

impl fmt::Display for FieldType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FieldType::Bool => write!(f, "bool"),
            FieldType::Int8 => write!(f, "int8"),
            FieldType::Int16 => write!(f, "int16"),
            FieldType::Uint16 => write!(f, "uint16"),
            FieldType::Int32 => write!(f, "int32"),
            FieldType::Uint32 => write!(f, "uint32"),
            FieldType::Int64 => write!(f, "int64"),
            FieldType::Uuid => write!(f, "uuid"),
            FieldType::Float64 => write!(f, "float64"),
            FieldType::String => write!(f, "string"),
            FieldType::Bytes => write!(f, "bytes"),
            FieldType::Records => write!(f, "records"),
            FieldType::Struct(name) => write!(f, "{}", name),
            FieldType::Array(element_type) => write!(f, "[]{}", element_type),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_primitives() {
        assert_eq!(FieldType::parse("bool").unwrap(), FieldType::Bool);
        assert_eq!(FieldType::parse("int8").unwrap(), FieldType::Int8);
        assert_eq!(FieldType::parse("int16").unwrap(), FieldType::Int16);
        assert_eq!(FieldType::parse("uint16").unwrap(), FieldType::Uint16);
        assert_eq!(FieldType::parse("int32").unwrap(), FieldType::Int32);
        assert_eq!(FieldType::parse("uint32").unwrap(), FieldType::Uint32);
        assert_eq!(FieldType::parse("int64").unwrap(), FieldType::Int64);
        assert_eq!(FieldType::parse("uuid").unwrap(), FieldType::Uuid);
        assert_eq!(FieldType::parse("float64").unwrap(), FieldType::Float64);
        assert_eq!(FieldType::parse("string").unwrap(), FieldType::String);
        assert_eq!(FieldType::parse("bytes").unwrap(), FieldType::Bytes);
        assert_eq!(FieldType::parse("records").unwrap(), FieldType::Records);
    }

    #[test]
    fn test_parse_struct() {
        let field_type = FieldType::parse("TopicData").unwrap();
        assert!(field_type.is_struct());
        assert_eq!(field_type.type_name(), Some("TopicData"));
    }

    #[test]
    fn test_parse_array() {
        let field_type = FieldType::parse("[]int32").unwrap();
        assert!(field_type.is_array());
        assert_eq!(
            field_type.element_type(),
            Some(&FieldType::Int32)
        );
    }

    #[test]
    fn test_parse_struct_array() {
        let field_type = FieldType::parse("[]TopicData").unwrap();
        assert!(field_type.is_struct_array());
    }

    #[test]
    fn test_parse_array_of_arrays_fails() {
        assert!(FieldType::parse("[][]int32").is_err());
    }

    #[test]
    fn test_fixed_length() {
        assert_eq!(FieldType::Bool.fixed_length(), Some(1));
        assert_eq!(FieldType::Int8.fixed_length(), Some(1));
        assert_eq!(FieldType::Int16.fixed_length(), Some(2));
        assert_eq!(FieldType::Int32.fixed_length(), Some(4));
        assert_eq!(FieldType::Int64.fixed_length(), Some(8));
        assert_eq!(FieldType::Uuid.fixed_length(), Some(16));
        assert_eq!(FieldType::Float64.fixed_length(), Some(8));
        assert_eq!(FieldType::String.fixed_length(), None);
        assert_eq!(FieldType::Bytes.fixed_length(), None);
    }

    #[test]
    fn test_can_be_nullable() {
        assert!(!FieldType::Bool.can_be_nullable());
        assert!(!FieldType::Int32.can_be_nullable());
        assert!(FieldType::String.can_be_nullable());
        assert!(FieldType::Bytes.can_be_nullable());
        assert!(FieldType::Records.can_be_nullable());
        assert!(FieldType::Struct("Test".to_string()).can_be_nullable());
        assert!(FieldType::Array(Box::new(FieldType::Int32)).can_be_nullable());
    }

    #[test]
    fn test_to_string() {
        assert_eq!(FieldType::Bool.to_string(), "bool");
        assert_eq!(FieldType::Int32.to_string(), "int32");
        assert_eq!(FieldType::String.to_string(), "string");
        assert_eq!(FieldType::Struct("TopicData".to_string()).to_string(), "TopicData");
        assert_eq!(
            FieldType::Array(Box::new(FieldType::Int32)).to_string(),
            "[]int32"
        );
    }
}
