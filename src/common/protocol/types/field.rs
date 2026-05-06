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

//! Translation of `org.apache.kafka.common.protocol.types.Field`.

use crate::common::errors::KafkaError;
use crate::common::protocol::types::r#type::Type;
use crate::common::protocol::types::value::Value;

/// A field in a [`crate::common::protocol::types::Schema`].
///
/// Mirrors `org.apache.kafka.common.protocol.types.Field`. Each field has a
/// name, a type, an optional doc string, and an optional default value.
#[derive(Debug, Clone, PartialEq)]
pub struct Field {
    /// Field name.
    pub name: String,
    /// Optional documentation string.
    pub doc_string: Option<String>,
    /// Field type. Renamed from `type` due to the Rust keyword; the Java
    /// name is preserved via the `r#type` raw identifier.
    pub r#type: Type,
    /// Whether the field has a default value.
    pub has_default_value: bool,
    /// The default value (only meaningful if `has_default_value` is true,
    /// or when `r#type` is nullable).
    pub default_value: Value,
}

impl Field {
    /// Construct a `Field` mirroring
    /// `Field(String, Type, String, boolean, Object)`.
    pub fn new(
        name: impl Into<String>,
        r#type: Type,
        doc_string: Option<String>,
        has_default_value: bool,
        default_value: Value,
    ) -> Result<Self, KafkaError> {
        if has_default_value {
            r#type.validate(&default_value)?;
        }
        Ok(Field { name: name.into(), doc_string, r#type, has_default_value, default_value })
    }

    /// Construct a `Field` mirroring `Field(String, Type, String)`.
    pub fn with_doc(name: impl Into<String>, r#type: Type, doc_string: impl Into<String>) -> Self {
        Field {
            name: name.into(),
            doc_string: Some(doc_string.into()),
            r#type,
            has_default_value: false,
            default_value: Value::Null,
        }
    }

    /// Construct a `Field` mirroring `Field(String, Type, String, Object)`.
    pub fn with_default(
        name: impl Into<String>,
        r#type: Type,
        doc_string: impl Into<String>,
        default_value: Value,
    ) -> Result<Self, KafkaError> {
        r#type.validate(&default_value)?;
        Ok(Field {
            name: name.into(),
            doc_string: Some(doc_string.into()),
            r#type,
            has_default_value: true,
            default_value,
        })
    }

    /// Construct a `Field` mirroring `Field(String, Type)`.
    pub fn no_doc(name: impl Into<String>, r#type: Type) -> Self {
        Field {
            name: name.into(),
            doc_string: None,
            r#type,
            has_default_value: false,
            default_value: Value::Null,
        }
    }
}

/// Tagged-fields section field, mirrors `Field.TaggedFieldsSection`.
///
/// The Java class is a thin subclass of `Field` with a fixed name
/// (`_tagged_fields`) and doc string (`The tagged fields`). In Rust we
/// expose a constructor pair instead of subtyping.
pub struct TaggedFieldsSection;

impl TaggedFieldsSection {
    const NAME: &'static str = "_tagged_fields";
    const DOC_STRING: &'static str = "The tagged fields";

    /// Mirrors `Field.TaggedFieldsSection.of(Object... fields)`.
    pub fn of(
        fields: Vec<(i32, crate::common::protocol::types::field::Field)>,
    ) -> crate::common::protocol::types::field::Field {
        let tagged = crate::common::protocol::types::tagged_fields::TaggedFields::from_pairs(fields);
        crate::common::protocol::types::field::Field {
            name: Self::NAME.to_string(),
            doc_string: Some(Self::DOC_STRING.to_string()),
            r#type: Type::TaggedFields(Box::new(tagged)),
            has_default_value: false,
            default_value: Value::Null,
        }
    }
}
