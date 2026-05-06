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

//! Translation of `org.apache.kafka.common.protocol.types.TaggedFields`.

use std::collections::BTreeMap;
use std::fmt;

use crate::common::protocol::types::field::Field;

/// Represents a tagged-fields section of a flexible-version protocol
/// message. Mirrors `org.apache.kafka.common.protocol.types.TaggedFields`.
#[derive(Debug, Clone, PartialEq)]
pub struct TaggedFields {
    fields: BTreeMap<i32, Field>,
}

impl TaggedFields {
    /// Construct from an existing tag→field map.
    pub fn new(fields: BTreeMap<i32, Field>) -> Self {
        TaggedFields { fields }
    }

    /// Construct from `(tag, field)` pairs. Mirrors
    /// `TaggedFields.of(Object... fields)` — the Java method takes a
    /// varargs of alternating `Integer` tag / `Field` value objects, which
    /// in Rust we tighten to `(i32, Field)` pairs.
    pub fn from_pairs(pairs: Vec<(i32, Field)>) -> Self {
        let mut map = BTreeMap::new();
        for (tag, field) in pairs {
            map.insert(tag, field);
        }
        TaggedFields { fields: map }
    }

    /// Number of declared tags. Mirrors `TaggedFields#numFields`.
    pub fn num_fields(&self) -> usize {
        self.fields.len()
    }

    /// Map of declared tags to fields. Mirrors `TaggedFields#fields`.
    pub fn fields(&self) -> &BTreeMap<i32, Field> {
        &self.fields
    }
}

impl fmt::Display for TaggedFields {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("TAGGED_FIELDS_TYPE_NAME(")?;
        let mut prefix = "";
        for (tag, field) in &self.fields {
            f.write_str(prefix)?;
            prefix = ", ";
            write!(f, "{tag} -> {}:{}", field.name, field.r#type)?;
        }
        f.write_str(")")
    }
}
