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

//! Translation of `org.apache.kafka.common.protocol.types.ArrayOf`.

use crate::common::protocol::types::r#type::Type;

/// Represents a type for an array of a particular type. Mirrors
/// `org.apache.kafka.common.protocol.types.ArrayOf`.
#[derive(Debug, Clone, PartialEq)]
pub struct ArrayOf {
    element_type: Type,
    nullable: bool,
}

impl ArrayOf {
    /// Construct a non-nullable array of `element_type`. Mirrors
    /// `new ArrayOf(Type)`.
    pub fn new(element_type: Type) -> Self {
        ArrayOf { element_type, nullable: false }
    }

    /// Construct a nullable array. Mirrors `ArrayOf.nullable(Type)`.
    pub fn nullable(element_type: Type) -> Self {
        ArrayOf { element_type, nullable: true }
    }

    /// Whether this array accepts `null` as a value. Mirrors
    /// `Type#isNullable`.
    pub fn is_nullable(&self) -> bool {
        self.nullable
    }

    /// Element type. Mirrors `Type#arrayElementType`.
    pub fn element_type(&self) -> &Type {
        &self.element_type
    }
}
