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

#![allow(unused_imports)]

pub mod code_buffer;
pub mod entity_type;
pub mod field_spec;
pub mod field_type;
pub mod message_spec;
pub mod message_spec_type;
pub mod request_listener_type;
pub mod schema_generator;
pub mod struct_spec;
pub mod versions;

/// Deserializes a schema boolean that may be written either as a JSON boolean
/// (`"ignorable": true`) or as a quoted string (`"ignorable": "true"`).
///
/// Both forms occur in Kafka's own schemas — `WriteShareGroupStateRequest`,
/// `ReadShareGroupStateSummaryResponse` and `DescribeShareGroupOffsetsResponse`
/// quote `ignorable`, while the other 177 occurrences do not. Java accepts both
/// because Jackson coerces a `"true"` / `"false"` string to a boolean; `serde` is
/// strict and would reject the quoted form, so this restores Jackson's leniency.
///
/// Applied to every boolean property of a schema rather than only the ones quoted
/// today: the strict form fails by producing a stub type with no fields, which the
/// build reports as success, so the failure is silent (PLAN §9.10).
pub(crate) fn deserialize_lenient_bool<'de, D>(deserializer: D) -> Result<bool, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::Deserialize;

    #[derive(Deserialize)]
    #[serde(untagged)]
    enum BoolOrString {
        Bool(bool),
        Str(String),
    }

    match BoolOrString::deserialize(deserializer)? {
        BoolOrString::Bool(value) => Ok(value),
        BoolOrString::Str(text) => text
            .parse::<bool>()
            .map_err(|_| serde::de::Error::invalid_value(serde::de::Unexpected::Str(&text), &"\"true\" or \"false\"")),
    }
}

pub use code_buffer::CodeBuffer;
pub use entity_type::EntityType;
pub use field_spec::FieldSpec;
pub use field_type::FieldType;
pub use message_spec::MessageSpec;
pub use message_spec_type::MessageSpecType;
pub use request_listener_type::RequestListenerType;
pub use schema_generator::{SchemaGenerator, StructRegistry};
pub use struct_spec::StructSpec;
pub use versions::Versions;
