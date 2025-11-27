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

use super::entity_type::EntityType;
use super::field_type::FieldType;
use super::versions::Versions;
use regex::Regex;
use serde::{Deserialize, Serialize};

/// Specification for a field in a Kafka message schema.
///
/// Translated from org.apache.kafka.message.FieldSpec
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FieldSpec {
    name: String,

    #[serde(default = "default_versions_string")]
    versions: String,

    #[serde(default)]
    fields: Vec<FieldSpec>,

    #[serde(rename = "type")]
    field_type: String,

    #[serde(default, rename = "mapKey")]
    map_key: bool,

    #[serde(default, rename = "nullableVersions")]
    nullable_versions: Option<String>,

    #[serde(default, rename = "default")]
    field_default: Option<serde_json::Value>,

    #[serde(default)]
    ignorable: bool,

    #[serde(default, rename = "entityType")]
    entity_type: EntityType,

    #[serde(default)]
    about: String,

    #[serde(default, rename = "taggedVersions")]
    tagged_versions: Option<String>,

    #[serde(default, rename = "flexibleVersions")]
    flexible_versions: Option<String>,

    #[serde(default)]
    tag: Option<i32>,

    #[serde(default, rename = "zeroCopy")]
    zero_copy: bool,

    // Parsed/computed fields (not in JSON)
    #[serde(skip)]
    parsed_type: Option<FieldType>,

    #[serde(skip)]
    parsed_versions: Option<Versions>,

    #[serde(skip)]
    parsed_nullable_versions: Option<Versions>,

    #[serde(skip)]
    parsed_tagged_versions: Option<Versions>,
}

fn default_versions_string() -> String {
    String::new()
}

// Remove the custom deserialize functions as we'll handle parsing in validate()

impl FieldSpec {
    /// Validates and initializes the field spec after deserialization.
    pub fn validate(&mut self) -> Result<(), String> {
        // Validate field name
        let valid_names = Regex::new(r"^[A-Za-z]([A-Za-z0-9]*)$").unwrap();
        if !valid_names.is_match(&self.name) {
            return Err(format!("Invalid field name {}", self.name));
        }

        // Parse tagged versions
        self.parsed_tagged_versions = Some(Versions::parse(self.tagged_versions.as_deref(), Versions::NONE)?);

        // Parse versions with default to tagged versions if not set
        let tagged_vers = self.parsed_tagged_versions.unwrap();
        let default_versions = if tagged_vers.empty() {
            Versions::NONE
        } else {
            tagged_vers
        };

        self.parsed_versions = Some(if self.versions.is_empty() {
            default_versions
        } else {
            Versions::parse(Some(&self.versions), default_versions)?
        });

        // Parse field type
        self.parsed_type = Some(FieldType::parse(&self.field_type)?);

        // Parse nullable versions
        self.parsed_nullable_versions = Some(Versions::parse(self.nullable_versions.as_deref(), Versions::NONE)?);

        // Validate nullable versions
        let nullable_vers = self.parsed_nullable_versions.unwrap();
        if !nullable_vers.empty() {
            let field_type = self.parsed_type.as_ref().unwrap();
            if !field_type.can_be_nullable() {
                return Err(format!("Type {} cannot be nullable.", field_type));
            }
        }

        // Verify entity type matches field type
        self.entity_type
            .verify_type_matches(&self.name, self.parsed_type.as_ref().unwrap())?;

        // Validate tag invariants
        self.check_tag_invariants()?;

        // Validate zero copy flag
        if self.zero_copy {
            if !self.parsed_type.as_ref().unwrap().is_bytes() {
                return Err(format!(
                    "Invalid zeroCopy value for {}. Only fields of type bytes can use zeroCopy flag.",
                    self.name
                ));
            }
        }

        // Validate fields for arrays and structs
        if !self.fields.is_empty() {
            let field_type = self.parsed_type.as_ref().unwrap();
            if !field_type.is_array() && !field_type.is_struct() {
                return Err(format!("Non-array or Struct field {} cannot have fields", self.name));
            }

            // Validate nested fields recursively
            for field in &mut self.fields {
                field.validate()?;
            }
        }

        Ok(())
    }

    fn check_tag_invariants(&self) -> Result<(), String> {
        let tagged_vers = self.parsed_tagged_versions.unwrap();

        if let Some(tag_value) = self.tag {
            if tag_value < 0 {
                return Err(format!(
                    "Field {} specifies a tag of {}. Tags cannot be negative.",
                    self.name, tag_value
                ));
            }

            if tagged_vers.empty() {
                return Err(format!(
                    "Field {} specifies a tag of {}, but has no tagged versions.",
                    self.name, tag_value
                ));
            }

            let nullable_vers = self.parsed_nullable_versions.unwrap();
            let nullable_tagged = nullable_vers.intersect(tagged_vers);
            if !nullable_tagged.empty() && nullable_tagged != tagged_vers {
                return Err(format!(
                    "Field {} specifies nullableVersions {} and taggedVersions {}. \
                     Either all tagged versions must be nullable, or none must be.",
                    self.name, nullable_vers, tagged_vers
                ));
            }

            if tagged_vers.highest() < i16::MAX {
                return Err(format!(
                    "Field {} specifies taggedVersions {}, which is not open-ended.",
                    self.name, tagged_vers
                ));
            }

            let versions = self.parsed_versions.unwrap();
            if tagged_vers.intersect(versions) != tagged_vers {
                return Err(format!(
                    "Field {} specifies taggedVersions {}, and versions {}. \
                     taggedVersions must be a subset of versions.",
                    self.name, tagged_vers, versions
                ));
            }
        } else if !tagged_vers.empty() {
            return Err(format!(
                "Field {} does not specify a tag, but specifies tagged versions of {}.",
                self.name, tagged_vers
            ));
        }

        Ok(())
    }

    // Accessors
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn versions(&self) -> Versions {
        self.parsed_versions.unwrap_or(Versions::NONE)
    }

    pub fn fields(&self) -> &[FieldSpec] {
        &self.fields
    }

    pub fn field_type(&self) -> &FieldType {
        self.parsed_type.as_ref().expect("Field type not parsed")
    }

    pub fn map_key(&self) -> bool {
        self.map_key
    }

    pub fn nullable_versions(&self) -> Versions {
        self.parsed_nullable_versions.unwrap_or(Versions::NONE)
    }

    pub fn field_default(&self) -> Option<&serde_json::Value> {
        self.field_default.as_ref()
    }

    pub fn ignorable(&self) -> bool {
        self.ignorable
    }

    pub fn entity_type(&self) -> EntityType {
        self.entity_type
    }

    pub fn about(&self) -> &str {
        &self.about
    }

    pub fn tagged_versions(&self) -> Versions {
        self.parsed_tagged_versions.unwrap_or(Versions::NONE)
    }

    pub fn flexible_versions(&self) -> Option<Versions> {
        self.flexible_versions
            .as_ref()
            .and_then(|s| Versions::parse(Some(s), Versions::NONE).ok())
    }

    pub fn tag(&self) -> Option<i32> {
        self.tag
    }

    pub fn zero_copy(&self) -> bool {
        self.zero_copy
    }

    // Naming convention helpers
    pub fn camel_case_name(&self) -> String {
        to_camel_case(&self.name)
    }

    pub fn snake_case_name(&self) -> String {
        to_snake_case(&self.name)
    }
}

// Helper functions for naming conventions
fn to_camel_case(s: &str) -> String {
    if s.is_empty() {
        return String::new();
    }
    let mut chars = s.chars();
    let first = chars.next().unwrap().to_lowercase().to_string();
    first + chars.as_str()
}

fn to_snake_case(s: &str) -> String {
    let mut result = String::new();
    for (i, c) in s.chars().enumerate() {
        if c.is_uppercase() && i > 0 {
            result.push('_');
        }
        result.push(c.to_lowercase().next().unwrap());
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_to_camel_case() {
        assert_eq!(to_camel_case("TopicName"), "topicName");
        assert_eq!(to_camel_case("Acks"), "acks");
    }

    #[test]
    fn test_to_snake_case() {
        assert_eq!(to_snake_case("TopicName"), "topic_name");
        assert_eq!(to_snake_case("Acks"), "acks");
        assert_eq!(to_snake_case("TransactionalId"), "transactional_id");
    }
}
