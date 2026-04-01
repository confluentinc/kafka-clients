/*
 * Copyright 2025 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

use crate::message::{FieldSpec, Versions};
use serde::{Deserialize, Deserializer};
use std::collections::HashSet;

/// Specification for a structure in a Kafka message schema
#[derive(Debug, Clone, PartialEq)]
pub struct StructSpec {
    name: String,
    versions: Versions,
    deprecated_versions: Versions,
    fields: Vec<FieldSpec>,
    has_keys: bool,
}

impl StructSpec {
    /// Create a new StructSpec with validation
    pub fn new(
        name: String,
        versions_str: Option<&str>,
        deprecated_versions_str: Option<&str>,
        mut fields: Vec<FieldSpec>,
    ) -> Result<Self, String> {
        let versions = match Versions::parse(versions_str, Versions::NONE)? {
            v if v.empty() => {
                return Err(format!("You must specify the version of the {} structure.", name));
            },
            v => v,
        };

        let deprecated_versions = Versions::parse(deprecated_versions_str, Versions::NONE)?;

        // Validate each field (this parses field types, versions, etc.)
        for field in &mut fields {
            field.validate()?;
        }

        // Validate fields
        let mut tags = HashSet::new();
        let mut names = HashSet::new();

        for field in &fields {
            // Check for duplicate tag IDs
            if let Some(tag) = field.tag()
                && !tags.insert(tag) {
                    return Err(format!(
                        "In {}, field {} has a duplicate tag ID {}. All tag IDs must be unique.",
                        name,
                        field.name(),
                        tag
                    ));
                }

            // Check for duplicate names
            if !names.insert(field.name()) {
                return Err(format!(
                    "In {}, field {} has a duplicate name. All field names must be unique.",
                    name,
                    field.name()
                ));
            }
        }

        // Tag IDs should be contiguous and start at 0
        for i in 0..tags.len() {
            if !tags.contains(&(i as i32)) {
                return Err(format!(
                    "In {}, the tag IDs are not contiguous. Make use of tag {} before using any higher tag IDs.",
                    name, i
                ));
            }
        }

        let has_keys = fields.iter().any(|f| f.map_key());

        Ok(StructSpec { name, versions, deprecated_versions, fields, has_keys })
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn versions(&self) -> Versions {
        self.versions
    }

    pub fn versions_string(&self) -> String {
        self.versions.to_string()
    }

    pub fn deprecated_versions(&self) -> Versions {
        self.deprecated_versions
    }

    pub fn fields(&self) -> &[FieldSpec] {
        &self.fields
    }

    pub fn has_keys(&self) -> bool {
        self.has_keys
    }
}

// Custom deserialization to handle validation during JSON parsing
impl<'de> Deserialize<'de> for StructSpec {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct StructSpecHelper {
            name: String,
            versions: Option<String>,
            #[serde(rename = "deprecatedVersions")]
            deprecated_versions: Option<String>,
            fields: Option<Vec<FieldSpec>>,
        }

        let helper = StructSpecHelper::deserialize(deserializer)?;

        StructSpec::new(
            helper.name,
            helper.versions.as_deref(),
            helper.deprecated_versions.as_deref(),
            helper.fields.unwrap_or_default(),
        )
        .map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json;

    #[test]
    fn test_struct_spec_creation() {
        let spec = StructSpec::new("TestStruct".to_string(), Some("0-5"), None, vec![]).unwrap();

        assert_eq!(spec.name(), "TestStruct");
        assert_eq!(spec.versions(), Versions::new(0, 5).unwrap());
        assert_eq!(spec.fields().len(), 0);
        assert!(!spec.has_keys());
    }

    #[test]
    fn test_struct_spec_requires_versions() {
        let result = StructSpec::new("TestStruct".to_string(), Some("none"), None, vec![]);

        assert!(result.is_err());
        assert!(result.unwrap_err().contains("must specify the version"));
    }

    #[test]
    fn test_duplicate_field_names() {
        let field1 = serde_json::from_str::<FieldSpec>(
            r#"{
            "name": "field1",
            "type": "int32",
            "versions": "0+"
        }"#,
        )
        .unwrap();

        let field2 = serde_json::from_str::<FieldSpec>(
            r#"{
            "name": "field1",
            "type": "string",
            "versions": "0+"
        }"#,
        )
        .unwrap();

        let result = StructSpec::new("TestStruct".to_string(), Some("0+"), None, vec![field1, field2]);

        assert!(result.is_err());
        assert!(result.unwrap_err().contains("duplicate name"));
    }

    #[test]
    fn test_duplicate_tag_ids() {
        let field1 = serde_json::from_str::<FieldSpec>(
            r#"{
            "name": "field1",
            "type": "int32",
            "versions": "0+",
            "tag": 0,
            "taggedVersions": "0+"
        }"#,
        )
        .unwrap();

        let field2 = serde_json::from_str::<FieldSpec>(
            r#"{
            "name": "field2",
            "type": "string",
            "versions": "0+",
            "tag": 0,
            "taggedVersions": "0+"
        }"#,
        )
        .unwrap();

        let result = StructSpec::new("TestStruct".to_string(), Some("0+"), None, vec![field1, field2]);

        assert!(result.is_err());
        assert!(result.unwrap_err().contains("duplicate tag ID"));
    }

    #[test]
    fn test_non_contiguous_tag_ids() {
        let field1 = serde_json::from_str::<FieldSpec>(
            r#"{
            "name": "field1",
            "type": "int32",
            "versions": "0+",
            "tag": 0,
            "taggedVersions": "0+"
        }"#,
        )
        .unwrap();

        let field2 = serde_json::from_str::<FieldSpec>(
            r#"{
            "name": "field2",
            "type": "string",
            "versions": "0+",
            "tag": 2,
            "taggedVersions": "0+"
        }"#,
        )
        .unwrap();

        let result = StructSpec::new("TestStruct".to_string(), Some("0+"), None, vec![field1, field2]);

        assert!(result.is_err());
        assert!(result.unwrap_err().contains("not contiguous"));
    }

    #[test]
    fn test_deserialize_from_json() {
        let json = r#"{
            "name": "TestStruct",
            "versions": "0-3",
            "fields": [
                {
                    "name": "field1",
                    "type": "int32",
                    "versions": "0+"
                }
            ]
        }"#;

        let spec: StructSpec = serde_json::from_str(json).unwrap();
        assert_eq!(spec.name(), "TestStruct");
        assert_eq!(spec.versions(), Versions::new(0, 3).unwrap());
        assert_eq!(spec.fields().len(), 1);
    }
}
