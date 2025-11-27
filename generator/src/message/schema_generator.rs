/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

use crate::message::{CodeBuffer, FieldSpec, FieldType, MessageSpec, StructSpec, Versions};
use std::collections::{BTreeMap, HashMap, HashSet};

/// Generates Schemas for Kafka MessageData classes
pub struct SchemaGenerator {
    /// Maps message names to message information
    messages: HashMap<String, MessageInfo>,

    /// The versions that implement a KIP-482 flexible schema
    message_flexible_versions: Versions,

    /// Registry of common structs
    struct_registry: StructRegistry,
}

/// Schema information for a particular message
struct MessageInfo {
    /// The versions of this message that we want to generate a schema for
    versions: Versions,

    /// Maps versions to schema declaration code
    /// If the schema for a particular version is the same as that of a previous version,
    /// there will be no entry in the map for it
    schema_for_version: BTreeMap<i16, CodeBuffer>,
}

impl MessageInfo {
    fn new(versions: Versions) -> Self {
        MessageInfo { versions, schema_for_version: BTreeMap::new() }
    }
}

/// Registry with the structures we're generating
pub struct StructRegistry {
    structs: HashMap<String, StructInfo>,
    common_struct_names: HashSet<String>,
}

struct StructInfo {
    spec: StructSpec,
    #[allow(dead_code)]
    parent_versions: Versions,
}

impl StructRegistry {
    pub fn new() -> Self {
        StructRegistry { structs: HashMap::new(), common_struct_names: HashSet::new() }
    }

    /// Register all the structures contained in a message spec
    pub fn register(&mut self, message: &MessageSpec) -> Result<(), String> {
        // Register common structures
        for struct_spec in message.common_structs() {
            let name = struct_spec.name();
            if !Self::first_is_capitalized(name) {
                return Err(format!(
                    "Can't process structure {}: the first letter of structure names must be capitalized.",
                    name
                ));
            }
            if self.structs.contains_key(name) {
                return Err(format!("Common struct {} was specified twice.", name));
            }
            self.structs.insert(
                name.to_string(),
                StructInfo { spec: struct_spec.clone(), parent_versions: struct_spec.versions() },
            );
            self.common_struct_names.insert(name.to_string());
        }

        // Register inline structures
        self.add_struct_specs(message.valid_versions(), message.fields())?;
        Ok(())
    }

    fn add_struct_specs(&mut self, parent_versions: Versions, fields: &[FieldSpec]) -> Result<(), String> {
        for field in fields {
            let type_name = match field.field_type() {
                FieldType::Array(element_type) if element_type.is_struct() => Some(element_type.to_string()),
                field_type if field_type.is_struct() => Some(field_type.to_string()),
                _ => None,
            };

            if let Some(type_name) = type_name {
                if self.common_struct_names.contains(&type_name) {
                    // If we're using a common structure, we can't specify its fields
                    if !field.fields().is_empty() {
                        return Err(format!("Can't re-specify the common struct {} as an inline struct.", type_name));
                    }
                } else if self.structs.contains_key(&type_name) {
                    return Err(format!("Struct {} was specified twice.", type_name));
                } else {
                    // Synthesize a StructSpec from the fields
                    let versions_str = field.versions().to_string();
                    let spec = StructSpec::new(type_name.clone(), Some(&versions_str), None, field.fields().to_vec())?;
                    self.structs.insert(type_name, StructInfo { spec, parent_versions });
                }

                self.add_struct_specs(parent_versions.intersect(field.versions()), field.fields())?;
            }
        }
        Ok(())
    }

    pub fn find_struct(&self, field: &FieldSpec) -> Result<&StructSpec, String> {
        let struct_field_name = match field.field_type() {
            FieldType::Array(element_type) => element_type.to_string(),
            field_type if field_type.is_struct() => field_type.to_string(),
            _ => {
                return Err(format!("Field {} cannot be treated as a structure.", field.name()));
            },
        };

        self.find_struct_by_name(&struct_field_name)
    }

    pub fn find_struct_by_name(&self, name: &str) -> Result<&StructSpec, String> {
        self.structs
            .get(name)
            .map(|info| &info.spec)
            .ok_or_else(|| format!("Unable to locate a specification for the structure {}", name))
    }

    pub fn common_structs(&self) -> impl Iterator<Item = &StructSpec> {
        self.common_struct_names
            .iter()
            .filter_map(|name| self.structs.get(name).map(|info| &info.spec))
    }

    fn first_is_capitalized(s: &str) -> bool {
        s.chars().next().map_or(false, |c| c.is_uppercase())
    }
}

impl Default for StructRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl SchemaGenerator {
    pub fn new() -> Self {
        SchemaGenerator {
            messages: HashMap::new(),
            message_flexible_versions: Versions::NONE,
            struct_registry: StructRegistry::new(),
        }
    }

    /// Generate schemas for a message
    pub fn generate_schemas(&mut self, message: &MessageSpec) -> Result<(), String> {
        self.message_flexible_versions = message.flexible_versions();

        // Register all structures
        self.struct_registry.register(message)?;

        // Collect common structs to avoid borrowing issues
        let common_structs: Vec<(String, StructSpec, Versions)> = self
            .struct_registry
            .common_structs()
            .map(|s| (s.name().to_string(), s.clone(), message.struct_spec().versions()))
            .collect();

        // First generate schemas for common structures
        for (name, struct_spec, versions) in common_structs {
            self.generate_schemas_for_struct(&name, &struct_spec, versions)?;
        }

        // Generate schemas for inline structures
        self.generate_schemas_for_struct(
            &message.data_class_name(),
            message.struct_spec(),
            message.struct_spec().versions(),
        )?;

        Ok(())
    }

    fn generate_schemas_for_struct(
        &mut self,
        class_name: &str,
        struct_spec: &StructSpec,
        parent_versions: Versions,
    ) -> Result<(), String> {
        let versions = parent_versions.intersect(struct_spec.versions());

        // Skip if already processed
        if self.messages.contains_key(class_name) {
            return Ok(());
        }

        let mut message_info = MessageInfo::new(versions);

        // Process leaf classes (nested structs) first
        for field in struct_spec.fields() {
            if field.field_type().is_struct_array() {
                let element_type = match field.field_type() {
                    FieldType::Array(et) => et.to_string(),
                    _ => unreachable!(),
                };
                let nested_struct = self.struct_registry.find_struct(field)?.clone();
                self.generate_schemas_for_struct(&element_type, &nested_struct, versions)?;
            } else if field.field_type().is_struct() {
                let type_name = field.field_type().to_string();
                let nested_struct = self.struct_registry.find_struct(field)?.clone();
                self.generate_schemas_for_struct(&type_name, &nested_struct, versions)?;
            }
        }

        // Generate schemas for each version
        let mut prev: Option<CodeBuffer> = None;
        for v in versions.lowest()..=versions.highest() {
            let mut cur = CodeBuffer::new();
            self.generate_schema_for_version(struct_spec, v, &mut cur)?;

            // Only create a new entry if the schema changed
            if prev.as_ref() != Some(&cur) {
                message_info.schema_for_version.insert(v, cur.clone());
            }
            prev = Some(cur);
        }

        self.messages.insert(class_name.to_string(), message_info);
        Ok(())
    }

    fn generate_schema_for_version(
        &self,
        struct_spec: &StructSpec,
        version: i16,
        buffer: &mut CodeBuffer,
    ) -> Result<(), String> {
        // Find the last valid field index
        let last_valid_index = self.find_last_valid_field_index(struct_spec, version, false);

        let mut final_line = last_valid_index.map_or(0, |idx| idx);
        if self.message_flexible_versions.contains(version) {
            final_line += 1;
        }

        buffer.printf("Schema {");
        buffer.increment_indent();
        buffer.printf("fields: vec![");
        buffer.increment_indent();

        // Generate field entries
        if let Some(last_idx) = last_valid_index {
            for (i, field) in struct_spec.fields().iter().enumerate() {
                if i > last_idx {
                    break;
                }

                if !field.versions().contains(version) || field.tagged_versions().contains(version) {
                    continue;
                }

                let field_type_str = self.field_type_to_schema_type(
                    field.field_type(),
                    field.nullable_versions().contains(version),
                    version,
                )?;

                let comma = if i == final_line { "" } else { "," };
                buffer.printf(format!(
                    "Field {{ name: \"{}\", field_type: {}, about: \"{}\" }}{}",
                    field.snake_case_name(),
                    field_type_str,
                    field.about(),
                    comma
                ));
            }
        }

        // Add tagged fields if flexible
        if self.message_flexible_versions.contains(version) {
            self.generate_tagged_fields_schema(struct_spec, version, buffer)?;
        }

        buffer.decrement_indent();
        buffer.printf("]");
        buffer.decrement_indent();
        buffer.printf("}");

        Ok(())
    }

    fn find_last_valid_field_index(&self, struct_spec: &StructSpec, version: i16, tagged_only: bool) -> Option<usize> {
        struct_spec
            .fields()
            .iter()
            .enumerate()
            .rev()
            .find(|(_, field)| {
                let version_ok = field.versions().contains(version);
                let tagged_ok = if tagged_only {
                    field.tagged_versions().contains(version)
                } else {
                    !field.tagged_versions().contains(version)
                };
                version_ok && tagged_ok
            })
            .map(|(idx, _)| idx)
    }

    fn generate_tagged_fields_schema(
        &self,
        struct_spec: &StructSpec,
        version: i16,
        buffer: &mut CodeBuffer,
    ) -> Result<(), String> {
        let last_valid_index = self.find_last_valid_field_index(struct_spec, version, true);

        if let Some(last_idx) = last_valid_index {
            buffer.printf("// Tagged fields:");
            for (i, field) in struct_spec.fields().iter().enumerate() {
                if i > last_idx {
                    break;
                }

                if !field.versions().contains(version) || !field.tagged_versions().contains(version) {
                    continue;
                }

                let field_type_str = self.field_type_to_schema_type(
                    field.field_type(),
                    field.nullable_versions().contains(version),
                    version,
                )?;

                let comma = if i == last_idx { "" } else { "," };
                buffer.printf(format!(
                    "TaggedField {{ tag: {}, name: \"{}\", field_type: {}, about: \"{}\" }}{}",
                    field.tag().unwrap_or(-1),
                    field.snake_case_name(),
                    field_type_str,
                    field.about(),
                    comma
                ));
            }
        }

        Ok(())
    }

    fn field_type_to_schema_type(
        &self,
        field_type: &FieldType,
        nullable: bool,
        version: i16,
    ) -> Result<String, String> {
        let flexible = self.message_flexible_versions.contains(version);

        match field_type {
            FieldType::Bool => {
                if nullable {
                    return Err("Type Bool cannot be nullable.".to_string());
                }
                Ok("SchemaType::Boolean".to_string())
            },
            FieldType::Int8 => {
                if nullable {
                    return Err("Type Int8 cannot be nullable.".to_string());
                }
                Ok("SchemaType::Int8".to_string())
            },
            FieldType::Int16 => {
                if nullable {
                    return Err("Type Int16 cannot be nullable.".to_string());
                }
                Ok("SchemaType::Int16".to_string())
            },
            FieldType::Uint16 => {
                if nullable {
                    return Err("Type Uint16 cannot be nullable.".to_string());
                }
                Ok("SchemaType::Uint16".to_string())
            },
            FieldType::Uint32 => {
                if nullable {
                    return Err("Type Uint32 cannot be nullable.".to_string());
                }
                Ok("SchemaType::Uint32".to_string())
            },
            FieldType::Int32 => {
                if nullable {
                    return Err("Type Int32 cannot be nullable.".to_string());
                }
                Ok("SchemaType::Int32".to_string())
            },
            FieldType::Int64 => {
                if nullable {
                    return Err("Type Int64 cannot be nullable.".to_string());
                }
                Ok("SchemaType::Int64".to_string())
            },
            FieldType::Uuid => {
                if nullable {
                    return Err("Type Uuid cannot be nullable.".to_string());
                }
                Ok("SchemaType::Uuid".to_string())
            },
            FieldType::Float64 => {
                if nullable {
                    return Err("Type Float64 cannot be nullable.".to_string());
                }
                Ok("SchemaType::Float64".to_string())
            },
            FieldType::String => {
                if flexible {
                    Ok(if nullable {
                        "SchemaType::CompactNullableString"
                    } else {
                        "SchemaType::CompactString"
                    }
                    .to_string())
                } else {
                    Ok(if nullable {
                        "SchemaType::NullableString"
                    } else {
                        "SchemaType::String"
                    }
                    .to_string())
                }
            },
            FieldType::Bytes => {
                if flexible {
                    Ok(if nullable {
                        "SchemaType::CompactNullableBytes"
                    } else {
                        "SchemaType::CompactBytes"
                    }
                    .to_string())
                } else {
                    Ok(if nullable {
                        "SchemaType::NullableBytes"
                    } else {
                        "SchemaType::Bytes"
                    }
                    .to_string())
                }
            },
            FieldType::Records => {
                if flexible {
                    Ok("SchemaType::CompactRecords".to_string())
                } else {
                    Ok("SchemaType::Records".to_string())
                }
            },
            FieldType::Array(element_type) => {
                let element_schema = self.field_type_to_schema_type(element_type, false, version)?;
                if flexible {
                    let prefix = if nullable {
                        "CompactArrayOf::nullable"
                    } else {
                        "CompactArrayOf::new"
                    };
                    Ok(format!("{}({})", prefix, element_schema))
                } else {
                    let prefix = if nullable { "ArrayOf::nullable" } else { "ArrayOf::new" };
                    Ok(format!("{}({})", prefix, element_schema))
                }
            },
            FieldType::Struct(struct_name) => {
                let floor_version = self.floor_version(struct_name, version)?;
                Ok(format!("{}::SCHEMA_{}", struct_name, floor_version))
            },
        }
    }

    /// Find the lowest schema version for a given class that is the same as the given version
    fn floor_version(&self, class_name: &str, version: i16) -> Result<i16, String> {
        let message = self
            .messages
            .get(class_name)
            .ok_or_else(|| format!("Unable to find message info for class {}", class_name))?;

        message
            .schema_for_version
            .range(..=version)
            .next_back()
            .map(|(v, _)| *v)
            .ok_or_else(|| format!("No schema version found for class {} at version {}", class_name, version))
    }

    /// Write the message schema to the provided buffer
    pub fn write_schema(&self, class_name: &str, buffer: &mut CodeBuffer) -> Result<(), String> {
        let message_info = self
            .messages
            .get(class_name)
            .ok_or_else(|| format!("Unable to find message info for class {}", class_name))?;

        let versions = message_info.versions;

        // Generate schema constants for each version
        for v in versions.lowest()..=versions.highest() {
            if let Some(declaration) = message_info.schema_for_version.get(&v) {
                buffer.printf(format!("pub const SCHEMA_{}: Schema =", v));
                buffer.increment_indent();
                for line in declaration.lines() {
                    buffer.printf(line);
                }
                buffer.decrement_indent();
                buffer.printf("");
            } else {
                buffer.printf(format!("pub const SCHEMA_{}: Schema = SCHEMA_{};", v, v - 1));
                buffer.printf("");
            }
        }

        // Generate SCHEMAS array
        buffer.printf("pub const SCHEMAS: &[Option<&Schema>] = &[");
        buffer.increment_indent();
        for v in 0..versions.lowest() {
            let comma = if v == versions.highest() { "" } else { "," };
            buffer.printf(format!("None{}", comma));
        }
        for v in versions.lowest()..=versions.highest() {
            let comma = if v == versions.highest() { "" } else { "," };
            buffer.printf(format!("Some(&SCHEMA_{}){}", v, comma));
        }
        buffer.decrement_indent();
        buffer.printf("];");
        buffer.printf("");

        buffer.printf(format!("pub const LOWEST_SUPPORTED_VERSION: i16 = {};", versions.lowest()));
        buffer.printf(format!("pub const HIGHEST_SUPPORTED_VERSION: i16 = {};", versions.highest()));

        Ok(())
    }
}

impl Default for SchemaGenerator {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_struct_registry_creation() {
        let registry = StructRegistry::new();
        assert!(registry.structs.is_empty());
        assert!(registry.common_struct_names.is_empty());
    }

    #[test]
    fn test_first_is_capitalized() {
        assert!(StructRegistry::first_is_capitalized("TestStruct"));
        assert!(!StructRegistry::first_is_capitalized("testStruct"));
        assert!(!StructRegistry::first_is_capitalized(""));
    }

    #[test]
    fn test_schema_generator_creation() {
        let generator = SchemaGenerator::new();
        assert!(generator.messages.is_empty());
        assert_eq!(generator.message_flexible_versions, Versions::NONE);
    }
}
