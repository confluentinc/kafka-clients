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

//! Message generator library - can be used from both build.rs and CLI binary

#![allow(dead_code)]

mod message;

use message::{FieldSpec, FieldType, MessageSpec, StructSpec, Versions};
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

/// Main entry point for generating message code from JSON specifications
pub fn generate_messages(input_dir: &Path, output_dir: &Path) -> Result<(), Box<dyn std::error::Error>> {
    eprintln!("Reading message specifications from: {}", input_dir.display());
    eprintln!("Writing generated code to: {}", output_dir.display());

    // Find all JSON files
    let spec_files = find_json_files(input_dir)?;
    eprintln!("Found {} message specifications", spec_files.len());

    // Create output directory
    fs::create_dir_all(output_dir)?;

    // Process each specification
    let mut success_count = 0;
    for spec_file in &spec_files {
        match process_spec_file(spec_file, output_dir) {
            Ok(()) => success_count += 1,
            Err(e) => {
                eprintln!("  Error: {}", e);
                // Generate stub on error so tests can still compile
                if let Some(file_name) = spec_file.file_stem().and_then(|s| s.to_str()) {
                    let _ = generate_stub_file(file_name, output_dir);
                }
            },
        }
    }

    // Generate mod.rs to include all generated modules
    generate_mod_file(&spec_files, output_dir)?;

    eprintln!(
        "Successfully generated {} out of {} message types",
        success_count,
        spec_files.len()
    );
    eprintln!("Generated code at: {}", output_dir.display());

    Ok(())
}

fn find_json_files(dir: &Path) -> Result<Vec<PathBuf>, Box<dyn std::error::Error>> {
    let mut json_files = Vec::new();

    if dir.is_dir() {
        for entry in fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();

            if path.is_file() && path.extension().and_then(|s| s.to_str()) == Some("json") {
                json_files.push(path);
            }
        }
    }

    json_files.sort();
    Ok(json_files)
}

fn process_spec_file(spec_file: &Path, output_dir: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let file_name = spec_file.file_stem().and_then(|s| s.to_str()).ok_or("Invalid file name")?;

    eprintln!("  Processing: {}", file_name);

    // Read and parse JSON spec
    let json_content = fs::read_to_string(spec_file)?;

    // Strip comments (JSON with comments support)
    let json_content = strip_json_comments(&json_content);

    // Parse message spec
    let message_spec: MessageSpec =
        serde_json::from_str(&json_content).map_err(|e| format!("Failed to parse {}: {}", file_name, e))?;

    // Note: Validation happens automatically during deserialization via MessageSpec::new()

    // Write generated code to file
    let module_name = to_snake_case(file_name);
    let output_file = output_dir.join(format!("{}.rs", module_name));

    let mut file = fs::File::create(&output_file)?;

    // Write license header
    write_license_header(&mut file)?;

    // Write generated code
    writeln!(file, "//! Generated from {}.json", file_name)?;
    writeln!(file)?;
    writeln!(file, "#![allow(unused_imports)]")?;
    writeln!(file, "#![allow(dead_code)]")?;
    writeln!(file)?;
    writeln!(file, "use crate::common::protocol::{{Readable, Writable, RawTaggedField}};")?;
    writeln!(file, "use crate::common::Uuid;")?;
    writeln!(file)?;

    // Generate the message struct
    generate_message_struct(&mut file, &message_spec)?;

    Ok(())
}

fn generate_message_struct(file: &mut fs::File, spec: &MessageSpec) -> Result<(), Box<dyn std::error::Error>> {
    let struct_spec = spec.struct_spec();
    let data_class_name = format!("{}Data", spec.name());
    let flexible_versions = spec.flexible_versions();

    // Generate common structs first (defined at message level)
    for common_struct in spec.common_structs() {
        generate_common_struct(file, common_struct, flexible_versions)?;
    }

    // Generate nested structs (for array element types and direct struct types)
    for field in struct_spec.fields() {
        // Always try to generate nested struct - the function will determine if it's needed
        generate_nested_struct(file, field, flexible_versions)?;
    }

    // Generate main struct
    writeln!(file, "/// {}", spec.name())?;
    if let Some(api_key) = spec.api_key() {
        writeln!(file, "/// API Key: {}", api_key)?;
    }
    writeln!(
        file,
        "/// Valid Versions: {}-{}",
        struct_spec.versions().lowest(),
        struct_spec.versions().highest()
    )?;
    writeln!(file, "#[derive(Debug, Clone, PartialEq)]")?;
    writeln!(file, "pub struct {} {{", data_class_name)?;

    // Generate fields
    for field in struct_spec.fields() {
        let field_name = to_snake_case(field.name());
        let field_name = escape_rust_keyword(&field_name);
        let rust_type = field_type_to_rust(field.field_type());

        if !field.about().is_empty() {
            writeln!(file, "    /// {}", field.about())?;
        }
        writeln!(file, "    pub {}: {},", field_name, rust_type)?;
    }

    writeln!(file, "}}")?;
    writeln!(file)?;

    // Generate impl block
    writeln!(file, "impl {} {{", data_class_name)?;

    // Constructor
    writeln!(file, "    pub fn new() -> Self {{")?;
    writeln!(file, "        Self {{")?;
    for field in struct_spec.fields() {
        let field_name = to_snake_case(field.name());
        let field_name = escape_rust_keyword(&field_name);
        let default_val = get_default_value(field.field_type(), field.field_default());
        writeln!(file, "            {}: {},", field_name, default_val)?;
    }
    writeln!(file, "        }}")?;
    writeln!(file, "    }}")?;
    writeln!(file)?;

    // Version constants
    writeln!(
        file,
        "    pub const LOWEST_SUPPORTED_VERSION: i16 = {};",
        struct_spec.versions().lowest()
    )?;
    writeln!(
        file,
        "    pub const HIGHEST_SUPPORTED_VERSION: i16 = {};",
        struct_spec.versions().highest()
    )?;

    if let Some(api_key) = spec.api_key() {
        writeln!(file, "    pub const API_KEY: i16 = {};", api_key)?;
    }
    writeln!(file)?;

    // read() method
    generate_read_method(file, &data_class_name, struct_spec, flexible_versions)?;
    writeln!(file)?;

    // write() method
    generate_write_method(file, &data_class_name, struct_spec, flexible_versions)?;

    writeln!(file, "}}")?;
    writeln!(file)?;

    Ok(())
}

fn generate_nested_struct(
    file: &mut fs::File,
    field: &FieldSpec,
    flexible_versions: Versions,
) -> Result<(), Box<dyn std::error::Error>> {
    // Get the struct name from either direct Struct type or Array(Struct) type
    let struct_name = match field.field_type() {
        FieldType::Struct(name) => {
            // Direct struct field (e.g., CurrentLeader: LeaderIdAndEpoch)
            name.clone()
        },
        FieldType::Array(element_type) => {
            if let FieldType::Struct(name) = element_type.as_ref() {
                // Array of structs (e.g., Topics: []TopicData)
                name.clone()
            } else {
                return Ok(()); // Not a struct array
            }
        },
        _ => return Ok(()), // Not a struct type
    };

    // Only generate if the field has nested fields
    if field.fields().is_empty() {
        return Ok(());
    }

    // First, recursively generate any nested structs within this nested struct
    for nested_field in field.fields() {
        if !nested_field.fields().is_empty() {
            generate_nested_struct(file, nested_field, flexible_versions)?;
        }
    }

    writeln!(file, "/// Nested struct for {}", struct_name)?;
    writeln!(file, "#[derive(Debug, Clone, PartialEq)]")?;
    writeln!(file, "pub struct {} {{", struct_name)?;

    for nested_field in field.fields() {
        let field_name = to_snake_case(nested_field.name());
        let field_name = escape_rust_keyword(&field_name);
        let rust_type = field_type_to_rust(nested_field.field_type());

        if !nested_field.about().is_empty() {
            writeln!(file, "    /// {}", nested_field.about())?;
        }
        writeln!(file, "    pub {}: {},", field_name, rust_type)?;
    }

    writeln!(file, "}}")?;
    writeln!(file)?;

    // Generate impl for nested struct with new(), read(), and write()
    writeln!(file, "impl {} {{", struct_name)?;
    writeln!(file, "    pub fn new() -> Self {{")?;
    writeln!(file, "        Self {{")?;
    for nested_field in field.fields() {
        let field_name = to_snake_case(nested_field.name());
        let field_name = escape_rust_keyword(&field_name);
        let default_val = get_default_value(nested_field.field_type(), nested_field.field_default());
        writeln!(file, "            {}: {},", field_name, default_val)?;
    }
    writeln!(file, "        }}")?;
    writeln!(file, "    }}")?;
    writeln!(file)?;

    // Create a temporary StructSpec from the field for read/write generation
    let versions_str = format!("{}-{}", field.versions().lowest(), field.versions().highest());
    let struct_spec = StructSpec::new(struct_name.clone(), Some(&versions_str), None, field.fields().to_vec())?;

    // Generate read method
    generate_read_method(file, &struct_name, &struct_spec, flexible_versions)?;
    writeln!(file)?;

    // Generate write method
    generate_write_method(file, &struct_name, &struct_spec, flexible_versions)?;

    writeln!(file, "}}")?;
    writeln!(file)?;

    Ok(())
}

fn generate_common_struct(
    file: &mut fs::File,
    struct_spec: &StructSpec,
    flexible_versions: Versions,
) -> Result<(), Box<dyn std::error::Error>> {
    let struct_name = struct_spec.name();

    // First, recursively generate any nested structs within this common struct
    for field in struct_spec.fields() {
        if !field.fields().is_empty() {
            generate_nested_struct(file, field, flexible_versions)?;
        }
    }

    writeln!(file, "/// Common struct: {}", struct_name)?;
    writeln!(
        file,
        "/// Valid Versions: {}-{}",
        struct_spec.versions().lowest(),
        struct_spec.versions().highest()
    )?;
    writeln!(file, "#[derive(Debug, Clone, PartialEq)]")?;
    writeln!(file, "pub struct {} {{", struct_name)?;

    for field in struct_spec.fields() {
        let field_name = to_snake_case(field.name());
        let field_name = escape_rust_keyword(&field_name);
        let rust_type = field_type_to_rust(field.field_type());

        if !field.about().is_empty() {
            writeln!(file, "    /// {}", field.about())?;
        }
        writeln!(file, "    pub {}: {},", field_name, rust_type)?;
    }

    writeln!(file, "}}")?;
    writeln!(file)?;

    // Generate impl with new(), read(), and write()
    writeln!(file, "impl {} {{", struct_name)?;
    writeln!(file, "    pub fn new() -> Self {{")?;
    writeln!(file, "        Self {{")?;
    for field in struct_spec.fields() {
        let field_name = to_snake_case(field.name());
        let field_name = escape_rust_keyword(&field_name);
        let default_val = get_default_value(field.field_type(), field.field_default());
        writeln!(file, "            {}: {},", field_name, default_val)?;
    }
    writeln!(file, "        }}")?;
    writeln!(file, "    }}")?;
    writeln!(file)?;

    // Generate read method for common struct
    generate_read_method(file, struct_name, struct_spec, flexible_versions)?;
    writeln!(file)?;

    // Generate write method for common struct
    generate_write_method(file, struct_name, struct_spec, flexible_versions)?;

    writeln!(file, "}}")?;
    writeln!(file)?;

    Ok(())
}

fn generate_tagged_field_read(
    file: &mut fs::File,
    tagged_fields: &[&FieldSpec],
    _flexible_versions: Versions,
    indented: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let indent = if indented { "    " } else { "" };

    writeln!(
        file,
        "{}        let num_tagged_fields = readable.read_unsigned_varint()?;",
        indent
    )?;
    writeln!(file, "{}        for _ in 0..num_tagged_fields {{", indent)?;
    writeln!(file, "{}            let tag = readable.read_unsigned_varint()?;", indent)?;
    writeln!(file, "{}            let size = readable.read_unsigned_varint()?;", indent)?;
    writeln!(file, "{}            match tag {{", indent)?;

    // Generate cases for each known tagged field
    for field in tagged_fields {
        if let Some(tag) = field.tag() {
            let field_name = to_snake_case(field.name());
            let field_name = escape_rust_keyword(&field_name);
            writeln!(file, "{}                {} => {{", indent, tag)?;
            writeln!(file, "{}                    // Tagged field: {}", indent, field.name())?;

            // Generate the read code for this tagged field
            match field.field_type() {
                FieldType::String => {
                    writeln!(
                        file,
                        "{}                    let length = readable.read_unsigned_varint()?;",
                        indent
                    )?;
                    writeln!(file, "{}                    if length == 0 {{", indent)?;
                    writeln!(file, "{}                        result.{} = String::new();", indent, field_name)?;
                    writeln!(file, "{}                    }} else {{", indent)?;
                    writeln!(
                        file,
                        "{}                        let mut bytes = vec![0u8; (length - 1) as usize];",
                        indent
                    )?;
                    writeln!(file, "{}                        readable.read_bytes(&mut bytes)?;", indent)?;
                    writeln!(
                        file,
                        "{}                        result.{} = String::from_utf8(bytes)",
                        indent, field_name
                    )?;
                    writeln!(
                        file,
                        "{}                            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;",
                        indent
                    )?;
                    writeln!(file, "{}                    }}", indent)?;
                },
                FieldType::Bool => {
                    writeln!(
                        file,
                        "{}                    result.{} = readable.read_byte()? != 0;",
                        indent, field_name
                    )?;
                },
                FieldType::Int8 => {
                    writeln!(
                        file,
                        "{}                    result.{} = readable.read_byte()? as i8;",
                        indent, field_name
                    )?;
                },
                FieldType::Int16 => {
                    writeln!(
                        file,
                        "{}                    result.{} = readable.read_short()?;",
                        indent, field_name
                    )?;
                },
                FieldType::Int32 => {
                    writeln!(
                        file,
                        "{}                    result.{} = readable.read_int()?;",
                        indent, field_name
                    )?;
                },
                FieldType::Int64 => {
                    writeln!(
                        file,
                        "{}                    result.{} = readable.read_long()?;",
                        indent, field_name
                    )?;
                },
                FieldType::Uuid => {
                    writeln!(
                        file,
                        "{}                    result.{} = readable.read_uuid()?;",
                        indent, field_name
                    )?;
                },
                FieldType::Array(element_type) => {
                    writeln!(
                        file,
                        "{}                    let length = readable.read_unsigned_varint()?;",
                        indent
                    )?;
                    writeln!(file, "{}                    if length == 0 {{", indent)?;
                    writeln!(file, "{}                        result.{} = Vec::new();", indent, field_name)?;
                    writeln!(file, "{}                    }} else {{", indent)?;
                    writeln!(file, "{}                        let length = length - 1;", indent)?;
                    writeln!(
                        file,
                        "{}                        result.{} = Vec::with_capacity(length as usize);",
                        indent, field_name
                    )?;
                    writeln!(file, "{}                        for _ in 0..length {{", indent)?;

                    match element_type.as_ref() {
                        FieldType::String => {
                            writeln!(
                                file,
                                "{}                            let len = readable.read_unsigned_varint()?;",
                                indent
                            )?;
                            writeln!(file, "{}                            if len == 0 {{", indent)?;
                            writeln!(
                                file,
                                "{}                                result.{}.push(String::new());",
                                indent, field_name
                            )?;
                            writeln!(file, "{}                            }} else {{", indent)?;
                            writeln!(
                                file,
                                "{}                                let mut bytes = vec![0u8; (len - 1) as usize];",
                                indent
                            )?;
                            writeln!(
                                file,
                                "{}                                readable.read_bytes(&mut bytes)?;",
                                indent
                            )?;
                            writeln!(
                                file,
                                "{}                                result.{}.push(String::from_utf8(bytes)",
                                indent, field_name
                            )?;
                            writeln!(
                                file,
                                "{}                                    .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?);",
                                indent
                            )?;
                            writeln!(file, "{}                            }}", indent)?;
                        },
                        FieldType::Struct(struct_name) => {
                            writeln!(
                                file,
                                "{}                            result.{}.push({}::read(readable, version)?);",
                                indent, field_name, struct_name
                            )?;
                        },
                        FieldType::Uuid => {
                            writeln!(
                                file,
                                "{}                            result.{}.push(readable.read_uuid()?);",
                                indent, field_name
                            )?;
                        },
                        _ => {
                            // Other primitive types
                            writeln!(
                                file,
                                "{}                            // TODO: implement for {:?}",
                                indent, element_type
                            )?;
                        },
                    }

                    writeln!(file, "{}                        }}", indent)?;
                    writeln!(file, "{}                    }}", indent)?;
                },
                FieldType::Struct(struct_name) => {
                    // For structs, we read the bytes and then parse the struct from them
                    writeln!(
                        file,
                        "{}                    let mut struct_bytes = vec![0u8; size as usize];",
                        indent
                    )?;
                    writeln!(file, "{}                    readable.read_bytes(&mut struct_bytes)?;", indent)?;
                    writeln!(
                        file,
                        "{}                    let mut struct_accessor = crate::common::protocol::ByteBufferAccessor::from_bytes(struct_bytes);",
                        indent
                    )?;
                    writeln!(
                        file,
                        "{}                    result.{} = {}::read(&mut struct_accessor, version)?;",
                        indent, field_name, struct_name
                    )?;
                },
                _ => {
                    writeln!(
                        file,
                        "{}                    // TODO: implement for {:?}",
                        indent,
                        field.field_type()
                    )?;
                },
            }

            writeln!(file, "{}                }}", indent)?;
        }
    }

    writeln!(file, "{}                _ => {{", indent)?;
    writeln!(file, "{}                    // Unknown tagged field, skip it", indent)?;
    writeln!(file, "{}                    readable.read_array(size as usize)?;", indent)?;
    writeln!(file, "{}                }}", indent)?;
    writeln!(file, "{}            }}", indent)?;
    writeln!(file, "{}        }}", indent)?;

    Ok(())
}

fn generate_tagged_field_write(
    file: &mut fs::File,
    tagged_fields: &[&FieldSpec],
    _flexible_versions: Versions,
    indented: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let indent = if indented { "    " } else { "" };

    // Count how many tagged fields are actually set for this version
    writeln!(file, "{}        // Write tagged fields (flexible version)", indent)?;
    writeln!(file, "{}        let mut num_tagged_fields = 0u32;", indent)?;

    for field in tagged_fields {
        let field_name = to_snake_case(field.name());
        let field_name = escape_rust_keyword(&field_name);
        let tagged_versions = field.tagged_versions();

        // Check if this tagged field should be written for this version
        if !tagged_versions.empty() {
            if tagged_versions.highest() >= i16::MAX {
                writeln!(file, "{}        if version >= {} {{", indent, tagged_versions.lowest())?;
            } else {
                writeln!(
                    file,
                    "{}        if version >= {} && version <= {} {{",
                    indent,
                    tagged_versions.lowest(),
                    tagged_versions.highest()
                )?;
            }

            // Check if field has non-default value (for optional fields)
            match field.field_type() {
                FieldType::String => {
                    writeln!(file, "{}            if !self.{}.is_empty() {{", indent, field_name)?;
                    writeln!(file, "{}                num_tagged_fields += 1;", indent)?;
                    writeln!(file, "{}            }}", indent)?;
                },
                FieldType::Array(_) => {
                    writeln!(file, "{}            if !self.{}.is_empty() {{", indent, field_name)?;
                    writeln!(file, "{}                num_tagged_fields += 1;", indent)?;
                    writeln!(file, "{}            }}", indent)?;
                },
                _ => {
                    // For non-optional types, always write
                    writeln!(file, "{}            num_tagged_fields += 1;", indent)?;
                },
            }

            writeln!(file, "{}        }}", indent)?;
        }
    }

    writeln!(file, "{}        writable.write_unsigned_varint(num_tagged_fields)?;", indent)?;

    // Now write each tagged field
    for field in tagged_fields {
        if let Some(tag) = field.tag() {
            let field_name = to_snake_case(field.name());
            let field_name = escape_rust_keyword(&field_name);
            let tagged_versions = field.tagged_versions();

            if !tagged_versions.empty() {
                if tagged_versions.highest() >= i16::MAX {
                    writeln!(file, "{}        if version >= {} {{", indent, tagged_versions.lowest())?;
                } else {
                    writeln!(
                        file,
                        "{}        if version >= {} && version <= {} {{",
                        indent,
                        tagged_versions.lowest(),
                        tagged_versions.highest()
                    )?;
                }

                // Check if we should write this field
                let should_write = match field.field_type() {
                    FieldType::String | FieldType::Array(_) => {
                        format!("!self.{}.is_empty()", field_name)
                    },
                    _ => "true".to_string(),
                };

                writeln!(file, "{}            if {} {{", indent, should_write)?;
                writeln!(
                    file,
                    "{}                writable.write_unsigned_varint({})?; // tag",
                    indent, tag
                )?;

                // Calculate and write size, then write the field data
                match field.field_type() {
                    FieldType::String => {
                        writeln!(file, "{}                let bytes = self.{}.as_bytes();", indent, field_name)?;
                        writeln!(
                            file,
                            "{}                let size = (bytes.len() as u32) + 1; // +1 for varint length",
                            indent
                        )?;
                        writeln!(file, "{}                writable.write_unsigned_varint(size)?;", indent)?;
                        writeln!(
                            file,
                            "{}                writable.write_unsigned_varint((bytes.len() as u32) + 1)?;",
                            indent
                        )?;
                        writeln!(file, "{}                writable.write_bytes(bytes)?;", indent)?;
                    },
                    FieldType::Bool => {
                        writeln!(
                            file,
                            "{}                writable.write_unsigned_varint(1)?; // size = 1 byte",
                            indent
                        )?;
                        writeln!(
                            file,
                            "{}                writable.write_byte(if self.{} {{ 1 }} else {{ 0 }})?;",
                            indent, field_name
                        )?;
                    },
                    FieldType::Int8 => {
                        writeln!(
                            file,
                            "{}                writable.write_unsigned_varint(1)?; // size = 1 byte",
                            indent
                        )?;
                        writeln!(file, "{}                writable.write_byte(self.{})?;", indent, field_name)?;
                    },
                    FieldType::Int16 => {
                        writeln!(
                            file,
                            "{}                writable.write_unsigned_varint(2)?; // size = 2 bytes",
                            indent
                        )?;
                        writeln!(file, "{}                writable.write_short(self.{})?;", indent, field_name)?;
                    },
                    FieldType::Int32 => {
                        writeln!(
                            file,
                            "{}                writable.write_unsigned_varint(4)?; // size = 4 bytes",
                            indent
                        )?;
                        writeln!(file, "{}                writable.write_int(self.{})?;", indent, field_name)?;
                    },
                    FieldType::Int64 => {
                        writeln!(
                            file,
                            "{}                writable.write_unsigned_varint(8)?; // size = 8 bytes",
                            indent
                        )?;
                        writeln!(file, "{}                writable.write_long(self.{})?;", indent, field_name)?;
                    },
                    FieldType::Uuid => {
                        writeln!(
                            file,
                            "{}                writable.write_unsigned_varint(16)?; // size = 16 bytes",
                            indent
                        )?;
                        writeln!(file, "{}                writable.write_uuid(&self.{})?;", indent, field_name)?;
                    },
                    FieldType::Array(_) => {
                        writeln!(file, "{}                // TODO: calculate actual size for array", indent)?;
                        writeln!(file, "{}                // For now, write a placeholder", indent)?;
                        writeln!(
                            file,
                            "{}                writable.write_unsigned_varint(0)?; // size placeholder",
                            indent
                        )?;
                    },
                    FieldType::Struct(_) => {
                        // For structs, we need to calculate the size first by writing to a temp buffer
                        writeln!(file, "{}                // Calculate struct size", indent)?;
                        writeln!(
                            file,
                            "{}                let mut size_accessor = crate::common::protocol::ByteBufferAccessor::new(256);",
                            indent
                        )?;
                        writeln!(
                            file,
                            "{}                self.{}.write(&mut size_accessor, version)?;",
                            indent, field_name
                        )?;
                        writeln!(file, "{}                let size = size_accessor.len() as u32;", indent)?;
                        writeln!(file, "{}                writable.write_unsigned_varint(size)?;", indent)?;
                        writeln!(file, "{}                writable.write_bytes(size_accessor.buffer())?;", indent)?;
                    },
                    _ => {
                        writeln!(
                            file,
                            "{}                // TODO: implement for {:?}",
                            indent,
                            field.field_type()
                        )?;
                    },
                }

                writeln!(file, "{}            }}", indent)?;
                writeln!(file, "{}        }}", indent)?;
            }
        }
    }

    Ok(())
}

fn generate_read_method(
    file: &mut fs::File,
    class_name: &str,
    struct_spec: &StructSpec,
    flexible_versions: Versions,
) -> Result<(), Box<dyn std::error::Error>> {
    writeln!(
        file,
        "    pub fn read(readable: &mut dyn Readable, version: i16) -> std::io::Result<Self> {{"
    )?;

    let lowest = struct_spec.versions().lowest();
    let highest = struct_spec.versions().highest();

    // Generate version check - avoid useless comparison when highest is i16::MAX
    if highest >= i16::MAX {
        writeln!(file, "        if version < {} {{", lowest)?;
    } else {
        writeln!(file, "        if version < {} || version > {} {{", lowest, highest)?;
    }

    writeln!(file, "            return Err(std::io::Error::new(")?;
    writeln!(file, "                std::io::ErrorKind::InvalidData,")?;
    writeln!(
        file,
        "                format!(\"Invalid version {{}} for {}\", version),",
        class_name
    )?;
    writeln!(file, "            ));")?;
    writeln!(file, "        }}")?;
    writeln!(file)?;
    writeln!(file, "        let mut result = Self::new();")?;
    writeln!(file)?;

    // Generate read for each non-tagged field
    for field in struct_spec.fields() {
        if field.tagged_versions().empty() {
            generate_field_read(file, field, flexible_versions)?;
        }
    }

    // Read tagged fields if this is a flexible version
    let tagged_fields: Vec<&FieldSpec> = struct_spec.fields().iter().filter(|f| !f.tagged_versions().empty()).collect();

    if !flexible_versions.empty() && !tagged_fields.is_empty() {
        writeln!(file)?;
        if flexible_versions.lowest() == 0 {
            // All versions are flexible - always read tagged fields
            writeln!(file, "        // Read tagged fields (flexible version)")?;
            generate_tagged_field_read(file, &tagged_fields, flexible_versions, false)?;
        } else {
            // Only some versions are flexible
            if flexible_versions.highest() >= i16::MAX {
                writeln!(file, "        if version >= {} {{", flexible_versions.lowest())?;
            } else {
                writeln!(
                    file,
                    "        if version >= {} && version <= {} {{",
                    flexible_versions.lowest(),
                    flexible_versions.highest()
                )?;
            }
            writeln!(file, "            // Read tagged fields (flexible version)")?;
            generate_tagged_field_read(file, &tagged_fields, flexible_versions, true)?;
            writeln!(file, "        }}")?;
        }
    } else if !flexible_versions.empty() {
        // No tagged fields defined, just skip unknown ones
        writeln!(file)?;
        if flexible_versions.lowest() == 0 {
            writeln!(file, "        // Read tagged fields (flexible version)")?;
            writeln!(file, "        let num_tagged_fields = readable.read_unsigned_varint()?;")?;
            writeln!(file, "        for _ in 0..num_tagged_fields {{")?;
            writeln!(file, "            let _tag = readable.read_unsigned_varint()?;")?;
            writeln!(file, "            let size = readable.read_unsigned_varint()?;")?;
            writeln!(file, "            readable.read_array(size as usize)?;")?;
            writeln!(file, "        }}")?;
        } else {
            if flexible_versions.highest() >= i16::MAX {
                writeln!(file, "        if version >= {} {{", flexible_versions.lowest())?;
            } else {
                writeln!(
                    file,
                    "        if version >= {} && version <= {} {{",
                    flexible_versions.lowest(),
                    flexible_versions.highest()
                )?;
            }
            writeln!(file, "            // Read tagged fields (flexible version)")?;
            writeln!(file, "            let num_tagged_fields = readable.read_unsigned_varint()?;")?;
            writeln!(file, "            for _ in 0..num_tagged_fields {{")?;
            writeln!(file, "                let _tag = readable.read_unsigned_varint()?;")?;
            writeln!(file, "                let size = readable.read_unsigned_varint()?;")?;
            writeln!(file, "                readable.read_array(size as usize)?;")?;
            writeln!(file, "            }}")?;
            writeln!(file, "        }}")?;
        }
    }

    writeln!(file, "        Ok(result)")?;
    writeln!(file, "    }}")?;

    Ok(())
}

fn generate_write_method(
    file: &mut fs::File,
    class_name: &str,
    struct_spec: &StructSpec,
    flexible_versions: Versions,
) -> Result<(), Box<dyn std::error::Error>> {
    writeln!(
        file,
        "    pub fn write(&self, writable: &mut dyn Writable, version: i16) -> std::io::Result<()> {{"
    )?;

    let lowest = struct_spec.versions().lowest();
    let highest = struct_spec.versions().highest();

    // Generate version check - avoid useless comparison when highest is i16::MAX
    if highest >= i16::MAX {
        writeln!(file, "        if version < {} {{", lowest)?;
    } else {
        writeln!(file, "        if version < {} || version > {} {{", lowest, highest)?;
    }

    writeln!(file, "            return Err(std::io::Error::new(")?;
    writeln!(file, "                std::io::ErrorKind::InvalidData,")?;
    writeln!(
        file,
        "                format!(\"Invalid version {{}} for {}\", version),",
        class_name
    )?;
    writeln!(file, "            ));")?;
    writeln!(file, "        }}")?;
    writeln!(file)?;

    // Generate write for each non-tagged field
    for field in struct_spec.fields() {
        if field.tagged_versions().empty() {
            generate_field_write(file, field, flexible_versions)?;
        }
    }

    // Write tagged fields if this is a flexible version
    let tagged_fields: Vec<&FieldSpec> = struct_spec.fields().iter().filter(|f| !f.tagged_versions().empty()).collect();

    if !flexible_versions.empty() && !tagged_fields.is_empty() {
        writeln!(file)?;
        if flexible_versions.lowest() == 0 {
            // All versions are flexible - always write tagged fields
            generate_tagged_field_write(file, &tagged_fields, flexible_versions, false)?;
        } else {
            // Only some versions are flexible
            if flexible_versions.highest() >= i16::MAX {
                writeln!(file, "        if version >= {} {{", flexible_versions.lowest())?;
            } else {
                writeln!(
                    file,
                    "        if version >= {} && version <= {} {{",
                    flexible_versions.lowest(),
                    flexible_versions.highest()
                )?;
            }
            generate_tagged_field_write(file, &tagged_fields, flexible_versions, true)?;
            writeln!(file, "        }}")?;
        }
    } else if !flexible_versions.empty() {
        // No tagged fields defined, just write 0
        writeln!(file)?;
        if flexible_versions.lowest() == 0 {
            writeln!(file, "        // Write tagged fields (flexible version)")?;
            writeln!(file, "        writable.write_unsigned_varint(0)?; // No tagged fields")?;
        } else {
            if flexible_versions.highest() >= i16::MAX {
                writeln!(file, "        if version >= {} {{", flexible_versions.lowest())?;
            } else {
                writeln!(
                    file,
                    "        if version >= {} && version <= {} {{",
                    flexible_versions.lowest(),
                    flexible_versions.highest()
                )?;
            }
            writeln!(file, "            // Write tagged fields (flexible version)")?;
            writeln!(file, "            writable.write_unsigned_varint(0)?; // No tagged fields")?;
            writeln!(file, "        }}")?;
        }
    }

    writeln!(file, "        Ok(())")?;
    writeln!(file, "    }}")?;

    Ok(())
}

fn generate_field_read(
    file: &mut fs::File,
    field: &FieldSpec,
    flexible_versions: Versions,
) -> Result<(), Box<dyn std::error::Error>> {
    let field_name = to_snake_case(field.name());
    let field_name = escape_rust_keyword(&field_name);
    let versions = field.versions();

    // Determine indentation based on whether we have a version check
    let has_version_check = versions != Versions::ALL;
    let indent = if has_version_check { "            " } else { "        " };

    // Version check - avoid useless comparison when highest is i16::MAX
    if has_version_check {
        if versions.highest() >= i16::MAX {
            writeln!(file, "        if version >= {} {{", versions.lowest())?;
        } else {
            writeln!(
                file,
                "        if version >= {} && version <= {} {{",
                versions.lowest(),
                versions.highest()
            )?;
        }
    }

    match field.field_type() {
        FieldType::Bool => {
            writeln!(file, "{}result.{} = readable.read_byte()? != 0;", indent, field_name)?;
        },
        FieldType::Int8 => {
            writeln!(file, "{}result.{} = readable.read_byte()? as i8;", indent, field_name)?;
        },
        FieldType::Int16 => {
            writeln!(file, "{}result.{} = readable.read_short()?;", indent, field_name)?;
        },
        FieldType::Int32 => {
            writeln!(file, "{}result.{} = readable.read_int()?;", indent, field_name)?;
        },
        FieldType::Int64 => {
            writeln!(file, "{}result.{} = readable.read_long()?;", indent, field_name)?;
        },
        FieldType::Uint16 => {
            writeln!(file, "{}result.{} = readable.read_unsigned_short()?;", indent, field_name)?;
        },
        FieldType::Uint32 => {
            writeln!(file, "{}result.{} = readable.read_unsigned_int()?;", indent, field_name)?;
        },
        FieldType::Uuid => {
            writeln!(file, "{}result.{} = readable.read_uuid()?;", indent, field_name)?;
        },
        FieldType::Float64 => {
            writeln!(file, "{}result.{} = readable.read_double()?;", indent, field_name)?;
        },
        FieldType::String => {
            // Check if this version uses flexible encoding
            if !flexible_versions.empty() {
                if flexible_versions.lowest() == 0 {
                    // All versions are flexible
                    writeln!(file, "{}// Flexible version: read_unsigned_varint length + 1", indent)?;
                    writeln!(file, "{}let len = readable.read_unsigned_varint()?;", indent)?;
                    writeln!(file, "{}if len == 0 {{", indent)?;
                    writeln!(
                        file,
                        "{}    return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, \"Null string not allowed\"));",
                        indent
                    )?;
                    writeln!(file, "{}}}", indent)?;
                    writeln!(file, "{}let length = len - 1;", indent)?;
                } else {
                    if flexible_versions.highest() >= i16::MAX {
                        writeln!(file, "{}let length = if version >= {} {{", indent, flexible_versions.lowest())?;
                    } else {
                        writeln!(
                            file,
                            "{}let length = if version >= {} && version <= {} {{",
                            indent,
                            flexible_versions.lowest(),
                            flexible_versions.highest()
                        )?;
                    }
                    writeln!(file, "{}    // Flexible version: read_unsigned_varint length + 1", indent)?;
                    writeln!(file, "{}    let len = readable.read_unsigned_varint()?;", indent)?;
                    writeln!(file, "{}    if len == 0 {{", indent)?;
                    writeln!(
                        file,
                        "{}        return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, \"Null string not allowed\"));",
                        indent
                    )?;
                    writeln!(file, "{}    }}", indent)?;
                    writeln!(file, "{}    len - 1", indent)?;
                    writeln!(file, "{}}} else {{", indent)?;
                    writeln!(file, "{}    // Standard version: read_short length", indent)?;
                    writeln!(file, "{}    let len = readable.read_short()?;", indent)?;
                    writeln!(file, "{}    if len < 0 {{", indent)?;
                    writeln!(
                        file,
                        "{}        return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, \"Negative string length\"));",
                        indent
                    )?;
                    writeln!(file, "{}    }}", indent)?;
                    writeln!(file, "{}    len as u32", indent)?;
                    writeln!(file, "{}}};", indent)?
                }
            } else {
                writeln!(file, "{}// Standard version: read_short length", indent)?;
                writeln!(file, "{}let length = {{", indent)?;
                writeln!(file, "{}    let len = readable.read_short()?;", indent)?;
                writeln!(file, "{}    if len < 0 {{", indent)?;
                writeln!(
                    file,
                    "{}        return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, \"Negative string length\"));",
                    indent
                )?;
                writeln!(file, "{}    }}", indent)?;
                writeln!(file, "{}    len as u32", indent)?;
                writeln!(file, "{}}};", indent)?;
            }
            writeln!(file, "{}let mut bytes = vec![0u8; length as usize];", indent)?;
            writeln!(file, "{}readable.read_bytes(&mut bytes)?;", indent)?;
            writeln!(file, "{}result.{} = String::from_utf8(bytes)", indent, field_name)?;
            writeln!(
                file,
                "{}    .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;",
                indent
            )?
        },
        FieldType::Bytes | FieldType::Records => {
            // Check if this version uses flexible encoding
            if !flexible_versions.empty() {
                if flexible_versions.highest() >= i16::MAX {
                    writeln!(file, "            let length = if version >= {} {{", flexible_versions.lowest())?;
                } else {
                    writeln!(
                        file,
                        "            let length = if version >= {} && version <= {} {{",
                        flexible_versions.lowest(),
                        flexible_versions.highest()
                    )?;
                }
                writeln!(file, "                // Flexible version: read_unsigned_varint length + 1")?;
                writeln!(file, "                let len = readable.read_unsigned_varint()?;")?;
                writeln!(file, "                if len == 0 {{ 0 }} else {{ len - 1 }}")?;
                writeln!(file, "            }} else {{")?;
                writeln!(file, "                // Standard version: read_int length")?;
                writeln!(file, "                let len = readable.read_int()?;")?;
                writeln!(file, "                if len < 0 {{ 0 }} else {{ len as u32 }}")?;
                writeln!(file, "            }};")?;
            } else {
                writeln!(file, "            // Standard version: read_int length")?;
                writeln!(file, "            let length = {{")?;
                writeln!(file, "                let len = readable.read_int()?;")?;
                writeln!(file, "                if len < 0 {{ 0 }} else {{ len as u32 }}")?;
                writeln!(file, "            }};")?;
            }
            writeln!(file, "            if length == 0 {{")?;
            writeln!(file, "                result.{} = Vec::new();", field_name)?;
            writeln!(file, "            }} else {{")?;
            writeln!(file, "                let mut bytes = vec![0u8; length as usize];")?;
            writeln!(file, "                readable.read_bytes(&mut bytes)?;")?;
            writeln!(file, "                result.{} = bytes;", field_name)?;
            writeln!(file, "            }}")?;
        },
        FieldType::Array(element_type) => {
            // Check if this version uses flexible encoding
            if !flexible_versions.empty() {
                if flexible_versions.lowest() == 0 {
                    // All versions are flexible
                    writeln!(file, "            // Flexible version: read_unsigned_varint length + 1")?;
                    writeln!(file, "            let len = readable.read_unsigned_varint()?;")?;
                    writeln!(file, "            if len == 0 {{")?;
                    writeln!(
                        file,
                        "                return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, \"Null array not allowed\"));"
                    )?;
                    writeln!(file, "            }}")?;
                    writeln!(file, "            let length = len - 1;")?;
                } else {
                    if flexible_versions.highest() >= i16::MAX {
                        writeln!(file, "            let length = if version >= {} {{", flexible_versions.lowest())?;
                    } else {
                        writeln!(
                            file,
                            "            let length = if version >= {} && version <= {} {{",
                            flexible_versions.lowest(),
                            flexible_versions.highest()
                        )?;
                    }
                    writeln!(file, "                // Flexible version: read_unsigned_varint length + 1")?;
                    writeln!(file, "                let len = readable.read_unsigned_varint()?;")?;
                    writeln!(file, "                if len == 0 {{")?;
                    writeln!(
                        file,
                        "                    return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, \"Null array not allowed\"));"
                    )?;
                    writeln!(file, "                }}")?;
                    writeln!(file, "                len - 1")?;
                    writeln!(file, "            }} else {{")?;
                    writeln!(file, "                // Standard version: read_int length")?;
                    writeln!(file, "                let len = readable.read_int()?;")?;
                    writeln!(file, "                if len < 0 {{")?;
                    writeln!(
                        file,
                        "                    return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, \"Negative array length\"));"
                    )?;
                    writeln!(file, "                }}")?;
                    writeln!(file, "                len as u32")?;
                    writeln!(file, "            }};")?;
                }
            } else {
                writeln!(file, "            // Standard version: read_int length")?;
                writeln!(file, "            let length = {{")?;
                writeln!(file, "                let len = readable.read_int()?;")?;
                writeln!(file, "                if len < 0 {{")?;
                writeln!(
                    file,
                    "                    return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, \"Negative array length\"));"
                )?;
                writeln!(file, "                }}")?;
                writeln!(file, "                len as u32")?;
                writeln!(file, "            }};")?;
            }
            writeln!(file, "            result.{} = Vec::with_capacity(length as usize);", field_name)?;
            writeln!(file, "            for _ in 0..length {{")?;
            generate_array_element_read(file, element_type.as_ref(), &field_name)?;
            writeln!(file, "            }}")?;
        },
        FieldType::Struct(struct_name) => {
            writeln!(
                file,
                "{}result.{} = {}::read(readable, version)?;",
                indent, field_name, struct_name
            )?;
        },
    }

    if versions != Versions::ALL {
        writeln!(file, "        }}")?;
    }
    writeln!(file)?;

    Ok(())
}

fn generate_array_element_read(
    file: &mut fs::File,
    element_type: &FieldType,
    array_name: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    match element_type {
        FieldType::Bool => {
            writeln!(file, "                result.{}.push(readable.read_byte()? != 0);", array_name)?;
        },
        FieldType::Int8 => {
            writeln!(file, "                result.{}.push(readable.read_byte()? as i8);", array_name)?;
        },
        FieldType::Int16 => {
            writeln!(file, "                result.{}.push(readable.read_short()?);", array_name)?;
        },
        FieldType::Int32 => {
            writeln!(file, "                result.{}.push(readable.read_int()?);", array_name)?;
        },
        FieldType::Int64 => {
            writeln!(file, "                result.{}.push(readable.read_long()?);", array_name)?;
        },
        FieldType::Uint16 => {
            writeln!(
                file,
                "                result.{}.push(readable.read_unsigned_short()?);",
                array_name
            )?;
        },
        FieldType::Uint32 => {
            writeln!(
                file,
                "                result.{}.push(readable.read_unsigned_int()?);",
                array_name
            )?;
        },
        FieldType::Uuid => {
            writeln!(file, "                result.{}.push(readable.read_uuid()?);", array_name)?;
        },
        FieldType::Float64 => {
            writeln!(file, "                result.{}.push(readable.read_double()?);", array_name)?;
        },
        FieldType::String => {
            writeln!(file, "                let length = readable.read_short()?;")?;
            writeln!(file, "                if length < 0 {{")?;
            writeln!(
                file,
                "                    return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, \"Negative string length\"));"
            )?;
            writeln!(file, "                }}")?;
            writeln!(file, "                let mut bytes = vec![0u8; length as usize];")?;
            writeln!(file, "                readable.read_bytes(&mut bytes)?;")?;
            writeln!(file, "                result.{}.push(String::from_utf8(bytes)", array_name)?;
            writeln!(
                file,
                "                    .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?);"
            )?;
        },
        FieldType::Bytes | FieldType::Records => {
            writeln!(file, "                let length = readable.read_int()?;")?;
            writeln!(file, "                if length < 0 {{")?;
            writeln!(file, "                    result.{}.push(Vec::new());", array_name)?;
            writeln!(file, "                }} else {{")?;
            writeln!(file, "                    let mut bytes = vec![0u8; length as usize];")?;
            writeln!(file, "                    readable.read_bytes(&mut bytes)?;")?;
            writeln!(file, "                    result.{}.push(bytes);", array_name)?;
            writeln!(file, "                }}")?;
        },
        FieldType::Struct(struct_name) => {
            writeln!(
                file,
                "                result.{}.push({}::read(readable, version)?);",
                array_name, struct_name
            )?;
        },
        FieldType::Array(_) => {
            // Nested arrays not common in Kafka protocol
            writeln!(file, "                // TODO: Nested array not implemented")?;
        },
    }

    Ok(())
}

fn generate_field_write(
    file: &mut fs::File,
    field: &FieldSpec,
    flexible_versions: Versions,
) -> Result<(), Box<dyn std::error::Error>> {
    let field_name = to_snake_case(field.name());
    let field_name = escape_rust_keyword(&field_name);
    let versions = field.versions();

    // Determine if version check is needed
    let has_version_check = versions != Versions::ALL;
    let indent = if has_version_check { "            " } else { "        " };

    // Version check - avoid useless comparison when highest is i16::MAX
    if has_version_check {
        if versions.highest() >= i16::MAX {
            writeln!(file, "        if version >= {} {{", versions.lowest())?;
        } else {
            writeln!(
                file,
                "        if version >= {} && version <= {} {{",
                versions.lowest(),
                versions.highest()
            )?;
        }
    }

    match field.field_type() {
        FieldType::Bool => {
            writeln!(
                file,
                "{}writable.write_byte(if self.{} {{ 1 }} else {{ 0 }})?;",
                indent, field_name
            )?;
        },
        FieldType::Int8 => {
            writeln!(file, "{}writable.write_byte(self.{})?;", indent, field_name)?;
        },
        FieldType::Int16 => {
            writeln!(file, "{}writable.write_short(self.{})?;", indent, field_name)?;
        },
        FieldType::Int32 => {
            writeln!(file, "{}writable.write_int(self.{})?;", indent, field_name)?;
        },
        FieldType::Int64 => {
            writeln!(file, "{}writable.write_long(self.{})?;", indent, field_name)?;
        },
        FieldType::Uint16 => {
            writeln!(file, "{}writable.write_unsigned_short(self.{})?;", indent, field_name)?;
        },
        FieldType::Uint32 => {
            writeln!(file, "{}writable.write_unsigned_int(self.{})?;", indent, field_name)?;
        },
        FieldType::Uuid => {
            writeln!(file, "{}writable.write_uuid(&self.{})?;", indent, field_name)?;
        },
        FieldType::Float64 => {
            writeln!(file, "{}writable.write_double(self.{})?;", indent, field_name)?;
        },
        FieldType::String => {
            writeln!(file, "{}let bytes = self.{}.as_bytes();", indent, field_name)?;
            // Check if this version uses flexible encoding
            if !flexible_versions.empty() {
                if flexible_versions.lowest() == 0 {
                    // All versions are flexible
                    writeln!(file, "{}// Flexible version: write_unsigned_varint(length + 1)", indent)?;
                    writeln!(file, "{}writable.write_unsigned_varint((bytes.len() as u32) + 1)?;", indent)?;
                } else {
                    if flexible_versions.highest() >= i16::MAX {
                        writeln!(file, "{}if version >= {} {{", indent, flexible_versions.lowest())?;
                    } else {
                        writeln!(
                            file,
                            "{}if version >= {} && version <= {} {{",
                            indent,
                            flexible_versions.lowest(),
                            flexible_versions.highest()
                        )?;
                    }
                    writeln!(file, "{}    // Flexible version: write_unsigned_varint(length + 1)", indent)?;
                    writeln!(file, "{}    writable.write_unsigned_varint((bytes.len() as u32) + 1)?;", indent)?;
                    writeln!(file, "{}}} else {{", indent)?;
                    writeln!(file, "{}    // Standard version: write_short(length)", indent)?;
                    writeln!(file, "{}    writable.write_short(bytes.len() as i16)?;", indent)?;
                    writeln!(file, "{}}}", indent)?;
                }
            } else {
                writeln!(file, "{}// Standard version: write_short(length)", indent)?;
                writeln!(file, "{}writable.write_short(bytes.len() as i16)?;", indent)?;
            }
            writeln!(file, "{}writable.write_bytes(bytes)?;", indent)?;
        },
        FieldType::Bytes | FieldType::Records => {
            // Check if this version uses flexible encoding
            if !flexible_versions.empty() {
                if flexible_versions.lowest() == 0 {
                    // All versions are flexible
                    writeln!(file, "            // Flexible version: write_unsigned_varint(length + 1)")?;
                    writeln!(
                        file,
                        "            writable.write_unsigned_varint((self.{}.len() as u32) + 1)?;",
                        field_name
                    )?;
                } else {
                    if flexible_versions.highest() >= i16::MAX {
                        writeln!(file, "            if version >= {} {{", flexible_versions.lowest())?;
                    } else {
                        writeln!(
                            file,
                            "            if version >= {} && version <= {} {{",
                            flexible_versions.lowest(),
                            flexible_versions.highest()
                        )?;
                    }
                    writeln!(file, "                // Flexible version: write_unsigned_varint(length + 1)")?;
                    writeln!(
                        file,
                        "                writable.write_unsigned_varint((self.{}.len() as u32) + 1)?;",
                        field_name
                    )?;
                    writeln!(file, "            }} else {{")?;
                    writeln!(file, "                // Standard version: write_int(length)")?;
                    writeln!(file, "                writable.write_int(self.{}.len() as i32)?;", field_name)?;
                    writeln!(file, "            }}")?;
                }
            } else {
                writeln!(file, "            // Standard version: write_int(length)")?;
                writeln!(file, "            writable.write_int(self.{}.len() as i32)?;", field_name)?;
            }
            writeln!(file, "            writable.write_bytes(&self.{})?;", field_name)?;
        },
        FieldType::Array(element_type) => {
            // Check if this version uses flexible encoding
            if !flexible_versions.empty() {
                if flexible_versions.lowest() == 0 {
                    // All versions are flexible
                    writeln!(file, "            // Flexible version: write_unsigned_varint(length + 1)")?;
                    writeln!(
                        file,
                        "            writable.write_unsigned_varint((self.{}.len() as u32) + 1)?;",
                        field_name
                    )?;
                } else {
                    if flexible_versions.highest() >= i16::MAX {
                        writeln!(file, "            if version >= {} {{", flexible_versions.lowest())?;
                    } else {
                        writeln!(
                            file,
                            "            if version >= {} && version <= {} {{",
                            flexible_versions.lowest(),
                            flexible_versions.highest()
                        )?;
                    }
                    writeln!(file, "                // Flexible version: write_unsigned_varint(length + 1)")?;
                    writeln!(
                        file,
                        "                writable.write_unsigned_varint((self.{}.len() as u32) + 1)?;",
                        field_name
                    )?;
                    writeln!(file, "            }} else {{")?;
                    writeln!(file, "                // Standard version: write_int(length)")?;
                    writeln!(file, "                writable.write_int(self.{}.len() as i32)?;", field_name)?;
                    writeln!(file, "            }}")?;
                }
            } else {
                writeln!(file, "            // Standard version: write_int(length)")?;
                writeln!(file, "            writable.write_int(self.{}.len() as i32)?;", field_name)?;
            }
            writeln!(file, "{}for element in &self.{} {{", indent, field_name)?;
            generate_array_element_write(file, element_type.as_ref())?;
            writeln!(file, "{}}}", indent)?;
        },
        FieldType::Struct(_) => {
            writeln!(file, "{}self.{}.write(writable, version)?;", indent, field_name)?;
        },
    }

    if has_version_check {
        writeln!(file, "        }}")?;
    }
    writeln!(file)?;

    Ok(())
}

fn generate_array_element_write(
    file: &mut fs::File,
    element_type: &FieldType,
) -> Result<(), Box<dyn std::error::Error>> {
    match element_type {
        FieldType::Bool => {
            writeln!(file, "                writable.write_byte(if *element {{ 1 }} else {{ 0 }})?;")?;
        },
        FieldType::Int8 => {
            writeln!(file, "                writable.write_byte(*element)?;")?;
        },
        FieldType::Int16 => {
            writeln!(file, "                writable.write_short(*element)?;")?;
        },
        FieldType::Int32 => {
            writeln!(file, "                writable.write_int(*element)?;")?;
        },
        FieldType::Int64 => {
            writeln!(file, "                writable.write_long(*element)?;")?;
        },
        FieldType::Uint16 => {
            writeln!(file, "                writable.write_unsigned_short(*element)?;")?;
        },
        FieldType::Uint32 => {
            writeln!(file, "                writable.write_unsigned_int(*element)?;")?;
        },
        FieldType::Uuid => {
            writeln!(file, "                writable.write_uuid(element)?;")?;
        },
        FieldType::Float64 => {
            writeln!(file, "                writable.write_double(*element)?;")?;
        },
        FieldType::String => {
            writeln!(file, "                let bytes = element.as_bytes();")?;
            writeln!(file, "                writable.write_short(bytes.len() as i16)?;")?;
            writeln!(file, "                writable.write_bytes(bytes)?;")?;
        },
        FieldType::Bytes | FieldType::Records => {
            writeln!(file, "                writable.write_int(element.len() as i32)?;")?;
            writeln!(file, "                writable.write_bytes(element)?;")?;
        },
        FieldType::Struct(_) => {
            writeln!(file, "                element.write(writable, version)?;")?;
        },
        FieldType::Array(_) => {
            // Nested arrays not common
            writeln!(file, "                // TODO: Nested array not implemented")?;
        },
    }

    Ok(())
}

fn field_type_to_rust(field_type: &FieldType) -> String {
    match field_type {
        FieldType::Bool => "bool".to_string(),
        FieldType::Int8 => "i8".to_string(),
        FieldType::Int16 => "i16".to_string(),
        FieldType::Int32 => "i32".to_string(),
        FieldType::Int64 => "i64".to_string(),
        FieldType::Uint16 => "u16".to_string(),
        FieldType::Uint32 => "u32".to_string(),
        FieldType::Uuid => "Uuid".to_string(),
        FieldType::Float64 => "f64".to_string(),
        FieldType::String => "String".to_string(),
        FieldType::Bytes => "Vec<u8>".to_string(),
        FieldType::Records => "Vec<u8>".to_string(),
        FieldType::Array(element_type) => {
            format!("Vec<{}>", field_type_to_rust(element_type))
        },
        FieldType::Struct(name) => name.clone(),
    }
}

fn get_default_value(field_type: &FieldType, default: Option<&serde_json::Value>) -> String {
    // Use explicit default if provided
    if let Some(val) = default {
        match val {
            serde_json::Value::Bool(b) => return b.to_string(),
            serde_json::Value::Number(n) => {
                // Handle special numeric strings that need parsing
                return n.to_string();
            },
            serde_json::Value::String(s) => {
                // Special case: "null" string for nullable fields means empty/default
                if s == "null" {
                    match field_type {
                        FieldType::String => return "String::new()".to_string(),
                        FieldType::Bytes | FieldType::Records => return "Vec::new()".to_string(),
                        FieldType::Array(_) => return "Vec::new()".to_string(),
                        _ => {}, // Fall through
                    }
                }

                // Try to parse as number if field type is numeric
                match field_type {
                    FieldType::Int8
                    | FieldType::Int16
                    | FieldType::Int32
                    | FieldType::Int64
                    | FieldType::Uint16
                    | FieldType::Uint32 => {
                        // Handle hex notation like "0x7fffffff"
                        if s.starts_with("0x") {
                            if let Ok(num) = i64::from_str_radix(&s[2..], 16) {
                                return num.to_string();
                            }
                        } else if s.starts_with("-0x") {
                            if let Ok(num) = i64::from_str_radix(&s[3..], 16) {
                                return format!("-{}", num);
                            }
                        } else if let Ok(_) = s.parse::<i64>() {
                            // It's a valid number string, return it as-is (no quotes)
                            return s.to_string();
                        }
                    },
                    FieldType::Float64 => {
                        if let Ok(_) = s.parse::<f64>() {
                            return s.to_string();
                        }
                    },
                    FieldType::Bool => {
                        if s == "true" {
                            return "true".to_string();
                        } else if s == "false" {
                            return "false".to_string();
                        }
                    },
                    FieldType::String => {
                        // Regular string value - need .to_string() call
                        if s.is_empty() {
                            return "String::new()".to_string();
                        }
                        return format!("\"{}\".to_string()", s);
                    },
                    _ => {},
                }

                // If we can't parse it as the expected type, use type default
            },
            serde_json::Value::Null => {}, // Fall through to type default
            _ => {},
        }
    }

    // Type defaults
    match field_type {
        FieldType::Bool => "false".to_string(),
        FieldType::Int8 | FieldType::Int16 | FieldType::Int32 | FieldType::Int64 => "0".to_string(),
        FieldType::Uint16 | FieldType::Uint32 => "0".to_string(),
        FieldType::Float64 => "0.0".to_string(),
        FieldType::String => "String::new()".to_string(),
        FieldType::Bytes | FieldType::Records => "Vec::new()".to_string(),
        FieldType::Array(_) => "Vec::new()".to_string(),
        FieldType::Uuid => "Uuid::zero()".to_string(),
        FieldType::Struct(name) => format!("{}::new()", name),
    }
}

fn escape_rust_keyword(name: &str) -> String {
    const KEYWORDS: &[&str] = &[
        "as", "break", "const", "continue", "crate", "else", "enum", "extern", "false", "fn", "for", "if", "impl",
        "in", "let", "loop", "match", "mod", "move", "mut", "pub", "ref", "return", "self", "Self", "static", "struct",
        "super", "trait", "true", "type", "unsafe", "use", "where", "while", "async", "await", "dyn", "abstract",
        "become", "box", "do", "final", "macro", "override", "priv", "typeof", "unsized", "virtual", "yield", "try",
        "union",
    ];

    if KEYWORDS.contains(&name) {
        format!("r#{}", name)
    } else {
        name.to_string()
    }
}

fn generate_mod_file(spec_files: &[PathBuf], output_dir: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let mod_file = output_dir.join("mod.rs");
    let mut file = fs::File::create(mod_file)?;

    write_license_header(&mut file)?;

    writeln!(file, "// Generated message modules")?;
    writeln!(file)?;

    for spec_file in spec_files {
        let file_name = spec_file.file_stem().and_then(|s| s.to_str()).ok_or("Invalid file name")?;
        let module_name = to_snake_case(file_name);
        writeln!(file, "pub mod {};", module_name)?;
    }

    Ok(())
}

fn write_license_header(file: &mut fs::File) -> Result<(), Box<dyn std::error::Error>> {
    writeln!(file, "/*")?;
    writeln!(file, " * Licensed to the Apache Software Foundation (ASF) under one or more")?;
    writeln!(file, " * contributor license agreements. See the NOTICE file distributed with")?;
    writeln!(file, " * this work for additional information regarding copyright ownership.")?;
    writeln!(
        file,
        " * The ASF licenses this file to You under the Apache License, Version 2.0"
    )?;
    writeln!(
        file,
        " * (the \"License\"); you may not use this file except in compliance with"
    )?;
    writeln!(file, " * the License. You may obtain a copy of the License at")?;
    writeln!(file, " *")?;
    writeln!(file, " *    http://www.apache.org/licenses/LICENSE-2.0")?;
    writeln!(file, " *")?;
    writeln!(file, " * Unless required by applicable law or agreed to in writing, software")?;
    writeln!(file, " * distributed under the License is distributed on an \"AS IS\" BASIS,")?;
    writeln!(
        file,
        " * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied."
    )?;
    writeln!(file, " * See the License for the specific language governing permissions and")?;
    writeln!(file, " * limitations under the License.")?;
    writeln!(file, " */")?;
    writeln!(file)?;
    Ok(())
}

fn generate_stub_file(file_name: &str, output_dir: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let module_name = to_snake_case(file_name);
    let output_file = output_dir.join(format!("{}.rs", module_name));

    let mut file = fs::File::create(&output_file)?;

    write_license_header(&mut file)?;

    writeln!(file, "//! Generated from {}.json (stub due to parse error)", file_name)?;
    writeln!(file)?;
    writeln!(file, "#![allow(dead_code)]")?;
    writeln!(file)?;
    writeln!(file, "#[derive(Debug, Clone)]")?;
    writeln!(file, "pub struct {}Data {{", file_name)?;
    writeln!(file, "    // Fields will be generated when spec can be parsed")?;
    writeln!(file, "}}")?;
    writeln!(file)?;
    writeln!(file, "impl {}Data {{", file_name)?;
    writeln!(file, "    pub fn new() -> Self {{")?;
    writeln!(file, "        {}Data {{}}", file_name)?;
    writeln!(file, "    }}")?;
    writeln!(file, "}}")?;

    Ok(())
}

fn strip_json_comments(json: &str) -> String {
    let mut result = String::new();
    let mut in_string = false;
    let mut escape_next = false;
    let mut chars = json.chars().peekable();

    while let Some(c) = chars.next() {
        if escape_next {
            result.push(c);
            escape_next = false;
            continue;
        }

        if c == '\\' && in_string {
            result.push(c);
            escape_next = true;
            continue;
        }

        if c == '"' {
            in_string = !in_string;
            result.push(c);
            continue;
        }

        if !in_string && c == '/' {
            if let Some(&next_c) = chars.peek() {
                if next_c == '/' {
                    // Line comment - skip until newline
                    chars.next();
                    while let Some(c) = chars.next() {
                        if c == '\n' {
                            result.push('\n');
                            break;
                        }
                    }
                    continue;
                }
            }
        }

        result.push(c);
    }

    result
}

pub fn to_snake_case(s: &str) -> String {
    let mut result = String::new();
    let chars: Vec<char> = s.chars().collect();

    for i in 0..chars.len() {
        let c = chars[i];

        if c.is_uppercase() {
            // Add underscore before uppercase letter if:
            // - Not at the beginning
            // - Previous char was lowercase or digit
            // - OR this is the last uppercase in a sequence (e.g., "APIVersion" -> "API_Version")
            if i > 0 {
                let prev_is_lower = chars[i - 1].is_lowercase() || chars[i - 1].is_numeric();
                let next_is_lower = i + 1 < chars.len() && chars[i + 1].is_lowercase();

                if prev_is_lower || (next_is_lower && i > 0 && chars[i - 1].is_uppercase()) {
                    result.push('_');
                }
            }
            result.push(c.to_ascii_lowercase());
        } else {
            result.push(c);
        }
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_to_snake_case() {
        assert_eq!(to_snake_case("ProduceRequest"), "produce_request");
        assert_eq!(to_snake_case("FetchRequest"), "fetch_request");
        assert_eq!(to_snake_case("APIVersionsRequest"), "api_versions_request");
        assert_eq!(to_snake_case("API"), "api");
        assert_eq!(to_snake_case("SimpleHTTPServer"), "simple_http_server");
        assert_eq!(to_snake_case("ALLCAPS"), "allcaps");
    }

    #[test]
    fn test_strip_json_comments() {
        let json = r#"{
            // This is a comment
            "name": "test", // inline comment
            "value": 123
        }"#;

        let stripped = strip_json_comments(json);
        assert!(!stripped.contains("// This is a comment"));
        assert!(!stripped.contains("// inline comment"));
        assert!(stripped.contains("\"name\""));
        assert!(stripped.contains("\"test\""));
    }
}
