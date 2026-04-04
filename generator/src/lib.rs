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

//! Message generator library - can be used from both build.rs and CLI binary

#![deny(warnings)]
#![allow(dead_code)]

mod message;

use message::{FieldSpec, FieldType, MessageSpec, MessageSpecType, RequestListenerType, StructSpec, Versions};
use std::collections::BTreeMap;
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

/// Intermediate structure to collect request/response specs for one API key.
struct ApiData {
    api_key: i16,
    request_spec: Option<MessageSpec>,
    response_spec: Option<MessageSpec>,
}

impl ApiData {
    fn name(&self, fallback_names: &BTreeMap<i16, String>) -> String {
        if let Some(ref spec) = self.request_spec {
            spec.name().strip_suffix("Request").unwrap_or(spec.name()).to_string()
        } else if let Some(ref spec) = self.response_spec {
            spec.name().strip_suffix("Response").unwrap_or(spec.name()).to_string()
        } else if let Some(name) = fallback_names.get(&self.api_key) {
            name.clone()
        } else {
            panic!("Neither requestSpec nor responseSpec is defined for API key {}", self.api_key);
        }
    }

    fn has_valid_versions(&self) -> bool {
        self.request_spec
            .as_ref()
            .is_some_and(|s| s.valid_versions().highest() >= s.valid_versions().lowest())
    }
}

/// Generate an `api_message_type.rs` file — the Rust equivalent of Java's generated
/// `ApiMessageType` enum.
///
/// Reads all JSON message specifications from `input_dir`, collects per-API-key metadata
/// (version ranges, flexible version thresholds, listener types, deprecation info),
/// and writes a Rust source file to `output_dir/api_message_type.rs`.
pub fn generate_api_message_type(input_dir: &Path, output_dir: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let spec_files = find_json_files(input_dir)?;

    let mut apis: BTreeMap<i16, ApiData> = BTreeMap::new();

    for spec_file in &spec_files {
        let json_content = fs::read_to_string(spec_file)?;
        let json_content = strip_json_comments(&json_content);

        // Try full MessageSpec parsing first
        match serde_json::from_str::<MessageSpec>(&json_content) {
            Ok(spec) => {
                let api_key = match spec.api_key() {
                    Some(k) => k,
                    None => continue,
                };
                let entry =
                    apis.entry(api_key)
                        .or_insert_with(|| ApiData { api_key, request_spec: None, response_spec: None });
                match spec.msg_type() {
                    MessageSpecType::Request => entry.request_spec = Some(spec),
                    MessageSpecType::Response => entry.response_spec = Some(spec),
                    _ => {},
                }
            },
            Err(_) => {
                // Fallback: specs with validVersions="none" may fail full parsing.
                // Extract just apiKey, name, type to register the API key.
                let parsed: serde_json::Value = match serde_json::from_str(&json_content) {
                    Ok(v) => v,
                    Err(_) => continue,
                };
                let api_key = match parsed.get("apiKey").and_then(|v| v.as_i64()) {
                    Some(k) => k as i16,
                    None => continue,
                };
                let msg_type = parsed.get("type").and_then(|v| v.as_str()).unwrap_or("");
                let name = parsed.get("name").and_then(|v| v.as_str()).unwrap_or("").to_string();
                let base_name = name
                    .strip_suffix("Request")
                    .or_else(|| name.strip_suffix("Response"))
                    .unwrap_or(&name)
                    .to_string();

                // Only register the entry with a name if it doesn't exist yet
                let entry =
                    apis.entry(api_key)
                        .or_insert_with(|| ApiData { api_key, request_spec: None, response_spec: None });

                // Create a minimal stub MessageSpec for the removed API
                if (msg_type == "request" && entry.request_spec.is_none())
                    || (msg_type == "response" && entry.response_spec.is_none())
                {
                    // We need at least the name for display. We'll generate a stub
                    // with valid_versions="none" (lowest > highest).
                    // Use the fallback name stored in the ApiData::name() via the
                    // existing request_spec or response_spec if available.
                    // For removed APIs, we simply leave the specs as None and handle
                    // them in code generation.
                    let _ = base_name; // name is captured via the entry
                }
            },
        }
    }

    // For entries that have no specs at all (fully removed APIs), we need to
    // provide a name. Collect names from the JSON as a fallback.
    let mut api_names: BTreeMap<i16, String> = BTreeMap::new();
    for spec_file in &spec_files {
        let json_content = fs::read_to_string(spec_file)?;
        let json_content = strip_json_comments(&json_content);
        let parsed: serde_json::Value = match serde_json::from_str(&json_content) {
            Ok(v) => v,
            Err(_) => continue,
        };
        if let (Some(api_key), Some(name)) = (
            parsed.get("apiKey").and_then(|v| v.as_i64()),
            parsed.get("name").and_then(|v| v.as_str()),
        ) {
            let base = name
                .strip_suffix("Request")
                .or_else(|| name.strip_suffix("Response"))
                .unwrap_or(name);
            api_names.entry(api_key as i16).or_insert_with(|| base.to_string());
        }
    }

    fs::create_dir_all(output_dir)?;
    let output_file = output_dir.join("api_message_type.rs");
    let mut file = fs::File::create(&output_file)?;

    write_license_header(&mut file)?;
    writeln!(file, "//! Generated from JSON message specifications.")?;
    writeln!(file, "//!")?;
    writeln!(file, "//! Rust equivalent of Java's generated `ApiMessageType` enum.")?;
    writeln!(
        file,
        "//! Provides version ranges, header version logic, and listener information"
    )?;
    writeln!(file, "//! for each Kafka API key.")?;
    writeln!(file)?;
    writeln!(file, "#![allow(unused_imports)]")?;
    writeln!(file)?;
    writeln!(file, "use crate::common::protocol::Schema;")?;
    writeln!(file)?;

    // --- ListenerType enum ---
    writeln!(file, "/// Kafka listener types.")?;
    writeln!(file, "#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]")?;
    writeln!(file, "pub enum ListenerType {{")?;
    writeln!(file, "    Broker,")?;
    writeln!(file, "    Controller,")?;
    writeln!(file, "}}")?;
    writeln!(file)?;

    // --- ApiMessageType enum ---
    writeln!(file, "/// Identifiers and metadata for every Kafka API.")?;
    writeln!(file, "#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]")?;
    writeln!(file, "#[allow(non_camel_case_types)]")?;
    writeln!(file, "pub enum ApiMessageType {{")?;

    for data in apis.values() {
        let name = to_snake_case(&data.name(&api_names)).to_uppercase();
        writeln!(file, "    /// {} (key {})", data.name(&api_names), data.api_key)?;
        writeln!(file, "    {},", name)?;
    }
    writeln!(file, "}}")?;
    writeln!(file)?;

    // --- impl block ---
    writeln!(file, "impl ApiMessageType {{")?;

    // api_key()
    writeln!(file, "    /// The permanent and immutable numeric id of this API.")?;
    writeln!(file, "    pub fn api_key(self) -> i16 {{")?;
    writeln!(file, "        match self {{")?;
    for data in apis.values() {
        let variant = to_snake_case(&data.name(&api_names)).to_uppercase();
        writeln!(file, "            Self::{} => {},", variant, data.api_key)?;
    }
    writeln!(file, "        }}")?;
    writeln!(file, "    }}")?;
    writeln!(file)?;

    // name()
    writeln!(file, "    /// The human-readable name of this API.")?;
    writeln!(file, "    pub fn name(self) -> &'static str {{")?;
    writeln!(file, "        match self {{")?;
    for data in apis.values() {
        let variant = to_snake_case(&data.name(&api_names)).to_uppercase();
        writeln!(file, "            Self::{} => \"{}\",", variant, data.name(&api_names))?;
    }
    writeln!(file, "        }}")?;
    writeln!(file, "    }}")?;
    writeln!(file)?;

    // lowest_supported_version()
    writeln!(file, "    /// The lowest supported version of this API.")?;
    writeln!(file, "    pub fn lowest_supported_version(self) -> i16 {{")?;
    writeln!(file, "        match self {{")?;
    for data in apis.values() {
        let variant = to_snake_case(&data.name(&api_names)).to_uppercase();
        let low = data.request_spec.as_ref().map(|s| s.valid_versions().lowest()).unwrap_or(0);
        writeln!(file, "            Self::{} => {},", variant, low)?;
    }
    writeln!(file, "        }}")?;
    writeln!(file, "    }}")?;
    writeln!(file)?;

    // highest_supported_version()
    writeln!(file, "    /// The highest supported version of this API.")?;
    writeln!(
        file,
        "    pub fn highest_supported_version(self, enable_unstable_last_version: bool) -> i16 {{"
    )?;
    writeln!(file, "        let highest = match self {{")?;
    for data in apis.values() {
        let variant = to_snake_case(&data.name(&api_names)).to_uppercase();
        let high = data.request_spec.as_ref().map(|s| s.valid_versions().highest()).unwrap_or(-1);
        writeln!(file, "            Self::{} => {},", variant, high)?;
    }
    writeln!(file, "        }};")?;
    writeln!(
        file,
        "        if !self.latest_version_unstable() || enable_unstable_last_version {{"
    )?;
    writeln!(file, "            highest")?;
    writeln!(file, "        }} else {{")?;
    writeln!(file, "            highest - 1")?;
    writeln!(file, "        }}")?;
    writeln!(file, "    }}")?;
    writeln!(file)?;

    // lowest_deprecated_version()
    writeln!(file, "    /// The lowest deprecated version of this API.")?;
    writeln!(file, "    pub fn lowest_deprecated_version(self) -> i16 {{")?;
    writeln!(file, "        match self {{")?;
    for data in apis.values() {
        let variant = to_snake_case(&data.name(&api_names)).to_uppercase();
        let low = data
            .request_spec
            .as_ref()
            .map(|s| s.struct_spec().deprecated_versions().lowest())
            .unwrap_or(0);
        writeln!(file, "            Self::{} => {},", variant, low)?;
    }
    writeln!(file, "        }}")?;
    writeln!(file, "    }}")?;
    writeln!(file)?;

    // highest_deprecated_version()
    writeln!(file, "    /// The highest deprecated version of this API.")?;
    writeln!(file, "    pub fn highest_deprecated_version(self) -> i16 {{")?;
    writeln!(file, "        match self {{")?;
    for data in apis.values() {
        let variant = to_snake_case(&data.name(&api_names)).to_uppercase();
        let high = data
            .request_spec
            .as_ref()
            .map(|s| s.struct_spec().deprecated_versions().highest())
            .unwrap_or(-1);
        writeln!(file, "            Self::{} => {},", variant, high)?;
    }
    writeln!(file, "        }}")?;
    writeln!(file, "    }}")?;
    writeln!(file)?;

    // latest_version_unstable()
    writeln!(file, "    /// Whether the latest version is unstable.")?;
    writeln!(file, "    pub fn latest_version_unstable(self) -> bool {{")?;
    writeln!(file, "        match self {{")?;
    for data in apis.values() {
        let variant = to_snake_case(&data.name(&api_names)).to_uppercase();
        let unstable = data.request_spec.as_ref().map(|s| s.latest_version_unstable()).unwrap_or(false);
        writeln!(file, "            Self::{} => {},", variant, unstable)?;
    }
    writeln!(file, "        }}")?;
    writeln!(file, "    }}")?;
    writeln!(file)?;

    // listeners()
    writeln!(file, "    /// The listener types this API is available on.")?;
    writeln!(file, "    pub fn listeners(self) -> &'static [ListenerType] {{")?;
    writeln!(file, "        match self {{")?;
    for data in apis.values() {
        let variant = to_snake_case(&data.name(&api_names)).to_uppercase();
        let listeners: Vec<String> = data
            .request_spec
            .as_ref()
            .map(|s| {
                s.listeners()
                    .iter()
                    .map(|l| match l {
                        RequestListenerType::Broker => "ListenerType::Broker".to_string(),
                        RequestListenerType::Controller => "ListenerType::Controller".to_string(),
                    })
                    .collect()
            })
            .unwrap_or_default();
        writeln!(file, "            Self::{} => &[{}],", variant, listeners.join(", "))?;
    }
    writeln!(file, "        }}")?;
    writeln!(file, "    }}")?;
    writeln!(file)?;

    // request_header_version()
    writeln!(file, "    /// The request header version to use for a given API version.")?;
    writeln!(file, "    pub fn request_header_version(self, version: i16) -> i16 {{")?;
    writeln!(file, "        match self {{")?;
    for data in apis.values() {
        let variant = to_snake_case(&data.name(&api_names)).to_uppercase();
        if let Some(ref spec) = data.request_spec {
            let valid = spec.valid_versions();
            if valid.highest() < valid.lowest() {
                // No valid versions
                continue;
            }
            let flex = spec.flexible_versions();
            if flex == Versions::NONE {
                // No flexible versions — always header v1
                writeln!(file, "            Self::{} => 1,", variant)?;
            } else if flex.lowest() <= valid.lowest() {
                // All valid versions are flexible — always header v2
                writeln!(file, "            Self::{} => 2,", variant)?;
            } else {
                writeln!(
                    file,
                    "            Self::{} => if version >= {} {{ 2 }} else {{ 1 }},",
                    variant,
                    flex.lowest()
                )?;
            }
        }
    }
    writeln!(file, "            #[allow(unreachable_patterns)]")?;
    writeln!(file, "            _ => 1,")?;
    writeln!(file, "        }}")?;
    writeln!(file, "    }}")?;
    writeln!(file)?;

    // response_header_version()
    writeln!(file, "    /// The response header version to use for a given API version.")?;
    writeln!(file, "    ///")?;
    writeln!(file, "    /// ApiVersionsResponse always uses header version 0 (KIP-511).")?;
    writeln!(file, "    pub fn response_header_version(self, version: i16) -> i16 {{")?;
    writeln!(file, "        match self {{")?;
    for data in apis.values() {
        let variant = to_snake_case(&data.name(&api_names)).to_uppercase();
        // ApiVersionsResponse always returns header v0 (KIP-511)
        if data.api_key == 18 {
            writeln!(
                file,
                "            Self::{} => 0, // ApiVersionsResponse always uses v0 header (KIP-511)",
                variant
            )?;
            continue;
        }
        if let Some(ref spec) = data.response_spec {
            let valid = spec.valid_versions();
            if valid.highest() < valid.lowest() {
                continue;
            }
            let flex = spec.flexible_versions();
            if flex == Versions::NONE {
                writeln!(file, "            Self::{} => 0,", variant)?;
            } else if flex.lowest() <= valid.lowest() {
                writeln!(file, "            Self::{} => 1,", variant)?;
            } else {
                writeln!(
                    file,
                    "            Self::{} => if version >= {} {{ 1 }} else {{ 0 }},",
                    variant,
                    flex.lowest()
                )?;
            }
        }
    }
    writeln!(file, "            #[allow(unreachable_patterns)]")?;
    writeln!(file, "            _ => 0,")?;
    writeln!(file, "        }}")?;
    writeln!(file, "    }}")?;
    writeln!(file)?;

    // from_api_key()
    writeln!(file, "    /// Look up an `ApiMessageType` by its numeric API key.")?;
    writeln!(file, "    pub fn from_api_key(api_key: i16) -> Option<Self> {{")?;
    writeln!(file, "        match api_key {{")?;
    for data in apis.values() {
        let variant = to_snake_case(&data.name(&api_names)).to_uppercase();
        writeln!(file, "            {} => Some(Self::{}),", data.api_key, variant)?;
    }
    writeln!(file, "            _ => None,")?;
    writeln!(file, "        }}")?;
    writeln!(file, "    }}")?;
    writeln!(file)?;

    // request_schema()
    writeln!(file, "    /// Returns the request schema for this API at the given version.")?;
    writeln!(file, "    pub fn request_schema(self, version: i16) -> Schema {{")?;
    writeln!(file, "        match self {{")?;
    for data in apis.values() {
        let variant = to_snake_case(&data.name(&api_names)).to_uppercase();
        if data.request_spec.is_some() && data.has_valid_versions() {
            let module = format!("{}_data", to_snake_case(&format!("{}Request", data.name(&api_names))));
            let struct_name = format!("{}RequestData", data.name(&api_names));
            writeln!(
                file,
                "            Self::{} => crate::{}::{}::schema(version),",
                variant, module, struct_name
            )?;
        }
    }
    writeln!(file, "            _ => Schema::new(Vec::new()),")?;
    writeln!(file, "        }}")?;
    writeln!(file, "    }}")?;
    writeln!(file)?;

    // response_schema()
    writeln!(file, "    /// Returns the response schema for this API at the given version.")?;
    writeln!(file, "    pub fn response_schema(self, version: i16) -> Schema {{")?;
    writeln!(file, "        match self {{")?;
    for data in apis.values() {
        let variant = to_snake_case(&data.name(&api_names)).to_uppercase();
        if data.response_spec.is_some() {
            let module = format!("{}_data", to_snake_case(&format!("{}Response", data.name(&api_names))));
            let struct_name = format!("{}ResponseData", data.name(&api_names));
            writeln!(
                file,
                "            Self::{} => crate::{}::{}::schema(version),",
                variant, module, struct_name
            )?;
        }
    }
    writeln!(file, "            _ => Schema::new(Vec::new()),")?;
    writeln!(file, "        }}")?;
    writeln!(file, "    }}")?;

    writeln!(file, "}}")?;

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

    // Write generated code to file (append _data to match the generated struct name)
    let module_name = format!("{}_data", to_snake_case(file_name));
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
    writeln!(
        file,
        "use crate::common::protocol::{{Field, Readable, Schema, SchemaType, Writable, RawTaggedField, Message, ApiMessage, ObjectSerializationCache, MessageSizeAccumulator, ByteBufferAccessor}};"
    )?;
    writeln!(file, "use crate::common::Uuid;")?;
    writeln!(file, "use std::fmt;")?;
    writeln!(file, "use std::hash::{{Hash, Hasher}};")?;
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
    generate_struct_derives_and_impls(file, &data_class_name, struct_spec.fields())?;
    writeln!(file, "pub struct {} {{", data_class_name)?;

    // Generate fields
    for field in struct_spec.fields() {
        let field_name = to_snake_case(field.name());
        let field_name = escape_rust_keyword(&field_name);
        let rust_type = field_type_to_rust_for_field(field);

        if !field.about().is_empty() {
            writeln!(file, "    /// {}", field.about())?;
        }
        writeln!(file, "    pub {}: {},", field_name, rust_type)?;
    }

    writeln!(file, "    /// Unknown tagged fields for forward compatibility.")?;
    writeln!(file, "    pub unknown_tagged_fields: Vec<RawTaggedField>,")?;

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
        let default_val = get_default_value_for_field(field);
        writeln!(file, "            {}: {},", field_name, default_val)?;
    }
    writeln!(file, "            unknown_tagged_fields: Vec::new(),")?;
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
    writeln!(file)?;

    // schema() method
    generate_schema_method(file, struct_spec, flexible_versions)?;

    // Builder setters
    generate_builder_setters(file, struct_spec)?;

    writeln!(file, "}}")?;
    writeln!(file)?;

    // Generate impl Message
    generate_message_impl(file, &data_class_name, struct_spec, flexible_versions)?;

    // Generate impl ApiMessage (only for top-level message structs)
    let api_key = spec.api_key().unwrap_or(-1);
    writeln!(file, "impl ApiMessage for {} {{", data_class_name)?;
    writeln!(file, "    fn api_key(&self) -> i16 {{ {} }}", api_key)?;
    writeln!(file, "}}")?;
    writeln!(file)?;

    // Generate Display impl
    generate_display_impl(file, &data_class_name)?;

    // Generate manual Eq/Hash for structs with f64 fields
    generate_manual_eq_hash(file, &data_class_name, struct_spec.fields())?;

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
    generate_struct_derives_and_impls(file, &struct_name, field.fields())?;
    writeln!(file, "pub struct {} {{", struct_name)?;

    for nested_field in field.fields() {
        let field_name = to_snake_case(nested_field.name());
        let field_name = escape_rust_keyword(&field_name);
        let rust_type = field_type_to_rust_for_field(nested_field);

        if !nested_field.about().is_empty() {
            writeln!(file, "    /// {}", nested_field.about())?;
        }
        writeln!(file, "    pub {}: {},", field_name, rust_type)?;
    }

    writeln!(file, "    /// Unknown tagged fields for forward compatibility.")?;
    writeln!(file, "    pub unknown_tagged_fields: Vec<RawTaggedField>,")?;

    writeln!(file, "}}")?;
    writeln!(file)?;

    // Generate impl for nested struct with new(), read(), and write()
    writeln!(file, "impl {} {{", struct_name)?;
    writeln!(file, "    pub fn new() -> Self {{")?;
    writeln!(file, "        Self {{")?;
    for nested_field in field.fields() {
        let field_name = to_snake_case(nested_field.name());
        let field_name = escape_rust_keyword(&field_name);
        let default_val = get_default_value_for_field(nested_field);
        writeln!(file, "            {}: {},", field_name, default_val)?;
    }
    writeln!(file, "            unknown_tagged_fields: Vec::new(),")?;
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

    // Builder setters
    generate_builder_setters(file, &struct_spec)?;

    writeln!(file, "}}")?;
    writeln!(file)?;

    // Generate impl Message
    generate_message_impl(file, &struct_name, &struct_spec, flexible_versions)?;

    // Generate Display impl
    generate_display_impl(file, &struct_name)?;

    // Generate manual Eq/Hash for structs with f64 fields
    generate_manual_eq_hash(file, &struct_name, field.fields())?;

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
    generate_struct_derives_and_impls(file, struct_name, struct_spec.fields())?;
    writeln!(file, "pub struct {} {{", struct_name)?;

    for field in struct_spec.fields() {
        let field_name = to_snake_case(field.name());
        let field_name = escape_rust_keyword(&field_name);
        let rust_type = field_type_to_rust_for_field(field);

        if !field.about().is_empty() {
            writeln!(file, "    /// {}", field.about())?;
        }
        writeln!(file, "    pub {}: {},", field_name, rust_type)?;
    }

    writeln!(file, "    /// Unknown tagged fields for forward compatibility.")?;
    writeln!(file, "    pub unknown_tagged_fields: Vec<RawTaggedField>,")?;

    writeln!(file, "}}")?;
    writeln!(file)?;

    // Generate impl with new(), read(), and write()
    writeln!(file, "impl {} {{", struct_name)?;
    writeln!(file, "    pub fn new() -> Self {{")?;
    writeln!(file, "        Self {{")?;
    for field in struct_spec.fields() {
        let field_name = to_snake_case(field.name());
        let field_name = escape_rust_keyword(&field_name);
        let default_val = get_default_value_for_field(field);
        writeln!(file, "            {}: {},", field_name, default_val)?;
    }
    writeln!(file, "            unknown_tagged_fields: Vec::new(),")?;
    writeln!(file, "        }}")?;
    writeln!(file, "    }}")?;
    writeln!(file)?;

    // Generate read method for common struct
    generate_read_method(file, struct_name, struct_spec, flexible_versions)?;
    writeln!(file)?;

    // Generate write method for common struct
    generate_write_method(file, struct_name, struct_spec, flexible_versions)?;

    // Builder setters
    generate_builder_setters(file, struct_spec)?;

    writeln!(file, "}}")?;
    writeln!(file)?;

    // Generate impl Message
    generate_message_impl(file, struct_name, struct_spec, flexible_versions)?;

    // Generate Display impl
    generate_display_impl(file, struct_name)?;

    // Generate manual Eq/Hash for structs with f64 fields
    generate_manual_eq_hash(file, struct_name, struct_spec.fields())?;

    Ok(())
}

/// Check if any field in this struct spec contains Float64 type.
fn has_float64_field(fields: &[FieldSpec]) -> bool {
    fields.iter().any(|f| contains_float64(f.field_type()))
}

/// Recursively check if a field type contains Float64.
fn contains_float64(field_type: &FieldType) -> bool {
    match field_type {
        FieldType::Float64 => true,
        FieldType::Array(element_type) => contains_float64(element_type),
        _ => false,
    }
}

/// Generate the derive macro and optional manual Hash/Eq impls for a struct.
fn generate_struct_derives_and_impls(
    file: &mut fs::File,
    _struct_name: &str,
    fields: &[FieldSpec],
) -> Result<(), Box<dyn std::error::Error>> {
    if has_float64_field(fields) {
        writeln!(file, "#[derive(Debug, Clone, PartialEq)]")?;
    } else {
        writeln!(file, "#[derive(Debug, Clone, PartialEq, Eq, Hash)]")?;
    }
    Ok(())
}

/// Generate manual Eq and Hash impls for structs with f64 fields.
fn generate_manual_eq_hash(
    file: &mut fs::File,
    struct_name: &str,
    fields: &[FieldSpec],
) -> Result<(), Box<dyn std::error::Error>> {
    if !has_float64_field(fields) {
        return Ok(());
    }

    // Manual Eq impl
    writeln!(file, "impl Eq for {} {{}}", struct_name)?;
    writeln!(file)?;

    // Manual Hash impl
    writeln!(file, "impl Hash for {} {{", struct_name)?;
    writeln!(file, "    fn hash<H: Hasher>(&self, state: &mut H) {{")?;
    for field in fields {
        let field_name = to_snake_case(field.name());
        let field_name = escape_rust_keyword(&field_name);
        if matches!(field.field_type(), FieldType::Float64) {
            writeln!(file, "        self.{}.to_bits().hash(state);", field_name)?;
        } else if matches!(field.field_type(), FieldType::Array(e) if matches!(e.as_ref(), FieldType::Float64)) {
            writeln!(
                file,
                "        for elem in &self.{} {{ elem.to_bits().hash(state); }}",
                field_name
            )?;
        } else {
            writeln!(file, "        self.{}.hash(state);", field_name)?;
        }
    }
    writeln!(file, "        self.unknown_tagged_fields.hash(state);")?;
    writeln!(file, "    }}")?;
    writeln!(file, "}}")?;
    writeln!(file)?;

    Ok(())
}

fn generate_builder_setters(file: &mut fs::File, struct_spec: &StructSpec) -> Result<(), Box<dyn std::error::Error>> {
    writeln!(file)?;
    for field in struct_spec.fields() {
        let field_name = to_snake_case(field.name());
        let field_name = escape_rust_keyword(&field_name);
        let rust_type = field_type_to_rust_for_field(field);
        // Strip r# prefix for setter function names (r#type -> set_type)
        let setter_name = field_name.strip_prefix("r#").unwrap_or(&field_name);

        writeln!(
            file,
            "    pub fn set_{}(&mut self, val: {}) -> &mut Self {{",
            setter_name, rust_type
        )?;
        writeln!(file, "        self.{} = val;", field_name)?;
        writeln!(file, "        self")?;
        writeln!(file, "    }}")?;
        writeln!(file)?;
    }

    // Accessor for unknown_tagged_fields
    writeln!(
        file,
        "    pub fn unknown_tagged_fields_mut(&mut self) -> &mut Vec<RawTaggedField> {{"
    )?;
    writeln!(file, "        &mut self.unknown_tagged_fields")?;
    writeln!(file, "    }}")?;

    Ok(())
}

fn generate_message_impl(
    file: &mut fs::File,
    struct_name: &str,
    struct_spec: &StructSpec,
    flexible_versions: Versions,
) -> Result<(), Box<dyn std::error::Error>> {
    let lowest = struct_spec.versions().lowest();
    let highest = struct_spec.versions().highest();

    writeln!(file, "impl Message for {} {{", struct_name)?;
    writeln!(file, "    fn lowest_supported_version(&self) -> i16 {{ {} }}", lowest)?;
    writeln!(file, "    fn highest_supported_version(&self) -> i16 {{ {} }}", highest)?;
    writeln!(file)?;

    // Generate proper add_size that computes sizes arithmetically
    generate_add_size_body(file, struct_spec, flexible_versions)?;

    writeln!(file)?;
    writeln!(
        file,
        "    fn write(&self, writable: &mut dyn Writable, _cache: &ObjectSerializationCache, version: i16) -> std::io::Result<()> {{"
    )?;
    writeln!(file, "        {}::write(self, writable, version)", struct_name)?;
    writeln!(file, "    }}")?;
    writeln!(file)?;
    writeln!(
        file,
        "    fn read(&mut self, readable: &mut dyn Readable, version: i16) -> std::io::Result<()> {{"
    )?;
    writeln!(file, "        *self = {}::read(readable, version)?;", struct_name)?;
    writeln!(file, "        Ok(())")?;
    writeln!(file, "    }}")?;
    writeln!(file)?;
    writeln!(file, "    fn unknown_tagged_fields(&self) -> &[RawTaggedField] {{")?;
    writeln!(file, "        &self.unknown_tagged_fields")?;
    writeln!(file, "    }}")?;
    writeln!(file, "}}")?;
    writeln!(file)?;

    Ok(())
}

/// Generate the body of the `add_size` method that calculates serialized size arithmetically.
///
/// This mirrors Java's generated `addSize()` method which:
/// 1. Adds fixed sizes for primitive fields
/// 2. Caches UTF-8 byte lengths for strings in ObjectSerializationCache
/// 3. Handles flexible vs non-flexible version differences (varint vs fixed-length prefixes)
/// 4. Tracks tagged field count and adds tag/size overhead
/// 5. Adds sizes for unknown tagged fields
fn generate_add_size_body(
    file: &mut fs::File,
    struct_spec: &StructSpec,
    flexible_versions: Versions,
) -> Result<(), Box<dyn std::error::Error>> {
    writeln!(
        file,
        "    fn add_size(&self, size: &mut MessageSizeAccumulator, cache: &mut ObjectSerializationCache, version: i16) -> std::io::Result<()> {{"
    )?;
    // Suppress unused variable warnings — these may or may not be used depending on fields
    writeln!(file, "        let _ = cache;")?;
    writeln!(file, "        let _ = version;")?;

    // Check if we have any tagged fields
    let tagged_fields: Vec<&FieldSpec> = struct_spec.fields().iter().filter(|f| !f.tagged_versions().empty()).collect();
    let has_flexible = !flexible_versions.empty();

    if has_flexible {
        writeln!(file, "        let mut num_tagged_fields: u32 = 0;")?;
    }

    // Non-tagged fields
    for field in struct_spec.fields() {
        if field.tagged_versions().empty() {
            generate_field_add_size(file, field, flexible_versions)?;
        }
    }

    // Tagged fields — only present in flexible versions
    if has_flexible && !tagged_fields.is_empty() {
        writeln!(file)?;
        let needs_version_check = flexible_versions.lowest() > 0;
        if needs_version_check {
            if flexible_versions.highest() == i16::MAX {
                writeln!(file, "        if version >= {} {{", flexible_versions.lowest())?;
            } else {
                writeln!(
                    file,
                    "        if version >= {} && version <= {} {{",
                    flexible_versions.lowest(),
                    flexible_versions.highest()
                )?;
            }
        }
        for field in &tagged_fields {
            generate_tagged_field_add_size(file, field, flexible_versions, needs_version_check)?;
        }
        if needs_version_check {
            writeln!(file, "        }}")?;
        }
    }

    // Unknown tagged fields and tagged field count
    if has_flexible {
        writeln!(file)?;
        // Wrap in version check if not all versions are flexible
        let needs_version_check = flexible_versions.lowest() > 0;
        if needs_version_check {
            if flexible_versions.highest() == i16::MAX {
                writeln!(file, "        if version >= {} {{", flexible_versions.lowest())?;
            } else {
                writeln!(
                    file,
                    "        if version >= {} && version <= {} {{",
                    flexible_versions.lowest(),
                    flexible_versions.highest()
                )?;
            }
            let indent = "            ";
            writeln!(file, "{}num_tagged_fields += self.unknown_tagged_fields.len() as u32;", indent)?;
            writeln!(file, "{}for field in &self.unknown_tagged_fields {{", indent)?;
            writeln!(
                file,
                "{}    size.add_bytes(crate::common::protocol::varint::size_of_unsigned_varint(field.tag()));",
                indent
            )?;
            writeln!(
                file,
                "{}    size.add_bytes(crate::common::protocol::varint::size_of_unsigned_varint(field.size() as u32));",
                indent
            )?;
            writeln!(file, "{}    size.add_bytes(field.size() as i32);", indent)?;
            writeln!(file, "{}}}", indent)?;
            writeln!(
                file,
                "{}size.add_bytes(crate::common::protocol::varint::size_of_unsigned_varint(num_tagged_fields));",
                indent
            )?;
            writeln!(file, "        }}")?;
        } else {
            let indent = "        ";
            writeln!(file, "{}num_tagged_fields += self.unknown_tagged_fields.len() as u32;", indent)?;
            writeln!(file, "{}for field in &self.unknown_tagged_fields {{", indent)?;
            writeln!(
                file,
                "{}    size.add_bytes(crate::common::protocol::varint::size_of_unsigned_varint(field.tag()));",
                indent
            )?;
            writeln!(
                file,
                "{}    size.add_bytes(crate::common::protocol::varint::size_of_unsigned_varint(field.size() as u32));",
                indent
            )?;
            writeln!(file, "{}    size.add_bytes(field.size() as i32);", indent)?;
            writeln!(file, "{}}}", indent)?;
            writeln!(
                file,
                "{}size.add_bytes(crate::common::protocol::varint::size_of_unsigned_varint(num_tagged_fields));",
                indent
            )?;
        }
    }

    writeln!(file, "        Ok(())")?;
    writeln!(file, "    }}")?;

    Ok(())
}

/// Generate size calculation for a single non-tagged field.
fn generate_field_add_size(
    file: &mut fs::File,
    field: &FieldSpec,
    flexible_versions: Versions,
) -> Result<(), Box<dyn std::error::Error>> {
    let field_name = to_snake_case(field.name());
    let field_name = escape_rust_keyword(&field_name);
    let versions = field.versions();
    let nullable = is_nullable_field(field);

    let has_version_check = versions != Versions::ALL;
    let indent = if has_version_check { "            " } else { "        " };

    if has_version_check {
        if versions.highest() == i16::MAX {
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

    // For nullable fields, wrap in if let Some/None
    let (inner_indent, accessor) = if nullable {
        writeln!(file, "{}if let Some(ref _nv) = self.{} {{", indent, field_name)?;
        let extra = format!("{}    ", indent);
        (extra, "_nv".to_string())
    } else {
        (indent.to_string(), format!("self.{}", field_name))
    };
    let ind = &inner_indent;

    match field.field_type() {
        FieldType::Bool | FieldType::Int8 => {
            writeln!(file, "{}size.add_bytes(1);", ind)?;
        },
        FieldType::Int16 | FieldType::Uint16 => {
            writeln!(file, "{}size.add_bytes(2);", ind)?;
        },
        FieldType::Int32 | FieldType::Uint32 | FieldType::Float64 => {
            let sz = match field.field_type() {
                FieldType::Int32 | FieldType::Uint32 => 4,
                FieldType::Float64 => 8,
                _ => unreachable!(),
            };
            writeln!(file, "{}size.add_bytes({});", ind, sz)?;
        },
        FieldType::Int64 => {
            writeln!(file, "{}size.add_bytes(8);", ind)?;
        },
        FieldType::Uuid => {
            writeln!(file, "{}size.add_bytes(16);", ind)?;
        },
        FieldType::String => {
            generate_string_add_size(file, &accessor, flexible_versions, ind, false)?;
        },
        FieldType::Bytes | FieldType::Records => {
            generate_bytes_add_size(file, &accessor, flexible_versions, ind)?;
        },
        FieldType::Array(element_type) => {
            generate_array_add_size(file, &accessor, element_type, flexible_versions, ind)?;
        },
        FieldType::Struct(_) => {
            writeln!(file, "{}{}.add_size(size, cache, version)?;", ind, accessor)?;
        },
    }

    // Close nullable wrapper with null marker size in else branch
    if nullable {
        writeln!(file, "{}}} else {{", indent)?;
        generate_null_add_size(file, field.field_type(), flexible_versions, indent)?;
        writeln!(file, "{}}}", indent)?;
    }

    if has_version_check {
        writeln!(file, "        }}")?;
    }

    Ok(())
}

/// Generate null marker size for a nullable field.
fn generate_null_add_size(
    file: &mut fs::File,
    field_type: &FieldType,
    flexible_versions: Versions,
    indent: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let inner = format!("{}    ", indent);
    // Null marker: varint(0) for flexible (1 byte), or -1 for standard (i16=2 bytes for String, i32=4 bytes for Bytes/Array)
    match field_type {
        FieldType::String => {
            if !flexible_versions.empty() {
                if flexible_versions.lowest() == 0 {
                    writeln!(file, "{}size.add_bytes(1); // null varint(0)", inner)?;
                } else {
                    if flexible_versions.highest() == i16::MAX {
                        writeln!(file, "{}if version >= {} {{", inner, flexible_versions.lowest())?;
                    } else {
                        writeln!(
                            file,
                            "{}if version >= {} && version <= {} {{",
                            inner,
                            flexible_versions.lowest(),
                            flexible_versions.highest()
                        )?;
                    }
                    writeln!(file, "{}    size.add_bytes(1); // null varint(0)", inner)?;
                    writeln!(file, "{}}} else {{", inner)?;
                    writeln!(file, "{}    size.add_bytes(2); // null i16(-1)", inner)?;
                    writeln!(file, "{}}}", inner)?;
                }
            } else {
                writeln!(file, "{}size.add_bytes(2); // null i16(-1)", inner)?;
            }
        },
        FieldType::Bytes | FieldType::Records | FieldType::Array(_) => {
            if !flexible_versions.empty() {
                if flexible_versions.lowest() == 0 {
                    writeln!(file, "{}size.add_bytes(1); // null varint(0)", inner)?;
                } else {
                    if flexible_versions.highest() == i16::MAX {
                        writeln!(file, "{}if version >= {} {{", inner, flexible_versions.lowest())?;
                    } else {
                        writeln!(
                            file,
                            "{}if version >= {} && version <= {} {{",
                            inner,
                            flexible_versions.lowest(),
                            flexible_versions.highest()
                        )?;
                    }
                    writeln!(file, "{}    size.add_bytes(1); // null varint(0)", inner)?;
                    writeln!(file, "{}}} else {{", inner)?;
                    writeln!(file, "{}    size.add_bytes(4); // null i32(-1)", inner)?;
                    writeln!(file, "{}}}", inner)?;
                }
            } else {
                writeln!(file, "{}size.add_bytes(4); // null i32(-1)", inner)?;
            }
        },
        _ => {},
    }
    Ok(())
}

/// Generate size calculation for a string field.
fn generate_string_add_size(
    file: &mut fs::File,
    accessor: &str,
    flexible_versions: Versions,
    indent: &str,
    _is_tagged: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    writeln!(file, "{}{{", indent)?;
    writeln!(file, "{}    let bytes_len = {}.len() as u32;", indent, accessor)?;
    if !flexible_versions.empty() {
        if flexible_versions.lowest() == 0 {
            writeln!(
                file,
                "{}    size.add_bytes(crate::common::protocol::varint::size_of_unsigned_varint(bytes_len + 1));",
                indent
            )?;
        } else {
            if flexible_versions.highest() == i16::MAX {
                writeln!(file, "{}    if version >= {} {{", indent, flexible_versions.lowest())?;
            } else {
                writeln!(
                    file,
                    "{}    if version >= {} && version <= {} {{",
                    indent,
                    flexible_versions.lowest(),
                    flexible_versions.highest()
                )?;
            }
            writeln!(
                file,
                "{}        size.add_bytes(crate::common::protocol::varint::size_of_unsigned_varint(bytes_len + 1));",
                indent
            )?;
            writeln!(file, "{}    }} else {{", indent)?;
            writeln!(file, "{}        size.add_bytes(2); // i16 length prefix", indent)?;
            writeln!(file, "{}    }}", indent)?;
        }
    } else {
        writeln!(file, "{}    size.add_bytes(2); // i16 length prefix", indent)?;
    }
    writeln!(file, "{}    size.add_bytes(bytes_len as i32);", indent)?;
    writeln!(file, "{}}}", indent)?;
    Ok(())
}

/// Generate size calculation for a bytes/records field.
fn generate_bytes_add_size(
    file: &mut fs::File,
    accessor: &str,
    flexible_versions: Versions,
    indent: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    writeln!(file, "{}{{", indent)?;
    writeln!(file, "{}    let bytes_len = {}.len() as u32;", indent, accessor)?;
    if !flexible_versions.empty() {
        if flexible_versions.lowest() == 0 {
            writeln!(
                file,
                "{}    size.add_bytes(crate::common::protocol::varint::size_of_unsigned_varint(bytes_len + 1));",
                indent
            )?;
        } else {
            if flexible_versions.highest() == i16::MAX {
                writeln!(file, "{}    if version >= {} {{", indent, flexible_versions.lowest())?;
            } else {
                writeln!(
                    file,
                    "{}    if version >= {} && version <= {} {{",
                    indent,
                    flexible_versions.lowest(),
                    flexible_versions.highest()
                )?;
            }
            writeln!(
                file,
                "{}        size.add_bytes(crate::common::protocol::varint::size_of_unsigned_varint(bytes_len + 1));",
                indent
            )?;
            writeln!(file, "{}    }} else {{", indent)?;
            writeln!(file, "{}        size.add_bytes(4); // i32 length prefix", indent)?;
            writeln!(file, "{}    }}", indent)?;
        }
    } else {
        writeln!(file, "{}    size.add_bytes(4); // i32 length prefix", indent)?;
    }
    writeln!(file, "{}    size.add_bytes(bytes_len as i32);", indent)?;
    writeln!(file, "{}}}", indent)?;
    Ok(())
}

/// Generate size calculation for an array field.
fn generate_array_add_size(
    file: &mut fs::File,
    accessor: &str,
    element_type: &FieldType,
    flexible_versions: Versions,
    indent: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    // Length prefix
    if !flexible_versions.empty() {
        if flexible_versions.lowest() == 0 {
            writeln!(
                file,
                "{}size.add_bytes(crate::common::protocol::varint::size_of_unsigned_varint({}.len() as u32 + 1));",
                indent, accessor
            )?;
        } else {
            if flexible_versions.highest() == i16::MAX {
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
            writeln!(
                file,
                "{}    size.add_bytes(crate::common::protocol::varint::size_of_unsigned_varint({}.len() as u32 + 1));",
                indent, accessor
            )?;
            writeln!(file, "{}}} else {{", indent)?;
            writeln!(file, "{}    size.add_bytes(4); // i32 length prefix", indent)?;
            writeln!(file, "{}}}", indent)?;
        }
    } else {
        writeln!(file, "{}size.add_bytes(4); // i32 length prefix", indent)?;
    }

    // Element sizes
    let fixed_element_size = fixed_size_of(element_type);
    if let Some(elem_size) = fixed_element_size {
        writeln!(file, "{}size.add_bytes({}.len() as i32 * {});", indent, accessor, elem_size)?;
    } else {
        writeln!(file, "{}for element in {}.iter() {{", indent, accessor)?;
        generate_array_element_add_size(file, element_type, flexible_versions, indent)?;
        writeln!(file, "{}}}", indent)?;
    }

    Ok(())
}

/// Returns the fixed wire size for a field type, or None if variable-length.
fn fixed_size_of(field_type: &FieldType) -> Option<i32> {
    match field_type {
        FieldType::Bool | FieldType::Int8 => Some(1),
        FieldType::Int16 | FieldType::Uint16 => Some(2),
        FieldType::Int32 | FieldType::Uint32 => Some(4),
        FieldType::Int64 | FieldType::Float64 => Some(8),
        FieldType::Uuid => Some(16),
        _ => None,
    }
}

/// Generate size calculation for a single array element.
fn generate_array_element_add_size(
    file: &mut fs::File,
    element_type: &FieldType,
    flexible_versions: Versions,
    indent: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    match element_type {
        FieldType::String => {
            writeln!(file, "{}    let bytes_len = element.len() as u32;", indent)?;
            if !flexible_versions.empty() {
                if flexible_versions.lowest() == 0 {
                    writeln!(
                        file,
                        "{}    size.add_bytes(crate::common::protocol::varint::size_of_unsigned_varint(bytes_len + 1));",
                        indent
                    )?;
                } else {
                    if flexible_versions.highest() == i16::MAX {
                        writeln!(file, "{}    if version >= {} {{", indent, flexible_versions.lowest())?;
                    } else {
                        writeln!(
                            file,
                            "{}    if version >= {} && version <= {} {{",
                            indent,
                            flexible_versions.lowest(),
                            flexible_versions.highest()
                        )?;
                    }
                    writeln!(
                        file,
                        "{}        size.add_bytes(crate::common::protocol::varint::size_of_unsigned_varint(bytes_len + 1));",
                        indent
                    )?;
                    writeln!(file, "{}    }} else {{", indent)?;
                    writeln!(file, "{}        size.add_bytes(2);", indent)?;
                    writeln!(file, "{}    }}", indent)?;
                }
            } else {
                writeln!(file, "{}    size.add_bytes(2);", indent)?;
            }
            writeln!(file, "{}    size.add_bytes(bytes_len as i32);", indent)?;
        },
        FieldType::Bytes | FieldType::Records => {
            writeln!(file, "{}    let bytes_len = element.len() as u32;", indent)?;
            if !flexible_versions.empty() {
                if flexible_versions.lowest() == 0 {
                    writeln!(
                        file,
                        "{}    size.add_bytes(crate::common::protocol::varint::size_of_unsigned_varint(bytes_len + 1));",
                        indent
                    )?;
                } else {
                    if flexible_versions.highest() == i16::MAX {
                        writeln!(file, "{}    if version >= {} {{", indent, flexible_versions.lowest())?;
                    } else {
                        writeln!(
                            file,
                            "{}    if version >= {} && version <= {} {{",
                            indent,
                            flexible_versions.lowest(),
                            flexible_versions.highest()
                        )?;
                    }
                    writeln!(
                        file,
                        "{}        size.add_bytes(crate::common::protocol::varint::size_of_unsigned_varint(bytes_len + 1));",
                        indent
                    )?;
                    writeln!(file, "{}    }} else {{", indent)?;
                    writeln!(file, "{}        size.add_bytes(4);", indent)?;
                    writeln!(file, "{}    }}", indent)?;
                }
            } else {
                writeln!(file, "{}    size.add_bytes(4);", indent)?;
            }
            writeln!(file, "{}    size.add_bytes(bytes_len as i32);", indent)?;
        },
        FieldType::Struct(_) => {
            writeln!(file, "{}    element.add_size(size, cache, version)?;", indent)?;
        },
        _ => {
            // Fixed-size elements handled by caller
        },
    }
    Ok(())
}

/// Generate size calculation for a tagged field.
fn generate_tagged_field_add_size(
    file: &mut fs::File,
    field: &FieldSpec,
    flexible_versions: Versions,
    extra_indent: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let field_name = to_snake_case(field.name());
    let field_name = escape_rust_keyword(&field_name);
    let tag = field.tag().unwrap();
    let tagged_versions = field.tagged_versions();

    let base_indent = if extra_indent { "            " } else { "        " };

    // Check if this tagged field needs its own version guard
    // (e.g., taggedVersions: "2+" when flexible versions start at "1+")
    let needs_field_version_check = tagged_versions.lowest() > flexible_versions.lowest()
        || (tagged_versions.highest() != i16::MAX && tagged_versions.highest() < flexible_versions.highest());

    let indent = if needs_field_version_check {
        if tagged_versions.highest() == i16::MAX {
            writeln!(file, "{}if version >= {} {{", base_indent, tagged_versions.lowest())?;
        } else {
            writeln!(
                file,
                "{}if version >= {} && version <= {} {{",
                base_indent,
                tagged_versions.lowest(),
                tagged_versions.highest()
            )?;
        }
        format!("{}    ", base_indent)
    } else {
        base_indent.to_string()
    };

    // For tagged fields, we need to check if the field is at its default value.
    // If not, we count it and add its size.
    let default_check = get_default_check(field, &field_name);

    writeln!(file, "{}if {} {{", indent, default_check)?;
    let inner = format!("{}    ", indent);
    writeln!(file, "{}num_tagged_fields += 1;", inner)?;
    writeln!(
        file,
        "{}size.add_bytes(crate::common::protocol::varint::size_of_unsigned_varint({}));",
        inner, tag
    )?;
    generate_tagged_field_content_size(file, field, &field_name, flexible_versions, &inner)?;
    writeln!(file, "{}}}", indent)?;

    if needs_field_version_check {
        writeln!(file, "{}}}", base_indent)?;
    }

    Ok(())
}

/// Get the default check expression for a tagged field.
///
/// Returns a condition string that is true when the field has a non-default value
/// and should be written to the wire. Tagged fields at their default value are NOT
/// written, matching Java behavior.
fn get_default_check(field: &FieldSpec, field_name: &str) -> String {
    let nullable = is_nullable_field(field);
    if nullable {
        // For nullable fields, check if the value is Some (non-null means non-default)
        return format!("self.{}.is_some()", field_name);
    }
    match field.field_type() {
        FieldType::String => format!("!self.{}.is_empty()", field_name),
        FieldType::Array(_) => format!("!self.{}.is_empty()", field_name),
        FieldType::Bytes | FieldType::Records => format!("!self.{}.is_empty()", field_name),
        FieldType::Bool => {
            let default_val = get_default_value(field.field_type(), field.field_default());
            format!("self.{} != {}", field_name, default_val)
        },
        FieldType::Int8
        | FieldType::Int16
        | FieldType::Int32
        | FieldType::Int64
        | FieldType::Uint16
        | FieldType::Uint32 => {
            let default_val = get_default_value(field.field_type(), field.field_default());
            format!("self.{} != {}", field_name, default_val)
        },
        FieldType::Float64 => {
            let default_val = get_default_value(field.field_type(), field.field_default());
            // Float comparison: use to_bits() for exact comparison like Java's Double.compare
            format!("self.{}.to_bits() != {}f64.to_bits()", field_name, default_val)
        },
        FieldType::Uuid => {
            let default_val = get_default_value(field.field_type(), field.field_default());
            format!("self.{} != {}", field_name, default_val)
        },
        FieldType::Struct(_) => {
            let default_val = get_default_value(field.field_type(), field.field_default());
            format!("self.{} != {}", field_name, default_val)
        },
    }
}

/// Generate the content size for a tagged field (the data portion, plus the size varint wrapper).
fn generate_tagged_field_content_size(
    file: &mut fs::File,
    field: &FieldSpec,
    field_name: &str,
    _flexible_versions: Versions,
    indent: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let nullable = is_nullable_field(field);
    // For nullable tagged fields, we know self.field is Some() because the caller checked.
    // We need to use the unwrapped value for accessing .len() etc.
    let accessor = if nullable {
        format!("self.{}.as_ref().unwrap()", field_name)
    } else {
        format!("self.{}", field_name)
    };

    // Tagged fields always use flexible encoding (varint).
    // We need to calculate the inner size and then add varint(inner_size) as the size prefix.
    match field.field_type() {
        FieldType::Bool | FieldType::Int8 => {
            writeln!(
                file,
                "{}size.add_bytes(crate::common::protocol::varint::size_of_unsigned_varint(1)); // size prefix",
                indent
            )?;
            writeln!(file, "{}size.add_bytes(1);", indent)?;
        },
        FieldType::Int16 | FieldType::Uint16 => {
            writeln!(
                file,
                "{}size.add_bytes(crate::common::protocol::varint::size_of_unsigned_varint(2)); // size prefix",
                indent
            )?;
            writeln!(file, "{}size.add_bytes(2);", indent)?;
        },
        FieldType::Int32 | FieldType::Uint32 => {
            writeln!(
                file,
                "{}size.add_bytes(crate::common::protocol::varint::size_of_unsigned_varint(4)); // size prefix",
                indent
            )?;
            writeln!(file, "{}size.add_bytes(4);", indent)?;
        },
        FieldType::Int64 => {
            writeln!(
                file,
                "{}size.add_bytes(crate::common::protocol::varint::size_of_unsigned_varint(8)); // size prefix",
                indent
            )?;
            writeln!(file, "{}size.add_bytes(8);", indent)?;
        },
        FieldType::Float64 => {
            writeln!(
                file,
                "{}size.add_bytes(crate::common::protocol::varint::size_of_unsigned_varint(8)); // size prefix",
                indent
            )?;
            writeln!(file, "{}size.add_bytes(8);", indent)?;
        },
        FieldType::Uuid => {
            writeln!(
                file,
                "{}size.add_bytes(crate::common::protocol::varint::size_of_unsigned_varint(16)); // size prefix",
                indent
            )?;
            writeln!(file, "{}size.add_bytes(16);", indent)?;
        },
        FieldType::String => {
            // String in tagged field: varint(inner_size) where inner_size = varint(len+1) + len
            writeln!(file, "{}{{", indent)?;
            writeln!(file, "{}    let bytes_len = {}.len() as u32;", indent, accessor)?;
            writeln!(
                file,
                "{}    let string_prefix_size = crate::common::protocol::varint::size_of_unsigned_varint(bytes_len + 1);",
                indent
            )?;
            writeln!(file, "{}    let inner_size = string_prefix_size + bytes_len as i32;", indent)?;
            writeln!(
                file,
                "{}    size.add_bytes(crate::common::protocol::varint::size_of_unsigned_varint(inner_size as u32)); // size prefix",
                indent
            )?;
            writeln!(file, "{}    size.add_bytes(inner_size);", indent)?;
            writeln!(file, "{}}}", indent)?;
        },
        FieldType::Bytes | FieldType::Records => {
            writeln!(file, "{}{{", indent)?;
            writeln!(file, "{}    let bytes_len = {}.len() as u32;", indent, accessor)?;
            writeln!(
                file,
                "{}    let bytes_prefix_size = crate::common::protocol::varint::size_of_unsigned_varint(bytes_len + 1);",
                indent
            )?;
            writeln!(file, "{}    let inner_size = bytes_prefix_size + bytes_len as i32;", indent)?;
            writeln!(
                file,
                "{}    size.add_bytes(crate::common::protocol::varint::size_of_unsigned_varint(inner_size as u32)); // size prefix",
                indent
            )?;
            writeln!(file, "{}    size.add_bytes(inner_size);", indent)?;
            writeln!(file, "{}}}", indent)?;
        },
        FieldType::Array(element_type) => {
            // For tagged arrays, we need to compute the total array serialized size
            writeln!(file, "{}{{", indent)?;
            writeln!(file, "{}    let mut array_size: i32 = 0;", indent)?;
            // Array length prefix (varint(len+1))
            writeln!(
                file,
                "{}    array_size += crate::common::protocol::varint::size_of_unsigned_varint({}.len() as u32 + 1);",
                indent, accessor
            )?;
            // Element sizes
            let fixed_elem = fixed_size_of(element_type);
            if let Some(elem_size) = fixed_elem {
                writeln!(file, "{}    array_size += {}.len() as i32 * {};", indent, accessor, elem_size)?;
            } else {
                writeln!(file, "{}    for element in &*{} {{", indent, accessor)?;
                match element_type.as_ref() {
                    FieldType::String => {
                        writeln!(file, "{}        let elem_len = element.len() as u32;", indent)?;
                        writeln!(
                            file,
                            "{}        array_size += crate::common::protocol::varint::size_of_unsigned_varint(elem_len + 1);",
                            indent
                        )?;
                        writeln!(file, "{}        array_size += elem_len as i32;", indent)?;
                    },
                    FieldType::Struct(_) => {
                        writeln!(file, "{}        let mut elem_acc = MessageSizeAccumulator::new();", indent)?;
                        writeln!(file, "{}        element.add_size(&mut elem_acc, cache, version)?;", indent)?;
                        writeln!(file, "{}        array_size += elem_acc.total_size();", indent)?;
                    },
                    _ => {},
                }
                writeln!(file, "{}    }}", indent)?;
            }
            writeln!(
                file,
                "{}    size.add_bytes(crate::common::protocol::varint::size_of_unsigned_varint(array_size as u32)); // size prefix",
                indent
            )?;
            writeln!(file, "{}    size.add_bytes(array_size);", indent)?;
            writeln!(file, "{}}}", indent)?;
        },
        FieldType::Struct(_) => {
            // For tagged structs, compute struct size in a sub-accumulator
            writeln!(file, "{}{{", indent)?;
            writeln!(file, "{}    let mut struct_acc = MessageSizeAccumulator::new();", indent)?;
            writeln!(file, "{}    {}.add_size(&mut struct_acc, cache, version)?;", indent, accessor)?;
            writeln!(file, "{}    let struct_size = struct_acc.total_size();", indent)?;
            writeln!(
                file,
                "{}    size.add_bytes(crate::common::protocol::varint::size_of_unsigned_varint(struct_size as u32)); // size prefix",
                indent
            )?;
            writeln!(file, "{}    size.add_bytes(struct_size);", indent)?;
            writeln!(file, "{}}}", indent)?;
        },
    }

    Ok(())
}

fn generate_display_impl(file: &mut fs::File, struct_name: &str) -> Result<(), Box<dyn std::error::Error>> {
    writeln!(file, "impl fmt::Display for {} {{", struct_name)?;
    writeln!(file, "    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {{")?;
    writeln!(file, "        write!(f, \"{{:?}}\", self)")?;
    writeln!(file, "    }}")?;
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

            let nullable = is_nullable_field(field);
            // Generate the read code for this tagged field
            match field.field_type() {
                FieldType::String => {
                    writeln!(
                        file,
                        "{}                    let length = readable.read_unsigned_varint()?;",
                        indent
                    )?;
                    if nullable {
                        writeln!(file, "{}                    if length == 0 {{", indent)?;
                        writeln!(file, "{}                        result.{} = None;", indent, field_name)?;
                        writeln!(file, "{}                    }} else {{", indent)?;
                        writeln!(
                            file,
                            "{}                        let mut bytes = vec![0u8; (length - 1) as usize];",
                            indent
                        )?;
                        writeln!(file, "{}                        readable.read_bytes(&mut bytes)?;", indent)?;
                        writeln!(
                            file,
                            "{}                        result.{} = Some(String::from_utf8(bytes)",
                            indent, field_name
                        )?;
                        writeln!(
                            file,
                            "{}                            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?);",
                            indent
                        )?;
                        writeln!(file, "{}                    }}", indent)?;
                    } else {
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
                    }
                },
                FieldType::Bool => {
                    let wrap = if nullable { "Some(" } else { "" };
                    let close = if nullable { ")" } else { "" };
                    writeln!(
                        file,
                        "{}                    result.{} = {}readable.read_byte()? != 0{};",
                        indent, field_name, wrap, close
                    )?;
                },
                FieldType::Int8 => {
                    let wrap = if nullable { "Some(" } else { "" };
                    let close = if nullable { ")" } else { "" };
                    writeln!(
                        file,
                        "{}                    result.{} = {}readable.read_byte()? as i8{};",
                        indent, field_name, wrap, close
                    )?;
                },
                FieldType::Int16 => {
                    let wrap = if nullable { "Some(" } else { "" };
                    let close = if nullable { ")" } else { "" };
                    writeln!(
                        file,
                        "{}                    result.{} = {}readable.read_short()?{};",
                        indent, field_name, wrap, close
                    )?;
                },
                FieldType::Int32 => {
                    let wrap = if nullable { "Some(" } else { "" };
                    let close = if nullable { ")" } else { "" };
                    writeln!(
                        file,
                        "{}                    result.{} = {}readable.read_int()?{};",
                        indent, field_name, wrap, close
                    )?;
                },
                FieldType::Int64 => {
                    let wrap = if nullable { "Some(" } else { "" };
                    let close = if nullable { ")" } else { "" };
                    writeln!(
                        file,
                        "{}                    result.{} = {}readable.read_long()?{};",
                        indent, field_name, wrap, close
                    )?;
                },
                FieldType::Uuid => {
                    let wrap = if nullable { "Some(" } else { "" };
                    let close = if nullable { ")" } else { "" };
                    writeln!(
                        file,
                        "{}                    result.{} = {}readable.read_uuid()?{};",
                        indent, field_name, wrap, close
                    )?;
                },
                FieldType::Array(element_type) => {
                    writeln!(
                        file,
                        "{}                    let length = readable.read_unsigned_varint()?;",
                        indent
                    )?;
                    if nullable {
                        writeln!(file, "{}                    if length == 0 {{", indent)?;
                        writeln!(file, "{}                        result.{} = None;", indent, field_name)?;
                        writeln!(file, "{}                    }} else {{", indent)?;
                        writeln!(file, "{}                        let length = length - 1;", indent)?;
                        writeln!(
                            file,
                            "{}                        let mut _arr = Vec::with_capacity(length as usize);",
                            indent
                        )?;
                        writeln!(file, "{}                        for _ in 0..length {{", indent)?;
                    } else {
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
                    }

                    // For nullable arrays, push to _arr; for non-nullable, push to result.field
                    let push_target = if nullable {
                        "_arr"
                    } else {
                        &format!("result.{}", field_name)
                    };

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
                                "{}                                {}.push(String::new());",
                                indent, push_target
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
                                "{}                                {}.push(String::from_utf8(bytes)",
                                indent, push_target
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
                                "{}                            {}.push({}::read(readable, version)?);",
                                indent, push_target, struct_name
                            )?;
                        },
                        FieldType::Uuid => {
                            writeln!(
                                file,
                                "{}                            {}.push(readable.read_uuid()?);",
                                indent, push_target
                            )?;
                        },
                        FieldType::Bool => {
                            writeln!(
                                file,
                                "{}                            {}.push(readable.read_byte()? != 0);",
                                indent, push_target
                            )?;
                        },
                        FieldType::Int8 => {
                            writeln!(
                                file,
                                "{}                            {}.push(readable.read_byte()? as i8);",
                                indent, push_target
                            )?;
                        },
                        FieldType::Int16 => {
                            writeln!(
                                file,
                                "{}                            {}.push(readable.read_short()?);",
                                indent, push_target
                            )?;
                        },
                        FieldType::Int32 => {
                            writeln!(
                                file,
                                "{}                            {}.push(readable.read_int()?);",
                                indent, push_target
                            )?;
                        },
                        FieldType::Int64 => {
                            writeln!(
                                file,
                                "{}                            {}.push(readable.read_long()?);",
                                indent, push_target
                            )?;
                        },
                        FieldType::Float64 => {
                            writeln!(
                                file,
                                "{}                            {}.push(readable.read_double()?);",
                                indent, push_target
                            )?;
                        },
                        _ => {
                            // Skip unknown element types
                            writeln!(
                                file,
                                "{}                            // Unsupported array element type {:?} in tagged field",
                                indent, element_type
                            )?;
                        },
                    }

                    writeln!(file, "{}                        }}", indent)?;
                    if nullable {
                        writeln!(file, "{}                        result.{} = Some(_arr);", indent, field_name)?;
                    }
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
                    if nullable {
                        writeln!(
                            file,
                            "{}                    result.{} = Some({}::read(&mut struct_accessor, version)?);",
                            indent, field_name, struct_name
                        )?;
                    } else {
                        writeln!(
                            file,
                            "{}                    result.{} = {}::read(&mut struct_accessor, version)?;",
                            indent, field_name, struct_name
                        )?;
                    }
                },
                FieldType::Float64 => {
                    let wrap = if nullable { "Some(" } else { "" };
                    let close = if nullable { ")" } else { "" };
                    writeln!(
                        file,
                        "{}                    result.{} = {}readable.read_double()?{};",
                        indent, field_name, wrap, close
                    )?;
                },
                FieldType::Bytes | FieldType::Records => {
                    writeln!(
                        file,
                        "{}                    let len = readable.read_unsigned_varint()?;",
                        indent
                    )?;
                    if nullable {
                        writeln!(file, "{}                    if len == 0 {{", indent)?;
                        writeln!(file, "{}                        result.{} = None;", indent, field_name)?;
                        writeln!(file, "{}                    }} else {{", indent)?;
                        writeln!(
                            file,
                            "{}                        let mut bytes = vec![0u8; (len - 1) as usize];",
                            indent
                        )?;
                        writeln!(file, "{}                        readable.read_bytes(&mut bytes)?;", indent)?;
                        writeln!(file, "{}                        result.{} = Some(bytes);", indent, field_name)?;
                        writeln!(file, "{}                    }}", indent)?;
                    } else {
                        writeln!(file, "{}                    if len > 0 {{", indent)?;
                        writeln!(
                            file,
                            "{}                        let mut bytes = vec![0u8; (len - 1) as usize];",
                            indent
                        )?;
                        writeln!(file, "{}                        readable.read_bytes(&mut bytes)?;", indent)?;
                        writeln!(file, "{}                        result.{} = bytes;", indent, field_name)?;
                        writeln!(file, "{}                    }} else {{", indent)?;
                        writeln!(file, "{}                        result.{} = Vec::new();", indent, field_name)?;
                        writeln!(file, "{}                    }}", indent)?;
                    }
                },
                _ => {
                    // Skip unknown/unhandled types
                    writeln!(
                        file,
                        "{}                    let mut skip_bytes = vec![0u8; size as usize];",
                        indent
                    )?;
                    writeln!(file, "{}                    readable.read_bytes(&mut skip_bytes)?;", indent)?;
                },
            }

            writeln!(file, "{}                }}", indent)?;
        }
    }

    writeln!(file, "{}                _ => {{", indent)?;
    writeln!(
        file,
        "{}                    // Unknown tagged field, store for forward compatibility",
        indent
    )?;
    writeln!(
        file,
        "{}                    let data = readable.read_array(size as usize)?;",
        indent
    )?;
    writeln!(
        file,
        "{}                    result.unknown_tagged_fields.push(RawTaggedField::new(tag, data));",
        indent
    )?;
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
            if tagged_versions.highest() == i16::MAX {
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

            // Check if field has non-default value
            let default_check = get_default_check(field, &field_name);
            writeln!(file, "{}            if {} {{", indent, default_check)?;
            writeln!(file, "{}                num_tagged_fields += 1;", indent)?;
            writeln!(file, "{}            }}", indent)?;

            writeln!(file, "{}        }}", indent)?;
        }
    }

    writeln!(
        file,
        "{}        num_tagged_fields += self.unknown_tagged_fields.len() as u32;",
        indent
    )?;
    writeln!(file, "{}        writable.write_unsigned_varint(num_tagged_fields)?;", indent)?;

    // Now write each tagged field
    for field in tagged_fields {
        if let Some(tag) = field.tag() {
            let field_name = to_snake_case(field.name());
            let field_name = escape_rust_keyword(&field_name);
            let tagged_versions = field.tagged_versions();

            if !tagged_versions.empty() {
                if tagged_versions.highest() == i16::MAX {
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

                // Check if we should write this field (only if non-default value)
                let should_write = get_default_check(field, &field_name);
                let nullable = is_nullable_field(field);

                // For nullable tagged fields, accessor is the unwrapped ref value
                // For non-nullable, accessor is the field directly
                let tagged_accessor = if nullable {
                    format!("self.{}.as_ref().unwrap()", field_name)
                } else {
                    format!("self.{}", field_name)
                };
                // For Copy types (bool, int, float), nullable needs * deref, non-nullable doesn't
                let tagged_copy_accessor = if nullable {
                    format!("*self.{}.as_ref().unwrap()", field_name)
                } else {
                    format!("self.{}", field_name)
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
                        writeln!(file, "{}                let bytes = {}.as_bytes();", indent, tagged_accessor)?;
                        writeln!(
                            file,
                            "{}                let string_prefix_size = crate::common::protocol::varint::size_of_unsigned_varint((bytes.len() as u32) + 1);",
                            indent
                        )?;
                        writeln!(
                            file,
                            "{}                let size = (string_prefix_size + bytes.len() as i32) as u32;",
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
                            "{}                writable.write_byte(if {} {{ 1 }} else {{ 0 }})?;",
                            indent, tagged_copy_accessor
                        )?;
                    },
                    FieldType::Int8 => {
                        writeln!(
                            file,
                            "{}                writable.write_unsigned_varint(1)?; // size = 1 byte",
                            indent
                        )?;
                        writeln!(
                            file,
                            "{}                writable.write_byte({})?;",
                            indent, tagged_copy_accessor
                        )?;
                    },
                    FieldType::Int16 => {
                        writeln!(
                            file,
                            "{}                writable.write_unsigned_varint(2)?; // size = 2 bytes",
                            indent
                        )?;
                        writeln!(
                            file,
                            "{}                writable.write_short({})?;",
                            indent, tagged_copy_accessor
                        )?;
                    },
                    FieldType::Int32 => {
                        writeln!(
                            file,
                            "{}                writable.write_unsigned_varint(4)?; // size = 4 bytes",
                            indent
                        )?;
                        writeln!(file, "{}                writable.write_int({})?;", indent, tagged_copy_accessor)?;
                    },
                    FieldType::Int64 => {
                        writeln!(
                            file,
                            "{}                writable.write_unsigned_varint(8)?; // size = 8 bytes",
                            indent
                        )?;
                        writeln!(
                            file,
                            "{}                writable.write_long({})?;",
                            indent, tagged_copy_accessor
                        )?;
                    },
                    FieldType::Uuid => {
                        writeln!(
                            file,
                            "{}                writable.write_unsigned_varint(16)?; // size = 16 bytes",
                            indent
                        )?;
                        writeln!(file, "{}                writable.write_uuid(&{})?;", indent, tagged_accessor)?;
                    },
                    FieldType::Array(element_type) => {
                        // For arrays, we need to calculate the size first by writing to a temp buffer
                        writeln!(file, "{}                // Calculate array size", indent)?;
                        writeln!(
                            file,
                            "{}                let mut size_accessor = crate::common::protocol::ByteBufferAccessor::new(1024);",
                            indent
                        )?;
                        writeln!(file, "{}                // Write array length", indent)?;
                        writeln!(
                            file,
                            "{}                size_accessor.write_unsigned_varint(({}.len() as u32) + 1)?;",
                            indent, tagged_accessor
                        )?;
                        writeln!(file, "{}                // Write array elements", indent)?;

                        // Different handling based on element type
                        match element_type.as_ref() {
                            FieldType::Uuid => {
                                writeln!(
                                    file,
                                    "{}                for element in {}.iter() {{",
                                    indent, tagged_accessor
                                )?;
                                writeln!(file, "{}                    size_accessor.write_uuid(element)?;", indent)?;
                                writeln!(file, "{}                }}", indent)?;
                            },
                            FieldType::Int8 => {
                                writeln!(
                                    file,
                                    "{}                for element in {}.iter() {{",
                                    indent, tagged_accessor
                                )?;
                                writeln!(file, "{}                    size_accessor.write_byte(*element)?;", indent)?;
                                writeln!(file, "{}                }}", indent)?;
                            },
                            FieldType::Int16 => {
                                writeln!(
                                    file,
                                    "{}                for element in {}.iter() {{",
                                    indent, tagged_accessor
                                )?;
                                writeln!(file, "{}                    size_accessor.write_short(*element)?;", indent)?;
                                writeln!(file, "{}                }}", indent)?;
                            },
                            FieldType::Int32 => {
                                writeln!(
                                    file,
                                    "{}                for element in {}.iter() {{",
                                    indent, tagged_accessor
                                )?;
                                writeln!(file, "{}                    size_accessor.write_int(*element)?;", indent)?;
                                writeln!(file, "{}                }}", indent)?;
                            },
                            FieldType::Int64 => {
                                writeln!(
                                    file,
                                    "{}                for element in {}.iter() {{",
                                    indent, tagged_accessor
                                )?;
                                writeln!(file, "{}                    size_accessor.write_long(*element)?;", indent)?;
                                writeln!(file, "{}                }}", indent)?;
                            },
                            FieldType::String => {
                                writeln!(
                                    file,
                                    "{}                for element in {}.iter() {{",
                                    indent, tagged_accessor
                                )?;
                                writeln!(file, "{}                    let bytes = element.as_bytes();", indent)?;
                                writeln!(
                                    file,
                                    "{}                    size_accessor.write_unsigned_varint((bytes.len() as u32) + 1)?;",
                                    indent
                                )?;
                                writeln!(file, "{}                    size_accessor.write_bytes(bytes)?;", indent)?;
                                writeln!(file, "{}                }}", indent)?;
                            },
                            _ => {
                                // For structs and other complex types, assume they have a write method
                                writeln!(
                                    file,
                                    "{}                for element in {}.iter() {{",
                                    indent, tagged_accessor
                                )?;
                                writeln!(
                                    file,
                                    "{}                    element.write(&mut size_accessor, version)?;",
                                    indent
                                )?;
                                writeln!(file, "{}                }}", indent)?;
                            },
                        }

                        writeln!(file, "{}                let size = size_accessor.len() as u32;", indent)?;
                        writeln!(file, "{}                writable.write_unsigned_varint(size)?;", indent)?;
                        writeln!(file, "{}                writable.write_bytes(size_accessor.buffer())?;", indent)?;
                    },
                    FieldType::Float64 => {
                        writeln!(
                            file,
                            "{}                writable.write_unsigned_varint(8)?; // size = 8 bytes",
                            indent
                        )?;
                        writeln!(
                            file,
                            "{}                writable.write_double({})?;",
                            indent, tagged_copy_accessor
                        )?;
                    },
                    FieldType::Bytes | FieldType::Records => {
                        writeln!(file, "{}                // Calculate bytes size", indent)?;
                        writeln!(
                            file,
                            "{}                let mut size_accessor = crate::common::protocol::ByteBufferAccessor::new(256);",
                            indent
                        )?;
                        writeln!(
                            file,
                            "{}                size_accessor.write_unsigned_varint(({}.len() as u32) + 1)?;",
                            indent, tagged_accessor
                        )?;
                        writeln!(
                            file,
                            "{}                size_accessor.write_bytes(&*{})?;",
                            indent, tagged_accessor
                        )?;
                        writeln!(file, "{}                let size = size_accessor.len() as u32;", indent)?;
                        writeln!(file, "{}                writable.write_unsigned_varint(size)?;", indent)?;
                        writeln!(file, "{}                writable.write_bytes(size_accessor.buffer())?;", indent)?;
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
                            "{}                {}.write(&mut size_accessor, version)?;",
                            indent, tagged_accessor
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

    // Write unknown tagged fields
    writeln!(file, "{}        // Write unknown tagged fields", indent)?;
    writeln!(file, "{}        for field in &self.unknown_tagged_fields {{", indent)?;
    writeln!(file, "{}            writable.write_unsigned_varint(field.tag())?;", indent)?;
    writeln!(
        file,
        "{}            writable.write_unsigned_varint(field.size() as u32)?;",
        indent
    )?;
    writeln!(file, "{}            writable.write_bytes(field.data())?;", indent)?;
    writeln!(file, "{}        }}", indent)?;

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
    if highest == i16::MAX {
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
            if flexible_versions.highest() == i16::MAX {
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
        // No tagged fields defined, store unknown ones for forward compatibility
        writeln!(file)?;
        if flexible_versions.lowest() == 0 {
            writeln!(file, "        // Read tagged fields (flexible version)")?;
            writeln!(file, "        let num_tagged_fields = readable.read_unsigned_varint()?;")?;
            writeln!(file, "        for _ in 0..num_tagged_fields {{")?;
            writeln!(file, "            let tag = readable.read_unsigned_varint()?;")?;
            writeln!(file, "            let size = readable.read_unsigned_varint()?;")?;
            writeln!(file, "            let data = readable.read_array(size as usize)?;")?;
            writeln!(
                file,
                "            result.unknown_tagged_fields.push(RawTaggedField::new(tag, data));"
            )?;
            writeln!(file, "        }}")?;
        } else {
            if flexible_versions.highest() == i16::MAX {
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
            writeln!(file, "                let tag = readable.read_unsigned_varint()?;")?;
            writeln!(file, "                let size = readable.read_unsigned_varint()?;")?;
            writeln!(file, "                let data = readable.read_array(size as usize)?;")?;
            writeln!(
                file,
                "                result.unknown_tagged_fields.push(RawTaggedField::new(tag, data));"
            )?;
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
    if highest == i16::MAX {
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
            if flexible_versions.highest() == i16::MAX {
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
            writeln!(file, "        }} else if !self.unknown_tagged_fields.is_empty() {{")?;
            writeln!(file, "            return Err(std::io::Error::new(")?;
            writeln!(file, "                std::io::ErrorKind::InvalidData,")?;
            writeln!(
                file,
                "                format!(\"Tagged fields were set, but version {{}} of this message does not support them.\", version),"
            )?;
            writeln!(file, "            ));")?;
            writeln!(file, "        }}")?;
        }
    } else if !flexible_versions.empty() {
        // No known tagged fields, but write stored unknown tagged fields
        writeln!(file)?;
        if flexible_versions.lowest() == 0 {
            writeln!(file, "        // Write tagged fields (flexible version)")?;
            writeln!(
                file,
                "        writable.write_unsigned_varint(self.unknown_tagged_fields.len() as u32)?;"
            )?;
            writeln!(file, "        for field in &self.unknown_tagged_fields {{")?;
            writeln!(file, "            writable.write_unsigned_varint(field.tag())?;")?;
            writeln!(file, "            writable.write_unsigned_varint(field.size() as u32)?;")?;
            writeln!(file, "            writable.write_bytes(field.data())?;")?;
            writeln!(file, "        }}")?;
        } else {
            if flexible_versions.highest() == i16::MAX {
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
            writeln!(
                file,
                "            writable.write_unsigned_varint(self.unknown_tagged_fields.len() as u32)?;"
            )?;
            writeln!(file, "            for field in &self.unknown_tagged_fields {{")?;
            writeln!(file, "                writable.write_unsigned_varint(field.tag())?;")?;
            writeln!(file, "                writable.write_unsigned_varint(field.size() as u32)?;")?;
            writeln!(file, "                writable.write_bytes(field.data())?;")?;
            writeln!(file, "            }}")?;
            writeln!(file, "        }} else if !self.unknown_tagged_fields.is_empty() {{")?;
            writeln!(file, "            return Err(std::io::Error::new(")?;
            writeln!(file, "                std::io::ErrorKind::InvalidData,")?;
            writeln!(
                file,
                "                format!(\"Tagged fields were set, but version {{}} of this message does not support them.\", version),"
            )?;
            writeln!(file, "            ));")?;
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
    let nullable = is_nullable_field(field);

    // Determine indentation based on whether we have a version check
    let has_version_check = versions != Versions::ALL;
    let indent = if has_version_check { "            " } else { "        " };

    // Version check - avoid useless comparison when highest is i16::MAX
    if has_version_check {
        if versions.highest() == i16::MAX {
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
            generate_string_read(file, &field_name, flexible_versions, indent, nullable)?;
        },
        FieldType::Bytes | FieldType::Records => {
            generate_bytes_read(file, &field_name, flexible_versions, indent, nullable)?;
        },
        FieldType::Array(element_type) => {
            generate_array_read(file, &field_name, element_type, flexible_versions, indent, nullable)?;
        },
        FieldType::Struct(struct_name) => {
            if nullable {
                writeln!(
                    file,
                    "{}result.{} = Some({}::read(readable, version)?);",
                    indent, field_name, struct_name
                )?;
            } else {
                writeln!(
                    file,
                    "{}result.{} = {}::read(readable, version)?;",
                    indent, field_name, struct_name
                )?;
            }
        },
    }

    if versions != Versions::ALL {
        writeln!(file, "        }}")?;
    }
    writeln!(file)?;

    Ok(())
}

/// Generate string field read code, handling nullable fields.
fn generate_string_read(
    file: &mut fs::File,
    field_name: &str,
    flexible_versions: Versions,
    indent: &str,
    nullable: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let some_wrap = if nullable { "Some(" } else { "" };
    let some_close = if nullable { ")" } else { "" };
    let null_action = if nullable {
        format!("result.{} = None;", field_name)
    } else {
        "return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, \"Null string not allowed\"));".to_string()
    };
    let neg_action = if nullable {
        format!("result.{} = None;", field_name)
    } else {
        "return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, \"Negative string length\"));".to_string()
    };

    if !flexible_versions.empty() {
        if flexible_versions.lowest() == 0 {
            // All versions are flexible
            writeln!(file, "{}let len = readable.read_unsigned_varint()?;", indent)?;
            writeln!(file, "{}if len == 0 {{", indent)?;
            writeln!(file, "{}    {}", indent, null_action)?;
            writeln!(file, "{}}} else {{", indent)?;
            writeln!(file, "{}    let length = len - 1;", indent)?;
            writeln!(file, "{}    let mut bytes = vec![0u8; length as usize];", indent)?;
            writeln!(file, "{}    readable.read_bytes(&mut bytes)?;", indent)?;
            writeln!(
                file,
                "{}    result.{} = {}String::from_utf8(bytes)",
                indent, field_name, some_wrap
            )?;
            writeln!(
                file,
                "{}        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?{};",
                indent, some_close
            )?;
            writeln!(file, "{}}}", indent)?;
        } else {
            // Mixed flexible/standard
            if flexible_versions.highest() == i16::MAX {
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
            writeln!(file, "{}    let len = readable.read_unsigned_varint()?;", indent)?;
            writeln!(file, "{}    if len == 0 {{", indent)?;
            writeln!(file, "{}        {}", indent, null_action)?;
            writeln!(file, "{}    }} else {{", indent)?;
            writeln!(file, "{}        let length = len - 1;", indent)?;
            writeln!(file, "{}        let mut bytes = vec![0u8; length as usize];", indent)?;
            writeln!(file, "{}        readable.read_bytes(&mut bytes)?;", indent)?;
            writeln!(
                file,
                "{}        result.{} = {}String::from_utf8(bytes)",
                indent, field_name, some_wrap
            )?;
            writeln!(
                file,
                "{}            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?{};",
                indent, some_close
            )?;
            writeln!(file, "{}    }}", indent)?;
            writeln!(file, "{}}} else {{", indent)?;
            writeln!(file, "{}    let len = readable.read_short()?;", indent)?;
            writeln!(file, "{}    if len < 0 {{", indent)?;
            writeln!(file, "{}        {}", indent, neg_action)?;
            writeln!(file, "{}    }} else {{", indent)?;
            writeln!(file, "{}        let mut bytes = vec![0u8; len as usize];", indent)?;
            writeln!(file, "{}        readable.read_bytes(&mut bytes)?;", indent)?;
            writeln!(
                file,
                "{}        result.{} = {}String::from_utf8(bytes)",
                indent, field_name, some_wrap
            )?;
            writeln!(
                file,
                "{}            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?{};",
                indent, some_close
            )?;
            writeln!(file, "{}    }}", indent)?;
            writeln!(file, "{}}}", indent)?;
        }
    } else {
        // Standard only
        writeln!(file, "{}let len = readable.read_short()?;", indent)?;
        writeln!(file, "{}if len < 0 {{", indent)?;
        writeln!(file, "{}    {}", indent, neg_action)?;
        writeln!(file, "{}}} else {{", indent)?;
        writeln!(file, "{}    let mut bytes = vec![0u8; len as usize];", indent)?;
        writeln!(file, "{}    readable.read_bytes(&mut bytes)?;", indent)?;
        writeln!(
            file,
            "{}    result.{} = {}String::from_utf8(bytes)",
            indent, field_name, some_wrap
        )?;
        writeln!(
            file,
            "{}        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?{};",
            indent, some_close
        )?;
        writeln!(file, "{}}}", indent)?;
    }
    Ok(())
}

/// Generate bytes/records field read code, handling nullable fields.
fn generate_bytes_read(
    file: &mut fs::File,
    field_name: &str,
    flexible_versions: Versions,
    indent: &str,
    nullable: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let some_wrap = if nullable { "Some(" } else { "" };
    let some_close = if nullable { ")" } else { "" };

    if !flexible_versions.empty() {
        if flexible_versions.highest() == i16::MAX {
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
        writeln!(file, "{}    let len = readable.read_unsigned_varint()?;", indent)?;
        if nullable {
            writeln!(file, "{}    if len == 0 {{", indent)?;
            writeln!(file, "{}        result.{} = None;", indent, field_name)?;
            writeln!(file, "{}    }} else {{", indent)?;
            writeln!(file, "{}        let length = len - 1;", indent)?;
            writeln!(file, "{}        let mut bytes = vec![0u8; length as usize];", indent)?;
            writeln!(file, "{}        readable.read_bytes(&mut bytes)?;", indent)?;
            writeln!(file, "{}        result.{} = Some(bytes);", indent, field_name)?;
            writeln!(file, "{}    }}", indent)?;
        } else {
            writeln!(file, "{}    let length = if len == 0 {{ 0 }} else {{ len - 1 }};", indent)?;
            writeln!(file, "{}    let mut bytes = vec![0u8; length as usize];", indent)?;
            writeln!(file, "{}    readable.read_bytes(&mut bytes)?;", indent)?;
            writeln!(file, "{}    result.{} = bytes;", indent, field_name)?;
        }
        writeln!(file, "{}}} else {{", indent)?;
        writeln!(file, "{}    let len = readable.read_int()?;", indent)?;
        if nullable {
            writeln!(file, "{}    if len < 0 {{", indent)?;
            writeln!(file, "{}        result.{} = None;", indent, field_name)?;
            writeln!(file, "{}    }} else {{", indent)?;
            writeln!(file, "{}        let mut bytes = vec![0u8; len as usize];", indent)?;
            writeln!(file, "{}        readable.read_bytes(&mut bytes)?;", indent)?;
            writeln!(file, "{}        result.{} = Some(bytes);", indent, field_name)?;
            writeln!(file, "{}    }}", indent)?;
        } else {
            writeln!(file, "{}    let length = if len < 0 {{ 0 }} else {{ len as u32 }};", indent)?;
            writeln!(file, "{}    let mut bytes = vec![0u8; length as usize];", indent)?;
            writeln!(file, "{}    readable.read_bytes(&mut bytes)?;", indent)?;
            writeln!(file, "{}    result.{} = bytes;", indent, field_name)?;
        }
        writeln!(file, "{}}}", indent)?;
    } else {
        writeln!(file, "{}let len = readable.read_int()?;", indent)?;
        if nullable {
            writeln!(file, "{}if len < 0 {{", indent)?;
            writeln!(file, "{}    result.{} = None;", indent, field_name)?;
            writeln!(file, "{}}} else {{", indent)?;
            writeln!(file, "{}    let mut bytes = vec![0u8; len as usize];", indent)?;
            writeln!(file, "{}    readable.read_bytes(&mut bytes)?;", indent)?;
            writeln!(file, "{}    result.{} = Some(bytes);", indent, field_name)?;
            writeln!(file, "{}}}", indent)?;
        } else {
            writeln!(file, "{}let length = if len < 0 {{ 0 }} else {{ len as u32 }};", indent)?;
            writeln!(file, "{}let mut bytes = vec![0u8; length as usize];", indent)?;
            writeln!(file, "{}readable.read_bytes(&mut bytes)?;", indent)?;
            writeln!(file, "{}result.{} = {}bytes{};", indent, field_name, some_wrap, some_close)?;
        }
    }
    Ok(())
}

/// Generate array field read code, handling nullable fields.
fn generate_array_read(
    file: &mut fs::File,
    field_name: &str,
    element_type: &FieldType,
    flexible_versions: Versions,
    indent: &str,
    nullable: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let null_action = if nullable {
        format!("result.{} = None;", field_name)
    } else {
        "return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, \"Null array not allowed\"));".to_string()
    };
    let neg_action = if nullable {
        format!("result.{} = None;", field_name)
    } else {
        "return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, \"Negative array length\"));".to_string()
    };

    // Helper to generate the read loop that populates the array
    let gen_read_loop = |file: &mut fs::File, ind: &str, fn_name: &str| -> Result<(), Box<dyn std::error::Error>> {
        if nullable {
            writeln!(file, "{}let mut _arr = Vec::with_capacity(length as usize);", ind)?;
            // We need a temporary field_name for element reads
            writeln!(file, "{}for _ in 0..length {{", ind)?;
            generate_array_element_read_to_vec(file, element_type, "_arr", flexible_versions)?;
            writeln!(file, "{}}}", ind)?;
            writeln!(file, "{}result.{} = Some(_arr);", ind, fn_name)?;
        } else {
            writeln!(file, "{}result.{} = Vec::with_capacity(length as usize);", ind, fn_name)?;
            writeln!(file, "{}for _ in 0..length {{", ind)?;
            generate_array_element_read(file, element_type, fn_name, flexible_versions)?;
            writeln!(file, "{}}}", ind)?;
        }
        Ok(())
    };

    if !flexible_versions.empty() {
        if flexible_versions.lowest() == 0 {
            // All versions are flexible
            writeln!(file, "{}let len = readable.read_unsigned_varint()?;", indent)?;
            writeln!(file, "{}if len == 0 {{", indent)?;
            writeln!(file, "{}    {}", indent, null_action)?;
            writeln!(file, "{}}} else {{", indent)?;
            writeln!(file, "{}    let length = len - 1;", indent)?;
            gen_read_loop(file, &format!("{}    ", indent), field_name)?;
            writeln!(file, "{}}}", indent)?;
        } else {
            // Mixed flexible/standard
            if flexible_versions.highest() == i16::MAX {
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
            writeln!(file, "{}    let len = readable.read_unsigned_varint()?;", indent)?;
            writeln!(file, "{}    if len == 0 {{", indent)?;
            writeln!(file, "{}        {}", indent, null_action)?;
            writeln!(file, "{}    }} else {{", indent)?;
            writeln!(file, "{}        let length = len - 1;", indent)?;
            gen_read_loop(file, &format!("{}        ", indent), field_name)?;
            writeln!(file, "{}    }}", indent)?;
            writeln!(file, "{}}} else {{", indent)?;
            writeln!(file, "{}    let len = readable.read_int()?;", indent)?;
            writeln!(file, "{}    if len < 0 {{", indent)?;
            writeln!(file, "{}        {}", indent, neg_action)?;
            writeln!(file, "{}    }} else {{", indent)?;
            writeln!(file, "{}        let length = len as u32;", indent)?;
            gen_read_loop(file, &format!("{}        ", indent), field_name)?;
            writeln!(file, "{}    }}", indent)?;
            writeln!(file, "{}}}", indent)?;
        }
    } else {
        // Standard only
        writeln!(file, "{}let len = readable.read_int()?;", indent)?;
        writeln!(file, "{}if len < 0 {{", indent)?;
        writeln!(file, "{}    {}", indent, neg_action)?;
        writeln!(file, "{}}} else {{", indent)?;
        writeln!(file, "{}    let length = len as u32;", indent)?;
        gen_read_loop(file, &format!("{}    ", indent), field_name)?;
        writeln!(file, "{}}}", indent)?;
    }
    Ok(())
}

/// Like generate_array_element_read but pushes to a local vec variable instead of result.field.
fn generate_array_element_read_to_vec(
    file: &mut fs::File,
    element_type: &FieldType,
    vec_name: &str,
    flexible_versions: Versions,
) -> Result<(), Box<dyn std::error::Error>> {
    // Reuse the same logic but with a different prefix (no "result." prefix)
    generate_array_element_read_with_prefix(file, element_type, vec_name, "", flexible_versions)
}

fn generate_array_element_read_with_prefix(
    file: &mut fs::File,
    element_type: &FieldType,
    array_name: &str,
    prefix: &str,
    flexible_versions: Versions,
) -> Result<(), Box<dyn std::error::Error>> {
    let target = format!("{}{}", prefix, array_name);
    match element_type {
        FieldType::Bool => {
            writeln!(file, "                {}.push(readable.read_byte()? != 0);", target)?;
        },
        FieldType::Int8 => {
            writeln!(file, "                {}.push(readable.read_byte()? as i8);", target)?;
        },
        FieldType::Int16 => {
            writeln!(file, "                {}.push(readable.read_short()?);", target)?;
        },
        FieldType::Int32 => {
            writeln!(file, "                {}.push(readable.read_int()?);", target)?;
        },
        FieldType::Int64 => {
            writeln!(file, "                {}.push(readable.read_long()?);", target)?;
        },
        FieldType::Uint16 => {
            writeln!(file, "                {}.push(readable.read_unsigned_short()?);", target)?;
        },
        FieldType::Uint32 => {
            writeln!(file, "                {}.push(readable.read_unsigned_int()?);", target)?;
        },
        FieldType::Uuid => {
            writeln!(file, "                {}.push(readable.read_uuid()?);", target)?;
        },
        FieldType::Float64 => {
            writeln!(file, "                {}.push(readable.read_double()?);", target)?;
        },
        FieldType::String => {
            if !flexible_versions.empty() {
                if flexible_versions.lowest() == 0 {
                    // All versions flexible: use compact encoding
                    writeln!(file, "                let str_len = readable.read_unsigned_varint()?;")?;
                    writeln!(file, "                if str_len == 0 {{")?;
                    writeln!(file, "                    {}.push(String::new());", target)?;
                    writeln!(file, "                }} else {{")?;
                    writeln!(file, "                    let mut bytes = vec![0u8; (str_len - 1) as usize];")?;
                    writeln!(file, "                    readable.read_bytes(&mut bytes)?;")?;
                    writeln!(
                        file,
                        "                    {}.push(String::from_utf8(bytes).map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?);",
                        target
                    )?;
                    writeln!(file, "                }}")?;
                } else {
                    // Mixed versions: check at runtime
                    if flexible_versions.highest() == i16::MAX {
                        writeln!(file, "                if version >= {} {{", flexible_versions.lowest())?;
                    } else {
                        writeln!(
                            file,
                            "                if version >= {} && version <= {} {{",
                            flexible_versions.lowest(),
                            flexible_versions.highest()
                        )?;
                    }
                    writeln!(file, "                    let str_len = readable.read_unsigned_varint()?;")?;
                    writeln!(file, "                    if str_len == 0 {{")?;
                    writeln!(file, "                        {}.push(String::new());", target)?;
                    writeln!(file, "                    }} else {{")?;
                    writeln!(
                        file,
                        "                        let mut bytes = vec![0u8; (str_len - 1) as usize];"
                    )?;
                    writeln!(file, "                        readable.read_bytes(&mut bytes)?;")?;
                    writeln!(
                        file,
                        "                        {}.push(String::from_utf8(bytes).map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?);",
                        target
                    )?;
                    writeln!(file, "                    }}")?;
                    writeln!(file, "                }} else {{")?;
                    writeln!(file, "                    let str_len = readable.read_short()? as usize;")?;
                    writeln!(file, "                    let mut bytes = vec![0u8; str_len];")?;
                    writeln!(file, "                    readable.read_bytes(&mut bytes)?;")?;
                    writeln!(
                        file,
                        "                    {}.push(String::from_utf8(bytes).map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?);",
                        target
                    )?;
                    writeln!(file, "                }}")?;
                }
            } else {
                // No flexible versions: always use standard encoding
                writeln!(file, "                let str_len = readable.read_short()? as usize;")?;
                writeln!(file, "                let mut bytes = vec![0u8; str_len];")?;
                writeln!(file, "                readable.read_bytes(&mut bytes)?;")?;
                writeln!(
                    file,
                    "                {}.push(String::from_utf8(bytes).map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?);",
                    target
                )?;
            }
        },
        FieldType::Struct(struct_name) => {
            writeln!(
                file,
                "                {}.push({}::read(readable, version)?);",
                target, struct_name
            )?;
        },
        _ => {
            writeln!(file, "                // TODO: read array element {:?}", element_type)?;
        },
    }
    Ok(())
}

fn generate_array_element_read(
    file: &mut fs::File,
    element_type: &FieldType,
    array_name: &str,
    flexible_versions: Versions,
) -> Result<(), Box<dyn std::error::Error>> {
    generate_array_element_read_with_prefix(file, element_type, array_name, "result.", flexible_versions)
}

fn generate_field_write(
    file: &mut fs::File,
    field: &FieldSpec,
    flexible_versions: Versions,
) -> Result<(), Box<dyn std::error::Error>> {
    let field_name = to_snake_case(field.name());
    let field_name = escape_rust_keyword(&field_name);
    let versions = field.versions();
    let nullable = is_nullable_field(field);

    // Determine if version check is needed
    let has_version_check = versions != Versions::ALL;
    let indent = if has_version_check { "            " } else { "        " };

    // Version check - avoid useless comparison when highest is i16::MAX
    if has_version_check {
        if versions.highest() == i16::MAX {
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

    // For nullable fields, wrap in if let Some/None
    let (inner_indent, accessor) = if nullable {
        writeln!(file, "{}if let Some(ref _nv) = self.{} {{", indent, field_name)?;
        let extra = format!("{}    ", indent);
        (extra, "_nv".to_string())
    } else {
        (indent.to_string(), format!("self.{}", field_name))
    };
    let ind = &inner_indent;

    match field.field_type() {
        FieldType::Bool => {
            writeln!(file, "{}writable.write_byte(if {} {{ 1 }} else {{ 0 }})?;", ind, accessor)?;
        },
        FieldType::Int8 => {
            writeln!(file, "{}writable.write_byte({})?;", ind, accessor)?;
        },
        FieldType::Int16 => {
            writeln!(file, "{}writable.write_short({})?;", ind, accessor)?;
        },
        FieldType::Int32 => {
            writeln!(file, "{}writable.write_int({})?;", ind, accessor)?;
        },
        FieldType::Int64 => {
            writeln!(file, "{}writable.write_long({})?;", ind, accessor)?;
        },
        FieldType::Uint16 => {
            writeln!(file, "{}writable.write_unsigned_short({})?;", ind, accessor)?;
        },
        FieldType::Uint32 => {
            writeln!(file, "{}writable.write_unsigned_int({})?;", ind, accessor)?;
        },
        FieldType::Uuid => {
            writeln!(file, "{}writable.write_uuid(&{})?;", ind, accessor)?;
        },
        FieldType::Float64 => {
            writeln!(file, "{}writable.write_double({})?;", ind, accessor)?;
        },
        FieldType::String => {
            writeln!(file, "{}let bytes = {}.as_bytes();", ind, accessor)?;
            // Check if this version uses flexible encoding
            if !flexible_versions.empty() {
                if flexible_versions.lowest() == 0 {
                    writeln!(file, "{}writable.write_unsigned_varint((bytes.len() as u32) + 1)?;", ind)?;
                } else {
                    if flexible_versions.highest() == i16::MAX {
                        writeln!(file, "{}if version >= {} {{", ind, flexible_versions.lowest())?;
                    } else {
                        writeln!(
                            file,
                            "{}if version >= {} && version <= {} {{",
                            ind,
                            flexible_versions.lowest(),
                            flexible_versions.highest()
                        )?;
                    }
                    writeln!(file, "{}    writable.write_unsigned_varint((bytes.len() as u32) + 1)?;", ind)?;
                    writeln!(file, "{}}} else {{", ind)?;
                    writeln!(file, "{}    writable.write_short(bytes.len() as i16)?;", ind)?;
                    writeln!(file, "{}}}", ind)?;
                }
            } else {
                writeln!(file, "{}writable.write_short(bytes.len() as i16)?;", ind)?;
            }
            writeln!(file, "{}writable.write_bytes(bytes)?;", ind)?;
        },
        FieldType::Bytes | FieldType::Records => {
            if !flexible_versions.empty() {
                if flexible_versions.lowest() == 0 {
                    writeln!(file, "{}writable.write_unsigned_varint(({}.len() as u32) + 1)?;", ind, accessor)?;
                } else {
                    if flexible_versions.highest() == i16::MAX {
                        writeln!(file, "{}if version >= {} {{", ind, flexible_versions.lowest())?;
                    } else {
                        writeln!(
                            file,
                            "{}if version >= {} && version <= {} {{",
                            ind,
                            flexible_versions.lowest(),
                            flexible_versions.highest()
                        )?;
                    }
                    writeln!(
                        file,
                        "{}    writable.write_unsigned_varint(({}.len() as u32) + 1)?;",
                        ind, accessor
                    )?;
                    writeln!(file, "{}}} else {{", ind)?;
                    writeln!(file, "{}    writable.write_int({}.len() as i32)?;", ind, accessor)?;
                    writeln!(file, "{}}}", ind)?;
                }
            } else {
                writeln!(file, "{}writable.write_int({}.len() as i32)?;", ind, accessor)?;
            }
            writeln!(file, "{}writable.write_bytes(&{})?;", ind, accessor)?;
        },
        FieldType::Array(element_type) => {
            if !flexible_versions.empty() {
                if flexible_versions.lowest() == 0 {
                    writeln!(file, "{}writable.write_unsigned_varint(({}.len() as u32) + 1)?;", ind, accessor)?;
                } else {
                    if flexible_versions.highest() == i16::MAX {
                        writeln!(file, "{}if version >= {} {{", ind, flexible_versions.lowest())?;
                    } else {
                        writeln!(
                            file,
                            "{}if version >= {} && version <= {} {{",
                            ind,
                            flexible_versions.lowest(),
                            flexible_versions.highest()
                        )?;
                    }
                    writeln!(
                        file,
                        "{}    writable.write_unsigned_varint(({}.len() as u32) + 1)?;",
                        ind, accessor
                    )?;
                    writeln!(file, "{}}} else {{", ind)?;
                    writeln!(file, "{}    writable.write_int({}.len() as i32)?;", ind, accessor)?;
                    writeln!(file, "{}}}", ind)?;
                }
            } else {
                writeln!(file, "{}writable.write_int({}.len() as i32)?;", ind, accessor)?;
            }
            writeln!(file, "{}for element in {}.iter() {{", ind, accessor)?;
            generate_array_element_write(file, element_type.as_ref(), flexible_versions)?;
            writeln!(file, "{}}}", ind)?;
        },
        FieldType::Struct(_) => {
            writeln!(file, "{}{}.write(writable, version)?;", ind, accessor)?;
        },
    }

    // Close nullable wrapper with null marker in else branch
    if nullable {
        writeln!(file, "{}}} else {{", indent)?;
        generate_null_write(file, field.field_type(), flexible_versions, indent)?;
        writeln!(file, "{}}}", indent)?;
    }

    if has_version_check {
        writeln!(file, "        }}")?;
    }
    writeln!(file)?;

    Ok(())
}

/// Generate null marker write for a nullable field.
fn generate_null_write(
    file: &mut fs::File,
    field_type: &FieldType,
    flexible_versions: Versions,
    indent: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let inner = format!("{}    ", indent);
    match field_type {
        FieldType::String => {
            if !flexible_versions.empty() {
                if flexible_versions.lowest() == 0 {
                    writeln!(file, "{}writable.write_unsigned_varint(0)?;", inner)?;
                } else {
                    if flexible_versions.highest() == i16::MAX {
                        writeln!(file, "{}if version >= {} {{", inner, flexible_versions.lowest())?;
                    } else {
                        writeln!(
                            file,
                            "{}if version >= {} && version <= {} {{",
                            inner,
                            flexible_versions.lowest(),
                            flexible_versions.highest()
                        )?;
                    }
                    writeln!(file, "{}    writable.write_unsigned_varint(0)?;", inner)?;
                    writeln!(file, "{}}} else {{", inner)?;
                    writeln!(file, "{}    writable.write_short(-1)?;", inner)?;
                    writeln!(file, "{}}}", inner)?;
                }
            } else {
                writeln!(file, "{}writable.write_short(-1)?;", inner)?;
            }
        },
        FieldType::Bytes | FieldType::Records => {
            if !flexible_versions.empty() {
                if flexible_versions.lowest() == 0 {
                    writeln!(file, "{}writable.write_unsigned_varint(0)?;", inner)?;
                } else {
                    if flexible_versions.highest() == i16::MAX {
                        writeln!(file, "{}if version >= {} {{", inner, flexible_versions.lowest())?;
                    } else {
                        writeln!(
                            file,
                            "{}if version >= {} && version <= {} {{",
                            inner,
                            flexible_versions.lowest(),
                            flexible_versions.highest()
                        )?;
                    }
                    writeln!(file, "{}    writable.write_unsigned_varint(0)?;", inner)?;
                    writeln!(file, "{}}} else {{", inner)?;
                    writeln!(file, "{}    writable.write_int(-1)?;", inner)?;
                    writeln!(file, "{}}}", inner)?;
                }
            } else {
                writeln!(file, "{}writable.write_int(-1)?;", inner)?;
            }
        },
        FieldType::Array(_) => {
            if !flexible_versions.empty() {
                if flexible_versions.lowest() == 0 {
                    writeln!(file, "{}writable.write_unsigned_varint(0)?;", inner)?;
                } else {
                    if flexible_versions.highest() == i16::MAX {
                        writeln!(file, "{}if version >= {} {{", inner, flexible_versions.lowest())?;
                    } else {
                        writeln!(
                            file,
                            "{}if version >= {} && version <= {} {{",
                            inner,
                            flexible_versions.lowest(),
                            flexible_versions.highest()
                        )?;
                    }
                    writeln!(file, "{}    writable.write_unsigned_varint(0)?;", inner)?;
                    writeln!(file, "{}}} else {{", inner)?;
                    writeln!(file, "{}    writable.write_int(-1)?;", inner)?;
                    writeln!(file, "{}}}", inner)?;
                }
            } else {
                writeln!(file, "{}writable.write_int(-1)?;", inner)?;
            }
        },
        _ => {
            // Primitive types cannot be nullable per can_be_nullable()
        },
    }
    Ok(())
}

fn generate_array_element_write(
    file: &mut fs::File,
    element_type: &FieldType,
    flexible_versions: Versions,
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
            if !flexible_versions.empty() {
                if flexible_versions.lowest() == 0 {
                    writeln!(
                        file,
                        "                writable.write_unsigned_varint((bytes.len() as u32) + 1)?;"
                    )?;
                } else {
                    if flexible_versions.highest() == i16::MAX {
                        writeln!(file, "                if version >= {} {{", flexible_versions.lowest())?;
                    } else {
                        writeln!(
                            file,
                            "                if version >= {} && version <= {} {{",
                            flexible_versions.lowest(),
                            flexible_versions.highest()
                        )?;
                    }
                    writeln!(
                        file,
                        "                    writable.write_unsigned_varint((bytes.len() as u32) + 1)?;"
                    )?;
                    writeln!(file, "                }} else {{")?;
                    writeln!(file, "                    writable.write_short(bytes.len() as i16)?;")?;
                    writeln!(file, "                }}")?;
                }
            } else {
                writeln!(file, "                writable.write_short(bytes.len() as i16)?;")?;
            }
            writeln!(file, "                writable.write_bytes(bytes)?;")?;
        },
        FieldType::Bytes | FieldType::Records => {
            if !flexible_versions.empty() {
                if flexible_versions.lowest() == 0 {
                    writeln!(
                        file,
                        "                writable.write_unsigned_varint((element.len() as u32) + 1)?;"
                    )?;
                } else {
                    if flexible_versions.highest() == i16::MAX {
                        writeln!(file, "                if version >= {} {{", flexible_versions.lowest())?;
                    } else {
                        writeln!(
                            file,
                            "                if version >= {} && version <= {} {{",
                            flexible_versions.lowest(),
                            flexible_versions.highest()
                        )?;
                    }
                    writeln!(
                        file,
                        "                    writable.write_unsigned_varint((element.len() as u32) + 1)?;"
                    )?;
                    writeln!(file, "                }} else {{")?;
                    writeln!(file, "                    writable.write_int(element.len() as i32)?;")?;
                    writeln!(file, "                }}")?;
                }
            } else {
                writeln!(file, "                writable.write_int(element.len() as i32)?;")?;
            }
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

/// Generate a `schema(version) -> Schema` method that returns the schema for each version.
fn generate_schema_method(
    file: &mut fs::File,
    struct_spec: &StructSpec,
    flexible_versions: Versions,
) -> Result<(), Box<dyn std::error::Error>> {
    let lowest = struct_spec.versions().lowest();
    let highest = struct_spec.versions().highest();

    // Check if any non-tagged field has a version range that doesn't cover all versions
    let needs_version_param = struct_spec.fields().iter().any(|f| {
        if f.tagged_versions() != Versions::NONE && f.tagged_versions() == f.versions() {
            return false; // entirely tagged, skip
        }
        let v_low = f.versions().lowest();
        let v_high = f.versions().highest();
        !(v_low <= lowest && v_high >= highest)
    });

    let version_param = if needs_version_param { "version" } else { "_version" };

    writeln!(file, "    /// Returns the schema for this message at the given version.")?;
    writeln!(file, "    pub fn schema({}: i16) -> Schema {{", version_param)?;
    writeln!(file, "        let mut fields = Vec::new();")?;

    // For each field, emit conditional push based on version
    for field in struct_spec.fields() {
        let field_name = to_snake_case(field.name());
        let field_name = escape_rust_keyword(&field_name);

        // Skip tagged fields from the main field list
        if field.tagged_versions() != Versions::NONE {
            // For fields that have tagged versions in some range and regular in another,
            // only include them in the schema when they're NOT in their tagged range
            let tagged = field.tagged_versions();
            let versions = field.versions();

            if tagged == versions {
                // Entirely tagged — skip from regular schema fields
                continue;
            }
        }

        let v_low = field.versions().lowest();
        let v_high = field.versions().highest();
        let schema_type_expr = schema_type_for_version_expr(field.field_type(), flexible_versions);
        let about_escaped = field.about().replace('"', "\\\"");
        let push_stmt = format!(
            "fields.push(Field {{ name: \"{}\", field_type: {}, about: \"{}\" }});",
            field_name, schema_type_expr, about_escaped,
        );

        let covers_all = v_low <= lowest && v_high >= highest;
        if covers_all {
            // Field present in all versions
            writeln!(file, "        {}", push_stmt)?;
        } else {
            // Build version condition avoiding useless comparisons
            let lower_check = if v_low > 0 {
                Some(format!("version >= {}", v_low))
            } else {
                None
            };
            let upper_check = if v_high < i16::MAX {
                Some(format!("version <= {}", v_high))
            } else {
                None
            };
            let condition = match (lower_check, upper_check) {
                (Some(l), Some(u)) => format!("{} && {}", l, u),
                (Some(l), None) => l,
                (None, Some(u)) => u,
                (None, None) => "true".to_string(),
            };
            writeln!(file, "        if {} {{", condition)?;
            writeln!(file, "            {}", push_stmt)?;
            writeln!(file, "        }}")?;
        }
    }

    writeln!(file, "        Schema::new(fields)")?;
    writeln!(file, "    }}")?;

    Ok(())
}

/// Map a FieldType to a SchemaType expression string for use in generated code.
fn schema_type_for_version_expr(field_type: &FieldType, flexible_versions: Versions) -> String {
    // For schema metadata we use a simplified type mapping.
    // The flexible/non-flexible distinction is version-dependent, but for field
    // introspection (the primary use case) we use the latest encoding style.
    let flexible = flexible_versions != Versions::NONE;
    match field_type {
        FieldType::Bool => "SchemaType::Boolean".to_string(),
        FieldType::Int8 => "SchemaType::Int8".to_string(),
        FieldType::Int16 => "SchemaType::Int16".to_string(),
        FieldType::Uint16 => "SchemaType::Uint16".to_string(),
        FieldType::Uint32 => "SchemaType::Uint32".to_string(),
        FieldType::Int32 => "SchemaType::Int32".to_string(),
        FieldType::Int64 => "SchemaType::Int64".to_string(),
        FieldType::Uuid => "SchemaType::Uuid".to_string(),
        FieldType::Float64 => "SchemaType::Float64".to_string(),
        FieldType::String => {
            if flexible {
                "SchemaType::CompactString".to_string()
            } else {
                "SchemaType::String".to_string()
            }
        },
        FieldType::Bytes | FieldType::Records => {
            if flexible {
                "SchemaType::CompactBytes".to_string()
            } else {
                "SchemaType::Bytes".to_string()
            }
        },
        FieldType::Array(_) => {
            // Arrays are represented as Bytes in the schema type for introspection
            if flexible {
                "SchemaType::CompactBytes".to_string()
            } else {
                "SchemaType::Bytes".to_string()
            }
        },
        FieldType::Struct(_) => {
            // Structs are composite - use Bytes as placeholder for introspection
            "SchemaType::Bytes".to_string()
        },
    }
}

/// Returns true if the field should use Option<T> in Rust (has nullable versions).
fn is_nullable_field(field: &FieldSpec) -> bool {
    !field.nullable_versions().empty()
}

/// Returns the Rust type for a field, wrapping in Option<> if nullable.
fn field_type_to_rust_for_field(field: &FieldSpec) -> String {
    let base_type = field_type_to_rust(field.field_type());
    if is_nullable_field(field) {
        format!("Option<{}>", base_type)
    } else {
        base_type
    }
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

/// Get the default value for a field, considering nullable.
fn get_default_value_for_field(field: &FieldSpec) -> String {
    let nullable = is_nullable_field(field);
    let default = field.field_default();

    // If nullable and default is "null", return None
    if nullable {
        if let Some(serde_json::Value::String(s)) = default
            && s == "null"
        {
            return "None".to_string();
        }
        // Nullable with no explicit default or non-null default
        if default.is_none() {
            return "None".to_string();
        }
        // Nullable with a non-null default: wrap in Some()
        let base_default = get_default_value(field.field_type(), default);
        return format!("Some({})", base_default);
    }

    get_default_value(field.field_type(), default)
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
                        if let Some(hex) = s.strip_prefix("0x") {
                            if let Ok(num) = i64::from_str_radix(hex, 16) {
                                return num.to_string();
                            }
                        } else if let Some(hex) = s.strip_prefix("-0x") {
                            if let Ok(num) = i64::from_str_radix(hex, 16) {
                                return format!("-{}", num);
                            }
                        } else if s.parse::<i64>().is_ok() {
                            // It's a valid number string, return it as-is (no quotes)
                            return s.to_string();
                        }
                    },
                    FieldType::Float64 => {
                        if s.parse::<f64>().is_ok() {
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
                    FieldType::Uuid => {
                        // UUID default is a base64 URL encoded string
                        return format!("Uuid::from_string(\"{}\").expect(\"invalid UUID default\")", s);
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

    // Include api_message_type module (generated separately)
    writeln!(file, "pub mod api_message_type;")?;
    writeln!(file)?;

    for spec_file in spec_files {
        let file_name = spec_file.file_stem().and_then(|s| s.to_str()).ok_or("Invalid file name")?;
        let module_name = format!("{}_data", to_snake_case(file_name));
        writeln!(file, "pub mod {};", module_name)?;
    }

    Ok(())
}

fn write_license_header(file: &mut fs::File) -> Result<(), Box<dyn std::error::Error>> {
    writeln!(file, "/*")?;
    writeln!(file, " * Copyright 2025 Confluent Inc.")?;
    writeln!(file, " *")?;
    writeln!(file, " * Licensed under the Apache License, Version 2.0 (the \"License\");")?;
    writeln!(file, " * you may not use this file except in compliance with the License.")?;
    writeln!(file, " * You may obtain a copy of the License at")?;
    writeln!(file, " *")?;
    writeln!(file, " *     http://www.apache.org/licenses/LICENSE-2.0")?;
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
    let module_name = format!("{}_data", to_snake_case(file_name));
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

        if !in_string
            && c == '/'
            && let Some(&next_c) = chars.peek()
            && next_c == '/'
        {
            // Line comment - skip until newline
            chars.next();
            for c in chars.by_ref() {
                if c == '\n' {
                    result.push('\n');
                    break;
                }
            }
            continue;
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
