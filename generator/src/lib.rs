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
    let mut generated_modules = Vec::new();
    for spec_file in &spec_files {
        match process_spec_file(spec_file, output_dir) {
            Ok(module_and_type) => {
                success_count += 1;
                generated_modules.push(module_and_type);
            },
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
    generate_mod_file(&spec_files, &generated_modules, output_dir)?;

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

    // enum_name()
    //
    // Java's generated `ApiMessageType` carries two distinct spellings of the API
    // name: the public `name` field (`"Metadata"`, emitted above as `name()`) and
    // the enum constant name (`"METADATA"`), which `Enum.name()` returns and which
    // the generated `toString()` delegates to
    // (`ApiMessageTypeGenerator.generateToString`). Java code that interpolates the
    // enum value itself — `"..." + apiKey` — gets the constant spelling, so the
    // Rust translation needs it as an accessor too.
    writeln!(file, "    /// The name of this enum variant, e.g. `\"METADATA\"`.")?;
    writeln!(file, "    ///")?;
    writeln!(
        file,
        "    /// This is the Rust equivalent of Java's `Enum.name()` on the generated"
    )?;
    writeln!(
        file,
        "    /// `ApiMessageType` enum, which its `toString()` also returns. It differs"
    )?;
    writeln!(
        file,
        "    /// from [`Self::name`], the translation of Java's public `name` field, which"
    )?;
    writeln!(file, "    /// carries the specification spelling (e.g. `\"Metadata\"`).")?;
    writeln!(file, "    pub fn enum_name(self) -> &'static str {{")?;
    writeln!(file, "        match self {{")?;
    for data in apis.values() {
        let variant = to_snake_case(&data.name(&api_names)).to_uppercase();
        writeln!(file, "            Self::{} => \"{}\",", variant, variant)?;
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
    writeln!(file)?;

    // Display — Java's generated `toString()` returns `this.name()`, the enum
    // constant name (`ApiMessageTypeGenerator.generateToString`).
    writeln!(file, "impl std::fmt::Display for ApiMessageType {{")?;
    writeln!(
        file,
        "    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {{"
    )?;
    writeln!(file, "        f.write_str(self.enum_name())")?;
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

/// Generates one message module and returns `(module_name, top_level_type)` so the
/// caller can emit the parent re-export CLAUDE.md §2 requires for the top-level type.
fn process_spec_file(spec_file: &Path, output_dir: &Path) -> Result<(String, String), Box<dyn std::error::Error>> {
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
    writeln!(file, "use bytes::Bytes;")?;
    writeln!(file, "use std::fmt;")?;
    writeln!(file, "use std::hash::{{Hash, Hasher}};")?;
    writeln!(file)?;

    // Generate the message struct
    generate_message_struct(&mut file, &message_spec)?;

    Ok((module_name, format!("{}Data", message_spec.name())))
}

fn generate_message_struct(file: &mut fs::File, spec: &MessageSpec) -> Result<(), Box<dyn std::error::Error>> {
    let struct_spec = spec.struct_spec();
    let data_class_name = format!("{}Data", spec.name());
    let flexible_versions = spec.flexible_versions();

    // Generate common structs first (defined at message level).
    // Java: `generateClass(commonStruct, commonStruct.versions())`
    // (`MessageDataGenerator.java:129-134`) — a common struct is its own version root.
    for common_struct in spec.common_structs() {
        generate_common_struct(file, common_struct, flexible_versions)?;
    }

    // Generate nested structs (for array element types and direct struct types).
    // Java threads `parentVersions.intersect(struct.versions())` into each subclass
    // (`MessageDataGenerator.java:170-184`); at the top level that is just the
    // message's own version range.
    for field in struct_spec.fields() {
        // Always try to generate nested struct - the function will determine if it's needed
        generate_nested_struct(file, field, flexible_versions, struct_spec.versions())?;
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

    // write() method. Java's top-level call passes the message's own range as
    // `parentVersions` (`MessageDataGenerator.java:65-68`).
    generate_write_method(file, &data_class_name, struct_spec, flexible_versions, struct_spec.versions())?;
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

/// `parent_versions` is Java's `parentVersions` for this subclass: the enclosing
/// struct's *effective* range (`MessageDataGenerator.java:176`,
/// `parentVersions.intersect(struct.versions())`). It is intersected with this
/// struct's own declared range to obtain the versions at which `write()` can actually
/// be invoked — which is what decides whether a field's version gate has a reachable
/// `else` half. See [`non_ignorable_check_applies`].
fn generate_nested_struct(
    file: &mut fs::File,
    field: &FieldSpec,
    flexible_versions: Versions,
    parent_versions: Versions,
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

    // Java's `curVersions` for this struct: the versions at which the enclosing struct
    // can actually reach it (`MessageDataGenerator.java:718`).
    let cur_versions = parent_versions.intersect(field.versions());

    // First, recursively generate any nested structs within this nested struct
    for nested_field in field.fields() {
        if !nested_field.fields().is_empty() {
            generate_nested_struct(file, nested_field, flexible_versions, cur_versions)?;
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
    generate_write_method(file, &struct_name, &struct_spec, flexible_versions, parent_versions)?;

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
            generate_nested_struct(file, field, flexible_versions, struct_spec.versions())?;
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

    // Generate write method for common struct. Java treats a common struct as its own
    // version root: `generateClass(commonStruct, commonStruct.versions())`
    // (`MessageDataGenerator.java:129-134`).
    generate_write_method(file, struct_name, struct_spec, flexible_versions, struct_spec.versions())?;

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
        "    fn write(&mut self, writable: &mut dyn Writable, _cache: &ObjectSerializationCache, version: i16) -> std::io::Result<()> {{"
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
            let effective_flex = field_flexible_versions(field, flexible_versions);
            generate_field_add_size(file, field, effective_flex)?;
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
                "{}    size.add_bytes(crate::common::protocol::ByteUtils::size_of_unsigned_varint(field.tag()));",
                indent
            )?;
            writeln!(
                file,
                "{}    size.add_bytes(crate::common::protocol::ByteUtils::size_of_unsigned_varint(field.size() as u32));",
                indent
            )?;
            writeln!(file, "{}    size.add_bytes(field.size() as i32);", indent)?;
            writeln!(file, "{}}}", indent)?;
            writeln!(
                file,
                "{}size.add_bytes(crate::common::protocol::ByteUtils::size_of_unsigned_varint(num_tagged_fields));",
                indent
            )?;
            writeln!(file, "        }}")?;
        } else {
            let indent = "        ";
            writeln!(file, "{}num_tagged_fields += self.unknown_tagged_fields.len() as u32;", indent)?;
            writeln!(file, "{}for field in &self.unknown_tagged_fields {{", indent)?;
            writeln!(
                file,
                "{}    size.add_bytes(crate::common::protocol::ByteUtils::size_of_unsigned_varint(field.tag()));",
                indent
            )?;
            writeln!(
                file,
                "{}    size.add_bytes(crate::common::protocol::ByteUtils::size_of_unsigned_varint(field.size() as u32));",
                indent
            )?;
            writeln!(file, "{}    size.add_bytes(field.size() as i32);", indent)?;
            writeln!(file, "{}}}", indent)?;
            writeln!(
                file,
                "{}size.add_bytes(crate::common::protocol::ByteUtils::size_of_unsigned_varint(num_tagged_fields));",
                indent
            )?;
        }
    }

    writeln!(file, "        Ok(())")?;
    writeln!(file, "    }}")?;

    Ok(())
}

/// Returns the effective flexible versions for a field, taking into account
/// the per-field `flexibleVersions` override.
///
/// Corresponds to `MessageDataGenerator.fieldFlexibleVersions` in the Java generator.
/// When a field specifies `"flexibleVersions": "none"`, the field always uses
/// non-flexible encoding regardless of the message-level flexible versions.
fn field_flexible_versions(field: &FieldSpec, message_flexible_versions: Versions) -> Versions {
    if let Some(field_flex) = field.flexible_versions() {
        // Validate that the field's flexible versions are a subset of the message's
        if message_flexible_versions.intersect(field_flex) != field_flex {
            panic!(
                "The flexible versions for field {} are {:?}, which are not a subset of the \
                 flexible versions for the message as a whole, which are {:?}",
                field.name(),
                field_flex,
                message_flexible_versions
            );
        }
        field_flex
    } else {
        message_flexible_versions
    }
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
        FieldType::Bytes => {
            generate_bytes_add_size(file, &accessor, flexible_versions, ind, false)?;
        },
        FieldType::Records => {
            generate_bytes_add_size(file, &accessor, flexible_versions, ind, true)?;
        },
        FieldType::Array(element_type) => {
            generate_array_add_size(file, &accessor, element_type, flexible_versions, ind)?;
        },
        FieldType::Struct(_) => {
            if nullable {
                // For nullable struct fields, add 1 byte for the presence indicator
                // in versions where the field is nullable.
                let nullable_versions = field.nullable_versions();
                if nullable_versions.lowest() == 0 {
                    writeln!(file, "{}size.add_bytes(1); // non-null presence byte", ind)?;
                } else {
                    writeln!(file, "{}if version >= {} {{", ind, nullable_versions.lowest())?;
                    writeln!(file, "{}    size.add_bytes(1); // non-null presence byte", ind)?;
                    writeln!(file, "{}}}", ind)?;
                }
            }
            writeln!(file, "{}{}.add_size(size, cache, version)?;", ind, accessor)?;
        },
    }

    // Close nullable wrapper with null marker size in else branch
    if nullable {
        writeln!(file, "{}}} else {{", indent)?;
        if matches!(field.field_type(), FieldType::Struct(_)) {
            // For nullable struct fields, null marker is a single byte (presence byte = -1)
            let inner = format!("{}    ", indent);
            writeln!(file, "{}size.add_bytes(1); // null struct presence byte", inner)?;
        } else {
            generate_null_add_size(file, field.field_type(), flexible_versions, indent)?;
        }
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
                "{}    size.add_bytes(crate::common::protocol::ByteUtils::size_of_unsigned_varint(bytes_len + 1));",
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
                "{}        size.add_bytes(crate::common::protocol::ByteUtils::size_of_unsigned_varint(bytes_len + 1));",
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
///
/// When `zero_copy` is true (Records fields), the data size is tracked via
/// `add_zero_copy_bytes` so the SendBuilder allocates the main buffer without
/// it and handles the records via scatter-gather I/O.
fn generate_bytes_add_size(
    file: &mut fs::File,
    accessor: &str,
    flexible_versions: Versions,
    indent: &str,
    zero_copy: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let add_data_fn = if zero_copy { "add_zero_copy_bytes" } else { "add_bytes" };
    writeln!(file, "{}{{", indent)?;
    writeln!(file, "{}    let bytes_len = {}.len() as u32;", indent, accessor)?;
    if !flexible_versions.empty() {
        if flexible_versions.lowest() == 0 {
            writeln!(
                file,
                "{}    size.add_bytes(crate::common::protocol::ByteUtils::size_of_unsigned_varint(bytes_len + 1));",
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
                "{}        size.add_bytes(crate::common::protocol::ByteUtils::size_of_unsigned_varint(bytes_len + 1));",
                indent
            )?;
            writeln!(file, "{}    }} else {{", indent)?;
            writeln!(file, "{}        size.add_bytes(4); // i32 length prefix", indent)?;
            writeln!(file, "{}    }}", indent)?;
        }
    } else {
        writeln!(file, "{}    size.add_bytes(4); // i32 length prefix", indent)?;
    }
    writeln!(file, "{}    size.{}(bytes_len as i32);", indent, add_data_fn)?;
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
                "{}size.add_bytes(crate::common::protocol::ByteUtils::size_of_unsigned_varint({}.len() as u32 + 1));",
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
                "{}    size.add_bytes(crate::common::protocol::ByteUtils::size_of_unsigned_varint({}.len() as u32 + 1));",
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
                        "{}    size.add_bytes(crate::common::protocol::ByteUtils::size_of_unsigned_varint(bytes_len + 1));",
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
                        "{}        size.add_bytes(crate::common::protocol::ByteUtils::size_of_unsigned_varint(bytes_len + 1));",
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
                        "{}    size.add_bytes(crate::common::protocol::ByteUtils::size_of_unsigned_varint(bytes_len + 1));",
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
                        "{}        size.add_bytes(crate::common::protocol::ByteUtils::size_of_unsigned_varint(bytes_len + 1));",
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
        "{}size.add_bytes(crate::common::protocol::ByteUtils::size_of_unsigned_varint({}));",
        inner, tag
    )?;
    generate_tagged_field_content_size(file, field, &field_name, flexible_versions, &inner)?;
    writeln!(file, "{}}}", indent)?;

    if needs_field_version_check {
        writeln!(file, "{}}}", base_indent)?;
    }

    Ok(())
}

/// True when Java's `FieldSpec.fieldDefault` would render this field's default as
/// the literal `null` (`FieldSpec.java:400-475`).
///
/// Only `string` / `bytes` / `struct` / `array` reach it through an explicit
/// `"default": "null"` in the spec. `records` reaches it *unconditionally* — Java has a
/// bare `else if (type.isRecords()) return "null";` (`FieldSpec.java:452-453`) with no
/// nullability or explicit-default test, which is why it is checked first here.
fn field_default_is_null(field: &FieldSpec) -> bool {
    if matches!(field.field_type(), FieldType::Records) {
        return true;
    }
    matches!(field.field_default(), Some(serde_json::Value::String(s)) if s == "null")
}

/// The "is this field set to something other than its default?" condition.
///
/// Mirrors `FieldSpec.generateNonDefaultValueCheck`
/// (`kafka/generator/.../FieldSpec.java:587-641`), always with Java's
/// `nullableVersions = field.nullableVersions()` argument.
///
/// Java uses this one predicate for **two** purposes, and so does this generator:
///
///   1. "should this tagged field be serialised at all?" — a tagged field at its
///      default is omitted from the wire (`MessageDataGenerator.java:779`, `:1163`);
///   2. "is a value being dropped by a version gate?" — the guard emitted by
///      [`generate_non_ignorable_field_error`] (`MessageDataGenerator.java:794`).
///
/// Keep the two uses on this single function: divergence between them is exactly the
/// class of bug the guard exists to catch.
fn get_default_check(field: &FieldSpec, field_name: &str) -> String {
    // Java's `nullableVersions.empty()` test. `is_nullable_field` is its negation.
    let nullable = is_nullable_field(field);
    let default_is_null = field_default_is_null(field);

    match field.field_type() {
        // Java `type().isArray()` branch (FieldSpec.java:593-601).
        FieldType::Array(_) => {
            if default_is_null {
                format!("self.{}.is_some()", field_name)
            } else if !nullable {
                format!("!self.{}.is_empty()", field_name)
            } else {
                format!("self.{}.as_ref().map_or(true, |v| !v.is_empty())", field_name)
            }
        },
        // Java `type().isBytes()` branch (FieldSpec.java:602-621). `records` is NOT
        // `isBytes()` in Java — only `BytesFieldType` overrides it (`FieldType.java:257`)
        // — so `records` falls through to the final `else` below.
        FieldType::Bytes => {
            if default_is_null {
                format!("self.{}.is_some()", field_name)
            } else if !nullable {
                format!("!self.{}.is_empty()", field_name)
            } else {
                format!("self.{}.as_ref().map_or(true, |v| !v.is_empty())", field_name)
            }
        },
        // Java `isString() || isStruct() || UUIDFieldType` branch (FieldSpec.java:622-632).
        FieldType::String => {
            if default_is_null {
                format!("self.{}.is_some()", field_name)
            } else {
                // Java compares against `fieldDefault`, which is `""` when the spec sets no
                // default. Comparing against a borrowed `&str` rather than an owned
                // `String::new()` keeps the emitted code free of `clippy::cmp_owned`, and
                // `!is_empty()` is the same predicate as `!equals("")`.
                let literal = match field.field_default() {
                    Some(serde_json::Value::String(s)) if !s.is_empty() => Some(s.clone()),
                    _ => None,
                };
                match (literal, nullable) {
                    (None, false) => format!("!self.{}.is_empty()", field_name),
                    (None, true) => format!("self.{}.as_ref().map_or(true, |v| !v.is_empty())", field_name),
                    (Some(d), false) => format!("self.{} != \"{}\"", field_name, d),
                    (Some(d), true) => format!("self.{}.as_deref() != Some(\"{}\")", field_name, d),
                }
            }
        },
        FieldType::Struct(struct_name) => {
            if default_is_null {
                format!("self.{}.is_some()", field_name)
            } else if !nullable {
                format!("self.{} != {}::new()", field_name, struct_name)
            } else {
                format!(
                    "self.{}.is_none() || self.{}.as_ref().unwrap() != &{}::new()",
                    field_name, field_name, struct_name
                )
            }
        },
        FieldType::Uuid => {
            let default_val = get_default_value(field.field_type(), field.field_default());
            if !nullable {
                format!("self.{} != {}", field_name, default_val)
            } else {
                format!(
                    "self.{}.is_none() || self.{}.as_ref().unwrap() != &{}",
                    field_name, field_name, default_val
                )
            }
        },
        // Java `BoolFieldType` branch (FieldSpec.java:633-636): a bare `if (field)` when
        // the default is false, `if (!field)` when it is true. Spelling it that way
        // rather than `!= false` also keeps `clippy::bool_comparison` quiet.
        FieldType::Bool => {
            let default_val = get_default_value(field.field_type(), field.field_default());
            if default_val == "true" {
                format!("!self.{}", field_name)
            } else {
                format!("self.{}", field_name)
            }
        },
        // Java's final `else` (FieldSpec.java:637-639): `field != <default>`. `records`
        // lands here with a `null` default, i.e. a plain presence test.
        FieldType::Records => {
            if nullable {
                format!("self.{}.is_some()", field_name)
            } else {
                // No spec in the corpus declares a non-nullable `records` field; the Rust
                // type is then a bare `Bytes`, which has no `null` state to test.
                format!("!self.{}.is_empty()", field_name)
            }
        },
        FieldType::Float64 => {
            let default_val = get_default_value(field.field_type(), field.field_default());
            // Float comparison: use to_bits() for exact comparison like Java's Double.compare
            format!("self.{}.to_bits() != {}f64.to_bits()", field_name, default_val)
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
    }
}

/// True when Java's generator would emit the "non-default value at an unsupported
/// version" guard for `field` inside `write()`.
///
/// Two conditions, both taken from Java:
///
///   - `!field.ignorable()` — `MessageDataGenerator.java:792`. An `"ignorable": true`
///     field is silently dropped by Java too, so adding the guard there would reject
///     encodings Java accepts (see `.claude/rules/producer-transactions.md` §11).
///   - the `else` half of the field's version conditional is reachable —
///     `VersionConditional.generate` (`VersionConditional.java:189-218`) only emits
///     `ifNotMember` when `possibleVersions - containingVersions` is non-empty.
///     `possibleVersions` is the struct's own range, because the generated `write()`
///     has already rejected every version outside it.
fn non_ignorable_check_applies(field: &FieldSpec, struct_versions: Versions) -> bool {
    if field.ignorable() {
        return false;
    }
    field.versions().lowest() > struct_versions.lowest() || field.versions().highest() < struct_versions.highest()
}

/// Emits the body of Java's non-ignorable-field guard: the `throw` that refuses to
/// encode a message the requested version cannot represent.
///
/// Mirrors `FieldSpec.generateNonIgnorableFieldCheck` (`FieldSpec.java:652-665`). The
/// caller supplies the surrounding `if` / `else if`, because the two call sites reach
/// this branch differently.
///
/// The message names the field with `FieldSpec::camel_case_name` — Java's
/// `camelCaseName()` (`FieldSpec.java:188-190`, `:661`), i.e. the spec name with a
/// lower-cased first letter, not the snake_case Rust identifier — so the text is
/// byte-identical to the Java client's for the same field.
fn generate_non_ignorable_field_error(
    file: &mut fs::File,
    field: &FieldSpec,
    indent: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    writeln!(file, "{}return Err(std::io::Error::new(", indent)?;
    writeln!(file, "{}    std::io::ErrorKind::InvalidData,", indent)?;
    writeln!(
        file,
        "{}    format!(\"Attempted to write a non-default {} at version {{}}\", version),",
        indent,
        field.camel_case_name()
    )?;
    writeln!(file, "{}));", indent)?;
    Ok(())
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
    // For nullable tagged fields that pass the default check, the value may be None
    // (null, which differs from the default empty value) or Some(non-default).
    // For types that need .len() (String, Bytes, Array), we must handle None separately.
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
                "{}size.add_bytes(crate::common::protocol::ByteUtils::size_of_unsigned_varint(1)); // size prefix",
                indent
            )?;
            writeln!(file, "{}size.add_bytes(1);", indent)?;
        },
        FieldType::Int16 | FieldType::Uint16 => {
            writeln!(
                file,
                "{}size.add_bytes(crate::common::protocol::ByteUtils::size_of_unsigned_varint(2)); // size prefix",
                indent
            )?;
            writeln!(file, "{}size.add_bytes(2);", indent)?;
        },
        FieldType::Int32 | FieldType::Uint32 => {
            writeln!(
                file,
                "{}size.add_bytes(crate::common::protocol::ByteUtils::size_of_unsigned_varint(4)); // size prefix",
                indent
            )?;
            writeln!(file, "{}size.add_bytes(4);", indent)?;
        },
        FieldType::Int64 => {
            writeln!(
                file,
                "{}size.add_bytes(crate::common::protocol::ByteUtils::size_of_unsigned_varint(8)); // size prefix",
                indent
            )?;
            writeln!(file, "{}size.add_bytes(8);", indent)?;
        },
        FieldType::Float64 => {
            writeln!(
                file,
                "{}size.add_bytes(crate::common::protocol::ByteUtils::size_of_unsigned_varint(8)); // size prefix",
                indent
            )?;
            writeln!(file, "{}size.add_bytes(8);", indent)?;
        },
        FieldType::Uuid => {
            writeln!(
                file,
                "{}size.add_bytes(crate::common::protocol::ByteUtils::size_of_unsigned_varint(16)); // size prefix",
                indent
            )?;
            writeln!(file, "{}size.add_bytes(16);", indent)?;
        },
        FieldType::String => {
            // String in tagged field: varint(inner_size) where inner_size = varint(len+1) + len
            // For nullable: None encodes as varint(0), inner_size = 1
            writeln!(file, "{}{{", indent)?;
            if nullable {
                writeln!(file, "{}    if let Some(ref val) = self.{} {{", indent, field_name)?;
                writeln!(file, "{}        let bytes_len = val.len() as u32;", indent)?;
                writeln!(
                    file,
                    "{}        let string_prefix_size = crate::common::protocol::ByteUtils::size_of_unsigned_varint(bytes_len + 1);",
                    indent
                )?;
                writeln!(
                    file,
                    "{}        let inner_size = string_prefix_size + bytes_len as i32;",
                    indent
                )?;
                writeln!(
                    file,
                    "{}        size.add_bytes(crate::common::protocol::ByteUtils::size_of_unsigned_varint(inner_size as u32));",
                    indent
                )?;
                writeln!(file, "{}        size.add_bytes(inner_size);", indent)?;
                writeln!(file, "{}    }} else {{", indent)?;
                // null encoding: varint(0) = 1 byte, so inner_size = 1
                writeln!(
                    file,
                    "{}        size.add_bytes(crate::common::protocol::ByteUtils::size_of_unsigned_varint(1)); // size prefix for null",
                    indent
                )?;
                writeln!(file, "{}        size.add_bytes(1); // varint(0) for null", indent)?;
                writeln!(file, "{}    }}", indent)?;
            } else {
                writeln!(file, "{}    let bytes_len = {}.len() as u32;", indent, accessor)?;
                writeln!(
                    file,
                    "{}    let string_prefix_size = crate::common::protocol::ByteUtils::size_of_unsigned_varint(bytes_len + 1);",
                    indent
                )?;
                writeln!(file, "{}    let inner_size = string_prefix_size + bytes_len as i32;", indent)?;
                writeln!(
                    file,
                    "{}    size.add_bytes(crate::common::protocol::ByteUtils::size_of_unsigned_varint(inner_size as u32)); // size prefix",
                    indent
                )?;
                writeln!(file, "{}    size.add_bytes(inner_size);", indent)?;
            }
            writeln!(file, "{}}}", indent)?;
        },
        FieldType::Bytes | FieldType::Records => {
            writeln!(file, "{}{{", indent)?;
            if nullable {
                writeln!(file, "{}    if let Some(ref val) = self.{} {{", indent, field_name)?;
                writeln!(file, "{}        let bytes_len = val.len() as u32;", indent)?;
                writeln!(
                    file,
                    "{}        let bytes_prefix_size = crate::common::protocol::ByteUtils::size_of_unsigned_varint(bytes_len + 1);",
                    indent
                )?;
                writeln!(file, "{}        let inner_size = bytes_prefix_size + bytes_len as i32;", indent)?;
                writeln!(
                    file,
                    "{}        size.add_bytes(crate::common::protocol::ByteUtils::size_of_unsigned_varint(inner_size as u32));",
                    indent
                )?;
                writeln!(file, "{}        size.add_bytes(inner_size);", indent)?;
                writeln!(file, "{}    }} else {{", indent)?;
                // null encoding: varint(0) = 1 byte, so inner_size = 1
                writeln!(
                    file,
                    "{}        size.add_bytes(crate::common::protocol::ByteUtils::size_of_unsigned_varint(1)); // size prefix for null",
                    indent
                )?;
                writeln!(file, "{}        size.add_bytes(1); // varint(0) for null", indent)?;
                writeln!(file, "{}    }}", indent)?;
            } else {
                writeln!(file, "{}    let bytes_len = {}.len() as u32;", indent, accessor)?;
                writeln!(
                    file,
                    "{}    let bytes_prefix_size = crate::common::protocol::ByteUtils::size_of_unsigned_varint(bytes_len + 1);",
                    indent
                )?;
                writeln!(file, "{}    let inner_size = bytes_prefix_size + bytes_len as i32;", indent)?;
                writeln!(
                    file,
                    "{}    size.add_bytes(crate::common::protocol::ByteUtils::size_of_unsigned_varint(inner_size as u32)); // size prefix",
                    indent
                )?;
                writeln!(file, "{}    size.add_bytes(inner_size);", indent)?;
            }
            writeln!(file, "{}}}", indent)?;
        },
        FieldType::Array(element_type) => {
            // For tagged arrays, we need to compute the total array serialized size
            writeln!(file, "{}{{", indent)?;
            writeln!(file, "{}    let mut array_size: i32 = 0;", indent)?;
            // Array length prefix (varint(len+1))
            writeln!(
                file,
                "{}    array_size += crate::common::protocol::ByteUtils::size_of_unsigned_varint({}.len() as u32 + 1);",
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
                            "{}        array_size += crate::common::protocol::ByteUtils::size_of_unsigned_varint(elem_len + 1);",
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
                "{}    size.add_bytes(crate::common::protocol::ByteUtils::size_of_unsigned_varint(array_size as u32)); // size prefix",
                indent
            )?;
            writeln!(file, "{}    size.add_bytes(array_size);", indent)?;
            writeln!(file, "{}}}", indent)?;
        },
        FieldType::Struct(_) => {
            // For tagged structs, compute struct size in a sub-accumulator
            if nullable {
                let has_null_default =
                    matches!(field.field_default(), Some(serde_json::Value::String(s)) if s == "null");
                if has_null_default {
                    // Default is null, so we only enter here when Some.
                    // Tagged field data = presence indicator varint(1) + struct content.
                    // Size prefix encodes the total data size (1 + struct_size).
                    writeln!(file, "{}{{", indent)?;
                    writeln!(file, "{}    let mut struct_acc = MessageSizeAccumulator::new();", indent)?;
                    writeln!(file, "{}    {}.add_size(&mut struct_acc, cache, version)?;", indent, accessor)?;
                    writeln!(file, "{}    let struct_size = struct_acc.total_size();", indent)?;
                    writeln!(
                        file,
                        "{}    let content_size = 1 + struct_size; // presence indicator + struct",
                        indent
                    )?;
                    writeln!(
                        file,
                        "{}    size.add_bytes(crate::common::protocol::ByteUtils::size_of_unsigned_varint(content_size as u32)); // size prefix",
                        indent
                    )?;
                    writeln!(file, "{}    size.add_bytes(content_size);", indent)?;
                    writeln!(file, "{}}}", indent)?;
                } else {
                    // Default is non-null. The field can be None (encoding null) or Some (encoding the struct).
                    writeln!(file, "{}if let Some(ref val) = self.{} {{", indent, field_name)?;
                    writeln!(file, "{}    let mut struct_acc = MessageSizeAccumulator::new();", indent)?;
                    writeln!(file, "{}    val.add_size(&mut struct_acc, cache, version)?;", indent)?;
                    writeln!(file, "{}    let struct_size = struct_acc.total_size();", indent)?;
                    writeln!(
                        file,
                        "{}    let content_size = 1 + struct_size; // presence indicator + struct",
                        indent
                    )?;
                    writeln!(
                        file,
                        "{}    size.add_bytes(crate::common::protocol::ByteUtils::size_of_unsigned_varint(content_size as u32)); // size prefix",
                        indent
                    )?;
                    writeln!(file, "{}    size.add_bytes(content_size);", indent)?;
                    writeln!(file, "{}}} else {{", indent)?;
                    writeln!(
                        file,
                        "{}    // Null: just the null presence indicator varint(0) = 1 byte",
                        indent
                    )?;
                    writeln!(
                        file,
                        "{}    size.add_bytes(crate::common::protocol::ByteUtils::size_of_unsigned_varint(1)); // size prefix for 1 byte",
                        indent
                    )?;
                    writeln!(file, "{}    size.add_bytes(1); // varint(0) null indicator", indent)?;
                    writeln!(file, "{}}}", indent)?;
                }
            } else {
                writeln!(file, "{}{{", indent)?;
                writeln!(file, "{}    let mut struct_acc = MessageSizeAccumulator::new();", indent)?;
                writeln!(file, "{}    {}.add_size(&mut struct_acc, cache, version)?;", indent, accessor)?;
                writeln!(file, "{}    let struct_size = struct_acc.total_size();", indent)?;
                writeln!(
                    file,
                    "{}    size.add_bytes(crate::common::protocol::ByteUtils::size_of_unsigned_varint(struct_size as u32)); // size prefix",
                    indent
                )?;
                writeln!(file, "{}    size.add_bytes(struct_size);", indent)?;
                writeln!(file, "{}}}", indent)?;
            }
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
                            "{}                        if length as usize > readable.remaining() {{",
                            indent
                        )?;
                        writeln!(
                            file,
                            "{}                            return Err(std::io::Error::new(std::io::ErrorKind::InvalidData,",
                            indent
                        )?;
                        writeln!(
                            file,
                            "{}                                format!(\"Tried to allocate a collection of size {{}}, but there are only {{}} bytes remaining.\", length, readable.remaining())));",
                            indent
                        )?;
                        writeln!(file, "{}                        }}", indent)?;
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
                            "{}                        if length as usize > readable.remaining() {{",
                            indent
                        )?;
                        writeln!(
                            file,
                            "{}                            return Err(std::io::Error::new(std::io::ErrorKind::InvalidData,",
                            indent
                        )?;
                        writeln!(
                            file,
                            "{}                                format!(\"Tried to allocate a collection of size {{}}, but there are only {{}} bytes remaining.\", length, readable.remaining())));",
                            indent
                        )?;
                        writeln!(file, "{}                        }}", indent)?;
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
                    if nullable {
                        // For nullable tagged structs, read a varint presence indicator first.
                        // If <= 0, the struct is null. If > 0, read the struct.
                        writeln!(
                            file,
                            "{}                    if readable.read_unsigned_varint()? <= 0 {{",
                            indent
                        )?;
                        writeln!(file, "{}                        result.{} = None;", indent, field_name)?;
                        writeln!(file, "{}                    }} else {{", indent)?;
                        writeln!(
                            file,
                            "{}                        result.{} = Some({}::read(readable, version)?);",
                            indent, field_name, struct_name
                        )?;
                        writeln!(file, "{}                    }}", indent)?;
                    } else {
                        // For non-nullable structs, read the bytes and parse the struct
                        writeln!(
                            file,
                            "{}                    let mut struct_bytes = vec![0u8; size as usize];",
                            indent
                        )?;
                        writeln!(file, "{}                    readable.read_bytes(&mut struct_bytes)?;", indent)?;
                        writeln!(
                            file,
                            "{}                    let mut struct_accessor = crate::common::protocol::ByteBufferAccessor::new(struct_bytes);",
                            indent
                        )?;
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
                // Mutable accessor for write() calls (which take &mut self)
                let tagged_accessor_mut = if nullable {
                    format!("self.{}.as_mut().unwrap()", field_name)
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
                        if nullable {
                            writeln!(file, "{}                if let Some(ref val) = self.{} {{", indent, field_name)?;
                            writeln!(file, "{}                    let bytes = val.as_bytes();", indent)?;
                            writeln!(
                                file,
                                "{}                    let string_prefix_size = crate::common::protocol::ByteUtils::size_of_unsigned_varint((bytes.len() as u32) + 1);",
                                indent
                            )?;
                            writeln!(
                                file,
                                "{}                    let size = (string_prefix_size + bytes.len() as i32) as u32;",
                                indent
                            )?;
                            writeln!(file, "{}                    writable.write_unsigned_varint(size)?;", indent)?;
                            writeln!(
                                file,
                                "{}                    writable.write_unsigned_varint((bytes.len() as u32) + 1)?;",
                                indent
                            )?;
                            writeln!(file, "{}                    writable.write_bytes(bytes)?;", indent)?;
                            writeln!(file, "{}                }} else {{", indent)?;
                            // null encoding: size = 1 (varint(0))
                            writeln!(
                                file,
                                "{}                    writable.write_unsigned_varint(1)?; // size for null",
                                indent
                            )?;
                            writeln!(
                                file,
                                "{}                    writable.write_unsigned_varint(0)?; // null",
                                indent
                            )?;
                            writeln!(file, "{}                }}", indent)?;
                        } else {
                            writeln!(file, "{}                let bytes = {}.as_bytes();", indent, tagged_accessor)?;
                            writeln!(
                                file,
                                "{}                let string_prefix_size = crate::common::protocol::ByteUtils::size_of_unsigned_varint((bytes.len() as u32) + 1);",
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
                        }
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
                            "{}                let mut size_accessor = crate::common::protocol::ByteBufferAccessor::new(Vec::with_capacity(1024));",
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
                                    "{}                for element in {}.iter_mut() {{",
                                    indent, tagged_accessor_mut
                                )?;
                                writeln!(file, "{}                    size_accessor.write_uuid(element)?;", indent)?;
                                writeln!(file, "{}                }}", indent)?;
                            },
                            FieldType::Int8 => {
                                writeln!(
                                    file,
                                    "{}                for element in {}.iter_mut() {{",
                                    indent, tagged_accessor_mut
                                )?;
                                writeln!(file, "{}                    size_accessor.write_byte(*element)?;", indent)?;
                                writeln!(file, "{}                }}", indent)?;
                            },
                            FieldType::Int16 => {
                                writeln!(
                                    file,
                                    "{}                for element in {}.iter_mut() {{",
                                    indent, tagged_accessor_mut
                                )?;
                                writeln!(file, "{}                    size_accessor.write_short(*element)?;", indent)?;
                                writeln!(file, "{}                }}", indent)?;
                            },
                            FieldType::Int32 => {
                                writeln!(
                                    file,
                                    "{}                for element in {}.iter_mut() {{",
                                    indent, tagged_accessor_mut
                                )?;
                                writeln!(file, "{}                    size_accessor.write_int(*element)?;", indent)?;
                                writeln!(file, "{}                }}", indent)?;
                            },
                            FieldType::Int64 => {
                                writeln!(
                                    file,
                                    "{}                for element in {}.iter_mut() {{",
                                    indent, tagged_accessor_mut
                                )?;
                                writeln!(file, "{}                    size_accessor.write_long(*element)?;", indent)?;
                                writeln!(file, "{}                }}", indent)?;
                            },
                            FieldType::String => {
                                writeln!(
                                    file,
                                    "{}                for element in {}.iter_mut() {{",
                                    indent, tagged_accessor_mut
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
                                    "{}                for element in {}.iter_mut() {{",
                                    indent, tagged_accessor_mut
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
                        if nullable {
                            writeln!(file, "{}                if let Some(ref val) = self.{} {{", indent, field_name)?;
                            writeln!(file, "{}                    // Calculate bytes size", indent)?;
                            writeln!(
                                file,
                                "{}                    let mut size_accessor = crate::common::protocol::ByteBufferAccessor::new(Vec::with_capacity(256));",
                                indent
                            )?;
                            writeln!(
                                file,
                                "{}                    size_accessor.write_unsigned_varint((val.len() as u32) + 1)?;",
                                indent
                            )?;
                            writeln!(file, "{}                    size_accessor.write_bytes(val)?;", indent)?;
                            writeln!(file, "{}                    let size = size_accessor.len() as u32;", indent)?;
                            writeln!(file, "{}                    writable.write_unsigned_varint(size)?;", indent)?;
                            writeln!(
                                file,
                                "{}                    writable.write_bytes(size_accessor.buffer())?;",
                                indent
                            )?;
                            writeln!(file, "{}                }} else {{", indent)?;
                            // null encoding: size = 1 (varint(0))
                            writeln!(
                                file,
                                "{}                    writable.write_unsigned_varint(1)?; // size for null",
                                indent
                            )?;
                            writeln!(
                                file,
                                "{}                    writable.write_unsigned_varint(0)?; // null",
                                indent
                            )?;
                            writeln!(file, "{}                }}", indent)?;
                        } else {
                            writeln!(file, "{}                // Calculate bytes size", indent)?;
                            writeln!(
                                file,
                                "{}                let mut size_accessor = crate::common::protocol::ByteBufferAccessor::new(Vec::with_capacity(256));",
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
                        }
                    },
                    FieldType::Struct(_) => {
                        if nullable {
                            let has_null_default =
                                matches!(field.field_default(), Some(serde_json::Value::String(s)) if s == "null");
                            if !has_null_default {
                                // Nullable struct with non-null default: need to handle both null and non-null cases.
                                // When null: write tag + size(1) + varint(0)
                                // When non-null: write tag + size(struct_size+1) + varint(1) + struct_data
                                writeln!(file, "{}                if self.{}.is_none() {{", indent, field_name)?;
                                writeln!(
                                    file,
                                    "{}                    writable.write_unsigned_varint(1)?; // size = 1 byte",
                                    indent
                                )?;
                                writeln!(
                                    file,
                                    "{}                    writable.write_unsigned_varint(0)?; // null presence indicator",
                                    indent
                                )?;
                                writeln!(file, "{}                }} else {{", indent)?;
                                writeln!(
                                    file,
                                    "{}                    // Calculate struct size (with presence indicator)",
                                    indent
                                )?;
                                writeln!(
                                    file,
                                    "{}                    let mut size_accessor = crate::common::protocol::ByteBufferAccessor::new(Vec::with_capacity(256));",
                                    indent
                                )?;
                                writeln!(
                                    file,
                                    "{}                    size_accessor.write_unsigned_varint(1)?; // non-null presence indicator",
                                    indent
                                )?;
                                writeln!(
                                    file,
                                    "{}                    {}.write(&mut size_accessor, version)?;",
                                    indent, tagged_accessor_mut
                                )?;
                                writeln!(file, "{}                    let size = size_accessor.len() as u32;", indent)?;
                                writeln!(file, "{}                    writable.write_unsigned_varint(size)?;", indent)?;
                                writeln!(
                                    file,
                                    "{}                    writable.write_bytes(size_accessor.buffer())?;",
                                    indent
                                )?;
                                writeln!(file, "{}                }}", indent)?;
                            } else {
                                // Nullable struct with null default: only written when non-null
                                writeln!(
                                    file,
                                    "{}                // Calculate struct size (with presence indicator)",
                                    indent
                                )?;
                                writeln!(
                                    file,
                                    "{}                let mut size_accessor = crate::common::protocol::ByteBufferAccessor::new(Vec::with_capacity(256));",
                                    indent
                                )?;
                                writeln!(
                                    file,
                                    "{}                size_accessor.write_unsigned_varint(1)?; // non-null presence indicator",
                                    indent
                                )?;
                                writeln!(
                                    file,
                                    "{}                {}.write(&mut size_accessor, version)?;",
                                    indent, tagged_accessor_mut
                                )?;
                                writeln!(file, "{}                let size = size_accessor.len() as u32;", indent)?;
                                writeln!(file, "{}                writable.write_unsigned_varint(size)?;", indent)?;
                                writeln!(
                                    file,
                                    "{}                writable.write_bytes(size_accessor.buffer())?;",
                                    indent
                                )?;
                            }
                        } else {
                            // Non-nullable struct
                            writeln!(file, "{}                // Calculate struct size", indent)?;
                            writeln!(
                                file,
                                "{}                let mut size_accessor = crate::common::protocol::ByteBufferAccessor::new(Vec::with_capacity(256));",
                                indent
                            )?;
                            writeln!(
                                file,
                                "{}                {}.write(&mut size_accessor, version)?;",
                                indent, tagged_accessor_mut
                            )?;
                            writeln!(file, "{}                let size = size_accessor.len() as u32;", indent)?;
                            writeln!(file, "{}                writable.write_unsigned_varint(size)?;", indent)?;
                            writeln!(file, "{}                writable.write_bytes(size_accessor.buffer())?;", indent)?;
                        }
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
            let effective_flex = field_flexible_versions(field, flexible_versions);
            generate_field_read(file, field, effective_flex)?;
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

/// `parent_versions` mirrors Java's `generateClassWriter(className, struct, parentVersions)`
/// (`MessageDataGenerator.java:702-703`). Only the non-ignorable-field guard consults it,
/// via `curVersions = parentVersions.intersect(struct.versions())` (`:718`).
fn generate_write_method(
    file: &mut fs::File,
    class_name: &str,
    struct_spec: &StructSpec,
    flexible_versions: Versions,
    parent_versions: Versions,
) -> Result<(), Box<dyn std::error::Error>> {
    writeln!(
        file,
        "    pub fn write(&mut self, writable: &mut dyn Writable, version: i16) -> std::io::Result<()> {{"
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

    // Generate write for each non-tagged field.
    //
    // Java walks *every* field here and wraps each one in a version conditional whose
    // `else` half carries the non-ignorable guard (`MessageDataGenerator.java:720-798`).
    // This generator writes tagged fields from a separate block further down, so a
    // tagged field only contributes its guard at this point in field order — which is
    // where Java emits it too.
    let struct_versions = struct_spec.versions().intersect(parent_versions);
    for field in struct_spec.fields() {
        if field.tagged_versions().empty() {
            let effective_flex = field_flexible_versions(field, flexible_versions);
            generate_field_write(file, field, effective_flex, struct_versions)?;
        } else if non_ignorable_check_applies(field, struct_versions) {
            let field_name = escape_rust_keyword(&to_snake_case(field.name()));
            let versions = field.versions();
            let unsupported =
                if versions.lowest() > struct_versions.lowest() && versions.highest() < struct_versions.highest() {
                    format!("(version < {} || version > {})", versions.lowest(), versions.highest())
                } else if versions.lowest() > struct_versions.lowest() {
                    format!("version < {}", versions.lowest())
                } else {
                    format!("version > {}", versions.highest())
                };
            writeln!(
                file,
                "        if {} && ({}) {{",
                unsupported,
                get_default_check(field, &field_name)
            )?;
            generate_non_ignorable_field_error(file, field, "            ")?;
            writeln!(file, "        }}")?;
            writeln!(file)?;
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
        FieldType::Bytes => {
            generate_bytes_read(file, &field_name, flexible_versions, indent, nullable)?;
        },
        FieldType::Records => {
            generate_records_read(file, &field_name, flexible_versions, indent, nullable)?;
        },
        FieldType::Array(element_type) => {
            generate_array_read(file, &field_name, element_type, flexible_versions, indent, nullable)?;
        },
        FieldType::Struct(struct_name) => {
            if nullable {
                // For nullable struct fields, Java uses a presence byte:
                // byte < 0 means null, byte >= 0 means struct data follows.
                // The nullable_versions determine when the presence byte is used.
                let nullable_versions = field.nullable_versions();
                if nullable_versions.lowest() == 0 {
                    // All versions are nullable: always read presence byte
                    writeln!(file, "{}if readable.read_byte()? < 0 {{", indent)?;
                    writeln!(file, "{}    result.{} = None;", indent, field_name)?;
                    writeln!(file, "{}}} else {{", indent)?;
                    writeln!(
                        file,
                        "{}    result.{} = Some({}::read(readable, version)?);",
                        indent, field_name, struct_name
                    )?;
                    writeln!(file, "{}}}", indent)?;
                } else {
                    // Version-specific nullability: only read presence byte in nullable versions
                    writeln!(file, "{}if version >= {} {{", indent, nullable_versions.lowest())?;
                    writeln!(file, "{}    if readable.read_byte()? < 0 {{", indent)?;
                    writeln!(file, "{}        result.{} = None;", indent, field_name)?;
                    writeln!(file, "{}    }} else {{", indent)?;
                    writeln!(
                        file,
                        "{}        result.{} = Some({}::read(readable, version)?);",
                        indent, field_name, struct_name
                    )?;
                    writeln!(file, "{}    }}", indent)?;
                    writeln!(file, "{}}} else {{", indent)?;
                    writeln!(
                        file,
                        "{}    result.{} = Some({}::read(readable, version)?);",
                        indent, field_name, struct_name
                    )?;
                    writeln!(file, "{}}}", indent)?;
                }
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
            writeln!(file, "{}        let bytes = readable.read_array(length as usize)?;", indent)?;
            writeln!(file, "{}        result.{} = Some(bytes);", indent, field_name)?;
            writeln!(file, "{}    }}", indent)?;
        } else {
            writeln!(file, "{}    let length = if len == 0 {{ 0 }} else {{ len - 1 }};", indent)?;
            writeln!(file, "{}    let bytes = readable.read_array(length as usize)?;", indent)?;
            writeln!(file, "{}    result.{} = bytes;", indent, field_name)?;
        }
        writeln!(file, "{}}} else {{", indent)?;
        writeln!(file, "{}    let len = readable.read_int()?;", indent)?;
        if nullable {
            writeln!(file, "{}    if len < 0 {{", indent)?;
            writeln!(file, "{}        result.{} = None;", indent, field_name)?;
            writeln!(file, "{}    }} else {{", indent)?;
            writeln!(file, "{}        let bytes = readable.read_array(len as usize)?;", indent)?;
            writeln!(file, "{}        result.{} = Some(bytes);", indent, field_name)?;
            writeln!(file, "{}    }}", indent)?;
        } else {
            writeln!(file, "{}    let length = if len < 0 {{ 0 }} else {{ len as u32 }};", indent)?;
            writeln!(file, "{}    let bytes = readable.read_array(length as usize)?;", indent)?;
            writeln!(file, "{}    result.{} = bytes;", indent, field_name)?;
        }
        writeln!(file, "{}}}", indent)?;
    } else {
        writeln!(file, "{}let len = readable.read_int()?;", indent)?;
        if nullable {
            writeln!(file, "{}if len < 0 {{", indent)?;
            writeln!(file, "{}    result.{} = None;", indent, field_name)?;
            writeln!(file, "{}}} else {{", indent)?;
            writeln!(file, "{}    let bytes = readable.read_array(len as usize)?;", indent)?;
            writeln!(file, "{}    result.{} = Some(bytes);", indent, field_name)?;
            writeln!(file, "{}}}", indent)?;
        } else {
            writeln!(file, "{}let length = if len < 0 {{ 0 }} else {{ len as u32 }};", indent)?;
            writeln!(file, "{}let bytes = readable.read_array(length as usize)?;", indent)?;
            writeln!(file, "{}result.{} = {}bytes{};", indent, field_name, some_wrap, some_close)?;
        }
    }
    Ok(())
}

/// Generate read code for a `records` field, handling nullable fields.
///
/// Identical in structure to [`generate_bytes_read`] but reads via
/// [`Readable::read_bytes_owned`], which returns a zero-copy refcounted
/// [`bytes::Bytes`] slice of the source buffer when the reader supports it
/// (e.g. `BytesReader` on the receive path — consumer-threading.md §27).
fn generate_records_read(
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
            writeln!(
                file,
                "{}        let bytes = readable.read_bytes_owned(length as usize)?;",
                indent
            )?;
            writeln!(file, "{}        result.{} = Some(bytes);", indent, field_name)?;
            writeln!(file, "{}    }}", indent)?;
        } else {
            writeln!(file, "{}    let length = if len == 0 {{ 0 }} else {{ len - 1 }};", indent)?;
            writeln!(file, "{}    let bytes = readable.read_bytes_owned(length as usize)?;", indent)?;
            writeln!(file, "{}    result.{} = bytes;", indent, field_name)?;
        }
        writeln!(file, "{}}} else {{", indent)?;
        writeln!(file, "{}    let len = readable.read_int()?;", indent)?;
        if nullable {
            writeln!(file, "{}    if len < 0 {{", indent)?;
            writeln!(file, "{}        result.{} = None;", indent, field_name)?;
            writeln!(file, "{}    }} else {{", indent)?;
            writeln!(file, "{}        let bytes = readable.read_bytes_owned(len as usize)?;", indent)?;
            writeln!(file, "{}        result.{} = Some(bytes);", indent, field_name)?;
            writeln!(file, "{}    }}", indent)?;
        } else {
            writeln!(file, "{}    let length = if len < 0 {{ 0 }} else {{ len as u32 }};", indent)?;
            writeln!(file, "{}    let bytes = readable.read_bytes_owned(length as usize)?;", indent)?;
            writeln!(file, "{}    result.{} = bytes;", indent, field_name)?;
        }
        writeln!(file, "{}}}", indent)?;
    } else {
        writeln!(file, "{}let len = readable.read_int()?;", indent)?;
        if nullable {
            writeln!(file, "{}if len < 0 {{", indent)?;
            writeln!(file, "{}    result.{} = None;", indent, field_name)?;
            writeln!(file, "{}}} else {{", indent)?;
            writeln!(file, "{}    let bytes = readable.read_bytes_owned(len as usize)?;", indent)?;
            writeln!(file, "{}    result.{} = Some(bytes);", indent, field_name)?;
            writeln!(file, "{}}}", indent)?;
        } else {
            writeln!(file, "{}let length = if len < 0 {{ 0 }} else {{ len as u32 }};", indent)?;
            writeln!(file, "{}let bytes = readable.read_bytes_owned(length as usize)?;", indent)?;
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
        // Bounds check: validate array length against remaining bytes before allocating,
        // to prevent OOM from malicious messages with huge array lengths.
        writeln!(file, "{}if length as usize > readable.remaining() {{", ind)?;
        writeln!(
            file,
            "{}    return Err(std::io::Error::new(std::io::ErrorKind::InvalidData,",
            ind
        )?;
        writeln!(
            file,
            "{}        format!(\"Tried to allocate a collection of size {{}}, but there are only {{}} bytes remaining.\", length, readable.remaining())));",
            ind
        )?;
        writeln!(file, "{}}}", ind)?;
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
    struct_versions: Versions,
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

    // `Writable::write_records` takes `Bytes` by value, so a records field needs an
    // owned handle. Use `.clone()`, NOT `.take()`: cloning a `bytes::Bytes` bumps a
    // reference count and copies no payload, so it is already zero-copy, whereas
    // `.take()` leaves `None` behind and so *mutates the message as a side effect of
    // serialising it*. Java's `write` never modifies the message, and the difference
    // is observable — a second `write` of the same object emitted a null record set,
    // and `size()` computed after a `write` disagreed with the bytes written.
    // Covered by `records_serde_test::test_null_and_empty_records_are_distinct_on_the_wire`.
    // All other nullable fields use `ref mut` for mutable access.
    let is_records = matches!(field.field_type(), FieldType::Records);
    let (inner_indent, accessor) = if nullable {
        if is_records {
            writeln!(file, "{}if let Some(_nv) = self.{}.clone() {{", indent, field_name)?;
        } else {
            writeln!(file, "{}if let Some(ref mut _nv) = self.{} {{", indent, field_name)?;
        }
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
        FieldType::Bytes => {
            generate_bytes_length_prefix_write(file, &accessor, flexible_versions, ind)?;
            writeln!(file, "{}writable.write_bytes(&{})?;", ind, accessor)?;
        },
        FieldType::Records => {
            // Records use write_records for zero-copy scatter-gather I/O.
            // The length prefix is written into the main buffer; the data
            // is moved into a separate scatter-gather buffer.
            if nullable {
                // accessor is _nv, a `Bytes` handle cloned from the field (see the
                // accessor comment: clone, not take, so `write` has no side effect).
                generate_bytes_length_prefix_write(file, &accessor, flexible_versions, ind)?;
                writeln!(file, "{}writable.write_records({})?;", ind, accessor)?;
            } else {
                // Non-nullable: clone the owned handle rather than `std::mem::take`.
                // A `bytes::Bytes` clone is a reference-count bump that copies no
                // payload (still zero-copy per CLAUDE.md §12), and — unlike
                // `std::mem::take` — it does NOT mutate the message as a side effect
                // of serialising, mirroring the nullable path above. `std::mem::take`
                // left the field empty after a `write`, so a second `write` emitted an
                // empty record set and a `size()` computed after a `write` disagreed
                // with the bytes written (the same bug the nullable path was fixed to
                // avoid; `records` is the sole non-nullable case today —
                // `FetchSnapshotResponse.UnalignedRecords`).
                writeln!(file, "{}let _records_data = {}.clone();", ind, accessor)?;
                generate_bytes_length_prefix_write(file, "_records_data", flexible_versions, ind)?;
                writeln!(file, "{}writable.write_records(_records_data)?;", ind)?;
            }
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
            writeln!(file, "{}for element in {}.iter_mut() {{", ind, accessor)?;
            generate_array_element_write(file, element_type.as_ref(), flexible_versions)?;
            writeln!(file, "{}}}", ind)?;
        },
        FieldType::Struct(_) => {
            if nullable {
                // For nullable struct fields, write a presence byte before the struct.
                // In nullable versions: write byte(1) for non-null, byte(-1) for null.
                // In non-nullable versions: write struct directly (no presence byte).
                let nullable_versions = field.nullable_versions();
                if nullable_versions.lowest() == 0 {
                    // All versions are nullable: always write presence byte
                    writeln!(file, "{}writable.write_byte(1)?; // non-null presence byte", ind)?;
                } else {
                    writeln!(file, "{}if version >= {} {{", ind, nullable_versions.lowest())?;
                    writeln!(file, "{}    writable.write_byte(1)?; // non-null presence byte", ind)?;
                    writeln!(file, "{}}}", ind)?;
                }
            }
            writeln!(file, "{}{}.write(writable, version)?;", ind, accessor)?;
        },
    }

    // Close nullable wrapper with null marker in else branch
    if nullable {
        writeln!(file, "{}}} else {{", indent)?;
        if matches!(field.field_type(), FieldType::Struct(_)) {
            // For nullable struct fields, write presence byte in nullable versions,
            // and error in non-nullable versions (matching Java's NullPointerException).
            let nullable_versions = field.nullable_versions();
            let inner = format!("{}    ", indent);
            if nullable_versions.lowest() == 0 {
                // All versions are nullable: always write null presence byte
                writeln!(file, "{}writable.write_byte(-1)?; // null struct presence byte", inner)?;
            } else {
                writeln!(file, "{}if version >= {} {{", inner, nullable_versions.lowest())?;
                writeln!(file, "{}    writable.write_byte(-1)?; // null struct presence byte", inner)?;
                writeln!(file, "{}}} else {{", inner)?;
                writeln!(
                    file,
                    "{}    return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, \"Null value for non-nullable struct field\"));",
                    inner
                )?;
                writeln!(file, "{}}}", inner)?;
            }
        } else {
            generate_null_write(file, field.field_type(), flexible_versions, indent)?;
        }
        writeln!(file, "{}}}", indent)?;
    }

    if has_version_check {
        // The `else` half of Java's field version conditional: refuse to encode a value
        // the requested version has no room for, instead of dropping it.
        // `MessageDataGenerator.java:792-797`.
        if non_ignorable_check_applies(field, struct_versions) {
            writeln!(file, "        }} else if {} {{", get_default_check(field, &field_name))?;
            generate_non_ignorable_field_error(file, field, "            ")?;
        }
        writeln!(file, "        }}")?;
    }
    writeln!(file)?;

    Ok(())
}

/// Generate the length prefix for a bytes/records field write.
///
/// The `accessor` is an expression that has `.len()` (e.g. `_nv` for `Vec<u8>`,
/// or `_records_len` for a pre-computed length variable).
fn generate_bytes_length_prefix_write(
    file: &mut fs::File,
    accessor: &str,
    flexible_versions: Versions,
    indent: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    if !flexible_versions.empty() {
        if flexible_versions.lowest() == 0 {
            writeln!(
                file,
                "{}writable.write_unsigned_varint(({}.len() as u32) + 1)?;",
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
                "{}    writable.write_unsigned_varint(({}.len() as u32) + 1)?;",
                indent, accessor
            )?;
            writeln!(file, "{}}} else {{", indent)?;
            writeln!(file, "{}    writable.write_int({}.len() as i32)?;", indent, accessor)?;
            writeln!(file, "{}}}", indent)?;
        }
    } else {
        writeln!(file, "{}writable.write_int({}.len() as i32)?;", indent, accessor)?;
    }
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
        FieldType::Struct(_) => {
            // For nullable struct fields, write a null presence byte.
            // In nullable versions: write byte(-1) to indicate null.
            // In non-nullable versions: this is an error (Java throws NullPointerException).
            // Since we can't easily access nullable_versions here, we write byte(-1)
            // unconditionally. The version check is handled by the caller's nullable wrapper.
            writeln!(file, "{}writable.write_byte(-1)?; // null struct presence byte", inner)?;
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

/// Map a FieldType to a SchemaType expression string for a specific (flexible, nullable) combination.
fn schema_type_for(field_type: &FieldType, flexible: bool, nullable: bool) -> String {
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
        FieldType::String => match (flexible, nullable) {
            (true, true) => "SchemaType::CompactNullableString",
            (true, false) => "SchemaType::CompactString",
            (false, true) => "SchemaType::NullableString",
            (false, false) => "SchemaType::String",
        }
        .to_string(),
        FieldType::Bytes => match (flexible, nullable) {
            (true, true) => "SchemaType::CompactNullableBytes",
            (true, false) => "SchemaType::CompactBytes",
            (false, true) => "SchemaType::NullableBytes",
            (false, false) => "SchemaType::Bytes",
        }
        .to_string(),
        FieldType::Records => {
            if flexible {
                "SchemaType::CompactRecords".to_string()
            } else {
                "SchemaType::Records".to_string()
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

/// Compute the schema type expression for a field. The result may be version-dependent
/// when flexibility or nullability boundaries fall within the field's version range,
/// producing an inline if/else expression.
fn schema_type_expr_for_field(field: &FieldSpec, flexible_versions: Versions) -> String {
    let field_type = field.field_type();
    let nullable_versions = field.nullable_versions();
    let v_low = field.versions().lowest();
    let v_high = field.versions().highest();

    // Collect version boundaries where (flexible, nullable) may change
    let mut breakpoints = vec![v_low];
    if !flexible_versions.empty() {
        let fl = flexible_versions.lowest();
        if fl > v_low && fl <= v_high {
            breakpoints.push(fl);
        }
    }
    if !nullable_versions.empty() {
        let nl = nullable_versions.lowest();
        if nl > v_low && nl <= v_high {
            breakpoints.push(nl);
        }
        let nh = nullable_versions.highest();
        if nh < v_high {
            breakpoints.push(nh + 1);
        }
    }
    breakpoints.sort();
    breakpoints.dedup();

    // Compute (start_version, schema_type) for each region, deduplicating adjacent identical types
    let mut regions: Vec<(i16, String)> = Vec::new();
    for &bp in &breakpoints {
        let flexible = !flexible_versions.empty() && flexible_versions.contains(bp);
        let nullable = !nullable_versions.empty() && nullable_versions.contains(bp);
        let st = schema_type_for(field_type, flexible, nullable);
        if regions.last().is_none_or(|(_, last_st)| *last_st != st) {
            regions.push((bp, st));
        }
    }

    if regions.len() == 1 {
        regions.into_iter().next().unwrap().1
    } else {
        // Build nested if/else: last region becomes outermost if, first becomes else
        let mut expr = regions[0].1.clone();
        for (start, st) in regions.iter().skip(1) {
            expr = format!("if version >= {} {{ {} }} else {{ {} }}", start, st, expr);
        }
        expr
    }
}

/// Returns true if the schema type for this field varies by version.
fn field_has_version_dependent_schema_type(field: &FieldSpec, flexible_versions: Versions) -> bool {
    let v_low = field.versions().lowest();
    let v_high = field.versions().highest();

    // Check if flexibility boundary falls within field's version range
    if !flexible_versions.empty() {
        let fl = flexible_versions.lowest();
        if fl > v_low && fl <= v_high {
            match field.field_type() {
                FieldType::String | FieldType::Bytes | FieldType::Records | FieldType::Array(_) => return true,
                _ => {},
            }
        }
    }

    // Check if nullability boundary falls within field's version range
    let nullable_versions = field.nullable_versions();
    if !nullable_versions.empty() {
        let nl = nullable_versions.lowest();
        let nh = nullable_versions.highest();
        if (nl > v_low && nl <= v_high) || (nh < v_high) {
            match field.field_type() {
                FieldType::String | FieldType::Bytes => return true,
                _ => {},
            }
        }
    }

    false
}

/// Generate a `schema(version) -> Schema` method that returns the schema for each version.
fn generate_schema_method(
    file: &mut fs::File,
    struct_spec: &StructSpec,
    flexible_versions: Versions,
) -> Result<(), Box<dyn std::error::Error>> {
    let lowest = struct_spec.versions().lowest();
    let highest = struct_spec.versions().highest();

    // The version parameter is needed if any non-tagged field has a version range
    // that doesn't cover all versions, or if any field's schema type varies by version
    let needs_version_param = struct_spec.fields().iter().any(|f| {
        if f.tagged_versions() != Versions::NONE && f.tagged_versions() == f.versions() {
            return false; // entirely tagged, skip
        }
        let v_low = f.versions().lowest();
        let v_high = f.versions().highest();
        if !(v_low <= lowest && v_high >= highest) {
            return true;
        }
        field_has_version_dependent_schema_type(f, flexible_versions)
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
            let tagged = field.tagged_versions();
            let versions = field.versions();

            if tagged == versions {
                // Entirely tagged — skip from regular schema fields
                continue;
            }
        }

        let v_low = field.versions().lowest();
        let v_high = field.versions().highest();
        let schema_type_expr = schema_type_expr_for_field(field, flexible_versions);
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
        // The `records` wire field is a zero-copy refcounted buffer so the
        // FetchResponse payload (and the producer batch on the write path) can
        // travel without an intermediate copy (consumer-threading.md §27).
        FieldType::Records => "Bytes".to_string(),
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
        // A `records` field defaults to null *unconditionally* in Java —
        // `FieldSpec.fieldDefault` has a bare `else if (type.isRecords()) return
        // "null";` (FieldSpec.java:453-454) with no nullability or explicit-default
        // check, unlike the `isBytes()` branch immediately above it. Handled before
        // everything else so the asymmetry is explicit: `records` is NOT `bytes`
        // here, even though the two share an arm nearly everywhere else in this
        // generator. Getting this wrong makes an unset record set encode as a
        // zero-length buffer where Java encodes null (-1).
        if matches!(field.field_type(), FieldType::Records) {
            return "None".to_string();
        }
        if let Some(serde_json::Value::String(s)) = default
            && s == "null"
        {
            return "None".to_string();
        }
        // Nullable with no explicit default: Java defaults to non-null empty values
        // for String, Bytes, Struct and Array. Only fields with an explicit
        // "default": "null" in the JSON spec default to null/None.
        //
        // The array arm is the same rule CLAUDE.md §2 states for string/bytes, and it
        // comes from the same place: `FieldSpec.fieldDefault`'s `type.isArray()` branch
        // returns `new <List>(0)` and only returns `"null"` when the spec asks for it
        // (`FieldSpec.java:465-475`) — `validateNullDefault()` is reached *only* on that
        // explicit path. Defaulting these to `None` instead made a default-constructed
        // message encode a null array where Java encodes an empty one, and made Java's
        // own `MessageTest.testOffsetFetchRequestVersions` (which leaves `Topics` unset
        // at v8+) trip the non-default-at-unsupported-version guard.
        if default.is_none() {
            match field.field_type() {
                FieldType::String => return "Some(String::new())".to_string(),
                FieldType::Bytes => return "Some(Vec::new())".to_string(),
                FieldType::Array(_) => return "Some(Vec::new())".to_string(),
                FieldType::Struct(struct_name) => {
                    return format!("Some({}::new())", struct_name);
                },
                // No spec in either corpus declares a nullable field of any other type,
                // so this arm is unreachable; Java has no `null` default for them either.
                _ => return "None".to_string(),
            }
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
                        FieldType::Bytes => return "Vec::new()".to_string(),
                        FieldType::Records => return "Bytes::new()".to_string(),
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
                    FieldType::Float64 if s.parse::<f64>().is_ok() => {
                        return s.to_string();
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
        FieldType::Bytes => "Vec::new()".to_string(),
        FieldType::Records => "Bytes::new()".to_string(),
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

fn generate_mod_file(
    spec_files: &[PathBuf],
    generated_modules: &[(String, String)],
    output_dir: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
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

    // Re-export each module's top-level message struct at the parent. The crate root
    // globs this module (`pub use generated::*;`), so callers reach the type as
    // `confluent_kafka::ProduceRequestData` rather than through the per-message module
    // (CLAUDE.md §2: import through the parent re-export, not the file module path).
    //
    // The modules themselves stay public, unlike the hand-written file modules: their
    // *nested* struct names collide across modules (`PartitionData` is declared in 20
    // of them), so the module path is the only qualifier those types have — which is
    // exactly the Java outer-class qualifier §2 reserves the submodule path for.
    //
    // A spec that fails to parse gets a stub file and no entry here, so no re-export
    // is emitted for a module that has no top-level struct to re-export.
    writeln!(file)?;
    for (module_name, data_class_name) in generated_modules {
        writeln!(file, "pub use {}::{};", module_name, data_class_name)?;
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

    fn field(json: &str) -> FieldSpec {
        let mut spec: FieldSpec = serde_json::from_str(json).expect("valid field spec");
        spec.validate().expect("field spec validates");
        spec
    }

    fn versions(lowest: i16, highest: i16) -> Versions {
        Versions::new(lowest, highest).expect("valid range")
    }

    /// Java names the field in the guard message with `camelCaseName()`
    /// (`FieldSpec.java:188-190`, used at `:661`), not the snake_case Rust identifier,
    /// so the emitted text is byte-identical to the Java client's for the same field.
    #[test]
    fn test_camel_case_name_matches_java_lower_case_first() {
        let name = |n: &str| field(&format!(r#"{{ "name": "{n}", "type": "int32", "versions": "0+" }}"#));
        assert_eq!(name("ProducerId").camel_case_name(), "producerId");
        assert_eq!(name("Enable2Pc").camel_case_name(), "enable2Pc");
        assert_eq!(name("KeepPreparedTxn").camel_case_name(), "keepPreparedTxn");
        // Already-camelCase spec names (the test corpus uses these) are unchanged.
        assert_eq!(name("processId").camel_case_name(), "processId");
    }

    /// The `"ignorable"` flag is the whole gate: Java emits the guard only under
    /// `if (!field.ignorable())` (`MessageDataGenerator.java:792`), and adding it to an
    /// ignorable field would start rejecting encodings Java accepts
    /// (`.claude/rules/producer-transactions.md` §11).
    #[test]
    fn test_non_ignorable_check_skips_ignorable_fields() {
        // InitProducerIdRequest.ProducerId: v3+ in a 0-6 message, not ignorable.
        let producer_id = field(r#"{ "name": "ProducerId", "type": "int64", "versions": "3+", "default": "-1" }"#);
        assert!(non_ignorable_check_applies(&producer_id, versions(0, 6)));

        // TxnOffsetCommitRequest.CommittedLeaderEpoch: v2+ but "ignorable": true.
        let leader_epoch = field(
            r#"{ "name": "CommittedLeaderEpoch", "type": "int32", "versions": "2+",
                 "default": "-1", "ignorable": true }"#,
        );
        assert!(!non_ignorable_check_applies(&leader_epoch, versions(0, 5)));
    }

    /// Java only emits the `else` half when `possibleVersions - containingVersions` is
    /// non-empty (`VersionConditional.java:189-218`). A field spanning the whole struct
    /// range has no unsupported version to guard against.
    #[test]
    fn test_non_ignorable_check_needs_a_reachable_unsupported_version() {
        let all_versions = field(r#"{ "name": "GroupId", "type": "string", "versions": "0+" }"#);
        assert!(!non_ignorable_check_applies(&all_versions, versions(0, 6)));

        // The nested-struct case: `ListOffsetsResponse` is 1-11 and its `Partitions`
        // struct is declared "0+", but `Timestamp` is "1+" — so once the struct range is
        // intersected with the parent's, v0 is unreachable and Java emits no guard.
        let timestamp = field(r#"{ "name": "Timestamp", "type": "int64", "versions": "1+", "default": "-1" }"#);
        assert!(!non_ignorable_check_applies(&timestamp, versions(1, 11)));
        // Without that intersection the same field would be guarded — this is the
        // difference `generate_write_method`'s `parent_versions` argument exists for.
        assert!(non_ignorable_check_applies(&timestamp, versions(0, 11)));

        // A field removed in later versions is guarded on the upper side too.
        let replica_id = field(r#"{ "name": "ReplicaId", "type": "int32", "versions": "0-14", "default": "-1" }"#);
        assert!(non_ignorable_check_applies(&replica_id, versions(0, 18)));
    }

    /// `get_default_check` mirrors `FieldSpec.generateNonDefaultValueCheck`
    /// (`FieldSpec.java:587-641`), which Java reuses verbatim for both the tagged-field
    /// "should this be written?" test and this guard.
    #[test]
    fn test_get_default_check_mirrors_java_per_type() {
        // Numeric: `field != <default>`.
        let producer_id = field(r#"{ "name": "ProducerId", "type": "int64", "versions": "3+", "default": "-1" }"#);
        assert_eq!(get_default_check(&producer_id, "producer_id"), "self.producer_id != -1");

        // Bool: Java writes a bare `if (field)` / `if (!field)` rather than `!= false`.
        let enable_2pc = field(r#"{ "name": "Enable2Pc", "type": "bool", "versions": "6+", "default": "false" }"#);
        assert_eq!(get_default_check(&enable_2pc, "enable2_pc"), "self.enable2_pc");
        let auto_create =
            field(r#"{ "name": "AllowAutoTopicCreation", "type": "bool", "versions": "4+", "default": "true" }"#);
        assert_eq!(
            get_default_check(&auto_create, "allow_auto_topic_creation"),
            "!self.allow_auto_topic_creation"
        );

        // Explicit `"default": "null"` — Java's `field != null`.
        let instance_id = field(
            r#"{ "name": "GroupInstanceId", "type": "string", "versions": "3+",
                 "nullableVersions": "3+", "default": "null" }"#,
        );
        assert_eq!(
            get_default_check(&instance_id, "group_instance_id"),
            "self.group_instance_id.is_some()"
        );

        // Nullable without an explicit null default — Java's `field == null || !field.isEmpty()`.
        // Null counts as *non*-default here, which is why the field's own default has to
        // be the empty collection (see `get_default_value_for_field`).
        let topics = field(
            r#"{ "name": "Topics", "type": "[]OffsetFetchRequestTopic", "versions": "0-7",
                 "nullableVersions": "2-7" }"#,
        );
        assert_eq!(
            get_default_check(&topics, "topics"),
            "self.topics.as_ref().map_or(true, |v| !v.is_empty())"
        );

        // Non-nullable string with no default — Java's `!field.equals("")`.
        let member_id = field(r#"{ "name": "MemberId", "type": "string", "versions": "3+", "default": "" }"#);
        assert_eq!(get_default_check(&member_id, "member_id"), "!self.member_id.is_empty()");

        // Non-nullable array — Java's `!field.isEmpty()`.
        let keys = field(r#"{ "name": "CoordinatorKeys", "type": "[]string", "versions": "4+" }"#);
        assert_eq!(
            get_default_check(&keys, "coordinator_keys"),
            "!self.coordinator_keys.is_empty()"
        );

        // Struct — Java's `!field.equals(new Struct())`.
        let leader = field(r#"{ "name": "CurrentLeader", "type": "LeaderIdAndEpoch", "versions": "12+" }"#);
        assert_eq!(
            get_default_check(&leader, "current_leader"),
            "self.current_leader != LeaderIdAndEpoch::new()"
        );
    }

    /// Generates a nested struct into a scratch file and returns the emitted Rust.
    fn emit_nested_struct(field: &FieldSpec, parent_versions: Versions) -> String {
        let path = std::env::temp_dir().join(format!(
            "ckr-gen-{}-{}-{}.rs",
            std::process::id(),
            parent_versions.lowest(),
            parent_versions.highest()
        ));
        {
            let mut file = fs::File::create(&path).expect("create scratch file");
            generate_nested_struct(&mut file, field, Versions::NONE, parent_versions).expect("generate");
        }
        let emitted = fs::read_to_string(&path).expect("read scratch file");
        let _ = fs::remove_file(&path);
        emitted
    }

    /// Pins the `parent_versions` threading at the **emission** level, not just in the
    /// predicate.
    ///
    /// Java hands each subclass `parentVersions.intersect(struct.versions())`
    /// (`MessageDataGenerator.java:176`, `:183`) and intersects again at `:718`, so a
    /// nested struct declared wider than its enclosing message is still only guarded
    /// over versions the message can reach. This is the `ListOffsetsResponse` shape:
    /// the message is 1-11, its `Partitions` struct is declared `0+`, and `Timestamp`
    /// is `1+` — so v0 is unreachable and Java emits no guard.
    ///
    /// Reverting the threading (passing the field's own range as the parent) puts the
    /// guard back, which is exactly the 7-guard over-emission this argument suppressed.
    /// Nothing else in the suite fails if it regresses: the spurious guards land on
    /// versions no message-level round-trip can reach.
    #[test]
    fn test_nested_struct_guard_respects_the_enclosing_message_versions() {
        let partitions = field(
            r#"{ "name": "Partitions", "type": "[]ListOffsetsPartitionResponse", "versions": "0+",
                 "fields": [
                   { "name": "Timestamp", "type": "int64", "versions": "1+", "default": "-1" }
                 ] }"#,
        );

        let within_reach = emit_nested_struct(&partitions, versions(1, 11));
        assert!(
            !within_reach.contains("Attempted to write a non-default timestamp"),
            "v0 is unreachable from a 1-11 message, so Java emits no guard:\n{within_reach}"
        );

        let v0_reachable = emit_nested_struct(&partitions, versions(0, 11));
        assert!(
            v0_reachable.contains("Attempted to write a non-default timestamp at version"),
            "with v0 reachable the guard is required:\n{v0_reachable}"
        );
    }

    /// An unset nullable array is the **empty** list, not null: `FieldSpec.fieldDefault`
    /// returns `new <List>(0)` and only yields `"null"` on an explicit
    /// `"default": "null"` (`FieldSpec.java:465-475`).
    #[test]
    fn test_nullable_array_defaults_to_empty_not_none() {
        let implicit = field(
            r#"{ "name": "Topics", "type": "[]OffsetFetchRequestTopic", "versions": "0-7",
                 "nullableVersions": "2-7" }"#,
        );
        assert_eq!(get_default_value_for_field(&implicit), "Some(Vec::new())");

        let explicit = field(
            r#"{ "name": "Topics", "type": "[]MetadataRequestTopic", "versions": "0+",
                 "nullableVersions": "0+", "default": "null" }"#,
        );
        assert_eq!(get_default_value_for_field(&explicit), "None");
    }
}
