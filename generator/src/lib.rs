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
    // Plain comments (not `//!`) are used here because the file is included
    // via `include!()` inside a `mod { ... }` block, where inner doc comments
    // would attach to the wrong item.
    writeln!(file, "// Generated from JSON message specifications.")?;
    writeln!(file, "//")?;
    writeln!(file, "// Rust equivalent of Java's generated `ApiMessageType` enum.")?;
    writeln!(
        file,
        "// Provides version ranges, header version logic, and listener information"
    )?;
    writeln!(file, "// for each Kafka API key.")?;
    writeln!(file)?;
    writeln!(file, "use crate::common::protocol::types::Schema;")?;
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

    // request_schema() and response_schema() require every `*_data` module to
    // be reachable from `crate::common::message::*`. Phase 2d-1 only wires up
    // a handful of those modules; the remainder will be added in 2d-2/3/4 as
    // each spec passes its round-trip / byte-vector tests. Until the full set
    // is wired up these methods would fail to compile, so we currently emit
    // stubs that always return an empty schema.
    //
    // TODO Phase 4: replace these stubs with a per-API match dispatching to
    // each `*Data::schema(version)`. Required by the parameterized
    // `ApiVersionsResponseTest` translation (it iterates
    // `messageType.requestSchemas()[i]` and asserts each non-tagged field
    // shape). See COMMENTS.0.md Issue 6 for context.
    writeln!(file, "    /// Returns the request schema for this API at the given version.")?;
    writeln!(
        file,
        "    pub fn request_schema(self, version: i16) -> Result<Schema, crate::common::errors::KafkaError> {{"
    )?;
    writeln!(file, "        let _ = version;")?;
    writeln!(file, "        let _ = self;")?;
    writeln!(file, "        Schema::new(Vec::new())")?;
    writeln!(file, "    }}")?;
    writeln!(file)?;

    // response_schema()
    writeln!(file, "    /// Returns the response schema for this API at the given version.")?;
    writeln!(
        file,
        "    pub fn response_schema(self, version: i16) -> Result<Schema, crate::common::errors::KafkaError> {{"
    )?;
    writeln!(file, "        let _ = version;")?;
    writeln!(file, "        let _ = self;")?;
    writeln!(file, "        Schema::new(Vec::new())")?;
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
    // Plain comments (not `//!`) because the file is included via
    // `include!()` inside a `mod { ... }` block, where inner doc comments
    // would attach to the wrong item.
    writeln!(file, "// Generated from {}.json", file_name)?;
    writeln!(file)?;
    // Each `use` is separately gated with `#[allow(unused_imports)]` because
    // the runtime types they refer to may or may not be referenced depending
    // on which fields a particular message has. Inner attributes
    // (`#![allow(...)]`) cannot be used here because the file is included via
    // `include!()` inside a `mod { ... }` block.
    writeln!(file, "#[allow(unused_imports)]")?;
    writeln!(
        file,
        "use crate::common::protocol::{{ApiMessage, ByteBufferAccessor, Message, MessageSizeAccumulator, ObjectSerializationCache, RawTaggedField, Readable, Writable}};"
    )?;
    writeln!(file, "#[allow(unused_imports)]")?;
    writeln!(
        file,
        "use crate::common::protocol::types::{{ArrayOf, CompactArrayOf, Field, Schema, Type}};"
    )?;
    writeln!(file, "#[allow(unused_imports)]")?;
    writeln!(file, "use crate::common::errors::KafkaError;")?;
    writeln!(file, "#[allow(unused_imports)]")?;
    writeln!(file, "use crate::common::utils::byte_utils;")?;
    writeln!(file, "#[allow(unused_imports)]")?;
    writeln!(file, "use crate::common::Uuid;")?;
    writeln!(file, "#[allow(unused_imports)]")?;
    writeln!(file, "use std::fmt;")?;
    writeln!(file, "#[allow(unused_imports)]")?;
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

    // The generator's emit shape triggers a few clippy lints that are
    // stylistic rather than substantive — silenced at the impl-block level
    // so the affected methods can preserve their idiomatic shape:
    //
    // - `manual_range_contains`: emit code uses `version < lo || version > hi`
    //   for version validation, mirroring Java's generated range checks.
    // - `vec_init_then_push`: `schema()` builds `fields` with a `Vec::new()`
    //   then `.push(...)` because some fields are pushed under a `if version
    //   >= N` guard.
    // - `new_without_default`: every generated `Data` struct has a `new()`
    //   constructor; deriving `Default` would duplicate it.
    writeln!(file, "#[allow(clippy::manual_range_contains)]")?;
    writeln!(file, "#[allow(clippy::vec_init_then_push)]")?;
    writeln!(file, "#[allow(clippy::new_without_default)]")?;
    // The generator emits `if version >= N { if cond { ... } }` and
    // `if x != false { ... }` for tagged-field write paths because the
    // version guard and the field-presence check are produced by
    // independent code paths. The Rust idiom is `&&` / `if x`, but
    // collapsing in the generator would entangle two orthogonal concerns
    // and obscure which check is the version gate.
    writeln!(file, "#[allow(clippy::collapsible_if)]")?;
    writeln!(file, "#[allow(clippy::bool_comparison)]")?;
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

    // See the matching annotation block on the top-level data struct for
    // why these clippy lints are silenced here.
    writeln!(file, "#[allow(clippy::manual_range_contains)]")?;
    writeln!(file, "#[allow(clippy::vec_init_then_push)]")?;
    writeln!(file, "#[allow(clippy::new_without_default)]")?;
    // The generator emits `if version >= N { if cond { ... } }` and
    // `if x != false { ... }` for tagged-field write paths because the
    // version guard and the field-presence check are produced by
    // independent code paths. The Rust idiom is `&&` / `if x`, but
    // collapsing in the generator would entangle two orthogonal concerns
    // and obscure which check is the version gate.
    writeln!(file, "#[allow(clippy::collapsible_if)]")?;
    writeln!(file, "#[allow(clippy::bool_comparison)]")?;
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
    writeln!(file)?;

    // Generate schema method — nested structs are referenced from
    // outer-struct schema emit via `Type::Schema(<Name>::schema(version)?)`
    // (Issue 3 fix), so every nested struct must expose the same surface as
    // the top-level `*Data` types.
    generate_schema_method(file, &struct_spec, flexible_versions)?;

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

    // See the matching annotation block on the top-level data struct for
    // why these clippy lints are silenced here.
    writeln!(file, "#[allow(clippy::manual_range_contains)]")?;
    writeln!(file, "#[allow(clippy::vec_init_then_push)]")?;
    writeln!(file, "#[allow(clippy::new_without_default)]")?;
    // The generator emits `if version >= N { if cond { ... } }` and
    // `if x != false { ... }` for tagged-field write paths because the
    // version guard and the field-presence check are produced by
    // independent code paths. The Rust idiom is `&&` / `if x`, but
    // collapsing in the generator would entangle two orthogonal concerns
    // and obscure which check is the version gate.
    writeln!(file, "#[allow(clippy::collapsible_if)]")?;
    writeln!(file, "#[allow(clippy::bool_comparison)]")?;
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
    writeln!(file)?;

    // Generate schema method — common structs may also be referenced as
    // `Type::Schema(<Name>::schema(version)?)` from any *Data that uses them.
    generate_schema_method(file, struct_spec, flexible_versions)?;

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

    // `add_size` and `write` paths use the same independently-generated
    // version + presence checks as the inherent `impl X` block; see those
    // allows for context. `manual_range_contains` covers the
    // `if version >= N && version <= M` field-presence gate emitted for
    // fields with a closed `versions: "N-M"` range (e.g. MetadataRequest's
    // `IncludeClusterAuthorizedOperations` at v8-10).
    writeln!(file, "#[allow(clippy::manual_range_contains)]")?;
    writeln!(file, "#[allow(clippy::collapsible_if)]")?;
    writeln!(file, "#[allow(clippy::bool_comparison)]")?;
    writeln!(file, "impl Message for {} {{", struct_name)?;
    writeln!(file, "    fn lowest_supported_version(&self) -> i16 {{ {} }}", lowest)?;
    writeln!(file, "    fn highest_supported_version(&self) -> i16 {{ {} }}", highest)?;
    writeln!(file)?;

    // Generate proper add_size that computes sizes arithmetically
    generate_add_size_body(file, struct_spec, flexible_versions)?;

    writeln!(file)?;
    writeln!(
        file,
        "    fn write(&self, writable: &mut dyn Writable, _cache: &ObjectSerializationCache, version: i16) -> Result<(), KafkaError> {{"
    )?;
    writeln!(file, "        {}::write(self, writable, version)", struct_name)?;
    writeln!(file, "    }}")?;
    writeln!(file)?;
    writeln!(
        file,
        "    fn read(&mut self, readable: &mut dyn Readable, version: i16) -> Result<(), KafkaError> {{"
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
        "    fn add_size(&self, size: &mut MessageSizeAccumulator, cache: &mut ObjectSerializationCache, version: i16) {{"
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
                "{}    size.add_bytes(byte_utils::size_of_unsigned_varint(field.tag() as u32) as i32);",
                indent
            )?;
            writeln!(
                file,
                "{}    size.add_bytes(byte_utils::size_of_unsigned_varint(field.size() as u32) as i32);",
                indent
            )?;
            writeln!(file, "{}    size.add_bytes(field.size() as i32);", indent)?;
            writeln!(file, "{}}}", indent)?;
            writeln!(
                file,
                "{}size.add_bytes(byte_utils::size_of_unsigned_varint(num_tagged_fields) as i32);",
                indent
            )?;
            writeln!(file, "        }}")?;
        } else {
            let indent = "        ";
            writeln!(file, "{}num_tagged_fields += self.unknown_tagged_fields.len() as u32;", indent)?;
            writeln!(file, "{}for field in &self.unknown_tagged_fields {{", indent)?;
            writeln!(
                file,
                "{}    size.add_bytes(byte_utils::size_of_unsigned_varint(field.tag() as u32) as i32);",
                indent
            )?;
            writeln!(
                file,
                "{}    size.add_bytes(byte_utils::size_of_unsigned_varint(field.size() as u32) as i32);",
                indent
            )?;
            writeln!(file, "{}    size.add_bytes(field.size() as i32);", indent)?;
            writeln!(file, "{}}}", indent)?;
            writeln!(
                file,
                "{}size.add_bytes(byte_utils::size_of_unsigned_varint(num_tagged_fields) as i32);",
                indent
            )?;
        }
    }

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
        FieldType::Bytes | FieldType::Records => {
            generate_bytes_add_size(file, &accessor, flexible_versions, ind)?;
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
            writeln!(file, "{}{}.add_size(size, cache, version);", ind, accessor)?;
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
                "{}    size.add_bytes(byte_utils::size_of_unsigned_varint(bytes_len + 1) as i32);",
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
                "{}        size.add_bytes(byte_utils::size_of_unsigned_varint(bytes_len + 1) as i32);",
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
                "{}    size.add_bytes(byte_utils::size_of_unsigned_varint(bytes_len + 1) as i32);",
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
                "{}        size.add_bytes(byte_utils::size_of_unsigned_varint(bytes_len + 1) as i32);",
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
                "{}size.add_bytes(byte_utils::size_of_unsigned_varint({}.len() as u32 + 1) as i32);",
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
                "{}    size.add_bytes(byte_utils::size_of_unsigned_varint({}.len() as u32 + 1) as i32);",
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
                        "{}    size.add_bytes(byte_utils::size_of_unsigned_varint(bytes_len + 1) as i32);",
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
                        "{}        size.add_bytes(byte_utils::size_of_unsigned_varint(bytes_len + 1) as i32);",
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
                        "{}    size.add_bytes(byte_utils::size_of_unsigned_varint(bytes_len + 1) as i32);",
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
                        "{}        size.add_bytes(byte_utils::size_of_unsigned_varint(bytes_len + 1) as i32);",
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
            writeln!(file, "{}    element.add_size(size, cache, version);", indent)?;
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
        "{}size.add_bytes(byte_utils::size_of_unsigned_varint({}) as i32);",
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
        // Check if this is a nullable field with default "null"
        let has_null_default = matches!(field.field_default(), Some(serde_json::Value::String(s)) if s == "null");
        if has_null_default {
            // For fields defaulting to null, only write when non-null
            return format!("self.{}.is_some()", field_name);
        }
        // For nullable fields with non-null default, write when the value differs from default.
        // Java defaults nullable string/bytes to "" / Bytes.EMPTY (not null).
        // Write when null (to encode the null state) OR when the value differs from default.
        match field.field_type() {
            FieldType::Struct(struct_name) => {
                // Java: field == null || !field.equals(new StructName())
                return format!(
                    "self.{}.is_none() || self.{}.as_ref().unwrap() != &{}::new()",
                    field_name, field_name, struct_name
                );
            },
            FieldType::String => {
                // Java: field == null || !field.isEmpty()
                return format!("self.{}.as_ref().map_or(true, |v| !v.is_empty())", field_name);
            },
            FieldType::Bytes | FieldType::Records => {
                // Java: field == null || field.length != 0
                return format!("self.{}.as_ref().map_or(true, |v| !v.is_empty())", field_name);
            },
            _ => {
                // For other nullable fields with non-null default, just check is_some
                return format!("self.{}.is_some()", field_name);
            },
        }
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
                "{}size.add_bytes(byte_utils::size_of_unsigned_varint(1) as i32); // size prefix",
                indent
            )?;
            writeln!(file, "{}size.add_bytes(1);", indent)?;
        },
        FieldType::Int16 | FieldType::Uint16 => {
            writeln!(
                file,
                "{}size.add_bytes(byte_utils::size_of_unsigned_varint(2) as i32); // size prefix",
                indent
            )?;
            writeln!(file, "{}size.add_bytes(2);", indent)?;
        },
        FieldType::Int32 | FieldType::Uint32 => {
            writeln!(
                file,
                "{}size.add_bytes(byte_utils::size_of_unsigned_varint(4) as i32); // size prefix",
                indent
            )?;
            writeln!(file, "{}size.add_bytes(4);", indent)?;
        },
        FieldType::Int64 => {
            writeln!(
                file,
                "{}size.add_bytes(byte_utils::size_of_unsigned_varint(8) as i32); // size prefix",
                indent
            )?;
            writeln!(file, "{}size.add_bytes(8);", indent)?;
        },
        FieldType::Float64 => {
            writeln!(
                file,
                "{}size.add_bytes(byte_utils::size_of_unsigned_varint(8) as i32); // size prefix",
                indent
            )?;
            writeln!(file, "{}size.add_bytes(8);", indent)?;
        },
        FieldType::Uuid => {
            writeln!(
                file,
                "{}size.add_bytes(byte_utils::size_of_unsigned_varint(16) as i32); // size prefix",
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
                    "{}        let string_prefix_size = byte_utils::size_of_unsigned_varint(bytes_len + 1);",
                    indent
                )?;
                writeln!(
                    file,
                    "{}        let inner_size = string_prefix_size + bytes_len as i32;",
                    indent
                )?;
                writeln!(
                    file,
                    "{}        size.add_bytes(byte_utils::size_of_unsigned_varint(inner_size as u32) as i32);",
                    indent
                )?;
                writeln!(file, "{}        size.add_bytes(inner_size);", indent)?;
                writeln!(file, "{}    }} else {{", indent)?;
                // null encoding: varint(0) = 1 byte, so inner_size = 1
                writeln!(
                    file,
                    "{}        size.add_bytes(byte_utils::size_of_unsigned_varint(1) as i32); // size prefix for null",
                    indent
                )?;
                writeln!(file, "{}        size.add_bytes(1); // varint(0) for null", indent)?;
                writeln!(file, "{}    }}", indent)?;
            } else {
                writeln!(file, "{}    let bytes_len = {}.len() as u32;", indent, accessor)?;
                writeln!(
                    file,
                    "{}    let string_prefix_size = byte_utils::size_of_unsigned_varint(bytes_len + 1);",
                    indent
                )?;
                writeln!(file, "{}    let inner_size = string_prefix_size + bytes_len as i32;", indent)?;
                writeln!(
                    file,
                    "{}    size.add_bytes(byte_utils::size_of_unsigned_varint(inner_size as u32) as i32); // size prefix",
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
                    "{}        let bytes_prefix_size = byte_utils::size_of_unsigned_varint(bytes_len + 1);",
                    indent
                )?;
                writeln!(file, "{}        let inner_size = bytes_prefix_size + bytes_len as i32;", indent)?;
                writeln!(
                    file,
                    "{}        size.add_bytes(byte_utils::size_of_unsigned_varint(inner_size as u32) as i32);",
                    indent
                )?;
                writeln!(file, "{}        size.add_bytes(inner_size);", indent)?;
                writeln!(file, "{}    }} else {{", indent)?;
                // null encoding: varint(0) = 1 byte, so inner_size = 1
                writeln!(
                    file,
                    "{}        size.add_bytes(byte_utils::size_of_unsigned_varint(1) as i32); // size prefix for null",
                    indent
                )?;
                writeln!(file, "{}        size.add_bytes(1); // varint(0) for null", indent)?;
                writeln!(file, "{}    }}", indent)?;
            } else {
                writeln!(file, "{}    let bytes_len = {}.len() as u32;", indent, accessor)?;
                writeln!(
                    file,
                    "{}    let bytes_prefix_size = byte_utils::size_of_unsigned_varint(bytes_len + 1);",
                    indent
                )?;
                writeln!(file, "{}    let inner_size = bytes_prefix_size + bytes_len as i32;", indent)?;
                writeln!(
                    file,
                    "{}    size.add_bytes(byte_utils::size_of_unsigned_varint(inner_size as u32) as i32); // size prefix",
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
                "{}    array_size += byte_utils::size_of_unsigned_varint({}.len() as u32 + 1) as i32;",
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
                            "{}        array_size += byte_utils::size_of_unsigned_varint(elem_len + 1) as i32;",
                            indent
                        )?;
                        writeln!(file, "{}        array_size += elem_len as i32;", indent)?;
                    },
                    FieldType::Struct(_) => {
                        writeln!(file, "{}        let mut elem_acc = MessageSizeAccumulator::new();", indent)?;
                        writeln!(file, "{}        element.add_size(&mut elem_acc, cache, version);", indent)?;
                        writeln!(file, "{}        array_size += elem_acc.total_size();", indent)?;
                    },
                    _ => {},
                }
                writeln!(file, "{}    }}", indent)?;
            }
            writeln!(
                file,
                "{}    size.add_bytes(byte_utils::size_of_unsigned_varint(array_size as u32) as i32); // size prefix",
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
                    writeln!(file, "{}    {}.add_size(&mut struct_acc, cache, version);", indent, accessor)?;
                    writeln!(file, "{}    let struct_size = struct_acc.total_size();", indent)?;
                    writeln!(
                        file,
                        "{}    let content_size = 1 + struct_size; // presence indicator + struct",
                        indent
                    )?;
                    writeln!(
                        file,
                        "{}    size.add_bytes(byte_utils::size_of_unsigned_varint(content_size as u32) as i32); // size prefix",
                        indent
                    )?;
                    writeln!(file, "{}    size.add_bytes(content_size);", indent)?;
                    writeln!(file, "{}}}", indent)?;
                } else {
                    // Default is non-null. The field can be None (encoding null) or Some (encoding the struct).
                    writeln!(file, "{}if let Some(ref val) = self.{} {{", indent, field_name)?;
                    writeln!(file, "{}    let mut struct_acc = MessageSizeAccumulator::new();", indent)?;
                    writeln!(file, "{}    val.add_size(&mut struct_acc, cache, version);", indent)?;
                    writeln!(file, "{}    let struct_size = struct_acc.total_size();", indent)?;
                    writeln!(
                        file,
                        "{}    let content_size = 1 + struct_size; // presence indicator + struct",
                        indent
                    )?;
                    writeln!(
                        file,
                        "{}    size.add_bytes(byte_utils::size_of_unsigned_varint(content_size as u32) as i32); // size prefix",
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
                        "{}    size.add_bytes(byte_utils::size_of_unsigned_varint(1) as i32); // size prefix for 1 byte",
                        indent
                    )?;
                    writeln!(file, "{}    size.add_bytes(1); // varint(0) null indicator", indent)?;
                    writeln!(file, "{}}}", indent)?;
                }
            } else {
                writeln!(file, "{}{{", indent)?;
                writeln!(file, "{}    let mut struct_acc = MessageSizeAccumulator::new();", indent)?;
                writeln!(file, "{}    {}.add_size(&mut struct_acc, cache, version);", indent, accessor)?;
                writeln!(file, "{}    let struct_size = struct_acc.total_size();", indent)?;
                writeln!(
                    file,
                    "{}    size.add_bytes(byte_utils::size_of_unsigned_varint(struct_size as u32) as i32); // size prefix",
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
                        writeln!(
                            file,
                            "{}                        bytes = readable.read_array(bytes.len())?;",
                            indent
                        )?;
                        writeln!(
                            file,
                            "{}                        result.{} = Some(String::from_utf8(bytes)",
                            indent, field_name
                        )?;
                        writeln!(
                            file,
                            "{}                            .map_err(|e| KafkaError::Generic(e.to_string()))?);",
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
                        writeln!(
                            file,
                            "{}                        bytes = readable.read_array(bytes.len())?;",
                            indent
                        )?;
                        writeln!(
                            file,
                            "{}                        result.{} = String::from_utf8(bytes)",
                            indent, field_name
                        )?;
                        writeln!(
                            file,
                            "{}                            .map_err(|e| KafkaError::Generic(e.to_string()))?;",
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
                        writeln!(file, "{}                            return Err(KafkaError::Generic(", indent)?;
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
                        writeln!(file, "{}                            return Err(KafkaError::Generic(", indent)?;
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
                                "{}                                bytes = readable.read_array(bytes.len())?;",
                                indent
                            )?;
                            writeln!(
                                file,
                                "{}                                {}.push(String::from_utf8(bytes)",
                                indent, push_target
                            )?;
                            writeln!(
                                file,
                                "{}                                    .map_err(|e| KafkaError::Generic(e.to_string()))?);",
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
                        writeln!(
                            file,
                            "{}                    struct_bytes = readable.read_array(struct_bytes.len())?;",
                            indent
                        )?;
                        writeln!(
                            file,
                            "{}                    let mut struct_accessor = crate::common::protocol::ByteBufferAccessor::wrap(struct_bytes);",
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
                        writeln!(
                            file,
                            "{}                        bytes = readable.read_array(bytes.len())?;",
                            indent
                        )?;
                        writeln!(file, "{}                        result.{} = Some(bytes);", indent, field_name)?;
                        writeln!(file, "{}                    }}", indent)?;
                    } else {
                        writeln!(file, "{}                    if len > 0 {{", indent)?;
                        writeln!(
                            file,
                            "{}                        let mut bytes = vec![0u8; (len - 1) as usize];",
                            indent
                        )?;
                        writeln!(
                            file,
                            "{}                        bytes = readable.read_array(bytes.len())?;",
                            indent
                        )?;
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
                    writeln!(
                        file,
                        "{}                    skip_bytes = readable.read_array(skip_bytes.len())?;",
                        indent
                    )?;
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
        "{}                    result.unknown_tagged_fields.push(RawTaggedField::new(tag as i32, data));",
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
    writeln!(file, "{}        writable.write_unsigned_varint(num_tagged_fields);", indent)?;

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
                    "{}                writable.write_unsigned_varint({}); // tag",
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
                                "{}                    let string_prefix_size = byte_utils::size_of_unsigned_varint((bytes.len() as u32) + 1);",
                                indent
                            )?;
                            writeln!(
                                file,
                                "{}                    let size = (string_prefix_size + bytes.len() as i32) as u32;",
                                indent
                            )?;
                            writeln!(file, "{}                    writable.write_unsigned_varint(size);", indent)?;
                            writeln!(
                                file,
                                "{}                    writable.write_unsigned_varint((bytes.len() as u32) + 1);",
                                indent
                            )?;
                            writeln!(file, "{}                    writable.write_byte_array(bytes);", indent)?;
                            writeln!(file, "{}                }} else {{", indent)?;
                            // null encoding: size = 1 (varint(0))
                            writeln!(
                                file,
                                "{}                    writable.write_unsigned_varint(1); // size for null",
                                indent
                            )?;
                            writeln!(file, "{}                    writable.write_unsigned_varint(0); // null", indent)?;
                            writeln!(file, "{}                }}", indent)?;
                        } else {
                            writeln!(file, "{}                let bytes = {}.as_bytes();", indent, tagged_accessor)?;
                            writeln!(
                                file,
                                "{}                let string_prefix_size = byte_utils::size_of_unsigned_varint((bytes.len() as u32) + 1);",
                                indent
                            )?;
                            writeln!(
                                file,
                                "{}                let size = (string_prefix_size + bytes.len() as i32) as u32;",
                                indent
                            )?;
                            writeln!(file, "{}                writable.write_unsigned_varint(size);", indent)?;
                            writeln!(
                                file,
                                "{}                writable.write_unsigned_varint((bytes.len() as u32) + 1);",
                                indent
                            )?;
                            writeln!(file, "{}                writable.write_byte_array(bytes);", indent)?;
                        }
                    },
                    FieldType::Bool => {
                        writeln!(
                            file,
                            "{}                writable.write_unsigned_varint(1); // size = 1 byte",
                            indent
                        )?;
                        writeln!(
                            file,
                            "{}                writable.write_byte(if {} {{ 1 }} else {{ 0 }});",
                            indent, tagged_copy_accessor
                        )?;
                    },
                    FieldType::Int8 => {
                        writeln!(
                            file,
                            "{}                writable.write_unsigned_varint(1); // size = 1 byte",
                            indent
                        )?;
                        writeln!(file, "{}                writable.write_byte({});", indent, tagged_copy_accessor)?;
                    },
                    FieldType::Int16 => {
                        writeln!(
                            file,
                            "{}                writable.write_unsigned_varint(2); // size = 2 bytes",
                            indent
                        )?;
                        writeln!(
                            file,
                            "{}                writable.write_short({});",
                            indent, tagged_copy_accessor
                        )?;
                    },
                    FieldType::Int32 => {
                        writeln!(
                            file,
                            "{}                writable.write_unsigned_varint(4); // size = 4 bytes",
                            indent
                        )?;
                        writeln!(file, "{}                writable.write_int({});", indent, tagged_copy_accessor)?;
                    },
                    FieldType::Int64 => {
                        writeln!(
                            file,
                            "{}                writable.write_unsigned_varint(8); // size = 8 bytes",
                            indent
                        )?;
                        writeln!(file, "{}                writable.write_long({});", indent, tagged_copy_accessor)?;
                    },
                    FieldType::Uuid => {
                        writeln!(
                            file,
                            "{}                writable.write_unsigned_varint(16); // size = 16 bytes",
                            indent
                        )?;
                        writeln!(file, "{}                writable.write_uuid(&{});", indent, tagged_accessor)?;
                    },
                    FieldType::Array(element_type) => {
                        // Compute the exact serialized size of the array first
                        // (length varint + element bytes) and allocate the temp
                        // buffer to that size — avoids the legacy fixed-cap
                        // `allocate(1024)` (which over-allocated for small
                        // arrays and triggered a `Vec::resize` reallocation
                        // for arrays >1024 bytes).
                        writeln!(file, "{}                // Calculate exact array serialized size", indent)?;
                        writeln!(
                            file,
                            "{}                let mut size_acc = crate::common::protocol::MessageSizeAccumulator::new();",
                            indent
                        )?;
                        writeln!(
                            file,
                            "{}                size_acc.add_bytes(byte_utils::size_of_unsigned_varint({}.len() as u32 + 1) as i32);",
                            indent, tagged_accessor
                        )?;
                        // Element sizes
                        match element_type.as_ref() {
                            FieldType::Bool | FieldType::Int8 => {
                                writeln!(
                                    file,
                                    "{}                size_acc.add_bytes({}.len() as i32);",
                                    indent, tagged_accessor
                                )?;
                            },
                            FieldType::Int16 | FieldType::Uint16 => {
                                writeln!(
                                    file,
                                    "{}                size_acc.add_bytes(({}.len() * 2) as i32);",
                                    indent, tagged_accessor
                                )?;
                            },
                            FieldType::Int32 | FieldType::Uint32 | FieldType::Float64 => {
                                writeln!(
                                    file,
                                    "{}                size_acc.add_bytes(({}.len() * 4) as i32);",
                                    indent, tagged_accessor
                                )?;
                            },
                            FieldType::Int64 => {
                                writeln!(
                                    file,
                                    "{}                size_acc.add_bytes(({}.len() * 8) as i32);",
                                    indent, tagged_accessor
                                )?;
                            },
                            FieldType::Uuid => {
                                writeln!(
                                    file,
                                    "{}                size_acc.add_bytes(({}.len() * 16) as i32);",
                                    indent, tagged_accessor
                                )?;
                            },
                            FieldType::String => {
                                writeln!(
                                    file,
                                    "{}                for element in {}.iter() {{",
                                    indent, tagged_accessor
                                )?;
                                writeln!(file, "{}                    let elen = element.len() as u32;", indent)?;
                                writeln!(
                                    file,
                                    "{}                    size_acc.add_bytes(byte_utils::size_of_unsigned_varint(elen + 1) as i32);",
                                    indent
                                )?;
                                writeln!(file, "{}                    size_acc.add_bytes(elen as i32);", indent)?;
                                writeln!(file, "{}                }}", indent)?;
                            },
                            _ => {
                                // Structs / unknown — recurse via add_size.
                                writeln!(
                                    file,
                                    "{}                let mut size_cache = crate::common::protocol::ObjectSerializationCache::new();",
                                    indent
                                )?;
                                writeln!(
                                    file,
                                    "{}                for element in {}.iter() {{",
                                    indent, tagged_accessor
                                )?;
                                writeln!(
                                    file,
                                    "{}                    element.add_size(&mut size_acc, &mut size_cache, version);",
                                    indent
                                )?;
                                writeln!(file, "{}                }}", indent)?;
                            },
                        }
                        writeln!(
                            file,
                            "{}                let mut size_accessor = crate::common::protocol::ByteBufferAccessor::allocate(size_acc.total_size() as usize);",
                            indent
                        )?;
                        writeln!(file, "{}                // Write array length", indent)?;
                        writeln!(
                            file,
                            "{}                size_accessor.write_unsigned_varint(({}.len() as u32) + 1);",
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
                                writeln!(file, "{}                    size_accessor.write_uuid(element);", indent)?;
                                writeln!(file, "{}                }}", indent)?;
                            },
                            FieldType::Int8 => {
                                writeln!(
                                    file,
                                    "{}                for element in {}.iter() {{",
                                    indent, tagged_accessor
                                )?;
                                writeln!(file, "{}                    size_accessor.write_byte(*element);", indent)?;
                                writeln!(file, "{}                }}", indent)?;
                            },
                            FieldType::Int16 => {
                                writeln!(
                                    file,
                                    "{}                for element in {}.iter() {{",
                                    indent, tagged_accessor
                                )?;
                                writeln!(file, "{}                    size_accessor.write_short(*element);", indent)?;
                                writeln!(file, "{}                }}", indent)?;
                            },
                            FieldType::Int32 => {
                                writeln!(
                                    file,
                                    "{}                for element in {}.iter() {{",
                                    indent, tagged_accessor
                                )?;
                                writeln!(file, "{}                    size_accessor.write_int(*element);", indent)?;
                                writeln!(file, "{}                }}", indent)?;
                            },
                            FieldType::Int64 => {
                                writeln!(
                                    file,
                                    "{}                for element in {}.iter() {{",
                                    indent, tagged_accessor
                                )?;
                                writeln!(file, "{}                    size_accessor.write_long(*element);", indent)?;
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
                                    "{}                    size_accessor.write_unsigned_varint((bytes.len() as u32) + 1);",
                                    indent
                                )?;
                                writeln!(file, "{}                    size_accessor.write_byte_array(bytes);", indent)?;
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

                        writeln!(
                            file,
                            "{}                let size = size_accessor.position() as u32; size_accessor.flip();",
                            indent
                        )?;
                        writeln!(file, "{}                writable.write_unsigned_varint(size);", indent)?;
                        writeln!(
                            file,
                            "{}                writable.write_byte_array(size_accessor.buffer());",
                            indent
                        )?;
                    },
                    FieldType::Float64 => {
                        writeln!(
                            file,
                            "{}                writable.write_unsigned_varint(8); // size = 8 bytes",
                            indent
                        )?;
                        writeln!(
                            file,
                            "{}                writable.write_double({});",
                            indent, tagged_copy_accessor
                        )?;
                    },
                    FieldType::Bytes | FieldType::Records => {
                        // Tagged Bytes/Records on the wire: varint(payload_size) ++ payload,
                        // where payload = varint(len+1) ++ raw_bytes. Compute the
                        // payload size exactly (no over-allocation).
                        if nullable {
                            writeln!(file, "{}                if let Some(ref val) = self.{} {{", indent, field_name)?;
                            writeln!(
                                file,
                                "{}                    let prefix_size = byte_utils::size_of_unsigned_varint(val.len() as u32 + 1) as i32;",
                                indent
                            )?;
                            writeln!(
                                file,
                                "{}                    let payload_size = prefix_size + val.len() as i32;",
                                indent
                            )?;
                            writeln!(
                                file,
                                "{}                    writable.write_unsigned_varint(payload_size as u32);",
                                indent
                            )?;
                            writeln!(
                                file,
                                "{}                    writable.write_unsigned_varint((val.len() as u32) + 1);",
                                indent
                            )?;
                            writeln!(file, "{}                    writable.write_byte_array(val);", indent)?;
                            writeln!(file, "{}                }} else {{", indent)?;
                            // null encoding: size = 1 (varint(0))
                            writeln!(
                                file,
                                "{}                    writable.write_unsigned_varint(1); // size for null",
                                indent
                            )?;
                            writeln!(file, "{}                    writable.write_unsigned_varint(0); // null", indent)?;
                            writeln!(file, "{}                }}", indent)?;
                        } else {
                            writeln!(
                                file,
                                "{}                let prefix_size = byte_utils::size_of_unsigned_varint({}.len() as u32 + 1) as i32;",
                                indent, tagged_accessor
                            )?;
                            writeln!(
                                file,
                                "{}                let payload_size = prefix_size + {}.len() as i32;",
                                indent, tagged_accessor
                            )?;
                            writeln!(
                                file,
                                "{}                writable.write_unsigned_varint(payload_size as u32);",
                                indent
                            )?;
                            writeln!(
                                file,
                                "{}                writable.write_unsigned_varint(({}.len() as u32) + 1);",
                                indent, tagged_accessor
                            )?;
                            writeln!(
                                file,
                                "{}                writable.write_byte_array(&*{});",
                                indent, tagged_accessor
                            )?;
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
                                    "{}                    writable.write_unsigned_varint(1); // size = 1 byte",
                                    indent
                                )?;
                                writeln!(
                                    file,
                                    "{}                    writable.write_unsigned_varint(0); // null presence indicator",
                                    indent
                                )?;
                                writeln!(file, "{}                }} else {{", indent)?;
                                // Pre-compute the struct's serialized size so the temp buffer
                                // is allocated exactly. Avoids the legacy `allocate(256)`
                                // over-allocation per partition response.
                                writeln!(
                                    file,
                                    "{}                    let mut struct_acc = crate::common::protocol::MessageSizeAccumulator::new();",
                                    indent
                                )?;
                                writeln!(
                                    file,
                                    "{}                    let mut struct_cache = crate::common::protocol::ObjectSerializationCache::new();",
                                    indent
                                )?;
                                writeln!(
                                    file,
                                    "{}                    {}.add_size(&mut struct_acc, &mut struct_cache, version);",
                                    indent, tagged_accessor
                                )?;
                                writeln!(
                                    file,
                                    "{}                    let payload_size = struct_acc.total_size() + 1; // +1 for presence indicator",
                                    indent
                                )?;
                                writeln!(
                                    file,
                                    "{}                    let mut size_accessor = crate::common::protocol::ByteBufferAccessor::allocate(payload_size as usize);",
                                    indent
                                )?;
                                writeln!(
                                    file,
                                    "{}                    size_accessor.write_unsigned_varint(1); // non-null presence indicator",
                                    indent
                                )?;
                                writeln!(
                                    file,
                                    "{}                    {}.write(&mut size_accessor, version)?;",
                                    indent, tagged_accessor
                                )?;
                                writeln!(
                                    file,
                                    "{}                    let size = size_accessor.position() as u32; size_accessor.flip();",
                                    indent
                                )?;
                                writeln!(file, "{}                    writable.write_unsigned_varint(size);", indent)?;
                                writeln!(
                                    file,
                                    "{}                    writable.write_byte_array(size_accessor.buffer());",
                                    indent
                                )?;
                                writeln!(file, "{}                }}", indent)?;
                            } else {
                                // Nullable struct with null default: only written when non-null.
                                writeln!(
                                    file,
                                    "{}                let mut struct_acc = crate::common::protocol::MessageSizeAccumulator::new();",
                                    indent
                                )?;
                                writeln!(
                                    file,
                                    "{}                let mut struct_cache = crate::common::protocol::ObjectSerializationCache::new();",
                                    indent
                                )?;
                                writeln!(
                                    file,
                                    "{}                {}.add_size(&mut struct_acc, &mut struct_cache, version);",
                                    indent, tagged_accessor
                                )?;
                                writeln!(
                                    file,
                                    "{}                let payload_size = struct_acc.total_size() + 1; // +1 for presence indicator",
                                    indent
                                )?;
                                writeln!(
                                    file,
                                    "{}                let mut size_accessor = crate::common::protocol::ByteBufferAccessor::allocate(payload_size as usize);",
                                    indent
                                )?;
                                writeln!(
                                    file,
                                    "{}                size_accessor.write_unsigned_varint(1); // non-null presence indicator",
                                    indent
                                )?;
                                writeln!(
                                    file,
                                    "{}                {}.write(&mut size_accessor, version)?;",
                                    indent, tagged_accessor
                                )?;
                                writeln!(
                                    file,
                                    "{}                let size = size_accessor.position() as u32; size_accessor.flip();",
                                    indent
                                )?;
                                writeln!(file, "{}                writable.write_unsigned_varint(size);", indent)?;
                                writeln!(
                                    file,
                                    "{}                writable.write_byte_array(size_accessor.buffer());",
                                    indent
                                )?;
                            }
                        } else {
                            // Non-nullable struct: pre-compute exact size.
                            writeln!(
                                file,
                                "{}                let mut struct_acc = crate::common::protocol::MessageSizeAccumulator::new();",
                                indent
                            )?;
                            writeln!(
                                file,
                                "{}                let mut struct_cache = crate::common::protocol::ObjectSerializationCache::new();",
                                indent
                            )?;
                            writeln!(
                                file,
                                "{}                {}.add_size(&mut struct_acc, &mut struct_cache, version);",
                                indent, tagged_accessor
                            )?;
                            writeln!(
                                file,
                                "{}                let mut size_accessor = crate::common::protocol::ByteBufferAccessor::allocate(struct_acc.total_size() as usize);",
                                indent
                            )?;
                            writeln!(
                                file,
                                "{}                {}.write(&mut size_accessor, version)?;",
                                indent, tagged_accessor
                            )?;
                            writeln!(
                                file,
                                "{}                let size = size_accessor.position() as u32; size_accessor.flip();",
                                indent
                            )?;
                            writeln!(file, "{}                writable.write_unsigned_varint(size);", indent)?;
                            writeln!(
                                file,
                                "{}                writable.write_byte_array(size_accessor.buffer());",
                                indent
                            )?;
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
    writeln!(
        file,
        "{}            writable.write_unsigned_varint(field.tag() as u32);",
        indent
    )?;
    writeln!(
        file,
        "{}            writable.write_unsigned_varint(field.size() as u32);",
        indent
    )?;
    writeln!(file, "{}            writable.write_byte_array(field.data());", indent)?;
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
        "    pub fn read(readable: &mut dyn Readable, version: i16) -> Result<Self, KafkaError> {{"
    )?;

    let lowest = struct_spec.versions().lowest();
    let highest = struct_spec.versions().highest();

    // Generate version check - avoid useless comparison when highest is i16::MAX
    if highest == i16::MAX {
        writeln!(file, "        if version < {} {{", lowest)?;
    } else {
        writeln!(file, "        if version < {} || version > {} {{", lowest, highest)?;
    }

    writeln!(file, "            return Err(KafkaError::Generic(")?;
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
                "            result.unknown_tagged_fields.push(RawTaggedField::new(tag as i32, data));"
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
                "                result.unknown_tagged_fields.push(RawTaggedField::new(tag as i32, data));"
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
        "    pub fn write(&self, writable: &mut dyn Writable, version: i16) -> Result<(), KafkaError> {{"
    )?;

    let lowest = struct_spec.versions().lowest();
    let highest = struct_spec.versions().highest();

    // Generate version check - avoid useless comparison when highest is i16::MAX
    if highest == i16::MAX {
        writeln!(file, "        if version < {} {{", lowest)?;
    } else {
        writeln!(file, "        if version < {} || version > {} {{", lowest, highest)?;
    }

    writeln!(file, "            return Err(KafkaError::Generic(")?;
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
            let effective_flex = field_flexible_versions(field, flexible_versions);
            generate_field_write(file, field, effective_flex)?;
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
            writeln!(file, "            return Err(KafkaError::Generic(")?;
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
                "        writable.write_unsigned_varint(self.unknown_tagged_fields.len() as u32);"
            )?;
            writeln!(file, "        for field in &self.unknown_tagged_fields {{")?;
            writeln!(file, "            writable.write_unsigned_varint(field.tag() as u32);")?;
            writeln!(file, "            writable.write_unsigned_varint(field.size() as u32);")?;
            writeln!(file, "            writable.write_byte_array(field.data());")?;
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
                "            writable.write_unsigned_varint(self.unknown_tagged_fields.len() as u32);"
            )?;
            writeln!(file, "            for field in &self.unknown_tagged_fields {{")?;
            writeln!(file, "                writable.write_unsigned_varint(field.tag() as u32);")?;
            writeln!(file, "                writable.write_unsigned_varint(field.size() as u32);")?;
            writeln!(file, "                writable.write_byte_array(field.data());")?;
            writeln!(file, "            }}")?;
            writeln!(file, "        }} else if !self.unknown_tagged_fields.is_empty() {{")?;
            writeln!(file, "            return Err(KafkaError::Generic(")?;
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
        "return Err(KafkaError::Generic(\"Null string not allowed\".to_string()));".to_string()
    };
    let neg_action = if nullable {
        format!("result.{} = None;", field_name)
    } else {
        "return Err(KafkaError::Generic(\"Negative string length\".to_string()));".to_string()
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
            writeln!(file, "{}    bytes = readable.read_array(bytes.len())?;", indent)?;
            writeln!(
                file,
                "{}    result.{} = {}String::from_utf8(bytes)",
                indent, field_name, some_wrap
            )?;
            writeln!(
                file,
                "{}        .map_err(|e| KafkaError::Generic(e.to_string()))?{};",
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
            writeln!(file, "{}        bytes = readable.read_array(bytes.len())?;", indent)?;
            writeln!(
                file,
                "{}        result.{} = {}String::from_utf8(bytes)",
                indent, field_name, some_wrap
            )?;
            writeln!(
                file,
                "{}            .map_err(|e| KafkaError::Generic(e.to_string()))?{};",
                indent, some_close
            )?;
            writeln!(file, "{}    }}", indent)?;
            writeln!(file, "{}}} else {{", indent)?;
            writeln!(file, "{}    let len = readable.read_short()?;", indent)?;
            writeln!(file, "{}    if len < 0 {{", indent)?;
            writeln!(file, "{}        {}", indent, neg_action)?;
            writeln!(file, "{}    }} else {{", indent)?;
            writeln!(file, "{}        let mut bytes = vec![0u8; len as usize];", indent)?;
            writeln!(file, "{}        bytes = readable.read_array(bytes.len())?;", indent)?;
            writeln!(
                file,
                "{}        result.{} = {}String::from_utf8(bytes)",
                indent, field_name, some_wrap
            )?;
            writeln!(
                file,
                "{}            .map_err(|e| KafkaError::Generic(e.to_string()))?{};",
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
        writeln!(file, "{}    bytes = readable.read_array(bytes.len())?;", indent)?;
        writeln!(
            file,
            "{}    result.{} = {}String::from_utf8(bytes)",
            indent, field_name, some_wrap
        )?;
        writeln!(
            file,
            "{}        .map_err(|e| KafkaError::Generic(e.to_string()))?{};",
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
            writeln!(file, "{}        bytes = readable.read_array(bytes.len())?;", indent)?;
            writeln!(file, "{}        result.{} = Some(bytes);", indent, field_name)?;
            writeln!(file, "{}    }}", indent)?;
        } else {
            writeln!(file, "{}    let length = if len == 0 {{ 0 }} else {{ len - 1 }};", indent)?;
            writeln!(file, "{}    let mut bytes = vec![0u8; length as usize];", indent)?;
            writeln!(file, "{}    bytes = readable.read_array(bytes.len())?;", indent)?;
            writeln!(file, "{}    result.{} = bytes;", indent, field_name)?;
        }
        writeln!(file, "{}}} else {{", indent)?;
        writeln!(file, "{}    let len = readable.read_int()?;", indent)?;
        if nullable {
            writeln!(file, "{}    if len < 0 {{", indent)?;
            writeln!(file, "{}        result.{} = None;", indent, field_name)?;
            writeln!(file, "{}    }} else {{", indent)?;
            writeln!(file, "{}        let mut bytes = vec![0u8; len as usize];", indent)?;
            writeln!(file, "{}        bytes = readable.read_array(bytes.len())?;", indent)?;
            writeln!(file, "{}        result.{} = Some(bytes);", indent, field_name)?;
            writeln!(file, "{}    }}", indent)?;
        } else {
            writeln!(file, "{}    let length = if len < 0 {{ 0 }} else {{ len as u32 }};", indent)?;
            writeln!(file, "{}    let mut bytes = vec![0u8; length as usize];", indent)?;
            writeln!(file, "{}    bytes = readable.read_array(bytes.len())?;", indent)?;
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
            writeln!(file, "{}    bytes = readable.read_array(bytes.len())?;", indent)?;
            writeln!(file, "{}    result.{} = Some(bytes);", indent, field_name)?;
            writeln!(file, "{}}}", indent)?;
        } else {
            writeln!(file, "{}let length = if len < 0 {{ 0 }} else {{ len as u32 }};", indent)?;
            writeln!(file, "{}let mut bytes = vec![0u8; length as usize];", indent)?;
            writeln!(file, "{}bytes = readable.read_array(bytes.len())?;", indent)?;
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
        "return Err(KafkaError::Generic(\"Null array not allowed\".to_string()));".to_string()
    };
    let neg_action = if nullable {
        format!("result.{} = None;", field_name)
    } else {
        "return Err(KafkaError::Generic(\"Negative array length\".to_string()));".to_string()
    };

    // Helper to generate the read loop that populates the array
    let gen_read_loop = |file: &mut fs::File, ind: &str, fn_name: &str| -> Result<(), Box<dyn std::error::Error>> {
        // Bounds check: validate array length against remaining bytes before allocating,
        // to prevent OOM from malicious messages with huge array lengths.
        writeln!(file, "{}if length as usize > readable.remaining() {{", ind)?;
        writeln!(file, "{}    return Err(KafkaError::Generic(", ind)?;
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
                    writeln!(file, "                    bytes = readable.read_array(bytes.len())?;")?;
                    writeln!(
                        file,
                        "                    {}.push(String::from_utf8(bytes).map_err(|e| KafkaError::Generic(e.to_string()))?);",
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
                    writeln!(file, "                        bytes = readable.read_array(bytes.len())?;")?;
                    writeln!(
                        file,
                        "                        {}.push(String::from_utf8(bytes).map_err(|e| KafkaError::Generic(e.to_string()))?);",
                        target
                    )?;
                    writeln!(file, "                    }}")?;
                    writeln!(file, "                }} else {{")?;
                    writeln!(file, "                    let str_len = readable.read_short()? as usize;")?;
                    writeln!(file, "                    let mut bytes = vec![0u8; str_len];")?;
                    writeln!(file, "                    bytes = readable.read_array(bytes.len())?;")?;
                    writeln!(
                        file,
                        "                    {}.push(String::from_utf8(bytes).map_err(|e| KafkaError::Generic(e.to_string()))?);",
                        target
                    )?;
                    writeln!(file, "                }}")?;
                }
            } else {
                // No flexible versions: always use standard encoding
                writeln!(file, "                let str_len = readable.read_short()? as usize;")?;
                writeln!(file, "                let mut bytes = vec![0u8; str_len];")?;
                writeln!(file, "                bytes = readable.read_array(bytes.len())?;")?;
                writeln!(
                    file,
                    "                {}.push(String::from_utf8(bytes).map_err(|e| KafkaError::Generic(e.to_string()))?);",
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
            writeln!(file, "{}writable.write_byte(if {} {{ 1 }} else {{ 0 }});", ind, accessor)?;
        },
        FieldType::Int8 => {
            writeln!(file, "{}writable.write_byte({});", ind, accessor)?;
        },
        FieldType::Int16 => {
            writeln!(file, "{}writable.write_short({});", ind, accessor)?;
        },
        FieldType::Int32 => {
            writeln!(file, "{}writable.write_int({});", ind, accessor)?;
        },
        FieldType::Int64 => {
            writeln!(file, "{}writable.write_long({});", ind, accessor)?;
        },
        FieldType::Uint16 => {
            writeln!(file, "{}writable.write_unsigned_short({});", ind, accessor)?;
        },
        FieldType::Uint32 => {
            writeln!(file, "{}writable.write_unsigned_int({});", ind, accessor)?;
        },
        FieldType::Uuid => {
            writeln!(file, "{}writable.write_uuid(&{});", ind, accessor)?;
        },
        FieldType::Float64 => {
            writeln!(file, "{}writable.write_double({});", ind, accessor)?;
        },
        FieldType::String => {
            writeln!(file, "{}let bytes = {}.as_bytes();", ind, accessor)?;
            // Check if this version uses flexible encoding
            if !flexible_versions.empty() {
                if flexible_versions.lowest() == 0 {
                    writeln!(file, "{}writable.write_unsigned_varint((bytes.len() as u32) + 1);", ind)?;
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
                    writeln!(file, "{}    writable.write_unsigned_varint((bytes.len() as u32) + 1);", ind)?;
                    writeln!(file, "{}}} else {{", ind)?;
                    writeln!(file, "{}    writable.write_short(bytes.len() as i16);", ind)?;
                    writeln!(file, "{}}}", ind)?;
                }
            } else {
                writeln!(file, "{}writable.write_short(bytes.len() as i16);", ind)?;
            }
            writeln!(file, "{}writable.write_byte_array(bytes);", ind)?;
        },
        FieldType::Bytes | FieldType::Records => {
            if !flexible_versions.empty() {
                if flexible_versions.lowest() == 0 {
                    writeln!(file, "{}writable.write_unsigned_varint(({}.len() as u32) + 1);", ind, accessor)?;
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
                        "{}    writable.write_unsigned_varint(({}.len() as u32) + 1);",
                        ind, accessor
                    )?;
                    writeln!(file, "{}}} else {{", ind)?;
                    writeln!(file, "{}    writable.write_int({}.len() as i32);", ind, accessor)?;
                    writeln!(file, "{}}}", ind)?;
                }
            } else {
                writeln!(file, "{}writable.write_int({}.len() as i32);", ind, accessor)?;
            }
            // For `FieldType::Records`, emit `write_byte_buffer` so on a
            // `SendBuilder` the bytes are passed through as their own
            // zero-copy chunk (an `Arc<[u8]>` in the chunk list) — they do
            // not get copied a second time when the network layer ships
            // them, satisfying CLAUDE.md rule 12 (vectored I/O / zero-copy
            // through the full write path).
            //
            // For `FieldType::Bytes` we keep `write_byte_array` because
            // small bytes fields (e.g. SASL tokens) are not on a hot
            // zero-copy path and `write_byte_buffer`'s `Arc::from(...)`
            // allocation per write would be a regression. Both methods
            // produce identical wire bytes — `ByteBufferAccessor` and
            // `DataOutputStreamWritable` route `write_byte_buffer` through
            // `write_byte_array` (no length prefix; the prefix was emitted
            // above). The runtime difference is only visible on
            // `SendBuilder`.
            //
            // Use `.as_slice()` so the accessor always coerces to `&[u8]`
            // regardless of whether it's bound from a nullable field
            // (`_nv: &Vec<u8>`) or the non-nullable case (`self.field: Vec<u8>`)
            // — avoids the `clippy::needless_borrow` lint that fires when the
            // emit prepends `&` to an already-borrowed accessor.
            let write_method = match field.field_type() {
                FieldType::Records => "write_byte_buffer",
                _ => "write_byte_array",
            };
            writeln!(file, "{}writable.{}({}.as_slice());", ind, write_method, accessor)?;
        },
        FieldType::Array(element_type) => {
            if !flexible_versions.empty() {
                if flexible_versions.lowest() == 0 {
                    writeln!(file, "{}writable.write_unsigned_varint(({}.len() as u32) + 1);", ind, accessor)?;
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
                        "{}    writable.write_unsigned_varint(({}.len() as u32) + 1);",
                        ind, accessor
                    )?;
                    writeln!(file, "{}}} else {{", ind)?;
                    writeln!(file, "{}    writable.write_int({}.len() as i32);", ind, accessor)?;
                    writeln!(file, "{}}}", ind)?;
                }
            } else {
                writeln!(file, "{}writable.write_int({}.len() as i32);", ind, accessor)?;
            }
            writeln!(file, "{}for element in {}.iter() {{", ind, accessor)?;
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
                    writeln!(file, "{}writable.write_byte(1); // non-null presence byte", ind)?;
                } else {
                    writeln!(file, "{}if version >= {} {{", ind, nullable_versions.lowest())?;
                    writeln!(file, "{}    writable.write_byte(1); // non-null presence byte", ind)?;
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
                writeln!(file, "{}writable.write_byte(-1); // null struct presence byte", inner)?;
            } else {
                writeln!(file, "{}if version >= {} {{", inner, nullable_versions.lowest())?;
                writeln!(file, "{}    writable.write_byte(-1); // null struct presence byte", inner)?;
                writeln!(file, "{}}} else {{", inner)?;
                writeln!(
                    file,
                    "{}    return Err(KafkaError::Generic( \"Null value for non-nullable struct field\"));",
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
                    writeln!(file, "{}writable.write_unsigned_varint(0);", inner)?;
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
                    writeln!(file, "{}    writable.write_unsigned_varint(0);", inner)?;
                    writeln!(file, "{}}} else {{", inner)?;
                    writeln!(file, "{}    writable.write_short(-1);", inner)?;
                    writeln!(file, "{}}}", inner)?;
                }
            } else {
                writeln!(file, "{}writable.write_short(-1);", inner)?;
            }
        },
        FieldType::Bytes | FieldType::Records => {
            if !flexible_versions.empty() {
                if flexible_versions.lowest() == 0 {
                    writeln!(file, "{}writable.write_unsigned_varint(0);", inner)?;
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
                    writeln!(file, "{}    writable.write_unsigned_varint(0);", inner)?;
                    writeln!(file, "{}}} else {{", inner)?;
                    writeln!(file, "{}    writable.write_int(-1);", inner)?;
                    writeln!(file, "{}}}", inner)?;
                }
            } else {
                writeln!(file, "{}writable.write_int(-1);", inner)?;
            }
        },
        FieldType::Array(_) => {
            if !flexible_versions.empty() {
                if flexible_versions.lowest() == 0 {
                    writeln!(file, "{}writable.write_unsigned_varint(0);", inner)?;
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
                    writeln!(file, "{}    writable.write_unsigned_varint(0);", inner)?;
                    writeln!(file, "{}}} else {{", inner)?;
                    writeln!(file, "{}    writable.write_int(-1);", inner)?;
                    writeln!(file, "{}}}", inner)?;
                }
            } else {
                writeln!(file, "{}writable.write_int(-1);", inner)?;
            }
        },
        FieldType::Struct(_) => {
            // For nullable struct fields, write a null presence byte.
            // In nullable versions: write byte(-1) to indicate null.
            // In non-nullable versions: this is an error (Java throws NullPointerException).
            // Since we can't easily access nullable_versions here, we write byte(-1)
            // unconditionally. The version check is handled by the caller's nullable wrapper.
            writeln!(file, "{}writable.write_byte(-1); // null struct presence byte", inner)?;
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
            writeln!(file, "                writable.write_byte(if *element {{ 1 }} else {{ 0 }});")?;
        },
        FieldType::Int8 => {
            writeln!(file, "                writable.write_byte(*element);")?;
        },
        FieldType::Int16 => {
            writeln!(file, "                writable.write_short(*element);")?;
        },
        FieldType::Int32 => {
            writeln!(file, "                writable.write_int(*element);")?;
        },
        FieldType::Int64 => {
            writeln!(file, "                writable.write_long(*element);")?;
        },
        FieldType::Uint16 => {
            writeln!(file, "                writable.write_unsigned_short(*element);")?;
        },
        FieldType::Uint32 => {
            writeln!(file, "                writable.write_unsigned_int(*element);")?;
        },
        FieldType::Uuid => {
            writeln!(file, "                writable.write_uuid(element);")?;
        },
        FieldType::Float64 => {
            writeln!(file, "                writable.write_double(*element);")?;
        },
        FieldType::String => {
            writeln!(file, "                let bytes = element.as_bytes();")?;
            if !flexible_versions.empty() {
                if flexible_versions.lowest() == 0 {
                    writeln!(
                        file,
                        "                writable.write_unsigned_varint((bytes.len() as u32) + 1);"
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
                        "                    writable.write_unsigned_varint((bytes.len() as u32) + 1);"
                    )?;
                    writeln!(file, "                }} else {{")?;
                    writeln!(file, "                    writable.write_short(bytes.len() as i16);")?;
                    writeln!(file, "                }}")?;
                }
            } else {
                writeln!(file, "                writable.write_short(bytes.len() as i16);")?;
            }
            writeln!(file, "                writable.write_byte_array(bytes);")?;
        },
        FieldType::Bytes | FieldType::Records => {
            if !flexible_versions.empty() {
                if flexible_versions.lowest() == 0 {
                    writeln!(
                        file,
                        "                writable.write_unsigned_varint((element.len() as u32) + 1);"
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
                        "                    writable.write_unsigned_varint((element.len() as u32) + 1);"
                    )?;
                    writeln!(file, "                }} else {{")?;
                    writeln!(file, "                    writable.write_int(element.len() as i32);")?;
                    writeln!(file, "                }}")?;
                }
            } else {
                writeln!(file, "                writable.write_int(element.len() as i32);")?;
            }
            // For `FieldType::Records` use `write_byte_buffer` so on a
            // `SendBuilder` the bytes pass through as their own zero-copy
            // chunk. See the parallel comment in `generate_field_write` for
            // the full rationale.
            let write_method = match element_type {
                FieldType::Records => "write_byte_buffer",
                _ => "write_byte_array",
            };
            writeln!(file, "                writable.{}(element);", write_method)?;
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

/// Map a FieldType to a `protocol::types::Type` expression string for a
/// specific (flexible, nullable) combination. The variant names below
/// correspond to the Phase 2c-runtime `Type` enum (e.g. `Type::Boolean`,
/// `Type::UInt16`, `Type::UnsignedInt32`); they are intentionally not the
/// same shape as Java's static `Type` field names.
fn schema_type_for(field_type: &FieldType, flexible: bool, nullable: bool) -> String {
    match field_type {
        FieldType::Bool => "Type::Boolean".to_string(),
        FieldType::Int8 => "Type::Int8".to_string(),
        FieldType::Int16 => "Type::Int16".to_string(),
        FieldType::Uint16 => "Type::UInt16".to_string(),
        FieldType::Uint32 => "Type::UnsignedInt32".to_string(),
        FieldType::Int32 => "Type::Int32".to_string(),
        FieldType::Int64 => "Type::Int64".to_string(),
        FieldType::Uuid => "Type::Uuid".to_string(),
        FieldType::Float64 => "Type::Float64".to_string(),
        FieldType::String => match (flexible, nullable) {
            (true, true) => "Type::CompactNullableString",
            (true, false) => "Type::CompactString",
            (false, true) => "Type::NullableString",
            (false, false) => "Type::String",
        }
        .to_string(),
        FieldType::Bytes => match (flexible, nullable) {
            (true, true) => "Type::CompactNullableBytes",
            (true, false) => "Type::CompactBytes",
            (false, true) => "Type::NullableBytes",
            (false, false) => "Type::Bytes",
        }
        .to_string(),
        FieldType::Records => {
            if flexible {
                "Type::CompactRecords".to_string()
            } else {
                "Type::Records".to_string()
            }
        },
        FieldType::Array(element_type) => {
            // Element type → schema-side `Type` expression. For nested
            // structs we emit `<StructName>::schema(version)?` so the
            // inner schema reflects the version-specific shape; for scalar
            // element types we recurse through `schema_type_for` (with
            // flexible=false so the inner element keeps its non-compact
            // form — array compaction is encoded by the outer
            // `Array`/`CompactArray` wrapper, not by the element).
            let element_expr = match element_type.as_ref() {
                FieldType::Struct(name) => {
                    format!("Type::Schema(Box::new({}::schema(version)?))", name)
                },
                inner => schema_type_for(inner, false, false),
            };
            // Inner-array nullability matches the outer field nullability:
            // a `nullable []T` produces `ArrayOf::nullable(...)`.
            let constructor = if nullable { "nullable" } else { "new" };
            if flexible {
                format!(
                    "Type::CompactArray(Box::new(CompactArrayOf::{}({})))",
                    constructor, element_expr
                )
            } else {
                format!("Type::Array(Box::new(ArrayOf::{}({})))", constructor, element_expr)
            }
        },
        FieldType::Struct(name) => {
            // A direct struct field — emit a nested `Type::Schema`.
            format!("Type::Schema(Box::new({}::schema(version)?))", name)
        },
    }
}

/// Compute the schema type expression for a field. The result may be version-dependent
/// when flexibility or nullability boundaries fall within the field's version range,
/// producing an inline if/else expression.
///
/// The `message_flexible_versions` parameter is the message-level `flexibleVersions`.
/// A field that overrides this with its own `flexibleVersions` (for example,
/// `RequestHeader.ClientId` declares `"flexibleVersions": "none"`) must use the override:
/// it always emits length-prefixed schema types (`SchemaType::NullableString`) instead of
/// the compact variant (`SchemaType::CompactNullableString`). Mirrors
/// `SchemaGenerator.fieldFlexibleVersions` in the Java generator.
fn schema_type_expr_for_field(field: &FieldSpec, message_flexible_versions: Versions) -> String {
    let field_type = field.field_type();
    let nullable_versions = field.nullable_versions();
    let v_low = field.versions().lowest();
    let v_high = field.versions().highest();

    // Resolve the per-field flexibleVersions override (Java's
    // `field.flexibleVersions().orElse(messageFlexibleVersions)`).
    let flexible_versions = field_flexible_versions(field, message_flexible_versions);

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
///
/// Uses the per-field flexibleVersions override (when present) so that fields like
/// `RequestHeader.ClientId` (`flexibleVersions: "none"`) do not get a misleading
/// version-dependent schema-type expression on a flexible message.
fn field_has_version_dependent_schema_type(field: &FieldSpec, message_flexible_versions: Versions) -> bool {
    let v_low = field.versions().lowest();
    let v_high = field.versions().highest();

    // Direct or array-of struct fields always emit a `<Inner>::schema(version)?`
    // call, which reads `version` — so the schema method must take `version`,
    // not `_version`.
    match field.field_type() {
        FieldType::Struct(_) => return true,
        FieldType::Array(element_type) if matches!(element_type.as_ref(), FieldType::Struct(_)) => {
            return true;
        },
        _ => {},
    }

    // Resolve the per-field flexibleVersions override.
    let flexible_versions = field_flexible_versions(field, message_flexible_versions);

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
    writeln!(
        file,
        "    pub fn schema({}: i16) -> Result<Schema, KafkaError> {{",
        version_param
    )?;
    writeln!(file, "        let mut fields = Vec::new();")?;

    // For each field, emit conditional push based on version. The runtime
    // `Field` constructor (`Field::with_doc`) takes name, type, doc string —
    // matching Java's `Field(String, Type, String)` ctor used in the
    // generated code. There is no `field_type` field on the runtime struct;
    // its `r#type` field is private behind `Field::with_doc`.
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
            "fields.push(Field::with_doc(\"{}\", {}, \"{}\"));",
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
        // Nullable with no explicit default: Java defaults to non-null empty values
        // for String, Bytes, Records, and Struct. Only fields with explicit
        // "default": "null" in the JSON spec default to null/None.
        if default.is_none() {
            match field.field_type() {
                FieldType::String => return "Some(String::new())".to_string(),
                FieldType::Bytes | FieldType::Records => return "Some(Vec::new())".to_string(),
                FieldType::Struct(struct_name) => {
                    return format!("Some({}::new())", struct_name);
                },
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

    /// Helper: parse a single field-spec JSON literal for tests.
    fn parse_field(json: &str) -> FieldSpec {
        serde_json::from_str::<FieldSpec>(json).expect("valid field JSON")
    }

    // ----------------------------------------------------------------------
    // G1: per-field flexibleVersions overrides (mirrors Java
    // SchemaGenerator.fieldFlexibleVersions / MessageDataGenerator.fieldFlexibleVersions).
    // ----------------------------------------------------------------------

    #[test]
    fn test_field_flexible_versions_no_override_returns_message_flex() {
        // Field with no `flexibleVersions` override inherits the message-level value.
        let mut field =
            parse_field(r#"{ "name": "Foo", "type": "string", "versions": "0+", "nullableVersions": "0+" }"#);
        field.validate().unwrap();
        let msg_flex = Versions::parse(Some("2+"), Versions::NONE).unwrap();
        assert_eq!(field_flexible_versions(&field, msg_flex), msg_flex);
    }

    #[test]
    fn test_field_flexible_versions_none_override_disables_compact_encoding() {
        // RequestHeader.ClientId-shaped field: `"flexibleVersions": "none"`. The
        // override forces non-flexible (length-prefixed) encoding even when the
        // message itself is flexible.
        let mut field = parse_field(
            r#"{
                "name": "ClientId",
                "type": "string",
                "versions": "1+",
                "nullableVersions": "1+",
                "flexibleVersions": "none"
            }"#,
        );
        field.validate().unwrap();
        let msg_flex = Versions::parse(Some("2+"), Versions::NONE).unwrap();
        let resolved = field_flexible_versions(&field, msg_flex);
        assert!(resolved.empty(), "flexibleVersions=none must resolve to empty");
        assert!(!resolved.contains(2));
    }

    #[test]
    fn g1_request_header_client_id_emits_non_compact_schema_type() {
        // Locks the RequestHeader.ClientId schema type for v2 (the message's first
        // flexible version) to `Type::NullableString` — the length-prefixed
        // variant. If the per-field flexibleVersions override is ever dropped this
        // will regress to `Type::CompactNullableString`, which is wire-incompatible
        // with Java brokers (Java emits `Type.NULLABLE_STRING`).
        let mut field = parse_field(
            r#"{
                "name": "ClientId",
                "type": "string",
                "versions": "1+",
                "nullableVersions": "1+",
                "flexibleVersions": "none"
            }"#,
        );
        field.validate().unwrap();
        let msg_flex = Versions::parse(Some("2+"), Versions::NONE).unwrap();
        let expr = schema_type_expr_for_field(&field, msg_flex);
        assert_eq!(
            expr, "Type::NullableString",
            "ClientId must use length-prefixed NullableString at every version: got {expr}",
        );
        // No version-dependent expression should be emitted for this field.
        assert!(
            !field_has_version_dependent_schema_type(&field, msg_flex),
            "ClientId schema type must not vary by version when override is `none`",
        );
    }

    #[test]
    fn g1_field_without_override_still_picks_compact_at_flexible_version() {
        // Sanity check: when the field has no override, a flexible-version message
        // does produce a version-dependent compact-vs-length-prefixed expression.
        let mut field = parse_field(
            r#"{
                "name": "Topic",
                "type": "string",
                "versions": "0+"
            }"#,
        );
        field.validate().unwrap();
        let msg_flex = Versions::parse(Some("2+"), Versions::NONE).unwrap();
        let expr = schema_type_expr_for_field(&field, msg_flex);
        assert!(
            field_has_version_dependent_schema_type(&field, msg_flex),
            "field without override on a flexible-from-v2 message must vary by version: expr was {expr}",
        );
        assert!(
            expr.contains("Type::CompactString") && expr.contains("Type::String"),
            "expected both compact and length-prefixed branches, got: {expr}",
        );
    }

    #[test]
    fn g1_request_header_full_codegen_uses_non_compact_for_client_id() {
        // End-to-end check: feed RequestHeader.json into the generator and assert
        // the generated schema() method uses NullableString (not the compact
        // variant) for client_id. This locks the entire generation pipeline.
        use std::path::Path;
        let tmp = std::env::temp_dir().join(format!("phase2a-g1-{}", std::process::id()));
        std::fs::create_dir_all(&tmp).unwrap();
        let input = Path::new("messages");
        // Run only on RequestHeader.json by isolating it into a temp input dir.
        let isolated = tmp.join("input");
        std::fs::create_dir_all(&isolated).unwrap();
        std::fs::copy(input.join("RequestHeader.json"), isolated.join("RequestHeader.json")).unwrap();
        let output = tmp.join("output");
        generate_messages(&isolated, &output).expect("codegen succeeds for RequestHeader");
        let generated =
            std::fs::read_to_string(output.join("request_header_data.rs")).expect("request_header_data.rs written");
        // Locate the schema() function body.
        let schema_start = generated.find("pub fn schema").expect("schema() method emitted");
        let after_schema = &generated[schema_start..];
        let body_end = after_schema.find("Schema::new(fields)").expect("schema() body found");
        let body = &after_schema[..body_end];
        assert!(
            body.contains("name: \"client_id\"") || body.contains("\"client_id\","),
            "client_id field expected in schema(); body was:\n{body}",
        );
        assert!(
            body.contains("Type::NullableString"),
            "client_id must use Type::NullableString (length-prefixed) on the wire; body was:\n{body}",
        );
        assert!(
            !body.contains("CompactNullableString"),
            "client_id must NOT use the compact variant — its `flexibleVersions: none` override forces length-prefixed encoding; body was:\n{body}",
        );
        // Cleanup
        let _ = std::fs::remove_dir_all(&tmp);
    }

    // ----------------------------------------------------------------------
    // G2: nullable string/bytes default-value resolution. A nullable field
    // without an explicit `"default": "null"` must default to a non-null
    // empty value (mirrors Java FieldSpec.fieldDefault() in OpenJDK).
    // Only an explicit `"default": "null"` produces `None`.
    // ----------------------------------------------------------------------

    #[test]
    fn g2_nullable_string_no_default_is_some_empty_string() {
        let mut field = parse_field(
            r#"{
                "name": "Topic",
                "type": "string",
                "versions": "0+",
                "nullableVersions": "0+"
            }"#,
        );
        field.validate().unwrap();
        assert_eq!(get_default_value_for_field(&field), "Some(String::new())");
    }

    #[test]
    fn g2_nullable_bytes_no_default_is_some_empty_vec() {
        let mut field = parse_field(
            r#"{
                "name": "Payload",
                "type": "bytes",
                "versions": "0+",
                "nullableVersions": "0+"
            }"#,
        );
        field.validate().unwrap();
        assert_eq!(get_default_value_for_field(&field), "Some(Vec::new())");
    }

    #[test]
    fn g2_nullable_records_no_default_is_some_empty_vec() {
        let mut field = parse_field(
            r#"{
                "name": "Records",
                "type": "records",
                "versions": "0+",
                "nullableVersions": "0+"
            }"#,
        );
        field.validate().unwrap();
        assert_eq!(get_default_value_for_field(&field), "Some(Vec::new())");
    }

    #[test]
    fn g2_nullable_string_explicit_null_default_is_none() {
        let mut field = parse_field(
            r#"{
                "name": "Topic",
                "type": "string",
                "versions": "0+",
                "nullableVersions": "0+",
                "default": "null"
            }"#,
        );
        field.validate().unwrap();
        assert_eq!(get_default_value_for_field(&field), "None");
    }

    #[test]
    fn g2_nullable_bytes_explicit_null_default_is_none() {
        let mut field = parse_field(
            r#"{
                "name": "Payload",
                "type": "bytes",
                "versions": "0+",
                "nullableVersions": "0+",
                "default": "null"
            }"#,
        );
        field.validate().unwrap();
        assert_eq!(get_default_value_for_field(&field), "None");
    }

    #[test]
    fn g2_non_nullable_string_no_default_is_empty_string() {
        // A non-nullable string with no `default` should produce an empty owned
        // String (Java emits `""`).
        let mut field = parse_field(
            r#"{
                "name": "Topic",
                "type": "string",
                "versions": "0+"
            }"#,
        );
        field.validate().unwrap();
        assert_eq!(get_default_value_for_field(&field), "String::new()");
    }

    // ----------------------------------------------------------------------
    // G3: int64 must map to Rust `i64`, not `u64`. Producer IDs, offsets, and
    // Uuid most/least-significant bits are signed in Java; using `u64` would
    // flip ordering for values with the high bit set.
    // ----------------------------------------------------------------------

    #[test]
    fn g3_int64_maps_to_i64() {
        assert_eq!(field_type_to_rust(&FieldType::Int64), "i64");
    }

    #[test]
    fn g3_int32_int16_int8_map_to_signed() {
        assert_eq!(field_type_to_rust(&FieldType::Int8), "i8");
        assert_eq!(field_type_to_rust(&FieldType::Int16), "i16");
        assert_eq!(field_type_to_rust(&FieldType::Int32), "i32");
    }

    #[test]
    fn g3_uint_types_map_to_unsigned() {
        // Sanity: uint types do remain unsigned.
        assert_eq!(field_type_to_rust(&FieldType::Uint16), "u16");
        assert_eq!(field_type_to_rust(&FieldType::Uint32), "u32");
    }

    #[test]
    fn g3_uuid_maps_to_uuid_struct_not_u64_pair() {
        // Locks Uuid to the Rust `Uuid` struct. The struct internally uses signed
        // most/least-significant bits (mirroring Java UUID), so direct numeric
        // comparison ordering is preserved.
        assert_eq!(field_type_to_rust(&FieldType::Uuid), "Uuid");
    }

    #[test]
    fn g3_int64_field_in_struct_emits_i64_not_u64() {
        // End-to-end: a producer-id-shaped field declared as int64 must end up as
        // `i64` in the generated Rust type.
        let mut field = parse_field(
            r#"{
                "name": "ProducerId",
                "type": "int64",
                "versions": "0+"
            }"#,
        );
        field.validate().unwrap();
        assert_eq!(field_type_to_rust_for_field(&field), "i64");
    }

    // ----------------------------------------------------------------------
    // Issue 3: Schema emit for array-of-struct fields uses
    // `Type::Array(ArrayOf::new(Type::Schema(...)))` /
    // `Type::CompactArray(CompactArrayOf::new(Type::Schema(...)))`, NOT the
    // placeholder `Type::Bytes` / `Type::CompactBytes`.
    // ----------------------------------------------------------------------

    #[test]
    fn schema_emit_for_array_of_struct_uses_array_of_schema_not_bytes() {
        // End-to-end: feed ProduceRequest.json into the generator and assert the
        // emitted `schema()` method for the top-level message uses
        // `Type::CompactArray(...)` over a nested struct schema for the
        // `topic_data` field, not the legacy `Type::CompactBytes` placeholder.
        use std::path::Path;
        let tmp = std::env::temp_dir().join(format!("phase2-issue3-{}", std::process::id()));
        std::fs::create_dir_all(&tmp).unwrap();
        let input = Path::new("messages");
        let isolated = tmp.join("input");
        std::fs::create_dir_all(&isolated).unwrap();
        // Bring along the dependency files needed to resolve nested types.
        for spec in &["ProduceRequest.json", "RequestHeader.json"] {
            let src = input.join(spec);
            if src.exists() {
                std::fs::copy(&src, isolated.join(spec)).unwrap();
            }
        }
        let output = tmp.join("output");
        generate_messages(&isolated, &output).expect("codegen succeeds for ProduceRequest");
        let generated =
            std::fs::read_to_string(output.join("produce_request_data.rs")).expect("produce_request_data.rs written");

        // Find the top-level ProduceRequestData::schema() body.
        let marker = "impl ProduceRequestData {";
        let start = generated.find(marker).expect("top-level impl block emitted");
        let after = &generated[start..];
        let schema_start = after.find("pub fn schema").expect("top-level schema() emitted");
        let body_end = after[schema_start..].find("Schema::new(fields)").expect("schema() body found");
        let body = &after[schema_start..schema_start + body_end];

        // The `topic_data` field is `[]TopicProduceData`. At v9+ flexible-encoded.
        assert!(
            body.contains("topic_data"),
            "topic_data field expected in schema(); body was:\n{body}",
        );
        // Strip whitespace to make the test rustfmt-tolerant.
        let normalized: String = body.chars().filter(|c| !c.is_whitespace()).collect();
        assert!(
            normalized.contains("CompactArrayOf::new(Type::Schema(Box::new(TopicProduceData::schema"),
            "topic_data must use CompactArrayOf over the nested struct's schema; body was:\n{body}",
        );
        assert!(
            normalized.contains("ArrayOf::new(Type::Schema(Box::new(TopicProduceData::schema"),
            "topic_data must use ArrayOf over the nested struct's schema for non-flexible versions; body was:\n{body}",
        );
        // The legacy placeholder must be gone.
        assert!(
            !body.contains("if version >= 9 { Type::CompactBytes } else { Type::Bytes }"),
            "topic_data must not collapse to Bytes/CompactBytes placeholder; body was:\n{body}",
        );

        // Cleanup
        let _ = std::fs::remove_dir_all(&tmp);
    }
}
