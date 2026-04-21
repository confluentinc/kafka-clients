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

use std::env;
use std::fs;
use std::path::Path;
use std::process::Command;

fn format_generated_dir(dir: &Path) {
    if let Ok(entries) = fs::read_dir(dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().is_some_and(|ext| ext == "rs") {
                let _ = Command::new("rustfmt").arg("--edition").arg("2021").arg(&path).status();
            }
        }
    }
}

fn main() {
    println!("cargo:rerun-if-changed=generator/messages/");
    println!("cargo:rerun-if-changed=generator/test-messages/");
    println!("cargo:rerun-if-changed=src/bin/message_generator.rs");

    let out_dir = env::var("OUT_DIR").unwrap();
    let generated_dir = Path::new(&out_dir).join("generated");

    // Call the generator library function to generate message code
    eprintln!("Generating message code from JSON specifications...");

    match generator::generate_messages(Path::new("generator/messages"), &generated_dir) {
        Ok(()) => {
            eprintln!("Message generation complete.");
        },
        Err(e) => {
            eprintln!("Error: Message generation failed: {}", e);
            panic!("Failed to generate message code: {}", e);
        },
    }

    // Generate api_message_type.rs (Rust equivalent of Java's generated ApiMessageType)
    eprintln!("Generating ApiMessageType...");
    match generator::generate_api_message_type(Path::new("generator/messages"), &generated_dir) {
        Ok(()) => {
            eprintln!("ApiMessageType generation complete.");
        },
        Err(e) => {
            eprintln!("Error: ApiMessageType generation failed: {}", e);
            panic!("Failed to generate ApiMessageType: {}", e);
        },
    }

    // Generate test-only message types from test-messages/
    // Always generated (build.rs can't distinguish test vs release), but only
    // included in the crate when the "test-messages" feature is enabled.
    let test_messages_dir = Path::new("generator/test-messages");
    if test_messages_dir.exists() {
        let test_generated_dir = Path::new(&out_dir).join("test_generated");
        eprintln!("Generating test-only message code...");
        match generator::generate_messages(test_messages_dir, &test_generated_dir) {
            Ok(()) => {
                // Remove api_message_type from test mod.rs — test messages don't need it
                // and it would conflict with the main generated api_message_type.
                let mod_file = test_generated_dir.join("mod.rs");
                if let Ok(content) = fs::read_to_string(&mod_file) {
                    let filtered: String = content
                        .lines()
                        .filter(|line| !line.contains("api_message_type"))
                        .map(|line| format!("{}\n", line))
                        .collect();
                    let _ = fs::write(&mod_file, filtered);
                }
                // Remove the unused api_message_type.rs file
                let _ = fs::remove_file(test_generated_dir.join("api_message_type.rs"));
                eprintln!("Test message generation complete.");
            },
            Err(e) => {
                eprintln!("Error: Test message generation failed: {}", e);
                panic!("Failed to generate test message code: {}", e);
            },
        }
        format_generated_dir(&test_generated_dir);
    }

    // Format generated files
    eprintln!("Formatting generated files...");
    format_generated_dir(&generated_dir);
    eprintln!("Generated files formatted.");

    // Generate C header when the ffi feature is enabled
    #[cfg(feature = "ffi")]
    {
        println!("cargo:rerun-if-changed=src/");
        println!("cargo:rerun-if-changed=cbindgen.toml");

        let crate_dir = env::var("CARGO_MANIFEST_DIR").unwrap();
        let header_path = Path::new(&crate_dir).join("target/include/confluent_kafka.h");

        if let Some(parent) = header_path.parent() {
            fs::create_dir_all(parent).expect("Failed to create target/include/");
        }

        let config = cbindgen::Config::from_file("cbindgen.toml").expect("Failed to read cbindgen.toml");

        match cbindgen::Builder::new().with_crate(&crate_dir).with_config(config).generate() {
            Ok(bindings) => {
                bindings.write_to_file(&header_path);
                eprintln!("C header generated: {}", header_path.display());
            },
            Err(e) => {
                eprintln!("Warning: cbindgen failed: {}", e);
            },
        }
    }
}
