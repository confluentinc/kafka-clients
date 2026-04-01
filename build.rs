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

fn main() {
    println!("cargo:rerun-if-changed=generator/messages/");
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

    // Format generated files
    eprintln!("Formatting generated files...");
    if let Ok(entries) = fs::read_dir(&generated_dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().is_some_and(|ext| ext == "rs") {
                let _ = Command::new("rustfmt").arg("--edition").arg("2021").arg(&path).status();
            }
        }
        eprintln!("Generated files formatted.");
    }
}
