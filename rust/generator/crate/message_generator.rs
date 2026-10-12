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

//! The Kafka message generator CLI.
//!
//! This is a thin wrapper around the generator crate's generate_messages function.

use generator::generate_messages;
use std::env;
use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();

    if args.len() < 5 {
        eprintln!("Usage: {} --input <input_dir> --output <output_dir>", args[0]);
        eprintln!("  --input, -i    Input directory containing JSON message specifications");
        eprintln!("  --output, -o   Output directory for generated Rust code");
        std::process::exit(1);
    }

    let mut input_dir = None;
    let mut output_dir = None;

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--input" | "-i" => {
                i += 1;
                if i < args.len() {
                    input_dir = Some(PathBuf::from(&args[i]));
                }
            },
            "--output" | "-o" => {
                i += 1;
                if i < args.len() {
                    output_dir = Some(PathBuf::from(&args[i]));
                }
            },
            _ => {},
        }
        i += 1;
    }

    let input_dir = input_dir.ok_or("--input directory required")?;
    let output_dir = output_dir.ok_or("--output directory required")?;

    // Call the library function
    generate_messages(&input_dir, &output_dir)?;

    Ok(())
}
