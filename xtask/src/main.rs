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

use std::env;
use std::fs;
use std::path::PathBuf;
use std::process::{exit, Command};

fn main() -> anyhow::Result<()> {
    let task = env::args().nth(1);

    match task.as_deref() {
        Some("format") => format()?,
        Some("format-check") => format_check()?,
        Some("check-generated") => check_generated()?,
        _ => print_help(),
    }

    Ok(())
}

fn format() -> anyhow::Result<()> {
    println!("🎨 Formatting Rust code...");

    // Format main crate
    run_command("cargo", &["fmt"])?;

    // Format generator crate
    run_command("cargo", &["fmt", "--manifest-path", "generator/Cargo.toml"])?;

    // Format generated files
    println!("🎨 Formatting generated files...");
    format_generated_files()?;

    println!("✅ Formatting complete!");
    Ok(())
}

fn format_check() -> anyhow::Result<()> {
    println!("🔍 Checking Rust code formatting...");

    // Check main crate
    let main_result = Command::new("cargo").args(&["fmt", "--", "--check"]).status()?;

    // Check generator crate
    let gen_result = Command::new("cargo")
        .args(&["fmt", "--manifest-path", "generator/Cargo.toml", "--", "--check"])
        .status()?;

    if !main_result.success() || !gen_result.success() {
        eprintln!("❌ Code is not formatted. Run: cargo xtask format");
        exit(1);
    }

    println!("✅ All code is properly formatted!");
    Ok(())
}

fn check_generated() -> anyhow::Result<()> {
    println!("🔍 Checking generated code formatting...");

    let generated_files = find_generated_files()?;

    if generated_files.is_empty() {
        println!("⚠️  No generated files found (build first with cargo build)");
        return Ok(());
    }

    println!("   Checking {} generated file(s)", generated_files.len());

    let mut has_errors = false;
    for file in &generated_files {
        let output = Command::new("rustfmt").arg("--check").arg(file).output()?;

        if !output.status.success() {
            eprintln!("❌ Formatting issues in: {}", file.display());
            has_errors = true;
        }
    }

    if has_errors {
        eprintln!("\n❌ Generated code has formatting issues. Run: cargo build && cargo xtask format");
        exit(1);
    }

    println!("✅ All generated code is properly formatted!");
    Ok(())
}

fn format_generated_files() -> anyhow::Result<()> {
    let generated_files = find_generated_files()?;

    if generated_files.is_empty() {
        println!("⚠️  No generated files found (build first with cargo build)");
        return Ok(());
    }

    println!("   Found {} generated file(s)", generated_files.len());

    for file in &generated_files {
        let status = Command::new("rustfmt").arg(file).status()?;

        if !status.success() {
            anyhow::bail!("Failed to format: {}", file.display());
        }
    }

    Ok(())
}

fn find_generated_files() -> anyhow::Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    let target_dir = PathBuf::from("target/debug/build");

    if !target_dir.exists() {
        return Ok(files);
    }

    for entry in fs::read_dir(target_dir)? {
        let entry = entry?;
        let path = entry.path();

        if path.is_dir() && path.file_name().unwrap().to_str().unwrap().starts_with("confluent-kafka-rust-") {
            let generated_dir = path.join("out/generated");
            if generated_dir.exists() {
                for file_entry in fs::read_dir(generated_dir)? {
                    let file_entry = file_entry?;
                    let file_path = file_entry.path();
                    if file_path.extension().map_or(false, |ext| ext == "rs") {
                        files.push(file_path);
                    }
                }
            }
        }
    }

    Ok(files)
}

fn run_command(program: &str, args: &[&str]) -> anyhow::Result<()> {
    let status = Command::new(program).args(args).status()?;

    if !status.success() {
        anyhow::bail!("Command failed: {} {}", program, args.join(" "));
    }

    Ok(())
}

fn print_help() {
    eprintln!(
        "Tasks:
  format          Format all Rust code including generated files
  format-check    Check if code is formatted correctly
  check-generated Check generated code formatting only (no changes)

Usage:
  cargo xtask format
  cargo xtask format-check
  cargo xtask check-generated"
    );
}
