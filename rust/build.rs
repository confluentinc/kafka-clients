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

// The generator's sources, compiled into this build script rather than taken
// as a `generator` dependency: a path-only dependency cannot be published, and
// these sources ship inside the package (see `generator/crate/Cargo.toml`).
#[path = "generator/src/lib.rs"]
mod generator;

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
    // The emitted code depends on the generator's own source, not just on the
    // message specs. Without this, editing the generator leaves the previously
    // generated files in OUT_DIR untouched and the build silently stale.
    println!("cargo:rerun-if-changed=generator/src/");

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
        // The published package leaves the C FFI out (see the `include` list in
        // `Cargo.toml`): fail with a clear message rather than rustc's "file not
        // found for module `ffi`".
        if !Path::new("src/ffi").exists() {
            panic!(
                "the `ffi` feature is not available in the published crate: \
                 the C FFI is built from the source repository (c/ and python/)"
            );
        }

        println!("cargo:rerun-if-changed=src/");
        println!("cargo:rerun-if-changed=cbindgen.toml");

        let crate_dir = env::var("CARGO_MANIFEST_DIR").unwrap();
        let header_path = Path::new(&crate_dir).join("target/include/confluent_kafka.h");

        if let Some(parent) = header_path.parent() {
            fs::create_dir_all(parent).expect("Failed to create target/include/");
        }

        let config = cbindgen::Config::from_file("cbindgen.toml").expect("Failed to read cbindgen.toml");

        // The header is the C API: a binding compiled against a stale one
        // fails at link time with no hint of why, so a cbindgen failure fails
        // the build here instead.
        let mut bindings = cbindgen::Builder::new()
            .with_crate(&crate_dir)
            .with_config(config)
            .generate()
            .unwrap_or_else(|e| panic!("cbindgen failed to generate the C header: {e}"));
        bindings.write_to_file(&header_path);
        eprintln!("C header generated: {}", header_path.display());

        // The same bindings with every function replaced by the `_err` twin
        // `#[ffi_guard]` (crate `ffi-macros`) emits beside it. cbindgen reads
        // the unexpanded source and never sees the twins, so they are derived
        // here from the functions it parsed, by the rule the macro follows.
        let bindings_path = Path::new(&crate_dir).join("target/include/confluent_kafka_bindings.h");
        bindings_header::to_err_twins(&mut bindings);
        bindings.write_to_file(&bindings_path);
        eprintln!("C header generated: {}", bindings_path.display());
    }
}

/// `confluent_kafka_bindings.h`: `confluent_kafka.h` with every function `f`
/// replaced by its `_err` twin.
#[cfg(feature = "ffi")]
mod bindings_header {
    use cbindgen::Bindings;
    use cbindgen::ir::{Function, FunctionArgument, Path, PrimitiveType, Type};

    /// Printed above the include guard, in place of `cbindgen.toml`'s `header`.
    const PREAMBLE: &str = r#"/*
 * The C API for language bindings.
 *
 * This header declares the types of confluent_kafka.h and, for each of its
 * functions f, the twin f_err, which reports every failure, a caught Rust
 * panic included, as the kafka_common_Error_t * it returns (NULL on success).
 * A twin has the documentation and the preconditions of f:
 *   - kafka_common_Error_t *f(args): f_err has the same signature and
 *     behaviour;
 *   - T f(args), for any other T: kafka_common_Error_t *f_err(args,
 *     T *out_result) writes f's value to *out_result only when it returns
 *     NULL;
 *   - void f(args): kafka_common_Error_t *f_err(args);
 *   - void f_cb(args, cb, opaque): kafka_common_Error_t *f_cb_err(args, cb,
 *     opaque) returns NULL. As for f_cb, its completion reports every
 *     outcome exactly once, a caught panic included, whether the panic
 *     happens on the calling thread or later on the task running the
 *     operation. A completion with no error parameter
 *     (kafka_admin_Admin_close_cb_err,
 *     kafka_admin_Admin_close_with_timeout_cb_err,
 *     kafka_producer_Callback_on_completion_cb_err) fires with opaque alone,
 *     the panic showing only in the log.
 * A caught panic has the code kafka_common_Error_LOCAL_ILLEGAL_STATE and a
 * message beginning "Rust panic caught at the FFI boundary in f_err". The
 * caller owns a returned error.
 *
 * A panic is a bug in this library and may leave the handle it happened on in
 * an inconsistent state: destroy that handle and create a new one. Later calls
 * on it can keep failing, because a poisoned lock is never cleared; its
 * _destroy_err still frees it and runs its pending callbacks, each exactly
 * once.
 *
 * Include either this header or confluent_kafka.h, never both: they declare
 * the same types. C programs use confluent_kafka.h, whose functions return
 * their values directly.
 */"#;

    /// Replaces every function of `bindings` by its twin and gives the header
    /// its own include guard and preamble.
    pub(super) fn to_err_twins(bindings: &mut Bindings) {
        let error = bindings
            .functions
            .iter()
            .map(|function| &function.ret)
            .find(|ret| is_error_handle(ret))
            .cloned()
            .expect("the C API has a function returning kafka_common_Error_t *");
        for function in &mut bindings.functions {
            to_err_twin(function, &error);
        }
        bindings.config.include_guard = Some("CONFLUENT_KAFKA_BINDINGS_H".to_owned());
        bindings.config.header = Some(PREAMBLE.to_owned());
    }

    /// `function` -> `<name>_err`, by the rule of `ffi-macros`' twin.
    fn to_err_twin(function: &mut Function, error: &Type) {
        let name = function.path.name().to_owned();
        let twin = format!("{name}_err");
        let ret = std::mem::replace(&mut function.ret, error.clone());
        let note = if name.ends_with("_cb") {
            // Exactly the exports whose `#[ffi_guard]` takes `on_panic`
            // (`test_on_panic_exports_are_the_cb_exports` in `src/ffi/mod.rs`).
            format!(
                "The `_err` twin of `{name}`: it returns NULL, and the completion reports every outcome, a caught panic included, as for `{name}`."
            )
        } else if is_error_handle(&ret) {
            format!("The `_err` twin of `{name}`, with the same contract.")
        } else if matches!(ret, Type::Primitive(PrimitiveType::Void)) {
            format!("The `_err` twin of `{name}`: it returns NULL, or the panic it caught.")
        } else {
            function.args.push(FunctionArgument {
                name: Some("out_result".to_owned()),
                ty: Type::Ptr { ty: Box::new(ret), is_const: false, is_nullable: false, is_ref: false },
                array_length: None,
            });
            format!(
                "The `_err` twin of `{name}`: it writes the value `{name}` returns to `*out_result` and returns NULL, or returns the panic it caught and leaves `*out_result` unwritten."
            )
        };
        function.path = Path::new(twin);
        let doc = &mut function.documentation.doc_comment;
        if !doc.is_empty() {
            doc.push(String::new());
        }
        doc.push(format!(" {note}"));
    }

    /// Whether `ty` is `kafka_common_Error_t *`.
    fn is_error_handle(ty: &Type) -> bool {
        matches!(ty, Type::Ptr { ty, is_const: false, .. }
            if matches!(ty.as_ref(), Type::Path(path) if path.export_name() == "kafka_common_Error_t"))
    }
}
