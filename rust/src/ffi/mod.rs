// Copyright 2025 Confluent Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! C FFI layer for the Kafka producer, consumer and admin APIs.
//!
//! This module exposes those APIs via C-callable `extern "C"` functions,
//! allowing non-Rust code (C, C++, Python via ctypes, etc.) to use the Kafka
//! client.
//!
//! Every scalar crossing the boundary is a fixed-width signed integer or a
//! `double` (CLAUDE.md §4): `i8` for Java `byte` and `boolean`, `i16`, `i32`,
//! `i64`; no `bool`, `usize` or unsigned type appears in a signature, and the
//! only `u8` is the `data` pointer inside [`util::kafka_Bytes_t`]. Lengths and
//! counts use `i32`, and negative values (-1) signal "not set" for optional
//! **scalar** fields and for out-of-range element accessors.
//!
//! A fallible function returns `*mut kafka_common_Error_t` (null on success)
//! and delivers its value through a trailing `out_<name>` parameter. Owned
//! strings are freed with [`util::kafka_string_destroy`]; collections cross as
//! the package-less [`util::kafka_List_t`] and [`util::kafka_Map_t`] of
//! `void *`, byte buffers as [`util::kafka_Bytes_t`]. A Java `KafkaFuture<T>`
//! is [`kafka_future::kafka_common_KafkaFuture_t`]; the callbacks a client
//! queues for its `__execute_callbacks` live in [`callback_queue`].
//!
//! A nullable Java collection crosses as a null `kafka_List_t *` /
//! `kafka_Map_t *` (distinct from an empty one); `kafka_List_size` and
//! `kafka_Map_size` are never negative, since a size feeds straight into
//! `malloc(size * n)` and `for (int32_t i = 0; i < size; i++)` on the C side.
//! The first client created from C initializes the default `RUST_LOG` logger
//! ([`common::init_default_logger`]).
//!
//! # Panics never cross the C boundary
//!
//! Every exported function catches a Rust panic before it can unwind into its
//! caller, which would abort the process, and reports it instead. A fallible
//! function returns the panic as its `kafka_common_Error_t *` and leaves its
//! `out_<name>` parameters unwritten, as for any other failure: read them only
//! after a NULL return. The error's code is
//! `kafka_common_ErrorCode_LOCAL_ILLEGAL_STATE` and its message begins
//! `Rust panic caught at the FFI boundary in <function>`. Any other function
//! returns the failure value of its return type:
//!
//! - a pointer: NULL;
//! - an `int8_t`: 0, false for a boolean. An `int8_t` carrying a Java `byte`
//!   code whose 0 is a valid constant (`kafka_common_IsolationLevel_id`, say)
//!   returns -1 instead, as its documentation states;
//! - another integer: -1, but 0 for a `*_count` or `*_size` accessor, a size
//!   that returns -1 for a null key or value
//!   (`kafka_consumer_ConsumerRecord_serialized_key_size`) included;
//! - a `double`: NaN;
//! - a `kafka_Bytes_t`: a NULL one (Java's `null` array);
//! - `kafka_common_ErrorCode_e` (`kafka_common_Error_code`):
//!   `kafka_common_ErrorCode_UNKNOWN_SERVER_ERROR`; another C enum: the
//!   enumerator its documentation names;
//! - a borrowed `const kafka_common_Error_t *`: an error describing the panic,
//!   which is never freed.
//!
//! A function returning nothing only logs the panic.
//!
//! A `_cb` function reports a caught panic the way it reports every other
//! outcome: it queues its completion, carrying the error and the caller's
//! `opaque`, on the client's callback queue, exactly once, and the client's
//! `__execute_callbacks` runs it like any other completion. That completion is
//! where `opaque` comes back to be released; there is no other release hook.
//!
//! A panic is a bug in this library and may leave the handle it happened on in
//! an inconsistent state: destroy that handle and create a new one. Later calls
//! on it can keep failing, because a poisoned lock is never cleared; its
//! `_destroy` still frees it and runs its pending callbacks, each exactly once.
//!
//! The same paragraphs open the generated C header (`header` in
//! `cbindgen.toml`). The mechanism is the [`ffi_guard`] attribute, whose runtime
//! half is `common::ffi_guard_or`.
//!
//! # Feature Gate
//!
//! This module is only compiled when the `ffi` feature is enabled.

pub(crate) mod admin;
pub(crate) mod callback_queue;
pub(crate) mod common;
pub(crate) mod consumer;
mod error_predicates;
pub(crate) mod kafka_future;
pub(crate) mod producer;
pub(crate) mod util;

/// Guards an exported function against a Rust panic unwinding into its C
/// caller; see the module docs and the `ffi-macros` crate.
pub(crate) use ffi_macros::ffi_guard;

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};

    /// Every Rust file under `src/ffi`, with its path relative to the crate root
    /// and its source text, in a stable order.
    fn sources() -> Vec<(String, String)> {
        fn walk(dir: &Path, out: &mut Vec<PathBuf>) {
            for entry in std::fs::read_dir(dir).expect("read src/ffi") {
                let path = entry.expect("dir entry").path();
                if path.is_dir() {
                    walk(&path, out);
                } else if path.extension().is_some_and(|ext| ext == "rs") {
                    out.push(path);
                }
            }
        }
        let root = Path::new(env!("CARGO_MANIFEST_DIR"));
        let mut paths = Vec::new();
        walk(&root.join("src/ffi"), &mut paths);
        paths.sort();
        paths
            .into_iter()
            .map(|path| {
                let text = std::fs::read_to_string(&path).expect("read source");
                let rel = path.strip_prefix(root).expect("under the crate root").display().to_string();
                (rel, text)
            })
            .collect()
    }

    /// Whether the attribute that ends on the line just above `index` is an
    /// `#[ffi_guard...]`. The attribute may span several lines, so this walks up
    /// from its last line to the line that opens it. Comment lines inside the
    /// attribute are part of it (the `on_panic` closures carry the soundness
    /// comment clippy requires above their `unsafe` block); a comment directly
    /// above `#[unsafe(no_mangle)]` is not an attribute and fails.
    fn preceded_by_ffi_guard(lines: &[&str], index: usize) -> bool {
        let Some(mut line) = index.checked_sub(1) else {
            return false;
        };
        let last = lines[line].trim();
        if !last.ends_with(']') || last.starts_with("//") {
            return false;
        }
        loop {
            let text = lines[line].trim_start();
            if text.starts_with("#[") {
                return text.starts_with("#[ffi_guard");
            }
            if line == 0 || text.is_empty() {
                return false;
            }
            line -= 1;
        }
    }

    /// Every exported function is guarded against a panic unwinding into C (D8 of
    /// `design/current/appsec-7665-4521-ffi-panic-guard.md`): each
    /// `#[unsafe(no_mangle)]` must be immediately preceded by the last line of an
    /// `#[ffi_guard...]` attribute. A new entry point without one fails here with
    /// its file and line.
    #[test]
    #[ignore = "the split FFI is not guarded yet; #207 is ported onto it in the next commits"]
    fn test_every_exported_function_is_guarded() {
        let mut offenders = Vec::new();
        let mut total = 0;
        for (file, text) in sources() {
            let lines: Vec<&str> = text.lines().collect();
            let mut exported = 0;
            for (index, line) in lines.iter().enumerate() {
                if line.trim() != "#[unsafe(no_mangle)]" {
                    continue;
                }
                exported += 1;
                if !preceded_by_ffi_guard(&lines, index) {
                    offenders.push(format!("{file}:{}", index + 1));
                }
            }
            total += exported;
        }
        assert!(
            offenders.is_empty(),
            "exported functions without #[ffi_guard] directly above #[unsafe(no_mangle)]: {offenders:?}"
        );
        // Sanity check that the scan saw the whole surface rather than, say, a
        // renamed attribute spelling: 2306 exports after the FFI was split into
        // one file per class.
        assert!(total >= 2306, "only {total} exported functions found");
    }

    /// The `int8_t` exports carrying a Java `byte` code: each enum's domain
    /// includes 0 (`IsolationLevel.READ_UNCOMMITTED`, `AlterConfigOp.OpType.SET`,
    /// `ElectionType.PREFERRED`, or an `UNKNOWN`) and none includes -1, so a
    /// caught panic must return -1 rather than the derived 0 the remaining
    /// `int8_t` exports, all booleans, return. Pins the set in both directions:
    /// a code guarded with the bare attribute, or a boolean given -1, fails.
    #[test]
    fn test_byte_code_exports_fall_back_to_minus_one() {
        const BYTE_CODES: [&str; 10] = [
            "kafka_admin_AlterConfigOp_OpType_id",
            "kafka_admin_FeatureUpdate_UpgradeType_code",
            "kafka_admin_ScramMechanism_type",
            "kafka_common_ElectionType_value",
            "kafka_common_IsolationLevel_id",
            "kafka_common_acl_AclOperation_code",
            "kafka_common_acl_AclPermissionType_code",
            "kafka_common_config_ConfigResource_Type_id",
            "kafka_common_resource_PatternType_code",
            "kafka_common_resource_ResourceType_code",
        ];
        let mut minus_one = Vec::new();
        let mut i8_exports = 0;
        for (_, text) in sources() {
            let lines: Vec<&str> = text.lines().collect();
            for (index, line) in lines.iter().enumerate() {
                if line.trim() != "#[unsafe(no_mangle)]" {
                    continue;
                }
                // The signature runs from the line below to the one opening the body.
                let end = (index + 1..lines.len())
                    .find(|&line| lines[line].trim_end().ends_with('{'))
                    .expect("an exported fn has a body");
                let signature = lines[index + 1..=end]
                    .iter()
                    .map(|line| line.trim())
                    .collect::<Vec<_>>()
                    .join(" ");
                if !signature.trim_end_matches('{').trim_end().ends_with("-> i8") {
                    continue;
                }
                i8_exports += 1;
                let name = signature
                    .split("fn ")
                    .nth(1)
                    .and_then(|rest| rest.split('(').next())
                    .expect("an exported fn has a name")
                    .to_string();
                if index > 0 && lines[index - 1].trim() == "#[ffi_guard(fallback = -1)]" {
                    minus_one.push(name);
                }
            }
        }
        minus_one.sort();
        let mut expected: Vec<String> = BYTE_CODES.iter().map(|name| name.to_string()).collect();
        expected.sort();
        assert_eq!(minus_one, expected);
        assert!(i8_exports >= 263, "only {i8_exports} int8_t exports found");
    }

    /// The scanner itself: a guard directly above passes, including one whose
    /// attribute spans lines, with or without a `// SAFETY:` comment inside its
    /// closure; anything else between the guard and `#[unsafe(no_mangle)]`
    /// (another attribute, a comment line), or no guard at all, fails.
    #[test]
    fn test_preceded_by_ffi_guard() {
        let guarded = ["/// Docs.", "#[ffi_guard]", "#[unsafe(no_mangle)]"];
        assert!(preceded_by_ffi_guard(&guarded, 2));
        let multi_line = [
            "#[ffi_guard(",
            "    on_panic = |err| unsafe { callback(box_error(err), user_data) }",
            ")]",
            "#[unsafe(no_mangle)]",
        ];
        assert!(preceded_by_ffi_guard(&multi_line, 3));
        let safety_comment_inside = [
            "#[ffi_guard(on_panic = |err| {",
            "    // SAFETY: the guard fires the caller's own callback once.",
            "    unsafe { callback(box_error(err), user_data) }",
            "})]",
            "#[unsafe(no_mangle)]",
        ];
        assert!(preceded_by_ffi_guard(&safety_comment_inside, 4));
        let comment_between = ["#[ffi_guard]", "// see [D8]", "#[unsafe(no_mangle)]"];
        assert!(!preceded_by_ffi_guard(&comment_between, 2));
        let other_attribute_between = ["#[ffi_guard]", "#[allow(dead_code)]", "#[unsafe(no_mangle)]"];
        assert!(!preceded_by_ffi_guard(&other_attribute_between, 2));
        let unguarded = ["/// Docs.", "#[unsafe(no_mangle)]"];
        assert!(!preceded_by_ffi_guard(&unguarded, 1));
        let first_line = ["#[unsafe(no_mangle)]"];
        assert!(!preceded_by_ffi_guard(&first_line, 0));
    }
}
