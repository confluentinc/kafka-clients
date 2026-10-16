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
//! only `u8` is the `data` pointer inside `kafka_Bytes_t`. Lengths and counts
//! use `i32`, and negative values (-1) signal "not set" for optional
//! **scalar** fields and for out-of-range element accessors.
//!
//! A fallible function returns its value (or nothing) and takes a trailing
//! `out_err: *mut *mut kafka_common_Error_t`, which must not be null and must
//! point at null on entry; on failure it stores an owned error there and
//! returns its failure value (`common::FailValue`: null, a null
//! `kafka_Bytes_t`, 0 for an `i8`, -1 for another integer). Owned
//! strings are freed with `kafka_string_destroy`; collections cross as the
//! package-less `kafka_List_t` and `kafka_Map_t` of `void *`, byte buffers as
//! `kafka_Bytes_t`. A Java `KafkaFuture<T>` is `kafka_common_KafkaFuture_t`;
//! the callbacks a client queues for its `__execute_callbacks` live in the
//! crate-private `callback_queue` module. The C types and functions are
//! documented in the generated header rather than here.
//!
//! A nullable Java collection crosses as a null `kafka_List_t *` /
//! `kafka_Map_t *` (distinct from an empty one); `kafka_List_size` and
//! `kafka_Map_size` are never negative, since a size feeds straight into
//! `malloc(size * n)` and `for (int32_t i = 0; i < size; i++)` on the C side.
//! The first client created from C initializes the default `RUST_LOG` logger
//! (`init_default_logger`).
//!
//! # Panics never cross the C boundary
//!
//! Every exported function catches a Rust panic before it can unwind into its
//! caller, which would abort the process, and reports it instead. A fallible
//! function stores the panic in `*out_err` and returns its failure value, as
//! for any other failure: test `*out_err`, not the value, since null and -1
//! can be valid answers. The error's code is
//! `kafka_common_Error_LOCAL_ILLEGAL_STATE` and its message begins
//! `Rust panic caught at the FFI boundary in <function>`. Any other function
//! has no `out_err`: it logs the panic and, unless it is a `_cb` function
//! (below), returns the failure value of its return type:
//!
//! - a pointer: NULL;
//! - an `int8_t`: 0, false for a boolean. An `int8_t` carrying a Java `byte`
//!   code whose 0 is a valid constant (`kafka_common_IsolationLevel_id`, say)
//!   returns -1 instead, as its documentation states;
//! - another integer: -1, but 0 for a `*_count` or `*_size` accessor, a size
//!   that returns -1 for a null key or value
//!   (`kafka_consumer_ConsumerRecord_serialized_key_size`) included. An
//!   integer whose Java domain holds that value but leaves out another one
//!   returns the other instead, as its documentation states
//!   (`kafka_admin_NewTopic_replication_factor` returns 0). In particular an
//!   integer accessor that maps Java's empty `Optional`, or `null`, to -1
//!   returns -2 when its Java domain is that or a value >= 0
//!   (`kafka_consumer_ConsumerRecord_leader_epoch`,
//!   `kafka_producer_ProducerRecord_partition`), and keeps -1 when Java gives
//!   -2 a meaning (`kafka_admin_MemberDescription_member_epoch`, where -2 is a
//!   static member that left, and the admin options' `timeout_ms`), or when no
//!   value is outside its domain (`kafka_common_Error_throttle_time_ms`, which
//!   holds any `int` Java's constructor was given);
//! - a `double`: NaN;
//! - a `kafka_Bytes_t`: a NULL one (Java's `null` array);
//! - `kafka_common_Error_e` (`kafka_common_Error__enum`):
//!   `kafka_common_Error_UNKNOWN_SERVER_ERROR`; another C enum: the
//!   enumerator its documentation names, its `UNKNOWN` one when it has one;
//! - a borrowed `const kafka_common_Error_t *` (`kafka_common_Error_source`):
//!   NULL, as for no error.
//!
//! A function returning nothing only logs the panic.
//!
//! The failure value is not always distinct from a real answer: a nullable
//! pointer or `kafka_Bytes_t`, a boolean, a `double` that may be NaN, a C enum
//! without an `UNKNOWN` enumerator, and an integer whose -1 (or, for a count or
//! size, 0) is itself a valid value return a value the function can also
//! return without a panic. The integers include Java's sentinels:
//! `NO_TIMESTAMP`, an unknown offset or partition, the id of `Node.noNode()`,
//! `kafka_admin_AbortTransactionSpec_producer_epoch`, and the empty optional
//! values whose Java domain holds -2 (a member epoch, a consumer's lag, a log
//! dir's byte counts, a node id, an admin option's timeout). Such a panic shows
//! only in the error-level log line every caught panic writes.
//!
//! A `_cb` function reports a caught panic the way it reports every other
//! outcome, exactly once, whether the panic happens on the calling thread or
//! later on the task running the operation: its completion carries the error
//! (and a NULL pointer, or -1 for an `int64_t`, as its value) and the caller's
//! `opaque`, queued on the client's callback queue for the client's
//! `__execute_callbacks` to run like any other completion. A handle with no
//! callback queue (a standalone listener or `OffsetCommitCallback`, a future of
//! no client, a `ConsumerHandle` whose consumer is destroyed) runs it on the
//! calling thread before returning, as it runs every completion. A completion
//! with no error parameter (`kafka_admin_Admin_close_cb`,
//! `kafka_admin_Admin_close_with_timeout_cb`,
//! `kafka_producer_Callback_on_completion_cb`) fires with `opaque`
//! alone, the panic showing only in the log. That completion is where `opaque`
//! comes back to be released; there is no other release hook.
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
//! # Two headers
//!
//! The failure values above serve a C programmer, who can accept that a panic
//! sometimes looks like an answer. A language binding cannot: it must raise
//! whenever Rust panicked. So `build.rs` writes two headers over the same
//! library:
//!
//! - `confluent_kafka.h`, for C users: the functions above, where only those
//!   that are fallible in Rust take `out_err`;
//! - `confluent_kafka_bindings.h`, for language bindings: the same types, and
//!   a form of every function that reports every failure, a caught panic
//!   included, through a trailing `kafka_common_Error_t **out_err`.
//!
//! | `f` in `confluent_kafka.h` | in `confluent_kafka_bindings.h` |
//! |---|---|
//! | fallible: `T f(args, kafka_common_Error_t **out_err)` | `f` itself |
//! | a `_cb` function | `f_cb` itself: its completion reports every outcome, a caught panic included |
//! | any other `T f(args)`, `void` included | only `T f_err(args, kafka_common_Error_t **out_err)`, which [`ffi_guard`] emits beside `f`: on a caught panic it stores the error in `*out_err` and returns `f`'s panic value, otherwise it returns what `f` returns |
//!
//! A twin has the documentation and the preconditions of `f`, and a caught
//! panic has the code and message above, naming the twin. `out_err` has the
//! same contract everywhere: not null, `*out_err` null on entry, written only
//! on failure. A program includes one of the two headers, never both, since
//! they declare the same types. The function-pointer typedefs (interface
//! methods, `_cb_t` completions, the `__set_callbacks_notify` hook) are the
//! same in both: they are implemented by the caller, not exported.
//! `ffi::tests` pins both headers against the exports.
//!
//! # Feature Gate
//!
//! This module is only compiled when the `ffi` feature is enabled.

pub(crate) mod admin;
pub(crate) mod callback_queue;
pub(crate) mod callback_result;
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
        // renamed attribute spelling: 2307 exports after the FFI was split into
        // one file per class.
        assert!(total >= 2307, "only {total} exported functions found");
    }

    /// Each exported function with the text of the `#[ffi_guard...]` attribute
    /// above it, in source order. `test_every_exported_function_is_guarded`
    /// pins that the attribute is there.
    fn exports() -> Vec<(String, String)> {
        exports_with_signatures()
            .into_iter()
            .map(|(name, guard, _)| (name, guard))
            .collect()
    }

    /// [`exports`], each with its signature: the text from `fn` to the opening
    /// brace of its body, whitespace collapsed.
    fn exports_with_signatures() -> Vec<(String, String, String)> {
        let mut found = Vec::new();
        for (_, text) in sources() {
            let lines: Vec<&str> = text.lines().collect();
            for (index, line) in lines.iter().enumerate() {
                if line.trim() != "#[unsafe(no_mangle)]" {
                    continue;
                }
                let name = lines[index + 1..]
                    .iter()
                    .find_map(|line| line.split_once("fn ").map(|(_, rest)| rest))
                    .and_then(|rest| rest.split('(').next())
                    .expect("an exported fn has a name")
                    .to_string();
                let mut start = index;
                while start > 0 && !lines[start].trim_start().starts_with("#[ffi_guard") {
                    start -= 1;
                }
                let signature: Vec<&str> = lines[index + 1..]
                    .iter()
                    .map(|line| line.trim())
                    .take_while(|line| !line.ends_with('{'))
                    .chain(
                        lines[index + 1..]
                            .iter()
                            .map(|line| line.trim())
                            .find(|line| line.ends_with('{')),
                    )
                    .collect();
                let signature = signature.join(" ").replace("( ", "(").replace(", )", ")");
                found.push((name, lines[start..index].join("\n"), signature));
            }
        }
        found
    }

    /// The names of the functions a generated C header declares: cbindgen puts
    /// the return type and the name of each on one unindented line, while
    /// comments, typedefs (function pointers included), struct bodies and
    /// preprocessor lines start otherwise.
    fn declared_functions(header: &str) -> Vec<String> {
        let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("target/include").join(header);
        let text = std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
        let mut names: Vec<String> = text
            .lines()
            .filter(|line| {
                !line.starts_with([' ', '/', '#', '}', '{']) && !line.starts_with("typedef") && line.contains('(')
            })
            .map(|line| {
                let before = line.split('(').next().expect("split yields one piece");
                before.rsplit([' ', '*']).next().expect("rsplit yields one piece").to_string()
            })
            .collect();
        names.sort();
        names
    }

    /// `confluent_kafka.h` declares the exports, and `confluent_kafka_bindings.h`
    /// (written by `build.rs`) the fallible ones and the `_cb` ones under their
    /// own names and every other one as its `_err` twin, which `#[ffi_guard]`
    /// emits: both are pinned against the source, so a header that lost or
    /// gained a function, or a twin that `build.rs` and the macro name or
    /// select differently, fails here. A fallible export is one whose last
    /// parameter is `out_err`; no other parameter is named so. No export of its
    /// own ends in `_err`, which would collide with the twin of another one.
    #[test]
    fn test_bindings_header_declares_the_twin_of_every_export() {
        const OUT_ERR: &str = "out_err: *mut *mut kafka_common_Error_t";
        let exports = exports_with_signatures();
        let misplaced: Vec<&String> = exports
            .iter()
            .filter(|(_, _, signature)| {
                let fallible =
                    signature.contains(&format!("{OUT_ERR})")) || signature.contains(&format!("{OUT_ERR},)"));
                names_out_err(signature) && !fallible
            })
            .map(|(name, _, _)| name)
            .collect();
        assert!(misplaced.is_empty(), "`out_err` not the last parameter: {misplaced:?}");
        let fallible = |signature: &str| names_out_err(signature);
        let mut names: Vec<String> = exports.iter().map(|(name, _, _)| name.clone()).collect();
        names.sort();
        let hand_written: Vec<&String> = names.iter().filter(|name| name.ends_with("_err")).collect();
        assert!(hand_written.is_empty(), "exports named like an `_err` twin: {hand_written:?}");
        assert_eq!(declared_functions("confluent_kafka.h"), names);
        let mut bindings: Vec<String> = exports
            .iter()
            .map(|(name, _, signature)| {
                if fallible(signature) || name.ends_with("_cb") {
                    name.clone()
                } else {
                    format!("{name}_err")
                }
            })
            .collect();
        bindings.sort();
        assert_eq!(declared_functions("confluent_kafka_bindings.h"), bindings);
        let plain = exports
            .iter()
            .filter(|(name, _, signature)| fallible(signature) || name.ends_with("_cb"))
            .count();
        assert!(names.len() >= 2315, "only {} exported functions found", names.len());
        assert!(plain >= 261, "only {plain} fallible or `_cb` exports found");
    }

    /// Whether `text` names the identifier `out_err`, not merely contains it
    /// (`kafka_common_Error_is_timeout_error` does).
    fn names_out_err(text: &str) -> bool {
        let is_ident = |c: char| c.is_ascii_alphanumeric() || c == '_';
        text.match_indices("out_err")
            .any(|(at, word)| !text[..at].ends_with(is_ident) && !text[at + word.len()..].starts_with(is_ident))
    }

    /// Every fallible export declares `out_err` last in `confluent_kafka.h` too,
    /// with the C type the contract names.
    #[test]
    fn test_fallible_declarations_end_in_out_err() {
        let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("target/include/confluent_kafka.h");
        let text = std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
        // Drop the comments, which name `out_err` in prose.
        let mut code = String::new();
        let mut rest = text.as_str();
        while let Some((before, after)) = rest.split_once("/*") {
            code.push_str(before);
            rest = after.split_once("*/").map_or("", |(_, after)| after);
        }
        code.push_str(rest);
        let declarations = code.split(';').filter(|item| names_out_err(item));
        let wrong: Vec<&str> = declarations
            .filter(|item| !item.trim_end().ends_with("kafka_common_Error_t **out_err)"))
            .collect();
        assert!(wrong.is_empty(), "{wrong:#?}");
    }

    /// The exports whose `#[ffi_guard]` takes `on_panic` are exactly the `_cb`
    /// ones, which report a caught panic through their completion. The macro
    /// gives an `on_panic` export no twin, and `build.rs`, on that ground,
    /// declares every `_cb` function under its own name.
    #[test]
    fn test_on_panic_exports_are_the_cb_exports() {
        let mismatched: Vec<String> = exports()
            .into_iter()
            .filter(|(name, guard)| guard.contains("on_panic") != name.ends_with("_cb"))
            .map(|(name, _)| name)
            .collect();
        assert!(
            mismatched.is_empty(),
            "`on_panic` without `_cb`, or `_cb` without `on_panic`: {mismatched:?}"
        );
    }

    /// The `int8_t` exports carrying a Java `byte` code: each enum's domain
    /// includes 0 (`IsolationLevel.READ_UNCOMMITTED`, `AlterConfigOp.OpType.SET`,
    /// `ElectionType.PREFERRED`, or an `UNKNOWN`) and none includes -1, so a
    /// caught panic must return -1 rather than the derived 0 the remaining
    /// `int8_t` exports return. Those are booleans, and one tri-state
    /// `Optional<Boolean>` (`kafka_admin_MemberDescription_upgraded`: 1, 0, or -1
    /// for empty) whose every value, 0 included, is legal, so no fallback could
    /// signal the panic. Pins the set in both directions: a code guarded with the
    /// bare attribute, or a boolean given -1, fails.
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

    /// Every export guarded with an explicit `fallback`, other than the byte
    /// codes pinned above, with the value it returns on a caught panic: the
    /// borrowed `const kafka_common_Error_t *` getters (NULL, as for no error),
    /// the C enum getters (their `UNKNOWN` enumerator, or the first one when the
    /// enum has none) and the integers whose derived value is a valid answer
    /// (`NewTopic` counts, where -1 is the broker default; `*_count` / `*_size`
    /// accessors whose 0 is legal; `TimestampType.id`, whose -1 is
    /// `NO_TIMESTAMP_TYPE`; the accessors mapping Java's empty `Optional`, or
    /// null, to -1, whose Java domain is that or a value >= 0, so they return
    /// -2). An enum value is compared by its last path segment.
    /// Pins the set in both directions, so a new explicit fallback, or one
    /// dropped back to the derived value, is a reviewed change.
    #[test]
    fn test_explicit_fallbacks() {
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
        const EXPECTED: [(&str, &str); 72] = [
            ("kafka_admin_AlterConfigOp_OpType__enum", "kafka_admin_AlterConfigOp_OpType_SET"),
            (
                "kafka_admin_ConfigEntry_ConfigSource__enum",
                "kafka_admin_ConfigEntry_ConfigSource_UNKNOWN",
            ),
            (
                "kafka_admin_ConfigEntry_ConfigType__enum",
                "kafka_admin_ConfigEntry_ConfigType_UNKNOWN",
            ),
            ("kafka_admin_ConsumerGroupDescription_group_epoch", "-2"),
            ("kafka_admin_ConsumerGroupDescription_target_assignment_epoch", "-2"),
            ("kafka_admin_DeleteAclsResult_FilterResult_error", "null()"),
            ("kafka_admin_FeatureMetadata_finalized_features_epoch", "-2"),
            (
                "kafka_admin_FeatureUpdate_UpgradeType__enum",
                "kafka_admin_FeatureUpdate_UpgradeType_UNKNOWN",
            ),
            ("kafka_admin_ListOffsetsResult_ListOffsetsResultInfo_leader_epoch", "-2"),
            ("kafka_admin_LogDirDescription_error", "null()"),
            ("kafka_admin_NewTopic_num_partitions", "0"),
            ("kafka_admin_NewTopic_replication_factor", "0"),
            ("kafka_admin_OffsetSpec__enum", "kafka_admin_OffsetSpec_EARLIEST"),
            ("kafka_admin_ProducerState_coordinator_epoch", "-2"),
            ("kafka_admin_ProducerState_current_transaction_start_offset", "-2"),
            ("kafka_admin_ReplicaInfo_size", "-1"),
            ("kafka_admin_ScramMechanism__enum", "kafka_admin_ScramMechanism_UNKNOWN"),
            ("kafka_admin_TransactionDescription_transaction_start_time_ms", "-2"),
            ("kafka_admin_TransactionState__enum", "kafka_admin_TransactionState_UNKNOWN"),
            (
                "kafka_admin_UserScramCredentialAlteration__enum",
                "kafka_admin_UserScramCredentialAlteration_UPSERTION",
            ),
            ("kafka_common_ClassicGroupState__enum", "kafka_common_ClassicGroupState_UNKNOWN"),
            ("kafka_common_Cluster_partition_count_for_topic", "-2"),
            ("kafka_common_ConsumerCommitFailedError_source", "null()"),
            ("kafka_common_ConsumerLogTruncationError_source", "null()"),
            ("kafka_common_ConsumerNoOffsetForPartitionError_source", "null()"),
            ("kafka_common_ConsumerOffsetOutOfRangeError_source", "null()"),
            ("kafka_common_ConsumerRetriableCommitFailedError_source", "null()"),
            ("kafka_common_DuplicateResourceError_source", "null()"),
            ("kafka_common_ElectionType__enum", "kafka_common_ElectionType_PREFERRED"),
            ("kafka_common_Error_source", "null()"),
            ("kafka_common_GroupAuthorizationError_source", "null()"),
            ("kafka_common_GroupState__enum", "kafka_common_GroupState_UNKNOWN"),
            ("kafka_common_GroupType__enum", "kafka_common_GroupType_UNKNOWN"),
            ("kafka_common_InvalidTopicError_source", "null()"),
            (
                "kafka_common_IsolationLevel__enum",
                "kafka_common_IsolationLevel_READ_UNCOMMITTED",
            ),
            ("kafka_common_MetricValue__enum", "kafka_common_MetricValue_DOUBLE"),
            ("kafka_common_QuotaViolationError_source", "null()"),
            (
                "kafka_common_RecordDeserializationError_DeserializationErrorOrigin__enum",
                "kafka_common_RecordDeserializationError_DeserializationErrorOrigin_KEY",
            ),
            ("kafka_common_RecordDeserializationError_source", "null()"),
            ("kafka_common_RecordTooLargeError_source", "null()"),
            ("kafka_common_ResourceNotFoundError_source", "null()"),
            ("kafka_common_ThrottlingQuotaExceededError_source", "null()"),
            ("kafka_common_TopicAuthorizationError_source", "null()"),
            ("kafka_common_TopicCollection__enum", "kafka_common_TopicCollection_TOPIC_IDS"),
            ("kafka_common_acl_AclOperation__enum", "kafka_common_acl_AclOperation_UNKNOWN"),
            (
                "kafka_common_acl_AclPermissionType__enum",
                "kafka_common_acl_AclPermissionType_UNKNOWN",
            ),
            (
                "kafka_common_config_ConfigResource_Type__enum",
                "kafka_common_config_ConfigResource_Type_UNKNOWN",
            ),
            (
                "kafka_common_config_SslClientAuth__enum",
                "kafka_common_config_SslClientAuth_REQUIRED",
            ),
            (
                "kafka_common_metrics_MetricValueProvider__enum",
                "kafka_common_metrics_MetricValueProvider_MEASURABLE",
            ),
            (
                "kafka_common_metrics_Sensor_RecordingLevel__enum",
                "kafka_common_metrics_Sensor_RecordingLevel_INFO",
            ),
            (
                "kafka_common_metrics_TimeUnit__enum",
                "kafka_common_metrics_TimeUnit_NANOSECONDS",
            ),
            ("kafka_common_metrics_stats_Rate_window_size", "-1"),
            ("kafka_common_metrics_stats_SampledStat_Sample_event_count", "-1"),
            ("kafka_common_metrics_stats_SimpleRate_window_size", "-1"),
            (
                "kafka_common_quota_ClientQuotaMatch__enum",
                "kafka_common_quota_ClientQuotaMatch_DEFAULT",
            ),
            (
                "kafka_common_record_TimestampType__enum",
                "kafka_common_record_TimestampType_NO_TIMESTAMP_TYPE",
            ),
            ("kafka_common_record_TimestampType_id", "-2"),
            (
                "kafka_common_resource_PatternType__enum",
                "kafka_common_resource_PatternType_UNKNOWN",
            ),
            (
                "kafka_common_resource_ResourceType__enum",
                "kafka_common_resource_ResourceType_UNKNOWN",
            ),
            (
                "kafka_common_security_auth_SecurityProtocol__enum",
                "kafka_common_security_auth_SecurityProtocol_PLAINTEXT",
            ),
            (
                "kafka_consumer_CloseOptions_GroupMembershipOperation__enum",
                "kafka_consumer_CloseOptions_GroupMembershipOperation_LEAVE_GROUP",
            ),
            ("kafka_consumer_CloseOptions_timeout", "-2"),
            ("kafka_consumer_ConsumerPartitionAssignor_Subscription_generation_id", "-2"),
            ("kafka_consumer_ConsumerRecord_delivery_count", "-2"),
            ("kafka_consumer_ConsumerRecord_leader_epoch", "-2"),
            ("kafka_consumer_GroupProtocol__enum", "kafka_consumer_GroupProtocol_CLASSIC"),
            ("kafka_consumer_MockConsumer_last_poll_timeout", "-2"),
            ("kafka_consumer_OffsetAndMetadata_leader_epoch", "-2"),
            ("kafka_consumer_OffsetAndTimestamp_leader_epoch", "-2"),
            ("kafka_producer_MockProducer_commit_count", "-1"),
            ("kafka_producer_ProducerRecord_partition", "-2"),
            ("kafka_producer_ProducerRecord_timestamp", "-2"),
        ];
        let mut found = Vec::new();
        for (_, text) in sources() {
            let lines: Vec<&str> = text.lines().collect();
            for (index, line) in lines.iter().enumerate() {
                if line.trim() != "#[unsafe(no_mangle)]" || index == 0 {
                    continue;
                }
                let Some(fallback) = lines[index - 1]
                    .trim()
                    .strip_prefix("#[ffi_guard(fallback = ")
                    .and_then(|rest| rest.strip_suffix(")]"))
                else {
                    continue;
                };
                let name = lines[index + 1..]
                    .iter()
                    .find_map(|line| line.split("fn ").nth(1))
                    .and_then(|rest| rest.split('(').next())
                    .expect("an exported fn has a name");
                if BYTE_CODES.contains(&name) {
                    continue;
                }
                let value = fallback.rsplit("::").next().expect("rsplit yields one item");
                found.push((name.to_string(), value.to_string()));
            }
        }
        found.sort();
        let mut expected: Vec<(String, String)> = EXPECTED
            .iter()
            .map(|(name, value)| (name.to_string(), value.to_string()))
            .collect();
        expected.sort();
        assert_eq!(found, expected);
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
