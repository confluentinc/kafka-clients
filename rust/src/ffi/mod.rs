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
//! All types exposed across the FFI boundary use fixed-width types (`i32`,
//! `i64`, `bool`, pointers) for cross-platform portability. Lengths and counts
//! use `i32`, and negative values (-1) signal "not set" for optional **scalar**
//! fields and for out-of-range element accessors.
//!
//! A `*_count` accessor is never negative in any of these modules: a count feeds
//! straight into `malloc(count * n)` and into `for (size_t i = 0; i < count; i++)`
//! on the C side, so an in-band sentinel there would be a memory-safety hazard.
//! Where a Java collection is nullable and null must stay distinct from empty,
//! the count reports 0 and a separate `*_has_<field>` predicate carries the
//! presence bit — see the "Counts are never negative" section of `admin`.
//!
//! # Panics never cross the C boundary
//!
//! Every exported function catches a Rust panic before it can unwind into its
//! caller, which would abort the process. After a caught panic the function
//! returns its failure value instead: NULL, `false`, -1 (0 for a `*_count`
//! accessor), NaN for a `double`, `kafka_common_ErrorCode_UNKNOWN_SERVER_ERROR`
//! for `kafka_common_Error_code`, or an error handle for a function that returns
//! one. A function with an `out_error` parameter also stores an error describing
//! the panic there. A function that reports ordinary failures through a
//! completion callback reports the panic through that callback instead, exactly
//! once, unless its documentation says that a synchronous failure does not
//! invoke the callback. Such a function reports the panic as a synchronous
//! failure, through its return value and `out_error` where it has one, and the
//! guard itself does not invoke the callback. If such a panic is raised after
//! the record or request was already handed to the library's background work,
//! the callback can still fire for it later; on a return that reports a panic,
//! do not release `user_data` yourself: the callback, or the
//! `user_data_destroy` hook where there is one, releases it when it runs. The
//! error's code is `kafka_common_ErrorCode_LOCAL_ILLEGAL_STATE` and its message
//! begins "Rust panic caught at the FFI boundary in <function>".
//!
//! A panic is a bug in this library and may leave the handle it happened on in
//! an inconsistent state: destroy that handle and create a new one. Later calls
//! on it can keep failing, because a poisoned lock is never cleared.
//!
//! The same paragraphs open the generated C header (`header` in
//! `cbindgen.toml`). The mechanism is the [`ffi_guard`] attribute, whose runtime
//! half is `common::ffi_guard_or`.
//!
//! # Feature Gate
//!
//! This module is only compiled when the `ffi` feature is enabled.

pub(crate) mod admin;
pub(crate) mod common;
pub(crate) mod consumer;
pub(crate) mod consumer_handle;
pub(crate) mod producer;

/// Guards an exported function against a Rust panic unwinding into its C
/// caller; see the module docs and the `ffi-macros` crate.
pub(crate) use ffi_macros::ffi_guard;

#[cfg(test)]
mod tests {
    /// Every file in this module that exports functions, with its source text.
    const SOURCES: [(&str, &str); 5] = [
        ("src/ffi/admin.rs", include_str!("admin.rs")),
        ("src/ffi/common.rs", include_str!("common.rs")),
        ("src/ffi/consumer.rs", include_str!("consumer.rs")),
        ("src/ffi/consumer_handle.rs", include_str!("consumer_handle.rs")),
        ("src/ffi/producer.rs", include_str!("producer.rs")),
    ];

    /// Whether the attribute that ends on the line just above `index` is an
    /// `#[ffi_guard...]`. The attribute may span several lines, so this walks up
    /// from its last line to the line that opens it.
    fn preceded_by_ffi_guard(lines: &[&str], index: usize) -> bool {
        let Some(mut line) = index.checked_sub(1) else {
            return false;
        };
        if !lines[line].trim_end().ends_with(']') {
            return false;
        }
        loop {
            let text = lines[line].trim_start();
            if text.starts_with("#[") {
                return text.starts_with("#[ffi_guard");
            }
            if line == 0 || text.is_empty() || text.starts_with("//") {
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
        for (file, text) in SOURCES {
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
            assert!(exported > 0, "{file} exports no function; the list of sources is stale");
            total += exported;
        }
        assert!(
            offenders.is_empty(),
            "exported functions without #[ffi_guard] directly above #[unsafe(no_mangle)]: {offenders:?}"
        );
        // Sanity check that the scan saw the whole surface rather than, say, a
        // renamed attribute spelling: 811 when the guard was introduced, revised
        // to 788 after merging master's removal of 23 deprecated/internal
        // functions (listClientMetricsResources, ConsumerGroupListing,
        // ConsumerGroupDescription_state, ListConsumerGroupsResult,
        // listConsumerGroups, CorrelationIdMismatchError and its 3 accessors,
        // ConsumerGroupMetadata_new, Consumer_close_with_timeout) that are no
        // longer part of Java's public API surface (CLAUDE.md §3).
        assert!(total >= 788, "only {total} exported functions found");
    }

    /// The scanner itself: a guard directly above passes, including one whose
    /// attribute spans lines; anything else between the guard and
    /// `#[unsafe(no_mangle)]`, or no guard at all, fails.
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
        let other_attribute_between = ["#[ffi_guard]", "#[allow(dead_code)]", "#[unsafe(no_mangle)]"];
        assert!(!preceded_by_ffi_guard(&other_attribute_between, 2));
        let unguarded = ["/// Docs.", "#[unsafe(no_mangle)]"];
        assert!(!preceded_by_ffi_guard(&unguarded, 1));
        let first_line = ["#[unsafe(no_mangle)]"];
        assert!(!preceded_by_ffi_guard(&first_line, 0));
    }
}
