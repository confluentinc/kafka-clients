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
//! presence bit — see the "Counts are never negative" section of [`admin`].
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
//! once. The error's code is `kafka_common_ErrorCode_LOCAL_ILLEGAL_STATE` and its
//! message begins "Rust panic caught at the FFI boundary in <function>".
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
// Transitional: plan §2.2 applies the attribute to every entry point and
// removes this allow.
#[allow(unused_imports)]
pub(crate) use ffi_macros::ffi_guard;
