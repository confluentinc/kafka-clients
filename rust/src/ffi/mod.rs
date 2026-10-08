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
//! queues for its `_execute_callbacks` live in [`callback_queue`].
//!
//! A `*_count` accessor is never negative in any of these modules: a count feeds
//! straight into `malloc(count * n)` and into `for (size_t i = 0; i < count; i++)`
//! on the C side, so an in-band sentinel there would be a memory-safety hazard.
//! Where a Java collection is nullable and null must stay distinct from empty,
//! the count reports 0 and a separate `*_has_<field>` predicate carries the
//! presence bit — see the "Counts are never negative" section of `admin`.
//!
//! # Feature Gate
//!
//! This module is only compiled when the `ffi` feature is enabled.

pub(crate) mod admin;
pub(crate) mod callback_queue;
pub(crate) mod common;
pub(crate) mod consumer;
pub(crate) mod consumer_handle;
mod error_predicates;
pub(crate) mod kafka_future;
pub(crate) mod producer;
pub(crate) mod util;
