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
//! A nullable Java collection crosses as a null `kafka_List_t *` /
//! `kafka_Map_t *` (distinct from an empty one); `kafka_List_size` and
//! `kafka_Map_size` are never negative, since a size feeds straight into
//! `malloc(size * n)` and `for (int32_t i = 0; i < size; i++)` on the C side.
//! The first client created from C initializes the default `RUST_LOG` logger
//! ([`common::init_default_logger`]).
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
