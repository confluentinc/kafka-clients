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

//! C FFI layer for the Kafka producer API.
//!
//! This module exposes the Producer API via C-callable `extern "C"` functions,
//! allowing non-Rust code (C, C++, Python via ctypes, etc.) to use the Kafka
//! producer.
//!
//! All types exposed across the FFI boundary use fixed-width types (`i32`,
//! `i64`, `bool`, pointers) for cross-platform portability. Lengths and counts
//! use `i32`, and negative values (-1) signal "not set" for optional fields.
//!
//! # Feature Gate
//!
//! This module is only compiled when the `ffi` feature is enabled.

pub(crate) mod producer;
