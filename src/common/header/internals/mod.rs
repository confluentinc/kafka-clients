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

//! Translation of `org.apache.kafka.common.header.internals`.
//!
//! Per CLAUDE.md rule 2 ("Classes whose package contains `internal` MUST use
//! only `pub(crate)`"), every type in this module is crate-private at the
//! file level, with public re-exports through the parent `header` module
//! when needed by external code (e.g. `ProducerRecord::headers()` returns a
//! reference to a `RecordHeaders`).

pub mod record_header;
pub mod record_headers;

pub use record_header::RecordHeader;
pub use record_headers::{RecordHeaders, RecordHeadersError};
