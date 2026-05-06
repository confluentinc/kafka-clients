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

//! Translation of `org.apache.kafka.common.header`.

// CLAUDE.md rule 2 mandates each Java class lives in its own file (so
// `Header` lives in `header/header.rs`). Clippy's `module_inception` lint
// would otherwise flag the same-name child module.
#[allow(clippy::module_inception)]
pub mod header;
pub mod headers;
pub mod internals;

pub use header::Header;
pub use headers::Headers;
pub use internals::{RecordHeader, RecordHeaders, RecordHeadersError};
