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

//! Common utility classes (org.apache.kafka.common.utils)
//!
//! # Dead-code lint
//!
//! None of the translated classes is `@InterfaceAudience.Public` in Java, so
//! they are crate-private. They are translated in full (DoD #2), but the client uses only part of it; the
//! rest has no caller yet, or only the translated tests. Nothing outside the
//! crate can reach it, so the module allows dead code rather than dropping
//! Java methods.

#![expect(dead_code)]

mod byte_utils;
mod exponential_backoff;
mod log_context;
#[macro_use]
mod log_macros;
mod producer_id_and_epoch;
// `utils.rs` inside `utils/` mirrors Java's `org.apache.kafka.common.utils.Utils`
// sitting inside the `utils` package; the struct is reached through the
// re-export below, never through this module path (CLAUDE.md §2).
#[expect(clippy::module_inception)]
mod utils;

pub(crate) use byte_utils::ByteUtils;
pub(crate) use exponential_backoff::ExponentialBackoff;
pub(crate) use log_context::LogContext;
pub(crate) use producer_id_and_epoch::ProducerIdAndEpoch;
pub(crate) use utils::Utils;
