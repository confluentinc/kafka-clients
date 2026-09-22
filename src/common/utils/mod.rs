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

mod exponential_backoff;
mod log_context;
#[macro_use]
mod log_macros;
mod producer_id_and_epoch;
// `utils.rs` inside `utils/` mirrors Java's `org.apache.kafka.common.utils.Utils`
// sitting inside the `utils` package; the struct is reached through the
// re-export below, never through this module path (CLAUDE.md §2).
#[allow(clippy::module_inception)]
mod utils;

pub use exponential_backoff::ExponentialBackoff;
pub use log_context::LogContext;
pub use producer_id_and_epoch::ProducerIdAndEpoch;
pub use utils::Utils;
