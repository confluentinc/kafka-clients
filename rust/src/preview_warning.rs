// Copyright 2026 Confluent Inc.
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

//! Preview-status warning logged when a client starts.
//!
//! Rust-only: there is no Java counterpart. It exists so every client
//! (`KafkaProducer`, `AsyncKafkaConsumer`, `KafkaAdminClient`) logs the same
//! text from one place while the crate is below 1.0.

use crate::common::utils::internals::LogContext;
use crate::kafka_warn;

/// The warning logged once by every client on startup.
pub(crate) const PREVIEW_WARNING: &str = "The Rust client is in preview and not yet recommended for production \
     use. The public API is not stable before the 1.0 GA release and may change. We welcome your feedback while \
     the design is still open";

/// Logs [`PREVIEW_WARNING`] at WARN level with the client's `LogContext` prefix.
pub(crate) fn log_preview_warning(log_context: &LogContext) {
    kafka_warn!(log_context, "{}", PREVIEW_WARNING);
}
