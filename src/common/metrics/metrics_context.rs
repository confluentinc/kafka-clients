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

//! Additional context labels exposed alongside metrics.
//!
//! Translated from `org.apache.kafka.common.metrics.MetricsContext`.

use std::collections::HashMap;

/// The metrics namespace label key (formerly the JMX prefix).
pub const NAMESPACE: &str = "_namespace";

/// Encapsulates additional context labels exposed via a
/// [`MetricsReporter`](crate::common::metrics::MetricsReporter).
///
/// The labels always include a [`NAMESPACE`] entry indicating the component
/// exposing metrics (for example `kafka.consumer`), plus any freeform fields.
/// A label value may be absent (`None`), matching Java's nullable map values.
pub trait MetricsContext {
    /// The labels for this metrics context; never empty but values may be
    /// absent.
    fn context_labels(&self) -> &HashMap<String, Option<String>>;
}
