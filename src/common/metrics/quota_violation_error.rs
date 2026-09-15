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

//! `org.apache.kafka.common.metrics.QuotaViolationException`.

use crate::common::kafka_error::{ErrorCode, ErrorHierarchy, ErrorMessage, ErrorSource};
use crate::common::{Error, MetricName};

/// Raised when a sensor records a value that takes a metric outside the bounds
/// configured as its quota.
///
/// Corresponds to Java's `QuotaViolationException`. It has no entry in `Errors`,
/// so it carries no protocol code.
///
/// Java `extends` chain:
///    `QuotaViolationException` -> `KafkaException`
///
/// Hand-written rather than declared with `kafka_error_class!` because it adds
/// subclass state (the metric, the recorded value and the bound) and overrides
/// `toString()` instead of taking the generated
/// `"<TypeName>: <message>"` form.
///
/// Two deliberate deviations from the Java class:
///
///  - Java holds the `KafkaMetric` itself and exposes it as `metric()`. This
///    holds the metric's [`MetricName`], because `KafkaMetric` owns a
///    `Box<dyn Measurable>` and so is neither `Clone` nor `Debug` — both of
///    which [`Error`] requires. Nothing is lost for a client: Java's own
///    `toString()` uses only `metric.metricName()`, and the client has no caller
///    of `metric()` (the broker's `ClientQuotaManager` is the only one in the
///    Kafka tree).
///  - Java overrides `fillInStackTrace()` to skip capturing a stack trace, since
///    quota violations are frequent and the trace is never read. Rust errors do
///    not capture one at all, so there is nothing to suppress.
#[derive(Clone, Debug)]
pub struct QuotaViolationError {
    metric_name: MetricName,
    value: f64,
    bound: f64,
    source: Option<Box<Error>>,
}

impl QuotaViolationError {
    /// Create the error, mirroring Java's
    /// `QuotaViolationException(KafkaMetric metric, double value, double bound)`.
    pub fn new(metric_name: MetricName, value: f64, bound: f64) -> Self {
        Self { metric_name, value, bound, source: None }
    }

    /// The name of the metric that violated its quota.
    ///
    /// Stands in for Java's `metric()` — see the type-level note.
    pub fn metric_name(&self) -> &MetricName {
        &self.metric_name
    }

    /// The value that was recorded. Mirrors Java's `value()`.
    pub fn value(&self) -> f64 {
        self.value
    }

    /// The quota bound that was crossed. Mirrors Java's `bound()`.
    pub fn bound(&self) -> f64 {
        self.bound
    }

    /// The underlying cause, if any. Mirrors Java's `getCause()`.
    pub fn source(&self) -> Option<&Error> {
        self.source.as_deref()
    }
}

impl ErrorSource for QuotaViolationError {
    fn source(&self) -> Option<&Error> {
        self.source.as_deref()
    }
}

impl std::error::Error for QuotaViolationError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.source.as_deref().map(|e| e as &(dyn std::error::Error + 'static))
    }
}

impl ErrorMessage for QuotaViolationError {
    /// Java's constructor calls the no-argument `super()`, so `getMessage()`
    /// returns `null`; the empty string is this crate's spelling of that. The
    /// descriptive text lives in `Display`, exactly as it does in Java's
    /// `toString()` override.
    fn message(&self) -> &str {
        ""
    }
}

// No protocol code: `QuotaViolationException` has no entry in `Errors.java`, and
// walking its superclasses (Java's `Errors.forException`) finds none either, so
// the trait default of `Errors::UnknownServerError` is the right answer.
impl ErrorCode for QuotaViolationError {}

impl ErrorHierarchy for QuotaViolationError {
    fn is_kafka_error(&self) -> bool {
        true
    }
}

impl std::fmt::Display for QuotaViolationError {
    /// Java's `toString()` override:
    /// `getClass().getName() + ": '" + metric.metricName() + "' violated quota.
    /// Actual: " + value + ", Threshold: " + bound`.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "QuotaViolationError: '{}' violated quota. Actual: {}, Threshold: {}",
            self.metric_name, self.value, self.bound
        )
    }
}
