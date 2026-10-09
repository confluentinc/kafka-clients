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

//! Translated from `org.apache.kafka.common.errors.BootstrapResolutionException`.

use crate::common::error::kafka_error_type;

kafka_error_type! {
    /// Indicates that the `NetworkClient` was unable to resolve a DNS address within
    /// the time specified by `bootstrap.resolve.timeout.ms`
    /// (Java's `CommonClientConfigs#BOOTSTRAP_RESOLVE_TIMEOUT_MS_CONFIG`).
    ///
    /// This is an unrecoverable error: the failure is permanently attached to the client and is
    /// returned on every subsequent API call. Callers must close the client and construct a new
    /// one after resolving the underlying DNS or `bootstrap.servers` configuration issue.
    ///
    /// This error is only returned when `bootstrap.resolve.timeout.ms` is set to a positive
    /// value. This feature is evolving and may undergo compatibility-breaking changes in a minor
    /// release (Java marks the class `@InterfaceStability.Evolving`).
    ///
    /// Corresponds to Java's `BootstrapResolutionException`. It has no entry in `Errors`, so
    /// it carries no protocol code.
    ///
    /// Java `extends` chain:
    ///    `BootstrapResolutionException` -> `KafkaException`
    ///
    /// Like [`WakeupError`](super::WakeupError) it bypasses `ApiException`, so it answers
    /// `true` to `is_kafka_error` and `false` to `is_api_error` (and to every other
    /// hierarchy predicate).
    #[doc(alias = "org.apache.kafka.common.errors.BootstrapResolutionException")]
    BootstrapResolutionError,
    extends: [
        is_kafka_error,
    ],
}
