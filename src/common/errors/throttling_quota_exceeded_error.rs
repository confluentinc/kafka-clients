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

//! Translated from `org.apache.kafka.common.errors.ThrottlingQuotaExceededException`.

use std::fmt;

use crate::common::Error;
use crate::common::Errors;
use crate::common::error::{ErrorCode, ErrorHierarchy, ErrorMessage, ErrorSource};

/// The request was throttled due to a quota violation.
///
/// Corresponds to Java's `ThrottlingQuotaExceededException`, error code
/// [`Errors::ThrottlingQuotaExceeded`].
///
/// Java `extends` chain:
///    `ThrottlingQuotaExceededException` -> `RetriableException` ->
///   `ApiException` -> `KafkaException`
///
/// Hand-written rather than declared with `kafka_error_class!` because it adds
/// a field: Java's constructor is
/// `ThrottlingQuotaExceededException(int throttleTimeMs, String message)`.
#[derive(Clone, Debug)]
pub struct ThrottlingQuotaExceededError {
    message: String,
    /// The amount of time to wait before retrying, in milliseconds.
    pub throttle_time_ms: i32,
}

impl ThrottlingQuotaExceededError {
    /// Create a new throttling quota exceeded error.
    pub fn new(throttle_time_ms: i32, message: impl Into<String>) -> Self {
        Self { message: message.into(), throttle_time_ms }
    }

    /// Create the error with the default message for its error code.
    pub fn with_default_message(throttle_time_ms: i32) -> Self {
        Self::new(throttle_time_ms, Errors::ThrottlingQuotaExceeded.message())
    }

    /// The error message.
    pub fn message(&self) -> &str {
        &self.message
    }

    /// The amount of time to wait before retrying, in milliseconds.
    ///
    /// Mirrors Java's `ThrottlingQuotaExceededException.throttleTimeMs()`.
    pub fn throttle_time_ms(&self) -> i32 {
        self.throttle_time_ms
    }
}

impl fmt::Display for ThrottlingQuotaExceededError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ThrottlingQuotaExceededError: {}", self.message)
    }
}

impl ErrorMessage for ThrottlingQuotaExceededError {
    fn message(&self) -> &str {
        &self.message
    }
}

impl ErrorCode for ThrottlingQuotaExceededError {
    fn error(&self) -> Errors {
        Errors::ThrottlingQuotaExceeded
    }
}

impl ErrorHierarchy for ThrottlingQuotaExceededError {
    fn is_kafka_error(&self) -> bool {
        true
    }
    fn is_api_error(&self) -> bool {
        true
    }
    fn is_retriable_error(&self) -> bool {
        true
    }
}

impl ErrorSource for ThrottlingQuotaExceededError {
    // `ThrottlingQuotaExceededException` exposes no `Throwable cause` constructor, so its cause is
    // always null in Java; the trait default (`None`) is that answer.
}

impl ThrottlingQuotaExceededError {
    /// Always `None`: `ThrottlingQuotaExceededException` exposes no `Throwable cause` constructor, so its
    /// cause is null in Java too. Present as an inherent method so it shadows
    /// both `ErrorSource::source` and `std::error::Error::source`, keeping
    /// `x.source()` unambiguous and typed.
    pub fn source(&self) -> Option<&Error> {
        None
    }
}

impl std::error::Error for ThrottlingQuotaExceededError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Option::<&Error>::None.map(|e| e as &(dyn std::error::Error + 'static))
    }
}
