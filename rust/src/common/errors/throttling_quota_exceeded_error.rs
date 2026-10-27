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
use crate::common::error::{ErrorCode, ErrorHierarchy, ErrorMessage, ErrorSource};
use crate::common::protocol::Errors;

/// Error thrown if an operation on a resource exceeds the throttling quota.
///
/// # Translation notes
///
/// Error code `Errors::ThrottlingQuotaExceeded`.
// Hand-written rather than declared with `kafka_error_type!` because it adds the
// `throttleTimeMs` field.
#[derive(Clone, Debug)]
#[doc(alias = "org.apache.kafka.common.errors.ThrottlingQuotaExceededException")]
pub struct ThrottlingQuotaExceededError {
    message: String,
    /// The amount of time to wait before retrying, in milliseconds.
    pub(crate) throttle_time_ms: i32,
}

impl ThrottlingQuotaExceededError {
    /// Creates a throttling quota exceeded error.
    ///
    /// # Arguments
    ///
    /// - `throttle_time_ms` - the amount of time to wait before retrying, in milliseconds
    /// - `message` - the error's message
    #[doc(alias = "org.apache.kafka.common.errors.ThrottlingQuotaExceededException#ThrottlingQuotaExceededException")]
    pub fn new(throttle_time_ms: i32, message: impl Into<String>) -> Self {
        Self { message: message.into(), throttle_time_ms }
    }

    /// Creates the error with the default message of `Errors::ThrottlingQuotaExceeded`, as the
    /// broker's error code is turned into an error.
    ///
    /// # Arguments
    ///
    /// - `throttle_time_ms` - the amount of time to wait before retrying, in milliseconds
    #[doc(alias = "org.apache.kafka.common.errors.ThrottlingQuotaExceededException#ThrottlingQuotaExceededException")]
    pub fn with_default_message(throttle_time_ms: i32) -> Self {
        Self::new(throttle_time_ms, Errors::ThrottlingQuotaExceeded.message())
    }

    /// Returns the error message: Java's `getMessage()`.
    #[doc(alias = "rust-only")]
    pub fn message(&self) -> &str {
        &self.message
    }

    /// Returns the amount of time to wait before retrying, in milliseconds.
    #[doc(alias = "org.apache.kafka.common.errors.ThrottlingQuotaExceededException#throttleTimeMs")]
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
    fn is_throttling_quota_exceeded_error(&self) -> bool {
        true
    }
}

impl ErrorSource for ThrottlingQuotaExceededError {
    // `ThrottlingQuotaExceededException` exposes no `Throwable cause` constructor, so its cause is
    // always null in Java; the trait default (`None`) is that answer.
}

impl ThrottlingQuotaExceededError {
    /// Returns the underlying cause: always `None`, as Java's class has no constructor taking
    /// one.
    // An inherent method, so it shadows both `ErrorSource::source` and
    // `std::error::Error::source`, keeping `x.source()` unambiguous and typed.
    #[doc(alias = "rust-only")]
    pub fn source(&self) -> Option<&Error> {
        None
    }
}

impl std::error::Error for ThrottlingQuotaExceededError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Option::<&Error>::None.map(|e| e as &(dyn std::error::Error + 'static))
    }
}
