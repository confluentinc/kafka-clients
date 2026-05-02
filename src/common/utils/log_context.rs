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

//! Provides contextual log message prefixes for Kafka client components.
//!
//! Translated from `org.apache.kafka.common.utils.LogContext`.
//!
//! This class provides a way to instrument loggers with a common context which
//! can be used to automatically enrich log messages. For example, in the
//! KafkaProducer, it is often useful to know the clientId of the producer, so
//! this can be added to a context object which can then be passed to all of the
//! dependent components. This removes the need to manually add the clientId to
//! each message.

/// Provides contextual log message prefixes, matching Java's `LogContext`.
///
/// Created once per client instance and passed to all components.
/// The prefix is prepended to every log message automatically via
/// the `kafka_*` logging macros.
///
/// Translated from `org.apache.kafka.common.utils.LogContext`.
#[derive(Clone, Debug)]
pub struct LogContext {
    prefix: String,
}

impl LogContext {
    /// Creates a new `LogContext` with the given prefix string.
    ///
    /// # Arguments
    ///
    /// * `prefix` - The prefix to prepend to all log messages.
    ///
    /// # Examples
    ///
    /// ```
    /// use confluent_kafka::common::utils::LogContext;
    ///
    /// let ctx = LogContext::new(format!("[Producer clientId={}] ", "my-producer"));
    /// assert_eq!(ctx.prefix(), "[Producer clientId=my-producer] ");
    /// ```
    pub fn new(prefix: impl Into<String>) -> Self {
        Self { prefix: prefix.into() }
    }

    /// Creates a new `LogContext` with an empty prefix.
    ///
    /// Corresponds to Java's no-arg `LogContext()` constructor.
    pub fn empty() -> Self {
        Self { prefix: String::new() }
    }

    /// Returns the log prefix string.
    pub fn prefix(&self) -> &str {
        &self.prefix
    }
}

impl Default for LogContext {
    fn default() -> Self {
        Self::empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_with_prefix() {
        let ctx = LogContext::new("[Producer clientId=test] ");
        assert_eq!(ctx.prefix(), "[Producer clientId=test] ");
    }

    #[test]
    fn test_empty() {
        let ctx = LogContext::empty();
        assert_eq!(ctx.prefix(), "");
    }

    #[test]
    fn test_default() {
        let ctx = LogContext::default();
        assert_eq!(ctx.prefix(), "");
    }

    #[test]
    fn test_clone() {
        let ctx = LogContext::new("[Producer clientId=test] ");
        let cloned = ctx.clone();
        assert_eq!(ctx.prefix(), cloned.prefix());
    }

    #[test]
    fn test_debug() {
        let ctx = LogContext::new("[Producer clientId=test] ");
        let debug_str = format!("{:?}", ctx);
        assert!(debug_str.contains("LogContext"));
        assert!(debug_str.contains("Producer clientId=test"));
    }

    #[test]
    fn test_new_with_format() {
        let client_id = "producer-1";
        let ctx = LogContext::new(format!("[Producer clientId={}] ", client_id));
        assert_eq!(ctx.prefix(), "[Producer clientId=producer-1] ");
    }

    #[test]
    fn test_new_with_null_equivalent() {
        // Java: new LogContext(null) => prefix becomes ""
        // Rust: we use empty() for the null case
        let ctx = LogContext::empty();
        assert_eq!(ctx.prefix(), "");
    }
}
