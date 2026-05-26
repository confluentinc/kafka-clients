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

//! Translation of `org.apache.kafka.common.utils.LogContext`.
//!
//! In Java, `LogContext` is a factory that wraps SLF4J `Logger` instances
//! with a context-prefixing decorator (`LocationAwareKafkaLogger` /
//! `LocationIgnorantKafkaLogger`). The wrapper exists because SLF4J doesn't
//! have a native MDC-style context for arbitrary prefixes.
//!
//! In Rust we use the `log` crate. There is no equivalent decorator pattern;
//! `log` macros expand to a `log::log!` call that already supports
//! `format_args!` interpolation. We therefore translate `LogContext` to a
//! tiny wrapper that:
//!
//! 1. Stores the prefix string (cheap clones via `Arc<str>` per CLAUDE.md
//!    rule 11 since the same `LogContext` is cloned across every component
//!    of a `KafkaProducer`).
//! 2. Exposes [`LogContext::log_prefix`] for use with `log::info!("{prefix}{msg}")`-style
//!    formatting at call sites.
//! 3. Provides a [`LogContext::format`] helper so call sites that want to
//!    prepend the prefix once can do so without repeating the format
//!    expression.
//!
//! Higher-fidelity wrapping (e.g. a `Logger` enum with `info`, `warn`, `error`
//! methods that auto-prepend) is not needed: every Rust call site is already
//! a one-liner like `log::info!("{}message", ctx.log_prefix())`.

use std::sync::Arc;

/// A reusable prefix factored out so dependent components log uniformly.
/// Mirrors `org.apache.kafka.common.utils.LogContext`.
#[derive(Clone)]
pub struct LogContext {
    log_prefix: Arc<str>,
}

impl LogContext {
    /// Construct an empty context (no prefix). Mirrors `new LogContext()`.
    pub fn new() -> Self {
        LogContext { log_prefix: Arc::from("") }
    }

    /// Construct with the given prefix. `None` becomes the empty prefix to
    /// match Java's `prefix == null ? "" : prefix` behaviour.
    pub fn with_prefix(prefix: Option<&str>) -> Self {
        LogContext { log_prefix: Arc::from(prefix.unwrap_or("")) }
    }

    /// Returns the prefix string. Mirrors `logPrefix()`.
    pub fn log_prefix(&self) -> &str {
        &self.log_prefix
    }

    /// Format `message` with the context prefix prepended. Equivalent to
    /// `log_prefix() + message` in Java.
    pub fn format(&self, message: &str) -> String {
        let mut s = String::with_capacity(self.log_prefix.len() + message.len());
        s.push_str(&self.log_prefix);
        s.push_str(message);
        s
    }
}

impl Default for LogContext {
    fn default() -> Self {
        LogContext::new()
    }
}

#[cfg(test)]
mod tests {
    // The Java client has no `LogContextTest.java` (only the SLF4J wrapper
    // logic, which is delegated to the `log` crate in Rust). We test the
    // small contract we expose: the prefix is stored verbatim, defaults to
    // empty, and `format` concatenates correctly.

    use super::*;

    #[test]
    fn default_prefix_is_empty() {
        let ctx = LogContext::new();
        assert_eq!(ctx.log_prefix(), "");
    }

    #[test]
    fn null_prefix_becomes_empty() {
        let ctx = LogContext::with_prefix(None);
        assert_eq!(ctx.log_prefix(), "");
    }

    #[test]
    fn prefix_is_stored_verbatim() {
        let ctx = LogContext::with_prefix(Some("[Producer clientId=abc] "));
        assert_eq!(ctx.log_prefix(), "[Producer clientId=abc] ");
    }

    #[test]
    fn format_prepends_prefix() {
        let ctx = LogContext::with_prefix(Some("[X] "));
        assert_eq!(ctx.format("hello"), "[X] hello");
    }

    #[test]
    fn clone_is_cheap_and_shares_prefix() {
        let ctx = LogContext::with_prefix(Some("shared"));
        let copy = ctx.clone();
        assert_eq!(copy.log_prefix(), ctx.log_prefix());
        assert_eq!(copy.log_prefix().as_ptr(), ctx.log_prefix().as_ptr());
    }
}
