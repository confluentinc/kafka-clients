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

//! Logging macros that prepend a [`LogContext`](super::LogContext) prefix.
//!
//! These macros wrap the standard [`log`] crate macros and automatically
//! prepend the `LogContext` prefix to every log message, matching Java's
//! `LogContext.logger()` behavior.
//!
//! The `module_path!()` inside `log::*!` expands at the **call site**, so
//! `RUST_LOG` filtering works by the caller's module path.
//!
//! # Usage
//!
//! ```ignore
//! use confluent_kafka::common::utils::LogContext;
//!
//! let ctx = LogContext::new("[Producer clientId=my-producer] ");
//! kafka_debug!(ctx, "Starting Kafka producer I/O task.");
//! kafka_warn!(ctx, "Error connecting to node {}: {}", node_id, err);
//! ```

/// Log at ERROR level with a [`LogContext`](super::LogContext) prefix.
#[macro_export]
macro_rules! kafka_error {
    ($ctx:expr, $($arg:tt)*) => {
        log::error!("{}{}", $ctx.prefix(), format_args!($($arg)*))
    };
}

/// Log at WARN level with a [`LogContext`](super::LogContext) prefix.
#[macro_export]
macro_rules! kafka_warn {
    ($ctx:expr, $($arg:tt)*) => {
        log::warn!("{}{}", $ctx.prefix(), format_args!($($arg)*))
    };
}

/// Log at INFO level with a [`LogContext`](super::LogContext) prefix.
#[macro_export]
macro_rules! kafka_info {
    ($ctx:expr, $($arg:tt)*) => {
        log::info!("{}{}", $ctx.prefix(), format_args!($($arg)*))
    };
}

/// Log at DEBUG level with a [`LogContext`](super::LogContext) prefix.
#[macro_export]
macro_rules! kafka_debug {
    ($ctx:expr, $($arg:tt)*) => {
        log::debug!("{}{}", $ctx.prefix(), format_args!($($arg)*))
    };
}

/// Log at TRACE level with a [`LogContext`](super::LogContext) prefix.
#[macro_export]
macro_rules! kafka_trace {
    ($ctx:expr, $($arg:tt)*) => {
        log::trace!("{}{}", $ctx.prefix(), format_args!($($arg)*))
    };
}
