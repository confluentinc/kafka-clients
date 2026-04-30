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

//! Translation of `org.apache.kafka.common.config.ConfigException`.
//!
//! Java's `ConfigException` is a `KafkaException` subclass. We collapse it
//! into [`KafkaError::Config`] which carries the same human-readable error
//! message. This module exposes a constructor function that mirrors the
//! Java constructor for callsite-translation readability.

use crate::common::errors::KafkaError;

/// Construct a `ConfigException` with `name` and `value` formatted into the
/// message. Mirrors Java's `new ConfigException(String name, Object value, String message)`.
///
/// `value` is formatted via [`std::fmt::Display`] (matching Java's
/// `Object.toString()` semantics), so a `&str` argument prints without quotes
/// and a `&ConfigValue::Int(5)` prints as `5` rather than `Int(5)`. The Rust
/// log/UX is then identical to the Java client's.
pub fn new(name: &str, value: impl std::fmt::Display, message: &str) -> KafkaError {
    KafkaError::Config(format!("Invalid value {value} for configuration {name}: {message}"))
}

/// Construct a `ConfigException` with just a free-form message. Mirrors
/// `new ConfigException(String message)`.
pub fn message(msg: impl Into<String>) -> KafkaError {
    KafkaError::Config(msg.into())
}
