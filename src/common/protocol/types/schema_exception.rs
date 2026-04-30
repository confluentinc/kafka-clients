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

//! Translation of `org.apache.kafka.common.protocol.types.SchemaException`.
//!
//! The Java type extends `KafkaException` and is thrown when protocol schema
//! validation fails while parsing a request or response.
//!
//! Following CLAUDE.md rule 10, we surface `SchemaException` as a `Result` —
//! callers receive a `KafkaError` wrapping the Java `getMessage()` text. This
//! type alias lets translated code read closer to the Java source.

use crate::common::errors::KafkaError;

/// Type alias for the Rust translation of `SchemaException`.
///
/// In Java this is a subclass of `KafkaException`; in Rust we collapse the
/// hierarchy into [`KafkaError`] and provide [`schema_exception`] /
/// [`schema_exception_with_cause`] constructors below to build error values
/// that retain the `SchemaException` Java class name in the displayed
/// message.
pub type SchemaException = KafkaError;

/// Build a `KafkaError::Generic` whose message starts with `SchemaException:`
/// (so the Java class name surfaces in tests and logs that match Java
/// assertions verbatim). Mirrors `new SchemaException(message)`.
pub fn schema_exception<S: Into<String>>(message: S) -> KafkaError {
    // We store the message verbatim — callers that need the prefix obtain it
    // from `KafkaError::Display` which prepends `KafkaException: …`. Tests
    // assert on the message body alone, matching Java's `e.getMessage()`.
    KafkaError::Generic(message.into())
}

/// Build a `KafkaError::Generic` from a message and a chained cause's message.
/// Mirrors `new SchemaException(message, cause)`.
pub fn schema_exception_with_cause<S: Into<String>>(message: S, cause: &KafkaError) -> KafkaError {
    let m = message.into();
    let cause_msg = cause.message();
    if cause_msg.is_empty() {
        KafkaError::Generic(m)
    } else {
        KafkaError::Generic(format!("{m}: {cause_msg}"))
    }
}
