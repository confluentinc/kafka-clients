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

//! Translated from `java.lang.IllegalArgumentException`.
//!
//! One of the four JDK classes the client raises directly. They carry the
//! `Local` prefix per CLAUDE.md §2 — every `java.*` error does, independently of
//! its subpackage — and they have no wire code, because a broker never reports
//! them.

use crate::common::error::message_only_error;

message_only_error! {
    /// Illegal argument error — an invalid argument was provided to a method.
    ///
    /// Corresponds to Java's `java.lang.IllegalArgumentException`, a sibling of
    /// `KafkaException` rather than a subclass, so no predicate holds for it.
    LocalIllegalArgumentError
}
