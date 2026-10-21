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

//! Translated from `java.lang.UnsupportedOperationException`.
//!
//! See [`LocalIllegalArgumentError`](crate::common::LocalIllegalArgumentError)
//! for why the five JDK classes carry the `Local` prefix and no wire code.

use crate::common::error::message_only_error;

message_only_error! {
    /// Unsupported operation error — the requested operation is not supported.
    ///
    /// Corresponds to Java's `java.lang.UnsupportedOperationException`; like
    /// [`LocalIllegalArgumentError`](crate::common::LocalIllegalArgumentError),
    /// outside the `KafkaException` hierarchy. Not to be confused with
    /// `UnsupportedVersionError`, Java's wire-level `UnsupportedVersionException`
    /// for a broker that lacks an API version.
    // translates java.lang.UnsupportedOperationException, a JDK runtime exception (CLAUDE.md §2)
    #[doc(alias = "rust-only")]
    LocalUnsupportedOperationError
}
