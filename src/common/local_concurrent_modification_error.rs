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

//! Translated from `java.util.ConcurrentModificationException`.
//!
//! See [`LocalIllegalArgumentError`](crate::common::LocalIllegalArgumentError)
//! for why the four JDK classes carry the `Local` prefix and no wire code.

use crate::common::kafka_error::message_only_error;

message_only_error! {
    /// Concurrent modification error — the consumer was accessed from more than
    /// one task.
    ///
    /// Corresponds to Java's `java.util.ConcurrentModificationException`, thrown
    /// by `KafkaConsumer.acquire()` ("KafkaConsumer is not safe for
    /// multi-threaded access"). A plain `RuntimeException`, so no predicate
    /// holds for it.
    LocalConcurrentModificationError
}
