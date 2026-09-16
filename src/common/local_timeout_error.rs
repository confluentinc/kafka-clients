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

//! Translated from `java.util.concurrent.TimeoutException`.
//!
//! Not to be confused with `org.apache.kafka.common.errors.TimeoutException`,
//! which is [`TimeoutError`](crate::common::errors::TimeoutError) — see the type
//! docs below for why the two must stay apart.

use crate::common::kafka_error::message_only_error;

message_only_error! {
    /// A wait on a future timed out.
    ///
    /// Corresponds to Java's `java.util.concurrent.TimeoutException`, thrown by
    /// `Future.get(timeout, unit)` — **not** to
    /// `org.apache.kafka.common.errors.TimeoutException`, which is a
    /// `RetriableException` under `KafkaException` and is spelled
    /// [`Error::timeout`](crate::common::Error::timeout). The two are unrelated
    /// classes that share a simple name; conflating them makes a local await
    /// timeout answer `true` to `is_retriable_error()` / `is_api_error()` /
    /// `is_kafka_error()` and report a wire code, none of which Java does. A
    /// plain checked `java.util` class outside the Kafka hierarchy, so no
    /// predicate holds.
    ///
    /// The `Local` prefix marks this as a JDK class rather than a Kafka one, per
    /// CLAUDE.md §2 — every `java.*` error carries it, independently of the
    /// subpackage. That is what separates this from
    /// [`Error::Timeout`](crate::common::Error::Timeout): Java tells the two
    /// `TimeoutException`s apart by package, and a flat enum cannot, so the
    /// prefix carries what the package used to. It also says something the
    /// package name would not: these errors are raised *here*, never reported by
    /// a broker, which is why none of them has a wire code.
    LocalTimeoutError
}
