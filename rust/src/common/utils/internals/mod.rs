// Copyright 2026 Confluent Inc.
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

//! Internal utility classes (org.apache.kafka.common.utils.internals)
//!
//! Kafka 4.4 (KAFKA-20297) moved the internal utilities of
//! `org.apache.kafka.common.utils` into this package. Java ships no
//! `package-info.java` for it; the parent package's documentation describes
//! these classes as "internal utilities and not part of the supported Kafka
//! API; their implementation may change without warning between releases".
//! The package name contains `internal`, so every class here is crate-private
//! (CLAUDE.md §2).
//!
//! # Dead-code lint
//!
//! The classes are translated in full (DoD #2), but the client uses only part
//! of them; the rest has no caller yet, or only the translated tests. Nothing
//! outside the crate can reach them, so the module allows dead code rather
//! than dropping Java methods.

#![expect(dead_code)]

mod byte_buffer_output_stream;
mod byte_utils;
mod exponential_backoff;
mod log_context;
mod producer_id_and_epoch;

pub(crate) use byte_buffer_output_stream::ByteBufferOutputStream;
pub(crate) use byte_utils::ByteUtils;
pub(crate) use exponential_backoff::ExponentialBackoff;
pub(crate) use log_context::LogContext;
pub(crate) use producer_id_and_epoch::ProducerIdAndEpoch;
