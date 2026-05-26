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

//! Translation of `org.apache.kafka.common.header.Headers`.

use crate::common::header::internals::record_header::RecordHeader;
use crate::common::header::internals::record_headers::RecordHeadersError;

/// A mutable ordered collection of [`Header`] objects. Multiple headers may
/// share a key — the order they were added in is preserved.
///
/// In Java this is an interface with `add`, `remove`, `lastHeader`, etc.
/// We translate it as a trait so consumer code (e.g. `ConsumerRecord` in a
/// later phase) can swap in alternative implementations.
///
/// `add` and `remove` may fail when the [`crate::common::header::internals::record_headers::RecordHeaders`]
/// has been marked read-only (Java throws `IllegalStateException`); we
/// surface that as `Result<&mut Self, RecordHeadersError>` so the producer
/// path doesn't have to wrap call sites in a panic-catcher.
pub trait Headers {
    /// Append a header. Mirrors `Headers.add(Header)`.
    fn add(&mut self, header: RecordHeader) -> Result<&mut Self, RecordHeadersError>;

    /// Construct and append a header from `(key, value)`. `value` may be
    /// `None` (null in Java).
    fn add_kv(&mut self, key: &str, value: Option<&[u8]>) -> Result<&mut Self, RecordHeadersError>;

    /// Remove every header whose key equals `key`.
    fn remove(&mut self, key: &str) -> Result<&mut Self, RecordHeadersError>;

    /// Return the most recently added header for `key`, or `None`.
    fn last_header(&self, key: &str) -> Option<&RecordHeader>;

    /// Iterate every header with the given key in insertion order.
    fn headers<'a>(&'a self, key: &'a str) -> Box<dyn Iterator<Item = &'a RecordHeader> + 'a>;

    /// Return all headers as a `Vec<&RecordHeader>` in insertion order.
    fn to_vec(&self) -> Vec<&RecordHeader>;
}
