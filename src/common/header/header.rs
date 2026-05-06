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

//! Translation of `org.apache.kafka.common.header.Header`.

/// A key-value pair carried alongside a record. Mirrors Java's `Header`
/// interface.
///
/// `key()` is non-null in Java. `value()` is allowed to be null and the
/// translation surfaces that as `Option<&[u8]>`.
///
/// Per CLAUDE.md rule 12 (zero-copy through the write path), the borrowed
/// `&[u8]` returned by `value()` must not be copied by callers; the producer
/// path serializes directly into the batch buffer. The owned data lives in
/// the implementing struct.
pub trait Header {
    /// Returns the key of the header. Always non-empty in practice; never
    /// null in Java.
    fn key(&self) -> &str;

    /// Returns the value of the header. Returns `None` when the value is
    /// null in the Kafka wire form.
    fn value(&self) -> Option<&[u8]>;
}
