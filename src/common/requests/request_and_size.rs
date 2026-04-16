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

//! A simple struct combining a parsed request with its size.
//!
//! Corresponds to `org.apache.kafka.common.requests.RequestAndSize`.

use super::ConcreteRequest;

/// A parsed request together with the number of bytes it consumed from the buffer.
///
/// Corresponds to `RequestAndSize` in Java.
#[derive(Debug)]
pub struct RequestAndSize {
    /// The parsed request.
    pub request: ConcreteRequest,
    /// The number of bytes consumed during parsing.
    pub size: usize,
}

impl RequestAndSize {
    /// Creates a new `RequestAndSize`.
    pub fn new(request: ConcreteRequest, size: usize) -> Self {
        Self { request, size }
    }
}
