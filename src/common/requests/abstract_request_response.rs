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

//! Translation of `org.apache.kafka.common.requests.AbstractRequestResponse`.

use crate::common::protocol::Message;

/// Java's `AbstractRequestResponse` is a marker interface exposing a single
/// `data()` method that returns the underlying generated `ApiMessage`.
///
/// In Rust we use a trait. Concrete implementors are
/// [`super::AbstractRequest`], [`super::AbstractResponse`],
/// [`super::RequestHeader`], and [`super::ResponseHeader`].
///
/// We expose `data()` as `&dyn Message` rather than `&dyn ApiMessage`
/// because every consumer in the codebase calls one of the `Message`
/// methods (`size`, `write`, `read`, `add_size`). Header data structs
/// (`RequestHeaderData`, `ResponseHeaderData`) implement `Message` but
/// not `ApiMessage` (Java's headers do not extend `ApiMessage`), so the
/// `Message` upper bound is the natural common ancestor.
pub trait AbstractRequestResponse {
    /// Returns the generated message data backing this wrapper.
    fn data(&self) -> &dyn Message;
}
