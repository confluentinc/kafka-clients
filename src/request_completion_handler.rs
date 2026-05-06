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

//! Translation of `org.apache.kafka.clients.RequestCompletionHandler`.
//!
//! Note: the Java type lives in `org.apache.kafka.clients` (not `common`),
//! so it sits at the crate root rather than under `common::`.

use crate::ClientResponse;

/// Callback fired when a [`crate::ClientRequest`] completes (response
/// received, disconnection, or timeout). Mirrors the Java interface.
///
/// Consumers must hold completion handlers behind
/// `Arc<dyn RequestCompletionHandler>` so `Sender` and the network thread
/// can share ownership without cloning.
pub trait RequestCompletionHandler: std::fmt::Debug + Send + Sync {
    /// Invoked with the completed [`ClientResponse`]. Mirrors
    /// `RequestCompletionHandler.onComplete(ClientResponse response)`.
    fn on_complete(&self, response: &ClientResponse);
}
