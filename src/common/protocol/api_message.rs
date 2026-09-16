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

//! ApiMessage trait for top-level Kafka API messages.
//!
//! Corresponds to org.apache.kafka.common.protocol.ApiMessage

use super::Message;

/// Trait for top-level Kafka API messages.
///
/// An ApiMessage is a Message which is part of the top-level Kafka API,
/// identified by an API key.
///
/// Corresponds to org.apache.kafka.common.protocol.ApiMessage
pub trait ApiMessage: Message {
    /// Returns the API key of this message, or -1 if there is none.
    fn api_key(&self) -> i16;
}
