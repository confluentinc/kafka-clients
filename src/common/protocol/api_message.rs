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

//! Translation of `org.apache.kafka.common.protocol.ApiMessage`.

use crate::common::protocol::Message;

/// A Message which is part of the top-level Kafka API. Mirrors the Java
/// `ApiMessage` interface, which extends `Message` with an API key.
pub trait ApiMessage: Message {
    /// Returns the API key of this message, or `-1` if there is none.
    fn api_key(&self) -> i16;
}
