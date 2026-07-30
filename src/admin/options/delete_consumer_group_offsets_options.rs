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

//! Options for `Admin::delete_consumer_group_offsets`.
//!
//! Corresponds to `org.apache.kafka.clients.admin.DeleteConsumerGroupOffsetsOptions`.

/// Options for `Admin::delete_consumer_group_offsets`.
///
/// Corresponds to `org.apache.kafka.clients.admin.DeleteConsumerGroupOffsetsOptions`.
/// Java's class adds no fields beyond `AbstractOptions` (only `timeoutMs`).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct DeleteConsumerGroupOffsetsOptions {
    timeout_ms: Option<i32>,
}

impl DeleteConsumerGroupOffsetsOptions {
    /// Creates default options.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the operation timeout in milliseconds (or `None` for the default).
    #[must_use]
    pub fn timeout_ms(mut self, timeout_ms: Option<i32>) -> Self {
        self.timeout_ms = timeout_ms;
        self
    }

    /// The operation timeout in milliseconds, or `None` for the default.
    pub fn timeout(&self) -> Option<i32> {
        self.timeout_ms
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_and_setter() {
        let options = DeleteConsumerGroupOffsetsOptions::new();
        assert_eq!(options.timeout(), None);
        let options = options.timeout_ms(Some(100));
        assert_eq!(options.timeout(), Some(100));
    }
}
