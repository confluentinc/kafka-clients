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

//! Options for `Admin::list_consumer_group_offsets`.
//!
//! Corresponds to `org.apache.kafka.clients.admin.ListConsumerGroupOffsetsOptions`.

/// Options for `Admin::list_consumer_group_offsets`.
///
/// Corresponds to `org.apache.kafka.clients.admin.ListConsumerGroupOffsetsOptions`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ListConsumerGroupOffsetsOptions {
    timeout_ms: Option<i32>,
    require_stable: bool,
}

impl ListConsumerGroupOffsetsOptions {
    /// Creates default options.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets an optional `requireStable` flag. Mirrors `requireStable(boolean)`.
    #[must_use]
    pub fn require_stable(mut self, require_stable: bool) -> Self {
        self.require_stable = require_stable;
        self
    }

    /// Set the operation timeout in milliseconds (or `None` for the default).
    #[must_use]
    pub fn set_timeout_ms(mut self, timeout_ms: Option<i32>) -> Self {
        self.timeout_ms = timeout_ms;
        self
    }

    /// Whether stable offsets are required. Mirrors `requireStable()`.
    pub fn should_require_stable(&self) -> bool {
        self.require_stable
    }

    /// The operation timeout in milliseconds, or `None` for the default.
    pub fn timeout_ms(&self) -> Option<i32> {
        self.timeout_ms
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_and_setter() {
        let options = ListConsumerGroupOffsetsOptions::new();
        assert!(!options.should_require_stable());
        let options = options.require_stable(true).set_timeout_ms(Some(300));
        assert!(options.should_require_stable());
        assert_eq!(options.timeout_ms(), Some(300));
    }
}
