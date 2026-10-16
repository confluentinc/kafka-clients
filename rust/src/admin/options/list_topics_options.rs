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

//! Options for `Admin::list_topics`.
//!
//! Corresponds to `org.apache.kafka.clients.admin.ListTopicsOptions`.

/// Options for `Admin::list_topics`.
///
/// Corresponds to `org.apache.kafka.clients.admin.ListTopicsOptions`.
///
/// Equality is Java's `equals`, which compares `listInternal` only: two
/// options differing only in their timeout are equal.
#[derive(Clone, Debug, Default)]
#[doc(alias = "org.apache.kafka.clients.admin.ListTopicsOptions")]
pub struct ListTopicsOptions {
    timeout_ms: Option<i32>,
    list_internal: bool,
}

impl ListTopicsOptions {
    /// Creates default options (`list_internal=false`, default API timeout).
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the timeout in milliseconds for this operation, or `None` to use the
    /// default API timeout for the `AdminClient`.
    #[must_use]
    pub fn set_timeout_ms(mut self, timeout_ms: Option<i32>) -> Self {
        self.timeout_ms = timeout_ms;
        self
    }

    /// The timeout in milliseconds for this operation, or `None` if the default
    /// API timeout should be used.
    pub fn timeout_ms(&self) -> Option<i32> {
        self.timeout_ms
    }

    /// Set whether we should list internal topics.
    #[must_use]
    pub fn set_list_internal(mut self, list_internal: bool) -> Self {
        self.list_internal = list_internal;
        self
    }

    /// Return true if we should list internal topics.
    pub fn should_list_internal(&self) -> bool {
        self.list_internal
    }
}

/// Java's `equals` (`ListTopicsOptions.java:69-75`) compares `listInternal`
/// only; the timeout inherited from `AbstractOptions` takes no part, in
/// `hashCode` (`:77-80`) either.
impl PartialEq for ListTopicsOptions {
    fn eq(&self, other: &Self) -> bool {
        // Destructured, so a field added to the struct must be placed on one
        // side of Java's comparison or the other.
        let Self { timeout_ms: _, list_internal } = self;
        *list_internal == other.list_internal
    }
}

impl Eq for ListTopicsOptions {}

impl std::fmt::Display for ListTopicsOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ListTopicsOptions(listInternal={})", self.list_internal)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_match_java() {
        let options = ListTopicsOptions::new();
        assert_eq!(options.timeout_ms(), None);
        assert!(!options.should_list_internal());
    }

    /// New test, no Java original: as in Java, the timeout takes no part in
    /// equality, so `new ListTopicsOptions().timeoutMs(1)` equals
    /// `new ListTopicsOptions().timeoutMs(2)`; `listInternal` does.
    #[test]
    fn equality_ignores_the_timeout() {
        assert_eq!(
            ListTopicsOptions::new().set_timeout_ms(Some(1)),
            ListTopicsOptions::new().set_timeout_ms(Some(2))
        );
        assert_eq!(ListTopicsOptions::new().set_timeout_ms(Some(1)), ListTopicsOptions::new());
        assert_ne!(
            ListTopicsOptions::new().set_timeout_ms(Some(1)).set_list_internal(true),
            ListTopicsOptions::new().set_timeout_ms(Some(1))
        );
    }

    #[test]
    fn fluent_setters_and_equality() {
        let a = ListTopicsOptions::new().set_list_internal(true);
        let b = ListTopicsOptions::new().set_list_internal(true);
        assert_eq!(a, b);
        assert!(a.should_list_internal());
        assert_ne!(a, ListTopicsOptions::new());
    }
}
