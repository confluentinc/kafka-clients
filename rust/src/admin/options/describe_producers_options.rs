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

//! Options for `Admin::describe_producers`.

/// Options for `Admin::describe_producers`.
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
#[doc(alias = "org.apache.kafka.clients.admin.DescribeProducersOptions")]
pub struct DescribeProducersOptions {
    timeout_ms: Option<i32>,
    broker_id: Option<i32>,
}

impl DescribeProducersOptions {
    /// Creates default options (default API timeout, no broker override).
    #[doc(alias = "org.apache.kafka.clients.admin.DescribeProducersOptions#DescribeProducersOptions")]
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the timeout in milliseconds for this operation, or `None` if the default
    /// API timeout for the `AdminClient` should be used.
    ///
    /// # Arguments
    ///
    /// - `timeout_ms` - the timeout in milliseconds, or `None` for the default API timeout.
    ///
    /// # Returns
    ///
    /// This `DescribeProducersOptions` instance.
    #[must_use]
    #[doc(alias = "org.apache.kafka.clients.admin.AbstractOptions#timeoutMs")]
    pub fn set_timeout_ms(mut self, timeout_ms: Option<i32>) -> Self {
        self.timeout_ms = timeout_ms;
        self
    }

    /// Returns the timeout in milliseconds for this operation, or `None` if the
    /// default API timeout for the `AdminClient` should be used.
    #[doc(alias = "org.apache.kafka.clients.admin.AbstractOptions#timeoutMs")]
    pub fn timeout_ms(&self) -> Option<i32> {
        self.timeout_ms
    }

    /// Sets the broker id to query for the topic partitions: the request is sent directly to that
    /// broker rather than to each partition's leader.
    ///
    /// # Arguments
    ///
    /// - `broker_id` - the broker id.
    ///
    /// # Returns
    ///
    /// This `DescribeProducersOptions` instance.
    #[must_use]
    #[doc(alias = "org.apache.kafka.clients.admin.DescribeProducersOptions#brokerId")]
    pub fn set_broker_id(mut self, broker_id: i32) -> Self {
        self.broker_id = Some(broker_id);
        self
    }

    /// Returns the broker id to query, or `None` if not set.
    #[doc(alias = "org.apache.kafka.clients.admin.DescribeProducersOptions#brokerId")]
    pub fn broker_id(&self) -> Option<i32> {
        self.broker_id
    }
}

impl std::fmt::Display for DescribeProducersOptions {
    /// Java's `toString()`: the broker id prints as Java's `OptionalInt`
    /// (`OptionalInt[3]`, `OptionalInt.empty`) and an unset timeout as `null`.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let broker_id = self
            .broker_id
            .map_or_else(|| "OptionalInt.empty".to_string(), |id| format!("OptionalInt[{id}]"));
        let timeout_ms = self.timeout_ms.map_or_else(|| "null".to_string(), |t| t.to_string());
        write!(f, "DescribeProducersOptions(brokerId={broker_id}, timeoutMs={timeout_ms})")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn to_string_follows_java() {
        assert_eq!(
            DescribeProducersOptions::new().to_string(),
            "DescribeProducersOptions(brokerId=OptionalInt.empty, timeoutMs=null)"
        );
        assert_eq!(
            DescribeProducersOptions::new()
                .set_broker_id(3)
                .set_timeout_ms(Some(500))
                .to_string(),
            "DescribeProducersOptions(brokerId=OptionalInt[3], timeoutMs=500)"
        );
    }

    #[test]
    fn defaults_match_java() {
        let options = DescribeProducersOptions::new();
        assert_eq!(options.timeout_ms(), None);
        assert_eq!(options.broker_id(), None);
    }

    #[test]
    fn fluent_broker_id_and_timeout() {
        let options = DescribeProducersOptions::new().set_broker_id(3).set_timeout_ms(Some(5000));
        assert_eq!(options.broker_id(), Some(3));
        assert_eq!(options.timeout_ms(), Some(5000));
    }
}
