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

//! Cluster configuration descriptor for integration tests.
//!
//! Describes the shape of a Kafka cluster that a group of tests needs.
//! Tests with identical `ClusterConfig` share one container, keeping
//! Docker container startup overhead amortized across many tests.

use std::collections::BTreeMap;

/// Describes cluster requirements for a group of tests.
///
/// Tests with identical `ClusterConfig` share one container.
/// The `Hash` and `Eq` implementations ensure that identical
/// configurations map to the same pool entry.
#[derive(Clone, Debug, Hash, Eq, PartialEq)]
pub struct ClusterConfig {
    /// Number of brokers (default: 1).
    pub brokers: u16,
    /// Extra server properties (key=value pairs set as environment variables).
    ///
    /// `BTreeMap` is used instead of `HashMap` so that `Hash` is deterministic.
    pub server_properties: BTreeMap<String, String>,
}

impl ClusterConfig {
    /// Single broker with custom server properties.
    pub fn with_properties(props: BTreeMap<String, String>) -> Self {
        Self { brokers: 1, server_properties: props }
    }
}

impl Default for ClusterConfig {
    /// Default single-broker cluster with no extra properties.
    fn default() -> Self {
        Self { brokers: 1, server_properties: BTreeMap::new() }
    }
}
