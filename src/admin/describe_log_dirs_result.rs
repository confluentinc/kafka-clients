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

//! The result of `Admin::describe_log_dirs`.
//!
//! Corresponds to `org.apache.kafka.clients.admin.DescribeLogDirsResult`.

use std::collections::HashMap;

use crate::common::KafkaFuture;

use super::LogDirDescription;

/// The result of the `Admin::describe_log_dirs` call.
///
/// Corresponds to `org.apache.kafka.clients.admin.DescribeLogDirsResult`.
pub struct DescribeLogDirsResult {
    futures: HashMap<i32, KafkaFuture<HashMap<String, LogDirDescription>>>,
}

impl DescribeLogDirsResult {
    /// Creates a new result from the per-broker futures.
    pub(crate) fn new(futures: HashMap<i32, KafkaFuture<HashMap<String, LogDirDescription>>>) -> Self {
        Self { futures }
    }

    /// Returns a map from broker id to a future which can be used to check the
    /// information of partitions on each individual broker. The result of the
    /// future is a map from broker log directory path to a description of that
    /// log directory.
    pub fn descriptions(&self) -> &HashMap<i32, KafkaFuture<HashMap<String, LogDirDescription>>> {
        &self.futures
    }

    /// Returns a future which succeeds only if all the brokers have responded
    /// without error, yielding a map from broker id to a map from broker log
    /// directory path to a description of that log directory.
    ///
    /// Corresponds to `DescribeLogDirsResult.allDescriptions`.
    pub fn all_descriptions(&self) -> KafkaFuture<HashMap<i32, HashMap<String, LogDirDescription>>> {
        let entries: Vec<(i32, KafkaFuture<HashMap<String, LogDirDescription>>)> =
            self.futures.iter().map(|(k, v)| (*k, v.clone())).collect();
        KafkaFuture::join_map(entries)
    }
}
