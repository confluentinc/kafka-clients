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

//! Translation of `org.apache.kafka.common.requests.AbstractRequest.Builder`.

use crate::common::errors::KafkaError;
use crate::common::protocol::ApiKey;
use crate::common::requests::AbstractRequest;

/// Translation of the abstract inner class
/// `org.apache.kafka.common.requests.AbstractRequest.Builder`.
///
/// Java's `Builder<T extends AbstractRequest>` is parameterised over the
/// concrete request type. Rust's analogue would be a generic trait, but
/// the consumer (`ClientRequest`) needs to hold a heterogeneous list of
/// builders behind a `dyn` reference, so we erase the build target and
/// return `Box<dyn AbstractRequest>`.
///
/// Consumers that need a typed return can implement an additional method
/// on their concrete builder type — at the moment Phase 5a only needs
/// the erased shape.
pub trait AbstractRequestBuilder: std::fmt::Debug + std::marker::Send + std::marker::Sync {
    /// Mirrors `Builder.apiKey()`.
    fn api_key(&self) -> &'static ApiKey;

    /// Mirrors `Builder.oldestAllowedVersion()`.
    fn oldest_allowed_version(&self) -> i16;

    /// Mirrors `Builder.latestAllowedVersion()`.
    fn latest_allowed_version(&self) -> i16;

    /// Mirrors `Builder.build(short version)`.
    fn build(&self, version: i16) -> Result<Box<dyn AbstractRequest>, KafkaError>;

    /// Mirrors `Builder.build()` (no-arg) — defaults to building at
    /// `latest_allowed_version()`.
    fn build_latest(&self) -> Result<Box<dyn AbstractRequest>, KafkaError> {
        self.build(self.latest_allowed_version())
    }
}
