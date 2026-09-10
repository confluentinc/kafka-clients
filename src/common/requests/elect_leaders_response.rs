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

//! ElectLeaders response handling.
//!
//! Corresponds to `org.apache.kafka.common.requests.ElectLeadersResponse`.

use std::collections::HashMap;
use std::io;

use crate::common::protocol::{ApiKeys, Errors, Readable};
use crate::common::{Error, TopicPartition};
use crate::elect_leaders_response_data::{ElectLeadersResponseData, ReplicaElectionResult};

use super::abstract_response::update_error_counts;

/// An ElectLeaders response.
///
/// Corresponds to `org.apache.kafka.common.requests.ElectLeadersResponse`.
#[derive(Debug, Clone)]
pub struct ElectLeadersResponse {
    data: ElectLeadersResponseData,
}

/// The parameters of Java's four-argument `ElectLeadersResponse` constructor
/// (`ElectLeadersResponse.java:42`) that do not fit in the derived method name.
///
/// Java's two constructors (`:37`, `:42`) share no parameter name, so all four
/// of `:42`'s parameters reach its derived name. CLAUDE.md §2 caps that at three
/// and makes this struct the method's *only* parameter, so every Java parameter
/// lives here. This struct has no Java counterpart: it exists solely to satisfy
/// that naming rule (DoD #7).
///
/// It deliberately has **no** `Default`. Java declares no `ElectLeadersResponse`
/// overload that omits any of these four, so none has a Java-derived default —
/// for `version` in particular a synthesised `0` would silently drop the
/// top-level error code (`:49` encodes it only for v1+). Construct it with
/// [`ElectLeadersResponseOptionsBuilder::new`] and set all four: [`ElectLeadersResponseOptionsBuilder::build`] panics if any of `throttle_time_ms`, `error_code`, `election_results`, `version` was not set.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct ElectLeadersResponseOptions {
    /// Java's `throttleTimeMs`.
    pub throttle_time_ms: i32,
    /// Java's `errorCode`.
    pub error_code: i16,
    /// Java's `electionResults`.
    pub election_results: Vec<ReplicaElectionResult>,
    /// Java's `version`.
    pub version: i16,
}

/// Fluent builder for [`ElectLeadersResponseOptions`].
///
/// Per CLAUDE.md §2 [`Self::new`] takes no parameters, every parameter has a
/// fluent setter, and [`Self::build`] validates the mandatory ones — panicking
/// if they were not set. Like [`ElectLeadersResponseOptions`] it has no Java counterpart and
/// exists solely to satisfy that naming rule (DoD #7).
pub struct ElectLeadersResponseOptionsBuilder {
    throttle_time_ms: Option<i32>,
    error_code: Option<i16>,
    election_results: Option<Vec<ReplicaElectionResult>>,
    version: Option<i16>,
}

impl Default for ElectLeadersResponseOptionsBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl ElectLeadersResponseOptionsBuilder {
    /// Creates a builder with every mandatory parameter unset and every other
    /// parameter at the value Java passes on the caller's behalf.
    pub fn new() -> Self {
        Self { throttle_time_ms: None, error_code: None, election_results: None, version: None }
    }

    /// Sets [`ElectLeadersResponseOptions::throttle_time_ms`], a mandatory parameter: [`Self::build`]
    /// panics if it was not set.
    pub fn set_throttle_time_ms(mut self, throttle_time_ms: i32) -> Self {
        self.throttle_time_ms = Some(throttle_time_ms);
        self
    }
    /// Sets [`ElectLeadersResponseOptions::error_code`], a mandatory parameter: [`Self::build`]
    /// panics if it was not set.
    pub fn set_error_code(mut self, error_code: i16) -> Self {
        self.error_code = Some(error_code);
        self
    }
    /// Sets [`ElectLeadersResponseOptions::election_results`], a mandatory parameter: [`Self::build`]
    /// panics if it was not set.
    pub fn set_election_results(mut self, election_results: Vec<ReplicaElectionResult>) -> Self {
        self.election_results = Some(election_results);
        self
    }
    /// Sets [`ElectLeadersResponseOptions::version`], a mandatory parameter: [`Self::build`]
    /// panics if it was not set.
    pub fn set_version(mut self, version: i16) -> Self {
        self.version = Some(version);
        self
    }

    /// Returns the built options.
    ///
    /// Per CLAUDE.md §2 the mandatory parameters are validated here rather than
    /// being named in the constructor, so a later Java version that makes one of
    /// them optional changes the set this accepts instead of adding a second
    /// constructor. Today there is one mandatory set: `throttle_time_ms`, `error_code`, `election_results`, `version`.
    ///
    /// # Panics
    ///
    /// Panics if any parameter of that set was not given a setter call.
    pub fn build(self) -> ElectLeadersResponseOptions {
        ElectLeadersResponseOptions {
            throttle_time_ms: self.throttle_time_ms.unwrap_or_else(|| Self::missing("throttle_time_ms")),
            error_code: self.error_code.unwrap_or_else(|| Self::missing("error_code")),
            election_results: self.election_results.unwrap_or_else(|| Self::missing("election_results")),
            version: self.version.unwrap_or_else(|| Self::missing("version")),
        }
    }

    /// Panics naming a mandatory parameter [`Self::build`] found unset.
    fn missing(parameter: &str) -> ! {
        panic!("ElectLeadersResponseOptionsBuilder::build: mandatory parameter `{parameter}` was not set");
    }
}

impl ElectLeadersResponse {
    /// Creates a new `ElectLeadersResponse` from the underlying data.
    ///
    /// Corresponds to Java's `ElectLeadersResponse(ElectLeadersResponseData)`
    /// (`ElectLeadersResponse.java:37`).
    pub fn new_data(data: ElectLeadersResponseData) -> Self {
        Self { data }
    }

    /// Creates a response from throttle time, top-level error code and per-topic
    /// results.
    ///
    /// Corresponds to Java's
    /// `ElectLeadersResponse(int, short, List<ReplicaElectionResult>, short)`
    /// (`ElectLeadersResponse.java:42`) — the error code is only encoded for v1+.
    pub fn new_options(options: ElectLeadersResponseOptions) -> Self {
        let ElectLeadersResponseOptions { throttle_time_ms, error_code, election_results, version } = options;
        let mut data = ElectLeadersResponseData::new();
        data.set_throttle_time_ms(throttle_time_ms);
        if version >= 1 {
            data.set_error_code(error_code);
        }
        data.set_replica_election_results(election_results);
        Self { data }
    }

    /// Returns the API key for this response.
    pub fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::ELECT_LEADERS
    }

    /// Returns a reference to the underlying data.
    pub fn data(&self) -> &ElectLeadersResponseData {
        &self.data
    }

    /// Returns a mutable reference to the underlying data.
    pub(crate) fn data_mut(&mut self) -> &mut ElectLeadersResponseData {
        &mut self.data
    }

    /// Returns the throttle time in milliseconds.
    pub fn throttle_time_ms(&self) -> i32 {
        self.data.throttle_time_ms
    }

    /// Sets the throttle time in the response.
    pub fn maybe_set_throttle_time_ms(&mut self, throttle_time_ms: i32) {
        self.data.set_throttle_time_ms(throttle_time_ms);
    }

    /// Returns the error counts aggregated across the top-level error and all
    /// partition results.
    pub fn error_counts(&self) -> HashMap<Errors, i32> {
        let mut counts = HashMap::new();
        update_error_counts(&mut counts, Errors::for_code(self.data.error_code));
        for result in &self.data.replica_election_results {
            for partition_result in &result.partition_result {
                update_error_counts(&mut counts, Errors::for_code(partition_result.error_code));
            }
        }
        counts
    }

    /// Parses an `ElectLeadersResponse` from a readable buffer at the given
    /// version.
    ///
    /// # Errors
    ///
    /// Returns an error if parsing fails.
    pub fn parse(readable: &mut dyn Readable, version: i16) -> io::Result<Self> {
        let data = ElectLeadersResponseData::read(readable, version)?;
        Ok(Self::new_data(data))
    }

    /// Whether the client should throttle on this response (always true).
    pub fn should_client_throttle(&self, _version: i16) -> bool {
        true
    }

    /// Converts the response data into a per-partition election result map.
    ///
    /// A partition maps to `None` if it succeeded, or to the corresponding
    /// error otherwise.
    ///
    /// Mirrors `ElectLeadersResponse.electLeadersResult(ElectLeadersResponseData)`.
    pub fn elect_leaders_result(data: &ElectLeadersResponseData) -> HashMap<TopicPartition, Option<Error>> {
        let mut map = HashMap::new();
        for topic_results in &data.replica_election_results {
            for partition_result in &topic_results.partition_result {
                let error = Errors::for_code(partition_result.error_code);
                let value = if error == Errors::None {
                    None
                } else {
                    // Java: `error.exception(partitionResult.errorMessage())`. A null
                    // message must leave the code's own text in place, which
                    // `unwrap_or_default()` would shadow with an empty string.
                    Some(error.error_with_optional_message(partition_result.error_message.as_deref()))
                };
                map.insert(
                    TopicPartition::new(topic_results.topic.clone(), partition_result.partition_id),
                    value,
                );
            }
        }
        map
    }
}

impl std::fmt::Display for ElectLeadersResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ElectLeadersResponse(data={:?})", self.data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::elect_leaders_response_data::PartitionResult;

    fn result(topic: &str, partition: i32, error: Errors, message: Option<&str>) -> ReplicaElectionResult {
        let mut partition_result = PartitionResult::new();
        partition_result.set_partition_id(partition);
        partition_result.set_error_code(error.code());
        partition_result.set_error_message(message.map(str::to_string));
        let mut election_result = ReplicaElectionResult::new();
        election_result.set_topic(topic.to_string());
        election_result.set_partition_result(vec![partition_result]);
        election_result
    }

    #[test]
    fn from_results_encodes_error_code_only_for_v1_plus() {
        let v0 = ElectLeadersResponse::new_options(
            ElectLeadersResponseOptionsBuilder::new()
                .set_throttle_time_ms(0)
                .set_error_code(Errors::NotController.code())
                .set_election_results(Vec::new())
                .set_version(0)
                .build(),
        );
        assert_eq!(v0.data().error_code, Errors::None.code());
        let v1 = ElectLeadersResponse::new_options(
            ElectLeadersResponseOptionsBuilder::new()
                .set_throttle_time_ms(0)
                .set_error_code(Errors::NotController.code())
                .set_election_results(Vec::new())
                .set_version(1)
                .build(),
        );
        assert_eq!(v1.data().error_code, Errors::NotController.code());
    }

    #[test]
    fn elect_leaders_result_maps_success_and_error() {
        let mut data = ElectLeadersResponseData::new();
        data.set_replica_election_results(vec![
            result("t", 0, Errors::None, None),
            result("t", 1, Errors::ClusterAuthorizationFailed, Some("nope")),
        ]);
        let map = ElectLeadersResponse::elect_leaders_result(&data);
        assert!(map.get(&TopicPartition::new("t", 0)).unwrap().is_none());
        let err = map.get(&TopicPartition::new("t", 1)).unwrap().as_ref().unwrap();
        assert_eq!(err.error(), Errors::ClusterAuthorizationFailed);
        assert_eq!(err.message(), "nope");
    }

    /// Java is `error.exception(partitionResult.errorMessage())`, which falls back
    /// to the code's own text when the message is null. Building the error with
    /// `unwrap_or_default()` instead would shadow that text with an empty string.
    #[test]
    fn elect_leaders_result_keeps_the_default_text_when_the_message_is_null() {
        let mut data = ElectLeadersResponseData::new();
        data.set_replica_election_results(vec![result("t", 0, Errors::ClusterAuthorizationFailed, None)]);
        let map = ElectLeadersResponse::elect_leaders_result(&data);
        let err = map.get(&TopicPartition::new("t", 0)).unwrap().as_ref().unwrap();
        assert_eq!(err.error(), Errors::ClusterAuthorizationFailed);
        assert_eq!(err.message(), Errors::ClusterAuthorizationFailed.message());
        assert!(!err.message().is_empty());

        // An empty but present message is kept verbatim, as Java's null-only test does.
        let mut data = ElectLeadersResponseData::new();
        data.set_replica_election_results(vec![result("t", 0, Errors::ClusterAuthorizationFailed, Some(""))]);
        let map = ElectLeadersResponse::elect_leaders_result(&data);
        assert_eq!(map.get(&TopicPartition::new("t", 0)).unwrap().as_ref().unwrap().message(), "");
    }

    #[test]
    fn error_counts_includes_top_level_and_partition_errors() {
        let mut data = ElectLeadersResponseData::new();
        data.set_error_code(Errors::None.code());
        data.set_replica_election_results(vec![result("t", 0, Errors::ClusterAuthorizationFailed, None)]);
        let response = ElectLeadersResponse::new_data(data);
        let counts = response.error_counts();
        assert_eq!(counts.get(&Errors::None), Some(&1));
        assert_eq!(counts.get(&Errors::ClusterAuthorizationFailed), Some(&1));
    }

    /// CLAUDE.md §2: the mandatory parameters are validated in
    /// [`ElectLeadersResponseOptionsBuilder::build`], not named in the constructor, so a
    /// builder left untouched panics naming the first one it finds unset.
    #[test]
    #[should_panic(
        expected = "ElectLeadersResponseOptionsBuilder::build: mandatory parameter `throttle_time_ms` was not set"
    )]
    fn elect_leaders_response_options_builder_build_panics_when_no_mandatory_parameter_is_set() {
        let _ = ElectLeadersResponseOptionsBuilder::new().build();
    }
}
