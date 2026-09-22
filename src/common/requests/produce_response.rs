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

//! Produce response handling.
//!
//! Corresponds to `org.apache.kafka.common.requests.ProduceResponse`.

use crate::common::Error;
use std::collections::HashMap;
use std::io;

use crate::ProduceResponseData;
use crate::common::protocol::{ApiKeys, Errors, Readable};
use crate::produce_response_data::LeaderIdAndEpoch;

/// A Produce response.
///
/// Possible error codes:
/// - [`Errors::CorruptMessage`]
/// - [`Errors::UnknownTopicOrPartition`]
/// - [`Errors::NotLeaderOrFollower`]
/// - [`Errors::MessageTooLarge`]
/// - [`Errors::InvalidTopicError`]
/// - [`Errors::RecordListTooLarge`]
/// - [`Errors::NotEnoughReplicas`]
/// - [`Errors::NotEnoughReplicasAfterAppend`]
/// - [`Errors::InvalidRequiredAcks`]
/// - [`Errors::TopicAuthorizationFailed`]
/// - [`Errors::UnsupportedForMessageFormat`]
/// - [`Errors::InvalidProducerEpoch`]
/// - [`Errors::ClusterAuthorizationFailed`]
/// - [`Errors::TransactionalIdAuthorizationFailed`]
/// - [`Errors::InvalidRecord`]
/// - [`Errors::InvalidTxnState`]
///
/// Corresponds to `org.apache.kafka.common.requests.ProduceResponse`.
#[derive(Debug, Clone)]
pub struct ProduceResponse {
    data: ProduceResponseData,
}

impl ProduceResponse {
    /// Sentinel value for an invalid offset.
    pub const INVALID_OFFSET: i64 = -1;

    /// Creates a new `ProduceResponse` from data.
    pub fn new(data: ProduceResponseData) -> Self {
        Self { data }
    }

    /// Returns a reference to the underlying data.
    pub fn data(&self) -> &ProduceResponseData {
        &self.data
    }

    /// Returns a mutable reference to the underlying data.
    pub fn data_mut(&mut self) -> &mut ProduceResponseData {
        &mut self.data
    }

    /// Returns the API key for this response.
    pub fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::PRODUCE
    }

    /// Returns the error counts for this response.
    ///
    /// Iterates over all partition responses and counts each error code.
    pub fn error_counts(&self) -> HashMap<Errors, i32> {
        let mut counts = HashMap::new();
        for topic in &self.data.responses {
            for partition in &topic.partition_responses {
                let error = Errors::for_code(partition.error_code);
                super::AbstractResponse::update_error_counts(&mut counts, error);
            }
        }
        counts
    }

    /// Returns the throttle time in milliseconds.
    pub fn throttle_time_ms(&self) -> i32 {
        self.data.throttle_time_ms
    }

    /// Sets the throttle time in the response.
    pub fn maybe_set_throttle_time_ms(&mut self, throttle_time_ms: i32) {
        self.data.set_throttle_time_ms(throttle_time_ms);
    }

    /// Returns whether the client should throttle upon receiving this response.
    ///
    /// Client-side throttling is enabled starting from version 6.
    pub fn should_client_throttle(&self, version: i16) -> bool {
        version >= 6
    }

    /// Parses a `ProduceResponse` from a readable buffer at the given version.
    ///
    /// # Errors
    ///
    /// Returns an error if parsing fails.
    pub fn parse(readable: &mut dyn Readable, version: i16) -> io::Result<Self> {
        let data = ProduceResponseData::read(readable, version)?;
        Ok(Self::new(data))
    }
}

impl std::fmt::Display for ProduceResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ProduceResponse(data={:?})", self.data)
    }
}

/// A partition-level response within a produce response.
///
/// Corresponds to `ProduceResponse.PartitionResponse` in Java.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PartitionResponse {
    /// The error for this partition.
    pub error: Errors,
    /// The base offset assigned to the records.
    pub base_offset: i64,
    /// The log append time (-1 if CreateTime is used).
    pub log_append_time: i64,
    /// The log start offset.
    pub log_start_offset: i64,
    /// Per-record errors (batch index and optional error message).
    pub record_errors: Vec<RecordError>,
    /// Optional error message.
    pub error_message: Option<String>,
    /// The current leader for this partition, used by the producer to discover
    /// the leader when a `NOT_LEADER_OR_FOLLOWER` error is returned.
    pub current_leader: LeaderIdAndEpoch,
}

/// The parameters of Java's widest `ProduceResponse.PartitionResponse`
/// constructor (`ProduceResponse.java:188`).
///
/// Java's six constructors (`:168`, `:172`, `:176`, `:180`, `:184`, `:188`)
/// intersect on `{error}`, so the widest form carries six parameters into its
/// derived name. CLAUDE.md §2 caps that at three parameters and makes this
/// struct the method's *only* parameter, so every Java parameter lives here —
/// `error` included. This struct has no Java counterpart: it exists solely to
/// satisfy that naming rule (DoD #7).
///
/// Because the cap applies to the *whole* group, Java's `:184` and `:188` forms
/// derive the same name — `with_options`, differing only in whether they supply
/// `currentLeader`. They therefore collapse into the single constructor below,
/// with `:184`'s `new ProduceResponseData.LeaderIdAndEpoch()` becoming this
/// struct's initial `current_leader`.
///
/// It deliberately has **no** `Default`. Every field *except* `error` is
/// supplied by a narrower Java overload on the caller's behalf, and
/// [`PartitionResponseOptionsBuilder::new`] carries exactly those values; `error` is
/// what even Java's narrowest form (`:168`) takes from its caller, so it has no
/// Java-derived default — and a synthesised `Errors::None` would silently turn a
/// failed partition into a successful one.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct PartitionResponseOptions {
    /// Java's `error`.
    pub error: Errors,
    /// Java's `baseOffset`. Starts as `INVALID_OFFSET`, as in `:168`.
    pub base_offset: i64,
    /// Java's `logAppendTime`. Starts as `RecordBatch.NO_TIMESTAMP`, as in
    /// `:168`.
    pub log_append_time: i64,
    /// Java's `logStartOffset`. Starts as `INVALID_OFFSET`, as in `:168`.
    pub log_start_offset: i64,
    /// Java's `recordErrors`. Starts empty, as in `:176`
    /// (`Collections.emptyList()`).
    pub record_errors: Vec<RecordError>,
    /// Java's `errorMessage`. Starts as `None`, as in `:176`/`:180`.
    pub error_message: Option<String>,
    /// Java's `currentLeader`. Starts as `new LeaderIdAndEpoch()`, as in `:184`.
    pub current_leader: LeaderIdAndEpoch,
}

/// Fluent builder for [`PartitionResponseOptions`].
///
/// Per CLAUDE.md §2 [`Self::new`] takes no parameters, every parameter has a
/// fluent setter, and [`Self::build`] validates the mandatory ones — returning
/// [`Error::LocalIllegalArgument`] if they were not set. Like [`PartitionResponseOptions`] it has no Java counterpart and
/// exists solely to satisfy that naming rule (DoD #7).
pub struct PartitionResponseOptionsBuilder {
    error: Option<Errors>,
    base_offset: i64,
    log_append_time: i64,
    log_start_offset: i64,
    record_errors: Vec<RecordError>,
    error_message: Option<String>,
    current_leader: LeaderIdAndEpoch,
}

impl Default for PartitionResponseOptionsBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl PartitionResponseOptionsBuilder {
    /// Creates a builder with every mandatory parameter unset and every other
    /// parameter at the value Java passes on the caller's behalf.
    pub fn new() -> Self {
        Self {
            error: None,
            base_offset: ProduceResponse::INVALID_OFFSET,
            log_append_time: crate::common::record::internal::RecordBatch::NO_TIMESTAMP,
            log_start_offset: ProduceResponse::INVALID_OFFSET,
            record_errors: Vec::new(),
            error_message: None,
            current_leader: LeaderIdAndEpoch::new(),
        }
    }

    /// Sets [`PartitionResponseOptions::error`], a mandatory parameter: [`Self::build`]
    /// panics if it was not set.
    pub fn set_error(mut self, error: Errors) -> Self {
        self.error = Some(error);
        self
    }
    /// Sets [`PartitionResponseOptions::base_offset`].
    pub fn set_base_offset(mut self, base_offset: i64) -> Self {
        self.base_offset = base_offset;
        self
    }
    /// Sets [`PartitionResponseOptions::log_append_time`].
    pub fn set_log_append_time(mut self, log_append_time: i64) -> Self {
        self.log_append_time = log_append_time;
        self
    }
    /// Sets [`PartitionResponseOptions::log_start_offset`].
    pub fn set_log_start_offset(mut self, log_start_offset: i64) -> Self {
        self.log_start_offset = log_start_offset;
        self
    }
    /// Sets [`PartitionResponseOptions::record_errors`].
    pub fn set_record_errors(mut self, record_errors: Vec<RecordError>) -> Self {
        self.record_errors = record_errors;
        self
    }
    /// Sets [`PartitionResponseOptions::error_message`].
    pub fn set_error_message(mut self, error_message: Option<String>) -> Self {
        self.error_message = error_message;
        self
    }
    /// Sets [`PartitionResponseOptions::current_leader`].
    pub fn set_current_leader(mut self, current_leader: LeaderIdAndEpoch) -> Self {
        self.current_leader = current_leader;
        self
    }

    /// Returns the built options.
    ///
    /// Per CLAUDE.md §2 the mandatory parameters are validated here rather than
    /// being named in the constructor, so a later Java version that makes one of
    /// them optional changes the set this accepts instead of adding a second
    /// constructor. Today there is one mandatory set: `error`.
    ///
    /// # Errors
    ///
    /// Returns [`Error::LocalIllegalArgument`] naming the first parameter of that
    /// set which was not given a setter call. Only presence is checked here;
    /// semantic validation belongs to the method the options are passed to
    /// (CLAUDE.md §2).
    pub fn build(self) -> Result<PartitionResponseOptions, Error> {
        Ok(PartitionResponseOptions {
            error: self.error.ok_or_else(|| Self::missing("error"))?,
            base_offset: self.base_offset,
            log_append_time: self.log_append_time,
            log_start_offset: self.log_start_offset,
            record_errors: self.record_errors,
            error_message: self.error_message,
            current_leader: self.current_leader,
        })
    }

    /// Builds the [`Error::LocalIllegalArgument`] naming a mandatory parameter
    /// [`Self::build`] found unset.
    fn missing(parameter: &str) -> Error {
        Error::local_illegal_argument(format!(
            "PartitionResponseOptionsBuilder::build: mandatory parameter `{parameter}` was not set"
        ))
    }
}

impl PartitionResponse {
    /// Creates a `PartitionResponse` with just an error code (all offsets invalid).
    ///
    /// Corresponds to Java's `PartitionResponse(Errors)`
    /// (`ProduceResponse.java:168`), the overload whose parameters equal the
    /// group's intersection.
    pub fn new(error: Errors) -> Self {
        Self {
            error,
            base_offset: ProduceResponse::INVALID_OFFSET,
            log_append_time: crate::common::record::internal::RecordBatch::NO_TIMESTAMP,
            log_start_offset: ProduceResponse::INVALID_OFFSET,
            record_errors: Vec::new(),
            error_message: None,
            current_leader: LeaderIdAndEpoch::new(),
        }
    }

    /// Creates a `PartitionResponse` with error and message (all offsets invalid).
    ///
    /// Corresponds to Java's `PartitionResponse(Errors, String)`
    /// (`ProduceResponse.java:172`).
    pub fn with_error_message(error: Errors, error_message: Option<String>) -> Self {
        Self {
            error,
            base_offset: ProduceResponse::INVALID_OFFSET,
            log_append_time: crate::common::record::internal::RecordBatch::NO_TIMESTAMP,
            log_start_offset: ProduceResponse::INVALID_OFFSET,
            record_errors: Vec::new(),
            error_message,
            current_leader: LeaderIdAndEpoch::new(),
        }
    }

    /// Creates a `PartitionResponse` with all fields.
    ///
    /// Corresponds to Java's `PartitionResponse(Errors, long, long, long,
    /// List<RecordError>, String)` (`ProduceResponse.java:184`) **and**
    /// `PartitionResponse(Errors, long, long, long, List<RecordError>, String,
    /// LeaderIdAndEpoch)` (`:188`): both derive this same name under CLAUDE.md
    /// §2, so they collapse into one constructor, whose only parameter is
    /// [`PartitionResponseOptions`]. Leaving that struct's `current_leader` at
    /// its initial value — `:184`'s own `new LeaderIdAndEpoch()` — gives `:184`'s
    /// behaviour.
    pub fn with_options(options: PartitionResponseOptions) -> Self {
        let PartitionResponseOptions {
            error,
            base_offset,
            log_append_time,
            log_start_offset,
            record_errors,
            error_message,
            current_leader,
        } = options;
        Self {
            error,
            base_offset,
            log_append_time,
            log_start_offset,
            record_errors,
            error_message,
            current_leader,
        }
    }
}

impl std::fmt::Display for PartitionResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{error: {:?}, offset: {}, logAppendTime: {}, logStartOffset: {}, recordErrors: {:?}, currentLeader: {:?}, errorMessage: {}}}",
            self.error,
            self.base_offset,
            self.log_append_time,
            self.log_start_offset,
            self.record_errors,
            self.current_leader,
            self.error_message.as_deref().unwrap_or("null"),
        )
    }
}

/// A per-record error within a produce response.
///
/// Corresponds to `ProduceResponse.RecordError` in Java.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct RecordError {
    /// The batch index of the record that caused the error.
    pub batch_index: i32,
    /// Optional error message.
    pub message: Option<String>,
}

impl RecordError {
    /// Creates a `RecordError` with batch index and optional message.
    pub fn with_message(batch_index: i32, message: Option<String>) -> Self {
        Self { batch_index, message }
    }

    /// Creates a `RecordError` with just a batch index (no message).
    pub fn new(batch_index: i32) -> Self {
        Self { batch_index, message: None }
    }
}

impl std::fmt::Display for RecordError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "RecordError(batchIndex={}, message={})",
            self.batch_index,
            match &self.message {
                Some(m) => format!("'{}'", m),
                None => "null".to_string(),
            }
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ProduceResponseData;
    use crate::produce_response_data::{PartitionProduceResponse, TopicProduceResponse};

    #[test]
    fn test_produce_response_basic() {
        let data = ProduceResponseData::new();
        let response = ProduceResponse::new(data);
        assert_eq!(*response.api_key(), ApiKeys::PRODUCE);
        assert_eq!(response.throttle_time_ms(), 0);
    }

    #[test]
    fn test_produce_response_error_counts() {
        let mut ppr1 = PartitionProduceResponse::new();
        ppr1.set_error_code(Errors::None.code());

        let mut ppr2 = PartitionProduceResponse::new();
        ppr2.set_error_code(Errors::UnknownTopicOrPartition.code());

        let mut ppr3 = PartitionProduceResponse::new();
        ppr3.set_error_code(Errors::UnknownTopicOrPartition.code());

        let mut tpr = TopicProduceResponse::new();
        tpr.set_name("test".to_string());
        tpr.set_partition_responses(vec![ppr1, ppr2, ppr3]);

        let mut data = ProduceResponseData::new();
        data.set_responses(vec![tpr]);

        let response = ProduceResponse::new(data);
        let counts = response.error_counts();
        assert_eq!(counts.get(&Errors::None), Some(&1));
        assert_eq!(counts.get(&Errors::UnknownTopicOrPartition), Some(&2));
    }

    #[test]
    fn test_produce_response_throttle() {
        let mut data = ProduceResponseData::new();
        data.set_throttle_time_ms(500);
        let response = ProduceResponse::new(data);
        assert_eq!(response.throttle_time_ms(), 500);
    }

    #[test]
    fn test_should_client_throttle() {
        let data = ProduceResponseData::new();
        let response = ProduceResponse::new(data);
        assert!(!response.should_client_throttle(5));
        assert!(response.should_client_throttle(6));
        assert!(response.should_client_throttle(9));
    }

    #[test]
    fn test_partition_response_from_error() {
        let pr = PartitionResponse::new(Errors::UnknownTopicOrPartition);
        assert_eq!(pr.error, Errors::UnknownTopicOrPartition);
        assert_eq!(pr.base_offset, ProduceResponse::INVALID_OFFSET);
        assert!(pr.error_message.is_none());
        assert!(pr.record_errors.is_empty());
    }

    #[test]
    fn test_record_error_display() {
        let re = RecordError::with_message(5, Some("bad record".to_string()));
        assert_eq!(re.to_string(), "RecordError(batchIndex=5, message='bad record')");

        let re_none = RecordError::new(3);
        assert_eq!(re_none.to_string(), "RecordError(batchIndex=3, message=null)");
    }

    /// CLAUDE.md §2: the mandatory parameters are validated in
    /// [`PartitionResponseOptionsBuilder::build`], not named in the constructor, so a
    /// builder left untouched panics naming the first one it finds unset.
    #[test]
    fn partition_response_options_builder_build_errors_when_no_mandatory_parameter_is_set() {
        let Err(error) = PartitionResponseOptionsBuilder::new().build() else {
            panic!("build must reject the unset mandatory parameter");
        };
        assert!(matches!(error, Error::LocalIllegalArgument(_)), "{error:?}");
        assert_eq!(
            error.message(),
            "PartitionResponseOptionsBuilder::build: mandatory parameter `error` was not set"
        );
    }
}
