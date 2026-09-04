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

use std::collections::HashMap;
use std::io;

use crate::common::protocol::{ApiKeys, Errors, Readable};
use crate::produce_response_data::{LeaderIdAndEpoch, ProduceResponseData};

/// Sentinel value for an invalid offset.
pub const INVALID_OFFSET: i64 = -1;

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
                super::abstract_response::update_error_counts(&mut counts, error);
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
/// constructor (`ProduceResponse.java:188`) that do not fit in the derived
/// method name.
///
/// Java's six constructors (`:168`, `:172`, `:176`, `:180`, `:184`, `:188`)
/// intersect on `{error}`, so the widest form carries six parameters into its
/// derived name. CLAUDE.md §2 caps that at three parameters and moves the
/// remainder here. This struct has no Java counterpart: it exists solely to
/// satisfy that naming rule (DoD #7).
///
/// Because the cap applies to the *whole* group, Java's `:184` and `:188` forms
/// derive the same name — `new_base_offset_log_append_time_options`, differing
/// only in whether they supply `currentLeader`. They therefore collapse into the
/// single constructor below, with `:184`'s
/// `new ProduceResponseData.LeaderIdAndEpoch()` becoming this struct's
/// `current_leader` default.
///
/// Its `Default` is Java's own throughout — every field is supplied by a
/// narrower Java overload on the caller's behalf: `log_start_offset` by `:168`
/// (`INVALID_OFFSET`), `record_errors` by `:176` (`Collections.emptyList()`),
/// `error_message` by `:176`/`:180` (`null`), and `current_leader` by `:184`.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct PartitionResponseOptions {
    /// Java's `logStartOffset`.
    pub log_start_offset: i64,
    /// Java's `recordErrors`.
    pub record_errors: Vec<RecordError>,
    /// Java's `errorMessage`.
    pub error_message: Option<String>,
    /// Java's `currentLeader`.
    pub current_leader: LeaderIdAndEpoch,
}

impl Default for PartitionResponseOptions {
    fn default() -> Self {
        Self {
            log_start_offset: INVALID_OFFSET,
            record_errors: Vec::new(),
            error_message: None,
            current_leader: LeaderIdAndEpoch::new(),
        }
    }
}

impl PartitionResponseOptions {
    /// Creates the options carrying the given log start offset, per-record
    /// errors, error message and current leader.
    pub fn new(
        log_start_offset: i64,
        record_errors: Vec<RecordError>,
        error_message: Option<String>,
        current_leader: LeaderIdAndEpoch,
    ) -> Self {
        Self { log_start_offset, record_errors, error_message, current_leader }
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
            base_offset: INVALID_OFFSET,
            log_append_time: crate::common::record::internal::RecordBatch::NO_TIMESTAMP,
            log_start_offset: INVALID_OFFSET,
            record_errors: Vec::new(),
            error_message: None,
            current_leader: LeaderIdAndEpoch::new(),
        }
    }

    /// Creates a `PartitionResponse` with error and message (all offsets invalid).
    ///
    /// Corresponds to Java's `PartitionResponse(Errors, String)`
    /// (`ProduceResponse.java:172`).
    pub fn new_error_message(error: Errors, error_message: Option<String>) -> Self {
        Self {
            error,
            base_offset: INVALID_OFFSET,
            log_append_time: crate::common::record::internal::RecordBatch::NO_TIMESTAMP,
            log_start_offset: INVALID_OFFSET,
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
    /// §2, so they collapse into one constructor. Pass
    /// [`PartitionResponseOptions::default`] — whose `current_leader` is `:184`'s
    /// own `new LeaderIdAndEpoch()` — to get `:184`'s behaviour.
    pub fn new_base_offset_log_append_time_options(
        error: Errors,
        base_offset: i64,
        log_append_time: i64,
        options: PartitionResponseOptions,
    ) -> Self {
        let PartitionResponseOptions { log_start_offset, record_errors, error_message, current_leader } = options;
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
    pub fn new_message(batch_index: i32, message: Option<String>) -> Self {
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
    use crate::produce_response_data::{PartitionProduceResponse, ProduceResponseData, TopicProduceResponse};

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
        assert_eq!(pr.base_offset, INVALID_OFFSET);
        assert!(pr.error_message.is_none());
        assert!(pr.record_errors.is_empty());
    }

    #[test]
    fn test_record_error_display() {
        let re = RecordError::new_message(5, Some("bad record".to_string()));
        assert_eq!(re.to_string(), "RecordError(batchIndex=5, message='bad record')");

        let re_none = RecordError::new(3);
        assert_eq!(re_none.to_string(), "RecordError(batchIndex=3, message=null)");
    }
}
