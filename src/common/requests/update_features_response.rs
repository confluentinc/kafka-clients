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

//! UpdateFeatures response handling.
//!
//! Corresponds to `org.apache.kafka.common.requests.UpdateFeaturesResponse`.
//!
//! Possible error codes:
//!   - [`Errors::ClusterAuthorizationFailed`]
//!   - [`Errors::NotController`]
//!   - [`Errors::InvalidRequest`]
//!   - [`Errors::FeatureUpdateFailed`]

use std::collections::{BTreeSet, HashMap};
use std::io;

use crate::common::protocol::{ApiKeys, Errors, Readable};
use crate::update_features_response_data::{UpdatableFeatureResult, UpdateFeaturesResponseData};

use super::abstract_response::update_error_counts;

/// An UpdateFeatures response.
///
/// Corresponds to `org.apache.kafka.common.requests.UpdateFeaturesResponse`.
#[derive(Debug, Clone)]
pub struct UpdateFeaturesResponse {
    data: UpdateFeaturesResponseData,
}

impl UpdateFeaturesResponse {
    /// Creates a new `UpdateFeaturesResponse` from the underlying data.
    pub fn new(data: UpdateFeaturesResponseData) -> Self {
        Self { data }
    }

    /// Creates a response carrying the given top-level error, optionally
    /// echoing per-feature results (only populated when the top-level error is
    /// [`Errors::None`], mirroring `createWithErrors`).
    ///
    /// Mirrors `UpdateFeaturesResponse.createWithErrors`.
    pub fn create_with_errors(
        top_level_error: Errors,
        top_level_message: Option<String>,
        updates: &BTreeSet<String>,
        throttle_time_ms: i32,
    ) -> Self {
        let mut results = Vec::new();
        if top_level_error == Errors::None {
            for feature in updates {
                let mut result = UpdatableFeatureResult::new();
                result.set_feature(feature.clone());
                result.set_error_code(top_level_error.code());
                result.set_error_message(top_level_message.clone());
                results.push(result);
            }
        }
        let mut data = UpdateFeaturesResponseData::new();
        data.set_throttle_time_ms(throttle_time_ms);
        data.set_error_code(top_level_error.code());
        data.set_error_message(top_level_message);
        data.set_results(results);
        Self { data }
    }

    /// Returns the API key for this response.
    pub fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::UPDATE_FEATURES
    }

    /// Returns a reference to the underlying data.
    pub fn data(&self) -> &UpdateFeaturesResponseData {
        &self.data
    }

    /// Returns a mutable reference to the underlying data.
    pub(crate) fn data_mut(&mut self) -> &mut UpdateFeaturesResponseData {
        &mut self.data
    }

    /// Returns the top-level error code.
    pub fn top_level_error_code(&self) -> i16 {
        self.data.error_code
    }

    /// Returns the top-level error message, if any.
    pub fn top_level_error_message(&self) -> Option<&str> {
        self.data.error_message.as_deref()
    }

    /// Returns the throttle time in milliseconds.
    pub fn throttle_time_ms(&self) -> i32 {
        self.data.throttle_time_ms
    }

    /// Sets the throttle time in the response.
    pub fn maybe_set_throttle_time_ms(&mut self, throttle_time_ms: i32) {
        self.data.set_throttle_time_ms(throttle_time_ms);
    }

    /// Whether the client should throttle on this response.
    ///
    /// `UpdateFeaturesResponse` does not override `shouldClientThrottle` in
    /// Java, so it inherits `AbstractResponse`'s default of `false`.
    pub fn should_client_throttle(&self, _version: i16) -> bool {
        false
    }

    /// Returns the error counts aggregated across the top-level error and all
    /// per-feature results.
    pub fn error_counts(&self) -> HashMap<Errors, i32> {
        let mut counts = HashMap::new();
        update_error_counts(&mut counts, Errors::for_code(self.data.error_code));
        for result in &self.data.results {
            update_error_counts(&mut counts, Errors::for_code(result.error_code));
        }
        counts
    }

    /// Parses an `UpdateFeaturesResponse` from a readable buffer at the given
    /// version.
    ///
    /// # Errors
    ///
    /// Returns an error if parsing fails.
    pub fn parse(readable: &mut dyn Readable, version: i16) -> io::Result<Self> {
        let data = UpdateFeaturesResponseData::read(readable, version)?;
        Ok(Self::new(data))
    }
}

impl std::fmt::Display for UpdateFeaturesResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "UpdateFeaturesResponse(data={:?})", self.data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_with_errors_none_echoes_updates() {
        let mut updates = BTreeSet::new();
        updates.insert("f1".to_string());
        updates.insert("f2".to_string());
        let response = UpdateFeaturesResponse::create_with_errors(Errors::None, None, &updates, 0);
        assert_eq!(response.data().error_code, Errors::None.code());
        assert_eq!(response.data().results.len(), 2);
        for result in &response.data().results {
            assert_eq!(result.error_code, Errors::None.code());
        }
    }

    #[test]
    fn create_with_errors_top_level_error_has_no_results() {
        let updates = BTreeSet::new();
        let response = UpdateFeaturesResponse::create_with_errors(Errors::InvalidRequest, None, &updates, 10);
        assert_eq!(response.data().error_code, Errors::InvalidRequest.code());
        assert_eq!(response.data().throttle_time_ms, 10);
        assert!(response.data().results.is_empty());
    }

    #[test]
    fn error_counts_includes_top_level_and_results() {
        let mut updates = BTreeSet::new();
        updates.insert("f1".to_string());
        let response = UpdateFeaturesResponse::create_with_errors(Errors::None, None, &updates, 0);
        let counts = response.error_counts();
        // Top-level NONE plus one per-feature NONE result.
        assert_eq!(counts.get(&Errors::None), Some(&2));
    }

    /// Round-trips a response through the shared `ConcreteResponse` serialize /
    /// parse path.
    #[test]
    fn serialize_parse_round_trip() {
        let mut updates = BTreeSet::new();
        updates.insert("f1".to_string());
        let response = UpdateFeaturesResponse::create_with_errors(Errors::None, None, &updates, 5);
        let mut concrete = super::super::ConcreteResponse::UpdateFeatures(response);
        let bytes = concrete.serialize(1).unwrap();
        let mut readable = crate::common::ByteBufferAccessor::from_bytes(bytes.into_buffer());
        let parsed = UpdateFeaturesResponse::parse(&mut readable, 1).unwrap();
        assert_eq!(parsed.data().throttle_time_ms, 5);
        assert_eq!(parsed.data().error_code, Errors::None.code());
        assert_eq!(parsed.data().results.len(), 1);
        assert_eq!(parsed.data().results[0].feature, "f1");
    }

    /// Byte-level encoding test against a known vector. UpdateFeatures response
    /// v1 is flexible; the body is:
    ///   throttle_time_ms: int32 = 5 (00 00 00 05)
    ///   error_code: int16 = 0 (00 00)
    ///   error_message: compact nullable string = null (00)
    ///   results: compact array (len+1 = 0x02)
    ///     feature: compact string "f1" (0x03, 0x66 0x31)
    ///     error_code: int16 = 0 (00 00)
    ///     error_message: compact nullable string = null (00)
    ///     _tagged_fields: 0x00
    ///   _tagged_fields: 0x00
    #[test]
    fn serialize_known_byte_vector_v1() {
        let mut updates = BTreeSet::new();
        updates.insert("f1".to_string());
        let response = UpdateFeaturesResponse::create_with_errors(Errors::None, None, &updates, 5);
        let mut concrete = super::super::ConcreteResponse::UpdateFeatures(response);
        let bytes = concrete.serialize(1).unwrap();
        let expected: &[u8] = &[
            0x00, 0x00, 0x00, 0x05, // throttle_time_ms = 5
            0x00, 0x00, // error_code = 0
            0x00, // error_message = null
            0x02, // results array length + 1
            0x03, 0x66, 0x31, // feature "f1"
            0x00, 0x00, // result error_code = 0
            0x00, // result error_message = null
            0x00, // result tagged fields
            0x00, // response tagged fields
        ];
        assert_eq!(bytes.into_buffer().as_slice(), expected);
    }
}
