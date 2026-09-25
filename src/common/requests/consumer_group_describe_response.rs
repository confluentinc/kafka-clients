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

//! `ConsumerGroupDescribe` response handling.
//!
//! Corresponds to `org.apache.kafka.common.requests.ConsumerGroupDescribeResponse`.

use std::collections::HashMap;
use std::io;

use crate::ConsumerGroupDescribeResponseData;
use crate::common::protocol::{ApiKeys, Errors, Readable};

use super::AbstractResponse;

/// A `ConsumerGroupDescribe` response.
///
/// Corresponds to `org.apache.kafka.common.requests.ConsumerGroupDescribeResponse`.
#[derive(Debug, Clone)]
pub struct ConsumerGroupDescribeResponse {
    data: ConsumerGroupDescribeResponseData,
}

impl ConsumerGroupDescribeResponse {
    /// Creates a new `ConsumerGroupDescribeResponse` from the underlying data.
    pub fn new(data: ConsumerGroupDescribeResponseData) -> Self {
        Self { data }
    }

    /// Returns the API key for this response.
    pub fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::CONSUMER_GROUP_DESCRIBE
    }

    /// Returns a reference to the underlying data.
    pub fn data(&self) -> &ConsumerGroupDescribeResponseData {
        &self.data
    }

    /// Returns a mutable reference to the underlying data.
    pub(crate) fn data_mut(&mut self) -> &mut ConsumerGroupDescribeResponseData {
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

    /// Whether the client should throttle upon receiving this response.
    ///
    /// Mirrors the `AbstractResponse` default (`ConsumerGroupDescribeResponse`
    /// does not override it), which returns `false`.
    pub fn should_client_throttle(&self, _version: i16) -> bool {
        false
    }

    /// Returns error counts by [`Errors`], aggregated per described group.
    ///
    /// Mirrors `ConsumerGroupDescribeResponse.errorCounts`.
    pub fn error_counts(&self) -> HashMap<Errors, i32> {
        let mut counts = HashMap::new();
        for group in &self.data.groups {
            AbstractResponse::update_error_counts(&mut counts, Errors::for_code(group.error_code));
        }
        counts
    }

    /// Parses a `ConsumerGroupDescribeResponse` from a readable buffer at the
    /// given version.
    ///
    /// # Errors
    ///
    /// Returns an error if parsing fails.
    pub fn parse(readable: &mut dyn Readable, version: i16) -> io::Result<Self> {
        let data = ConsumerGroupDescribeResponseData::read(readable, version)?;
        Ok(Self::new(data))
    }
}

impl std::fmt::Display for ConsumerGroupDescribeResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::consumer_group_describe_response_data::DescribedGroup;

    /// Byte-level wire-encoding check for a v0 (flexible) response with one NONE
    /// group whose scalar fields carry their defaults. Leading fields:
    ///   throttle_time_ms: int32 = 0 -> 00 00 00 00
    ///   groups: compact array len 1 -> 0x02 (N+1)
    ///     error_code: int16 = 0 -> 00 00
    ///     error_message: compact nullable string = null -> 0x00
    ///     group_id "g1": compact string len 2 -> 0x03, bytes 67 31
    ///     group_state "": compact string len 0 -> 0x01
    ///     group_epoch: int32 = 0 -> 00 00 00 00
    ///     assignment_epoch: int32 = 0 -> 00 00 00 00
    ///     assignor_name "": compact string len 0 -> 0x01
    ///     members: compact array len 0 -> 0x01
    ///     authorized_operations: int32 default -2147483648 -> 80 00 00 00
    ///     group _tagged_fields: 0x00
    ///   response _tagged_fields: 0x00
    #[test]
    fn test_serialize_known_byte_vector_v0() {
        let mut data = ConsumerGroupDescribeResponseData::new();
        let mut group = DescribedGroup::new();
        group.set_group_id("g1".to_string()).set_error_code(Errors::None.code());
        data.set_groups(vec![group]);
        let mut resp =
            crate::common::requests::ConcreteResponse::ConsumerGroupDescribe(ConsumerGroupDescribeResponse::new(data));
        let expected: &[u8] = &[
            0x00, 0x00, 0x00, 0x00, // throttle_time_ms
            0x02, // groups compact array len 1
            0x00, 0x00, // error_code
            0x00, // error_message null
            0x03, 0x67, 0x31, // group_id "g1"
            0x01, // group_state ""
            0x00, 0x00, 0x00, 0x00, // group_epoch
            0x00, 0x00, 0x00, 0x00, // assignment_epoch
            0x01, // assignor_name ""
            0x01, // members compact array len 0
            0x80, 0x00, 0x00, 0x00, // authorized_operations default (MIN_VALUE)
            0x00, // group tagged fields
            0x00, // response tagged fields
        ];
        assert_eq!(resp.serialize(0).unwrap().into_buffer().as_slice(), expected);
    }

    #[test]
    fn test_error_counts_aggregates_per_group() {
        let mut g1 = DescribedGroup::new();
        g1.set_group_id("g1".to_string()).set_error_code(Errors::GroupIdNotFound.code());
        let mut g2 = DescribedGroup::new();
        g2.set_group_id("g2".to_string()).set_error_code(Errors::None.code());
        let mut data = ConsumerGroupDescribeResponseData::new();
        data.set_groups(vec![g1, g2]);
        let resp = ConsumerGroupDescribeResponse::new(data);
        let counts = resp.error_counts();
        assert_eq!(counts.get(&Errors::GroupIdNotFound).copied().unwrap_or(0), 1);
        assert_eq!(counts.get(&Errors::None).copied().unwrap_or(0), 1);
    }

    #[test]
    fn test_should_client_throttle_is_false() {
        let resp = ConsumerGroupDescribeResponse::new(ConsumerGroupDescribeResponseData::new());
        assert!(!resp.should_client_throttle(0));
        assert!(!resp.should_client_throttle(1));
    }
}
